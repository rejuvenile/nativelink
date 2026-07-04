// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::borrow::Borrow;
use core::fmt::Debug;
use core::hash::Hash;
use core::ops::RangeBounds;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use nativelink_config::stores::EvictionPolicy;
use nativelink_metric::MetricsComponent;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::background_spawn;
use crate::evicting_map::{ItemCallback, LenEntry, NoopCallback};
use crate::instant_wrapper::InstantWrapper;
use crate::metrics_utils::{Counter, CounterWithTime};

/// Maximum fraction of max_bytes that can be pinned (25%).
const PIN_CAP_FRACTION: f64 = 0.25;
/// Seconds before a pin automatically expires.
///
/// `pub` so downstream crates (e.g. `nativelink-service`'s chunked
/// commit-watchdog soft-warn layer) can derive ordering constants
/// parametrically against the pin TTL ceiling without duplicating the
/// literal. See `chunked_write_handler::CHUNKED_COMMIT_SOFT_WARN_SECS`
/// for the derived value (`PIN_TIMEOUT_SECS / 4 = 30`).
pub const PIN_TIMEOUT_SECS: u64 = 120;
/// #605: cadence of the periodic forced-drain tick in the background
/// `drain_evictions` loop. moka's weight-based eviction is
/// EVENTUALLY-CONSISTENT — it only enforces the byte cap on `insert` or
/// an explicit `run_pending_tasks`. A cache that took its overshoot via
/// the startup `insert_with_time` path (which defers `run_pending_tasks`
/// for throughput) and then sees no further runtime `insert` would trail
/// its cap indefinitely (the production #605 overshoot: a 40 GiB worker
/// fast tier observed at ~162 GB). This tick periodically calls
/// `run_pending_tasks_and_drain` so the cache converges to cap during
/// runtime, bounding the worst-case overshoot duration to one interval.
///
/// Value: aligned with the sibling pin-expiry maintenance tick in the
/// same loop (`Duration::from_secs(10)` at the `pin_check_interval`).
/// One shared maintenance heartbeat keeps the cadence reasoning in one
/// place; 10 s is short enough that a runtime overshoot is corrected
/// promptly yet long enough that the periodic `run_pending_tasks` walk
/// (which fires eviction unref/listener callbacks) is negligible
/// overhead on an at-or-under-cap cache (where it produces no eviction
/// events and returns after a single iteration). NOT a per-entry TTL —
/// it adds no `time_to_live`/`time_to_idle` to cache entries; it is a
/// loop-driven maintenance call exactly like `expire_stale_pins`.
const DRAIN_INTERVAL_SECS: u64 = 10;
// Eviction channel is unbounded (mpsc::unbounded_channel). Each EvictionEvent
// is ~64 bytes (Arc<K> + T). At 1M entries that's ~64MB, well within budget.
// Unbounded avoids blocking moka's internal lock during burst eviction
// (bounded channels overflow under heavy load — 3,656 overflows in 10 min
// with a 32K bounded channel).

/// Entry stored in the pinned map, alongside metadata for timeout
/// enforcement and size accounting.
#[derive(Debug)]
struct PinnedEntry<T> {
    data: T,
    pinned_at: Instant,
    size: u64,
    /// FL-681 Fix A: when `true`, this pin is held UNTIL the server's
    /// BlobsInStableStorage ack (`unpin_key`) and is EXEMPT from the
    /// `PIN_TIMEOUT_SECS` sweep in [`MokaEvictingMap::expire_stale_pins`].
    /// Worker-local F2 deferred-output blobs set this so their
    /// anti-eviction pin is released ONLY by BIS-ack — never by the 120s
    /// TTL — mirroring how in-memory mirror blobs are pinned indefinitely
    /// (the mirror-TTL sweeper was removed in `local_worker.rs` for the
    /// same data-loss reason). A normal (time-bounded) pin sets this
    /// `false` and keeps the TTL backstop.
    indefinite: bool,
}

/// An eviction event captured by the moka listener and sent to the
/// background drainer for async cleanup (unref + callbacks).
struct EvictionEvent<K, T> {
    key: Arc<K>,
    value: T,
    /// (#locality-map-drift) The EVICTED value's FROZEN logical-LWW counter,
    /// read off `value.stamp()` at the moment of eviction — NOT a fresh mint.
    /// Carried to the removal `callback` so the holdings eviction delta reports
    /// the counter the evicted value was inserted with; this is what guarantees
    /// `ts_evict(V) < ts_reinsert` even when the async callback is delivered
    /// out of order relative to a re-insert.
    ts_counter: u64,
}

/// Result of [`MokaEvictingMap::evict_unpinned_lru_bytes`].
///
/// `evicted_bytes` may be less than the requested target if too few
/// unpinned entries remain in the cache (e.g. all entries are pinned
/// during a heavy BIS-ack window). Callers (notably
/// `MemoryStore::check_backpressure_gate`) use the report for
/// observability — the eviction-then-recheck loop in production
/// re-checks `would_exceed_capacity` directly rather than trusting this
/// number to determine whether to admit the new write.
///
/// `iter_scanned` is the count of entries the iter loop visited
/// (post-#334 bundle fixup #8b — perf MAJOR). When this approaches
/// `EVICT_SCAN_HARD_CAP` (10K) the loop short-circuits to bound the
/// O(N)-walk worst case under heavy backpressure churn; emit
/// `iter_scanned` into tracing so a future operator can spot the
/// truncation if it ever fires (currently not expected at production
/// scales — the cas_FAST_SLOW MemoryStore caps at 1M entries).
#[derive(Debug, Default, Clone, Copy)]
pub struct EvictedReport {
    pub evicted_count: u64,
    pub evicted_bytes: u64,
    pub iter_scanned: u64,
    pub iter_truncated: bool,
}

/// Hard cap on entries `evict_unpinned_lru_bytes` may scan in a single
/// call. Bounds the worst-case O(N) walk so the call latency stays
/// predictable under sustained backpressure churn (the production
/// cas_FAST_SLOW MemoryStore is capped at 1M entries; without this
/// cap a bursty all-pinned scenario could iterate the full 1M before
/// returning empty-handed). Truncation is observability-only — the
/// caller's eviction-then-recheck loop in `check_backpressure_gate`
/// still emits the typed signal correctly when no room is freed.
const EVICT_SCAN_HARD_CAP: u64 = 10_000;

/// A cache backed by `moka::sync::Cache` with an API that mirrors
/// the previous LRU-based `EvictingMap`. Moka is configured with the
/// pure LRU eviction policy (no TinyLFU admission filter) so that
/// every insert is admitted unconditionally — required for a CAS where
/// silently dropping freshly-written blobs is unacceptable. Pinning is
/// handled via a side `DashMap` that keeps entries alive outside the
/// moka cache.
pub struct MokaEvictingMap<
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper,
    C: ItemCallback<Q> = NoopCallback,
> {
    cache: Cache<K, T>,
    /// Items pinned to prevent eviction. Shared with the eviction
    /// listener so it can check pin status before sending cleanup events.
    pinned: Arc<DashMap<K, PinnedEntry<T>>>,
    /// Total bytes currently pinned.
    pinned_bytes: AtomicU64,
    /// FL-681 Fix A: subset of `pinned_bytes` held by INDEFINITE
    /// (pinned-until-BIS-ack) pins. Tracked separately so the indefinite
    /// cap can be enforced without coupling to the time-bounded pin set.
    /// `indefinite_pinned_bytes <= pinned_bytes` always.
    indefinite_pinned_bytes: AtomicU64,
    /// 25% of max_bytes — ceiling for pinned data.
    pin_cap: u64,
    // CAPPED AT indefinite_pin_cap: FL-681 Fix A. An indefinite pin holds
    // a worker-local F2 output blob un-evictable until BIS-ack; under a
    // sustained server/BIS outage the set of pending-BIS pinned blobs
    // would otherwise grow without bound (no TTL releases it). This is the
    // measured cap on `indefinite_pinned_bytes`. Over-cap behavior is
    // BACKPRESSURE, never drop: `pin_key_indefinite` REFUSES (returns
    // `false`); the caller leaves the blob as a normal TTL-evictable pin
    // and retries the durability upload, so the source stays readable and
    // no blob is lost. Defaults to `pin_cap` (indefinite pins can never
    // exceed the total pin budget); `0` means "fall back to pin_cap".
    // Mirrors the `FastSlowStore::slow_writes_in_flight_max_bytes` cap +
    // typed-backpressure pattern.
    indefinite_pin_cap: u64,
    /// Optional BTreeSet index for range queries. Shared with the
    /// eviction listener for cleanup on eviction.
    btree: Arc<RwLock<Option<BTreeSet<K>>>>,
    /// Side queue of eviction events produced by the moka listener.
    /// Drained synchronously by `insert()` so that the insert's evicted
    /// items are `unref()`d before the caller continues (matching the
    /// original `EvictingMap` contract). Also drained by the background
    /// task for evictions triggered outside an `insert()` call (e.g. TTL
    /// expiry, explicit `remove()`). Each event is taken at most once.
    pending_evictions: Arc<parking_lot::Mutex<VecDeque<EvictionEvent<K, T>>>>,
    /// Wake signal for the background drainer. Listener sends `()` after
    /// pushing an event to `pending_evictions`. The actual event payload
    /// lives in `pending_evictions`, not the channel — a `()` on the
    /// channel means "there may be work to do in the side queue".
    eviction_tx: mpsc::UnboundedSender<()>,
    /// Receiver held until `start_background_eviction` moves it into
    /// the drainer task.
    eviction_rx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<()>>>,
    /// Callbacks to invoke on item removal.
    callbacks: RwLock<Vec<C>>,
    /// Fast-path flag to avoid taking the `callbacks` RwLock on hot
    /// read paths (`get`, `get_many`) when no callbacks are registered.
    /// Set to `true` (with `Release` ordering) by `add_item_callback`
    /// after the callback is appended; loaded with `Relaxed` on the
    /// hot path. Once set to true it is never cleared (callbacks are
    /// append-only on this type).
    has_callbacks_flag: AtomicBool,
    /// Anchor time for timestamp conversion.
    anchor_time: I,
    /// Configured max_bytes (used for pin cap and diagnostics).
    max_bytes: u64,
    /// Configured max_count (enforced alongside max_bytes if both set).
    max_count: u64,
    /// Whether the background drainer has been started.
    background_running: AtomicBool,
    /// (FL-688 v3 Stage C) Worker-startup reconcile gate. When `false`,
    /// the background `drain_interval` tick that forces moka's capacity
    /// check (`run_pending_tasks_and_drain`) is SUPPRESSED. Set to `false`
    /// at construction time for worker stores (via `startup_reconcile_gate:
    /// true` in FilesystemSpec → `FilesystemStore::set_startup_reconcile_gate`)
    /// and flipped to `true` by the server's `ReconcileCompleteRequest`
    /// signal (via `FilesystemStore::release_startup_reconcile_gate`), after
    /// which normal periodic eviction resumes. Default: `true` (gate off — no
    /// change for server-side FilesystemStores that never set it false).
    ///
    /// SAFETY: Only the background `drain_evictions` task reads this to
    /// gate the drain tick. The writer (`release_startup_reconcile_gate`)
    /// uses `Release` ordering; the reader uses `Acquire`. This is the
    /// SECONDARY protection (blocks the explicit LRU drain). The PRIMARY
    /// protection is the reconcile-pin itself (`pin_digest_indefinite_with_result`),
    /// which protects individual blobs from PER-INSERT moka eviction that
    /// this gate cannot block.
    reconcile_complete: Arc<AtomicBool>,
    /// (#locality-map-drift) Per-map monotonic logical clock. `fetch_add`-ed
    /// once per insert and FROZEN into the inserted value (`set_stamp`) so the
    /// counter travels with the value through moka's cache slot. A distinct
    /// value therefore gets a distinct, strictly-increasing counter; an
    /// eviction reads the evicted value's frozen counter (not a fresh tick), so
    /// `ts_evict(V) < ts_reinsert` holds under reordered async callback
    /// delivery. Only meaningful for the worker `FilesystemStore` map (whose
    /// `BlobChangeTracker` consumes it); other maps tick it harmlessly and
    /// their callbacks ignore it. `Relaxed` is sufficient: uniqueness +
    /// monotonicity of the returned value is all the LWW needs (the value↔
    /// counter binding is established by `set_stamp` before `cache.insert`).
    clock: AtomicU64,
    /// (#locality-map-drift) This worker process's `boot_epoch_id`
    /// (`worker_utils.rs`), the HIGH word of the logical LWW ts. A restarted
    /// worker's fresh boot_epoch dominates any stale counter from a prior
    /// process (lexicographic `(boot_epoch, counter)`), so its correct
    /// low-counter holdings re-register instead of wedging ABSENT. `0` for maps
    /// that don't participate (server-side stores).
    ///
    /// `AtomicU64` (not a plain `u64`) so the worker can stamp it via
    /// [`Self::set_boot_epoch`] AFTER construction — `FilesystemStore::new`
    /// runs in `nativelink-store`, which cannot call the worker's
    /// `boot_epoch_id()`; the worker sets it at boot, before any insert, in the
    /// same spot it arms `set_startup_reconcile_gate`. `Relaxed` throughout:
    /// set-once before the first insert on the single-threaded boot path, so no
    /// ordering against the reads is needed.
    boot_epoch: AtomicU64,
    // Metrics
    evicted_bytes: Counter,
    evicted_items: CounterWithTime,
    replaced_bytes: Counter,
    replaced_items: CounterWithTime,
    lifetime_inserted_bytes: Counter,
    /// Phantom for the Q type parameter.
    _q: core::marker::PhantomData<Q>,
}

impl<K, Q, T, I, C> Debug for MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper + Debug,
    C: ItemCallback<Q>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MokaEvictingMap")
            .field("entry_count", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .field(
                "pinned_bytes",
                &self.pinned_bytes.load(Ordering::Relaxed),
            )
            .field("pin_cap", &self.pin_cap)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

/// Hand-rolled `MetricsComponent` impl: `MokaEvictingMap`'s observable
/// state is split between live moka-cache accessors (`entry_count`,
/// `weighted_size`) and owned atomic / counter fields (`pinned_bytes`,
/// `pin_cap`, `max_bytes`, `max_count`, plus the lifetime counters).
/// The derive macro can only emit named struct fields, so it cannot
/// reach the cache accessors — hence the manual impl.
///
/// Required fields (per #380 / red-team #160 RECONSIDER 2026-05-11):
/// - `pinned_bytes`  — load-bearing for #332 falsifiability (pin-cap
///   headroom is meaningful only if the operator can scrape the live
///   pinned-byte usage).
/// - `pin_cap`       — companion to `pinned_bytes`; the ceiling.
/// - `entry_count`   — live moka entry count.
/// - `weighted_size` — KB-WEIGHT sum of resident entries (weigher rounds
///   each value up to 1 KB; does NOT include pinned). NOT bytes.
/// - `weighted_size_bytes` — byte-accurate view (`weighted_size × 1024`);
///   use this for byte-vs-disk comparisons.
/// - `evicted_bytes`, `evicted_items`, `replaced_bytes`,
///   `replaced_items`, `lifetime_inserted_bytes` — existing counters
///   already maintained by the eviction listener / insert paths.
///
/// Naming note: every field is published under the leaf name shown
/// here. The parent group (e.g. `evicting_map` from `MemoryStore`'s
/// derive) is already on the span stack at call time, so leaves do
/// NOT enter another `group!(field_metadata.name)` — that would
/// double-namespace the resulting Prometheus metric (e.g.
/// `..evicting_map_evicting_map_pinned_bytes`).
impl<K, Q, T, I, C> MetricsComponent for MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper,
    C: ItemCallback<Q>,
{
    fn publish(
        &self,
        _kind: nativelink_metric::MetricKind,
        _field_metadata: nativelink_metric::MetricFieldData,
    ) -> Result<nativelink_metric::MetricPublishKnownKindData, nativelink_metric::Error> {
        // Live cache accessors — gauges, not counters.
        let entry_count: u64 = self.cache.entry_count();
        // `weighted_size()` is the sum of moka WEIGHTS, and the weigher in
        // `with_anchor` emits weights in KB-units (`value.len().div_ceil(1024)`).
        // So `weighted_size` is a KB-WEIGHT count, NOT a byte count.
        // `weighted_size_bytes` below is the byte-accurate view (× the same
        // 1024 the weigher and `would_exceed_capacity` use), so an operator
        // can compare it to on-disk bytes directly without a 1024× misread.
        // SCALE is kept inline (not exposed publicly), mirroring the same
        // choice at the `would_exceed_capacity` call-site.
        const WEIGHER_SCALE: u64 = 1024;
        let weighted_size: u64 = self.cache.weighted_size();
        let weighted_size_bytes: u64 = weighted_size.saturating_mul(WEIGHER_SCALE);
        // Atomic gauge — current pinned bytes (admission/eviction/pin
        // composite invariant: `pinned_bytes <= pin_cap`).
        let pinned_bytes: u64 = self.pinned_bytes.load(Ordering::Relaxed);
        let pinned_count: u64 = self.pinned.len() as u64;
        // FL-681 Fix A: indefinite (pinned-until-BIS-ack) subset gauge.
        let indefinite_pinned_bytes: u64 =
            self.indefinite_pinned_bytes.load(Ordering::Relaxed);

        // Pinned-bytes gauge — load-bearing for #332 prophylactic pin-cap
        // headroom falsifiability. If this exceeds `pin_cap` in
        // production, #332's reclaim is insufficient and a follow-up
        // tracker is required.
        nativelink_metric::publish!(
            "pinned_bytes",
            &pinned_bytes,
            nativelink_metric::MetricKind::Default,
            "Bytes currently pinned (un-evictable) in this MokaEvictingMap. Composite invariant: must stay <= pin_cap; #332 / #380."
        );
        nativelink_metric::publish!(
            "pin_cap",
            &self.pin_cap,
            nativelink_metric::MetricKind::Default,
            "Configured pin cap = max_bytes * 25% (PIN_CAP_FRACTION). Ceiling for pinned_bytes; admission rejects new pins above this."
        );
        nativelink_metric::publish!(
            "pinned_count",
            &pinned_count,
            nativelink_metric::MetricKind::Default,
            "Number of currently-pinned entries (companion to pinned_bytes)."
        );
        nativelink_metric::publish!(
            "indefinite_pinned_bytes",
            &indefinite_pinned_bytes,
            nativelink_metric::MetricKind::Default,
            "FL-681 Fix A: bytes held by INDEFINITE (pinned-until-BIS-ack) pins — worker-local F2 deferred-output blobs exempt from the 120s TTL sweep. Subset of pinned_bytes; capped by indefinite_pin_cap with backpressure over-cap. A sustained rise tracks pending-BIS durability backlog under a server/BIS outage."
        );
        nativelink_metric::publish!(
            "indefinite_pin_cap",
            &self.indefinite_pin_cap,
            nativelink_metric::MetricKind::Default,
            "FL-681 Fix A: configured cap on indefinite_pinned_bytes. Over-cap behavior is backpressure (pin_key_indefinite refuses; caller retries — never drops). Defaults to pin_cap when configured 0."
        );
        nativelink_metric::publish!(
            "entry_count",
            &entry_count,
            nativelink_metric::MetricKind::Default,
            "Live entry count in the moka cache (does not include pinned-only entries)."
        );
        nativelink_metric::publish!(
            "weighted_size",
            &weighted_size,
            nativelink_metric::MetricKind::Default,
            "Live weighted size of moka cache entries in KB-WEIGHT units (NOT bytes): the weigher rounds each value up to 1 KB (value.len().div_ceil(1024)) and this is the sum of those weights. For a byte count, use weighted_size_bytes (= this × 1024). Pinned-only bytes accounted separately under pinned_bytes."
        );
        nativelink_metric::publish!(
            "weighted_size_bytes",
            &weighted_size_bytes,
            nativelink_metric::MetricKind::Default,
            "Live weighted size (bytes) of moka cache entries: weighted_size × 1024 (the weigher's KB scale, matching would_exceed_capacity). Use this — not weighted_size — for byte-vs-disk comparisons; reading weighted_size as bytes under-counts by 1024×. Pinned-only bytes accounted separately under pinned_bytes."
        );
        nativelink_metric::publish!(
            "max_bytes",
            &self.max_bytes,
            nativelink_metric::MetricKind::Default,
            "Configured max_bytes (capacity ceiling for the moka cache)."
        );
        nativelink_metric::publish!(
            "max_count",
            &self.max_count,
            nativelink_metric::MetricKind::Default,
            "Configured max_count (entry-count ceiling for the moka cache; 0 = unlimited)."
        );

        // Lifetime counters — monotone, suitable for Counter kind.
        nativelink_metric::publish!(
            "evicted_bytes",
            &self.evicted_bytes,
            nativelink_metric::MetricKind::Counter,
            "Cumulative bytes evicted from this MokaEvictingMap by LRU / TTL / explicit remove."
        );
        nativelink_metric::publish!(
            "evicted_items",
            &self.evicted_items,
            nativelink_metric::MetricKind::Component,
            "Cumulative entries evicted from this MokaEvictingMap (CounterWithTime — emits .counter and .last_time)."
        );
        nativelink_metric::publish!(
            "replaced_bytes",
            &self.replaced_bytes,
            nativelink_metric::MetricKind::Counter,
            "Cumulative bytes replaced (existing key overwritten) in this MokaEvictingMap."
        );
        nativelink_metric::publish!(
            "replaced_items",
            &self.replaced_items,
            nativelink_metric::MetricKind::Component,
            "Cumulative entries replaced in this MokaEvictingMap (CounterWithTime — emits .counter and .last_time)."
        );
        nativelink_metric::publish!(
            "lifetime_inserted_bytes",
            &self.lifetime_inserted_bytes,
            nativelink_metric::MetricKind::Counter,
            "Cumulative bytes inserted into this MokaEvictingMap over the process lifetime."
        );

        Ok(nativelink_metric::MetricPublishKnownKindData::Component)
    }
}

impl<K, Q, T, I, C> MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Ord + Hash + Eq + Debug + Send + Sync + 'static,
    T: LenEntry + Debug + Clone + Send + Sync + 'static,
    I: InstantWrapper,
    C: ItemCallback<Q> + Clone + 'static,
{
    pub fn new(config: &EvictionPolicy) -> Self
    where
        I: Default,
    {
        Self::with_anchor(config, I::default())
    }

    pub fn with_anchor(config: &EvictionPolicy, anchor_time: I) -> Self {
        // Default the indefinite-pin cap to `pin_cap` (25% of max_bytes):
        // indefinite pins are a subset of all pins and can never exceed
        // the total pin budget. `0` is interpreted below as "use pin_cap".
        // boot_epoch 0: default (no holdings LWW participation).
        Self::with_anchor_indefinite_cap_boot_epoch(config, anchor_time, 0, 0)
    }

    /// (#locality-map-drift) Constructor variant that stamps the map's
    /// per-mutation logical LWW ts with the worker's `boot_epoch_id`. Used by
    /// the worker `FilesystemStore` map so its holdings deltas carry
    /// `(boot_epoch, counter)` and a restarted worker's fresh epoch dominates
    /// stale server state. Server-side stores keep boot_epoch 0 via the other
    /// constructors (they never emit holdings deltas).
    pub fn with_anchor_and_boot_epoch(
        config: &EvictionPolicy,
        anchor_time: I,
        boot_epoch: u64,
    ) -> Self {
        Self::with_anchor_indefinite_cap_boot_epoch(config, anchor_time, 0, boot_epoch)
    }

    /// FL-681 Fix A constructor variant: same as [`Self::with_anchor`]
    /// but with an explicit cap on INDEFINITE (pinned-until-BIS-ack)
    /// bytes. `indefinite_pin_cap_bytes == 0` falls back to the normal
    /// `pin_cap` (25% of `max_bytes`). The cap is enforced at
    /// [`Self::pin_key_indefinite`] with BACKPRESSURE (refuse, never drop)
    /// over-cap semantics.
    pub fn with_anchor_and_indefinite_cap(
        config: &EvictionPolicy,
        anchor_time: I,
        indefinite_pin_cap_bytes: u64,
    ) -> Self {
        Self::with_anchor_indefinite_cap_boot_epoch(
            config,
            anchor_time,
            indefinite_pin_cap_bytes,
            0,
        )
    }

    /// (#locality-map-drift) Core constructor: `with_anchor_and_indefinite_cap`
    /// plus the per-mutation logical-LWW `boot_epoch`. All public constructors
    /// funnel here; server-side stores pass `boot_epoch = 0`.
    pub fn with_anchor_indefinite_cap_boot_epoch(
        config: &EvictionPolicy,
        anchor_time: I,
        indefinite_pin_cap_bytes: u64,
        boot_epoch: u64,
    ) -> Self {
        let max_bytes = config.max_bytes as u64;
        let max_count = config.max_count;
        let max_seconds = config.max_seconds;
        let evict_bytes = config.evict_bytes as u64;

        let (eviction_tx, eviction_rx) = mpsc::unbounded_channel::<()>();
        let listener_tx = eviction_tx.clone();

        // Shared state captured by the eviction listener closure.
        let pinned: Arc<DashMap<K, PinnedEntry<T>>> = Arc::new(DashMap::new());
        let listener_pinned = Arc::clone(&pinned);
        let btree: Arc<RwLock<Option<BTreeSet<K>>>> = Arc::new(RwLock::new(None));
        let listener_btree = Arc::clone(&btree);
        let pending_evictions: Arc<parking_lot::Mutex<VecDeque<EvictionEvent<K, T>>>> =
            Arc::new(parking_lot::Mutex::new(VecDeque::new()));
        let listener_pending = Arc::clone(&pending_evictions);

        let mut builder = Cache::builder();

        // LRU eviction policy. We deliberately do NOT use moka's default
        // TinyLFU admission filter — TinyLFU compares the candidate's
        // frequency estimate against the victim's and REJECTS the new
        // entry on a tie. For a content-addressed store where every
        // upload must be cached, that silently drops freshly-written
        // blobs:
        //
        //   1. update_oneshot writes the new blob to a temp file.
        //   2. emplace_file calls evicting_map.insert(new_key, new_arc).
        //   3. moka's TinyLFU sees both old and new entries with similar
        //      frequency estimates and EVICTS THE NEW ENTRY (cause=Size).
        //   4. emplace_file's still_ours check fails → returns Ok without
        //      renaming the temp file into the content path.
        //   5. The temp file is cleaned up by Drop. The new blob is gone,
        //      yet update_oneshot returned Ok. A subsequent get() on the
        //      same digest returns NotFound — silent data loss.
        //
        // LRU has no admission filter: a new insert always succeeds and
        // displaces the least-recently-used entry. That matches the
        // contract of the previous (parking_lot LRU) EvictingMap and is
        // the only safe behavior for a CAS slow-tier.
        builder = builder.eviction_policy(moka::policy::EvictionPolicy::lru());

        // Capacity: use max_bytes with low-watermark from evict_bytes.
        // Setting capacity to (max_bytes - evict_bytes) ensures moka
        // keeps headroom, similar to the old evict_bytes behavior.
        if max_bytes > 0 {
            // Moka's weigher returns u32 but we track bytes as u64.
            // Scale capacity and weights to KB granularity so items up
            // to 4TB fit in u32. A 1-byte item weighs 1 (minimum).
            //
            // Floor the effective capacity at 1: a 0-capacity cache
            // immediately evicts every insert, which silently loses
            // just-written blobs. This matters for tests that configure
            // very small max_bytes (e.g. 5) and for production corner
            // cases where `evict_bytes >= max_bytes`.
            const SCALE: u64 = 1024;
            let effective_capacity =
                (max_bytes.saturating_sub(evict_bytes) / SCALE).max(1);
            builder = builder
                .max_capacity(effective_capacity)
                .weigher(|_key: &K, value: &T| -> u32 {
                    let kb = value.len().div_ceil(SCALE);
                    u32::try_from(kb).unwrap_or(u32::MAX)
                });
        } else if max_count > 0 {
            builder = builder.max_capacity(max_count);
        }

        if max_seconds > 0 {
            builder = builder.time_to_idle(Duration::from_secs(u64::from(max_seconds)));
        }

        // Eviction listener: fires synchronously during moka operations.
        // - Replaced: skip — insert() handles replaced-item unref directly.
        // - Size/Expired/Explicit: check if pinned (skip if so, it's safe
        //   in the DashMap). Otherwise send to background drainer.
        builder = builder.eviction_listener(move |key: Arc<K>, value: T, cause: RemovalCause| {
            if cause == RemovalCause::Replaced {
                // insert() captured the old value via cache.get() and
                // will await its unref() before returning. Don't double-unref.
                return;
            }

            // If this key is pinned, the pin_key() flow already moved it
            // to the DashMap. The invalidate() triggered this listener but
            // the data is safe in the pinned map. Skip cleanup.
            let q: &Q = (*key).borrow();
            if listener_pinned.contains_key(q) {
                return;
            }

            // Clean up BTree index on eviction.
            {
                let btree_guard = listener_btree.read();
                if btree_guard.is_some() {
                    drop(btree_guard);
                    let mut btree_guard = listener_btree.write();
                    if let Some(ref mut set) = *btree_guard {
                        set.remove(q);
                    }
                }
            }

            // Push the event onto the sync side queue, then wake the
            // background drainer via a `()` signal. `insert()` drains the
            // same queue synchronously after `run_pending_tasks()` so the
            // evicted item's `unref()` completes before the caller returns
            // — this is the contract the original `EvictingMap` exposed.
            // (#locality-map-drift) Freeze the EVICTED value's logical-LWW
            // counter NOW (before `value` is moved) so the removal callback
            // carries the counter this value was inserted with — never a fresh
            // mint. This is the load-bearing difference from the flawed
            // "mint-at-eviction" (TLC Model A) design.
            let ts_counter = value.stamp();
            listener_pending.lock().push_back(EvictionEvent {
                key: Arc::clone(&key),
                value,
                ts_counter,
            });
            // Unbounded channel never blocks — send only fails if the
            // receiver is dropped (shutdown).
            let _ = listener_tx.send(());
        });

        let cache = builder.build();
        let pin_cap = (max_bytes as f64 * PIN_CAP_FRACTION) as u64;
        // FL-681 Fix A: an explicit `0` indefinite cap means "use the
        // total pin_cap" so indefinite pins can never exceed the overall
        // pin budget; a non-zero value is the operator-tuned cap.
        let indefinite_pin_cap = if indefinite_pin_cap_bytes == 0 {
            pin_cap
        } else {
            indefinite_pin_cap_bytes
        };

        Self {
            cache,
            pinned,
            pinned_bytes: AtomicU64::new(0),
            indefinite_pinned_bytes: AtomicU64::new(0),
            pin_cap,
            indefinite_pin_cap,
            btree,
            pending_evictions,
            eviction_tx,
            eviction_rx: parking_lot::Mutex::new(Some(eviction_rx)),
            callbacks: RwLock::new(Vec::new()),
            has_callbacks_flag: AtomicBool::new(false),
            anchor_time,
            max_bytes,
            max_count,
            background_running: AtomicBool::new(false),
            // Default `true`: gate is OFF by default. Server-side stores
            // never gate reconcile; only worker stores arm it (at construction
            // time via `startup_reconcile_gate: true` in FilesystemSpec →
            // `set_startup_reconcile_gate()` in `FilesystemStore::new`).
            reconcile_complete: Arc::new(AtomicBool::new(true)),
            // (#locality-map-drift) Start at 1 so the first real value's
            // counter is > 0 (0 is the proto default / "unset/legacy", treated
            // as oldest by the server gate); a genuine stamped value must never
            // collide with the unset sentinel.
            clock: AtomicU64::new(1),
            boot_epoch: AtomicU64::new(boot_epoch),
            evicted_bytes: Counter::default(),
            evicted_items: CounterWithTime::default(),
            replaced_bytes: Counter::default(),
            replaced_items: CounterWithTime::default(),
            lifetime_inserted_bytes: Counter::default(),
            _q: core::marker::PhantomData,
        }
    }

    /// Fast-path check: returns true if any items are pinned.
    #[inline]
    fn has_pinned(&self) -> bool {
        self.pinned_bytes.load(Ordering::Relaxed) > 0
    }

    // ---------------------------------------------------------------
    // get
    // ---------------------------------------------------------------

    pub async fn get(&self, key: &Q) -> Option<T> {
        // Atomic fast-path: skip DashMap probe when nothing is pinned.
        // Pinned-hit reads do NOT fire `on_get` — pinned entries are a
        // worker-side staging concept (a blob in flight to a sandbox)
        // and the locality_map already knows about pins.
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                return Some(entry.data.clone());
            }
        }
        let result = self.cache.get(key);
        if result.is_some() {
            self.fire_on_get(key);
        }
        result
    }

    /// (#locality-map-drift) Like [`Self::get`] but does NOT fire the `on_get`
    /// callback. For INTERNAL, non-logical lookups (e.g. `FilesystemStore`'s
    /// post-emplace `still_ours` ptr-eq verification) that must NOT emit a
    /// "recently read" heat signal.
    ///
    /// Why this matters for the holdings LWW: `on_get` mints a FRESH logical-LWW
    /// counter (so a genuine materialization read can supersede a stale
    /// out-of-order evict — the false-missing fix). But that fresh counter is
    /// HIGHER than the value's own frozen insert-counter, so if a spurious read
    /// fires between a value's insert and its GENUINE eviction, the eviction
    /// (carrying the value's lower insert-counter) LOSES the LWW and the blob is
    /// reported PRESENT while gone — a systematic false-POSITIVE on EVERY
    /// evicted-after-write blob. The `still_ours` check runs on exactly that
    /// insert→evict seam, so it must use this non-signalling lookup. Genuine
    /// materialization reads (`get`) still fire `on_get` (the design accepts the
    /// resulting transient, force_evict-healed false-positive for real reads).
    ///
    /// Still promotes the moka LRU (like `get`) — a just-written blob staying
    /// warm is desirable and independent of the holdings signal.
    pub async fn get_no_touch(&self, key: &Q) -> Option<T> {
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                return Some(entry.data.clone());
            }
        }
        self.cache.get(key)
    }

    /// Retrieve multiple values by key. Sequential iteration is intentional:
    /// Moka's `cache.get()` is synchronous (lock-free concurrent hash map),
    /// so 500 lookups complete in ~50us. Parallelism via `spawn_blocking` or
    /// `par_iter` would add more overhead than it saves.
    pub async fn get_many<'b, Iter>(&self, keys: Iter) -> Vec<Option<T>>
    where
        Iter: IntoIterator<Item = &'b Q>,
        Q: 'b,
    {
        let check_pinned = self.has_pinned();
        keys.into_iter()
            .map(|key| {
                // Pinned-hit reads do NOT fire `on_get` (see `get`).
                if check_pinned {
                    if let Some(entry) = self.pinned.get(key) {
                        return Some(entry.data.clone());
                    }
                }
                let result = self.cache.get(key);
                if result.is_some() {
                    self.fire_on_get(key);
                }
                result
            })
            .collect()
    }

    // ---------------------------------------------------------------
    // insert
    // ---------------------------------------------------------------

    pub async fn insert(&self, key: K, data: T) -> Option<T>
    where
        K: 'static,
    {
        let old = self.insert_inner(key, data);
        // Await unref on replaced item before returning. This preserves
        // the invariant that the old file is cleaned up before the caller
        // renames the new file into the content path.
        if let Some(ref value) = old {
            value.unref().await;
        }
        // Drain any eviction events this insert triggered and unref them
        // synchronously (before returning to the caller). The original
        // LRU `EvictingMap` unref'd evicted items inside `insert`, and
        // code / tests in FilesystemStore depend on that ordering
        // (e.g. `on_unref` hooks observed immediately after insert).
        self.drain_pending_evictions().await;
        old
    }

    /// Drain `pending_evictions` inline and fire `process_eviction_event`
    /// on each. Concurrency: the queue is a `parking_lot::Mutex` so we
    /// take items one at a time (not a bulk drain) to avoid holding the
    /// lock across awaits.
    async fn drain_pending_evictions(&self) {
        loop {
            let event = self.pending_evictions.lock().pop_front();
            match event {
                Some(ev) => self.process_eviction_event(ev).await,
                None => break,
            }
        }
    }

    /// Honor `insert_startup`'s post-batch-drain contract (see its
    /// doc-comment: "Caller should call cache.run_pending_tasks() after
    /// the full batch"). After a batch of `insert_with_time` (the
    /// startup load path), the caller must kick moka's capacity check +
    /// drain the eviction queue. Without this, on-disk content loaded
    /// above the cap stays above the cap until the first runtime
    /// `insert` — which on idle workers may never come.
    ///
    /// Loops `cache.run_pending_tasks()` + `drain_pending_evictions()`
    /// until idle. moka's per-call eviction is BOUNDED in principle (per
    /// moka's `DEFAULT_MAINTENANCE_TASK_TIMEOUT_MILLIS` ≈ 100 ms +
    /// `DEFAULT_EVICTION_BATCH_SIZE` ≈ 384), so a single call CAN return
    /// with `more_to_evict=true` and leave the cache above cap. In
    /// practice moka's internal `do_run_pending_tasks` re-loops until
    /// drained and reaches the time bound only on very large overshoots
    /// (~tens of GiB at production file-size granularity). The outer
    /// loop here is forward-defense for that edge case: at any
    /// production scale the post-call invariant
    /// `cache.weighted_size() ≤ max_capacity` actually holds. Bounded by
    /// `MAX_ITERATIONS` to defend against pathological cycles.
    ///
    /// Each iteration mirrors the post-`cache.insert()` enforcement
    /// block in `insert_inner`: one `run_pending_tasks()`, a `max_count`
    /// re-check that may fire a second one, then drain. (Anchors are
    /// by symbol to avoid line-cite drift across edits — this is the
    /// 3rd round of fix-ups where numeric cites have drifted.)
    pub async fn run_pending_tasks_and_drain(&self) {
        // 1000 iterations × ≤ 384 evictions/iter = 384 000 evictions
        // before warn-and-bail. Far above any plausible single-startup
        // overshoot; large enough that hitting the cap signals a bug.
        const MAX_ITERATIONS: u32 = 1000;
        for _ in 0..MAX_ITERATIONS {
            self.cache.run_pending_tasks();
            if self.max_count > 0
                && self.max_bytes > 0
                && self.cache.entry_count() > self.max_count
            {
                self.cache.run_pending_tasks();
            }
            // If moka produced no new eviction events this iteration, it
            // considers itself drained — done.
            if self.pending_evictions.lock().is_empty() {
                return;
            }
            self.drain_pending_evictions().await;
        }
        tracing::warn!(
            "run_pending_tasks_and_drain hit iteration cap ({}); cache may still be above capacity",
            MAX_ITERATIONS,
        );
    }

    pub async fn insert_with_time(
        &self,
        key: K,
        data: T,
        _seconds_since_anchor: i32,
    ) -> Option<T> {
        // Startup path: files are inserted oldest-first (sorted by atime).
        //
        // The `seconds_since_anchor` parameter is intentionally ignored.
        // Under the LRU policy moka pushes new entries to the MRU end of
        // the deque in insertion order, so the oldest-inserted entries
        // (i.e. files with the oldest atime) sit at the LRU position and
        // are evicted first under size pressure — preserving atime
        // ordering without needing a custom expiration policy.
        let old = self.insert_startup(key, data);
        if let Some(ref value) = old {
            value.unref().await;
        }
        old
    }

    fn insert_inner(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // Update BTree index.
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        // If key is pinned, replace in pinned map directly.
        if self.has_pinned() && self.pinned.contains_key(key.borrow()) {
            // FL-681 Fix A: preserve the existing pin's `indefinite` flag
            // across a re-insert of the same key. A fresh write of a digest
            // that is currently pinned-until-BIS-ack MUST NOT silently
            // demote it to time-bounded (that would re-open the TTL leak).
            let mut was_indefinite = false;
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                if entry.indefinite {
                    was_indefinite = true;
                    self.indefinite_pinned_bytes
                        .fetch_sub(entry.size, Ordering::Relaxed);
                }
                entry.data
            });
            // (#locality-map-drift) A pinned re-insert is a fresh PRESENT
            // mutation: mint + freeze a new counter into `data` so the
            // re-advertise carries a strictly-higher ts than any prior evict
            // of this key.
            let ts_counter = self.next_stamp();
            data.set_stamp(ts_counter);
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: data.clone(),
                    pinned_at: Instant::now(),
                    size,
                    indefinite: was_indefinite,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            if was_indefinite {
                self.indefinite_pinned_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            self.fire_on_insert_callbacks(&key, size, ts_counter);
            if old.is_some() {
                self.replaced_bytes.add(size);
                self.replaced_items.inc();
            }
            return old;
        }

        // Capture old value before insert for replaced-item unref.
        // The eviction listener skips Replaced events since we handle
        // cleanup here.
        // (#locality-map-drift) Mint + FREEZE the value's logical-LWW counter
        // BEFORE `cache.insert` moves `data`, so the counter travels with THIS
        // value through moka's cache slot and is read back off the evicted
        // value later (value-carried ts, never re-minted at eviction).
        let ts_counter = self.next_stamp();
        data.set_stamp(ts_counter);
        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // Process pending tasks so any size-driven eviction triggered by
        // this insert fires its listener before we return. The caller
        // (emplace_file) relies on the eviction event being queued before
        // its `still_ours` check.
        self.cache.run_pending_tasks();

        // Enforce max_count if both max_bytes and max_count are set.
        if self.max_count > 0
            && self.max_bytes > 0
            && self.cache.entry_count() > self.max_count
        {
            // run_pending_tasks again to trigger any additional eviction.
            self.cache.run_pending_tasks();
        }

        self.fire_on_insert_callbacks(&key, size, ts_counter);
        if existing.is_some() {
            self.replaced_bytes.add(size);
            self.replaced_items.inc();
        }
        existing
    }

    /// Startup-optimized insert: no frequency bump, no per-insert
    /// run_pending_tasks(). Caller should call cache.run_pending_tasks()
    /// after the full batch. Items enter at freq=0 in the frequency
    /// sketch and are pushed to MainProbation in insertion order when
    /// WriteOps are processed, so oldest-inserted entries sit at the
    /// front (LRU position) and are evicted first during size pressure.
    fn insert_startup(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // BTree update (if enabled).
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        // (#locality-map-drift) Freeze the value's logical-LWW counter before
        // `cache.insert` moves `data` (see `insert_inner`).
        let ts_counter = self.next_stamp();
        data.set_stamp(ts_counter);
        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // No frequency bump (no extra get()).
        // No run_pending_tasks() — deferred to caller.
        self.fire_on_insert_callbacks(&key, size, ts_counter);
        existing
    }

    /// (#locality-map-drift) Hand out the next monotonic logical-LWW counter.
    /// `Relaxed` is sufficient — the LWW needs only uniqueness + monotonicity of
    /// the returned value; the value↔counter binding is established by
    /// `set_stamp` before `cache.insert`.
    #[inline]
    fn next_stamp(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// (#locality-map-drift) The map's constant boot-epoch (HIGH word of every
    /// stamp it emits). See the `boot_epoch` field doc.
    #[inline]
    fn boot_epoch(&self) -> u64 {
        self.boot_epoch.load(Ordering::Relaxed)
    }

    /// (#locality-map-drift) Set the map's boot-epoch. The worker calls this at
    /// boot — BEFORE any insert — with `boot_epoch_id()` so its holdings deltas
    /// carry `(boot_epoch, counter)` and a restarted worker's fresh epoch
    /// dominates stale server state. Idempotent set-once in practice (single
    /// call on the boot path). `Relaxed` per the field doc.
    pub fn set_boot_epoch(&self, boot_epoch: u64) {
        self.boot_epoch.store(boot_epoch, Ordering::Relaxed);
    }

    /// (#locality-map-drift) `ts_counter` is the value's freshly-minted insert
    /// counter (already frozen into the value via `set_stamp` by the caller,
    /// before `cache.insert`). Fired with the map's constant `boot_epoch`.
    fn fire_on_insert_callbacks(&self, key: &K, size: u64, ts_counter: u64) {
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_insert(key.borrow(), size, self.boot_epoch(), ts_counter);
        }
    }

    /// Fire `on_get` callbacks for a public-read cache hit. Hot path:
    /// the AtomicBool fast path avoids touching the `callbacks` RwLock
    /// when no callbacks are registered (the common case for unit
    /// tests and stores configured without a tracker). `Relaxed` load
    /// pairs with the `Release` store in `add_item_callback`.
    ///
    /// Takes `&Q` (the borrowed key form) because the call sites in
    /// `get` / `get_many` already work in `&Q` and the callback trait
    /// itself is parameterized over `Q`. Using `&K` would force the
    /// caller to materialize an owned `K`, defeating the fast path.
    #[inline]
    fn fire_on_get(&self, key: &Q) {
        if !self.has_callbacks_flag.load(Ordering::Relaxed) {
            return;
        }
        // (#locality-map-drift) A read is a fresh PRESENT transition (`touched`)
        // that must be able to SUPERSEDE a stale evict — so it mints a FRESH
        // counter (higher than any prior evict of an older value of this key),
        // not the value's insert counter. This is what lets a worker that reads
        // a hot blob every action rescue it from a persistent false-missing.
        let ts_counter = self.next_stamp();
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_get(key, self.boot_epoch(), ts_counter);
        }
    }

    pub async fn insert_many<It>(&self, inserts: It) -> Vec<T>
    where
        It: IntoIterator<Item = (K, T)> + Send,
        <It as IntoIterator>::IntoIter: Send,
        K: 'static,
    {
        let mut replaced = Vec::new();
        for (key, data) in inserts {
            // Use insert_batch (no per-item run_pending_tasks) to avoid
            // N+1 maintenance passes. Process all pending tasks once at end.
            let old = self.insert_batch(key, data);
            if let Some(value) = old {
                value.unref().await;
                replaced.push(value);
            }
        }
        self.cache.run_pending_tasks();
        // Synchronously process any evictions triggered by the batch so
        // unref()/callbacks complete before returning.
        self.drain_pending_evictions().await;
        replaced
    }

    /// Batch-optimized insert: includes frequency bump but defers
    /// run_pending_tasks() to the caller. Used by insert_many().
    fn insert_batch(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // Update BTree index.
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        // If key is pinned, replace in pinned map directly.
        if self.has_pinned() && self.pinned.contains_key(key.borrow()) {
            // FL-681 Fix A: preserve the existing pin's `indefinite` flag
            // across a re-insert of the same key. A fresh write of a digest
            // that is currently pinned-until-BIS-ack MUST NOT silently
            // demote it to time-bounded (that would re-open the TTL leak).
            let mut was_indefinite = false;
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                if entry.indefinite {
                    was_indefinite = true;
                    self.indefinite_pinned_bytes
                        .fetch_sub(entry.size, Ordering::Relaxed);
                }
                entry.data
            });
            // (#locality-map-drift) A pinned re-insert is a fresh PRESENT
            // mutation: mint + freeze a new counter into `data` so the
            // re-advertise carries a strictly-higher ts than any prior evict
            // of this key.
            let ts_counter = self.next_stamp();
            data.set_stamp(ts_counter);
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: data.clone(),
                    pinned_at: Instant::now(),
                    size,
                    indefinite: was_indefinite,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            if was_indefinite {
                self.indefinite_pinned_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            self.fire_on_insert_callbacks(&key, size, ts_counter);
            if old.is_some() {
                self.replaced_bytes.add(size);
                self.replaced_items.inc();
            }
            return old;
        }

        // (#locality-map-drift) Freeze the value's logical-LWW counter before
        // `cache.insert` moves `data` (see `insert_inner`).
        let ts_counter = self.next_stamp();
        data.set_stamp(ts_counter);
        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // No run_pending_tasks — caller batches.
        self.fire_on_insert_callbacks(&key, size, ts_counter);
        if existing.is_some() {
            self.replaced_bytes.add(size);
            self.replaced_items.inc();
        }
        existing
    }

    // ---------------------------------------------------------------
    // remove
    // ---------------------------------------------------------------

    pub async fn remove(&self, key: &Q) -> bool {
        // Try pinned map first.
        if self.has_pinned() {
            if let Some((_, entry)) = self.pinned.remove(key) {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                // FL-681 Fix A fix-up (MAJOR-1a): keep the indefinite-pin
                // accounting symmetric on the explicit-remove path, exactly
                // as `unpin_key` does. `remove()` is reachable on an
                // indefinite-pinned (F2 output) key via
                // `FilesystemStore::remove_entry_for_digest` and the
                // stale/zero-byte-file eviction sites. Without this
                // decrement, `indefinite_pinned_bytes` leaks monotonically
                // upward, the `indefinite_pin_cap` saturates, and every new
                // F2 output falls back to a time-bounded pin — re-opening the
                // exact 120s-TTL silent-loss leak FL-681 fixes.
                if entry.indefinite {
                    self.indefinite_pinned_bytes
                        .fetch_sub(entry.size, Ordering::Relaxed);
                }
                self.update_btree_remove(key);

                // Fire callbacks + unref in background.
                // (#locality-map-drift) Read the REMOVED value's frozen
                // logical-LWW counter (this pinned-remove path bypasses the
                // moka eviction listener / `EvictionEvent`, so it must capture
                // the ts here) and carry it into the removal callback.
                let data = entry.data;
                let ts_counter = data.stamp();
                let callbacks = self.collect_removal_callbacks(key, ts_counter);
                drop(background_spawn!(
                    "moka_evicting_map_remove_cleanup",
                    async move {
                        let mut futs: FuturesUnordered<_> = callbacks.into_iter().collect();
                        while futs.next().await.is_some() {}
                        data.unref().await;
                    }
                ));
                return true;
            }
        }

        // Try moka cache. remove() returns the value and fires the
        // eviction listener (Explicit cause), which sends to the
        // background drainer for unref + callbacks.
        if self.cache.remove(key).is_some() {
            self.cache.run_pending_tasks();
            // BTree cleanup handled by eviction listener.
            return true;
        }
        false
    }

    /// (#locality-map-drift) Test hook: remove `key` from the moka cache but
    /// DEFER the removal-callback delivery. Returns `(frozen_counter,
    /// deliver)`, where `frozen_counter` is the EVICTED value's frozen
    /// logical-LWW counter (`value.stamp()`) captured at removal, and `deliver`
    /// is a closure that — given the counter to report — fires the removal
    /// `callback`s and awaits `unref`. This deterministically models moka's
    /// ASYNC eviction-listener reorder (the value leaves `R` NOW, the tracker
    /// mutation lands LATE, out of order vs a re-insert). The correct
    /// (value-carried) delivery passes `frozen_counter`; the Model A
    /// (mint-at-eviction) mutation passes a FRESH counter minted AFTER a
    /// re-insert (see `test_next_stamp`). Returns `None` if `key` isn't
    /// resident. Doc-hidden: exposes the internal decoupling for the composite
    /// drift test only.
    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub async fn test_remove_defer_callback(
        &self,
        key: &Q,
    ) -> Option<(
        u64,
        Box<
            dyn FnOnce(u64) -> core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send>>
                + Send
                + '_,
        >,
    )>
    where
        K: 'static + Clone,
        Q: Clone,
    {
        let value = self.cache.get(key)?;
        let frozen_counter = value.stamp();
        let owned_key = key.clone();
        // Remove from the cache WITHOUT letting the listener-drain deliver the
        // callback synchronously: invalidate + run_pending_tasks queues the
        // EvictionEvent, but we discard it and deliver via our own captured
        // ordering so the interleaving is fully test-controlled.
        self.cache.invalidate(key);
        self.cache.run_pending_tasks();
        while self.pending_evictions.lock().pop_front().is_some() {}
        let boot_epoch = self.boot_epoch();
        let callbacks_ref = &self.callbacks;
        let deliver: Box<
            dyn FnOnce(u64) -> core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send>>
                + Send
                + '_,
        > = Box::new(move |report_counter: u64| {
            let cbs = callbacks_ref.read();
            let callbacks: Vec<_> = cbs
                .iter()
                .map(|cb| cb.callback(owned_key.borrow(), boot_epoch, report_counter))
                .collect();
            drop(cbs);
            Box::pin(async move {
                let mut futs: FuturesUnordered<_> = callbacks.into_iter().collect();
                while futs.next().await.is_some() {}
                value.unref().await;
            })
        });
        Some((frozen_counter, deliver))
    }

    /// (#locality-map-drift) Test hook: hand out the next logical-LWW counter.
    /// Lets the composite drift test reproduce the Model A "mint-at-eviction"
    /// mutation by minting a FRESH counter AFTER a re-insert and delivering the
    /// deferred evict with it. Doc-hidden.
    #[doc(hidden)]
    #[must_use]
    pub fn test_next_stamp(&self) -> u64 {
        self.next_stamp()
    }

    pub async fn remove_if<F>(&self, key: &Q, cond: F) -> bool
    where
        F: FnOnce(&T) -> bool + Send,
    {
        // Check pinned first.
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                if cond(&entry.data) {
                    drop(entry);
                    return self.remove(key).await;
                }
                return false;
            }
        }

        // Check moka cache.
        if let Some(value) = self.cache.get(key) {
            if cond(&value) {
                return self.remove(key).await;
            }
        }
        false
    }

    fn update_btree_remove(&self, key: &Q) {
        let btree = self.btree.read();
        if btree.is_some() {
            drop(btree);
            let mut btree = self.btree.write();
            if let Some(ref mut set) = *btree {
                set.remove(key);
            }
        }
    }

    /// (#locality-map-drift) `ts_counter` is the REMOVED value's frozen
    /// logical-LWW counter (captured by the caller before the value is moved),
    /// carried into the removal `callback` so the eviction delta reports the
    /// counter this value was inserted with.
    fn collect_removal_callbacks(
        &self,
        key: &Q,
        ts_counter: u64,
    ) -> Vec<core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send>>> {
        let cbs = self.callbacks.read();
        cbs.iter()
            .map(|cb| cb.callback(key, self.boot_epoch(), ts_counter))
            .collect()
    }

    // ---------------------------------------------------------------
    // size queries
    // ---------------------------------------------------------------

    pub async fn size_for_key(&self, key: &Q) -> Option<u64> {
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                return Some(entry.data.len());
            }
        }
        self.cache.get(key).map(|v| v.len())
    }

    /// Note: the `peek` parameter is accepted for API compatibility but
    /// ignored. Moka has no non-promoting peek — `cache.get()` always
    /// updates the access time. For both ExistenceCacheStore and
    /// FilesystemStore has() checks, the LRU promotion is acceptable
    /// (and actually desirable for keeping hot blobs warm).
    ///
    /// IMPORTANT: this path intentionally does NOT fire `on_get`
    /// callbacks. `sizes_for_keys` is the existence-check entrypoint
    /// used by `FastSlowStore::has()` and `ExistenceCacheStore::update()`,
    /// each of which probes large key batches per request. Firing
    /// `on_get` here would explode the worker-side "recently read" set
    /// and overwhelm the locality_map broadcast on the server. Logical
    /// reads come exclusively from `get` / `get_many`.
    pub async fn sizes_for_keys<It, R>(
        &self,
        keys: It,
        results: &mut [Option<u64>],
        _peek: bool,
    ) where
        It: IntoIterator<Item = R> + Send,
        <It as IntoIterator>::IntoIter: Send,
        R: Borrow<Q> + Send,
    {
        let check_pinned = self.has_pinned();
        for (key, result) in keys.into_iter().zip(results.iter_mut()) {
            let k: &Q = key.borrow();
            if check_pinned {
                if let Some(entry) = self.pinned.get(k) {
                    *result = Some(entry.data.len());
                    continue;
                }
            }
            *result = self.cache.get(k).map(|v| v.len());
        }
    }

    // ---------------------------------------------------------------
    // pinning
    // ---------------------------------------------------------------

    pub fn pin_key(&self, key: K) -> bool {
        self.pin_key_with_mode(key, false)
    }

    /// FL-681 Fix A: pin `key` INDEFINITELY — held until BIS-ack
    /// (`unpin_key`), EXEMPT from the `PIN_TIMEOUT_SECS` sweep. Used by
    /// worker-local F2 deferred-output uploads so the source blob stays
    /// un-evictable until the server confirms durability, never dropping
    /// it at the 120s TTL. Subject to the `indefinite_pin_cap` (returns
    /// `false` over-cap as BACKPRESSURE — the caller must retry, not
    /// drop). Refreshing an already-pinned key UPGRADES it to indefinite.
    pub fn pin_key_indefinite(&self, key: K) -> bool {
        self.pin_key_with_mode(key, true)
    }

    /// Shared pin core for [`Self::pin_key`] / [`Self::pin_key_indefinite`].
    /// `indefinite=true` exempts the entry from the TTL sweep and enforces
    /// the indefinite-pin cap.
    fn pin_key_with_mode(&self, key: K, indefinite: bool) -> bool {
        let q: &Q = key.borrow();

        // Already pinned — refresh pin time. If this call requests an
        // indefinite pin and the existing entry is time-bounded, UPGRADE
        // it to indefinite (subject to the indefinite cap). An upgrade is
        // never a downgrade: an already-indefinite pin stays indefinite.
        if let Some(mut entry) = self.pinned.get_mut(q) {
            entry.pinned_at = Instant::now();
            if indefinite && !entry.indefinite {
                let size = entry.size;
                if !self.indefinite_cap_admits(size) {
                    warn!(
                        indefinite_pinned_bytes =
                            self.indefinite_pinned_bytes.load(Ordering::Relaxed),
                        entry_size = size,
                        indefinite_pin_cap = self.indefinite_pin_cap,
                        ?key,
                        "indefinite pin cap exceeded on upgrade, leaving pin time-bounded (backpressure)"
                    );
                    // Refused upgrade is BACKPRESSURE, not loss: the entry
                    // stays pinned (time-bounded). Report failure so the
                    // caller retries the durability upload.
                    return false;
                }
                entry.indefinite = true;
                self.indefinite_pinned_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            return true;
        }

        // Look up in cache (clone value while it's still in cache).
        let value = match self.cache.get(q) {
            Some(v) => v,
            None => return false,
        };

        let entry_size = value.len();

        // Enforce pin cap (total) and, for indefinite pins, the
        // indefinite-pin cap. Either rejection is BACKPRESSURE (refuse,
        // never drop): the blob stays in the LRU cache for the caller to
        // retry.
        if self.max_bytes != 0 {
            let current_pinned = self.pinned_bytes.load(Ordering::Relaxed);
            if current_pinned.saturating_add(entry_size) > self.pin_cap {
                warn!(
                    pinned_bytes = current_pinned,
                    entry_size,
                    pin_cap = self.pin_cap,
                    ?key,
                    "pin cap exceeded, refusing to pin"
                );
                return false;
            }
        }
        if indefinite && !self.indefinite_cap_admits(entry_size) {
            warn!(
                indefinite_pinned_bytes =
                    self.indefinite_pinned_bytes.load(Ordering::Relaxed),
                entry_size,
                indefinite_pin_cap = self.indefinite_pin_cap,
                ?key,
                "indefinite pin cap exceeded, refusing to pin (backpressure)"
            );
            return false;
        }

        // CRITICAL: Insert into pinned map FIRST, then invalidate from
        // cache. The eviction listener checks pinned map and skips
        // cleanup if the key is found there. This ordering prevents the
        // race where invalidate fires the listener before the item is
        // in the pinned map.
        self.pinned.insert(
            key.clone(),
            PinnedEntry {
                data: value,
                pinned_at: Instant::now(),
                size: entry_size,
                indefinite,
            },
        );
        self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);
        if indefinite {
            self.indefinite_pinned_bytes
                .fetch_add(entry_size, Ordering::Relaxed);
        }

        // Now safe to remove from cache — listener will see it's pinned.
        self.cache.invalidate(q);
        self.cache.run_pending_tasks();
        true
    }

    /// FL-681 Fix A: snapshot check of the indefinite-pin byte cap.
    /// Mirrors `FastSlowStore::check_slow_writes_capacity_gate` — an
    /// eventually-consistent SNAPSHOT is acceptable for backpressure (the
    /// cap STOPS an over-capacity hot loop; the caller's retry closes the
    /// residual race). `max_bytes == 0` (no byte budget configured) never
    /// gates.
    fn indefinite_cap_admits(&self, entry_size: u64) -> bool {
        if self.max_bytes == 0 {
            return true;
        }
        let current = self.indefinite_pinned_bytes.load(Ordering::Relaxed);
        current.saturating_add(entry_size) <= self.indefinite_pin_cap
    }

    pub fn pin_keys(&self, keys: &[K]) -> usize {
        let mut pinned = 0;
        for key in keys {
            let q: &Q = key.borrow();

            // Already pinned — refresh. Delegating to the per-key path keeps
            // the pin accounting in exactly one place.
            if self.pinned.contains_key(q) {
                if self.pin_key(key.clone()) {
                    pinned += 1;
                }
                continue;
            }

            let value = match self.cache.get(q) {
                Some(v) => v,
                None => continue,
            };

            let entry_size = value.len();
            if self.max_bytes != 0 {
                let current = self.pinned_bytes.load(Ordering::Relaxed);
                if current.saturating_add(entry_size) > self.pin_cap {
                    warn!(
                        pinned_bytes = current,
                        entry_size,
                        pin_cap = self.pin_cap,
                        attempted = keys.len(),
                        pinned,
                        "pin_keys: pin cap exceeded, leaving remaining keys unpinned",
                    );
                    break;
                }
            }

            // Insert into pinned FIRST (same ordering as pin_key).
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: value,
                    pinned_at: Instant::now(),
                    size: entry_size,
                    indefinite: false,
                },
            );
            self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);

            // Invalidate from cache (don't call run_pending_tasks per key).
            self.cache.invalidate(q);
            pinned += 1;
        }
        // Batch: process all invalidations at once.
        self.cache.run_pending_tasks();
        pinned
    }

    pub fn unpin_key(&self, key: &Q) {
        if let Some((owned_key, entry)) = self.pinned.remove(key) {
            self.pinned_bytes
                .fetch_sub(entry.size, Ordering::Relaxed);
            // FL-681 Fix A: keep the indefinite-pin accounting symmetric.
            // BIS-ack release of an indefinite pin frees indefinite-cap
            // headroom for the next pending-BIS blob.
            if entry.indefinite {
                self.indefinite_pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
            }
            // Move back into moka cache. Under LRU there is no admission
            // filter to fight, so a bare insert is sufficient.
            // (#locality-map-drift) Intentionally a bare `cache.insert` — NO
            // `set_stamp`, NO `fire_on_insert_callbacks`. The value keeps its
            // OWN frozen insert-stamp (assigned when it was first inserted before
            // being pinned), which is LWW-correct: a later GENUINE eviction of
            // this value will carry that stamp, and the holdings map already
            // learned this digest is PRESENT at insert time (unpin does not
            // change residency, so re-emitting a PRESENT delta is unnecessary).
            self.cache.insert(owned_key, entry.data);
        }
    }

    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }

    /// FL-681 Fix A: bytes currently held by INDEFINITE
    /// (pinned-until-BIS-ack) pins. A subset of [`Self::pinned_bytes`].
    /// Bounded by the indefinite-pin cap.
    pub fn indefinite_pinned_bytes(&self) -> u64 {
        self.indefinite_pinned_bytes.load(Ordering::Relaxed)
    }

    /// FL-681 Follow-up A (MAJOR-1b close-out): snapshot predicate for the
    /// worker's admission-side gate. Returns `true` when the indefinite-pin
    /// cap has NO headroom left for even a zero-byte blob — i.e. the next
    /// fresh F2 output's `pin_key_indefinite` would be REFUSED. The worker's
    /// action-acceptance path reads this and NAKs the new action with
    /// `Code::ResourceExhausted` so the scheduler re-queues it (true producer
    /// backpressure) instead of admitting an action whose output cannot be
    /// pinned-until-durable.
    ///
    /// Same eventually-consistent snapshot shape as `indefinite_cap_admits`
    /// (the cap STOPS an over-capacity hot loop; the scheduler's natural
    /// re-queue closes the residual race). `max_bytes == 0` (no byte budget
    /// configured) never gates — an uncapped store has no indefinite-pin cap
    /// to saturate. "Saturated" means `indefinite_pinned_bytes >= cap`: at the
    /// exact-full boundary the next real (>0-byte) F2 output's indefinite pin
    /// is already refused, so the gate must fire there, not one blob later.
    #[must_use]
    pub fn indefinite_pin_saturated(&self) -> bool {
        if self.max_bytes == 0 {
            return false;
        }
        self.indefinite_pinned_bytes.load(Ordering::Relaxed) >= self.indefinite_pin_cap
    }

    /// FL-681 Follow-up B (MAJOR-2 robust close-out): enumerate the keys of the
    /// INDEFINITE (pending-BIS-ack) pin subset. The worker re-advertises this
    /// set in the periodic BlobsAvailable heartbeat so a CAS digest whose
    /// `mark_stable` was missed (transient server existence-check failure) is
    /// re-driven to BIS within a bounded number of heartbeat ticks, WITHOUT
    /// waiting for a reconnect. Self-pruning: a BIS-ack `unpin_key` drops the
    /// digest, so it falls out of the next enumeration automatically.
    ///
    /// Bounded by `indefinite_pin_cap` (the whole point of the cap). Cheap
    /// DashMap iteration — no lock held across an `.await` by the caller.
    pub fn indefinite_pinned_digests(&self) -> Vec<K> {
        self.pinned
            .iter()
            .filter(|entry| entry.value().indefinite)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Test hook: rewind a pinned entry's `pinned_at` past the
    /// `PIN_TIMEOUT_SECS` deadline so the next `expire_stale_pins` sweep
    /// treats it as stale. Returns `true` if the key was pinned and was
    /// rewound, `false` otherwise. Doc-hidden because it bypasses the
    /// pin-refresh contract; integration tests use it to deterministically
    /// drive auto-unpin without waiting 120s of wall-clock.
    #[doc(hidden)]
    pub fn test_force_pin_expired(&self, key: &Q) -> bool {
        if let Some(mut entry) = self.pinned.get_mut(key) {
            entry.pinned_at = Instant::now()
                .checked_sub(Duration::from_secs(PIN_TIMEOUT_SECS + 1))
                .unwrap_or(entry.pinned_at);
            return true;
        }
        false
    }

    // ---------------------------------------------------------------
    // filtering / range
    // ---------------------------------------------------------------

    pub async fn enable_filtering(&self) {
        let mut btree = self.btree.write();
        if btree.is_none() {
            let mut set = BTreeSet::new();
            for (key, _value) in &self.cache {
                set.insert((*key).clone());
            }
            for entry in self.pinned.iter() {
                set.insert(entry.key().clone());
            }
            *btree = Some(set);
        }
    }

    pub async fn range<F>(
        &self,
        prefix_range: impl RangeBounds<Q> + Send,
        mut handler: F,
    ) -> u64
    where
        F: FnMut(&K, &T) -> bool + Send,
        K: Ord,
    {
        // Ensure BTree is built.
        {
            let btree = self.btree.read();
            if btree.is_none() {
                drop(btree);
                self.enable_filtering().await;
            }
        }

        let btree = self.btree.read();
        let set = btree.as_ref().expect("btree should be built");
        let check_pinned = self.has_pinned();
        let mut count = 0;
        for key in set.range(prefix_range) {
            let q: &Q = key.borrow();
            let value = if check_pinned {
                if let Some(entry) = self.pinned.get(q) {
                    Some(entry.data.clone())
                } else {
                    self.cache.get(q)
                }
            } else {
                self.cache.get(q)
            };
            // Skip keys evicted by moka but still in BTree (stale).
            if let Some(ref v) = value {
                if !handler(key, v) {
                    break;
                }
                count += 1;
            }
        }
        count
    }

    // ---------------------------------------------------------------
    // callbacks
    // ---------------------------------------------------------------

    pub fn add_item_callback(&self, callback: C) {
        self.callbacks.write().push(callback);
        // Publish with Release so the hot read path (which loads with
        // Relaxed) cannot observe `has_callbacks_flag == true` before it
        // would observe the appended callback under the RwLock. The
        // RwLock release inherent in `write()` already establishes the
        // necessary happens-before for the Vec contents; the AtomicBool
        // store is a separate signal whose Release ordering pairs with
        // the Acquire implicit in the subsequent RwLock `read()` in
        // `fire_on_get` / `fire_on_insert_callbacks`.
        self.has_callbacks_flag.store(true, Ordering::Release);
    }

    // ---------------------------------------------------------------
    // timestamps / diagnostics
    // ---------------------------------------------------------------

    pub fn get_all_entries_with_timestamps(&self) -> Vec<(K, i64)> {
        let anchor_epoch = self.anchor_time.unix_timestamp() as i64;
        let now_offset =
            i64::try_from(self.anchor_time.elapsed().as_secs()).unwrap_or(i64::MAX);

        let mut result = Vec::new();
        for (key, _value) in &self.cache {
            result.push(((*key).clone(), anchor_epoch + now_offset));
        }
        for entry in self.pinned.iter() {
            result.push((entry.key().clone(), anchor_epoch + now_offset));
        }
        result
    }

    pub async fn len_for_test(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize + self.pinned.len()
    }

    /// Returns `true` if a hypothetical insert of `incoming_bytes`
    /// would push the cache over its configured `max_bytes` ceiling
    /// (eventually-consistent — moka's `weighted_size()` only refreshes
    /// after `run_pending_tasks()`, so the answer is a best-effort
    /// snapshot of the most recent processed batch).
    ///
    /// Returns `false` when no byte cap is configured (`max_bytes == 0`)
    /// so callers without a byte budget never trip the predicate.
    ///
    /// Used by `MemoryStore` Phase 2.6 backpressure emission to refuse
    /// a write that would otherwise force eviction of a recent
    /// (potentially still-in-use) blob — the alternative is the historic
    /// silent-evict behavior, which is preserved when the operator
    /// keeps the kill-switch off.
    ///
    /// `weighted_size()` returns the sum of moka weights, which we
    /// scale by `1024` (the SCALE constant used by the weigher) to
    /// approximate the byte usage. Because the weigher rounds up to
    /// KB granularity, this slightly OVER-estimates current usage —
    /// which biases the predicate toward emitting backpressure
    /// EARLIER, never later, and so cannot cause a "should-have-evicted
    /// but didn't" silent-overrun.
    #[must_use]
    pub fn would_exceed_capacity(&self, incoming_bytes: u64) -> bool {
        if self.max_bytes == 0 {
            return false;
        }
        // SCALE matches the weigher in `with_anchor`. Keeping the
        // multiplier inline avoids exposing the constant publicly while
        // keeping the byte ↔ weight conversion local to one call-site.
        const SCALE: u64 = 1024;
        let cache_bytes = self.cache.weighted_size().saturating_mul(SCALE);
        // #334 Fix C: account for pinned bytes too. Pinned entries live
        // OUTSIDE moka's `cache` (in the `pinned` DashMap) and so do
        // NOT count toward `cache.weighted_size()`. But they DO consume
        // physical RSS — a 48 GB MemoryStore with 12 GB of pins still
        // has only 36 GB of evictable headroom. Without this, the
        // backpressure gate would mis-estimate capacity in the
        // BIS-ack-window scenario (the very scenario the pin/unpin
        // mechanism was added to address).
        let pinned_bytes_now = self.pinned_bytes.load(Ordering::Relaxed);
        let current_bytes = cache_bytes.saturating_add(pinned_bytes_now);
        // Round the incoming size up to KB to match the weigher's
        // own behavior: a 1-byte insert weighs 1 (=1 KB after scale),
        // not 0.
        let incoming_kb_bytes = incoming_bytes.div_ceil(SCALE).saturating_mul(SCALE);
        current_bytes.saturating_add(incoming_kb_bytes) > self.max_bytes
    }

    /// #334 Fix C eviction extension: actively evict UNPINNED entries
    /// from the moka cache to free at least `target_bytes` of headroom.
    /// Returns `(evicted_count, evicted_bytes)` — `evicted_bytes` may be
    /// less than `target_bytes` if too few unpinned entries remain.
    ///
    /// **Why this exists.** With `MemoryStore::emit_backpressure_enabled
    /// = true` (the production default for `cas_FAST_SLOW_STORE.fast`),
    /// `check_backpressure_gate` refuses inserts BEFORE moka's natural
    /// admission-driven LRU eviction can fire. Once the cache fills to
    /// cap, NOTHING evicts (no insert pressure, no TTL, no explicit
    /// removes), so the cache becomes write-once-evict-never until
    /// process restart. This helper restores eviction by giving the
    /// gate a way to free room before emitting backpressure.
    ///
    /// **Order caveat.** Moka's public API (`cache.iter()`) walks
    /// entries in arbitrary order, NOT LRU order. Moka's internal
    /// access-order queue (`access_order_q_node`) is `pub(crate)` and
    /// not exposed. The "LRU" in this method's name is therefore
    /// aspirational, not strict — we evict in iteration order, which is
    /// neither hot-first nor cold-first. This is acceptable because:
    ///   * Pinned entries (the load-bearing ones during the BIS ack
    ///     window) are stored OUTSIDE moka's `cache` in the `pinned`
    ///     DashMap, so this helper CANNOT evict them — pin protection
    ///     is preserved regardless of iteration order.
    ///   * Unpinned entries are by definition evictable; choosing them
    ///     in arbitrary order is no worse than LRU for the BIS-ack
    ///     durability invariant. A future moka release exposing an
    ///     ordered LRU walker (or a `coldest_n` API) can be slotted in
    ///     here without changing call sites.
    ///
    /// **Concurrency.** This is a sync method. Holds no awaits. Moka's
    /// `cache.iter()` and `cache.invalidate()` are lock-free /
    /// fine-grained-locked internally; concurrent inserts/reads remain
    /// non-blocking.
    pub fn evict_unpinned_lru_bytes(&self, target_bytes: u64) -> EvictedReport {
        if target_bytes == 0 {
            return EvictedReport::default();
        }
        let start = Instant::now();
        // Flush in-flight admissions/evictions so iter() sees a stable
        // snapshot of what's actually resident.
        self.cache.run_pending_tasks();

        let check_pinned = self.has_pinned();
        let mut evicted_bytes: u64 = 0;
        let mut evicted_count: u64 = 0;
        let mut iter_scanned: u64 = 0;
        let mut iter_truncated = false;

        // Collect candidates in iteration order. We collect first
        // (rather than invalidate during iteration) because moka's
        // iterator is documented to skip entries removed mid-iteration,
        // and we want a deterministic "I considered N entries" result
        // for tracing.
        //
        // #334 bundle fixup #8b (perf MAJOR): hard cap at
        // EVICT_SCAN_HARD_CAP entries per call. Bursty all-pinned
        // scenarios could otherwise walk the full 1M-entry production
        // cas_FAST_SLOW MemoryStore before giving up. The caller's
        // `would_exceed_capacity` re-check is still authoritative for
        // admission; this cap only bounds the SCAN, not the eviction
        // contract.
        for (key_arc, value) in self.cache.iter() {
            iter_scanned = iter_scanned.saturating_add(1);
            if evicted_bytes >= target_bytes {
                break;
            }
            if iter_scanned >= EVICT_SCAN_HARD_CAP {
                iter_truncated = true;
                break;
            }
            let q: &Q = (*key_arc).borrow();
            // Skip pinned entries — pin protection is the whole point.
            // (Note: pin_keys() invalidates from cache at ::pin_keys's
            // end, so pinned entries normally never appear here. The
            // check guards a stale-pin race window.)
            if check_pinned && self.pinned.contains_key(q) {
                continue;
            }
            let size = value.len();
            // Synchronous invalidate; the eviction listener will fire
            // and route the entry to the background drainer for
            // unref + callback. We do NOT await the unref here — that
            // would require an async fn AND a borrow over .await of
            // the moka iterator, neither acceptable.
            self.cache.invalidate(q);
            evicted_bytes = evicted_bytes.saturating_add(size);
            evicted_count = evicted_count.saturating_add(1);
        }

        // Re-flush so the post-eviction `weighted_size()` reflects the
        // invalidations we just issued. Without this, the caller's
        // `would_exceed_capacity` re-check would see the stale (pre-
        // eviction) size and emit backpressure unnecessarily.
        self.cache.run_pending_tasks();

        let elapsed_ms = start.elapsed().as_millis() as u64;
        // #85 P5 (2026-06-07): histogram-export the same elapsed_ms
        // already measured here so contention BELOW the 50 ms warn
        // is visible. Observation-only; no behavior change.
        crate::o11_probes::evicting_map_lock_histogram().observe(elapsed_ms);
        if iter_truncated || elapsed_ms > 50 {
            warn!(
                evicted_count,
                evicted_bytes,
                iter_scanned,
                iter_truncated,
                elapsed_ms,
                target_bytes,
                "evict_unpinned_lru_bytes: scan-cap hit or slow scan",
            );
        }

        EvictedReport {
            evicted_count,
            evicted_bytes,
            iter_scanned,
            iter_truncated,
        }
    }

    // ---------------------------------------------------------------
    // background eviction drainer
    // ---------------------------------------------------------------

    /// (FL-688 v3 Stage C) Disable the periodic explicit drain until
    /// `release_startup_reconcile_gate()` is called. Called once at worker
    /// boot, before any `insert_startup` calls, before the background eviction
    /// loop is started. The gate is `true` (drain enabled) by default; setting
    /// it `false` here blocks ONLY the `drain_interval` tick in
    /// `drain_evictions`. Per-insert eviction is unaffected — blobs must be
    /// pinned via `pin_digest_indefinite_with_result` for full protection.
    pub fn set_startup_reconcile_gate(&self) {
        self.reconcile_complete
            .store(false, Ordering::Release);
    }

    /// (FL-688 v3 Stage C) Release the reconcile gate. Called when the server
    /// sends `ReconcileCompleteRequest`. After this, the background drain tick
    /// runs normally. Returns an `Arc` clone of the flag so the filesystem
    /// store can hand it to the worker without extra indirection.
    pub fn release_startup_reconcile_gate(&self) {
        self.reconcile_complete
            .store(true, Ordering::Release);
    }

    /// (FL-688 v3 Stage C) Return a shared handle to the reconcile-complete
    /// flag. `FilesystemStore` uses this to expose the gate to `local_worker`.
    pub fn reconcile_complete_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reconcile_complete)
    }

    pub fn start_background_eviction(self: &Arc<Self>) {
        if self
            .background_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let this = Arc::clone(self);
        let rx = this
            .eviction_rx
            .lock()
            .take()
            .expect("start_background_eviction called twice");

        drop(background_spawn!(
            "moka_evicting_map_background",
            async move {
                this.drain_evictions(rx).await;
            }
        ));
    }

    async fn drain_evictions(
        self: &Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<()>,
    ) {
        let mut pin_check_interval = tokio::time::interval(Duration::from_secs(10));
        pin_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // #605: periodic forced-drain tick. moka's weight-based eviction is
        // eventually-consistent — it only enforces the byte cap on `insert`
        // or an explicit `run_pending_tasks`. A cache that took its
        // overshoot via the startup `insert_with_time` path (which defers
        // `run_pending_tasks`) and then sees no further runtime `insert`
        // trails its cap indefinitely (production: a 40 GiB worker fast tier
        // observed at ~162 GB). This tick periodically calls
        // `run_pending_tasks_and_drain` so the cache converges to cap during
        // runtime, bounding the worst-case overshoot to one interval.
        // Pin-safe by construction: pinned entries were MOVED OUT of the
        // moka cache into the side `pinned` DashMap (`pin_key_with_mode`'s
        // `cache.invalidate`), so a forced drain over `self.cache` cannot
        // evict them. On an at-or-under-cap cache the drain produces no
        // eviction events and returns after a single iteration — negligible
        // overhead. NOT a per-entry TTL: no `time_to_live`/`time_to_idle` is
        // added to entries; this is a loop-driven maintenance call like
        // `expire_stale_pins`.
        let mut drain_interval =
            tokio::time::interval(Duration::from_secs(DRAIN_INTERVAL_SECS));
        drain_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(()) = rx.recv() => {
                    // Coalesce additional wake signals — we'll drain the
                    // entire side queue below regardless of how many
                    // signals we got.
                    while rx.try_recv().is_ok() {}
                    self.drain_pending_evictions().await;
                }
                _ = pin_check_interval.tick() => {
                    self.expire_stale_pins().await;
                }
                _ = drain_interval.tick() => {
                    // (FL-688 v3 Stage C) Skip the forced drain during the
                    // worker startup reconcile window. The PRIMARY protection
                    // is reconcile-pin (`pin_digest_indefinite_with_result`);
                    // this gate is SECONDARY — it prevents the EXPLICIT
                    // periodic LRU sweep from racing the reconcile-pin call.
                    // Per-insert moka eviction (in `insert_inner`) cannot be
                    // gated here; pinning each blob handles that.
                    // Acquire matches the Release in `release_startup_reconcile_gate`.
                    if !self.reconcile_complete.load(Ordering::Acquire) {
                        continue;
                    }
                    // Force moka's capacity check + drain the resulting
                    // eviction events on THIS task (the sole owner of the
                    // shared `pending_evictions` queue) — never a parallel
                    // task, which would double-drain the queue.
                    self.run_pending_tasks_and_drain().await;
                }
            }
        }
    }

    async fn process_eviction_event(&self, event: EvictionEvent<K, T>) {
        let size = event.value.len();
        self.evicted_bytes.add(size);
        self.evicted_items.inc();

        event.value.unref().await;

        // (#locality-map-drift) Carry the EVICTED value's frozen logical-LWW
        // counter (captured off the value by the eviction listener) into the
        // removal callback — the value-carried ts, never a fresh mint.
        let ts_counter = event.ts_counter;
        let callbacks = {
            let cbs = self.callbacks.read();
            let q: &Q = (*event.key).borrow();
            cbs.iter()
                .map(|cb| cb.callback(q, self.boot_epoch(), ts_counter))
                .collect::<Vec<_>>()
        };
        if !callbacks.is_empty() {
            let mut futs: FuturesUnordered<_> = callbacks.into_iter().collect();
            while futs.next().await.is_some() {}
        }
        drop(event);
    }

    /// Sweep the `pinned` map and demote any entries whose
    /// `pinned_at + PIN_TIMEOUT_SECS` has elapsed back into the LRU
    /// cache. Public-but-doc-hidden so integration tests can drive the
    /// sweep deterministically without waiting on the 10s background
    /// ticker. The background loop in `start_background_eviction` calls
    /// this once per tick.
    #[doc(hidden)]
    pub async fn expire_stale_pins(&self) {
        let mut expired_keys = Vec::new();
        for entry in self.pinned.iter() {
            // FL-681 Fix A: INDEFINITE pins (worker-local F2
            // pinned-until-BIS-ack blobs) are EXEMPT from the TTL sweep.
            // They are released ONLY by the server's BIS-ack (`unpin_key`).
            // Demoting one here at the 120s TTL is the exact silent-loss
            // leak FL-681 fixes (3,881 `auto-unpinning expired pin` events
            // fleet-wide, including on the failing protoc_minimal digest).
            if entry.indefinite {
                continue;
            }
            if entry.pinned_at.elapsed().as_secs() >= PIN_TIMEOUT_SECS {
                expired_keys.push(entry.key().clone());
            }
        }
        for key in expired_keys {
            let q: &Q = key.borrow();
            if let Some((owned_key, entry)) = self.pinned.remove(q) {
                // Race guard: an entry selected as stale above could have
                // been UPGRADED to indefinite (BIS-window F2 re-pin)
                // between selection and this remove. If so, restore it
                // un-demoted — never drop an indefinite pin's BIS-bound
                // protection.
                if entry.indefinite {
                    self.pinned.insert(owned_key, entry);
                    continue;
                }
                let size = entry.size;
                info!(
                    ?key,
                    pin_timeout_secs = PIN_TIMEOUT_SECS,
                    entry_size = size,
                    "auto-unpinning expired pin"
                );
                self.pinned_bytes.fetch_sub(size, Ordering::Relaxed);
                // Put back into cache so it can be evicted normally.
                //
                // NOTE: this is NOT an eviction — the blob is still
                // resident in this `MokaEvictingMap`, just no longer
                // pinned. We deliberately do NOT route through
                // `process_eviction_event` (which would increment
                // `evicted_bytes` / `evicted_items` and fire the
                // removal `callback`). A previous code-review flagged
                // a counter double-count risk if pin-expiry was
                // accounted as eviction.
                //
                // We DO want listeners (e.g. BlobChangeTracker, which
                // feeds the server's locality_map via BlobsAvailable)
                // to re-acknowledge the blob now that it has crossed
                // back into the regular LRU pool. Fire `on_insert` so
                // the next BlobsAvailable broadcast carries a fresh
                // entry for it.
                // (#locality-map-drift) Pin-expiry demotes the blob back into
                // the LRU pool — a fresh PRESENT transition. Mint + freeze a
                // new counter so the re-ack carries a strictly-higher ts than
                // any prior evict of this key.
                let ts_counter = self.next_stamp();
                entry.data.set_stamp(ts_counter);
                self.cache.insert(key.clone(), entry.data);
                self.fire_on_insert_callbacks(&key, size, ts_counter);
                // Also fire the pin-expiry hook so durability listeners
                // (FastSlowStore) can record a pending-write retry. The
                // pin TTL firing means we have NO confirmation that the
                // slow-store write completed — the safe default is to
                // queue the digest for retry. Even if the slow-write
                // ultimately succeeds, the retry will see "already
                // present" via existence check and no-op. The cost of a
                // false positive is a single existence RPC; the cost of
                // a false negative is permanent data loss.
                self.fire_on_pin_expired_callbacks(&key, size);
            }
        }
    }

    fn fire_on_pin_expired_callbacks(&self, key: &K, size: u64) {
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_pin_expired(key.borrow(), size);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Inline coverage for the `on_get` hook added on top of the
    //! existing `tests/moka_evicting_map_test.rs` integration tests.
    //! These tests are colocated with the implementation because they
    //! exercise the AtomicBool fast-path field which is not part of
    //! the public surface.
    use core::future::Future;
    use core::pin::Pin;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::time::Instant;

    use nativelink_config::stores::EvictionPolicy;

    use super::{MokaEvictingMap, PinnedEntry, PIN_TIMEOUT_SECS};
    use crate::evicting_map::{ItemCallback, LenEntry};

    // ---------------------------------------------------------------
    // Test helpers (mirrors `tests/moka_evicting_map_test.rs` so the
    // inline tests are self-contained — the integration tests cannot
    // be reused here because they live in a separate crate target).
    // ---------------------------------------------------------------

    #[derive(Debug, Clone)]
    struct BytesEntry(u64);

    impl LenEntry for BytesEntry {
        fn len(&self) -> u64 {
            self.0
        }
        fn is_empty(&self) -> bool {
            self.0 == 0
        }
    }

    fn policy(max_bytes: usize, max_count: u64) -> EvictionPolicy {
        EvictionPolicy {
            max_bytes,
            evict_bytes: 0,
            max_seconds: 0,
            max_count,
        }
    }

    /// Callback that increments a counter for each hook invocation.
    /// Records the last key seen on each hook for assertion granularity.
    #[derive(Debug, Clone)]
    struct CountingCallback {
        get_count: Arc<AtomicU64>,
        insert_count: Arc<AtomicU64>,
        removal_count: Arc<AtomicU64>,
        pin_expired_count: Arc<AtomicU64>,
    }

    impl CountingCallback {
        fn new() -> Self {
            Self {
                get_count: Arc::new(AtomicU64::new(0)),
                insert_count: Arc::new(AtomicU64::new(0)),
                removal_count: Arc::new(AtomicU64::new(0)),
                pin_expired_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl ItemCallback<u64> for CountingCallback {
        fn callback(
            &self,
            _key: &u64,
            _ts_boot_epoch: u64,
            _ts_counter: u64,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            self.removal_count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        }

        fn on_insert(&self, _key: &u64, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {
            self.insert_count.fetch_add(1, Ordering::Relaxed);
        }

        fn on_get(&self, _key: &u64, _ts_boot_epoch: u64, _ts_counter: u64) {
            self.get_count.fetch_add(1, Ordering::Relaxed);
        }

        fn on_pin_expired(&self, _key: &u64, _size: u64) {
            self.pin_expired_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    type TestMapCb =
        MokaEvictingMap<u64, u64, BytesEntry, SystemTime, CountingCallback>;

    fn make_map_cb(cfg: &EvictionPolicy) -> TestMapCb {
        MokaEvictingMap::with_anchor(cfg, SystemTime::now())
    }

    /// FL-681: build a test map with an explicit indefinite-pin byte cap
    /// so the cap-backpressure path can be exercised deterministically
    /// without standing up a 20 GiB FilesystemStore.
    fn make_map_cb_indefinite_cap(
        cfg: &EvictionPolicy,
        indefinite_pin_cap_bytes: u64,
    ) -> TestMapCb {
        MokaEvictingMap::with_anchor_and_indefinite_cap(
            cfg,
            SystemTime::now(),
            indefinite_pin_cap_bytes,
        )
    }

    // ---------------------------------------------------------------
    // 1. on_get fires on cache.get hit
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_fires_on_cache_get_hit() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        map.insert(1, BytesEntry(10)).await;
        // `insert` does not fire `on_get`.
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "insert must not fire on_get"
        );

        // First read — must fire on_get exactly once.
        let v = map.get(&1).await;
        assert!(v.is_some(), "key should be present");
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            1,
            "on_get should fire once on cache hit"
        );

        // Second read — fires again.
        let _ = map.get(&1).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            2,
            "on_get should fire on every cache-hit read"
        );

        // get_many: hit two keys, miss one — only the two hits should fire.
        map.insert(2, BytesEntry(20)).await;
        let results = map.get_many(&[1u64, 2, 99]).await;
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            2 + 2,
            "get_many should fire on_get once per cache hit, not on misses"
        );
    }

    // ---------------------------------------------------------------
    // 2. on_get does NOT fire on cache.get miss
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_on_miss() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        // No inserts — every get is a miss.
        for k in 0..10u64 {
            let v = map.get(&k).await;
            assert!(v.is_none());
        }
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "on_get must not fire on cache miss"
        );

        // get_many of all-missing keys.
        let results = map.get_many(&[100u64, 101, 102]).await;
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(Option::is_none));
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "get_many must not fire on_get for any miss"
        );
    }

    // ---------------------------------------------------------------
    // 3. on_get does NOT fire from sizes_for_keys
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_from_sizes_for_keys() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        let insert_count = Arc::clone(&cb.insert_count);
        map.add_item_callback(cb);

        // Populate three keys (fires on_insert thrice, never on_get).
        for k in 0..3u64 {
            map.insert(k, BytesEntry((k + 1) * 100)).await;
        }
        assert_eq!(insert_count.load(Ordering::Relaxed), 3);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "insert must not fire on_get"
        );

        // sizes_for_keys: pure existence-check path. Must NOT fire on_get
        // even for present keys (FastSlowStore::has() / ExistenceCacheStore
        // would otherwise explode the touched set).
        let keys = [0u64, 1, 2, 99];
        let mut sizes = [None; 4];
        map.sizes_for_keys(keys.iter(), &mut sizes, false).await;
        assert_eq!(sizes[0], Some(100));
        assert_eq!(sizes[1], Some(200));
        assert_eq!(sizes[2], Some(300));
        assert_eq!(sizes[3], None);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "sizes_for_keys must not fire on_get"
        );

        // Also confirm size_for_key does not fire (single-key existence path).
        let _ = map.size_for_key(&0).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "size_for_key must not fire on_get"
        );
    }

    // ---------------------------------------------------------------
    // 4. on_get fast path: no fire when no callbacks registered
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_fast_path_no_callbacks() {
        // No callback added — `has_callbacks_flag` stays false and the
        // fire_on_get fast path returns immediately. We cannot
        // directly observe "no RwLock taken" without instrumentation,
        // but we can observe (a) the flag remains false and (b) reads
        // succeed. A sentinel callback added afterwards must then
        // start firing.
        let cfg = policy(0, 100);
        let map: MokaEvictingMap<
            u64,
            u64,
            BytesEntry,
            SystemTime,
            CountingCallback,
        > = MokaEvictingMap::with_anchor(&cfg, SystemTime::now());

        // Initial state: flag is false.
        assert!(
            !map.has_callbacks_flag.load(Ordering::Relaxed),
            "flag should start false (no callbacks registered)"
        );

        map.insert(1, BytesEntry(10)).await;

        // Many reads with no callbacks — flag must remain false.
        for _ in 0..50 {
            let v = map.get(&1).await;
            assert!(v.is_some());
        }
        assert!(
            !map.has_callbacks_flag.load(Ordering::Relaxed),
            "flag must remain false until add_item_callback is called"
        );

        // Now register a callback — flag must flip to true and reads
        // must fire on_get.
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);
        assert!(
            map.has_callbacks_flag.load(Ordering::Relaxed),
            "add_item_callback must publish has_callbacks_flag=true"
        );

        let _ = map.get(&1).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            1,
            "on_get should fire once a callback is registered"
        );
    }

    // ---------------------------------------------------------------
    // 5. on_get is NOT fired from a pinned-hit read
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_for_pinned_hit() {
        // Pinned entries are a worker-side staging concept — they
        // already broadcast as pinned to the locality_map. Double-firing
        // on_get for pinned reads would be redundant churn.
        let cfg = policy(100 * 1024, 0);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        map.insert(1, BytesEntry(2048)).await;
        assert!(map.pin_key(1), "pin should succeed");

        // Read while pinned.
        let v = map.get(&1).await;
        assert!(v.is_some(), "pinned key should still be readable");
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "pinned-hit reads must not fire on_get"
        );

        // get_many path — same expectation for pinned hits.
        drop(map.get_many(&[1u64]).await);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "pinned-hit reads must not fire on_get from get_many either"
        );

        // Cleanup so eviction-listener side-effects don't fire under teardown.
        map.unpin_key(&1);
    }

    // ---------------------------------------------------------------
    // 6. expire_stale_pins fires on_insert (NOT eviction callbacks).
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn expire_stale_pins_fires_on_insert_not_eviction() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let insert_count = Arc::clone(&cb.insert_count);
        let removal_count = Arc::clone(&cb.removal_count);
        let pin_expired_count = Arc::clone(&cb.pin_expired_count);
        map.add_item_callback(cb);

        // Insert + pin.
        map.insert(1, BytesEntry(2048)).await;
        assert!(map.pin_key(1), "pin should succeed");
        let baseline_inserts = insert_count.load(Ordering::Relaxed);

        // Force a stale pin by rewinding pinned_at past PIN_TIMEOUT_SECS.
        // We bypass the public API — directly mutate the DashMap entry.
        {
            let mut entry = map
                .pinned
                .get_mut(&1u64)
                .expect("key 1 should be pinned");
            // Roll back pinned_at by enough to exceed the timeout.
            entry.pinned_at = Instant::now()
                - core::time::Duration::from_secs(PIN_TIMEOUT_SECS + 1);
            // Sanity: ensure we built a valid PinnedEntry with the same
            // size we put in (sanity-checks the test setup, not the SUT).
            let _: &PinnedEntry<BytesEntry> = &*entry;
        }

        // Run the expiry sweep directly — no need to wait 10s for the
        // background ticker.
        map.expire_stale_pins().await;

        // The blob should now be back in the cache (not in pinned map).
        assert_eq!(map.pinned_bytes(), 0, "pin should be cleared");
        assert!(
            map.get(&1).await.is_some(),
            "blob should still be reachable from cache after pin-expiry"
        );

        // expire_stale_pins must fire on_insert exactly once for the
        // re-announced blob, and must NOT fire the removal callback
        // (pin-expiry is not an eviction).
        assert_eq!(
            insert_count.load(Ordering::Relaxed),
            baseline_inserts + 1,
            "pin-expiry should fire on_insert once for the re-announced blob"
        );
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "pin-expiry must NOT fire the removal callback",
        );
        // Durability hook: pin auto-expiry MUST fire on_pin_expired so
        // FastSlowStore can record the digest as failed_slow_writes.
        // Without this, a slow-write that hangs longer than the pin TTL
        // silently downgrades the blob from "pending upload retry" to
        // "evictable / no retry tracking" — the original bug at
        // worker-08 2026-04-23T00:08:53.
        assert_eq!(
            pin_expired_count.load(Ordering::Relaxed),
            1,
            "pin-expiry MUST fire on_pin_expired exactly once",
        );
    }

    /// #605 regression test: a startup load past the cap stays past the cap
    /// (no eviction listener fires) until `run_pending_tasks_and_drain` is
    /// called, after which moka enforces the byte cap synchronously. Models
    /// what `FilesystemStore::new` does after `add_files_to_cache`.
    ///
    /// Mutation step: revert `run_pending_tasks_and_drain` to just
    /// `drain_pending_evictions()` (drop the `cache.run_pending_tasks()`
    /// call) — the post-drain assertion red-fails because moka was never
    /// asked to enforce the cap.
    #[tokio::test]
    async fn run_pending_tasks_and_drain_evicts_startup_overshoot() {
        let cfg = policy(100, 0); // 100-byte cap, no count cap.
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);

        // Simulate startup load: 20 entries of 10 bytes each = 200 bytes (2x cap).
        // insert_with_time is the path `FilesystemStore::add_files_to_cache` uses;
        // by contract (insert_startup doc-comment) it skips
        // `cache.run_pending_tasks()` for throughput.
        for k in 0..20u64 {
            map.insert_with_time(k, BytesEntry(10), 0).await;
        }

        // BEFORE the drain: nothing has kicked moka's capacity check, so the
        // eviction listener has NOT fired. This IS the production failure
        // mode (worker-05: 124 GiB on 40 GiB cap, zero evict log lines).
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "#605 precondition: insert_with_time must NOT fire eviction (the bug being fixed)",
        );

        // The fix: run moka's capacity check + drain the listener queue
        // (looped until idle so the bounded per-call eviction doesn't leave
        // the cache above cap).
        map.run_pending_tasks_and_drain().await;

        // AFTER the drain: the listener fired AND the cache is at-or-below
        // cap. The cap assertion is the load-bearing one — a single
        // `run_pending_tasks()` is bounded by
        // `DEFAULT_EVICTION_BATCH_SIZE`, so for a 2×-overshoot it can
        // produce SOME evictions yet leave the cache still over. Only the
        // loop-until-idle behaviour guarantees the end-state invariant.
        let evicted = removal_count.load(Ordering::Relaxed);
        assert!(
            evicted > 0,
            "#605 fix: run_pending_tasks_and_drain must fire the eviction listener for over-cap entries (got 0; cap not being enforced)",
        );
        let remaining = map.cache.entry_count();
        let remaining_bytes = remaining * 10; // 10 = BytesEntry size
        assert!(
            remaining_bytes <= 100,
            "#605 fix: post-drain cache must be at-or-below cap (got {remaining} entries × 10 B = {remaining_bytes} B, cap = 100 B). \
             A single run_pending_tasks() exits at moka's eviction-batch ceiling; only the loop in run_pending_tasks_and_drain brings the cache fully under cap.",
        );
    }

    /// #605 large-overshoot property test: post-drain cache must satisfy
    /// `entry_count × size ≤ cap` even for an overshoot WAY above the
    /// per-iteration eviction-batch ceiling. This test inserts 2000 ×
    /// 1-byte entries into a 100-byte cap (20×, well above moka's
    /// `DEFAULT_EVICTION_BATCH_SIZE` ≈ 384).
    ///
    /// NOTE on the loop: a single-iteration mutation of the helper
    /// (one `run_pending_tasks` + one drain, no loop) PASSES this test
    /// on debug-build infrastructure. Moka's internal
    /// `do_run_pending_tasks` itself loops `evict_lru_entries` until
    /// drained OR the 100 ms maintenance-task timeout fires, so for a
    /// 2000 × 1 B workload the inner loop drains the queue in a single
    /// outer call. The outer loop in `run_pending_tasks_and_drain`
    /// becomes load-bearing only at production scale (~30–80 GiB
    /// overshoot at ~10–100 KiB file granularity = ~1M–10M eviction
    /// events, where the 100 ms inner-timeout CAN fire). This test
    /// guards the END-STATE PROPERTY (cap-enforced) — which holds with
    /// or without the loop on this workload size — not the loop's
    /// necessity per se.
    #[tokio::test]
    async fn run_pending_tasks_and_drain_loops_for_large_overshoot() {
        let cfg = policy(100, 0); // 100-byte cap.
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        map.add_item_callback(cb);

        // 2000 × 1-byte entries = 2000 B (20× cap), well above moka's
        // per-call eviction batch (~384). At this scale moka's inner
        // re-loop drains in one outer call; at production scale it
        // wouldn't, which is when the helper's outer loop matters.
        for k in 0..2000u64 {
            map.insert_with_time(k, BytesEntry(1), 0).await;
        }

        map.run_pending_tasks_and_drain().await;

        // Cache must be ≤ cap. With 1-byte entries, cap=100 means at
        // most 100 entries remain.
        let remaining = map.cache.entry_count();
        assert!(
            remaining <= 100,
            "#605 fix loop: post-drain cache must be at-or-below cap for overshoots > eviction-batch ceiling \
             (got {remaining} entries × 1 B = {remaining} B, cap = 100 B). A single run_pending_tasks call \
             is bounded by DEFAULT_EVICTION_BATCH_SIZE; only the loop in run_pending_tasks_and_drain brings \
             the cache fully under cap.",
        );
    }

    /// #605 (F3b) composite-invariant regression: the worker
    /// `FilesystemStore`'s eviction corner must drive the moka cache
    /// under cap *during runtime* (not just at startup), via the
    /// periodic forced-drain arm wired into the live `drain_evictions`
    /// `select!` loop. This is what closes the real #605 overshoot
    /// (worker-06 hit ~162 GB on a 40 GiB cap): moka's weight-based
    /// eviction is EVENTUALLY-CONSISTENT and only kicks its capacity
    /// check on `insert` / explicit `run_pending_tasks`, so a cache
    /// that took its overshoot via `insert_with_time` (the startup
    /// path, which defers `run_pending_tasks` for throughput) and then
    /// sees no further runtime `insert` trails the cap indefinitely.
    ///
    /// Composite invariant (admission/eviction/pin triangle,
    /// `.claude/rules/admission-eviction-pin.md`):
    ///   `cap-set ⇒ (periodic forced-drain converges weighted_size→cap
    ///              OR admission rejects)`.
    /// This test DEGRADES TWO corners and proves the third compensates:
    ///   - admission gate: ABSENT (a bare `MokaEvictingMap` has no
    ///     `ResourceExhausted` admission path — over-cap `insert_with_time`
    ///     always succeeds via the LRU policy), AND
    ///   - pin: PRESENT (one entry is pinned INDEFINITELY — the
    ///     pending-BIS-ack case, the strongest pin, EXEMPT from the TTL
    ///     sweep).
    /// With admission gone, the eviction corner (the new periodic drain)
    /// is the SOLE defense and MUST bring the UNPINNED bytes under cap
    /// while leaving the pinned bytes untouched.
    ///
    /// Determinism: `start_paused = true` + `current_thread` freezes the
    /// clock; the test fires the `drain_interval` tick by explicitly
    /// `advance`-ing past `DRAIN_INTERVAL` and yields the single runtime
    /// thread so the spawned `drain_evictions` task runs the arm to
    /// completion. No wall-clock sleep is used for synchronization — the
    /// advance is deterministic and the bounded iteration count is the
    /// deadlock detector.
    ///
    /// Mutation step 1 (remove the new arm): delete the
    /// `drain_interval.tick()` arm from `drain_evictions` → the cache
    /// never converges, the bounded advance loop exhausts, and the
    /// post-loop assertion red-fails with the bespoke "#605: periodic
    /// forced-drain missing — cache stays over cap" message.
    /// Mutation step 2 (drain evicts pinned): make the forced drain able
    /// to evict pinned entries → the pin-survival assertion red-fails.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn periodic_forced_drain_arm_converges_unpinned_under_cap() {
        // moka's effective max_capacity in the same KB-WEIGHT units the
        // weigher produces: `(max_bytes / 1024).max(1) = (100/1024).max(1)
        // = 1`. Each 10-byte entry weighs `10.div_ceil(1024) = 1`
        // KB-WEIGHT, so the cap holds at most ONE unpinned resident entry.
        // Convergence is asserted via the EVICTION LISTENER firing (see the
        // advance loop below), NOT via `weighted_size()`/`entry_count()`:
        // moka's size accessors only refresh after a `run_pending_tasks`,
        // which we deliberately never call from the test (calling it would
        // BE the fix's job and defeat the precondition).
        let cfg = policy(100, 0);
        let effective_capacity = 1u64; // matches with_anchor's floor-at-1.
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);

        // Pin ONE entry INDEFINITELY (the pending-BIS-ack F2 case) BEFORE
        // taking the overshoot. Order matters: `pin_key_with_mode` ends
        // with `cache.invalidate(key); cache.run_pending_tasks()`
        // (moka_evicting_map.rs:1227-1228) — that `run_pending_tasks`
        // WOULD enforce the cap. Pinning while the cache is small (1 entry
        // at cap) keeps that enforcement a no-op, and MOVES the pinned
        // entry OUT of the moka cache into the side `pinned` DashMap, so by
        // construction the later forced drain — which only operates on
        // `self.cache` — cannot evict it.
        map.insert(0u64, BytesEntry(10)).await;
        assert!(
            map.pin_key_indefinite(0),
            "indefinite pin should succeed for a present key"
        );

        // NOW take the overshoot via the STARTUP path (`insert_with_time`
        // → `insert_startup`), which by contract skips `run_pending_tasks`
        // for throughput (moka_evicting_map.rs:850, "deferred to caller").
        // 19 × 10-byte entries = 19 KB-WEIGHT, 19× the 1-unit cap, with
        // NOTHING calling `run_pending_tasks`. This is the production
        // failure precondition: moka has NOT enforced the cap and (absent
        // a runtime insert) never will on its own.
        for k in 1..20u64 {
            map.insert_with_time(k, BytesEntry(10), 0).await;
        }

        // Precondition: moka has NOT enforced the cap yet — the eviction
        // listener has fired ZERO times. This is the exact production
        // failure mode (worker-06: 162 GB on a 40 GiB cap, zero evict
        // log lines): the startup load via `insert_with_time` deferred
        // `run_pending_tasks`, so moka is over cap and stays there. We
        // do NOT call `run_pending_tasks` here — that would BE the fix and
        // defeat the test.
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "#605 precondition: insert_with_time must NOT have fired eviction \
             (moka is over cap and un-enforced — the runtime-overshoot being \
             fixed)",
        );

        // Start the live background loop (the production composition:
        // `FilesystemStore::new` calls this after the startup load).
        map.start_background_eviction();

        // Drive the periodic drain arm deterministically: advance past
        // `DRAIN_INTERVAL` to fire `drain_interval.tick()`, then yield the
        // single runtime thread so the spawned task runs the drain. Bound
        // the loop (deadlock detector); wrap in a `timeout` that becomes a
        // REAL detector because we advance the clock each iteration.
        //
        // Convergence signal: the EVICTION LISTENER firing
        // (`removal_count`), NOT `entry_count()`/`weighted_size()`. moka's
        // size accessors are eventually-consistent (they only refresh after
        // a `run_pending_tasks`) and can transiently read low even when no
        // eviction has happened — so they are NOT arm-dependent and would
        // green spuriously. The eviction listener fires ONLY when a moka op
        // actually runs pending tasks; with the startup load un-enforced and
        // NO runtime insert, the ONLY thing that runs pending tasks is the
        // periodic drain arm. 19 unpinned entries over a 1-unit cap MUST
        // evict 18 (the cap keeps exactly one), so `removal_count == 18` is
        // the deterministic, fully arm-dependent end-state.
        const EXPECTED_EVICTIONS: u64 = 18; // 19 unpinned − 1 kept at cap.
        const MAX_TICKS: u32 = 50;
        let drain_period = core::time::Duration::from_secs(super::DRAIN_INTERVAL_SECS);
        // Deadlock-detector budget must exceed the bounded advance loop's
        // total virtual time (`MAX_TICKS × DRAIN_INTERVAL` = 500 s) so that
        // the BOUNDED LOOP — not this timeout — is what trips when the
        // drain arm is absent (giving the bespoke convergence message, not
        // a misleading "deadlock"). Under `start_paused` this virtual
        // budget costs zero wall-clock; it only fires if a real hang stalls
        // the advance loop itself.
        let converged = tokio::time::timeout(core::time::Duration::from_secs(3600), async {
            for _ in 0..MAX_TICKS {
                tokio::time::advance(drain_period).await;
                // Let the spawned `drain_evictions` task run the arm to
                // completion (all its awaits resolve immediately on the
                // test callback). A few yields guarantee progress on the
                // current-thread runtime regardless of `select!` ordering.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                if removal_count.load(Ordering::Relaxed) >= EXPECTED_EVICTIONS {
                    return true;
                }
            }
            false
        })
        .await
        .expect("deadlock: forced-drain convergence loop did not finish");

        assert!(
            converged,
            "#605: periodic forced-drain missing — cache stays over cap \
             (eviction listener fired {} times, expected {} after {} drain \
             ticks). The `drain_evictions` loop must call \
             `run_pending_tasks_and_drain` on a periodic tick so moka's \
             eventually-consistent eviction converges under sustained \
             runtime ingest.",
            removal_count.load(Ordering::Relaxed),
            EXPECTED_EVICTIONS,
            MAX_TICKS,
        );

        // The UNPINNED resident entries converged to ≤ cap (exactly one
        // unpinned entry survives the 1-unit cap). `entry_count()` is
        // reliable HERE because the drain just ran `run_pending_tasks`.
        assert!(
            map.cache.entry_count() <= effective_capacity,
            "#605: post-drain UNPINNED entry_count must be ≤ cap \
             (got {}, cap {})",
            map.cache.entry_count(),
            effective_capacity,
        );

        // The INDEFINITELY-PINNED entry MUST survive the forced drain —
        // pinning moved it out of moka; the drain only touches the cache.
        assert!(
            map.pinned.contains_key(&0u64),
            "#605 pin-survival: indefinitely-pinned (pending-BIS) entry was \
             evicted by the periodic forced-drain — the drain must only \
             evict UNPINNED cache entries, never the side `pinned` map"
        );
        assert_eq!(
            map.pinned_bytes(),
            10,
            "#605 pin-survival: pinned byte accounting must be intact after \
             the forced drain (the pinned blob is held until BIS-ack, never \
             dropped by eviction)"
        );
        assert!(
            map.get(&0).await.is_some(),
            "#605 pin-survival: indefinitely-pinned blob must remain \
             reachable after the periodic forced-drain"
        );

        map.unpin_key(&0);
    }

    // ---------------------------------------------------------------
    // 7. on_pin_expired fires ONLY on pin-expiry (not on insert/get/
    //    eviction). Guards the durability invariant: false positives
    //    cause spurious reuploads; false negatives cause data loss.
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_pin_expired_fires_only_on_pin_expiry() {
        let cfg = policy(64, 0); // tiny: forces eviction
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let pin_expired_count = Arc::clone(&cb.pin_expired_count);
        map.add_item_callback(cb);

        // Insert → must NOT fire on_pin_expired.
        map.insert(1, BytesEntry(8)).await;
        assert_eq!(pin_expired_count.load(Ordering::Relaxed), 0);

        // get → must NOT fire on_pin_expired.
        let _ = map.get(&1).await;
        assert_eq!(pin_expired_count.load(Ordering::Relaxed), 0);

        // Force eviction by overflowing capacity. Insert several entries
        // larger than the map's max_bytes (64) so moka evicts.
        for k in 2..=10 {
            map.insert(k, BytesEntry(32)).await;
        }
        // Allow moka's internal pending tasks to drain.
        map.cache.run_pending_tasks();
        // Eviction must NOT fire on_pin_expired.
        assert_eq!(
            pin_expired_count.load(Ordering::Relaxed),
            0,
            "eviction must NOT fire on_pin_expired",
        );
    }

    // ---------------------------------------------------------------
    // FL-681 Fix A: indefinite (pinned-until-durable) pins are EXEMPT
    // from the PIN_TIMEOUT_SECS sweep. A worker-local F2 output blob's
    // anti-eviction pin must be released ONLY by the server's BIS-ack
    // (unpin), never by the 120s TTL — matching how in-memory mirror
    // blobs are already pinned indefinitely (the mirror-TTL sweeper was
    // removed in local_worker.rs for exactly this reason). This is the
    // core regression for the 3,881-event silent-loss leak.
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn f2_pin_survives_past_ttl_until_bis() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        map.insert(1, BytesEntry(2048)).await;
        // Pin INDEFINITELY (the F2 output-blob pin).
        assert!(
            map.pin_key_indefinite(1),
            "indefinite pin should succeed for a present key"
        );

        // Drive time past the TTL deadline: rewind pinned_at past
        // PIN_TIMEOUT_SECS so the sweep WOULD demote a normal pin.
        {
            let mut entry = map
                .pinned
                .get_mut(&1u64)
                .expect("key 1 should be pinned");
            entry.pinned_at = Instant::now()
                - core::time::Duration::from_secs(PIN_TIMEOUT_SECS + 1);
        }

        // Run the sweep with NO BIS-ack. The indefinite pin MUST survive.
        map.expire_stale_pins().await;

        assert_eq!(
            map.pinned_bytes(),
            2048,
            "blob evicted before BIS-durable: indefinite F2 pin was swept by the 120s TTL — \
             this is the 3,881-event silent-loss leak (FL-681 Fix A)"
        );
        assert!(
            map.pinned.contains_key(&1u64),
            "blob evicted before BIS-durable: indefinite pin entry removed by TTL sweep"
        );
        assert!(
            map.get(&1).await.is_some(),
            "blob evicted before BIS-durable: indefinitely-pinned F2 output unreachable after TTL sweep"
        );

        map.unpin_key(&1);
    }

    #[tokio::test]
    async fn bis_ack_releases_indefinite_pin() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        map.insert(1, BytesEntry(2048)).await;
        assert!(map.pin_key_indefinite(1), "indefinite pin should succeed");

        // Rewind past the TTL to prove the sweep is a no-op for indefinite
        // pins (the pin is held by BIS-ack semantics, not time).
        {
            let mut entry = map
                .pinned
                .get_mut(&1u64)
                .expect("key 1 should be pinned");
            entry.pinned_at = Instant::now()
                - core::time::Duration::from_secs(PIN_TIMEOUT_SECS + 1);
        }
        map.expire_stale_pins().await;
        assert_eq!(map.pinned_bytes(), 2048, "indefinite pin must survive sweep");

        // Deliver the BIS-ack: unpin releases the indefinite pin and the
        // blob becomes LRU-evictable again (back in cache, pinned_bytes 0).
        map.unpin_key(&1);
        assert_eq!(
            map.pinned_bytes(),
            0,
            "BIS-ack (unpin) must release the indefinite pin so the blob is evictable"
        );
        assert!(
            !map.pinned.contains_key(&1u64),
            "BIS-ack must remove the indefinite pin entry from the pinned map"
        );
        assert!(
            map.get(&1).await.is_some(),
            "released blob must remain reachable from the LRU cache after BIS-ack"
        );
    }

    #[tokio::test]
    async fn indefinite_pin_cap_backpressures_not_drops() {
        // Cap indefinite pins at 4096 bytes. The third 2048-byte indefinite
        // pin would push the indefinite total to 6144 > 4096 and must be
        // REFUSED (backpressure) — NOT silently dropped or accepted.
        let cfg = policy(1024 * 1024, 0);
        let map = Arc::new(make_map_cb_indefinite_cap(&cfg, 4096));

        for k in 0..3u64 {
            map.insert(k, BytesEntry(2048)).await;
        }

        assert!(map.pin_key_indefinite(0), "first indefinite pin fits under cap");
        assert!(map.pin_key_indefinite(1), "second indefinite pin fills cap exactly");
        // Third exceeds cap → refused. The blob is NOT dropped (still in
        // cache, normally evictable) and the caller treats `false` as
        // backpressure: keep the source readable + retry.
        assert!(
            !map.pin_key_indefinite(2),
            "indefinite-pin cap exceeded must REFUSE (backpressure), not silently accept"
        );
        assert_eq!(
            map.indefinite_pinned_bytes(),
            4096,
            "over-cap indefinite pin must not be accounted (refused, not dropped)"
        );
        // The refused blob is still present in the map — NOT lost.
        assert!(
            map.get(&2).await.is_some(),
            "backpressure must NOT drop the blob: refused-pin source stays readable for retry"
        );

        // Releasing one indefinite pin (BIS-ack) frees cap; the previously
        // refused pin now succeeds — proving backpressure is transient, not
        // terminal.
        map.unpin_key(&0);
        assert!(
            map.pin_key_indefinite(2),
            "after a BIS-ack frees cap headroom, the backpressured pin must succeed"
        );

        map.unpin_key(&1);
        map.unpin_key(&2);
    }

    // FL-681 Fix A fix-up (MAJOR-1a): the explicit-remove path
    // (`remove()` / `remove_if()`) must keep the indefinite-pin accounting
    // symmetric, exactly as `unpin_key`. `remove()` is reachable on an
    // indefinite-pinned (F2 output) key via
    // `FilesystemStore::remove_entry_for_digest` + stale/zero-byte
    // eviction. If `remove()` decrements only `pinned_bytes` and not
    // `indefinite_pinned_bytes`, the indefinite total leaks monotonically
    // upward, the cap saturates, and every new F2 output silently falls
    // back to a time-bounded pin — re-opening the 120s-TTL leak.
    #[tokio::test]
    async fn remove_of_indefinite_pin_restores_indefinite_bytes() {
        // Cap indefinite pins at 4096 bytes so a leak is observable: after
        // removing the first indefinite key, a third 2048-byte indefinite
        // pin must fit (proving the cap headroom was reclaimed).
        let cfg = policy(1024 * 1024, 0);
        let map = Arc::new(make_map_cb_indefinite_cap(&cfg, 4096));

        for k in 0..3u64 {
            map.insert(k, BytesEntry(2048)).await;
        }
        assert!(map.pin_key_indefinite(0), "first indefinite pin fits under cap");
        assert!(map.pin_key_indefinite(1), "second indefinite pin fills cap exactly");
        assert_eq!(
            map.indefinite_pinned_bytes(),
            4096,
            "two 2048-byte indefinite pins should account for exactly 4096 bytes"
        );

        // Explicitly remove an indefinite-pinned key (the
        // `remove_entry_for_digest` / stale-eviction path). This MUST
        // decrement `indefinite_pinned_bytes`, not just `pinned_bytes`.
        assert!(
            map.remove(&0u64).await,
            "remove() of a pinned key should report it was removed"
        );
        assert_eq!(
            map.indefinite_pinned_bytes(),
            2048,
            "indefinite_pinned_bytes leak: remove() of an indefinite-pinned key did not free \
             its indefinite-cap headroom — the cap will saturate and re-open the 120s-TTL leak"
        );
        assert_eq!(
            map.pinned_bytes(),
            2048,
            "remove() must also decrement the total pinned_bytes for the removed entry"
        );

        // Falsification: the leaked headroom must be REUSABLE. With the cap
        // at 4096 and only key 1 (2048) still indefinitely pinned, a fresh
        // 2048-byte indefinite pin of key 2 must now fit. If remove() leaked
        // the indefinite accounting, the cap would still read 4096 and this
        // pin would be wrongly refused.
        assert!(
            map.pin_key_indefinite(2),
            "indefinite_pinned_bytes leak: cap headroom freed by remove() was not reusable — \
             a new F2 output is refused an indefinite pin and falls back to the 120s TTL"
        );

        map.unpin_key(&1);
        map.unpin_key(&2);
    }

    // FL-681 Fix A fix-up (MAJOR-1a) sibling: `remove_if()` delegates to
    // `remove()`, so the same indefinite-accounting symmetry must hold when
    // the conditional removal predicate fires.
    #[tokio::test]
    async fn remove_if_of_indefinite_pin_restores_indefinite_bytes() {
        let cfg = policy(1024 * 1024, 0);
        let map = Arc::new(make_map_cb_indefinite_cap(&cfg, 4096));

        map.insert(0u64, BytesEntry(2048)).await;
        assert!(map.pin_key_indefinite(0), "indefinite pin should succeed");
        assert_eq!(map.indefinite_pinned_bytes(), 2048);

        // remove_if with a predicate that fires routes through remove().
        assert!(
            map.remove_if(&0u64, |_| true).await,
            "remove_if() with a true predicate should remove the pinned key"
        );
        assert_eq!(
            map.indefinite_pinned_bytes(),
            0,
            "indefinite_pinned_bytes leak via remove_if(): the conditional-remove path did not \
             free indefinite-cap headroom"
        );
    }

    // FL-681 Follow-up A (MAJOR-1b close-out): the admission-side gate reads
    // `indefinite_pin_saturated()` to decide whether to NAK a new action with
    // `ResourceExhausted`. The predicate is the snapshot mirror of
    // `indefinite_cap_admits(0)`: TRUE when there is NO indefinite-cap headroom
    // left for even a zero-byte blob, FALSE while any headroom remains.
    #[tokio::test]
    async fn indefinite_pin_saturated_tracks_cap_headroom() {
        // Cap indefinite pins at 4096 bytes.
        let cfg = policy(1024 * 1024, 0);
        let map = Arc::new(make_map_cb_indefinite_cap(&cfg, 4096));

        for k in 0..2u64 {
            map.insert(k, BytesEntry(2048)).await;
        }

        // No indefinite pins yet → headroom exists → NOT saturated.
        assert!(
            !map.indefinite_pin_saturated(),
            "empty indefinite-pin set must report headroom (not saturated)"
        );

        assert!(map.pin_key_indefinite(0), "first indefinite pin fits under cap");
        // 2048 of 4096 used → still headroom → NOT saturated.
        assert!(
            !map.indefinite_pin_saturated(),
            "indefinite_pin_saturated must be FALSE while indefinite-cap headroom remains — \
             the admission gate would wrongly NAK actions and churn the scheduler"
        );

        assert!(map.pin_key_indefinite(1), "second indefinite pin fills cap exactly");
        // 4096 of 4096 used → no headroom → SATURATED.
        assert!(
            map.indefinite_pin_saturated(),
            "indefinite_pin_saturated must be TRUE once the indefinite cap is full — \
             without it the admission gate never fires and fresh F2 outputs are lost"
        );

        // BIS-ack release of one pin reclaims headroom → de-saturates.
        map.unpin_key(&0);
        assert!(
            !map.indefinite_pin_saturated(),
            "indefinite_pin_saturated must clear once a BIS-ack frees cap headroom — \
             the gate is transient backpressure, not a terminal stall"
        );

        map.unpin_key(&1);
    }

    // FL-681 Follow-up B (MAJOR-2 robust close-out): the worker re-advertises
    // its pending-BIS CAS pin set on a periodic heartbeat. The enumeration MUST
    // return EXACTLY the `indefinite == true` subset of the pinned map — a
    // time-bounded pin is NOT pending-BIS and must be excluded, and an
    // unpinned entry must drop out (self-pruning) so the heartbeat shrinks as
    // BIS-acks land.
    #[tokio::test]
    async fn indefinite_pinned_digests_enumerates_only_indefinite_subset() {
        let cfg = policy(1024 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        for k in 0..3u64 {
            map.insert(k, BytesEntry(2048)).await;
        }

        // Mixed pin states: key 0 indefinite, key 1 time-bounded, key 2 unpinned.
        assert!(map.pin_key_indefinite(0), "indefinite pin of key 0 should succeed");
        assert!(map.pin_key(1), "time-bounded pin of key 1 should succeed");

        let enumerated = map.indefinite_pinned_digests();
        assert_eq!(
            enumerated,
            vec![0u64],
            "indefinite_pinned_digests must return EXACTLY the indefinite subset — a \
             time-bounded pin (key 1) is not pending-BIS and an unpinned key (key 2) must \
             not be re-advertised, or the heartbeat would re-drive mark_stable for blobs that \
             were never pending durability"
        );

        // BIS-ack releases key 0 → it must drop out of the next enumeration
        // (self-pruning: the heartbeat shrinks as acks land).
        map.unpin_key(&0);
        assert!(
            map.indefinite_pinned_digests().is_empty(),
            "after a BIS-ack unpin, the released digest must NOT appear in the heartbeat set — \
             the pending-BIS re-advertisement is self-pruning"
        );

        map.unpin_key(&1);
    }

    // FL-681 Follow-up A: when no byte budget is configured (`max_bytes == 0`)
    // the gate must NEVER fire — mirroring `indefinite_cap_admits`'s
    // `max_bytes == 0 => admit` short-circuit. An uncapped store has no
    // indefinite-pin cap to saturate.
    #[tokio::test]
    async fn indefinite_pin_saturated_never_fires_when_uncapped() {
        // max_bytes == 0 ⇒ no byte budget ⇒ no cap to saturate.
        let cfg = policy(0, 100);
        let map = Arc::new(make_map_cb(&cfg));

        map.insert(0u64, BytesEntry(2048)).await;
        assert!(map.pin_key_indefinite(0), "indefinite pin should succeed when uncapped");
        assert!(
            !map.indefinite_pin_saturated(),
            "an uncapped store (max_bytes == 0) has no indefinite-pin cap and must never \
             report saturated — gating it would wedge a store the cap does not govern"
        );

        map.unpin_key(&0);
    }

    // ---------------------------------------------------------------
    // FL-688 v3 Stage C — drain-tick suppressed while startup
    // reconcile gate is armed; converges after release.
    //
    // Invariant: the periodic forced-drain arm MUST be skipped while
    // `reconcile_complete == false` (gate armed). Premature drain evicts
    // blobs before `reconcile_pin` can protect them, producing data loss
    // at startup.
    //
    // Mutation: remove the `if !self.reconcile_complete.load(Ordering::Acquire)`
    // + `continue` guard at `moka_evicting_map.rs:1811-1812`. Without it,
    // drain fires during the gate-armed phase → 18 evictions observed →
    // the "drain-tick suppression regression" assertion fires.
    // ---------------------------------------------------------------

    /// Gate armed → drain-tick `continue`s (0 evictions).
    /// Gate released → drain-tick fires → cache converges (≥ 1 eviction).
    ///
    /// Uses the same setup as `periodic_forced_drain_arm_converges_unpinned_under_cap`
    /// (19 entries at 1-unit cap, `start_paused` clock) but arms the startup
    /// reconcile gate BEFORE starting `start_background_eviction`. No eviction
    /// must occur while armed. After release, convergence must occur.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_tick_suppressed_while_startup_reconcile_gate_armed() {
        // Same sizing as the convergence test: 19 over-cap entries + 1-unit cap.
        let cfg = policy(100, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);

        // Take the overshoot via the startup path.
        for k in 0..19u64 {
            map.insert_with_time(k, BytesEntry(10), 0).await;
        }

        // ARM the gate: sets reconcile_complete = false → drain tick will
        // `continue` without running `run_pending_tasks_and_drain`.
        map.set_startup_reconcile_gate();

        // Start the background loop AFTER arming so the gate is definitely
        // set before the first drain tick fires.
        map.start_background_eviction();

        let drain_period = core::time::Duration::from_secs(super::DRAIN_INTERVAL_SECS);

        // Phase 1: Advance 10 drain ticks while gate armed. The drain arm
        // must `continue` every time — ZERO evictions expected.
        const ARMED_TICKS: u32 = 10;
        let gated_drain_result = tokio::time::timeout(
            core::time::Duration::from_secs(3600),
            async {
                for _ in 0..ARMED_TICKS {
                    tokio::time::advance(drain_period).await;
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                }
            },
        )
        .await;
        assert!(gated_drain_result.is_ok(), "deadlock: gate-armed advance loop hung");

        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "drain-tick suppression regression: {} evictions occurred while startup \
             reconcile gate was armed (expected 0). MUTATION target: the \
             `if !self.reconcile_complete.load(Ordering::Acquire)` + `continue` \
             guard at moka_evicting_map.rs:1811-1812 prevents the periodic \
             forced-drain from running before reconcile-pin has protected all \
             worker blobs. Without it, blobs are evicted before being pinned.",
            removal_count.load(Ordering::Relaxed),
        );

        // Phase 2: Release the gate, then advance until drain converges.
        map.release_startup_reconcile_gate();

        const EXPECTED_EVICTIONS: u64 = 18; // 19 entries − 1 kept at 1-unit cap.
        const MAX_TICKS: u32 = 50;
        let converged = tokio::time::timeout(core::time::Duration::from_secs(3600), async {
            for _ in 0..MAX_TICKS {
                tokio::time::advance(drain_period).await;
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                if removal_count.load(Ordering::Relaxed) >= EXPECTED_EVICTIONS {
                    return true;
                }
            }
            false
        })
        .await
        .expect("deadlock: post-release convergence loop hung");

        assert!(
            converged,
            "drain-tick suppression regression (post-release): after \
             `release_startup_reconcile_gate` the periodic forced-drain must \
             converge (eviction listener fired {} times, expected ≥ {} after \
             {} drain ticks). Check that `release_startup_reconcile_gate` stores \
             `true` with `Ordering::Release` matching the `Ordering::Acquire` \
             load in the drain arm.",
            removal_count.load(Ordering::Relaxed),
            EXPECTED_EVICTIONS,
            MAX_TICKS,
        );
    }
}
