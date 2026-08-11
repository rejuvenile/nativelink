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
use core::fmt::{Debug, Display};
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
use tokio::sync::{Notify, mpsc};
use tracing::{info, warn};

use crate::background_spawn;
use crate::evicting_map::{ItemCallback, LenEntry, NoopCallback};
use crate::instant_wrapper::InstantWrapper;
use crate::metrics_utils::{Counter, CounterWithTime};

/// Maximum fraction of max_bytes that can be pinned (25%).
const PIN_CAP_FRACTION: f64 = 0.25;
/// FL-681 NAK boundary fix: `indefinite_pin_saturated` fires within
/// `pin_cap / PIN_SATURATION_HEADROOM_DIVISOR` of `pin_cap` — i.e. once
/// `real_pinned` is within 5% of the total pin budget. The band exists so the
/// admission gate NAKs a new action just BEFORE the total pin-cap refusal at
/// `pin_key_with_mode` starts rejecting variable-sized outputs, rather than only
/// at the exact-full boundary that a refused pin (which adds no bytes) can sit
/// just below forever. Divisor 20 = fire within 5% of `pin_cap`.
const PIN_SATURATION_HEADROOM_DIVISOR: u64 = 20; // fire within 5% of pin_cap
/// #speculative-prefetch P0: fraction of max_bytes reserved for SPECULATIVE
/// pins (5% — a fifth of the 25% total `pin_cap`). Speculative pins draw from
/// this small, DISJOINT sub-budget so they can never consume the headroom a
/// real-action pin needs (the C3/C5 starvation the invariant-prover
/// machine-checked; fix proven in `.claude/tla/SpeculativePinBudgetFixed.tla`).
/// 5% is conservative: on a 20 GiB worker that is ~1 GiB — enough for one cold
/// input tree's blob set (single-in-flight G5 bounds it to one speculative
/// construct at a time) yet small enough that the remaining ~4 GiB of the
/// 5 GiB `pin_cap` is untouched by speculation.
const SPECULATIVE_PIN_CAP_FRACTION: f64 = 0.05;
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
/// (FINDING 2 moka-eviction-wedge, 2026-07-28; upstream fix landed
/// 2026-08-11) Number of CONSECUTIVE drain-arm evaluations that must
/// observe the cache STRICTLY OVER `max_bytes` with ZERO size-eviction
/// progress (`size_evicted_items` unchanged) before the self-healing
/// fallback evictor fires.
///
/// **The bug this detects (moka-rs/moka#590), in two halves.** A deque
/// node can be ORPHANED — still linked in the probation deque, but no
/// longer reachable from the backing concurrent hash table (CHT):
///
///  * **Mint.** Writes reach the policy tier asynchronously over a
///    bounded `WriteOp` channel, so there is a window between a CHT
///    mutation and the enqueue of its op. If a key's CHT slot is removed
///    inside that window, the ops can drain `Remove`-then-`Upsert`; the
///    stale `Upsert` then reached `handle_admit` via an UNGUARDED
///    `handle_upsert` (0.12.15 `sync/base_cache.rs:1456`) and pushed a
///    deque node for an entry that no longer exists.
///  * **Permanence.** `evict_lru_entries` peeks the probation front and
///    tries a guarded `remove_if`, which fails for an orphan. It falls to
///    `skip_updated_entry_ao`, whose key-PRESENT branch calls
///    `move_to_back_ao_in_deque` — moving the MAP entry's node, never the
///    peeked orphan at the front. The next pass re-peeks the same node.
///    `more_to_evict` goes false, `do_run_pending_tasks` takes its
///    no-progress exit, and size eviction is dead for the life of the
///    process.
///
/// **Two corrections to our original internal diagnosis** (both were
/// wrong in the direction of making the bug look narrower than it is;
/// see `docs/moka-eviction-livelock-upstream-report.md`):
///  1. We had attributed the mint to a gen-checked `WriteOp::Remove`
///     skipping the unlink. It is not that: `handle_remove_without_timer_wheel`
///     branches on `is_admitted()` and unlinks by pointer, so genuine
///     removals are clean. The mint is the unguarded `handle_admit`.
///  2. The wedge is NOT specific to our pin path. We had concluded only
///     invalidate-then-reinsert could mint it. The reproducer's plain
///     `insert` mode stalls just as reliably with ZERO `invalidate` calls
///     — size eviction removes the CHT slot and plain key reuse supplies
///     the rest. ANY moka LRU cache with key reuse under eviction
///     pressure is exposed; our pin path (`pin_key_with_mode` =
///     `cache.invalidate`, `unpin_key` = bare `cache.insert`) made it
///     more FREQUENT, not uniquely possible.
///
/// **Fixed upstream in 0.12.16** (moka-rs/moka#592, released 2026-08-09;
/// we run `=0.12.16`). #592 kills the MINT: an `EntryInfo::is_retired`
/// flag is set inside the CHT's post-CAS `with_previous_entry` callback,
/// so it linearises with the bucket CAS that unlinked the entry, and
/// `handle_upsert`/`handle_admit` short-circuit on a retired entry
/// (0.12.16 `sync/base_cache.rs:1515` and `:1785`). Verified against our
/// workload by A/B reproducer: 0.12.15 → 6/6 stalled at 185-202x cap;
/// 0.12.16 → 6/6 converged to 1.00x.
///
/// **Why this sensor survives the fix, as a BACKSTOP.** #592 removed the
/// only known minting path; it did NOT change the permanence half.
/// `skip_updated_entry_ao` (0.12.16 `sync/base_cache.rs:2148`) and
/// `move_to_back_ao_in_deque` (`common/concurrent/deques.rs:95`) are
/// byte-identical to 0.12.15 — an orphan that reaches the probation
/// front by ANY future path still wedges eviction permanently and
/// silently. Post-bump, `eviction_wedge_selfheal_total > 0` is the ONLY
/// signal that would tell us the upstream fix is incomplete for our
/// workload (or that a different eviction bug exists); removing this
/// makes that unobservable. It is cheap when not firing.
///
/// Production impact that motivated it (fleet artifact 2026-07-28): 2 of
/// 10 workers at 3.36× over a 40 GB budget with evictions FROZEN for
/// 8h 36m at scrape time (the widely-quoted 4-day figure is the
/// disk-full/NAK duration, not the measured frozen window). The
/// frozen-evictions requirement is what prevents false-firing during
/// normal at-cap operation: a healthy at-cap cache under churn
/// size-evicts every tick, so the counter always advances and the
/// fallback never triggers.
const WEDGE_FROZEN_TICKS_TRIGGER: u64 = 3;
/// Bound on fallback key-walk rounds per ARM INVOCATION (metering,
/// review 9fd52fc0 pair-a MAJOR-4 / pair-b P3). Each round is capped at
/// `EVICT_SCAN_HARD_CAP` (10 000) evictions and each worker-FS eviction
/// unlinks a file, so an unbounded single pass would monopolize the
/// drain task for the whole heal — starving the `pin_check_interval`
/// arm (whose `MissedTickBehavior::Skip` DROPS missed pin-expiry
/// sweeps) and dumping the full eviction burst into the
/// BlobChangeTracker → BlobsAvailable delta path in one message. Two
/// rounds ≈ ≤20 000 evictions ≈ ≤5 chunks of the resend ring (cap 256)
/// per invocation; the heal then RE-ARMS on the next tick/kick via
/// `selfheal_resume` (no fresh frozen-tick accumulation) until the
/// observation converges under budget. Convergence sizing: the incident
/// worker (~66 000 evictions) heals in ~4 invocations ≈ 40 s; the
/// largest deployed map (server cas slow tier, ~3.4M entries,
/// `prod-server.json5:137`) at a hypothetical 20% overshoot (~680K
/// evictions) heals in ~34 invocations ≈ 6 min — each invocation
/// bounded, the loop servicing pin sweeps between them.
const WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION: u32 = 2;
/// Minimum interval between self-heal WARN emissions (duty-cycle
/// hygiene, review 9fd52fc0 pair-b C5). A pathological no-candidates
/// wedge re-fires every `WEDGE_FROZEN_TICKS_TRIGGER` evaluations
/// (~30 s → ~2 880 warns/day/store without a limit); 15 min bounds it
/// to ≤96/day while `eviction_wedge_selfheal_total` still counts every
/// firing and `eviction_wedge_detected` carries the persistent state.
const WEDGE_SELFHEAL_WARN_MIN_INTERVAL_MILLIS: u64 = 900_000;
/// (FINDING 2 piece 3) Minimum interval between ACCEPTED
/// [`MokaEvictingMap::kick_drain`] wakes of the background drain arm. The
/// disk-pressure NAK site kicks on every refused action; one drain pass
/// per few seconds is plenty (the periodic arm already runs every
/// `DRAIN_INTERVAL_SECS` anyway) and the limit keeps a NAK storm from
/// turning into a busy drain loop.
const DRAIN_KICK_MIN_INTERVAL_MILLIS: u64 = 5_000;
/// Diagnostic-only stale-pin alert threshold. An INDEFINITE pin
/// (worker-local F2 output blob, held until the server's
/// BlobsInStableStorage ack calls [`MokaEvictingMap::unpin_key`]) that is
/// still held past this age is surfaced by
/// [`MokaEvictingMap::sweep_stale_indefinite_pins`] as un-acked. Purely
/// observational — the sweep NEVER releases a pin (release stays
/// BIS-ack-driven). A sustained stale set is the signature of the
/// production leak where indefinite pins are never BIS-acked and
/// accumulate to `indefinite_pin_cap`.
const STALE_INDEFINITE_PIN_ALERT_SECS: u64 = 30;
/// Cap on individual per-pin WARN lines emitted in a single
/// [`MokaEvictingMap::sweep_stale_indefinite_pins`] sweep, so a large
/// pending-BIS backlog cannot flood the log. One summary WARN carrying the
/// full `stale_count` / `stale_bytes` totals is always emitted after the
/// (capped) per-pin lines.
const STALE_PIN_ALERT_MAX_LINES: usize = 50;
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
    /// #speculative-prefetch P0: when `true`, this pin was taken by a
    /// SPECULATIVE pre-fetch construct and its size is counted in
    /// `speculative_pinned_bytes` (a subset of `pinned_bytes`). Mutually
    /// exclusive with `indefinite` (speculative pins are TIME-BOUNDED, held
    /// against the small disjoint speculative sub-budget). The exclusion is
    /// ENFORCED, not just documented: if `pin_key_indefinite` upgrades a
    /// speculative entry, it RECLASSIFIES it spec→real (clears this flag +
    /// moves the bytes out of `speculative_pinned_bytes`), since an indefinite
    /// pin is a real pin (#speculative-prefetch C1). Used by `unpin_key` /
    /// `remove` to keep `speculative_pinned_bytes` symmetric on release.
    speculative: bool,
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
    /// (FINDING 2 fix-up, red-team alt-framing 2) moka's removal cause,
    /// captured by the eviction listener. `process_eviction_event`
    /// advances the wedge sensor (`size_evicted_items`) ONLY for
    /// `RemovalCause::Size` — the cause class the wedge actually kills.
    /// Counting all causes would let TTL expiries (`Expired`, e.g. the
    /// server targetkey store's `max_seconds: 604800`) or explicit
    /// removes permanently mask a size-eviction wedge.
    cause: RemovalCause,
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

/// Summary of one [`MokaEvictingMap::sweep_stale_indefinite_pins`] pass,
/// returned so tests can assert the detection contract deterministically
/// (the sweep itself only emits `warn!` diagnostics). Purely observational.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StalePinAlertSummary {
    /// INDEFINITE pins found held past `STALE_INDEFINITE_PIN_ALERT_SECS`.
    pub stale_count: u64,
    /// Sum of `PinnedEntry::size` over those stale pins.
    pub stale_bytes: u64,
    /// Per-pin WARN lines actually emitted (`stale_count`, capped at
    /// `STALE_PIN_ALERT_MAX_LINES`). Distinct from `stale_count` so the
    /// flood cap is independently testable.
    pub lines_emitted: usize,
}

/// (FINDING 2 moka-eviction-wedge) Outcome of one
/// [`MokaEvictingMap::maybe_selfheal_wedged_eviction`] evaluation,
/// returned so tests can assert each branch of the trigger predicate
/// with a bespoke message instead of inferring state from side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WedgeSelfHealOutcome {
    /// Startup reconcile gate armed — evaluation suppressed, identically
    /// to the drain tick itself (FL-688 v3 Stage C: the fallback must
    /// never race the reconcile-pin window).
    SuppressedReconcileGate,
    /// No byte budget configured (`max_bytes == 0`) — the trigger
    /// predicate can never hold.
    NoByteBudget,
    /// Observed weighted bytes AT-OR-UNDER `max_bytes` — within budget,
    /// trigger state reset (frozen-tick count zeroed, resume latch and
    /// wedge gauge cleared). Includes exact equality: the trigger is
    /// STRICT `>` (operator decision 2026-07-29 on review 9fd52fc0
    /// MAJOR-2) because at `weighted == max` moka owes zero eviction —
    /// a wedge is unobservable there, so firing would be a guaranteed
    /// false positive.
    UnderBudget,
    /// Strictly over budget but `size_evicted_items` advanced since the
    /// previous evaluation: SIZE eviction is making progress, no wedge.
    /// Trigger state reset. This is the branch that prevents
    /// false-firing during normal at-cap operation.
    ProgressObserved,
    /// Strictly over budget with frozen size-evictions, but below the
    /// `WEDGE_FROZEN_TICKS_TRIGGER` consecutive-evaluation threshold.
    /// Carries the frozen-tick count so far.
    Accumulating(u64),
    /// Trigger met (or a metered heal resumed via `selfheal_resume`) —
    /// the fallback key-walk evictor ran this invocation.
    Fired {
        /// Entries evicted across the rounds of THIS invocation.
        evicted_count: u64,
        /// Bytes (value `len()` sum) evicted across THIS invocation.
        evicted_bytes: u64,
        /// Key-walk rounds executed this invocation (each bounded by
        /// `EVICT_SCAN_HARD_CAP`; at most
        /// `WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION`).
        rounds: u32,
    },
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
    /// #speculative-prefetch P0: subset of `pinned_bytes` held by SPECULATIVE
    /// (pre-fetch) pins. Tracked separately so (a) the small speculative cap is
    /// enforced independently, and (b) — the load-bearing part — the real-action
    /// pin admission check SUBTRACTS this from `pinned_bytes`, so speculative
    /// bytes never consume real-pin headroom. `speculative_pinned_bytes <=
    /// pinned_bytes` always. Speculative pins are TIME-BOUNDED (NOT indefinite):
    /// they stay subject to the `PIN_TIMEOUT_SECS` sweep (evict-first).
    speculative_pinned_bytes: AtomicU64,
    // CAPPED AT speculative_pin_cap (5% of max_bytes): a DISJOINT, smaller
    // sub-budget so a burst of speculative pre-fetch pins can never refuse a
    // concurrent real action's `pin_key` (invariant-prover BLOCK, TLC-proven in
    // SpeculativePinBudgetFixed.tla). Over-cap behavior is BACKPRESSURE, never
    // drop: `pin_key_speculative` REFUSES (returns `false`); the caller leaves
    // the blob LRU-evictable and the prefetch simply does less — a real action
    // is never harmed. `max_bytes==0` (no byte budget) never gates.
    speculative_pin_cap: u64,
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
    /// (FINDING 2 moka-eviction-wedge) Consecutive drain-arm evaluations
    /// that observed the cache strictly over `max_bytes` with
    /// `size_evicted_items` unchanged. Reset to 0 whenever the cache is
    /// at-or-under budget or size-evictions progress; at
    /// `WEDGE_FROZEN_TICKS_TRIGGER` the fallback evictor fires. Only the
    /// background drain task writes it (single evaluator), so `Relaxed`
    /// suffices.
    selfheal_frozen_ticks: AtomicU64,
    /// (FINDING 2, sensor) Count of `RemovalCause::Size` evictions that
    /// flowed through `process_eviction_event`. The wedge sensor reads
    /// THIS — not `evicted_items` — because the moka livelock kills SIZE
    /// eviction specifically; TTL expiries (server targetkey store,
    /// `max_seconds: 604800`) or explicit removes advancing a
    /// coarse-grained counter would permanently mask a size-wedge
    /// (review 9fd52fc0 red-team alt-framing 2).
    size_evicted_items: AtomicU64,
    /// (FINDING 2) `size_evicted_items` value at the previous drain-arm
    /// evaluation — the frozen-evictions sensor baseline.
    selfheal_last_size_evicted_items: AtomicU64,
    /// (FINDING 2, metering) `true` when the previous heal invocation hit
    /// `WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION` while still observing
    /// over-budget AND made progress: the next drain-arm evaluation
    /// resumes the heal immediately (no fresh frozen-tick accumulation)
    /// so a large overshoot converges across metered invocations instead
    /// of one loop-monopolizing pass (review 9fd52fc0 pair-a MAJOR-4).
    selfheal_resume: AtomicBool,
    /// (FINDING 2, warn duty-cycle) `drain_kick_anchor.elapsed()` millis
    /// (+1; 0 = never) of the last self-heal WARN emission — rate-limits
    /// the warn to one per `WEDGE_SELFHEAL_WARN_MIN_INTERVAL_MILLIS`
    /// (pair-b C5).
    last_selfheal_warn_millis: AtomicU64,
    /// (FINDING 2 piece 2) Wedge gauge: `true` while the self-heal trigger
    /// condition holds (at-or-over budget AND evictions frozen for the
    /// full `WEDGE_FROZEN_TICKS_TRIGGER` window). Published as the
    /// `eviction_wedge_detected` 0/1 gauge so a wedged evictor pages
    /// instead of sitting dark (3.3× over budget + 3K NAKs paged nothing).
    eviction_wedge_detected: AtomicBool,
    /// (FINDING 2 piece 1) Count of self-heal firings. `CounterWithTime`
    /// so the render also carries `last_time` — "when did the wedge last
    /// self-heal" is the staleness signal an operator alert wants.
    eviction_wedge_selfheal_total: CounterWithTime,
    /// (FINDING 2, test seam) Phantom EXTRA bytes ADDED to the real
    /// weighted-bytes observation. A genuinely over-budget cache with a
    /// FRESH `weighted_size()` is by construction the wedged state (any
    /// completed maintenance pass either evicts to cap or is wedged),
    /// which tests cannot reproduce without moka's bug; this seam models
    /// the wedge faithfully — a constant stuck overage on top of the
    /// REAL evictable residency — so the observation FALLS as the heal
    /// evicts real entries and the convergence/stop logic is exercised
    /// for real. The EVICTION-EXECUTION half always runs against the
    /// real cache. cfg-gated out of production builds (review 9fd52fc0
    /// pair-a MINOR-5 / pair-b C4).
    #[cfg(any(test, feature = "test-utils"))]
    test_observed_extra_bytes: AtomicU64,
    /// (FINDING 2, test seam) When non-zero, overrides
    /// `EVICT_SCAN_HARD_CAP` so multi-round heal invocations are
    /// exercisable with small fixtures (pair-b T2). cfg-gated out of
    /// production builds.
    #[cfg(any(test, feature = "test-utils"))]
    test_evict_scan_cap_override: AtomicU64,
    /// (FINDING 2, test seam) When non-zero, overrides
    /// `DRAIN_KICK_MIN_INTERVAL_MILLIS` so the rate-limiter's RELEASE
    /// direction is testable with a short real sleep (pair-b T4).
    /// cfg-gated out of production builds.
    #[cfg(any(test, feature = "test-utils"))]
    test_drain_kick_interval_override: AtomicU64,
    /// (FINDING 2 piece 3) Wake signal for the background drain arm. The
    /// disk-pressure NAK site kicks this (via
    /// `FilesystemStore::kick_eviction_drain`) so the admission gate
    /// actively drives eviction (`gate ⇒ evict`) instead of only refusing
    /// work while the evictor may be wedged.
    drain_kick: Notify,
    /// (FINDING 2 piece 3) Rate-limit anchor for `kick_drain`.
    drain_kick_anchor: Instant,
    /// (FINDING 2 piece 3) `drain_kick_anchor.elapsed()` millis (+1 so 0
    /// means "never kicked") of the last ACCEPTED kick.
    last_drain_kick_millis: AtomicU64,
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
        // #speculative-prefetch P0: speculative (pre-fetch) subset gauge.
        let speculative_pinned_bytes: u64 =
            self.speculative_pinned_bytes.load(Ordering::Relaxed);

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
            "speculative_pinned_bytes",
            &speculative_pinned_bytes,
            nativelink_metric::MetricKind::Default,
            "#speculative-prefetch P0: bytes held by SPECULATIVE (pre-fetch) pins — a DISJOINT subset of pinned_bytes EXCLUDED from the real-action pin_cap admission check so speculation can never refuse a real action's pin. Time-bounded (evict-first via the 120s sweep); capped by speculative_pin_cap with backpressure over-cap."
        );
        nativelink_metric::publish!(
            "speculative_pin_cap",
            &self.speculative_pin_cap,
            nativelink_metric::MetricKind::Default,
            "#speculative-prefetch P0: configured cap on speculative_pinned_bytes = max_bytes * 5% (SPECULATIVE_PIN_CAP_FRACTION, a fifth of the 25% pin_cap). Over-cap behavior is backpressure (pin_key_speculative refuses; the prefetch pins less — never drops, never harms a real action)."
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
        // (FINDING 2 piece 2) Wedge observability, on the same
        // live-rendering tree as weighted_size_bytes (dark-counter trap:
        // the production wedge sat at 3.3x over budget + 3K disk NAKs and
        // nothing paged).
        // (pair-b C1) `max_bytes == 0` = no byte budget: overshoot is
        // meaningless, not "the whole cache" — mirror the guards in
        // `would_exceed_capacity` and `maybe_selfheal_wedged_eviction`.
        let overshoot_bytes: u64 = if self.max_bytes == 0 {
            0
        } else {
            weighted_size_bytes.saturating_sub(self.max_bytes)
        };
        let eviction_wedge_detected: u64 =
            u64::from(self.eviction_wedge_detected.load(Ordering::Relaxed));
        nativelink_metric::publish!(
            "overshoot_bytes",
            &overshoot_bytes,
            nativelink_metric::MetricKind::Default,
            "FINDING 2: bytes the live weighted size (weighted_size_bytes) exceeds max_bytes; 0 while at-or-under budget. A sustained non-zero value with evicted_items frozen is the moka eviction-wedge signature — alert on it."
        );
        nativelink_metric::publish!(
            "eviction_wedge_detected",
            &eviction_wedge_detected,
            nativelink_metric::MetricKind::Default,
            "FINDING 2: 1 while the eviction-wedge self-heal trigger condition holds (weighted size at-or-over max_bytes AND evicted_items frozen for the full trigger window); 0 otherwise. Page on sustained 1 — the built-in fallback evictor is compensating for a wedged moka evictor. Since the moka 0.12.16 bump (upstream fix moka-rs/moka#592) this is expected to stay 0; a sustained 1 means the upstream fix is incomplete for our workload or a different eviction bug exists."
        );
        nativelink_metric::publish!(
            "eviction_wedge_selfheal_total",
            &self.eviction_wedge_selfheal_total,
            nativelink_metric::MetricKind::Component,
            "FINDING 2: eviction-wedge self-heal firings (CounterWithTime — emits .counter and .last_time; one increment + one warn per firing, never per evicted entry). Since the moka 0.12.16 bump this counter is the PRIMARY residual-risk signal: any non-zero value is evidence the upstream #592 fix did not fully cover our workload. Alert on first increment, not on a rate."
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
    // `Display` (added for the stale-pin diagnostic in
    // `sweep_stale_indefinite_pins`) is satisfied by every production Q:
    // StoreKey, DigestInfo, OperationId, and the u64 test key. Compile-time
    // bound only — zero runtime behavior change.
    Q: Ord + Hash + Eq + Debug + Display + Send + Sync + 'static,
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
                // (FINDING 2 fix-up) carry moka's removal cause so the
                // wedge sensor counts ONLY `RemovalCause::Size` events.
                cause,
            });
            // Unbounded channel never blocks — send only fails if the
            // receiver is dropped (shutdown).
            let _ = listener_tx.send(());
        });

        let cache = builder.build();
        // FL-681 NAK boundary fix: honor the operator-configured `pin_cap_bytes`
        // when non-zero; otherwise derive the historical 25%-of-max_bytes cap so
        // existing configs are unchanged. This is the ceiling the admission NAK
        // gate (`indefinite_pin_saturated`) and the total pin refusal measure against.
        let pin_cap = if config.pin_cap_bytes == 0 {
            (max_bytes as f64 * PIN_CAP_FRACTION) as u64
        } else {
            config.pin_cap_bytes
        };
        // #speculative-prefetch P0: the disjoint speculative sub-budget
        // (5% of max_bytes, a fifth of the 25% pin_cap). Speculative pins draw
        // only from this; the real-pin check excludes speculative bytes.
        let speculative_pin_cap = (max_bytes as f64 * SPECULATIVE_PIN_CAP_FRACTION) as u64;
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
            speculative_pinned_bytes: AtomicU64::new(0),
            pin_cap,
            indefinite_pin_cap,
            speculative_pin_cap,
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
            selfheal_frozen_ticks: AtomicU64::new(0),
            size_evicted_items: AtomicU64::new(0),
            selfheal_last_size_evicted_items: AtomicU64::new(0),
            selfheal_resume: AtomicBool::new(false),
            last_selfheal_warn_millis: AtomicU64::new(0),
            eviction_wedge_detected: AtomicBool::new(false),
            eviction_wedge_selfheal_total: CounterWithTime::default(),
            #[cfg(any(test, feature = "test-utils"))]
            test_observed_extra_bytes: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            test_evict_scan_cap_override: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            test_drain_kick_interval_override: AtomicU64::new(0),
            drain_kick: Notify::new(),
            drain_kick_anchor: Instant::now(),
            last_drain_kick_millis: AtomicU64::new(0),
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
        if let Some(ref value) = result {
            // (#locality-map-drift) Carry the value's frozen insert stamp so the
            // on_get delta matches the insert delta (see `fire_on_get`).
            self.fire_on_get(key, value.stamp());
        }
        result
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
                if let Some(ref value) = result {
                    self.fire_on_get(key, value.stamp());
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
            // #speculative-prefetch P0: preserve the speculative flag across a
            // re-insert too, and keep `speculative_pinned_bytes` symmetric.
            let mut was_speculative = false;
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                if entry.indefinite {
                    was_indefinite = true;
                    self.indefinite_pinned_bytes
                        .fetch_sub(entry.size, Ordering::Relaxed);
                }
                if entry.speculative {
                    was_speculative = true;
                    self.speculative_pinned_bytes
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
                    speculative: was_speculative,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            if was_indefinite {
                self.indefinite_pinned_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            if was_speculative {
                self.speculative_pinned_bytes
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
    fn fire_on_get(&self, key: &Q, ts_counter: u64) {
        if !self.has_callbacks_flag.load(Ordering::Relaxed) {
            return;
        }
        // (#locality-map-drift) A read carries the RESIDENT VALUE'S FROZEN insert
        // counter (`value.stamp()`), NOT a fresh mint. So the on_get delta is
        // PRESENT@(boot_epoch, c_insert) — IDENTICAL to the value's insert delta
        // (idempotent at the tracker + server LWW).
        //
        // Why NOT a fresh mint (this was the original design, DISPROVEN — TLC
        // HoldingsTouch A_GateKill VIOLATES NoGateKilledGenuineEvict): a fresh
        // counter would be HIGHER than the value's own insert counter, so a read
        // between insert and the value's GENUINE eviction would out-rank that
        // eviction (which carries the low insert counter) → the blob would stay
        // falsely PRESENT forever (systematic false-positive on every
        // read-then-evicted blob). Carrying the value stamp avoids that.
        //
        // The false-NEGATIVE rescue still works: a STALE evict carries a
        // PREVIOUS value's OLDER counter (re-admission mints a strictly-higher
        // insert counter), so this read's PRESENT@c_current still supersedes it
        // (c_current > c_prev). (TLC HoldingsTouch B: StaleEvictSuppressed still
        // exercised.)
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
            // #speculative-prefetch P0: preserve the speculative flag across a
            // re-insert too, and keep `speculative_pinned_bytes` symmetric.
            let mut was_speculative = false;
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                if entry.indefinite {
                    was_indefinite = true;
                    self.indefinite_pinned_bytes
                        .fetch_sub(entry.size, Ordering::Relaxed);
                }
                if entry.speculative {
                    was_speculative = true;
                    self.speculative_pinned_bytes
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
                    speculative: was_speculative,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            if was_indefinite {
                self.indefinite_pinned_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            if was_speculative {
                self.speculative_pinned_bytes
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
                // #speculative-prefetch P0: symmetric to indefinite — a
                // speculative-pinned blob removed via the explicit-remove path
                // must free the speculative sub-budget, else it leaks upward
                // and later speculative pins are wrongly refused.
                if entry.speculative {
                    self.speculative_pinned_bytes
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
                // #speculative-prefetch C1: an indefinite pin IS a real pin
                // (held until BIS-ack, TTL-exempt). If this digest was
                // previously pinned SPECULATIVELY (input==F2-output digest
                // coincidence), RECLASSIFY it spec→real: move its bytes OUT of
                // `speculative_pinned_bytes` and clear the flag. Otherwise the
                // entry would stay TTL-exempt yet keep inflating the speculative
                // gauge (shrinking its own 5% sub-budget) AND keep being
                // subtracted from the real-pin admission check (real headroom
                // undercounted). This keeps the `PinnedEntry` speculative↔
                // indefinite mutual-exclusion doc honest.
                //
                // INVARIANT NOTE (corrects this change's commit invariant-walk
                // over-claim of "tightens, never loosens"): the spec->indef move
                // is NOT atomic. For a ~1-instruction window a concurrent real-pin
                // check-then-act can still see these bytes in `speculative_pinned_bytes`
                // and transiently over-admit a real pin by <= speculative_pin_cap.
                // DEGRADED-not-broken: the physical ceiling (pin_cap +
                // speculative_pin_cap) is UNCHANGED from the pre-C1 base, it self-heals
                // <= PIN_TIMEOUT_SECS, and it REPLACES a worse DURABLE hole (persistent
                // speculative-gauge inflation + real-headroom undercount). TLC-modeled:
                // `.claude/tla/SpeculativePinBudgetReclassify.tla` (NoOverPinQuiescent
                // violated on the transient, PhysicalOverShootBounded HOLDS).
                if entry.speculative {
                    entry.speculative = false;
                    self.speculative_pinned_bytes
                        .fetch_sub(size, Ordering::Relaxed);
                }
                // FL-688 advertise-on-pin (time-bounded -> indefinite UPGRADE
                // transition). F1 fire-once — CAPPED AT indefinite_pin_cap: the
                // fire-once "already advertised on pin" gate is the `pinned`
                // entry's `indefinite` flag itself. on_pin fires ONLY on this
                // false->true transition; every later re-pin of an
                // already-indefinite key hits the `return true` below WITHOUT
                // re-firing. The set of indefinitely-pinned keys is bounded by
                // `indefinite_pin_cap` (over-cap pins are REFUSED as
                // backpressure above, so they never fire on_pin). No separate
                // dedup set is added: it would have to mirror `pinned`
                // membership exactly (cleared on `unpin_key`, re-armed on a
                // fresh pin lifecycle) — redundant state.
                // F5 stamp: mint a FRESH counter, freeze it into the value (so a
                // later genuine eviction carries the same stamp and the tie
                // resolves ABSENT), drop the shard guard, then fire — the
                // PRESENT@fresh delta strictly out-ranks any prior evict.
                let ts_counter = self.next_stamp();
                entry.data.set_stamp(ts_counter);
                drop(entry);
                self.fire_on_pin_callbacks(&key, size, ts_counter);
                return true;
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
            // #speculative-prefetch P0 (invariant-prover BLOCK, TLC-proven):
            // EXCLUDE speculative bytes from the real-action pin admission
            // check so a burst of speculative pre-fetch pins can NEVER refuse
            // a real action's pin (the C3/C5 starvation). The two budgets are
            // DISJOINT: real pins see `pin_cap`, speculative pins see their own
            // smaller `speculative_pin_cap`.
            // (`.claude/tla/SpeculativePinBudgetFixed.tla`, NoStarve HOLDS.)
            let real_pinned = self
                .pinned_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(self.speculative_pinned_bytes.load(Ordering::Relaxed));
            if real_pinned.saturating_add(entry_size) > self.pin_cap {
                warn!(
                    real_pinned,
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

        // FL-688 advertise-on-pin (fresh INDEFINITE pin transition). F1
        // fire-once — CAPPED AT indefinite_pin_cap (see the upgrade path
        // above): a brand-new `pinned` entry advertises exactly once; the
        // indefinite flag is the fire-once gate on any subsequent re-pin. F5
        // stamp: mint + freeze a FRESH counter into the value BEFORE it moves
        // into the pinned map, so the on_pin PRESENT delta strictly out-ranks
        // any prior ABSENT evict of this key under the tracker LWW (an equal
        // frozen stamp would lose the tie and be suppressed). Only indefinite
        // pins advertise — time-bounded/speculative pins are not the FL-688
        // leak class and self-heal via the TTL-sweep on_insert re-ack.
        let pin_advert_stamp = if indefinite {
            let ts_counter = self.next_stamp();
            value.set_stamp(ts_counter);
            Some(ts_counter)
        } else {
            None
        };

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
                // #speculative-prefetch P0: pin_key / pin_key_indefinite are
                // real-action pins, never speculative.
                speculative: false,
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
        // Fire the advertise-on-pin hook AFTER the entry is durably in the
        // pinned map and out of the cache (no shard/callback lock overlap).
        if let Some(ts_counter) = pin_advert_stamp {
            self.fire_on_pin_callbacks(&key, entry_size, ts_counter);
        }
        true
    }

    /// #speculative-prefetch P0: pin `key` as a SPECULATIVE (pre-fetch) pin,
    /// drawing ONLY from the small disjoint `speculative_pin_cap` sub-budget.
    /// TIME-BOUNDED (subject to the `PIN_TIMEOUT_SECS` sweep — evict-first),
    /// NOT indefinite. Returns `false` if the digest was absent (eviction race)
    /// OR the speculative sub-budget is full (BACKPRESSURE — the caller leaves
    /// the blob LRU-evictable and the pre-fetch simply pins less). Crucially,
    /// a speculative pin is EXCLUDED from the real-action pin admission check
    /// (`pin_key`), so it can NEVER refuse a concurrent real action's pin —
    /// the disjoint-sub-budget property the invariant-prover machine-checked
    /// (`.claude/tla/SpeculativePinBudgetFixed.tla`, NoStarve HOLDS).
    pub fn pin_key_speculative(&self, key: K) -> bool {
        let q: &Q = key.borrow();

        // Already pinned — just refresh the pin time. Do NOT change its
        // speculative/real classification (a real pin that already exists for
        // this digest stays real; a speculative refresh keeps it speculative).
        if let Some(mut entry) = self.pinned.get_mut(q) {
            entry.pinned_at = Instant::now();
            return true;
        }

        let value = match self.cache.get(q) {
            Some(v) => v,
            None => return false,
        };
        let entry_size = value.len();

        // Speculative sub-budget check ONLY (disjoint from pin_cap). Over-cap
        // is BACKPRESSURE (refuse, never drop).
        if !self.speculative_cap_admits(entry_size) {
            warn!(
                speculative_pinned_bytes =
                    self.speculative_pinned_bytes.load(Ordering::Relaxed),
                entry_size,
                speculative_pin_cap = self.speculative_pin_cap,
                ?key,
                "speculative pin cap exceeded, refusing to pin (backpressure) — real actions unaffected"
            );
            return false;
        }

        // Insert into pinned FIRST, then invalidate (same ordering as pin_key).
        self.pinned.insert(
            key.clone(),
            PinnedEntry {
                data: value,
                pinned_at: Instant::now(),
                size: entry_size,
                indefinite: false,
                speculative: true,
            },
        );
        self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.speculative_pinned_bytes
            .fetch_add(entry_size, Ordering::Relaxed);

        self.cache.invalidate(q);
        self.cache.run_pending_tasks();
        true
    }

    /// #speculative-prefetch P0: snapshot check of the speculative-pin
    /// sub-budget. Mirrors [`Self::indefinite_cap_admits`]. `max_bytes == 0`
    /// (no byte budget) never gates.
    fn speculative_cap_admits(&self, entry_size: u64) -> bool {
        if self.max_bytes == 0 {
            return true;
        }
        let current = self.speculative_pinned_bytes.load(Ordering::Relaxed);
        current.saturating_add(entry_size) <= self.speculative_pin_cap
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
                // #speculative-prefetch P0: EXCLUDE speculative bytes from the
                // real-action batch-pin check (disjoint sub-budget). Same
                // property as pin_key_with_mode above.
                let real_pinned = self
                    .pinned_bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.speculative_pinned_bytes.load(Ordering::Relaxed));
                if real_pinned.saturating_add(entry_size) > self.pin_cap {
                    warn!(
                        real_pinned,
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
                    // #speculative-prefetch P0: pin_keys is a real-action batch pin.
                    speculative: false,
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
            // #speculative-prefetch P0: keep the speculative sub-budget
            // symmetric on release (adoption / explicit unpin).
            if entry.speculative {
                self.speculative_pinned_bytes
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

    /// #speculative-prefetch P0: bytes currently held by SPECULATIVE
    /// (pre-fetch) pins. A subset of [`Self::pinned_bytes`], bounded by the
    /// small disjoint `speculative_pin_cap`. EXCLUDED from the real-action
    /// pin admission check so speculation never starves a real pin.
    pub fn speculative_pinned_bytes(&self) -> u64 {
        self.speculative_pinned_bytes.load(Ordering::Relaxed)
    }

    /// FL-681 Fix A: bytes currently held by INDEFINITE
    /// (pinned-until-BIS-ack) pins. A subset of [`Self::pinned_bytes`].
    /// Bounded by the indefinite-pin cap.
    pub fn indefinite_pinned_bytes(&self) -> u64 {
        self.indefinite_pinned_bytes.load(Ordering::Relaxed)
    }

    /// FL-681 NAK boundary fix: snapshot predicate for the worker's
    /// admission-side gate. Returns `true` when the TOTAL real-pin budget has
    /// almost no headroom left — specifically when
    /// `real_pinned >= pin_cap − pin_cap / PIN_SATURATION_HEADROOM_DIVISOR`
    /// (within 5% of `pin_cap`), where
    /// `real_pinned = pinned_bytes − speculative_pinned_bytes`. The worker's
    /// action-acceptance path reads this and NAKs the new action with
    /// `Code::ResourceExhausted` so the scheduler re-queues it (true producer
    /// backpressure) instead of admitting an action whose F2 output cannot be
    /// pinned-until-durable.
    ///
    /// Gating on `real_pinned` vs `pin_cap` — the SAME quantity and ceiling the
    /// total pin refusal in `pin_key_with_mode` uses — makes this predicate
    /// PREDICT that refusal: it fires just before the cap starts rejecting
    /// variable-sized outputs, not one blob after `pinned_bytes` reaches the
    /// exact cap. (The prior gate `indefinite_pinned_bytes >= indefinite_pin_cap`
    /// was silently dead: a refused pin adds no bytes, so `indefinite_pinned_bytes`
    /// stuck below the cap and `>= cap` ~never tripped with variable sizes.)
    /// Disk is fungible across cache/pinned/inputs, so a full pin budget from
    /// ANY source is legitimate backpressure. NOTE: post-fix
    /// `pending_bis_pin_max_bytes` (the `indefinite_pin_cap`) NO LONGER
    /// influences the NAK — the gate changed both the numerator (indefinite →
    /// real) and the cap (`indefinite_pin_cap` → `pin_cap`).
    ///
    /// Same eventually-consistent snapshot shape as `indefinite_cap_admits`
    /// (the cap STOPS an over-capacity hot loop; the scheduler's natural
    /// re-queue closes the residual race). `max_bytes == 0` (no byte budget
    /// configured) never gates — an uncapped store has no pin cap to saturate.
    #[must_use]
    pub fn indefinite_pin_saturated(&self) -> bool {
        if self.max_bytes == 0 {
            return false;
        }
        // Integer headroom band: `pin_cap / DIVISOR <= pin_cap` so the
        // subtraction cannot underflow; do NOT write `pin_cap * 19 / 20` (u64
        // overflow for large caps).
        let real_pinned = self
            .pinned_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(self.speculative_pinned_bytes.load(Ordering::Relaxed));
        real_pinned >= self.pin_cap - self.pin_cap / PIN_SATURATION_HEADROOM_DIVISOR
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

        // DEADLOCK-SAFETY (2026-07-07 graceful-shutdown drain deadlock):
        // Snapshot the matching keys under the btree READ lock, then RELEASE
        // the lock BEFORE touching moka's `cache.get()`. We must NOT hold
        // `self.btree.read()` across `cache.get()`: moka's `sync::Cache::get`
        // performs amortized maintenance (`do_run_pending_tasks` →
        // `evict_lru_entries`) and fires the eviction listener SYNCHRONOUSLY
        // on this stack, and that listener acquires `self.btree.write()`
        // (line ~674). parking_lot's RwLock is non-reentrant and
        // write-preferring, so a write acquire under a same-stack read guard
        // blocks forever — the exact self-deadlock that wedged the server on
        // SIGTERM (`StoreManager::flush_slow_writes` → `MemoryStore::list` →
        // this `range` → `cache.get` → listener → `btree.write()`). The
        // snapshot iterates the BTreeSet (pure in-memory, no moka) under the
        // read lock only, so no lock is held when the listener needs to write.
        //
        // CAPPED AT btree size (= `max_count`, the map's configured entry cap;
        // 1M for the production cas_FAST_SLOW MemoryStore): the snapshot is a
        // subset of the resident key set, which moka bounds by `max_count`.
        // This is the SAME bound the sole caller
        // (`FastSlowStore::flush_fast_to_slow_at_shutdown`) already allocates
        // when it collects the full key set — the snapshot is moved earlier
        // (and lock-released-first), not newly unbounded.
        let candidates: Vec<K> = {
            let btree = self.btree.read();
            let set = btree.as_ref().expect("btree should be built");
            set.range(prefix_range).cloned().collect()
        };

        let check_pinned = self.has_pinned();
        let mut count = 0;
        for key in &candidates {
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
            // Skip keys evicted by moka but still in BTree (stale) — including
            // any evicted by the maintenance a `cache.get()` above triggers.
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
    ///   * (FINDING 2 fix-up, review 9fd52fc0 MAJOR-3) The wedge
    ///     self-heal (`maybe_selfheal_wedged_eviction`, reached from the
    ///     drain tick and the disk-NAK kick) made this the recovery
    ///     eviction path for EVERY byte-budgeted `MokaEvictingMap` —
    ///     worker fast tier AND server tiers — whose load-bearing
    ///     property is WORKING-SET retention, not just durability.
    ///     Arbitrary order costs cache-hit fidelity: a full incident
    ///     heal (~66 000 entries over ~4 metered invocations) discards a
    ///     hash-arbitrary rather than coldest-first subset of the
    ///     working set. Accepted deliberately: the alternative is an
    ///     unboundedly ratcheting overshoot with a permanently dead
    ///     evictor (3.36× over budget in the incident), and moka exposes
    ///     no ordered walk. The order caveat is scoped to WEDGE RECOVERY
    ///     — normal (non-wedged) eviction remains moka's LRU. Since the
    ///     0.12.16 bump (moka-rs/moka#592 fixes the wedge upstream) the
    ///     wedge-recovery ENTRY into this method is expected to be
    ///     dormant, so this fidelity cost should now be hypothetical;
    ///     the admission-gate caller (`check_backpressure_gate`) still
    ///     reaches it on the normal path and is unaffected by the bump.
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
        // (FINDING 2, test seam) scan cap is overridable in test builds
        // so multi-round metered heals are exercisable with small
        // fixtures; production always uses EVICT_SCAN_HARD_CAP.
        #[cfg(any(test, feature = "test-utils"))]
        let scan_cap = match self.test_evict_scan_cap_override.load(Ordering::Relaxed) {
            0 => EVICT_SCAN_HARD_CAP,
            cap => cap,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let scan_cap = EVICT_SCAN_HARD_CAP;
        for (key_arc, value) in self.cache.iter() {
            iter_scanned = iter_scanned.saturating_add(1);
            if evicted_bytes >= target_bytes {
                break;
            }
            if iter_scanned >= scan_cap {
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

    /// (FINDING 2, test seam) Add phantom EXTRA bytes to the wedge
    /// trigger's weighted-bytes observation (models the wedge's stuck
    /// overage on top of the REAL evictable residency — see
    /// `test_observed_extra_bytes`). `0` restores the real observation.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_inflate_wedge_observation(&self, extra_bytes: u64) {
        self.test_observed_extra_bytes
            .store(extra_bytes, Ordering::Relaxed);
    }

    /// (FINDING 2, test seam) Override `EVICT_SCAN_HARD_CAP` for the
    /// fallback key walk so multi-round heals are exercisable with small
    /// fixtures. `0` restores the production cap.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_force_evict_scan_cap(&self, cap: u64) {
        self.test_evict_scan_cap_override
            .store(cap, Ordering::Relaxed);
    }

    /// (FINDING 2, test seam) Shrink the kick rate-limiter's minimum
    /// interval so the limiter's RELEASE direction is testable with a
    /// short real sleep instead of a full 5 s
    /// `DRAIN_KICK_MIN_INTERVAL_MILLIS` wait (pair-b T4; the anchor is
    /// `std::time::Instant`, unaffected by tokio's paused clock, so the
    /// real elapsed-time comparison is exercised unmodified). `0`
    /// restores the production interval.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn test_force_drain_kick_min_interval(&self, millis: u64) {
        self.test_drain_kick_interval_override
            .store(millis, Ordering::Relaxed);
    }

    /// (FINDING 2, test seam) Drive one drain-arm self-heal evaluation
    /// deterministically (the production callers are the two drain arms
    /// via `gated_drain_arm`).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub async fn test_maybe_selfheal_wedged_eviction(&self) -> WedgeSelfHealOutcome {
        self.maybe_selfheal_wedged_eviction().await
    }

    /// (FINDING 2) The weighted-bytes observation the wedge trigger reads:
    /// the moka weigher's KB-weight sum scaled back to bytes — the SAME
    /// derivation `would_exceed_capacity` uses for its cache term (pinned
    /// bytes deliberately excluded here: the fallback substitutes for
    /// moka's OWN evictor, which only sees cache residents; pinned entries
    /// live outside the cache under their separately-capped 25% budget).
    /// The weigher rounds sizes UP, so this over-estimates true bytes —
    /// the safe direction (can never under-fire on a real overshoot).
    fn observed_weighted_bytes(&self) -> u64 {
        const SCALE: u64 = 1024;
        let real = self.cache.weighted_size().saturating_mul(SCALE);
        #[cfg(any(test, feature = "test-utils"))]
        {
            return real.saturating_add(
                self.test_observed_extra_bytes.load(Ordering::Relaxed),
            );
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        real
    }

    /// (FINDING 2) One evaluation of the eviction-wedge self-heal trigger,
    /// run by BOTH background drain arms (periodic tick + NAK kick) right
    /// after `run_pending_tasks_and_drain`.
    ///
    /// **Status: SAFETY NET, not the primary mitigation.** The bug it was
    /// built for (moka-rs/moka#590) is FIXED upstream in moka 0.12.16 —
    /// the version this crate pins. It is retained deliberately: #592
    /// fixed the orphan MINT but left the livelock geometry
    /// (`skip_updated_entry_ao`) untouched, and
    /// `eviction_wedge_selfheal_total > 0` on 0.12.16 is our only signal
    /// that the fix is incomplete for our workload. See
    /// `WEDGE_FROZEN_TICKS_TRIGGER` for the full mechanism and both
    /// corrections to the original diagnosis.
    ///
    /// Trigger: the cache is STRICTLY OVER `max_bytes` AND
    /// `size_evicted_items` has not advanced for
    /// `WEDGE_FROZEN_TICKS_TRIGGER` consecutive evaluations. In a wedged
    /// state `run_pending_tasks` returns normally but evicts 0 forever
    /// while the weight ledger stays accurate — this evaluation detects
    /// exactly that signature and re-establishes the budget via the
    /// deque-INDEPENDENT key walk (`evict_unpinned_lru_bytes`).
    ///
    /// **Deque-independence chain, re-traced hop-by-hop against moka
    /// 0.12.16** (it was originally traced through 0.12.15; every hop
    /// still holds, and none of the four reads the probation front):
    ///  1. SELECTION — `cache.iter()` walks the CHT segments.
    ///     `src/sync/cache.rs` is byte-identical between 0.12.15 and
    ///     0.12.16, so this hop is unchanged.
    ///  2. REMOVAL — `cache.invalidate` → `invalidate_with_hash`
    ///     (`src/sync/cache.rs:1577`) → `Inner::remove_entry`. This hop
    ///     CHANGED SHAPE in 0.12.16: `Inner::remove_entry`
    ///     (`sync/base_cache.rs:1106`) now unlinks via
    ///     `remove_entry_if_and` and flips `EntryInfo::retire()` inside
    ///     the post-CAS callback. It is still a pure CHT unlink plus a
    ///     `WriteOp::Remove` enqueue — it reads no deque state — so
    ///     deque-independence is PRESERVED, and the retire flag is what
    ///     makes a heal-issued invalidate additionally immune to minting
    ///     a fresh orphan from a racing stale `Upsert`.
    ///  3. WEIGHT LEDGER — the `WriteOp::Remove` drain reaches
    ///     `handle_remove_without_timer_wheel`
    ///     (0.12.16 `sync/base_cache.rs:1891`, was `:1808`), which unlinks
    ///     each entry's OWN node by pointer via `deqs.unlink_ao`. Body is
    ///     unchanged; 0.12.16 only adds a debug-mode precondition assert
    ///     that the entry is already retired (satisfied by hop 2).
    ///  4. ORDERING — `do_run_pending_tasks`
    ///     (0.12.16 `sync/base_cache.rs:1190`, was `:1181`) is
    ///     BYTE-IDENTICAL: writes are still applied before
    ///     `evict_lru_entries`, and the loop still breaks on no progress.
    ///  5. SENSOR INPUT — `RemovalCause::Size` still reaches the eviction
    ///     listener from the LRU path (0.12.16 `sync/base_cache.rs:2401`,
    ///     was `:2304`), which is the only thing `size_evicted_items`
    ///     counts.
    ///
    /// METERED (review 9fd52fc0 pair-a MAJOR-4 / pair-b P3): each
    /// invocation runs at most `WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION`
    /// key-walk rounds with a `yield_now` between rounds, then RE-ARMS
    /// via `selfheal_resume` so the next tick/kick continues the heal —
    /// the select loop keeps servicing the pin-expiry arms between
    /// invocations and the BlobChangeTracker delta burst stays bounded
    /// per BlobsAvailable message. Production callers: the two drain
    /// arms via `gated_drain_arm`; tests drive it via the cfg-gated
    /// `test_maybe_selfheal_wedged_eviction`.
    async fn maybe_selfheal_wedged_eviction(&self) -> WedgeSelfHealOutcome {
        // Suppressed IDENTICALLY to the drain tick while the FL-688
        // startup reconcile gate is armed (belt + braces: both arms also
        // check before calling). Leaves trigger state untouched.
        if !self.reconcile_complete.load(Ordering::Acquire) {
            return WedgeSelfHealOutcome::SuppressedReconcileGate;
        }
        if self.max_bytes == 0 {
            return WedgeSelfHealOutcome::NoByteBudget;
        }
        let observed = self.observed_weighted_bytes();
        let evicted_now = self.size_evicted_items.load(Ordering::Relaxed);
        // STRICT `>` threshold (operator decision 2026-07-29, review
        // 9fd52fc0 MAJOR-2, superseding the earlier `>=` directive): at
        // exact equality moka owes ZERO eviction
        // (`weights_to_evict = weighted_size.saturating_sub(max)`), so a
        // wedge is mathematically unobservable there and firing would be
        // a guaranteed false positive — and every deployed `max_bytes`
        // is an exact multiple of 1024, making equality routinely
        // reachable by an idle at-cap cache. The original no-SLACK
        // rationale still holds: there is no multiplier band a wedge
        // could park inside — nothing sits strictly between `max` and
        // `max` — so any real overshoot (≥ one weigher unit over)
        // triggers. Compared in the weigher-derived bytes against the
        // same `self.max_bytes` that `would_exceed_capacity` uses.
        if observed <= self.max_bytes {
            self.selfheal_frozen_ticks.store(0, Ordering::Relaxed);
            self.selfheal_last_size_evicted_items
                .store(evicted_now, Ordering::Relaxed);
            self.selfheal_resume.store(false, Ordering::Relaxed);
            self.eviction_wedge_detected.store(false, Ordering::Relaxed);
            return WedgeSelfHealOutcome::UnderBudget;
        }
        let resumed = self.selfheal_resume.load(Ordering::Relaxed);
        if !resumed {
            let last = self
                .selfheal_last_size_evicted_items
                .swap(evicted_now, Ordering::Relaxed);
            if evicted_now != last {
                // SIZE evictions are progressing — no wedge. Normal
                // at-cap operation lands here every tick, which is what
                // prevents false-firing. Non-Size progress (TTL
                // `Expired`, explicit removes) deliberately does NOT
                // land here — see `size_evicted_items`.
                self.selfheal_frozen_ticks.store(0, Ordering::Relaxed);
                self.eviction_wedge_detected.store(false, Ordering::Relaxed);
                return WedgeSelfHealOutcome::ProgressObserved;
            }
            let frozen = self.selfheal_frozen_ticks.fetch_add(1, Ordering::Relaxed) + 1;
            if frozen < WEDGE_FROZEN_TICKS_TRIGGER {
                return WedgeSelfHealOutcome::Accumulating(frozen);
            }
        }

        // Trigger met (or metered heal resumed) — run the fallback for
        // ONE bounded invocation.
        self.eviction_wedge_detected.store(true, Ordering::Relaxed);
        let mut total_count = 0u64;
        let mut total_bytes = 0u64;
        let mut rounds = 0u32;
        while rounds < WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION {
            // Per-round target: the current overage. Under a real wedge
            // the observation is the live weighted size, which FALLS as
            // the heal evicts, so the loop stops exactly when the cache
            // is back at-or-under `max_bytes` (strict `>` here too — at
            // equality there is nothing owed). `observed_weighted_bytes`
            // adds the cfg-gated test overage on top of the real value.
            let live = self.observed_weighted_bytes();
            if live <= self.max_bytes {
                break;
            }
            let target = live - self.max_bytes;
            let report = self.evict_unpinned_lru_bytes(target);
            // Route the invalidations through the NORMAL eviction
            // pipeline (listener → event → unref + removal callbacks) so
            // disk file, index entry, and BlobChangeTracker delta stay
            // consistent. This task is the background drainer (sole
            // owner of the periodic drain), so draining inline here is
            // the same single-consumer discipline as the drain arms.
            self.drain_pending_evictions().await;
            // Metering (pair-b P3): each round is a bounded synchronous
            // stretch (scan + invalidate + listener per entry); yield so
            // co-scheduled tasks run between rounds.
            tokio::task::yield_now().await;
            total_count += report.evicted_count;
            total_bytes += report.evicted_bytes;
            rounds += 1;
            if report.evicted_count == 0 {
                // No evictable candidates (everything remaining is
                // pin-protected or the cache is empty). Leave the wedge
                // gauge set; the trigger re-accumulates and re-fires on
                // its normal duty cycle.
                break;
            }
        }
        // Re-arm (metering): if this bounded invocation made progress
        // but the observation is still over budget, the next drain-arm
        // evaluation RESUMES the heal immediately — no fresh frozen-tick
        // accumulation. (The heal's own `Explicit` invalidations do not
        // advance the Size sensor, so waiting for "frozen" again would
        // only add dead ticks to convergence.)
        let still_over = self.observed_weighted_bytes() > self.max_bytes;
        self.selfheal_resume
            .store(still_over && total_count > 0, Ordering::Relaxed);
        self.selfheal_frozen_ticks.store(0, Ordering::Relaxed);
        self.selfheal_last_size_evicted_items.store(
            self.size_evicted_items.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        if !resumed {
            // Count every LOGICAL firing (resumed continuations are part
            // of the same firing: no increment, no warn). The warn is
            // additionally rate-limited (pair-b C5) — the gauge +
            // counter carry the persistent state between warns.
            self.eviction_wedge_selfheal_total.inc();
            let now_millis = self.drain_kick_anchor.elapsed().as_millis() as u64 + 1;
            let last_warn = self.last_selfheal_warn_millis.load(Ordering::Relaxed);
            if last_warn == 0
                || now_millis.saturating_sub(last_warn)
                    >= WEDGE_SELFHEAL_WARN_MIN_INTERVAL_MILLIS
            {
                self.last_selfheal_warn_millis
                    .store(now_millis, Ordering::Relaxed);
                warn!(
                    target: "nativelink::eviction_wedge_selfheal",
                    observed_weighted_bytes = observed,
                    max_bytes = self.max_bytes,
                    evicted_count = total_count,
                    evicted_bytes = total_bytes,
                    rounds,
                    frozen_ticks = WEDGE_FROZEN_TICKS_TRIGGER,
                    "moka eviction wedge self-heal fired: size-evictions made no \
                     progress while strictly over budget (FINDING 2 \
                     stale-probation-front livelock signature); metered fallback \
                     key-walk evictor ran (continues across ticks until under \
                     budget)"
                );
            }
        }
        WedgeSelfHealOutcome::Fired {
            evicted_count: total_count,
            evicted_bytes: total_bytes,
            rounds,
        }
    }

    /// (FINDING 2 piece 3) Rate-limited wake of the background drain arm.
    /// Called from the worker's disk-pressure NAK site (via
    /// `FilesystemStore::kick_eviction_drain`).
    ///
    /// Honest scope (review 9fd52fc0 MINOR-6): the `gate ⇒ evict`
    /// composite is CLOSED BY PIECE 1 — the periodic
    /// `DRAIN_INTERVAL_SECS = 10` tick already runs the capacity drain +
    /// wedge self-heal unconditionally. This kick only removes up to one
    /// tick interval of latency (≤10 s) between a disk-NAK and the next
    /// drain pass. Cheap, so kept — but it is a latency optimisation,
    /// not the invariant's load-bearing mechanism.
    ///
    /// Non-blocking: one atomic compare-exchange + `notify_one`; no
    /// locks, no awaits — safe inline on any path. Returns whether the
    /// kick was accepted (`false` = within `DRAIN_KICK_MIN_INTERVAL_MILLIS`
    /// of the previous accepted kick, or lost a race to a concurrent
    /// kicker — either way a drain pass is already imminent).
    pub fn kick_drain(&self) -> bool {
        // `+ 1` so a stored 0 always means "never kicked".
        let now_millis = self.drain_kick_anchor.elapsed().as_millis() as u64 + 1;
        let last = self.last_drain_kick_millis.load(Ordering::Relaxed);
        #[cfg(any(test, feature = "test-utils"))]
        let min_interval = match self.test_drain_kick_interval_override.load(Ordering::Relaxed) {
            0 => DRAIN_KICK_MIN_INTERVAL_MILLIS,
            v => v,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let min_interval = DRAIN_KICK_MIN_INTERVAL_MILLIS;
        if last != 0 && now_millis.saturating_sub(last) < min_interval {
            return false;
        }
        if self
            .last_drain_kick_millis
            .compare_exchange(last, now_millis, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        // `notify_one` stores a permit if the drain arm is not currently
        // awaiting, so a kick between select iterations is never lost.
        self.drain_kick.notify_one();
        true
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
                    // Diagnostic-only (observability): surface INDEFINITE pins
                    // held past 30s without a BIS-ack. Releases nothing; shares
                    // the existing 10s maintenance cadence. Independent of
                    // `expire_stale_pins` (which SKIPS indefinite pins), so order
                    // within the tick does not matter.
                    self.sweep_stale_indefinite_pins();
                }
                _ = drain_interval.tick() => {
                    self.gated_drain_arm().await;
                }
                // (FINDING 2 piece 3) Disk-NAK drain kick: the worker's
                // disk-pressure gate wakes this arm (rate-limited at
                // `kick_drain`) so a NAK is followed by a drain pass
                // within ~0 s instead of up to one DRAIN_INTERVAL tick —
                // a ≤10 s latency optimisation on the `gate ⇒ evict`
                // composite that the periodic tick arm (piece 1) closes.
                // Runs the SAME gated body as the periodic tick.
                () = self.drain_kick.notified() => {
                    self.gated_drain_arm().await;
                }
            }
        }
    }

    /// Shared body of the periodic drain tick and the disk-NAK kick arm.
    ///
    /// (FL-688 v3 Stage C) Skips the forced drain during the worker
    /// startup reconcile window. The PRIMARY protection is reconcile-pin
    /// (`pin_digest_indefinite_with_result`); this gate is SECONDARY — it
    /// prevents the EXPLICIT periodic LRU sweep from racing the
    /// reconcile-pin call. Per-insert moka eviction (in `insert_inner`)
    /// cannot be gated here; pinning each blob handles that. Acquire
    /// matches the Release in `release_startup_reconcile_gate`.
    ///
    /// When the gate is open: forces moka's capacity check + drains the
    /// resulting eviction events on THIS task (the sole owner of the
    /// shared `pending_evictions` queue) — never a parallel task, which
    /// would double-drain the queue — then runs one eviction-wedge
    /// self-heal evaluation (FINDING 2 piece 1).
    async fn gated_drain_arm(&self) {
        if !self.reconcile_complete.load(Ordering::Acquire) {
            return;
        }
        self.run_pending_tasks_and_drain().await;
        self.maybe_selfheal_wedged_eviction().await;
    }

    async fn process_eviction_event(&self, event: EvictionEvent<K, T>) {
        let size = event.value.len();
        self.evicted_bytes.add(size);
        self.evicted_items.inc();
        // (FINDING 2 fix-up) The wedge sensor counts ONLY size evictions:
        // the moka livelock kills `RemovalCause::Size` specifically, and
        // non-Size progress (TTL `Expired` on the targetkey store's
        // `max_seconds`, explicit removes, the heal's own `Explicit`
        // invalidations) must not reset the freeze counter.
        if event.cause == RemovalCause::Size {
            self.size_evicted_items.fetch_add(1, Ordering::Relaxed);
        }

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

    /// Diagnostic sweep (observability ONLY — releases nothing): scan the
    /// `pinned` map for INDEFINITE pins held longer than
    /// `STALE_INDEFINITE_PIN_ALERT_SECS` without a BlobsInStableStorage
    /// ack ([`Self::unpin_key`]), emitting a WARN per stale pin (capped at
    /// `STALE_PIN_ALERT_MAX_LINES`) plus one summary WARN carrying the full
    /// `stale_count` / `stale_bytes` totals. Emits nothing when no pin is
    /// stale. A sustained stale set is the signature of the production
    /// indefinite-pin leak: pins that are never BIS-acked accumulate to
    /// `indefinite_pin_cap` and wedge the fleet.
    ///
    /// NEVER releases a pin — release stays BIS-ack-driven, exactly as
    /// [`Self::expire_stale_pins`] leaves indefinite pins untouched. Called
    /// once per `pin_check_interval` tick by the background `drain_evictions`
    /// loop; `#[doc(hidden)] pub` so tests can drive it deterministically.
    /// Returns the sweep summary for assertions.
    #[doc(hidden)]
    pub fn sweep_stale_indefinite_pins(&self) -> StalePinAlertSummary {
        let threshold = Duration::from_secs(STALE_INDEFINITE_PIN_ALERT_SECS);
        // Collect (key, age_secs, size) while iterating so the DashMap shard
        // guards are dropped BEFORE any `warn!` — the emission holds no lock
        // (and there is no `.await` anywhere in this method).
        let mut stale: Vec<(K, u64, u64)> = Vec::new();
        for entry in self.pinned.iter() {
            if !entry.indefinite {
                continue;
            }
            let age = entry.pinned_at.elapsed();
            if age < threshold {
                continue;
            }
            stale.push((entry.key().clone(), age.as_secs(), entry.size));
        }
        if stale.is_empty() {
            return StalePinAlertSummary::default();
        }
        let stale_count = stale.len() as u64;
        let stale_bytes: u64 = stale.iter().map(|(_, _, size)| *size).sum();
        let lines_emitted = stale.len().min(STALE_PIN_ALERT_MAX_LINES);
        for (key, age_secs, size_bytes) in stale.into_iter().take(STALE_PIN_ALERT_MAX_LINES) {
            let q: &Q = key.borrow();
            warn!(
                target: "nativelink::stale_pin_alert",
                stale_pin = %q,
                age_secs,
                size_bytes,
                "indefinite pin not BIS-acked after 30s"
            );
        }
        warn!(
            target: "nativelink::stale_pin_alert",
            stale_count,
            stale_bytes,
            "stale indefinite-pin sweep summary"
        );
        StalePinAlertSummary {
            stale_count,
            stale_bytes,
            lines_emitted,
        }
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
                // #speculative-prefetch P0: a speculative pin is TIME-BOUNDED,
                // so it reaches this sweep (evict-first). Free its sub-budget.
                if entry.speculative {
                    self.speculative_pinned_bytes
                        .fetch_sub(size, Ordering::Relaxed);
                }
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

    /// FL-688 advertise-on-pin: fire `on_pin` callbacks when `key` crosses into
    /// an INDEFINITE (held-until-BIS-ack) pin. `ts_counter` is a FRESHLY minted
    /// counter (`next_stamp`) already frozen into the value via `set_stamp`, so
    /// the holdings tracker's PRESENT delta STRICTLY out-ranks any prior ABSENT
    /// evict of this key (a frozen/insert stamp could tie-lose the ABSENT tie
    /// under the tracker LWW and be silently suppressed — F5). Fired ONCE per
    /// pin lifecycle: only at the false->true `indefinite` transition (the
    /// caller gates this), so a re-pin of an already-indefinite key never
    /// re-advertises.
    fn fire_on_pin_callbacks(&self, key: &K, size: u64, ts_counter: u64) {
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_pin(key.borrow(), size, self.boot_epoch(), ts_counter);
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

    use super::{
        MokaEvictingMap, PinnedEntry, WedgeSelfHealOutcome, PIN_SATURATION_HEADROOM_DIVISOR,
        PIN_TIMEOUT_SECS, WEDGE_FROZEN_TICKS_TRIGGER, WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION,
    };
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
            // FL-681: leave pin_cap_bytes 0 so pin_cap stays DERIVED
            // (max_bytes * PIN_CAP_FRACTION) — the boundary test relies on it.
            pin_cap_bytes: 0,
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

    // ---------------------------------------------------------------
    // Diagnostic stale-INDEFINITE-pin alert (observability ONLY): an
    // indefinite pin held past STALE_INDEFINITE_PIN_ALERT_SECS without a
    // BIS-ack is surfaced by `sweep_stale_indefinite_pins`. The sweep
    // NEVER releases a pin — these tests assert the DETECTION contract
    // (which pins are flagged) and the flood cap.
    // ---------------------------------------------------------------

    /// Rewind a pinned entry's `pinned_at` by `secs` so it reads as aged.
    fn age_indefinite_pin_secs(map: &TestMapCb, key: u64, secs: u64) {
        let mut entry = map.pinned.get_mut(&key).expect("key should be pinned");
        entry.pinned_at = Instant::now() - core::time::Duration::from_secs(secs);
    }

    #[tokio::test]
    async fn stale_pin_alert_flags_only_aged_indefinite_pins() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // key 0: indefinite + aged 31s → STALE.
        map.insert(0, BytesEntry(2048)).await;
        assert!(map.pin_key_indefinite(0), "indefinite pin of key 0");
        age_indefinite_pin_secs(&map, 0, 31);

        // key 1: indefinite + fresh (age ~0) → NOT stale (below 30s).
        map.insert(1, BytesEntry(4096)).await;
        assert!(map.pin_key_indefinite(1), "indefinite pin of key 1");

        // key 2: TIME-BOUNDED + aged 31s → NOT stale (not indefinite).
        map.insert(2, BytesEntry(8192)).await;
        assert!(map.pin_key(2), "time-bounded pin of key 2");
        age_indefinite_pin_secs(&map, 2, 31);

        let summary = map.sweep_stale_indefinite_pins();
        assert_eq!(
            summary.stale_count, 1,
            "only the AGED INDEFINITE pin (key 0) is stale — a fresh indefinite \
             pin (key 1) and an aged TIME-BOUNDED pin (key 2) must be excluded"
        );
        assert_eq!(
            summary.stale_bytes, 2048,
            "stale_bytes must be key 0's size only (2048), not key 1/2's bytes"
        );
        assert_eq!(
            summary.lines_emitted, 1,
            "exactly one per-pin WARN line for the single stale pin"
        );

        // Observability only: the sweep must NOT release the pin.
        assert!(
            map.pinned.contains_key(&0u64),
            "sweep must NOT unpin a stale indefinite pin — release is BIS-ack-only"
        );
        assert_eq!(
            map.pinned_bytes(),
            2048 + 4096 + 8192,
            "no pin may be released by the diagnostic sweep"
        );
    }

    #[tokio::test]
    async fn stale_pin_alert_emits_nothing_when_none_stale() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // Fresh indefinite pin (age ~0) — not stale.
        map.insert(0, BytesEntry(2048)).await;
        assert!(map.pin_key_indefinite(0), "indefinite pin");

        let summary = map.sweep_stale_indefinite_pins();
        assert_eq!(
            summary,
            super::StalePinAlertSummary::default(),
            "no indefinite pin is past 30s — the sweep must report an empty summary \
             (stale_count / stale_bytes / lines_emitted all zero)"
        );
    }

    #[tokio::test]
    async fn stale_pin_alert_caps_warn_lines_but_summary_counts_all() {
        // Large pin budget so 60 indefinite pins all fit.
        let cfg = policy(64 * 1024 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        let n: u64 = super::STALE_PIN_ALERT_MAX_LINES as u64 + 10; // 60
        for k in 0..n {
            map.insert(k, BytesEntry(1024)).await;
            assert!(map.pin_key_indefinite(k), "indefinite pin");
            age_indefinite_pin_secs(&map, k, 31);
        }

        let summary = map.sweep_stale_indefinite_pins();
        assert_eq!(
            summary.stale_count, n,
            "summary must count ALL stale pins ({n}), not just the emitted lines"
        );
        assert_eq!(
            summary.stale_bytes,
            n * 1024,
            "summary bytes must total ALL stale pins"
        );
        assert_eq!(
            summary.lines_emitted,
            super::STALE_PIN_ALERT_MAX_LINES,
            "per-pin WARN lines must be capped at STALE_PIN_ALERT_MAX_LINES (50) to \
             avoid flooding, even though 60 pins are stale"
        );
    }

    #[tokio::test]
    async fn stale_pin_alert_threshold_is_30s() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // key 0 aged 29s → below threshold → NOT stale.
        map.insert(0, BytesEntry(1024)).await;
        assert!(map.pin_key_indefinite(0), "indefinite pin key 0");
        age_indefinite_pin_secs(&map, 0, 29);

        // key 1 aged 31s → above threshold → stale.
        map.insert(1, BytesEntry(1024)).await;
        assert!(map.pin_key_indefinite(1), "indefinite pin key 1");
        age_indefinite_pin_secs(&map, 1, 31);

        let summary = map.sweep_stale_indefinite_pins();
        assert_eq!(
            summary.stale_count, 1,
            "only the pin aged past STALE_INDEFINITE_PIN_ALERT_SECS (30s) is stale: \
             key 1 (31s) yes, key 0 (29s) no"
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

    // FL-681 NAK boundary fix: the admission-side gate reads
    // `indefinite_pin_saturated()` to decide whether to NAK a new action with
    // `ResourceExhausted`. Post-fix the predicate gates on the TOTAL real-pin
    // budget (`real_pinned = pinned_bytes − speculative_pinned_bytes`) vs
    // `pin_cap` MINUS a 5% headroom band, so it fires as soon as the total
    // refusal at `pin_key_with_mode` (which also gates on `real_pinned` vs
    // `pin_cap`) is imminent — NOT one blob after `pinned_bytes` reaches the
    // exact cap. The OLD predicate (`indefinite_pinned_bytes >= indefinite_pin_cap`)
    // never fired with variable-sized outputs because a refused pin adds no
    // bytes, so `indefinite_pinned_bytes` stuck below the cap forever.
    //
    // `pin_cap` is DERIVED (`max_bytes * PIN_CAP_FRACTION = 0.25`), so choose
    // `max_bytes = 16384` → `pin_cap = 4096`, headroom threshold
    // `4096 − 4096/PIN_SATURATION_HEADROOM_DIVISOR = 4096 − 204 = 3892`. This
    // uses the REAL total-pin budget (`pin_cap`) — NOT `indefinite_pin_cap`
    // (the old, wrong field); `make_map_cb` leaves `indefinite_pin_cap`
    // defaulted to `pin_cap` so it does not confound the boundary.
    #[tokio::test]
    async fn real_pin_saturated_fires_before_hard_cap_with_variable_sizes() {
        // max_bytes 16384 → derived pin_cap = 4096; headroom threshold = 3892.
        let cfg = policy(16384, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // (a) Pin a 3900-byte entry indefinitely → real_pinned = 3900. This is
        // BELOW the hard cap (4096) but AT/ABOVE the 3892 headroom threshold.
        map.insert(0u64, BytesEntry(3900)).await;
        assert!(map.pin_key_indefinite(0), "first indefinite pin (3900 B) fits under pin_cap 4096");
        assert_eq!(
            map.pinned_bytes(),
            3900,
            "real pin should account 3900 bytes after the first indefinite pin"
        );

        // (b) Attempt a second pin that OVERSHOOTS the cap: 3900 + 300 = 4200 > 4096
        // → refused by the total pin-cap check. real_pinned STAYS 3900 (a refused
        // pin adds no bytes) — this is exactly the window the old `>= cap` gate missed.
        map.insert(1u64, BytesEntry(300)).await;
        assert!(
            !map.pin_key_indefinite(1),
            "second indefinite pin (300 B) must be REFUSED: 3900 + 300 = 4200 exceeds pin_cap 4096"
        );
        assert_eq!(
            map.pinned_bytes(),
            3900,
            "a refused pin must add no bytes — real_pinned stays 3900 (the boundary-gap window)"
        );

        // (c) The gate MUST fire at 3900: 3900 >= 3892 (pin_cap − 5%). This is the
        // refuse-but-below-hard-cap window: further real pins are already being
        // refused, so admission must NAK. The OLD gate (>= 4096) would report
        // FALSE here (3900 >= 4096 is false) → boundary gap → NAK never fires.
        assert!(
            map.indefinite_pin_saturated(),
            "boundary gap: saturated false while pins are being refused — the gate must fire \
             at real_pinned 3900 >= pin_cap−5% (3892), the window where the total pin cap is \
             already refusing new pins; the old `>= pin_cap` gate missed it and the NAK was dead"
        );

        // (d) A BIS-ack `unpin_key` frees headroom → de-saturates (transient
        // backpressure, not a terminal stall).
        map.unpin_key(&0);
        assert!(
            !map.indefinite_pin_saturated(),
            "indefinite_pin_saturated must clear once a BIS-ack frees pin_cap headroom — \
             real_pinned drops to 0 (< 3892), the gate is transient backpressure"
        );
    }

    // FL-681 NAK boundary fix (de-confound MINOR-a): the predicate's NUMERATOR
    // is `real_pinned = pinned_bytes − speculative_pinned_bytes`, NOT
    // `pinned_bytes` and NOT `indefinite_pinned_bytes`. The original boundary
    // test pinned a single indefinite blob, collapsing all three to the same
    // value — so a numerator mutation to `pinned_bytes` or `indefinite_pinned_bytes`
    // wrongly PASSED. This test holds a SPECULATIVE pin (so pinned_bytes >
    // real_pinned) AND a TIME-BOUNDED real pin (so real_pinned >
    // indefinite_pinned_bytes), then checks two boundary points that only
    // `real_pinned` gets right.
    #[tokio::test]
    async fn real_pin_saturated_numerator_excludes_speculative_and_isnt_indefinite_only() {
        // max_bytes 16384 → pin_cap = 4096, threshold = 3892,
        // speculative_pin_cap = 16384 * 0.05 = 819.
        let cfg = policy(16384, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // Speculative pin of 800 B (fits the 819 speculative sub-budget). It
        // adds to pinned_bytes AND speculative_pinned_bytes, so it is EXCLUDED
        // from real_pinned.
        map.insert(10u64, BytesEntry(800)).await;
        assert!(map.pin_key_speculative(10), "speculative pin (800 B) fits the 819 sub-budget");
        assert_eq!(map.speculative_pinned_bytes(), 800, "speculative gauge must read 800");

        // Indefinite pin of 3800 B → real_pinned = 3800 (indefinite counts as
        // real), pinned_bytes = 4600, indefinite_pinned_bytes = 3800.
        map.insert(11u64, BytesEntry(3800)).await;
        assert!(map.pin_key_indefinite(11), "indefinite pin (3800 B) fits: real 0+3800 <= 4096");
        assert_eq!(map.pinned_bytes(), 4600, "pinned_bytes = 800 spec + 3800 indefinite");
        assert_eq!(map.indefinite_pinned_bytes(), 3800, "indefinite subset = 3800");

        // CHECKPOINT 1 — real_pinned = 4600 − 800 = 3800 (< 3892) → NOT saturated.
        // A `pinned_bytes` numerator (4600 >= 3892) would WRONGLY report saturated
        // here → this assertion RED-fails under numerator → pinned_bytes.
        assert!(
            !map.indefinite_pin_saturated(),
            "numerator must EXCLUDE speculative: real_pinned = pinned_bytes(4600) − \
             speculative(800) = 3800 < threshold 3892 → NOT saturated. A pinned_bytes numerator \
             (4600 >= 3892) would wrongly gate a worker whose real pin budget still has headroom"
        );

        // Add a TIME-BOUNDED (non-indefinite, non-speculative) real pin of 200 B
        // → real_pinned = 4000 (>= 3892), but indefinite_pinned_bytes stays 3800
        // (< 3892). Total refusal: real 3800 + 200 = 4000 <= 4096 → admitted.
        map.insert(12u64, BytesEntry(200)).await;
        assert!(map.pin_key(12), "time-bounded pin (200 B) fits: real 3800+200 = 4000 <= 4096");
        assert_eq!(map.pinned_bytes(), 4800, "pinned_bytes = 800 + 3800 + 200");
        assert_eq!(map.indefinite_pinned_bytes(), 3800, "indefinite subset UNCHANGED at 3800");

        // CHECKPOINT 2 — real_pinned = 4800 − 800 = 4000 (>= 3892) → SATURATED.
        // An `indefinite_pinned_bytes` numerator (3800 < 3892) would WRONGLY report
        // NOT saturated here → this assertion RED-fails under numerator →
        // indefinite_pinned_bytes.
        assert!(
            map.indefinite_pin_saturated(),
            "numerator must be real_pinned, NOT indefinite_pinned_bytes: real_pinned = 4000 \
             (indefinite 3800 + time-bounded 200) >= threshold 3892 → SATURATED. An \
             indefinite_pinned_bytes numerator (3800 < 3892) would leave the gate dead while \
             the total pin budget is full of time-bounded + indefinite real pins"
        );
    }

    // FL-681 NAK boundary fix (de-confound MINOR-b): the predicate's CEILING is
    // `pin_cap` (total pin budget), NOT `indefinite_pin_cap`. The original test's
    // helper left `indefinite_pin_cap == pin_cap`, masking the choice. Here
    // `indefinite_pin_cap` is set FAR above `pin_cap` so that gating on the wrong
    // ceiling would never fire.
    #[tokio::test]
    async fn real_pin_saturated_ceiling_is_pin_cap_not_indefinite_cap() {
        // max_bytes 16384 → derived pin_cap = 4096, threshold = 3892.
        // indefinite_pin_cap set to 100_000 (>> pin_cap) via the indefinite-cap
        // helper — a deliberately distinguishable ceiling.
        let cfg = policy(16384, 0);
        let map = Arc::new(make_map_cb_indefinite_cap(&cfg, 100_000));

        // Indefinite pin of 3900 B → real_pinned = 3900 (>= 3892). It fits the
        // total pin cap (0 + 3900 <= 4096) AND the large indefinite cap.
        map.insert(20u64, BytesEntry(3900)).await;
        assert!(map.pin_key_indefinite(20), "indefinite pin (3900 B) fits both caps");
        assert_eq!(map.pinned_bytes(), 3900, "real_pinned = 3900");

        // real_pinned 3900 >= pin_cap−5% (3892) → SATURATED against pin_cap.
        // Against indefinite_pin_cap the threshold would be 100_000 − 5_000 =
        // 95_000, so 3900 >= 95_000 is FALSE → an indefinite_pin_cap ceiling
        // would report NOT saturated → this assertion RED-fails under ceiling →
        // indefinite_pin_cap.
        assert!(
            map.indefinite_pin_saturated(),
            "ceiling must be pin_cap (4096), NOT indefinite_pin_cap (100_000): real_pinned 3900 \
             >= pin_cap−5% (3892) → SATURATED. An indefinite_pin_cap ceiling would need \
             real_pinned >= 95_000 and never fire — the gate would be dead exactly as before"
        );
    }

    // FL-681 NAK boundary fix (de-confound MINOR-c): pin the 5% band WIDTH and
    // POSITION. Asserts the constant is 20 (declaration-site value) AND that a
    // real_pinned BELOW the threshold does NOT saturate — a divisor mutation
    // (e.g. 20→2, which widens the band to 50%) would fire early and RED-fail
    // the below-threshold assertion.
    #[tokio::test]
    async fn real_pin_saturated_band_is_five_percent_at_divisor_twenty() {
        assert_eq!(
            PIN_SATURATION_HEADROOM_DIVISOR, 20,
            "the saturation headroom band is pin_cap/PIN_SATURATION_HEADROOM_DIVISOR; the \
             boundary tests assume divisor 20 (5%). If this changed, re-derive the thresholds."
        );

        // max_bytes 16384 → pin_cap = 4096; divisor 20 → threshold = 4096 − 204 = 3892.
        let cfg = policy(16384, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // Pin 3800 B indefinitely → real_pinned = 3800, which is BELOW the
        // divisor-20 threshold (3892) but ABOVE a divisor-2 threshold (2048).
        map.insert(30u64, BytesEntry(3800)).await;
        assert!(map.pin_key_indefinite(30), "indefinite pin (3800 B) fits under pin_cap 4096");
        assert_eq!(map.pinned_bytes(), 3800, "real_pinned = 3800");

        // 3800 < 3892 → NOT saturated at divisor 20. A divisor of 2 would put the
        // threshold at 2048, so 3800 >= 2048 → wrongly SATURATED → this assertion
        // RED-fails under divisor 20→2 (band too wide, gate fires too early).
        assert!(
            !map.indefinite_pin_saturated(),
            "band position: real_pinned 3800 is BELOW the divisor-20 threshold (3892) so the gate \
             must NOT fire. A wider band (e.g. divisor 2 → threshold 2048) would saturate here and \
             NAK a worker with real headroom — over-eager backpressure churns the scheduler"
        );

        // Sanity: crossing to 3900 (>= 3892) DOES saturate — the band's near edge.
        map.unpin_key(&30);
        map.insert(31u64, BytesEntry(3900)).await;
        assert!(map.pin_key_indefinite(31), "indefinite pin (3900 B) fits under pin_cap 4096");
        assert!(
            map.indefinite_pin_saturated(),
            "band near-edge: real_pinned 3900 >= threshold 3892 → SATURATED (confirms the band \
             fires at 5% below pin_cap, not lower)"
        );
    }

    // FL-681 NAK boundary fix (§2 config knob): the per-store `pin_cap_bytes`
    // config field must OVERRIDE the derived 25%-of-max_bytes `pin_cap` when
    // non-zero, and fall back to the derived value when 0. This is what lets an
    // operator raise the pin budget to 50% of max_bytes without touching max_bytes.
    // FL-681 prod-incident probe (2026-07-11): the deployed config's
    // `pin_cap_bytes: 20000000000` did NOT take effect (enforced pin_cap stayed
    // at the 25% derived 10GB). The override test below sets the field DIRECTLY,
    // so it cannot catch a DESERIALIZATION gap. This parses the EXACT deployed
    // JSON to test the full config→pin_cap path.
    #[tokio::test]
    async fn pin_cap_bytes_deserializes_from_config_json_and_reaches_pin_cap() {
        let json = r#"{"max_bytes": 40000000000, "pin_cap_bytes": 20000000000}"#;
        let cfg: EvictionPolicy =
            serde_json::from_str(json).expect("eviction_policy must deserialize");
        assert_eq!(cfg.max_bytes, 40_000_000_000, "max_bytes control (known-working)");
        assert_eq!(
            cfg.pin_cap_bytes, 20_000_000_000,
            "DESERIALIZATION dropped pin_cap_bytes (got {}) — would explain prod pin_cap=10GB",
            cfg.pin_cap_bytes
        );
        let map = Arc::new(make_map_cb(&cfg));
        assert_eq!(
            map.pin_cap, 20_000_000_000,
            "deserialized pin_cap_bytes must reach the eviction map's pin_cap (got {})",
            map.pin_cap
        );
    }

    #[tokio::test]
    async fn pin_cap_bytes_config_overrides_derived_pin_cap() {
        // Derived-default path: pin_cap_bytes 0 → pin_cap = max_bytes * 25%.
        let derived_cfg = policy(16384, 0);
        let derived_map = Arc::new(make_map_cb(&derived_cfg));
        assert_eq!(
            derived_map.pin_cap,
            4096,
            "pin_cap_bytes=0 must derive pin_cap as max_bytes(16384) * PIN_CAP_FRACTION(0.25) = 4096"
        );

        // Override path: a non-zero pin_cap_bytes must be used verbatim, even
        // when it differs from the derived 25% (here 8192 = 50% of max_bytes).
        let mut override_cfg = policy(16384, 0);
        override_cfg.pin_cap_bytes = 8192;
        let override_map = Arc::new(make_map_cb(&override_cfg));
        assert_eq!(
            override_map.pin_cap,
            8192,
            "a non-zero pin_cap_bytes(8192) must override the derived 25% cap(4096) — \
             the operator-tuned total pin budget did not reach the eviction map"
        );
        // And the override cap is the ceiling the total pin refusal / NAK gate
        // now measure against: a 5000-byte pin fits under 8192 but would have
        // been refused under the derived 4096.
        override_map.insert(0u64, BytesEntry(5000)).await;
        assert!(
            override_map.pin_key_indefinite(0),
            "a 5000-byte pin must fit under the overridden 8192-byte pin_cap (it would \
             exceed the derived 4096 cap) — proving pin_cap_bytes governs admission"
        );
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
    // ---------------------------------------------------------------
    // #speculative-prefetch P0 (invariant-prover BLOCK, TLC-proven):
    // the speculative pin sub-budget is DISJOINT from the real-action
    // pin budget — a speculative pin can NEVER refuse a real pin.
    // See `.claude/tla/SpeculativePinBudgetFixed.tla` (NoStarve HOLDS).
    // ---------------------------------------------------------------

    /// A speculative pin that has consumed the *shared* `pinned_bytes`
    /// must NOT count against the real-action `pin_cap` — otherwise a
    /// concurrent real action's `pin_key` is refused, its blob stays
    /// LRU-evictable, and its populate is starved (the C3/C5 corner the
    /// invariant-prover machine-checked as BROKEN under the shared
    /// budget). The fix subtracts `speculative_pinned_bytes` from the
    /// real-pin admission check, making the two budgets disjoint.
    #[tokio::test]
    async fn speculative_pin_never_refuses_a_real_pin() {
        // max_bytes = 100 KiB -> pin_cap = 25 KiB (25%). Speculative pins
        // are held under their OWN small cap; here we saturate the SHARED
        // `pinned_bytes` with speculative pins, then assert a real pin
        // STILL succeeds. Under the pre-fix shared budget it would be refused.
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // Speculative cap = 5% of max_bytes = 5 KiB. Insert + speculatively
        // pin blobs totalling 5 KiB — fills the speculative sub-budget AND
        // adds 5 KiB to the shared `pinned_bytes`.
        for k in 0..5u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        for k in 0..5u64 {
            assert!(
                map.pin_key_speculative(k),
                "speculative pin {k} must fit under the 5 KiB speculative sub-budget"
            );
        }
        assert_eq!(
            map.speculative_pinned_bytes(),
            5 * 1024,
            "5 speculative pins of 1 KiB each account for exactly 5 KiB"
        );
        assert_eq!(
            map.pinned_bytes(),
            5 * 1024,
            "speculative pins are a SUBSET of the shared pinned_bytes total"
        );

        // A REAL action pins a 20 KiB blob then a 4 KiB blob -> real-only
        // total 24 KiB (<= 25 KiB pin_cap -> both must ADMIT). Under the
        // pre-fix shared accounting the shared total would be 5 + 24 = 29 KiB
        // > 25 KiB pin_cap, refusing the real pin — the starvation bug.
        map.insert(100, BytesEntry(20 * 1024)).await;
        map.insert(101, BytesEntry(4 * 1024)).await;
        assert!(
            map.pin_key(100),
            "composite invariant violated (2026-07-05): speculative pins refused a \
             real action's 20 KiB pin via the shared pin_cap (disjoint sub-budget \
             not subtracted from the real-pin admission check)"
        );
        assert!(
            map.pin_key(101),
            "composite invariant violated (2026-07-05): speculative pins refused a \
             real action's 4 KiB pin — real-only total is 24 KiB <= 25 KiB pin_cap; \
             only the shared-budget bug (counting 5 KiB speculative into the real \
             check -> 29 KiB > 25 KiB) can refuse it"
        );
        // Real pins are NOT speculative -> speculative total unchanged.
        assert_eq!(
            map.speculative_pinned_bytes(),
            5 * 1024,
            "a real pin must not be accounted as speculative"
        );
    }

    /// The speculative sub-budget is itself bounded (evict-first, not
    /// unbounded): a speculative pin over the small speculative cap is
    /// REFUSED (backpressure), never dropped. Mirrors the FL-681
    /// indefinite-cap backpressure contract.
    #[tokio::test]
    async fn speculative_pin_cap_refuses_over_budget_not_drops() {
        // max_bytes = 100 KiB -> speculative cap = 5 KiB (5%).
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));
        for k in 0..7u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        // First 5 fit (5 KiB); the 6th would push to 6 KiB > 5 KiB cap.
        for k in 0..5u64 {
            assert!(map.pin_key_speculative(k), "speculative pin {k} fits under cap");
        }
        assert!(
            !map.pin_key_speculative(5),
            "speculative pin over the 5 KiB sub-budget must REFUSE (backpressure)"
        );
        assert_eq!(
            map.speculative_pinned_bytes(),
            5 * 1024,
            "over-cap speculative pin must not be accounted (refused, not dropped)"
        );
        // The refused blob is still present — NOT lost.
        assert!(
            map.get(&5).await.is_some(),
            "backpressure must NOT drop the blob: refused speculative-pin source stays readable"
        );
    }

    /// Releasing a speculative pin (`unpin_key` on TTL sweep / adoption)
    /// must decrement `speculative_pinned_bytes` symmetrically, so the
    /// sub-budget headroom is reclaimed — otherwise the speculative total
    /// leaks upward and every later speculative pin is wrongly refused.
    #[tokio::test]
    async fn unpin_of_speculative_pin_restores_speculative_bytes() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));
        for k in 0..5u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        for k in 0..5u64 {
            assert!(map.pin_key_speculative(k));
        }
        assert_eq!(map.speculative_pinned_bytes(), 5 * 1024);
        // Unpin one -> speculative total drops by exactly that blob's size.
        map.unpin_key(&0);
        assert_eq!(
            map.speculative_pinned_bytes(),
            4 * 1024,
            "unpin of a speculative pin must decrement speculative_pinned_bytes \
             (leak -> sub-budget saturates -> later speculative pins wrongly refused)"
        );
        assert_eq!(
            map.pinned_bytes(),
            4 * 1024,
            "unpin must also decrement the shared pinned_bytes for the removed entry"
        );
        // Reclaimed headroom is reusable: a fresh speculative pin now fits.
        map.insert(10, BytesEntry(1024)).await;
        assert!(
            map.pin_key_speculative(10),
            "after unpin freed sub-budget headroom, a new speculative pin must succeed"
        );
    }

    /// #speculative-prefetch C1 (perf-optimizer): promoting a SPECULATIVE pin to
    /// INDEFINITE (`pin_key_indefinite` upgrade) must RECLASSIFY it spec→real —
    /// an indefinite pin IS a real pin (held until BIS-ack, TTL-exempt). If the
    /// upgrade left the entry counted in `speculative_pinned_bytes`, the entry
    /// would (a) stay TTL-exempt yet inflate the speculative gauge
    /// semi-permanently (shrinking its own 5% sub-budget → later legit
    /// speculative pins wrongly refused) and (b) keep being SUBTRACTED from the
    /// real-pin admission check (real headroom undercounted). The fix decrements
    /// `speculative_pinned_bytes` and clears the `speculative` flag on upgrade,
    /// making the `PinnedEntry` "mutually exclusive with indefinite" doc honest.
    #[tokio::test]
    async fn indefinite_upgrade_of_speculative_pin_reclassifies_to_real() {
        // max_bytes = 100 KiB → pin_cap = 25 KiB, speculative_pin_cap = 5 KiB,
        // indefinite_pin_cap defaults to pin_cap (25 KiB).
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        map.insert(1, BytesEntry(4 * 1024)).await;
        assert!(
            map.pin_key_speculative(1),
            "the 4 KiB speculative pin fits the 5 KiB speculative sub-budget"
        );
        assert_eq!(
            map.speculative_pinned_bytes(),
            4 * 1024,
            "speculative pin accounts 4 KiB in the speculative sub-budget"
        );
        assert_eq!(map.indefinite_pinned_bytes(), 0, "not yet indefinite");
        assert_eq!(map.pinned_bytes(), 4 * 1024, "one 4 KiB pin in the shared total");

        // UPGRADE the same digest to an indefinite pin (the input-digest ==
        // F2-output-digest coincidence). The entry must be RECLASSIFIED: its
        // 4 KiB moves OUT of `speculative_pinned_bytes` INTO the indefinite
        // accounting; the shared `pinned_bytes` is unchanged (same entry).
        assert!(
            map.pin_key_indefinite(1),
            "indefinite upgrade of an existing (speculative) pin must succeed"
        );
        assert_eq!(
            map.speculative_pinned_bytes(),
            0,
            "C1: indefinite upgrade must DECREMENT speculative_pinned_bytes and \
             clear the speculative flag — an indefinite pin is real, not speculative \
             (leaving it inflates the gauge semi-permanently and undercounts real headroom)"
        );
        assert_eq!(
            map.indefinite_pinned_bytes(),
            4 * 1024,
            "the reclassified pin is now counted as indefinite"
        );
        assert_eq!(
            map.pinned_bytes(),
            4 * 1024,
            "reclassification does not change the shared pinned_bytes (same entry)"
        );

        // Now that the entry is real (not subtracted from the real-pin check),
        // real-pin admission sees the full 4 KiB as real. Fill the real budget
        // to exactly pin_cap using the reclassified 4 KiB as real headroom: a
        // 21 KiB real pin fits (4 + 21 = 25 = pin_cap), a further 1 KiB does NOT.
        map.insert(2, BytesEntry(21 * 1024)).await;
        assert!(
            map.pin_key(2),
            "a 21 KiB real pin fits: reclassified 4 KiB + 21 KiB = 25 KiB = pin_cap"
        );
        map.insert(3, BytesEntry(1024)).await;
        assert!(
            !map.pin_key(3),
            "C1: real-pin admission must COUNT the reclassified pin as real — real \
             total is already at the 25 KiB pin_cap (4 reclassified + 21), so a further \
             1 KiB real pin must be REFUSED. If the 4 KiB were still subtracted as \
             speculative, real_pinned would read 21 KiB and this pin would wrongly admit."
        );
    }

    /// #speculative-prefetch testing-czar (a): the BATCH `pin_keys` real-pin
    /// admission site must ALSO exclude speculative bytes (disjoint sub-budget).
    /// The two existing no-starve tests exercise only the per-digest `pin_key`
    /// path; `pin_keys` has its OWN `saturating_sub(speculative_pinned_bytes)`
    /// site that is otherwise unexercised — and this is a sensitive pin path
    /// (2026-05-08 OOM class). Saturate the speculative sub-budget, then assert a
    /// BATCH real pin STILL admits every key.
    #[tokio::test]
    async fn batch_pin_keys_never_refuses_a_real_pin_over_speculative_pins() {
        // max_bytes = 100 KiB → pin_cap = 25 KiB (25%), speculative cap = 5 KiB.
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));

        // Saturate the 5 KiB speculative sub-budget (adds 5 KiB to the shared
        // pinned_bytes) via the per-digest speculative path.
        for k in 0..5u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        for k in 0..5u64 {
            assert!(map.pin_key_speculative(k), "speculative pin {k} fits the 5 KiB cap");
        }
        assert_eq!(map.speculative_pinned_bytes(), 5 * 1024);
        assert_eq!(map.pinned_bytes(), 5 * 1024);

        // A REAL BATCH pin of two DIFFERENT digests: 20 KiB + 4 KiB. Real-only
        // total is 24 KiB ≤ 25 KiB pin_cap → BOTH must be admitted by `pin_keys`.
        // Under the pre-fix shared budget the batch check would see
        // 5 + 20 = 25 (ok) then 25 + 4 = 29 > 25 → `break` after the first,
        // returning 1 (the second real pin starved by the speculative bytes).
        map.insert(100, BytesEntry(20 * 1024)).await;
        map.insert(101, BytesEntry(4 * 1024)).await;
        let pinned = map.pin_keys(&[100, 101]);
        assert_eq!(
            pinned,
            2,
            "composite invariant violated (2026-07-05): the BATCH pin_keys real-pin \
             check refused a real action's pins over saturated speculative bytes — \
             real-only total is 24 KiB ≤ 25 KiB pin_cap; only the shared-budget bug \
             (counting 5 KiB speculative into the batch check → 29 KiB > 25 KiB) can \
             leave the second key unpinned. The disjoint speculative sub-budget must \
             be subtracted from the pin_keys admission check too."
        );
        assert_eq!(
            map.speculative_pinned_bytes(),
            5 * 1024,
            "a real batch pin must not be accounted as speculative"
        );
    }

    // ---------------------------------------------------------------
    // FINDING 2 (moka-eviction-wedge): self-healing fallback evictor +
    // wedge observability + disk-NAK drain kick.
    //
    // Simulation seam: a genuinely over-budget cache with a FRESH
    // `weighted_size()` is by construction the WEDGED state (a completed
    // moka maintenance pass either evicts to cap or is livelocked on the
    // stale probation front), which a test cannot reproduce without
    // moka's bug — and since the 0.12.16 bump (upstream fix
    // moka-rs/moka#592) it cannot be reproduced against the pinned moka
    // AT ALL, which is exactly why this seam is the only way to keep the
    // self-heal's own logic under test as a backstop.
    // `test_inflate_wedge_observation(extra)` therefore adds
    // a PHANTOM stuck overage on top of the REAL residency — the
    // observation falls as the heal evicts real entries, so the
    // convergence/stop logic is exercised for real — while the
    // EVICTION-EXECUTION half always runs against the REAL map (real
    // `cache.iter()` walk, real `invalidate`, real listener → event →
    // unref → removal callbacks).
    // ---------------------------------------------------------------

    /// (FINDING 2) Budget for the wedge tests: 100 KiB = 100 weigher
    /// KB-units, so 1024-byte entries map 1:1 onto weights.
    const WEDGE_MAX_BYTES: usize = 100 * 1024;

    /// Map holding `n` 1 KiB entries inserted via the RUNTIME path (each
    /// insert enforces the cap; with `n` ≤ 100 the cache is under budget,
    /// no eviction fires, `evicted_items` stays frozen at 0).
    async fn wedge_map_with_entries(n: u64) -> (Arc<TestMapCb>, Arc<AtomicU64>) {
        let cfg = policy(WEDGE_MAX_BYTES, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);
        for k in 0..n {
            map.insert(k, BytesEntry(1024)).await;
        }
        (map, removal_count)
    }

    /// Piece 1 primary contract (invariant-walk test): the fallback
    /// evictor FIRES after `WEDGE_FROZEN_TICKS_TRIGGER` consecutive
    /// at-or-over-budget evaluations with frozen evictions, and evicts
    /// the overshoot from the REAL map through the NORMAL eviction
    /// pipeline.
    ///
    /// Mutation step: comment out the trigger (the `Fired` branch call to
    /// the heal loop, or the frozen-tick accumulation) in
    /// `maybe_selfheal_wedged_eviction` → red-fails with the bespoke
    /// "fallback evictor must FIRE" message.
    #[tokio::test]
    async fn fallback_evictor_fires_when_eviction_wedged_at_cap() {
        let (map, removal_count) = wedge_map_with_entries(50).await;
        // Inject an 80 KiB phantom stuck overage: observed = 50 KiB real
        // + 80 KiB = 130 KiB, strictly over the 100 KiB budget (30 KiB
        // overshoot). Real map: 50 KiB resident, size-evictions frozen.
        map.test_inflate_wedge_observation(80 * 1024);

        for expect_ticks in 1..WEDGE_FROZEN_TICKS_TRIGGER {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::Accumulating(expect_ticks),
                "below the {WEDGE_FROZEN_TICKS_TRIGGER}-evaluation trigger the fallback must only \
                 ACCUMULATE frozen ticks (got {outcome:?} at tick {expect_ticks})"
            );
        }
        let outcome = tokio::time::timeout(
            core::time::Duration::from_secs(5),
            map.test_maybe_selfheal_wedged_eviction(),
        )
        .await
        .expect("must not deadlock — self-heal eviction-event drain wedged");
        match outcome {
            WedgeSelfHealOutcome::Fired {
                evicted_count,
                evicted_bytes,
                rounds,
            } => {
                assert_eq!(
                    evicted_count, 30,
                    "self-heal must evict exactly the overshoot (30 KiB / 1 KiB entries = 30) \
                     and STOP once the observation converges to the budget"
                );
                assert_eq!(
                    evicted_bytes,
                    30 * 1024,
                    "self-heal evicted_bytes must equal the 30 KiB overshoot"
                );
                assert_eq!(
                    rounds, 1,
                    "a 30-entry overshoot must heal in one key-walk round (the round-2 \
                     re-read sees 20 KiB real + 80 KiB phantom = exactly max -> at-or-under \
                     -> converged)"
                );
            }
            other => panic!(
                "moka-wedge self-heal: fallback evictor must FIRE after \
                 {WEDGE_FROZEN_TICKS_TRIGGER} frozen strictly-over-budget evaluations — \
                 without it eviction stays permanently dead while over budget (FINDING 2 \
                 fleet artifact: 3.36x over max_bytes, size-evictions frozen 8h36m at \
                 scrape); got {other:?}"
            ),
        }
        // Index-visibility: fallback evictions must flow through the
        // normal eviction listener → event → removal callback pipeline
        // (disk file + index entry + BlobChangeTracker delta stay
        // consistent).
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            30,
            "fallback evictions must fire the normal removal callbacks (index-visibility \
             contract) — got {} callback firings for 30 evictions",
            removal_count.load(Ordering::Relaxed),
        );
        // Piece 2 accounting.
        assert!(
            map.eviction_wedge_detected.load(Ordering::Relaxed),
            "piece 2: eviction_wedge_detected gauge must be 1 while the trigger condition holds"
        );
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            1,
            "piece 1: eviction_wedge_selfheal_total must count exactly ONE firing (one warn \
             per firing, not per evicted entry)"
        );
        // Restoring the real (under-budget) observation resets state and
        // clears the gauge on the next evaluation.
        map.test_inflate_wedge_observation(0);
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert_eq!(
            outcome,
            WedgeSelfHealOutcome::UnderBudget,
            "with the phantom overage cleared the real 20 KiB residency is under budget"
        );
        assert!(
            !map.eviction_wedge_detected.load(Ordering::Relaxed),
            "piece 2: the wedge gauge must CLEAR once the cache is observed under budget"
        );
    }

    /// Inert direction 1: strictly under `max_bytes` the fallback never
    /// accumulates or fires; an interleaved under-budget observation
    /// RESETS the consecutive-frozen-tick count.
    ///
    /// Mutation step: remove the `observed < max_bytes` early-return (or
    /// the frozen-tick reset in it) → red-fails with the bespoke
    /// under-budget / reset messages.
    #[tokio::test]
    async fn fallback_evictor_inert_below_max_and_requires_consecutive_ticks() {
        let (map, removal_count) = wedge_map_with_entries(50).await;

        // 49 KiB phantom: observed = 50 + 49 = 99 KiB, strictly under.
        map.test_inflate_wedge_observation(49 * 1024);
        for _ in 0..5 {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::UnderBudget,
                "under-budget evaluations must be inert — a fallback that fires below \
                 max_bytes would evict a healthy cache (got {outcome:?})"
            );
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "no fallback eviction may fire below max_bytes"
        );
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            0,
            "selfheal_total must stay 0 below max_bytes"
        );

        // over, over, UNDER, over, over → no fire (each frozen run is
        // below the 3-evaluation trigger); the 3rd consecutive fires.
        // 60 KiB phantom: observed = 50 + 60 = 110 KiB, strictly over.
        let over_extra = 60 * 1024;
        map.test_inflate_wedge_observation(over_extra);
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(1)
        );
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(2)
        );
        map.test_inflate_wedge_observation(49 * 1024);
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::UnderBudget,
            "an under-budget observation must reset the frozen-tick accumulation"
        );
        map.test_inflate_wedge_observation(over_extra);
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(1),
            "frozen-tick count must restart from 1 after an under-budget reset — \
             NON-consecutive over-budget observations must never fire"
        );
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(2)
        );
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert!(
            matches!(outcome, WedgeSelfHealOutcome::Fired { .. }),
            "the {WEDGE_FROZEN_TICKS_TRIGGER}rd CONSECUTIVE frozen at-or-over evaluation \
             must fire (got {outcome:?})"
        );
    }

    /// Inert direction 2 (the no-false-fire proof for normal at-cap
    /// operation): at-or-over budget WITH real eviction progress between
    /// evaluations never accumulates or fires — evictions progressing
    /// every tick ⇒ no trigger.
    ///
    /// Mutation step: remove the `evicted_now != last` progress branch →
    /// red-fails with the bespoke "progressing evictions" message.
    #[tokio::test]
    async fn fallback_evictor_inert_when_evictions_progressing_at_cap() {
        // Fill EXACTLY to the 100-weight cap so each further runtime
        // insert genuinely evicts (real moka LRU eviction → the
        // `evicted_items` sensor advances).
        let (map, _removal_count) = wedge_map_with_entries(100).await;
        // 50 KiB phantom on a genuinely at-cap map: observed ~150 KiB,
        // strictly over throughout.
        map.test_inflate_wedge_observation(50 * 1024);
        for k in 0..6u64 {
            map.insert(1000 + k, BytesEntry(1024)).await;
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::ProgressObserved,
                "at-or-over budget with PROGRESSING evictions must never fire the fallback \
                 — normal at-cap operation evicts every tick and is not a wedge (got \
                 {outcome:?} on insert #{k})"
            );
        }
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            0,
            "selfheal_total must stay 0 while evictions progress at cap"
        );
        assert!(
            !map.eviction_wedge_detected.load(Ordering::Relaxed),
            "wedge gauge must stay clear while evictions progress at cap"
        );
    }

    /// Composite-invariant direction: the fallback must NEVER evict a
    /// pinned entry of ANY class — real (`pin_key`), indefinite/BIS
    /// (`pin_key_indefinite`, the FL-688 durability replica), speculative
    /// (`pin_key_speculative`) — nor a RACE-WINDOW key that is still
    /// resident in the moka cache while already present in `pinned`
    /// (models the `pin_key_with_mode` window between the DashMap insert
    /// and `cache.invalidate`; the key-walk's `pinned.contains_key` skip
    /// is the ONLY protection for it).
    ///
    /// Mutation step: comment out the `pinned.contains_key` skip in
    /// `evict_unpinned_lru_bytes` → the race-window assertion red-fails.
    #[tokio::test]
    async fn fallback_evictor_never_evicts_pinned_any_class() {
        let (map, _removal_count) = wedge_map_with_entries(40).await;
        map.insert(900, BytesEntry(1024)).await;
        assert!(map.pin_key(900), "real pin of a present key must succeed");
        map.insert(901, BytesEntry(1024)).await;
        assert!(
            map.pin_key_indefinite(901),
            "indefinite pin of a present key must succeed"
        );
        map.insert(902, BytesEntry(1024)).await;
        assert!(
            map.pin_key_speculative(902),
            "speculative pin of a present key must succeed"
        );
        map.insert(903, BytesEntry(1024)).await;
        map.pinned.insert(
            903,
            PinnedEntry {
                data: BytesEntry(1024),
                pinned_at: Instant::now(),
                size: 1024,
                indefinite: false,
                speculative: false,
            },
        );

        // Phantom overage far larger than everything evictable → the
        // heal tries to evict every unpinned resident.
        map.test_inflate_wedge_observation(10 * WEDGE_MAX_BYTES as u64);
        let mut outcome = WedgeSelfHealOutcome::UnderBudget;
        for _ in 0..WEDGE_FROZEN_TICKS_TRIGGER {
            outcome = tokio::time::timeout(
                core::time::Duration::from_secs(5),
                map.test_maybe_selfheal_wedged_eviction(),
            )
            .await
            .expect("must not deadlock — self-heal eviction-event drain wedged");
        }
        let WedgeSelfHealOutcome::Fired { evicted_count, .. } = outcome else {
            panic!("self-heal must fire under a forced 10x overshoot (got {outcome:?})");
        };
        assert_eq!(
            evicted_count, 40,
            "the fallback must evict ONLY the 40 unpinned residents — never a pinned \
             (real/indefinite/speculative) or race-window entry (got {evicted_count})"
        );
        for (key, class) in [(900u64, "real"), (901, "indefinite/BIS"), (902, "speculative")] {
            assert!(
                map.get(&key).await.is_some(),
                "FL-688 durability: the {class}-pinned blob (key {key}) must survive the \
                 fallback evictor — evicting a pinned blob loses the durability replica"
            );
        }
        assert!(
            map.get(&903).await.is_some(),
            "race-window: a key resident in the cache while present in `pinned` must be \
             skipped by the fallback key-walk (`pinned.contains_key` guard)"
        );
        for k in 0..40u64 {
            assert!(
                map.get(&k).await.is_none(),
                "unpinned entry {k} must have been evicted by the forced 10x-overshoot heal"
            );
        }
        // Pin accounting intact (the 3 pins taken via the pin API; the
        // race-window DashMap insert deliberately bypasses accounting).
        assert_eq!(
            map.pinned_bytes(),
            3 * 1024,
            "pinned byte accounting must be intact after the fallback fired"
        );
        assert_eq!(
            map.indefinite_pinned_bytes(),
            1024,
            "indefinite (BIS) pin accounting must be intact after the fallback fired"
        );
        assert_eq!(
            map.speculative_pinned_bytes(),
            1024,
            "speculative pin accounting must be intact after the fallback fired"
        );
    }

    /// Composite-invariant direction: while the FL-688 startup reconcile
    /// gate is armed the fallback is suppressed IDENTICALLY to the drain
    /// tick; after release it fires normally.
    ///
    /// Mutation step: remove the `reconcile_complete` check in
    /// `maybe_selfheal_wedged_eviction` → red-fails with the bespoke
    /// suppression message.
    #[tokio::test]
    async fn fallback_evictor_suppressed_while_reconcile_gate_armed() {
        let (map, removal_count) = wedge_map_with_entries(50).await;
        map.test_inflate_wedge_observation(2 * WEDGE_MAX_BYTES as u64);
        map.set_startup_reconcile_gate();
        for _ in 0..5 {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::SuppressedReconcileGate,
                "FL-688 Stage C: the fallback evictor must be suppressed IDENTICALLY to \
                 the drain tick while the startup reconcile gate is armed — firing during \
                 the reconcile window races the reconcile-pin protection (got {outcome:?})"
            );
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "no fallback eviction may fire while the reconcile gate is armed"
        );
        map.release_startup_reconcile_gate();
        for expect_ticks in 1..WEDGE_FROZEN_TICKS_TRIGGER {
            assert_eq!(
                map.test_maybe_selfheal_wedged_eviction().await,
                WedgeSelfHealOutcome::Accumulating(expect_ticks),
                "after gate release the trigger must accumulate normally"
            );
        }
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert!(
            matches!(outcome, WedgeSelfHealOutcome::Fired { .. }),
            "after gate release the fallback must fire normally (got {outcome:?})"
        );
    }

    /// Piece 1 WIRING: the periodic drain-tick arm itself must invoke the
    /// self-heal evaluation (after `run_pending_tasks_and_drain`). Mirrors
    /// the `periodic_forced_drain_arm_converges_unpinned_under_cap`
    /// paused-clock technique.
    ///
    /// Mutation step: delete the `maybe_selfheal_wedged_eviction` call
    /// from the drain-tick arm → the bounded advance loop exhausts and
    /// red-fails with the bespoke WIRING message.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_tick_arm_invokes_wedge_selfheal() {
        let (map, removal_count) = wedge_map_with_entries(50).await;
        map.test_inflate_wedge_observation(80 * 1024); // 30 KiB overshoot
        map.start_background_eviction();
        let drain_period = core::time::Duration::from_secs(super::DRAIN_INTERVAL_SECS);
        const MAX_TICKS: u32 = 20;
        let converged = tokio::time::timeout(core::time::Duration::from_secs(3600), async {
            for _ in 0..MAX_TICKS {
                tokio::time::advance(drain_period).await;
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                if removal_count.load(Ordering::Relaxed) >= 30 {
                    return true;
                }
            }
            false
        })
        .await
        .expect("deadlock: drain-tick selfheal convergence loop did not finish");
        assert!(
            converged,
            "piece 1 WIRING: the periodic drain arm must invoke \
             maybe_selfheal_wedged_eviction after run_pending_tasks_and_drain — with the \
             trigger observation forced over budget and evictions frozen, {MAX_TICKS} \
             ticks produced {} fallback evictions (expected >= 30). A wedged moka evictor \
             never self-heals without this arm (FINDING 2).",
            removal_count.load(Ordering::Relaxed),
        );
        assert!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed) >= 1,
            "selfheal_total must count the tick-arm firing"
        );
    }

    /// Piece 3 WIRING (map level): `kick_drain` must wake the background
    /// drain arm WITHOUT waiting for the 10 s tick (paused clock ⇒ no
    /// tick can fire), and the kick arm must run BOTH halves of the drain
    /// body: `run_pending_tasks_and_drain` (observed via the startup-
    /// overshoot evictions) AND the self-heal evaluation (observed via
    /// the frozen-evictions sensor baseline advancing).
    ///
    /// Mutation steps: (a) comment `drain_kick.notify_one()` in
    /// `kick_drain` → the bounded yield loop exhausts, red-fails the
    /// "kick must wake" message; (b) delete the selfheal call from the
    /// kick arm → the sensor-baseline assertion red-fails.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_kick_wakes_drain_arm_without_tick() {
        // Startup-overshoot recipe (mirrors the periodic-drain test): 19
        // 10-byte entries over a 1-unit cap via `insert_with_time` — moka
        // has NOT enforced the cap and, with the clock paused, never will
        // on its own.
        let cfg = policy(100, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);
        // Start the loop FIRST and let the intervals' IMMEDIATE first
        // ticks fire on the still-empty map (tokio intervals complete
        // their first tick at once); with the clock paused no further
        // tick can ever fire, so everything after this point is
        // kick-driven only.
        map.start_background_eviction();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        // NOW take the startup overshoot (deferred maintenance).
        for k in 0..19u64 {
            map.insert_with_time(k, BytesEntry(10), 0).await;
        }
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "precondition: without a kick and without a further tick the drain arm must \
             not run (startup overshoot must still be un-enforced)"
        );
        // Inflate the wedge observation so the kick arm's selfheal
        // evaluation is observable via the sensor baseline.
        map.test_inflate_wedge_observation(200 * 1024);
        assert!(
            map.kick_drain(),
            "first kick must be accepted (rate limiter fresh)"
        );
        // Bounded yield loop (NOT a paused-clock timeout: the hot loop
        // prevents auto-advance, so a timeout would never fire; loop
        // exhaustion is the clean failure).
        let mut woke = false;
        for _ in 0..1000 {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            if removal_count.load(Ordering::Relaxed) >= 18 {
                woke = true;
                break;
            }
        }
        assert!(
            woke,
            "piece 3 WIRING: kick_drain must wake the background drain arm WITHOUT \
             waiting for the 10 s tick (gate => evict: the disk-NAK site depends on this \
             wake) — the startup overshoot stayed un-evicted after the kick"
        );
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            18,
            "the kick-driven drain must run run_pending_tasks_and_drain to convergence \
             (19 entries - 1 kept at the 1-unit cap = 18 evictions)"
        );
        // The kick arm must ALSO have run the selfheal evaluation: the
        // 18 drain evictions are `RemovalCause::Size` (moka capacity
        // enforcement), so they advanced the Size sensor and the
        // evaluation records ProgressObserved + updates the baseline.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            map.selfheal_last_size_evicted_items.load(Ordering::Relaxed),
            18,
            "piece 3: the kick arm must run the wedge selfheal evaluation after the \
             drain — the frozen-size-evictions sensor baseline must have observed the \
             18 Size evictions (stayed at {} instead)",
            map.selfheal_last_size_evicted_items.load(Ordering::Relaxed),
        );
    }

    /// Piece 3: kick delivery is rate-limited so a NAK storm cannot turn
    /// the drain loop busy — AND the limiter RELEASES after the interval
    /// (asymmetric-contract coverage, pair-b T4: a permanently-latched
    /// limiter would silently kill the kick path after the first NAK of
    /// the process's life).
    ///
    /// Mutation steps: (a) remove the interval check in `kick_drain` →
    /// the second-kick assertion red-fails; (b) latch the limiter
    /// (`if last != 0 { return false; }`) → the release assertion
    /// red-fails.
    #[tokio::test]
    async fn drain_kick_rate_limited_and_releases() {
        let cfg = policy(WEDGE_MAX_BYTES, 0);
        let map = Arc::new(make_map_cb(&cfg));
        // Shrink the interval to 50 ms so the RELEASE half needs only a
        // short real sleep; the elapsed-time comparison itself runs
        // unmodified against the std::time::Instant anchor.
        map.test_force_drain_kick_min_interval(50);
        assert!(map.kick_drain(), "first kick must be accepted");
        assert!(
            !map.kick_drain(),
            "an immediate second kick must be rate-limited (one accepted kick per \
             DRAIN_KICK_MIN_INTERVAL_MILLIS) — a disk-NAK storm must not become a busy \
             drain loop"
        );
        // Real elapsed time past the (shrunk) interval — the limiter
        // must RELEASE.
        tokio::time::sleep(core::time::Duration::from_millis(80)).await;
        assert!(
            map.kick_drain(),
            "the kick rate-limiter must RELEASE once the minimum interval has elapsed \
             since the last accepted kick — a permanently-latched limiter makes the \
             disk-NAK kick path dead after the first NAK of the process's life \
             (gate => evict latency guarantee silently lost)"
        );
        assert!(
            !map.kick_drain(),
            "after the released kick is accepted the limiter must re-arm"
        );
    }

    /// Piece 3 + FL-688 Stage C: a kick delivered while the startup
    /// reconcile gate is armed must NOT drain (the kick ARM honors the
    /// gate identically to the periodic tick).
    ///
    /// Mutation step: remove the `reconcile_complete` check from the kick
    /// arm → the zero-evictions assertion red-fails.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn drain_kick_suppressed_while_reconcile_gate_armed() {
        let cfg = policy(100, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);
        for k in 0..19u64 {
            map.insert_with_time(k, BytesEntry(10), 0).await;
        }
        map.set_startup_reconcile_gate();
        map.start_background_eviction();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            map.kick_drain(),
            "kick DELIVERY is not gated (the ARM checks the gate) — must be accepted"
        );
        // Bounded yield budget for the (suppressed) arm to run.
        for _ in 0..1000 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "FL-688 Stage C: a kick received while the startup reconcile gate is armed \
             must be a no-op — the kick arm must honor the gate identically to the \
             periodic drain tick (evictions fired during the reconcile window)"
        );
    }

    /// Boundary semantics (operator decision 2026-07-29, review 9fd52fc0
    /// MAJOR-2 / pair-b T3): at EXACTLY `max_bytes` the trigger must NOT
    /// arm — moka owes zero eviction at equality
    /// (`weights_to_evict = weighted_size.saturating_sub(max) = 0`), so
    /// a wedge is mathematically unobservable there, and every deployed
    /// `max_bytes` is 1024-divisible so an idle at-cap cache reaches
    /// equality routinely. One weigher unit over IS a genuine overshoot
    /// and must trigger normally (the no-slack rationale).
    ///
    /// Mutation step: revert the strict `>` to `>=` (make the reset
    /// branch `observed < self.max_bytes`) → the exact-cap assertions
    /// red-fail with the bespoke "at EXACTLY max_bytes" message.
    #[tokio::test]
    async fn fallback_evictor_no_fire_at_exact_cap() {
        let (map, removal_count) = wedge_map_with_entries(50).await;
        // Phantom 50 KiB: observed = 50 KiB real + 50 KiB = exactly max.
        map.test_inflate_wedge_observation(50 * 1024);
        for _ in 0..5 {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::UnderBudget,
                "at EXACTLY max_bytes the trigger must NOT arm (strict >, operator \
                 decision 2026-07-29): moka owes zero eviction at equality, so a wedge \
                 is unobservable there — firing would evict from a correctly-behaving \
                 at-cap cache and teach operators to ignore the wedge signal (got \
                 {outcome:?})"
            );
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "no eviction may fire at exact cap"
        );
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            0,
            "selfheal_total must stay 0 at exact cap (a spurious count here is the \
             cried-wolf failure mode)"
        );
        assert!(
            !map.eviction_wedge_detected.load(Ordering::Relaxed),
            "wedge gauge must stay clear at exact cap"
        );
        // ONE weigher unit over is a genuine overshoot: normal trigger.
        map.test_inflate_wedge_observation(51 * 1024);
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(1),
            "one weigher unit over max_bytes must arm the trigger (no-slack rationale)"
        );
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::Accumulating(2)
        );
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert_eq!(
            outcome,
            WedgeSelfHealOutcome::Fired {
                evicted_count: 1,
                evicted_bytes: 1024,
                rounds: 1
            },
            "a one-weigher-unit overshoot must heal by evicting exactly one entry"
        );
    }

    /// pair-b T1: the `NoByteBudget` guard is load-bearing. Without it,
    /// `max_bytes == 0` makes every evaluation read strictly-over-budget
    /// (`observed > 0`), and the third drains a COUNT-capped map — the
    /// deployed server existence_cache shape (`prod-server.json5:237-239`,
    /// `max_count: 50000000`, no `max_bytes`) — to EMPTY every 3 ticks,
    /// forever.
    ///
    /// Mutation step: delete the `if self.max_bytes == 0` guard → the
    /// NoByteBudget assertions red-fail (third evaluation fires and the
    /// removal-count assertion trips).
    #[tokio::test]
    async fn fallback_evictor_inert_without_byte_budget() {
        // Count-capped only: max_bytes == 0, max_count = 100.
        let cfg = policy(0, 100);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);
        for k in 0..50u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        // Even a huge phantom overage must not matter: there is no byte
        // budget to enforce.
        map.test_inflate_wedge_observation(10 * 1024 * 1024);
        for _ in 0..5 {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::NoByteBudget,
                "a map with max_bytes == 0 has no byte budget: every evaluation must \
                 return NoByteBudget — without this guard the heal reads any non-empty \
                 count-capped map as infinitely over budget (got {outcome:?})"
            );
        }
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "NoByteBudget guard is load-bearing: without it the fallback drains a \
             COUNT-capped map (the deployed server existence_cache shape) to EMPTY \
             every 3 ticks"
        );
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            0,
            "selfheal_total must stay 0 on a byte-budget-less map"
        );
        assert_eq!(
            map.len_for_test().await,
            50,
            "all 50 entries of the count-capped map must survive"
        );
    }

    /// pair-b T2 + pair-a MAJOR-4: the METERED multi-round heal. Small
    /// scan cap (11 → 10 evictions/round) + a 95 KiB phantom overage on
    /// 50 real KiB drives the full metering machinery:
    ///   * per-invocation round cap (2) with truncated rounds,
    ///   * `selfheal_resume` re-arm (continuations fire WITHOUT fresh
    ///     frozen-tick accumulation and WITHOUT re-counting the firing),
    ///   * per-round observation re-read (target falls as real entries
    ///     are evicted),
    ///   * convergence stop at at-or-under budget (strict `>` there
    ///     too).
    ///
    /// Mutation steps (each must red-fail):
    ///   (a) round cap → `while rounds < 1` → invocation 1 returns
    ///       `rounds: 1, evicted_count: 10`;
    ///   (b) per-round `target` → `0` → rounds evict nothing;
    ///   (c) round cap removed (`u32::MAX`) → invocation 1 heals to
    ///       convergence in one pass (`evicted_count: 45`);
    ///   (d) resume latch never set → the 4th evaluation is
    ///       `Accumulating(1)` instead of `Fired`.
    #[tokio::test]
    async fn wedge_selfheal_metered_rounds_resume_and_convergence() {
        let (map, removal_count) = wedge_map_with_entries(50).await;
        // 10 evictions per round (scan-cap check breaks at the 11th
        // scanned entry, matching EVICT_SCAN_HARD_CAP's semantics).
        map.test_force_evict_scan_cap(11);
        map.test_inflate_wedge_observation(95 * 1024);

        for expect_ticks in 1..WEDGE_FROZEN_TICKS_TRIGGER {
            assert_eq!(
                map.test_maybe_selfheal_wedged_eviction().await,
                WedgeSelfHealOutcome::Accumulating(expect_ticks)
            );
        }
        // Invocation 1 (initial firing): 2 metered rounds x 10 entries,
        // still over -> re-armed for resume.
        let outcome = tokio::time::timeout(
            core::time::Duration::from_secs(5),
            map.test_maybe_selfheal_wedged_eviction(),
        )
        .await
        .expect("must not deadlock — metered heal invocation 1 wedged");
        assert_eq!(
            outcome,
            WedgeSelfHealOutcome::Fired {
                evicted_count: 20,
                evicted_bytes: 20 * 1024,
                rounds: 2
            },
            "metering: the initial firing must run EXACTLY \
             WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION \
             ({WEDGE_SELFHEAL_MAX_ROUNDS_PER_INVOCATION}) scan-capped rounds and stop \
             (an unmetered heal monopolizes the drain loop, starving the pin-expiry \
             sweeps and dumping the full BlobsAvailable delta burst in one message)"
        );
        // Invocation 2: RESUMED immediately — no fresh frozen-tick
        // accumulation between metered invocations.
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert_eq!(
            outcome,
            WedgeSelfHealOutcome::Fired {
                evicted_count: 20,
                evicted_bytes: 20 * 1024,
                rounds: 2
            },
            "resume: a metered heal that stopped at the round cap while still over \
             budget must CONTINUE on the next evaluation (Accumulating here means the \
             re-arm latch is lost and convergence gains 30 s of dead ticks per 2 rounds)"
        );
        // Invocation 3: evicts the last 5 over-budget entries, then the
        // round-2 re-read sees 5 KiB real + 95 KiB phantom = exactly max
        // -> at-or-under -> converged (strict `>` at the round boundary).
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert_eq!(
            outcome,
            WedgeSelfHealOutcome::Fired {
                evicted_count: 5,
                evicted_bytes: 5 * 1024,
                rounds: 1
            },
            "convergence: the heal must stop evicting the moment the observation is \
             at-or-under max_bytes — not drain the map"
        );
        // Converged: next evaluation is a plain UnderBudget reset.
        assert_eq!(
            map.test_maybe_selfheal_wedged_eviction().await,
            WedgeSelfHealOutcome::UnderBudget,
            "post-convergence the trigger must reset (observation sits exactly at max)"
        );
        assert!(
            !map.eviction_wedge_detected.load(Ordering::Relaxed),
            "wedge gauge must clear after convergence"
        );
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            45,
            "the full metered heal must evict exactly the 45-entry overage across \
             invocations (5 of 50 entries survive)"
        );
        assert_eq!(
            map.eviction_wedge_selfheal_total.counter.load(Ordering::Relaxed),
            1,
            "resumed continuations are the SAME logical firing: selfheal_total must \
             count 1, not one per invocation"
        );
    }

    /// Sensor refinement (review 9fd52fc0 red-team alt-framing 2): the
    /// freeze sensor counts ONLY `RemovalCause::Size` evictions. On the
    /// server targetkey store (`max_seconds: 604800`) TTL expiries
    /// advance the coarse `evicted_items` counter continuously, so a
    /// coarse sensor would read a dead size-evictor as "progressing"
    /// FOREVER — the wedge permanently masked.
    ///
    /// Real TTL composition: `max_seconds: 1` → moka `time_to_idle(1s)`,
    /// a real 1.3 s idle, and the drain-arm's own
    /// `run_pending_tasks_and_drain` expiring the entries (cause
    /// `Expired`) exactly as the production tick would.
    ///
    /// Mutation step: point the sensor back at
    /// `self.evicted_items.counter` → the first evaluation returns
    /// `ProgressObserved` (the 50 Expired events reset the freeze
    /// counter) and red-fails the bespoke TTL-masking message.
    #[tokio::test]
    async fn wedge_sensor_ignores_non_size_eviction_progress() {
        let cfg = EvictionPolicy {
            max_bytes: WEDGE_MAX_BYTES,
            evict_bytes: 0,
            max_seconds: 1, // time_to_idle(1s) — the targetkey shape
            max_count: 0,
            pin_cap_bytes: 0,
        };
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);
        for k in 0..50u64 {
            map.insert(k, BytesEntry(1024)).await;
        }
        // Real idle past the 1 s TTI so every entry is expired.
        tokio::time::sleep(core::time::Duration::from_millis(1300)).await;
        // The drain arm's first half (exactly what the production tick
        // runs before the evaluation): expires all 50 entries with
        // cause `Expired`.
        map.run_pending_tasks_and_drain().await;
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            50,
            "fixture: the TTI sweep must have expired all 50 entries"
        );
        assert_eq!(
            map.evicted_items.counter.load(Ordering::Relaxed),
            50,
            "fixture: the coarse evicted_items counter must have advanced by the 50 \
             Expired events (this is the progress a coarse sensor would wrongly credit)"
        );
        assert_eq!(
            map.size_evicted_items.load(Ordering::Relaxed),
            0,
            "fixture: no Size eviction happened — the Size sensor must not move on \
             Expired events"
        );
        // Strictly-over observation with a frozen SIZE sensor: the
        // trigger must accumulate and fire despite the coarse counter's
        // TTL progress.
        map.test_inflate_wedge_observation(150 * 1024);
        for expect_ticks in 1..WEDGE_FROZEN_TICKS_TRIGGER {
            let outcome = map.test_maybe_selfheal_wedged_eviction().await;
            assert_eq!(
                outcome,
                WedgeSelfHealOutcome::Accumulating(expect_ticks),
                "TTL-expiry (RemovalCause::Expired) progress must NOT reset the \
                 size-wedge freeze counter — on the server targetkey store \
                 (max_seconds: 604800) TTL churn would otherwise permanently mask a \
                 dead size-evictor (got {outcome:?})"
            );
        }
        let outcome = map.test_maybe_selfheal_wedged_eviction().await;
        assert!(
            matches!(outcome, WedgeSelfHealOutcome::Fired { .. }),
            "the size-wedge must fire despite continuous TTL-expiry progress on the \
             coarse counter (got {outcome:?})"
        );
    }
}
