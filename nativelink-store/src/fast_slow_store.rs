// Copyright 2024-2025 The NativeLink Authors. All rights reserved.
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

use core::cmp::{max, min};
use core::future::Future;
use core::ops::Range;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::sync::{Arc, Weak};
#[cfg(feature = "chunked_fast_slow")]
use std::sync::OnceLock;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::join;
use futures::stream::{FuturesUnordered, StreamExt};
use nativelink_config::stores::{FastSlowSpec, StoreDirection};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, WriteHalfGuard, make_buf_channel_pair_with_size,
};
use nativelink_util::common::{DigestInfo, make_precondition_failure_any};
use nativelink_util::fs;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DelegationChildren, IS_MIRROR_REQUEST, ItemCallback, MarkStableDelegation, PinDelegation,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, StoreOptimizations,
    UploadSizeInfo, slow_update_store_with_file,
};
use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};

// TODO(palfrey) This store needs to be evaluated for more efficient memory usage,
// there are many copies happening internally.

// Per-key loader handle. Used only as a unique allocation whose
// `Arc::ptr_eq` distinguishes loader generations during cleanup —
// `LoaderGuard::Drop` removes the populating_digests entry only when
// the stored Arc ptr-equals its own. `Arc<()>` is sufficient (the
// previous `Arc<OnceCell<()>>` was a vestige from before the
// spawn-detach refactor removed `OnceCell::get_or_try_init`).
type Loader = Arc<()>;

/// Default maximum aggregate bytes held in `mirror_blobs`. The runtime cap
/// is held in `FastSlowStore::mirror_blobs_max_bytes` and is only overridable
/// by tests via `set_mirror_blobs_max_bytes_for_test`. When exceeded,
/// `insert_mirror_blob` returns `Err(ResourceExhausted)` so the mirror
/// writer can record a per-peer failure and route the next attempt elsewhere.
const DEFAULT_MIRROR_BLOBS_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// Concurrency cap on the per-key fan-out used by `batch_get_part_unchunked`
/// when `local_only_reads` is enabled. Each in-flight `get_part_unchunked`
/// reserves a `BytesMut` plus a buf_channel pair (~3 MiB chunks × 24 slots
/// ≈ 72 MiB peak), so an unbounded fan-out on a 100-key batch could commit
/// ~7 GiB. 16 keeps the worst-case footprint near 1 GiB while still
/// overlapping enough I/O to saturate the local filesystem tier.
const LOCAL_ONLY_READS_BATCH_CONCURRENCY: usize = 16;

/// Wall-clock deadline for the background slow-store write task. If the
/// spawn has not produced a terminal Ok/Err result within this window,
/// the watchdog records the digest in `failed_slow_writes` so a worker
/// reconnect retries the upload. The spawn is NOT aborted — if it
/// eventually succeeds, the next `BlobsInStableStorage` ack drops the
/// retry entry. Set lower than `PIN_TIMEOUT_SECS = 120` (in
/// `MokaEvictingMap`) so the failed-set insert lands BEFORE the pin
/// auto-unpins; without that ordering, an auto-unpin could quietly
/// downgrade the blob to evictable while the watchdog has not yet
/// fired. The pin-expiry callback (registered on the fast store) is the
/// secondary safety net for hangs longer than 120s.
const SLOW_WRITE_WATCHDOG_SECS: u64 = 60;

tokio::task_local! {
    /// Per-call opt-in: when set on the calling task, the populate-NotFound
    /// terminal branch in [`FastSlowStore::get_part`] (the `producer_err.code
    /// == NotFound` arm at the `streaming_inner.is_terminal()` check) returns
    /// `Err(NotFound)` WITHOUT calling `writer.send_error()`. The CALLER takes
    /// responsibility for any downstream writer-termination semantics.
    ///
    /// Used by [`crate::worker_proxy_store::WorkerProxyStore::get_part_sequential`]
    /// — when the local store reports NotFound, WorkerProxyStore re-uses
    /// the same outer writer for its peer-fetch fallback. Without this
    /// flag, FastSlowStore's deadlock-defense termination (added for
    /// `VerifyStore`'s `tokio::join!(get_fut, check_fut)` writer-
    /// termination contract) closes the outer writer and the peer's
    /// bytes can never be delivered to the consumer — bug #171.
    ///
    /// Coverage extends to ALL `slow_store.get_part(&mut *guard, ...)`
    /// fallback sites that may return NotFound while the OUTER writer
    /// still has zero bytes written (NoopUpdates / ReadOnly bypass,
    /// fast-store-evicted-after-populate, sliding-window-evicted, AND
    /// the WAITER-path streaming-reader-error fallback). Without
    /// extending the gate to the waiter site, a parallel-chunk request
    /// that races multiple `get_part` calls for the same digest through
    /// a single FastSlowStore would have its WAITERS' Drop fallback
    /// terminate the OUTER writer the moment the populator's NotFound
    /// flows through the streaming buffer — defeating the populator-
    /// side gate at the same chain depth (#171 parallel-path leak,
    /// 2026-04-27).
    ///
    /// Coverage ALSO extends to the three early-return NotFound sites
    /// that route to peer-fetch in production (red-team #171 follow-up,
    /// 2026-04-27):
    ///
    /// 1. `mirror_blobs` size mismatch (line ~2949) — the in-memory
    ///    mirror cache held a phantom-positive entry whose bytes
    ///    didn't match `digest.size_bytes()`. The corrupt entry has
    ///    been removed; the blob may still live on slow-tier or peers,
    ///    so peer-fetch on the still-open OUTER writer IS recoverable.
    /// 2. `in_flight_slow_writes` size mismatch (line ~3101) — the
    ///    in-flight chunks didn't sum to the digest size. Same shape:
    ///    the bad entry has been removed; the blob may still live
    ///    elsewhere, so peer-fetch is the recovery path.
    /// 3. `local_only_reads` NotFound (line ~3157) — the worker public-
    ///    CAS variant DELIBERATELY refuses slow-tier fallthrough so
    ///    the asking server "tries a different peer rather than looping
    ///    the request back through this worker's slow tier." The
    ///    upstream `WorkerProxyStore`'s peer-fetch IS that "different
    ///    peer" path — terminating the OUTER writer here defeats the
    ///    very design intent of `local_only_reads`.
    ///
    /// All three preserve VerifyStore's deadlock-defense when the gate
    /// is NOT set (no upstream peer-fetch caller above): the helper
    /// falls through to `commit_delegated_if_ok(&Err)` which leaves the
    /// `WriteHalfGuard` Drop fallback armed.
    ///
    /// Sites that do NOT honor this flag, even with the gate set:
    /// fast_store truncation (Code::Internal — semantics indicate
    /// corruption, not absence; consumer must see structured Internal
    /// error), fast_store non-NotFound err path (preserves the inner
    /// store's structured error code), and the populator streaming-
    /// reader-error branch when bytes have already been sent
    /// (peer-fetch cannot recover without producing a corrupt-prefix-
    /// from-populate + full-peer-copy stream — see existing inline
    /// `bytes_written == 0` guard at line ~3392-3402).
    pub static INNER_MISS_NO_TERMINATE: bool;
}

/// Commits the [`WriteHalfGuard`] honoring the
/// [`INNER_MISS_NO_TERMINATE`] task-local. Three-way commit:
///
/// 1. **Ok**: identical to `commit_delegated_if_ok(&Ok)`. The sub-call
///    already sent EOF, so just suppress the Drop fallback.
/// 2. **Err(NotFound) with no bytes written AND gate set**: suppress
///    the Drop fallback so the OUTER writer survives for the upstream
///    caller's recovery path (typically `WorkerProxyStore`'s peer-
///    fetch fallback). The caller now owns terminating the writer
///    when its outer fn returns.
/// 3. **Err otherwise (the COMMON non-WPS case)**: explicitly terminate
///    the writer with the structured error via `guard.fail(err)`. This
///    REPLACES the previous behavior which left the Drop fallback
///    armed → fired the synthesized `"buf_channel: writer dropped
///    without commit"` error and an `error!` log
///    `"WriteHalfGuard fired Drop fallback: function exited without
///    explicit commit"`. The structured `err` is the better signal
///    (preserves Code, message, err_tip chain) than Drop's generic
///    Internal, AND the loud "this is a bug" Drop log was firing at
///    ~835/min in production from non-WPS callers — actively
///    misleading because the helper IS the explicit commit, just
///    delegated. Calling `guard.fail` here makes the commit explicit
///    in the same place the helper was already committing on Ok, and
///    eliminates the false-positive Drop log without losing any
///    diagnostic for genuine sub-store contract violations (those
///    callers don't go through the helper).
///
/// This is the gate-aware analogue of the inline checks at the
/// populator terminal-state branch (line ~3290) and the populator
/// streaming-reader-error branch (line ~3422). All three sites
/// share the same contract: when the upstream caller has opted in, an
/// inner-NotFound with no bytes written must NOT terminate the OUTER
/// writer.
///
/// Also called from the three early-return NotFound paths in
/// `FastSlowStore::get_part` that route to peer-fetch in production:
/// `mirror_blobs` size mismatch, `in_flight_slow_writes` size mismatch,
/// and `local_only_reads`. At each of those sites the caller passes a
/// synthesized `Err(NotFound)` (no inner await happened) so the gate
/// suppresses the Drop fallback while the caller still propagates the
/// structured Err to its parent — see [`INNER_MISS_NO_TERMINATE`] for
/// the per-site rationale.
fn commit_with_inner_miss_gate(
    guard: &mut WriteHalfGuard<'_>,
    res: &Result<(), Error>,
    bytes_before: u64,
) {
    match res {
        Ok(()) => {
            guard.commit_delegated_if_ok(&Ok::<(), Error>(()));
        }
        Err(err) => {
            if err.code == Code::NotFound
                && guard.get_bytes_written() == bytes_before
                && INNER_MISS_NO_TERMINATE.try_with(|v| *v).unwrap_or(false)
            {
                // Gate set + recoverable inner-miss: suppress the Drop
                // fallback. Upstream caller owns peer-fetch recovery
                // on the still-open OUTER writer.
                guard.commit_delegated_if_ok(&Ok::<(), Error>(()));
                return;
            }
            // Non-gated path (the COMMON case): explicit termination
            // with the structured error. `guard.fail` is idempotent
            // (`send_error` only records the first error) so callers
            // that already terminated the writer themselves before
            // invoking the helper (e.g. the inner non-NotFound match
            // arm at the terminal-state branch) are unaffected.
            let _ = guard.fail(err.clone());
        }
    }
}

/// Listener registered on the fast store's eviction map so the
/// `on_pin_expired` hook lands the digest in `failed_slow_writes`. The
/// pin TTL firing without an explicit unpin would, by itself, be
/// ambiguous: it could mean (a) a real silent slow-write hang we MUST
/// retry, or (b) a `DirectoryCache` download-pin that auto-expired
/// because the action took longer than `PIN_TIMEOUT_SECS` (no slow-write
/// ever existed for that digest — the blob came in via download).
///
/// To distinguish, the listener consults `in_flight_slow_writes` of its
/// owning `FastSlowStore`. Only digests that have an outstanding
/// background slow-write spawn (populated by `update` /
/// `update_oneshot`) are queued for retry. Download-pin expiries are
/// silently skipped — no warn, no failed-set entry — which both
/// eliminates the spurious "queueing digest for slow-write retry"
/// log churn (5774 events / 10 min observed on workers from
/// `directory_cache.rs` pins) AND prevents `failed_slow_writes` from
/// accumulating dead-weight entries that, on reconnect, would attempt
/// to re-upload blobs the server already has.
///
/// As a side-effect this also dedupes the multi-listener fan-out:
/// `local_worker.rs` registers three `PinExpireFailedWritesListener`
/// instances against the SAME underlying fast store (one per
/// `FastSlowStore::new` / `new_with_shared_failed_writes` site). Each
/// listener carries its OWNING wrapper's `in_flight_slow_writes` Arc
/// (every wrapper has its own in-flight map; only `failed_slow_writes`
/// is shared). A real silent slow-write hang shows up in the in-flight
/// of ONLY the wrapper that owned the spawn, so exactly one of the
/// three listeners fires the warn + insert per pin expiry — collapsing
/// the previously observed 3× warn amplification to 1×.
#[derive(Debug)]
struct PinExpireFailedWritesListener {
    failed_slow_writes: Arc<Mutex<HashSet<DigestInfo>>>,
    /// Per-wrapper in-flight slow-write set. Used as the "is this pin
    /// associated with a slow-write owned by *this* wrapper?" gate.
    /// Skipping the warn + failed-set insert when the digest is absent
    /// from this map is what makes the listener idempotent across the
    /// 3-wrapper composition AND scopes the durability path to actual
    /// uploads (vs `DirectoryCache` download pins).
    in_flight_slow_writes: Arc<Mutex<HashMap<StoreKey<'static>, Vec<Bytes>>>>,
}

impl ItemCallback for PinExpireFailedWritesListener {
    fn callback<'a>(
        &'a self,
        _store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        // Eviction is unrelated to pin-expiry; nothing to do.
        Box::pin(core::future::ready(()))
    }

    fn on_pin_expired(&self, store_key: StoreKey<'_>, _size: u64) {
        if let StoreKey::Digest(digest) = store_key {
            // Only act when *this* wrapper has a slow-write outstanding
            // for the digest. The two skip cases are:
            //   1. `DirectoryCache` download pin (no slow-write ever
            //      created — blob came in via download).
            //   2. Slow-write was initiated by a sibling wrapper
            //      sharing the fast store (the sibling's listener is
            //      the one that should fire).
            // In either case, queueing the digest into `failed_slow_writes`
            // would produce a dead-weight reconnect retry for a blob the
            // server already has.
            let owned_key = StoreKey::Digest(digest);
            if !self.in_flight_slow_writes.lock().contains_key(&owned_key) {
                return;
            }
            self.failed_slow_writes.lock().insert(digest);
            warn!(
                ?digest,
                "fast-store pin auto-expired with in-flight slow-write; \
                 queueing digest for slow-write retry on reconnect"
            );
        }
    }
}

/// Best-effort registration of the pin-expiry listener on the fast
/// store. Stores that don't implement `register_item_callback` (e.g.
/// `NoopStore` in unit tests) silently no-op. A registration failure
/// here is logged but not fatal — the slow-write path's other
/// failed_slow_writes inserts still cover the in-band error case; the
/// pin-expiry listener is the safety net for the SILENT-hang case
/// (slow-write neither succeeds nor errors before pin TTL fires).
///
/// Registers a listener that carries BOTH the `failed_slow_writes`
/// Arc (typically shared across wrappers) AND the `in_flight_slow_writes`
/// Arc (per-wrapper). The in-flight gate is what makes the
/// listener safe to register multiple times against the same fast
/// store — see the listener's doc comment for the rationale.
fn register_pin_expire_listener(
    fast_store: &Store,
    failed_slow_writes: Arc<Mutex<HashSet<DigestInfo>>>,
    in_flight_slow_writes: Arc<Mutex<HashMap<StoreKey<'static>, Vec<Bytes>>>>,
) {
    let listener: Arc<dyn ItemCallback> = Arc::new(PinExpireFailedWritesListener {
        failed_slow_writes,
        in_flight_slow_writes,
    });
    if let Err(err) = fast_store.register_item_callback(listener) {
        warn!(
            ?err,
            "FastSlowStore: failed to register pin-expire callback on fast store; \
             slow-write hangs that exceed PIN_TIMEOUT_SECS will not auto-queue retries"
        );
    }
}

// TODO(palfrey) We should consider copying the data in the background to allow the
// client to hang up while the data is buffered. An alternative is to possibly make a
// "BufferedStore" that could be placed on the "slow" store that would hang up early
// if data is in the buffer.
#[derive(Debug, MetricsComponent)]
pub struct FastSlowStore {
    #[metric(group = "fast_store")]
    fast_store: Store,
    fast_direction: StoreDirection,
    #[metric(group = "slow_store")]
    slow_store: Store,
    slow_direction: StoreDirection,
    weak_self: Weak<Self>,
    #[metric]
    metrics: FastSlowStoreMetrics,
    // De-duplicate requests for the fast store, only the first streams, others
    // are blocked.  This may feel like it's causing a slow down of tasks, but
    // actually it's faster because we're not downloading the file multiple
    // times are doing loads of duplicate IO.
    populating_digests: Mutex<HashMap<StoreKey<'static>, (Loader, Arc<StreamingBlobInner>)>>,
    /// Holds data for blobs whose background slow-store write is still in
    /// progress. If the fast store evicts the blob before the slow write
    /// completes, `get_part` serves from this map to prevent NotFound gaps.
    in_flight_slow_writes: Arc<Mutex<HashMap<StoreKey<'static>, Vec<Bytes>>>>,
    /// #212 fixup B2: chunked-path in-flight digest set. Separate from
    /// `in_flight_slow_writes` because the legacy map's `Vec<Bytes>`
    /// shape would (a) double-count memory vs the chunked-driver pin
    /// and (b) trip the legacy map's size-mismatch eviction guard
    /// (which removes entries whose total_len != digest.size_bytes()
    /// — empty-Vec markers would be evicted on first lookup).
    /// Populated + drained by `BazelChunkedDispatcherImpl::dispatch`.
    /// Read by `has_with_results` (returns digest.size_bytes() when
    /// present) and waited by `flush_slow_writes` (graceful drain).
    chunked_in_flight_digests: Arc<Mutex<HashSet<DigestInfo>>>,
    /// Notified when in_flight_slow_writes becomes empty. Used by
    /// `flush_slow_writes` to wait for all background writes to complete.
    in_flight_empty_notify: Arc<Notify>,
    /// Digests that have completed their background slow store write.
    /// Drained by the BlobsInStableStorage loop when notified.
    stable_digests: Arc<Mutex<Vec<DigestInfo>>>,
    /// Wakes the BlobsInStableStorage loop when new digests are available.
    stable_notify: Arc<Notify>,
    /// Set to true during shutdown to prevent new background slow writes
    /// from being spawned while we flush existing ones.
    shutting_down: AtomicBool,
    /// Digests whose background slow-store write failed. Tracked so the
    /// worker can retry uploads on reconnect.
    failed_slow_writes: Arc<Mutex<HashSet<DigestInfo>>>,
    /// Blobs received via server-side mirror that are held in memory only.
    /// These are pinned on the worker indefinitely until the server confirms
    /// the blob is in stable storage via `BlobsInStableStorage`. Per the
    /// mirror-durability invariant: if the server is down or restarting and
    /// has lost the blob, the worker is the *only* durable holder — dropping
    /// the pin on a TTL would lose data. The 2 GiB cap (see
    /// `MIRROR_BLOBS_MAX_BYTES`) is the only bound; the server is expected to
    /// reclaim entries promptly via stable-storage acks.
    ///
    /// Lock acquisition order: `mirror_blobs` BEFORE `mirror_changes`. See
    /// the comment on `mirror_changes` for the deadlock rationale.
    mirror_blobs: Mutex<HashMap<DigestInfo, (Bytes, Instant)>>,
    /// Parallel `(store_id, digest)`-keyed index of dispatcher-pushed
    /// pins (task #168 item 5; partial Plan B5). The values carry no
    /// payload — the bytes still live in `mirror_blobs` keyed by
    /// `DigestInfo`. This index exists ONLY so the worker's
    /// `BlobsAvailableNotification.pinned_mirror_entries` (proto field
    /// 16) can report the (store_id, digest) snapshot to the server in
    /// sorted order (BTreeMap iteration is sorted by `store_id` ASCII —
    /// matches the binary-search precondition in
    /// `EphemeralServerSidePin::observe_pinned_mirror_ack`).
    ///
    /// Rationale for keeping this as a parallel index instead of
    /// keying `mirror_blobs` by `(store_id, digest)` directly (the
    /// full Plan B5):
    ///   - Multi-store collisions on the same digest are inherently
    ///     deduplicable at the byte level (same digest ⇒ same bytes by
    ///     CAS contract), so the storage-level last-writer-wins
    ///     behavior is benign.
    ///   - Production code (`get_part`, `has`, eviction, the
    ///     `mirror_blobs_size_mismatch` regression test) all key on
    ///     `DigestInfo` — moving to a tuple key requires updating each
    ///     call site, which is a separate refactor with its own test
    ///     coverage.
    ///   - Field-16 reporting only needs the `(store_id, digest)`
    ///     coverage, not separate byte storage.
    ///
    /// Insert: `insert_dispatched_mirror_blob` populates this and
    /// `mirror_blobs` together. Remove: when a digest leaves
    /// `mirror_blobs` (`remove_mirror_blobs`, eviction), every
    /// matching `(_, digest)` entry is removed from this index.
    /// Lock acquisition order: this index AFTER `mirror_blobs`.
    dispatched_mirror_pins: Mutex<BTreeMap<(Arc<str>, DigestInfo), ()>>,
    /// Total bytes currently held in `mirror_blobs`. Tracked separately to
    /// enforce `mirror_blobs_max_bytes` without iterating the map.
    mirror_blobs_total_bytes: AtomicU64,
    /// Cap on aggregate mirror bytes; defaults to
    /// `DEFAULT_MIRROR_BLOBS_MAX_BYTES`. Mutable only via the
    /// `set_mirror_blobs_max_bytes_for_test` test hook.
    mirror_blobs_max_bytes: AtomicU64,
    /// Tracks added/removed mirror digests since the last `drain_mirror_changes`
    /// call so the worker's `BlobsAvailable` loop can send incremental updates
    /// without re-snapshotting the whole map.
    ///
    /// Lock acquisition order: `mirror_blobs` BEFORE `mirror_changes`. All
    /// sites that take both locks (including `snapshot_and_reset_mirror_changes`)
    /// MUST acquire `mirror_blobs` first. Inverting the order risks an AB/BA
    /// deadlock with `insert_mirror_blob` / `remove_mirror_blobs`.
    mirror_changes: Mutex<MirrorChanges>,
    /// Notified on every mirror-blob insert/remove so the worker's
    /// `BlobsAvailable` loop can wake immediately.
    mirror_changes_notify: Arc<Notify>,
    /// When true, reads NEVER fall through to the slow store on local miss.
    /// Mirror blobs and the local fast store are still consulted; if the
    /// blob is absent from both (and from the in-flight slow-write buffer),
    /// `get_part`, `has_with_results`, and `batch_get_part_unchunked` return
    /// `NotFound` instead of forwarding to `slow_store`.
    ///
    /// Hard-coded on by the worker's public CAS server wiring (see
    /// [`FastSlowStore::with_local_only_reads`] and `local_worker.rs`) to
    /// prevent a recursive wedge: server asks worker B for digest D → B's
    /// local CAS misses → B's slow tier (`GrpcStore`→server) asks the
    /// server → server's locality map says B has D → server proxies back
    /// to B → indefinite mutual stream-blocking. Workers must answer
    /// `NotFound` on a peer-fetch local miss; the server then routes to a
    /// different peer or serves from its own CAS.
    ///
    /// Writes (mirror inserts, action-execution updates) are unaffected —
    /// only the read fallthrough to the slow tier is suppressed.
    ///
    /// Stored as `AtomicBool` so [`FastSlowStore::with_local_only_reads`]
    /// can flip it through a shared `Arc` without needing `Arc::get_mut`
    /// (`Arc::new_cyclic` leaves a `Weak<Self>` outstanding, which would
    /// make `Arc::get_mut` always return `None`). Writers set it once at
    /// construction; readers see it via `Ordering::Relaxed` since it is
    /// not synchronizing other state.
    local_only_reads: AtomicBool,
    /// #212 Phase 2.5 read-cascade hook: optional registry of in-flight
    /// `ChunkedDriver`s. When set AND [`Self::chunked_reads_enabled`]
    /// is `true`, [`FastSlowStore::get_part`] consults the registry
    /// between the existing in-flight-slow-writes step and the slow
    /// store (design §6.3 step 2 — the `failed_writes` per-chunk pin).
    ///
    /// `None` when the chunked-write handler has not registered the
    /// registry on this `FastSlowStore` (default for every existing
    /// production wiring; the hook lights up only after Phase 2.7
    /// wires the chunked-write handler). Wrapped in
    /// `std::sync::OnceLock<Arc<...>>` (#220 D7): set-once at startup,
    /// lock-free reads on the `get_part` hot path. The constructor
    /// signature does NOT take the registry because the registry's
    /// concrete type lives in `nativelink-store`, but the production
    /// wiring lives in `nativelink-service` (post-hoc install).
    #[cfg(feature = "chunked_fast_slow")]
    chunked_read_registry: OnceLock<Arc<crate::chunked::chunked_read_registry::ChunkedReadRegistry>>,
    /// #212 Phase 2.5 runtime kill-switch for the read cascade's
    /// `failed_writes` pin step. Default OFF — even with the
    /// `chunked_fast_slow` feature compiled in AND a registry wired,
    /// reads continue to skip the pin step until an operator flips
    /// this with [`Self::enable_chunked_reads`]. Mirrors the
    /// `chunked_writes_enabled` kill-switch on `GrpcStore` (Phase
    /// 2.4) — write-side and read-side are independent toggles per
    /// design §14.x phased rollout.
    #[cfg(feature = "chunked_fast_slow")]
    chunked_reads_enabled: AtomicBool,
    /// #212 Phase 2.7 Bazel-facing internal chunking dispatcher.
    ///
    /// When `Some(_)` AND the
    /// `crate::chunked::bazel_facing_internal_chunking_enabled()`
    /// kill-switch returns true AND the incoming blob is at least
    /// `crate::chunked::CHUNK_SIZE` bytes, `update()` hands the upstream
    /// `DropCloserReadHalf` to the dispatcher instead of the legacy
    /// background-spawn slow-tier write path. The dispatcher returns Ok
    /// as soon as the last chunk is admitted to the per-blob driver
    /// mpsc (β async-commit; anti-#203). The fast-tier (MemoryStore)
    /// write that already happens in `update()` provides the in-memory
    /// replica during the async-commit window.
    ///
    /// When `None` (the default for production today) OR the
    /// kill-switch is OFF, `update()` uses the legacy code path with
    /// no behavior change.
    ///
    /// Wrapped in `std::sync::OnceLock<Arc<dyn _>>` (#220 D7): set-once
    /// at startup, lock-free reads on the `update()` hot path. The
    /// dispatcher is installed AFTER `Arc<FastSlowStore>` construction
    /// (the dispatcher's impl in `nativelink-service` requires the
    /// `FilesystemStore` that lives inside the `FastSlowStore`'s slow
    /// tier; circular construction is avoided by post-hoc wiring).
    /// Symmetric with `chunked_read_registry` above.
    #[cfg(feature = "chunked_fast_slow")]
    bazel_chunked_dispatcher: OnceLock<crate::chunked::BazelChunkedDispatcherArc>,
    /// Phase 2.7 size threshold above which `update()` engages the
    /// chunked dispatcher. Production: `CHUNK_SIZE` (1 MiB). Tests
    /// override via `set_chunked_size_threshold_for_test` so a small
    /// blob exercises the chunked path without burning megabytes of
    /// test memory per chunk.
    ///
    /// `AtomicU64` so tests can mutate without `Arc::get_mut`.
    #[cfg(feature = "chunked_fast_slow")]
    chunked_size_threshold: AtomicU64,
}

/// Pending mirror-blob deltas. `added` and `removed` are mutually exclusive
/// per digest within the window (an insert + remove cancels out, and vice
/// versa) so the worker never advertises a digest it has already dropped.
#[derive(Debug, Default)]
pub struct MirrorChanges {
    pub added: HashSet<DigestInfo>,
    pub removed: HashSet<DigestInfo>,
}

// This guard ensures that the populating_digests is cleared even if the future
// is dropped, it is cancel safe.
//
// Holds an `'static` key so the guard can be moved into the spawned producer
// task (see [`FastSlowStore::run_producer`]); without `'static` the spawn
// would fail to satisfy `Send` for non-`'static` lifetimes.
struct LoaderGuard {
    weak_store: Weak<FastSlowStore>,
    key: StoreKey<'static>,
    loader: Option<Loader>,
    /// Streaming buffer shared between the populating thread and waiters.
    /// Waiters read from this to observe the producer's chunks and
    /// terminal state.
    streaming_inner: Arc<StreamingBlobInner>,
    /// True if this guard created a new entry (we're the populator).
    /// False if another thread is already populating (we're a waiter).
    is_new: bool,
}

impl Drop for LoaderGuard {
    fn drop(&mut self) {
        let Some(store) = self.weak_store.upgrade() else {
            // The store has already gone away, nothing to remove from.
            return;
        };
        let Some(loader) = self.loader.take() else {
            // This should never happen, but we do it to be safe.
            return;
        };

        let mut guard = store.populating_digests.lock();
        // Lookup-by-borrow avoids the previous `self.key.borrow().into_owned()`
        // clone on every Drop. We hold `self.key: StoreKey<'static>` directly
        // and reuse it for both the lookup and the conditional remove.
        let should_remove = match guard.get(&self.key) {
            Some((existing_loader, _)) if Arc::ptr_eq(existing_loader, &loader) => {
                drop(loader);
                // Re-lookup after dropping our own loader Arc so the
                // strong_count check sees only the map's own ref + any
                // live LoaderGuards (waiters / inline producer).
                guard
                    .get(&self.key)
                    .is_some_and(|(l, _)| Arc::strong_count(l) == 1)
            }
            _ => false,
        };
        if should_remove {
            guard.remove(&self.key);
        }
    }
}

impl FastSlowStore {
    pub fn new(spec: &FastSlowSpec, fast_store: Store, slow_store: Store) -> Arc<Self> {
        let failed_slow_writes: Arc<Mutex<HashSet<DigestInfo>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let in_flight_slow_writes: Arc<Mutex<HashMap<StoreKey<'static>, Vec<Bytes>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        register_pin_expire_listener(
            &fast_store,
            failed_slow_writes.clone(),
            in_flight_slow_writes.clone(),
        );
        let store = Arc::new_cyclic(|weak_self| Self {
            fast_store,
            fast_direction: spec.fast_direction,
            slow_store,
            slow_direction: spec.slow_direction,
            weak_self: weak_self.clone(),
            metrics: FastSlowStoreMetrics::default(),
            populating_digests: Mutex::new(HashMap::new()),
            in_flight_slow_writes,
            chunked_in_flight_digests: Arc::new(Mutex::new(HashSet::new())),
            in_flight_empty_notify: Arc::new(Notify::new()),
            stable_digests: Arc::new(Mutex::new(Vec::new())),
            stable_notify: Arc::new(Notify::new()),
            shutting_down: AtomicBool::new(false),
            failed_slow_writes,
            mirror_blobs: Mutex::new(HashMap::new()),
            dispatched_mirror_pins: Mutex::new(BTreeMap::new()),
            mirror_blobs_total_bytes: AtomicU64::new(0),
            mirror_blobs_max_bytes: AtomicU64::new(DEFAULT_MIRROR_BLOBS_MAX_BYTES),
            mirror_changes: Mutex::new(MirrorChanges::default()),
            mirror_changes_notify: Arc::new(Notify::new()),
            local_only_reads: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_read_registry: OnceLock::new(),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_reads_enabled: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            bazel_chunked_dispatcher: OnceLock::new(),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_size_threshold: AtomicU64::new(crate::chunked::CHUNK_SIZE as u64),
        });
        // #212 Phase 2.5: honor the production config knob. The
        // `enable_chunked_reads()` runtime API still exists for tests
        // and operator admin tooling, but production opts in through
        // the JSON config field landed in the same series.
        #[cfg(feature = "chunked_fast_slow")]
        if spec.chunked_reads_enabled {
            store.enable_chunked_reads();
        }
        store
    }

    pub fn in_flight_slow_write_count(&self) -> usize {
        self.in_flight_slow_writes.lock().len()
    }

    /// #212 fixup B2: shared handle to the chunked in-flight digest set.
    /// The Phase 2.7 `BazelChunkedDispatcherImpl` registers + removes
    /// digests in this set for the duration of the chunked dispatch.
    /// Reads from this set are folded into:
    ///   - `has_with_results` (returns `digest.size_bytes()` when
    ///     present)
    ///   - `flush_slow_writes` (graceful-drain waits for this set to
    ///     empty too)
    /// This is intentionally SEPARATE from `in_flight_slow_writes`
    /// because the legacy map stores `Vec<Bytes>` of the actual chunk
    /// bytes (used by the get_part replay path); chunked writes track
    /// their bytes via the chunked-driver pin (registered through the
    /// `ChunkedReadRegistry`) so storing a parallel copy in the legacy
    /// map would double-count memory AND trip the legacy map's
    /// size-mismatch eviction guard.
    #[must_use]
    pub fn chunked_in_flight_digests_handle(
        &self,
    ) -> Arc<Mutex<HashSet<DigestInfo>>> {
        self.chunked_in_flight_digests.clone()
    }

    /// #212 fixup B2: shared handle to the empty-notify so the dispatcher
    /// reaper can fire it when the in_flight_slow_writes map drains. The
    /// `flush_slow_writes` waiter relies on this notify to wake on
    /// chunked-path drains the same way it wakes on legacy spawn-task
    /// drains.
    #[must_use]
    pub fn in_flight_empty_notify_handle(&self) -> Arc<Notify> {
        self.in_flight_empty_notify.clone()
    }

    /// Returns a closure that, when invoked with a digest, pushes it
    /// onto the stable-digests queue and wakes the BIS broadcast loop.
    ///
    /// Used by the chunked-write dispatcher (`BazelChunkedDispatcherImpl`)
    /// to mirror the legacy `update`/`update_oneshot` background-spawn
    /// behavior at `fast_slow_store.rs:3449-3450`. Without this push,
    /// chunked-committed digests never appear in the BIS broadcast →
    /// upstream worker `mirror_blobs` and server fast-tier pins
    /// accumulate without bound until the 120 s pin TTL drains them
    /// (the production-incident-2026-05-06 mechanism that drove the
    /// MemoryStore to its 48 GB cap and produced ResourceExhausted).
    ///
    /// Lock acquisition: parking_lot::Mutex, single push + one
    /// notify_one. Never holds across `.await` (the closure is
    /// synchronous). Calling more than once for the same digest is
    /// idempotent at the protocol layer (the broadcast tolerates
    /// duplicates) but should be avoided to keep the queue bounded —
    /// the chunked dispatcher pushes exactly once on commit success.
    #[must_use]
    pub fn stable_digests_pusher(&self) -> Arc<dyn Fn(DigestInfo) + Send + Sync> {
        let stable_digests = self.stable_digests.clone();
        let stable_notify = self.stable_notify.clone();
        Arc::new(move |digest: DigestInfo| {
            stable_digests.lock().push(digest);
            stable_notify.notify_one();
        })
    }

    /// #212 Phase 2.7 helper: dispatch a Bazel-facing write through
    /// the chunked path. Tees the upstream bytes into BOTH the fast
    /// tier (MemoryStore — in-memory replica satisfying ≥2-replica
    /// during the async-commit window) AND the chunked dispatcher
    /// (which drives the per-blob `ChunkedDriver`).
    ///
    /// Returns Ok as soon as the dispatcher's admission completes (not
    /// after commit). The driver continues on its own task; reads
    /// during the async-commit window are served from the fast tier
    /// (inline write below) OR via Phase 2.5's `failed_writes` /
    /// in-flight pin once that lands.
    ///
    /// (β) async-commit. Anti-#203 mandatory.
    #[cfg(feature = "chunked_fast_slow")]
    async fn update_via_chunked_dispatcher(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
        digest: DigestInfo,
        dispatcher: crate::chunked::BazelChunkedDispatcherArc,
        update_start: std::time::Instant,
    ) -> Result<(), Error> {
        debug!(
            ?key,
            ?size_info,
            size_bytes = digest.size_bytes(),
            "FastSlowStore::update: chunked-dispatch path (Phase 2.7)",
        );

        // Build two channels: one to the fast tier (existing
        // contract), one to the chunked dispatcher. The data-stream
        // future tees each chunk into both.
        let (mut fast_tx, fast_rx) = make_buf_channel_pair_with_size(128);
        let (mut chunk_tx, chunk_rx) = make_buf_channel_pair_with_size(128);

        let data_stream_fut = async move {
            // Writer-termination contract for `join3(data_stream_fut,
            // fast_store_fut, dispatch_fut)`: any `?` Err exit below
            // historically dropped `fast_tx` / `chunk_tx` silently, so the
            // sibling `fast_store_fut` (= `inner.update(rx)`) and
            // `dispatch_fut` (= `dispatcher.dispatch(chunk_rx)`) blocked
            // on `rx.recv().await` until the OS dropped the senders, then
            // synthesized a generic `"Sender dropped before sending EOF"`
            // Internal — drowning out the actionable upstream cause.
            //
            // The two `WriteHalfGuard` wraps below upgrade the Drop
            // fallback so that on any `?` propagation the guards' `Drop`
            // (`buf_channel.rs:516-520`) synthesizes a structured Internal
            // carrying the greppable `"buf_channel: writer dropped without
            // commit"` marker. Both paired readers observe a structured
            // Err immediately instead of the misleading "Sender dropped"
            // derivative. The OUTER `join3(...)` result still preserves
            // the actionable upstream cause via `data_res` — Drop fallback
            // on the readers is purely defense-in-depth so they unblock,
            // not the carrier of the upstream cause. Latent harden in the
            // #245 family — see audit at
            // `.claude/audits/245-fast-slow-store-sender-drop.md` Phase 4
            // sibling-bug list.
            let mut fast_guard = WriteHalfGuard::new(&mut fast_tx);
            let mut chunk_guard = WriteHalfGuard::new(&mut chunk_tx);
            loop {
                let buffer = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to read buffer in fast_slow chunked dispatch")?;
                if buffer.is_empty() {
                    fast_guard
                        .commit_eof()
                        .err_tip(|| "Failed to send eof to fast store (chunked path)")?;
                    chunk_guard
                        .commit_eof()
                        .err_tip(|| "Failed to send eof to chunked dispatcher")?;
                    return Result::<(), Error>::Ok(());
                }
                let buf_for_chunk = buffer.clone();
                (*fast_guard).send(buffer).await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to send to fast store in chunked dispatch: {:?}",
                        e
                    )
                })?;
                (*chunk_guard).send(buf_for_chunk).await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to send to chunked dispatcher: {:?}",
                        e
                    )
                })?;
            }
        };

        let fast_store_fut = self.fast_store.update(key.borrow(), fast_rx, size_info);
        let dispatch_fut = dispatcher.dispatch(digest, chunk_rx);

        let (data_res, fast_res, dispatch_res) =
            futures::future::join3(data_stream_fut, fast_store_fut, dispatch_fut).await;

        let data_elapsed = update_start.elapsed();

        if let Err(err) = data_res {
            error!(
                ?key,
                elapsed_ms = data_elapsed.as_millis() as u64,
                ?err,
                "FastSlowStore::update (chunked): data stream failed",
            );
            return Err(err);
        }
        if let Err(err) = &fast_res {
            error!(
                ?key,
                elapsed_ms = data_elapsed.as_millis() as u64,
                ?err,
                "FastSlowStore::update (chunked): fast store write failed",
            );
        }
        fast_res?;

        // Pin the digest in the fast store: this is what guarantees the
        // in-memory replica survives until the chunked driver's commit
        // lands (or until BlobsInStableStorage acks).
        if let StoreKey::Digest(d) = &key {
            self.fast_store.pin_digests(&[*d]);
        }

        let dispatch_committed_size = match dispatch_res {
            Ok(size) => size,
            Err(err) => {
                // Admission rejection (global budget, mpsc full, dup
                // digest) or in-stream error. The fast tier already has
                // the blob; record the digest for retry-on-reconnect so
                // the mirror protocol can recover.
                if let StoreKey::Digest(d) = &key {
                    self.failed_slow_writes.lock().insert(*d);
                    self.fast_store.pin_digests(&[*d]);
                }
                error!(
                    ?key,
                    elapsed_ms = data_elapsed.as_millis() as u64,
                    ?err,
                    "FastSlowStore::update (chunked): dispatcher admission FAILED — \
                     blob pinned in fast tier, will retry on reconnect"
                );
                return Err(err);
            }
        };

        info!(
            ?key,
            elapsed_ms = data_elapsed.as_millis() as u64,
            size_bytes = digest.size_bytes(),
            committed_size = dispatch_committed_size,
            "FastSlowStore::update (chunked): admission complete; \
             driver continues async (β commit)"
        );

        Ok(())
    }

    /// #212 Phase 2.7: install the Bazel-facing internal-chunking
    /// dispatcher. The dispatcher's implementation lives in
    /// `nativelink-service` (the per-blob `ChunkedDriver` machinery is
    /// wired into the RPC layer there); we accept the trait object here
    /// to break the otherwise-circular dependency.
    ///
    /// Production wiring: call this from the server bootstrap AFTER
    /// constructing the FastSlowStore + FilesystemStore (see
    /// `src/bin/nativelink.rs` slow-tier setup; chunked-dispatch
    /// installation is a no-op when `chunked_fast_slow` is not
    /// compiled in).
    ///
    /// The actual decision to USE this dispatcher per-call is gated on
    /// `crate::chunked::bazel_facing_internal_chunking_enabled()` (the
    /// kill-switch defaulting to OFF) AND blob size; see `update()`.
    ///
    /// `#220` D7: backed by `OnceLock`. The first install wins; a
    /// second install attempt is silently ignored (callers today install
    /// once at startup, so the difference is invisible). The lock-free
    /// read on the `update()` hot path is the win.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn set_bazel_chunked_dispatcher(
        &self,
        dispatcher: crate::chunked::BazelChunkedDispatcherArc,
    ) {
        // OnceLock::set returns Err if already initialized; this matches
        // the production "install once at startup" wiring contract. We
        // intentionally drop the Err — repeat installs are no-ops.
        let _ = self.bazel_chunked_dispatcher.set(dispatcher);
    }

    /// Read-only accessor for the chunked dispatcher; tests use this
    /// to confirm wiring. Returns `None` if no dispatcher is installed.
    /// `#220` D7: lock-free `OnceLock::get` + one `Arc::clone`.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn bazel_chunked_dispatcher(
        &self,
    ) -> Option<crate::chunked::BazelChunkedDispatcherArc> {
        self.bazel_chunked_dispatcher.get().cloned()
    }

    /// Read-only accessor for the fast-tier `Store`. Phase 2.7 tests
    /// use this to verify the fast-tier MemoryStore visibility surface
    /// during the (β) async-commit window — `FastSlowStore::has()`
    /// itself does NOT consult the fast tier by default (only slow +
    /// in_flight_slow_writes + mirror_blobs unless `local_only_reads`
    /// is set).
    #[must_use]
    pub fn fast_store_handle(&self) -> &Store {
        &self.fast_store
    }

    /// Read-only accessor for the slow-tier `Store`.
    #[must_use]
    pub fn slow_store_handle(&self) -> &Store {
        &self.slow_store
    }

    /// #212 fixup S1: clone the slow-tier `Store` (one `Arc` bump). The
    /// production wiring at server startup (in `src/bin/nativelink.rs`)
    /// uses this to get the slow-tier `Arc<dyn StoreDriver>`, which it
    /// downcasts to `Arc<FilesystemStore<FileEntryImpl>>` for
    /// constructing the chunked dispatcher.
    #[must_use]
    pub fn slow_store_clone(&self) -> Store {
        self.slow_store.clone()
    }

    /// #212 fixup S4 test accessor: returns `true` iff the digest is
    /// currently in `failed_slow_writes`. The set is populated when a
    /// slow-tier write fails (legacy spawn path OR chunked-dispatch
    /// admission rejection); the entry is consumed by the
    /// retry-on-reconnect path (mirror protocol). Tests assert
    /// presence to verify the failure-recovery hook fired.
    #[must_use]
    pub fn failed_slow_writes_contains(&self, digest: &DigestInfo) -> bool {
        self.failed_slow_writes.lock().contains(digest)
    }

    /// Phase 2.7 chunked size threshold (cached on this store). Reads
    /// from a relaxed atomic; cheap on the `update()` hot path.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_size_threshold(&self) -> u64 {
        self.chunked_size_threshold.load(Ordering::Relaxed)
    }

    /// Test-only: override the chunked size threshold (default
    /// `CHUNK_SIZE` = 1 MiB). Used by Phase 2.7 tests to exercise the
    /// chunked dispatch path with small blobs without burning megabytes
    /// per chunk.
    #[cfg(all(feature = "chunked_fast_slow", any(test, feature = "test-utils")))]
    #[doc(hidden)]
    pub fn set_chunked_size_threshold_for_test(&self, threshold: u64) {
        self.chunked_size_threshold
            .store(threshold, Ordering::Relaxed);
    }

    /// Test-only: insert a synthetic in-flight slow-write entry. Used by the
    /// `flush_slow_writes` lost-wakeup regression test to drive the predicate
    /// without spinning up the full populate machinery.
    #[doc(hidden)]
    pub fn test_insert_in_flight(&self, key: StoreKey<'static>, chunks: Vec<Bytes>) {
        self.in_flight_slow_writes.lock().insert(key, chunks);
    }

    /// Test-only: remove an in-flight slow-write entry and fire
    /// `notify_waiters()` if the map is now empty — mirrors the production
    /// completion path (see lines around `in_flight_empty_notify.notify_waiters()`).
    #[doc(hidden)]
    pub fn test_remove_in_flight_and_notify(&self, key: StoreKey<'static>) {
        let now_empty = {
            let mut guard = self.in_flight_slow_writes.lock();
            guard.remove(&key);
            guard.is_empty()
        };
        if now_empty {
            self.in_flight_empty_notify.notify_waiters();
        }
    }

    /// Returns the streaming-blob inner for an in-flight populate, if one
    /// exists for `key`. Diagnostic / test helper: lets callers (and
    /// regression tests) inspect terminal state and verify that errors
    /// propagated via `send_error` BEFORE the writer was dropped, rather
    /// than the writer's `Drop` impl setting a generic Internal error.
    #[doc(hidden)]
    pub fn populating_streaming_inner(
        &self,
        key: StoreKey<'_>,
    ) -> Option<Arc<StreamingBlobInner>> {
        let owned = key.into_owned();
        self.populating_digests
            .lock()
            .get(&owned)
            .map(|(_, inner)| Arc::clone(inner))
    }

    /// Test-only: install a pre-built `StreamingBlobInner` into
    /// `populating_digests` so a subsequent `get_part` waiter
    /// deterministically enters the terminal-state branch. Used by the
    /// task-#124 regression test (`failed_populate_does_not_reissue_slow_store_probe`)
    /// to reproduce production state where the producer already
    /// terminated with an error and a new waiter arrives.
    #[doc(hidden)]
    pub fn test_install_terminal_populate(
        &self,
        key: StoreKey<'static>,
        streaming_inner: Arc<StreamingBlobInner>,
    ) {
        let loader: Loader = Arc::new(());
        self.populating_digests
            .lock()
            .insert(key, (loader, streaming_inner));
    }

    /// Test-only: insert a `mirror_blobs` entry without the size-validation
    /// invariant that `insert_mirror_blob` enforces. Used by the
    /// writer-termination regression tests to install a phantom-positive
    /// (data.len() != digest.size_bytes()) entry and exercise the
    /// `mirror_blobs` size-mismatch early return at `get_part`.
    #[doc(hidden)]
    pub fn test_insert_mirror_blob_unchecked(&self, digest: DigestInfo, data: Bytes) {
        let now = Instant::now();
        let data_len = data.len() as u64;
        let mut blobs = self.mirror_blobs.lock();
        if let Some((old, _)) = blobs.insert(digest, (data, now)) {
            let old_len = old.len() as u64;
            if data_len > old_len {
                self.mirror_blobs_total_bytes
                    .fetch_add(data_len - old_len, Ordering::Relaxed);
            } else if old_len > data_len {
                self.mirror_blobs_total_bytes
                    .fetch_sub(old_len - data_len, Ordering::Relaxed);
            }
        } else {
            self.mirror_blobs_total_bytes
                .fetch_add(data_len, Ordering::Relaxed);
        }
    }

    /// Diagnostic / test-only counter: every `tokio::spawn` performed by the
    /// populate machinery in `spawn_populate_producer_with_role` increments
    /// this counter. The inline-fast-path in [`copy_slow_to_fast`] leaves
    /// it unchanged for single-caller cache-miss populates. Used by
    /// `populate_inline_does_not_spawn` to keep the optimisation honest.
    #[doc(hidden)]
    pub fn populate_spawn_count(&self) -> u64 {
        self.metrics
            .populate_spawn_count
            .load(Ordering::Acquire)
    }

    /// Fence out new background slow writes and wait for all existing
    /// ones to complete, with a timeout. Returns the number of writes
    /// still pending when the timeout expired (0 = all flushed).
    ///
    /// #212 fixup B2: also waits for the chunked-path in-flight digest
    /// set to drain. Both maps share the same `in_flight_empty_notify`
    /// (the chunked-dispatch reaper fires it when its set drains) so a
    /// single notify-wait covers both.
    pub async fn flush_slow_writes(&self, timeout: Duration) -> usize {
        self.shutting_down.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Register the notified future BEFORE checking the count to
            // avoid missing a notification between check and await.
            // `Notify::notified()` does not register interest until the
            // future is first polled, so we must `pin!` + `enable()` to
            // arm the subscription before evaluating the predicate.
            // Otherwise a `notify_waiters()` racing with the predicate
            // check (e.g. the in-flight write completing concurrently)
            // is silently dropped — the canonical tokio::sync::Notify
            // lost-wakeup pattern. See sibling fixes f1750357 (cleanup
            // wait) and the streaming_blob audit aeb299cfb735c56b1.
            let notified = self.in_flight_empty_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let legacy_count = self.in_flight_slow_writes.lock().len();
            let chunked_count = self.chunked_in_flight_digests.lock().len();
            if legacy_count == 0 && chunked_count == 0 {
                return 0;
            }
            match tokio::time::timeout_at(deadline, notified).await {
                Ok(()) => continue,
                Err(_) => {
                    let guard = self.in_flight_slow_writes.lock();
                    let chunked_guard = self.chunked_in_flight_digests.lock();
                    let remaining = guard.len() + chunked_guard.len();
                    if remaining > 0 {
                        warn!(
                            legacy = guard.len(),
                            chunked = chunked_guard.len(),
                            "FastSlowStore::flush_slow_writes: timed out waiting \
                             for background writes to complete"
                        );
                        for (key, chunks) in guard.iter() {
                            let bytes: usize = chunks.iter().map(|b| b.len()).sum();
                            warn!(
                                ?key,
                                bytes,
                                "FastSlowStore: unflushed write at shutdown"
                            );
                        }
                        for digest in chunked_guard.iter() {
                            warn!(
                                ?digest,
                                "FastSlowStore: unflushed chunked-path write at shutdown"
                            );
                        }
                    }
                    return remaining;
                }
            }
        }
    }

    /// Phase 2 of graceful shutdown (#210): enumerate every key in the
    /// fast tier and write any blob the slow tier does not yet hold to
    /// the slow tier before returning. Returns the number of fast-tier
    /// blobs that could NOT be persisted within the deadline (0 when all
    /// blobs are durable).
    ///
    /// **Why this exists in addition to [`Self::flush_slow_writes`]:**
    /// `flush_slow_writes` only drains the in-flight write map populated
    /// when a write enters `update`/`update_oneshot` and spawns a
    /// background slow-store task. Blobs that landed in the fast tier
    /// via OTHER paths (slow-tier read back-population, peer mirror
    /// promotion, retry-after-failed-slow-write where the in-flight
    /// entry was already removed, or any direct write to the fast store
    /// that bypassed the spawn) are invisible to that drain. With a
    /// MemoryStore fast tier (`cas_FAST_SLOW_STORE`,
    /// `SMALL_CAS_CACHED`), every such blob dies with the process and
    /// the build later fails with "Lost inputs no longer available
    /// remotely" because the AC entry references a CAS digest that
    /// vanished. #206's investigation observed 9904 of 66174 worker-
    /// reported blobs missing for 6+ hours after restart, with 4 of 5
    /// sampled small-blob digests missing from Redis.
    ///
    /// **Contract directions** (CLAUDE.md asymmetric-coverage rule):
    /// - **Under-action:** every fast-tier blob NOT in slow tier MUST be
    ///   written to the slow tier before this returns (within the
    ///   deadline). Failure mode: lost data on restart.
    /// - **Over-action / deadline:** this MUST return within ~`deadline`
    ///   even if the slow tier is so slow it cannot drain everything.
    ///   Failure mode: a wedged slow tier blocks SIGTERM-to-exit
    ///   forever, which `nativelink.rs:1488`'s 35s drain wait would
    ///   interpret as a hang.
    /// - **Per-entry tolerance:** a single failing slow-tier write MUST
    ///   NOT abort the loop — the next blob still has a chance.
    /// - **Skip-existing:** a blob the slow tier already has MUST NOT be
    ///   re-written (avoids amplifying tail-latency on stores like
    ///   FilesystemStore where update_oneshot is non-trivial).
    ///
    /// Mirror blobs (`mirror_blobs`) are intentionally NOT flushed here
    /// — those are pinned in memory only by design until the server's
    /// `BlobsInStableStorage` ack arrives, and writing them to the slow
    /// tier would defeat the mirror-only invariant. The
    /// `mirror_blobs_max_bytes` cap already bounds their footprint.
    pub async fn flush_fast_to_slow_at_shutdown(&self, deadline: Duration) -> usize {
        // Snapshot every key currently in the fast tier. We use the
        // generic `StoreDriver::list` API so this works for any fast
        // tier that implements list, but in production this is a
        // `MemoryStore` whose list is a btree-ordered iteration over
        // the moka-backed evicting map (see `MemoryStore::list`).
        //
        // Collecting the keys up-front (before doing any work) lets us
        // (a) drop the btree read-lock immediately, (b) iterate the
        // snapshot deterministically without re-locking on every
        // iteration, and (c) bound the per-blob deadline check by a
        // simple counter.
        let started = Instant::now();
        let mut keys: Vec<StoreKey<'static>> = Vec::new();
        {
            // The handler captures &mut keys but cannot escape this
            // block, so the borrow is sound across the .await.
            let collected = self
                .fast_store
                .list(.., |key| {
                    keys.push(key.borrow().into_owned());
                    true
                })
                .await;
            if let Err(err) = collected {
                // Some stores (e.g. NoopStore in tests, or any store
                // that rejects list with Unimplemented) cannot be
                // enumerated. Log and bail — there is no useful work
                // we can do without enumeration.
                warn!(
                    ?err,
                    fast_store = %self.fast_store.inner_store(
                        Option::<StoreKey<'_>>::None
                    ).get_name(),
                    "FastSlowStore::flush_fast_to_slow_at_shutdown: fast tier \
                     does not support list; cannot enumerate to flush"
                );
                return 0;
            }
        }
        let snapshot_count = keys.len();
        info!(
            snapshot_count,
            deadline_secs = deadline.as_secs(),
            fast_store = %self.fast_store.inner_store(
                Option::<StoreKey<'_>>::None
            ).get_name(),
            slow_store = %self.slow_store.inner_store(
                Option::<StoreKey<'_>>::None
            ).get_name(),
            "FastSlowStore::flush_fast_to_slow_at_shutdown: enumerated fast tier"
        );

        if snapshot_count == 0 {
            return 0;
        }

        // Per-blob loop. We honor the deadline by checking
        // `started.elapsed()` before each iteration — once we cross it,
        // every remaining key is reported as unflushed. Per-entry
        // errors are logged but do not abort the loop.
        let mut flushed = 0usize;
        let mut skipped_already = 0usize;
        let mut errored = 0usize;
        let mut deadline_remaining = snapshot_count;
        for (idx, key) in keys.iter().enumerate() {
            if started.elapsed() >= deadline {
                let unflushed = snapshot_count - idx;
                warn!(
                    snapshot_count,
                    flushed,
                    skipped_already,
                    errored,
                    unflushed,
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "FastSlowStore::flush_fast_to_slow_at_shutdown: deadline \
                     exceeded; remaining blobs will be lost on exit"
                );
                deadline_remaining = unflushed;
                break;
            }
            // Skip blobs the slow tier already holds. A failed `has`
            // check is treated as "unknown" → write anyway (better to
            // duplicate-write than to silently lose).
            match self.slow_store.has(key.borrow()).await {
                Ok(Some(_)) => {
                    skipped_already += 1;
                    deadline_remaining = deadline_remaining.saturating_sub(1);
                    continue;
                }
                Ok(None) => {} // proceed to write
                Err(err) => {
                    debug!(
                        ?key,
                        ?err,
                        "FastSlowStore::flush_fast_to_slow_at_shutdown: slow.has \
                         failed; will attempt write anyway"
                    );
                }
            }

            // Read the bytes from the fast tier. We use
            // `get_part_unchunked` to materialize the full blob — for
            // MemoryStore this is a near-zero-copy fetch from the
            // BytesWrapper chain. We deliberately do NOT stream into
            // the slow tier via a buf_channel: the slow stores at this
            // point are Redis (oneshot) or FilesystemStore (also
            // oneshot-ish), and `update_oneshot` is the cheaper path
            // when the data is already in memory.
            let data = match self
                .fast_store
                .get_part_unchunked(key.borrow(), 0, None)
                .await
            {
                Ok(b) => b,
                Err(err) => {
                    // Likely an evicted-since-list race or a corrupt
                    // entry. Skip-and-continue is the only safe
                    // response — there is nothing to write.
                    debug!(
                        ?key,
                        ?err,
                        "FastSlowStore::flush_fast_to_slow_at_shutdown: fast.get \
                         failed; skipping (likely evicted between list and read)"
                    );
                    errored += 1;
                    deadline_remaining = deadline_remaining.saturating_sub(1);
                    continue;
                }
            };

            // Write to the slow tier. Per-entry errors are logged and
            // skipped; we do NOT propagate them. (Returning early would
            // strand later blobs.)
            match self.slow_store.update_oneshot(key.borrow(), data).await {
                Ok(()) => {
                    flushed += 1;
                }
                Err(err) => {
                    warn!(
                        ?key,
                        ?err,
                        "FastSlowStore::flush_fast_to_slow_at_shutdown: slow.update \
                         failed; blob will be lost on exit"
                    );
                    errored += 1;
                }
            }
            deadline_remaining = deadline_remaining.saturating_sub(1);
        }

        let elapsed_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if errored > 0 || deadline_remaining > 0 {
            // Loud summary line that ops will grep for after a restart
            // when investigating "Lost inputs" / missing-CAS reports.
            warn!(
                snapshot_count,
                flushed,
                skipped_already,
                errored,
                deadline_exceeded = deadline_remaining,
                elapsed_ms,
                "FastSlowStore::flush_fast_to_slow_at_shutdown: COMPLETED WITH \
                 UNFLUSHED OR FAILED BLOBS — these will be lost on exit"
            );
        } else {
            info!(
                snapshot_count,
                flushed,
                skipped_already,
                elapsed_ms,
                "FastSlowStore::flush_fast_to_slow_at_shutdown: drained fast tier"
            );
        }
        // Unflushed = errored + deadline-exceeded. Skipped-already is NOT
        // counted as unflushed because the slow tier already holds the blob.
        errored + deadline_remaining
    }

    pub const fn fast_store(&self) -> &Store {
        &self.fast_store
    }

    pub const fn slow_store(&self) -> &Store {
        &self.slow_store
    }

    pub const fn fast_direction(&self) -> StoreDirection {
        self.fast_direction
    }

    pub const fn slow_direction(&self) -> StoreDirection {
        self.slow_direction
    }

    pub fn get_arc(&self) -> Option<Arc<Self>> {
        self.weak_self.upgrade()
    }

    /// Drain all digests that have completed their slow store write since the last drain.
    /// Called by the BlobsInStableStorage batching loop.
    pub fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        let mut guard = self.stable_digests.lock();
        std::mem::take(&mut *guard)
    }

    /// Drain digests whose background slow-store write failed.
    /// Called by the worker on reconnect to retry uploads.
    pub fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        let mut guard = self.failed_slow_writes.lock();
        guard.drain().collect()
    }

    /// Remove digests from the failed/pending set, e.g. when the server
    /// confirms stable storage via BlobsInStableStorage.
    pub fn ack_digests(&self, digests: &[DigestInfo]) {
        let mut guard = self.failed_slow_writes.lock();
        for digest in digests {
            guard.remove(digest);
        }
    }

    /// Create a new FastSlowStore that shares the failed_slow_writes
    /// tracking set with another store. Used so the worker CAS server
    /// store and RunningActionsManager store track pending uploads in
    /// the same place.
    pub fn new_with_shared_failed_writes(
        spec: &FastSlowSpec,
        fast_store: Store,
        slow_store: Store,
        other: &Arc<Self>,
    ) -> Arc<Self> {
        let shared = other.failed_slow_writes.clone();
        let in_flight_slow_writes: Arc<Mutex<HashMap<StoreKey<'static>, Vec<Bytes>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        register_pin_expire_listener(&fast_store, shared.clone(), in_flight_slow_writes.clone());
        let store = Arc::new_cyclic(|weak_self| Self {
            fast_store,
            fast_direction: spec.fast_direction,
            slow_store,
            slow_direction: spec.slow_direction,
            weak_self: weak_self.clone(),
            metrics: FastSlowStoreMetrics::default(),
            populating_digests: Mutex::new(HashMap::new()),
            in_flight_slow_writes,
            chunked_in_flight_digests: Arc::new(Mutex::new(HashSet::new())),
            in_flight_empty_notify: Arc::new(Notify::new()),
            stable_digests: Arc::new(Mutex::new(Vec::new())),
            stable_notify: Arc::new(Notify::new()),
            shutting_down: AtomicBool::new(false),
            failed_slow_writes: shared,
            mirror_blobs: Mutex::new(HashMap::new()),
            dispatched_mirror_pins: Mutex::new(BTreeMap::new()),
            mirror_blobs_total_bytes: AtomicU64::new(0),
            mirror_blobs_max_bytes: AtomicU64::new(DEFAULT_MIRROR_BLOBS_MAX_BYTES),
            mirror_changes: Mutex::new(MirrorChanges::default()),
            mirror_changes_notify: Arc::new(Notify::new()),
            local_only_reads: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_read_registry: OnceLock::new(),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_reads_enabled: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            bazel_chunked_dispatcher: OnceLock::new(),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_size_threshold: AtomicU64::new(crate::chunked::CHUNK_SIZE as u64),
        });
        // #212 Phase 2.5: honor the production config knob (mirror of
        // `new`).
        #[cfg(feature = "chunked_fast_slow")]
        if spec.chunked_reads_enabled {
            store.enable_chunked_reads();
        }
        store
    }

    /// Flip on `local_only_reads` mode. See the field-level comment for the
    /// rationale. Intended as a one-shot construction-time flip; chain
    /// directly off the constructor at `local_worker.rs` callsite. Stored
    /// as an atomic so this works on a freshly-built `Arc<Self>` (which
    /// already has a `Weak<Self>` outstanding from `Arc::new_cyclic`).
    #[must_use]
    pub fn with_local_only_reads(self: Arc<Self>) -> Arc<Self> {
        self.local_only_reads.store(true, Ordering::Relaxed);
        self
    }

    /// Returns `true` if this instance is configured to refuse slow-store
    /// fallback on local miss (worker public CAS server variant).
    #[inline]
    pub fn local_only_reads(&self) -> bool {
        self.local_only_reads.load(Ordering::Relaxed)
    }

    /// #212 Phase 2.5: install the in-flight chunked-driver registry
    /// for the design §6.3 step 2 read cascade. Wired by the chunked-
    /// write handler (Phase 2.7) at server startup once. The registry
    /// is consulted from [`Self::get_part`] AFTER the kill-switch
    /// [`Self::chunked_reads_enabled`] is `true`; absent the switch
    /// flip, this is dead-store memory and the read cascade behaves
    /// exactly like origin/main.
    ///
    /// Set-once: returns `true` when the registry is newly installed,
    /// `false` when one was already installed (the supplied registry
    /// is dropped). `#220` D7: backed by `OnceLock` for symmetry with
    /// [`Self::set_bazel_chunked_dispatcher`] and lock-free reads on
    /// the `get_part` hot path. Today's production wiring installs once
    /// at server start, so the second-install branch is exercised only
    /// by tests intentionally probing idempotence.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn set_chunked_read_registry(
        &self,
        registry: Arc<crate::chunked::chunked_read_registry::ChunkedReadRegistry>,
    ) -> bool {
        self.chunked_read_registry.set(registry).is_ok()
    }

    /// #212 Phase 2.5: snapshot of the currently-installed registry
    /// (or `None`). Used by tests to assert the wire-up.
    /// `#220` D7: lock-free `OnceLock::get` + one `Arc::clone`.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_read_registry(
        &self,
    ) -> Option<Arc<crate::chunked::chunked_read_registry::ChunkedReadRegistry>> {
        self.chunked_read_registry.get().cloned()
    }

    /// #212 Phase 2.5: flip the read-cascade kill-switch ON. With the
    /// switch ON AND a registry installed via
    /// [`Self::set_chunked_read_registry`], [`Self::get_part`] consults
    /// the registry between the existing in-flight-slow-writes step
    /// and the slow store. With the switch OFF the registry is
    /// ignored — preserves the pre-Phase-2.5 read path exactly.
    ///
    /// Mirrors the [`crate::grpc_store::GrpcStore`]
    /// `enable_chunked_writes` ramp pattern (Phase 2.4): compile +
    /// ship, flip later.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn enable_chunked_reads(&self) {
        self.chunked_reads_enabled.store(true, Ordering::Relaxed);
        info!("FastSlowStore: chunked-read cascade ENABLED (Phase 2.5)");
    }

    /// #212 Phase 2.5: flip the read-cascade kill-switch OFF. Used by
    /// the operator rollback path AND by tests that wire a registry
    /// but want to assert the OFF behaviour does not consult it.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn disable_chunked_reads(&self) {
        self.chunked_reads_enabled.store(false, Ordering::Relaxed);
        info!("FastSlowStore: chunked-read cascade DISABLED (Phase 2.5)");
    }

    /// Returns `true` if [`Self::enable_chunked_reads`] has been called
    /// AND a registry is installed. Read cascade in [`Self::get_part`]
    /// uses the same composite check.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_reads_enabled(&self) -> bool {
        self.chunked_reads_enabled.load(Ordering::Relaxed)
    }

    /// Remove mirror blobs that the server has confirmed are in stable storage.
    /// Records each removed digest in the mirror change tracker so the worker
    /// emits a corresponding `evicted_digests` entry on its next BlobsAvailable.
    pub fn remove_mirror_blobs(&self, digests: &[DigestInfo]) {
        // Hold both locks for the duration so an interleaved
        // `drain_mirror_changes` + snapshot from the BlobsAvailable loop sees
        // a coherent view of pin map vs change tracker (no torn state where
        // a digest is removed from `mirror_blobs` but the `removed` delta has
        // not been recorded yet).
        let mut blobs = self.mirror_blobs.lock();
        let mut changes = self.mirror_changes.lock();
        let mut freed = 0u64;
        let mut any_removed = false;
        for digest in digests {
            if let Some((data, _)) = blobs.remove(digest) {
                freed += data.len() as u64;
                changes.added.remove(digest);
                changes.removed.insert(*digest);
                any_removed = true;
            }
        }
        drop(changes);
        drop(blobs);
        if freed > 0 {
            self.mirror_blobs_total_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
        // task #168 item 5: also clean up the parallel
        // `dispatched_mirror_pins` index for removed digests so the
        // worker's next `BlobsAvailable` field 16 snapshot does not
        // re-advertise an unpinned (store_id, digest). Removes EVERY
        // matching `(_, digest)` entry across all source store_ids
        // (multiple stores' pins for the same digest collapse on
        // confirm).
        if any_removed {
            self.retain_pins_dropping(digests);
        }
    }

    /// Drop every `dispatched_mirror_pins` entry whose `DigestInfo`
    /// appears in `digests`, then `notify_one()` if anything was
    /// removed. Shared between [`Self::remove_mirror_blobs`] (CAS BIS
    /// ack) and [`Self::remove_local_ac_pins`] (worker AC BIS ack /
    /// fast-tier eviction). Builds a `HashSet<DigestInfo>` once so
    /// the retain is `O(|pins| + |digests|)`, not `O(|pins| × |digests|)`.
    fn retain_pins_dropping(&self, digests: &[DigestInfo]) {
        if digests.is_empty() {
            return;
        }
        // Build the lookup set OUTSIDE the lock so a long `digests`
        // slice cannot extend the parking_lot critical section.
        let lookup: std::collections::HashSet<&DigestInfo> = digests.iter().collect();
        let mut pins = self.dispatched_mirror_pins.lock();
        if pins.is_empty() {
            return;
        }
        let before = pins.len();
        pins.retain(|(_, d), ()| !lookup.contains(d));
        let removed = before - pins.len();
        drop(pins);
        if removed > 0 {
            self.mirror_changes_notify.notify_one();
        }
    }

    /// Current number of mirror blobs held in memory.
    pub fn mirror_blob_count(&self) -> usize {
        self.mirror_blobs.lock().len()
    }

    /// Current total bytes held in `mirror_blobs`. Used by
    /// `send_periodic_blobs_available` to advertise capacity to the
    /// server's mirror picker (review #1: pre-check capacity before
    /// consuming the source stream).
    pub fn mirror_blobs_used_bytes(&self) -> u64 {
        self.mirror_blobs_total_bytes.load(Ordering::Relaxed)
    }

    /// Configured cap on `mirror_blobs` aggregate bytes. Reported with
    /// `mirror_blobs_used_bytes` so the server's picker can compute
    /// remaining capacity per peer.
    pub fn mirror_blobs_max_bytes(&self) -> u64 {
        self.mirror_blobs_max_bytes.load(Ordering::Relaxed)
    }

    /// Test-only: override the mirror-blob byte cap so cap-exceeded paths
    /// can be exercised without allocating gigabytes. Production code MUST
    /// NOT call this — the cap is sized for production memory budgets.
    /// Gated on `cfg(test)` (in-crate use) and the `test-utils` feature
    /// (external integration tests) so it cannot leak into release builds.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn set_mirror_blobs_max_bytes_for_test(&self, cap: u64) {
        self.mirror_blobs_max_bytes.store(cap, Ordering::Relaxed);
    }

    /// Snapshot of all mirror-blob digests currently held.
    ///
    /// Legacy accessor: most callers should use
    /// [`Self::snapshot_and_reset_mirror_changes`] instead, which also
    /// drains the change tracker atomically. This is kept for tests and
    /// any future caller that genuinely wants a snapshot without
    /// touching deltas.
    // O(N) under lock — N is bounded by `mirror_blobs_max_bytes / blob_size`.
    pub fn mirror_blob_digests(&self) -> Vec<DigestInfo> {
        let guard = self.mirror_blobs.lock();
        guard.keys().copied().collect()
    }

    /// Atomically swap out and return the accumulated mirror-blob deltas
    /// since the last call. The internal state is replaced with empty sets.
    pub fn drain_mirror_changes(&self) -> MirrorChanges {
        let mut guard = self.mirror_changes.lock();
        core::mem::take(&mut *guard)
    }

    /// Atomic "drain deltas, then take full snapshot" used by the worker's
    /// full-snapshot path. Drain happens FIRST so that any concurrent
    /// `remove_mirror_blobs` racing the call cannot land between the two
    /// operations and lose its `removed` delta. Returns
    /// `(drained_changes, snapshot_digests)`. The drained `removed` set MUST
    /// be merged into `evicted_digests` on the wire to keep the locality map
    /// consistent.
    pub fn snapshot_and_reset_mirror_changes(
        &self,
    ) -> (MirrorChanges, Vec<DigestInfo>) {
        // Hold both locks across the swap+snapshot so neither an inserter
        // nor a remover can interleave and split a single change across
        // the boundary. The snapshot reflects exactly the post-drain state.
        //
        // Acquisition order: `mirror_blobs` BEFORE `mirror_changes` to match
        // the canonical order documented on the field declarations and used
        // by `insert_mirror_blob` / `remove_mirror_blobs`. Inverting the
        // order here would AB/BA-deadlock with concurrent inserters under
        // load (regression test:
        // `lock_ordering_no_deadlock_under_contention`).
        let blobs_guard = self.mirror_blobs.lock();
        let mut changes_guard = self.mirror_changes.lock();
        let drained = core::mem::take(&mut *changes_guard);
        let snapshot: Vec<DigestInfo> = blobs_guard.keys().copied().collect();
        drop(changes_guard);
        drop(blobs_guard);
        (drained, snapshot)
    }

    /// Wakes when mirror-blob inserts or removes happen. Used by the
    /// worker's BlobsAvailable loop.
    ///
    /// Single-consumer: only the BlobsAvailable loop awaits this. Adding a
    /// second consumer requires switching to `notify_waiters()` at the
    /// emit sites, otherwise one waiter would steal notifications from the
    /// other.
    pub fn mirror_changes_notify(&self) -> Arc<Notify> {
        self.mirror_changes_notify.clone()
    }

    /// Insert a mirror blob, updating bookkeeping (total bytes + change
    /// tracker). Returns `Err(Code::ResourceExhausted)` if the configured
    /// `MIRROR_BLOBS_MAX_BYTES` cap was exceeded. The Err propagates back
    /// through the mirror writer (`worker_proxy_store::mirror_blob_via_stream`)
    /// into `record_mirror_failure`, so locality/quarantine can route the
    /// next attempt to a different peer instead of the server believing
    /// the mirror succeeded.
    fn insert_mirror_blob(&self, digest: DigestInfo, data: Bytes) -> Result<(), Error> {
        let data_len = data.len() as u64;
        // LOAD-BEARING INVARIANT: mirror_blobs entries MUST satisfy
        // data.len() == digest.size_bytes() because the read path slices by
        // offset/length without re-validating, and serves Ok+EOF (no NotFound)
        // when offset >= data.len(). An entry with data.len() < digest.size_bytes()
        // for a non-zero digest produces a 0-byte successful gRPC stream that
        // the grpc_store.rs:1453 workaround was previously catching by inferring
        // "stale worker." With this guard the bug is observable here at the
        // source instead of propagating silently across the network.
        let expected = digest.size_bytes();
        if data_len != expected {
            warn!(
                %digest,
                data_len,
                expected_size = expected,
                "insert_mirror_blob: rejected — data size does not match digest \
                 (would create a phantom positive that produces 0-byte streams)"
            );
            return Err(make_err!(
                Code::Internal,
                "insert_mirror_blob: data.len()={data_len} != digest.size_bytes()={expected} \
                 for {digest}"
            ));
        }
        let now = Instant::now();
        // Single critical section across blobs + change tracker so an
        // intervening drain/snapshot cannot split the bookkeeping for one
        // logical insert.
        let mut blobs = self.mirror_blobs.lock();
        let current = self.mirror_blobs_total_bytes.load(Ordering::Relaxed);
        let cap = self.mirror_blobs_max_bytes.load(Ordering::Relaxed);
        if current + data_len > cap {
            drop(blobs);
            // warn (not debug) — silent drops here mean the cap is being
            // exercised under real load; we want this in operator logs.
            warn!(
                %digest,
                data_len,
                current_total = current,
                cap,
                "mirror blob dropped — memory cap exceeded; server will need to re-upload"
            );
            return Err(make_err!(
                Code::ResourceExhausted,
                "mirror blob {digest} dropped: memory cap {cap} exceeded \
                 (current_total={current}, blob_len={data_len})"
            ));
        }
        let mut changes = self.mirror_changes.lock();
        if let Some((old_data, _)) = blobs.insert(digest, (data, now)) {
            let old_len = old_data.len() as u64;
            if data_len >= old_len {
                self.mirror_blobs_total_bytes.fetch_add(data_len - old_len, Ordering::Relaxed);
            } else {
                self.mirror_blobs_total_bytes.fetch_sub(old_len - data_len, Ordering::Relaxed);
            }
        } else {
            self.mirror_blobs_total_bytes.fetch_add(data_len, Ordering::Relaxed);
        }
        // Record in change tracker (insert wins over a pending removal).
        changes.removed.remove(&digest);
        changes.added.insert(digest);
        drop(changes);
        drop(blobs);
        self.mirror_changes_notify.notify_one();
        Ok(())
    }

    /// Public insert_mirror_blob entry-point used by the worker's
    /// `Update::BatchWriteSmallBlobs` handler (Bug A small-CAS peer-mirror
    /// dispatcher, task #153). Carries the source `store_id` for future
    /// (store_id, digest) keying — currently logged but not part of the
    /// HashMap key (per plan B5 the BTreeMap refactor is a follow-up).
    ///
    /// Errors propagate from the underlying private `insert_mirror_blob`
    /// (cap-exceeded ⇒ `Code::ResourceExhausted`; size-mismatch ⇒
    /// `Code::Internal`). Caller logs and drops; the dispatcher's
    /// EphemeralServerSidePin TTL handles the unacked entry.
    pub fn insert_dispatched_mirror_blob(
        &self,
        store_id: &str,
        digest: DigestInfo,
        data: Bytes,
    ) -> Result<(), Error> {
        debug!(
            store_id,
            %digest,
            data_len = data.len(),
            "insert_dispatched_mirror_blob"
        );
        // Underlying `mirror_blobs` is keyed by `DigestInfo` (Plan B5
        // is parallel-indexed for now per `dispatched_mirror_pins` doc).
        // Multi-store collisions on the same digest are benign (CAS
        // contract: same digest ⇒ same bytes), so byte-storage
        // last-writer-wins on the HashMap is correct.
        self.insert_mirror_blob(digest, data)?;
        // task #168 item 5: record the (store_id, digest) so the
        // worker's BlobsAvailable loop can emit `pinned_mirror_entries`
        // (proto field 16) sorted by `store_id` ASCII (BTreeMap
        // iteration order). The index is consulted only for reporting;
        // get_part / has / eviction continue to key on `DigestInfo`.
        let key: Arc<str> = Arc::from(store_id);
        self.dispatched_mirror_pins.lock().insert((key, digest), ());
        Ok(())
    }

    /// Snapshot the dispatcher-pushed pin set as a sorted `Vec<(store_id,
    /// digest)>`. Returns BTreeMap iteration order (sorted by `store_id`
    /// ASCII first, then by `DigestInfo`). The worker's
    /// `BlobsAvailableNotification.pinned_mirror_entries` (proto field
    /// 16) is populated from this snapshot — sorted order is the
    /// precondition for the server's binary-search `self_filter` in
    /// `EphemeralServerSidePin::observe_pinned_mirror_ack`.
    ///
    /// Cheap when empty (BTreeMap::is_empty is O(1) → returns Vec::new
    /// without locking). Otherwise allocates one Vec — the snapshot is
    /// owned + Send so the worker can build the proto without holding
    /// the lock across `.await`.
    pub fn dispatched_mirror_pin_snapshot(&self) -> Vec<(Arc<str>, DigestInfo)> {
        let pins = self.dispatched_mirror_pins.lock();
        if pins.is_empty() {
            return Vec::new();
        }
        pins.iter().map(|(k, ())| k.clone()).collect()
    }

    /// Register a worker-local AC entry that has just been written to
    /// the AC FastSlowStore's fast tier (and is in flight to the slow
    /// tier). Unlike CAS mirroring (`insert_dispatched_mirror_blob`),
    /// AC writes originate ON the worker — the worker IS the producer
    /// of the bytes, so we don't need to store the data in
    /// `mirror_blobs` (no peer push will ever be served from there).
    /// We only need to record the `(store_id, digest)` tuple in
    /// `dispatched_mirror_pins` so the worker's next `BlobsAvailable`
    /// tick advertises the AC entry to the server via a SEPARATE proto
    /// field (`pinned_ac_mirror_entries: 17`).
    ///
    /// **Hard partition vs. CAS pins.** This entry is reported to the
    /// server on the AC-only proto channel; the server inserts it into
    /// the AC pin registry, NOT the CAS-shared `BlobLocalityMap`. AC
    /// pins MUST NOT route through the locality map because
    /// `action_digest` is by REAPI design the same digest as the
    /// Action proto in CAS — registering AC pins against the locality
    /// map would weaponize the upload-skip short-circuits in
    /// `bytestream_server::write` and `cas_server::batch_update_blobs`,
    /// silently losing Action proto bytes.
    ///
    /// **Read-side wiring lands in a follow-up commit.** Today this
    /// method's value is establishing the wire channel; the AC peer-
    /// fetch path is not yet wired. With no consumer today,
    /// MemoryStore-eviction-vs-pin divergence is dormant — it becomes
    /// load-bearing the moment a reader lands. At that point the
    /// eviction-callback drain MUST be added so pin state matches
    /// fast-tier residency.
    /// TODO: AC peer-fetch + pin-eviction interlock.
    pub fn insert_local_ac_pin(&self, store_id: &str, digest: DigestInfo) {
        debug!(store_id, %digest, "insert_local_ac_pin");
        let key: Arc<str> = Arc::from(store_id);
        self.dispatched_mirror_pins.lock().insert((key, digest), ());
        // Wake the worker's BlobsAvailable loop so the entry advertises
        // promptly (mirrors the wake from `insert_dispatched_mirror_blob`
        // via `insert_mirror_blob`'s `mirror_changes_notify.notify_one()`).
        self.mirror_changes_notify.notify_one();
    }

    /// Remove worker-local AC pin entries for the supplied digests
    /// across all `store_id`s. Called from the `BlobsInStableStorage`
    /// handler when the server confirms the AC entry has been
    /// persisted. Unlike [`Self::remove_mirror_blobs`] this does NOT
    /// touch `mirror_blobs` or `mirror_blobs_total_bytes` — AC pins
    /// never lived there.
    pub fn remove_local_ac_pins(&self, digests: &[DigestInfo]) {
        self.retain_pins_dropping(digests);
    }

    /// Snapshot just the AC pin entries (those whose store_id matches
    /// `ac_store_id`) as a sorted `Vec<DigestInfo>`. Used by
    /// `send_periodic_blobs_available` to populate the dedicated
    /// `pinned_ac_mirror_entries: 17` field — keeping the AC slice
    /// cleanly partitioned from the CAS pin slice on the wire so the
    /// server's CAS `register_blobs(...)` call can NEVER see an AC
    /// entry by accident.
    pub fn dispatched_ac_pin_snapshot_for_store(
        &self,
        ac_store_id: &str,
    ) -> Vec<DigestInfo> {
        let pins = self.dispatched_mirror_pins.lock();
        if pins.is_empty() {
            return Vec::new();
        }
        pins.iter()
            .filter_map(|((sid, d), ())| (sid.as_ref() == ac_store_id).then_some(*d))
            .collect()
    }

    /// Default per-blob streaming buffer: 64 MiB sliding window.
    const POPULATE_STREAM_BUFFER_BYTES: u64 = 64 * 1024 * 1024;

    fn get_loader(&self, key: StoreKey<'_>) -> LoaderGuard {
        // Get a single loader instance that's used to populate the fast store
        // for this digest.  If another request comes in then it's de-duplicated.
        // Pre-compute owned keys outside the lock to minimize lock hold time.
        // One for the hashmap key, one to keep inside the LoaderGuard.
        let owned_key = key.borrow().into_owned();
        let key_for_guard = owned_key.clone();
        let digest = match key.borrow() {
            StoreKey::Digest(d) => d,
            _ => DigestInfo::zero_digest(),
        };
        // Use `get` first so the Occupied (waiter) hot path avoids the
        // extra clone the `entry()` API would force. The Vacant (populator)
        // path still inserts once. The Loader is `Arc<()>` (vestige from
        // 01b68015's spawn-detach refactor — no OnceCell needed).
        let mut guard = self.populating_digests.lock();
        let (loader, streaming_inner, is_new) =
            if let Some((l, s)) = guard.get(&owned_key) {
                (l.clone(), s.clone(), false)
            } else {
                let inner = Arc::new(StreamingBlobInner::new(
                    digest,
                    Self::POPULATE_STREAM_BUFFER_BYTES,
                ));
                let loader: Loader = Arc::new(());
                guard.insert(
                    owned_key,
                    (Arc::clone(&loader), Arc::clone(&inner)),
                );
                (loader, inner, true)
            };
        drop(guard);
        LoaderGuard {
            weak_store: self.weak_self.clone(),
            key: key_for_guard,
            loader: Some(loader),
            streaming_inner,
            is_new,
        }
    }

    /// Producer body: drives the slow→fast copy and fans data out to the
    /// streaming buffer. Returns the producer's merged terminal status so
    /// the inline caller in [`copy_slow_to_fast`] can propagate it
    /// directly without re-reading the streaming buffer; the spawn-detach
    /// path in [`spawn_populate_producer_with_role`] simply discards it.
    ///
    /// Always terminates the streaming buffer before returning:
    /// - On success: `send_eof` after both `data_stream_fut` and
    ///   `fast_store.update` complete.
    /// - On error: `send_error` with the structured upstream error.
    /// The `StreamingBlobWriter::Drop` fallback ("writer dropped without
    /// sending EOF") remains as a safety net only — it should never fire
    /// from successful normal-return paths from this function.
    ///
    /// Cancel-safety: if the awaiting future is dropped mid-flight, the
    /// streaming buffer terminates via `StreamingBlobWriter::Drop` with
    /// the generic "writer dropped without sending EOF" Internal error.
    /// Callers that cannot tolerate this (e.g., `get_part` which serves
    /// gRPC streaming reads) MUST drive this future from a `tokio::spawn`
    /// so caller cancellation does not propagate. Callers that bound the
    /// producer's lifetime to themselves (`copy_slow_to_fast` from action
    /// downloads) accept this trade in exchange for skipping the spawn.
    async fn run_producer(
        arc_self: Arc<Self>,
        loader_guard: LoaderGuard,
        mut streaming_writer: StreamingBlobWriter,
    ) -> Result<(), Error> {
        // The guard's Drop removes the populating_digests entry when this
        // function returns (by panic or normal completion). Holding it for
        // the producer's full lifetime is what allows late-arriving
        // waiters to find this populate in the map and join in.
        let key = loader_guard.key.borrow();
        let producer_start = Instant::now();
        debug!(
            %key,
            slow_store = %arc_self.slow_store.inner_store(Some(key.borrow())).get_name(),
            "populate run_producer entry",
        );
        // Tracks whether `slow_store.has()` was actually called AND
        // returned `Some(_)`. The PHANTOM BLOB invariant only applies
        // to that case — the LazyExistenceOnSync branch short-circuits
        // to `Ok(MaxSize(u64::MAX))` WITHOUT calling has(), so a
        // subsequent NotFound from the populate path is a normal miss,
        // not a has-said-Some-but-blob-vanished race. (Investigator
        // ae69e88918d055f5e: 178 false-alarm warns / 10 min on
        // production workers were all from this conflation.)
        let mut has_actually_returned_some = false;
        let head_result: Result<UploadSizeInfo, Error> = async {
            // failpoint: simulate slow store being unavailable during populate.
            // exercises the error propagation path when the slow store cannot
            // be read during a cache-miss populate operation.
            #[cfg(feature = "failpoints")]
            fail::fail_point!("fast_slow_populate_slow_store_unavailable", |_| {
                Err(make_err!(
                    Code::Unavailable,
                    "failpoint: slow store unavailable during populate"
                ))
            });

            if arc_self
                .slow_store
                .inner_store(Some(key.borrow()))
                .optimized_for(StoreOptimizations::LazyExistenceOnSync)
            {
                trace!(
                    %key,
                    store_name = %arc_self.slow_store.inner_store(Some(key.borrow())).get_name(),
                    "Skipping .has() check due to LazyExistenceOnSync optimization"
                );
                Ok(UploadSizeInfo::MaxSize(u64::MAX))
            } else {
                let size = arc_self
                    .slow_store
                    .has(key.borrow())
                    .await
                    .err_tip(|| "Failed to run has() on slow store")?
                    .ok_or_else(|| {
                        debug!(
                            %key,
                            slow_store = %arc_self.slow_store.inner_store(Some(key.borrow())).get_name(),
                            "CAS read miss: blob not found in slow store"
                        );
                        let digest = key.borrow().into_digest();
                        Error::not_found_with_detail(
                            format!(
                                "Object {} not found in either fast or slow store. \
                                If using multiple workers, ensure all workers share the same CAS storage path.",
                                key.as_str(),
                            ),
                            make_precondition_failure_any(digest),
                        )
                    })?;
                // Only set on the true has-then-Some path so the
                // PHANTOM BLOB warn fires only on the actual invariant
                // violation (has=Some, populate=NotFound).
                has_actually_returned_some = true;
                Ok(UploadSizeInfo::ExactSize(size))
            }
        }
        .await;
        let head_elapsed_ms = producer_start.elapsed().as_millis() as u64;
        match &head_result {
            Ok(size) => debug!(
                %key,
                head_elapsed_ms,
                ?size,
                "populate head_result Ok",
            ),
            Err(err) => debug!(
                %key,
                head_elapsed_ms,
                code = ?err.code,
                "populate head_result Err",
            ),
        }
        let reader_stream_size = match head_result {
            Ok(size) => size,
            Err(err) => {
                let returned = err.clone();
                let elapsed_ms = producer_start.elapsed().as_millis() as u64;
                debug!(
                    %key,
                    elapsed_ms,
                    code = ?returned.code,
                    "populate sending streaming_writer.send_error after head failure",
                );
                streaming_writer.send_error(err);
                return Err(returned);
            }
        };

        let mut counted_hit = false;

        // Use 128 slots (~32MiB at 256KiB chunks) for dual-store
        // read-through to reduce backpressure between fast and slow stores.
        let (mut fast_tx, fast_rx) = make_buf_channel_pair_with_size(128);
        let (slow_tx, mut slow_rx) = make_buf_channel_pair_with_size(128);
        // The data-stream loop runs until slow_rx EOFs or errors. The
        // future MUST own `fast_tx` so it is dropped (releasing fast_rx)
        // when the future completes — `tokio::join!` drops completed
        // branches via `MaybeDone::set` immediately, freeing held
        // resources.
        //
        // `streaming_writer` is returned in BOTH branches so the outer
        // code can terminate the buffer AFTER the join! completes — i.e.
        // only after `fast_store.update` finishes. This way
        // `is_terminal=true` on the buffer means the fast store is
        // fully populated, which is what `copy_slow_to_fast` waiters
        // rely on. The safety-net Drop on the writer only fires if
        // neither send_eof nor send_error was called — which can no
        // longer happen here because the outer match unconditionally
        // calls one of them.
        //
        // Clone arc_self for the data_stream_fut closure so the
        // original remains usable for slow_store_fut / fast_store_fut.
        let arc_for_stream = Arc::clone(&arc_self);
        let key_for_stream = key.borrow().into_owned();
        let key_for_slow = key.borrow().into_owned();
        let key_for_fast = key.borrow().into_owned();
        let data_stream_fut = async move {
            let stream_start = Instant::now();
            debug!(
                key = %key_for_stream,
                "populate data_stream branch entry",
            );
            let mut first_chunk_ms: Option<u64> = None;
            let mut chunks: u64 = 0;
            let mut total_bytes: u64 = 0;
            // Inner block returns the data-stream result. The outer
            // unconditionally repackages the writer back so the caller
            // can terminate the buffer AFTER the join! completes.
            let result: Result<Result<(), Error>, Error> = async {
                loop {
                    let output_buf = slow_rx
                        .recv()
                        .await
                        .err_tip(|| "Failed to read data buffer from slow store")?;
                    if output_buf.is_empty() {
                        return Ok(fast_tx.send_eof());
                    }
                    if first_chunk_ms.is_none() {
                        first_chunk_ms = Some(stream_start.elapsed().as_millis() as u64);
                    }
                    chunks += 1;
                    total_bytes += output_buf.len() as u64;

                    if !counted_hit {
                        arc_for_stream
                            .metrics
                            .slow_store_hit_count
                            .fetch_add(1, Ordering::Acquire);
                        counted_hit = true;
                    }

                    let output_buf_len = u64::try_from(output_buf.len())
                        .err_tip(|| "Could not output_buf.len() to u64")?;
                    arc_for_stream
                        .metrics
                        .slow_store_downloaded_bytes
                        .fetch_add(output_buf_len, Ordering::Acquire);

                    // Push into the streaming buffer for waiters. The
                    // only failure mode is `is_terminal()` already set
                    // (e.g. an out-of-band cancel-poison) — readers have
                    // already moved on, so we ignore the result and keep
                    // pushing into `fast_tx` to populate the fast store.
                    // `send()` itself does not wait on any waiter; waiter
                    // backpressure is handled by the sliding-window
                    // eviction inside `StreamingBlobWriter::send`.
                    let _send_res = streaming_writer.send(output_buf.clone()).await;

                    fast_tx
                        .send(output_buf)
                        .await
                        .err_tip(|| "Failed to write to fast store in fast_slow store")?;
                }
            }
            .await;
            let elapsed_ms = stream_start.elapsed().as_millis() as u64;
            match &result {
                Ok(_) => debug!(
                    key = %key_for_stream,
                    elapsed_ms,
                    first_chunk_ms = ?first_chunk_ms,
                    chunks,
                    total_bytes,
                    "populate data_stream branch Ok",
                ),
                Err(err) => debug!(
                    key = %key_for_stream,
                    elapsed_ms,
                    first_chunk_ms = ?first_chunk_ms,
                    chunks,
                    total_bytes,
                    code = ?err.code,
                    "populate data_stream branch Err",
                ),
            }
            // Crucial: drop fast_tx BEFORE returning so fast_rx (driving
            // fast_store.update) sees the channel close. Without this,
            // the join! deadlocks on an error path because fast_tx
            // remains alive in the closure's captures even though the
            // logical loop has returned.
            //
            // tokio::join! drops completed branches via MaybeDone::set,
            // which would drop fast_tx — but only when ALL of the
            // returned tuple is consumed. By dropping inside the
            // closure, we guarantee timely release for the unhappy path.
            drop(fast_tx);
            (streaming_writer, result)
        };

        let slow_store_fut = {
            let arc_for_slow = Arc::clone(&arc_self);
            async move {
                let t0 = Instant::now();
                debug!(
                    key = %key_for_slow,
                    "populate slow_store.get branch entry",
                );
                let res = arc_for_slow.slow_store.get(key_for_slow.borrow(), slow_tx).await;
                let elapsed_ms = t0.elapsed().as_millis() as u64;
                match &res {
                    Ok(()) => debug!(
                        key = %key_for_slow,
                        elapsed_ms,
                        "populate slow_store.get branch Ok",
                    ),
                    Err(err) => debug!(
                        key = %key_for_slow,
                        elapsed_ms,
                        code = ?err.code,
                        "populate slow_store.get branch Err",
                    ),
                }
                res
            }
        };
        let fast_store_fut = {
            let arc_for_fast = Arc::clone(&arc_self);
            async move {
                let t0 = Instant::now();
                debug!(
                    key = %key_for_fast,
                    "populate fast_store.update branch entry",
                );
                let res = arc_for_fast
                    .fast_store
                    .update(key_for_fast.borrow(), fast_rx, reader_stream_size)
                    .await;
                let elapsed_ms = t0.elapsed().as_millis() as u64;
                match &res {
                    Ok(()) => debug!(
                        key = %key_for_fast,
                        elapsed_ms,
                        "populate fast_store.update branch Ok",
                    ),
                    Err(err) => debug!(
                        key = %key_for_fast,
                        elapsed_ms,
                        code = ?err.code,
                        "populate fast_store.update branch Err",
                    ),
                }
                res
            }
        };

        let ((mut writer_back, data_stream_res), slow_res, fast_res) =
            join!(data_stream_fut, slow_store_fut, fast_store_fut);
        let join_elapsed_ms = producer_start.elapsed().as_millis() as u64;
        debug!(
            %key,
            join_elapsed_ms,
            "populate join3 returned",
        );

        // Compose the producer's terminal status. NotFound from the
        // slow store wins (matches prior behavior); else any failure is
        // reported via the merged error.
        let merged: Result<(), Error> = match data_stream_res {
            Ok(fast_eof_res) => fast_eof_res.merge(fast_res).merge(slow_res),
            Err(err) => match slow_res {
                Err(slow_err) if slow_err.code == Code::NotFound => Err(slow_err),
                _ => fast_res.merge(slow_res).merge(Err(err)),
            },
        };
        // Phantom-blob signal: slow_store.has() said the blob was present,
        // but the populate path (data stream / slow_store.get) reported
        // NotFound. This indicates a race or corruption between has() and
        // get() — the blob disappeared from the slow store between checks.
        // Gated on `has_actually_returned_some` (NOT `head_result.is_ok()`)
        // because the LazyExistenceOnSync branch above returns Ok WITHOUT
        // calling has(), and a downstream NotFound there is a normal miss,
        // not a phantom. Demoted to `warn!` because the genuine has-evict
        // race self-heals (the locality entry is dropped on the next
        // try_read failure).
        if has_actually_returned_some {
            if let Err(err) = &merged {
                if err.code == Code::NotFound {
                    warn!(
                        %key,
                        slow_store = %arc_self.slow_store.inner_store(Some(key.borrow())).get_name(),
                        ?err,
                        "PHANTOM BLOB: slow_store.has() returned Some, but populate path returned NotFound"
                    );
                }
            }
        }
        let returned = match &merged {
            Ok(()) => Ok(()),
            Err(err) => Err(err.clone()),
        };
        match merged {
            Ok(()) => {
                let elapsed_ms = producer_start.elapsed().as_millis() as u64;
                debug!(
                    %key,
                    elapsed_ms,
                    "populate calling streaming_writer.send_eof",
                );
                // Ignore the Result from send_eof: it only errors if a
                // terminal state was already set (e.g. via a panic
                // during streaming send), which we are content to leave
                // in place. The buffer is terminal either way.
                drop(writer_back.send_eof());
            }
            Err(err) => {
                let elapsed_ms = producer_start.elapsed().as_millis() as u64;
                debug!(
                    %key,
                    elapsed_ms,
                    code = ?err.code,
                    "populate calling streaming_writer.send_error",
                );
                writer_back.send_error(err);
            }
        }
        let total_elapsed_ms = producer_start.elapsed().as_millis() as u64;
        match &returned {
            Ok(()) => debug!(
                %key,
                total_elapsed_ms,
                "populate run_producer exit Ok",
            ),
            Err(err) => warn!(
                %key,
                total_elapsed_ms,
                code = ?err.code,
                "populate run_producer exit failed — slow-store populate did not \
                 deliver the blob to the fast tier",
            ),
        }
        // writer_back drops here — terminal state is already set via
        // send_eof or send_error above, so the safety-net Drop is a no-op.
        // loader_guard drops here, removing the populating_digests entry
        // (subject to the strong-count check in LoaderGuard::Drop).
        returned
    }

    /// Drain the streaming buffer until terminal state, discarding chunks.
    /// Used by callers that need to wait for the producer to finish but
    /// do not consume the data themselves (e.g. `copy_slow_to_fast`).
    /// Returns the producer's terminal result.
    ///
    /// Terminal state is the source of truth: the outer loop checks
    /// `terminal_result()` BEFORE creating a reader. If the producer
    /// finished, we return its actual outcome regardless of how much
    /// sliding-window data still happens to be readable — buffered
    /// chunks alone do NOT prove the producer succeeded (nit #6 from
    /// `01b68015`'s code review).
    ///
    /// `Code::Unavailable` from the inner reader means our cursor fell
    /// behind the sliding window (the blob exceeds
    /// `POPULATE_STREAM_BUFFER_BYTES` and the producer outpaced our
    /// drain). Break out and let the outer loop re-check terminal state
    /// or recreate the reader at the new earliest cursor.
    ///
    /// `#[doc(hidden)] pub` so the regression tests
    /// `drain_streaming_buffer_propagates_terminal_error_over_buffered_data`
    /// and `drain_streaming_buffer_eviction_race_propagates_error`
    /// can drive the function directly with a hand-built
    /// `StreamingBlobInner`. Internal helper otherwise.
    #[doc(hidden)]
    pub async fn drain_streaming_buffer(
        streaming_inner: &Arc<StreamingBlobInner>,
    ) -> Result<(), Error> {
        loop {
            // Producer's terminal state wins over buffered chunks. This
            // both eliminates the prior buggy "data means success"
            // recovery branch and short-circuits the common case where
            // the producer already finished by the time the drain runs.
            if let Some(terminal) = streaming_inner.terminal_result() {
                return terminal;
            }
            let mut reader = nativelink_util::streaming_blob::StreamingBlobReader::new(
                streaming_inner.clone(),
            );
            loop {
                match reader.next_chunk().await {
                    Ok(c) if c.is_empty() => return Ok(()),
                    Ok(_) => {}
                    Err(err) if err.code == Code::Unavailable => {
                        // Cursor fell behind the sliding window. Bounce
                        // back to the outer loop, which re-checks
                        // terminal first (no second-class data path).
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    /// Internal helper: copy a blob from the slow store into the fast store,
    /// using the de-duplicating loader. Assumes the caller has already verified
    /// the blob is not in the fast store (or does not care).
    ///
    /// Inline-fast-path optimisation: when this caller acquires the loader
    /// as the populator (`is_new=true`), the producer runs **inline** in
    /// the caller's task — no `tokio::spawn`, no streaming-buffer drain.
    /// The producer's terminal `Result` is returned directly, eliminating
    /// the per-populate spawn (~100 ns) and the per-chunk channel hop
    /// through the streaming buffer (~500 ns/chunk). Waiters that joined
    /// late still go through the streaming buffer as before.
    ///
    /// Cancel-safety trade: if the requester's future is dropped mid-
    /// populate, the inline producer dies with it; the streaming buffer
    /// terminates via `StreamingBlobWriter::Drop` with the generic
    /// "writer dropped without sending EOF" Internal error, and any
    /// late-arriving waiters fall back to the slow store with that error
    /// surfaced through the existing waiter recovery path. This is
    /// acceptable for `copy_slow_to_fast` callers (action-bound
    /// `populate_fast_store{,_unchecked}` from the worker) which rarely
    /// cancel mid-populate. The cancellation-prone `get_part` path keeps
    /// `spawn_populate_producer_with_role`'s spawn-detach for full
    /// cancellation safety (covered by `populate_survives_caller_cancellation`).
    async fn copy_slow_to_fast(&self, key: StoreKey<'_>) -> Result<(), Error> {
        // If the fast store is noop or read only or update only then this is an error.
        if self
            .fast_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.fast_direction == StoreDirection::ReadOnly
            || self.fast_direction == StoreDirection::Update
        {
            return Err(make_err!(
                Code::Internal,
                "Attempt to populate fast store that is read only or noop"
            ));
        }

        let arc_self = self.get_arc().ok_or_else(|| {
            make_err!(
                Code::Internal,
                "FastSlowStore dropped during populate"
            )
        })?;
        let loader_guard = arc_self.get_loader(key.borrow());
        let streaming_inner = Arc::clone(&loader_guard.streaming_inner);
        if loader_guard.is_new {
            // Inline fast path: we own the populator. Run the producer
            // in this task — no spawn, no streaming-buffer drain — and
            // return its terminal Result directly. Cancellation drops
            // the producer (see method-level Cancel-safety note).
            //
            // Construct the writer immediately; `LoaderGuard` is moved
            // into `run_producer` and ensures the populating_digests
            // entry is cleared on producer return / drop.
            let writer = StreamingBlobWriter::new(streaming_inner);
            Self::run_producer(arc_self, loader_guard, writer)
                .await
                .err_tip(|| "Failed to populate()")
        } else {
            // Late waiter: another caller is already running the
            // producer (either inline in their own task or spawn-
            // detached via `get_part`). Drop our guard and observe the
            // producer via the streaming buffer, the same way pre-fix
            // waiters did. Data is discarded — we care only that the
            // producer reaches a terminal state.
            drop(loader_guard);
            Self::drain_streaming_buffer(&streaming_inner)
                .await
                .err_tip(|| "Failed to populate()")
        }
    }

    /// Spawn the populator for `key` if not already running, returning
    /// the shared streaming buffer plus a flag indicating whether THIS
    /// caller is the populator (`is_new=true` at loader-acquisition).
    /// Idempotent: subsequent callers for the same key share the buffer
    /// and do not re-spawn.
    ///
    /// The producer runs detached on its own tokio task — cancellation
    /// of the caller does not cancel the producer. This is the
    /// cancellation-safe path used by `get_part` for streaming gRPC
    /// reads. The cheaper inline-fast-path in [`copy_slow_to_fast`] is
    /// preferred when the caller can bound the producer's lifetime to
    /// itself.
    ///
    /// Each spawn bumps `populate_spawn_count` so the inline-fast-path
    /// optimisation can be regression-tested via that counter.
    fn spawn_populate_producer_with_role(
        arc_self: Arc<Self>,
        key: StoreKey<'_>,
    ) -> (Arc<StreamingBlobInner>, bool) {
        // CRITICAL ORDERING: we hold `populating_digests` ACROSS the
        // `tokio::spawn` so the producer task is enqueued BEFORE the entry
        // is publicly observable. Without this, a concurrent caller could
        // (a) acquire the lock between our `insert` and our `spawn`,
        // (b) find the entry, clone the streaming_inner, drop the lock,
        // (c) attach a reader and start awaiting on the streaming buffer,
        // (d) all before the producer task even hits the runqueue.
        // Under runtime contention the producer might then sit unscheduled
        // for >60s (the reader's notify deadline), surfacing as a wedge
        // with `producer_task=<none>` because the OnceLock for
        // producer_task_id is set on first send. tokio::spawn is sync
        // (just enqueues), so holding the parking_lot Mutex across it is
        // a brief critical-section extension, not a hazard.
        //
        // copy_slow_to_fast (the inline-producer caller) is unaffected
        // because it runs the producer in the same task synchronously
        // after get_loader returns; there's no spawn-then-publish gap.
        let owned_key = key.borrow().into_owned();
        let key_for_guard = owned_key.clone();
        let digest = match key.borrow() {
            StoreKey::Digest(d) => d,
            _ => DigestInfo::zero_digest(),
        };
        let mut guard = arc_self.populating_digests.lock();
        if let Some((_l, s)) = guard.get(&owned_key) {
            // Late-arriving waiter: another caller already inserted the
            // entry AND (by virtue of this same lock) already spawned the
            // producer. Safe to drop the lock and observe the producer
            // via the returned streaming_inner.
            let streaming_inner = Arc::clone(s);
            drop(guard);
            return (streaming_inner, false);
        }
        // First caller: build inner + writer + LoaderGuard, spawn the
        // producer, then publish the entry — all under the lock.
        let inner = Arc::new(StreamingBlobInner::new(
            digest,
            Self::POPULATE_STREAM_BUFFER_BYTES,
        ));
        let loader: Loader = Arc::new(());
        let writer = StreamingBlobWriter::new(Arc::clone(&inner));
        let arc_for_producer = Arc::clone(&arc_self);
        let loader_guard = LoaderGuard {
            weak_store: arc_self.weak_self.clone(),
            key: key_for_guard,
            loader: Some(Arc::clone(&loader)),
            streaming_inner: Arc::clone(&inner),
            is_new: true,
        };
        arc_self
            .metrics
            .populate_spawn_count
            .fetch_add(1, Ordering::Release);
        // The JoinHandle is intentionally dropped — the producer is
        // detached and runs to completion regardless of caller lifetime.
        // The producer's terminal Result is discarded here; waiters
        // observe terminal state via the streaming buffer.
        drop(tokio::spawn(async move {
            drop(Self::run_producer(arc_for_producer, loader_guard, writer).await);
        }));
        guard.insert(owned_key, (loader, Arc::clone(&inner)));
        drop(guard);
        (inner, true)
    }

    /// If `key` is currently held in the in-memory `mirror_blobs` map,
    /// write its bytes into the fast store and return `true`. Otherwise
    /// return `false` (and the caller falls back to the slow-store
    /// populate path). This is what makes `populate_fast_store_*` and
    /// the directory-cache hardlink path work correctly when the only
    /// surviving copy of a blob is in `mirror_blobs` — without it, a
    /// mirror-only blob would be re-fetched from the slow store and, if
    /// the server is the slow store and is down or has lost the blob,
    /// the populate would fail with NotFound even though the worker
    /// holds the bytes in memory.
    async fn materialize_mirror_to_fast(
        &self,
        key: StoreKey<'_>,
    ) -> Result<bool, Error> {
        let digest = key.borrow().into_digest();
        let maybe_data = self
            .mirror_blobs
            .lock()
            .get(&digest)
            .map(|(d, _)| d.clone());
        let Some(data) = maybe_data else {
            return Ok(false);
        };
        // Write directly to fast_store via the standard update path.
        // `update_oneshot` is a single-buffer write — no streaming
        // required since the bytes are already in RAM.
        self.fast_store
            .update_oneshot(digest, data)
            .await
            .err_tip(|| {
                "materialize_mirror_to_fast: writing in-memory mirror blob to fast store"
            })?;
        Ok(true)
    }

    /// Ensure our fast store is populated. This should be kept as a low
    /// cost function. Since the data itself is shared and not copied it should be fairly
    /// low cost to just discard the data, but does cost a few mutex locks while
    /// streaming.
    pub async fn populate_fast_store(&self, key: StoreKey<'_>) -> Result<(), Error> {
        let maybe_size_info = self
            .fast_store
            .has(key.borrow())
            .await
            .err_tip(|| "While querying in populate_fast_store")?;
        if maybe_size_info.is_some() {
            return Ok(());
        }

        // If we hold a mirror copy in memory, materialize from there
        // instead of round-tripping the slow store. This is the
        // server-restart-resilience path: a mirror-only blob's bytes
        // live nowhere else.
        if self.materialize_mirror_to_fast(key.borrow()).await? {
            return Ok(());
        }

        self.copy_slow_to_fast(key).await
    }

    /// Like [`populate_fast_store`](Self::populate_fast_store) but skips the
    /// `has()` check on the fast store. Use this when the caller has already
    /// verified that the blob is missing from the fast store (e.g. via a prior
    /// batch `has_with_results` call) to avoid a redundant existence check.
    ///
    /// Verifies the blob is present after `copy_slow_to_fast` returns Ok and
    /// retries once if not. This guards against the FilesystemStore's
    /// silent-Ok-on-eviction race: when many parallel populates burst against
    /// a near-full fast store, an emplace can be evicted before its rename
    /// completes, the underlying `emplace_file` returns Ok (the data IS in
    /// the cache via a replacement OR is gone via eviction — the caller can't
    /// tell), and a downstream `get_file_entry_for_digest` then fails with
    /// NotFound. The post-write `has()` distinguishes the two cases without
    /// changing the underlying contract.
    /// Verify that `key` is present on `fast_store`. Wrapped in a helper so
    /// the post-copy verify in [`populate_fast_store_unchecked`] can be
    /// deterministically forced to report `Ok(false)` from a failpoint
    /// without faking the underlying store's behavior. Without the
    /// failpoint compiled in this is just a single `has()` call.
    async fn verify_present_with_failpoint(
        fast_store: &Store,
        key: StoreKey<'_>,
        _failpoint_name: &'static str,
        err_tip: &'static str,
    ) -> Result<bool, Error> {
        #[cfg(feature = "failpoints")]
        fail::fail_point!(_failpoint_name, |_| { Ok(false) });
        Ok(fast_store
            .has(key)
            .await
            .err_tip(|| err_tip)?
            .is_some())
    }

    pub async fn populate_fast_store_unchecked(&self, key: StoreKey<'_>) -> Result<(), Error> {
        // If we hold a mirror copy in memory, materialize from there
        // instead of round-tripping the slow store. Mirror-only blobs
        // that live nowhere else (server lost the blob, or has not yet
        // accepted the upload) MUST resolve via this path or the worker
        // would re-fetch from the slow store and fail.
        match self.materialize_mirror_to_fast(key.borrow()).await {
            Ok(true) => {
                // Verify it actually landed (same eviction-race guard
                // as the slow-store path). If not present, the next
                // copy_slow_to_fast attempt is the natural retry.
                if Self::verify_present_with_failpoint(
                    &self.fast_store,
                    key.borrow(),
                    "fast_slow_populate_unchecked_force_evict_first",
                    "populate_fast_store_unchecked: mirror-materialize verify",
                )
                .await?
                {
                    return Ok(());
                }
                warn!(
                    %key,
                    "populate_fast_store_unchecked: mirror-materialized blob evicted before verify; falling back to slow store",
                );
            }
            Ok(false) => {} // No mirror copy; fall through to slow store.
            Err(err) => {
                warn!(
                    %key,
                    ?err,
                    "populate_fast_store_unchecked: mirror-materialize failed; falling back to slow store",
                );
            }
        }
        if let Err(err) = self.copy_slow_to_fast(key.borrow()).await {
            error!(
                %key,
                ?err,
                "populate_fast_store_unchecked: copy_slow_to_fast failed",
            );
            return Err(err);
        }
        // Confirm the blob actually landed. has() on the fast store is a
        // single hashmap lookup on the EvictingMap — sub-microsecond.
        if Self::verify_present_with_failpoint(
            &self.fast_store,
            key.borrow(),
            "fast_slow_populate_unchecked_force_evict_first",
            "populate_fast_store_unchecked: post-write verify",
        )
        .await?
        {
            return Ok(());
        }
        warn!(
            %key,
            "populate_fast_store_unchecked: blob evicted between copy and verify, retrying once",
        );
        if let Err(err) = self.copy_slow_to_fast(key.borrow()).await {
            error!(
                %key,
                ?err,
                "populate_fast_store_unchecked: copy_slow_to_fast retry failed",
            );
            return Err(err);
        }
        if Self::verify_present_with_failpoint(
            &self.fast_store,
            key.borrow(),
            "fast_slow_populate_unchecked_force_evict_second",
            "populate_fast_store_unchecked: retry verify",
        )
        .await?
        {
            return Ok(());
        }
        // Note: a single retry is intentional. Under sustained over-pressure
        // (in-flight working set > free fast-store capacity), a second retry
        // sees the same cache state and won't help. The Aborted return is
        // the signal that the caller is over-batching — fix at that layer
        // (bound by in-flight bytes, pre-evict, or pin the batch) rather
        // than retrying harder here.
        error!(
            %key,
            "populate_fast_store_unchecked: blob not present after copy + retry — over-pressure or upstream lost the blob",
        );
        Err(make_err!(
            Code::Aborted,
            "populate_fast_store_unchecked: blob {key} not present after copy + retry; fast store is over-pressured for the in-flight populate batch",
        ))
    }

    /// Stream a file's contents to a store via a buf_channel, reading from
    /// an independently opened file descriptor. Used by the parallel
    /// `update_with_whole_file` path to feed data to the store that does
    /// NOT receive the file handle (the other store gets the file for its
    /// move/hardlink optimization). Unlike the previous `read_file_to_vec`
    /// approach, this streams chunks directly without buffering the entire
    /// file in memory.
    ///
    /// The `path` must point to the file to read. A new fd is opened from
    /// the path to avoid sharing seek position with the original FileSlot
    /// (try_clone shares the kernel file description, causing races).
    async fn stream_path_to_store(
        path: std::path::PathBuf,
        store: &Store,
        key: StoreKey<'_>,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let (mut tx, rx) = make_buf_channel_pair_with_size(128);
        let write_fut = store.update(key.borrow(), rx, upload_size);

        // Read in 256 KiB chunks — true streaming via an mpsc bridge.
        // The blocking reader sends one chunk at a time through the bridge
        // channel; blocking_send() applies backpressure so only a few
        // chunks are in memory at once (channel capacity = 4).
        const CHUNK_SIZE: usize = 256 * 1024;
        let (bridge_tx, mut bridge_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, Error>>(4);

        let read_handle = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut file = match std::fs::File::open(&path) {
                Ok(fd) => fd,
                Err(e) => {
                    let _ = bridge_tx.blocking_send(Err(make_err!(
                        Code::Internal,
                        "Failed to open file for streaming: {:?}",
                        e
                    )));
                    return;
                }
            };
            loop {
                let mut buf = vec![0u8; CHUNK_SIZE];
                let mut filled = 0;
                // Fill the buffer completely (or until EOF) to avoid
                // sending many tiny trailing chunks.
                while filled < CHUNK_SIZE {
                    match file.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                            continue
                        }
                        Err(e) => {
                            let err = make_err!(
                                Code::Internal,
                                "Failed to read file in stream_path_to_store: {:?}",
                                e
                            );
                            // Best-effort send of error; receiver may be gone.
                            let _ = bridge_tx.blocking_send(Err(err));
                            return;
                        }
                    }
                }
                if filled == 0 {
                    break; // EOF — drop bridge_tx to signal completion
                }
                buf.truncate(filled);
                // blocking_send applies backpressure — blocks if the
                // channel is full, keeping memory bounded.
                if bridge_tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
                    // Receiver dropped (e.g. store write failed); stop reading.
                    return;
                }
            }
            // bridge_tx is dropped here, closing the channel.
        });

        let forward_fut = async move {
            while let Some(result) = bridge_rx.recv().await {
                let chunk = result?;
                tx.send(chunk).await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to send chunk in stream_path_to_store: {:?}",
                        e
                    )
                })?;
            }
            tx.send_eof()
                .err_tip(|| "Failed to send EOF in stream_path_to_store")?;
            Result::<(), Error>::Ok(())
        };

        let (write_res, forward_res) = join!(write_fut, forward_fut);
        // Join the blocking task to propagate panics.
        read_handle
            .await
            .map_err(|e| make_err!(Code::Internal, "spawn_blocking join error: {:?}", e))?;
        forward_res?;
        write_res
    }

    /// Like [`stream_path_to_store`], but accepts an already-opened
    /// [`std::fs::File`] instead of a path. Use this when the caller must
    /// guarantee the fd is opened before a concurrent rename can move the
    /// file (e.g. `FilesystemStore::emplace_file` background rename).
    ///
    /// On POSIX, an open fd survives the rename of its directory entry —
    /// opening before the `join!()` eliminates the TOCTOU race.
    async fn stream_file_to_store(
        file: std::fs::File,
        store: &Store,
        key: StoreKey<'_>,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let (mut tx, rx) = make_buf_channel_pair_with_size(128);
        let write_fut = store.update(key.borrow(), rx, upload_size);

        const CHUNK_SIZE: usize = 256 * 1024;
        let (bridge_tx, mut bridge_rx) =
            tokio::sync::mpsc::channel::<Result<Bytes, Error>>(4);

        let read_handle = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut file = file;
            loop {
                let mut buf = vec![0u8; CHUNK_SIZE];
                let mut filled = 0;
                while filled < CHUNK_SIZE {
                    match file.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            let err = make_err!(
                                Code::Internal,
                                "Failed to read file in stream_file_to_store: {:?}",
                                e
                            );
                            let _ = bridge_tx.blocking_send(Err(err));
                            return;
                        }
                    }
                }
                if filled == 0 {
                    break;
                }
                buf.truncate(filled);
                if bridge_tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
                    return;
                }
            }
        });

        let forward_fut = async move {
            while let Some(result) = bridge_rx.recv().await {
                let chunk = result?;
                tx.send(chunk).await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to send chunk in stream_file_to_store: {:?}",
                        e
                    )
                })?;
            }
            tx.send_eof()
                .err_tip(|| "Failed to send EOF in stream_file_to_store")?;
            Result::<(), Error>::Ok(())
        };

        let (write_res, forward_res) = join!(write_fut, forward_fut);
        read_handle
            .await
            .map_err(|e| make_err!(Code::Internal, "spawn_blocking join error: {:?}", e))?;
        forward_res?;
        write_res
    }

    /// Returns the range of bytes that should be sent given a slice bounds
    /// offset so the output range maps the `received_range.start` to 0.
    // TODO(palfrey) This should be put into utils, as this logic is used
    // elsewhere in the code.
    pub fn calculate_range(
        received_range: &Range<u64>,
        send_range: &Range<u64>,
    ) -> Result<Option<Range<usize>>, Error> {
        // Protect against subtraction overflow.
        if received_range.start >= received_range.end {
            return Ok(None);
        }

        let start = max(received_range.start, send_range.start);
        let end = min(received_range.end, send_range.end);
        if received_range.contains(&start) && received_range.contains(&(end - 1)) {
            // Offset both to the start of the received_range.
            let calculated_range_start = usize::try_from(start - received_range.start)
                .err_tip(|| "Could not convert (start - received_range.start) to usize")?;
            let calculated_range_end = usize::try_from(end - received_range.start)
                .err_tip(|| "Could not convert (end - received_range.start) to usize")?;
            Ok(Some(calculated_range_start..calculated_range_end))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl StoreDriver for FastSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        key: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // If our slow store is a noop store, it'll always return a 404,
        // so only check the fast store in such case.
        let slow_store = self.slow_store.inner_store::<StoreKey<'_>>(None);
        if slow_store.optimized_for(StoreOptimizations::NoopDownloads) {
            return self.fast_store.has_with_results(key, results).await;
        }
        if self.local_only_reads.load(Ordering::Relaxed) {
            // Worker public CAS server variant — see `local_only_reads`
            // field comment. Consult fast → in-flight → mirror; never the
            // slow tier. Note the ordering differs from `get_part` (which
            // checks mirror first); for content-addressed CAS the sizes
            // are identical so either order is correct, but the divergence
            // is non-obvious and worth flagging.
            self.fast_store.has_with_results(key, results).await?;
            {
                let in_flight = self.in_flight_slow_writes.lock();
                for (k, result) in key.iter().zip(results.iter_mut()) {
                    if result.is_none() {
                        let owned = k.borrow().into_owned();
                        if let Some(chunks) = in_flight.get(&owned) {
                            let total_len: u64 = chunks.iter().map(|c| c.len() as u64).sum();
                            *result = Some(total_len);
                        }
                    }
                }
            }
            // #212 fixup B2: chunked in-flight set; size = digest.size_bytes()
            // because the actual bytes are tracked via the chunked-driver
            // pin, not stored in this set.
            {
                let chunked = self.chunked_in_flight_digests.lock();
                if !chunked.is_empty() {
                    for (k, result) in key.iter().zip(results.iter_mut()) {
                        if result.is_none() {
                            let digest = k.borrow().into_digest();
                            if chunked.contains(&digest) {
                                *result = Some(digest.size_bytes());
                            }
                        }
                    }
                }
            }
            {
                let mirror = self.mirror_blobs.lock();
                for (k, result) in key.iter().zip(results.iter_mut()) {
                    if result.is_none() {
                        let digest = k.borrow().into_digest();
                        if let Some((data, _)) = mirror.get(&digest) {
                            *result = Some(data.len() as u64);
                        }
                    }
                }
            }
            return Ok(());
        }
        // Only check the slow store because if it's not there, then something
        // down stream might be unable to get it.  This should not affect
        // workers as they only use get() and a CAS can use an
        // ExistenceCacheStore to avoid the bottleneck.
        self.slow_store.has_with_results(key, results).await?;
        // Fill in any blobs that are in-flight (written to fast store but
        // background slow write not yet complete).
        {
            let in_flight = self.in_flight_slow_writes.lock();
            if !in_flight.is_empty() {
                for (k, result) in key.iter().zip(results.iter_mut()) {
                    if result.is_none() {
                        let owned = k.borrow().into_owned();
                        if let Some(chunks) = in_flight.get(&owned) {
                            let total_len: u64 =
                                chunks.iter().map(|c| c.len() as u64).sum();
                            debug!(
                                key = %owned.as_str(),
                                data_len = total_len,
                                "has_with_results: found blob in in-flight map \
                                 (not yet on slow store)",
                            );
                            *result = Some(total_len);
                        }
                    }
                }
            }
        }
        // #212 fixup B2: chunked in-flight digest set. Separate from the
        // legacy `in_flight_slow_writes` because chunked-path writes
        // store their bytes in the chunked-driver pin (registered via
        // ChunkedReadRegistry) rather than in the legacy map. Size =
        // digest.size_bytes() since the digest fully specifies the
        // expected payload size.
        {
            let chunked = self.chunked_in_flight_digests.lock();
            if !chunked.is_empty() {
                for (k, result) in key.iter().zip(results.iter_mut()) {
                    if result.is_none() {
                        let digest = k.borrow().into_digest();
                        if chunked.contains(&digest) {
                            debug!(
                                ?digest,
                                "has_with_results: found blob in chunked-path \
                                 in-flight set (not yet on slow store)",
                            );
                            *result = Some(digest.size_bytes());
                        }
                    }
                }
            }
        }
        // Check mirror blobs for any still-missing digests.
        {
            let mirror = self.mirror_blobs.lock();
            if !mirror.is_empty() {
                for (k, result) in key.iter().zip(results.iter_mut()) {
                    if result.is_none() {
                        let digest = k.borrow().into_digest();
                        if let Some((data, _)) = mirror.get(&digest) {
                            *result = Some(data.len() as u64);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Mirror writes: hold blob data in memory only, skip both disk and
        // server. The server already has this blob persisted and is pushing
        // a copy to us for read locality. Data is cleaned up when
        // BlobsInStableStorage arrives or after a TTL.
        let is_mirror = IS_MIRROR_REQUEST.try_with(|v| *v).unwrap_or(false);
        if is_mirror {
            let digest = key.borrow().into_digest();
            let mut chunks = bytes::BytesMut::with_capacity(digest.size_bytes() as usize);
            loop {
                let chunk = reader
                    .recv()
                    .await
                    .err_tip(|| "mirror recv in FastSlowStore::update")?;
                if chunk.is_empty() {
                    break; // EOF
                }
                chunks.extend_from_slice(&chunk);
            }
            let data = chunks.freeze();
            // Propagate cap-exceeded back to the mirror writer so it can
            // record a per-peer failure (see `insert_mirror_blob`).
            self.insert_mirror_blob(digest, data)?;
            return Ok(());
        }

        // If either one of our stores is a noop store, bypass the multiplexing
        // and just use the store that is not a noop store.
        let ignore_slow = self
            .slow_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.slow_direction == StoreDirection::ReadOnly
            || self.slow_direction == StoreDirection::Get;
        let ignore_fast = self
            .fast_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.fast_direction == StoreDirection::ReadOnly
            || self.fast_direction == StoreDirection::Get;
        if ignore_slow && ignore_fast {
            // We need to drain the reader to avoid the writer complaining that we dropped
            // the connection prematurely.
            reader
                .drain()
                .await
                .err_tip(|| "In FastFlowStore::update")?;
            return Ok(());
        }
        if ignore_slow {
            let result = self.fast_store.update(key.borrow(), reader, size_info).await;
            if result.is_ok() {
                if let StoreKey::Digest(digest) = &key {
                    self.fast_store.pin_digests(&[*digest]);
                    // Track as needing upload — the slow store was skipped,
                    // so the blob only exists locally. On reconnect the
                    // worker will upload it if the server hasn't acked via
                    // BlobsInStableStorage.
                    self.failed_slow_writes.lock().insert(*digest);
                }
            }
            return result;
        }
        if ignore_fast {
            return self.slow_store.update(key, reader, size_info).await;
        }

        // Failpoint: simulate a failure during the update path after
        // bypass logic has passed. Tests that errors in the write path
        // propagate correctly and the reader is not left hanging.
        #[cfg(feature = "failpoints")]
        fail::fail_point!("fast_slow_store_update_fail", |_| {
            Err(make_err!(
                Code::Internal,
                "failpoint: update failed in fast_slow_store"
            ))
        });

        let update_start = std::time::Instant::now();

        // #212 Phase 2.7: Bazel-facing internal chunking branch.
        //
        // When (a) the key is `StoreKey::Digest` (Str-keyed AC entries
        // are not eligible — see fixup D6 below), (b) the kill-switch
        // is ON, (c) a chunked dispatcher has been installed, AND
        // (d) the blob is at least the chunked size threshold
        // (production: CHUNK_SIZE = 1 MiB; tests can override via
        // `set_chunked_size_threshold_for_test`), hand the upstream to
        // the dispatcher. The dispatcher returns Ok as soon as the
        // final chunk is admitted (β async-commit; anti-#203). The
        // fast-tier (MemoryStore) write happens inline below in the
        // standard path; for chunked-dispatch we MUST still mirror the
        // fast-tier write so the in-memory replica covers the
        // async-commit window. We therefore tee the bytes into the
        // dispatcher AND the fast tier in parallel, mirroring the
        // existing `tokio::join!(data_stream_fut, fast_store_fut)`
        // pattern.
        //
        // The kill-switch defaults OFF; flipping it ON in production
        // requires explicit user sign-off per CLAUDE.md
        // `feedback_async_to_sync_requires_explicit_signoff`.
        #[cfg(feature = "chunked_fast_slow")]
        {
            // #212 Phase 2.7 fixup D6 (code-reviewer §MAJOR 3): gate on
            // KEY SHAPE first — only `StoreKey::Digest` is eligible for
            // chunked dispatch. AC-store entries are `StoreKey::Str`
            // (sub-KiB ActionResult protos); they cannot exceed the
            // 1 MiB CHUNK_SIZE threshold under any realistic workload,
            // and the `BazelChunkedDispatcher::dispatch` API requires a
            // real `DigestInfo` (the per-blob `ChunkedDriver` machinery
            // keys on the CAS digest). Inverting the gate avoids the
            // hot-path `into_digest()` Blake3 hash on every Str-keyed
            // `update()` once the kill-switch is flipped ON in
            // production. (This is also a latent correctness fix:
            // `StoreKey::Str::into_digest()` would synthesize a digest
            // whose `size_bytes` equals the key length, never the blob
            // payload, so the size-threshold check was meaningless for
            // Str-keys anyway.)
            if let StoreKey::Digest(digest) = &key {
                if crate::chunked::bazel_facing_internal_chunking_enabled()
                    && digest.size_bytes() >= self.chunked_size_threshold()
                {
                    if let Some(dispatcher) = self.bazel_chunked_dispatcher() {
                        let digest = *digest;
                        return self
                            .update_via_chunked_dispatcher(
                                key,
                                reader,
                                size_info,
                                digest,
                                dispatcher,
                                update_start,
                            )
                            .await;
                    }
                }
            }
        }

        // Decoupled write: stream to fast store while accumulating data,
        // then spawn a background task for the slow store write.
        // This prevents slow-store latency (e.g. ZFS txg sync) from
        // blocking the fast-store (MemoryStore) write path.
        let (mut fast_tx, fast_rx) = make_buf_channel_pair_with_size(128);

        debug!(
            ?key,
            ?size_info,
            "FastSlowStore::update: start",
        );

        // Read from upstream, forward to fast store, collect chunks as
        // Vec<Bytes> (O(1) refcount bump per chunk, no copying) for the
        // background slow store write.
        let data_stream_fut = async move {
            // Writer-termination contract for `join!(data_stream_fut,
            // fast_store_fut)`: any `?` Err exit historically dropped
            // `fast_tx` silently, so the sibling `fast_store_fut`
            // (= `fast_store.update(fast_rx)`) blocked on `rx.recv().await`
            // until the OS dropped the sender, then synthesized a generic
            // `"Sender dropped before sending EOF"` Internal — drowning
            // out the actionable upstream cause from `reader.recv()`.
            //
            // The `WriteHalfGuard` wrap below upgrades the Drop fallback
            // so on any `?` propagation the guard's `Drop`
            // (`buf_channel.rs:516-520`) synthesizes a structured Internal
            // carrying the greppable `"buf_channel: writer dropped without
            // commit"` marker. The paired fast_store_fut reader observes
            // a structured Err immediately instead of the misleading
            // "Sender dropped" derivative. The OUTER `join!(...)` result
            // still preserves the actionable upstream cause via
            // `data_res` — Drop fallback on the reader is purely
            // defense-in-depth so it unblocks, not the carrier of the
            // upstream cause. Latent harden in the #245 family — see
            // audit at `.claude/audits/245-fast-slow-store-sender-drop.md`
            // Phase 4 sibling-bug list.
            let mut fast_guard = WriteHalfGuard::new(&mut fast_tx);
            let mut chunks: Vec<Bytes> = Vec::new();
            loop {
                let buffer = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to read buffer in fastslow store")?;
                if buffer.is_empty() {
                    fast_guard.commit_eof().err_tip(
                        || "Failed to write eof to fast store in fast_slow store update",
                    )?;
                    return Result::<Vec<Bytes>, Error>::Ok(chunks);
                }
                chunks.push(buffer.clone());
                (*fast_guard).send(buffer).await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Failed to send message to fast_store in fast_slow_store {:?}",
                        e
                    )
                })?;
            }
        };

        let fast_store_fut = self.fast_store.update(key.borrow(), fast_rx, size_info);
        let (data_res, fast_res) = join!(data_stream_fut, fast_store_fut);
        let data = match data_res {
            Ok(d) => d,
            Err(err) => {
                error!(
                    ?key,
                    elapsed_ms = update_start.elapsed().as_millis() as u64,
                    ?err,
                    "FastSlowStore::update: data stream failed",
                );
                return Err(err);
            }
        };
        if let Err(err) = &fast_res {
            error!(
                ?key,
                elapsed_ms = update_start.elapsed().as_millis() as u64,
                ?err,
                "FastSlowStore::update: fast store write failed",
            );
        }
        fast_res?;

        // Pin the digest in the fast store to prevent eviction until the
        // server confirms stable storage via BlobsInStableStorage.
        if let StoreKey::Digest(digest) = &key {
            self.fast_store.pin_digests(&[*digest]);
        }

        let bytes_sent: u64 = data.iter().map(|c| c.len() as u64).sum();
        let fast_elapsed = update_start.elapsed();
        debug!(
            ?key,
            fast_ms = fast_elapsed.as_millis(),
            total_bytes = bytes_sent,
            "FastSlowStore::update: fast store complete, spawning background slow write",
        );

        // During shutdown, write directly to the slow store (blocking the
        // caller) instead of spawning a background task that would be killed.
        if self.shutting_down.load(Ordering::Acquire) {
            let (mut tx, rx) = make_buf_channel_pair_with_size(128);
            let write_fut = self.slow_store.update(key.borrow(), rx, size_info);
            let send_fut = async {
                for chunk in data {
                    tx.send(chunk).await.map_err(|e| {
                        make_err!(Code::Internal, "shutdown flush send: {:?}", e)
                    })?;
                }
                tx.send_eof()
                    .err_tip(|| "shutdown flush send_eof")?;
                Result::<(), Error>::Ok(())
            };
            let (write_result, send_result) = tokio::join!(write_fut, send_fut);
            return send_result.and(write_result);
        }

        // Insert into in-flight map so get_part can serve this blob even if
        // the fast store evicts it before the slow write completes.
        let owned_key = key.borrow().into_owned();
        self.in_flight_slow_writes
            .lock()
            .insert(owned_key.clone(), data.clone());

        let in_flight = self.in_flight_slow_writes.clone();
        let in_flight_empty = self.in_flight_empty_notify.clone();
        let stable_digests_ref = self.stable_digests.clone();
        let stable_notify_ref = self.stable_notify.clone();
        let failed_writes_ref = self.failed_slow_writes.clone();
        let fast_store_ref = self.fast_store.clone();
        let slow_store = self.slow_store.clone();
        let key_for_bg = owned_key.clone();
        let spawn_instant = std::time::Instant::now();
        debug!(
            ?key,
            bytes_sent,
            "FastSlowStore::update: background slow write spawned",
        );
        tokio::spawn(async move {
            let schedule_delay_ms = spawn_instant.elapsed().as_millis();
            if schedule_delay_ms > 100 {
                warn!(
                    key = ?key_for_bg,
                    schedule_delay_ms,
                    bytes_sent,
                    "FastSlowStore: background slow write task was \
                     delayed before starting",
                );
            }
            let slow_start = std::time::Instant::now();
            // Stream collected chunks to slow store via buf_channel,
            // avoiding a single large concatenation.
            let (mut slow_tx, slow_rx) = make_buf_channel_pair_with_size(128);
            let write_fut = slow_store.update(
                key_for_bg.borrow(),
                slow_rx,
                UploadSizeInfo::ExactSize(bytes_sent),
            );
            let send_fut = async {
                for chunk in data {
                    slow_tx.send(chunk).await.map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to send chunk to slow store: {:?}",
                            e
                        )
                    })?;
                }
                slow_tx.send_eof().err_tip(
                    || "Failed to send eof to slow store in background write",
                )?;
                Result::<(), Error>::Ok(())
            };
            // Watchdog: if the slow-write hasn't terminated by
            // SLOW_WRITE_WATCHDOG_SECS, queue the digest for retry on
            // reconnect WITHOUT aborting the in-flight write. The
            // GrpcStore default has `rpc_timeout_s = 0` (disabled), so
            // a stuck transport can block this spawn indefinitely; the
            // pin-expiry callback only fires at PIN_TIMEOUT_SECS=120,
            // so without the watchdog there's a 60s+ window where the
            // failure is invisible. Spawned task is aborted on terminal
            // result via the `completed` flag so a watchdog firing
            // microseconds after the join completes doesn't double-
            // insert (the existing failure recovery below would already
            // have).
            let completed = Arc::new(AtomicBool::new(false));
            let watchdog_handle = {
                let completed = completed.clone();
                let key_for_watchdog = key_for_bg.clone();
                let failed_writes_ref = failed_writes_ref.clone();
                let fast_store_ref = fast_store_ref.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(SLOW_WRITE_WATCHDOG_SECS)).await;
                    if completed.load(Ordering::Acquire) {
                        return;
                    }
                    if let StoreKey::Digest(digest) = &key_for_watchdog {
                        warn!(
                            ?digest,
                            watchdog_secs = SLOW_WRITE_WATCHDOG_SECS,
                            total_bytes = bytes_sent,
                            "FastSlowStore: background slow write exceeded watchdog \
                             deadline; queueing for retry-on-reconnect (write task NOT \
                             aborted — may still complete)"
                        );
                        failed_writes_ref.lock().insert(*digest);
                        // Re-pin so the blob survives PIN_TIMEOUT_SECS even
                        // if the original pin was the only thing keeping it
                        // alive. pin_keys refreshes the deadline on an
                        // already-pinned entry.
                        fast_store_ref.pin_digests(&[*digest]);
                    }
                })
            };
            let (write_result, send_result) = tokio::join!(write_fut, send_fut);
            completed.store(true, Ordering::Release);
            watchdog_handle.abort();

            let slow_ms = slow_start.elapsed().as_millis();
            let mut result = send_result.and(write_result);

            // Failpoint: force background slow-write failure regardless of
            // the actual outcome. Used by the race-fix regression test and
            // by tests that exercise the completion-listener wiring. The
            // failpoint flips `result` to Err so the failure-recovery path
            // (pin + failed-set insert) executes, exercising the closed
            // race window between in_flight removal and pin.
            #[cfg(feature = "failpoints")]
            {
                fn forced_failure() -> Result<(), Error> {
                    fail::fail_point!("fast_slow_background_slow_write_fail", |_| {
                        Err(make_err!(
                            Code::Internal,
                            "failpoint: background slow write forced failure"
                        ))
                    });
                    Ok(())
                }
                if let Err(err) = forced_failure() {
                    result = Err(err);
                }
            }

            // CRITICAL: failure recovery (pin + failed-set insert) MUST run
            // BEFORE removing from `in_flight_slow_writes`. Previously we
            // removed first, opening a microseconds-to-ms window in which
            // the blob was reachable from neither in_flight nor (if
            // MemoryStore had already evicted) the fast store, while the
            // ExistenceCache still claimed it existed. Reordering closes
            // the race: by the time in_flight is empty, the blob is either
            // pinned in fast store (failure path) or stable_digests has
            // been notified (success path) and the listener has fired.
            match &result {
                Ok(()) => {
                    if let StoreKey::Digest(digest) = &key_for_bg {
                        stable_digests_ref.lock().push(*digest);
                        stable_notify_ref.notify_one();
                    }
                    debug!(
                        key = ?key_for_bg,
                        schedule_delay_ms,
                        slow_ms,
                        bytes_sent,
                        "FastSlowStore::update: background slow write complete",
                    );
                }
                Err(e) => {
                    if let StoreKey::Digest(digest) = &key_for_bg {
                        failed_writes_ref.lock().insert(*digest);
                        // Re-pin so the blob survives until reconnect retry.
                        // Without this, the 120s auto-expire could allow
                        // eviction before the worker reconnects.
                        fast_store_ref.pin_digests(&[*digest]);
                    }
                    error!(
                        key = ?key_for_bg,
                        schedule_delay_ms,
                        slow_ms,
                        bytes_sent,
                        error = ?e,
                        "FastSlowStore::update: background slow write FAILED — \
                         blob pinned, will retry on reconnect",
                    );
                }
            }

            // Now safe to remove the in-flight entry — failure recovery
            // has already observed the terminal state.
            {
                let mut guard = in_flight.lock();
                guard.remove(&key_for_bg);
                if guard.is_empty() {
                    in_flight_empty.notify_waiters();
                }
            }
        });

        Ok(())
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
        // Mirror writes: hold in memory only.
        let is_mirror = IS_MIRROR_REQUEST.try_with(|v| *v).unwrap_or(false);
        if is_mirror {
            let digest = key.borrow().into_digest();
            // Propagate cap-exceeded back to the mirror writer so it can
            // record a per-peer failure (see `insert_mirror_blob`).
            self.insert_mirror_blob(digest, data)?;
            return Ok(());
        }

        let ignore_slow = self
            .slow_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.slow_direction == StoreDirection::ReadOnly
            || self.slow_direction == StoreDirection::Get;
        let ignore_fast = self
            .fast_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.fast_direction == StoreDirection::ReadOnly
            || self.fast_direction == StoreDirection::Get;

        if ignore_slow && ignore_fast {
            return Ok(());
        }
        if ignore_slow {
            let result = self.fast_store.update_oneshot(key.borrow(), data).await;
            if result.is_ok() {
                if let StoreKey::Digest(digest) = &key {
                    self.fast_store.pin_digests(&[*digest]);
                    self.failed_slow_writes.lock().insert(*digest);
                }
            }
            return result;
        }
        if ignore_fast {
            return self.slow_store.update_oneshot(key, data).await;
        }

        // Failpoint: simulate a failure during the oneshot update path.
        // Tests that errors propagate correctly and no partial data
        // is left in the fast or slow store.
        #[cfg(feature = "failpoints")]
        fail::fail_point!("fast_slow_store_update_oneshot_fail", |_| {
            Err(make_err!(
                Code::Internal,
                "failpoint: update_oneshot failed in fast_slow_store"
            ))
        });

        let data_len = data.len();
        debug!(
            ?key,
            data_len,
            "FastSlowStore::update_oneshot: start",
        );

        // Write to fast store first (blocking — typically MemoryStore, near-instant).
        let fast_start = std::time::Instant::now();
        let fast_result = self
            .fast_store
            .update_oneshot(key.borrow(), data.clone())
            .await;
        let fast_ms = fast_start.elapsed().as_millis();
        if let Err(ref err) = fast_result {
            error!(
                ?key,
                fast_ms,
                data_len,
                ?err,
                "FastSlowStore::update_oneshot: fast store write failed",
            );
        }
        fast_result?;

        // Pin the digest in the fast store to prevent eviction until the
        // server confirms stable storage via BlobsInStableStorage.
        if let StoreKey::Digest(digest) = &key {
            self.fast_store.pin_digests(&[*digest]);
        }

        // During shutdown, write directly instead of spawning background task.
        if self.shutting_down.load(Ordering::Acquire) {
            return self.slow_store.update_oneshot(key, data).await;
        }

        // Spawn background slow store write.
        let owned_key = key.borrow().into_owned();
        self.in_flight_slow_writes
            .lock()
            .insert(owned_key.clone(), vec![data.clone()]);

        let in_flight = self.in_flight_slow_writes.clone();
        let in_flight_empty = self.in_flight_empty_notify.clone();
        let stable_digests_ref = self.stable_digests.clone();
        let stable_notify_ref = self.stable_notify.clone();
        let failed_writes_ref = self.failed_slow_writes.clone();
        let fast_store_ref = self.fast_store.clone();
        let slow_store = self.slow_store.clone();
        let key_for_bg = owned_key.clone();
        let spawn_instant = std::time::Instant::now();
        debug!(
            ?key,
            data_len,
            "FastSlowStore::update_oneshot: background slow write spawned",
        );
        tokio::spawn(async move {
            let schedule_delay_ms = spawn_instant.elapsed().as_millis();
            if schedule_delay_ms > 100 {
                warn!(
                    key = ?key_for_bg,
                    schedule_delay_ms,
                    data_len,
                    "FastSlowStore::update_oneshot: background slow write task \
                     was delayed before starting",
                );
            }
            let slow_start = std::time::Instant::now();
            // Watchdog: see streaming `update` path for full rationale.
            // GrpcStore default has rpc_timeout_s = 0 (disabled) so a
            // stuck transport blocks indefinitely; this watchdog records
            // the digest in `failed_slow_writes` after
            // SLOW_WRITE_WATCHDOG_SECS without aborting the spawn.
            let completed = Arc::new(AtomicBool::new(false));
            let watchdog_handle = {
                let completed = completed.clone();
                let key_for_watchdog = key_for_bg.clone();
                let failed_writes_ref = failed_writes_ref.clone();
                let fast_store_ref = fast_store_ref.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(SLOW_WRITE_WATCHDOG_SECS)).await;
                    if completed.load(Ordering::Acquire) {
                        return;
                    }
                    if let StoreKey::Digest(digest) = &key_for_watchdog {
                        warn!(
                            ?digest,
                            watchdog_secs = SLOW_WRITE_WATCHDOG_SECS,
                            data_len,
                            "FastSlowStore: background slow oneshot write exceeded \
                             watchdog deadline; queueing for retry-on-reconnect \
                             (write task NOT aborted — may still complete)"
                        );
                        failed_writes_ref.lock().insert(*digest);
                        fast_store_ref.pin_digests(&[*digest]);
                    }
                })
            };
            let mut result = slow_store
                .update_oneshot(key_for_bg.borrow(), data)
                .await;
            completed.store(true, Ordering::Release);
            watchdog_handle.abort();

            // Failpoint: force background slow-write failure (matches the
            // failpoint in the streaming `update` path). Tests use this to
            // verify the post-spawn ordering invariant — failure recovery
            // (pin + failed-set insert) MUST run before in_flight removal.
            #[cfg(feature = "failpoints")]
            {
                fn forced_failure() -> Result<(), Error> {
                    fail::fail_point!("fast_slow_background_slow_write_fail", |_| {
                        Err(make_err!(
                            Code::Internal,
                            "failpoint: background slow write forced failure"
                        ))
                    });
                    Ok(())
                }
                if let Err(err) = forced_failure() {
                    result = Err(err);
                }
            }

            let slow_ms = slow_start.elapsed().as_millis();
            // CRITICAL ordering: failure recovery before in_flight removal
            // (see streaming `update` path for full rationale).
            match &result {
                Ok(()) => {
                    if let StoreKey::Digest(digest) = &key_for_bg {
                        stable_digests_ref.lock().push(*digest);
                        stable_notify_ref.notify_one();
                    }
                    debug!(
                        key = ?key_for_bg,
                        schedule_delay_ms,
                        slow_ms,
                        data_len,
                        "FastSlowStore::update_oneshot: background slow write complete",
                    );
                }
                Err(e) => {
                    if let StoreKey::Digest(digest) = &key_for_bg {
                        failed_writes_ref.lock().insert(*digest);
                        // Re-pin so the blob survives until reconnect retry.
                        fast_store_ref.pin_digests(&[*digest]);
                    }
                    error!(
                        key = ?key_for_bg,
                        schedule_delay_ms,
                        slow_ms,
                        data_len,
                        error = ?e,
                        "FastSlowStore::update_oneshot: background slow write FAILED — \
                         blob pinned, will retry on reconnect",
                    );
                }
            }

            // Now safe to remove in-flight entry — failure recovery has
            // already run.
            {
                let mut guard = in_flight.lock();
                guard.remove(&key_for_bg);
                if guard.is_empty() {
                    in_flight_empty.notify_waiters();
                }
            }
        });

        Ok(())
    }

    /// `FastSlowStore` has optimizations for dealing with files.
    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        optimization == StoreOptimizations::FileUpdates
    }

    /// Optimized variation to consume the file if one of the stores is a
    /// filesystem store. This makes the operation a move instead of a copy
    /// dramatically increasing performance for large files.
    ///
    /// When both stores need the data, the file is read into memory once and
    /// then written to both stores in parallel. The store that supports
    /// `FileUpdates` receives the original file handle (for move/hardlink),
    /// while the other store receives the data via a streaming channel.
    async fn update_with_whole_file(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        path: OsString,
        mut file: fs::FileSlot,
        upload_size: UploadSizeInfo,
    ) -> Result<Option<fs::FileSlot>, Error> {
        trace!(
            key = ?key,
            ?upload_size,
            "FastSlowStore::update_with_whole_file: starting",
        );
        if self
            .fast_store
            .optimized_for(StoreOptimizations::FileUpdates)
        {
            let need_slow = !self
                .slow_store
                .inner_store(Some(key.borrow()))
                .optimized_for(StoreOptimizations::NoopUpdates)
                && self.slow_direction != StoreDirection::ReadOnly
                && self.slow_direction != StoreDirection::Get;
            let need_fast = self.fast_direction != StoreDirection::ReadOnly
                && self.fast_direction != StoreDirection::Get;

            if need_slow && need_fast {
                // Open a separate fd from the path for the slow store
                // BEFORE starting the fast_fut. The fast store's
                // update_with_whole_file (FilesystemStore) renames/moves
                // the file out of its original location via emplace_file.
                // On POSIX, an open fd survives a rename of its path, so
                // opening before the rename races is safe. Opening after
                // the join!() starts risks ENOENT if emplace_file's
                // background rename completes first.
                let slow_file = std::fs::File::open(std::path::Path::new(&path))
                    .map_err(|e| make_err!(
                        Code::Internal,
                        "Failed to open file for slow store streaming: {:?}",
                        e
                    ))?;
                let slow_fut = Self::stream_file_to_store(
                    slow_file,
                    &self.slow_store,
                    key.borrow(),
                    upload_size,
                );
                let fast_fut = self
                    .fast_store
                    .update_with_whole_file(key.borrow(), path, file, upload_size);

                let (slow_res, fast_res) = join!(slow_fut, fast_fut);
                slow_res.err_tip(|| "In FastSlowStore::update_with_whole_file slow_store")?;
                return fast_res.err_tip(|| "In FastSlowStore::update_with_whole_file fast_store");
            }

            if need_slow {
                // Fast store is read-only; only write to slow store.
                trace!("FastSlowStore::update_with_whole_file: uploading to slow_store only");
                file = slow_update_store_with_file(
                    self.slow_store.as_store_driver_pin(),
                    key.borrow(),
                    file,
                    upload_size,
                )
                .await
                .err_tip(|| "In FastSlowStore::update_with_whole_file slow_store")?;
                return Ok(Some(file));
            }

            if !need_fast {
                return Ok(Some(file));
            }
            return self
                .fast_store
                .update_with_whole_file(key, path, file, upload_size)
                .await;
        }

        if self
            .slow_store
            .optimized_for(StoreOptimizations::FileUpdates)
        {
            let ignore_fast = self
                .fast_store
                .inner_store(Some(key.borrow()))
                .optimized_for(StoreOptimizations::NoopUpdates)
                || self.fast_direction == StoreDirection::ReadOnly
                || self.fast_direction == StoreDirection::Get;
            let ignore_slow = self.slow_direction == StoreDirection::ReadOnly
                || self.slow_direction == StoreDirection::Get;

            if !ignore_fast && !ignore_slow {
                // Open a separate fd from the path for the fast store
                // BEFORE starting slow_fut. The slow store's
                // update_with_whole_file (FilesystemStore) renames/moves
                // the file out of its original location via emplace_file.
                // On POSIX, an open fd survives a rename of its path, so
                // opening before the rename races is safe. Opening after
                // the join!() starts risks ENOENT if emplace_file's
                // background rename completes first.
                let fast_file = std::fs::File::open(std::path::Path::new(&path))
                    .map_err(|e| make_err!(
                        Code::Internal,
                        "Failed to open file for fast store streaming: {:?}",
                        e
                    ))?;
                let fast_fut = Self::stream_file_to_store(
                    fast_file,
                    &self.fast_store,
                    key.borrow(),
                    upload_size,
                );
                let slow_fut = self
                    .slow_store
                    .update_with_whole_file(key.borrow(), path, file, upload_size);

                let (fast_res, slow_res) = join!(fast_fut, slow_fut);
                fast_res.err_tip(|| "In FastSlowStore::update_with_whole_file fast_store")?;
                return slow_res.err_tip(|| "In FastSlowStore::update_with_whole_file slow_store");
            }

            if !ignore_fast {
                file = slow_update_store_with_file(
                    self.fast_store.as_store_driver_pin(),
                    key.borrow(),
                    file,
                    upload_size,
                )
                .await
                .err_tip(|| "In FastSlowStore::update_with_whole_file fast_store")?;
            }
            if ignore_slow {
                return Ok(Some(file));
            }
            return self
                .slow_store
                .update_with_whole_file(key, path, file, upload_size)
                .await;
        }

        let file = slow_update_store_with_file(self, key, file, upload_size)
            .await
            .err_tip(|| "In FastSlowStore::update_with_whole_file")?;
        Ok(Some(file))
    }

    // LINT: writer-termination policy for `get_part` (do NOT bypass).
    //
    // Every exit path of this function MUST terminate the borrowed `writer`
    // (either with `send_eof` on success or `send_error` on failure) before
    // returning. Failure to do so deadlocks any wrapping layer that paired
    // this writer with a reader inside `tokio::join!` (e.g.
    // `VerifyStore::get_part`'s `(get_fut, check_fut)` pattern over a
    // freshly-built `tx`/`rx`). Production consequences of an omission have
    // historically been multi-hour Bazel build wedges (see CLAUDE.md
    // "Test in production composition, not in isolation").
    //
    // STRUCTURAL ENFORCEMENT: wrap `writer` in `WriteHalfGuard` (`guard`).
    // Verbs: `commit_eof()?` (happy path, send EOF), `commit_delegated_if_ok(&res)`
    // (only suppress Drop on Ok — preferred for sub-store delegation so
    // a sub-store contract violation is caught by the Drop fallback),
    // `return Err(guard.fail(err))` (explicit failure). Any uncommitted
    // exit fires the Drop fallback (synthesized Internal) so paired
    // readers can't deadlock. See `WriteHalfGuard` rustdoc.
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let mut guard = WriteHalfGuard::new(writer);

        // Check mirror blob cache first — these are blobs the server pushed
        // to us that we hold in memory only.
        {
            let digest = key.borrow().into_digest();
            let maybe_data = self.mirror_blobs.lock().get(&digest).map(|(d, _)| d.clone());
            if let Some(data) = maybe_data {
                // Defensive guard against a phantom-positive entry: by the
                // insert_mirror_blob invariant, data.len() must equal
                // digest.size_bytes(). If it doesn't (corrupted/legacy entry
                // pre-dating the source-side validation), remove it and
                // return NotFound so the caller routes to a different
                // source — better than silently serving Ok+EOF (which
                // a previous workaround in grpc_store.rs translated into a
                // retryable NotFound; that workaround has been removed).
                let expected = digest.size_bytes() as usize;
                if data.len() != expected {
                    warn!(
                        %digest,
                        data_len = data.len(),
                        expected_size = expected,
                        "mirror_blobs entry size mismatch — removing phantom + returning NotFound"
                    );
                    // Use the canonical remove path so total-bytes accounting,
                    // mirror_changes (server-eviction notify), and the
                    // mirror_changes_notify wake-up all stay consistent. A
                    // raw HashMap.remove here would leak `mirror_blobs_total_bytes`
                    // and silently leave the server's locality_map pointing
                    // at this worker for a digest the worker has just discarded.
                    self.remove_mirror_blobs(&[digest]);
                    // #171 sibling: route through `commit_with_inner_miss_gate`
                    // so the OUTER writer survives when the caller (typically
                    // `WorkerProxyStore::get_part_sequential`) has opted in
                    // via `INNER_MISS_NO_TERMINATE`. The corrupt mirror entry
                    // was just evicted; the blob may still live on slow-tier
                    // or peers, so peer-fetch on the same writer is the
                    // recovery path. Without the gate, the Drop fallback
                    // closes the OUTER writer and peer-fetch fails with
                    // `"Tried to send while stream is closed"`. When the
                    // gate is NOT set, the helper falls through to
                    // `commit_delegated_if_ok(&Err)` which leaves the
                    // VerifyStore deadlock-defense armed (under-action
                    // contract preserved).
                    let bytes_before = guard.get_bytes_written();
                    let res: Result<(), Error> = Err(make_err!(
                        Code::NotFound,
                        "mirror_blobs entry for {digest} had wrong size \
                         ({} != {expected}) — entry removed",
                        data.len()
                    ));
                    commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
                    return res;
                }
                let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
                if offset_usize < data.len() {
                    let end = length
                        .and_then(|l| usize::try_from(l).ok())
                        .map(|l| offset_usize.saturating_add(l).min(data.len()))
                        .unwrap_or(data.len());
                    let slice = data.slice(offset_usize..end);
                    if !slice.is_empty() {
                        guard
                            .send(slice)
                            .await
                            .err_tip(|| "Failed to send mirror blob data")?;
                    }
                }
                guard
                    .commit_eof()
                    .err_tip(|| "Failed to send EOF for mirror blob")?;
                return Ok(());
            }
        }

        // Failpoint: simulate fast store returning NotFound during get_part.
        // Exercises the fallback from fast store to slow store populate path,
        // which is critical for serving data when the local cache misses.
        #[cfg(feature = "failpoints")]
        fail::fail_point!("fast_slow_get_part_fast_store_not_found", |_| {
            // The `fail_point!` macro expands to roughly
            //   `if let Some(res) = fail::eval(...) { return res; }`
            // — the `return` is in the OUTER `get_part` fn body, NOT in
            // this closure. So when this branch fires the outer fn
            // unwinds and `guard`'s `Drop` runs from the outer fn's
            // stack frame, synthesizing the Internal terminator that
            // unblocks any paired reader. (The closure scope itself
            // could capture `guard` if needed; we don't because the
            // macro's outer-fn `return` already gives us correct
            // unwinding without an explicit `guard.fail(...)`.)
            Err(make_err!(Code::NotFound, "failpoint: fast store not found"))
        });

        // Try the fast store directly — avoids the extra has() round-trip.
        // On NotFound (with no bytes written), fall through to slow store.
        let bytes_before = guard.get_bytes_written();
        match self
            .fast_store
            .get_part(key.borrow(), &mut *guard, offset, length)
            .await
        {
            Ok(()) => {
                let bytes_written = guard.get_bytes_written() - bytes_before;
                // Validate full reads against digest size to detect truncated entries.
                let expected_size = match key.borrow() {
                    StoreKey::Digest(d) => d.size_bytes(),
                    StoreKey::Str(_) => 0,
                };
                if expected_size > 0 && offset == 0 && length.is_none()
                    && bytes_written < expected_size
                {
                    error!(
                        ?key,
                        bytes_written,
                        expected_size,
                        "fast store returned truncated data, cannot recover (bytes already sent)"
                    );
                    // Bytes were already written — we cannot fall through
                    // to slow store. Return an error so the caller retries
                    // the whole operation. `guard.fail(...)` sends the
                    // structured error to terminate any paired reader; even
                    // if the inner fast store DID send EOF, surfacing the
                    // structured Internal is preferable to an
                    // ambiguously-truncated stream the receiver would
                    // otherwise observe as a clean EOF.
                    return Err(guard.fail(make_err!(
                        Code::Internal,
                        "Fast store returned {bytes_written} bytes but expected {expected_size}"
                    )));
                }
                self.metrics
                    .fast_store_hit_count
                    .fetch_add(1, Ordering::Acquire);
                self.metrics
                    .fast_store_downloaded_bytes
                    .fetch_add(bytes_written, Ordering::Acquire);
                // We're already inside the `Ok(())` arm of the match —
                // the inner fast_store's get_part contract guarantees it
                // terminated the writer (sent its own EOF) on success.
                // The `&Ok::<(), Error>(())` sentinel exists only to
                // unconditionally suppress the Drop fallback; the
                // `_if_ok` conditional is dead here (always true). Calling
                // `guard.commit_eof()` instead would re-trigger
                // `send_eof` against an already-EOF'd channel and emit a
                // noisy "Stream already closed when eof already was sent"
                // warn per request (see `buf_channel.rs::send_eof`).
                // Re-adding a `commit()` no-arg helper for one site is
                // below the bar (rust-crate-reviewer: "defer; one usage
                // site is below the bar for adding a public verb").
                guard.commit_delegated_if_ok(&Ok::<(), Error>(()));
                return Ok(());
            }
            Err(err) if err.code == Code::NotFound && guard.get_bytes_written() == bytes_before => {
                // Fast store miss — no bytes written, safe to fall through.
                debug!(
                    ?key,
                    "fast store miss, falling through to slow store"
                );
            }
            Err(err) => {
                // Non-NotFound err OR NotFound-with-partial-bytes: surface
                // the structured error so paired readers don't deadlock.
                return Err(guard.fail(err));
            }
        }

        // Check in-flight slow writes: the blob may have been evicted from the
        // fast store while its background slow-store write is still in progress.
        {
            let owned_key = key.borrow().into_owned();
            let maybe_chunks = self.in_flight_slow_writes.lock().get(&owned_key).cloned();
            if let Some(chunks) = maybe_chunks {
                let total_len: usize = chunks.iter().map(|c| c.len()).sum();
                // Defensive guard analogous to the mirror_blobs branch above:
                // for a non-zero digest, the in-flight entry MUST sum to the
                // digest's full size — it's a snapshot of the chunks just
                // written to the fast store. If it's short, prefer NotFound
                // over silently serving an empty/truncated stream.
                if let StoreKey::Digest(d) = key.borrow() {
                    let expected = d.size_bytes() as usize;
                    if total_len != expected {
                        warn!(
                            digest = %d,
                            total_len,
                            expected_size = expected,
                            "in_flight entry size mismatch — returning NotFound \
                             instead of serving short stream"
                        );
                        // Match the canonical bg-write completion path
                        // (line ~2199): remove + notify if empty so any
                        // graceful-shutdown waiter on `in_flight_empty_notify`
                        // doesn't miss its wake-up.
                        {
                            let mut in_flight_guard = self.in_flight_slow_writes.lock();
                            in_flight_guard.remove(&owned_key);
                            if in_flight_guard.is_empty() {
                                self.in_flight_empty_notify.notify_waiters();
                            }
                        }
                        // #171 sibling: route through `commit_with_inner_miss_gate`
                        // so the OUTER writer survives when the caller has
                        // opted in via `INNER_MISS_NO_TERMINATE`. The
                        // bad in-flight entry was just evicted; the blob
                        // may still live on slow-tier or peers, so peer-fetch
                        // on the same writer is the recovery path. Same
                        // shape as the mirror_blobs size-mismatch sibling
                        // above.
                        let bytes_before = guard.get_bytes_written();
                        let res: Result<(), Error> = Err(make_err!(
                            Code::NotFound,
                            "in_flight_slow_writes entry for {d} had wrong total \
                             size ({total_len} != {expected}) — entry removed"
                        ));
                        commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
                        return res;
                    }
                }
                let offset_usize = usize::try_from(offset)
                    .err_tip(|| "Could not convert offset to usize")?;
                let end = length
                    .and_then(|l| usize::try_from(l).ok())
                    .map(|l| (offset_usize.saturating_add(l)).min(total_len))
                    .unwrap_or(total_len);
                if offset_usize < end {
                    // Walk the chunk list, skipping/slicing to honor offset and length.
                    let mut pos: usize = 0;
                    for chunk in &chunks {
                        let chunk_end = pos + chunk.len();
                        if chunk_end <= offset_usize {
                            pos = chunk_end;
                            continue;
                        }
                        if pos >= end {
                            break;
                        }
                        let start_in_chunk = offset_usize.saturating_sub(pos);
                        let end_in_chunk = (end - pos).min(chunk.len());
                        guard
                            .send(chunk.slice(start_in_chunk..end_in_chunk))
                            .await
                            .err_tip(|| "Failed to send in-flight data in fast_slow get_part")?;
                        pos = chunk_end;
                    }
                }
                guard
                    .commit_eof()
                    .err_tip(|| "Failed to send EOF for in-flight data")?;
                debug!(
                    ?key,
                    data_len = total_len,
                    "Served blob from in-flight slow-write buffer (fast store evicted it)",
                );
                return Ok(());
            }
        }

        // #212 Phase 2.5: read-cascade step 2 — `failed_writes` per-chunk
        // pin (design §6.3). If the chunked-write handler currently has a
        // `ChunkedDriver` in flight for this digest, its in-memory
        // per-chunk pin may cover the requested byte range BEFORE the
        // slow-store rename completes. Serve from the pin if covered;
        // otherwise fall through to the slow store.
        //
        // Behind two gates per the operator-rollback contract:
        //   1. `chunked_fast_slow` cargo feature (compile-time gate).
        //   2. `chunked_reads_enabled` AtomicBool kill-switch (runtime
        //      gate). With either gate OFF the cascade is byte-identical
        //      to origin/main — the registry is never consulted, the
        //      driver pin is never read.
        //
        // Per design §6.3: only digest-keyed reads are eligible (chunked
        // writes are always digest-keyed). String-keyed reads (AC) skip
        // this step entirely.
        #[cfg(feature = "chunked_fast_slow")]
        if self.chunked_reads_enabled.load(Ordering::Relaxed)
            && let StoreKey::Digest(digest) = key.borrow()
        {
            // #220 D7: lock-free read of the registry handle from the
            // post-startup `OnceLock`. One Arc::clone, no contention.
            let registry_snapshot = self.chunked_read_registry.get().cloned();
            if let Some(registry) = registry_snapshot {
                if let Some(driver) = registry.get(&digest) {
                    // Compute the requested byte length: when `length`
                    // is `None` the caller wants the rest of the blob
                    // from `offset`. The driver's accessor is
                    // all-or-nothing for the requested range — partial
                    // coverage falls through.
                    let want_len = length.unwrap_or_else(|| {
                        digest.size_bytes().saturating_sub(offset)
                    });
                    if let Some(bytes) = driver.try_get_chunk_from_pin(offset, want_len) {
                        // Pin hit — serve the whole assembled range
                        // from memory. Atomic counter; no awaits inside
                        // the registry critical section.
                        registry.record_pin_hit();
                        let bytes_len = bytes.len();
                        if !bytes.is_empty() {
                            guard
                                .send(bytes)
                                .await
                                .err_tip(|| "Failed to send chunked-pin data in fast_slow get_part")?;
                        }
                        guard
                            .commit_eof()
                            .err_tip(|| "Failed to send EOF for chunked-pin data")?;
                        debug!(
                            ?key,
                            offset,
                            ?length,
                            served_bytes = bytes_len,
                            "Served blob from #212 chunked-driver pin (slow-store commit not yet complete)",
                        );
                        return Ok(());
                    }
                    // #252 P5 fix: driver registered but range not yet
                    // covered. Falling through to slow tier returns
                    // NotFound (file not renamed until commit) which
                    // poisons EC's stale-positive detector, evicting
                    // the (legitimate) cached Some and cycling: 5,549
                    // events / 50min observed in production 2026-05-04.
                    //
                    // Return Code::Unavailable instead. EC's
                    // is_unrecoverable_read_error filter (existence_
                    // cache_store.rs:567-574) explicitly excludes
                    // Unavailable as "transient — leaves the cache
                    // alone". Bazel's RemoteRetrier treats Unavailable
                    // as retryable per gRPC convention; the next
                    // attempt typically finds the chunks landed.
                    registry.record_pin_partial_miss();
                    return Err(make_err!(
                        Code::Unavailable,
                        "chunked-pin partial miss: driver in flight but range not yet covered; \
                         retry after chunked write completes (#252)"
                    ));
                } else {
                    // Registry installed but no entry for this digest →
                    // no chunked write in flight; fall through normally.
                    registry.record_pin_miss();
                }
            }
        }

        // Worker public CAS server variant — see `local_only_reads` field
        // comment for the recursive-wedge rationale. Mirror + fast +
        // in-flight have all missed; returning NotFound forces the asking
        // server to try a different peer rather than looping the request
        // back through this worker's slow tier.
        if self.local_only_reads.load(Ordering::Relaxed) {
            debug!(
                ?key,
                "local_only_reads: returning NotFound instead of falling through to slow store"
            );
            // #171 sibling: route through `commit_with_inner_miss_gate`
            // so the OUTER writer survives when the caller has opted in
            // via `INNER_MISS_NO_TERMINATE`. The ENTIRE PURPOSE of
            // `local_only_reads` is to force the asking server to "try
            // a different peer rather than looping the request back
            // through this worker's slow tier." When the upstream
            // `WorkerProxyStore` is wired for peer-fetch, that is
            // EXACTLY the recovery path `local_only_reads` is designed
            // to enable; terminating the OUTER writer here defeats the
            // design intent. When the gate is NOT set, the helper
            // falls through to `commit_delegated_if_ok(&Err)` so the
            // VerifyStore deadlock-defense stays armed.
            let bytes_before = guard.get_bytes_written();
            let res: Result<(), Error> = Err(make_err!(
                Code::NotFound,
                "FastSlowStore local_only_reads: blob not present on this worker"
            ));
            commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
            return res;
        }

        // If the fast store is noop or read only or update only then bypass it.
        if self
            .fast_store
            .inner_store(Some(key.borrow()))
            .optimized_for(StoreOptimizations::NoopUpdates)
            || self.fast_direction == StoreDirection::ReadOnly
            || self.fast_direction == StoreDirection::Update
        {
            self.metrics
                .slow_store_hit_count
                .fetch_add(1, Ordering::Acquire);
            // The slow_store's get_part contract terminates the writer on
            // Ok (EOF). On Err the contract is also termination, but a
            // sub-store contract violation (Err without send_error) would
            // silently deadlock paired readers without the Drop fallback.
            // Use `commit_with_inner_miss_gate` so the safety net stays
            // armed on Err — except when the caller has set
            // `INNER_MISS_NO_TERMINATE` and the err is NotFound with no
            // bytes written, in which case the upstream caller owns
            // peer-fetch recovery on the still-open OUTER writer (#171
            // parallel-path coverage).
            let bytes_before = guard.get_bytes_written();
            let res = self
                .slow_store
                .get_part(key, &mut *guard, offset, length)
                .await;
            commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
            res?;
            self.metrics
                .slow_store_downloaded_bytes
                .fetch_add(guard.get_bytes_written(), Ordering::Acquire);
            return Ok(());
        }

        // Spawn the producer if we're the first caller for this key
        // (no-op otherwise). Capture `is_populator_caller` to preserve
        // the pre-fix asymmetry on errors: the populator's caller
        // propagates errors directly (matching the prior
        // `loader.get_or_try_init(populate).await?` semantics), while
        // waiters fall back to the slow store as they always have
        // (covers genuine sliding-window evictions and rare upstream
        // failures the populator's caller would not retry from).
        //
        // TODO(fast_slow_asymmetry): Consider unifying the two paths
        // and always falling through to the slow store on streaming
        // buffer errors (post-fix the spawn-detach guarantees the
        // streaming buffer terminates with the producer's structured
        // error rather than Drop's generic Internal, so the populator
        // caller would also get a usable error from the slow-store
        // fallback). Requires updating the failpoint test suite that
        // currently asserts populator-vs-waiter error semantics.
        let arc_self = self.get_arc().ok_or_else(|| {
            make_err!(Code::Internal, "FastSlowStore dropped during get_part")
        })?;
        let (streaming_inner, is_populator_caller) =
            Self::spawn_populate_producer_with_role(arc_self, key.borrow());

        // If the producer already finished, branch on its terminal state:
        //
        // - Producer Err NotFound: the producer's `send_error` carries the
        //   structured upstream failure (typically NotFound from
        //   slow_store.has() in `run_producer`). The fast store was
        //   never populated, so probing it would always return NotFound
        //   and the slow-store fallback would re-issue the same has()
        //   that the producer just failed on. Returning the producer's
        //   error directly skips both wasted RPCs AND the previously
        //   misleading "fast store item evicted after populate" warn,
        //   which fired for every waiter on every failed-populate digest
        //   (1825 fires / 3 stale-positive digests / 20 min observed in
        //   production on 2026-04-24).
        //
        // - Producer Err non-NotFound (Internal "writer dropped",
        //   Aborted, Unavailable): transient stream-level failure where
        //   the blob may still be present in slow_store. Fall through
        //   to the slow-store fallback so a recoverable read can succeed.
        //   In production we observe ~13 "writer dropped" events / 2hr
        //   on buildcache that benefit from this fallback.
        //
        // - Producer Ok: the fast store HAS the data unless evicted
        //   between producer-EOF and this read. Probe; on NotFound
        //   fall back to slow with the (now-accurate) eviction warn.
        if streaming_inner.is_terminal() {
            if let Some(Err(producer_err)) = streaming_inner.terminal_result() {
                if producer_err.code == Code::NotFound {
                    debug!(
                        ?key,
                        code = ?producer_err.code,
                        "populate already failed with NotFound, returning producer error directly"
                    );
                    // #171: when wrapped by `WorkerProxyStore`, the outer
                    // writer is reused for peer-fetch fallback. Calling
                    // `guard.fail` here closes the outer channel and the
                    // peer's bytes can never be delivered. WorkerProxyStore
                    // sets `INNER_MISS_NO_TERMINATE = true` for exactly this
                    // case; honor it by returning the structured Err WITHOUT
                    // terminating the writer. The caller owns downstream
                    // termination from this point on.
                    let no_terminate = INNER_MISS_NO_TERMINATE
                        .try_with(|v| *v)
                        .unwrap_or(false);
                    if no_terminate {
                        // Suppress the WriteHalfGuard Drop fallback — the
                        // caller (WorkerProxyStore) is now responsible for
                        // terminating the writer (with peer bytes on
                        // success, or with its own send_error on final
                        // peer-fetch failure when its outer fn returns).
                        guard.commit_delegated_if_ok(&Ok::<(), Error>(()));
                        return Err(producer_err);
                    }
                    return Err(guard.fail(producer_err));
                }
                // Non-NotFound terminal Err (Code::Internal "writer
                // dropped", Aborted, Unavailable, etc.) — fall through
                // to the slow-store fallback below; the blob may still
                // be present even though the producer's stream failed.
            }
            let bytes_before = guard.get_bytes_written();
            // Sub-call terminates the writer on Ok; the gate-aware
            // commit at the bottom keeps the Drop fallback armed on Err
            // (catching sub-store contract violations) EXCEPT when the
            // caller set `INNER_MISS_NO_TERMINATE` and the err is
            // NotFound with no bytes written — in which case the
            // upstream caller owns peer-fetch recovery (#171
            // parallel-path coverage).
            let res = match self
                .fast_store
                .get_part(key.borrow(), &mut *guard, offset, length)
                .await
            {
                Ok(()) => Ok(()),
                Err(err)
                    if err.code == Code::NotFound
                        && guard.get_bytes_written() == bytes_before =>
                {
                    warn!(
                        ?key,
                        "fast store item evicted after populate, reading from slow store"
                    );
                    self.slow_store
                        .get_part(key.borrow(), &mut *guard, offset, length)
                        .await
                }
                Err(err) => {
                    // Terminate the writer before returning so callers
                    // (e.g. VerifyStore's tokio::join! over a tx/rx pair)
                    // don't deadlock awaiting EOF/error.
                    Err(guard.fail(err))
                }
            };
            commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
            return res;
        }

        // For blobs larger than the sliding window, early chunks may
        // have been evicted before we could read them. If our cursor is
        // already past chunk 0, fall back to the slow store directly.
        let earliest = streaming_inner.earliest_chunk_idx();
        if earliest > 0 {
            debug!(
                ?key,
                earliest,
                "streaming populate: chunks evicted, falling back to slow store"
            );
            let bytes_before = guard.get_bytes_written();
            let res = self
                .slow_store
                .get_part(key.borrow(), &mut *guard, offset, length)
                .await;
            commit_with_inner_miss_gate(&mut guard, &res, bytes_before);
            return res;
        }

        debug!(
            ?key,
            is_populator_caller,
            "streaming populate: reading concurrently from populate buffer"
        );
        let mut reader = nativelink_util::streaming_blob::StreamingBlobReader::new(
            streaming_inner,
        );
        let mut pos = 0u64;
        let end = offset + length.unwrap_or(u64::MAX);
        loop {
            match reader.next_chunk().await {
                Ok(chunk) if chunk.is_empty() => break, // EOF
                Ok(chunk) => {
                    let chunk_end = pos + chunk.len() as u64;
                    if chunk_end > offset && pos < end {
                        let start = if pos < offset {
                            (offset - pos) as usize
                        } else {
                            0
                        };
                        let stop = if chunk_end > end {
                            chunk.len() - (chunk_end - end) as usize
                        } else {
                            chunk.len()
                        };
                        if start < stop {
                            guard
                                .send(chunk.slice(start..stop))
                                .await
                                .err_tip(|| "Failed to send streaming populate data")?;
                        }
                    }
                    pos = chunk_end;
                    if pos >= end {
                        break;
                    }
                }
                Err(err) => {
                    if is_populator_caller {
                        // Pre-fix populator semantics: errors propagate
                        // directly, no slow-store fallback. Match the
                        // prior `loader.get_or_try_init(populate).await?`
                        // behavior so existing failpoint tests and
                        // user-visible error contracts hold.
                        //
                        // #171 sibling: same INNER_MISS_NO_TERMINATE gate
                        // as the terminal-state branch above. Only honor
                        // for NotFound AND when no bytes have been written
                        // yet (`get_bytes_written() == 0`) — once any
                        // bytes have flowed to the outer writer, peer-
                        // fetch cannot recover without producing a
                        // corrupt-prefix-from-populate + full-peer-copy
                        // stream. WorkerProxyStore already enforces this
                        // bytes_written_by_inner > 0 → no-fallback rule;
                        // mirror it here so the writer-termination skip
                        // is symmetric with the peer-fetch eligibility.
                        let bytes_written = guard.get_bytes_written();
                        let no_terminate = err.code == Code::NotFound
                            && bytes_written == 0
                            && INNER_MISS_NO_TERMINATE
                                .try_with(|v| *v)
                                .unwrap_or(false);
                        if no_terminate {
                            guard.commit_delegated_if_ok(&Ok::<(), Error>(()));
                            return Err(err)
                                .err_tip(|| "populate failed for the requesting caller");
                        }
                        return Err(guard.fail(err)).err_tip(|| {
                            "populate failed for the requesting caller"
                        });
                    }
                    // Waiter path: streaming buffer error (producer
                    // errored or cursor fell behind sliding window).
                    // Fall back to slow store with offset adjusted to
                    // account for any bytes already sent — sending
                    // duplicates would trip VerifyStore.
                    //
                    // After the spawn-detach fix, this branch should
                    // no longer fire for caller cancellation (which was
                    // the production WARN flood); only genuine producer
                    // errors or sliding-window evictions reach it.
                    //
                    // #171 parallel-path coverage: a parallel-chunk
                    // request that races multiple `get_part` calls for
                    // the same digest through a single FastSlowStore
                    // turns ALL but one caller into WAITERS here. The
                    // populator-NotFound flowing through the streaming
                    // buffer wakes every waiter at this Err arm; the
                    // waiter's slow-store fallback then ALSO returns
                    // NotFound (slow tier really is empty), and the
                    // pre-fix `commit_delegated_if_ok(&Err)` left the
                    // Drop fallback armed → terminating the OUTER
                    // writer. Each waiter's WPS would then see the
                    // inner-NotFound and fall through to peer-fetch on
                    // a now-closed writer. Honor the gate here so the
                    // waiter's OUTER writer survives for peer-fetch.
                    let bytes_already_sent = guard.get_bytes_written();
                    let new_offset = offset + bytes_already_sent;
                    let new_length = length.map(|l| l.saturating_sub(bytes_already_sent));
                    warn!(
                        ?key,
                        %err,
                        bytes_already_sent,
                        new_offset,
                        "streaming populate reader error, falling back to slow store"
                    );
                    let res = self
                        .slow_store
                        .get_part(key.borrow(), &mut *guard, new_offset, new_length)
                        .await;
                    commit_with_inner_miss_gate(
                        &mut guard,
                        &res,
                        bytes_already_sent,
                    );
                    return res;
                }
            }
        }
        guard
            .commit_eof()
            .err_tip(|| "Failed to send EOF after streaming populate")?;
        Ok(())
    }

    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        // Worker public CAS server variant — see `local_only_reads` field
        // comment. Route per-key through `get_part_unchunked` so each
        // fetch consults mirror + fast + in-flight before short-circuiting
        // to NotFound (no slow-store fallthrough). The fast-batch shortcut
        // below would skip the mirror map and fan out to the slow tier on
        // miss, both of which we must avoid here. The Semaphore caps
        // memory committed to concurrent fetches: each `get_part_unchunked`
        // can hold ~72 MiB of in-flight buffers (24-slot buf_channel × ~3
        // MiB chunks plus a `BytesMut`), so an unbounded fan-out on a
        // 100-key batch could commit ~7 GiB.
        if self.local_only_reads.load(Ordering::Relaxed) {
            let n = keys.len();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(
                LOCAL_ONLY_READS_BATCH_CONCURRENCY,
            ));
            let futs: FuturesUnordered<_> = keys
                .into_iter()
                .enumerate()
                .map(|(idx, key)| {
                    let semaphore = Arc::clone(&semaphore);
                    async move {
                        // Permit acquisition cannot fail: we never close
                        // the semaphore.
                        let _permit = semaphore.acquire_owned().await.expect(
                            "LOCAL_ONLY_READS_BATCH_CONCURRENCY semaphore is never closed",
                        );
                        let result = self.get_part_unchunked(key, 0, length).await;
                        (idx, result)
                    }
                })
                .collect();
            let mut results: Vec<Result<Bytes, Error>> =
                vec![Err(make_err!(Code::Internal, "batch slot not filled")); n];
            let mut stream = futs;
            while let Some((idx, result)) = stream.next().await {
                results[idx] = result;
            }
            return results;
        }

        // Try the fast store batch first.
        let mut results = Pin::new(self.fast_store.as_store_driver())
            .batch_get_part_unchunked(keys.iter().map(|k| k.borrow()).collect(), length)
            .await;

        // Collect indices that missed in fast store for slow store fallback.
        let mut slow_indices: Vec<usize> = Vec::new();
        let mut slow_keys: Vec<StoreKey<'_>> = Vec::new();
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                if e.code == Code::NotFound {
                    slow_indices.push(i);
                    slow_keys.push(keys[i].borrow());
                }
            }
        }

        if !slow_indices.is_empty() {
            let slow_results = Pin::new(self.slow_store.as_store_driver())
                .batch_get_part_unchunked(slow_keys, length)
                .await;
            for (slot, slow_result) in slow_indices.into_iter().zip(slow_results) {
                results[slot] = slow_result;
            }
        }

        results
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // Composite registration is not atomic: if the second register fails
        // after the first succeeds, the inner stores are left in an asymmetric
        // state (one has the callback, the other doesn't). The StoreDriver
        // trait does not currently expose `unregister_item_callback`, so we
        // cannot roll back the partial registration. Most impls are infallible
        // (Vec::push under a lock), so this rarely fires in practice. If it
        // does, log loudly so the operator can detect the asymmetry and
        // restart the process.
        self.fast_store.register_item_callback(callback.clone())?;
        if let Err(err) = self.slow_store.register_item_callback(callback) {
            warn!(
                ?err,
                "FastSlowStore: slow_store register_item_callback failed AFTER fast_store \
                 succeeded — composite is in an asymmetric state (fast has the callback, \
                 slow does not). Trait has no unregister API; restart to recover."
            );
            return Err(err);
        }
        Ok(())
    }

    /// FastSlowStore IS the leaf for stable digests — `populate_fast_store`
    /// pushes into `self.stable_digests` whenever a slow-store write
    /// completes. Inner stores (Memory + Filesystem) do not contribute
    /// independently. Hence `Leaf` + the two overrides below that read
    /// FastSlowStore's owned state directly.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// FastSlowStore fans the pin out to BOTH fast and slow inner stores
    /// (the slow one because in worker `FastSlowStore { fast: Memory, slow:
    /// Filesystem }` the actual pin lives on the slow tier). Hence `Many`.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        let mut children = DelegationChildren::new();
        children.push(self.fast_store.as_store_driver());
        children.push(self.slow_store.as_store_driver());
        PinDelegation::Many(children)
    }

    fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        let mut guard = self.stable_digests.lock();
        std::mem::take(&mut *guard)
    }

    fn stable_notify(&self) -> Arc<Notify> {
        self.stable_notify.clone()
    }

    /// FastSlowStore IS the producer leaf for `mark_stable` — it owns the
    /// BIS feeder queue. `Leaf` makes the trait default a no-op; the
    /// override below pushes into `self.stable_digests` directly. Inner
    /// stores (Memory + Filesystem) do not contribute independently.
    /// (Task #157 / C+D folded mark_stable into the forced-delegation
    /// enum mechanism.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }

    /// Push the given digests into the BIS feeder queue and wake the
    /// broadcast loop. Used by the worker API server's BlobsAvailable
    /// handler to cover every path where the server has a digest
    /// stably without the existing `update_oneshot` success-arm having
    /// fired (deduplicated uploads, tree-children, mirror_blobs already
    /// stably stored, etc.). Without this hook the worker's pin
    /// (durable under pin v2) would leak forever for those paths.
    /// Idempotent: the broadcast loop dedups downstream and the
    /// worker's `unpin_digest` is itself idempotent.
    fn mark_stable(&self, digests: &[DigestInfo]) {
        if digests.is_empty() {
            return;
        }
        {
            let mut guard = self.stable_digests.lock();
            guard.extend_from_slice(digests);
        }
        self.stable_notify.notify_one();
    }


    fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        let mut guard = self.failed_slow_writes.lock();
        guard.drain().collect()
    }
}

#[derive(Debug, Default, MetricsComponent)]
struct FastSlowStoreMetrics {
    #[metric(help = "Hit count for the fast store")]
    fast_store_hit_count: AtomicU64,
    #[metric(help = "Downloaded bytes from the fast store")]
    fast_store_downloaded_bytes: AtomicU64,
    #[metric(help = "Hit count for the slow store")]
    slow_store_hit_count: AtomicU64,
    #[metric(help = "Downloaded bytes from the slow store")]
    slow_store_downloaded_bytes: AtomicU64,
    /// Counts every `tokio::spawn` issued by the populate machinery in
    /// `spawn_populate_producer_with_role`. The inline-fast-path in
    /// `copy_slow_to_fast` keeps this counter unchanged for single-
    /// caller cache-miss populates; only `get_part` (cancellation-prone
    /// gRPC streaming reads) bumps it. Used by
    /// `populate_inline_does_not_spawn` to pin the optimisation against
    /// regression.
    #[metric(help = "Count of tokio::spawn issued by the populate machinery")]
    populate_spawn_count: AtomicU64,
}

impl Drop for FastSlowStore {
    fn drop(&mut self) {
        let guard = self.in_flight_slow_writes.lock();
        if guard.is_empty() {
            return;
        }
        warn!(
            count = guard.len(),
            "FastSlowStore: dropping with in-flight slow writes, \
             these blobs will NOT be persisted to the slow store"
        );
        for (key, chunks) in guard.iter() {
            let bytes: usize = chunks.iter().map(|b| b.len()).sum();
            warn!(?key, bytes, "FastSlowStore: unflushed write lost on shutdown");
        }
    }
}

default_health_status_indicator!(FastSlowStore);
