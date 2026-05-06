// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! Bug A small-CAS peer-mirror dispatcher (SKELETON).
//!
//! Spec: `.claude/plans/bug-a-small-cas-peer-mirror.md`.
//!
//! Today the small-blob short-circuit at `bytestream_server.rs:2059, 2109-2189`
//! lies to Bazel about workers holding small blobs they do not actually hold.
//! The `SmallBlobDispatcher` extends the existing peer-mirror infrastructure
//! to small CAS+AC blobs (≤ `SMALL_BLOB_THRESHOLD`) by piggybacking byte-push
//! on the existing `UpdateForWorker` bidi stream as a new
//! `BatchWriteSmallBlobs` variant. The dispatcher makes the Bazel "lie"
//! eventually-true within ~RTT.
//!
//! # Status
//!
//! This is a SKELETON. It implements:
//!
//! 1. The `EphemeralServerSidePin` per-FastSlowStore pin set with insert,
//!    cap-checked admission, sorted-by-store_id ack-and-unpin, and `len /
//!    total_bytes` accounting.
//! 2. The `SmallBlobDispatcher` struct + `enqueue` precondition checks
//!    (size, store_id format).
//! 3. The `SMALL_BLOB_THRESHOLD` constant (16 KiB per the plan).
//!
//! It does NOT implement (deferred to follow-up commits — see plan §
//! "Order of operations"):
//!
//! - The per-`(endpoint, boot_epoch_id, store_id)` mpsc + drainer task
//!   (steps 2-4 of the plan).
//! - Wiring to `bytestream_server::inner_write_oneshot:1946`,
//!   `cas_server::inner_batch_update_blobs:437`,
//!   `ac_server::inner_update_action_result:184` (step 6).
//! - `WorkerApiServer::handle_blobs_available` extension to broadcast
//!   `pinned_mirror_entries` (step 4).
//! - The `LOCALITY_MIN_BLOB_SIZE = 64*1024` removal at
//!   `bytestream_server.rs:2109` (step 9, gated by feature flag).
//! - `TimedDispatchCall<F>` instrumentation (B6).
//!
//! # Naming
//!
//! Per C12: server-side pin storage type is named `EphemeralServerSidePin`
//! (NOT `mirror_blobs`-shaped naming) to prevent future readers conflating
//! this with the worker-side `mirror_blobs` map or the v2 pin redesign.
//!
//! # Concurrency
//!
//! - `EphemeralServerSidePin`: a `parking_lot::Mutex<HashMap<DigestInfo,
//!   Bytes>>` plus an `AtomicU64` for total bytes. `total_bytes` is
//!   updated WHILE the state lock is held so concurrent observers
//!   never see a torn `(len, total_bytes)` pair (perf-optimizer #153
//!   MAJOR + red-team B4).
//! - The pin set holds `Bytes` (Arc-counted), so insert / remove is O(1).
//! - `observe_pinned_mirror_ack` does a single binary search over the
//!   sorted-by-store_id `entries` slice and removes ONLY the entries
//!   whose `store_id` matches `self.store_id`. Other stores' broadcasts
//!   are O(log N) no-ops.
//! - `unpin_on_disconnect` clears every entry on every registered
//!   store. The dispatcher does NOT track per-worker push attribution
//!   in v1 — see `SmallBlobDispatcher::unpin_on_disconnect` doc for the
//!   correctness argument and follow-up TODO.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BatchWriteSmallBlobsRequest, MirrorPinEntry, SmallBlobEntry, UpdateForWorker,
    update_for_worker::Update as UpdateForWorkerUpdate,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreDriver, StoreKey};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::completeness_checking_store::CompletenessCheckingStore;
use crate::existence_cache_store::ExistenceCacheStore;
use crate::fast_slow_store::FastSlowStore;
use crate::verify_store::VerifyStore;

/// The maximum blob size (bytes) that the dispatcher accepts. Larger blobs
/// take the existing streaming path (`worker_proxy_store::mirror_blob_to_random_worker`).
///
/// Doc-tied to 16 KiB by `small_blob_dispatcher_test::small_blob_threshold_is_16kib`;
/// changing this constant must be explicit and reviewed.
pub const SMALL_BLOB_THRESHOLD: usize = 16 * 1024;

/// Default: feature flag is off until the dispatcher is fully wired and
/// canary-validated.
const DEFAULT_SMALL_BLOB_MIRROR_ENABLED: bool = false;

/// Default per-(worker, store) mpsc capacity. Per C3 (zero-window coalesce)
/// steady-state depth is 0-1; spike depth bounded by network RTT × producer
/// rate. 32 is generous for typical RTT 1ms × 30K writes/sec.
const DEFAULT_MAX_PENDING_PER_WORKER: usize = 32;

/// Default per-batch byte ceiling. Bounded gRPC message size. ~16 entries at
/// `SMALL_BLOB_THRESHOLD` max.
const DEFAULT_MAX_BATCH_BYTES: usize = 256 * 1024;

/// Default per-FastSlowStore pin set byte cap. Two stores (CAS + AC) →
/// 512 MiB worst-case per server. Per-worker × num_workers × per-store =
/// aggregate memory budget; size for production fleet.
const DEFAULT_PIN_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

/// Operator-tunable knobs for the dispatcher. Mirrors the "Knobs" table in
/// the plan; defaults match production-canary plan rec.
///
/// Per the unpin_on_disconnect refactor (red-team #153 B3 + testing-czar
/// #153 MAJOR-1 + testing-czar #168 MAJOR-2): there is intentionally NO
/// `pin_ttl`. Pin entries are durable until one of:
///
/// 1. The worker advertises them in `BlobsAvailable.pinned_mirror_entries`
///    (proto field 16) and `observe_pinned_mirror_ack` removes them.
/// 2. The dispatcher's `unpin_on_disconnect(endpoint, boot_epoch_id)` is
///    called from `WorkerApiServer`'s disconnect-cleanup task, which
///    drops every entry in every registered pin set.
///
/// Memory bound is `pin_max_bytes`. A previous design had a `pin_ttl`
/// field with no purge loop attached — the field was dead weight that
/// invited a leak (a worker that disconnected without acking would leak
/// its pin entries indefinitely). The v2 pin protocol requires durable
/// pins until explicit unpin; a TTL is incompatible.
#[derive(Clone, Debug)]
pub struct SmallBlobDispatcherConfig {
    /// Master feature flag. `false` ⇒ all `enqueue` calls are no-ops; no
    /// drainer tasks spawned; no telemetry counters tick. Operator MUST
    /// explicitly enable for canary rollout.
    pub small_blob_mirror_enabled: bool,
    /// Per-(worker, store) bounded mpsc capacity. See
    /// `DEFAULT_MAX_PENDING_PER_WORKER`.
    pub max_pending_per_worker: usize,
    /// Per-batch byte ceiling. See `DEFAULT_MAX_BATCH_BYTES`.
    pub max_batch_bytes: usize,
    /// Per-FastSlowStore pin set byte cap. See `DEFAULT_PIN_MAX_BYTES`.
    pub pin_max_bytes: u64,
}

impl Default for SmallBlobDispatcherConfig {
    fn default() -> Self {
        Self {
            small_blob_mirror_enabled: DEFAULT_SMALL_BLOB_MIRROR_ENABLED,
            max_pending_per_worker: DEFAULT_MAX_PENDING_PER_WORKER,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            pin_max_bytes: DEFAULT_PIN_MAX_BYTES,
        }
    }
}

/// Server-side pin tracking for bytes the dispatcher pushed to a worker
/// that the worker has not yet ack'd.
///
/// Lifetime: durable until explicit unpin (via `observe_pinned_mirror_ack`
/// when the worker advertises the digest in
/// `BlobsAvailable.pinned_mirror_entries`, OR via `unpin_on_disconnect`
/// when `WorkerApiServer` notices the worker disconnected). NOT
/// TTL-evicted. Memory bound is `pin_max_bytes` (per-store).
///
/// Each entry (`DigestInfo` -> `Bytes`) costs `data.len()` plus a small
/// fixed overhead. Cap is per-store (`pin_max_bytes`) so two stores
/// (CAS + AC) sum to `2 × pin_max_bytes` per server.
///
/// **NOT** the v2 worker-side pin contract; **NOT** the worker-side
/// `mirror_blobs` map. This lives on the SERVER for in-flight push tracking
/// only; durability is preserved by the underlying Redis layer.
///
/// Concurrency: short critical section (HashMap insert / remove + atomic
/// total_bytes update). Used under `parking_lot::Mutex`. Lock order vs the
/// dispatcher Mutex: dispatcher Mutex BEFORE per-store pin-set Mutex.
/// `observe_pinned_mirror_ack` MUST NOT call back into the dispatcher
/// under the pin-set lock (no back-edge).
///
/// Per perf-optimizer #153 MAJOR + red-team B4: every method that mutates
/// the HashMap also updates `total_bytes` BEFORE releasing the state
/// lock. Decoupling the two (e.g. `drop(state); fetch_sub(...)`) was the
/// previous design and introduced a lost-update race where a concurrent
/// reader saw `len = 0` AND `total_bytes != 0`, OR a concurrent insert
/// admitted a payload that put the actual byte total above `cap`.
pub struct EphemeralServerSidePin {
    cap: u64,
    state: Mutex<HashMap<DigestInfo, Bytes>>,
    total_bytes: AtomicU64,
}

impl EphemeralServerSidePin {
    /// Build a new per-FastSlowStore pin set with `cap` bytes of admission
    /// quota.
    pub fn new(cap: u64) -> Self {
        Self {
            cap,
            state: Mutex::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// Returns the cap-projection counter as an `Acquire` atomic load.
    ///
    /// Consistent with `len()` ONLY for callers that hold `state.lock()`
    /// (the same critical section that mutates state also publishes the
    /// new total under `AcqRel`/`Release`). Unsynchronized callers
    /// (metrics, logs, tests) may observe a transient skew of one
    /// in-flight insert/remove operation against the matching `len()`
    /// snapshot — fine for those use-cases (the lock-held atomic-update
    /// fix only guarantees pair-consistency for in-lock observers).
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.state.lock().len()
    }

    /// Configured byte capacity. Read-only accessor for periodic
    /// metrics emit (per #168 dist-systems MAJOR-1).
    pub fn cap(&self) -> u64 {
        self.cap
    }

    /// True if the pin set is empty.
    pub fn is_empty(&self) -> bool {
        self.state.lock().is_empty()
    }

    /// True if the pin set currently holds `digest`.
    pub fn contains(&self, digest: &DigestInfo) -> bool {
        self.state.lock().contains_key(digest)
    }

    /// Insert a new pin entry. Rejects with `Code::ResourceExhausted` if
    /// the new total would exceed `cap`. The bytes are held under the
    /// per-store lock; caller's `Bytes` clone is O(1) refcount-bump.
    ///
    /// `total_bytes` is updated WHILE the state lock is held so that a
    /// concurrent reader cannot observe a torn `(len, total_bytes)` pair
    /// (per perf-optimizer #153 MAJOR + red-team B4).
    pub fn insert(&self, digest: DigestInfo, data: Bytes) -> Result<(), Error> {
        let new_bytes = data.len() as u64;
        let mut state = self.state.lock();
        // `Acquire` keeps the load consistent with the matching
        // `Release` / `AcqRel` on every other writer (per code-reviewer
        // #168 NIT-2: pick ONE ordering and use it everywhere).
        let current_total = self.total_bytes.load(Ordering::Acquire);
        // Subtract old entry's size if we're replacing.
        let old_size = state.get(&digest).map(|d| d.len() as u64).unwrap_or(0);
        let projected = current_total.saturating_sub(old_size).saturating_add(new_bytes);
        if projected > self.cap {
            drop(state);
            warn!(
                %digest,
                new_bytes,
                current_total,
                cap = self.cap,
                "EphemeralServerSidePin: cap exceeded; rejecting insert"
            );
            return Err(make_err!(
                Code::ResourceExhausted,
                "EphemeralServerSidePin: cap {} exceeded (current_total={current_total}, \
                 new_bytes={new_bytes})",
                self.cap
            ));
        }
        state.insert(digest, data);
        // Update atomic UNDER the state lock so an observer racing a
        // remove cannot see a torn `(len, total_bytes)` pair. `Release`
        // pairs with the `Acquire` reader at `total_bytes()` and the
        // `AcqRel` updater at `remove_one` (consistent ordering per
        // code-reviewer #168 NIT-2).
        self.total_bytes.store(projected, Ordering::Release);
        Ok(())
    }

    /// Remove a single pin entry by digest. No-op if not present.
    ///
    /// `total_bytes` fetch_sub is performed WHILE the state lock is held
    /// (perf-optimizer #153 MAJOR + red-team B4). Releasing the lock
    /// before the atomic update would let a concurrent
    /// `len()` / `total_bytes()` reader observe `len = 0 ∧ total_bytes != 0`
    /// (or worse — a concurrent `insert` race the cap check against
    /// stale `total_bytes` and admit a payload that exceeds `cap`).
    pub fn remove_one(&self, digest: &DigestInfo) {
        let mut state = self.state.lock();
        if let Some(data) = state.remove(digest) {
            let removed = data.len() as u64;
            self.total_bytes.fetch_sub(removed, Ordering::AcqRel);
        }
    }

    /// Drop every pin entry unconditionally and zero the byte accounting.
    ///
    /// Called by `SmallBlobDispatcher::unpin_on_disconnect` when
    /// `WorkerApiServer` notices a worker has disconnected — the
    /// disconnected worker can no longer ack pushed blobs via
    /// `observe_pinned_mirror_ack`, so the in-flight push tracker for
    /// those bytes would otherwise leak forever. The bytes themselves
    /// remain in the slow tier; this only frees the in-flight
    /// server-side push tracker.
    ///
    /// `total_bytes` is updated UNDER the state lock for the same
    /// reason as `remove_one` / `insert` — concurrent readers must
    /// never see a torn `(len, total_bytes)` pair.
    pub fn unpin_on_disconnect(&self) {
        let mut state = self.state.lock();
        let freed: u64 = state.values().map(|b| b.len() as u64).sum();
        state.clear();
        // Zero the atomic UNDER the state lock to keep `len` and
        // `total_bytes` consistent for any concurrent observer.
        self.total_bytes.store(0, Ordering::Release);
        debug!(freed, "EphemeralServerSidePin::unpin_on_disconnect dropped pin entries");
    }

    /// Remove all entries the worker has acked.
    ///
    /// `entries` MUST be sorted by `store_id` ASCII (the worker advertises
    /// from a `BTreeMap`-keyed iterator per plan B5; the proto wire form
    /// preserves order). We binary-search for the contiguous slice
    /// matching `self_store_id` and remove only those entries.
    ///
    /// Routing — Option F (broadcast + self-filter):
    ///   1. The WorkerApiServer broadcasts every `BlobsAvailable` ack to
    ///      EVERY registered FastSlowStore.
    ///   2. Each FastSlowStore calls this method with its own `store_id`.
    ///   3. Stores whose `store_id` is absent from `entries` no-op in
    ///      O(log N). Stores whose `store_id` is present remove their
    ///      slice in O(K log K) where K is the slice length.
    ///
    /// Per C8 mutation: invert the `low / high` bounds; the test
    /// `observe_pinned_mirror_ack_filters_by_store_id` MUST fail because
    /// a wrong-direction binary search silently unpins the wrong store.
    pub fn observe_pinned_mirror_ack(
        &self,
        self_store_id: &str,
        entries: &[MirrorPinEntry],
    ) {
        if entries.is_empty() {
            return;
        }
        // Binary-search for the contiguous slice of entries whose
        // `store_id == self_store_id`. We use lower-bound + upper-bound
        // partition_point calls (stable since Rust 1.52).
        let low = entries.partition_point(|e| e.store_id.as_str() < self_store_id);
        if low == entries.len() || entries[low].store_id != self_store_id {
            // No entries for this store. O(log N) no-op.
            return;
        }
        let high = low + entries[low..].partition_point(|e| e.store_id == self_store_id);
        let slice = &entries[low..high];
        debug!(
            self_store_id,
            slice_len = slice.len(),
            total_entries = entries.len(),
            "EphemeralServerSidePin: ack-and-unpin slice"
        );
        let mut state = self.state.lock();
        let mut freed = 0u64;
        for entry in slice {
            let Some(proto_digest) = entry.digest.as_ref() else {
                warn!(
                    self_store_id,
                    "EphemeralServerSidePin: ack entry has no digest; skipping"
                );
                continue;
            };
            let Ok(digest) = DigestInfo::try_from(proto_digest.clone()) else {
                warn!(
                    self_store_id,
                    "EphemeralServerSidePin: ack entry has invalid digest; skipping"
                );
                continue;
            };
            if let Some(data) = state.remove(&digest) {
                freed += data.len() as u64;
            }
        }
        // Update atomic UNDER the state lock (perf-optimizer #153 MAJOR
        // + red-team B4) — concurrent readers must never see a torn
        // `(len, total_bytes)` pair.
        if freed > 0 {
            self.total_bytes.fetch_sub(freed, Ordering::AcqRel);
        }
    }
}

/// One in-flight blob entry queued for a (endpoint, boot_epoch_id,
/// store_id) worker. The drainer task pops these and packs them into a
/// `BatchWriteSmallBlobsRequest`.
#[derive(Debug, Clone)]
struct DispatchItem {
    digest: DigestInfo,
    data: Bytes,
    /// `store_id` is keyed at the queue level so it does NOT need to
    /// be repeated on each item; we still carry it to populate the
    /// wire-level `SmallBlobEntry.store_id` without extra lookups.
    store_id: Arc<str>,
}

/// One mpsc producer handle for a `(endpoint, boot_epoch_id, store_id)`
/// triple. The dispatcher's outer Mutex guards the lookup; the actual
/// send is bounded `try_send` (no `.await` while holding the Mutex).
#[derive(Clone)]
struct PerWorkerStoreState {
    sender: mpsc::Sender<DispatchItem>,
}

/// The server-singleton dispatcher. Manages per-`(endpoint, boot_epoch_id,
/// store_id)` bounded mpsc queues and drainer tasks per plan §"Concurrency
/// design".
///
/// Concurrency invariants (per plan):
/// - Dispatcher Mutex BEFORE per-store pin-set Mutex (prevents AB/BA).
/// - Drainer holds NO locks during send (S3 future-regression guard).
/// - `worker_tx: UnboundedSender<UpdateForWorker>` is sync `send()`; no
///   `.await` happens between the drainer's mpsc `recv()` and the worker
///   send.
pub struct SmallBlobDispatcher {
    config: SmallBlobDispatcherConfig,
    /// Per-`(endpoint, boot_epoch_id, store_id)` mpsc Sender. Lookup is
    /// Mutex-guarded (short critical section). On a miss the enqueue is
    /// silently dropped — production callers are post-Bazel-ack
    /// fire-and-forget and MUST NOT block on registration races.
    queues: Mutex<HashMap<(Arc<str>, u64, Arc<str>), PerWorkerStoreState>>,
    /// Per-`(endpoint, boot_epoch_id)` mpsc::UnboundedSender that the
    /// drainer task sends `UpdateForWorker` messages into. Registered by
    /// `WorkerApiServer::inner_connect_worker`.
    worker_txs: Mutex<HashMap<(Arc<str>, u64), mpsc::UnboundedSender<UpdateForWorker>>>,
    /// Per-`store_id` `EphemeralServerSidePin` set. Populated on
    /// successful enqueue; depleted by `observe_pinned_mirror_ack`.
    pin_sets: Mutex<HashMap<Arc<str>, Arc<EphemeralServerSidePin>>>,
    /// Diagnostic counter: number of `enqueue` calls that successfully
    /// passed precondition + feature-flag gates AND landed in a queue.
    /// Production callers should rely on pin-set telemetry as the primary
    /// health signal (per B6); this counter is for tests + low-frequency
    /// debug.
    dispatched_count: AtomicUsize,
    /// Diagnostic counter: number of `schedule_dispatch_to_all_workers`
    /// calls that fanned out to ≥1 worker. Each call counts once,
    /// regardless of `connected_workers().len()` — pair with
    /// `dispatched_count` (per-worker) to derive the average fan-out.
    /// Used by the periodic metrics task to expose dispatcher activity
    /// in operator logs (per #168 dist-systems MAJOR-1).
    fan_out_count_total: AtomicU64,
    /// Diagnostic counter: number of `enqueue` calls that synchronously
    /// passed gates but were dropped because a per-store pin set hit
    /// `pin_max_bytes`. Visible in periodic metrics; non-zero indicates
    /// a hot store under sustained burst.
    skipped_pin_full_total: AtomicU64,
    /// Diagnostic counter: number of `enqueue` calls that synchronously
    /// passed gates but were dropped because the per-(worker, store)
    /// `try_send` returned `Full`. Non-zero indicates a slow worker
    /// (drainer falling behind RTT × producer rate).
    skipped_queue_full_total: AtomicU64,
}

/// Manual `Debug` impl. Producer servers (`bytestream_server.rs`,
/// `cas_server.rs`, `ac_server.rs`) carry `Option<Arc<Self>>` in
/// `#[derive(Debug)]` structs, which requires `Self: Debug`. The
/// internal state is mostly `Mutex<HashMap>` and is not interesting
/// to log verbatim; we surface the operationally-useful counters
/// instead.
impl core::fmt::Debug for SmallBlobDispatcher {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SmallBlobDispatcher")
            .field("enabled", &self.config.small_blob_mirror_enabled)
            .field("dispatched_count", &self.dispatched_count())
            .field("fan_out_count_total", &self.fan_out_count_total())
            .field("skipped_pin_full_total", &self.skipped_pin_full_total())
            .field("skipped_queue_full_total", &self.skipped_queue_full_total())
            .field("pin_set_count", &self.pin_sets.lock().len())
            .field("worker_tx_count", &self.worker_txs.lock().len())
            .finish_non_exhaustive()
    }
}

impl SmallBlobDispatcher {
    /// Build a new dispatcher with the given config. With the feature
    /// flag off (default), `enqueue` is a no-op.
    pub fn new(config: SmallBlobDispatcherConfig) -> Self {
        Self {
            config,
            queues: Mutex::new(HashMap::new()),
            worker_txs: Mutex::new(HashMap::new()),
            pin_sets: Mutex::new(HashMap::new()),
            dispatched_count: AtomicUsize::new(0),
            fan_out_count_total: AtomicU64::new(0),
            skipped_pin_full_total: AtomicU64::new(0),
            skipped_queue_full_total: AtomicU64::new(0),
        }
    }

    /// True if the master feature flag is on. Producers use this as a
    /// cheap zero-cost gate at the hook site (`if !disp.is_enabled() {
    /// return }`) so the per-blob fan-out path is fully short-circuited
    /// when the dispatcher is canary-disabled — no `connected_workers()`
    /// snapshot, no per-worker validation. The same flag is re-checked
    /// inside `enqueue` (defense in depth), so this accessor is purely a
    /// hot-path optimization.
    pub fn is_enabled(&self) -> bool {
        self.config.small_blob_mirror_enabled
    }

    /// Diagnostic accessor: number of `schedule_dispatch_to_all_workers`
    /// calls that successfully spawned a fan-out task.
    pub fn fan_out_count_total(&self) -> u64 {
        self.fan_out_count_total.load(Ordering::Relaxed)
    }

    /// Diagnostic accessor: number of `enqueue` calls that were dropped
    /// because the per-store pin set was at `pin_max_bytes`.
    pub fn skipped_pin_full_total(&self) -> u64 {
        self.skipped_pin_full_total.load(Ordering::Relaxed)
    }

    /// Diagnostic accessor: number of `enqueue` calls that were dropped
    /// because the per-(worker, store) mpsc `try_send` returned `Full`.
    pub fn skipped_queue_full_total(&self) -> u64 {
        self.skipped_queue_full_total.load(Ordering::Relaxed)
    }

    /// Diagnostic accessor: how many calls passed all gates and landed
    /// in a per-(worker, store) mpsc.
    pub fn dispatched_count(&self) -> usize {
        self.dispatched_count.load(Ordering::Relaxed)
    }

    /// Diagnostic accessor: is a `worker_tx` registered for `(endpoint,
    /// boot_epoch_id)`? Used by tests to assert that the disconnect /
    /// boot-epoch wipe paths cleared per-(endpoint, epoch) state.
    /// Production callers should NOT branch on this — use the registered
    /// `worker_tx`'s send-error result as the authoritative liveness
    /// signal. Gated on `cfg(test)` (in-crate use) and the `test-utils`
    /// feature (cross-crate use, e.g. `nativelink-service` integration
    /// tests).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn has_worker_tx_for_test(&self, endpoint: &str, boot_epoch_id: u64) -> bool {
        let key: Arc<str> = Arc::from(endpoint);
        self.worker_txs.lock().contains_key(&(key, boot_epoch_id))
    }

    /// Snapshot every `(endpoint, boot_epoch_id)` for which a `worker_tx`
    /// is currently registered. Used by tests + diagnostics; producers
    /// use [`Self::connected_workers_with_senders`] to skip the
    /// per-worker `worker_txs.lock()` round-trip (perf-optimizer #168
    /// NIT-1).
    pub fn connected_workers(&self) -> Vec<(Arc<str>, u64)> {
        self.worker_txs
            .lock()
            .keys()
            .map(|(ep, epoch)| (ep.clone(), *epoch))
            .collect()
    }

    /// Snapshot every connected worker plus its `worker_tx` `Sender`
    /// clone. Used by `schedule_dispatch_to_all_workers` to fan-out
    /// without re-locking `worker_txs` per worker (perf-optimizer #168
    /// NIT-1: kill the per-iteration `worker_txs.lock()` round-trip).
    /// O(N) under the `worker_txs` Mutex; N ≤ fleet size (~10 today,
    /// bounded). Snapshots into an owned `Vec` so the caller does not
    /// hold the lock across `.await`.
    pub fn connected_workers_with_senders(
        &self,
    ) -> Vec<(Arc<str>, u64, mpsc::UnboundedSender<UpdateForWorker>)> {
        self.worker_txs
            .lock()
            .iter()
            .map(|((ep, epoch), tx)| (ep.clone(), *epoch, tx.clone()))
            .collect()
    }

    /// Diagnostic accessor: how many per-`(endpoint, boot_epoch_id, *)`
    /// queue entries exist (one per `store_id`). Used by tests to assert
    /// that boot-epoch wipe / disconnect cleared the per-(worker, epoch)
    /// drainer queues. Gated on `cfg(test)` (in-crate use) and the
    /// `test-utils` feature (cross-crate use, e.g. `nativelink-service`
    /// integration tests).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn queue_count_for_worker_for_test(&self, endpoint: &str, boot_epoch_id: u64) -> usize {
        let key: Arc<str> = Arc::from(endpoint);
        self.queues
            .lock()
            .keys()
            .filter(|(ep, epoch, _)| *ep == key && *epoch == boot_epoch_id)
            .count()
    }

    /// Register the `EphemeralServerSidePin` set for a `store_id`. Called
    /// at server startup (one per FastSlowStore that opts into the
    /// dispatcher).
    pub fn register_pin_set(&self, store_id: &str, pin: Arc<EphemeralServerSidePin>) {
        let key: Arc<str> = Arc::from(store_id);
        self.pin_sets.lock().insert(key, pin);
    }

    /// Lookup a registered pin set by `store_id`. Used by the
    /// WorkerApiServer's `handle_blobs_available` to fan out
    /// `pinned_mirror_entries` (field 16) acks (broadcast + self-filter
    /// per plan Option F).
    pub fn pin_set_for(&self, store_id: &str) -> Option<Arc<EphemeralServerSidePin>> {
        self.pin_sets.lock().get(store_id).cloned()
    }

    /// All registered pin sets. Used to broadcast `observe_pinned_mirror_ack`
    /// across every registered FastSlowStore on each `BlobsAvailable` tick.
    /// Returns `(store_id, pin_set)` pairs.
    pub fn all_pin_sets(&self) -> Vec<(Arc<str>, Arc<EphemeralServerSidePin>)> {
        self.pin_sets
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Broadcast a `pinned_mirror_entries` ack (proto field 16) to every
    /// registered per-store `EphemeralServerSidePin`. Each pin set
    /// binary-searches the `entries` slice for its own `store_id` region
    /// and removes confirmed-held entries (Option F: broadcast +
    /// self-filter). Stores whose `store_id` is absent from `entries`
    /// no-op in O(log N).
    ///
    /// Called by `WorkerApiServer::handle_blobs_available` on each
    /// BlobsAvailable tick that carries a non-empty `pinned_mirror_entries`.
    /// Snapshots the registered pin sets under the dispatcher Mutex first,
    /// then releases it before iterating — so the per-pin-set Mutex
    /// acquisitions never compose with the dispatcher Mutex (preserves
    /// the dispatcher-BEFORE-pin-set lock order).
    ///
    /// Per C8 mutation: invert the per-store binary-search bounds and
    /// the unit-level test
    /// `observe_pinned_mirror_ack_filters_by_store_id` MUST fail.
    pub fn broadcast_pinned_mirror_ack(&self, entries: &[MirrorPinEntry]) {
        if entries.is_empty() {
            return;
        }
        let snapshot = self.all_pin_sets();
        debug!(
            entry_count = entries.len(),
            store_count = snapshot.len(),
            "SmallBlobDispatcher::broadcast_pinned_mirror_ack"
        );
        for (store_id, pin_set) in snapshot {
            pin_set.observe_pinned_mirror_ack(store_id.as_ref(), entries);
        }
    }

    /// Register a worker's `UpdateForWorker` sender at connect time. The
    /// dispatcher uses this `worker_tx` from the drainer task to deliver
    /// `BatchWriteSmallBlobs`. On reconnect with a NEW `boot_epoch_id`
    /// (per B4 + worker_api_server.rs:220), call this AGAIN — the new
    /// `(endpoint, new_boot_epoch_id)` entry replaces a stale one,
    /// dropping its sender; old drainer tasks then exit gracefully when
    /// their Sender clones are dropped + their per-(worker, store)
    /// queue's Receiver hits None.
    pub fn register_worker(
        &self,
        endpoint: &str,
        boot_epoch_id: u64,
        worker_tx: mpsc::UnboundedSender<UpdateForWorker>,
    ) {
        let key: Arc<str> = Arc::from(endpoint);
        self.worker_txs.lock().insert((key, boot_epoch_id), worker_tx);
    }

    /// Drop the `worker_tx` for a worker (called at disconnect). Pending
    /// drainers will see their `worker_tx` sends fail and exit.
    pub fn unregister_worker(&self, endpoint: &str, boot_epoch_id: u64) {
        let key: Arc<str> = Arc::from(endpoint);
        self.worker_txs.lock().remove(&(key.clone(), boot_epoch_id));
        // Also drop any per-(worker, store) queues whose Sender we own —
        // the drainer's `recv()` returns None when we drop the Sender,
        // so the drainer task exits. Per B4: stale queue cleanup on
        // disconnect.
        let mut queues = self.queues.lock();
        queues.retain(|(ep, epoch, _), _| !(*ep == key && *epoch == boot_epoch_id));
    }

    /// Drop every pin entry on every registered store after the worker
    /// at `(endpoint, boot_epoch_id)` disconnects. Called from
    /// `WorkerApiServer`'s disconnect-cleanup task in addition to
    /// `unregister_worker` — the two together (a) prevent new pushes
    /// from queuing for the dead worker AND (b) release the in-flight
    /// push tracker so `pin_max_bytes` is not consumed forever by
    /// digests the worker can no longer ack.
    ///
    /// **Per-worker push attribution gap (v1 limitation):** the
    /// per-store `EphemeralServerSidePin` set is keyed by `DigestInfo`,
    /// NOT by `(endpoint, boot_epoch_id)`. The dispatcher does NOT
    /// track which worker pushed which entry. So this method clears
    /// the ENTIRE pin set for every registered store — the
    /// `endpoint` / `boot_epoch_id` arguments are recorded in the log
    /// for audit but do not gate the clear.
    ///
    /// This is correct under the assumption that a single worker
    /// disconnect signals every in-flight push from that worker is
    /// lost, AND in a single-worker fleet the over-broad clear is
    /// equivalent to the precise clear. For staggered fleets it
    /// temporarily drops in-flight push tracking for entries destined
    /// to OTHER workers; those workers will continue to ack via
    /// `BlobsAvailable.pinned_mirror_entries`, so the worst-case
    /// effect is one duplicate push per orphaned entry on the next
    /// blob update — bounded and self-healing.
    ///
    /// TODO(#168 follow-up): track per-(endpoint, boot_epoch_id) push
    /// attribution so unpin_on_disconnect only clears that worker's
    /// in-flight entries.
    pub fn unpin_on_disconnect(&self, endpoint: &str, boot_epoch_id: u64) {
        let snapshot = self.all_pin_sets();
        debug!(
            endpoint,
            boot_epoch_id,
            store_count = snapshot.len(),
            "SmallBlobDispatcher::unpin_on_disconnect clearing per-store pin sets"
        );
        for (_store_id, pin_set) in snapshot {
            pin_set.unpin_on_disconnect();
        }
    }

    /// Schedule a fan-out dispatch to every connected worker. **SYNC**:
    /// returns immediately after spawning the work, so the caller's RPC
    /// future does NOT block on dispatch. The Bazel-facing ack remains
    /// immediate per `feedback_no_sync_slow_write_ack` and the
    /// `feedback_async_to_sync_requires_explicit_signoff` rule (#168
    /// perf-optimizer MAJOR-1, BLOCKER — addresses the #203 OOM
    /// cascade shape: any sync-coupling between Bazel ack and the
    /// dispatcher path is a regression in waiting).
    ///
    /// Synchronous fast-fail order (no allocation, no spawn):
    /// 1. Feature flag off → return.
    /// 2. `data.len() > SMALL_BLOB_THRESHOLD` → return.
    ///
    /// Otherwise spawns a `tokio::task` carrying the snapshot of
    /// connected workers + their senders, and runs the per-worker
    /// `enqueue` calls there. Per-worker errors are logged inside the
    /// task; nothing propagates back to the caller.
    ///
    /// **No `.await` is performed by this method.** The producer
    /// hook contract is: call this, then continue immediately.
    ///
    /// Callers' hook-site contract:
    /// 1. Call AFTER the slow-tier write (`update_oneshot`) returns Ok —
    ///    durability via slow tier is the precondition for dispatch.
    /// 2. Skip when the upload is itself a mirror (`is_mirror`) or
    ///    originates from a worker (`is_worker`) — those paths must not
    ///    recursively re-dispatch (USER DIRECTIVE).
    /// 3. Pass `data.clone()` (Bytes is Arc-counted, O(1)).
    /// 4. Pass `store_id: Arc<str>` so the producer doesn't re-allocate
    ///    on every call (perf-optimizer #168 NIT-1).
    pub fn schedule_dispatch_to_all_workers(
        self: &Arc<Self>,
        store_id: Arc<str>,
        digest: DigestInfo,
        data: Bytes,
    ) {
        // Cheap synchronous gate — bail BEFORE any allocation or spawn
        // (perf-optimizer #168 NIT — keep the disabled-path zero-cost).
        if !self.config.small_blob_mirror_enabled {
            return;
        }
        // #168 testing-czar MAJOR-1: the producer hooks
        // (`bytestream_server::inner_write_oneshot`,
        // `cas_server::inner_batch_update_blobs`,
        // `ac_server::inner_update_action_result`) own the
        // `size_bytes <= SMALL_BLOB_THRESHOLD` gate. The dispatcher
        // intentionally does NOT re-check size here — duplicating the
        // gate made the producer-side mutation test require a double
        // mutation (comment out BOTH gates) before the test red-failed,
        // which made the test structurally vacuous (a producer-gate
        // regression would silently pass). The producer hooks are the
        // sole authority for the size gate.
        // Spawn the fan-out and return. The spawn allocation + task
        // wake is the only per-call overhead when the dispatcher is on.
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.dispatch_to_all_workers_inner(store_id, digest, data)
                .await;
        });
    }

    /// Inner fan-out implementation invoked from the spawned task.
    /// Snapshots `connected_workers_with_senders` ONCE (perf-optimizer
    /// #168 NIT-1 + NIT-2), iterates with the resolved senders so
    /// per-worker `enqueue_with_sender` does NOT re-lock `worker_txs`.
    async fn dispatch_to_all_workers_inner(
        self: Arc<Self>,
        store_id: Arc<str>,
        digest: DigestInfo,
        data: Bytes,
    ) {
        let workers = self.connected_workers_with_senders();
        if workers.is_empty() {
            debug!(
                store_id = store_id.as_ref(),
                %digest,
                data_len = data.len(),
                "dispatch_to_all_workers: no connected workers; skipping"
            );
            return;
        }
        self.fan_out_count_total.fetch_add(1, Ordering::Relaxed);
        debug!(
            store_id = store_id.as_ref(),
            %digest,
            data_len = data.len(),
            worker_count = workers.len(),
            "dispatch_to_all_workers: fanning out small blob"
        );
        for (endpoint, boot_epoch_id, worker_tx) in workers {
            if let Err(err) = self
                .enqueue_with_sender(
                    endpoint.clone(),
                    boot_epoch_id,
                    store_id.clone(),
                    digest,
                    data.clone(),
                    worker_tx,
                )
                .await
            {
                warn!(
                    store_id = store_id.as_ref(),
                    %digest,
                    endpoint = endpoint.as_ref(),
                    boot_epoch_id,
                    ?err,
                    "dispatch_to_all_workers: per-worker enqueue failed; continuing fan-out"
                );
            }
        }
    }

    /// Async fan-out wrapper around `dispatch_to_all_workers_inner`.
    /// Test-only / non-hot path: production producers go through
    /// `schedule_dispatch_to_all_workers` (sync) so the Bazel ack is
    /// not delayed by the fan-out.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub async fn dispatch_to_all_workers_for_test(
        self: &Arc<Self>,
        store_id: &str,
        digest: DigestInfo,
        data: Bytes,
    ) {
        if !self.config.small_blob_mirror_enabled {
            return;
        }
        // Test-only mirror of `schedule_dispatch_to_all_workers`: the
        // size gate lives in the producer hooks, not here. Test
        // callers MUST honor `data.len() <= SMALL_BLOB_THRESHOLD`.
        if data.len() > SMALL_BLOB_THRESHOLD {
            return;
        }
        Arc::clone(self)
            .dispatch_to_all_workers_inner(Arc::from(store_id), digest, data)
            .await;
    }

    /// Spawn a periodic info-logger that emits dispatcher activity
    /// metrics on `period`. The loop holds a `Weak<Self>` and exits
    /// when the dispatcher `Arc` is dropped (i.e., `Weak::upgrade`
    /// returns `None`). The returned `JoinHandle` is for caller
    /// observability ONLY — dropping it does NOT cancel the loop.
    /// (#168 code-reviewer S2: previous doc claimed the loop exits
    /// when the join handle is dropped; that was incorrect.)
    ///
    /// Per #168 dist-systems MAJOR-1: operators need to detect
    /// pin-set capacity exhaustion, queue-full, and fan-out coverage
    /// without a separate metric. One log line per `period` is the
    /// agreed visibility hook (cheap; ~64 chars + per-store fields).
    pub fn spawn_periodic_metrics(
        self: &Arc<Self>,
        period: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // Skip the immediate first tick so we don't log a
            // useless all-zeros line at startup.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(disp) = weak.upgrade() else {
                    return;
                };
                let pin_sets = disp.all_pin_sets();
                for (store_id, pin_set) in &pin_sets {
                    info!(
                        store_id = store_id.as_ref(),
                        bytes_used = pin_set.total_bytes(),
                        bytes_cap = pin_set.cap(),
                        len = pin_set.len(),
                        "small_blob_dispatcher: pin set capacity"
                    );
                }
                info!(
                    fan_out_count_total = disp.fan_out_count_total(),
                    dispatched_count = disp.dispatched_count(),
                    skipped_pin_full_total = disp.skipped_pin_full_total(),
                    skipped_queue_full_total = disp.skipped_queue_full_total(),
                    pin_set_count = pin_sets.len(),
                    "small_blob_dispatcher: activity metrics"
                );
            }
        })
    }

    /// Enqueue a single small blob for push to a specific worker.
    ///
    /// Preconditions (validated synchronously, fast-fail with no allocation):
    ///
    /// - When `small_blob_mirror_enabled = false`: return `Ok(())` no-op.
    /// - `data.len() <= SMALL_BLOB_THRESHOLD` (per C9). Larger blobs MUST
    ///   take the existing streaming path.
    /// - `store_id` non-empty + matches `[a-zA-Z_][a-zA-Z0-9_]*` (Rust
    ///   ident rules; per C11 as relaxed to accommodate production store
    ///   names like `cas_STORE` that mix case).
    ///
    /// On precondition failure: returns `Err(InvalidArgument)`. Caller
    /// MUST NOT propagate this back to Bazel (the dispatcher path is
    /// post-Bazel-ack fire-and-forget per `feedback_no_sync_slow_write_ack`).
    /// Log + drop is the production behavior — the caller's `if let Err`
    /// guard at the hook site does this.
    ///
    /// On unregistered-worker miss (no `worker_tx` for `(endpoint,
    /// boot_epoch_id)`): silently drop with `Ok(())`. The pin set is NOT
    /// modified (bytes never made it to a worker). This is the
    /// "stale-or-disconnected worker" case from plan §"Failure modes
    /// (general)".
    pub async fn enqueue(
        &self,
        endpoint: &str,
        boot_epoch_id: u64,
        store_id: &str,
        digest: DigestInfo,
        data: Bytes,
    ) -> Result<(), Error> {
        // Preconditions: feature flag + size + store_id format.
        if !self.config.small_blob_mirror_enabled {
            return Ok(());
        }
        if data.len() > SMALL_BLOB_THRESHOLD {
            return Err(make_input_err!(
                "SmallBlobDispatcher::enqueue: data.len()={} exceeds SMALL_BLOB_THRESHOLD={} \
                 (digest={digest}, store_id={store_id}); larger blobs must take the streaming \
                 path",
                data.len(),
                SMALL_BLOB_THRESHOLD,
            ));
        }
        if !is_valid_store_id(store_id) {
            return Err(make_input_err!(
                "SmallBlobDispatcher::enqueue: invalid store_id {store_id:?} \
                 (must be non-empty + match `[a-zA-Z_][a-zA-Z0-9_]*` per plan C11)"
            ));
        }
        let endpoint_key: Arc<str> = Arc::from(endpoint);
        let store_key: Arc<str> = Arc::from(store_id);
        // Resolve worker_tx (lookup-only, no .await under lock).
        let worker_tx_opt = {
            let txs = self.worker_txs.lock();
            txs.get(&(endpoint_key.clone(), boot_epoch_id)).cloned()
        };
        let Some(worker_tx) = worker_tx_opt else {
            // Unregistered or stale boot_epoch_id. Per plan §"Failure
            // modes": drop silently; the slow tier still has the bytes.
            debug!(
                endpoint,
                boot_epoch_id,
                store_id,
                %digest,
                data_len = data.len(),
                "enqueue: no worker_tx registered for (endpoint, boot_epoch_id); dropping"
            );
            return Ok(());
        };
        self.enqueue_with_sender(
            endpoint_key,
            boot_epoch_id,
            store_key,
            digest,
            data,
            worker_tx,
        )
        .await
    }

    /// Internal enqueue variant with a pre-resolved `worker_tx`.
    ///
    /// The fan-out fast-path (`schedule_dispatch_to_all_workers` →
    /// `dispatch_to_all_workers_inner`) calls this directly so the
    /// per-worker `worker_txs.lock()` round-trip is paid ONCE per
    /// fan-out (perf-optimizer #168 NIT-2).
    ///
    /// Preconditions are NOT re-checked here — the caller (either the
    /// public `enqueue` after its own validation OR the
    /// `dispatch_to_all_workers_inner` after the same set of gates)
    /// must have validated `data.len()` and `store_id` shape already.
    /// However the pin-set / queue-full / cap-exceeded *runtime*
    /// gates DO run here.
    ///
    /// **`worker_tx_for_initial_drainer_spawn` semantics (#168
    /// code-reviewer S3 footgun):** this parameter is consumed ONLY
    /// when this call is the FIRST enqueue for `(endpoint_key,
    /// boot_epoch_id, store_key)` — at that point the per-(worker,
    /// store) queue does not exist yet and we spawn the drainer task
    /// using this `worker_tx` as the upstream sender. On every
    /// SUBSEQUENT enqueue for the same triple, the spawned drainer
    /// already holds the `worker_tx` it was given on its first call,
    /// so the parameter passed to this method is silently dropped.
    /// Callers MUST pass a `worker_tx` clone that is REGISTRY-
    /// CONSISTENT (i.e., the same one that was registered via
    /// `register_worker(endpoint, boot_epoch_id, ...)`) so a
    /// hypothetical first-time spawn produces the right drainer.
    async fn enqueue_with_sender(
        &self,
        endpoint_key: Arc<str>,
        boot_epoch_id: u64,
        store_key: Arc<str>,
        digest: DigestInfo,
        data: Bytes,
        worker_tx_for_initial_drainer_spawn: mpsc::UnboundedSender<UpdateForWorker>,
    ) -> Result<(), Error> {
        // Resolve the pin set; required for accounting. Missing pin set
        // is a config error (FastSlowStore did not register). Per item E
        // (#168 defense-in-depth): use `debug!` so a dispatcher-enabled
        // but partially-registered config does NOT log-flood under load.
        // The startup registration (`nativelink.rs`) is the operator-
        // actionable surface; per-blob noise here adds no signal.
        let pin_set_opt = self.pin_set_for(store_key.as_ref());
        let Some(pin_set) = pin_set_opt else {
            debug!(
                endpoint = endpoint_key.as_ref(),
                store_id = store_key.as_ref(),
                %digest,
                "enqueue: no EphemeralServerSidePin registered for store_id; dropping"
            );
            return Ok(());
        };
        // Insert into pin set BEFORE handing to the drainer so the worker's
        // ack can never race ahead of our pin record. If the cap is
        // exceeded, drop without queuing.
        if let Err(err) = pin_set.insert(digest, data.clone()) {
            self.skipped_pin_full_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                endpoint = endpoint_key.as_ref(),
                store_id = store_key.as_ref(),
                %digest,
                ?err,
                "enqueue: pin set cap exceeded; dropping"
            );
            return Ok(());
        }
        // Acquire-or-spawn the per-(endpoint, boot_epoch_id, store_id)
        // queue + drainer task. Short critical section — Mutex held only
        // across HashMap entry resolution.
        let item = DispatchItem {
            digest,
            data,
            store_id: store_key.clone(),
        };
        let sender = {
            let mut queues = self.queues.lock();
            let key = (endpoint_key.clone(), boot_epoch_id, store_key.clone());
            if let Some(state) = queues.get(&key) {
                state.sender.clone()
            } else {
                let (tx, rx) = mpsc::channel::<DispatchItem>(self.config.max_pending_per_worker);
                queues.insert(key.clone(), PerWorkerStoreState { sender: tx.clone() });
                drop(queues);
                // Spawn the drainer. Holds NO locks during send.
                let max_batch_bytes = self.config.max_batch_bytes;
                let endpoint_for_log = endpoint_key.clone();
                let store_id_for_log = store_key.clone();
                let pin_set_for_drainer = pin_set.clone();
                tokio::spawn(drainer_task(
                    endpoint_for_log,
                    boot_epoch_id,
                    store_id_for_log,
                    rx,
                    worker_tx_for_initial_drainer_spawn,
                    max_batch_bytes,
                    pin_set_for_drainer,
                ));
                tx
            }
        };
        // Bounded try_send: on Full drop with warn (per plan
        // §"Concurrency design"). Pin set was already populated; remove
        // it to keep accounting honest.
        if let Err(err) = sender.try_send(item) {
            // Roll back the pin entry — bytes will never land on the
            // worker.
            pin_set.remove_one(&digest);
            self.skipped_queue_full_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                endpoint = endpoint_key.as_ref(),
                store_id = store_key.as_ref(),
                %digest,
                ?err,
                "enqueue: per-worker dispatch queue full; dropping (pin entry rolled back)"
            );
            return Ok(());
        }
        debug!(
            endpoint = endpoint_key.as_ref(),
            store_id = store_key.as_ref(),
            %digest,
            "SmallBlobDispatcher::enqueue dispatched"
        );
        self.dispatched_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Per-`(endpoint, boot_epoch_id, store_id)` drainer task. Runs until
/// the upstream Sender drops (`recv()` returns None) — typically when
/// the worker disconnects and `unregister_worker` clears the queue.
///
/// Per decision #3 (zero-window opportunistic coalesce): pull the first
/// item via `recv()`, then `try_recv()` until empty OR cumulative
/// `data.len()` exceeds `max_batch_bytes`. Send the batch as a single
/// `UpdateForWorker { batch_write_small_blobs }` message.
///
/// Per S3 (drainer-no-locks-during-send invariant): this function
/// holds NO mutex while calling `worker_tx.send`. The `pin_set` is only
/// touched on send-failure rollback — the success path leaves the
/// dispatcher's locks untouched.
async fn drainer_task(
    endpoint: Arc<str>,
    boot_epoch_id: u64,
    store_id: Arc<str>,
    mut rx: mpsc::Receiver<DispatchItem>,
    worker_tx: mpsc::UnboundedSender<UpdateForWorker>,
    max_batch_bytes: usize,
    pin_set: Arc<EphemeralServerSidePin>,
) {
    debug!(
        %endpoint,
        boot_epoch_id,
        %store_id,
        "drainer_task: start"
    );
    while let Some(first) = rx.recv().await {
        // Pull the first item then opportunistically drain pending up
        // to the byte cap.
        let mut total_bytes = first.data.len();
        let mut batch: Vec<DispatchItem> = vec![first];
        while total_bytes < max_batch_bytes {
            match rx.try_recv() {
                Ok(item) => {
                    total_bytes += item.data.len();
                    batch.push(item);
                }
                Err(_) => break,
            }
        }
        let blob_count = batch.len();
        // task #168 item 10 (per plan B6): wall-time the encode +
        // worker_tx.send so operators can see dispatch latency from
        // logs without a separate metric. We log the duration AFTER
        // the send so the success and failure paths both report it.
        let send_started = std::time::Instant::now();
        let proto_blobs: Vec<SmallBlobEntry> = batch
            .iter()
            .map(|item| SmallBlobEntry {
                digest: Some(ProtoDigest::from(item.digest)),
                data: item.data.clone(),
                store_id: item.store_id.to_string(),
            })
            .collect();
        let msg = UpdateForWorker {
            update: Some(UpdateForWorkerUpdate::BatchWriteSmallBlobs(
                BatchWriteSmallBlobsRequest { blobs: proto_blobs },
            )),
        };
        if let Err(send_err) = worker_tx.send(msg) {
            let wall_ms = send_started.elapsed().as_millis() as u64;
            // worker_tx closed (worker disconnected) — roll back every
            // pin entry we held for this batch and exit.
            warn!(
                %endpoint,
                boot_epoch_id,
                %store_id,
                blob_count,
                total_bytes,
                wall_ms,
                ?send_err,
                "drainer_task: worker_tx closed; rolling back pin entries + exiting"
            );
            for item in &batch {
                pin_set.remove_one(&item.digest);
            }
            return;
        }
        let wall_ms = send_started.elapsed().as_millis() as u64;
        debug!(
            %endpoint,
            boot_epoch_id,
            %store_id,
            blob_count,
            total_bytes,
            wall_ms,
            "drainer_task: batch sent"
        );
    }
    debug!(
        %endpoint,
        boot_epoch_id,
        %store_id,
        "drainer_task: queue closed; exiting"
    );
}

/// Synthetic small-key (size 0) used by [`find_fast_slow_for_pin`] to
/// route `SizePartitioning`'s `inner_store(Some(key))` to its
/// `lower_store` branch (the side that holds small CAS blobs in
/// production: `SMALL_CAS_CACHED = FastSlow{ fast: MemoryStore, slow:
/// RefStore→Redis }`). Without this, `inner_store(None)` returns `self`
/// and the walker bails before reaching the FastSlowStore.
fn synthetic_small_key() -> StoreKey<'static> {
    StoreKey::Digest(DigestInfo::new([0u8; 32], 0))
}

/// Walk the production store wrapper chain to find the underlying
/// [`FastSlowStore`] that backs a CAS or AC instance. Used by the
/// `#168` startup wire-up in `src/bin/nativelink.rs` to register a
/// per-store [`EphemeralServerSidePin`] for every CAS + AC store
/// whose chain bottoms out at a FastSlowStore.
///
/// The walker recurses through every wrapper that returns `self` from
/// the trait-default `inner_store(None)` (`ExistenceCacheStore`,
/// `VerifyStore`, `CompletenessCheckingStore`). For each, it drills
/// into the right inner via the wrapper's concrete accessor:
///
/// - `ExistenceCacheStore.inner_store()` → its single backend.
/// - `VerifyStore.inner_store()` → its single backend.
/// - `CompletenessCheckingStore.ac_store()` → the AC chain (NOT
///   `cas_store`, which is the secondary verification side that the
///   completeness check uses internally; the producer-hook fan-out
///   targets the AC entry path that owns the digest).
///
/// Stops on the first wrapper that is not recognized AND does not
/// unwrap further (`inner_store(_)` returns the same pointer as `self`).
///
/// **#168 dist-systems MINOR-1 / security Q5:** the production AC
/// chain is `Completeness{ AC_BACKEND_CACHED = FastSlow{ fast:
/// MemoryStore, slow: RefStore→Redis } }`. Without the
/// `CompletenessCheckingStore` branch the walker bails immediately,
/// the AC dispatcher pin set is never registered, and the AC
/// fan-out path is silently inert in production.
pub fn find_fast_slow_for_pin(store: &dyn StoreDriver) -> Option<&FastSlowStore> {
    if let Some(fss) = store.as_any().downcast_ref::<FastSlowStore>() {
        return Some(fss);
    }
    if let Some(ecs) = store
        .as_any()
        .downcast_ref::<ExistenceCacheStore<std::time::SystemTime>>()
    {
        return find_fast_slow_for_pin(
            ecs.inner_store().inner_store(Some(synthetic_small_key())),
        );
    }
    if let Some(vs) = store.as_any().downcast_ref::<VerifyStore>() {
        return find_fast_slow_for_pin(
            vs.inner_store().inner_store(Some(synthetic_small_key())),
        );
    }
    if let Some(ccs) = store.as_any().downcast_ref::<CompletenessCheckingStore>() {
        return find_fast_slow_for_pin(
            ccs.ac_store().inner_store(Some(synthetic_small_key())),
        );
    }
    let inner = store.inner_store(Some(synthetic_small_key()));
    if core::ptr::eq(
        inner as *const dyn StoreDriver,
        store as *const dyn StoreDriver,
    ) {
        return None;
    }
    find_fast_slow_for_pin(inner)
}

/// Validate `store_id` per plan C11 (relaxed to Rust-ident rules).
/// Format `[a-zA-Z_][a-zA-Z0-9_]*`. Originally lowercase-only; relaxed to
/// accept production store names that mix case (e.g. `cas_STORE`,
/// `WORKER_FAST_SLOW_STORE`) without forcing a config rename.
pub fn is_valid_store_id(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_valid_store_id_accepts_documented_examples() {
        assert!(is_valid_store_id("cas"));
        assert!(is_valid_store_id("ac"));
        assert!(is_valid_store_id("cas_small"));
        assert!(is_valid_store_id("cas2"));
        assert!(is_valid_store_id("a"));
    }

    #[test]
    fn is_valid_store_id_accepts_production_store_names() {
        // Regression for #168: production configs use mixed-case names
        // like `cas_STORE`. The pre-relaxation regex `[a-z][a-z0-9_]*`
        // silently disabled the dispatcher in production. The relaxed
        // form `[a-zA-Z_][a-zA-Z0-9_]*` accepts these.
        assert!(is_valid_store_id("cas_STORE"));
        assert!(is_valid_store_id("cas_INNER"));
        assert!(is_valid_store_id("WORKER_FAST_SLOW_STORE"));
        assert!(is_valid_store_id("AC_BACKEND_CACHED"));
        assert!(is_valid_store_id("Cas"));
        assert!(is_valid_store_id("_underscore_start"));
    }

    #[test]
    fn is_valid_store_id_rejects_documented_invalids() {
        assert!(!is_valid_store_id(""));
        assert!(!is_valid_store_id("1cas"));
        assert!(!is_valid_store_id("cas-small"));
        assert!(!is_valid_store_id("cas.small"));
        assert!(!is_valid_store_id("cas/small"));
        assert!(!is_valid_store_id("cas store"));
    }

    /// #168 review Fix 2 (red-team Q1) regression:
    ///
    /// The per-(worker, store) queue is `mpsc::channel(max_pending_per_worker)`,
    /// a BOUNDED sender, so `try_send` returns `TrySendError::Full` once
    /// the queue is at capacity AND the drainer has not yet drained any
    /// item. The dispatcher's `skipped_queue_full_total` counter MUST
    /// tick AND the pin entry MUST be rolled back so accounting stays
    /// honest under sustained burst.
    ///
    /// We use `tokio::test(start_paused = true)` to prevent the spawned
    /// drainer task from running between our two synchronous enqueues.
    /// With `max_pending_per_worker = 1` the second enqueue is
    /// guaranteed to hit a full queue (the drainer hasn't been
    /// scheduled yet).
    ///
    /// Mutation step: set `dispatcher_cfg.max_pending_per_worker = 1024`
    /// (or remove the `try_send` Full handling) → this test red-fails.
    #[nativelink_macro::nativelink_test(flavor = "current_thread", start_paused = true)]
    async fn enqueue_queue_full_increments_counter_and_rolls_back_pin() {
        let cfg = SmallBlobDispatcherConfig {
            small_blob_mirror_enabled: true,
            // Capacity 1: second item MUST hit queue-full because the
            // drainer task has not been scheduled (start_paused = true).
            max_pending_per_worker: 1,
            ..Default::default()
        };
        let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
        let pin = Arc::new(EphemeralServerSidePin::new(256 * 1024 * 1024));
        dispatcher.register_pin_set("cas_STORE", pin.clone());
        let (worker_tx, _worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
        dispatcher.register_worker("grpc://fake:1", 1, worker_tx);

        let digest_a = DigestInfo::new([0xAA; 32], 4);
        let digest_b = DigestInfo::new([0xBB; 32], 4);
        let data_a = Bytes::from_static(b"AAAA");
        let data_b = Bytes::from_static(b"BBBB");

        // First enqueue: lazily spawns the drainer + leaves capacity 0.
        let r1 = dispatcher
            .enqueue("grpc://fake:1", 1, "cas_STORE", digest_a, data_a)
            .await;
        assert!(r1.is_ok(), "first enqueue must succeed: {r1:?}");

        // Second enqueue WITHOUT yielding to the runtime — drainer is
        // still parked because the runtime is paused, queue is at
        // capacity. `try_send` MUST return `TrySendError::Full` and the
        // dispatcher MUST roll the pin entry back.
        let r2 = dispatcher
            .enqueue("grpc://fake:1", 1, "cas_STORE", digest_b, data_b)
            .await;
        assert!(
            r2.is_ok(),
            "queue-full enqueue MUST surface as Ok(()) (drop-with-warn — fire-and-forget contract)"
        );
        assert_eq!(
            dispatcher.skipped_queue_full_total(),
            1,
            "skipped_queue_full_total MUST tick on TrySendError::Full \
             — without bounded mpsc::channel + try_send Full handling, \
             this counter never advances and operators cannot detect a \
             slow worker (red-team Q1)"
        );
        // Pin set MUST contain ONLY the first digest: the second
        // enqueue's pin entry was rolled back when try_send returned
        // Full.
        assert!(
            pin.contains(&digest_a),
            "first digest's pin entry MUST be retained (queued in drainer)"
        );
        assert!(
            !pin.contains(&digest_b),
            "queue-full digest's pin entry MUST be rolled back so accounting stays honest"
        );
    }

    /// #168 dist-systems MINOR-1 / security Q5 regression:
    ///
    /// Production AC chain shape is
    /// `Completeness{ AC_BACKEND_CACHED = FastSlow{ fast: MemoryStore,
    /// slow: RefStore→Redis } }`. Before the
    /// `CompletenessCheckingStore` branch was added, the walker fell
    /// through to `inner_store(_)` (which returns `self` for the
    /// composite trait) and bailed without ever registering an AC pin
    /// set — the AC dispatcher fan-out path was inert in production.
    ///
    /// Mutation step (per CLAUDE.md TDD step 5): comment out the
    /// `CompletenessCheckingStore` branch in `find_fast_slow_for_pin`
    /// → this test MUST red-fail with the bespoke
    /// `"#168 walker MUST recurse through CompletenessCheckingStore into AC chain"`
    /// message.
    #[nativelink_macro::nativelink_test]
    async fn find_fast_slow_for_pin_recurses_through_completeness_checking_store_ac_chain() {
        use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
        use nativelink_util::store_trait::Store;

        use crate::completeness_checking_store::CompletenessCheckingStore;
        use crate::fast_slow_store::FastSlowStore;
        use crate::memory_store::MemoryStore;

        // Build the AC backing chain: FastSlow{ fast: MemoryStore,
        // slow: MemoryStore } (Memory stands in for the production
        // Redis ref_store; the walker only inspects wrapper types,
        // not the slow tier semantics).
        let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
        let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
        let ac_backend_fss: Arc<FastSlowStore> = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Memory(MemorySpec::default()),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
            },
            fast,
            slow,
        );
        let ac_backend_ptr: *const FastSlowStore = Arc::as_ptr(&ac_backend_fss);

        // Wrap with CompletenessCheckingStore on top, with a separate
        // CAS-side store. The walker MUST drill into ac_store, NOT
        // cas_store.
        let ac_store_for_completeness = Store::new(ac_backend_fss);
        let cas_store_unrelated = Store::new(MemoryStore::new(&MemorySpec::default()));
        let ccs = CompletenessCheckingStore::new(
            ac_store_for_completeness,
            cas_store_unrelated,
        );

        // Walk via the public helper. `&*ccs` derefs the Arc to the
        // inner CompletenessCheckingStore, which implements
        // `StoreDriver`. `find_fast_slow_for_pin` first downcasts to
        // CompletenessCheckingStore, then drills into `ac_store()` —
        // exactly the production-shape walk performed by
        // `src/bin/nativelink.rs`'s startup loop for AC stores.
        let driver: &dyn StoreDriver = &*ccs;
        let found = find_fast_slow_for_pin(driver).expect(
            "#168 walker MUST recurse through CompletenessCheckingStore into AC chain \
             (production AC shape: Completeness{ AC_BACKEND_CACHED = FastSlow{...} })",
        );

        // Verify the resolved FastSlowStore is the one we built (same
        // pointer == same FastSlowStore instance, not a sibling
        // returned from the cas_store branch).
        assert!(
            core::ptr::eq(found as *const FastSlowStore, ac_backend_ptr),
            "walker resolved a DIFFERENT FastSlowStore than the AC-backing one — \
             likely walked into cas_store instead of ac_store"
        );
    }
}
