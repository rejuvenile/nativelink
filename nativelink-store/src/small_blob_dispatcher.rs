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
//!   (Bytes, Instant)>>` plus an `AtomicU64` for total bytes.
//! - The pin set holds `Bytes` (Arc-counted), so insert / remove is O(1).
//! - `observe_pinned_mirror_ack` does a single binary search over the
//!   sorted-by-store_id `entries` slice and removes ONLY the entries
//!   whose `store_id` matches `self.store_id`. Other stores' broadcasts
//!   are O(log N) no-ops.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BatchWriteSmallBlobsRequest, MirrorPinEntry, SmallBlobEntry, UpdateForWorker,
    update_for_worker::Update as UpdateForWorkerUpdate,
};
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, warn};

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

/// Default TTL fallback on a server-side pin entry. Real lifetime is ~RTT
/// (sub-100ms); this only fires on worker silence / disconnect / reconnect
/// race. Bytes are still in slow tier — TTL expiry just frees server pin
/// memory and falls back to slow-tier reads.
const DEFAULT_PIN_TTL: Duration = Duration::from_secs(10);

/// Operator-tunable knobs for the dispatcher. Mirrors the "Knobs" table in
/// the plan; defaults match production-canary plan rec.
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
    /// Server-side pin TTL fallback. See `DEFAULT_PIN_TTL`.
    pub pin_ttl: Duration,
}

impl Default for SmallBlobDispatcherConfig {
    fn default() -> Self {
        Self {
            small_blob_mirror_enabled: DEFAULT_SMALL_BLOB_MIRROR_ENABLED,
            max_pending_per_worker: DEFAULT_MAX_PENDING_PER_WORKER,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            pin_max_bytes: DEFAULT_PIN_MAX_BYTES,
            pin_ttl: DEFAULT_PIN_TTL,
        }
    }
}

/// Server-side pin tracking for bytes the dispatcher pushed to a worker
/// that the worker has not yet ack'd.
///
/// Lifetime: ~RTT, TTL-evicted. Each entry (`DigestInfo` -> `(Bytes,
/// Instant)`) costs `data.len() + 24` bytes-ish. Cap is per-store
/// (`pin_max_bytes`) so two stores (CAS + AC) sum to `2 × pin_max_bytes`
/// per server.
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
pub struct EphemeralServerSidePin {
    cap: u64,
    state: Mutex<HashMap<DigestInfo, (Bytes, Instant)>>,
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

    /// Total bytes currently held in the pin set.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.state.lock().len()
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
    pub fn insert(&self, digest: DigestInfo, data: Bytes) -> Result<(), Error> {
        let new_bytes = data.len() as u64;
        let mut state = self.state.lock();
        let current_total = self.total_bytes.load(Ordering::Relaxed);
        // Subtract old entry's size if we're replacing.
        let old_size = state.get(&digest).map(|(d, _)| d.len() as u64).unwrap_or(0);
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
        state.insert(digest, (data, Instant::now()));
        // Update atomic AFTER mutation so an observer that races a remove
        // sees the post-insert value (not a torn intermediate).
        self.total_bytes.store(projected, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a single pin entry by digest. No-op if not present.
    pub fn remove_one(&self, digest: &DigestInfo) {
        let mut state = self.state.lock();
        if let Some((data, _)) = state.remove(digest) {
            let removed = data.len() as u64;
            drop(state);
            self.total_bytes.fetch_sub(removed, Ordering::Relaxed);
        }
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
            if let Some((data, _)) = state.remove(&digest) {
                freed += data.len() as u64;
            }
        }
        drop(state);
        if freed > 0 {
            self.total_bytes.fetch_sub(freed, Ordering::Relaxed);
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
        }
    }

    /// Diagnostic accessor: how many calls passed all gates and landed
    /// in a per-(worker, store) mpsc.
    pub fn dispatched_count(&self) -> usize {
        self.dispatched_count.load(Ordering::Relaxed)
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

    /// Enqueue a single small blob for push to a specific worker.
    ///
    /// Preconditions (validated synchronously, fast-fail with no allocation):
    ///
    /// - When `small_blob_mirror_enabled = false`: return `Ok(())` no-op.
    /// - `data.len() <= SMALL_BLOB_THRESHOLD` (per C9). Larger blobs MUST
    ///   take the existing streaming path.
    /// - `store_id` non-empty + matches `[a-z][a-z0-9_]*` (per C11).
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
        if !self.config.small_blob_mirror_enabled {
            return Ok(());
        }
        // Precondition: size.
        if data.len() > SMALL_BLOB_THRESHOLD {
            return Err(make_input_err!(
                "SmallBlobDispatcher::enqueue: data.len()={} exceeds SMALL_BLOB_THRESHOLD={} \
                 (digest={digest}, store_id={store_id}); larger blobs must take the streaming \
                 path",
                data.len(),
                SMALL_BLOB_THRESHOLD,
            ));
        }
        // Precondition: store_id format.
        if !is_valid_store_id(store_id) {
            return Err(make_input_err!(
                "SmallBlobDispatcher::enqueue: invalid store_id {store_id:?} \
                 (must be non-empty + match `[a-z][a-z0-9_]*` per plan C11)"
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
        // Resolve the pin set; required for accounting. Missing pin set
        // is a config error (FastSlowStore did not register) — drop with
        // a warn but do NOT propagate (post-ack fire-and-forget).
        let pin_set_opt = self.pin_set_for(store_id);
        let Some(pin_set) = pin_set_opt else {
            warn!(
                endpoint,
                store_id,
                %digest,
                "enqueue: no EphemeralServerSidePin registered for store_id; \
                 dropping (FastSlowStore did not register at startup — operator-actionable)"
            );
            return Ok(());
        };
        // Insert into pin set BEFORE handing to the drainer so the worker's
        // ack can never race ahead of our pin record. If the cap is
        // exceeded, drop without queuing.
        if let Err(err) = pin_set.insert(digest, data.clone()) {
            warn!(
                endpoint,
                store_id,
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
                    worker_tx,
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
            warn!(
                endpoint,
                store_id,
                %digest,
                ?err,
                "enqueue: per-worker dispatch queue full; dropping (pin entry rolled back)"
            );
            return Ok(());
        }
        debug!(
            endpoint,
            store_id,
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
            // worker_tx closed (worker disconnected) — roll back every
            // pin entry we held for this batch and exit.
            warn!(
                %endpoint,
                boot_epoch_id,
                %store_id,
                blob_count,
                total_bytes,
                ?send_err,
                "drainer_task: worker_tx closed; rolling back pin entries + exiting"
            );
            for item in &batch {
                pin_set.remove_one(&item.digest);
            }
            return;
        }
        debug!(
            %endpoint,
            boot_epoch_id,
            %store_id,
            blob_count,
            total_bytes,
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

/// Validate `store_id` per plan C11. Format `[a-z][a-z0-9_]*`.
fn is_valid_store_id(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
    fn is_valid_store_id_rejects_documented_invalids() {
        assert!(!is_valid_store_id(""));
        assert!(!is_valid_store_id("Cas"));
        assert!(!is_valid_store_id("1cas"));
        assert!(!is_valid_store_id("cas-small"));
        assert!(!is_valid_store_id("cas.small"));
        assert!(!is_valid_store_id("cas/small"));
    }
}
