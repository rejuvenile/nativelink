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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MirrorPinEntry;
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
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

/// The server-singleton dispatcher. Manages per-`(endpoint, boot_epoch_id,
/// store_id)` queues and drainer tasks (TODO: drainer not yet wired —
/// see module-level Status section).
///
/// Today this exposes:
///
/// - `enqueue(...)`: precondition checks + (when wired) push-into-mpsc.
///   When `small_blob_mirror_enabled = false` the call is a no-op
///   returning `Ok(())`.
///
/// Future commits will add:
///
/// - A `parking_lot::Mutex<HashMap<(Arc<str>, u64, Arc<str>),
///   PerWorkerStoreState>>` map (per plan "Component owners" + B4 keying).
/// - A spawned drainer task per state that zero-window-coalesces pending
///   items into a `BatchWriteSmallBlobsRequest` and sends over the
///   `worker_tx: mpsc::UnboundedSender<UpdateForWorker>`.
/// - `TimedDispatchCall<F>` instrumentation (B6).
///
/// Concurrency invariants (per plan "Concurrency design"):
/// - Dispatcher Mutex BEFORE per-store pin-set Mutex (prevents AB/BA).
/// - Drainer holds NO locks during send (S3 future-regression guard).
pub struct SmallBlobDispatcher {
    config: SmallBlobDispatcherConfig,
    /// Diagnostic counter: number of `enqueue` calls that successfully
    /// passed precondition and feature-flag gates. Currently only used
    /// in tests because the drainer is not wired yet — production
    /// callers should use the pin-set telemetry as the primary health
    /// signal (per B6).
    dispatched_count: AtomicUsize,
}

impl SmallBlobDispatcher {
    /// Build a new dispatcher with the given config. With the feature
    /// flag off (default), `enqueue` is a no-op.
    pub fn new(config: SmallBlobDispatcherConfig) -> Self {
        Self {
            config,
            dispatched_count: AtomicUsize::new(0),
        }
    }

    /// Diagnostic accessor: how many calls passed all gates and (would)
    /// have hit the per-(worker, store) mpsc.
    pub fn dispatched_count(&self) -> usize {
        self.dispatched_count.load(Ordering::Relaxed)
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
    pub async fn enqueue(
        &self,
        endpoint: &str,
        _boot_epoch_id: u64,
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
        // TODO(bug-a small-CAS peer-mirror): wire to per-(endpoint,
        // boot_epoch_id, store_id) mpsc + drainer. Currently no-op +
        // counter increment. The pin set + worker-side
        // BatchWriteSmallBlobs handler are in separate commits; this
        // skeleton lands the API surface so the trait + tests can
        // compile and reviewers see the eventual call site.
        debug!(
            endpoint,
            store_id,
            %digest,
            data_len = data.len(),
            "SmallBlobDispatcher::enqueue (skeleton — drainer not yet wired)"
        );
        self.dispatched_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
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
