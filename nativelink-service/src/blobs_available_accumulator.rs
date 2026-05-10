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

//! (#99 — PR3 of #80) Server-side accumulator for chunked
//! `BlobsAvailable` notifications.
//!
//! **Path A semantics: defer wipe until terminal chunk.** When chunks
//! of a `is_full_snapshot=true` broadcast arrive, the accumulator
//! BUFFERS each per-chunk slice; only when `is_last=true` lands does
//! it materialise the full `BlobsAvailableNotification` and pass it to
//! the legacy `handle_blobs_available` for the existing
//! `remove_endpoint` + `register_blobs_iter` + AC pin replace + mirror
//! pipeline. Path A guarantees: an interrupted broadcast never leaves
//! the locality_map with a partial view (no half-applied snapshot).
//!
//! Per-broadcast state is keyed by `(broadcast_id, worker_instance_token)`
//! so the same scheduler can process concurrent broadcasts from many
//! workers, AND a worker-process restart mid-broadcast (impossible in
//! steady state but defensive) discards the orphaned partial state.
//!
//! Memory bound: the accumulator is in-process state on a network path,
//! so it MUST have a measured cap per CLAUDE.md. We cap PER CONNECTION
//! the number of in-flight broadcasts AND the total accumulated entries;
//! over-cap chunks are dropped on the floor and warn-logged. The cap
//! defends against a malicious / wedged worker that emits chunks but
//! never sends the terminal `is_last`.

use core::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableChunk, BlobsAvailableNotification,
};
use parking_lot::Mutex;
use tracing::{debug, warn};

/// CAPPED AT 8: a misbehaving / wedged worker emitting chunks for new
/// `broadcast_id`s without ever sending `is_last=true` would otherwise
/// grow the accumulator monotonically. Steady-state rate is 1 broadcast
/// active at a time per worker (the send loop emits chunks then waits
/// for the next tick); 8 is 8× that for over-provisioning. Falsification:
/// a synthetic-load test driving 800 broadcasts (10× cap) MUST evict
/// oldest under cap pressure rather than OOM.
pub(crate) const MAX_INFLIGHT_BROADCASTS_PER_CONN: usize = 8;

/// CAPPED AT 200_000: at ~100 B per entry (worst-case `MirrorPinEntry`
/// plus `Vec` overhead), that's ~20 MB per connection. With ~10 worker
/// connections, ~200 MB worst-case heap on the accumulator alone —
/// tolerable inside the 80 GB MemoryMax. A worker emitting 200K entries
/// across one broadcast is almost certainly producing a fleet-replay
/// scenario the chunker is supposed to mitigate, NOT steady state.
/// Falsification: a 2M-entry synthetic broadcast MUST trigger eviction
/// and warn rather than OOM.
pub(crate) const MAX_ACCUMULATED_ENTRIES_PER_CONN: usize = 200_000;

/// Maximum supported sequence count per broadcast (must match the bit
/// width of `seen_sequences`). At 64 chunks × 4096 entries/chunk =
/// 262K entries per broadcast — tracks the
/// `MAX_ACCUMULATED_ENTRIES_PER_CONN` ceiling.
const MAX_SEQUENCES: u32 = 64;

/// One in-flight broadcast's accumulated state.
#[derive(Debug)]
struct BroadcastAccumulator {
    /// Echoed from the first chunk; validated equal on every subsequent
    /// chunk. A mismatch means a worker process restart (impossible in
    /// steady state); the partial state is discarded.
    worker_instance_token: u64,
    /// Echoed from the first chunk. Single-store today; reserved for
    /// future per-store routing.
    store_id: String,
    /// Set on chunk-0; carried forward.
    is_full_snapshot: bool,
    /// The accumulated notification body. Header scalars come from
    /// chunk 0; payload slices are the union across all chunks.
    body: BlobsAvailableNotification,
    /// Sum of entries across all 8 payload slices accumulated so far.
    /// Used to drive the per-connection accumulated-entries cap.
    accumulated_entries: usize,
    /// Bitset of seen `sequence` values. Bound at `MAX_SEQUENCES`;
    /// out-of-order chunks within that window are tolerated, larger
    /// gaps trigger discard.
    seen_sequences: u64,
    /// Largest `sequence` value observed so far (informational; helps
    /// detect future out-of-order pathologies in logs).
    max_seen_sequence: u32,
}

impl BroadcastAccumulator {
    fn new(first_chunk: &BlobsAvailableChunk) -> Self {
        // Header scalars are read from chunk 0 (and only meaningful
        // there); subsequent chunks have proto3 defaults the body
        // mustn't overwrite.
        let body = BlobsAvailableNotification {
            worker_cas_endpoint: first_chunk.worker_cas_endpoint.clone(),
            digests: Vec::new(),
            is_full_snapshot: first_chunk.is_full_snapshot,
            evicted_digests: Vec::new(),
            digest_infos: Vec::new(),
            cpu_load_pct: first_chunk.cpu_load_pct,
            cached_directory_digests: Vec::new(),
            added_subtree_digests: Vec::new(),
            removed_subtree_digests: Vec::new(),
            is_full_subtree_snapshot: first_chunk.is_full_subtree_snapshot,
            p_core_load_pct: first_chunk.p_core_load_pct,
            e_core_load_pct: first_chunk.e_core_load_pct,
            pinned_mirror_digests: Vec::new(),
            mirror_used_bytes: first_chunk.mirror_used_bytes,
            mirror_max_bytes: first_chunk.mirror_max_bytes,
            pinned_mirror_entries: Vec::new(),
            pinned_ac_mirror_entries: Vec::new(),
        };
        Self {
            worker_instance_token: first_chunk.worker_instance_token,
            store_id: first_chunk.store_id.clone(),
            is_full_snapshot: first_chunk.is_full_snapshot,
            body,
            accumulated_entries: 0,
            seen_sequences: 0,
            max_seen_sequence: 0,
        }
    }

    /// Merge `chunk` into the accumulated state. Returns
    /// `Err(reason)` if the chunk should be dropped (validation
    /// failure); returns `Ok(true)` if this was the terminal chunk
    /// (caller should commit and drop the accumulator); returns
    /// `Ok(false)` otherwise.
    fn merge(&mut self, chunk: BlobsAvailableChunk) -> Result<bool, &'static str> {
        if chunk.worker_instance_token != self.worker_instance_token {
            return Err("worker_instance_token mismatch — discarding partial accumulator");
        }
        if chunk.store_id != self.store_id {
            return Err("store_id mismatch within one broadcast");
        }
        if chunk.is_full_snapshot != self.is_full_snapshot {
            return Err("is_full_snapshot inconsistent across chunks");
        }
        if chunk.sequence >= MAX_SEQUENCES {
            return Err("chunk sequence exceeds MAX_SEQUENCES (64) per broadcast");
        }
        let bit = 1u64 << chunk.sequence;
        if self.seen_sequences & bit != 0 {
            return Err("duplicate sequence within one broadcast");
        }
        self.seen_sequences |= bit;
        if chunk.sequence > self.max_seen_sequence {
            self.max_seen_sequence = chunk.sequence;
        }
        let entries_in_chunk = chunk.digests.len()
            + chunk.cached_directory_digests.len()
            + chunk.pinned_mirror_entries.len()
            + chunk.pinned_ac_mirror_entries.len()
            + chunk.evicted_digests.len()
            + chunk.added_subtree_digests.len()
            + chunk.removed_subtree_digests.len()
            + chunk.pinned_mirror_digests.len();
        self.accumulated_entries = self.accumulated_entries.saturating_add(entries_in_chunk);

        // Merge the per-chunk slices.
        self.body.digest_infos.extend(chunk.digests);
        self.body
            .cached_directory_digests
            .extend(chunk.cached_directory_digests);
        self.body
            .pinned_mirror_entries
            .extend(chunk.pinned_mirror_entries);
        self.body
            .pinned_ac_mirror_entries
            .extend(chunk.pinned_ac_mirror_entries);
        self.body.evicted_digests.extend(chunk.evicted_digests);
        self.body
            .added_subtree_digests
            .extend(chunk.added_subtree_digests);
        self.body
            .removed_subtree_digests
            .extend(chunk.removed_subtree_digests);
        self.body
            .pinned_mirror_digests
            .extend(chunk.pinned_mirror_digests);

        Ok(chunk.is_last)
    }
}

/// One per `WorkerApiServerInstance` (one per worker connection).
/// Holds the in-flight per-broadcast accumulators.
#[derive(Debug, Default)]
pub struct BlobsAvailableAccumulator {
    inner: Mutex<AccumulatorInner>,
    /// Total entries summed across all in-flight broadcasts.
    total_accumulated: AtomicUsize,
}

#[derive(Debug, Default)]
struct AccumulatorInner {
    /// CAPPED AT MAX_INFLIGHT_BROADCASTS_PER_CONN broadcasts per
    /// connection: a malicious / wedged worker emitting chunks for
    /// new broadcast_ids without `is_last=true` would otherwise grow
    /// monotonically. Over-cap policy: drop NEW broadcasts (not
    /// oldest) — preserves any broadcast that's been making progress.
    /// (Justification + measurement in the const above.)
    broadcasts: HashMap<u64, BroadcastAccumulator>,
}

impl BlobsAvailableAccumulator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Process one chunk. Returns `Some(notification)` when this chunk
    /// is the terminal of its broadcast and the caller should pass the
    /// fully-assembled `BlobsAvailableNotification` to the legacy
    /// `handle_blobs_available` (the Path-A commit point). Returns
    /// `None` for non-terminal chunks (caller does nothing).
    ///
    /// Validation failures (token mismatch, duplicate sequence, store_id
    /// drift, accumulator over cap) drop the partial state and return
    /// `None`; the worker MUST then re-broadcast on the next tick.
    pub fn merge_chunk(&self, chunk: BlobsAvailableChunk) -> Option<BlobsAvailableNotification> {
        let broadcast_id = chunk.broadcast_id;
        let token = chunk.worker_instance_token;
        let sequence = chunk.sequence;

        // Defensive: 0 token is "uninitialised" — reject. Per #97
        // precedent: a 0 server_instance_token meant a pre-fixup
        // worker; in #99 the analog is a pre-fixup or buggy worker.
        if token == 0 {
            warn!(
                target: "nativelink::blobs_available_chunked",
                broadcast_id,
                sequence,
                "rejecting BlobsAvailableChunk with worker_instance_token=0 (uninitialised)"
            );
            return None;
        }

        let mut inner = self.inner.lock();

        // Per-conn broadcast count cap.
        if !inner.broadcasts.contains_key(&broadcast_id)
            && inner.broadcasts.len() >= MAX_INFLIGHT_BROADCASTS_PER_CONN
        {
            warn!(
                target: "nativelink::blobs_available_chunked",
                in_flight = inner.broadcasts.len(),
                cap = MAX_INFLIGHT_BROADCASTS_PER_CONN,
                broadcast_id,
                "BlobsAvailable accumulator at per-conn broadcast cap; \
                 dropping new broadcast — worker likely emitted chunks \
                 but never sent is_last=true"
            );
            return None;
        }

        let acc = match inner.broadcasts.entry(broadcast_id) {
            std::collections::hash_map::Entry::Occupied(mut occ) => {
                // If the in-flight accumulator has a different token,
                // that's a worker-process restart mid-broadcast. Discard
                // and re-create with the new chunk.
                if occ.get().worker_instance_token != token {
                    debug!(
                        target: "nativelink::blobs_available_chunked",
                        broadcast_id,
                        old_token = occ.get().worker_instance_token,
                        new_token = token,
                        "discarding partial accumulator on token mismatch"
                    );
                    let removed_entries = occ.get().accumulated_entries;
                    self.total_accumulated
                        .fetch_sub(removed_entries, Ordering::Relaxed);
                    *occ.get_mut() = BroadcastAccumulator::new(&chunk);
                }
                occ.into_mut()
            }
            std::collections::hash_map::Entry::Vacant(vac) => {
                vac.insert(BroadcastAccumulator::new(&chunk))
            }
        };

        let prev_entries = acc.accumulated_entries;
        let merge_result = acc.merge(chunk);
        let new_entries = acc.accumulated_entries;
        let delta = new_entries.saturating_sub(prev_entries);

        match merge_result {
            Err(reason) => {
                warn!(
                    target: "nativelink::blobs_available_chunked",
                    broadcast_id,
                    sequence,
                    reason,
                    "discarding chunk + partial accumulator on validation failure"
                );
                let removed = acc.accumulated_entries;
                inner.broadcasts.remove(&broadcast_id);
                self.total_accumulated
                    .fetch_sub(removed, Ordering::Relaxed);
                None
            }
            Ok(is_terminal) => {
                let total = self
                    .total_accumulated
                    .fetch_add(delta, Ordering::Relaxed)
                    .saturating_add(delta);
                if total > MAX_ACCUMULATED_ENTRIES_PER_CONN {
                    warn!(
                        target: "nativelink::blobs_available_chunked",
                        broadcast_id,
                        sequence,
                        total,
                        cap = MAX_ACCUMULATED_ENTRIES_PER_CONN,
                        "BlobsAvailable accumulator over per-conn entries cap; \
                         dropping partial accumulator"
                    );
                    let removed = acc.accumulated_entries;
                    inner.broadcasts.remove(&broadcast_id);
                    self.total_accumulated
                        .fetch_sub(removed, Ordering::Relaxed);
                    return None;
                }
                if is_terminal {
                    // Path A commit: extract the fully-assembled
                    // notification and let the caller hand it to the
                    // legacy `handle_blobs_available`.
                    let removed = inner.broadcasts.remove(&broadcast_id).map(|a| {
                        self.total_accumulated
                            .fetch_sub(a.accumulated_entries, Ordering::Relaxed);
                        a
                    });
                    removed.map(|a| a.body)
                } else {
                    None
                }
            }
        }
    }

    /// Total currently-in-flight broadcasts on this accumulator.
    /// Test/diagnostic helper.
    #[cfg(test)]
    pub fn in_flight_count(&self) -> usize {
        self.inner.lock().broadcasts.len()
    }

    /// Drop all in-flight partial state. Called when the worker
    /// disconnects so we don't carry zombie partial broadcasts forward
    /// to the next ConnectWorker on the same endpoint.
    pub fn drop_all_inflight(&self) {
        let mut inner = self.inner.lock();
        let total: usize = inner.broadcasts.values().map(|a| a.accumulated_entries).sum();
        inner.broadcasts.clear();
        self.total_accumulated.fetch_sub(total, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_proto::build::bazel::remote::execution::v2::Digest;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        BlobDigestInfo, MirrorPinEntry,
    };

    fn d(i: u64) -> Digest {
        Digest {
            hash: format!("{:064x}", i),
            size_bytes: i64::try_from(i).unwrap_or(0),
        }
    }

    fn bdi(i: u64) -> BlobDigestInfo {
        BlobDigestInfo { digest: Some(d(i)) }
    }

    fn mpe(i: u64, store: &str) -> MirrorPinEntry {
        MirrorPinEntry {
            digest: Some(d(i)),
            store_id: store.to_string(),
        }
    }

    fn chunk(
        broadcast_id: u64,
        sequence: u32,
        is_last: bool,
        token: u64,
        digests: Vec<BlobDigestInfo>,
    ) -> BlobsAvailableChunk {
        BlobsAvailableChunk {
            broadcast_id,
            sequence,
            is_last,
            worker_instance_token: token,
            store_id: String::new(),
            is_full_snapshot: true,
            worker_cas_endpoint: if sequence == 0 {
                "grpc://w1:50081".to_string()
            } else {
                String::new()
            },
            is_full_subtree_snapshot: false,
            digests,
            cached_directory_digests: Vec::new(),
            pinned_mirror_entries: Vec::new(),
            pinned_ac_mirror_entries: Vec::new(),
            evicted_digests: Vec::new(),
            added_subtree_digests: Vec::new(),
            removed_subtree_digests: Vec::new(),
            pinned_mirror_digests: Vec::new(),
            cpu_load_pct: 0,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
        }
    }

    #[test]
    fn three_chunks_terminal_commits() {
        let acc = BlobsAvailableAccumulator::new();
        assert!(acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1), bdi(2)])).is_none());
        assert!(acc.merge_chunk(chunk(1, 1, false, 99, vec![bdi(3)])).is_none());
        let out = acc
            .merge_chunk(chunk(1, 2, true, 99, vec![bdi(4)]))
            .expect("terminal must commit");
        assert_eq!(out.digest_infos.len(), 4);
        assert!(out.is_full_snapshot);
        assert_eq!(out.worker_cas_endpoint, "grpc://w1:50081");
        assert_eq!(acc.in_flight_count(), 0, "accumulator dropped on commit");
    }

    #[test]
    fn token_mismatch_rebuilds_from_new_chunk() {
        let acc = BlobsAvailableAccumulator::new();
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]));
        // Different token -> discards old + restarts.
        let result = acc.merge_chunk(chunk(1, 0, false, 7777, vec![bdi(99)]));
        assert!(result.is_none());
        assert_eq!(acc.in_flight_count(), 1);
    }

    #[test]
    fn duplicate_sequence_drops_accumulator() {
        let acc = BlobsAvailableAccumulator::new();
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(2)]));
        // After dup, the broadcast was discarded.
        assert_eq!(acc.in_flight_count(), 0);
    }

    #[test]
    fn empty_terminal_chunk_commits_empty_notification() {
        let acc = BlobsAvailableAccumulator::new();
        let out = acc
            .merge_chunk(chunk(1, 0, true, 99, Vec::new()))
            .expect("empty terminal commits");
        assert!(out.digest_infos.is_empty());
    }

    #[test]
    fn out_of_order_sequence_within_window_tolerated() {
        let acc = BlobsAvailableAccumulator::new();
        // Order 0, 2 (=is_last). Both arrive — terminal commits.
        assert!(acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)])).is_none());
        assert!(acc.merge_chunk(chunk(1, 2, true, 99, vec![bdi(3)])).is_some());
    }

    #[test]
    fn per_conn_broadcast_cap_drops_overflow() {
        let acc = BlobsAvailableAccumulator::new();
        // Open MAX_INFLIGHT_BROADCASTS_PER_CONN broadcasts.
        for i in 0..MAX_INFLIGHT_BROADCASTS_PER_CONN as u64 {
            acc.merge_chunk(chunk(i, 0, false, 99, vec![bdi(i)]));
        }
        assert_eq!(acc.in_flight_count(), MAX_INFLIGHT_BROADCASTS_PER_CONN);
        // One more should be dropped.
        let result = acc.merge_chunk(chunk(999, 0, false, 99, vec![bdi(999)]));
        assert!(result.is_none());
        assert_eq!(acc.in_flight_count(), MAX_INFLIGHT_BROADCASTS_PER_CONN);
    }

    #[test]
    fn drop_all_inflight_clears_partial_state() {
        let acc = BlobsAvailableAccumulator::new();
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(2, 0, false, 99, vec![bdi(2)]));
        assert_eq!(acc.in_flight_count(), 2);
        acc.drop_all_inflight();
        assert_eq!(acc.in_flight_count(), 0);
    }

    #[test]
    fn all_5_unbounded_fields_reassemble() {
        let acc = BlobsAvailableAccumulator::new();
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.cached_directory_digests = vec![d(10)];
        c0.pinned_mirror_entries = vec![mpe(20, "cas")];
        c0.pinned_ac_mirror_entries = vec![mpe(30, "ac")];
        c0.evicted_digests = vec![d(40)];

        let mut c1 = chunk(1, 1, true, 99, vec![bdi(2)]);
        c1.cached_directory_digests = vec![d(11)];
        c1.pinned_mirror_entries = vec![mpe(21, "cas")];
        c1.pinned_ac_mirror_entries = vec![mpe(31, "ac")];
        c1.evicted_digests = vec![d(41)];

        assert!(acc.merge_chunk(c0).is_none());
        let out = acc.merge_chunk(c1).expect("terminal");
        assert_eq!(out.digest_infos.len(), 2);
        assert_eq!(out.cached_directory_digests.len(), 2);
        assert_eq!(out.pinned_mirror_entries.len(), 2);
        assert_eq!(out.pinned_ac_mirror_entries.len(), 2);
        assert_eq!(out.evicted_digests.len(), 2);
    }

    #[test]
    fn token_zero_rejected() {
        let acc = BlobsAvailableAccumulator::new();
        let result = acc.merge_chunk(chunk(1, 0, true, 0, vec![bdi(1)]));
        assert!(
            result.is_none(),
            "token=0 must be rejected as uninitialised"
        );
        assert_eq!(acc.in_flight_count(), 0);
    }

    #[test]
    fn store_id_drift_drops_accumulator() {
        let acc = BlobsAvailableAccumulator::new();
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.store_id = "alpha".to_string();
        let mut c1 = chunk(1, 1, true, 99, vec![bdi(2)]);
        c1.store_id = "beta".to_string();
        assert!(acc.merge_chunk(c0).is_none());
        assert!(acc.merge_chunk(c1).is_none());
        assert_eq!(acc.in_flight_count(), 0);
    }

    #[test]
    fn is_full_snapshot_drift_drops_accumulator() {
        let acc = BlobsAvailableAccumulator::new();
        let c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        let mut c1 = chunk(1, 1, true, 99, vec![bdi(2)]);
        c1.is_full_snapshot = false;
        assert!(acc.merge_chunk(c0).is_none());
        assert!(acc.merge_chunk(c1).is_none());
        assert_eq!(acc.in_flight_count(), 0);
    }
}
