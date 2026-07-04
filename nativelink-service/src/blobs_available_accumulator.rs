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
//!
//! **Sequence-completeness gate (post-invariant-prover).** At terminal
//! commit, the accumulator verifies that every sequence in
//! `[0, max_seen_sequence]` has landed AND that `max_seen_sequence ==
//! sequence_count - 1` (which is the bit pattern checked via
//! `seen_sequences == (1 << sequence_count) - 1`). If gaps are present
//! the partial accumulator is dropped and the worker re-broadcasts on
//! the next tick. Without this gate, a terminal chunk arriving
//! out-of-order or with intermediate chunks lost commits a strict-
//! subset partial — the half-applied-snapshot bug class the entire
//! design is meant to prevent.
//!
//! **Header-scalar gate.** Header scalars (worker_cas_endpoint,
//! cpu_load_pct, mirror_used_bytes, etc.) only ride on `sequence == 0`
//! per the wire contract. The accumulator stores them in
//! `Option<HeaderScalars>`, populated only when `sequence == 0` lands.
//! If the terminal commits without `sequence == 0` ever having
//! arrived, refuse to commit (subsumed by the sequence-completeness
//! gate above).

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableChunk, BlobsAvailableNotification,
};
use parking_lot::Mutex;
use tracing::{debug, warn};

/// (FL-688 v3 §3.8) Outcome of [`BlobsAvailableAccumulator::merge_chunk_outcome`].
///
/// Distinguishes "the chunk was ACCEPTED into the accumulator" (the
/// server should send a `BlobsAvailableAck` so the worker drops it from
/// its resend buffer) from "the chunk was DROPPED" (a validation / cap /
/// sequence-completeness failure — the server must NOT ack, so the worker
/// keeps it buffered and re-advertises on reconnect). This is the same
/// acceptance/drop discipline as the reverse-direction `handle_bis_chunk`
/// ack-gate, where a partial failure suppresses the ack.
#[derive(Debug)]
pub enum MergeOutcome {
    /// The chunk merged successfully. `Some(notification)` iff this was
    /// the terminal chunk and the sequence-completeness gate passed (the
    /// caller commits it via `handle_blobs_available`); `None` for an
    /// accepted non-terminal chunk. EITHER way the server acks it.
    Accepted(Option<BlobsAvailableNotification>),
    /// The chunk was rejected (token mismatch/zero, a per-chunk/per-conn
    /// cap, or a terminal that failed the completeness/chunk-0 gate). The
    /// partial state is dropped; the server must NOT ack.
    Dropped,
}

/// CAPPED AT 8: a misbehaving / wedged worker emitting chunks for new
/// `broadcast_id`s without ever sending `is_last=true` would otherwise
/// grow the accumulator monotonically. Steady-state rate is 1 broadcast
/// active at a time per worker (the send loop emits chunks then waits
/// for the next tick); 8 is 8× that for over-provisioning. Falsification:
/// a synthetic-load test driving 800 broadcasts (10× cap) MUST evict
/// oldest under cap pressure rather than OOM.
pub(crate) const MAX_INFLIGHT_BROADCASTS_PER_CONN: usize = 8;

/// CAPPED AT 1_000_000: post-#99-fixup raise to match the AC-pin
/// per-endpoint cap (`DEFAULT_MAX_AC_PINS_PER_ENDPOINT = 1_000_000` in
/// `ac_pin_registry.rs:229`). A worker holding up to 1M AC pins (or 1M
/// digests + cached_directory_digests + pinned_mirror_entries combined)
/// must be able to round-trip its full snapshot through the chunked
/// path; the prior 200K cap silently dropped legitimate worker traffic
/// (red-team BLOCK / dsr BLOCK-1).
///
/// At ~100 B per entry (worst-case `MirrorPinEntry` plus `Vec`
/// overhead), that's ~100 MB per connection. With ~10 worker
/// connections in production, ~1 GB worst-case heap — well inside the
/// 80 GB MemoryMax. A worker emitting 1M+ entries across one broadcast
/// is producing a fleet-replay snapshot the chunker is supposed to
/// transport, NOT something we should silently drop.
///
/// Falsification: a 2M-entry synthetic broadcast (2× the cap) MUST
/// trigger eviction and warn rather than OOM. See test
/// `entries_cap_drops_partial_at_2x_threshold`.
pub(crate) const MAX_ACCUMULATED_ENTRIES_PER_CONN: usize = 1_000_000;

/// Maximum supported sequence count per broadcast. Bound MUST exceed
/// the worst-case chunk count emitted by the worker chunker so that
/// large broadcasts can round-trip without silent server-side discard.
///
/// Sized so that
/// `MAX_SEQUENCES * BLOBS_AVAILABLE_PER_CHUNK >= MAX_ACCUMULATED_ENTRIES_PER_CONN`:
///   256 sequences × 4096 entries/chunk = 1_048_576 entries — slightly
///   above the 1M per-conn entries cap, which is the relevant
///   first-to-fire ceiling.
///
/// The bitset is held in `u128` (256/2 bits) — see the doc on
/// `BroadcastAccumulator::seen_sequences_lo`/`_hi`.
pub(crate) const MAX_SEQUENCES: u32 = 256;

/// Cap on payload entries in a SINGLE chunk. Defends against a worker
/// (or a malicious client masquerading as one) emitting an oversized
/// chunk that overshoots the `MAX_ACCUMULATED_ENTRIES_PER_CONN` cap by
/// many MB BEFORE the cap-check fires (security review M1).
///
/// `BLOBS_AVAILABLE_PER_CHUNK` (the chunker's per-chunk soft cap) is
/// 4096; 2× that gives ~10 KiB headroom for proto overhead while
/// preventing transient peak overshoot.
pub(crate) const MAX_ENTRIES_PER_CHUNK: usize = 8192;

/// Header scalars copied verbatim from `chunk.sequence == 0`. Stored
/// as `Option<HeaderScalars>` so a broadcast that COMMITS without
/// `sequence == 0` ever having arrived (gap in priors) can be
/// rejected at terminal time — sequence-completeness gate subsumes
/// this, but storing as `Option` makes the contract explicit.
#[derive(Debug, Default, Clone)]
struct HeaderScalars {
    worker_cas_endpoint: String,
    is_full_subtree_snapshot: bool,
    cpu_load_pct: u32,
    p_core_load_pct: u32,
    e_core_load_pct: u32,
    mirror_used_bytes: u64,
    mirror_max_bytes: u64,
    // (FL-681) Indefinite-pin-cap saturation, carried from chunk 0.
    indefinite_pin_saturated: bool,
    // Host memory pressure, carried from chunk 0 (#37 rev-4).
    // `swap_used_bytes` + `memory_pressure_level` are OBSERVABILITY scalars
    // (the latter is MiB below the free-floor + the fail-open ranking key);
    // `memory_pressured` is the coarse gate verdict the matcher consumes via
    // `update_worker_swap_pressure` after reassembly.
    swap_used_bytes: u64,
    memory_pressure_level: u32,
    memory_pressured: bool,
    // (F4) Disk pressure, carried from chunk 0. `available_disk_bytes` is the
    // observability + least-pressured fail-open ranking key (MORE free = LESS
    // pressured); `disk_pressured` is the coarse gate verdict the matcher
    // consumes via `update_worker_disk_pressure` after reassembly.
    available_disk_bytes: u64,
    disk_pressured: bool,
}

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
    /// Set on chunk-0; carried forward across all chunks (the chunker
    /// repeats this on every chunk and the accumulator validates
    /// consistency).
    is_full_snapshot: bool,
    /// Header scalars from `sequence == 0`, populated only when that
    /// chunk arrives. `None` means `sequence == 0` has not landed
    /// yet.
    header_scalars: Option<HeaderScalars>,
    /// Per-chunk slice payload — accumulated across chunks, applied
    /// only on terminal commit.
    body: BlobsAvailableNotification,
    /// Sum of entries across all 8 payload slices accumulated so far.
    /// Used to drive the per-connection accumulated-entries cap.
    accumulated_entries: usize,
    /// Bitset of seen `sequence` values, low 128 bits (sequences 0-127).
    seen_sequences_lo: u128,
    /// Bitset of seen `sequence` values, high 128 bits (sequences
    /// 128-255).
    seen_sequences_hi: u128,
    /// Largest `sequence` value observed so far. Used by the
    /// sequence-completeness gate at terminal commit.
    max_seen_sequence: u32,
}

impl BroadcastAccumulator {
    /// Create a fresh accumulator. The caller MUST then call
    /// `merge(first_chunk)` to fold in the chunk's payload + header.
    /// Accumulator state begins empty; nothing about the first chunk
    /// is read at construction time (that's the merge's job).
    fn new(first_chunk: &BlobsAvailableChunk) -> Self {
        Self {
            worker_instance_token: first_chunk.worker_instance_token,
            store_id: first_chunk.store_id.clone(),
            is_full_snapshot: first_chunk.is_full_snapshot,
            header_scalars: None,
            body: BlobsAvailableNotification::default(),
            accumulated_entries: 0,
            seen_sequences_lo: 0,
            seen_sequences_hi: 0,
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
            return Err("chunk sequence exceeds MAX_SEQUENCES (256) per broadcast");
        }
        let bit_lo = if chunk.sequence < 128 {
            1u128 << chunk.sequence
        } else {
            0
        };
        let bit_hi = if chunk.sequence >= 128 {
            1u128 << (chunk.sequence - 128)
        } else {
            0
        };
        let already_seen = (self.seen_sequences_lo & bit_lo) != 0
            || (self.seen_sequences_hi & bit_hi) != 0;
        if already_seen {
            return Err("duplicate sequence within one broadcast");
        }
        self.seen_sequences_lo |= bit_lo;
        self.seen_sequences_hi |= bit_hi;
        if chunk.sequence > self.max_seen_sequence {
            self.max_seen_sequence = chunk.sequence;
        }

        // Header scalars: populated only on sequence == 0. If
        // sequence > 0 carries non-default scalars, that's a chunker
        // bug (chunker MUST emit defaults for non-zero sequences); we
        // do not silently absorb them.
        if chunk.sequence == 0 {
            // Defensive: refuse if seq=0 already populated (would
            // already have been caught by the duplicate-sequence
            // check above, but the explicit handling here makes the
            // contract clearer).
            self.header_scalars = Some(HeaderScalars {
                worker_cas_endpoint: chunk.worker_cas_endpoint.clone(),
                is_full_subtree_snapshot: chunk.is_full_subtree_snapshot,
                cpu_load_pct: chunk.cpu_load_pct,
                p_core_load_pct: chunk.p_core_load_pct,
                e_core_load_pct: chunk.e_core_load_pct,
                mirror_used_bytes: chunk.mirror_used_bytes,
                mirror_max_bytes: chunk.mirror_max_bytes,
                indefinite_pin_saturated: chunk.indefinite_pin_saturated,
                swap_used_bytes: chunk.swap_used_bytes,
                memory_pressure_level: chunk.memory_pressure_level,
                memory_pressured: chunk.memory_pressured,
                available_disk_bytes: chunk.available_disk_bytes,
                disk_pressured: chunk.disk_pressured,
            });
        }

        let entries_in_chunk = entries_in_chunk(&chunk);
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
        // (#locality-map-drift) Merge the ts-carrying eviction list too — the
        // chunked path is the PRODUCTION path (all ByteStream CAS writes), so
        // dropping it here would make the ts-gate inert in prod.
        self.body
            .evicted_blob_infos
            .extend(chunk.evicted_blob_infos);
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

    /// Sequence-completeness gate: returns true iff `seen_sequences`
    /// is exactly `[0, max_seen_sequence]` (no gaps). Called at
    /// terminal-commit time; if false, the partial is rejected.
    fn sequences_contiguous(&self) -> bool {
        let count = (self.max_seen_sequence as usize).saturating_add(1);
        if count <= 128 {
            let expected = if count == 128 {
                u128::MAX
            } else {
                (1u128 << count) - 1
            };
            self.seen_sequences_lo == expected && self.seen_sequences_hi == 0
        } else if count <= 256 {
            let high_count = count - 128;
            let expected_hi = if high_count == 128 {
                u128::MAX
            } else {
                (1u128 << high_count) - 1
            };
            self.seen_sequences_lo == u128::MAX && self.seen_sequences_hi == expected_hi
        } else {
            // Unreachable; MAX_SEQUENCES caps sequence at 255.
            false
        }
    }

    /// Format the seen-sequence bitset for diagnostic logging.
    fn seen_sequences_debug(&self) -> String {
        format!(
            "lo=0x{:032x} hi=0x{:032x}",
            self.seen_sequences_lo, self.seen_sequences_hi
        )
    }
}

/// Sum of entries across all payload slices on a chunk.
fn entries_in_chunk(chunk: &BlobsAvailableChunk) -> usize {
    chunk.digests.len()
        + chunk.cached_directory_digests.len()
        + chunk.pinned_mirror_entries.len()
        + chunk.pinned_ac_mirror_entries.len()
        + chunk.evicted_digests.len()
        // (#locality-map-drift) count the dual-emitted ts eviction list too.
        + chunk.evicted_blob_infos.len()
        + chunk.added_subtree_digests.len()
        + chunk.removed_subtree_digests.len()
        + chunk.pinned_mirror_digests.len()
}

/// Per-reason drop counters. Surface server-side chunk-drop reasons
/// to operators (dsr MAJOR-1: worker has no observability into
/// silent server-side drops without these). Each counter is bumped
/// whenever the matching code path fires; warn-level logs carry the
/// reason string AND the counter values stay queryable for dashboards.
///
/// `#[derive(MetricsComponent)]` (S1 from the 8fa531ae code-reviewer
/// pass) makes these counters discoverable on the metrics tree
/// — without it the AtomicU64s live only in process memory and never
/// reach Prometheus scrapes (cf. red-team BLOCK-2 on `WorkerApiMetrics`
/// at `worker_api_server.rs:171-201`). The counters get wired to the
/// `WorkerApiServer` parent surface via an `Arc<ChunkDropCounts>` field
/// on `WorkerApiMetrics`, shared (Arc-cloned) into every
/// `WorkerConnection`'s accumulator so per-connection drops aggregate
/// into a single set of server-wide counters. Note: the
/// `RootMetricsComponent` publisher itself is NOT wired in
/// `bin/nativelink.rs` today (separate tracker #160 — Wire
/// RootMetricsComponent publisher in src/bin/nativelink.rs); this
/// change pre-positions so when #160 lands, chunk-drop counters appear
/// on dashboards without further plumbing.
#[derive(Debug, Default, MetricsComponent)]
pub struct ChunkDropCounts {
    #[metric(
        help = "Total BlobsAvailable chunks rejected because their \
                worker_instance_token field was 0 (uninitialised — \
                pre-fixup or buggy worker per the #97 precedent)."
    )]
    pub dropped_token_zero: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks that triggered a token-mismatch \
                rebuild — the partial accumulator for the broadcast was \
                discarded because a chunk arrived with a worker_instance_token \
                differing from the one that started the broadcast (worker \
                process restart mid-broadcast; impossible in steady state)."
    )]
    pub dropped_token_mismatch: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks dropped because the per-connection \
                in-flight broadcast cap (MAX_INFLIGHT_BROADCASTS_PER_CONN = 8) \
                was reached. Indicates a worker emitted chunks for new \
                broadcast_ids without ever sending the terminal is_last."
    )]
    pub dropped_per_conn_broadcasts_cap: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks dropped because the per-connection \
                accumulated-entries cap (MAX_ACCUMULATED_ENTRIES_PER_CONN = \
                1_000_000) projection exceeded the cap. The partial accumulator \
                is dropped; the worker re-broadcasts on the next tick."
    )]
    pub dropped_per_conn_entries_cap: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks rejected because a single chunk \
                carried more than MAX_ENTRIES_PER_CHUNK entries (security M1 \
                / Fix #11; defends against transient peak-memory overshoot \
                from a hostile chunk)."
    )]
    pub dropped_per_chunk_entries_cap: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks rejected because the chunk's \
                sequence value exceeded MAX_SEQUENCES (256). Indicates a \
                worker chunker bug — chunker MUST cap sequence at 255 per \
                BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST."
    )]
    pub dropped_sequence_cap: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable chunks rejected by other validation \
                gates (store_id mismatch within one broadcast, \
                is_full_snapshot inconsistency across chunks, duplicate \
                sequence within one broadcast). The partial accumulator is \
                dropped; the worker re-broadcasts on the next tick."
    )]
    pub dropped_validation_other: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable terminal commits rejected because the \
                sequence-completeness gate observed a gap in [0, \
                max_seen_sequence] (Fix #1 / invariant-prover BLOCK). \
                Without this gate a partial commit half-applies a strict \
                subset of the worker's snapshot — the bug class the entire \
                Path A semantics is meant to prevent."
    )]
    pub dropped_incomplete_sequence: AtomicU64,
    #[metric(
        help = "Total BlobsAvailable terminal commits rejected because the \
                sequence=0 chunk (which carries header scalars) never \
                arrived (Fix #3). Without header_scalars the legacy \
                handle_blobs_available cannot route the snapshot."
    )]
    pub dropped_missing_chunk_zero: AtomicU64,
}

impl ChunkDropCounts {
    /// Snapshot all counters as a `HashMap` for testing or metrics
    /// export.
    #[cfg(test)]
    pub fn snapshot(&self) -> HashMap<&'static str, u64> {
        let mut out = HashMap::new();
        out.insert(
            "dropped_token_zero",
            self.dropped_token_zero.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_token_mismatch",
            self.dropped_token_mismatch.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_per_conn_broadcasts_cap",
            self.dropped_per_conn_broadcasts_cap
                .load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_per_conn_entries_cap",
            self.dropped_per_conn_entries_cap.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_per_chunk_entries_cap",
            self.dropped_per_chunk_entries_cap.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_sequence_cap",
            self.dropped_sequence_cap.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_validation_other",
            self.dropped_validation_other.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_incomplete_sequence",
            self.dropped_incomplete_sequence.load(Ordering::Relaxed),
        );
        out.insert(
            "dropped_missing_chunk_zero",
            self.dropped_missing_chunk_zero.load(Ordering::Relaxed),
        );
        out
    }
}

/// One per `WorkerApiServerInstance` (one per worker connection).
/// Holds the in-flight per-broadcast accumulators.
#[derive(Debug, Default)]
pub struct BlobsAvailableAccumulator {
    inner: Mutex<AccumulatorInner>,
    /// Per-reason drop counters; queryable by tests and metrics
    /// dashboards. (dsr MAJOR-1)
    ///
    /// `Arc<ChunkDropCounts>` so the production wiring can share a single
    /// counter pool across every per-connection accumulator. Each
    /// `BlobsAvailableAccumulator::new()` call creates a fresh
    /// (per-test) `Arc`; production constructs via
    /// `BlobsAvailableAccumulator::new_with_drop_counts(shared)` so
    /// every connection bumps the same set of counters that
    /// `WorkerApiMetrics::chunked_blobs_available_drop_counts`
    /// publishes.
    pub drop_counts: Arc<ChunkDropCounts>,
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
    /// Total entries summed across all in-flight broadcasts. Always
    /// accessed under the same `inner` lock as `broadcasts`; not
    /// atomic.
    total_accumulated: usize,
}

impl BlobsAvailableAccumulator {
    /// Construct with a fresh per-instance `ChunkDropCounts`. Used by
    /// tests; production callers should prefer
    /// `new_with_drop_counts` so all per-connection accumulators
    /// aggregate into a single counter pool that the metrics tree
    /// publishes once.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Construct with a shared `Arc<ChunkDropCounts>`. The same Arc
    /// must be the one held by `WorkerApiMetrics::chunked_blobs_available_drop_counts`
    /// so per-connection drops aggregate into a single set of
    /// server-wide counters that surface on the metrics tree (S1 from
    /// the 8fa531ae code-reviewer pass).
    pub fn new_with_drop_counts(drop_counts: Arc<ChunkDropCounts>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(AccumulatorInner::default()),
            drop_counts,
        })
    }

    /// Process one chunk. Returns `Some(notification)` when this chunk
    /// is the terminal of its broadcast AND the sequence-completeness
    /// gate passes; the caller should pass the fully-assembled
    /// `BlobsAvailableNotification` to the legacy
    /// `handle_blobs_available` (the Path-A commit point). Returns
    /// `None` for non-terminal chunks (caller does nothing) and for
    /// terminal chunks that fail the completeness gate (silent
    /// drop + warn).
    ///
    /// Validation failures (token mismatch, duplicate sequence, store_id
    /// drift, accumulator over cap) drop the partial state and return
    /// `None`; the worker MUST then re-broadcast on the next tick.
    ///
    /// This is a thin `Option`-returning wrapper over
    /// [`Self::merge_chunk_outcome`] preserved for the many existing
    /// callers/tests that only care about the terminal notification. The
    /// `BlobsAvailableAck` server path uses `merge_chunk_outcome` so it
    /// can distinguish "accepted (ack it)" from "dropped (do NOT ack so
    /// the worker resends)".
    pub fn merge_chunk(&self, chunk: BlobsAvailableChunk) -> Option<BlobsAvailableNotification> {
        match self.merge_chunk_outcome(chunk) {
            MergeOutcome::Accepted(notification) => notification,
            MergeOutcome::Dropped => None,
        }
    }

    /// (FL-688 v3 §3.8) Process one chunk, reporting whether it was
    /// ACCEPTED (merged into the accumulator — the server should send a
    /// `BlobsAvailableAck` so the worker drops it from its resend buffer)
    /// or DROPPED (a validation/cap/completeness failure — the server
    /// must NOT ack so the worker keeps it buffered and re-advertises on
    /// reconnect). A terminal accept additionally carries the assembled
    /// `BlobsAvailableNotification` to commit. This is the same
    /// acceptance/drop discipline as `handle_bis_chunk`'s ack-gate in the
    /// reverse direction (a partial failure suppresses the ack).
    pub fn merge_chunk_outcome(&self, chunk: BlobsAvailableChunk) -> MergeOutcome {
        let broadcast_id = chunk.broadcast_id;
        let token = chunk.worker_instance_token;
        let sequence = chunk.sequence;

        // Defensive: 0 token is "uninitialised" — reject. Per #97
        // precedent: a 0 server_instance_token meant a pre-fixup
        // worker; in #99 the analog is a pre-fixup or buggy worker.
        if token == 0 {
            self.drop_counts
                .dropped_token_zero
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                target: "nativelink::blobs_available_chunked",
                broadcast_id,
                sequence,
                reason = "token_zero",
                "rejecting BlobsAvailableChunk with worker_instance_token=0 (uninitialised)"
            );
            return MergeOutcome::Dropped;
        }

        // Per-chunk entry-count cap (security M1 / Fix #11). Defends
        // against a transient memory overshoot when a single hostile
        // chunk carries far more entries than `BLOBS_AVAILABLE_PER_CHUNK`.
        // Fired BEFORE merge so peak accumulator memory stays bounded
        // by `MAX_ACCUMULATED_ENTRIES_PER_CONN + MAX_ENTRIES_PER_CHUNK`,
        // not `MAX_ACCUMULATED_ENTRIES_PER_CONN + (one chunk worth of
        // hostile data)`.
        let in_chunk = entries_in_chunk(&chunk);
        if in_chunk > MAX_ENTRIES_PER_CHUNK {
            self.drop_counts
                .dropped_per_chunk_entries_cap
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                target: "nativelink::blobs_available_chunked",
                broadcast_id,
                sequence,
                in_chunk,
                cap = MAX_ENTRIES_PER_CHUNK,
                reason = "per_chunk_entries_cap",
                "rejecting BlobsAvailableChunk with oversized per-chunk entry count"
            );
            return MergeOutcome::Dropped;
        }

        let mut inner = self.inner.lock();

        // Per-conn broadcast count cap.
        if !inner.broadcasts.contains_key(&broadcast_id)
            && inner.broadcasts.len() >= MAX_INFLIGHT_BROADCASTS_PER_CONN
        {
            self.drop_counts
                .dropped_per_conn_broadcasts_cap
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                target: "nativelink::blobs_available_chunked",
                in_flight = inner.broadcasts.len(),
                cap = MAX_INFLIGHT_BROADCASTS_PER_CONN,
                broadcast_id,
                reason = "per_conn_broadcasts_cap",
                "BlobsAvailable accumulator at per-conn broadcast cap; \
                 dropping new broadcast — worker likely emitted chunks \
                 but never sent is_last=true"
            );
            return MergeOutcome::Dropped;
        }

        // Per-conn entries cap pre-merge (Fix #11): bound transient
        // peak memory by checking the projected total BEFORE applying
        // the merge. Without this, the cap-check would fire AFTER the
        // chunk's payload was already extended into the accumulator's
        // body — peak memory could overshoot by one chunk's worth
        // before the broadcast is dropped.
        //
        // Inlined fresh read of `inner.total_accumulated` (no
        // `prev_total` local) per red-team + assumption-auditor:
        // snapshot-then-write-back is the exact anti-pattern we just
        // fixed in the Ok branch (commit f8fd6fd2). This site is
        // currently safe — the early-return below means no write-back
        // happens after the cap fires — but a future refactor that
        // moves a write-back into this site (e.g. recovery bookkeeping)
        // would re-introduce the bug. Keeping the read as a fresh
        // expression makes the cap-projection-only intent explicit.
        let projected_total = inner.total_accumulated.saturating_add(in_chunk);
        if projected_total > MAX_ACCUMULATED_ENTRIES_PER_CONN {
            self.drop_counts
                .dropped_per_conn_entries_cap
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                target: "nativelink::blobs_available_chunked",
                broadcast_id,
                sequence,
                projected_total,
                cap = MAX_ACCUMULATED_ENTRIES_PER_CONN,
                reason = "per_conn_entries_cap",
                "BlobsAvailable accumulator pre-merge cap projection \
                 exceeds per-conn entries cap; dropping partial accumulator"
            );
            // Drop the in-flight accumulator (if any) so subsequent
            // chunks don't get re-accumulated into stale state.
            if let Some(removed) = inner.broadcasts.remove(&broadcast_id) {
                inner.total_accumulated = inner
                    .total_accumulated
                    .saturating_sub(removed.accumulated_entries);
            }
            return MergeOutcome::Dropped;
        }

        // Token-mismatch rebuild is handled with a brief Occupied
        // borrow that drops before we re-grab a mutable reference to
        // do the merge. This avoids cross-field borrows of `inner`
        // during the merge.
        if let Some(existing) = inner.broadcasts.get(&broadcast_id) {
            if existing.worker_instance_token != token {
                self.drop_counts
                    .dropped_token_mismatch
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    target: "nativelink::blobs_available_chunked",
                    broadcast_id,
                    old_token = existing.worker_instance_token,
                    new_token = token,
                    reason = "token_mismatch",
                    "discarding partial accumulator on token mismatch"
                );
                let removed_entries = existing.accumulated_entries;
                inner.total_accumulated =
                    inner.total_accumulated.saturating_sub(removed_entries);
                inner.broadcasts.insert(broadcast_id, BroadcastAccumulator::new(&chunk));
            }
        } else {
            inner
                .broadcasts
                .insert(broadcast_id, BroadcastAccumulator::new(&chunk));
        }

        // SAFETY: we just inserted (or kept) the entry; unwrap is fine.
        let acc = inner
            .broadcasts
            .get_mut(&broadcast_id)
            .expect("broadcast_id was just inserted/preserved");
        let prev_entries = acc.accumulated_entries;
        let merge_result = acc.merge(chunk);
        let new_entries = acc.accumulated_entries;
        let delta = new_entries.saturating_sub(prev_entries);
        // Snapshot what we need from `acc` before re-borrowing `inner`.
        let acc_accumulated_entries = acc.accumulated_entries;

        match merge_result {
            Err(reason) => {
                // Specific drop-reason counter selection.
                if reason.starts_with("chunk sequence exceeds MAX_SEQUENCES") {
                    self.drop_counts
                        .dropped_sequence_cap
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.drop_counts
                        .dropped_validation_other
                        .fetch_add(1, Ordering::Relaxed);
                }
                warn!(
                    target: "nativelink::blobs_available_chunked",
                    broadcast_id,
                    sequence,
                    reason,
                    "discarding chunk + partial accumulator on validation failure"
                );
                inner.broadcasts.remove(&broadcast_id);
                // Subtraction safety: `BroadcastAccumulator::merge` only
                // mutates `accumulated_entries` AFTER every validation
                // gate (the `entries_in_chunk(&chunk)` + saturating_add
                // is reached only past every `Err` return). So when
                // merge() returns Err:
                //   - keep-existing case: acc was the prior accumulator
                //     and `acc_accumulated_entries == prev_entries`,
                //     which is already counted in inner.total_accumulated
                //     from PRIOR successful merges. Subtracting it
                //     correctly removes those prior contributions, and
                //     the failing chunk's payload (still in `chunk`,
                //     never extended into acc.body) carries no
                //     accumulator state forward.
                //   - new / token-mismatch-rebuild case: acc is the
                //     freshly-constructed `BroadcastAccumulator::new(&chunk)`
                //     whose `accumulated_entries` starts at 0 and was not
                //     mutated by the failing merge() — so subtracting 0
                //     is a no-op and the prior token-mismatch wipe (the
                //     `saturating_sub(removed_entries)` near the
                //     `existing.worker_instance_token != token` branch
                //     above) stands. This branch will not double-subtract.
                // Audit citation: `merge()` Err returns span the early
                // validation gates from the `worker_instance_token`
                // mismatch through the `duplicate sequence within one
                // broadcast` Err — all precede the
                // `accumulated_entries` mutation.
                inner.total_accumulated =
                    inner.total_accumulated.saturating_sub(acc_accumulated_entries);
                MergeOutcome::Dropped
            }
            Ok(is_terminal) => {
                // Read CURRENT total — NOT a pre-wipe snapshot — so the
                // token-mismatch wipe in the
                // `existing.worker_instance_token != token` branch
                // above (the `saturating_sub(removed_entries)` call
                // before `BroadcastAccumulator::new(&chunk)` is
                // re-inserted) is preserved. Pre-fix,
                // `prev_total.saturating_add(delta)` unconditionally
                // overwrote the wipe, leaving the stale broadcast's
                // entries double-counted in `total_accumulated`
                // indefinitely. Regression-tested by
                // `token_mismatch_drift_total_accumulated_consistency`.
                inner.total_accumulated = inner.total_accumulated.saturating_add(delta);
                if !is_terminal {
                    // Accepted a non-terminal chunk: its slice is now in
                    // the accumulator → ack it so the worker drops it from
                    // its resend buffer (drain-on-ack).
                    return MergeOutcome::Accepted(None);
                }
                // Terminal chunk arrived. Apply the sequence-completeness
                // gate (Fix #1, invariant-prover BLOCK).
                let removed = inner.broadcasts.remove(&broadcast_id).expect(
                    "broadcast_id was just merged; HashMap entry must be present",
                );
                inner.total_accumulated = inner
                    .total_accumulated
                    .saturating_sub(removed.accumulated_entries);
                let contiguous = removed.sequences_contiguous();
                let saw_chunk_zero = removed.header_scalars.is_some();

                if !saw_chunk_zero {
                    // Terminal arrived but sequence == 0 never landed:
                    // header scalars are missing. Without
                    // `worker_cas_endpoint`, the legacy handler can't
                    // route the snapshot. Reject.
                    self.drop_counts
                        .dropped_missing_chunk_zero
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "nativelink::blobs_available_chunked",
                        broadcast_id,
                        sequence,
                        max_seen_sequence = removed.max_seen_sequence,
                        reason = "missing_chunk_zero",
                        "BlobsAvailable terminal arrived but sequence=0 \
                         never landed; rejecting partial commit (header \
                         scalars missing)"
                    );
                    return MergeOutcome::Dropped;
                }

                if !contiguous {
                    self.drop_counts
                        .dropped_incomplete_sequence
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "nativelink::blobs_available_chunked",
                        broadcast_id,
                        seen_sequences = %removed.seen_sequences_debug(),
                        max_seen_sequence = removed.max_seen_sequence,
                        reason = "incomplete_sequence",
                        "BlobsAvailable terminal arrived with incomplete \
                         sequence; rejecting partial commit (worker \
                         re-broadcasts on next tick)"
                    );
                    return MergeOutcome::Dropped;
                }

                // Path A commit: assemble the body with header scalars
                // from chunk-0 and return.
                let mut body = removed.body;
                if let Some(headers) = &removed.header_scalars {
                    body.worker_cas_endpoint = headers.worker_cas_endpoint.clone();
                    body.is_full_subtree_snapshot = headers.is_full_subtree_snapshot;
                    body.cpu_load_pct = headers.cpu_load_pct;
                    body.p_core_load_pct = headers.p_core_load_pct;
                    body.e_core_load_pct = headers.e_core_load_pct;
                    body.mirror_used_bytes = headers.mirror_used_bytes;
                    body.mirror_max_bytes = headers.mirror_max_bytes;
                    body.indefinite_pin_saturated = headers.indefinite_pin_saturated;
                    body.swap_used_bytes = headers.swap_used_bytes;
                    body.memory_pressure_level = headers.memory_pressure_level;
                    body.memory_pressured = headers.memory_pressured;
                    body.available_disk_bytes = headers.available_disk_bytes;
                    body.disk_pressured = headers.disk_pressured;
                }
                body.is_full_snapshot = removed.is_full_snapshot;
                MergeOutcome::Accepted(Some(body))
            }
        }
    }

    /// Total currently-in-flight broadcasts on this accumulator.
    /// Test/diagnostic helper.
    #[cfg(test)]
    pub fn in_flight_count(&self) -> usize {
        self.inner.lock().broadcasts.len()
    }

    /// Total accumulated entries summed across all in-flight
    /// broadcasts. Test/diagnostic helper.
    #[cfg(test)]
    pub fn total_accumulated_entries(&self) -> usize {
        self.inner.lock().total_accumulated
    }

    /// Drop all in-flight partial state. Called when the worker
    /// disconnects so we don't carry zombie partial broadcasts forward
    /// to the next ConnectWorker on the same endpoint.
    pub fn drop_all_inflight(&self) {
        let mut inner = self.inner.lock();
        inner.broadcasts.clear();
        inner.total_accumulated = 0;
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
        BlobDigestInfo {
            digest: Some(d(i)),
            ts_boot_epoch: 0,
            ts_counter: 0,
        }
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
            evicted_blob_infos: Vec::new(),
            added_subtree_digests: Vec::new(),
            removed_subtree_digests: Vec::new(),
            pinned_mirror_digests: Vec::new(),
            cpu_load_pct: 0,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            indefinite_pin_saturated: false,
            swap_used_bytes: 0,
            memory_pressure_level: 0,
            memory_pressured: false,
            available_disk_bytes: 0,
            disk_pressured: false,
        }
    }

    #[test]
    fn three_chunks_terminal_commits() {
        let acc = BlobsAvailableAccumulator::new();
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1), bdi(2)]))
                .is_none()
        );
        assert!(
            acc.merge_chunk(chunk(1, 1, false, 99, vec![bdi(3)]))
                .is_none()
        );
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

    /// Regression test for the `total_accumulated` bookkeeping drift on
    /// token-mismatch rebuild, surfaced by the code-reviewer's audit of
    /// the prior fixup at 8fa531ae. Pre-fix `merge_chunk` captured
    /// `prev_total = inner.total_accumulated` BEFORE the token-mismatch
    /// wipe (`saturating_sub(removed_entries)` in the
    /// `existing.worker_instance_token != token` branch of `merge_chunk`)
    /// decremented it, then unconditionally overwrote
    /// `inner.total_accumulated = prev_total + delta` at the end. The
    /// wipe was undone, leaving the stale broadcast's entries
    /// double-counted in `total_accumulated` indefinitely (until next
    /// process restart). Impact: per-conn entries cap fires prematurely
    /// once enough token-mismatches accumulate, dropping legitimate
    /// broadcasts.
    ///
    /// Test sequence:
    ///   1. broadcast 1, token=99, seq=0, N=80 entries, NOT terminal.
    ///      Verify total_accumulated == 80.
    ///   2. broadcast 1, token=7777, seq=0, M=20 entries (token-mismatch
    ///      rebuild fires). ASSERT total_accumulated == 20 (NOT 100).
    ///   3. broadcast 1, token=7777, seq=1 terminal, K=5 entries.
    ///      Verify accumulator drained on commit (total_accumulated == 0).
    ///
    /// Step 2 is the bug-detection assertion. Pre-fix it red-fails with
    /// `left: 100, right: 20`.
    ///
    /// Mutation step (CLAUDE.md TDD #5): revert the fix in the Ok branch
    /// of `merge_chunk` (the `inner.total_accumulated =
    /// inner.total_accumulated.saturating_add(delta)` line — change it
    /// back to `prev_total.saturating_add(delta)`). This test MUST
    /// red-fail at the step-2 assertion with the bespoke message
    /// `expected total_accumulated == M after token-mismatch rebuild,
    /// got total_accumulated == N + M — bookkeeping drift not fixed`.
    #[test]
    fn token_mismatch_drift_total_accumulated_consistency() {
        let acc = BlobsAvailableAccumulator::new();

        // Step 1: open broadcast 1 with token=99, N=80 entries.
        let n_entries = 80usize;
        let payload_n: Vec<BlobDigestInfo> =
            (0..n_entries as u64).map(bdi).collect();
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, payload_n)).is_none(),
            "non-terminal first chunk must not commit"
        );
        assert_eq!(
            acc.total_accumulated_entries(),
            n_entries,
            "after first chunk total_accumulated must equal first-chunk entries"
        );

        // Step 2: token-mismatch rebuild with token=7777, M=20 entries.
        // The token-mismatch wipe (in the
        // `existing.worker_instance_token != token` branch of
        // merge_chunk) must drop the 80 stale entries; the Ok-branch
        // increment (`inner.total_accumulated =
        // inner.total_accumulated.saturating_add(delta)`) must add the
        // 20 new entries on top of the WIPED total, not on top of the
        // pre-wipe snapshot.
        let m_entries = 20usize;
        let payload_m: Vec<BlobDigestInfo> =
            (0..m_entries as u64).map(bdi).collect();
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 7777, payload_m)).is_none(),
            "non-terminal rebuild chunk must not commit"
        );
        // Bug-detection assertion: pre-fix, this fails with left=100 right=20.
        assert_eq!(
            acc.total_accumulated_entries(),
            m_entries,
            "expected total_accumulated == M after token-mismatch rebuild, \
             got total_accumulated == N + M — bookkeeping drift not fixed"
        );
        // Sanity: token_mismatch counter incremented exactly once.
        let snap = acc.drop_counts.snapshot();
        assert_eq!(
            snap["dropped_token_mismatch"], 1,
            "token-mismatch drop counter must increment exactly once on rebuild"
        );

        // Step 3: terminal commit on the rebuilt broadcast. After commit
        // the accumulator must be empty AND total_accumulated zero
        // (the post-commit subtraction in the terminal arm of the Ok
        // branch — `saturating_sub(removed.accumulated_entries)` after
        // `inner.broadcasts.remove(&broadcast_id)` — must cancel the
        // bookkeeping drift if any).
        let k_entries = 5usize;
        let payload_k: Vec<BlobDigestInfo> =
            (0..k_entries as u64).map(bdi).collect();
        let out = acc
            .merge_chunk(chunk(1, 1, true, 7777, payload_k))
            .expect("terminal commit must succeed for in-order rebuild");
        assert_eq!(out.digest_infos.len(), m_entries + k_entries);
        assert_eq!(acc.in_flight_count(), 0);
        assert_eq!(
            acc.total_accumulated_entries(),
            0,
            "after terminal commit total_accumulated must drain to zero — \
             any non-zero value indicates the bookkeeping drift survived \
             the terminal commit subtraction"
        );
    }

    #[test]
    fn duplicate_sequence_drops_accumulator() {
        let acc = BlobsAvailableAccumulator::new();
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(2)]));
        // After dup, the broadcast was discarded.
        assert_eq!(acc.in_flight_count(), 0);
    }

    /// Sibling-bug guard for the Err-branch subtraction in
    /// `merge_chunk` (the `inner.total_accumulated =
    /// inner.total_accumulated.saturating_sub(acc_accumulated_entries)`
    /// in the `Err(reason) =>` arm). The Err branch subtracts
    /// `acc_accumulated_entries` from `inner.total_accumulated`. The
    /// subtraction is safe today because `BroadcastAccumulator::merge`
    /// returns Err only at validation gates that all precede the
    /// `accumulated_entries = self.accumulated_entries.saturating_add(
    /// entries_in_chunk)` mutation — see the doc comment at the
    /// subtraction site for the per-case proof.
    ///
    /// This test exercises the multi-broadcast Err-branch composition
    /// where multiple in-flight broadcasts coexist and one of them
    /// triggers an Err: the OTHER broadcast's contributions to
    /// `total_accumulated` MUST survive intact (no double-subtract /
    /// no over-subtract that bleeds into other broadcasts).
    ///
    /// Mutation step: in the Err branch, change the saturating_sub to
    /// subtract a wrong value (e.g. `acc_accumulated_entries * 2` or
    /// `inner.total_accumulated`); the second-broadcast assertion below
    /// must red-fail with the bespoke "Err-branch over-subtracted"
    /// message.
    #[test]
    fn err_branch_does_not_double_subtract() {
        let acc = BlobsAvailableAccumulator::new();

        // Open broadcast 1 with 50 entries (success path).
        let payload_1: Vec<BlobDigestInfo> = (0..50u64).map(bdi).collect();
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, payload_1)).is_none(),
            "non-terminal first chunk on broadcast 1 must not commit"
        );
        assert_eq!(acc.total_accumulated_entries(), 50);

        // Open broadcast 2 with 30 entries (success path).
        let payload_2: Vec<BlobDigestInfo> = (0..30u64).map(bdi).collect();
        assert!(
            acc.merge_chunk(chunk(2, 0, false, 99, payload_2)).is_none(),
            "non-terminal first chunk on broadcast 2 must not commit"
        );
        assert_eq!(acc.total_accumulated_entries(), 80);
        assert_eq!(acc.in_flight_count(), 2);

        // Drive broadcast 2 into the Err branch via duplicate-sequence
        // (sequence 0 already seen). merge() rejects at the
        // `duplicate sequence within one broadcast` Err — BEFORE the
        // `accumulated_entries = ... saturating_add(entries_in_chunk)`
        // mutation. The Err handler then subtracts
        // `acc_accumulated_entries` (== 30 — the prior successful merge
        // sum) from total_accumulated and removes broadcast 2 from the
        // map. Broadcast 1's 50 entries MUST remain intact.
        let payload_2_dup: Vec<BlobDigestInfo> = (0..7u64).map(bdi).collect();
        let result = acc.merge_chunk(chunk(2, 0, false, 99, payload_2_dup));
        assert!(result.is_none(), "duplicate-sequence chunk must not commit");

        // Broadcast 2 was discarded; broadcast 1 survives.
        assert_eq!(acc.in_flight_count(), 1);
        // Critical assertion: only broadcast 2's 30 entries were
        // subtracted; broadcast 1's 50 entries remain. If the Err branch
        // double-subtracted (e.g. removed acc.body's pre-merge state
        // PLUS the failing chunk's payload) total would be 50 - 7 = 43
        // or some other wrong value.
        assert_eq!(
            acc.total_accumulated_entries(),
            50,
            "Err-branch over-subtracted: broadcast 2 Err removed broadcast \
             1's contributions from total_accumulated; expected 50 \
             (broadcast 1 untouched), got something else"
        );
        // The validation_other counter incremented exactly once.
        let snap = acc.drop_counts.snapshot();
        assert_eq!(
            snap["dropped_validation_other"], 1,
            "exactly one validation_other drop must fire on duplicate sequence"
        );
    }

    #[test]
    fn empty_terminal_chunk_commits_empty_notification() {
        let acc = BlobsAvailableAccumulator::new();
        let out = acc
            .merge_chunk(chunk(1, 0, true, 99, Vec::new()))
            .expect("empty terminal commits");
        assert!(out.digest_infos.is_empty());
    }

    /// Smoking-gun test for invariant-prover BLOCK item 5 (Fix #5).
    /// A terminal chunk arriving with a gap in the sequence set MUST
    /// be rejected by the sequence-completeness gate. Pre-fix, the
    /// shipped accumulator committed the partial; the test asserted
    /// `is_some()` and enshrined the bug as intended behavior.
    #[test]
    fn out_of_order_sequence_with_gap_rejected() {
        let acc = BlobsAvailableAccumulator::new();
        // Send chunk 0 (carries header scalars), then chunk 2 as
        // terminal — chunk 1 was never delivered.
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]))
                .is_none()
        );
        let result = acc.merge_chunk(chunk(1, 2, true, 99, vec![bdi(3)]));
        assert!(
            result.is_none(),
            "sequence-completeness gate must reject incomplete commit; \
             saw is_some() instead — Fix #1 (invariant-prover BLOCK) regression"
        );
        // The accumulator MUST also be dropped on rejection.
        assert_eq!(acc.in_flight_count(), 0);
        // And the per-reason metric must have fired exactly once.
        let snap = acc.drop_counts.snapshot();
        assert_eq!(
            snap["dropped_incomplete_sequence"], 1,
            "incomplete-sequence drop counter must increment exactly once"
        );
    }

    /// In-order arrival (sequence 0, 1, 2 with is_last on 2) is the
    /// production-realistic gRPC FIFO case. The completeness gate
    /// passes; the terminal commits.
    #[test]
    fn three_chunks_in_order_commits_on_terminal() {
        let acc = BlobsAvailableAccumulator::new();
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]))
                .is_none()
        );
        assert!(
            acc.merge_chunk(chunk(1, 1, false, 99, vec![bdi(2)]))
                .is_none()
        );
        let out = acc
            .merge_chunk(chunk(1, 2, true, 99, vec![bdi(3)]))
            .expect("in-order three chunks with terminal-last must commit");
        assert_eq!(out.digest_infos.len(), 3);
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
        let snap = acc.drop_counts.snapshot();
        assert_eq!(snap["dropped_per_conn_broadcasts_cap"], 1);
    }

    #[test]
    fn drop_all_inflight_clears_partial_state() {
        let acc = BlobsAvailableAccumulator::new();
        acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(2, 0, false, 99, vec![bdi(2)]));
        assert_eq!(acc.in_flight_count(), 2);
        acc.drop_all_inflight();
        assert_eq!(acc.in_flight_count(), 0);
        assert_eq!(acc.total_accumulated_entries(), 0);
    }

    #[test]
    fn all_5_unbounded_fields_reassemble() {
        let acc = BlobsAvailableAccumulator::new();
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.cached_directory_digests = vec![d(10)];
        c0.pinned_mirror_entries = vec![mpe(20, "cas")];
        c0.pinned_ac_mirror_entries = vec![mpe(30, "ac")];
        c0.evicted_digests = vec![d(40)];
        c0.evicted_blob_infos = vec![bdi(40)];

        let mut c1 = chunk(1, 1, true, 99, vec![bdi(2)]);
        c1.cached_directory_digests = vec![d(11)];
        c1.pinned_mirror_entries = vec![mpe(21, "cas")];
        c1.pinned_ac_mirror_entries = vec![mpe(31, "ac")];
        c1.evicted_digests = vec![d(41)];
        c1.evicted_blob_infos = vec![bdi(41)];

        assert!(acc.merge_chunk(c0).is_none());
        let out = acc.merge_chunk(c1).expect("terminal");
        assert_eq!(out.digest_infos.len(), 2);
        assert_eq!(out.cached_directory_digests.len(), 2);
        assert_eq!(out.pinned_mirror_entries.len(), 2);
        assert_eq!(out.pinned_ac_mirror_entries.len(), 2);
        assert_eq!(out.evicted_digests.len(), 2);
        // (#locality-map-drift) the ts-carrying eviction list must ALSO merge
        // across chunks (the chunked path is production).
        assert_eq!(out.evicted_blob_infos.len(), 2);
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
        let snap = acc.drop_counts.snapshot();
        assert_eq!(snap["dropped_token_zero"], 1);
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

    /// Falsification test for the per-conn entries cap (Fix #11 +
    /// Fix #2). Drives the cap by emitting one large broadcast with
    /// many chunks; the cap must trigger eviction at threshold.
    /// Without the pre-merge cap, 2× the cap of synthetic data would
    /// transiently swell peak accumulator memory.
    #[test]
    fn entries_cap_drops_partial_at_2x_threshold() {
        let acc = BlobsAvailableAccumulator::new();
        // Emit chunks until we exceed MAX_ACCUMULATED_ENTRIES_PER_CONN.
        // Each chunk carries MAX_ENTRIES_PER_CHUNK = 8192 entries.
        // 1M / 8192 = ~123 chunks before the cap fires.
        let chunks_to_send = (2 * MAX_ACCUMULATED_ENTRIES_PER_CONN) / MAX_ENTRIES_PER_CHUNK;
        let mut accepted = 0;
        for seq in 0..chunks_to_send as u32 {
            let payload: Vec<BlobDigestInfo> = (0..MAX_ENTRIES_PER_CHUNK as u64)
                .map(|i| bdi(u64::from(seq) * 1_000_000 + i))
                .collect();
            let mut c = chunk(1, seq, false, 99, payload);
            // Header scalars only on chunk 0 (rest defaults).
            if seq != 0 {
                c.worker_cas_endpoint = String::new();
            }
            let out = acc.merge_chunk(c);
            // None is the expected return for non-terminal chunks AND
            // for the cap-eviction case. We track which fired by
            // counting accumulated entries.
            if out.is_some() {
                panic!("non-terminal chunks must not commit");
            }
            if acc.in_flight_count() == 1 {
                accepted += 1;
            } else {
                // Cap fired; broadcast was dropped.
                break;
            }
        }
        let snap = acc.drop_counts.snapshot();
        assert!(
            snap["dropped_per_conn_entries_cap"] >= 1,
            "MAX_ACCUMULATED_ENTRIES_PER_CONN cap must trigger eviction at threshold; \
             saw {} entries accepted before drop, drop_counts={:?}",
            accepted * MAX_ENTRIES_PER_CHUNK,
            snap
        );
        assert_eq!(
            acc.in_flight_count(),
            0,
            "accumulator must be dropped on cap eviction"
        );
        assert_eq!(
            acc.total_accumulated_entries(),
            0,
            "total_accumulated counter must zero after cap eviction"
        );
        // We must have accepted at least roughly 90% of the cap before
        // the drop fires (1M / 8192 ~= 122 chunks; allow some slack).
        let expected_min = (MAX_ACCUMULATED_ENTRIES_PER_CONN * 9 / 10) / MAX_ENTRIES_PER_CHUNK;
        assert!(
            accepted >= expected_min,
            "cap should not fire long before threshold; accepted={} expected_min={}",
            accepted,
            expected_min
        );
    }

    /// Per-chunk entries cap (Fix #11 / security M1). A single chunk
    /// with > MAX_ENTRIES_PER_CHUNK entries must be rejected before
    /// merge to bound transient peak memory.
    #[test]
    fn per_chunk_entries_cap_rejects_oversized_chunk() {
        let acc = BlobsAvailableAccumulator::new();
        let payload: Vec<BlobDigestInfo> = (0..(MAX_ENTRIES_PER_CHUNK as u64 + 1))
            .map(bdi)
            .collect();
        let result = acc.merge_chunk(chunk(1, 0, false, 99, payload));
        assert!(result.is_none(), "oversized chunk must be rejected");
        assert_eq!(acc.in_flight_count(), 0);
        let snap = acc.drop_counts.snapshot();
        assert_eq!(snap["dropped_per_chunk_entries_cap"], 1);
    }

    /// Fix #3: header scalars must come from `sequence == 0` only.
    /// If seq=0 never arrives, the terminal commit is rejected
    /// because header_scalars is None.
    #[test]
    fn header_scalars_only_from_chunk_zero() {
        let acc = BlobsAvailableAccumulator::new();
        // Chunk 0 carries scalars; chunk 1 (terminal) leaves them at
        // proto3 defaults per the wire contract.
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.cpu_load_pct = 42;
        c0.mirror_used_bytes = 12345;
        c0.is_full_subtree_snapshot = true;
        // (FL-681) Saturation rides chunk 0; the terminal chunk leaves it at
        // the proto3 default `false` and MUST NOT clobber the carried value.
        c0.indefinite_pin_saturated = true;

        let c1 = chunk(1, 1, true, 99, vec![bdi(2)]);
        // c1 has worker_cas_endpoint = "" and cpu_load_pct = 0 by helper, and
        // indefinite_pin_saturated = false (proto3 default for the terminal).

        assert!(acc.merge_chunk(c0).is_none());
        let out = acc.merge_chunk(c1).expect("terminal commits");
        assert_eq!(out.cpu_load_pct, 42);
        assert_eq!(out.mirror_used_bytes, 12345);
        assert!(out.is_full_subtree_snapshot);
        assert_eq!(out.worker_cas_endpoint, "grpc://w1:50081");
        assert!(
            out.indefinite_pin_saturated,
            "FL-681: chunk-0 indefinite_pin_saturated=true was lost in chunked \
             reassembly (the terminal chunk's default false clobbered it)"
        );
    }

    /// Memory-pressure scalars + the `memory_pressured` verdict ride chunk 0
    /// and the accumulator MUST carry them forward into the reassembled
    /// notification — the terminal chunk leaves them at the proto3 default,
    /// so without the carry-forward copy the values are silently lost in
    /// chunked reassembly (exactly the half-applied-header bug class the
    /// FL-681 `indefinite_pin_saturated` carry guards against).
    ///
    /// Mutation step (CLAUDE.md TDD #5): comment out the
    /// `body.swap_used_bytes = headers.swap_used_bytes;` /
    /// `body.memory_pressure_level = headers.memory_pressure_level;` /
    /// `body.memory_pressured = headers.memory_pressured;` lines in the
    /// terminal-commit arm of `merge_chunk`. This test red-fails with the
    /// bespoke "lost in chunked reassembly" message below.
    #[test]
    fn swap_pressure_scalars_carried_forward_from_chunk_zero() {
        let acc = BlobsAvailableAccumulator::new();
        // Chunk 0 carries the memory scalars; chunk 1 (terminal) leaves
        // them at proto3 default per the wire contract.
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.swap_used_bytes = 9_876_543_210;
        c0.memory_pressure_level = 4242;
        c0.memory_pressured = true;

        // Terminal chunk: memory scalars default (helper). If the
        // accumulator failed to carry chunk-0's values, the terminal's
        // default would win and these assertions would see 0/false.
        let c1 = chunk(1, 1, true, 99, vec![bdi(2)]);

        assert!(acc.merge_chunk(c0).is_none());
        let out = acc.merge_chunk(c1).expect("terminal commits");
        assert_eq!(
            out.swap_used_bytes, 9_876_543_210,
            "chunk-0 swap_used_bytes was lost in chunked reassembly (the \
             terminal chunk's default 0 clobbered the carried value — \
             accumulator carry-forward missing)"
        );
        assert_eq!(
            out.memory_pressure_level, 4242,
            "chunk-0 memory_pressure_level was lost in chunked reassembly \
             (the terminal chunk's default 0 clobbered the carried value — \
             accumulator carry-forward missing)"
        );
        assert!(
            out.memory_pressured,
            "chunk-0 memory_pressured verdict was lost in chunked reassembly \
             (the terminal chunk's default false clobbered the carried value \
             — accumulator carry-forward missing)"
        );
    }

    /// (F4) `available_disk_bytes` + the `disk_pressured` verdict ride chunk 0
    /// and the accumulator MUST carry them forward into the reassembled
    /// notification (same half-applied-header bug class as the swap fields).
    ///
    /// Mutation step (CLAUDE.md TDD #5): comment out the
    /// `body.available_disk_bytes = headers.available_disk_bytes;` /
    /// `body.disk_pressured = headers.disk_pressured;` lines in the
    /// terminal-commit arm of `merge_chunk`. This test red-fails with the
    /// bespoke "lost in chunked reassembly" message below.
    #[test]
    fn disk_pressure_scalars_carried_forward_from_chunk_zero() {
        let acc = BlobsAvailableAccumulator::new();
        let mut c0 = chunk(1, 0, false, 99, vec![bdi(1)]);
        c0.available_disk_bytes = 123_456_789_012;
        c0.disk_pressured = true;

        let c1 = chunk(1, 1, true, 99, vec![bdi(2)]);

        assert!(acc.merge_chunk(c0).is_none());
        let out = acc.merge_chunk(c1).expect("terminal commits");
        assert_eq!(
            out.available_disk_bytes, 123_456_789_012,
            "chunk-0 available_disk_bytes was lost in chunked reassembly (the \
             terminal chunk's default 0 clobbered the carried value — \
             accumulator carry-forward missing)"
        );
        assert!(
            out.disk_pressured,
            "chunk-0 disk_pressured verdict was lost in chunked reassembly (the \
             terminal chunk's default false clobbered the carried value — \
             accumulator carry-forward missing)"
        );
    }

    /// Fix #3: terminal arriving without sequence=0 is rejected.
    /// Header scalars would be missing.
    #[test]
    fn terminal_without_chunk_zero_rejected() {
        let acc = BlobsAvailableAccumulator::new();
        // Send chunk 1 first (non-terminal).
        assert!(
            acc.merge_chunk(chunk(1, 1, false, 99, vec![bdi(2)]))
                .is_none()
        );
        // Now chunk 2 as terminal — sequence=0 never arrived.
        let result = acc.merge_chunk(chunk(1, 2, true, 99, vec![bdi(3)]));
        assert!(
            result.is_none(),
            "terminal without chunk-0 must be rejected (header scalars missing)"
        );
        let snap = acc.drop_counts.snapshot();
        // Note: the missing-chunk-zero check fires AFTER the
        // sequence-completeness check (which also fails here because
        // chunk 0 is missing). Either counter being non-zero
        // indicates the drop fired. The completeness gate is checked
        // first, so dropped_incomplete_sequence is what bumps.
        let total_drops = snap["dropped_incomplete_sequence"]
            + snap["dropped_missing_chunk_zero"];
        assert_eq!(
            total_drops, 1,
            "exactly one rejection must fire on terminal-without-chunk-0; saw {:?}",
            snap
        );
    }

    /// Fix #2: MAX_SEQUENCES = 256 must allow ~1M-entry broadcasts
    /// to round-trip without sequence-cap rejection.
    #[test]
    fn high_sequence_within_max_sequences_accepted() {
        let acc = BlobsAvailableAccumulator::new();
        // Send chunk at sequence 200 (within 256) — non-terminal.
        // First send sequence 0 so header scalars populate.
        assert!(
            acc.merge_chunk(chunk(1, 0, false, 99, vec![bdi(1)]))
                .is_none()
        );
        let result = acc.merge_chunk(chunk(1, 200, false, 99, vec![bdi(2)]));
        assert!(
            result.is_none(),
            "non-terminal chunk at sequence 200 (under 256) must be accepted"
        );
        // The accumulator must still hold the broadcast.
        assert_eq!(acc.in_flight_count(), 1);
    }

    /// Fix #2: sequence == 256 must be rejected (cap is 256).
    #[test]
    fn sequence_at_max_sequences_rejected() {
        let acc = BlobsAvailableAccumulator::new();
        let result = acc.merge_chunk(chunk(1, 256, true, 99, vec![bdi(1)]));
        assert!(result.is_none(), "sequence == 256 must be rejected");
        let snap = acc.drop_counts.snapshot();
        assert_eq!(snap["dropped_sequence_cap"], 1);
    }

    /// Per-reason drop-counter coverage: every documented drop reason
    /// has an exercising test that proves the matching counter
    /// increments. Each scenario starts from a clean accumulator
    /// (drop_all_inflight) so per-conn caps don't bleed across.
    #[test]
    fn drop_counters_cover_all_documented_reasons() {
        let acc = BlobsAvailableAccumulator::new();

        // 1. token_zero
        acc.merge_chunk(chunk(1, 0, true, 0, vec![bdi(1)]));

        // 2. per_chunk_entries_cap
        let oversized: Vec<BlobDigestInfo> =
            (0..(MAX_ENTRIES_PER_CHUNK as u64 + 1)).map(bdi).collect();
        acc.merge_chunk(chunk(2, 0, false, 99, oversized));

        // 3. per_conn_broadcasts_cap
        acc.drop_all_inflight();
        for i in 100..(100 + MAX_INFLIGHT_BROADCASTS_PER_CONN as u64) {
            acc.merge_chunk(chunk(i, 0, false, 99, vec![bdi(i)]));
        }
        acc.merge_chunk(chunk(999, 0, false, 99, vec![bdi(999)]));

        // 4. token_mismatch — needs a clean accumulator (broadcast_id
        // 50 must be allocatable; the broadcast cap from step 3 is
        // still full).
        acc.drop_all_inflight();
        acc.merge_chunk(chunk(50, 0, false, 99, vec![bdi(1)]));
        // Send a different sequence so the token-mismatch rebuild
        // path fires (sequence-0 already populated by first call;
        // duplicate sequence would be caught earlier).
        acc.merge_chunk(chunk(50, 1, false, 7777, vec![bdi(2)]));

        // 5. validation_other (duplicate sequence — same broadcast,
        //    same sequence, same token)
        acc.drop_all_inflight();
        acc.merge_chunk(chunk(60, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(60, 0, false, 99, vec![bdi(2)]));

        // 6. sequence_cap
        acc.drop_all_inflight();
        acc.merge_chunk(chunk(70, 256, true, 99, vec![bdi(1)]));

        // 7. incomplete_sequence
        acc.drop_all_inflight();
        acc.merge_chunk(chunk(80, 0, false, 99, vec![bdi(1)]));
        acc.merge_chunk(chunk(80, 2, true, 99, vec![bdi(2)]));

        // Note: dropped_missing_chunk_zero is structurally unreachable
        // when the sequence-completeness gate is checked FIRST — any
        // terminal arriving without seq=0 also has gaps in
        // seen_sequences, so the completeness gate fires first.
        // The counter exists for defense-in-depth + future code paths.

        let snap = acc.drop_counts.snapshot();
        assert!(snap["dropped_token_zero"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_per_chunk_entries_cap"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_per_conn_broadcasts_cap"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_token_mismatch"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_validation_other"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_sequence_cap"] >= 1, "snap: {:?}", snap);
        assert!(snap["dropped_incomplete_sequence"] >= 1, "snap: {:?}", snap);
    }

    /// Regression test for #354 (testing-czar MAJOR-1 from #99 fix-up-2
    /// cadre review). `BlobsAvailableAccumulator::new_with_drop_counts`
    /// has exactly one production caller — `WorkerApiServer` at
    /// `worker_api_server.rs` (search literal `Arc::clone(&metrics
    /// .chunked_blobs_available_drop_counts)`) — and zero pre-#354 test
    /// callers. The contract: every per-connection accumulator
    /// constructed via `new_with_drop_counts` shares ONE `ChunkDropCounts`
    /// pool, so a future refactor that swaps `Arc::clone` for
    /// `Arc::new(ChunkDropCounts::default())` would silently fork the
    /// counters per connection — server-wide totals would underreport,
    /// dashboards would lie, and no existing test would red-fail.
    ///
    /// Composition exercised: TWO accumulators both built with
    /// `new_with_drop_counts(Arc::clone(&shared))`. Each is driven
    /// through a `dropped_token_mismatch` path (open broadcast with
    /// token=99, send second chunk with token=7777 — the
    /// `existing.worker_instance_token != token` branch of `merge_chunk`
    /// fires `fetch_add(1, Ordering::Relaxed)` on `dropped_token_mismatch`).
    /// Three increments via accumulator A + five via accumulator B must
    /// surface as `8` through the original `shared` Arc handle, AND the
    /// view through A's own `drop_counts`, AND the view through B's own
    /// `drop_counts` — all three handles are aliases for ONE
    /// `ChunkDropCounts` instance.
    ///
    /// Mutation step (CLAUDE.md TDD #5): replace the `Arc::clone` in
    /// `worker_api_server.rs` (or the `drop_counts` field assignment
    /// inside `new_with_drop_counts`) with
    /// `Arc::new(ChunkDropCounts::default())`. This test red-fails with
    /// the bespoke message naming the regression.
    #[test]
    fn new_with_drop_counts_aggregates_across_accumulators() {
        let shared = Arc::new(ChunkDropCounts::default());
        let acc_a = BlobsAvailableAccumulator::new_with_drop_counts(Arc::clone(&shared));
        let acc_b = BlobsAvailableAccumulator::new_with_drop_counts(Arc::clone(&shared));

        // Drive 3 token-mismatch increments through accumulator A.
        // Each pair (open with token=99, then send a chunk with
        // token=7777 reusing the same broadcast_id) bumps
        // `dropped_token_mismatch` exactly once.
        for broadcast_id in [10u64, 11, 12] {
            assert!(
                acc_a
                    .merge_chunk(chunk(broadcast_id, 0, false, 99, vec![bdi(1)]))
                    .is_none()
            );
            assert!(
                acc_a
                    .merge_chunk(chunk(broadcast_id, 0, false, 7777, vec![bdi(2)]))
                    .is_none()
            );
        }

        // Drive 5 token-mismatch increments through accumulator B.
        for broadcast_id in [20u64, 21, 22, 23, 24] {
            assert!(
                acc_b
                    .merge_chunk(chunk(broadcast_id, 0, false, 99, vec![bdi(1)]))
                    .is_none()
            );
            assert!(
                acc_b
                    .merge_chunk(chunk(broadcast_id, 0, false, 7777, vec![bdi(2)]))
                    .is_none()
            );
        }

        let shared_view = shared.dropped_token_mismatch.load(Ordering::Relaxed);
        let a_view = acc_a.drop_counts.dropped_token_mismatch.load(Ordering::Relaxed);
        let b_view = acc_b.drop_counts.dropped_token_mismatch.load(Ordering::Relaxed);

        assert_eq!(
            shared_view, 8,
            "Arc<ChunkDropCounts> shared-aggregation broken — N accumulators \
             with Arc::clone MUST sum into one shared counter pool. If this \
             test fails, someone replaced Arc::clone with Arc::new in \
             worker_api_server.rs (the call to \
             BlobsAvailableAccumulator::new_with_drop_counts) or inside \
             new_with_drop_counts itself; got shared_view={shared_view}, \
             expected 8 (= 3 from acc_a + 5 from acc_b)"
        );
        assert_eq!(
            a_view, shared_view,
            "Arc<ChunkDropCounts> aliasing broken — acc_a.drop_counts and \
             the shared Arc must observe the same counter value; got \
             a_view={a_view}, shared_view={shared_view}"
        );
        assert_eq!(
            b_view, shared_view,
            "Arc<ChunkDropCounts> aliasing broken — acc_b.drop_counts and \
             the shared Arc must observe the same counter value; got \
             b_view={b_view}, shared_view={shared_view}"
        );

        // Pointer identity: the three Arcs must point at the SAME
        // ChunkDropCounts allocation. `Arc::ptr_eq` is the load-bearing
        // assertion; equal counter values could in principle arise from
        // two independent counters that happen to coincide, but
        // pointer-equality forces the test to falsify if anyone forks
        // the Arc.
        assert!(
            Arc::ptr_eq(&shared, &acc_a.drop_counts),
            "shared Arc and acc_a.drop_counts must point at the same \
             ChunkDropCounts allocation — Arc::ptr_eq returned false, \
             meaning new_with_drop_counts forked the Arc"
        );
        assert!(
            Arc::ptr_eq(&shared, &acc_b.drop_counts),
            "shared Arc and acc_b.drop_counts must point at the same \
             ChunkDropCounts allocation — Arc::ptr_eq returned false, \
             meaning new_with_drop_counts forked the Arc"
        );
    }

    /// (#28) End-to-end producer→accumulator seam test for the swap
    /// pressure scalars. The per-half unit tests prove placement
    /// (`swap_fields_ride_chunk_zero_only` in the chunker) and reassembly
    /// from HAND-BUILT chunks (`swap_pressure_scalars_carried_forward_
    /// from_chunk_zero`); neither crosses the producer→accumulator seam
    /// with the REAL chunker output. This closes that gap:
    ///
    ///   producer  — `nativelink_util::blobs_available_chunking::
    ///               chunk_blobs_available` (the real worker-side chunker,
    ///               splits the notification, writes the swap scalars on
    ///               chunk 0 only).
    ///   accumulator — `BlobsAvailableAccumulator::merge_chunk` (the real
    ///               server reassembly, carries chunk-0's header scalars
    ///               forward into the terminal-commit notification).
    ///
    /// The input is sized to force MULTIPLE chunks (`max_per_chunk = 3`
    /// over 10 digests → ≥4 chunks) so the carry-forward is genuinely
    /// exercised: the terminal chunk leaves the swap scalars at proto3
    /// default 0, and only the carry-forward arm can resurrect chunk-0's
    /// non-zero values into the reassembled notification.
    ///
    /// Mutation step (CLAUDE.md TDD #5): comment out the
    /// `body.swap_used_bytes = headers.swap_used_bytes;` /
    /// `body.memory_pressure_level = headers.memory_pressure_level;` /
    /// `body.memory_pressured = headers.memory_pressured;` lines in the
    /// terminal-commit arm of `merge_chunk`: this test red-fails with the
    /// bespoke "did not survive the real chunker→accumulator round-trip"
    /// message below, NOT a generic is_none/0 assert that another bug could
    /// also trip.
    #[test]
    fn swap_pressure_survives_real_chunker_to_accumulator_roundtrip() {
        use nativelink_util::blobs_available_chunking::chunk_blobs_available;

        const SWAP_USED: u64 = 7_654_321_098;
        const LEVEL: u32 = 31337;
        const BROADCAST_ID: u64 = 4242;
        const TOKEN: u64 = 0xCAFEF00D;

        let notification = BlobsAvailableNotification {
            // Non-zero memory pressure on the INPUT notification — the
            // signal that must survive the full chunk→reassemble path.
            swap_used_bytes: SWAP_USED,
            memory_pressure_level: LEVEL,
            memory_pressured: true,
            // 10 digests at 3-per-chunk forces ≥4 chunks, so the memory
            // scalars (chunk-0-only) must be carried forward across a
            // terminal chunk that zeroes them.
            digest_infos: (0..10).map(bdi).collect(),
            ..Default::default()
        };

        let chunks =
            chunk_blobs_available(notification, BROADCAST_ID, TOKEN, String::new(), 3)
                .expect("real chunker must split the 10-digest notification");
        assert!(
            chunks.len() >= 4,
            "test premise: input must force a multi-chunk broadcast so the \
             carry-forward is actually exercised; saw {} chunk(s)",
            chunks.len()
        );
        // Sanity: the producer writes the memory scalars on chunk 0 only —
        // the terminal chunk MUST present default, so a missing
        // carry-forward in the accumulator would surface as 0/false below.
        assert_eq!(chunks[0].swap_used_bytes, SWAP_USED);
        assert_eq!(chunks[0].memory_pressure_level, LEVEL);
        assert!(chunks[0].memory_pressured);
        let terminal = chunks.last().expect("at least one chunk");
        assert!(terminal.is_last, "last chunk must be terminal");
        assert_eq!(
            terminal.swap_used_bytes, 0,
            "wire contract: terminal (non-zero sequence) chunk leaves \
             swap_used_bytes at the proto3 default 0"
        );
        assert_eq!(terminal.memory_pressure_level, 0);
        assert!(!terminal.memory_pressured);

        // Drive the REAL accumulator with the REAL chunker output.
        let acc = BlobsAvailableAccumulator::new();
        let mut reassembled = None;
        for chunk in chunks {
            if let Some(notification) = acc.merge_chunk(chunk) {
                reassembled = Some(notification);
            }
        }
        let reassembled = reassembled.expect(
            "terminal chunk must commit the reassembled notification through \
             the real chunker→accumulator seam",
        );

        assert_eq!(
            reassembled.swap_used_bytes, SWAP_USED,
            "swap_used_bytes did not survive the real chunker→accumulator \
             round-trip — the terminal chunk's default 0 clobbered chunk-0's \
             carried value (accumulator carry-forward missing)"
        );
        assert_eq!(
            reassembled.memory_pressure_level, LEVEL,
            "memory_pressure_level did not survive the real chunker→ \
             accumulator round-trip — the terminal chunk's default 0 clobbered \
             chunk-0's carried value (accumulator carry-forward missing)"
        );
        assert!(
            reassembled.memory_pressured,
            "memory_pressured verdict did not survive the real chunker→ \
             accumulator round-trip — the terminal chunk's default false \
             clobbered chunk-0's carried value (accumulator carry-forward \
             missing)"
        );
    }

    /// (c18ace44 guard) Compile-time litmus: explicitly constructs
    /// `BlobsAvailableNotification` naming EVERY field without
    /// `..Default::default()`. If a new field is added to the proto
    /// without updating this test, it fails to compile with:
    ///   "missing field `<new_field>` in initializer of
    ///    `BlobsAvailableNotification`"
    /// This catches the c18ace44 incident class at build time rather
    /// than at runtime or code-review time. The test body only needs to
    /// compile — the `let _` discards the value.
    ///
    /// HOW TO UPDATE: when the proto gains a new field, add it here.
    /// If a field is removed, remove it here. Do NOT add
    /// `..Default::default()` — that re-opens the gap.
    #[test]
    fn proto_fields_exhaustive_bans_missing() {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            BlobDigestInfo, BlobsAvailableNotification, MirrorPinEntry,
        };
        use nativelink_proto::build::bazel::remote::execution::v2::Digest;

        let _: BlobsAvailableNotification = BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: Vec::<Digest>::new(),
            is_full_snapshot: false,
            // (#locality-map-drift) evicted_digests stays Digest (legacy, no tag
            // reuse); evicted_blob_infos (below) is the ts-carrying companion.
            evicted_digests: Vec::<Digest>::new(),
            digest_infos: Vec::new(),
            cpu_load_pct: 0,
            cached_directory_digests: Vec::<Digest>::new(),
            added_subtree_digests: Vec::<Digest>::new(),
            removed_subtree_digests: Vec::<Digest>::new(),
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: Vec::<Digest>::new(),
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: Vec::<MirrorPinEntry>::new(),
            pinned_ac_mirror_entries: Vec::<MirrorPinEntry>::new(),
            indefinite_pin_saturated: false,
            swap_used_bytes: 0,
            memory_pressure_level: 0,
            memory_pressured: false,
            available_disk_bytes: 0,
            disk_pressured: false,
            evicted_blob_infos: Vec::<BlobDigestInfo>::new(),
        };
        // The test is a compile-time check; no runtime assertions needed.
    }
}
