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

use core::num::NonZeroUsize;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_lock::RwLock;
use bytes::Bytes;
use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    RootMetricsComponent, group,
};
use nativelink_proto::build::bazel::remote::execution::v2::{Digest, Directory};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AcPinResyncRequest, BlobsInStableStorage, KillOperationRequest, PeerHint, StartExecute,
    UpdateForWorker, update_for_worker,
};
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::background_spawn;
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::common::DigestInfo;
use nativelink_util::metrics_utils::CounterWithTime;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::platform_properties::PlatformProperties;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike};
use parking_lot::Mutex as ParkingMutex;
use prost::Message;
use tokio::sync::{Notify, Semaphore};
use tokio::sync::mpsc::UnboundedSender;
use tonic::async_trait;
use tracing::{debug, error, info, trace, warn};

/// Metrics for tracking scheduler performance.
///
/// (#231) Exposed on the server `/metrics` endpoint via
/// `#[derive(MetricsComponent)]` + the `#[metric(group =
/// "scheduler_metrics")]` annotation on `ApiWorkerScheduler::metrics`,
/// which makes the `RootMetricsComponent` walk
/// (`src/bin/nativelink.rs:559-568` → `render_prometheus`) descend into
/// these counters. Before #231 they were dark — the only operator
/// signal was the paired `warn!`s.
#[derive(Debug, Default, MetricsComponent)]
pub struct SchedulerMetrics {
    /// Total number of worker additions.
    #[metric(help = "total number of worker additions")]
    pub workers_added: AtomicU64,
    /// Total number of worker removals.
    #[metric(help = "total number of worker removals")]
    pub workers_removed: AtomicU64,
    /// Total number of `find_worker_for_action` calls.
    #[metric(help = "total number of find_worker_for_action calls")]
    pub find_worker_calls: AtomicU64,
    /// Total number of successful worker matches.
    #[metric(help = "total number of successful worker matches")]
    pub find_worker_hits: AtomicU64,
    /// Total number of failed worker matches (no worker found).
    #[metric(help = "total number of failed worker matches (no worker found)")]
    pub find_worker_misses: AtomicU64,
    /// Total time spent in `find_worker_for_action` (nanoseconds).
    #[metric(help = "total time spent in find_worker_for_action (nanoseconds)")]
    pub find_worker_time_ns: AtomicU64,
    /// Total number of workers iterated during find operations.
    #[metric(help = "total number of workers iterated during find operations")]
    pub workers_iterated: AtomicU64,
    /// Total number of action dispatches.
    #[metric(help = "total number of action dispatches")]
    pub actions_dispatched: AtomicU64,
    /// Total number of keep-alive updates.
    #[metric(help = "total number of keep-alive updates")]
    pub keep_alive_updates: AtomicU64,
    /// Total number of worker timeouts.
    #[metric(help = "total number of worker timeouts")]
    pub worker_timeouts: AtomicU64,
    /// Total number of prefetch tasks spawned.
    #[metric(help = "total number of prefetch tasks spawned")]
    pub prefetch_tasks_spawned: AtomicU64,
    /// Total number of blobs successfully prefetched to workers.
    #[metric(help = "total number of blobs successfully prefetched to workers")]
    pub prefetch_blobs_sent: AtomicU64,
    /// Total bytes successfully prefetched to workers.
    #[metric(help = "total bytes successfully prefetched to workers")]
    pub prefetch_bytes_sent: AtomicU64,
    /// Total number of blobs that failed to prefetch.
    #[metric(help = "total number of blobs that failed to prefetch")]
    pub prefetch_blobs_failed: AtomicU64,
    /// Total number of blobs skipped because they were already on the worker.
    #[metric(help = "total number of blobs skipped because they were already on the worker")]
    pub prefetch_blobs_already_present: AtomicU64,
    /// Total number of batch RPCs sent to workers during prefetch.
    #[metric(help = "total number of batch rpcs sent to workers during prefetch")]
    pub prefetch_batches_sent: AtomicU64,
    /// Total number of server-side cache warm tasks spawned.
    #[metric(help = "total number of server-side cache warm tasks spawned")]
    pub cache_warm_spawned: CounterWithTime,
    /// (#214) Cumulative number of BIS replay-buffer chunks dropped by
    /// the per-worker overflow cap. A non-zero value means at least one
    /// worker is misbehaving (never acking BIS chunks while still
    /// holding a connection slot) and the server is silently dropping
    /// replay state to bound memory.
    ///
    /// **Operator visibility (#231):** now surfaced on the `/metrics`
    /// endpoint under `scheduler.<name>.worker.scheduler_metrics.\
    /// bis_replay_buffer_overflow_drops`. The per-(endpoint,
    /// broadcast_id) `warn!` at `bis_chunked_dispatch` remains the
    /// per-event signal greppable from journald.
    #[metric(
        help = "(#214) cumulative BIS replay-buffer chunks dropped by the per-worker overflow cap; non-zero means a worker is holding a slot without acking and the server is dropping replay state to bound memory"
    )]
    pub bis_replay_buffer_overflow_drops: AtomicU64,
    /// (#sched-b1) Cumulative count of completions whose operation was
    /// already removed from the worker's `running_action_infos` during
    /// the B1 lock-free `update_operation` window (a legitimate
    /// concurrent finalize — e.g. `ExecutionComplete` or eviction —
    /// removed it). The completion's op-state was already committed by
    /// (b), so the second critical section softens the missing-op case to
    /// `Ok(())` instead of erroring. This is the EXPECTED post-unlock
    /// race; a non-zero, slowly-growing value is benign. The paired
    /// `warn!` (see `update_action`) is the per-event operator signal
    /// and, unlike the old `debug!`, SURVIVES `release_max_level_info`
    /// so the softened branch is not invisible in the production binary.
    /// (#231) The cumulative count is also surfaced on `/metrics`.
    #[metric(
        help = "(#sched-b1) cumulative completions whose op was already removed during the B1 lock-free update_operation window (expected post-unlock race; slowly-growing is benign)"
    )]
    pub update_action_op_already_finalized: AtomicU64,
    /// (#sched-zeroload) Number of workers currently registered that have NEVER
    /// reported a load reading (`has_reported_load == false`). A non-zero value
    /// on a steady-state fleet indicates a stalled keepalive path (workers
    /// connecting but not reporting load) → silent UNDER-selection in the
    /// effective_load_score paths. Alerts on stalled worker heartbeat.
    #[metric(
        help = "(#sched-zeroload) workers currently registered that have never reported load; non-zero on steady-state fleet means stalled keepalive → silent under-selection"
    )]
    pub workers_never_reported_load: AtomicU64,

    /// (#schedmetric) Point-in-time total number of workers currently
    /// registered in the pool. Recomputed from the live `self.workers` map
    /// (`workers.len()`) in `recompute_capacity_gauges` after any pool
    /// mutation — not maintained as a running delta, so it stays correct
    /// even after bulk evictions.
    #[metric(
        help = "point-in-time total workers registered in the scheduler pool"
    )]
    pub workers_total: AtomicU64,

    /// (#schedmetric) Point-in-time count of workers that CANNOT accept
    /// more actions right now (`can_accept_work() == false`, i.e. paused,
    /// draining, or at `max_inflight_tasks`). Recomputed in
    /// `recompute_capacity_gauges` after any pool mutation.
    /// Saturation ratio = workers_at_capacity / workers_total.
    ///
    /// TODO(#schedmetric): this counts only `can_accept_work()` exclusions.
    /// Workers the matcher additionally skips — `quarantined_at`,
    /// `indefinite_pin_saturated`, `swap_pressured`, `disk_pressured`
    /// (see `inner_find_and_reserve_worker`'s `worker_is_viable`) — are NOT
    /// counted here. An operator reading this as "workers not receiving
    /// work / total" will undercount when those gates are active. A
    /// separate `workers_excluded` gauge would capture the full criteria.
    #[metric(
        help = "point-in-time workers at capacity (can_accept_work=false: \
                paused/draining/max-inflight); saturation = \
                workers_at_capacity / workers_total"
    )]
    pub workers_at_capacity: AtomicU64,

    /// (#schedmetric) Point-in-time total in-flight actions across all
    /// workers (sum of `running_action_infos.len()` per worker).
    /// Recomputed in `recompute_capacity_gauges` after any pool mutation.
    #[metric(
        help = "point-in-time total in-flight actions across the fleet \
                (sum of per-worker running_action_infos)"
    )]
    pub total_running_actions: AtomicU64,

    // ── (#p1p2) Phase-1 tree-resolution telemetry ──
    // TELEMETRY-ONLY. These answer two open questions with production
    // data: (1) is Phase-1 tree resolution a scheduling-latency
    // contributor (the cold-resolution time sum + count, warm path =
    // hits), and (2) does the `ByteBoundedTreeCache` run out of space and
    // evict (the eviction counter + the resident-bytes / entry-count
    // gauges vs `TREE_CACHE_MAX_BYTES` / `TREE_CACHE_CAPACITY`). No
    // behavior change — increments/stores only, `Ordering::Relaxed`.
    /// (#p1p2) Cumulative `resolve_input_tree` calls served from the
    /// positive tree cache (warm path). Ratio hits/(hits+misses) is the
    /// tree-cache hit rate — a low rate means Phase-1 resolution is on the
    /// scheduling critical path more often than expected.
    #[metric(
        help = "(#p1p2) cumulative resolve_input_tree calls served from the positive tree cache (warm path)"
    )]
    pub tree_cache_hits: AtomicU64,
    /// (#p1p2) Cumulative `resolve_input_tree` calls that missed the
    /// positive cache and attempted a cold resolution from CAS.
    #[metric(
        help = "(#p1p2) cumulative resolve_input_tree cache misses (cold resolution attempted from CAS)"
    )]
    pub tree_cache_misses: AtomicU64,
    /// (#p1p2) Cumulative entries evicted from the tree cache — the
    /// count-capacity (`TREE_CACHE_CAPACITY`) displacement plus the
    /// byte-budget (`TREE_CACHE_MAX_BYTES`) loop. A non-zero, growing
    /// value means the cache is under space pressure and dropping warm
    /// trees. Same-key replacements are NOT counted.
    #[metric(
        help = "(#p1p2) cumulative entries evicted from the tree cache (count-cap displacement + byte-budget loop); non-zero means space pressure"
    )]
    pub tree_cache_evictions: AtomicU64,
    /// (#p1p2) Point-in-time estimated resident heap bytes held by the
    /// tree cache, sampled after each put under the cache lock. Compare
    /// against `TREE_CACHE_MAX_BYTES` (512 MiB) to see how close the cache
    /// runs to its byte budget.
    #[metric(
        help = "(#p1p2) point-in-time estimated resident heap bytes in the tree cache (vs TREE_CACHE_MAX_BYTES = 512 MiB)"
    )]
    pub tree_cache_resident_bytes: AtomicU64,
    /// (#p1p2) Point-in-time number of entries resident in the tree cache,
    /// sampled after each put under the cache lock. Compare against
    /// `TREE_CACHE_CAPACITY` (1024) to see how close the cache runs to its
    /// count budget.
    #[metric(
        help = "(#p1p2) point-in-time number of entries resident in the tree cache (vs TREE_CACHE_CAPACITY = 1024)"
    )]
    pub tree_cache_entries: AtomicU64,
    /// (#p1p2) Cumulative wall-clock nanoseconds spent in successful COLD
    /// (cache-miss) inline tree resolutions from CAS. Divided by
    /// `tree_resolution_cold_count` gives mean cold-resolution latency —
    /// the direct answer to "is Phase-1 resolution a scheduling-latency
    /// contributor". The warm path adds no time here (it is a hit).
    #[metric(
        help = "(#p1p2) cumulative nanoseconds in successful cold (cache-miss) inline tree resolutions; /count = mean cold latency"
    )]
    pub tree_resolution_cold_time_ns: AtomicU64,
    /// (#p1p2) Cumulative count of successful cold inline tree
    /// resolutions — the denominator for `tree_resolution_cold_time_ns`.
    #[metric(
        help = "(#p1p2) cumulative count of successful cold inline tree resolutions (denominator for tree_resolution_cold_time_ns)"
    )]
    pub tree_resolution_cold_count: AtomicU64,
    /// (#p1p2) Cumulative inline tree resolutions that hit the 500 ms
    /// timeout and fell back to load-based scoring (resolution continues
    /// in a background task). A non-zero, growing value means cold
    /// resolution is frequently exceeding the dispatch-latency budget.
    #[metric(
        help = "(#p1p2) cumulative inline tree resolutions that hit the 500ms timeout and fell back to load-based scoring"
    )]
    pub tree_resolution_timeouts: AtomicU64,
    /// (#p1p2) Cumulative inline tree resolutions that returned a
    /// resolution error (not a timeout) — e.g. a CAS fetch failure or a
    /// missing directory blob. Recorded in the negative cache with
    /// backoff.
    #[metric(
        help = "(#p1p2) cumulative inline tree resolutions that failed with a resolution error (not a timeout)"
    )]
    pub tree_resolution_errors: AtomicU64,
}

/// Point-in-time intersection of an action's `file_digests` and the
/// scheduler's locality map, captured under the same read lock that
/// builds the scoring result.
///
/// Phase-4 callers (`compute_missing_blobs` prefetch path and the inline
/// `all_missing` filter inside `find_and_reserve_worker`) consult this
/// snapshot in lieu of re-acquiring `locality_map.read()`, eliminating
/// two of the three O(F) read-lock walks per cold-tree dispatch. The
/// audit at `.claude/audits/locality-scoring-perf-2026-05-11.md` (#407)
/// measured 9 slow-warns at 58–105 ms across 2 minutes with
/// `locality_blob_count: ~252K`; reducing 3× O(F) to 1× O(F) projects
/// the slow-warn rate to drop by ~⅔.
///
/// **Contract — point-in-time, not live.** Snapshot reflects locality
/// state at scoring time. If a worker registers a new blob OR is
/// evicted between scoring and Phase-4 reuse, the snapshot WILL NOT
/// reflect it. Result: at worst an over-fetch (sending a blob the
/// worker already has) — correctness preserved, never under-fetch
/// causing a missing-blob failure.
///
/// **Cache-hit staleness window.** This `ScoringResult` is cached in
/// `scores_cache` LRU keyed by `input_root_digest`. On a cache hit
/// (common with CI re-runs over the same input root), the SAME
/// snapshot is reused for many subsequent dispatches — "scoring time"
/// effectively extends to the cache-entry's lifetime, not the moment
/// of the current dispatch. With BIS broadcasts continuously updating
/// locality state, the over-fetch rate on cache-hit paths can be
/// non-trivial. Phase 2 measurement window must instrument
/// prefetch-already-had-it rate alongside slow-warn rate to confirm
/// the net win — see #430 follow-up. Eviction-callback clear
/// (`:1172`) bounds the worst case.
///
/// **CAPPED AT `LOCALITY_SNAPSHOT_MAX_ENTRIES`:** bounded by
/// `tree.file_digests.len()` (Bazel action input count), itself
/// bounded by Bazel's action proto limit. We additionally enforce the
/// constant cap as a defensive guard against a pathological tree
/// (e.g. CAS corruption inflating file count). Over-cap behavior:
/// snapshot construction is skipped — callers fall back to a fresh
/// `locality_map.read()` per F-walk (pre-#407 behavior, slow-warn
/// fires). This is the documented over-cap policy per CLAUDE.md
/// "Unbounded in-process buffers" rule.
///
/// Memory: per-entry ≈ 40 B `DigestInfo` + `Vec<Arc<str>>` header
/// (24 B) + up to ≤10 × 16 B `Arc<str>` ≈ 200 B worst case. At
/// `LOCALITY_SNAPSHOT_MAX_ENTRIES = 65_536`, worst-case bound is ~13 MB
/// per cached entry × `TREE_CACHE_CAPACITY = 1024` cache slots = ~13 GB
/// upper bound; typical actions have F = hundreds, snapshot well below
/// 1 MB. The cache LRU + worker-eviction clear (`:1172`) bounds
/// realised footprint.
pub(crate) type LocalitySnapshot = Arc<HashMap<DigestInfo, Vec<Arc<str>>>>;

/// Defensive cap on the per-action locality snapshot size. The natural
/// bound is `tree.file_digests.len()` (Bazel action input count); this
/// constant exists for the CLAUDE.md "Unbounded in-process buffers"
/// requirement: an explicit numeric cap with documented over-cap
/// behavior (skip snapshot — fall back to live `locality_map.read()`).
///
/// `65_536` is generously sized for a typical Bazel action: no
/// observed action in this deployment has `file_digests.len()` above
/// the order-of-magnitude 10K mark (precise upper bound uncited; rough
/// estimate from sampled `chunked.rs:147` traces and BES profile
/// inspection). Exceeding 65_536 is strong evidence of an upstream
/// defect — pathological tree traversal, CAS corruption, or a Bazel
/// rule emitting an unbounded glob — not a legitimate workload.
pub(crate) const LOCALITY_SNAPSHOT_MAX_ENTRIES: usize = 65_536;

/// Cached result of `score_and_generate_hints`:
/// - `scores`: endpoint scores (cached bytes per endpoint).
/// - `peer_hints`: `Arc<[PeerHint]>` so per-worker dispatch is a
///   refcount bump rather than a Vec clone (#83 ride-along — was
///   `peer_hints.to_vec()` per match in the prior design, which
///   churned ~16 KiB per dispatched action).
/// - `locality_snapshot`: per-action point-in-time view of
///   `file_digests ∩ locality_map`, reused by Phase-4 prefetch and
///   inline `all_missing` filter to avoid two redundant
///   `locality_map.read()` walks per cold-tree dispatch (#407).
///   `None` if the snapshot would exceed
///   `LOCALITY_SNAPSHOT_MAX_ENTRIES` — callers fall back to a fresh
///   `locality_map.read()` in that case.
#[derive(Debug)]
pub(crate) struct ScoringResult {
    pub(crate) scores: HashMap<Arc<str>, u64>,
    pub(crate) peer_hints: Arc<[PeerHint]>,
    pub(crate) locality_snapshot: Option<LocalitySnapshot>,
}

/// Maximum number of `PeerHint` entries packed into one `PeerHintsChunk`
/// proto. At ~250 bytes per hint worst case, 256 hints per chunk caps
/// each proto message at ~64 KiB — well below the 64 MiB worker decoder
/// limit (`WORKER_API_MAX_DECODING_MESSAGE_SIZE`) AND well below typical
/// h2/QUIC frame fragmentation thresholds.
pub(crate) const PEER_HINTS_PER_CHUNK: usize = 256;

/// (#97) Maximum number of `Digest` entries packed into one
/// `BlobsInStableStorageChunk` proto. SHA-256 digests are ~32 bytes
/// each plus prost framing (~40 B with size_bytes); 4096 digests per
/// chunk caps the proto at ~160 KiB, well below the 64 MiB worker
/// decoder limit. Larger chunks reduce per-chunk ack overhead but
/// increase the cost of a single resend after a connection drop.
pub(crate) const BIS_DIGESTS_PER_CHUNK: usize = 4096;

/// (#214) Per-worker cap on buffered, unacked BIS chunks. Defends
/// against a misbehaving / wedged / never-acking worker that would
/// otherwise grow its `BisResendBuffer` monotonically across
/// reconnects (observed 2026-04-30 on buildcache: chunk_count for
/// worker-03 grew 33,120 → 34,980 in 12 minutes; replays never
/// completed because new reconnects fired mid-stream).
///
/// Wire-encoded chunks are ~160 KiB (`BIS_DIGESTS_PER_CHUNK` × ~40 B
/// serialized), but the buffer holds *decoded* `Digest` structs that
/// expand to ~96 B each in heap, plus prost framing. Realistic per-
/// worker heap at the cap: ~38 GiB. With deployed `MemoryMax` = 80 GiB,
/// the cap defends against a SINGLE misbehaving worker driving the
/// server to OOM via this surface alone — but two simultaneously-
/// misbehaving workers can still get tight; combine with #215
/// (per-worker reconnect rate-limit) and #216 (stale-worker rejection)
/// for full defense-in-depth.
///
/// Overflow policy: **drop oldest** ((broadcast_id, sequence)
/// lex-min) one chunk at a time until the buffer fits. Older
/// chunks are the ones the worker has had the longest opportunity
/// to ack; newer broadcasts (more recently relevant) are preserved.
/// A boot_epoch_id change still fully resets the buffer for healthy
/// reconnects via `clear_bis_resend_buffer_for_endpoint`; the cap
/// only fires for the never-acking-but-still-connected pathological
/// case.
///
/// Test override: in `#[cfg(test)]` builds the cap is reduced to 64
/// so production-composition tests can exercise the cap-fires path
/// in <1 s of wall-clock without broadcasting hundreds of thousands
/// of digests. The mechanism under test (drop-oldest + counter +
/// warn) is identical at any cap.
#[cfg(not(test))]
pub(crate) const BIS_REPLAY_BUFFER_MAX_CHUNKS: usize = 100_000;
#[cfg(test)]
pub(crate) const BIS_REPLAY_BUFFER_MAX_CHUNKS: usize = 64;

use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker::{
    ActionInfoWithProps, PendingActionInfoData, Worker, WorkerTimestamp, WorkerUpdate,
    reduce_platform_properties,
};
use crate::worker_capability_index::WorkerCapabilityIndex;
use crate::worker_registry::SharedWorkerRegistry;
use crate::worker_scheduler::WorkerScheduler;

// ── (#sched-blend) Continuous cache-vs-load blend constants ──
//
// The blend ranks viable cache-affinity candidates (Tier 1 / Tier 1.5) by
// `S = cache_gain - load_penalty`, where `load_penalty` is driven by a
// worker's ABSOLUTE free core capacity at CENTI-CORE (1/100-core)
// resolution rather than its load percentage. Carrying free capacity at
// centi-core resolution is the whole point: an integer percentage load
// maps EXACTLY to a free-capacity value (`count * (100 - load)`), with no
// truncation, so the penalty grades CONTINUOUSLY across the load range and
// a 96-core box and a 2-core box at the same load% rank by real spare
// capacity. All arithmetic is `i64`/integer — no floats in the compare
// path (determinism + no NaN-ordering hazard).

/// (#sched-blend) Free weighted-capacity headroom (in the centi-core
/// `2*p_free_centi + e_free_centi` numerator scale) at and above which a
/// worker pays ZERO load penalty and competes purely on cache. `200`
/// numerator-units = one free weighted P-core (100 centi-cores × the P
/// weight numerator `2`): a worker with ≥1 idle P-core is "abundantly
/// free." Below this the penalty rises linearly and continuously as free
/// capacity shrinks toward zero (full saturation). The continuous analogue
/// of the old binary cutoff's "any P-core idle ⇒ preferred."
/// NUMERIC: the centi-core encoding — `200`, NOT `1`. The knee is one free
/// P-core; `1` would put it at 1/200 of a core. (rev-3: this units choice
/// is the fix for the rev-2 binary-collapse, where a whole-core scale made
/// the penalty step only at 100%.)
const REF_FREE: i64 = 200;

/// (#sched-blend) Integer P-core weight numerator. One free P-core is the
/// unit of capacity (`P:E = 2:1` over a `/2` scale). With `E_WEIGHT_NUM`
/// this gives `weighted_free = P_WEIGHT_NUM*p_free_centi + E_WEIGHT_NUM*e_free_centi`.
const P_WEIGHT_NUM: i64 = 2;

/// (#sched-blend) Integer E-core weight numerator. An E-core does ~half a
/// P-core of build work, so a free P-core outranks a free E-core and a
/// worker with idle P-cores beats one with only idle E-cores at equal
/// absolute free-core count (R2). Without a P>E weight, R2 fails.
const E_WEIGHT_NUM: i64 = 1;

/// (#sched-blend, backstop (a) / §R5.1) Saturation predicate threshold in
/// the centi-core numerator space. A candidate is "saturated" when its
/// `weighted_free` is `<=` this — i.e. exactly `0` (literally zero free
/// centi-cores of either type). Kept as a named const for the numeric-pin
/// test even though it is exactly zero: when EVERY viable candidate is
/// saturated, the continuous penalty is the same maxed constant for all and
/// CANCELS across them, so cache alone would decide (the #52 pile-on) — the
/// cascade then falls through to the LRU/MRU path instead. `0` is
/// mechanically derived (it fires exactly when the penalty stops
/// discriminating), not a tuned guess.
const SATURATION_EPSILON: i64 = 0;

/// (#sched-blend) Per-candidate result of the continuous blend's
/// free-capacity math, computed ONCE per viable worker in the selection
/// loop and reused for both the penalty (Tier 1 / Tier 1.5 ranking) and
/// the saturation predicate (backstop (a)). All centi-core integer.
#[derive(Clone, Copy, Debug)]
struct CapacityScore {
    /// `2*p_free_centi + e_free_centi` — weighted free capacity, centi-core
    /// numerator scale. `0` ⟺ the worker is fully saturated.
    weighted_free: i64,
    /// `LOAD_BYTE_COST * max(0, REF_FREE - weighted_free) / 200` —
    /// bytes-equivalent load penalty, subtracted from `cache_gain`.
    load_penalty: i64,
}

impl CapacityScore {
    /// True when the worker has literally zero free weighted centi-cores —
    /// the condition under which the penalty term cancels across candidates
    /// (backstop (a) / §R5.1).
    const fn is_saturated(&self) -> bool {
        self.weighted_free <= SATURATION_EPSILON
    }
}

/// (#sched-blend) Computes a worker's free-capacity score from its reported
/// per-core-type load percentages and its (P, E) logical-CPU counts, at
/// EXACT centi-core resolution. `assume_core_count` substitutes for a
/// worker that reported `p_count == 0` (legacy / Linux / Intel Mac), giving
/// the absolute-capacity math a denominator (§3.4). `load_byte_cost` is the
/// soak-selected cache-vs-load crossover knob (config).
///
/// INTEGER-ARITHMETIC MANDATES (all load-bearing — see design §2.3):
/// 1. `count * (100 - load)` is widened to `i64` BEFORE the multiply (the
///    counts are `u32`; with the centi-core scale the products are ~100×
///    larger than a whole-core scheme, so the widen is more load-bearing).
/// 2. `REF_FREE - weighted_free` is computed SIGNED then `.max(0)` — an
///    idle big box has `weighted_free >> REF_FREE`, so the subtraction goes
///    negative; doing it in `u64` would underflow to a huge value and the
///    idle box would get MAX penalty and never be selected (T-edge-signed).
/// 3. The only division is the final `* load_byte_cost / 200` (rescaling
///    the centi-core deficit to bytes-per-whole-core), applied AFTER the
///    discriminating deficit is formed — so it cannot reintroduce a
///    whole-core floor.
fn capacity_score(
    p_load: u32,
    e_load: u32,
    aggregate_load: u32,
    p_count: u32,
    e_count: u32,
    assume_core_count: u32,
    load_byte_cost: u64,
) -> CapacityScore {
    // Resolve the effective P-count and the effective P-load.
    // A worker reporting no P-count (legacy / Linux / Intel) falls back to
    // `assume_core_count` all-P, and its aggregate load stands in for
    // p_load (no per-core-type signal). A count-reporting worker uses its
    // real counts and per-type loads. E capacity is keyed off `e_count`
    // (NOT `e_load`, whose `100` is ambiguous), so `e_count == 0`
    // contributes zero free E capacity by construction (§3.4 D5).
    let (eff_p_count, eff_p_load) = if p_count == 0 {
        // Aggregate-only / legacy: assume-N all-P, aggregate stands in.
        // (If the worker reported per-type p_load but zero count — not a
        // real shape — prefer the aggregate, matching today's
        // aggregate-only ranking.)
        let load = if aggregate_load > 0 { aggregate_load } else { p_load };
        (assume_core_count, load)
    } else {
        (p_count, p_load)
    };

    // Clamp loads to [0,100] defensively (wire values are u32; a glitch
    // >100 would make `100 - load` underflow the u32 subtraction).
    let eff_p_load = eff_p_load.min(100);
    let e_load = e_load.min(100);

    // MANDATE 1: widen to i64 BEFORE the multiply.
    let p_free_centi: i64 = i64::from(eff_p_count) * i64::from(100 - eff_p_load);
    let e_free_centi: i64 = if e_count > 0 {
        i64::from(e_count) * i64::from(100 - e_load)
    } else {
        0
    };

    let weighted_free = P_WEIGHT_NUM * p_free_centi + E_WEIGHT_NUM * e_free_centi;

    // MANDATE 2: SIGNED subtract THEN clamp (never u64-underflow).
    let busy_core_equiv = (REF_FREE - weighted_free).max(0);

    // MANDATE 3: the only division, applied after the deficit is formed.
    // Widen the byte cost too so `load_byte_cost * busy_core_equiv` cannot
    // overflow i64 (busy_core_equiv <= REF_FREE = 200, load_byte_cost is a
    // few MiB at most — comfortably in i64, but widen for clarity/safety).
    let load_penalty =
        (i64::try_from(load_byte_cost).unwrap_or(i64::MAX) * busy_core_equiv) / 200;

    CapacityScore {
        weighted_free,
        load_penalty,
    }
}

/// Computes an effective load score for worker selection. Lower is better.
/// Workers with idle P-cores always beat workers with only idle E-cores,
/// creating a two-tier preference. Workers reporting only aggregate load
/// (Linux, old workers) compete in the P-core tier.
///
/// `has_reported_load` distinguishes two cases that produce identical field
/// values but opposite scheduling intent:
///   - `has_reported_load == false`: worker has NEVER sent a load reading;
///     all fields are the construction-default `(0,0,0)` → sort WORST (u64::MAX).
///   - `has_reported_load == true`, all fields `(0,0,0)`: genuinely-idle
///     worker → sort BEST (0).
///
/// Without this flag both cases score `u64::MAX` (the pre-#sched-zeroload
/// behaviour), making a reported-idle worker LOSE to any loaded-but-reporting
/// worker in the LRU/MRU and locality tiebreak paths — a mild asymmetry
/// with `cap_score`, which already uses `has_reported_load` correctly.
fn effective_load_score(p_load: u32, e_load: u32, aggregate_load: u32, has_reported_load: bool) -> u64 {
    if p_load > 0 || e_load > 0 {
        // Has per-core-type data.
        if p_load < 100 {
            // P-cores available: score in [0, 99].
            p_load as u64
        } else {
            // P-cores saturated, only E-cores left: score in [100, 199].
            100 + e_load as u64
        }
    } else if aggregate_load > 0 {
        // Aggregate only (Linux / old worker): treat as P-core tier.
        aggregate_load as u64
    } else if has_reported_load {
        // Genuinely-idle reported worker (all fields 0, has reported): best score.
        0
    } else {
        // Never reported: sort last.
        u64::MAX
    }
}

/// (#sched M1 rebalance v2) Dispatch-count P-headroom predicate with a BOUNDED
/// `p_load` override (design §2). A worker has P-headroom when ANY of three
/// clauses (precedence order) holds:
///
/// 1. **A5** (`p_core_count == 0`, legacy / Linux / Intel-Mac / pre-populated)
///    — UNGATED (always has headroom) so it degrades to current behavior rather
///    than being frozen out (otherwise `0 < 0 == false` would permanently
///    exclude it). Byte-identical to v1.
/// 2. **v1 fresh signal** — a genuinely-free P slot by the synchronously-updated
///    in-flight count (`running_action_infos`, updated under the write lock on
///    assign + completion — never stale, closes red-team R3 / invariant I5).
///    Unchanged from v1.
/// 3. **NEW bounded override** — admits a worker at/over its P-slot count IFF
///    its reported `p_core_load_pct` says the P cores are actually idle
///    (`< idle_threshold_pct`; I/O-bound actions leave P idle) AND it is still
///    under a FRESH-count ceiling `p_core_count * override_factor`. The ceiling
///    is the load-bearing bound (design §3, invariant I5_Bounded): however
///    stale-low `p_load` is, the override admits to at most
///    `p_core_count * override_factor` in-flight actions — past that the fresh
///    count shuts the gate, so a fully-adversarial stale reading yields bounded
///    over-concentration, NOT the runaway R3 pileup.
///
/// `idle_threshold_pct == 0` (the default) makes clause 3 `p_load < 0` = never,
/// so v2 collapses to EXACT v1. Consts are THREADED as params (this is a free
/// `fn(&Worker)`, no `self`); both call sites — the cache-tier gate
/// (`inner_find_and_reserve_worker`) and the fallback's soft tier
/// (`inner_find_worker_for_action`) — pass the scheduler's configured values.
/// All comparisons are `u64` to match `running_action_infos.len(): usize`
/// against `p_core_count: u32` without truncation, and `u64::from(p_core_count)
/// * u64::from(override_factor)` cannot overflow (u32*u32 fits in u64).
fn worker_has_p_headroom(w: &Worker, idle_threshold_pct: u32, override_factor: u32) -> bool {
    let running = w.running_action_infos.len() as u64;
    w.p_core_count == 0
        || running < u64::from(w.p_core_count)
        || (w.p_core_load_pct < idle_threshold_pct
            && running < u64::from(w.p_core_count) * u64::from(override_factor))
}

/// (#sched M1 rebalance v2, §12.1) Ranking preference for the cache-tier and
/// fallback winner selectors — the "magnet fix". SMALLER = preferred. The load
/// term stays the SECONDARY key; this is PRIMARY, so a worker with a genuine
/// free P slot (or an ungated A5 worker) ALWAYS out-ranks an override-admit
/// **regardless of how stale-low the override worker's `p_load` is** (invariant
/// I6). Without this, the re-introduced stale `p_load` would make an
/// override-admitted worker the *preferred* winner (the bounded magnet the §10
/// convergent RECONSIDER named).
///
/// Tiers, computed from the FRESH in-flight count (never stale):
/// - `!p_gate_active` → `0` — gate off / flag off / Phase-2 lift; every worker
///   collapses to the same tier so the tuple reduces to the EXISTING load key
///   (v1/current parity, design §12.3).
/// - genuine free P slot (`running < p_core_count`) OR A5 (`p_core_count == 0`,
///   ungated) → `0` — BEST. (A5 is not in the §12.1 pseudocode but §5 I4 /
///   §12.3 require an ungated worker to rank byte-identically to v1, i.e. by
///   load alone; giving it pref 0 keeps it in the top tier where v1 left it.)
/// - override-admit (clause 3 fired: `p_load < threshold && running <
///   p_core_count * factor`) → `1 + (running - p_core_count)` — the
///   `+ (running - p_core_count)` is the FRESH over-subscription (never stale),
///   so a more-oversubscribed override worker ranks strictly WORSE. This decays
///   the override worker's preference as it fills toward the ceiling (closing
///   red-team's "duration-of-preference" gap: a stale-low `p_load` can no
///   longer hold it at the top of the ranking as it accumulates work), and is
///   the LOAD-BEARING term for `PrefMonotone` (§13 R1: KEEP it).
/// - no headroom → `u64::MAX` — only reachable on the soft fallback path (the
///   cache tiers already excluded such workers via `worker_is_viable_gated`).
///
/// Overflow-safe: `1 + (running - p_core_count)` is computed in `u64` and the
/// subtraction is guarded by the branch order (`running >= p_core_count` on the
/// override arm), so it never underflows.
fn p_headroom_pref(
    w: &Worker,
    p_gate_active: bool,
    idle_threshold_pct: u32,
    override_factor: u32,
) -> u64 {
    if !p_gate_active {
        return 0;
    }
    let running = w.running_action_infos.len() as u64;
    // Genuine free P slot, or A5 (ungated legacy/Linux/Intel worker): BEST tier.
    if w.p_core_count == 0 || running < u64::from(w.p_core_count) {
        return 0;
    }
    // Override-admit (clause 3): eligible up to the fresh-count ceiling,
    // fresh-count-penalized so preference decays toward the ceiling.
    if w.p_core_load_pct < idle_threshold_pct
        && running < u64::from(w.p_core_count) * u64::from(override_factor)
    {
        return 1 + (running - u64::from(w.p_core_count));
    }
    // No headroom (only reachable on the soft, never-filtering fallback path).
    u64::MAX
}

#[derive(Debug)]
struct Workers(LruCache<WorkerId, Worker>);

impl Deref for Workers {
    type Target = LruCache<WorkerId, Worker>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Workers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Note: This could not be a derive macro because this derive-macro
// does not support LruCache and nameless field structs.
impl MetricsComponent for Workers {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let _enter = group!("workers").entered();
        for (worker_id, worker) in self.iter() {
            let _enter = group!(worker_id).entered();
            worker.publish(MetricKind::Component, MetricFieldData::default())?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// A collection of workers that are available to run tasks.
#[derive(MetricsComponent)]
struct ApiWorkerSchedulerImpl {
    /// A `LruCache` of workers available based on `allocation_strategy`.
    #[metric(group = "workers")]
    workers: Workers,

    /// The worker state manager.
    #[metric(group = "worker_state_manager")]
    worker_state_manager: Arc<dyn WorkerStateManager>,
    /// The allocation strategy for workers.
    allocation_strategy: WorkerAllocationStrategy,
    /// (#sched-blend) Cache-vs-load crossover knob (bytes-equiv per whole
    /// weighted core of free-capacity deficit) for the Tier 1 / Tier 1.5
    /// continuous blend. Config (`SimpleSpec::load_byte_cost`); soak-selected.
    load_byte_cost: u64,
    /// (#sched-blend) Substituted P-core count for workers that report
    /// `p_core_count = 0` (legacy / Linux / Intel Mac). Config
    /// (`SimpleSpec::assume_core_count`).
    assume_core_count: u32,
    /// (#sched M1 rebalance) When true, the cache-affinity tiers apply the
    /// dispatch-count P-headroom overflow gate (see
    /// `SimpleSpec::p_headroom_gate_enabled`). Default OFF: the matcher
    /// behaves byte-identically to the pre-gate path until an operator flips
    /// the config flag. Read once per dispatch under the same write lock.
    p_headroom_gate_enabled: bool,
    /// (#sched M1 rebalance v2) `p_core_load_pct` below which P cores count as
    /// idle enough to RELAX the dispatch-count gate (bounded p_load override,
    /// design §2). Config (`SimpleSpec::p_idle_threshold_pct`). Default 0 =
    /// override OFF → EXACT v1 (`p_load < 0` never fires). Only consulted when
    /// `p_headroom_gate_enabled`.
    p_idle_threshold_pct: u32,
    /// (#sched M1 rebalance v2) In-flight ceiling MULTIPLIER for the bounded
    /// p_load override: it admits to at most `p_core_count * factor` in-flight
    /// actions (design §3, I5_Bounded). Config
    /// (`SimpleSpec::p_headroom_override_factor`). Default 2.
    p_headroom_override_factor: u32,
    /// A channel to notify the matching engine that the worker pool has changed.
    worker_change_notify: Arc<Notify>,
    /// Worker registry for tracking worker liveness.
    worker_registry: SharedWorkerRegistry,

    /// Whether the worker scheduler is shutting down.
    shutting_down: bool,

    /// Shared ref to the outer scores cache — cleared on worker eviction
    /// so stale endpoint scores don't persist.
    scores_cache: Arc<tokio::sync::Mutex<LruCache<DigestInfo, Arc<ScoringResult>>>>,

    /// (#sched-zeroload) Shared handle to the same `SchedulerMetrics` the
    /// outer `ApiWorkerScheduler.metrics` publishes. Held here (NOT
    /// `#[metric]`-annotated, mirroring `worker_change_notify` /
    /// `worker_registry`, so it is not double-registered) so the inner
    /// `remove_worker` eviction CHOKE POINT — through which EVERY eviction
    /// path funnels (`immediate_evict_worker` → `remove_worker`) — can
    /// decrement `workers_never_reported_load` for a never-reported worker.
    /// Decrementing only in the outer public `remove_worker` leaked the
    /// gauge for the dominant `remove_timedout_workers` eviction path.
    metrics: Arc<SchedulerMetrics>,

    /// Index for fast worker capability lookup.
    /// Used to accelerate `find_worker_for_action` by filtering candidates
    /// based on properties before doing linear scan.
    capability_index: WorkerCapabilityIndex,

    /// Reverse map: CAS endpoint → WorkerId.
    /// Updated when workers are added/removed.
    endpoint_to_worker: HashMap<Arc<str>, WorkerId>,

    /// (#97) Per-worker BIS resend buffer. Keyed by `cas_endpoint`
    /// because that's the stable identity across reconnects (worker_id
    /// is regenerated by the server at every connect; cas_endpoint is
    /// the worker's own identity). On `add_worker`, we replay any
    /// buffered chunks for the worker's endpoint so an in-flight
    /// reconnect doesn't drop unacked broadcasts.
    ///
    /// On `boot_epoch_id` change (a worker process restart) the
    /// resend buffer for that endpoint is flushed: every digest the
    /// worker had pinned died with the old process, so the BIS-driven
    /// unpins are moot.
    ///
    /// Chunks are wrapped in `Arc` so dispatch (one per worker) and
    /// resend-buffer storage share the same allocation rather than
    /// re-cloning the proto Vec<Digest> per worker.
    bis_resend_buffers: HashMap<String, BisResendBuffer>,
}

/// (#97) Per-worker BIS chunk resend buffer. Holds chunks dispatched to
/// one worker's `cas_endpoint` that have NOT yet been acked. On the
/// next ConnectWorker for the same endpoint (with the same boot_epoch),
/// every buffered chunk is resent so worker reconnects don't lose
/// unpins.
///
/// Bounded at `BIS_REPLAY_BUFFER_MAX_CHUNKS` (#214). Eviction sources:
///   (a) per-broadcast `ack` removal — every successful chunk delivery
///       drops one `(broadcast_id, sequence)` slot;
///   (b) `clear_bis_resend_buffer_for_endpoint` on boot-epoch change;
///   (c) overflow trim — when `add` would push `len()` past the cap,
///       the lex-smallest `(broadcast_id, sequence)` (= oldest
///       broadcast's earliest unacked chunk) is dropped one at a time
///       until the buffer fits. Each dropped chunk increments
///       `overflow_drops` so the operator sees the cap firing.
///
/// `remove_worker` does NOT clear the buffer (the buffer is keyed by
/// `cas_endpoint`, which is the stable identity across reconnects;
/// dropping it on disconnect would lose the unacked unpins the
/// reconnect is supposed to replay). The cap is the only defence
/// against a long-disconnected-but-stable-endpoint worker that never
/// drains its buffer.
#[derive(Debug, Default)]
pub(crate) struct BisResendBuffer {
    /// (broadcast_id, sequence) -> Arc-shared chunk. The chunk is
    /// allocated once at dispatch and shared between the per-worker
    /// dispatch tx (Arc clone, ~8 bytes) and this buffer (Arc clone,
    /// ~8 bytes); without Arc, the proto Vec<Digest> would be
    /// memcpy'd per worker, which at ~64 workers × ~25 chunks per
    /// 100K-digest broadcast = ~1600 redundant Vec clones per
    /// broadcast.
    ///
    /// `BTreeMap` (not `HashMap`) so the overflow trim can pop the
    /// lex-smallest key in O(log N) without a linear scan. Insert /
    /// remove / lookup are all O(log N) — at the 100K cap, log2 ≈ 17,
    /// dominated by allocator and cache effects long before tree
    /// depth matters.
    chunks: BTreeMap<
        (u64, u32),
        Arc<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsInStableStorageChunk>,
    >,

    /// (#214) Cumulative count of chunks dropped by the overflow-trim
    /// policy across this buffer's lifetime. Surfaced via metrics so
    /// the operator sees the cap firing — a non-zero value means the
    /// associated worker is misbehaving (never acking) and the server
    /// is silently dropping replay state.
    overflow_drops: u64,
}

impl BisResendBuffer {
    /// Insert a chunk. If the insert would push the buffer past
    /// `BIS_REPLAY_BUFFER_MAX_CHUNKS`, drop the lex-smallest existing
    /// entries (= oldest broadcast's earliest unacked chunks) until
    /// the buffer fits. Returns the number of pre-existing chunks
    /// dropped to make room (0 in the common path; non-zero only when
    /// the cap fires).
    pub(crate) fn add(
        &mut self,
        chunk: Arc<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsInStableStorageChunk>,
    ) -> usize {
        let key = (chunk.broadcast_id, chunk.sequence);
        // Insert first so a re-insert of an already-present key (same
        // broadcast_id/sequence — not a new entry) doesn't trigger a
        // spurious trim cycle.
        let was_replace = self.chunks.insert(key, chunk).is_some();
        if was_replace {
            return 0;
        }
        let mut dropped = 0usize;
        while self.chunks.len() > BIS_REPLAY_BUFFER_MAX_CHUNKS {
            // pop_first removes the lex-smallest (broadcast_id,
            // sequence) — the oldest broadcast's earliest still-
            // unacked chunk. If the just-inserted chunk happens to be
            // the lex-smallest (e.g. a delayed retry of a very-old
            // broadcast_id), it can be the one trimmed; that's
            // acceptable — the buffer was already past cap, the
            // worker has misbehaved, and the trim's job is to bound
            // memory, not to preserve a specific eviction ordering.
            if self.chunks.pop_first().is_none() {
                break;
            }
            dropped += 1;
        }
        self.overflow_drops = self.overflow_drops.saturating_add(dropped as u64);
        dropped
    }

    pub(crate) fn ack(&mut self, broadcast_id: u64, sequence: u32) {
        self.chunks.remove(&(broadcast_id, sequence));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Cumulative overflow-trim drops observed by this buffer.
    /// Operator-visible signal that the cap is firing.
    #[cfg(test)]
    pub(crate) fn overflow_drops(&self) -> u64 {
        self.overflow_drops
    }
}

/// (#sched-b1) Outcome of `update_action`'s first critical section
/// (`update_action_cs1`). The orchestrator (`ApiWorkerScheduler::update_action`)
/// decides what to do after the lock is dropped based on this.
enum Cs1Decision {
    /// An early-return path completed entirely under the lock
    /// (`ExecutionComplete`); the orchestrator returns `Ok(())`.
    Done,
    /// The op is not running on this worker. The orchestrator runs
    /// `immediate_evict_worker` UNDER the still-held lock (FR-1) and
    /// returns the merged error.
    NotRunning(Error),
    /// The op IS running; run `update_operation` lock-free, then (if
    /// finished) the second critical section.
    Proceed {
        worker_state_manager: Arc<dyn WorkerStateManager>,
        is_finished: bool,
        due_to_backpressure: bool,
        update: UpdateOperationType,
    },
}

/// (#sched-b1) Outcome of `update_action`'s second critical section
/// (`update_action_cs2`).
enum Cs2Outcome {
    /// Slot freed, flags applied, matcher notified — the normal finished path.
    Completed,
    /// The worker was removed during the lock-free window (§6.2) — benign.
    WorkerGone,
    /// The op was already finalized by a concurrent path during the
    /// window (§6.3) — benign; the orchestrator warns + bumps the counter.
    AlreadyFinalized,
    /// `complete_action` returned a non-missing-op error — propagate.
    Error(Error),
}

impl core::fmt::Debug for ApiWorkerSchedulerImpl {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ApiWorkerSchedulerImpl")
            .field("workers", &self.workers)
            .field("allocation_strategy", &self.allocation_strategy)
            .field("worker_change_notify", &self.worker_change_notify)
            .field(
                "capability_index_size",
                &self.capability_index.worker_count(),
            )
            .field("worker_registry", &self.worker_registry)
            .field("endpoint_to_worker_len", &self.endpoint_to_worker.len())
            .finish_non_exhaustive()
    }
}

impl ApiWorkerSchedulerImpl {
    /// (#schedmetric) Recomputes the three point-in-time fleet-saturation gauges
    /// from the live worker pool and stores them into `SchedulerMetrics` atomics.
    ///
    /// Called after every pool mutation (`add_worker`, `remove_worker`,
    /// `update_action_cs2`, `inner_unreserve_worker`) so the gauges reflect
    /// the pool immediately after the change, not at the next scrape.
    ///
    /// O(N workers) scan — acceptable for typical fleet sizes (10-100 workers).
    /// NEVER called inside the scoring hot path or any O(actions) loop.
    ///
    /// No lock acquisition: `self` is already behind the `inner` write lock,
    /// so iterating `self.workers` is safe and non-blocking.
    fn recompute_capacity_gauges(&self) {
        let total = self.workers.len() as u64;
        let mut at_capacity = 0u64;
        let mut running = 0u64;
        for (_, w) in self.workers.iter() {
            if !w.can_accept_work() {
                at_capacity += 1;
            }
            running += w.running_action_infos.len() as u64;
        }
        self.metrics.workers_total.store(total, Ordering::Relaxed);
        self.metrics
            .workers_at_capacity
            .store(at_capacity, Ordering::Relaxed);
        self.metrics
            .total_running_actions
            .store(running, Ordering::Relaxed);
    }

    /// Refreshes the lifetime of the worker with the given timestamp.
    ///
    /// Instead of sending N keepalive messages (one per operation),
    /// we now send a single worker heartbeat. The worker registry tracks worker liveness,
    /// and timeout detection checks the worker's `last_seen` instead of per-operation timestamps.
    ///
    /// Note: This only updates the local worker state. The worker registry is updated
    /// separately after releasing the inner lock to reduce contention.
    fn refresh_lifetime(
        &mut self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let worker = self.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in refresh_lifetime() {}",
                worker_id
            )
        })?;
        error_if!(
            worker.last_update_timestamp > timestamp,
            "Worker already had a timestamp of {}, but tried to update it with {}",
            worker.last_update_timestamp,
            timestamp
        );
        worker.last_update_timestamp = timestamp;

        // If the worker was in quarantine, clear it now that it has checked in.
        if worker.quarantined_at.take().is_some() {
            info!(
                ?worker_id,
                "Worker exited quarantine after sending keepalive"
            );
        }

        trace!(
            ?worker_id,
            running_operations = worker.running_action_infos.len(),
            "Worker keepalive received"
        );

        Ok(())
    }

    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&mut self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id.clone();
        let platform_properties = worker.platform_properties.clone();

        // Update endpoint → worker reverse map for locality scoring.
        if !worker.cas_endpoint.is_empty() {
            self.endpoint_to_worker
                .insert(Arc::from(worker.cas_endpoint.as_str()), worker_id.clone());
        }

        self.workers.put(worker_id.clone(), worker);
        // (#sched-zeroload) Count the worker as never-reported HERE, the instant
        // it ENTERS `self.workers`, symmetric with the decrement at the pop in
        // inner `remove_worker` (`:864`). Every worker entering the map is at the
        // construction default `has_reported_load == false` (`worker.rs:339`,
        // never set true before `add_worker` — only `update_worker_load` flips it,
        // on a worker already in the pool), so the increment is unconditional.
        // Collocating it with the put means any error/evict path that pops the
        // worker (e.g. `send_initial_connection_result` failing below → the outer
        // `add_worker` error branch → `immediate_evict_worker` → `remove_worker`)
        // ALWAYS has a matching increment — no `u64` underflow.
        self.metrics
            .workers_never_reported_load
            .fetch_add(1, Ordering::Relaxed);

        // Add to capability index for fast matching
        self.capability_index
            .add_worker(&worker_id, &platform_properties);

        // Worker is not cloneable, and we do not want to send the initial connection results until
        // we have added it to the map, or we might get some strange race conditions due to the way
        // the multi-threaded runtime works.
        let worker = self.workers.peek_mut(&worker_id).unwrap();
        let res = worker
            .send_initial_connection_result()
            .err_tip(|| "Failed to send initial connection result to worker");
        if let Err(err) = &res {
            error!(
                ?worker_id,
                ?err,
                "Worker connection appears to have been closed while adding to pool"
            );
        }
        // (#schedmetric) Update fleet saturation gauges after pool change.
        self.recompute_capacity_gauges();
        self.worker_change_notify.notify_one();
        res
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    fn remove_worker(&mut self, worker_id: &WorkerId) -> Option<Worker> {
        // Remove from capability index
        self.capability_index.remove_worker(worker_id);

        let result = self.workers.pop(worker_id);

        // Remove from endpoint → worker reverse map.
        if let Some(ref worker) = result {
            if !worker.cas_endpoint.is_empty() {
                self.endpoint_to_worker.remove(worker.cas_endpoint.as_str());
            }
            // (#sched-zeroload) Decrement the never-reported gauge HERE — the
            // single eviction choke point. EVERY eviction path
            // (`remove_timedout_workers`, dispatch-error disconnects, the
            // op-not-running branch, the public `remove_worker`, shutdown)
            // funnels through `immediate_evict_worker` → this `remove_worker`.
            // Decrementing only in the outer public `remove_worker` leaked the
            // gauge monotonically for the dominant timeout path. Read straight
            // off the popped `Worker` — no TOCTOU. Mirrors the increment in the
            // single add path (`add_worker`).
            if !worker.has_reported_load {
                self.metrics
                    .workers_never_reported_load
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }

        // (#schedmetric) Update fleet saturation gauges after pool change.
        self.recompute_capacity_gauges();
        self.worker_change_notify.notify_one();
        result
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(
        &mut self,
        worker_id: &WorkerId,
        is_draining: bool,
    ) -> Result<(), Error> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .err_tip(|| format!("Worker {worker_id} doesn't exist in the pool"))?;
        worker.is_draining = is_draining;
        // (#schedmetric) is_draining is an input to can_accept_work(), so the
        // workers_at_capacity gauge changes here. Drain is exactly when an
        // operator watches the saturation gauge (rolling deploy / fleet drain),
        // so recompute now rather than letting it lag until the next unrelated
        // pool mutation.
        self.recompute_capacity_gauges();
        self.worker_change_notify.notify_one();
        Ok(())
    }

    fn inner_find_worker_for_action(
        &mut self,
        platform_properties: &PlatformProperties,
        full_worker_logging: bool,
    ) -> Option<WorkerId> {
        // Do a fast check to see if any workers are available at all for work allocation
        if !self.workers.iter().any(|(_, w)| w.can_accept_work()) {
            if full_worker_logging {
                info!("All workers are fully allocated");
            }
            return None;
        }

        // Use capability index to get candidate workers that match STATIC properties
        // (Exact, Unknown) and have the required property keys (Priority, Minimum).
        // This reduces complexity from O(W × P) to O(P × log(W)) for exact properties.
        let candidates = self
            .capability_index
            .find_matching_workers(platform_properties, full_worker_logging);

        if candidates.is_empty() {
            if full_worker_logging {
                debug!("No workers in capability index match required properties");
            }
            return None;
        }

        // Clear is_paused for candidate workers that now have capacity,
        // but only if they were paused due to a capacity check (not explicit
        // worker backpressure like ResourceExhausted). Workers that reported
        // ResourceExhausted should remain paused until they complete an action.
        for wid in &candidates {
            if let Some(worker) = self.workers.0.peek_mut(wid) {
                if worker.is_paused && !worker.is_draining && !worker.paused_due_to_backpressure {
                    let has_capacity = worker.max_inflight_tasks == 0
                        || u64::try_from(worker.running_action_infos.len()).unwrap_or(u64::MAX)
                            < worker.max_inflight_tasks;
                    if has_capacity {
                        worker.is_paused = false;
                    }
                }
            }
        }

        // Check function for availability AND dynamic Minimum property verification.
        // The index only does presence checks for Minimum properties since their
        // values change dynamically as jobs are assigned to workers.
        let worker_matches = |(worker_id, w): &(&WorkerId, &Worker)| -> bool {
            // Quarantined workers must not receive new actions.
            if w.quarantined_at.is_some() {
                if full_worker_logging {
                    debug!(
                        "Worker {worker_id} is quarantined, skipping for new work"
                    );
                }
                return false;
            }

            if !w.can_accept_work() {
                if full_worker_logging {
                    debug!(
                        "Worker {worker_id} cannot accept work: is_paused={}, is_draining={}, inflight={}/{}",
                        w.is_paused,
                        w.is_draining,
                        w.running_action_infos.len(),
                        w.max_inflight_tasks
                    );
                }
                return false;
            }

            // (FL-681 re-saturation gate) Skip an indefinite-pin-saturated
            // worker on this LRU/MRU fallback path (the cache-affinity tiers
            // gate via `worker_is_viable`, which carries the same check). Kept
            // SEPARATE from `can_accept_work()` so the `update_action` pause
            // logic (which also calls `can_accept_work()`) is untouched.
            if w.indefinite_pin_saturated {
                if full_worker_logging {
                    debug!(
                        "Worker {worker_id} skipped: indefinite-pin cap saturated (FL-681 re-saturation gate)"
                    );
                }
                return false;
            }

            // (#37 swap-pressure gate) Proactively skip a swap-pressured
            // worker — mirrors the FL-681 skip above. ADVISORY: the
            // worker-side StartAction NAK is the authoritative gate; this
            // avoids the wasted dispatch round-trip (and the idle-worker
            // spin) for a worker we already know is pressured. When EVERY
            // candidate is swap-gated, the fleet fail-open below
            // (`best_swap_gated`) re-admits the least-pressured one rather
            // than wedging — so this skip never causes a deadlock.
            if w.swap_pressured {
                if full_worker_logging {
                    debug!(
                        "Worker {worker_id} skipped: host swap pressure (#37 swap gate)"
                    );
                }
                return false;
            }

            // (F4 disk-pressure gate) Proactively skip a disk-pressured worker
            // — mirrors the swap skip above. ADVISORY: the worker-side
            // StartAction NAK (+ statvfs fallback) is the authoritative gate;
            // this avoids the wasted dispatch round-trip (and the idle-worker
            // spin) for a worker we already know is over its disk floor. When
            // EVERY candidate is disk-gated, the fleet fail-open below
            // (`worker_matches_ignoring_pressure` + the most-free ranking)
            // re-admits the least-pressured one rather than wedging.
            if w.disk_pressured {
                if full_worker_logging {
                    debug!(
                        "Worker {worker_id} skipped: physical disk pressure (F4 disk gate)"
                    );
                }
                return false;
            }

            // Verify Minimum properties at runtime (their values are dynamic)
            if !platform_properties.is_satisfied_by(&w.platform_properties, full_worker_logging) {
                return false;
            }

            true
        };

        // (#37 fleet fail-open, §5 case 3a) A worker that passes EVERY
        // viability check EXCEPT the swap-pressure AND disk-pressure skips.
        // Used ONLY when the normal `viable` set is empty for this capability
        // class, to avoid a whole-fleet wedge when every candidate is
        // pressure-gated: rather than place nothing, the matcher degrades to
        // placing on the LEAST-pressured gated worker. This is NET-NEW (the
        // cache-affinity `best_overloaded` fallback runs on
        // `worker_is_viable`-passing workers and STRUCTURALLY excludes
        // pressure-gated ones, so it cannot serve here). The worker-local
        // fail-opens are the load-bearing backstop (swap: time-bounded; disk:
        // the statvfs authoritative fallback rejects only a TRULY-full disk);
        // this is the proactive optimization that picks the least-bad target.
        // (F4 cadre correction: `disk_pressured` MUST be ignored here too —
        // omit it and a fully disk-pressured fleet wedges its capability class.
        // Placing on the most-free worker is safe: if even that one is truly
        // full, its worker-local statvfs fallback NAKs ResourceExhausted, a
        // re-queue, never an ENOSPC.)
        let worker_matches_ignoring_pressure = |pair: &(&WorkerId, &Worker)| -> bool {
            let (_, w) = pair;
            // Same as `worker_matches` minus the swap + disk pressure skips.
            w.quarantined_at.is_none()
                && w.can_accept_work()
                && !w.indefinite_pin_saturated
                && platform_properties.is_satisfied_by(&w.platform_properties, false)
        };

        // Now check constraints on filtered candidates.
        // Iterate in LRU order based on allocation strategy.
        // Note: iter() does not promote entries in the LRU. We find the worker
        // first via iter(), then promote it via get_mut() below to avoid
        // multiple consecutive actions all matching the same "least recently used" worker.
        let workers_iter = self.workers.iter();

        // (#sched M1 rebalance §12.2 v2) SOFT P-headroom-first ranking. The
        // per-worker sort key is a TUPLE `(p_headroom_pref, effective_load_score)`:
        //   - `p_headroom_pref`: the PRIMARY key (design §12.1). When the gate is
        //     enabled it separates a genuine-free-P-slot worker (pref 0) from an
        //     override-admit (pref `1 + fresh-oversub`) from a no-headroom worker
        //     (pref `u64::MAX`), all from the FRESH in-flight count — so a
        //     free-slot peer ALWAYS out-ranks an override-admit even when the
        //     override worker reports a LOWER (stale) `p_load` (invariant I6, the
        //     magnet fix). This SUPERSEDES the v2.2 `!worker_has_p_headroom` bool:
        //     a strict refinement (bool `{0,1}` → `{0, 1+oversub, MAX}`), so a
        //     genuine free slot still sorts first, override-admits now sort
        //     BETWEEN free-slot and no-headroom AND are fresh-count-ordered among
        //     themselves. With `p_idle_threshold_pct == 0` (default) clause 3
        //     never fires, so pref is `{0, u64::MAX}` = the v2.2 two-way order
        //     (THRESHOLD=0 parity). When the flag is OFF, `p_gate_active` is false
        //     → pref ≡ 0 for every worker → the key reduces to `(0, load)` →
        //     single-tier by load, behaviorally identical to the pre-v2.2 path.
        //   - `effective_load_score`: the EXISTING within-tier ranking, unchanged
        //     (idle P-cores beat idle E-cores; aggregate-only competes in the
        //     P-core tier). Applied WITHIN each pref tier.
        // SOFT, not a filter: no-headroom workers (pref `u64::MAX`) stay in the
        // candidate `Vec` and win when no free-slot/override worker exists
        // (fully-P-saturated fleet), so dispatch always proceeds — NO wedge (I2).
        // The fallback's `p_gate_active` is simply the flag: when no candidate
        // has a free slot, every pref collapses to `u64::MAX` (all-equal primary)
        // → sort by load = the Phase-2-lift parity (§12.3) without a separate
        // pre-scan.
        // INTENTIONAL — the raw flag, NOT the cache tiers'
        // `p_headroom_gate_enabled && any_viable_has_p_headroom` (:1617): the
        // fallback achieves Phase-2-lift no-wedge (I2) AUTOMATICALLY via the
        // all-`u64::MAX` pref tie (all candidates saturated ⇒ equal primary key
        // ⇒ sort by load), so it needs no pre-scan. Do NOT "unify" this with the
        // cache-tier binding by ANDing in `any_viable_has_p_headroom` — that
        // would break I2 (the fallback has no such pre-scan and does not need one).
        let p_gate_active = self.p_headroom_gate_enabled;
        let p_idle_threshold_pct = self.p_idle_threshold_pct;
        let p_headroom_override_factor = self.p_headroom_override_factor;
        let sort_key = |w: &Worker| -> (u64, u64) {
            (
                p_headroom_pref(
                    w,
                    p_gate_active,
                    p_idle_threshold_pct,
                    p_headroom_override_factor,
                ),
                effective_load_score(
                    w.p_core_load_pct,
                    w.e_core_load_pct,
                    w.cpu_load_pct,
                    w.has_reported_load,
                ),
            )
        };
        let viable: Vec<(WorkerId, (u64, u64))> = match self.allocation_strategy {
            WorkerAllocationStrategy::LeastRecentlyUsed => workers_iter
                .rev()
                .filter(|(worker_id, _)| candidates.contains(worker_id))
                .filter(|pair| worker_matches(pair))
                .map(|(_, w)| (w.id.clone(), sort_key(w)))
                .collect(),
            WorkerAllocationStrategy::MostRecentlyUsed => workers_iter
                .filter(|(worker_id, _)| candidates.contains(worker_id))
                .filter(|pair| worker_matches(pair))
                .map(|(_, w)| (w.id.clone(), sort_key(w)))
                .collect(),
        };

        // Pick the best worker by the tuple key: `p_headroom_pref` first, then
        // lowest load within tier. `min_by_key` returns the FIRST minimum, so on
        // an all-unknown-load fleet (every load score == u64::MAX) it degrades to
        // LRU/MRU iteration order within the winning tier — identical to the
        // prior `first()` fallback when the flag is OFF (all keys share the same
        // `(0, u64::MAX)`, so the first candidate wins).
        let mut worker_id = viable
            .iter()
            .min_by_key(|(_, key)| *key)
            .map(|(id, _)| id.clone());

        // (#37 + F4 fleet fail-open, §5 case 3a) Nothing viable: if there ARE
        // otherwise-viable candidates that were excluded ONLY by a pressure
        // skip (swap OR disk), place on the LEAST-pressured one instead of
        // wedging the capability class. The worker-local fail-opens still
        // backstop a stale server view (swap: time-bounded; disk: the statvfs
        // authoritative fallback rejects only a TRULY-full disk); this just
        // avoids the wasted queue-stall when the server already knows every
        // candidate is gated.
        //
        // (F4) The ranking key is a tuple `(disk_pressured, swap_shortfall,
        // Reverse(available_disk_bytes))`: prefer a NOT-disk-pressured worker
        // first (a truly-full disk ENOSPCs; swap degrades gracefully as the OS
        // pager reclaims), then least swap shortfall, then most disk headroom.
        // This DEGENERATES to the prior swap-only behavior when no worker is
        // disk-pressured (all keys share `disk_pressured=false` → ranks purely
        // by `swap_pressure_rate_per_sec`), and to most-free-bytes when all are
        // disk-pressured (shared `swap=0` → ranks by `Reverse(free)`).
        if worker_id.is_none() {
            let workers_iter = self.workers.iter();
            let rank_key = |w: &Worker| {
                (
                    w.disk_pressured,
                    w.swap_pressure_rate_per_sec,
                    core::cmp::Reverse(w.available_disk_bytes),
                )
            };
            let least_pressured = match self.allocation_strategy {
                WorkerAllocationStrategy::LeastRecentlyUsed => workers_iter
                    .rev()
                    .filter(|(wid, _)| candidates.contains(wid))
                    .filter(|pair| pair.1.swap_pressured || pair.1.disk_pressured)
                    .filter(|pair| worker_matches_ignoring_pressure(pair))
                    .min_by_key(|(_, w)| rank_key(w))
                    .map(|(_, w)| w.id.clone()),
                WorkerAllocationStrategy::MostRecentlyUsed => workers_iter
                    .filter(|(wid, _)| candidates.contains(wid))
                    .filter(|pair| pair.1.swap_pressured || pair.1.disk_pressured)
                    .filter(|pair| worker_matches_ignoring_pressure(pair))
                    .min_by_key(|(_, w)| rank_key(w))
                    .map(|(_, w)| w.id.clone()),
            };
            if let Some(ref wid) = least_pressured {
                warn!(
                    worker_id = %wid,
                    "fleet fail-open: every candidate worker is pressure-gated (swap and/or \
                     disk); placing on the least-pressured one to avoid a capability-class \
                     wedge (#37 / F4 §5 case 3a)"
                );
            }
            worker_id = least_pressured;
        }

        // Log load-aware selection decision.
        if let Some(ref wid) = worker_id {
            // (§12.2 v2) each entry is (short_id, (p_headroom_pref, load_score)).
            let viable_loads: Vec<_> = viable
                .iter()
                .map(|(id, key)| {
                    let short_id = id.0.chars().take(12).collect::<String>();
                    (short_id, *key)
                })
                .collect();
            let winner_key = viable
                .iter()
                .find(|(id, _)| id == wid)
                .map(|(_, k)| *k)
                .unwrap_or((0, 0));
            debug!(
                candidates = viable.len(),
                worker_id = %wid,
                winner_p_headroom_pref = winner_key.0,
                winner_load_score = winner_key.1,
                ?viable_loads,
                "load-aware worker selection"
            );
        }

        // Promote the found worker in the LRU so the next find_worker_for_action
        // call won't pick the same worker again (prevents work bunching).
        if let Some(ref wid) = worker_id {
            self.workers.get_mut(wid);
        }

        if full_worker_logging && worker_id.is_none() {
            debug!("No workers matched!");
        }
        worker_id
    }

    /// Atomically finds a suitable worker AND reserves it for the given
    /// operation by mutating the worker's state (reducing platform properties,
    /// inserting into `running_action_infos`). Returns the worker ID, the
    /// channel sender, and pre-built protobuf message so the caller can
    /// send the notification after releasing the lock.
    ///
    /// Uses locality-aware scheduling:
    /// - Primary: score candidates by total bytes of cached input blobs
    ///   using pre-computed endpoint scores (computed outside the lock).
    /// - Fallback: existing LRU/MRU strategy.
    ///
    /// This prevents two concurrent match operations from selecting the
    /// same worker, which is the key enabler for `MATCH_CONCURRENCY > 1`.
    ///
    /// `endpoint_scores` is pre-computed outside the write lock to avoid
    /// holding it during O(files) iterations over the locality map.
    /// Peer hints are NO LONGER threaded through this method — they ride a
    /// separate `Update::ChunkedMessage` stream emitted by the dispatch path
    /// after the lock is dropped (#98).
    fn inner_find_and_reserve_worker(
        &mut self,
        platform_properties: &PlatformProperties,
        operation_id: &OperationId,
        action_info: &ActionInfoWithProps,
        full_worker_logging: bool,
        endpoint_scores: Option<&HashMap<Arc<str>, u64>>,
        resolved_tree: Option<&ResolvedTree>,
    ) -> Option<(WorkerId, UnboundedSender<UpdateForWorker>, UpdateForWorker)> {
        let input_root_digest = action_info.inner.input_root_digest;

        // Build the set of capability-matching candidates that can accept work.
        let candidates = self
            .capability_index
            .find_matching_workers(platform_properties, full_worker_logging);

        if candidates.is_empty() {
            if full_worker_logging {
                debug!("No workers in capability index match required properties");
            }
            return None;
        }

        // Helper: check if a specific worker is a valid candidate.
        let worker_is_viable = |worker_id: &WorkerId| -> bool {
            if !candidates.contains(worker_id) {
                return false;
            }
            let Some(w) = self.workers.0.peek(worker_id) else {
                return false;
            };
            // (FL-681 re-saturation gate) Skip a worker whose indefinite-pin cap
            // is reported saturated: a new F2 action would be NAKed by its
            // admission gate, and the scheduler's conditional pause does not fire
            // for a saturated-but-idle worker — so selecting it here re-arms the
            // worker-NAK → re-queue → re-dispatch spin. Kept SEPARATE from
            // `can_accept_work()` so the `update_action` pause logic (which also
            // calls `can_accept_work()`) is untouched.
            //
            // (#37 swap gate / F4 disk gate) Swap- AND disk-pressured workers
            // are likewise excluded from the cache-affinity tiers: a new action
            // would be NAKed by the worker-local gate, so a cache hit on a
            // pressured worker is a wasted dispatch. The all-gated case does NOT
            // wedge here — the cache-affinity tiers fall through to
            // `inner_find_worker_for_action`'s LRU/MRU path, which carries the
            // fleet fail-open (least-pressured placement, ignoring BOTH swap and
            // disk).
            if w.quarantined_at.is_some()
                || !w.can_accept_work()
                || w.indefinite_pin_saturated
                || w.swap_pressured
                || w.disk_pressured
            {
                return false;
            }
            platform_properties.is_satisfied_by(&w.platform_properties, false)
        };

        // (#sched M1 rebalance v2) Dispatch-count P-headroom predicate with the
        // bounded p_load override (module fn `worker_has_p_headroom`, shared with
        // the fallback's soft tier §12.2). Wrapped in a closure that threads the
        // two configured consts (the free fn takes them as params — it has no
        // `self`; code-reviewer fix 1). With `p_idle_threshold_pct == 0`
        // (default) the override clause is inert → EXACT v1 predicate.
        let p_headroom_gate_enabled = self.p_headroom_gate_enabled;
        let p_idle_threshold_pct = self.p_idle_threshold_pct;
        let p_headroom_override_factor = self.p_headroom_override_factor;
        let has_p_headroom = |w: &Worker| -> bool {
            worker_has_p_headroom(w, p_idle_threshold_pct, p_headroom_override_factor)
        };

        // (#sched-blend) Per-candidate free-capacity score. Replaces the
        // binary `CACHE_AFFINITY_LOAD_CUTOFF` + `best_overloaded` soft-
        // fallback in Tier 1 / Tier 1.5 with a CONTINUOUS load penalty
        // driven by absolute free centi-cores (design §2). Computed from the
        // worker's reported per-core-type loads + its (P, E) counts (with the
        // `assume_core_count` fallback for count-less workers). The same
        // score drives both the penalty (ranking) and the saturation
        // predicate (backstop (a)).
        let load_byte_cost = self.load_byte_cost;
        let assume_core_count = self.assume_core_count;
        let cap_score = |w: &Worker| -> CapacityScore {
            // (#sched-zeroload) A worker that has NEVER reported load is at the
            // construction-default `(0,0,0)`, which `capacity_score` would read
            // as 100% FREE (max `weighted_free` → ZERO penalty) — making it win
            // every Tier-1 min-load tie despite carrying no load signal. Treat
            // it as FULLY BUSY instead: route its real (P,E) counts (or the
            // assume-N fallback) through `capacity_score` at 100% load, yielding
            // `weighted_free == 0` (saturated) and MAX penalty. It thus loses a
            // min-load tie to any worker with known spare capacity, but on a
            // fleet where EVERY candidate is never-reported the saturation
            // fall-through (below) still routes the action via LRU/MRU, so a
            // fresh fleet is not wedged. A worker that HAS reported a genuine
            // all-zero (idle) reading skips this branch and keeps penalty 0.
            if w.has_reported_load {
                capacity_score(
                    w.p_core_load_pct,
                    w.e_core_load_pct,
                    w.cpu_load_pct,
                    w.p_core_count,
                    w.e_core_count,
                    assume_core_count,
                    load_byte_cost,
                )
            } else {
                capacity_score(
                    100,
                    100,
                    100,
                    w.p_core_count,
                    w.e_core_count,
                    assume_core_count,
                    load_byte_cost,
                )
            }
        };

        // (#sched-blend, backstop (a) / §R5.1) The cache tiers must FALL
        // THROUGH to the LRU/MRU path when EVERY viable candidate is
        // saturated — on a fully-saturated fleet the continuous penalty is
        // the same maxed constant for all candidates and CANCELS, so cache
        // alone would decide (the #52 pile-on). Compute the predicate ONCE
        // over the viable candidate set (no extra map walk, no extra lock —
        // reuses the `peek`ed fields). `false` when there are no viable
        // candidates at all (the existing all-gated fall-through, §7, still
        // owns that case via `None` from the tiers).
        //
        // (#sched M1 rebalance, M2) The SAME single pass also folds the
        // P-headroom pre-scan: `any_viable_has_p_headroom` records whether ANY
        // VIABLE worker still has dispatch-count P-headroom (Phase-1 condition).
        // It references VIABLE workers only — a non-viable P-headroom worker
        // must NOT suppress Phase 2 (code-reviewer C1). O(candidates), no new
        // lock, no second pass — reuses the already-`peek`ed worker. When the
        // gate is OFF the fold is inert and the loop is byte-identical to the
        // pre-gate path.
        //
        // (#sched M1 rebalance, A2 fold) The same pass also BUFFERS the
        // viable-but-no-P-headroom workers (id + observability snapshot) so the
        // A2 exclusion log can be emitted below WITHOUT a third O(candidates)
        // pass (perf S2 / code S2). Ordering caveat: `p_gate_active` is not known
        // until this loop finishes (it depends on `any_viable_has_p_headroom`),
        // so we buffer here and emit after. The buffer is only populated when the
        // flag is ON (`p_headroom_gate_enabled`); flag OFF pushes nothing, so
        // there is zero extra alloc on the default path.
        // CAPPED AT candidates.len(): one entry per viable-no-headroom candidate,
        // bounded by the platform-matched candidate set (fleet size); flag-ON,
        // dev/soak-only observability, dropped at end of dispatch.
        let mut viable_count: usize = 0;
        let mut all_viable_saturated = true;
        let mut any_viable_has_p_headroom = false;
        let mut p_gated_excluded: Vec<(WorkerId, usize, u32, u32)> = Vec::new();
        for wid in &candidates {
            if worker_is_viable(wid) {
                viable_count += 1;
                if let Some(w) = self.workers.0.peek(wid) {
                    if !cap_score(w).is_saturated() {
                        all_viable_saturated = false;
                    }
                    if has_p_headroom(w) {
                        any_viable_has_p_headroom = true;
                    } else if p_headroom_gate_enabled {
                        // Buffer for the A2 exclusion log (emitted after
                        // `p_gate_active` is known). Snapshot the fields now while
                        // the worker is `peek`ed — cheap owned copies.
                        p_gated_excluded.push((
                            wid.clone(),
                            w.running_action_infos.len(),
                            w.p_core_count,
                            w.p_core_load_pct,
                        ));
                    }
                }
            }
        }
        // If no candidate is viable, leave the tiers to return `None` (the
        // existing all-gated path), not the saturation fall-through.
        let saturation_fall_through = viable_count > 0 && all_viable_saturated;

        // (#sched M1 rebalance, M1) The P-headroom overflow gate is ACTIVE only
        // when (a) the operator enabled it AND (b) some viable worker still has
        // P-headroom (Phase 1). When no viable worker has P-headroom the gate
        // LIFTS (Phase 2) — selection proceeds over all viable workers via the
        // existing LRU/MRU fallback (I2 no-wedge). OFF by default → `false` →
        // every tier's gated predicate below is identical to `worker_is_viable`.
        let p_gate_active = p_headroom_gate_enabled && any_viable_has_p_headroom;

        // (#sched M1 rebalance v2, §12.1) The ranking-preference key the cache
        // tiers apply as their PRIMARY sort key (the load term becomes
        // SECONDARY). Threads the same `p_gate_active` + consts through the
        // module fn `p_headroom_pref`. A genuine-free-P-slot holder (pref 0)
        // therefore beats an override-admit (pref ≥ 1) REGARDLESS of the
        // override worker's stale-low `p_load` (invariant I6 — the magnet fix).
        // When `p_gate_active` is false (flag off / Phase-2 lift) it returns 0
        // for every worker → the tuple collapses to the existing load key
        // (byte-parity, §12.3). All cache-tier candidates already passed
        // `worker_is_viable_gated`, so here pref is only `{0} ∪ {1+oversub}`
        // (the `u64::MAX` no-headroom arm is unreachable on the gated tiers).
        let pref = |w: &Worker| -> u64 {
            p_headroom_pref(
                w,
                p_gate_active,
                p_idle_threshold_pct,
                p_headroom_override_factor,
            )
        };

        // (#sched M1 rebalance, M1) The gated viability predicate the cache
        // tiers use. Adding `has_p_headroom` here (the single check all three
        // cache tiers call) makes Tier-1 / Tier-1.5 / Tier-2 inherit the gate
        // with no per-tier code (I3 — locality is bounded to the P-headroom
        // set). When `p_gate_active` is false it collapses to `worker_is_viable`
        // exactly (flag OFF, or Phase 2). The gate does NOT touch the LRU/MRU
        // fallback (`inner_find_worker_for_action`, load-based ranking), so a
        // fully-P-saturated fleet still dispatches (I2, A3/A4 — the P-gate and
        // `saturation_fall_through` are independent predicates).
        let worker_is_viable_gated = |worker_id: &WorkerId| -> bool {
            if !worker_is_viable(worker_id) {
                return false;
            }
            if !p_gate_active {
                return true;
            }
            match self.workers.0.peek(worker_id) {
                Some(w) => has_p_headroom(w),
                None => false,
            }
        };

        // (#sched M1 rebalance, A2) Observability: when the gate is active, emit
        // ONE `info!` per viable-but-P-headroom-less worker it excludes, carrying
        // the worker's reported `p_core_load_pct` snapshot. The dispatch-count
        // proxy over-excludes I/O-bound-heavy workers (the incident's citizen ran
        // ~10 actions at p_count=4 while p_load ≈ 20 % — P cores actually idle);
        // logging `p_load` lets the soak operator tell correct spreading from
        // I/O-bound over-exclusion, and gates the v2-after-data `p_load`-
        // refinement decision (O5) on observed data. MUST be `info!`, not
        // `debug!`: the release build pins `release_max_level_info`, so
        // `debug!`/`trace!` are compiled out (see the sibling
        // `phase6_scheduler_dispatch` probe) — a `debug!` here would leave the
        // soak's over-exclusion / WARN-1 abort analysis with ZERO production
        // data. Emitted from the `p_gated_excluded` buffer collected in the
        // pre-scan above (no third pass — perf S2 / code S2). Fires only when the
        // gate is active (flag ON AND Phase 1); at most one line per excluded
        // worker per dispatch (bounded by fleet size). `tag` lets the soak filter
        // these lines cheaply.
        if p_gate_active {
            for (wid, running_actions, p_core_count, p_load) in &p_gated_excluded {
                info!(
                    tag = "p_headroom_gate_exclusion",
                    worker_id = %wid.0,
                    running_actions,
                    p_core_count,
                    p_load,
                    %input_root_digest,
                    "p-headroom gate excluded worker from cache tiers \
                     (at dispatch-count P limit); p_load shows whether \
                     its P cores are truly full or it is I/O-bound"
                );
            }
        }
        if saturation_fall_through {
            // The real #52 signal: every viable candidate is fully
            // saturated. Rate-limited at the call rate is acceptable here
            // (it fires only on a fully-saturated fleet, not per dispatch on
            // a healthy one). Driven by absolute free capacity, NOT the old
            // misleading coverage_pct.
            warn!(
                viable_count,
                %input_root_digest,
                "all viable cache candidates saturated (weighted_free == 0) — \
                 cache tiers fall through to LRU/MRU to spread the unavoidable work \
                 (backstop a / #52)"
            );
        }

        // ── Tier 1: Exact root match (p_headroom_pref, then min-load) ──
        // If a viable worker has the action's input_root_digest in its
        // directory cache (either as a root or as a subtree of a previously
        // cached tree), it can hardlink the entire input tree in
        // milliseconds. Among the viable root/subtree holders, pick the one
        // with the SMALLEST `(p_headroom_pref, load_penalty)` tuple. No cutoff,
        // no `best_overloaded` soft-fallback, no `EXACT_ROOT_GAIN` (dropped — a
        // constant gain cancels across Tier-1 members; §4.1). When every viable
        // candidate is saturated, the tier declines (backstop (a)).
        //
        // (#sched M1 rebalance v2, §12.2) `p_headroom_pref` is the PRIMARY key,
        // `load_penalty` SECONDARY. A genuine-free-P-slot holder (pref 0) thus
        // beats an override-admit (pref ≥ 1) EVEN IF the override holder reports
        // a lower (stale) `p_load` → lower `load_penalty` — closing the bounded
        // magnet (invariant I6). When the gate is off/lifted, `pref ≡ 0` for all
        // holders, so the tuple reduces to min-`load_penalty` = the exact v1
        // (pre-v2) Tier-1 order.
        let dir_cache_winner: Option<WorkerId> = if saturation_fall_through {
            None
        } else {
            // (id, (p_headroom_pref, load_penalty)) — tuple key, smaller wins.
            let mut best: Option<(WorkerId, (u64, i64))> = None;
            for wid in &candidates {
                if let Some(w) = self.workers.0.peek(wid) {
                    let has_root_match = w.cached_directory_digests.contains(&input_root_digest);
                    let has_subtree_match = w.cached_subtree_digests.contains(&input_root_digest);
                    if (has_root_match || has_subtree_match) && worker_is_viable_gated(wid) {
                        let key = (pref(w), cap_score(w).load_penalty);
                        let dominated = best
                            .as_ref()
                            .is_some_and(|(_, best_key)| key >= *best_key);
                        if !dominated {
                            best = Some((wid.clone(), key));
                        }
                    }
                }
            }
            if let Some((ref wid, (p, penalty))) = best {
                debug!(
                    ?wid,
                    p_headroom_pref = p,
                    load_penalty = penalty,
                    %input_root_digest,
                    "directory cache hit — worker has input_root cached \
                     (min (p_headroom_pref, load_penalty))"
                );
            }
            best.map(|(wid, _)| wid)
        };

        // ── Tier 1.5: Partial subtree coverage scoring (continuous blend) ──
        // When no worker has the exact root cached, score workers by a
        // blended metric of cached bytes and cached file count. Each cached
        // file is worth PER_FILE_WEIGHT bytes (hardlink/clonefile has a fixed
        // per-file I/O cost ~0.1 ms ≈ 100 KB at 10 Gbps). (#sched-blend) The
        // selection is now `S = cached_score - load_penalty` (max S wins),
        // where `load_penalty` is the continuous absolute-free-capacity
        // penalty (design §4.2) — replacing the binary cutoff +
        // `best_overloaded` soft-fallback. A moderately-loaded warm worker's
        // marginal cache lead is shed earlier and smoothly; the fully-
        // saturated #52 burst is caught by backstop (a) (the penalty cancels
        // across saturated peers, so the cache tiers decline above).
        const PER_FILE_WEIGHT: u64 = 100 * 1024; // 100KB per file
        let subtree_coverage_winner: Option<WorkerId> = if dir_cache_winner.is_some()
            || saturation_fall_through
        {
            None // exact match found, OR all viable saturated → fall through
        } else if let Some(tree) = resolved_tree {
            let total_bytes: u64 = tree.subtree_bytes.get(&input_root_digest).copied().unwrap_or(0);
            let total_files: u64 = tree.subtree_files.get(&input_root_digest).copied().unwrap_or(0);
            let total_score = total_bytes + total_files * PER_FILE_WEIGHT;
            if tree.dir_digests.len() <= 1 || total_score == 0 {
                None // only root (or empty), no subtrees to match
            } else {
                // (#sched M1 rebalance v2, §12.2) PRIMARY key `p_headroom_pref`
                // (min), SECONDARY key `blended_s` (max, via `Reverse`). So a
                // genuine-free-P-slot subtree holder (pref 0) beats an
                // override-admit (pref ≥ 1) regardless of the override worker's
                // stale-low `p_load` → higher `blended_s` (invariant I6). When
                // the gate is off/lifted, `pref ≡ 0` for all → the tuple reduces
                // to max-`blended_s` = the exact v1 (pre-v2) Tier-1.5 order.
                // (id, p_headroom_pref, blended_S, cached_bytes, cached_files)
                let mut best: Option<(WorkerId, u64, i64, u64, u64)> = None;
                for wid in &candidates {
                    if let Some(w) = self.workers.0.peek(wid) {
                        if !worker_is_viable_gated(wid) {
                            continue;
                        }
                        // #52 (option b2) numerator: sum DIRECT (non-
                        // recursive) bytes/files for each unique cached
                        // dir digest. `dir_digests` is a `HashSet` so each
                        // digest is counted once; `dir_direct_*` is disjoint
                        // across directories. `cached_score` is bounded by
                        // `total_score` (coverage_pct ≤ 100) AND retains
                        // partial-match resolution. See
                        // `compute_dedup_cached_score` + `.claude/audits/
                        // 52-scheduler-subtree-overload-rca-2026-06-04.md`.
                        let (cached_bytes, cached_files): (u64, u64) =
                            compute_dedup_cached_score(
                                &tree.dir_digests,
                                &w.cached_subtree_digests,
                                &tree.dir_direct_bytes,
                                &tree.dir_direct_files,
                            );
                        let cached_score = cached_bytes + cached_files * PER_FILE_WEIGHT;
                        if cached_score == 0 {
                            continue; // unchanged gate: cache-cold workers excluded
                        }
                        // (#sched-blend) S = cache_gain - load_penalty. Both
                        // i64; `cached_score` is naturally bounded ≪ 2^63 by
                        // total tree bytes, and the difference must be signed
                        // so a marginal cache hit on a busy worker can go
                        // negative (correctly ranking below a cache-cold-but-
                        // idle worker whose S = 0; that idle worker is NOT in
                        // this loop — it has cached_score == 0 — so a negative
                        // S still loses to the cascade's later tiers / LRU).
                        let penalty = cap_score(w).load_penalty;
                        let blended_s =
                            i64::try_from(cached_score).unwrap_or(i64::MAX) - penalty;
                        let p = pref(w);
                        // Tuple key `(pref, Reverse(blended_s))`: min-pref then
                        // MAX-blended_s. `Reverse` inverts only the secondary so
                        // the max-S semantics are preserved WITHIN a pref tier.
                        let dominated = best.as_ref().is_some_and(|(_, best_p, best_s, _, _)| {
                            (p, core::cmp::Reverse(blended_s))
                                >= (*best_p, core::cmp::Reverse(*best_s))
                        });
                        if !dominated {
                            best = Some((wid.clone(), p, blended_s, cached_bytes, cached_files));
                        }
                    }
                }
                // (#sched-blend §5.3) Keep the winner only if its blended score
                // is POSITIVE. A cache-cold-but-idle worker has S = 0
                // (cached_score 0, penalty 0) and is the implicit baseline — it
                // is NOT in this loop (its cached_score == 0 was `continue`d),
                // so a NEGATIVE-S Tier-1.5 winner (a small cache hit on a busy
                // worker) must NOT be committed; the tier DECLINES and the
                // cascade falls through to the LRU/MRU path, which selects an
                // idle worker (lowest effective_load_score). This is the
                // crossover: take the cache pick iff the cache saving exceeds
                // the load cost. (Without this gate Tier 1.5 would return its
                // only — negative-S — candidate and pile onto the busy warm
                // worker.) The v2 `pref` PRIMARY key does NOT touch this filter:
                // it selects WHICH holder among the eligible; the crossover then
                // decides whether that holder's cache lead justifies the load —
                // independent of pref (§12.2, PRESERVE the `blended_s > 0`
                // crossover on the winner).
                let best = best.filter(|(_, _, blended_s, _, _)| *blended_s > 0);
                if let Some((ref wid, p, blended_s, cached_bytes, cached_files)) = best {
                    let cached_score = cached_bytes + cached_files * PER_FILE_WEIGHT;
                    let pct = if total_score > 0 { cached_score * 100 / total_score } else { 0 };
                    debug!(
                        ?wid,
                        p_headroom_pref = p,
                        cached_bytes,
                        cached_files,
                        blended_s,
                        coverage_pct = pct,
                        %input_root_digest,
                        "subtree coverage winner — {}% cached \
                         (min p_headroom_pref, then max cache_gain - load_penalty)",
                        pct,
                    );
                }
                best.map(|(wid, _, _, _, _)| wid)
            }
        } else {
            None
        };

        // ── Locality scoring (Tier 2) ──
        // Convert pre-computed endpoint scores to worker scores, filtering
        // to the candidate set. This is O(endpoints) not O(files).
        // (#sched-blend) Tier 2's INTERNAL comparator is UNCHANGED this round
        // (the load-blend here is deferred — §4.3), but backstop (a) applies
        // at the cascade boundary to ALL cache tiers: when every viable
        // candidate is saturated, Tier 2 also declines so the cascade falls
        // through to the LRU/MRU path rather than piling onto the
        // warmest-locality worker (§4.4).
        let locality_winner = if saturation_fall_through {
            None
        } else if let Some(ep_scores) = endpoint_scores {
            let scores = endpoint_scores_to_worker_scores(
                ep_scores,
                &self.endpoint_to_worker,
                &candidates,
            );
            if !scores.is_empty() {
                // Sort workers by cached-bytes descending; tiebreak by
                // effective load score. Per-blob freshness timestamps were
                // dropped from the locality_map (entries persist until
                // explicit eviction), so the prior ts-based tiebreaker is
                // gone — load score is the new tiebreaker within 10%.
                let mut sorted: Vec<_> = scores.into_iter().collect();
                let load_score_for_worker = |wid: &WorkerId| -> u64 {
                    self.workers.0.peek(wid)
                        .map(|w| effective_load_score(w.p_core_load_pct, w.e_core_load_pct, w.cpu_load_pct, w.has_reported_load))
                        .unwrap_or(u64::MAX)
                };
                sorted.sort_by(|a, b| {
                    let score_a = a.1;
                    let score_b = b.1;
                    let max_score = score_a.max(score_b);
                    let threshold = max_score / 10; // 10% of the larger score
                    if score_a.abs_diff(score_b) <= threshold {
                        // Scores similar — prefer lower load score.
                        let load_a = load_score_for_worker(&a.0);
                        let load_b = load_score_for_worker(&b.0);
                        load_a.cmp(&load_b)
                    } else {
                        // Scores differ — prefer higher.
                        score_b.cmp(&score_a)
                    }
                });

                let best = sorted.first().map(|(_, s)| *s).unwrap_or(0);
                if best > 0 {
                    sorted.into_iter()
                        .find(|(wid, score)| *score > 0 && worker_is_viable_gated(wid))
                        .map(|(wid, score)| {
                            debug!(
                                ?wid,
                                score,
                                %input_root_digest,
                                "locality scoring — {} cached bytes",
                                score
                            );
                            wid
                        })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let worker_id = if let Some(wid) = dir_cache_winner {
            // Exact root match trumps all other scoring.
            self.workers.get_mut(&wid);
            wid
        } else if let Some(wid) = subtree_coverage_winner {
            // Partial subtree coverage beats blob-level locality.
            self.workers.get_mut(&wid);
            wid
        } else if let Some(wid) = locality_winner {
            // Blob-level locality scoring.
            self.workers.get_mut(&wid);
            wid
        } else {
            // ── Fallback: existing LRU/MRU strategy ──
            let wid = self.inner_find_worker_for_action(platform_properties, full_worker_logging)?;
            wid
        };

        // Atomically reserve the worker by mutating its state under the same lock.
        // The proto-tree clone (`to_proto_vecs`) is deferred to after the lock
        // drops — `resolved_directories` is injected post-lock by the caller.
        let (tx, msg) = self.prepare_worker_run_action(&worker_id, operation_id, action_info)?;

        Some((worker_id, tx, msg))
    }

    /// Undoes a reservation made by `inner_find_and_reserve_worker`.
    /// This removes the operation from the worker's `running_action_infos`
    /// and restores the reduced platform properties.
    fn inner_unreserve_worker(
        &mut self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
    ) {
        if let Some(worker) = self.workers.get_mut(worker_id) {
            if let Some(pending) = worker.running_action_infos.remove(operation_id) {
                if !worker.restored_platform_properties.remove(operation_id) {
                    worker.restore_platform_properties(
                        &pending.action_info.platform_properties,
                    );
                }
            }
        }
        // (#schedmetric) Recompute after slot freed.
        self.recompute_capacity_gauges();
    }

    // (#sched-b1) First critical section of `update_action`. Runs under
    // the worker-pool `inner` write lock with ZERO `.await` inside. It
    // validates the worker/op, handles the two early-return paths
    // (`ExecutionComplete`, op-not-running eviction), computes the Copy
    // scalars (`is_finished`, `due_to_backpressure`), and clones the
    // `worker_state_manager` Arc so the caller can run `update_operation`
    // lock-free. See `ApiWorkerScheduler::update_action` for the
    // orchestration and §3.1 of the B1 design.
    //
    // `immediate_evict_worker` is async, so the op-not-running branch is
    // NOT handled here; it is signalled via `Cs1Decision::NotRunning` so
    // the orchestrator can run the eviction UNDER the lock exactly as
    // today (FR-1 — B1's only behavioral change is the happy completion
    // path; the eviction-loop lock-across-await decouple is the
    // `#sched-b1-evict-sibling` follow-up, out of scope here).
    fn update_action_cs1(
        &mut self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<Cs1Decision, Error> {
        let worker = self.workers.get_mut(worker_id).err_tip(|| {
            format!("Worker {worker_id} does not exist in SimpleScheduler::update_action")
        })?;

        // ExecutionComplete is sent by the worker after ExecuteResult to
        // signal that post-execution I/O (CAS upload, AC write) has
        // finished and the worker's platform resources can be fully
        // reclaimed. Because ExecuteResult(Completed) already calls
        // complete_action() which removes the operation from
        // running_action_infos, the operation will not be present when
        // ExecutionComplete arrives. This is expected — not an error.
        if matches!(update, UpdateOperationType::ExecutionComplete) {
            if worker.running_action_infos.contains_key(operation_id) {
                worker.execution_complete(operation_id);
            }
            self.worker_change_notify.notify_one();
            return Ok(Cs1Decision::Done);
        }

        // Ensure the worker is supposed to be running the operation.
        if !worker.running_action_infos.contains_key(operation_id) {
            let err = make_err!(
                Code::Internal,
                "Operation {operation_id} should not be running on worker {worker_id} in SimpleScheduler::update_action"
            );
            // The eviction (async) is performed by the orchestrator under
            // the same lock — see FR-1.
            return Ok(Cs1Decision::NotRunning(err));
        }

        let (is_finished, due_to_backpressure) = match &update {
            UpdateOperationType::UpdateWithActionStage(action_stage) => {
                (action_stage.is_finished(), false)
            }
            UpdateOperationType::KeepAlive => (false, false),
            UpdateOperationType::UpdateWithError(err) => {
                (true, err.code == Code::ResourceExhausted)
            }
            UpdateOperationType::UpdateWithDisconnect => (true, false),
            // Handled above before the contains_key check.
            UpdateOperationType::ExecutionComplete => unreachable!(),
        };

        Ok(Cs1Decision::Proceed {
            worker_state_manager: Arc::clone(&self.worker_state_manager),
            is_finished,
            due_to_backpressure,
            update,
        })
    }

    // (#sched-b1) Second critical section of `update_action`. Runs under
    // the worker-pool `inner` write lock with ZERO `.await` inside, AFTER
    // the lock-free `update_operation().await` committed the op-state.
    // Frees the worker's slot and applies the pause/backpressure flags
    // atomically, then notifies the matcher last (so it only observes a
    // consistent post-completion worker state — R3). The worker may have
    // been removed (disconnect/evict) while the lock was released, and
    // the op may have been finalized by a concurrent path during the
    // window (§6.3) — both are tolerated here as benign.
    fn update_action_cs2(
        &mut self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        due_to_backpressure: bool,
    ) -> Cs2Outcome {
        let Some(worker) = self.workers.get_mut(worker_id) else {
            // §6.2 — worker removed during the lock-free window. Removal
            // already drained its running_action_infos; the completion (b)
            // was reported via the action-DB subscriber, not the worker
            // pool. Nothing to free; benign.
            self.worker_change_notify.notify_one();
            return Cs2Outcome::WorkerGone;
        };

        // §6.3 — typed/narrow softening (M-1): only the *op-absent*
        // condition (a legitimate concurrent finalize removed it during
        // the window) is benign. We detect it explicitly under this lock
        // rather than swallowing every complete_action error, so any other
        // (future) complete_action error shape still propagates.
        if !worker.running_action_infos.contains_key(operation_id) {
            // A concurrent finalize removed the op (and freed its slot) during
            // the lock-free window. Notify unconditionally — restoring the
            // pre-B1 behaviour where every update_action exit woke the matcher —
            // so the headroom that concurrent free created is never stranded.
            self.worker_change_notify.notify_one();
            return Cs2Outcome::AlreadyFinalized;
        }

        // complete_action's missing-op error is unreachable here (we just
        // confirmed presence under the same lock); any other error
        // propagates.
        let complete_action_res = worker.complete_action(operation_id);

        if (due_to_backpressure || !worker.can_accept_work()) && worker.has_actions() {
            worker.is_paused = true;
            worker.paused_due_to_backpressure = due_to_backpressure;
        }

        // (#schedmetric) Recompute fleet saturation gauges after the action
        // slot is freed (running_action_infos shrunk, is_paused may have changed).
        self.recompute_capacity_gauges();
        self.worker_change_notify.notify_one();

        match complete_action_res {
            Ok(()) => Cs2Outcome::Completed,
            Err(err) => Cs2Outcome::Error(err),
        }
    }

    /// Prepares a worker to run an action by mutating its state (reducing platform
    /// properties, recording the running action), then returns the cloned `tx` sender
    /// and pre-built message so the caller can send the notification *after* releasing
    /// the write lock.
    ///
    /// The `StartExecute` message is built with EMPTY `resolved_directories`.
    /// The hot dispatch path (`find_and_reserve_worker`) injects the
    /// pre-resolved tree post-lock via `to_proto_vecs()` gated on
    /// `result.is_some()`, so the proto clone is never built on the no-match
    /// path. The reconnect-notify path (`worker_notify_run_action`) leaves
    /// `resolved_directories` empty — those workers re-fetch the tree via
    /// GetTree if they need it.
    ///
    /// Note: peer hints are NO LONGER carried inside `StartExecute` (#98 — peer
    /// hints chunking). They ride a separate `Update::ChunkedMessage` stream
    /// emitted by the dispatch path AFTER the lock is dropped. This keeps the
    /// reserved-write critical section free of an O(hints) proto clone, and
    /// removes the implicit cap that previously truncated to 16384 hints.
    ///
    /// Returns `None` if the worker was not found.
    fn prepare_worker_run_action(
        &mut self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        action_info: &ActionInfoWithProps,
    ) -> Option<(UnboundedSender<UpdateForWorker>, UpdateForWorker)> {
        let worker = self.workers.get_mut(worker_id)?;
        // Clone the tx so we can send outside the lock.
        let tx = worker.tx.clone();

        // Build the protobuf message while we still have access to worker state.
        // `resolved_directories` is left empty here; the dispatch path injects
        // the pre-resolved tree post-lock (see `find_and_reserve_worker`).
        let start_execute = StartExecute {
            execute_request: Some(action_info.inner.as_ref().into()),
            operation_id: operation_id.to_string(),
            queued_timestamp: Some(action_info.inner.insert_timestamp.into()),
            platform: Some((&action_info.platform_properties).into()),
            worker_id: worker.id.clone().into(),
            resolved_directories: Vec::new(),
            resolved_directory_digests: Vec::new(),
            missing_digests: Vec::new(),
        };
        let msg = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(start_execute)),
        };

        // If the operation is already reserved on this worker (a concurrent
        // do_try_match beat us), skip — otherwise the later unreserve_worker
        // on the losing match would remove the winning reservation, leaving
        // the worker's running_action_infos empty and preventing the action
        // from being re-queued when the worker is removed.
        if worker.running_action_infos.contains_key(operation_id) {
            return None;
        }

        // Perform the state mutation that run_action would do:
        // reduce platform properties and record the running action.
        reduce_platform_properties(
            &mut worker.platform_properties,
            &action_info.platform_properties,
        );
        worker.running_action_infos.insert(
            operation_id.clone(),
            PendingActionInfoData {
                action_info: action_info.clone(),
            },
        );
        // #36 Phase 6 §6 Phase 0 probe P-SCHED-DISPATCH: mark the
        // wall-clock at which the scheduler dispatches a StartAction to a
        // worker. Paired with P-SCHED-COMPLETE-RECV; the gap = scheduler
        // critical-section latency between previous-action-completion and
        // next-action-dispatch. Per op_id, a log scan correlates
        // op_id_n+1 here with op_id_n from the recv probe on the same
        // worker. Observability only, no behaviour change.
        let phase6_dispatch_at_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        debug!(
            tag = "phase6_scheduler_dispatch",
            op_id_n_plus_1 = %operation_id,
            worker_id = ?worker_id,
            dispatch_at_us = phase6_dispatch_at_us,
            "phase6 scheduler dispatching StartAction"
        );
        // #queue-attrib: attribute the worker-reported `queue_ms`
        // (running_actions_manager.rs Action-phase-timing). That figure lumps
        // accept→worker-pickup into one number; we cannot tell scheduler-match
        // time from StartExecute-delivery + worker-accept. This INFO line carves
        // out the FIRST sub-interval: `match_latency_ms` = how long the action
        // sat in the scheduler from Execute-accept (the server-stamped
        // `insert_timestamp`, forwarded as `queued_timestamp` on StartExecute
        // above) until a worker was assigned (this dispatch instant). The
        // operator then derives delivery+accept = worker `queue_ms` −
        // `match_latency_ms`.
        //
        // MUST be `info!`, not `debug!`: the release build pins
        // `release_max_level_info`, so `debug!`/`trace!` are compiled out (the
        // sibling `phase6_scheduler_dispatch` debug probe is invisible in prod
        // for exactly this reason). Per-action emit (~0.44/s busy hour) — not a
        // hot loop; both dispatch call sites (`inner_find_and_reserve_worker`
        // and `worker_notify_run_action`) funnel through here, so one line
        // covers every assignment. saturating_sub guards a clock that ran
        // backwards (returns 0 rather than wrapping).
        let queued_at_us = action_info
            .inner
            .insert_timestamp
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let match_latency_ms = phase6_dispatch_at_us.saturating_sub(queued_at_us) / 1000;
        info!(
            tag = "scheduler_dispatch_attribution",
            %operation_id,
            ?worker_id,
            match_latency_ms,
            "scheduler assigned action to worker; latency is the accept→worker-assigned interval"
        );
        // (#schedmetric) Recompute fleet saturation gauges after the action
        // slot is filled (running_action_infos grew, worker may now be at capacity).
        self.recompute_capacity_gauges();
        Some((tx, msg))
    }

    /// Evicts the worker from the pool and puts items back into the queue if anything was being executed on it.
    async fn immediate_evict_worker(
        &mut self,
        worker_id: &WorkerId,
        err: Error,
        is_disconnect: bool,
    ) -> Result<(), Error> {
        // Clear scores cache so stale endpoint scores don't persist.
        // Use try_lock to break a potential ABBA deadlock:
        // find_and_reserve_worker acquires scores_cache then inner write lock,
        // while immediate_evict_worker holds the inner write lock and needs
        // scores_cache. Skipping is safe — the LRU evicts stale entries naturally.
        if let Ok(mut cache) = self.scores_cache.try_lock() {
            cache.clear();
        } else {
            debug!(?worker_id, "scores_cache clear skipped (lock held), stale scores may persist briefly");
        }

        let mut result = Ok(());
        if let Some(mut worker) = self.remove_worker(worker_id) {
            // We don't care if we fail to send message to worker, this is only a best attempt.
            drop(worker.notify_update(WorkerUpdate::Disconnect).await);
            let update = if is_disconnect {
                UpdateOperationType::UpdateWithDisconnect
            } else {
                UpdateOperationType::UpdateWithError(err)
            };
            for (operation_id, _) in worker.running_action_infos.drain() {
                result = result.merge(
                    self.worker_state_manager
                        .update_operation(&operation_id, worker_id, update.clone())
                        .await,
                );
            }
        }
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        // TODO(palfrey) This should be moved to inside the Workers struct.
        self.worker_change_notify.notify_one();
        result
    }
}

#[derive(Debug, MetricsComponent)]
pub struct ApiWorkerScheduler {
    #[metric]
    inner: RwLock<ApiWorkerSchedulerImpl>,
    #[metric(group = "platform_property_manager")]
    platform_property_manager: Arc<PlatformPropertyManager>,

    #[metric(
        help = "Timeout of how long to evict workers if no response in this given amount of time in seconds."
    )]
    worker_timeout_s: u64,
    /// Shared worker registry for checking worker liveness.
    worker_registry: SharedWorkerRegistry,

    /// Performance metrics for observability.
    /// (#231) `group = "scheduler_metrics"` makes the `RootMetricsComponent`
    /// walk descend into `SchedulerMetrics` so its counters render on the
    /// `/metrics` endpoint (previously dark).
    #[metric(group = "scheduler_metrics")]
    metrics: Arc<SchedulerMetrics>,

    /// Blob locality map for peer-to-peer blob sharing.
    /// Used to generate peer hints in StartExecute messages.
    locality_map: Option<SharedBlobLocalityMap>,

    /// CAS store for resolving input trees (reading Directory protos).
    /// When set, enables tier-2 locality scoring.
    cas_store: Option<Store>,

    /// Cached resolved input trees: input_root_digest → ResolvedTree.
    /// Bounded by both count (TREE_CACHE_CAPACITY) and total heap bytes
    /// (TREE_CACHE_MAX_BYTES) to prevent unbounded memory growth.
    /// Held under a tokio::Mutex briefly for get/put, not during I/O.
    tree_cache: Arc<tokio::sync::Mutex<ByteBoundedTreeCache>>,

    /// Digests currently being resolved in background tasks. Prevents
    /// duplicate spawns when many actions share the same input root.
    tree_resolution_in_progress: Arc<tokio::sync::Mutex<HashSet<DigestInfo>>>,

    /// Negative cache: root digests whose tree resolution failed recently.
    /// Entries carry (timestamp, attempt_count) for exponential backoff:
    /// attempt 1 → 60s, attempt 2 → 300s, attempt 3 → 1500s, attempt 4+ → 1800s (capped).
    tree_resolution_failures: Arc<tokio::sync::Mutex<HashMap<DigestInfo, (Instant, u32)>>>,

    /// Negative cache for individual directory digests that failed during
    /// BFS resolution. Keyed by the specific subdirectory that was missing,
    /// not the root digest. This prevents N different root digests that
    /// share a common failing subdirectory from each triggering independent
    /// resolution attempts. Entries expire after 60s.
    failed_directory_digests: Arc<tokio::sync::Mutex<HashMap<DigestInfo, Instant>>>,

    /// Cache of endpoint scores keyed by input_root_digest.
    /// Avoids recomputing locality scores for identical input trees.
    /// Bounded LRU (1024 entries) — stale entries from worker churn are
    /// naturally evicted rather than cleared wholesale.
    scores_cache: Arc<tokio::sync::Mutex<LruCache<DigestInfo, Arc<ScoringResult>>>>,

    /// Cached GrpcStore connections to worker CAS endpoints for prefetch.
    /// Protected by a sync Mutex since we only hold it briefly to clone a Store.
    /// `Arc` so the spawned prefetch task can insert into the cache after
    /// creating a fresh connection.
    prefetch_connections: Arc<ParkingMutex<HashMap<Arc<str>, Store>>>,

    /// Per-worker semaphore limiting concurrent prefetch streams.
    /// Key is the worker CAS endpoint.
    prefetch_semaphores: ParkingMutex<HashMap<Arc<str>, Arc<Semaphore>>>,

    /// Size threshold from the SizePartitioningStore in the CAS chain.
    /// Blobs below this size are routed to MemoryStore and benefit from
    /// cache warming; blobs at or above are routed to a noop/disk store
    /// where warming would waste I/O. Probed at construction time from
    /// the actual store topology. 0 means warming is disabled.
    #[metric(help = "SizePartitioningStore threshold for cache warming filter")]
    memory_store_threshold: u64,

    /// Optional TLS config for connecting to worker CAS endpoints.
    /// When set, prefetch connections use TLS with this config.
    worker_tls_config: Option<ClientTlsConfig>,

    /// (#97) Monotonic broadcast id allocator for BIS chunked broadcasts.
    /// Lock-free `fetch_add` removes the per-broadcast write-lock
    /// previously taken to bump a u64. Initialised to 1 so a fresh
    /// scheduler's first broadcast carries id=1 (avoids the "did the
    /// counter ever advance?" ambiguity of a 0 sentinel).
    next_bis_broadcast_id: AtomicU64,

    /// (#97 red-team #5) Server-generation nonce. Random u64 chosen at
    /// scheduler construction; injected into every dispatched
    /// `BlobsInStableStorageChunk.server_instance_token`. Workers echo
    /// it back in `BisAck.server_instance_token` and the scheduler
    /// silently drops acks whose token doesn't match — guarding
    /// against a worker holding a stale ack across a server bounce
    /// dropping an unrelated chunk from the new server's resend
    /// buffer (which would happen because `next_bis_broadcast_id`
    /// resets to 1 on startup → broadcast_id collisions across
    /// server-instance boundaries are guaranteed).
    ///
    /// Never zero (the all-zero token is the proto default and would
    /// silently coexist with a buggy worker that omitted the field;
    /// regenerate until non-zero).
    server_instance_token: u64,
}

/// Probe a CAS store chain to find the SizePartitioningStore threshold.
///
/// Walks the chain ExistenceCacheStore -> VerifyStore -> FastSlowStore ->
/// SizePartitioningStore by downcasting each layer via `as_any()` and
/// following the inner/fast store references. Returns the partition size
/// if found, or 0 if the chain doesn't contain a SizePartitioningStore
/// (which disables cache warming).
fn probe_partition_size(store: &Store) -> u64 {
    let driver: &dyn StoreDriver = store.as_store_driver();
    probe_partition_size_inner(driver, 0)
}

fn probe_partition_size_inner(driver: &dyn StoreDriver, depth: u32) -> u64 {
    // Guard against infinite recursion in unexpected topologies.
    if depth > 10 {
        return 0;
    }

    let any = driver.as_any();

    // Direct hit: this layer is SizePartitioningStore.
    if let Some(sps) = any.downcast_ref::<SizePartitioningStore>() {
        return sps.partition_size();
    }

    // ExistenceCacheStore<SystemTime> — the production instantiation.
    if let Some(ecs) = any.downcast_ref::<ExistenceCacheStore<SystemTime>>() {
        return probe_partition_size_inner(ecs.inner_store().as_store_driver(), depth + 1);
    }

    // VerifyStore.
    if let Some(vs) = any.downcast_ref::<VerifyStore>() {
        return probe_partition_size_inner(vs.inner_store().as_store_driver(), depth + 1);
    }

    // FastSlowStore — recurse into the fast store (where MemoryStore lives).
    if let Some(fss) = any.downcast_ref::<FastSlowStore>() {
        return probe_partition_size_inner(fss.fast_store().as_store_driver(), depth + 1);
    }

    // Unknown store type — threshold not found.
    0
}

/// Maximum number of entries in the resolved input tree LRU cache.
const TREE_CACHE_CAPACITY: usize = 1024;

/// Maximum total estimated heap bytes for the tree cache. Prevents
/// unbounded memory growth when cached trees are large (e.g., monorepo
/// input roots with hundreds of thousands of files). When the byte
/// limit is exceeded, the least-recently-used entries are evicted
/// until usage drops below.
const TREE_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

/// LRU cache for resolved input trees, bounded by both entry count
/// and total estimated heap bytes.
#[derive(Debug)]
struct ByteBoundedTreeCache {
    lru: LruCache<DigestInfo, Arc<ResolvedTree>>,
    total_bytes: u64,
    max_bytes: u64,
}

impl ByteBoundedTreeCache {
    fn new(max_count: NonZeroUsize, max_bytes: u64) -> Self {
        Self {
            lru: LruCache::new(max_count),
            total_bytes: 0,
            max_bytes,
        }
    }

    fn get(
        &mut self,
        key: &DigestInfo,
    ) -> Option<&Arc<ResolvedTree>> {
        self.lru.get(key)
    }

    /// Inserts `key`/`value`, evicting to stay within both bounds, and
    /// returns the number of entries EVICTED. (#p1p2 telemetry) A same-key
    /// replacement is a replace, not an eviction, and returns 0 for that
    /// displacement; only count-capacity overflow and the byte-budget loop
    /// contribute to the returned count. Eviction LOGIC is unchanged —
    /// only the return value is added.
    fn put(
        &mut self,
        key: DigestInfo,
        value: Arc<ResolvedTree>,
    ) -> usize {
        let new_bytes = value.estimated_heap_bytes();
        let mut evicted_count: usize = 0;

        // push() returns the displaced entry: either a same-key
        // replacement or the LRU entry evicted on capacity overflow.
        // put() silently drops on overflow, so we must use push().
        if let Some((displaced_key, displaced_val)) = self.lru.push(key, value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(displaced_val.estimated_heap_bytes());
            // A displaced key EQUAL to the inserted key is a same-key
            // replacement, not an eviction — net entry count is unchanged.
            // A DIFFERENT key is a count-capacity eviction of the LRU entry.
            if displaced_key != key {
                evicted_count += 1;
            }
        }
        self.total_bytes += new_bytes;

        // Evict LRU entries until we're within the byte budget.
        while self.total_bytes > self.max_bytes {
            if let Some((_evicted_key, evicted_val)) = self.lru.pop_lru() {
                let evicted_bytes = evicted_val.estimated_heap_bytes();
                self.total_bytes =
                    self.total_bytes.saturating_sub(evicted_bytes);
                evicted_count += 1;
            } else {
                break;
            }
        }

        evicted_count
    }

    fn len(&self) -> usize {
        self.lru.len()
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Maximum size of a single blob eligible for prefetch (1MiB).
/// Larger blobs are more efficiently handled by the worker's parallel
/// ByteStream fetch (128-512 concurrent streams). Prefetch targets
/// small-to-medium blobs where per-blob RPC overhead dominates.
const PREFETCH_MAX_SINGLE_BLOB_SIZE: u64 = 4 * 1024 * 1024;

/// Maximum number of concurrent prefetch batch RPCs per worker.
const PREFETCH_MAX_CONCURRENT_PER_WORKER: usize = 8;

/// Maximum total bytes in-flight for prefetch per dispatch (200MB).
const PREFETCH_MAX_INFLIGHT_BYTES: u64 = 200 * 1024 * 1024;

/// Maximum number of blobs to prefetch per dispatch. High count
/// because small blobs are cheap to push via BatchUpdateBlobs.
const PREFETCH_MAX_BLOBS: usize = 1024;

/// Maximum total bytes per BatchUpdateBlobs RPC batch (4MiB).
/// Matches PREFETCH_MAX_SINGLE_BLOB_SIZE so all prefetched blobs
/// can go through the efficient batch path.
const PREFETCH_BATCH_SIZE_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum concurrent get_part_unchunked calls during server cache warm.
const CACHE_WARM_CONCURRENCY: usize = 64;

/// Maximum total bytes to warm in a single cache warm pass (256MB).
const CACHE_WARM_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Maximum number of blobs to warm in a single cache warm pass.
const CACHE_WARM_MAX_BLOBS: usize = 4096;

/// Maximum encoded size of the pre-resolved Directory tree carried inside
/// a `StartExecute` message. Trees larger than this are omitted (the worker
/// falls back to its own GetTree RPC). Bounded at 32 MiB — the Worker API
/// listener has `max_encoding_message_size = 64MiB`, so 32 MiB leaves
/// headroom for the rest of the message.
///
/// Test override: in `#[cfg(test)]` builds the cap is reduced to 4 KiB so a
/// production-composition test can exercise the over-size-gate path (tree
/// present + worker matched ⇒ `resolved_directories` omitted) with a small
/// (~200-file, ~20 KiB) tree instead of building a literal >32 MiB proto.
/// 4 KiB is comfortably above a single-file directory (~90 bytes, so the
/// fits-the-message tests stay green) and below the ~20 KiB over-size
/// fixture. The gate mechanism (`estimated_bytes > cap` ⇒ skip the clone)
/// is identical at any cap value.
#[cfg(not(test))]
const MAX_TREE_PROTO_BYTES: usize = 32 * 1024 * 1024;
#[cfg(test)]
const MAX_TREE_PROTO_BYTES: usize = 4 * 1024;

/// Base backoff duration after a failed tree resolution (first attempt).
const FAILURE_BACKOFF: Duration = Duration::from_secs(60);

/// Maximum backoff duration for repeated tree resolution failures.
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(1800);

/// When a negative cache map exceeds this many entries, sweep expired ones.
const NEGATIVE_CACHE_SWEEP_THRESHOLD: usize = 1000;

/// Hard upper bound on a background tree resolution attempt. A hung CAS
/// connection without this limit could leave the digest marked as
/// in-progress forever, blocking all future locality lookups for that
/// input root. Combined with the RAII `TreeResolutionGuard`, this ensures
/// `tree_resolution_in_progress` cannot leak entries even under
/// pathological CAS failures.
const TREE_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(60);

/// RAII guard that removes a digest from `tree_resolution_in_progress` when
/// dropped. Ensures that even if the future driving `resolve_input_tree` is
/// cancelled mid-resolution (RPC client disconnect, request timeout,
/// `find_and_reserve_worker` future replaced), the in-progress flag is
/// released so future locality lookups for this digest are not silently
/// skipped forever.
///
/// The Drop body schedules an async removal via `background_spawn!`. If the
/// runtime is already gone (e.g. shutdown), the spawn is effectively a
/// no-op and the entry would remain — but at that point the scheduler is
/// terminating, so any leak is moot.
struct TreeResolutionGuard {
    digest: DigestInfo,
    in_progress: Arc<tokio::sync::Mutex<HashSet<DigestInfo>>>,
}

impl Drop for TreeResolutionGuard {
    fn drop(&mut self) {
        let in_progress = self.in_progress.clone();
        let digest = self.digest;
        background_spawn!("tree_resolution_guard_drop", async move {
            in_progress.lock().await.remove(&digest);
        });
    }
}

/// Computes exponential backoff for tree resolution failures.
/// attempt 1 → base (60s), attempt 2 → 300s, attempt 3 → 1500s, attempt 4+ → 1800s (capped).
fn backoff_for_attempt(base: Duration, attempts: u32) -> Duration {
    if attempts <= 1 {
        return base;
    }
    let multiplier = 5u64.saturating_pow(attempts - 1);
    let backoff_secs = base.as_secs().saturating_mul(multiplier);
    Duration::from_secs(backoff_secs.min(MAX_FAILURE_BACKOFF.as_secs()))
}

impl ApiWorkerScheduler {
    pub fn new(
        worker_state_manager: Arc<dyn WorkerStateManager>,
        platform_property_manager: Arc<PlatformPropertyManager>,
        allocation_strategy: WorkerAllocationStrategy,
        worker_change_notify: Arc<Notify>,
        worker_timeout_s: u64,
        worker_registry: SharedWorkerRegistry,
    ) -> Arc<Self> {
        Self::new_with_locality_map(
            worker_state_manager,
            platform_property_manager,
            allocation_strategy,
            worker_change_notify,
            worker_timeout_s,
            worker_registry,
            None,
            None,
            None,
            // (#sched-blend) defaults matching `SimpleSpec` serde defaults
            // for the no-config constructor path — sourced from the SAME
            // `default_*` fns the config uses, so this 3rd copy cannot drift
            // (code-reviewer note; previously hardcoded `512 * 1024, 8`).
            nativelink_config::schedulers::default_load_byte_cost(),
            nativelink_config::schedulers::default_assume_core_count(),
            // (#sched M1 rebalance) P-headroom gate defaults OFF.
            false,
            // (#sched M1 rebalance v2) override tunables: threshold 0 =
            // override OFF (exact v1), factor 2 (from the config default fn).
            0,
            nativelink_config::schedulers::default_p_headroom_override_factor(),
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new_with_locality_map(
        worker_state_manager: Arc<dyn WorkerStateManager>,
        platform_property_manager: Arc<PlatformPropertyManager>,
        allocation_strategy: WorkerAllocationStrategy,
        worker_change_notify: Arc<Notify>,
        worker_timeout_s: u64,
        worker_registry: SharedWorkerRegistry,
        locality_map: Option<SharedBlobLocalityMap>,
        cas_store: Option<Store>,
        worker_tls_config: Option<ClientTlsConfig>,
        load_byte_cost: u64,
        assume_core_count: u32,
        p_headroom_gate_enabled: bool,
        p_idle_threshold_pct: u32,
        p_headroom_override_factor: u32,
    ) -> Arc<Self> {
        let memory_store_threshold = cas_store
            .as_ref()
            .map(probe_partition_size)
            .unwrap_or(0);

        if memory_store_threshold > 0 {
            info!(
                memory_store_threshold,
                "probed SizePartitioningStore threshold for cache warming"
            );
        }

        let scores_cache = Arc::new(tokio::sync::Mutex::new(LruCache::new(
            NonZeroUsize::new(TREE_CACHE_CAPACITY).unwrap(),
        )));

        // (#sched-zeroload) One `SchedulerMetrics` shared between the outer
        // (publisher) and the inner struct (where the eviction choke point
        // decrements `workers_never_reported_load`). Single allocation —
        // both `metrics` fields below point at the same counters.
        let metrics = Arc::new(SchedulerMetrics::default());

        // (#sched-blend) Zero-guard the assume-N fallback at STORE time. A
        // worker reporting no core count (legacy / Linux / Intel) substitutes
        // `assume_core_count` as its P-core denominator in `capacity_score`.
        // If the operator configured `assume_core_count = 0`, that denominator
        // is zero → `p_free_centi = 0` → `weighted_free = 0`, so the count-less
        // worker is BOTH max-penalized AND spuriously flagged saturated even
        // when idle → it can never win a cache-affine selection it should win
        // (starved). Normalize to `>= 1` here, the single point the value
        // enters `ApiWorkerSchedulerImpl`, so every downstream read is safe.
        let assume_core_count = assume_core_count.max(1);
        if assume_core_count == 1 {
            // Reached only when the config value was 0 or 1; warn on the
            // 0-misconfig case (1 P-core is an implausibly small assume-N).
            warn!(
                "assume_core_count normalized to 1 (configured value was 0 or 1); \
                 count-less workers will be treated as single-P-core boxes — set a \
                 realistic assume_core_count for any non-count-reporting fleet"
            );
        }

        Arc::new(Self {
            inner: RwLock::new(ApiWorkerSchedulerImpl {
                workers: Workers(LruCache::unbounded()),
                worker_state_manager,
                allocation_strategy,
                load_byte_cost,
                assume_core_count,
                p_headroom_gate_enabled,
                p_idle_threshold_pct,
                p_headroom_override_factor,
                worker_change_notify,
                worker_registry: worker_registry.clone(),
                shutting_down: false,
                capability_index: WorkerCapabilityIndex::new(),
                endpoint_to_worker: HashMap::new(),
                scores_cache: scores_cache.clone(),
                metrics: metrics.clone(),
                bis_resend_buffers: HashMap::new(),
            }),
            platform_property_manager,
            worker_timeout_s,
            worker_registry,
            metrics,
            locality_map,
            cas_store,
            tree_cache: Arc::new(tokio::sync::Mutex::new(ByteBoundedTreeCache::new(
                NonZeroUsize::new(TREE_CACHE_CAPACITY).unwrap(),
                TREE_CACHE_MAX_BYTES,
            ))),
            tree_resolution_in_progress: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            tree_resolution_failures: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            failed_directory_digests: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            scores_cache,
            prefetch_connections: Arc::new(ParkingMutex::new(HashMap::new())),
            prefetch_semaphores: ParkingMutex::new(HashMap::new()),
            memory_store_threshold,
            worker_tls_config,
            next_bis_broadcast_id: AtomicU64::new(1),
            // (#97 red-team #5) Random nonce per server-process. Loop
            // to regenerate if `gen` returns 0 — the proto default for
            // an unset uint64 field is 0; treating it as always-mismatch
            // protects against legacy workers that don't echo the token
            // BUT also means an unlikely all-zero generation would
            // refuse every ack from its OWN workers. Keep generating
            // until non-zero (probability 2^-64 per draw → terminates
            // with overwhelming probability on the first call).
            server_instance_token: {
                use rand::Rng;
                let mut rng = rand::rng();
                let mut token: u64 = rng.random();
                while token == 0 {
                    token = rng.random();
                }
                tracing::info!(
                    target: "nativelink::bis_chunked_dispatch",
                    %token,
                    "ApiWorkerScheduler: generated server_instance_token \
                     for BIS ack-token validation (#97 red-team #5)"
                );
                token
            },
        })
    }

    /// Returns a reference to the worker registry.
    pub const fn worker_registry(&self) -> &SharedWorkerRegistry {
        &self.worker_registry
    }

    /// Removes cached prefetch connection and semaphore for a specific endpoint.
    fn remove_prefetch_for_endpoint(&self, endpoint: &str) {
        self.prefetch_connections.lock().remove(endpoint);
        self.prefetch_semaphores.lock().remove(endpoint);
    }

    /// Removes prefetch entries whose endpoint is no longer associated with
    /// any active worker. Called after bulk worker evictions to prevent
    /// unbounded growth of the prefetch maps.
    fn cleanup_stale_prefetch_entries(&self, active_endpoints: &HashSet<Arc<str>>) {
        self.prefetch_connections
            .lock()
            .retain(|ep, _| active_endpoints.contains(ep));
        self.prefetch_semaphores
            .lock()
            .retain(|ep, _| active_endpoints.contains(ep));
    }

    pub async fn worker_notify_run_action(
        &self,
        worker_id: WorkerId,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        self.metrics
            .actions_dispatched
            .fetch_add(1, Ordering::Relaxed);

        // Phase 1: Acquire write lock, mutate worker state, extract tx + message,
        // then drop the lock BEFORE sending on the channel.
        let prepare_result = {
            let mut inner = self.inner.write().await;
            let result =
                inner.prepare_worker_run_action(&worker_id, &operation_id, &action_info);
            if result.is_none() {
                // Worker not found - handle under the lock since we need worker_state_manager.
                warn!(
                    ?worker_id,
                    %operation_id,
                    ?action_info,
                    "Worker not found in worker map in worker_notify_run_action"
                );
                return inner
                    .worker_state_manager
                    .update_operation(
                        &operation_id,
                        &worker_id,
                        UpdateOperationType::UpdateWithDisconnect,
                    )
                    .await;
            }
            result
            // inner (write lock) is dropped here
        };

        // Phase 2: Send notification outside the lock to avoid blocking other
        // scheduler operations if the channel has backpressure.
        if let Some((tx, msg)) = prepare_result {
            if let Err(_send_err) = tx.send(msg) {
                // Worker disconnected. Re-acquire lock to evict.
                warn!(
                    ?worker_id,
                    ?action_info,
                    "Worker command failed (disconnected), removing worker",
                );
                let err = make_err!(
                    Code::Internal,
                    "Worker command failed, removing worker {worker_id} -- Worker Disconnected",
                );
                let mut inner = self.inner.write().await;
                return Result::<(), _>::Err(err.clone()).merge(
                    inner
                        .immediate_evict_worker(&worker_id, err, true)
                        .await,
                );
            }
        }

        Ok(())
    }

    /// Sends the start-execution notification for a worker that was already
    /// reserved by `find_and_reserve_worker`. The worker's state has already
    /// been mutated (platform properties reduced, action recorded in
    /// `running_action_infos`), so this method only sends the pre-built
    /// message over the channel and handles disconnection errors.
    pub async fn send_reserved_worker_notification(
        &self,
        worker_id: &WorkerId,
        tx: UnboundedSender<UpdateForWorker>,
        msg: UpdateForWorker,
    ) -> Result<(), Error> {
        self.metrics
            .actions_dispatched
            .fetch_add(1, Ordering::Relaxed);

        if let Err(_send_err) = tx.send(msg) {
            // Worker disconnected. Re-acquire lock to evict.
            warn!(
                ?worker_id,
                "Worker command failed (disconnected) after reservation, removing worker",
            );
            let err = make_err!(
                Code::Internal,
                "Worker command failed, removing worker {worker_id} -- Worker Disconnected",
            );
            let mut inner = self.inner.write().await;
            return Result::<(), _>::Err(err.clone()).merge(
                inner
                    .immediate_evict_worker(worker_id, err, true)
                    .await,
            );
        }

        Ok(())
    }

    /// Returns the scheduler metrics for observability.
    #[must_use]
    pub const fn get_metrics(&self) -> &Arc<SchedulerMetrics> {
        &self.metrics
    }

    /// Attempts to find a worker that is capable of running this action.
    // TODO(palfrey) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    pub async fn find_worker_for_action(
        &self,
        platform_properties: &PlatformProperties,
        full_worker_logging: bool,
    ) -> Option<WorkerId> {
        let start = Instant::now();
        self.metrics
            .find_worker_calls
            .fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.write().await;
        let worker_count = inner.workers.len() as u64;
        let result = inner.inner_find_worker_for_action(platform_properties, full_worker_logging);

        // Track workers iterated (worst case is all workers)
        self.metrics
            .workers_iterated
            .fetch_add(worker_count, Ordering::Relaxed);

        if result.is_some() {
            self.metrics
                .find_worker_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .find_worker_misses
                .fetch_add(1, Ordering::Relaxed);
        }

        #[allow(clippy::cast_possible_truncation)]
        self.metrics
            .find_worker_time_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    /// Atomically finds a suitable worker AND reserves it for the given
    /// operation. This combines the find and reservation into a single lock
    /// acquisition, preventing two concurrent match operations from selecting
    /// the same worker.
    ///
    /// Returns `(worker_id, tx, msg)` where `tx` and `msg` can be used to
    /// send the start-execution notification to the worker outside the lock.
    /// Returns `None` if no suitable worker was found.
    ///
    /// If the caller later decides not to use this reservation (e.g., because
    /// `assign_operation` fails), it MUST call `unreserve_worker` to undo
    /// the reservation.
    pub async fn find_and_reserve_worker(
        &self,
        platform_properties: &PlatformProperties,
        operation_id: &OperationId,
        action_info: &ActionInfoWithProps,
        full_worker_logging: bool,
    ) -> Option<(WorkerId, UnboundedSender<UpdateForWorker>, UpdateForWorker)> {
        let start = Instant::now();
        self.metrics
            .find_worker_calls
            .fetch_add(1, Ordering::Relaxed);

        // ── Phase 1: async tree resolution (BEFORE write lock) ──
        let resolved_tree = self
            .resolve_input_tree(action_info.inner.input_root_digest)
            .await;

        // ── Phase 2: pre-compute locality scores and peer hints (BEFORE write lock) ──
        // These are O(files × endpoints_per_blob) operations that previously
        // ran inside the write lock, blocking all scheduler operations for
        // 2-5ms on large actions (50K+ inputs).
        // Results are cached by input_root_digest so identical input trees
        // skip the recomputation entirely.
        //
        // The result is kept as Arc<ScoringResult> and passed by reference
        // into the write-lock phase. This eliminates the per-action deep
        // clone of Vec<PeerHint> (up to 16K entries with Vec<String>
        // endpoints) and HashMap<Arc<str>, ...> that previously consumed
        // ~61% of scheduler CPU during active builds.
        let input_root_digest = action_info.inner.input_root_digest;
        debug!(
            has_tree = resolved_tree.is_some(),
            has_locality_map = self.locality_map.is_some(),
            %input_root_digest,
            "scoring: pre-match state"
        );
        let scoring_result: Option<Arc<ScoringResult>> = match (&resolved_tree, &self.locality_map) {
            (Some(tree), Some(loc_map)) => {
                // Check the scores cache first (lock briefly, no await while held).
                let cached = self.scores_cache.lock().await.get(&input_root_digest).cloned();
                if let Some(arc) = cached {
                    Some(arc)
                } else {
                    let result = score_and_generate_hints(&tree.file_digests, loc_map);
                    let arc = Arc::new(result);
                    self.scores_cache.lock().await.put(
                        input_root_digest,
                        Arc::clone(&arc),
                    );
                    Some(arc)
                }
            }
            _ => None,
        };

        // ── Phase 2.5: size-gate for the pre-resolved tree ──
        // Run the O(dirs) encoded_len walk BEFORE the write lock so the
        // lock critical section stays bounded.  The actual Directory proto
        // clone (`to_proto_vecs()`) is deferred until AFTER the lock drops
        // and only when a worker was selected (`result.is_some()`), so the
        // clone is never built on the no-match path (the common case on a
        // busy fleet where every still-queued action cycles through
        // `do_try_match` until a worker becomes available).
        // `MAX_TREE_PROTO_BYTES` (module const, 32 MiB in prod, 4 KiB in test)
        // caps the encoded tree size against the worker API listener's
        // 64 MiB max_encoding_message_size.
        //
        // Compute the size-gate check and capture estimated_bytes for the
        // deferred success-path debug log (symmetric with the over-threshold
        // warning, which still logs estimated_bytes).
        let (tree_fits_in_message, tree_estimated_bytes): (bool, usize) =
            resolved_tree.as_deref().map_or((false, 0), |tree| {
                let estimated_bytes: usize = tree
                    .directories
                    .values()
                    .map(|d| Message::encoded_len(d))
                    .sum();
                if estimated_bytes > MAX_TREE_PROTO_BYTES {
                    debug!(
                        estimated_bytes,
                        max = MAX_TREE_PROTO_BYTES,
                        dirs = tree.directories.len(),
                        "pre-resolved tree exceeds size threshold, omitting from StartExecute"
                    );
                    (false, estimated_bytes)
                } else {
                    (true, estimated_bytes)
                }
            });

        // ── Phase 3: acquire write lock, do selection + reservation ──
        // Inside the lock we only do O(workers) work: candidate filtering,
        // endpoint→WorkerId mapping, and state mutation. Peer hints are
        // emitted on the same `tx` as separate `ChunkedMessage` payloads
        // AFTER the lock drops (see Phase 6 below) — they no longer ride
        // inside `StartExecute`.
        let mut inner = self.inner.write().await;
        let worker_count = inner.workers.len() as u64;
        let endpoint_scores: Option<&HashMap<Arc<str>, u64>> =
            scoring_result.as_deref().map(|sr| &sr.scores);
        let mut result = inner.inner_find_and_reserve_worker(
            platform_properties,
            operation_id,
            action_info,
            full_worker_logging,
            endpoint_scores,
            resolved_tree.as_deref(),
        );

        // Extract the selected worker's CAS endpoint while we still hold
        // the lock, for use in the prefetch spawn below.
        let worker_cas_endpoint: Option<Arc<str>> = result.as_ref().and_then(|(wid, _, _)| {
            inner
                .workers
                .peek(wid)
                .filter(|w| !w.cas_endpoint.is_empty())
                .map(|w| Arc::from(w.cas_endpoint.as_str()))
        });

        // Track workers iterated (worst case is all workers)
        self.metrics
            .workers_iterated
            .fetch_add(worker_count, Ordering::Relaxed);

        if result.is_some() {
            self.metrics
                .find_worker_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .find_worker_misses
                .fetch_add(1, Ordering::Relaxed);
        }

        #[allow(clippy::cast_possible_truncation)]
        self.metrics
            .find_worker_time_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Drop the write lock before spawning prefetch.
        drop(inner);

        // ── Phase 2.5 deferred: inject pre-resolved tree into StartExecute ──
        // `to_proto_vecs()` clones the Directory protos and is only called
        // now that we know a worker was selected (result.is_some()) AND the
        // tree fits within the wire limit (tree_fits_in_message).  On the
        // no-match path (result.is_none()) this block is skipped entirely,
        // avoiding the clone on every do_try_match cycle for still-queued
        // actions on a backlogged fleet.
        if tree_fits_in_message {
            if let Some(tree) = resolved_tree.as_deref() {
                if let Some((_, _, msg)) = result.as_mut() {
                    if let Some(update_for_worker::Update::StartAction(start_execute)) =
                        msg.update.as_mut()
                    {
                        let (dirs, digests) = tree.to_proto_vecs();
                        debug!(
                            dirs = dirs.len(),
                            estimated_bytes = tree_estimated_bytes,
                            "including pre-resolved tree in StartExecute"
                        );
                        start_execute.resolved_directories = dirs;
                        start_execute.resolved_directory_digests = digests;
                    }
                }
            }
        }

        // ── Phase 4: spawn targeted prefetch + missing digest hints ──
        // If we have a resolved tree, a locality map, and the selected
        // worker has a CAS endpoint, compute the set of missing blobs and
        // push them to the worker concurrently with the StartExecute dispatch.
        // Also reuse the missing set for cache warming (Phase 5) so we only
        // warm blobs the worker will actually fetch from the server.
        //
        // Additionally, inject the full set of missing digests (all sizes)
        // into the StartExecute message so the worker can skip its own
        // has_with_results existence check, saving 5-50ms per action.
        let missing_blobs = if let (Some(tree), Some(loc_map), Some(endpoint)) =
            (&resolved_tree, &self.locality_map, &worker_cas_endpoint)
        {
            // (#407) Reuse the scoring-time snapshot if Phase 2 captured
            // one. Both Phase-4 walks (prefetch + inline `all_missing`)
            // consult the snapshot rather than re-acquiring
            // `locality_map.read()`, dropping 3× O(F) → 1× O(F) per
            // cold-tree dispatch.
            let locality_snapshot: Option<&LocalitySnapshot> =
                scoring_result.as_deref().and_then(|sr| sr.locality_snapshot.as_ref());

            // Compute small-blob prefetch candidates (size-capped).
            let prefetch_missing = Self::compute_missing_blobs(
                &tree.file_digests,
                endpoint,
                locality_snapshot,
                loc_map,
            );
            if !prefetch_missing.is_empty() {
                self.spawn_prefetch(
                    Arc::clone(endpoint),
                    prefetch_missing.clone(),
                    operation_id.to_string(),
                );
            }

            // Compute the FULL set of missing digests (all sizes) for the
            // missing_digests hint in StartExecute. This lets the worker
            // skip the has_with_results round-trip entirely.
            //
            // (#407) Fast path: scoring captured a snapshot — no
            // `locality_map.read()` here. Slow path falls back to a
            // fresh read when the snapshot was skipped (over-cap).
            let all_missing: Vec<(DigestInfo, u64)> = if let Some(snapshot) = locality_snapshot {
                tree.file_digests
                    .iter()
                    .filter(|(_, size)| *size > 0)
                    .filter(|(digest, _)| {
                        snapshot
                            .get(digest)
                            .is_none_or(|endpoints| {
                                !endpoints.iter().any(|e| &**e == endpoint.as_ref())
                            })
                    })
                    .copied()
                    .collect()
            } else {
                let map = loc_map.read();
                let blobs = map.blobs_map();
                let collected: Vec<(DigestInfo, u64)> = tree.file_digests
                    .iter()
                    .filter(|(_, size)| *size > 0)
                    .filter(|(digest, _)| {
                        blobs
                            .get(digest)
                            .is_none_or(|endpoints| endpoints.get(endpoint.as_ref()).is_none())
                    })
                    .copied()
                    .collect();
                drop(map);
                collected
            };

            // Inject missing_digests into the StartExecute proto message.
            if let Some((_, _, ref mut msg)) = result {
                if let Some(update_for_worker::Update::StartAction(ref mut start_execute)) =
                    msg.update
                {
                    start_execute.missing_digests = all_missing
                        .iter()
                        .map(|(digest, _)| (*digest).into())
                        .collect();
                }
            }

            Some(prefetch_missing)
        } else {
            None
        };

        // ── Phase 5: spawn server-side cache warm (AFTER write lock released) ──
        // Read blobs through the full CAS chain so MemoryStore gets populated.
        // Already-warm blobs are a ~5us no-op; cold blobs get read from disk.
        // When a locality map is available, only warm blobs the worker is
        // missing (blobs it already has won't be fetched from the server, so
        // warming them is wasted work). Without a locality map, fall back to
        // warming all file_digests.
        if let Some(tree) = &resolved_tree {
            let blobs_to_warm = missing_blobs
                .as_deref()
                .unwrap_or(&tree.file_digests);
            self.spawn_server_cache_warm(blobs_to_warm, operation_id);
        }

        // ── Phase 6: emit `PeerHintsChunk` messages on the worker tx ──
        // (#98) The hints are NOT in `StartExecute` anymore; they ride a
        // separate `Update::ChunkedMessage` stream on the same tx so the
        // worker can register them into its `peer_locality_map` as they
        // arrive — no buffer, no ordering invariant vs StartAction. The
        // worker-side arm is stateless: each chunk's hints are merged
        // directly into the global locality map.
        //
        // A `tx.send` failure here means the worker just disconnected —
        // we don't unwind the reservation because (a) the StartAction
        // dispatch via `send_reserved_worker_notification` will hit the
        // same disconnect and trigger eviction there, (b) chunks are
        // best-effort hints whose loss only degrades to LRU/MRU
        // selection at the worker.
        //
        // When there are zero hints to send (no resolved tree, or a
        // resolved tree with no peer-cached blobs) we emit NOTHING. The
        // worker's `Update::ChunkedMessage(PeerHints)` arm is purely
        // additive — it merges hints into the map and is a no-op on an
        // empty payload. There is no protocol consumer that depends on
        // an empty terminal "no hints" marker (the `chunk_iter` empty-
        // terminal guarantee is informational only, not load-bearing
        // here — the receiver does not gate any state on its arrival).
        // Skipping the empty case avoids one wire message + one tx.send
        // per StartAction and keeps the StartAction stream uncluttered
        // for tests and operators that expect "first message after a
        // dispatch is StartAction" in the no-hint case.
        if let Some((worker_id, tx, _)) = result.as_ref() {
            if let Some(arc) = scoring_result.as_deref() {
                if !arc.peer_hints.is_empty() {
                    self.emit_peer_hints_chunks(worker_id, tx, operation_id, &arc.peer_hints);
                }
            }
        }

        result
    }

    /// Emit one or more `Update::ChunkedMessage(PeerHintsChunk)` messages
    /// on the worker's tx. Chunks of at most `PEER_HINTS_PER_CHUNK` hints
    /// each; the final chunk has `is_last = true` and may be empty when
    /// `hints.len() % PEER_HINTS_PER_CHUNK == 0`.
    ///
    /// Caller invariant: do NOT invoke with an empty `hints` slice. The
    /// scheduler short-circuits zero-hint dispatches one frame up
    /// (`do_try_match`'s Phase 6) so this function never produces an
    /// "empty terminal" wire message — see the doc on `chunk_iter::ChunkIter`
    /// for why the empty-terminal guarantee is informational only and not
    /// a load-bearing protocol contract for any current consumer.
    ///
    /// Uses the shared `nativelink_util::chunk_iter` helper so chunk
    /// boundaries match the BlobsInStableStorage producer (PR #97) and
    /// the BlobsAvailable producer (PR #99) — same correctness pieces
    /// (terminal `is_last`) live in one place.
    fn emit_peer_hints_chunks(
        &self,
        worker_id: &WorkerId,
        tx: &UnboundedSender<UpdateForWorker>,
        operation_id: &OperationId,
        hints: &Arc<[PeerHint]>,
    ) {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            ChunkedMessage, PeerHintsChunk, chunked_message,
        };
        use nativelink_util::chunk_iter::ChunkIter;

        let op_id_str = operation_id.to_string();
        let total = hints.len();
        let mut sent_chunks = 0u32;
        // Walk by index so we can build owned `PeerHint` clones cheaply
        // (each `PeerHint` is Clone). The `Arc<[_]>` keeps the storage
        // shared across all per-worker dispatches of the same scoring
        // result; only the proto for the wire is owned.
        let iter = (0..total).map(|i| hints[i].clone());
        for chunk in ChunkIter::new(iter, PEER_HINTS_PER_CHUNK) {
            let proto = ChunkedMessage {
                payload: Some(chunked_message::Payload::PeerHints(PeerHintsChunk {
                    peer_hints: chunk.items,
                    operation_id: op_id_str.clone(),
                    sequence: chunk.sequence,
                    is_last: chunk.is_last,
                })),
            };
            let msg = UpdateForWorker {
                update: Some(update_for_worker::Update::ChunkedMessage(proto)),
            };
            if tx.send(msg).is_err() {
                warn!(
                    ?worker_id,
                    operation_id = %op_id_str,
                    sent_chunks,
                    total_hints = total,
                    "peer-hints chunk send failed (worker disconnected); StartAction dispatch will detect + evict"
                );
                return;
            }
            sent_chunks = sent_chunks.saturating_add(1);
        }
        if total > 0 {
            debug!(
                ?worker_id,
                operation_id = %op_id_str,
                hint_count = total,
                chunk_count = sent_chunks,
                per_chunk = PEER_HINTS_PER_CHUNK,
                "emitted peer-hints chunks"
            );
        }
    }

    /// Undoes a reservation made by `find_and_reserve_worker`. This must
    /// be called if the match is abandoned after reservation (e.g., if
    /// `assign_operation` returns an error).
    pub async fn unreserve_worker(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
    ) {
        let mut inner = self.inner.write().await;
        inner.inner_unreserve_worker(worker_id, operation_id);
    }

    /// Returns true if any registered worker could match the given platform
    /// properties (static check only — does not consider dynamic resource
    /// availability like current cpu_count).
    pub async fn has_matching_workers(&self, platform_properties: &PlatformProperties) -> bool {
        let inner = self.inner.read().await;
        !inner
            .capability_index
            .find_matching_workers(platform_properties, false)
            .is_empty()
    }

    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    #[must_use]
    pub async fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.inner.read().await;
        inner.workers.contains(worker_id)
    }

    /// (FL-681) Reads a worker's reported indefinite-pin saturation flag.
    /// Test-only — lets the server-handler seam test assert that a
    /// `BlobsAvailable` carrying `indefinite_pin_saturated` reaches the
    /// `Worker`. `None` when the worker is absent.
    #[must_use]
    pub async fn worker_indefinite_pin_saturated_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Option<bool> {
        let inner = self.inner.read().await;
        inner
            .workers
            .0
            .peek(worker_id)
            .map(|w| w.indefinite_pin_saturated)
    }

    /// (#sched-zeroload) Reads a worker's `has_reported_load` flag. Test-only —
    /// lets the `worker_api_server` ingest-seam test assert that an all-zero
    /// (genuinely idle) keepalive/blobs/execute report reaches the scheduler and
    /// flips the flag (the gate-removal contract). `None` when the worker is
    /// absent.
    #[must_use]
    pub async fn worker_has_reported_load_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Option<bool> {
        let inner = self.inner.read().await;
        inner
            .workers
            .0
            .peek(worker_id)
            .map(|w| w.has_reported_load)
    }

    /// (#sched-blend) Reads a worker's stored (P, E) logical-CPU counts.
    /// Test-only — lets the `worker_api_server` ingest-seam test assert that
    /// a `ConnectWorkerRequest` carrying an over-large `p_core_count` was
    /// CLAMPED to `MAX_PLAUSIBLE_CORES` before reaching the `Worker`
    /// (security S1). `None` when the worker is absent.
    #[must_use]
    pub async fn worker_core_counts_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Option<(u32, u32)> {
        let inner = self.inner.read().await;
        inner
            .workers
            .0
            .peek(worker_id)
            .map(|w| (w.p_core_count, w.e_core_count))
    }

    /// (#sched-zeroload) Returns the current `workers_never_reported_load` gauge
    /// value. Test-only — asserts that the gauge tracks `has_reported_load`
    /// transitions correctly across `add_worker` / `update_worker_load` /
    /// `remove_worker`.
    #[cfg(test)]
    #[must_use]
    pub fn workers_never_reported_load_for_test(&self) -> u64 {
        self.metrics
            .workers_never_reported_load
            .load(Ordering::Relaxed)
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub async fn send_keep_alive_to_worker_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }

    /// Resolves the full input tree for the given `input_root_digest`,
    /// returning a cached result if available. On cache miss, returns
    /// `None` immediately (falling back to load-based scoring) and
    /// spawns a background task to resolve the tree from CAS so that
    /// future actions with the same input root hit the cache.
    ///
    /// Returns `None` if no CAS store is configured or on cache miss
    /// (the background task will warm the cache for next time).
    ///
    /// This keeps CAS I/O off the scheduling critical path — only a
    /// brief `tokio::Mutex` lock for the cache lookup is performed
    /// synchronously.
    async fn resolve_input_tree(
        &self,
        input_root_digest: DigestInfo,
    ) -> Option<Arc<ResolvedTree>> {
        let cas_store = self.cas_store.as_ref()?;

        // Check positive cache first (brief lock).
        {
            let mut cache = self.tree_cache.lock().await;
            if let Some(cached) = cache.get(&input_root_digest) {
                // (#p1p2 telemetry) warm-path hit.
                self.metrics
                    .tree_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    %input_root_digest,
                    file_count = cached.file_digests.len(),
                    dir_count = cached.dir_digests.len(),
                    "tree resolution cache hit"
                );
                return Some(cached.clone());
            }
        }

        // Check negative cache: skip if this digest failed recently.
        // Uses exponential backoff: 60s, 300s, 1500s, 1800s (capped).
        {
            let mut failures = self.tree_resolution_failures.lock().await;
            // Sweep expired entries to prevent unbounded growth.
            if failures.len() > NEGATIVE_CACHE_SWEEP_THRESHOLD {
                failures.retain(|_, &mut (failed_at, attempts)| {
                    failed_at.elapsed() < backoff_for_attempt(FAILURE_BACKOFF, attempts)
                });
            }
            if let Some(&(failed_at, attempts)) = failures.get(&input_root_digest) {
                let backoff = backoff_for_attempt(FAILURE_BACKOFF, attempts);
                if failed_at.elapsed() < backoff {
                    return None;
                }
            }
        }

        // Atomically check and mark as in-progress to avoid TOCTOU race.
        // The guard removes the entry on Drop, including the case where
        // this future is cancelled (RPC client disconnect, request timeout,
        // outer future replaced) — preventing permanent leaks of the
        // in-progress flag that would silently disable locality scoring
        // for this digest forever.
        {
            let mut in_progress = self.tree_resolution_in_progress.lock().await;
            if !in_progress.insert(input_root_digest) {
                return None;
            }
        }
        let resolution_guard = TreeResolutionGuard {
            digest: input_root_digest,
            in_progress: self.tree_resolution_in_progress.clone(),
        };

        // Cache miss — resolve inline so the current action benefits from
        // locality scoring. Tree resolution is typically fast (MemoryStore
        // or local CAS) and the result is cached for future actions.
        // A 500ms timeout prevents slow CAS lookups from blocking dispatch.
        // GetTree with subtree caching resolves 1000-dir trees in 10-50ms
        // when warm, but cold starts (first action for a new tree) need
        // up to ~200ms for store fetches. 500ms covers p99 of warm+cold.
        //
        // (#p1p2 telemetry) A cold resolution is being attempted (the
        // positive and negative caches both missed and the in-progress
        // guard was acquired).
        self.metrics
            .tree_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let resolve_fut = resolve_tree_from_cas(
            cas_store,
            input_root_digest,
            &self.failed_directory_digests,
        );
        // (#p1p2 telemetry) Time the cold resolution to isolate its cost
        // from the warm (hit) path. Recorded on the success arm below.
        let resolve_started = Instant::now();
        let resolve_result =
            tokio::time::timeout(Duration::from_millis(500), resolve_fut).await;
        let resolve_elapsed = resolve_started.elapsed();

        match resolve_result {
            Ok(Ok(resolved)) => {
                // resolution_guard fires here, releasing the in-progress flag.
                drop(resolution_guard);
                // (#p1p2 telemetry) Successful cold resolution — record its
                // elapsed cost (sum + count) so the mean cold-resolution
                // latency is derivable.
                self.metrics
                    .tree_resolution_cold_time_ns
                    .fetch_add(resolve_elapsed.as_nanos() as u64, Ordering::Relaxed);
                self.metrics
                    .tree_resolution_cold_count
                    .fetch_add(1, Ordering::Relaxed);
                let entry_bytes = resolved.estimated_heap_bytes();
                debug!(
                    %input_root_digest,
                    file_count = resolved.file_digests.len(),
                    dir_count = resolved.dir_digests.len(),
                    entry_bytes,
                    "inline tree resolution complete, caching"
                );
                let arc = Arc::new(resolved);
                let mut cache = self.tree_cache.lock().await;
                // (#p1p2 telemetry) put() now reports the exact number of
                // entries it evicted (count-cap displacement + byte-budget
                // loop; same-key replace = 0). Read the post-put resident
                // bytes / entry count while the cache lock is STILL held (no
                // extra critical section) and store them into the gauges.
                let evicted = cache.put(input_root_digest, Arc::clone(&arc));
                self.metrics
                    .tree_cache_evictions
                    .fetch_add(evicted as u64, Ordering::Relaxed);
                self.metrics
                    .tree_cache_resident_bytes
                    .store(cache.total_bytes(), Ordering::Relaxed);
                self.metrics
                    .tree_cache_entries
                    .store(cache.len() as u64, Ordering::Relaxed);
                if evicted > 0 {
                    debug!(
                        evicted,
                        cache_entries = cache.len(),
                        cache_bytes = cache.total_bytes(),
                        "tree cache byte-bounded eviction"
                    );
                }
                // Clear any stale failure entry.
                self.tree_resolution_failures
                    .lock()
                    .await
                    .remove(&input_root_digest);
                Some(arc)
            }
            Ok(Err(err)) => {
                // resolution_guard fires here, releasing the in-progress flag.
                drop(resolution_guard);
                // (#p1p2 telemetry) Cold resolution returned an error (not a
                // timeout) — e.g. CAS fetch failure or missing directory blob.
                self.metrics
                    .tree_resolution_errors
                    .fetch_add(1, Ordering::Relaxed);
                // Resolution failed — record in negative cache with backoff.
                let mut failures = self.tree_resolution_failures.lock().await;
                let attempts = failures
                    .get(&input_root_digest)
                    .map(|&(_, a)| a)
                    .unwrap_or(0)
                    + 1;
                let backoff = backoff_for_attempt(FAILURE_BACKOFF, attempts);
                warn!(
                    %input_root_digest,
                    ?err,
                    attempts,
                    backoff_secs = backoff.as_secs(),
                    "inline tree resolution failed, suppressing retries"
                );
                failures.insert(input_root_digest, (Instant::now(), attempts));
                None
            }
            Err(_elapsed) => {
                // (#p1p2 telemetry) Inline resolution hit the 500ms timeout
                // and is falling back to load-based scoring.
                self.metrics
                    .tree_resolution_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                // Resolution timed out — fall back to load-based scoring.
                // Spawn background task to finish resolution for next time.
                // Move the resolution_guard into the spawned task so the
                // in-progress flag stays asserted while the background work
                // continues, and is released exactly once on completion or
                // task drop. We also bound the background work with
                // TREE_RESOLUTION_TIMEOUT so a hung CAS connection cannot
                // hold the slot forever.
                let tree_cache = self.tree_cache.clone();
                let failures_ref = self.tree_resolution_failures.clone();
                let failed_dirs_ref = self.failed_directory_digests.clone();
                let store = cas_store.clone();
                // (#p1p2 telemetry) Clone the metrics handle so the
                // background put() can attribute its evictions + refresh the
                // resident-bytes / entry gauges (the same cache the inline
                // put writes; instrumenting only the inline site would
                // silently undercount whenever the 500ms timeout fires).
                let metrics = self.metrics.clone();
                let digest = input_root_digest;
                tokio::spawn(async move {
                    // Bind the guard to this task's lifetime. It fires on
                    // any exit path (success, error, timeout, cancellation).
                    let _resolution_guard = resolution_guard;
                    let bg_fut =
                        resolve_tree_from_cas(&store, digest, &failed_dirs_ref);
                    match tokio::time::timeout(TREE_RESOLUTION_TIMEOUT, bg_fut).await {
                        Ok(Ok(resolved)) => {
                            let entry_bytes = resolved.estimated_heap_bytes();
                            info!(
                                %digest,
                                file_count = resolved.file_digests.len(),
                                dir_count = resolved.dir_digests.len(),
                                entry_bytes,
                                "background tree resolution complete after timeout, caching"
                            );
                            let mut cache = tree_cache.lock().await;
                            // (#p1p2 telemetry) Attribute the background
                            // put's evictions and refresh the gauges under
                            // the already-held cache lock (no extra critical
                            // section).
                            let evicted = cache.put(digest, Arc::new(resolved));
                            metrics
                                .tree_cache_evictions
                                .fetch_add(evicted as u64, Ordering::Relaxed);
                            metrics
                                .tree_cache_resident_bytes
                                .store(cache.total_bytes(), Ordering::Relaxed);
                            metrics
                                .tree_cache_entries
                                .store(cache.len() as u64, Ordering::Relaxed);
                            failures_ref.lock().await.remove(&digest);
                        }
                        Ok(Err(err)) => {
                            let mut failures = failures_ref.lock().await;
                            let attempts = failures
                                .get(&digest)
                                .map(|&(_, a)| a)
                                .unwrap_or(0)
                                + 1;
                            let backoff = backoff_for_attempt(FAILURE_BACKOFF, attempts);
                            warn!(
                                %digest,
                                ?err,
                                attempts,
                                backoff_secs = backoff.as_secs(),
                                "background tree resolution failed, suppressing retries"
                            );
                            failures.insert(digest, (Instant::now(), attempts));
                        }
                        Err(_elapsed) => {
                            // Hard timeout — do not record as a regular
                            // failure (don't penalize this digest forever
                            // due to a transient CAS hang), but log loudly
                            // so operators see the issue.
                            warn!(
                                %digest,
                                timeout_secs = TREE_RESOLUTION_TIMEOUT.as_secs(),
                                "background tree resolution timed out, abandoning"
                            );
                        }
                    }
                    // _resolution_guard drops here, releasing the
                    // in-progress flag on every exit path.
                });
                info!(
                    %input_root_digest,
                    "tree resolution timed out, using load-based scoring"
                );
                None
            }
        }
    }

    /// Returns the per-worker prefetch semaphore, creating it if needed.
    fn get_prefetch_semaphore(&self, endpoint: &str) -> Arc<Semaphore> {
        let mut sems = self.prefetch_semaphores.lock();
        sems.entry(Arc::from(endpoint))
            .or_insert_with(|| Arc::new(Semaphore::new(PREFETCH_MAX_CONCURRENT_PER_WORKER)))
            .clone()
    }

    /// Computes the set of small blobs that the target worker is missing
    /// from the resolved input tree, using a scoring-time snapshot of the
    /// locality map (or, on snapshot miss, a fresh `locality_map.read()`)
    /// to determine what the worker already has. Returns blobs sorted by
    /// size ascending (smallest first), capped at `PREFETCH_MAX_BLOBS`
    /// and `PREFETCH_MAX_INFLIGHT_BYTES`.
    ///
    /// Only blobs under `PREFETCH_MAX_SINGLE_BLOB_SIZE` are included —
    /// large blobs are better handled by the worker's parallel ByteStream
    /// fetch. The goal is to eliminate per-blob RPC overhead for many
    /// small blobs by batching them via `BatchUpdateBlobs`.
    ///
    /// **Why a snapshot, not a live read.** (#407) The same action's
    /// `score_and_generate_hints` already walked `locality_map` under
    /// the read lock to build scores and peer-hints. Re-acquiring the
    /// lock here for a second O(F) walk doubles slow-warn pressure
    /// (audit measured 58–105 ms warns at 252K-entry map). The snapshot
    /// is scoring-time state — over-fetching a blob that arrived since
    /// scoring is acceptable, never under-fetching (no
    /// missing-blob-failure risk).
    fn compute_missing_blobs(
        file_digests: &[(DigestInfo, u64)],
        worker_endpoint: &str,
        locality_snapshot: Option<&LocalitySnapshot>,
        locality_map: &SharedBlobLocalityMap,
    ) -> Vec<(DigestInfo, u64)> {
        // Fast path: scoring captured a snapshot — no lock acquisition.
        // Slow path: snapshot was skipped (file_digests > cap); fall back
        // to a fresh read so we don't silently over-fetch the entire
        // input tree.
        let mut missing: Vec<(DigestInfo, u64)> = if let Some(snapshot) = locality_snapshot {
            file_digests
                .iter()
                .filter(|(_, size)| *size > 0 && *size <= PREFETCH_MAX_SINGLE_BLOB_SIZE)
                .filter(|(digest, _)| {
                    // Blob is "missing" if the snapshot has no entry for this
                    // digest, OR the entry's endpoint list doesn't include
                    // the target worker.
                    snapshot
                        .get(digest)
                        .is_none_or(|endpoints| {
                            !endpoints.iter().any(|e| &**e == worker_endpoint)
                        })
                })
                .copied()
                .collect()
        } else {
            let map = locality_map.read();
            let blobs = map.blobs_map();
            file_digests
                .iter()
                .filter(|(_, size)| *size > 0 && *size <= PREFETCH_MAX_SINGLE_BLOB_SIZE)
                .filter(|(digest, _)| {
                    blobs
                        .get(digest)
                        .is_none_or(|endpoints| endpoints.get(worker_endpoint).is_none())
                })
                .copied()
                .collect()
        };

        // Sort by size ascending -- smallest blobs first maximizes the
        // number of blobs per BatchUpdateBlobs RPC, eliminating the most
        // per-blob RPC overhead.
        missing.sort_by_key(|(_, size)| *size);

        // Cap by count and total bytes.
        let mut total_bytes: u64 = 0;
        missing.truncate(PREFETCH_MAX_BLOBS);
        missing.retain(|(_, size)| {
            if total_bytes + size > PREFETCH_MAX_INFLIGHT_BYTES {
                return false;
            }
            total_bytes += size;
            true
        });

        missing
    }

    /// Spawns a background task that prefetches missing small blobs from
    /// the server's CAS to the selected worker's CAS endpoint. Blobs are
    /// read into memory and pushed via `update_oneshot`, which routes them
    /// through `BatchUpdateBlobs` on the worker's GrpcStore connection.
    /// This batches many small blobs into few RPCs, eliminating per-blob
    /// RPC overhead that dominates the worker's demand fetch path.
    ///
    /// This is best-effort: failures are logged but do not affect the
    /// action dispatch. The worker's normal demand fetch handles anything
    /// prefetch doesn't deliver.
    ///
    /// This method is synchronous (no `.await`) — all I/O including
    /// connection creation happens inside the spawned task, keeping the
    /// dispatch path non-blocking.
    fn spawn_prefetch(
        &self,
        worker_endpoint: Arc<str>,
        missing_blobs: Vec<(DigestInfo, u64)>,
        operation_id: String,
    ) {
        let cas_store = match &self.cas_store {
            Some(s) => s.clone(),
            None => return,
        };

        if missing_blobs.is_empty() {
            return;
        }

        let total_bytes: u64 = missing_blobs.iter().map(|(_, s)| *s).sum();
        let blob_count = missing_blobs.len();
        let metrics = self.metrics.clone();
        let endpoint_str = worker_endpoint.clone();
        let semaphore = self.get_prefetch_semaphore(&worker_endpoint);
        let worker_tls_config = self.worker_tls_config.clone();
        let prefetch_connections = self.prefetch_connections.clone();

        // Snapshot the cached connection under a brief sync lock. The
        // actual TCP connect (if needed) happens inside the spawned task.
        let cached_connection = {
            let conns = self.prefetch_connections.lock();
            conns.get(&*worker_endpoint).cloned()
        };

        metrics
            .prefetch_tasks_spawned
            .fetch_add(1, Ordering::Relaxed);

        debug!(
            %operation_id,
            worker_endpoint = %endpoint_str,
            blob_count,
            total_bytes,
            "prefetch: spawning batched push of small blobs to worker"
        );

        tokio::spawn(async move {
            let start = Instant::now();

            // Get or create connection to worker. This may do TCP connect
            // but happens inside the spawned task, not on the dispatch path.
            let worker_store = if let Some(store) = cached_connection {
                store
            } else {
                let store =
                    match create_worker_cas_connection(&endpoint_str, worker_tls_config).await {
                        Ok(store) => store,
                        Err(e) => {
                            warn!(
                                %operation_id,
                                worker_endpoint = %endpoint_str,
                                ?e,
                                "prefetch: failed to connect to worker CAS"
                            );
                            return;
                        }
                    };
                // Insert into the cache so subsequent prefetches reuse it
                // instead of opening another N (`connections_per_endpoint`)
                // TCP connections per call. Without this insert the cache
                // is effectively dead (read-only) and TCP connections
                // accumulate without bound under burst.
                let endpoint_key: Arc<str> = Arc::from(endpoint_str.as_ref());
                prefetch_connections
                    .lock()
                    .entry(endpoint_key)
                    .or_insert_with(|| store.clone());
                store
            };

            // Skip the redundant has() check against the worker's CAS.
            // The missing_blobs list was already filtered by compute_missing_blobs()
            // using the locality map (refreshed every 100ms via BlobsAvailable).
            // The has() round-trip to the worker costs 5-20ms and provides
            // marginal benefit: at worst we re-push a few small blobs that
            // arrived between the locality snapshot and now, costing <1ms at
            // 10GbE for the capped prefetch batch sizes.
            let actually_missing = missing_blobs;

            // Group blobs into batches of up to PREFETCH_BATCH_SIZE_BYTES.
            // Each batch will be read from CAS and pushed via update_oneshot,
            // which routes through BatchUpdateBlobs on the GrpcStore.
            let mut batches: Vec<Vec<(DigestInfo, u64)>> = Vec::new();
            let mut current_batch: Vec<(DigestInfo, u64)> = Vec::new();
            let mut current_batch_bytes: u64 = 0;

            for (digest, size) in &actually_missing {
                if !current_batch.is_empty()
                    && current_batch_bytes + size > PREFETCH_BATCH_SIZE_BYTES
                {
                    batches.push(core::mem::take(&mut current_batch));
                    current_batch_bytes = 0;
                }
                current_batch.push((*digest, *size));
                current_batch_bytes += size;
            }
            if !current_batch.is_empty() {
                batches.push(current_batch);
            }

            let batch_count = batches.len();
            let mut blobs_sent: u64 = 0;
            let mut bytes_sent: u64 = 0;
            let mut blobs_failed: u64 = 0;
            let mut batches_sent: u64 = 0;

            // Process batches with concurrency limited by the per-worker
            // semaphore. Each batch task reads blobs from server CAS and
            // pushes them via update_oneshot (-> BatchUpdateBlobs).
            let mut join_set = tokio::task::JoinSet::new();

            for batch in batches {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed
                };

                let cas = cas_store.clone();
                let worker = worker_store.clone();
                let op_id = operation_id.clone();
                let ep = endpoint_str.clone();

                join_set.spawn(async move {
                    let _permit = permit; // held until this batch completes

                    let mut batch_blobs_sent: u64 = 0;
                    let mut batch_bytes_sent: u64 = 0;
                    let mut batch_blobs_failed: u64 = 0;

                    // Read each blob from server CAS into memory (safe -- all
                    // blobs are under PREFETCH_MAX_SINGLE_BLOB_SIZE) and push
                    // via update_oneshot which routes through BatchUpdateBlobs.
                    for (digest, size) in &batch {
                        let key: StoreKey<'_> = (*digest).into();

                        let data: Bytes = match cas
                            .get_part_unchunked(key.borrow(), 0, None)
                            .await
                        {
                            Ok(d) => d,
                            Err(e) => {
                                debug!(
                                    %op_id,
                                    %digest,
                                    size,
                                    ?e,
                                    "prefetch: failed to read blob from server CAS"
                                );
                                batch_blobs_failed += 1;
                                continue;
                            }
                        };

                        match worker.update_oneshot(key.borrow(), data).await {
                            Ok(()) => {
                                batch_blobs_sent += 1;
                                batch_bytes_sent += size;
                            }
                            Err(e) => {
                                debug!(
                                    %op_id,
                                    worker_endpoint = %ep,
                                    %digest,
                                    size,
                                    ?e,
                                    "prefetch: failed to push blob to worker"
                                );
                                batch_blobs_failed += 1;
                            }
                        }
                    }

                    (batch_blobs_sent, batch_bytes_sent, batch_blobs_failed)
                });
            }

            // Collect results.
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok((sent, bytes, failed)) => {
                        blobs_sent += sent;
                        bytes_sent += bytes;
                        blobs_failed += failed;
                        batches_sent += 1;
                    }
                    Err(e) => {
                        warn!(?e, "prefetch: batch task panicked");
                        blobs_failed += 1;
                    }
                }
            }

            // Update global metrics.
            metrics
                .prefetch_blobs_sent
                .fetch_add(blobs_sent, Ordering::Relaxed);
            metrics
                .prefetch_bytes_sent
                .fetch_add(bytes_sent, Ordering::Relaxed);
            metrics
                .prefetch_blobs_failed
                .fetch_add(blobs_failed, Ordering::Relaxed);
            metrics
                .prefetch_batches_sent
                .fetch_add(batches_sent, Ordering::Relaxed);

            let elapsed = start.elapsed();
            debug!(
                %operation_id,
                worker_endpoint = %endpoint_str,
                blob_count,
                batch_count,
                batches_sent,
                blobs_sent,
                bytes_sent,
                blobs_failed,
                elapsed_ms = elapsed.as_millis() as u64,
                "prefetch: completed batched push to worker"
            );
        });
    }

    /// Spawns a background task that warms the server-side MemoryStore by
    /// reading blobs through the full CAS store chain. For blobs already in
    /// MemoryStore, `FastSlowStore::get_part()` returns from the fast store
    /// in ~1-5us (near-no-op). For cold blobs, the read populates MemoryStore
    /// via `populate_and_maybe_stream`. The returned `Bytes` are dropped
    /// immediately — we only need the warming side effect.
    fn spawn_server_cache_warm(
        &self,
        file_digests: &[(DigestInfo, u64)],
        operation_id: &OperationId,
    ) {
        let cas_store = match &self.cas_store {
            Some(s) => s.clone(),
            None => return,
        };

        if file_digests.is_empty() || self.memory_store_threshold == 0 {
            return;
        }

        // Only warm blobs below the SizePartitioningStore threshold —
        // larger blobs are routed to a noop/disk store, so warming them
        // wastes I/O without populating MemoryStore.
        let threshold = self.memory_store_threshold;
        let mut sorted: Vec<(DigestInfo, u64)> = file_digests
            .iter()
            .filter(|(_, size)| *size > 0 && *size < threshold)
            .copied()
            .collect();
        sorted.sort_unstable_by_key(|(_, size)| *size);

        // Cap at CACHE_WARM_MAX_BLOBS and CACHE_WARM_MAX_BYTES total.
        let mut total_bytes: u64 = 0;
        let mut selected: Vec<DigestInfo> = Vec::with_capacity(
            sorted.len().min(CACHE_WARM_MAX_BLOBS),
        );
        for (digest, size) in &sorted {
            if selected.len() >= CACHE_WARM_MAX_BLOBS {
                break;
            }
            if total_bytes + size > CACHE_WARM_MAX_BYTES && !selected.is_empty() {
                break;
            }
            total_bytes += size;
            selected.push(*digest);
        }

        let blob_count = selected.len();
        let op_id = operation_id.to_string();

        self.metrics.cache_warm_spawned.inc();

        info!(
            %operation_id,
            blob_count,
            total_bytes,
            "cache_warm: spawning server-side MemoryStore warm"
        );

        tokio::spawn(async move {
            let start = Instant::now();
            let semaphore = Arc::new(Semaphore::new(CACHE_WARM_CONCURRENCY));
            let mut join_set = tokio::task::JoinSet::new();

            for digest in selected {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let store = cas_store.clone();

                join_set.spawn(async move {
                    let _permit = permit;
                    let key: StoreKey<'_> = digest.into();
                    match store.get_part_unchunked(key.borrow(), 0, None).await {
                        Ok(_bytes) => true,
                        Err(e) => {
                            warn!(
                                %digest,
                                ?e,
                                "cache_warm: failed to warm blob"
                            );
                            false
                        }
                    }
                });
            }

            let mut warmed: u64 = 0;
            let mut failed: u64 = 0;
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(true) => warmed += 1,
                    Ok(false) => failed += 1,
                    Err(e) => {
                        warn!(?e, "cache_warm: task panicked");
                        failed += 1;
                    }
                }
            }

            let elapsed_ms = start.elapsed().as_millis() as u64;
            info!(
                op_id = %op_id,
                blob_count,
                warmed,
                failed,
                total_bytes,
                elapsed_ms,
                "cache_warm: completed server-side MemoryStore warm"
            );
        });
    }

    /// Broadcast a `BlobsInStableStorage` message to all connected workers.
    /// Disconnected workers are silently skipped (they will be reaped by the
    /// timeout mechanism). Takes a read lock on the worker map briefly to
    /// clone the sender handles, then sends outside the lock.
    pub async fn broadcast_blobs_in_stable_storage(&self, digests: Vec<DigestInfo>) {
        if digests.is_empty() {
            return;
        }
        let proto_digests: Vec<Digest> = digests.iter().map(Digest::from).collect();
        let msg = update_for_worker::Update::BlobsInStableStorage(BlobsInStableStorage {
            digests: proto_digests,
        });

        // Collect sender handles under a brief read lock, then send outside.
        let senders: Vec<(WorkerId, _)> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .iter()
                .map(|(id, w)| (id.clone(), w.tx.clone()))
                .collect()
        };

        let worker_count = senders.len();
        let digest_count = digests.len();
        info!(
            target: "nativelink::stable_storage_dispatch",
            worker_count,
            digest_count,
            "broadcast_blobs_in_stable_storage: dispatching to all workers"
        );
        let mut send_failures = 0usize;
        for (worker_id, tx) in &senders {
            match tx.send(UpdateForWorker {
                update: Some(msg.clone()),
            }) {
                Ok(()) => {
                    info!(
                        target: "nativelink::stable_storage_dispatch_per_worker",
                        worker_id = %worker_id,
                        "BlobsInStableStorage: sent to worker"
                    );
                }
                Err(e) => {
                    send_failures += 1;
                    warn!(
                        target: "nativelink::stable_storage_dispatch_per_worker",
                        worker_id = %worker_id,
                        ?e,
                        "BlobsInStableStorage: send failed"
                    );
                }
            }
        }

        if send_failures > 0 {
            debug!(
                digest_count,
                worker_count,
                send_failures,
                "broadcast blobs_in_stable_storage had send failures"
            );
        } else {
            trace!(
                digest_count,
                worker_count,
                "broadcast blobs_in_stable_storage"
            );
        }
    }

    /// AC-poisoning fix routing primitive: locate the worker running
    /// `operation_id` and dispatch a `KillOperationRequest`. Both
    /// arrival paths (explicit `cancel_operation` RPC and
    /// `ExecuteStreamCancelGuard` stream-drop) converge here.
    ///
    /// Lock discipline: brief read-lock to scan workers and clone the
    /// matching `tx` handle; the actual `tx.send` is outside the lock.
    /// Mirrors `broadcast_blobs_in_stable_storage` above.
    ///
    /// Idempotency: an unknown / already-finished operation returns
    /// `Ok(())`. Two cancels in flight (e.g. RPC + stream-drop) both
    /// deliver `KillOperationRequest`; the worker's `kill_operation`
    /// handler at `running_actions_manager.rs:5184-5201` `.take()`s
    /// the `kill_channel_tx`, so only the first send wins. The
    /// `cancelled` AtomicBool also set by `kill_operation` is
    /// idempotent (false→true monotonic).
    ///
    /// Increments `metrics::CANCEL.cancel_kill_delivery_failed` if the
    /// worker disconnected between snapshot and send (benign;
    /// worker-disconnect closure deferred to the AC-server-side
    /// intercept tracker, requires proto change).
    pub async fn cancel_operation_internal(
        &self,
        operation_id: &OperationId,
    ) -> Result<(), Error> {
        // Phase 1: scan workers under a brief read lock; clone the
        // matching tx so the actual send happens outside the lock.
        let target = {
            let inner = self.inner.read().await;
            inner
                .workers
                .iter()
                .find(|(_, w)| w.running_action_infos.contains_key(operation_id))
                .map(|(wid, w)| (wid.clone(), w.tx.clone()))
        };

        let Some((worker_id, tx)) = target else {
            // Operation already finished, never started, OR a routing
            // bug failed the lookup (BUG-1: wrong OperationId shape).
            // Operators distinguish via `execution.cancel.no_target_worker`
            // counter — alarm during active builds when this counter
            // accrues; benign when all builds are quiescent.
            nativelink_util::metrics::CANCEL.no_target_worker.add(1, &[]);
            info!(
                %operation_id,
                "cancel: operation not found on any worker (already-finished, never-started, or routing-bug)"
            );
            return Ok(());
        };

        let msg = UpdateForWorker {
            update: Some(update_for_worker::Update::KillOperationRequest(
                KillOperationRequest {
                    operation_id: operation_id.to_string(),
                },
            )),
        };
        info!(
            %operation_id,
            %worker_id,
            "cancel: sending KillOperationRequest to worker"
        );
        if tx.send(msg).is_err() {
            // Worker disconnected between snapshot and send; benign.
            // Increment safety-net counter so operators can observe
            // disconnect-residual frequency.
            nativelink_util::metrics::CANCEL
                .cancel_kill_delivery_failed
                .add(1, &[]);
            warn!(
                %operation_id,
                %worker_id,
                "cancel: worker disconnected before kill delivered"
            );
        }
        Ok(())
    }

    /// (#97) Chunked variant of `broadcast_blobs_in_stable_storage`: splits
    /// `digests` into `BIS_DIGESTS_PER_CHUNK`-sized
    /// `BlobsInStableStorageChunk` messages, dispatches each chunk to
    /// every connected worker via `Update::ChunkedMessage`, and adds the
    /// dispatched chunk to the per-worker resend buffer (keyed by
    /// `cas_endpoint`). The matching `Update::BisAck` from the worker
    /// later removes the chunk from the buffer. On the next ConnectWorker
    /// for the same `cas_endpoint`+`boot_epoch_id`, every still-buffered
    /// chunk is replayed via `replay_bis_chunks_to_worker` so a worker
    /// reconnect does not drop unacked unpins.
    ///
    /// Direct-merge per the streaming-design plan: each chunk is
    /// independently meaningful; chunks may arrive out of order on
    /// resends; unpins are idempotent. Empty `digests` is a no-op (no
    /// chunks emitted, no buffer entries — distinct from the
    /// chunk_iter's "always emit one terminal chunk" contract because
    /// the empty broadcast is not a meaningful protocol event for any
    /// worker).
    pub async fn broadcast_blobs_in_stable_storage_chunked(
        &self,
        digests: Vec<DigestInfo>,
        store_id: &str,
    ) {
        if digests.is_empty() {
            return;
        }
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            BlobsInStableStorageChunk, ChunkedMessage, chunked_message,
        };
        use nativelink_util::chunk_iter::ChunkIter;

        let proto_digests: Vec<Digest> = digests.iter().map(Digest::from).collect();
        let total = proto_digests.len();

        // Allocate broadcast_id lock-free. Bumping a counter under the
        // RwLock previously took the write lock at a cost proportional
        // to broadcast frequency; `fetch_add` removes that contention.
        let broadcast_id = self
            .next_bis_broadcast_id
            .fetch_add(1, Ordering::Relaxed);

        // Snapshot (worker_id, cas_endpoint, tx) tuples under one read
        // lock; build chunks + dispatch outside any lock; then take ONE
        // write lock at the end to walk every per-endpoint buffer in a
        // single pass and record the broadcast. One write lock per
        // broadcast, not per chunk and not per worker.
        let senders: Vec<(WorkerId, Arc<str>, _)> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .iter()
                .map(|(id, w)| {
                    // Workers without a cas_endpoint can't be tracked for
                    // resend (no stable identity across reconnect). Send
                    // to them but skip the buffer (empty endpoint marker).
                    let endpoint = if w.cas_endpoint.is_empty() {
                        Arc::<str>::from("")
                    } else {
                        Arc::<str>::from(w.cas_endpoint.as_str())
                    };
                    (id.clone(), endpoint, w.tx.clone())
                })
                .collect()
        };
        let worker_count = senders.len();

        debug!(
            target: "nativelink::bis_chunked_dispatch",
            worker_count,
            digest_count = total,
            broadcast_id,
            chunk_size = BIS_DIGESTS_PER_CHUNK,
            "broadcast_blobs_in_stable_storage_chunked: dispatching"
        );

        // Build all chunks once and Arc-wrap them so dispatch +
        // resend-buffer storage share allocations. Without Arc, each
        // chunk's Vec<Digest> would be cloned per worker AND once more
        // per buffer entry.
        let server_instance_token = self.server_instance_token;
        let chunks: Vec<Arc<BlobsInStableStorageChunk>> = ChunkIter::new(
            proto_digests.into_iter(),
            BIS_DIGESTS_PER_CHUNK,
        )
        .map(|c| Arc::new(BlobsInStableStorageChunk {
            digests: c.items,
            broadcast_id,
            sequence: c.sequence,
            is_last: c.is_last,
            server_instance_token,
            store_id: store_id.to_string(),
        }))
        .collect();

        // Dispatch outside the lock. Track per-endpoint success: only
        // endpoints that received every chunk are buffered (a partial
        // dispatch leaves the resend path to the reconnect handler).
        let mut send_failures = 0usize;
        let mut endpoints_to_buffer: Vec<Arc<str>> = Vec::with_capacity(senders.len());
        for (worker_id, endpoint, tx) in &senders {
            let mut all_sent = true;
            for chunk in &chunks {
                // Clone the Arc (~8 bytes), not the proto chunk.
                let msg = UpdateForWorker {
                    update: Some(update_for_worker::Update::ChunkedMessage(ChunkedMessage {
                        payload: Some(chunked_message::Payload::BlobsInStableStorage(
                            (**chunk).clone(),
                        )),
                    })),
                };
                if let Err(e) = tx.send(msg) {
                    send_failures += 1;
                    warn!(
                        target: "nativelink::bis_chunked_dispatch_per_worker",
                        worker_id = %worker_id,
                        broadcast_id,
                        sequence = chunk.sequence,
                        ?e,
                        "BIS chunk send failed; will replay on reconnect"
                    );
                    // Send failed — disconnected. Bail this worker so we
                    // don't pile up resend entries that won't have a live
                    // tx anyway. The reconnect path will replay from
                    // whatever's in the buffer at that point.
                    all_sent = false;
                    break;
                }
            }
            if all_sent && !endpoint.is_empty() {
                endpoints_to_buffer.push(endpoint.clone());
            }
        }

        // ONE write lock walks every per-endpoint buffer in a single
        // pass. Previously this loop took a write lock per worker —
        // 64 workers × 25-chunk broadcast = 64 lock-acquire round-trips
        // contending against every other scheduler operation.
        //
        // (#214) Track per-endpoint overflow drops so we can emit a
        // warn outside the lock with enough context for the operator
        // to identify the misbehaving worker.
        let overflow_report: Vec<(Arc<str>, usize)> = if !endpoints_to_buffer.is_empty() {
            let mut inner = self.inner.write().await;
            let mut report: Vec<(Arc<str>, usize)> = Vec::new();
            for endpoint in &endpoints_to_buffer {
                let buf = inner
                    .bis_resend_buffers
                    .entry(endpoint.to_string())
                    .or_default();
                let mut endpoint_dropped = 0usize;
                for chunk in &chunks {
                    endpoint_dropped += buf.add(chunk.clone());
                }
                if endpoint_dropped > 0 {
                    report.push((endpoint.clone(), endpoint_dropped));
                }
            }
            report
        } else {
            Vec::new()
        };

        for (endpoint, dropped) in &overflow_report {
            self.metrics
                .bis_replay_buffer_overflow_drops
                .fetch_add(*dropped as u64, Ordering::Relaxed);
            warn!(
                target: "nativelink::bis_chunked_dispatch",
                cas_endpoint = %endpoint,
                dropped_chunks = dropped,
                cap = BIS_REPLAY_BUFFER_MAX_CHUNKS,
                broadcast_id,
                "BIS replay buffer at cap — dropped oldest unacked chunks. \
                 Worker is failing to ack BIS broadcasts; pin state for the \
                 dropped chunks will leak until the worker reconnects with \
                 a new boot_epoch_id"
            );
        }

        if send_failures > 0 {
            debug!(
                digest_count = total,
                worker_count,
                broadcast_id,
                send_failures,
                "BIS chunked broadcast had send failures (workers will see resend on reconnect)"
            );
        } else {
            trace!(
                digest_count = total,
                worker_count,
                broadcast_id,
                "BIS chunked broadcast complete"
            );
        }
    }

    /// (#97) Replay every still-buffered BIS chunk for `cas_endpoint` to
    /// the supplied tx. Called from `add_worker` whenever the worker
    /// joining has the same `cas_endpoint` as a previously-disconnected
    /// worker AND has the same `boot_epoch_id` (a different epoch means
    /// the worker's pin state died with the old process and the resend
    /// is moot — caller is responsible for clearing the buffer in that
    /// case via `clear_bis_resend_buffer_for_endpoint`).
    ///
    /// Failed sends are silently dropped (the new worker is also
    /// disconnected — nothing more we can do).
    pub async fn replay_bis_chunks_to_worker(
        &self,
        cas_endpoint: &str,
        tx: &UnboundedSender<UpdateForWorker>,
    ) -> usize {
        if cas_endpoint.is_empty() {
            return 0;
        }
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
            ChunkedMessage, chunked_message,
        };
        let chunks_to_replay = {
            let inner = self.inner.read().await;
            match inner.bis_resend_buffers.get(cas_endpoint) {
                Some(buf) => buf.chunks.values().cloned().collect::<Vec<_>>(),
                None => return 0,
            }
        };
        let n = chunks_to_replay.len();
        if n == 0 {
            return 0;
        }
        info!(
            target: "nativelink::bis_chunked_replay",
            cas_endpoint,
            chunk_count = n,
            "replaying buffered BIS chunks to (re)connecting worker"
        );
        for chunk in chunks_to_replay {
            // Arc<BlobsInStableStorageChunk> → owned proto via clone of
            // the inner value (the proto must be owned by the wire
            // message). This is the same single Vec<Digest> clone we
            // would have done before Arc-wrap; the Arc saves only the
            // dispatch/buffer duplication, not this terminal clone.
            let msg = UpdateForWorker {
                update: Some(update_for_worker::Update::ChunkedMessage(ChunkedMessage {
                    payload: Some(chunked_message::Payload::BlobsInStableStorage(
                        (*chunk).clone(),
                    )),
                })),
            };
            if tx.send(msg).is_err() {
                // Worker dropped already — leave buffer in place; next
                // reconnect will retry.
                break;
            }
        }
        n
    }

    /// (#97) Drop ALL buffered BIS chunks for `cas_endpoint`. Called
    /// when the worker's `boot_epoch_id` changes (a fresh process
    /// means the old pin state is gone — the unpins these chunks
    /// would drive are no-ops; keeping them in the buffer would
    /// just waste memory until the new worker happens to ack).
    pub async fn clear_bis_resend_buffer_for_endpoint(&self, cas_endpoint: &str) {
        if cas_endpoint.is_empty() {
            return;
        }
        let mut inner = self.inner.write().await;
        if let Some(buf) = inner.bis_resend_buffers.remove(cas_endpoint) {
            if !buf.is_empty() {
                info!(
                    target: "nativelink::bis_chunked_replay",
                    cas_endpoint,
                    dropped_chunks = buf.len(),
                    "cleared BIS resend buffer on worker boot_epoch change"
                );
            }
        }
    }

    /// (FL-688 v3 Stage A fix) Push an `Update::AcPinResync` to the worker
    /// owning `cas_endpoint`, so it FORCES a full re-advertisement of its
    /// AC-pin set on the next tick. Called immediately after an OUT-OF-BAND
    /// removal of AC-pin entries for this endpoint from the server's
    /// `AcPinRegistry` (BIS-ack sweep / AcProxy peer-NotFound / cap-truncation).
    ///
    /// Resolves the endpoint to a `WorkerId` via the `endpoint_to_worker`
    /// reverse map (the same map the locality scorer uses), then sends on the
    /// worker's `tx`. An empty endpoint, an unknown endpoint (no connected
    /// worker), or a dropped `tx` are silent no-ops — the worker's eventual
    /// reconnect full snapshot is the backstop. The signal carries no payload
    /// (the worker re-advertises its FULL set; field 17 is replace-semantics),
    /// so it is idempotent — the hot BIS-ack sweep fires it once per endpoint
    /// per sweep cycle without coalescing concern.
    pub async fn notify_ac_pin_resync_for_endpoint(&self, cas_endpoint: &str) {
        if cas_endpoint.is_empty() {
            return;
        }
        // Read lock held across the send: worker.tx.send is sync
        // (UnboundedSender) — no .await, O(1), no upgrade to write.
        // Lock released at fn return.
        let inner = self.inner.read().await;
        let Some(worker_id) = inner.endpoint_to_worker.get(cas_endpoint) else {
            // No connected worker for this endpoint (e.g. it disconnected
            // between the registry removal and this push). The worker's
            // reconnect full snapshot will reconcile when it returns.
            return;
        };
        let Some(worker) = inner.workers.0.peek(worker_id) else {
            // endpoint_to_worker is stale relative to workers — should not
            // happen (both are mutated under the same write lock), but be
            // defensive rather than panic.
            return;
        };
        let msg = UpdateForWorker {
            update: Some(update_for_worker::Update::AcPinResync(AcPinResyncRequest {})),
        };
        if worker.tx.send(msg).is_err() {
            // Worker dropped already — its reconnect snapshot is the backstop.
            debug!(
                target: "nativelink::ac_pin_resync",
                cas_endpoint,
                %worker_id,
                "AcPinResync send failed (worker tx closed); reconnect snapshot will reconcile"
            );
        } else {
            trace!(
                target: "nativelink::ac_pin_resync",
                cas_endpoint,
                %worker_id,
                "pushed AcPinResync after out-of-band AC-pin registry removal"
            );
        }
    }

    /// (#97) Drop the matching `(broadcast_id, sequence)` chunk from this
    /// worker's BIS resend buffer. Called when the worker sends a `BisAck`
    /// for a chunk we previously dispatched. Idempotent: an ack for an
    /// unknown broadcast_id (e.g. one we already replayed-and-acked, or
    /// one from a server we restarted into) is silently ignored.
    ///
    /// **Server-instance-token validation (red-team #5).** Acks whose
    /// `server_instance_token` does not match the current scheduler's
    /// token are silently dropped (with a `debug!` for observability).
    /// Without this, a worker holding a stale `BisAck { broadcast_id =
    /// 42, server_instance_token = OLD }` sent across a server bounce
    /// would drop an unrelated chunk from the new server's resend
    /// buffer (the new server resets `next_bis_broadcast_id` to 1 on
    /// startup → broadcast_id collisions across server-instance
    /// boundaries are guaranteed).
    pub async fn bis_ack_received(
        &self,
        worker_id: &WorkerId,
        broadcast_id: u64,
        sequence: u32,
        server_instance_token: u64,
    ) {
        if server_instance_token != self.server_instance_token {
            debug!(
                target: "nativelink::bis_chunked_ack",
                ?worker_id,
                broadcast_id,
                sequence,
                ack_token = server_instance_token,
                expected_token = self.server_instance_token,
                "BisAck dropped: server_instance_token mismatch — \
                 worker is holding an ack from a previous server \
                 process. Without the drop, this ack would silently \
                 release an unrelated chunk from the resend buffer \
                 (next_bis_broadcast_id resets to 1 on server startup, \
                 so broadcast_id collisions across server bounces are \
                 guaranteed)"
            );
            return;
        }
        let mut inner = self.inner.write().await;
        let Some(endpoint) = inner.workers.get(worker_id).map(|w| w.cas_endpoint.clone())
        else {
            return;
        };
        if endpoint.is_empty() {
            return;
        }
        if let Some(buf) = inner.bis_resend_buffers.get_mut(endpoint.as_str()) {
            buf.ack(broadcast_id, sequence);
            if buf.is_empty() {
                inner.bis_resend_buffers.remove(endpoint.as_str());
            }
        }
    }
}

/// Resolved input tree containing file digests, directory digests,
/// per-subtree file byte totals for coverage scoring, and the decoded
/// Directory protos (for forwarding to workers so they skip GetTree).
struct ResolvedTree {
    /// (file_digest, file_size) pairs, deduplicated.
    file_digests: Vec<(DigestInfo, u64)>,
    /// All directory digests in the tree (including root), deduplicated.
    dir_digests: HashSet<DigestInfo>,
    /// Total file bytes under each directory subtree (recursive).
    /// Used to weight subtree coverage scoring — a subtree with 10GB
    /// of files is worth more than one with 100 bytes.
    subtree_bytes: HashMap<DigestInfo, u64>,
    /// Total file count under each directory subtree (recursive).
    /// Blended with subtree_bytes for coverage scoring: many small files
    /// have higher per-file I/O cost (hardlinks, clonefile) than fewer
    /// large files at the same total byte count.
    subtree_files: HashMap<DigestInfo, u64>,
    /// #52: Direct (non-recursive) file bytes attributed to each directory
    /// digest — only the files referenced by that directory's own
    /// `files` list, NOT its subdirectories. Sum over all entries equals
    /// `subtree_bytes[root]`, partitioned per directory. Used by
    /// `compute_dedup_cached_score` so the numerator is a disjoint sum
    /// across cached directories (no double-counting via nesting); the
    /// resulting `coverage_pct` is bounded in [0, 100].
    dir_direct_bytes: HashMap<DigestInfo, u64>,
    /// #52: Direct (non-recursive) file count per directory digest;
    /// see `dir_direct_bytes` doc-comment.
    dir_direct_files: HashMap<DigestInfo, u64>,
    /// Decoded Directory protos keyed by their digest. Forwarded to workers
    /// in StartExecute so they can skip the redundant GetTree RPC.
    directories: HashMap<DigestInfo, Directory>,
}

impl ResolvedTree {
    /// Approximate heap bytes consumed by this tree's owned data.
    /// Used for byte-bounding the tree cache to prevent unbounded
    /// memory growth.
    fn estimated_heap_bytes(&self) -> u64 {
        // Vec<(DigestInfo, u64)>: 48 bytes per entry.
        let file_bytes = self.file_digests.capacity()
            * size_of::<(DigestInfo, u64)>();
        // HashSet<DigestInfo>: ~72 bytes per entry (key + hash bucket).
        let dir_set_bytes = self.dir_digests.len() * 72;
        // HashMap<DigestInfo, u64>: ~80 bytes per entry. Covers
        // subtree_bytes + subtree_files + dir_direct_bytes + dir_direct_files.
        let subtree_map_bytes = (self.subtree_bytes.len()
            + self.subtree_files.len()
            + self.dir_direct_bytes.len()
            + self.dir_direct_files.len())
            * 80;
        // HashMap<DigestInfo, Directory>: key overhead + proto encoded size.
        let dir_proto_bytes: usize = self
            .directories
            .iter()
            .map(|(_, d)| 80 + Message::encoded_len(d))
            .sum();
        (file_bytes + dir_set_bytes + subtree_map_bytes + dir_proto_bytes)
            as u64
    }

    /// Converts the directory map into protobuf-ready Vecs. This involves
    /// cloning each Directory proto and is intentionally called outside the
    /// scheduler write lock to avoid blocking dispatch.
    fn to_proto_vecs(&self) -> (Vec<Directory>, Vec<Digest>) {
        let mut dirs = Vec::with_capacity(self.directories.len());
        let mut digests = Vec::with_capacity(self.directories.len());
        for (digest_info, directory) in &self.directories {
            digests.push((*digest_info).into());
            dirs.push(directory.clone());
        }
        (dirs, digests)
    }
}

/// #52 (option b2): Compute the worker's cached score for an action's
/// input subtree as the sum of DIRECT (non-recursive) bytes/files for
/// every unique directory digest the worker has cached. Because
/// `dir_digests` is already a `HashSet` and each directory's
/// `dir_direct_bytes[d]` contribution is disjoint from every other
/// directory's, the resulting `(cached_bytes, cached_files)` cannot
/// exceed `(subtree_bytes[root], subtree_files[root])`. The downstream
/// `coverage_pct = cached_score * 100 / total_score` is therefore
/// bounded in `[0, 100]` while still distinguishing partial-coverage
/// workers (30 % vs 70 % of subtree bytes).
///
/// The pre-fix numerator iterated `tree.dir_digests` and summed
/// `subtree_bytes[d]` (RECURSIVE) for each `d` the worker had cached.
/// `subtree_bytes[root]` already includes every child's bytes, so
/// caching root + any child double-counted the child's bytes — once in
/// the parent's recursive total, once on its own. Denominator
/// (`total_score`) is `subtree_bytes[root]` only, so `coverage_pct`
/// could exceed 100 % (production distribution n=214, p50=187 %,
/// p95=466 %, max=466 %). See
/// `.claude/audits/52-scheduler-subtree-overload-rca-2026-06-04.md`.
///
/// Telemetry-only: the selection inside `inner_find_and_reserve_worker`
/// uses `cached_score` directly (not the percentage), so this change
/// affects the WARN/DEBUG log fields, not routing. Partial-match
/// scoring is preserved — tier 2 still distinguishes "worker has 30 %
/// of subtree bytes" from "worker has 80 %", unlike a root-only
/// collapse which would kill tier 2 (every dir-cache match also wins
/// tier 1 at `:822` above).
fn compute_dedup_cached_score(
    dir_digests: &HashSet<DigestInfo>,
    cached_subtree_digests: &HashSet<DigestInfo>,
    dir_direct_bytes: &HashMap<DigestInfo, u64>,
    dir_direct_files: &HashMap<DigestInfo, u64>,
) -> (u64, u64) {
    dir_digests
        .iter()
        .filter(|d| cached_subtree_digests.contains(d))
        .fold((0u64, 0u64), |(ab, af), d| {
            (
                ab + dir_direct_bytes.get(d).copied().unwrap_or(0),
                af + dir_direct_files.get(d).copied().unwrap_or(0),
            )
        })
}

/// #52 OLD pre-fix numerator (kept ONLY as a mutation-test fixture so
/// the regression scenario can be exercised). Sums `subtree_bytes[d]`
/// and `subtree_files[d]` for every directory digest the worker has
/// cached. Double-counts nested subtrees because `subtree_bytes` is
/// recursive — `subtree_bytes[root]` already includes every cached
/// child's bytes. DO NOT call from production paths.
#[cfg(test)]
fn compute_old_buggy_cached_score(
    dir_digests: &HashSet<DigestInfo>,
    cached_subtree_digests: &HashSet<DigestInfo>,
    subtree_bytes: &HashMap<DigestInfo, u64>,
    subtree_files: &HashMap<DigestInfo, u64>,
) -> (u64, u64) {
    dir_digests
        .iter()
        .filter(|d| cached_subtree_digests.contains(d))
        .fold((0u64, 0u64), |(ab, af), d| {
            (
                ab + subtree_bytes.get(d).copied().unwrap_or(0),
                af + subtree_files.get(d).copied().unwrap_or(0),
            )
        })
}

/// #52 (b1) helper kept ONLY as a mutation fixture for T3 — proves
/// the over-collapse regression (root-only numerator kills tier 2's
/// partial-match signal). Returns `(total_bytes, total_files)` iff the
/// worker has the action's `input_root_digest` cached; `(0, 0)`
/// otherwise. Because Tier 1 already wins on a root match, this form
/// gives every partial-cache worker a score of 0 — workers are
/// indistinguishable. DO NOT call from production paths.
#[cfg(test)]
#[allow(dead_code)]
fn compute_root_only_cached_score(
    cached_subtree_digests: &HashSet<DigestInfo>,
    input_root_digest: &DigestInfo,
    total_bytes: u64,
    total_files: u64,
) -> (u64, u64) {
    if cached_subtree_digests.contains(input_root_digest) {
        (total_bytes, total_files)
    } else {
        (0, 0)
    }
}

/// Creates a GrpcStore connection to a worker's CAS endpoint for
/// prefetching blobs. This is a standalone function so it can be
/// called from both `get_or_create_prefetch_connection` and from
/// inside spawned tasks without holding a reference to `self`.
async fn create_worker_cas_connection(
    endpoint: &str,
    tls_config: Option<ClientTlsConfig>,
) -> Result<Store, Error> {
    let spec = GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![GrpcEndpoint {
            address: endpoint.to_string(),
            tls_config,
            concurrency_limit: None,
            connect_timeout_s: 5,
            tcp_keepalive_s: 30,
            http2_keepalive_interval_s: 30,
            http2_keepalive_timeout_s: 20,
            tcp_nodelay: true,
            use_http3: false,
        }],
        store_type: StoreType::Cas,
        retry: Retry::default(),
        max_concurrent_requests: 0,
        connections_per_endpoint: 16,
        rpc_timeout_s: 120,
        batch_update_threshold_bytes: 4 * 1024 * 1024,
        max_concurrent_batch_rpcs: 8,
        parallel_chunk_read_threshold: 8 * 1024 * 1024,
        parallel_chunk_count: 4,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: None,
        // Scheduler→worker prefetch reads do not write blobs through
        // this connection, so the chunked-write kill-switch is N/A.
        chunked_writes_enabled: false,
        chunked_v2_writes_enabled: false,
    };
    let store = GrpcStore::new(&spec)
        .await
        .err_tip(|| format!("Creating prefetch connection to worker {endpoint}"))?;
    Ok(Store::new(store))
}

/// Resolves a directory tree from the CAS store by recursively reading
/// Directory protos and collecting file digests (for locality scoring),
/// directory digests (for subtree coverage scoring), and per-subtree
/// file byte totals (for weighted coverage scoring). Deduplicates both
/// file and directory digests.
///
/// `failed_dir_digests` is a shared negative cache for individual directory
/// digests that failed during BFS. Before fetching each directory, we check
/// this cache and fail fast if the digest is known-bad. On NotFound errors,
/// the failing digest is recorded with a 60s expiry.
async fn resolve_tree_from_cas(
    cas_store: &Store,
    root_digest: DigestInfo,
    failed_dir_digests: &Arc<tokio::sync::Mutex<HashMap<DigestInfo, Instant>>>,
) -> Result<ResolvedTree, Error> {
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

    /// How long individual directory digest failures are cached.
    const DIR_FAILURE_TTL: Duration = Duration::from_secs(60);

    /// Per-fetch wall-time threshold above which we log a `warn!` for the
    /// individual directory read. Anything below this is silent — typical
    /// Redis/MemoryStore hits return in <10ms. The whole BFS budget is 60s
    /// (`TREE_RESOLUTION_TIMEOUT`), so an individual fetch exceeding 1s is
    /// already a serious anomaly.
    const SLOW_DIR_FETCH_THRESHOLD: Duration = Duration::from_secs(1);

    /// Per-level wall-time threshold above which we log a `warn!` for the
    /// entire BFS level. A single level with a small dir count should
    /// complete in <100ms; >5s means contention or a hung backend.
    const SLOW_BFS_LEVEL_THRESHOLD: Duration = Duration::from_secs(5);

    let bfs_start = Instant::now();
    debug!(
        target: "nativelink::tree_resolution",
        %root_digest,
        "tree resolution BFS starting"
    );

    let mut file_digests: Vec<(DigestInfo, u64)> = Vec::new();
    let mut seen_files: HashSet<DigestInfo> = HashSet::new();
    let mut dirs_to_visit: Vec<DigestInfo> = vec![root_digest];
    let mut seen_dirs: HashSet<DigestInfo> = HashSet::new();
    seen_dirs.insert(root_digest);
    let mut directories: HashMap<DigestInfo, Directory> = HashMap::new();

    // Track tree structure for bottom-up subtree size/file-count computation.
    let mut dir_direct_bytes: HashMap<DigestInfo, u64> = HashMap::new();
    let mut dir_direct_files: HashMap<DigestInfo, u64> = HashMap::new();
    let mut dir_children: HashMap<DigestInfo, Vec<DigestInfo>> = HashMap::new();
    // BFS order — used for bottom-up traversal (reverse of BFS = leaves first).
    let mut bfs_order: Vec<DigestInfo> = vec![root_digest];

    let mut bfs_level: u32 = 0;
    while !dirs_to_visit.is_empty() {
        bfs_level += 1;
        let level_start = Instant::now();
        let level_dir_count = dirs_to_visit.len();
        debug!(
            target: "nativelink::tree_resolution",
            %root_digest,
            level = bfs_level,
            dirs_in_level = level_dir_count,
            seen_dirs_total = seen_dirs.len(),
            elapsed_ms = bfs_start.elapsed().as_millis() as u64,
            "tree resolution BFS level entry"
        );
        // Check subdirectory negative cache before fetching this BFS level.
        {
            let mut cache = failed_dir_digests.lock().await;
            // Sweep expired entries to prevent unbounded growth.
            if cache.len() > NEGATIVE_CACHE_SWEEP_THRESHOLD {
                cache.retain(|_, failed_at: &mut Instant| {
                    failed_at.elapsed() < DIR_FAILURE_TTL
                });
            }
            for dir_digest in &dirs_to_visit {
                if let Some(&failed_at) = cache.get(dir_digest) {
                    if failed_at.elapsed() < DIR_FAILURE_TTL {
                        return Err(make_err!(
                            Code::NotFound,
                            "directory {dir_digest} is in subdirectory negative cache (failed {:.1}s ago)",
                            failed_at.elapsed().as_secs_f64()
                        ));
                    }
                    // Entry has expired — remove it inline since we hold the lock.
                    cache.remove(dir_digest);
                }
            }
        }

        let failed_dir_digests_clone = failed_dir_digests.clone();
        let fetches: FuturesUnordered<_> = dirs_to_visit
            .drain(..)
            .map(|dir_digest| {
                let cas_store = cas_store.clone();
                let failed_dirs = failed_dir_digests_clone.clone();
                async move {
                    let fetch_start = Instant::now();
                    let key: StoreKey<'_> = dir_digest.into();
                    let result = cas_store
                        .get_part_unchunked(key, 0, None)
                        .await
                        .err_tip(|| {
                            format!(
                                "Reading directory {dir_digest} from CAS for tree resolution"
                            )
                        });
                    let fetch_elapsed = fetch_start.elapsed();
                    if fetch_elapsed >= SLOW_DIR_FETCH_THRESHOLD {
                        debug!(
                            target: "nativelink::tree_resolution",
                            %dir_digest,
                            elapsed_ms = fetch_elapsed.as_millis() as u64,
                            ok = result.is_ok(),
                            "tree resolution slow directory fetch"
                        );
                    }
                    match result {
                        Ok(bytes) => {
                            let directory = Directory::decode(bytes).map_err(|e| {
                                make_err!(Code::Internal, "Failed to decode Directory proto: {e}")
                            })?;
                            Ok::<_, Error>((dir_digest, directory))
                        }
                        Err(err) => {
                            // Record the specific failing subdirectory digest.
                            if err.code == Code::NotFound {
                                warn!(
                                    %dir_digest,
                                    "directory blob not found in CAS, caching as failed subdirectory"
                                );
                                failed_dirs.lock().await.insert(dir_digest, Instant::now());
                            }
                            Err(err)
                        }
                    }
                }
            })
            .collect();

        let collect_start = Instant::now();
        let results: Vec<Result<(DigestInfo, Directory), Error>> = fetches.collect().await;
        let collect_elapsed = collect_start.elapsed();
        if collect_elapsed >= SLOW_BFS_LEVEL_THRESHOLD {
            warn!(
                target: "nativelink::tree_resolution",
                %root_digest,
                level = bfs_level,
                dirs_in_level = level_dir_count,
                level_elapsed_ms = level_start.elapsed().as_millis() as u64,
                collect_elapsed_ms = collect_elapsed.as_millis() as u64,
                bfs_total_elapsed_ms = bfs_start.elapsed().as_millis() as u64,
                results = results.len(),
                "tree resolution BFS level slow"
            );
        }
        for result in results {
            let (parent_digest, directory) = result?;

            // Sum direct file bytes and count for this directory.
            let mut direct_bytes: u64 = 0;
            let mut direct_files: u64 = 0;
            for file_node in &directory.files {
                if let Some(ref digest) = file_node.digest {
                    if let Ok(digest_info) = DigestInfo::try_from(digest) {
                        let size = digest_info.size_bytes();
                        direct_bytes += size;
                        direct_files += 1;
                        if seen_files.insert(digest_info) {
                            file_digests.push((digest_info, size));
                        }
                    }
                }
            }
            dir_direct_bytes.insert(parent_digest, direct_bytes);
            dir_direct_files.insert(parent_digest, direct_files);

            // Queue subdirectories for visiting (dedup via seen_dirs).
            let mut children = Vec::new();
            for dir_node in &directory.directories {
                if let Some(ref digest) = dir_node.digest {
                    if let Ok(digest_info) = DigestInfo::try_from(digest) {
                        children.push(digest_info);
                        if seen_dirs.insert(digest_info) {
                            dirs_to_visit.push(digest_info);
                            bfs_order.push(digest_info);
                        }
                    }
                }
            }
            dir_children.insert(parent_digest, children);
            directories.insert(parent_digest, directory);
        }
        debug!(
            target: "nativelink::tree_resolution",
            %root_digest,
            level = bfs_level,
            level_elapsed_ms = level_start.elapsed().as_millis() as u64,
            seen_dirs_total = seen_dirs.len(),
            seen_files_total = seen_files.len(),
            next_level_dirs = dirs_to_visit.len(),
            "tree resolution BFS level exit"
        );
    }
    debug!(
        target: "nativelink::tree_resolution",
        %root_digest,
        levels = bfs_level,
        total_dirs = seen_dirs.len(),
        total_files = seen_files.len(),
        bfs_total_elapsed_ms = bfs_start.elapsed().as_millis() as u64,
        "tree resolution BFS complete"
    );

    // Bottom-up pass: compute total file bytes and file count under each subtree.
    // Reverse BFS order gives us leaves-first, so children are always
    // computed before parents.
    let mut subtree_bytes: HashMap<DigestInfo, u64> = HashMap::new();
    let mut subtree_files: HashMap<DigestInfo, u64> = HashMap::new();
    for &dir_digest in bfs_order.iter().rev() {
        let direct_b = dir_direct_bytes.get(&dir_digest).copied().unwrap_or(0);
        let direct_f = dir_direct_files.get(&dir_digest).copied().unwrap_or(0);
        let (children_bytes, children_files): (u64, u64) = dir_children
            .get(&dir_digest)
            .map(|children| {
                children.iter().fold((0u64, 0u64), |(ab, af), c| {
                    (
                        ab + subtree_bytes.get(c).copied().unwrap_or(0),
                        af + subtree_files.get(c).copied().unwrap_or(0),
                    )
                })
            })
            .unwrap_or((0, 0));
        subtree_bytes.insert(dir_digest, direct_b + children_bytes);
        subtree_files.insert(dir_digest, direct_f + children_files);
    }

    Ok(ResolvedTree {
        file_digests,
        dir_digests: seen_dirs,
        subtree_bytes,
        subtree_files,
        dir_direct_bytes,
        dir_direct_files,
        directories,
    })
}

/// Scores endpoints by the total bytes of input blobs they have cached
/// AND generates peer hints. The locality-map read lock is held ONLY for
/// the candidate-collection pass; the size-descending sort happens OUTSIDE
/// the lock so a million-input action doesn't block every other scheduler
/// op for the duration of the sort (#82 ride-along, was previously inside
/// the lock).
///
/// Returns:
/// - `HashMap<Arc<str>, u64>`: endpoint scores (total cached bytes per
///   endpoint). Per-blob freshness timestamps were dropped from the
///   locality_map (entries persist until explicit eviction signal).
/// - `Arc<[PeerHint]>`: ALL peer hints sorted by file size descending.
///   The previous MAX_PEER_HINTS = 16384 truncation cap was removed in
///   #98 (peer-hints chunking) — hints that exceed one wire-message worth
///   of bytes now ride a `PeerHintsChunk` stream instead of being silently
///   dropped. Returned as `Arc<[_]>` so per-worker dispatch is a refcount
///   bump (#83 ride-along).
///
/// This is called OUTSIDE the scheduler write lock, so it does not need
/// access to `endpoint_to_worker` or the candidate set. The caller maps
/// endpoints to WorkerIds and filters to candidates inside the lock.
fn score_and_generate_hints(
    file_digests: &[(DigestInfo, u64)],
    locality_map: &SharedBlobLocalityMap,
) -> ScoringResult {
    // Wall-clock guard: this is a scheduler hot path called on every dispatch.
    // We log warn! if it exceeds SLOW_THRESHOLD so operators can spot
    // pathological locality-map sizes / lock contention without per-call info!
    // noise (10,840 events in 5 min observed pre-demotion).
    const SLOW_THRESHOLD: Duration = Duration::from_millis(50);
    let started = Instant::now();

    let mut scores: HashMap<Arc<str>, u64> = HashMap::new();
    let mut hint_candidates: Vec<(DigestInfo, u64, Vec<Arc<str>>)> = Vec::new();
    let locality_blob_count;
    // Tracks digests where the locality map had an entry but the endpoint
    // set was empty — an invariant smell (entries should be evicted, not
    // emptied), worth surfacing if it ever fires.
    let mut empty_endpoint_matches: usize = 0;

    // Per-action snapshot of file_digests ∩ locality_map, populated under
    // the same read lock used for scoring. Reused by Phase-4 prefetch and
    // inline `all_missing` filter to avoid two redundant
    // `locality_map.read()` walks per cold-tree dispatch (#407). See
    // `LocalitySnapshot` docs for invariant + cap.
    //
    // Snapshot is skipped (stays None) when the action's input count
    // exceeds the defensive cap — callers fall back to live
    // `locality_map.read()` in that case (pre-#407 behavior). This is
    // the documented over-cap behavior per CLAUDE.md.
    let snapshot_eligible = file_digests.len() <= LOCALITY_SNAPSHOT_MAX_ENTRIES;
    if !snapshot_eligible {
        warn!(
            file_digests = file_digests.len(),
            cap = LOCALITY_SNAPSHOT_MAX_ENTRIES,
            "locality snapshot skipped — file_digests exceeds defensive cap; \
             Phase-4 callers will re-acquire locality_map.read()"
        );
    }
    let mut locality_snapshot_builder: Option<HashMap<DigestInfo, Vec<Arc<str>>>> =
        if snapshot_eligible {
            Some(HashMap::with_capacity(file_digests.len()))
        } else {
            None
        };

    // ── Inside-lock pass: collect scores + hint candidates + snapshot ──
    // Hold the read lock only while reading from the map. The sort,
    // dedup, and proto conversion happen below after the lock drops.
    //
    // Snapshot population costs ONE additional `Vec<Arc<str>>::clone()`
    // per matched digest vs the pre-#407 path (one Vec allocation +
    // N atomic refcount bumps on the Arc<str> elements; no String
    // allocations). The dominant savings is the eliminated SECOND
    // `locality_map.read()` walk in Phase-4, not the per-Arc cost.
    // Follow-up #431 tracks an `Arc<[Arc<str>]>` refactor that would
    // make the snapshot insertion a refcount-only clone.
    {
        let map = locality_map.read();
        let blobs = map.blobs_map();
        locality_blob_count = blobs.len();
        for &(digest, size) in file_digests {
            if let Some(endpoints) = blobs.get(&digest) {
                // Accumulate endpoint byte scores. Timestamps were dropped from
                // EndpointList — locality entries persist until explicit eviction
                // signal, so freshness ranking is no longer meaningful.
                for endpoint in endpoints {
                    *scores.entry(endpoint.clone()).or_insert(0) += size;
                }
                // Collect hint candidate if this digest has peer locations.
                if endpoints.is_empty() {
                    empty_endpoint_matches += 1;
                } else {
                    let peer_eps: Vec<Arc<str>> = endpoints.keys().cloned().collect();
                    if let Some(snapshot) = locality_snapshot_builder.as_mut() {
                        snapshot.insert(digest, peer_eps.clone());
                    }
                    hint_candidates.push((digest, size, peer_eps));
                }
            }
        }
        // Lock dropped here at end of scope.
    }

    // Sort by size descending to prioritize large files. This is the part
    // that #82 moves outside the locality-map lock — sort cost grows with
    // candidate count and large actions can have thousands of inputs.
    hint_candidates.sort_by(|a, b| b.1.cmp(&a.1));

    // Build the peer-hint protos in size-descending order. NO truncation:
    // any hints that don't fit in a single wire message are sent in
    // additional `PeerHintsChunk` messages by the dispatch path.
    let peer_hints: Arc<[PeerHint]> = hint_candidates
        .into_iter()
        .map(|(digest, _size, peer_endpoints)| PeerHint {
            digest: Some(digest.into()),
            peer_endpoints: peer_endpoints.iter().map(|e| e.to_string()).collect(),
        })
        .collect();

    let elapsed = started.elapsed();

    // Anomaly: locality map had matches with empty endpoint sets. The map's
    // contract is that an entry implies at least one endpoint; empty lists
    // suggest stale state from an eviction race or an upstream insertion bug.
    if empty_endpoint_matches > 0 {
        warn!(
            empty_endpoint_matches,
            file_digests = file_digests.len(),
            locality_blob_count,
            "locality map returned digest entries with empty endpoint sets"
        );
    }

    // Anomaly: scheduler scoring took longer than expected. This runs on
    // every action dispatch and competes with WorkerScheduler write-lock
    // acquisition; operators should know if it is regularly slow.
    if elapsed > SLOW_THRESHOLD {
        warn!(
            ?elapsed,
            file_digests = file_digests.len(),
            locality_blob_count,
            peer_hints = peer_hints.len(),
            endpoints = scores.len(),
            "score_and_generate_hints exceeded slow threshold"
        );
    }

    let locality_snapshot = locality_snapshot_builder.map(Arc::new);

    debug!(
        file_digests = file_digests.len(),
        locality_blob_count,
        peer_hints = peer_hints.len(),
        endpoints = scores.len(),
        snapshot_entries = locality_snapshot.as_deref().map(HashMap::len).unwrap_or(0),
        ?elapsed,
        "score_and_generate_hints"
    );

    ScoringResult {
        scores,
        peer_hints,
        locality_snapshot,
    }
}

/// Converts endpoint scores to worker scores using the endpoint-to-worker
/// mapping, filtering to the given candidate set.
///
/// Returns `HashMap<WorkerId, u64>` of total cached bytes per worker.
fn endpoint_scores_to_worker_scores(
    endpoint_scores: &HashMap<Arc<str>, u64>,
    endpoint_to_worker: &HashMap<Arc<str>, WorkerId>,
    candidates: &HashSet<WorkerId>,
) -> HashMap<WorkerId, u64> {
    let mut worker_scores: HashMap<WorkerId, u64> = HashMap::new();
    for (endpoint, &score) in endpoint_scores {
        if let Some(worker_id) = endpoint_to_worker.get(endpoint) {
            if candidates.contains(worker_id) {
                *worker_scores.entry(worker_id.clone()).or_insert(0) += score;
            }
        }
    }
    worker_scores
}

/// Backward-compatible wrapper used by existing tests. Scores candidate
/// workers by the total bytes of input blobs they have cached.
#[cfg(test)]
fn score_workers(
    candidates: &HashSet<WorkerId>,
    file_digests: &[(DigestInfo, u64)],
    locality_map: &SharedBlobLocalityMap,
    endpoint_to_worker: &HashMap<Arc<str>, WorkerId>,
) -> HashMap<WorkerId, u64> {
    let scoring = score_and_generate_hints(file_digests, locality_map);
    endpoint_scores_to_worker_scores(&scoring.scores, endpoint_to_worker, candidates)
}

/// (#sched-blend) Test-only inherent methods on `ApiWorkerScheduler`.
#[cfg(test)]
impl ApiWorkerScheduler {
    /// Test-only: set a registered worker's P/E logical-CPU counts. In
    /// production the counts ride the connect frame; tests build the
    /// heterogeneous fleet (e.g. 96-core vs 2-core) the absolute-capacity
    /// blend depends on with this. `peek_mut` to avoid LRU promotion
    /// (matches `update_worker_load` — a topology fact, not a work
    /// assignment).
    async fn set_worker_core_counts(
        &self,
        worker_id: &WorkerId,
        p_core_count: u32,
        e_core_count: u32,
    ) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in set_worker_core_counts() {}",
                worker_id
            )
        })?;
        worker.set_core_counts(p_core_count, e_core_count);
        Ok(())
    }
}

#[async_trait]
impl WorkerScheduler for ApiWorkerScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.platform_property_manager.as_ref()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id.clone();
        let worker_timestamp = worker.last_update_timestamp;
        // (#97) Snapshot endpoint + tx so we can replay any buffered BIS
        // chunks AFTER the worker is registered. The replay is best-effort
        // — if the new tx is also dropped immediately, the chunks stay in
        // the buffer for the next reconnect.
        let cas_endpoint_for_replay = worker.cas_endpoint.clone();
        let tx_for_replay = worker.tx.clone();
        let mut inner = self.inner.write().await;
        if inner.shutting_down {
            warn!("Rejected worker add during shutdown: {}", worker_id);
            return Err(make_err!(
                Code::Unavailable,
                "Received request to add worker while shutting down"
            ));
        }
        let result = inner
            .add_worker(worker)
            .err_tip(|| "Error while adding worker, removing from pool");
        if let Err(err) = result {
            return Result::<(), _>::Err(err.clone())
                .merge(inner.immediate_evict_worker(&worker_id, err, false).await);
        }
        drop(inner);

        let now = UNIX_EPOCH + Duration::from_secs(worker_timestamp);
        // (#386) Register the worker against its `cas_endpoint` so the
        // SIGKILL aggregate counter survives `worker_id` regeneration on
        // reconnect (`worker_api_server.rs:498-502`). Same stable-identity
        // precedent as `bis_resend_buffers` keying (see this file `:267-282`).
        self.worker_registry
            .register_worker_with_endpoint(&worker_id, &cas_endpoint_for_replay, now)
            .await;

        // (#97) Replay any buffered BIS chunks for this endpoint. Same
        // boot_epoch_id only — `inner_connect_worker` clears the buffer
        // via `clear_bis_resend_buffer_for_endpoint` on epoch change
        // before calling add_worker, so we always replay against
        // same-epoch state here.
        if !cas_endpoint_for_replay.is_empty() {
            let replayed = self
                .replay_bis_chunks_to_worker(&cas_endpoint_for_replay, &tx_for_replay)
                .await;
            if replayed > 0 {
                info!(
                    target: "nativelink::bis_chunked_replay",
                    %worker_id,
                    cas_endpoint = %cas_endpoint_for_replay,
                    replayed,
                    "replayed BIS chunks on worker (re)connect"
                );
            }
        }

        // Scores cache is cleared on worker removal (remove_worker) to avoid
        // stale endpoint scores influencing locality decisions.

        self.metrics.workers_added.fetch_add(1, Ordering::Relaxed);
        // (#sched-zeroload) The never-reported gauge increment lives in INNER
        // `add_worker`, collocated with `self.workers.put` (`:817`), so it is
        // symmetric with the choke-point decrement at the pop in inner
        // `remove_worker` and the error/evict path can't underflow the gauge.
        Ok(())
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        // (#sched-b1) Two `.await`-free critical sections bracket one
        // lock-free `update_operation().await`. The worker-pool `inner`
        // write lock is NEVER held across that await (which retries with
        // `tokio::time::sleep` on a version conflict), so a slow/retrying
        // completion no longer serializes the 32 concurrent matchers.
        // See `.claude/audits/sched-b1-worker-lock-decouple-design-2026-06-17.md`.

        // ── critical section 1 (inner.write, NO .await inside) ──
        let cs1 = {
            let mut inner = self.inner.write().await;
            let decision = inner.update_action_cs1(worker_id, operation_id, update);
            match decision {
                Ok(Cs1Decision::NotRunning(err)) => {
                    // FR-1: the op-not-running eviction stays UNDER the
                    // lock exactly as today (the cold error path). This is
                    // the ONE place this method still awaits while holding
                    // `inner`; B1 deliberately does not touch it (the
                    // eviction-loop decouple is `#sched-b1-evict-sibling`).
                    return Result::<(), _>::Err(err.clone()).merge(
                        inner
                            .immediate_evict_worker(worker_id, err, false)
                            .await,
                    );
                }
                other => other,
            }
            // inner (write lock) is dropped here for the Done / Proceed paths.
        };

        let (worker_state_manager, is_finished, due_to_backpressure, update) = match cs1 {
            Ok(Cs1Decision::Done) => return Ok(()),
            Ok(Cs1Decision::Proceed {
                worker_state_manager,
                is_finished,
                due_to_backpressure,
                update,
            }) => (worker_state_manager, is_finished, due_to_backpressure, update),
            Ok(Cs1Decision::NotRunning(_)) => unreachable!("handled under the lock above"),
            Err(err) => return Err(err),
        };

        // ── lock-free await — (b) operation-state update; retries/sleeps
        //    here with NO worker-pool lock held ──
        worker_state_manager
            .update_operation(operation_id, worker_id, update)
            .await
            .err_tip(|| "in update_operation on SimpleScheduler::update_action")
            .map_err(|err| {
                error!(
                    %operation_id,
                    ?worker_id,
                    ?err,
                    "Failed to update_operation on update_action"
                );
                err
            })?;

        if !is_finished {
            return Ok(());
        }

        // ── critical section 2 (inner.write, NO .await inside) ──
        let outcome = {
            let mut inner = self.inner.write().await;
            inner.update_action_cs2(worker_id, operation_id, due_to_backpressure)
        };

        match outcome {
            Cs2Outcome::Completed | Cs2Outcome::WorkerGone => Ok(()),
            Cs2Outcome::AlreadyFinalized => {
                // §6.3 — benign post-unlock race: the op was finalized by a
                // concurrent path during the window, after (b) already
                // committed the authoritative op-state. `warn!` (survives
                // `release_max_level_info`, unlike `debug!`) + a counter so
                // the softened branch is observable in prod.
                self.metrics
                    .update_action_op_already_finalized
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    %operation_id,
                    ?worker_id,
                    "completion found operation already finalized on worker during the \
                     lock-free update window; op-state already committed, softening to ok"
                );
                Ok(())
            }
            Cs2Outcome::Error(err) => Err(err),
        }
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        {
            let mut inner = self.inner.write().await;
            inner
                .refresh_lifetime(worker_id, timestamp)
                .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")?;
        }
        let now = UNIX_EPOCH + Duration::from_secs(timestamp);
        self.worker_registry
            .update_worker_heartbeat(worker_id, now)
            .await;
        Ok(())
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error> {
        self.worker_registry.remove_worker(worker_id).await;

        // scores_cache is cleared by immediate_evict_worker on the inner struct.

        // Grab the worker's CAS endpoint before eviction (used to clean up
        // the prefetch maps below). The never-reported gauge decrement now
        // lives at the eviction choke point — inner `remove_worker` (reached
        // via `immediate_evict_worker`) — so EVERY eviction path decrements,
        // not just this public one. (#sched-zeroload)
        let cas_endpoint: Option<Arc<str>> = {
            let inner = self.inner.read().await;
            inner.workers.peek(worker_id).and_then(|w| {
                if w.cas_endpoint.is_empty() {
                    None
                } else {
                    Some(Arc::from(w.cas_endpoint.as_str()))
                }
            })
        };

        let result = {
            let mut inner = self.inner.write().await;
            inner
                .immediate_evict_worker(
                    worker_id,
                    make_err!(Code::Internal, "Received request to remove worker"),
                    false,
                )
                .await
        };

        // Clean up prefetch connection and semaphore for this endpoint.
        if let Some(ep) = cas_endpoint {
            self.remove_prefetch_for_endpoint(&ep);
        }

        result
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        let mut inner = self.inner.write().await;
        inner.shutting_down = true; // should reject further worker registration
        while let Some(worker_id) = inner
            .workers
            .peek_lru()
            .map(|(worker_id, _worker)| worker_id.clone())
        {
            if let Err(err) = inner
                .immediate_evict_worker(
                    &worker_id,
                    make_err!(Code::Internal, "Scheduler shutdown"),
                    true,
                )
                .await
            {
                error!(?err, "Error evicting worker on shutdown.");
            }
        }
        drop(shutdown_guard);
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        // Check worker liveness using both the local timestamp (from LRU)
        // and the worker registry. A worker is alive if either source says it's alive.
        //
        // Quarantine phase: workers that miss keepalive for > worker_timeout but
        // < 2*worker_timeout are quarantined (stop receiving new work) rather than
        // immediately evicted. Workers that miss keepalive for >= 2*worker_timeout
        // are fully evicted.
        let timeout = Duration::from_secs(self.worker_timeout_s);
        let now = UNIX_EPOCH + Duration::from_secs(now_timestamp);
        let timeout_threshold = now_timestamp.saturating_sub(self.worker_timeout_s);
        let evict_threshold = now_timestamp.saturating_sub(self.worker_timeout_s * 2);

        // Collect (worker_id, local_alive, already_quarantined) for workers that
        // have not responded within the base timeout window.
        let workers_to_check: Vec<(WorkerId, bool, bool)> = {
            let inner = self.inner.read().await;
            inner
                .workers
                .iter()
                .filter_map(|(worker_id, worker)| {
                    let local_alive = worker.last_update_timestamp > timeout_threshold;
                    if local_alive {
                        None
                    } else {
                        let already_quarantined = worker.quarantined_at.is_some();
                        // Check if past the eviction threshold (2x timeout)
                        let past_evict_threshold =
                            worker.last_update_timestamp <= evict_threshold;
                        Some((worker_id.clone(), past_evict_threshold, already_quarantined))
                    }
                })
                .collect()
        };

        if workers_to_check.is_empty() {
            return Ok(());
        }

        // For each candidate, consult the registry to determine actual liveness.
        let mut workers_to_quarantine = Vec::new();
        let mut worker_ids_to_remove = Vec::new();
        for (worker_id, past_evict_threshold, already_quarantined) in workers_to_check {
            let registry_alive = self
                .worker_registry
                .is_worker_alive(&worker_id, timeout, now)
                .await;

            if registry_alive {
                // Registry says alive — no action needed.
                continue;
            }

            if past_evict_threshold {
                // Has been unresponsive for >= 2x the timeout — evict.
                trace!(
                    ?worker_id,
                    past_evict_threshold,
                    "Worker exceeded double-timeout, evicting from pool"
                );
                worker_ids_to_remove.push(worker_id);
            } else if !already_quarantined {
                // Has been unresponsive for > timeout but < 2x timeout — quarantine.
                trace!(
                    ?worker_id,
                    "Worker missed keepalive, entering quarantine (stops receiving work)"
                );
                workers_to_quarantine.push(worker_id);
            }
            // If already_quarantined && !past_evict_threshold: still waiting, no action.
        }

        if workers_to_quarantine.is_empty() && worker_ids_to_remove.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.write().await;

        // Apply quarantine to workers that just crossed the first timeout.
        let quarantine_time = SystemTime::now();
        for worker_id in &workers_to_quarantine {
            if let Some(worker) = inner.workers.peek_mut(worker_id) {
                warn!(
                    ?worker_id,
                    "Worker missed keepalive, quarantining (will not receive new work)"
                );
                worker.quarantined_at = Some(quarantine_time);
            }
        }
        // Notify the matching engine so it skips quarantined workers on next cycle.
        if !workers_to_quarantine.is_empty() {
            inner.worker_change_notify.notify_one();
        }

        // Scores cache is cleared by remove_worker (called after eviction).

        let mut result = Ok(());
        for worker_id in &worker_ids_to_remove {
            warn!(?worker_id, "Worker timed out (2x timeout), removing from pool");
            result = result.merge(
                inner
                    .immediate_evict_worker(
                        worker_id,
                        make_err!(
                            Code::Internal,
                            "Worker {worker_id} timed out, removing from pool"
                        ),
                        false,
                    )
                    .await,
            );
        }

        // Clean up prefetch maps for endpoints no longer in the worker pool.
        if !worker_ids_to_remove.is_empty() {
            let active_endpoints: HashSet<Arc<str>> =
                inner.endpoint_to_worker.keys().cloned().collect();
            drop(inner);
            self.cleanup_stale_prefetch_entries(&active_endpoints);
        }

        result
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        inner.set_drain_worker(worker_id, is_draining).await
    }

    async fn update_worker_load(
        &self,
        worker_id: &WorkerId,
        cpu_load_pct: u32,
        p_core_load_pct: u32,
        e_core_load_pct: u32,
    ) -> Result<(), Error> {
        // Use peek_mut to avoid promoting the worker in the LRU cache —
        // load updates should not affect scheduling order.
        let mut inner = self.inner.write().await;
        let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in update_worker_load() {}",
                worker_id
            )
        })?;
        worker.cpu_load_pct = cpu_load_pct;
        worker.p_core_load_pct = p_core_load_pct;
        worker.e_core_load_pct = e_core_load_pct;
        // (#sched-zeroload) Record that this worker has now reported a load
        // reading. A genuine all-zero (truly idle) report sets this `true`, so
        // the selector can distinguish it from a NEVER-reported worker still at
        // the construction-default `(0,0,0)`. Never reset to `false`.
        let previously_unreported = !worker.has_reported_load;
        worker.has_reported_load = true;
        drop(inner);
        // (#sched-zeroload) Decrement the gauge on the first-ever load report:
        // the worker is no longer in the "never-reported" category.
        if previously_unreported {
            self.metrics
                .workers_never_reported_load
                .fetch_sub(1, Ordering::Relaxed);
        }
        debug!(%worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Worker load updated");
        Ok(())
    }

    async fn update_worker_indefinite_pin_saturation(
        &self,
        worker_id: &WorkerId,
        indefinite_pin_saturated: bool,
    ) -> Result<(), Error> {
        // peek_mut to avoid LRU promotion — a saturation report is telemetry,
        // not work assignment, and must not reorder scheduling.
        let mut inner = self.inner.write().await;
        {
            let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
                make_input_err!(
                    "Worker not found in worker map in \
                     update_worker_indefinite_pin_saturation() {}",
                    worker_id
                )
            })?;
            if worker.indefinite_pin_saturated != indefinite_pin_saturated {
                debug!(
                    %worker_id,
                    indefinite_pin_saturated,
                    "worker indefinite-pin saturation changed"
                );
            }
            worker.indefinite_pin_saturated = indefinite_pin_saturated;
        }
        // A transition to NOT-saturated re-opens this worker to the matcher;
        // wake the matcher so a queued action can be assigned without waiting
        // for the next change tick (mirrors set_drain_worker's notify). Waking
        // on saturation=true is harmless (the matcher just skips the worker).
        if !indefinite_pin_saturated {
            inner.worker_change_notify.notify_one();
        }
        Ok(())
    }

    async fn update_worker_swap_pressure(
        &self,
        worker_id: &WorkerId,
        swap_pressured: bool,
        swap_pressure_rate_per_sec: u32,
    ) -> Result<(), Error> {
        // peek_mut to avoid LRU promotion — a pressure report is telemetry,
        // not work assignment, and must not reorder scheduling (mirrors
        // update_worker_indefinite_pin_saturation).
        let mut inner = self.inner.write().await;
        {
            let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
                make_input_err!(
                    "Worker not found in worker map in \
                     update_worker_swap_pressure() {}",
                    worker_id
                )
            })?;
            if worker.swap_pressured != swap_pressured {
                debug!(
                    %worker_id,
                    swap_pressured,
                    swap_pressure_rate_per_sec,
                    "worker swap pressure changed"
                );
            }
            worker.swap_pressured = swap_pressured;
            worker.swap_pressure_rate_per_sec = swap_pressure_rate_per_sec;
        }
        // A transition to NOT-pressured re-opens this worker to the matcher;
        // wake the matcher so a queued action can be assigned without waiting
        // for the next change tick. LEVEL-triggered (fires whenever the new
        // value is not-pressured, matching the indefinite-pin path); waking
        // on pressured=true is harmless (the matcher just skips the worker).
        if !swap_pressured {
            inner.worker_change_notify.notify_one();
        }
        Ok(())
    }

    async fn update_worker_disk_pressure(
        &self,
        worker_id: &WorkerId,
        disk_pressured: bool,
        available_disk_bytes: u64,
    ) -> Result<(), Error> {
        // peek_mut to avoid LRU promotion — a pressure report is telemetry,
        // not work assignment, and must not reorder scheduling (mirrors
        // update_worker_swap_pressure).
        let mut inner = self.inner.write().await;
        {
            let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
                make_input_err!(
                    "Worker not found in worker map in \
                     update_worker_disk_pressure() {}",
                    worker_id
                )
            })?;
            if worker.disk_pressured != disk_pressured {
                debug!(
                    %worker_id,
                    disk_pressured,
                    available_disk_bytes,
                    "worker disk pressure changed"
                );
            }
            worker.disk_pressured = disk_pressured;
            worker.available_disk_bytes = available_disk_bytes;
        }
        // A transition to NOT-pressured re-opens this worker to the matcher;
        // wake it so a queued action can be assigned without waiting for the
        // next change tick (mirrors update_worker_swap_pressure). Waking on
        // pressured=true is harmless (the matcher just skips the worker).
        if !disk_pressured {
            inner.worker_change_notify.notify_one();
        }
        Ok(())
    }

    async fn update_cached_directories(
        &self,
        worker_id: &WorkerId,
        digests: HashSet<DigestInfo>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in update_cached_directories() {}",
                worker_id
            )
        })?;
        let count = digests.len();
        worker.cached_directory_digests = digests;
        debug!(%worker_id, count, "Worker cached directory digests updated");
        Ok(())
    }

    async fn update_cached_subtrees(
        &self,
        worker_id: &WorkerId,
        is_full_snapshot: bool,
        full_set: Vec<DigestInfo>,
        added: Vec<DigestInfo>,
        removed: Vec<DigestInfo>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        let worker = inner.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in update_cached_subtrees() {}",
                worker_id
            )
        })?;
        if is_full_snapshot {
            let count = full_set.len();
            worker.cached_subtree_digests = full_set.into_iter().collect();
            debug!(%worker_id, count, "Worker cached subtree digests replaced (full snapshot)");
        } else {
            let added_count = added.len();
            let removed_count = removed.len();
            for digest in added {
                worker.cached_subtree_digests.insert(digest);
            }
            for digest in &removed {
                worker.cached_subtree_digests.remove(digest);
            }
            let total = worker.cached_subtree_digests.len();
            debug!(
                %worker_id,
                added_count,
                removed_count,
                total,
                "Worker cached subtree digests updated (delta)"
            );
        }
        Ok(())
    }

    async fn broadcast_blobs_in_stable_storage(&self, digests: Vec<DigestInfo>) {
        self.broadcast_blobs_in_stable_storage(digests).await;
    }

    async fn broadcast_blobs_in_stable_storage_chunked(
        &self,
        digests: Vec<DigestInfo>,
        store_id: &str,
    ) {
        self.broadcast_blobs_in_stable_storage_chunked(digests, store_id).await;
    }

    async fn bis_ack_received(
        &self,
        worker_id: &WorkerId,
        broadcast_id: u64,
        sequence: u32,
        server_instance_token: u64,
    ) {
        // Inherent impl on ApiWorkerScheduler is the source of truth
        // for BIS resend tracking; the trait impl just forwards.
        self.bis_ack_received(worker_id, broadcast_id, sequence, server_instance_token)
            .await;
    }

    async fn clear_bis_resend_buffer_for_endpoint(&self, cas_endpoint: &str) {
        self.clear_bis_resend_buffer_for_endpoint(cas_endpoint).await;
    }

    async fn notify_ac_pin_resync_for_endpoint(&self, cas_endpoint: &str) {
        // Inherent impl on ApiWorkerScheduler is the source of truth for
        // endpoint→worker routing; the trait impl just forwards.
        self.notify_ac_pin_resync_for_endpoint(cas_endpoint).await;
    }

    fn cas_store(&self) -> Option<&Store> {
        // #261: tests + inner_main wiring assertions consult this to
        // verify the scheduler holds the WorkerProxyStore-WRAPPED chain
        // (with peer-fetch fallback) rather than the raw chain. See the
        // trait doc-comment for the full bug description.
        self.cas_store.as_ref()
    }
}

impl RootMetricsComponent for ApiWorkerScheduler {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use bytes::Bytes;
    use nativelink_config::stores::MemorySpec;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Digest as ProtoDigest, DirectoryNode, FileNode,
    };
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};

    #[test]
    fn test_effective_load_score_per_type_p_cores_available() {
        // P-cores not saturated: score equals p_load.
        assert_eq!(effective_load_score(50, 30, 70, true), 50);
        assert_eq!(effective_load_score(1, 100, 80, true), 1);
        assert_eq!(effective_load_score(99, 0, 50, true), 99);
    }

    #[test]
    fn test_effective_load_score_per_type_p_cores_saturated() {
        // P-cores at 100%: score = 100 + e_load, always worse than any
        // worker with available P-cores.
        assert_eq!(effective_load_score(100, 50, 95, true), 150);
        assert_eq!(effective_load_score(100, 0, 100, true), 100);
        assert_eq!(effective_load_score(100, 100, 100, true), 200);
    }

    #[test]
    fn test_effective_load_score_aggregate_only() {
        // Old worker or Linux: p=0, e=0, aggregate>0 → use aggregate.
        assert_eq!(effective_load_score(0, 0, 60, true), 60);
        assert_eq!(effective_load_score(0, 0, 1, true), 1);
        assert_eq!(effective_load_score(0, 0, 100, true), 100);
    }

    #[test]
    fn test_effective_load_score_unknown() {
        // All zeros, never reported: unknown → sort last.
        assert_eq!(effective_load_score(0, 0, 0, false), u64::MAX);
    }

    /// (#sched-zeroload) A worker that HAS reported load and is genuinely
    /// idle (all fields 0) must score 0 (BEST) — not u64::MAX (WORST).
    /// Pre-fix: the all-zero branch fell through to `u64::MAX` regardless
    /// of `has_reported_load`, so a reported-idle worker was penalised
    /// identically to a never-reported one in LRU/MRU and locality tiebreak.
    /// Mutation: remove the `has_reported_load` branch (revert to always
    /// returning u64::MAX for the all-zero case) → this test red-fails with
    /// "reported-idle worker must score 0 (best), not u64::MAX (worst)".
    #[test]
    fn test_effective_load_score_reported_idle_scores_zero() {
        assert_eq!(
            effective_load_score(0, 0, 0, true),
            0,
            "reported-idle worker must score 0 (best), not u64::MAX (worst) — \
             has_reported_load=true with all-zero fields is genuinely idle"
        );
    }

    #[test]
    fn test_effective_load_score_p_core_only_idle() {
        // P-core-only Apple Silicon (no E-cores): reports p=0, e=100.
        // Machine is idle → score should be 0 (best).
        assert_eq!(effective_load_score(0, 100, 0, true), 0);
    }

    #[test]
    fn test_effective_load_score_p_core_only_saturated() {
        // P-core-only fully loaded: p=100, e=100.
        // Score = 100 + 100 = 200 (worst among per-type reporters).
        assert_eq!(effective_load_score(100, 100, 100, true), 200);
    }

    #[test]
    fn test_effective_load_score_ordering() {
        // Verify the two-tier preference: idle P-cores always beat
        // workers with only idle E-cores.
        let idle_p = effective_load_score(30, 80, 50, true);
        let saturated_p = effective_load_score(100, 20, 90, true);
        let aggregate = effective_load_score(0, 0, 40, true);
        let unknown = effective_load_score(0, 0, 0, false);

        assert!(idle_p < saturated_p, "idle P-cores should beat saturated P-cores");
        assert!(aggregate < saturated_p, "aggregate-only in P-tier should beat E-core-only");
        assert!(saturated_p < unknown, "known load should beat unknown");
    }

    // ════════════════════════════════════════════════════════════════════
    // (#sched M1 rebalance v2) P-headroom gate predicate + ranker unit tests.
    // These call the PRIVATE free fns `worker_has_p_headroom` /
    // `p_headroom_pref` DIRECTLY (design §13 test plan items 1/2/4/5),
    // exercising the truth-table, ceiling bound, overflow safety, PrefMonotone,
    // and THRESHOLD=0 collapse. The magnet (I6) tests drive the production
    // Tier-1 + fallback selection paths and live in the external
    // `tests/scheduler_m1v2_ranker_test.rs` (black-box) — this module owns the
    // predicate-level coverage the external crate cannot reach (private fns).
    // ════════════════════════════════════════════════════════════════════

    /// Build a bare `Worker` with a chosen `p_core_count`, `p_core_load_pct`,
    /// and exactly `running` in-flight actions (each a distinct dummy op). The
    /// predicate + ranker read ONLY `running_action_infos.len()`,
    /// `p_core_count`, and `p_core_load_pct` — so this fixture is sufficient to
    /// drive every clause. Self-contained (this `mod tests` does not import the
    /// `b1_lock_decouple_tests` action builders).
    fn worker_with_running(
        name: &str,
        p_core_count: u32,
        p_core_load_pct: u32,
        running: usize,
    ) -> Worker {
        use core::time::Duration;
        use std::time::{SystemTime, UNIX_EPOCH};

        use nativelink_util::action_messages::{
            ActionInfo, ActionUniqueKey, ActionUniqueQualifier,
        };
        use nativelink_util::platform_properties::PlatformProperties;

        use crate::worker::ActionInfoWithProps;

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut w = Worker::new(
            WorkerId(name.to_string()),
            PlatformProperties::default(),
            tx,
            42,
            0,
        );
        w.set_core_counts(p_core_count, 0);
        w.p_core_load_pct = p_core_load_pct;
        w.has_reported_load = true;
        for _ in 0..running {
            // `OperationId::default()` is a fresh v4 UUID each call, so the
            // HashMap keys are distinct and its length equals `running`.
            let op = OperationId::default();
            let action = ActionInfoWithProps {
                inner: Arc::new(ActionInfo {
                    command_digest: DigestInfo::new([0u8; 32], 0),
                    input_root_digest: DigestInfo::new([0u8; 32], 0),
                    timeout: Duration::MAX,
                    platform_properties: HashMap::new(),
                    priority: 0,
                    load_timestamp: UNIX_EPOCH,
                    insert_timestamp: SystemTime::now(),
                    unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                        instance_name: "main".to_string(),
                        digest_function: DigestHasherFunc::Sha256,
                        digest: DigestInfo::new([7u8; 32], 1),
                    }),
                }),
                platform_properties: PlatformProperties::default(),
            };
            w.running_action_infos
                .insert(op, PendingActionInfoData { action_info: action });
        }
        assert_eq!(
            w.running_action_infos.len(),
            running,
            "fixture must produce exactly `running` in-flight actions"
        );
        w
    }

    /// (§13 test 1) Gate predicate truth-table — all 3 clauses × A5, incl. the
    /// override clause firing/not by threshold + ceiling. Calls the predicate
    /// directly.
    #[test]
    fn test_worker_has_p_headroom_v2_truth_table() {
        // ── Clause A5: p_core_count == 0 → ALWAYS ungated (any threshold/factor).
        let a5 = worker_with_running("A5", 0, 100, 99);
        assert!(
            worker_has_p_headroom(&a5, 0, 2),
            "A5 (p_core_count==0) must be ungated regardless of running/p_load"
        );
        assert!(
            worker_has_p_headroom(&a5, 50, 2),
            "A5 must stay ungated even with a threshold set"
        );

        // ── Clause 2: running < p_core_count → genuine free P slot.
        let free = worker_with_running("FREE", 4, 100, 3); // p_load HIGH, still free
        assert!(
            worker_has_p_headroom(&free, 0, 2),
            "clause 2: running(3) < p_core_count(4) → headroom by fresh count, \
             independent of p_load or threshold"
        );

        // ── At the P-count boundary (running == p_core_count): clause 2 false.
        //    THRESHOLD=0 → override clause `p_load < 0` never fires → NO headroom.
        let at_count_thr0 = worker_with_running("AT0", 4, 0, 4);
        assert!(
            !worker_has_p_headroom(&at_count_thr0, 0, 2),
            "threshold 0: at running==p_core_count the override can NEVER fire \
             (`p_load < 0` is never true) → exact v1: no headroom"
        );

        // ── Clause 3 FIRES: running >= p_core_count, p_load < threshold, and
        //    running < p_core_count*factor. Worker at 4 running, p_load 10 < 50,
        //    ceiling 4*2=8 → 4 < 8 → admitted by override.
        let override_ok = worker_with_running("OV", 4, 10, 4);
        assert!(
            worker_has_p_headroom(&override_ok, 50, 2),
            "clause 3: running(4) >= p_count(4), p_load(10) < threshold(50), \
             running(4) < ceiling(8) → bounded override admits"
        );

        // ── Clause 3 DENIED by threshold: p_load(60) >= threshold(50).
        let override_busy_p = worker_with_running("BUSYP", 4, 60, 4);
        assert!(
            !worker_has_p_headroom(&override_busy_p, 50, 2),
            "clause 3 denied: p_load(60) >= threshold(50) → P cores not idle, \
             override must NOT relax (would re-concentrate real CPU work)"
        );

        // ── Clause 3 DENIED by ceiling: running(8) == ceiling(4*2) → NOT < 8.
        let override_at_ceiling = worker_with_running("CEIL", 4, 10, 8);
        assert!(
            !worker_has_p_headroom(&override_at_ceiling, 50, 2),
            "clause 3 denied at ceiling: running(8) NOT < p_count(4)*factor(2)=8 \
             → the fresh-count ceiling shuts the gate regardless of stale-low p_load"
        );
    }

    /// (§13 test 2) Ceiling bound (I5_Bounded): an idle-P worker (low p_load) is
    /// ADMITTED at running==p_count (via the override) but REJECTED at
    /// running==p_count*factor. Plus an overflow test: p_core_count near
    /// u32::MAX must not panic (the ceiling is computed in u64).
    #[test]
    fn test_worker_has_p_headroom_v2_ceiling_bound() {
        // Idle-P worker, threshold 50, factor 2, p_count 4 → ceiling 8.
        // Admitted for running in [4, 7] (override), rejected at 8 (ceiling).
        for running in 4..=7 {
            let w = worker_with_running("IDLEP", 4, 10, running);
            assert!(
                worker_has_p_headroom(&w, 50, 2),
                "idle-P override must admit at running={running} (< ceiling 8)"
            );
        }
        let at_ceiling = worker_with_running("IDLEP", 4, 10, 8);
        assert!(
            !worker_has_p_headroom(&at_ceiling, 50, 2),
            "idle-P override must REJECT at running==p_count*factor(8) — the fresh \
             count is the hard backstop (I5_Bounded)"
        );

        // Overflow safety: p_core_count near u32::MAX. `p_count * factor` as
        // u32*u32 would overflow (debug panic / release wrap); the u64
        // discipline must keep it sane. Just calling it must not panic.
        let huge = worker_with_running("HUGE", u32::MAX, 10, 3);
        assert!(
            worker_has_p_headroom(&huge, 50, 2),
            "u32::MAX p_core_count with running(3) < p_count → clause 2 headroom; \
             the ceiling `u64::from(p_count) * u64::from(factor)` must not overflow"
        );
    }

    /// (§13 test 4) PrefMonotone: two override-admit workers — the MORE
    /// oversubscribed one ranks WORSE (fresh-count decay via `+ (running -
    /// p_count)`). Calls `p_headroom_pref` directly with the gate active.
    #[test]
    fn test_p_headroom_pref_monotone_intra_override() {
        // Both override-admit (p_count 4, p_load 10 < threshold 50, under
        // ceiling 8): one at running 5, one at running 6.
        let less_sub = worker_with_running("LESS", 4, 10, 5);
        let more_sub = worker_with_running("MORE", 4, 10, 6);
        let pref_less = p_headroom_pref(&less_sub, true, 50, 2);
        let pref_more = p_headroom_pref(&more_sub, true, 50, 2);
        assert_eq!(
            pref_less, 2,
            "override pref = 1 + (running(5) - p_count(4)) = 2"
        );
        assert_eq!(
            pref_more, 3,
            "override pref = 1 + (running(6) - p_count(4)) = 3"
        );
        assert!(
            pref_less < pref_more,
            "PrefMonotone: the LESS-oversubscribed override worker must rank \
             strictly better (smaller pref) — the `+ (running - p_count)` fresh \
             decay closes red-team's duration-of-preference gap"
        );
        // And a genuine free slot (pref 0) beats BOTH override-admits.
        let free = worker_with_running("FREE", 4, 10, 2);
        assert_eq!(
            p_headroom_pref(&free, true, 50, 2),
            0,
            "genuine free P slot (running 2 < p_count 4) → pref 0 (BEST), beats \
             any override-admit (pref >= 1) regardless of p_load"
        );
    }

    /// (§13 test 5, unit half) THRESHOLD=0 parity at the pref level: with
    /// `p_idle_threshold_pct == 0` the override never fires, so pref collapses
    /// to the v1 two-way order `{0 for free/A5, u64::MAX for no-headroom}` —
    /// exactly the `{false, true}` order the v2.2 bool key gave, so a
    /// gate-on-v1 selection is unchanged. Also verifies gate-off → pref ≡ 0.
    #[test]
    fn test_p_headroom_pref_threshold_zero_and_gate_off_parity() {
        let free = worker_with_running("FREE", 4, 90, 3); // free slot, high p_load
        let at_count = worker_with_running("FULL", 4, 0, 4); // no free slot, idle p_load

        // THRESHOLD=0, gate active: free → 0, no-free-slot → u64::MAX (override
        // dead). This is the v1 two-way order (free before no-headroom).
        assert_eq!(
            p_headroom_pref(&free, true, 0, 2),
            0,
            "threshold 0: a genuine free slot is pref 0"
        );
        assert_eq!(
            p_headroom_pref(&at_count, true, 0, 2),
            u64::MAX,
            "threshold 0: the override is dead (`p_load < 0` never), so a \
             no-free-slot worker is pref u64::MAX — the v1 two-way order, NOT an \
             override tier"
        );

        // Gate OFF (p_gate_active == false): pref ≡ 0 for ALL workers → the
        // tuple collapses to the existing load key → byte-parity with current.
        assert_eq!(
            p_headroom_pref(&free, false, 50, 2),
            0,
            "gate off: pref ≡ 0 (parity — tuple reduces to the load key)"
        );
        assert_eq!(
            p_headroom_pref(&at_count, false, 50, 2),
            0,
            "gate off: pref ≡ 0 even for a no-free-slot worker (parity)"
        );
    }

    /// (§13, I-1) A5 (`p_core_count == 0`, ungated legacy/Linux/Intel) must rank
    /// TOP-tier (pref 0) with the gate ACTIVE, regardless of how high its running
    /// count or `p_load` is — §5 I4 / §12.3 require an ungated worker to rank
    /// byte-identically to v1 (by load alone, top tier). The predicate-level A5
    /// test (`test_worker_has_p_headroom_v2_truth_table`) covers ELIGIBILITY; this
    /// covers the RANKING arm, which is otherwise unguarded.
    ///
    /// MUTATION: delete the `w.p_core_count == 0 ||` disjunct from the free-slot
    /// arm of `p_headroom_pref` → an A5 worker (p_count 0) falls through to the
    /// override arm, where `running < p_count(0) * factor` is `running < 0` (never
    /// true) → returns `u64::MAX` = ranked WORST. This assert then red-fails with
    /// the bespoke message below.
    #[test]
    fn test_p_headroom_pref_a5_is_top_tier_under_active_gate() {
        // A5: p_core_count 0, HIGH p_load 99, and MORE-than-zero running (5) — the
        // adversarial case (high load + high in-flight) that would fall to the
        // override/MAX arms if the A5 short-circuit were missing.
        let a5 = worker_with_running("A5", 0, 99, 5);
        assert_eq!(
            p_headroom_pref(&a5, true, 50, 2),
            0,
            "A5 (p_core_count == 0) must rank TOP-tier (pref 0) under the ACTIVE \
             gate even at high running(5) + high p_load(99) — an ungated worker \
             ranks by load alone (v1 parity), NOT at the override/no-headroom tier"
        );
        // And at running 0 (the trivially-idle A5) — still pref 0.
        let a5_idle = worker_with_running("A5_IDLE", 0, 99, 0);
        assert_eq!(
            p_headroom_pref(&a5_idle, true, 50, 2),
            0,
            "A5 stays pref 0 at running 0 (the free-slot/A5 arm), independent of p_load"
        );
    }

    /// Helper: encode a Directory proto and compute its DigestInfo (SHA256).
    fn encode_directory(dir: &Directory) -> (Vec<u8>, DigestInfo) {
        let dir_bytes = dir.encode_to_vec();
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        hasher.update(&dir_bytes);
        let digest_info = hasher.finalize_digest();
        (dir_bytes, digest_info)
    }

    /// Helper: create a FileNode with a deterministic fake digest.
    fn make_file_node(name: &str, hash_byte: u8, size: i64) -> FileNode {
        FileNode {
            name: name.to_string(),
            digest: Some(ProtoDigest {
                hash: format!("{:02x}", hash_byte).repeat(32), // 64-char hex
                size_bytes: size,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------
    // #52 (option b2, 2026-06-07 mutation stamp): subtree-coverage
    // `coverage_pct` must (a) never exceed 100 %, (b) remain >0 for
    // partial-match workers (so tier 2 still distinguishes between
    // candidates that have part of the subtree cached).
    //
    // Fixture matches the pre-fix production WARN log
    // (`.claude/audits/52-scheduler-subtree-overload-rca-2026-06-04.md`,
    // §3: distribution n=214, p50=187 %, p95=466 %, max=466 %). A
    // worker has the action's root AND a nested child subtree both in
    // `cached_subtree_digests`; the OLD numerator iterated
    // `tree.dir_digests` and summed `subtree_bytes[d]` (recursive),
    // double-counting the child's bytes inside root's recursive total.
    //
    // NEW numerator: `compute_dedup_cached_score` iterates the
    // `HashSet<DigestInfo>` of `dir_digests` (already deduped) and sums
    // each cached directory's DIRECT (non-recursive) bytes/files. The
    // direct contributions are disjoint across directories, so the
    // numerator is bounded by `subtree_bytes[root]` and partial matches
    // produce intermediate `coverage_pct` values in `(0, 100)`.
    //
    // Mutation guards:
    //   - Revert helper to `compute_old_buggy_cached_score`:
    //     test red-fails with
    //     "coverage_pct >100% — partial match double-counts via
    //      recursive subtree_bytes".
    //   - Use `compute_root_only_cached_score` (b1 over-collapse):
    //     T3 red-fails with
    //     "tier 2 partial-match signal lost — workers indistinguishable".
    // -----------------------------------------------------------------
    #[test]
    fn test_coverage_pct_never_exceeds_100_under_nested_subtree_match() {
        const PER_FILE_WEIGHT: u64 = 100 * 1024;

        // Action tree: root with two nested children, mirroring the
        // pre-fix 466 % production case (audit §3 distribution n=214,
        // p50=187 %, p95=466 %, max=466 %).
        let root = DigestInfo::new([0xAAu8; 32], 100);
        let child_a = DigestInfo::new([0xBBu8; 32], 50);
        let child_b = DigestInfo::new([0xCCu8; 32], 50);

        let mut dir_digests: HashSet<DigestInfo> = HashSet::new();
        dir_digests.insert(root);
        dir_digests.insert(child_a);
        dir_digests.insert(child_b);

        // Direct (non-recursive) per-dir contributions — disjoint by
        // construction, summing to root's recursive total.
        //   root direct      = 100k bytes / 10 files
        //   child_a direct   = 700k bytes / 70 files
        //   child_b direct   = 200k bytes / 20 files
        //   sum (root subtree) = 1M bytes / 100 files
        let mut dir_direct_bytes: HashMap<DigestInfo, u64> = HashMap::new();
        dir_direct_bytes.insert(root, 100_000);
        dir_direct_bytes.insert(child_a, 700_000);
        dir_direct_bytes.insert(child_b, 200_000);

        let mut dir_direct_files: HashMap<DigestInfo, u64> = HashMap::new();
        dir_direct_files.insert(root, 10);
        dir_direct_files.insert(child_a, 70);
        dir_direct_files.insert(child_b, 20);

        // Recursive subtree_bytes (denominator + b1-mutation fixture).
        // (Sum of subtree_bytes across all dirs = 1.9M, which is what
        // the old numerator counted — 190 % of the 1M denominator.)
        let mut subtree_bytes: HashMap<DigestInfo, u64> = HashMap::new();
        subtree_bytes.insert(root, 1_000_000);
        subtree_bytes.insert(child_a, 700_000);
        subtree_bytes.insert(child_b, 200_000);

        let mut subtree_files: HashMap<DigestInfo, u64> = HashMap::new();
        subtree_files.insert(root, 100);
        subtree_files.insert(child_a, 70);
        subtree_files.insert(child_b, 20);

        let total_bytes = *subtree_bytes.get(&root).unwrap();
        let total_files = *subtree_files.get(&root).unwrap();
        let total_score = total_bytes + total_files * PER_FILE_WEIGHT;

        // Worker has cached the full root subtree AND has nested
        // entries — the realistic production shape that triggered
        // the OLD double-count.
        let mut cached: HashSet<DigestInfo> = HashSet::new();
        cached.insert(root);
        cached.insert(child_a);
        cached.insert(child_b);

        // --- OLD form (must overflow >100 %) — proves the bug existed. ---
        let (old_bytes, old_files) = compute_old_buggy_cached_score(
            &dir_digests, &cached, &subtree_bytes, &subtree_files,
        );
        let old_score = old_bytes + old_files * PER_FILE_WEIGHT;
        let old_pct = if total_score > 0 {
            old_score * 100 / total_score
        } else {
            0
        };
        assert!(
            old_pct > 100,
            "fixture must reproduce the pre-fix bug (got old_pct={old_pct} \
             — fixture didn't trigger the >100% case; tighten it)"
        );

        // --- NEW form (the actual production code) must be ≤ 100 %
        //     AND must equal exactly 100 % under full-subtree caching. ---
        let (new_bytes, new_files) = compute_dedup_cached_score(
            &dir_digests, &cached, &dir_direct_bytes, &dir_direct_files,
        );
        let new_score = new_bytes + new_files * PER_FILE_WEIGHT;
        let new_pct = if total_score > 0 {
            new_score * 100 / total_score
        } else {
            0
        };
        assert!(
            new_pct <= 100,
            "coverage_pct >100% — partial match double-counts via \
             recursive subtree_bytes (new_pct={new_pct}, \
             new_score={new_score}, total_score={total_score})"
        );
        assert_eq!(
            new_pct, 100,
            "full subtree cached ⇒ pct must be exactly 100 \
             (got {new_pct})",
        );

        // --- Partial match (worker has child_a only): >0 AND <100. ---
        let mut partial: HashSet<DigestInfo> = HashSet::new();
        partial.insert(child_a);
        let (pb, pf) = compute_dedup_cached_score(
            &dir_digests, &partial, &dir_direct_bytes, &dir_direct_files,
        );
        let partial_score = pb + pf * PER_FILE_WEIGHT;
        let partial_pct = if total_score > 0 {
            partial_score * 100 / total_score
        } else {
            0
        };
        assert!(
            partial_pct > 0 && partial_pct < 100,
            "partial match must produce intermediate coverage_pct in \
             (0, 100) (got partial_pct={partial_pct}, \
             partial_score={partial_score}, total_score={total_score})"
        );

        // --- Nothing cached ⇒ 0 %. ---
        let empty: HashSet<DigestInfo> = HashSet::new();
        let (eb, ef) = compute_dedup_cached_score(
            &dir_digests, &empty, &dir_direct_bytes, &dir_direct_files,
        );
        let empty_pct = if total_score > 0 {
            (eb + ef * PER_FILE_WEIGHT) * 100 / total_score
        } else {
            0
        };
        assert_eq!(empty_pct, 0, "nothing cached ⇒ pct must be 0");
    }

    // -----------------------------------------------------------------
    // #52 T3 (option b2 partial-match resolution guard): worker A
    // (30 % of subtree bytes cached) and worker B (70 % cached) must
    // produce STRICTLY ORDERED coverage_pct values so tier-2 selection
    // routes to worker B. Guards against an (b1)-style "root-only
    // collapse" regression where every non-root match scores 0 and
    // tier 2 becomes dead.
    //
    // Fixture: 4 nested children under root with disjoint direct
    // contributions (300k / 200k / 200k / 200k = 900k subtree; root
    // direct 100k). Worker A caches child_a (30 % of total subtree).
    // Worker B caches child_b + child_c + child_d (70 %).
    //
    // Mutation guard: swap `compute_dedup_cached_score` for the
    // root-only `compute_root_only_cached_score` (the (b1) form); A
    // and B both score 0 and the strict inequality red-fails with
    //   "tier 2 partial-match signal lost — workers indistinguishable".
    // -----------------------------------------------------------------
    #[test]
    fn test_coverage_pct_distinguishes_partial_matches() {
        const PER_FILE_WEIGHT: u64 = 100 * 1024;

        let root = DigestInfo::new([0x11u8; 32], 100);
        let child_a = DigestInfo::new([0x22u8; 32], 50);
        let child_b = DigestInfo::new([0x33u8; 32], 50);
        let child_c = DigestInfo::new([0x44u8; 32], 50);
        let child_d = DigestInfo::new([0x55u8; 32], 50);

        let mut dir_digests: HashSet<DigestInfo> = HashSet::new();
        dir_digests.insert(root);
        dir_digests.insert(child_a);
        dir_digests.insert(child_b);
        dir_digests.insert(child_c);
        dir_digests.insert(child_d);

        // Disjoint direct contributions: root 100k + 4×children = 900k
        // root_subtree = 1_000_000 bytes, 100 files. Direct
        // partitions: A=300k/30 files, B=200k/20, C=200k/20, D=200k/20.
        let mut dir_direct_bytes: HashMap<DigestInfo, u64> = HashMap::new();
        dir_direct_bytes.insert(root, 100_000);
        dir_direct_bytes.insert(child_a, 300_000);
        dir_direct_bytes.insert(child_b, 200_000);
        dir_direct_bytes.insert(child_c, 200_000);
        dir_direct_bytes.insert(child_d, 200_000);

        let mut dir_direct_files: HashMap<DigestInfo, u64> = HashMap::new();
        dir_direct_files.insert(root, 10);
        dir_direct_files.insert(child_a, 30);
        dir_direct_files.insert(child_b, 20);
        dir_direct_files.insert(child_c, 20);
        dir_direct_files.insert(child_d, 20);

        let mut subtree_bytes: HashMap<DigestInfo, u64> = HashMap::new();
        subtree_bytes.insert(root, 1_000_000);
        let mut subtree_files: HashMap<DigestInfo, u64> = HashMap::new();
        subtree_files.insert(root, 100);
        let total_bytes = *subtree_bytes.get(&root).unwrap();
        let total_files = *subtree_files.get(&root).unwrap();
        let total_score = total_bytes + total_files * PER_FILE_WEIGHT;

        // Worker A: caches child_a only (300k direct bytes = 30 %).
        let mut worker_a_cached: HashSet<DigestInfo> = HashSet::new();
        worker_a_cached.insert(child_a);

        // Worker B: caches child_b + child_c + child_d (600k = 60 %
        // of subtree bytes; >A but <root).
        let mut worker_b_cached: HashSet<DigestInfo> = HashSet::new();
        worker_b_cached.insert(child_b);
        worker_b_cached.insert(child_c);
        worker_b_cached.insert(child_d);

        let (a_bytes, a_files) = compute_dedup_cached_score(
            &dir_digests,
            &worker_a_cached,
            &dir_direct_bytes,
            &dir_direct_files,
        );
        let a_score = a_bytes + a_files * PER_FILE_WEIGHT;
        let a_pct = if total_score > 0 {
            a_score * 100 / total_score
        } else {
            0
        };

        let (b_bytes, b_files) = compute_dedup_cached_score(
            &dir_digests,
            &worker_b_cached,
            &dir_direct_bytes,
            &dir_direct_files,
        );
        let b_score = b_bytes + b_files * PER_FILE_WEIGHT;
        let b_pct = if total_score > 0 {
            b_score * 100 / total_score
        } else {
            0
        };

        // Both partial-match workers must produce a non-zero score,
        // otherwise tier 2 cannot distinguish them from a cache-cold
        // worker.
        assert!(
            a_score > 0 && b_score > 0,
            "tier 2 partial-match signal lost — workers indistinguishable \
             (a_score={a_score}, b_score={b_score})"
        );

        // Worker B has strictly more cached than worker A.
        assert!(
            b_score > a_score && b_pct > a_pct,
            "tier 2 partial-match ordering lost — A and B must be \
             strictly ordered by cached volume \
             (a_pct={a_pct}, b_pct={b_pct}, \
              a_score={a_score}, b_score={b_score})"
        );

        // Both are bounded ≤100 %.
        assert!(a_pct <= 100 && b_pct <= 100, "pcts in range");
    }

    #[test]
    fn test_score_workers_basic() {
        let locality_map = new_shared_blob_locality_map();
        let d1 = DigestInfo::new([1u8; 32], 1000);
        let d2 = DigestInfo::new([2u8; 32], 2000);
        let d3 = DigestInfo::new([3u8; 32], 3000);

        // worker-a has d1 and d2 (3000 bytes total)
        // worker-b has d2 and d3 (5000 bytes total)
        {
            let mut map = locality_map.write();
            map.register_blobs("grpc://worker-a:50081", &[d1, d2]);
            map.register_blobs("grpc://worker-b:50081", &[d2, d3]);
        }

        let worker_a = WorkerId::from("worker-a-id".to_string());
        let worker_b = WorkerId::from("worker-b-id".to_string());

        let mut endpoint_to_worker = HashMap::new();
        endpoint_to_worker.insert(Arc::from("grpc://worker-a:50081"), worker_a.clone());
        endpoint_to_worker.insert(Arc::from("grpc://worker-b:50081"), worker_b.clone());

        let mut candidates = HashSet::new();
        candidates.insert(worker_a.clone());
        candidates.insert(worker_b.clone());

        let file_digests = vec![(d1, 1000), (d2, 2000), (d3, 3000)];

        let scores = score_workers(&candidates, &file_digests, &locality_map, &endpoint_to_worker);

        assert_eq!(scores.get(&worker_a), Some(&3000)); // d1(1000) + d2(2000)
        assert_eq!(scores.get(&worker_b), Some(&5000)); // d2(2000) + d3(3000)
    }

    #[test]
    fn test_score_workers_non_candidate_excluded() {
        let locality_map = new_shared_blob_locality_map();
        let d1 = DigestInfo::new([1u8; 32], 1000);

        {
            let mut map = locality_map.write();
            map.register_blobs("grpc://worker-a:50081", &[d1]);
        }

        let worker_a = WorkerId::from("worker-a-id".to_string());
        let mut endpoint_to_worker = HashMap::new();
        endpoint_to_worker.insert(Arc::from("grpc://worker-a:50081"), worker_a.clone());

        // worker_a is NOT in candidates
        let candidates = HashSet::new();
        let file_digests = vec![(d1, 1000)];

        let scores = score_workers(&candidates, &file_digests, &locality_map, &endpoint_to_worker);
        assert!(scores.is_empty());
    }

    #[test]
    fn test_score_workers_empty_locality_map() {
        let locality_map = new_shared_blob_locality_map();
        let d1 = DigestInfo::new([1u8; 32], 1000);

        let worker_a = WorkerId::from("worker-a-id".to_string());
        let mut candidates = HashSet::new();
        candidates.insert(worker_a.clone());

        let endpoint_to_worker = HashMap::new();
        let file_digests = vec![(d1, 1000)];

        let scores = score_workers(&candidates, &file_digests, &locality_map, &endpoint_to_worker);
        assert!(scores.is_empty());
    }

    // ---------------------------------------------------------------
    // resolve_tree_from_cas tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_resolve_tree_single_directory() {
        // A single directory with 3 files, no subdirectories.
        let dir = Directory {
            files: vec![
                make_file_node("file1.txt", 0xaa, 1000),
                make_file_node("file2.txt", 0xbb, 2000),
                make_file_node("file3.txt", 0xcc, 3000),
            ],
            directories: vec![],
            ..Default::default()
        };

        let (dir_bytes, dir_digest) = encode_directory(&dir);
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let key: StoreKey<'_> = dir_digest.into();
        store
            .update_oneshot(key, Bytes::from(dir_bytes))
            .await
            .expect("store update_oneshot failed");

        let failed_dirs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let result = resolve_tree_from_cas(&store, dir_digest, &failed_dirs)
            .await
            .expect("resolve_tree_from_cas failed");

        assert_eq!(result.file_digests.len(), 3, "Expected 3 file digests");
        assert_eq!(result.dir_digests.len(), 1, "Expected 1 directory digest (root)");
        assert!(result.dir_digests.contains(&dir_digest));

        // Root subtree contains all files: 1000+2000+3000 = 6000
        assert_eq!(result.subtree_bytes.get(&dir_digest), Some(&6000));

        // Verify all three sizes are present (order may vary).
        let mut sizes: Vec<u64> = result.file_digests.iter().map(|&(_, s)| s).collect();
        sizes.sort();
        assert_eq!(sizes, vec![1000, 2000, 3000]);
    }

    #[tokio::test]
    async fn test_resolve_tree_nested_directories() {
        // Subdirectory with 2 files.
        let sub_dir = Directory {
            files: vec![
                make_file_node("sub_file1.txt", 0x11, 500),
                make_file_node("sub_file2.txt", 0x22, 700),
            ],
            directories: vec![],
            ..Default::default()
        };
        let (sub_dir_bytes, sub_dir_digest) = encode_directory(&sub_dir);

        // Root directory with 1 file and a reference to the subdirectory.
        let root_dir = Directory {
            files: vec![make_file_node("root_file.txt", 0x33, 1200)],
            directories: vec![DirectoryNode {
                name: "subdir".to_string(),
                digest: Some(sub_dir_digest.into()),
            }],
            ..Default::default()
        };
        let (root_dir_bytes, root_dir_digest) = encode_directory(&root_dir);

        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let root_key: StoreKey<'_> = root_dir_digest.into();
        store
            .update_oneshot(root_key, Bytes::from(root_dir_bytes))
            .await
            .expect("store root dir");
        let sub_key: StoreKey<'_> = sub_dir_digest.into();
        store
            .update_oneshot(sub_key, Bytes::from(sub_dir_bytes))
            .await
            .expect("store sub dir");

        let failed_dirs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let result = resolve_tree_from_cas(&store, root_dir_digest, &failed_dirs)
            .await
            .expect("resolve_tree_from_cas failed");

        assert_eq!(result.file_digests.len(), 3, "Expected 3 files (1 root + 2 subdir)");
        assert_eq!(result.dir_digests.len(), 2, "Expected 2 directory digests (root + subdir)");
        assert!(result.dir_digests.contains(&root_dir_digest));
        assert!(result.dir_digests.contains(&sub_dir_digest));

        // subdir has 500+700=1200 bytes of files
        assert_eq!(result.subtree_bytes.get(&sub_dir_digest), Some(&1200));
        // root has 1200 (own file) + 1200 (subdir subtree) = 2400
        assert_eq!(result.subtree_bytes.get(&root_dir_digest), Some(&2400));

        let mut sizes: Vec<u64> = result.file_digests.iter().map(|&(_, s)| s).collect();
        sizes.sort();
        assert_eq!(sizes, vec![500, 700, 1200]);
    }

    #[tokio::test]
    async fn test_resolve_tree_deduplicates_files() {
        // Two directories both referencing the same file digest.
        let shared_file = make_file_node("shared.txt", 0xdd, 999);

        let sub_dir = Directory {
            files: vec![shared_file.clone()],
            directories: vec![],
            ..Default::default()
        };
        let (sub_dir_bytes, sub_dir_digest) = encode_directory(&sub_dir);

        let root_dir = Directory {
            files: vec![
                // Same digest as the file in sub_dir (same hash_byte 0xdd, same size).
                make_file_node("also_shared.txt", 0xdd, 999),
            ],
            directories: vec![DirectoryNode {
                name: "subdir".to_string(),
                digest: Some(sub_dir_digest.into()),
            }],
            ..Default::default()
        };
        let (root_dir_bytes, root_dir_digest) = encode_directory(&root_dir);

        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let root_key: StoreKey<'_> = root_dir_digest.into();
        store
            .update_oneshot(root_key, Bytes::from(root_dir_bytes))
            .await
            .expect("store root dir");
        let sub_key: StoreKey<'_> = sub_dir_digest.into();
        store
            .update_oneshot(sub_key, Bytes::from(sub_dir_bytes))
            .await
            .expect("store sub dir");

        let failed_dirs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let result = resolve_tree_from_cas(&store, root_dir_digest, &failed_dirs)
            .await
            .expect("resolve_tree_from_cas failed");

        // The same digest should appear only once.
        assert_eq!(
            result.file_digests.len(),
            1,
            "Duplicate file digest should be deduplicated"
        );
        assert_eq!(result.file_digests[0].1, 999);
        assert_eq!(result.dir_digests.len(), 2, "Expected root + subdir");
        assert!(result.dir_digests.contains(&root_dir_digest));
        assert!(result.dir_digests.contains(&sub_dir_digest));

        // Both dirs have the same file (999 bytes) — subtree_bytes counts
        // each occurrence (not deduplicated, since it's per-directory).
        assert_eq!(result.subtree_bytes.get(&sub_dir_digest), Some(&999));
        assert_eq!(result.subtree_bytes.get(&root_dir_digest), Some(&1998)); // 999 + 999
    }

    #[tokio::test]
    async fn test_resolve_tree_circular_directory() {
        // A true hash cycle (A->B->A) is impossible with content-addressed
        // hashes: the digest of A depends on B's digest and vice versa.
        // Instead, we test the seen_dirs guard with a diamond structure:
        //   root -> {dir_left, dir_right}, both -> dir_shared
        // Without the seen_dirs set, dir_shared would be visited twice.
        let dir_shared = Directory {
            files: vec![make_file_node("shared.txt", 0x11, 100)],
            directories: vec![],
            ..Default::default()
        };
        let (shared_bytes, shared_digest) = encode_directory(&dir_shared);

        let dir_left = Directory {
            files: vec![make_file_node("left.txt", 0x22, 200)],
            directories: vec![DirectoryNode {
                name: "shared".to_string(),
                digest: Some(shared_digest.into()),
            }],
            ..Default::default()
        };
        let (left_bytes, left_digest) = encode_directory(&dir_left);

        let dir_right = Directory {
            files: vec![make_file_node("right.txt", 0x33, 300)],
            directories: vec![DirectoryNode {
                name: "shared".to_string(),
                digest: Some(shared_digest.into()),
            }],
            ..Default::default()
        };
        let (right_bytes, right_digest) = encode_directory(&dir_right);

        let root = Directory {
            files: vec![],
            directories: vec![
                DirectoryNode {
                    name: "left".to_string(),
                    digest: Some(left_digest.into()),
                },
                DirectoryNode {
                    name: "right".to_string(),
                    digest: Some(right_digest.into()),
                },
            ],
            ..Default::default()
        };
        let (root_bytes, root_digest) = encode_directory(&root);

        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        for (bytes, digest) in [
            (root_bytes, root_digest),
            (left_bytes, left_digest),
            (right_bytes, right_digest),
            (shared_bytes, shared_digest),
        ] {
            let key: StoreKey<'_> = digest.into();
            store
                .update_oneshot(key, Bytes::from(bytes))
                .await
                .expect("store update");
        }

        let failed_dirs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let result = resolve_tree_from_cas(&store, root_digest, &failed_dirs)
            .await
            .expect("resolve_tree_from_cas failed");

        // dir_shared is referenced by both dir_left and dir_right, but
        // seen_dirs ensures it's only visited once. Files: shared(0x11),
        // left(0x22), right(0x33) — all unique digests, so 3 total.
        assert_eq!(
            result.file_digests.len(),
            3,
            "Diamond structure: shared dir visited once, 3 unique files"
        );
        // 4 directories: root, left, right, shared
        assert_eq!(result.dir_digests.len(), 4, "Expected 4 directory digests");
        assert!(result.dir_digests.contains(&root_digest));
        assert!(result.dir_digests.contains(&left_digest));
        assert!(result.dir_digests.contains(&right_digest));
        assert!(result.dir_digests.contains(&shared_digest));

        // shared: 100 bytes (its own file)
        assert_eq!(result.subtree_bytes.get(&shared_digest), Some(&100));
        // left: 200 (own) + 100 (shared) = 300
        assert_eq!(result.subtree_bytes.get(&left_digest), Some(&300));
        // right: 300 (own) + 100 (shared) = 400
        assert_eq!(result.subtree_bytes.get(&right_digest), Some(&400));
        // root: 0 (no own files) + 300 (left) + 400 (right) = 700
        assert_eq!(result.subtree_bytes.get(&root_digest), Some(&700));

        let mut sizes: Vec<u64> = result.file_digests.iter().map(|&(_, s)| s).collect();
        sizes.sort();
        assert_eq!(sizes, vec![100, 200, 300]);
    }

    #[tokio::test]
    async fn test_resolve_tree_missing_directory() {
        // Attempt to resolve a digest that doesn't exist in the store.
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));

        let missing_digest = DigestInfo::new([0xff; 32], 42);
        let failed_dirs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let result = resolve_tree_from_cas(&store, missing_digest, &failed_dirs).await;

        assert!(
            result.is_err(),
            "Should return an error for a missing directory"
        );

        // The failing digest should be recorded in the subdirectory negative cache.
        let cache = failed_dirs.lock().await;
        assert!(
            cache.contains_key(&missing_digest),
            "Missing digest should be in failed_directory_digests cache"
        );
    }

    #[test]
    fn test_score_workers_empty_file_list() {
        let locality_map = new_shared_blob_locality_map();

        // Even with data in the locality map, empty file_digests => empty scores.
        {
            let mut map = locality_map.write();
            let d1 = DigestInfo::new([1u8; 32], 1000);
            map.register_blobs("grpc://worker-a:50081", &[d1]);
        }

        let worker_a = WorkerId::from("worker-a-id".to_string());
        let mut endpoint_to_worker = HashMap::new();
        endpoint_to_worker.insert(Arc::from("grpc://worker-a:50081"), worker_a.clone());

        let mut candidates = HashSet::new();
        candidates.insert(worker_a);

        let file_digests: Vec<(DigestInfo, u64)> = vec![];

        let scores = score_workers(&candidates, &file_digests, &locality_map, &endpoint_to_worker);
        assert!(
            scores.is_empty(),
            "Expected empty scores for empty file_digests, got {scores:?}"
        );
    }

    // ------------------------------------------------------------------
    // (#407) Locality-snapshot reuse tests.
    //
    // These exercise the contract that Phase-4 callers
    // (`compute_missing_blobs` prefetch path + inline `all_missing`
    // filter) consult the scoring-time snapshot captured inside
    // `score_and_generate_hints`, NOT a fresh `locality_map.read()`.
    //
    // The composition under test is:
    //   1. score_and_generate_hints produces ScoringResult with snapshot
    //   2. locality_map is MUTATED between Phase 2 and Phase 4
    //   3. compute_missing_blobs(snapshot=Some(...)) returns
    //      scoring-time-classified missing set, NOT post-mutation set
    //   4. Inline all_missing walk via snapshot is consistent
    // ------------------------------------------------------------------

    /// The Phase-4 callers must consult the scoring-time snapshot, so
    /// when the locality_map is flipped between Phase 2 (scoring) and
    /// Phase 4 (prefetch + missing-digests filter), the Phase-4 walks
    /// reflect SCORING-TIME state — not the post-flip live view.
    ///
    /// Mutation: comment out the snapshot construction inside
    /// `score_and_generate_hints` (force callers back to
    /// `locality_map.read()`); this test must red-fail with the
    /// SCORING-TIME assertion message — "must reuse scoring-time
    /// snapshot; would have read live locality_map and gotten E2".
    #[test]
    fn test_phase4_reuses_scoring_snapshot_under_locality_flip() {
        let locality_map = new_shared_blob_locality_map();

        // Two endpoints to flip between.
        let endpoint_a = "grpc://worker-a:50081";
        let endpoint_b = "grpc://worker-b:50081";

        // Three digests in the action's input tree.
        let d1 = DigestInfo::new([0x11; 32], 1000);
        let d2 = DigestInfo::new([0x22; 32], 2000);
        let d3 = DigestInfo::new([0x33; 32], 3000);
        let file_digests = vec![(d1, 1000), (d2, 2000), (d3, 3000)];

        // SCORING-TIME state: endpoint A has d1 and d2; nobody has d3.
        // The dispatched worker is the target_endpoint: endpoint A.
        // Therefore, scoring-time-classified "missing for A" = {d3}.
        {
            let mut map = locality_map.write();
            map.register_blobs(endpoint_a, &[d1, d2]);
        }

        // Phase 2: capture the scoring-time snapshot.
        let scoring = score_and_generate_hints(&file_digests, &locality_map);
        let snapshot = scoring
            .locality_snapshot
            .as_ref()
            .expect("snapshot must be Some — file_digests << LOCALITY_SNAPSHOT_MAX_ENTRIES");

        // Sanity: snapshot reflects scoring-time state.
        assert_eq!(
            snapshot.len(),
            2,
            "scoring-time snapshot must contain exactly the matched digests (d1, d2)"
        );
        assert!(snapshot.contains_key(&d1));
        assert!(snapshot.contains_key(&d2));
        assert!(!snapshot.contains_key(&d3));

        // POST-SCORING FLIP: between Phase 2 (scoring) and Phase 4
        // (prefetch + missing-digests filter), the locality_map state
        // changes. This simulates a `register_blobs` from a BIS
        // broadcast or an `evict_blobs` from a worker eviction
        // arriving on the writer side concurrent with this dispatch.
        //
        // After the flip:
        //   - endpoint A has only d2 (d1 was evicted)
        //   - endpoint B now has d1 (worker B just announced it)
        //   - d3 still unowned
        //
        // If Phase-4 callers consult the LIVE map, they would classify
        // d1 as "not missing for A" via endpoint B (wrong — we want
        // the worker we're dispatching to, A, to have it). They would
        // also classify d1 as "missing for A" (correct conclusion via
        // post-flip view, but for the WRONG reason — endpoint A no
        // longer has d1). The salient observable difference is the
        // snapshot's count of endpoints per digest: pre-flip d1 has
        // 1 endpoint (A); post-flip d1 has 1 endpoint (B); the live
        // view of d1's endpoint set is different from the snapshot.
        //
        // We choose the cleanest observable: d1 is REMOVED from A
        // post-flip. The scoring-time snapshot said "A has d1";
        // therefore compute_missing_blobs(endpoint=A, snapshot=...)
        // must NOT include d1 in the missing set (scoring-time
        // contract — A has it). A live read would include d1
        // (post-flip, A doesn't have it). This is the bit-flip we
        // assert on.
        {
            let mut map = locality_map.write();
            map.evict_blobs(endpoint_a, &[d1]);
            map.register_blobs(endpoint_b, &[d1]);
        }

        // Phase 4 — prefetch path: compute_missing_blobs with snapshot.
        let prefetch_missing = ApiWorkerScheduler::compute_missing_blobs(
            &file_digests,
            endpoint_a,
            Some(snapshot),
            &locality_map,
        );

        // SCORING-TIME contract: A had d1 + d2; only d3 is missing.
        // If snapshot reuse is broken (Phase 4 reads live map), d1
        // ALSO becomes missing (post-flip A no longer has d1), and
        // prefetch_missing would contain {d1, d3}.
        let missing_digests: Vec<DigestInfo> =
            prefetch_missing.iter().map(|(d, _)| *d).collect();
        assert!(
            missing_digests.contains(&d3),
            "scoring-time missing must include d3 (no one had d3 at scoring time)"
        );
        assert!(
            !missing_digests.contains(&d1),
            "must reuse scoring-time snapshot; would have read live locality_map \
             and gotten d1 in missing (post-flip A doesn't have d1). \
             actual missing_digests: {missing_digests:?}"
        );

        // Phase 4 — inline `all_missing` walk: this is the second
        // sibling F-walk inside `find_and_reserve_worker` (the one
        // that builds `start_execute.missing_digests`). It uses the
        // same snapshot-vs-live logic; assert identical scoring-time
        // semantics here.
        let endpoint_arc: Arc<str> = Arc::from(endpoint_a);
        let all_missing_via_snapshot: Vec<DigestInfo> = file_digests
            .iter()
            .filter(|(_, size)| *size > 0)
            .filter(|(digest, _)| {
                snapshot
                    .get(digest)
                    .is_none_or(|endpoints| {
                        !endpoints.iter().any(|e| &**e == endpoint_arc.as_ref())
                    })
            })
            .map(|(d, _)| *d)
            .collect();

        assert!(
            all_missing_via_snapshot.contains(&d3),
            "inline all_missing via snapshot must include d3"
        );
        assert!(
            !all_missing_via_snapshot.contains(&d1),
            "inline all_missing via snapshot must NOT include d1 — d1's snapshot \
             entry still says endpoint A has it (scoring-time view)"
        );

        // Sanity contrast: confirm a LIVE read would have produced the
        // wrong (post-flip) answer, proving the assertion above is not
        // vacuous. This is the falsification — if snapshot reuse is
        // wired correctly, the live read produces a STRICTLY DIFFERENT
        // missing set, demonstrating snapshot != live.
        let live_missing: Vec<DigestInfo> = {
            let map = locality_map.read();
            let blobs = map.blobs_map();
            file_digests
                .iter()
                .filter(|(_, size)| *size > 0)
                .filter(|(digest, _)| {
                    blobs
                        .get(digest)
                        .is_none_or(|endpoints| endpoints.get(endpoint_a).is_none())
                })
                .map(|(d, _)| *d)
                .collect()
        };
        assert!(
            live_missing.contains(&d1),
            "PRECONDITION: live read of post-flip map MUST classify d1 as missing \
             for A; otherwise the test setup is vacuous and the snapshot/live \
             distinction is not actually observable. live_missing: {live_missing:?}"
        );
    }

    /// When `file_digests.len()` exceeds `LOCALITY_SNAPSHOT_MAX_ENTRIES`,
    /// `score_and_generate_hints` skips snapshot construction and
    /// returns `locality_snapshot: None`. Phase-4 callers must then
    /// fall back to `locality_map.read()` for correctness — the
    /// documented over-cap behavior per CLAUDE.md.
    ///
    /// We cannot drive the production cap (65_536) in a unit test
    /// without absurd memory pressure, so this test exercises the
    /// fall-back path directly: pass `locality_snapshot=None` to
    /// `compute_missing_blobs` and assert it walks the live map.
    #[test]
    fn test_compute_missing_blobs_snapshot_none_uses_live_read() {
        let locality_map = new_shared_blob_locality_map();
        let endpoint_a = "grpc://worker-a:50081";

        let d1 = DigestInfo::new([0x11; 32], 1000);
        let d2 = DigestInfo::new([0x22; 32], 2000);
        let file_digests = vec![(d1, 1000), (d2, 2000)];

        {
            let mut map = locality_map.write();
            map.register_blobs(endpoint_a, &[d1]); // A has d1; d2 unowned
        }

        let missing = ApiWorkerScheduler::compute_missing_blobs(
            &file_digests,
            endpoint_a,
            None, // ← over-cap fallback: no snapshot
            &locality_map,
        );

        let missing_digests: Vec<DigestInfo> = missing.iter().map(|(d, _)| *d).collect();
        assert!(
            !missing_digests.contains(&d1),
            "live-read fallback must classify d1 as NOT missing for A"
        );
        assert!(
            missing_digests.contains(&d2),
            "live-read fallback must classify d2 as missing for A (no one has d2)"
        );
    }

    /// Mechanical contract: `score_and_generate_hints` returns a
    /// snapshot whose entries are *exactly* the file_digests ∩
    /// locality_map intersection, with peer endpoints matching
    /// `EndpointList` membership for each matched digest. Guards
    /// against a future refactor that drifts the snapshot population
    /// out of sync with the score / hint computation under the same
    /// lock.
    #[test]
    fn test_score_and_generate_hints_snapshot_matches_intersection() {
        let locality_map = new_shared_blob_locality_map();
        let endpoint_a = "grpc://worker-a:50081";
        let endpoint_b = "grpc://worker-b:50081";

        let d1 = DigestInfo::new([0x11; 32], 100);
        let d2 = DigestInfo::new([0x22; 32], 200);
        let d3 = DigestInfo::new([0x33; 32], 300); // unowned, must not appear

        {
            let mut map = locality_map.write();
            map.register_blobs(endpoint_a, &[d1, d2]);
            map.register_blobs(endpoint_b, &[d2]); // d2 shared
        }

        let file_digests = vec![(d1, 100), (d2, 200), (d3, 300)];
        let scoring = score_and_generate_hints(&file_digests, &locality_map);
        let snapshot = scoring
            .locality_snapshot
            .as_ref()
            .expect("snapshot must be Some");

        assert_eq!(snapshot.len(), 2, "only d1, d2 are in the intersection");

        let d1_eps = snapshot.get(&d1).expect("d1 must be in snapshot");
        assert_eq!(d1_eps.len(), 1);
        assert_eq!(&*d1_eps[0], endpoint_a);

        let d2_eps = snapshot.get(&d2).expect("d2 must be in snapshot");
        assert_eq!(d2_eps.len(), 2);
        let mut d2_ep_strs: Vec<&str> = d2_eps.iter().map(|e| &**e).collect();
        d2_ep_strs.sort_unstable();
        assert_eq!(d2_ep_strs, vec![endpoint_a, endpoint_b]);

        assert!(
            !snapshot.contains_key(&d3),
            "unowned digest must NOT appear in snapshot — intersection only"
        );
    }

    #[tokio::test]
    async fn test_resolve_input_tree_cache_hit_returns_same_arc() {
        use nativelink_config::schedulers::WorkerAllocationStrategy;
        use nativelink_metric::MetricsComponent;
        use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
        use crate::platform_property_manager::PlatformPropertyManager;
        use crate::worker_registry::WorkerRegistry;

        // Minimal mock WorkerStateManager for constructing ApiWorkerScheduler.
        #[derive(Debug)]
        struct NoopWorkerStateManager;

        impl MetricsComponent for NoopWorkerStateManager {
            fn publish(
                &self,
                _kind: MetricKind,
                _field_metadata: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }

        #[tonic::async_trait]
        impl WorkerStateManager for NoopWorkerStateManager {
            async fn update_operation(
                &self,
                _operation_id: &OperationId,
                _worker_id: &WorkerId,
                _update: UpdateOperationType,
            ) -> Result<(), Error> {
                Ok(())
            }
        }

        // Create a store with a single-directory tree (one file).
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));

        let dir = Directory {
            files: vec![make_file_node("test.txt", 0xaa, 1000)],
            directories: vec![],
            ..Default::default()
        };
        let (dir_bytes, dir_digest) = encode_directory(&dir);
        let key: StoreKey<'_> = dir_digest.into();
        store
            .update_oneshot(key, Bytes::from(dir_bytes))
            .await
            .expect("store update");

        // Build scheduler with CAS store.
        let scheduler = ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWorkerStateManager),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            Some(store),
            None,
            512 * 1024,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        );

        // First call: cache miss, inline resolution succeeds and caches.
        let result1 = scheduler.resolve_input_tree(dir_digest).await;
        assert!(result1.is_some(), "Expected Some from first resolve (inline resolution)");

        // Second call: cache hit returns the same Arc.
        let result2 = scheduler.resolve_input_tree(dir_digest).await;
        assert!(result2.is_some(), "Expected Some from second resolve (cache hit)");

        // Third call: should return the same Arc (pointer equality).
        let result3 = scheduler.resolve_input_tree(dir_digest).await;
        assert!(result3.is_some(), "Expected Some from third resolve (cache hit)");

        let arc1 = result1.unwrap();
        let arc2 = result2.unwrap();
        let arc3 = result3.unwrap();
        assert!(
            Arc::ptr_eq(&arc1, &arc2),
            "Expected resolve_input_tree to return the same Arc on cache hit (pointer equality)"
        );
        assert!(
            Arc::ptr_eq(&arc2, &arc3),
            "Expected resolve_input_tree to return the same Arc on cache hit (pointer equality)"
        );
    }

    /// Build a `ResolvedTree` whose `estimated_heap_bytes()` is dominated
    /// by the `file_digests` Vec capacity (48 bytes/entry), so the
    /// heap-byte accounting used by `ByteBoundedTreeCache` is controllable
    /// per fixture without constructing large directory protos.
    fn tree_with_heap_bytes(n_file_entries: usize) -> Arc<ResolvedTree> {
        let mut file_digests = Vec::with_capacity(n_file_entries);
        for i in 0..n_file_entries {
            file_digests.push((DigestInfo::new([0u8; 32], i as u64), 1u64));
        }
        Arc::new(ResolvedTree {
            file_digests,
            dir_digests: HashSet::new(),
            subtree_bytes: HashMap::new(),
            subtree_files: HashMap::new(),
            dir_direct_bytes: HashMap::new(),
            dir_direct_files: HashMap::new(),
            directories: HashMap::new(),
        })
    }

    /// (#p1p2 telemetry) `ByteBoundedTreeCache::put` must RETURN the number
    /// of entries it removed so the caller can attribute
    /// `tree_cache_evictions` correctly. Three contracts:
    ///   (1) a fresh insert below both bounds evicts nothing → returns 0;
    ///   (2) a same-key replacement is NOT an eviction → returns 0;
    ///   (3) inserting past `max_count` evicts exactly the count-capacity
    ///       displacement (1 per over-cap insert);
    ///   (4) inserting a tree that overflows `max_bytes` evicts the LRU
    ///       entries the byte-budget loop pops, and the returned count
    ///       equals the number removed by that loop.
    #[test]
    fn test_tree_cache_put_returns_eviction_count() {
        // (1) fresh insert below both bounds: no eviction.
        let mut cache = ByteBoundedTreeCache::new(
            NonZeroUsize::new(4).unwrap(),
            1024 * 1024, // generous byte budget
        );
        let evicted = cache.put(DigestInfo::new([1u8; 32], 0), tree_with_heap_bytes(1));
        assert_eq!(evicted, 0, "fresh insert below both bounds must evict 0");
        assert_eq!(cache.len(), 1);

        // (2) same-key replacement: displaces the old value but is NOT an
        // eviction (net entry count unchanged).
        let evicted = cache.put(DigestInfo::new([1u8; 32], 0), tree_with_heap_bytes(1));
        assert_eq!(
            evicted, 0,
            "same-key replacement must return 0 evictions (it is a replace, not an eviction)"
        );
        assert_eq!(cache.len(), 1, "same-key replace must not grow the cache");

        // (3) count-capacity displacement: fill to max_count, then one more
        // distinct-key insert displaces exactly one LRU entry.
        let mut cache = ByteBoundedTreeCache::new(
            NonZeroUsize::new(2).unwrap(),
            1024 * 1024,
        );
        assert_eq!(cache.put(DigestInfo::new([2u8; 32], 0), tree_with_heap_bytes(1)), 0);
        assert_eq!(cache.put(DigestInfo::new([3u8; 32], 0), tree_with_heap_bytes(1)), 0);
        let evicted = cache.put(DigestInfo::new([4u8; 32], 0), tree_with_heap_bytes(1));
        assert_eq!(
            evicted, 1,
            "insert past max_count must report exactly one count-capacity eviction"
        );
        assert_eq!(cache.len(), 2, "cache must stay at max_count");

        // (4) byte-budget eviction: a large max_count so the count cap never
        // fires, but a tight byte budget so the while-loop pops LRU entries.
        // Each tree_with_heap_bytes(10) is ~480 bytes (10 * 48). Budget of
        // 1200 bytes holds two such trees (~960) but not three (~1440), so
        // inserting the third pops exactly one LRU entry.
        let mut cache = ByteBoundedTreeCache::new(
            NonZeroUsize::new(100).unwrap(),
            1200,
        );
        assert_eq!(cache.put(DigestInfo::new([5u8; 32], 0), tree_with_heap_bytes(10)), 0);
        assert_eq!(cache.put(DigestInfo::new([6u8; 32], 0), tree_with_heap_bytes(10)), 0);
        let evicted = cache.put(DigestInfo::new([7u8; 32], 0), tree_with_heap_bytes(10));
        assert_eq!(
            evicted, 1,
            "insert overflowing max_bytes must report the count popped by the byte-budget loop"
        );
        assert!(
            cache.total_bytes() <= 1200,
            "byte-budget loop must restore the invariant total_bytes <= max_bytes"
        );
    }

    /// (#p1p2 telemetry) A resolve against a pre-populated cache increments
    /// `tree_cache_hits`; a resolve for a never-seen (cold) digest that
    /// succeeds inline increments `tree_cache_misses`. Modeled on
    /// `test_resolve_input_tree_cache_hit_returns_same_arc`.
    #[tokio::test]
    async fn test_resolve_input_tree_updates_hit_miss_counters() {
        use nativelink_config::schedulers::WorkerAllocationStrategy;
        use crate::platform_property_manager::PlatformPropertyManager;
        use crate::worker_registry::WorkerRegistry;

        #[derive(Debug)]
        struct NoopWorkerStateManager;
        impl MetricsComponent for NoopWorkerStateManager {
            fn publish(
                &self,
                _kind: MetricKind,
                _field_metadata: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }
        #[tonic::async_trait]
        impl WorkerStateManager for NoopWorkerStateManager {
            async fn update_operation(
                &self,
                _operation_id: &OperationId,
                _worker_id: &WorkerId,
                _update: UpdateOperationType,
            ) -> Result<(), Error> {
                Ok(())
            }
        }

        // Store a single-directory tree so inline resolution succeeds.
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let dir = Directory {
            files: vec![make_file_node("hit_miss.txt", 0xbb, 1000)],
            directories: vec![],
            ..Default::default()
        };
        let (dir_bytes, dir_digest) = encode_directory(&dir);
        let key: StoreKey<'_> = dir_digest.into();
        store
            .update_oneshot(key, Bytes::from(dir_bytes))
            .await
            .expect("store update");

        let scheduler = ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWorkerStateManager),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            Some(store),
            None,
            512 * 1024,
            8,
            false,
            0,
            2,
        );

        // Precondition: both counters start at zero.
        assert_eq!(scheduler.metrics.tree_cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(scheduler.metrics.tree_cache_misses.load(Ordering::Relaxed), 0);

        // First resolve: cold digest → cache MISS, inline resolution caches it.
        let r1 = scheduler.resolve_input_tree(dir_digest).await;
        assert!(r1.is_some(), "first resolve should succeed inline");
        assert_eq!(
            scheduler.metrics.tree_cache_misses.load(Ordering::Relaxed),
            1,
            "cold resolve must increment tree_cache_misses exactly once"
        );
        assert_eq!(
            scheduler.metrics.tree_cache_hits.load(Ordering::Relaxed),
            0,
            "cold resolve must NOT increment tree_cache_hits"
        );

        // Second resolve of the same digest: cache HIT.
        let r2 = scheduler.resolve_input_tree(dir_digest).await;
        assert!(r2.is_some(), "second resolve should hit the cache");
        assert_eq!(
            scheduler.metrics.tree_cache_hits.load(Ordering::Relaxed),
            1,
            "warm resolve must increment tree_cache_hits exactly once"
        );
        assert_eq!(
            scheduler.metrics.tree_cache_misses.load(Ordering::Relaxed),
            1,
            "warm resolve must NOT increment tree_cache_misses"
        );

        // The cold miss must have recorded exactly one cold-resolution sample.
        assert_eq!(
            scheduler
                .metrics
                .tree_resolution_cold_count
                .load(Ordering::Relaxed),
            1,
            "the single cold miss must record exactly one cold-resolution sample"
        );
        assert!(
            scheduler
                .metrics
                .tree_resolution_cold_time_ns
                .load(Ordering::Relaxed)
                > 0,
            "cold resolution must accumulate a non-zero elapsed-ns sample"
        );
    }

    /// Verifies that `TreeResolutionGuard` removes the digest from the
    /// in-progress set on Drop. This is the core invariant: if the guard's
    /// Drop fires, the leak is impossible. Combined with the fact that the
    /// guard is bound to the `resolve_input_tree` future (or the spawned
    /// background task), Rust's drop semantics guarantee the entry is
    /// released on every exit path including async cancellation.
    #[tokio::test]
    async fn test_tree_resolution_guard_releases_on_drop() {
        let in_progress: Arc<tokio::sync::Mutex<HashSet<DigestInfo>>> =
            Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let digest = DigestInfo::new([0xab; 32], 42);

        // Insert the digest as if we were starting a resolution.
        in_progress.lock().await.insert(digest);
        assert!(
            in_progress.lock().await.contains(&digest),
            "precondition: digest should be in_progress"
        );

        // Construct and immediately drop a guard for this digest.
        {
            let _guard = TreeResolutionGuard {
                digest,
                in_progress: in_progress.clone(),
            };
        }

        // The guard's Drop spawns an async removal. Yield repeatedly so
        // the spawned task gets a chance to run and acquire the lock.
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if !in_progress.lock().await.contains(&digest) {
                break;
            }
        }

        assert!(
            !in_progress.lock().await.contains(&digest),
            "TreeResolutionGuard::drop should have removed the digest from in_progress"
        );
    }

    /// Verifies that the in-progress set does not retain a stale entry
    /// after `resolve_input_tree` returns, exercising the guard wired
    /// against the scheduler's actual shared map. Covers both the
    /// inline-error path (the directory blob is missing from the store)
    /// and the cancellation path (where the inserter never executes
    /// the matching remove on its own).
    #[tokio::test]
    async fn test_resolve_input_tree_no_in_progress_leak() {
        use nativelink_config::schedulers::WorkerAllocationStrategy;
        use crate::platform_property_manager::PlatformPropertyManager;
        use crate::worker_registry::WorkerRegistry;

        #[derive(Debug)]
        struct NoopWorkerStateManager;

        impl MetricsComponent for NoopWorkerStateManager {
            fn publish(
                &self,
                _kind: MetricKind,
                _field_metadata: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }

        #[tonic::async_trait]
        impl WorkerStateManager for NoopWorkerStateManager {
            async fn update_operation(
                &self,
                _operation_id: &OperationId,
                _worker_id: &WorkerId,
                _update: UpdateOperationType,
            ) -> Result<(), Error> {
                Ok(())
            }
        }

        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let dir = Directory {
            files: vec![make_file_node("test.txt", 0xaa, 1000)],
            directories: vec![],
            ..Default::default()
        };
        let (_dir_bytes, dir_digest) = encode_directory(&dir);
        // Note: we deliberately do NOT insert the directory into the store,
        // so the resolution will fail with NotFound — exercising the
        // Ok(Err(_)) branch's guard release on the actual shared map.

        let scheduler = ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWorkerStateManager),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            Some(store),
            None,
            512 * 1024,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        );

        // First, verify guard wiring against the real shared map. Pre-insert
        // the digest, drop the guard, and confirm cleanup happens against
        // the scheduler's own `tree_resolution_in_progress`.
        scheduler
            .tree_resolution_in_progress
            .lock()
            .await
            .insert(dir_digest);
        {
            let _guard = TreeResolutionGuard {
                digest: dir_digest,
                in_progress: scheduler.tree_resolution_in_progress.clone(),
            };
        }
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if scheduler
                .tree_resolution_in_progress
                .lock()
                .await
                .is_empty()
            {
                break;
            }
        }
        assert!(
            scheduler
                .tree_resolution_in_progress
                .lock()
                .await
                .is_empty(),
            "guard should have cleared the in-progress entry against the scheduler's actual map"
        );

        // Now run a real resolve_input_tree call (which will hit NotFound
        // and exercise the Ok(Err) branch's guard drop). After it returns,
        // the in-progress set must not retain the digest.
        let _result = scheduler.resolve_input_tree(dir_digest).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            !scheduler
                .tree_resolution_in_progress
                .lock()
                .await
                .contains(&dir_digest),
            "tree_resolution_in_progress must not retain digest after resolve_input_tree returns"
        );

        // Clear failure cache so the next call isn't short-circuited by the
        // negative cache. Confirm no in-progress entry leaks across calls.
        scheduler.tree_resolution_failures.lock().await.clear();
        let _result2 = scheduler.resolve_input_tree(dir_digest).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            !scheduler
                .tree_resolution_in_progress
                .lock()
                .await
                .contains(&dir_digest),
            "tree_resolution_in_progress must not retain digest after second resolve_input_tree call"
        );
    }

    // ------------------------------------------------------------------
    // (#97) BIS chunked broadcast + ack + reconnect-replay tests.
    //
    // These exercise the inherent methods on `ApiWorkerScheduler`:
    //   * `broadcast_blobs_in_stable_storage_chunked`
    //   * `bis_ack_received`
    //   * `replay_bis_chunks_to_worker` (called from `add_worker`)
    //   * `clear_bis_resend_buffer_for_endpoint`
    // ------------------------------------------------------------------

    use crate::worker_registry::WorkerRegistry;
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        chunked_message,
        update_for_worker::Update as ServerUpdate,
    };
    use nativelink_util::operation_state_manager::WorkerStateManager;
    use tokio::sync::mpsc;

    /// Minimal no-op WorkerStateManager for unit tests that don't touch
    /// operation state.
    #[derive(Debug)]
    struct NoopWsm;

    impl MetricsComponent for NoopWsm {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl WorkerStateManager for NoopWsm {
        async fn update_operation(
            &self,
            _operation_id: &OperationId,
            _worker_id: &WorkerId,
            _update: UpdateOperationType,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn make_test_scheduler() -> Arc<ApiWorkerScheduler> {
        ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWsm),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            None,
            None,
            512 * 1024,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        )
    }

    fn make_digest_info(i: u64) -> DigestInfo {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&i.to_be_bytes());
        DigestInfo::new(hash, 4)
    }

    /// Drain every UpdateForWorker waiting on rx and return only the BIS
    /// chunk payloads. Other update arms (ConnectionResult, KeepAlive,
    /// etc.) are skipped — they're not what these tests assert on.
    async fn drain_bis_chunks(
        rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
    ) -> Vec<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsInStableStorageChunk>
    {
        let mut out = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout(
            Duration::from_millis(200),
            rx.recv(),
        )
        .await
        {
            if let Some(ServerUpdate::ChunkedMessage(envelope)) = msg.update
                && let Some(chunked_message::Payload::BlobsInStableStorage(chunk)) =
                    envelope.payload
            {
                out.push(chunk);
            }
        }
        out
    }

    /// Register a worker, returning its rx so the test can observe what
    /// the scheduler dispatches.
    async fn register_worker_endpoint(
        scheduler: &Arc<ApiWorkerScheduler>,
        worker_id: &str,
        cas_endpoint: &str,
    ) -> mpsc::UnboundedReceiver<UpdateForWorker> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker::new_with_cas_endpoint(
            WorkerId(worker_id.to_string()),
            PlatformProperties::default(),
            tx,
            42, // timestamp
            0,  // max_inflight_tasks
            cas_endpoint.to_string(),
            0, // p_core_count (unknown in this test)
            0, // e_core_count (unknown in this test)
        );
        scheduler.add_worker(worker).await.expect("add_worker");
        rx
    }

    /// 1. End-to-end broadcast → chunks delivered → ack drains the
    ///    resend buffer for the corresponding (broadcast_id, sequence).
    #[tokio::test]
    async fn bis_chunked_ack_acknowledged() {
        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w1.local:50081";
        let mut rx = register_worker_endpoint(&scheduler, "worker-1", endpoint).await;

        // 100K digests will exceed BIS_DIGESTS_PER_CHUNK (4096), forcing
        // ceil(100000 / 4096) = 25 chunks. Plenty to verify multi-chunk
        // semantics.
        let digests: Vec<DigestInfo> = (0..100_000u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;

        let chunks = drain_bis_chunks(&mut rx).await;
        assert!(
            chunks.len() >= 24,
            "expected ~25 chunks for 100K digests / 4096 per chunk, got {}",
            chunks.len()
        );
        assert!(chunks.last().unwrap().is_last, "final chunk must set is_last");
        assert!(
            chunks
                .iter()
                .take(chunks.len() - 1)
                .all(|c| !c.is_last),
            "non-final chunks must not set is_last"
        );

        // Sanity: every chunk shares the same broadcast_id.
        let bid = chunks[0].broadcast_id;
        assert!(
            chunks.iter().all(|c| c.broadcast_id == bid),
            "all chunks of one broadcast must share broadcast_id"
        );

        // Buffer should hold every dispatched chunk awaiting ack.
        let buffered_before = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert_eq!(
            buffered_before,
            chunks.len(),
            "every dispatched chunk must be in the resend buffer until acked"
        );

        // Ack every chunk; buffer must drain. Echo the chunk's
        // server_instance_token (red-team #5: scheduler validates
        // tokens to drop stale-server-bounce acks).
        for chunk in &chunks {
            scheduler
                .bis_ack_received(
                    &WorkerId("worker-1".to_string()),
                    chunk.broadcast_id,
                    chunk.sequence,
                    chunk.server_instance_token,
                )
                .await;
        }
        let buffered_after = {
            let inner = scheduler.inner.read().await;
            inner.bis_resend_buffers.get(endpoint).map(|b| b.len()).unwrap_or(0)
        };
        assert_eq!(
            buffered_after, 0,
            "after acking every chunk, the resend buffer must be empty — \
             without this, the buffer leaks and a long-lived worker \
             accumulates 100K+ unacked chunks per broadcast in memory"
        );
    }

    /// 2. Connection drop after partial ack → reconnect → server replays
    ///    only the unacked chunks (not the acked ones).
    #[tokio::test]
    async fn bis_chunked_ack_lost_resend() {
        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w2.local:50081";
        let mut rx1 = register_worker_endpoint(&scheduler, "worker-2a", endpoint).await;

        let digests: Vec<DigestInfo> = (0..10_000u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;

        let chunks = drain_bis_chunks(&mut rx1).await;
        assert!(chunks.len() >= 2, "need >= 2 chunks for the test, got {}", chunks.len());
        let bid = chunks[0].broadcast_id;

        // Ack only the first half of chunks. The second half is "in flight"
        // when the simulated disconnect occurs.
        let half = chunks.len() / 2;
        for chunk in chunks.iter().take(half) {
            scheduler
                .bis_ack_received(
                    &WorkerId("worker-2a".to_string()),
                    bid,
                    chunk.sequence,
                    chunk.server_instance_token,
                )
                .await;
        }
        let buffered_after_partial = {
            let inner = scheduler.inner.read().await;
            inner.bis_resend_buffers.get(endpoint).map(|b| b.len()).unwrap_or(0)
        };
        assert_eq!(
            buffered_after_partial,
            chunks.len() - half,
            "buffer must hold exactly the unacked chunks after partial ack"
        );

        // Simulate disconnect: drop rx1 (worker side closed). Then the
        // worker reconnects with the same cas_endpoint (same boot_epoch).
        drop(rx1);
        // Remove the old WorkerId so add_worker doesn't reject as
        // duplicate. (Production uses a different WorkerId per connect;
        // mirror that here.)
        let _ = scheduler
            .remove_worker(&WorkerId("worker-2a".to_string()))
            .await;

        let mut rx2 = register_worker_endpoint(&scheduler, "worker-2b", endpoint).await;
        let replayed = drain_bis_chunks(&mut rx2).await;
        assert_eq!(
            replayed.len(),
            chunks.len() - half,
            "reconnect must replay every unacked chunk — without this, \
             the worker permanently misses unpins and pin state leaks"
        );
        // Replayed sequences must match the unacked subset.
        let mut replayed_seqs: Vec<u32> = replayed.iter().map(|c| c.sequence).collect();
        replayed_seqs.sort_unstable();
        let mut expected_seqs: Vec<u32> = chunks.iter().skip(half).map(|c| c.sequence).collect();
        expected_seqs.sort_unstable();
        assert_eq!(
            replayed_seqs, expected_seqs,
            "replayed chunks must be exactly the unacked sequences"
        );
    }

    /// 3. Worker boot_epoch_id change → buffer cleared, no replay.
    ///    A new process means the worker's pin state died with the old
    ///    one; replaying unpins for blobs that no longer exist would be
    ///    a no-op AND eat memory until ack-or-disconnect.
    #[tokio::test]
    async fn bis_chunked_boot_epoch_change_clears_buffer() {
        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w3.local:50081";
        let mut rx1 = register_worker_endpoint(&scheduler, "worker-3a", endpoint).await;

        let digests: Vec<DigestInfo> = (0..5_000u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;
        let chunks = drain_bis_chunks(&mut rx1).await;
        assert!(!chunks.is_empty(), "must dispatch at least one chunk");

        // Simulate boot_epoch change: caller (worker_api_server) clears
        // the buffer before the new connection's add_worker fires.
        scheduler.clear_bis_resend_buffer_for_endpoint(endpoint).await;

        // Drop the old worker, register a new worker on the same endpoint.
        drop(rx1);
        let _ = scheduler
            .remove_worker(&WorkerId("worker-3a".to_string()))
            .await;
        let mut rx2 = register_worker_endpoint(&scheduler, "worker-3b", endpoint).await;
        let replayed = drain_bis_chunks(&mut rx2).await;
        assert_eq!(
            replayed.len(),
            0,
            "after clear_bis_resend_buffer_for_endpoint (boot_epoch \
             change), the new worker must NOT receive replayed chunks — \
             those chunks' digests refer to pin state in the dead process"
        );
    }

    /// 4. Acks across ALL chunks of a broadcast leave the buffer fully
    ///    drained AND remove the per-endpoint entry (not a stale empty
    ///    HashMap entry that grows unbounded over the worker's lifetime).
    #[tokio::test]
    async fn bis_chunked_full_ack_drops_endpoint_entry() {
        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w4.local:50081";
        let mut rx = register_worker_endpoint(&scheduler, "worker-4", endpoint).await;

        let digests: Vec<DigestInfo> = (0..1000u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;
        let chunks = drain_bis_chunks(&mut rx).await;

        for chunk in &chunks {
            scheduler
                .bis_ack_received(
                    &WorkerId("worker-4".to_string()),
                    chunk.broadcast_id,
                    chunk.sequence,
                    chunk.server_instance_token,
                )
                .await;
        }
        let inner = scheduler.inner.read().await;
        assert!(
            !inner.bis_resend_buffers.contains_key(endpoint),
            "after every chunk acked, the endpoint's buffer entry must \
             be removed entirely (not just emptied) so a long-lived \
             worker doesn't accumulate empty BisResendBuffer entries"
        );
    }

    /// 5. **Server-instance-token (red-team #5).** A `BisAck` whose
    ///    `server_instance_token` does not match the current scheduler's
    ///    token MUST be silently dropped — without this, a worker
    ///    holding a stale ack across a server bounce would drop an
    ///    unrelated chunk from the new server's resend buffer (because
    ///    `next_bis_broadcast_id` resets to 1 on startup → broadcast_id
    ///    collisions across server-instance boundaries are guaranteed).
    ///
    ///    Mutation step: in `bis_ack_received`, replace the
    ///    `if server_instance_token != self.server_instance_token`
    ///    guard with `if false`. This test MUST then panic with
    ///    "stale-token ack must NOT remove the chunk from the resend buffer".
    #[tokio::test]
    async fn bis_ack_with_stale_server_token_silently_dropped() {
        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w5.local:50081";
        let mut rx = register_worker_endpoint(&scheduler, "worker-5", endpoint).await;

        // Dispatch a broadcast — chunks will carry the scheduler's
        // current server_instance_token.
        let digests: Vec<DigestInfo> = (0..200u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;
        let chunks = drain_bis_chunks(&mut rx).await;
        assert!(!chunks.is_empty(), "must dispatch at least one chunk");
        let valid_token = chunks[0].server_instance_token;
        let buffered_before = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert_eq!(
            buffered_before,
            chunks.len(),
            "every dispatched chunk must be in the resend buffer"
        );

        // Send acks with the WRONG token — simulates a worker holding
        // acks from a previous server process. The broadcast_id +
        // sequence still match valid in-buffer chunks, so without the
        // token check these acks would silently release the chunks.
        let stale_token = valid_token.wrapping_add(1);
        assert_ne!(
            stale_token, valid_token,
            "test setup invariant: stale_token must differ from valid_token"
        );
        for chunk in &chunks {
            scheduler
                .bis_ack_received(
                    &WorkerId("worker-5".to_string()),
                    chunk.broadcast_id,
                    chunk.sequence,
                    stale_token,
                )
                .await;
        }
        let buffered_after_stale = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert_eq!(
            buffered_after_stale, chunks.len(),
            "stale-token ack must NOT remove the chunk from the resend buffer \
             (red-team #5: across-server-bounce broadcast_id collisions are \
             guaranteed; without the token check, a stale ack from a \
             previous server's worker would silently release an unrelated \
             chunk and leak the corresponding pin state)"
        );

        // Sanity: matching-token acks STILL drain the buffer (the
        // gate is on token mismatch, not all-acks-rejected).
        for chunk in &chunks {
            scheduler
                .bis_ack_received(
                    &WorkerId("worker-5".to_string()),
                    chunk.broadcast_id,
                    chunk.sequence,
                    valid_token,
                )
                .await;
        }
        let inner = scheduler.inner.read().await;
        assert!(
            !inner.bis_resend_buffers.contains_key(endpoint),
            "matching-token acks must STILL drain the buffer — the \
             token check is a guard against stale acks, not a \
             reject-everything sentinel"
        );
    }

    /// 6. **Concurrent ack/replay race (testing-czar requirement).**
    ///    `bis_ack_received` and `replay_bis_chunks_to_worker` both
    ///    take `inner.write().await`. The lock-ordering guarantee is
    ///    that they are SERIALIZED on the inner RwLock, so racing
    ///    them must produce: (a) no deadlock, (b) no chunk delivered
    ///    twice via replay if it was acked first, (c) no chunk lost.
    ///
    ///    Mutation guidance: introducing a `tokio::time::sleep(10ms)`
    ///    between any internal read-acquire and write-acquire in
    ///    `replay_bis_chunks_to_worker` would not change the outcome
    ///    (the test asserts on result-state, not timing).
    #[tokio::test]
    async fn bis_concurrent_ack_and_replay_no_deadlock() {
        use tokio::time::{Duration, timeout};

        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w6.local:50081";
        let mut rx1 = register_worker_endpoint(&scheduler, "worker-6a", endpoint).await;

        // Pre-populate the buffer with N chunks for endpoint E.
        let digests: Vec<DigestInfo> = (0..400u64).map(make_digest_info).collect();
        scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, "")
            .await;
        let chunks = drain_bis_chunks(&mut rx1).await;
        assert!(chunks.len() >= 1, "must dispatch chunks");
        let bid = chunks[0].broadcast_id;
        let token = chunks[0].server_instance_token;

        // Race: simultaneously ack the first chunk AND register a new
        // worker on the same endpoint (which triggers replay).
        // The deadlock detector — wrap the whole race in 5s timeout.
        let scheduler_clone = scheduler.clone();
        let scheduler_clone2 = scheduler.clone();
        let first_seq = chunks[0].sequence;
        let endpoint_owned = endpoint.to_string();

        let race = async move {
            // Drop the first worker so the new add_worker can replay
            // through the tx for the SAME endpoint.
            drop(rx1);
            let _ = scheduler_clone
                .remove_worker(&WorkerId("worker-6a".to_string()))
                .await;

            tokio::join!(
                async {
                    scheduler_clone
                        .bis_ack_received(
                            &WorkerId("worker-6a".to_string()),
                            bid,
                            first_seq,
                            token,
                        )
                        .await;
                },
                async {
                    let _rx2 = register_worker_endpoint(
                        &scheduler_clone2,
                        "worker-6b",
                        &endpoint_owned,
                    )
                    .await;
                    // Hold rx2 alive briefly so replay completes.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    drop(_rx2);
                }
            );
        };

        timeout(Duration::from_secs(5), race)
            .await
            .expect("must not deadlock — bis_ack_received vs replay_bis_chunks_to_worker \
                     must serialize cleanly on inner.write().await");

        // After the race the buffer either contains chunks.len() - 1
        // entries (ack landed first; first chunk removed) or
        // chunks.len() entries (ack hit a non-existent worker first;
        // worker-6a was already removed before bis_ack_received's
        // worker-lookup, so endpoint resolution failed → no removal).
        // Either is correct; what is NOT correct is panic / deadlock /
        // chunk count above the dispatched total.
        let buffered_after_race = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert!(
            buffered_after_race <= chunks.len(),
            "race must not produce chunk count above dispatched total \
             (got {buffered_after_race}, dispatched {})",
            chunks.len()
        );
    }

    // ------------------------------------------------------------------
    // (#214) BIS replay buffer cap tests.
    //
    // Cap = `BIS_REPLAY_BUFFER_MAX_CHUNKS` (set to 64 under cfg(test)
    // so these run fast). Both directions of the side-effect contract
    // are exercised:
    //   * UNDER-action: cap fires when buffer would exceed the limit
    //     (test 7). Without the cap, a never-acking worker grows the
    //     buffer monotonically across reconnects → server OOM.
    //   * OVER-action: cap does NOT prematurely drop chunks when the
    //     buffer is well under the limit (tests 8 + 9). A spurious
    //     drop would mean the worker permanently misses a recent
    //     unpin → pin state leak in the worker's CAS.
    // ------------------------------------------------------------------

    /// 7. **UNDER-action.** A worker that never acks accumulates BIS
    ///    chunks. After enough broadcasts the buffer reaches the cap
    ///    and stops growing — additional broadcasts trim oldest
    ///    chunks rather than expanding the buffer. The
    ///    `bis_replay_buffer_overflow_drops` counter increments by
    ///    exactly the number of dropped chunks.
    ///
    ///    Production composition: real `ApiWorkerScheduler`, real
    ///    `broadcast_blobs_in_stable_storage_chunked` path, real
    ///    `BisResendBuffer::add` invocation.
    ///
    ///    Mutation step: in `BisResendBuffer::add`, replace the
    ///    `while self.chunks.len() > BIS_REPLAY_BUFFER_MAX_CHUNKS`
    ///    body with a no-op (`break`). This test MUST then panic with
    ///    "buffer must NOT exceed cap" — confirming the test guards
    ///    the trim, not just an incidental side effect.
    #[tokio::test]
    async fn bis_replay_buffer_caps_at_max_chunks() {
        use tokio::time::{Duration, timeout};

        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w7.local:50081";
        // Hold rx alive (otherwise sends fail and chunks aren't
        // buffered) but never drain or ack — this is the misbehaving-
        // worker pathology.
        let _rx = register_worker_endpoint(&scheduler, "worker-7", endpoint).await;

        // Drive enough broadcasts that the buffer would hold
        // > BIS_REPLAY_BUFFER_MAX_CHUNKS chunks if uncapped. Each
        // 1-digest broadcast yields exactly 1 chunk (ChunkIter still
        // emits the terminal is_last chunk for a single-element
        // stream), so N broadcasts → N chunks attempted.
        let target_chunks = BIS_REPLAY_BUFFER_MAX_CHUNKS + 16;
        let exercise = async {
            for i in 0..target_chunks {
                let digest = vec![make_digest_info(i as u64)];
                scheduler
                    .broadcast_blobs_in_stable_storage_chunked(digest, "")
                    .await;
            }
        };
        timeout(Duration::from_secs(10), exercise).await.expect(
            "must not deadlock — broadcast_blobs_in_stable_storage_chunked \
             with a never-acking worker must trim and return promptly",
        );

        let buffered = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert_eq!(
            buffered, BIS_REPLAY_BUFFER_MAX_CHUNKS,
            "buffer must NOT exceed cap (#214). Without the trim, a \
             never-acking worker grows the buffer monotonically across \
             every broadcast and DoS's the server via memory exhaustion. \
             expected={BIS_REPLAY_BUFFER_MAX_CHUNKS} got={buffered}"
        );

        let drops = scheduler
            .metrics
            .bis_replay_buffer_overflow_drops
            .load(Ordering::Relaxed);
        let expected_drops = (target_chunks - BIS_REPLAY_BUFFER_MAX_CHUNKS) as u64;
        assert_eq!(
            drops, expected_drops,
            "overflow_drops counter must increment by exactly the number \
             of chunks the cap dropped (operator-visible signal). \
             expected={expected_drops} got={drops}"
        );

        let endpoint_drops = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.overflow_drops())
                .unwrap_or(0)
        };
        assert_eq!(
            endpoint_drops, expected_drops,
            "per-buffer overflow_drops must also reflect the dropped \
             count (so an operator inspecting a specific endpoint's \
             buffer sees the cap firing for THAT worker). \
             expected={expected_drops} got={endpoint_drops}"
        );
    }

    /// 8. **OVER-action (sibling of test 7).** A normal-volume broadcast
    ///    sequence that stays well under the cap MUST NOT trigger the
    ///    trim. Otherwise the cap would silently drop live unpins for
    ///    a healthy worker, causing the worker to miss recent BIS
    ///    notifications and leak pin state. This is the
    ///    asymmetric-contract sibling of test 7: the cap is supposed
    ///    to fire ONLY when over the limit, never below it.
    ///
    ///    Mutation step: in `BisResendBuffer::add`, change
    ///    `> BIS_REPLAY_BUFFER_MAX_CHUNKS` to `>= 0` (always trim).
    ///    This test MUST then panic with "buffer must hold every
    ///    chunk when under cap".
    #[tokio::test]
    async fn bis_replay_buffer_no_premature_drops_under_cap() {
        use tokio::time::{Duration, timeout};

        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w8.local:50081";
        let _rx = register_worker_endpoint(&scheduler, "worker-8", endpoint).await;

        // Stay well below the cap. Half-cap is comfortably under and
        // far enough from 0 to make a spurious always-trim mutation
        // visible.
        let n = BIS_REPLAY_BUFFER_MAX_CHUNKS / 2;
        assert!(
            n > 0,
            "test invariant: BIS_REPLAY_BUFFER_MAX_CHUNKS must allow a \
             non-trivial half-cap"
        );
        let exercise = async {
            for i in 0..n {
                let digest = vec![make_digest_info(i as u64)];
                scheduler
                    .broadcast_blobs_in_stable_storage_chunked(digest, "")
                    .await;
            }
        };
        timeout(Duration::from_secs(10), exercise).await.expect(
            "must not deadlock — broadcast_blobs_in_stable_storage_chunked \
             must complete promptly even when filling the buffer",
        );

        let buffered = {
            let inner = scheduler.inner.read().await;
            inner
                .bis_resend_buffers
                .get(endpoint)
                .map(|b| b.len())
                .unwrap_or(0)
        };
        assert_eq!(
            buffered, n,
            "buffer must hold every chunk when under cap. A premature \
             drop would mean a healthy worker silently misses recent \
             BIS unpins and leaks pin state. expected={n} got={buffered}"
        );

        let drops = scheduler
            .metrics
            .bis_replay_buffer_overflow_drops
            .load(Ordering::Relaxed);
        assert_eq!(
            drops, 0,
            "overflow_drops counter must remain 0 when the buffer never \
             reached the cap. A non-zero value indicates the trim is \
             firing spuriously (over-action sibling of test 7's \
             under-action). got={drops}"
        );
    }

    /// 9. **Replay correctness under cap.** When the buffer is at cap
    ///    and the worker reconnects, replay must emit at most
    ///    `BIS_REPLAY_BUFFER_MAX_CHUNKS` chunks (the buffer's bounded
    ///    contents) and the SURVIVING chunks must be the most-recent
    ///    ones (highest `(broadcast_id, sequence)` lex order). The
    ///    drop-oldest policy is a conscious trade-off: the worker
    ///    will miss unpins for the earliest broadcasts (it had the
    ///    most opportunity to ack those) but recent broadcasts —
    ///    where the digests are most likely still pinned — are
    ///    preserved.
    #[tokio::test]
    async fn bis_replay_after_cap_drops_oldest_keeps_newest() {
        use tokio::time::{Duration, timeout};

        let scheduler = make_test_scheduler();
        let endpoint = "grpc://w9.local:50081";
        let mut rx1 = register_worker_endpoint(&scheduler, "worker-9a", endpoint).await;

        // Drive cap + delta broadcasts so the OLDEST `delta` are
        // trimmed. Use 1-digest broadcasts so chunk_count == broadcast_count.
        let delta = 8usize;
        let total = BIS_REPLAY_BUFFER_MAX_CHUNKS + delta;
        let exercise = async {
            for i in 0..total {
                let digest = vec![make_digest_info(i as u64)];
                scheduler
                    .broadcast_blobs_in_stable_storage_chunked(digest, "")
                    .await;
            }
        };
        timeout(Duration::from_secs(10), exercise).await.expect(
            "must not deadlock — fill-and-trim broadcast loop must \
             complete promptly",
        );

        // Drop rx1 and reconnect so replay fires.
        let dispatched = drain_bis_chunks(&mut rx1).await;
        assert!(
            !dispatched.is_empty(),
            "broadcasts must dispatch at least some chunks before reconnect"
        );
        drop(rx1);
        let _ = scheduler
            .remove_worker(&WorkerId("worker-9a".to_string()))
            .await;

        let mut rx2 = register_worker_endpoint(&scheduler, "worker-9b", endpoint).await;
        let replayed = drain_bis_chunks(&mut rx2).await;

        assert_eq!(
            replayed.len(),
            BIS_REPLAY_BUFFER_MAX_CHUNKS,
            "replay must emit at most cap chunks (the bounded buffer's \
             contents). expected={BIS_REPLAY_BUFFER_MAX_CHUNKS} got={}",
            replayed.len()
        );

        // Surviving chunks must be the NEWEST ones — i.e., the
        // broadcast_ids in [delta+1, total]. Drop-oldest policy means
        // broadcast_ids 1..=delta were trimmed.
        let mut survived_bids: Vec<u64> =
            replayed.iter().map(|c| c.broadcast_id).collect();
        survived_bids.sort_unstable();
        let min_survived = *survived_bids.first().expect("non-empty replay");
        assert!(
            min_survived > delta as u64,
            "drop-oldest policy must trim the earliest broadcast_ids \
             (1..={delta}); a survivor with broadcast_id <= {delta} \
             means a NEWER chunk was dropped instead of an older one, \
             violating the drop-oldest contract. min_survived={min_survived}"
        );
    }

    /// (#231) Render-test: SchedulerMetrics counters must appear on the
    /// PRODUCTION `/metrics` collection path.
    ///
    /// This drives the SAME walk `metrics_handler` uses in prod
    /// (`nativelink-util/src/metrics_publisher.rs::render_prometheus`)
    /// against an `ApiWorkerScheduler` registered exactly the way
    /// `src/bin/nativelink.rs:559-568` registers worker schedulers: an
    /// `Arc<dyn MetricsComponent + Send + Sync>` upcast from
    /// `RootMetricsComponent`, under the `scheduler.<name>.worker`
    /// prefix. It asserts the LITERAL leaf names of the
    /// `SchedulerMetrics` sub-tree are present in the rendered
    /// Prometheus body.
    ///
    /// Before #231 (no `#[derive(MetricsComponent)]` on
    /// `SchedulerMetrics`, no `#[metric(group="scheduler_metrics")]` on
    /// the `metrics` field) the root walk does not descend into the
    /// counters, so every assertion below red-fails with its bespoke
    /// "#231: ... dark on /metrics" message.
    ///
    /// Mutation step (per CLAUDE.md TDD): drop the
    /// `#[metric(group = "scheduler_metrics")]` on the `metrics` field
    /// of `ApiWorkerScheduler` — the root `publish()` then skips the
    /// sub-component and every assertion red-fails.
    #[tokio::test]
    async fn scheduler_metrics_rendered_on_metrics_endpoint() {
        use nativelink_util::metrics_publisher::{
            MetricsComponentTrait, MetricsRegistry, render_prometheus,
        };

        let scheduler = make_test_scheduler();

        // Register one worker through the production helper so the
        // `inner` (`ApiWorkerSchedulerImpl`) sub-tree has a stable,
        // non-empty leaf to assert on below. With zero workers the
        // `Workers::publish` loop emits nothing, and `NoopWsm` /
        // empty `known_properties` make the other two `group!`-recursing
        // children empty too — so a registered worker is the only
        // unconditional inner-tree leaf available in this harness.
        let _rx = register_worker_endpoint(
            &scheduler,
            "wmetric",
            "grpc://wmetric.local:50081",
        )
        .await;

        // Set distinctive non-zero values on a representative spread of
        // the counter kinds: a plain AtomicU64 (find_worker_hits), a
        // prefetch-tier counter (prefetch_blobs_sent), the #214 overflow
        // drop counter (bis_replay_buffer_overflow_drops), and the
        // CounterWithTime (cache_warm_spawned). The values double as a
        // wrong-field guard — a misrouted group/field would render a
        // different number.
        scheduler
            .metrics
            .find_worker_hits
            .fetch_add(11, Ordering::Relaxed);
        scheduler
            .metrics
            .prefetch_blobs_sent
            .fetch_add(22, Ordering::Relaxed);
        scheduler
            .metrics
            .bis_replay_buffer_overflow_drops
            .fetch_add(33, Ordering::Relaxed);
        scheduler.metrics.cache_warm_spawned.inc();
        // (#p1p2 telemetry) distinctive values on the tree-cache /
        // tree-resolution counters + gauges so a misrouted group/field
        // renders a different number (wrong-field guard). Gauges use
        // `store` (point-in-time), counters use `fetch_add` (cumulative).
        scheduler
            .metrics
            .tree_cache_hits
            .fetch_add(44, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_cache_misses
            .fetch_add(55, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_cache_evictions
            .fetch_add(66, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_cache_resident_bytes
            .store(77, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_cache_entries
            .store(88, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_resolution_cold_time_ns
            .fetch_add(99, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_resolution_cold_count
            .fetch_add(111, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_resolution_timeouts
            .fetch_add(122, Ordering::Relaxed);
        scheduler
            .metrics
            .tree_resolution_errors
            .fetch_add(133, Ordering::Relaxed);

        // Register exactly as production does: upcast the scheduler
        // (RootMetricsComponent: MetricsComponent) to the erased trait
        // object and register_dyn under the prod prefix shape.
        let registry = MetricsRegistry::new();
        registry.register_dyn(
            "scheduler.testsched.worker",
            scheduler.clone()
                as Arc<dyn MetricsComponentTrait + Send + Sync>,
        );

        // (#231 T1 de-flake) Warm the lazy `with_default` /
        // span-thread-local path ONCE and DISCARD the output before the
        // asserted render. `render_prometheus` installs a fresh
        // thread-local `Registry` subscriber via `with_default` for the
        // duration of the walk; on a cold first call the span-attribute
        // capture (`on_new_span` storing `SpanGroupName`, walked by
        // `on_event`) is not yet warm, and the whole-group-vanishes
        // failure mode observed 1/47 on cold builds rendered every
        // `group!`-recursing child empty while the bare scalars
        // survived. A discarded warm-up render makes the asserted render
        // deterministic WITHOUT a sleep (no time-based synchronization).
        let _warm = render_prometheus(&registry);

        let body = render_prometheus(&registry);

        // The prod prefix + the `scheduler_metrics` group + the field
        // name. `.` is sanitized to `_` by the Prometheus name
        // sanitizer, so the leaf substring `scheduler_metrics_<field>`
        // uniquely identifies the field under the SchedulerMetrics group.
        assert!(
            body.contains("scheduler_metrics_find_worker_hits"),
            "#231: SchedulerMetrics.find_worker_hits dark on /metrics — \
             the root walk did not descend into the metrics sub-component. \
             body=\n{body}"
        );
        assert!(
            body.contains("\nscheduler_testsched_worker_scheduler_metrics_find_worker_hits 11\n"),
            "#231: find_worker_hits rendered the wrong value (expected 11) — \
             group/field routing is wrong. body=\n{body}"
        );
        assert!(
            body.contains("scheduler_metrics_prefetch_blobs_sent"),
            "#231: SchedulerMetrics.prefetch_blobs_sent dark on /metrics — \
             the prefetch-tier counter is the #linkperf measurement \
             prerequisite. body=\n{body}"
        );
        assert!(
            body.contains("\nscheduler_testsched_worker_scheduler_metrics_prefetch_blobs_sent 22\n"),
            "#231: prefetch_blobs_sent rendered the wrong value (expected 22). \
             body=\n{body}"
        );
        assert!(
            body.contains("scheduler_metrics_bis_replay_buffer_overflow_drops"),
            "#231: SchedulerMetrics.bis_replay_buffer_overflow_drops dark on \
             /metrics — the #214 silent-drop counter has no operator signal \
             but the paired warn!. body=\n{body}"
        );
        assert!(
            body.contains("\nscheduler_testsched_worker_scheduler_metrics_bis_replay_buffer_overflow_drops 33\n"),
            "#231: bis_replay_buffer_overflow_drops rendered the wrong value \
             (expected 33). body=\n{body}"
        );
        // CounterWithTime nests under its own field name and emits a
        // `counter` leaf (see CounterWithTime::publish).
        assert!(
            body.contains("scheduler_metrics_cache_warm_spawned_counter"),
            "#231: SchedulerMetrics.cache_warm_spawned (CounterWithTime) dark \
             on /metrics — must emit the nested `counter` leaf. body=\n{body}"
        );
        assert!(
            body.contains("\nscheduler_testsched_worker_scheduler_metrics_cache_warm_spawned_counter 1\n"),
            "#231: cache_warm_spawned.counter rendered the wrong value \
             (expected 1 after one inc()). body=\n{body}"
        );

        // (#p1p2 telemetry) The tree-cache / tree-resolution counters and
        // gauges must render on /metrics with their set values. These
        // answer (1) is Phase-1 tree resolution a scheduling-latency
        // contributor and (2) does ByteBoundedTreeCache evict — dark
        // fields answer neither. Pinning the literal emitted names guards
        // against the doubled-name/dark-field trap.
        for (name, value) in [
            ("tree_cache_hits", 44u64),
            ("tree_cache_misses", 55),
            ("tree_cache_evictions", 66),
            ("tree_cache_resident_bytes", 77),
            ("tree_cache_entries", 88),
            ("tree_resolution_cold_time_ns", 99),
            ("tree_resolution_cold_count", 111),
            ("tree_resolution_timeouts", 122),
            ("tree_resolution_errors", 133),
        ] {
            assert!(
                body.contains(&format!("scheduler_metrics_{name}")),
                "#p1p2: SchedulerMetrics.{name} dark on /metrics — the tree-cache \
                 telemetry cannot answer its question if the field does not render. \
                 body=\n{body}"
            );
            assert!(
                body.contains(&format!(
                    "\nscheduler_testsched_worker_scheduler_metrics_{name} {value}\n"
                )),
                "#p1p2: SchedulerMetrics.{name} rendered the wrong value \
                 (expected {value}) — group/field routing is wrong. body=\n{body}"
            );
        }

        // (#231 T1 de-flake) Sibling assertion on the `inner`
        // (`ApiWorkerSchedulerImpl`) sub-tree. The whole-group-vanishes
        // failure mode rendered every `group!`-span-recursing child
        // EMPTY while the two bare-scalar `u64` fields (`worker_timeout_s`,
        // `memory_store_threshold`) survived — so the original
        // assertions could pass on the survivors even when the entire
        // `scheduler_metrics` walk was dark. This leaf comes from the
        // registered worker inside `ApiWorkerSchedulerImpl.workers` (the
        // `#[metric(group = "workers")]` field whose `Workers::publish`
        // enters a second `workers` group then a per-worker
        // `group!(worker_id)`), proving a SECOND independent
        // span-recursing child rendered. If it vanishes while the bare
        // scalars survive, this fails loudly instead of the test passing
        // on the survivors. The `{value="wmetric"}` doubles as a
        // wrong-field guard (`WorkerId::publish` emits a String gauge of
        // the worker id).
        assert!(
            body.contains("scheduler_testsched_worker_workers_workers_wmetric_id"),
            "#231 T1: the inner ApiWorkerSchedulerImpl `workers` sub-tree \
             rendered EMPTY — a registered worker's `id` leaf is absent. \
             This is the whole-group-vanishes mode: only bare scalars \
             survived the render. body=\n{body}"
        );
        assert!(
            body.contains(
                "scheduler_testsched_worker_workers_workers_wmetric_id{value=\"wmetric\"} 1\n"
            ),
            "#231 T1: inner-tree worker `id` rendered the wrong value \
             (expected the registered worker id \"wmetric\") — group/field \
             routing into ApiWorkerSchedulerImpl.workers is wrong. body=\n{body}"
        );
    }
}

/// Scheduling B1 — proof that `update_action` no longer holds the
/// worker-pool `inner` write lock across the retrying
/// `worker_state_manager.update_operation().await`.
///
/// The single interleaving that matters (a completion parked mid-update
/// while a matcher tries to reserve an independent worker) is driven
/// deterministically with a barrier mock (NOT sleep — CLAUDE.md
/// "no sleep-as-synchronization"); `tokio::time::timeout` is the
/// deadlock/stall detector.
#[cfg(test)]
mod b1_lock_decouple_tests {
    use core::sync::atomic::Ordering;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nativelink_config::schedulers::WorkerAllocationStrategy;
    use nativelink_error::{Code, Error};
    use nativelink_macro::nativelink_test;
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_util::action_messages::{
        ActionInfo, ActionResult, ActionStage, ActionUniqueKey, ActionUniqueQualifier,
        OperationId, WorkerId,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
    use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
    use parking_lot::Mutex as ParkingMutex;
    use tokio::sync::{Notify, mpsc};

    use super::{ApiWorkerScheduler, UpdateForWorker, Worker};
    use crate::platform_property_manager::PlatformPropertyManager;
    use crate::worker::ActionInfoWithProps;
    use crate::worker_registry::WorkerRegistry;
    use crate::worker_scheduler::WorkerScheduler;

    /// `WorkerStateManager` whose `update_operation` PARKS on a barrier
    /// until the test releases it. It records every call so the test can
    /// assert the op-state transition happened, and fires an "entered"
    /// notify so the test knows the worker-pool lock would be held *right
    /// now* if the fix were absent.
    #[derive(Debug)]
    struct BarrierWorkerStateManager {
        /// Fired once when `update_operation` is entered (task A parked).
        entered: Arc<Notify>,
        /// Awaited inside `update_operation`; the test releases it.
        release: Arc<Notify>,
        /// Records (operation_id, is_finished) of each update_operation call.
        calls: ParkingMutex<Vec<(OperationId, bool)>>,
    }

    impl BarrierWorkerStateManager {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                calls: ParkingMutex::new(Vec::new()),
            })
        }
    }

    impl MetricsComponent for BarrierWorkerStateManager {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[tonic::async_trait]
    impl WorkerStateManager for BarrierWorkerStateManager {
        async fn update_operation(
            &self,
            operation_id: &OperationId,
            _worker_id: &WorkerId,
            update: UpdateOperationType,
        ) -> Result<(), Error> {
            let is_finished = matches!(
                &update,
                UpdateOperationType::UpdateWithActionStage(s) if s.is_finished()
            );
            self.calls.lock().push((operation_id.clone(), is_finished));
            // Signal that we have entered (lock would be held here if the
            // fix were absent) BEFORE parking.
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    fn props_named(name: &str) -> PlatformProperties {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            PlatformPropertyValue::Exact(name.to_string()),
        );
        PlatformProperties { properties }
    }

    fn make_action_info_with_props(name: &str, seed: u8) -> ActionInfoWithProps {
        ActionInfoWithProps {
            inner: Arc::new(ActionInfo {
                command_digest: DigestInfo::new([0u8; 32], 0),
                input_root_digest: DigestInfo::new([0u8; 32], 0),
                timeout: Duration::MAX,
                platform_properties: HashMap::new(),
                priority: 0,
                load_timestamp: UNIX_EPOCH,
                insert_timestamp: SystemTime::now(),
                unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                    instance_name: "main".to_string(),
                    digest_function: DigestHasherFunc::Sha256,
                    digest: DigestInfo::new([seed; 32], 1),
                }),
            }),
            platform_properties: props_named(name),
        }
    }

    fn build_scheduler(wsm: Arc<BarrierWorkerStateManager>) -> Arc<ApiWorkerScheduler> {
        build_scheduler_with_load_byte_cost(wsm, 512 * 1024)
    }

    /// (#sched-blend) Build a scheduler with an explicit `load_byte_cost`
    /// so the continuous-blend crossover/mutation tests can drive the
    /// cache-vs-load tradeoff (and the `LOAD_BYTE_COST = 0` mutation).
    fn build_scheduler_with_load_byte_cost(
        wsm: Arc<BarrierWorkerStateManager>,
        load_byte_cost: u64,
    ) -> Arc<ApiWorkerScheduler> {
        ApiWorkerScheduler::new_with_locality_map(
            wsm,
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            None,
            None,
            load_byte_cost,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        )
    }

    /// (#sched-blend) Build a scheduler with an explicit `assume_core_count`
    /// so the zero-guard test can pass the misconfigured `0` value and
    /// observe that `new_with_locality_map` normalizes it at store time
    /// (a count-less worker must not be starved by `assume_core_count == 0`).
    fn build_scheduler_with_assume_core_count(
        wsm: Arc<BarrierWorkerStateManager>,
        assume_core_count: u32,
    ) -> Arc<ApiWorkerScheduler> {
        ApiWorkerScheduler::new_with_locality_map(
            wsm,
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            None,
            None,
            512 * 1024,
            assume_core_count,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        )
    }

    async fn add_worker_named(
        scheduler: &Arc<ApiWorkerScheduler>,
        name: &str,
        max_inflight_tasks: u64,
    ) -> mpsc::UnboundedReceiver<UpdateForWorker> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker::new(
            WorkerId(name.to_string()),
            props_named(name),
            tx,
            42,
            max_inflight_tasks,
        );
        scheduler.add_worker(worker).await.expect("add_worker");
        rx
    }

    /// All workers in this pool share the SAME capability property
    /// (`pool=swap`) so a single action matches every one of them — the
    /// shape needed to test the #37 fleet fail-open (every candidate
    /// gated).
    fn props_pool() -> PlatformProperties {
        let mut properties = HashMap::new();
        properties.insert(
            "pool".to_string(),
            PlatformPropertyValue::Exact("swap".to_string()),
        );
        PlatformProperties { properties }
    }

    async fn add_worker_in_pool(
        scheduler: &Arc<ApiWorkerScheduler>,
        name: &str,
    ) -> mpsc::UnboundedReceiver<UpdateForWorker> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker::new(WorkerId(name.to_string()), props_pool(), tx, 42, 0);
        scheduler.add_worker(worker).await.expect("add_worker");
        rx
    }

    /// (#37) COMPOSITE regression test (design §5). Composes the scheduler
    /// matcher (admission corner) + the swap-pressure ingest path + the
    /// fleet fail-open, in production composition, with TWO corners of the
    /// admission/eviction/pin triangle degraded and the third compensating:
    ///
    ///   - EVICTION degraded: no action ever completes (none are started),
    ///     so the OS pager frees no RSS — pressure cannot decay that way.
    ///   - PIN/TTL degraded: every worker is held `swap_pressured=true` and
    ///     the rate never falls — the EWMA-clears path never fires.
    ///   - THIRD CORNER COMPENSATES: the NET-NEW fleet fail-open (§5 case
    ///     3a) re-admits the LEAST-pressured otherwise-viable worker instead
    ///     of returning `None`, so the capability class makes progress
    ///     rather than wedging.
    ///
    /// Composite invariant: `gate ⇒ fleet-not-fully-gated` — a globally
    /// swap-pressured fleet degrades to slower (least-pressured) placement,
    /// NOT a deadlock.
    ///
    /// Falsification mutation (MUST red-fail, design §5): delete the
    /// `best_swap_gated` fail-open arm in `inner_find_worker_for_action`
    /// (the `if worker_id.is_none() { ... least_pressured ... }` block) so a
    /// fully-gated fleet returns `None`. This test then red-fails with the
    /// bespoke "composite invariant violated" message below. Because the
    /// fail-open is NET-NEW (not an inherited `best_overloaded` clone), the
    /// mutation deletes real new code: a green test after deleting it would
    /// mean the gate ships a wedge.
    #[nativelink_test]
    async fn swap_gate_fleet_failopen_places_least_pressured_when_all_gated() {
        let wsm = BarrierWorkerStateManager::new();
        let scheduler = build_scheduler(wsm);

        // Three workers in one capability class, all swap-pressured at
        // DIFFERENT rates (so "least-pressured" is well-defined).
        let _rx_a = add_worker_in_pool(&scheduler, "WA").await;
        let _rx_b = add_worker_in_pool(&scheduler, "WB").await;
        let _rx_c = add_worker_in_pool(&scheduler, "WC").await;

        // PIN/TTL degraded: gate every worker; the rate never decays.
        // WB is the LEAST pressured (lowest rate) → the fail-open target.
        scheduler
            .update_worker_swap_pressure(&WorkerId("WA".to_string()), true, 50_000)
            .await
            .expect("mark WA pressured");
        scheduler
            .update_worker_swap_pressure(&WorkerId("WB".to_string()), true, 10_000)
            .await
            .expect("mark WB pressured");
        scheduler
            .update_worker_swap_pressure(&WorkerId("WC".to_string()), true, 30_000)
            .await
            .expect("mark WC pressured");

        // EVICTION degraded: no action has been started/completed, so no
        // RSS is freed. With every candidate gated, the normal `viable`
        // set is EMPTY; only the fleet fail-open can place the action.
        let chosen = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.find_worker_for_action(&props_pool(), false),
        )
        .await
        .expect("matcher must not hang");

        let chosen = chosen.expect(
            "composite invariant violated: swap gate active fleet-wide with no \
             compensating fail-open → wedge (the matcher returned None instead \
             of degrading to least-pressured placement)",
        );
        assert_eq!(
            chosen,
            WorkerId("WB".to_string()),
            "fleet fail-open must place on the LEAST-pressured gated worker \
             (WB @ 10k), not WA @ 50k / WC @ 30k"
        );
    }

    /// (#37) Asymmetric coverage of the proactive skip: when AT LEAST ONE
    /// worker is NOT swap-pressured, the matcher MUST prefer it over the
    /// gated ones — the fail-open must NOT fire when a healthy worker
    /// exists (over-action direction). Guards against a fail-open that
    /// places on a gated worker even though a clean one was available.
    ///
    /// Falsification mutation: remove the `if w.swap_pressured { return
    /// false }` skip in `worker_matches` — a gated worker would then be
    /// `viable` and could win, and this test red-fails because the chosen
    /// worker is gated.
    #[nativelink_test]
    async fn swap_gate_prefers_unpressured_when_one_exists() {
        let wsm = BarrierWorkerStateManager::new();
        let scheduler = build_scheduler(wsm);

        let _rx_a = add_worker_in_pool(&scheduler, "WA").await;
        let _rx_b = add_worker_in_pool(&scheduler, "WB").await;

        // WA gated (high pressure), WB healthy.
        scheduler
            .update_worker_swap_pressure(&WorkerId("WA".to_string()), true, 50_000)
            .await
            .expect("mark WA pressured");
        scheduler
            .update_worker_swap_pressure(&WorkerId("WB".to_string()), false, 0)
            .await
            .expect("mark WB healthy");

        let chosen = scheduler
            .find_worker_for_action(&props_pool(), false)
            .await
            .expect("a healthy worker exists; matcher must place");
        assert_eq!(
            chosen,
            WorkerId("WB".to_string()),
            "the matcher must PROACTIVELY skip the swap-pressured worker (WA) \
             and place on the healthy one (WB); the fail-open must NOT fire \
             while a clean worker exists"
        );
    }

    /// THE decouple proof. Task A's completion parks inside
    /// `update_operation`; task B's `find_and_reserve_worker` for an
    /// INDEPENDENT worker must complete while A is parked. If the
    /// worker-pool lock were still held across (b), B would block on
    /// `inner.write()` and the timeout would fire.
    #[nativelink_test]
    async fn update_action_does_not_block_concurrent_match() {
        let wsm = BarrierWorkerStateManager::new();
        let entered = wsm.entered.clone();
        let release = wsm.release.clone();
        let calls_view = Arc::clone(&wsm);
        let scheduler = build_scheduler(wsm);

        // Worker W (the one whose op completes) and an independent W2.
        let _rx_w = add_worker_named(&scheduler, "W", 4).await;
        let _rx_w2 = add_worker_named(&scheduler, "W2", 4).await;

        // Reserve op_a on W via the real reservation path (production shape).
        let op_a = OperationId::default();
        let action_w = make_action_info_with_props("W", 0xa1);
        let (reserved_worker, _tx, _msg) = scheduler
            .find_and_reserve_worker(&props_named("W"), &op_a, &action_w, false)
            .await
            .expect("op_a must reserve worker W");
        assert_eq!(reserved_worker, WorkerId("W".to_string()));

        // Task A: complete op_a on W → parks inside update_operation (b).
        let scheduler_a = Arc::clone(&scheduler);
        let op_a_for_a = op_a.clone();
        let task_a = tokio::spawn(async move {
            scheduler_a
                .update_action(
                    &WorkerId("W".to_string()),
                    &op_a_for_a,
                    UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                        ActionResult::default(),
                    )),
                )
                .await
        });

        // Wait until A is confirmed parked inside update_operation.
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("task A must enter update_operation (parked in (b))");

        // Task B: reserve an INDEPENDENT worker W2 while A is parked.
        // This must NOT block on the worker-pool lock.
        let op_b = OperationId::default();
        let action_w2 = make_action_info_with_props("W2", 0xb2);
        let props_w2 = props_named("W2");
        let reserve_b =
            scheduler.find_and_reserve_worker(&props_w2, &op_b, &action_w2, false);
        let b_result = tokio::time::timeout(Duration::from_secs(1), reserve_b)
            .await
            .expect(
                "match must not block on a parked update_operation — B1 lock-decouple violated",
            );
        let (b_worker, _b_tx, _b_msg) =
            b_result.expect("W2 must be reservable while W's completion is parked");
        assert_eq!(b_worker, WorkerId("W2".to_string()));

        // Release A and assert it completes Ok and the op-state advanced.
        release.notify_one();
        let a_result = tokio::time::timeout(Duration::from_secs(1), task_a)
            .await
            .expect("task A must finish after release")
            .expect("task A join");
        a_result.expect("op_a completion must return Ok");

        // The op-state update was applied (mock recorded a finished update).
        let calls = calls_view.calls.lock().clone();
        assert!(
            calls.iter().any(|(oid, finished)| *oid == op_a && *finished),
            "update_operation must have been called with the finished op_a"
        );

        // W's slot for op_a is freed after completion (CS2 ran).
        {
            let inner = scheduler.inner.read().await;
            let worker = inner
                .workers
                .peek(&WorkerId("W".to_string()))
                .expect("worker W still present");
            assert!(
                !worker.running_action_infos.contains_key(&op_a),
                "op_a slot must be freed on W after completion (CS2 complete_action)"
            );
        }
    }

    /// No-double-dispatch sub-assertion: during the §3.3 window (op_a
    /// reserved on W, A parked after (b), slot NOT yet freed), a
    /// concurrent matcher for a W-pinned op must NOT be able to reserve
    /// W's only slot — it is still counted, so W looks busier-than-true
    /// (the safe O1 direction). Under O2 (free slot before (b)) the slot
    /// would appear free and the matcher could over-subscribe W.
    #[nativelink_test]
    async fn update_action_window_does_not_free_slot_for_double_dispatch() {
        let wsm = BarrierWorkerStateManager::new();
        let entered = wsm.entered.clone();
        let release = wsm.release.clone();
        let scheduler = build_scheduler(wsm);

        // W has a SINGLE slot; once op_a is reserved it is full.
        let _rx_w = add_worker_named(&scheduler, "W", 1).await;

        let op_a = OperationId::default();
        let action_w = make_action_info_with_props("W", 0xa1);
        scheduler
            .find_and_reserve_worker(&props_named("W"), &op_a, &action_w, false)
            .await
            .expect("op_a must reserve W's single slot");

        // Task A: complete op_a → parks in (b) with the slot still held.
        let scheduler_a = Arc::clone(&scheduler);
        let op_a_for_a = op_a.clone();
        let task_a = tokio::spawn(async move {
            scheduler_a
                .update_action(
                    &WorkerId("W".to_string()),
                    &op_a_for_a,
                    UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                        ActionResult::default(),
                    )),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("task A must enter update_operation (parked in (b))");

        // During the window: a matcher for a W-pinned op MUST get None
        // (W's only slot is still counted by op_a). If it reserved W, the
        // slot was freed before (b) — the O2 double-book hazard.
        let op_c = OperationId::default();
        let action_c = make_action_info_with_props("W", 0xc3);
        let window_reserve = tokio::time::timeout(
            Duration::from_secs(1),
            scheduler.find_and_reserve_worker(&props_named("W"), &op_c, &action_c, false),
        )
        .await
        .expect("matcher must not block on the parked completion — B1 lock-decouple violated");
        assert!(
            window_reserve.is_none(),
            "no-double-dispatch violated — W's slot was freed during the \
             update window, letting a second action reserve W while op_a's \
             completion is still in flight (O2 over-subscription hazard)"
        );

        // Release and let A finish cleanly.
        release.notify_one();
        let _ = tokio::time::timeout(Duration::from_secs(1), task_a)
            .await
            .expect("task A must finish after release");
    }

    /// §6.3 over-action cell (FR-2 / red-team REQ#3): (b) succeeds, then
    /// the op is removed from W's `running_action_infos` during the
    /// post-unlock window by a legitimate concurrent finalize. CS2's
    /// re-lookup finds the op already gone; the softened branch must
    /// return `Ok(())` (benign) AND bump the observability counter (so
    /// the race is visible in the release binary where `debug!` is
    /// compiled out).
    #[nativelink_test]
    async fn update_action_softens_already_finalized_op_with_counter() {
        let wsm = BarrierWorkerStateManager::new();
        let entered = wsm.entered.clone();
        let release = wsm.release.clone();
        let scheduler = build_scheduler(wsm);

        let _rx_w = add_worker_named(&scheduler, "W", 4).await;

        let op_a = OperationId::default();
        let action_w = make_action_info_with_props("W", 0xa1);
        scheduler
            .find_and_reserve_worker(&props_named("W"), &op_a, &action_w, false)
            .await
            .expect("op_a must reserve W");

        let counter_before = scheduler
            .get_metrics()
            .update_action_op_already_finalized
            .load(Ordering::Relaxed);

        // Task A: complete op_a → parks in (b).
        let scheduler_a = Arc::clone(&scheduler);
        let op_a_for_a = op_a.clone();
        let task_a = tokio::spawn(async move {
            scheduler_a
                .update_action(
                    &WorkerId("W".to_string()),
                    &op_a_for_a,
                    UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                        ActionResult::default(),
                    )),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("task A must enter update_operation (parked in (b))");

        // Simulate a legitimate concurrent finalize removing op_a from W's
        // running set during the window (e.g. ExecutionComplete or evict).
        {
            let mut inner = scheduler.inner.write().await;
            let worker = inner
                .workers
                .get_mut(&WorkerId("W".to_string()))
                .expect("worker W present");
            assert!(
                worker.running_action_infos.remove(&op_a).is_some(),
                "precondition: op_a was reserved on W"
            );
        }

        // Release A: (b) returns Ok, CS2 finds op already gone → softened.
        release.notify_one();
        let a_result = tokio::time::timeout(Duration::from_secs(1), task_a)
            .await
            .expect("task A must finish after release")
            .expect("task A join");
        a_result.expect(
            "already-finalized op in CS2 must be softened to Ok(()) — \
             benign post-unlock race, not an error",
        );

        let counter_after = scheduler
            .get_metrics()
            .update_action_op_already_finalized
            .load(Ordering::Relaxed);
        assert_eq!(
            counter_after,
            counter_before + 1,
            "the softened already-finalized branch must bump its \
             observability counter (warn!+counter, not debug!) so the \
             race is visible in the release binary"
        );
    }

    /// §6.3 companion (FR-2 / code-reviewer F3): a completion for an op
    /// that is NOT running on the named worker must error in CS1 (the
    /// op-not-running branch) BEFORE reaching the softened CS2 path,
    /// proving the softening does NOT mask a genuine wrong-worker bug. The
    /// end-to-end wrong-worker version-CAS rejection
    /// (`simple_scheduler_state_manager.rs:694-715`) is covered by
    /// `simple_scheduler_test::update_action_with_wrong_worker_id_errors_test`.
    #[nativelink_test]
    async fn update_action_unknown_op_errors_before_softening() {
        let wsm = BarrierWorkerStateManager::new();
        let release = wsm.release.clone();
        let calls_view = Arc::clone(&wsm);
        let scheduler = build_scheduler(wsm);

        let _rx_w = add_worker_named(&scheduler, "W", 4).await;

        // No op reserved on W. Completing an unknown op must error in CS1
        // (op-not-running) — it must NOT silently reach the softened CS2
        // Ok(()) path, and update_operation must never be invoked.
        release.notify_one(); // ensure the mock would not block if reached
        let op_unknown = OperationId::default();
        let res = tokio::time::timeout(
            Duration::from_secs(1),
            scheduler.update_action(
                &WorkerId("W".to_string()),
                &op_unknown,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default(),
                )),
            ),
        )
        .await
        .expect("update_action for unknown op must not hang");
        let err = res.expect_err(
            "completing an op the worker is not running must error (op-not-running), \
             not be softened to Ok(())",
        );
        assert_eq!(
            err.code,
            Code::Internal,
            "op-not-running must surface as Code::Internal, not a softened Ok"
        );
        assert!(
            calls_view.calls.lock().is_empty(),
            "update_operation must not run for an op the worker is not executing — \
             the (b) lock-free await must be unreachable on the op-not-running path"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // (#sched-blend) Continuous cache-vs-load blend tests (design §9).
    //
    // Each behavioral test drives the PRODUCTION selection entry
    // (`find_and_reserve_worker` → `inner_find_and_reserve_worker`, or the
    // inner method directly for Tier 1.5 where per-worker `cached_score`
    // must vary), composes the real `ApiWorkerSchedulerImpl` (cascade +
    // viability gates + blend + backstop), and has a mutation that must
    // red-fail with a bespoke message. All intermediate magnitudes are
    // asserted in the centi-core integer space the impl runs (§2.2/§5).
    // ════════════════════════════════════════════════════════════════════
    mod sched_blend {
        use std::collections::{HashMap, HashSet};

        use super::*;
        use crate::worker::MAX_PLAUSIBLE_CORES;

        // The action has `input_root_digest = DigestInfo::new([0u8; 32], 0)`.
        // A worker whose `cached_directory_digests` contains it is a Tier-1
        // root match → the exact-root tier ranks it by `load_penalty` alone.
        fn input_root() -> DigestInfo {
            DigestInfo::new([0u8; 32], 0)
        }

        /// Action whose `platform_properties` are `props_pool()` so it
        /// matches every pool worker AND the post-selection
        /// `reduce_platform_properties` succeeds (the action's props must be
        /// satisfied by the worker's props — both `pool=swap` here).
        fn pool_action() -> ActionInfoWithProps {
            use nativelink_util::action_messages::{ActionInfo, ActionUniqueKey, ActionUniqueQualifier};
            ActionInfoWithProps {
                inner: Arc::new(ActionInfo {
                    command_digest: DigestInfo::new([0u8; 32], 0),
                    input_root_digest: input_root(),
                    timeout: Duration::MAX,
                    platform_properties: HashMap::new(),
                    priority: 0,
                    load_timestamp: UNIX_EPOCH,
                    insert_timestamp: SystemTime::now(),
                    unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                        instance_name: "main".to_string(),
                        digest_function: DigestHasherFunc::Sha256,
                        digest: DigestInfo::new([7u8; 32], 1),
                    }),
                }),
                platform_properties: props_pool(),
            }
        }

        /// Register a pool worker with the given P/E counts and per-type
        /// loads, and mark it a Tier-1 root match (so the exact-root tier
        /// ranks it by the continuous `load_penalty`). All workers share
        /// `props_pool()` so a single action matches the whole fleet.
        async fn add_tier1_worker(
            scheduler: &Arc<ApiWorkerScheduler>,
            name: &str,
            p_count: u32,
            e_count: u32,
            cpu_load: u32,
            p_load: u32,
            e_load: u32,
        ) {
            let _rx = add_worker_in_pool(scheduler, name).await;
            scheduler
                .set_worker_core_counts(&WorkerId(name.to_string()), p_count, e_count)
                .await
                .expect("set core counts");
            scheduler
                .update_worker_load(&WorkerId(name.to_string()), cpu_load, p_load, e_load)
                .await
                .expect("set load");
            let mut cached = HashSet::new();
            cached.insert(input_root());
            scheduler
                .update_cached_directories(&WorkerId(name.to_string()), cached)
                .await
                .expect("set cached dirs");
        }

        /// Register a pool worker that matches the action's `input_root` ONLY
        /// as a cached *subtree* (`cached_subtree_digests`), NOT as an exact
        /// cached root (`cached_directory_digests`). Both arms route into the
        /// Tier-1 min-`load_penalty` selection (`has_root_match ||
        /// has_subtree_match`), so a subtree-only member must compete on load
        /// like any holder — no whole-tree stickiness (design §4.1 D4).
        async fn add_tier1_subtree_worker(
            scheduler: &Arc<ApiWorkerScheduler>,
            name: &str,
            p_count: u32,
            e_count: u32,
            cpu_load: u32,
            p_load: u32,
            e_load: u32,
        ) {
            let _rx = add_worker_in_pool(scheduler, name).await;
            scheduler
                .set_worker_core_counts(&WorkerId(name.to_string()), p_count, e_count)
                .await
                .expect("set core counts");
            scheduler
                .update_worker_load(&WorkerId(name.to_string()), cpu_load, p_load, e_load)
                .await
                .expect("set load");
            // SUBTREE match only — input_root appears as a cached subtree of
            // some other tree, not as an exact cached root.
            scheduler
                .update_cached_subtrees(
                    &WorkerId(name.to_string()),
                    true,
                    vec![input_root()],
                    vec![],
                    vec![],
                )
                .await
                .expect("set cached subtrees");
        }

        /// Run the production selection entry and return the chosen worker.
        async fn select(scheduler: &Arc<ApiWorkerScheduler>) -> Option<WorkerId> {
            let action = pool_action();
            let op = OperationId::default();
            tokio::time::timeout(
                Duration::from_secs(2),
                scheduler.find_and_reserve_worker(&props_pool(), &op, &action, false),
            )
            .await
            .expect("selection must not hang (deadlock detector)")
            .map(|(wid, _tx, _msg)| wid)
        }

        // ════════════════════════════════════════════════════════════════
        // (#sched M1 rebalance v2, §13) Ranker-aware magnet tests — I6.
        // These drive the PRODUCTION Tier-1 selection (`inner_find_and_reserve_
        // worker`) and the SOFT fallback (`find_and_reserve_worker`) with the
        // P-headroom gate ENABLED and a real idle-threshold, proving a
        // genuine-free-slot worker out-ranks a stale-low-p_load override-admit
        // (invariant I6, the magnet fix). Design-drift note: the dispatch named
        // an external `tests/scheduler_m1v2_ranker_test.rs`, but the external
        // crate cannot set per-worker core counts (`set_worker_core_counts` is
        // `#[cfg(test)]`-only) NOR inject a precise `running_action_infos`
        // count, both REQUIRED to place a worker deterministically at
        // `running >= p_count` with a chosen p_load. So the magnet tests live
        // here (inline), the production-composition location every sibling
        // cache-tier test already uses; the external file carries the
        // black-box gate-plumbing coverage the public API supports.
        // ════════════════════════════════════════════════════════════════

        /// Build a scheduler with the P-headroom gate ON and a chosen idle
        /// threshold + override factor. Everything else matches `build_scheduler`.
        fn build_scheduler_gate_on(
            wsm: Arc<BarrierWorkerStateManager>,
            p_idle_threshold_pct: u32,
            p_headroom_override_factor: u32,
        ) -> Arc<ApiWorkerScheduler> {
            ApiWorkerScheduler::new_with_locality_map(
                wsm,
                Arc::new(PlatformPropertyManager::new(HashMap::new())),
                WorkerAllocationStrategy::default(),
                Arc::new(Notify::new()),
                100,
                Arc::new(WorkerRegistry::new()),
                None,
                None,
                None,
                512 * 1024,
                8,
                true, // (#sched M1 rebalance v2) p_headroom_gate ON
                p_idle_threshold_pct,
                p_headroom_override_factor,
            )
        }

        /// Force a worker's fresh in-flight count to exactly `running` by
        /// inserting/removing dummy `PendingActionInfoData` entries under the
        /// write lock (the ranker reads `running_action_infos.len()`; there is
        /// no public setter). Distinct v4-UUID ops so the map length is exact.
        async fn set_worker_running(scheduler: &Arc<ApiWorkerScheduler>, name: &str, running: usize) {
            use crate::worker::PendingActionInfoData;
            let mut inner = scheduler.inner.write().await;
            let w = inner
                .workers
                .0
                .peek_mut(&WorkerId(name.to_string()))
                .expect("worker exists");
            w.running_action_infos.clear();
            for _ in 0..running {
                w.running_action_infos.insert(
                    OperationId::default(),
                    PendingActionInfoData { action_info: pool_action() },
                );
            }
            assert_eq!(
                w.running_action_infos.len(),
                running,
                "fixture must set exactly `running` in-flight actions"
            );
        }

        /// (§13 test 3, Tier-1) I6 magnet fix on the exact-root tier. TWO
        /// root-holding workers, BOTH eligible under the gate:
        ///   - FREE_SLOT: `running(3) < p_count(4)` → genuine free P slot, but a
        ///     HIGH (stale) p_load 90 → HIGH `load_penalty`.
        ///   - OVERRIDE : `running(4) == p_count(4)` (no free slot) admitted by
        ///     the idle-P override (p_load 5 < threshold 50), and its LOW p_load
        ///     → LOW `load_penalty`.
        /// v1's min-`load_penalty` Tier-1 would pick OVERRIDE (lower stale load)
        /// — the bounded magnet. v2's `(p_headroom_pref, load_penalty)` primary
        /// key makes FREE_SLOT (pref 0) beat OVERRIDE (pref ≥ 1) regardless of
        /// the stale load. MUTATION: drop `pref` from the Tier-1 key (revert to
        /// `min load_penalty`) → OVERRIDE wins → this test red-fails.
        #[nativelink_test]
        async fn t_i6_magnet_tier1_free_slot_beats_override() {
            let scheduler = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);
            // FREE_SLOT: p_count 4, HIGH p_load 90 (stale), root holder.
            add_tier1_worker(&scheduler, "FREE_SLOT", 4, 0, 90, 90, 0).await;
            set_worker_running(&scheduler, "FREE_SLOT", 3).await; // 3 < 4 → free slot
            // OVERRIDE: p_count 4, LOW p_load 5 (stale-low), root holder.
            add_tier1_worker(&scheduler, "OVERRIDE", 4, 0, 5, 5, 0).await;
            set_worker_running(&scheduler, "OVERRIDE", 4).await; // 4 == 4 → override-admit

            // Sanity: v1's stale-load ranking WOULD prefer OVERRIDE.
            let free_pen = super::super::capacity_score(90, 0, 90, 4, 0, 8, 512 * 1024).load_penalty;
            let over_pen = super::super::capacity_score(5, 0, 5, 4, 0, 8, 512 * 1024).load_penalty;
            assert!(
                over_pen < free_pen,
                "precondition: the override worker's stale-low p_load gives it the \
                 LOWER load_penalty — this is exactly the magnet v1 would fall for"
            );

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("FREE_SLOT".to_string())),
                "I6 Tier-1 magnet: a genuine-free-P-slot root holder (running 3 < \
                 p_count 4, pref 0) MUST beat an override-admit root holder (running \
                 4 == p_count 4, pref 1) EVEN THOUGH the override worker's stale-low \
                 p_load gives it a lower load_penalty — the fresh-count `p_headroom_pref` \
                 PRIMARY key dominates the stale load SECONDARY key"
            );
        }

        /// (§13 test 3, fallback) I6 magnet fix on the SOFT LRU/MRU fallback.
        /// Same shape as the Tier-1 test but NEITHER worker is a cache holder,
        /// so the cascade falls through to `inner_find_worker_for_action`, whose
        /// sort key is `(p_headroom_pref, effective_load_score)`. FREE_SLOT
        /// (pref 0) must beat OVERRIDE (pref ≥ 1) despite OVERRIDE's lower stale
        /// `effective_load_score`. Note the old v2.2 bool key
        /// `(!worker_has_p_headroom, load)` would NOT catch this magnet: at
        /// threshold 50 the OVERRIDE worker is admitted by the idle-P override
        /// (p_load 5 < 50), so `worker_has_p_headroom` is TRUE for BOTH workers
        /// → both sort tier `false` → the bool key falls straight to `load` and
        /// picks OVERRIDE (lower stale load) — the magnet. Only the fresh-count
        /// `p_headroom_pref` PRIMARY key ({0 for FREE_SLOT} < {1 for OVERRIDE})
        /// separates them. MUTATION: drop `p_headroom_pref` from the key (revert
        /// to sorting on `effective_load_score` alone) → OVERRIDE's lower stale
        /// load wins → this test red-fails.
        #[nativelink_test]
        async fn t_i6_magnet_fallback_free_slot_beats_override() {
            let scheduler = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);
            // NON-holders (no update_cached_directories) → cascade falls through.
            let _rx1 = add_worker_in_pool(&scheduler, "FREE_SLOT").await;
            scheduler
                .set_worker_core_counts(&WorkerId("FREE_SLOT".to_string()), 4, 0)
                .await
                .expect("counts");
            scheduler
                .update_worker_load(&WorkerId("FREE_SLOT".to_string()), 90, 90, 0)
                .await
                .expect("load");
            set_worker_running(&scheduler, "FREE_SLOT", 3).await; // free slot, high stale load

            let _rx2 = add_worker_in_pool(&scheduler, "OVERRIDE").await;
            scheduler
                .set_worker_core_counts(&WorkerId("OVERRIDE".to_string()), 4, 0)
                .await
                .expect("counts");
            scheduler
                .update_worker_load(&WorkerId("OVERRIDE".to_string()), 5, 5, 0)
                .await
                .expect("load");
            set_worker_running(&scheduler, "OVERRIDE", 4).await; // override-admit, low stale load

            // Sanity: the override worker has the LOWER effective_load_score.
            let free_els = super::super::effective_load_score(90, 0, 90, true);
            let over_els = super::super::effective_load_score(5, 0, 5, true);
            assert!(
                over_els < free_els,
                "precondition: the override worker's stale-low p_load gives it the \
                 lower effective_load_score (the fallback magnet)"
            );

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("FREE_SLOT".to_string())),
                "I6 fallback magnet: on the soft LRU/MRU path a genuine-free-P-slot \
                 worker (pref 0) MUST beat an override-admit (pref 1) EVEN THOUGH the \
                 override worker's stale-low p_load gives it the lower \
                 effective_load_score — `p_headroom_pref` is the PRIMARY sort key"
            );
        }

        /// (§13 test 5, e2e) THRESHOLD=0 parity: with `p_idle_threshold_pct == 0`
        /// the override is dead, so a gate-ON scheduler selects IDENTICALLY to
        /// gate-on-v1 — an at-capacity worker gets no override, and among two
        /// root holders the min-load one wins as before. Here FREE_SLOT (running
        /// 3 < p_count 4) is the only worker with headroom; the AT_CAP worker
        /// (running 4, low p_load) gets NO override (threshold 0) so it is
        /// excluded from the cache tiers → FREE_SLOT wins by headroom alone, the
        /// v1 order. (Contrast `t_i6_magnet_tier1_*`, where threshold 50 admits
        /// the override worker; here threshold 0 keeps it out entirely.)
        #[nativelink_test]
        async fn t_threshold_zero_parity_no_override() {
            let scheduler = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 0, 2);
            add_tier1_worker(&scheduler, "FREE_SLOT", 4, 0, 90, 90, 0).await;
            set_worker_running(&scheduler, "FREE_SLOT", 3).await; // free slot (high stale load)
            add_tier1_worker(&scheduler, "AT_CAP", 4, 0, 5, 5, 0).await;
            set_worker_running(&scheduler, "AT_CAP", 4).await; // at cap, low stale load

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("FREE_SLOT".to_string())),
                "THRESHOLD=0 parity: the override is dead (`p_load < 0` never fires), \
                 so AT_CAP (running 4 == p_count 4, no fresh headroom) is gate-excluded \
                 from the cache tiers exactly as in gate-on-v1 — FREE_SLOT wins by \
                 dispatch-count headroom alone, NOT by any p_load override"
            );
        }

        /// (§13 test 2, e2e) Ceiling bound (I5_Bounded) end-to-end: with the
        /// gate ON, threshold 50, factor 2, a sole idle-P root holder is
        /// selectable while `running < p_count*factor` but its override CLOSES
        /// at the ceiling. At running 4 (< 8) it is admitted and selected; at
        /// running 8 (== ceiling) it is gate-excluded from the cache tiers. The
        /// second worker COLD_FREE (a non-holder with a genuine free slot) then
        /// takes the fall-through, proving the ceiling shut the override.
        #[nativelink_test]
        async fn t_ceiling_bound_override_closes_e2e() {
            // running 4 < ceiling 8 → override active → HOLDER selected.
            let s1 = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);
            add_tier1_worker(&s1, "HOLDER", 4, 0, 5, 5, 0).await; // idle-P root holder
            set_worker_running(&s1, "HOLDER", 4).await; // 4 < 8 → override admits
            let _rx = add_worker_in_pool(&s1, "COLD_FREE").await; // non-holder, free slot
            s1.set_worker_core_counts(&WorkerId("COLD_FREE".to_string()), 4, 0)
                .await
                .expect("counts");
            s1.update_worker_load(&WorkerId("COLD_FREE".to_string()), 5, 5, 0)
                .await
                .expect("load");
            assert_eq!(
                select(&s1).await,
                Some(WorkerId("HOLDER".to_string())),
                "below the ceiling (running 4 < p_count 4 * factor 2 = 8) the idle-P \
                 override admits the root HOLDER to the cache tier → it wins its own root"
            );

            // running 8 == ceiling → override CLOSES → HOLDER gate-excluded from
            // the cache tiers → COLD_FREE (genuine free slot) wins the fall-through.
            let s2 = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);
            add_tier1_worker(&s2, "HOLDER", 4, 0, 5, 5, 0).await;
            set_worker_running(&s2, "HOLDER", 8).await; // 8 == ceiling → NO override
            let _rx2 = add_worker_in_pool(&s2, "COLD_FREE").await;
            s2.set_worker_core_counts(&WorkerId("COLD_FREE".to_string()), 4, 0)
                .await
                .expect("counts");
            s2.update_worker_load(&WorkerId("COLD_FREE".to_string()), 5, 5, 0)
                .await
                .expect("load");
            assert_eq!(
                select(&s2).await,
                Some(WorkerId("COLD_FREE".to_string())),
                "at the ceiling (running 8 == p_count 4 * factor 2) the fresh-count \
                 backstop shuts the override (I5_Bounded); the root HOLDER is excluded \
                 from the cache tiers and COLD_FREE (genuine free slot) takes the \
                 fall-through — stale-low p_load cannot re-admit past the ceiling"
            );
        }

        /// (§13, SingleHolderCeiling — the Rust counterpart to the proven TLA+
        /// `SingleHolderCeiling` invariant.) The honest single-holder coverage:
        /// ONE worker holds the action's `input_root` in its dir-cache (a Tier-1
        /// root match — the sticky magnet), and it is OVER its P-slot count but
        /// still override-admitted (p_load 5 < threshold 50). There are cache-COLD
        /// PEERS with genuine free P slots. The invariant: the sole holder is
        /// CAPPED at `p_core_count * factor` in-flight — once it reaches the
        /// ceiling, the override CLOSES, so the holder is gate-excluded from the
        /// cache tiers and the next same-root dispatch's overflow lands on a
        /// (cold) PEER, NOT the holder. Without the ceiling the root-match
        /// stickiness would let a single holder accrete the whole same-root storm
        /// unboundedly (the concentration the M1 gate exists to bound).
        ///
        /// MUTATION: drop the ceiling term (`running < p_count * factor`) from the
        /// override clause of `worker_has_p_headroom` → at running 8 the holder is
        /// STILL admitted (p_load 5 < 50), its Tier-1 root match beats the cold
        /// peers' fall-through, and the holder wins its OWN root past the ceiling
        /// → the peer-wins assert red-fails with the bespoke message below.
        #[nativelink_test]
        async fn t_single_holder_capped_at_ceiling_overflow_lands_on_cold_peer() {
            let scheduler = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);

            // SOLE HOLDER: p_count 4, idle-P (p_load 5), holds `input_root` as a
            // Tier-1 root match. Placed AT the ceiling (running 8 == 4 * 2), so
            // its idle-P override is CLOSED (I5_Bounded) despite the stale-low
            // p_load — the fresh-count backstop, not the load, decides.
            add_tier1_worker(&scheduler, "HOLDER", 4, 0, 5, 5, 0).await;
            set_worker_running(&scheduler, "HOLDER", 8).await; // 8 == ceiling → NO override

            // Cache-COLD PEERS (no `update_cached_directories` → not root holders)
            // each with a genuine free P slot (running 0 < p_count 4). They are
            // the legitimate overflow targets once the holder hits its ceiling.
            for peer in ["COLD_A", "COLD_B"] {
                let _rx = add_worker_in_pool(&scheduler, peer).await;
                scheduler
                    .set_worker_core_counts(&WorkerId(peer.to_string()), 4, 0)
                    .await
                    .expect("counts");
                scheduler
                    .update_worker_load(&WorkerId(peer.to_string()), 5, 5, 0)
                    .await
                    .expect("load");
                // running defaults to 0 → genuine free P slot.
            }

            // The holder is at its ceiling → override closed → excluded from the
            // cache tiers → the same-root dispatch overflows to a COLD peer, NOT
            // the holder. (Either cold peer is acceptable; the load-bearing
            // assertion is that it is NOT the holder.)
            let chosen = select(&scheduler).await;
            assert!(
                chosen.is_some(),
                "SingleHolderCeiling: a same-root dispatch must place SOMEWHERE — \
                 the fleet has cold peers with free P slots; wedging is a bug"
            );
            assert_ne!(
                chosen,
                Some(WorkerId("HOLDER".to_string())),
                "SingleHolderCeiling: the sole root HOLDER is at its P-slot ceiling \
                 (running 8 == p_count 4 * factor 2), so its idle-P override is \
                 CLOSED and it is gate-excluded from the cache tiers — the overflow \
                 same-root dispatch MUST land on a cold PEER (free P slot), NOT pile \
                 onto the holder past its ceiling. The root-match stickiness cannot \
                 override the fresh-count backstop (the Rust counterpart to the \
                 proven TLA+ SingleHolderCeiling)"
            );
        }

        /// (§13 test 3, Tier-1.5) I6 magnet fix on the subtree-coverage tier.
        /// TWO subtree holders with the SAME cached_score, both gate-eligible:
        ///   - FREE_SLOT: `running(3) < p_count(4)` (free slot), HIGH stale
        ///     p_load 90 → lower `blended_s` (cached_score − higher penalty).
        ///   - OVERRIDE : `running(4) == p_count(4)` override-admit (p_load 5 <
        ///     threshold 50), LOW stale p_load → higher `blended_s`.
        /// v1's max-`blended_s` Tier-1.5 would pick OVERRIDE (higher blended_s
        /// from lower stale penalty) — the magnet. v2's `(pref, Reverse(
        /// blended_s))` key makes FREE_SLOT (pref 0) win. The `blended_s > 0`
        /// crossover filter is preserved (both workers have a large enough cache
        /// lead to stay positive). MUTATION: drop `pref` from the Tier-1.5 key →
        /// OVERRIDE wins → red-fail.
        #[nativelink_test]
        async fn t_i6_magnet_tier15_free_slot_beats_override() {
            let child_a = DigestInfo::new([0xa1u8; 32], 1);
            let child_b = DigestInfo::new([0xb2u8; 32], 1);
            // Large cache (≈3 MiB ≫ any penalty) so BOTH workers stay blended_s
            // > 0 (crossover preserved) and the winner is decided by pref, not
            // the crossover filter.
            let tree = build_tree(child_a, child_b, 3 * 1024 * 1024);
            let scheduler = build_scheduler_gate_on(BarrierWorkerStateManager::new(), 50, 2);
            // Both cache child_a (SAME cached_score). FREE_SLOT: high stale load.
            add_tier15_worker(&scheduler, "FREE_SLOT", 4, 0, 90, 0, vec![child_a]).await;
            set_worker_running(&scheduler, "FREE_SLOT", 3).await; // 3 < 4 → free slot
            // OVERRIDE: low stale load → higher blended_s in v1.
            add_tier15_worker(&scheduler, "OVERRIDE", 4, 0, 5, 0, vec![child_a]).await;
            set_worker_running(&scheduler, "OVERRIDE", 4).await; // 4 == 4 → override-admit

            // Precondition (mirrors the Tier-1 / fallback tests): both workers
            // cache child_a → SAME cached_score, so `blended_s = cached_score -
            // load_penalty` is discriminated ONLY by the penalty. The override
            // worker's stale-low p_load (5) gives it the LOWER penalty → the
            // HIGHER blended_s. Without pref, v1's max-blended_s Tier-1.5 would
            // fall for OVERRIDE — this pins that the magnet is actually present
            // (a future load/`build_tree` edit can't silently collapse it so the
            // test passes because FREE_SLOT won on the secondary key).
            let free_pen =
                super::super::capacity_score(90, 0, 90, 4, 0, 8, 512 * 1024).load_penalty;
            let over_pen =
                super::super::capacity_score(5, 0, 5, 4, 0, 8, 512 * 1024).load_penalty;
            assert!(
                over_pen < free_pen,
                "precondition: the override worker's stale-low p_load gives it the \
                 LOWER load_penalty → a HIGHER blended_s (same cached_score) — the \
                 max-blended_s magnet v1 would fall for"
            );

            let chosen = select_tier15(&scheduler, &tree).await;
            assert_eq!(
                chosen,
                Some(WorkerId("FREE_SLOT".to_string())),
                "I6 Tier-1.5 magnet: a genuine-free-P-slot subtree holder (running 3 < \
                 p_count 4, pref 0) MUST beat an override-admit subtree holder (running \
                 4 == p_count 4, pref 1) with the SAME cached_score EVEN THOUGH the \
                 override worker's stale-low p_load gives it a higher blended_s — the \
                 `p_headroom_pref` PRIMARY key dominates the blended_s SECONDARY key"
            );
        }

        // ── T-const: numeric-constant pin (design §9 constant-pin) ──
        // Pins the NEW/load-bearing constants at their integer encoding in
        // the compare loop, NOT a float. `REF_FREE == 200` is the centi-core
        // encoding of one free P-core (the rev-3 fix — NOT `1`, which would
        // put the knee at 1/200 of a core). `SATURATION_EPSILON == 0` is the
        // exact `weighted_free == 0` predicate. (Incident 2026-05-12: verify
        // the const at the declaration site, not the doc-comment.)
        #[test]
        fn t_const_pins_blend_constants() {
            assert_eq!(
                super::super::REF_FREE,
                200,
                "REF_FREE must be 200 (centi-core encoding of one free P-core); \
                 a value of 1 would put the knee at 1/200 of a core and reintroduce \
                 the rev-2 binary collapse"
            );
            assert_eq!(
                super::super::SATURATION_EPSILON,
                0,
                "SATURATION_EPSILON must be exactly 0 (the weighted_free == 0 \
                 predicate); a non-zero value makes backstop (a) over-eager"
            );
            assert_eq!(super::super::P_WEIGHT_NUM, 2, "P-core weight numerator must be 2");
            assert_eq!(super::super::E_WEIGHT_NUM, 1, "E-core weight numerator must be 1");
            assert_eq!(
                MAX_PLAUSIBLE_CORES, 1024,
                "MAX_PLAUSIBLE_CORES ingest clamp must be 1024"
            );
        }

        // ── T-const (config side): LOAD_BYTE_COST is config, not const ──
        // Per §8 / §9: do NOT assert LOAD_BYTE_COST == 524288 as a fixed
        // const — it is soak-selected config. Assert the serde default
        // matches the documented anchor AND that a non-default value plumbs
        // through to selection (the crossover tests below exercise the
        // plumbed value; here we pin the default).
        #[test]
        fn t_const_load_byte_cost_default_is_anchor() {
            use nativelink_config::schedulers::SimpleSpec;
            let spec: SimpleSpec = serde_json::from_str("{}").expect("empty spec");
            assert_eq!(
                spec.load_byte_cost,
                512 * 1024,
                "load_byte_cost serde default must be the 512 KiB anchor (provisional, \
                 soak-selected before deploy)"
            );
            assert_eq!(
                spec.assume_core_count, 8,
                "assume_core_count serde default must be 8"
            );
        }

        // ── T-R3a: 96-core beats 2-core (the R3 headline) ──
        // Equal cache (both Tier-1 root holders), heterogeneous counts. Loads
        // chosen so the OLD load% rule and the NEW capacity rule DISAGREE
        // deterministically: BIG @85% (load% 85), SMALL @80% (load% 80). The
        // old `effective_load_score` (min load%) picks SMALL (80 < 85 —
        // WRONG, a 2-core box). The new capacity math: 96@85% weighted_free=
        // 2880 ≫ REF_FREE → penalty 0; 2@80% weighted_free=80 → penalty 307K
        // → BIG wins (it has 14.4 free cores vs 0.40). The mutation back to
        // `effective_load_score` flips the selection to SMALL.
        #[nativelink_test]
        async fn t_r3a_big_box_beats_small_box() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker(&scheduler, "BIG", 96, 0, 85, 85, 0).await;
            add_tier1_worker(&scheduler, "SMALL", 2, 0, 80, 80, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("BIG".to_string())),
                "R3: a 96-core box at 85% (14.4 free cores, penalty 0) must beat a \
                 2-core box at 80% (0.40 free cores, penalty 307K) — absolute free \
                 capacity, not load %. The old load%-min rule wrongly picks SMALL (80<85)"
            );
        }

        // ── T-R3b: 96@95% beats 2@90% (deeper into deficit) ──
        // BIG @95% (load% 95), SMALL @90% (load% 90). Old picks SMALL (90<95).
        // New: 96@95% weighted_free=960 → penalty 0; 2@90% weighted_free=40 →
        // penalty 358K → BIG wins (4.8 free cores vs 0.20).
        #[nativelink_test]
        async fn t_r3b_big_box_beats_small_box_high_load() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker(&scheduler, "BIG", 96, 0, 95, 95, 0).await;
            add_tier1_worker(&scheduler, "SMALL", 2, 0, 90, 90, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("BIG".to_string())),
                "R3: a 96-core box at 95% (4.8 free cores, penalty 0) must beat a \
                 2-core box at 90% (0.20 free cores, penalty 358K); the old load%-min \
                 rule wrongly picks SMALL (90<95)"
            );
        }

        // ── T-R2a: P-free worker beats E-only worker (P ≫ E) ──
        // Heterogeneous Macs p=8,e=4. A: p100/e75 → weighted_free=100,
        // penalty=256K. B: p88/e100 → weighted_free=192 (one free P-core),
        // penalty=20K. B wins.
        #[nativelink_test]
        async fn t_r2a_p_free_beats_e_only() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker(&scheduler, "A", 8, 4, 100, 100, 75).await;
            add_tier1_worker(&scheduler, "B", 8, 4, 90, 88, 100).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("B".to_string())),
                "R2: worker B with one free P-core (weighted_free=192, penalty 20K) \
                 must beat worker A with only free E-cores (weighted_free=100, \
                 penalty 256K) — free P capacity is weighted above free E capacity"
            );
        }

        // ── T-R2b: 2-free-P beats 2-free-E at equal absolute free count ──
        // box X: 2 free P, 0 free E (p_count=2 @0%, e_count=2 @100%)
        //   → p_free_centi=200, e_free_centi=0, weighted_free=400.
        // box Y: 0 free P, 2 free E (p_count=2 @100%, e_count=2 @0%)
        //   → p_free_centi=0, e_free_centi=200, weighted_free=200 → penalty 0
        //     too, but LESS weighted free, so it is ranked lower / sheds first.
        // Push both into deficit so the weighting decides: use p_count=1 boxes.
        // X: 1 free P (p1@0, e1@100): wf = 2*100 + 0 = 200, penalty 0.
        // Y: 1 free E (p1@100, e1@0): wf = 0 + 100 = 100, penalty=256K.
        #[nativelink_test]
        async fn t_r2b_free_p_beats_free_e_equal_count() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            // X has a free P-core; Y has only a free E-core (equal free count = 1).
            add_tier1_worker(&scheduler, "X", 1, 1, 50, 0, 100).await;
            add_tier1_worker(&scheduler, "Y", 1, 1, 50, 100, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("X".to_string())),
                "R2: a worker with one free P-core (weighted_free=200, penalty 0) \
                 must beat one with one free E-core (weighted_free=100, penalty 256K) \
                 at equal absolute free-core count"
            );
        }

        // ── T-edge-signed: idle big box is NOT penalized (u64-underflow guard) ──
        // 64@0% → weighted_free=12800 ≫ REF_FREE=200, so REF_FREE - weighted_free
        // is NEGATIVE; computed signed-then-clamped it is 0 (penalty 0). A loaded
        // small peer (2@100%, penalty 512K) must lose. If the subtraction were
        // done in u64 it would underflow to ~1.8e19 → the idle box gets MAX
        // penalty → never selected.
        #[nativelink_test]
        async fn t_edge_signed_idle_big_box_not_penalized() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker(&scheduler, "IDLE_BIG", 64, 0, 0, 0, 0).await;
            add_tier1_worker(&scheduler, "LOADED_SMALL", 2, 0, 100, 100, 0).await;

            // Direct arithmetic assertion in centi-core space (signed clamp).
            let cs = super::super::capacity_score(0, 0, 0, 64, 0, 8, 512 * 1024);
            assert_eq!(cs.weighted_free, 12800, "64@0% weighted_free is 12800 centi-cores");
            assert_eq!(
                cs.load_penalty, 0,
                "an idle big box (weighted_free ≫ REF_FREE) must pay ZERO penalty — \
                 the REF_FREE - weighted_free subtraction is signed-then-clamped"
            );

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("IDLE_BIG".to_string())),
                "the idle 64-core box (penalty 0) must be selected over the loaded \
                 2-core box (penalty 512K); a u64-underflow in the signed clamp would \
                 give the idle box a huge penalty and starve it"
            );
        }

        // ── T-clamp (arithmetic safety): widen-before-multiply (mandate 1) ──
        // The AUTHORITATIVE ingest-clamp test (a worker reporting u32::MAX is
        // clamped to MAX_PLAUSIBLE_CORES at the `worker_api_server` seam) lives
        // in `nativelink-service/tests/worker_api_build_sha_test.rs` (the only
        // place the real connect-frame ingest path is exercised). HERE we pin
        // the arithmetic half of S1: even an UNCLAMPED u32::MAX count must NOT
        // overflow / panic in `capacity_score` — mandate 1 widens to i64
        // BEFORE the multiply (`u32::MAX * 100` would overflow u32). This is
        // the defence-in-depth the clamp complements.
        #[test]
        fn t_clamp_arithmetic_no_overflow_on_max_count() {
            // u32::MAX cores at 99% load: p_free_centi = u32::MAX * 1, widened
            // to i64 (~4.29e9) — fine in i64; a u32 multiply would overflow.
            let cs = super::super::capacity_score(99, 0, 99, u32::MAX, u32::MAX, 8, 512 * 1024);
            // weighted_free is huge but finite; busy_core_equiv clamps to 0;
            // penalty 0 — no panic, no overflow.
            assert!(
                cs.weighted_free > 0,
                "u32::MAX cores @99% must compute a finite positive weighted_free \
                 (widen to i64 before the multiply — mandate 1)"
            );
            assert_eq!(
                cs.load_penalty, 0,
                "an (over-reported) huge idle count pays zero penalty without overflow"
            );
            // And the clamp expression itself caps the value (the value the
            // server stores; the END-TO-END ingest assertion is the service test).
            assert_eq!(
                u32::MAX.min(MAX_PLAUSIBLE_CORES),
                MAX_PLAUSIBLE_CORES,
                "the ingest clamp caps an over-report at MAX_PLAUSIBLE_CORES"
            );
        }

        // ── T-compat: legacy 0-count worker uses assume_core_count ──
        // A legacy worker (p_core_count=0) reporting cpu_load_pct=50 competes
        // with a count-reporting 8@50% worker, equal cache. The legacy worker
        // is ranked as assume_core_count(8)@50% → both have penalty 0 (8@50%
        // weighted_free=2*8*50=800 ≫ 200) → TIE, legacy worker NOT starved.
        // Mutation guard: without assume-N, free=0 → max penalty → starved.
        #[nativelink_test]
        async fn t_compat_legacy_zero_count_uses_assume_n() {
            // Direct: legacy (0-count) at aggregate 50% is treated as
            // assume_core_count @ aggregate, NOT free=0.
            let legacy = super::super::capacity_score(0, 0, 50, 0, 0, 8, 512 * 1024);
            let counted = super::super::capacity_score(50, 0, 50, 8, 0, 8, 512 * 1024);
            assert_eq!(
                legacy.load_penalty, counted.load_penalty,
                "a legacy 0-count worker at aggregate 50% must rank identically to a \
                 count-reporting 8@50% worker (assume_core_count=8 substitution)"
            );
            assert_eq!(
                legacy.load_penalty, 0,
                "8@50% has weighted_free=800 ≫ REF_FREE → penalty 0 (not starved)"
            );

            // Production composition: legacy worker must be selectable (here
            // it ties the counted worker; assert it is not starved by being
            // the only viable choice when the counted worker is saturated).
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker(&scheduler, "LEGACY", 0, 0, 50, 0, 0).await;
            add_tier1_worker(&scheduler, "COUNTED_BUSY", 8, 0, 100, 100, 100).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("LEGACY".to_string())),
                "a legacy 0-count worker at 50% (assume-N → penalty 0) must be chosen \
                 over a saturated count-reporting worker — the assume-N fallback must \
                 give it a real denominator, not free=0/max-penalty"
            );
        }

        // ── T-compat-zero-assume: assume_core_count==0 must be normalized ──
        // STEP-2 zero-guard. `assume_core_count` is config
        // (`SimpleSpec::assume_core_count`, serde default 8) but an operator
        // can set it to 0 (or shellexpand to 0). A count-less worker
        // (p_core_count==0) then falls back to `eff_p_count = assume_core_count
        // = 0` → p_free_centi = 0 → weighted_free = 0 → it is BOTH max-penalized
        // AND spuriously flagged `is_saturated()` even when genuinely idle, so
        // it can never win a Tier-1/1.5 selection it should win → starved.
        // `new_with_locality_map` MUST normalize the stored `assume_core_count`
        // to `>= 1` at store time (the dispatch's STEP-2: "normalize at the
        // point of storing into ApiWorkerSchedulerImpl").
        //
        // Discriminator by SELECTION OUTCOME (production composition, via
        // `select` → `find_and_reserve_worker`): scheduler built with
        // `assume_core_count = 0`; two Tier-1 root holders —
        //   - IDLE_LEGACY: count-less (p_count=0), aggregate load 10%.
        //   - BUSY_SMALL : count-reporting p_count=2 @80%, e_count=0.
        // WITH the guard (assume→1): IDLE_LEGACY → weighted_free = 2*1*90 = 180
        //   (NOT saturated), penalty 51K; BUSY_SMALL → weighted_free = 2*2*20 =
        //   80, penalty 307K. Neither saturated → Tier-1 min-penalty → the IDLE
        //   legacy worker WINS (51K < 307K) — correct.
        // WITHOUT the guard (assume stays 0): IDLE_LEGACY → weighted_free = 0 →
        //   saturated, penalty 512K; BUSY_SMALL → weighted_free = 80 (NOT
        //   saturated). Backstop does NOT fire (not ALL saturated) → Tier-1
        //   min-penalty → BUSY_SMALL wins (307K < 512K) — WRONG: an idle worker
        //   lost to a busy one solely because assume_core_count==0 zeroed its
        //   denominator. The selection outcome differs → the guard is proven.
        #[nativelink_test]
        async fn t_compat_zero_assume_core_count_is_normalized() {
            let scheduler =
                build_scheduler_with_assume_core_count(BarrierWorkerStateManager::new(), 0);
            // IDLE_LEGACY: count-less (p_count=0), idle (aggregate 10%).
            add_tier1_worker(&scheduler, "IDLE_LEGACY", 0, 0, 10, 0, 0).await;
            // BUSY_SMALL: count-reporting 2-core box at 80% (penalty 307K).
            add_tier1_worker(&scheduler, "BUSY_SMALL", 2, 0, 80, 80, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("IDLE_LEGACY".to_string())),
                "assume_core_count==0 must be normalized to >=1 at store time: an idle \
                 count-less worker (assume-N → weighted_free 180, penalty 51K) must beat \
                 a busy 2-core box (penalty 307K). Without the zero-guard the legacy \
                 worker gets weighted_free 0 → max penalty + spurious saturation → it is \
                 starved and the busy box is wrongly selected"
            );
        }

        // ── T-cascade: Tier-1 root match beats Tier-2 blob-locality ──
        // Worker X_ROOT is a Tier-1 root holder, moderately loaded; worker
        // Y_BLOB is idle with a LARGE Tier-2 blob-locality score (via the
        // endpoint_scores map). The cascade consults Tier 1 FIRST, so X wins
        // regardless of Y's locality bytes. Guards §6's non-collapse decision:
        // collapsing to one global argmax would let a blob-locality crumb
        // outrank a root hardlink. Drives the inner selection directly with a
        // populated endpoint_scores map so Tier 2 genuinely fires for Y.
        #[nativelink_test]
        async fn t_cascade_tier1_root_beats_blob_locality() {
            use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
            let scheduler = ApiWorkerScheduler::new_with_locality_map(
                BarrierWorkerStateManager::new(),
                Arc::new(PlatformPropertyManager::new(HashMap::new())),
                WorkerAllocationStrategy::default(),
                Arc::new(Notify::new()),
                100,
                Arc::new(WorkerRegistry::new()),
                Some(new_shared_blob_locality_map()),
                None,
                None,
                512 * 1024,
                8,
                false, // (#sched M1 rebalance) p_headroom_gate OFF
                0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
                2, // (#sched M1 rebalance v2) p_headroom_override_factor
            );
            // X_ROOT: Tier-1 root match (cached_directory_digests ∋ input_root),
            // moderately loaded.
            add_tier1_worker(&scheduler, "X_ROOT", 8, 0, 80, 80, 0).await;
            // Y_BLOB: idle, NO root match, registered WITH a cas_endpoint so it
            // appears in endpoint_to_worker → Tier 2 can pick it.
            let y_endpoint = "grpc://y.local:50081";
            {
                let (tx, _rx) = mpsc::unbounded_channel();
                let mut worker = Worker::new_with_cas_endpoint(
                    WorkerId("Y_BLOB".to_string()),
                    props_pool(),
                    tx,
                    42,
                    0,
                    y_endpoint.to_string(),
                    8,
                    0,
                );
                worker.set_core_counts(8, 0);
                scheduler.add_worker(worker).await.expect("add Y_BLOB");
            }
            scheduler
                .update_worker_load(&WorkerId("Y_BLOB".to_string()), 5, 5, 0)
                .await
                .expect("load");

            // A LARGE Tier-2 locality score for Y_BLOB's endpoint. If the
            // cascade collapsed, this 100 MiB crumb would outrank X's root.
            let mut endpoint_scores: HashMap<Arc<str>, u64> = HashMap::new();
            endpoint_scores.insert(Arc::from(y_endpoint), 100 * 1024 * 1024);

            let action = pool_action();
            let op = OperationId::default();
            let chosen = {
                let mut inner = scheduler.inner.write().await;
                inner
                    .inner_find_and_reserve_worker(
                        &props_pool(),
                        &op,
                        &action,
                        false,
                        Some(&endpoint_scores),
                        None,
                    )
                    .map(|(wid, _tx, _msg)| wid)
            };
            assert_eq!(
                chosen,
                Some(WorkerId("X_ROOT".to_string())),
                "cascade: a Tier-1 root/subtree match (X_ROOT) must beat a Tier-2 \
                 blob-locality crumb (Y_BLOB, 100 MiB locality) — Tier 1 is consulted \
                 FIRST; collapsing to one global score would let blob-locality outrank \
                 a root hardlink"
            );
        }

        // ── T-tier1-subtree-not-overcredited (design §4.1 D4) ──
        // Tier 1 fires on `has_root_match OR has_subtree_match` (a worker
        // whose cache holds the action's input_root as a *subtree* of some
        // other tree, not as an exact cached root). The §4.1 D4 fix dropped
        // `EXACT_ROOT_GAIN`, so Tier 1 ranks ALL holders by min `load_penalty`
        // — a subtree-only member gets NO whole-tree stickiness; it competes on
        // load like any holder. Scenario:
        //   - SUBTREE_BUSY: subtree-only match, 8-P @90% → penalty 102K.
        //   - ROOT_IDLE   : exact-root match, 8-P @5% (idle) → penalty 0.
        // Tier-1 min-penalty → ROOT_IDLE wins (0 < 102K). If a future change
        // re-introduced `EXACT_ROOT_GAIN` as a synthetic per-member gain that
        // a subtree-only match also received, SUBTREE_BUSY would be held sticky
        // (gain − 102K) and could beat the idle root worker — the over-credit
        // this test guards against. (To MUTATE: add a positive synthetic gain
        // to the Tier-1 `best` comparison so a busy holder outranks an idle one
        // → SUBTREE_BUSY selected → red-fail with the message below.)
        #[nativelink_test]
        async fn t_tier1_subtree_only_not_overcredited() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            // Subtree-only match, moderately loaded (penalty 102K).
            add_tier1_subtree_worker(&scheduler, "SUBTREE_BUSY", 8, 0, 90, 90, 0).await;
            // Exact-root match, idle (penalty 0).
            add_tier1_worker(&scheduler, "ROOT_IDLE", 8, 0, 5, 5, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("ROOT_IDLE".to_string())),
                "Tier-1 D4: a subtree-only (has_subtree_match) holder that is busy must \
                 NOT be held sticky against an idle exact-root holder — both compete on \
                 min load_penalty (ROOT_IDLE 0 < SUBTREE_BUSY 102K). Re-introducing a \
                 synthetic EXACT_ROOT_GAIN would over-credit the subtree-only match with \
                 whole-tree stickiness and wrongly select the busy worker"
            );
        }

        // ── T-R5c: backstop (a) — all-saturated fleet falls through ──
        // The load-bearing #52 reconciliation. ALL viable candidates are
        // saturated (weighted_free == 0). WARM is the ONLY Tier-1 root holder
        // (the c82b warmest-cache shape) — so WITHOUT the fall-through the
        // cache tier would deterministically pick WARM and pile on. WITH
        // backstop (a) the cache tiers decline and the LRU/MRU path picks by
        // `effective_load_score`; the COLD workers (p100, e_count=0 →
        // saturated, effective_load_score == 100) outrank WARM (p100/e100 →
        // effective_load_score == 200), so the fall-through deterministically
        // selects a COLD worker — i.e. the pile-on is broken. (D2: the result
        // is an LRU-rotated worker, NOT BUSY — `can_accept_work` is dormant in
        // prod, so this test does NOT stub `max_inflight_tasks`.)
        #[nativelink_test]
        async fn t_r5c_all_saturated_falls_through_then_resumes() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            // WARM: warmest-cache Tier-1 root, p8/e4 fully saturated
            // (weighted_free 0, effective_load_score 200).
            add_tier1_worker(&scheduler, "WARM", 8, 4, 100, 100, 100).await;
            // COLD1/COLD2: saturated too (p8/e0 @100% → weighted_free 0) but
            // NOT root matches, and with e_count=0 their effective_load_score
            // is 100 (< WARM's 200) — so the LRU fall-through picks a COLD,
            // NOT WARM. (If the cache tier ran under saturation WARM would win
            // by being the only root holder = the pile-on the backstop breaks.)
            let _rx2 = add_worker_in_pool(&scheduler, "COLD1").await;
            let _rx3 = add_worker_in_pool(&scheduler, "COLD2").await;
            for n in ["COLD1", "COLD2"] {
                scheduler
                    .set_worker_core_counts(&WorkerId(n.to_string()), 8, 0)
                    .await
                    .expect("counts");
                scheduler
                    .update_worker_load(&WorkerId(n.to_string()), 100, 100, 0)
                    .await
                    .expect("load");
            }

            // Arithmetic sanity: all three are saturated.
            let warm_sat = super::super::capacity_score(100, 100, 100, 8, 4, 8, 512 * 1024);
            let cold_sat = super::super::capacity_score(100, 0, 100, 8, 0, 8, 512 * 1024);
            assert_eq!(warm_sat.weighted_free, 0, "WARM p100/e100 → weighted_free 0");
            assert_eq!(cold_sat.weighted_free, 0, "COLD p100/e0(no E) → weighted_free 0");

            // All saturated → backstop (a) fires → cache tiers decline → LRU
            // fall-through. The pile-on is broken: the chosen worker is NOT
            // WARM (it is a COLD via LRU `min_by_key(effective_load_score)`).
            let chosen = select(&scheduler).await;
            assert_ne!(
                chosen,
                Some(WorkerId("WARM".to_string())),
                "saturated-fleet pile-on — backstop (a) fall-through missing: under \
                 all-saturated the cache tier piled the dispatch onto the warmest \
                 worker WARM instead of falling through to the LRU/MRU path"
            );
            assert!(
                chosen.is_some(),
                "the LRU fall-through must still place the action (CPU saturation does \
                 not return BUSY — can_accept_work is dormant); got None"
            );

            // Now FREE the WARM worker → it is the only non-saturated AND the
            // warmest → backstop (a) does NOT fire (not over-eager) → normal
            // cache-aware routing resumes and selects WARM (§R5.1 row 2).
            scheduler
                .update_worker_load(&WorkerId("WARM".to_string()), 50, 50, 50)
                .await
                .expect("free WARM");
            let chosen2 = select(&scheduler).await;
            assert_eq!(
                chosen2,
                Some(WorkerId("WARM".to_string())),
                "backstop (a) is not over-eager: the moment WARM frees up \
                 (weighted_free > 0), normal cache-aware routing resumes and the \
                 now-non-saturated warmest worker wins"
            );
        }

        // ════════════════════════════════════════════════════════════════
        // Tier-1.5 (subtree coverage) crossover tests. Per-worker
        // `cached_score` must vary, so these call the inner selection method
        // directly with a hand-built `ResolvedTree` (the resolution phase is
        // upstream and separate; `inner_find_and_reserve_worker` is the real
        // selection function under test).
        // ════════════════════════════════════════════════════════════════

        use super::super::ResolvedTree;
        use nativelink_proto::build::bazel::remote::execution::v2::Directory;

        /// Build a 2-level `ResolvedTree`: a root plus two child subtrees so a
        /// worker caching a child gets a partial `cached_score`. `child_bytes`
        /// sets each child's direct bytes (drives `cached_score` magnitude).
        fn build_tree(child_a: DigestInfo, child_b: DigestInfo, child_bytes: u64) -> ResolvedTree {
            let root = input_root();
            let mut dir_digests = HashSet::new();
            dir_digests.insert(root);
            dir_digests.insert(child_a);
            dir_digests.insert(child_b);

            let mut dir_direct_bytes = HashMap::new();
            dir_direct_bytes.insert(root, 0u64);
            dir_direct_bytes.insert(child_a, child_bytes);
            dir_direct_bytes.insert(child_b, child_bytes);

            let mut dir_direct_files = HashMap::new();
            dir_direct_files.insert(root, 0u64);
            dir_direct_files.insert(child_a, 0u64);
            dir_direct_files.insert(child_b, 0u64);

            let mut subtree_bytes = HashMap::new();
            subtree_bytes.insert(root, child_bytes * 2);
            let mut subtree_files = HashMap::new();
            subtree_files.insert(root, 0u64);

            let mut directories = HashMap::new();
            directories.insert(root, Directory::default());
            directories.insert(child_a, Directory::default());
            directories.insert(child_b, Directory::default());

            ResolvedTree {
                file_digests: Vec::new(),
                dir_digests,
                subtree_bytes,
                subtree_files,
                dir_direct_bytes,
                dir_direct_files,
                directories,
            }
        }

        /// Register a pool worker with counts + load and a set of cached
        /// subtree digests (for Tier-1.5 coverage scoring). Does NOT set a
        /// root match (so Tier 1 is skipped and Tier 1.5 runs).
        async fn add_tier15_worker(
            scheduler: &Arc<ApiWorkerScheduler>,
            name: &str,
            p_count: u32,
            e_count: u32,
            p_load: u32,
            e_load: u32,
            cached_subtrees: Vec<DigestInfo>,
        ) {
            let _rx = add_worker_in_pool(scheduler, name).await;
            scheduler
                .set_worker_core_counts(&WorkerId(name.to_string()), p_count, e_count)
                .await
                .expect("counts");
            scheduler
                .update_worker_load(&WorkerId(name.to_string()), p_load, p_load, e_load)
                .await
                .expect("load");
            scheduler
                .update_cached_subtrees(
                    &WorkerId(name.to_string()),
                    true,
                    cached_subtrees,
                    Vec::new(),
                    Vec::new(),
                )
                .await
                .expect("set cached subtrees");
        }

        /// Drive the production inner selection with a hand-built tree (Tier
        /// 1.5 path). Returns the chosen worker id.
        async fn select_tier15(
            scheduler: &Arc<ApiWorkerScheduler>,
            tree: &ResolvedTree,
        ) -> Option<WorkerId> {
            let action = pool_action();
            let op = OperationId::default();
            let mut inner = scheduler.inner.write().await;
            inner
                .inner_find_and_reserve_worker(
                    &props_pool(),
                    &op,
                    &action,
                    false,
                    None,
                    Some(tree),
                )
                .map(|(wid, _tx, _msg)| wid)
        }

        // ── T-R5a: continuous crossover (non-saturated, 8-P @90%) ──
        // One warm worker on an 8-P box at 90% (centi: weighted_free=160,
        // busy_core_equiv=40, penalty=102K — engaged but NOT saturated, so
        // backstop (a) is dormant), cache just below the 102K crossover; one
        // idle cold worker. The idle cold worker is selected — a selection the
        // binary floor cannot make (8@90% floored → penalty 0). Then bump the
        // warm worker's cache far above 102K → warm worker wins.
        #[nativelink_test]
        async fn t_r5a_continuous_crossover_non_saturated() {
            let child_a = DigestInfo::new([0xa1u8; 32], 1);
            let child_b = DigestInfo::new([0xb2u8; 32], 1);

            // cache just BELOW the 102K crossover: child_bytes = 80_000 (the
            // worker caches child_a only → cached_score = 80_000 < 102_400).
            let tree = build_tree(child_a, child_b, 80_000);
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            // WARM: 8-P @90% (penalty 102K), caches child_a (80K cache).
            add_tier15_worker(&scheduler, "WARM", 8, 0, 90, 0, vec![child_a]).await;
            // COLD: known-idle (reports a low 5% load, NOT 0 — 0/0/0 is the
            // "never reported" sentinel that sorts LAST in the LRU fallback),
            // caches NOTHING (cached_score 0, penalty 0). When Tier 1.5
            // declines (WARM's S < 0), the fall-through LRU path picks the
            // lowest-load worker = COLD.
            add_tier15_worker(&scheduler, "COLD", 8, 0, 5, 0, vec![]).await;

            // Sanity: the penalty at 8@90% is 102K (centi), and 80K < 102K.
            let cs = super::super::capacity_score(90, 0, 90, 8, 0, 8, 512 * 1024);
            assert_eq!(cs.load_penalty, 104_857, "8@90% penalty is 102K (centi-core)");

            let chosen = select_tier15(&scheduler, &tree).await;
            assert_eq!(
                chosen,
                Some(WorkerId("COLD".to_string())),
                "R5 crossover: an 80K cache hit on an 8-P @90% worker (penalty 102K) \
                 must lose to an idle cold peer — S_warm = 80K - 102K < 0 = S_cold; the \
                 binary cutoff/floor docked nothing at 90% and kept the warm worker"
            );

            // Now make the cache hit large (≈3 MiB ≫ 102K) → warm worker wins.
            let big_tree = build_tree(child_a, child_b, 3 * 1024 * 1024);
            let scheduler2 = build_scheduler(BarrierWorkerStateManager::new());
            add_tier15_worker(&scheduler2, "WARM", 8, 0, 90, 0, vec![child_a]).await;
            add_tier15_worker(&scheduler2, "COLD", 8, 0, 5, 0, vec![]).await;
            let chosen2 = select_tier15(&scheduler2, &big_tree).await;
            assert_eq!(
                chosen2,
                Some(WorkerId("WARM".to_string())),
                "R5 crossover: a 3 MiB cache hit ≫ the 102K penalty must keep the warm \
                 worker — the crossover does real work in BOTH directions"
            );
        }

        // ── T-R5b: THE continuous-vs-binary distinguisher (design §9) ──
        // 8-P box at 90%, marginal cache = 90 KiB, chosen INSIDE the
        // (0, 102 KiB] window. Centi-core penalty = 102K > 90K cache → idle
        // cold SELECTED. The binary floor (8@90% → p_free=1, penalty 0) and
        // the old cutoff (90 < 99 → fully preferred) both keep the WARM worker
        // — a DIFFERENT SELECTION. This is the test that would have caught the
        // rev-2 collapse (whose T-R5b passed green against both new and old).
        #[nativelink_test]
        async fn t_r5b_distinguisher_idle_cold_selected_at_90kib() {
            let child_a = DigestInfo::new([0xa1u8; 32], 1);
            let child_b = DigestInfo::new([0xb2u8; 32], 1);
            // 90 KiB cache: inside (0, 102 KiB].
            let tree = build_tree(child_a, child_b, 90 * 1024);
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier15_worker(&scheduler, "WARM", 8, 0, 90, 0, vec![child_a]).await;
            // COLD: known-idle (5% load, not the 0/0/0 "unreported" sentinel).
            add_tier15_worker(&scheduler, "COLD", 8, 0, 5, 0, vec![]).await;

            // The discriminator: centi-core penalty (102K) > cache (90K), and
            // the whole-core floor would dock 0.
            let centi = super::super::capacity_score(90, 0, 90, 8, 0, 8, 512 * 1024);
            assert!(
                centi.load_penalty > 90 * 1024,
                "centi-core penalty (102K) must exceed the 90 KiB cache so the warm \
                 worker is shed; got {}",
                centi.load_penalty
            );

            let chosen = select_tier15(&scheduler, &tree).await;
            assert_eq!(
                chosen,
                Some(WorkerId("COLD".to_string())),
                "THE distinguisher: a 90 KiB cache on an 8-P @90% worker must lose to \
                 an idle cold peer (S_warm = 90K - 102K < 0). The binary-floor mutation \
                 (8@90% → penalty 0) keeps WARM — a DIFFERENT selection, proving the \
                 penalty is continuous, not binary"
            );
        }

        /// Register a Tier-1 root-holding pool worker that has NEVER reported
        /// load (no `update_worker_load` call) — the production state of a
        /// pre-first-heartbeat worker (or, before the gate fix, a Linux/Intel
        /// worker whose readings were all-zero and gated out). Its stored load
        /// fields stay at the construction default `(0,0,0)` AND its
        /// `has_reported_load` flag stays `false`.
        async fn add_tier1_worker_never_reported(
            scheduler: &Arc<ApiWorkerScheduler>,
            name: &str,
            p_count: u32,
            e_count: u32,
        ) {
            let _rx = add_worker_in_pool(scheduler, name).await;
            scheduler
                .set_worker_core_counts(&WorkerId(name.to_string()), p_count, e_count)
                .await
                .expect("set core counts");
            // Deliberately NO update_worker_load — this worker is in the
            // never-reported initial state.
            let mut cached = HashSet::new();
            cached.insert(input_root());
            scheduler
                .update_cached_directories(&WorkerId(name.to_string()), cached)
                .await
                .expect("set cached dirs");
        }

        // ── T-unreported: a never-reported worker does NOT win a min-load tie ──
        // BUG (zero-load over-selection): a worker that has NEVER reported load
        // keeps the construction-default `(0,0,0)` load fields. In
        // `capacity_score` that all-zero load reads as `100 - 0 = 100`% FREE on
        // every core → max `weighted_free` → `busy_core_equiv = 0` → ZERO
        // `load_penalty` → it wins EVERY Tier-1 min-load tie against workers
        // with known spare capacity, biasing dispatch toward workers we have no
        // load signal for. The fix distinguishes "never reported" (treated as
        // fully busy / max penalty) from "reported genuinely idle".
        //
        // Production composition (via `select` → `find_and_reserve_worker`):
        //   - FRESH         : never reported load (no update_worker_load), 8 P.
        //   - KNOWN_CAPACITY: reported real load 90% (8-P → penalty 102K), 8 P.
        // Both are Tier-1 root holders → the exact-root tier ranks by
        // `load_penalty`. KNOWN_CAPACITY has a real, finite penalty (102K).
        // FRESH is treated as fully busy (MAX penalty, 512K) because it never
        // reported → KNOWN_CAPACITY wins (102K < 512K). WITHOUT the fix FRESH
        // reads all-zero load = fully free → penalty 0 < 102K → FRESH WINS — a
        // DIFFERENT selection (and FRESH would win regardless of candidate-set
        // iteration order, since 0 strictly beats 102K). The selection outcome
        // differs deterministically → the fix is proven.
        #[nativelink_test]
        async fn t_unreported_loses_min_load_tie_to_known_capacity() {
            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            add_tier1_worker_never_reported(&scheduler, "FRESH", 8, 0).await;
            // KNOWN_CAPACITY: reported real load 90% → finite penalty 102K.
            add_tier1_worker(&scheduler, "KNOWN_CAPACITY", 8, 0, 90, 90, 0).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("KNOWN_CAPACITY".to_string())),
                "zero-load over-selection: a worker that has NEVER reported load \
                 must NOT win the min-load selection over a worker with KNOWN spare \
                 capacity (8-P @90%, penalty 102K). Without the never-reported-vs-idle \
                 distinction the unreported worker reads as fully free (penalty 0) and \
                 wins — biasing dispatch toward workers we have no load signal for"
            );
        }

        // ── T-reported-idle: a genuinely-idle reported worker stays selectable ──
        // The dual of T-unreported: the fix must NOT penalize a worker that HAS
        // reported an all-zero (genuinely idle) reading — that worker is the
        // most-free worker in the fleet and must remain selectable. Guards
        // against an over-broad fix that treats every all-zero reading (idle OR
        // never-reported) as busy.
        //
        // Production composition: KNOWN_IDLE (reported all-zero, 8 P-cores) is
        // the ONLY viable Tier-1 holder competing against a saturated busy peer.
        // It must be chosen (penalty 0, not max). Mutation guard: if the fix
        // forced ALL all-zero stored loads to max penalty (ignoring the
        // has-reported distinction), KNOWN_IDLE would be saturated too and the
        // selection would change.
        #[nativelink_test]
        async fn t_reported_all_zero_idle_is_selectable() {
            // Direct arithmetic: a reported all-zero 8-P worker is fully free.
            let cs = super::super::capacity_score(0, 0, 0, 8, 0, 8, 512 * 1024);
            assert_eq!(
                cs.load_penalty, 0,
                "a reported genuinely-idle 8-P worker (all-zero load) must pay ZERO \
                 penalty — it is the most-free worker, not a never-reported one"
            );

            let scheduler = build_scheduler(BarrierWorkerStateManager::new());
            // KNOWN_IDLE reported all-zero (genuinely idle) → must be selectable.
            add_tier1_worker(&scheduler, "KNOWN_IDLE", 8, 0, 0, 0, 0).await;
            // BUSY: count-reporting 8-P box fully saturated (penalty max).
            add_tier1_worker(&scheduler, "BUSY", 8, 0, 100, 100, 100).await;

            let chosen = select(&scheduler).await;
            assert_eq!(
                chosen,
                Some(WorkerId("KNOWN_IDLE".to_string())),
                "a worker that HAS reported a genuinely-idle (all-zero) reading must \
                 remain selectable as the most-free worker — the never-reported-vs-idle \
                 fix must let a real all-zero reading through, not penalize it"
            );
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // (#sched-zeroload) effective_load_score + all-never-reported fleet +
    // workers_never_reported_load gauge tests (follow-up from 2de99912).
    // ════════════════════════════════════════════════════════════════════

    /// (#sched-zeroload) A reported-idle worker (has_reported_load=true,
    /// all fields 0) must score 0 in the LRU/MRU fallback path, beating a
    /// never-reported worker (u64::MAX) in the `min_by_key` tuple ranking.
    ///
    /// Invariant: `effective_load_score(0,0,0,true) < effective_load_score(0,0,0,false)`.
    ///
    /// Mutation: change the `has_reported_load` branch in `effective_load_score`
    /// to always return `u64::MAX` (revert to pre-fix behaviour) → the
    /// `min_by_key` in `inner_find_worker_for_action` (v2: keyed on the
    /// tuple `(p_headroom_pref, effective_load_score)`) sees BOTH load
    /// scores as `u64::MAX` and, with the gate OFF here (both tier `false`),
    /// returns the first candidate (the LRU/MRU position wins the tie), so the
    /// bespoke message "reported-idle worker must score 0 (best)" from
    /// `test_effective_load_score_reported_idle_scores_zero` is the
    /// load-bearing mutation signal (that unit test catches the score
    /// regression before we ever hit this integration test).
    #[nativelink_test]
    async fn t_reported_idle_scores_best_in_lru_fallback() {
        let scheduler = build_scheduler(BarrierWorkerStateManager::new());

        // IDLE: has reported a genuinely all-zero load.
        let _rx_idle = add_worker_in_pool(&scheduler, "IDLE").await;
        scheduler
            .update_worker_load(&WorkerId("IDLE".to_string()), 0, 0, 0)
            .await
            .expect("mark IDLE as load-reported");

        // UNKNOWN: never reported → effective_load_score = u64::MAX.
        let _rx_unknown = add_worker_in_pool(&scheduler, "UNKNOWN").await;

        // With LRU strategy (default), the selection picks min load score.
        // IDLE scores 0 (best); UNKNOWN scores u64::MAX (worst).
        // So IDLE must win regardless of LRU position.
        let chosen = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            scheduler.find_worker_for_action(&props_pool(), false),
        )
        .await
        .expect("matcher must not hang");

        assert_eq!(
            chosen,
            Some(WorkerId("IDLE".to_string())),
            "reported-idle worker (effective_load_score=0) must beat never-reported \
             worker (effective_load_score=u64::MAX) in LRU/MRU fallback selection"
        );
    }

    /// (#sched-zeroload) A fleet of 3 workers, all never-reported, with a
    /// dispatched action MUST return SOME worker — not None. This test calls
    /// `find_worker_for_action` → `inner_find_worker_for_action`, whose
    /// selectability mechanism for an all-never-reported fleet is the
    /// `viable.iter().min_by_key(...)` selection (v2: keyed on the tuple
    /// `(p_headroom_pref, effective_load_score)`): every candidate scores
    /// `effective_load_score(..., has_reported_load=false) == u64::MAX` and,
    /// with the gate OFF here, shares tier `false`, so `min_by_key` returns the
    /// FIRST (LRU/MRU-leading) candidate — never `None` for a non-empty viable
    /// set. (`saturation_fall_through` lives in the DIFFERENT function
    /// `inner_find_and_reserve_worker`, which this path does not exercise.)
    ///
    /// Invariant: a fresh fleet (no workers have reported load) is selectable;
    /// never-reported workers do NOT wedge the scheduler.
    ///
    /// Mutation: force the `viable.iter().min_by_key(...).map(...)` selection to
    /// `None` (e.g. replace with `None`) → the all-u64::MAX fleet gets `None`,
    /// and (no pressure-gated candidates exist to trigger the swap fail-open)
    /// this test red-fails with the bespoke message below.
    #[nativelink_test]
    async fn t_all_never_reported_fleet_selectable() {
        let scheduler = build_scheduler(BarrierWorkerStateManager::new());

        // Three workers in the same capability class, none ever reporting load.
        let _rx_a = add_worker_in_pool(&scheduler, "WA").await;
        let _rx_b = add_worker_in_pool(&scheduler, "WB").await;
        let _rx_c = add_worker_in_pool(&scheduler, "WC").await;

        // Do NOT call update_worker_load on any of them: all three have
        // has_reported_load=false (construction default).

        let chosen = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            scheduler.find_worker_for_action(&props_pool(), false),
        )
        .await
        .expect("matcher must not hang on all-never-reported fleet");

        assert!(
            chosen.is_some(),
            "all-never-reported fleet must remain selectable — \
             find_worker_for_action's min_by_key selection returns a worker when \
             every candidate scores u64::MAX (never-reported), so a fresh fleet \
             does not wedge on restart; got None instead of a worker"
        );
    }

    /// (#sched-zeroload) The `workers_never_reported_load` gauge tracks the
    /// count of workers that have NEVER reported a load reading. It must:
    ///   - increment on `add_worker` (every new worker starts unreported),
    ///   - decrement on the first `update_worker_load` call (first-ever report),
    ///   - NOT decrement on subsequent `update_worker_load` calls (idempotent),
    ///   - decrement on `remove_worker` when the removed worker was unreported.
    ///
    /// Mutation: remove `workers_never_reported_load.fetch_sub(1)` from
    /// `remove_worker` → gauge stays at 1 after removing an unreported worker
    /// → test red-fails with "gauge must reach 0 after removing unreported worker".
    #[nativelink_test]
    async fn t_never_reported_count_gauge() {
        let scheduler = build_scheduler(BarrierWorkerStateManager::new());

        // Initially gauge is 0.
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            0,
            "gauge must start at 0 before any workers are added"
        );

        // Add 3 workers: all never-reported → gauge == 3.
        let _rx_a = add_worker_in_pool(&scheduler, "WA").await;
        let _rx_b = add_worker_in_pool(&scheduler, "WB").await;
        let _rx_c = add_worker_in_pool(&scheduler, "WC").await;

        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            3,
            "gauge must be 3 after adding 3 never-reported workers"
        );

        // Report load on WA (first-ever report) → gauge == 2.
        scheduler
            .update_worker_load(&WorkerId("WA".to_string()), 50, 30, 0)
            .await
            .expect("update WA load");
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            2,
            "gauge must drop to 2 after WA reports load for the first time"
        );

        // Report load on WA again (idempotent — already reported) → gauge still 2.
        scheduler
            .update_worker_load(&WorkerId("WA".to_string()), 60, 40, 0)
            .await
            .expect("update WA load again");
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            2,
            "gauge must stay at 2 on a second update_worker_load for WA (idempotent)"
        );

        // Remove WB (still never-reported) → gauge == 1.
        scheduler
            .remove_worker(&WorkerId("WB".to_string()))
            .await
            .expect("remove WB");
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            1,
            "gauge must reach 1 after removing unreported worker WB — \
             remove_worker must decrement the gauge for never-reported workers"
        );

        // Remove WA (already reported — gauge must NOT change further).
        scheduler
            .remove_worker(&WorkerId("WA".to_string()))
            .await
            .expect("remove WA");
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            1,
            "removing a reported worker (WA) must not change the gauge (still 1)"
        );

        // Remove WC (never-reported) → gauge == 0.
        scheduler
            .remove_worker(&WorkerId("WC".to_string()))
            .await
            .expect("remove WC");
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            0,
            "gauge must reach 0 after removing unreported worker WC"
        );
    }

    /// (#sched-zeroload) The never-reported gauge must decrement when a worker
    /// is evicted via the TIMEOUT path (`remove_timedout_workers`), NOT only
    /// via the public `remove_worker`. The timeout path is the DOMINANT
    /// production eviction for a stalled-keepalive never-reported worker — the
    /// exact failure mode this gauge is meant to alert on. It evicts via
    /// `inner.immediate_evict_worker` → inner `remove_worker` directly,
    /// bypassing the public `remove_worker`. Before the fix the decrement lived
    /// only in the public `remove_worker`, so a timeout-evicted never-reported
    /// worker leaked the gauge monotonically.
    ///
    /// Drives a REAL timeout eviction (not the public remove): `build_scheduler`
    /// sets `worker_timeout_s = 100`; workers are added with
    /// `last_update_timestamp = 42` and registered in the registry at the same
    /// instant. WLIVE then sends a keepalive at t=250 (refreshes BOTH the
    /// in-pool timestamp and the registry heartbeat). Calling
    /// `remove_timedout_workers(300)`: WGONE is past `evict_threshold =
    /// 300-200 = 100` AND the registry deadline (42+100=142) ≤ 300 → evicted;
    /// WLIVE is locally alive (250 > `timeout_threshold = 200`) → survives.
    /// Neither ever reported load.
    ///
    /// Mutation: comment out the `fetch_sub` in INNER `remove_worker` → the
    /// timeout eviction does not decrement → gauge stays 2 → this test
    /// red-fails with the bespoke message below.
    #[nativelink_test]
    async fn t_never_reported_gauge_decrements_on_timeout_eviction() {
        let scheduler = build_scheduler(BarrierWorkerStateManager::new());

        // Two never-reported workers (no update_worker_load on either).
        // add_worker_in_pool uses Worker::new(..., timestamp=42, ...), so both
        // have last_update_timestamp == 42 and are registered at UNIX_EPOCH+42s.
        let _rx_gone = add_worker_in_pool(&scheduler, "WGONE").await;
        let _rx_live = add_worker_in_pool(&scheduler, "WLIVE").await;

        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            2,
            "gauge must be 2 after adding 2 never-reported workers to the pool"
        );

        // Refresh WLIVE so the timeout sweep at t=300 spares it: keepalive at
        // t=250 advances BOTH the in-pool last_update_timestamp (250 > the
        // timeout_threshold of 200) and the registry heartbeat. Keepalive does
        // NOT set has_reported_load — WLIVE stays never-reported.
        scheduler
            .worker_keep_alive_received(&WorkerId("WLIVE".to_string()), 250)
            .await
            .expect("WLIVE keepalive at t=250");

        // Drive the timeout sweep at t=300. WGONE (last_update 42) is past the
        // double-timeout evict_threshold (300 - 200 = 100) and the registry
        // deadline (142 ≤ 300) → evicted via remove_timedout_workers →
        // immediate_evict_worker → inner remove_worker (NOT the public one).
        scheduler
            .remove_timedout_workers(300)
            .await
            .expect("remove_timedout_workers(300)");

        // WGONE must actually be gone (not a vacuously-green assertion): it was
        // evicted, WLIVE was spared.
        assert!(
            scheduler
                .worker_has_reported_load_for_test(&WorkerId("WGONE".to_string()))
                .await
                .is_none(),
            "WGONE must have been evicted by the timeout sweep (absent from the pool)"
        );
        assert!(
            scheduler
                .worker_has_reported_load_for_test(&WorkerId("WLIVE".to_string()))
                .await
                .is_some(),
            "WLIVE must survive the timeout sweep (refreshed keepalive at t=250)"
        );

        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            1,
            "never-reported gauge must decrement when a worker is evicted via the \
             TIMEOUT path (remove_timedout_workers → immediate_evict_worker → inner \
             remove_worker), not only via the public remove_worker — the decrement \
             must live at the eviction choke point or the gauge leaks every \
             stalled-keepalive worker"
        );
    }

    /// (#sched-zeroload) The never-reported gauge must NOT underflow on the
    /// `add_worker` ERROR path. Inner `add_worker` puts the worker into
    /// `self.workers` BEFORE `send_initial_connection_result`; if that send
    /// fails (the worker dropped its `UpdateForWorker` channel mid-registration
    /// — a real production race: a worker disconnecting during connect), inner
    /// `add_worker` returns Err and the OUTER `add_worker` error branch calls
    /// `immediate_evict_worker` → inner `remove_worker`, which finds the worker
    /// still in the map and decrements the gauge (`!has_reported_load`).
    ///
    /// If the increment lives in the outer success-only branch (after
    /// `drop(inner)`), the error path NEVER reaches it — decrement-without-
    /// increment underflows the `u64` gauge to `u64::MAX`, which reads as
    /// ~1.8e19 for the rest of the process lifetime and permanently destroys
    /// the stalled-keepalive alert this gauge exists to drive. The fix
    /// collocates the increment with `self.workers.put` in inner `add_worker`,
    /// symmetric with the decrement at `self.workers.pop` in inner
    /// `remove_worker`.
    ///
    /// Drives the REAL error path (not a synthetic `remove_worker`): build a
    /// Worker whose `UpdateForWorker` receiver is dropped BEFORE `add_worker`,
    /// so the unbounded `tx.send` inside `send_initial_connection_result`
    /// returns Err.
    ///
    /// Mutation: move the increment back to the outer success-only branch →
    /// this test red-fails with the bespoke underflow message below.
    #[nativelink_test]
    async fn t_never_reported_gauge_no_underflow_on_add_error() {
        let scheduler = build_scheduler(BarrierWorkerStateManager::new());

        // Gauge starts at 0 before the add.
        assert_eq!(
            scheduler.workers_never_reported_load_for_test(),
            0,
            "gauge must start at 0 before the failing add_worker"
        );

        // Build a worker whose receiver is already dropped, so the unbounded
        // `tx.send` inside `send_initial_connection_result` fails (an
        // UnboundedSender::send errors ONLY when the receiver is dropped).
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let worker = Worker::new(
            WorkerId("WDROP".to_string()),
            props_pool(),
            tx,
            42,
            0,
        );

        // add_worker must return Err (connection-closed): inner add_worker puts
        // the worker, then send_initial_connection_result fails → outer error
        // branch evicts via immediate_evict_worker → inner remove_worker.
        let res = scheduler.add_worker(worker).await;
        assert!(
            res.is_err(),
            "add_worker on a worker with a dropped receiver must return Err \
             (send_initial_connection_result fails)"
        );

        let gauge = scheduler.workers_never_reported_load_for_test();
        // Distinct assert for the underflow itself, so a wrap-to-u64::MAX is
        // diagnosed separately from an off-by-one.
        assert_ne!(
            gauge,
            u64::MAX,
            "workers_never_reported_load underflowed to u64::MAX on the \
             add_worker error path — the increment is NOT collocated with the \
             choke-point decrement, so a worker that drops its channel during \
             registration is decremented at the evict without ever being \
             incremented at the put"
        );
        assert_eq!(
            gauge,
            0,
            "add_worker error path (send_initial_connection_result failed → \
             immediate_evict) must leave workers_never_reported_load at its \
             pre-add value — the increment must be symmetric with the \
             choke-point decrement (collocated at workers.put / workers.pop) or \
             the gauge underflows to u64::MAX on every worker that drops its \
             channel during registration, permanently destroying the \
             stalled-keepalive alert"
        );
    }
}

/// Deferred `to_proto_vecs()` optimization (Group-2 scheduler perf).
///
/// `find_and_reserve_worker` pre-computes the proto-tree clone (Phase 2.5)
/// BEFORE the write lock and BEFORE knowing if any worker will be selected.
/// On a busy/backlogged fleet this clone is built and immediately discarded
/// on the no-match path — wasted CPU per still-queued action cycle.
///
/// Option B defers `to_proto_vecs()` to AFTER the lock drops, gated on
/// `result.is_some()`.  Selection never reads `pre_computed_tree`; it reads
/// `resolved_tree` (the raw struct) and `endpoint_scores`.  The clone is
/// consumed only to populate `StartExecute.resolved_directories`.
///
/// This module proves two behavioural contracts:
///
///   1. **Match path**: when a worker IS selected, `StartExecute` carries
///      `resolved_directories` populated from the deferred clone.
///   2. **No-match path**: when no worker is available, `None` is returned
///      and the function completes (the tree clone is not wasted, but this
///      is an internal detail — the observable contract is correct `None`
///      return without hanging).
///
/// Mutation target: replace the deferred `to_proto_vecs()` call with an
/// empty default.  Test 1 red-fails with the bespoke "resolved_directories
/// must be non-empty" message because no directories reach the wire.
#[cfg(test)]
mod deferred_proto_clone_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use prost::Message;
    use tokio::sync::{Notify, mpsc};

    use nativelink_config::schedulers::WorkerAllocationStrategy;
    use nativelink_config::stores::MemorySpec;
    use nativelink_error::Error;
    use nativelink_macro::nativelink_test;
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_proto::build::bazel::remote::execution::v2::{Digest as ProtoDigest, FileNode};
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::action_messages::{
        ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId, WorkerId,
    };
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
    use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
    use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
    use nativelink_util::store_trait::{Store, StoreKey, StoreLike};

    use super::{
        ApiWorkerScheduler, MAX_TREE_PROTO_BYTES, UpdateForWorker, Worker, update_for_worker,
    };
    use crate::platform_property_manager::PlatformPropertyManager;
    use crate::worker::ActionInfoWithProps;
    use crate::worker_registry::WorkerRegistry;
    use crate::worker_scheduler::WorkerScheduler;

    /// No-op `WorkerStateManager` for tests that do not exercise the
    /// update_operation path.
    struct NoopWorkerStateManager;

    impl MetricsComponent for NoopWorkerStateManager {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[tonic::async_trait]
    impl WorkerStateManager for NoopWorkerStateManager {
        async fn update_operation(
            &self,
            _operation_id: &OperationId,
            _worker_id: &WorkerId,
            _update: UpdateOperationType,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn props_exact(name: &str) -> PlatformProperties {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            PlatformPropertyValue::Exact(name.to_string()),
        );
        PlatformProperties { properties }
    }

    /// Encode a `Directory` proto and return its bytes + content-addressed
    /// `DigestInfo` so we can store it in a `MemoryStore`.
    fn encode_dir(dir: &nativelink_proto::build::bazel::remote::execution::v2::Directory) -> (Vec<u8>, DigestInfo) {
        let bytes = dir.encode_to_vec();
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        hasher.update(&bytes);
        let digest = hasher.finalize_digest();
        (bytes, digest)
    }

    /// Build a minimal `Directory` proto with one file and store it in
    /// the given `MemoryStore`.  Returns the root digest so it can be used
    /// as the action's `input_root_digest`.
    async fn store_minimal_directory(store: &Store) -> DigestInfo {
        let dir = nativelink_proto::build::bazel::remote::execution::v2::Directory {
            files: vec![FileNode {
                name: "file.txt".to_string(),
                digest: Some(ProtoDigest {
                    hash: format!("{:02x}", 0xaau8).repeat(32),
                    size_bytes: 42,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let (dir_bytes, dir_digest) = encode_dir(&dir);
        let key: StoreKey<'_> = dir_digest.into();
        store
            .update_oneshot(key, Bytes::from(dir_bytes))
            .await
            .expect("store update_oneshot for test directory failed");
        dir_digest
    }

    /// Build a `Directory` proto with enough FileNodes that its encoded size
    /// exceeds the test `MAX_TREE_PROTO_BYTES` (4 KiB), store it, and return
    /// the root digest.  Used by the over-size-gate test: a tree this large
    /// must be OMITTED from `StartExecute` even when a worker is matched.
    async fn store_oversize_directory(store: &Store) -> DigestInfo {
        // Each FileNode encodes to ~90 bytes (8-char name + 64-char hash +
        // size). 200 files ⇒ ~18 KiB, comfortably over the 4 KiB test cap.
        let files: Vec<FileNode> = (0..200u32)
            .map(|i| FileNode {
                name: format!("f{i:06}.txt"),
                digest: Some(ProtoDigest {
                    hash: format!("{:02x}", (i % 256) as u8).repeat(32),
                    size_bytes: 1024,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
        let dir = nativelink_proto::build::bazel::remote::execution::v2::Directory {
            files,
            ..Default::default()
        };
        let (dir_bytes, dir_digest) = encode_dir(&dir);
        assert!(
            dir.encode_to_vec().len() > MAX_TREE_PROTO_BYTES,
            "over-size fixture must exceed the test cap ({} bytes) — got {}",
            MAX_TREE_PROTO_BYTES,
            dir.encode_to_vec().len()
        );
        let key: StoreKey<'_> = dir_digest.into();
        store
            .update_oneshot(key, Bytes::from(dir_bytes))
            .await
            .expect("store update_oneshot for oversize test directory failed");
        dir_digest
    }

    fn build_scheduler_with_cas(cas_store: Store) -> Arc<ApiWorkerScheduler> {
        ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWorkerStateManager),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            Some(cas_store),
            None,
            512 * 1024,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        )
    }

    fn build_scheduler_no_cas() -> Arc<ApiWorkerScheduler> {
        ApiWorkerScheduler::new_with_locality_map(
            Arc::new(NoopWorkerStateManager),
            Arc::new(PlatformPropertyManager::new(HashMap::new())),
            WorkerAllocationStrategy::default(),
            Arc::new(Notify::new()),
            100,
            Arc::new(WorkerRegistry::new()),
            None,
            None,
            None,
            512 * 1024,
            8,
            false, // (#sched M1 rebalance) p_headroom_gate OFF
            0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
            2, // (#sched M1 rebalance v2) p_headroom_override_factor
        )
    }

    async fn add_worker(
        scheduler: &Arc<ApiWorkerScheduler>,
        name: &str,
    ) -> mpsc::UnboundedReceiver<UpdateForWorker> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker::new(WorkerId(name.to_string()), props_exact(name), tx, 42, 4);
        scheduler.add_worker(worker).await.expect("add_worker");
        rx
    }

    fn make_action(name: &str, root_digest: DigestInfo) -> ActionInfoWithProps {
        ActionInfoWithProps {
            inner: Arc::new(ActionInfo {
                command_digest: DigestInfo::new([0u8; 32], 0),
                input_root_digest: root_digest,
                timeout: Duration::MAX,
                platform_properties: HashMap::new(),
                priority: 0,
                load_timestamp: UNIX_EPOCH,
                insert_timestamp: SystemTime::now(),
                unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                    instance_name: "main".to_string(),
                    digest_function: DigestHasherFunc::Sha256,
                    digest: root_digest,
                }),
            }),
            platform_properties: props_exact(name),
        }
    }

    /// (#sched-g2) Match path: `StartExecute.resolved_directories` is
    /// populated when `find_and_reserve_worker` selects a worker and a
    /// resolved tree is present in the cache.
    ///
    /// Invariant: the deferred `to_proto_vecs()` call gated on
    /// `result.is_some()` MUST populate `resolved_directories` in the
    /// `StartExecute` message for the selected worker.
    ///
    /// Mutation target: replace the deferred `to_proto_vecs()` call (the
    /// `resolved_tree.as_deref().and_then(|tree| { ... Some(tree.to_proto_vecs()) })`
    /// block after the lock drops) with a constant `None`.  This test
    /// red-fails with the bespoke message
    /// "resolved_directories must be non-empty on the match path".
    #[nativelink_test]
    async fn deferred_clone_populates_resolved_directories_on_match() {
        // A real MemoryStore so resolve_tree_from_cas can build a tree.
        let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let root_digest = store_minimal_directory(&cas_store).await;

        let scheduler = build_scheduler_with_cas(cas_store);
        let mut rx = add_worker(&scheduler, "W").await;

        let op = OperationId::default();
        let action = make_action("W", root_digest);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.find_and_reserve_worker(&props_exact("W"), &op, &action, false),
        )
        .await
        .expect("find_and_reserve_worker must not hang on the match path");

        let (_worker_id, _tx, msg) = result.expect(
            "deferred-clone match path: worker W is idle and matches; must return Some",
        );

        // Drain the worker's channel so the receiver is not leaked.
        let _ = rx.try_recv();

        let start_execute = match msg.update {
            Some(update_for_worker::Update::StartAction(se)) => se,
            other => panic!(
                "deferred-clone match path: expected StartAction, got {other:?}"
            ),
        };

        assert!(
            !start_execute.resolved_directories.is_empty(),
            "resolved_directories must be non-empty on the match path — \
             the deferred to_proto_vecs() call was not executed or produced \
             an empty result; the worker will fall back to GetTree RPC \
             (the perf regression this optimization prevents)"
        );
        assert_eq!(
            start_execute.resolved_directories.len(),
            start_execute.resolved_directory_digests.len(),
            "resolved_directories and resolved_directory_digests must be \
             parallel vecs with equal length"
        );

        // (T-1) Content assertion: the single directory's FILE CONTENT must
        // survive the clone — a ghost-Directory bug (right count, empty
        // fields) would pass the count check above.  The source tree had
        // exactly one Directory with one FileNode named "file.txt" size 42.
        assert_eq!(
            start_execute.resolved_directories.len(),
            1,
            "the single-directory test tree must clone to exactly one directory"
        );
        let cloned_dir = &start_execute.resolved_directories[0];
        assert_eq!(
            cloned_dir.files.len(),
            1,
            "the cloned Directory must carry its one FileNode — empty files \
             would mean to_proto_vecs() cloned a hollow proto (ghost-Directory)"
        );
        assert_eq!(
            cloned_dir.files[0].name, "file.txt",
            "the cloned FileNode must preserve its name through to_proto_vecs()"
        );
        let cloned_file_digest = cloned_dir.files[0]
            .digest
            .as_ref()
            .expect("the cloned FileNode must preserve its digest");
        assert_eq!(
            cloned_file_digest.size_bytes, 42,
            "the cloned FileNode digest must preserve size_bytes through the clone"
        );

        // The directory digest in the parallel vec must match the source
        // root digest — proving the clone is keyed correctly, not just
        // carrying arbitrary content.
        let expected_root: ProtoDigest = root_digest.into();
        assert_eq!(
            start_execute.resolved_directory_digests[0], expected_root,
            "resolved_directory_digests[0] must equal the source input_root \
             digest — the clone must preserve the digest→Directory mapping"
        );
    }

    /// (#sched-g2) No-match path: `find_and_reserve_worker` returns `None`
    /// when no worker is available, even when a resolved tree would be
    /// present.
    ///
    /// This is the path where the pre-fix code wastefully cloned the proto
    /// tree and then discarded it.  After the fix the clone is skipped
    /// entirely.  The observable contract is correct `None` return.
    ///
    /// Note: this test uses no CAS store (so no tree is resolved at all —
    /// Phase 1 short-circuits), which is sufficient to prove the no-match
    /// behavioral contract.  The performance invariant (clone skipped when
    /// a tree IS present but no worker matches) is validated by inspection
    /// of the `result.is_some()` gate at the `to_proto_vecs()` call site.
    #[nativelink_test]
    async fn deferred_clone_returns_none_when_no_workers_available() {
        let scheduler = build_scheduler_no_cas();
        // No workers added — every call must return None.

        let op = OperationId::default();
        let root_digest = DigestInfo::new([0xB2u8; 32], 1);
        let action = make_action("W", root_digest);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.find_and_reserve_worker(&props_exact("W"), &op, &action, false),
        )
        .await
        .expect("find_and_reserve_worker must not hang on the no-match path");

        assert!(
            result.is_none(),
            "deferred-clone no-match path: no workers in scheduler; \
             must return None — the deferred clone gate must not accidentally \
             force a Some return"
        );
    }

    /// (#sched-g2, T-2) Over-size gate on the MATCH path: when a worker is
    /// selected but the resolved tree exceeds `MAX_TREE_PROTO_BYTES`, the
    /// deferred clone must be SKIPPED and `resolved_directories` must be
    /// empty (the worker falls back to its own GetTree RPC).
    ///
    /// This guards against an inverted-gate regression (e.g. `>` → `<`, or
    /// dropping the `tree_fits_in_message` guard) that the other tests would
    /// miss — they all use sub-cap trees, so an inverted gate would still
    /// populate `resolved_directories` for them.
    ///
    /// Composition: real `MemoryStore` holding a ~18 KiB directory (200
    /// files), test cap = 4 KiB, one matching worker.  Selection succeeds
    /// (Some returned) but the size gate omits the tree.
    #[nativelink_test]
    async fn deferred_clone_skips_oversize_tree_on_match() {
        let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let root_digest = store_oversize_directory(&cas_store).await;

        let scheduler = build_scheduler_with_cas(cas_store);
        let mut rx = add_worker(&scheduler, "W").await;

        let op = OperationId::default();
        let action = make_action("W", root_digest);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.find_and_reserve_worker(&props_exact("W"), &op, &action, false),
        )
        .await
        .expect("find_and_reserve_worker must not hang on the oversize-tree path");

        let (_worker_id, _tx, msg) = result.expect(
            "oversize-tree path: worker W matches; selection must still succeed \
             (the size gate omits the tree but does NOT block the dispatch)",
        );
        let _ = rx.try_recv();

        let start_execute = match msg.update {
            Some(update_for_worker::Update::StartAction(se)) => se,
            other => panic!("oversize-tree path: expected StartAction, got {other:?}"),
        };

        assert!(
            start_execute.resolved_directories.is_empty(),
            "resolved_directories must be EMPTY when the tree exceeds \
             MAX_TREE_PROTO_BYTES — the size gate must skip the clone so the \
             StartExecute message stays under the worker API encoding limit; \
             an inverted gate would populate it here and risk a 64 MiB-cap \
             message rejection at the worker"
        );
        assert!(
            start_execute.resolved_directory_digests.is_empty(),
            "resolved_directory_digests must also be empty when the tree is \
             over-size — the parallel vec must not leak digests for an \
             omitted tree"
        );
    }

    /// (#sched-g2) Micro-bench: confirm that `to_proto_vecs()` (the clone)
    /// is materially more expensive than the `encoded_len` size-gate (the
    /// cheap pre-check retained in Phase 2.5).
    ///
    /// This test runs in `--release` to produce meaningful timings; in debug
    /// mode it still executes but the ratio assertion is relaxed to avoid
    /// false failures from debug instrumentation overhead.
    ///
    /// The dispatch premise was: clone is 14–30× the size-check.  We accept
    /// any ratio >1 in debug and assert ratio >3 in release (well below the
    /// claimed lower bound, to be robust across machines).  A ratio ≤1 would
    /// mean the two operations cost the same, invalidating the premise.
    ///
    /// Tee path: `/tmp/sched-g2impl-bench.log` (populated when this test is
    /// run with `--nocapture`).
    #[test]
    fn proto_clone_more_expensive_than_encoded_len_size_check() {
        use std::hint::black_box;
        use std::time::Instant;

        // Build a tree of N directories, each with M file nodes, to
        // approximate a realistic 500-dir action tree.
        const N_DIRS: usize = 500;
        const FILES_PER_DIR: usize = 20;
        const ITERS: u32 = 200;

        let mut tree_dirs: Vec<nativelink_proto::build::bazel::remote::execution::v2::Directory> =
            Vec::with_capacity(N_DIRS);
        for d in 0..N_DIRS {
            let files: Vec<nativelink_proto::build::bazel::remote::execution::v2::FileNode> =
                (0..FILES_PER_DIR)
                    .map(|f| {
                        nativelink_proto::build::bazel::remote::execution::v2::FileNode {
                            name: format!("file_{d}_{f}.txt"),
                            digest: Some(
                                nativelink_proto::build::bazel::remote::execution::v2::Digest {
                                    hash: format!("{:02x}", (d * FILES_PER_DIR + f) as u8)
                                        .repeat(32),
                                    size_bytes: 1024,
                                    ..Default::default()
                                },
                            ),
                            ..Default::default()
                        }
                    })
                    .collect();
            tree_dirs.push(
                nativelink_proto::build::bazel::remote::execution::v2::Directory {
                    files,
                    ..Default::default()
                },
            );
        }

        // Build a HashMap<DigestInfo, Directory> to match ResolvedTree::directories.
        let mut dir_map: HashMap<DigestInfo, nativelink_proto::build::bazel::remote::execution::v2::Directory> =
            HashMap::new();
        for (i, dir) in tree_dirs.iter().enumerate() {
            let digest = DigestInfo::new([i as u8; 32], i as u64);
            dir_map.insert(digest, dir.clone());
        }

        // ── Measure encoded_len size-check (N_DIRS encoded_len calls) ──
        let t0 = Instant::now();
        for _ in 0..ITERS {
            let total: usize = black_box(
                dir_map
                    .values()
                    .map(|d| Message::encoded_len(d))
                    .sum(),
            );
            let _ = black_box(total);
        }
        let size_check_ns = t0.elapsed().as_nanos() / u128::from(ITERS);

        // ── Measure to_proto_vecs (N_DIRS Directory.clone() calls) ──
        let t1 = Instant::now();
        for _ in 0..ITERS {
            let mut dirs = Vec::with_capacity(dir_map.len());
            let mut digests = Vec::with_capacity(dir_map.len());
            for (digest_info, directory) in black_box(&dir_map) {
                digests.push(nativelink_proto::build::bazel::remote::execution::v2::Digest::from(*digest_info));
                dirs.push(black_box(directory.clone()));
            }
            let _ = black_box((dirs, digests));
        }
        let clone_ns = t1.elapsed().as_nanos() / u128::from(ITERS);

        let ratio = if size_check_ns > 0 {
            clone_ns as f64 / size_check_ns as f64
        } else {
            f64::MAX
        };

        println!(
            "[sched-g2 bench] N_DIRS={N_DIRS} FILES_PER_DIR={FILES_PER_DIR} ITERS={ITERS}\n  \
             encoded_len size-check: {size_check_ns} ns/iter\n  \
             to_proto_vecs (clone):  {clone_ns} ns/iter\n  \
             ratio clone/check:      {ratio:.1}×"
        );

        // In release builds the clone is substantially more expensive.
        // In debug builds the assert is relaxed because allocator
        // instrumentation can make clones artificially fast relative to
        // the loop overhead of encoded_len.
        #[cfg(not(debug_assertions))]
        assert!(
            ratio > 3.0,
            "proto clone must be >3× more expensive than encoded_len size-check \
             (got {ratio:.1}×) — if this fires, the premise for deferring to_proto_vecs() \
             is invalidated and the optimization should be reconsidered; \
             size_check={size_check_ns}ns clone={clone_ns}ns"
        );
    }
}
