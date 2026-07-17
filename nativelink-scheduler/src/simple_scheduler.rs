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

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::{Future, StreamExt, future};
use nativelink_config::schedulers::SimpleSpec;
use nativelink_config::stores::ClientTlsConfig;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_proto::com::github::trace_machina::nativelink::events::{
    Event, OriginEvent, RequestEvent, event, request_event,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_util::action_messages::{ActionInfo, ActionState, OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, OperationStageFlags, OrderDirection, UpdateOperationType,
};
use nativelink_util::origin_event::{OriginMetadata, get_node_id};
use nativelink_util::platform_properties::PlatformProperties;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use opentelemetry::KeyValue;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::context::{Context, FutureExt as OtelFutureExt};
use opentelemetry_semantic_conventions::attribute::ENDUSER_ID;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio::time::Duration;
use tracing::{debug, error, info, info_span, warn};
use uuid::Uuid;

use crate::api_worker_scheduler::{
    ApiWorkerScheduler, HOLD_COUNTERS_LOG_INTERVAL_S, compute_dedup_cached_score,
    emit_inject_observe_counters_log, emit_prediction_accuracy_counters_log,
    emit_resource_profile_counters_log,
    emit_speculative_hold_counters_log,
};
use crate::awaited_action_db::{AwaitedActionDb, CLIENT_KEEPALIVE_DURATION};
use crate::dag_criticality::{self, DagState};
use crate::known_platform_property_provider::KnownPlatformPropertyProvider;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::resource_profile_persist;
use crate::simple_scheduler_state_manager::SimpleSchedulerStateManager;
use crate::worker::{ActionInfoWithProps, Worker, WorkerTimestamp};
use crate::worker_registry::WorkerRegistry;
use crate::worker_scheduler::WorkerScheduler;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
/// A 5-second timeout causes unnecessary worker churn on any brief network
/// hiccup or GC pause, so we use a more generous default.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 30;

/// Mark operations as completed with error if no client has updated them
/// within this duration.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_CLIENT_ACTION_TIMEOUT_S: u64 = 60;

/// Default times a job can retry before failing.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_MAX_JOB_RETRIES: usize = 3;

// ─────────────────────────── Batch-affinity probe ───────────────────────────
//
// (#batch-affinity) OBSERVABILITY-ONLY probe measuring the POTENTIAL
// profitability of batch (multi-task) dir-cache-affinity scheduling. It does
// NOT change assignment, does NOT add any delay, and does NOT influence which
// worker is chosen — it only MEASURES whether looking at all pending tasks
// together (dimension A) or briefly delaying to accumulate related tasks
// (dimension B) WOULD improve dir-cache affinity. The emitted gauges/counter
// justify (or refute) building real batch scheduling later. See the two pure
// functions and `RecentRootsWindow` below; all three are `pub` so the exact
// arithmetic and window boundary are unit-testable without a live scheduler.

/// (#batch-affinity, dimension A) Instantaneous co-location surplus over a
/// snapshot of the pending set's `input_root_digest`s.
///
/// Returns `(surplus, max_group)` where:
/// - `surplus = n - distinct_roots` — a RAW UPPER BOUND on the pending
///   assignments that could reuse a *peer pending task's* dir-cache locality if
///   a batch assignment grouped same-input-root tasks together. It is an upper
///   bound, NOT the realized benefit, because the worker-side cache signal
///   (`cached_directory_digests` / `cached_subtree_digests`) is populated only
///   by worker-reported `BlobsAvailable` AFTER an action materializes its
///   inputs — never at dispatch. So the surplus is genuinely uncaptured only
///   for the COLD subset (a build-startup burst of same-root peers, where no
///   worker yet reports the root → they scatter via LRU/MRU); WARM steady-state
///   roots are ALREADY Tier-1 co-located by the existing affinity routing, and
///   this raw gauge cannot separate cold from warm. Greedy one-at-a-time
///   assignment cannot co-locate a still-queued same-root peer WITHIN a single
///   match cycle (cross-cycle, the worker's cache report may have already
///   landed and the existing Tier-1 routing absorbs it). `surplus == 0` means
///   every pending op has a unique root, so batch grouping buys nothing.
/// - `max_group` — the size of the largest same-input-root group among the
///   pending ops (the biggest single batch a grouper could form). `0` for an
///   empty set, `1` when all roots are distinct.
///
/// Interpret ALONGSIDE the existing `find_worker_hits` / `find_worker_misses`
/// baseline: a high surplus that is already fully absorbed by locality routing
/// (high hit rate) is NOT a green light for batch scheduling — the cold-burst
/// subset is the real signal.
///
/// Pure function of the input slice so the surplus/max-group arithmetic is
/// unit-testable at its boundaries (`[A,A,B,C] → (1,2)`, all-distinct `→ (0,1)`,
/// all-same `→ (n-1, n)`, empty `→ (0,0)`).
pub fn colocation_surplus(pending_input_roots: &[DigestInfo]) -> (u64, u64) {
    if pending_input_roots.is_empty() {
        return (0, 0);
    }
    // Count occurrences per distinct root. Bounded by the caller's sample cap
    // (see `MAX_PENDING_AFFINITY_SAMPLE`), so this is a bounded scratch map.
    let mut counts: HashMap<DigestInfo, u64> = HashMap::new();
    for root in pending_input_roots {
        *counts.entry(*root).or_insert(0) += 1;
    }
    let n = pending_input_roots.len() as u64;
    let distinct = counts.len() as u64;
    let max_group = counts.values().copied().max().unwrap_or(0);
    (n - distinct, max_group)
}

/// (#batch-affinity, dimension B) The accumulation window a hypothetical batch
/// scheduler would use: if a related task arrives within this window of a peer,
/// a delay of this length would have let them batch. 250ms is short enough that
/// the added latency would be negligible against typical action execution
/// times, yet long enough to catch the tight bursts Bazel emits at build
/// startup. Pinned in `batch_affinity_metrics_test::affinity_window_const_is_250ms`.
pub const AFFINITY_ARRIVAL_WINDOW: Duration = Duration::from_millis(250);

/// (#batch-affinity, dimension B) Decision: would a batch scheduler that
/// accumulated for `window` have grouped an arrival at `now` with a peer last
/// seen at `last_seen`? Closed interval — an arrival exactly `window` after the
/// peer still counts (the delay would have just captured it). Saturating on the
/// (impossible-in-practice) `last_seen > now` case so a clock hiccup can never
/// panic. Pure `(now, last_seen, window) → bool` so the boundary (`== window`
/// in, `window + 1ns` out) is unit-testable.
///
/// Timestamps are `SystemTime`, the type the scheduler's injectable clock
/// (`now_fn().now()`) produces in BOTH prod (`SystemTime::now`) and tests
/// (`MockInstantWrapped` → `UNIX_EPOCH + MockClock::time()`), so the dim-B
/// counter is driven by the same mockable clock the rest of the scheduler uses
/// — no wall-clock dependence in tests.
pub fn is_within_affinity_window(now: SystemTime, last_seen: SystemTime, window: Duration) -> bool {
    // `duration_since` errs when `last_seen > now`; treat that (a clock hiccup)
    // as 0 elapsed → within window, mirroring `Instant::saturating_duration_since`.
    now.duration_since(last_seen).unwrap_or(Duration::ZERO) <= window
}

/// (#batch-affinity, dimension B) Maximum number of distinct recently-seen
/// input roots retained by `RecentRootsWindow`.
///
// CAPPED AT 4096: `RecentRootsWindow` lives on the scheduler and is fed one
// entry per arriving action (`inner_add_action`, a network-reachable path).
// Without a cap a flood of distinct-input-root actions would grow it
// unboundedly. 4096 distinct roots × (32-byte digest + Instant) ≈ 200 KiB —
// negligible, and far more than the number of *distinct* input roots that can
// plausibly arrive inside a 250ms window (each entry older than the window is
// dead weight anyway). Over-cap behavior: the OLDEST entry (by insertion via
// the FIFO `order` deque) is evicted, matching the observation that the window
// only cares about recent arrivals. This is a probe-only structure — dropping
// an entry can only *undercount* window matches (a conservative bias for an
// observability metric), never corrupt scheduling state.
pub const RECENT_ROOTS_MAX_ENTRIES: usize = 4096;

/// (#batch-affinity, dimension B) Bounded map of
/// `input_root_digest → last-arrival Instant`, used to estimate how many
/// co-location opportunities a small accumulation delay would capture.
///
/// `record_arrival` returns `true` iff the arriving root was already present
/// with a last-seen time inside `AFFINITY_ARRIVAL_WINDOW` — i.e. a
/// `AFFINITY_ARRIVAL_WINDOW`-length delay would have let this task batch with a
/// peer. It always updates the entry to the new arrival time.
///
/// Observability-only: this never influences assignment. It is intentionally
/// NOT `MetricsComponent` (it holds per-digest keyed state, which the derive
/// cannot emit as labels — the SCALAR counter derived from it lives in
/// `BatchAffinityMetrics`).
///
/// Correctness note: a stale entry (last seen longer ago than the window) left
/// in the map can NEVER produce a false within-window match, because
/// `record_arrival` re-checks freshness via `is_within_affinity_window` against
/// the stored time. Staleness therefore costs only memory, which the entry cap
/// bounds — so no TTL sweep is required for correctness, only the cap.
#[derive(Debug, Default)]
pub struct RecentRootsWindow {
    // CAPPED AT RECENT_ROOTS_MAX_ENTRIES: see the const's justification. Bounded
    // by evicting the FIFO-oldest key when the map would exceed the cap.
    // Timestamps are `SystemTime` (the injectable-clock type — see
    // `is_within_affinity_window`), so window matching is mock-clock-driven.
    last_seen: HashMap<DigestInfo, SystemTime>,
    // FIFO insertion order for O(1) oldest-key eviction. One entry per distinct
    // key (updates do not re-push), bounded to the same cap as `last_seen`.
    order: std::collections::VecDeque<DigestInfo>,
}

impl RecentRootsWindow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current number of retained roots. Test/observability accessor.
    pub fn len(&self) -> usize {
        self.last_seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last_seen.is_empty()
    }

    /// Record an arrival of `root` at `now` (a `SystemTime` from the injectable
    /// clock). Returns `true` iff a peer with the same root was last seen within
    /// `AFFINITY_ARRIVAL_WINDOW` (a captured batch opportunity). Always updates
    /// the entry to `now` and enforces the entry cap.
    pub fn record_arrival(&mut self, root: DigestInfo, now: SystemTime) -> bool {
        let within_window = match self.last_seen.get(&root) {
            Some(&prev) => is_within_affinity_window(now, prev, AFFINITY_ARRIVAL_WINDOW),
            None => false,
        };
        // Insert-or-update. Push to the FIFO order deque only on FIRST insert so
        // each distinct key appears exactly once; updates keep the original
        // insertion slot (the deque tracks first-seen order for cap eviction,
        // not last-seen — a refreshed entry is still cap-evictable, just biased
        // toward oldest-first, which is the desired bound behavior).
        if self.last_seen.insert(root, now).is_none() {
            self.order.push_back(root);
        }
        // Cap enforcement only: evict FIFO-oldest keys until at/under cap.
        // Bounded loop — runs at most (len - cap) iterations, which is 1 in
        // steady state (we add one entry per call). Stale-but-uncapped entries
        // are harmless (see the struct doc-comment) so no TTL sweep is needed.
        while self.last_seen.len() > RECENT_ROOTS_MAX_ENTRIES {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.last_seen.remove(&oldest);
                }
                None => break,
            }
        }
        within_window
    }
}

/// (#batch-affinity) Maximum number of highest-priority pending ops the
/// instantaneous co-location surplus (dimension A) is computed over per match
/// cycle. `pub` so the sample cap can be pinned in a test and asserted at the
/// `sampled_ops` saturation boundary.
///
// CAPPED AT 512: `do_try_match` already owns the priority-sorted pending set;
// the surplus pass calls `as_action_info()` once per sampled op. On the
// DEPLOYED memory backend that is a cheap in-memory `watch::borrow().clone()` —
// the SAME kind of call the matcher makes anyway. On the (supported but not
// deployed) Redis/store backend it would be a store round-trip per op, so the
// whole dim-A pass is gated OFF for that backend (see
// `pending_affinity_probe_enabled`); this cap only bounds the memory-backend
// cost. Bounding to the first 512 (the highest-priority ops — the ones a batch
// scheduler would assign imminently) keeps the per-cycle probe cost O(512) even
// when a build-startup burst queues tens of thousands of actions, while still
// covering far more than the number of workers. When the pending set exceeds
// this, the surplus/max_group gauges describe the sampled prefix (reported via
// the `sampled_ops` gauge so operators can see saturation); this only
// UNDERCOUNTS the true surplus — a conservative bias for an observability metric.
pub const MAX_PENDING_AFFINITY_SAMPLE: usize = 512;

/// (#batch-sched) Bytes-equivalent weight of a single cached input file when
/// blending the `(cached_bytes, cached_files)` returned by
/// `compute_dedup_cached_score` into a scalar match score `s`:
/// `s = cached_bytes + cached_files * PER_FILE_WEIGHT`. MIRRORS the production
/// Tier-1.5 dispatch constant (`api_worker_scheduler.rs`, the local
/// `PER_FILE_WEIGHT` inside `inner_find_and_reserve_worker`) so the
/// counterfactual probe's `s(i,j)` is the SAME score the real scheduler ranks
/// on — a divergent weight would make the (B−G) delta compare against a
/// fiction. The equality is pinned by
/// `batch_sched_gain_test::per_file_weight_matches_dispatch`. `pub` so that
/// test can read it.
pub const PER_FILE_WEIGHT: u64 = 100 * 1024; // 100KB per file — see api_worker_scheduler.rs

/// (#batch-sched) One sampled pending action whose `ResolvedTree` was ALREADY
/// cached (a `tree_cache` peek hit — the probe NEVER resolves a tree). Carries
/// the owned subtree structure needed to score it against a worker's cache:
/// the directory-digest membership set and the disjoint per-directory direct
/// byte/file weights (copied out of the cached `ResolvedTree`). The `Vec` these
/// live in is in PRIORITY order (the pending set is already priority-sorted),
/// which the greedy counterfactual walks.
#[derive(Debug, Clone)]
pub struct BatchSchedAction {
    /// All directory digests in this action's input tree (root + subtrees).
    pub dir_digests: HashSet<DigestInfo>,
    /// Direct (non-recursive) file bytes attributed to each directory digest.
    /// Disjoint across directories (no double-counting via nesting) — the same
    /// partition `compute_dedup_cached_score` sums over.
    pub dir_direct_bytes: HashMap<DigestInfo, u64>,
    /// Direct (non-recursive) file COUNT per directory digest; blended into `s`
    /// via `PER_FILE_WEIGHT`.
    pub dir_direct_files: HashMap<DigestInfo, u64>,
}

/// (#batch-sched / M1-replay) One worker in the counterfactual, snapshotted
/// under the scheduler lock; the solve then runs lock-free over these owned
/// copies. FAITHFULLY REPLAYS the live M1 P-headroom gate: the CONTENTION driver
/// is the FRESH in-flight count (`running`) against `p_core_count` cache-tier
/// eligibility (mirroring `worker_has_p_headroom` in `api_worker_scheduler.rs`),
/// NOT an `max_inflight_tasks` slot budget. The gate is CONFIRMED ON in prod
/// (25,303 `p_headroom_gate_exclusion` events observed live 2026-07-02) at
/// `p_idle_threshold_pct == 0` (v1 behavior — workers at `running==p_core` with
/// `p_load` as low as 0 are still excluded, so the override clause never fires).
/// Every sampled action is PLACED (contention comes from the gate, not a slot
/// count); when the gate lifts (no viable worker has headroom) cache-tier opens
/// to all.
#[derive(Debug, Clone)]
pub struct BatchSchedWorker {
    /// The worker's cached directory subtree digests (its warm set at snapshot
    /// time). Scored against each action's `dir_digests`.
    pub cached_subtree_digests: HashSet<DigestInfo>,
    /// (M1-replay) SEED: the worker's FRESH in-flight action count at snapshot
    /// (`running_action_infos.len()`). This is the contention driver — mirror of
    /// the production gate's fresh count. RISES by 1 for each action the solver
    /// assigns to this worker (so it can lose p_headroom mid-solve, exactly as
    /// the real gate does under the write lock). Seeded from real state, NOT
    /// zeroed.
    // CAPPED AT (unbounded in principle, but) the sampled window size: `running`
    // only ever rises by one per placed action within a single solve; a per-cycle
    // scratch counter, not a network buffer.
    pub running: u64,
    /// (M1-replay) The worker's real reported P-core logical count. `has_p_headroom`
    /// is `p_core_count == 0 || running < p_core_count || running < p+e ||
    /// (override)`; a `p_core_count == 0` worker (legacy/Linux/Intel-Mac) is
    /// ALWAYS ungated (A5).
    pub p_core_count: u32,
    /// (#sched-work-conservation) The worker's real reported E-core logical
    /// count. Feeds the total-core term of `has_p_headroom` (`running <
    /// p_core_count + e_core_count`) so the observability-only counterfactual's
    /// eligibility decisions stay byte-identical to the dispatch gate after the
    /// E-core spill fix. GAUGE FIDELITY only — the batch solve routes zero
    /// traffic.
    pub e_core_count: u32,
    /// (M1-replay) The worker's real reported P-core load percent. Feeds the
    /// bounded override clause of `has_p_headroom` (`p_core_load_pct <
    /// idle_threshold_pct && running < p_core_count*override_factor`). Inert at the
    /// prod `idle_threshold_pct == 0`, carried so the mirror is EXACT if the
    /// threshold is ever raised.
    pub p_core_load_pct: u32,
    /// The worker's continuous load penalty (`CapacityScore.load_penalty`),
    /// computed ONCE from the real snapshot (NOT recomputed per assignment — the
    /// gate is the contention, not a load ramp). Used ONLY by the greedy
    /// counterfactual's `argmax_j (s − load_penalty_j)` so greedy models the
    /// production Tier-1.5 load blend. Batch maximizes raw `Σ s`.
    pub load_penalty: i64,
}

/// (M1-replay) The live M1 P-headroom gate configuration, snapshotted from the
/// scheduler (`p_headroom_gate_enabled` / `p_idle_threshold_pct` /
/// `p_headroom_override_factor`) and threaded into the pure solve so the
/// counterfactual's eligibility decisions are byte-identical to the dispatch
/// gate. Prod values: `enabled = true`, `idle_threshold_pct = 0` (v1),
/// `override_factor = 2` (inert at threshold 0).
#[derive(Debug, Clone, Copy)]
pub struct BatchSchedGateCfg {
    /// Mirror of `self.p_headroom_gate_enabled`. When `false` the gate is never
    /// active and every worker is cache-tier-eligible (pre-gate parity).
    pub enabled: bool,
    /// Mirror of `self.p_idle_threshold_pct`. `0` (prod) makes the override
    /// clause `p_load < 0` = never → EXACT v1 predicate.
    pub idle_threshold_pct: u32,
    /// Mirror of `self.p_headroom_override_factor`. Bounds the override to
    /// `p_core_count * override_factor` in-flight actions when the threshold is
    /// nonzero.
    pub override_factor: u32,
}

impl BatchSchedGateCfg {
    /// (#sched-work-conservation) Mirror of `worker_has_p_headroom`
    /// (`api_worker_scheduler.rs`). A worker has P-headroom when: A5
    /// `p_core_count == 0` (ungated legacy), OR a genuine free P slot by the
    /// FRESH count (`running < p_core_count`), OR the total-core term (E-core
    /// spill: `running < p_core_count + e_core_count`), OR the bounded idle-P
    /// override (`p_load < idle_threshold_pct && running < p_core_count *
    /// override_factor`). All `u64` to match `running` against the `u32` core
    /// counts without truncation. GAUGE FIDELITY only — kept byte-identical to
    /// the dispatch gate so the counterfactual gauge does not diverge by the
    /// E-spill fix; the batch solve routes zero traffic.
    fn has_p_headroom(&self, w: &BatchSchedWorker) -> bool {
        w.p_core_count == 0
            || w.running < u64::from(w.p_core_count)
            || w.running < u64::from(w.p_core_count) + u64::from(w.e_core_count)
            || (w.p_core_load_pct < self.idle_threshold_pct
                && w.running < u64::from(w.p_core_count) * u64::from(self.override_factor))
    }
}

/// (#batch-sched) The counterfactual result the probe stores into gauges.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BatchSchedGain {
    /// `(B − G) / G * 100`, floored, `0` when `G == 0` (guard: no greedy match
    /// to improve on, or empty inputs). `B := max(B_heuristic, G)` — a real
    /// batch scheduler never does worse than greedy, so `gain_pct` is a genuine
    /// LOWER BOUND on the batch gain (the from-scratch heuristic may miss gains
    /// the optimal batch would capture; see `greedy_fallback`).
    pub gain_pct: u64,
    /// Shared-subtree mass: `Σ dir_direct_bytes` over directory digests
    /// appearing in ≥2 sampled actions' `dir_digests`, / total sampled subtree
    /// bytes, `× 100` floored. `0` when there are no sampled subtree bytes.
    pub subtree_overlap_pct: u64,
    /// Number of cached (scored) actions the solve ran over. Coverage guardrail
    /// — `gain_pct` is only meaningful at `≥ 2`.
    pub sample_actions: u64,
    /// Number of capacity-bearing workers the solve ran over. Coverage
    /// guardrail — `gain_pct` is only meaningful at `≥ 2`.
    pub sample_workers: u64,
    /// (#batch-sched) TRUE for this cycle iff the from-scratch batch heuristic
    /// `B_heuristic` scored STRICTLY BELOW the greedy baseline `G` — i.e. the
    /// heuristic (which is NOT the optimum) picked a worse assignment than
    /// greedy this cycle, so `B` was floored up to `G` (`max`) and `gain_pct`
    /// is 0. The probe increments `batch_sched_greedy_fallback_total` on each
    /// such cycle so operators can see how often the heuristic undersells its
    /// own gain. NOT an error — a real batch scheduler would simply keep the
    /// greedy result; this flag just makes the floor visible.
    pub greedy_fallback: bool,
    /// (M1-replay) The greedy baseline `G` — the aggregate chosen `cache_score`
    /// under priority-order assignment. Exposed raw (not just as the `gain_pct`
    /// denominator) so a scrape can see the absolute cache-match mass greedy
    /// captures, and so the "greedy is near-optimal under lift" regime is
    /// legible (a large `greedy_score` at `gain_pct == 0`).
    pub greedy_score: u64,
    /// (M1-replay diagnostic gauge) Fraction of the GREEDY assignment steps on
    /// which the gate was ACTIVE (some viable worker still had p_headroom, so
    /// cache-tier eligibility was restricted), `× 100`. `100` = every step
    /// contended (the gate band); `0` = every step ran with the gate lifted
    /// (all-full, cache-tier open to all). Reveals WHICH regime the fleet is in
    /// when interpreting `gain_pct`. `0` when there were no greedy steps.
    pub gate_active_frac: u64,
    /// (M1-replay diagnostic gauge) Mean SEEDED `running` count across the
    /// sampled workers, `× 100` (so the sub-integer mean is legible). Reveals how
    /// deep in the gate band the fleet sits (near `p_core_count` = contended;
    /// well below = idle). `0` when there are no sampled workers.
    pub mean_seed_running: u64,
}

/// (#batch-sched) Scalar subtree-match score `s(i,j)` for `action` against
/// `worker_cache`: the SAME `compute_dedup_cached_score` atom Tier-1.5 dispatch
/// uses, blended to a scalar via `PER_FILE_WEIGHT`. Kept private to the solver;
/// the counterfactual is only meaningful because this is byte-identical to the
/// dispatch score.
fn batch_sched_score(action: &BatchSchedAction, worker_cache: &HashSet<DigestInfo>) -> u64 {
    let (cached_bytes, cached_files) = compute_dedup_cached_score(
        &action.dir_digests,
        worker_cache,
        &action.dir_direct_bytes,
        &action.dir_direct_files,
    );
    cached_bytes + cached_files * PER_FILE_WEIGHT
}

/// (#batch-sched / M1-replay) OBSERVABILITY-ONLY counterfactual: over the sampled
/// pending window `actions` (PRIORITY order) and `workers` (seeded from real
/// state), compute the aggregate subtree-match score under the current GREEDY
/// priority-order assignment `G` vs a global BATCH assignment `B` (reorder +
/// intra-batch warming), each subject to the LIVE M1 P-headroom gate, and report
/// `(B − G)/G` as `gain_pct` plus coverage/overlap/regime guardrails. PURE: no
/// I/O, no lock, no scheduler state — the caller extracts `actions`/`workers`
/// from already-cached data. It does NOT change dispatch (the real scheduler
/// stays greedy).
///
/// M1 GATE REPLAY (the contention model — `gate_cfg`): a worker is CACHE-TIER-
/// ELIGIBLE only when the gate permits it, mirroring the production dispatch gate
/// in `inner_find_and_reserve_worker`. Per assignment step:
/// - `gate_active = gate_cfg.enabled && (∃ worker j with has_p_headroom(j))`,
///   recomputed as `running_j` rises (the Phase-1/Phase-2 fold: when NO worker
///   has p_headroom the gate LIFTS and cache-tier opens to ALL).
/// - eligible(j) = `!gate_active || has_p_headroom(j)`.
///
/// `has_p_headroom` = `BatchSchedGateCfg::has_p_headroom` = the byte-exact mirror
/// of `worker_has_p_headroom` (fresh-count `running < p_core_count`, A5
/// `p_core==0` ungated, bounded idle-P override — inert at the prod
/// `idle_threshold_pct == 0`). The gate is CONFIRMED ON in prod (25,303 exclusion
/// events observed live 2026-07-02) at threshold 0 (v1). The CONTENTION is the
/// gate; there is NO `max_inflight_tasks` slot budget — EVERY action is placed
/// (once the gate lifts there is always ≥1 eligible worker).
///
/// GREEDY `G` (models the CACHE-AFFINITY greedy — Tier-1.5 load-blend, NOT the
/// whole selector): walk `actions` in priority order; each takes
/// `argmax_j (s(i,j) − load_penalty_j)` over CACHE-ELIGIBLE workers, then
/// `running_j += 1` (so the worker can lose p_headroom for the NEXT action,
/// exactly as the real gate does under the write lock). It adds the chosen
/// `s(i,j)` (NOT the penalized value — the metric measures cache-match mass, the
/// penalty only steers the choice) ONLY when the chosen pick clears the Tier-1.5
/// `blended_s > 0` CROSSOVER (`s(i,j) − load_penalty_j > 0`); when the cache
/// saving does NOT beat the load cost production DECLINES the cache tier and the
/// action falls to an idle LRU worker with NO cache benefit → it contributes 0
/// (the action is still PLACED — `running_j` rises — so the gate keeps evolving).
/// `load_penalty_j` is CONSTANT (computed once from the real snapshot; the gate is
/// the contention, not a load ramp). Greedy does NOT model warming — it scores
/// each worker's snapshot cache, matching one-at-a-time dispatch.
///
/// SCOPE: `G` reproduces production's Tier-1.5 `argmax(s − load_penalty)` cache-
/// affinity ranking UNDER THE M1 GATE, INCLUDING the `blended_s > 0` crossover
/// (a load-dominated marginal pick is shed to the idle LRU path and contributes
/// 0, exactly as `api_worker_scheduler.rs` `best.filter(|blended_s| *blended_s >
/// 0)` does) — so `gain_pct` is an EXACT measure of the realizable reorder/warming
/// benefit over the full production selector's supra-threshold picks, not an
/// up-biased upper bound. It deliberately holds aside only the `p_headroom_pref`
/// SECONDARY-ranking key (M1 v2 — the intra-tier magnet fix) and the exact-root
/// (Tier-1) / LRU (Tier-2) tiers, because the metric ISOLATES the subtree-cache-
/// affinity ASSIGNMENT gap (does subtree-aware batching beat the cache-affinity
/// greedy) while faithfully modeling the gate's ELIGIBILITY restriction and the
/// crossover (the real contention + the real load-shed). `gain_pct` is therefore
/// the batch gap over the CACHE-AFFINITY GREEDY within the gated eligibility and
/// the load-crossover, not over the exact-root/LRU tiers or the pref key.
///
/// BATCH `B` (global, order-free, warming, gated): greedy-global max-weight —
/// each round picks the current best `(i, j)` pair over CACHE-ELIGIBLE workers
/// (gate re-evaluated as `running` rises), assigns it, `running_j += 1`, and
/// models INTRA-BATCH WARMING (add `A_i.dir_digests` to `W_j`'s will-be-warm set
/// so a later co-located action assigned to `W_j` scores the shared subtree as
/// cached — the action LANDS on `W_j` and materializes its tree there whether or
/// not the crossover credits the cache match). Ranks by RAW `s` (max) — the
/// cache-match mass; the load penalty only steers greedy. It credits the chosen
/// `s` ONLY when it clears the SAME Tier-1.5 `blended_s > 0` crossover greedy
/// applies (`s − load_penalty_j > 0`); a load-dominated pick contributes 0
/// (shed to the idle LRU path), applied SYMMETRICALLY so batch cannot claim a
/// cache credit greedy zeroes for the same pick. Because warming + the gate
/// CHANGE eligibility/scores after each assignment, pairs are re-derived each
/// round. `O(rounds × actions × workers)` set-membership, bounded by the sampled
/// window and ~10-worker fleet.
///
/// `B` is defined `max(B_heuristic, G)`: the from-scratch greedy-global is a
/// HEURISTIC, not the optimum, so it can score below `G` on contended cycles;
/// a real batch scheduler never does worse than greedy (it keeps the better of
/// its global solution and the greedy baseline). This makes `gain_pct` a genuine
/// LOWER BOUND (`≥ 0`, no silent clip) and sets `greedy_fallback` when the
/// heuristic underperformed so the counter surfaces how often that happens.
///
/// `gain_pct` is guarded on `G == 0` (empty inputs, or every greedy placement
/// cold) → `0`, avoiding divide-by-zero; a genuine positive potential requires
/// a nonzero greedy baseline (read alongside `sample_actions`/`sample_workers`).
///
/// Diagnostics: `greedy_score = G`; `gate_active_frac` = fraction of greedy steps
/// with the gate active (×100 — 100 = fully contended band, 0 = all-lifted);
/// `mean_seed_running` = mean seeded `running` across sampled workers (×100).
/// These make the REGIME visible in a scrape when interpreting `gain_pct`.
pub fn compute_batch_sched_gain(
    actions: &[BatchSchedAction],
    workers: &[BatchSchedWorker],
    gate_cfg: BatchSchedGateCfg,
) -> BatchSchedGain {
    let sample_actions = actions.len() as u64;
    let sample_workers = workers.len() as u64;

    let subtree_overlap_pct = compute_subtree_overlap_pct(actions);

    // (M1-replay diagnostic) Mean seeded running across sampled workers, ×100.
    // Computed over the SEED counts (before any solve mutates them) so it reports
    // the fleet's real depth in the gate band. `0` when no workers.
    let mean_seed_running = if workers.is_empty() {
        0
    } else {
        let sum_running: u64 = workers.iter().map(|w| w.running).sum();
        sum_running * 100 / workers.len() as u64
    };

    // Nothing to assign in either direction → no gain, but still report
    // coverage + overlap + the seed diagnostic so the reader sees WHY gain is 0.
    if actions.is_empty() || workers.is_empty() {
        return BatchSchedGain {
            gain_pct: 0,
            subtree_overlap_pct,
            sample_actions,
            sample_workers,
            greedy_fallback: false,
            greedy_score: 0,
            gate_active_frac: 0,
            mean_seed_running,
        };
    }

    let (greedy, gate_active_steps, greedy_steps) =
        greedy_assignment_score(actions, workers, gate_cfg);
    let batch_heuristic = batch_assignment_score(actions, workers, gate_cfg);

    // (M1-replay diagnostic) Fraction of greedy steps with the gate active, ×100.
    // Reveals the contended band (100) vs the all-lifted regime (0). `greedy_steps`
    // is `actions.len()` (every action is placed), but guard against 0 defensively.
    let gate_active_frac = if greedy_steps == 0 {
        0
    } else {
        gate_active_steps * 100 / greedy_steps
    };

    // `batch_assignment_score` is a from-scratch greedy-global HEURISTIC, NOT
    // the optimal batch assignment — it can score BELOW `greedy` on contended
    // cycles. A real batch scheduler would never do worse than greedy: it picks
    // the better of its global solution and the greedy baseline. So define
    // `B := max(B_heuristic, G)`. This makes `gain_pct = (B−G)/G ≥ 0` a genuine
    // LOWER BOUND (no silent clip), and `greedy_fallback` records when the
    // heuristic underperformed so the counter surfaces how often it undersells.
    let greedy_fallback = batch_heuristic < greedy;
    let batch = batch_heuristic.max(greedy);
    // Guard G == 0: no greedy cache-match to improve on (divide-by-zero).
    let gain_pct = if greedy == 0 {
        0
    } else {
        (batch - greedy) * 100 / greedy
    };

    BatchSchedGain {
        gain_pct,
        subtree_overlap_pct,
        sample_actions,
        sample_workers,
        greedy_fallback,
        greedy_score: greedy,
        gate_active_frac,
        mean_seed_running,
    }
}

/// (M1-replay) Compute `gate_active` for the CURRENT step: the gate is active
/// only when enabled AND some worker still has p_headroom (Phase-1). When NO
/// worker has headroom the gate LIFTS (Phase-2) and cache-tier opens to all.
/// Mirrors `p_gate_active = p_headroom_gate_enabled && any_viable_has_p_headroom`
/// in `inner_find_and_reserve_worker`. (All sampled workers are viable — the
/// probe snapshot already applied the viability filter, matching the production
/// pre-scan that folds `any_viable_has_p_headroom` over VIABLE workers only.)
fn gate_active_now(workers: &[BatchSchedWorker], gate_cfg: BatchSchedGateCfg) -> bool {
    gate_cfg.enabled && workers.iter().any(|w| gate_cfg.has_p_headroom(w))
}

/// (M1-replay) GREEDY `G`: priority-order, one action at a time, each grabs its
/// `argmax_j (s − load_penalty_j)` over CACHE-ELIGIBLE workers (the live M1 gate),
/// then increments that worker's fresh `running` count (so it can lose
/// p_headroom for the next action). Sum the chosen `s` (unpenalized —
/// `load_penalty` steers the choice but the metric measures cache-match mass).
/// No warming (models one-at-a-time dispatch). Returns
/// `(total_score, gate_active_steps, total_steps)` for the diagnostic gauges.
///
/// EVERY action is placed: when no worker has p_headroom the gate LIFTS, so the
/// eligible set is never empty (all viable workers become eligible). If an action
/// finds no cache match on any eligible worker it still lands on the argmax
/// (best `−load_penalty`) eligible worker, scoring that worker's real match
/// (usually ~0) — matching production, which always dispatches a matched action.
fn greedy_assignment_score(
    actions: &[BatchSchedAction],
    workers: &[BatchSchedWorker],
    gate_cfg: BatchSchedGateCfg,
) -> (u64, u64, u64) {
    // Local mutable running counts, seeded from the real snapshot; rise as we
    // assign (the fresh-count contention the gate keys on).
    let mut state: Vec<BatchSchedWorker> = workers.to_vec();
    let mut total: u64 = 0;
    let mut gate_active_steps: u64 = 0;
    let mut steps: u64 = 0;
    for action in actions {
        steps += 1;
        // Recompute the gate for THIS step over the current running counts: the
        // Phase-1/Phase-2 fold. As workers fill, they lose headroom; when all
        // lose it the gate lifts and cache-tier opens to all.
        let gate_active = gate_active_now(&state, gate_cfg);
        if gate_active {
            gate_active_steps += 1;
        }
        // argmax over CACHE-ELIGIBLE workers of (s − load_penalty). Eligible =
        // gate lifted OR this worker still has p_headroom.
        let mut best: Option<(usize, i64, u64)> = None; // (worker_idx, penalized, raw_s)
        for (j, worker) in state.iter().enumerate() {
            if gate_active && !gate_cfg.has_p_headroom(worker) {
                continue; // gate-excluded from the cache tiers
            }
            let raw_s = batch_sched_score(action, &worker.cached_subtree_digests);
            let penalized = i64::try_from(raw_s).unwrap_or(i64::MAX) - worker.load_penalty;
            let dominated = best.is_some_and(|(_, best_pen, _)| penalized <= best_pen);
            if !dominated {
                best = Some((j, penalized, raw_s));
            }
        }
        if let Some((j, penalized, raw_s)) = best {
            // The action is PLACED (the fresh count rises so the gate can shut
            // this worker out for the next action) regardless of the crossover.
            state[j].running += 1;
            // (Tier-1.5 `blended_s > 0` crossover, `api_worker_scheduler.rs`
            // `best.filter(|blended_s| *blended_s > 0)`) — credit the cache match
            // ONLY when it beats the load cost (`penalized = raw_s − load_penalty
            // > 0`). When `penalized ≤ 0` production DECLINES the cache tier and
            // the action falls to an idle LRU worker with NO cache benefit → this
            // action contributes 0 to the cache-match mass (NOT `raw_s`). This
            // makes G a faithful mirror that does not over-credit load-shed
            // marginal picks (an up-bias on `gain_pct`).
            if penalized > 0 {
                total += raw_s;
            }
        }
        // `best` is None only if the eligible set is empty, which cannot happen
        // when there is ≥1 worker: if the gate is active some worker has
        // headroom (else it would have lifted); if lifted every worker is
        // eligible. So every action is placed.
    }
    (total, gate_active_steps, steps)
}

/// (M1-replay) BATCH `B`: global greedy-max-weight with intra-batch warming,
/// under the live M1 P-headroom gate. Each round picks the current best
/// `(action, worker)` pair over CACHE-ELIGIBLE workers (re-derived against the
/// evolving warm set AND the evolving gate), assigns it, increments that
/// worker's fresh `running` count, adds the action's `dir_digests` to that
/// worker's warm set, and sums the RAW score `s`. Rounds continue while ANY
/// unassigned action can still be placed on an eligible worker — INCLUDING
/// zero-score (cold) placements, because a cold placement WARMS its worker so a
/// later co-located sibling assigned there scores the shared subtree (the
/// co-location benefit is exactly this second-order effect). Reorder is inherent
/// — pairs are ranked globally, not per-action in priority order.
///
/// Gate re-evaluation: like greedy, the gate is recomputed each round over the
/// current `running` counts (Phase-1/Phase-2 fold). A worker that fills to
/// `running >= p_core_count` loses p_headroom and drops out of the cache-eligible
/// set; when NO worker has headroom the gate lifts and cache-tier opens to all.
/// So every action is eventually placeable (the eligible set is never empty when
/// ≥1 worker exists).
///
/// Pair ranking key (max wins): PRIMARY the current score `s`; SECONDARY, on a
/// tie (notably the all-cold tie), the overlap of the action's `dir_digests`
/// with the candidate worker's CURRENT warm set — so once one sibling warms a
/// worker, the tie-break pulls its co-located siblings onto the SAME worker,
/// grouping a shared cold subtree onto one materialization rather than
/// scattering it. Ties beyond that resolve to the smallest (action_idx,
/// worker_idx) for determinism.
fn batch_assignment_score(
    actions: &[BatchSchedAction],
    workers: &[BatchSchedWorker],
    gate_cfg: BatchSchedGateCfg,
) -> u64 {
    // Local mutable worker state (running rises as we assign — the gate keys on
    // the fresh count).
    let mut state: Vec<BatchSchedWorker> = workers.to_vec();
    // Per-worker will-be-warm set, seeded with the snapshot cache and grown as
    // actions are assigned (the intra-batch warming model).
    let mut warm: Vec<HashSet<DigestInfo>> = workers
        .iter()
        .map(|w| w.cached_subtree_digests.clone())
        .collect();
    let mut assigned: Vec<bool> = vec![false; actions.len()];
    let mut total: u64 = 0;

    // Each round assigns exactly one pair (if any is feasible); at most
    // `actions.len()` rounds. Re-deriving scores each round captures warming AND
    // the gate re-evaluation.
    for _round in 0..actions.len() {
        // Recompute the gate for THIS round over the current running counts.
        let gate_active = gate_active_now(&state, gate_cfg);
        // (score, warm_overlap) tuple maximized; ties → first-seen (smallest
        // action_idx then worker_idx).
        let mut best: Option<(usize, usize, u64, usize)> = None; // (i, j, s, warm_overlap)
        for (i, action) in actions.iter().enumerate() {
            if assigned[i] {
                continue;
            }
            for (j, worker) in state.iter().enumerate() {
                if gate_active && !gate_cfg.has_p_headroom(worker) {
                    continue; // gate-excluded from the cache tiers this round
                }
                let s = batch_sched_score(action, &warm[j]);
                // Secondary key: how many of this action's dir digests the
                // worker's warm set ALREADY holds (co-location grouping on the
                // all-cold tie). Cheap set-membership over the action's dirs.
                let warm_overlap = action
                    .dir_digests
                    .iter()
                    .filter(|d| warm[j].contains(d))
                    .count();
                let dominated = best.is_some_and(|(_, _, best_s, best_ov)| {
                    (s, warm_overlap) <= (best_s, best_ov)
                });
                if !dominated {
                    best = Some((i, j, s, warm_overlap));
                }
            }
        }
        match best {
            Some((i, j, s, _)) => {
                assigned[i] = true;
                // The action LANDS on this worker (fresh count rises → the gate
                // can shut it out next round) and materializes its input tree
                // there (warming), regardless of the crossover — a real batch
                // scheduler still places the action on SOME worker and warms it.
                state[j].running += 1;
                // Warm the chosen worker with this action's subtrees so a later
                // co-located action assigned here scores the shared subtree.
                for d in &actions[i].dir_digests {
                    warm[j].insert(*d);
                }
                // (Tier-1.5 `blended_s > 0` crossover — the SAME predicate greedy
                // applies, `api_worker_scheduler.rs` `best.filter(|blended_s|
                // *blended_s > 0)`) — credit the cache match ONLY when it beats
                // the load cost (`s − load_penalty > 0`). When `s − load_penalty
                // ≤ 0` production DECLINES the cache tier and the action falls to
                // an idle LRU worker with NO cache benefit → contribute 0 (NOT
                // `s`). Applied symmetrically to G and B so batch cannot claim a
                // cache credit greedy zeroes for the same load-dominated pick.
                if i64::try_from(s).unwrap_or(i64::MAX) - state[j].load_penalty > 0 {
                    total += s;
                }
            }
            // No unassigned action has any eligible worker this round. With ≥1
            // worker the gate lift guarantees eligibility, so this only triggers
            // once all actions are assigned (or there are zero workers) → stop.
            None => break,
        }
    }
    total
}

/// (#batch-sched) `subtree_overlap_pct`: `Σ dir_direct_bytes` over directory
/// digests appearing in ≥2 sampled actions' `dir_digests`, / total sampled
/// subtree bytes, `× 100` floored. Measures "is there batchable structure" —
/// expected non-zero for builds sharing external-crate rlibs / source dirs.
/// A shared digest's direct bytes are counted ONCE in the numerator (its value
/// is taken from the first action that carries it — disjoint per-directory, so
/// all carriers agree). The denominator sums every action's every directory's
/// direct bytes (with multiplicity across actions — total sampled subtree mass).
fn compute_subtree_overlap_pct(actions: &[BatchSchedAction]) -> u64 {
    // Count in how many DISTINCT actions each directory digest appears, and
    // remember one direct-byte value for it.
    // digest → (action_count, direct_bytes)
    let mut appearances: HashMap<DigestInfo, (u64, u64)> = HashMap::new();
    let mut total_bytes: u64 = 0;
    for action in actions {
        for d in &action.dir_digests {
            let bytes = action.dir_direct_bytes.get(d).copied().unwrap_or(0);
            total_bytes += bytes;
            let entry = appearances.entry(*d).or_insert((0, bytes));
            entry.0 += 1;
            // Keep the first-seen byte value (disjoint partition → identical
            // across actions carrying the same directory digest).
        }
    }
    if total_bytes == 0 {
        return 0;
    }
    let shared_bytes: u64 = appearances
        .values()
        .filter(|(count, _)| *count >= 2)
        .map(|(_, bytes)| *bytes)
        .sum();
    shared_bytes * 100 / total_bytes
}

/// (#output-locality-probe) The OPPORTUNITY result the output-affinity probe
/// stores into gauges. Measures how often a ready action's INPUT directory
/// subtree matches an OUTPUT directory a STILL-CONNECTED worker recently
/// produced — the ceiling of what "route a consumer to the worker that produced
/// its inputs" could exploit, a signal the scheduler does NOT currently have (its
/// only affinity signal is input-tree overlap via `cached_subtree_digests`).
///
/// This is OPPORTUNITY (a match EXISTS: a connected worker produced this
/// directory), which is SEPARATE from realizability (the worker must ALSO still
/// hold the output bytes locally to hardlink them — that is a worker-side
/// retention change, out of scope for this probe). `map_size` is filled by the
/// caller from the bounded map, not by the pure function.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutputAffinityGain {
    /// Fraction (×100, floored) of sampled ready actions with ≥1 input directory
    /// digest that hits the output→producer map for a producer STILL CONNECTED.
    /// `0` when there are no sampled actions.
    pub match_frac: u64,
    /// Byte-mass of matched output subtrees, using the SAME model as
    /// `compute_dedup_cached_score` + the Tier-1.5 dispatch blend:
    /// `Σ dir_direct_bytes[d] + dir_direct_files[d]·PER_FILE_WEIGHT` over every
    /// matched directory digest `d`, summed across sampled actions. Comparable to
    /// the `batch_sched_*` byte numbers (same weighting).
    pub matched_bytes: u64,
    /// Distinct producing workers matched this cycle (a producer counts once even
    /// if it produced several matched directories across the sample).
    pub distinct_producers: u64,
    /// Coverage denominator: number of sampled ready actions the computation ran
    /// over (mirrors `batch_sched_sample_actions`). `match_frac` is meaningful
    /// only at `≥ 1`.
    pub sample_actions: u64,
}

/// (#output-locality-probe) PURE (no I/O, no lock, no scheduler state): over the
/// sampled ready-action window `sampled` (each carrying the action's input
/// directory digests + the disjoint per-directory direct byte/file weights,
/// already produced by the `tree_cache` peek the batch probe does), count how
/// often an input directory digest matches an OUTPUT directory recently produced
/// by a STILL-CONNECTED worker.
///
/// A directory digest `d` of action `i` MATCHES iff `producer_map[d]` exists AND
/// that producer is in `connected`. The `producer_map` is keyed on the
/// constituent `Directory` digests of recently-produced output `Tree`s (root +
/// children) — NOT on the `Tree` digest itself, because a downstream consumer
/// references an output directory by its root/child `Directory` digest (which is
/// what appears in the consumer's input-tree `dir_digests`), and the `Tree`
/// digest is a digest of a DIFFERENT message shape that never appears in an input
/// tree (see the design doc + REAPI `OutputDirectory.tree_digest`).
///
/// Byte-mass uses the SAME weighting as the Tier-1.5 dispatch score
/// (`compute_dedup_cached_score` + `PER_FILE_WEIGHT`), summed over matched
/// directories across all sampled actions, so `matched_bytes` is directly
/// comparable to the `batch_sched_*` numbers.
///
/// It does NOT change dispatch — the real scheduler has no output-affinity tier.
pub fn compute_output_affinity(
    sampled: &[BatchSchedAction],
    producer_map: &HashMap<DigestInfo, WorkerId>,
    connected: &HashSet<WorkerId>,
) -> OutputAffinityGain {
    let sample_actions = sampled.len() as u64;
    let mut actions_with_match: u64 = 0;
    let mut matched_bytes: u64 = 0;
    let mut distinct_producers: HashSet<WorkerId> = HashSet::new();

    for action in sampled {
        let mut this_action_matched = false;
        for d in &action.dir_digests {
            // A directory the action needs as INPUT that a worker recently
            // PRODUCED as OUTPUT — and that worker is still connected (so it
            // could, retention permitting, serve the consumer).
            if let Some(producer) = producer_map.get(d) {
                if connected.contains(producer) {
                    this_action_matched = true;
                    distinct_producers.insert(producer.clone());
                    // Same byte model as compute_dedup_cached_score + Tier-1.5:
                    // direct bytes + direct files × PER_FILE_WEIGHT for this
                    // matched directory digest (disjoint partition → no
                    // double-count across directories of the same action).
                    let direct_bytes = action.dir_direct_bytes.get(d).copied().unwrap_or(0);
                    let direct_files = action.dir_direct_files.get(d).copied().unwrap_or(0);
                    matched_bytes += direct_bytes + direct_files * PER_FILE_WEIGHT;
                }
            }
        }
        if this_action_matched {
            actions_with_match += 1;
        }
    }

    let match_frac = if sample_actions == 0 {
        0
    } else {
        actions_with_match * 100 / sample_actions
    };

    OutputAffinityGain {
        match_frac,
        matched_bytes,
        distinct_producers: distinct_producers.len() as u64,
        sample_actions,
    }
}

/// (#output-locality-probe / file-level) The OPPORTUNITY result the FILE-level
/// output-affinity probe stores into gauges. Sibling to `OutputAffinityGain` but
/// measuring INPUT-FILE ∩ recently-produced-OUTPUT-FILE overlap — the signal that
/// feeds the EXISTING `score_and_generate_hints(&tree.file_digests, loc_map)`
/// peer-fetch/prefetch mechanism (`api_worker_scheduler.rs`), which today never
/// sees outputs in its locality map.
///
/// `matched_bytes` (sum of matched file SIZES) is the HEADLINE — the directory
/// probe showed match_frac inflates on content-free digests while byte-mass is
/// the honest signal. `largest_contributor_bytes` exposes whether a SINGLE
/// ubiquitous file digest dominates the mass (the file-level analog of the
/// empty-`Directory{}` artifact: a common small file compiled into many outputs).
/// `map_size` is filled by the caller from the bounded map.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutputFileAffinityGain {
    /// Fraction (×100, floored) of sampled ready actions with ≥1 input FILE
    /// digest that hits the output-file→producer map for a STILL-CONNECTED
    /// producer. `0` when there are no sampled actions.
    pub match_frac: u64,
    /// HEADLINE: sum of the SIZES (bytes) of matched input files across all
    /// sampled actions — the byte-mass a peer-fetch/hardlink from the producer
    /// would move. A file digest is counted once PER ACTION (an action's
    /// `file_digests` is already deduplicated).
    pub matched_bytes: u64,
    /// Distinct producing workers matched this cycle (counted once each).
    pub distinct_producers: u64,
    /// Coverage denominator: number of sampled ready actions scored.
    pub sample_actions: u64,
    /// The single output-file digest contributing the MOST matched bytes this
    /// cycle (its size × how many sampled actions matched it). Surfaces a
    /// ubiquitous-but-nonzero dominator so `matched_bytes` is not silently one
    /// hot file — the file-level guard against the empty-`Directory{}`-class
    /// artifact. `0` when nothing matched.
    pub largest_contributor_bytes: u64,
}

/// (#output-locality-probe / file-level) PURE (no I/O, no lock, no scheduler
/// state): over the sampled ready-action window `sampled` (each carrying the
/// action's input `(file_digest, size)` pairs from `ResolvedTree.file_digests`),
/// count how often an input FILE digest matches a file recently PRODUCED as
/// output by a STILL-CONNECTED worker, and sum the matched file SIZES.
///
/// A file digest `f` of action `i` MATCHES iff `producer_map[f]` exists AND that
/// producer is in `connected`. `matched_bytes` sums the file size for each match
/// (per action). `largest_contributor_bytes` tracks the single output-file digest
/// whose (size × matching-action-count) is largest, so a reader can tell whether
/// the mass is one ubiquitous file or genuinely spread — the file-level analog of
/// the directory probe's empty-`Directory{}` diagnosis.
///
/// It does NOT change dispatch — the scheduler has no output-file-affinity tier;
/// this measures the ceiling for feeding outputs into the existing
/// `score_and_generate_hints` locality map.
pub fn compute_output_file_affinity(
    sampled: &[OutputFileAffinityAction],
    producer_map: &HashMap<DigestInfo, WorkerId>,
    connected: &HashSet<WorkerId>,
) -> OutputFileAffinityGain {
    let sample_actions = sampled.len() as u64;
    let mut actions_with_match: u64 = 0;
    let mut matched_bytes: u64 = 0;
    let mut distinct_producers: HashSet<WorkerId> = HashSet::new();
    // Per matched file digest: (its size, how many actions matched it) → the
    // (size × count) contribution to matched_bytes, so we can report the single
    // largest contributor.
    let mut contributor_counts: HashMap<DigestInfo, (u64, u64)> = HashMap::new();

    for action in sampled {
        let mut this_action_matched = false;
        for (f, size) in &action.file_digests {
            // A file the action needs as INPUT that a worker recently PRODUCED as
            // OUTPUT — and that worker is still connected (so a peer-fetch/prefetch
            // hint to it would be actionable).
            if let Some(producer) = producer_map.get(f) {
                if connected.contains(producer) {
                    this_action_matched = true;
                    distinct_producers.insert(producer.clone());
                    matched_bytes += *size;
                    let entry = contributor_counts.entry(*f).or_insert((*size, 0));
                    entry.1 += 1;
                }
            }
        }
        if this_action_matched {
            actions_with_match += 1;
        }
    }

    let match_frac = if sample_actions == 0 {
        0
    } else {
        actions_with_match * 100 / sample_actions
    };

    // Largest single-digest contribution = max over matched digests of size×count.
    let largest_contributor_bytes = contributor_counts
        .values()
        .map(|(size, count)| size * count)
        .max()
        .unwrap_or(0);

    OutputFileAffinityGain {
        match_frac,
        matched_bytes,
        distinct_producers: distinct_producers.len() as u64,
        sample_actions,
        largest_contributor_bytes,
    }
}

/// (#output-locality-probe / file-level) One sampled ready action's INPUT file
/// digests + sizes, copied from the cached `ResolvedTree.file_digests` at the
/// probe sample point. `file_digests` is already deduplicated per action (the
/// BFS `seen_files` set), so each digest appears once.
#[derive(Debug, Clone)]
pub struct OutputFileAffinityAction {
    /// `(file_blob_digest, size_bytes)` for every distinct file in the action's
    /// input tree.
    pub file_digests: Vec<(DigestInfo, u64)>,
}

/// (#batch-affinity, dimension B) Type-erased injectable clock producing the
/// `SystemTime` that timestamps arrivals. `SystemTime::now` in prod;
/// `MockInstantWrapped`'s `now()` (mock-clock-driven) in tests. Erased so
/// `SimpleScheduler` need not carry the `NowFn`/`InstantWrapper` generics.
type AffinityClock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

/// (#batch-affinity) OBSERVABILITY-ONLY scalar gauges/counter estimating the
/// potential profitability of batch (multi-task) dir-cache-affinity scheduling.
/// None of these fields influence assignment; they only MEASURE whether a
/// hypothetical batch/delay strategy would help. Emitted on `/metrics` under
/// `scheduler.<name>.action.batch_affinity.<field>` (the action-scheduler
/// registration prefix — see `src/bin/nativelink.rs:591`).
///
/// Interpretation: the HEADLINE is `batch_sched_gain_pct` — the aggregate
/// dir-cache-match improvement a subtree-aware batch assignment would buy over
/// the current CACHE-AFFINITY greedy assignment THIS cycle (`(B−G)/G`; design
/// `.claude/audits/batch-scheduling-subtree-assignment-metric-design-2026-07-02.md`).
/// SCOPE: `G` models production's Tier-1.5 `argmax(s − load_penalty)`
/// cache-affinity ranking UNDER THE LIVE M1 P-HEADROOM GATE, INCLUDING the
/// `blended_s > 0` crossover (a load-dominated marginal pick is shed to the idle
/// LRU path and contributes 0, mirroring `api_worker_scheduler.rs`
/// `best.filter(|blended_s| *blended_s > 0)`) — so `gain_pct` is an EXACT measure
/// of the realizable reorder/warming benefit over the selector's supra-threshold
/// picks, NOT an up-biased upper bound. It holds aside only the `p_headroom_pref`
/// SECONDARY key (M1 v2) and the exact-root (Tier-1) / LRU (Tier-2) tiers, so the
/// gauge ISOLATES the subtree-cache-affinity assignment gap. It is a genuine LOWER
/// BOUND: `B := max(B_heuristic, G)`, so `gain_pct ≥ 0` and a rising
/// `batch_sched_greedy_fallback_total` means the heuristic undersold the gain on
/// those cycles (read "≥ this", not "no gain"). It is a per-cycle POTENTIAL, not a
/// realized speedup.
///
/// READ `gain_pct` ONLY WITH ITS COMPANIONS — it is meaningful only when
/// `batch_sched_sample_actions ≥ 2`, `batch_sched_sample_workers ≥ 2`, and
/// `batch_sched_uncached_skipped` is a small fraction (design §6), AND alongside
/// `batch_sched_gate_active_frac` (≈100 = the CONTENDED band = the batch-target
/// regime where reorder can help; ≈0 with a large `batch_sched_greedy_score` =
/// the heavy-load lift regime where greedy is already near-optimal) and
/// `batch_sched_subtree_overlap_pct` (is there batchable structure at all).
///
/// CAVEAT — `gain_pct == 0` does NOT mean "batch scheduling is not worth
/// building." This gauge measures batch-over-greedy ASSIGNMENT QUALITY WITHIN the
/// current M1 gate's eligibility. The GATE ITSELF is the dominant cache loss: it
/// over-excludes idle-P cache-holders on the large majority of contended cycles
/// (the upstream loss that the M1 v2 `p_headroom_pref` rebalance addresses),
/// which is INVISIBLE to this gauge (the gauge takes the gate as given and only
/// asks whether reorder+warming beats greedy under it). So a low `gain_pct` is a
/// verdict on batch-vs-greedy under the gate, NOT on the value of fixing the gate.
///
/// CAVEAT — GATE-ON PREMISE. This model assumes the live M1 P-headroom gate is ON
/// (verified 2026-07-02: 25,303 `p_headroom_gate_exclusion` events since boot;
/// `p_headroom_gate_enabled: true` in the deployed config, `p_idle_threshold_pct
/// == 0` → v1). The gauge threads the live `BatchSchedGateCfg` snapshot, so if the
/// config DRIFTS to gate-OFF on a restart the model reverts to the ungated
/// selector: with no eligibility restriction greedy already reaches each action's
/// best cache holder and `gain_pct` collapses to a structural ≈0 — NOT a signal
/// that batching is worthless, but that the premise this gauge is designed for
/// (the gated regime) no longer holds. Cross-check `batch_sched_gate_active_frac`
/// (0 across all cycles with the fleet contended = the gate is off).
///
/// `batch_sched_subtree_overlap_pct` answers "is there batchable structure"
/// (shared-subtree mass) — expected non-zero for builds sharing external-crate
/// rlibs / source dirs. `arrival_within_250ms_total` is the orthogonal temporal
/// batching signal (still valid).
///
/// The `*_exact_root_reference` gauges are the SUPERSEDED exact-input-root
/// signal (`sampled_ops − distinct(input_root_digest)`). They key on the FULL
/// input root, which build actions essentially never share (each compile's input
/// tree is unique), so they read near-always 0 for builds and MEASURE THE WRONG
/// THING — kept only as a labeled reference so dashboards migrating off them see
/// continuity, NOT as a batch-scheduling green light. Prefer `batch_sched_*`.
///
/// These are per-key SCALAR aggregates on purpose: `MetricsComponent` cannot
/// emit dynamic per-digest/per-worker labels, so we aggregate to gauges +
/// a counter. The `#[metric]` names are STABLE — dashboards key on them.
#[derive(Debug, Default, MetricsComponent)]
pub struct BatchAffinityMetrics {
    /// (#batch-sched) HEADLINE gauge: `(B − G) / G * 100` — the aggregate
    /// subtree-cache-match score improvement a global batch assignment (reorder +
    /// intra-batch warming) would buy over the current greedy priority-order
    /// assignment, both UNDER THE LIVE M1 P-HEADROOM GATE + the `blended_s > 0`
    /// load crossover (so this is an EXACT realizable gain, not an upper bound),
    /// computed over the sampled cached pending window at the last match cycle.
    /// `0` when `G == 0` or coverage is empty. MEANINGFUL ONLY with
    /// `batch_sched_sample_actions/workers ≥ 2`, low `batch_sched_uncached_skipped`,
    /// and read against `batch_sched_gate_active_frac` (design §5/§6). `gain_pct ==
    /// 0` is NOT "batch not worth building" — the GATE itself is the dominant cache
    /// loss and is invisible here; this only measures batch-vs-greedy UNDER it.
    /// Assumes the gate is ON (prod: enabled, threshold 0); a config drift to
    /// gate-OFF collapses this to a structural ≈0 (see the struct-level doc).
    #[metric(
        help = "batch-scheduling potential: (B-G)/G percent aggregate dir-cache-match gain of a subtree-aware batch assignment over the current greedy, last match cycle; read with sample_actions/workers>=2 and low uncached_skipped"
    )]
    pub batch_sched_gain_pct: AtomicU64,

    /// (#batch-sched) Gauge: shared-subtree mass — `Σ dir_direct_bytes` over
    /// directory digests appearing in ≥2 sampled actions' `dir_digests`, / total
    /// sampled subtree bytes, percent. "Is there batchable structure" (expected
    /// non-zero for builds); independent of worker state.
    #[metric(
        help = "shared-subtree mass percent: sum of direct bytes of directory digests shared by >=2 sampled pending actions over total sampled subtree bytes, last match cycle"
    )]
    pub batch_sched_subtree_overlap_pct: AtomicU64,

    /// (#batch-sched) Coverage guardrail gauge: number of sampled pending actions
    /// whose `ResolvedTree` was ALREADY cached (peek hit) and thus scored in the
    /// counterfactual. `gain_pct` is a low-coverage artifact below 2.
    #[metric(
        help = "number of sampled pending actions with an already-cached resolved tree scored in the batch counterfactual, last match cycle (gain_pct meaningful only at >=2)"
    )]
    pub batch_sched_sample_actions: AtomicU64,

    /// (#batch-sched) Coverage guardrail gauge: number of workers-with-capacity
    /// the counterfactual assigned over. `gain_pct` is a low-coverage artifact
    /// below 2.
    #[metric(
        help = "number of capacity-bearing workers in the batch counterfactual, last match cycle (gain_pct meaningful only at >=2)"
    )]
    pub batch_sched_sample_workers: AtomicU64,

    /// (#batch-sched) Coverage guardrail gauge: number of sampled pending actions
    /// SKIPPED because their `ResolvedTree` was NOT yet cached (a `tree_cache`
    /// peek miss — the probe NEVER resolves a tree). A large fraction means the
    /// `gain_pct` sample is unrepresentative (design §3/§6).
    #[metric(
        help = "sampled pending actions skipped because their resolved tree was not yet cached (probe never resolves); a large fraction means gain_pct is low-coverage, last match cycle"
    )]
    pub batch_sched_uncached_skipped: AtomicU64,

    /// (#batch-sched) CUMULATIVE counter: match cycles in which the from-scratch
    /// batch HEURISTIC scored BELOW the greedy baseline `G`, so `B` was floored
    /// up to `G` (`B := max(B_heuristic, G)`) and `gain_pct` reported 0 for that
    /// cycle. A growing value means the heuristic UNDERSELLS the batch gain on
    /// contended cycles (it is a lower bound, not the optimum) — so a small,
    /// non-zero `batch_sched_gain_pct` alongside a rising fallback count should
    /// be read as "≥ this, possibly more", not "batch does not help". NOT an
    /// error condition.
    #[metric(
        help = "cumulative match cycles where the from-scratch batch heuristic underperformed greedy (B floored to G); a rising value means batch_sched_gain_pct is a loose lower bound on those cycles"
    )]
    pub batch_sched_greedy_fallback_total: AtomicU64,

    /// (M1-replay) Gauge: the greedy baseline `G` — the aggregate chosen
    /// `cache_score` under priority-order assignment at the last match cycle. The
    /// `gain_pct` denominator, exposed raw so a large `greedy_score` at
    /// `gain_pct == 0` reads as "greedy already near-optimal (heavy-load lift
    /// regime)", not "no opportunity".
    #[metric(
        help = "batch-scheduling greedy baseline G: aggregate chosen dir-cache-match score under the current greedy priority-order assignment, last match cycle (the gain_pct denominator)"
    )]
    pub batch_sched_greedy_score: AtomicU64,

    /// (M1-replay diagnostic) Gauge: fraction of the GREEDY assignment steps on
    /// which the live M1 P-headroom gate was ACTIVE (some viable worker still had
    /// p_headroom → cache-tier eligibility restricted), ×100. `100` = fully
    /// contended band; `0` = every step ran with the gate lifted (all-full).
    /// Reveals WHICH regime the fleet is in when interpreting `batch_sched_gain_pct`.
    #[metric(
        help = "fraction (x100) of greedy assignment steps where the M1 P-headroom gate was active (contended band) vs lifted (all-full), last match cycle; interpret batch_sched_gain_pct against this"
    )]
    pub batch_sched_gate_active_frac: AtomicU64,

    /// (M1-replay diagnostic) Gauge: mean SEEDED `running` (fresh in-flight)
    /// count across the sampled workers, ×100 (so the sub-integer mean is
    /// legible). Reveals how deep in the gate band the fleet sits (near
    /// `p_core_count` = contended; well below = idle).
    #[metric(
        help = "mean seeded fresh in-flight (running_action_infos.len()) count across sampled workers, x100, last match cycle; how deep in the M1 gate band the fleet sits"
    )]
    pub batch_sched_mean_seed_running: AtomicU64,

    /// (SUPERSEDED — exact-input-root reference) Gauge:
    /// `pending_ops − distinct_input_roots` over the sampled pending prefix. Keys
    /// on the FULL `input_root_digest`, which build actions essentially never
    /// share → near-always 0 for builds (MEASURES THE WRONG THING; superseded by
    /// `batch_sched_gain_pct`). Kept, relabeled, only for dashboard continuity.
    #[metric(
        help = "SUPERSEDED exact-input-root reference: sampled pending ops minus distinct input roots; near-always 0 for builds (they never share a full input root) — prefer batch_sched_gain_pct"
    )]
    pub colocation_surplus_exact_root_reference: AtomicU64,

    /// (SUPERSEDED — exact-input-root reference) Gauge: size of the largest
    /// same-input-root pending group. Same exact-root limitation as
    /// `colocation_surplus_exact_root_reference`; kept only for continuity.
    #[metric(
        help = "SUPERSEDED exact-input-root reference: largest same-input-root pending group; near-always 1 for builds — prefer batch_sched_subtree_overlap_pct"
    )]
    pub max_group_exact_root_reference: AtomicU64,

    /// Gauge: number of pending ops the exact-root reference was computed over at
    /// the last match cycle (== min(pending, MAX_PENDING_AFFINITY_SAMPLE)). Also
    /// the window size the batch counterfactual sampled BEFORE the cached-tree
    /// filter (`batch_sched_sample_actions` + `batch_sched_uncached_skipped`
    /// partition it).
    #[metric(
        help = "pending ops sampled at last match cycle; equals the sample cap when the pending set is larger; partitioned by the batch probe into sample_actions + uncached_skipped"
    )]
    pub sampled_ops: AtomicU64,

    /// Dimension B counter: cumulative count of arriving actions whose input
    /// root matched a peer seen within AFFINITY_ARRIVAL_WINDOW (250ms). Each
    /// increment is one co-location opportunity a 250ms accumulation delay
    /// would have captured.
    #[metric(
        help = "cumulative arrivals whose input root matched a peer within 250ms; each is a co-location opportunity a small accumulation delay would capture"
    )]
    pub arrival_within_250ms_total: AtomicU64,
}

/// (#output-locality-probe) OBSERVABILITY-ONLY gauges quantifying the OUTPUT-
/// LOCALITY OPPORTUNITY: how often a ready action's INPUT directory subtree
/// matches an OUTPUT directory a STILL-CONNECTED worker recently produced. This
/// is the ceiling of what "route a consumer to the worker that produced its
/// inputs" could exploit — a signal the scheduler does NOT currently have (its
/// only affinity signal is INPUT-tree overlap via `cached_subtree_digests`,
/// whose batch-scheduling gain measured ~0%). Emitted on `/metrics` under
/// `scheduler.<name>.action.output_affinity.<field>` — a SIBLING namespace to
/// `...action.batch_affinity.batch_sched_*`.
///
/// None of these fields influence assignment; they only MEASURE the opportunity.
/// Interpretation: this is OPPORTUNITY (a match EXISTS — a connected worker
/// produced this directory), which is SEPARATE from realizability (the worker
/// must ALSO still hold the output bytes locally to hardlink them — a worker-side
/// retention change, OUT OF SCOPE for this probe). Read `output_affinity_match_frac`
/// against `output_affinity_sample_actions ≥ 1` (coverage) and
/// `output_affinity_map_size` (is the bounded map saturated / binding).
#[derive(Debug, Default, MetricsComponent)]
pub struct OutputAffinityMetrics {
    /// (#output-locality-probe) HEADLINE gauge: fraction (×100) of sampled ready
    /// actions with ≥1 input directory digest that matches an output directory a
    /// STILL-CONNECTED worker recently produced, last match cycle. `0` when
    /// coverage (`output_affinity_sample_actions`) is 0. This is the OUTPUT-
    /// locality opportunity ceiling; compare it against the INPUT-overlap signal
    /// (`batch_sched_*`) to see which affinity dimension is larger.
    // Field names are the LEAF under the `output_affinity` group → the emitted
    // metric is `..._action_output_affinity_match_frac` (no doubled prefix; the
    // group already carries `output_affinity`, mirroring how `batch_affinity`'s
    // fields are `batch_sched_*`, not `batch_affinity_*`).
    #[metric(
        help = "output-locality opportunity: percent of sampled ready actions with >=1 input dir produced as output by a still-connected worker, last match cycle; read with sample_actions>=1"
    )]
    pub match_frac: AtomicU64,

    /// (#output-locality-probe) Gauge: byte-mass of matched output subtrees, SAME
    /// weighting as the Tier-1.5 dispatch score (`compute_dedup_cached_score` +
    /// `PER_FILE_WEIGHT`: `Σ dir_direct_bytes + dir_direct_files·100KiB` over
    /// matched dirs), summed across sampled actions, last match cycle. Comparable
    /// to the `batch_sched_*` byte numbers.
    #[metric(
        help = "byte-mass of matched output subtrees (direct bytes + files*100KiB, same model as batch_sched), summed across sampled actions, last match cycle"
    )]
    pub matched_bytes: AtomicU64,

    /// (#output-locality-probe) Gauge: distinct producing workers matched this
    /// cycle (a producer counts once even if it produced several matched dirs).
    #[metric(
        help = "distinct still-connected producing workers matched by the sampled ready actions, last match cycle"
    )]
    pub distinct_producers: AtomicU64,

    /// (#output-locality-probe) Coverage guardrail gauge: number of sampled ready
    /// actions the match computation ran over (those whose input tree was already
    /// cached — mirrors `batch_sched_sample_actions`). `output_affinity_match_frac`
    /// is a low-coverage artifact below 1.
    #[metric(
        help = "number of sampled ready actions with an already-cached input tree scored in the output-affinity computation, last match cycle (match_frac meaningful only at >=1)"
    )]
    pub sample_actions: AtomicU64,

    /// (#output-locality-probe) Gauge: current entries in the bounded
    /// output→producer map (Directory digests of recently-produced outputs). If
    /// this sits at the cap (`OUTPUT_PRODUCER_MAP_CAP`), the recency window is
    /// binding — the oldest output dirs are being LRU-evicted, so `match_frac` is
    /// a lower bound on the true opportunity over a longer window.
    #[metric(
        help = "current entries in the bounded output->producer directory-digest map; at the cap means the recency window is binding (match_frac is then a lower bound)"
    )]
    pub map_size: AtomicU64,
}

/// (#output-locality-probe / file-level) OBSERVABILITY-ONLY gauges quantifying the
/// FILE-level OUTPUT-locality opportunity: how often a ready action's INPUT files
/// overlap files a STILL-CONNECTED worker recently produced as output. This is the
/// ceiling for feeding outputs into the EXISTING
/// `score_and_generate_hints(&tree.file_digests, loc_map)` peer-fetch/prefetch
/// mechanism (which today never sees outputs in its locality map). Emitted under
/// `scheduler.<name>.action.output_file_affinity.<field>` — a SIBLING namespace to
/// `output_affinity` (directory-level) and `batch_affinity` (input-overlap).
///
/// `matched_bytes` (sum of matched file SIZES) is the HEADLINE — the directory
/// probe showed match_frac inflates on content-free digests while byte-mass is the
/// honest signal. `largest_contributor_bytes` exposes whether a single ubiquitous
/// file dominates the mass. Read `match_frac` against `sample_actions ≥ 1`.
#[derive(Debug, Default, MetricsComponent)]
pub struct OutputFileAffinityMetrics {
    /// (#output-locality-probe / file-level) Gauge: fraction (×100) of sampled
    /// ready actions with ≥1 input FILE digest a STILL-CONNECTED worker recently
    /// produced as output, last match cycle. `0` when `sample_actions` is 0. Read
    /// `matched_bytes` as the honest headline; a high match_frac with low
    /// matched_bytes is content-light (see the directory probe's empty-dir finding).
    #[metric(
        help = "file-level output-locality opportunity: percent of sampled ready actions with >=1 input file produced as output by a still-connected worker, last match cycle; read matched_bytes as the honest headline"
    )]
    pub match_frac: AtomicU64,

    /// (#output-locality-probe / file-level) HEADLINE gauge: sum of the SIZES
    /// (bytes) of matched input files across sampled actions, last match cycle —
    /// the byte-mass a peer-fetch/hardlink from the producer would move. This is
    /// the load-bearing signal (real bytes), unlike the frac.
    #[metric(
        help = "HEADLINE: sum of matched input-file sizes (bytes) across sampled ready actions, last match cycle — the byte-mass a peer-fetch from the producer would move"
    )]
    pub matched_bytes: AtomicU64,

    /// (#output-locality-probe / file-level) Gauge: distinct producing workers
    /// matched this cycle.
    #[metric(
        help = "distinct still-connected producing workers matched by the sampled ready actions' input files, last match cycle"
    )]
    pub distinct_producers: AtomicU64,

    /// (#output-locality-probe / file-level) Coverage guardrail gauge: number of
    /// sampled ready actions scored (input tree already cached). `match_frac` /
    /// `matched_bytes` are low-coverage artifacts below 1.
    #[metric(
        help = "number of sampled ready actions with an already-cached input tree scored in the file-level output-affinity computation, last match cycle"
    )]
    pub sample_actions: AtomicU64,

    /// (#output-locality-probe / file-level) Gauge: current entries in the bounded
    /// output-file→producer map. At the cap (`OUTPUT_FILE_PRODUCER_MAP_CAP`) the
    /// recency window is binding, so match_frac/matched_bytes are lower bounds.
    #[metric(
        help = "current entries in the bounded output-file->producer file-digest map; at the cap means the recency window is binding (matched_bytes is then a lower bound)"
    )]
    pub map_size: AtomicU64,

    /// (#output-locality-probe / file-level) DIAGNOSTIC gauge: the single output
    /// file digest contributing the MOST matched bytes this cycle (its size × how
    /// many actions matched it). If this ≈ `matched_bytes`, ONE ubiquitous file
    /// dominates (the file-level analog of the empty-`Directory{}` artifact); if
    /// ≪ `matched_bytes`, the opportunity is genuinely spread across files.
    #[metric(
        help = "the single output-file digest contributing the most matched bytes (size x matching-action-count) this cycle; if ~= matched_bytes then one ubiquitous file dominates (content-light), if << then the opportunity is spread"
    )]
    pub largest_contributor_bytes: AtomicU64,
}

struct SimpleSchedulerActionStateResult {
    client_operation_id: OperationId,
    action_state_result: Box<dyn ActionStateResult>,
}

impl SimpleSchedulerActionStateResult {
    fn new(
        client_operation_id: OperationId,
        action_state_result: Box<dyn ActionStateResult>,
    ) -> Self {
        Self {
            client_operation_id,
            action_state_result,
        }
    }
}

#[async_trait]
impl ActionStateResult for SimpleSchedulerActionStateResult {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        let (mut action_state, origin_metadata) = self
            .action_state_result
            .as_state()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")?;
        // We need to ensure the client is not aware of the downstream
        // operation id, so override it before it goes out.
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok((action_state, origin_metadata))
    }

    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        let (mut action_state, origin_metadata) = self
            .action_state_result
            .changed()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")?;
        // We need to ensure the client is not aware of the downstream
        // operation id, so override it before it goes out.
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok((action_state, origin_metadata))
    }

    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        self.action_state_result
            .as_action_info()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
#[derive(MetricsComponent)]
pub struct SimpleScheduler {
    /// Manager for matching engine side of the state manager.
    #[metric(group = "matching_engine_state_manager")]
    matching_engine_state_manager: Arc<dyn MatchingEngineStateManager>,

    /// Manager for client state of this scheduler.
    #[metric(group = "client_state_manager")]
    client_state_manager: Arc<dyn ClientStateManager>,

    /// Manager for platform of this scheduler.
    #[metric(group = "platform_properties")]
    platform_property_manager: Arc<PlatformPropertyManager>,

    /// A `Workers` pool that contains all workers that are available to execute actions in a priority
    /// order based on the allocation strategy.
    #[metric(group = "worker_scheduler")]
    worker_scheduler: Arc<ApiWorkerScheduler>,

    /// The sender to send origin events to the origin events.
    // CAPPED: bounded mpsc::channel(max_event_queue_size) at construction
    // (`src/bin/nativelink.rs` origin-event wiring); None in prod
    // (experimental_origin_events unset), so no unbounded growth on this path.
    maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,

    /// Background task that tries to match actions to workers. If this struct
    /// is dropped the spawn will be cancelled as well.
    task_worker_matching_spawn: JoinHandleDropGuard<()>,

    /// (#obs-tuning) OBSERVABILITY-ONLY periodic task that emits ONE
    /// `tag = "speculative_hold_counters"` info-log (Stage-A/Stage-B decision
    /// counters) plus one `tag = "worker_construct_latency"` line per worker
    /// every `HOLD_COUNTERS_LOG_INTERVAL_S`, so those DARK-on-`/metrics`
    /// counters are scrapeable from journalctl for tuning `T_SETUP` / the hold
    /// caps against measured data. Interval-driven (NOT per-match); holds a
    /// `Weak<ApiWorkerScheduler>` so it exits when the scheduler drops. If this
    /// struct is dropped the spawn is cancelled as well. Reads the atomics +
    /// per-worker gossip `Relaxed`; changes NO scheduling decision.
    task_hold_counters_log_spawn: JoinHandleDropGuard<()>,

    /// (#task-resource-profile Phase-3 §12) Background task that periodically snapshots
    /// the resource-profile map to `resource_profile_persist_path` (interval
    /// `resource_profile_persist_interval_secs`). `None` when persistence is OFF (no
    /// path configured). Holds a `Weak<ApiWorkerScheduler>` so it exits when the
    /// scheduler drops; dropping this guard cancels it. The snapshot clones the map
    /// under the `parking_lot` lock then serializes + writes OFF the lock (no lock/await
    /// overlap); writes are atomic (tmp + rename) and NEVER fsync'd (advisory data).
    task_resource_profile_persist_spawn: Option<JoinHandleDropGuard<()>>,

    /// (#dag-criticality) Background task that periodically recomputes the DAG criticality
    /// snapshot (SCC + longest-path) and, when `dag_edge_store_persist_path` is set,
    /// persists the edge store. `None` when `dag_critical_path_enabled` is off. The
    /// `JoinHandleDropGuard` cancels it on scheduler drop.
    task_dag_recompute_spawn: Option<JoinHandleDropGuard<()>>,

    /// Every duration, do logging of worker matching
    /// e.g. "worker busy", "can't find any worker"
    /// Set to None to disable. This is quite noisy, so we limit it
    worker_match_logging_interval: Option<Duration>,

    /// Maximum number of actions that can be matched per client
    /// (identified by `instance_name`) in one matching cycle.
    /// 0 means unlimited (fair scheduling disabled).
    max_matches_per_client_per_cycle: usize,

    /// (#batch-affinity) OBSERVABILITY-ONLY scalar gauges/counter estimating
    /// whether batch (multi-task) dir-cache-affinity scheduling would pay off.
    /// Read-only w.r.t. assignment — see `BatchAffinityMetrics`.
    #[metric(group = "batch_affinity")]
    batch_affinity_metrics: BatchAffinityMetrics,

    /// (#output-locality-probe) OBSERVABILITY-ONLY gauges quantifying the OUTPUT-
    /// locality opportunity (a ready action's input subtree matches an output a
    /// still-connected worker recently produced). Read-only w.r.t. assignment —
    /// see `OutputAffinityMetrics`. SIBLING namespace to `batch_affinity`.
    #[metric(group = "output_affinity")]
    output_affinity_metrics: OutputAffinityMetrics,

    /// (#output-locality-probe / file-level) OBSERVABILITY-ONLY gauges quantifying
    /// the FILE-level output-locality opportunity (a ready action's input files
    /// overlap files a still-connected worker recently produced). Read-only w.r.t.
    /// assignment — see `OutputFileAffinityMetrics`. SIBLING namespace to
    /// `output_affinity` (directory-level) and `batch_affinity`.
    #[metric(group = "output_file_affinity")]
    output_file_affinity_metrics: OutputFileAffinityMetrics,

    /// (#batch-affinity, dimension B) Bounded recent-arrival window feeding
    /// `batch_affinity_metrics.arrival_within_250ms_total`. Guarded by a
    /// `parking_lot::Mutex` acquired only for the synchronous `record_arrival`
    /// call in `inner_add_action` (never held across `.await`).
    recent_roots_window: Mutex<RecentRootsWindow>,

    /// (#batch-affinity, dimension B) The scheduler's injectable clock,
    /// type-erased to `SystemTime` at construction from the same `now_fn` the
    /// state manager uses (`SystemTime::now` in prod, `MockInstantWrapped` in
    /// tests). Used ONLY to timestamp arrivals in `inner_add_action` so the
    /// dim-B window is driven by the mockable clock, not the wall clock.
    affinity_clock: AffinityClock,

    /// (#batch-affinity, dimension A / #sched-affinity-probe) Gate for the
    /// dimension-A pending-set surplus pass. OPT-IN: `true` only when the operator
    /// set `spec.pending_affinity_probe_enabled` (default FALSE — the quadratic
    /// obs-only probe is opt-in) AND the backend is non-Redis (on Redis each
    /// `as_action_info()` is a store round-trip, so up to
    /// `MAX_PENDING_AFFINITY_SAMPLE` sequential GETs on the match-cycle critical
    /// path — force-OFF there regardless of the flag). Derived from
    /// `spec.pending_affinity_probe_enabled` && `spec.experimental_backend` at
    /// construction; does NOT change assignment.
    pending_affinity_probe_enabled: bool,

    /// (#specprefetch) Feature gate. When `true`, `do_try_match` emits
    /// `PrefetchInputs` to idle workers for still-queued actions once the
    /// backlog depth exceeds `speculative_prefetch_backlog_threshold`.
    enable_speculative_prefetch: bool,

    /// (#specprefetch) Minimum queue depth before speculative prefetch is
    /// triggered. Mirrors `SimpleSpec::speculative_prefetch_backlog_threshold`.
    speculative_prefetch_backlog_threshold: u64,

    /// (#specprefetch) TTL forwarded in the `PrefetchInputs` proto so the
    /// worker self-fires a cleanup timer. Mirrors `SimpleSpec::speculative_prefetch_ttl_s`.
    speculative_prefetch_ttl_s: u64,
}

impl core::fmt::Debug for SimpleScheduler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SimpleScheduler")
            .field("platform_property_manager", &self.platform_property_manager)
            .field("worker_scheduler", &self.worker_scheduler)
            .field("maybe_origin_event_tx", &self.maybe_origin_event_tx)
            .field(
                "task_worker_matching_spawn",
                &self.task_worker_matching_spawn,
            )
            .field(
                "task_hold_counters_log_spawn",
                &self.task_hold_counters_log_spawn,
            )
            .finish_non_exhaustive()
    }
}

impl SimpleScheduler {
    fn origin_event_id(event: &Event) -> String {
        Uuid::now_v6(&get_node_id(Some(event)))
            .hyphenated()
            .to_string()
    }

    fn scheduler_start_execute_event(
        worker_id: &WorkerId,
        operation_id: &OperationId,
        action_info: &ActionInfoWithProps,
    ) -> Event {
        let start_execute = StartExecute {
            execute_request: Some(action_info.inner.as_ref().into()),
            operation_id: operation_id.to_string(),
            queued_timestamp: Some(action_info.inner.insert_timestamp.into()),
            platform: Some((&action_info.platform_properties).into()),
            worker_id: worker_id.to_string(),
            // merge v1.6.1: our StartExecute proto carries extra fork fields
            // (resolved_directories, missing_digests, missing_digest_peers, ...);
            // the scheduler-start-execute telemetry event does not populate them.
            ..Default::default()
        };
        Event {
            event: Some(event::Event::Request(RequestEvent {
                event: Some(request_event::Event::SchedulerStartExecute(start_execute)),
            })),
        }
    }

    async fn publish_scheduler_start_execute(
        maybe_origin_event_tx: Option<&mpsc::Sender<OriginEvent>>,
        origin_metadata: &OriginMetadata,
        event_id: String,
        event: Event,
    ) {
        let Some(origin_event_tx) = maybe_origin_event_tx else {
            return;
        };

        let origin_event = OriginEvent {
            version: 0,
            event_id,
            parent_event_id: String::new(),
            bazel_request_metadata: origin_metadata.bazel_metadata.clone(),
            identity: origin_metadata.identity.clone(),
            event: Some(event),
        };

        // Awaited send (not try_send): backpressure rather than drop, so the
        // start-execute event that later resource-usage events reference as
        // their parent isn't silently lost when the queue is full.
        if let Err(err) = origin_event_tx.send(origin_event).await {
            warn!(
                ?err,
                "Failed to publish scheduler start execute origin event"
            );
        }
    }

    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this
    /// action with the results and state changes of the already running
    /// action. If the task cannot be executed immediately it will be queued
    /// for execution based on priority and other metrics.
    /// All further updates to the action will be provided through the returned
    /// value.
    async fn inner_add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        // (#batch-affinity, dimension B) OBSERVABILITY-ONLY: record this
        // arrival's input root and count it if a peer arrived within the
        // accumulation window. This does NOT delay or alter the add — it only
        // measures whether a hypothetical small accumulation delay would have
        // let this task batch with a peer for dir-cache affinity. The lock is
        // held only for the synchronous `record_arrival` (no `.await` inside).
        {
            // Timestamp via the injectable clock (mockable in tests) so the
            // dim-B window is deterministic, not wall-clock-dependent. Captured
            // before the lock so the clock closure is not called under it.
            let now = (self.affinity_clock)();
            let matched = self
                .recent_roots_window
                .lock()
                .record_arrival(action_info.input_root_digest, now);
            if matched {
                self.batch_affinity_metrics
                    .arrival_within_250ms_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        // (#p1p2) Capture the input root before `action_info` is moved into
        // `add_action` so we can prefetch its tree ahead of match. `DigestInfo`
        // is `Copy`, so this is free.
        let input_root_digest = action_info.input_root_digest;
        let action_state_result = self
            .client_state_manager
            .add_action(client_operation_id.clone(), action_info)
            .await
            .err_tip(|| "In SimpleScheduler::add_action")?;

        // (#p1p2) Fire-and-forget: kick off an ahead-of-time tree resolution
        // so this action's input tree is (usually) warm in the scheduler's
        // `tree_cache` by the time it reaches `find_and_reserve_worker`,
        // moving the ~59ms mean cold resolution + its 2s timeout tail OFF the
        // dispatch critical path. This is data-justified by the 2026-07-02
        // dual-benchmark (37% of cold resolves abandoned locality scoring at
        // the inline cap). `prefetch_input_tree` self-bounds via a semaphore
        // (CAPPED AT `TREE_PREFETCH_CONCURRENCY`) and spawns the CAS I/O onto
        // a background task — the inline portion is only a cheap cache peek +
        // a non-blocking permit try, so it does NOT delay the enqueue and
        // holds no lock across the spawn. Done AFTER the action is enqueued so
        // a failed add does not trigger a wasted prefetch.
        self.worker_scheduler
            .prefetch_input_tree(input_root_digest)
            .await;

        Ok(Box::new(SimpleSchedulerActionStateResult::new(
            client_operation_id.clone(),
            action_state_result,
        )))
    }

    async fn inner_filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'_>, Error> {
        self.client_state_manager
            .filter_operations(filter)
            .await
            .err_tip(|| "In SimpleScheduler::find_by_client_operation_id getting filter result")
    }

    async fn get_queued_operations(&self) -> Result<ActionStateResultStream<'_>, Error> {
        let filter = OperationFilter {
            stages: OperationStageFlags::Queued,
            order_by_priority_direction: Some(OrderDirection::Desc),
            ..Default::default()
        };
        self.matching_engine_state_manager
            .filter_operations(filter)
            .await
            .err_tip(|| "In SimpleScheduler::get_queued_operations getting filter result")
    }

    pub async fn do_try_match_for_test(&self) -> Result<(), Error> {
        self.do_try_match(true).await
    }

    /// #speculative-prefetch test hook: expose the concrete
    /// [`ApiWorkerScheduler`] so integration tests can drive
    /// `send_prefetch_inputs` (and the coalesce-guard reap) DETERMINISTICALLY,
    /// without arranging the hard-to-force backlog-more-than-idle-workers race
    /// (which made the prior emission tests vacuous — testing-czar C1/C2).
    #[must_use]
    pub fn worker_scheduler_for_test(&self) -> &Arc<ApiWorkerScheduler> {
        &self.worker_scheduler
    }

    // TODO(palfrey) This is an O(n*m) (aka n^2) algorithm. In theory we
    // can create a map of capabilities of each worker and then try and match
    // the actions to the worker using the map lookup (ie. map reduce).
    async fn do_try_match(&self, full_worker_logging: bool) -> Result<(), Error> {
        /// Maximum number of actions to process concurrently during matching.
        /// find_and_reserve_worker atomically finds AND reserves the worker
        /// (reducing platform properties and inserting into running_action_infos)
        /// under a single lock acquisition, so concurrent matches cannot
        /// select the same worker.
        ///
        /// Increased from 8 to 32 to reduce queue drain time during burst
        /// scheduling (e.g. build startup). With 10+ workers the higher
        /// concurrency prevents a backlog without meaningful lock contention
        /// since the worker registry write lock is held briefly per match.
        const MATCH_CONCURRENCY: usize = 32;

        // Cache for computed platform properties, keyed by sorted key-value
        // pairs. This avoids recomputing the same PlatformProperties for
        // actions that share identical platform requirements (the common case).
        let props_cache: std::sync::Mutex<
            HashMap<Vec<(String, String)>, Arc<PlatformProperties>>,
        > = std::sync::Mutex::new(HashMap::new());

        // Per-client match counter for fair scheduling. When
        // max_matches_per_client_per_cycle > 0, limits how many actions
        // from the same instance_name can be matched in one cycle,
        // preventing a single client from monopolizing all workers.
        let per_client_matches: std::sync::Mutex<HashMap<String, usize>> =
            std::sync::Mutex::new(HashMap::new());
        let max_per_client = self.max_matches_per_client_per_cycle;

        let start = Instant::now();

        let stream = self
            .get_queued_operations()
            .await
            .err_tip(|| "Failed to get queued operations in do_try_match")?;

        let query_elapsed = start.elapsed();
        if query_elapsed > Duration::from_secs(1) {
            warn!(
                elapsed_ms = query_elapsed.as_millis(),
                "Slow get_queued_operations query"
            );
        }

        // Collect all queued actions so we own them, then process up to
        // MATCH_CONCURRENCY concurrently using FuturesUnordered. Each action
        // independently finds a worker and assigns itself; conflicts are
        // resolved by the existing error handling (Aborted codes, None from
        // find_worker, etc.).
        let queued_actions: Vec<Box<dyn ActionStateResult>> = stream.collect().await;
        // #speculative-prefetch (perf MINOR-1): remember the pre-match queue
        // depth (already owned, free) so the backlog trigger below can
        // SHORT-CIRCUIT — skipping its second get_queued_operations query + the
        // per-op store GETs entirely when the queue was already below the
        // threshold. Without this, every do_try_match cycle paid the re-query.
        let primary_queue_depth = queued_actions.len() as u64;

        // (#batch-affinity, dimension A / #sched-affinity-probe) OBSERVABILITY-ONLY
        // and OPT-IN (default OFF): over the current pending set, measure the
        // co-location surplus a batch assignment could exploit that greedy
        // one-at-a-time misses. This reuses the already-collected `queued_actions`
        // (by reference — it is NOT consumed here; matching below still owns every
        // op) and samples only the first MAX_PENDING_AFFINITY_SAMPLE highest-priority
        // ops. It does NOT change which worker is chosen or introduce any delay — it
        // only records gauges.
        //
        // Gated on `pending_affinity_probe_enabled`, which is FALSE by default (the
        // probe's dominant kernel `compute_batch_sched_gain` is quadratic — 24.6s at
        // the 512 sample cap on a warm fleet, the observed 20-27s match-cycle
        // collapse — and nothing auto-consumes its gauges, so it must not run
        // always-on in prod; an investigation opts in via config). It is
        // additionally force-OFF on the Redis/store backend (each `as_action_info()`
        // would be a store GET) — see `pending_affinity_probe_enabled`.
        if self.pending_affinity_probe_enabled {
            self.record_pending_affinity_surplus(&queued_actions).await;
        }

        let mut futures_set = futures::stream::FuturesUnordered::<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>>,
        >::new();
        let mut action_iter = queued_actions.into_iter();
        let mut result = Ok(());

        // Seed the initial batch.
        for action_state_result in action_iter.by_ref().take(MATCH_CONCURRENCY) {
            futures_set.push(Box::pin(Self::match_action_to_worker_cached(
                action_state_result,
                self.worker_scheduler.as_ref(),
                self.matching_engine_state_manager.as_ref(),
                self.platform_property_manager.as_ref(),
                &props_cache,
                &per_client_matches,
                max_per_client,
                self.maybe_origin_event_tx.as_ref(),
                full_worker_logging,
            )));
        }

        // Process futures as they complete, adding new ones to maintain concurrency.
        while let Some(match_result) = futures_set.next().await {
            result = result.merge(match_result);

            if let Some(action_state_result) = action_iter.next() {
                futures_set.push(Box::pin(Self::match_action_to_worker_cached(
                    action_state_result,
                    self.worker_scheduler.as_ref(),
                    self.matching_engine_state_manager.as_ref(),
                    self.platform_property_manager.as_ref(),
                    &props_cache,
                    &per_client_matches,
                    max_per_client,
                    self.maybe_origin_event_tx.as_ref(),
                    full_worker_logging,
                )));
            }
        }

        let total_elapsed = start.elapsed();
        if total_elapsed > Duration::from_secs(5) {
            warn!(
                total_ms = total_elapsed.as_millis(),
                query_ms = query_elapsed.as_millis(),
                "Slow do_try_match cycle"
            );
        }

        // (#specprefetch, #specprefetch-rebind §2.1) Speculative prefetch backlog
        // trigger.
        //
        // After the normal match cycle, if the feature is enabled and the still-
        // queued depth is at or above the threshold, emit `PrefetchInputs` (tag-15)
        // to the PREDICTED (capability-matched, best-locality) worker for each
        // top-priority still-queued action — capacity-agnostic, so a BUSY worker
        // is a valid target (that is the whole point: a backlog means workers are
        // busy; the old idle-only target NEVER fired — design §0). We re-query
        // rather than carrying a "not-matched" list because actions may have been
        // matched by OTHER concurrent `do_try_match` callers and we want the
        // freshest view of what is still truly queued.
        //
        // Per-op dedup: `send_prefetch_inputs` coalesces if a prefetch was already
        // emitted for this op (G5 fan-out=1). Per-worker PLACEMENT: a same-input
        // fan-out is spread across the top-K locality holders by the per-cycle
        // per-worker cap (`PREFETCH_PER_WORKER_PER_CYCLE_CAP`, §2.4), threaded via
        // `per_cycle_prefetch_targets` below. The missing_digest_peers field is
        // empty here; the worker's `WorkerProxyStore` initiates P2P pulls when
        // StartAction arrives with peer hints. No per-RPC timeout: worker has its
        // own self-fired TTL (min(ttl_s, 120)s). Gate: feature flag OFF = no-op
        // (byte-identical to pre-feature behavior).
        if self.enable_speculative_prefetch
            && primary_queue_depth >= self.speculative_prefetch_backlog_threshold
        {
            // Re-query for the FRESHEST still-queued view (other concurrent
            // do_try_match callers may have matched some) ONLY now that the
            // pre-match depth already cleared the threshold — this bounds the
            // extra query + per-op GETs to the backlog regime the feature
            // targets, instead of every cycle (perf MINOR-1 / code-review S1).
            if let Ok(stream) = self.get_queued_operations().await {
                let still_queued: Vec<Box<dyn ActionStateResult>> = stream.collect().await;
                let queue_depth = still_queued.len() as u64;
                if queue_depth >= self.speculative_prefetch_backlog_threshold {
                    // (#specprefetch-rebind §2.4) Per-cycle per-worker prefetch
                    // counter. A same-input fan-out makes the capacity-agnostic
                    // selector return the SAME best-locality worker for every
                    // queued op; this map lets `send_prefetch_inputs` cap emits
                    // per worker (PREFETCH_PER_WORKER_PER_CYCLE_CAP) so prewarms
                    // SPREAD across the top-K locality holders instead of piling
                    // on one. Scoped to THIS do_try_match cycle (dropped at the
                    // end of the block).
                    // CAPPED AT candidates.len(): one entry per worker that
                    // received a prefetch this cycle, bounded by fleet size;
                    // dropped at end of cycle.
                    let mut per_cycle_prefetch_targets: HashMap<WorkerId, usize> = HashMap::new();
                    // (#specprefetch-rebind) Consume `send_prefetch_inputs`'s
                    // return so a delivered emit is not a SILENT drop (MAJOR-1:
                    // the boondoggle hid behind a discarded bool). The reason-
                    // aware counters (`speculative_prefetch_emitted` /
                    // `_no_target` / `_coalesce_suppressed`) live inside the fn —
                    // the bool alone cannot distinguish no-target from coalesced —
                    // so this per-cycle tally is a greppable dev summary; the
                    // process-singleton counters carry the prod-visible signal.
                    let mut delivered_this_cycle = 0usize;
                    let mut considered_this_cycle = 0usize;
                    for action_state_result in still_queued
                        .into_iter()
                        .take(self.speculative_prefetch_backlog_threshold as usize)
                    {
                        if let Ok((action_info, _)) = action_state_result.as_action_info().await {
                            // Build platform properties for candidate selection.
                            let mut cache_key: Vec<(String, String)> =
                                action_info.platform_properties.clone().into_iter().collect();
                            cache_key.sort();
                            let platform_properties = match {
                                let c = props_cache.lock()
                                    .unwrap_or_else(|e| e.into_inner());
                                c.get(&cache_key).cloned()
                            } {
                                Some(pp) => pp,
                                None => {
                                    match self.platform_property_manager
                                        .make_platform_properties(action_info.platform_properties.clone())
                                    {
                                        Ok(pp) => {
                                            let pp = Arc::new(pp);
                                            props_cache.lock()
                                                .unwrap_or_else(|e| e.into_inner())
                                                .insert(cache_key, Arc::clone(&pp));
                                            pp
                                        }
                                        Err(_) => continue,
                                    }
                                }
                            };
                            // Retrieve the matching engine's client operation_id (the prefetch
                            // coalesce-guard + reserve key).
                            let operation_id = match action_state_result.as_state().await {
                                Ok((action_state, _)) => action_state.client_operation_id.clone(),
                                Err(_) => continue,
                            };
                            considered_this_cycle += 1;
                            if self.worker_scheduler.send_prefetch_inputs(
                                &platform_properties,
                                &operation_id,
                                action_info.input_root_digest,
                                vec![],
                                self.speculative_prefetch_ttl_s,
                                &mut per_cycle_prefetch_targets,
                            ).await {
                                delivered_this_cycle += 1;
                            }
                        }
                    }
                    // (#specprefetch-rebind) Per-cycle emit summary — makes a
                    // backlog cycle that considered ops but delivered ZERO
                    // prefetches visible in dev logs (the silent-no-op the
                    // process counters also catch). `queue_depth` is the
                    // re-queried still-queued count that cleared the threshold.
                    debug!(
                        considered = considered_this_cycle,
                        delivered = delivered_this_cycle,
                        queue_depth,
                        "speculative prefetch backlog cycle emit summary",
                    );
                }
            }
        }

        result
    }

    /// (#batch-affinity / #batch-sched) OBSERVABILITY-ONLY: over the
    /// (already-collected, priority-sorted) pending set, store (a) the SUPERSEDED
    /// exact-input-root reference gauges and (b) the HEADLINE batch-scheduling
    /// counterfactual (`batch_sched_*`). Samples at most
    /// `MAX_PENDING_AFFINITY_SAMPLE` highest-priority ops; each sampled op costs
    /// one `as_action_info()` call — on the memory backend (the deployed
    /// topology, the only one this method runs on; the caller gates it OFF for
    /// the Redis/store backend) a cheap in-memory `watch::borrow().clone()`, the
    /// same KIND of call the matcher makes, but a SEPARATE bounded pass. Ops whose
    /// `as_action_info()` fails are skipped — a best-effort probe must never fail
    /// the match cycle.
    ///
    /// The batch counterfactual (`worker_scheduler.batch_sched_gain_for_probe`)
    /// operates ONLY over sampled roots whose `ResolvedTree` is ALREADY cached
    /// (a `tree_cache` peek — NO resolution, NO fetch, NO LRU bump); uncached
    /// roots are skipped and counted (`batch_sched_uncached_skipped`). It reads
    /// worker caches + capacities under the scheduler lock, then solves lock-free.
    ///
    /// This method has NO effect on assignment: it borrows `queued_actions`,
    /// mutates only the gauge atomics, and returns `()`.
    async fn record_pending_affinity_surplus(
        &self,
        queued_actions: &[Box<dyn ActionStateResult>],
    ) {
        let mut pending_roots: Vec<DigestInfo> =
            Vec::with_capacity(queued_actions.len().min(MAX_PENDING_AFFINITY_SAMPLE));
        for action_state_result in queued_actions.iter().take(MAX_PENDING_AFFINITY_SAMPLE) {
            if let Ok((action_info, _)) = action_state_result.as_action_info().await {
                pending_roots.push(action_info.input_root_digest);
            }
        }

        // (SUPERSEDED reference) exact-input-root surplus/max-group.
        let (surplus, max_group) = colocation_surplus(&pending_roots);
        self.batch_affinity_metrics
            .colocation_surplus_exact_root_reference
            .store(surplus, Ordering::Relaxed);
        self.batch_affinity_metrics
            .max_group_exact_root_reference
            .store(max_group, Ordering::Relaxed);
        self.batch_affinity_metrics
            .sampled_ops
            .store(pending_roots.len() as u64, Ordering::Relaxed);

        // (#batch-sched) HEADLINE counterfactual over the sampled roots whose
        // resolved trees are ALREADY cached (peek-only; the probe never
        // resolves). Reads worker cache/capacity under the scheduler lock, then
        // solves lock-free. `uncached_skipped` = sampled − scored.
        let (gain, uncached_skipped) = self
            .worker_scheduler
            .batch_sched_gain_for_probe(&pending_roots)
            .await;
        self.batch_affinity_metrics
            .batch_sched_gain_pct
            .store(gain.gain_pct, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_subtree_overlap_pct
            .store(gain.subtree_overlap_pct, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_sample_actions
            .store(gain.sample_actions, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_sample_workers
            .store(gain.sample_workers, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_uncached_skipped
            .store(uncached_skipped, Ordering::Relaxed);
        // (M1-replay) The greedy baseline + the two regime-diagnostic gauges, so
        // a scrape can tell the contended band from the all-lifted regime when
        // reading gain_pct.
        self.batch_affinity_metrics
            .batch_sched_greedy_score
            .store(gain.greedy_score, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_gate_active_frac
            .store(gain.gate_active_frac, Ordering::Relaxed);
        self.batch_affinity_metrics
            .batch_sched_mean_seed_running
            .store(gain.mean_seed_running, Ordering::Relaxed);
        // (#batch-sched) Count cycles where the from-scratch heuristic
        // underperformed greedy (B floored to G). CUMULATIVE — fetch_add, not
        // store — so operators see how OFTEN gain_pct is a loose lower bound.
        if gain.greedy_fallback {
            self.batch_affinity_metrics
                .batch_sched_greedy_fallback_total
                .fetch_add(1, Ordering::Relaxed);
        }

        // (#output-locality-probe) HEADLINE output-affinity opportunity over the
        // SAME sampled roots (reusing `pending_roots`), against the bounded
        // output→producer map + the still-connected worker set. Peek-only tree
        // reads + one map/worker snapshot, then a pure solve — no resolution, no
        // routing effect. SIBLING to the batch-sched block above.
        let (out_gain, out_map_size) = self
            .worker_scheduler
            .output_affinity_for_probe(&pending_roots)
            .await;
        self.output_affinity_metrics
            .match_frac
            .store(out_gain.match_frac, Ordering::Relaxed);
        self.output_affinity_metrics
            .matched_bytes
            .store(out_gain.matched_bytes, Ordering::Relaxed);
        self.output_affinity_metrics
            .distinct_producers
            .store(out_gain.distinct_producers, Ordering::Relaxed);
        self.output_affinity_metrics
            .sample_actions
            .store(out_gain.sample_actions, Ordering::Relaxed);
        self.output_affinity_metrics
            .map_size
            .store(out_map_size, Ordering::Relaxed);

        // (#output-locality-probe / file-level) FILE-level output-affinity over the
        // SAME sampled roots: input file_digests ∩ recently-produced output files
        // for a still-connected producer. matched_bytes (sum of matched file
        // SIZES) is the HEADLINE; largest_contributor_bytes flags a ubiquitous
        // single file. Peek-only + one map/worker snapshot + pure solve. SIBLING to
        // the directory-level and batch-sched blocks above.
        let (file_gain, file_map_size) = self
            .worker_scheduler
            .output_file_affinity_for_probe(&pending_roots)
            .await;
        self.output_file_affinity_metrics
            .match_frac
            .store(file_gain.match_frac, Ordering::Relaxed);
        self.output_file_affinity_metrics
            .matched_bytes
            .store(file_gain.matched_bytes, Ordering::Relaxed);
        self.output_file_affinity_metrics
            .distinct_producers
            .store(file_gain.distinct_producers, Ordering::Relaxed);
        self.output_file_affinity_metrics
            .sample_actions
            .store(file_gain.sample_actions, Ordering::Relaxed);
        self.output_file_affinity_metrics
            .map_size
            .store(file_map_size, Ordering::Relaxed);
        self.output_file_affinity_metrics
            .largest_contributor_bytes
            .store(file_gain.largest_contributor_bytes, Ordering::Relaxed);
    }

    /// Matches a single action to a worker, using a shared cache for computed
    /// platform properties to avoid redundant recomputation across actions
    /// with identical platform requirements.
    ///
    /// When `max_per_client > 0`, enforces fair scheduling by limiting how
    /// many actions from the same `instance_name` can be matched per cycle.
    /// Actions that exceed the limit are skipped (left in queue for next cycle).
    async fn match_action_to_worker_cached(
        action_state_result: Box<dyn ActionStateResult>,
        workers: &ApiWorkerScheduler,
        matching_engine_state_manager: &dyn MatchingEngineStateManager,
        platform_property_manager: &PlatformPropertyManager,
        props_cache: &std::sync::Mutex<
            HashMap<Vec<(String, String)>, Arc<PlatformProperties>>,
        >,
        per_client_matches: &std::sync::Mutex<HashMap<String, usize>>,
        max_per_client: usize,
        maybe_origin_event_tx: Option<&mpsc::Sender<OriginEvent>>,
        full_worker_logging: bool,
    ) -> Result<(), Error> {
        let (action_info, maybe_origin_metadata) = action_state_result
            .as_action_info()
            .await
            .err_tip(|| "Failed to get action_info from as_action_info_result stream")?;

        // Fair scheduling: atomically check and optimistically increment the
        // per-client counter. If the client has hit its limit, skip the action.
        // If the match later fails, we decrement to undo the reservation.
        let client_name = action_info.instance_name().clone();
        let claimed_slot = if max_per_client > 0 {
            let mut map = per_client_matches.lock().unwrap_or_else(|e| e.into_inner());
            let count = map.entry(client_name.clone()).or_insert(0);
            if *count >= max_per_client {
                // Skip — action stays queued for next cycle.
                return Ok(());
            }
            *count += 1;
            true
        } else {
            false
        };

        // Helper to undo the optimistic increment on failure paths.
        let undo_claim = |per_client_matches: &std::sync::Mutex<HashMap<String, usize>>,
                          client_name: &str| {
            let mut map = per_client_matches.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(count) = map.get_mut(client_name) {
                *count = count.saturating_sub(1);
            }
        };

        // Build a deterministic cache key from the raw platform
        // properties (sorted key-value pairs).
        let mut cache_key: Vec<(String, String)> =
            action_info.platform_properties.clone().into_iter().collect();
        cache_key.sort();

        // Look up or compute and cache the platform properties.
        //
        // `make_platform_properties` returns `Code::InvalidArgument` when the
        // action declares a property the scheduler does not know about (i.e.
        // not in `supported_platform_properties`). That is **client input
        // error**, not scheduler-state corruption: bubbling it out of the
        // matcher would (a) fail this entire `do_try_match` cycle, (b)
        // increment `consecutive_match_errors` (intended for genuine state
        // damage), (c) trigger the misleading "scheduler data structure
        // corruption — restart may be required" alert after 10 cycles, and
        // (d) leave the offending action queued so it re-fires on every
        // poll. We instead reject the action back to its originating client
        // as a terminal `Code::FailedPrecondition` and continue the matching
        // loop. We deliberately re-tag the error as `FailedPrecondition` (not
        // `InvalidArgument`) because the scheduler's state-manager retry gate
        // already routes `FailedPrecondition` errors through the
        // `missing_inputs` terminal-no-retry branch (see
        // `simple_scheduler_state_manager.rs:819`). Routing through the
        // existing terminal branch avoids a parallel "is_input_validation"
        // gate that would also catch genuine corruption-class
        // `Code::InvalidArgument` errors (e.g. `awaited_action_decode` serde
        // failures, `ClientIdToOperationId::decode` failures) which MUST
        // continue to bump `consecutive_match_errors` and the corruption
        // alert. Cached entries are pre-validated, so this rejection only
        // fires the first time a unique cache_key is seen.
        let platform_properties = {
            let cached = {
                let cache = props_cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.get(&cache_key).cloned()
            };
            if let Some(cached) = cached {
                cached
            } else {
                match platform_property_manager
                    .make_platform_properties(action_info.platform_properties.clone())
                {
                    Ok(computed) => {
                        let arc = Arc::new(computed);
                        let mut cache =
                            props_cache.lock().unwrap_or_else(|e| e.into_inner());
                        cache.insert(cache_key, arc.clone());
                        arc
                    }
                    Err(err) if err.code == Code::InvalidArgument => {
                        // Reject to client. Need operation_id to do so.
                        let operation_id = match action_state_result.as_state().await {
                            Ok((action_state, _)) => action_state.client_operation_id.clone(),
                            Err(state_err) => {
                                // Couldn't even get the operation_id — undo
                                // the per-client claim and surface the state
                                // lookup error (this is a real failure, not
                                // input validation).
                                if claimed_slot {
                                    undo_claim(per_client_matches, &client_name);
                                }
                                return Err(state_err.append(
                                    "Failed to get state for unknown-platform-property \
                                     rejection in SimpleScheduler::do_try_match",
                                ));
                            }
                        };
                        warn!(
                            %operation_id,
                            properties = ?action_info.platform_properties,
                            ?err,
                            "rejecting action to client: invalid platform property \
                             (unknown name, malformed minimum value, or other \
                             input-validation failure from make_platform_properties)"
                        );
                        // Re-tag as `FailedPrecondition` so the
                        // state-manager's existing `missing_inputs` retry
                        // gate (`simple_scheduler_state_manager.rs:819`)
                        // routes the rejection straight to terminal
                        // `Completed` without re-queueing. Keeping the
                        // original `Code::InvalidArgument` would slip past
                        // that gate and silently cap-out the retry counter
                        // before reporting failure (and would conflict with
                        // genuine corruption-class `InvalidArgument` errors
                        // like `awaited_action_decode` serde failures, which
                        // MUST continue to retry/alert). Tagged
                        // `FailedPrecondition` is also REAPI-non-retryable
                        // and semantically accurate ("scheduler does not
                        // declare this platform property as a precondition").
                        let reject_err = make_err!(
                            Code::FailedPrecondition,
                            "rejecting action: scheduler does not declare this \
                             platform property in supported_platform_properties: \
                             {err:?}"
                        );
                        if let Err(assign_err) = matching_engine_state_manager
                            .assign_operation(&operation_id, Err(reject_err))
                            .await
                        {
                            // Couldn't record the rejection. Undo the
                            // per-client claim and propagate. Aborted is
                            // benign (lost a version-conflict race); other
                            // codes are real.
                            if claimed_slot {
                                undo_claim(per_client_matches, &client_name);
                            }
                            if assign_err.code == Code::Aborted {
                                return Ok(());
                            }
                            return Err(assign_err.append(
                                "Failed to record action rejection in \
                                 SimpleScheduler::do_try_match",
                            ));
                        }
                        // Successfully rejected. Undo the per-client claim
                        // (rejected actions don't consume a worker slot) and
                        // continue the matching loop.
                        if claimed_slot {
                            undo_claim(per_client_matches, &client_name);
                        }
                        return Ok(());
                    }
                    Err(err) => return Err(err.append(
                        "Failed to make platform properties in SimpleScheduler::do_try_match",
                    )),
                }
            }
        };

        let action_info_with_props = ActionInfoWithProps {
            inner: action_info,
            platform_properties: (*platform_properties).clone(),
            // merge v1.6.1: populate origin_metadata so the resource-usage origin
            // event (ApiWorkerScheduler::record_action_resource_usage, which reads
            // this back from running_action_infos) carries the action's identity.
            // scheduler_start_execute_event_id stays None on this concurrent-reserve
            // matching path — the start-execute origin event is not emitted here
            // (worker_id is only known post-reserve, but the recorded copy would
            // need the event_id pre-reserve). See deferred_tasks.md.
            origin_metadata: maybe_origin_metadata.clone().unwrap_or_default(),
            scheduler_start_execute_event_id: None,
        };

        // Extract the operation_id from the action_state BEFORE finding a
        // worker, so we can pass it to find_and_reserve_worker for atomic
        // reservation.
        let operation_id = {
            let (action_state, _origin_metadata) = action_state_result
                .as_state()
                .await
                .err_tip(|| "Failed to get action_info from as_state_result stream")?;
            action_state.client_operation_id.clone()
        };

        // Atomically find a worker AND reserve it for this operation.
        // The worker's platform properties are reduced and the action is
        // recorded in running_action_infos under a single lock acquisition,
        // preventing concurrent matches from selecting the same worker.
        let (worker_id, tx, msg) = match workers
            .find_and_reserve_worker(
                &action_info_with_props.platform_properties,
                &operation_id,
                &action_info_with_props,
                full_worker_logging,
            )
            .await
        {
            Some(result) => result,
            // No worker found — undo the optimistic increment.
            None => {
                if claimed_slot {
                    undo_claim(per_client_matches, &client_name);
                }
                return Ok(());
            }
        };

        // Tell the matching engine that the operation is being assigned to a worker.
        let assign_result = matching_engine_state_manager
            .assign_operation(&operation_id, Ok(&worker_id))
            .await
            .err_tip(|| "Failed to assign operation in do_try_match");
        if let Err(err) = assign_result {
            // Undo the worker reservation since the assignment failed.
            workers.unreserve_worker(&worker_id, &operation_id).await;
            if claimed_slot {
                undo_claim(per_client_matches, &client_name);
            }
            if err.code == Code::Aborted {
                // The operation was cancelled due to another operation
                // being assigned to the worker.
                return Ok(());
            }
            // Any other error is a real error.
            return Err(err);
        }

        let origin_metadata = maybe_origin_metadata.unwrap_or_default();
        let ctx = Context::current_with_baggage(vec![KeyValue::new(
            ENDUSER_ID,
            origin_metadata.identity,
        )]);

        let notify_fut = async {
            debug!(
                %worker_id,
                %operation_id,
                ?action_info_with_props,
                "Notifying worker of operation"
            );
            workers
                .send_reserved_worker_notification(&worker_id, tx, msg)
                .await
                .err_tip(|| {
                    "Failed to send_reserved_worker_notification in SimpleScheduler::do_try_match"
                })
        };

        let notify_result = info_span!("do_try_match")
            .in_scope(|| notify_fut)
            .with_context(ctx)
            .await;

        // merge v1.6.1: publish the scheduler start-execute origin event for this
        // assignment (upstream #2398/#2413) once the worker has been notified, and
        // stamp its event id back onto the reserved running action so a later
        // resource-usage origin event links to it as `parent_event_id`. On our
        // concurrent-reserve matching path the worker_id (hence the event id) is
        // only known AFTER `find_and_reserve_worker` already recorded the
        // action_info, so `set_scheduler_start_execute_event_id` performs the
        // post-reserve linkage that upstream's inline matcher did inline.
        // Inert when origin events are disabled (production default → `None`).
        if notify_result.is_ok() {
            if let Some(origin_event_tx) = maybe_origin_event_tx {
                let event = Self::scheduler_start_execute_event(
                    &worker_id,
                    &operation_id,
                    &action_info_with_props,
                );
                let event_id = Self::origin_event_id(&event);
                workers
                    .set_scheduler_start_execute_event_id(
                        &worker_id,
                        &operation_id,
                        event_id.clone(),
                    )
                    .await;
                Self::publish_scheduler_start_execute(
                    Some(origin_event_tx),
                    &action_info_with_props.origin_metadata,
                    event_id,
                    event,
                )
                .await;
            }
        }

        notify_result
    }
}

impl SimpleScheduler {
    pub fn new<A: AwaitedActionDb>(
        spec: &SimpleSpec,
        awaited_action_db: A,
        task_change_notify: Arc<Notify>,
        maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        Self::new_with_cas_store(
            spec,
            awaited_action_db,
            task_change_notify,
            maybe_origin_event_tx,
            None,
            None,
            None,
        )
    }

    pub fn new_with_cas_store<A: AwaitedActionDb>(
        spec: &SimpleSpec,
        awaited_action_db: A,
        task_change_notify: Arc<Notify>,
        maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,
        cas_store: Option<nativelink_util::store_trait::Store>,
        locality_map: Option<nativelink_util::blob_locality_map::SharedBlobLocalityMap>,
        worker_tls_config: Option<ClientTlsConfig>,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        Self::new_with_callback(
            spec,
            awaited_action_db,
            || {
                // Yield to allow other tasks to make progress between match
                // cycles. A full 1ms sleep is too aggressive and caps matching
                // to ~1000 cycles/sec. sleep(ZERO) defers to the next timer
                // tick, preventing busy-spinning when no other tasks are
                // runnable (unlike yield_now which returns immediately).
                tokio::time::sleep(Duration::ZERO)
            },
            task_change_notify,
            SystemTime::now,
            maybe_origin_event_tx,
            cas_store,
            locality_map,
            worker_tls_config,
        )
    }

    pub fn new_with_callback<
        Fut: Future<Output = ()> + Send,
        F: Fn() -> Fut + Send + Sync + 'static,
        A: AwaitedActionDb,
        I: InstantWrapper,
        NowFn: Fn() -> I + Clone + Send + Unpin + Sync + 'static,
    >(
        spec: &SimpleSpec,
        awaited_action_db: A,
        on_matching_engine_run: F,
        task_change_notify: Arc<Notify>,
        now_fn: NowFn,
        maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,
        cas_store: Option<nativelink_util::store_trait::Store>,
        locality_map: Option<nativelink_util::blob_locality_map::SharedBlobLocalityMap>,
        worker_tls_config: Option<ClientTlsConfig>,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        let platform_property_manager = Arc::new(PlatformPropertyManager::new(
            spec.supported_platform_properties
                .clone()
                .unwrap_or_default(),
        ));

        // (#batch-affinity) Capture the injectable clock (type-erased to
        // `SystemTime`) BEFORE `now_fn` is moved into the state manager below.
        // `now_fn` is `Clone`; `I::now()` yields `SystemTime` in prod
        // (`SystemTime::now`) and mock-clock time in tests (`MockInstantWrapped`).
        let affinity_clock: AffinityClock = {
            let now_fn = now_fn.clone();
            Arc::new(move || now_fn().now())
        };
        // (#batch-affinity, dim A / #sched-affinity-probe) Gate the pending-set
        // surplus pass. Requires BOTH: (1) the explicit opt-in flag
        // `spec.pending_affinity_probe_enabled` (default FALSE — see
        // `default_pending_affinity_probe_enabled`; the quadratic obs-only probe is
        // opt-in, an investigation sets it true) AND (2) a non-Redis backend (on
        // Redis each sampled `as_action_info()` is a store round-trip on the
        // match-cycle critical path, so the probe is force-OFF there regardless of
        // the flag). Observability only — does not affect assignment.
        let pending_affinity_probe_enabled = spec.pending_affinity_probe_enabled
            && !matches!(
                spec.experimental_backend,
                Some(nativelink_config::schedulers::ExperimentalSimpleSchedulerBackend::Redis(_))
            );

        let mut worker_timeout_s = spec.worker_timeout_s;
        if worker_timeout_s == 0 {
            worker_timeout_s = DEFAULT_WORKER_TIMEOUT_S;
        }

        let mut client_action_timeout_s = spec.client_action_timeout_s;
        if client_action_timeout_s == 0 {
            client_action_timeout_s = DEFAULT_CLIENT_ACTION_TIMEOUT_S;
        }
        // This matches the value of CLIENT_KEEPALIVE_DURATION which means that
        // tasks are going to be dropped all over the place, this isn't a good
        // setting.
        if client_action_timeout_s <= CLIENT_KEEPALIVE_DURATION.as_secs() {
            error!(
                client_action_timeout_s,
                "Setting client_action_timeout_s to less than the client keep alive interval is going to cause issues, please set above {}.",
                CLIENT_KEEPALIVE_DURATION.as_secs()
            );
        }

        let mut max_job_retries = spec.max_job_retries;
        if max_job_retries == 0 {
            max_job_retries = DEFAULT_MAX_JOB_RETRIES;
        }

        let worker_change_notify = Arc::new(Notify::new());

        // Create shared worker registry for single heartbeat per worker.
        let worker_registry = Arc::new(WorkerRegistry::new());

        // (#dag-criticality) Create the shared DAG state (kill-switch) BEFORE the awaited-
        // action-db is moved into the state manager, and inject it so the db folds the
        // published criticality snapshot into the sort key at enqueue. The SAME `Arc` is
        // also handed to the worker scheduler below (producer/consumer/duration recording +
        // the background recompute that publishes the snapshot). Off => `None` everywhere
        // (no accumulation; sort key byte-identical to the pre-feature key).
        let dag_state: Option<Arc<DagState>> = if spec.dag_critical_path_enabled {
            Some(Arc::new(DagState::new()))
        } else {
            None
        };
        awaited_action_db.set_dag_criticality(dag_state.clone());

        let state_manager = SimpleSchedulerStateManager::new(
            max_job_retries,
            Duration::from_secs(worker_timeout_s),
            Duration::from_secs(client_action_timeout_s),
            Duration::from_secs(spec.max_action_executing_timeout_s),
            awaited_action_db,
            now_fn,
            Some(worker_registry.clone()),
        );

        let worker_scheduler = ApiWorkerScheduler::new_with_locality_map(
            state_manager.clone(),
            platform_property_manager.clone(),
            spec.allocation_strategy,
            worker_change_notify.clone(),
            worker_timeout_s,
            worker_registry,
            locality_map,
            cas_store,
            worker_tls_config,
            // (#sched-blend) continuous cache-vs-load blend tunables.
            spec.load_byte_cost,
            spec.assume_core_count,
            // (#sched M1 rebalance) dispatch-count P-headroom overflow gate;
            // default OFF (byte-identical to the pre-gate matcher until an
            // operator enables it).
            spec.p_headroom_gate_enabled,
            // (#sched M1 rebalance v2) bounded p_load override tunables;
            // threshold default 0 = override OFF (exact v1), factor default 2.
            // Inert unless `p_headroom_gate_enabled` AND threshold > 0.
            spec.p_idle_threshold_pct,
            spec.p_headroom_override_factor,
            // (#p2p-prefetch) worker-driven P2P input prefetch shed; default
            // OFF (byte-identical to today — full server-push prefetch + empty
            // inline field until an operator enables it).
            spec.enable_p2p_input_prefetch,
            // (#specprefetch-rebind Stage B) temporal hold-vs-rebind gate;
            // default OFF (byte-identical assignment until an operator enables
            // it — the hold has a p99-regression risk Stage A does not).
            spec.enable_speculative_hold,
            maybe_origin_event_tx.clone(),
        );

        // (#specprefetch-rebind Stage C) Wire the SAME `now_fn`-derived clock the
        // state manager uses into the worker scheduler, so exec-start stamping
        // and the duration EWMA (Stage B's `T_wait_W` inputs) are driven by the
        // mockable clock (`SystemTime::now` in prod, `MockInstantWrapped` in
        // tests). `affinity_clock` is `Arc<dyn Fn() -> SystemTime + …>`,
        // structurally identical to `ExecClock`. Synchronous (uncontended
        // try_write on the freshly-built Arc) so this non-async constructor can
        // wire it inline. Without this call the scheduler would fall back to the
        // wall clock, which is correct in prod but not mockable in tests.
        worker_scheduler.set_exec_clock(affinity_clock.clone());

        // (#sched-decision-trace) Wire the diagnostic dispatch-decision-trace
        // switch from config (default false). Observability-only: an operator
        // turns it ON briefly to see, per candidate worker, which predicate is
        // holding queued actions off idle-looking workers, then OFF. Mirrors the
        // `set_exec_clock` one-shot wiring above.
        worker_scheduler.set_decision_trace_enabled(spec.scheduler_decision_trace_enabled);

        // (#sched-cpu-first §7) Wire the winner-ranking policy from config
        // (default `CacheAffinityFirst` = byte-identical to today) + the
        // synthetic-load pct-per-task for `CpuIdleFirst`. Mirrors the
        // `set_decision_trace_enabled` one-shot wiring above (uncontended
        // try_write on the freshly-built Arc). Selection-only.
        worker_scheduler
            .set_placement_mode(spec.placement_mode, spec.cpu_first_synthetic_pct_per_task);

        // (#task-resource-profile Phase-3) Wire the memory-prediction enforcement
        // gates from config (all default OFF / factor 1.0 = byte-identical declared-
        // only reservation ledger). Mirrors the `set_placement_mode` one-shot wiring
        // above (uncontended try_write on the freshly-built Arc). The observe metric
        // ships ON regardless of these gates.
        worker_scheduler.set_phase3_enforcement(
            spec.phase3_raise_enabled,
            spec.phase3_down_overcommit_enabled,
            spec.phase3_overcommit_max_factor,
            spec.resource_profile_persist_max_age_secs,
        );

        // (#task-resource-profile Phase-3 §12) Load the persisted resource-profile map
        // BEFORE the scheduler serves any action (this runs in the sync constructor,
        // before the returned scheduler is wired to accept work). A one-time bounded
        // startup read (std::fs) — NOT a hot/serving path, so it is not the
        // never-block-a-worker case. A missing / corrupt / version-mismatched file logs
        // a `warn` and starts FRESH (never panics). Loaded aggs are stamped with the
        // snapshot AGE so the DOWN direction can refuse a too-old snapshot (§12 staleness).
        if let Some(path) = spec.resource_profile_persist_path.as_deref() {
            match std::fs::read(path) {
                Ok(bytes) => match resource_profile_persist::deserialize_snapshot(&bytes) {
                    Ok((snapshot_unix_secs, entries)) => {
                        let now_unix = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let age_secs = now_unix.saturating_sub(snapshot_unix_secs);
                        let n = worker_scheduler.resource_profile_load_entries(entries, age_secs);
                        info!(
                            tag = "resource_profile_persist_load",
                            path,
                            loaded_entries = n,
                            snapshot_age_secs = age_secs,
                            "loaded persisted resource-profile snapshot"
                        );
                    }
                    Err(err) => warn!(
                        tag = "resource_profile_persist_load",
                        path,
                        %err,
                        "resource-profile snapshot corrupt / version-mismatch — starting fresh"
                    ),
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    info!(
                        tag = "resource_profile_persist_load",
                        path, "no prior resource-profile snapshot — starting fresh"
                    );
                }
                Err(err) => warn!(
                    tag = "resource_profile_persist_load",
                    path,
                    %err,
                    "resource-profile snapshot unreadable — starting fresh"
                ),
            }
        }

        // (#dag-criticality) Hand the SAME shared DAG state (created above, already injected
        // into the awaited-action-db) to the worker scheduler for producer/consumer/duration
        // recording on the completion + dispatch paths + the background recompute that
        // publishes the criticality snapshot. Mirrors `set_phase3_enforcement`.
        worker_scheduler.set_dag_state(dag_state.clone());

        // (#dag-criticality) Load the persisted edge store BEFORE serving — a SELF-CONTAINED
        // sibling file (own "NLDG" versioned header), NOT the resource-profile snapshot. A
        // one-time bounded startup read (std::fs, not a serving path). Missing / corrupt /
        // version-mismatch => `warn` + start fresh (never panics). An initial `recompute`
        // publishes a snapshot from the loaded edges so criticality is live immediately.
        if let (Some(dag_state), Some(path)) = (
            dag_state.as_ref(),
            spec.dag_edge_store_persist_path.as_deref(),
        ) {
            match std::fs::read(path) {
                Ok(bytes) => match dag_criticality::deserialize_dag_snapshot(&bytes) {
                    Ok((_snapshot_unix_secs, data)) => {
                        let (loaded_edges, loaded_nodes) = dag_state.load_persist(data);
                        dag_state.recompute();
                        info!(
                            tag = "dag_edge_store_load",
                            path, loaded_edges, loaded_nodes, "loaded persisted DAG edge store"
                        );
                    }
                    Err(err) => warn!(
                        tag = "dag_edge_store_load",
                        path,
                        %err,
                        "DAG edge store corrupt / version-mismatch — starting fresh"
                    ),
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => info!(
                    tag = "dag_edge_store_load",
                    path, "no prior DAG edge store — starting fresh"
                ),
                Err(err) => warn!(
                    tag = "dag_edge_store_load",
                    path,
                    %err,
                    "DAG edge store unreadable — starting fresh"
                ),
            }
        }

        let worker_scheduler_clone = worker_scheduler.clone();

        let action_scheduler = Arc::new_cyclic(move |weak_self| -> Self {
            let weak_inner = weak_self.clone();
            let task_worker_matching_spawn =
                spawn!("simple_scheduler_task_worker_matching", async move {
                    let mut last_match_successful = true;
                    let mut worker_match_logging_last: Option<Instant> = None;
                    let mut last_stall_check: Option<Instant> = None;
                    let mut consecutive_match_errors: u32 = 0;
                    // Break out of the loop only when the inner is dropped.
                    loop {
                        let task_change_fut = task_change_notify.notified();
                        let worker_change_fut = worker_change_notify.notified();
                        tokio::pin!(task_change_fut);
                        tokio::pin!(worker_change_fut);
                        // Wait for either of these futures to be ready.
                        let state_changed = future::select(task_change_fut, worker_change_fut);
                        if last_match_successful {
                            let _ = state_changed.await;
                        } else {
                            // If the last match failed, then run again after a short sleep.
                            // This resolves issues where we tried to re-schedule a job to
                            // a disconnected worker.  The sleep ensures we don't enter a
                            // hard loop if there's something wrong inside do_try_match.
                            let sleep_fut = tokio::time::sleep(Duration::from_millis(100));
                            tokio::pin!(sleep_fut);
                            let _ = future::select(state_changed, sleep_fut).await;
                        }

                        let result = match weak_inner.upgrade() {
                            Some(scheduler) => {
                                let now = Instant::now();
                                let full_worker_logging = {
                                    match scheduler.worker_match_logging_interval {
                                        None => false,
                                        Some(duration) => match worker_match_logging_last {
                                            None => true,
                                            Some(when) => now.duration_since(when) >= duration,
                                        },
                                    }
                                };

                                let res = scheduler.do_try_match(full_worker_logging).await;
                                if full_worker_logging {
                                    let operations_stream = scheduler
                                        .matching_engine_state_manager
                                        .filter_operations(OperationFilter::default())
                                        .await
                                        .err_tip(|| "In action_scheduler getting filter result");

                                    let mut oldest_actions_in_state: HashMap<
                                        String,
                                        BTreeSet<Arc<ActionState>>,
                                    > = HashMap::new();
                                    let max_items = 5;

                                    match operations_stream {
                                        Ok(stream) => {
                                            let actions = stream
                                                .filter_map(|item| async move {
                                                    match item.as_ref().as_state().await {
                                                        Ok((action_state, _origin_metadata)) => {
                                                            Some(action_state)
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                ?e,
                                                                "Failed to get action state!"
                                                            );
                                                            None
                                                        }
                                                    }
                                                })
                                                .collect::<Vec<_>>()
                                                .await;
                                            for action_state in &actions {
                                                let name = action_state.stage.name();
                                                if let Some(values) =
                                                    oldest_actions_in_state.get_mut(&name)
                                                {
                                                    values.insert(action_state.clone());
                                                    if values.len() > max_items {
                                                        values.pop_first();
                                                    }
                                                } else {
                                                    let mut values = BTreeSet::new();
                                                    values.insert(action_state.clone());
                                                    oldest_actions_in_state.insert(name, values);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(?e, "Failed to get operations list!");
                                        }
                                    }

                                    for value in oldest_actions_in_state.values() {
                                        let mut items = vec![];
                                        for item in value {
                                            items.push(item.to_string());
                                        }
                                        info!(?items, "Oldest actions in state");
                                    }

                                    worker_match_logging_last.replace(now);
                                }

                                // Stall detection: every 30s, check for actions stuck
                                // in Queued state for >60s. Only fires as an error when
                                // no actions are executing (true deadlock). If workers are
                                // busy executing, queued stalls are just capacity limits.
                                let should_check_stalls = match last_stall_check {
                                    None => true,
                                    Some(when) => now.duration_since(when) >= Duration::from_secs(30),
                                };
                                if should_check_stalls {
                                    last_stall_check = Some(now);
                                    let stall_threshold = Duration::from_secs(60);
                                    match scheduler
                                        .matching_engine_state_manager
                                        .filter_operations(OperationFilter {
                                            stages: OperationStageFlags::Queued,
                                            order_by_priority_direction: Some(OrderDirection::Desc),
                                            ..Default::default()
                                        })
                                        .await
                                    {
                                        Ok(queued_stream) => {
                                            let queued_actions: Vec<_> = queued_stream.collect().await;
                                            let mut stalled_count: usize = 0;
                                            let mut unmatchable_count: usize = 0;
                                            let prop_manager = scheduler.worker_scheduler.get_platform_property_manager();
                                            for action_state_result in &queued_actions {
                                                if let Ok((state, _)) = action_state_result.as_state().await {
                                                    if let Ok(elapsed) = state.last_transition_timestamp.elapsed() {
                                                        if elapsed > stall_threshold {
                                                            stalled_count += 1;
                                                            // Check if any worker could ever match this action.
                                                            match action_state_result.as_action_info().await {
                                                                Ok((action_info, _)) => {
                                                                    match prop_manager.make_platform_properties(
                                                                        action_info.platform_properties.clone(),
                                                                    ) {
                                                                        Ok(props) => {
                                                                            if !scheduler.worker_scheduler.has_matching_workers(&props).await {
                                                                                error!(
                                                                                    operation_id = %state.client_operation_id,
                                                                                    action_digest = %state.action_digest,
                                                                                    properties = ?action_info.platform_properties,
                                                                                    "Action queued >60s with NO matching workers — \
                                                                                     no registered worker can satisfy its platform requirements"
                                                                                );
                                                                                unmatchable_count += 1;
                                                                            }
                                                                        }
                                                                        Err(e) => {
                                                                            warn!(
                                                                                operation_id = %state.client_operation_id,
                                                                                ?e,
                                                                                "Failed to parse platform properties for stalled action — cannot check matchability"
                                                                            );
                                                                        }
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    warn!(
                                                                        operation_id = %state.client_operation_id,
                                                                        ?e,
                                                                        "Failed to get action_info for stalled action — cannot check matchability"
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            let matchable_stalled = stalled_count - unmatchable_count;
                                            if matchable_stalled > 0 {
                                                // Check if workers are actively executing. If so,
                                                // the queue backlog is just capacity pressure.
                                                let executing_count = match scheduler
                                                    .matching_engine_state_manager
                                                    .filter_operations(OperationFilter {
                                                        stages: OperationStageFlags::Executing,
                                                        ..Default::default()
                                                    })
                                                    .await
                                                {
                                                    Ok(s) => s.count().await,
                                                    Err(e) => {
                                                        // Query failed — assume workers are busy
                                                        // rather than raising a false deadlock alarm.
                                                        warn!(?e, "Failed to query executing actions for stall check");
                                                        usize::MAX
                                                    }
                                                };

                                                if executing_count > 0 {
                                                    warn!(
                                                        stalled_count = matchable_stalled,
                                                        total_queued = queued_actions.len(),
                                                        executing_count,
                                                        unmatchable_count,
                                                        "Actions waiting in queue >60s (workers at capacity)"
                                                    );
                                                } else {
                                                    error!(
                                                        stalled_count = matchable_stalled,
                                                        total_queued = queued_actions.len(),
                                                        unmatchable_count,
                                                        "Actions stalled in Queued state >60s with NO executing actions (possible scheduling deadlock)"
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                ?e,
                                                "Failed to query queued actions for stall check — scheduler state may be corrupted"
                                            );
                                        }
                                    }
                                }

                                res
                            }
                            // If the inner went away it means the scheduler is shutting
                            // down, so we need to resolve our future.
                            None => return,
                        };
                        last_match_successful = result.is_ok();
                        if let Err(err) = &result {
                            // The platform-property class of input-validation
                            // failure is caught upstream inside
                            // `match_action_to_worker_cached` (re-tagged
                            // `Code::FailedPrecondition`, action terminally
                            // rejected via `assign_operation`, matcher returns
                            // `Ok`). Any error reaching THIS branch is from a
                            // genuinely scheduler-internal source (missing
                            // worker_id, awaited_action_decode serde failure,
                            // pollee panic, etc.) — exactly the corruption
                            // class the counter+restart-alert was designed
                            // for. Don't filter by code here; that would mask
                            // real corruption signals (CLAUDE.md "Fix root
                            // causes, not symptoms"; #416 audit).
                            consecutive_match_errors += 1;
                            if consecutive_match_errors >= 10 {
                                error!(
                                    consecutive_match_errors,
                                    ?err,
                                    "do_try_match failing consecutively — \
                                     possible scheduler data structure corruption. \
                                     A server restart may be required to recover.",
                                );
                            } else {
                                error!(?err, "Error while running do_try_match");
                            }
                        } else {
                            consecutive_match_errors = 0;
                        }

                        on_matching_engine_run().await;
                    }
                    // Unreachable.
                });

            // (#obs-tuning) OBSERVABILITY-ONLY periodic decision-counter log.
            // A SEPARATE interval-driven task (NOT folded into the match loop,
            // which is `select`-driven and would emit per-match — the
            // expensive-obs-probe-in-hot-loop class). Holds a
            // `Weak<ApiWorkerScheduler>` so it exits when the scheduler drops;
            // the `JoinHandleDropGuard` also cancels it on struct drop. Reads
            // the atomics + per-worker gossip `Relaxed`; NO scheduling effect.
            let weak_worker_scheduler = Arc::downgrade(&worker_scheduler);
            let task_hold_counters_log_spawn =
                spawn!("simple_scheduler_hold_counters_log", async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(
                        HOLD_COUNTERS_LOG_INTERVAL_S,
                    ));
                    // The first tick fires immediately; skip it so the first
                    // emit lands one full interval after startup (the counters
                    // are all zero at t=0 — no information).
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        let Some(worker_scheduler) = weak_worker_scheduler.upgrade() else {
                            // Scheduler dropped — nothing left to observe.
                            return;
                        };
                        emit_speculative_hold_counters_log(worker_scheduler.get_metrics());
                        // (#task-resource-profile Phase-2a) Same spawn-once cadence,
                        // same DARK-on-/metrics rationale: surface the profile-map
                        // counters so the observe-only signal is readable in prod.
                        emit_resource_profile_counters_log(worker_scheduler.get_metrics());
                        // (#task-resource-profile Phase-2b) Same spawn-once cadence,
                        // same DARK-on-/metrics rationale: surface the inject-observe
                        // counterfactual counters so the observe-only accuracy signal
                        // (the enforce phase depends on) is readable in prod.
                        emit_inject_observe_counters_log(worker_scheduler.get_metrics());
                        // (#task-resource-profile Phase-2c) Same spawn-once cadence,
                        // same DARK-on-/metrics rationale: surface the leave-one-out
                        // prediction-accuracy counters — the load-bearing Phase-3
                        // accuracy gate (accuracy_predicted_under ≈ 0 ⇒ tail is a
                        // safe reservation) — so the observe-only signal is readable.
                        emit_prediction_accuracy_counters_log(worker_scheduler.get_metrics());
                        for (worker_id, construct_latency_ms_p95) in
                            worker_scheduler.construct_latency_snapshot().await
                        {
                            // #perf: DEMOTED info! → debug!. This per-worker
                            // gossip line fired ~1,796/15min but is INERT — the
                            // `T_SETUP` hold gate it was added to feed is not
                            // wired (0 fires), so it is LOGGED-only and feeds
                            // nothing today (Chesterton's Fence: the value's
                            // consumer never landed). Unlike the load-bearing
                            // #247/#477 lifecycle logs (rate-limited, kept at
                            // info!), demoting an inert log to debug! is the
                            // correct fix even though `release_max_level_info`
                            // strips it from the release binary — there is no
                            // signal to preserve. The underlying value stays
                            // available via `construct_latency_snapshot()` (still
                            // computed each cycle) + `update_worker_construct_latency`
                            // (the gossip mechanism is untouched); a future
                            // hold-gate consumer or a debug build re-surfaces it.
                            debug!(
                                tag = "worker_construct_latency",
                                worker_id = %worker_id.0,
                                construct_latency_ms_p95,
                                "worker-gossiped cold dir-cache construct latency (decayed p95 ms); \
                                 T_SETUP tuning input — LOGGED only, not yet consumed by the hold gate"
                            );
                        }
                    }
                    // Unreachable.
                });

            // (#task-resource-profile Phase-3 §12) Background snapshot task — periodic
            // persist of the resource-profile map. Spawned ONLY when a path is
            // configured. Holds a `Weak<ApiWorkerScheduler>` (exits on scheduler drop).
            // Each tick: snapshot (clone under the map lock, release), then serialize +
            // atomically write (tmp + rename) OFF the lock via `tokio::fs` — NO fsync,
            // NO lock held across `.await`. A failed write is logged, not fatal.
            let task_resource_profile_persist_spawn = spec
                .resource_profile_persist_path
                .as_ref()
                .map(|persist_path| {
                    let persist_path = std::path::PathBuf::from(persist_path);
                    let interval_secs = spec.resource_profile_persist_interval_secs.max(1);
                    let weak_ws = Arc::downgrade(&worker_scheduler);
                    spawn!("simple_scheduler_resource_profile_persist", async move {
                        let mut interval =
                            tokio::time::interval(Duration::from_secs(interval_secs));
                        interval.tick().await; // skip the immediate first tick (empty map)
                        loop {
                            interval.tick().await;
                            let Some(ws) = weak_ws.upgrade() else {
                                return; // scheduler dropped
                            };
                            // Clone the resident entries out from under the map lock, then
                            // DROP the Arc before any await (do not hold it across I/O).
                            let entries = ws.resource_profile_snapshot_entries();
                            drop(ws);
                            if entries.is_empty() {
                                continue;
                            }
                            let now_unix = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let count = entries.len();
                            match resource_profile_persist::serialize_snapshot(entries, now_unix) {
                                Ok(bytes) => {
                                    if let Err(err) = resource_profile_persist::write_snapshot_bytes(
                                        &persist_path,
                                        &bytes,
                                    )
                                    .await
                                    {
                                        warn!(
                                            tag = "resource_profile_persist_write",
                                            path = %persist_path.display(),
                                            %err,
                                            "resource-profile snapshot write failed (advisory; will retry next interval)"
                                        );
                                    } else {
                                        debug!(
                                            tag = "resource_profile_persist_write",
                                            path = %persist_path.display(),
                                            entries = count,
                                            "persisted resource-profile snapshot"
                                        );
                                    }
                                }
                                Err(err) => warn!(
                                    tag = "resource_profile_persist_write",
                                    %err,
                                    "resource-profile snapshot serialize failed"
                                ),
                            }
                        }
                    })
                });

            // (#dag-criticality) Background recompute + (optional) persist. Recompute
            // publishes the criticality snapshot (Tarjan SCC + reverse-topo longest-path,
            // O(V+E)) off the strict-leaf lock; when a persist path is set, the edge store
            // is serialized (off the lock) + written atomically (tmp+rename, NO fsync).
            let task_dag_recompute_spawn = if spec.dag_critical_path_enabled {
                let interval_secs = spec.dag_recompute_interval_secs.max(1);
                let persist_path = spec
                    .dag_edge_store_persist_path
                    .as_ref()
                    .map(std::path::PathBuf::from);
                let weak_ws = Arc::downgrade(&worker_scheduler);
                Some(spawn!("simple_scheduler_dag_recompute", async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
                    interval.tick().await; // skip the immediate first tick (empty store)
                    loop {
                        interval.tick().await;
                        let Some(ws) = weak_ws.upgrade() else {
                            return; // scheduler dropped
                        };
                        let Some(dag_state) = ws.dag_state() else {
                            drop(ws);
                            continue; // feature toggled off
                        };
                        drop(ws);
                        // Publish a fresh criticality snapshot (off the leaf lock).
                        dag_state.recompute();
                        // Best-effort persist when configured.
                        if let Some(path) = persist_path.as_ref() {
                            let data = dag_state.persist_snapshot();
                            if data.edges.is_empty() && data.durations.is_empty() {
                                continue;
                            }
                            let now_unix = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            match dag_criticality::serialize_dag_snapshot(data, now_unix) {
                                Ok(bytes) => {
                                    if let Err(err) =
                                        dag_criticality::write_dag_snapshot_bytes(path, &bytes).await
                                    {
                                        warn!(
                                            tag = "dag_edge_store_write",
                                            path = %path.display(),
                                            %err,
                                            "DAG edge store write failed (advisory; will retry next interval)"
                                        );
                                    }
                                }
                                Err(err) => warn!(
                                    tag = "dag_edge_store_write",
                                    %err,
                                    "DAG edge store serialize failed"
                                ),
                            }
                        }
                    }
                }))
            } else {
                None
            };

            let worker_match_logging_interval = match spec.worker_match_logging_interval_s {
                // -1 or 0 means disabled (0 used to cause expensive logging on every call)
                -1 | 0 => None,
                signed_secs => {
                    if let Ok(secs) = TryInto::<u64>::try_into(signed_secs) {
                        Some(Duration::from_secs(secs))
                    } else {
                        error!(
                            worker_match_logging_interval_s = spec.worker_match_logging_interval_s,
                            "Valid values for worker_match_logging_interval_s are -1, 0, or a positive integer, setting to disabled",
                        );
                        None
                    }
                }
            };
            Self {
                matching_engine_state_manager: state_manager.clone(),
                client_state_manager: state_manager.clone(),
                worker_scheduler,
                platform_property_manager,
                maybe_origin_event_tx,
                task_worker_matching_spawn,
                task_hold_counters_log_spawn,
                task_resource_profile_persist_spawn,
                task_dag_recompute_spawn,
                worker_match_logging_interval,
                max_matches_per_client_per_cycle: spec.max_matches_per_client_per_cycle,
                batch_affinity_metrics: BatchAffinityMetrics::default(),
                output_affinity_metrics: OutputAffinityMetrics::default(),
                output_file_affinity_metrics: OutputFileAffinityMetrics::default(),
                recent_roots_window: Mutex::new(RecentRootsWindow::new()),
                affinity_clock,
                pending_affinity_probe_enabled,
                enable_speculative_prefetch: spec.enable_speculative_prefetch,
                speculative_prefetch_backlog_threshold: spec.speculative_prefetch_backlog_threshold,
                speculative_prefetch_ttl_s: spec.speculative_prefetch_ttl_s,
            }
        });
        (action_scheduler, worker_scheduler_clone)
    }
}

#[async_trait]
impl ClientStateManager for SimpleScheduler {
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        self.inner_add_action(client_operation_id, action_info)
            .await
    }

    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error> {
        self.inner_filter_operations(filter).await
    }

    /// Routes cancel through `ApiWorkerScheduler::cancel_operation_internal`,
    /// which resolves the operation→worker assignment and dispatches a
    /// `KillOperationRequest`. Part of the AC-poisoning fix composite
    /// invariant (Phase D base design): cancel arrives via this path
    /// → worker sets `RunningActionImpl::cancelled` → AC write
    /// suppressed at `local_worker.rs` publish closure.
    ///
    /// Bazel-facing callers pass a CLIENT operation_id (what Bazel
    /// knows from `Operation.name`); the worker map is keyed by the
    /// matching engine's INTERNAL operation_id. This method
    /// translates client→internal via
    /// `client_state_manager.client_operation_id_to_operation_id`. If
    /// translation returns `None` (the input may already be an
    /// internal id, e.g. test callers passing `start.operation_id`),
    /// the caller's value is forwarded as-is to
    /// `cancel_operation_internal`, which is itself idempotent on
    /// unknown ids.
    async fn cancel_operation(&self, operation_id: &OperationId) -> Result<(), Error> {
        let target = self
            .client_state_manager
            .client_operation_id_to_operation_id(operation_id)
            .await
            .err_tip(|| "In SimpleScheduler::cancel_operation translating client→internal")?
            .unwrap_or_else(|| operation_id.clone());
        self.worker_scheduler
            .cancel_operation_internal(&target)
            .await
    }
}

#[async_trait]
impl KnownPlatformPropertyProvider for SimpleScheduler {
    async fn get_known_properties(&self, _instance_name: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .worker_scheduler
            .get_platform_property_manager()
            .get_known_properties()
            .keys()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl WorkerScheduler for SimpleScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.worker_scheduler.get_platform_property_manager()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        self.worker_scheduler.add_worker(worker).await
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_action(worker_id, operation_id, update)
            .await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .worker_keep_alive_received(worker_id, timestamp)
            .await
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error> {
        self.worker_scheduler.remove_worker(worker_id).await
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        self.worker_scheduler.shutdown(shutdown_guard).await;
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.worker_scheduler
            .remove_timedout_workers(now_timestamp)
            .await
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        self.worker_scheduler
            .set_drain_worker(worker_id, is_draining)
            .await
    }

    async fn update_worker_load(
        &self,
        worker_id: &WorkerId,
        cpu_load_pct: u32,
        p_core_load_pct: u32,
        e_core_load_pct: u32,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_worker_load(worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct)
            .await
    }

    async fn update_worker_indefinite_pin_saturation(
        &self,
        worker_id: &WorkerId,
        indefinite_pin_saturated: bool,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_worker_indefinite_pin_saturation(worker_id, indefinite_pin_saturated)
            .await
    }

    async fn update_worker_swap_pressure(
        &self,
        worker_id: &WorkerId,
        swap_pressured: bool,
        swap_pressure_rate_per_sec: u32,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_worker_swap_pressure(worker_id, swap_pressured, swap_pressure_rate_per_sec)
            .await
    }

    async fn update_worker_disk_pressure(
        &self,
        worker_id: &WorkerId,
        disk_pressured: bool,
        available_disk_bytes: u64,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_worker_disk_pressure(worker_id, disk_pressured, available_disk_bytes)
            .await
    }

    async fn update_worker_construct_latency(
        &self,
        worker_id: &WorkerId,
        construct_latency_ms_p95: u32,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_worker_construct_latency(worker_id, construct_latency_ms_p95)
            .await
    }

    async fn update_cached_directories(
        &self,
        worker_id: &WorkerId,
        digests: HashSet<DigestInfo>,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_cached_directories(worker_id, digests)
            .await
    }

    async fn update_cached_subtrees(
        &self,
        worker_id: &WorkerId,
        is_full_snapshot: bool,
        full_set: Vec<DigestInfo>,
        added: Vec<DigestInfo>,
        removed: Vec<DigestInfo>,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_cached_subtrees(worker_id, is_full_snapshot, full_set, added, removed)
            .await
    }

    async fn broadcast_blobs_in_stable_storage(&self, digests: Vec<DigestInfo>) {
        self.worker_scheduler
            .broadcast_blobs_in_stable_storage(digests)
            .await;
    }

    async fn broadcast_blobs_in_stable_storage_chunked(
        &self,
        digests: Vec<DigestInfo>,
        store_id: &str,
    ) {
        self.worker_scheduler
            .broadcast_blobs_in_stable_storage_chunked(digests, store_id)
            .await;
    }

    async fn bis_ack_received(
        &self,
        worker_id: &WorkerId,
        broadcast_id: u64,
        sequence: u32,
        server_instance_token: u64,
    ) {
        self.worker_scheduler
            .bis_ack_received(worker_id, broadcast_id, sequence, server_instance_token)
            .await;
    }

    async fn clear_bis_resend_buffer_for_endpoint(&self, cas_endpoint: &str) {
        self.worker_scheduler
            .clear_bis_resend_buffer_for_endpoint(cas_endpoint)
            .await;
    }
}

impl RootMetricsComponent for SimpleScheduler {}

#[cfg(test)]
mod batch_sched_gain_test {
    //! (#p1p2 M1-replay) Prod-shape tests for the batch-scheduling counterfactual
    //! that FAITHFULLY REPLAYS the live M1 P-headroom gate. The gate is CONFIRMED
    //! ON in prod (25,303 `p_headroom_gate_exclusion` events observed live
    //! 2026-07-02; workers at `running_actions=4, p_core_count=4, p_load=0/2/5`
    //! are STILL excluded — proving `p_idle_threshold_pct == 0`, i.e. v1 behavior:
    //! `has_p_headroom ⟺ p_core == 0 || running < p_core`). The contention driver
    //! is the FRESH-count `p_core_count` cache-tier eligibility WITH the Phase-2
    //! lift (when no viable worker has headroom, the gate lifts and cache-tier
    //! opens to all). These fixtures use the real M4 shape (p_core=4, e_core=6)
    //! and seed `running` SPANNING the gate boundary.

    use std::collections::{HashMap, HashSet};

    use nativelink_util::common::DigestInfo;

    use super::{
        BatchSchedAction, BatchSchedGateCfg, BatchSchedWorker, PER_FILE_WEIGHT,
        compute_batch_sched_gain,
    };

    /// A distinct digest keyed by a single seed byte (the rest zero). Only
    /// membership + the attached direct-byte weight matter for `s(i,j)`.
    fn dg(seed: u8) -> DigestInfo {
        DigestInfo::new([seed; 32], 0)
    }

    /// One action carrying `(digest, direct_bytes)` pairs. `dir_direct_files`
    /// left empty so `s = Σ matching direct_bytes` (isolates the gate/assignment
    /// logic from the `PER_FILE_WEIGHT` blend — the blend equality is pinned
    /// separately by `per_file_weight_matches_dispatch`).
    fn mk_action(dirs: &[(DigestInfo, u64)]) -> BatchSchedAction {
        let mut dir_digests = HashSet::new();
        let mut dir_direct_bytes = HashMap::new();
        let dir_direct_files = HashMap::new();
        for (d, bytes) in dirs {
            dir_digests.insert(*d);
            dir_direct_bytes.insert(*d, *bytes);
        }
        BatchSchedAction {
            dir_digests,
            dir_direct_bytes,
            dir_direct_files,
        }
    }

    /// One M4-shape worker: real `p_core_count`/`p_core_load_pct`, seeded fresh
    /// `running` count (the contention driver), a warm cache, and a CONSTANT
    /// `load_penalty` (computed once from the real snapshot in production; here
    /// passed directly — most fixtures use 0 so cache-fit alone discriminates).
    fn mk_worker(
        cache: &[DigestInfo],
        running: u64,
        p_core_count: u32,
        p_core_load_pct: u32,
        load_penalty: i64,
    ) -> BatchSchedWorker {
        BatchSchedWorker {
            cached_subtree_digests: cache.iter().copied().collect(),
            running,
            p_core_count,
            // (#sched-work-conservation) These solver fixtures test the
            // assignment logic, not the E-spill cap; `e_core_count = 0` makes the
            // total-core term collapse to `running < p_core_count`, so their gate
            // behavior is byte-identical to before the fix.
            e_core_count: 0,
            p_core_load_pct,
            load_penalty,
        }
    }

    /// The live-replay gate config: enabled, threshold 0 (v1 behavior — the
    /// override clause never fires, matching the 25,303-exclusion prod data),
    /// factor 2 (prod default; inert at threshold 0).
    const GATE_ON_V1: BatchSchedGateCfg = BatchSchedGateCfg {
        enabled: true,
        idle_threshold_pct: 0,
        override_factor: 2,
    };

    /// (#sched-work-conservation) The observability-only batch mirror must
    /// consult E-core capacity exactly like the dispatch gate
    /// (`worker_has_p_headroom`), so the counterfactual gauge does not diverge by
    /// the E-spill fix. CPU-bound worker (p_load 98 >= threshold 50 → idle-P
    /// override dead), 4 P + 6 E = 10 total cores.
    ///
    /// MUTATION: removing the `|| w.running < p + e` term from
    /// `BatchSchedGateCfg::has_p_headroom` makes the admit assertions RED-fail
    /// (the mirror would deny at running >= 4), proving the term is load-bearing.
    #[test]
    fn test_batch_mirror_p_headroom_spills_to_e_cores() {
        // Prod config: enabled, threshold 50, factor 4.
        let gate = BatchSchedGateCfg {
            enabled: true,
            idle_threshold_pct: 50,
            override_factor: 4,
        };
        let mk = |running: u64| BatchSchedWorker {
            cached_subtree_digests: HashSet::new(),
            running,
            p_core_count: 4,
            e_core_count: 6, // 4 P + 6 E = 10 total cores
            p_core_load_pct: 98, // CPU-bound → idle-P override dead
            load_penalty: 0,
        };
        // E-spill band [4, 10): headroom via the total-core term.
        for running in 4..=9 {
            assert!(
                gate.has_p_headroom(&mk(running)),
                "batch mirror: CPU-bound worker with 6 idle E-cores has headroom \
                 at running={running} (< p+e = 10) — gauge fidelity with the \
                 dispatch gate's E-core spill"
            );
        }
        // Equilibrium: running == p+e = 10 → denied (E full, override dead).
        assert!(
            !gate.has_p_headroom(&mk(10)),
            "batch mirror: at running == p+e(10) both P and E slots are full and \
             the CPU-bound override is dead → denied, matching the dispatch gate"
        );
    }

    /// (Test 1) CONTENDED-BAND GAIN. Warm cache-holders are gate-EXCLUDED
    /// (`running ≥ p_core=4`); only a few coldish workers retain p_headroom
    /// (`running < 4`). Two queued tasks whose only p_headroom-eligible options
    /// differ in partial-cache-fit such that a GLOBAL assignment of the limited
    /// p_headroom slots beats greedy priority-order.
    ///
    /// Fixture (all p_core=4, e_core omitted, load_penalty 0):
    ///   - `WA` cache {p,q}, running=4 → EXCLUDED (holds the big A2 match but gated out).
    ///   - `WC` cache {p,q}, running=3 → p_headroom (1 slot). Holds A1's p (100) AND A2's q (1000).
    ///   - `WD` cache {r},   running=3 → p_headroom (1 slot). Holds A1's r (90) only.
    ///   - A1 (priority 0) dir {p:100, r:90}. A2 (priority 1) dir {q:1000}.
    ///
    /// GREEDY (priority order): A1's argmax over the eligible {WC(s=100), WD(s=90)}
    /// → WC (100); WC running 3→4 → LOSES headroom. A2 now: WC excluded (fresh count),
    /// WA excluded (seeded ≥4), only WD eligible (s=0 for q). G = 100 + 0 = 100.
    /// BATCH (global): A1→WD (90), A2→WC (1000). B = 1090.
    /// gain = (1090−100)/100 = 990.
    #[test]
    fn contended_band_gain_reorder_beats_priority() {
        let p = dg(1);
        let q = dg(2);
        let r = dg(3);
        let actions = vec![
            mk_action(&[(p, 100), (r, 90)]), // A1, priority 0
            mk_action(&[(q, 1000)]),         // A2, priority 1
        ];
        let workers = vec![
            mk_worker(&[p, q], 4, 4, 50, 0), // WA excluded (running==p_core)
            mk_worker(&[p, q], 3, 4, 50, 0), // WC p_headroom, 1 slot
            mk_worker(&[r], 3, 4, 50, 0),    // WD p_headroom, 1 slot
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 100,
            "GREEDY (priority order): A1→WC(100) consumes WC's last fresh-count \
             p_headroom slot (running 3→4); A2 then finds WC gate-excluded and WA \
             seeded-excluded, so it lands cold on WD (0) → G=100. got {}",
            gain.greedy_score
        );
        assert_eq!(
            gain.gain_pct, 990,
            "BATCH reorders: A1→WD(90), A2→WC(1000) → B=1090; gain=(1090−100)/100=990. \
             A subtree-aware global assignment of the LIMITED p_headroom slots beats \
             greedy priority-order. got {}",
            gain.gain_pct
        );
    }

    /// (Test 2) PHASE-2 LIFT → GAIN 0 (all workers full). ALL workers at
    /// `running ≥ p_core=4` → no viable worker has p_headroom → the gate LIFTS →
    /// cache-tier opens to ALL workers → greedy assigns each task to its unique
    /// cache holder by cache lead → batch cannot improve → `gain_pct == 0`. This
    /// documents the "under heavy load the gate is inert, greedy near-optimal"
    /// regime. The NON-ZERO `greedy_score` is what the lift BUYS: without the lift
    /// no worker would be cache-eligible and greedy would strand (G=0).
    #[test]
    fn phase2_lift_all_full_gain_zero_but_greedy_nonzero() {
        let da = dg(10);
        let db = dg(11);
        let actions = vec![
            mk_action(&[(da, 500)]), // A1 → only WA caches da
            mk_action(&[(db, 700)]), // A2 → only WB caches db
        ];
        let workers = vec![
            mk_worker(&[da], 4, 4, 90, 0), // WA full (running==p_core)
            mk_worker(&[db], 5, 4, 90, 0), // WB full (running>p_core)
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 1200,
            "the PHASE-2 LIFT opens cache-tier to all when no worker has headroom: \
             greedy places A1→WA(500) + A2→WB(700) → G=1200. WITHOUT the lift, no \
             worker is cache-eligible and greedy strands at 0. got {}",
            gain.greedy_score
        );
        assert_eq!(
            gain.gain_pct, 0,
            "with the gate lifted, greedy already reaches each action's unique cache \
             holder; a global batch cannot beat it → gain 0 (the heavy-load inert \
             regime). got {}",
            gain.gain_pct
        );
        assert_eq!(
            gain.gate_active_frac, 0,
            "gate LIFTED for every greedy step (no viable worker has headroom) → \
             gate_active_frac 0. got {}",
            gain.gate_active_frac
        );
    }

    /// (Test 3) FRESH-COUNT CONTENTION: assigning `p_core − running` tasks to a
    /// worker makes it lose p_headroom (cache-tier-ineligible for the NEXT action)
    /// — verified through the eligibility recompute. `WC` (running=3, p_core=4,
    /// caches d) has exactly ONE headroom slot; `WFill` (running=0, p_core=4, cold)
    /// keeps the gate ACTIVE so the lift does not re-admit WC. Two actions both
    /// matching d: greedy gives the 1st to WC (running 3→4, now excluded); the 2nd
    /// finds WC gate-excluded and lands cold on WFill. Only ONE d-match is captured.
    #[test]
    fn fresh_count_recompute_shuts_worker_out_after_p_core_assignments() {
        let d = dg(20);
        let actions = vec![mk_action(&[(d, 1000)]), mk_action(&[(d, 1000)])];
        let workers = vec![
            mk_worker(&[d], 3, 4, 50, 0), // WC: 1 headroom slot, caches d
            mk_worker(&[], 0, 4, 50, 0),  // WFill: deep headroom, keeps gate active, cold
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 1000,
            "the FRESH-count recompute: A1→WC captures d (1000) and pushes WC to \
             running=4 (== p_core) → WC loses p_headroom; A2 finds WC gate-excluded \
             (WFill still holds the gate active) and lands cold on WFill (0). Exactly \
             ONE d-match captured → G=1000. If the recompute were broken (running not \
             re-evaluated) both would pile on WC → 2000. got {}",
            gain.greedy_score
        );
    }

    /// (Test 4) `greedy_score` + the two diagnostic gauges render. Contended-band
    /// fixture (Test 1): `gate_active_frac` and `mean_seed_running` must carry the
    /// regime-revealing values a scrape needs to interpret gain.
    #[test]
    fn greedy_score_and_diagnostic_gauges_render() {
        let p = dg(1);
        let q = dg(2);
        let r = dg(3);
        let actions = vec![mk_action(&[(p, 100), (r, 90)]), mk_action(&[(q, 1000)])];
        let workers = vec![
            mk_worker(&[p, q], 4, 4, 50, 0),
            mk_worker(&[p, q], 3, 4, 50, 0),
            mk_worker(&[r], 3, 4, 50, 0),
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 100,
            "greedy_score (=G) must render for the contended-band regime. got {}",
            gain.greedy_score
        );
        // Both greedy steps ran with the gate ACTIVE (some viable worker retained
        // p_headroom on each step) → gate_active_frac = 100 (×100 of fraction 1.0).
        assert_eq!(
            gain.gate_active_frac, 100,
            "both greedy assignment steps had a viable p_headroom worker → gate_active \
             on 2/2 steps → gate_active_frac = 100 (×100). Reveals the CONTENDED band \
             (vs 0 = all-lifted). got {}",
            gain.gate_active_frac
        );
        // mean seeded running across the 3 sampled workers = (4+3+3)/3 = 3.33…,
        // ×100 floored = 333.
        assert_eq!(
            gain.mean_seed_running, 333,
            "mean seeded running across sampled workers = (4+3+3)/3 = 3.33 → ×100 = 333. \
             Reveals how deep in the gate band the fleet sits. got {}",
            gain.mean_seed_running
        );
    }

    /// (Test 5) TIER-1.5 `blended_s > 0` CROSSOVER: when the argmax cache worker's
    /// `cached_score − load_penalty ≤ 0` (the cache saving does NOT beat the load
    /// cost), production's Tier-1.5 DECLINES that pick (`api_worker_scheduler.rs`
    /// `best.filter(|blended_s| *blended_s > 0)`) — the action falls to an idle
    /// LRU worker with cache contribution 0. The model mirrors this: such an action
    /// contributes 0 (NOT its `cached_score`) to BOTH G and B, so `gain_pct` is an
    /// EXACT measure of the realizable reorder benefit over supra-threshold picks
    /// (not an up-biased upper bound that over-credits load-shed marginal picks).
    ///
    /// Fixture (gate ON, p_core=4; `WHold` deep-headroom keeps the gate active):
    ///   - `A1` (priority 0) dir {y:100} — a SMALL cache value.
    ///   - `A2` (priority 1) dir {z:900} — a LARGE cache value.
    ///   - `WShared` caches {y,z}, running=3 → ONE headroom slot; holds BOTH y & z,
    ///     load_penalty 0.
    ///   - `WLoaded` caches {y}, running=3 → ONE headroom slot; holds y ONLY,
    ///     load_penalty 200 (so a y-match here is `100 − 200 = −100` — load-dominated).
    ///   - `WHold`  caches {}, running=0 → deep headroom, load_penalty 1000, cold.
    ///
    /// GREEDY (priority order): A1(y)→WShared (100; WShared's only slot consumed,
    ///   running 3→4 → excluded). A2(z) then finds WShared gate-excluded; the only
    ///   remaining eligible cache option is WLoaded, which does NOT cache z → cold
    ///   (raw_s 0) → contributes 0. G = 100 (the z-value is STRANDED by priority
    ///   order + the gate) — identical with or without the crossover (z found no
    ///   cache match to shed).
    /// BATCH (reorder): A2(z)→WShared (900 — the big match placed well; WShared
    ///   →running 4 excluded). A1(y) then lands on WLoaded (its only remaining
    ///   cache option), where `100 − 200 = −100 ≤ 0` → the CROSSOVER zeroes it (it
    ///   fell to the LRU/idle path). B = 900 + 0 = 900 → gain = (900−100)/100 = 800.
    /// WITHOUT the crossover BATCH would ALSO count A1's load-shed 100 on WLoaded →
    ///   B = 1000 → gain 900 (the ~1.77× family of up-bias). The crossover makes
    ///   the gain EXACT (800 = ONLY the realizable z-reorder benefit).
    #[test]
    fn crossover_load_dominated_pick_contributes_zero_exact_gain() {
        let y = dg(30);
        let z = dg(31);
        let actions = vec![
            mk_action(&[(y, 100)]), // A1, priority 0 — small
            mk_action(&[(z, 900)]), // A2, priority 1 — large
        ];
        let workers = vec![
            mk_worker(&[y, z], 3, 4, 50, 0),   // WShared: 1 slot, holds y AND z
            mk_worker(&[y], 3, 4, 50, 200),    // WLoaded: 1 slot, holds y, load-dominated
            mk_worker(&[], 0, 4, 50, 1000),    // WHold: deep headroom, keeps gate active, cold
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 100,
            "GREEDY: priority order gives WShared's ONLY slot to the small A1(y)=100, \
             then A2(z) is gate-excluded from WShared and finds no z-cache elsewhere \
             (WLoaded caches only y) → z stranded cold → G=100. got {}",
            gain.greedy_score
        );
        assert_eq!(
            gain.gain_pct, 800,
            "the Tier-1.5 `blended_s > 0` CROSSOVER: BATCH reorders A2(z)→WShared (900), \
             spilling A1(y) to the LOADED WLoaded where 100−200=−100 ≤ 0 → the crossover \
             zeroes it (fell to the LRU/idle path) → B=900+0=900 → gain=(900−100)/100=800 \
             (ONLY the realizable z-reorder benefit). WITHOUT the crossover B would count \
             the load-shed 100 → B=1000 → gain 900 (the up-bias). got {}",
            gain.gain_pct
        );
    }

    /// (Test 6) CROSSOVER SYMMETRY: G and B apply the `blended_s > 0` crossover
    /// under the IDENTICAL predicate, so BATCH cannot claim a cache credit that
    /// GREEDY zeroes for the SAME (action, worker) pairing. Fixture forces G and B
    /// to the SAME assignment (A1→WA, A2→WB), so the ONLY thing that could produce
    /// a non-zero gain is an asymmetric crossover (batch skipping the filter that
    /// greedy applies). Asserting `gain_pct == 0` proves the crossover is symmetric.
    ///
    /// Fixture (gate ON, p_core=4; `WHold` keeps the gate active):
    ///   - `A1` dir {s:1000} — only `WA` caches s. `A2` dir {d:300} — only `WB` caches d.
    ///   - `WA` caches {s,d}, running=3 → ONE slot; supra s (1000), load_penalty 0.
    ///   - `WB` caches {d},   running=3 → ONE slot; marginal d, load_penalty 500
    ///     (a d-match here is `300 − 500 = −200` — load-dominated).
    ///   - `WHold` caches {}, running=0 → deep headroom, load_penalty 1000, cold.
    ///
    /// GREEDY: A1(s)→WA (1000; WA running 3→4 excluded). A2(d): WA excluded, WHold
    ///   caches no d → only WB is a d-holder; `300 − 500 = −200 ≤ 0` → crossover
    ///   zeroes it → contributes 0. G = 1000.
    /// BATCH: same pairing — A1(s)→WA (1000; excluded), A2(d)→WB where the crossover
    ///   ALSO zeroes the `−200` pick → B = 1000. gain = 0. If BATCH did NOT apply
    ///   the crossover it would credit A2's 300 on WB → B = 1300 → gain 30. gain==0
    ///   proves batch honors the SAME crossover as greedy (no batch-only advantage).
    #[test]
    fn crossover_applies_symmetrically_to_greedy_and_batch() {
        let s = dg(40);
        let d = dg(41);
        let actions = vec![mk_action(&[(s, 1000)]), mk_action(&[(d, 300)])];
        let workers = vec![
            mk_worker(&[s, d], 3, 4, 50, 0),   // WA: 1 slot, supra s, holds d too
            mk_worker(&[d], 3, 4, 50, 500),    // WB: 1 slot, marginal d, load-dominated
            mk_worker(&[], 0, 4, 50, 1000),    // WHold: deep headroom, keeps gate active
        ];
        let gain = compute_batch_sched_gain(&actions, &workers, GATE_ON_V1);
        assert_eq!(
            gain.greedy_score, 1000,
            "GREEDY: A1(s)→WA (1000, WA excluded after); A2(d) lands on the loaded WB \
             where 300−500=−200 ≤ 0 → crossover zeroes it → G=1000. got {}",
            gain.greedy_score
        );
        assert_eq!(
            gain.gain_pct, 0,
            "SYMMETRY: G and B assign IDENTICALLY (A1→WA, A2→WB); the crossover zeroes \
             the SAME load-dominated A2/WB pick in BOTH → B=G=1000 → gain 0. If batch \
             skipped the crossover it would credit A2's 300 → B=1300 → gain 30; gain==0 \
             proves the crossover is applied symmetrically. got {}",
            gain.gain_pct
        );
    }

    /// The blend constant `s = bytes + files*PER_FILE_WEIGHT` must match the
    /// production Tier-1.5 dispatch constant (a divergent weight would make the
    /// (B−G) delta compare against a fiction). Pins the equality the model relies
    /// on.
    #[test]
    fn per_file_weight_matches_dispatch() {
        assert_eq!(
            PER_FILE_WEIGHT,
            100 * 1024,
            "PER_FILE_WEIGHT must equal the api_worker_scheduler.rs Tier-1.5 constant \
             (100 KiB/file) so s(i,j) is byte-identical to the dispatch score"
        );
    }
}
