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

use std::collections::HashMap;

#[cfg(feature = "dev-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::serde_utils::{
    convert_duration_with_shellexpand, convert_duration_with_shellexpand_and_negative,
    convert_numeric_with_shellexpand, convert_string_with_shellexpand,
};
use crate::stores::{GrpcEndpoint, Retry, StoreRefName};

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum SchedulerSpec {
    Simple(SimpleSpec),
    Grpc(GrpcSpec),
    CacheLookup(CacheLookupSpec),
    PropertyModifier(PropertyModifierSpec),
    HistoricalResource(HistoricalResourceSpec),
}

/// When the scheduler matches tasks to workers that are capable of running
/// the task, this value will be used to determine how the property is treated.
#[derive(Deserialize, Serialize, Debug, Clone, Copy, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum PropertyType {
    /// Requires the platform property to be a number (integer or floating-point)
    /// and when the scheduler looks for appropriate worker nodes that are
    /// capable of executing the task, the task will not run on a node that
    /// has less than this value.
    Minimum,

    /// Requires the platform property to be a string and when the scheduler
    /// looks for appropriate worker nodes that are capable of executing the
    /// task, the task will not run on a node that does not have this property
    /// set to the value with exact string match.
    Exact,

    /// Does not restrict on this value and instead will be passed to the worker
    /// as an informational piece.
    /// TODO(palfrey) In the future this will be used by the scheduler and worker
    /// to cause the scheduler to prefer certain workers over others, but not
    /// restrict them based on these values.
    Priority,

    //// Allows jobs to be requested with said key, but without requiring workers
    //// to have that key
    Ignore,
}

/// When a worker is being searched for to run a job, this will be used
/// on how to choose which worker should run the job when multiple
/// workers are able to run the task.
#[derive(Copy, Clone, Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum WorkerAllocationStrategy {
    /// Prefer workers that have been least recently used to run a job.
    #[default]
    LeastRecentlyUsed,
    /// Prefer workers that have been most recently used to run a job.
    MostRecentlyUsed,
}

/// (#sched-cpu-first) Selects how the worker matcher RANKS the winner among
/// eligible workers. Eligibility (viability, pressure gates, the P-headroom
/// gate) is IDENTICAL in both modes — only the winner-ranking differs.
#[derive(Copy, Clone, Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum PlacementMode {
    /// (DEFAULT) Cache-affinity-first: the exact-root / subtree-coverage /
    /// blob-locality cascade steers work to the worker that already holds the
    /// action's input tree, so a cold reconstruct is avoided. Byte-identical to
    /// the pre-`#sched-cpu-first` matcher. Correct for the mixed / fetch-bound
    /// workload where input-fetch is the throughput lever.
    #[default]
    CacheAffinityFirst,
    /// CPU-idle-first: rank eligible workers by their (synthetic-compensated)
    /// P-core load ascending (lowest-P-load wins), abandoning cache affinity
    /// while any worker has an idle P-core (Regime A). Only when EVERY viable
    /// worker's P-load rounds >= 90% does cache affinity return as a tiebreak
    /// within the least-loaded total-CPU decile bucket (Regime B). For a
    /// CPU-bound-dominant workload (rustc-heavy compiles) where getting onto an
    /// idle P-core dominates the (post-clonefile ~10ms) cache-affinity saving.
    /// OPT-IN, operator-toggled per build phase — see the design at
    /// `.claude/audits/scheduler-cpu-first-placement-mode-design-2026-07-15.md`.
    CpuIdleFirst,
}

// defaults to every 10s
const fn default_worker_match_logging_interval_s() -> i64 {
    10
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct SimpleSpec {
    /// A list of supported platform properties mapped to how these properties
    /// are used when the scheduler looks for worker nodes capable of running
    /// the task.
    ///
    /// For example, a value of:
    /// ```json
    /// { "cpu_count": "minimum", "cpu_arch": "exact" }
    /// ```
    /// With a job that contains:
    /// ```json
    /// { "cpu_count": "8", "cpu_arch": "arm" }
    /// ```
    /// Will result in the scheduler filtering out any workers that do not have
    /// `"cpu_arch" = "arm"` and filter out any workers that have less than 8 cpu
    /// cores available.
    ///
    /// The property names here must match the property keys provided by the
    /// worker nodes when they join the pool. In other words, the workers will
    /// publish their capabilities to the scheduler when they join the worker
    /// pool. If the worker fails to notify the scheduler of its (for example)
    /// `"cpu_arch"`, the scheduler will never send any jobs to it, if all jobs
    /// have the `"cpu_arch"` label. There is no special treatment of any platform
    /// property labels other and entirely driven by worker configs and this
    /// config.
    pub supported_platform_properties: Option<HashMap<String, PropertyType>>,

    /// The amount of time to retain completed actions for in case
    /// a `WaitExecution` is called after the action has completed.
    /// Default: 60 seconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub retain_completed_for_s: u32,

    /// Mark operations as completed with error if no client has updated them
    /// within this duration.
    /// Default: 60 seconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub client_action_timeout_s: u64,

    /// Remove workers from pool once the worker has not responded in this
    /// amount of time in seconds.
    /// Default: 5 seconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub worker_timeout_s: u64,

    /// Maximum time (seconds) an action can stay in Executing state without
    /// any worker update before being timed out and re-queued.
    /// This applies regardless of worker keepalive status, catching cases
    /// where a worker is alive (sending keepalives) but stuck on a specific
    /// action. Set to 0 to disable (relies only on `worker_timeout_s`).
    ///
    /// Default: 0 (disabled)
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_action_executing_timeout_s: u64,

    /// If a job returns an internal error or times out this many times when
    /// attempting to run on a worker the scheduler will return the last error
    /// to the client. Jobs will be retried and this configuration is to help
    /// prevent one rogue job from infinitely retrying and taking up a lot of
    /// resources when the task itself is the one causing the server to go
    /// into a bad state.
    /// Default: 3
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_job_retries: usize,

    /// The strategy used to assign workers jobs.
    #[serde(default)]
    pub allocation_strategy: WorkerAllocationStrategy,

    /// The storage backend to use for the scheduler.
    /// Default: memory
    pub experimental_backend: Option<ExperimentalSimpleSchedulerBackend>,

    /// Every N seconds, do logging of worker matching
    /// e.g. "worker busy", "can't find any worker"
    /// Defaults to 10s. Can be set to -1 to disable
    #[serde(
        default = "default_worker_match_logging_interval_s",
        deserialize_with = "convert_duration_with_shellexpand_and_negative"
    )]
    pub worker_match_logging_interval_s: i64,

    /// Maximum number of actions that can be matched to workers for a single
    /// client (identified by `instance_name`) in one matching cycle. When
    /// multiple clients are competing for workers, this prevents one client
    /// from monopolizing all available workers by round-robin interleaving
    /// actions from different clients.
    ///
    /// Set to 0 to disable fair scheduling (unlimited matches per client
    /// per cycle). Default: 0 (disabled).
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_matches_per_client_per_cycle: usize,

    /// Name of the CAS store used for resolving input trees during
    /// locality-aware scheduling. When set, the scheduler resolves the
    /// full input tree for each action and scores workers by how many
    /// input bytes they already have cached.
    ///
    /// This should reference a CAS store in the `stores` section.
    /// If not set, locality-aware tree scoring is disabled (only the
    /// action affinity tier is used).
    #[serde(default)]
    pub cas_store: Option<StoreRefName>,

    /// (#sched-blend) Cache-vs-load crossover knob for the continuous
    /// cache-affinity blend (Tier 1 / Tier 1.5). Bytes-equivalent cost
    /// charged per ONE WHOLE weighted core of free-capacity deficit: a
    /// cache-warm-but-busy worker must save at least this many input
    /// bytes per weighted core it is into deficit to still be preferred
    /// over an idle peer. Larger ⇒ cache affinity dominates longer into
    /// load; smaller ⇒ load sheds cache affinity earlier.
    ///
    /// Default: 524288 (512 KiB). PROVISIONAL — this is the design's
    /// reasoned *anchor* (≈ 5 cached files, or one medium blob), NOT a
    /// measured optimum, and it sits on a routing-flip boundary for
    /// marginal cache hits. It is intended to be SOAK-SELECTED before the
    /// first production deploy by sweeping {128 KiB, 512 KiB, 2 MiB} and
    /// shipping the sweep-selected value; deploy the chosen value via
    /// this config, do not rely on the anchor as if it were validated.
    #[serde(default = "default_load_byte_cost", deserialize_with = "convert_numeric_with_shellexpand")]
    pub load_byte_cost: u64,

    /// (#sched-blend) Substituted P-core count for workers that report
    /// `p_core_count = 0` on their connect frame (legacy worker / Linux /
    /// Intel Mac with no perflevel sysctl). Gives the absolute-capacity
    /// blend a denominator so a count-less worker is order-preserving
    /// among other count-less workers (same relative ranking the prior
    /// %-only path gave them) and intentionally *under*-credited versus a
    /// count-reporting worker (the safe direction — we do not over-load a
    /// worker whose true capacity is unknown). Inert in an all-Apple-Silicon
    /// fleet (every Mac reports real counts).
    ///
    /// Default: 8.
    #[serde(default = "default_assume_core_count", deserialize_with = "convert_numeric_with_shellexpand")]
    pub assume_core_count: u32,

    /// (#sched M1 rebalance) When enabled, the worker matcher applies a
    /// dispatch-count P-headroom overflow gate on the cache-affinity tiers
    /// (exact-root / subtree-coverage / blob-locality): while ANY viable
    /// worker still has P-headroom (fewer in-flight actions than its
    /// advertised `p_core_count`), a worker WITHOUT P-headroom is excluded
    /// from those tiers so a saturated cache holder's surplus overflows to a
    /// P-headroom peer instead of piling onto already-full P cores (the
    /// 2026-06-30 sole-holder domino). When NO viable worker has P-headroom
    /// the gate lifts and selection proceeds over all viable workers via the
    /// existing LRU/MRU fallback (no wedge). A worker advertising
    /// `p_core_count == 0` is treated as ungated (never frozen out).
    ///
    /// Default: true (ENABLED, drift-proof — per user 2026-07-07: don't default
    /// features to OFF, it delays fixing their bugs; config drift already
    /// silently reverted this once). The flag remains an operational
    /// KILL-SWITCH: setting it `false` in config restores the byte-identical
    /// pre-gate matcher. Selection-only; no data-plane, ack, pin, or
    /// memory-gate effect.
    #[serde(default = "default_true")]
    pub p_headroom_gate_enabled: bool,

    /// (#sched M1 rebalance v2) `p_core_load_pct` below which a worker's P
    /// cores count as reported-idle enough to RELAX the dispatch-count gate:
    /// a worker at/over its `p_core_count` in-flight actions but reporting
    /// `p_core_load_pct < p_idle_threshold_pct` is I/O-bound (its P cores are
    /// idle), so the gate admits it — up to a FRESH-count ceiling
    /// (`p_core_count * p_headroom_override_factor`) that bounds the blast
    /// radius of a stale-low `p_load` (design §2/§3). Only consulted when
    /// `p_headroom_gate_enabled` is on.
    ///
    /// Default: 0 (= override OFF → EXACT v1 behavior; `p_load < 0` is never
    /// true, so clause 3 can never fire). A bare `#[serde(default)]` (u32 0)
    /// is correct HERE precisely because 0 is the intended "override off"
    /// value — landing the code changes nothing until an operator sets a
    /// threshold. Contrast `p_headroom_override_factor`, whose 0 would silently
    /// disable the override even with a set threshold, so it needs a named
    /// default fn.
    #[serde(default)]
    pub p_idle_threshold_pct: u32,

    /// (#sched M1 rebalance v2) In-flight ceiling MULTIPLIER for the bounded
    /// p_load override: the override (above) can admit a worker to at most
    /// `p_core_count * p_headroom_override_factor` in-flight actions. Past
    /// that, the fresh dispatch-count — never stale — shuts the gate, so the
    /// worst case from a fully-adversarial stale-low `p_load` is bounded
    /// over-concentration of `factor×` the P-core count on one worker, NOT a
    /// runaway pileup (design §3, invariant I5_Bounded). Only consulted when
    /// `p_headroom_gate_enabled` is on AND `p_idle_threshold_pct > 0`.
    ///
    /// Default: 2 (via `default_p_headroom_override_factor`). CRITICAL: a bare
    /// `#[serde(default)]` here would yield u32 `0` → ceiling
    /// `p_core_count * 0 == 0` → the override could NEVER admit any worker
    /// (`running < 0` is never true), silently disabling the whole v2 feature
    /// even with a set threshold. The named default fn keeps the deserialized
    /// empty-config value at 2.
    #[serde(default = "default_p_headroom_override_factor")]
    pub p_headroom_override_factor: u32,

    /// (#p2p-prefetch) When enabled, the scheduler carries per-missing-blob
    /// peer CAS endpoints INLINE in `StartExecute.missing_digest_peers` and
    /// STOPS server-prefetching the peer-held missing blobs — the assigned
    /// worker pulls those inputs P2P from a peer instead (worker-driven P2P
    /// input prefetch). The server keeps prefetching the server-only missing
    /// blobs (no peer holds them). The worker's `WorkerProxyStore` race
    /// co-launches a server fetch as the fallback, so a peer miss/slow/down
    /// degrades to today's server latency, never a stall.
    ///
    /// Default: false (OFF) — byte-identical to today. With the flag OFF the
    /// scheduler prefetches the full missing set exactly as before and the
    /// inline field is left empty (the worker registers nothing extra). This
    /// is the "never worse than today" guarantee and the rollback lever.
    /// The realizable offload fraction is UNMEASURED; the feature must not
    /// ship on the ~85-90% ceiling metric — leave OFF until the landed
    /// `worker_proxy_peer_fetch_*` counters measure realizable win-vs-fallback
    /// on a canary (design §7).
    #[serde(default)]
    pub enable_p2p_input_prefetch: bool,

    /// (#sched-affinity-probe) OPT-IN master gate for the OBSERVABILITY-ONLY
    /// pending-set affinity/batch-scheduling probe (`record_pending_affinity_surplus`).
    /// When `true` the probe updates its `batch_affinity` / `output_affinity`
    /// gauges each match cycle; when `false` (the DEFAULT —
    /// `default_pending_affinity_probe_enabled`) it never runs (ZERO overhead).
    ///
    /// Default is OFF because the probe is R&D instrumentation that NOTHING
    /// auto-consumes, its dominant kernel `compute_batch_sched_gain` is QUADRATIC
    /// (`O(actions² · workers · digests)` — 24.6s at the 512 sample cap on a warm
    /// fleet; pinned at 17.84% of scheduler CPU during a live 27s match cycle), and
    /// its signal only exists under a deep backlog — exactly the regime that the
    /// quadratic cost collapses to a 20-27s `do_try_match` cycle. It must therefore
    /// NOT run always-on in prod; an investigation enables it deliberately. See
    /// `.claude/audits/affinity-probe-slow-match-2026-07-06/`. The probe NEVER
    /// affects worker selection.
    ///
    /// Independent of the backend force-off: on the Redis/store backend the probe
    /// is ALWAYS disabled regardless of this flag (each sampled op would be a
    /// store round-trip on the match-cycle critical path); the effective gate is
    /// `this && !matches!(experimental_backend, Redis)`.
    #[serde(default = "default_pending_affinity_probe_enabled")]
    pub pending_affinity_probe_enabled: bool,

    /// (speculative-prefetch Increment 1) Master gate. Default: true (ENABLED,
    /// drift-proof — per user 2026-07-07: don't default features to OFF, it
    /// delays fixing their bugs). When enabled, a backlog of
    /// `speculative_prefetch_backlog_threshold` or more queued actions triggers
    /// the speculative pre-fetch for the highest-priority not-yet-eligible
    /// action. The flag remains an operational KILL-SWITCH: setting it `false`
    /// in config emits NO tag-15 `PrefetchInputs` signals, leaves the
    /// `prefetch_affinity` map empty, and makes the worker pre-fetch path
    /// unreachable — byte-identical to pre-Increment-1 (the off-path is pinned
    /// by the `t1_feature_gate_off_no_prefetch_inputs` regression test, which
    /// sets the flag `false` explicitly).
    #[serde(default = "default_true")]
    pub enable_speculative_prefetch: bool,

    /// (#specprefetch-rebind Stage B) Master gate for the TEMPORAL hold-vs-rebind
    /// decision. SEPARATE from `enable_speculative_prefetch` (Stage A). Default:
    /// true (ENABLED, drift-proof — per user 2026-07-07: don't default features
    /// to OFF, it delays fixing their bugs). When enabled, the matcher may
    /// return `None` (re-queue the op) instead of rebinding to a free-but-cold
    /// worker X when a P-SATURATED holder W of the op's `input_root_digest` (a
    /// holder that pcore-first excluded from the cache tiers for lack of P-headroom)
    /// is expected to REGAIN a P-slot (`T_wait_W < T_setup`) before X could
    /// re-construct the tree — trading a bounded queue wait for a saved tree
    /// construction on the critical path (design §2.3-v3). Gated internally on
    /// `p_gate_active` (the pcore-first gate ON and some worker with P-headroom), so
    /// it is a refinement of pcore-first, inert when that gate is off or lifted.
    /// The flag remains an operational KILL-SWITCH: setting it `false` in config
    /// makes the matcher NEVER hold — `inner_find_and_reserve_worker` assigns to
    /// the best available worker exactly as before (byte-identical; the
    /// `flag_off_no_hold` regression test, which sets the flag `false`
    /// explicitly, pins this).
    ///
    /// p99-regression risk on a BIMODAL fleet (a single global duration EWMA
    /// mis-estimates a long-compile worker as "about to free" → holds → p99
    /// regresses) is why the kill-switch exists: the `hold_regret` counter
    /// (design §2.3.6) sizes that risk, and if it fires in prod the operator
    /// sets the flag `false`. §1's "no measurement gate" governs the
    /// WORKLOAD-class decision (design §2.3.7).
    #[serde(default = "default_true")]
    pub enable_speculative_hold: bool,

    /// (speculative-prefetch Increment 1) Minimum queued-action backlog count
    /// that triggers a speculative `PrefetchInputs` signal. Conservative ship
    /// default = 3: "at least 3 actions are queued waiting for a slot" is a
    /// reliable signal that cold-input latency will dominate the next dispatch
    /// window. Lower values increase prefetch frequency (more bandwidth use);
    /// higher values reduce it (more critical-path cold fetches). 0 means
    /// "never trigger" when `enable_speculative_prefetch` is false (the gate
    /// collapses to `false && ...`); a non-zero default here is irrelevant
    /// when the master gate is off. The value 3 was chosen because at ≥3
    /// queued actions a single cold construct (~1364ms) is unlikely to drain
    /// the backlog before the next prefetch window opens.
    ///
    /// Numeric-constant note (reviewer rule): verify at this decl line, not
    /// from doc-comment. The literal value below is the ship default.
    #[serde(default = "default_speculative_prefetch_backlog_threshold")]
    pub speculative_prefetch_backlog_threshold: u64,

    /// (speculative-prefetch Increment 1) Time-to-live for a speculative pin
    /// in seconds. FORWARDED on the wire (`PrefetchInputs.ttl_s`) so this knob
    /// is LIVE end-to-end: the worker self-fires a pin release after
    /// `min(this, PIN_TIMEOUT_SECS=120)` seconds if the real StartAction has
    /// not arrived. Default 60s: long enough for typical scheduler latency
    /// under backlog, short enough to reclaim pins before the 120s sweep
    /// catches them (keeps the speculative-pin sub-budget from saturating on
    /// stale ops). A worker receiving `0` (e.g. an older scheduler) uses its
    /// own 60s default.
    ///
    /// MUST NOT be derived from `worker_timeout_s` (default 0 = disabled).
    #[serde(default = "default_speculative_prefetch_ttl_s")]
    pub speculative_prefetch_ttl_s: u64,

    /// (#sched-decision-trace) DIAGNOSTIC master switch for the scheduler
    /// dispatch-decision trace. When `true`, the worker matcher emits an
    /// INFO-level `tag = "sched_decision_trace"` dump at the dispatch decision
    /// point (`inner_find_and_reserve_worker`) showing, per capability-matched
    /// candidate worker, WHICH predicate held it back from taking a queued
    /// action: viability/pressure gate (quarantine, paused, indefinite-pin
    /// saturation, swap, disk), the `is_satisfied_by` Minimum-resource
    /// reservation (`memory_kb` / `cpu_count` / `disk_*`, decremented by
    /// `reduce_platform_properties` as jobs land), or the dispatch-count
    /// p-headroom gate. It is the ground-truth answer to "queued actions aren't
    /// placing on workers that look idle — which predicate is the limiter?".
    ///
    /// Rate-limited to at most one dump per second (a wall-clock token on the
    /// worker registry) and emitted only while a candidate set exists, so it
    /// cannot flood the log or dominate the match-cycle hot loop (the 2026-07-06
    /// hot-loop-fold trap): the flag is checked FIRST, before any formatting or
    /// clock read, so a flag-OFF dispatch pays only a bool load.
    ///
    /// MUST be INFO (not debug/trace): the release build pins
    /// `release_max_level_info`, so debug/trace are compiled out in prod and the
    /// dump would be dark — the same reason the sibling `p_headroom_gate_exclusion`
    /// probe is INFO.
    ///
    /// Default: false (OFF). This is a short-lived DIAGNOSTIC an operator turns
    /// ON via config to settle a live placement question, then OFF. It is
    /// OBSERVABILITY-ONLY: no dispatch decision changes when it is on.
    #[serde(default)]
    pub scheduler_decision_trace_enabled: bool,

    /// (#sched-cpu-first) Worker winner-RANKING policy. `CacheAffinityFirst`
    /// (the default) is byte-identical to the pre-`#sched-cpu-first` matcher;
    /// `CpuIdleFirst` ranks by (synthetic-compensated) P-core load ascending,
    /// re-admitting cache affinity only as a tiebreak when the whole fleet is
    /// P-saturated (see `PlacementMode`). Eligibility (viability, pressure
    /// gates, the P-headroom gate) is UNCHANGED in both modes — only the
    /// winner-ranking differs.
    ///
    /// Default: `CacheAffinityFirst` (no behavior change when omitted). This is
    /// NOT a dark-counter feature flag but a first-class alternative POLICY
    /// whose correctness is workload-dependent: defaulting `CpuIdleFirst` on
    /// would deterministically regress the known-dominant fetch-bound / mixed
    /// workload. The operator selects `CpuIdleFirst` per-scheduler (or per build
    /// phase) for a CPU-bound-labeled worker pool; selection = exercise (see the
    /// design §7 reconciliation).
    #[serde(default)]
    pub placement_mode: PlacementMode,

    /// (#sched-cpu-first §3) Synthetic P-load percentage points ADDED per
    /// assigned-but-not-yet-reported action when ranking under `CpuIdleFirst`.
    /// Bridges the report lag (`p_core_load_pct` trails assignment by up to the
    /// ~2.5s keepalive report interval): a just-assigned worker ranks WORSE
    /// until its next load report resets the snapshot, so a dispatch burst
    /// spreads across the fleet instead of piling on the one worker that still
    /// reports idle. Consulted ONLY in `CpuIdleFirst`.
    ///
    /// Default: 25 (≈ one P-core's worth at p_core_count = 4). Fixed for now;
    /// eventually driven by the per-task historical CPU from the profile map
    /// (Track-B). Soak-tunable — a too-large value over-suppresses a lightly-
    /// loaded worker (design F2); the effective value is clamped so total
    /// effective P-load never exceeds 100.
    #[serde(
        default = "default_cpu_first_synthetic_pct_per_task",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub cpu_first_synthetic_pct_per_task: u32,

    /// (#task-resource-profile Phase-3 §7 RAISE) Master gate for the RAISE
    /// enforcement direction: reserve `max(declared_memory_kb, profiled_tail)`
    /// instead of the client-declared `memory_kb`. OOM-SAFE (it only ever tightens
    /// the reservation upward toward the measured worst-case tail), so a link step
    /// whose client under-declares memory cannot over-pack a worker into an OOM.
    ///
    /// Default: false (OFF). Phase-3 is an ARCHITECTURAL change to the reservation
    /// ledger; it ships flag-gated OFF and is enabled per-workload once the observe
    /// metric (`down_opportunity_*` / `predicted_tail_over_actual_*`) shows the
    /// profiles are K-mature and accurate. When OFF the reservation is byte-identical
    /// to today's declared-only ledger (the observe metric still emits). The flag is
    /// the operational KILL-SWITCH, not a permanent resting state.
    ///
    /// ENABLE PRECONDITION (operator gate): deploy THIS binary to ALL workers before
    /// enabling. RAISE's starvation clamp is inert while a worker reports
    /// `total_memory_kb=0` (an un-upgraded worker), so a mixed fleet can reserve the
    /// profiled tail against a worker that never advertises capacity. Roll the binary
    /// fleet-wide first, then flip this per-workload.
    #[serde(default)]
    pub phase3_raise_enabled: bool,

    /// (#task-resource-profile Phase-3 §3 DOWN) Master gate for the DOWN
    /// statistical-OVERCOMMIT direction: reserve a CENTRAL estimate
    /// `p50 × (1 + margin(variance, tier))` clamped into
    /// `[declared / phase3_overcommit_max_factor, declared]` — BELOW the declared
    /// value — so more actions pack per worker. Safety rests on the worker
    /// `memory_gate` NAK backstop (re-queue on real pressure), NOT the prediction.
    ///
    /// Default: false (OFF) AND inert even when ON until
    /// `phase3_overcommit_max_factor > 1.0` (the floor `declared / 1.0 == declared`
    /// pins the reserve at declared). HARD-OFF until a future workload + the observe
    /// metric show real DOWN headroom and the backstop is proven. When OFF the
    /// reservation is byte-identical to today.
    ///
    /// ENABLE PRECONDITION (operator gate — hard): DOWN-overcommit's ONLY OOM backstop
    /// is the worker `memory_gate` NAK (re-queue on real pressure). That gate is
    /// DEFAULT-OFF and is currently DISABLED fleet-wide (#64: false-tripped). Do NOT set
    /// `phase3_overcommit_max_factor > 1.0` until the `memory_gate` is re-enabled AND
    /// soak-proven fleet-wide. Also deploy this binary to ALL workers first (see
    /// `phase3_raise_enabled`). With no backstop, overcommit reserves BELOW declared with
    /// nothing to catch a wrong-low prediction → OOM.
    #[serde(default)]
    pub phase3_down_overcommit_enabled: bool,

    /// (#task-resource-profile Phase-3 §6 floor) The profile-INDEPENDENT overcommit
    /// bound: the DOWN reserve is floored at `declared / phase3_overcommit_max_factor`,
    /// so a single wrong-low prediction can under-reserve by at most this factor
    /// (a config CONSTANT, NOT derived from the poisonable per-key histogram). It is
    /// ALSO the DOWN kill-dial: `1.0` (the default) makes the floor equal the declared
    /// value → DOWN is INERT even when `phase3_down_overcommit_enabled` is true; the
    /// operator dials it up (e.g. `2.0` = allow reserving down to half of declared)
    /// per workload as the observe metric justifies. Values `< 1.0` are clamped to
    /// `1.0` at use (a factor below 1 would RAISE the floor above declared, which is
    /// the RAISE direction's job). Has no effect on the RAISE direction.
    ///
    /// Numeric-constant note (reviewer rule): verify at the `default_phase3_overcommit_max_factor`
    /// declaration, not from this doc-comment. The literal there is the ship default.
    #[serde(default = "default_phase3_overcommit_max_factor")]
    pub phase3_overcommit_max_factor: f64,

    /// (#task-resource-profile Phase-3 §12) Filesystem path for persisting the
    /// resource-profile map (per-key sample-count histograms) across server restarts,
    /// so learned profiles survive a bounce without a re-warm tax. `None` (the default)
    /// = persistence OFF (the map starts empty and re-accumulates each boot). The data
    /// is ADVISORY and re-accumulates, so durability is NOT required: writes are atomic
    /// (tmp + rename) but NEVER fsync'd (ZFS `sync=disabled`; the no-fsync hard rule).
    ///
    /// The snapshot is a versioned wincode blob; a missing / corrupt / version-mismatch
    /// file logs a `warn` and starts FRESH — it NEVER panics or crashes startup.
    #[serde(default)]
    pub resource_profile_persist_path: Option<String>,

    /// (#task-resource-profile Phase-3 §12) Seconds between background snapshots of the
    /// resource-profile map when `resource_profile_persist_path` is set. Default 300
    /// (5 min). The snapshot CLONES the map under the `parking_lot` lock, RELEASES the
    /// lock, THEN serializes + writes off the lock — no I/O or `.await` is ever held
    /// across the map lock (the never-block-a-worker rule).
    #[serde(
        default = "default_resource_profile_persist_interval_secs",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub resource_profile_persist_interval_secs: u64,

    /// (#task-resource-profile Phase-3 §12 staleness) Maximum age (seconds) of a LOADED
    /// snapshot for which the DOWN-overcommit direction will trust a loaded profile for
    /// LOWERING a reservation. Default 604800 (7 days). OBSERVE + RAISE use loaded
    /// profiles freely (stale ⇒ over-reserve at worst, never OOM); DOWN additionally
    /// requires (a) the snapshot age < this AND (b) >=1 FRESH sample folded since load —
    /// so a shifted distribution raises variance and widens the DOWN margin
    /// automatically. Scopes the OOM risk of stale data to the lowering direction.
    #[serde(
        default = "default_resource_profile_persist_max_age_secs",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub resource_profile_persist_max_age_secs: u64,

    /// (#dag-criticality) Kill-switch for the DAG-from-history critical-path
    /// prioritization: reconstruct a target-level build DAG from persisted edge
    /// history and use a longest-path criticality score as a WITHIN-priority-band
    /// tie-break in the already-priority-sorted pending set. ADVISORY,
    /// correctness-neutral (a non-confident/absent node degrades to today's FIFO),
    /// and inert unless a backlog exists. Default `true` (shipped ON per the
    /// anti-dark-counter rule; the flag is an operational KILL-SWITCH — set `false`
    /// to disable without a redeploy). When off, the sort key is band-0 and its
    /// ordering is ORDER-EQUIVALENT to the pre-feature `[priority | inverted_insert_ts]`
    /// within a ~194-day insert window, and no edge/producer/duration state is
    /// accumulated. NOTE: the kill-switch restores the pre-feature ORDER, not the
    /// pre-feature BYTES — `new_with_criticality` unconditionally narrows the inverted
    /// insert timestamp from 32 to 24 bits (band 0 folded into the freed high 8 bits),
    /// so a band-0 key differs BYTE-wise from the old 32-bit-timestamp key while
    /// sorting identically until the (inverted) seconds wrap past 2^24 (~194 days).
    #[serde(default = "default_true")]
    pub dag_critical_path_enabled: bool,

    /// (#dag-criticality) Filesystem path for persisting the DAG edge store (stable-key
    /// edges + per-node duration histograms) across restarts, so the criticality
    /// substrate survives a bounce. `None` (default) = persistence OFF (re-accumulates
    /// each boot). SELF-CONTAINED — a SIBLING file with its OWN versioned header (magic
    /// "NLDG"), NOT the resource-profile snapshot. The data is ADVISORY and
    /// re-accumulates: writes are atomic (tmp + rename) but NEVER fsync'd (ZFS
    /// `sync=disabled`; the no-fsync hard rule). A missing / corrupt / version-mismatch
    /// file logs a `warn` and starts FRESH — never panics or crashes startup.
    #[serde(default)]
    pub dag_edge_store_persist_path: Option<String>,

    /// (#dag-criticality) Seconds between background recomputes of the criticality
    /// snapshot (Tarjan SCC + reverse-topo longest-path, O(V+E), sub-ms) and, when
    /// `dag_edge_store_persist_path` is set, the edge-store snapshot write. Default 300
    /// (5 min). The recompute snapshots edges + node weights under the strict-LEAF
    /// `parking_lot` lock, RELEASES the lock, then runs the DP + serializes + writes off
    /// the lock — no lock or I/O is ever held across `.await`.
    #[serde(
        default = "default_dag_recompute_interval_secs",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub dag_recompute_interval_secs: u64,
}

/// Manual `Default` that mirrors the serde defaults EXACTLY.
///
/// `#[derive(Default)]` would ignore the `#[serde(default = "fn")]`
/// attributes (they fire only on DEserialization), yielding
/// `load_byte_cost == 0`, `assume_core_count == 0`, and
/// `worker_match_logging_interval_s == 0` — none of which match a
/// deserialized-empty config. That divergence made every test that builds a
/// scheduler from `SimpleSpec::default()` run load-blind (`load_byte_cost == 0`
/// ⇒ zero `load_penalty` for all workers ⇒ the load-aware selection blend was
/// never exercised). This impl keeps `SimpleSpec::default()` byte-identical to
/// `serde_json5::from_str::<SimpleSpec>("{}")`; the equivalence is pinned by
/// `nativelink-config/tests/simple_spec_default_test.rs`. Each field below is
/// annotated with the serde default it must match — a drift red-fails that
/// test on the specific field.
impl Default for SimpleSpec {
    fn default() -> Self {
        Self {
            // No serde default → Option default (None).
            supported_platform_properties: None,
            // #[serde(default, deserialize_with)] → type default (0).
            retain_completed_for_s: 0,
            // #[serde(default, deserialize_with)] → type default (0).
            client_action_timeout_s: 0,
            // #[serde(default, deserialize_with)] → type default (0).
            worker_timeout_s: 0,
            // #[serde(default, deserialize_with)] → type default (0).
            max_action_executing_timeout_s: 0,
            // #[serde(default, deserialize_with)] → type default (0).
            max_job_retries: 0,
            // #[serde(default)] → WorkerAllocationStrategy::default().
            allocation_strategy: WorkerAllocationStrategy::default(),
            // No serde default → Option default (None).
            experimental_backend: None,
            // #[serde(default = "default_worker_match_logging_interval_s")] → 10.
            worker_match_logging_interval_s: default_worker_match_logging_interval_s(),
            // #[serde(default, deserialize_with)] → type default (0).
            max_matches_per_client_per_cycle: 0,
            // #[serde(default)] → Option default (None).
            cas_store: None,
            // #[serde(default = "default_load_byte_cost")] → 512 KiB.
            load_byte_cost: default_load_byte_cost(),
            // #[serde(default = "default_assume_core_count")] → 8.
            assume_core_count: default_assume_core_count(),
            // #[serde(default = "default_true")] → true (ENABLED by default,
            // drift-proof, per user 2026-07-07; kill-switch via config `false`).
            p_headroom_gate_enabled: true,
            // #[serde(default)] → type default (0) = override OFF (exact v1).
            p_idle_threshold_pct: 0,
            // #[serde(default = "default_p_headroom_override_factor")] → 2.
            // NOT the u32 type default (0), which would disable the override.
            p_headroom_override_factor: default_p_headroom_override_factor(),
            // #[serde(default)] → bool default (false) = P2P input prefetch OFF
            // (byte-identical to today until an operator enables it).
            enable_p2p_input_prefetch: false,
            // #[serde(default = "default_pending_affinity_probe_enabled")] → false
            // (probe is OPT-IN; an absent-in-config scheduler leaves it OFF).
            pending_affinity_probe_enabled: default_pending_affinity_probe_enabled(),
            // #[serde(default = "default_true")] → true = speculative prefetch
            // ENABLED by default (drift-proof, per user 2026-07-07; kill-switch
            // via config `false`, off-path pinned by
            // `t1_feature_gate_off_no_prefetch_inputs`).
            enable_speculative_prefetch: true,
            // (#specprefetch-rebind Stage B) #[serde(default = "default_true")]
            // → true = temporal hold gate ENABLED by default (drift-proof, per
            // user 2026-07-07; kill-switch via config `false`, off-path pinned by
            // `flag_off_no_hold`).
            enable_speculative_hold: true,
            // #[serde(default = "default_speculative_prefetch_backlog_threshold")] → 3.
            speculative_prefetch_backlog_threshold:
                default_speculative_prefetch_backlog_threshold(),
            // #[serde(default = "default_speculative_prefetch_ttl_s")] → 60.
            speculative_prefetch_ttl_s: default_speculative_prefetch_ttl_s(),
            // #[serde(default)] → bool default (false) = decision-trace diagnostic
            // OFF (observability-only; an operator turns it ON briefly to diagnose
            // a placement question, then OFF).
            scheduler_decision_trace_enabled: false,
            // #[serde(default)] → PlacementMode::default() = CacheAffinityFirst
            // (byte-identical to today until an operator selects CpuIdleFirst).
            placement_mode: PlacementMode::default(),
            // #[serde(default = "default_cpu_first_synthetic_pct_per_task")] → 25.
            cpu_first_synthetic_pct_per_task: default_cpu_first_synthetic_pct_per_task(),
            // (#task-resource-profile Phase-3) #[serde(default)] → bool false =
            // RAISE enforcement OFF (byte-identical declared-only ledger until an
            // operator enables it; the observe metric still emits).
            phase3_raise_enabled: false,
            // #[serde(default)] → bool false = DOWN overcommit OFF (and inert until
            // phase3_overcommit_max_factor > 1.0 even if flipped on).
            phase3_down_overcommit_enabled: false,
            // #[serde(default = "default_phase3_overcommit_max_factor")] → 1.0 =
            // DOWN inert (floor == declared). NOT the f64 type default (0.0), which
            // would make the floor `declared/0` = +inf and invert the clamp.
            phase3_overcommit_max_factor: default_phase3_overcommit_max_factor(),
            // #[serde(default)] → Option default (None) = profile persistence OFF.
            resource_profile_persist_path: None,
            // #[serde(default = "...")] → 300s.
            resource_profile_persist_interval_secs:
                default_resource_profile_persist_interval_secs(),
            // #[serde(default = "...")] → 604800s (7d).
            resource_profile_persist_max_age_secs:
                default_resource_profile_persist_max_age_secs(),
            // (#dag-criticality) #[serde(default = "default_true")] → true (shipped ON,
            // kill-switch via config `false`; advisory + correctness-neutral).
            dag_critical_path_enabled: true,
            // (#dag-criticality) #[serde(default)] → Option default (None) = edge-store
            // persistence OFF (re-accumulates each boot).
            dag_edge_store_persist_path: None,
            // (#dag-criticality) #[serde(default = "...")] → 300s.
            dag_recompute_interval_secs: default_dag_recompute_interval_secs(),
        }
    }
}

/// (#dag-criticality) Default background recompute/persist interval (300s = 5 min). MUST
/// be a named default fn (a bare `#[serde(default)]` u64 0 would spin the recompute task
/// with no delay). Numeric-constant rule: this literal is authoritative.
pub const fn default_dag_recompute_interval_secs() -> u64 {
    300
}

/// (#task-resource-profile Phase-3 §12) Default background-snapshot interval (300s =
/// 5 min). MUST be a named default fn (a bare `#[serde(default)]` u64 0 would spin the
/// snapshot task with no delay). `pub` for the single-source-of-truth reason. Numeric-
/// constant rule: this literal is authoritative.
pub const fn default_resource_profile_persist_interval_secs() -> u64 {
    300
}

/// (#task-resource-profile Phase-3 §12) Default max loaded-snapshot age the DOWN
/// direction will trust for lowering (604800s = 7 days). `pub` single-source-of-truth.
/// Numeric-constant rule: this literal is authoritative.
pub const fn default_resource_profile_persist_max_age_secs() -> u64 {
    604_800
}

/// (#task-resource-profile Phase-3 §6) Default profile-independent overcommit
/// factor: `1.0` = DOWN inert (the floor `declared / 1.0 == declared` pins the
/// reserve at the declared value even when `phase3_down_overcommit_enabled` is on).
/// MUST be a named default fn (not a bare `#[serde(default)]`, whose f64 `0.0`
/// would make the floor `declared / 0.0 == +inf` and break the clamp). An operator
/// dials it up per workload (e.g. `2.0` = reserve down to half of declared). `pub`
/// for the single-source-of-truth reason as the sibling defaults.
///
/// Numeric-constant rule: the literal below is the authoritative ship default.
pub fn default_phase3_overcommit_max_factor() -> f64 {
    1.0
}

/// (#sched-cpu-first §3) Default synthetic P-load per assigned-but-unreported
/// action under `CpuIdleFirst` (25 pct ≈ one P-core at p_core_count = 4). Named
/// default fn (not a bare `#[serde(default)]` u32 0, which would disable the
/// anti-pile synthetic bridge entirely). `pub` so any no-config constructor
/// path sources the SAME value (single source of truth — no drift).
pub const fn default_cpu_first_synthetic_pct_per_task() -> u32 {
    25
}

/// Serde default of `true` for scheduler feature flags that are ON by default.
/// Used by `p_headroom_gate_enabled`, `enable_speculative_prefetch`, and
/// `enable_speculative_hold` so that omitting the flag from a config leaves the
/// feature ENABLED (config drift can no longer silently dark it). The flag stays
/// a kill-switch: setting it `false` in config still disables the feature.
/// Ship features ON — a default-off flag never runs, so its bugs never surface
/// and its counters are dark (per user 2026-07-07).
const fn default_true() -> bool {
    true
}

/// (#sched-blend) Default cache-vs-load crossover anchor (512 KiB).
/// PROVISIONAL — see `SimpleSpec::load_byte_cost`; soak-select before deploy.
/// `pub` so the scheduler's no-config `ApiWorkerScheduler::new` path sources
/// the SAME value (single source of truth — no hardcoded 3rd copy to drift).
pub const fn default_load_byte_cost() -> u64 {
    512 * 1024
}

/// (#sched-blend) Default substituted P-core count for count-less workers.
/// `pub` for the same single-source-of-truth reason as `default_load_byte_cost`.
pub const fn default_assume_core_count() -> u32 {
    8
}

/// (#sched M1 rebalance v2) Default in-flight ceiling multiplier for the
/// bounded p_load override. MUST be a named default fn (not a bare
/// `#[serde(default)]`, which would yield 0 → `p_core_count * 0 == 0` ceiling
/// → the override never fires). See `SimpleSpec::p_headroom_override_factor`.
/// `pub` for the same single-source-of-truth reason as `default_load_byte_cost`.
pub const fn default_p_headroom_override_factor() -> u32 {
    2
}

/// (#sched-affinity-probe) Default for `pending_affinity_probe_enabled`: `false`
/// (OPT-IN). The probe is observability-only R&D instrumentation that nothing
/// auto-consumes, its kernel is quadratic, and its signal only exists under the
/// deep backlog that same quadratic cost collapses (20-27s `do_try_match`), so it
/// must NOT run always-on in prod; an investigation enables it deliberately by
/// setting the flag `true` in the scheduler config (user decision 2026-07-06). A
/// bare `#[serde(default)]` on a `bool` also yields `false`, but keeping a named
/// default fn holds the single-source-of-truth shape of the sibling defaults and
/// documents the intent. `pub` for the same single-source-of-truth reason.
///
/// Numeric-constant rule: the literal below is the authoritative default.
pub const fn default_pending_affinity_probe_enabled() -> bool {
    false
}

/// (speculative-prefetch Increment 1) Conservative ship default for the
/// speculative prefetch backlog trigger threshold. 3 queued actions is a
/// reliable signal that cold-input latency will dominate the next dispatch
/// window; at ≥3 queued actions a single cold construct (~1364ms) is unlikely
/// to drain the backlog before the next prefetch window opens.
///
/// Numeric-constant rule: the literal below is the authoritative value;
/// doc-comments and commit messages may drift without updating the constant.
pub const fn default_speculative_prefetch_backlog_threshold() -> u64 {
    3
}

/// (speculative-prefetch Increment 1) Default TTL for speculative pins (60s).
/// Long enough for typical scheduler latency under backlog, short enough to
/// reclaim pins before the 120s `PIN_TIMEOUT_SECS` sweep catches them.
/// The effective lifetime is `min(speculative_prefetch_ttl_s, PIN_TIMEOUT_SECS)`.
///
/// Numeric-constant rule: the literal below is the authoritative value.
pub const fn default_speculative_prefetch_ttl_s() -> u64 {
    60
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ExperimentalSimpleSchedulerBackend {
    /// Use an in-memory store for the scheduler.
    Memory,
    /// Use a redis store for the scheduler.
    Redis(ExperimentalRedisSchedulerBackend),
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExperimentalRedisSchedulerBackend {
    /// A reference to the redis store to use for the scheduler.
    /// Note: This MUST resolve to a `RedisSpec`.
    pub redis_store: StoreRefName,
}

/// A scheduler that simply forwards requests to an upstream scheduler.  This
/// is useful to use when doing some kind of local action cache or CAS away from
/// the main cluster of workers.  In general, it's more efficient to point the
/// build at the main scheduler directly though.
#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct GrpcSpec {
    /// The upstream scheduler to forward requests to.
    pub endpoint: GrpcEndpoint,

    /// Retry configuration to use when a network request fails.
    #[serde(default)]
    pub retry: Retry,

    /// Limit the number of simultaneous upstream requests to this many.  A
    /// value of zero is treated as unlimited.  If the limit is reached the
    /// request is queued.
    /// Default: unlimited
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_concurrent_requests: usize,

    /// The number of connections to make to each specified endpoint to balance
    /// the load over multiple TCP connections.
    /// Default: 1.
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub connections_per_endpoint: usize,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CacheLookupSpec {
    /// The reference to the action cache store used to return cached
    /// actions from rather than running them again.
    /// To prevent unintended issues, this store should probably be a `CompletenessCheckingSpec`.
    pub ac_store: StoreRefName,

    /// The nested scheduler to use if cache lookup fails.
    pub scheduler: Box<SchedulerSpec>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PlatformPropertyAddition {
    /// The name of the property to add.
    pub name: String,
    /// The value to assign to the property.
    pub value: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PlatformPropertyReplacement {
    /// The name of the property to replace.
    pub name: String,
    /// The the value to match against, if unset then any instance matches.
    #[serde(default)]
    pub value: Option<String>,
    /// The new name of the property.
    pub new_name: String,
    /// The value to assign to the property, if unset will remain the same.
    #[serde(default)]
    pub new_value: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum PropertyModification {
    /// Add a property to the action properties.
    Add(PlatformPropertyAddition),
    /// Remove a named property from the action.
    Remove(String),
    /// If a property is found, then replace it with another one.
    Replace(PlatformPropertyReplacement),
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PropertyModifierSpec {
    /// A list of modifications to perform to incoming actions for the nested
    /// scheduler.  These are performed in order and blindly, so removing a
    /// property that doesn't exist is fine and overwriting an existing property
    /// is also fine.  If adding properties that do not exist in the nested
    /// scheduler is not supported and will likely cause unexpected behaviour.
    pub modifications: Vec<PropertyModification>,

    /// The nested scheduler to use after modifying the properties.
    pub scheduler: Box<SchedulerSpec>,
}

const fn default_historical_resource_refresh_interval_s() -> u64 {
    30
}

fn default_historical_resource_cpu_property_name() -> String {
    "cpu_count".to_string()
}

fn default_historical_resource_memory_property_name() -> String {
    "memory_kb".to_string()
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HistoricalResourceSpec {
    /// JSON file containing historical resource hints keyed by Bazel
    /// `RequestMetadata` `target_id` and/or `action_mnemonic`.
    ///
    /// Supported file shapes:
    /// ```json
    /// [
    ///   { "target_id": "//pkg:test", "action_mnemonic": "TestRunner", "cpu_count": 2, "memory_kb": 12582912 }
    /// ]
    /// ```
    /// or:
    /// ```json
    /// { "hints": [ ... ] }
    /// ```
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub hints_file: String,

    /// Reload interval for `hints_file`. Set to 0 to load once.
    /// Default: 30 seconds
    #[serde(
        default = "default_historical_resource_refresh_interval_s",
        deserialize_with = "convert_duration_with_shellexpand"
    )]
    pub refresh_interval_s: u64,

    /// Platform property name used for CPU minimums.
    /// Default: `cpu_count`
    #[serde(
        default = "default_historical_resource_cpu_property_name",
        deserialize_with = "convert_string_with_shellexpand"
    )]
    pub cpu_property_name: String,

    /// Platform property name used for memory minimums, expressed in KiB.
    /// Default: `memory_kb`
    #[serde(
        default = "default_historical_resource_memory_property_name",
        deserialize_with = "convert_string_with_shellexpand"
    )]
    pub memory_property_name: String,

    /// The nested scheduler to use after applying resource hints.
    pub scheduler: Box<SchedulerSpec>,
}
