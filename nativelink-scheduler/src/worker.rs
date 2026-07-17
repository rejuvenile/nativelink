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

use core::hash::{Hash, Hasher};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_error::{Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, StartExecute, UpdateForWorker, update_for_worker,
};
use nativelink_util::action_messages::{ActionInfo, OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::metrics_utils::{AsyncCounterWrapper, CounterWithTime, FuncCounterWrapper};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use tokio::sync::mpsc::UnboundedSender;

use crate::resource_profile::ProfileTier;

pub type WorkerTimestamp = u64;

/// Represents the action info and the platform properties of the action.
/// These platform properties have the type of the properties as well as
/// the value of the properties, unlike `ActionInfo`, which only has the
/// string value of the properties.
#[derive(Clone, Debug, MetricsComponent)]
pub struct ActionInfoWithProps {
    /// The action info of the action.
    #[metric(group = "action_info")]
    pub inner: Arc<ActionInfo>,
    /// The platform properties of the action.
    #[metric(group = "platform_properties")]
    pub platform_properties: PlatformProperties,
    /// Origin metadata used when publishing scheduler-side telemetry for this action.
    pub origin_metadata: OriginMetadata,
    /// `OriginEvent` id for the `scheduler_start_execute` request.
    pub scheduler_start_execute_event_id: Option<String>,
}

/// Notifications to send worker about a requested state change.
#[derive(Debug)]
pub enum WorkerUpdate {
    /// Requests that the worker begin executing this action.
    RunAction(Box<(OperationId, ActionInfoWithProps)>),

    /// Request that the worker is no longer in the pool and may discard any jobs.
    Disconnect,
}

#[derive(Debug, MetricsComponent)]
pub struct PendingActionInfoData {
    #[metric]
    pub action_info: ActionInfoWithProps,

    /// (#specprefetch-rebind Stage C) Monotonic-ish wall-clock instant at which
    /// this op was RESERVED to the worker (the Executing transition), stamped
    /// from the scheduler's injected clock (`SystemTime::now` in prod,
    /// `MockInstantWrapped`'s mock-clock in tests) at the single reserve point
    /// (`ApiWorkerSchedulerImpl::prepare_worker_run_action`). `elapsed = now −
    /// start` is the op's live in-flight time; it feeds `worker_time_to_free`
    /// (Stage B's `T_wait_W`) and the duration EWMA on completion.
    ///
    /// `None` means "not stamped": the reconnect-notify `Worker::run_action`
    /// insert path (dead in production — `notify_update` is only ever called
    /// with `WorkerUpdate::Disconnect`) carries no clock, so a record inserted
    /// there contributes ZERO to `worker_time_to_free` and does not update the
    /// EWMA. This is the correct degenerate behavior: an un-timed record is
    /// simply invisible to the temporal estimate.
    pub exec_start_time: Option<SystemTime>,

    /// (#task-resource-profile Phase-2c) OBSERVE-ONLY dispatch-time memory
    /// prediction, stashed here so the completion path can do a TRUE out-of-sample
    /// leave-one-out accuracy check: the action's ACTUAL peak measured against the
    /// tail that stood AT THIS ACTION'S DISPATCH — NOT re-derived at completion, so
    /// same-key samples that folded in AFTER this action was dispatched can never
    /// leak in (removing the hindsight bias the cadre flagged).
    ///
    /// `Some((tier, tail_kb, prior_samples))` = the tail-aware memory reservation the
    /// enforce phase WOULD have stood at dispatch, peeked (non-recency-bumping) from
    /// the profile-so-far by `ApiWorkerScheduler::find_and_reserve_worker` via the
    /// fine→coarse hierarchical lookup. `tier` records WHICH tier resolved
    /// ([`ProfileTier::Fine`] or [`ProfileTier::Coarse`]) so the completion-path
    /// accuracy classification (and the eventual Phase-3 down-override, which must
    /// NOT trust a coarse tail) can treat a coarse prediction conservatively.
    /// `None` = no profile existed at EITHER tier at dispatch (map still warming,
    /// absent Bazel baggage, or the dead reconnect-notify insert path).
    ///
    /// It lives IN this per-op record, so it is auto-cleaned on EVERY terminal path
    /// (`complete_action`, `inner_unreserve_worker`, `immediate_evict_worker` drain,
    /// `remove_worker`) with zero bespoke cleanup — no side map, no leak surface.
    /// Read (never enforced) at completion by `record_action_resource_usage`.
    pub dispatch_memory_prediction: Option<(ProfileTier, u64, u64)>,
}

/// (#sched-blend, security S1) Upper bound on the worker-reported P/E
/// logical-CPU counts, applied at the connect-frame ingest seam
/// (`worker_api_server::inner_connect_worker`). The counts feed the
/// continuous cache-vs-load blend's *penalty denominator*: a worker
/// reporting a huge count would compute near-infinite free capacity →
/// zero load penalty *regardless of its real load* → it would win every
/// cache-tied selection and become a placement monopoly (and feed the
/// widen-before-multiply overflow surface). Workers are trusted, so this
/// is robustness defence-in-depth (a sysctl glitch / future-chip
/// mis-report / config typo), symmetric with the existing hello-frame
/// string bound (`MAX_HELLO_STRING_LEN = 256`). `1024` is generous for
/// any real machine (largest current servers are ~256 logical CPUs).
pub const MAX_PLAUSIBLE_CORES: u32 = 1024;

/// Represents a connection to a worker and used as the medium to
/// interact with the worker from the client/scheduler.
#[derive(Debug, MetricsComponent)]
pub struct Worker {
    /// Unique identifier of the worker.
    #[metric(help = "The unique identifier of the worker.")]
    pub id: WorkerId,

    /// Properties that describe the capabilities of this worker.
    #[metric(group = "platform_properties")]
    pub platform_properties: PlatformProperties,

    /// Channel to send commands from scheduler to worker.
    pub tx: UnboundedSender<UpdateForWorker>,

    /// The action info of the running actions on the worker.
    #[metric(group = "running_action_infos")]
    pub running_action_infos: HashMap<OperationId, PendingActionInfoData>,

    /// If the properties were restored already then it's added to this set.
    pub restored_platform_properties: HashSet<OperationId>,

    /// Timestamp of last time this worker had been communicated with.
    // Warning: Do not update this timestamp without updating the placement of the worker in
    // the LRUCache in the Workers struct.
    #[metric(help = "Last time this worker was communicated with.")]
    pub last_update_timestamp: WorkerTimestamp,

    /// Whether the worker rejected the last action due to back pressure.
    #[metric(help = "If the worker is paused.")]
    pub is_paused: bool,

    /// Whether the pause was caused by explicit worker backpressure
    /// (ResourceExhausted) as opposed to a capacity check. When true,
    /// the scheduler should not auto-clear is_paused based on capacity
    /// alone — it should wait for the worker to complete an action.
    pub paused_due_to_backpressure: bool,

    /// Whether the worker is draining.
    #[metric(help = "If the worker is draining.")]
    pub is_draining: bool,

    /// Maximum inflight tasks for this worker (or 0 for unlimited)
    #[metric(help = "Maximum inflight tasks for this worker (or 0 for unlimited)")]
    pub max_inflight_tasks: u64,

    /// When this worker entered quarantine (i.e. missed keepalive for
    /// > worker_timeout but < 2*worker_timeout). While quarantined the
    /// worker will not receive new actions but is not yet evicted.
    /// Reset to `None` when a keepalive is received.
    pub quarantined_at: Option<SystemTime>,

    /// The worker's CAS gRPC endpoint for peer blob serving.
    /// Empty if the worker does not support peer serving.
    #[metric(help = "The worker's CAS endpoint for peer blob sharing.")]
    pub cas_endpoint: String,

    /// CPU utilization percentage (0-100) reported by the worker, sampled every 100ms.
    /// 0 means unknown (worker hasn't reported load yet).
    #[metric(help = "CPU load percentage reported by the worker.")]
    pub cpu_load_pct: u32,

    /// (#sched-zeroload) Whether this worker has EVER reported a load reading
    /// (via `update_worker_load`). `false` at construction and until the first
    /// report. The load fields default to `(0,0,0)`, which in `capacity_score`
    /// is INDISTINGUISHABLE from a genuinely-idle "all-zero" reading: both read
    /// as 100% free → max `weighted_free` → ZERO `load_penalty` → the worker
    /// wins every Tier-1 min-load tie. This flag lets the selector treat a
    /// NEVER-reported worker as fully busy (max penalty) — so it does NOT win a
    /// min-load tie over a worker with known spare capacity — while a worker
    /// that HAS reported a genuine all-zero reading (`has_reported_load == true`,
    /// load `(0,0,0)`) stays the most-free worker and remains selectable.
    /// Set to `true` (never back to `false`) the first time the worker reports.
    pub has_reported_load: bool,

    /// (#sched-cpu-first §3) Snapshot of `running_action_infos.len()` taken at
    /// the last load report (`update_worker_load`). Under `CpuIdleFirst`
    /// placement the ranker adds SYNTHETIC P-load for the actions assigned SINCE
    /// that snapshot (`running_action_infos.len() - running_at_last_load_report`,
    /// `saturating_sub`) to bridge the ~2.5s report lag, so a dispatch burst does
    /// not pile onto the one worker still reporting idle. Reset to the current
    /// in-flight count on every load report (the report has "caught up"); a
    /// completion-draining worker decays to synthetic 0 via `saturating_sub`.
    /// `0` at construction and until the first report (no synthetic bias yet).
    /// Read/written ONLY under the worker-pool write lock. Inert under
    /// `CacheAffinityFirst` (the default) — no ranker consults it there.
    #[metric(help = "running_action_infos.len() snapshot at the last load report (CpuIdleFirst synthetic-load base).")]
    pub running_at_last_load_report: usize,

    /// Performance-core CPU utilization (0-100). 0 means unknown.
    #[metric(help = "P-core load percentage reported by the worker.")]
    pub p_core_load_pct: u32,

    /// Efficiency-core CPU utilization (0-100). 0 means unknown.
    /// 100 on CPUs without E-cores.
    #[metric(help = "E-core load percentage reported by the worker.")]
    pub e_core_load_pct: u32,

    /// (#obs-tuning) OBSERVABILITY-ONLY. The worker's last-gossiped DECAYED p95
    /// COLD dir-cache construct latency in milliseconds — the `construct_fetch_p95`
    /// estimator (a time-decayed fixed-bucket histogram, `o11_probes.rs`) from the
    /// worker's global `DirCacheCounters`, carried on every `BlobsAvailable`
    /// heartbeat (chunk-0 scalar). This is the real cold-tree reconstruct cost
    /// `T_SETUP` should eventually equal (biased to the expensive tail); it is
    /// currently only LOGGED (periodic `tag = "worker_construct_latency"`) for
    /// tuning — the hold gate does NOT consume it, so this field changes NO
    /// scheduling decision. `0` is AMBIGUOUS (three meanings): the worker reported
    /// no cold constructs yet (empty histogram); OR is a pre-#obs-tuning worker
    /// (proto3 default); OR — added by the wall-clock-decay follow-up — an IDLE
    /// worker's recent constructs all decayed below the mass sentinel. A future
    /// programmatic `T_SETUP` consumer MUST map `0` → the `T_SETUP` floor ("no
    /// signal → use the constant"), NEVER "0 ms construct cost" (that would invert
    /// the hold-vs-rebind decision for every idle-then-cold worker).
    #[metric(help = "Worker-gossiped decayed-p95 cold dir-cache construct latency (ms); T_SETUP tuning input, LOGGED only.")]
    pub construct_latency_ms_p95: u32,

    /// (#sched-blend) Number of performance (P) logical CPUs the worker
    /// reported on its connect hello frame. Static for the worker's
    /// lifetime (logical CPU topology does not change at runtime), so it
    /// rides the connect frame, not the per-tick load path. `0` means the
    /// worker did not report a count (legacy / Linux / Intel Mac); the
    /// continuous cache-vs-load blend substitutes `assume_core_count` for
    /// the absolute-capacity math. Clamped at ingest to
    /// `MAX_PLAUSIBLE_CORES` (`worker_api_server`), so an over-report
    /// cannot zero its load penalty and monopolize placement.
    #[metric(help = "Number of P logical CPUs reported by the worker (0 = unknown).")]
    pub p_core_count: u32,

    /// (#sched-blend) Number of efficiency (E) logical CPUs the worker
    /// reported on its connect hello frame. `0` means none / unknown — the
    /// blend's E-capacity term is keyed off this count (not `e_core_load_pct`,
    /// whose `100` is ambiguous between "no E-cores" and "E saturated"), so
    /// `e_core_count == 0` contributes zero free E capacity by construction.
    /// Clamped at ingest to `MAX_PLAUSIBLE_CORES`.
    #[metric(help = "Number of E logical CPUs reported by the worker (0 = none/unknown).")]
    pub e_core_count: u32,

    /// (#task-resource-profile Phase-3 §6) Total physical RAM this worker reported
    /// on its connect hello frame, in KiB. Static for the worker's lifetime (RAM
    /// does not change at runtime), so it rides the connect frame, not the per-tick
    /// load path. `0` means the worker did not report it (legacy worker, or a
    /// platform where the RAM query failed) — the Phase-3 RAISE starvation clamp
    /// treats `0` as "unknown / contributes no capacity ceiling" so a non-reporting
    /// worker never becomes the clamp's `max_worker_total_memory_kb`. NEEDED because
    /// `platform_properties["memory_kb"]` holds only the REMAINING reservation
    /// budget (decremented in place by `reduce_platform_properties`), not the total.
    #[metric(help = "Total physical RAM reported by the worker (KiB); 0 = unknown.")]
    pub total_memory_kb: u64,

    /// (FL-681 re-saturation gate) Whether the worker's local CAS
    /// FilesystemStore reported its indefinite-pin cap saturated in its last
    /// `BlobsAvailable` heartbeat. While `true`, the matcher
    /// (`inner_find_and_reserve_worker`) skips this worker for new actions so a
    /// saturated-but-idle worker is not re-dispatched into the worker-NAK →
    /// re-queue → re-dispatch spin (the admission-gate pause at `update_action`
    /// is conditional on the worker having OTHER in-flight actions, which a
    /// saturated-but-idle worker does not). PROACTIVE matcher backpressure; the
    /// worker-side admission NAK remains the last-resort backstop for the
    /// report-staleness window between heartbeats. `false` for workers that
    /// never report saturation (pre-FL-681 / uncapped stores).
    #[metric(help = "If the worker's indefinite-pin cap is reported saturated.")]
    pub indefinite_pin_saturated: bool,

    /// (#37 rev-4 memory-pressure gate) Whether the worker reported
    /// sustained host memory pressure in its last `BlobsAvailable` heartbeat
    /// (the coarse 1-bit verdict: free-floor breached OR re-fault EWMA over
    /// threshold). While `true`, the matcher PROACTIVELY skips this worker
    /// for new actions (mirrors `indefinite_pin_saturated`) so a
    /// pressured-but-idle worker is not selected and then forced to
    /// worker-side NAK → re-queue → re-dispatch spin. ADVISORY ONLY: the
    /// authoritative gate is the worker's local atomic (the StartAction
    /// NAK); this is the proactive optimization. `false` for workers that
    /// never report pressure (pre-#37 / gate disabled / sampler stale).
    /// The fleet fail-open (`api_worker_scheduler`) overrides this skip
    /// when EVERY candidate is memory-gated, degrading to least-pressured
    /// placement rather than a wedge.
    #[metric(help = "If the worker reported sustained host memory pressure.")]
    pub swap_pressured: bool,

    /// (#task-memgate-twosignal) The worker's last-reported compressor-CHURN
    /// scalar — `min(compress_ewma, decompress_ewma)` in events/sec (the
    /// re-keyed wire field 20; it FORMERLY carried MiB-below-the-free-floor,
    /// hence the historical proto comment). Used ONLY to rank the
    /// least-pressured worker in the fleet fail-open (when all candidates are
    /// memory-gated) and to feed the reactive overcommit churn-throttle.
    /// Observability + tie-break; NOT a gate input on its own. `0` = unknown /
    /// no pressure reported. Higher = more thrashing, so the `min_by_key`
    /// fail-open ranking selects the least-thrashing worker.
    #[metric(
        help = "Worker-reported compressor-churn scalar (min(compress,decompress) EWMA, events/sec)."
    )]
    pub mem_pressure_churn_scalar: u32,

    /// (F4) Whether the worker reported physical disk pressure on its
    /// CAS/work_directory volume in its last `BlobsAvailable` heartbeat (free
    /// bytes below `DISK_FREE_FLOOR_BYTES`). While `true`, the matcher
    /// PROACTIVELY skips this worker for new actions (mirrors
    /// `swap_pressured` / `indefinite_pin_saturated`) so a disk-pressured-but-
    /// idle worker is not selected and then forced to worker-side NAK →
    /// re-queue → re-dispatch spin. ADVISORY ONLY: the authoritative gate is
    /// the worker's local atomic + statvfs fallback (the StartAction NAK).
    /// `false` for workers that never report disk pressure (pre-F4 / sampler
    /// stale → fail-open). The fleet fail-open (`api_worker_scheduler`)
    /// overrides this skip when EVERY candidate is disk-gated, degrading to
    /// least-pressured (most-free-bytes) placement rather than a wedge.
    #[metric(help = "If the worker reported physical disk pressure on its CAS volume.")]
    pub disk_pressured: bool,

    /// (F4) The worker's last-reported free bytes on its CAS/work_directory
    /// volume, used ONLY to rank the least-pressured worker in the disk fleet
    /// fail-open (when all candidates are disk-gated). Observability +
    /// tie-break; NOT a gate input on its own. `0` = unknown / volume full.
    /// Higher = MORE free = LESS pressured, so the `max_by_key` fail-open
    /// ranking selects the worker with the most headroom (the inverse of the
    /// memory `min_by_key` shortfall ranking).
    #[metric(help = "Worker-reported free bytes on its CAS volume (fail-open ranking).")]
    pub available_disk_bytes: u64,

    /// Digests of input root directories cached in the worker's directory cache.
    /// The scheduler gives routing preference to workers that already have the
    /// action's input_root_digest cached.
    pub cached_directory_digests: HashSet<DigestInfo>,

    /// All subtree digests (roots + subtrees) from the worker's directory cache.
    /// Updated via delta encoding from BlobsAvailableNotification.
    /// The scheduler uses this for subtree-aware scheduling: checking whether
    /// the action's input_root_digest appears as ANY subtree in any cached entry.
    pub cached_subtree_digests: HashSet<DigestInfo>,

    /// Stats about the worker.
    #[metric]
    metrics: Arc<Metrics>,
}

fn send_msg_to_worker(
    tx: &UnboundedSender<UpdateForWorker>,
    msg: update_for_worker::Update,
) -> Result<(), Error> {
    tx.send(UpdateForWorker { update: Some(msg) })
        .map_err(|err| Error::from_std_err(Code::Internal, &err).append("Worker disconnected"))
}

/// Reduces the platform properties available on the worker based on the platform properties provided.
/// This is used because we allow more than 1 job to run on a worker at a time, and this is how the
/// scheduler knows if more jobs can run on a given worker.
pub(crate) fn reduce_platform_properties(
    parent_props: &mut PlatformProperties,
    reduction_props: &PlatformProperties,
) {
    debug_assert!(reduction_props.is_satisfied_by(parent_props, false));
    for (property, prop_value) in &reduction_props.properties {
        if let PlatformPropertyValue::Minimum(value) = prop_value {
            let worker_props = &mut parent_props.properties;
            if let &mut PlatformPropertyValue::Minimum(worker_value) =
                &mut worker_props.get_mut(property).unwrap()
            {
                *worker_value -= value;
            }
        }
    }
}

impl Worker {
    pub fn new(
        id: WorkerId,
        platform_properties: PlatformProperties,
        tx: UnboundedSender<UpdateForWorker>,
        timestamp: WorkerTimestamp,
        max_inflight_tasks: u64,
    ) -> Self {
        // (#sched-blend) The no-endpoint path has no connect frame, so core
        // counts are unknown (`0,0`) → the blend uses the `assume_core_count`
        // fallback. Tests set real counts via `set_core_counts` (below).
        Self::new_with_cas_endpoint(
            id,
            platform_properties,
            tx,
            timestamp,
            max_inflight_tasks,
            String::new(),
            0,
            0,
            0,
        )
    }

    pub fn new_with_cas_endpoint(
        id: WorkerId,
        platform_properties: PlatformProperties,
        tx: UnboundedSender<UpdateForWorker>,
        timestamp: WorkerTimestamp,
        max_inflight_tasks: u64,
        cas_endpoint: String,
        p_core_count: u32,
        e_core_count: u32,
        total_memory_kb: u64,
    ) -> Self {
        Self {
            id,
            platform_properties,
            tx,
            running_action_infos: HashMap::new(),
            restored_platform_properties: HashSet::new(),
            last_update_timestamp: timestamp,
            is_paused: false,
            paused_due_to_backpressure: false,
            is_draining: false,
            max_inflight_tasks,
            quarantined_at: None,
            cas_endpoint,
            cpu_load_pct: 0,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            // (#obs-tuning) OBSERVABILITY-ONLY: 0 until the worker gossips a
            // cold-construct latency; never a scheduling input.
            construct_latency_ms_p95: 0,
            has_reported_load: false,
            // (#sched-cpu-first §3) No load report yet → no synthetic base.
            running_at_last_load_report: 0,
            p_core_count,
            e_core_count,
            total_memory_kb,
            indefinite_pin_saturated: false,
            swap_pressured: false,
            mem_pressure_churn_scalar: 0,
            disk_pressured: false,
            available_disk_bytes: 0,
            cached_directory_digests: HashSet::new(),
            cached_subtree_digests: HashSet::new(),
            metrics: Arc::new(Metrics {
                connected_timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                actions_completed: CounterWithTime::default(),
                run_action: AsyncCounterWrapper::default(),
                keep_alive: FuncCounterWrapper::default(),
                notify_disconnect: CounterWithTime::default(),
            }),
        }
    }

    /// (#sched-blend) Test-only setter for the P/E logical-CPU counts.
    /// In production the counts arrive only on the connect hello frame
    /// (`new_with_cas_endpoint`); the no-endpoint test path (`new`)
    /// defaults them to `(0,0)`. Heterogeneous-fleet tests (e.g. 96-core
    /// vs 2-core) need real counts to exercise the absolute-capacity blend,
    /// so they call this after `add_worker_*`.
    #[cfg(test)]
    pub fn set_core_counts(&mut self, p_core_count: u32, e_core_count: u32) {
        self.p_core_count = p_core_count;
        self.e_core_count = e_core_count;
    }

    /// Sends the initial connection information to the worker. This generally is just meta info.
    /// This should only be sent once and should always be the first item in the stream.
    pub fn send_initial_connection_result(&mut self) -> Result<(), Error> {
        send_msg_to_worker(
            &self.tx,
            update_for_worker::Update::ConnectionResult(ConnectionResult {
                worker_id: self.id.clone().into(),
            }),
        )
        .err_tip(|| format!("Failed to send ConnectionResult to worker : {}", self.id))
    }

    /// Notifies the worker of a requested state change.
    pub async fn notify_update(&mut self, worker_update: WorkerUpdate) -> Result<(), Error> {
        match worker_update {
            WorkerUpdate::RunAction(action) => {
                let (operation_id, action_info) = *action;
                self.run_action(operation_id, action_info).await
            }
            WorkerUpdate::Disconnect => {
                self.metrics.notify_disconnect.inc();
                send_msg_to_worker(&self.tx, update_for_worker::Update::Disconnect(()))
            }
        }
    }

    pub fn keep_alive(&mut self) -> Result<(), Error> {
        let tx = &mut self.tx;
        let id = &self.id;
        self.metrics.keep_alive.wrap(move || {
            send_msg_to_worker(tx, update_for_worker::Update::KeepAlive(()))
                .err_tip(|| format!("Failed to send KeepAlive to worker : {id}"))
        })
    }

    async fn run_action(
        &mut self,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        let tx = &mut self.tx;
        let worker_platform_properties = &mut self.platform_properties;
        let running_action_infos = &mut self.running_action_infos;
        let worker_id = self.id.clone().into();
        self.metrics
            .run_action
            .wrap(async move {
                let action_info_clone = action_info.clone();
                let operation_id_string = operation_id.to_string();
                let start_execute = StartExecute {
                    execute_request: Some(action_info_clone.inner.as_ref().into()),
                    operation_id: operation_id_string,
                    queued_timestamp: Some(action_info.inner.insert_timestamp.into()),
                    platform: Some((&action_info.platform_properties).into()),
                    worker_id,
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                    // (#p2p-prefetch) This builder does not run the Phase-4
                    // locality walk (that lives in `api_worker_scheduler.rs`),
                    // so no inline peer hints are carried here.
                    missing_digest_peers: Vec::new(),
                };
                reduce_platform_properties(
                    worker_platform_properties,
                    &action_info.platform_properties,
                );
                // (#specprefetch-rebind Stage C) `exec_start_time: None` — this
                // reconnect-notify path (dead in prod: `notify_update` is only
                // called with `Disconnect`) has no injected clock, so the record
                // stays un-timed and invisible to `worker_time_to_free`/EWMA.
                running_action_infos.insert(
                    operation_id,
                    PendingActionInfoData {
                        action_info,
                        exec_start_time: None,
                        // (#task-resource-profile Phase-2c) reconnect-notify insert
                        // is dead in prod (Disconnect-only) → no dispatch prediction.
                        dispatch_memory_prediction: None,
                    },
                );

                send_msg_to_worker(tx, update_for_worker::Update::StartAction(start_execute))
            })
            .await
    }

    pub(crate) fn execution_complete(&mut self, operation_id: &OperationId) {
        if let Some((operation_id, pending_action_info)) =
            self.running_action_infos.remove_entry(operation_id)
        {
            self.restored_platform_properties
                .insert(operation_id.clone());
            self.restore_platform_properties(&pending_action_info.action_info.platform_properties);
            self.running_action_infos
                .insert(operation_id, pending_action_info);
        }
    }

    // (#sched-b1) Synchronous: the body has no `.await`. It is called
    // from `ApiWorkerScheduler::update_action`'s second critical section,
    // which holds the worker-pool `inner` write lock and MUST NOT suspend
    // while held (the B1 lock-decouple invariant). The caller re-checks
    // `running_action_infos` under the same lock before calling, so the
    // missing-op error here is unreachable on the production path; it is
    // retained only as a defensive contract for any direct caller.
    pub(crate) fn complete_action(&mut self, operation_id: &OperationId) -> Result<(), Error> {
        let pending_action_info = self.running_action_infos.remove(operation_id).err_tip(|| {
            format!(
                "Worker {} tried to complete operation {} that was not running",
                self.id, operation_id
            )
        })?;
        if !self.restored_platform_properties.remove(operation_id) {
            self.restore_platform_properties(&pending_action_info.action_info.platform_properties);
        }
        self.is_paused = false;
        self.paused_due_to_backpressure = false;
        self.metrics.actions_completed.inc();
        Ok(())
    }

    pub fn has_actions(&self) -> bool {
        !self.running_action_infos.is_empty()
    }

    pub(crate) fn restore_platform_properties(&mut self, props: &PlatformProperties) {
        for (property, prop_value) in &props.properties {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let PlatformPropertyValue::Minimum(worker_value) =
                    worker_props.get_mut(property).unwrap()
                {
                    *worker_value += value;
                }
            }
        }
    }

    pub fn can_accept_work(&self) -> bool {
        !self.is_paused
            && !self.is_draining
            && (self.max_inflight_tasks == 0
                || u64::try_from(self.running_action_infos.len()).unwrap_or(u64::MAX)
                    < self.max_inflight_tasks)
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Worker {}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Default, MetricsComponent)]
struct Metrics {
    #[metric(help = "The timestamp of when this worker connected.")]
    connected_timestamp: u64,
    #[metric(help = "The number of actions completed for this worker.")]
    actions_completed: CounterWithTime,
    #[metric(help = "The number of actions started for this worker.")]
    run_action: AsyncCounterWrapper,
    #[metric(help = "The number of keep_alive sent to this worker.")]
    keep_alive: FuncCounterWrapper,
    #[metric(help = "The number of notify_disconnect sent to this worker.")]
    notify_disconnect: CounterWithTime,
}
