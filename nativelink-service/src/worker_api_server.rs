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

use core::convert::Into;
use core::pin::Pin;
use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// (#216) Allowed build SHAs for stale-worker detection. When set and
// non-empty, we reject `connect_worker` requests whose
// `ConnectWorkerRequest.build_sha` is not in the set. Wrapped in
// `Arc<HashSet>` so cheap clone-on-handoff to the per-connection
// validation path; `HashSet` (rather than `Vec`) so the membership
// test is O(1) — connect frequency is low (one per worker reconnect)
// but the cost of an O(n) scan grows with allowlist length and the
// expected operator pattern is "current SHA + previous 10".

use futures::stream::unfold;
use futures::{Stream, StreamExt};
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_server::{
    WorkerApi, WorkerApiServer as Server,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    execute_result, BlobsAvailableAck, ExecuteComplete, ExecuteResult, GoingAwayRequest,
    KeepAliveRequest, ReconcileCompleteRequest, UpdateForScheduler, UpdateForWorker,
    UploadMissingBlobsRequest,
};
use nativelink_store::small_blob_dispatcher::SmallBlobDispatcher;
use nativelink_util::ac_pin_registry::SharedAcPinRegistry;
use nativelink_util::blob_locality_map::{
    PersistedEndpoint, PersistedLocalityMap, ReloadedLocalitySummary, SharedBlobLocalityMap,
};
use nativelink_util::common::DigestInfo;
use nativelink_scheduler::worker::{MAX_PLAUSIBLE_CORES, Worker};
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::background_spawn;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::operation_state_manager::UpdateOperationType;
use nativelink_util::platform_properties::PlatformProperties;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use rand::RngCore;
use tokio::sync::mpsc;
use tokio::time::interval;
use tonic::{Response, Status};
use tracing::{debug, error, info, warn, instrument, Level};
use uuid::Uuid;

use nativelink_proto::build::bazel::remote::execution::v2::Digest;

use crate::worker_quiesce::ShutdownQuiesce;

pub type ConnectWorkerStream =
    Pin<Box<dyn Stream<Item = Result<UpdateForWorker, Status>> + Send + Sync + 'static>>;

pub type NowFn = Box<dyn Fn() -> Result<Duration, Error> + Send + Sync>;

#[derive(MetricsComponent)]
pub struct WorkerApiServer {
    scheduler: Arc<dyn WorkerScheduler>,
    now_fn: Arc<NowFn>,
    node_id: [u8; 6],
    locality_map: Option<SharedBlobLocalityMap>,
    /// Server-side AC pin registry. INTENTIONALLY a separate data
    /// structure from `locality_map` (which is CAS-shared) so AC pin
    /// advertisements (proto field 17 `pinned_ac_mirror_entries`)
    /// CANNOT route into the CAS-side locality_map and weaponize
    /// `bytestream_server::write` / `cas_server::batch_update_blobs`
    /// upload short-circuits via `WorkerProxyStore::has_with_results`.
    ///
    /// `None` for tests / standalone runs without AC pin advertisement.
    /// Populated in production from
    /// `nativelink_util::ac_pin_registry::new_shared_ac_pin_registry()`.
    ///
    /// Read-side wiring (a future commit) will land an AC peer-fetch
    /// path that consults this registry directly. With no consumer
    /// today, the registry's purpose is purely to validate the wire
    /// channel end-to-end.
    // CAPPED: AcPinRegistry enforces DEFAULT_MAX_AC_PINS_PER_ENDPOINT = 1_000_000 internally.
    ac_pin_registry: Option<SharedAcPinRegistry>,
    /// (#12 H4 invariant — phase 1/3) Pending output locality registry.
    ///
    /// Tracks which output digests are EXPECTED to become server-visible
    /// before the AC entry referencing them is published. Phase 2 wires
    /// the registration from `UpdateActionResult` (worker sets
    /// `cas_endpoint` on the request; server inserts output digests here
    /// BEFORE committing the AC entry). Phase 3 wires CCS consult so the
    /// completeness check treats these digests as present.
    ///
    /// INTENTIONALLY a SECOND `AcPinRegistry` instance — NOT the CAS
    /// `locality_map`. Routing pending-output entries into `locality_map`
    /// would weaponize the CAS upload short-circuit:
    /// `WorkerProxyStore::has_with_results` reads from `locality_map`;
    /// a hit there causes `bytestream_server::write` and
    /// `cas_server::batch_update_blobs` to SKIP the upload of the
    /// corresponding bytes. The Action proto digest IS by REAPI design
    /// the same as the `action_digest` key in CAS — routing AC-related
    /// digests through the CAS locality map would silently drop Action
    /// proto uploads, producing permanent data loss.
    ///
    ///   ╔══════════════════════════════════════════════════════════════╗
    ///   ║ SHORT-CIRCUIT GUARD: this registry MUST NEVER be consulted  ║
    ///   ║ by any `has_with_results` path. The upload short-circuit     ║
    ///   ║ lives in `WorkerProxyStore::has_with_results` which only     ║
    ///   ║ reads `locality_map`. Keep these two data structures         ║
    ///   ║ structurally separated so no future refactor accidentally    ║
    ///   ║ merges them.                                                  ║
    ///   ╚══════════════════════════════════════════════════════════════╝
    ///
    /// Lifecycle: wipe_endpoint fires on worker disconnect AND on
    /// boot-epoch change — same hooks as `ac_pin_registry` and
    /// `locality_map`. `None` for tests / standalone runs without the
    /// H4 invariant enforcement.
    ///
    /// Note: the deleted `register_action_result_digests` approach (cited at
    /// the BlobsAvailable handler ~:1431) was removed for an mpsc::channel(1)
    /// eviction race; this registry is fed server-side at AC publish time
    /// (phase 2) and never shares that channel.
    // CAPPED: AcPinRegistry enforces DEFAULT_MAX_AC_PINS_PER_ENDPOINT = 1_000_000 internally.
    pending_output_locality_registry: Option<SharedAcPinRegistry>,
    /// CAS store for checking blob existence during backfill requests.
    cas_store: Option<Store>,
    /// Optional handle on the `WorkerProxyStore` so we can plumb
    /// per-worker mirror capacity reports (review #1) into the
    /// picker's pre-check filter. None for tests / standalone runs
    /// without peer mirroring.
    worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
    /// Optional handle on the `SmallBlobDispatcher` so we can:
    ///   - register the worker's `UpdateForWorker` Sender at connect
    ///     (`register_worker`) and drop it at disconnect (`unregister_worker`);
    ///   - broadcast `BlobsAvailableNotification.pinned_mirror_entries`
    ///     (proto field 16) to every registered FastSlowStore via
    ///     `broadcast_pinned_mirror_ack` so each store binary-searches its
    ///     own `store_id` slice and unpins acked entries.
    /// `None` for tests / standalone runs without the small-blob mirror.
    /// Per task #168: the dispatcher mechanism is plumbed in here even
    /// when the feature flag is off; broadcast / register paths are no-ops
    /// in that case.
    small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    /// Per-endpoint connection state used by the #141 boot_epoch wipe
    /// path. For each CAS endpoint we track:
    ///   - `boot_epoch_id` from the worker's most recent
    ///     ConnectWorkerRequest (used to decide whether the next
    ///     reconnect needs a wipe), and
    ///   - the WorkerId of the connection that owns the endpoint right
    ///     now (used by the disconnect-cleanup task to suppress its
    ///     own remove_endpoint when a newer connection has taken over).
    ///
    /// Co-located with the locality_map field rather than living in
    /// the scheduler because the wipe needs to be ordered with the
    /// `locality_map.write()` lock and with each `WorkerConnection`'s
    /// disconnect-cleanup task — both of which operate inside this
    /// server, not the scheduler.
    endpoint_state: Arc<parking_lot::Mutex<HashMap<String, EndpointState>>>,
    /// (#58 directive-3) When the startup locality reload primed the sentinel
    /// (`__reloaded_unconfirmed__`) entries. The never-reconnect sweep
    /// (`LocalityPersister::sweep_unconfirmed`) refuses to drop any sentinel
    /// entry until this is at least `grace` old — so the `grace` arg is
    /// load-bearing under ANY scheduler (one-shot at grace OR a periodic tick
    /// that fires before grace must NOT prematurely sweep). Process-wide
    /// singleton shared with every `LocalityPersister` clone (reload stamps it;
    /// sweep reads it). `None` until a reload runs.
    ///
    /// `tokio::time::Instant` (NOT `std::time::Instant`): the never-reconnect
    /// sweep schedule (`run_never_reconnect_sweep`) sleeps `grace` then compares
    /// `baseline.elapsed() >= grace`. Both the sleep AND the elapsed-comparison
    /// MUST read the SAME clock or the production-timing test cannot virtualize
    /// the 600 s window under `tokio::time::pause()` — `std::time::Instant` is
    /// the real wall clock, which `pause()` does NOT advance, so a virtualized
    /// `sleep(grace)` would leave `baseline.elapsed()` at ~0 and the gate would
    /// no-op. In production (never paused) `tokio::time::Instant` delegates to
    /// the real monotonic clock, so runtime behavior is identical.
    locality_reload_baseline: Arc<parking_lot::Mutex<Option<tokio::time::Instant>>>,
    /// (#387) Per-endpoint flap-detection history. Held in a SEPARATE
    /// `parking_lot::Mutex` from `endpoint_state` so it survives the
    /// disconnect-cleanup `state.remove(&cas_endpoint)` at
    /// `:1108-1166`. The OOM-loop pattern (process dies → kernel RST
    /// → server's per-connection background task observes the stream
    /// break in ms → disconnect-cleanup runs while no new connection
    /// has arrived → `state.remove`) is the exact case the detector
    /// is built to surface, and folding flap history into
    /// `EndpointState` made the detector silent in that case — see
    /// `.claude/reviews/387-first-pass/distributed-systems-reviewer.md`
    /// BLOCK-FIX-FIRST.
    ///
    /// Lock-order rule: NEVER hold both `endpoint_state` and
    /// `flap_history` at the same time. The connect path takes
    /// `endpoint_state` first (for the #141 wipe), releases it, then
    /// takes `flap_history` (for the flap check). The disconnect
    /// path takes only `endpoint_state` and never touches
    /// `flap_history`. No call site holds both — the linter for this
    /// is "search the file for `endpoint_state.lock()` and check that
    /// every `flap_history.lock()` is OUTSIDE that guard's scope."
    // UNBOUNDED-OK: one entry per cas_endpoint; #216 build_sha
    // allowlist gates the universe of connecting endpoints; not
    // attacker-controlled. Per-entry deque also UNBOUNDED-OK (capped
    // at FLAP_THRESHOLD by eviction-on-push, comment on field).
    flap_history: Arc<parking_lot::Mutex<HashMap<String, FlapHistory>>>,
    /// (#216) Allowed worker build SHAs. `None` = validation disabled
    /// (every worker is accepted). `Some(set)` with `set` non-empty =
    /// reject any `ConnectWorkerRequest.build_sha` not present in the
    /// set with `Code::FailedPrecondition`. An EMPTY set is treated
    /// as "validation enabled, every SHA mismatches" — operators who
    /// truly want to disable validation should pass `None`.
    compatible_build_shas: Option<Arc<HashSet<String>>>,
    /// Counters for the BlobsAvailable mark_stable / backfill pipeline.
    /// Shared across every `WorkerConnection` and its background tasks
    /// so a single counter aggregates server-wide. Wired into the
    /// metrics tree under `worker_api` so operators can alert on the
    /// `mark_stable_has_with_results_failures` counter.
    #[metric(group = "worker_api")]
    metrics: Arc<WorkerApiMetrics>,
    /// (#sigkill-gap) Worker-intake quiesce latch, flipped at SIGTERM Phase 0b
    /// (`src/bin/nativelink.rs`) BEFORE the unbounded flush phases. Cloned into
    /// every `WorkerConnection`; read at the entry of the HANDLER-invoked
    /// `request_missing_blob_uploads` (the backfill + pinned-mirror-pull feeds)
    /// to suppress NEW worker-upload solicitation during the shutdown drain so
    /// it converges to a fixed point instead of being storm-fed. The
    /// server-initiated `ShutdownPuller::run` does NOT consult this (it passes
    /// `None`) — the shutdown PULL stays open. Constructed internally (like
    /// `metrics` / `endpoint_state`) so the public constructors' arg lists are
    /// unchanged; the bin obtains the write handle via
    /// `shutdown_quiesce_handle`.
    shutdown_quiesce: ShutdownQuiesce,
    // #212 v4.5: WriteChunked moved off `WorkerApi` to the new
    // `CasExtensions` service so it can be registered on the same
    // listener as `cas` / `bytestream` (see
    // `chunked_write_handler::ChunkedWriteHandler` impl `CasExtensions`
    // and `bin/nativelink.rs`'s service-wiring loop). This struct no
    // longer carries a chunked_write_handler field — the handler is
    // installed as its own tonic service, NOT as a method on
    // `WorkerApi`.
}

impl RootMetricsComponent for WorkerApiServer {}

/// Counters for the BlobsAvailable mark_stable / backfill pipeline.
/// Wrapped in `Arc` so the per-worker `WorkerConnection` instances and the
/// background spawned tasks share a single counter across the process.
///
/// `mark_stable_has_with_results_failures` is the counter the operator
/// alerts on per red-team F5: when the existence check fails, the
/// previous code logged `error!` and silently moved on, masking a
/// sustained problem (CLAUDE.md "Belt-and-suspenders masks bugs").
/// Now we increment a counter alongside the log line so a sustained
/// error rate is observable in the metric stream without grepping logs.
///
/// `#[derive(MetricsComponent)]` + the `WorkerApiServer`-side
/// `#[metric(group = "worker_api")]` wiring make these counters visible
/// to the metrics tree (per the existing `metric` infra). Without the
/// derive the AtomicU64 lives only in memory and can only be observed
/// via the `WorkerApiServer::metrics()` accessor — defeats the
/// "operator-alertable" purpose of the counter (red-team BLOCK-2 on
/// agent-a4f84244 / task #157).
#[derive(Debug, Default, MetricsComponent)]
pub struct WorkerApiMetrics {
    /// Total times `cas_store.has_with_results` failed inside
    /// `request_missing_blob_uploads`. A sustained increase indicates
    /// either a CAS store outage or a bug; without this counter the only
    /// signal is `error!` log lines.
    #[metric(
        help = "Total `cas_store.has_with_results` failures during BlobsAvailable \
                mark_stable / backfill. Sustained non-zero rate indicates a CAS \
                outage or bug; firing means worker pins for present digests are \
                not being acked and missing digests are not being requested for \
                upload (each tick recovers, but the counter exposes the rate)."
    )]
    pub mark_stable_has_with_results_failures: AtomicU64,

    /// (durability-ack v3 Stage 1, pair-a F2) Total `cas_store.has_durably`
    /// failures inside the BlobsAvailable `mark_stable` keystone gate.
    /// SEPARATE from `mark_stable_has_with_results_failures`: the v3 keystone
    /// added a second existence query (`has_durably`, the slow-tier-only
    /// durable-presence check that gates the BIS→unpin oath) whose Err arm
    /// must skip `mark_stable` this round. Folding both failures into one
    /// counter conflated two distinct sources (the upload-request existence
    /// check vs. the durable-gate check); an operator alerting on the metric
    /// could not tell which query was failing. A sustained non-zero rate here
    /// means durable digests are not being acked this round (each tick
    /// recovers, but the counter exposes the rate).
    #[metric(
        help = "Total `cas_store.has_durably` failures during the BlobsAvailable \
                mark_stable keystone gate (durability-ack v3). Distinct from \
                mark_stable_has_with_results_failures: this is the slow-tier-only \
                durable-presence query that gates the BIS→unpin oath. Sustained \
                non-zero rate means durable digests are not being acked this round \
                (each BlobsAvailable tick recovers, but the counter exposes the rate)."
    )]
    pub mark_stable_has_durably_failures: AtomicU64,

    /// (#216) Total `connect_worker` requests rejected because the
    /// worker's reported `build_sha` was not in the configured
    /// `compatible_build_shas` allowlist. Operators alert on a
    /// sustained non-zero rate to catch deploys that left a worker
    /// behind on a stale binary (which previously manifested as
    /// silent reconnect storms — see the diagnostic at
    /// `/tmp/wedge-digest-1777583500-SYNTHESIS.md`).
    #[metric(
        help = "Total worker connections rejected because the worker's reported \
                build_sha was not in the compatible_build_shas allowlist. A \
                sustained non-zero rate indicates a deploy left one or more \
                workers behind on a stale binary; redeploy the offending host \
                or expand the allowlist if the rollout is intentional."
    )]
    pub stale_workers_rejected_total: AtomicU64,

    /// (#387) Total flap-detection warns emitted on the
    /// `worker reconnect storm` log line. Increments at the same moment
    /// the `warn!` fires (after the cooldown gate), so the metric and
    /// the journald line are 1:1. Sustained non-zero rate means at
    /// least one worker on the fleet is restart-looping at process
    /// granularity (likely whole-process OOM kill, not the per-action
    /// SIGKILL covered by
    /// `simple_scheduler_state_manager.rs:758-766`). This counter
    /// rides on the `worker_api` group that is already published
    /// through `#[metric(group = "worker_api")]` at
    /// `WorkerApiServer.metrics`, so it is scrapable today — distinct
    /// from #386's signal which can only rely on `warn!` until #380
    /// lands.
    #[metric(
        help = "Total `worker reconnect storm` warns emitted by the boot_epoch \
                flap detector. Increments 1:1 with the journald warn line. \
                Sustained non-zero rate indicates one or more workers are \
                restart-looping at the process level (whole-process OOM kill, \
                not per-action SIGKILL); check the warn line for the offending \
                cas_endpoint."
    )]
    pub worker_flap_warns_total: AtomicU64,

    /// (#12 H4 invariant) Total registrations into
    /// `pending_output_locality_registry`. Incremented once per
    /// `UpdateActionResult` RPC that carries a live `cas_endpoint`
    /// and at least one output digest. Sustained increase = workers
    /// are publishing ARs with valid endpoint attribution; sustained
    /// zero after phase 2 lands = wire not connected or liveness
    /// check always failing.
    #[metric(
        help = "[Phase 1: always 0 — increment wired in phase 2] \
                Total registrations into pending_output_locality_registry \
                (one per UpdateActionResult with a live cas_endpoint). \
                Sustained zero after phase 2 lands = wire not connected or \
                liveness check always failing."
    )]
    pub pending_output_registrations_total: AtomicU64,

    /// (#99 S1 code-reviewer follow-up) Per-reason BlobsAvailable
    /// chunk-drop counters (`ChunkDropCounts`). Shared via Arc with
    /// every per-connection `BlobsAvailableAccumulator` so all
    /// per-connection drops aggregate into a single set of
    /// server-wide counters. Without this Arc-share, each connection
    /// would have its own `ChunkDropCounts` and per-connection
    /// counters would die with the connection — operators couldn't
    /// alert on a sustained drop rate. The
    /// `#[metric(group = "chunked_blobs_available")]` annotation
    /// publishes the inner counters under that group on the metrics
    /// tree (each `ChunkDropCounts` field has its own
    /// `#[metric(help = ...)]` per the `MetricsComponent` derive).
    /// Note: production-visible only after `RootMetricsComponent`
    /// publisher lands per #160.
    #[metric(group = "chunked_blobs_available")]
    pub chunked_blobs_available_drop_counts:
        Arc<crate::blobs_available_accumulator::ChunkDropCounts>,

    /// (#sigkill-gap) Total worker-upload solicitations SUPPRESSED by the
    /// shutdown worker-intake quiesce latch (Phase 0b). Incremented at the
    /// HANDLER-invoked `request_missing_blob_uploads` entry whenever the latch
    /// is set — i.e. the count of backfill / pinned-mirror-pull feeds the
    /// quiesce would have run during the shutdown drain. A NON-ZERO and STILL
    /// RISING value during shutdown confirms the latch is doing its job (the
    /// storm is being suppressed). A non-zero value paired with the shutdown
    /// drain failing to converge would indicate a quiesce ESCAPE (a
    /// worker-solicited feed that bypasses this gate). Observability only; the
    /// latch never auto-acts. Server-initiated `ShutdownPuller::run` does NOT
    /// increment this — it passes no latch and is the drain, not the storm.
    #[metric(
        help = "Total worker-upload solicitations suppressed by the shutdown \
                worker-intake quiesce latch (SIGTERM Phase 0b). Rising during \
                shutdown = the latch is suppressing the backfill storm so the \
                unbounded flush can converge."
    )]
    pub shutdown_suppressed_backfill_solicitations_total: AtomicU64,
}

/// (#387) Flap-detection thresholds. Three boot_epoch_id changes for
/// the same `cas_endpoint` within `FLAP_WINDOW` are unambiguously a
/// worker restart loop — a healthy `just deploy` is one rolling
/// restart, well under threshold. After a warn fires we suppress
/// re-warn for `FLAP_COOLDOWN` so a sustained 1-per-minute flap
/// doesn't drown the log. These are `const` so an operator can tune
/// them at compile time; runtime configuration was deemed not
/// load-bearing for the initial signal (#387 audit Q2).
const FLAP_THRESHOLD: usize = 3;
const FLAP_WINDOW: Duration = Duration::from_secs(300);
const FLAP_COOLDOWN: Duration = Duration::from_secs(60);

/// Per-endpoint state for the #141 boot_epoch wipe path. See
/// `WorkerApiServer::endpoint_state` for design.
#[derive(Debug, Clone)]
struct EndpointState {
    boot_epoch: u64,
    /// Worker ID of the connection that currently owns the endpoint.
    /// The disconnect-cleanup task only fires its `remove_endpoint`
    /// when this matches its own worker ID — otherwise a newer
    /// connection has taken over and the cleanup must be suppressed
    /// to avoid wiping the new worker's just-registered entries.
    owner_worker_id: WorkerId,
}

/// (#387) Per-endpoint flap-detection state. Lives in
/// `WorkerApiServer::flap_history` — a separate map from
/// `endpoint_state` so disconnect-cleanup's `state.remove(&cas_endpoint)`
/// at the bottom of `WorkerConnection`'s background task
/// (`worker_api_server.rs:1108-1166`) does NOT wipe the deque /
/// cooldown / last-seen-epoch values. Without this separation the
/// OOM-loop pattern (process dies → kernel RST → server disconnect
/// cleanup runs in ms → worker relaunches seconds later) wipes the
/// deque between every connect, so the detector only fires on the
/// rare race-flap where a new connection arrives BEFORE the old one's
/// cleanup runs — exactly the opposite of the OOM-loop case the
/// detector is built to surface.
///
/// `last_seen_epoch` replaces the previous `EndpointState`-sourced
/// `prev.boot_epoch` lookup: it is the last `boot_epoch_id` we
/// observed for this endpoint across ALL prior connects, regardless
/// of whether `endpoint_state` still has an entry. An epoch change
/// (or `new_boot_epoch == 0`, the legacy-worker "cannot tell new from
/// old" path inherited from #141) triggers a flap push.
#[derive(Debug, Default, Clone)]
struct FlapHistory {
    /// Last `boot_epoch_id` we observed on a connect for this endpoint.
    /// `None` means we have never seen a connect for this endpoint.
    /// Update at the end of every connect. SURVIVES disconnect cleanup
    /// — that is the load-bearing property over the prior
    /// `EndpointState.boot_epoch` lookup.
    last_seen_epoch: Option<u64>,
    /// Sliding window of timestamps at which this endpoint's
    /// `boot_epoch_id` was observed to change. Bounded above by
    /// `FLAP_THRESHOLD` after each insert evicts entries older than
    /// `FLAP_WINDOW`. Timestamps come from `now_fn`.
    // UNBOUNDED-OK: capped at FLAP_THRESHOLD entries per endpoint after
    // each push (older entries dropped). One push per worker reconnect;
    // not attacker-controlled (#216 build_sha allowlist gates connects).
    epoch_changes: VecDeque<Duration>,
    /// Last time the flap-warn fired for this endpoint, used to
    /// suppress re-warn within `FLAP_COOLDOWN`. `None` if the warn
    /// has not yet fired.
    last_flap_warn_at: Option<Duration>,
}

impl core::fmt::Debug for WorkerApiServer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WorkerApiServer")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl WorkerApiServer {
    pub fn new(
        config: &WorkerApiConfig,
        schedulers: &HashMap<String, Arc<dyn WorkerScheduler>>,
        locality_map: Option<SharedBlobLocalityMap>,
        cas_store: Option<Store>,
        worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
        ac_pin_registry: Option<SharedAcPinRegistry>,
        // (#12 H4 phase 1) See `pending_output_locality_registry` field doc.
        pending_output_locality_registry: Option<SharedAcPinRegistry>,
    ) -> Result<Self, Error> {
        let node_id = {
            let mut out = [0; 6];
            rand::rng().fill_bytes(&mut out);
            out
        };
        for scheduler in schedulers.values() {
            // This will protect us from holding a reference to the scheduler forever in the
            // event our ExecutionServer dies. Our scheduler is a weak ref, so the spawn will
            // eventually see the Arc went away and return.
            let weak_scheduler = Arc::downgrade(scheduler);
            background_spawn!("worker_api_server", async move {
                let mut ticker = interval(Duration::from_secs(1));
                loop {
                    ticker.tick().await;
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Error: system time is now behind unix epoch");
                    match weak_scheduler.upgrade() {
                        Some(scheduler) => {
                            if let Err(err) =
                                scheduler.remove_timedout_workers(timestamp.as_secs()).await
                            {
                                error!(?err, "Failed to remove_timedout_workers",);
                            }
                        }
                        // If we fail to upgrade, our service is probably destroyed, so return.
                        None => return,
                    }
                }
            });
        }

        Self::new_with_now_fn(
            config,
            schedulers,
            Box::new(move || {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| make_err!(Code::Internal, "System time is now behind unix epoch"))
            }),
            node_id,
            locality_map,
            cas_store,
            worker_proxy,
            small_blob_dispatcher,
            ac_pin_registry,
            pending_output_locality_registry,
        )
    }

    /// Same as `new()`, but you can pass a custom `now_fn`, that returns a Duration since `UNIX_EPOCH`
    /// representing the current time. Used mostly in  unit tests.
    pub fn new_with_now_fn(
        config: &WorkerApiConfig,
        schedulers: &HashMap<String, Arc<dyn WorkerScheduler>>,
        now_fn: NowFn,
        node_id: [u8; 6],
        locality_map: Option<SharedBlobLocalityMap>,
        cas_store: Option<Store>,
        worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
        ac_pin_registry: Option<SharedAcPinRegistry>,
        pending_output_locality_registry: Option<SharedAcPinRegistry>,
    ) -> Result<Self, Error> {
        let scheduler = schedulers
            .get(&config.scheduler)
            .err_tip(|| {
                format!(
                    "Scheduler needs config for '{}' because it exists in worker_api",
                    config.scheduler
                )
            })?
            .clone();
        // (#216) Convert the configured Vec<String> allowlist into an
        // Arc<HashSet<String>> for O(1) membership checks at connect
        // time. `None` and `Some(empty)` collapse to `None` here so
        // both shapes mean "validation disabled" — preserving the
        // backward-compat behavior. To enable validation with an
        // empty list (reject EVERY worker), the operator must set
        // `compatible_build_shas: [""]` (which permits only legacy
        // empty-SHA workers) or list specific SHAs.
        let compatible_build_shas = config
            .compatible_build_shas
            .as_ref()
            .filter(|v| !v.is_empty())
            .map(|v| Arc::new(v.iter().cloned().collect::<HashSet<String>>()));
        if let Some(ref set) = compatible_build_shas {
            info!(
                allowlist_size = set.len(),
                "worker_api: build-SHA allowlist enabled; workers reporting a \
                 build_sha not in this set will be rejected with FailedPrecondition"
            );
        }
        Ok(Self {
            scheduler,
            now_fn: Arc::new(now_fn),
            node_id,
            locality_map,
            ac_pin_registry,
            pending_output_locality_registry,
            cas_store,
            worker_proxy,
            small_blob_dispatcher,
            endpoint_state: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            locality_reload_baseline: Arc::new(parking_lot::Mutex::new(None)),
            flap_history: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            compatible_build_shas,
            metrics: Arc::new(WorkerApiMetrics::default()),
            shutdown_quiesce: ShutdownQuiesce::new(),
        })
    }

    /// (#sigkill-gap) Returns a clone of the worker-intake quiesce latch's
    /// WRITE handle. The bin captures this into the SIGTERM closure and calls
    /// `.quiesce()` at Phase 0b — BEFORE the unbounded flush — so the
    /// handler-invoked backfill / pinned-mirror-pull feeds stop soliciting new
    /// worker uploads and the drain converges. Cheap to clone (one
    /// `Arc<AtomicBool>`); observes the SAME latch the `WorkerConnection`s read.
    #[must_use]
    pub fn shutdown_quiesce_handle(&self) -> ShutdownQuiesce {
        self.shutdown_quiesce.clone()
    }

    /// Returns a clone of the metrics handle so callers (e.g. tests,
    /// metrics scrape integration) can observe the BlobsAvailable
    /// mark_stable / backfill counters directly.
    pub fn metrics(&self) -> Arc<WorkerApiMetrics> {
        self.metrics.clone()
    }

    /// (#387) Test-only: returns `true` if there is currently NO
    /// `EndpointState` entry for `endpoint`. Used by the disconnect-
    /// cleanup-to-completion test
    /// (`worker_flap_detection_survives_disconnect_cleanup_test`) to
    /// poll until the per-connection background task has run its
    /// `state.remove(&cas_endpoint)` at `:1108-1166` before the next
    /// connect. The new connection without this fence would race the
    /// cleanup and exercise the same-endpoint-already-cloned-deque
    /// path that pre-existed in `EndpointState` — defeating the
    /// purpose of moving the deque into `flap_history`.
    ///
    /// Held lock window is the parking_lot `endpoint_state` mutex
    /// (sync, no `.await`), so this is safe to call from any test
    /// context.
    pub fn endpoint_state_is_empty_for_testing(&self, endpoint: &str) -> bool {
        !self.endpoint_state.lock().contains_key(endpoint)
    }

    /// (#12 H4 invariant) Liveness check for `pending_output_locality_registry`
    /// registration. Returns `true` iff `endpoint` is currently present in the
    /// `endpoint_state` map — i.e. there is an active worker connection
    /// claiming that endpoint.
    ///
    /// Used by the `UpdateActionResult` handler (phase 2) to validate the
    /// worker-supplied `cas_endpoint` field before registering output digests.
    /// An endpoint not in `endpoint_state` is either spoofed, stale, or a
    /// typo — silently ignored to prevent phantom entries in the registry.
    ///
    /// Exposed as `pub` so tests can assert the check is wired.
    pub fn pending_output_endpoint_is_live(&self, endpoint: &str) -> bool {
        self.endpoint_state.lock().contains_key(endpoint)
    }

    /// (#12 H4 invariant) Test/diagnostic accessor: returns a clone of the
    /// `pending_output_locality_registry` handle, if configured.
    pub fn pending_output_locality_registry(&self) -> Option<SharedAcPinRegistry> {
        self.pending_output_locality_registry.clone()
    }

    /// Test accessor: returns a clone of the `ac_pin_registry` handle, if configured.
    /// Used in Test 6 as a positive tripwire to confirm the BlobsAvailable background
    /// task processed a tick before asserting the pending registry is unaffected.
    pub fn ac_pin_registry_for_testing(&self) -> Option<SharedAcPinRegistry> {
        self.ac_pin_registry.clone()
    }

    /// (#12 H4 phase 2) Returns a `SharedLivenessChecker` that delegates to
    /// `endpoint_state` — used by `AcServer` to validate `cas_endpoint` on
    /// `UpdateActionResult` before registering output digests into
    /// `pending_output_locality_registry`. Captures the same `Arc` that
    /// `connect_worker` / `inner_connect_worker` mutate, so it reflects the
    /// live worker set without any additional synchronization.
    pub fn liveness_checker(&self) -> crate::ac_server::SharedLivenessChecker {
        let state = self.endpoint_state.clone();
        std::sync::Arc::new(move |endpoint: &str| state.lock().contains_key(endpoint))
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    async fn inner_connect_worker(
        &self,
        mut update_stream: impl Stream<Item = Result<UpdateForScheduler, Status>>
        + Unpin
        + Send
        + 'static,
    ) -> Result<Response<ConnectWorkerStream>, Error> {
        let first_message = update_stream
            .next()
            .await
            .err_tip(|| "Missing first message for connect_worker")?
            .err_tip(|| "Error reading first message for connect_worker")?;
        let Some(Update::ConnectWorkerRequest(connect_worker_request)) = first_message.update
        else {
            return Err(make_err!(
                Code::Internal,
                "First message was not a ConnectWorkerRequest"
            ));
        };

        // (#216 defense-in-depth) Bound the operator-visible string
        // fields BEFORE we use them as `tracing` field values or
        // `Display` substitutions. The hello frame is the worker's
        // first message and the only constraint on string lengths is
        // the gRPC frame ceiling (multi-MiB). A buggy or malicious
        // worker could send megabyte-long `worker_id_prefix` /
        // `cas_endpoint` / `build_sha` values; without a bound,
        // `warn!` would dump them into the JSON-formatted log stream
        // and the `Display`-formatted error would propagate them up
        // through every retrying caller. 256 bytes is generous for
        // legitimate values (`build_sha` is fixed at 16; UUID-ish
        // worker_id_prefixes are <32; `grpc://host:port` URIs are
        // typically <64) and small enough to keep the failure log
        // line bounded.
        const MAX_HELLO_STRING_LEN: usize = 256;
        for (field_name, value) in [
            ("worker_id_prefix", connect_worker_request.worker_id_prefix.as_str()),
            ("cas_endpoint", connect_worker_request.cas_endpoint.as_str()),
            ("build_sha", connect_worker_request.build_sha.as_str()),
        ] {
            if value.len() > MAX_HELLO_STRING_LEN {
                warn!(
                    field_name,
                    value_len = value.len(),
                    max_allowed = MAX_HELLO_STRING_LEN,
                    "rejecting connect_worker: hello frame field exceeds bound — \
                     refusing to log the value to avoid log-injection / unbounded memory"
                );
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Worker hello frame field {:?} length {} exceeds the {}-byte limit; \
                     reject to bound log noise and avoid log-injection",
                    field_name,
                    value.len(),
                    MAX_HELLO_STRING_LEN
                ));
            }
        }

        // (#216) Stale-worker detection. Reject the connection BEFORE
        // any side effects (worker_id allocation, locality_map wipe,
        // dispatcher registration) so a rejected worker leaves zero
        // residual state. The allowlist is opt-in: when
        // `compatible_build_shas` is `None`, every reported SHA
        // (including the empty string from legacy workers) is
        // accepted unchanged.
        //
        // Diagnostic context: the bug at #216 (worker-03 running an
        // old binary) was silent because the WIRE protocol was still
        // backward-compatible — the worker accepted the connection
        // and only the per-frame decode of `BatchWriteSmallBlobs`
        // failed downstream. Validation here makes the failure mode
        // EAGER and LOUD: a single WARN per stale-worker connect
        // attempt + an alertable counter, instead of N kilo-errors
        // per second buried in the worker log.
        if let Some(ref allowlist) = self.compatible_build_shas {
            let reported = connect_worker_request.build_sha.as_str();
            if !allowlist.contains(reported) {
                self.metrics
                    .stale_workers_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    reported_build_sha = %reported,
                    allowlist_size = allowlist.len(),
                    worker_id_prefix = %connect_worker_request.worker_id_prefix,
                    cas_endpoint = %connect_worker_request.cas_endpoint,
                    "rejecting connect_worker: reported build_sha not in compatible_build_shas \
                     allowlist; redeploy this worker or extend the allowlist"
                );
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "Worker build_sha {:?} is not in the scheduler's compatible_build_shas \
                     allowlist (allowlist contains {} entries). This usually means the worker \
                     is running a stale binary; redeploy the worker via the project Justfile \
                     to pick up the current build, or extend the scheduler's \
                     compatible_build_shas list if the rollout is intentional.",
                    reported,
                    allowlist.len()
                ));
            }
        }

        let worker_cas_endpoint = connect_worker_request.cas_endpoint.clone();
        let new_boot_epoch = connect_worker_request.boot_epoch_id;

        let (tx, rx) = mpsc::unbounded_channel();

        // First convert our proto platform properties into one our scheduler understands.
        let platform_properties = {
            let mut platform_properties = PlatformProperties::default();
            for property in connect_worker_request.properties {
                let platform_property_value = self
                    .scheduler
                    .get_platform_property_manager()
                    .make_prop_value(&property.name, &property.value)
                    .err_tip(|| "Bad Property during connect_worker()")?;
                platform_properties
                    .properties
                    .insert(property.name.clone(), platform_property_value);
            }
            platform_properties
        };

        // Clone tx so WorkerConnection can send messages back to the worker
        // (e.g. UploadMissingBlobs requests) independently of the scheduler.
        let worker_tx = tx.clone();

        // Allocate the worker_id BEFORE the #141 wipe so the new owner
        // can be recorded atomically with the wipe.
        let worker_id = WorkerId(format!(
            "{}{}",
            connect_worker_request.worker_id_prefix,
            Uuid::now_v6(&self.node_id).hyphenated()
        ));

        // #141: boot_epoch_id one-way wipe. Detect a new worker process
        // taking over the same CAS endpoint and clear its prior locality
        // entries before the worker can send any BlobsAvailable. The
        // wipe MUST happen here — before `add_worker` (which triggers
        // ConnectionResult that the worker waits on before sending
        // BlobsAvailable) — so the new worker's incoming registrations
        // cannot interleave with or precede the wipe.
        //
        // Atomicity vs. the OLD WorkerConnection's disconnect-cleanup
        // task: the cleanup task suppresses its own remove_endpoint
        // when `endpoint_state.owner_worker_id` no longer matches its
        // own (a newer connection has taken over). Holding the
        // endpoint_state mutex across the locality_map.write() wipe +
        // the owner_worker_id update guarantees the OLD task cannot
        // observe a half-applied state where the old owner is still
        // recorded but the locality_map has already been mutated.
        // (#387) `_prev_was_some` captures whether `endpoint_state`
        // had an entry for this endpoint at the moment we took the
        // lock. Live-but-unused-in-production by design: it is
        // referenced only by the commented mutation-step block at
        // `:768-770` that simulates the BLOCK-FIX-FIRST lifecycle
        // defect WITHOUT re-taking the `endpoint_state` lock outside
        // its critical section (which would violate the lock-order
        // rule on `flap_history`). Leading underscore silences the
        // `unused_variable` lint while preserving the seam.
        let mut _prev_was_some = false;
        let needs_bis_buffer_clear = if !worker_cas_endpoint.is_empty() {
            let mut state = self.endpoint_state.lock();
            let prev = state.get(&worker_cas_endpoint).cloned();
            _prev_was_some = prev.is_some();
            // Wipe whenever the new epoch differs from the prev epoch,
            // OR when the new epoch is 0 (legacy worker — we cannot
            // distinguish "transient reconnect" from "fresh process").
            // If there is no prev (first connect ever from this
            // endpoint) the locality_map is already empty for this
            // endpoint, so the wipe is a no-op but still safe.
            let needs_wipe = prev
                .as_ref()
                .is_some_and(|p| p.boot_epoch != new_boot_epoch || new_boot_epoch == 0);
            if needs_wipe {
                if let Some(ref locality_map) = self.locality_map {
                    locality_map.write().remove_endpoint(&worker_cas_endpoint);
                }
                // Sibling of locality_map wipe for the AC pin path —
                // the new boot_epoch_id means a fresh process has
                // taken over the endpoint; its AC pin entries (if
                // any from a recent re-advertisement on the way up)
                // start empty so the prior process's lingering AC
                // pins must be wiped to avoid drift between the
                // server's registry and the new worker's empty
                // `dispatched_mirror_pins` map. Field 17 advertises
                // the FULL CURRENT snapshot every tick, so the new
                // worker's first BlobsAvailable will re-populate the
                // registry — server-side wipe + worker re-advertise
                // is convergent.
                if let Some(ref ac_pin_registry) = self.ac_pin_registry {
                    ac_pin_registry.wipe_endpoint(&worker_cas_endpoint);
                }
                // (#12 H4) Sibling wipe for pending_output_locality_registry:
                // the new boot_epoch means a fresh worker process; any
                // pending output locality pins from the prior process are
                // stale and must be dropped so phase-2 re-advertisements
                // start from a clean slate.
                if let Some(ref pending) = self.pending_output_locality_registry {
                    pending.wipe_endpoint(&worker_cas_endpoint);
                }
                // #174: boot-epoch wipe dispatcher leak. When a worker
                // reconnects with a new boot_epoch BEFORE OLD's
                // disconnect-cleanup task runs, OLD's
                // `dispatcher.worker_txs[(endpoint, prev_epoch)]` and
                // per-(worker, prev_epoch) queues would leak forever —
                // OLD's later cleanup hits the ownership-check guard
                // (NEW owns the endpoint, see WorkerConnection::start
                // disconnect path) and SKIPS its
                // `dispatcher.unregister_worker` /
                // `dispatcher.unpin_on_disconnect` calls. Symmetric to
                // the locality_map wipe just above; see also #141
                // (which fixed locality_map for the same race class).
                // Dormant under `small_blob_mirror_enabled=false`;
                // becomes an active leak the moment the flag flips.
                //
                // Both calls happen while the `endpoint_state` lock is
                // held — same atomicity argument as the locality_map
                // wipe: the OLD disconnect task observes a fully wiped
                // dispatcher state OR an unwiped one, never a partial
                // state that could lose entries from EITHER epoch.
                if let Some(ref dispatcher) = self.small_blob_dispatcher {
                    if let Some(ref prev_state) = prev {
                        dispatcher.unregister_worker(
                            &worker_cas_endpoint,
                            prev_state.boot_epoch,
                        );
                        dispatcher.unpin_on_disconnect(
                            &worker_cas_endpoint,
                            prev_state.boot_epoch,
                        );
                    }
                }
                info!(
                    endpoint = %worker_cas_endpoint,
                    prev_epoch = prev.as_ref().map(|p| p.boot_epoch),
                    new_epoch = new_boot_epoch,
                    "wiped locality_map + dispatcher state on worker boot_epoch_id change"
                );
            }
            state.insert(
                worker_cas_endpoint.clone(),
                EndpointState {
                    boot_epoch: new_boot_epoch,
                    owner_worker_id: worker_id.clone(),
                },
            );
            // (#387) DROP the `endpoint_state` guard before touching
            // `flap_history`. Lock-order rule: never hold both. See
            // the doc-comment on `WorkerApiServer::flap_history`.
            drop(state);
            needs_wipe
        } else {
            false
        };

        // (#387) Flap detection. Sources `last_seen_epoch` from
        // `flap_history` — a separate map that is NEVER cleared by
        // disconnect cleanup. The OOM-loop pattern (disconnect
        // cleanup wipes `endpoint_state` between every reconnect)
        // would silently empty the deque if we read it from
        // `EndpointState`; with `flap_history` separate, the deque
        // accumulates across reconnects regardless of whether
        // `endpoint_state` was cleared in between.
        //
        // Fires when the observed epoch differs from
        // `last_seen_epoch` (a new worker process took over the
        // endpoint) OR the new epoch is 0 (legacy worker — same
        // "cannot tell new from old" path #141 inherits, so we treat
        // every epoch-0 connect as a fresh process).
        //
        // First-ever connect: `last_seen_epoch.is_none()` → no flap
        // push, but we record the epoch so the NEXT differing-epoch
        // connect fires correctly.
        if !worker_cas_endpoint.is_empty() {
            // MUTATION-STEP (commented in production): uncommenting
            // the wipe below simulates the BLOCK-FIX-FIRST defect
            // (folding flap state back into `EndpointState` so it is
            // wiped by disconnect-cleanup's `state.remove`). Gated on
            // `!_prev_was_some` so it only wipes in the OOM-loop case
            // (no prev `EndpointState` at connect time → the
            // disconnect-cleanup task had already run); the
            // race-flap case (prev still owned) keeps history,
            // matching the original buggy semantics exactly. The
            // BLOCK-FIX regression test
            // `worker_flap_detection_survives_disconnect_cleanup_test`
            // MUST red-fail with the "flap detector wiped by
            // disconnect cleanup — operator-blind to OOM-loop
            // pattern" assertion when this line is uncommented.
            //
            // if !_prev_was_some {
            //     self.flap_history.lock().remove(&worker_cas_endpoint);
            // }
            let mut hist = self.flap_history.lock();
            let entry = hist.entry(worker_cas_endpoint.clone()).or_default();
            let is_epoch_change = entry
                .last_seen_epoch
                .is_some_and(|prev_epoch| prev_epoch != new_boot_epoch || new_boot_epoch == 0);
            if is_epoch_change {
                // Use `now_fn` (not `SystemTime::now()`) so tests can
                // drive the window/cooldown logic deterministically;
                // production wraps `SystemTime::now() - UNIX_EPOCH`.
                let now_dur = (self.now_fn)()?;
                // Drop entries older than `FLAP_WINDOW`. The deque is
                // already sorted oldest-front because pushes are
                // monotonic in `now_dur`, so a single front-pop loop
                // suffices.
                while let Some(front) = entry.epoch_changes.front() {
                    if now_dur.saturating_sub(*front) > FLAP_WINDOW {
                        entry.epoch_changes.pop_front();
                    } else {
                        break;
                    }
                }
                entry.epoch_changes.push_back(now_dur);
                if entry.epoch_changes.len() >= FLAP_THRESHOLD {
                    let cooldown_active = entry
                        .last_flap_warn_at
                        .is_some_and(|prev_warn| {
                            now_dur.saturating_sub(prev_warn) < FLAP_COOLDOWN
                        });
                    if !cooldown_active {
                        warn!(
                            endpoint = %worker_cas_endpoint,
                            flips_in_window = entry.epoch_changes.len(),
                            window_secs = FLAP_WINDOW.as_secs(),
                            "worker reconnect storm — process restarting repeatedly \
                             (likely whole-process OOM, not just per-action SIGKILL)"
                        );
                        self.metrics
                            .worker_flap_warns_total
                            .fetch_add(1, Ordering::Relaxed);
                        entry.last_flap_warn_at = Some(now_dur);
                    }
                }
            }
            entry.last_seen_epoch = Some(new_boot_epoch);
        }

        // (#97) On boot_epoch change, clear the BIS resend buffer for
        // this endpoint BEFORE add_worker triggers replay. The new
        // process has fresh pin state; replaying old chunks would just
        // waste memory until the worker happens to ack. Done outside
        // the sync `endpoint_state` mutex because the call is async.
        if needs_bis_buffer_clear && !worker_cas_endpoint.is_empty() {
            self.scheduler
                .clear_bis_resend_buffer_for_endpoint(&worker_cas_endpoint)
                .await;
        }

        // Now register the worker with the scheduler. This triggers
        // ConnectionResult — and only then will the worker begin
        // sending BlobsAvailable.
        {
            // (#sched-blend security S1) Clamp the worker-reported core
            // counts at the ingest seam, symmetric with the string bound
            // above (`MAX_HELLO_STRING_LEN`). The counts feed the blend's
            // penalty denominator; an unclamped over-report (sysctl glitch /
            // future-chip / config typo) would compute near-infinite free
            // capacity → zero load penalty regardless of real load → the
            // worker monopolizes cache-tied placement. This is the
            // authoritative ingest clamp, not the proto.
            let p_core_count = connect_worker_request.p_core_count.min(MAX_PLAUSIBLE_CORES);
            let e_core_count = connect_worker_request.e_core_count.min(MAX_PLAUSIBLE_CORES);
            let worker = Worker::new_with_cas_endpoint(
                worker_id.clone(),
                platform_properties,
                tx,
                (self.now_fn)()?.as_secs(),
                connect_worker_request.max_inflight_tasks,
                worker_cas_endpoint.clone(),
                p_core_count,
                e_core_count,
            );
            self.scheduler
                .add_worker(worker)
                .await
                .err_tip(|| "Failed to add worker in inner_connect_worker()")?;
        }

        // task #168 (item 6): plumb the worker's UpdateForWorker Sender
        // into the SmallBlobDispatcher so the per-`(endpoint,
        // boot_epoch_id, store_id)` drainer task can deliver
        // `BatchWriteSmallBlobs`. Replaces any prior entry for the same
        // `(endpoint, boot_epoch_id)` (rare; #141 wipe handles old
        // process). On disconnect (below in `WorkerConnection::start`'s
        // task body), `unregister_worker` drops the Sender + per-(worker,
        // store) queues, causing drainers to exit gracefully.
        if let Some(dispatcher) = self.small_blob_dispatcher.as_ref() {
            if !worker_cas_endpoint.is_empty() {
                dispatcher.register_worker(
                    &worker_cas_endpoint,
                    new_boot_epoch,
                    worker_tx.clone(),
                );
            }
        }

        WorkerConnection::start(
            self.scheduler.clone(),
            self.now_fn.clone(),
            worker_id.clone(),
            self.locality_map.clone(),
            self.ac_pin_registry.clone(),
            self.pending_output_locality_registry.clone(),
            self.cas_store.clone(),
            self.worker_proxy.clone(),
            self.small_blob_dispatcher.clone(),
            worker_cas_endpoint,
            new_boot_epoch,
            self.endpoint_state.clone(),
            worker_tx,
            self.metrics.clone(),
            self.shutdown_quiesce.clone(),
            update_stream,
        );

        Ok(Response::new(Box::pin(unfold(
            (rx, worker_id),
            move |state| async move {
                let (mut rx, worker_id) = state;
                if let Some(update_for_worker) = rx.recv().await {
                    return Some((Ok(update_for_worker), (rx, worker_id)));
                }
                warn!(
                    ?worker_id,
                    "UpdateForWorker channel was closed, thus closing connection to worker node",
                );

                None
            },
        ))))
    }

    pub async fn inner_connect_worker_for_testing(
        &self,
        update_stream: impl Stream<Item = Result<UpdateForScheduler, Status>> + Unpin + Send + 'static,
    ) -> Result<Response<ConnectWorkerStream>, Error> {
        self.inner_connect_worker(update_stream).await
    }

    /// (#58 directive-2: server durability bundle — shutdown WORKER-PULL phase)
    ///
    /// Pull every worker-resident CAS blob the server does NOT already hold
    /// durably down to the server CAS, so that after a restart the server
    /// serves ALL blobs locally with ZERO dependence on worker re-backfill
    /// (closes the 2026-06-23 restart's "lost input #21" window — design
    /// `shutdown-pull-worker-blobs-design-2026-06-23.md`).
    ///
    /// MUST run BEFORE worker eviction (the SIGTERM handler reorders it ahead
    /// of `scheduler.shutdown`): eviction disconnects workers, wiping both the
    /// enumeration source (`locality_map`) and the transport
    /// (`SmallBlobDispatcher` `worker_tx`). You cannot pull from a worker you
    /// have already evicted.
    ///
    /// UNBOUNDED time (operator directive 2026-06-23: "can't shutdown until we
    /// have pulled all remote blobs on workers into disk on the server"). NO
    /// per-RPC timeout — liveness is the skip-policy + the no-progress
    /// watchdog, consistent with the no-internal-RPC-timeout invariant. The
    /// loop terminates when the residual is empty OR every residual digest has
    /// zero connected source OR the no-progress watchdog fires; never wedges.
    ///
    /// Mechanism: REUSES the existing worker-push backfill
    /// (`WorkerConnection::request_missing_blob_uploads`) — the SAME
    /// `has_with_results`-gated `mark_stable` (BIS durability oath, design §9,
    /// NOT weakened) + the SAME `UploadMissingBlobs` send the worker handles
    /// via `local_worker.rs:handle_upload_missing_blobs` (streams blob → server
    /// CAS, never buffers the whole blob). The ONLY new code here is the
    /// completion-detection poll loop + the enumeration + the skip-policy + the
    /// progress logging. No new proto, no new transfer mechanism, no fsync —
    /// blobs land via the normal async CAS write path (durability = mirror
    /// ≥2-replica + ZFS `sync=disabled`).
    pub async fn pull_all_worker_blobs_at_shutdown(&self) -> ShutdownPullSummary {
        let Some(puller) = self.shutdown_puller() else {
            info!(
                "shutdown pull phase: no locality_map / cas_store / dispatcher \
                 configured — nothing to pull (standalone / test run without \
                 worker mirror)"
            );
            return ShutdownPullSummary::default();
        };
        puller.run().await
    }

    /// (#58 directive-2) Build the standalone [`ShutdownPuller`] handle the
    /// SIGTERM closure drives BEFORE the `WorkerApiServer` is consumed by
    /// `into_service` (the tonic service owns it by value, so the bin cannot
    /// keep the server itself — design §3.4 / F5). The handle holds cheap
    /// clones of exactly the four shared handles the pull reads
    /// (`locality_map`, `cas_store`, `small_blob_dispatcher`, `metrics`); all
    /// are process-wide `Arc`/`Store` singletons, so the handle observes the
    /// SAME live locality map + CAS the server does. Returns `None` when any of
    /// the three required handles is absent (standalone / test runs).
    #[must_use]
    pub fn shutdown_puller(&self) -> Option<ShutdownPuller> {
        let locality_map = self.locality_map.clone()?;
        let cas_store = self.cas_store.clone()?;
        let dispatcher = self.small_blob_dispatcher.clone()?;
        Some(ShutdownPuller {
            locality_map,
            cas_store,
            dispatcher,
            metrics: self.metrics.clone(),
        })
    }

    /// (#58 directive-3: server durability bundle — persist the locality map)
    ///
    /// Snapshot the live blob-locality map JOINED with `endpoint_state` (which
    /// carries the per-endpoint `boot_epoch`) into the on-disk
    /// [`PersistedLocalityMap`] shape, then write it atomically to `path`.
    ///
    /// Runs at graceful SIGTERM **Phase 3.5 — AFTER directive-2's worker-pull,
    /// BEFORE worker eviction**. Eviction's `remove_endpoint` wipes the map, so
    /// the persist MUST precede it (the same crux directive-2's pull has).
    ///
    /// NO fsync (HARD CONSTRAINT): the write is `tokio::fs::write`(tmp) +
    /// `tokio::fs::rename` — an atomic torn-write-avoidance primitive, NOT a
    /// durability primitive. Durability is best-effort (ZFS txg-commit + worker
    /// re-announce on the rebuild path); a SIGKILL skips this entirely and the
    /// map rebuilds from worker full-snapshot `BlobsAvailable`. The bincode
    /// encode runs in `spawn_blocking` so a multi-hundred-MB serialize never
    /// blocks a tokio worker.
    ///
    /// Returns the number of (endpoint, digest) pairs persisted (for logging).
    /// Fails-soft: a serialize / write error is returned as `Err` so the SIGTERM
    /// handler can log it and proceed to eviction (never blocks exit). Delegates
    /// to [`LocalityPersister`] (the handle the bin uses post-`into_service`);
    /// this method exists so tests + the in-server path share one code path.
    pub async fn persist_locality_to_disk(&self, path: &std::path::Path) -> Result<usize, Error> {
        let Some(persister) = self.locality_persister() else {
            info!("locality persist: no locality_map configured — nothing to persist");
            return Ok(0);
        };
        persister.persist_to_disk(path).await
    }

    /// (#58 directive-3) Reload the persisted locality map from `path` and prime
    /// BOTH `locality_map` AND `endpoint_state`. Delegates to
    /// [`LocalityPersister::reload_from_disk`] — see its docs for the
    /// priming-trap guard (design §4.5) and the fail-open guarantee.
    pub async fn reload_locality_from_disk(
        &self,
        path: &std::path::Path,
    ) -> Result<ReloadedLocalitySummary, Error> {
        let Some(persister) = self.locality_persister() else {
            info!("locality reload: no locality_map configured — nothing to reload");
            return Ok(ReloadedLocalitySummary::empty());
        };
        persister.reload_from_disk(path).await
    }

    /// (#58 directive-3) Never-reconnect TTL sweep (design §4.4). Delegates to
    /// [`LocalityPersister::sweep_unconfirmed`].
    pub fn sweep_unconfirmed_reloaded_locality(&self, grace: Duration) -> usize {
        let Some(persister) = self.locality_persister() else {
            return 0;
        };
        persister.sweep_unconfirmed(grace)
    }

    /// (#58 directive-3) Build the standalone [`LocalityPersister`] handle the
    /// SIGTERM closure + startup reload drive, extracted BEFORE the
    /// `WorkerApiServer` is consumed by `into_service` (the same pattern as
    /// [`WorkerApiServer::shutdown_puller`]). Holds cheap clones of the
    /// `locality_map` + `endpoint_state` (both process-wide singletons) so the
    /// handle observes the SAME live state the server does. Returns `None` when
    /// there is no `locality_map` (standalone / test runs).
    #[must_use]
    pub fn locality_persister(&self) -> Option<LocalityPersister> {
        let locality_map = self.locality_map.clone()?;
        Some(LocalityPersister {
            locality_map,
            endpoint_state: self.endpoint_state.clone(),
            reload_baseline: self.locality_reload_baseline.clone(),
        })
    }
}

/// (#58 directive-2: server durability bundle — shutdown WORKER-PULL phase)
///
/// Standalone handle that drives the shutdown worker-pull. Held by the SIGTERM
/// closure in `nativelink.rs` (built via [`WorkerApiServer::shutdown_puller`]
/// before the server is moved into its tonic service) and used by tests via
/// [`WorkerApiServer::pull_all_worker_blobs_at_shutdown`], which delegates here.
///
/// Pulls every worker-resident CAS blob the server does NOT already hold
/// durably down to the server CAS, so that after a restart the server serves
/// ALL blobs locally with ZERO dependence on worker re-backfill (closes the
/// 2026-06-23 restart's "lost input #21" window — design
/// `shutdown-pull-worker-blobs-design-2026-06-23.md`).
///
/// MUST run BEFORE worker eviction (the SIGTERM handler reorders it ahead of
/// `scheduler.shutdown`): eviction disconnects workers, wiping both the
/// enumeration source (`locality_map`) and the transport (`SmallBlobDispatcher`
/// `worker_tx`). You cannot pull from a worker you have already evicted.
///
/// UNBOUNDED time (operator directive 2026-06-23: "can't shutdown until we have
/// pulled all remote blobs on workers into disk on the server"). NO per-RPC
/// timeout — liveness is the skip-policy + the no-progress watchdog, consistent
/// with the no-internal-RPC-timeout invariant. The loop terminates when the
/// residual is empty OR every residual digest has zero connected source OR the
/// no-progress watchdog fires; never wedges.
///
/// Mechanism: REUSES the existing worker-push backfill
/// (`WorkerConnection::request_missing_blob_uploads`) — the SAME
/// `has_with_results`-gated `mark_stable` (BIS durability oath, design §9, NOT
/// weakened) + the SAME `UploadMissingBlobs` send the worker handles via
/// `local_worker.rs:handle_upload_missing_blobs` (streams blob → server CAS,
/// never buffers the whole blob). The ONLY new code is the completion-detection
/// poll loop + the enumeration + the skip-policy + the progress logging. No new
/// proto, no new transfer mechanism, no fsync — blobs land via the normal async
/// CAS write path (durability = mirror ≥2-replica + ZFS `sync=disabled`).
#[derive(Clone)]
pub struct ShutdownPuller {
    locality_map: SharedBlobLocalityMap,
    cas_store: Store,
    dispatcher: Arc<SmallBlobDispatcher>,
    metrics: Arc<WorkerApiMetrics>,
}

impl ShutdownPuller {
    /// Drive the shutdown worker-pull to completion. See the type docs.
    pub async fn run(&self) -> ShutdownPullSummary {
        let start = Instant::now();
        let locality_map = &self.locality_map;
        let cas_store = &self.cas_store;
        let dispatcher = &self.dispatcher;

        // Enumeration (design §4): snapshot the locality map ONCE under the
        // read lock into an owned Vec, then drop the lock. digest → the set of
        // worker endpoints that hold it.
        // ~72 B/pair (string-encoded DigestInfo), CAPPED AT pair_count:
        // one-shot locality snapshot + a small endpoint Vec, dropped when this
        // fn returns. Bounded by the fleet CAS union; blob BYTES are never
        // collected here — they stream worker → server CAS → disk via the
        // existing backfill path.
        let pull_set: Vec<(DigestInfo, Vec<Arc<str>>)> = {
            let guard = locality_map.read();
            guard
                .blobs_map()
                .iter()
                .map(|(digest, endpoints)| {
                    (*digest, endpoints.iter().cloned().collect::<Vec<Arc<str>>>())
                })
                .collect()
        };
        let connected_count = dispatcher.connected_workers_with_senders().len();
        info!(
            pull_set = pull_set.len(),
            connected_workers = connected_count,
            "shutdown pull phase starting"
        );
        if pull_set.is_empty() {
            return ShutdownPullSummary {
                pulled: 0,
                at_risk_skipped: 0,
                elapsed_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            };
        }

        // Residual: digest → known-holder endpoints. Mutated as digests land.
        let mut residual: HashMap<DigestInfo, Vec<Arc<str>>> = pull_set.into_iter().collect();

        // Drop digests ALREADY present in the server CAS up front (design §4.2:
        // the directive-1 flush already persisted MemoryStore-only blobs, so a
        // large fraction of the set is typically already present). These are
        // NOT counted in `pulled` — `pulled` reports only blobs the pull itself
        // localized (digests that go absent → present DURING the loop).
        Self::drop_present_from_residual(cas_store, &mut residual).await;

        let mut pulled = 0usize;
        let mut no_progress_iters: u32 = 0;
        let mut at_risk_skipped = 0usize;

        loop {
            if residual.is_empty() {
                break;
            }

            // Recompute the connected-worker set + transport EVERY iteration so
            // a mid-pull disconnect drops that worker from the source set and
            // the next iteration re-routes to a surviving replica (design §5.2).
            let connected: HashMap<Arc<str>, mpsc::UnboundedSender<UpdateForWorker>> = dispatcher
                .connected_workers_with_senders()
                .into_iter()
                .map(|(endpoint, _epoch, tx)| (endpoint, tx))
                .collect();

            // Count residual digests that STILL have ≥1 connected source.
            // A digest is unpullable ONLY if ALL of its locality-workers are
            // disconnected (design §5.3); given ≥2-replica that requires ≥2
            // simultaneous worker losses at shutdown.
            let pullable_remaining = residual
                .values()
                .filter(|endpoints| endpoints.iter().any(|ep| connected.contains_key(ep)))
                .count();

            if pullable_remaining == 0 {
                // Nothing left has a connected source: every residual digest is
                // unpullable. Skip them all (NEVER wedge) and exit. Justified by
                // ≥2-replica (design §5.3): a zero-connected-source blob is
                // exactly the case where the invariant was already violated
                // (both replicas gone). Phase 3 cannot conjure a blob no
                // connected worker holds, and refusing to exit would only
                // deadlock the restart — strictly worse than a single-blob gap.
                for digest in residual.keys() {
                    let endpoints: Vec<&Arc<str>> = residual[digest].iter().collect();
                    warn!(
                        ?digest,
                        known_workers = ?endpoints,
                        "at-risk unpullable on shutdown: all_disconnected — no \
                         connected worker holds this blob; skipping (≥2-replica \
                         already violated, refusing to wedge the restart)"
                    );
                }
                // Escalate the entire residual to at-risk-skip and exit. (To
                // reproduce the zero-source wedge for the
                // `shutdown_pull_skips_zero_source_blob_and_does_not_hang`
                // mutation: replace these three lines with `continue;` — the
                // loop then spins forever and the test's deadlock detector
                // fires with its bespoke "MUST NOT wedge" message.)
                at_risk_skipped += residual.len();
                residual.clear();
                break;
            }

            // For each connected worker, drive the existing backfill send-half
            // for the subset of the residual that worker holds. A FRESH
            // per-iteration inflight map means no digest is suppressed across
            // iterations, so a re-route to a sibling on the NEXT iteration is
            // immediate (not blocked by the 60 s production re-request window).
            let fresh_inflight = parking_lot::Mutex::new(HashMap::new());
            for (endpoint, tx) in &connected {
                let subset: Vec<DigestInfo> = residual
                    .iter()
                    .filter(|(_, endpoints)| endpoints.iter().any(|ep| ep == endpoint))
                    .map(|(d, _)| *d)
                    .collect();
                if subset.is_empty() {
                    continue;
                }
                // Synthetic WorkerId for the backfill log lines (transport is
                // the tx; the id is only used for logging in the send-half).
                let worker_id = WorkerId(endpoint.to_string());
                WorkerConnection::request_missing_blob_uploads(
                    cas_store,
                    tx,
                    &worker_id,
                    &subset,
                    &fresh_inflight,
                    // send_uploads_if_missing: the pull MUST send (no cooldown).
                    true,
                    &self.metrics,
                    // (#sigkill-gap) `None` = NOT subject to the worker-intake
                    // quiesce latch: ShutdownPuller IS the shutdown drain (the
                    // server-initiated pull), not the storm. It must keep
                    // soliciting worker uploads even after Phase 0b set the
                    // latch — that is the whole point of the pull phase.
                    None,
                )
                .await;
            }

            // Give the worker uploads a moment to land, then re-check the
            // residual for completion (design §3.2 step 4).
            tokio::time::sleep(SHUTDOWN_PULL_POLL_INTERVAL).await;
            let before = residual.len();
            Self::drop_present_from_residual(cas_store, &mut residual).await;
            let landed = before - residual.len();
            pulled += landed;

            // No-progress watchdog (design §7): a half-open / stalled worker
            // keeps a digest's source "connected" but never uploads. If the
            // residual does not shrink for SHUTDOWN_PULL_NO_PROGRESS_ITERS
            // consecutive polls, escalate the remaining residual to at-risk-
            // skip and exit so the unbounded loop cannot wedge the restart.
            if landed == 0 {
                no_progress_iters += 1;
            } else {
                no_progress_iters = 0;
            }
            info!(
                pulled,
                remaining = residual.len(),
                at_risk = at_risk_skipped,
                no_progress_iters,
                "shutdown pull progress"
            );
            if no_progress_iters >= SHUTDOWN_PULL_NO_PROGRESS_ITERS {
                for digest in residual.keys() {
                    let endpoints: Vec<&Arc<str>> = residual[digest].iter().collect();
                    warn!(
                        ?digest,
                        known_workers = ?endpoints,
                        no_progress_iters,
                        "at-risk unpullable on shutdown: stalled_source — a \
                         connected worker holds this blob but has not uploaded \
                         within the no-progress window; skipping (refusing to \
                         wedge the restart)"
                    );
                }
                at_risk_skipped += residual.len();
                residual.clear();
                break;
            }
        }

        let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        info!(
            pulled,
            at_risk_skipped, elapsed_ms, "shutdown pull complete"
        );
        ShutdownPullSummary {
            pulled,
            at_risk_skipped,
            elapsed_ms,
        }
    }

    /// Remove from `residual` every digest now present in the server CAS,
    /// batched (`BACKFILL_BATCH_SIZE`) to avoid an O(N) per-digest existence
    /// storm. Errors on a batch are logged and that batch is left in the
    /// residual (a transient `has_with_results` failure must not drop a
    /// not-yet-durable blob from the pull set).
    async fn drop_present_from_residual(
        cas_store: &Store,
        residual: &mut HashMap<DigestInfo, Vec<Arc<str>>>,
    ) {
        let digests: Vec<DigestInfo> = residual.keys().copied().collect();
        for chunk in digests.chunks(BACKFILL_BATCH_SIZE) {
            let keys: Vec<StoreKey<'_>> = chunk.iter().map(|d| StoreKey::from(*d)).collect();
            let mut results = vec![None; keys.len()];
            if let Err(err) = cas_store.has_with_results(&keys, &mut results).await {
                warn!(
                    ?err,
                    count = chunk.len(),
                    "shutdown pull: has_with_results failed for a residual batch; \
                     leaving it in the pull set (a transient failure must not \
                     drop a not-yet-durable blob)"
                );
                continue;
            }
            for (digest, present) in chunk.iter().zip(results.iter()) {
                if present.is_some() {
                    residual.remove(digest);
                }
            }
        }
    }
}

#[tonic::async_trait]
impl WorkerApi for WorkerApiServer {
    type ConnectWorkerStream = ConnectWorkerStream;

    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn connect_worker(
        &self,
        grpc_request: tonic::Request<tonic::Streaming<UpdateForScheduler>>,
    ) -> Result<Response<Self::ConnectWorkerStream>, Status> {
        let resp = self
            .inner_connect_worker(grpc_request.into_inner())
            .await
            .map_err(Into::into);
        if resp.is_ok() {
            debug!(return = "Ok(<stream>)");
        }
        resp
    }

    // #212 v4.5: WriteChunked moved off WorkerApi to CasExtensions; see
    // `chunked_write_handler::ChunkedWriteHandler`'s `CasExtensions`
    // impl. This trait no longer carries a `write_chunked` method.
}

/// Maximum number of missing digests to request per UploadMissingBlobs message.
/// Keeps individual requests manageable and avoids overwhelming the worker.
const BACKFILL_BATCH_SIZE: usize = 1000;

/// Minimum seconds between backfill checks for a single worker.
/// With 10 workers sending BlobsAvailable every 100ms, this prevents
/// up to 100 has_with_results calls/sec on the server CAS.
const BACKFILL_COOLDOWN_SECS: u64 = 5;

/// Seconds after which a backfill request is considered stale and can be
/// re-requested. If a worker hasn't uploaded the blob within this window,
/// the request is assumed to have failed silently.
const BACKFILL_INFLIGHT_TIMEOUT_SECS: u64 = 60;

/// (#58 directive-2 shutdown worker-pull) Poll interval between completion
/// re-checks in `pull_all_worker_blobs_at_shutdown`. Short enough that the
/// shutdown pull observes worker uploads landing promptly; long enough that
/// the `has_with_results` re-check of the residual is not a hot loop. The
/// existence check is mostly served by `ExistenceCacheStore` so the per-poll
/// cost is small even at fleet scale.
const SHUTDOWN_PULL_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// (#58 directive-2) No-progress watchdog threshold for the shutdown pull.
/// A half-open / stalled worker (live TCP, no upload) keeps a digest's source
/// "connected" but never delivers the blob; without a watchdog the unbounded
/// loop would hang forever (a restart-wedge strictly worse than the NotFound
/// window the pull replaces — design §7). If this many consecutive poll
/// iterations reduce the residual by ZERO, the pull escalates the entire
/// remaining residual to at-risk-skip and exits.
///
/// Unlike the steady-state backfill path, `ShutdownPuller` creates a FRESH
/// in-flight map on every outer loop iteration — there is no
/// `BACKFILL_INFLIGHT_TIMEOUT_SECS = 60` dedup window here. The original
/// 280-iter / 70-second window was calibrated against that dedup window and
/// is unjustified for this code path; it caused an unnecessary ~70-second
/// stall when workers cannot find blobs locally (evicted from their
/// `FilesystemStore`).
///
/// 20 × 250 ms = 5 s is sufficient because the watchdog counts only
/// CONSECUTIVE zero-progress polls (`no_progress_iters` resets to 0 the
/// instant ANY blob lands — `landed > 0` below), and progress is gated by the
/// FAST tier: `drop_present_from_residual` calls `has_with_results`, which
/// returns `Some` as soon as a pulled blob is in the server's MemoryStore /
/// in-flight map — it does NOT wait for the slow-tier (ZFS `tank`) write
/// (durability is the Phase-3.6 post-pull flush's job, not the pull's). So a
/// worker that is actually uploading registers progress within ~1 poll
/// interval of its small-blob gRPC round-trip, not within a ZFS-txg latency.
/// The watchdog therefore fires only after 5 s in which NO connected worker
/// landed ANY blob in the fast tier — i.e. all sources are genuinely silent.
/// Phase 2 has already returned (sequential, `nativelink.rs`) so the pull is
/// not contending with the memory-flush for write bandwidth.
///
/// Skipping the residual when the watchdog fires is safe NOT because of the
/// `at_risk` log counter (that field is the running `at_risk_skipped` count —
/// at iteration 190 in the live incident it was 0 only because the watchdog
/// had not yet fired) but because of the ≥2-replica / mirror_blobs invariant:
/// a blob whose sole holder is a connected-but-silent worker is the FL-688
/// surface the pull tries to close, and at-risk-skip merely falls back to the
/// pre-#58 behavior (worker re-backfill on reconnect) rather than wedging the
/// restart forever.
const SHUTDOWN_PULL_NO_PROGRESS_ITERS: u32 = 20;

/// Outcome of `WorkerApiServer::pull_all_worker_blobs_at_shutdown`.
///
/// `pulled` + `at_risk_skipped` together account for the whole pull set; a
/// non-zero `at_risk_skipped` means some worker-only blob(s) could not be
/// localized because no connected worker held them (the ≥2-replica invariant
/// was already violated for those — design §5). The SIGTERM handler logs this
/// and proceeds: refusing to exit would deadlock the restart, which is
/// strictly worse than the durability gap the ≥2-replica invariant already
/// makes unlikely.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShutdownPullSummary {
    /// Worker-only digests localized to the server CAS during the pull.
    pub pulled: usize,
    /// Worker-only digests SKIPPED because no connected worker held them
    /// (zero-connected-source) or the no-progress watchdog fired.
    pub at_risk_skipped: usize,
    /// Wall-clock duration of the pull, milliseconds.
    pub elapsed_ms: u64,
}

/// (#58 directive-3) Synthetic `owner_worker_id` stamped on every endpoint
/// primed into `endpoint_state` at reload, BEFORE any worker reconnects. A real
/// connect overwrites it with a live `WorkerId` (`inner_connect_worker`
/// `state.insert`), so an endpoint still carrying this sentinel after the grace
/// window is one whose worker never reconnected — the never-reconnect TTL sweep
/// (`LocalityPersister::sweep_unconfirmed`) drops exactly those.
const RELOADED_UNCONFIRMED_OWNER: &str = "__reloaded_unconfirmed__";

/// (#58 directive-3) Default never-reconnect grace TTL (design §4.4). A
/// reloaded endpoint whose worker has not reconnected within this window after
/// boot is swept (its persisted entries dropped). 600 s is generous for normal
/// worker reconnect (seconds-to-low-minutes) yet bounds how long a
/// decommissioned/renamed worker's stale entries haunt the map. The bin passes
/// this; tests pass `Duration::ZERO` to force every still-unconfirmed entry past
/// grace.
pub const LOCALITY_PERSIST_RECONNECT_GRACE_SECS: u64 = 600;

/// (#58 directive-3 — server durability bundle: persist the locality map)
///
/// Standalone handle that drives the shutdown locality-map PERSIST and the
/// startup RELOAD + reconciliation priming. Built via
/// [`WorkerApiServer::locality_persister`] BEFORE the server is moved into its
/// tonic service (the same extraction pattern as [`ShutdownPuller`]); held by
/// the SIGTERM closure (persist) and the startup reload task (reload) in
/// `nativelink.rs`, and exercised by the integration tests via the
/// `WorkerApiServer::{persist,reload,sweep}_*` delegators.
///
/// Holds cheap clones of the two shared handles the persist/reload read+mutate:
/// `locality_map` (the digest→endpoint index) and `endpoint_state` (the
/// endpoint→boot_epoch map the #141 wipe reconciles against). Both are
/// process-wide singletons, so the handle observes the SAME live state the
/// server does.
///
/// NO fsync (HARD CONSTRAINT): the persist writes a tmp file then atomically
/// `tokio::fs::rename`s it — a torn-write-avoidance primitive, NOT a durability
/// primitive. Durability is best-effort (ZFS txg-commit + worker re-announce);
/// a SIGKILL skips the persist and the map rebuilds from worker full-snapshot
/// `BlobsAvailable` (slower but lossless — the map is an INDEX, not a durable
/// copy of any blob).
#[derive(Clone)]
pub struct LocalityPersister {
    locality_map: SharedBlobLocalityMap,
    endpoint_state: Arc<parking_lot::Mutex<HashMap<String, EndpointState>>>,
    /// When the startup reload primed the sentinel entries. The never-reconnect
    /// sweep refuses to drop any sentinel entry until this is at least `grace`
    /// old, so the `grace` arg is load-bearing under ANY scheduler (one-shot OR
    /// periodic). `None` until a reload has run (a sweep before any reload is a
    /// no-op — there are no sentinel entries to drop). Shared across clones so
    /// the reload clone's stamp is visible to the sweep clone. See
    /// `sweep_unconfirmed`. `tokio::time::Instant` (NOT std) so the sweep's
    /// `sleep(grace)` and `baseline.elapsed()` share one virtualizable clock —
    /// see `WorkerApiServer::locality_reload_baseline` for the full rationale.
    reload_baseline: Arc<parking_lot::Mutex<Option<tokio::time::Instant>>>,
}

impl LocalityPersister {
    /// Snapshot the live locality map JOINED with `endpoint_state` (for the
    /// per-endpoint `boot_epoch`) and write it atomically to `path`.
    ///
    /// Runs at graceful SIGTERM Phase 3.5 — AFTER directive-2's worker-pull,
    /// BEFORE worker eviction (eviction's `remove_endpoint` wipes the map, so
    /// the persist MUST precede it). Returns the number of (endpoint, digest)
    /// pairs persisted (for logging). The bincode encode runs in
    /// `spawn_blocking` so a multi-hundred-MB serialize never blocks a tokio
    /// worker; the file write/rename use `tokio::fs` (async). NO sync primitive.
    pub async fn persist_to_disk(&self, path: &std::path::Path) -> Result<usize, Error> {
        // Snapshot BOTH maps under their locks into owned data, then DROP the
        // locks before the (potentially large) encode + the .await write —
        // never hold a lock across .await.
        // ~72 B/pair (string-encoded DigestInfo), CAPPED AT pair_count: one
        // owned Vec, dropped after the write. NOT a network path —
        // shutdown-local. Blob BYTES are never collected; only the (digest hash
        // + size) index.
        let endpoint_digests = self.locality_map.read().snapshot_endpoint_blobs();
        let entries: Vec<PersistedEndpoint> = {
            let state = self.endpoint_state.lock();
            endpoint_digests
                .into_iter()
                .map(|(endpoint, digests)| {
                    // The boot_epoch JOIN: an endpoint present in the locality
                    // map but absent from endpoint_state (no live connection at
                    // snapshot time) persists with boot_epoch 0 — the #141 wipe
                    // treats a 0 epoch as "cannot distinguish", so it is wiped on
                    // the worker's next connect (conservative; never leaks).
                    let boot_epoch = state
                        .get(endpoint.as_ref())
                        .map_or(0, |s| s.boot_epoch);
                    PersistedEndpoint {
                        cas_endpoint: endpoint.as_ref().to_string(),
                        boot_epoch,
                        digests,
                    }
                })
                .collect()
        };

        let pair_count: usize = entries.iter().map(|e| e.digests.len()).sum();
        let persisted_at_unix_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let persisted = PersistedLocalityMap::new(persisted_at_unix_s, entries);

        // Encode off the tokio worker (CPU-bound on a multi-hundred-MB struct).
        let bytes = tokio::task::spawn_blocking(move || persisted.serialize_to_bytes())
            .await
            .err_tip(|| "locality persist: serialize task panicked")??;

        // Atomic rename: write tmp, then rename over the canonical path. NO
        // fsync — ZFS txg-commit + best-effort recovery is the durability model.
        let tmp_path = path.with_extension(format!("bin.tmp-{}", std::process::id()));
        tokio::fs::write(&tmp_path, &bytes)
            .await
            .err_tip(|| format!("locality persist: write tmp {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, path)
            .await
            .err_tip(|| format!("locality persist: rename {} -> {}", tmp_path.display(), path.display()))?;
        info!(
            endpoints = self.endpoint_state.lock().len(),
            pairs = pair_count,
            bytes = bytes.len(),
            path = %path.display(),
            "locality persist: wrote map (atomic rename, no fsync)"
        );
        Ok(pair_count)
    }

    /// Reload the persisted map from `path` and prime BOTH `locality_map` AND
    /// `endpoint_state` (with the persisted `boot_epoch` + a sentinel owner).
    ///
    /// THE PRIMING TRAP (design §4.5, the single biggest risk): priming ONLY
    /// `locality_map` and not `endpoint_state` makes `prev == None` on a
    /// rebooted worker's reconnect, so the #141 wipe's `needs_wipe` is false and
    /// the stale persisted entries LEAK. Priming `endpoint_state[endpoint] = {
    /// boot_epoch: B, owner: sentinel }` makes `prev == Some(B)`, so a different
    /// boot_epoch on reconnect fires the EXISTING wipe and a matching one keeps
    /// the entries until the first full snapshot. The reconciliation IS the #141
    /// logic — this priming is the only new code that makes it fire.
    ///
    /// FAIL-OPEN (design §3.3): a missing / corrupt / version-mismatched file
    /// returns `Ok(empty)` (NOT `Err`) so a reload failure degrades to
    /// worker-re-announce and NEVER wedges startup. Only an unexpected read I/O
    /// error other than NotFound is surfaced — and even then the caller in the
    /// bin maps it to fail-open + flips the readiness gate.
    pub async fn reload_from_disk(
        &self,
        path: &std::path::Path,
    ) -> Result<ReloadedLocalitySummary, Error> {
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    path = %path.display(),
                    "locality reload: no persist file (fresh boot / SIGKILL last \
                     run) — starting with an empty map, will rebuild from worker \
                     re-announce"
                );
                return Ok(ReloadedLocalitySummary::empty());
            }
            Err(e) => {
                // Any other read error fails OPEN: log + empty, never wedge.
                error!(
                    path = %path.display(),
                    err = %e,
                    "locality reload: read error — failing OPEN with an empty map"
                );
                return Ok(ReloadedLocalitySummary::empty());
            }
        };

        // Decode off the tokio worker (CPU-bound on a large file).
        let decoded = tokio::task::spawn_blocking(move || {
            PersistedLocalityMap::deserialize_from_bytes(&bytes)
        })
        .await
        .err_tip(|| "locality reload: decode task panicked")?;
        let persisted = match decoded {
            Ok(p) => p,
            Err(e) => {
                // Corrupt / wrong-magic / version-mismatch fails OPEN.
                warn!(
                    path = %path.display(),
                    err = %e,
                    "locality reload: corrupt/foreign persist file — failing OPEN \
                     with an empty map"
                );
                return Ok(ReloadedLocalitySummary::empty());
            }
        };

        let persisted_at_unix_s = persisted.persisted_at_unix_s;
        let mut digests_loaded = 0usize;
        let mut endpoints_loaded = 0usize;
        // Prime BOTH maps. Take the endpoint_state lock for the whole reload so a
        // concurrently-connecting worker either sees the fully-primed prev or no
        // prev — never a half-primed state. (Workers cannot connect before the
        // worker-API listener accepts, but the reload runs concurrently with
        // listener bind, so this is defensive.) Lock-order rule: endpoint_state
        // is taken first and never co-held with flap_history — satisfied (we
        // never touch flap_history here).
        {
            let mut state = self.endpoint_state.lock();
            let mut locality = self.locality_map.write();
            for entry in persisted.entries {
                if entry.digests.is_empty() {
                    continue;
                }
                locality.register_blobs(&entry.cas_endpoint, &entry.digests);
                digests_loaded += entry.digests.len();
                endpoints_loaded += 1;
                // THE PRIMING (trap guard): record the persisted boot_epoch +
                // the sentinel owner so the #141 connect-path wipe reconciles
                // this endpoint on reconnect.
                //
                // `or_insert` (NOT `insert`): the reload runs CONCURRENTLY with
                // the worker-API listener bind (workers may reconnect during the
                // reload window). If a worker has ALREADY connected for this
                // endpoint, `endpoint_state[ep]` is a LIVE entry carrying its
                // real `WorkerId` + boot_epoch — clobbering it with the sentinel
                // would (a) re-stamp a live worker as "unconfirmed" so the
                // never-reconnect sweep could DROP its locality at grace, and
                // (b) overwrite a correct boot_epoch with the stale persisted
                // one. The priming-trap guard only matters for endpoints that
                // have NOT reconnected yet (the leak case); those are ABSENT
                // here, so `or_insert` primes them exactly as before. A
                // reconnected worker already won the #141 reconciliation, so it
                // needs no priming.
                state.entry(entry.cas_endpoint).or_insert_with(|| EndpointState {
                    boot_epoch: entry.boot_epoch,
                    owner_worker_id: WorkerId(RELOADED_UNCONFIRMED_OWNER.to_string()),
                });
            }
        }
        // Stamp the reload baseline so the never-reconnect sweep can enforce
        // `grace` (it refuses to drop a sentinel entry until the reload is at
        // least `grace` old). Set AFTER the priming so a concurrent sweep never
        // sees a baseline before the sentinels it gates exist.
        // `tokio::time::Instant` (see field docs): same clock the sweep's
        // `sleep(grace)` advances, so the grace gate is testable under `pause()`.
        *self.reload_baseline.lock() = Some(tokio::time::Instant::now());
        info!(
            endpoints = endpoints_loaded,
            digests = digests_loaded,
            persisted_at_unix_s,
            path = %path.display(),
            "locality reload: primed locality_map + endpoint_state from persist \
             file (boot_epoch reconciliation armed)"
        );
        Ok(ReloadedLocalitySummary {
            endpoints_loaded,
            digests_loaded,
            persisted_at_unix_s,
        })
    }

    /// Never-reconnect TTL sweep (design §4.4). Drops the reloaded entries for
    /// every endpoint that STILL carries the reload sentinel owner (its worker
    /// never reconnected) AND is past `grace`. A reconnected worker's sentinel
    /// was overwritten by a real `WorkerId` at connect, so it is NOT swept.
    /// Returns the number of endpoints swept.
    ///
    /// Lock-order: takes `endpoint_state` first, then `locality_map.write()` for
    /// the `remove_endpoint` — the SAME order the #141 wipe uses
    /// (`inner_connect_worker`), so the sweep cannot race a concurrent reconnect
    /// into a half-applied state. Never touches `flap_history`.
    ///
    /// `grace` is LOAD-BEARING: the sweep refuses to drop ANY sentinel entry
    /// until the startup reload baseline (`reload_baseline`, stamped at the end
    /// of `reload_from_disk`) is at least `grace` old. This makes the sweep safe
    /// under a periodic scheduler (a tick before grace is a no-op) AND under a
    /// one-shot-at-grace scheduler, rather than relying on the caller to fire at
    /// exactly `grace`. A sweep before any reload (no baseline) is a no-op.
    /// Identity (the sentinel) AND time (the baseline + grace) are BOTH checked:
    /// a reconnect clears the sentinel, and grace must have elapsed.
    #[must_use]
    pub fn sweep_unconfirmed(&self, grace: Duration) -> usize {
        // Time gate: do not sweep until the reload is at least `grace` old.
        // Before any reload there is no baseline (and no sentinel entries), so
        // this is a no-op. `saturating` via the `Option` + `elapsed` compare.
        match *self.reload_baseline.lock() {
            None => return 0,
            Some(baseline) if baseline.elapsed() < grace => {
                return 0;
            }
            Some(_) => {}
        }
        let mut state = self.endpoint_state.lock();
        let stale: Vec<String> = state
            .iter()
            .filter(|(_, s)| s.owner_worker_id.0 == RELOADED_UNCONFIRMED_OWNER)
            .map(|(endpoint, _)| endpoint.clone())
            .collect();
        if stale.is_empty() {
            return 0;
        }
        let mut locality = self.locality_map.write();
        for endpoint in &stale {
            locality.remove_endpoint(endpoint);
            state.remove(endpoint);
        }
        drop(locality);
        drop(state);
        info!(
            swept = stale.len(),
            "locality reload: swept never-reconnected reloaded endpoints past grace"
        );
        stale.len()
    }

    /// (#58 directive-3 — task #66) The startup never-reconnect sweep SCHEDULER.
    ///
    /// Drives the single sweep fire at `grace` after the RELOAD BASELINE — NOT
    /// after boot. This is the load-bearing ordering: `reload_from_disk` stamps
    /// `reload_baseline` at its END (after the async file read + decode), so the
    /// baseline is `boot + reload_duration`. If the grace timer started at boot
    /// (sleeping `grace` from `t_spawn ≈ boot`), then at fire time
    /// `baseline.elapsed() = grace − reload_duration < grace`, and
    /// `sweep_unconfirmed`'s strict `<` time-gate returns 0 EVERY boot — the
    /// sweep is a guaranteed no-op and never-reconnect entries leak forever
    /// (distsys MAJOR-1, re-opened). Awaiting `reload_done` BEFORE the `sleep`
    /// guarantees the timer starts at-or-after the baseline stamp, so
    /// `baseline.elapsed() >= grace` holds at fire.
    ///
    /// `reload_done` is the reload-completion signal — in the bin, the
    /// `bazel_ready` readiness gate (flipped `true` only AFTER `reload_from_disk`
    /// returns, which is after the baseline is stamped); in tests, the
    /// `reload_from_disk` future itself (so the test couples the sleep-start to
    /// the baseline-stamp, the exact production composition). Consumes `self`
    /// (the task owns the handle for its lifetime). Returns the number of
    /// endpoints swept (for logging / assertion).
    ///
    /// NO blocking primitive: `tokio::time::sleep` (virtualizable under
    /// `tokio::time::pause`), no `std::thread::sleep`. One-shot, not periodic:
    /// the reload happens once per boot, so a single correctly-timed fire covers
    /// the whole reloaded set; a worker that reconnects after grace simply
    /// re-registers (its entries are live, not sentinel).
    #[must_use]
    pub async fn run_never_reconnect_sweep<F>(self, reload_done: F, grace: Duration) -> usize
    where
        F: core::future::Future<Output = ()>,
    {
        // Start the grace timer from the BASELINE, not boot: await reload
        // completion FIRST so the sleep begins at-or-after the baseline stamp.
        reload_done.await;
        tokio::time::sleep(grace).await;
        self.sweep_unconfirmed(grace)
    }
}

struct WorkerConnection {
    scheduler: Arc<dyn WorkerScheduler>,
    now_fn: Arc<NowFn>,
    worker_id: WorkerId,
    locality_map: Option<SharedBlobLocalityMap>,
    /// AC pin registry (separate from `locality_map`); see
    /// `WorkerApiServer::ac_pin_registry` for design.
    // CAPPED: AcPinRegistry enforces DEFAULT_MAX_AC_PINS_PER_ENDPOINT = 1_000_000 internally.
    ac_pin_registry: Option<SharedAcPinRegistry>,
    /// (#12 H4) Pending output locality registry — second AcPinRegistry
    /// instance, separate from `ac_pin_registry`. Wiped on disconnect and
    /// boot-epoch change via the same ownership-check guard as `ac_pin_registry`.
    ///
    /// SHORT-CIRCUIT GUARD: NEVER consult this registry from
    /// `has_with_results` or any path reachable from it.  See
    /// `WorkerApiServer::pending_output_locality_registry` for the full
    /// design note.
    // CAPPED: AcPinRegistry enforces DEFAULT_MAX_AC_PINS_PER_ENDPOINT = 1_000_000 internally.
    pending_output_locality_registry: Option<SharedAcPinRegistry>,
    /// CAS store for checking blob existence during backfill.
    cas_store: Option<Store>,
    /// WorkerProxyStore handle for plumbing per-endpoint mirror
    /// capacity reports (review #1).
    worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
    /// SmallBlobDispatcher handle (task #168). Used to:
    ///   - broadcast `pinned_mirror_entries` (proto field 16) on every
    ///     `BlobsAvailable` tick;
    ///   - call `unregister_worker(endpoint, boot_epoch)` on disconnect
    ///     so per-(worker, store) drainers exit gracefully.
    small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    cas_endpoint: String,
    /// `boot_epoch_id` reported by this connection. Logged on cleanup
    /// for #141 traceability.
    boot_epoch: u64,
    /// Shared per-endpoint connection state — owned by
    /// `WorkerApiServer`. The disconnect-cleanup task reads this to
    /// decide whether its `remove_endpoint` is still authoritative
    /// (the entry's `owner_worker_id` must match this connection's
    /// `worker_id`; otherwise a newer connection has already taken
    /// over the endpoint and our cleanup would erase its entries).
    endpoint_state: Arc<parking_lot::Mutex<HashMap<String, EndpointState>>>,
    /// Channel to send messages back to this worker.
    worker_tx: mpsc::UnboundedSender<UpdateForWorker>,
    /// Epoch seconds of the last backfill check for this worker.
    /// Used to enforce a per-worker cooldown between backfill runs.
    last_backfill_epoch_secs: AtomicU64,
    /// Digests currently being backfilled (requested from the worker but not
    /// yet confirmed in the server CAS). Keyed by digest, value is the time
    /// the request was sent. Entries older than `BACKFILL_INFLIGHT_TIMEOUT_SECS`
    /// are considered stale and eligible for re-request.
    backfill_inflight: Arc<parking_lot::Mutex<HashMap<DigestInfo, Instant>>>,
    /// Shared metrics handle (cloned from `WorkerApiServer::metrics`).
    metrics: Arc<WorkerApiMetrics>,
    /// (#sigkill-gap) Worker-intake quiesce latch (cloned from
    /// `WorkerApiServer::shutdown_quiesce`). Read at the entry of the
    /// HANDLER-invoked `request_missing_blob_uploads` so the backfill +
    /// pinned-mirror-pull feeds stop soliciting new worker uploads once SIGTERM
    /// Phase 0b flips it.
    shutdown_quiesce: ShutdownQuiesce,
    /// (#99) Per-connection accumulator for chunked
    /// `BlobsAvailableChunk` envelopes. Path A semantics: chunks
    /// buffer here until `is_last=true` lands; the accumulator then
    /// hands the fully-assembled `BlobsAvailableNotification` to the
    /// existing `handle_blobs_available` for the legacy
    /// `remove_endpoint` + `register_blobs_iter` + AC pin replace +
    /// mirror pipeline. Bound: at most
    /// `MAX_INFLIGHT_BROADCASTS_PER_CONN` × per-broadcast cap of
    /// `MAX_ACCUMULATED_ENTRIES_PER_CONN` entries (~20 MB worst-case
    /// per connection). Dropped on disconnect via `drop_all_inflight`.
    blobs_available_accumulator: Arc<crate::blobs_available_accumulator::BlobsAvailableAccumulator>,
    /// (FL-688 v3 Stage C — DOC-FIX-1) Whether we have already sent the
    /// `ReconcileCompleteRequest` signal (tag 14) to this worker on this
    /// connection. We send it exactly ONCE per `WorkerConnection`: after the
    /// first full-snapshot `BlobsAvailableNotification` is processed.
    ///
    /// Reconnects produce a new `WorkerConnection` with this SERVER-SIDE flag
    /// reset to `false`, so the server will re-send `ReconcileComplete` on
    /// every new connection — which is correct, because a reconnecting worker
    /// needs its gate re-released.
    ///
    /// IMPORTANT: this flag controls only the SERVER side. The WORKER-SIDE gate
    /// (`FilesystemStore::reconcile_complete` Arc<AtomicBool>) is armed ONCE at
    /// `FilesystemStore::new` and is NOT re-armed on reconnect (see
    /// `local_worker.rs:4343-4347`, MAJOR-1 fix). After first release the worker
    /// gate stays released — subsequent `ReconcileComplete` signals from the
    /// server are handled as no-ops on the worker side (load returns `true`,
    /// no-op store).
    ///
    /// Uses `Arc<AtomicBool>` so the flag can be shared into the
    /// `blobs_available_mark_stable_and_backfill` background task and the
    /// `ReconcileComplete` send can occur as the LAST statement of that task
    /// (BLOCK-1 ordering fix: uploads BEFORE gate-release, same task).
    reconcile_complete_sent: Arc<AtomicBool>,
}

impl WorkerConnection {
    #[allow(clippy::too_many_arguments)]
    fn start(
        scheduler: Arc<dyn WorkerScheduler>,
        now_fn: Arc<NowFn>,
        worker_id: WorkerId,
        locality_map: Option<SharedBlobLocalityMap>,
        ac_pin_registry: Option<SharedAcPinRegistry>,
        pending_output_locality_registry: Option<SharedAcPinRegistry>,
        cas_store: Option<Store>,
        worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
        cas_endpoint: String,
        boot_epoch: u64,
        endpoint_state: Arc<parking_lot::Mutex<HashMap<String, EndpointState>>>,
        worker_tx: mpsc::UnboundedSender<UpdateForWorker>,
        metrics: Arc<WorkerApiMetrics>,
        shutdown_quiesce: ShutdownQuiesce,
        mut connection: impl Stream<Item = Result<UpdateForScheduler, Status>> + Unpin + Send + 'static,
    ) {
        let instance = Self {
            scheduler,
            now_fn,
            ac_pin_registry,
            pending_output_locality_registry,
            worker_id,
            locality_map,
            cas_store,
            worker_proxy,
            small_blob_dispatcher,
            cas_endpoint,
            boot_epoch,
            endpoint_state,
            worker_tx,
            last_backfill_epoch_secs: AtomicU64::new(0),
            backfill_inflight: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            // (#99) Per-connection accumulator. Created fresh per
            // ConnectWorker so a worker reconnect starts with empty
            // partial state. The drop_counts Arc is shared with the
            // server-level `WorkerApiMetrics`
            // (`chunked_blobs_available_drop_counts`) so per-reason
            // drops aggregate across every connection on this server
            // and surface on the metrics tree (S1 follow-up to the
            // 8fa531ae code-reviewer pass). Built BEFORE moving
            // `metrics` into the struct so we can read its Arc field.
            blobs_available_accumulator:
                crate::blobs_available_accumulator::BlobsAvailableAccumulator::new_with_drop_counts(
                    Arc::clone(&metrics.chunked_blobs_available_drop_counts),
                ),
            metrics,
            shutdown_quiesce,
            reconcile_complete_sent: Arc::new(AtomicBool::new(false)),
        };

        background_spawn!("worker_api", async move {
            let mut had_going_away = false;
            while let Some(maybe_update) = connection.next().await {
                let update = match maybe_update.map(|u| u.update) {
                    Ok(Some(update)) => update,
                    Ok(None) => {
                        tracing::warn!(worker_id=?instance.worker_id, "Empty update");
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(worker_id=?instance.worker_id, ?err, "Error from worker");
                        break;
                    }
                };
                let result = match update {
                    Update::ConnectWorkerRequest(_connect_worker_request) => Err(make_err!(
                        Code::Internal,
                        "Got ConnectWorkerRequest after initial message for {}",
                        instance.worker_id
                    )),
                    Update::KeepAliveRequest(keep_alive_request) => {
                        instance.inner_keep_alive(keep_alive_request).await
                    }
                    Update::GoingAwayRequest(going_away_request) => {
                        had_going_away = true;
                        instance.inner_going_away(going_away_request).await
                    }
                    Update::ExecuteResult(execute_result) => {
                        instance.inner_execution_response(execute_result).await
                    }
                    Update::ExecuteComplete(execute_complete) => {
                        instance.execution_complete(execute_complete).await
                    }
                    Update::BlobsAvailable(notification) => {
                        instance.handle_blobs_available(notification).await
                    }
                    Update::BlobsEvicted(_notification) => {
                        // Dead code path: evictions now go through
                        // BlobsAvailableNotification.evicted_digests.
                        // Kept for wire compatibility with older workers.
                        Ok(())
                    }
                    Update::BisAck(ack) => {
                        // (#97) BIS chunked-ack receipt — route into the
                        // scheduler so the per-worker resend buffer can
                        // drop the matching chunk. Default trait impl is
                        // a no-op; only api_worker_scheduler tracks the
                        // resend state.
                        //
                        // The scheduler's `bis_ack_received` validates
                        // `server_instance_token` against its own token
                        // (red-team #5: stale acks across server bounces
                        // would otherwise drop unrelated chunks).
                        instance
                            .scheduler
                            .bis_ack_received(
                                &instance.worker_id,
                                ack.broadcast_id,
                                ack.sequence,
                                ack.server_instance_token,
                            )
                            .await;
                        Ok(())
                    }
                    Update::ChunkedMessage(envelope) => {
                        // (#99) One chunk of a streaming protocol message
                        // FROM the worker. Today the only payload arm is
                        // `BlobsAvailableChunk`; future PRs may add more
                        // worker→server chunked types under the same
                        // envelope.
                        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::chunked_message;
                        match envelope.payload {
                            Some(chunked_message::Payload::BlobsAvailable(chunk)) => {
                                // (FL-688 v3 §3.8) Capture the ack triple
                                // BEFORE `merge_chunk_outcome` consumes the
                                // chunk. The worker echoes its
                                // `worker_instance_token` back so it can
                                // drop acks across its own bounce.
                                let ack_broadcast_id = chunk.broadcast_id;
                                let ack_sequence = chunk.sequence;
                                let ack_worker_token = chunk.worker_instance_token;
                                use crate::blobs_available_accumulator::MergeOutcome;
                                let outcome = instance
                                    .blobs_available_accumulator
                                    .merge_chunk_outcome(chunk);
                                match outcome {
                                    MergeOutcome::Accepted(maybe_notification) => {
                                        // The chunk merged → ack it
                                        // (per-chunk, NOT per-broadcast) so
                                        // the worker drops it from its
                                        // resend buffer (drain-on-ack). The
                                        // ack rides the server→worker
                                        // `UpdateForWorker` mpsc; a lost ack
                                        // is benign — the worker keeps the
                                        // chunk buffered and RETRANSMITS it
                                        // each tick via `replay_unacked_chunks`
                                        // (drain-on-ack flip) until a fresh
                                        // ack drains the slot.
                                        //
                                        // LOAD-BEARING: ack EVERY accepted
                                        // chunk — terminal (`is_last`) AND
                                        // non-terminal. The worker's per-tick
                                        // replay re-sends EVERY still-unacked
                                        // slot, so a non-terminal chunk that
                                        // is accepted-but-not-acked would be
                                        // resent forever. Moving this
                                        // `worker_tx.send(ack)` INSIDE the
                                        // `if let Some(notification)`
                                        // terminal-commit block below (it
                                        // looks redundant to ack a
                                        // non-terminal chunk) would leave
                                        // multi-chunk deltas' non-terminal
                                        // slots undrained → an unbounded
                                        // per-tick resend storm bounded only
                                        // by the 256 over-cap valve. Do NOT.
                                        //
                                        // ACK-BEFORE-EFFECT: this ack fires
                                        // BEFORE `handle_blobs_available`
                                        // applies the locality/AC-pin/mirror
                                        // effects (terminal chunk only,
                                        // below). If that terminal commit
                                        // returns Err AFTER the ack, the
                                        // worker has already drained the slot
                                        // and replay can no longer re-drive
                                        // it — that window self-heals via the
                                        // worker's RECONNECT full snapshot,
                                        // NOT via replay. So "acked" means
                                        // "server accepted the chunk for
                                        // processing," NOT "server durably
                                        // registered the locality": the
                                        // future eviction-gate stage (v3 §3.4)
                                        // MUST treat a single ack as
                                        // accepted-for-processing and confirm
                                        // durable registration separately.
                                        if let Err(err) =
                                            instance.worker_tx.send(UpdateForWorker {
                                                update: Some(
                                                    update_for_worker::Update::BlobsAvailableAck(
                                                        BlobsAvailableAck {
                                                            broadcast_id: ack_broadcast_id,
                                                            sequence: ack_sequence,
                                                            worker_instance_token: ack_worker_token,
                                                        },
                                                    ),
                                                ),
                                            })
                                        {
                                            tracing::warn!(
                                                worker_id=?instance.worker_id,
                                                broadcast_id = ack_broadcast_id,
                                                sequence = ack_sequence,
                                                ?err,
                                                "failed to send BlobsAvailableAck \
                                                 (worker_tx closed); worker will \
                                                 re-advertise on reconnect"
                                            );
                                        }
                                        if let Some(notification) = maybe_notification {
                                            // Path A commit: terminal chunk
                                            // assembled the full
                                            // notification. Hand to the
                                            // legacy handler (remove_endpoint
                                            // wipe when is_full_snapshot +
                                            // register_blobs_iter + AC pin
                                            // replace + mirror pipeline,
                                            // ATOMICALLY).
                                            instance.handle_blobs_available(notification).await
                                        } else {
                                            // Accepted non-terminal: nothing
                                            // more until the terminal lands.
                                            Ok(())
                                        }
                                    }
                                    MergeOutcome::Dropped => {
                                        // Validation/cap/completeness failure
                                        // — do NOT ack (suppress, mirroring
                                        // handle_bis_chunk) so the worker
                                        // keeps the chunk buffered and
                                        // re-advertises on reconnect. The
                                        // accumulator already warn-logged the
                                        // specific drop reason.
                                        Ok(())
                                    }
                                }
                            }
                            // Other payload arms (PeerHints,
                            // BlobsInStableStorage) are scheduler→worker
                            // direction; receiving them on the
                            // worker→server stream is invalid.
                            Some(other) => {
                                tracing::warn!(
                                    worker_id=?instance.worker_id,
                                    payload=?core::mem::discriminant(&other),
                                    "ChunkedMessage with wrong-direction \
                                     payload arm on worker→server stream; \
                                     ignoring"
                                );
                                Ok(())
                            }
                            None => {
                                tracing::warn!(
                                    worker_id=?instance.worker_id,
                                    "ChunkedMessage with empty payload on \
                                     worker→server stream; ignoring"
                                );
                                Ok(())
                            }
                        }
                    }
                };
                if let Err(err) = result {
                    let msg = format!("{err:?}");
                    if msg.contains("Worker not found") {
                        // Worker was evicted from scheduler (timeout or server restart).
                        // Send Disconnect so the worker knows to reconnect, then close
                        // the stream.
                        warn!(worker_id=?instance.worker_id, "worker not in scheduler map, sending disconnect");
                        let _ = instance.worker_tx.send(UpdateForWorker {
                            update: Some(update_for_worker::Update::Disconnect(())),
                        });
                        break;
                    }
                    tracing::warn!(worker_id=?instance.worker_id, ?err, "Error processing worker message");
                }
            }
            tracing::debug!(worker_id=?instance.worker_id, "Update for scheduler dropped");

            // (#99) Discard any in-flight chunked BlobsAvailable
            // partial state. Path A semantics: a broadcast that never
            // receives its terminal chunk MUST NOT leak its accumulated
            // entries forward to the next connection (the worker
            // re-broadcasts on reconnect anyway). Bounded memory
            // contract requires this drop on disconnect.
            instance.blobs_available_accumulator.drop_all_inflight();

            // Clean up locality map on disconnect — but ONLY if the
            // endpoint is still owned by THIS connection. A newer
            // connection that landed since we registered will have
            // overwritten `endpoint_state[endpoint].owner_worker_id`;
            // in that case our cleanup would wipe the NEW worker's
            // just-registered entries (the production race documented
            // in #141 — a same-epoch reconnect is the worst case
            // because epoch alone cannot distinguish old vs. new
            // connection). Hold the endpoint_state mutex across the
            // locality_map.write() so no other thread can flip the
            // owner out from under us between the check and the wipe.
            //
            // The dispatcher's `unregister_worker` + `unpin_on_disconnect`
            // calls are made INSIDE the same ownership-check guard
            // (per code-reviewer #168 MAJOR-2): a same-epoch reconnect
            // race that has already claimed the endpoint must NOT have
            // its dispatcher state clobbered by the OLD connection's
            // disconnect-cleanup task.
            if !instance.cas_endpoint.is_empty() {
                let mut state = instance.endpoint_state.lock();
                let current_owner = state
                    .get(&instance.cas_endpoint)
                    .map(|s| s.owner_worker_id.clone());
                if current_owner.as_ref() == Some(&instance.worker_id) {
                    if let Some(ref locality_map) = instance.locality_map {
                        locality_map.write().remove_endpoint(&instance.cas_endpoint);
                    }
                    // AC pin sibling: drop every server-side AC pin
                    // claim attached to this endpoint when the
                    // disconnect cleanup runs and our connection is
                    // still the owner. Prevents AC pin entries from
                    // outliving the worker connection that
                    // advertised them.
                    if let Some(ref ac_pin_registry) = instance.ac_pin_registry {
                        ac_pin_registry.wipe_endpoint(&instance.cas_endpoint);
                    }
                    // (#12 H4) Sibling wipe for pending_output_locality_registry:
                    // any pending output pins from this now-disconnected worker
                    // are stale; phase 2 will re-register on the next
                    // UpdateActionResult from the reconnected worker.
                    if let Some(ref pending) = instance.pending_output_locality_registry {
                        pending.wipe_endpoint(&instance.cas_endpoint);
                    }
                    // task #168 (item 6 + unpin_on_disconnect refactor):
                    //
                    //   1. Drop the worker_tx the dispatcher holds so any
                    //      in-flight per-(worker, store) drainer task
                    //      exits gracefully (its UnboundedSender clones
                    //      drop, the mpsc Receiver hits None, the
                    //      drainer returns). Also drops the per-(worker,
                    //      store) queue Senders so a stale boot_epoch
                    //      reconnect does not inherit dead queues.
                    //   2. Release the in-flight server-side push
                    //      tracker (`EphemeralServerSidePin`) for this
                    //      worker — the disconnected worker can no
                    //      longer ack pushed blobs via
                    //      `BlobsAvailable.pinned_mirror_entries`, so
                    //      without this call `pin_max_bytes` would
                    //      fill with ghost pins until the dispatcher
                    //      rejected new admissions with
                    //      ResourceExhausted. (TTL-based eviction was
                    //      removed in the unpin_on_disconnect refactor;
                    //      explicit release on disconnect replaces it.)
                    //
                    // Both calls are gated by the same
                    // owner-still-matches check that gates the
                    // locality_map.write() above (per code-reviewer
                    // #168 MAJOR-2).
                    if let Some(ref dispatcher) = instance.small_blob_dispatcher {
                        dispatcher.unregister_worker(&instance.cas_endpoint, instance.boot_epoch);
                        dispatcher.unpin_on_disconnect(&instance.cas_endpoint, instance.boot_epoch);
                    }
                    // Drop the per-endpoint state: with no live
                    // connection on this endpoint, any future connect
                    // will hit the "no prev" branch and skip the wipe
                    // (the locality_map is already empty for this
                    // endpoint after the line above).
                    state.remove(&instance.cas_endpoint);
                    info!(
                        worker_id=?instance.worker_id,
                        endpoint=%instance.cas_endpoint,
                        boot_epoch=instance.boot_epoch,
                        "Removed worker from blob locality map on disconnect"
                    );
                } else {
                    info!(
                        worker_id=?instance.worker_id,
                        endpoint=%instance.cas_endpoint,
                        my_epoch=instance.boot_epoch,
                        ?current_owner,
                        "Skipped locality_map wipe on disconnect — endpoint has been claimed by a newer connection"
                    );
                }
            }

            if !had_going_away {
                drop(instance.scheduler.remove_worker(&instance.worker_id).await);
            }
        });
    }

    async fn inner_keep_alive(&self, keep_alive_request: KeepAliveRequest) -> Result<(), Error> {
        self.scheduler
            .worker_keep_alive_received(&self.worker_id, (self.now_fn)()?.as_secs())
            .await
            .err_tip(|| "Could not process keep_alive from worker in inner_keep_alive()")?;
        let cpu_load_pct = keep_alive_request.cpu_load_pct;
        let p_core_load_pct = keep_alive_request.p_core_load_pct;
        let e_core_load_pct = keep_alive_request.e_core_load_pct;
        // (#sched-zeroload) UNCONDITIONAL update (previously gated on
        // `> 0`): a genuine all-zero (truly idle) reading is the load-bearing
        // signal that distinguishes a reported-idle worker from a never-reported
        // one. Gating it left a truly-idle worker indistinguishable from
        // pre-first-heartbeat — both stuck at the construction-default `(0,0,0)`
        // → max free-capacity → zero load penalty → over-selection. Recording
        // the zero sets `has_reported_load = true` in the scheduler.
        debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "KeepAlive received with CPU load");
        if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
            warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
        }
        Ok(())
    }

    async fn inner_going_away(&self, _going_away_request: GoingAwayRequest) -> Result<(), Error> {
        self.scheduler
            .remove_worker(&self.worker_id)
            .await
            .err_tip(|| "While calling WorkerApiServer::inner_going_away")?;
        Ok(())
    }

    async fn inner_execution_response(&self, execute_result: ExecuteResult) -> Result<(), Error> {
        let operation_id = OperationId::from(execute_result.operation_id);

        match execute_result
            .result
            .err_tip(|| "Expected result to exist in ExecuteResult")?
        {
            execute_result::Result::ExecuteResponse(finished_result) => {
                // Output-digest registration in the locality map happens
                // exclusively via BlobsAvailable now (audit Path 2 at
                // `.claude/audits/task-139-lost-eviction/audit.md`):
                // the previous in-place `register_action_result_digests`
                // raced `evicted_digests` on the same `mpsc::channel(1)`
                // and could permanently stale the locality map. The
                // worker's BlobsAvailable backstop is 100 ms with
                // immediate Notify on insert, so the latency we lose by
                // dropping the early registration is sub-100 ms. The
                // mark_stable hook that covers already-cached outputs
                // (BIS-coverage audit A1) lives on the BlobsAvailable
                // handler now too — every pin path the worker reports
                // through BlobsAvailable is covered.
                let exit_code = finished_result.result.as_ref().map_or(-1, |r| r.exit_code);
                let action_stage = finished_result
                    .try_into()
                    .err_tip(|| "Failed to convert ExecuteResponse into an ActionStage")?;
                info!(
                    worker_id=?self.worker_id,
                    %operation_id,
                    exit_code,
                    "action completed by worker"
                );
                // #36 Phase 6 §6 Phase 0 probe P-SCHED-COMPLETE-RECV: mark
                // the wall-clock at which the scheduler receives action N's
                // completion. Paired with P-SCHED-DISPATCH at
                // prepare_worker_run_action; the gap = the server-leg
                // component of the action-boundary latency Phase 6 would
                // hide. Observability only, no behaviour change.
                let phase6_recv_at_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0);
                debug!(
                    tag = "phase6_scheduler_complete_recv",
                    op_id_n = %operation_id,
                    recv_at_us = phase6_recv_at_us,
                    "phase6 scheduler received action-N completion"
                );
                self.scheduler
                    .update_action(
                        &self.worker_id,
                        &operation_id,
                        UpdateOperationType::UpdateWithActionStage(action_stage),
                    )
                    .await
                    .err_tip(|| format!("Failed to operation {operation_id}"))?;
            }
            execute_result::Result::InternalError(e) => {
                error!(
                    worker_id=?self.worker_id,
                    %operation_id,
                    ?e,
                    "action failed with internal error"
                );
                self.scheduler
                    .update_action(
                        &self.worker_id,
                        &operation_id,
                        UpdateOperationType::UpdateWithError(e.into()),
                    )
                    .await
                    .err_tip(|| format!("Failed to operation {operation_id}"))?;
            }
        }
        Ok(())
    }

    /// (FL-688 v3 Stage C — BLOCK-1 fix) Static helper: send
    /// `ReconcileCompleteRequest` (tag 14) exactly once.  Called from:
    ///  - The 3 non-backfill exits (no_locality_map, empty_endpoint, fall-through)
    ///    via `try_send_reconcile_complete` on the synchronous handler path —
    ///    these paths send ZERO UploadMissingBlobs so ordering is trivially
    ///    correct.
    ///  - The backfill path INSIDE `background_spawn!`, as the LAST statement
    ///    after the UploadMissingBlobs chunk loop — this is the BLOCK-1 ordering
    ///    fix: uploads ≺ gate-release, guaranteed by program order in one task.
    ///
    /// `reconcile_sent` is `Arc<AtomicBool>` (shared with the struct field) so
    /// the spawned task can take a clone without self capture.  `compare_exchange`
    /// ensures exactly-once even if two full-snapshot ticks race (both reach this
    /// fn before the first send completes; the second's CAS fails and returns).
    fn send_reconcile_complete_static(
        is_full_snapshot: bool,
        reconcile_sent: &AtomicBool,
        worker_tx: &mpsc::UnboundedSender<UpdateForWorker>,
        worker_id: &WorkerId,
    ) {
        if !is_full_snapshot {
            return;
        }
        // Exactly-once: flip false→true atomically.
        if reconcile_sent
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            // Already sent; skip.
            return;
        }
        let msg = UpdateForWorker {
            update: Some(update_for_worker::Update::ReconcileComplete(
                ReconcileCompleteRequest {},
            )),
        };
        if worker_tx.send(msg).is_err() {
            warn!(
                worker_id=?worker_id,
                "ReconcileComplete: worker channel closed before send"
            );
        } else {
            info!(
                worker_id=?worker_id,
                "ReconcileComplete sent: first full BlobsAvailable snapshot processed, \
                 worker startup reconcile gate will be released"
            );
        }
    }

    /// Thin wrapper around `send_reconcile_complete_static` for use on `&self`
    /// from the 3 non-backfill handler exits.
    fn try_send_reconcile_complete(&self, is_full_snapshot: bool) {
        Self::send_reconcile_complete_static(
            is_full_snapshot,
            &self.reconcile_complete_sent,
            &self.worker_tx,
            &self.worker_id,
        );
    }

    async fn handle_blobs_available(
        &self,
        notification: nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableNotification,
    ) -> Result<(), Error> {
        // (Probe #4) Total wall-clock spent inside handle_blobs_available
        // — server-side reception cost per BlobsAvailable tick. Emits at
        // each of the three return points below so per-tick cost is
        // attributable across the three exit paths (no-locality-map,
        // empty-endpoint, normal). Pairs with the worker-side
        // `ac_pin_scan_elapsed_us` to bound the empty-tick storm.
        let handle_blobs_available_start = Instant::now();
        // #168 security MEDIUM Q2 (hoisted from the field-16 block to
        // function entry per second-pass review): a worker self-reports
        // its own `worker_cas_endpoint` in BlobsAvailable. The notification
        // is consumed at THREE sinks below — pinned_mirror_entries (field
        // 16), `record_mirror_capacity`, and the consolidated
        // `register_blobs_iter` / `evict_blobs` / `remove_endpoint` block
        // (field 13 + full-snapshot wipe). All three trust this string;
        // a malicious worker could spoof another peer's endpoint to
        // pollute that peer's locality_map (sinks 1+3) or attribute
        // capacity drift (sink 2). Sink #3's `is_full_snapshot` ×
        // `remove_endpoint` is the load-bearing concern: it lets a
        // malicious worker WIPE another peer's locality entries.
        //
        // Audit at function entry so the warn! covers ALL sinks. Detective
        // only — we still process the report (a benign misconfig is
        // recoverable; rejecting would prevent legit endpoint roll). The
        // operator-visible audit trail is in the log.
        if !notification.worker_cas_endpoint.is_empty()
            && notification.worker_cas_endpoint != self.cas_endpoint
        {
            warn!(
                worker_id=?self.worker_id,
                reported_endpoint=%notification.worker_cas_endpoint,
                connected_endpoint=%self.cas_endpoint,
                "BlobsAvailable: worker self-reported worker_cas_endpoint differs \
                 from connected endpoint — possible spoof or misconfiguration; \
                 processing the report but logging for audit (#168 security MEDIUM Q2; \
                 covers all 3 sinks: pinned_mirror_entries, record_mirror_capacity, \
                 register_blobs_iter+remove_endpoint)"
            );
        }
        // task #168 (item 1): broadcast `pinned_mirror_entries` (proto
        // field 16) to every registered FastSlowStore via the dispatcher.
        // Each `EphemeralServerSidePin` binary-searches the entries slice
        // for its own `store_id` region and removes confirmed-held
        // entries (Option F: broadcast + self-filter; per plan §"Routing").
        // Pre-existing field 13 path (`pinned_mirror_digests`, below)
        // is UNCHANGED — fields 13 and 16 have distinct semantics per
        // the proto comment and are processed independently.
        //
        // task #168 item K — USER DIRECTIVE: ALSO eagerly register the
        // dispatcher-pushed digests in the server-side `locality_map`
        // keyed by the worker's `cas_endpoint`. Without this, the
        // server's knowledge that worker W now holds digest D would
        // lag the next periodic field-13 (`digests`) tick — meaning an
        // action referencing D scheduled in the meantime would (a)
        // trigger a redundant peer-fetch from another worker (wasted
        // bandwidth), (b) potentially be re-dispatched by the
        // dispatcher because the server doesn't know W has it, OR (c)
        // be scheduled away from W, missing the locality-affinity
        // optimization.
        //
        // Coupling this to the SAME tick that carries the ack closes
        // the action-arrival window: the worker triggers this tick
        // eagerly via `mirror_changes_notify.notify_one()` in
        // `FastSlowStore::insert_mirror_blob` (called from
        // `handle_batch_write_small_blobs` after each successful
        // batch), so latency from dispatch to locality-update is
        // bounded by ~RTT (sub-second) — well inside any reasonable
        // action-arrival window.
        //
        // We register BEFORE acking the dispatcher (broadcast_pinned_mirror_ack
        // releases the pin) so any concurrent reader sees locality
        // before the pin is released. Skipped when no `locality_map`
        // is configured (test contexts without WorkerProxyStore).
        //
        // #168 follow-up (A2): folded into the consolidated
        // `locality_map.write()` block below — saves one write-lock
        // per BlobsAvailable tick carrying both `pinned_mirror_entries`
        // and `digests` / `pinned_mirror_digests`. We extract field-16
        // digests up-front (the only locality-relevant payload of the
        // entries) so the consolidated `register_blobs_iter` call can
        // chain them; the `broadcast_pinned_mirror_ack` call is
        // deferred to AFTER the consolidated write's `drop(map)` so
        // the "register BEFORE ack" invariant is preserved.
        // `pinned_mirror_ack_entries` is `Some` iff this tick carries
        // a non-empty field-16 payload; the ack fires at EVERY
        // exit path below (the two early returns at `no_locality_map`
        // / `empty_endpoint`, the consolidated-block spawn return,
        // and the fall-through) so semantics for the
        // ack-when-locality-skipped paths are preserved.
        let pinned_mirror_field16_digests: Vec<DigestInfo> = if notification
            .pinned_mirror_entries
            .is_empty()
        {
            Vec::new()
        } else {
            notification
                .pinned_mirror_entries
                .iter()
                .filter_map(|e| {
                    e.digest
                        .as_ref()
                        .and_then(|d| DigestInfo::try_from(d.clone()).ok())
                })
                .collect()
        };
        let pinned_mirror_ack_entries: Option<&[_]> = if notification
            .pinned_mirror_entries
            .is_empty()
        {
            None
        } else {
            Some(notification.pinned_mirror_entries.as_slice())
        };

        // AC pin advertisement (proto field 17, Option A) — kept in a
        // SEPARATE branch from the CAS field above so the AC entries
        // CANNOT be routed into the CAS-shared `BlobLocalityMap` even
        // by accident. The dedicated `AcPinRegistry` is fed instead;
        // it has no CAS-side reader, so there is no path by which AC
        // pins could weaponize the upload short-circuits in
        // `bytestream_server::write` / `cas_server::batch_update_blobs`.
        //
        // **Replace-snapshot semantics.** Field 17 carries the worker's
        // FULL CURRENT AC pin snapshot every `BlobsAvailable` tick.
        // The server's per-endpoint set is REPLACED (not additively
        // merged) with that snapshot via `replace_endpoint_ac_pins`,
        // so stale entries from prior ticks are dropped on every
        // tick by construction — no explicit drain channel is needed
        // for the steady-state registration path. (Endpoint-lifecycle
        // drains — `wipe_endpoint` on disconnect / boot-epoch flip
        // and `remove_digests_for_endpoint_in_store` from the
        // BIS-ack sweep — are still wired separately.)
        //
        // Field 17 is processed UNCONDITIONALLY (including when
        // empty): an empty advertisement means "the worker has no
        // AC pins this tick" and must clear the per-endpoint row.
        if let Some(ref ac_pin_registry) = self.ac_pin_registry {
            let endpoint = if notification.worker_cas_endpoint.is_empty() {
                self.cas_endpoint.as_str()
            } else {
                notification.worker_cas_endpoint.as_str()
            };
            if !endpoint.is_empty() {
                let count = notification.pinned_ac_mirror_entries.len();
                // Hard-cap the intermediate `entries` Vec at the
                // per-endpoint cap. Pre-allocating with `with_capacity`
                // is just a hint — without an explicit length check
                // inside the push loop, a hostile worker advertising
                // 100M entries grows `entries` to 100M before
                // `replace_endpoint_ac_pins` can truncate, defeating
                // the OOM bound the doc-comment claimed. Audit
                // 2026-05-07 retro-cadre fixup follow-up.
                let cap = nativelink_util::ac_pin_registry::DEFAULT_MAX_AC_PINS_PER_ENDPOINT;
                let cap_hint = count.min(cap);
                let mut entries: Vec<(std::sync::Arc<str>, DigestInfo)> =
                    Vec::with_capacity(cap_hint);
                let mut over_cap_dropped: usize = 0;
                for entry in &notification.pinned_ac_mirror_entries {
                    if entries.len() >= cap {
                        over_cap_dropped += 1;
                        continue;
                    }
                    let Some(proto_digest) = entry.digest.as_ref() else {
                        continue;
                    };
                    let Ok(digest) = DigestInfo::try_from(proto_digest.clone()) else {
                        continue;
                    };
                    if entry.store_id.is_empty() {
                        continue;
                    }
                    entries.push((
                        std::sync::Arc::from(entry.store_id.as_str()),
                        digest,
                    ));
                }
                if over_cap_dropped > 0 {
                    tracing::warn!(
                        endpoint,
                        cap,
                        received = count,
                        over_cap_dropped,
                        "BlobsAvailable: pinned_ac_mirror_entries exceeds per-endpoint cap; \
                         truncating at caller (defense-in-depth — registry replace also caps)"
                    );
                }
                let registered = entries.len();
                // Site C — no AcPinResync push emitted here intentionally.
                // Cap-truncation via replace is convergence-inert: a
                // force-re-advertise would clear the worker's memo → the
                // worker re-sends the same over-cap set → the server
                // truncates again → same tail lost every cycle → cannot
                // converge. The removed periodic heartbeat never converged
                // it either. Correct steady-state: the over-cap tail is
                // simply not registered on the server; the worker re-emits
                // it on every BlobsAvailable tick and it is always
                // truncated. See `.claude/reviews/58940c18-v3-acpinresync/`.
                ac_pin_registry.replace_endpoint_ac_pins(endpoint, &entries);
                debug!(
                    worker_id=?self.worker_id,
                    endpoint,
                    received=count,
                    registered,
                    "BlobsAvailable: replaced pinned_ac_mirror_entries snapshot in AcPinRegistry"
                );
            }
        }

        let cpu_load_pct = notification.cpu_load_pct;
        let p_core_load_pct = notification.p_core_load_pct;
        let e_core_load_pct = notification.e_core_load_pct;
        // (#sched-zeroload) UNCONDITIONAL update (previously gated on `> 0`) —
        // see `inner_keep_alive`: a genuine all-zero reading must be recorded so
        // a truly-idle worker is distinguished from a never-reported one.
        debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "BlobsAvailable received with CPU load");
        if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
            warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
        }

        // (FL-681 re-saturation gate) Plumb the worker's indefinite-pin-cap
        // saturation into the scheduler so the matcher skips a saturated worker.
        // UNCONDITIONAL (unlike CPU load, which skips the `0 == unknown`
        // sentinel): `false` is the load-bearing "drained, re-selectable" signal
        // — gating it would leave a once-saturated worker permanently excluded.
        // Idempotent on the scheduler side (a no-op when the value is unchanged).
        let indefinite_pin_saturated = notification.indefinite_pin_saturated;
        if let Err(err) = self
            .scheduler
            .update_worker_indefinite_pin_saturation(&self.worker_id, indefinite_pin_saturated)
            .await
        {
            warn!(worker_id=?self.worker_id, ?err, indefinite_pin_saturated, "Failed to update worker indefinite-pin saturation");
        }

        // (#37 rev-4 memory-pressure gate) Plumb the worker's coarse
        // memory-pressure verdict + level to the matcher (proactive skip /
        // fleet fail-open ranking). Carried on BOTH the periodic heartbeat
        // and the one-shot post-action delta (like indefinite_pin_saturated)
        // so a pressured worker's flag is not clobbered the instant an action
        // completes. Unconditional plumb of the real boolean (never inferred
        // from a missing value) — a fully-dark worker is handled by the
        // keepalive/quarantine path, not the memory path (§3a rule 3).
        let memory_pressured = notification.memory_pressured;
        let memory_pressure_level = notification.memory_pressure_level;
        if let Err(err) = self
            .scheduler
            .update_worker_swap_pressure(&self.worker_id, memory_pressured, memory_pressure_level)
            .await
        {
            warn!(worker_id=?self.worker_id, ?err, memory_pressured, memory_pressure_level, "Failed to update worker memory pressure");
        }

        // (F4) Plumb the worker's coarse disk-pressure verdict + free-bytes
        // level to the matcher (proactive skip / least-pressured fail-open
        // ranking), mirroring the memory-pressure plumb above. Carried on BOTH
        // the periodic heartbeat and the one-shot post-action delta so a
        // disk-pressured worker's flag is not clobbered when an action
        // completes. ADVISORY ONLY: the authoritative gate is the worker-local
        // StartAction NAK (+ statvfs fallback); this is the proactive matcher
        // hint so a pressured worker is not selected then forced to NAK.
        let disk_pressured = notification.disk_pressured;
        let available_disk_bytes = notification.available_disk_bytes;
        if let Err(err) = self
            .scheduler
            .update_worker_disk_pressure(&self.worker_id, disk_pressured, available_disk_bytes)
            .await
        {
            warn!(worker_id=?self.worker_id, ?err, disk_pressured, available_disk_bytes, "Failed to update worker disk pressure");
        }

        // Mirror capacity report (review #1): the worker advertises its
        // current `mirror_blobs` total bytes and configured cap on every
        // BlobsAvailable. Plumb them into the WorkerProxyStore picker
        // so subsequent mirror writes can pre-check capacity per peer
        // and avoid consuming a source stream we know cannot be
        // accepted. mirror_max_bytes == 0 means the worker has no CAS
        // server / mirror store; skip the report rather than store
        // (used=0, max=0) which would make `fits()` always succeed.
        if notification.mirror_max_bytes > 0 {
            if let Some(ref proxy) = self.worker_proxy {
                proxy.record_mirror_capacity(
                    &notification.worker_cas_endpoint,
                    notification.mirror_used_bytes,
                    notification.mirror_max_bytes,
                );
            }
        }

        // Update the worker's cached directory digests if any were reported (legacy path).
        if !notification.cached_directory_digests.is_empty() && !notification.is_full_subtree_snapshot {
            let cached_dirs: HashSet<DigestInfo> = notification
                .cached_directory_digests
                .iter()
                .filter_map(|d| DigestInfo::try_from(d.clone()).ok())
                .collect();
            let count = cached_dirs.len();
            debug!(worker_id=?self.worker_id, count, "BlobsAvailable received with cached directory digests");
            if let Err(err) = self.scheduler.update_cached_directories(&self.worker_id, cached_dirs).await {
                warn!(worker_id=?self.worker_id, ?err, count, "Failed to update cached directory digests");
            }
        }

        // Handle delta-encoded subtree digest updates.
        let has_subtree_update = notification.is_full_subtree_snapshot
            || !notification.added_subtree_digests.is_empty()
            || !notification.removed_subtree_digests.is_empty();
        if has_subtree_update {
            let is_full = notification.is_full_subtree_snapshot;
            let full_set: Vec<DigestInfo> = if is_full {
                notification
                    .cached_directory_digests
                    .iter()
                    .filter_map(|d| DigestInfo::try_from(d.clone()).ok())
                    .collect()
            } else {
                Vec::new()
            };
            let added: Vec<DigestInfo> = notification
                .added_subtree_digests
                .iter()
                .filter_map(|d| DigestInfo::try_from(d.clone()).ok())
                .collect();
            let removed: Vec<DigestInfo> = notification
                .removed_subtree_digests
                .iter()
                .filter_map(|d| DigestInfo::try_from(d.clone()).ok())
                .collect();
            let full_count = full_set.len();
            let added_count = added.len();
            let removed_count = removed.len();
            debug!(
                worker_id=?self.worker_id,
                is_full,
                full_count,
                added_count,
                removed_count,
                "BlobsAvailable received with subtree digest updates"
            );
            if let Err(err) = self
                .scheduler
                .update_cached_subtrees(
                    &self.worker_id,
                    is_full,
                    full_set,
                    added,
                    removed,
                )
                .await
            {
                warn!(
                    worker_id=?self.worker_id,
                    ?err,
                    is_full,
                    full_count,
                    added_count,
                    removed_count,
                    "Failed to update cached subtree digests"
                );
            }
        }

        let Some(ref locality_map) = self.locality_map else {
            // A2 fold: field-16 ack still fires when locality_map is
            // unset (pre-fold the standalone block did the ack before
            // these early-return checks; preserve that semantic).
            if let Some(entries) = pinned_mirror_ack_entries {
                if let Some(ref dispatcher) = self.small_blob_dispatcher {
                    dispatcher.broadcast_pinned_mirror_ack(entries);
                }
            }
            // (FL-688 v3 Stage C — DOC-FIX-1) Send gate-release even when
            // no locality map is configured — a worker with only pinned-mirror
            // blobs and no CAS endpoint still needs its executor unblocked.
            self.try_send_reconcile_complete(notification.is_full_snapshot);
            let handle_blobs_available_elapsed_ms =
                handle_blobs_available_start.elapsed().as_millis() as u64;
            debug!(
                handle_blobs_available_elapsed_ms,
                exit_path = "no_locality_map",
                "handle_blobs_available complete"
            );
            return Ok(());
        };
        let endpoint = if notification.worker_cas_endpoint.is_empty() {
            &self.cas_endpoint
        } else {
            &notification.worker_cas_endpoint
        };
        if endpoint.is_empty() {
            // A2 fold: same semantics as the no_locality_map exit above —
            // ack the dispatcher even though we cannot register locality.
            if let Some(entries) = pinned_mirror_ack_entries {
                if let Some(ref dispatcher) = self.small_blob_dispatcher {
                    dispatcher.broadcast_pinned_mirror_ack(entries);
                }
            }
            // (FL-688 v3 Stage C — DOC-FIX-1) Same as no_locality_map: send
            // gate-release unconditionally on first full snapshot.
            self.try_send_reconcile_complete(notification.is_full_snapshot);
            let handle_blobs_available_elapsed_ms =
                handle_blobs_available_start.elapsed().as_millis() as u64;
            debug!(
                handle_blobs_available_elapsed_ms,
                exit_path = "empty_endpoint",
                "handle_blobs_available complete"
            );
            return Ok(());
        }

        let is_full_snapshot = notification.is_full_snapshot;

        // Process evicted digests (incremental updates report evictions here).
        let evicted: Vec<DigestInfo> = notification
            .evicted_digests
            .into_iter()
            .filter_map(|d| d.try_into().ok())
            .collect();

        // Collect digests from digest_infos (preferred) and legacy digests.
        // The proto used to carry per-blob last_access_timestamp; that field
        // is now reserved (see worker_api.proto) — locality entries persist
        // until an explicit eviction signal, so timestamps are no longer
        // needed for filtering.
        let mut digests: Vec<DigestInfo> = notification
            .digest_infos
            .into_iter()
            .filter_map(|info| info.digest.and_then(|d| DigestInfo::try_from(d).ok()))
            .collect();
        digests.extend(
            notification
                .digests
                .into_iter()
                .filter_map(|d| DigestInfo::try_from(d).ok()),
        );

        // Pinned mirror digests: blobs the worker is holding *only* in
        // memory because the server pushed them as a mirror. The worker is
        // the durable holder until we ack via BlobsInStableStorage. We:
        //   1. Register them in the locality map alongside normal digests so
        //      reads from this worker can find them, AND
        //   2. Always check existence and request `UploadMissingBlobs` for
        //      any that aren't stably stored on the server. We deliberately
        //      bypass the per-worker BACKFILL_COOLDOWN here — these are the
        //      *only* copies; latency to durability matters more than the
        //      tiny extra existence check load.
        let pinned_mirror: Vec<DigestInfo> = notification
            .pinned_mirror_digests
            .into_iter()
            .filter_map(|d| DigestInfo::try_from(d).ok())
            .collect();
        if !pinned_mirror.is_empty() {
            debug!(
                worker_id=?self.worker_id,
                count=pinned_mirror.len(),
                "BlobsAvailable received pinned mirror digests"
            );
        }
        // Pinned-mirror digests are registered in the locality map in a
        // dedicated `register_blobs` call below (alongside `digests`) — we do
        // NOT extend `digests` here. Doing so would cause both the generic
        // backfill path (which respects BACKFILL_COOLDOWN) and the dedicated
        // mirror-pull path (which bypasses it) to schedule the same uploads;
        // `backfill_inflight` deduplicates them, but the duplicate work is
        // wasteful and the duplication obscures intent.

        // Acquire the write lock once for all mutations to avoid repeated
        // lock acquisition and eliminate inconsistency windows.
        //
        // Order matters: evictions BEFORE registrations. This ensures stale
        // entries are cleaned up before new ones are added, preventing a
        // window where a digest appears available on a worker that just
        // evicted it.
        let mut map = locality_map.write();

        if is_full_snapshot {
            // Remove all existing entries for this endpoint first.
            map.remove_endpoint(endpoint);
        }

        if !evicted.is_empty() {
            debug!(
                worker_id=?self.worker_id,
                endpoint,
                count=evicted.len(),
                "Processing evicted digests from BlobsAvailable"
            );
            map.evict_blobs(endpoint, &evicted);
        }

        // Collapse generic + pinned-mirror + field-16-pinned-mirror-entries
        // registrations into a single `register_blobs_iter` call so we
        // allocate the endpoint `Arc<str>` once per tick instead of three
        // times (10 workers × 100ms = ~300 alloc/sec saved). The iterator
        // form chains all slices without building an intermediate `Vec`.
        // Pinned-mirror digests still take a SEPARATE mirror-pull code
        // path below — combining the locality registration does not
        // merge their backfill scheduling. Field-16 entries are also
        // ack'd via `broadcast_pinned_mirror_ack` AFTER `drop(map)` to
        // preserve the "register BEFORE ack" invariant (#168 A2 fold).
        if !digests.is_empty() || !pinned_mirror.is_empty() || !pinned_mirror_field16_digests.is_empty() {
            debug!(
                worker_id=?self.worker_id,
                endpoint,
                count=digests.len(),
                pinned_mirror_count=pinned_mirror.len(),
                pinned_mirror_field16_count=pinned_mirror_field16_digests.len(),
                is_full_snapshot,
                "Registering blobs available from worker"
            );
            map.register_blobs_iter(
                endpoint,
                digests
                    .iter()
                    .copied()
                    .chain(pinned_mirror.iter().copied())
                    .chain(pinned_mirror_field16_digests.iter().copied()),
            );
        }

        // Mirror-pull pipeline: any digest the worker is holding pinned in
        // memory MUST be pulled into the server's stable storage promptly,
        // since the worker is the only durable holder. We bypass the
        // BACKFILL_COOLDOWN throttle here — the cost of one extra existence
        // check per worker per tick is negligible compared to the durability
        // window we close. Once the upload lands in the server's slow store,
        // the FastSlowStore push to `stable_digests` triggers the broadcast
        // loop in `nativelink.rs` which sends `BlobsInStableStorage` back to
        // the worker, dropping the pin.
        if !pinned_mirror.is_empty() {
            if let Some(ref cas_store) = self.cas_store {
                let pinned = pinned_mirror.clone();
                let cas = cas_store.clone();
                let tx = self.worker_tx.clone();
                let worker_id = self.worker_id.clone();
                let inflight = self.backfill_inflight.clone();
                let metrics = self.metrics.clone();
                // (#sigkill-gap) Handler-invoked pinned-mirror-pull feed: pass
                // the worker-intake latch so SIGTERM Phase 0b suppresses it
                // (the convergent BLOCK pair-a/red-team flagged this cooldown-
                // bypassing feed as a quiesce escape if left ungated).
                let quiesce = self.shutdown_quiesce.clone();
                background_spawn!("pull_pinned_mirror_blobs", async move {
                    Self::request_missing_blob_uploads(
                        &cas,
                        &tx,
                        &worker_id,
                        &pinned,
                        &inflight,
                        // Pinned-mirror digests bypass the cooldown — these
                        // are the only durable copies; latency to durability
                        // matters more than the throttle.
                        true,
                        &metrics,
                        Some(&quiesce),
                    )
                    .await;
                });
            }
        }

        // After updating the locality map, do TWO things on every tick:
        //
        //   (1) ALWAYS run has_with_results + mark_stable for the digests
        //       the worker reports it holds. This is the BIS-pipeline
        //       feed — every 100 ms we ack the worker's pinned digests
        //       that the server already has stably, so the worker can
        //       unpin promptly. Per red-team F2 + reviewer feedback on
        //       task #140: previously this rode on the cooldown-gated
        //       backfill path, so worst-case mark_stable latency was
        //       0–5 s + BIS lag. Decoupled here so worst-case latency
        //       is ~one BlobsAvailable tick (~100 ms backstop).
        //
        //   (2) COOLDOWN-GATED, send UploadMissingBlobs requests for
        //       digests the worker has but the server doesn't. The
        //       cooldown is what actually exists to throttle the
        //       (rare) upload protocol — has_with_results itself is
        //       cheap (mostly served by ExistenceCacheStore).
        //
        // Both branches share the same `has_with_results` call inside
        // `request_missing_blob_uploads`; the `send_uploads_if_missing`
        // flag controls whether step (2) actually executes.
        if !digests.is_empty() {
            if let Some(ref cas_store) = self.cas_store {
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let last = self.last_backfill_epoch_secs.load(Ordering::Relaxed);
                let cooldown_passed = now_secs.saturating_sub(last) >= BACKFILL_COOLDOWN_SECS
                    && self
                        .last_backfill_epoch_secs
                        .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok();

                let all_digests: Vec<DigestInfo> = digests.clone();
                let cas = cas_store.clone();
                let tx = self.worker_tx.clone();
                let worker_id = self.worker_id.clone();
                let inflight = self.backfill_inflight.clone();
                let metrics = self.metrics.clone();
                // (#sigkill-gap) Handler-invoked backfill + mark_stable feed:
                // pass the worker-intake latch so SIGTERM Phase 0b suppresses
                // it (this is the `total=11887 missing=11887` storm observed
                // mid-shutdown).
                let quiesce = self.shutdown_quiesce.clone();
                // (BLOCK-1 fix) Clone the pieces needed for try_send_reconcile_complete
                // so the send can be the LAST statement of the spawned task — after the
                // UploadMissingBlobs chunk loop. Before this fix, the send was on the
                // synchronous handler path (fire-and-forget spawn + immediate send on the
                // handler), which is NOT "after uploads": the spawned task may not have
                // run yet when the handler-side send fires. The worker channel is
                // unbounded, but the two sends are on DIFFERENT tasks — the handler's
                // channel-send can be ordered before OR after the spawned task's sends.
                // Fix: move it INSIDE the spawn so program order within a single task
                // guarantees UploadMissingBlobs ≺ ReconcileComplete.
                let reconcile_sent_flag = Arc::clone(&self.reconcile_complete_sent);
                let reconcile_worker_tx = self.worker_tx.clone();
                let reconcile_worker_id = self.worker_id.clone();
                // Drop the locality map write lock before spawning.
                drop(map);
                // A2 fold: ack field-16 entries strictly AFTER the
                // consolidated `locality_map.write()` guard drops, so
                // any reader that takes the read lock observing the
                // pin-release also observes the just-registered
                // locality entries (register BEFORE ack).
                if let Some(entries) = pinned_mirror_ack_entries {
                    if let Some(ref dispatcher) = self.small_blob_dispatcher {
                        dispatcher.broadcast_pinned_mirror_ack(entries);
                    }
                }
                background_spawn!(
                    "blobs_available_mark_stable_and_backfill",
                    async move {
                        Self::request_missing_blob_uploads(
                            &cas,
                            &tx,
                            &worker_id,
                            &all_digests,
                            &inflight,
                            cooldown_passed,
                            &metrics,
                            Some(&quiesce),
                        )
                        .await;
                        // (BLOCK-1 fix) ReconcileComplete is sent HERE, as the LAST
                        // statement of this task, AFTER all UploadMissingBlobs sends.
                        // Program order within a single task guarantees the worker
                        // channel receives uploads before the gate-release.
                        // The 3 non-backfill paths (no_locality_map, empty_endpoint,
                        // fall-through) keep their synchronous sends — those paths
                        // send ZERO UploadMissingBlobs so order is irrelevant.
                        Self::send_reconcile_complete_static(
                            is_full_snapshot,
                            &reconcile_sent_flag,
                            &reconcile_worker_tx,
                            &reconcile_worker_id,
                        );
                    }
                );
                let handle_blobs_available_elapsed_ms =
                    handle_blobs_available_start.elapsed().as_millis() as u64;
                debug!(
                    handle_blobs_available_elapsed_ms,
                    exit_path = "background_backfill_spawned",
                    "handle_blobs_available complete"
                );
                return Ok(());
            }
        }

        // A2 fold: fall-through path — drop the consolidated write guard
        // by name BEFORE acking so the "register BEFORE ack" invariant
        // holds. `map` was bound at the top of the consolidated block;
        // if control reached here without taking the
        // `background_backfill_spawned` early return above, the guard
        // is still held. Explicit `drop(map)` documents the ordering.
        drop(map);
        if let Some(entries) = pinned_mirror_ack_entries {
            if let Some(ref dispatcher) = self.small_blob_dispatcher {
                dispatcher.broadcast_pinned_mirror_ack(entries);
            }
        }
        // (FL-688 v3 Stage C — DOC-FIX-1) Fall-through: no digests or no
        // cas_store, so no UploadMissingBlobs were sent. Still send the gate-
        // release — this is the exact case the DOC-FIX-1 fix targets: a worker
        // with all blobs already on server (or no blobs at all) must still get
        // its executor unblocked.
        self.try_send_reconcile_complete(is_full_snapshot);
        let handle_blobs_available_elapsed_ms =
            handle_blobs_available_start.elapsed().as_millis() as u64;
        debug!(
            handle_blobs_available_elapsed_ms,
            exit_path = "fall_through",
            "handle_blobs_available complete"
        );
        Ok(())
    }

    /// Check which of `digests` are missing from the server CAS and send
    /// UploadMissingBlobs requests to the worker for each batch. ALSO
    /// `mark_stable` the present subset so the BIS broadcast loop tells
    /// the worker it is safe to unpin those digests (see audit at
    /// `.claude/audits/task-139-lost-eviction/audit.md` Path 2 +
    /// `.claude/reviews/bis-coverage-for-already-cached-outputs/audit.md`):
    /// every pin path the worker reports through BlobsAvailable is
    /// covered here, replacing the deleted `register_action_result_digests`.
    ///
    /// `send_uploads_if_missing` controls only the second half — the
    /// `UploadMissingBlobs` send. The mark_stable half ALWAYS runs (per
    /// red-team F2: pin-release latency must be ~one BlobsAvailable tick
    /// (~100 ms) for v2 durable pins, not 0–5 s + BIS lag). Setting the
    /// flag to `false` corresponds to "we are inside the per-worker
    /// `BACKFILL_COOLDOWN_SECS` window for the upload-protocol throttle"
    /// — mark_stable still runs.
    ///
    /// Deduplicates against in-flight requests: digests that were requested
    /// within the last `BACKFILL_INFLIGHT_TIMEOUT_SECS` are skipped to avoid
    /// redundant uploads. Digests that have since appeared in the CAS (or
    /// whose requests have timed out) are removed from the in-flight set.
    /// `quiesce` is the worker-intake shutdown latch. The HANDLER-invoked feeds
    /// (the `handle_blobs_available` backfill spawn + the pinned-mirror-pull
    /// spawn) pass `Some(&self.shutdown_quiesce)`; the server-INITIATED
    /// `ShutdownPuller::run` passes `None`. When the latch is set this fn
    /// early-returns at the very entry — soliciting NOTHING and running NO CAS
    /// existence work (`has_with_results` / `has_durably`) — so the shutdown
    /// drain converges instead of being storm-fed AND the 111% mid-shutdown CPU
    /// (the per-tick existence scans) stops. The mark_stable / BIS-unpin oath is
    /// also suppressed for shutdown: the server is exiting, so post-restart BIS
    /// re-acks (same as the evict/GoingAway path today); workers stay pinned
    /// (failed_slow_writes + FS pin) until the restarted server re-acks.
    async fn request_missing_blob_uploads(
        cas_store: &Store,
        worker_tx: &mpsc::UnboundedSender<UpdateForWorker>,
        worker_id: &WorkerId,
        digests: &[DigestInfo],
        inflight: &parking_lot::Mutex<HashMap<DigestInfo, Instant>>,
        send_uploads_if_missing: bool,
        metrics: &WorkerApiMetrics,
        quiesce: Option<&ShutdownQuiesce>,
    ) {
        // Phase 0b worker-intake quiesce (handler-invoked feeds only). The
        // shutdown PULL passes `quiesce = None` and is therefore never
        // suppressed — it IS the drain, not the storm. See the type-level
        // `ShutdownQuiesce` doc + the SIGTERM Phase 0b in `src/bin/nativelink.rs`.
        if quiesce.is_some_and(ShutdownQuiesce::is_quiesced) {
            // Count the suppressed solicitation so an operator can confirm the
            // latch is doing its job during shutdown (a rising value = the
            // storm is being suppressed) and so a quiesce ESCAPE (a feed that
            // bypasses this gate) is detectable. Observability only.
            metrics
                .shutdown_suppressed_backfill_solicitations_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        if digests.is_empty() {
            return;
        }

        // Check existence on the server CAS.
        let keys: Vec<StoreKey<'_>> = digests
            .iter()
            .map(|d| StoreKey::from(*d))
            .collect();
        let mut results = vec![None; keys.len()];
        if let Err(err) = cas_store.has_with_results(&keys, &mut results).await {
            // Per red-team F5 + CLAUDE.md "Belt-and-suspenders masks bugs":
            // increment a NOISY metric counter alongside the error log so
            // operators can alert on sustained failures without having to
            // grep logs. The error log alone makes this a quiet self-heal
            // (the comment about "next tick recovers" is only true if the
            // failure is transient — sustained failures are exactly the
            // case the metric exists to catch).
            metrics
                .mark_stable_has_with_results_failures
                .fetch_add(1, Ordering::Relaxed);
            error!(
                worker_id=?worker_id,
                ?err,
                count=digests.len(),
                "backfill+mark_stable: failed to check CAS existence; \
                 worker pins for present digests will not be acked this round \
                 (next BlobsAvailable tick recovers) and missing digests will \
                 not be requested for upload (next backfill tick recovers); \
                 metric `mark_stable_has_with_results_failures` incremented"
            );
            return;
        }

        let now = Instant::now();
        let timeout = Duration::from_secs(BACKFILL_INFLIGHT_TIMEOUT_SECS);

        // Build a set of digests confirmed present in the CAS for O(1) lookup
        // during the retain loop (avoids O(inflight * digests) linear scan).
        let present_in_cas: HashSet<DigestInfo> = digests
            .iter()
            .zip(results.iter())
            .filter_map(|(d, r)| r.map(|_| *d))
            .collect();

        // mark_stable for the DURABLE subset (durability-ack v3 §3.0
        // keystone): the worker's pin on a digest is the ≥2-replica
        // backstop while the server's copy is only in volatile RAM (the
        // fast tier / in-flight slow-write maps / RAM-only mirror). The BIS
        // broadcast loop drains `stable_digests`, emits `BlobsInStableStorage`
        // to the worker, which calls `unpin_digest` (`local_worker.rs:717`) —
        // dropping the worker's copy. That unpin OATH may fire ONLY once the
        // server holds a DURABLE copy (survives a restart), never on RAM-only
        // presence: unpinning a RAM-only digest would discard the only
        // durable copy of a `mirror_blob` on the next server restart.
        //
        // So the gate is `has_durably` (the SLOW-tier-only query), NOT the
        // `has_with_results` above. `has_with_results` (which the FSS
        // satisfies from the fast tier + in-flight maps + mirror) still
        // drives the upload-request decision below — a blob present in RAM
        // need not be re-requested for upload — but it must NOT gate the
        // unpin oath. This is strictly-safer than the prior code: BIS now
        // fires LATER (once durable), never earlier. Idempotent: the
        // broadcast loop dedups downstream and `unpin_digest` is itself
        // idempotent.
        let durable_keys: Vec<StoreKey<'_>> =
            digests.iter().map(|d| StoreKey::from(*d)).collect();
        let mut durable_results = vec![None; durable_keys.len()];
        match cas_store
            .has_durably(&durable_keys, &mut durable_results)
            .await
        {
            Ok(()) => {
                let durable_vec: Vec<DigestInfo> = digests
                    .iter()
                    .zip(durable_results.iter())
                    .filter_map(|(d, r)| r.map(|_| *d))
                    .collect();
                if !durable_vec.is_empty() {
                    cas_store.mark_stable(&durable_vec);
                }
            }
            Err(err) => {
                // A failed durable check must NOT fire the unpin oath (a
                // false-durable would lose data). Skip mark_stable this
                // round; the next BlobsAvailable tick recovers. Surface a
                // noisy counter alongside the log. pair-a F2: use the
                // DISTINCT `mark_stable_has_durably_failures` counter — NOT
                // `mark_stable_has_with_results_failures` above — so an
                // operator alerting on the metric stream can tell the
                // durable-gate query apart from the upload-request existence
                // query (the two have different recovery implications).
                metrics
                    .mark_stable_has_durably_failures
                    .fetch_add(1, Ordering::Relaxed);
                error!(
                    worker_id=?worker_id,
                    ?err,
                    count=digests.len(),
                    "mark_stable: has_durably check failed; worker pins for \
                     durable digests will not be acked this round (next \
                     BlobsAvailable tick recovers); metric \
                     `mark_stable_has_durably_failures` incremented"
                );
            }
        }

        // The mark_stable side runs every BlobsAvailable tick (~100 ms);
        // the upload side is cooldown-gated. When inside the cooldown
        // window we still ran has_with_results above (cheap, mostly served
        // by ExistenceCacheStore) and pushed mark_stable for the present
        // subset — but we skip the missing-digest enumeration and the
        // UploadMissingBlobs send. The next BlobsAvailable tick that
        // passes the cooldown will pick up any digests still missing.
        if !send_uploads_if_missing {
            return;
        }

        // Collect missing digests, filtering out those already in-flight.
        let missing: Vec<DigestInfo> = {
            let mut inflight_guard = inflight.lock();

            // Clean up: remove digests that have appeared in the CAS or
            // whose requests have timed out.
            inflight_guard.retain(|digest, requested_at| {
                // Remove if timed out.
                if now.duration_since(*requested_at) >= timeout {
                    return false;
                }
                // Remove if the digest is now present in the CAS.
                if present_in_cas.contains(digest) {
                    return false;
                }
                // Keep if still missing and not timed out.
                true
            });

            digests
                .iter()
                .zip(results.iter())
                .filter_map(|(d, r)| {
                    // Only consider digests missing from the CAS.
                    if r.is_some() {
                        return None;
                    }
                    // Skip if already in-flight.
                    if inflight_guard.contains_key(d) {
                        return None;
                    }
                    Some(*d)
                })
                .collect()
        };

        if missing.is_empty() {
            return;
        }

        info!(
            worker_id=?worker_id,
            total=digests.len(),
            missing=missing.len(),
            "backfill: requesting worker upload missing blobs"
        );

        // Record in-flight digests and send in batches.
        {
            let mut inflight_guard = inflight.lock();
            for d in &missing {
                inflight_guard.insert(*d, now);
            }
        }

        for chunk in missing.chunks(BACKFILL_BATCH_SIZE) {
            let proto_digests: Vec<Digest> = chunk
                .iter()
                .map(|d| Digest::from(*d))
                .collect();
            let msg = UpdateForWorker {
                update: Some(update_for_worker::Update::UploadMissingBlobs(
                    UploadMissingBlobsRequest {
                        digests: proto_digests,
                    },
                )),
            };
            if worker_tx.send(msg).is_err() {
                warn!(
                    worker_id=?worker_id,
                    "backfill: worker channel closed, cannot send upload request"
                );
                return;
            }
        }
    }

    async fn execution_complete(&self, execute_complete: ExecuteComplete) -> Result<(), Error> {
        let cpu_load_pct = execute_complete.cpu_load_pct;
        let p_core_load_pct = execute_complete.p_core_load_pct;
        let e_core_load_pct = execute_complete.e_core_load_pct;
        // (#sched-zeroload) UNCONDITIONAL update (previously gated on `> 0`) —
        // see `inner_keep_alive`: a genuine all-zero reading must be recorded so
        // a truly-idle worker is distinguished from a never-reported one.
        debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "ExecuteComplete received with CPU load");
        if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
            warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
        }
        let operation_id = OperationId::from(execute_complete.operation_id);
        info!(
            worker_id=?self.worker_id,
            %operation_id,
            "execution complete, CAS upload finished"
        );
        self.scheduler
            .update_action(
                &self.worker_id,
                &operation_id,
                UpdateOperationType::ExecutionComplete,
            )
            .await
            .err_tip(|| format!("Failed to operation {operation_id}"))?;
        Ok(())
    }
}
