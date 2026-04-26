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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    execute_result, ExecuteComplete, ExecuteResult, GoingAwayRequest, KeepAliveRequest,
    UpdateForScheduler, UpdateForWorker, UploadMissingBlobsRequest,
};
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::common::DigestInfo;
use nativelink_scheduler::worker::Worker;
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

pub type ConnectWorkerStream =
    Pin<Box<dyn Stream<Item = Result<UpdateForWorker, Status>> + Send + Sync + 'static>>;

pub type NowFn = Box<dyn Fn() -> Result<Duration, Error> + Send + Sync>;

#[derive(MetricsComponent)]
pub struct WorkerApiServer {
    scheduler: Arc<dyn WorkerScheduler>,
    now_fn: Arc<NowFn>,
    node_id: [u8; 6],
    locality_map: Option<SharedBlobLocalityMap>,
    /// CAS store for checking blob existence during backfill requests.
    cas_store: Option<Store>,
    /// Optional handle on the `WorkerProxyStore` so we can plumb
    /// per-worker mirror capacity reports (review #1) into the
    /// picker's pre-check filter. None for tests / standalone runs
    /// without peer mirroring.
    worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
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
    /// Counters for the BlobsAvailable mark_stable / backfill pipeline.
    /// Shared across every `WorkerConnection` and its background tasks
    /// so a single counter aggregates server-wide. Wired into the
    /// metrics tree under `worker_api` so operators can alert on the
    /// `mark_stable_has_with_results_failures` counter.
    #[metric(group = "worker_api")]
    metrics: Arc<WorkerApiMetrics>,
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
}

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
        Ok(Self {
            scheduler,
            now_fn: Arc::new(now_fn),
            node_id,
            locality_map,
            cas_store,
            worker_proxy,
            endpoint_state: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            metrics: Arc::new(WorkerApiMetrics::default()),
        })
    }

    /// Returns a clone of the metrics handle so callers (e.g. tests,
    /// metrics scrape integration) can observe the BlobsAvailable
    /// mark_stable / backfill counters directly.
    pub fn metrics(&self) -> Arc<WorkerApiMetrics> {
        self.metrics.clone()
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
        if !worker_cas_endpoint.is_empty() {
            let mut state = self.endpoint_state.lock();
            let prev = state.get(&worker_cas_endpoint).cloned();
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
                info!(
                    endpoint = %worker_cas_endpoint,
                    prev_epoch = prev.as_ref().map(|p| p.boot_epoch),
                    new_epoch = new_boot_epoch,
                    "wiped locality_map on worker boot_epoch_id change"
                );
            }
            state.insert(
                worker_cas_endpoint.clone(),
                EndpointState {
                    boot_epoch: new_boot_epoch,
                    owner_worker_id: worker_id.clone(),
                },
            );
        }

        // Now register the worker with the scheduler. This triggers
        // ConnectionResult — and only then will the worker begin
        // sending BlobsAvailable.
        {
            let worker = Worker::new_with_cas_endpoint(
                worker_id.clone(),
                platform_properties,
                tx,
                (self.now_fn)()?.as_secs(),
                connect_worker_request.max_inflight_tasks,
                worker_cas_endpoint.clone(),
            );
            self.scheduler
                .add_worker(worker)
                .await
                .err_tip(|| "Failed to add worker in inner_connect_worker()")?;
        }

        WorkerConnection::start(
            self.scheduler.clone(),
            self.now_fn.clone(),
            worker_id.clone(),
            self.locality_map.clone(),
            self.cas_store.clone(),
            self.worker_proxy.clone(),
            worker_cas_endpoint,
            new_boot_epoch,
            self.endpoint_state.clone(),
            worker_tx,
            self.metrics.clone(),
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

struct WorkerConnection {
    scheduler: Arc<dyn WorkerScheduler>,
    now_fn: Arc<NowFn>,
    worker_id: WorkerId,
    locality_map: Option<SharedBlobLocalityMap>,
    /// CAS store for checking blob existence during backfill.
    cas_store: Option<Store>,
    /// WorkerProxyStore handle for plumbing per-endpoint mirror
    /// capacity reports (review #1).
    worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
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
}

impl WorkerConnection {
    fn start(
        scheduler: Arc<dyn WorkerScheduler>,
        now_fn: Arc<NowFn>,
        worker_id: WorkerId,
        locality_map: Option<SharedBlobLocalityMap>,
        cas_store: Option<Store>,
        worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
        cas_endpoint: String,
        boot_epoch: u64,
        endpoint_state: Arc<parking_lot::Mutex<HashMap<String, EndpointState>>>,
        worker_tx: mpsc::UnboundedSender<UpdateForWorker>,
        metrics: Arc<WorkerApiMetrics>,
        mut connection: impl Stream<Item = Result<UpdateForScheduler, Status>> + Unpin + Send + 'static,
    ) {
        let instance = Self {
            scheduler,
            now_fn,
            worker_id,
            locality_map,
            cas_store,
            worker_proxy,
            cas_endpoint,
            boot_epoch,
            endpoint_state,
            worker_tx,
            last_backfill_epoch_secs: AtomicU64::new(0),
            backfill_inflight: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            metrics,
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
            if !instance.cas_endpoint.is_empty() {
                let mut state = instance.endpoint_state.lock();
                let current_owner = state
                    .get(&instance.cas_endpoint)
                    .map(|s| s.owner_worker_id.clone());
                if current_owner.as_ref() == Some(&instance.worker_id) {
                    if let Some(ref locality_map) = instance.locality_map {
                        locality_map.write().remove_endpoint(&instance.cas_endpoint);
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
        if cpu_load_pct > 0 || p_core_load_pct > 0 || e_core_load_pct > 0 {
            debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "KeepAlive received with CPU load");
            if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
                warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
            }
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

    async fn handle_blobs_available(
        &self,
        notification: nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableNotification,
    ) -> Result<(), Error> {
        let cpu_load_pct = notification.cpu_load_pct;
        let p_core_load_pct = notification.p_core_load_pct;
        let e_core_load_pct = notification.e_core_load_pct;
        if cpu_load_pct > 0 || p_core_load_pct > 0 || e_core_load_pct > 0 {
            debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "BlobsAvailable received with CPU load");
            if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
                warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
            }
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
            return Ok(());
        };
        let endpoint = if notification.worker_cas_endpoint.is_empty() {
            &self.cas_endpoint
        } else {
            &notification.worker_cas_endpoint
        };
        if endpoint.is_empty() {
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

        // Collapse generic + pinned-mirror registrations into a single
        // `register_blobs_iter` call so we allocate the endpoint `Arc<str>`
        // once per tick instead of twice (10 workers × 100ms = ~200
        // alloc/sec saved). The iterator form chains both slices without
        // building an intermediate `Vec`. Pinned-mirror digests still take
        // a SEPARATE mirror-pull code path below — combining the locality
        // registration does not merge their backfill scheduling.
        if !digests.is_empty() || !pinned_mirror.is_empty() {
            debug!(
                worker_id=?self.worker_id,
                endpoint,
                count=digests.len(),
                pinned_mirror_count=pinned_mirror.len(),
                is_full_snapshot,
                "Registering blobs available from worker"
            );
            map.register_blobs_iter(
                endpoint,
                digests.iter().copied().chain(pinned_mirror.iter().copied()),
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
                // Drop the locality map write lock before spawning.
                drop(map);
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
                        )
                        .await;
                    }
                );
                return Ok(());
            }
        }

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
    async fn request_missing_blob_uploads(
        cas_store: &Store,
        worker_tx: &mpsc::UnboundedSender<UpdateForWorker>,
        worker_id: &WorkerId,
        digests: &[DigestInfo],
        inflight: &parking_lot::Mutex<HashMap<DigestInfo, Instant>>,
        send_uploads_if_missing: bool,
        metrics: &WorkerApiMetrics,
    ) {
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

        // mark_stable for the present subset: the server has each of these
        // digests in stable storage, so the worker's pin is no longer
        // load-bearing. The BIS broadcast loop in `nativelink.rs` drains
        // `stable_digests` and emits `BlobsInStableStorage` to every
        // worker, which calls `unpin_digest` (`local_worker.rs:717`).
        // The has_with_results check above guarantees we never tell the
        // worker to unpin a digest the server doesn't actually have
        // (which would lose the only durable copy of a `mirror_blob`).
        // Idempotent: the broadcast loop dedups downstream and
        // `unpin_digest` is itself idempotent.
        if !present_in_cas.is_empty() {
            let present_vec: Vec<DigestInfo> = present_in_cas.iter().copied().collect();
            cas_store.mark_stable(&present_vec);
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
        if cpu_load_pct > 0 || p_core_load_pct > 0 || e_core_load_pct > 0 {
            debug!(worker_id=?self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct, "ExecuteComplete received with CPU load");
            if let Err(err) = self.scheduler.update_worker_load(&self.worker_id, cpu_load_pct, p_core_load_pct, e_core_load_pct).await {
                warn!(worker_id=?self.worker_id, ?err, cpu_load_pct, p_core_load_pct, e_core_load_pct, "Failed to update worker load");
            }
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
