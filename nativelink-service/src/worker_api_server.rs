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
    execute_result, ExecuteComplete, ExecuteResult, GoingAwayRequest, KeepAliveRequest,
    UpdateForScheduler, UpdateForWorker, UploadMissingBlobsRequest,
};
use nativelink_store::small_blob_dispatcher::SmallBlobDispatcher;
use nativelink_util::ac_pin_registry::SharedAcPinRegistry;
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
    ac_pin_registry: Option<SharedAcPinRegistry>,
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
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
        ac_pin_registry: Option<SharedAcPinRegistry>,
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
            cas_store,
            worker_proxy,
            small_blob_dispatcher,
            endpoint_state: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            compatible_build_shas,
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
        let needs_bis_buffer_clear = if !worker_cas_endpoint.is_empty() {
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
            needs_wipe
        } else {
            false
        };

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
            self.cas_store.clone(),
            self.worker_proxy.clone(),
            self.small_blob_dispatcher.clone(),
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

struct WorkerConnection {
    scheduler: Arc<dyn WorkerScheduler>,
    now_fn: Arc<NowFn>,
    worker_id: WorkerId,
    locality_map: Option<SharedBlobLocalityMap>,
    /// AC pin registry (separate from `locality_map`); see
    /// `WorkerApiServer::ac_pin_registry` for design.
    ac_pin_registry: Option<SharedAcPinRegistry>,
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
}

impl WorkerConnection {
    #[allow(clippy::too_many_arguments)]
    fn start(
        scheduler: Arc<dyn WorkerScheduler>,
        now_fn: Arc<NowFn>,
        worker_id: WorkerId,
        locality_map: Option<SharedBlobLocalityMap>,
        ac_pin_registry: Option<SharedAcPinRegistry>,
        cas_store: Option<Store>,
        worker_proxy: Option<Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>>,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
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
            ac_pin_registry,
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
                                if let Some(notification) = instance
                                    .blobs_available_accumulator
                                    .merge_chunk(chunk)
                                {
                                    // Path A commit: accumulator yielded a
                                    // fully-assembled notification on
                                    // is_last=true. Hand to the legacy
                                    // handler, which performs the
                                    // remove_endpoint wipe (when
                                    // is_full_snapshot=true) +
                                    // register_blobs_iter + AC pin
                                    // replace + mirror pipeline
                                    // ATOMICALLY in one block.
                                    instance.handle_blobs_available(notification).await
                                } else {
                                    // Non-terminal chunk: nothing more
                                    // to do until the terminal arrives.
                                    Ok(())
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
        // #168 perf-optimizer MINOR: this block currently takes
        // `locality_map.write()` SEPARATELY from the consolidated
        // block at line ~1252 below — two write-locks per
        // BlobsAvailable notification carrying both
        // `pinned_mirror_entries` and `digests` / `pinned_mirror_digests`.
        // Folding into the consolidated block would save one write-lock
        // per tick (bounded by ~10 workers × ~10 ticks/sec = ~100
        // acquisitions/sec saved), but requires moving the
        // `broadcast_pinned_mirror_ack` call too while preserving the
        // "register BEFORE pin release" ordering invariant. The
        // intervening mirror-pull async work (lines ~1300-1339) makes
        // a clean fold structurally awkward — deferred with this TODO
        // because the perf cost is small and the contract is subtle.
        // TODO(#168 follow-up): fold locality_map.write() into the
        // consolidated block and move broadcast_pinned_mirror_ack
        // after the consolidated write so the "register BEFORE ack"
        // invariant is preserved.
        if !notification.pinned_mirror_entries.is_empty() {
            if let Some(ref locality_map) = self.locality_map {
                let endpoint = if notification.worker_cas_endpoint.is_empty() {
                    self.cas_endpoint.as_str()
                } else {
                    notification.worker_cas_endpoint.as_str()
                };
                // (Spoof-check audit log was hoisted to function entry;
                // it covers ALL sinks of `worker_cas_endpoint`, not just
                // this one.)
                if !endpoint.is_empty() {
                    let digests: Vec<DigestInfo> = notification
                        .pinned_mirror_entries
                        .iter()
                        .filter_map(|e| {
                            e.digest
                                .as_ref()
                                .and_then(|d| DigestInfo::try_from(d.clone()).ok())
                        })
                        .collect();
                    if !digests.is_empty() {
                        debug!(
                            worker_id=?self.worker_id,
                            endpoint,
                            count=digests.len(),
                            "BlobsAvailable: registering dispatcher-pushed pinned_mirror_entries in locality_map (#168 item K)"
                        );
                        locality_map.write().register_blobs(endpoint, &digests);
                    }
                }
            }
            if let Some(ref dispatcher) = self.small_blob_dispatcher {
                dispatcher.broadcast_pinned_mirror_ack(&notification.pinned_mirror_entries);
            }
        }

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
