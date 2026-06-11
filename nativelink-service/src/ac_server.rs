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
use core::fmt::Debug;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use nativelink_config::cas_server::{AcStoreConfig, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, GetActionResultRequest, UpdateActionResultRequest,
};
use nativelink_store::ac_utils::{ESTIMATED_DIGEST_SIZE, get_and_decode_digest};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::ac_pin_registry::SharedAcPinRegistry;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_util::log_utils::throughput_mbps;
use nativelink_util::stall_detector::StallGuard;
use nativelink_util::store_trait::{IS_AC_PEER_FETCH, IS_MIRROR_REQUEST, Store, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, debug, error, error_span, instrument, warn};

/// A callable that returns `true` iff the given `cas_endpoint` belongs to a
/// currently-connected worker. Populated from `WorkerApiServer`'s live
/// `endpoint_state` map via [`WorkerApiServer::liveness_checker`].
///
/// `None` on listeners that have no `worker_api` service (e.g. the
/// Bazel-facing port 50051) — in that case all registration is skipped.
pub type SharedLivenessChecker = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Maximum length of a `cas_endpoint` string the server will accept for
/// pending-output registration. Oversized strings are warn-logged and
/// silently ignored — the AC update itself still succeeds.
///
/// (Revision 5 item c — defense-in-depth; workers are mTLS-trusted so this
/// guards against config mistakes and forwarded-header bugs, not active
/// adversaries.)
const MAX_CAS_ENDPOINT_LEN: usize = 256;

/// AC-store id used when registering output digests in the pending registry.
/// Matches the convention used by `BlobsAvailable` field-17 entries: the
/// `store_id` is an empty string for CAS and the AC-store name for AC pins.
///
/// For output-locality entries the digest→endpoint mapping is CAS-level
/// (the output blobs live in the worker's CAS), so an empty store_id is
/// correct — the CCS consult will ask the pending registry "is this CAS
/// digest fetchable?" not "is this AC entry pinned?".
pub const PENDING_STORE_ID: &str = "";

#[derive(Debug, Clone)]
pub struct AcStoreInfo {
    store: Store,
    read_only: bool,
}

pub struct AcServer {
    stores: HashMap<String, AcStoreInfo>,
    /// (#12 H4 phase 2) Optional registry for worker-output locality.
    ///
    /// When `Some` and a `UpdateActionResult` arrives from the worker plane
    /// (`x-nativelink-worker` header set) with a non-empty `cas_endpoint`,
    /// the server inserts each output digest → `cas_endpoint` BEFORE
    /// committing the AC entry. This makes the H4 invariant structural:
    ///
    ///   locality-visible(outputs) happens-before AC-publish
    ///
    /// `None` on listeners without a co-resident `worker_api` service.
    ///
    /// CAPPED: inherits `AcPinRegistry`'s `DEFAULT_MAX_AC_PINS_PER_ENDPOINT =
    /// 1_000_000` per endpoint. Over-cap insertions are silently skipped by the
    /// registry (with a warn). The BIS-drain loop and disconnect-wipe keep the
    /// live count well below cap in practice.
    pending_output_locality_registry: Option<SharedAcPinRegistry>,
    /// Liveness checker sourced from the co-resident `WorkerApiServer`.
    /// Returns `true` iff the given `cas_endpoint` is in the server's
    /// live connected-worker set. A claimed endpoint that fails this check
    /// is warn-logged and silently ignored — no registration, no AC error.
    liveness_checker: Option<SharedLivenessChecker>,
    // CAPPED AT 1: single monotonically-increasing u64; no bounding needed.
    /// (#12 H4 phase 2) Total `UpdateActionResult` RPCs that successfully
    /// registered output digests into `pending_output_locality_registry`.
    /// Incremented once per RPC with a live, non-empty, non-oversized
    /// `cas_endpoint`. Sustained zero after phase 2 lands = wire not
    /// connected or liveness check always rejecting.
    pending_output_registrations_total: Arc<AtomicU64>,
}

impl Debug for AcServer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcServer").finish()
    }
}

impl AcServer {
    pub fn new(
        configs: &[WithInstanceName<AcStoreConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        Self::new_with_pending_registry(configs, store_manager, None, None)
    }

    /// Construct with an optional `pending_output_locality_registry` and
    /// liveness checker.
    ///
    /// Called from `nativelink.rs` on listeners that also host a
    /// `worker_api` service (port 50051 in production). The `registry`
    /// and `liveness_checker` are `None` on listeners with no workers.
    pub fn new_with_pending_registry(
        configs: &[WithInstanceName<AcStoreConfig>],
        store_manager: &StoreManager,
        pending_output_locality_registry: Option<SharedAcPinRegistry>,
        liveness_checker: Option<SharedLivenessChecker>,
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(configs.len());
        for config in configs {
            let store = store_manager.get_store(&config.ac_store).ok_or_else(|| {
                make_input_err!("'ac_store': '{}' does not exist", config.ac_store)
            })?;
            stores.insert(
                config.instance_name.to_string(),
                AcStoreInfo {
                    store,
                    read_only: config.read_only,
                },
            );
        }
        Ok(Self {
            stores: stores.clone(),
            pending_output_locality_registry,
            liveness_checker,
            pending_output_registrations_total: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    /// Returns the total number of successful output-digest registrations
    /// into `pending_output_locality_registry` since server startup.
    ///
    /// Value is 0 on listeners constructed via [`Self::new`] (no registry).
    pub fn pending_output_registrations_total(&self) -> u64 {
        self.pending_output_registrations_total
            .load(Ordering::Acquire)
    }

    /// Collect CAS digests referenced by `action_result`'s output files,
    /// stdout, and stderr. Mirrors the logic in
    /// `completeness_checking_store::get_digests_and_output_dirs` but
    /// restricted to the flat-file outputs (no Tree decoding — tree children
    /// are resolved by the CCS path at query time). Returns only non-zero
    /// digests to avoid inserting the well-known empty-blob digest.
    fn collect_output_digests(action_result: &ActionResult) -> Vec<DigestInfo> {
        let mut digests = Vec::new();
        for file in &action_result.output_files {
            if let Some(d) = file.digest.as_ref() {
                if d.size_bytes > 0 {
                    if let Ok(di) = DigestInfo::try_from(d.clone()) {
                        digests.push(di);
                    }
                }
            }
        }
        for dir in &action_result.output_directories {
            if let Some(d) = dir.tree_digest.as_ref() {
                if d.size_bytes > 0 {
                    if let Ok(di) = DigestInfo::try_from(d.clone()) {
                        digests.push(di);
                    }
                }
            }
        }
        if let Some(d) = action_result.stdout_digest.as_ref() {
            if d.size_bytes > 0 {
                if let Ok(di) = DigestInfo::try_from(d.clone()) {
                    digests.push(di);
                }
            }
        }
        if let Some(d) = action_result.stderr_digest.as_ref() {
            if d.size_bytes > 0 {
                if let Ok(di) = DigestInfo::try_from(d.clone()) {
                    digests.push(di);
                }
            }
        }
        digests
    }

    /// Register `action_result`'s output digests in
    /// `pending_output_locality_registry` for `cas_endpoint` BEFORE the AC
    /// entry is committed.
    ///
    /// Preconditions checked here (not by caller):
    ///   - `cas_endpoint` is non-empty.
    ///   - `cas_endpoint.len() ≤ MAX_CAS_ENDPOINT_LEN` (Revision 5 item c).
    ///   - `cas_endpoint` passes `liveness_checker` (endpoint is connected).
    ///
    /// Any precondition failure → warn + return (AC update continues).
    fn register_output_locality(&self, cas_endpoint: &str, action_result: &ActionResult) {
        let (Some(registry), Some(checker)) = (
            self.pending_output_locality_registry.as_ref(),
            self.liveness_checker.as_ref(),
        ) else {
            // No registry on this listener → skip silently.
            return;
        };

        if cas_endpoint.is_empty() {
            return;
        }

        // Revision 5 item (c): 256-byte length guard.
        if cas_endpoint.len() > MAX_CAS_ENDPOINT_LEN {
            warn!(
                cas_endpoint_len = cas_endpoint.len(),
                max = MAX_CAS_ENDPOINT_LEN,
                "UpdateActionResult: cas_endpoint exceeds max length, ignoring for \
                 pending_output_locality_registry (AC update still proceeds)"
            );
            return;
        }

        // Liveness check: endpoint must be a currently-connected worker.
        if !checker(cas_endpoint) {
            warn!(
                %cas_endpoint,
                "UpdateActionResult: cas_endpoint not in live worker set, ignoring \
                 pending_output_locality_registry registration (AC update still proceeds)"
            );
            return;
        }

        let store_id: Arc<str> = Arc::from(PENDING_STORE_ID);
        let digests = Self::collect_output_digests(action_result);
        if digests.is_empty() {
            return;
        }
        for digest in digests {
            registry.register_ac_pin(cas_endpoint, store_id.clone(), digest);
        }
        // Revision 5 item (b): increment per-UAR-with-live-endpoint counter.
        self.pending_output_registrations_total
            .fetch_add(1, Ordering::AcqRel);
    }

    async fn inner_get_action_result(
        &self,
        request: GetActionResultRequest,
        is_peer_fetch: bool,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        // TODO(palfrey) We should write a test for these errors.
        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special
        // store — UNLESS this RPC is itself a server-side AC peer-fetch
        // hop (`x-nativelink-peer-fetch: 1`). In that case, taking the
        // GrpcStore shortcut would dial the next hop with the same
        // store_info (e.g. central server), and `AcProxyStore` upstream
        // would re-fan-out to the same worker, producing
        // server → worker → server recursion until h2 keepalive
        // (`60s`). Refusing the shortcut routes the read through the
        // wrapping composition; the inner `get_and_decode_digest` call
        // below is scoped under `IS_AC_PEER_FETCH=true` so that the
        // wrapping `AcProxyStore` (if any) skips its own fan-out and
        // any wrapped `GrpcStore` re-attaches the
        // `x-nativelink-peer-fetch` header outbound. The recursion
        // terminates at the first wrapper that does not need to ask
        // someone else — typically returning a clean NotFound.
        // (#463 fix-up: perf-optimizer BLOCK; mirrors the
        // `IS_MIRROR_REQUEST` write-side pattern.)
        if !is_peer_fetch
            && let Some(grpc_store) = store_info
                .store
                .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.get_action_result(Request::new(request)).await;
        }

        let get_start = std::time::Instant::now();
        let res = IS_AC_PEER_FETCH
            .scope(
                is_peer_fetch,
                get_and_decode_digest::<ActionResult>(&store_info.store, digest.into()),
            )
            .await;
        match res {
            Ok(action_result) => {
                let elapsed = get_start.elapsed();
                let size_bytes = action_result.encoded_len() as u64;
                debug!(
                    ?digest,
                    size_bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    throughput_mbps = format!("{:.1}", throughput_mbps(size_bytes, elapsed)),
                    "AC read completed",
                );
                Ok(Response::new(action_result))
            }
            Err(mut e) => {
                let elapsed = get_start.elapsed();
                if e.code == Code::NotFound {
                    // `get_action_result` is frequent to get NotFound errors, so remove all
                    // messages to save space.
                    e.messages.clear();
                    debug!(
                        elapsed_us = elapsed.as_micros() as u64,
                        "AC read NotFound",
                    );
                }
                Err(e)
            }
        }
    }

    async fn inner_update_action_result(
        &self,
        request: UpdateActionResultRequest,
        is_mirror: bool,
        is_worker: bool,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if store_info.read_only {
            return Err(make_err!(
                Code::PermissionDenied,
                "The store '{instance_name}' is read only on this endpoint",
            ));
        }

        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_info
            .store
            .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.update_action_result(Request::new(request)).await;
        }

        let action_result = request
            .action_result
            .err_tip(|| "Action result was not set in message")?;

        // (#12 H4 phase 2) Register output localities BEFORE committing the AC
        // entry. This is the structural ordering guarantee: the pending registry
        // is always visible before any AC entry that references these outputs.
        //
        // Revision 4 (auditor): IS_WORKER is dead end-to-end on the AC path
        // unless explicitly wired. The worker sets `IS_WORKER_REQUEST` in its
        // scope (running_actions_manager.rs); GrpcStore propagates it as the
        // `x-nativelink-worker` header (grpc_store.rs:1915). This site
        // extracts that header (analogous to `IS_MIRROR_REQUEST` extraction
        // in this same function). Production-path test:
        // `ac_server_h4_registration_test::live_endpoint_registers_output_digests_and_increments_counter`.
        //
        // Chesterton citation: `register_action_result_digests` (removed in
        // worker_api_server.rs:1329) did §3a-like registration via an
        // mpsc::channel(1) eviction race; removed because the channel could
        // drop registrations under load. Server-side publish-time registration
        // here is synchronous and in the same handler — no channel, no race.
        // Registration intentionally precedes the AC store write below.
        // A failed AC commit leaves a bounded orphan entry in the pending
        // registry (drained by the BIS-ack loop with store_id="", disconnect
        // wipe on worker reconnect, or per-endpoint cap drop) — accepted per
        // design §3.
        if is_worker {
            self.register_output_locality(&request.cas_endpoint, &action_result);
        }

        // AC integrity contract: `digest` is the `action_digest` (the
        // CAS digest of the *Action* proto). `store_data` below is the
        // serialized *ActionResult* proto — a different message under
        // the same key. `H(store_data) != digest` in general; AC entries
        // are NOT content-addressed by their bytes. See
        // `docs/ac-integrity-contract.md` and the rationale on
        // `nativelink_store::verify_store::VerifyStore`.
        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Provided ActionResult could not be serialized")?;

        let size_bytes = store_data.len() as u64;
        let start = std::time::Instant::now();
        let result = IS_MIRROR_REQUEST
            .scope(is_mirror, async {
                store_info
                    .store
                    .update_oneshot(digest, store_data.freeze())
                    .await
                    .err_tip(|| "Failed to update in action cache")
            })
            .await;
        let elapsed = start.elapsed();
        match &result {
            Ok(()) => {
                debug!(
                    ?digest,
                    size_bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    throughput_mbps = format!("{:.1}", throughput_mbps(size_bytes, elapsed)),
                    "AC write completed",
                );
            }
            Err(e) => {
                error!(
                    ?digest,
                    size_bytes,
                    elapsed_ms = elapsed.as_millis() as u64,
                    ?e,
                    "AC write failed",
                );
            }
        }
        result?;
        Ok(Response::new(action_result))
    }
}

#[tonic::async_trait]
impl ActionCache for AcServer {
    #[instrument(
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        // #463 fix-up (perf-optimizer BLOCK): peer-fetch hops are
        // marked with `x-nativelink-peer-fetch: 1` by
        // `AcProxyStore::try_read_from_peer` (via `GrpcStore::
        // get_action_result`). When set, the inner handler refuses
        // the `GrpcStore` shortcut so a worker whose AC store is
        // itself a bare `GrpcStore` cannot loop the read back at
        // the central server. Mirrors the `x-nativelink-mirror`
        // metadata propagation in `update_action_result` below.
        let is_peer_fetch = grpc_request
            .metadata()
            .contains_key("x-nativelink-peer-fetch");
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "AC::get_action_result",
        );
        let result = self
            .inner_get_action_result(request, is_peer_fetch)
            .instrument(error_span!("ac_server_get_action_result"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In AcServer::get_action_result")?,
            )
            .await;

        if let Err(ref err) = result {
            if err.code != Code::NotFound {
                error!(error = ?err, "Error in get_action_result");
            }
        }

        result.map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        // Mirror writes (server-to-server replication) carry the
        // `x-nativelink-mirror` metadata so we can scope the
        // `IS_MIRROR_REQUEST` task-local for downstream stores.
        let is_mirror = grpc_request
            .metadata()
            .contains_key("x-nativelink-mirror");
        // (#12 H4 phase 2) Worker-plane writes carry `x-nativelink-worker`.
        // Extraction mirrors the `IS_MIRROR_REQUEST` pattern above.
        // Revision 4 (auditor): IS_WORKER was dead on the AC path because
        // neither running_actions_manager nor ac_server extracted/set it.
        // Both wiring points are now in place:
        //   - Worker sets IS_WORKER_REQUEST scope in running_actions_manager
        //     (upload_ac_results); GrpcStore propagates as this header.
        //   - This extraction (the server-side wiring point).
        //
        // The liveness check (in register_output_locality) is the active guard
        // for fabricated registrations: a false-positive entry costs at most one
        // CCS-consult fetch miss → re-execution; workers and all mTLS cert
        // holders are trusted per project policy
        // (memory: feedback_remote_workers_are_trusted.md).
        let is_worker = grpc_request
            .metadata()
            .contains_key("x-nativelink-worker");
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "AC::update_action_result",
        );
        IS_MIRROR_REQUEST
            .scope(
                is_mirror,
                self.inner_update_action_result(request, is_mirror, is_worker)
                    .instrument(error_span!("ac_server_update_action_result"))
                    .with_context(
                        make_ctx_for_hash_func(digest_function)
                            .err_tip(|| "In AcServer::update_action_result")?,
                    ),
            )
            .await
            .map_err(Into::into)
    }
}
