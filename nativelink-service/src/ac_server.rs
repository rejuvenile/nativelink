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
use std::collections::HashMap;

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
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_util::log_utils::throughput_mbps;
use nativelink_util::stall_detector::StallGuard;
use nativelink_util::store_trait::{IS_AC_PEER_FETCH, IS_MIRROR_REQUEST, Store, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, debug, error, error_span, instrument};

#[derive(Debug, Clone)]
pub struct AcStoreInfo {
    store: Store,
    read_only: bool,
}

pub struct AcServer {
    stores: HashMap<String, AcStoreInfo>,
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
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
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
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "AC::update_action_result",
        );
        IS_MIRROR_REQUEST
            .scope(
                is_mirror,
                self.inner_update_action_result(request, is_mirror)
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
