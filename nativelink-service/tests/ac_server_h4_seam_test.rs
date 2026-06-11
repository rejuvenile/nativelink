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

//! Real-tonic-boundary seam test for the x-nativelink-worker header chain.
//!
//! # Design authority
//!
//! dangling-ac-design-2026-06-10.md §3 Revision 5, Phase 3 Deliverable (d):
//!
//!   > A test that composes ALL seams in the IS_WORKER_REQUEST → header →
//!   > registration pipeline end-to-end using a real tonic server.
//!
//! # Seams under test
//!
//! 1. Worker side: `IS_WORKER_REQUEST.scope(true, grpc_store.update_action_result(...))`
//!    (`nativelink-store/src/grpc_store.rs:~1911`)
//! 2. Transport: real tonic TCP channel + `x-nativelink-worker` header injection
//!    in `GrpcStore::update_action_result` (`grpc_store.rs:~1915`)
//! 3. Server side: `AcServer::update_action_result` extraction of
//!    `x-nativelink-worker` header → `is_worker=true` →
//!    `register_output_locality` fires
//!
//! A header rename on ONE side (injection site OR extraction site) breaks the
//! invariant while all existing unit tests continue to pass.
//!
//! # What it asserts
//!
//! After `IS_WORKER_REQUEST.scope(true, grpc_store.update_action_result(...))`
//! completes, `registry.snapshot_endpoint(endpoint)` must be `Some` (the output
//! digest was registered in the pending registry via the real tonic boundary).
//!
//! # Mutation
//!
//! Change the header name at the injection site in `GrpcStore::update_action_result`
//! from `"x-nativelink-worker"` to `"x-nativelink-worker-renamed"`:
//! → test red-fails with:
//!   "seam-crossing: IS_WORKER_REQUEST scope did not reach AcServer registration
//!    — header name mismatch"
//!
//! # Seam boundary proof (comment)
//!
//! This test is the ONLY test that can detect a header-name divergence between
//! the GrpcStore injection site (grpc_store.rs:1915) and the AcServer extraction
//! site (ac_server.rs:540). Component-level unit tests at each end cannot detect
//! such a divergence because:
//!   - grpc_store tests mock the server side (no real tonic extraction).
//!   - ac_server tests inject the header manually (no real GrpcStore emission).
//! Only a test that composes both ends over a real tonic transport can detect it.

use core::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;

use nativelink_config::cas_server::WithInstanceName;
use nativelink_config::stores::{GrpcSpec, MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, OutputFile, UpdateActionResultRequest, action_cache_server::ActionCacheServer,
    digest_function,
};
use nativelink_service::ac_server::{AcServer, SharedLivenessChecker};
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::ac_pin_registry::{SharedAcPinRegistry, new_shared_ac_pin_registry};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::IS_WORKER_REQUEST;
use parking_lot::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const INSTANCE_NAME: &str = "seam_test";
/// Hex hash used for the output file digest.
const FILE_HASH: &str = "ccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabb";
const FILE_SIZE: i64 = 1024;
/// Hex hash used for the action digest.
const ACTION_HASH: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
const ACTION_SIZE: i64 = 42;

fn make_liveness_checker(endpoints: &[String]) -> SharedLivenessChecker {
    let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(endpoints.iter().cloned().collect()));
    Arc::new(move |ep: &str| set.lock().contains(ep))
}

/// Spin up a real tonic server hosting `AcServer` and return:
///   - bound port (for GrpcStore endpoint)
///   - `registry` (to assert registrations after the RPC)
///   - server JoinHandle (abort on test exit)
async fn spawn_ac_tonic_server(
    registry: SharedAcPinRegistry,
    live_endpoints: Vec<String>,
) -> Result<(u16, tokio::task::JoinHandle<()>), Error> {
    let sm = Arc::new(StoreManager::new());
    sm.add_store(
        "ac_seam",
        store_factory(&StoreSpec::Memory(MemorySpec::default()), &sm, None).await?,
    );

    let checker = make_liveness_checker(&live_endpoints);

    let ac_server = AcServer::new_with_pending_registry(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: nativelink_config::cas_server::AcStoreConfig {
                ac_store: "ac_seam".to_string(),
                read_only: false,
            },
        }],
        &sm,
        Some(registry),
        Some(checker),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind must succeed");
    let port = listener
        .local_addr()
        .expect("local_addr must be set")
        .port();
    let incoming = TcpListenerStream::new(listener);

    let svc = ActionCacheServer::new(ac_server);
    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    Ok((port, handle))
}

/// Build a `GrpcStore` pointing at `http://127.0.0.1:{port}` for AC RPCs.
async fn make_grpc_store(port: u16) -> Result<Arc<GrpcStore>, Error> {
    // Parse via serde_json5 to get serde defaults applied cleanly, without
    // requiring a `Default` impl on `GrpcSpec`.
    let spec_json = format!(
        r#"{{
            "instance_name": "{INSTANCE_NAME}",
            "endpoints": [{{
                "address": "http://127.0.0.1:{port}",
                "connect_timeout_s": 5,
                "tcp_nodelay": true,
                "use_http3": false
            }}],
            "store_type": "ac",
            "connections_per_endpoint": 1
        }}"#
    );
    let spec: GrpcSpec = serde_json5::from_str(&spec_json)
        .map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "GrpcSpec parse failed: {e:?}"
            )
        })?;
    GrpcStore::new(&spec).await
}

// ─── The seam test ─────────────────────────────────────────────────────────────

/// End-to-end seam-crossing test for IS_WORKER_REQUEST → x-nativelink-worker
/// header → AcServer extraction → pending_output_locality_registry registration.
///
/// Seams:
///   S1 (worker scope): IS_WORKER_REQUEST.scope(true, ...) wraps the GrpcStore call.
///   S2 (header inject): GrpcStore::update_action_result injects x-nativelink-worker.
///   S3 (header extract): AcServer::update_action_result extracts x-nativelink-worker.
///   S4 (registration): register_output_locality inserts digest into registry.
///
/// Mutation: rename "x-nativelink-worker" → "x-nativelink-worker-renamed" at S2
/// (grpc_store.rs:~1915). S3 extracts "x-nativelink-worker" (unchanged) →
/// is_worker=false → register_output_locality not called → registry stays empty →
/// test red-fails:
///   "seam-crossing: IS_WORKER_REQUEST scope did not reach AcServer registration
///    — header name mismatch"
#[nativelink_test]
async fn is_worker_request_scope_propagates_through_real_tonic_boundary() -> Result<(), Error> {
    let registry = new_shared_ac_pin_registry();

    // Build the GrpcStore first so we know its local address for the liveness checker.
    // The cas_endpoint we'll send is the grpc store's own address representation.
    // In production this is the worker's own CAS port; in this test we use a
    // placeholder because we only care that the endpoint passes liveness.
    let cas_endpoint = "grpc://test-worker:50081".to_string();

    // Spawn the server, pre-declaring cas_endpoint as live.
    let (port, server_handle) = spawn_ac_tonic_server(
        registry.clone(),
        vec![cas_endpoint.clone()],
    )
    .await?;

    // Build a GrpcStore pointing at the in-process tonic server.
    let grpc_store = make_grpc_store(port).await?;
    let grpc_store_for_rpc = grpc_store.clone();

    // Build the UAR request with the output file and cas_endpoint.
    let ar = ActionResult {
        output_files: vec![OutputFile {
            path: "out/lib.rlib".to_string(),
            digest: Some(Digest {
                hash: FILE_HASH.to_string(),
                size_bytes: FILE_SIZE,
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let uar = UpdateActionResultRequest {
        instance_name: INSTANCE_NAME.to_string(),
        action_digest: Some(Digest {
            hash: ACTION_HASH.to_string(),
            size_bytes: ACTION_SIZE,
        }),
        action_result: Some(ar),
        results_cache_policy: None,
        digest_function: digest_function::Value::Sha256.into(),
        cas_endpoint: cas_endpoint.clone(),
    };

    // ─── S1: IS_WORKER_REQUEST.scope(true, ...) ──────────────────────────────
    // This propagates through:
    //   S2: GrpcStore::update_action_result → injects x-nativelink-worker header
    //       (grpc_store.rs:~1915)
    //   S3: AcServer::update_action_result → extracts x-nativelink-worker header
    //       (ac_server.rs:~540) → is_worker=true
    //   S4: register_output_locality → inserts digest into pending registry
    //
    // If S2 renames the header and S3 keeps "x-nativelink-worker", is_worker=false
    // at S3, S4 never fires, registry stays empty → test fails with bespoke message.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        IS_WORKER_REQUEST.scope(
            true,
            grpc_store_for_rpc.update_action_result(tonic::Request::new(uar)),
        ),
    )
    .await
    .expect("UAR RPC must complete within 5s — tonic server spawn or connect failed")
    .map_err(|e| {
        nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "GrpcStore::update_action_result failed: {e:?}"
        )
    })?;
    drop(result);

    // ─── Assert: registry must contain the output digest ─────────────────────
    //
    // If IS_WORKER_REQUEST → x-nativelink-worker is working end-to-end,
    // the pending registry must show an entry for cas_endpoint after the RPC.
    let snap = tokio::time::timeout(
        Duration::from_secs(5),
        async { registry.snapshot_endpoint(&cas_endpoint) },
    )
    .await
    .expect("snapshot must be synchronous — timeout is deadlock detector");

    assert!(
        snap.is_some(),
        "seam-crossing: IS_WORKER_REQUEST scope did not reach AcServer registration \
         — header name mismatch; seams: \
         S1=IS_WORKER_REQUEST.scope, S2=GrpcStore header inject, \
         S3=AcServer header extract, S4=register_output_locality; \
         registry empty after real-tonic UAR with is_worker=true scope; \
         likely cause: header name diverged between S2 and S3",
    );

    // Verify the registered digest matches the output file's digest.
    let snap = snap.unwrap();
    let expected_digest = DigestInfo::try_new(FILE_HASH, FILE_SIZE)
        .expect("test digest must be valid");
    let registered_digests: Vec<DigestInfo> = snap.into_iter().map(|(_, d)| d).collect();
    assert!(
        registered_digests.contains(&expected_digest),
        "seam-crossing: digest mismatch — expected {expected_digest:?} in registry; \
         found {registered_digests:?}; \
         the registration ran but wrote a different digest than declared in the UAR",
    );

    server_handle.abort();
    Ok(())
}
