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

//! Integration test for #463: mount `AcServer` on the worker's
//! `cas_server_port` listener.
//!
//! ## Why
//!
//! Server-side `AcProxyStore::create_worker_connection` (in
//! `nativelink-store/src/ac_proxy_store.rs`) dials the worker's
//! `worker_cas_endpoint` URL (typically `grpcs://<worker>:40081`) with a
//! `StoreType::Ac` `GrpcStore`, issuing
//! `/build.bazel.remote.execution.v2.ActionCache/GetActionResult`.
//!
//! Before this commit, the worker's listener — built in
//! `nativelink-worker/src/local_worker.rs` inside the
//! `if let Some(cas_port) = config.cas_server_port` block — mounted only
//! `CasServer + ByteStreamServer`. Tonic synthesized
//! `Status::unimplemented(...)` for every AC RPC, surfacing as
//! `Code::Unimplemented` to the server (1503 warns/day since #277
//! shipped 2026-05-07).
//!
//! ## Production composition
//!
//! Test composes exactly what production builds inside the
//! `local_worker.rs` `cas_server_port` block:
//!   - `StoreManager` with the AC store registered
//!   - `AcServer::new` with `instance_name: ""` (matches what
//!     `AcProxyStore::create_worker_connection` sends — `String::new()`)
//!   - `AcServer::into_service()` mounted on a `tonic::transport::Server`
//!     listener
//!
//! Without TLS the client/server dial each other over plain TCP. The
//! TLS wrap is symmetric — TLS doesn't change which service handles a
//! given path, so a plain-TCP test is sufficient to assert the path is
//! mounted.

use core::time::Duration;
use std::sync::Arc;

use bytes::BytesMut;
use prost::Message;
use tonic::Code;
use tonic::transport::{Channel, Endpoint, Server};

use nativelink_config::cas_server::{AcStoreConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, GetActionResultRequest, digest_function,
};
use nativelink_service::ac_server::AcServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreLike;
use nativelink_worker::local_worker::build_cas_router;

/// Outer timeout. The wire-up test should complete in well under a
/// second on any machine; 30s guards against a CI machine that's
/// briefly overloaded. A regression manifests as a `Code::Unimplemented`
/// response (or a hang exceeding this budget).
const OUTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Hash for the test action digest. Chosen at random; not load-bearing.
const TEST_HASH: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

/// Build a `StoreManager` with a single in-memory store at the given
/// name. Mirrors the production wire-up which uses an in-memory fast
/// tier for the AC store (`worker.json5: AC_MAIN_STORE.fast_slow.fast =
/// memory`).
async fn make_store_manager_with_ac() -> Arc<StoreManager> {
    let store_manager = Arc::new(StoreManager::new());
    let store = store_factory(
        &StoreSpec::Memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await
    .expect("memory store factory");
    store_manager.add_store("worker_ac", store);
    store_manager
}

/// Insert a synthetic `ActionResult` proto under `digest` in the AC
/// store, simulating a prior `upload_ac_results` from this worker.
async fn populate_ac_store(store_manager: &StoreManager) -> DigestInfo {
    let store = store_manager.get_store("worker_ac").expect("ac store");
    let action_result = ActionResult {
        exit_code: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    action_result
        .encode(&mut buf)
        .expect("encode ActionResult");
    let bytes = buf.freeze();
    let digest = DigestInfo::try_new(TEST_HASH, bytes.len() as i64).expect("valid digest");
    store
        .update_oneshot(digest, bytes)
        .await
        .expect("update_oneshot must succeed in MemoryStore");
    digest
}

/// Worker-listener message size limits (matched to the production
/// `WORKER_CAS_MAX_*` constants in `local_worker.rs`).
const TEST_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Spawn a tonic server on an ephemeral port using the production
/// `build_cas_router` helper. CAS service is always present; ByteStream
/// service is always present (matching the production worker listener);
/// AC service is conditional on `mount_ac=true`.
///
/// **Production composition.** Going through `build_cas_router` ensures
/// the test cannot drift from production by skipping a service. The
/// pre-fix-up version of this helper hand-rolled
/// `Server::builder().add_service(cas_svc)` with NO ByteStream service
/// (testing-czar MAJOR-1, #463 fix-up).
///
/// `mount_ac=true` is the post-#463 production form. `mount_ac=false`
/// is the pre-#463 production form AND the mutation form for the
/// regression assertion below: the test's success-path call MUST fail
/// when `mount_ac=false`.
async fn spawn_cas_listener(
    store_manager: Arc<StoreManager>,
    mount_ac: bool,
) -> (Channel, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let cas_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: nativelink_config::cas_server::CasStoreConfig {
            cas_store: "worker_ac".to_string(),
        },
    }];
    let bytestream_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: nativelink_config::cas_server::ByteStreamConfig {
            cas_store: "worker_ac".to_string(),
            ..Default::default()
        },
    }];
    let cas_server = nativelink_service::cas_server::CasServer::new(
        &cas_configs,
        &store_manager,
        None,
    )
    .expect("CasServer::new");
    let bytestream_server = nativelink_service::bytestream_server::ByteStreamServer::new(
        &bytestream_configs,
        &store_manager,
        None,
    )
    .expect("ByteStreamServer::new");

    let ac_server_opt = if mount_ac {
        let ac_configs = vec![WithInstanceName {
            instance_name: String::new(),
            config: AcStoreConfig {
                ac_store: "worker_ac".to_string(),
                // Match production (post-#463 security fix-up).
                read_only: true,
            },
        }];
        Some(AcServer::new(&ac_configs, &store_manager).expect("AcServer::new"))
    } else {
        None
    };

    let routes = build_cas_router(
        cas_server,
        bytestream_server,
        ac_server_opt,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_MESSAGE_SIZE,
    );

    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_routes(routes)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    let channel = Endpoint::try_from(format!("http://127.0.0.1:{port}"))
        .expect("valid endpoint")
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("connect to in-process AC server");
    (channel, handle)
}

/// **After-fix invariant.** When an AcServer is mounted on the worker
/// CAS listener (the wire-up this commit adds), an `ActionCacheClient`
/// dial with `GetActionResult` for a populated digest returns
/// `Ok(ActionResult)`, NOT `Code::Unimplemented`.
///
/// Mutation: pass `mount_ac=false` to `spawn_cas_listener`. Test must
/// red-fail with the bespoke `panic!("expected ActionCacheServer
/// mounted on cas_server_port — #463 wire-up regression: status=...")`
/// message. Restoring `mount_ac=true` makes it green again.
#[tokio::test(flavor = "multi_thread")]
async fn worker_cas_listener_mounts_ac_server_test() {
    let store_manager = make_store_manager_with_ac().await;
    let digest = populate_ac_store(&store_manager).await;
    let (channel, server_handle) = spawn_cas_listener(store_manager, true).await;

    let mut client = ActionCacheClient::new(channel);

    let result = tokio::time::timeout(OUTER_TIMEOUT, async {
        client
            .get_action_result(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(Digest {
                    hash: digest.packed_hash().to_string(),
                    size_bytes: digest.size_bytes() as i64,
                }),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: vec![],
                digest_function: digest_function::Value::Sha256.into(),
            })
            .await
    })
    .await;

    server_handle.abort();

    let resp = result
        .expect("outer timeout — AcServer call hung — wire-up incomplete")
        .unwrap_or_else(|status| {
            panic!(
                "expected ActionCacheServer mounted on cas_server_port — \
                 #463 wire-up regression: status={:?}, msg={:?}",
                status.code(),
                status.message()
            )
        });
    let ar = resp.into_inner();
    assert_eq!(
        ar.exit_code, 0,
        "decoded ActionResult did not match populated entry"
    );
}

/// **Pre-fix-shaped negative control.** Without an AcServer mounted on
/// the worker listener (only CAS service mounted, the pre-#463
/// shape), the AC RPC lands on no handler — tonic synthesizes
/// `Code::Unimplemented`. This was the production behavior from #277
/// (2026-05-07) until this commit and is exactly the 1503 warns/day
/// pattern in the AcProxyStore peer-fetch logs.
///
/// Keeping this test green guards against accidental no-op rebases —
/// it asserts that the framework's behavior without an AcServer is
/// `Unimplemented` (not `NotFound`, not `Internal`).
#[tokio::test(flavor = "multi_thread")]
async fn worker_cas_listener_without_ac_returns_unimplemented_test() {
    let store_manager = make_store_manager_with_ac().await;
    let (channel, server_handle) = spawn_cas_listener(store_manager, false).await;

    let mut client = ActionCacheClient::new(channel);

    let result = tokio::time::timeout(OUTER_TIMEOUT, async {
        client
            .get_action_result(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(Digest {
                    hash: TEST_HASH.to_string(),
                    size_bytes: 1,
                }),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: vec![],
                digest_function: digest_function::Value::Sha256.into(),
            })
            .await
    })
    .await;

    server_handle.abort();

    let status = result
        .expect("outer timeout — Unimplemented should be synthesized fast")
        .expect_err("expected Err(Unimplemented) without AcServer mounted");
    assert_eq!(
        status.code(),
        Code::Unimplemented,
        "without AcServer mounted, tonic should synthesize Unimplemented for AC RPCs — \
         got code={:?}, msg={:?}",
        status.code(),
        status.message()
    );
}

/// **#463 NotFound regression (over-action coverage).** When an
/// `AcServer` is mounted but the AC store has no entry for the
/// requested digest, the response MUST be `Code::NotFound` — NOT a
/// synthesised `Ok(empty ActionResult)` and NOT `Code::Unimplemented`.
/// This guards the dominant post-#463 outcome (the commit message
/// names this: "After this fix, NotFound becomes the dominant outcome
/// for evicted-pin cases").
///
/// Production composition: [`spawn_cas_listener`] uses
/// `build_cas_router` so this test exercises CAS + ByteStream + AC,
/// matching the `local_worker.rs` `cas_server_port` block exactly.
///
/// Mutation: comment out the `Err(e)` arm in
/// `nativelink-service/src/ac_server.rs::inner_get_action_result`
/// (the branch that surfaces NotFound) and replace with
/// `Ok(Response::new(ActionResult::default()))` — test must red-fail
/// with the bespoke `"#463 NotFound regression: ..."` message.
#[tokio::test(flavor = "multi_thread")]
async fn worker_cas_listener_returns_notfound_for_missing_digest_test() {
    // Empty store — populate_ac_store is NOT called.
    let store_manager = make_store_manager_with_ac().await;
    let (channel, server_handle) = spawn_cas_listener(store_manager, true).await;

    let mut client = ActionCacheClient::new(channel);

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        client
            .get_action_result(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(Digest {
                    hash: TEST_HASH.to_string(),
                    size_bytes: 1,
                }),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: vec![],
                digest_function: digest_function::Value::Sha256.into(),
            })
            .await
    })
    .await;

    server_handle.abort();

    let outcome = result.expect("outer timeout — NotFound should be synthesized fast");
    match outcome {
        Ok(response) => {
            let ar = response.into_inner();
            panic!(
                "#463 NotFound regression: status_code=Ok, msg={ar:?} — \
                 over-action: empty store should NotFound, not Ok(empty ActionResult)"
            );
        }
        Err(status) if status.code() == Code::NotFound => {
            // Expected: NotFound surfaces cleanly through tonic.
        }
        Err(status) => {
            panic!(
                "#463 NotFound regression: status_code={:?}, msg={:?} — \
                 over-action: empty store should NotFound, not Ok(empty ActionResult)",
                status.code(),
                status.message()
            );
        }
    }
}

/// **#463 recursion-defense regression (perf-optimizer BLOCK proof).**
/// When the worker's AC store is wrapped in an `AcProxyStore` whose
/// only registered peer endpoint is *the same listener*, an inbound
/// `GetActionResult` would loop server → worker → server → ... unless
/// the recursion-defense gate fires.
///
/// **Topology:**
/// ```text
///   AcServer { store: AcProxyStore { inner: empty MemoryStore,
///                                    peers: [self_endpoint] } }
/// ```
///
/// **First hop** (no header):
///   - `AcServer.inner_get_action_result(is_peer_fetch=false)`
///   - `AcProxyStore::get_part` → inner NotFound → `try_read_from_peer`
///   - `try_read_from_peer` scopes `IS_AC_PEER_FETCH=true`, dials
///     `self_endpoint` via the injected `GrpcStore` worker connection
///   - `GrpcStore::get_action_result` reads task-local, attaches
///     `x-nativelink-peer-fetch: 1` header on outbound
///
/// **Second hop** (header present):
///   - `AcServer.inner_get_action_result(is_peer_fetch=true)`
///   - Refuses GrpcStore shortcut (none here anyway), scopes
///     `IS_AC_PEER_FETCH=true` around the inner call
///   - `AcProxyStore::get_part` reads task-local → SKIPS fan-out
///   - Returns inner NotFound → bubbles back up to first hop
///   - First hop's `try_read_from_peer` returns NotFound after the
///     recorded peer-attempt, removes the registry entry, surfaces
///     NotFound to the client
///
/// Without the gate at any of (a) header attach in
/// `GrpcStore::get_action_result`, (b) header read in
/// `AcServer.get_action_result`, (c) scope in
/// `AcServer.inner_get_action_result`, (d) gate in
/// `AcProxyStore::get_part`: the recursion would form and the test
/// would time out at 5s.
///
/// **Mutation candidates** (each must red-fail with the bespoke
/// `"#463 recursion-defense regression: ..."` message):
///   - Comment out `IS_AC_PEER_FETCH.scope(true, ...)` in
///     `ac_proxy_store.rs::try_read_from_peer`
///   - Comment out the metadata insert in
///     `grpc_store.rs::get_action_result`
///   - Comment out the `is_peer_fetch` check / gate in
///     `ac_server.rs::inner_get_action_result`
///   - Comment out the gate at the top of
///     `ac_proxy_store.rs::get_part`
#[tokio::test(flavor = "multi_thread")]
async fn worker_cas_listener_recursion_defense_test() {
    use nativelink_store::ac_proxy_store::AcProxyStore;
    use nativelink_store::grpc_store::GrpcStore;
    use nativelink_util::ac_pin_registry::new_shared_ac_pin_registry;
    use nativelink_util::store_trait::Store;

    let store_manager = Arc::new(StoreManager::new());

    // 1. Inner: empty MemoryStore — always returns NotFound.
    let inner_memory_store = store_factory(
        &StoreSpec::Memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await
    .expect("memory store factory");

    // 2. Build a digest the AcProxyStore will look up.
    let digest =
        DigestInfo::try_new(TEST_HASH, 1).expect("valid digest");

    // 3. Bind ephemeral port FIRST so we know the loopback URL up
    //    front (we need it to register the endpoint in the AC pin
    //    registry and to inject the loopback worker connection).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let self_endpoint = format!("http://127.0.0.1:{port}");

    // 4. Pin registry with the test digest advertised under our own
    //    endpoint. This is the trigger that makes AcProxyStore want
    //    to peer-fetch.
    let registry = new_shared_ac_pin_registry();
    registry.register_ac_pin(&self_endpoint, Arc::from("test_store"), digest);

    // 5. Construct AcProxyStore + inject a `StoreType::Ac` GrpcStore
    //    pointing at the same listener as the loopback worker
    //    connection. Without `inject_worker_connection`, the proxy
    //    would lazily build the connection via tls config etc.; the
    //    test seam keeps the test plaintext.
    let proxy = AcProxyStore::new(inner_memory_store, registry);
    let loopback_grpc = GrpcStore::new(&nativelink_config::stores::GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![nativelink_config::stores::GrpcEndpoint {
            address: self_endpoint.clone(),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 2,
            tcp_keepalive_s: 30,
            http2_keepalive_interval_s: 30,
            http2_keepalive_timeout_s: 60,
            tcp_nodelay: true,
            use_http3: false,
        }],
        store_type: nativelink_config::stores::StoreType::Ac,
        retry: nativelink_config::stores::Retry::default(),
        max_concurrent_requests: 0,
        connections_per_endpoint: 1,
        rpc_timeout_s: 5,
        batch_update_threshold_bytes: 0,
        max_concurrent_batch_rpcs: 0,
        parallel_chunk_read_threshold: 0,
        parallel_chunk_count: 0,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: Some(2000),
        chunked_writes_enabled: false,
        chunked_v2_writes_enabled: false,
    })
    .await
    .expect("loopback GrpcStore");
    proxy.inject_worker_connection(&self_endpoint, Store::new(loopback_grpc));

    // 6. Mount the proxy as the AC store and stand up the listener.
    store_manager.add_store("worker_ac", Store::new(proxy));

    let cas_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: nativelink_config::cas_server::CasStoreConfig {
            cas_store: "worker_ac".to_string(),
        },
    }];
    let bytestream_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: nativelink_config::cas_server::ByteStreamConfig {
            cas_store: "worker_ac".to_string(),
            ..Default::default()
        },
    }];
    let cas_server = nativelink_service::cas_server::CasServer::new(
        &cas_configs,
        &store_manager,
        None,
    )
    .expect("CasServer::new");
    let bytestream_server = nativelink_service::bytestream_server::ByteStreamServer::new(
        &bytestream_configs,
        &store_manager,
        None,
    )
    .expect("ByteStreamServer::new");
    let ac_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: AcStoreConfig {
            ac_store: "worker_ac".to_string(),
            read_only: true,
        },
    }];
    let ac_server = AcServer::new(&ac_configs, &store_manager).expect("AcServer::new");
    let routes = build_cas_router(
        cas_server,
        bytestream_server,
        Some(ac_server),
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_MESSAGE_SIZE,
    );

    let server_handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_routes(routes)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    let channel = Endpoint::try_from(self_endpoint.clone())
        .expect("valid endpoint")
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("connect to loopback AC server");
    let mut client = ActionCacheClient::new(channel);

    // 7. Issue the AC GetActionResult. The recursion-defense gate
    //    must terminate the inner peer-fetch within the 5s budget.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        client
            .get_action_result(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(Digest {
                    hash: digest.packed_hash().to_string(),
                    size_bytes: digest.size_bytes() as i64,
                }),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: vec![],
                digest_function: digest_function::Value::Sha256.into(),
            })
            .await
    })
    .await;

    server_handle.abort();

    match result {
        Err(_elapsed) => {
            panic!(
                "#463 recursion-defense regression: AC peer-fetch looped past 5s — \
                 header gate stripped (expected NotFound within budget)"
            );
        }
        Ok(Err(status)) if status.code() == Code::NotFound => {
            // Expected: peer-fetch terminates via the inner-store
            // skip and surfaces NotFound.
        }
        Ok(Ok(response)) => {
            let ar = response.into_inner();
            panic!(
                "#463 recursion-defense regression: expected NotFound, got Ok(...) — \
                 inner store has no entry; recursion gate may have allowed \
                 cross-talk: {ar:?}"
            );
        }
        Ok(Err(status)) => {
            panic!(
                "#463 recursion-defense regression: expected NotFound, got \
                 status_code={:?}, msg={:?}",
                status.code(),
                status.message()
            );
        }
    }
}
