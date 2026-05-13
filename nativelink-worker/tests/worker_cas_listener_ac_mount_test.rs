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
use prost::Message;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Code;

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

/// Spawn a tonic server on an ephemeral port mounting `CasServer` plus
/// (when `mount_ac` is true) `AcServer`. This mirrors the production
/// composition built by `local_worker.rs` for `cas_server_port` — the
/// CAS service is always present, AC service is conditional on
/// `ac_store` being plumbed.
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

    // CAS service is always mounted — production worker CAS listener
    // exposes both CAS and (post-#463) AC; tests use it to assert the
    // tonic Router otherwise behaves correctly.
    let cas_configs = vec![WithInstanceName {
        instance_name: String::new(),
        config: nativelink_config::cas_server::CasStoreConfig {
            cas_store: "worker_ac".to_string(),
        },
    }];
    let cas_server = nativelink_service::cas_server::CasServer::new(
        &cas_configs,
        &store_manager,
        None,
    )
    .expect("CasServer::new");
    let cas_svc = cas_server.into_service();

    let ac_svc_opt = if mount_ac {
        let ac_configs = vec![WithInstanceName {
            instance_name: String::new(),
            config: AcStoreConfig {
                ac_store: "worker_ac".to_string(),
                read_only: false,
            },
        }];
        let ac_server = AcServer::new(&ac_configs, &store_manager).expect("AcServer::new");
        Some(ac_server.into_service())
    } else {
        None
    };

    let handle = tokio::spawn(async move {
        let mut router = Server::builder().add_service(cas_svc);
        if let Some(ac_svc) = ac_svc_opt {
            router = router.add_service(ac_svc);
        }
        drop(router.serve_with_incoming(incoming).await);
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
