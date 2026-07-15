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

//! #FL-688 (D) — Single end-to-end A+B test crossing the WHOLE chain:
//! fresh-output worker upload → wire header → server G1 carve-out → durable
//! write to inner store.
//!
//! Today the A (server carve-out `bytestream_server.rs:3364` `&& !is_worker &&
//! !is_mirror`) and B (worker scope `IS_WORKER_REQUEST.scope(true)` in
//! `local_worker.rs handle_upload_missing_blobs`) tests exist in separate files.
//! The (A) follow-up seam test (fresh_output_is_worker_header_seam_test.rs)
//! proves the new scope wrap in `spawn_upload_to_remote_impl` stamps the header.
//!
//! THIS test is the single integration test crossing BOTH fixes simultaneously
//! for the fresh-output upload path, in the PRODUCTION topology:
//!
//!   worker `spawn_upload_to_remote_for_test`
//!     └─ IS_WORKER_REQUEST.scope(true) per-future wrap (fix A follow-up)
//!     └─ FastSlowStore{fast: FilesystemStore, slow: WorkerProxyStore(GrpcStore)}
//!        └─ GrpcStore::write stamps `x-nativelink-worker` on the wire
//!        └─ real tonic transport
//!        └─ ByteStreamServer (fix A server-side G1 carve-out: `&& !is_worker`)
//!           └─ WorkerProxyStore(inner MemoryStore) — locality seeded so G1
//!              WOULD fire if `!is_worker` guard were absent
//!           └─ inner MemoryStore.has(digest) → SOME after upload succeeds
//!
//! ## Mutation coverage (both A fixes must be present)
//!
//! Mutation (A-server): remove `&& !is_worker && !is_mirror` from the G1 gate
//!   in `bytestream_server.rs` → G1 fires on is_worker=true → inner store
//!   stays empty → test red-fails with bespoke message:
//!   "G1 server carve-out missing — inner store empty after fresh-output
//!   upload; G1 must not fire on worker uploads (FL-688 A+D mutation)".
//!
//! Mutation (A-worker): remove `IS_WORKER_REQUEST.scope(true, ...)` from
//!   `uploads.push` in `running_actions_manager.rs spawn_upload_to_remote_impl`
//!   → header absent → server sees is_worker=false → G1 fires → inner store
//!   empty → same bespoke red-fail.
//!
//! Both mutations produce the SAME terminal assertion failure because the
//! root invariant is the same: "fresh-output upload MUST land in the server's
//! durable inner store when the digest is locality-advertised."

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use tokio::time::timeout;

use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::ResultExt;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStreamServer;
use nativelink_service::bytestream_server::ByteStreamServer as NlByteStreamServer;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::action_messages::{ActionResult, FileInfo, NameOrPath};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningActionsManagerArgs, RunningActionsManagerImpl,
};

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(15);
const CAS_STORE_NAME: &str = "cas_STORE";
const INSTANCE_NAME: &str = "main";

fn make_temp_path(label: &str) -> String {
    format!(
        "{}/{}/{}",
        std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string()),
        rand::random::<u64>(),
        label,
    )
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

fn make_grpc_spec(addr: String) -> nativelink_config::stores::GrpcSpec {
    nativelink_config::stores::GrpcSpec {
        // Must match the server's instance_name so the resource_name prefix is
        // parseable. GrpcStore prepends instance_name to the resource_name;
        // the server routes on it. Mismatch → "'instance_name' not configured".
        instance_name: INSTANCE_NAME.to_string(),
        endpoints: vec![nativelink_config::stores::GrpcEndpoint {
            address: addr,
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 0,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
            tcp_nodelay: true,
            use_http3: false,
        }],
        store_type: nativelink_config::stores::StoreType::Cas,
        retry: nativelink_config::stores::Retry {
            max_retries: 0,
            ..Default::default()
        },
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 5,
        batch_update_threshold_bytes: 1_048_576,
        max_concurrent_batch_rpcs: 8,
        parallel_chunk_read_threshold: 0,
        parallel_chunk_count: 0,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: None,
        chunked_writes_enabled: false,
        use_legacy_resource_names: false,
    }
}

/// Spawn a real `NlByteStreamServer` (with the G1 carve-out) backed by
/// `WorkerProxyStore(MemoryStore)`. The locality map is seeded with `digest`
/// so that `WorkerProxyStore::has(digest)` returns `Some` WITHOUT the blob
/// in the inner store — exactly the G1-bug scenario. Returns:
///   - the server's bound address (for the worker's GrpcStore)
///   - the inner `MemoryStore` (for post-upload has_with_results assertion)
///   - a `JoinHandle` to abort on test teardown
async fn spawn_g1_server_with_locality_seeded(
    digest: DigestInfo,
) -> (
    String,
    Store,
    tokio::task::JoinHandle<()>,
) {
    let inner_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality = new_shared_blob_locality_map();
    // Seed the locality map so WPS::has(digest) returns Some WITHOUT the blob
    // being in the inner MemoryStore. This is the G1-bug state: the uploading
    // worker's own locality advertisement causes the G1 gate to fire
    // (returning phantom-ok without persisting) unless the `!is_worker` carve-
    // out is present.
    locality
        .write()
        .register_blobs("grpc://worker-under-test:50071", &[digest]);
    let proxy = WorkerProxyStore::new(inner_mem.clone(), locality);
    let manager = Arc::new(StoreManager::new());
    manager.add_store(CAS_STORE_NAME, Store::new(proxy));

    let nl_bs_server = NlByteStreamServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: ByteStreamConfig {
                cas_store: CAS_STORE_NAME.to_string(),
                persist_stream_on_disconnect_timeout_s: 0,
                max_bytes_per_stream: 4 * 1024 * 1024, // 4 MiB
                ..Default::default()
            },
        }],
        manager.as_ref(),
        None,
    )
    .expect("NlByteStreamServer::new");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("http://127.0.0.1:{port}");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(nl_bs_server))
            .serve_with_incoming(incoming)
            .await;
    });

    (addr, inner_mem, handle)
}

async fn build_manager_with_server(
    server_addr: String,
) -> (Arc<FilesystemStore>, Arc<RunningActionsManagerImpl>) {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content"),
        temp_path: make_temp_path("tmp"),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store = FilesystemStore::new(&fast_config).await.unwrap();
    let grpc = GrpcStore::new(&make_grpc_spec(server_addr)).await.unwrap();
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(Store::new(grpc), locality_map);
    // FastSlowStore::new returns Arc<FastSlowStore> directly.
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config.clone()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast_store.clone()),
        Store::new(proxy),
    );

    let root_action_directory = make_temp_path("root_action_dir");
    nativelink_util::common::fs::create_dir_all(&root_action_directory)
        .await
        .unwrap();
    let manager = Arc::new(
        RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: fss.clone(),
            ac_store: None,
            ac_mirror_target: None,
            historical_store: Store::new(fss),
            upload_action_result_config:
                &nativelink_config::cas_server::UploadActionResultConfig {
                    upload_ac_results_strategy:
                        nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::from_secs(30),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            deferred_output_uploads_enabled: false,
        })
        .unwrap(),
    );
    (fast_store, manager)
}

/// (D) END-TO-END A+B test: fresh-output worker upload → wire header → server
/// G1 carve-out → inner durable store.
///
/// The server's locality map is seeded with `digest` so G1 WOULD fire (returning
/// phantom-ok) if either fix is missing:
///   - Missing A (server carve-out): G1 fires → inner store stays empty.
///   - Missing A-follow-up (worker scope): header absent → is_worker=false →
///     G1 fires → inner store stays empty.
///
/// Both mutations produce the same red-fail: the bespoke assertion below.
///
/// Seams crossed (production composition):
///   1. Worker:   `spawn_upload_to_remote_for_test` (real upload loop)
///   2. Task-local: IS_WORKER_REQUEST.scope(true) per-future wrap (A follow-up)
///   3. GrpcStore: ByteStream write stamps `x-nativelink-worker` on the wire
///   4. Wire:     real tonic transport (TCP, in-process)
///   5. Server:   `NlByteStreamServer::bytestream_write` G1 gate (A carve-out)
///   6. Inner:    `MemoryStore::update` — the durable storage
///   7. Assert:   `inner.has_with_results(digest)` returns Some
#[nativelink_test]
async fn fresh_output_upload_e2e_writes_through_g1_server_to_inner_store()
-> Result<(), Box<dyn core::error::Error>> {
    let payload = Bytes::from_static(b"g1_fresh_output_e2e_write_through_test");
    let digest = mk_digest(0x61, payload.len());

    // Spawn the real server with G1 carve-out; locality seeded so G1 WOULD
    // fire without the !is_worker guard.
    let (server_addr, inner_mem, server_handle) =
        spawn_g1_server_with_locality_seeded(digest).await;

    // Build the worker-side RunningActionsManagerImpl.
    let (fast_store, manager) = build_manager_with_server(server_addr).await;

    // Seed the output blob on the worker's fast (FilesystemStore) tier —
    // simulating the state after inner_upload_results completes.
    fast_store
        .as_pin()
        .update_oneshot(nativelink_util::store_trait::StoreKey::from(digest), payload)
        .await
        .err_tip(|| "seed fast tier")?;

    // Verify inner store does NOT have the blob pre-upload (sanity check).
    {
        let mut pre = vec![None];
        inner_mem
            .as_store_driver_pin()
            .has_with_results(&[nativelink_util::store_trait::StoreKey::from(digest)], &mut pre)
            .await
            .err_tip(|| "has pre-check")?;
        assert!(
            pre[0].is_none(),
            "pre-condition: inner server store must not have the blob before upload"
        );
    }

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("out.bin".to_string()),
            digest,
            is_executable: false,
        }],
        ..Default::default()
    };

    let handle = manager
        .spawn_upload_to_remote_for_test(&action_result, None)
        .expect("upload task must be scheduled");

    timeout(DEADLOCK_DETECTOR, handle)
        .await
        .expect("spawn_upload_to_remote must not deadlock — G1 e2e write-through")
        .expect("upload task must not panic");

    server_handle.abort();

    // Post-upload: inner store MUST have the blob.
    let mut post = vec![None];
    inner_mem
        .as_store_driver_pin()
        .has_with_results(&[nativelink_util::store_trait::StoreKey::from(digest)], &mut post)
        .await
        .err_tip(|| "has post-check")?;
    assert!(
        post[0].is_some(),
        "G1 server carve-out missing OR IS_WORKER_REQUEST scope missing from \
         spawn_upload_to_remote_impl — inner store empty after fresh-output \
         upload; G1 must not fire on worker uploads (FL-688 A+D mutation). \
         Mutation guide: (1) remove `&& !is_worker && !is_mirror` from G1 gate \
         in bytestream_server.rs, OR (2) remove IS_WORKER_REQUEST.scope(true) \
         from uploads.push in running_actions_manager.rs spawn_upload_to_remote_impl \
         — either mutation produces this failure."
    );

    Ok(())
}
