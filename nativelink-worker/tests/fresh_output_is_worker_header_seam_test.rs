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

//! #FL-688 (A) follow-up — `spawn_upload_to_remote_impl` fresh-output upload
//! must carry `x-nativelink-worker=1` on the wire.
//!
//! ## The gap
//!
//! The G1 (B) fix (`local_worker.rs::handle_upload_missing_blobs`) wraps each
//! per-blob upload future in `IS_WORKER_REQUEST.scope(true, ...)` so the
//! `x-nativelink-worker` header is stamped on the wire. The ANALOGOUS path —
//! `running_actions_manager.rs::spawn_upload_to_remote_impl` — uploads freshly
//! produced action outputs to the server's slow tier AFTER action completion.
//! This path runs inside a `tokio::spawn` (`:6461`) which STRIPS task-locals:
//! the inner per-digest upload futures run without `IS_WORKER_REQUEST` set,
//! so GrpcStore does NOT stamp the header → the server sees `is_worker=false`
//! → if the digest is already locality-advertised, the G1 short-circuit
//! phantom-acks it (same pre-existing data-loss WINDOW the G1 fix closed for
//! backfill; here it is only *recovered* later by backfill).
//!
//! ## Fix
//!
//! Wrap each `uploads.push(async move { ... })` in
//! `IS_WORKER_REQUEST.scope(true, ...)` INSIDE the spawned task, mirroring
//! the landed pattern in `local_worker.rs handle_upload_missing_blobs`.
//!
//! ## Why per-future scoping is correct
//!
//! The chain from the spawned task through to GrpcStore is spawn-free:
//! `slow_store` inside the spawned task is
//! `WorkerProxyStore → GrpcStore::update → ByteStream write` (all inline,
//! no nested `tokio::spawn`). The `IS_WORKER_REQUEST.scope(true, fut)` wraps
//! the entire per-digest retry loop, so every `slow_store.update_oneshot` and
//! `slow_store.update` call inside that loop reads the task-local correctly.
//!
//! ## Seams crossed (per `.claude/rules/testing-contracts.md`)
//!
//!   1. Producer:     `RunningActionsManagerImpl::spawn_upload_to_remote_impl`
//!                    (real, via `spawn_upload_to_remote_for_test`).
//!   2. Task-local:   `IS_WORKER_REQUEST.scope(true)` per-future wrap (fix A).
//!                    This is the ONLY seam the test falsifies: mutate by
//!                    removing the wrap → header absent → test red-fails.
//!   3. Store chain:  `FastSlowStore{fast: FilesystemStore (output bytes),
//!                    slow: WorkerProxyStore(real GrpcStore → in-proc server)}`
//!                    — mirrors the production topology.
//!   4. WPS/GrpcStore passthrough: inline update → ByteStream write (no spawn).
//!   5. Header-stamp: `GrpcStore::write` (:1687/:1801) reads IS_WORKER_REQUEST
//!                    and inserts `x-nativelink-worker` on the stream open.
//!   6. Wire receiver: in-process tonic ByteStream server; records and ACKs.
//!
//! ## Mutation
//!
//! Remove `IS_WORKER_REQUEST.scope(true, ...)` wrap from `uploads.push` in
//! `running_actions_manager.rs spawn_upload_to_remote_impl` → task-local
//! absent → header not stamped → test red-fails with bespoke message:
//! "fresh-output upload did not carry x-nativelink-worker — IS_WORKER_REQUEST
//! scope missing from spawn_upload_to_remote_impl (FL-688 A follow-up)".

use core::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use tokio::time::timeout;

use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::action_messages::{ActionResult, FileInfo, NameOrPath};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningActionsManagerArgs, RunningActionsManagerImpl,
};

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(15);

/// In-process ByteStream server that records the `x-nativelink-worker`
/// header on every received `write` RPC, then ACKs the full committed size.
struct HeaderCapturingByteStream {
    last_x_nativelink_worker: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl ByteStream for HeaderCapturingByteStream {
    type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

    async fn read(
        &self,
        _request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("read not used"))
    }

    async fn write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let header_value = request
            .metadata()
            .get("x-nativelink-worker")
            .and_then(|v| v.to_str().ok())
            .map(std::string::ToString::to_string);
        *self.last_x_nativelink_worker.lock().unwrap() = header_value;
        let mut inbound = request.into_inner();
        let mut committed: i64 = 0;
        while let Ok(Some(chunk)) = inbound.message().await {
            committed += chunk.data.len() as i64;
            if chunk.finish_write {
                break;
            }
        }
        Ok(tonic::Response::new(WriteResponse { committed_size: committed }))
    }

    async fn query_write_status(
        &self,
        _request: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("query_write_status not used"))
    }
}

async fn spawn_capturing_bytestream_server(
) -> (String, Arc<Mutex<Option<String>>>, tokio::task::JoinHandle<()>) {
    let capture: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let bs = HeaderCapturingByteStream {
        last_x_nativelink_worker: capture.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(bs))
            .serve_with_incoming(incoming)
            .await;
    });
    (format!("http://127.0.0.1:{port}"), capture, handle)
}

fn make_grpc_spec(addr: String) -> nativelink_config::stores::GrpcSpec {
    nativelink_config::stores::GrpcSpec {
        instance_name: String::new(),
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

/// Build the PRODUCTION worker store topology:
/// `FastSlowStore{fast: FilesystemStore (output bytes),
///               slow: WorkerProxyStore(real GrpcStore → addr)}`
/// Returns both the FSS and the concrete FilesystemStore so we can seed
/// the output blob onto the fast tier.
async fn make_fss_over_grpc(
    addr: String,
) -> (Arc<FilesystemStore>, Arc<FastSlowStore>) {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content"),
        temp_path: make_temp_path("tmp"),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store = FilesystemStore::new(&fast_config).await.unwrap();
    let grpc = GrpcStore::new(&make_grpc_spec(addr)).await.unwrap();
    let locality_map = nativelink_util::blob_locality_map::new_shared_blob_locality_map();
    let proxy = nativelink_store::worker_proxy_store::WorkerProxyStore::new(
        Store::new(grpc),
        locality_map,
    );
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
    (fast_store, fss)
}

async fn build_manager(cas_store: Arc<FastSlowStore>) -> Arc<RunningActionsManagerImpl> {
    let root_action_directory = make_temp_path("root_action_dir");
    nativelink_util::common::fs::create_dir_all(&root_action_directory)
        .await
        .unwrap();
    Arc::new(
        RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: cas_store.clone(),
            ac_store: None,
            ac_mirror_target: None,
            historical_store: Store::new(cas_store),
            upload_action_result_config:
                &nativelink_config::cas_server::UploadActionResultConfig {
                    upload_ac_results_strategy:
                        nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
            max_action_timeout: core::time::Duration::MAX,
            max_upload_timeout: core::time::Duration::from_secs(30),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: core::time::Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            // SYNCHRONOUS mode: gives up after SYNC_MAX_RETRIES on a
            // retryable error. Here the server ACKs, so the loop completes
            // after one successful attempt.
            deferred_output_uploads_enabled: false,
        })
        .unwrap(),
    )
}

/// (A) SMALL fresh-output upload (≤1 MiB) from `spawn_upload_to_remote_impl`
/// MUST carry `x-nativelink-worker=1` on the wire.
///
/// Pre-fix: the upload runs inside `tokio::spawn` which strips IS_WORKER_REQUEST
/// → header absent → server sees is_worker=false → G1 short-circuits on locality
/// → blob never durably stored → server re-requests forever.
///
/// Fix: wrap `uploads.push(async move { ... })` in
/// `IS_WORKER_REQUEST.scope(true, ...)` inside the spawned task.
///
/// Mutation: remove the scope wrap → header absent → test red-fails with
/// "fresh-output upload did not carry x-nativelink-worker — IS_WORKER_REQUEST
/// scope missing from spawn_upload_to_remote_impl (FL-688 A follow-up)".
#[nativelink_test]
async fn fresh_output_small_upload_carries_is_worker_header_on_wire() {
    let payload = Bytes::from_static(b"fresh_output_small_fl688_a");
    let digest = mk_digest(0x51, payload.len());

    let (addr, capture, server_handle) = spawn_capturing_bytestream_server().await;
    let (fast_store, fss) = make_fss_over_grpc(addr).await;

    // Seed the output blob onto the FAST tier (FilesystemStore). This is
    // the state after `inner_upload_results` completes and before
    // `spawn_upload_to_remote` is called: the blob is on the worker's disk
    // but has not yet been pushed to the server's slow tier.
    fast_store
        .as_pin()
        .update_oneshot(nativelink_util::store_trait::StoreKey::from(digest), payload)
        .await
        .expect("seed fast tier");

    let manager = build_manager(fss).await;
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
        .expect("upload task must be scheduled (one output digest)");

    timeout(DEADLOCK_DETECTOR, handle)
        .await
        .expect("spawn_upload_to_remote must not deadlock — fresh output is_worker seam")
        .expect("upload task must not panic");

    server_handle.abort();

    let captured = capture.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "fresh-output upload did not carry x-nativelink-worker — IS_WORKER_REQUEST \
         scope missing from spawn_upload_to_remote_impl (FL-688 A follow-up). \
         The server sees is_worker=false and, if the digest is locality-advertised, \
         the G1 short-circuit phantom-acks it without storing the blob durably. \
         Got {captured:?}"
    );
}

/// (A) LARGE fresh-output upload (>1 MiB → streaming ByteStream `update`)
/// also MUST carry `x-nativelink-worker=1`. Covers the streaming upload branch
/// (same spawned task, same IS_WORKER_REQUEST scope).
#[nativelink_test]
async fn fresh_output_large_upload_carries_is_worker_header_on_wire() {
    // 1 MiB + 1 byte → exceeds BATCH_THRESHOLD (1 MiB) → streaming branch.
    let size = 1024 * 1024 + 1;
    let payload = Bytes::from(vec![0xA3_u8; size]);
    let digest = mk_digest(0x52, size);

    let (addr, capture, server_handle) = spawn_capturing_bytestream_server().await;
    let (fast_store, fss) = make_fss_over_grpc(addr).await;

    fast_store
        .as_pin()
        .update_oneshot(nativelink_util::store_trait::StoreKey::from(digest), payload)
        .await
        .expect("seed fast tier");

    let manager = build_manager(fss).await;
    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("large_out.bin".to_string()),
            digest,
            is_executable: false,
        }],
        ..Default::default()
    };

    let handle = manager
        .spawn_upload_to_remote_for_test(&action_result, None)
        .expect("upload task must be scheduled (one large output digest)");

    timeout(DEADLOCK_DETECTOR, handle)
        .await
        .expect("spawn_upload_to_remote must not deadlock — large fresh output is_worker seam")
        .expect("upload task must not panic");

    server_handle.abort();

    let captured = capture.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "large fresh-output upload did not carry x-nativelink-worker — IS_WORKER_REQUEST \
         scope missing from spawn_upload_to_remote_impl large (streaming) branch \
         (FL-688 A follow-up). Got {captured:?}"
    );
}
