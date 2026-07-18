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

//! #FL-688 (B) — WORKER→SERVER backfill upload must carry `x-nativelink-worker`
//! on the wire. END-TO-END seam test (the proof the WHOLE A+B fix works), in the
//! PRODUCTION store topology.
//!
//! The server-side G1 carve-out (A, bytestream_server.rs:3364) and the batch
//! carve-out (cas_server.rs:458) BOTH key on `is_worker`. But the worker's
//! backfill upload (`handle_upload_missing_blobs`) ran in a fresh task with
//! `IS_WORKER_REQUEST` UNSET, so `GrpcStore` stamped NO `x-nativelink-worker`
//! header → the server saw `is_worker=false` → G1 skipped it / the batch handler
//! ack-gated it (the exact loop worker writes MUST skip) → the worker's
//! sole-copy backfill blob never converged → the server re-requested the same
//! "missing" set every ~61.5s forever (HIGH data-loss risk; the silent Ok also
//! defeated the W4 Err-only requeue_failed_push retry).
//!
//! Fix (B, ONE part — the per-blob scope wrap):
//!   local_worker.rs `handle_upload_missing_blobs`: wrap each per-blob upload
//!   future in `IS_WORKER_REQUEST.scope(true, ...)` so the task-local exists to
//!   be read by `GrpcStore` when it stamps the header.
//!
//! WHY (B) alone suffices — the PRODUCTION chain is SPAWN-FREE (verified at
//! source, local_worker.rs:5752-5806): when `cas_server_port.is_some()` the
//! worker builds `effective_cas_store` = `FastSlowStore{fast=disk,
//! slow=WorkerProxyStore(central GrpcStore)}`. So in `handle_upload_missing_blobs`
//! `slow_store = cas_store.slow_store()` is the **WorkerProxyStore**, NOT a bare
//! GrpcStore.
//!   - `WorkerProxyStore` has NO `update_oneshot` override → it uses the
//!     `StoreDriver` default `update_oneshot` (store_trait.rs:1215), which is
//!     `try_join!(send_fut, self.update(...))` — INLINE, no spawn.
//!   - `WorkerProxyStore::update` (worker_proxy_store.rs:4638) is a pure inline
//!     passthrough: `self.inner.update(...).await`.
//!   - `GrpcStore::update` (grpc_store.rs:3475) for a ≤1 MiB blob (below
//!     `CHUNK_SIZE`, chunked disabled on the worker) falls through to the legacy
//!     ByteStream `write` path, which reads `IS_WORKER_REQUEST` (:1687) and
//!     stamps `x-nativelink-worker` (:1801) — INLINE.
//! The entire ≤1 MiB backfill chain therefore streams; it NEVER enters
//! `GrpcStore::update_oneshot`'s BatchUpdateBlobs coalesce queue (the spawn that
//! would strip the task-local). A live probe (a209c717) confirmed zero worker
//! BatchUpdateBlobs reach the server, corroborating that production uses the
//! streaming path. (An earlier draft added a `GrpcStore::update_oneshot`
//! direct-route bypass "B'"; it targeted a path that is DEAD in this topology —
//! the WPS-wrap "oneshot-path-DEAD" trap — and was dropped. This test pins that:
//! the small-blob case PASSES with (A)+(B) and NO B'.)
//!
//! Seams crossed (per `.claude/rules/testing-contracts.md` identify-the-seam),
//! in the PRODUCTION topology:
//!   1. Producer:      `LocalWorkerImpl::handle_upload_missing_blobs` (real,
//!                      via `handle_upload_missing_blobs_for_test`).
//!   2. Task-local:    the `IS_WORKER_REQUEST.scope(true)` per-blob wrap (B).
//!   3. Store chain:   `FastSlowStore{fast: Memory, slow: WorkerProxyStore(real
//!                      GrpcStore → in-proc server)}` — mirrors local_worker.rs:5758.
//!                      The requested blob is held in FSS `mirror_blobs` (the
//!                      production "worker holds a pinned mirror copy the server
//!                      is asking it to upload back" state).
//!   4. WPS passthrough: `WorkerProxyStore` default `update_oneshot` → `::update`
//!                      → inner `GrpcStore::update` (all inline, no spawn).
//!   5. Header-stamp:  `GrpcStore::write` (:1687/:1801) reads `IS_WORKER_REQUEST`
//!                      and inserts `x-nativelink-worker` on the streaming write.
//!   6. Wire/receiver: the in-process tonic ByteStream server reads the inbound
//!                      metadata — standing in for the server's `is_worker`
//!                      extraction that selects plain-vs-skip/ack-gate.
//!
//! Mutation (red): remove the `IS_WORKER_REQUEST.scope(true)` wrap in
//! local_worker.rs (B) → task-local unset → header absent → bespoke red-fail on
//! BOTH the small-blob and large-blob tests.

use core::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest,
    GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse,
    batch_update_blobs_response,
};
use nativelink_proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use nativelink_worker::local_worker::handle_upload_missing_blobs_for_test;
use tokio::time::timeout;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use utils::local_worker_test_utils::MockWorkerApiClient;
use utils::mock_running_actions_manager::MockRunningActionsManager;

const DEADLOCK_DETECTOR: Duration = Duration::from_secs(10);

/// In-process CAS server that records the `x-nativelink-worker` header value
/// of every received `BatchUpdateBlobs` request, ACKs the blobs OK, and reports
/// every `FindMissingBlobs` digest as MISSING (the production backfill state:
/// the server does not hold the blob, which is why it is backfilling).
struct HeaderCapturingCasServer {
    last_x_nativelink_worker: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl ContentAddressableStorage for HeaderCapturingCasServer {
    async fn find_missing_blobs(
        &self,
        request: tonic::Request<FindMissingBlobsRequest>,
    ) -> Result<tonic::Response<FindMissingBlobsResponse>, tonic::Status> {
        // Report ALL digests missing — the server does not have them (that is
        // why it requested the backfill). The FSS `has_with_results` then
        // resolves the blob from `mirror_blobs`.
        let req = request.into_inner();
        Ok(tonic::Response::new(FindMissingBlobsResponse {
            missing_blob_digests: req.blob_digests,
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: tonic::Request<BatchUpdateBlobsRequest>,
    ) -> Result<tonic::Response<BatchUpdateBlobsResponse>, tonic::Status> {
        let header_value = request
            .metadata()
            .get("x-nativelink-worker")
            .and_then(|v| v.to_str().ok())
            .map(std::string::ToString::to_string);
        *self.last_x_nativelink_worker.lock().unwrap() = header_value;
        let req = request.into_inner();
        let responses = req
            .requests
            .into_iter()
            .map(|r| batch_update_blobs_response::Response {
                digest: r.digest,
                status: Some(nativelink_proto::google::rpc::Status {
                    code: 0,
                    message: String::new(),
                    details: vec![],
                }),
            })
            .collect();
        Ok(tonic::Response::new(BatchUpdateBlobsResponse { responses }))
    }

    async fn batch_read_blobs(
        &self,
        _request: tonic::Request<BatchReadBlobsRequest>,
    ) -> Result<tonic::Response<BatchReadBlobsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("batch_read_blobs not used"))
    }

    type GetTreeStream = futures::stream::Empty<Result<GetTreeResponse, tonic::Status>>;

    async fn get_tree(
        &self,
        _request: tonic::Request<GetTreeRequest>,
    ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_tree not used"))
    }

    async fn split_blob(
        &self,
        _request: tonic::Request<SplitBlobRequest>,
    ) -> Result<tonic::Response<SplitBlobResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("split_blob not used"))
    }

    async fn splice_blob(
        &self,
        _request: tonic::Request<SpliceBlobRequest>,
    ) -> Result<tonic::Response<SpliceBlobResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("splice_blob not used"))
    }
}

/// In-process ByteStream server that records the `x-nativelink-worker` header
/// on every received `write` request (the large/streaming backfill path).
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
        // Drain the inbound stream and ack the total bytes received so the
        // GrpcStore write loop sees a clean Ok.
        let mut inbound = request.into_inner();
        let mut committed: i64 = 0;
        while let Ok(Some(chunk)) = inbound.message().await {
            committed += chunk.data.len() as i64;
            if chunk.finish_write {
                break;
            }
        }
        Ok(tonic::Response::new(WriteResponse {
            committed_size: committed,
        }))
    }

    async fn query_write_status(
        &self,
        _request: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("query_write_status not used"))
    }
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Build a `GrpcSpec` pointing at `addr`. `batch_update_threshold_bytes` is left
/// at the production default (1 MiB) for fidelity, but it is INERT on this path:
/// in the WPS-wrapped topology the small-blob upload reaches `GrpcStore::update`
/// (streaming ByteStream), NOT `GrpcStore::update_oneshot`, so the BatchUpdateBlobs
/// coalesce-queue spawn is never entered — the chain streams in-task and (B)'s
/// scope alone carries the header. `retry.max_retries = 0` so a single RPC is observed.
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
        // Production default (stores.rs:1424). In the WPS-wrapped topology the
        // upload reaches `GrpcStore::update` (streaming ByteStream), NOT
        // `GrpcStore::update_oneshot`, so the coalesce route is never taken on the
        // write path — this value affects only `update_oneshot`, which the WPS
        // slow tier does not invoke. Kept at the production default for fidelity.
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

/// Compose the PRODUCTION worker store topology:
/// `FastSlowStore{fast: Memory, slow: WorkerProxyStore(real GrpcStore → addr)}`
/// — mirroring `local_worker.rs:5758` where the worker wraps its slow tier in a
/// `WorkerProxyStore` whenever `cas_server_port.is_some()`. Using the WPS wrap
/// is load-bearing: `handle_upload_missing_blobs`'s `slow_store` is then the WPS
/// (default `update_oneshot` → inline `update` → inner `GrpcStore::update` →
/// streaming ByteStream write), exactly the spawn-free production chain. (A bare
/// `slow = GrpcStore` would instead route small blobs through
/// `GrpcStore::update_oneshot`'s coalesce-queue spawn — a NON-production path.)
///
/// The requested blob is seeded into `mirror_blobs` (the tier the FSS default
/// `has_with_results` + `get_part` consult — see `backfill_upload_requeue_test`
/// `seed_mirror` for the full rationale).
async fn make_fss_over_grpc(addr: String, digest: DigestInfo, payload: Bytes) -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let grpc = GrpcStore::new(&make_grpc_spec(addr))
        .await
        .expect("GrpcStore::new");
    // Production topology: wrap the GrpcStore slow tier in a WorkerProxyStore
    // (local_worker.rs:5758). The locality map is empty — irrelevant to the
    // upload write path; it only affects WPS read-side peer racing.
    let locality_map = nativelink_util::blob_locality_map::new_shared_blob_locality_map();
    let proxy = nativelink_store::worker_proxy_store::WorkerProxyStore::new(
        Store::new(grpc),
        locality_map,
    );
    let slow = Store::new(proxy);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        slow,
    );
    fss.test_insert_mirror_blob_unchecked(digest, payload);
    fss
}

/// Spawn an in-proc tonic server with BOTH the CAS and ByteStream
/// header-capturing fixtures sharing one capture slot, and return its
/// `http://` address + the capture slot + the server task handle.
async fn spawn_capturing_server() -> (String, Arc<Mutex<Option<String>>>, tokio::task::JoinHandle<()>)
{
    let capture: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let cas = HeaderCapturingCasServer {
        last_x_nativelink_worker: capture.clone(),
    };
    let bs = HeaderCapturingByteStream {
        last_x_nativelink_worker: capture.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ContentAddressableStorageServer::new(cas))
            .add_service(ByteStreamServer::new(bs))
            .serve_with_incoming(incoming)
            .await;
    });
    (format!("http://127.0.0.1:{port}"), capture, handle)
}

/// (B) SMALL-blob backfill (≤1 MiB) in the PRODUCTION WPS-wrapped topology: the
/// upload RPC MUST carry `x-nativelink-worker = 1` on the wire. The blob streams
/// through `WorkerProxyStore` default `update_oneshot` → inline `update` →
/// `GrpcStore::update` → legacy ByteStream `write` (no coalesce-queue spawn), so
/// the `IS_WORKER_REQUEST.scope(true)` wrap (B) ALONE carries the header — NO
/// `GrpcStore::update_oneshot` direct-route bypass ("B'") is needed (that path is
/// never reached via the WPS slow tier). This passing with (A)+(B) and no B' is
/// the falsification that B' was unnecessary.
#[nativelink_test]
async fn backfill_small_blob_upload_carries_is_worker_header_on_wire() {
    let payload = Bytes::from_static(b"small_backfill_blob_seam");
    let digest = mk_digest(21, payload.len());

    let (addr, capture, server_handle) = spawn_capturing_server().await;
    let fss = make_fss_over_grpc(addr, digest, payload).await;

    let ram = Arc::new(MockRunningActionsManager::new());
    ram.set_cas_store(fss);

    timeout(
        DEADLOCK_DETECTOR,
        handle_upload_missing_blobs_for_test::<MockWorkerApiClient, _>(&ram, vec![digest], 4),
    )
    .await
    .expect("handle_upload_missing_blobs (small) must not deadlock — backfill is_worker seam");

    server_handle.abort();

    let captured = capture.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "small-blob backfill upload did not carry x-nativelink-worker — \
         IS_WORKER_REQUEST.scope(true) wrap missing (B). In the production \
         WPS-wrapped topology the ≤1 MiB blob streams (WPS default update_oneshot \
         → inline update → GrpcStore::update → ByteStream write), so the scope \
         wrap alone must carry the header. The server otherwise sees is_worker=false \
         and skips the worker's sole-copy backfill blob (FL-688 stuck loop). \
         Got {captured:?}"
    );
}

/// (B) LARGE-blob backfill (>1 MiB → streaming ByteStream `update`): the
/// streaming write runs in-task (no coalesce spawn), so (B) alone suffices.
/// Still asserted end-to-end to cover BOTH per-blob upload branches.
#[nativelink_test]
async fn backfill_large_blob_upload_carries_is_worker_header_on_wire() {
    // 1 MiB + 1 byte → exceeds the handler's STREAMING_THRESHOLD (1 MiB) → the
    // streaming `slow_store.update` branch (ByteStream write).
    let size = 1024 * 1024 + 1;
    let payload = Bytes::from(vec![0x5A_u8; size]);
    let digest = mk_digest(22, size);

    let (addr, capture, server_handle) = spawn_capturing_server().await;
    let fss = make_fss_over_grpc(addr, digest, payload).await;

    let ram = Arc::new(MockRunningActionsManager::new());
    ram.set_cas_store(fss);

    timeout(
        DEADLOCK_DETECTOR,
        handle_upload_missing_blobs_for_test::<MockWorkerApiClient, _>(&ram, vec![digest], 4),
    )
    .await
    .expect("handle_upload_missing_blobs (streaming) must not deadlock — backfill is_worker seam");

    server_handle.abort();

    let captured = capture.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "streaming backfill upload did not carry x-nativelink-worker — \
         IS_WORKER_REQUEST.scope wrap missing (B). The server then sees \
         is_worker=false and skips the worker's sole-copy backfill blob (FL-688). \
         Got {captured:?}"
    );
}
