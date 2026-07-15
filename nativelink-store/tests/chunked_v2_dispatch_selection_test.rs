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

//! v1 WriteChunked removed: `update_via_chunked_inner` now dispatches the
//! V2 wire shape UNCONDITIONALLY (there is no write-path selector). This
//! test proves the worker upload path is always V2.
//!
//! Test geometry:
//! - Bind an in-process `CasExtensions` server whose `write_chunked` records
//!   "v1" and whose `write_chunked_v2` records "v2".
//! - Build a `GrpcStore` pointed at the server, enable chunked writes.
//! - Drive two CHUNK_SIZE blobs: BOTH must reach the server via the V2 RPC
//!   (`write_chunked_v2`); the server must NEVER see the v1 `write_chunked`.
//!
//! Mutation hint: in `grpc_store.rs::update_via_chunked_inner`, swapping the
//! `WorkerApiWriteChunkedV2Dispatcher` construction back to the (removed)
//! v1 `WorkerApiWriteChunkedDispatcher` would make the server record "v1"
//! and this test fails on `assert_eq!(log, &["v2", "v2"])`.
//!
//! Transport scope: this test drives the Tcp arm only (`dual_transport:
//! false`, `use_http3: false` — the production config). The Quic and Dual
//! arms construct the identical V2 dispatcher; exercising them would
//! require binding a QUIC server for zero additional branch coverage.

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk, WriteChunkedFrame, WriteChunkedResponse,
    cas_extensions_server::{CasExtensions, CasExtensionsServer},
    write_chunked_frame,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreKey, StoreLike, UploadSizeInfo};
use sha2::{Digest as _, Sha256};
use tokio_stream::StreamExt;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// Fake `CasExtensions` server that records which RPC was called.
struct RecordingServer {
    rpc_log: Arc<Mutex<Vec<&'static str>>>,
}

#[tonic::async_trait]
impl CasExtensions for RecordingServer {
    type WriteChunkedV2Stream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<
                        WriteChunkedFrame,
                        tonic::Status,
                    >,
                > + Send
                + 'static,
        >,
    >;

    async fn write_chunked(
        &self,
        request: tonic::Request<tonic::Streaming<WriteChunk>>,
    ) -> Result<tonic::Response<WriteChunkedResponse>, tonic::Status> {
        // Record the RPC name.
        {
            let mut log = self.rpc_log.lock().unwrap();
            log.push("v1");
        }
        // Drain inbound chunks and compute total bytes.
        let mut req = request.into_inner();
        let mut total_bytes: u64 = 0;
        while let Some(chunk_result) = req.next().await {
            let chunk = chunk_result.map_err(|e| {
                tonic::Status::internal(format!("chunk read error: {e}"))
            })?;
            total_bytes += chunk.chunk_bytes.len() as u64;
        }
        Ok(tonic::Response::new(WriteChunkedResponse {
            committed_digest: None,
            committed_size: total_bytes,
        }))
    }

    async fn write_chunked_v2(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteChunk>>,
    ) -> Result<tonic::Response<Self::WriteChunkedV2Stream>, tonic::Status> {
        // Record the RPC name.
        {
            let mut log = self.rpc_log.lock().unwrap();
            log.push("v2");
        }
        // Drain the inbound stream to compute total bytes.
        let mut req = _request.into_inner();
        let mut total_bytes: u64 = 0;
        while let Some(chunk_result) = req.next().await {
            let chunk = chunk_result.map_err(|e| {
                tonic::Status::internal(format!("chunk read error: {e}"))
            })?;
            total_bytes += chunk.chunk_bytes.len() as u64;
        }
        // V2 protocol: exactly one FinalResponse frame (no acks needed).
        let frame = WriteChunkedFrame {
            payload: Some(write_chunked_frame::Payload::FinalResponse(
                WriteChunkedResponse {
                    committed_digest: None,
                    committed_size: total_bytes,
                },
            )),
        };
        let stream = tokio_stream::once(Ok::<_, tonic::Status>(frame));
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// End-to-end test verifying that `update_via_chunked_inner` dispatches the
/// V2 RPC UNCONDITIONALLY now that the v1 WriteChunked path is removed.
///
/// This is NOT a tautology — it drives actual writes through the store and
/// observes which RPC the server receives.
///
/// Mutation verification: reverting the V2 dispatcher construction in
/// `grpc_store.rs::update_via_chunked_inner` to a v1 dispatcher would make
/// the server record "v1" and the `["v2", "v2"]` assertion fires.
#[nativelink_test]
async fn chunked_worker_upload_always_uses_v2_rpc_end_to_end() -> Result<(), Error> {
    let rpc_log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    // Bind an in-process CasExtensions server on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_rpc_log = rpc_log.clone();
    let server_handle = tokio::spawn(async move {
        let server = RecordingServer {
            rpc_log: server_rpc_log,
        };
        let _ = tonic::transport::Server::builder()
            .add_service(CasExtensionsServer::new(server))
            .serve_with_incoming(incoming)
            .await;
    });

    // Build GrpcStore with chunked_writes_enabled=false (default); the
    // worker upload path is unconditionally V2.
    let spec = GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![GrpcEndpoint {
            address: format!("http://127.0.0.1:{port}"),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 1,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
            tcp_nodelay: true,
            use_http3: false,
        }],
        store_type: StoreType::Cas,
        retry: Retry {
            max_retries: 0,
            delay: 0.0,
            jitter: 0.0,
            ..Default::default()
        },
        max_concurrent_requests: 0,
        connections_per_endpoint: 1,
        rpc_timeout_s: 5,
        batch_update_threshold_bytes: 0,
        max_concurrent_batch_rpcs: 1,
        parallel_chunk_read_threshold: 0,
        parallel_chunk_count: 0,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: Some(2000),
        chunked_writes_enabled: false,
        use_legacy_resource_names: false,
    };
    let store = GrpcStore::new(&spec).await?;
    store.enable_chunked_writes();

    // Write #1: no flag toggling. v1 removed → MUST select the V2
    // dispatcher → WriteChunkedV2 RPC.
    let blob_a = vec![0xa5_u8; CHUNK_SIZE];
    let digest_a = DigestInfo::new(sha256(&blob_a), CHUNK_SIZE as u64);
    let (mut tx, rx) = make_buf_channel_pair();
    let send_task = tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob_a)).await);
        drop(tx.send_eof());
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        StoreLike::update(
            &*store,
            StoreKey::from(digest_a),
            rx,
            UploadSizeInfo::ExactSize(CHUNK_SIZE as u64),
        ),
    )
    .await
    .expect("must not deadlock — V2 dispatch must complete or fail promptly");
    send_task.abort();
    result.expect("write #1 must succeed against the recording server");

    {
        let log = rpc_log.lock().unwrap();
        assert_eq!(
            log.as_slice(),
            &["v2"],
            "v1 WriteChunked removed: the FIRST worker upload MUST select the V2 \
             dispatcher (WriteChunkedV2 RPC), but got {:?}. update_via_chunked_inner \
             must construct WorkerApiWriteChunkedV2Dispatcher unconditionally.",
            *log
        );
    }

    // Write #2 (no toggling): also MUST select V2.
    let blob_b = vec![0xb3_u8; CHUNK_SIZE];
    let digest_b = DigestInfo::new(sha256(&blob_b), CHUNK_SIZE as u64);
    let (mut tx, rx) = make_buf_channel_pair();
    let send_task = tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob_b)).await);
        drop(tx.send_eof());
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        StoreLike::update(
            &*store,
            StoreKey::from(digest_b),
            rx,
            UploadSizeInfo::ExactSize(CHUNK_SIZE as u64),
        ),
    )
    .await
    .expect("must not deadlock — V2 dispatch must complete or fail promptly");
    send_task.abort();
    result.expect("write #2 must succeed against the recording server");

    {
        let log = rpc_log.lock().unwrap();
        assert_eq!(
            log.as_slice(),
            &["v2", "v2"],
            "v1 WriteChunked removed: EVERY worker upload MUST select the V2 \
             dispatcher (WriteChunkedV2 RPC); the server must NEVER receive the v1 \
             write_chunked RPC. Got {:?}. If a \"v1\" appears, update_via_chunked_inner \
             is still constructing the removed V1 dispatcher.",
            *log
        );
    }

    server_handle.abort();
    Ok(())
}
