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

//! #550 Phase 3: end-to-end test that the `chunked_v2_writes_enabled` flag
//! selects the correct dispatcher in `update_via_chunked_inner`.
//!
//! Test geometry:
//! - Bind an in-process `CasExtensions` server whose `write_chunked` records
//!   "v1" and whose `write_chunked_v2` records "v2".
//! - Build a `GrpcStore` pointed at the server, enable chunked writes.
//! - Drive a CHUNK_SIZE blob with V2 flag OFF: must call V1 RPC.
//! - Enable V2 flag, drive a second blob: must call V2 RPC.
//!
//! Mutation hint: in `grpc_store.rs` the `if v2 {` branch in
//! `update_via_chunked_inner` — flipping to `if !v2 {` must cause this test
//! to fail on the V2 assertion.
//!
//! Transport scope: this test drives the Tcp arm only (`dual_transport:
//! false`, `use_http3: false` — the production config). The Quic
//! (`:2588`) and Dual (`:2606`) arms select the dispatcher with the
//! identical `if v2 { .. } else { .. }` expression; exercising them would
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

/// #550 Phase 3: end-to-end test verifying that the `chunked_v2_writes_enabled`
/// flag selects the correct dispatcher (V1 vs V2 RPC) in
/// `update_via_chunked_inner`.
///
/// This is NOT a tautology — it drives actual writes through the store and
/// observes which RPC the server receives.
///
/// Mutation verification: flipping `if v2 {` to `if !v2 {` in
/// `grpc_store.rs::update_via_chunked_inner` must cause the V2 assertion to
/// fail.
#[nativelink_test]
async fn chunked_v2_flag_selects_v2_rpc_end_to_end() -> Result<(), Error> {
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

    // Build GrpcStore with chunked_writes_enabled=false (default) and
    // chunked_v2_writes_enabled=false (default).
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
        chunked_v2_writes_enabled: false,
    };
    let store = GrpcStore::new(&spec).await?;
    store.enable_chunked_writes();

    // Write #1: V2 flag is OFF (default). Must select V1 dispatcher → WriteChunked RPC.
    let blob_v1 = vec![0xa5_u8; CHUNK_SIZE];
    let digest_v1 = DigestInfo::new(sha256(&blob_v1), CHUNK_SIZE as u64);
    let (mut tx, rx) = make_buf_channel_pair();
    let send_task = tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob_v1)).await);
        drop(tx.send_eof());
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        StoreLike::update(
            &*store,
            StoreKey::from(digest_v1),
            rx,
            UploadSizeInfo::ExactSize(CHUNK_SIZE as u64),
        ),
    )
    .await
    .expect("must not deadlock — V1 dispatch must complete or fail promptly");
    send_task.abort();
    result.expect("V1 write must succeed against the recording server");

    {
        let log = rpc_log.lock().unwrap();
        assert_eq!(
            log.as_slice(),
            &["v1"],
            "#550 Phase 3: flag=false MUST select the V1 dispatcher (WriteChunked RPC), \
             but got {:?}. The if/else branch in update_via_chunked_inner is not gating \
             on the correct flag value.",
            *log
        );
    }

    // Enable V2 flag, write #2. Must select V2 dispatcher → WriteChunkedV2 RPC.
    store.enable_chunked_v2_writes();

    let blob_v2 = vec![0xb3_u8; CHUNK_SIZE];
    let digest_v2 = DigestInfo::new(sha256(&blob_v2), CHUNK_SIZE as u64);
    let (mut tx, rx) = make_buf_channel_pair();
    let send_task = tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob_v2)).await);
        drop(tx.send_eof());
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        StoreLike::update(
            &*store,
            StoreKey::from(digest_v2),
            rx,
            UploadSizeInfo::ExactSize(CHUNK_SIZE as u64),
        ),
    )
    .await
    .expect("must not deadlock — V2 dispatch must complete or fail promptly");
    send_task.abort();
    result.expect("V2 write must succeed against the recording server");

    {
        let log = rpc_log.lock().unwrap();
        assert_eq!(
            log.as_slice(),
            &["v1", "v2"],
            "#550 Phase 3: after enable_chunked_v2_writes(), the V2 dispatcher \
             (WriteChunkedV2 RPC) MUST be selected. Got {:?}. \
             This proves the if/else branch in update_via_chunked_inner gates \
             on the correct flag value.",
            *log
        );
    }

    server_handle.abort();
    Ok(())
}
