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

//! #212 Phase 2.4 fixup B2 (perf-optimizer): the chunked-write path
//! must call `evict_pool_on_transport_err` on transport-shaped
//! failures, mirroring the existing #147 fix that the legacy
//! ByteStream `write` / `get_part` paths already apply. Without this,
//! a GOAWAY-shaped error on a chunked-stream attempt leaves the dead
//! channel sitting in the `ConnectionManager` pool; the next request
//! picks it up and immediately re-fails.
//!
//! Test geometry:
//! - Bind an in-process `CasExtensions` server whose `write_chunked`
//!   returns `Code::Unavailable` (transport-shaped — passes
//!   `looks_like_dead_channel`). #212 v4.5: WriteChunked moved off
//!   `WorkerApi` to `CasExtensions` so it routes via the worker's
//!   outbound CAS-endpoint channel.
//! - Build a `GrpcStore` against the in-process server, enable the
//!   chunked-write kill-switch, push a >=CHUNK_SIZE blob.
//! - Capture `tracing` output and assert the
//!   `GrpcStore::evict_pool_on_transport_err entry (#147 trace)` log
//!   line fires with `predicate_matched=true` (the eviction request
//!   went through to `ConnectionManager`).
//!
//! Mutation step (manual): comment out the
//! `if let Err(ref err) = result { self.evict_pool_on_transport_err(err); }`
//! block in `update_via_chunked_inner` and re-run — the test must
//! fail with the SPECIFIC "predicate_matched=true ... did not appear
//! in tracing output" message.

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectWorkerRequest, UpdateForScheduler, UpdateForWorker, WriteChunk,
    WriteChunkedResponse,
    cas_extensions_server::{CasExtensions, CasExtensionsServer},
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreKey, StoreLike, UploadSizeInfo};
use sha2::{Digest as _, Sha256};

/// Fake `CasExtensions` server that always returns `Code::Unavailable`
/// for `write_chunked` — the production GOAWAY-shaped error class that
/// triggers the #147 eviction. #212 v4.5: routed via `CasExtensions`
/// (CAS endpoint) rather than `WorkerApi` (worker_api endpoint).
struct UnavailableOnWriteChunked;

#[tonic::async_trait]
impl CasExtensions for UnavailableOnWriteChunked {
    type WriteChunkedV2Stream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<
                        nativelink_proto::com::github::trace_machina::nativelink::remote_execution::WriteChunkedFrame,
                        tonic::Status,
                    >,
                > + Send
                + 'static,
        >,
    >;

    async fn write_chunked(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteChunk>>,
    ) -> Result<tonic::Response<WriteChunkedResponse>, tonic::Status> {
        // Pull in the unused proto symbols so cargo's dead-code lint
        // does not trip on the imports the test no longer uses for
        // its dispatcher trait.
        let _ = ConnectWorkerRequest::default();
        let _ = std::marker::PhantomData::<UpdateForScheduler>;
        let _ = std::marker::PhantomData::<UpdateForWorker>;
        Err(tonic::Status::unavailable(
            "fake worker: GOAWAY-shaped failure for #147 eviction test",
        ))
    }

    async fn write_chunked_v2(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteChunk>>,
    ) -> Result<tonic::Response<Self::WriteChunkedV2Stream>, tonic::Status> {
        Err(tonic::Status::unavailable(
            "fake worker: GOAWAY-shaped failure (write_chunked_v2 stub for trait completeness)",
        ))
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// `nativelink_test` macro wires `tracing-test` automatically; the
/// `logs_contain` helper is in scope inside `#[nativelink_test]` test
/// bodies.
#[nativelink_test]
async fn chunked_path_evicts_pool_on_transport_err() -> Result<(), Error> {
    // Bind an in-process WorkerApi server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(CasExtensionsServer::new(UnavailableOnWriteChunked))
            .serve_with_incoming(incoming)
            .await;
    });

    // Build a GrpcStore pointing at the in-process server; enable the
    // chunked kill-switch and push a >= CHUNK_SIZE payload so the
    // `update_via_chunked_inner` gate fires.
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

    let blob = vec![0xa5_u8; CHUNK_SIZE];
    let digest = DigestInfo::new(sha256(&blob), CHUNK_SIZE as u64);

    let (mut tx, rx) = make_buf_channel_pair();
    let send_task = tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob)).await);
        drop(tx.send_eof());
    });

    // The store should fail (Unavailable) AND we should observe the
    // eviction trace.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        StoreLike::update(&*store, StoreKey::from(digest), rx, UploadSizeInfo::ExactSize(CHUNK_SIZE as u64)),
    )
    .await
    .expect("must not deadlock — chunked update against an Unavailable server should fail promptly");

    server_handle.abort();
    send_task.abort();

    drop(result.expect_err(
        "fake server returns Unavailable; chunked update must propagate Err",
    ));

    // The #147 eviction trace MUST have fired with predicate_matched=true.
    // The trace is emitted by `evict_pool_on_transport_err` which the B2
    // fixup wires into the chunked path.
    assert!(
        logs_contain(
            "GrpcStore::evict_pool_on_transport_err entry (#147 trace)",
        ),
        "#147 trace did not appear in tracing output — \
         B2 fixup (evict_pool_on_transport_err in chunked path) is missing",
    );
    assert!(
        logs_contain("predicate_matched=true"),
        "evict_pool_on_transport_err did fire but `predicate_matched=true` \
         is missing — the Unavailable error did not classify as dead-channel \
         (test wiring or classifier regression)",
    );
    Ok(())
}
