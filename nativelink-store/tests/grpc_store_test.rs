use core::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    FindMissingBlobsRequest, digest_function,
};
use nativelink_proto::google::bytestream::byte_stream_server::{
    ByteStream, ByteStreamServer,
};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse,
    WriteRequest, WriteResponse,
};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::proto_stream_utils::WriteRequestStreamWrapper;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;

fn make_test_endpoint() -> GrpcEndpoint {
    GrpcEndpoint {
        address: "http://foobar".into(),
        tls_config: None,
        concurrency_limit: None,
        connect_timeout_s: 0,
        tcp_keepalive_s: 0,
        http2_keepalive_interval_s: 0,
        http2_keepalive_timeout_s: 0,
        tcp_nodelay: true,
        use_http3: false,
    }
}

fn make_test_spec() -> GrpcSpec {
    GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![make_test_endpoint()],
        store_type: StoreType::Cas,
        retry: Retry::default(),
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 1,
        batch_update_threshold_bytes: 0,
        max_concurrent_batch_rpcs: 8,
        parallel_chunk_read_threshold: 0,
        parallel_chunk_count: 0,
        dual_transport: false,
        zstd_compression: false,
        connection_acquire_timeout_ms: None,
    }
}

#[nativelink_test]
async fn fast_find_missing_blobs() -> Result<(), Error> {
    let spec = make_test_spec();
    let store = GrpcStore::new(&spec).await?;
    let request = Request::new(FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![],
        digest_function: digest_function::Value::Sha256.into(),
    });
    let res = timeout(Duration::from_secs(1), async move {
        store.find_missing_blobs(request).await
    })
    .await??;
    let inner_res = res.into_inner();
    assert_eq!(inner_res.missing_blob_digests.len(), 0);
    Ok(())
}

/// Verify that GrpcStore can be constructed with zstd_compression enabled.
/// The actual compression negotiation requires a real server, but we verify
/// the store builds without error and that find_missing_blobs still works
/// (the endpoint is fake, so the RPC completes immediately with empty results).
#[nativelink_test]
async fn grpc_store_with_zstd_compression_creates_successfully() -> Result<(), Error> {
    let mut spec = make_test_spec();
    spec.zstd_compression = true;
    let store = GrpcStore::new(&spec).await?;
    // Exercise the client creation path by issuing a find_missing_blobs.
    let request = Request::new(FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![],
        digest_function: digest_function::Value::Sha256.into(),
    });
    let res = timeout(Duration::from_secs(1), async move {
        store.find_missing_blobs(request).await
    })
    .await??;
    assert_eq!(res.into_inner().missing_blob_digests.len(), 0);
    Ok(())
}

/// Verify that zstd_compression=false (default) also works as before.
#[nativelink_test]
async fn grpc_store_without_zstd_compression() -> Result<(), Error> {
    let spec = make_test_spec();
    assert!(!spec.zstd_compression, "default should be false");
    let store = GrpcStore::new(&spec).await?;
    let request = Request::new(FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![],
        digest_function: digest_function::Value::Sha256.into(),
    });
    let res = timeout(Duration::from_secs(1), async move {
        store.find_missing_blobs(request).await
    })
    .await??;
    assert_eq!(res.into_inner().missing_blob_digests.len(), 0);
    Ok(())
}

// --- Per-chunk progress timeout integration test (item 7) ---
//
// End-to-end coverage that the per-chunk no-progress timer in
// `WriteState::with_progress_timeout` actually surfaces as a
// `DeadlineExceeded` from `GrpcStore::write` when the producer stalls
// against a real ByteStream gRPC server (in-process tonic).

/// Minimal `ByteStream` server that accepts `write` requests but never
/// reads any chunk from the inbound stream — the connection stays open,
/// no acknowledgement is ever sent, and `committed_size` is never
/// returned. This forces the client-side per-chunk timer to be the only
/// thing that can terminate the call.
struct StallingByteStream;

#[tonic::async_trait]
impl ByteStream for StallingByteStream {
    type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

    async fn read(
        &self,
        _request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("read not used in this test"))
    }

    async fn write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        // Drain the inbound stream silently — never send a WriteResponse.
        // While the client is still streaming, the server polls but does
        // not commit; the client side sees no progress acknowledgement.
        // When the client tears the stream down (which is what the
        // per-chunk timer in WriteStateWrapper triggers — it ends the
        // outbound stream with `Ready(None)` after setting
        // `read_stream_error`), the inbound stream EOFs here and we
        // return a Cancelled status so the test doesn't wedge waiting
        // for a response. The TEST asserts on the client-side error
        // (DeadlineExceeded with "no progress"), which is the
        // `read_stream_error` propagated by GrpcStore::write — NOT this
        // server-side Cancelled, which is just to unblock the gRPC
        // client's wait for a server response.
        let mut inbound = request.into_inner();
        while let Ok(Some(_chunk)) = inbound.message().await {
            // Discard. The client will time out per-chunk and tear down.
        }
        Err(tonic::Status::cancelled("client aborted stream"))
    }

    async fn query_write_status(
        &self,
        _request: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "query_write_status not used in this test",
        ))
    }
}

/// `GrpcStore::write` against a server that accepts but stalls must
/// return `DeadlineExceeded` carrying the "no progress" wording from the
/// per-chunk timer (set by `WriteStateWrapper::poll_next` and surfaced
/// via `take_read_stream_error`).
///
/// This guards the full path: per-chunk timer fires → wrapper ends
/// stream with `read_stream_error` set → retry loop sees the recorded
/// error → `can_resume()` is false → propagates as `Err`.
#[nativelink_test]
async fn grpc_store_write_returns_deadline_exceeded_when_transport_stalls()
-> Result<(), Error> {
    // Bind on a free port and hand a TcpListenerStream to a tonic Server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(StallingByteStream))
            .serve_with_incoming(incoming)
            .await;
    });

    // Construct a GrpcStore pointing at the in-process server, with a
    // 1-second per-chunk progress timeout and zero retries (the per-chunk
    // DeadlineExceeded is intentionally non-resumable; disable retries
    // to keep the test cheap and the assertion stable).
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 1;
    spec.retry = Retry {
        max_retries: 0,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    // Producer: send a single resource-name-bearing chunk, then go silent.
    // The per-chunk timer is armed on the first chunk's delivery; with
    // no follow-up chunk and a 1s progress budget, the timer must fire
    // well within the 5s outer test timeout.
    let (tx, rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    let resource_name = format!(
        "/uploads/test-uuid/blobs/{}/{}",
        "0".repeat(64),
        128, // claim 128 bytes total so the wrapper waits for more after the 4-byte first chunk
    );
    tx.send(Ok(WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"data"),
    }))
    .unwrap();
    // Hold tx alive in a background task so the channel never closes —
    // EOF would let the wrapper drain cleanly without exercising the
    // stuck-transport code path.
    let tx_holder = tokio::spawn(async move {
        let _keep = tx;
        std::future::pending::<()>().await;
    });

    let stream = WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    let result = timeout(Duration::from_secs(5), store.write(stream)).await;

    server_handle.abort();
    tx_holder.abort();

    let inner = result.expect("test outer timeout — per-chunk timer didn't fire in 5s");
    let err = inner.expect_err("expected DeadlineExceeded, got success");
    assert_eq!(
        err.code,
        Code::DeadlineExceeded,
        "expected DeadlineExceeded, got {err:?}",
    );
    assert!(
        err.messages.iter().any(|m| m.contains("no progress")),
        "expected 'no progress' wording, got: {:?}",
        err.messages,
    );
    Ok(())
}

