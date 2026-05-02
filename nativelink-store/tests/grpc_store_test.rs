use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::proto_stream_utils::WriteRequestStreamWrapper;
use nativelink_util::store_trait::{StoreKey, StoreLike};
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
        chunked_writes_enabled: false,
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

// --- Bug B regression: parallel-chunk read of a tiny blob ---
//
// Forensic context (from `.claude/reviews/wedge-mechanism-mirror-blobs/audit.md`,
// digests f479989b...-183 and ddb73a...-183, 2026-04-25):
//
// The server's CAS read path called `GrpcStore::get_part` with `length:
// Some(10_485_760)` (10 MiB) on a 183-byte blob. Because the caller
// passed `length` rather than letting `get_part` derive it from the
// digest, `effective_length` jumped to 10 MiB, exceeded the 8 MiB
// `parallel_chunk_read_threshold`, and routed into `get_part_parallel`
// with `chunk_count=8`. The math then split the request into 8
// sub-ranges of ~1.25 MiB each:
//
//   chunk 0: offset=0,        length=1310720
//   chunk 1: offset=1310720,  length=1310720
//   ...
//   chunk 7: offset=9175040,  length=1310720
//
// The peer worker (which legitimately had the 183-byte blob on disk)
// served chunk 0 with 183 bytes + clean `Status::OK` trailer, and EOFed
// chunks 1-7 immediately (offset past blob size). Even with the
// post-2fe4b1cb `CleanShort` classifier folding the EOF chunks back
// into `Ok`, the parallel splitter still issues 8 RPCs for a 183-byte
// payload — wasteful, and the wedge wording in production logs
// (`"Tried to send while stream is closed", "while writing parallel
// chunk data", "in GrpcStore::get_part_parallel write"`) shows the
// path remains failure-prone under concurrent load (race-loser
// `JoinHandle::abort()`, h2 RST_STREAM bursts, GOAWAY-stuck channels
// per #147).
//
// The fix: clamp `effective_length` to the actual remaining bytes in
// the blob (`digest.size_bytes() - offset`) before the parallel-vs-
// single-stream gate. This keeps tiny blobs on the single-stream path
// regardless of what `length` the caller passes. As defense in depth,
// `get_part_parallel` itself also clamps `chunk_count` so the splitter
// never produces more chunks than the requested range has bytes.
//
// This regression test wraps an in-process tonic ByteStream worker that
// faithfully serves a 183-byte blob. Pre-fix code path enters
// `get_part_parallel` with chunk_count=8 (verified by the
// `read_request_count` assertion below); post-fix it stays on the
// single-stream path and issues exactly 1 RPC.
//
// Mutation evidence: revert the `effective_length` clamp at
// `grpc_store.rs:get_part` AND the `chunk_count` clamp at
// `grpc_store.rs:get_part_parallel`; this test must FAIL on the
// `read_request_count <= 2` assertion (counting 8 RPCs).
struct TinyBlobByteStream {
    /// 183-byte payload returned for chunk 0 (offset=0).
    payload: Bytes,
    /// Counts the number of read requests received, so the test can
    /// assert that the server actually got fewer requests after the fix.
    read_request_count: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ByteStream for TinyBlobByteStream {
    type ReadStream = futures::stream::BoxStream<
        'static,
        Result<ReadResponse, tonic::Status>,
    >;

    async fn read(
        &self,
        request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        self.read_request_count.fetch_add(1, Ordering::SeqCst);
        let req = request.into_inner();
        let payload = self.payload.clone();
        let blob_size = payload.len() as i64;
        let offset = req.read_offset;
        let read_limit = req.read_limit;

        // Build the response stream. Clean Status::OK trailer in all
        // cases (we do not synthesize errors), modeling a healthy peer
        // that has the blob in full and honors REAPI ByteStream
        // semantics: serve up to `read_limit` bytes starting at
        // `read_offset`, where `read_limit == 0` means "to end of
        // resource."
        let stream: Self::ReadStream = if offset >= blob_size {
            // Past the blob — return immediate clean EOF (no data
            // frames). This is what a real worker does when the peer
            // requests an offset >= the blob size: `Status::OK` with
            // an empty body.
            Box::pin(futures::stream::empty())
        } else {
            // Honor the caller's read_limit (REAPI: 0 == read to end).
            let start = offset as usize;
            let max_end = payload.len();
            let end = if read_limit == 0 {
                max_end
            } else {
                let limited = start + read_limit as usize;
                limited.min(max_end)
            };
            let slice = payload.slice(start..end);
            Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
                data: slice,
            })]))
        };
        Ok(tonic::Response::new(stream))
    }

    async fn write(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("write not used in this test"))
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

/// Bug B regression: `GrpcStore::get_part` for a 183-byte blob with
/// `length: Some(10 MiB)` must NOT shred itself across 8 parallel
/// chunk RPCs. Pre-fix this routed into `get_part_parallel` with
/// `chunk_count=8`, issuing 7 wasted RPCs for offsets past the blob
/// size and creating production wedge fodder ("Tried to send while
/// stream is closed" / "in GrpcStore::get_part_parallel write" under
/// concurrent contention per the audit). Post-fix the request stays
/// on the single-stream path (or, if it does enter parallel, clamps
/// chunk_count to 1) and returns the 183 bytes + clean EOF using ≤2
/// RPCs.
///
/// Mutation step (per CLAUDE.md): comment out the `effective_length`
/// clamp at `grpc_store.rs:get_part` AND the `chunk_count` clamp at
/// `grpc_store.rs:get_part_parallel`; this test MUST FAIL on the
/// `read_request_count <= 2` assertion (counting 8 RPCs). A passing
/// test after mutation means the clamps are not load-bearing.
#[nativelink_test]
async fn grpc_store_tiny_blob_with_oversized_length_does_not_wedge_parallel()
-> Result<(), Error> {
    // 183-byte payload — same size as the production wedge digests
    // f479989b...-183 and ddb73a...-183.
    const PAYLOAD_LEN: usize = 183;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| i as u8).collect();
    let payload = Bytes::from(payload_vec);

    let read_request_count = Arc::new(AtomicU64::new(0));
    let server_impl = TinyBlobByteStream {
        payload: payload.clone(),
        read_request_count: read_request_count.clone(),
    };

    // Bind on a free port and serve the in-process ByteStream.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(server_impl))
            .serve_with_incoming(incoming)
            .await;
    });

    // Construct a GrpcStore mirroring the production config that
    // produced the wedge: parallel_chunk_read_threshold = 8 MiB
    // (default) and parallel_chunk_count = 8. The pre-fix
    // `effective_length = length.unwrap_or(...)` = 10 MiB DOES exceed
    // the 8 MiB threshold → routes into parallel and shreds the
    // 183-byte blob into 8 sub-ranges. The post-fix `effective_length
    // = min(length, blob_remaining) = min(10 MiB, 183) = 183` does
    // NOT exceed 8 MiB → stays on the single-stream path → exactly 1
    // ReadRequest RPC. Zero retries to keep failure modes crisp.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.parallel_chunk_read_threshold = 8 * 1024 * 1024; // production default
    spec.parallel_chunk_count = 8;
    spec.retry = Retry {
        max_retries: 0,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    // Build the digest. The 32-byte zero hash is fine — the in-process
    // server doesn't validate the hash, only honors the offset/limit
    // semantics. Size = 183 (matches the wedge digests).
    let digest = DigestInfo::try_new(
        "0000000000000000000000000000000000000000000000000000000000000000",
        PAYLOAD_LEN as u64,
    )?;
    let key: StoreKey<'_> = digest.into();

    // The wedge shape: caller passes length=Some(10 MiB) for a 183-byte
    // blob. Pre-fix this triggers parallel path with chunk_count=8.
    let oversized_length = 10 * 1024 * 1024_u64;

    let (writer, mut reader) = make_buf_channel_pair();

    let store_clone = store.clone();
    let key_owned: StoreKey<'static> = key.borrow().into_owned();
    let get_part_fut = async move {
        let mut writer_mut = writer;
        store_clone
            .get_part(key_owned, &mut writer_mut, 0, Some(oversized_length))
            .await
        // Mirror the production composition: WorkerProxyStore wraps the
        // peer's get_part with a `tokio::join!` between the get_part
        // future and a forward future reading from the writer's paired
        // reader. If the implementation early-returns Err without
        // terminating the writer, that join deadlocks — exactly the
        // class of bug CLAUDE.md "Test in production composition"
        // warns about. Here the writer drops on function exit.
    };

    let collect_fut = async move {
        let mut total = bytes::BytesMut::new();
        loop {
            // Bounded chunk size: large enough to receive the whole
            // 183-byte payload in one or two recvs.
            let chunk = reader.consume(Some(8192)).await?;
            if chunk.is_empty() {
                break;
            }
            total.extend_from_slice(&chunk);
        }
        Ok::<Bytes, Error>(total.freeze())
    };

    // 5s outer timeout per CLAUDE.md template — the deadlock detector.
    // A `tokio::time::Elapsed` would convert to an error; combined with
    // the specific assertion message it makes a hang surface clearly.
    let outcome = timeout(
        Duration::from_secs(5),
        async move { tokio::join!(get_part_fut, collect_fut) },
    )
    .await
    .expect(
        "must not deadlock — Bug B writer-termination contract violated, \
         or get_part_parallel hangs on shredded chunks",
    );

    server_handle.abort();

    let (get_res, collect_res) = outcome;

    // The receive side's bytes arrive whether or not get_part errors,
    // but we want both: a clean Ok return AND the right bytes received
    // AND no parallel-path error wording in the failure message.
    let received = collect_res.expect(
        "collect must read 183 bytes from the writer cleanly; \
         parallel-path stream-closed wedges produce buf_channel errors here",
    );

    if let Err(err) = get_res {
        // Surface diagnostic: print the production wedge wording so a
        // failing run on pre-fix code points at exactly the audit error.
        let messages = err.messages.join(" / ");
        panic!(
            "Bug B regression — get_part failed with code={:?}, messages: {messages}\n\n\
             Pre-fix wedge wording: \"Tried to send while stream is closed\" / \
             \"in GrpcStore::get_part_parallel write\". The fix clamps \
             effective_length and chunk_count so a 183-byte blob never enters \
             the 8-way parallel path.",
            err.code,
        );
    }

    assert_eq!(
        received.len(),
        PAYLOAD_LEN,
        "expected to receive exactly {PAYLOAD_LEN} bytes from the writer, got {} \
         (this asserts the writer was correctly EOF-terminated and not aborted \
         mid-stream by the parallel-collector failure)",
        received.len(),
    );
    assert_eq!(
        received.as_ref(),
        payload.as_ref(),
        "received bytes must equal the 183-byte payload",
    );

    // Sanity: after the fix, the server should see at most 1 read
    // request (single-stream path), not 8. We accept ≤ 2 to allow for
    // the chunk_count=1 defense-in-depth path inside get_part_parallel.
    let count = read_request_count.load(Ordering::SeqCst);
    assert!(
        count <= 2,
        "expected ≤2 ReadRequest RPCs for a 183-byte blob (single-stream or \
         clamped-to-1 parallel), got {count}. Pre-fix the parallel path \
         issued 8 RPCs."
    );

    Ok(())
}

/// Bug B regression — INNER `chunk_count` clamp isolation.
///
/// Companion test to `grpc_store_tiny_blob_with_oversized_length_does_not_wedge_parallel`
/// (which exercises the OUTER `effective_length` clamp at `get_part`).
///
/// The outer clamp keeps tiny blobs on the single-stream path. But for
/// blobs that legitimately exceed `parallel_chunk_read_threshold`, the
/// parallel path is entered and the INNER `chunk_count` clamp at
/// `get_part_parallel` is the only thing that prevents N concurrent
/// RPCs when the blob has bytes for fewer than N chunks.
///
/// Construction: blob_size = 16 MiB, threshold = 8 MiB, parallel_chunk_count
/// = 32, caller-supplied length = Some(64 MiB). After the outer clamp,
/// effective_length = min(64 MiB, 16 MiB) = 16 MiB ≥ 8 MiB → parallel path
/// IS entered. Inside `get_part_parallel`, max_useful_chunks =
/// ceil(16 MiB / 8 MiB) = 2, so the inner clamp collapses chunk_count
/// from 32 down to 2 — exactly 2 RPCs. Without the inner clamp, the
/// splitter would issue 32 RPCs of ~512 KiB each, every one within the
/// blob (so the data is correct), but 30 of them are pure CAS-fan-out
/// waste and feed the race-loser-abort h2 RST_STREAM contention pattern
/// from #147.
///
/// Mutation evidence (per CLAUDE.md): revert ONLY the `chunk_count` clamp
/// at `grpc_store.rs:get_part_parallel` (replace
/// `let chunk_count = self.parallel_chunk_count.min(max_useful_chunks);`
/// with `let chunk_count = self.parallel_chunk_count;`). Keep the OUTER
/// `effective_length` clamp at `get_part` intact. This test MUST FAIL on
/// the `read_request_count <= 2` assertion (counting 32 RPCs). A passing
/// test after that mutation means the inner clamp is not load-bearing
/// for this composition.
#[nativelink_test]
async fn grpc_store_chunk_count_clamp_collapses_parallel_to_useful_chunks()
-> Result<(), Error> {
    // 16 MiB payload — large enough to enter the parallel path even
    // after the outer clamp folds the caller's oversized length down
    // to the actual blob size.
    const PAYLOAD_LEN: usize = 16 * 1024 * 1024;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| i as u8).collect();
    let payload = Bytes::from(payload_vec);

    let read_request_count = Arc::new(AtomicU64::new(0));
    let server_impl = TinyBlobByteStream {
        payload: payload.clone(),
        read_request_count: read_request_count.clone(),
    };

    // Bind on a free port and serve the in-process ByteStream.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(server_impl))
            .serve_with_incoming(incoming)
            .await;
    });

    // Construct a GrpcStore where the parallel path WILL be entered for
    // this blob (outer clamp does not rescue): threshold = 8 MiB,
    // parallel_chunk_count = 32. The 16 MiB blob fits comfortably above
    // the threshold but only has bytes for 2 chunks of >= 8 MiB; without
    // the inner `chunk_count` clamp the splitter would issue 32 RPCs.
    // Zero retries to keep failure modes crisp.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.parallel_chunk_read_threshold = 8 * 1024 * 1024;
    spec.parallel_chunk_count = 32;
    spec.retry = Retry {
        max_retries: 0,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    // Build the digest. Hash content unimportant — the in-process server
    // doesn't validate. Size = 16 MiB matches the payload.
    let digest = DigestInfo::try_new(
        "0000000000000000000000000000000000000000000000000000000000000000",
        PAYLOAD_LEN as u64,
    )?;
    let key: StoreKey<'_> = digest.into();

    // Caller passes length=Some(64 MiB) — over-large but legal REAPI.
    // After the outer clamp this becomes 16 MiB (= blob_size), still
    // ≥ threshold, so the parallel path IS entered. The INNER clamp is
    // what prevents 32 RPCs from being issued.
    let oversized_length = 64 * 1024 * 1024_u64;

    let (writer, mut reader) = make_buf_channel_pair();

    let store_clone = store.clone();
    let key_owned: StoreKey<'static> = key.borrow().into_owned();
    let get_part_fut = async move {
        let mut writer_mut = writer;
        store_clone
            .get_part(key_owned, &mut writer_mut, 0, Some(oversized_length))
            .await
    };

    let collect_fut = async move {
        let mut total = bytes::BytesMut::new();
        loop {
            // Bounded chunk size: 1 MiB recvs comfortably handle
            // 16 MiB total in 16 iterations.
            let chunk = reader.consume(Some(1024 * 1024)).await?;
            if chunk.is_empty() {
                break;
            }
            total.extend_from_slice(&chunk);
        }
        Ok::<Bytes, Error>(total.freeze())
    };

    // 10s outer timeout — generous for a 16 MiB local-loopback transfer
    // even on a loaded CI runner. The deadlock detector for any
    // writer-termination contract violation in the parallel path.
    let outcome = timeout(
        Duration::from_secs(10),
        async move { tokio::join!(get_part_fut, collect_fut) },
    )
    .await
    .expect(
        "must not deadlock — Bug B writer-termination contract violated, \
         or get_part_parallel hangs on shredded chunks",
    );

    server_handle.abort();

    let (get_res, collect_res) = outcome;

    let received = collect_res.expect(
        "collect must read 16 MiB from the writer cleanly; \
         parallel-path stream-closed wedges produce buf_channel errors here",
    );

    if let Err(err) = get_res {
        let messages = err.messages.join(" / ");
        panic!(
            "Bug B regression — get_part failed with code={:?}, messages: {messages}\n\n\
             Pre-fix wedge wording: \"Tried to send while stream is closed\" / \
             \"in GrpcStore::get_part_parallel write\". The fix clamps \
             chunk_count so a 16 MiB blob never fans out to 32 sub-RPCs.",
            err.code,
        );
    }

    assert_eq!(
        received.len(),
        PAYLOAD_LEN,
        "expected to receive exactly {PAYLOAD_LEN} bytes from the writer, got {} \
         (this asserts the writer was correctly EOF-terminated and not aborted \
         mid-stream by the parallel-collector failure)",
        received.len(),
    );
    assert_eq!(
        received.as_ref(),
        payload.as_ref(),
        "received bytes must equal the 16 MiB payload",
    );

    // The discriminator: with the inner `chunk_count` clamp present,
    // chunk_count = min(32, ceil(16 MiB / 8 MiB)) = min(32, 2) = 2 →
    // exactly 2 RPCs. Without the clamp, the splitter would issue
    // chunk_count = parallel_chunk_count = 32 RPCs. The bytes still
    // arrive in either case (every chunk is within blob bounds), so a
    // weaker assertion would not catch the regression — only the RPC
    // count distinguishes the two states.
    let count = read_request_count.load(Ordering::SeqCst);
    assert!(
        count <= 2,
        "expected ≤2 ReadRequest RPCs for a 16 MiB blob with parallel_chunk_count=32 \
         and threshold=8 MiB (clamped to max_useful_chunks=2), got {count}. \
         Without the inner chunk_count clamp at get_part_parallel the splitter \
         would issue 32 sub-RPCs."
    );

    Ok(())
}

