use core::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, GetActionResultRequest, GetTreeRequest,
    GetTreeResponse, UpdateActionResultRequest, batch_update_blobs_response,
    digest_function,
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
use nativelink_util::store_trait::{IS_WORKER_REQUEST, StoreKey, StoreLike};
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
        chunked_v2_writes_enabled: false,
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

// --- Per-chunk progress diagnostic integration test ---
//
// End-to-end coverage that the per-chunk no-progress timer in
// `WriteState::with_progress_timeout` is **diagnostic-only** since
// 2026-05-14: it emits `warn!` + bumps
// `GRPC_WRITE_SLOW_CHUNK_TOTAL` when the producer stalls against a
// real ByteStream gRPC server (in-process tonic), but does NOT abort
// the call with `DeadlineExceeded`. The previous abort-on-elapse
// behaviour was masking real bugs (chunked-write retry-rejection
// cascade #476, ci-mac-2 Tailscale slowness misattributed to server
// WRITE_TIMEOUT) — see
// `.claude/audits/timeout-removal-2026-05-14/`.

/// Minimal `ByteStream` server that accepts `write` requests but never
/// reads any chunk from the inbound stream — the connection stays open,
/// no acknowledgement is ever sent, and `committed_size` is never
/// returned. The client-side timer must observe the stall but NOT kill
/// the call (the test outer `timeout` confirms the wrapper hangs
/// instead of returning DeadlineExceeded).
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
        // The client will keep streaming forever (diagnostic-only timer
        // does not tear down). The test aborts the server task to
        // unblock cleanup.
        let mut inbound = request.into_inner();
        while let Ok(Some(_chunk)) = inbound.message().await {
            // Discard.
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
/// (a) not return any error within the test budget — the diagnostic
/// timer does NOT abort — and (b) bump
/// `GRPC_WRITE_SLOW_CHUNK_TOTAL` plus emit the diagnostic `warn!`
/// line so SREs see the slow chunk.
///
/// This is the integration counterpart to
/// `write_state_progress_timer_warns_but_does_not_abort_on_threshold`
/// in `nativelink-util/tests/proto_stream_utils_test.rs`.
///
/// **Mutation step:** restoring the abort-on-elapse behaviour
/// (`local_state.read_stream_error = Some(make_err!(...));
/// Poll::Ready(None)`) flips the `timeout(...).await` result from
/// `Err(Elapsed)` to `Ok(Err(DeadlineExceeded))` and red-fails the
/// `.expect_err("...")` line below with the bespoke message.
#[nativelink_test]
async fn grpc_store_write_diagnostic_timer_does_not_abort_when_transport_stalls()
-> Result<(), Error> {
    use nativelink_util::proto_stream_utils::GRPC_WRITE_SLOW_CHUNK_TOTAL;

    let baseline = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);

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
    // 1-second per-chunk progress *diagnostic* and zero retries.
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
    // The per-chunk diagnostic timer arms on the first chunk's delivery;
    // with no follow-up and a 1s budget, the diagnostic must fire several
    // times within the 4s outer test timeout. The wrapper must NOT
    // terminate.
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

    // 4 s budget × 1 s diagnostic window = expect ≥3 increments.
    let result = timeout(Duration::from_secs(4), store.write(stream)).await;

    server_handle.abort();
    tx_holder.abort();

    // The OUTER timeout MUST fire — that's how we know the diagnostic
    // timer did NOT abort the in-flight RPC. Mutation: restoring the
    // pre-2026-05-14 abort-on-elapse converts `result` from
    // `Err(Elapsed)` into `Ok(Err(DeadlineExceeded))` and this
    // `.expect_err(...)` panics with the bespoke message.
    let _elapsed = result.expect_err(
        "WriteState progress timer must NOT abort the stream — \
         diagnostic-only per 2026-05-14",
    );

    // Diagnostic counter must have advanced: with a 1s budget and ≥4s
    // of silence we expect ≥3 increments (one per window boundary
    // crossed). Use a generous lower bound to absorb scheduler jitter.
    let after = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);
    let delta = after.saturating_sub(baseline);
    assert!(
        delta >= 1,
        "expected ≥1 diagnostic counter increment, got {delta} \
         (baseline={baseline}, after={after})",
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

// ===================================================================
// #168 code-reviewer S1 regression: IS_WORKER_REQUEST → x-nativelink-worker
// header propagation across batch_update_blobs / write / update_action_result.
// ===================================================================
//
// Production CAS chain: WorkerProxyStore → ... → GrpcStore. When a
// worker uploads to the server (e.g., action result outputs via
// bytestream, batch CAS update, or AC update_action_result), the
// `IS_WORKER_REQUEST` task-local is set to `true` somewhere on the
// stack. Without propagation into the GrpcStore RPC metadata, the
// server's CAS dispatcher hook treats the upload as a Bazel-originated
// write and fans the bytes back out to all workers (including the
// originator), wasting RTTs on a redundant push.
//
// These tests bind in-process tonic CAS / AC servers that capture the
// inbound `x-nativelink-worker` header value, then call the GrpcStore
// method under `IS_WORKER_REQUEST.scope(true, ...)` and assert the
// header arrived. Mutation step: remove the header injection at
// `nativelink-store/src/grpc_store.rs:batch_update_blobs / write /
// update_action_result` → the assertion below MUST red-fail.

/// CAS server fixture that records the `x-nativelink-worker` header
/// value of every received `BatchUpdateBlobs` request.
struct HeaderCapturingCasServer {
    last_x_nativelink_worker: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl ContentAddressableStorage for HeaderCapturingCasServer {
    async fn find_missing_blobs(
        &self,
        _request: tonic::Request<FindMissingBlobsRequest>,
    ) -> Result<
        tonic::Response<
            nativelink_proto::build::bazel::remote::execution::v2::FindMissingBlobsResponse,
        >,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "find_missing_blobs not used in this header test",
        ))
    }

    async fn batch_update_blobs(
        &self,
        request: tonic::Request<BatchUpdateBlobsRequest>,
    ) -> Result<tonic::Response<BatchUpdateBlobsResponse>, tonic::Status> {
        let header_value = request
            .metadata()
            .get("x-nativelink-worker")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
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

    type GetTreeStream =
        futures::stream::Empty<Result<GetTreeResponse, tonic::Status>>;

    async fn get_tree(
        &self,
        _request: tonic::Request<GetTreeRequest>,
    ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_tree not used"))
    }
}

/// AC server fixture that records the `x-nativelink-worker` header on
/// every received `update_action_result` request.
struct HeaderCapturingAcServer {
    last_x_nativelink_worker: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl ActionCache for HeaderCapturingAcServer {
    async fn get_action_result(
        &self,
        _request: tonic::Request<GetActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_action_result not used"))
    }

    async fn update_action_result(
        &self,
        request: tonic::Request<UpdateActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
        let header_value = request
            .metadata()
            .get("x-nativelink-worker")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        *self.last_x_nativelink_worker.lock().unwrap() = header_value;
        Ok(tonic::Response::new(ActionResult::default()))
    }
}

/// ByteStream server fixture that records the `x-nativelink-worker`
/// header on every received `write` request.
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
            .map(|s| s.to_string());
        *self.last_x_nativelink_worker.lock().unwrap() = header_value;
        // Drain the inbound stream and ack with the total bytes
        // received so the GrpcStore retry loop sees a clean Ok.
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

#[nativelink_test]
async fn t_grpc_store_propagates_is_worker_header_on_batch_update_blobs()
-> Result<(), Error> {
    let last_header: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let server_fixture = HeaderCapturingCasServer {
        last_x_nativelink_worker: last_header.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ContentAddressableStorageServer::new(server_fixture))
            .serve_with_incoming(incoming)
            .await;
    });

    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.retry = Retry { max_retries: 0, ..Default::default() };
    let store = GrpcStore::new(&spec).await?;

    let digest = DigestInfo::new([0xAA; 32], 4);
    let req = BatchUpdateBlobsRequest {
        instance_name: String::new(),
        requests: vec![
            nativelink_proto::build::bazel::remote::execution::v2::batch_update_blobs_request::Request {
                digest: Some(digest.into()),
                data: Bytes::from_static(b"AAAA"),
                compressor: 0,
            },
        ],
        digest_function: digest_function::Value::Sha256.into(),
    };

    // Drive the upload under IS_WORKER_REQUEST.scope(true, ...).
    let _resp = IS_WORKER_REQUEST
        .scope(true, async {
            store.batch_update_blobs(Request::new(req)).await
        })
        .await?;

    server_handle.abort();

    let captured = last_header.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "#168 code-reviewer S1: GrpcStore::batch_update_blobs MUST propagate \
         IS_WORKER_REQUEST=true into the `x-nativelink-worker` request \
         metadata so the server's CAS dispatcher skips fan-out on \
         worker-originated batch uploads"
    );
    Ok(())
}

#[nativelink_test]
async fn t_grpc_store_propagates_is_worker_header_on_update_action_result()
-> Result<(), Error> {
    let last_header: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let server_fixture = HeaderCapturingAcServer {
        last_x_nativelink_worker: last_header.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ActionCacheServer::new(server_fixture))
            .serve_with_incoming(incoming)
            .await;
    });

    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.store_type = StoreType::Ac;
    spec.retry = Retry { max_retries: 0, ..Default::default() };
    let store = GrpcStore::new(&spec).await?;

    let digest = DigestInfo::new([0xBB; 32], 8);
    let req = UpdateActionResultRequest {
        instance_name: String::new(),
        action_digest: Some(digest.into()),
        action_result: Some(ActionResult::default()),
        results_cache_policy: None,
        digest_function: digest_function::Value::Sha256.into(),
        // (#12 H4) Not populated in this test — phase 1 only.
        cas_endpoint: String::new(),
    };

    let _resp = IS_WORKER_REQUEST
        .scope(true, async {
            store.update_action_result(Request::new(req)).await
        })
        .await?;

    server_handle.abort();

    let captured = last_header.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "#168 code-reviewer S1: GrpcStore::update_action_result MUST propagate \
         IS_WORKER_REQUEST=true into the `x-nativelink-worker` request \
         metadata so the server's AC dispatcher skips fan-out on \
         worker-originated AC updates"
    );
    Ok(())
}

#[nativelink_test]
async fn t_grpc_store_propagates_is_worker_header_on_write()
-> Result<(), Error> {
    let last_header: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let server_fixture = HeaderCapturingByteStream {
        last_x_nativelink_worker: last_header.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(server_fixture))
            .serve_with_incoming(incoming)
            .await;
    });

    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.retry = Retry { max_retries: 0, ..Default::default() };
    let store = GrpcStore::new(&spec).await?;

    // Producer: send one chunk with finish_write=true.
    let (tx, rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    let resource_name = format!(
        "/uploads/test-uuid/blobs/{}/{}",
        "0".repeat(64),
        4,
    );
    tx.send(Ok(WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: true,
        data: Bytes::from_static(b"data"),
    }))
    .unwrap();
    drop(tx);

    let stream = WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx))
        .await
        .unwrap();

    let _ = IS_WORKER_REQUEST
        .scope(true, async { store.write(stream).await })
        .await?;

    server_handle.abort();

    let captured = last_header.lock().unwrap().clone();
    assert_eq!(
        captured.as_deref(),
        Some("1"),
        "#168 code-reviewer S1: GrpcStore::write MUST propagate \
         IS_WORKER_REQUEST=true into the `x-nativelink-worker` request \
         metadata so the server's bytestream dispatcher skips fan-out \
         on worker-originated bytestream uploads"
    );
    Ok(())
}

// --- #550 Phase 3: chunked_v2_writes_enabled flag tests ---
// Gated on `chunked_fast_slow` because the flag is behind the same cfg
// on `GrpcStore::chunked_v2_writes_enabled` and the accessor methods.

// `make_test_spec()` leaves `chunked_v2_writes_enabled` at its default
// (`false`); this test does NOT re-set it, so it verifies that the spec
// default flows through `GrpcStore::new()` into the `AtomicBool` rather
// than re-asserting a value the test itself wrote. The serde-absent
// default is covered separately by the `nativelink-config` crate test.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn chunked_v2_writes_enabled_defaults_false() -> Result<(), Error> {
    let spec = make_test_spec();
    let store = GrpcStore::new(&spec).await?;
    assert!(
        !store.chunked_v2_writes_enabled(),
        "#550 Phase 3: chunked_v2_writes_enabled MUST default to false \
         — pre-change behavior is V1 dispatcher"
    );
    Ok(())
}

/// Verify that constructing GrpcStore with `chunked_v2_writes_enabled=true`
/// in the config actually enables the V2 dispatcher flag.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn chunked_v2_writes_enabled_config_flag_is_honored() -> Result<(), Error> {
    let mut spec = make_test_spec();
    spec.chunked_v2_writes_enabled = true;
    let store = GrpcStore::new(&spec).await?;
    assert!(
        store.chunked_v2_writes_enabled(),
        "#550 Phase 3: chunked_v2_writes_enabled config flag MUST be \
         honored at construction — V2 dispatcher should be active"
    );
    Ok(())
}

/// Verify the runtime enable/disable API works correctly.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn chunked_v2_writes_enabled_runtime_toggle() -> Result<(), Error> {
    let spec = make_test_spec();
    let store = GrpcStore::new(&spec).await?;
    // Default: false
    assert!(
        !store.chunked_v2_writes_enabled(),
        "Must start disabled"
    );
    // Enable
    store.enable_chunked_v2_writes();
    assert!(
        store.chunked_v2_writes_enabled(),
        "Must be true after enable"
    );
    // Disable
    store.disable_chunked_v2_writes();
    assert!(
        !store.chunked_v2_writes_enabled(),
        "Must be false after disable"
    );
    // Enable again (idempotent)
    store.enable_chunked_v2_writes();
    assert!(
        store.chunked_v2_writes_enabled(),
        "Must be true after re-enable"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// #2 Fix-C: latched-pool abort tests.
//
// A tower-Buffer whose internal worker panicked or is dropped returns
// `Code::Unknown "Service was not ready: buffered service failed: …"` for
// every subsequent RPC almost instantaneously (the latch is in-process —
// the buffer worker returns without hitting the network). If the retrier
// keeps retrying a latched pool with a long backoff schedule it burns the
// full retry budget (~28s at 1+2+3+7+14s) before `run_producer` sets a
// streaming-blob terminal, leaving readers to time out at 30s instead of
// falling back within seconds.
//
// Fix-C adds a `consecutive_latched_fails` counter in the retry state.
// After `LATCHED_POOL_ABORT_THRESHOLD` consecutive instant-fail
// "buffered service" errors the retrier returns `RetryResult::Err`
// immediately — `run_producer` sets terminal, readers fall back.
//
// **Test strategy:** stand up an in-process gRPC server that returns the
// latched-pool message from every `read()` call without delay. Set the
// GrpcStore retry config to a high max_retries and a significant delay
// (0.5 s × max_retries=5 = ~16 s full budget). Fix-C should abort after 2
// attempts. Assert (a) get_part returns an error and (b) the call
// completes within a tight time budget (3 s) — far faster than the full
// retry schedule.
//
// **Mutation step:** comment out the
// `if local_state.consecutive_latched_fails >= LATCHED_POOL_ABORT_THRESHOLD`
// early-return block in `get_part_single_stream`. The outer
// `tokio::time::timeout(3s, …)` fires and the test panics with the bespoke
// "Fix-C abort contract violated — get_part burned the full retry budget
// on a latched pool" message.
// ---------------------------------------------------------------------------

/// In-process ByteStream server that returns the latched-pool error
/// signature for every `read()` request. Simulates a worker whose h2
/// channel's tower-Buffer has latched.
struct LatchedPoolByteStream;

#[tonic::async_trait]
impl ByteStream for LatchedPoolByteStream {
    type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

    async fn read(
        &self,
        _request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        // tower-Buffer ServiceError message — the exact text that
        // `looks_like_latched_pool` matches on.
        Err(tonic::Status::unknown(
            "Service was not ready: buffered service failed: timed out",
        ))
    }

    async fn write(
        &self,
        _request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in Fix-C test"))
    }

    async fn query_write_status(
        &self,
        _request: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used in Fix-C test"))
    }
}

/// #2 Fix-C under-action test: `get_part` against a latched-pool server
/// (all `read()` RPCs return the buffered-service error message instantly)
/// must abort after `LATCHED_POOL_ABORT_THRESHOLD` consecutive instant
/// failures and return an error — NOT burn the full retry budget.
///
/// The test uses the `latched_pool_bypass_timing` failpoint to bypass the
/// `elapsed_ms < LATCHED_POOL_INSTANT_FAIL_MS` timing gate so the test
/// doesn't flake when loopback RTT spikes above 50ms under scheduler load.
///
/// The test sets `delay: 0.5, max_retries: 5` (full budget ~16 s via
/// exponential backoff). Without Fix-C the test times out at 3 s; with
/// Fix-C it completes in < 2 s (2 roundtrips + 1 retry delay).
///
/// **Mutation step:** comment out the consecutive-latched-fails early-return
/// block (`if local_state.consecutive_latched_fails >= …`) in
/// `get_part_single_stream`. The outer `timeout(3 s, …)` fires and the test
/// panics with "Fix-C abort contract violated — get_part burned the full
/// retry budget on a latched pool instead of aborting early".
#[cfg(feature = "failpoints")]
#[serial_test::serial(failpoints)]
#[nativelink_test]
async fn fix_c_latched_pool_abort_returns_error_fast() -> Result<(), Error> {
    // RAII guard: disarms the failpoint even if the test panics, so the
    // serial(failpoints) group is not left with a poisoned failpoint state.
    struct FailpointGuard;
    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            let _ = fail::cfg("latched_pool_bypass_timing", "off");
        }
    }
    // Bypass the timing gate — prevents flakes when loopback RTT > 50ms.
    fail::cfg("latched_pool_bypass_timing", "return").unwrap();
    let _fp_guard = FailpointGuard;

    // Bind on a free port and start the in-process latched-pool server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(LatchedPoolByteStream))
            .serve_with_incoming(incoming)
            .await;
    });

    // GrpcStore with a 5-retry schedule and 0.5 s base delay.
    // Full budget without Fix-C: 0.5 + 1 + 2 + 4 + 8 ≈ 16 s.
    // With Fix-C: 2 latched roundtrips + ~1 retry delay ≈ < 2 s.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0; // no per-RPC deadline — let Fix-C fire
    spec.retry = Retry {
        max_retries: 5,
        delay: 0.5,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    let digest = DigestInfo::try_new(&"0".repeat(64), 4096).unwrap();
    let (mut tx, rx) = make_buf_channel_pair();

    // Run get_part with a 3 s outer timeout.  Without Fix-C this deadline
    // fires and the test panics.  With Fix-C the call returns an error
    // (from the retrier) long before the deadline.
    let get_result = tokio::time::timeout(
        Duration::from_secs(3),
        store.get_part(StoreKey::Digest(digest), &mut tx, 0, None),
    )
    .await
    .expect(
        "Fix-C abort contract violated — get_part burned the full retry budget \
         on a latched pool instead of aborting early; consecutive_latched_fails \
         >= LATCHED_POOL_ABORT_THRESHOLD must trigger RetryResult::Err (#2 Fix-C)",
    );

    server_handle.abort();

    // get_part must return Err, not Ok — the pool is permanently latched.
    // Explicitly match instead of is_err() to capture the Ok value for
    // diagnostics if Fix-C ever stops firing.
    match get_result {
        Err(_) => {} // expected
        Ok(()) => panic!(
            "Fix-C: get_part must return Err when pool is latched, got Ok(()) — \
             the early-abort path in get_part_single_stream is not firing; \
             check consecutive_latched_fails increment and LATCHED_POOL_ABORT_THRESHOLD"
        ),
    }
    // Drop rx after assertion: keep the read half alive until we are done
    // inspecting the write-half's result.
    drop(rx);

    Ok(())
}

/// #2 Fix-C over-action test: `get_part` against a server whose `read()`
/// returns `Code::Unknown` but with a DIFFERENT message (not "buffered
/// service") must NOT abort early via Fix-C. With zero retries the test
/// completes immediately (no backoff to burn); the key assertion is that
/// the call returns without hanging — the Fix-C abort path must NOT fire
/// for non-latched Unknown errors.
///
/// Documents the over-action direction: `looks_like_latched_pool` must
/// NOT match on `Code::Unknown` alone — the "buffered service" substring
/// is the discriminator.
#[nativelink_test]
async fn fix_c_non_latched_unknown_does_not_misclassify() -> Result<(), Error> {
    struct TransportUnknownByteStream;

    #[tonic::async_trait]
    impl ByteStream for TransportUnknownByteStream {
        type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

        async fn read(
            &self,
            _request: tonic::Request<ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            Err(tonic::Status::unknown("transport error: connection reset"))
        }

        async fn write(
            &self,
            _request: tonic::Request<tonic::Streaming<WriteRequest>>,
        ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn query_write_status(
            &self,
            _request: tonic::Request<QueryWriteStatusRequest>,
        ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(TransportUnknownByteStream))
            .serve_with_incoming(incoming)
            .await;
    });

    // Zero retries so the test completes immediately — we are not testing
    // retry exhaustion speed, just that a non-buffered-service Unknown
    // error does not trigger the Fix-C abort counter.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.retry = Retry {
        max_retries: 0,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    let digest = DigestInfo::try_new(&"0".repeat(64), 4096).unwrap();
    let (mut tx, rx) = make_buf_channel_pair();

    let get_result = tokio::time::timeout(
        Duration::from_secs(3),
        store.get_part(StoreKey::Digest(digest), &mut tx, 0, None),
    )
    .await
    .expect("non-latched Unknown must complete within 3 s (0 retries) — must not hang");
    drop(rx);

    assert!(
        get_result.is_err(),
        "non-latched-pool get_part must return Err when server always errors"
    );

    server_handle.abort();
    Ok(())
}

// ---------------------------------------------------------------------------
// #7 Fix-C extension: latched-pool abort for `get_part_parallel` (≥8 MiB path)
//
// The existing `fix_c_latched_pool_abort_returns_error_fast` test only
// exercises `get_part_single_stream` (blobs below `parallel_chunk_read_threshold`).
// The assumption-auditor (claim #7) confirmed that `get_part_parallel` had
// NO `looks_like_latched_pool` logic — large blobs burned the full 30s notify
// timeout on a latched pool.
//
// Fix-C is extended to the parallel path via a per-request shared
// `Arc<AtomicU32>` (`parallel_latched_fails`) that spans all chunk retriers
// for a single `get_part_parallel` invocation. When any chunk's attempt hits
// LATCHED_POOL_ABORT_THRESHOLD, it returns `RetryResult::Err` — which
// propagates out through `try_for_each` short-circuiting all sibling fetches
// and terminating the parallel read fast.
//
// Counter scope is per-request (NOT per-chunk) because the pool is shared:
// N chunks instant-failing simultaneously is ONE latched-pool signal.  Per-chunk
// counters would require N×THRESHOLD attempts before aborting, delaying abort
// on a fully-latched pool.
//
// **Mutation step (under-action):** comment out the
// `if latched_fails >= LATCHED_POOL_ABORT_THRESHOLD` block in
// `get_part_parallel`. The outer `timeout(3 s, …)` fires and the test panics
// with "Fix-C parallel under-action violated — get_part_parallel burned the
// full retry budget on a latched pool instead of aborting early".
//
// **Over-action test:** healthy parallel reads complete without abort; a
// non-buffered-service Unknown error does not trigger the abort counter.
// ---------------------------------------------------------------------------

/// #7 Fix-C under-action: `get_part` on a blob ≥ `parallel_chunk_read_threshold`
/// against a latched-pool server must abort after `LATCHED_POOL_ABORT_THRESHOLD`
/// consecutive instant failures and return error within 3 s — NOT burn the full
/// retry budget (~16 s at 5 retries × 0.5 s base).
///
/// Routing: blob_size = 16 MiB, threshold = 1 byte → parallel path IS entered
/// regardless of actual size, guaranteeing the test exercises `get_part_parallel`
/// rather than `get_part_single_stream`.
///
/// **Mutation step:** comment out the latched-fails threshold abort in
/// `get_part_parallel`. The outer `timeout(3 s)` fires and panics with the
/// bespoke "Fix-C parallel under-action violated" message.
#[cfg(feature = "failpoints")]
#[serial_test::serial(failpoints)]
#[nativelink_test]
async fn fix_c_latched_pool_abort_parallel_path_returns_error_fast() -> Result<(), Error> {
    struct FailpointGuard;
    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            let _ = fail::cfg("latched_pool_bypass_timing", "off");
        }
    }
    // Bypass the timing gate so the test does not depend on loopback RTT.
    fail::cfg("latched_pool_bypass_timing", "return").unwrap();
    let _fp_guard = FailpointGuard;

    // Counting variant of LatchedPoolByteStream: one increment per `read()` RPC.
    // After abort, `server_rpc_count <= LATCHED_POOL_ABORT_THRESHOLD` proves the
    // shared counter fires on 2 TOTAL latched attempts (not 2 per-chunk), binding
    // the "one latched signal per request" contract (#7 testing-czar NIT).
    let server_rpc_count = Arc::new(AtomicU32::new(0));
    let counter_for_server = server_rpc_count.clone();

    struct CountingLatchedByteStream {
        counter: Arc<AtomicU32>,
    }
    #[tonic::async_trait]
    impl ByteStream for CountingLatchedByteStream {
        type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;
        async fn read(
            &self,
            _request: tonic::Request<ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Err(tonic::Status::unknown(
                "Service was not ready: buffered service failed: timed out",
            ))
        }
        async fn write(
            &self,
            _request: tonic::Request<tonic::Streaming<WriteRequest>>,
        ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }
        async fn query_write_status(
            &self,
            _request: tonic::Request<QueryWriteStatusRequest>,
        ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(CountingLatchedByteStream {
                counter: counter_for_server,
            }))
            .serve_with_incoming(incoming)
            .await;
    });

    // Route into get_part_parallel: threshold = 1 byte so any non-empty blob
    // takes the parallel path.  parallel_chunk_count = 4; full budget without
    // Fix-C: 0.5 + 1 + 2 + 4 + 8 ≈ 16 s.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.parallel_chunk_read_threshold = 1; // route everything into parallel
    spec.parallel_chunk_count = 4;
    spec.retry = Retry {
        max_retries: 5,
        delay: 0.5,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    // 16 MiB blob so parallel_chunk_read_threshold=1 routes it to the parallel path.
    const BLOB_SIZE: u64 = 16 * 1024 * 1024;
    let digest = DigestInfo::try_new(&"1".repeat(64), BLOB_SIZE).unwrap();
    let (mut tx, rx) = make_buf_channel_pair();

    let get_result = tokio::time::timeout(
        Duration::from_secs(3),
        store.get_part(StoreKey::Digest(digest), &mut tx, 0, None),
    )
    .await
    .expect(
        "Fix-C parallel under-action violated — get_part_parallel burned the full \
         retry budget on a latched pool instead of aborting early; the shared \
         parallel_latched_fails counter must reach LATCHED_POOL_ABORT_THRESHOLD \
         and return RetryResult::Err (#7 Fix-C extension for get_part_parallel)",
    );

    server_handle.abort();

    match get_result {
        Err(_) => {} // expected: parallel path aborted on latched pool
        Ok(()) => panic!(
            "Fix-C parallel: get_part must return Err when pool is latched, got Ok(()) — \
             the parallel latched-pool abort path is not firing; \
             check parallel_latched_fails increment and LATCHED_POOL_ABORT_THRESHOLD \
             in get_part_parallel (#7)"
        ),
    }

    // Per-request shared counter: abort fires after LATCHED_POOL_ABORT_THRESHOLD (=2)
    // TOTAL increments across ALL chunks, not 2 per-chunk.  On a fully-latched pool
    // with 4 chunks, per-chunk counters would allow up to 4×2=8 attempts; the shared
    // counter must fire within at most LATCHED_POOL_ABORT_THRESHOLD RPCs per request.
    // We allow up to parallel_chunk_count (=4) because chunks dispatch concurrently
    // and two chunks may both hit attempt 1 before the abort propagates.
    let rpc_count = server_rpc_count.load(Ordering::Relaxed);
    assert!(
        rpc_count <= 4, // parallel_chunk_count — not max_retries × chunk_count (=20)
        "Fix-C parallel: shared counter contract violated — {rpc_count} RPCs fired \
         at abort but expected ≤ parallel_chunk_count (4); per-chunk counters would \
         allow up to max_retries×chunk_count RPCs before aborting, far more than the \
         shared counter's LATCHED_POOL_ABORT_THRESHOLD (#7 testing-czar NIT)"
    );

    drop(rx);
    Ok(())
}

/// #7 Fix-C over-action: a healthy parallel read against a server that
/// actually serves data must complete without spurious abort.
///
/// Uses a tiny payload served over the parallel path (threshold=1 byte) to
/// confirm the Fix-C abort counter does NOT fire for successful reads.
#[nativelink_test]
async fn fix_c_parallel_healthy_read_completes_without_abort() -> Result<(), Error> {
    // A tiny ByteStream server that returns real data.
    struct SmallPayloadByteStream;
    const SMALL_PAYLOAD: &[u8] = b"hello from parallel path";

    #[tonic::async_trait]
    impl ByteStream for SmallPayloadByteStream {
        type ReadStream = futures::stream::Once<
            futures::future::Ready<Result<ReadResponse, tonic::Status>>,
        >;

        async fn read(
            &self,
            _request: tonic::Request<ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            Ok(tonic::Response::new(futures::stream::once(futures::future::ready(Ok(
                ReadResponse {
                    data: bytes::Bytes::from_static(SMALL_PAYLOAD),
                },
            )))))
        }

        async fn write(
            &self,
            _request: tonic::Request<tonic::Streaming<WriteRequest>>,
        ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn query_write_status(
            &self,
            _request: tonic::Request<QueryWriteStatusRequest>,
        ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(SmallPayloadByteStream))
            .serve_with_incoming(incoming)
            .await;
    });

    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.parallel_chunk_read_threshold = 1; // route into parallel
    spec.parallel_chunk_count = 2;
    spec.retry = Retry {
        max_retries: 0,
        delay: 0.0,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    let digest = DigestInfo::try_new(
        &"2".repeat(64),
        SMALL_PAYLOAD.len() as u64,
    )
    .unwrap();
    let (writer, mut reader) = make_buf_channel_pair();

    let store_clone = store.clone();
    let key_owned: StoreKey<'static> = StoreKey::Digest(digest);
    let get_part_fut = async move {
        let mut writer_mut = writer;
        store_clone.get_part(key_owned, &mut writer_mut, 0, None).await
    };
    let collect_fut = async move {
        let mut total = bytes::BytesMut::new();
        loop {
            let chunk = reader.consume(Some(4096)).await?;
            if chunk.is_empty() {
                break;
            }
            total.extend_from_slice(&chunk);
        }
        Ok::<bytes::Bytes, Error>(total.freeze())
    };

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        async move { tokio::join!(get_part_fut, collect_fut) },
    )
    .await
    .expect("healthy parallel read must complete within 5 s — must not abort spuriously");

    server_handle.abort();

    let (get_res, collect_res) = outcome;
    // Assert get_part itself returned Ok — a Fix-C spurious abort would return
    // Err here even if some bytes had already flowed, catching partial-abort
    // scenarios that a collect_res-only assertion would miss (#7 distsys m3).
    //
    // NOTE: `collect_res` may contain more bytes than SMALL_PAYLOAD because the
    // mock serves the full payload for EVERY chunk (ignoring range params), and
    // parallel_chunk_count=2 issues 2 read RPCs.  We assert the payload is
    // non-empty and starts with SMALL_PAYLOAD rather than checking exact equality.
    get_res.expect(
        "Fix-C over-action violated — healthy parallel get_part returned Err; \
         the Fix-C abort counter must NOT fire on successful reads (#7)"
    );
    let received = collect_res.expect(
        "Fix-C over-action violated — healthy parallel read failed to deliver data; \
         the Fix-C abort counter must NOT fire on successful reads (#7)"
    );
    assert!(
        !received.is_empty() && received.starts_with(SMALL_PAYLOAD),
        "Fix-C over-action: data mismatch — parallel read returned unexpected bytes; \
         expected payload starting with SMALL_PAYLOAD, got {received:?} (#7 distsys m3)"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// #8 Fix-C extension: latched-pool abort for `GrpcStore::write`
//
// `GrpcStore::write` is the incident's TRIGGER path (write stalls created the
// pool latch) and also a victim: workers uploading action outputs against a
// latched pool burned the full ~28s retry budget (backoff 0.5+1+2+4+8+16s).
//
// Fix-C is extended to the write path via a `consecutive_latched_fails: u32`
// counter captured in the unfold closure's captured environment, mirroring
// `get_part_single_stream`'s `LocalState::consecutive_latched_fails`.
//
// Timing for write: the tower-Buffer ServiceError fires synchronously when
// tonic calls `poll_ready` on the acquired channel, AFTER channel acquisition.
// A post-connection Instant (`rpc_start`) is recorded inside `rpc_fut` after
// channel acquisition; the elapsed at RPC error is used as the timing gate.
//
// **Durability analysis:** fast-aborting a write on a latched pool does NOT
// widen the durability hole. The latched pool means every attempt returns
// instantly with ServiceError — no bytes are sent to the server in any attempt.
// After `LATCHED_POOL_ABORT_THRESHOLD` attempts, all retries in the budget
// would also instant-fail (pool still latched; Fix-A takes one eviction/attempt
// but needs multiple seconds for reconnect). The fast-abort produces the same
// outcome as budget exhaustion: write returns Err. The caller (FSS slow-tier
// async write → slow_tier_async_fail accounting, or worker's upload_to_remote)
// handles the Err identically either way. The ≥2-replica invariant is not
// affected: a write that fails fast on a latched pool was never going to
// succeed within the budget anyway.
//
// **Mutation step (under-action):** comment out the
// `if consecutive_latched_fails >= LATCHED_POOL_ABORT_THRESHOLD` block in
// `GrpcStore::write`. The outer `timeout(3 s)` fires and panics with the bespoke
// "Fix-C write under-action violated" message.
//
// **Over-action test:** a non-latched transient write error (Code::Unavailable
// with a non-"buffered service" message) must still retry and NOT abort early.
// ---------------------------------------------------------------------------

/// #8 Fix-C under-action: `GrpcStore::write` against a latched-pool server
/// must abort after `LATCHED_POOL_ABORT_THRESHOLD` consecutive instant
/// failures and return error within 3 s.
///
/// **Mutation step:** comment out the consecutive-latched-fails abort in
/// `GrpcStore::write`'s `rpc_err` arm. The outer `timeout(3 s)` fires and
/// panics with "Fix-C write under-action violated — GrpcStore::write burned
/// the full retry budget on a latched pool".
#[cfg(feature = "failpoints")]
#[serial_test::serial(failpoints)]
#[nativelink_test]
async fn fix_c_write_latched_pool_aborts_fast() -> Result<(), Error> {
    // LatchedPoolByteStream's `write()` handler returns unimplemented —
    // we need a server that returns the latched-pool message from `write`.
    struct LatchedWriteByteStream;

    #[tonic::async_trait]
    impl ByteStream for LatchedWriteByteStream {
        type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

        async fn read(
            &self,
            _request: tonic::Request<ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used in write test"))
        }

        async fn write(
            &self,
            _request: tonic::Request<tonic::Streaming<WriteRequest>>,
        ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
            // tower-Buffer ServiceError message — what `looks_like_latched_pool` matches.
            Err(tonic::Status::unknown(
                "Service was not ready: buffered service failed: timed out",
            ))
        }

        async fn query_write_status(
            &self,
            _request: tonic::Request<QueryWriteStatusRequest>,
        ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used in write test"))
        }
    }

    struct FailpointGuard;
    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            let _ = fail::cfg("latched_pool_bypass_timing", "off");
        }
    }
    fail::cfg("latched_pool_bypass_timing", "return").unwrap();
    let _fp_guard = FailpointGuard;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(LatchedWriteByteStream))
            .serve_with_incoming(incoming)
            .await;
    });

    // 5 retries × 0.5 s base → full budget ~16 s without Fix-C.
    // With Fix-C: 2 roundtrips + ~1 retry delay ≈ < 2 s.
    let mut spec = make_test_spec();
    spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
    spec.rpc_timeout_s = 0;
    spec.retry = Retry {
        max_retries: 5,
        delay: 0.5,
        jitter: 0.0,
        ..Default::default()
    };
    let store = GrpcStore::new(&spec).await?;

    // Build a minimal write stream: one tiny chunk.
    let (unbounded_tx, unbounded_rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<WriteRequest, Error>,
    >();
    let stream = UnboundedReceiverStream::new(unbounded_rx);

    // Send one chunk with finish_write=true then drop the sender so the
    // channel closes.  Without drop, the UnboundedReceiverStream returns
    // Poll::Pending forever and the h2 stream never sends END_STREAM, so
    // tonic's streaming write hangs awaiting more chunks.
    unbounded_tx
        .send(Ok(WriteRequest {
            resource_name: format!(
                "/uploads/{uuid}/blobs/{hash}/{size}",
                uuid = uuid_str(),
                hash = "a".repeat(64),
                size = 4,
            ),
            write_offset: 0,
            finish_write: true,
            data: bytes::Bytes::from_static(b"test"),
        }))
        .ok();
    // Drop sender so the stream closes after the first chunk.
    drop(unbounded_tx);

    let write_stream = WriteRequestStreamWrapper::from(stream).await?;
    let write_result = tokio::time::timeout(
        Duration::from_secs(3),
        store.write(write_stream),
    )
    .await
    .expect(
        "Fix-C write under-action violated — GrpcStore::write burned the full \
         retry budget on a latched pool instead of aborting early; \
         consecutive_latched_fails >= LATCHED_POOL_ABORT_THRESHOLD must trigger \
         RetryResult::Err in the write rpc_err arm (#8 Fix-C extension for write)",
    );

    server_handle.abort();

    match write_result {
        Err(ref e) => {
            // #8 Fix-C: verify the error is the latched-pool classifier error
            // (contains "buffered service") — not some other failure mode.
            // The timeout above is the discriminator between Fix-C fast-abort
            // and budget exhaustion (budget exhaustion takes ~16 s; Fix-C < 2 s).
            // This message assertion binds the abort to the specific error class,
            // so a mutation changing the write server to return e.g. Unavailable
            // (which Fix-C would NOT classify) would fail here even if the abort
            // fires via a different path.
            //
            // **Mutation step for this assertion:** change LatchedWriteByteStream to
            // return `Status::unavailable("transient")` instead of the buffered-service
            // message.  Fix-C no longer fires (classifier returns false), the outer 3-s
            // timeout fires, and the test panics with the "Fix-C write under-action
            // violated" message — NOT this assertion.  This assertion is only reached
            // when Fix-C fires AND returns the wrong error kind.
            assert!(
                e.to_string().contains("buffered service"),
                "Fix-C write: expected abort error to contain 'buffered service' \
                 (the latched-pool classifier error) but got: {e:?} — \
                 the abort must originate from looks_like_latched_pool firing on \
                 the tower-Buffer ServiceError (#8 convergent finding: red-team + testing-czar)"
            );
        }
        Ok(_) => panic!(
            "Fix-C write: GrpcStore::write must return Err when pool is latched, \
             got Ok — the write latched-pool abort is not firing; \
             check consecutive_latched_fails in GrpcStore::write's rpc_err arm (#8)"
        ),
    }
    Ok(())
}

/// #8 Fix-C over-action: non-latched write errors must NOT abort early.
///
/// Covers two over-action variants mirroring the read-side sibling
/// (`fix_c_non_latched_unknown_does_not_misclassify`):
///
/// **Variant A — Code::Unavailable**: `looks_like_latched_pool` requires
/// `Code::Unknown`; `Unavailable` does not match, so Fix-C counter stays 0 and
/// all `max_retries` attempts are consumed.
///
/// **Variant B — Code::Unknown + wrong message**: the code matches but the
/// `"buffered service"` substring does NOT.  Fix-C counter must also stay 0
/// and all retries consumed.  This is the "almost looks like a latch" case.
///
/// Both variants use an `Arc<AtomicU32>` RPC counter.  After the write
/// completes, the counter must equal `1 + max_retries` (= 3).  This kills the
/// surviving mutation (removing the reset-branch `store(0, Relaxed)` for
/// non-latched errors): without the reset, errors accumulate in
/// `write_latched_fails`, reach `LATCHED_POOL_ABORT_THRESHOLD` at attempt 2,
/// and abort early with `count == 2 ≠ 3` → assertion fails.
///
/// **Mutation step:** remove the `else { write_latched_fails.store(0, …) }`
/// reset branch in `GrpcStore::write`'s `rpc_err` arm.  With
/// `Code::Unavailable` and `max_retries=2` the counter reaches 2 at attempt 2
/// and aborts early; `server_rpc_count` will be 2, not 3.  The assertion
/// `assert_eq!(count, 3, "…")` then fails with:
/// "Fix-C over-action: reset-branch removed — Fix-C fired on non-latched
///  Unavailable error at attempt 2; expected 3 RPCs (all retries consumed)"
#[nativelink_test]
async fn fix_c_write_non_latched_unknown_does_not_abort_early() -> Result<(), Error> {
    // Shared RPC counter (reset between variants).
    let rpc_counter = Arc::new(AtomicU32::new(0));

    // -----------------------------------------------------------------------
    // Variant A: Code::Unavailable — code does not match `looks_like_latched_pool`.
    // -----------------------------------------------------------------------
    {
        let counter_a = rpc_counter.clone();

        struct TransientWriteByteStream {
            counter: Arc<AtomicU32>,
        }

        #[tonic::async_trait]
        impl ByteStream for TransientWriteByteStream {
            type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

            async fn read(
                &self,
                _request: tonic::Request<ReadRequest>,
            ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
                Err(tonic::Status::unimplemented("not used"))
            }

            async fn write(
                &self,
                _request: tonic::Request<tonic::Streaming<WriteRequest>>,
            ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
                self.counter.fetch_add(1, Ordering::Relaxed);
                // Variant A: Unavailable — must not trigger Fix-C abort.
                Err(tonic::Status::unavailable("transient: connection reset"))
            }

            async fn query_write_status(
                &self,
                _request: tonic::Request<QueryWriteStatusRequest>,
            ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
                Err(tonic::Status::unimplemented("not used"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ByteStreamServer::new(TransientWriteByteStream {
                    counter: counter_a,
                }))
                .serve_with_incoming(incoming)
                .await;
        });

        // max_retries=2 → 1 initial + 2 retries = 3 total RPCs consumed.
        let mut spec = make_test_spec();
        spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
        spec.rpc_timeout_s = 0;
        spec.retry = Retry {
            max_retries: 2,
            delay: 0.1,
            jitter: 0.0,
            ..Default::default()
        };
        let store = GrpcStore::new(&spec).await?;

        let (unbounded_tx, unbounded_rx) = tokio::sync::mpsc::unbounded_channel::<
            Result<WriteRequest, Error>,
        >();
        let stream = UnboundedReceiverStream::new(unbounded_rx);
        unbounded_tx
            .send(Ok(WriteRequest {
                resource_name: format!(
                    "/uploads/{uuid}/blobs/{hash}/{size}",
                    uuid = uuid_str(),
                    hash = "b".repeat(64),
                    size = 4,
                ),
                write_offset: 0,
                finish_write: true,
                data: bytes::Bytes::from_static(b"over"),
            }))
            .ok();
        drop(unbounded_tx);

        let write_stream = WriteRequestStreamWrapper::from(stream).await?;
        let write_result = tokio::time::timeout(
            Duration::from_secs(5),
            store.write(write_stream),
        )
        .await
        .expect(
            "Fix-C over-action (Unavailable): non-latched transient write must complete \
             within 5 s — must NOT hang or abort early (#8)",
        );

        server_handle.abort();

        assert!(
            write_result.is_err(),
            "Fix-C over-action (Unavailable): write must return Err after retries; \
             Fix-C must NOT convert a non-latched Unavailable error into Ok (#8)"
        );
        let count_a = rpc_counter.load(Ordering::Relaxed);
        assert_eq!(
            count_a, 3,
            "Fix-C over-action: reset-branch removed — Fix-C fired on non-latched \
             Unavailable error at attempt 2; expected 3 RPCs (1 + max_retries=2, all \
             retries consumed) but got {count_a}; \
             check write_latched_fails.store(0) in rpc_err else branch (#8)"
        );
    }

    // -----------------------------------------------------------------------
    // Variant B: Code::Unknown + wrong message.
    // Code matches but "buffered service" substring absent — must not classify
    // as latched.  Mirrors `fix_c_non_latched_unknown_does_not_misclassify`.
    // -----------------------------------------------------------------------
    {
        rpc_counter.store(0, Ordering::Relaxed);
        let counter_b = rpc_counter.clone();

        struct UnknownWrongMsgByteStream {
            counter: Arc<AtomicU32>,
        }

        #[tonic::async_trait]
        impl ByteStream for UnknownWrongMsgByteStream {
            type ReadStream = futures::stream::Empty<Result<ReadResponse, tonic::Status>>;

            async fn read(
                &self,
                _request: tonic::Request<ReadRequest>,
            ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
                Err(tonic::Status::unimplemented("not used"))
            }

            async fn write(
                &self,
                _request: tonic::Request<tonic::Streaming<WriteRequest>>,
            ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
                self.counter.fetch_add(1, Ordering::Relaxed);
                // Variant B: Unknown code but no "buffered service" substring.
                // `looks_like_latched_pool` requires BOTH code AND substring.
                Err(tonic::Status::unknown("transport error: peer connection reset"))
            }

            async fn query_write_status(
                &self,
                _request: tonic::Request<QueryWriteStatusRequest>,
            ) -> Result<tonic::Response<QueryWriteStatusResponse>, tonic::Status> {
                Err(tonic::Status::unimplemented("not used"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ByteStreamServer::new(UnknownWrongMsgByteStream {
                    counter: counter_b,
                }))
                .serve_with_incoming(incoming)
                .await;
        });

        let mut spec = make_test_spec();
        spec.endpoints[0].address = format!("http://127.0.0.1:{port}");
        spec.rpc_timeout_s = 0;
        spec.retry = Retry {
            max_retries: 2,
            delay: 0.1,
            jitter: 0.0,
            ..Default::default()
        };
        let store = GrpcStore::new(&spec).await?;

        let (unbounded_tx, unbounded_rx) = tokio::sync::mpsc::unbounded_channel::<
            Result<WriteRequest, Error>,
        >();
        let stream = UnboundedReceiverStream::new(unbounded_rx);
        unbounded_tx
            .send(Ok(WriteRequest {
                resource_name: format!(
                    "/uploads/{uuid}/blobs/{hash}/{size}",
                    uuid = uuid_str(),
                    hash = "c".repeat(64),
                    size = 4,
                ),
                write_offset: 0,
                finish_write: true,
                data: bytes::Bytes::from_static(b"over"),
            }))
            .ok();
        drop(unbounded_tx);

        let write_stream = WriteRequestStreamWrapper::from(stream).await?;
        let write_result = tokio::time::timeout(
            Duration::from_secs(5),
            store.write(write_stream),
        )
        .await
        .expect(
            "Fix-C over-action (Unknown+wrong-msg): non-latched write must complete \
             within 5 s — must NOT abort early on Unknown without 'buffered service' (#8)",
        );

        server_handle.abort();

        assert!(
            write_result.is_err(),
            "Fix-C over-action (Unknown+wrong-msg): write must return Err; \
             Fix-C must NOT fire on Unknown without 'buffered service' substring (#8)"
        );
        let count_b = rpc_counter.load(Ordering::Relaxed);
        assert_eq!(
            count_b, 3,
            "Fix-C over-action (Unknown+wrong-msg): expected 3 RPCs (1 + max_retries=2) \
             but got {count_b}; Fix-C must not classify Unknown+wrong-message as a latch \
             (#8 code-reviewer NIT-3 / testing-czar SHOULD)"
        );
    }

    Ok(())
}

/// Helper: generate a UUID-shaped string for write resource names.
fn uuid_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("{t:08x}-0000-0000-0000-000000000000")
}
