//! #55: GrpcStore::write must drain the reader to EOF on Code::AlreadyExists
//! instead of returning Ok(committed_size=0) while the producer still has
//! chunks pending.
//!
//! ## Background (RCA: `.claude/audits/51-dsym-receiver-disconnect-rca-2026-06-04.md`)
//!
//! Worker uploads a >= 256 MiB DWARF binary via the legacy ByteStream Write
//! path (above `MAX_CHUNKED_BLOB_SIZE` skips chunked dispatch). The server
//! already holds the digest (post-reboot backfill race), so its ByteStream
//! `Write` returns `Code::AlreadyExists` after consuming the first prefix.
//! `GrpcStore::write`'s retrier at `grpc_store.rs:1500-1505` silenced this
//! to `RetryResult::Ok(WriteResponse { committed_size: 0 })` and returned —
//! WITHOUT draining the upstream `DropCloserReadHalf`. The producer task in
//! `FastSlowStore::stream_file_to_store` was still pumping 256 KiB chunks
//! into `tx`; once `rx` dropped, `tx.send` returned
//! `make_err!(Code::Internal, "Failed to write to data, receiver
//! disconnected")`. The post-#476 join match at
//! `fast_slow_store.rs:4613-4618` surfaced this as the action-level error,
//! masking the real cause.
//!
//! ## Invariant under test
//!
//! When the consumer (server ByteStream Write) returns
//! `Code::AlreadyExists` mid-stream, the producer's downstream `tx.send`
//! must NOT fail with "receiver disconnected" — `GrpcStore::write` must
//! drain its reader to EOF before returning Ok so the producer can
//! complete its send loop cleanly.
//!
//! ## Seams crossed (production composition)
//!
//! 1. Producer: a background task pumps chunks into `tx` via
//!    `tx.send(...).await` then `tx.send_eof()` — the same pattern used by
//!    `FastSlowStore::stream_file_to_store`'s `forward_fut`.
//! 2. Consumer: real `GrpcStore::update` (no mock of the GrpcStore
//!    interface) which constructs the `unfold` stream over the
//!    `DropCloserReadHalf`, wraps it in `WriteRequestStreamWrapper`, and
//!    invokes the legacy `self.write(...)` retrier.
//! 3. Server: in-process tonic `ByteStream` impl that reads one chunk,
//!    notifies the producer via a `tokio::sync::Notify`, then returns
//!    `tonic::Status::already_exists(...)` — mirroring the production
//!    server-side dedup short-circuit. The notify is what makes the
//!    test deterministic: the producer waits on it before sending its
//!    next chunk, guaranteeing the symptom-triggering send happens
//!    AFTER the server has signaled AlreadyExists and (without the fix)
//!    the consumer has dropped its `rx`.
//! 4. Join: `tokio::join!(consumer, producer)` to assert both halves
//!    complete cleanly (consumer Ok, producer Ok). Wrapping
//!    `tokio::time::timeout(5s)` is the deadlock detector.
//!
//! ## Mutation
//!
//! Comment out the drain loop in `grpc_store.rs:1500-1505` AlreadyExists
//! arm and the test red-fails with the bespoke
//! `"GrpcStore::write left reader undrained on AlreadyExists —
//! producer sees receiver disconnected (#55 symptom)"` message.

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::byte_stream_server::{
    ByteStream, ByteStreamServer,
};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse,
    WriteRequest, WriteResponse,
};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::buf_channel::make_buf_channel_pair_with_size;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreKey, StoreLike, UploadSizeInfo};
use tokio::sync::Notify;
use tokio::time::timeout;

fn make_test_endpoint(address: String) -> GrpcEndpoint {
    GrpcEndpoint {
        address,
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

fn make_test_spec(port: u16) -> GrpcSpec {
    GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![make_test_endpoint(format!("http://127.0.0.1:{port}"))],
        store_type: StoreType::Cas,
        retry: Retry {
            max_retries: 0,
            delay: 0.0,
            jitter: 0.0,
            ..Default::default()
        },
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 0,
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

/// In-process `ByteStream` that consumes exactly one chunk, notifies the
/// producer via `already_exists_signaled`, then returns
/// `tonic::Status::already_exists(...)`. Mirrors the production server's
/// dedup short-circuit on a digest the server already holds.
struct AlreadyExistsAfterOneChunk {
    chunks_consumed: Arc<AtomicU64>,
    already_exists_signaled: Arc<Notify>,
}

#[tonic::async_trait]
impl ByteStream for AlreadyExistsAfterOneChunk {
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
        // Read exactly one inbound chunk (so the producer side gets
        // partway through its send loop), then signal AlreadyExists.
        // After returning, the server-side stream half closes; tonic will
        // tear down the receive half. The client's `WriteState` retrier
        // observes the `Code::AlreadyExists` and (post-fix) must drain the
        // upstream reader to EOF before returning Ok.
        let mut inbound = request.into_inner();
        if let Ok(Some(_chunk)) = inbound.message().await {
            self.chunks_consumed.fetch_add(1, Ordering::SeqCst);
        }
        // Notify the producer side that the server is about to return
        // AlreadyExists. The producer waits on this before sending its
        // next chunk; without the wait the producer might race to send
        // all chunks before the consumer drops `rx`, sidestepping the
        // symptom-triggering ordering.
        self.already_exists_signaled.notify_one();
        Err(tonic::Status::already_exists("blob already exists"))
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

/// In-process `ByteStream` that consumes ALL inbound chunks then returns a
/// normal `WriteResponse`. Used as the negative control (T3) — the
/// fix-side drain MUST NOT regress the normal happy path.
struct EchoBytesUntilEof {
    chunks_consumed: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ByteStream for EchoBytesUntilEof {
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
        let mut inbound = request.into_inner();
        let mut committed_size: i64 = 0;
        loop {
            match inbound.message().await {
                Ok(Some(msg)) => {
                    self.chunks_consumed.fetch_add(1, Ordering::SeqCst);
                    committed_size += msg.data.len() as i64;
                    if msg.finish_write {
                        break;
                    }
                }
                Ok(None) => break,
                Err(status) => return Err(status),
            }
        }
        Ok(tonic::Response::new(WriteResponse { committed_size }))
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

/// Bind a free port and spawn an in-process tonic `ByteStream` server with
/// the given impl. Returns `(port, server_join_handle)`.
async fn spawn_server<S>(server_impl: S) -> (u16, tokio::task::JoinHandle<()>)
where
    S: ByteStream + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ByteStreamServer::new(server_impl))
            .serve_with_incoming(incoming)
            .await;
    });
    (port, handle)
}

/// T1 — Production-composition test for #55.
///
/// Real `GrpcStore::update` called with a `DropCloserReadHalf` produced
/// by a background producer task that mirrors
/// `FastSlowStore::stream_file_to_store::forward_fut`. The in-process
/// server returns `Code::AlreadyExists` after consuming one chunk and
/// notifies the producer to send a second chunk AFTER the server has
/// returned AlreadyExists.
///
/// Pre-fix: the consumer (`GrpcStore::update`) returns Ok BEFORE the
/// producer's second send; `rx` drops; the producer's `tx.send` for
/// chunk 1 fails with "receiver disconnected"; the producer task
/// returns Err.
///
/// Post-fix: `GrpcStore::write`'s AlreadyExists arm drains the reader
/// to EOF before returning Ok. The drain consumes chunk 1 (and any
/// subsequent chunks + EOF), so the producer's sends complete cleanly
/// and `tx.send_eof()` lands without "receiver disconnected".
///
/// **Mutation:** comment out the drain loop in `grpc_store.rs:1500-1505`;
/// this test red-fails on the `producer_res.expect(...)` line with the
/// bespoke message below.
#[nativelink_test]
async fn grpc_store_write_drains_reader_on_already_exists() -> Result<(), Error> {
    let chunks_consumed = Arc::new(AtomicU64::new(0));
    let already_exists_signaled = Arc::new(Notify::new());
    let server_impl = AlreadyExistsAfterOneChunk {
        chunks_consumed: chunks_consumed.clone(),
        already_exists_signaled: already_exists_signaled.clone(),
    };
    let (port, server_handle) = spawn_server(server_impl).await;

    let spec = make_test_spec(port);
    let store = GrpcStore::new(&spec).await?;

    // Build the digest. Hash content is unimportant; the in-process
    // server doesn't validate it.
    const CHUNK_LEN: usize = 1024 * 1024;
    const N_CHUNKS: usize = 4;
    const PAYLOAD_LEN: u64 = (CHUNK_LEN * N_CHUNKS) as u64;
    let digest = DigestInfo::try_new(
        "0000000000000000000000000000000000000000000000000000000000000000",
        PAYLOAD_LEN,
    )?;
    let key: StoreKey<'static> = StoreKey::from(digest).into_owned();

    // Tiny channel capacity (1 slot) so the producer's second send has
    // to block until the consumer makes progress. Combined with the
    // server-side Notify, this forces the producer to attempt
    // `tx.send(chunk_1)` strictly AFTER the server has returned
    // AlreadyExists and (pre-fix) the consumer has dropped `rx`.
    let (mut tx, rx) = make_buf_channel_pair_with_size(1);

    let already_exists_signaled_for_producer = already_exists_signaled.clone();
    let producer_fut = async move {
        // Send chunk 0 (the consumer / server will consume this one).
        let chunk0 = Bytes::from(vec![0u8; CHUNK_LEN]);
        tx.send(chunk0).await.map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "Failed to send chunk in stream_file_to_store: {:?}",
                e
            )
        })?;

        // Wait until the server has signaled AlreadyExists. Without
        // the fix, the consumer drops `rx` immediately after the
        // server returns; the next send below will fail with
        // "receiver disconnected".
        already_exists_signaled_for_producer.notified().await;

        // Continue sending the remaining chunks + EOF. With the
        // drain fix in place, these all complete cleanly because
        // GrpcStore::write's AlreadyExists arm is draining `rx`.
        for i in 1..N_CHUNKS {
            let chunk = Bytes::from(vec![(i & 0xFF) as u8; CHUNK_LEN]);
            tx.send(chunk).await.map_err(|e| {
                nativelink_error::make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to send chunk in stream_file_to_store: {:?}",
                    e
                )
            })?;
        }
        tx.send_eof().map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "Failed to send EOF: {:?}",
                e
            )
        })
    };

    let key_for_consumer = key.borrow().into_owned();
    let consumer_fut = async {
        store
            .update(
                key_for_consumer,
                rx,
                UploadSizeInfo::ExactSize(PAYLOAD_LEN),
            )
            .await
    };

    // 5s deadlock-detector budget per CLAUDE.md.
    let joined = timeout(
        Duration::from_secs(5),
        async { tokio::join!(consumer_fut, producer_fut) },
    )
    .await
    .expect(
        "GrpcStore::write must drain reader to EOF on AlreadyExists — \
         receiver-disconnect symptom would re-fire (#55)",
    );

    server_handle.abort();

    let (consumer_res, producer_res) = joined;

    // Consumer side: AlreadyExists must be silenced to Ok (existing
    // behavior; the drain doesn't change this).
    consumer_res.expect(
        "GrpcStore::update must silence AlreadyExists to Ok — \
         the drain fix preserves this behavior",
    );

    // Producer side: with the drain in place, every tx.send completes
    // cleanly. Pre-fix this is Err("Failed to send chunk in
    // stream_file_to_store: ... receiver disconnected").
    producer_res.expect(
        "GrpcStore::write left reader undrained on AlreadyExists — \
         producer sees receiver disconnected (#55 symptom)",
    );

    // Sanity check the in-process server actually received at least one
    // chunk before returning AlreadyExists. If 0, the server returned
    // AlreadyExists before reading anything and the test wouldn't have
    // exercised the mid-stream symptom.
    assert!(
        chunks_consumed.load(Ordering::SeqCst) >= 1,
        "server consumed 0 chunks before AlreadyExists; test setup wrong",
    );
    Ok(())
}

/// T3 — Negative control: the normal happy path is unaffected by the
/// drain. The fix only changes the AlreadyExists arm; this asserts that
/// regular updates still pass cleanly with both consumer and producer
/// returning Ok.
///
/// **Mutation:** the drain in the AlreadyExists arm doesn't apply on
/// this path, so a mutation of the drain code is silent here. T3's
/// purpose is regression-detection only: any code change that breaks
/// normal writes (e.g. an over-broad drain in the wrong arm) red-fails
/// here on the consumer-Ok assertion.
#[nativelink_test]
async fn grpc_store_write_normal_path_unaffected() -> Result<(), Error> {
    let chunks_consumed = Arc::new(AtomicU64::new(0));
    let server_impl = EchoBytesUntilEof {
        chunks_consumed: chunks_consumed.clone(),
    };
    let (port, server_handle) = spawn_server(server_impl).await;

    let spec = make_test_spec(port);
    let store = GrpcStore::new(&spec).await?;

    const CHUNK_LEN: usize = 1024 * 1024;
    const N_CHUNKS: usize = 4;
    const PAYLOAD_LEN: u64 = (CHUNK_LEN * N_CHUNKS) as u64;
    let digest = DigestInfo::try_new(
        "0000000000000000000000000000000000000000000000000000000000000000",
        PAYLOAD_LEN,
    )?;
    let key: StoreKey<'static> = StoreKey::from(digest).into_owned();

    let (mut tx, rx) = make_buf_channel_pair_with_size(128);

    let producer_fut = async move {
        for i in 0..N_CHUNKS {
            let chunk = Bytes::from(vec![(i & 0xFF) as u8; CHUNK_LEN]);
            tx.send(chunk).await.map_err(|e| {
                nativelink_error::make_err!(
                    nativelink_error::Code::Internal,
                    "Failed to send chunk: {:?}",
                    e
                )
            })?;
        }
        tx.send_eof().map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "Failed to send EOF: {:?}",
                e
            )
        })
    };

    let key_for_consumer = key.borrow().into_owned();
    let consumer_fut = async {
        store
            .update(
                key_for_consumer,
                rx,
                UploadSizeInfo::ExactSize(PAYLOAD_LEN),
            )
            .await
    };

    let joined = timeout(
        Duration::from_secs(5),
        async { tokio::join!(consumer_fut, producer_fut) },
    )
    .await
    .expect(
        "GrpcStore::update normal path must not deadlock — \
         T3 regression detector",
    );

    server_handle.abort();

    let (consumer_res, producer_res) = joined;
    consumer_res
        .expect("GrpcStore::update normal happy path must return Ok (T3 regression)");
    producer_res
        .expect("producer must complete cleanly on normal happy path (T3 regression)");

    // Server saw all N chunks (sanity).
    assert!(
        chunks_consumed.load(Ordering::SeqCst) >= N_CHUNKS as u64,
        "server consumed {} chunks, expected >= {}",
        chunks_consumed.load(Ordering::SeqCst),
        N_CHUNKS,
    );
    Ok(())
}
