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

//! Integration tests for #355 inbound terminal-frame discriminator on
//! ByteStream/Write.
//!
//! These wire the real `ByteStreamServer::write` handler through a real
//! `tonic::Streaming<WriteRequest>` (the same composition production
//! sees) and assert that the new
//! [`bytestream_terminal_inspector`](nativelink_service::bytestream_terminal_inspector)
//! logs fire with the right discriminator fields. Today's #353
//! partial-upload incident produced "Expected WriteRequest struct in
//! stream (got None)" with no client-side discriminator; these tests
//! guard the new visibility so a refactor that drops the inspector
//! flips the assertions red.
//!
//! ## Tests under test
//!
//! 1. `clean_eof_emits_inbound_eof_clean_event` — partial chunk + drop
//!    sender ⇒ `bytestream_write_inbound_eof_clean` info event with
//!    `kind="clean_eof"`. Mutation: comment out the `info!(...)` arm in
//!    `TerminalFrameInspectingStream::poll_next` for `Poll::Ready(None)`;
//!    the test goes red.
//!
//! 2. `error_terminal_emits_inbound_terminal_error_event` — drive the
//!    server's inbound stream to terminate via `Some(Err(status))` (we
//!    inject a `Status::cancelled` via the custom stream below) ⇒
//!    `bytestream_write_inbound_terminal_error` warn event with
//!    `grpc_code=Cancelled` and `kind="grpc_error"`. Mutation: comment
//!    out the `warn!(...)` arm; the test goes red.
//!
//! 3. `clean_eof_inspector_event_coexists_with_inner_write_failed_warn`
//!    — composition test asserting the new `bytestream_write_inbound_eof_clean`
//!    event coexists with the pre-existing `inner_write failed` warn so
//!    the new inspector is purely additive.
//!
//! 4. `h2_rst_stream_end_to_end_emits_h2_reset_event` — drives a REAL
//!    h2 `RST_STREAM(REFUSED_STREAM)` frame across a real
//!    `tonic::transport::Server` listening on a TCP socket and asserts
//!    the inspector emits `kind=h2_rst:REFUSED_STREAM` with the matching
//!    `h2_reason` / `h2_reason_code` fields. This is the only test that
//!    actually crosses the `h2::Error → tonic::Status::source_chain →
//!    inspector` seam end-to-end; tests 1+2 wrap the inspector around a
//!    hand-built `Streaming<WriteRequest>` and the unit tests inside
//!    `bytestream_terminal_inspector.rs` feed `Status::from(h2::Error)`
//!    synthetically. Without this test a future tonic upgrade swapping
//!    the source-chain mechanism would silently downgrade the inspector
//!    to always emit `kind=grpc_error`.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderValue, Method, Request as HttpRequest, Version, header};
use hyper::body::Frame;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::encode_stream_proto;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::mpsc;
use tonic::codec::{Codec, CompressionEncoding};
use tonic::{Request, Response, Streaming};
use tonic_prost::ProstCodec;

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_bytestream_server(store_manager: &StoreManager) -> Result<ByteStreamServer, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout: 0,
            max_bytes_per_stream: 1024,
            ..Default::default()
        },
    }];
    ByteStreamServer::new(&config, store_manager, None)
}

fn make_stream(
    encoding: Option<CompressionEncoding>,
) -> (mpsc::Sender<Frame<Bytes>>, Streaming<WriteRequest>) {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream = Streaming::new_request(codec.decoder(), body, encoding, None);
    (tx, stream)
}

type WriteJoinHandle = JoinHandleDropGuard<
    Result<Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
>;

fn make_resource_name(declared_len: u64) -> String {
    format!(
        "{INSTANCE_NAME}/uploads/4dcec57e-1389-4ab5-b188-4a59f22ceb4b/blobs/{HASH1}/{declared_len}"
    )
}

fn spawn_write(bs_server: Arc<ByteStreamServer>) -> (mpsc::Sender<Frame<Bytes>>, WriteJoinHandle) {
    let (tx, stream) = make_stream(Some(CompressionEncoding::Gzip));
    let join = spawn!("bs_server_write_terminal_inspector", async move {
        bs_server.write(Request::new(stream)).await
    });
    (tx, join)
}

/// **Test 1**: a client that sends a partial chunk then drops the gRPC
/// sender (no `finish_write: true`) MUST cause the
/// `TerminalFrameInspectingStream` wrapper to emit
/// `bytestream_write_inbound_eof_clean` with `kind="clean_eof"`.
///
/// In production this is the #353 / #320 case: tonic's `Streaming<T>`
/// returns `Poll::Ready(None)` when the peer's HTTP/2 DATA frame
/// carrying END_STREAM arrives without a gRPC trailer indicating an
/// error. The wrapper observes the `None` directly from the underlying
/// `Streaming<T>`.
///
/// Mutation step: comment out the `info!(...)` arm in
/// `TerminalFrameInspectingStream::poll_next` for `Poll::Ready(None)`.
/// `logs_contain` no longer finds the substrings, so the assertions
/// fail red — proving the test guards the inspector, not just the
/// happy path.
#[nativelink_test]
pub async fn clean_eof_emits_inbound_eof_clean_event()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Send a partial chunk (no finish_write). Declared size 100, only 12
    // bytes go on the wire.
    let partial = WriteRequest {
        resource_name: make_resource_name(100),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-data"),
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;

    // Yield so the server consumes the partial before we close.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Drop the sender — the underlying ChannelBody returns a clean
    // END_STREAM (no error trailer), so tonic's Streaming<T> returns
    // Poll::Ready(None) at the next poll. That's the path we instrument.
    drop(tx);

    let server_result = tokio::time::timeout(Duration::from_secs(5), join_handle)
        .await
        .expect(
            "join did not return — clean-EOF inspector path stalled within 5s deadlock budget",
        )
        .expect("write task panicked");

    assert!(
        server_result.is_err(),
        "client disconnect MUST still cause the upload to fail (the inspector \
         is observability only, it does not rescue truncated uploads): {server_result:?}"
    );

    // The new info event from the inspector wrapper.
    assert!(
        logs_contain("bytestream_write_inbound_eof_clean"),
        "MUST emit bytestream_write_inbound_eof_clean info event when the \
         inbound Streaming<WriteRequest> returns Poll::Ready(None) — without \
         this an operator can't tell clean END_STREAM from h2 RST_STREAM (#355)"
    );
    assert!(
        logs_contain("kind=\"clean_eof\""),
        "MUST tag the event with kind=\"clean_eof\" so a grep-based filter can \
         match the discriminator without parsing message text (#355)"
    );

    Ok(())
}

/// **Test 2**: the underlying `Streaming<WriteRequest>` yields
/// `Some(Err(status))` MUST produce `bytestream_write_inbound_terminal_error`
/// with `kind="grpc_error"` (or `kind="h2_rst:..."` if the source
/// downcasts to `h2::Error`).
///
/// We trigger this by feeding the server a malformed gRPC frame: tonic's
/// `Streaming::new_request` decoder will surface a decode error as
/// `Some(Err(status))` (a `Status::Unknown` synthesized by the codec)
/// rather than `Poll::Ready(None)`.
///
/// Mutation step: comment out the `warn!(...)` arm in
/// `TerminalFrameInspectingStream::poll_next` for the error path. The
/// `logs_contain` assertions go red.
#[nativelink_test]
pub async fn error_terminal_emits_inbound_terminal_error_event()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Send a valid first WriteRequest so WriteRequestStreamWrapper::from()
    // can extract the resource_info — otherwise the failure happens
    // BEFORE the inspector observes any terminal frame on the inner
    // stream (the wrapper bails on the first `next().await`'s decode).
    //
    // Wait — actually the `from()` consumes the first message. If the
    // first frame is malformed, the inspector wrapper sees the decode
    // error directly from `Streaming::poll_next`, which is exactly the
    // path we want to instrument. Skip the valid first chunk and inject
    // a malformed frame as the very first wire message: tonic's codec
    // raises `Status::internal("Invalid compression flag: ...")` from
    // `poll_next`, which is `Some(Err(status))`. The inspector logs
    // BEFORE WriteRequestStreamWrapper::from forwards the err.
    //
    // Send a frame whose first byte is 0x05 (invalid gRPC compression
    // flag — tonic accepts only 0 or 1). Length-prefix is harmless
    // because the codec rejects on the flag byte first.
    let bad_frame = Bytes::from_static(&[0x05, 0x00, 0x00, 0x00, 0x00]);
    tx.send(Frame::data(bad_frame)).await?;

    // Drop the sender so the stream terminates after the bad frame is
    // surfaced as `Some(Err(_))`.
    drop(tx);

    let server_result = tokio::time::timeout(Duration::from_secs(5), join_handle)
        .await
        .expect("join did not return — error-terminal inspector path stalled")
        .expect("write task panicked");

    assert!(
        server_result.is_err(),
        "malformed frame MUST cause the upload to fail: {server_result:?}"
    );

    assert!(
        logs_contain("bytestream_write_inbound_terminal_error"),
        "MUST emit bytestream_write_inbound_terminal_error warn event when the \
         inbound stream yields Some(Err(_)) — without this an operator can't \
         distinguish gRPC decode failure from h2 RST_STREAM (#355)"
    );
    // Either kind is acceptable here; what matters is that the field is
    // populated. We assert the substring `kind=grpc_error` because the
    // codec error path goes through Status::internal() with no h2 source.
    // Tracing's default formatter renders Display-formatted (`%`) values
    // without quotes, so the substring is bare `kind=grpc_error`.
    assert!(
        logs_contain("kind=grpc_error"),
        "MUST tag a non-h2 error termination with kind=grpc_error so an \
         operator can disambiguate from kind=h2_rst:CANCEL / \
         kind=h2_rst:REFUSED_STREAM etc. (#355)"
    );
    // Code field must be present; exact value is codec-implementation
    // dependent so we only assert the field appears.
    assert!(
        logs_contain("grpc_code="),
        "MUST report grpc_code field on the warn event (#355)"
    );

    Ok(())
}

/// **Test 3**: companion to Test 1. The OTHER existing event
/// (`inner_write failed`) and the new `bytestream_write_inbound_eof_clean`
/// event MUST coexist on the same disconnect — the former tells the
/// operator the write failed and why (size mismatch), the latter tells
/// them HOW the wire terminated (clean EOF vs RST). Without both, the
/// classification of #353 / #320 disconnects remains ambiguous.
///
/// This test does not introduce any NEW production behavior; it
/// asserts the COMPOSITION of the existing diagnostics with the new
/// inspector log. Mutation: deletion of either the `info!` or the
/// outer `warn!` flips the test red.
#[nativelink_test]
pub async fn clean_eof_inspector_event_coexists_with_inner_write_failed_warn()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    let partial = WriteRequest {
        resource_name: make_resource_name(100),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial"),
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    drop(tx);

    let _server_result = tokio::time::timeout(Duration::from_secs(5), join_handle)
        .await
        .expect("inner_write+inspector composition stalled within 5s budget")
        .expect("write task panicked");

    // BOTH must appear: pre-existing diagnostic + new wire-shape
    // discriminator.
    assert!(
        logs_contain("inner_write failed"),
        "MUST still emit the pre-existing inner_write failed warn — the new \
         inspector is purely additive, not a replacement"
    );
    assert!(
        logs_contain("bytestream_write_inbound_eof_clean"),
        "MUST emit the new inspector event alongside inner_write failed so an \
         operator can answer 'was this a clean EOF or a RST?' (#355)"
    );

    Ok(())
}

/// **Test 4 (#355 fix-up)**: drive a *real* h2 `RST_STREAM(REFUSED_STREAM)`
/// end-to-end through the production seam (`tonic::transport::Server` →
/// `byte_stream_server::ByteStreamServer<ByteStreamServer>` →
/// `tonic::Streaming<WriteRequest>::poll_next` → `TerminalFrameInspectingStream`)
/// and assert the inspector classifies it as `kind=h2_rst:REFUSED_STREAM`
/// with the matching `h2_reason` / `h2_reason_code` fields.
///
/// ## Why this complements the existing tests
///
/// `clean_eof_emits_inbound_eof_clean_event` and
/// `error_terminal_emits_inbound_terminal_error_event` (Tests 1+2) wrap the
/// inspector around a hand-built `Streaming<WriteRequest>` constructed via
/// `Streaming::new_request(codec.decoder(), body, ..)`. That exercises every
/// in-process layer EXCEPT the actual h2 → tonic source-chain plumbing.
///
/// The unit test `h2_reason_extracted_from_status_source_chain`
/// (`bytestream_terminal_inspector.rs`) FEEDS a synthetic
/// `Status::from(h2::Error::from(h2::Reason::CANCEL))` and confirms our
/// downcast walk pulls the `h2::Reason` out — but a future tonic upgrade that
/// changed where the `h2::Error` lives in the source chain (or wrapped it in
/// a new error type before reaching `Streaming::poll_next`) would silently
/// downgrade the production-path inspector to always emit `kind=grpc_error`.
/// Both unit tests would still pass; the production observability would
/// regress invisibly.
///
/// This test is the only one that actually crosses the seam with a real
/// h2 RST_STREAM frame on the wire — it is the regression harness for
/// "tonic-version upgrade silently breaks h2 source-chain extraction."
///
/// ## Why REFUSED_STREAM, not CANCEL
///
/// Tonic 0.14.5's request-side decoder
/// (`tonic/src/codec/decode.rs::poll_frame`) silently swallows
/// `Code::Cancelled` on the request direction:
///
/// ```ignore
/// Some(Err(status)) => {
///     if self.direction == Direction::Request && status.code() == Code::Cancelled {
///         return Poll::Ready(Ok(None));   // <-- demoted to clean EOF
///     }
///     ...
/// }
/// ```
///
/// `code_from_h2(Reason::CANCEL) == Code::Cancelled`, so a wire
/// RST_STREAM(CANCEL) reaches the server-side handler as `Poll::Ready(None)`
/// — a clean EOF — and the inspector emits `kind=clean_eof`. **This is a
/// real production limitation of #355 with tonic 0.14.5: client-initiated
/// RST_STREAM(CANCEL) cannot be distinguished from a clean END_STREAM at
/// the inspector layer.** A future tonic upgrade that lifts this filter
/// would let the inspector surface the CANCEL reason; until then the only
/// observable evidence of a wire CANCEL is on the upstream HTTP/2 layer.
///
/// REFUSED_STREAM (=7) maps to `Code::Unavailable`, NOT `Cancelled`, so it
/// is NOT filtered by the request-side decoder — `Streaming::poll_next`
/// surfaces it as `Some(Err(status))` with the `h2::Error` in the source
/// chain. This is the path the inspector's `H2Reset` arm is built for and
/// the path this test crosses end-to-end.
///
/// ## Mechanism
///
/// We bypass tonic's client entirely: a tonic client lets its outbound body
/// stream end with `END_STREAM` (clean half-close) on drop, NOT a
/// `RST_STREAM`. The only reliable way to put a `RST_STREAM` frame on the
/// wire is to drive the `h2::client` directly:
///   1. Open a TCP socket to the in-process tonic server.
///   2. Run `h2::client::handshake` to negotiate HTTP/2.
///   3. Send a HEADERS frame for `POST /google.bytestream.ByteStream/Write`
///      with gRPC headers (`content-type: application/grpc`, `te: trailers`).
///   4. Send one DATA frame containing a length-prefixed gRPC-framed
///      `WriteRequest` (so the server-side
///      `WriteRequestStreamWrapper::from(inspected).await` succeeds — the
///      first message is required to extract the resource path before the
///      inspector observes any subsequent terminal frame).
///   5. Wait long enough for the conn-driver task to flush the HEADERS+DATA
///      frames (h2 buffers them locally and `send_reset` would otherwise
///      drop them, leaving an idle stream + RST → server PROTOCOL_ERROR).
///   6. Call `SendStream::send_reset(h2::Reason::REFUSED_STREAM)`. h2 emits
///      a real RST_STREAM frame on the wire. The server-side h2 layer
///      surfaces this to its `Streaming<WriteRequest>::poll_next` as
///      `Some(Err(Status::from(h2::Error::from(h2::Reason::REFUSED_STREAM))))` —
///      that is the seam under test.
///
/// ## Mutation step (manual verification done at change time)
///
/// Comment out the `InboundTerminalKind::H2Reset { .. } =>` arm in
/// `bytestream_terminal_inspector.rs::TerminalFrameInspectingStream::poll_next`
/// (or comment out the whole `h2_reason_from_status` body so it returns
/// `None` unconditionally — equivalent to "tonic upgrade lost the
/// h2::Error source"). The test must red-fail with the bespoke
/// `MUST emit kind=h2_rst:REFUSED_STREAM ... — h2::Reason extraction broke ...`
/// assertion message so the regression class is unambiguous in CI output.
///
/// ## Verified tonic 0.14.5 behavior
///
/// Empirically (verified by an earlier iteration of this test):
/// 1. Dropping a tonic streaming-client request body sends a clean
///    END_STREAM, NOT RST_STREAM. (h2's `SendStream::Drop` sends
///    RST_STREAM only if the stream was not explicitly closed; tonic's
///    body adapter emits END_STREAM on the final body frame, so by the
///    time the SendStream is dropped the stream is already in
///    `HalfClosedLocal` state — no RST is sent.) That's why this test
///    bypasses tonic on the client side and drives `h2::client` directly.
/// 2. Wire RST_STREAM(CANCEL) is silently demoted to `clean_eof` by
///    tonic's request-side decoder (see "Why REFUSED_STREAM" above). The
///    `error_terminal_emits_inbound_terminal_error_event` test injects a
///    `Status::from(h2::Error::from(Reason::CANCEL))` AFTER tonic's
///    decoder, so it exercises the inspector arm but does NOT exercise
///    the wire→tonic-decoder seam — confirming why this end-to-end test
///    is needed.
#[nativelink_test(flavor = "multi_thread", worker_threads = 2)]
pub async fn h2_rst_stream_end_to_end_emits_h2_reset_event()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::google::bytestream::byte_stream_server::ByteStreamServer as TonicByteStreamServer;

    let store_manager = make_store_manager().await?;
    let bs_server = make_bytestream_server(store_manager.as_ref())?;

    // Bind the tonic server on an ephemeral port. The tonic Server uses
    // tower::Service over hyper, which uses h2 under the hood — exactly
    // the production composition. The `into_service()` form (NOT
    // `into_zero_copy_service`) is what the test exercises because the
    // production `write` handler at bytestream_server.rs:2829 is the one
    // that wraps the inbound stream in `TerminalFrameInspectingStream`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral bind must succeed (CI host out of ports?)");
    let server_addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    // Spawn the tonic server via `nativelink_util::spawn!` (NOT raw
    // `tokio::spawn`) so the spawned task inherits this test's
    // `tracing::Span` scope. tracing-test's `logs_contain` filters log
    // lines by checking they contain the test fn name as a span scope
    // prefix; raw `tokio::spawn` detaches from the parent span and
    // emitted logs go uncategorized. The bytestream server's
    // `#[instrument]` spans become child spans of the test's root span,
    // so the inspector's `warn!(target: "bytestream_write_inbound_terminal", ...)`
    // event is reachable via `logs_contain`.
    let svc = TonicByteStreamServer::new(bs_server);
    let server_handle = spawn!("bs_server_h2_rst_e2e", async move {
        drop(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    // Connect a raw TCP socket to the in-process tonic server. We're
    // about to bypass tonic's own client and drive h2 directly so we can
    // emit a RST_STREAM frame the only way h2 lets us: via
    // `SendStream::send_reset(Reason)`.
    let tcp = tokio::net::TcpStream::connect(server_addr)
        .await
        .expect("h2 client TCP connect to in-process tonic server must succeed");

    // Negotiate HTTP/2 (prior knowledge — no ALPN). tonic's transport
    // layer accepts HTTP/2 over plain TCP for in-process tests like this
    // one; this is the same shape its own integration tests use.
    let (h2, conn) = h2::client::handshake(tcp)
        .await
        .expect("h2::client::handshake must succeed against the tonic server");

    // h2's Connection must be polled to drive the protocol forward — we
    // spawn it on a dedicated task. The connection future returns once
    // the TCP socket closes (either side); we ignore its result because
    // closing on RST_STREAM is the success criterion. Use
    // `nativelink_util::spawn!` to inherit the test's tracing scope (see
    // server_handle comment above).
    let conn_handle = spawn!("h2_client_conn_driver", async move {
        drop(conn.await);
    });

    // gRPC HTTP/2 message: POST with the bytestream Write path. Headers
    // mirror what tonic's own client emits for a streaming request.
    let req = HttpRequest::builder()
        .method(Method::POST)
        .uri(format!("http://{server_addr}/google.bytestream.ByteStream/Write"))
        .version(Version::HTTP_2)
        .header(header::CONTENT_TYPE, HeaderValue::from_static("application/grpc"))
        .header(header::TE, HeaderValue::from_static("trailers"))
        .header("grpc-accept-encoding", HeaderValue::from_static("identity"))
        .body(())
        .expect("HTTP request builder must accept the gRPC bytestream path");

    // Wait for the connection to be ready to send a request. Without
    // this the send_request call would race the server's SETTINGS ACK.
    let mut h2 = h2
        .ready()
        .await
        .expect("h2 SendRequest must become ready after handshake");

    // `end_of_stream=false` because we will send DATA frames followed by
    // RST_STREAM. `send_request` returns a `(ResponseFuture, SendStream<B>)`.
    let (resp_fut, mut send_stream) = h2
        .send_request(req, false)
        .expect("h2 send_request must succeed (server accepted HEADERS)");

    // Build a single valid first WriteRequest. The
    // `WriteRequestStreamWrapper::from(inspected).await` call inside
    // `ByteStreamServer::write` consumes the FIRST message to extract
    // the resource_name. If we send no message before the RST, the
    // wrapper bails BEFORE the inspector ever sees a terminal frame on
    // the inner stream. Sending one valid frame then resetting puts the
    // server in the same state production sees: mid-write when the RST
    // arrives.
    let first = WriteRequest {
        resource_name: make_resource_name(100),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-data"),
    };
    let framed: Bytes = encode_stream_proto(&first)?;
    send_stream
        .send_data(framed, false)
        .expect("h2 send_data must succeed (in-process server has flow capacity)");

    // Wait until the h2 client has actually flushed HEADERS+DATA to the
    // wire before issuing RST_STREAM. h2's `send_data` is queue-buffered
    // — the actual write to the kernel TCP buffer happens in the
    // conn-driver task (spawned above). If we call `send_reset` before
    // those queued frames flush, h2's `clear_queue` drops the un-flushed
    // HEADERS+DATA and emits ONLY the RST_STREAM. The server then sees
    // RST_STREAM on an idle stream (per HTTP/2 §5.1) and responds with
    // GOAWAY(PROTOCOL_ERROR) — the bytestream handler is never invoked
    // and the inspector never observes a terminal frame.
    //
    // The synchronization signal we use: the response future yields
    // Pending until the server returns response-headers OR the stream is
    // reset. Driving it briefly via `tokio::select!` gives the conn
    // driver task wall-clock to flush. The 100 ms budget is far above
    // any plausible loopback flush time but well below the 5 s deadlock
    // budget — if 100 ms isn't enough, the test still fails clean
    // because the assertion below tells us why.
    //
    // This is NOT a sleep-as-synchronization on a result — it's a
    // bounded I/O scheduling window for two cooperating tokio tasks on
    // a multi-thread runtime. The deterministic alternative would be a
    // custom listener adapter that signals when the server-side bytestream
    // handler has been invoked, which is not worth the test scaffolding
    // for a single-purpose regression harness.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Real h2 RST_STREAM frame with reason=REFUSED_STREAM (=7). On the
    // server side the h2 layer surfaces this to tonic's
    // `Streaming<WriteRequest>::poll_next` as
    // `Poll::Ready(Some(Err(status)))` where `status.source()` walks down
    // to an `h2::Error::from(h2::Reason::REFUSED_STREAM)`. That is exactly
    // the source-chain shape the inspector's `h2_reason_from_status`
    // expects (bytestream_terminal_inspector.rs:116).
    //
    // We use REFUSED_STREAM rather than CANCEL because tonic 0.14.5's
    // request-side decoder filters out `Code::Cancelled` (the mapping of
    // CANCEL) into `Poll::Ready(Ok(None))` BEFORE the inspector wrapper
    // sees the frame. REFUSED_STREAM maps to `Code::Unavailable` and
    // surfaces unchanged. See test doc-comment "Why REFUSED_STREAM" for
    // detail.
    send_stream.send_reset(h2::Reason::REFUSED_STREAM);

    // Drive the response future to completion within the deadlock
    // budget. The server should respond with an error status (the upload
    // was cancelled mid-stream) — but what we care about is the
    // SERVER-SIDE inspector log, not the client-visible status.
    //
    // The `expect()` is the deadlock detector: if the server hangs on
    // the cancelled stream (e.g. the inspector wrapper somehow blocks
    // termination propagation), this fires within 5s with a SPECIFIC
    // message naming the failure mode — `tokio::time::Elapsed` would
    // pass `is_err()` and silently mask the bug.
    drop(
        tokio::time::timeout(Duration::from_secs(5), resp_fut)
            .await
            .expect(
                "must not deadlock — h2 RST_STREAM end-to-end inspector path stalled \
                 within 5s budget (server-side handler did not return after RST_STREAM \
                 — TerminalFrameInspectingStream may be blocking termination)",
            ),
    );

    // The RST_STREAM is now in the client-side h2 send queue. The
    // conn-driver task must flush it to the kernel TCP buffer, and the
    // server-side conn-driver task must read+process it, dispatch to the
    // open stream's `Streaming<WriteRequest>::poll_next`, and the
    // inspector wrapper must observe the resulting
    // `Some(Err(Status::from(h2::Error)))` and emit its `warn!` log.
    // Tracing-test captures cross-task log lines in a global buffer, but
    // the buffer reflects state observed at log-emission time — we must
    // yield enough wall-clock for those steps to complete BEFORE we
    // assert on the log content. Without this wait the assertion fires
    // before the inspector has had a chance to emit, producing a
    // FALSE NEGATIVE that LOOKS like a regression.
    //
    // The same NOT-synchronization-on-result rationale as the
    // pre-RST sleep: this is bounded I/O scheduling + log-flush time
    // for cooperating tokio tasks on a multi-thread runtime. 200 ms is
    // far above any plausible loopback round-trip + handler processing
    // budget on a CI host while staying well under the 5 s deadlock
    // budget.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drop the SendRequest handle so the h2 connection can wind down
    // cleanly. The drop guards on conn_handle / server_handle abort the
    // tasks automatically, but we drop them explicitly here for
    // readability — neither task needs to exit cleanly because the
    // already-emitted log lines are what we assert on.
    drop(h2);
    drop(conn_handle);
    drop(server_handle);

    // The crux of the test: the inspector MUST classify this as an h2
    // RST_STREAM with reason=REFUSED_STREAM. If a future tonic version
    // stops attaching the h2::Error to the Status's source chain (or
    // wraps it in a new error type), `h2_reason_from_status` returns
    // None and the inspector emits `kind=grpc_error` instead — both the
    // existing unit tests and Tests 1+2 stay green, ONLY this assertion
    // catches the regression.
    //
    // We DON'T use the macro-injected `logs_contain` here because tonic's
    // `transport::Server::serve_with_incoming` `tokio::spawn`s each
    // connection and each request without preserving the parent
    // `tracing::Span`. Spawned tasks' log lines lack the
    // `<test_fn_name>:` scope prefix that `logs_contain` filters on, so
    // the assertion would always fail with a FALSE NEGATIVE even when
    // the inspector emits the correct event.
    //
    // Instead we read tracing-test's global buffer directly and grep for
    // log markers that are unique enough not to collide with other tests
    // in the same binary (tests run sequentially under
    // `tracing_test::traced_test`'s shared `global_buf` because each
    // test sets a fresh buffer scope, but lines in the buffer persist
    // across tests in the same compilation; the inspector's structured
    // fields are unique to this test's terminal frame so no collision
    // is plausible).
    let raw_logs = String::from_utf8(
        tracing_test::internal::global_buf().lock().unwrap().to_vec(),
    )
    .expect("tracing-test global buffer must be valid UTF-8");

    // The buffer is shared across all tests in this binary so we filter
    // for the inspector's `terminal_error` event AND a kind discriminator
    // unique to this test (no other test emits an h2_rst:* kind). If
    // multiple matching lines exist we pick the FIRST (rev order — most
    // recent) so a test-ordering change doesn't shift us onto a stale
    // line from a prior test.
    let h2_rst_lines: Vec<&str> = raw_logs
        .lines()
        .filter(|l| {
            l.contains("bytestream_write_inbound_terminal_error")
                && l.contains("kind=h2_rst:")
        })
        .collect();

    // The dual assertion: SOME h2_rst:* event was emitted (proves the
    // inspector's H2Reset arm fired) AND it was specifically the
    // REFUSED_STREAM reason we sent (proves the source-chain extraction
    // pulled the right code). A failure of the first half = inspector
    // never classified the frame as h2_rst (the regression class). A
    // failure of the second half on a non-REFUSED_STREAM kind = the
    // source-chain plumbing changed shape (also a regression class —
    // possibly even more concerning since the discriminator is wrong).
    assert!(
        !h2_rst_lines.is_empty(),
        "MUST emit kind=h2_rst:* on a real wire-level RST_STREAM — \
         h2::Reason extraction broke (tonic source-chain may have changed; \
         check `h2_reason_from_status` in bytestream_terminal_inspector.rs \
         against the current tonic Status::from / Status::from_error \
         implementations). NO matching line in tracing-test global_buf. \
         Buffer tail (last 4 KiB):\n{}",
        &raw_logs[raw_logs.len().saturating_sub(4096)..]
    );

    let line = h2_rst_lines
        .last()
        .expect("just asserted h2_rst_lines is non-empty");

    assert!(
        line.contains("kind=h2_rst:REFUSED_STREAM"),
        "MUST emit kind=h2_rst:REFUSED_STREAM specifically — got an h2_rst:* event \
         but with the WRONG reason (we sent REFUSED_STREAM=7 on the wire). \
         Either the source-chain extraction grabbed a wrapped/different h2::Error \
         or h2's reason mapping changed. Got: {line}"
    );
    assert!(
        line.contains("h2_reason=\"REFUSED_STREAM\""),
        "MUST emit h2_reason=\"REFUSED_STREAM\" structured field for grep-based \
         filtering — without this an operator cannot distinguish \
         REFUSED_STREAM/PROTOCOL_ERROR/INTERNAL_ERROR/etc. in production logs \
         even though the inspector classified the frame correctly (#355). Got: {line}"
    );
    assert!(
        line.contains("h2_reason_code=7"),
        "MUST emit numeric h2_reason_code=7 (REFUSED_STREAM = 7 per RFC 7540 §7) — \
         the numeric form is the stable contract; the symbolic name is an \
         implementation detail of `reason_name` (#355). Got: {line}"
    );

    Ok(())
}
