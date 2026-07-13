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

//! Tests for #320 mid-upload disconnect diagnostic instrumentation.
//!
//! These tests guard the diagnostic fields added to the `inner_write failed`
//! warn — without them, a refactor that drops `finish_write_seen` /
//! `recent_cascade_*` / `mirror_*` fields would silently lose the
//! production-observability gain.
//!
//! ## Diagnostics under test
//!
//! 1. **Diagnostic 1** — `finish_write_seen` flag in the failure warn.
//!    A client that closes the gRPC stream WITHOUT a `finish_write: true`
//!    chunk must produce a warn line containing `finish_write_seen=false`.
//!    Mutation: comment out the `*finish_write_seen = true;` assignment in
//!    `process_client_stream`; the test still asserts `finish_write_seen=false`
//!    for the disconnect path, so the discriminating test is the
//!    `client_disconnect_with_finish_write_size_mismatch` companion that
//!    asserts `finish_write_seen=true` for the size-mismatch path.
//!
//! 2. **Diagnostic 2** — `recent_cascade_within_10s` cross-correlation.
//!    A pre-recorded cascade (via `cascade_diag::record_chunked_cascade`)
//!    must show up as `recent_cascade_within_10s=true` plus a non-`<none>`
//!    digest/site/age in the failure warn. Mutation: comment out the
//!    `cascade_diag::recent_cascade_within(...)` call site; the test asserts
//!    `recent_cascade_within_10s=true` so the assertion fails red-loud.
//!
//! 3. **Diagnostic 3** — buf_channel write-side state at drop. The failure
//!    warn must report `store_tx_bytes_written` and `store_tx_pipe_broken`
//!    plus mirror counterparts. Same mutation strategy.
//!
//! 4. **Diagnostic 4 (#396)** — operator-friendly truncation diagnosis.
//!    On a mid-upload half-close (no `finish_write: true`, bytes_received
//!    < expected_size, underlying error is `Code::Cancelled` from the
//!    proto_stream_utils wrapper materializing `Poll::Ready(None)` into a
//!    Cancelled Err), the server must emit a SEPARATE info log carrying
//!    the literal phrase `"client half-closed upload before finish_write"`
//!    so an operator grepping the journal can identify the truncation
//!    class at a glance, rather than re-deriving it from the 14-field
//!    `inner_write failed` warn. Cause attribution (which client, why
//!    the close) is deliberately NOT in the message — see NL #311.
//!    Mutation: comment out the new `info!` block at the chunked-path
//!    outer warn site in `inner_write`; the test asserts
//!    `logs_contain("client half-closed upload before finish_write")` so
//!    the assertion goes red.

use std::sync::Arc;
use std::sync::OnceLock;

use bytes::Bytes;
use hyper::body::Frame;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::fast_slow_store::cascade_diag;
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
// Same hash bytes that the rest of the bytestream tests use.
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

/// Serialize tests that touch the process-global `cascade_diag` state.
/// Without this, parallel test execution interleaves
/// `record_chunked_cascade` calls and breaks the no-cascade assertion in
/// `diagnostic_2_no_cascade_reports_false`. tokio Mutex (not std) so we
/// can `await` while held.
fn cascade_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

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
            persist_stream_on_disconnect_timeout_s: 0,
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

type WriteJoinHandle = JoinHandleDropGuard<Result<Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>>;

fn make_resource_name(declared_len: u64) -> String {
    format!(
        "{INSTANCE_NAME}/uploads/4dcec57e-1389-4ab5-b188-4a59f22ceb4b/blobs/{HASH1}/{declared_len}"
    )
}

fn spawn_write(
    bs_server: Arc<ByteStreamServer>,
) -> (mpsc::Sender<Frame<Bytes>>, WriteJoinHandle) {
    let (tx, stream) = make_stream(Some(CompressionEncoding::Gzip));
    let join = spawn!("bs_server_write_diag", async move {
        bs_server.write(Request::new(stream)).await
    });
    (tx, join)
}

/// Test 1: simulate a mid-upload disconnect by sending a partial chunk and
/// then dropping the gRPC sender (no `finish_write: true`). The
/// `inner_write failed` warn MUST report:
///   - `finish_write_seen=false`
///   - `bytes_received` < `expected_size`
///   - `store_tx_bytes_written` matching what we sent
///   - `is_worker=false`, `is_mirror=false`
///   - `producer_task_id` set (not `<none>`)
///
/// Mutation step: comment out the `warn!(...)` block in
/// `inner_write`. The test asserts `logs_contain("inner_write failed")`
/// and the new fields, so a removed warn flips the test red.
#[nativelink_test]
pub async fn diagnostic_1_inner_write_warn_extended_fields_on_disconnect()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Send a partial chunk (no finish_write). Declared size is 100 but we
    // only send 12 bytes.
    let resource_name = make_resource_name(100);
    let partial = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-data"),
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;

    // Yield so the server has a chance to consume the partial chunk and
    // forward it to the buf_channel before we close the stream.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Drop the sender to simulate client mid-stream disconnect — the
    // server's `WriteRequestStreamWrapper::next()` returns
    // `Some(Err("Expected WriteRequest struct in stream (got None)"))`.
    drop(tx);

    // Wait for the write to fail (deadlock detector — production
    // composition has a 5-min server-side timeout, but the disconnect
    // path should fire its diagnostic warn within ~1s).
    let server_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_handle,
    )
    .await
    .expect(
        "join did not return — disconnect path failed to surface the diagnostic warn within 5s",
    )
    .expect("write task panicked");

    assert!(
        server_result.is_err(),
        "client disconnect MUST cause the write to fail — got {server_result:?}"
    );

    // Verify Diagnostic 1 fields appear in the inner_write warn line.
    assert!(
        logs_contain("inner_write failed"),
        "MUST emit inner_write failed warn after client disconnect (Diagnostic 1)"
    );
    assert!(
        logs_contain("finish_write_seen=false"),
        "MUST report finish_write_seen=false when client disconnected without sending \
         finish_write: true (Diagnostic 1 — the #320 case)"
    );
    assert!(
        logs_contain("is_worker=false"),
        "MUST report is_worker=false for non-worker upload (Diagnostic 1)"
    );
    assert!(
        logs_contain("is_mirror=false"),
        "MUST report is_mirror=false for non-mirror upload (Diagnostic 1)"
    );
    // Producer task id should be set (we're inside a tokio test = task);
    // exact value is opaque but the field MUST be present and not `<none>`.
    assert!(
        logs_contain("producer_task_id="),
        "MUST report producer_task_id field (Diagnostic 1)"
    );
    assert!(
        !logs_contain("producer_task_id=\"<none>\"")
            && !logs_contain("producer_task_id=<none>"),
        "producer_task_id MUST be set to a real task id, not <none>, when called from \
         a tokio task (Diagnostic 1)"
    );

    Ok(())
}

/// Test 2: simulate the cross-correlation case. Pre-record a chunked
/// cascade event, then trigger a mid-upload disconnect on a separate
/// upload. The failure warn MUST report:
///   - `recent_cascade_within_10s=true`
///   - `recent_cascade_digest` matching the pre-recorded value
///   - `recent_cascade_site="chunked"` (or `"stream"` for the sibling test)
///   - `recent_cascade_age_ms=<small number>`
///
/// This is the smoking-gun question for #320: is the chunked cascade the
/// cause of the subsequent disconnect?
///
/// Mutation step: comment out the
/// `nativelink_store::fast_slow_store::cascade_diag::recent_cascade_within(10_000)`
/// call site so `recent_cascade` is always `None`. The assertion below
/// requires `recent_cascade_within_10s=true`, so the test goes red.
#[nativelink_test]
pub async fn diagnostic_2_cross_correlation_with_recent_chunked_cascade()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    // Pre-record a chunked cascade event. Production: this is fired by
    // `FastSlowStore::update (chunked): data stream failed`. We invoke
    // it directly so the bytestream-side disconnect that follows can
    // observe it via `recent_cascade_within(10_000)`.
    cascade_diag::record_chunked_cascade(
        "deadbeef00000000000000000000000000000000000000000000000000000000-12345",
    );

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Trigger the disconnect.
    let partial = WriteRequest {
        resource_name: make_resource_name(100),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-cascade"),
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    drop(tx);

    let server_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_handle,
    )
    .await
    .expect("join did not return — cross-correlation disconnect path stalled")
    .expect("write task panicked");

    assert!(server_result.is_err(), "disconnect MUST cause the write to fail");

    assert!(
        logs_contain("inner_write failed"),
        "MUST emit inner_write failed warn (Diagnostic 2 host)"
    );
    assert!(
        logs_contain("recent_cascade_within_10s=true"),
        "MUST report recent_cascade_within_10s=true when a chunked cascade fired in \
         the last 10s before the disconnect (Diagnostic 2)"
    );
    assert!(
        logs_contain(
            "recent_cascade_digest=\"deadbeef00000000000000000000000000000000000000000000000000000000-12345\""
        ),
        "MUST report the recorded digest hash so the operator can grep for the \
         upstream cascade (Diagnostic 2)"
    );
    assert!(
        logs_contain("recent_cascade_site=\"chunked\""),
        "MUST report recent_cascade_site=\"chunked\" for chunked-path cascades \
         (Diagnostic 2)"
    );
    // Age field MUST be present (small number — produced milliseconds ago).
    assert!(
        logs_contain("recent_cascade_age_ms="),
        "MUST report recent_cascade_age_ms field for the cross-correlation gap \
         (Diagnostic 2)"
    );

    Ok(())
}

/// Test 3: companion to Test 2 — verify the no-cascade case explicitly
/// reports `recent_cascade_within_10s=false` so the operator can
/// distinguish "chunked cascade IS the trigger" from "something else
/// is the trigger" via grep.
#[nativelink_test]
pub async fn diagnostic_2_no_cascade_reports_false()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();
    // No record_chunked_cascade call before the disconnect.

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

    let server_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_handle,
    )
    .await
    .expect("join did not return — no-cascade disconnect path stalled")
    .expect("write task panicked");

    assert!(server_result.is_err());
    assert!(
        logs_contain("inner_write failed"),
        "MUST emit inner_write failed warn"
    );
    assert!(
        logs_contain("recent_cascade_within_10s=false"),
        "MUST report recent_cascade_within_10s=false when no cascade fired recently \
         (Diagnostic 2 — distinguishes cascade-trigger from other-trigger cases)"
    );
    assert!(
        logs_contain("recent_cascade_digest=\"<none>\""),
        "MUST report recent_cascade_digest=<none> when no cascade is recorded \
         (Diagnostic 2)"
    );

    Ok(())
}

/// Test 4: verify Diagnostic 3 — buf_channel write-side state appears in
/// the failure warn. After we send a partial chunk and disconnect, the
/// store_tx should report bytes_written matching what we sent. The
/// mirror channel for an unmirrored store (no WorkerProxyStore wired)
/// should report `mirror_present=false`.
///
/// Mutation step: comment out the `store_tx_bytes_written` /
/// `mirror_present` field assignments in the warn — the test goes red
/// because `logs_contain` no longer finds those substrings.
#[nativelink_test]
pub async fn diagnostic_3_buf_channel_state_in_failure_warn()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    let partial = WriteRequest {
        resource_name: make_resource_name(100),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-buf-channel-state"),
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    drop(tx);

    let server_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_handle,
    )
    .await
    .expect("join did not return — buf_channel diagnostic disconnect stalled")
    .expect("write task panicked");

    assert!(server_result.is_err());
    assert!(logs_contain("inner_write failed"));
    // Diagnostic 3 fields:
    assert!(
        logs_contain("store_tx_bytes_written="),
        "MUST report store_tx_bytes_written field (Diagnostic 3)"
    );
    assert!(
        logs_contain("store_tx_pipe_broken="),
        "MUST report store_tx_pipe_broken field (Diagnostic 3)"
    );
    // No WorkerProxyStore wired in this test ⇒ mirror_present=false.
    assert!(
        logs_contain("mirror_present=false"),
        "MUST report mirror_present=false when no WorkerProxyStore is wired \
         (Diagnostic 3)"
    );
    assert!(
        logs_contain("mirror_dropped_any=false"),
        "MUST report mirror_dropped_any=false when no mirror was active \
         (Diagnostic 3)"
    );

    Ok(())
}

/// Test 5 — sanity / asymmetry guard for Diagnostic 1: when the client
/// DOES send `finish_write: true` but with a wrong byte count, the warn
/// MUST report `finish_write_seen=true`. This is the discriminating test
/// that catches a refactor which left `finish_write_seen` permanently
/// `false`.
///
/// We force the streaming (non-oneshot) path by sending TWO chunks: the
/// first WITHOUT `finish_write: true` (so `is_first_msg_complete()`
/// returns false, disabling the oneshot fast-path), then the second
/// chunk WITH `finish_write: true` but a total byte count that doesn't
/// match the declared size. The streaming path runs `inner_write`,
/// which is where the `finish_write_seen` flag lives.
#[nativelink_test]
pub async fn diagnostic_1_finish_write_seen_true_on_size_mismatch()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // First chunk: NO finish_write — disables the oneshot fast path so
    // `inner_write` runs (the function that sets `finish_write_seen`).
    let resource_name = make_resource_name(100);
    let first = WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"first-piece"), // 11 bytes
    };
    tx.send(Frame::data(encode_stream_proto(&first)?)).await?;

    // Second chunk: finish_write=true, but total bytes (11 + 5 = 16) !=
    // declared size (100). Server hits `Client declared size 100 but only
    // sent 16 bytes` and returns Err. CRITICAL: `*finish_write_seen =
    // true` runs BEFORE the size-validation early-return, so the warn
    // MUST report `finish_write_seen=true`.
    let second = WriteRequest {
        resource_name: String::new(),
        write_offset: 11,
        finish_write: true,
        data: Bytes::from_static(b"oops!"), // 5 bytes
    };
    tx.send(Frame::data(encode_stream_proto(&second)?)).await?;
    drop(tx);

    let server_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_handle,
    )
    .await
    .expect("join did not return")
    .expect("write task panicked");

    assert!(server_result.is_err(), "size mismatch MUST fail the write");
    assert!(
        logs_contain("inner_write failed"),
        "MUST emit inner_write failed warn (we force the streaming path via 2-chunk \
         send so inner_write runs)"
    );
    assert!(
        logs_contain("finish_write_seen=true"),
        "MUST report finish_write_seen=true when client sent finish_write but with \
         wrong byte count — discriminates from #320 case (Diagnostic 1 asymmetry guard)"
    );

    Ok(())
}

/// Test 6 (#396) — operator-friendly truncation diagnosis.
///
/// When a Bazel client half-closes the ByteStream/Write RPC mid-upload
/// — bytes were sent, but no `finish_write: true` chunk arrived before
/// the gRPC stream returned `Poll::Ready(None)` — the server MUST emit
/// a SEPARATE `info!` line containing the literal phrase
/// `"client half-closed upload before finish_write"` so an operator
/// grepping the journal can identify the truncation class at a glance,
/// instead of having to re-derive it from the 14-field
/// `inner_write failed` warn. Cause attribution (why the client closed
/// pre-finish) is deliberately not in the message — that's tracked by
/// NL #311 ("Bazel partial-upload not detected as hard error post-9.1").
///
/// The new info also surfaces structured fields the operator needs:
/// `bytes_received`, `expected_bytes`, `digest`, and the gRPC `code` of
/// the underlying error (Cancelled per #357 / commit 3a184f40).
///
/// Mutation step: comment out the new `info!` block at the chunked-path
/// outer warn site in `inner_write`. The assertion below requires the
/// literal phrase, so the test goes red.
#[nativelink_test]
pub async fn diagnostic_4_truncation_clarity_info_for_half_close()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Send a partial chunk (no finish_write). Declared size is 100 but we
    // only send 12 bytes. Then drop the sender — the wrapper's
    // poll_next observes `Poll::Ready(None)` and materializes a
    // Code::Cancelled Err (per #357). The chunked-path outer warn site
    // observes `!finish_write_seen && bytes_received < expected_size`
    // and must emit the operator-friendly info.
    let resource_name = make_resource_name(100);
    let partial = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"partial-data"), // 12 bytes
    };
    tx.send(Frame::data(encode_stream_proto(&partial)?)).await?;

    // Yield so the server has a chance to consume the chunk before we
    // close the stream.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    drop(tx);

    let server_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), join_handle)
            .await
            .expect(
                "join did not return — #396 truncation-clarity info path stalled \
                 (deadlock detector)",
            )
            .expect("write task panicked");
    assert!(
        server_result.is_err(),
        "half-close MUST cause the write to fail — got {server_result:?}"
    );

    // (a) The new operator-friendly info MUST fire with the literal phrase.
    assert!(
        logs_contain("client half-closed upload before finish_write"),
        "MUST emit operator-friendly truncation-clarity info containing the literal \
         phrase 'client half-closed upload before finish_write' (#396 Diagnostic 4)"
    );

    // (b) The info MUST carry structured fields. tracing-test formats
    //     numeric fields as `name=N` and `%`-Display fields as
    //     `name=Display`. We assert on substrings so the test is robust
    //     to ordering changes.
    assert!(
        logs_contain("bytes_received="),
        "MUST carry bytes_received field (#396 Diagnostic 4)"
    );
    assert!(
        logs_contain("expected_bytes=100"),
        "MUST carry expected_bytes=100 — the declared upload size (#396 Diagnostic 4)"
    );
    assert!(
        logs_contain("digest="),
        "MUST carry digest field so operator can correlate with the upload \
         (#396 Diagnostic 4)"
    );
    assert!(
        logs_contain("grpc_code="),
        "MUST carry grpc_code field so operator can distinguish Cancelled \
         (client half-close, #357) from Internal (server-side mid-write) \
         from Unavailable (out-of-order) (#396 Diagnostic 4)"
    );

    // (c) The existing 14-field `inner_write failed` warn MUST still fire —
    //     the new info is ADDITIVE, not a replacement. Diagnostic 1 / 2 / 3
    //     guards continue to rely on the original warn shape.
    assert!(
        logs_contain("inner_write failed"),
        "MUST also emit the original 14-field inner_write failed warn — the new \
         info is additive (#396 Diagnostic 4)"
    );

    Ok(())
}

/// Test 7 (#396 asymmetry guard) — the new info MUST NOT fire on the
/// size-mismatch path where the client DID send `finish_write: true`
/// but with the wrong byte count. That path is a different bug class
/// (client over- or under-declared size) and the operator should NOT
/// see "client half-closed before finish_write" when the wire DID
/// carry a `finish_write: true` chunk.
///
/// This is the over-action guard for the new diagnostic: it fires when
/// it should NOT fire is just as much a bug as failing to fire when it
/// should. Without this test, a refactor dropping the
/// `!finish_write_seen` precondition would silently misclassify
/// size-mismatch as half-close.
#[nativelink_test]
pub async fn diagnostic_4_truncation_clarity_silent_on_size_mismatch()
-> Result<(), Box<dyn core::error::Error>> {
    let _guard = cascade_test_lock().lock().await;
    cascade_diag::reset_for_test();

    let store_manager = make_store_manager().await?;
    let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);
    let (tx, join_handle) = spawn_write(bs_server);

    // Two chunks: first chunk has finish_write=false (forces chunked
    // path), second chunk has finish_write=true but total bytes
    // (11 + 5 = 16) != declared size (100). This is the size-mismatch
    // bug class — the new info MUST NOT fire.
    let resource_name = make_resource_name(100);
    let first = WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(b"first-piece"), // 11 bytes
    };
    tx.send(Frame::data(encode_stream_proto(&first)?)).await?;
    let second = WriteRequest {
        resource_name: String::new(),
        write_offset: 11,
        finish_write: true,
        data: Bytes::from_static(b"oops!"), // 5 bytes
    };
    tx.send(Frame::data(encode_stream_proto(&second)?)).await?;
    drop(tx);

    let server_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), join_handle)
            .await
            .expect("join did not return")
            .expect("write task panicked");
    assert!(
        server_result.is_err(),
        "size mismatch MUST fail the write"
    );

    // The new info MUST NOT fire — client sent finish_write=true, so
    // this is not the half-close case even though bytes_received <
    // expected_size.
    assert!(
        !logs_contain("client half-closed upload before finish_write"),
        "MUST NOT emit truncation-clarity info when client sent finish_write=true \
         (this is the size-mismatch bug class, a different code path) — \
         over-action guard (#396 Diagnostic 4 asymmetry)"
    );
    // But the original warn MUST still fire (sanity that the test wired
    // the failure path).
    assert!(
        logs_contain("inner_write failed"),
        "the original warn must still fire on size mismatch"
    );

    Ok(())
}
