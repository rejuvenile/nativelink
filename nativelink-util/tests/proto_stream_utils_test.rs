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

use core::sync::atomic::Ordering;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_util::common::DigestInfo;
use nativelink_util::proto_stream_utils::{
    GRPC_WRITE_SLOW_CHUNK_TOTAL, WriteRequestStreamWrapper, WriteState, WriteStateWrapper,
};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio_stream::wrappers::UnboundedReceiverStream;

const INSTANCE_NAME: &str = "test-instance";

// Regression test for TraceMachina/nativelink#745.
#[nativelink_test]
async fn ensure_no_errors_if_only_first_message_has_resource_name_set() -> Result<(), Error> {
    const RAW_DATA: &str = "thisdatafoo";
    const DIGEST: DigestInfo = DigestInfo::new([0u8; 32], RAW_DATA.len() as u64);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();

    let message1 = WriteRequest {
        resource_name: format!(
            "{INSTANCE_NAME}/uploads/some-uuid/blobs/{}/{}",
            DIGEST.packed_hash(),
            DIGEST.size_bytes()
        ),
        write_offset: 0,
        finish_write: false,
        data: Bytes::from_static(&RAW_DATA.as_bytes()[..4]),
    };
    let message2 = WriteRequest {
        resource_name: String::new(),
        write_offset: 4,
        finish_write: false,
        data: Bytes::from_static(&RAW_DATA.as_bytes()[4..8]),
    };
    let message3 = WriteRequest {
        resource_name: String::new(),
        write_offset: 8,
        finish_write: true,
        data: Bytes::from_static(&RAW_DATA.as_bytes()[8..]),
    };

    {
        tx.send(Ok(message1.clone())).unwrap();
        tx.send(Ok(message2.clone())).unwrap();
        tx.send(Ok(message3.clone())).unwrap();
        drop(tx); // Close the channel.
    }

    let local_state = Arc::new(Mutex::new(WriteState::with_progress_timeout(
        INSTANCE_NAME.to_string(),
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?,
        Duration::ZERO,
    )));
    let mut write_state_wrapper = WriteStateWrapper::new(local_state.clone());

    {
        // Ensure we transported our data properly.
        assert_eq!(write_state_wrapper.next().await, Some(message1));
        assert_eq!(write_state_wrapper.next().await, Some(message2));
        assert_eq!(write_state_wrapper.next().await, Some(message3));
        assert_eq!(write_state_wrapper.next().await, None);

        // Ensure no stream errors were set.
        assert_eq!(local_state.lock().take_read_stream_error(), None);
    }

    Ok(())
}

// Regression test: a resumed/replayed upload (Bazel client retry, or
// GrpcStore::write Retrier replaying WriteState::cached_messages after a
// transport error) starts a fresh wrapper with bytes_received=0 but the
// cached WriteRequests carry their original write_offset. The high-watermark
// accumulator must tolerate the replayed prefix without spuriously rejecting
// the upload as oversize.
#[nativelink_test]
async fn replayed_prefix_does_not_trip_overrun_check() -> Result<(), Error> {
    // 100-byte logical upload split into two 50-byte chunks, then the same
    // two chunks replayed at their original offsets.
    const TOTAL_LEN: usize = 100;
    const DIGEST: DigestInfo = DigestInfo::new([0u8; 32], TOTAL_LEN as u64);
    let payload = vec![0u8; TOTAL_LEN];

    let resource_name = format!(
        "{INSTANCE_NAME}/uploads/some-uuid/blobs/{}/{}",
        DIGEST.packed_hash(),
        DIGEST.size_bytes()
    );

    let make_chunk = |offset: i64, range: core::ops::Range<usize>, finish: bool| WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: offset,
        finish_write: finish,
        data: Bytes::copy_from_slice(&payload[range]),
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(make_chunk(0, 0..50, false))).unwrap();
    tx.send(Ok(make_chunk(50, 50..100, false))).unwrap();
    // Replayed prefix - overlaps fully with what was already received.
    tx.send(Ok(make_chunk(0, 0..50, false))).unwrap();
    tx.send(Ok(make_chunk(50, 50..100, true))).unwrap();
    drop(tx);

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    // All four messages must be yielded without an overrun error.
    for _ in 0..4 {
        let next = wrapper.next().await.expect("expected a message");
        next.expect("replayed prefix must not be rejected as overrun");
    }
    // Stream EOF; bytes_received should equal expected_size since the last
    // chunk's high-watermark covered the full range.
    assert!(wrapper.next().await.is_none());
    assert_eq!(wrapper.bytes_received, TOTAL_LEN);

    Ok(())
}

// Control: a genuine overrun (chunks whose combined coverage exceeds the
// declared size) must still be rejected.
#[nativelink_test]
async fn genuine_overrun_is_still_rejected() -> Result<(), Error> {
    const EXPECTED_LEN: usize = 100;
    const OVERRUN_LEN: usize = 120;
    const DIGEST: DigestInfo = DigestInfo::new([0u8; 32], EXPECTED_LEN as u64);
    let payload = vec![0u8; OVERRUN_LEN];

    let resource_name = format!(
        "{INSTANCE_NAME}/uploads/some-uuid/blobs/{}/{}",
        DIGEST.packed_hash(),
        DIGEST.size_bytes()
    );

    let make_chunk = |offset: i64, range: core::ops::Range<usize>, finish: bool| WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: offset,
        finish_write: finish,
        data: Bytes::copy_from_slice(&payload[range]),
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(make_chunk(0, 0..60, false))).unwrap();
    tx.send(Ok(make_chunk(60, 60..120, true))).unwrap();
    drop(tx);

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    // First chunk fits within the declared size.
    let first = wrapper.next().await.expect("expected first message");
    first.expect("first chunk must not be rejected");

    // Second chunk pushes the high-watermark to 120, beyond the 100-byte
    // declared size, and must be rejected.
    let second = wrapper.next().await.expect("expected second message");
    let err = second.expect_err("genuine overrun must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("sent too much data"),
        "expected overrun error, got: {msg}"
    );
    assert!(msg.contains("expected=100"), "missing expected= field: {msg}");
    assert!(
        msg.contains("write_offset=60"),
        "missing write_offset= field: {msg}"
    );
    assert!(msg.contains("chunk_len=60"), "missing chunk_len= field: {msg}");
    assert!(
        msg.contains("bytes_received=120"),
        "missing bytes_received= field: {msg}"
    );

    Ok(())
}

// Per-chunk progress timeout tests for WriteStateWrapper.
//
// The whole-RPC `tokio::time::timeout` previously wrapped GrpcStore::write
// killed slow-but-progressing mirror writes. The replacement is a
// per-chunk no-progress timer enforced inside WriteStateWrapper, configured
// via WriteState::with_progress_timeout.

const HASH_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn chunk(
    offset: i64,
    data: &'static [u8],
    finish: bool,
    expected_size: Option<usize>,
) -> WriteRequest {
    let resource_name = if let Some(size) = expected_size {
        format!("{INSTANCE_NAME}/uploads/some-uuid/blobs/{HASH_HEX}/{size}")
    } else {
        String::new()
    };
    WriteRequest {
        resource_name,
        write_offset: offset,
        finish_write: finish,
        data: Bytes::from_static(data),
    }
}

async fn make_state(
    rx: tokio::sync::mpsc::UnboundedReceiver<Result<WriteRequest, Error>>,
    progress_timeout: Duration,
) -> Result<Arc<Mutex<WriteState<UnboundedReceiverStream<Result<WriteRequest, Error>>, Error>>>, Error>
{
    let wrapper = WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;
    Ok(Arc::new(Mutex::new(WriteState::with_progress_timeout(
        INSTANCE_NAME.to_string(),
        wrapper,
        progress_timeout,
    ))))
}

/// Slow-but-progressing producer: chunks arrive every 5s for 30s total
/// (6 chunks). Per-chunk progress timeout of 15s must NOT fire — the
/// transport is making forward progress, just slowly. This is the exact
/// mirror-write scenario the whole-RPC 15s deadline was killing on
/// 2026-04-23.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timeout_allows_slow_but_progressing_producer()
-> Result<(), Error> {
    // 6 chunks × 4 bytes = 24 bytes total payload.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    // Send the first chunk synchronously so WriteRequestStreamWrapper::from
    // can extract resource_info without blocking on time advancement.
    tx.send(Ok(chunk(0, b"data", false, Some(24)))).unwrap();
    let state = make_state(rx, Duration::from_secs(15)).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    // Spawn a producer that emits the remaining 5 chunks at 5s intervals.
    let producer = tokio::spawn(async move {
        for i in 1..6 {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let finish = i == 5;
            tx.send(Ok(chunk(i64::from(i) * 4, b"data", finish, None)))
                .unwrap();
        }
        drop(tx);
    });

    let mut received = 0;
    while let Some(msg) = wrapper.next().await {
        received += 1;
        if msg.finish_write {
            break;
        }
    }
    producer.await.unwrap();

    // Drain the EOF after finish_write.
    assert_eq!(wrapper.next().await, None);
    assert_eq!(received, 6, "expected 6 chunks delivered without timeout");
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "no progress timeout should have fired",
    );
    Ok(())
}

/// Stuck producer: emits chunks then stops sending for >3× the timer
/// budget. Per the **2026-05-14 diagnostic-only conversion**, the
/// per-chunk progress timer must (a) emit `warn!` lines + bump the
/// process-wide [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] counter at each
/// `progress_timeout` boundary, and (b) NOT terminate the stream, NOT
/// set `read_stream_error`, NOT abort the gRPC RPC. Once the producer
/// resumes, the upload must complete cleanly.
///
/// **Mutation step:** restoring the abort-on-elapse behaviour
/// (`local_state.read_stream_error = Some(...); Poll::Ready(None)`)
/// must red-fail this test with the bespoke
/// "WriteState progress timer must NOT abort the stream" message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timer_warns_but_does_not_abort_on_threshold()
-> Result<(), Error> {
    let baseline = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);

    // 4-chunk × 4-byte upload, total 16 bytes.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(16)))).unwrap();
    tx.send(Ok(chunk(4, b"data", false, None))).unwrap();

    // 10-second progress budget; producer goes silent for ~33 s
    // (3.3× the budget) before delivering the rest. Old behaviour:
    // wrapper terminates with DeadlineExceeded after 10 s. New
    // behaviour: 3 warns emitted, stream stays alive, upload finishes.
    let state = make_state(rx, Duration::from_secs(10)).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    // Drain the two pre-sent chunks.
    let first = wrapper.next().await.expect("first chunk");
    assert_eq!(first.write_offset, 0);
    let second = wrapper.next().await.expect("second chunk");
    assert_eq!(second.write_offset, 4);

    // Producer sleeps 33s then sends the final two chunks.
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(33)).await;
        tx.send(Ok(chunk(8, b"data", false, None))).unwrap();
        tx.send(Ok(chunk(12, b"data", true, None))).unwrap();
        drop(tx);
    });

    // Drain the final two chunks. With the OLD abort-on-elapse
    // behaviour, the wrapper would have returned None after ~10s and
    // these `expect` calls would panic — that is the mutation
    // signature the bespoke message documents.
    let third = wrapper
        .next()
        .await
        .expect(
            "WriteState progress timer must NOT abort the stream — \
             diagnostic-only per 2026-05-14",
        );
    assert_eq!(third.write_offset, 8, "third chunk offset wrong");
    let fourth = wrapper.next().await.expect("fourth chunk");
    assert_eq!(fourth.write_offset, 12, "fourth chunk offset wrong");
    assert!(fourth.finish_write, "fourth chunk should carry finish_write");

    // Drain EOF.
    assert_eq!(wrapper.next().await, None);
    producer.await.unwrap();

    // Diagnostic-only side effects:
    //   1. `read_stream_error` must NEVER be set by the diagnostic
    //      timer (only the resource_name parse error path sets it).
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "diagnostic-only timer must NOT record read_stream_error",
    );
    //   2. The process-wide counter must have advanced. With a 10 s
    //      budget and ~33 s gap we expect ≥3 increments (one per
    //      window boundary crossed).
    let after = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);
    let delta = after.saturating_sub(baseline);
    assert!(
        delta >= 3,
        "expected ≥3 slow-chunk counter increments (one per window), got {delta} \
         (baseline={baseline}, after={after})",
    );
    //   3. The per-event warn must have fired — captured by
    //      `tracing_test::traced_test` wired by `#[nativelink_test]`.
    assert!(
        logs_contain("GrpcStore::write made no progress"),
        "expected per-event diagnostic warn line",
    );
    Ok(())
}

/// Each delivered chunk resets the timer — a producer pacing chunks 14s
/// apart (just under the 15s budget) must succeed across multiple chunks.
/// Guards against an off-by-one where the timer is armed once and never
/// reset.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timeout_resets_on_each_chunk() -> Result<(), Error> {
    // 4 chunks × 4 bytes = 16 bytes total payload.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(16)))).unwrap();
    let state = make_state(rx, Duration::from_secs(15)).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    let producer = tokio::spawn(async move {
        for i in 1..4 {
            tokio::time::sleep(Duration::from_secs(14)).await;
            let finish = i == 3;
            tx.send(Ok(chunk(i64::from(i) * 4, b"data", finish, None)))
                .unwrap();
        }
        drop(tx);
    });

    let mut received = 0;
    while let Some(msg) = wrapper.next().await {
        received += 1;
        if msg.finish_write {
            break;
        }
    }
    producer.await.unwrap();

    assert_eq!(wrapper.next().await, None);
    assert_eq!(received, 4, "expected 4 chunks delivered (timer reset each chunk)");
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "timer must reset per chunk; no error expected",
    );
    Ok(())
}

/// A producer that sends a single `finish_write=true` chunk and immediately
/// drops the channel must complete cleanly: the per-chunk timer must not
/// fire even if wall-clock time is later advanced past the timeout window.
/// (`write_finished` short-circuits `WriteRequestStreamWrapper::poll_next`
/// to `Ready(None)` before the inner stream is polled again, so the timer
/// is never armed for a chunk that will never come.)
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timeout_does_not_fire_on_immediate_eof()
-> Result<(), Error> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", true, Some(4)))).unwrap();
    drop(tx);

    let state = make_state(rx, Duration::from_secs(15)).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    let first = wrapper.next().await.expect("single chunk");
    assert!(first.finish_write, "chunk must carry finish_write=true");
    assert_eq!(wrapper.next().await, None, "EOF immediately after finish_write");

    // Advance well beyond the 15s budget: nothing should happen because the
    // wrapper has already emitted its final EOF.
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "no progress timeout should fire after clean EOF",
    );
    Ok(())
}

/// `progress_timeout = Duration::ZERO` is the documented "no timer"
/// configuration. Even when the producer stalls indefinitely and wall-clock
/// time advances by a large amount, no `read_stream_error` may be set.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timeout_disabled_when_zero() -> Result<(), Error> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(8)))).unwrap();

    let state = make_state(rx, Duration::ZERO).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    let first = wrapper.next().await.expect("first chunk");
    assert_eq!(first.write_offset, 0);

    // Race the wrapper's next poll against an aggressive time advance. The
    // wrapper must remain Pending — it can only resolve when a new chunk
    // arrives or the channel closes.
    let next_fut = wrapper.next();
    tokio::pin!(next_fut);
    tokio::select! {
        biased;
        () = async {
            // Walk forward 1 hour in 1-minute steps. With timer disabled
            // there is nothing armed to fire on these advances.
            for _ in 0..60 {
                tokio::time::advance(Duration::from_secs(60)).await;
            }
        } => {}
        msg = &mut next_fut => panic!("unexpected wrapper message with timer disabled: {msg:?}"),
    }

    // After 1h of silence, deliver the final chunk so the wrapper drains
    // cleanly. This also confirms the wrapper was genuinely just waiting.
    tx.send(Ok(chunk(4, b"data", true, None))).unwrap();
    drop(tx);
    let last = (&mut next_fut).await.expect("final chunk after long stall");
    assert!(last.finish_write);
    assert_eq!(wrapper.next().await, None);
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "disabled timer must never fire",
    );
    Ok(())
}

/// After a transport error and `WriteState::resume`, the per-chunk
/// timer must re-arm correctly. Drain the resume_queue (cached chunks
/// replayed without consulting the inner stream) — the diagnostic
/// timer must NOT fire spuriously during the cached replay (no warn,
/// no counter bump). Then a stall on the inner stream must produce a
/// diagnostic warn + counter bump, but the wrapper must NOT terminate.
///
/// Guards against (a) a stale `Sleep` from the previous attempt firing
/// spuriously during the cached replay, and (b) the timer never
/// re-arming once the resume_queue drains.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_diagnostic_survives_resume() -> Result<(), Error> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(20)))).unwrap();
    tx.send(Ok(chunk(4, b"data", false, None))).unwrap();
    let state = make_state(rx, Duration::from_secs(15)).await?;
    {
        let mut wrapper = WriteStateWrapper::new(state.clone());
        // Drain both chunks into cached_messages. (cached_messages is a
        // 2-slot ring filled by push_message on every successful chunk.)
        assert!(wrapper.next().await.is_some());
        assert!(wrapper.next().await.is_some());
        // Drop the wrapper — simulate the GrpcStore::write retry loop
        // discarding the in-flight ByteStream client mid-RPC.
    }

    // Caller-side resume: equivalent to GrpcStore::write's retry path
    // calling `local_state_locked.resume()` after a transport-level error.
    state.lock().resume();

    let after_resume = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);

    let mut wrapper = WriteStateWrapper::new(state.clone());
    // resume_queue replays the two cached chunks without polling the
    // inner stream, so the per-chunk timer must NOT fire on these even
    // if a stale `Sleep` was carried over from the previous attempt.
    tokio::time::advance(Duration::from_secs(20)).await;
    let r0 = wrapper.next().await.expect("replayed chunk 0");
    assert_eq!(r0.write_offset, 0);
    let r1 = wrapper.next().await.expect("replayed chunk 1");
    assert_eq!(r1.write_offset, 4);
    // After the cached replay, no slow-chunk events should have
    // accrued (the resumed_message branch returns before the timer
    // logic).
    assert_eq!(
        GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed),
        after_resume,
        "diagnostic timer must NOT fire during cached replay",
    );

    // Now race the next poll (which will be Pending — tx is alive but
    // silent) against a long time advance and verify that:
    //   - the wrapper stays alive (does NOT return None)
    //   - the counter advances (diagnostic warn + bump)
    //   - read_stream_error stays unset
    let next_fut = wrapper.next();
    tokio::pin!(next_fut);
    tokio::select! {
        biased;
        () = async {
            // Walk forward 50s — 3+ windows of the 15s budget.
            for _ in 0..50 {
                tokio::time::advance(Duration::from_secs(1)).await;
            }
        } => {}
        msg = &mut next_fut => panic!(
            "WriteState progress timer must NOT abort the stream — \
             diagnostic-only per 2026-05-14 (post-resume); got msg={msg:?}",
        ),
    }

    let after_stall = GRPC_WRITE_SLOW_CHUNK_TOTAL.load(Ordering::Relaxed);
    assert!(
        after_stall.saturating_sub(after_resume) >= 3,
        "expected ≥3 diagnostic increments after 50s of silence with 15s window; \
         baseline={after_resume} after={after_stall}",
    );
    assert!(
        state.lock().take_read_stream_error().is_none(),
        "diagnostic-only timer must NOT record read_stream_error post-resume",
    );

    // Drop tx so the future can resolve cleanly without further data;
    // `next_fut` will see channel-EOF (a Cancelled error per #357).
    drop(tx);
    let resolved = (&mut next_fut).await;
    // Some(Err) (Cancelled — no finish_write) or Some(Ok) is permitted;
    // None is permitted iff write_finished was set. The crucial check
    // above already proved the diagnostic timer didn't kill us; here we
    // just keep the test deterministic so it doesn't hang.
    drop(resolved);
    Ok(())
}

// --------------------------------------------------------------------
// #357: clean client half-close mid-stream MUST surface as
// `Code::Cancelled`, NOT `Code::InvalidArgument`.
//
// Background: Bazel's DynamicSpawnStrategy normally cancels in-flight
// ByteStream uploads via clean half-close (HTTP/2 END_STREAM with no
// preceding error frame) when the local-execution branch wins the
// race against remote. The previous `make_input_err!` at the
// "got None" branch surfaced this Bazel-by-design cancellation as
// `InvalidArgument`, which Bazel's status classifier treats as a
// fatal protocol error rather than a retryable cancellation,
// polluting build logs with thousands of misleading errors per CI run
// (#353 RCA).
//
// The discriminator is `!self.write_finished`: when the inner stream
// returns `None` and the wrapper has not yet observed
// `finish_write: true` from the client, the upload was abandoned
// mid-flight (whether mid-data or pre-data — both are client choices,
// not protocol errors). The genuine `InvalidArgument` cases — byte
// overrun, finish_write+wrong-size — flow through different branches
// (the `error_if!` size check at the `write_finished` short-circuit,
// and the explicit overrun check) and are covered by the
// `genuine_overrun_is_still_rejected` and
// `client_finish_write_true_with_byte_mismatch_returns_invalid_argument`
// tests respectively.
//
// Mutation step: revert the fix in
// `nativelink-util/src/proto_stream_utils.rs` to `make_input_err!`
// for the "got None" branch. Both
// `client_half_close_*_returns_cancelled` tests must red-fail with
// the bespoke `expect()` messages below.
// --------------------------------------------------------------------

/// Bazel half-closes mid-upload (sends N data chunks, then END_STREAM
/// with N < expected_size) when the DynamicSpawnStrategy local branch
/// wins. The wrapper MUST surface this as `Code::Cancelled` — not
/// `Code::InvalidArgument` — so Bazel's gRPC status classifier treats
/// it as the by-design cancellation it is, not a fatal protocol
/// error.
#[nativelink_test]
async fn client_half_close_mid_stream_returns_cancelled() -> Result<(), Error> {
    // Declare a 24-byte upload, send only 8 bytes (2 chunks × 4),
    // then close cleanly without finish_write=true.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(24)))).unwrap();
    tx.send(Ok(chunk(4, b"data", false, None))).unwrap();
    drop(tx); // Clean half-close — no error frame, no finish_write.

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    // Drain the two delivered chunks.
    wrapper.next().await.expect("first chunk").expect("first chunk ok");
    wrapper.next().await.expect("second chunk").expect("second chunk ok");

    // The next poll observes the inner-stream EOF without
    // finish_write=true having been seen — this is the #353 case.
    let next = wrapper.next().await.expect("EOF must surface as Some(Err) not None");
    let err = next.expect_err(
        "clean client half-close mid-stream MUST surface as Cancelled (not InvalidArgument) — see #353 RCA",
    );
    assert_eq!(
        err.code,
        Code::Cancelled,
        "client half-close mid-stream MUST be Cancelled (not {:?}) so Bazel treats it as retryable cancellation, not a fatal protocol error — see #357",
        err.code,
    );

    Ok(())
}

/// Bazel cancels the upload before sending any data chunk (only the
/// resource-name first message arrived). Same discriminator
/// (`!write_finished`) — same verdict (`Code::Cancelled`).
#[nativelink_test]
async fn client_half_close_before_any_data_returns_cancelled() -> Result<(), Error> {
    // First message carries the resource name (with `data` empty) so
    // `from()` can extract resource_info. Then the client closes
    // without ever sending data or finish_write=true.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"", false, Some(24)))).unwrap();
    drop(tx);

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    // The first message (cached in `first_msg`) is yielded.
    let first = wrapper.next().await.expect("first message").expect("first ok");
    assert_eq!(first.write_offset, 0);

    // The next poll falls through to the inner stream which is
    // already at EOF — same "got None" branch, same Cancelled
    // verdict.
    let next = wrapper.next().await.expect("EOF must surface as Some(Err) not None");
    let err = next.expect_err(
        "clean client half-close mid-stream MUST surface as Cancelled (not InvalidArgument) — see #353 RCA",
    );
    assert_eq!(
        err.code,
        Code::Cancelled,
        "pre-data client half-close MUST be Cancelled (not {:?}) — Bazel cancelling before sending data is still a cancellation, see #357",
        err.code,
    );

    Ok(())
}

/// Control: a well-formed upload (finish_write=true, bytes match
/// declared size) must yield clean EOF (`None`) without any error.
/// Guards against a regression that collapses Case D into the
/// Cancelled branch.
#[nativelink_test]
async fn client_finish_write_true_with_complete_bytes_succeeds() -> Result<(), Error> {
    // 24-byte upload, 6 chunks × 4 bytes, last chunk carries
    // finish_write=true and bytes_received hits expected_size.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(24)))).unwrap();
    for i in 1..5 {
        tx.send(Ok(chunk(i64::from(i) * 4, b"data", false, None))).unwrap();
    }
    tx.send(Ok(chunk(20, b"data", true, None))).unwrap();
    drop(tx);

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    for _ in 0..6 {
        wrapper
            .next()
            .await
            .expect("chunk must yield")
            .expect("chunk must succeed");
    }
    // After finish_write=true the wrapper short-circuits with
    // Poll::Ready(None) on the next poll, signalling clean EOF.
    assert!(
        wrapper.next().await.is_none(),
        "complete upload MUST yield clean None EOF, not an error — Case D",
    );

    Ok(())
}

/// Control: `finish_write=true` but `bytes_received != expected_size`.
/// This is Case E — the genuine `InvalidArgument` case (client claimed
/// done but byte count mismatched) — and MUST remain
/// `Code::InvalidArgument`. Without this control test, a future
/// change could collapse all "got None"-shaped paths into Cancelled
/// and silently strip the size-validation contract.
#[nativelink_test]
async fn client_finish_write_true_with_byte_mismatch_returns_invalid_argument()
-> Result<(), Error> {
    // Declare 24 bytes but stop at 8 with finish_write=true. The
    // wrapper accepts the chunk (high-watermark 8 < 24, no overrun),
    // sets `write_finished`, then on the next poll the size check at
    // the `write_finished` short-circuit fires.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(24)))).unwrap();
    tx.send(Ok(chunk(4, b"data", true, None))).unwrap();
    drop(tx);

    let mut wrapper =
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?;

    wrapper.next().await.expect("first chunk").expect("first ok");
    wrapper.next().await.expect("second chunk").expect("second ok");

    // The next poll triggers the size check.
    let next = wrapper.next().await.expect("size mismatch must surface as Some(Err)");
    let err = next.expect_err(
        "finish_write=true with byte-count mismatch MUST surface as InvalidArgument — Case E",
    );
    assert_eq!(
        err.code,
        Code::InvalidArgument,
        "finish_write=true with byte mismatch MUST stay InvalidArgument (got {:?}) — this is the genuine protocol-error path, NOT the #357 Cancelled path",
        err.code,
    );

    Ok(())
}
