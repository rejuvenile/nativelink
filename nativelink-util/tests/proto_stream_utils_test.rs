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

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_util::common::DigestInfo;
use nativelink_util::proto_stream_utils::{
    WriteRequestStreamWrapper, WriteState, WriteStateWrapper,
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

    let local_state = Arc::new(Mutex::new(WriteState::new(
        INSTANCE_NAME.to_string(),
        WriteRequestStreamWrapper::from(UnboundedReceiverStream::new(rx)).await?,
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

/// Stuck producer: emits chunks then stops sending for >15s. The per-chunk
/// progress timer must fire, the wrapper must end the stream (signalling
/// EOF to the gRPC client), and `take_read_stream_error` must return the
/// DeadlineExceeded error so the retry loop sees the right cause.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn write_state_progress_timeout_fires_when_producer_stalls() -> Result<(), Error> {
    // 2 chunks delivered + 1 stuck = expected 12 bytes; the stall fires
    // before the third chunk arrives, so the wrapper never reaches EOF
    // and the size check never runs (it only triggers on write_finished).
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WriteRequest, Error>>();
    tx.send(Ok(chunk(0, b"data", false, Some(12)))).unwrap();
    tx.send(Ok(chunk(4, b"data", false, None))).unwrap();
    let state = make_state(rx, Duration::from_secs(15)).await?;
    let mut wrapper = WriteStateWrapper::new(state.clone());

    // Drain the two pre-sent chunks.
    let first = wrapper.next().await.expect("first chunk");
    assert_eq!(first.write_offset, 0);
    let second = wrapper.next().await.expect("second chunk");
    assert_eq!(second.write_offset, 4);

    // tx is held by the channel (never closed) — only the timer can end
    // the stream. Wrapper should yield None after 15s of silence.
    let next = wrapper.next().await;
    assert_eq!(next, None, "wrapper must end on no-progress timeout");

    let err = state
        .lock()
        .take_read_stream_error()
        .expect("DeadlineExceeded must be recorded");
    assert_eq!(err.code, Code::DeadlineExceeded, "wrong code: {err:?}");
    assert!(
        err.messages.iter().any(|m| m.contains("no progress")),
        "expected 'no progress' wording, got: {:?}",
        err.messages
    );

    drop(tx);
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
