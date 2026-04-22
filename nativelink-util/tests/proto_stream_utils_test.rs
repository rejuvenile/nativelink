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

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_error::Error;
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
