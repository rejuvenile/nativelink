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

use core::task::Poll;
use core::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::poll;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_util::buf_channel::{
    WriteHalfGuard, make_buf_channel_pair, make_buf_channel_pair_with_size,
};
use pretty_assertions::assert_eq;
use tokio::sync::mpsc::error::TrySendError;
use tokio::try_join;

/// Deadlock-detector timeout for WriteHalfGuard tests. Per CLAUDE.md
/// "Test in production composition, not in isolation" — every `rx.recv()`
/// after a guard exit MUST be wrapped in `tokio::time::timeout` with a
/// SPECIFIC `.expect("must not deadlock — ...")` so a regression in the
/// writer-termination contract fails fast (5s) rather than hanging the
/// CI runner.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

const DATA1: &str = "foo";
const DATA2: &str = "bar";
const DATA3: &str = "foobar1234";

#[nativelink_test]
async fn smoke_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    tx.send(DATA1.into()).await?;
    tx.send(DATA2.into()).await?;
    assert_eq!(rx.recv().await?, DATA1);
    assert_eq!(rx.recv().await?, DATA2);
    Ok(())
}

#[nativelink_test]
async fn bytes_written_test() -> Result<(), Error> {
    let (mut tx, _rx) = make_buf_channel_pair();
    tx.send(DATA1.into()).await?;
    assert_eq!(tx.get_bytes_written(), DATA1.len() as u64);
    tx.send(DATA2.into()).await?;
    assert_eq!(tx.get_bytes_written(), (DATA1.len() + DATA2.len()) as u64);
    Ok(())
}

#[nativelink_test]
async fn sending_eof_sets_pipe_broken_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let tx_fut = async move {
        tx.send(DATA1.into()).await?;
        assert_eq!(tx.is_pipe_broken(), false);
        tx.send_eof()?;
        assert_eq!(tx.is_pipe_broken(), true);
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        assert_eq!(rx.recv().await?, Bytes::from(DATA1));
        assert_eq!(rx.recv().await?, Bytes::new());
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

#[nativelink_test]
async fn consume_all_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let tx_fut = async move {
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        assert_eq!(
            rx.consume(None).await?,
            Bytes::from(format!("{DATA1}{DATA2}{DATA1}{DATA2}"))
        );
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

/// Test to ensure data is optimized so that the exact same pointer is received
/// when calling `collect_all_with_size_hint` when a copy is not needed.
#[nativelink_test]
async fn consume_all_is_optimized_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let sent_data = Bytes::from(DATA1);
    let send_data_ptr = sent_data.as_ptr();
    let tx_fut = async move {
        tx.send(sent_data).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        // Because data is 1 chunk and an EOF, we should not need to copy
        // and should get the exact same pointer.
        let received_data = rx.consume(None).await?;
        assert_eq!(received_data.as_ptr(), send_data_ptr);
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

#[nativelink_test]
async fn consume_some_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let tx_fut = async move {
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        let all_data = Bytes::from(format!("{DATA1}{DATA2}{DATA1}{DATA2}"));
        assert_eq!(rx.consume(Some(1)).await?, all_data.slice(0..1));
        assert_eq!(rx.consume(Some(3)).await?, all_data.slice(1..4));
        assert_eq!(rx.consume(Some(4)).await?, all_data.slice(4..8));
        // Last chunk take too much data and expect EOF to be hit.
        assert_eq!(rx.consume(Some(100)).await?, all_data.slice(8..12));
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

/// This test ensures that when we are taking just one message in the stream,
/// we don't need to concat the data together and instead return a view to
/// the original data instead of making a copy.
#[nativelink_test]
async fn consume_some_optimized_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let first_chunk = Bytes::from(DATA1);
    let first_chunk_ptr = first_chunk.as_ptr();
    let tx_fut = async move {
        tx.send(first_chunk).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        assert_eq!(rx.consume(Some(1)).await?.as_ptr(), first_chunk_ptr);
        assert_eq!(rx.consume(Some(100)).await?.as_ptr(), unsafe {
            first_chunk_ptr.add(1)
        });
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

#[nativelink_test]
async fn consume_some_reads_eof() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    tx.send(DATA1.into()).await?;

    let consume_fut = rx.consume(Some(DATA1.len()));
    tokio::pin!(consume_fut);
    assert_eq!(
        poll!(&mut consume_fut),
        Poll::Pending,
        "Consume should not have completed yet"
    );
    tx.send_eof()?;
    assert_eq!(consume_fut.await?, Bytes::from(DATA1));
    Ok(())
}

#[nativelink_test]
async fn simple_stream_test() -> Result<(), Error> {
    use futures::StreamExt;
    let (mut tx, mut rx) = make_buf_channel_pair();
    let tx_fut = async move {
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send(DATA1.into()).await?;
        tx.send(DATA2.into()).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        assert_eq!(
            rx.next().await.map(|v| v.err_tip(|| "")),
            Some(Ok(Bytes::from(DATA1)))
        );
        assert_eq!(
            rx.next().await.map(|v| v.err_tip(|| "")),
            Some(Ok(Bytes::from(DATA2)))
        );
        assert_eq!(
            rx.next().await.map(|v| v.err_tip(|| "")),
            Some(Ok(Bytes::from(DATA1)))
        );
        assert_eq!(
            rx.next().await.map(|v| v.err_tip(|| "")),
            Some(Ok(Bytes::from(DATA2)))
        );
        assert_eq!(rx.next().await.map(|v| v.err_tip(|| "")), None);
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

#[nativelink_test]
async fn send_and_take_fuzz_test() -> Result<(), Error> {
    const DATA3_END_POS: usize = DATA3.len() + 1;
    for data_size in 1..DATA3_END_POS {
        let data: Vec<u8> = DATA3.as_bytes()[0..data_size].to_vec();

        for write_size in 1..DATA3_END_POS {
            for read_size in 1..DATA3_END_POS {
                let tx_data = Bytes::from(data.clone());
                let expected_data = Bytes::from(data.clone());

                let (mut tx, mut rx) = make_buf_channel_pair();

                let tx_fut = async move {
                    for i in (0..data_size).step_by(write_size) {
                        tx.send(tx_data.slice(i..core::cmp::min(data_size, i + write_size)))
                            .await?;
                    }
                    tx.send_eof()?;
                    Result::<(), Error>::Ok(())
                };
                let rx_fut = async move {
                    let mut round_trip_data = BytesMut::new();
                    for _ in (0..data_size).step_by(read_size) {
                        round_trip_data.extend(rx.consume(Some(read_size)).await?.iter());
                    }
                    assert_eq!(round_trip_data.freeze(), expected_data);
                    rx.drain().await?;
                    Result::<(), Error>::Ok(())
                };
                try_join!(tx_fut, rx_fut)?;
            }
        }
    }
    Ok(())
}

#[nativelink_test]
async fn rx_gets_error_if_tx_drops_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let tx_fut = async move {
        tx.send(DATA1.into()).await?;
        Result::<(), Error>::Ok(())
    };
    let rx_fut = async move {
        assert_eq!(rx.recv().await?, Bytes::from(DATA1));
        assert_eq!(
            rx.recv().await,
            Err(make_err!(
                Code::Internal,
                "Sender dropped before sending EOF"
            ))
        );
        Result::<(), Error>::Ok(())
    };
    try_join!(tx_fut, rx_fut)?;
    Ok(())
}

#[nativelink_test]
async fn bind_buffered_test() -> Result<(), Error> {
    let (mut tx_source, mut rx_bind) = make_buf_channel_pair();
    let (mut tx_bind, mut rx_final) = make_buf_channel_pair();
    try_join!(
        async move {
            let result = tx_bind.bind_buffered(&mut rx_bind).await;
            assert!(result.is_err(), "Should be error, got: {result:?}");
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("Sender dropped before sending EOF")
            );
            Ok(())
        },
        async move {
            tx_source.send(DATA1.into()).await.unwrap();
            drop(tx_source);
            assert_eq!(
                rx_final.recv().await,
                Err(make_err!(
                    Code::Internal,
                    "Sender dropped before sending EOF"
                ))
            );
            Result::<_, Error>::Ok(())
        }
    )
    .unwrap();
    Ok(())
}

#[nativelink_test]
async fn eof_can_send_twice() -> Result<(), Error> {
    let (mut tx, _rx) = make_buf_channel_pair();
    tx.send(DATA1.into()).await.unwrap();
    tx.send_eof().unwrap();
    // EOF needs to be able to be sent twice just in case a "retry" is triggered.
    tx.send_eof().unwrap();
    Ok(())
}

#[nativelink_test]
async fn set_max_recent_data_size_no_eof_then_retry_test() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair();
    {
        rx.set_max_recent_data_size(1024);
        tx.send(DATA1.into()).await.unwrap();
        drop(tx);
        assert_eq!(rx.recv().await.unwrap(), Bytes::from(DATA1));
        assert_eq!(
            rx.recv().await,
            Err(make_err!(
                Code::Internal,
                "Sender dropped before sending EOF"
            ))
        );
    }
    {
        rx.try_reset_stream().unwrap();
        assert_eq!(
            rx.recv().await,
            Err(make_err!(
                Code::Internal,
                "Sender dropped before sending EOF"
            ))
        );
    }
    Ok(())
}

// -------------------------------------------------------------------
// try_send: non-blocking, best-effort enqueue used by the bytestream
// mirror tee. Replaces the old `tokio::time::timeout(100ms, send())`
// pattern that permanently disabled the mirror on a single slow chunk.
// -------------------------------------------------------------------

#[nativelink_test]
async fn try_send_succeeds_when_capacity_available() -> Result<(), Error> {
    let (mut tx, mut rx) = make_buf_channel_pair_with_size(2);
    tx.try_send(Bytes::from(DATA1)).expect("first try_send");
    tx.try_send(Bytes::from(DATA2)).expect("second try_send");
    assert_eq!(tx.get_bytes_written(), (DATA1.len() + DATA2.len()) as u64);
    assert_eq!(rx.recv().await?, Bytes::from(DATA1));
    assert_eq!(rx.recv().await?, Bytes::from(DATA2));
    Ok(())
}

#[nativelink_test]
async fn try_send_returns_full_when_channel_saturated_and_writer_stays_alive() {
    // Capacity 1: the second try_send must fail with Full, the writer
    // must remain usable, and after the receiver drains a slot the
    // third try_send must succeed. This is the regression case for
    // the bytestream mirror "permanent disable on one slow chunk" bug.
    let (mut tx, mut rx) = make_buf_channel_pair_with_size(1);
    tx.try_send(Bytes::from(DATA1)).expect("first slot");
    let dropped_chunk = Bytes::from(DATA2);
    match tx.try_send(dropped_chunk.clone()) {
        Err(TrySendError::Full(returned)) => {
            // Caller must regain ownership of the chunk on Full.
            assert_eq!(returned, dropped_chunk);
        }
        other => panic!("expected Full, got {other:?}"),
    }
    // bytes_written must NOT have advanced for the dropped chunk.
    assert_eq!(
        tx.get_bytes_written(),
        DATA1.len() as u64,
        "bytes_written must not count dropped chunks",
    );
    // Writer must still be usable.
    assert!(!tx.is_pipe_broken());
    // Drain a slot, then the next try_send must succeed.
    assert_eq!(rx.recv().await.unwrap(), Bytes::from(DATA1));
    tx.try_send(Bytes::from(DATA3))
        .expect("third try_send after drain");
    assert_eq!(rx.recv().await.unwrap(), Bytes::from(DATA3));
}

#[nativelink_test]
async fn try_send_returns_closed_when_receiver_dropped() {
    let (mut tx, rx) = make_buf_channel_pair_with_size(4);
    drop(rx);
    let chunk = Bytes::from(DATA1);
    match tx.try_send(chunk.clone()) {
        Err(TrySendError::Closed(returned)) => {
            assert_eq!(returned, chunk);
        }
        other => panic!("expected Closed, got {other:?}"),
    }
    // After Closed, the writer must short-circuit subsequent sends.
    assert!(tx.is_pipe_broken());
    match tx.try_send(Bytes::from(DATA2)) {
        Err(TrySendError::Closed(_)) => {}
        other => panic!("expected Closed on second send, got {other:?}"),
    }
}

#[nativelink_test]
async fn try_send_rejects_zero_length_buf_as_closed() {
    // EOF must go through send_eof; try_send of an empty Bytes is
    // rejected as Closed (mirrors the behavior of the async send()).
    let (mut tx, _rx) = make_buf_channel_pair_with_size(4);
    match tx.try_send(Bytes::new()) {
        Err(TrySendError::Closed(_)) => {}
        other => panic!("expected Closed for empty buf, got {other:?}"),
    }
    // Counter must remain unchanged.
    assert_eq!(tx.get_bytes_written(), 0);
}

/// Spec: dropping a `WriteHalfGuard` without committing MUST surface a
/// synthesized Internal on the reader side so a paired reader unblocks.
/// This is the contract the guard exists to enforce.
#[nativelink_test]
async fn write_half_guard_uncommitted_drop_terminates_with_internal() {
    let (mut tx, mut rx) = make_buf_channel_pair();
    {
        let _guard = WriteHalfGuard::new(&mut tx);
        // No commit_eof, no commit_delegated_if_ok, no fail.
    }
    let recv_result = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated");
    let err = recv_result.expect_err(
        "Drop fallback MUST surface as an error on the reader side; \
         without it the paired reader would deadlock forever",
    );
    assert_eq!(err.code, Code::Internal, "fallback err.code MUST be Internal");
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("WriteHalfGuard fired Drop fallback")),
        "fallback err MUST identify itself so operators can grep for the missing commit site, got: {err:?}",
    );
}

/// Spec: `commit_eof()` MUST suppress the Drop fallback AND deliver a
/// clean EOF to the receiver.
#[nativelink_test]
async fn write_half_guard_commit_eof_delivers_clean_eof() {
    let (mut tx, mut rx) = make_buf_channel_pair();
    {
        let mut guard = WriteHalfGuard::new(&mut tx);
        guard.commit_eof().expect("send_eof must succeed on a fresh channel");
    }
    let chunk = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect("clean EOF must surface as Ok(empty)");
    assert!(chunk.is_empty(), "expected EOF, got: {} bytes", chunk.len());
}

/// Spec: `fail(err)` MUST terminate the writer with the structured error
/// AND return the same error so the caller can `return Err(guard.fail(err))`.
#[nativelink_test]
async fn write_half_guard_fail_propagates_structured_error() {
    let (mut tx, mut rx) = make_buf_channel_pair();
    let returned = {
        let mut guard = WriteHalfGuard::new(&mut tx);
        guard.fail(make_err!(Code::NotFound, "test-marker-NotFound"))
    };
    assert_eq!(returned.code, Code::NotFound);
    assert!(returned.messages.iter().any(|m| m.contains("test-marker-NotFound")));

    let err = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect_err("fail() must surface as Err");
    assert_eq!(err.code, Code::NotFound, "receiver MUST see the structured Code");
    assert!(
        err.messages.iter().any(|m| m.contains("test-marker-NotFound")),
        "receiver MUST see the structured message, got: {err:?}",
    );
}

/// Spec: `commit_delegated_if_ok(&Ok(()))` MUST suppress Drop;
/// `commit_delegated_if_ok(&Err(...))` MUST leave Drop armed so a sub-store
/// that returned Err WITHOUT terminating is caught at Drop time.
#[nativelink_test]
async fn write_half_guard_commit_delegated_if_ok_arms_drop_on_err() {
    // Ok branch: clean EOF surfaces, no Drop fallback.
    let (mut tx_ok, mut rx_ok) = make_buf_channel_pair();
    {
        let mut guard = WriteHalfGuard::new(&mut tx_ok);
        guard.send_eof().expect("send_eof must succeed");
        let res: Result<(), Error> = Ok(());
        guard.commit_delegated_if_ok(&res);
    }
    let chunk = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx_ok.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect("Ok branch must surface clean EOF");
    assert!(chunk.is_empty(), "Ok branch expected EOF, got {} bytes", chunk.len());

    // Err branch: sub-store returned Err WITHOUT terminating; Drop fallback fires.
    let (mut tx_err, mut rx_err) = make_buf_channel_pair();
    {
        let mut guard = WriteHalfGuard::new(&mut tx_err);
        let res: Result<(), Error> = Err(make_err!(Code::Internal, "sub-store err"));
        guard.commit_delegated_if_ok(&res);
    }
    let err = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx_err.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect_err("Err branch MUST surface Drop fallback (sub-store didn't terminate)");
    assert_eq!(err.code, Code::Internal);
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("WriteHalfGuard fired Drop fallback")),
        "Err branch MUST surface the synthesized Internal, got: {err:?}",
    );
}

/// Spec: `commit_eof()` followed by `Drop` MUST produce exactly one
/// terminator (a clean EOF) — never an EOF immediately followed by the
/// synthesized Internal.
#[nativelink_test]
async fn commit_eof_then_drop_produces_exactly_one_terminator() {
    let (mut tx, mut rx) = make_buf_channel_pair();
    {
        let mut guard = WriteHalfGuard::new(&mut tx);
        guard.commit_eof().expect("send_eof must succeed on a fresh channel");
    }
    let chunk = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect("clean EOF must surface");
    assert!(chunk.is_empty(), "first recv MUST be EOF, got {} bytes", chunk.len());
    let chunk2 = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect("post-EOF recv MUST stay EOF, not surface a synthesized Internal");
    assert!(chunk2.is_empty(), "post-EOF recv MUST stay EOF, got {} bytes", chunk2.len());
}

/// Spec: dropping the guard while a chunk is queued MUST surface a coherent
/// terminal error to the reader, not corrupt the in-flight chunk.
#[nativelink_test]
async fn drop_during_active_send_does_not_corrupt_stream() {
    let (mut tx, mut rx) = make_buf_channel_pair_with_size(1);
    tx.send(Bytes::from_static(b"first-chunk"))
        .await
        .expect("first send into 1-slot channel must succeed");
    {
        let _guard = WriteHalfGuard::new(&mut tx);
        // No commit — Drop fires immediately on this scope exit.
    }
    let first = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect("first chunk must arrive intact");
    assert_eq!(&first[..], b"first-chunk", "in-flight chunk MUST NOT be corrupted by Drop");
    let err = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, rx.recv())
        .await
        .expect("must not deadlock — writer-termination contract violated")
        .expect_err("post-Drop recv MUST surface the synthesized Internal");
    assert_eq!(err.code, Code::Internal);
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("WriteHalfGuard fired Drop fallback")),
        "post-Drop err MUST be the synthesized Internal, got: {err:?}",
    );
}
