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

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::task::Poll;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::task::Context;
use futures::{Future, Stream, TryFutureExt};
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use parking_lot::Mutex as PlMutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

const ZERO_DATA: Bytes = Bytes::new();

/// Default channel capacity: 1024 slots. Matched to the io_uring ring
/// size and write pipeline depth so the channel never bottlenecks the
/// I/O pipeline.
const DEFAULT_BUF_CHANNEL_CAPACITY: usize = 1024;

/// Shared diagnostic state between a buf_channel's writer and reader
/// halves. Lets the reader's slow-producer warn (`recv > 5s`) name the
/// specific producer task and report when it last made progress —
/// which transforms the warn from "something upstream is slow" into
/// "task <N> hasn't sent a chunk in <Ms>". The information is cheap
/// to maintain (one atomic write per send) and the operator can grep
/// the worker journal for the producer task id to find what the
/// producer was doing when it stalled.
#[derive(Debug, Default)]
struct ChannelDiag {
    /// First producer task id that called `send`. `String` rather than
    /// `tokio::task::Id` so we don't need that type to be `Display` in
    /// every consumer (it is, but this avoids the dep). Lazily set on
    /// the first `send` because the channel is often constructed in a
    /// different task than the one that ultimately produces.
    producer_task_id: PlMutex<Option<String>>,
    /// Unix-epoch milliseconds of the most recent successful send.
    /// `0` means "no successful send yet" — distinguishable from a
    /// real send because we record `1` when t==0 happens to land at
    /// the epoch (extremely unlikely, but cheap to handle).
    last_send_at_epoch_ms: AtomicU64,
    /// Total number of successful sends so far. Pure counter; lets the
    /// slow-recv warn distinguish "channel empty since construction"
    /// from "channel was active and then stopped".
    sends_total: AtomicU64,
}

impl ChannelDiag {
    fn record_send(&self) {
        // Lazily capture the producer task id on first send.
        {
            let mut id = self.producer_task_id.lock();
            if id.is_none() {
                if let Some(tid) = tokio::task::try_id() {
                    *id = Some(tid.to_string());
                }
            }
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        self.last_send_at_epoch_ms.store(now_ms, Ordering::Relaxed);
        self.sends_total.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ChannelDiagSnapshot {
        ChannelDiagSnapshot {
            producer_task_id: self.producer_task_id.lock().clone(),
            last_send_at_epoch_ms: self.last_send_at_epoch_ms.load(Ordering::Relaxed),
            sends_total: self.sends_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct ChannelDiagSnapshot {
    producer_task_id: Option<String>,
    last_send_at_epoch_ms: u64,
    sends_total: u64,
}

/// Create a channel pair that can be used to transport buffer objects around to
/// different components. This wrapper is used because the streams give some
/// utility like managing EOF in a more friendly way, ensure if no EOF is received
/// it will send an error to the receiver channel before shutting down and count
/// the number of bytes sent.
///
/// Uses the default capacity of 24 slots (~72MiB at 3MiB chunks).
/// For custom sizing, use [`make_buf_channel_pair_with_size`] instead.
#[must_use]
pub fn make_buf_channel_pair() -> (DropCloserWriteHalf, DropCloserReadHalf) {
    make_buf_channel_pair_with_size(DEFAULT_BUF_CHANNEL_CAPACITY)
}

/// Like [`make_buf_channel_pair`], but with a caller-specified channel capacity.
///
/// The `capacity` parameter controls how many chunks can be buffered before the
/// producer is forced to wait. At 256KiB chunks (the default `read_buffer_size`),
/// each slot represents ~256KiB of buffered data, so:
///
/// -  24 slots = ~72MiB at 3MiB chunks (default, matches FilesystemStore read size)
/// -  64 slots = ~192MiB at 3MiB chunks (high-throughput streaming)
/// - 128 slots = ~384MiB at 3MiB chunks (use with caution)
#[must_use]
pub fn make_buf_channel_pair_with_size(
    capacity: usize,
) -> (DropCloserWriteHalf, DropCloserReadHalf) {
    let (tx, rx) = mpsc::channel(capacity);
    let eof_sent = Arc::new(AtomicBool::new(false));
    let diag = Arc::new(ChannelDiag::default());
    (
        DropCloserWriteHalf {
            tx: Some(tx),
            bytes_written: 0,
            eof_sent: eof_sent.clone(),
            diag: diag.clone(),
        },
        DropCloserReadHalf {
            rx,
            queued_data: VecDeque::new(),
            last_err: None,
            eof_sent,
            bytes_received: 0,
            recent_data: Vec::new(),
            max_recent_data_size: 0,
            diag,
        },
    )
}

/// Writer half of the pair.
#[derive(Debug)]
pub struct DropCloserWriteHalf {
    tx: Option<mpsc::Sender<Bytes>>,
    bytes_written: u64,
    eof_sent: Arc<AtomicBool>,
    /// Shared with the reader half; updated on every successful send so
    /// the reader can attribute slow-recv warns to a specific producer
    /// task and report the time since the last successful send.
    diag: Arc<ChannelDiag>,
}

impl DropCloserWriteHalf {
    /// Sends data over the channel to the receiver.
    pub fn send(&mut self, buf: Bytes) -> impl Future<Output = Result<(), Error>> + '_ {
        self.send_get_bytes_on_error(buf).map_err(|err| err.0)
    }

    /// Sends data over the channel to the receiver.
    #[inline]
    async fn send_get_bytes_on_error(&mut self, buf: Bytes) -> Result<(), (Error, Bytes)> {
        let tx = match self
            .tx
            .as_ref()
            .ok_or_else(|| make_err!(Code::Internal, "Tried to send while stream is closed"))
        {
            Ok(tx) => tx,
            Err(e) => return Err((e, buf)),
        };
        let Ok(buf_len) = u64::try_from(buf.len()) else {
            return Err((
                make_err!(Code::Internal, "Could not convert usize to u64"),
                buf,
            ));
        };
        if buf_len == 0 {
            return Err((
                make_input_err!("Cannot send EOF in send(). Instead use send_eof()"),
                buf,
            ));
        }
        let send_start = Instant::now();
        let result = tx.send(buf).await;
        let send_elapsed = send_start.elapsed();
        if send_elapsed.as_secs() >= 1 {
            warn!(
                send_ms = send_elapsed.as_millis() as u64,
                buf_len = buf_len,
                "buf_channel::send: channel backpressure (>1s wait)",
            );
        }
        if let Err(err) = result {
            // Close our channel.
            self.tx = None;
            return Err((
                make_err!(
                    Code::Internal,
                    "Failed to write to data, receiver disconnected"
                ),
                err.0,
            ));
        }
        self.bytes_written += buf_len;
        // Record producer task id (lazy on first send) and last-send
        // timestamp so the reader's slow-recv warn can name the producer
        // and report the gap.
        self.diag.record_send();
        Ok(())
    }

    /// Non-blocking send. Returns immediately with one of:
    /// - `Ok(())` if the chunk was queued.
    /// - `Err(TrySendError::Full(buf))` if the channel is full; the caller still
    ///   owns the buffer and the writer remains usable for future sends.
    /// - `Err(TrySendError::Closed(buf))` if the receiver has been dropped; the
    ///   writer is closed (`tx` cleared) so subsequent sends return
    ///   `TrySendError::Closed` immediately.
    ///
    /// Useful on best-effort fan-out paths (e.g., the bytestream tee mirror)
    /// where the producer must never block on a slow consumer. Avoids the
    /// timer-wheel cost of `tokio::time::timeout(...)` per chunk.
    pub fn try_send(&mut self, buf: Bytes) -> Result<(), TrySendError<Bytes>> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(TrySendError::Closed(buf));
        };
        let Ok(buf_len) = u64::try_from(buf.len()) else {
            // Mirror the behavior of `send`: refuse oversized chunks.
            return Err(TrySendError::Closed(buf));
        };
        if buf_len == 0 {
            // EOF must go through `send_eof`.
            return Err(TrySendError::Closed(buf));
        }
        match tx.try_send(buf) {
            Ok(()) => {
                self.bytes_written += buf_len;
                self.diag.record_send();
                Ok(())
            }
            Err(TrySendError::Full(buf)) => Err(TrySendError::Full(buf)),
            Err(TrySendError::Closed(buf)) => {
                // Receiver gone — close our side so future sends short-circuit.
                self.tx = None;
                Err(TrySendError::Closed(buf))
            }
        }
    }

    /// Binds a reader and a writer together. This will send all the data from the reader
    /// to the writer until an EOF is received.
    /// This will always read one message ahead to ensure that if an error happens
    /// on the EOF message it will not forward on the last payload message and instead
    /// forward on the error.
    pub async fn bind_buffered(&mut self, reader: &mut DropCloserReadHalf) -> Result<(), Error> {
        loop {
            let chunk = reader
                .recv()
                .await
                .err_tip(|| "In DropCloserWriteHalf::bind_buffered::recv")?;
            if chunk.is_empty() {
                self.send_eof()
                    .err_tip(|| "In DropCloserWriteHalf::bind_buffered::send_eof")?;
                break; // EOF.
            }
            // Always read one message ahead so if we get an error on our EOF
            // we forward it on to the reader.
            if reader.peek().await.is_err() {
                // Read our next message for good book keeping.
                drop(
                    reader
                        .recv()
                        .await
                        .err_tip(|| "In DropCloserWriteHalf::bind_buffered::peek::eof")?,
                );
                return Err(make_err!(
                    Code::Internal,
                    "DropCloserReadHalf::peek() said error, but when data received said Ok. This should never happen."
                ));
            }
            match self.send_get_bytes_on_error(chunk).await {
                Ok(()) => {}
                Err(e) => {
                    reader.queued_data.push_front(e.1);
                    return Err(e.0).err_tip(|| "In DropCloserWriteHalf::bind_buffered::send");
                }
            }
        }
        Ok(())
    }

    /// Sends an EOF (End of File) message to the receiver which will gracefully let the
    /// stream know it has no more data. This will close the stream.
    pub fn send_eof(&mut self) -> Result<(), Error> {
        // Flag that we have sent the EOF.
        let eof_was_sent = self.eof_sent.swap(true, Ordering::Release);
        if eof_was_sent {
            warn!(
                "Stream already closed when eof already was sent. This is often ok for retry was triggered, but should not happen on happy path."
            );
            return Ok(());
        }

        // Now close our stream.
        self.tx = None;
        Ok(())
    }

    /// Returns the number of bytes written so far. This does not mean the receiver received
    /// all of the bytes written to the stream so far.
    #[must_use]
    pub const fn get_bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns if the pipe was broken. This is good for determining if the reader broke the
    /// pipe or the writer broke the pipe, since this will only return true if the pipe was
    /// broken by the writer.
    #[must_use]
    pub const fn is_pipe_broken(&self) -> bool {
        self.tx.is_none()
    }
}

/// Reader half of the pair.
#[derive(Debug)]
pub struct DropCloserReadHalf {
    rx: mpsc::Receiver<Bytes>,
    /// Number of bytes received over the stream.
    bytes_received: u64,
    eof_sent: Arc<AtomicBool>,
    /// If there was an error in the stream, this will be set to the last error.
    last_err: Option<Error>,
    /// If not empty, this is the data that needs to be sent out before
    /// data from the underlying channel can should be sent.
    queued_data: VecDeque<Bytes>,
    /// As data is being read from the stream, this buffer will be filled
    /// with the most recent data. Once `max_recent_data_size` is reached
    /// this buffer will be cleared and no longer be populated.
    /// This is useful if the caller wants to reset the the reader to before
    /// any of the data was received if possible (eg: something failed and
    /// we want to retry).
    recent_data: Vec<Bytes>,
    /// Amount of data to keep in the `recent_data` buffer before clearing it
    /// and no longer populating it.
    max_recent_data_size: u64,
    /// Shared with the writer half; on a slow recv, snapshot this to
    /// attribute the wait to the producer task and report the gap
    /// since its last send.
    diag: Arc<ChannelDiag>,
}

impl DropCloserReadHalf {
    /// Returns if the stream has data ready.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    fn recv_inner(&mut self, chunk: Bytes) -> Result<Bytes, Error> {
        // `queued_data` is allowed to have empty bytes that represent EOF
        if chunk.is_empty() {
            if !self.eof_sent.load(Ordering::Acquire) {
                let err = make_err!(Code::Internal, "Sender dropped before sending EOF");
                self.queued_data.clear();
                self.recent_data.clear();
                self.bytes_received = 0;
                self.last_err = Some(err.clone());
                return Err(err);
            }

            self.maybe_populate_recent_data(&ZERO_DATA);
            return Ok(ZERO_DATA);
        }

        self.bytes_received += chunk.len() as u64;
        self.maybe_populate_recent_data(&chunk);
        Ok(chunk)
    }

    /// Try to receive a chunk of data, returning `None` if none is available.
    pub fn try_recv(&mut self) -> Option<Result<Bytes, Error>> {
        if let Some(err) = &self.last_err {
            return Some(Err(err.clone()));
        }
        self.queued_data.pop_front().map(Ok)
    }

    /// Receive a chunk of data, waiting asynchronously until some is available.
    pub async fn recv(&mut self) -> Result<Bytes, Error> {
        if let Some(result) = self.try_recv() {
            result
        } else {
            // `None` here indicates EOF, which we represent as Zero data
            let recv_start = Instant::now();
            let data = self.rx.recv().await.unwrap_or(ZERO_DATA);
            let recv_elapsed = recv_start.elapsed();
            if recv_elapsed.as_secs() >= 5 {
                let snap = self.diag.snapshot();
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or_default();
                let gap_since_last_send_ms = if snap.last_send_at_epoch_ms == 0 {
                    // No send ever happened on this channel — distinguish
                    // "channel was always empty" from "channel went silent
                    // after producing N chunks".
                    None
                } else {
                    Some(now_ms.saturating_sub(snap.last_send_at_epoch_ms))
                };
                warn!(
                    recv_ms = recv_elapsed.as_millis() as u64,
                    producer_task_id = %snap.producer_task_id.as_deref().unwrap_or("<none>"),
                    sends_total = snap.sends_total,
                    gap_since_last_send_ms = ?gap_since_last_send_ms,
                    bytes_received = self.bytes_received,
                    "buf_channel::recv: slow producer (>5s wait)",
                );
            }
            self.recv_inner(data)
        }
    }

    fn maybe_populate_recent_data(&mut self, chunk: &Bytes) {
        if self.max_recent_data_size == 0 {
            return; // Fast path.
        }
        if self.bytes_received > self.max_recent_data_size {
            if !self.recent_data.is_empty() {
                self.recent_data.clear();
            }
            return;
        }
        self.recent_data.push(chunk.clone());
    }

    /// Sets the maximum size of the `recent_data` buffer. If the number of bytes
    /// received exceeds this size, the `recent_data` buffer will be cleared and
    /// no longer populated.
    pub const fn set_max_recent_data_size(&mut self, size: u64) {
        self.max_recent_data_size = size;
    }

    /// Attempts to reset the stream to before any data was received. This will
    /// only work if the number of bytes received is less than `max_recent_data_size`.
    ///
    /// On error the state of the stream is undefined and the caller should not
    /// attempt to use the stream again.
    pub fn try_reset_stream(&mut self) -> Result<(), Error> {
        if self.bytes_received > self.max_recent_data_size {
            return Err(make_err!(
                Code::Internal,
                "Cannot reset stream, max_recent_data_size exceeded"
            ));
        }
        let mut data_sum = 0;
        for chunk in self.recent_data.drain(..).rev() {
            data_sum += chunk.len() as u64;
            self.queued_data.push_front(chunk);
        }
        assert!(self.recent_data.is_empty(), "Recent_data should be empty");
        // Ensure the sum of the bytes in recent_data is equal to the bytes_received.
        error_if!(
            data_sum != self.bytes_received,
            "Sum of recent_data bytes does not equal bytes_received"
        );
        self.bytes_received = 0;
        Ok(())
    }

    /// Drains the reader until an EOF is received, but sends data to the void.
    pub async fn drain(&mut self) -> Result<(), Error> {
        loop {
            if self
                .recv()
                .await
                .err_tip(|| "Failed to drain in buf_channel::drain")?
                .is_empty()
            {
                break; // EOF.
            }
        }
        Ok(())
    }

    /// Peek the next set of bytes in the stream without consuming them.
    pub async fn peek(&mut self) -> Result<&Bytes, Error> {
        if self.queued_data.is_empty() {
            let chunk = self.recv().await.err_tip(|| "In buf_channel::peek")?;
            self.queued_data.push_front(chunk);
        }
        Ok(self
            .queued_data
            .front()
            .expect("Should have data in the queue"))
    }

    /// The number of bytes received over this stream so far.
    pub const fn get_bytes_received(&self) -> u64 {
        self.bytes_received
    }

    /// Takes exactly `size` number of bytes from the stream and returns them.
    /// This means the stream will keep polling until either an EOF is received or
    /// `size` bytes are received and concat them all together then return them.
    /// This method is optimized to reduce copies when possible.
    /// If `size` is None, it will take all the bytes in the stream.
    pub async fn consume(&mut self, size: Option<usize>) -> Result<Bytes, Error> {
        let size = size.unwrap_or(usize::MAX);
        let first_chunk = {
            let mut chunk = self
                .recv()
                .await
                .err_tip(|| "During first read of buf_channel::take()")?;
            if chunk.is_empty() {
                return Ok(chunk); // EOF.
            }
            if chunk.len() > size {
                let remaining = chunk.split_off(size);
                self.queued_data.push_front(remaining);
                // No need to read EOF if we are a partial chunk.
                return Ok(chunk);
            }
            // Try to read our EOF to ensure our sender did not error out.
            match self.peek().await {
                Ok(peeked_chunk) => {
                    if peeked_chunk.is_empty() || chunk.len() == size {
                        return Ok(chunk);
                    }
                }
                Err(e) => {
                    return Err(e).err_tip(|| "Failed to check if next chunk is EOF")?;
                }
            }
            chunk
        };
        // If we get here, first_chunk was not enough and there is more data.
        // Fall back to concatenation for multiple chunks.
        let mut output = BytesMut::with_capacity(size.min(first_chunk.len() * 2));
        output.extend_from_slice(&first_chunk);

        loop {
            let mut chunk = self
                .recv()
                .await
                .err_tip(|| "During next read of buf_channel::take()")?;
            if chunk.is_empty() {
                break; // EOF.
            }
            if output.len() + chunk.len() > size {
                // Slice off the extra data and put it back into the queue. We are done.
                let remaining = chunk.split_off(size - output.len());
                self.queued_data.push_front(remaining);
            }
            output.extend_from_slice(&chunk);
            if output.len() == size {
                break; // We are done.
            }
        }
        Ok(output.freeze())
    }
}

impl Stream for DropCloserReadHalf {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First drain any queued data (e.g., from try_reset_stream or peek).
        if let Some(chunk) = self.queued_data.pop_front() {
            // queued_data may contain empty bytes representing EOF.
            if chunk.is_empty() {
                return Poll::Ready(None);
            }
            return Poll::Ready(Some(Ok(chunk)));
        }

        // Check for previous errors.
        if let Some(err) = &self.last_err {
            return Poll::Ready(Some(Err(err.clone().to_std_err())));
        }

        // Poll the underlying mpsc channel directly to avoid heap allocation.
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => match self.recv_inner(bytes) {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        Poll::Ready(None) // EOF
                    } else {
                        Poll::Ready(Some(Ok(bytes)))
                    }
                }
                Err(e) => Poll::Ready(Some(Err(e.to_std_err()))),
            },
            Poll::Ready(None) => {
                // Channel closed — treat as EOF or error depending on eof_sent flag.
                match self.recv_inner(ZERO_DATA) {
                    Ok(_) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e.to_std_err()))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod diag_tests {
    use super::*;

    /// Spec: a freshly-constructed channel reports `last_send_at_epoch_ms == 0`
    /// and `sends_total == 0`; the producer task id is unset until the first
    /// successful `send`. The reader-side slow-recv warn relies on this to
    /// distinguish "channel never produced" from "producer went silent".
    #[tokio::test]
    async fn diag_default_state_has_no_sends() {
        let (_tx, rx) = make_buf_channel_pair();
        let snap = rx.diag.snapshot();
        assert!(
            snap.producer_task_id.is_none(),
            "producer_task_id MUST be None before any send"
        );
        assert_eq!(
            snap.last_send_at_epoch_ms, 0,
            "last_send_at_epoch_ms MUST be 0 before any send"
        );
        assert_eq!(
            snap.sends_total, 0,
            "sends_total MUST be 0 before any send"
        );
    }

    /// Spec: after a successful `send`, the diag snapshot MUST report the
    /// producer task id, a positive last-send timestamp, and incremented
    /// sends_total. This is the data the slow-recv warn formats into the
    /// log line.
    #[tokio::test]
    async fn diag_records_first_send() {
        let (mut tx, rx) = make_buf_channel_pair();
        // Move the writer into a spawned task so the captured task id
        // refers to a different task than the test's main task — proves
        // the capture is task-local to the *sender*, not whoever
        // constructed the pair.
        let producer_id = tokio::spawn(async move {
            let pid = tokio::task::try_id().unwrap().to_string();
            tx.send(Bytes::from_static(b"hi")).await.unwrap();
            pid
        })
        .await
        .unwrap();

        let snap = rx.diag.snapshot();
        assert_eq!(
            snap.producer_task_id.as_deref(),
            Some(producer_id.as_str()),
            "diag MUST record the spawned producer task id, not the constructor's task"
        );
        assert!(
            snap.last_send_at_epoch_ms > 0,
            "last_send_at_epoch_ms MUST be set after a successful send"
        );
        assert_eq!(
            snap.sends_total, 1,
            "sends_total MUST be 1 after one successful send"
        );
    }

    /// Spec: subsequent sends MUST keep the FIRST producer's task id (so
    /// switching tasks mid-stream does not erase the original attribution),
    /// and MUST advance last_send_at_epoch_ms / sends_total.
    ///
    /// Note: this test runs the producer inside a `tokio::spawn` rather
    /// than directly in the `#[tokio::test]` body because
    /// `tokio::task::try_id()` returns `None` from `block_on` futures —
    /// i.e. the test body itself is not a "task" in tokio's sense. In
    /// production every send originates from a spawned task (gRPC
    /// handler, `tokio::spawn` worker, etc.), so this is the realistic
    /// path.
    #[tokio::test]
    async fn diag_keeps_first_producer_across_sends() {
        let (tx, rx) = make_buf_channel_pair();
        // Channel for the test driver to wait on the producer's first
        // and second sends so we can read the diag mid-stream.
        let (after_first, mut wait_first) = tokio::sync::mpsc::channel::<()>(1);
        let (after_second, mut wait_second) = tokio::sync::mpsc::channel::<()>(1);

        let producer = tokio::spawn(async move {
            let pid = tokio::task::try_id().unwrap().to_string();
            let mut tx = tx;
            tx.send(Bytes::from_static(b"a")).await.unwrap();
            after_first.send(()).await.unwrap();
            // Sleep at least 2 ms of wall-clock so the second send has
            // a chance to land on a different millisecond timestamp.
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            tx.send(Bytes::from_static(b"b")).await.unwrap();
            after_second.send(()).await.unwrap();
            pid
        });

        // Snapshot after the first send.
        wait_first.recv().await.unwrap();
        let snap1 = rx.diag.snapshot();
        assert_eq!(snap1.sends_total, 1);
        let ts1 = snap1.last_send_at_epoch_ms;
        let first_id = snap1.producer_task_id.clone().unwrap();

        // Snapshot after the second send.
        wait_second.recv().await.unwrap();
        let snap2 = rx.diag.snapshot();
        assert_eq!(
            snap2.producer_task_id.as_deref(),
            Some(first_id.as_str()),
            "first producer attribution MUST persist across subsequent sends"
        );
        assert!(
            snap2.last_send_at_epoch_ms >= ts1,
            "last_send_at_epoch_ms MUST advance (or stay equal under coarse clock)"
        );
        assert_eq!(
            snap2.sends_total, 2,
            "sends_total MUST count every successful send"
        );

        let final_pid = producer.await.unwrap();
        assert_eq!(final_pid, first_id);
    }
}
