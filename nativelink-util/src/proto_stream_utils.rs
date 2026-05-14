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

use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context, Poll};
use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_proto::google::bytestream::{ReadResponse, WriteRequest};
use parking_lot::Mutex;
use tokio::time::Sleep;
use tonic::{Status, Streaming};
use tracing::warn;

use crate::resource_info::ResourceInfo;

/// Process-wide counter of "no progress within `progress_timeout`"
/// observations on `WriteStateWrapper::poll_next`. Each increment
/// corresponds to a `warn!` line (rate-limited via
/// [`record_grpc_write_slow_chunk_and_maybe_warn`]) describing a single
/// gap exceeded; if a stream stays stuck for N×`progress_timeout`, the
/// counter advances by N.
///
/// **Diagnostic-only** (per user direction 2026-05-14, supersedes the
/// 2026-04-23 abort-and-retry semantics). The timer never aborts the
/// stream; dead-connection detection belongs to the transport layer
/// (h2 keepalive 30s/20s, TCP keepalive, QUIC keepalive 5s).
///
/// Counter is `pub` for downstream metrics surfaces and integration
/// tests; this is the same pattern as
/// `chunked::chunked_driver::CHUNKED_DRIVER_PWRITE_TIMEOUT_TOTAL`.
pub static GRPC_WRITE_SLOW_CHUNK_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Sliding-window threshold for the loud `warn!`. Below this we still
/// emit one `warn!` per slow-chunk event (so SREs can grep for the
/// instance name + total at any rate); the rate-limited line surfaces
/// when many gaps land inside the same window.
const SLOW_CHUNK_WARN_THRESHOLD: u64 = 10;

/// Sliding window for the rate-warn trigger. 60 s matches typical
/// scrape intervals; if more than [`SLOW_CHUNK_WARN_THRESHOLD`] slow
/// chunks land in this window the SRE sees the louder warn alongside
/// the next scrape. Same shape as the chunked-driver counterpart for
/// operator familiarity.
const SLOW_CHUNK_WARN_WINDOW: Duration = Duration::from_secs(60);

/// Last-warn timestamp + count baseline (paired). Single-writer (the
/// `poll_next` Pending arm under the per-state mutex), but the counter
/// is process-wide so the warn-rate state must be too.
static SLOW_CHUNK_WARN_STATE: parking_lot::Mutex<Option<(std::time::Instant, u64)>> =
    parking_lot::Mutex::new(None);

/// Bump [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] and emit a per-event `warn!`,
/// plus a louder rate-limited `warn!` when more than
/// [`SLOW_CHUNK_WARN_THRESHOLD`] events land in the same
/// [`SLOW_CHUNK_WARN_WINDOW`]. Diagnostic-only — the caller continues
/// awaiting the inner stream after this returns.
fn record_grpc_write_slow_chunk_and_maybe_warn(instance_name: &str, progress_timeout_s: u64) {
    let new_total = GRPC_WRITE_SLOW_CHUNK_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    warn!(
        target: "nativelink_util::proto_stream",
        instance_name,
        progress_timeout_s,
        total_slow_chunks = new_total,
        "GrpcStore::write made no progress for >={progress_timeout_s}s; \
         continuing per diagnostic-only design (2026-05-14)",
    );
    let now = std::time::Instant::now();
    let mut guard = SLOW_CHUNK_WARN_STATE.lock();
    let baseline = guard.unwrap_or((now, new_total.saturating_sub(1)));
    let (last_at, count_at_last) = baseline;
    let delta = new_total.saturating_sub(count_at_last);
    let elapsed = now.duration_since(last_at);
    if delta >= SLOW_CHUNK_WARN_THRESHOLD && elapsed <= SLOW_CHUNK_WARN_WINDOW {
        warn!(
            target: "nativelink_util::proto_stream",
            instance_name,
            total_slow_chunks = new_total,
            slow_chunks_in_window = delta,
            window_secs = SLOW_CHUNK_WARN_WINDOW.as_secs(),
            "GrpcStore::write: slow-chunk events exceeding {SLOW_CHUNK_WARN_THRESHOLD}/window — \
             transport may be wedged; correlate with h2/TCP keepalive state",
        );
        *guard = Some((now, new_total));
    } else if elapsed > SLOW_CHUNK_WARN_WINDOW {
        // Reset baseline; healthy rate.
        *guard = Some((now, new_total));
    } else if guard.is_none() {
        // First-call seed.
        *guard = Some(baseline);
    }
}

pub struct WriteRequestStreamWrapper<T> {
    pub resource_info: ResourceInfo<'static>,
    pub bytes_received: usize,
    stream: T,
    first_msg: Option<WriteRequest>,
    pub write_finished: bool,
}

impl<T> Debug for WriteRequestStreamWrapper<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteRequestStreamWrapper")
            .field("resource_info", &self.resource_info)
            .field("bytes_received", &self.bytes_received)
            .field("first_msg", &self.first_msg)
            .field("write_finished", &self.write_finished)
            .finish()
    }
}

impl<T, E> WriteRequestStreamWrapper<T>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin,
    E: Into<Error>,
{
    pub async fn from(mut stream: T) -> Result<Self, Error> {
        let first_msg = stream
            .next()
            .await
            .err_tip(|| "Error receiving first message in stream")?
            .err_tip(|| "Expected WriteRequest struct in stream (from)")?;

        let resource_info = ResourceInfo::new(&first_msg.resource_name, true)
            .err_tip(|| {
                format!(
                    "Could not extract resource info from first message of stream: {}",
                    first_msg.resource_name
                )
            })?
            .to_owned();

        Ok(Self {
            resource_info,
            bytes_received: 0,
            stream,
            first_msg: Some(first_msg),
            write_finished: false,
        })
    }

    pub async fn next(&mut self) -> Option<Result<WriteRequest, Error>> {
        futures::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    pub const fn is_first_msg(&self) -> bool {
        self.first_msg.is_some()
    }

    /// Returns whether the first message has `finish_write` set to true.
    /// This indicates a single-shot upload where all data is in one message.
    pub fn is_first_msg_complete(&self) -> bool {
        self.first_msg.as_ref().is_some_and(|msg| msg.finish_write)
    }
}

impl<T, E> Stream for WriteRequestStreamWrapper<T>
where
    E: Into<Error>,
    T: Stream<Item = Result<WriteRequest, E>> + Unpin,
{
    type Item = Result<WriteRequest, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If the stream said that the previous message was the last one, then
        // return a stream EOF (i.e. None).
        if self.write_finished {
            error_if!(
                self.bytes_received != self.resource_info.expected_size,
                "Did not send enough data. Expected {}, but so far received {}",
                self.resource_info.expected_size,
                self.bytes_received
            );
            return Poll::Ready(None);
        }

        // Gets the next message, this is either the cached first or a
        // subsequent message from the wrapped Stream.
        let maybe_message = if let Some(first_msg) = self.first_msg.take() {
            Ok(first_msg)
        } else {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(maybe_message)) => maybe_message
                    .err_tip(|| format!("Stream error at byte {}", self.bytes_received)),
                // Bazel's DynamicSpawnStrategy half-closes the
                // ByteStream upload (HTTP/2 END_STREAM, no error frame)
                // when the local-execution branch wins the race
                // against remote. That's a cancellation, not a
                // protocol error — `Code::Cancelled` so the client's
                // gRPC status classifier treats it as retryable.
                // `make_input_err!` (InvalidArgument) misclassified
                // this as fatal and polluted Bazel build logs with
                // thousands of misleading errors per CI run. See #357.
                Poll::Ready(None) => Err(make_err!(
                    Code::Cancelled,
                    "client closed write stream mid-upload before sending finish_write=true (bytes_received={}/{})",
                    self.bytes_received,
                    self.resource_info.expected_size,
                )),
            }
        };

        // If we successfully got a message, update our internal state with the
        // message meta data.
        Poll::Ready(Some(maybe_message.and_then(|message| {
            self.write_finished = message.finish_write;
            // High-watermark accumulator: a resumed/replayed upload (Bazel
            // client retry, or our own GrpcStore Retrier replaying
            // WriteState::cached_messages) starts a fresh wrapper with
            // bytes_received=0 but the WriteRequests carry their original
            // write_offset. Summing data.len() per chunk would double-count
            // the replayed prefix and spuriously trip the overrun check
            // below before bytestream_server's downstream dedup at
            // bytestream_server.rs:1370 ever sees the offset. Saturating
            // arithmetic keeps malformed input (negative offset, oversized
            // chunk_end) from wrapping; the overrun check still catches it.
            let chunk_offset =
                usize::try_from(message.write_offset).unwrap_or(usize::MAX);
            let chunk_end = chunk_offset.saturating_add(message.data.len());
            self.bytes_received = self.bytes_received.max(chunk_end);

            // Check that we haven't read past the expected end.
            if self.bytes_received > self.resource_info.expected_size {
                Err(make_input_err!(
                    "sent too much data: expected={}, write_offset={}, chunk_len={}, bytes_received={}",
                    self.resource_info.expected_size,
                    message.write_offset,
                    message.data.len(),
                    self.bytes_received
                ))
            } else {
                Ok(message)
            }
        })))
    }
}

/// Represents the state of the first response in a `FirstStream`.
#[derive(Debug)]
pub enum FirstResponseState {
    /// Contains an optional first response that hasn't been consumed yet.
    /// A `None` value indicates the first response was EOF.
    Unused(Option<ReadResponse>),
    /// Indicates the first response has been consumed and future reads should
    /// come from the underlying stream.
    Used,
}

/// This provides a buffer for the first response from GrpcStore.read in order
/// to allow the first read to occur within the retry loop.  That means that if
/// the connection establishes fine, but reading the first byte of the file
/// fails we have the ability to retry before returning to the caller.
#[derive(Debug)]
pub struct FirstStream {
    /// The current state of the first response. When in the `Unused` state,
    /// contains an optional response which could be `None` or an EOF.
    /// Once consumed, transitions to the `Used` state.
    state: FirstResponseState,
    /// The stream to get responses from after the first response is consumed.
    stream: Streaming<ReadResponse>,
}

impl FirstStream {
    /// Creates a new `FirstStream` with the given first response and underlying
    /// stream.
    pub const fn new(
        first_response: Option<ReadResponse>,
        stream: Streaming<ReadResponse>,
    ) -> Self {
        Self {
            state: FirstResponseState::Unused(first_response),
            stream,
        }
    }
}

impl Stream for FirstStream {
    type Item = Result<ReadResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match mem::replace(&mut self.state, FirstResponseState::Used) {
            FirstResponseState::Unused(first_response) => Poll::Ready(first_response.map(Ok)),
            FirstResponseState::Used => Pin::new(&mut self.stream).poll_next(cx),
        }
    }
}

/// This structure wraps all of the information required to perform a write
/// request on the `GrpcStore`.  It stores the last message retrieved which allows
/// the write to resume since the UUID allows upload resume at the server.
#[derive(Debug)]
pub struct WriteState<T, E>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
    E: Into<Error> + 'static,
{
    instance_name: String,
    read_stream_error: Option<Error>,
    read_stream: WriteRequestStreamWrapper<T>,
    // Tonic doesn't appear to report an error until it has taken two messages,
    // therefore we are required to buffer the last two messages.
    cached_messages: [Option<WriteRequest>; 2],
    // When resuming after an error, the previous messages are cloned into this
    // queue upfront to allow them to be served back.
    resume_queue: [Option<WriteRequest>; 2],
    // An optimisation to avoid having to manage resume_queue when it's empty.
    is_resumed: bool,
    // Per-chunk no-progress timeout (DIAGNOSTIC-ONLY since 2026-05-14).
    // Zero disables the timer entirely. Reset to `Instant::now() +
    // duration` on each successful chunk; if it elapses while waiting on
    // the inner stream we emit a `warn!` (rate-limited) + bump
    // [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] + re-arm the deadline for another
    // window. The wrapper does NOT terminate, does NOT set
    // `read_stream_error`, and does NOT abort the gRPC RPC. Dead-
    // connection detection belongs to the transport layer (h2/TCP/QUIC
    // keepalive); per-chunk timers are observability only.
    //
    // Lives here (not in `WriteStateWrapper`) so it survives across the
    // retry loop's repeated wrapper construction and is uncontended under
    // the existing Mutex.
    progress_timeout: Duration,
    progress_deadline: Option<Pin<Box<Sleep>>>,
}

impl<T, E> WriteState<T, E>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
    E: Into<Error> + 'static,
{
    /// Construct a `WriteState` with a per-chunk no-progress
    /// **diagnostic** timer.
    ///
    /// **Diagnostic-only** (per user direction 2026-05-14). When the
    /// configured `progress_timeout` elapses without an inbound chunk,
    /// the wrapper emits a `warn!` line, increments the process-wide
    /// [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] counter, and re-arms the timer
    /// for another window — it does NOT terminate the stream, set
    /// `read_stream_error`, or return `DeadlineExceeded`. A long-stalled
    /// stream therefore produces one `warn!` per `progress_timeout`
    /// window, giving operators ongoing visibility into hours-long
    /// stuck streams. Real dead-connection detection is left to the
    /// transport layer (h2 keepalive 30s/20s, TCP keepalive, QUIC
    /// keepalive 5s) — application-layer timers were masking real bugs
    /// (chunked-write retry-rejection cascade #476, ci-mac-2 Tailscale
    /// slowness, `>=2-replica` durability invariant breakage on the
    /// 641 mirror_stream events on 2026-04-23).
    ///
    /// `progress_timeout = Duration::ZERO` disables the timer entirely
    /// (no warns, no counter bumps). Every caller must make a deliberate
    /// choice — there is no convenience `new` constructor — because the
    /// observability budget should be tuned to the upstream's expected
    /// pace. A typical Bazel uploader stretches a 50 MB blob over tens
    /// of seconds; setting `progress_timeout` near the per-chunk
    /// inter-arrival upper bound (rather than the whole-RPC ceiling)
    /// surfaces meaningful slow-chunk events without spamming.
    pub const fn with_progress_timeout(
        instance_name: String,
        read_stream: WriteRequestStreamWrapper<T>,
        progress_timeout: Duration,
    ) -> Self {
        Self {
            instance_name,
            read_stream_error: None,
            read_stream,
            cached_messages: [None, None],
            resume_queue: [None, None],
            is_resumed: false,
            progress_timeout,
            progress_deadline: None,
        }
    }

    fn push_message(&mut self, message: WriteRequest) {
        self.cached_messages.swap(0, 1);
        self.cached_messages[0] = Some(message);
    }

    const fn resumed_message(&mut self) -> Option<WriteRequest> {
        if self.is_resumed {
            // The resume_queue is a circular buffer, that we have to shift,
            // since its only got two elements its a trivial swap.
            self.resume_queue.swap(0, 1);
            let message = self.resume_queue[0].take();
            if message.is_none() {
                self.is_resumed = false;
            }
            message
        } else {
            None
        }
    }

    pub const fn can_resume(&self) -> bool {
        self.read_stream_error.is_none()
            && (self.cached_messages[0].is_some() || self.read_stream.is_first_msg())
    }

    pub fn resume(&mut self) {
        self.resume_queue.clone_from(&self.cached_messages);
        self.is_resumed = true;
        // Drop any stale Sleep from the previous attempt. Harmless if left
        // (next non-cached chunk re-arms it), but explicit clearing avoids
        // surprising state retained across the retry boundary.
        self.progress_deadline = None;
    }

    /// Take any `read_stream_error` recorded by the wrapper.
    ///
    /// **Set sites (since the 2026-05-14 diagnostic-only conversion).**
    /// The per-chunk progress timer no longer sets this — it now emits
    /// `warn!` + bumps [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] and continues
    /// awaiting the inner stream. The remaining set site is the
    /// resource-name parse error path in `WriteStateWrapper::poll_next`
    /// (a malformed first-chunk URI). That error is structurally
    /// non-resumable (the stream's first message is corrupt), so
    /// `can_resume()` correctly returns false when it fires — the
    /// retry loop in `GrpcStore::write` will surface it as `Err` rather
    /// than retrying the same broken chunk.
    pub const fn take_read_stream_error(&mut self) -> Option<Error> {
        self.read_stream_error.take()
    }
}

/// A wrapper around `WriteState` to allow it to be reclaimed from the underlying
/// write call in the case of failure.
#[derive(Debug)]
pub struct WriteStateWrapper<T, E>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
    E: Into<Error> + 'static,
{
    shared_state: Arc<Mutex<WriteState<T, E>>>,
}

impl<T, E> WriteStateWrapper<T, E>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
    E: Into<Error> + 'static,
{
    pub const fn new(shared_state: Arc<Mutex<WriteState<T, E>>>) -> Self {
        Self { shared_state }
    }
}

impl<T, E> Stream for WriteStateWrapper<T, E>
where
    T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
    E: Into<Error> + 'static,
{
    type Item = WriteRequest;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const IS_UPLOAD_TRUE: bool = true;

        // This should be an uncontended lock since write was called.
        let mut local_state = self.shared_state.lock();
        // If this is the first or second call after a failure and we have
        // cached messages, then use the cached write requests.
        let cached_message = local_state.resumed_message();
        if cached_message.is_some() {
            return Poll::Ready(cached_message);
        }
        // Read a new write request from the downstream.
        match Pin::new(&mut local_state.read_stream).poll_next(cx) {
            Poll::Ready(maybe_message) => {
                // Make progress: arm the no-progress timer for the NEXT
                // chunk. The current chunk's wait time is bounded by the
                // arrival just observed, so we reset rather than carry a
                // partially-elapsed deadline. Reuse the existing `Sleep`
                // allocation when present — `Sleep::reset` is the
                // standard tokio idiom for re-armable timers and avoids
                // a fresh `Box::pin` per chunk (~17 fewer allocs per
                // 50MB blob at 3MiB chunks).
                if !local_state.progress_timeout.is_zero() {
                    let timeout = local_state.progress_timeout;
                    let new_deadline = tokio::time::Instant::now() + timeout;
                    match local_state.progress_deadline.as_mut() {
                        Some(d) => d.as_mut().reset(new_deadline),
                        None => {
                            local_state.progress_deadline =
                                Some(Box::pin(tokio::time::sleep(timeout)));
                        }
                    }
                }
                // Update the instance name in the write request and forward it on.
                let result = match maybe_message {
                    Some(Ok(mut message)) => {
                        if !message.resource_name.is_empty() {
                            // Replace the instance name in the resource name if it is
                            // different from the instance name in the write state.
                            match ResourceInfo::new(&message.resource_name, IS_UPLOAD_TRUE) {
                                Ok(mut resource_name) => {
                                    if resource_name.instance_name
                                        != local_state.instance_name
                                    {
                                        resource_name.instance_name =
                                            Cow::Borrowed(&local_state.instance_name);
                                        message.resource_name =
                                            resource_name.to_string(IS_UPLOAD_TRUE);
                                    }
                                }
                                Err(err) => {
                                    local_state.read_stream_error = Some(err);
                                    return Poll::Ready(None);
                                }
                            }
                        }
                        // Cache the last request in case there is an error to allow
                        // the upload to be resumed.
                        local_state.push_message(message.clone());
                        Some(message)
                    }
                    Some(Err(err)) => {
                        local_state.read_stream_error = Some(err);
                        None
                    }
                    None => None,
                };
                Poll::Ready(result)
            }
            Poll::Pending => {
                // Inner stream not ready — check the per-chunk no-progress
                // **diagnostic** timer if enabled. We treat each
                // Pending->Ready transition as the moment we "made
                // progress"; while still Pending we race the configured
                // timeout against further pollings.
                //
                // **Diagnostic-only since 2026-05-14.** When the deadline
                // fires we (a) emit a `warn!` line + bump
                // [`GRPC_WRITE_SLOW_CHUNK_TOTAL`] so SREs see the slow
                // chunk, (b) re-arm the deadline for another
                // `progress_timeout` window so a stream that stays stuck
                // for hours produces one warn per window (not just one),
                // and (c) return `Poll::Pending` so the inner stream
                // keeps awaiting. We do NOT terminate, do NOT set
                // `read_stream_error`, do NOT abort the gRPC RPC.
                // Dead-connection detection is the transport's job
                // (h2/TCP/QUIC keepalive); per the user direction
                // 2026-05-14, application-layer timers are observability
                // only. They mask real bugs when used as kill-switches.
                if local_state.progress_timeout.is_zero() {
                    return Poll::Pending;
                }
                let timeout = local_state.progress_timeout;
                if local_state.progress_deadline.is_none() {
                    local_state.progress_deadline =
                        Some(Box::pin(tokio::time::sleep(timeout)));
                }
                let deadline = local_state
                    .progress_deadline
                    .as_mut()
                    .expect("initialized above");
                match deadline.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => {
                        // Diagnostic-only since 2026-05-14: emit a warn,
                        // bump the process-wide counter, RE-ARM the
                        // deadline for another `progress_timeout` window,
                        // and return `Poll::Pending` so the inner stream
                        // keeps awaiting. NOT an abort: do not set
                        // `read_stream_error`, do not return
                        // `Poll::Ready(None)`. Mutation point — restoring
                        // the pre-2026-05-14 abort-on-elapse here is what
                        // the bespoke "must NOT abort the stream" tests
                        // detect.
                        record_grpc_write_slow_chunk_and_maybe_warn(
                            &local_state.instance_name,
                            timeout.as_secs(),
                        );
                        // Re-arm: a stream that stays stuck for hours
                        // produces one warn per `progress_timeout`
                        // window, not just one. Reuse the existing Sleep
                        // allocation per the same idiom as the
                        // Pending->Ready arm above.
                        let new_deadline = tokio::time::Instant::now() + timeout;
                        local_state
                            .progress_deadline
                            .as_mut()
                            .expect("initialized above")
                            .as_mut()
                            .reset(new_deadline);
                        // Register interest in the new deadline so the
                        // task wakes on the next window boundary even if
                        // the inner stream never produces. Without this
                        // poll, a permanently-silent stream would never
                        // re-poll (Pending was already returned by the
                        // inner stream above) and we'd lose ongoing
                        // diagnostics.
                        let _ = local_state
                            .progress_deadline
                            .as_mut()
                            .expect("initialized above")
                            .as_mut()
                            .poll(cx);
                        Poll::Pending
                    }
                }
            }
        }
    }
}
