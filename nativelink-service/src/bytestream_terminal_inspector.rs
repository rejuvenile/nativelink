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

//! Inbound terminal-frame discriminator for ByteStream/Write (#355).
//!
//! ## Why this module exists
//!
//! The ByteStream/Write inbound stream can terminate in three observable
//! shapes that the existing `inner_write failed` warn cannot distinguish:
//!
//!   1. **Clean END_STREAM** — `poll_next` returns `Poll::Ready(None)` after
//!      the peer sent an HTTP/2 DATA frame with `END_STREAM` set and no
//!      gRPC trailer indicating an error. From the server's perspective
//!      this looks like Bazel app-level "I'm done sending" without ever
//!      having sent `WriteRequest { finish_write: true, .. }`. This is the
//!      #320 / #353 production observation.
//!
//!   2. **RST_STREAM with code** — `poll_next` returns `Poll::Ready(Some(
//!      Err(status)))` where `status.source()` downcasts to
//!      [`h2::Error`] and `h2::Error::reason()` returns the wire-level
//!      RST code (`CANCEL`, `REFUSED_STREAM`, `INTERNAL_ERROR`, …). Lets
//!      us tell "Bazel cancelled the upload" (`CANCEL=8`) from "h2 layer
//!      saw a protocol error" (`PROTOCOL_ERROR=1`) from "load shedder
//!      refused" (`REFUSED_STREAM=7`).
//!
//!   3. **gRPC-level error without h2 RST** — `poll_next` returns
//!      `Poll::Ready(Some(Err(status)))` but the source is *not* an
//!      [`h2::Error`]; for example `Status::internal("incomplete gRPC
//!      frame at end of body")` from the zero-copy path, or a
//!      `Status::cancelled` produced by hyper.
//!
//! Today's #353 partial-upload incident produced "Expected WriteRequest
//! struct in stream (got None)" with no client-side discriminator. This
//! module surfaces the three shapes as structured `tracing` events
//! (`bytestream_write_inbound_eof_clean` and
//! `bytestream_write_inbound_terminal_error`) so an operator can grep
//! and correlate against the existing `inner_write failed` warn (same
//! span ancestor, so `remote_addr` from the `http_connection` span is
//! attached automatically).
//!
//! ## What this module is NOT
//!
//! This is **pure observability** — it does not change any control-flow,
//! return value, or error path. The wrapped stream is a transparent
//! pass-through that fires a single per-stream log event the moment it
//! observes a terminal frame. No buffers are introduced (CLAUDE.md
//! "Unbounded in-process buffers on network paths are defects by
//! default" — this wrapper holds zero `Bytes`, only the underlying
//! stream and a one-shot "logged?" flag).

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures::Stream;
use nativelink_proto::google::bytestream::WriteRequest;
use tonic::Status;
use tracing::{info, warn};

/// Discriminator for the underlying cause of an inbound terminal frame
/// on a ByteStream/Write stream.
///
/// Returned by [`inspect_terminal_status`]. Consumed by the
/// [`TerminalFrameInspectingStream`] wrapper to choose which `tracing`
/// event to emit, and exposed publicly so callers can build their own
/// metrics on top of the discriminator without re-implementing the
/// downcast walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundTerminalKind {
    /// Underlying stream returned `Poll::Ready(None)` — clean END_STREAM
    /// with no gRPC error trailer.
    CleanEof,
    /// Underlying stream returned `Some(Err(status))` and `status.source()`
    /// downcasts to [`h2::Error`] with a non-`None` `reason()`.
    /// The `u32` is the on-wire h2 reason code (e.g. `CANCEL=8`,
    /// `REFUSED_STREAM=7`, `INTERNAL_ERROR=2`).
    H2Reset { reason_u32: u32, reason_name: &'static str },
    /// Underlying stream returned `Some(Err(status))` but no recoverable
    /// h2 RST code was attached — for example a hyper timeout, a tonic
    /// `Status::internal` synthesized by the zero-copy decoder, or a
    /// gRPC trailer with status != OK.
    GrpcError,
}

impl fmt::Display for InboundTerminalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CleanEof => f.write_str("clean_eof"),
            Self::H2Reset { reason_name, .. } => write!(f, "h2_rst:{reason_name}"),
            Self::GrpcError => f.write_str("grpc_error"),
        }
    }
}

/// Walk the source-chain of a [`tonic::Status`] looking for an
/// [`h2::Error`]; if found, decode its [`h2::Reason`] into a stable
/// `(u32, &'static str)` pair.
///
/// We *do not* rely on `h2::Reason::Display` here because some Reasons
/// (anything outside the spec-defined set) format as `"h2_reason(N)"`
/// and we want a clean ASCII name like `"CANCEL"` or `"unknown"` for
/// log readability. The exhaustive match below is the same one tonic
/// uses internally in `Status::code_from_h2`.
#[must_use]
pub fn h2_reason_from_status(status: &Status) -> Option<(u32, &'static str)> {
    use std::error::Error as _;
    let mut current: Option<&(dyn std::error::Error + 'static)> = status.source();
    while let Some(err) = current {
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            let reason = h2_err.reason()?;
            return Some((u32::from(reason), reason_name(reason)));
        }
        current = err.source();
    }
    None
}

/// Stable ASCII name for the well-known h2 reason codes per RFC 7540
/// §7. Anything else (codes outside the standard set, e.g. extensions)
/// returns `"other"` rather than panicking — this is observability,
/// not control flow.
const fn reason_name(reason: h2::Reason) -> &'static str {
    match reason {
        h2::Reason::NO_ERROR => "NO_ERROR",
        h2::Reason::PROTOCOL_ERROR => "PROTOCOL_ERROR",
        h2::Reason::INTERNAL_ERROR => "INTERNAL_ERROR",
        h2::Reason::FLOW_CONTROL_ERROR => "FLOW_CONTROL_ERROR",
        h2::Reason::SETTINGS_TIMEOUT => "SETTINGS_TIMEOUT",
        h2::Reason::STREAM_CLOSED => "STREAM_CLOSED",
        h2::Reason::FRAME_SIZE_ERROR => "FRAME_SIZE_ERROR",
        h2::Reason::REFUSED_STREAM => "REFUSED_STREAM",
        h2::Reason::CANCEL => "CANCEL",
        h2::Reason::COMPRESSION_ERROR => "COMPRESSION_ERROR",
        h2::Reason::CONNECT_ERROR => "CONNECT_ERROR",
        h2::Reason::ENHANCE_YOUR_CALM => "ENHANCE_YOUR_CALM",
        h2::Reason::INADEQUATE_SECURITY => "INADEQUATE_SECURITY",
        h2::Reason::HTTP_1_1_REQUIRED => "HTTP_1_1_REQUIRED",
        _ => "other",
    }
}

/// Pure helper: given a terminal `Some(Err(status))`, classify it as
/// either an h2 RST (with reason) or a generic gRPC error.
#[must_use]
pub fn inspect_terminal_status(status: &Status) -> InboundTerminalKind {
    if let Some((reason_u32, reason_name)) = h2_reason_from_status(status) {
        InboundTerminalKind::H2Reset { reason_u32, reason_name }
    } else {
        InboundTerminalKind::GrpcError
    }
}

/// Stream adapter that emits exactly one structured `tracing` event
/// the first time the underlying stream returns a terminal frame.
///
/// Wraps an inbound `Stream<Item = Result<WriteRequest, Status>>`
/// and forwards every poll unchanged. On the first
/// `Poll::Ready(None)` or `Poll::Ready(Some(Err(_)))` it logs:
///
///   - `info!(target: "bytestream_write_inbound_eof_clean", …)` for
///     a clean END_STREAM (no error frame on the wire).
///   - `warn!(target: "bytestream_write_inbound_terminal_error", …)`
///     for an error termination, with `kind`, `code`, and (when
///     available) `h2_reason` / `h2_reason_code`.
///
/// The `logged` flag prevents duplicate emissions if a caller
/// keeps polling past terminal — `Stream` semantics permit further
/// `None`s after the first, and we want exactly one event per
/// inbound stream.
///
/// The wrapper requires `S: Unpin` to avoid pulling in `pin-project`
/// for a single field. All inbound stream types we care about
/// (`tonic::Streaming<T>` and `ZeroCopyWriteStream<B>`) are already
/// `Unpin` — the latter via `Pin<Box<B>>` interior, the former by
/// virtue of every concrete codec output being `Unpin`. If a
/// non-`Unpin` stream ever needs wrapping, swap to `pin-project-lite`.
pub struct TerminalFrameInspectingStream<S> {
    inner: S,
    logged: bool,
}

impl<S> TerminalFrameInspectingStream<S> {
    /// Wrap `inner`. The wrapper is otherwise transparent.
    pub const fn new(inner: S) -> Self {
        Self { inner, logged: false }
    }
}

impl<S> fmt::Debug for TerminalFrameInspectingStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TerminalFrameInspectingStream")
            .field("logged", &self.logged)
            .finish()
    }
}

impl<S> Stream for TerminalFrameInspectingStream<S>
where
    S: Stream<Item = Result<WriteRequest, Status>> + Unpin,
{
    type Item = Result<WriteRequest, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.inner).poll_next(cx);
        match &poll {
            Poll::Ready(None) => {
                if !self.logged {
                    self.logged = true;
                    // info! because a clean END_STREAM is *expected* whenever
                    // the upstream layer chose not to send a `finish_write:
                    // true` chunk first. Whether that means "Bazel app
                    // cancelled before completing the WriteRequest" or
                    // "Bazel finished and the wrapper popped the terminator
                    // already" is decided by the OUTER `inner_write failed`
                    // warn (which fires only on the failure interpretation).
                    // The two events correlate via the `bytestream_write`
                    // span ancestor.
                    info!(
                        target: "bytestream_write_inbound_terminal",
                        kind = "clean_eof",
                        "bytestream_write_inbound_eof_clean"
                    );
                }
            }
            Poll::Ready(Some(Err(status))) => {
                if !self.logged {
                    self.logged = true;
                    let kind = inspect_terminal_status(status);
                    let code = status.code();
                    match kind {
                        InboundTerminalKind::H2Reset { reason_u32, reason_name } => {
                            warn!(
                                target: "bytestream_write_inbound_terminal",
                                kind = %kind,
                                grpc_code = ?code,
                                h2_reason = reason_name,
                                h2_reason_code = reason_u32,
                                message = %status.message(),
                                "bytestream_write_inbound_terminal_error"
                            );
                        }
                        InboundTerminalKind::GrpcError | InboundTerminalKind::CleanEof => {
                            // CleanEof is unreachable here (we'd be in the
                            // None arm), but listing it keeps the match
                            // exhaustive without `_` so a future variant
                            // forces a review of the log shape.
                            warn!(
                                target: "bytestream_write_inbound_terminal",
                                kind = %kind,
                                grpc_code = ?code,
                                message = %status.message(),
                                "bytestream_write_inbound_terminal_error"
                            );
                        }
                    }
                }
            }
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use core::task::Poll;

    use bytes::Bytes;
    use futures::stream;
    use nativelink_proto::google::bytestream::WriteRequest;
    use tonic::{Code, Status};

    use super::{InboundTerminalKind, h2_reason_from_status, inspect_terminal_status};

    fn dummy_request() -> WriteRequest {
        WriteRequest {
            resource_name: String::new(),
            write_offset: 0,
            finish_write: false,
            data: Bytes::new(),
        }
    }

    #[test]
    fn h2_reason_extracted_from_status_source_chain() {
        // Simulates exactly what tonic does when an h2 RST_STREAM arrives:
        // `Status::from(h2::Error)` stores the h2::Error as a source.
        let h2_err = h2::Error::from(h2::Reason::CANCEL);
        let status = Status::from(h2_err);
        let (code, name) = h2_reason_from_status(&status)
            .expect("h2 RST_STREAM with CANCEL must surface as Some(...)");
        assert_eq!(code, u32::from(h2::Reason::CANCEL));
        assert_eq!(name, "CANCEL");
    }

    #[test]
    fn inspect_classifies_h2_reset() {
        let h2_err = h2::Error::from(h2::Reason::REFUSED_STREAM);
        let status = Status::from(h2_err);
        let kind = inspect_terminal_status(&status);
        match kind {
            InboundTerminalKind::H2Reset { reason_u32, reason_name } => {
                assert_eq!(reason_u32, u32::from(h2::Reason::REFUSED_STREAM));
                assert_eq!(reason_name, "REFUSED_STREAM");
            }
            other => panic!("expected H2Reset, got {other:?}"),
        }
    }

    #[test]
    fn inspect_classifies_grpc_error_without_h2_source() {
        // A status with no source is a plain gRPC error.
        let status = Status::cancelled("client cancelled");
        assert_eq!(
            inspect_terminal_status(&status),
            InboundTerminalKind::GrpcError
        );
        // Same for status::internal etc.
        assert_eq!(
            inspect_terminal_status(&Status::internal("x")),
            InboundTerminalKind::GrpcError
        );
    }

    #[test]
    fn h2_reason_returns_none_for_status_without_h2_source() {
        let status = Status::new(Code::DeadlineExceeded, "tick");
        assert!(h2_reason_from_status(&status).is_none());
    }

    /// Direct poll-based test: feed terminal frames into the wrapper
    /// without going through tracing. We assert only the FORWARDED
    /// values; the tracing assertions live in the integration test
    /// (`bytestream_terminal_inspector_test.rs`) so they exercise the
    /// real subscriber wiring.
    #[tokio::test]
    async fn wrapper_forwards_clean_none_unchanged() {
        let s = stream::iter(vec![Ok(dummy_request())]);
        let mut wrapped = super::TerminalFrameInspectingStream::new(s);
        // Drive to completion via futures::StreamExt.
        use futures::StreamExt as _;
        let first = wrapped.next().await;
        assert!(matches!(first, Some(Ok(_))));
        let last = wrapped.next().await;
        assert!(last.is_none(), "wrapper must forward None unchanged");
        // Polling past terminal stays None and does not panic.
        let extra = wrapped.next().await;
        assert!(extra.is_none(), "second None after terminal must remain None");
    }

    #[tokio::test]
    async fn wrapper_forwards_error_unchanged() {
        let h2_err = h2::Error::from(h2::Reason::CANCEL);
        let status = Status::from(h2_err);
        let s = stream::iter(vec![Err(status)]);
        let mut wrapped = super::TerminalFrameInspectingStream::new(s);
        use futures::StreamExt as _;
        let first = wrapped.next().await;
        match first {
            Some(Err(s)) => {
                // The forwarded Status preserves its h2 source so the
                // wrapper does not drop the discriminator the very
                // mechanism we built this module to surface.
                assert!(h2_reason_from_status(&s).is_some());
            }
            other => panic!("expected Some(Err), got {other:?}"),
        }
    }

    #[test]
    fn poll_pending_does_not_log() {
        // Stream that yields Pending forever — wrapper must NOT mark itself
        // as logged on a Pending poll.
        let s = futures::stream::poll_fn(|_cx| Poll::<Option<Result<WriteRequest, Status>>>::Pending);
        let mut wrapped = Box::pin(super::TerminalFrameInspectingStream::new(s));
        let waker = futures::task::noop_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        for _ in 0..3 {
            assert!(matches!(
                futures::Stream::poll_next(wrapped.as_mut(), &mut cx),
                Poll::Pending
            ));
        }
        // Internal flag stays false — `logged` is private but we observe
        // via Debug formatting.
        assert!(format!("{wrapped:?}").contains("logged: false"));
    }
}
