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

//! #212 Phase 2.4: worker-side `WriteChunked` client.
//!
//! Producer for the server-side `WriteChunked` RPC handler shipped in
//! Phase 2.2/2.3 (`nativelink-service::chunked_write_handler`). The
//! worker reads its source bytes (CAS payload) from a
//! `DropCloserReadHalf` channel (the contract used by every `Store::update`
//! caller), splits them into `CHUNK_SIZE`-aligned chunks, computes a
//! per-chunk SHA-256 on `spawn_blocking` per #213 perf-opt NMA1, and
//! sends each chunk as a `WriteChunk` proto over a single
//! `WorkerApi/WriteChunked` client-streaming RPC.
//!
//! ## Lifecycle (anti-#203 invariant)
//!
//! `write_chunked_stream` returns control to its caller as soon as the
//! server replies with `WriteChunkedResponse` — there is no borrowed
//! writer pin-held across the wire. The caller's
//! `&mut DropCloserReadHalf` is consumed; once the function returns,
//! the caller may freely terminate its writer (`send_eof` or
//! `send_error`) per the writer-termination contract documented in
//! `feedback_writer_termination_class_2026_04_25`.
//!
//! ## Protocol
//!
//! - The first `WriteChunk` carries the digest.
//! - Each chunk is `CHUNK_SIZE`-aligned. The final chunk has
//!   `finish_chunk = true` and may be smaller than `CHUNK_SIZE`.
//! - Per §8.3.1 the SHA-256 of the chunk's bytes is sent on the same
//!   `WriteChunk` (`chunk_sha256` field). The server validates as the
//!   chunk arrives + reassembles + verifies the end-to-end digest at
//!   `finish_chunk`.
//!
//! ## Backpressure (Q8)
//!
//! On `Code::ResourceExhausted` carrying a `BackpressureSignal` detail
//! the client honors the server's `retry_after_ms` hint and retries the
//! WHOLE blob (a fresh `WriteChunked` stream) up to `max_attempts`
//! times. A bounded retry — not unbounded — to avoid the §15.5 R2
//! retry-storm-under-flapping pattern; if the server is genuinely
//! overloaded the per-attempt admission rejection is the correct
//! signal upstream.
//!
//! ## What this module does NOT do
//!
//! - **No fallback to legacy ByteStream** — the caller routes by
//!   feature-flag + size + kill-switch BEFORE calling into this
//!   module. Once you're here, you've committed to chunked transport.
//! - **No fan-out parallelism within a single blob** — the design Q4
//!   per-blob-mpsc-cap=16 lives on the SERVER side; this client sends
//!   chunks in arrival order (the worker has them in offset order
//!   from `DropCloserReadHalf::recv`). Phase 2.5+ may add parallel
//!   send paths; today we do single-stream in-order.
//! - **No connection management** — the caller passes a tonic
//!   `GrpcService`-shaped channel/connection. Pool eviction on
//!   transport errors is the caller's responsibility (mirrors the
//!   existing `evict_pool_on_transport_err` flow on `GrpcStore`).

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use core::future::Future;
use core::pin::Pin;

use bytes::{Bytes, BytesMut};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BackpressureSignal, WriteChunk, WriteChunkedResponse,
};
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tonic::Response;
use tracing::{debug, info, warn};

use crate::chunked::CHUNK_SIZE;
use crate::chunked_signal::error_has_backpressure_signal;

/// Type alias for the boxed-and-pinned future returned by
/// `WriteChunkedDispatcher::dispatch`. Manually-spelled rather than
/// `async fn` to keep the future's bounds explicit (Send + 'static)
/// and to dodge the `CoerceUnsized` higher-ranked-lifetime error
/// `async_trait` produces with deeply-nested `T: GrpcService<...>`
/// bounds in the impl.
pub type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<WriteChunkedResponse, Error>> + Send + 'static>>;

/// Transport-agnostic dispatcher for one `WorkerApi/WriteChunked`
/// RPC. Decoupling the chunked-write logic from the underlying
/// transport (a) keeps generic monomorphization shallow (the deep
/// generic stack of `tonic::client::GrpcService<...>` blew up
/// rustc's `optimized_mir` query during initial implementation) and
/// (b) lets tests substitute an in-process dispatcher without
/// spinning up tonic's full machinery.
///
/// Production wiring lives in `nativelink-store::grpc_store`'s
/// `WorkerApiWriteChunkedDispatcher` (TCP / QUIC / Dual transport
/// variants).
///
/// The dispatcher receives the prepared chunks as an owned `Vec`;
/// it materializes the wire stream internally via
/// `tokio_stream::iter`. This avoids passing a boxed-trait Stream
/// across crate boundaries (which generates fragile higher-ranked
/// lifetime obligations on the inner `tonic` future).
pub trait WriteChunkedDispatcher: Send + Sync {
    /// Send the prepared chunk stream and await the server's
    /// `WriteChunkedResponse`. The dispatcher is responsible for
    /// acquiring a connection, applying the per-attempt RPC, and
    /// returning the result. The chunked_client owns retry policy
    /// and chunk preparation; the dispatcher owns transport.
    ///
    /// Returns a boxed future (not `async fn`) per the
    /// `DispatchFuture` rationale.
    fn dispatch(&self, chunks: Vec<WriteChunk>) -> DispatchFuture;
}

/// Channel-acquisition future. Each `dispatch()` call invokes the
/// factory to obtain a fresh transport; the factory is responsible
/// for whatever pool-management / retry-aware acquisition policy
/// fits the deployment (e.g., `ConnectionManager::connection()` on
/// the TCP path, or just `Channel::clone()` on QUIC).
pub type ChannelAcquireFuture<T> =
    Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'static>>;

/// Convenience type for callers that want to construct a
/// dispatcher from a tonic `GrpcService`-shaped channel without
/// writing a separate trait impl. Each `dispatch()` call invokes
/// the `acquire_channel` factory to obtain a fresh transport (the
/// wire stream then closes when the dispatcher returns). Used by
/// `GrpcStore::update_via_chunked` to pull one TCP `Connection` (or
/// to clone the QUIC channel) per attempt.
///
/// The factory pattern (rather than a stored `T`) avoids requiring
/// `T: Clone` for callers — `nativelink_util::connection_manager::Connection`
/// is intentionally non-Cloneable because each instance ties to a
/// slot in the manager. Acquiring per-attempt also gives the
/// connection_manager its natural retry-on-transport-error path
/// (the dropped connection's slot returns to the pool; the next
/// retry's `acquire_channel().await` may pick a different slot).
pub struct WorkerApiWriteChunkedDispatcher<T> {
    /// Transport-acquisition factory. Returns a fresh `T` per
    /// dispatch — the dispatcher does NOT memoize the channel, so
    /// retries re-acquire from scratch.
    acquire_channel:
        Arc<dyn Fn() -> ChannelAcquireFuture<T> + Send + Sync + 'static>,
}

impl<T> core::fmt::Debug for WorkerApiWriteChunkedDispatcher<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WorkerApiWriteChunkedDispatcher")
            .finish_non_exhaustive()
    }
}

impl<T> WorkerApiWriteChunkedDispatcher<T> {
    /// Construct a dispatcher with a custom channel-acquisition
    /// factory. Used by `GrpcStore::update_via_chunked` to plug in
    /// `ConnectionManager::connection()` (TCP) or
    /// `Channel::clone()` (QUIC).
    pub fn with_factory<F>(acquire_channel: F) -> Self
    where
        F: Fn() -> ChannelAcquireFuture<T> + Send + Sync + 'static,
    {
        Self {
            acquire_channel: Arc::new(acquire_channel),
        }
    }
}

impl<T> WriteChunkedDispatcher for WorkerApiWriteChunkedDispatcher<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    T::Future: Send,
{
    fn dispatch(&self, chunks: Vec<WriteChunk>) -> DispatchFuture {
        let factory = Arc::clone(&self.acquire_channel);
        Box::pin(async move {
            let channel = factory().await?;
            let stream = tokio_stream::iter(chunks);
            let mut client = WorkerApiClient::new(channel);
            let response: Response<WriteChunkedResponse> = client
                .write_chunked(stream)
                .await
                .map_err(|status| {
                    let err: Error = status.into();
                    err.append("WorkerApi/WriteChunked RPC failed".to_string())
                })?;
            Ok(response.into_inner())
        })
    }
}

/// Default ceiling on retry attempts per blob. The server's
/// admission can reject with `Code::Aborted` (concurrent same-digest
/// stream) OR `Code::ResourceExhausted` + `BackpressureSignal`
/// (global budget / per-blob mpsc full). Both are transient by
/// design. Three attempts is the v1 perf-optimizer M6 starting
/// parameter — measurement-tuned post-deploy per design §8.6.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Cap on the per-attempt retry-after sleep. Defends against a
/// hostile or buggy server returning `retry_after_ms = u64::MAX`.
/// The value is generous enough to honor any sane server hint
/// (multi-second backoff for global-budget exhaustion is normal)
/// while bounding worst-case retry latency.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Counters published on the `GrpcStore`'s `MetricsComponent`. Held
/// behind an `Arc` so the same handle is shared between the
/// (cloned) GrpcStore Arc and the chunked-client invocations.
///
/// `Relaxed` ordering: observation-only counters; no other state
/// depends on the order of these increments.
#[derive(Debug, Default)]
pub struct ChunkedClientMetrics {
    /// Number of `write_chunked_stream` invocations that started
    /// (incremented BEFORE the first chunk goes on the wire).
    pub attempted_total: AtomicU64,
    /// Number of `write_chunked_stream` invocations that returned
    /// `Ok` (server acked commit + e2e SHA-256 verify).
    pub succeeded_total: AtomicU64,
    /// Per-attempt rejections with `Code::Aborted` (concurrent
    /// same-digest stream). Counts the rejections, NOT the eventual
    /// success after retry — operators reading this gauge know how
    /// often the producer races itself for the same digest.
    pub aborted_retried_total: AtomicU64,
    /// Per-attempt rejections with `Code::ResourceExhausted` +
    /// `BackpressureSignal` (Q8 admission backpressure).
    pub resource_exhausted_total: AtomicU64,
    /// Bytes successfully delivered via chunked transport. Equals
    /// the sum of declared digest sizes for `succeeded_total`
    /// invocations.
    pub bytes_sent_total: AtomicU64,
}

impl ChunkedClientMetrics {
    /// Construct an Arc-wrapped instance suitable for sharing
    /// between a `GrpcStore` and the per-call invocations.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

/// Per-call options. Today only the retry budget is configurable;
/// future fields can land here without churning every call site.
#[derive(Debug, Clone, Copy)]
pub struct ChunkedClientOptions {
    /// Maximum number of full-blob attempts before giving up.
    pub max_attempts: u32,
    /// Production chunk size in bytes. Defaults to `CHUNK_SIZE`
    /// (1 MiB); tests pass smaller for speed.
    pub chunk_size: usize,
}

impl Default for ChunkedClientOptions {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            chunk_size: CHUNK_SIZE,
        }
    }
}

/// Send a single CAS blob via the `WorkerApi/WriteChunked` RPC.
///
/// `channel` is a tonic `GrpcService`-shaped transport (typically a
/// `Connection` from `ConnectionManager` or a `tonic::transport::Channel`).
/// `digest` is the declared blob digest. `reader` is the source of
/// payload bytes — consumed end-to-end by this function (it reads
/// until EOF or the declared `digest.size_bytes()` worth of bytes
/// has arrived).
///
/// Returns the server-acknowledged committed size. Caller compares
/// this to `digest.size_bytes()` for sanity.
///
/// Anti-#203 invariant (per CLAUDE.md `feedback_async_to_sync_requires_explicit_signoff`):
/// this function returns control to its caller as soon as the
/// server's `WriteChunkedResponse` arrives. It does NOT hold any
/// borrowed writer reference across the RPC. The `reader` is owned
/// by-move; the caller's writer is independent of this call's
/// lifetime.
pub async fn write_chunked_stream(
    dispatcher: &dyn WriteChunkedDispatcher,
    digest: DigestInfo,
    mut reader: DropCloserReadHalf,
    options: ChunkedClientOptions,
    metrics: Arc<ChunkedClientMetrics>,
) -> Result<u64, Error> {
    metrics.attempted_total.fetch_add(1, Ordering::Relaxed);

    if options.chunk_size == 0 {
        return Err(make_err!(
            Code::InvalidArgument,
            "ChunkedClientOptions.chunk_size must be > 0; got 0 for digest {digest}"
        ));
    }
    if options.max_attempts == 0 {
        return Err(make_err!(
            Code::InvalidArgument,
            "ChunkedClientOptions.max_attempts must be > 0; got 0 for digest {digest}"
        ));
    }

    // Read the entire payload into chunks ONCE up-front. The buf-channel
    // reader is single-pass; we cannot reset it between retry attempts.
    // For typical worker mirror traffic the blob is <=64 MiB so peak
    // RSS is bounded; for the rare large-blob case the cost is still
    // O(blob_size) which the in-order ByteStream path also pays
    // (just spread differently). Computed SHA-256 hashes are
    // cached so the per-chunk hash burn happens ONCE even across
    // retries.
    let chunks = collect_and_hash_chunks(&mut reader, &digest, options.chunk_size).await?;

    let mut last_err: Option<Error> = None;
    for attempt in 1..=options.max_attempts {
        match send_one_attempt(dispatcher, digest, &chunks).await {
            Ok(committed_size) => {
                metrics.succeeded_total.fetch_add(1, Ordering::Relaxed);
                metrics
                    .bytes_sent_total
                    .fetch_add(committed_size, Ordering::Relaxed);
                if committed_size != digest.size_bytes() {
                    // Server returned a size that disagrees with the
                    // declared digest size. Treat as Internal — the
                    // server-side commit MUST equal the declared
                    // length per Phase 2.2/2.3 driver contract. A
                    // disagreement here means the wire OR the server
                    // miscounted; do not silently accept.
                    return Err(make_err!(
                        Code::Internal,
                        "WriteChunked server acked committed_size={committed_size} for digest {digest} \
                         (declared size {}); refusing to treat as durable",
                        digest.size_bytes()
                    ));
                }
                debug!(
                    %digest,
                    committed_size,
                    attempt,
                    "WriteChunked client: blob committed"
                );
                return Ok(committed_size);
            }
            Err(err) => {
                let retry_decision = classify_retryable(&err);
                match retry_decision {
                    RetryDecision::Retry { reason, retry_after } => {
                        match reason {
                            RetryReason::Aborted => {
                                metrics
                                    .aborted_retried_total
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            RetryReason::ResourceExhausted => {
                                metrics
                                    .resource_exhausted_total
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if attempt == options.max_attempts {
                            warn!(
                                %digest,
                                attempt,
                                ?reason,
                                "WriteChunked client: retry budget exhausted"
                            );
                            last_err = Some(err.append(format!(
                                "WriteChunked client gave up after {attempt} attempts \
                                 (last reason: {reason:?})"
                            )));
                            break;
                        }
                        info!(
                            %digest,
                            attempt,
                            ?reason,
                            retry_after_ms = retry_after.as_millis() as u64,
                            "WriteChunked client: retrying after server-hinted backoff"
                        );
                        tokio::time::sleep(retry_after).await;
                        continue;
                    }
                    RetryDecision::Abort => {
                        return Err(err);
                    }
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        make_err!(
            Code::Internal,
            "WriteChunked client exhausted retries with no last_err recorded for {digest}"
        )
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryReason {
    Aborted,
    ResourceExhausted,
}

#[derive(Debug, Clone, Copy)]
enum RetryDecision {
    Retry {
        reason: RetryReason,
        retry_after: Duration,
    },
    Abort,
}

/// Map the server's error → "retryable as a fresh attempt" or "give up".
///
/// Decision rules (per design §8.4 + §8.6):
/// - `Code::Aborted` carrying a `BackpressureSignal` (concurrent
///   same-digest stream rejection from
///   `chunked_write_handler.rs::handle_concurrent`) → retry after
///   the hint.
/// - `Code::ResourceExhausted` carrying a `BackpressureSignal`
///   (global budget OR per-blob mpsc full) → retry after the hint.
/// - Anything else → abort.
fn classify_retryable(err: &Error) -> RetryDecision {
    let has_signal = error_has_backpressure_signal(err);
    if !has_signal {
        return RetryDecision::Abort;
    }
    let reason = match err.code {
        Code::Aborted => RetryReason::Aborted,
        Code::ResourceExhausted => RetryReason::ResourceExhausted,
        // Backpressure signal attached to an unexpected code:
        // refuse to interpret. Producers must not retry on
        // arbitrary Codes; that risks a retry storm if the server
        // ever mis-tags an Internal error.
        _ => return RetryDecision::Abort,
    };
    let retry_after = decode_retry_after(err).min(MAX_RETRY_AFTER);
    RetryDecision::Retry { reason, retry_after }
}

/// Pull the `retry_after_ms` field out of the server's
/// `BackpressureSignal` detail. Falls back to a small default when
/// the proto fails to decode (shouldn't happen — encode_signal_any
/// is the only producer — but a malformed wire is not catastrophic
/// since we still cap at `MAX_RETRY_AFTER`).
fn decode_retry_after(err: &Error) -> Duration {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BACKPRESSURE_SIGNAL_TYPE_URL;
    for any in &err.details {
        if any.type_url != BACKPRESSURE_SIGNAL_TYPE_URL {
            continue;
        }
        match BackpressureSignal::decode(&*any.value) {
            Ok(signal) => return Duration::from_millis(signal.retry_after_ms),
            Err(decode_err) => {
                warn!(
                    ?decode_err,
                    "WriteChunked client: malformed BackpressureSignal detail; using fallback retry-after"
                );
            }
        }
    }
    // Fallback when the detail is missing OR unparseable. A short
    // backoff with mild jitter via the caller's overall retry shape.
    Duration::from_millis(50)
}

/// One attempt at a full WriteChunked stream. Sends every prepared
/// chunk via the dispatcher and waits for the server's
/// `WriteChunkedResponse`.
async fn send_one_attempt(
    dispatcher: &dyn WriteChunkedDispatcher,
    digest: DigestInfo,
    chunks: &[PreparedChunk],
) -> Result<u64, Error> {
    let proto_chunks: Vec<WriteChunk> =
        chunks.iter().map(|c| c.clone().into_proto(digest)).collect();
    let response = dispatcher
        .dispatch(proto_chunks)
        .await
        .err_tip(|| format!("WriteChunked dispatch failed for digest {digest}"))?;
    Ok(response.committed_size)
}

/// A single chunk pre-built (with SHA-256 already computed) so retries
/// don't pay the hash cost again.
#[derive(Debug, Clone)]
struct PreparedChunk {
    chunk_offset: u64,
    chunk_bytes: Bytes,
    chunk_sha256: [u8; 32],
    finish: bool,
}

impl PreparedChunk {
    fn into_proto(self, digest: DigestInfo) -> WriteChunk {
        WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: self.chunk_offset,
            chunk_bytes: self.chunk_bytes.to_vec(),
            chunk_sha256: self.chunk_sha256.to_vec(),
            finish_chunk: self.finish,
        }
    }
}

/// Read the full payload from `reader`, split into `chunk_size`-aligned
/// chunks, compute per-chunk SHA-256 on `spawn_blocking` (#213
/// perf-opt NMA1: do not block tokio workers on hash burn). Returns
/// the prepared chunk list ready for the wire.
///
/// Validates that the bytes received match the declared
/// `digest.size_bytes()` exactly; otherwise refuses to send anything
/// (the wire-side declared digest would mismatch the actual content).
async fn collect_and_hash_chunks(
    reader: &mut DropCloserReadHalf,
    digest: &DigestInfo,
    chunk_size: usize,
) -> Result<Vec<PreparedChunk>, Error> {
    let declared_size = digest.size_bytes();
    let mut chunks: Vec<PreparedChunk> = Vec::new();
    let mut current = BytesMut::with_capacity(chunk_size);
    let mut total_received: u64 = 0;
    let mut next_offset: u64 = 0;

    loop {
        let recv = reader
            .recv()
            .await
            .err_tip(|| format!("WriteChunked client: reader.recv() failed for digest {digest}"))?;
        if recv.is_empty() {
            // EOF.
            break;
        }
        total_received = total_received
            .checked_add(recv.len() as u64)
            .ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "WriteChunked client: total_received overflow for digest {digest}"
                )
            })?;
        if total_received > declared_size {
            return Err(make_input_err!(
                "WriteChunked client: reader produced more than declared size for digest {digest}: \
                 got > {declared_size} bytes"
            ));
        }
        // Append to the current chunk. May span more than one chunk
        // (if recv is large) so loop until consumed.
        let mut to_consume = recv;
        while !to_consume.is_empty() {
            let need = chunk_size - current.len();
            let take = to_consume.len().min(need);
            current.extend_from_slice(&to_consume[..take]);
            to_consume = to_consume.slice(take..);
            if current.len() == chunk_size {
                let bytes = current.split().freeze();
                let sha = hash_chunk_blocking(bytes.clone()).await?;
                chunks.push(PreparedChunk {
                    chunk_offset: next_offset,
                    chunk_bytes: bytes,
                    chunk_sha256: sha,
                    finish: false,
                });
                next_offset = next_offset
                    .checked_add(chunk_size as u64)
                    .ok_or_else(|| {
                        make_err!(
                            Code::Internal,
                            "WriteChunked client: next_offset overflow for digest {digest}"
                        )
                    })?;
            }
        }
    }
    if total_received != declared_size {
        return Err(make_input_err!(
            "WriteChunked client: reader produced {total_received} bytes but digest declared \
             {declared_size} for {digest}"
        ));
    }

    // Push the final chunk. For digest sizes that are an exact
    // multiple of CHUNK_SIZE, `current` is empty and the LAST chunk
    // already in `chunks` has finish=false. We need to flip its
    // finish flag to true (or push an empty finish chunk — but the
    // server expects the LAST data-bearing chunk to carry finish=true
    // when bytes line up exactly, NOT a separate empty finish for
    // non-zero blobs; per the §8.3 schema and the handler's
    // chunk-shape validation in `admit_chunk`, the final chunk's
    // chunk_bytes.len() can be < chunk_size OR == chunk_size, but for
    // non-final chunks must equal chunk_size).
    if !current.is_empty() {
        let bytes = current.split().freeze();
        let sha = hash_chunk_blocking(bytes.clone()).await?;
        chunks.push(PreparedChunk {
            chunk_offset: next_offset,
            chunk_bytes: bytes,
            chunk_sha256: sha,
            finish: true,
        });
    } else if let Some(last) = chunks.last_mut() {
        last.finish = true;
    } else {
        // declared_size > 0 with no chunks AND no current bytes is
        // impossible (we would have errored on the size mismatch
        // check above). Defensive return.
        return Err(make_err!(
            Code::Internal,
            "WriteChunked client: no chunks built for digest {digest} despite declared size > 0"
        ));
    }

    Ok(chunks)
}

/// SHA-256 a single chunk on `spawn_blocking`. `Bytes::clone()` is
/// O(1) (refcount bump), so the spawned task gets cheap zero-copy
/// access to the bytes.
async fn hash_chunk_blocking(bytes: Bytes) -> Result<[u8; 32], Error> {
    tokio::task::spawn_blocking(move || -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(&bytes);
        let out = h.finalize();
        let mut a = [0u8; 32];
        a.copy_from_slice(out.as_ref());
        a
    })
    .await
    .map_err(|join_err| {
        make_err!(
            Code::Internal,
            "WriteChunked client: spawn_blocking join error in per-chunk SHA-256: {join_err:?}"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::backpressure_signal::Reason;
    use nativelink_util::buf_channel::make_buf_channel_pair;

    use crate::chunked_signal::encode_backpressure_signal_any;

    fn sha256_of(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        let mut a = [0u8; 32];
        a.copy_from_slice(out.as_ref());
        a
    }

    /// `collect_and_hash_chunks` produces the expected number of
    /// chunks for a perfectly aligned blob and tags the LAST one
    /// with `finish=true`.
    #[tokio::test]
    async fn collect_aligned_blob_4_chunks_finish_on_last() {
        const CHUNK: usize = 4 * 1024;
        const N: usize = 4;
        let total = (N * CHUNK) as u64;
        let mut blob = Vec::with_capacity(N * CHUNK);
        for i in 0..N {
            blob.extend(std::iter::repeat(0xa0u8 + i as u8).take(CHUNK));
        }
        let digest = DigestInfo::new(sha256_of(&blob), total);

        let (mut tx, mut rx) = make_buf_channel_pair();
        let send = tokio::spawn(async move {
            tx.send(Bytes::from(blob)).await.unwrap();
            tx.send_eof().unwrap();
        });

        let chunks = collect_and_hash_chunks(&mut rx, &digest, CHUNK)
            .await
            .expect("chunk collection must succeed for aligned blob");
        send.await.unwrap();

        assert_eq!(chunks.len(), N, "expected {N} chunks for aligned blob");
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.chunk_offset, (i * CHUNK) as u64);
            assert_eq!(c.chunk_bytes.len(), CHUNK);
            assert_eq!(c.finish, i == N - 1, "only last chunk has finish=true");
            // Per-chunk SHA-256 matches.
            let expect = sha256_of(&c.chunk_bytes);
            assert_eq!(c.chunk_sha256, expect, "per-chunk SHA-256 must match");
        }
    }

    /// Blob whose size is NOT a multiple of CHUNK_SIZE produces a
    /// final chunk that is smaller than CHUNK_SIZE and carries
    /// finish=true.
    #[tokio::test]
    async fn collect_unaligned_blob_final_chunk_is_partial() {
        const CHUNK: usize = 1024;
        let blob: Vec<u8> = (0..(CHUNK * 2 + 17)).map(|i| (i & 0xff) as u8).collect();
        let total = blob.len() as u64;
        let digest = DigestInfo::new(sha256_of(&blob), total);

        let (mut tx, mut rx) = make_buf_channel_pair();
        let blob_for_send = blob.clone();
        let send = tokio::spawn(async move {
            tx.send(Bytes::from(blob_for_send)).await.unwrap();
            tx.send_eof().unwrap();
        });

        let chunks = collect_and_hash_chunks(&mut rx, &digest, CHUNK)
            .await
            .expect("must succeed for unaligned blob");
        send.await.unwrap();

        assert_eq!(chunks.len(), 3, "two full chunks + partial");
        assert_eq!(chunks[0].chunk_bytes.len(), CHUNK);
        assert_eq!(chunks[1].chunk_bytes.len(), CHUNK);
        assert_eq!(chunks[2].chunk_bytes.len(), 17);
        assert!(!chunks[0].finish);
        assert!(!chunks[1].finish);
        assert!(chunks[2].finish, "final partial chunk must carry finish=true");
        assert_eq!(chunks[2].chunk_offset, (CHUNK * 2) as u64);
    }

    /// Reader producing fewer bytes than declared digest.size errors
    /// before any chunk is sent. Guards against a partial-read
    /// silently shipping an inconsistent committed_size to the server.
    #[tokio::test]
    async fn collect_short_blob_returns_input_err() {
        const CHUNK: usize = 1024;
        let actual: Vec<u8> = vec![0u8; 500];
        let digest = DigestInfo::new(sha256_of(&actual), CHUNK as u64); // declares 1024, ships 500

        let (mut tx, mut rx) = make_buf_channel_pair();
        tokio::spawn(async move {
            tx.send(Bytes::from(actual)).await.unwrap();
            tx.send_eof().unwrap();
        });

        let err = collect_and_hash_chunks(&mut rx, &digest, CHUNK)
            .await
            .expect_err("must reject short read");
        assert_eq!(err.code, Code::InvalidArgument);
        assert!(
            err.messages
                .iter()
                .any(|m| m.contains("reader produced 500 bytes but digest declared 1024")),
            "must surface the actual vs declared mismatch; got {:?}",
            err.messages
        );
    }

    /// Reader producing MORE bytes than declared digest.size errors.
    #[tokio::test]
    async fn collect_long_blob_returns_input_err() {
        const CHUNK: usize = 1024;
        let actual: Vec<u8> = vec![0u8; 1500];
        let digest = DigestInfo::new(sha256_of(&actual), 1000);

        let (mut tx, mut rx) = make_buf_channel_pair();
        tokio::spawn(async move {
            tx.send(Bytes::from(actual)).await.unwrap();
            tx.send_eof().unwrap();
        });

        let err = collect_and_hash_chunks(&mut rx, &digest, CHUNK)
            .await
            .expect_err("must reject overlong read");
        assert_eq!(err.code, Code::InvalidArgument);
        assert!(
            err.messages
                .iter()
                .any(|m| m.contains("more than declared size")),
            "must surface overlong message; got {:?}",
            err.messages
        );
    }

    /// `classify_retryable` recognizes Aborted + BackpressureSignal
    /// as retryable.
    #[test]
    fn classify_aborted_with_backpressure_is_retry() {
        let any = encode_backpressure_signal_any(Reason::PerBlobMpscFull, 250);
        let err = Error::aborted_with_detail("concurrent-stream", any);
        match classify_retryable(&err) {
            RetryDecision::Retry { reason, retry_after } => {
                assert_eq!(reason, RetryReason::Aborted);
                assert_eq!(retry_after, Duration::from_millis(250));
            }
            RetryDecision::Abort => panic!("expected Retry; got Abort for {err:?}"),
        }
    }

    /// `classify_retryable` recognizes ResourceExhausted +
    /// BackpressureSignal as retryable.
    #[test]
    fn classify_resource_exhausted_with_backpressure_is_retry() {
        let any =
            encode_backpressure_signal_any(Reason::GlobalChunkBudgetExhausted, 100);
        let err = Error::resource_exhausted_backpressure("budget", any);
        match classify_retryable(&err) {
            RetryDecision::Retry { reason, retry_after } => {
                assert_eq!(reason, RetryReason::ResourceExhausted);
                assert_eq!(retry_after, Duration::from_millis(100));
            }
            RetryDecision::Abort => {
                panic!("expected Retry for ResourceExhausted+signal; got Abort: {err:?}");
            }
        }
    }

    /// `classify_retryable` does NOT retry on bare ResourceExhausted
    /// (no signal). That's the legacy h2 dead-channel shape; retrying
    /// here would mask a real transport issue.
    #[test]
    fn classify_bare_resource_exhausted_is_abort() {
        let err = make_err!(Code::ResourceExhausted, "no signal");
        matches!(classify_retryable(&err), RetryDecision::Abort)
            .then_some(())
            .expect("bare ResourceExhausted must NOT be retried");
    }

    /// `classify_retryable` does NOT retry on InvalidArgument even
    /// if the server (incorrectly) attached a BackpressureSignal —
    /// the producer must not retry malformed-input errors.
    #[test]
    fn classify_invalid_argument_with_signal_is_abort() {
        let any = encode_backpressure_signal_any(Reason::PerBlobMpscFull, 100);
        let mut err = make_input_err!("malformed");
        err.details.push(any);
        matches!(classify_retryable(&err), RetryDecision::Abort)
            .then_some(())
            .expect("InvalidArgument must NOT be retried regardless of signal");
    }

    /// `decode_retry_after` caps absurd hints at `MAX_RETRY_AFTER`.
    #[test]
    fn classify_retry_after_capped_at_max() {
        let any = encode_backpressure_signal_any(Reason::PerBlobMpscFull, u64::MAX);
        let err = Error::aborted_with_detail("crazy hint", any);
        match classify_retryable(&err) {
            RetryDecision::Retry { retry_after, .. } => {
                assert_eq!(
                    retry_after, MAX_RETRY_AFTER,
                    "u64::MAX retry-after must be capped at MAX_RETRY_AFTER"
                );
            }
            RetryDecision::Abort => panic!("expected Retry; got Abort"),
        }
    }
}
