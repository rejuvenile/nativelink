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
//! `CasExtensions/WriteChunked` client-streaming RPC. (#212 v4.5:
//! moved off `WorkerApi` so it routes via the worker's outbound
//! CAS-endpoint channel; see `cas_extensions.proto`.)
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
//!   per-blob-mpsc-cap (currently 256; bumped from 64 on 2026-05-12;
//!   was 16 before 2026-05-11; see #413 (200-540 MB blob cascade
//!   after cap=64) audit
//!   `.claude/audits/413-large-blob-cascade-design-20260511.md`)
//!   lives on the SERVER side; this client sends
//!   chunks in arrival order (the worker has them in offset order
//!   from `DropCloserReadHalf::recv`). Phase 2.5+ may add parallel
//!   send paths; today we do single-stream in-order.
//! - **No connection management** — the caller passes a tonic
//!   `GrpcService`-shaped channel/connection. Pool eviction on
//!   transport errors is the caller's responsibility (mirrors the
//!   existing `evict_pool_on_transport_err` flow on `GrpcStore`).

#![cfg(feature = "chunked_fast_slow")]

use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::cas_extensions_client::CasExtensionsClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BackpressureSignal, WriteChunk, WriteChunkedResponse,
};
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, default_digest_hasher_func};
use prost::Message as _;
use tracing::{debug, info, warn};

use crate::chunked::ChunkedWriteSource;
use crate::chunked_signal::{error_has_backpressure_signal, error_has_watchdog_timeout_signal};

/// Type alias for the boxed-and-pinned future returned by
/// `WriteChunkedDispatcher::dispatch`. Manually-spelled rather than
/// `async fn` to keep the future's bounds explicit (Send + 'static)
/// and to dodge the `CoerceUnsized` higher-ranked-lifetime error
/// `async_trait` produces with deeply-nested `T: GrpcService<...>`
/// bounds in the impl.
pub type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<WriteChunkedResponse, Error>> + Send + 'static>>;

/// Transport-agnostic dispatcher for one `CasExtensions/WriteChunked`
/// RPC. (#212 v4.5: was `WorkerApi/WriteChunked` before the routing
/// fix.) Decoupling the chunked-write logic from the underlying
/// transport (a) keeps generic monomorphization shallow (the deep
/// generic stack of `tonic::client::GrpcService<...>` blew up
/// rustc's `optimized_mir` query during initial implementation) and
/// (b) lets tests substitute an in-process dispatcher without
/// spinning up tonic's full machinery.
///
/// Production wiring lives in `nativelink-store::grpc_store`, which
/// constructs a `WorkerApiWriteChunkedV2Dispatcher` (TCP / QUIC / Dual
/// transport variants). The legacy v1 dispatcher has been removed.
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

/// #494-v3 Phase 2: v2 dispatcher using the bidi `WriteChunkedV2`
/// RPC. Sends every chunk and consumes the per-chunk ack stream,
/// returning the final `WriteChunkedResponse` once received.
///
/// **Why a separate dispatcher type:** the v1 dispatcher returns a
/// unary `WriteChunkedResponse`; the v2 dispatcher operates on the
/// bidi (server-streaming response) shape. Both implement the same
/// `WriteChunkedDispatcher` trait so callers can swap them
/// transparently — `WriteChunkedDispatcher::dispatch` flattens the v2
/// response stream into a single `WriteChunkedResponse` (i.e., the
/// final frame).
///
/// **Reactivity (passive):** this dispatcher does NOT skip chunks on
/// `ALREADY_HAVE` / `RACING_LOSER` acks. The server already handles
/// dedup transparently, so passive sending is correctness-equivalent
/// to active skipping; the only cost is wasted bandwidth (~256 MiB/
/// writer/digest worst case, accepted per the #494-v3 design). A
/// future optimization can split the send loop and the recv loop into
/// two tasks for active skipping.
pub struct WorkerApiWriteChunkedV2Dispatcher<T> {
    acquire_channel:
        Arc<dyn Fn() -> ChannelAcquireFuture<T> + Send + Sync + 'static>,
}

impl<T> core::fmt::Debug for WorkerApiWriteChunkedV2Dispatcher<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WorkerApiWriteChunkedV2Dispatcher")
            .finish_non_exhaustive()
    }
}

impl<T> WorkerApiWriteChunkedV2Dispatcher<T> {
    /// Construct a v2 dispatcher with a custom channel-acquisition
    /// factory. Mirrors the v1 ctor.
    pub fn with_factory<F>(acquire_channel: F) -> Self
    where
        F: Fn() -> ChannelAcquireFuture<T> + Send + Sync + 'static,
    {
        Self {
            acquire_channel: Arc::new(acquire_channel),
        }
    }
}

impl<T> WriteChunkedDispatcher for WorkerApiWriteChunkedV2Dispatcher<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    T::Future: Send,
{
    fn dispatch(&self, chunks: Vec<WriteChunk>) -> DispatchFuture {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::write_chunked_frame;
        use tokio_stream::StreamExt as _;
        let factory = Arc::clone(&self.acquire_channel);
        // #247+#477 DS-reviewer disambiguation: identify the wire shape
        // of this dispatch attempt + the digest extracted from the
        // first chunk. Worker-side log; emits once per attempt.
        let digest_str = chunks
            .first()
            .and_then(|c| c.digest.as_ref())
            .map_or_else(
                || "<no-first-chunk>".to_string(),
                |d| format!("{}-{}", d.hash, d.size_bytes),
            );
        let chunk_count = chunks.len();
        info!(
            target: "nativelink_store::chunked::chunked_client",
            writer_path = "worker_dispatch_v2",
            wire_shape = "v2",
            digest = %digest_str,
            chunk_count,
            "WriteChunkedV2 dispatch attempt",
        );
        Box::pin(async move {
            let channel = factory().await?;
            let stream = tokio_stream::iter(chunks);
            let mut client = CasExtensionsClient::new(channel);
            let response = client
                .write_chunked_v2(stream)
                .await
                .map_err(|status| {
                    let err: Error = status.into();
                    err.append("CasExtensions/WriteChunkedV2 RPC failed".to_string())
                })?;
            let mut frame_stream = response.into_inner();
            let mut final_response: Option<WriteChunkedResponse> = None;
            // Drain the response stream. Per-chunk acks are observed
            // (and could be acted on for chunk-skip optimization in a
            // future patch); the FinalResponse terminates.
            while let Some(frame_result) = frame_stream.next().await {
                let frame = frame_result.map_err(|status| {
                    let err: Error = status.into();
                    err.append("CasExtensions/WriteChunkedV2 stream errored".to_string())
                })?;
                match frame.payload {
                    Some(write_chunked_frame::Payload::Ack(_ack)) => {
                        // Passive consumption — server handles dedup
                        // transparently. Future optimization: drive a
                        // chunk-skip queue here.
                        continue;
                    }
                    Some(write_chunked_frame::Payload::FinalResponse(resp)) => {
                        final_response = Some(resp);
                        break;
                    }
                    None => {
                        return Err(make_err!(
                            Code::Internal,
                            "WriteChunkedV2 frame had no payload"
                        ));
                    }
                }
            }
            final_response.ok_or_else(|| {
                make_err!(
                    Code::Internal,
                    "WriteChunkedV2 stream ended without FinalResponse"
                )
            })
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
    /// #286 sub-item 3 fixup (code-reviewer MAJOR): per-attempt
    /// retries triggered by a watchdog-tagged
    /// `Code::DeadlineExceeded` (carrying a `WatchdogTimeoutSignal`
    /// detail). Distinct from `resource_exhausted_total` — operators
    /// reading these gauges separately can distinguish (a) genuine
    /// global-budget rejections (Q8 admission backpressure) from
    /// (b) slow-tier wedges trip the server's commit watchdog.
    /// Conflating them under `resource_exhausted_total` would mask
    /// a slow-tier wedge as an admission-backpressure signal — the
    /// red-team P1 finding for #286.
    pub watchdog_retried_total: AtomicU64,
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
///
/// #548 Phase 1: `Default` is intentionally NOT implemented. The
/// `source` discriminator is load-bearing for Phase 4 (#551) — silently
/// defaulting to `Worker` at any caller would be a CAS-poisoning attack
/// window when the BIS-elision branch lands (a Bazel caller silently
/// classified as Worker would skip the BIS broadcast and the worker
/// fleet would never see the digest). Each construction site MUST
/// specify `source` explicitly so a future contributor adding a new
/// caller is forced to think about WHO produced the bytes.
#[derive(Debug, Clone, Copy)]
pub struct ChunkedClientOptions {
    /// Maximum number of full-blob attempts before giving up.
    pub max_attempts: u32,
    /// Production chunk size in bytes. Production callers pass
    /// `CHUNK_SIZE` (1 MiB); tests pass smaller for speed.
    pub chunk_size: usize,
    /// #548 Phase 1: source of the bytes being written. Threaded
    /// end-to-end for Phase 4 (#551) to consume; Phase 1 stores the
    /// value but does NOT branch on it. Required at every construction
    /// site — no `Default` (see struct doc).
    pub source: ChunkedWriteSource,
}

/// Send a single CAS blob via the `CasExtensions/WriteChunked` RPC.
/// (#212 v4.5: routed via CasExtensions, not WorkerApi.)
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
    // #247+#477 DS-reviewer disambiguation: emit one info! per
    // worker-side WriteChunked dispatch entry so a worker-log scan can
    // attribute every worker→server chunked-write attempt to the
    // worker dispatcher. NOTE: worker logs route to
    // `~/Library/Logs/nativelink-worker.log` (macOS launchd) — they do
    // NOT reach the server's journal. Cross-host correlation requires
    // pulling both. The wire shape (v2) is logged separately
    // inside the dispatcher impl (`WorkerApiWriteChunkedV2Dispatcher::dispatch`).
    // #548 Phase 1 (Item 7): demoted info!→debug! after adding the
    // `source` field. Per CLAUDE.md "info! for state transitions", an
    // entry-point log is not a state transition; keeping it at info!
    // would expand the production journal by one extra field per
    // chunked-write start. debug! preserves opt-in observability
    // without survival past `release_max_level_info`.
    debug!(
        target: "nativelink_store::chunked::chunked_client",
        writer_path = "worker_chunked_client",
        %digest,
        expected_size = digest.size_bytes(),
        max_attempts = options.max_attempts,
        chunk_size = options.chunk_size,
        source = ?options.source,
        "write_chunked_stream entry",
    );
    let _phase4_source = options.source; // PHASE 4 (#551): branch on source == Worker — post-tonic-Ok return below suffices to release the worker pin under the "one SIGKILL" invariant (#545/#546); Phase 1 only THREADS the value

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
    // The dispatch path is gated by `MAX_CHUNKED_BLOB_SIZE` (256 MiB,
    // see #212 Phase 2.4 fixup B1 part 1) so peak in-memory cost is
    // bounded. Computed SHA-256 hashes are cached so the per-chunk
    // hash burn happens ONCE even across retries. Streaming retry
    // (which would lift the cap) is deferred to Phase 2.5+.
    let chunks = collect_and_hash_chunks(&mut reader, &digest, options.chunk_size).await?;

    // #212 Phase 2.4 fixup M2 (perf-optimizer): build the wire-side
    // `Vec<WriteChunk>` ONCE outside the retry loop. Each retry
    // attempt clones it (Bytes payload is refcounted so the chunk
    // bodies share a single backing buffer; only the Vec header +
    // per-chunk WriteChunk struct are duplicated — bounded at
    // ~CHUNK_COUNT * sizeof(WriteChunk) per retry). The previous
    // shape rebuilt the Vec per attempt, which doubled the small-
    // alloc churn under the retry path.
    let proto_chunks_template: Vec<WriteChunk> = chunks
        .into_iter()
        .map(|c| c.into_proto(digest))
        .collect();

    let mut last_err: Option<Error> = None;
    for attempt in 1..=options.max_attempts {
        match send_one_attempt(dispatcher, digest, &proto_chunks_template).await {
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
                let RetryDecision::Retry { reason, retry_after } = classify_retryable(&err)
                else {
                    return Err(err);
                };
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
                    RetryReason::WatchdogDeadline => {
                        metrics
                            .watchdog_retried_total
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
    /// #286 sub-item 3 fixup (code-reviewer MAJOR): the server's
    /// chunked-commit watchdog fired. Distinct from
    /// `ResourceExhausted` so operators can distinguish (a) genuine
    /// global-budget rejection from (b) slow-tier wedge — both are
    /// transient but they imply different mitigations. Drives a
    /// dedicated `watchdog_retried_total` metric counter.
    WatchdogDeadline,
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
/// - `Code::DeadlineExceeded` carrying a `WatchdogTimeoutSignal`
///   (#286 sub-item 3 fixup, red-team P1) → retry after a small
///   fallback backoff. The discriminator is load-bearing: the
///   server-side chunked commit watchdog
///   (`chunked_write_handler::run_async_commit_reaper`,
///   `CHUNKED_COMMIT_WATCHDOG_SECS=60`) synthesises a tagged
///   `Code::DeadlineExceeded` carrying the
///   `WatchdogTimeoutSignal` discriminator. The client retries
///   the WHOLE blob from byte 0 inside the existing 3-attempt
///   loop (the watchdog implies a transient slow-tier wedge — the
///   right shape for a fresh full-blob attempt).
///
///   **A bare `Code::DeadlineExceeded` (no discriminator) maps to
///   `Abort`.** Critical defense against red-team P1: the
///   #286/#283 retry arm was originally written as "any
///   `DeadlineExceeded` retries", which would silently swallow
///   future per-RPC `tonic::Request::set_timeout` deadlines, the
///   chunk-driver per-pwrite + e2e SHA timeouts in
///   `chunked_driver.rs`, and any other non-watchdog
///   `DeadlineExceeded`. Each of those is "give up", not "retry
///   the whole blob"; conflating them with watchdog firings
///   re-creates the #203 OOM-cascade shape (slow-tier transient
///   → upstream stall → per-chunk timeout → retry storm →
///   in-flight growth → SIGKILL).
/// - Anything else → abort.
fn classify_retryable(err: &Error) -> RetryDecision {
    // #286 sub-item 3 (red-team P1 fixup): retry only on
    // watchdog-tagged DeadlineExceeded errors. Bare
    // DeadlineExceeded (no discriminator) maps to Abort below.
    if err.code == Code::DeadlineExceeded && error_has_watchdog_timeout_signal(err) {
        let retry_after = decode_retry_after(err).min(MAX_RETRY_AFTER);
        return RetryDecision::Retry {
            reason: RetryReason::WatchdogDeadline,
            retry_after,
        };
    }
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
    use nativelink_proto::type_urls::BACKPRESSURE_SIGNAL_TYPE_URL;
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
///
/// `proto_chunks_template` is the wire-side `Vec<WriteChunk>` built
/// ONCE upstream (M2 fixup); per-attempt cost is the Vec clone
/// (header + per-chunk struct dup; the `Bytes` payloads are refcounted
/// shares of the original chunk buffers — no deep copy).
async fn send_one_attempt(
    dispatcher: &dyn WriteChunkedDispatcher,
    digest: DigestInfo,
    proto_chunks_template: &[WriteChunk],
) -> Result<u64, Error> {
    let proto_chunks: Vec<WriteChunk> = proto_chunks_template.to_vec();
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
    /// Build the wire-side `WriteChunk` proto. The `chunk_bytes`
    /// field is `bytes::Bytes` after the #212 Phase 2.4 fixup
    /// (`#[prost(bytes = "bytes")]` on the proto struct), so the
    /// move below is a refcount bump — no per-retry deep copy. The
    /// previous implementation called `chunk_bytes.to_vec()`, which
    /// memcpyed the full chunk into a fresh `Vec<u8>` on every
    /// attempt; that was the M1 finding from the perf-optimizer review.
    fn into_proto(self, digest: DigestInfo) -> WriteChunk {
        WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: self.chunk_offset,
            chunk_bytes: self.chunk_bytes,
            // `WriteChunk.chunk_sha256` is `bytes::Bytes` (proto `bytes`
            // mapped via `config.bytes(["."])`); copy the fixed 32-byte
            // hash in directly.
            chunk_sha256: Bytes::copy_from_slice(&self.chunk_sha256),
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
        // Unreachable when the caller honors the production size gate
        // (`digest.size_bytes() >= CHUNK_SIZE` in `GrpcStore::update`):
        // a declared-zero blob never enters this function. The
        // mismatch check above already errors when `total_received !=
        // declared_size`, so reaching this branch means
        // `total_received == declared_size == 0` AND no chunks were
        // pushed — possible only if a future caller passes
        // `declared_size = 0` directly. Treat as a contract violation
        // in the caller (per CLAUDE.md, no defensive Ok-fallback for
        // an unreachable shape; surface the error instead).
        return Err(make_err!(
            Code::Internal,
            "WriteChunked client: no chunks built for digest {digest} despite declared size > 0; \
             caller violated the size-gate contract (must pre-check declared_size > 0)"
        ));
    }

    Ok(chunks)
}

/// Hash a single chunk on `spawn_blocking` using the process-wide
/// default digest hasher (BLAKE3 in production, SHA-256 in tests).
/// `Bytes::clone()` is O(1) (refcount bump), so the spawned task gets
/// cheap zero-copy access to the bytes. #228 fix: the previous
/// hardcoded Sha256 mismatched BLAKE3-named declared digests.
async fn hash_chunk_blocking(bytes: Bytes) -> Result<[u8; 32], Error> {
    tokio::task::spawn_blocking(move || -> [u8; 32] {
        let mut h = default_digest_hasher_func().hasher();
        h.update(&bytes);
        let info = h.finalize_digest();
        **info.packed_hash()
    })
    .await
    .map_err(|join_err| {
        make_err!(
            Code::Internal,
            "WriteChunked client: spawn_blocking join error in per-chunk hash: {join_err:?}"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_macro::nativelink_test;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::backpressure_signal::Reason;
    use nativelink_util::buf_channel::make_buf_channel_pair;
    // Tests pin the digest function to Sha256 (the unit-test default
    // returned by `default_digest_hasher_func()` when nothing has been
    // set process-wide) so the production hashing path computes hashes
    // matching this ground-truth helper. #228 production fix routes
    // through `DigestHasher` instead of hardcoded sha2 in the
    // production code; tests stay on sha2 directly so the mismatch
    // surface is the single equality check.
    use sha2::{Digest as _, Sha256};

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
    #[nativelink_test]
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
    #[nativelink_test]
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
    #[nativelink_test]
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
    #[nativelink_test]
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
    #[nativelink_test]
    async fn classify_aborted_with_backpressure_is_retry() {
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
    #[nativelink_test]
    async fn classify_resource_exhausted_with_backpressure_is_retry() {
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
    #[nativelink_test]
    async fn classify_bare_resource_exhausted_is_abort() {
        let err = make_err!(Code::ResourceExhausted, "no signal");
        matches!(classify_retryable(&err), RetryDecision::Abort)
            .then_some(())
            .expect("bare ResourceExhausted must NOT be retried");
    }

    /// `classify_retryable` does NOT retry on InvalidArgument even
    /// if the server (incorrectly) attached a BackpressureSignal —
    /// the producer must not retry malformed-input errors.
    #[nativelink_test]
    async fn classify_invalid_argument_with_signal_is_abort() {
        let any = encode_backpressure_signal_any(Reason::PerBlobMpscFull, 100);
        let mut err = make_input_err!("malformed");
        err.details.push(any);
        matches!(classify_retryable(&err), RetryDecision::Abort)
            .then_some(())
            .expect("InvalidArgument must NOT be retried regardless of signal");
    }

    /// `decode_retry_after` caps absurd hints at `MAX_RETRY_AFTER`.
    #[nativelink_test]
    async fn classify_retry_after_capped_at_max() {
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

    /// **#286 sub-item 3 fixup (red-team P1, code-reviewer MAJOR):
    /// watchdog-tagged DeadlineExceeded retries.** A
    /// `Code::DeadlineExceeded` carrying a `WatchdogTimeoutSignal`
    /// detail (synthesised by `run_async_commit_reaper` in
    /// `chunked_write_handler.rs`) MUST classify as `Retry` with the
    /// `WatchdogDeadline` reason. The dedicated reason variant drives
    /// the `watchdog_retried_total` metric counter, separate from
    /// `resource_exhausted_total` so operators can distinguish
    /// genuine global-budget rejection from slow-tier wedge.
    ///
    /// **Mutation step (run at fixup authorship):** revert the
    /// `if ... && error_has_watchdog_timeout_signal(err) { ... }`
    /// gate in `classify_retryable` to a bare `if err.code ==
    /// Code::DeadlineExceeded`. The discriminator-tagged Err still
    /// classifies as Retry — but so does a bare DeadlineExceeded
    /// (the over-action test below would red-fail). The pair of
    /// tests guards both directions of the contract.
    #[nativelink_test]
    async fn classify_watchdog_tagged_deadline_exceeded_is_retry() {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::watchdog_timeout_signal;

        use crate::chunked_signal::encode_watchdog_timeout_signal_any;

        let detail = encode_watchdog_timeout_signal_any(
            watchdog_timeout_signal::Reason::ChunkedCommitWatchdog,
            60,
        );
        let err = Error::deadline_exceeded_with_detail(
            "chunked commit await_completion exceeded 60 s watchdog deadline",
            detail,
        );
        match classify_retryable(&err) {
            RetryDecision::Retry { reason, retry_after } => {
                assert_eq!(
                    reason,
                    RetryReason::WatchdogDeadline,
                    "watchdog-tagged DeadlineExceeded MUST map to the \
                     WatchdogDeadline reason variant — it drives the \
                     dedicated watchdog_retried_total metric counter, \
                     which operators read separately from \
                     resource_exhausted_total to distinguish slow-tier \
                     wedge from genuine global-budget rejection",
                );
                assert!(
                    retry_after <= MAX_RETRY_AFTER,
                    "retry-after must be bounded above by MAX_RETRY_AFTER \
                     even when the server attaches no hint; got={retry_after:?}",
                );
            }
            RetryDecision::Abort => panic!(
                "watchdog-tagged DeadlineExceeded MUST be retryable — \
                 #286 sub-item 3 regression: the discriminator gate \
                 must accept the watchdog detail"
            ),
        }
    }

    /// **#286 sub-item 3 fixup over-action (red-team P1):** a BARE
    /// `Code::DeadlineExceeded` (no `WatchdogTimeoutSignal` detail)
    /// MUST classify as `Abort`. This is the load-bearing red-team
    /// finding: without the discriminator gate, every
    /// `DeadlineExceeded` looks the same to the client, and a future
    /// blanket `tonic::Request::set_timeout` (a single-line config
    /// change anywhere in the call graph) would silently inherit the
    /// retry behavior intended only for the server-side watchdog —
    /// recreating the #203 OOM-cascade shape (slow-tier transient →
    /// upstream stall → per-chunk timeout → retry storm → in-flight
    /// growth → SIGKILL) at fleet scale.
    ///
    /// **Mutation step (run at fixup authorship):** drop the
    /// `&& error_has_watchdog_timeout_signal(err)` clause from the
    /// gate. Bare-DeadlineExceeded then classifies as Retry; this
    /// test red-fails with the bespoke `"bare DeadlineExceeded MUST
    /// classify as Abort"` message.
    #[nativelink_test]
    async fn classify_bare_deadline_exceeded_is_abort() {
        let err = make_err!(
            Code::DeadlineExceeded,
            "per-chunk pwrite timeout (chunked_driver.rs)"
        );
        assert!(
            matches!(classify_retryable(&err), RetryDecision::Abort),
            "bare DeadlineExceeded (no WatchdogTimeoutSignal detail) MUST \
             classify as Abort — without this gate, future per-RPC \
             tonic::Request::set_timeout deadlines and the chunked-driver \
             per-pwrite/e2e SHA timeouts would silently inherit the \
             watchdog retry behavior, recreating the #203 OOM-cascade \
             shape (red-team P1 finding for #286)"
        );
    }

    /// **#286 sub-item 3 fixup over-action (testing-czar MAJOR-2(b)):**
    /// classify_retryable must NOT retry on
    /// `Code::FailedPrecondition` when no detail is attached. The new
    /// DeadlineExceeded discriminator arm sits BEFORE the `has_signal`
    /// early-return; without this regression test, a future refactor
    /// that promotes the wildcard could silently make all errors
    /// retryable, hiding genuine "give up" signals.
    #[nativelink_test]
    async fn classify_failed_precondition_no_detail_is_abort() {
        let err = make_err!(
            Code::FailedPrecondition,
            "missing input precondition (e.g. blob not found)"
        );
        assert!(
            matches!(classify_retryable(&err), RetryDecision::Abort),
            "FailedPrecondition (no detail) MUST classify as Abort — \
             FailedPrecondition is a permanent caller error, retrying \
             would mask the real bug. This test guards the scope of \
             the new DeadlineExceeded arm: the discriminator gate \
             must NOT degrade the existing default-Abort behavior \
             for unrelated codes."
        );
    }

    /// **#550 Phase 3 (red-team version-skew finding):** a worker that
    /// has opted into V2 (`chunked_v2_writes_enabled = true`) but talks
    /// to a server without the `WriteChunkedV2` handler receives
    /// `Code::Unimplemented`. That error carries no `BackpressureSignal`
    /// and is not watchdog-tagged, so it MUST classify as `Abort`: the
    /// write fails loud and fast — no retry storm, no hang, and (by
    /// design) no silent fallback to the V1 dispatcher. This makes the
    /// "version skew is safe" claim explicit rather than relying on the
    /// implicit `!has_signal → Abort` fallthrough.
    #[nativelink_test]
    async fn classify_unimplemented_is_abort() {
        let err = make_err!(
            Code::Unimplemented,
            "server does not implement WriteChunkedV2"
        );
        assert!(
            matches!(classify_retryable(&err), RetryDecision::Abort),
            "Code::Unimplemented (no BackpressureSignal) MUST classify \
             as Abort — a V2-enabled worker hitting a server without the \
             WriteChunkedV2 handler must fail loud, not retry-storm or \
             hang. There is no fallback to the V1 dispatcher by design."
        );
    }

}
