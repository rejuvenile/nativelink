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
#[cfg(feature = "chunked_fast_slow")]
use core::sync::atomic::AtomicBool;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{FuturesUnordered, unfold};
use futures::{Future, Stream, StreamExt, TryStreamExt, future};
use nativelink_config::stores::GrpcSpec;
use nativelink_error::{Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetActionResultRequest, GetTreeRequest, GetTreeResponse, UpdateActionResultRequest,
    batch_update_blobs_request, compressor,
};
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::connection_manager::ConnectionManager;
use nativelink_util::digest_hasher::{DigestHasherFunc, default_digest_hasher_func};
use nativelink_util::health_utils::HealthStatusIndicator;
use nativelink_util::proto_stream_utils::{
    FirstStream, ReadProgressObserver, WriteRequestStreamWrapper, WriteState, WriteStateWrapper,
};
use nativelink_util::resource_info::ResourceInfo;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{
    IS_AC_PEER_FETCH, IS_MIRROR_REQUEST, IS_WORKER_REQUEST, ItemCallback, DurableDelegation, MarkStableDelegation,
    PinDelegation, StableDigestDelegation, StoreDriver, StoreKey, StoreOptimizations,
    UploadSizeInfo,
};
use nativelink_util::{default_health_status_indicator, tls_utils};
use opentelemetry::context::Context;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tonic::{Code, IntoRequest, Request, Response, Status, Streaming};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

// This store is usually a pass-through store, but can also be used as a CAS store. Using it as an
/// Maximum gRPC message decoding size. Must be larger than the biggest
/// possible response (e.g. batch_read_blobs, get_tree, or a single
/// ByteStream ReadResponse chunk). 256 MiB is generous while still
/// providing an OOM safety net.
const MAX_GRPC_DECODING_SIZE: usize = 256 * 1024 * 1024;

// AC store has one major side-effect... The has() function may not give the proper size of the
// underlying data. This might cause issues if embedded in certain stores.
struct PendingBatchEntry {
    digest: DigestInfo,
    data: Bytes,
    result_tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
}

/// Transport backend: TCP pool, QUIC channel, or both with per-RPC
/// selection based on benchmark data.
enum Transport {
    Tcp(ConnectionManager),
    #[cfg(feature = "quic")]
    Quic(tls_utils::QuicChannel),
    /// Dual transport: holds both TCP and QUIC connections. RPCs are
    /// routed to the best transport based on benchmark data:
    /// - QUIC: FindMissing, BatchUpdate, BatchRead, single-stream reads,
    ///   AC lookups, small oneshot writes
    /// - TCP: parallel chunked reads, large streaming writes
    #[cfg(feature = "quic")]
    Dual {
        tcp: ConnectionManager,
        quic: tls_utils::QuicChannel,
    },
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(cm) => f.debug_tuple("Tcp").field(cm).finish(),
            #[cfg(feature = "quic")]
            Self::Quic(_) => write!(f, "Quic"),
            #[cfg(feature = "quic")]
            Self::Dual { .. } => write!(f, "Dual(tcp+quic)"),
        }
    }
}

/// Outcome of one chunk-fetch attempt, used by `get_part_parallel`'s
/// post-loop classifier.
#[derive(Debug, PartialEq, Eq)]
enum ChunkAttemptOutcome {
    /// Got `chunk_length` bytes — the chunk is complete.
    Complete,
    /// `stream.next()` returned `None` (the peer's `Status::OK` trailer)
    /// before `chunk_length` bytes arrived. The bytestream contract says
    /// this is success: the resource is shorter than the requested range.
    /// Forensic: a 183-byte blob requested via the parallel path
    /// (`chunk_length` >> 183) used to land here and get misclassified
    /// as `DataLoss`, leading to the digest 1e08eefa…-183 zombie loop.
    CleanShort,
    /// The loop broke on an empty data frame with no terminal trailer
    /// observed yet. Most common cause: server-side `tx` channel
    /// dropped without a Status frame (e.g. FastSlowStore's
    /// `data_stream_fut` canceled mid-flight). Retry the residual
    /// range — the next attempt will either complete or surface a
    /// real status.
    AmbiguousEarlyBreak,
}

/// Returns `true` if `err` looks like a transport-level h2 failure
/// (post-GOAWAY stale-channel shape, #147). The production signature
/// (verified in buildcache journal 2026-04-25):
/// `Error { code: Internal, messages: ["Tried to send while stream is closed", ...] }`.
/// `Unavailable` / `Unknown` cover RST_STREAM / catch-all h2 mappings.
/// `ResourceExhausted` covers the rare case where tonic surfaces
/// `ENHANCE_YOUR_CALM` directly (server's GOAWAY reason) — but ONLY
/// when the error does NOT carry a `BackpressureSignal` discriminator
/// detail. With the discriminator present, the peer is asserting
/// honest #212 Q8 backpressure (per-blob mpsc full or global byte
/// budget exhausted) and evicting the h2 channel would tear down the
/// connection at backpressure rate. See design §13.1.1 point 2.
///
/// `Internal` is gated on a message check to avoid evicting on
/// server-app `make_err!(Internal, ...)` from valid RPCs. The h2 GOAWAY
/// shapes covered:
///   * "Tried to send while stream is closed" — production wedge signature.
///   * "h2 protocol error" — generic h2 framing/protocol failure.
///   * "buffer's worker closed unexpectedly" — h2 buffer task died.
///   * "h2 connection error" — the long-form h2 connection error message.
///   * "connection error" — the short-form h2 connection error message
///     (covers "connection error: the server sent GOAWAY" and similar
///     hyper/h2 surfacings).
///   * "broken pipe" — write to a half-closed socket.
fn looks_like_dead_channel(err: &Error) -> bool {
    match err.code {
        Code::Unavailable | Code::Unknown => true,
        Code::ResourceExhausted => {
            // #212 §13.1.1 point 2: a `BackpressureSignal` discriminator
            // means the peer is asserting honest backpressure. NEVER
            // evict the h2 channel in that case — production saturation
            // would replay #147 at backpressure rate.
            !crate::chunked_signal::error_has_backpressure_signal(err)
        }
        Code::Internal => err.messages.iter().any(|m| {
            m.contains("Tried to send while stream is closed")
                || m.contains("h2 protocol error")
                || m.contains("buffer's worker closed unexpectedly")
                || m.contains("h2 connection error")
                || m.contains("connection error")
                || m.contains("broken pipe")
        }),
        _ => false,
    }
}

/// Maximum elapsed time (ms) for an attempt to be classified as an
/// instant-fail latched-pool hit. tower-Buffer's `ServiceError` surfaces
/// synchronously (the buffer's worker holds the latch in RAM and returns
/// immediately), so genuine latched-pool attempts complete in <5ms.
/// We use 50ms to give ample headroom for scheduler jitter while still
/// being far below the minimum genuine transport RTT (~100ms LAN, ~1ms loopback).
const LATCHED_POOL_INSTANT_FAIL_MS: u64 = 50;

/// Number of consecutive instant latched-pool failures required before
/// aborting retries early. N=2 ensures we do not abort on a single
/// coincidentally-fast transient; after Fix-A, the first failure evicts
/// the dead channel — if the SECOND attempt (drawing a fresh or still-idle
/// channel from a pool of 32) also instant-fails, the pool is systemically
/// latched and further retries will not help within the retry budget.
const LATCHED_POOL_ABORT_THRESHOLD: u32 = 2;

/// Returns `true` if `err` is the tower-Buffer ServiceError that appears
/// when a channel's internal service worker panics or is dropped — the
/// "latched pool" signature. This is distinct from `looks_like_dead_channel`
/// (which covers GOAWAY, broken pipe, etc. and is used for eviction);
/// this predicate is ONLY used to decide whether to abort retries early
/// (#2 Fix-C).
///
/// **Why message matching:** tonic wraps the tower `BoxError` as
/// `Status::Unknown` with no typed discriminator; the only way to
/// distinguish it from other Unknown errors is the message text. The
/// message `"buffered service failed: ..."` is stable — it is the
/// `Display` impl of `tower::buffer::error::ServiceError` (see
/// <https://docs.rs/tower/latest/tower/buffer/error/struct.ServiceError.html>).
/// We deliberately match a SUBSTRING rather than an exact prefix to be
/// robust against upstream tonic/tower message composition changes.
fn looks_like_latched_pool(err: &Error, elapsed_ms: u64) -> bool {
    #[cfg(feature = "failpoints")]
    // Allow tests to bypass the timing gate so they don't depend on
    // loopback RTT being below LATCHED_POOL_INSTANT_FAIL_MS.
    fail::fail_point!("latched_pool_bypass_timing", |_| {
        return err.code == Code::Unknown
            && err.messages.iter().any(|m| m.contains("buffered service"));
    });
    if elapsed_ms > LATCHED_POOL_INSTANT_FAIL_MS {
        return false; // Not instant — genuine transport RTT, not a latch.
    }
    err.code == Code::Unknown
        && err
            .messages
            .iter()
            .any(|m| m.contains("buffered service"))
}

/// #FU QUIC read-failure diagnostic: which side of the latch-vs-backpressure
/// split a failed QUIC read attempt falls on. The split is the SAME
/// `LATCHED_POOL_INSTANT_FAIL_MS` (50 ms) boundary `looks_like_latched_pool`
/// uses to classify a latch, so the two buckets answer the discriminator
/// directly: a tight `Instant` cluster (`elapsed ≤ 50 ms`, mostly ≤5 ms)
/// confirms a genuine instant latch; a `Slow` skew (`elapsed > 50 ms`,
/// near the timeout bound) confirms live backpressure the ≤50 ms gate is
/// mis-classifying. Pure so it is unit-testable without a transport.
///
/// Gated on `quic`: there is no QUIC transport (and so no diagnostic) in
/// the no-quic build; gating keeps that build warning-free.
#[cfg(feature = "quic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuicReadFailBucket {
    /// `elapsed_ms <= LATCHED_POOL_INSTANT_FAIL_MS` — instant-fail shape.
    Instant,
    /// `elapsed_ms > LATCHED_POOL_INSTANT_FAIL_MS` — slow / backpressure shape.
    Slow,
}

/// Split a failed QUIC read attempt's elapsed time on the same 50 ms
/// boundary the latch classifier uses. `<=` goes to `Instant` so the
/// bucket boundary matches `looks_like_latched_pool` (which classifies a
/// latch only when `elapsed_ms <= LATCHED_POOL_INSTANT_FAIL_MS`).
#[cfg(feature = "quic")]
fn quic_read_fail_bucket(elapsed_ms: u64) -> QuicReadFailBucket {
    if elapsed_ms <= LATCHED_POOL_INSTANT_FAIL_MS {
        QuicReadFailBucket::Instant
    } else {
        QuicReadFailBucket::Slow
    }
}

/// Transport-variant discriminant for the QUIC read-failure diagnostic's
/// leg-gating predicate. Mirrors the three `Transport` variants but is a
/// plain `Copy` enum so `quic_read_leg` is unit-testable without standing
/// up a real `Transport` (whose arms hold non-`Copy` channel handles).
#[cfg(feature = "quic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadTransportKind {
    /// `Transport::Tcp` — TCP-only; no QUIC leg ever.
    Tcp,
    /// `Transport::Quic` — QUIC-only; every read traverses QUIC.
    Quic,
    /// `Transport::Dual` — TCP + QUIC; the leg depends on `prefer_tcp`.
    Dual,
}

/// Resolve which leg a failed read attempt ACTUALLY traversed, returning
/// `Some(transport_kind_tag)` iff it went over QUIC, else `None`.
///
/// The call site picks the leg via `read_internal(_, prefer_tcp)`
/// (`grpc_store.rs:read_internal`): single-stream passes `prefer_tcp =
/// false`, parallel passes `prefer_tcp = true`. Under `Dual`, `prefer_tcp
/// = true` routes over the TCP `ConnectionManager` (`read_internal` dual
/// arm) — so a parallel-chunk failure under `Dual` is a TCP-leg failure
/// that MUST NOT be counted by this QUIC probe (it would pollute the
/// `instant`:`slow` ratio the latch-vs-backpressure discriminator reads).
/// So a read went over QUIC iff `kind == Quic` OR (`kind == Dual` AND
/// `!prefer_tcp`).
///
/// The returned tag is the PRECISE leg, never the ambiguous `"dual"` for a
/// counted event: `"quic"` for the QUIC-only transport, `"dual/quic"` for
/// the QUIC leg of the Dual transport (matching `read_internal`'s own
/// `transport = "dual/quic"` trace label). Pure so it is unit-testable.
#[cfg(feature = "quic")]
fn quic_read_leg(kind: ReadTransportKind, prefer_tcp: bool) -> Option<&'static str> {
    match kind {
        // No QUIC leg exists under the TCP-only transport.
        ReadTransportKind::Tcp => None,
        // QUIC-only: every read traverses QUIC regardless of `prefer_tcp`.
        ReadTransportKind::Quic => Some("quic"),
        // Dual: `prefer_tcp = false` (single-stream) routes over QUIC;
        // `prefer_tcp = true` (parallel) routes over TCP — not counted.
        ReadTransportKind::Dual if !prefer_tcp => Some("dual/quic"),
        ReadTransportKind::Dual => None,
    }
}

/// Rate-limit minimum interval between diagnostic `warn!` emits: ~1/sec.
/// The 2026-06-23 burst was 46k lines; this gate bounds a recurrence to
/// roughly one emit per second per process while a `suppressed_since_last`
/// count preserves the true volume in each emitted line.
#[cfg(feature = "quic")]
const QUIC_READ_FAIL_LOG_MIN_INTERVAL_NANOS: u64 = 1_000_000_000;

/// Pure rate-limit gate over a monotonic-nanos clock. Returns `true` iff
/// this caller should emit, else `false` (suppress). `now_nanos` is
/// `Instant`-derived (NTP-safe monotonic — a wall-clock backward step
/// cannot wedge the gate).
///
/// The first call after process start sees `last_emit_nanos == 0` (the
/// `AtomicU64` initial value) and ALWAYS emits — even when `now_nanos` is
/// itself `< min_interval` (a QUIC read failure in the first second after
/// `QUIC_DIAG_EPOCH` init), so the very first diagnostic event of an
/// incident is never silently dropped. A real emit publishes a non-zero
/// `now_nanos` (the gate never publishes 0 after a true emit because the
/// epoch's first observable elapsed value is non-zero in practice; even if
/// it were 0, that only costs one extra emit, never a lost suppression).
///
/// Extracted as a pure function so the gate logic is unit-testable without
/// standing up a transport or racing a real clock: two events
/// `< min_interval` apart yield exactly one emit + one suppression.
#[cfg(feature = "quic")]
fn quic_read_fail_should_emit(
    now_nanos: u64,
    last_emit_nanos: u64,
    min_interval_nanos: u64,
) -> bool {
    if last_emit_nanos == 0 {
        return true; // never emitted — always allow the first line.
    }
    // `saturating_sub`: `now_nanos` is monotonic-derived so it should be
    // >= last_emit, but a concurrent updater could publish a slightly
    // later value between our load and this compare; saturating to 0 then
    // suppresses (0 < min_interval), which is the safe direction.
    now_nanos.saturating_sub(last_emit_nanos) >= min_interval_nanos
}

/// Process-start monotonic reference. `Instant` is not a `u64`, so the
/// rate-limit gate stores nanoseconds elapsed since this epoch in an
/// `AtomicU64`. Captured once at first use; monotonic and NTP-immune.
#[cfg(feature = "quic")]
static QUIC_DIAG_EPOCH: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

/// Monotonic nanoseconds since `QUIC_DIAG_EPOCH`, saturating at `u64::MAX`
/// (≈584 years — unreachable). Sole clock source for the rate-limit gate.
#[cfg(feature = "quic")]
fn quic_diag_now_nanos() -> u64 {
    u64::try_from(QUIC_DIAG_EPOCH.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Pure-function classifier for `get_part_parallel`'s per-chunk attempt
/// loop. Encapsulated so it can be unit-tested without standing up a
/// real gRPC bytestream server. Inputs:
///
/// * `clean_eof` — `true` iff the per-attempt `loop` exited because
///   `stream.next()` returned `None`. tonic's `Streaming::poll_next`
///   only yields `None` after the underlying response body finished
///   without a non-OK gRPC status, so `None` ⇔ `Status::OK` trailer.
///   `Some(Err(status))` (any non-OK trailer) is handled inside the
///   loop and never reaches this function.
/// * `bytes_received` — total bytes the per-chunk fetcher emitted
///   across all attempts so far, including this one.
/// * `chunk_length` — the requested range size for this chunk.
fn classify_chunk_attempt(
    clean_eof: bool,
    bytes_received: u64,
    chunk_length: u64,
) -> ChunkAttemptOutcome {
    if bytes_received == chunk_length {
        return ChunkAttemptOutcome::Complete;
    }
    if clean_eof {
        // Status::OK + bytes_received < chunk_length = resource exhausted.
        // The blob is shorter than the caller's requested range. Treat as
        // success (Bug B fix: the previous code returned DataLoss here).
        return ChunkAttemptOutcome::CleanShort;
    }
    ChunkAttemptOutcome::AmbiguousEarlyBreak
}

#[derive(Debug, MetricsComponent)]
pub struct GrpcStore {
    #[metric(help = "Instance name for the store")]
    instance_name: String,
    store_type: nativelink_config::stores::StoreType,
    retrier: Retrier,
    transport: Transport,
    /// Per-RPC timeout. `Duration::ZERO` means disabled.
    rpc_timeout: Duration,
    /// Blobs at or below this size use BatchUpdateBlobs instead of
    /// ByteStream.Write. 0 means disabled.
    batch_update_threshold: u64,
    /// Sender for batching entries. None when batching is disabled
    /// (threshold == 0).
    batch_tx: Option<tokio::sync::mpsc::UnboundedSender<PendingBatchEntry>>,
    /// Minimum blob size to trigger parallel chunked ByteStream reads.
    /// 0 means disabled.
    parallel_chunk_read_threshold: u64,
    /// Number of parallel Read RPCs for chunked reads.
    parallel_chunk_count: u64,
    /// Enable zstd compression at the tonic transport level.
    zstd_compression: bool,
    /// Cap on `cm.connection()` for write-side RPCs. None = wait
    /// indefinitely (current behavior); used by WorkerProxyStore at 3s
    /// to fast-fail mirror writes to dead workers.
    connection_acquire_timeout_ms: Option<u64>,
    /// Per-chunk retry outcome counters for `get_part_parallel`.
    /// `succeeded` means a chunk that needed at least one retry
    /// eventually returned its bytes; `failed` means the retrier
    /// exhausted attempts (or hit a non-retryable error after at
    /// least one retry). First-try successes do not increment either
    /// counter. Operators should watch these for sustained growth —
    /// rising rates indicate the per-chunk retry is band-aiding a
    /// real upstream problem (server flapping, h2 RST_STREAM bursts,
    /// etc.) rather than recovering rare transient glitches.
    #[metric(help = "Per-chunk retries in get_part_parallel that eventually succeeded")]
    parallel_chunk_retries_succeeded: AtomicU64,
    #[metric(help = "Per-chunk retries in get_part_parallel that exhausted retries and failed")]
    parallel_chunk_retries_failed: AtomicU64,
    /// #FU QUIC read-failure diagnostic (observability-only). Split on
    /// `LATCHED_POOL_INSTANT_FAIL_MS` (50 ms): a read RPC failure that
    /// ACTUALLY traversed the QUIC leg increments exactly one of these.
    /// Only genuine-QUIC read failures are counted — a read went over QUIC
    /// iff `transport == Quic` OR (`transport == Dual` AND `!prefer_tcp`)
    /// (see `quic_read_leg`); under `Dual` the parallel chunk path routes
    /// over TCP (`prefer_tcp = true`), and those TCP-leg failures are NOT
    /// counted here (counting them would pollute the ratio with TCP
    /// failures). The RATIO is the discriminator — a high `instant`:`slow`
    /// ratio confirms a genuine instant latch (Finding A); a `slow`-heavy
    /// skew confirms live backpressure the ≤50 ms latch gate is
    /// mis-classifying (Finding B). Cheap atomics.
    /// CAPPED AT 1: single monotonically-increasing u64 each; no bounding needed.
    /// Gated on `quic`: no QUIC transport exists in the no-quic build.
    #[cfg(feature = "quic")]
    #[metric(
        help = "Genuine-QUIC-leg read RPC failures with elapsed <= 50ms (instant-fail / candidate latch)"
    )]
    quic_read_instant_fail: AtomicU64,
    /// See `quic_read_instant_fail` — genuine-QUIC-leg read failures only
    /// (TCP-leg failures are not counted). CAPPED AT 1: single u64; no bounding needed.
    #[cfg(feature = "quic")]
    #[metric(
        help = "Genuine-QUIC-leg read RPC failures with elapsed > 50ms (slow / backpressure-shaped)"
    )]
    quic_read_slow_fail: AtomicU64,
    /// #FU rate-limit gate for the diagnostic `warn!`: monotonic nanos
    /// (since `QUIC_DIAG_EPOCH`) of the last emit. NOT a metric — internal
    /// gate state. Zero = never emitted. CAPPED AT 1: single u64.
    #[cfg(feature = "quic")]
    quic_read_fail_log_last_emit_nanos: AtomicU64,
    /// #FU count of diagnostic events suppressed by the rate-limit gate
    /// since the last emitted line; reset to 0 on each emit and carried in
    /// that line as `suppressed_since_last`. CAPPED AT 1: single u64.
    #[cfg(feature = "quic")]
    quic_read_fail_log_suppressed: AtomicU64,
    /// #212 Phase 2.4 runtime kill-switch for the worker→server
    /// chunked-write path. Default OFF — even with the
    /// `chunked_fast_slow` feature compiled in, blobs continue to take
    /// the legacy in-order ByteStream Write path until an operator
    /// flips this with `enable_chunked_writes()`. The "compile + ship,
    /// flip later" pattern matches `enable_batch_small_blob_reads` /
    /// `enable_locality_in_has` on `WorkerProxyStore`.
    #[cfg(feature = "chunked_fast_slow")]
    chunked_writes_enabled: AtomicBool,
    /// #550 Phase 3: selects V1 (unary) vs V2 (bidi-per-chunk-ack)
    /// dispatcher inside `update_via_chunked_inner`. Default OFF —
    /// preserves V1 behavior. Flip ON for opt-in rollout of the V2
    /// wire shape.
    #[cfg(feature = "chunked_fast_slow")]
    chunked_v2_writes_enabled: AtomicBool,
    /// #212 Phase 2.4 metrics for the chunked-write path. Counters
    /// are populated via `chunked_client::write_chunked_stream` and
    /// readable through the `chunked_metrics()` accessor. Folding into
    /// the `MetricsComponent` derive (i.e. adding `#[derive(MetricsComponent)]`
    /// to `ChunkedClientMetrics` plus `#[metric(group = "chunked_writes")]`
    /// on this field) is deferred to the follow-up commit that flips
    /// the kill-switch on in production; until then accessors are
    /// sufficient for tests and ad-hoc inspection.
    #[cfg(feature = "chunked_fast_slow")]
    chunked_metrics: Arc<crate::chunked::chunked_client::ChunkedClientMetrics>,
}

/// #548 Phase 1 (Seam 3): build the `ChunkedClientOptions` value that
/// `GrpcStore::update_via_chunked_inner` hands to the worker-side
/// chunked client. Extracted so the inline test can mutation-verify the
/// `source = Worker` hardcode without spinning up a tonic transport.
/// All callers of this function are inside `GrpcStore`; production has
/// exactly one (`update_via_chunked_inner`).
#[cfg(feature = "chunked_fast_slow")]
#[must_use]
fn grpc_store_chunked_client_options() -> crate::chunked::chunked_client::ChunkedClientOptions {
    use crate::chunked::CHUNK_SIZE;
    use crate::chunked::ChunkedWriteSource;
    use crate::chunked::chunked_client::{ChunkedClientOptions, DEFAULT_MAX_ATTEMPTS};

    // #548 Phase 1: source = Worker. `GrpcStore::update_via_chunked_inner`
    // is invoked from `GrpcStore::update`, the path a remote worker takes
    // to upload bytes to the server via the chunked RPC. The server-side
    // V1 InFlightEntry independently sets `source = Worker` at admission
    // (`chunked_write_handler.rs:1219`); the two values are derived from
    // WHICH RPC LANDED, not from any wire field (see SECURITY-NOTE on
    // `ChunkedWriteSource`). `ChunkedClientOptions` has no `Default`;
    // every field is explicit at every construction site.
    ChunkedClientOptions {
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        chunk_size: CHUNK_SIZE,
        source: ChunkedWriteSource::Worker,
    }
}

impl GrpcStore {
    pub async fn new(spec: &GrpcSpec) -> Result<Arc<Self>, Error> {
        Self::new_with_jitter(spec, spec.retry.make_jitter_fn()).await
    }

    pub async fn new_with_jitter(
        spec: &GrpcSpec,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
    ) -> Result<Arc<Self>, Error> {
        error_if!(
            spec.endpoints.is_empty(),
            "Expected at least 1 endpoint in GrpcStore"
        );

        let rpc_timeout = Duration::from_secs(spec.rpc_timeout_s);

        // Choose transport based on the first endpoint's use_http3 flag.
        #[cfg(feature = "quic")]
        let use_quic = spec.endpoints.first().is_some_and(|ep| ep.use_http3);
        #[cfg(not(feature = "quic"))]
        let use_quic = false;

        let transport = if use_quic {
            #[cfg(feature = "quic")]
            {
                let ep = &spec.endpoints[0];
                let connections = spec.connections_per_endpoint.max(1);

                if spec.dual_transport {
                    // Dual transport: create both TCP and QUIC connections.
                    let quic_channel = tls_utils::h3_channel(ep, connections)
                        .map_err(|e| make_input_err!("Failed to create QUIC channel: {e:?}"))?;

                    let mut tcp_endpoints = Vec::with_capacity(spec.endpoints.len());
                    for endpoint_config in &spec.endpoints {
                        // Skip QUIC-only endpoints — the TCP ConnectionManager
                        // can't connect to UDP-only ports.
                        if endpoint_config.use_http3 {
                            continue;
                        }
                        let endpoint = tls_utils::endpoint(endpoint_config).map_err(|e| {
                            make_input_err!("Invalid URI for GrpcStore endpoint (dual/tcp): {e:?}")
                        })?;
                        tcp_endpoints.push(endpoint);
                    }
                    let tcp_cm = ConnectionManager::new(
                        tcp_endpoints.into_iter(),
                        spec.connections_per_endpoint,
                        spec.max_concurrent_requests,
                        spec.retry.clone(),
                        jitter_fn.clone(),
                    );

                    info!(
                        address = %ep.address,
                        connections,
                        "GrpcStore: using dual transport (TCP for parallel reads/large writes, QUIC for batched/small RPCs)",
                    );
                    Transport::Dual {
                        tcp: tcp_cm,
                        quic: quic_channel,
                    }
                } else {
                    let channel = tls_utils::h3_channel(ep, connections)
                        .map_err(|e| make_input_err!("Failed to create QUIC channel: {e:?}"))?;
                    info!(
                        address = %ep.address,
                        connections,
                        "GrpcStore: using QUIC/HTTP3 transport",
                    );
                    Transport::Quic(channel)
                }
            }
            #[cfg(not(feature = "quic"))]
            {
                return Err(make_input_err!(
                    "use_http3 is set but the 'quic' feature is not enabled"
                ));
            }
        } else {
            let mut endpoints = Vec::with_capacity(spec.endpoints.len());
            for endpoint_config in &spec.endpoints {
                let endpoint = tls_utils::endpoint(endpoint_config)
                    .map_err(|e| make_input_err!("Invalid URI for GrpcStore endpoint : {e:?}"))?;
                endpoints.push(endpoint);
            }
            Transport::Tcp(ConnectionManager::new(
                endpoints.into_iter(),
                spec.connections_per_endpoint,
                spec.max_concurrent_requests,
                spec.retry.clone(),
                jitter_fn.clone(),
            ))
        };

        let batch_update_threshold = spec.batch_update_threshold_bytes;

        let (batch_tx, batch_rx) = if batch_update_threshold > 0 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let store = Arc::new(Self {
            instance_name: spec.instance_name.clone(),
            store_type: spec.store_type,
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn.clone(),
                spec.retry.clone(),
            ),
            transport,
            rpc_timeout,
            batch_update_threshold,
            batch_tx,
            parallel_chunk_read_threshold: spec.parallel_chunk_read_threshold,
            parallel_chunk_count: spec.parallel_chunk_count.max(1),
            zstd_compression: spec.zstd_compression,
            connection_acquire_timeout_ms: spec.connection_acquire_timeout_ms,
            parallel_chunk_retries_succeeded: AtomicU64::new(0),
            parallel_chunk_retries_failed: AtomicU64::new(0),
            #[cfg(feature = "quic")]
            quic_read_instant_fail: AtomicU64::new(0),
            #[cfg(feature = "quic")]
            quic_read_slow_fail: AtomicU64::new(0),
            #[cfg(feature = "quic")]
            quic_read_fail_log_last_emit_nanos: AtomicU64::new(0),
            #[cfg(feature = "quic")]
            quic_read_fail_log_suppressed: AtomicU64::new(0),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_writes_enabled: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_v2_writes_enabled: AtomicBool::new(false),
            #[cfg(feature = "chunked_fast_slow")]
            chunked_metrics: crate::chunked::chunked_client::ChunkedClientMetrics::new(),
        });

        if let Some(rx) = batch_rx {
            let weak = Arc::downgrade(&store);
            let max_concurrent = spec.max_concurrent_batch_rpcs.max(1) as usize;
            let semaphore = Arc::new(Semaphore::new(max_concurrent));
            tokio::spawn(Self::batch_flush_loop(weak, rx, semaphore));
            info!(
                batch_update_threshold,
                max_concurrent, "GrpcStore: BatchUpdateBlobs opportunistic batching enabled",
            );
        }

        // #212 Phase 2.4: honor the production config knob. The
        // `enable_chunked_writes()` runtime API still exists for tests
        // and operator admin tooling, but production opts in through
        // the JSON config field landed in the same series.
        #[cfg(feature = "chunked_fast_slow")]
        if spec.chunked_writes_enabled {
            store.enable_chunked_writes();
        }

        // #550 Phase 3: honor the V2 dispatcher config knob. Same
        // compile-ship/flip-later pattern as V1.
        #[cfg(feature = "chunked_fast_slow")]
        if spec.chunked_v2_writes_enabled {
            store.enable_chunked_v2_writes();
        }

        Ok(store)
    }

    /// #212 Phase 2.4 runtime kill-switch: enable the worker→server
    /// `WorkerApi/WriteChunked` path for blobs ≥ `CHUNK_SIZE` (1 MiB).
    /// Default OFF; flipping this ON routes large `update()` calls
    /// to `chunked::chunked_client::write_chunked_stream`. Blobs
    /// below `CHUNK_SIZE` continue to take the legacy in-order
    /// ByteStream Write path regardless.
    ///
    /// Idempotent. Safe to call multiple times. Takes effect on the
    /// NEXT `update()` invocation; in-flight calls are not affected.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn enable_chunked_writes(&self) {
        self.chunked_writes_enabled.store(true, Ordering::Relaxed);
        tracing::info!(
            instance_name = %self.instance_name,
            "GrpcStore: chunked writes enabled (worker→server WriteChunked path active for blobs >= CHUNK_SIZE)",
        );
    }

    /// Operator kill-switch: disable the chunked-write path. Blobs
    /// fall back to the legacy in-order ByteStream Write transport
    /// for the next `update()` and beyond.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn disable_chunked_writes(&self) {
        self.chunked_writes_enabled.store(false, Ordering::Relaxed);
        tracing::info!(
            instance_name = %self.instance_name,
            "GrpcStore: chunked writes disabled (legacy ByteStream Write path)",
        );
    }

    /// Inspector for the chunked-write kill-switch. `pub` so tests
    /// can assert state and operators can read the flag through any
    /// admin tool that gets a `&GrpcStore` handle.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_writes_enabled(&self) -> bool {
        self.chunked_writes_enabled.load(Ordering::Relaxed)
    }

    /// #550 Phase 3 runtime kill-switch: enable the V2 dispatcher
    /// (`WorkerApiWriteChunkedV2Dispatcher`) inside
    /// `update_via_chunked_inner()`. Default OFF — preserves V1
    /// behavior. Flip ON for opt-in rollout.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn enable_chunked_v2_writes(&self) {
        self.chunked_v2_writes_enabled.store(true, Ordering::Relaxed);
        tracing::info!(
            instance_name = %self.instance_name,
            "GrpcStore: chunked V2 writes enabled (WorkerApi/WriteChunkedV2 path active for blobs >= CHUNK_SIZE)",
        );
    }

    /// Operator kill-switch: disable the V2 dispatcher. Falls back to
    /// the V1 (`WorkerApiWriteChunkedDispatcher`) path.
    #[cfg(feature = "chunked_fast_slow")]
    pub fn disable_chunked_v2_writes(&self) {
        self.chunked_v2_writes_enabled.store(false, Ordering::Relaxed);
        tracing::info!(
            instance_name = %self.instance_name,
            "GrpcStore: chunked V2 writes disabled (V1 path)",
        );
    }

    /// Inspector for the V2 dispatcher kill-switch.
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_v2_writes_enabled(&self) -> bool {
        self.chunked_v2_writes_enabled.load(Ordering::Relaxed)
    }

    /// Read-only accessor for the chunked-write metrics. Used by
    /// tests + future Prometheus / metric-publish wiring (the field
    /// is not yet folded into the `MetricsComponent` derive because
    /// the path is dormant under the default kill-switch).
    #[cfg(feature = "chunked_fast_slow")]
    #[must_use]
    pub fn chunked_metrics(&self) -> &Arc<crate::chunked::chunked_client::ChunkedClientMetrics> {
        &self.chunked_metrics
    }

    /// Acquire a TCP channel for a write-side RPC. When
    /// `connection_acquire_timeout_ms` is set, fast-fails with
    /// `Code::Unavailable` if the connection_manager doesn't deliver a
    /// channel within that window — used by WorkerProxyStore to prevent
    /// mirror writes to a dead worker from queueing against the 256-slot
    /// connection backlog while reconnect attempts run on 1s backoff.
    /// Reads are intentionally NOT wrapped: cluster-start latency on the
    /// critical path should wait, not error.
    ///
    /// The "ConnectionRefused" prefix produced on timeout (see
    /// `ConnectionManager::connection_with_timeout`) is matched by
    /// `worker_proxy_store::is_definitive_unreachable` to fast-quarantine
    /// the dead worker.
    async fn acquire_write_channel(
        &self,
        cm: &ConnectionManager,
        ctx: &'static str,
    ) -> Result<nativelink_util::connection_manager::Connection, Error> {
        match self.connection_acquire_timeout_ms {
            Some(ms) => {
                cm.connection_with_timeout(ctx.into(), Duration::from_millis(ms))
                    .await
            }
            None => cm.connection(ctx.into()).await,
        }
    }

    /// Best-effort: if `err` is transport-shaped (#147), evict one
    /// idle TCP channel and queue a reconnect. No-op for QUIC.
    /// Public so external streaming callers (e.g.
    /// `running_actions_manager::resolve_directory_tree` consuming a
    /// `Streaming<GetTreeResponse>`) can apply the same recovery.
    pub fn evict_pool_on_transport_err(&self, err: &Error) {
        // #147 trace: log every call, including no-op cases. Hypothesis
        // (a) "evict is never called" is verified by counting these
        // log lines per failed get_part. Hypothesis (c)/(d) "the
        // predicate doesn't match the production error shape" is
        // verified by comparing `predicate_matched` against the error
        // code+message head we log alongside the failure site.
        let predicate_matched = looks_like_dead_channel(err);
        // Truncate the first message to the first 80 chars to avoid
        // log spam from long upstream-stack messages while preserving
        // enough text to disambiguate "Tried to send while stream is
        // closed" from "broken pipe" from "h2 protocol error" etc.
        let msg_head: &str = err
            .messages
            .first()
            .map(|m| m.as_str())
            .unwrap_or("<no_message>");
        let msg_head_short: String = msg_head.chars().take(80).collect();
        let transport_kind: &str = match &self.transport {
            Transport::Tcp(_) => "tcp",
            #[cfg(feature = "quic")]
            Transport::Quic(_) => "quic",
            #[cfg(feature = "quic")]
            Transport::Dual { .. } => "dual",
        };
        info!(
            code = ?err.code,
            %msg_head_short,
            predicate_matched,
            transport_kind,
            "GrpcStore::evict_pool_on_transport_err entry (#147 trace)",
        );
        if !predicate_matched {
            return;
        }
        let reason = format!("transport-shaped err in retry: code={:?}", err.code);
        match &self.transport {
            Transport::Tcp(cm) => cm.evict_idle_channel(None, reason),
            #[cfg(feature = "quic")]
            Transport::Quic(_) => {}
            #[cfg(feature = "quic")]
            Transport::Dual { tcp, .. } => tcp.evict_idle_channel(None, reason),
        }
    }

    /// #FU QUIC read-failure diagnostic (observability-only — NO behavior
    /// change). Records one failed read RPC for the latch-vs-backpressure
    /// discriminator. ONLY failures that ACTUALLY traversed the QUIC leg are
    /// in scope (TCP read failures are a different signature); every other
    /// leg is a no-op early-return.
    ///
    /// The caller passes the `prefer_tcp` it handed to `read_internal` so
    /// this method can resolve the leg that was actually used: single-stream
    /// passes `prefer_tcp = false` (→ QUIC under Dual, QUIC under Quic-only);
    /// parallel passes `prefer_tcp = true` (→ TCP under Dual, but QUIC under
    /// the Quic-only transport). A read went over QUIC iff `transport ==
    /// Quic` OR (`transport == Dual` AND `!prefer_tcp`) — see `quic_read_leg`.
    /// Crucially, under `Dual` the parallel path routes over TCP, so those
    /// failures are NOT counted here (counting them would pollute the
    /// `instant`:`slow` ratio with TCP failures). For a genuine QUIC-leg
    /// failure:
    ///
    /// 1. Increment exactly one split counter on the same
    ///    `LATCHED_POOL_INSTANT_FAIL_MS` (50 ms) boundary the latch
    ///    classifier uses — the `instant`:`slow` ratio IS the discriminator.
    /// 2. Emit a rate-limited (`~1/s`, monotonic-nanos gate) `warn!`
    ///    carrying `elapsed_ms`, the PRECISE `transport_kind` (`"quic"` for
    ///    the Quic-only transport, `"dual/quic"` for the Dual single-stream
    ///    leg — never the ambiguous `"dual"` for a counted event),
    ///    `latch_classified` (the `looks_like_latched_pool` result the live
    ///    retry path already computed), `code`, an 80-char `msg_head`, and
    ///    `suppressed_since_last` so a recurrence cannot reproduce the
    ///    46k-line 2026-06-23 burst yet the true volume is preserved.
    ///
    /// `warn!` (not `debug!`/`trace!`) because the prod binary compiles
    /// out sub-`info` levels under `release_max_level_info`. This method
    /// performs no retry/eviction/abort/routing side effect; it is purely
    /// additive observation called AFTER the live classifier at each site.
    ///
    /// Without the `quic` feature there is no QUIC transport, so the whole
    /// method is a no-op (keeps the call sites in `get_part_*` unconditional
    /// and warning-free in the no-quic build).
    #[cfg(not(feature = "quic"))]
    fn record_quic_read_fail(&self, _err: &Error, _elapsed_ms: u64, _prefer_tcp: bool) {}

    #[cfg(feature = "quic")]
    fn record_quic_read_fail(&self, err: &Error, elapsed_ms: u64, prefer_tcp: bool) {
        let kind = match &self.transport {
            Transport::Tcp(_) => ReadTransportKind::Tcp,
            Transport::Quic(_) => ReadTransportKind::Quic,
            Transport::Dual { .. } => ReadTransportKind::Dual,
        };
        // Count + log ONLY if this read actually went over the QUIC leg.
        // TCP-leg failures (TCP transport, or Dual + prefer_tcp) are simply
        // not observed by this probe. The tag is the precise leg.
        let Some(transport_kind) = quic_read_leg(kind, prefer_tcp) else {
            return;
        };

        match quic_read_fail_bucket(elapsed_ms) {
            QuicReadFailBucket::Instant => {
                self.quic_read_instant_fail.fetch_add(1, Ordering::Relaxed);
            }
            QuicReadFailBucket::Slow => {
                self.quic_read_slow_fail.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Rate-limit the warn! (monotonic-nanos gate). Suppressed events
        // still count so the emitted line carries true volume.
        let now_nanos = quic_diag_now_nanos();
        let last_emit = self.quic_read_fail_log_last_emit_nanos.load(Ordering::Relaxed);
        if !quic_read_fail_should_emit(now_nanos, last_emit, QUIC_READ_FAIL_LOG_MIN_INTERVAL_NANOS) {
            self.quic_read_fail_log_suppressed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Single-flight the emit: only the winner of the CAS publishes its
        // timestamp and prints; concurrent callers fall back to suppressed.
        if self
            .quic_read_fail_log_last_emit_nanos
            .compare_exchange(last_emit, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            self.quic_read_fail_log_suppressed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let suppressed_since_last = self.quic_read_fail_log_suppressed.swap(0, Ordering::Relaxed);

        // Truncate the first message to 80 chars (same pattern as
        // `evict_pool_on_transport_err`) to bound line width.
        let msg_head: &str = err
            .messages
            .first()
            .map(String::as_str)
            .unwrap_or("<no_message>");
        let msg_head_short: String = msg_head.chars().take(80).collect();
        let latch_classified = looks_like_latched_pool(err, elapsed_ms);
        warn!(
            elapsed_ms,
            transport_kind,
            latch_classified,
            code = ?err.code,
            %msg_head_short,
            suppressed_since_last,
            instant_fail_total = self.quic_read_instant_fail.load(Ordering::Relaxed),
            slow_fail_total = self.quic_read_slow_fail.load(Ordering::Relaxed),
            "GrpcStore QUIC read-failure diagnostic (#FU latch-vs-backpressure): \
             tight elapsed cluster <=5ms confirms genuine instant latch; \
             spread near the timeout bound confirms live backpressure the \
             50ms latch gate is mis-classifying",
        );
    }

    /// Creates a CAS client with zstd compression configured if enabled.
    fn cas_client<T>(&self, channel: T) -> ContentAddressableStorageClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<tonic::codegen::StdError>,
        T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    {
        let mut client = ContentAddressableStorageClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_DECODING_SIZE);
        if self.zstd_compression {
            client = client
                .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        }
        client
    }

    /// Creates a ByteStream client with zstd compression configured if enabled.
    fn bs_client<T>(&self, channel: T) -> ByteStreamClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<tonic::codegen::StdError>,
        T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    {
        let mut client =
            ByteStreamClient::new(channel).max_decoding_message_size(MAX_GRPC_DECODING_SIZE);
        if self.zstd_compression {
            client = client
                .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        }
        client
    }

    /// Creates an ActionCache client with zstd compression configured if enabled.
    fn ac_client<T>(&self, channel: T) -> ActionCacheClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<tonic::codegen::StdError>,
        T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    {
        let mut client =
            ActionCacheClient::new(channel).max_decoding_message_size(MAX_GRPC_DECODING_SIZE);
        if self.zstd_compression {
            client = client
                .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        }
        client
    }

    /// Maximum total payload size for a single BatchUpdateBlobs RPC.
    /// The RE API spec recommends servers support at least 4 MiB.
    const MAX_BATCH_TOTAL_SIZE: usize = 4 * 1024 * 1024;

    /// Send one or more blobs via a single BatchUpdateBlobs RPC.
    /// Returns per-entry results keyed by digest. The RE API does not
    /// guarantee response ordering, so we match by digest, not index.
    async fn do_batch_update(
        &self,
        digests: &[DigestInfo],
        entries: Vec<(DigestInfo, Bytes)>,
    ) -> HashMap<DigestInfo, Result<(), Error>> {
        let digest_function = Context::current()
            .get::<DigestHasherFunc>()
            .map_or_else(default_digest_hasher_func, |v| *v)
            .proto_digest_func()
            .into();

        // Deduplicate entries by digest — multiple callers may submit the
        // same blob in the same batch (e.g., identical stdout/stderr).
        let deduped: HashMap<DigestInfo, Bytes> = entries.into_iter().collect();
        let requests: Vec<_> = deduped
            .into_iter()
            .map(|(digest, data)| batch_update_blobs_request::Request {
                digest: Some(digest.into()),
                data,
                compressor: compressor::Value::Identity.into(),
            })
            .collect();

        let response = match self
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: String::new(), // Overwritten by batch_update_blobs()
                requests,
                digest_function,
            }))
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let err = e.append("In GrpcStore::do_batch_update");
                return digests.iter().map(|d| (*d, Err(err.clone()))).collect();
            }
        };

        // Build result map keyed by digest (RE API does not guarantee ordering).
        let mut results: HashMap<DigestInfo, Result<(), Error>> = response
            .into_inner()
            .responses
            .into_iter()
            .filter_map(|resp| {
                let digest = DigestInfo::try_from(resp.digest?).ok()?;
                let result = match &resp.status {
                    Some(status) if status.code != 0 => Err(make_input_err!(
                        "BatchUpdateBlobs failed: code={}, message={}",
                        status.code,
                        status.message
                    )),
                    _ => Ok(()),
                };
                Some((digest, result))
            })
            .collect();

        // Fill in missing responses as errors.
        for d in digests {
            results.entry(*d).or_insert_with(|| {
                Err(make_input_err!("BatchUpdateBlobs: no response for digest"))
            });
        }
        results
    }

    /// Background task that batches small blob uploads and flushes them
    /// as BatchUpdateBlobs RPCs. Uses opportunistic batching: wait for
    /// the first item, yield to let other ready tasks enqueue, then
    /// drain everything currently queued and fire immediately. Under
    /// low load each blob gets its own immediate batch. Under high load
    /// items naturally accumulate while RPCs are in flight, so the next
    /// drain picks up everything queued.
    ///
    /// Multiple batches can be in flight concurrently (up to `semaphore`
    /// permits), so the loop does not block on an RPC before collecting
    /// the next batch.
    async fn batch_flush_loop(
        weak: Weak<GrpcStore>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingBatchEntry>,
        semaphore: Arc<Semaphore>,
    ) {
        // An entry that didn't fit in the previous batch, carried forward.
        let mut held_entry: Option<PendingBatchEntry> = None;

        loop {
            // Use held entry from previous iteration, or wait for a new one.
            let first = if let Some(entry) = held_entry.take() {
                entry
            } else {
                match rx.recv().await {
                    Some(entry) => entry,
                    None => return, // Channel closed
                }
            };

            let mut batch = vec![first];
            let mut total_size = batch[0].data.len();

            // Yield once to let other ready tasks enqueue items.
            // No artificial delay — just gives concurrent callers a
            // chance to push to the channel before we drain it.
            tokio::task::yield_now().await;

            // Drain everything currently queued (non-blocking).
            loop {
                match rx.try_recv() {
                    Ok(entry) => {
                        let new_total = total_size + entry.data.len();
                        if new_total > Self::MAX_BATCH_TOTAL_SIZE && !batch.is_empty() {
                            // Would exceed limit — hold for next batch.
                            held_entry = Some(entry);
                            break;
                        }
                        total_size = new_total;
                        batch.push(entry);
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            let store = match weak.upgrade() {
                Some(s) => s,
                None => return, // GrpcStore dropped
            };

            // Acquire a permit before spawning the RPC task. This
            // limits the number of concurrent in-flight batch RPCs.
            // We acquire here (not inside the spawned task) so that
            // backpressure is applied to the collection loop: when all
            // permits are held, the loop blocks until one completes.
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return, // Semaphore closed — should not happen
            };

            let num = batch.len();
            trace!(count = num, total_size, "GrpcStore: flushing batch",);

            // Spawn the RPC and result distribution as a separate task
            // so the loop can immediately collect the next batch.
            tokio::spawn(async move {
                let digests: Vec<_> = batch.iter().map(|e| e.digest).collect();
                let (senders_with_digests, entries): (Vec<_>, Vec<_>) = batch
                    .into_iter()
                    .map(|e| ((e.digest, e.result_tx), (e.digest, e.data)))
                    .unzip();

                let results = store.do_batch_update(&digests, entries).await;

                for (digest, sender) in senders_with_digests {
                    // Use .get().cloned() instead of .remove() because multiple
                    // senders may reference the same digest (e.g., stdout and stderr
                    // with identical content in the same batch).
                    let result = results.get(&digest).cloned().unwrap_or_else(|| {
                        Err(make_input_err!(
                            "BatchUpdateBlobs: missing result for {digest:?}"
                        ))
                    });
                    drop(sender.send(result));
                }

                // Drop the permit after the RPC completes, freeing a
                // slot for the next batch.
                drop(permit);
            });
        }
    }

    async fn perform_request<F, Fut, R, I>(&self, input: I, mut request: F) -> Result<R, Error>
    where
        F: FnMut(I) -> Fut + Send + Copy,
        Fut: Future<Output = Result<R, Error>> + Send,
        R: Send,
        I: Send + Clone,
    {
        self.retrier
            .retry(unfold(input, move |input| async move {
                let input_clone = input.clone();
                Some((
                    request(input_clone)
                        .await
                        .map_or_else(RetryResult::Retry, RetryResult::Ok),
                    input,
                ))
            }))
            .await
    }

    pub async fn find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Error> {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let mut request = grpc_request.into_inner();

        // Some builds (Chromium for example) do lots of empty requests for some reason, so shortcut them
        if request.blob_digests.is_empty() {
            return Ok(Response::new(FindMissingBlobsResponse {
                missing_blob_digests: vec![],
            }));
        }

        request.instance_name.clone_from(&self.instance_name);
        self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("find_missing_blobs".into())
                        .await
                        .err_tip(|| "in find_missing_blobs")?;
                    self.cas_client(channel)
                        .find_missing_blobs(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::find_missing_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .cas_client(ch.clone())
                    .find_missing_blobs(Request::new(request))
                    .await
                    .err_tip(|| "in GrpcStore::find_missing_blobs (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Small/batched RPC: prefer QUIC (1.1x faster)
                    self.cas_client(quic.clone())
                        .find_missing_blobs(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::find_missing_blobs (dual/quic)")
                }
            }
        })
        .await
    }

    pub async fn batch_update_blobs(
        &self,
        grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Error> {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let mut request = grpc_request.into_inner();
        request.instance_name.clone_from(&self.instance_name);
        let is_mirror = IS_MIRROR_REQUEST.try_with(|v| *v).unwrap_or(false);
        // #168 code-reviewer S1: propagate `IS_WORKER_REQUEST` task-local
        // into the `x-nativelink-worker` request metadata. Without this
        // header, worker uploads to the server arrive with `is_worker=false`
        // and the server's CAS dispatcher fans the bytes back to all
        // workers (including the originator), wasting RTTs and risking
        // a benign-but-noisy redundant push (loop-prevention contract
        // covers correctness; this fix avoids redundant work).
        let is_worker = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
        self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_mirror {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-mirror",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            if is_worker {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-worker",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("batch_update_blobs".into())
                        .await
                        .err_tip(|| "in batch_update_blobs")?;
                    self.cas_client(channel)
                        .batch_update_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_update_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .cas_client(ch.clone())
                    .batch_update_blobs(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::batch_update_blobs (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Batched RPC: prefer QUIC (9x faster)
                    self.cas_client(quic.clone())
                        .batch_update_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_update_blobs (dual/quic)")
                }
            }
        })
        .await
    }

    pub async fn batch_read_blobs(
        &self,
        grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Error> {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let mut request = grpc_request.into_inner();
        request.instance_name.clone_from(&self.instance_name);
        let is_worker = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
        // #479: batch_read_blobs is a unary RPC, so per-chunk
        // observability collapses to wall-clock RPC time — capture it
        // and emit one INFO per call so SREs can grep
        // `GrpcStore::batch_read_blobs` to see every batch's latency
        // without a debug filter. The slow-call warn fires when the
        // RPC takes >5 s.
        let digest_count = request.digests.len();
        let call_start = std::time::Instant::now();
        let instance_name_for_log = self.instance_name.clone();
        let result = self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_worker {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-worker",
                    tonic::metadata::MetadataValue::from_static("true"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("batch_read_blobs".into())
                        .await
                        .err_tip(|| "in batch_read_blobs")?;
                    self.cas_client(channel)
                        .batch_read_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_read_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .cas_client(ch.clone())
                    .batch_read_blobs(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::batch_read_blobs (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Batched RPC: prefer QUIC
                    self.cas_client(quic.clone())
                        .batch_read_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_read_blobs (dual/quic)")
                }
            }
        })
        .await;
        let elapsed_ms = call_start.elapsed().as_millis() as u64;
        if elapsed_ms > 5000 {
            warn!(
                instance_name = %instance_name_for_log,
                digest_count,
                elapsed_ms,
                ok = result.is_ok(),
                "GrpcStore::batch_read_blobs slow call (>5s) — \
                 confirm transport health (h2/TCP/QUIC keepalive, slow-tier hiccup)",
            );
        }
        info!(
            instance_name = %instance_name_for_log,
            digest_count,
            elapsed_ms,
            ok = result.is_ok(),
            "GrpcStore::batch_read_blobs call completed (#479)",
        );
        result
    }

    pub async fn get_tree(
        &self,
        grpc_request: Request<GetTreeRequest>,
    ) -> Result<Response<Streaming<GetTreeResponse>>, Error> {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let mut request = grpc_request.into_inner();
        request.instance_name.clone_from(&self.instance_name);
        // Capture the call start so we can warn on slow get_tree
        // completions (#479): a get_tree that takes >5 s is a typical
        // sign of slow-tier hiccup or remote scheduler latency. Per-
        // chunk progress is harder to wire here because callers use
        // tonic's Streaming::message() (sync envelope, not the
        // Stream poll path); the per-chunk observer wraps READ-side
        // streaming reads instead. The wall-clock log on completion
        // closes the silence gap by giving SREs at least one INFO
        // line per get_tree call.
        let call_start = std::time::Instant::now();
        let instance_name_for_log = self.instance_name.clone();
        let result = self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("get_tree".into())
                        .await
                        .err_tip(|| "in get_tree")?;
                    self.cas_client(channel)
                        .get_tree(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_tree")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .cas_client(ch.clone())
                    .get_tree(Request::new(request))
                    .await
                    .err_tip(|| "in GrpcStore::get_tree (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Metadata RPC: prefer QUIC
                    self.cas_client(quic.clone())
                        .get_tree(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_tree (dual/quic)")
                }
            }
        })
        .await;
        // Diagnostic log: every call (success or failure) gets one
        // INFO so a journal grep `GrpcStore::get_tree` always shows
        // wall-clock time. The slow-call warn surfaces above the
        // INFO when wall-clock exceeds 5 s — operationally the same
        // shape as the bytestream_server slow-completion warn.
        let elapsed_ms = call_start.elapsed().as_millis() as u64;
        if elapsed_ms > 5000 {
            warn!(
                instance_name = %instance_name_for_log,
                elapsed_ms,
                ok = result.is_ok(),
                "GrpcStore::get_tree slow call (>5s) — \
                 confirm transport health (h2/TCP/QUIC keepalive, slow-tier hiccup)",
            );
        }
        info!(
            instance_name = %instance_name_for_log,
            elapsed_ms,
            ok = result.is_ok(),
            "GrpcStore::get_tree call completed (#479)",
        );
        result
    }

    fn get_read_request(&self, mut request: ReadRequest) -> Result<ReadRequest, Error> {
        const IS_UPLOAD_FALSE: bool = false;
        let mut resource_info = ResourceInfo::new(&request.resource_name, IS_UPLOAD_FALSE)?;
        if resource_info.instance_name != self.instance_name {
            resource_info.instance_name = Cow::Borrowed(&self.instance_name);
            request.resource_name = resource_info.to_string(IS_UPLOAD_FALSE);
        }
        Ok(request)
    }

    /// Returns `(stream, channel_id)` where `channel_id` is
    /// `Some((endpoint_index, connection_index))` for TCP channels and
    /// `None` for QUIC. The id is used by callers to evict the specific
    /// failing channel via `evict_channel_by_id` (#2 Fix-A).
    /// Returns `(stream, channel_id, post_conn_instant)` on success, or
    /// `(err, channel_id, post_conn_instant)` on error.
    ///
    /// `channel_id` is `Some(ep_idx, conn_idx)` if a connection was acquired
    /// before the failure — the caller uses it for targeted Fix-A eviction on
    /// pre-stream errors (item #2 fix-up). If connection acquisition itself
    /// fails, `channel_id` is `None`.
    ///
    /// `post_conn_instant` is captured right after `cm.connection()` returns,
    /// BEFORE the `.read()` RPC call. K2 fix: callers compute `elapsed_ms` from
    /// this instant — excluding connection-acquire queue wait — so the
    /// `looks_like_latched_pool` 50ms gate is not defeated by saturation-induced
    /// queue delays. If connection acquisition failed (no channel acquired),
    /// `post_conn_instant` is set at the moment of failure; elapsed from it will
    /// be ~0ms and will not affect the latched-pool classifier (those errors have
    /// `channel_id = None` and take a different eviction path).
    ///
    // NOTE: channel_id is Some only for Transport::Tcp and
    // Transport::Dual+prefer_tcp=true. For QUIC and Dual+prefer_tcp=false,
    // channel identity is not tracked; those arms return None.
    async fn read_internal(
        &self,
        request: ReadRequest,
        prefer_tcp: bool,
    ) -> Result<
        (
            impl Stream<Item = Result<ReadResponse, Status>> + use<>,
            Option<(usize, usize)>,
            std::time::Instant,
        ),
        (Error, Option<(usize, usize)>, std::time::Instant),
    > {
        let _ = prefer_tcp; // Used only in the Dual transport arm (quic feature)
        let mut grpc_request = Request::new(request);
        if IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false) {
            grpc_request.metadata_mut().insert(
                "x-nativelink-worker",
                tonic::metadata::MetadataValue::from_static("true"),
            );
        }
        // #147 trace: capture the resource_name BEFORE the request
        // moves into the gRPC call so we can log "channel X selected
        // for resource Y" — the same resource_name appears in
        // get_part_single_stream's logs, providing a join key.
        let resource_for_log = grpc_request.get_ref().resource_name.clone();
        let (mut response, channel_id, post_conn_instant) = match &self.transport {
            Transport::Tcp(cm) => {
                let channel = cm
                    .connection("bytestream_read".into())
                    .await
                    .err_tip(|| "in read_internal")
                    .map_err(|e| (e, None, std::time::Instant::now()))?;
                let (ep_idx, conn_idx) = channel.channel_id_for_log();
                // K2 fix: clock starts AFTER channel acquisition so the
                // elapsed used in looks_like_latched_pool excludes queue wait.
                let post_conn = std::time::Instant::now();
                info!(
                    resource_name = %resource_for_log,
                    transport = "tcp",
                    endpoint_index = ep_idx,
                    connection_index = conn_idx,
                    "GrpcStore::read_internal channel acquired (#147 trace)",
                );
                let resp = self.bs_client(channel)
                    .read(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::read")
                    .map_err(|e| (e, Some((ep_idx, conn_idx)), post_conn))?
                    .into_inner();
                (resp, Some((ep_idx, conn_idx)), post_conn)
            }
            #[cfg(feature = "quic")]
            Transport::Quic(ch) => {
                let post_conn = std::time::Instant::now();
                info!(
                    resource_name = %resource_for_log,
                    transport = "quic",
                    "GrpcStore::read_internal channel acquired (#147 trace)",
                );
                let resp = self.bs_client(ch.clone())
                    .read(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::read (quic)")
                    .map_err(|e| (e, None, post_conn))?
                    .into_inner();
                (resp, None, post_conn)
            }
            #[cfg(feature = "quic")]
            Transport::Dual { tcp, quic } => {
                if prefer_tcp {
                    // Parallel chunked reads: prefer TCP (2x faster at
                    // high concurrency)
                    let channel = tcp
                        .connection("bytestream_read".into())
                        .await
                        .err_tip(|| "in read_internal (dual/tcp)")
                        .map_err(|e| (e, None, std::time::Instant::now()))?;
                    let (ep_idx, conn_idx) = channel.channel_id_for_log();
                    let post_conn = std::time::Instant::now();
                    info!(
                        resource_name = %resource_for_log,
                        transport = "dual/tcp",
                        endpoint_index = ep_idx,
                        connection_index = conn_idx,
                        "GrpcStore::read_internal channel acquired (#147 trace)",
                    );
                    let resp = self.bs_client(channel)
                        .read(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::read (dual/tcp)")
                        .map_err(|e| (e, Some((ep_idx, conn_idx)), post_conn))?
                        .into_inner();
                    (resp, Some((ep_idx, conn_idx)), post_conn)
                } else {
                    // Single-stream reads: prefer QUIC (2.6x faster)
                    let post_conn = std::time::Instant::now();
                    info!(
                        resource_name = %resource_for_log,
                        transport = "dual/quic",
                        "GrpcStore::read_internal channel acquired (#147 trace)",
                    );
                    let resp = self.bs_client(quic.clone())
                        .read(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::read (dual/quic)")
                        .map_err(|e| (e, None, post_conn))?
                        .into_inner();
                    (resp, None, post_conn)
                }
            }
        };
        let first_response = response
            .message()
            .await
            .err_tip(|| "Fetching first chunk in GrpcStore::read()")
            .map_err(|e| (e, channel_id, post_conn_instant))?;
        // #479: wrap the response stream so SREs can observe per-chunk
        // arrival pacing (process-wide GRPC_READ_SLOW_CHUNK_TOTAL counter
        // + rate-limited warn) on EVERY single-stream read. The
        // observer is structurally transparent — diagnostic-only —
        // and never aborts the stream. The label disambiguates this
        // path from `get_part_parallel`'s parallel chunk reads in
        // journal greps.
        Ok((
            ReadProgressObserver::new(
                "GrpcStore::read_internal",
                FirstStream::new(first_response, response),
            ),
            channel_id,
            post_conn_instant,
        ))
    }

    pub async fn read<R>(
        &self,
        grpc_request: R,
    ) -> Result<impl Stream<Item = Result<ReadResponse, Status>> + use<R>, Error>
    where
        R: IntoRequest<ReadRequest>,
    {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let request = self.get_read_request(grpc_request.into_request().into_inner())?;
        self.perform_request(request, |request| async move {
            // Discard channel_id and post_conn_instant — the public `read` API
            // does not expose them; targeted eviction is internal to retry loops.
            self.read_internal(request, false).await
                .map(|(s, _, _)| s)
                .map_err(|(e, _, _)| e)
        })
        .await
    }

    pub async fn write<T, E>(
        &self,
        stream: WriteRequestStreamWrapper<T>,
    ) -> Result<Response<WriteResponse>, Error>
    where
        T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
        E: Into<Error> + 'static,
    {
        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        // Capture the mirror flag from the task-local before entering the
        // retry loop. The flag is set by WorkerProxyStore's mirror functions
        // and propagates through the GrpcStore to become an RPC header.
        let is_mirror = IS_MIRROR_REQUEST.try_with(|v| *v).unwrap_or(false);
        // #168 code-reviewer S1: capture IS_WORKER_REQUEST and propagate
        // as `x-nativelink-worker` header so the server can skip the
        // dispatcher fan-out on worker-originated bytestream uploads.
        let is_worker = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);

        // Per-chunk no-progress **diagnostic** timer (not a kill switch).
        // Configured via `rpc_timeout_s` but applied per-chunk: each
        // WriteRequest delivered from the upstream stream resets the
        // timer. When no chunk arrives for `rpc_timeout`, the wrapper
        // emits a `warn!` + bumps `proto_stream_utils::
        // GRPC_WRITE_SLOW_CHUNK_TOTAL` and re-arms the timer for another
        // window (so a stream stuck for hours produces one warn per
        // window, not just one). It does NOT abort the RPC, does NOT
        // return `DeadlineExceeded`, does NOT set `read_stream_error`.
        //
        // The pre-2026-05-14 abort-on-elapse and the older whole-RPC
        // `tokio::time::timeout` both killed legitimate slow-but-
        // progressing mirror writes (a 50 MB blob through a slow Bazel
        // client at 2 MB/s legitimately takes 25s end-to-end), each kill
        // breaking the >=2-replica durability invariant (641 events on
        // 2026-04-23). Dead-connection detection is now the transport's
        // job (h2 keepalive 30s/20s, TCP keepalive, QUIC keepalive 5s);
        // per-chunk timers are observability only (user direction
        // 2026-05-14, sibling to chunked_driver's PER_CHUNK_WRITE_TIMEOUT
        // diagnostic-only conversion).
        let rpc_timeout = self.rpc_timeout;
        let local_state = Arc::new(Mutex::new(WriteState::with_progress_timeout(
            self.instance_name.clone(),
            stream,
            rpc_timeout,
        )));

        let write_start = std::time::Instant::now();
        let instance_name = self.instance_name.clone();
        // #59 instrumentation: promoted trace!→info! at GrpcStore::write
        // entry. Per #56 RCA §8, the 1-46 ms worker-side `elapsed_ms`
        // observed on `receiver disconnected` events is currently
        // unobservable at info! level — we don't know whether
        // GrpcStore::write even entered, nor which retrier branch fired.
        // Observability-only; no behavior change. Demoted to debug after the
        // #56/#62 fix (6fc05054) was verified healthy in production.
        debug!(
            instance_name = %instance_name,
            progress_timeout_s = rpc_timeout.as_secs(),
            is_mirror,
            is_worker,
            arm_name = "entry",
            "#59 GrpcStore::write: starting ByteStream write",
        );
        let mut attempt: u32 = 0;
        // #8 Fix-C: consecutive instant-fail latched-pool attempts.
        // Arc<AtomicU32> (not mut u32) because the counter is accessed only
        // inside the inner `async move` block; a plain `mut u32` would be
        // moved into the first future and the outer FnMut closure would lose
        // ownership — resetting to 0 on every attempt.
        let write_latched_fails = Arc::new(AtomicU32::new(0));
        let result = self
            .retrier
            .retry(unfold(local_state, move |local_state| {
                attempt += 1;
                let instance_name = instance_name.clone();
                let write_latched_fails = write_latched_fails.clone();
                async move {
                    // The client write may occur on a separate thread and
                    // therefore in order to share the state with it we have to
                    // wrap it in a Mutex and retrieve it after the write
                    // has completed.  There is no way to get the value back
                    // from the client.
                    // #59 instrumentation: per #56 RCA §8 rec 2, info! at
                    // the top of the unfold closure with attempt + flags
                    // surfaces which retrier iteration is in flight.
                    debug!(
                        instance_name = %instance_name,
                        attempt,
                        is_mirror,
                        is_worker,
                        arm_name = "attempt_start",
                        "#59 GrpcStore::write: requesting connection from pool",
                    );
                    let conn_start = std::time::Instant::now();
                    // #8 Fix-C: record how many ms elapsed from conn_start when the
                    // channel was acquired (i.e., after acquire_write_channel returns).
                    // rpc_elapsed_ms = conn_start.elapsed() - channel_acquire_offset_ms
                    // excludes the connection-acquire queue wait (K2 lesson).
                    // u64::MAX means "channel not yet acquired" (Quic path sets it to 0).
                    // Plain AtomicU64 (not Arc): `rpc_fut` is `async { }` (non-move),
                    // so it borrows `channel_acquire_offset_ms` by shared reference
                    // from this `async move` scope. The outer error arm reads it after
                    // `rpc_fut.await` — same task, sequential, no concurrent access.
                    let channel_acquire_offset_ms = AtomicU64::new(u64::MAX);
                    let instance_for_rpc = instance_name.clone();
                    let local_state_for_rpc = local_state.clone();

                    /// Helper: build the tonic Request for a ByteStream write,
                    /// attaching the `x-nativelink-mirror` header when the
                    /// write originates from a server-side mirror operation,
                    /// AND the `x-nativelink-worker` header when the write
                    /// originates from a worker (so the server skips the
                    /// dispatcher fan-out — #168 code-reviewer S1).
                    fn make_write_request<T, E>(
                        state: Arc<Mutex<WriteState<T, E>>>,
                        is_mirror: bool,
                        is_worker: bool,
                    ) -> Request<WriteStateWrapper<T, E>>
                    where
                        T: Stream<Item = Result<WriteRequest, E>> + Unpin + Send + 'static,
                        E: Into<Error> + 'static,
                    {
                        let mut request = Request::new(WriteStateWrapper::new(state));
                        if is_mirror {
                            request.metadata_mut().insert(
                                "x-nativelink-mirror",
                                tonic::metadata::MetadataValue::from_static("1"),
                            );
                        }
                        if is_worker {
                            request.metadata_mut().insert(
                                "x-nativelink-worker",
                                tonic::metadata::MetadataValue::from_static("1"),
                            );
                        }
                        request
                    }

                    let rpc_fut = async {
                        match &self.transport {
                            Transport::Tcp(cm) => {
                                let channel = self
                                    .acquire_write_channel(cm, "bytestream_write")
                                    .await
                                    .err_tip(|| "in GrpcStore::write")?;
                                let conn_elapsed_ms = u64::try_from(
                                    conn_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                // #8 Fix-C: record ms-since-conn_start at channel acquisition.
                                // rpc_elapsed = total - this_value excludes queue wait (K2).
                                channel_acquire_offset_ms
                                    .store(conn_elapsed_ms, Ordering::Relaxed);
                                // #59 instrumentation: demoted to debug! (hot-loop scaffolding).
                                debug!(
                                    instance_name = %instance_for_rpc,
                                    conn_elapsed_ms,
                                    arm_name = "conn_acquired_tcp",
                                    "#59 GrpcStore::write: got connection, starting ByteStream.Write RPC",
                                );
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(channel)
                                    .write(make_write_request(local_state_for_rpc, is_mirror, is_worker))
                                    .await
                                    .err_tip(|| "in GrpcStore::write");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                // #59 instrumentation: demoted to debug! (hot-loop scaffolding).
                                debug!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    arm_name = "rpc_returned_tcp",
                                    "#59 GrpcStore::write: ByteStream.Write RPC returned",
                                );
                                res
                            }
                            #[cfg(feature = "quic")]
                            Transport::Quic(ch) => {
                                // QUIC has no separate acquire_write_channel;
                                // set conn_elapsed to 0 so rpc_elapsed ≈ total elapsed.
                                channel_acquire_offset_ms.store(0, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(ch.clone())
                                    .write(make_write_request(local_state_for_rpc, is_mirror, is_worker))
                                    .await
                                    .err_tip(|| "in GrpcStore::write (quic)");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                // #59 instrumentation: demoted to debug! (hot-loop scaffolding).
                                debug!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    arm_name = "rpc_returned_quic",
                                    "#59 GrpcStore::write: ByteStream.Write RPC returned (quic)",
                                );
                                res
                            }
                            #[cfg(feature = "quic")]
                            Transport::Dual { tcp, .. } => {
                                // Large streaming writes: prefer TCP (1.1x faster)
                                let channel = self
                                    .acquire_write_channel(tcp, "bytestream_write")
                                    .await
                                    .err_tip(|| "in GrpcStore::write (dual/tcp)")?;
                                let conn_elapsed_ms = u64::try_from(
                                    conn_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                // #8 Fix-C: record ms-since-conn_start at channel acquisition.
                                channel_acquire_offset_ms
                                    .store(conn_elapsed_ms, Ordering::Relaxed);
                                // #59 instrumentation: demoted to debug! (hot-loop scaffolding).
                                debug!(
                                    instance_name = %instance_for_rpc,
                                    conn_elapsed_ms,
                                    arm_name = "conn_acquired_dual",
                                    "#59 GrpcStore::write: got connection, starting ByteStream.Write RPC (dual/tcp)",
                                );
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(channel)
                                    .write(make_write_request(local_state_for_rpc, is_mirror, is_worker))
                                    .await
                                    .err_tip(|| "in GrpcStore::write (dual/tcp)");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                // #59 instrumentation: demoted to debug! (hot-loop scaffolding).
                                debug!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    arm_name = "rpc_returned_dual",
                                    "#59 GrpcStore::write: ByteStream.Write RPC returned (dual/tcp)",
                                );
                                res
                            }
                        }
                    };

                    // Per-chunk progress diagnostic is observed inside
                    // WriteStateWrapper::poll_next via WriteState's
                    // progress_deadline; it emits warns + bumps the
                    // process-wide counter but does NOT abort the RPC
                    // (diagnostic-only since 2026-05-14). The remaining
                    // `read_stream_error` set site is the structurally
                    // non-resumable resource_name parse error path. No
                    // outer whole-RPC deadline either — both the older
                    // tokio::time::timeout and the recent abort-on-
                    // elapse were killing slow-but-progressing mirror
                    // writes and breaking the >=2-replica durability
                    // invariant.
                    let result = rpc_fut.await;

                    // Get the state back from StateWrapper, this should be
                    // uncontended since write has returned.
                    let mut local_state_locked = local_state.lock();

                    // #59 instrumentation: surface read_stream_error
                    // arm-decision (otherwise it is silently masked into
                    // RetryResult::Err with no log line at info! level).
                    if let Some(err) = local_state_locked.take_read_stream_error() {
                        info!(
                            instance_name = %instance_name,
                            attempt,
                            ?err,
                            arm_name = "read_stream_error",
                            "#59 GrpcStore::write: read_stream_error set (per-chunk timer or resource-name parse)",
                        );
                        let result = RetryResult::Err(err.append("Where read_stream_error was set"));
                        drop(local_state_locked);
                        return Some((result, local_state));
                    }
                    let result = match result {
                        Ok(response) => {
                            // #59 instrumentation: per #56 RCA §4 O1 —
                            // normal success path (producer sent EOF,
                            // unfold returned None). This is the ONLY
                            // non-AlreadyExists Ok-returning path. Demoted to
                            // debug after the #56/#62 fix was verified healthy.
                            debug!(
                                instance_name = %instance_name,
                                attempt,
                                arm_name = "rpc_ok",
                                "#59 GrpcStore::write: RPC returned Ok (normal success)",
                            );
                            RetryResult::Ok(response)
                        }
                        Err(ref err)
                            if err.code == Code::AlreadyExists =>
                        {
                            // #59 instrumentation: per #56 RCA §4 O2 —
                            // AlreadyExists silenced to Ok. The audit
                            // REFUTED this as a production trigger
                            // (zero AlreadyExists log entries in 12.85 h
                            // of journal). If this fires in production,
                            // the refutation is wrong.
                            info!(
                                instance_name = %instance_name,
                                attempt,
                                ?err,
                                arm_name = "already_exists_silenced",
                                "#59 GrpcStore::write: AlreadyExists silenced to Ok (committed_size=0)",
                            );
                            RetryResult::Ok(Response::new(WriteResponse {
                                committed_size: 0,
                            }))
                        }
                        Err(ref err) => {
                            // #8 Fix-C: compute elapsed since channel acquisition
                            // (excludes queue wait, per K2 lesson).
                            // channel_acquire_offset_ms == u64::MAX means channel
                            // was never acquired (acquire_write_channel error); use
                            // total conn_start elapsed as a safe fallback.
                            let rpc_elapsed_ms = {
                                let total_ms = conn_start.elapsed().as_millis() as u64;
                                let conn_ms = channel_acquire_offset_ms
                                    .load(Ordering::Relaxed);
                                if conn_ms == u64::MAX {
                                    total_ms
                                } else {
                                    total_ms.saturating_sub(conn_ms)
                                }
                            };
                            // #59 instrumentation: arm_name distinguishes
                            // this from the silenced AlreadyExists branch
                            // in journal queries; keep the pre-existing
                            // warn! since it is the operator-facing alert
                            // for unmasked RPC failures.
                            warn!(
                                instance_name = %instance_name,
                                attempt,
                                ?err,
                                rpc_elapsed_ms,
                                can_resume = local_state_locked.can_resume(),
                                arm_name = "rpc_err",
                                "#59 GrpcStore::write: RPC failed",
                            );
                            // #147: belt-and-suspenders eviction.
                            self.evict_pool_on_transport_err(err);
                            // #8 Fix-C: if every attempt is an instant-fail
                            // latched-pool hit, abort retries early.
                            let consecutive_latched_fails =
                                if looks_like_latched_pool(err, rpc_elapsed_ms) {
                                    write_latched_fails.fetch_add(1, Ordering::Relaxed) + 1
                                } else {
                                    write_latched_fails.store(0, Ordering::Relaxed);
                                    0
                                };
                            if consecutive_latched_fails >= LATCHED_POOL_ABORT_THRESHOLD {
                                warn!(
                                    instance_name = %instance_name,
                                    attempt,
                                    consecutive_latched_fails,
                                    "GrpcStore: aborting write retries — pool is \
                                     systemically latched (all write attempts are \
                                     instant-fail tower-buffer ServiceError); \
                                     callers fall back instead of burning the ~28s \
                                     retry budget (#8 Fix-C)"
                                );
                                drop(local_state_locked);
                                return Some((RetryResult::Err(err.clone()), local_state));
                            }
                            if local_state_locked.can_resume() {
                                local_state_locked.resume();
                                RetryResult::Retry(err.clone())
                            } else {
                                RetryResult::Err(
                                    err.clone().append("Retry is not possible"),
                                )
                            }
                        }
                    };

                    drop(local_state_locked);
                    Some((result, local_state))
                }
            }))
            .await?;

        let total_elapsed = write_start.elapsed();
        let total_elapsed_ms = u64::try_from(total_elapsed.as_millis()).unwrap_or(u64::MAX);
        // #59 instrumentation: total elapsed surfaces the 1-46 ms fast-
        // fail vs full-RPC durations called out in #56 RCA §6 M3. Demoted to
        // debug after the #56/#62 fix was verified healthy in production.
        debug!(
            instance_name = %self.instance_name,
            total_elapsed_ms,
            arm_name = "completed",
            "#59 GrpcStore::write: completed successfully",
        );
        // The per-chunk progress timer hides whole-RPC duration from the
        // operator (the previous whole-RPC timeout used to surface it as
        // a cancellation). Keep the long-RPC signal as a warn on success
        // so we can still detect "the transport is healthy but extremely
        // slow" — e.g. WAN clients streaming large blobs.
        if total_elapsed > Duration::from_secs(60) {
            warn!(
                instance_name = %self.instance_name,
                total_elapsed_ms,
                "GrpcStore::write succeeded but took > 60s; chunks were progressing but transport is slow",
            );
        }
        Ok(result)
    }

    pub async fn query_write_status(
        &self,
        grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Error> {
        const IS_UPLOAD_TRUE: bool = true;

        error_if!(
            matches!(self.store_type, nativelink_config::stores::StoreType::Ac),
            "CAS operation on AC store"
        );

        let mut request = grpc_request.into_inner();

        let mut request_info = ResourceInfo::new(&request.resource_name, IS_UPLOAD_TRUE)?;
        if request_info.instance_name != self.instance_name {
            request_info.instance_name = Cow::Borrowed(&self.instance_name);
            request.resource_name = request_info.to_string(IS_UPLOAD_TRUE);
        }

        self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("query_write_status".into())
                        .await
                        .err_tip(|| "in query_write_status")?;
                    self.bs_client(channel)
                        .query_write_status(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::query_write_status")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .bs_client(ch.clone())
                    .query_write_status(Request::new(request))
                    .await
                    .err_tip(|| "in GrpcStore::query_write_status (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Small metadata RPC: prefer QUIC
                    self.bs_client(quic.clone())
                        .query_write_status(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::query_write_status (dual/quic)")
                }
            }
        })
        .await
    }

    pub async fn get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Error> {
        let mut request = grpc_request.into_inner();
        request.instance_name.clone_from(&self.instance_name);
        // #463 fix-up (perf-optimizer BLOCK): when the call originates
        // from `AcProxyStore::try_read_from_peer`, attach
        // `x-nativelink-peer-fetch: 1` so the receiving worker's
        // `AcServer` can recognise the peer-fetch hop and refuse to
        // re-shortcut into a worker-side `GrpcStore`. Without this
        // marker, a worker whose configured AC store is itself a bare
        // `GrpcStore` pointing back at the central server (the
        // in-tree `deployment-examples/.../worker.json5` shape) would
        // loop server → worker → server until h2 keepalive (`60s`).
        // Mirrors the established `IS_MIRROR_REQUEST` /
        // `x-nativelink-mirror` write-side pattern at
        // `update_action_result` below.
        let is_peer_fetch = IS_AC_PEER_FETCH.try_with(|v| *v).unwrap_or(false);
        self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_peer_fetch {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-peer-fetch",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("get_action_result".into())
                        .await
                        .err_tip(|| "in get_action_result")?;
                    self.ac_client(channel)
                        .get_action_result(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::get_action_result")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .ac_client(ch.clone())
                    .get_action_result(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::get_action_result (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // AC lookup: prefer QUIC
                    self.ac_client(quic.clone())
                        .get_action_result(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::get_action_result (dual/quic)")
                }
            }
        })
        .await
    }

    pub async fn update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Error> {
        let mut request = grpc_request.into_inner();
        request.instance_name.clone_from(&self.instance_name);
        // #168 code-reviewer S1: propagate IS_WORKER_REQUEST + IS_MIRROR_REQUEST
        // task-locals into the AC update_action_result RPC metadata so the
        // server's AC dispatcher hook can skip fan-out on worker- /
        // mirror-originated AC writes (loop prevention).
        let is_worker = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
        let is_mirror = IS_MIRROR_REQUEST.try_with(|v| *v).unwrap_or(false);
        self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_worker {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-worker",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            if is_mirror {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-mirror",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm
                        .connection("update_action_result".into())
                        .await
                        .err_tip(|| "in update_action_result")?;
                    self.ac_client(channel)
                        .update_action_result(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::update_action_result")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => self
                    .ac_client(ch.clone())
                    .update_action_result(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::update_action_result (quic)"),
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Small AC update: prefer QUIC
                    self.ac_client(quic.clone())
                        .update_action_result(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::update_action_result (dual/quic)")
                }
            }
        })
        .await
    }

    async fn get_action_result_from_digest(
        &self,
        digest: DigestInfo,
    ) -> Result<Response<ActionResult>, Error> {
        let action_result_request = GetActionResultRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(digest.into()),
            inline_stdout: false,
            inline_stderr: false,
            inline_output_files: Vec::new(),
            digest_function: Context::current()
                .get::<DigestHasherFunc>()
                .map_or_else(default_digest_hasher_func, |v| *v)
                .proto_digest_func()
                .into(),
        };
        self.get_action_result(Request::new(action_result_request))
            .await
    }

    async fn get_action_result_as_part(
        &self,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let action_result = self
            .get_action_result_from_digest(digest)
            .await
            .map(Response::into_inner)
            .err_tip(|| "Action result not found")?;
        // TODO: Would be better to avoid all the encoding and decoding in this
        //       file, however there's no way to currently get raw bytes from a
        //       generated prost request unfortunately.
        let mut value = BytesMut::new();
        action_result
            .encode(&mut value)
            .err_tip(|| "Could not encode upstream action result")?;

        let default_len = value.len() - offset;
        let length = length.unwrap_or(default_len).min(default_len);
        if length > 0 {
            writer
                .send(value.freeze().slice(offset..offset + length))
                .await
                .err_tip(|| "Failed to write data in grpc store")?;
        }
        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF in grpc store get_action_result_as_part")?;
        Ok(())
    }

    async fn update_action_result_from_bytes(
        &self,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<(), Error> {
        let action_result = ActionResult::decode(reader.consume(None).await?)
            .err_tip(|| "Failed to decode ActionResult in update_action_result_from_bytes")?;
        let update_action_request = UpdateActionResultRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(digest.into()),
            action_result: Some(action_result),
            results_cache_policy: None,
            digest_function: Context::current()
                .get::<DigestHasherFunc>()
                .map_or_else(default_digest_hasher_func, |v| *v)
                .proto_digest_func()
                .into(),
            // (#12 H4) Populated in phase 2 when the server-side
            // registration path is wired. Empty here — this code path
            // is GrpcStore's internal AC proxy, not a worker-originated
            // write.
            cas_endpoint: String::new(),
        };
        self.update_action_result(Request::new(update_action_request))
            .await
            .map(|_| ())
    }

    /// Single-stream ByteStream read with retry support. Used for blobs
    /// below the parallel chunk threshold.
    async fn get_part_single_stream(
        &self,
        resource_name: String,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let entry_at = std::time::Instant::now();
        info!(
            %resource_name,
            offset,
            ?length,
            "GrpcStore::get_part_single_stream entry",
        );
        struct LocalState<'a> {
            resource_name: String,
            writer: &'a mut DropCloserWriteHalf,
            read_offset: i64,
            read_limit: i64,
            /// Bytes received in the current stream attempt, reset on each
            /// retry. Used to detect empty responses from stale workers.
            bytes_received_this_stream: i64,
            /// Diagnostic: timestamp of the most recent stream frame
            /// (Some(message) or None). Used to log frames that took
            /// >100ms to arrive — surfaces gRPC stalls inside a single
            /// stream attempt without per-chunk noise.
            last_frame_at: std::time::Instant,
            attempt: u32,
            /// #2 Fix-C: consecutive instant-fail latched-pool attempts.
            /// When this reaches `LATCHED_POOL_ABORT_THRESHOLD` we abort
            /// retries immediately via `RetryResult::Err` so the FSS
            /// `run_producer` writes a streaming-entry terminal error and
            /// readers fall back (slow-store / peer-fetch) within seconds
            /// instead of burning the full 30s notify timeout.
            consecutive_latched_fails: u32,
        }

        let local_state = LocalState {
            resource_name,
            writer,
            read_offset: i64::try_from(offset).err_tip(|| "Could not convert offset to i64")?,
            read_limit: i64::try_from(length.unwrap_or(0))
                .err_tip(|| "Could not convert length to i64")?,
            bytes_received_this_stream: 0,
            last_frame_at: std::time::Instant::now(),
            attempt: 0,
            consecutive_latched_fails: 0,
        };

        let result = self.retrier
            .retry(unfold(local_state, move |mut local_state| async move {
                local_state.attempt += 1;
                info!(
                    resource_name = %local_state.resource_name,
                    attempt = local_state.attempt,
                    read_offset = local_state.read_offset,
                    "GrpcStore::get_part_single_stream attempt entry",
                );
                let request = ReadRequest {
                    resource_name: local_state.resource_name.clone(),
                    read_offset: local_state.read_offset,
                    read_limit: local_state.read_limit,
                };
                let (mut stream, channel_id) = match self
                    .read_internal(request, false)
                    .await
                {
                    Ok((stream, channel_id, _post_conn)) => (stream, channel_id),
                    Err((err, channel_id_opt, post_conn)) => {
                        // K2 fix: `post_conn` is set inside read_internal right
                        // after channel acquisition (before the .read() call).
                        // Using post_conn.elapsed() excludes the connection-acquire
                        // queue wait — so the latched-pool 50ms gate is not defeated
                        // by saturation-induced cm.connection() delays.
                        let attempt_elapsed_ms = post_conn.elapsed().as_millis() as u64;
                        let err = err.append("in GrpcStore::get_part()");
                        info!(
                            resource_name = %local_state.resource_name,
                            attempt = local_state.attempt,
                            attempt_elapsed_ms,
                            code = ?err.code,
                            "GrpcStore::get_part_single_stream read_internal failed",
                        );
                        // #2 Fix-A fix-up (item 2): if a channel was acquired before
                        // the pre-stream error, evict it by identity. The latch
                        // class (tower-Buffer ServiceError) fires after channel
                        // acquisition — channel_id_opt is Some for Tcp transport.
                        // For connection-acquisition failures, channel_id_opt is None
                        // and we fall back to legacy any-channel eviction.
                        if let Some((ep_idx, conn_idx)) = channel_id_opt {
                            match &self.transport {
                                Transport::Tcp(cm) => cm.evict_channel_by_id(
                                    ep_idx, conn_idx,
                                    format!("pre-stream err: code={:?} (#2 Fix-A)", err.code),
                                ),
                                #[cfg(feature = "quic")]
                                Transport::Dual { tcp, .. } => tcp.evict_channel_by_id(
                                    ep_idx, conn_idx,
                                    format!("pre-stream err: code={:?} (#2 Fix-A)", err.code),
                                ),
                                #[cfg(feature = "quic")]
                                Transport::Quic(_) => {}
                            }
                        } else {
                            // No channel identity (connection acquisition failed or
                            // QUIC transport) — use legacy any-channel eviction.
                            self.evict_pool_on_transport_err(&err);
                        }
                        // #FU QUIC read-failure diagnostic (observability-only):
                        // record the latch-vs-backpressure split. `prefer_tcp =
                        // false` here (single-stream), so under Dual this read
                        // went over QUIC; counted only on the genuine QUIC leg.
                        // No control-flow effect — reuses `attempt_elapsed_ms`.
                        self.record_quic_read_fail(&err, attempt_elapsed_ms, false);
                        // #2 Fix-C: if every attempt is an instant-fail
                        // latched-pool hit, abort retries early so FSS
                        // run_producer writes terminal immediately and readers
                        // fall back within seconds (not 30s).
                        // Note: for pre-stream errors, the tower-Buffer ServiceError
                        // fires synchronously after channel acquisition, so elapsed
                        // is ~0ms and look_like_latched_pool correctly classifies it.
                        if looks_like_latched_pool(&err, attempt_elapsed_ms) {
                            local_state.consecutive_latched_fails += 1;
                        } else {
                            local_state.consecutive_latched_fails = 0;
                        }
                        if local_state.consecutive_latched_fails >= LATCHED_POOL_ABORT_THRESHOLD {
                            warn!(
                                resource_name = %local_state.resource_name,
                                attempt = local_state.attempt,
                                consecutive_latched_fails = local_state.consecutive_latched_fails,
                                "GrpcStore: aborting retries — pool is systemically latched \
                                 (all attempts are instant-fail tower-buffer ServiceError); \
                                 FSS run_producer will set terminal immediately so readers \
                                 fall back instead of burning the 30s notify timeout (#2 Fix-C)"
                            );
                            return Some((RetryResult::Err(err), local_state));
                        }
                        return Some((RetryResult::Retry(err), local_state))
                    }
                };
                // A successful read_internal means the pool is not latched.
                local_state.consecutive_latched_fails = 0;

                // Reset per-stream counter so we detect empty responses even
                // when retrying at a non-zero read_offset.
                local_state.bytes_received_this_stream = 0;
                local_state.last_frame_at = std::time::Instant::now();

                loop {
                    let frame_wait_start = std::time::Instant::now();
                    let data = match stream.next().await {
                        None => Bytes::new(),
                        Some(Ok(message)) => message.data,
                        Some(Err(status)) => {
                            // #147: streaming-body errs bypass ResponseFuture::poll; evict here.
                            // #2 Fix-A: evict the specific channel that produced this error.
                            // channel_id is Some for Transport::Tcp and Dual+prefer_tcp=true
                            // (single-stream uses QUIC arm which returns None — falls through
                            // to the legacy any-channel eviction in the else branch).
                            let err: Error = status.into();
                            if let Some((ep_idx, conn_idx)) = channel_id {
                                match &self.transport {
                                    Transport::Tcp(cm) => cm.evict_channel_by_id(
                                        ep_idx, conn_idx,
                                        format!(
                                            "streaming-body transport err: code={:?} (#2 Fix-A)",
                                            err.code
                                        ),
                                    ),
                                    #[cfg(feature = "quic")]
                                    Transport::Dual { tcp, .. } => tcp.evict_channel_by_id(
                                        ep_idx, conn_idx,
                                        format!(
                                            "streaming-body transport err: code={:?} (#2 Fix-A)",
                                            err.code
                                        ),
                                    ),
                                    #[cfg(feature = "quic")]
                                    Transport::Quic(_) => {}
                                }
                            } else {
                                self.evict_pool_on_transport_err(&err);
                            }
                            return Some((
                                RetryResult::Retry(
                                    err.append(
                                        "While fetching message in \
                                         GrpcStore::get_part()",
                                    ),
                                ),
                                local_state,
                            ));
                        }
                    };
                    let frame_wait_ms = frame_wait_start.elapsed().as_millis() as u64;
                    if frame_wait_ms > 100 {
                        warn!(
                            resource_name = %local_state.resource_name,
                            attempt = local_state.attempt,
                            frame_wait_ms,
                            bytes_received_this_stream = local_state.bytes_received_this_stream,
                            "GrpcStore::get_part_single_stream slow frame",
                        );
                    }
                    local_state.last_frame_at = std::time::Instant::now();
                    let length = data.len() as i64;
                    if length == 0 {
                        // 0-byte chunk == EOF marker from the server. Source
                        // sides are now responsible for rejecting non-zero
                        // digests with empty data at insert time (see
                        // fast_slow_store.rs::insert_mirror_blob and the
                        // defensive guards in get_part for mirror_blobs +
                        // in_flight_slow_writes). The previous workaround
                        // here that translated empty-stream into a retryable
                        // NotFound was a downstream symptom-catcher; with
                        // the source-side invariants in place an empty
                        // stream genuinely means "blob is empty" and we
                        // forward EOF without inferring stale-worker.
                        //
                        // Observability: log when this stream attempt
                        // received zero bytes total. With the source-side
                        // validation in place (insert_mirror_blob +
                        // get_part defensive guards) this should be rare —
                        // the legitimate case is reading a zero-digest blob
                        // OR resuming a read at exactly offset == size.
                        // Anything else suggests a producer that bypasses
                        // insert_mirror_blob, which is the upstream bug
                        // we're hunting. The resource_name encodes the
                        // digest+size so an operator can decode whether
                        // the empty stream was legitimate.
                        if local_state.bytes_received_this_stream == 0 {
                            warn!(
                                resource_name = %local_state.resource_name,
                                read_offset = local_state.read_offset,
                                attempt = local_state.attempt,
                                "GrpcStore::get_part: peer returned 0-byte success \
                                 for this stream attempt — legitimate only if \
                                 zero-digest or offset==size; otherwise an upstream \
                                 invariant violation (see fast_slow_store.rs::insert_mirror_blob)"
                            );
                        }
                        let eof_result = local_state
                            .writer
                            .send_eof()
                            .err_tip(|| {
                                "Could not send eof in GrpcStore::get_part()"
                            })
                            .map_or_else(RetryResult::Err, RetryResult::Ok);
                        return Some((eof_result, local_state));
                    }
                    if let Err(err) = local_state
                        .writer
                        .send(data)
                        .await
                        .err_tip(|| {
                            "While sending in GrpcStore::get_part()"
                        })
                    {
                        // #147 trace: classify whether this writer-side
                        // error LOOKS like a dead-channel error (it
                        // SHOULDN'T — this is the downstream
                        // buf_channel reader being dropped, not the
                        // upstream gRPC channel being broken — but the
                        // production error string "Tried to send while
                        // stream is closed" is generated here AND
                        // matches `looks_like_dead_channel`. If we
                        // observe this branch firing in production
                        // with `looks_like_dead = true`, it means the
                        // 109k events are caused by writer-termination
                        // (caller dropped the read half), NOT by
                        // upstream channel staleness, and adding
                        // eviction here would be a misdiagnosis fix.
                        let looks_like_dead = looks_like_dead_channel(&err);
                        let msg_head: &str = err
                            .messages
                            .first()
                            .map(|m| m.as_str())
                            .unwrap_or("<no_message>");
                        let msg_head_short: String =
                            msg_head.chars().take(80).collect();
                        warn!(
                            resource_name = %local_state.resource_name,
                            attempt = local_state.attempt,
                            bytes_received_this_stream =
                                local_state.bytes_received_this_stream,
                            chunk_len = length,
                            code = ?err.code,
                            %msg_head_short,
                            looks_like_dead,
                            "GrpcStore::get_part_single_stream writer.send failed (#147 trace) — RetryResult::Err returned, no eviction invoked",
                        );
                        return Some((RetryResult::Err(err), local_state));
                    }
                    local_state.read_offset += length;
                    local_state.bytes_received_this_stream += length;
                }
            }))
            .await;
        let elapsed_ms = entry_at.elapsed().as_millis() as u64;
        match &result {
            Ok(()) => info!(elapsed_ms, "GrpcStore::get_part_single_stream exit Ok",),
            Err(err) => info!(
                elapsed_ms,
                code = ?err.code,
                "GrpcStore::get_part_single_stream exit Err",
            ),
        }
        result
    }

    /// Per-chunk channel capacity for streaming parallel reads.
    /// Each slot holds one gRPC ReadResponse frame (~1 MiB max with
    /// our h2 frame size). 8 slots = ~8 MiB buffered per chunk
    /// before backpressure stalls the fetcher.
    const PARALLEL_CHUNK_CHANNEL_SIZE: usize = 8;

    /// Parallel chunked ByteStream read. Splits the byte range into
    /// `parallel_chunk_count` sub-ranges, issues concurrent Read RPCs,
    /// and streams data to the writer in order via bounded per-chunk
    /// channels. Peak memory is bounded to approximately
    /// `chunk_count × channel_size × frame_size` (~32 MiB for 4 chunks)
    /// regardless of total blob size.
    ///
    /// `total_length` is the post-offset byte count the caller wants
    /// returned (already clamped to fit within the blob by the caller
    /// at `get_part`). `blob_size` is the FULL declared blob size from
    /// `digest.size_bytes()` and is used solely to bound `chunk_count`
    /// so the splitter never produces a chunk that lies past EOF — the
    /// distinction matters when a caller passes a small `length` from
    /// a large blob (only `blob_size` is the right ceiling for chunk
    /// math; `total_length` could be much smaller than the blob).
    async fn get_part_parallel(
        &self,
        resource_name: &str,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        total_length: u64,
        blob_size: u64,
    ) -> Result<(), Error> {
        // Bug B clamp: never request more chunks than the blob has
        // bytes when split into `parallel_chunk_read_threshold`-sized
        // pieces. Without this, a caller that passes a too-large
        // `length` (e.g. the audited 10 MiB caller-supplied limit on
        // a 183-byte blob) would split the request into N chunks all
        // pointing past EOF except chunk 0. The peer correctly EOFs
        // those chunks immediately; the post-`CleanShort` reader
        // recovers the data, but the splitter still issues N-1
        // wasted RPCs per read and creates contention fodder for
        // race-loser-abort h2 RST_STREAM bursts (#147 producer side)
        // and the "Tried to send while stream is closed" wedge shape
        // observed in production logs on 2026-04-25.
        //
        // Formula (per audit fix shape): cap `chunk_count` so the
        // splitter never produces a chunk that lies entirely past the
        // blob's last byte. We use `parallel_chunk_read_threshold`
        // as the natural minimum chunk size — the same number that
        // gates the parallel-vs-single-stream decision at the call
        // site, so a blob smaller than the threshold collapses to
        // chunk_count=1 (and would normally have stayed on the
        // single-stream path; this is defense in depth).
        let blob_remaining = blob_size.saturating_sub(offset);
        let max_useful_chunks = blob_remaining
            .div_ceil(self.parallel_chunk_read_threshold.max(1))
            .max(1);
        let chunk_count = self.parallel_chunk_count.min(max_useful_chunks);
        let base_chunk_size = total_length / chunk_count;
        let remainder = total_length % chunk_count;
        let read_start = std::time::Instant::now();
        info!(
            %resource_name,
            offset,
            total_length,
            blob_size,
            chunk_count,
            requested_chunk_count = self.parallel_chunk_count,
            "GrpcStore::get_part_parallel entry",
        );

        // Build chunk descriptors: (chunk_offset, chunk_length).
        let mut chunks: Vec<(u64, u64)> = Vec::with_capacity(chunk_count as usize);
        let mut current_offset = offset;
        for i in 0..chunk_count {
            let this_chunk = base_chunk_size + if i < remainder { 1 } else { 0 };
            if this_chunk == 0 {
                break;
            }
            chunks.push((current_offset, this_chunk));
            current_offset += this_chunk;
        }

        let actual_chunk_count = chunks.len();

        // #7 Fix-C: per-request consecutive-latched-pool-fail counter.
        // Shared (Arc) across all chunk retriers so that N chunks
        // instant-failing simultaneously counts as ONE latched signal,
        // not N independent signals.  Per-chunk counters would require
        // N × LATCHED_POOL_ABORT_THRESHOLD attempts before aborting —
        // on a fully-latched 32-channel pool, chunks abort in parallel
        // so the shared counter reaches the threshold in 2 total
        // attempts regardless of chunk_count.
        //
        // NOTE: resets on ANY successful channel acquisition (not just the
        // chunk that previously incremented it).  In a mixed-health pool,
        // one healthy chunk completing its first acquire resets the counter
        // to 0, preventing a spurious abort.  This is the intended
        // reset-wins behavior: if ANY chunk can acquire a healthy channel,
        // the pool is not fully latched.
        let parallel_latched_fails = Arc::new(AtomicU32::new(0));

        // Create a bounded channel per chunk. Fetch tasks push data
        // into their channel as it arrives from the gRPC stream;
        // the writer drains channels sequentially (ch0 then ch1 …).
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..actual_chunk_count)
            .map(|_| tokio::sync::mpsc::channel::<Bytes>(Self::PARALLEL_CHUNK_CHANNEL_SIZE))
            .unzip();

        // Fetch future: drives all chunk reads concurrently.
        // Each fetch streams data into its bounded channel.
        // On error, try_for_each short-circuits and drops remaining
        // futures (and their senders), which unblocks the writer.
        let fetch_all = {
            let fetches: FuturesUnordered<_> = chunks
                .into_iter()
                .zip(senders)
                .enumerate()
                .map(
                    |(idx, ((chunk_offset, chunk_length), tx))| {
                        let resource_name = resource_name.to_string();
                        let parallel_latched_fails =
                            parallel_latched_fails.clone();
                        async move {
                            // Per-chunk early-EOF retry: when the
                            // server's stream yields `None` (or an
                            // empty message) before delivering the
                            // full `chunk_length`, retry the residual
                            // range
                            // `[chunk_offset + bytes_received,
                            //   chunk_offset + chunk_length)` through
                            // `self.retrier`. This mirrors the
                            // resilience that `get_part_single_stream`
                            // provides for non-parallel reads, and
                            // converts a one-shot `DataLoss` into a
                            // transient, recoverable condition.
                            // Without this retry, a single network
                            // glitch (h2 RST_STREAM, server-side `tx`
                            // drop without a Status frame, or
                            // transient store hiccup) caused the
                            // entire `get_part_parallel` call to fail,
                            // which the upstream worker's
                            // `prepare_action_inputs` cannot recover
                            // from mid-tree-walk.
                            //
                            // Production symptom (worker logs):
                            //   parallel read chunk N: expected X
                            //   bytes but got 0 :
                            //   Sender dropped before sending EOF
                            struct ChunkState {
                                bytes_received: u64,
                                attempt: u32,
                            }
                            let initial_state = ChunkState {
                                bytes_received: 0,
                                attempt: 0,
                            };
                            // Mirror `state.attempt` outside the
                            // unfold so the outer code can classify
                            // the per-chunk retry outcome
                            // (succeeded / failed) for the
                            // `parallel_chunk_retries_*` metrics.
                            // We can't read `state` after retrier
                            // completion because the unfold consumes
                            // it.
                            let attempt_counter =
                                Arc::new(AtomicU32::new(0));
                            let attempt_counter_inner =
                                attempt_counter.clone();

                            let result = self.retrier
                                .retry(unfold(
                                    initial_state,
                                    |mut state| {
                                        let resource_name =
                                            resource_name.clone();
                                        let tx = tx.clone();
                                        let attempt_counter =
                                            attempt_counter_inner
                                                .clone();
                                        let parallel_latched_fails =
                                            parallel_latched_fails
                                                .clone();
                                        async move {
                                            state.attempt += 1;
                                            attempt_counter.store(
                                                state.attempt,
                                                Ordering::Relaxed,
                                            );
                                            let resume_offset =
                                                chunk_offset
                                                    + state
                                                        .bytes_received;
                                            let resume_limit =
                                                chunk_length
                                                    - state
                                                        .bytes_received;
                                            let read_offset_i64 =
                                                match i64::try_from(
                                                    resume_offset,
                                                ) {
                                                    Ok(v) => v,
                                                    Err(_) => {
                                                        return Some((
                                                            RetryResult::Err(make_err!(
                                                                Code::InvalidArgument,
                                                                "chunk {idx}: could not convert resume offset {resume_offset} to i64"
                                                            )),
                                                            state,
                                                        ));
                                                    }
                                                };
                                            let read_limit_i64 =
                                                match i64::try_from(
                                                    resume_limit,
                                                ) {
                                                    Ok(v) => v,
                                                    Err(_) => {
                                                        return Some((
                                                            RetryResult::Err(make_err!(
                                                                Code::InvalidArgument,
                                                                "chunk {idx}: could not convert resume limit {resume_limit} to i64"
                                                            )),
                                                            state,
                                                        ));
                                                    }
                                                };
                                            let request = ReadRequest {
                                                resource_name,
                                                read_offset: read_offset_i64,
                                                read_limit: read_limit_i64,
                                            };
                                            let (mut stream, _chunk_channel_id) = match self
                                                .read_internal(
                                                    request, true,
                                                )
                                                .await
                                            {
                                                Ok((stream, id, _post_conn)) => {
                                                    // Successful channel acquisition → pool not latched;
                                                    // reset the shared counter so a healthy attempt
                                                    // after a transient failure does not carry stale count.
                                                    parallel_latched_fails.store(0, Ordering::Relaxed);
                                                    (stream, id)
                                                }
                                                Err((err, _, post_conn)) => {
                                                    // #147: same as single-stream path.
                                                    // #7 Fix-C: check for latched-pool signature.
                                                    // `post_conn` is set inside read_internal right
                                                    // after channel acquisition (K2 fix) so elapsed
                                                    // excludes the connection-acquire queue wait.
                                                    let attempt_elapsed_ms =
                                                        post_conn.elapsed().as_millis() as u64;
                                                    self.evict_pool_on_transport_err(&err);
                                                    // #FU QUIC read-failure diagnostic
                                                    // (observability-only): record the
                                                    // latch-vs-backpressure split.
                                                    // `prefer_tcp = true` here (parallel),
                                                    // so under Dual this read went over
                                                    // TCP and is NOT counted as a QUIC
                                                    // read; under Quic-only it IS counted.
                                                    // No control-flow effect — reuses
                                                    // `attempt_elapsed_ms`.
                                                    self.record_quic_read_fail(&err, attempt_elapsed_ms, true);
                                                    if looks_like_latched_pool(&err, attempt_elapsed_ms) {
                                                        // Atomic add; another chunk racing here may
                                                        // also increment — first one to reach the
                                                        // threshold wins and both abort.
                                                        let fails = parallel_latched_fails
                                                            .fetch_add(1, Ordering::Relaxed)
                                                            + 1;
                                                        if fails >= LATCHED_POOL_ABORT_THRESHOLD {
                                                            warn!(
                                                                chunk_idx = idx,
                                                                attempt = state.attempt,
                                                                consecutive_latched_fails = fails,
                                                                "GrpcStore: aborting parallel retries — \
                                                                 pool is systemically latched (all \
                                                                 chunk attempts are instant-fail \
                                                                 tower-buffer ServiceError); \
                                                                 callers fall back instead of burning \
                                                                 the 30s notify timeout (#7 Fix-C)"
                                                            );
                                                            return Some((RetryResult::Err(err), state));
                                                        }
                                                    } else {
                                                        // Non-latched failure: reset consecutive
                                                        // counter so a single coincidentally-fast
                                                        // transient does not accumulate.
                                                        parallel_latched_fails
                                                            .store(0, Ordering::Relaxed);
                                                    }
                                                    return Some((
                                                        RetryResult::Retry(err.append(format!(
                                                            "in GrpcStore::get_part_parallel chunk {idx} (attempt {})",
                                                            state.attempt
                                                        ))),
                                                        state,
                                                    ));
                                                }
                                            };

                                            // Per-attempt counter: if
                                            // this attempt produced no
                                            // bytes, the next retry
                                            // requests the same range
                                            // again (state untouched);
                                            // if it produced some
                                            // bytes, the next retry
                                            // resumes at the new
                                            // offset.
                                            let mut bytes_this_attempt: u64 = 0;
                                            // Track HOW the loop exited so
                                            // the post-loop classifier
                                            // can distinguish a clean
                                            // `Status::OK` trailer
                                            // (`stream.next() == None`,
                                            // `clean_eof == true`) from
                                            // an empty-data-frame early
                                            // break (`clean_eof == false`).
                                            // tonic's `Streaming` returns
                                            // `Some(Err(status))` for any
                                            // non-OK trailer and `None`
                                            // only for `Status::OK` — so
                                            // a `None` here is unambiguously
                                            // the peer signalling
                                            // "resource exhausted at this
                                            // offset".
                                            let mut clean_eof = false;
                                            loop {
                                                match stream.next().await {
                                                    None => {
                                                        clean_eof = true;
                                                        break;
                                                    }
                                                    Some(Ok(message)) => {
                                                        if message.data.is_empty() {
                                                            break;
                                                        }
                                                        let n = message.data.len() as u64;
                                                        bytes_this_attempt += n;
                                                        state.bytes_received += n;
                                                        if tx.send(message.data).await.is_err() {
                                                            // Writer
                                                            // (output) dropped — terminal,
                                                            // do not retry.
                                                            return Some((
                                                                RetryResult::Err(make_err!(
                                                                    Code::Internal,
                                                                    "parallel read chunk {idx}: writer dropped receiver"
                                                                )),
                                                                state,
                                                            ));
                                                        }
                                                    }
                                                    Some(Err(status)) => {
                                                        // Trailer is a
                                                        // non-OK gRPC
                                                        // status. `NotFound`
                                                        // is terminal — the
                                                        // peer's blob is
                                                        // gone, retrying
                                                        // the same residual
                                                        // range will keep
                                                        // hitting the same
                                                        // 404. Surface as
                                                        // `RetryResult::Err`
                                                        // so the locality-
                                                        // eviction path in
                                                        // `WorkerProxyStore`
                                                        // sees the real
                                                        // code and clears
                                                        // the stale entry.
                                                        // Other codes
                                                        // (Unavailable,
                                                        // Internal, Aborted,
                                                        // …) are typically
                                                        // transient — let
                                                        // the retrier
                                                        // decide.
                                                        let code = status.code();
                                                        let err = Into::<Error>::into(status).append(format!(
                                                            "chunk {idx} at offset {resume_offset} (attempt {})",
                                                            state.attempt
                                                        ));
                                                        if code == Code::NotFound {
                                                            return Some((
                                                                RetryResult::Err(err),
                                                                state,
                                                            ));
                                                        }
                                                        // #147: streaming-body err.
                                                        self.evict_pool_on_transport_err(&err);
                                                        return Some((
                                                            RetryResult::Retry(err),
                                                            state,
                                                        ));
                                                    }
                                                }
                                            }

                                            // Classify the per-attempt
                                            // outcome via the pure helper
                                            // (testable in isolation):
                                            //   Complete            → Ok
                                            //   CleanShort          → Ok (Bug B fix)
                                            //   AmbiguousEarlyBreak → Retry(DataLoss)
                                            //
                                            // Bug B forensic context: a
                                            // 183-byte blob fetched via the
                                            // parallel path used to land
                                            // here as `bytes_received < chunk_length`
                                            // with `clean_eof = true`,
                                            // misclassified as DataLoss,
                                            // and looped infinitely on
                                            // digest 1e08eefa…-183 in
                                            // production. The peer's
                                            // bytestream upload-skip fast
                                            // path also saw `has()` return
                                            // true (locality lying because
                                            // eviction only ran on
                                            // `Code::NotFound`), so Bazel's
                                            // re-uploads were silently
                                            // dropped and the loop
                                            // continued forever.
                                            match classify_chunk_attempt(
                                                clean_eof,
                                                state.bytes_received,
                                                chunk_length,
                                            ) {
                                                ChunkAttemptOutcome::Complete
                                                | ChunkAttemptOutcome::CleanShort => {
                                                    return Some((
                                                        RetryResult::Ok(()),
                                                        state,
                                                    ));
                                                }
                                                ChunkAttemptOutcome::AmbiguousEarlyBreak => {}
                                            }
                                            warn!(
                                                chunk_idx = idx,
                                                resume_offset,
                                                resume_limit,
                                                bytes_this_attempt,
                                                bytes_received_total = state.bytes_received,
                                                expected = chunk_length,
                                                attempt = state.attempt,
                                                "parallel read chunk: stream ended early without Status trailer, will retry residual range"
                                            );
                                            Some((
                                                RetryResult::Retry(make_err!(
                                                    Code::DataLoss,
                                                    "parallel read chunk {idx}: stream ended early on attempt {} \
                                                     (got {bytes_this_attempt} this attempt, {} of {chunk_length} total)",
                                                    state.attempt,
                                                    state.bytes_received
                                                )),
                                                state,
                                            ))
                                        }
                                    },
                                ))
                                .await
                                .err_tip(|| {
                                    format!(
                                        "in GrpcStore::get_part_parallel chunk {idx}"
                                    )
                                });

                            // Classify the per-chunk retry outcome
                            // for the `parallel_chunk_retries_*`
                            // counters. We only count chunks where
                            // at least one retry attempt happened
                            // (`final_attempts > 1`); a first-try
                            // success or first-try permanent error
                            // (no retry attempted) does not bump
                            // either counter. `succeeded` means the
                            // retry recovered the chunk; `failed`
                            // means the retrier exhausted attempts
                            // (or the next attempt hit a
                            // non-retryable error).
                            let final_attempts = attempt_counter
                                .load(Ordering::Relaxed);
                            if final_attempts > 1 {
                                if result.is_ok() {
                                    self.parallel_chunk_retries_succeeded
                                        .fetch_add(1, Ordering::Relaxed);
                                } else {
                                    self.parallel_chunk_retries_failed
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            result
                        }
                    },
                )
                .collect();
            fetches.try_for_each(|()| future::ready(Ok(())))
        };

        // Writer future: drains channels in chunk order → output.
        // When a sender drops (fetch done or errored), recv()
        // returns None and we advance to the next channel.
        let write_all = async {
            let mut total_bytes: u64 = 0;
            for mut rx in receivers {
                while let Some(data) = rx.recv().await {
                    total_bytes += data.len() as u64;
                    writer
                        .send(data)
                        .await
                        .err_tip(|| "while writing parallel chunk data")?;
                }
            }
            Result::<u64, Error>::Ok(total_bytes)
        };

        let (fetch_result, write_result) = tokio::join!(fetch_all, write_all);
        // Check both — fetch errors take priority since they indicate
        // upstream data issues; write errors indicate downstream
        // backpressure or client disconnect.
        fetch_result.err_tip(|| "in GrpcStore::get_part_parallel fetch")?;
        let total_bytes = write_result.err_tip(|| "in GrpcStore::get_part_parallel write")?;

        writer
            .send_eof()
            .err_tip(|| "could not send eof in get_part_parallel")?;

        let elapsed = read_start.elapsed();
        let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
            (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
        } else {
            0.0
        };
        info!(
            %total_bytes,
            chunks = actual_chunk_count,
            elapsed_ms = elapsed.as_millis() as u64,
            throughput_mbps = format!("{throughput_mbps:.1}"),
            "parallel chunked ByteStream read complete"
        );

        Ok(())
    }

    /// #212 Phase 2.4: dispatch a single CAS blob via the
    /// worker→server `WorkerApi/WriteChunked` RPC. Acquires one
    /// fresh transport channel, then delegates to
    /// `chunked::chunked_client::write_chunked_stream`.
    ///
    /// Caller (`update`) is responsible for the size-and-kill-switch
    /// gate; this method ASSUMES it should run.
    ///
    /// On Dual transport, picks the TCP leg (matches the existing
    /// `GrpcStore::write` decision: large streaming writes prefer
    /// TCP per the 1.1× speed advantage measured in the original
    /// dual-transport benchmark).
    ///
    /// **Per-attempt transport acquisition.** The chunked-client
    /// retry loop calls `dispatcher.dispatch` once per retry. Each
    /// `dispatch` invocation runs the channel-acquisition factory
    /// to get a fresh transport — for TCP this re-enters
    /// `ConnectionManager::connection`, which rotates among healthy
    /// slots so a per-attempt transport failure does not pin the
    /// same dead channel for attempt N+1.
    ///
    /// The factory captures a `&'static`-equivalent reference to
    /// the GrpcStore via the `&Arc<Self>` indirection threaded from
    /// `update`'s `Pin<&Self>` (recovered via the manually-built
    /// `Pin<&Self>` cast — sound because the lifetime of the dispatcher
    /// is bounded by this `await`).
    #[cfg(feature = "chunked_fast_slow")]
    async fn update_via_chunked_inner(
        &self,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
    ) -> Result<(), Error> {
        use crate::chunked::chunked_client::{
            ChunkedClientOptions, WorkerApiWriteChunkedDispatcher,
            WorkerApiWriteChunkedV2Dispatcher, WriteChunkedDispatcher, write_chunked_stream,
        };

        let options: ChunkedClientOptions = grpc_store_chunked_client_options();
        let metrics = Arc::clone(&self.chunked_metrics);

        // Build a per-call dispatcher whose factory captures the
        // SHARED transport. For TCP / Dual we hand the factory a
        // pointer to the stable `ConnectionManager` (`&'self`,
        // promoted to `'static` via the `'static` bound on the
        // factory); the inner `acquire_write_channel` await yields
        // an owned `Connection` that lives only for the single
        // attempt. For QUIC the factory just clones the shared
        // `QuicChannel`.
        //
        // SAFETY: the factory closure stores raw pointers to
        // self.transport's internals in disguise via the closure;
        // `write_chunked_stream` returns BEFORE the `&self` borrow
        // ends (the `await` boundary holds the borrow), so the
        // factory's pointer is always valid when called.
        let result: Result<u64, Error> = match &self.transport {
            Transport::Tcp(cm) => {
                // Fresh-Arc-per-call: clone the manager handle so the
                // factory closure can be `'static`. ConnectionManager
                // is internally Arc-wrapped, so cloning is cheap.
                let cm_clone = cm.clone();
                let acquire_timeout_ms = self.connection_acquire_timeout_ms;
                let v2 = self.chunked_v2_writes_enabled.load(Ordering::Relaxed);
                let dispatcher: Box<dyn WriteChunkedDispatcher> = if v2 {
                    Box::new(WorkerApiWriteChunkedV2Dispatcher::with_factory(move || {
                        let cm = cm_clone.clone();
                        Box::pin(async move {
                            match acquire_timeout_ms {
                                Some(ms) => {
                                    cm.connection_with_timeout(
                                        "worker_api_write_chunked".to_string(),
                                        Duration::from_millis(ms),
                                    )
                                    .await
                                }
                                None => cm.connection("worker_api_write_chunked".to_string()).await,
                            }
                            .err_tip(|| "in GrpcStore::update_via_chunked_inner (tcp)")
                        })
                    }))
                } else {
                    Box::new(WorkerApiWriteChunkedDispatcher::with_factory(move || {
                        let cm = cm_clone.clone();
                        Box::pin(async move {
                            match acquire_timeout_ms {
                                Some(ms) => {
                                    cm.connection_with_timeout(
                                        "worker_api_write_chunked".to_string(),
                                        Duration::from_millis(ms),
                                    )
                                    .await
                                }
                                None => cm.connection("worker_api_write_chunked".to_string()).await,
                            }
                            .err_tip(|| "in GrpcStore::update_via_chunked_inner (tcp)")
                        })
                    }))
                };
                write_chunked_stream(&*dispatcher, digest, reader, options, metrics).await
            }
            #[cfg(feature = "quic")]
            Transport::Quic(ch) => {
                let ch = ch.clone();
                let v2 = self.chunked_v2_writes_enabled.load(Ordering::Relaxed);
                let dispatcher: Box<dyn WriteChunkedDispatcher> = if v2 {
                    Box::new(WorkerApiWriteChunkedV2Dispatcher::with_factory(move || {
                        let ch = ch.clone();
                        Box::pin(async move { Ok(ch) })
                    }))
                } else {
                    Box::new(WorkerApiWriteChunkedDispatcher::with_factory(move || {
                        let ch = ch.clone();
                        Box::pin(async move { Ok(ch) })
                    }))
                };
                write_chunked_stream(&*dispatcher, digest, reader, options, metrics).await
            }
            #[cfg(feature = "quic")]
            Transport::Dual { tcp, .. } => {
                let cm_clone = tcp.clone();
                let acquire_timeout_ms = self.connection_acquire_timeout_ms;
                let v2 = self.chunked_v2_writes_enabled.load(Ordering::Relaxed);
                let dispatcher: Box<dyn WriteChunkedDispatcher> = if v2 {
                    Box::new(WorkerApiWriteChunkedV2Dispatcher::with_factory(move || {
                        let cm = cm_clone.clone();
                        Box::pin(async move {
                            match acquire_timeout_ms {
                                Some(ms) => {
                                    cm.connection_with_timeout(
                                        "worker_api_write_chunked".to_string(),
                                        Duration::from_millis(ms),
                                    )
                                    .await
                                }
                                None => cm.connection("worker_api_write_chunked".to_string()).await,
                            }
                            .err_tip(|| "in GrpcStore::update_via_chunked_inner (dual/tcp)")
                        })
                    }))
                } else {
                    Box::new(WorkerApiWriteChunkedDispatcher::with_factory(move || {
                        let cm = cm_clone.clone();
                        Box::pin(async move {
                            match acquire_timeout_ms {
                                Some(ms) => {
                                    cm.connection_with_timeout(
                                        "worker_api_write_chunked".to_string(),
                                        Duration::from_millis(ms),
                                    )
                                    .await
                                }
                                None => cm.connection("worker_api_write_chunked".to_string()).await,
                            }
                            .err_tip(|| "in GrpcStore::update_via_chunked_inner (dual/tcp)")
                        })
                    }))
                };
                write_chunked_stream(&*dispatcher, digest, reader, options, metrics).await
            }
        };
        // #212 Phase 2.4 fixup B2 (perf-optimizer): mirror the #147
        // pool-eviction path that the legacy ByteStream `write` /
        // `get_part` paths run on transport-shaped errors. Without
        // this, a chunked-stream attempt that fails with a GOAWAY-
        // shaped error leaves the dead channel sitting in the
        // `ConnectionManager` pool; the next request picks it up and
        // immediately re-fails. The classifier (`looks_like_dead_channel`
        // inside `evict_pool_on_transport_err`) is the same one the
        // legacy paths use, so there is no risk of evicting a
        // BackpressureSignal-carrying ResourceExhausted (which is
        // explicitly NOT classified as dead-channel — see the
        // unit test `bare_resource_exhausted_looks_like_dead_channel`).
        if let Err(ref err) = result {
            self.evict_pool_on_transport_err(err);
        }
        // #62 Phase 2 instrumentation: per #56 RCA candidate (a),
        // capture committed_size reported by `write_chunked_stream`
        // vs expected_size from the digest, with explicit
        // truncation arm. The (Ok, Err) arm in
        // `FastSlowStore::stream_file_to_store` (#59) shows
        // `update_via_chunked_inner` returning Ok while the producer
        // is still streaming; this log answers WHETHER the chunked
        // dispatcher returned Ok with `committed_bytes < expected`
        // (early-Ok) or with `committed_bytes == expected`
        // (well-formed Ok, indicating the producer-side `send_error`
        // surfaces _after_ EOF was already accepted upstream).
        let expected_size = digest.size_bytes();
        match &result {
            Ok(committed_bytes) => {
                let delta = expected_size as i64 - *committed_bytes as i64;
                if delta == 0 {
                    debug!(
                        %digest,
                        committed_bytes = *committed_bytes,
                        expected_size,
                        delta,
                        arm_name = "chunked_inner_ok",
                        "#62 GrpcStore::update_via_chunked_inner: returned Ok at expected size",
                    );
                } else {
                    info!(
                        %digest,
                        committed_bytes = *committed_bytes,
                        expected_size,
                        delta,
                        arm_name = "chunked_inner_truncated",
                        "#62 GrpcStore::update_via_chunked_inner: returned Ok with committed < expected (early-Ok candidate)",
                    );
                }
            }
            Err(err) => {
                info!(
                    %digest,
                    expected_size,
                    ?err,
                    arm_name = "chunked_inner_err",
                    "#62 GrpcStore::update_via_chunked_inner: returned Err",
                );
            }
        }
        result.err_tip(|| format!("in GrpcStore::update_via_chunked_inner for digest {digest}"))?;
        Ok(())
    }
}

#[async_trait]
impl StoreDriver for GrpcStore {
    // NOTE: This function can only be safely used on CAS stores. AC stores may return a size that
    // is incorrect.
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        if matches!(self.store_type, nativelink_config::stores::StoreType::Ac) {
            keys.iter()
                .zip(results.iter_mut())
                .map(|(key, result)| async move {
                    // The length of an AC is incorrect, so we don't figure out the
                    // length, instead the biggest possible result is returned in the
                    // hope that we detect incorrect usage.
                    self.get_action_result_from_digest(key.borrow().into_digest())
                        .await?;
                    *result = Some(u64::MAX);
                    Ok::<_, Error>(())
                })
                .collect::<FuturesUnordered<_>>()
                .try_for_each(|()| future::ready(Ok(())))
                .await
                .err_tip(|| "Getting upstream action cache entry")?;
            return Ok(());
        }

        let missing_blobs_response = self
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: self.instance_name.clone(),
                blob_digests: keys
                    .iter()
                    .map(|k| k.borrow().into_digest().into())
                    .collect(),
                digest_function: Context::current()
                    .get::<DigestHasherFunc>()
                    .map_or_else(default_digest_hasher_func, |v| *v)
                    .proto_digest_func()
                    .into(),
            }))
            .await?
            .into_inner();

        // Since the ordering is not guaranteed above, the matching has to check
        // all missing blobs against all entries in the unsorted digest list.
        // To optimise this, the missing digests are sorted and then it is
        // efficient to perform a binary search for each digest within the
        // missing list.
        let mut missing_digests =
            Vec::with_capacity(missing_blobs_response.missing_blob_digests.len());
        for missing_digest in missing_blobs_response.missing_blob_digests {
            missing_digests.push(DigestInfo::try_from(missing_digest)?);
        }
        missing_digests.sort_unstable();
        for (digest, result) in keys
            .iter()
            .map(|v| v.borrow().into_digest())
            .zip(results.iter_mut())
        {
            match missing_digests.binary_search(&digest) {
                Ok(_) => *result = None,
                Err(_) => *result = Some(digest.size_bytes()),
            }
        }

        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        struct LocalState {
            resource_name: String,
            reader: DropCloserReadHalf,
            did_error: bool,
            bytes_received: i64,
        }

        let digest = key.into_digest();
        if matches!(self.store_type, nativelink_config::stores::StoreType::Ac) {
            return self.update_action_result_from_bytes(digest, reader).await;
        }

        // #212 Phase 2.4 dispatch: when (a) the `chunked_fast_slow`
        // feature is compiled AND (b) the runtime kill-switch is
        // flipped on AND (c) the blob is at least `CHUNK_SIZE` bytes,
        // route to the worker→server `WriteChunked` RPC. Smaller
        // blobs OR the kill-switch off OR the feature absent ⇒
        // legacy in-order ByteStream Write path (the "Fallback"
        // section below).
        //
        // The retry loop inside `write_chunked_stream` re-acquires a
        // fresh transport per attempt via the dispatcher's
        // `acquire_channel` factory (`ConnectionManager::connection()`
        // for TCP, `Channel::clone()` for QUIC) — `Clone` on
        // `ConnectionManager` (added for #212 Phase 2.4) makes this
        // work without threading an `Arc<Self>` through the trait
        // method.
        //
        // The upper bound (`MAX_CHUNKED_BLOB_SIZE`) is the v1 safety
        // cap: the chunked client buffers the entire payload in memory
        // up-front so retries can resend from a single-pass
        // `DropCloserReadHalf`. Without this cap a multi-GB blob would
        // peak the worker's RSS at O(blob_size). Streaming retry that
        // would let us lift the cap is deferred to Phase 2.5+.
        #[cfg(feature = "chunked_fast_slow")]
        if self.chunked_writes_enabled.load(Ordering::Relaxed)
            && digest.size_bytes() >= crate::chunked::CHUNK_SIZE as u64
            && digest.size_bytes() <= crate::chunked::MAX_CHUNKED_BLOB_SIZE
        {
            return self.update_via_chunked_inner(digest, reader).await;
        }

        let digest_function = Context::current()
            .get::<DigestHasherFunc>()
            .map_or_else(default_digest_hasher_func, |v| *v)
            .proto_digest_func()
            .as_str_name()
            .to_ascii_lowercase();

        let mut buf = Uuid::encode_buffer();
        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}/{}",
            &self.instance_name,
            Uuid::new_v4().hyphenated().encode_lower(&mut buf),
            digest_function,
            digest.packed_hash(),
            digest.size_bytes(),
        );
        trace!(
            resource_name = %resource_name,
            digest_hash = %digest.packed_hash(),
            digest_size = digest.size_bytes(),
            "GrpcStore::update: starting upload for digest",
        );
        let local_state = LocalState {
            resource_name,
            reader,
            did_error: false,
            bytes_received: 0,
        };

        let stream = Box::pin(unfold(local_state, |mut local_state| async move {
            if local_state.did_error {
                error!("GrpcStore::update() polled stream after error was returned");
                return None;
            }
            let data = match local_state
                .reader
                .recv()
                .await
                .err_tip(|| "In GrpcStore::update()")
            {
                Ok(data) => data,
                Err(err) => {
                    local_state.did_error = true;
                    return Some((Err(err), local_state));
                }
            };

            let write_offset = local_state.bytes_received;
            local_state.bytes_received += data.len() as i64;

            // Per the RE API spec, only the first WriteRequest needs the
            // resource_name; subsequent messages use an empty string.
            let resource_name = if write_offset == 0 {
                local_state.resource_name.clone()
            } else {
                String::new()
            };

            Some((
                Ok(WriteRequest {
                    resource_name,
                    write_offset,
                    finish_write: data.is_empty(), // EOF is when no data was polled.
                    data,
                }),
                local_state,
            ))
        }));

        self.write(
            WriteRequestStreamWrapper::from(stream)
                .await
                .err_tip(|| "in GrpcStore::update()")?,
        )
        .await
        .err_tip(|| "in GrpcStore::update()")?;

        Ok(())
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        // Route small CAS blobs through BatchUpdateBlobs.
        if !matches!(self.store_type, nativelink_config::stores::StoreType::Ac)
            && self.batch_update_threshold > 0
            && (data.len() as u64) <= self.batch_update_threshold
        {
            let digest = key.into_digest();

            if let Some(tx) = &self.batch_tx {
                // Queue for the background batch flush loop.
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(PendingBatchEntry {
                    digest,
                    data,
                    result_tx,
                })
                .map_err(|_| make_input_err!("Batch flush channel closed"))?;
                return result_rx
                    .await
                    .map_err(|_| make_input_err!("Batch flush loop dropped"))?;
            }

            // Fallback: immediate single-element BatchUpdateBlobs (no batch loop).
            let digests = [digest];
            let mut results = self.do_batch_update(&digests, vec![(digest, data)]).await;
            return results.remove(&digest).unwrap_or_else(|| {
                Err(make_input_err!("BatchUpdateBlobs: no response for digest"))
            });
        }

        // Fallback: standard ByteStream.Write via channel pair.
        let (mut tx, rx) = make_buf_channel_pair();
        let data_len =
            u64::try_from(data.len()).err_tip(|| "Could not convert data.len() to u64")?;
        let send_fut = async move {
            if !data.is_empty() {
                tx.send(data)
                    .await
                    .err_tip(|| "Failed to write data in update_oneshot")?;
            }
            tx.send_eof()
                .err_tip(|| "Failed to write EOF in update_oneshot")?;
            Ok(())
        };
        future::try_join(
            send_fut,
            self.update(key, rx, UploadSizeInfo::ExactSize(data_len)),
        )
        .await?;
        Ok(())
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        if optimization == StoreOptimizations::LazyExistenceOnSync
            && !matches!(self.store_type, nativelink_config::stores::StoreType::Ac)
        {
            return true;
        }
        optimization == StoreOptimizations::SubscribesToUpdateOneshot
            && self.batch_update_threshold > 0
            && !matches!(self.store_type, nativelink_config::stores::StoreType::Ac)
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let digest = key.into_digest();
        if matches!(self.store_type, nativelink_config::stores::StoreType::Ac) {
            let offset = usize::try_from(offset).err_tip(|| "Could not convert offset to usize")?;
            let length = length
                .map(|v| usize::try_from(v).err_tip(|| "Could not convert length to usize"))
                .transpose()?;

            return self
                .get_action_result_as_part(digest, writer, offset, length)
                .await;
        }

        // Shortcut for empty blobs.
        if digest.size_bytes() == 0 {
            return writer.send_eof();
        }

        let digest_function = Context::current()
            .get::<DigestHasherFunc>()
            .map_or_else(default_digest_hasher_func, |v| *v)
            .proto_digest_func()
            .as_str_name()
            .to_ascii_lowercase();

        let resource_name = format!(
            "{}/blobs/{}/{}/{}",
            &self.instance_name,
            digest_function,
            digest.packed_hash(),
            digest.size_bytes(),
        );

        // Determine the effective read length for parallel chunking.
        // Bug B (audit 2026-04-25): production callers occasionally
        // pass an over-large `length` (e.g. `Some(10 MiB)` for a
        // 183-byte blob — the value of the bytestream `read_limit`
        // forwarded from a Bazel client RPC). The pre-fix code took
        // that value at face value, exceeded the
        // `parallel_chunk_read_threshold` (8 MiB by default), and
        // shredded a 183-byte read into 8 parallel chunk RPCs all
        // pointing past EOF except chunk 0. Clamp to the actual
        // remaining bytes — `length` was always semantically "at most
        // N bytes from this offset" — so a tiny blob stays on the
        // single-stream path regardless of what the caller asked for.
        let blob_remaining = digest.size_bytes().saturating_sub(offset);
        let effective_length = length.unwrap_or(blob_remaining).min(blob_remaining);

        // Use parallel chunked reads for large blobs.
        if self.parallel_chunk_read_threshold > 0
            && effective_length >= self.parallel_chunk_read_threshold
            && self.parallel_chunk_count > 1
        {
            return self
                .get_part_parallel(
                    &resource_name,
                    writer,
                    offset,
                    effective_length,
                    digest.size_bytes(),
                )
                .await;
        }

        // Single-stream path for small blobs or when parallel reads
        // are disabled. Forward the clamped `effective_length` (not
        // the raw caller-supplied `length`) for symmetry with the
        // parallel branch above. REAPI ByteStream `read_limit` is
        // server-side clamped, so passing the un-clamped value would
        // be harmless on the wire today; tightening here prevents a
        // future refactor that uses `length` for client-side
        // allocation/timing from inheriting the un-clamped value.
        self.get_part_single_stream(resource_name, writer, offset, Some(effective_length))
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Err(Error::new(
            Code::Internal,
            "gRPC stores are incompatible with removal callbacks".to_string(),
        ))
    }

    /// `register_item_callback` rejects the registration with
    /// `Code::Internal` — a gRPC client leaf has no removal-event
    /// semantics on its side. Wrappers consult this flag so they can
    /// route the "no listener installed" condition to a `debug!`
    /// instead of an operator-visible startup `warn!` on every worker
    /// `WORKER_FAST_SLOW_STORE` (whose slow tier is `WorkerProxyStore`
    /// over `GrpcStore`).
    fn supports_removal_callbacks(&self) -> bool {
        false
    }

    /// GrpcStore is a leaf — the remote endpoint owns its own BIS pipeline
    /// (if any). This client side does not produce stable digests. Treat
    /// as a leaf with empty drains.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// GrpcStore is a leaf — pin requests do not propagate over the wire.
    /// The remote endpoint manages its own eviction. Local pin is a no-op.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// GrpcStore is a leaf — `mark_stable` is a no-op (the remote
    /// endpoint owns its own BIS pipeline; this client side does not
    /// push into BIS). (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

default_health_status_indicator!(GrpcStore);

#[cfg(test)]
mod tests {
    use nativelink_error::{Code, Error, make_err};

    #[cfg(feature = "quic")]
    use super::{
        QUIC_READ_FAIL_LOG_MIN_INTERVAL_NANOS, QuicReadFailBucket, ReadTransportKind,
        quic_read_fail_bucket, quic_read_fail_should_emit, quic_read_leg,
    };
    use super::{
        ChunkAttemptOutcome, LATCHED_POOL_ABORT_THRESHOLD, LATCHED_POOL_INSTANT_FAIL_MS,
        classify_chunk_attempt, looks_like_dead_channel, looks_like_latched_pool,
    };

    /// #548 Phase 1 Seam 3: the `ChunkedClientOptions` that
    /// `GrpcStore::update_via_chunked_inner` hands to the worker-side
    /// chunked client MUST carry `source = Worker`. Phase 4 (#551) will
    /// branch on this discriminator at the server-side InFlightEntry to
    /// elide the BIS broadcast (the worker is its own durable holder).
    /// A future contributor flipping the literal at the construction site
    /// would silently re-classify worker-originated writes — Phase 4's
    /// BIS-elision would not fire, slow-tier ack would still gate the
    /// worker's pin release, and the #547 phase0 mean-pin-lifetime
    /// optimisation Phase 1 sets up would silently regress.
    ///
    /// Mutation step: change the hardcode at
    /// `grpc_store_chunked_client_options` (`source: Worker` → any other
    /// variant). This test red-fails with the bespoke
    /// "#548 Phase 1 Seam 3: GrpcStore must construct ChunkedClientOptions
    /// with source = Worker" message.
    ///
    /// Note: V1 server-side InFlightEntry independently sets
    /// `source = Worker` at admission (see
    /// `chunked_write_handler.rs:1219`). The two values are derived from
    /// WHICH RPC LANDED, not from any wire field (SECURITY-NOTE on
    /// `ChunkedWriteSource`); this test pins the worker-side half of
    /// that derivation. The server-side half is pinned by the Seam 9
    /// test below in `bazel_facing_internal_chunking_test.rs`.
    #[cfg(feature = "chunked_fast_slow")]
    #[test]
    fn grpc_store_constructs_chunked_client_options_with_source_worker_548_phase1_seam3() {
        use crate::chunked::ChunkedWriteSource;
        use super::grpc_store_chunked_client_options;

        let options = grpc_store_chunked_client_options();
        assert_eq!(
            options.source,
            ChunkedWriteSource::Worker,
            "#548 Phase 1 Seam 3: GrpcStore must construct ChunkedClientOptions \
             with source = Worker — `update_via_chunked_inner` is reached \
             exclusively by remote workers uploading via `GrpcStore::update`. \
             If this fails, the worker-side classifier has been flipped and \
             Phase 4 (#551) BIS-elision will misroute every worker-originated \
             chunked write."
        );
    }

    /// #147 classifier: codes that DO indicate a stale/transport-broken
    /// channel must return true; application-level codes must return false.
    /// `Internal` is special-cased on message content to avoid evicting on
    /// healthy server-app `make_err!(Internal, ...)` errors.
    /// `ResourceExhausted` is special-cased on detail content (#212
    /// §13.1.1 point 2): a `BackpressureSignal` discriminator means the
    /// peer is asserting honest backpressure, NOT a dead channel — must
    /// NOT evict.
    #[test]
    fn looks_like_dead_channel_classifies_codes() {
        // True for the transport-shaped non-Internal codes.
        for code in [Code::Unavailable, Code::Unknown, Code::ResourceExhausted] {
            let err = make_err!(code, "some transport-ish failure");
            assert!(
                looks_like_dead_channel(&err),
                "{code:?} should be classified as transport-shaped"
            );
        }

        // True for the production Internal signature.
        let production_err: Error =
            make_err!(Code::Internal, "Tried to send while stream is closed");
        assert!(
            looks_like_dead_channel(&production_err),
            "the production wedge message must classify as dead channel"
        );

        // True for the other h2-shaped Internal messages we cover.
        for msg in [
            "h2 protocol error: connection error received",
            "buffer's worker closed unexpectedly",
            "broken pipe while sending h2 frame",
            "connection error: the server sent GOAWAY",
        ] {
            let err = make_err!(Code::Internal, "{msg}");
            assert!(
                looks_like_dead_channel(&err),
                "Internal+message {msg:?} should classify as dead channel"
            );
        }

        // FALSE for application-Internal (e.g. server-side make_err!).
        let app_internal = make_err!(Code::Internal, "verify size mismatch in CAS upload");
        assert!(
            !looks_like_dead_channel(&app_internal),
            "Internal with app-level message must NOT evict (false-positive guard)"
        );

        // FALSE for application-level codes.
        for code in [
            Code::NotFound,
            Code::DeadlineExceeded,
            Code::FailedPrecondition,
            Code::Cancelled,
            Code::Aborted,
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::AlreadyExists,
            Code::OutOfRange,
            Code::DataLoss,
            Code::Unimplemented,
        ] {
            let err = make_err!(code, "application-level outcome");
            assert!(
                !looks_like_dead_channel(&err),
                "{code:?} must NOT classify as dead channel"
            );
        }
    }

    /// #212 §13.1.1 point 2: `Code::ResourceExhausted` carrying a
    /// `BackpressureSignal` detail must NOT classify as a dead channel.
    /// If the peer is asserting honest backpressure (per Q8), evicting
    /// the underlying h2 channel reproduces the production #147
    /// stale-channel-reuse trace at backpressure rate (potentially
    /// hundreds/sec at saturation). Without this discriminator, every
    /// per-blob-mpsc-full event would tear down the h2 connection.
    ///
    /// ResourceExhausted WITHOUT the discriminator continues to be
    /// classified as dead channel — preserves the existing #147
    /// post-GOAWAY recovery shape (h2 ENHANCE_YOUR_CALM emerges as bare
    /// `Code::ResourceExhausted` with no detail).
    #[test]
    fn looks_like_dead_channel_respects_backpressure_signal() {
        use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::backpressure_signal;

        use crate::chunked_signal::encode_backpressure_signal_any;

        // ResourceExhausted + backpressure signal → false (do NOT evict).
        // Includes the new #212 fixup variants: MemoryStoreAtCapacity
        // (Phase 2.6) AND PinnedBytesExhausted (Phase 2.5/2.7 fixup B1).
        // Both MUST be treated as transient backpressure (NOT dead
        // channel) so the existing h2 channel survives.
        for reason in [
            backpressure_signal::Reason::GlobalChunkBudgetExhausted,
            backpressure_signal::Reason::PerBlobMpscFull,
            backpressure_signal::Reason::MemoryStoreAtCapacity,
            backpressure_signal::Reason::PinnedBytesExhausted,
            backpressure_signal::Reason::SlowWritesAtCapacity,
        ] {
            let any = encode_backpressure_signal_any(reason, 100);
            let err = Error::resource_exhausted_backpressure("backpressure", any);
            assert!(
                !looks_like_dead_channel(&err),
                "ResourceExhausted with BackpressureSignal({reason:?}) must NOT \
                 evict h2 channel — transient backpressure is not a dead-channel \
                 signal (#212 §13.1.1 point 2 fix; new variants per fixups Phase 2.6 + B1)",
            );
        }

        // ResourceExhausted WITHOUT the discriminator → true (still
        // treated as dead channel for the legacy h2 ENHANCE_YOUR_CALM
        // shape). This preserves the #147 post-GOAWAY recovery path.
        let raw = make_err!(Code::ResourceExhausted, "no signal attached");
        assert!(
            looks_like_dead_channel(&raw),
            "bare ResourceExhausted (no BackpressureSignal) must continue to \
             classify as dead channel — preserves #147 GOAWAY recovery",
        );
    }

    /// Bug B regression: a clean `Status::OK` trailer (`clean_eof = true`)
    /// with `bytes_received < chunk_length` must classify as
    /// `CleanShort` (treated as success), NOT as a truncation that needs
    /// retry. This is the smallest reproducer of the digest 1e08eefa…-183
    /// infinite zombie loop: the per-chunk fetcher saw 183 bytes from
    /// the peer, then `stream.next()` returned `None` (peer's
    /// `Status::OK` trailer), and the previous classifier emitted
    /// `RetryResult::Retry(Code::DataLoss)` because the chunk requested
    /// more bytes than the entire blob held.
    ///
    /// tonic's `Streaming::poll_next` only yields `None` after the
    /// underlying response body finished WITHOUT a non-OK gRPC status
    /// (see `tonic::codec::decode::Streaming::poll_next` →
    /// `inner.response()`), so `clean_eof == true` is unambiguous.
    #[test]
    fn classify_clean_eof_short_blob_is_clean_short() {
        let outcome = classify_chunk_attempt(
            /*clean_eof=*/ true, /*bytes_received=*/ 183,
            /*chunk_length=*/ 2_500_000,
        );
        assert_eq!(
            outcome,
            ChunkAttemptOutcome::CleanShort,
            "Bug B: peer returned 183 bytes + Status::OK for a request \
             asking for 2.5 MiB — must be CleanShort (success), not retry"
        );
    }

    /// Sanity: full chunk delivered + clean EOF still classifies as
    /// `Complete`. (`clean_eof` is irrelevant once `bytes_received == chunk_length`.)
    #[test]
    fn classify_full_chunk_with_clean_eof_is_complete() {
        let outcome = classify_chunk_attempt(true, 1024, 1024);
        assert_eq!(outcome, ChunkAttemptOutcome::Complete);
    }

    /// Sanity: full chunk delivered without clean EOF (e.g. the peer
    /// closed early after delivering exactly `chunk_length` bytes) is
    /// still `Complete`.
    #[test]
    fn classify_full_chunk_without_clean_eof_is_complete() {
        let outcome = classify_chunk_attempt(false, 1024, 1024);
        assert_eq!(outcome, ChunkAttemptOutcome::Complete);
    }

    /// Bug B regression: empty-data-frame early break (no clean EOF) +
    /// short bytes → `AmbiguousEarlyBreak`. The pre-fix behaviour
    /// (always retry) is preserved for this case — empty data frames
    /// with no trailer are suspicious enough to warrant a retry.
    #[test]
    fn classify_empty_data_break_is_ambiguous_early_break() {
        let outcome = classify_chunk_attempt(
            /*clean_eof=*/ false, /*bytes_received=*/ 100, /*chunk_length=*/ 1024,
        );
        assert_eq!(outcome, ChunkAttemptOutcome::AmbiguousEarlyBreak);
    }

    /// Edge case: zero bytes received with a clean EOF — typically
    /// reading at offset >= resource size. Must surface as success
    /// (the resource is exhausted), NOT data loss.
    #[test]
    fn classify_zero_bytes_clean_eof_is_clean_short() {
        let outcome = classify_chunk_attempt(true, 0, 4096);
        assert_eq!(outcome, ChunkAttemptOutcome::CleanShort);
    }

    /// Edge case: zero bytes received without a clean EOF — the loop
    /// broke on an empty data frame before any data and before a
    /// trailer. Stays `AmbiguousEarlyBreak` (retry).
    #[test]
    fn classify_zero_bytes_no_clean_eof_is_ambiguous() {
        let outcome = classify_chunk_attempt(false, 0, 4096);
        assert_eq!(outcome, ChunkAttemptOutcome::AmbiguousEarlyBreak);
    }

    /// Defensive: `bytes_received > chunk_length` is a server protocol
    /// violation (the server returned more bytes than asked for). The
    /// classifier should not panic; the equality check `== chunk_length`
    /// fails so we fall through. With `clean_eof = true` we treat as
    /// CleanShort (caller already accumulated the data via the writer
    /// and the digest hash check downstream will catch any corruption);
    /// with `clean_eof = false` we treat as AmbiguousEarlyBreak (retry).
    /// Either is defensible — the key invariant is "no panic and no
    /// silent DataLoss for clean-OK responses."
    #[test]
    fn classify_overrun_with_clean_eof_does_not_panic() {
        let outcome = classify_chunk_attempt(true, 5000, 4096);
        // overrun + clean_eof: not Complete (== fails), and clean_eof
        // path returns CleanShort.
        assert_eq!(outcome, ChunkAttemptOutcome::CleanShort);
    }

    // -----------------------------------------------------------------------
    // #2 Fix-C: `looks_like_latched_pool` classifier unit tests.
    //
    // The tower-Buffer ServiceError message is the only discriminator for
    // the latched-pool signature. The function gates Fix-C early-abort;
    // wrong classification either burns the full retry budget (false
    // negative) or prematurely aborts on genuine transport errors (false
    // positive). Both directions are tested below.
    // -----------------------------------------------------------------------

    /// True positive: Unknown code + "buffered service" substring + elapsed
    /// within the instant-fail threshold → `looks_like_latched_pool` returns
    /// `true`.
    ///
    /// **Mutation step:** flip the return value of `looks_like_latched_pool`
    /// to always `false`. This test must red-fail with
    /// "latched-pool true-positive: must detect the buffered-service signature".
    #[test]
    fn latched_pool_classifier_true_positive() {
        let err = make_err!(
            Code::Unknown,
            "Service was not ready: buffered service failed: timed out"
        );
        // elapsed within threshold
        assert!(
            looks_like_latched_pool(&err, LATCHED_POOL_INSTANT_FAIL_MS - 1),
            "latched-pool true-positive: must detect the buffered-service signature \
             for Unknown+instant-fail — Fix-C early-abort will not fire without it"
        );
    }

    /// False positive guard 1: correct message but elapsed ABOVE threshold.
    /// A real transport error that happens to contain "buffered service" in
    /// a log context and takes >50ms must NOT be classified as latched.
    ///
    /// **Mutation step:** remove the `elapsed_ms > LATCHED_POOL_INSTANT_FAIL_MS`
    /// early-return. This test must red-fail with
    /// "latched-pool false-positive: must not match when elapsed > threshold".
    #[test]
    fn latched_pool_classifier_false_positive_high_elapsed() {
        let err = make_err!(
            Code::Unknown,
            "Service was not ready: buffered service failed: timed out"
        );
        assert!(
            !looks_like_latched_pool(&err, LATCHED_POOL_INSTANT_FAIL_MS + 1),
            "latched-pool false-positive: must not match when elapsed > threshold \
             ({LATCHED_POOL_INSTANT_FAIL_MS}ms) — would prematurely abort retries \
             on slow-but-real transport errors"
        );
    }

    /// False positive guard 2: instant failure but different status code
    /// (e.g. `Unavailable`). Must NOT be classified as latched.
    #[test]
    fn latched_pool_classifier_false_positive_wrong_code() {
        let err = make_err!(
            Code::Unavailable,
            "Service was not ready: buffered service failed: timed out"
        );
        assert!(
            !looks_like_latched_pool(&err, 0),
            "latched-pool false-positive: Unavailable code must not match — \
             latched-pool is exclusively Code::Unknown"
        );
    }

    /// False positive guard 3: instant failure + Unknown code but message
    /// does NOT contain "buffered service" — plain transport unknown error.
    #[test]
    fn latched_pool_classifier_false_positive_unrelated_message() {
        let err = make_err!(Code::Unknown, "transport error: connection reset");
        assert!(
            !looks_like_latched_pool(&err, 0),
            "latched-pool false-positive: Unknown+instant without 'buffered service' \
             substring must not match — would abort retries on unrelated transport resets"
        );
    }

    /// Constant sanity check: the abort threshold must be ≥ 2. Lower values
    /// would abort on a single coincidentally-fast transient (false positive);
    /// Fix-A evicts on the first failure so the second attempt draws a
    /// fresh channel — N=2 is the minimum meaningful sample.
    ///
    /// **Mutation step:** set `LATCHED_POOL_ABORT_THRESHOLD = 1`. This test
    /// must red-fail with "LATCHED_POOL_ABORT_THRESHOLD must be exactly 2 ...".
    #[test]
    fn latched_pool_abort_threshold_is_at_least_two() {
        assert_eq!(
            LATCHED_POOL_ABORT_THRESHOLD, 2,
            "LATCHED_POOL_ABORT_THRESHOLD must be exactly 2 — the doc-comment \
             at its declaration explains why N=2 is the minimum meaningful value; \
             values > 2 would delay Fix-C abort against a fully-latched pool. \
             Changing this constant requires updating the doc-comment reasoning \
             AND re-verifying Fix-A's single-evict assumption (#2 Fix-C)"
        );
    }

    // -----------------------------------------------------------------------
    // #FU QUIC read-failure diagnostic: bucket-split + rate-limit gate.
    //
    // These pure functions are the discriminator's primary signal. The
    // bucket boundary MUST match `looks_like_latched_pool`'s 50ms gate so
    // the `instant`:`slow` ratio answers the latch-vs-backpressure question
    // directly; the rate-limit gate MUST bound a recurrence below the 46k
    // line 2026-06-23 burst while emitting at least one line per second.
    // -----------------------------------------------------------------------

    /// The bucket boundary is the SAME 50ms threshold the latch classifier
    /// uses: `elapsed <= 50` is `Instant`, `> 50` is `Slow`. The `<=`
    /// inclusivity matches `looks_like_latched_pool` (which classifies a
    /// latch only when `elapsed_ms <= LATCHED_POOL_INSTANT_FAIL_MS`), so a
    /// failure the classifier calls a latch always lands in `Instant`.
    ///
    /// **Mutation step:** change the comparison at `quic_read_fail_bucket`
    /// from `<=` to `<` (or flip the arms). This test red-fails with the
    /// bespoke "bucket boundary must match looks_like_latched_pool's 50ms
    /// gate exactly" message at the boundary value 50.
    #[cfg(feature = "quic")]
    #[test]
    fn quic_read_fail_bucket_splits_on_latch_threshold() {
        assert_eq!(
            quic_read_fail_bucket(0),
            QuicReadFailBucket::Instant,
            "0ms must bucket Instant — the genuine instant-latch shape (<=5ms)"
        );
        assert_eq!(
            quic_read_fail_bucket(LATCHED_POOL_INSTANT_FAIL_MS - 1),
            QuicReadFailBucket::Instant,
            "just below the 50ms boundary must bucket Instant"
        );
        assert_eq!(
            quic_read_fail_bucket(LATCHED_POOL_INSTANT_FAIL_MS),
            QuicReadFailBucket::Instant,
            "bucket boundary must match looks_like_latched_pool's 50ms gate \
             exactly: elapsed == LATCHED_POOL_INSTANT_FAIL_MS is the LAST value \
             the latch classifier still calls a latch (it returns false only \
             when elapsed > threshold), so it MUST bucket Instant — a `<` here \
             would split the boundary value into Slow and skew the discriminator"
        );
        assert_eq!(
            quic_read_fail_bucket(LATCHED_POOL_INSTANT_FAIL_MS + 1),
            QuicReadFailBucket::Slow,
            "just above the 50ms boundary must bucket Slow — the backpressure shape"
        );
        assert_eq!(
            quic_read_fail_bucket(5000),
            QuicReadFailBucket::Slow,
            "near-timeout-bound elapsed must bucket Slow (Finding B backpressure)"
        );
    }

    /// Rate-limit gate: two events `< min_interval` apart yield exactly one
    /// emit; the second is suppressed. An event `>= min_interval` after the
    /// last emit is allowed. First-ever event (`last_emit == 0`) always
    /// emits. This bounds a recurrence to ~1 line/sec.
    ///
    /// **Mutation step:** change `>=` to `>` (or `<` the whole comparison)
    /// at `quic_read_fail_should_emit`. The within-interval suppression
    /// assertion red-fails with "second event within 1s must be suppressed".
    #[cfg(feature = "quic")]
    #[test]
    fn quic_read_fail_rate_limit_gate_suppresses_within_interval() {
        let min = QUIC_READ_FAIL_LOG_MIN_INTERVAL_NANOS;

        // First-ever event (last_emit == 0) always emits.
        assert!(
            quic_read_fail_should_emit(1_000, 0, min),
            "first-ever event (last_emit == 0) must emit"
        );

        // Two events < min apart: the first emits and publishes `now`; the
        // second (only 1ms = 1_000_000ns later) must be suppressed.
        let first_now = 10 * min; // arbitrary monotonic point well past epoch
        assert!(
            quic_read_fail_should_emit(first_now, 0, min),
            "first of a burst must emit"
        );
        let second_now = first_now + 1_000_000; // +1ms, far below the 1s interval
        assert!(
            !quic_read_fail_should_emit(second_now, first_now, min),
            "second event within 1s must be suppressed — gate publishes the \
             first emit's timestamp and the +1ms delta is below the 1s interval; \
             without suppression a recurrence reproduces the 46k-line burst"
        );

        // An event exactly `min` after the last emit is allowed (boundary).
        assert!(
            quic_read_fail_should_emit(first_now + min, first_now, min),
            "an event exactly min_interval after the last emit must emit \
             (>= boundary) — the gate must not stall at exactly 1s"
        );
        // An event 1ns before `min` is still suppressed.
        assert!(
            !quic_read_fail_should_emit(first_now + min - 1, first_now, min),
            "an event 1ns before min_interval must still be suppressed"
        );
    }

    /// Leg-gating predicate: the QUIC diagnostic must count + log ONLY read
    /// failures that ACTUALLY traversed the QUIC leg. The routing each call
    /// site selects (`read_internal(_, prefer_tcp)`) determines the leg:
    ///   - single-stream passes `prefer_tcp = false` → QUIC under Dual,
    ///     QUIC under Quic-only;
    ///   - parallel passes `prefer_tcp = true` → TCP under Dual (NOT a QUIC
    ///     read — must not pollute the counters), but QUIC under Quic-only.
    /// So a read went over QUIC iff `kind == Quic` OR (`kind == Dual` AND
    /// `!prefer_tcp`). `quic_read_leg` returns `Some(leg)` exactly then, and
    /// the `leg` string is the PRECISE transport actually used so the warn!
    /// never tags a counted event as the ambiguous `"dual"`: `"quic"` for the
    /// Quic-only transport, `"dual/quic"` for the Dual single-stream leg.
    ///
    /// **Mutation step:** flip the `!prefer_tcp` condition at `quic_read_leg`
    /// (the Dual arm guard `if !prefer_tcp` → `if prefer_tcp`). The flip
    /// inverts the Dual arm entirely — `Dual + prefer_tcp=false` (genuinely
    /// over QUIC) now wrongly returns `None`, and `Dual + prefer_tcp=true`
    /// (over TCP) now wrongly returns `Some("dual/quic")`. Both directions
    /// are asserted below; the first-failing assertion red-fails with the
    /// bespoke "Dual + prefer_tcp=false (single-stream) routes over QUIC —
    /// must count, tagged \"dual/quic\"" message (verified `left: None,
    /// right: Some("dual/quic")`). Either direction failing proves the
    /// parallel chunk path's TCP failures would re-pollute the instant:slow
    /// ratio the discriminator depends on.
    #[cfg(feature = "quic")]
    #[test]
    fn quic_read_leg_gates_on_actual_transport_leg() {
        // Quic-only transport: BOTH prefer values go over QUIC (there is no
        // TCP leg to fall back to). Tag is the precise `"quic"`.
        assert_eq!(
            quic_read_leg(ReadTransportKind::Quic, false),
            Some("quic"),
            "Quic-only + prefer_tcp=false (single-stream) must count as a QUIC \
             read tagged \"quic\""
        );
        assert_eq!(
            quic_read_leg(ReadTransportKind::Quic, true),
            Some("quic"),
            "Quic-only + prefer_tcp=true (parallel) still goes over QUIC — there \
             is no TCP leg under the Quic-only transport — must count, tagged \"quic\""
        );

        // Dual + prefer_tcp=false (single-stream): routes over the QUIC leg.
        // Precise tag `"dual/quic"`, never the ambiguous `"dual"`.
        assert_eq!(
            quic_read_leg(ReadTransportKind::Dual, false),
            Some("dual/quic"),
            "Dual + prefer_tcp=false (single-stream) routes over QUIC — must count, \
             tagged \"dual/quic\" (NEVER the ambiguous \"dual\" for a counted event)"
        );

        // Dual + prefer_tcp=true (parallel chunk): routes over the TCP leg.
        // This is the bug the fix closes — these TCP failures must NOT be
        // counted by the QUIC probe.
        assert_eq!(
            quic_read_leg(ReadTransportKind::Dual, true),
            None,
            "Dual + prefer_tcp=true routes over TCP — must NOT count as a QUIC \
             read: the parallel chunk site passes prefer_tcp=true, so under the \
             Dual transport its failures are TCP-leg failures that would pollute \
             the instant:slow ratio the latch-vs-backpressure discriminator needs"
        );

        // Tcp-only transport: never a QUIC read, regardless of prefer.
        assert_eq!(
            quic_read_leg(ReadTransportKind::Tcp, false),
            None,
            "Tcp-only + prefer_tcp=false must NOT count — there is no QUIC leg"
        );
        assert_eq!(
            quic_read_leg(ReadTransportKind::Tcp, true),
            None,
            "Tcp-only + prefer_tcp=true must NOT count — there is no QUIC leg"
        );
    }
}
