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

use core::convert::Into;
use core::fmt::{Debug, Formatter};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures::future::pending;
use futures::stream::{StreamExt, unfold};
use futures::{Future, Stream, TryFutureExt, try_join};
use nativelink_config::cas_server::{ByteStreamConfig, InstanceName, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use nativelink_proto::google::bytestream::byte_stream_server::{
    ByteStream, ByteStreamServer as Server,
};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::small_blob_dispatcher::{SMALL_BLOB_THRESHOLD, SmallBlobDispatcher};
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair_with_size,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, make_ctx_for_hash_func,
};
use nativelink_util::log_utils::throughput_mbps;
use nativelink_util::proto_stream_utils::{ReadProgressObserver, WriteRequestStreamWrapper};
use nativelink_util::resource_info::ResourceInfo;
use nativelink_util::spawn;
use nativelink_util::stall_detector::StallGuard;
use nativelink_util::store_trait::{
    IS_MIRROR_REQUEST, IS_WORKER_REQUEST, REDIRECT_PREFIX, Store, StoreLike, StoreOptimizations,
    UploadSizeInfo,
};
use nativelink_util::streaming_blob::{InFlightBlobMap, StreamingBlobWriter};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::zero_copy_codec::{
    GrpcUnaryBody, ZeroCopyReadBody, ZeroCopyWriteStream, decode_unary_request,
    encode_grpc_unary_response,
};
use opentelemetry::context::FutureExt;
use parking_lot::Mutex;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::sleep;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument, Level, debug, error, error_span, info, instrument, trace, warn};

/// If this value changes update the documentation in the config definition.
const DEFAULT_PERSIST_STREAM_ON_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// If this value changes update the documentation in the config definition.
const DEFAULT_MAX_BYTES_PER_STREAM: usize = 3 * 1024 * 1024;

/// Default memory budget for partial (idle) writes: 256 MiB.
const DEFAULT_MAX_PARTIAL_WRITE_BYTES: u64 = 256 * 1024 * 1024;

/// Saturating decrement for an `AtomicU64`. Prevents wrapping to `u64::MAX`
/// if concurrent `fetch_sub` calls race (e.g., sweeper eviction + stream resume).
#[inline]
fn atomic_saturating_sub(counter: &AtomicU64, val: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        Some(cur.saturating_sub(val))
    });
}

/// Metrics for `ByteStream` server operations.
/// Tracks upload/download activity, throughput, and latency.
#[derive(Debug, Default)]
pub struct ByteStreamMetrics {
    /// Number of currently active uploads (includes idle streams waiting for resume)
    pub active_uploads: AtomicU64,
    /// Total number of write requests received
    pub write_requests_total: AtomicU64,
    /// Total number of successful write requests
    pub write_requests_success: AtomicU64,
    /// Total number of failed write requests
    pub write_requests_failure: AtomicU64,
    /// Total number of read requests received
    pub read_requests_total: AtomicU64,
    /// Total number of successful read requests
    pub read_requests_success: AtomicU64,
    /// Total number of failed read requests
    pub read_requests_failure: AtomicU64,
    /// Total number of `query_write_status` requests
    pub query_write_status_total: AtomicU64,
    /// Total bytes written via `ByteStream`
    pub bytes_written_total: AtomicU64,
    /// Total bytes read via `ByteStream`
    pub bytes_read_total: AtomicU64,
    /// Sum of write durations in nanoseconds (for average latency calculation)
    pub write_duration_ns: AtomicU64,
    /// Sum of read durations in nanoseconds (for average latency calculation)
    pub read_duration_ns: AtomicU64,
    /// Number of UUID collisions detected
    pub uuid_collisions: AtomicU64,
    /// Number of resumed uploads (client reconnected to existing stream)
    pub resumed_uploads: AtomicU64,
    /// Number of idle streams that timed out
    pub idle_stream_timeouts: AtomicU64,
    /// Current total bytes held in idle (partial) streams
    pub partial_write_bytes: AtomicU64,
    /// Number of idle streams evicted due to memory pressure
    pub idle_stream_evictions_memory: AtomicU64,
    /// Number of mirror tee chunks dropped because the mirror channel was full.
    /// Increments per dropped chunk; many drops on the same blob are still counted
    /// individually so we can see backpressure rate, not just affected blob count.
    pub mirror_chunks_dropped_backpressure: AtomicU64,
    /// Number of blobs whose mirror tee was incomplete due to one or more dropped
    /// chunks. Increments at most once per blob. Useful for sizing the impact on
    /// locality cache freshness.
    pub mirror_blobs_incomplete: AtomicU64,
}

impl MetricsComponent for ByteStreamMetrics {
    fn publish(
        &self,
        _kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let _enter = group!(field_metadata.name).entered();

        publish!(
            "active_uploads",
            &self.active_uploads,
            MetricKind::Counter,
            "Number of currently active uploads"
        );
        publish!(
            "write_requests_total",
            &self.write_requests_total,
            MetricKind::Counter,
            "Total write requests received"
        );
        publish!(
            "write_requests_success",
            &self.write_requests_success,
            MetricKind::Counter,
            "Total successful write requests"
        );
        publish!(
            "write_requests_failure",
            &self.write_requests_failure,
            MetricKind::Counter,
            "Total failed write requests"
        );
        publish!(
            "read_requests_total",
            &self.read_requests_total,
            MetricKind::Counter,
            "Total read requests received"
        );
        publish!(
            "read_requests_success",
            &self.read_requests_success,
            MetricKind::Counter,
            "Total successful read requests"
        );
        publish!(
            "read_requests_failure",
            &self.read_requests_failure,
            MetricKind::Counter,
            "Total failed read requests"
        );
        publish!(
            "query_write_status_total",
            &self.query_write_status_total,
            MetricKind::Counter,
            "Total query_write_status requests"
        );
        publish!(
            "bytes_written_total",
            &self.bytes_written_total,
            MetricKind::Counter,
            "Total bytes written via ByteStream"
        );
        publish!(
            "bytes_read_total",
            &self.bytes_read_total,
            MetricKind::Counter,
            "Total bytes read via ByteStream"
        );
        publish!(
            "write_duration_ns",
            &self.write_duration_ns,
            MetricKind::Counter,
            "Sum of write durations in nanoseconds"
        );
        publish!(
            "read_duration_ns",
            &self.read_duration_ns,
            MetricKind::Counter,
            "Sum of read durations in nanoseconds"
        );
        publish!(
            "uuid_collisions",
            &self.uuid_collisions,
            MetricKind::Counter,
            "Number of UUID collisions detected"
        );
        publish!(
            "resumed_uploads",
            &self.resumed_uploads,
            MetricKind::Counter,
            "Number of resumed uploads"
        );
        publish!(
            "idle_stream_timeouts",
            &self.idle_stream_timeouts,
            MetricKind::Counter,
            "Number of idle streams that timed out"
        );
        publish!(
            "partial_write_bytes",
            &self.partial_write_bytes,
            MetricKind::Counter,
            "Current total bytes held in idle streams"
        );
        publish!(
            "idle_stream_evictions_memory",
            &self.idle_stream_evictions_memory,
            MetricKind::Counter,
            "Idle streams evicted due to memory pressure"
        );
        publish!(
            "mirror_chunks_dropped_backpressure",
            &self.mirror_chunks_dropped_backpressure,
            MetricKind::Counter,
            "Mirror tee chunks dropped because the mirror channel was full"
        );
        publish!(
            "mirror_blobs_incomplete",
            &self.mirror_blobs_incomplete,
            MetricKind::Counter,
            "Blobs whose mirror tee was incomplete due to dropped chunks"
        );

        Ok(MetricPublishKnownKindData::Component)
    }
}

type BytesWrittenAndIdleStream = (Arc<AtomicU64>, Option<IdleStream>);

/// Type alias for the UUID key used in `active_uploads` `HashMap`.
/// Using u128 instead of String reduces memory allocations and improves
/// cache locality for `HashMap` operations.
type UuidKey = u128;

/// Parse a UUID string to a u128 for use as a `HashMap` key.
/// This avoids heap allocation for String keys and improves `HashMap` performance.
/// Falls back to hashing the string if it's not a valid hex UUID.
#[inline]
fn parse_uuid_to_key(uuid_str: &str) -> UuidKey {
    // UUIDs are typically 32 hex chars (128 bits) or 36 chars with dashes.
    // We'll try to parse as hex first, then fall back to hashing.
    let clean: String = uuid_str.chars().filter(char::is_ascii_hexdigit).collect();
    if clean.len() >= 16 {
        // Take up to 32 hex chars (128 bits)
        let hex_str = if clean.len() > 32 {
            &clean[..32]
        } else {
            &clean
        };
        u128::from_str_radix(hex_str, 16).unwrap_or_else(|_| {
            // Hash fallback for non-hex strings
            use core::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            uuid_str.hash(&mut hasher);
            u128::from(hasher.finish())
        })
    } else {
        // Short strings: use hash
        use core::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        uuid_str.hash(&mut hasher);
        u128::from(hasher.finish())
    }
}

pub struct InstanceInfo {
    store: Store,
    /// The configured `cas_store` name (e.g. `"cas_STORE"`). Used as the
    /// `store_id` in `SmallBlobDispatcher::schedule_dispatch_to_all_workers` so
    /// that the per-store `EphemeralServerSidePin` registered at startup
    /// (under the SAME name in `nativelink.rs:499-525`) receives the pin
    /// insert. Pre-allocated as `Arc<str>` so the per-blob hot path
    /// only does an O(1) refcount bump (perf-optimizer #168 NIT-1 —
    /// avoids `Arc::from(&str)` allocation per dispatch).
    cas_store_name_arc: Arc<str>,
    /// #168 producer-side hook: when `Some`, every successful oneshot
    /// CAS write of a blob ≤ `SMALL_BLOB_THRESHOLD` is fanned out to
    /// every connected worker via the dispatcher. `None` when no worker
    /// scheduler is configured (the dispatcher is not constructed in
    /// that case — see `nativelink.rs:409-534`).
    small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    // Max number of bytes to send on each grpc stream chunk.
    max_bytes_per_stream: usize,
    /// Active uploads keyed by UUID as u128 for better performance.
    /// Using u128 keys instead of String reduces heap allocations
    /// and improves `HashMap` lookup performance.
    active_uploads: Arc<Mutex<HashMap<UuidKey, BytesWrittenAndIdleStream>>>,
    /// How long to keep idle streams before timing them out.
    idle_stream_timeout: Duration,
    metrics: Arc<ByteStreamMetrics>,
    /// Handle to the global sweeper task. Kept alive for the lifetime of the instance.
    _sweeper_handle: Arc<JoinHandleDropGuard<()>>,
    /// In-flight CAS writes keyed by digest. When multiple RPCs arrive for
    /// the same digest concurrently, only the first performs the actual
    /// write; the rest subscribe to the watch channel and get the result.
    /// `None` = in progress, `Some(true)` = succeeded, `Some(false)` = failed.
    // UNBOUNDED-OK: digest-keyed dedup map. Each value is
    // `watch::Receiver<Option<bool>>` (~64 B). h2 stream concurrency is
    // NOT capped in production (no `experimental_http2_max_concurrent_streams`
    // set in `prod-server.json5`; tonic 0.14 default is `usize::MAX`). The
    // byte-budget makes this acceptable: at 10K concurrent in-flight
    // writes, total cost is ~640 KB. RAII `InFlightWritesGuard` at
    // `:495-509` removes entries on grpc-future-drop (cancel-safe per
    // #402: cancel-safe RAII guard for in_flight_writes).
    in_flight_writes: Arc<Mutex<HashMap<DigestInfo, tokio::sync::watch::Receiver<Option<bool>>>>>,
    /// Registry of in-flight streaming blobs.  Readers can discover and
    /// stream from uploads that have not yet committed to the store.
    /// Only populated when `streaming_read_while_write` is enabled.
    in_flight_blobs: Arc<InFlightBlobMap>,
    /// Whether the streaming read-while-write feature is enabled.
    streaming_read_while_write: bool,
    /// Per-blob buffer budget for streaming blobs (bytes).
    max_streaming_blob_buffer_bytes: u64,
    /// Maximum total bytes held across all partial (idle) uploads.
    /// 0 means unlimited (time-based eviction only).
    max_partial_write_bytes: u64,
    /// Current total bytes held in idle streams. Shared with the sweeper.
    partial_write_bytes: Arc<AtomicU64>,
}

impl Debug for InstanceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InstanceInfo")
            .field("store", &self.store)
            .field("cas_store_name", &self.cas_store_name_arc)
            .field(
                "small_blob_dispatcher",
                &self.small_blob_dispatcher.is_some(),
            )
            .field("max_bytes_per_stream", &self.max_bytes_per_stream)
            .field("active_uploads", &self.active_uploads)
            .field("idle_stream_timeout", &self.idle_stream_timeout)
            .field("metrics", &self.metrics)
            .field(
                "streaming_read_while_write",
                &self.streaming_read_while_write,
            )
            .field("in_flight_blobs", &self.in_flight_blobs)
            .finish()
    }
}

/// RAII guard for the `in_flight_writes` map (the deduplication of
/// concurrent ByteStream uploads for the same digest).
///
/// **#402 cancel-safety:** the previous code manually `insert`ed into
/// `in_flight_writes` before `write_fut.await` and manually `remove`d
/// after. The await is a cancellation point: if the gRPC stream is
/// cancelled (client
/// disconnect, RST_STREAM, server shutdown, runtime drop), the future
/// is dropped mid-await and the manual `remove` NEVER runs. Result:
/// every cancelled upload leaks one `HashMap` entry + one watch
/// channel, permanently. Subsequent RPCs for the same digest then
/// coalesce onto the orphaned entry — but the `Sender` was also dropped
/// by cancellation so they immediately observe `rx.changed() = Err`,
/// translate to "in-flight write failed, retrying", and waste a
/// coalescing round.
///
/// **Sibling of `InFlightChunkedGuard`** in `chunked_write_handler.rs`
/// (#401, the bug found by the same DSR-pattern audit). Same shape,
/// different in-flight tracker.
///
/// **Contract:**
///   * `new(map, digest, tx, rx)` inserts `(digest, rx)` into `map` and
///     takes ownership of `tx`. Returns the guard.
///   * `set_result(bool)` publishes the outcome to coalesced waiters via
///     the watch channel. Optional — if never called, Drop's `Sender`
///     drop signals failure to waiters (via `rx.changed() = Err`).
///   * `Drop` removes `(digest, _)` from `map` and drops the `Sender`
///     (which signals failure to any waiter that didn't observe
///     `set_result`'s value first). Drop runs on EVERY exit path
///     including cancellation.
///
/// **Drop is cancellation-safe:** removal of an absent key is a no-op
/// (`HashMap::remove` returns `None`), so spurious double-drops or
/// races with sibling cleanup are tolerated.
///
/// **Fields are private** — construction goes through `new`, removal
/// goes through Drop. There is no other API surface; the guard must
/// remain trivially auditable.
#[derive(Debug)]
pub struct InFlightWritesGuard {
    map: Arc<Mutex<HashMap<DigestInfo, tokio::sync::watch::Receiver<Option<bool>>>>>,
    digest: DigestInfo,
    /// Owned watch sender. `Option` so `set_result` can take it by
    /// value to publish, leaving Drop with `None` (the publish already
    /// dropped the sender by replacing it). `None` after a successful
    /// `set_result`; `Some` on the cancellation path. Either way Drop
    /// just drops whatever's left — no manual `send` required because
    /// `Sender::drop` itself is the failure signal that the production
    /// coalesced-waiter loop at `bytestream_server.rs:2349-2358`
    /// already understands (`rx.changed() = Err` → return false).
    tx: Option<tokio::sync::watch::Sender<Option<bool>>>,
}

impl InFlightWritesGuard {
    /// Insert `(digest, rx)` into `map` and return a guard that will
    /// remove it on Drop. Takes ownership of `tx` so the caller cannot
    /// accidentally publish the outcome through a side channel that
    /// bypasses the guard.
    ///
    /// Acquires the map lock once for the insert. Production callers
    /// that need to perform the insert under a pre-existing
    /// dedup lock (to avoid a race where two RPCs both become primary
    /// writers between unlock + re-lock) should use
    /// [`Self::from_inserted`] instead.
    #[must_use]
    pub fn new(
        map: Arc<Mutex<HashMap<DigestInfo, tokio::sync::watch::Receiver<Option<bool>>>>>,
        digest: DigestInfo,
        tx: tokio::sync::watch::Sender<Option<bool>>,
        rx: tokio::sync::watch::Receiver<Option<bool>>,
    ) -> Self {
        map.lock().insert(digest, rx);
        Self {
            map,
            digest,
            tx: Some(tx),
        }
    }

    /// Construct a guard whose `(digest, rx)` was ALREADY inserted into
    /// `map` by the caller (typically under a dedup lock the caller
    /// holds for race-freedom). Skips the insert; takes ownership of
    /// `tx` and is responsible for `Drop`-time removal.
    ///
    /// Use this when the caller needs to atomically check-then-insert
    /// under a single lock acquisition (the production primary-writer
    /// path). For the simpler "insert under our own lock" case, prefer
    /// [`Self::new`].
    #[must_use]
    pub fn from_inserted(
        map: Arc<Mutex<HashMap<DigestInfo, tokio::sync::watch::Receiver<Option<bool>>>>>,
        digest: DigestInfo,
        tx: tokio::sync::watch::Sender<Option<bool>>,
    ) -> Self {
        Self {
            map,
            digest,
            tx: Some(tx),
        }
    }

    /// Publish `succeeded` to coalesced waiters via the watch channel.
    ///
    /// The production code at `bytestream_server.rs:2501-2505` calls
    /// this on both success (`Ok(_)`) and error (`Err(_)`) paths so
    /// coalesced waiters learn the actual outcome rather than the
    /// cancellation-shaped "sender dropped" failure signal.
    ///
    /// Idempotent: subsequent calls are a no-op (the `tx` was already
    /// taken on the first call). The guard's Drop will still remove
    /// the map entry regardless of whether `set_result` was called.
    pub fn set_result(&mut self, succeeded: bool) {
        if let Some(tx) = self.tx.take() {
            // Send is best-effort; an Err just means no waiters subscribed,
            // which is benign and equivalent to the prior `let _ = tx.send(...)`.
            let _ = tx.send(Some(succeeded));
        }
    }
}

impl Drop for InFlightWritesGuard {
    fn drop(&mut self) {
        // Remove the map entry on every exit path. `HashMap::remove`
        // tolerates an absent key (returns None), so this is safe even
        // if some sibling path concurrently removed.
        self.map.lock().remove(&self.digest);
        // `self.tx` is dropped here as part of the struct drop. If
        // `set_result` was never called (cancellation path), the
        // dropped Sender causes any coalesced waiter's `rx.changed()`
        // to return `Err` → the production loop at
        // `bytestream_server.rs:2349-2358` translates to false (failure)
        // → waiter falls through to the retry path. So waiters never
        // hang on a cancelled writer's orphaned channel.
    }
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + 'static>>;
type StoreUpdateFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

/// Wrapper around a `ReadStream` that:
/// - bumps the process-wide
///   [`nativelink_util::proto_stream_utils::GRPC_READ_SLOW_CHUNK_TOTAL`]
///   counter + emits a `warn!` whenever no chunk arrives for
///   [`nativelink_util::proto_stream_utils::GRPC_READ_SLOW_CHUNK_THRESHOLD`]
///   (per-chunk progress observer),
/// - logs total bytes and elapsed time at `info!` level when the stream
///   completes (yields `None`) or is dropped before completion (#479).
///
/// **Diagnostic-only.** The per-chunk observer never aborts the stream;
/// it only bumps the counter and logs. Mirrors the WRITE-side
/// `WriteState::with_progress_timeout` behaviour after the 2026-05-14
/// kill-switch removal.
///
/// Why `info!` not `debug!` for the completion log: tracker #479
/// observed that READ paths emitted ZERO per-stream INFO lines in
/// production, misleading three sub-agent investigations of slow
/// downloads. Per `feedback_no_log_no_proof_of_silence.md`. Bumping
/// the per-stream completion line to `info!` lets SREs grep journal
/// for `ByteStream::read: CAS read completed` to see every download's
/// throughput + duration without redeploying with a debug filter.
struct LoggingReadStream {
    inner: ReadProgressObserver<ReadStream>,
    start_time: Instant,
    digest: DigestInfo,
    expected_size: u64,
    bytes_sent: u64,
    completed: bool,
    /// Static label for the LoggingReadStream's call-site (e.g.
    /// `"bytestream_server::read"`). Surfaced in completion logs so
    /// the journal entry self-identifies the seam — without this, the
    /// completion line is ambiguous between the chunked `read` path
    /// and the `zero_copy_read` fast path.
    label: &'static str,
}

impl LoggingReadStream {
    fn new(
        inner: ReadStream,
        start_time: Instant,
        digest: DigestInfo,
        expected_size: u64,
        label: &'static str,
    ) -> Self {
        // Wrap with the per-chunk progress observer BEFORE storing.
        // The observer is a thin Stream<Item=T> wrapper that pulls
        // from `inner` directly — its only side effect on the chunk
        // path is rearming a `Sleep` deadline. No allocation per
        // chunk; only on the first chunk arrival.
        let observed = ReadProgressObserver::new(label, inner);
        Self {
            inner: observed,
            start_time,
            digest,
            expected_size,
            bytes_sent: 0,
            completed: false,
            label,
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn log_completion(&mut self, status: &'static str, err: Option<&Status>) {
        let elapsed = self.start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let mbps = throughput_mbps(self.bytes_sent, elapsed);
        // effective_rate_kbps surfaces a kbps integer alongside the
        // mbps string so the warn-threshold check below and external
        // grep-on-bytes/elapsed don't have to re-parse the formatted
        // mbps value. Saturating to u64 keeps a single zero-byte
        // 0-ms drop from blowing up.
        let elapsed_secs = elapsed.as_secs_f64();
        let effective_rate_kbps = if elapsed_secs > 0.0 {
            ((self.bytes_sent as f64 / 1024.0) / elapsed_secs) as u64
        } else {
            0
        };
        // Slow-and-completed-read warn: surfaced when an operator-
        // visible download takes >5s AND was below 1 MB/s. Mirrors the
        // inbound write-side slow-stream warn shape so SREs can grep
        // `ByteStream::read.*slow_completion` and get one line per
        // affected download.
        let slow_completion = elapsed_ms > 5000 && effective_rate_kbps < 1000 && status == "ok";
        if slow_completion {
            warn!(
                target: "nativelink_service::bytestream",
                label = self.label,
                digest = %self.digest,
                expected_size = self.expected_size,
                bytes_sent = self.bytes_sent,
                elapsed_ms,
                effective_rate_kbps,
                throughput_mbps = %mbps,
                status,
                "ByteStream::read: slow read completion (>=5s, <1MB/s) — \
                 confirm transport health (h2/TCP/QUIC keepalive, slow-tier hiccup)",
            );
        }
        // #500: silent 0-byte-ok response class corrupts Bazel
        // downloads on post-OOM reconnect. A NotFound from
        // store.get_part SHOULD surface as status="error", but the
        // unfold in inner_read can yield Some((Ok(ReadResponse::default()),
        // Some(state))) on its first poll then EOF — Bazel sees
        // stream-complete with 0 bytes and accepts the (empty) data as
        // canonical, hashes prefix-only bytes accumulated across earlier
        // parallel streams, reports digest mismatch as a build failure.
        // Warn-classify so SREs can grep `ByteStream::read.*silent-zero`
        // and see the corrupting events immediately instead of buried
        // in the per-stream info! line.
        let silent_zero =
            self.bytes_sent == 0 && self.expected_size > 0 && status == "ok";
        if silent_zero {
            warn!(
                target: "nativelink_service::bytestream",
                label = self.label,
                digest = %self.digest,
                expected_size = self.expected_size,
                bytes_sent = self.bytes_sent,
                elapsed_ms,
                status,
                "ByteStream::read: silent-zero — stream ended with status=ok \
                 but 0 bytes sent on an expected_size>0 read; Bazel will \
                 accept this as a complete-empty stream and report digest \
                 mismatch (see #500)",
            );
        }
        info!(
            target: "nativelink_service::bytestream",
            label = self.label,
            digest = %self.digest,
            expected_size = self.expected_size,
            bytes_sent = self.bytes_sent,
            elapsed_ms,
            effective_rate_kbps,
            throughput_mbps = %mbps,
            status,
            code = ?err.map(tonic::Status::code),
            msg = err.map(tonic::Status::message).unwrap_or(""),
            "ByteStream::read: CAS read completed",
        );
    }
}

impl Stream for LoggingReadStream {
    type Item = Result<ReadResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The per-chunk observer is structurally pinned via
        // `pin_project!`; project the field via `Pin::new`. Inner is
        // `ReadProgressObserver<ReadStream>` and `ReadStream` is
        // `Pin<Box<...>>` (Unpin), so `inner` is Unpin so we can take
        // `&mut self` then `Pin::new(&mut self.inner)`.
        let result = Pin::new(&mut self.inner).poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(response))) => {
                self.bytes_sent += response.data.len() as u64;
            }
            Poll::Ready(None) => {
                self.completed = true;
                self.log_completion("ok", None);
            }
            Poll::Ready(Some(Err(status))) => {
                self.completed = true;
                self.log_completion("error", Some(status));
            }
            Poll::Pending => {}
        }
        result
    }
}

impl Drop for LoggingReadStream {
    fn drop(&mut self) {
        if !self.completed {
            self.log_completion("dropped", None);
        }
    }
}

struct StreamState {
    uuid: UuidKey,
    digest: DigestInfo,
    tx: DropCloserWriteHalf,
    store_update_fut: StoreUpdateFuture,
    /// #418: lifecycle invariant — a `StreamState` is resumable iff its
    /// `store_update_fut` is paused mid-await AND its `tx` is still
    /// connected to a live `rx`. This flag distinguishes the two
    /// possible end-states of `store_update_fut` when an
    /// `ActiveStreamGuard` is dropped:
    ///
    /// - **Paused (resumable):** the outer `try_join!` was cancelled
    ///   before `store.update()` returned. The future is dropped at its
    ///   current await point — `result =` never assigns, this flag
    ///   stays `false`, and the captured `rx` survives inside the
    ///   future's frame. The stashed `IdleStream` is safe to resume on
    ///   the next QueryWriteStatus-driven retry (legitimate Bazel
    ///   TCP-flap recovery — see `resume_write_*` test family).
    /// - **Completed-with-Err (unrecoverable):** `store.update()`
    ///   returned `Err`. The wrapped future's body runs to completion;
    ///   the captured `rx` is dropped at end-of-scope. `tx` is now
    ///   pointing at a dead receiver and any subsequent `tx.send` from
    ///   a resumed stream would dead-end inside `buf_channel.rs` with
    ///   `Code::Internal: "Tried to send while stream is closed"`
    ///   (production observation 2026-05-12 — 218 MB upload abandoned
    ///   when chunked driver returned `ResourceExhausted` mid-stream).
    ///
    /// `ActiveStreamGuard::drop` reads this flag (alongside a
    /// belt-and-suspenders `tx.is_pipe_broken()` check; see drop-site
    /// comment) and REMOVES the entry from `active_uploads` in the
    /// completed-with-Err case so the next retry hits
    /// `into_active_stream`'s `Vacant` branch. Cancellation does NOT
    /// set this flag — that is the load-bearing distinction.
    store_errored: Arc<AtomicBool>,
}

impl Debug for StreamState {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StreamState")
            .field("uuid", &format!("{:032x}", self.uuid))
            .finish()
    }
}

/// If a stream is in this state, it will automatically be put back into an `IdleStream` and
/// placed back into the `active_uploads` map as an `IdleStream` after it is dropped.
/// To prevent it from being put back into an `IdleStream` you must call `.graceful_finish()`.
struct ActiveStreamGuard {
    stream_state: Option<StreamState>,
    bytes_received: Arc<AtomicU64>,
    active_uploads: Arc<Mutex<HashMap<UuidKey, BytesWrittenAndIdleStream>>>,
    metrics: Arc<ByteStreamMetrics>,
    /// Shared counter tracking total bytes held in idle streams.
    partial_write_bytes: Arc<AtomicU64>,
}

impl ActiveStreamGuard {
    /// Consumes the guard. The stream will be considered "finished", will
    /// remove it from the `active_uploads`.
    fn graceful_finish(mut self) {
        let stream_state = self.stream_state.take().unwrap();
        self.active_uploads.lock().remove(&stream_state.uuid);
        // Decrement active uploads counter on successful completion
        self.metrics.active_uploads.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        let Some(stream_state) = self.stream_state.take() else {
            return; // If None it means we don't want it put back into an IdleStream.
        };
        let mut active_uploads = self.active_uploads.lock();
        let uuid = stream_state.uuid; // u128 is Copy, no clone needed
        let Some(active_uploads_slot) = active_uploads.get_mut(&uuid) else {
            error!(
                err = "Failed to find active upload. This should never happen.",
                uuid = format!("{:032x}", uuid),
            );
            return;
        };

        // #418: corrupt-state arm. Lifecycle invariant: a StreamState
        // is resumable iff its `store_update_fut` is paused mid-await
        // AND its `tx` is connected to a live `rx`. This branch fires
        // when EITHER half of that conjunct fails:
        //
        // - `store_errored == true` ⇒ the future ran to
        //   completion-with-Err (NOT cancelled — see field doc); the
        //   captured `rx` was dropped at end-of-scope, so `tx` is now
        //   pointing at a dead receiver. THIS is the load-bearing
        //   discriminator for the production class (chunked driver
        //   returning `ResourceExhausted` mid-stream).
        // - `tx.is_pipe_broken() == true` ⇒ belt-and-suspenders for
        //   any future failure mode that breaks the channel without
        //   touching the wrapped future's Err path. Not known to fire
        //   in the present codebase; kept as future-regression
        //   defense.
        //
        // Without this arm, recycling into `IdleStream` would leave a
        // dead-channel landmine for the next QueryWriteStatus-driven
        // retry — `process_client_stream`'s `tx.send` would dead-end
        // inside `buf_channel.rs` with `Code::Internal: "Tried to
        // send while stream is closed"` (production observation
        // 2026-05-12 — 218 MB upload abandoned). Sweeper TTL = 60s,
        // so the wedge spans the full retry budget.
        //
        // Composite invariant (per CLAUDE.md "Admission/Eviction/Pin
        // Composability"): the active_uploads triangle is
        //   (gate=IdleStream resume, eviction=60s sweeper,
        //    pin=active_uploads entry while in-use).
        // Pre-fix the gate was always active even when state was
        // unrecoverable, with no compensating eviction firing within
        // the retry budget. This branch CLOSES the gate by REMOVING
        // the entry from `active_uploads` so the next retry hits
        // `into_active_stream`'s `Vacant` branch and either (a)
        // restarts cleanly from offset 0, or (b) sees its non-zero
        // write_offset rejected with the documented `Code::Unavailable
        // (Partial upload state was lost; retry from committed
        // offset)` at `bytestream_server.rs:1697-1709` — the Bazel
        // client's QueryWriteStatus → committed_size=0 → restart loop.
        let store_errored = stream_state.store_errored.load(Ordering::Acquire);
        let tx_pipe_broken = stream_state.tx.is_pipe_broken();
        if store_errored || tx_pipe_broken {
            // Mirror `graceful_finish`'s active_uploads accounting:
            // the upload is no longer active, so decrement the
            // counter. Do NOT add bytes to `partial_write_bytes`
            // (no IdleStream is created so there's nothing to
            // memory-pressure-evict). Drop `stream_state` here at
            // end-of-block so the `tx` Sender goes away alongside
            // the (possibly already-dead) `rx`.
            let drop_reason = if store_errored {
                "store_update_fut errored"
            } else {
                "tx pipe broken"
            };
            warn!(
                uuid = format!("{:032x}", uuid),
                bytes_received = self.bytes_received.load(Ordering::Acquire),
                drop_reason,
                "#418: discarding corrupt StreamState — \
                 next retry will restart fresh (or surface Unavailable on offset mismatch)"
            );
            active_uploads.remove(&uuid);
            self.metrics.active_uploads.fetch_sub(1, Ordering::Relaxed);
            return;
        }

        // Track the bytes this stream holds as partial write memory.
        let stream_bytes = self.bytes_received.load(Ordering::Acquire);
        self.partial_write_bytes
            .fetch_add(stream_bytes, Ordering::Relaxed);
        self.metrics
            .partial_write_bytes
            .fetch_add(stream_bytes, Ordering::Relaxed);

        // Mark stream as idle with current timestamp.
        // The global sweeper will clean it up after idle_stream_timeout.
        // This avoids spawning a task per stream, reducing overhead from O(n) to O(1).
        active_uploads_slot.1 = Some(IdleStream {
            stream_state,
            idle_since: Instant::now(),
        });
    }
}

/// Represents a stream that is in the "idle" state. this means it is not currently being used
/// by a client. If it is not used within a certain amount of time it will be removed from the
/// `active_uploads` map automatically by the global sweeper task.
#[derive(Debug)]
struct IdleStream {
    stream_state: StreamState,
    /// When this stream became idle. Used by the global sweeper to determine expiration.
    idle_since: Instant,
}

impl IdleStream {
    fn into_active_stream(
        self,
        bytes_received: Arc<AtomicU64>,
        instance_info: &InstanceInfo,
    ) -> ActiveStreamGuard {
        // Decrement partial_write_bytes since this stream is no longer idle.
        let stream_bytes = bytes_received.load(Ordering::Acquire);
        atomic_saturating_sub(&instance_info.partial_write_bytes, stream_bytes);
        atomic_saturating_sub(&instance_info.metrics.partial_write_bytes, stream_bytes);

        ActiveStreamGuard {
            stream_state: Some(self.stream_state),
            bytes_received,
            active_uploads: instance_info.active_uploads.clone(),
            metrics: instance_info.metrics.clone(),
            partial_write_bytes: instance_info.partial_write_bytes.clone(),
        }
    }
}

/// Spawn a background task to mirror a blob to a random connected worker
/// for OOM redundancy. Fire-and-forget: errors are logged, not propagated.
///
/// When `data` is `Some`, the blob data is sent directly (used by the oneshot
/// and BatchUpdateBlobs paths where data is already in hand). When `None`,
/// the blob is re-read from the store (used by the streaming write path for
/// small blobs only).
fn mirror_blob_to_worker(store: &Store, digest: DigestInfo, data: Option<Bytes>) {
    // WorkerProxyStore is the outermost wrapper on CAS stores when workers
    // are configured. inner_store() delegates through, so we use as_any()
    // on the immediate store driver to find it.
    if store
        .as_store_driver()
        .as_any()
        .downcast_ref::<WorkerProxyStore>()
        .is_none()
    {
        return;
    }

    // Skip zero-length blobs — no value in mirroring them.
    if digest.size_bytes() == 0 {
        return;
    }

    let store = store.clone();
    nativelink_util::background_spawn!("mirror_blob_to_worker", async move {
        let blob_data = if let Some(d) = data {
            d
        } else {
            // Streaming path: re-read from store since we don't have the data buffered.
            match store.get_part_unchunked(digest, 0, None).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(
                        %digest,
                        ?e,
                        "mirror: failed to read blob for mirroring"
                    );
                    return;
                }
            }
        };

        // Re-obtain the proxy reference (store is cloned, driver is Arc'd).
        let Some(proxy) = store
            .as_store_driver()
            .as_any()
            .downcast_ref::<WorkerProxyStore>()
        else {
            return;
        };

        proxy.mirror_blob_to_random_worker(digest, blob_data).await;
    });
}

#[derive(Debug)]
pub struct ByteStreamServer {
    instance_infos: HashMap<InstanceName, InstanceInfo>,
}

impl ByteStreamServer {
    /// Generate a unique UUID key by `XOR`ing the base key with a nanosecond timestamp.
    /// This ensures virtually zero collision probability while being O(1).
    fn generate_unique_uuid_key(base_key: UuidKey) -> UuidKey {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // XOR with timestamp to create unique key
        base_key ^ timestamp
    }

    pub fn new(
        configs: &[WithInstanceName<ByteStreamConfig>],
        store_manager: &StoreManager,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    ) -> Result<Self, Error> {
        let mut instance_infos: HashMap<String, InstanceInfo> = HashMap::new();
        for config in configs {
            let idle_stream_timeout = if config.persist_stream_on_disconnect_timeout == 0 {
                DEFAULT_PERSIST_STREAM_ON_DISCONNECT_TIMEOUT
            } else {
                Duration::from_secs(config.persist_stream_on_disconnect_timeout as u64)
            };
            let _old_value = instance_infos.insert(
                config.instance_name.clone(),
                Self::new_with_timeout(
                    config,
                    store_manager,
                    idle_stream_timeout,
                    small_blob_dispatcher.clone(),
                )?,
            );
        }
        Ok(Self { instance_infos })
    }

    /// **Test-only accessor** for the `InFlightBlobMap` belonging to
    /// the named instance. Used by #44 Layer C tests to inject a
    /// pre-populated `StreamingBlobInner` whose buffer holds bytes
    /// past `digest.size_bytes()` (a Layer-A bypass scenario), so the
    /// server-side `inner_read` unfold cap is exercised end-to-end.
    /// Production callers MUST go through the ByteStream Write RPC.
    ///
    /// Cfg-gated behind `test-utils` — production builds cannot reach
    /// the `InFlightBlobMap` directly. The Layer-A admission cap on the
    /// writer side is the only legitimate path that mutates this map.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn in_flight_blobs_for_test(
        &self,
        instance_name: &str,
    ) -> Option<Arc<nativelink_util::streaming_blob::InFlightBlobMap>> {
        self.instance_infos
            .get(instance_name)
            .map(|i| Arc::clone(&i.in_flight_blobs))
    }

    pub fn new_with_timeout(
        config: &WithInstanceName<ByteStreamConfig>,
        store_manager: &StoreManager,
        idle_stream_timeout: Duration,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    ) -> Result<InstanceInfo, Error> {
        let store = store_manager
            .get_store(&config.cas_store)
            .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", config.cas_store))?;
        let max_bytes_per_stream = if config.max_bytes_per_stream == 0 {
            DEFAULT_MAX_BYTES_PER_STREAM
        } else {
            if config.max_bytes_per_stream > 4 * 1024 * 1024 {
                warn!(
                    configured = config.max_bytes_per_stream,
                    default = DEFAULT_MAX_BYTES_PER_STREAM,
                    "max_bytes_per_stream exceeds 4 MiB; Bazel and other REAPI clients \
                     typically have a 4 MiB gRPC inbound message limit and will reject \
                     oversized ByteStream.Read chunks with RESOURCE_EXHAUSTED"
                );
            }
            config.max_bytes_per_stream
        };

        let active_uploads: Arc<Mutex<HashMap<UuidKey, BytesWrittenAndIdleStream>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let metrics = Arc::new(ByteStreamMetrics::default());
        let partial_write_bytes = Arc::new(AtomicU64::new(0));

        let max_partial_write_bytes = if config.max_partial_write_bytes == 0 {
            DEFAULT_MAX_PARTIAL_WRITE_BYTES
        } else {
            config.max_partial_write_bytes
        };

        // Spawn a single global sweeper task that periodically cleans up expired idle streams.
        // This replaces per-stream timeout tasks, reducing task spawn overhead from O(n) to O(1).
        let sweeper_active_uploads = Arc::downgrade(&active_uploads);
        let sweeper_metrics = Arc::downgrade(&metrics);
        let sweeper_partial_write_bytes = Arc::downgrade(&partial_write_bytes);
        let sweep_interval = idle_stream_timeout / 2; // Check every half-timeout period
        let sweeper_handle = spawn!("bytestream_idle_stream_sweeper", async move {
            loop {
                sleep(sweep_interval).await;

                let Some(active_uploads) = sweeper_active_uploads.upgrade() else {
                    // InstanceInfo has been dropped, exit the sweeper
                    break;
                };
                let metrics = sweeper_metrics.upgrade();
                let partial_bytes = sweeper_partial_write_bytes.upgrade();

                let now = Instant::now();
                let mut expired_count = 0u64;
                let mut expired_bytes = 0u64;

                // Pass 1: evict streams that exceeded idle_stream_timeout
                {
                    let mut uploads = active_uploads.lock();
                    uploads.retain(|uuid, (bytes_received, maybe_idle)| {
                        if let Some(idle_stream) = maybe_idle {
                            if now.duration_since(idle_stream.idle_since) >= idle_stream_timeout {
                                debug!(
                                    msg = "Sweeping expired idle stream",
                                    uuid = format!("{:032x}", uuid),
                                );
                                expired_bytes += bytes_received.load(Ordering::Acquire);
                                expired_count += 1;
                                return false; // Remove this entry
                            }
                        }
                        true // Keep this entry
                    });
                }

                // Update metrics for time-based evictions
                if expired_count > 0 {
                    if let Some(m) = &metrics {
                        m.idle_stream_timeouts
                            .fetch_add(expired_count, Ordering::Relaxed);
                        atomic_saturating_sub(&m.active_uploads, expired_count);
                        atomic_saturating_sub(&m.partial_write_bytes, expired_bytes);
                    }
                    if let Some(pb) = &partial_bytes {
                        atomic_saturating_sub(pb, expired_bytes);
                    }
                    trace!(
                        msg = "Sweeper cleaned up expired streams",
                        count = expired_count,
                    );
                }

                // Pass 2: memory-pressure eviction -- evict oldest idle streams
                // until partial_write_bytes <= max_partial_write_bytes.
                if max_partial_write_bytes > 0 {
                    let current_bytes = partial_bytes
                        .as_ref()
                        .map_or(0, |pb| pb.load(Ordering::Relaxed));
                    if current_bytes > max_partial_write_bytes {
                        let mut memory_evicted_count = 0u64;
                        let mut memory_evicted_bytes = 0u64;

                        // Collect idle streams with their idle_since for sorting.
                        let mut idle_entries: Vec<(UuidKey, Instant, u64)> = Vec::new();
                        {
                            let uploads = active_uploads.lock();
                            for (uuid, (bytes_received, maybe_idle)) in uploads.iter() {
                                if let Some(idle_stream) = maybe_idle {
                                    idle_entries.push((
                                        *uuid,
                                        idle_stream.idle_since,
                                        bytes_received.load(Ordering::Acquire),
                                    ));
                                }
                            }
                        }

                        // Sort by idle_since ascending (oldest first).
                        idle_entries.sort_by_key(|&(_, idle_since, _)| idle_since);

                        let mut remaining_bytes = current_bytes;
                        let mut uuids_to_evict = Vec::new();
                        for (uuid, _, stream_bytes) in &idle_entries {
                            if remaining_bytes <= max_partial_write_bytes {
                                break;
                            }
                            uuids_to_evict.push(*uuid);
                            memory_evicted_bytes += stream_bytes;
                            remaining_bytes = remaining_bytes.saturating_sub(*stream_bytes);
                            memory_evicted_count += 1;
                        }

                        // Remove the selected entries. Re-check that each
                        // stream is still idle — it may have been resumed
                        // between the two lock acquisitions.
                        if !uuids_to_evict.is_empty() {
                            let mut uploads = active_uploads.lock();
                            let mut actually_evicted = 0u64;
                            let mut actually_evicted_bytes = 0u64;
                            for uuid in &uuids_to_evict {
                                if let Some((bytes_counter, maybe_idle)) = uploads.get(uuid) {
                                    if maybe_idle.is_some() {
                                        let bytes = bytes_counter.load(Ordering::Acquire);
                                        uploads.remove(uuid);
                                        actually_evicted += 1;
                                        actually_evicted_bytes += bytes;
                                    }
                                    // else: stream was resumed, skip it
                                }
                            }
                            memory_evicted_count = actually_evicted;
                            memory_evicted_bytes = actually_evicted_bytes;
                        }

                        if memory_evicted_count > 0 {
                            warn!(
                                evicted = memory_evicted_count,
                                evicted_bytes = memory_evicted_bytes,
                                budget = max_partial_write_bytes,
                                remaining = remaining_bytes,
                                "memory-pressure eviction triggered for idle streams",
                            );
                            if let Some(pb) = &partial_bytes {
                                atomic_saturating_sub(pb, memory_evicted_bytes);
                            }
                            if let Some(m) = &metrics {
                                atomic_saturating_sub(&m.partial_write_bytes, memory_evicted_bytes);
                                m.idle_stream_evictions_memory
                                    .fetch_add(memory_evicted_count, Ordering::Relaxed);
                                atomic_saturating_sub(&m.active_uploads, memory_evicted_count);
                            }
                        }
                    }
                }
            }
        });

        let max_streaming_blob_buffer_bytes = if config.max_streaming_blob_buffer_bytes == 0 {
            64 * 1024 * 1024 // 64 MiB default
        } else {
            config.max_streaming_blob_buffer_bytes as u64
        };

        Ok(InstanceInfo {
            store,
            cas_store_name_arc: Arc::from(config.cas_store.as_str()),
            small_blob_dispatcher,
            max_bytes_per_stream,
            active_uploads,
            idle_stream_timeout,
            metrics,
            _sweeper_handle: Arc::new(sweeper_handle),
            in_flight_writes: Arc::new(Mutex::new(HashMap::new())),
            in_flight_blobs: Arc::new(InFlightBlobMap::with_max_entries(
                nativelink_util::streaming_blob::DEFAULT_MAX_IN_FLIGHT_BLOBS,
            )),
            streaming_read_while_write: config.streaming_read_while_write,
            max_streaming_blob_buffer_bytes,
            max_partial_write_bytes,
            partial_write_bytes,
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    /// Wrap this server in a `ZeroCopyByteStreamService` that intercepts Write
    /// RPCs and decodes `WriteRequest` messages directly from HTTP body frames,
    /// bypassing tonic's `BytesMut` reassembly buffer.
    ///
    /// Read and QueryWriteStatus RPCs delegate to the standard tonic path.
    pub fn into_zero_copy_service(
        self,
        max_decoding_message_size: usize,
        max_encoding_message_size: usize,
    ) -> ZeroCopyByteStreamService {
        let inner = Arc::new(self);
        ZeroCopyByteStreamService {
            inner: inner.clone(),
            tonic_service: Server::from_arc(inner)
                .max_decoding_message_size(max_decoding_message_size)
                .max_encoding_message_size(max_encoding_message_size),
        }
    }

    /// Creates or joins an upload stream for the given UUID.
    ///
    /// This function handles three scenarios:
    /// 1. UUID doesn't exist - creates a new upload stream
    /// 2. UUID exists but is idle - resumes the existing stream
    /// 3. UUID exists and is active - generates a unique UUID by appending a nanosecond
    ///    timestamp to avoid collision, then creates a new stream with that UUID
    ///
    /// The nanosecond timestamp ensures virtually zero probability of collision since
    /// two concurrent uploads would need to both collide on the original UUID AND
    /// generate the unique UUID in the exact same nanosecond.
    fn create_or_join_upload_stream(
        &self,
        uuid_str: &str,
        instance: &InstanceInfo,
        digest: DigestInfo,
    ) -> ActiveStreamGuard {
        // Parse UUID string to u128 key for efficient HashMap operations
        let uuid_key = parse_uuid_to_key(uuid_str);

        // We handle the three cases in two phases to avoid holding the
        // mutex guard across a second .lock() call (which would deadlock
        // on parking_lot::Mutex since it is not reentrant).
        enum UploadAction {
            Resume(Box<ActiveStreamGuard>),
            New(u128, Arc<AtomicU64>),
            Collision(u128),
        }

        let action = {
            let mut active_uploads = instance.active_uploads.lock();
            match active_uploads.entry(uuid_key) {
                Entry::Occupied(mut entry) => {
                    let maybe_idle_stream = entry.get_mut();
                    if let Some(idle_stream) = maybe_idle_stream.1.take() {
                        // Case 2: Stream exists but is idle — verify the digest
                        // matches before resuming. A UUID reuse with a different
                        // digest would send wrong data to the original store update.
                        if idle_stream.stream_state.digest != digest {
                            // Decrement partial_write_bytes for the discarded idle stream.
                            let stale_bytes = maybe_idle_stream.0.load(Ordering::Acquire);
                            atomic_saturating_sub(&instance.partial_write_bytes, stale_bytes);
                            atomic_saturating_sub(&instance.metrics.partial_write_bytes, stale_bytes);
                            warn!(
                                uuid = format!("{:032x}", uuid_key),
                                original_digest = %idle_stream.stream_state.digest,
                                new_digest = %digest,
                                "Idle stream digest mismatch — discarding stale \
                                 stream and creating new one"
                            );
                            drop(idle_stream);
                            let bytes_received = Arc::new(AtomicU64::new(0));
                            *maybe_idle_stream = (bytes_received.clone(), None);
                            UploadAction::New(uuid_key, bytes_received)
                        } else {
                            let bytes_received = maybe_idle_stream.0.clone();
                            debug!(
                                msg = "Joining existing stream",
                                uuid = format!("{:032x}", entry.key())
                            );
                            instance
                                .metrics
                                .resumed_uploads
                                .fetch_add(1, Ordering::Relaxed);
                            UploadAction::Resume(Box::new(
                                idle_stream.into_active_stream(bytes_received, instance),
                            ))
                        }
                    } else {
                        // Case 3: Stream is active - generate a unique UUID to avoid collision
                        let original_key = *entry.key();
                        let unique_key = Self::generate_unique_uuid_key(original_key);
                        warn!(
                            msg = "UUID collision detected, generating unique UUID to prevent conflict",
                            original_uuid = format!("{:032x}", original_key),
                            unique_uuid = format!("{:032x}", unique_key)
                        );
                        UploadAction::Collision(unique_key)
                    }
                }
                Entry::Vacant(entry) => {
                    // Case 1: UUID doesn't exist, create new stream
                    let bytes_received = Arc::new(AtomicU64::new(0));
                    let uuid = *entry.key();
                    entry.insert((bytes_received.clone(), None));
                    UploadAction::New(uuid, bytes_received)
                }
            }
        }; // First lock guard dropped here.

        let (uuid, bytes_received, is_collision) = match action {
            UploadAction::Resume(guard) => return *guard,
            UploadAction::New(uuid, bytes_received) => (uuid, bytes_received, false),
            UploadAction::Collision(unique_key) => {
                let bytes_received = Arc::new(AtomicU64::new(0));
                let mut active_uploads = instance.active_uploads.lock();
                active_uploads.insert(unique_key, (bytes_received.clone(), None));
                (unique_key, bytes_received, true)
            }
        };

        // Track metrics for new upload
        instance
            .metrics
            .active_uploads
            .fetch_add(1, Ordering::Relaxed);
        if is_collision {
            instance
                .metrics
                .uuid_collisions
                .fetch_add(1, Ordering::Relaxed);
        }

        // Important: Do not return an error from this point onwards without
        // removing the entry from the map, otherwise that UUID becomes
        // unusable.

        // Use a larger buffer (256 slots = ~64MiB at 256KiB chunks) to sustain
        // high-throughput streaming at 10Gbps+ without backpressure stalls.
        let (tx, rx) = make_buf_channel_pair_with_size(256);
        let store = instance.store.clone();
        // #418: paused-vs-completed discriminator for the wrapped
        // `store.update()` future. `StreamState` is resumable iff its
        // future is paused mid-await AND its `tx` is connected to a
        // live `rx`. The two end-states this flag distinguishes:
        //
        // - **Paused (cancelled before completion).** `try_join!`
        //   dropped the future at its current await point — neither
        //   the assignment to `result` nor the `if` below runs, so
        //   `store_errored` stays `false`. The captured `rx` lives on
        //   inside the future's frame; the stashed `IdleStream` is
        //   safe to resume. This is the legitimate Bazel TCP-flap
        //   recovery path.
        // - **Completed-with-Err.** `store.update()` returned `Err`;
        //   the body runs to the `if` and flips the flag. `rx` is
        //   dropped at end-of-scope, so `tx` now points at a dead
        //   receiver. `Drop` reads the flag and DISCARDS the entry
        //   instead of recycling a corrupt StreamState.
        //
        // The `tx.is_pipe_broken()` check at the drop site is a
        // belt-and-suspenders guard for any future failure mode that
        // breaks the channel without going through this future's Err
        // path; it is NOT load-bearing for the present
        // completed-with-Err class.
        let store_errored = Arc::new(AtomicBool::new(false));
        let store_errored_for_fut = Arc::clone(&store_errored);
        let store_update_fut = Box::pin(async move {
            // We need to wrap `Store::update()` in a another future because we need to capture
            // `store` to ensure its lifetime follows the future and not the caller.
            let result = store
                // Bytestream always uses digest size as the actual byte size.
                .update(digest, rx, UploadSizeInfo::ExactSize(digest.size_bytes()))
                .await;
            // #418: only set on the completed-with-Err transition.
            // Cancellation (try_join! dropping this future before
            // `store.update()` returns) never reaches this line.
            if result.is_err() {
                store_errored_for_fut.store(true, Ordering::Release);
            }
            result
        });
        ActiveStreamGuard {
            stream_state: Some(StreamState {
                uuid,
                digest,
                tx,
                store_update_fut,
                store_errored,
            }),
            bytes_received,
            active_uploads: instance.active_uploads.clone(),
            metrics: instance.metrics.clone(),
            partial_write_bytes: instance.partial_write_bytes.clone(),
        }
    }

    async fn inner_read(
        &self,
        instance: &InstanceInfo,
        digest: DigestInfo,
        read_request: ReadRequest,
        is_worker: bool,
    ) -> Result<ReadStream, Error> {
        debug!(
            %digest,
            read_offset = read_request.read_offset,
            read_limit = read_request.read_limit,
            resource_name = %read_request.resource_name,
            is_worker,
            "ByteStream::inner_read entry",
        );
        // Check InFlightBlobMap first: if the blob is currently being
        // written, stream from the in-memory buffer instead of waiting
        // for the store commit. Skip errored entries — they represent
        // failed writes whose stale map entries haven't been cleaned up
        // yet. Falling through to the store read will serve the blob
        // from CAS if it was written by a concurrent/retry upload.
        if instance.streaming_read_while_write {
            if let Some(mut streaming_reader) = instance.in_flight_blobs.get_reader(&digest) {
                if streaming_reader.inner().has_error() {
                    info!(
                        %digest,
                        "inner_read: skipping errored in-flight blob, falling back to store"
                    );
                    // Remove the poisoned entry so future reads don't hit it.
                    if let Some(inner_arc) = instance.in_flight_blobs.get_inner(&digest) {
                        instance.in_flight_blobs.remove(&digest, &inner_arc);
                    }
                } else if streaming_reader.inner().earliest_chunk_idx() > 0 {
                    // Sliding window evicted early chunks — can't serve
                    // a full blob read from the beginning.  Fall through
                    // to the store read path.  Remove the entry so
                    // subsequent readers go straight to the store instead
                    // of hitting this branch and logging again.
                    info!(
                        %digest,
                        earliest_chunk_idx = streaming_reader.inner().earliest_chunk_idx(),
                        "inner_read: streaming blob window evicted early data, falling back to store"
                    );
                    if let Some(inner_arc) = instance.in_flight_blobs.get_inner(&digest) {
                        instance.in_flight_blobs.remove(&digest, &inner_arc);
                    }
                } else {
                info!(
                    %digest,
                    "inner_read: serving from in-flight streaming blob"
                );
                let max_bytes = instance.max_bytes_per_stream;
                let read_offset = u64::try_from(read_request.read_offset)
                    .err_tip(|| "Could not convert read_offset to u64")?;
                let read_limit = u64::try_from(read_request.read_limit)
                    .err_tip(|| "Could not convert read_limit to u64")?;
                let read_limit = if read_limit != 0 {
                    Some(read_limit)
                } else {
                    None
                };
                // #44 Layer C — emission cap at `digest.size_bytes()`.
                // Bazel's parallel-chunk Read(offset=N, limit=0) shape
                // (`fast_slow_store.rs:6709`) does NOT set `read_limit`,
                // so the read_limit trim below cannot guard against an
                // in-flight streaming buffer that holds more than the
                // declared digest size. Layer A on the producer side
                // closes the input; Layer C is the symmetric defense at
                // the server-side unfold, so a Layer-A regression OR an
                // alternate-producer path that pushed past the cap is
                // truncated at the wire-shape boundary instead of
                // bleeding `N + Δ` bytes to Bazel as pipeline-2487 did.
                let digest_size = digest.size_bytes();

                // State: (reader, bytes_sent, read_offset, read_limit, max_bytes, leftover)
                // `leftover` carries the unconsumed tail of a chunk that was
                // larger than max_bytes_per_stream, so we don't lose data
                // when splitting large streaming chunks into gRPC responses.
                let stream = unfold(
                    (streaming_reader, 0u64, read_offset, read_limit, max_bytes, Bytes::new()),
                    move |(mut reader, mut bytes_sent, read_offset, read_limit, max_bytes, mut leftover)| async move {
                        // Helper: given a usable Bytes slice, apply
                        // read_limit, digest-size (#44 Layer C), and
                        // max_bytes trimming, update bytes_sent, and
                        // return the response plus any leftover.
                        #[inline]
                        fn emit(
                            mut data: Bytes,
                            bytes_sent: &mut u64,
                            read_offset: u64,
                            read_limit: Option<u64>,
                            digest_size: u64,
                            max_bytes: usize,
                        ) -> (Bytes, Bytes) {
                            // Trim to read_limit if needed.
                            if let Some(limit) = read_limit {
                                let new_effective =
                                    (*bytes_sent + data.len() as u64) - read_offset;
                                if new_effective > limit {
                                    let overshoot = (new_effective - limit) as usize;
                                    data = data.slice(..data.len() - overshoot);
                                }
                            }

                            // #44 Layer C — trim to digest.size_bytes()
                            // if the chunk would extend the response
                            // past the declared blob size.
                            let projected_end =
                                *bytes_sent + data.len() as u64;
                            if projected_end > digest_size {
                                let overshoot =
                                    (projected_end - digest_size) as usize;
                                if overshoot >= data.len() {
                                    data = Bytes::new();
                                } else {
                                    data = data.slice(..data.len() - overshoot);
                                }
                            }

                            // Trim to max_bytes_per_stream, carrying leftover.
                            let lo = if data.len() > max_bytes {
                                let remainder = data.slice(max_bytes..);
                                data = data.slice(..max_bytes);
                                remainder
                            } else {
                                Bytes::new()
                            };

                            *bytes_sent += data.len() as u64;
                            (data, lo)
                        }

                        // #44 Layer C — fast-path cap: if a previous
                        // iteration already filled the response to
                        // `digest.size_bytes()`, end the stream cleanly
                        // before consulting the reader/leftover.
                        if bytes_sent >= digest_size && bytes_sent >= read_offset {
                            return None;
                        }

                        // Skip bytes before read_offset.
                        while bytes_sent < read_offset {
                            match reader.next_chunk().await {
                                Ok(chunk) if chunk.is_empty() => return None, // EOF
                                Ok(chunk) => {
                                    let chunk_end = bytes_sent + chunk.len() as u64;
                                    if chunk_end > read_offset {
                                        // Partial overlap — slice into the relevant portion.
                                        let skip = (read_offset - bytes_sent) as usize;
                                        let usable = chunk.slice(skip..);
                                        bytes_sent = chunk_end;

                                        // Apply read_limit.
                                        let effective = bytes_sent - read_offset;
                                        if let Some(limit) = read_limit {
                                            if effective >= limit {
                                                let trim = (effective - limit) as usize;
                                                let final_chunk = if trim > 0 && trim < usable.len()
                                                {
                                                    usable.slice(..usable.len() - trim)
                                                } else {
                                                    usable
                                                };
                                                if final_chunk.is_empty() {
                                                    return None;
                                                }
                                                // Re-adjust bytes_sent to match actual position.
                                                bytes_sent = read_offset + final_chunk.len() as u64;
                                                let resp = ReadResponse { data: final_chunk };
                                                return Some((
                                                    Ok(resp),
                                                    (
                                                        reader,
                                                        bytes_sent,
                                                        read_offset,
                                                        read_limit,
                                                        max_bytes,
                                                        Bytes::new(),
                                                    ),
                                                ));
                                            }
                                        }

                                        // Respect max_bytes_per_stream, carry leftover.
                                        // Reset bytes_sent to accurate position before emit.
                                        bytes_sent = read_offset;
                                        let (data, lo) = emit(usable, &mut bytes_sent, read_offset, read_limit, digest_size, max_bytes);
                                        if data.is_empty() {
                                            return None;
                                        }
                                        let resp = ReadResponse { data };
                                        return Some((
                                            Ok(resp),
                                            (
                                                reader,
                                                bytes_sent,
                                                read_offset,
                                                read_limit,
                                                max_bytes,
                                                lo,
                                            ),
                                        ));
                                    }
                                    bytes_sent = chunk_end;
                                    continue;
                                }
                                Err(e) => {
                                    return Some((
                                        Err(e.into()),
                                        (reader, bytes_sent, read_offset, read_limit, max_bytes, leftover),
                                    ));
                                }
                            }
                        }

                        // Check read_limit.
                        let effective_sent = bytes_sent - read_offset;
                        if let Some(limit) = read_limit {
                            if effective_sent >= limit {
                                return None;
                            }
                        }

                        // Use leftover from a previous oversized chunk before
                        // reading the next chunk from the streaming blob.
                        let chunk = if !leftover.is_empty() {
                            let lo = core::mem::take(&mut leftover);
                            Ok(lo)
                        } else {
                            reader.next_chunk().await
                        };

                        match chunk {
                            Ok(data) if data.is_empty() => None, // EOF
                            Ok(data) => {
                                let (data, lo) = emit(data, &mut bytes_sent, read_offset, read_limit, digest_size, max_bytes);

                                if data.is_empty() {
                                    return None;
                                }

                                let resp = ReadResponse { data };
                                Some((
                                    Ok(resp),
                                    (reader, bytes_sent, read_offset, read_limit, max_bytes, lo),
                                ))
                            }
                            Err(e) => Some((
                                Err(e.into()),
                                (reader, bytes_sent, read_offset, read_limit, max_bytes, leftover),
                            )),
                        }
                    },
                );

                return Ok(Box::pin(stream) as ReadStream);
            } // else (not errored)
            } // if let Some(streaming_reader)
        }

        // mark expected_drop: the unfold's `state` holds `rx` between
        // poll cycles. When the gRPC client cancels mid-stream (Bazel
        // build interrupt, retry, peer reset, etc.) the unfold's state
        // is dropped externally without observing EOF; the producer
        // (`get_part_fut` below, owning `tx`) is still alive and will
        // see the next `tx.send().await` fail with "receiver
        // disconnected". That sender-side error is already loud and
        // accurate — the receiver-side `buf_channel::receiver_dropped_mid_stream`
        // warn would be redundant noise (~495 events/10min on buildcache
        // 2026-05-07 review M1/S4). The `ExpectedDropRx` wrapper marks
        // `rx` on every Drop path so the centralized one-line wrapper
        // covers every exit uniformly:
        //   - normal EOF return at the `consume_ok_eof` branch (warn
        //     already suppressed by `eof_sent`; mark is a no-op duplicate)
        //   - server-detected size-too-large (state dropped at end of
        //     closure; producer error already surfaces via the
        //     `Err((... into()))` tuple item)
        //   - `consume_err` propagation (`last_err` already set inside
        //     rx, warn already suppressed; mark is a no-op duplicate)
        //   - external client cancellation (the load-bearing case)
        //
        // We can't put Drop on `ReaderState` itself because the closure
        // moves `state.maybe_get_part_result` and `state.get_part_fut`
        // out of the struct — Rust's E0509 forbids moves out of a Drop
        // type. Wrapping just `rx` keeps the rest of `ReaderState`
        // movable while preserving the mark-on-drop guarantee.
        struct ExpectedDropRx(DropCloserReadHalf);
        impl Drop for ExpectedDropRx {
            fn drop(&mut self) {
                self.0.mark_expected_drop();
            }
        }
        impl core::ops::Deref for ExpectedDropRx {
            type Target = DropCloserReadHalf;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl core::ops::DerefMut for ExpectedDropRx {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        struct ReaderState {
            max_bytes_per_stream: usize,
            rx: ExpectedDropRx,
            maybe_get_part_result: Option<Result<(), Error>>,
            get_part_fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
            // #44 Layer C — at-rest unfold cap state.
            // `bytes_sent_in_blob` tracks the absolute blob offset of
            // the next byte we will emit. Initialized to
            // `read_request.read_offset`; the store is invoked with the
            // same `read_offset` so `state.rx.consume()` yields bytes
            // starting at that absolute position. After each emission
            // we advance by the chunk length and refuse to emit past
            // `digest.size_bytes()` even if the underlying store
            // wrapper retained leftover Δ bytes (pipeline-2487 shape:
            // four `Read(offset=0, limit=0)` responses returned
            // `expected_size + delta` bytes; the unfold MUST truncate
            // at the digest size to close the wire-shape independently
            // of upstream-store correctness). See
            // `.claude/audits/45-pipeline-2487-overshoot-trigger-2026-06-04.md`.
            bytes_sent_in_blob: u64,
            digest_size: u64,
        }

        let read_limit = u64::try_from(read_request.read_limit)
            .err_tip(|| "Could not convert read_limit to u64")?;

        // Use a larger buffer (256 slots = ~64MiB at 256KiB chunks) to sustain
        // high-throughput streaming at 10Gbps+ without backpressure stalls.
        let (tx, rx) = make_buf_channel_pair_with_size(256);

        let read_limit = if read_limit != 0 {
            Some(read_limit)
        } else {
            None
        };

        let read_offset_u64 = u64::try_from(read_request.read_offset)
            .err_tip(|| "Could not convert read_offset to u64")?;

        // This allows us to call a destructor when the the object is dropped.
        let store = instance.store.clone();
        let state = Some(ReaderState {
            rx: ExpectedDropRx(rx),
            max_bytes_per_stream: instance.max_bytes_per_stream,
            maybe_get_part_result: None,
            bytes_sent_in_blob: read_offset_u64,
            digest_size: digest.size_bytes(),
            get_part_fut: Box::pin(async move {
                // Propagate the worker/non-worker distinction into the store
                // layer so WorkerProxyStore can decide whether to proxy or
                // redirect.
                IS_WORKER_REQUEST
                    .scope(is_worker, async {
                        store
                            .get_part(
                                digest,
                                tx,
                                read_offset_u64,
                                read_limit,
                            )
                            .await
                    })
                    .await
            }),
        });

        let read_stream_span = error_span!("read_stream");
        let entry_time = Instant::now();

        Ok(Box::pin(unfold(state, move |state| {
            async move {
            let mut state: ReaderState = state?; // If None our stream is done.
            let mut response = ReadResponse::default();
            {
                let consume_fut = state.rx.consume(Some(state.max_bytes_per_stream));
                tokio::pin!(consume_fut);
                debug!(
                    %digest,
                    branch = "consume_await_start",
                    elapsed_ms = entry_time.elapsed().as_millis() as u64,
                    "inner_read awaiting consume_fut",
                );
                loop {
                    tokio::select! {
                        read_result = &mut consume_fut => {
                            match read_result {
                                Ok(bytes) => {
                                    if bytes.is_empty() {
                                        // Symmetric with the consume_err branch at :1796-1816:
                                        // an upstream `get_part_fut` Err may have arrived via
                                        // the select arm at :1860 while `consume_fut` was
                                        // racing toward EOF. The `tx.send_eof()`-then-`Err`
                                        // shape from the producer surfaces here as
                                        // `Ok(empty)` (legitimate-EOF), not `consume_err`.
                                        // Without this check, the upstream error is silently
                                        // swallowed and Bazel sees status=ok with
                                        // bytes_sent=0 (#500 production-firing site;
                                        // BulkTransferException digest mismatches).
                                        //
                                        // `.take()` is safe: we `return Some(...)` on the
                                        // Err path, so the consume_err branch's later read of
                                        // `maybe_get_part_result` at :1800 is unreachable for
                                        // this stream iteration. Both branches are inside the
                                        // same `tokio::select!` match and are mutually
                                        // exclusive per iteration.
                                        if let Some(Err(err)) = state.maybe_get_part_result.take() {
                                            warn!(
                                                target: "nativelink_service::bytestream_server",
                                                %digest,
                                                branch = "consume_ok_eof_with_get_part_err",
                                                code = ?err.code,
                                                elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                                "inner_read EOF observed but get_part_fut had errored; propagating upstream error instead of silent EOF (#500)",
                                            );
                                            return Some((Err(err.into()), None));
                                        }
                                        debug!(
                                            %digest,
                                            branch = "consume_ok_eof",
                                            elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                            "inner_read consume returned empty (EOF)",
                                        );
                                        return None;
                                    }
                                    if bytes.len() > state.max_bytes_per_stream {
                                        let err = make_err!(Code::Internal, "Returned store size was larger than read size");
                                        return Some((Err(err.into()), None));
                                    }
                                    // #44 Layer C — at-rest unfold cap.
                                    // Cap emission at `digest.size_bytes()`
                                    // even if the underlying store wrapper
                                    // retained Δ bytes past the declared
                                    // blob size. Closes the at-rest path of
                                    // the pipeline-2487 wire-shape
                                    // independently of upstream-store
                                    // correctness. Mirrors the streaming-
                                    // branch unfold cap at `:1493`'s
                                    // `emit()` digest-size trim. See
                                    // `.claude/audits/45-pipeline-2487-overshoot-trigger-2026-06-04.md`.
                                    let mut bytes = bytes;
                                    let remaining = state.digest_size
                                        .saturating_sub(state.bytes_sent_in_blob);
                                    if (bytes.len() as u64) > remaining {
                                        let keep = usize::try_from(remaining)
                                            .unwrap_or(usize::MAX)
                                            .min(bytes.len());
                                        let dropped = bytes.len() - keep;
                                        warn!(
                                            %digest,
                                            branch = "at_rest_overshoot_cap",
                                            bytes_sent_in_blob = state.bytes_sent_in_blob,
                                            digest_size = state.digest_size,
                                            chunk_len = bytes.len(),
                                            dropped,
                                            "inner_read truncated at-rest store chunk to digest.size_bytes() (Layer C cap); upstream store retained Δ bytes past declared size",
                                        );
                                        bytes = bytes.slice(..keep);
                                        if bytes.is_empty() {
                                            // Already past digest boundary — end stream cleanly.
                                            return None;
                                        }
                                    }
                                    state.bytes_sent_in_blob =
                                        state.bytes_sent_in_blob
                                            .saturating_add(bytes.len() as u64);
                                    let bytes_len = bytes.len();
                                    response.data = bytes;
                                    trace!(response.data = format!("<redacted len({})>", response.data.len()));
                                    debug!(
                                        %digest,
                                        branch = "consume_ok",
                                        bytes_len,
                                        elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                        "inner_read consume returned chunk",
                                    );
                                    break;
                                }
                                Err(mut e) => {
                                    info!(
                                        %digest,
                                        branch = "consume_err",
                                        code = ?e.code,
                                        elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                        "inner_read consume returned error",
                                    );
                                    // We may need to propagate the error from reading the data through first.
                                    // For example, the NotFound error will come through `get_part_fut`, and
                                    // will not be present in `e`, but we need to ensure we pass NotFound error
                                    // code or the client won't know why it failed.
                                    let get_part_result = if let Some(result) = state.maybe_get_part_result {
                                        result
                                    } else {
                                        // This should never be `future::pending()` if maybe_get_part_result is
                                        // not set.
                                        state.get_part_fut.await
                                    };
                                    info!(
                                        %digest,
                                        branch = "get_part_resolved_after_consume_err",
                                        is_err = get_part_result.is_err(),
                                        elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                        "inner_read get_part_fut resolved after consume_err",
                                    );
                                    if let Err(err) = get_part_result {
                                        e = err.merge(e);
                                    }
                                    if e.code == Code::NotFound {
                                        // Trim the error code. Not Found is quite common and we don't want to send a large
                                        // error (debug) message for something that is common. We resize to just the last
                                        // message as it will be the most relevant.
                                        e.messages.truncate(1);
                                    }
                                    // Use appropriate log level: redirects and not-found are
                                    // expected protocol behavior, not errors.
                                    let is_redirect = e.code == Code::FailedPrecondition
                                        && e.messages.iter().any(|m| m.contains(REDIRECT_PREFIX));
                                    if is_redirect {
                                        // Redirects always produce a "Sender dropped before
                                        // sending EOF" artifact because get_part returns an
                                        // error (dropping tx) instead of streaming data. Trim
                                        // to just the redirect message for a clean response.
                                        e.messages.truncate(1);
                                        // #250: demoted from info! to debug!. NL_REDIRECT
                                        // FailedPrecondition is high-volume in production
                                        // (every Bazel-CAS Read for a blob the server hands
                                        // off to a worker peer) and previously contributed
                                        // to log-rate-driven OOMs (#186 false-alarm bursts,
                                        // #197 phantom-blob bursts, #253 / #255 backfill
                                        // bursts). The redirect is normal protocol behavior,
                                        // not a state transition or an anomaly — debug! is
                                        // the right level.
                                        debug!(response = ?e);
                                    } else if e.code == Code::NotFound {
                                        info!(response = ?e);
                                    } else {
                                        error!(response = ?e);
                                    }
                                    return Some((Err(e.into()), None))
                                }
                            }
                        },
                        result = &mut state.get_part_fut => {
                            debug!(
                                %digest,
                                branch = "get_part_done",
                                is_err = result.is_err(),
                                elapsed_ms = entry_time.elapsed().as_millis() as u64,
                                "inner_read get_part_fut resolved (still awaiting consume)",
                            );
                            state.maybe_get_part_result = Some(result);
                            // It is non-deterministic on which future will finish in what order.
                            // It is also possible that the `state.rx.consume()` call above may not be able to
                            // respond even though the publishing future is done.
                            // Because of this we set the writing future to pending so it never finishes.
                            // The `state.rx.consume()` future will eventually finish and return either the
                            // data or an error.
                            // An EOF will terminate the `state.rx.consume()` future, but we are also protected
                            // because we are dropping the writing future, it will drop the `tx` channel
                            // which will eventually propagate an error to the `state.rx.consume()` future if
                            // the EOF was not sent due to some other error.
                            state.get_part_fut = Box::pin(pending());
                        },
                    }
                }
            }
            Some((Ok(response), Some(state)))
        }.instrument(read_stream_span.clone())
        })) as ReadStream)
    }

    // We instrument tracing here as well as below because `stream` has a hash on it
    // that is extracted from the first stream message. If we only implemented it below
    // we would not have the hash available to us.
    #[instrument(
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip(self, instance_info),
    )]
    async fn inner_write(
        &self,
        instance_info: &InstanceInfo,
        digest: DigestInfo,
        stream: WriteRequestStreamWrapper<impl Stream<Item = Result<WriteRequest, Status>> + Unpin>,
        is_worker: bool,
        is_mirror: bool,
        // Slow-producer immunity: bumped after each successful chunk
        // recv so the StallGuard can suppress dumps when the server is
        // correctly waiting on a paused client. See
        // `.claude/audits/stall-cluster-2026-05-13-1941-1945.md`.
        progress_handle: Arc<AtomicU64>,
    ) -> Result<Response<WriteResponse>, Error> {
        async fn process_client_stream(
            mut stream: WriteRequestStreamWrapper<
                impl Stream<Item = Result<WriteRequest, Status>> + Unpin,
            >,
            tx: &mut DropCloserWriteHalf,
            mirror_tx: &mut Option<DropCloserWriteHalf>,
            mirror_dropped_any: &mut bool,
            metrics: &ByteStreamMetrics,
            streaming_blob_writer: &Option<StreamingBlobWriter>,
            outer_bytes_received: &Arc<AtomicU64>,
            // #320: outward-visible flag set the moment we observe a
            // `finish_write: true` chunk from the client, regardless of
            // whether subsequent validation succeeds. The outer warn
            // reports it so an operator can distinguish "client sent
            // finish_write but we rejected the byte count" (size mismatch
            // / extra-bytes / etc.) from "client closed the stream
            // without ever signaling completion" (the #320 case — the
            // gRPC stream returned None mid-upload). Without this flag,
            // both classes look identical in the existing warn.
            finish_write_seen: &mut bool,
            expected_size: u64,
            // Slow-producer immunity: bumped on each successful chunk
            // recv to mark forward server-side progress; the
            // StallGuard's dump trigger checks recency before firing.
            progress_handle: &Arc<AtomicU64>,
        ) -> Result<(), Error> {
            loop {
                // No app-layer recv timer. No-progress detection is layered
                // on the transport: h2 keepalive (30s/20s configured), TCP
                // keepalive (OS-level), QUIC keepalive (5s configured). When
                // those fire, `stream.next()` resolves to `Ok(Some(Err(...)))`
                // or `Ok(None)` and the loop exits. Sender-drop on connection
                // close is observed by `buf_channel`. See the lifecycle
                // doc-comment near `_stall_guard` in `write` for the full
                // no-progress story and the rationale for not adding an
                // app-layer timer (CLAUDE.md "Fix root causes, not symptoms";
                // "Falsify before fixing").
                let write_request = match stream.next().await {
                    // Code path for when client tries to gracefully close the stream.
                    // If this happens it means there's a problem with the data sent,
                    // because we always close the stream from our end before this point
                    // by counting the number of bytes sent from the client. If they send
                    // less than the amount they said they were going to send and then
                    // close the stream, we know there's a problem.
                    None => {
                        return Err(make_input_err!(
                            "Client closed stream before sending all data"
                        ));
                    }
                    // Code path for client stream error. Probably client disconnect.
                    Some(Err(err)) => return Err(err),
                    // Code path for received chunk of data.
                    Some(Ok(write_request)) => write_request,
                };
                // Mark forward server-side progress: a chunk arrived and
                // we're about to process it. The StallGuard observes this
                // to suppress stack dumps when the only "stall" is a
                // paused client (see
                // `.claude/audits/stall-cluster-2026-05-13-1941-1945.md`).
                nativelink_util::stall_detector::bump_progress_handle(progress_handle);

                if write_request.write_offset < 0 {
                    return Err(make_input_err!(
                        "Invalid negative write offset in write request: {}",
                        write_request.write_offset
                    ));
                }
                let write_offset = write_request.write_offset as u64;

                // If we get duplicate data because a client didn't know where
                // it left off from, then we can simply skip it.
                let data = if write_offset < tx.get_bytes_written() {
                    if (write_offset + write_request.data.len() as u64) < tx.get_bytes_written() {
                        if write_request.finish_write {
                            return Err(make_input_err!(
                                "Resumed stream finished at {} bytes when we already received {} bytes.",
                                write_offset + write_request.data.len() as u64,
                                tx.get_bytes_written()
                            ));
                        }
                        continue;
                    }
                    write_request.data.slice(
                        usize::try_from(tx.get_bytes_written() - write_offset)
                            .unwrap_or(usize::MAX)..,
                    )
                } else {
                    if write_offset != tx.get_bytes_written() {
                        // The client is trying to resume at an offset we
                        // don't have (e.g. the idle stream was swept).
                        // Return UNAVAILABLE so the client retries with
                        // QueryWriteStatus → committed_size=0 → restart.
                        return Err(make_err!(
                            Code::Unavailable,
                            "Received out of order data (write_offset {} but server has {}). \
                             Partial upload state was lost; retry from committed offset.",
                            write_offset,
                            tx.get_bytes_written()
                        ));
                    }
                    write_request.data
                };

                // Do not process EOF or weird stuff will happen.
                if !data.is_empty() {
                    // Tee: best-effort, non-blocking enqueue to the mirror channel
                    // (O(1) Bytes refcount bump). Use try_send so a slow mirror
                    // consumer never adds latency to the store-write hot path —
                    // and so a single full slot doesn't permanently disable the
                    // mirror for the rest of this blob (which the prior 100 ms
                    // timeout did). On Full we drop just this chunk and keep the
                    // writer alive; the mirror will end up incomplete, that's
                    // accepted (workers can re-fetch on demand).
                    if let Some(mtx) = mirror_tx {
                        match mtx.try_send(data.clone()) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                metrics
                                    .mirror_chunks_dropped_backpressure
                                    .fetch_add(1, Ordering::Relaxed);
                                if !*mirror_dropped_any {
                                    *mirror_dropped_any = true;
                                    metrics
                                        .mirror_blobs_incomplete
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                debug!(
                                    chunk_len = data.len(),
                                    "mirror tee channel full; dropping chunk"
                                );
                            }
                            Err(TrySendError::Closed(_)) => {
                                // Worker disconnected mid-stream; stop mirroring.
                                warn!("mirror channel closed, dropping mirror");
                                *mirror_tx = None;
                            }
                        }
                    }

                    // Append chunk to the streaming blob so concurrent readers
                    // can consume data before the store write completes.
                    //
                    // **#44 BS Write seam — Layer A Err is INTENTIONALLY
                    // swallowed at this seam.** `sbw.send` is the producer
                    // side of the in-flight streaming buffer; its Err return
                    // (carrying `STREAMING_BLOB_SILENT_OVERSHOOT_MARKER`)
                    // rejects bytes that would overshoot the
                    // streaming-buffer's declared upper bound. That
                    // rejection protects the streaming-buffer's
                    // read-while-write CONSUMERS — it does NOT protect the
                    // durability path. The durability path is the
                    // `tx.send(data).await` below: those bytes flow into
                    // `store.update(...)` and the mirror, both of which run
                    // their own validation (`VerifyStore` digest check at
                    // the slow tier; mirror's own `VerifyStore` at the
                    // worker side). If a Layer A reject happens here we
                    // log at `debug!` and fall through; the store +
                    // mirror chain is the durability authority and will
                    // independently reject a bad-byte upload at the digest
                    // check, surfacing as a regular `update` Err to the
                    // client.
                    //
                    // Architectural note (CLAUDE.md "no architectural
                    // change without sign-off"): tightening Layer A's Err
                    // into a tonic Status return would couple the
                    // streaming-buffer admission cap to the
                    // durability-write path's success/failure. Today
                    // those are decoupled — the streaming buffer is best-
                    // effort observability for in-flight readers, while
                    // the store + mirror chain is the durability
                    // authority. Keeping them decoupled means a
                    // read-while-write reader can be denied over-bytes
                    // (Layer A working) WITHOUT failing the durability
                    // write, which would in turn fail a Bazel upload that
                    // the durability chain would have accepted (the
                    // bytes might be acceptable to the store after a
                    // VerifyStore digest match).
                    if let Some(sbw) = streaming_blob_writer {
                        // Errors here are non-fatal — the streaming blob may
                        // have been terminated by a previous error, OR Layer
                        // A's OVERSHOOT cap (#44) rejected a chunk that
                        // would have pushed `bytes_written` past
                        // `expected_size_on_store_or_digest`. Either way,
                        // continue to the durability `tx.send` below; the
                        // store + mirror chain is the durability authority.
                        if let Err(e) = sbw.send(data.clone()).await {
                            debug!(?e, "streaming blob send failed (terminated, or #44 Layer A overshoot reject), continuing store write");
                        }
                    }

                    // We also need to process the possible EOF branch, so we can't early return.
                    if let Err(mut err) = tx.send(data).await {
                        err.code = Code::Internal;
                        return Err(err);
                    }
                    outer_bytes_received.store(tx.get_bytes_written(), Ordering::Release);
                }

                if expected_size < tx.get_bytes_written() {
                    return Err(make_input_err!("Received more bytes than expected"));
                }
                if write_request.finish_write {
                    // Surface the finish_write observation upward BEFORE the
                    // size-validation early-return below. If the byte count
                    // doesn't match, the function returns Err but the outer
                    // warn must still report `finish_write_seen=true` —
                    // distinguishing "client sent finish_write with wrong
                    // count" from "client closed mid-stream silently" (#320).
                    *finish_write_seen = true;
                    // Validate that we received the expected number of bytes
                    // before accepting the upload. The stream wrapper only
                    // validates on a *subsequent* poll_next after finish_write,
                    // which we never perform, so check here explicitly.
                    if tx.get_bytes_written() != expected_size {
                        return Err(make_input_err!(
                            "Client declared size {} but only sent {} bytes",
                            expected_size,
                            tx.get_bytes_written()
                        ));
                    }
                    // Send EOF to mirror (non-fatal, synchronous). Skip when we
                    // dropped any chunks: the byte count won't match expected_size
                    // and the receiver would either error on size mismatch or,
                    // worse, accept a corrupt blob. Instead of a silent writer
                    // drop (which surfaces in the mirror task as the generic
                    // `Code::Internal "Sender dropped before sending EOF"`),
                    // send a typed `Code::Aborted` carrying the
                    // `MIRROR_TEE_BACKPRESSURE_MARKER` so the downstream
                    // `worker_proxy_store::mirror_blob_via_stream` WARN site
                    // can demote the by-design event to DEBUG (#344). The
                    // single per-blob INFO at the end of `inner_write`
                    // ("receiver will re-fetch on demand") already records
                    // the operator-visible signal.
                    if let Some(mtx) = mirror_tx {
                        if *mirror_dropped_any {
                            mtx.send_error(make_err!(
                                Code::Aborted,
                                "{}",
                                nativelink_store::worker_proxy_store::MIRROR_TEE_BACKPRESSURE_MARKER
                            ));
                        } else if let Err(_err) = mtx.send_eof() {
                            warn!("mirror EOF send failed, dropping mirror");
                        }
                    }
                    // Gracefully close our store stream.
                    tx.send_eof()
                        .err_tip(|| "Failed to send EOF in ByteStream::write")?;
                    return Ok(());
                }
                // Continue.
            }
            // Unreachable.
        }

        let uuid = stream
            .resource_info
            .uuid
            .as_ref()
            .ok_or_else(|| make_input_err!("UUID must be set if writing data"))?;
        let mut active_stream_guard =
            self.create_or_join_upload_stream(uuid, instance_info, digest);
        let expected_size = stream.resource_info.expected_size as u64;

        // Set up tee mirror channel if WorkerProxyStore is available, blob is non-empty,
        // and the upload is NOT from a worker or a mirror. Workers already have the blob
        // locally — mirroring it back to another worker wastes bandwidth. Mirror writes
        // should not be re-mirrored to avoid infinite loops.
        let has_proxy = !is_worker
            && !is_mirror
            && digest.size_bytes() > 0
            && instance_info
                .store
                .as_store_driver()
                .as_any()
                .downcast_ref::<WorkerProxyStore>()
                .is_some();
        let (mut mirror_tx_opt, mirror_handle) = if has_proxy {
            let (mtx, mrx) = make_buf_channel_pair_with_size(16);
            let store_clone = instance_info.store.clone();
            let handle = nativelink_util::background_spawn!("mirror_tee_stream", async move {
                let Some(proxy) = store_clone
                    .as_store_driver()
                    .as_any()
                    .downcast_ref::<WorkerProxyStore>()
                else {
                    return;
                };
                proxy.mirror_blob_via_stream(digest, mrx).await;
            });
            (Some(mtx), Some(handle))
        } else {
            (None, None)
        };

        // Register a streaming blob so readers can consume data
        // before the store write commits (read-while-write).
        //
        // #49 v2 note: this seam does NOT call
        // `set_expected_size_on_store` because CAS upload-in-progress
        // reads are content-addressed — `digest.size_bytes()` IS the
        // on-store size by definition. The fallback to
        // `digest.size_bytes()` in `expected_size_on_store()` is correct
        // here. The AC case (where stored bytes ≠ declared digest size)
        // only fires on the FastSlowStore populate path, which calls
        // `set_expected_size_on_store` at `fast_slow_store.rs:3562`
        // after reading the authoritative size from `slow_store.has()`.
        let streaming_blob_writer = if instance_info.streaming_read_while_write {
            if let Some((writer, _reader)) = instance_info
                .in_flight_blobs
                .register(digest, instance_info.max_streaming_blob_buffer_bytes)
            {
                debug!(
                    %digest,
                    "registered streaming blob for read-while-write"
                );
                Some(writer)
            } else {
                debug!(
                    %digest,
                    "in-flight blob map at capacity, skipping read-while-write"
                );
                None
            }
        } else {
            None
        };

        let active_stream = active_stream_guard.stream_state.as_mut().unwrap();
        let write_start = std::time::Instant::now();
        let mut mirror_dropped_any = false;
        // #320 Diagnostic 1: track whether the client ever sent a
        // `finish_write: true` chunk before the gRPC stream ended. The
        // outer warn distinguishes "graceful client-side teardown after
        // finish_write" from "stream returned None mid-upload" — the
        // former is recoverable (size validated explicitly, fail-stop),
        // the latter is the #320 production observation.
        let mut finish_write_seen = false;
        let write_result = try_join!(
            process_client_stream(
                stream,
                &mut active_stream.tx,
                &mut mirror_tx_opt,
                &mut mirror_dropped_any,
                &instance_info.metrics,
                &streaming_blob_writer,
                &active_stream_guard.bytes_received,
                &mut finish_write_seen,
                expected_size,
                &progress_handle,
            ),
            (&mut active_stream.store_update_fut)
                .map_err(|err| { err.append("Error updating inner store") })
        );
        if mirror_dropped_any {
            // Single per-blob summary so we can correlate mirror gaps to specific
            // digests without spamming once per chunk. The chunk-level counter
            // mirror_chunks_dropped_backpressure tells us how many chunks were lost.
            info!(
                %digest,
                expected_size,
                "mirror tee best-effort skipped (chunks dropped to backpressure); receiver will re-fetch on demand"
            );
        }

        let bytes_received = active_stream_guard.bytes_received.load(Ordering::Relaxed);
        let elapsed_ms = write_start.elapsed().as_millis() as u64;
        if write_result.is_err() {
            // #320 Diagnostic 1: extended failure context. We capture the
            // fields necessary to disambiguate WHY the inner gRPC stream
            // returned `Ok(None)` mid-upload — the production observation
            // at 2026-05-07 20:52:18 had no surviving cause-trace.
            //
            // - finish_write_seen: did the client EVER send a chunk with
            //   `finish_write: true`? If false AND bytes_received <
            //   expected_size, the gRPC stream ended without graceful
            //   client-side teardown (the #320 case). If true with a
            //   size mismatch, this is the loud "Client declared X sent
            //   Y" path (a different bug class).
            // - is_worker / is_mirror: helps separate workload classes —
            //   was this a Bazel client, an inter-server mirror, or a
            //   worker upload? Each has different failure modes.
            // - producer_task_id: the bytestream handler's own task id;
            //   pairs with worker logs when an operator wants to grep
            //   for what this task was doing in the surrounding window.
            //
            // #320 Diagnostic 2: cross-correlation marker. If a chunked
            // cascade (FastSlowStore::update (chunked) data stream
            // failed) fired anywhere in the process within the last 10s,
            // log its digest + the gap so an operator can answer "is
            // the chunked cascade plausibly the trigger?" via grep.
            // 10s window chosen for the production observation: 720ms
            // gap × 14× safety = 10s; chunked cascades that fired
            // earlier than 10s before our disconnect are unlikely to
            // be causally linked.
            let recent_cascade =
                nativelink_store::fast_slow_store::cascade_diag::recent_cascade_within(10_000);
            // #320 Diagnostic 3: buf_channel write-side state at drop.
            // The store-write tx is held inside `active_stream.tx`.
            // Snapshot bytes-written and pipe-broken status so we can
            // tell whether the producer side finished its work but the
            // consumer (store driver) bailed, vs the producer aborted.
            let store_tx_bytes_written = active_stream.tx.get_bytes_written();
            let store_tx_pipe_broken = active_stream.tx.is_pipe_broken();
            // Mirror-side state: how many bytes did we successfully
            // forward to the mirror channel before the disconnect?
            // mirror_tx_opt is None either when no mirror was set up
            // (worker/mirror request, or no WorkerProxyStore) OR when
            // the mirror dropped earlier (closed). Distinguish via
            // mirror_dropped_any.
            let (mirror_present, mirror_bytes_forwarded, mirror_pipe_broken) = match &mirror_tx_opt
            {
                Some(mtx) => (true, mtx.get_bytes_written(), mtx.is_pipe_broken()),
                None => (false, 0u64, true),
            };
            let producer_task_id = tokio::task::try_id().map(|t| t.to_string());
            warn!(
                %digest,
                expected_size,
                bytes_received,
                elapsed_ms,
                finish_write_seen,
                is_worker,
                is_mirror,
                producer_task_id = producer_task_id.as_deref().unwrap_or("<none>"),
                store_tx_bytes_written,
                store_tx_pipe_broken,
                mirror_present,
                mirror_dropped_any,
                mirror_bytes_forwarded,
                mirror_pipe_broken,
                recent_cascade_within_10s = recent_cascade.is_some(),
                recent_cascade_digest = recent_cascade
                    .as_ref()
                    .map(|c| c.digest_hash.as_str())
                    .unwrap_or("<none>"),
                recent_cascade_site = recent_cascade
                    .as_ref()
                    .map(|c| c.site)
                    .unwrap_or("<none>"),
                recent_cascade_age_ms = recent_cascade.as_ref().map(|c| {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or_default();
                    now.saturating_sub(c.at_epoch_ms)
                }),
                err = ?write_result.as_ref().err(),
                "inner_write failed"
            );

            // #396: operator-friendly truncation-clarity diagnosis.
            //
            // The `inner_write failed` warn above carries 14 diagnostic
            // fields for triage but its message ("inner_write failed")
            // does not name the shape an operator sees in production.
            // The shape: a Bazel client half-closes the HTTP/2 stream
            // (END_STREAM, no error frame) without ever sending a
            // `WriteRequest { finish_write: true, .. }`. The wrapper
            // (`proto_stream_utils.rs:135`, #357) materializes this as
            // a `Code::Cancelled` Err; the chunked path here observes
            // `!finish_write_seen && bytes_received < expected_size`.
            //
            // The reason the client closes the stream pre-finish is
            // NOT YET ROOT-CAUSED. NL #311 ("Bazel partial-upload not
            // detected as hard error post-9.1") tracks the
            // client-side investigation. The 2026-05-11 incident
            // (ci-mac-1, ~30s into a 44 MB upload) was kernel-
            // confirmed to be a Bazel-internal close — not network,
            // not server timeout, not configured Bazel timeout.
            // Until #311 lands, this site emits the operator-grep
            // phrase so the class is greppable without forwarding a
            // possibly-wrong cause attribution.
            //
            // Emit a SEPARATE `info!` (not warn — this fires at
            // observed-frequent rates during incidents; warn would
            // flood) whose message contains the literal operator-grep
            // phrase. Fields are pre-named so triage doesn't re-derive
            // "is this a Cancel half-close or something else?" from
            // the 14-field warn above.
            //
            // Heuristic gating:
            //   - `!finish_write_seen`: client never sent
            //     `finish_write: true`. Distinguishes from
            //     size-mismatch (where finish_write WAS sent with
            //     wrong byte count) — a different bug class.
            //   - `bytes_received < expected_size`: confirms
            //     truncation, not a 0-byte connection setup race.
            //   - `err.code == Code::Cancelled`: matches the
            //     wrapper's materialized half-close error (#357).
            //     Filters out store-side mid-write Internal errors,
            //     out-of-order Unavailable errors, and other failure
            //     modes that COULD also have `!finish_write_seen`.
            if !finish_write_seen
                && bytes_received < expected_size
                && let Some(err) = write_result.as_ref().err()
                && err.code == Code::Cancelled
            {
                info!(
                    %digest,
                    bytes_received,
                    expected_bytes = expected_size,
                    grpc_code = ?err.code,
                    is_worker,
                    is_mirror,
                    "client half-closed upload before finish_write \
                     (received {bytes_received}/{expected_size} bytes; \
                     cause not yet root-caused — see NL #311)",
                );
            }
        } else if elapsed_ms > 5000 {
            info!(
                %digest,
                expected_size,
                bytes_received,
                elapsed_ms,
                "inner_write slow (>5s)"
            );
        }

        // Propagate terminal state to the streaming blob.
        if let Some(mut sbw) = streaming_blob_writer {
            match &write_result {
                Ok(_) => {
                    if let Err(e) = sbw.send_eof() {
                        debug!(?e, "streaming blob send_eof failed");
                    }
                }
                Err(e) => {
                    sbw.send_error(e.clone());
                }
            }

            // Schedule deferred removal from InFlightBlobMap after a grace
            // period so in-progress readers can finish consuming data.
            let in_flight_blobs = Arc::clone(&instance_info.in_flight_blobs);
            let inner_arc = instance_info.in_flight_blobs.get_inner(&digest);
            if let Some(inner_arc) = inner_arc {
                nativelink_util::background_spawn!("streaming_blob_grace_removal", async move {
                    sleep(Duration::from_secs(5)).await;
                    in_flight_blobs.remove(&digest, &inner_arc);
                    debug!(
                        %digest,
                        "removed streaming blob after grace period"
                    );
                });
            }
        }

        // Propagate the result after streaming blob cleanup.
        write_result?;

        // Fire-and-forget: drop the mirror handle without awaiting it.
        // The mirror task runs to completion (or failure) in the background.
        drop(mirror_handle);

        // Close our guard and consider the stream no longer active.
        active_stream_guard.graceful_finish();

        // #168 dist-systems MINOR-2: the chunked write path
        // (`inner_write`, this function) does NOT fan out small blobs
        // to the SmallBlobDispatcher. The fast-path `inner_write_oneshot`
        // does, and that path absorbs almost all small writes
        // (≤ SMALL_BLOB_THRESHOLD = 16 KiB) because Bazel pushes small
        // blobs in a single chunk. Wiring a hook here is structurally
        // awkward: the chunked path streams to `tx` (no accumulated
        // Bytes); collecting the bytes for fan-out would either buffer
        // (defeating the streaming optimization) or re-read from the
        // store post-write (extra round-trip). Defer.
        //
        // TODO(#168 follow-up): if production telemetry shows
        // small-blob writes routinely arriving via the chunked path
        // (>0.1% of small writes), wire a tee channel here that
        // accumulates ≤ SMALL_BLOB_THRESHOLD bytes for the dispatcher;
        // bail out (drop the buffer) once the size exceeds the
        // threshold so the streaming optimization is preserved for
        // larger blobs.

        // #62 Phase 2 instrumentation: capture the WriteResponse the
        // server sends to the chunked-write client. Per #56 RCA
        // candidate (b), correlate worker-side
        // `chunked_inner_truncated` (committed < expected) with
        // server-side WriteResponse to decide whether the server
        // actually reported committed_size < expected (would require
        // a code change that doesn't exist today — `expected_size as
        // i64` is unconditional here) or whether the client-side
        // fabrication happens elsewhere.
        debug!(
            %digest,
            committed_size = expected_size as i64,
            expected_size,
            delta = 0_i64,
            arm_name = "server_inner_write_terminal",
            "#62 ByteStream::Write: server returning WriteResponse (chunked path)",
        );
        Ok(Response::new(WriteResponse {
            committed_size: expected_size as i64,
        }))
    }

    /// Fast-path write that bypasses channel overhead for stores that support direct Bytes updates.
    /// This buffers all data in memory and calls `update_oneshot` directly.
    async fn inner_write_oneshot(
        &self,
        instance_info: &InstanceInfo,
        digest: DigestInfo,
        mut stream: WriteRequestStreamWrapper<
            impl Stream<Item = Result<WriteRequest, Status>> + Unpin,
        >,
        is_worker: bool,
        is_mirror: bool,
        // Slow-producer immunity: bumped after each successful chunk
        // recv so the StallGuard can suppress dumps when the server is
        // correctly waiting on a paused client. See
        // `.claude/audits/stall-cluster-2026-05-13-1941-1945.md`.
        progress_handle: Arc<AtomicU64>,
    ) -> Result<Response<WriteResponse>, Error> {
        let expected_size = stream.resource_info.expected_size as u64;

        let mut bytes_received: u64 = 0;
        // Accumulate data. Use Option<Bytes> for the single-chunk fast path
        // (avoids BytesMut allocation + copy when the entire blob arrives in
        // one WriteRequest, which is the common case for small blobs).
        let mut single_chunk: Option<Bytes> = None;
        let mut buffer: Option<BytesMut> = None;

        // Collect all data from client stream
        loop {
            // No app-layer recv timer (oneshot path). Mirrors the
            // multi-chunk path in `inner_write`. Transport keepalive
            // (h2/QUIC/TCP) covers dead connections; sender-drop on
            // close surfaces as `None`/`Err` from `stream.next()`. See
            // the lifecycle doc-comment near `_stall_guard` in `write`.
            let write_request = match stream.next().await {
                None => {
                    return Err(make_input_err!(
                        "Client closed stream before sending all data"
                    ));
                }
                Some(Err(err)) => return Err(err),
                Some(Ok(write_request)) => write_request,
            };
            // Mark forward server-side progress: a chunk arrived. The
            // StallGuard observes this to suppress stack dumps when the
            // only "stall" is a paused client (see
            // `.claude/audits/stall-cluster-2026-05-13-1941-1945.md`).
            nativelink_util::stall_detector::bump_progress_handle(&progress_handle);

            if write_request.write_offset < 0 {
                return Err(make_input_err!(
                    "Invalid negative write offset in write request: {}",
                    write_request.write_offset
                ));
            }
            let write_offset = write_request.write_offset as u64;

            // Handle duplicate/resumed data
            let data = if write_offset < bytes_received {
                if (write_offset + write_request.data.len() as u64) < bytes_received {
                    if write_request.finish_write {
                        return Err(make_input_err!(
                            "Resumed stream finished at {} bytes when we already received {} bytes.",
                            write_offset + write_request.data.len() as u64,
                            bytes_received
                        ));
                    }
                    continue;
                }
                write_request
                    .data
                    .slice(usize::try_from(bytes_received - write_offset).unwrap_or(usize::MAX)..)
            } else {
                if write_offset != bytes_received {
                    return Err(make_err!(
                        Code::Unavailable,
                        "Received out of order data (write_offset {} but server has {}). \
                         Partial upload state was lost; retry from committed offset.",
                        write_offset,
                        bytes_received
                    ));
                }
                write_request.data
            };

            if !data.is_empty() {
                bytes_received += data.len() as u64;
                if single_chunk.is_none() && buffer.is_none() {
                    // First chunk — hold zero-copy reference.
                    single_chunk = Some(data);
                } else {
                    // Second+ chunk — spill into BytesMut.
                    let buf = buffer.get_or_insert_with(|| {
                        let capacity = usize::try_from(expected_size.min(64 * 1024 * 1024))
                            .unwrap_or(64 * 1024 * 1024);
                        let mut b = BytesMut::with_capacity(capacity);
                        if let Some(first) = single_chunk.take() {
                            b.extend_from_slice(&first);
                        }
                        b
                    });
                    buf.extend_from_slice(&data);
                }
            }

            if expected_size < bytes_received {
                return Err(make_input_err!("Received more bytes than expected"));
            }

            if write_request.finish_write {
                // Validate that we received the expected number of bytes
                // before accepting the upload.
                if bytes_received != expected_size {
                    return Err(make_input_err!(
                        "Client declared size {} but only sent {} bytes",
                        expected_size,
                        bytes_received
                    ));
                }
                break;
            }
        }

        // Use the zero-copy single chunk if possible, otherwise the assembled buffer.
        let final_data = if let Some(buf) = buffer {
            buf.freeze()
        } else {
            single_chunk.unwrap_or_default()
        };

        // Clone data for mirroring before store write (Bytes clone is O(1) refcount bump).
        let mirror_data = final_data.clone();

        // Direct update without channel overhead
        let store = instance_info.store.clone();
        store
            .update_oneshot(digest, final_data.clone())
            .await
            .err_tip(|| "Error in update_oneshot")?;

        // Register streaming blob for read-while-write AFTER the store write
        // succeeds. Registering before the write would let readers see data
        // that might not persist if the write fails. The oneshot path has the
        // full blob in memory, so write it all at once and send EOF.
        //
        // #49 v2 note: this seam does NOT call
        // `set_expected_size_on_store` because CAS upload-in-progress
        // reads are content-addressed — `digest.size_bytes()` IS the
        // on-store size by definition (and `final_data.len() ==
        // digest.size_bytes()` by upload-write invariant). The fallback
        // to `digest.size_bytes()` in `expected_size_on_store()` is
        // correct here.
        if instance_info.streaming_read_while_write {
            if let Some((mut writer, _reader)) = instance_info
                .in_flight_blobs
                .register(digest, instance_info.max_streaming_blob_buffer_bytes)
            {
                let _ = writer.send(final_data).await;
                let _ = writer.send_eof();
            }
            // Schedule deferred removal so the map doesn't fill up (128 max).
            let in_flight_blobs = Arc::clone(&instance_info.in_flight_blobs);
            let inner_arc = instance_info.in_flight_blobs.get_inner(&digest);
            if let Some(inner_arc) = inner_arc {
                nativelink_util::background_spawn!("streaming_blob_oneshot_removal", async move {
                    sleep(Duration::from_secs(5)).await;
                    in_flight_blobs.remove(&digest, &inner_arc);
                });
            }
        }

        // #168 producer-side: fan out small blobs (≤ SMALL_BLOB_THRESHOLD)
        // to every connected worker via the SmallBlobDispatcher so future
        // actions on any worker can serve the blob locally without a
        // peer-fetch round-trip (proactive read-locality replication).
        //
        // Skip for `is_worker` (worker uploaded action results; would
        // loop them back to the originating worker) and `is_mirror`
        // (server-to-worker mirror push round-tripped via bytestream
        // — would re-loop). Both gates close the over-action sibling
        // (#168 testing-czar M1 / USER DIRECTIVE on loop prevention).
        //
        // Item F: track whether we dispatched so the random-single
        // `mirror_blob_to_worker` path below can be SUPPRESSED for
        // small blobs the dispatcher already handled (the dispatcher
        // gives every worker the bytes; the random-single mirror is
        // redundant for small blobs which are durable via Redis
        // SMALL_CAS_CACHED).
        //
        // Fire-and-forget — do not .await; see
        // `SmallBlobDispatcher::schedule_dispatch_to_all_workers` doc.
        let dispatched = if !is_worker
            && !is_mirror
            && bytes_received <= SMALL_BLOB_THRESHOLD as u64
        {
            if let Some(dispatcher) = instance_info.small_blob_dispatcher.as_ref() {
                dispatcher.schedule_dispatch_to_all_workers(
                    instance_info.cas_store_name_arc.clone(),
                    digest,
                    mirror_data.clone(),
                );
                true
            } else {
                false
            }
        } else {
            false
        };

        // Mirror to a random worker using the cloned data — no re-read needed.
        // Skip mirroring for worker uploads and mirror writes — workers already
        // have the blob, and mirror writes should not be re-mirrored.
        // Item F: also skip when the dispatcher already fanned out the
        // same bytes to every worker (small blobs are durable via Redis;
        // the random-single mirror would just duplicate bytes already
        // pushed by the dispatcher).
        if !is_worker && !is_mirror && !dispatched {
            mirror_blob_to_worker(&store, digest, Some(mirror_data));
        }

        // Note: bytes_written_total is updated in the caller (bytestream_write) based on result

        // #62 Phase 2 instrumentation: oneshot fast-path terminal.
        // Per #62, log committed_size and arm_name so journal queries
        // can distinguish chunked-path WriteResponse from oneshot
        // WriteResponse (different upstream code paths in worker
        // GrpcStore).
        debug!(
            %digest,
            committed_size = expected_size as i64,
            expected_size,
            delta = 0_i64,
            arm_name = "server_inner_write_oneshot_terminal",
            "#62 ByteStream::Write: server returning WriteResponse (oneshot path)",
        );
        Ok(Response::new(WriteResponse {
            committed_size: expected_size as i64,
        }))
    }

    async fn inner_query_write_status(
        &self,
        query_request: &QueryWriteStatusRequest,
    ) -> Result<Response<QueryWriteStatusResponse>, Error> {
        let mut resource_info = ResourceInfo::new(&query_request.resource_name, true)?;

        let instance = self
            .instance_infos
            .get(resource_info.instance_name.as_ref())
            .err_tip(|| {
                format!(
                    "'instance_name' not configured for '{}'",
                    &resource_info.instance_name
                )
            })?;
        let store_clone = instance.store.clone();

        let digest = DigestInfo::try_new(resource_info.hash.as_ref(), resource_info.expected_size)?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_clone.downcast_ref::<GrpcStore>(Some(digest.into())) {
            return grpc_store
                .query_write_status(Request::new(query_request.clone()))
                .await;
        }

        let uuid_str = resource_info
            .uuid
            .take()
            .ok_or_else(|| make_input_err!("UUID must be set if querying write status"))?;
        let uuid_key = parse_uuid_to_key(&uuid_str);

        {
            let active_uploads = instance.active_uploads.lock();
            if let Some((received_bytes, _maybe_idle_stream)) = active_uploads.get(&uuid_key) {
                return Ok(Response::new(QueryWriteStatusResponse {
                    committed_size: received_bytes.load(Ordering::Acquire) as i64,
                    // If we are in the active_uploads map, but the value is None,
                    // it means the stream is not complete.
                    complete: false,
                }));
            }
        }

        let has_fut = store_clone.has(digest);
        let item_size_or_none = has_fut.await.err_tip(|| "Failed to call .has() on store")?;
        // BLOCK-A (H2 sibling at QueryWriteStatus, #499 followup): mirror
        // the H2 phantom-success guard at `:2742`. The same `store.has()`
        // cascade returns `Some(declared_size)` for an in-flight chunked
        // write (v1 or v2) — the FSS-level `chunked_in_flight_digests`
        // set is consulted by `FastSlowStore::has_with_results` and
        // reports declared_size for sessions still mid-stream. If we
        // return `complete: true` here, Bazel believes the upload is
        // durable, but if the chunked commit subsequently fails, the
        // digest is silently lost.
        //
        // Mitigation: when the underlying store is a `FastSlowStore` AND
        // the digest is in the chunked in-flight set, return
        // `complete: false` with `committed_size: 0` so the Bazel client
        // re-sends from the start (the standard QueryWriteStatus
        // semantics: `complete=false` means "the upload is not durable;
        // continue / restart"). Without the guard, this RPC handler
        // breaks the `has()=Some ⇒ durable` invariant on Bazel's side.
        //
        // Sibling-bug audit: the same `store.has()` short-circuit pattern
        // exists at `bytestream_write` (`:2742`) — guarded since 9d8a66a9.
        // See `.claude/audits/concurrent-readers-vs-writers-2026-05-15.md`
        // H2 + ds-reviewer.md BLOCK-1.
        //
        // BLOCK-1 (DS-reviewer, #499 v3 follow-up): in production
        // `cas_STORE` is `WPS → VerifyStore → ExistenceCacheStore →
        // SizePartitioningStore → FastSlowStore`. `Store::downcast_ref`
        // walks via `inner_store(maybe_digest)`; `VerifyStore::inner_store`
        // returns `self`, so an inline `downcast_ref::<FastSlowStore>`
        // terminates at VerifyStore and returns `None`. The guard then
        // never fires in production. Use the canonical
        // `wrapper_walker::find_fast_slow_via_chain` walker (already
        // consumed by `bin/nativelink.rs`, `store_manager.rs`, and
        // `failed_writes_drain.rs`) — it special-cases
        // ExistenceCacheStore + VerifyStore via downcast+recurse and
        // descends `SizePartitioningStore` via `synthetic_large_key()`.
        let is_chunked_in_flight =
            nativelink_store::wrapper_walker::find_fast_slow_via_chain(
                store_clone.as_store_driver(),
            )
            .is_some_and(|fss| fss.is_chunked_in_flight(&digest));
        let Some(item_size) = item_size_or_none else {
            // We lie here and say that the stream needs to start over, even though
            // it was never started. This can happen when the client disconnects
            // before sending the first payload, but the client thinks it did send
            // the payload.
            return Ok(Response::new(QueryWriteStatusResponse {
                committed_size: 0,
                complete: false,
            }));
        };
        if is_chunked_in_flight {
            debug!(
                %digest,
                size_bytes = item_size,
                "QueryWriteStatus: H2 phantom-success guard fired — \
                 has() returned Some via chunked-in-flight, but the \
                 chunked commit may still fail. Returning complete=false \
                 so Bazel does not treat the upload as durable.",
            );
            return Ok(Response::new(QueryWriteStatusResponse {
                committed_size: 0,
                complete: false,
            }));
        }
        // Defense-in-depth at the wire boundary: `committed_size` is what Bazel
        // uses to position resumed uploads, so any inner-store `has()` that
        // reports a size larger than the requested digest's size would push
        // Bazel past the end of the blob and corrupt subsequent writes. The
        // primary contract is enforced inside each store, but this assert
        // catches any future regression at the single point where the value
        // is serialized to the wire (testing-czar hot-fix retro MAJOR-2).
        debug_assert!(
            item_size <= digest.size_bytes(),
            "committed_size {item_size} exceeds digest size {} for {digest}",
            digest.size_bytes()
        );
        Ok(Response::new(QueryWriteStatusResponse {
            committed_size: item_size as i64,
            complete: true,
        }))
    }

    /// Shared write implementation used by both the tonic `write()` handler and
    /// the zero-copy `zero_copy_write()` handler. All preamble (instance lookup,
    /// metrics, GrpcStore shortcut, has-check, oneshot decision) and postamble
    /// (logging, metrics, mirroring) live here so the two entry points are thin
    /// wrappers.
    async fn bytestream_write(
        &self,
        start_time: Instant,
        stream: WriteRequestStreamWrapper<
            impl Stream<Item = Result<WriteRequest, Status>> + Unpin + Send + 'static,
        >,
        zero_copy: bool,
        is_worker: bool,
        is_mirror: bool,
    ) -> Result<Response<WriteResponse>, Error> {
        let instance_name = stream.resource_info.instance_name.as_ref();
        let expected_size = stream.resource_info.expected_size as u64;
        let instance = self
            .instance_infos
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        // Track write request
        instance
            .metrics
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let store = instance.store.clone();

        let digest = DigestInfo::try_new(
            &stream.resource_info.hash,
            stream.resource_info.expected_size,
        )
        .err_tip(|| "Invalid digest input in ByteStream::write")?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(Some(digest.into())) {
            return grpc_store.write(stream).await.map_err(Into::into);
        }

        // Fast path: skip the write if the blob already exists.
        //
        // H2 (#499 followup): the `store.has()` cascade returns Some for
        // an in-flight chunked write (v1 or v2) — the
        // `chunked_in_flight_digests` set is consulted by
        // `FastSlowStore::has_with_results` and reports declared_size for
        // sessions still mid-stream. If we short-circuit on Some here,
        // a second concurrent ByteStream::write returns committed_size
        // BEFORE the first writer's commit lands; if that first commit
        // FAILS, the second client believes its upload was acked but the
        // data isn't durable. See
        // `.claude/audits/concurrent-readers-vs-writers-2026-05-15.md` H2.
        //
        // Mitigation: when the underlying store is a `FastSlowStore` AND
        // the digest is in the chunked in-flight set, skip the
        // phantom-success short-circuit and fall through to the standard
        // write path. CAVEAT (MAJOR-I, ds-reviewer MINOR-2): the
        // `in_flight_writes` watch-channel dedup below only catches
        // duplicate ByteStream::write callers — chunked-v2 (worker
        // WriteChunkedV2) and v1-chunked (Bazel internal-chunking
        // dispatcher) sessions do NOT populate `in_flight_writes`, so
        // the second ByteStream::write becomes the primary in
        // `in_flight_writes` and runs its own write through FSS. Both
        // writers race the slow store concurrently; CAS convergence
        // (digest = content) makes the on-disk byte sequence well-defined
        // (same bytes either way), and the chunked-write path's
        // `chunked_in_flight_digests` + race-state coordination
        // arbitrates the canonical-rename. The H2 guard is therefore
        // "don't lie about durability" — NOT "serialize against the
        // chunked writer." Bazel-to-Bazel concurrent uploads of the
        // same digest still coalesce on `in_flight_writes` (the original
        // dedup contract).
        let has_result = store.has(digest).await.unwrap_or(None);
        // BLOCK-1 (DS-reviewer, #499 v3 follow-up): production cas_STORE
        // is `WPS → VerifyStore → ExistenceCacheStore → SizePartitioningStore
        // → FastSlowStore`. Inline `downcast_ref::<FastSlowStore>` walks
        // via `inner_store(maybe_digest)` which terminates at VerifyStore
        // (`inner_store` shadowed to return `self`), so the downcast
        // returns `None` and this H2 guard becomes dead code in
        // production. Use the canonical
        // `wrapper_walker::find_fast_slow_via_chain` walker (consumed
        // also by `bin/nativelink.rs`, `store_manager.rs`,
        // `failed_writes_drain.rs`).
        let is_chunked_in_flight =
            nativelink_store::wrapper_walker::find_fast_slow_via_chain(
                store.as_store_driver(),
            )
            .is_some_and(|fss| fss.is_chunked_in_flight(&digest));
        if has_result.is_some() && !is_chunked_in_flight {
            debug!(
                %digest,
                size_bytes = expected_size,
                "ByteStream::write: skipped, blob already exists",
            );
            instance
                .metrics
                .write_requests_success
                .fetch_add(1, Ordering::Relaxed);
            // #62 Phase 2 instrumentation: has()-short-circuit terminal.
            // Server reports committed_size=expected without the
            // worker actually streaming. If a worker sees Ok with no
            // bytes transferred and #59 logs (Ok, Err) on a digest the
            // server short-circuited here, that maps to a producer
            // that never got its EOF acknowledged.
            info!(
                %digest,
                committed_size = expected_size as i64,
                expected_size,
                delta = 0_i64,
                arm_name = "server_has_short_circuit",
                "#62 ByteStream::Write: server returning WriteResponse (already-exists short-circuit)",
            );
            return Ok(Response::new(WriteResponse {
                committed_size: expected_size as i64,
            }));
        }
        if has_result.is_some() && is_chunked_in_flight {
            debug!(
                %digest,
                size_bytes = expected_size,
                "ByteStream::write: H2 phantom-success guard fired — \
                 has() returned Some via chunked-in-flight, but the \
                 chunked commit may still fail. Falling through to \
                 in_flight_writes dedup so the second writer waits for \
                 the first writer's outcome.",
            );
        }

        // Dedup in-flight writes: if another RPC is already writing this
        // exact digest, wait for it instead of writing again.
        //
        // #402 cancel-safety: when this RPC is the primary writer, we
        // create an `InFlightWritesGuard` that owns map removal +
        // outcome publishing. The guard's Drop ensures cancellation
        // mid-await still cleans up the map entry. See the type's
        // doc-comment.
        let in_flight_guard = {
            let mut guard = instance.in_flight_writes.lock();
            if let Some(rx) = guard.get(&digest) {
                let mut rx = rx.clone();
                drop(guard);
                // Another write is in progress — wait for its outcome.
                //
                // No wall-clock timeout here. The primary writer's
                // `InFlightWritesGuard` (`:495-509`) drops the watch
                // `Sender` on every primary exit path including
                // cancellation/disconnect (#402: cancel-safe RAII guard
                // for in_flight_writes). When the Sender drops, this
                // waiter's `rx.changed().await` returns `Err`, the loop
                // returns `false` (failure), and we fall through to do
                // our own write.
                //
                // Removed: 300s `COALESCE_TIMEOUT` which papered over
                // the same shape that motivated removing
                // `WRITE_TIMEOUT` — it killed legitimately slow primary
                // writers (e.g. CGNAT clients) AND any waiter coalesced
                // onto them. The watch-Sender-drop signal is precisely
                // the right wakeup; the wall-clock added nothing.
                let succeeded = loop {
                    if let Some(ok) = *rx.borrow_and_update() {
                        break ok;
                    }
                    if rx.changed().await.is_err() {
                        break false; // sender dropped = failure
                    }
                };
                if succeeded {
                    info!(
                        %digest,
                        size_bytes = expected_size,
                        "ByteStream::write: coalesced with in-flight write",
                    );
                    instance
                        .metrics
                        .write_requests_success
                        .fetch_add(1, Ordering::Relaxed);
                    // #62 Phase 2 instrumentation: dedup-coalesce terminal.
                    // This RPC won the dedup race and the primary
                    // writer's outcome was Ok — server reports
                    // committed_size=expected without the worker
                    // streaming any bytes on THIS RPC.
                    info!(
                        %digest,
                        committed_size = expected_size as i64,
                        expected_size,
                        delta = 0_i64,
                        arm_name = "server_dedup_coalesce_ok",
                        "#62 ByteStream::Write: server returning WriteResponse (in-flight dedup coalesce)",
                    );
                    return Ok(Response::new(WriteResponse {
                        committed_size: expected_size as i64,
                    }));
                }
                // In-flight write failed — fall through to do our own.
                warn!(
                    %digest,
                    size_bytes = expected_size,
                    "ByteStream::write: in-flight write failed, retrying",
                );
                None
            } else {
                // We're the first writer — create a watch channel,
                // insert (digest, rx) under the same lock that proved
                // we are the primary (no race with a concurrent second
                // arrival becoming a duplicate primary), then drop the
                // lock and hand `tx` to the RAII guard. The guard's
                // Drop removes the entry on every exit path including
                // cancellation (#402). `from_inserted` skips the insert
                // because we already did it under the dedup lock above.
                let (tx, rx) = tokio::sync::watch::channel(None);
                guard.insert(digest, rx);
                drop(guard);
                Some(InFlightWritesGuard::from_inserted(
                    Arc::clone(&instance.in_flight_writes),
                    digest,
                    tx,
                ))
            }
        };

        let digest_function = stream
            .resource_info
            .digest_function
            .as_deref()
            .map_or_else(
                || Ok(default_digest_hasher_func()),
                DigestHasherFunc::try_from,
            )?;

        // Check if store supports direct oneshot updates (bypasses channel overhead).
        // Use fast-path only when:
        // 1. Store supports oneshot optimization
        // 2. UUID is provided
        // 3. Size is under 64MB (memory safety)
        // 4. This is a NEW upload (UUID not already in active_uploads)
        // 5. The first message has finish_write=true (single-shot upload)
        let use_oneshot = if store.optimized_for(StoreOptimizations::SubscribesToUpdateOneshot)
            && expected_size <= 64 * 1024 * 1024
            && stream.resource_info.uuid.is_some()
        {
            let is_single_shot = stream.is_first_msg_complete();
            if is_single_shot {
                let uuid_str = stream.resource_info.uuid.as_ref().unwrap();
                let uuid_key = parse_uuid_to_key(uuid_str);
                !instance.active_uploads.lock().contains_key(&uuid_key)
            } else {
                false
            }
        } else {
            false
        };

        let oneshot = use_oneshot;
        debug!(
            %digest,
            expected_size,
            oneshot,
            zero_copy,
            "ByteStream::write: starting upload",
        );

        // Build label strings based on zero_copy flag. These must be
        // &'static str for tracing / err_tip messages.
        let (stall_label, tip_label, tip_oneshot_label) = if zero_copy {
            (
                "ByteStream::write(zero-copy)",
                "In ByteStreamServer::write(zero-copy)",
                "In ByteStreamServer::write(zero-copy, oneshot)",
            )
        } else {
            (
                "ByteStream::write",
                "In ByteStreamServer::write",
                "In ByteStreamServer::write (oneshot)",
            )
        };

        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            stall_label,
        );
        // Slow-producer immunity (#stall-cluster-2026-05-13): the
        // recv-loop bumps this handle on each chunk so the
        // stall_detector can distinguish "server wedged" from "Bazel
        // client paused 80s between chunks". See
        // `.claude/audits/stall-cluster-2026-05-13-1941-1945.md`.
        let progress_handle = _stall_guard.progress_handle();
        // No server-side timer on the bytestream Write RPC. No-progress
        // detection is fully layered on the transport + RAII cleanup:
        //   * h2 keepalive (30s/20s configured in `prod-server.json5`) closes
        //     DEAD connections when the client stops ACKing PINGs. The
        //     stream's `Streaming::next()` then resolves to `None` /
        //     `Some(Err(_))` and the recv-loop in `inner_write` /
        //     `inner_write_oneshot` returns Err, ending the RPC.
        //   * QUIC keepalive (5s) covers HTTP/3 transport with the same
        //     dead-connection signal class.
        //   * TCP keepalive at the OS level (enabled in `src/bin/nativelink.rs`)
        //     catches network drops independent of h2/QUIC.
        //   * Sender-drop on connection close → `buf_channel` observer:
        //     when the gRPC future is dropped (client RST_STREAM, server
        //     shutdown, runtime drop), the watch sender that the writer
        //     holds drops and any coalesced waiters observe
        //     `rx.changed() == Err(_)` and exit cleanly.
        //   * `InFlightWritesGuard` RAII (#402: cancel-safe RAII guard
        //     for in_flight_writes) cleans up the dedup map on
        //     grpc-future-drop.
        //   * Per-chunk store-side diagnostic threshold (e.g.
        //     `nativelink_store::chunked::chunked_driver::PER_CHUNK_WRITE_TIMEOUT`,
        //     diagnostic-only per #487 2026-05-16) emits warn! +
        //     bumps a counter on slow per-chunk pwrites in the
        //     SEPARATE `WriteChunked` RPC handled by
        //     `ChunkedWriteHandler`. It does NOT abort the blob, and
        //     does NOT apply to this Write RPC path at all —
        //     `inner_write` / `inner_write_oneshot` write directly
        //     via `StoreLike::update`, never through `ChunkedDriver`.
        //
        // No app-layer per-recv timer is added. Per CLAUDE.md "Falsify
        // before fixing": no production incident has surfaced an
        // app-stuck-but-TCP-alive client shape that the transport
        // mechanisms above wouldn't catch. The 2026-05-14 TSAN RCA (244
        // WRITE_TIMEOUT firings on 122 distinct digests / ~15 GB in 24
        // min surfacing as Bazel-visible FAILED_PRECONDITION cascades)
        // and the 2026-05-14 stall investigation (2 firings on ci-mac-2 at
        // 09:58:57 and 10:03:33 PDT) were ALL slow-but-progressing
        // CGNAT clients — the wall-clock kill was unjustified. This
        // restores the design from `fa6cdf43` (2026-04-23: drop 300s
        // outer write/coalesce deadlines).
        //
        // If a future production incident reveals a hypothetical
        // "app-stuck client holding a healthy TCP/h2 connection while
        // not sending bytes" failure mode, instrument-first per
        // CLAUDE.md "instrument first, theorize second" and add a timer
        // with real evidence — not on speculation.
        let write_fut = IS_MIRROR_REQUEST.scope(is_mirror, async {
            if use_oneshot {
                self.inner_write_oneshot(
                    instance,
                    digest,
                    stream,
                    is_worker,
                    is_mirror,
                    progress_handle.clone(),
                )
                .instrument(error_span!("bytestream_write_oneshot", %zero_copy))
                .with_context(make_ctx_for_hash_func(digest_function).err_tip(|| tip_label)?)
                .await
                .err_tip(|| tip_oneshot_label)
            } else {
                self.inner_write(
                    instance,
                    digest,
                    stream,
                    is_worker,
                    is_mirror,
                    progress_handle.clone(),
                )
                .instrument(error_span!("bytestream_write", %zero_copy))
                .with_context(make_ctx_for_hash_func(digest_function).err_tip(|| tip_label)?)
                .await
                .err_tip(|| tip_label)
            }
        });
        let result = write_fut.await;

        // Write finished — publish the outcome to coalesced waiters via
        // the guard's set_result. Map removal happens in the guard's
        // Drop at end of scope (or earlier if cancellation strikes).
        // Order matters: set_result publishes BEFORE Drop removes, so
        // new RPCs arriving in between still find + subscribe to the
        // existing entry, and `borrow_and_update()` returns the result
        // immediately.
        //
        // #402 cancel-safety: if the surrounding gRPC future is dropped
        // (client disconnect, RST_STREAM, server shutdown, runtime drop),
        // control never reaches here. The guard's Drop fires anyway,
        // removing the map entry and dropping the watch Sender —
        // coalesced waiters then observe rx.changed() = Err and translate
        // to "failure" via the loop at bytestream_server.rs:2349-2358.
        //
        // in_flight_guard = None means we coalesced onto a primary that
        // FAILED (loop returned `false`) and are now retrying as our own
        // non-primary writer; the original primary's guard already cleaned
        // up the map entry. Don't touch the map here.
        if let Some(mut guard) = in_flight_guard {
            guard.set_result(result.is_ok());
            // `guard` drops at end of this scope, removing the map entry.
        }

        // Track metrics
        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ns = start_time.elapsed().as_nanos() as u64;
        instance
            .metrics
            .write_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);

        match &result {
            Ok(_) => {
                let elapsed = start_time.elapsed();
                debug!(
                    %digest,
                    size_bytes = expected_size,
                    elapsed_ms = elapsed.as_millis() as u64,
                    throughput_mbps = format!("{:.1}", throughput_mbps(expected_size, elapsed)),
                    oneshot,
                    zero_copy,
                    "ByteStream::write: CAS write completed",
                );
                instance
                    .metrics
                    .write_requests_success
                    .fetch_add(1, Ordering::Relaxed);
                instance
                    .metrics
                    .bytes_written_total
                    .fetch_add(expected_size, Ordering::Relaxed);

                // Mirroring: the oneshot path mirrors inside inner_write_oneshot
                // with data already in hand. The streaming path tees chunks to
                // the mirror channel inside inner_write (simultaneous with store
                // write), so no post-write re-read is needed.
            }
            Err(e) => {
                error!(
                    %digest,
                    expected_size,
                    elapsed_ms = start_time.elapsed().as_millis() as u64,
                    oneshot,
                    zero_copy,
                    ?e,
                    "ByteStream::write: upload failed",
                );
                instance
                    .metrics
                    .write_requests_failure
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    /// Zero-copy write handler called from `ZeroCopyByteStreamService`.
    ///
    /// Accepts any `Stream<Item = Result<WriteRequest, Status>>` instead of
    /// the tonic-specific `Streaming<WriteRequest>`. The zero-copy stream has
    /// already decoded the gRPC frames without an intermediate copy.
    async fn zero_copy_write(
        &self,
        stream: impl Stream<Item = Result<WriteRequest, Status>> + Send + Unpin + 'static,
        metadata: &http::HeaderMap,
    ) -> Result<Response<WriteResponse>, Status> {
        let start_time = Instant::now();

        let is_worker = metadata.contains_key("x-nativelink-worker");
        let is_mirror = metadata.contains_key("x-nativelink-mirror");
        // #355: same terminal-frame inspector as the tonic write path. The
        // zero-copy stream's errors flow through `Status::from_error(e.into())`
        // (zero_copy_codec.rs:229), preserving the h2::Error in the source
        // chain so `inspect_terminal_status` can pull `h2::Reason` out.
        let inspected = crate::bytestream_terminal_inspector::TerminalFrameInspectingStream::new(
            stream,
        );
        let stream = WriteRequestStreamWrapper::from(inspected)
            .await
            .err_tip(|| "Could not unwrap first stream message")
            .map_err(Into::<Status>::into)?;

        self.bytestream_write(start_time, stream, true, is_worker, is_mirror)
            .await
            .map_err(Into::into)
    }

    /// Handle a ByteStream/Read RPC with zero-copy response encoding.
    ///
    /// This replicates the logic from the tonic `read()` handler but returns a
    /// `ZeroCopyReadBody` that emits the `Bytes` data payload without copying it
    /// through prost's encoder.
    async fn zero_copy_read(
        &self,
        read_request: ReadRequest,
        metadata: &http::HeaderMap,
    ) -> Result<http::Response<tonic::body::Body>, Status> {
        let start_time = Instant::now();

        let is_worker = metadata.contains_key("x-nativelink-worker");
        let resource_info = ResourceInfo::new(&read_request.resource_name, false)?;
        let instance_name = resource_info.instance_name.as_ref();
        let expected_size = resource_info.expected_size as u64;
        let instance = self
            .instance_infos
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))
            .map_err(Into::<Status>::into)?;

        // Track read request.
        instance
            .metrics
            .read_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let store = instance.store.clone();
        let digest = DigestInfo::try_new(resource_info.hash.as_ref(), resource_info.expected_size)
            .map_err(Into::<Status>::into)?;

        // GrpcStore shortcut: proxy the read directly.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(Some(digest.into())) {
            let stream = Box::pin(
                IS_WORKER_REQUEST
                    .scope(is_worker, async {
                        grpc_store
                            .read(Request::new(read_request))
                            .await
                            .map_err(Into::<Status>::into)
                    })
                    .await?,
            );
            let body = ZeroCopyReadBody::new(stream);
            let mut http_response = http::Response::new(tonic::body::Body::new(body));
            http_response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                tonic::metadata::GRPC_CONTENT_TYPE,
            );
            return Ok(http_response);
        }

        let digest_function = resource_info
            .digest_function
            .as_deref()
            .map_or_else(
                || Ok(default_digest_hasher_func()),
                DigestHasherFunc::try_from,
            )
            .map_err(Into::<Status>::into)?;

        // Covers stream setup only (inner_read returns a Stream).
        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "ByteStream::zero_copy_read",
        );

        let read_result = self
            .inner_read(instance, digest, read_request, is_worker)
            .instrument(error_span!("bytestream_zero_copy_read"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In ByteStreamServer::zero_copy_read")
                    .map_err(Into::<Status>::into)?,
            )
            .await
            .err_tip(|| "In ByteStreamServer::zero_copy_read");

        // Track metrics.
        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ns = start_time.elapsed().as_nanos() as u64;
        instance
            .metrics
            .read_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);

        match read_result {
            Ok(stream) => {
                debug!(
                    %digest,
                    size_bytes = expected_size,
                    elapsed_ms = start_time.elapsed().as_millis() as u64,
                    "ByteStream::zero_copy_read: CAS read stream created",
                );
                instance
                    .metrics
                    .read_requests_success
                    .fetch_add(1, Ordering::Relaxed);
                instance
                    .metrics
                    .bytes_read_total
                    .fetch_add(expected_size, Ordering::Relaxed);

                // Wrap in LoggingReadStream to track throughput, emit
                // per-chunk progress diagnostics (#479), and log
                // completion at INFO. The label disambiguates this
                // from the chunked `read` call site below in journal
                // greps.
                let logging = LoggingReadStream::new(
                    stream,
                    start_time,
                    digest,
                    expected_size,
                    "bytestream_server::zero_copy_read",
                );

                let body = ZeroCopyReadBody::new(logging);
                let mut http_response = http::Response::new(tonic::body::Body::new(body));
                http_response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(http_response)
            }
            Err(e) => {
                error!(
                    %digest,
                    size_bytes = expected_size,
                    elapsed_ms = start_time.elapsed().as_millis() as u64,
                    ?e,
                    "ByteStream::zero_copy_read: failed",
                );
                instance
                    .metrics
                    .read_requests_failure
                    .fetch_add(1, Ordering::Relaxed);
                Err(e.into())
            }
        }
    }
    /// Test/diagnostic helper: get current partial_write_bytes for a given instance.
    pub fn partial_write_bytes(&self, instance_name: &str) -> u64 {
        self.instance_infos
            .get(instance_name)
            .map_or(0, |info| info.partial_write_bytes.load(Ordering::Relaxed))
    }

    /// Test/diagnostic helper: get metrics for a given instance.
    pub fn metrics(&self, instance_name: &str) -> Option<Arc<ByteStreamMetrics>> {
        self.instance_infos
            .get(instance_name)
            .map(|info| info.metrics.clone())
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamServer {
    type ReadStream = ReadStream;

    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn read(
        &self,
        grpc_request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let start_time = Instant::now();

        let is_worker = grpc_request.metadata().contains_key("x-nativelink-worker");
        let read_request = grpc_request.into_inner();
        let resource_info = ResourceInfo::new(&read_request.resource_name, false)?;
        let instance_name = resource_info.instance_name.as_ref();
        let expected_size = resource_info.expected_size as u64;
        let instance = self
            .instance_infos
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        // Track read request
        instance
            .metrics
            .read_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let store = instance.store.clone();

        let digest = DigestInfo::try_new(resource_info.hash.as_ref(), resource_info.expected_size)?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(Some(digest.into())) {
            let stream = Box::pin(grpc_store.read(Request::new(read_request)).await?);
            return Ok(Response::new(stream));
        }

        let digest_function = resource_info.digest_function.as_deref().map_or_else(
            || Ok(default_digest_hasher_func()),
            DigestHasherFunc::try_from,
        )?;

        // Covers stream setup only (inner_read returns a Stream).
        // Actual data transfer stalls are not covered by this guard.
        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "ByteStream::read",
        );
        let resp = self
            .inner_read(instance, digest, read_request, is_worker)
            .instrument(error_span!("bytestream_read"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In BytestreamServer::read")?,
            )
            .await
            .err_tip(|| "In ByteStreamServer::read")
            .map(|stream| -> Response<Self::ReadStream> {
                // Wrap in LoggingReadStream to log when the client
                // finishes consuming all data (or drops the stream
                // early), bump the per-chunk progress observer
                // (#479), and elevate completion to INFO. The label
                // disambiguates this from `zero_copy_read` in journal
                // greps.
                let logging = LoggingReadStream::new(
                    stream,
                    start_time,
                    digest,
                    expected_size,
                    "bytestream_server::read",
                );
                // Falsifies whether the response stream produced its yield
                // BEFORE handing it to tonic/h3. If items appear here but
                // the worker never sees them, the wedge is downstream
                // (tonic-h3 / h3-quinn). Captured AFTER LoggingReadStream
                // so we observe what tonic actually polls.
                let logged = StreamExt::inspect(logging, move |item| match item {
                    Ok(resp) => info!(
                        %digest,
                        item_len = resp.data.len(),
                        "h3_outbound_yielded_ok",
                    ),
                    Err(status) => info!(
                        %digest,
                        code = ?status.code(),
                        msg = %status.message(),
                        "h3_outbound_yielded_err",
                    ),
                });
                Response::new(Box::pin(logged))
            });

        // Track metrics based on result
        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ns = start_time.elapsed().as_nanos() as u64;
        instance
            .metrics
            .read_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);

        match &resp {
            Ok(_) => {
                debug!(
                    %digest,
                    size_bytes = expected_size,
                    elapsed_ms = start_time.elapsed().as_millis() as u64,
                    "ByteStream::read: CAS read stream created",
                );
                instance
                    .metrics
                    .read_requests_success
                    .fetch_add(1, Ordering::Relaxed);
                instance
                    .metrics
                    .bytes_read_total
                    .fetch_add(expected_size, Ordering::Relaxed);
            }
            Err(e) => {
                error!(
                    %digest,
                    size_bytes = expected_size,
                    elapsed_ms = start_time.elapsed().as_millis() as u64,
                    ?e,
                    "ByteStream::read: failed",
                );
                instance
                    .metrics
                    .read_requests_failure
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        resp.map_err(Into::into)
    }

    #[instrument(
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn write(
        &self,
        grpc_request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let start_time = Instant::now();

        let is_worker = grpc_request.metadata().contains_key("x-nativelink-worker");
        let is_mirror = grpc_request.metadata().contains_key("x-nativelink-mirror");
        let request = grpc_request.into_inner();
        // #355: tap the inbound stream BEFORE WriteRequestStreamWrapper so we
        // observe the raw terminal frame (clean END_STREAM vs h2 RST_STREAM
        // with reason code vs gRPC error). The wrapper consumes the typed
        // `tonic::Status` and converts to the untyped nativelink `Error`,
        // dropping the h2 source-chain we need.
        let inspected = crate::bytestream_terminal_inspector::TerminalFrameInspectingStream::new(
            request,
        );
        let stream = WriteRequestStreamWrapper::from(inspected)
            .await
            .err_tip(|| "Could not unwrap first stream message")
            .map_err(Into::<Status>::into)?;

        self.bytestream_write(start_time, stream, false, is_worker, is_mirror)
            .await
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn query_write_status(
        &self,
        grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let request = grpc_request.into_inner();

        // Track query_write_status request - we need to parse the resource name to get the instance
        if let Ok(resource_info) = ResourceInfo::new(&request.resource_name, true) {
            if let Some(instance) = self
                .instance_infos
                .get(resource_info.instance_name.as_ref())
            {
                instance
                    .metrics
                    .query_write_status_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        self.inner_query_write_status(&request)
            .await
            .err_tip(|| "Failed on query_write_status() command")
            .map_err(Into::into)
    }
}

/// Tower service wrapper that intercepts ByteStream/Write RPCs and decodes
/// `WriteRequest` messages directly from raw HTTP body frames, eliminating the
/// copy into tonic's `BytesMut` reassembly buffer.
///
/// Read and QueryWriteStatus RPCs pass through to the inner tonic service
/// unchanged.
#[derive(Clone, Debug)]
pub struct ZeroCopyByteStreamService {
    inner: Arc<ByteStreamServer>,
    tonic_service: Server<ByteStreamServer>,
}

impl ZeroCopyByteStreamService {
    /// Apply compression settings to the inner tonic service (for non-Write RPCs).
    pub fn accept_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.tonic_service = self.tonic_service.accept_compressed(encoding);
        self
    }

    /// Apply compression settings to the inner tonic service (for non-Write RPCs).
    pub fn send_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.tonic_service = self.tonic_service.send_compressed(encoding);
        self
    }
}

impl tonic::server::NamedService for ZeroCopyByteStreamService {
    const NAME: &'static str = "google.bytestream.ByteStream";
}

impl tower::Service<http::Request<tonic::body::Body>> for ZeroCopyByteStreamService {
    type Response = http::Response<tonic::body::Body>;
    type Error = core::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path();

        if path == "/google.bytestream.ByteStream/Write" {
            let inner = self.inner.clone();
            Box::pin(async move {
                let (parts, body) = req.into_parts();
                let metadata = parts.headers;
                let stream = ZeroCopyWriteStream::new(body);

                let result = inner.zero_copy_write(stream, &metadata).await;

                match result {
                    Ok(response) => {
                        let (resp_metadata, write_response, _extensions) = response.into_parts();
                        // Encode the WriteResponse as a gRPC frame.
                        let body_bytes = encode_grpc_unary_response(&write_response);
                        let body = GrpcUnaryBody::new(body_bytes);
                        let mut http_response = http::Response::new(tonic::body::Body::new(body));
                        *http_response.headers_mut() = resp_metadata.into_headers();
                        http_response.headers_mut().insert(
                            http::header::CONTENT_TYPE,
                            tonic::metadata::GRPC_CONTENT_TYPE,
                        );
                        Ok(http_response)
                    }
                    Err(status) => Ok(status.into_http()),
                }
            })
        } else if path == "/google.bytestream.ByteStream/Read" {
            let inner = self.inner.clone();
            Box::pin(async move {
                let (parts, body) = req.into_parts();
                let metadata = parts.headers;

                // Decode the unary ReadRequest from the HTTP body.
                let read_request: ReadRequest = match decode_unary_request(body).await {
                    Ok(req) => req,
                    Err(status) => return Ok(status.into_http()),
                };

                match inner.zero_copy_read(read_request, &metadata).await {
                    Ok(http_response) => Ok(http_response),
                    Err(status) => Ok(status.into_http()),
                }
            })
        } else {
            // Delegate QueryWriteStatus to the standard tonic path.
            self.tonic_service.call(req)
        }
    }
}
