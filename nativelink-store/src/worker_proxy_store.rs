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
use core::time::Duration;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tonic::Request;
use tracing::{debug, error, info, trace, warn};

use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use nativelink_proto::build::bazel::remote::execution::v2::BatchReadBlobsRequest;
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
    make_buf_channel_pair_with_size,
};
use nativelink_util::common::{DigestInfo, make_precondition_failure_any};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::metrics_utils::CounterWithTime;
use nativelink_util::store_trait::{
    IS_MIRROR_REQUEST, IS_WORKER_REQUEST, ItemCallback, MarkStableDelegation, PinDelegation,
    REDIRECT_PREFIX, StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike,
    StoreOptimizations, UploadSizeInfo,
};

use crate::batch_read_coalescer::{BatchFn, BatchReadCoalescer};
use crate::chunked_signal::error_has_backpressure_signal;
use crate::fast_slow_store::INNER_MISS_NO_TERMINATE;
use crate::grpc_store::GrpcStore;
use crate::singleflight::SingleflightMap;

/// A store wrapper that transparently proxies CAS reads from workers when
/// the inner store returns NotFound. This enables worker-to-worker blob sharing.
///
/// Behavior:
/// - `get_part()`: Try inner store first. If NotFound, consult the locality map
///   for workers that have the digest, try reading from a worker.
/// - `has()` / `has_with_results()`: Check inner store first. For any digests
///   still missing, consult the locality map — if a worker has the blob, report
///   it as present. This is safe because workers pin blobs until they are
///   uploaded to the server CAS, so a locality entry implies the blob is
///   retrievable (either from the worker or already in the server CAS).
/// - `update()`: Pass through to inner store.
pub struct WorkerProxyStore {
    inner: Store,
    /// Blob locality map — digest → worker endpoints.
    locality_map: SharedBlobLocalityMap,
    /// Cached GrpcStore connections to worker endpoints.
    worker_connections: RwLock<HashMap<Arc<str>, Store>>,
    /// Per-endpoint mirror health for quarantining flaky workers and
    /// per-endpoint concurrency permits for fair fan-out.
    mirror_state: RwLock<HashMap<Arc<str>, MirrorEndpointState>>,
    /// Round-robin counter for mirror endpoint selection.
    mirror_counter: AtomicU64,
    /// When true, race peer fetches against server fetches in get_part.
    /// Only workers enable this — servers use the sequential path which
    /// (a) consults the locality map directly to proxy data for non-worker
    /// callers, or (b) returns a `Code::FailedPrecondition` `REDIRECT_PREFIX`
    /// error to worker callers so the worker can fetch directly from peers
    /// without server-side bandwidth amplification. `AtomicBool` so the
    /// toggle can be flipped after the proxy is wrapped in `Arc`.
    race_peers: AtomicBool,
    /// When true, the bytestream_write fast path consults the locality map
    /// after the inner-store check fails: if any worker is reported as
    /// holding the blob, the server synchronously confirms with that
    /// worker (`worker.has(digest)`) before short-circuiting the upload.
    /// The confirmation RPC also bumps the worker's LRU as a side effect.
    /// Default true; the toggle exists so an operator can disable the fast
    /// path at runtime if needed.
    consult_locality_in_has: AtomicBool,
    /// Optional TLS config for connecting to worker CAS endpoints.
    /// When set, connections use `grpcs://` with this TLS config.
    worker_tls_config: Option<ClientTlsConfig>,
    /// Total mirror attempts (any path).
    mirror_total_attempted: AtomicU64,
    /// Mirror attempts that completed without error.
    mirror_total_succeeded: AtomicU64,
    /// Mirror attempts skipped because no permit was available within the
    /// path's deadline (small-blob 50 ms timeout, streaming try_acquire).
    mirror_dropped_no_permit: AtomicU64,
    /// Mirror attempts skipped because no eligible (non-quarantined)
    /// endpoint could be selected.
    mirror_dropped_quarantined: AtomicU64,
    /// `#88: batch_read_small_blobs racing wastes bandwidth — both peer
    /// and server batches run to completion`. Opportunistic batching
    /// of small-blob server→worker proxy reads. Off by default —
    /// enabled via `enable_batch_small_blob_reads()` after a soak
    /// window. Once enabled, eligible reads (per
    /// `BatchReadCoalescer::is_eligible`) coalesce into one
    /// `BatchReadBlobs` RPC per coalesce window per endpoint instead
    /// of N individual ByteStream Read RPCs.
    batch_small_blob_reads: AtomicBool,
    /// Lazy-initialized `BatchReadCoalescer`. The coalescer needs a
    /// `BatchFn` closure that captures `Arc<Self>` to look up the
    /// per-endpoint GrpcStore connection — this is a self-referential
    /// dependency, so we hand the coalescer a `Weak<Self>` capture and
    /// initialize it on first use via `OnceLock`.
    batch_read_coalescer: OnceLock<Arc<BatchReadCoalescer>>,
    /// CDN-tee counters (#230). Track the cache-write side-task lifecycle
    /// for the streaming `get_part_and_cache` path. The four counters
    /// triangulate why a peer-fetched blob did or didn't end up in the
    /// inner cache — without them, audits like #229 had to infer from
    /// log markers and got 88 % silent-failure rates.
    ///
    /// `Arc<AtomicU64>` (not bare `AtomicU64`) because the cache-write
    /// task is spawned `tokio::spawn`-detached and outlives the
    /// `get_part_and_cache` call's `&self` borrow. The Arc lets the
    /// detached task increment the same counter the
    /// `MetricsComponent::publish` impl reads.
    ///
    /// Incremented when the spawned cache task starts (one per eligible
    /// peer-fetch, i.e. full-blob read ≤ `MAX_CACHE_BLOB_SIZE`).
    cdn_tee_cache_attempts_total: Arc<AtomicU64>,
    /// Incremented when the spawned cache task's `inner.update` returned
    /// `Ok` and the task completed within its timeout.
    cdn_tee_cache_completed_total: Arc<AtomicU64>,
    /// Incremented when the per-chunk forward loop observed
    /// `cache_tx.try_send` returning `Full` and abandoned the cache-tee
    /// for this blob (continuing forwarding to Bazel only).
    cdn_tee_cache_abandoned_full_total: Arc<AtomicU64>,
    /// Incremented when the consumer (Bazel) disconnected mid-blob
    /// (forward `Err`), causing the forward loop to drop `cache_tx`
    /// and abandon the cache-tee.
    cdn_tee_cache_abandoned_consumer_eof_total: Arc<AtomicU64>,
    /// #58 — counter incremented every time the peer-fetch error path at
    /// `:1650` fires (`WorkerProxyStore: peer fetch failed` with writer
    /// not pipe-broken). Lets `#35 OQ-8` preflight check the rate
    /// without journal grep. Observed 4 events / 7 d in production
    /// pre-counter (Phase 5 audit `2026-06-04`).
    worker_proxy_peer_fetch_notfound_total: CounterWithTime,
    /// (CCS-drop §3.3 / §8 Q4) Replacement for `ccs_incomplete_entries_counter`
    /// when `disable_completeness_check = true` removes CCS's stale-AC signal.
    ///
    /// Incremented in `get_part_sequential` when BOTH the inner store AND all
    /// worker peers return NotFound — the stale-AC symptom: Bazel received a
    /// cache-hit AC entry but the referenced blob is gone from both server CAS
    /// and every known worker. Each increment is one Bazel-visible NotFound
    /// that will trigger an eviction-retry re-execution.
    ///
    /// **Non-zero rate → stale AC entries accumulating.** Sustained rate
    /// growth → AC TTL / cap needs adjustment (§8 Q1). Zero for hours after
    /// enabling `disable_completeness_check` → CCS removal is safe from a
    /// stale-AC perspective (no new accumulation).
    ///
    /// Published under: `<store_key>_wps_cas_and_peer_notfound_total_counter`.
    wps_cas_and_peer_notfound_total: CounterWithTime,
    /// #130 — singleflight/dedup map for concurrent same-digest peer
    /// fetches. Collapses the "N callers, same digest, ms apart" cohort
    /// pattern into 1 leader peer-fetch + N-1 waiters that re-read from
    /// local CAS once the leader's cache task completes. Bypassed for
    /// partial-range reads, oversized blobs, and when the cap is
    /// exceeded — same gating predicate as the CDN-tee. The signal
    /// payload is empty (`Vec::new()`); waiters use it as a
    /// "cache-now-populated" barrier and stream from `inner.get_part`,
    /// preserving the `streaming_required` design from
    /// `project_sf_wireup_design_streaming_required` (no `Vec<Bytes>`
    /// buffering anywhere).
    singleflight: Arc<SingleflightMap>,
}

/// Per-endpoint mirror state: in-flight permits and consecutive-failure tracking.
struct MirrorEndpointState {
    /// Concurrency limit for in-flight mirror writes to this worker.
    /// Per-worker rather than global so one overloaded worker can't starve
    /// healthy workers of mirror capacity.
    permits: Arc<Semaphore>,
    /// Number of consecutive mirror failures since the last success.
    consecutive_failures: u32,
    /// Timestamp of the first failure in the current streak; used to decide
    /// whether the failures are bursty enough to warrant quarantine.
    first_failure_at: Option<Instant>,
    /// If `Some`, endpoint is in quarantine until this instant. While
    /// quarantined the endpoint is skipped during mirror selection.
    quarantined_until: Option<Instant>,
    /// Last reported `mirror_blobs` total bytes from the worker. Updated
    /// by `record_mirror_capacity` on every BlobsAvailable tick.
    /// `None` until the worker reports its first capacity (older workers
    /// or workers with no CAS server never report; treat as unknown ⇒
    /// no pre-check filtering).
    mirror_used_bytes: Option<u64>,
    /// Last reported `MIRROR_BLOBS_MAX_BYTES` from the worker. Stored
    /// per-endpoint because workers can be configured independently.
    mirror_max_bytes: Option<u64>,
}

impl MirrorEndpointState {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MIRROR_PERMITS_PER_WORKER)),
            consecutive_failures: 0,
            first_failure_at: None,
            quarantined_until: None,
            mirror_used_bytes: None,
            mirror_max_bytes: None,
        }
    }

    /// Returns true if a mirror write of `size_bytes` would fit within
    /// the last-reported capacity. Returns true for unknown capacity
    /// (no report yet) so we don't filter out workers we have no
    /// information about. Also returns true defensively when `max == 0`
    /// (treat as "no cap configured" rather than "instantly full") so a
    /// future code path that records a stale `(used, 0)` cannot lock the
    /// peer out of all picker rotations.
    fn fits(&self, size_bytes: u64) -> bool {
        match (self.mirror_used_bytes, self.mirror_max_bytes) {
            (Some(_), Some(0)) => true,
            (Some(used), Some(max)) => used.saturating_add(size_bytes) <= max,
            _ => true,
        }
    }
}

/// Maximum concurrent mirror writes per worker endpoint. Sized to keep
/// total in-flight bytes within the server's OOM budget: with ~10 workers,
/// 16 permits × 10 = 160 concurrent uploads, each of which can hold up to
/// ~72 MiB in its `buf_channel`. Higher values risk the RSS spike pattern
/// seen during the 2026-03-25 write burst.
const MIRROR_PERMITS_PER_WORKER: usize = 16;

/// CDN-tee cache mpsc capacity (#230). Bytes flow as:
///   peer chunks → forward to Bazel → `try_send` to cache mpsc → cache task
///                                                              → inner.update
///
/// The cap is small on purpose. The CDN-tee is a best-effort fan-out:
/// the user-explicit contract is "Bazel reader NEVER blocks on cache",
/// so the per-chunk loop uses `try_send` and abandons on `Full`. A
/// large buffer would let the cache task lag arbitrarily far behind
/// the peer-reader; a tiny buffer detects cache stall fast and frees
/// the abandon path to take over.
///
/// Why 4 specifically:
/// - large enough to absorb a micro-burst (peer delivers 2-3 chunks
///   back-to-back while cache_task is briefly preempted),
/// - small enough that a slow inner-store update (sustained cache-tier
///   latency, e.g. ZFS txg-sync hiccup) trips the abandon path within
///   one peer-reader "burst" rather than letting the cache task
///   accumulate hundreds of MiB of buffered chunks behind a stalled
///   write,
/// - matches the `mirror_channel = 16` direction the user requested
///   (tighter back-pressure, less memory) for adjacent fan-out paths,
///   and is even tighter because the cache-tee can be abandoned with
///   no correctness loss while a mirror cannot.
///
/// Total memory budget per in-flight peer-fetch:
///   - cache mpsc:   4 chunks × ~3 MiB `read_buffer_size` ≈ 12 MiB
///   - proxy buffer: bounded by `DEFAULT_BUF_CHANNEL_CAPACITY = 1024`
///     slots × peer chunk size. In practice peer chunks are also
///     `read_buffer_size`-bounded (~3 MiB on FilesystemStore-backed
///     peers), so the steady-state budget is dominated by whichever of
///     `proxy_rx`/cache mpsc the consumer is draining slowest. With the
///     post-#230 architecture the consumer drains proxy_rx at peer rate
///     (so it sits near-empty under healthy load); a stalled consumer
///     fills proxy_rx and the cache abandon path frees the cache mpsc.
///     Worst-case per fetch is therefore the larger of the two
///     channels' capacity-times-chunk-size product.
/// With ~50 concurrent peer-fetches across the fleet, 12 MiB cache + a
/// near-empty proxy_rx (consumer draining at peer rate) keeps total
/// well below the 8 GiB MemoryStore fast-tier budget that motivated
/// the 2026-03-25 OOM tuning. The Bazel-disconnect abandon path drops
/// `proxy_rx` immediately so the peer task unblocks fast (M1 fix,
/// 2026-05-02) — without it, proxy_tx could pin up to 1024 chunks per
/// orphaned fetch indefinitely.
const CDN_TEE_CACHE_MPSC_CAP: usize = 4;

/// Typed signal written into `cache_tx` via `send_error` on every
/// intentional best-effort cache fan-out abandonment (mpsc full or
/// Bazel consumer disconnected). Inner stores (`fast_slow_store`,
/// `existence_cache_store`) detect this marker to demote the resulting
/// error log from `error!` to `debug!` — the Bazel read already
/// succeeded and the blob is durable via the worker's own upload.
///
/// Inner-store demotion check: call
/// [`is_cache_fanout_abandonment`] which tests `err.code ==
/// Code::Aborted && err.messages.iter().any(|m|
/// m.contains(CACHE_FANOUT_ABANDONED_MARKER))`.
///
/// LOAD-BEARING INVARIANT: this relies on `err_tip` (via
/// `err_tip_with_code`) preserving `err.code` and the marker
/// substring across every store-layer hop between this producer
/// and the classifier.  If a future layer rewraps the error with
/// a fresh `Code` (e.g. `make_err!(Code::Internal, "… {e}")`)
/// the discriminator silently breaks — see the FU-7 follow-up
/// (typed proto-detail) for the robust future fix.  The
/// FSS-seam tests guard this invariant: a future layer absorbing
/// `Code::Aborted` would red-fail those tests.
pub(crate) const CACHE_FANOUT_ABANDONED_MARKER: &str =
    "WorkerProxyStore: cache fan-out abandoned (best-effort)";

/// Returns `true` iff `err` is an intentional best-effort
/// cache fan-out abandonment, signalled by
/// `Code::Aborted + CACHE_FANOUT_ABANDONED_MARKER`.
///
/// Called at every inner-store demotion site (`fast_slow_store`
/// chunked ~2003, non-chunked ~5228, `existence_cache_store`
/// ~616) to keep the predicate in one place and the error in
/// one place — a future change to the abandonment code or marker
/// only needs to be made here.
pub(crate) fn is_cache_fanout_abandonment(err: &nativelink_error::Error) -> bool {
    err.code == nativelink_error::Code::Aborted
        && err
            .messages
            .iter()
            .any(|m| m.contains(CACHE_FANOUT_ABANDONED_MARKER))
}

/// Wall-clock cap on the CDN-tee spawned cache task (#230). The task runs
/// `inner.update(cache_rx, ExactSize(digest.size_bytes()))`, which is
/// bounded only by the cache mpsc and the inner store's own internals.
/// Without this timeout, a wedged peer (stops sending chunks but never
/// EOFs cache_tx because `forward_fut` is also stalled) or a wedged
/// inner store could leak the spawned task and its FilesystemStore
/// in-flight tracker entry indefinitely.
///
/// 60s is the same order of magnitude as the per-chunk write timeout
/// upstream and gives a real ZFS hiccup time to finish without
/// hair-trigger abandoning a large blob mid-write.
const CDN_TEE_CACHE_TASK_TIMEOUT: Duration = Duration::from_secs(60);
/// Consecutive failures within `MIRROR_FAILURE_WINDOW` that trigger quarantine.
const MIRROR_FAILURE_THRESHOLD: u32 = 5;
/// Window over which `MIRROR_FAILURE_THRESHOLD` failures must occur to trigger
/// quarantine. Older streaks are reset rather than escalating.
const MIRROR_FAILURE_WINDOW: Duration = Duration::from_secs(10);
/// How long to skip a quarantined endpoint before retrying it.
const MIRROR_QUARANTINE_DURATION: Duration = Duration::from_secs(30);

impl core::fmt::Debug for WorkerProxyStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WorkerProxyStore")
            .field("inner", &self.inner)
            .field("worker_connections", &self.worker_connections.read().len())
            .finish()
    }
}

// Manual `MetricsComponent` impl rather than `derive` because per-endpoint
// gauges have variable cardinality — they need to be enumerated under the
// `mirror_state` lock at publish time. Snapshot-then-release so the read
// lock is held for the minimum window.
#[expect(
    clippy::cognitive_complexity,
    reason = "complexity arises from publish! macro expansion"
)]
impl MetricsComponent for WorkerProxyStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Inner store under its own group, mirroring the previous derive layout.
        {
            let _enter = group!("inner_store").entered();
            self.inner.publish(MetricKind::Component, MetricFieldData::default())?;
        }

        // #130 SingleflightMap counters and gauges. Without this group
        // the `#[metric]` annotations on `SingleflightMap` /
        // `SingleflightMapInner` (current_inflight_bytes,
        // total_dedup_hits, total_bypasses_cap, max_inflight_bytes,
        // total_waiter_fallback_to_direct) NEVER reach Prometheus
        // because the parent's manual `MetricsComponent::publish` impl
        // takes over from the (absent) derive.
        {
            let _enter = group!("singleflight").entered();
            self.singleflight
                .publish(MetricKind::Component, MetricFieldData::default())?;
        }

        publish!(
            "mirror_total_attempted",
            &self.mirror_total_attempted,
            MetricKind::Counter,
            "Total mirror attempts across all paths"
        );
        publish!(
            "mirror_total_succeeded",
            &self.mirror_total_succeeded,
            MetricKind::Counter,
            "Mirror attempts that completed without error"
        );
        publish!(
            "mirror_dropped_no_permit",
            &self.mirror_dropped_no_permit,
            MetricKind::Counter,
            "Mirrors skipped because no per-worker permit was available"
        );
        publish!(
            "mirror_dropped_quarantined",
            &self.mirror_dropped_quarantined,
            MetricKind::Counter,
            "Mirrors skipped because no eligible endpoint could be selected"
        );

        publish!(
            "cdn_tee_cache_attempts_total",
            self.cdn_tee_cache_attempts_total.as_ref(),
            MetricKind::Counter,
            "CDN-tee cache write tasks spawned (eligible peer-fetched blobs)"
        );
        publish!(
            "cdn_tee_cache_completed_total",
            self.cdn_tee_cache_completed_total.as_ref(),
            MetricKind::Counter,
            "CDN-tee cache write tasks that completed inner.update successfully"
        );
        publish!(
            "cdn_tee_cache_abandoned_full_total",
            self.cdn_tee_cache_abandoned_full_total.as_ref(),
            MetricKind::Counter,
            "CDN-tee writes abandoned because cache mpsc was full \
             (cache slower than peer-reader; Bazel kept being served)"
        );
        publish!(
            "cdn_tee_cache_abandoned_consumer_eof_total",
            self.cdn_tee_cache_abandoned_consumer_eof_total.as_ref(),
            MetricKind::Counter,
            "CDN-tee writes abandoned because Bazel consumer disconnected mid-blob"
        );
        publish!(
            "worker_proxy_peer_fetch_notfound_total",
            &self.worker_proxy_peer_fetch_notfound_total,
            MetricKind::Counter,
            "Peer-fetch errors logged at the non-derivative error! site \
             (writer still open). Per #35 OQ-8: production population for \
             Phase 5 BlobsAvailable; lets future preflights read rate \
             without journal grep"
        );
        publish!(
            "wps_cas_and_peer_notfound_total",
            &self.wps_cas_and_peer_notfound_total,
            MetricKind::Counter,
            "CCS-drop §3.3 stale-AC signal: incremented in get_part_sequential when \
             BOTH the inner CAS store AND all worker peers return NotFound — the \
             stale-AC symptom (Bazel received a cache-hit AC entry but the blob is \
             gone from server CAS and all known workers). Non-zero rate → stale AC \
             entries accumulating; sustained growth → AC TTL/cap needs adjustment."
        );

        // Snapshot per-endpoint state under a brief read lock, then publish
        // outside the lock so we never hold it across the macro's tracing
        // events.
        let snapshot: Vec<(Arc<str>, usize, u32, bool)> = {
            let state = self.mirror_state.read();
            state
                .iter()
                .map(|(ep, st)| {
                    let quarantined = st
                        .quarantined_until
                        .is_some_and(|t| t > Instant::now());
                    (
                        ep.clone(),
                        st.permits.available_permits(),
                        st.consecutive_failures,
                        quarantined,
                    )
                })
                .collect()
        };
        for (endpoint, available, failures, quarantined) in snapshot {
            let _enter = group!(endpoint.as_ref()).entered();
            publish!(
                "mirror_available_permits",
                &(available as u64),
                MetricKind::Counter,
                "Per-endpoint mirror permits available right now"
            );
            publish!(
                "mirror_consecutive_failures",
                &(u64::from(failures)),
                MetricKind::Counter,
                "Per-endpoint consecutive mirror failures"
            );
            publish!(
                "mirror_quarantined",
                &(u64::from(quarantined)),
                MetricKind::Counter,
                "Per-endpoint quarantine flag (1 if currently quarantined)"
            );
        }

        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Returns true if the error code indicates a connection-level failure,
/// meaning the cached connection should be removed.
fn is_connection_error(e: &Error) -> bool {
    matches!(e.code, Code::Unavailable | Code::Unknown)
}

/// Architectural invariant: if the local chain failed to serve, ALWAYS
/// consult peers before giving up. The bytestream Read RPC's defense against
/// missing blobs is the cluster-wide peer-fetch hop; any failure shape that
/// means "this digest can't be served from the local chain" must fall through
/// to `try_read_from_worker` instead of being returned directly.
///
/// Distinct from `existence_cache_store::is_unrecoverable_read_error`: the
/// existence-cache predicate is conservative because it controls whether to
/// drop a positive cache entry, and re-evicting on every connectivity blip
/// would force re-uploads (expensive). Here the cost asymmetry is reversed:
/// one extra peer RPC against a digest the cluster might still hold, vs.
/// surfacing a spurious read failure to the client. We therefore include
/// every code that means "blob not served by the local chain":
///   * `NotFound`           — primary case (local store evicted / never had it).
///   * `DataLoss`           — verifier rejected stored bytes; peer copy may be intact.
///   * `Internal`           — `StreamingBlobWriter::Drop`, async cancellation, etc.
///   * `OutOfRange`         — local store reported truncated blob; peer may have full size.
///   * `Unavailable`        — transient inner-store unavailability (e.g. ZFS hiccup).
///   * `Unknown`            — tonic maps unrecognized HTTP/2 statuses (e.g. proxy
///                            bouncing the connection mid-stream) to `Unknown`;
///                            same shape as `Internal` from the caller's view.
///   * `ResourceExhausted`  — local CPU / memory / connection cap saturated; peers
///                            with available capacity may still serve.
///
/// Codes deliberately excluded:
///   * `Aborted`            — used by tonic for ABA conflicts on AC writes; not a
///                            "blob can't be served" signal.
///   * `DeadlineExceeded`   — caller's deadline already passed; trying peers wastes
///                            work on a stream the caller has stopped reading.
///   * `Cancelled`          — caller has gone away.
///   * `PermissionDenied` / `Unauthenticated` — deliberate authz refusal that the
///                            caller must see.
///   * `InvalidArgument`    — client bug; peers will reject the same input.
///   * `FailedPrecondition` — reserved for the redirect-prefix protocol (handled
///                            in the explicit `FailedPrecondition` arm above).
///   * `Unimplemented` / `AlreadyExists` / `Ok` — not error shapes that map to
///                            "try peers".
fn should_try_peers(code: Code) -> bool {
    matches!(
        code,
        Code::NotFound
            | Code::DataLoss
            | Code::Internal
            | Code::OutOfRange
            | Code::Unavailable
            | Code::Unknown
            | Code::ResourceExhausted
    )
}

/// Locality-eviction policy: should this peer-fetch failure cause us to drop
/// the locality entry mapping `digest -> endpoint`?
///
/// We evict ONLY on signals that the peer genuinely no longer holds (or
/// cannot deliver) the bytes for this specific digest:
///   * `Code::NotFound`  — peer evicted the blob from its local cache.
///   * `Code::DataLoss`  — peer delivered corrupt or truncated bytes (the
///                         stored copy is unusable).
///
/// All other failures are treated as TRANSIENT for the locality entry. A
/// `DeadlineExceeded`, `Unavailable`, `Internal`, `Aborted`, or transport
/// blip does not prove the peer has lost the blob — only that this specific
/// fetch attempt failed. Evicting on transients permanently destroys
/// locality for blobs only one peer has, after a single network hiccup,
/// which then forces every subsequent FindMissingBlobs to miss the fast
/// path even though the peer still holds the data.
///
/// Worker-level health bookkeeping (quarantine, connection drop) is handled
/// separately by `is_connection_error` / `is_definitive_unreachable` and is
/// orthogonal to this digest-level policy.
fn should_evict_locality_on_peer_error(e: &Error) -> bool {
    matches!(e.code, Code::NotFound | Code::DataLoss)
}

/// Bytes a peer OWES for a Read RPC against a blob of `blob_size`,
/// starting at `offset`, with optional `length` (None = unbounded
/// tail). Used by the WPS Ok+0-bytes guard on both
/// `try_read_from_worker` and `try_read_from_endpoints` to decide
/// whether a clean-Ok+empty-stream is a stale-positive lie (peer
/// owed bytes, delivered zero → must evict locality) or a legitimate
/// zero-byte response (offset past EOF, or empty zero-digest blob).
///
/// #500: prior to this helper, the guard only fired for
/// `length.is_none()` whole-blob reads. Bazel's parallel-chunk reads
/// always carry `length=Some(chunk_size)`, so silent truncations
/// from a peer slipped through.
const fn owed_bytes(blob_size: u64, offset: u64, length: Option<u64>) -> u64 {
    if offset >= blob_size {
        return 0;
    }
    let tail = blob_size - offset;
    match length {
        None => tail,
        Some(l) => {
            if l < tail {
                l
            } else {
                tail
            }
        }
    }
}

/// Returns true for transport-level errors that prove the peer is gone:
/// `ConnectionRefused` (no listener), `NetworkUnreachable`, `HostUnreachable`.
/// Distinct from generic `Code::Unavailable` (which also covers transients
/// like `KeepAliveTimedOut`, `EOF without close_notify`, `RST_STREAM` —
/// those legitimately recover on retry and must NOT fast-quarantine).
fn is_definitive_unreachable(e: &Error) -> bool {
    if e.code != Code::Unavailable {
        return false;
    }
    e.messages.iter().any(|m| {
        m.contains("ConnectionRefused")
            || m.contains("NetworkUnreachable")
            || m.contains("HostUnreachable")
    })
}

/// Classification of a mirror-write failure for `record_mirror_failure`.
///
/// Quarantine policy depends on the kind:
///   * `DefinitiveUnreachable` — fast-quarantine on the first failure
///     (the peer is provably gone; round-robin to it is wasted I/O).
///   * `Generic` — only quarantine after `MIRROR_FAILURE_THRESHOLD`
///     failures inside `MIRROR_FAILURE_WINDOW` (transient errors recover
///     on retry; one or two failures must NOT quarantine).
///   * `Saturated` — the peer's mirror cap is full. The peer is healthy
///     but cannot accept this blob right now; the picker should route the
///     next attempt elsewhere. Saturation must NOT count toward the
///     consecutive-failure streak — it is not evidence the peer is broken.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum MirrorFailureKind {
    Generic,
    DefinitiveUnreachable,
    Saturated,
}

/// Classifies an Error from a mirror write into a quarantine policy
/// signal. Centralized so call sites cannot accidentally treat a
/// ResourceExhausted (cap-full) response the same as a generic transient.
fn classify_mirror_failure(e: &Error) -> MirrorFailureKind {
    if e.code == Code::ResourceExhausted {
        return MirrorFailureKind::Saturated;
    }
    if is_definitive_unreachable(e) {
        return MirrorFailureKind::DefinitiveUnreachable;
    }
    MirrorFailureKind::Generic
}

/// Marker substring embedded by the bytestream tee producer in the
/// `send_error` it fires on the mirror channel when one or more chunks
/// were dropped due to backpressure (the 16-slot mirror_tx tee filled).
///
/// The downstream mirror task observes this exact substring on the Error
/// returned from `GrpcStore::update` (the producer's `send_error` payload
/// is what `buf_channel::recv` surfaces, with `err_tip` strings appended
/// by intermediate layers but the original message preserved). When the
/// substring is present AND the Code is `Code::Aborted`, the
/// `mirror_stream: failed to stream blob to worker` event is the
/// by-design consequence of best-effort tee backpressure (the receiver
/// will re-fetch on demand), NOT a real worker failure — log at DEBUG.
///
/// (#344, 2026-05-09: 21/min residual WARN noise was double-counting an
/// expected event already logged at INFO by the producer.)
pub const MIRROR_TEE_BACKPRESSURE_MARKER: &str = "mirror tee backpressure: chunks dropped";

/// Returns true if the error originated from the bytestream tee producer
/// signalling that chunks were dropped to backpressure (and therefore
/// the mirror failure is by-design, not a network/peer fault).
///
/// Robust against `err_tip` wrapping by intermediate layers: matches on
/// the marker substring inside any of the error's accumulated messages,
/// AND requires `Code::Aborted` so a coincidental substring in a
/// different code path cannot trigger the demotion.
fn is_mirror_tee_backpressure_error(e: &Error) -> bool {
    e.code == Code::Aborted
        && e.messages
            .iter()
            .any(|m| m.contains(MIRROR_TEE_BACKPRESSURE_MARKER))
}

impl WorkerProxyStore {
    pub fn new(inner: Store, locality_map: SharedBlobLocalityMap) -> Arc<Self> {
        Arc::new(Self {
            inner,
            locality_map,
            worker_connections: RwLock::new(HashMap::new()),
            mirror_state: RwLock::new(HashMap::new()),
            mirror_counter: AtomicU64::new(0),
            race_peers: AtomicBool::new(false),
            consult_locality_in_has: AtomicBool::new(true),
            worker_tls_config: None,
            mirror_total_attempted: AtomicU64::new(0),
            mirror_total_succeeded: AtomicU64::new(0),
            mirror_dropped_no_permit: AtomicU64::new(0),
            mirror_dropped_quarantined: AtomicU64::new(0),
            batch_small_blob_reads: AtomicBool::new(false),
            batch_read_coalescer: OnceLock::new(),
            cdn_tee_cache_attempts_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_completed_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_abandoned_full_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_abandoned_consumer_eof_total: Arc::new(AtomicU64::new(0)),
            worker_proxy_peer_fetch_notfound_total: CounterWithTime::default(),
            wps_cas_and_peer_notfound_total: CounterWithTime::default(),
            singleflight: SingleflightMap::new(),
        })
    }

    /// Create a new WorkerProxyStore with TLS configuration for
    /// connecting to worker CAS endpoints.
    pub fn new_with_tls(
        inner: Store,
        locality_map: SharedBlobLocalityMap,
        tls_config: ClientTlsConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            locality_map,
            worker_connections: RwLock::new(HashMap::new()),
            mirror_state: RwLock::new(HashMap::new()),
            mirror_counter: AtomicU64::new(0),
            race_peers: AtomicBool::new(false),
            consult_locality_in_has: AtomicBool::new(true),
            worker_tls_config: Some(tls_config),
            mirror_total_attempted: AtomicU64::new(0),
            mirror_total_succeeded: AtomicU64::new(0),
            mirror_dropped_no_permit: AtomicU64::new(0),
            mirror_dropped_quarantined: AtomicU64::new(0),
            batch_small_blob_reads: AtomicBool::new(false),
            batch_read_coalescer: OnceLock::new(),
            cdn_tee_cache_attempts_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_completed_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_abandoned_full_total: Arc::new(AtomicU64::new(0)),
            cdn_tee_cache_abandoned_consumer_eof_total: Arc::new(AtomicU64::new(0)),
            worker_proxy_peer_fetch_notfound_total: CounterWithTime::default(),
            wps_cas_and_peer_notfound_total: CounterWithTime::default(),
            singleflight: SingleflightMap::new(),
        })
    }

    /// Enable racing peer fetches against server fetches.
    /// Only workers should call this — servers should leave it disabled.
    pub fn enable_race_peers(&self) {
        self.race_peers.store(true, Ordering::Relaxed);
    }

    /// `#88: batch_read_small_blobs racing wastes bandwidth — both peer
    /// and server batches run to completion`. Enable opportunistic
    /// batching of server→worker small-blob proxy reads. When enabled,
    /// concurrent same-target small-blob fetches coalesce into one
    /// `BatchReadBlobs` RPC per coalesce window per endpoint instead
    /// of N individual ByteStream Read RPCs.
    ///
    /// Default: OFF. Conservative — perf opt on existing path; soak
    /// before flipping. Production-safe to flip at runtime; takes
    /// effect on the next eligible read.
    pub fn enable_batch_small_blob_reads(&self) {
        self.batch_small_blob_reads.store(true, Ordering::Relaxed);
    }

    /// Disable opportunistic batching of small-blob proxy reads.
    /// Operator kill-switch — flips reads back to per-blob ByteStream
    /// Read.
    pub fn disable_batch_small_blob_reads(&self) {
        self.batch_small_blob_reads.store(false, Ordering::Relaxed);
    }

    /// Inspector: whether batched small-blob proxy reads are enabled.
    pub fn batch_small_blob_reads_enabled(&self) -> bool {
        self.batch_small_blob_reads.load(Ordering::Relaxed)
    }

    /// Enable the locality-aware fast paths in `has_with_results` and the
    /// bytestream_write fast-path. On by default; this is the kill-switch
    /// re-arm.
    pub fn enable_locality_in_has(&self) {
        self.consult_locality_in_has.store(true, Ordering::Relaxed);
    }

    /// Disable the locality-aware fast paths. Operator kill-switch — flips
    /// `has_with_results` and `has` back to inner-store-only so Bazel's
    /// FindMissingBlobs / bytestream short-circuit no longer trust the
    /// locality table.
    pub fn disable_locality_in_has(&self) {
        self.consult_locality_in_has.store(false, Ordering::Relaxed);
    }

    /// Inspector for the bytestream fast-path; returns whether locality
    /// consultation is enabled.
    pub fn locality_in_has_enabled(&self) -> bool {
        self.consult_locality_in_has.load(Ordering::Relaxed)
    }

    /// CDN-tee counter snapshot (#230). Returns
    /// `(attempts, completed, abandoned_full, abandoned_consumer_eof)`.
    /// Used by regression tests to assert which abandon path fired and
    /// to confirm counter-counter equality after a happy-path run. Also
    /// useful for ad-hoc operator probes when the metrics endpoint is
    /// not wired (returns the same values that
    /// `MetricsComponent::publish` exposes via Prometheus).
    pub fn cdn_tee_counters_snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.cdn_tee_cache_attempts_total.load(Ordering::Relaxed),
            self.cdn_tee_cache_completed_total.load(Ordering::Relaxed),
            self.cdn_tee_cache_abandoned_full_total.load(Ordering::Relaxed),
            self.cdn_tee_cache_abandoned_consumer_eof_total
                .load(Ordering::Relaxed),
        )
    }

    /// Test/observability: snapshot the #130 SingleflightMap counters
    /// `(total_dedup_hits, total_bypasses_cap)`. Useful for asserting
    /// dedup engaged in concurrent-fetch tests.
    #[must_use]
    pub fn singleflight_counters_snapshot(&self) -> (u64, u64) {
        (
            self.singleflight.total_dedup_hits(),
            self.singleflight.total_bypasses_cap(),
        )
    }

    /// Test/observability: cumulative count of waiters that fell back
    /// to direct peer-fetch after the leader signaled Ok but the CAS
    /// was empty (the WPS race-window degradation signal). Distinct
    /// from `total_dedup_hits` (which counts JOINS); this counts the
    /// subset of joins that DEGRADED to direct fetch. See
    /// `SingleflightMap::total_waiter_fallback_to_direct`.
    #[must_use]
    pub fn singleflight_waiter_fallback_to_direct_total(&self) -> u64 {
        self.singleflight.total_waiter_fallback_to_direct()
    }

    /// Add a worker endpoint to the connection pool.
    pub async fn add_worker_endpoint(&self, endpoint: &str) {
        if self.get_worker_connection(endpoint).is_some() {
            return;
        }
        self.get_or_create_connection(endpoint).await;
    }

    /// Returns the inner (server) store.
    pub fn inner_store(&self) -> &Store {
        &self.inner
    }

    /// Returns the locality map for looking up which peers have which digests.
    pub fn locality_map(&self) -> &SharedBlobLocalityMap {
        &self.locality_map
    }

    /// Test-only: snapshot the `mirror_total_attempted` counter.
    /// Used by `#168` integration tests to assert that the dispatcher's
    /// `schedule_dispatch_to_all_workers` correctly suppresses the
    /// duplicate `mirror_blob_to_random_worker` call (item F).
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn mirror_total_attempted_for_test(&self) -> u64 {
        self.mirror_total_attempted.load(Ordering::Relaxed)
    }

    /// Returns all currently-connected peer stores.
    pub fn peer_stores(&self) -> HashMap<Arc<str>, Store> {
        self.worker_connections.read().clone()
    }

    /// Remove a worker endpoint from the connection pool.
    pub fn remove_worker_endpoint(&self, endpoint: &str) {
        let mut conns = self.worker_connections.write();
        if conns.remove(endpoint).is_some() {
            info!(endpoint, "WorkerProxyStore: removed worker connection");
        }
    }

    /// Inject a pre-built Store as a worker connection for the given endpoint.
    /// This is primarily useful for testing, where you want to use a MemoryStore
    /// instead of a real GrpcStore.
    pub fn inject_worker_connection(&self, endpoint: &str, store: Store) {
        self.worker_connections
            .write()
            .insert(Arc::from(endpoint), store);
    }

    /// Get a cached connection to a worker endpoint, or None.
    fn get_worker_connection(&self, endpoint: &str) -> Option<Store> {
        self.worker_connections.read().get(endpoint).cloned()
    }

    /// Get or create a connection to a worker endpoint.
    /// Returns None if the connection could not be created.
    pub async fn get_or_create_connection(&self, endpoint: &str) -> Option<Store> {
        if let Some(store) = self.get_worker_connection(endpoint) {
            return Some(store);
        }
        match self.create_worker_connection(endpoint).await {
            Ok(store) => {
                self.worker_connections
                    .write()
                    .entry(Arc::from(endpoint))
                    .or_insert_with(|| store.clone());
                Some(store)
            }
            Err(e) => {
                trace!(endpoint, ?e, "WorkerProxyStore: failed to connect to peer");
                None
            }
        }
    }

    /// Create a minimal GrpcStore connection to a worker endpoint.
    async fn create_worker_connection(&self, endpoint: &str) -> Result<Store, Error> {
        let spec = GrpcSpec {
            instance_name: String::new(),
            endpoints: vec![GrpcEndpoint {
                address: endpoint.to_string(),
                tls_config: self.worker_tls_config.clone(),
                concurrency_limit: None,
                connect_timeout_s: 5,
                tcp_keepalive_s: 30,
                // Keepalive timeout is wide enough to survive a tokio runtime
                // stall + post-stall scheduling backlog. Anything tighter
                // causes mass mirror failure during write-burst stalls (we
                // saw 67 KeepAliveTimedOut in a single minute aligned with a
                // 4.9s stall + queue drain that exceeded 20s).
                http2_keepalive_interval_s: 30,
                http2_keepalive_timeout_s: 60,
                tcp_nodelay: true,
                // Use TCP (h2) for worker connections. QUIC was previously
                // used but dominated server CPU (~50%).
                use_http3: false,
            }],
            store_type: StoreType::Cas,
            retry: Retry::default(),
            max_concurrent_requests: 0,
            connections_per_endpoint: 64,
            // 15s, not the default 120s. The bytestream fast path wraps
            // worker.has() in a 50ms `tokio::time::timeout`; if the
            // outer timeout fires, dropping the future signals tonic to
            // RST_STREAM but the H2 stream slot stays accounted until the
            // peer ACKs. A tighter rpc_timeout caps the worst case so
            // zombie streams can't pile up against a wedged worker.
            rpc_timeout_s: 15,
            batch_update_threshold_bytes: 1_048_576, // 1MB: small blobs use BatchUpdateBlobs
            max_concurrent_batch_rpcs: 32,
            parallel_chunk_read_threshold: 8 * 1024 * 1024,
            parallel_chunk_count: 8,
            dual_transport: false,
            zstd_compression: false,
            // 3s cap on `cm.connection()` for mirror writes to a worker.
            // Without this, a dead worker queues writes against the 256-slot
            // connection backlog while reconnect attempts run on 1s backoff,
            // pinning per-worker mirror permits and 3 MiB Bytes per chunk.
            // 3s is wide enough to cover the post-stall reconnect tail
            // observed during write bursts; tighter would false-positive
            // healthy-but-busy workers (cf. perf review on Proposal 3).
            connection_acquire_timeout_ms: Some(3000),
            // Server→worker mirror connections never use the worker-side
            // chunked-write path; the chunked-write path is from worker
            // to server, not the reverse direction. Leave OFF.
            chunked_writes_enabled: false,
            chunked_v2_writes_enabled: false,
        };
        let store = GrpcStore::new(&spec)
            .await
            .err_tip(|| format!("Creating worker proxy connection to {endpoint}"))?;
        Ok(Store::new(store))
    }

    /// Lazily build (or retrieve) the per-`WorkerProxyStore`
    /// `BatchReadCoalescer`. The coalescer's `BatchFn` captures
    /// `Arc<Self>` so it can look up per-endpoint connections via
    /// `get_or_create_connection` + downcast to `GrpcStore` for
    /// `batch_read_blobs`. The capture uses `Weak<Self>` to avoid an
    /// `Arc` cycle.
    ///
    /// Caller MUST hold the parent `Arc<Self>` while submitting (which
    /// they do via the `Pin<&Self>` `StoreDriver` API + the surrounding
    /// `Store` `Arc`). If the `Weak` upgrades fail because the parent
    /// dropped, the coalescer's `BatchFn` returns
    /// `Code::Unavailable` per-digest — the caller's fallback path
    /// handles this gracefully.
    fn coalescer(self: &Arc<Self>) -> &Arc<BatchReadCoalescer> {
        self.batch_read_coalescer.get_or_init(|| {
            let weak: std::sync::Weak<Self> = Arc::downgrade(self);
            // BatchFn signature: (endpoint, digests, digest_function).
            // The drainer captures the FIRST PendingRead's
            // `DigestHasherFunc` from the OTel Context at submit-time
            // (B2 fix: Context::current() inside the spawned drainer
            // would otherwise be empty). The drainer also wraps this
            // call in `IS_WORKER_REQUEST.scope(captured, ...)` (B1
            // fix), so the inner `grpc.batch_read_blobs` reads the
            // correct flag for the `x-nativelink-worker` metadata.
            let batch_fn: BatchFn = Arc::new(
                move |endpoint: Arc<str>,
                      digests: Vec<DigestInfo>,
                      digest_function: DigestHasherFunc| {
                    let weak = weak.clone();
                    async move {
                        let mut out: HashMap<DigestInfo, Result<Bytes, Error>> =
                            HashMap::with_capacity(digests.len());
                        let Some(this) = weak.upgrade() else {
                            for d in digests {
                                out.insert(
                                    d,
                                    Err(make_err!(
                                        Code::Unavailable,
                                        "BatchReadCoalescer batch_fn: parent WorkerProxyStore was dropped"
                                    )),
                                );
                            }
                            return out;
                        };
                        let Some(store) = this.get_or_create_connection(&endpoint).await else {
                            for d in digests {
                                out.insert(
                                    d,
                                    Err(make_err!(
                                        Code::Unavailable,
                                        "BatchReadCoalescer batch_fn: no connection available for endpoint {endpoint}"
                                    )),
                                );
                            }
                            return out;
                        };
                        let Some(grpc) = store.downcast_ref::<GrpcStore>(None) else {
                            // Test injection: the connection is not a real
                            // GrpcStore (e.g. MemoryStore in unit tests).
                            // Fall back to per-blob `get_part_unchunked`
                            // against the injected store so tests can
                            // exercise the coalescer end-to-end without
                            // standing up a real gRPC server.
                            for d in digests {
                                let result = store.get_part_unchunked(d, 0, None).await;
                                out.insert(d, result);
                            }
                            return out;
                        };
                        // Production path: REAPI BatchReadBlobs. The
                        // request mirrors `running_actions_manager.rs`
                        // `execute_batch_read`, but `digest_function`
                        // comes from the captured (per-batch) hasher
                        // — NOT from `Context::current()` which is
                        // empty inside the spawned drainer.
                        let request = BatchReadBlobsRequest {
                            instance_name: String::new(),
                            digests: digests.iter().map(|d| (*d).into()).collect(),
                            acceptable_compressors: vec![],
                            digest_function: digest_function.proto_digest_func().into(),
                        };
                        let response = match grpc
                            .batch_read_blobs(Request::new(request))
                            .await
                        {
                            Ok(r) => r.into_inner(),
                            Err(e) => {
                                // RPC-level failure: fail every digest
                                // with the same upstream error so callers
                                // can fall back per-blob. Defensive cloning
                                // is fine here — failure path, not hot.
                                for d in digests {
                                    out.insert(d, Err(e.clone()));
                                }
                                return out;
                            }
                        };
                        // Index per-digest results by digest. RE API does
                        // NOT guarantee response ordering — match by digest.
                        for resp in response.responses {
                            let Some(proto_digest) = resp.digest else {
                                continue;
                            };
                            let Ok(digest) = DigestInfo::try_from(proto_digest) else {
                                continue;
                            };
                            let status_code = resp.status.as_ref().map_or(0, |s| s.code);
                            let result = if status_code == 0 {
                                // Length sanity: the worker's response
                                // payload MUST match the digest size — a
                                // shorter or longer payload is corruption
                                // (mirrors `validate_batch_read_responses`
                                // in `running_actions_manager.rs`).
                                let advertised = digest.size_bytes();
                                if resp.data.len() as u64 == advertised {
                                    Ok(resp.data)
                                } else {
                                    Err(make_err!(
                                        Code::DataLoss,
                                        "BatchReadCoalescer: peer returned {} bytes for \
                                         digest {digest} (advertised {advertised}); \
                                         dropping (corruption guard)",
                                        resp.data.len()
                                    ))
                                }
                            } else {
                                // NOTE: `Code::from_i32` returns
                                // `Code::Unknown` for unrecognized
                                // codes; both `Unknown` and `Internal`
                                // pass `should_try_peers` so the
                                // caller's fallback fires either way.
                                Err(make_err!(
                                    Code::from_i32(status_code),
                                    "BatchReadCoalescer: peer returned status code {status_code} \
                                     for digest {digest}: {}",
                                    resp.status
                                        .as_ref()
                                        .map(|s| s.message.as_str())
                                        .unwrap_or("(no message)")
                                ))
                            };
                            out.insert(digest, result);
                        }
                        out
                    }
                    .boxed()
                },
            );
            BatchReadCoalescer::new(batch_fn)
        })
    }

    /// Read a single small blob from `endpoint` via the batched
    /// coalescer, then forward the bytes to `writer` and tee to the
    /// inner store (matching the existing `get_part_and_cache` CDN-tee
    /// behavior).
    ///
    /// PRECONDITION: caller has already checked
    /// `BatchReadCoalescer::is_eligible(digest, offset, length)` and
    /// `batch_small_blob_reads_enabled()`. This method does not
    /// re-check those gates.
    ///
    /// On `Ok`: the bytes have been forwarded to `writer` (with
    /// `send` + `send_eof`) and submitted to the inner store. Caller
    /// MUST NOT touch the writer further.
    ///
    /// On `Err`: the writer has NOT been terminated (no `send`, no
    /// `send_error`, no `send_eof`). Caller is responsible for
    /// termination — typical pattern is to fall back to the per-blob
    /// `get_part_and_cache` path which then writes its own bytes /
    /// errors into the writer. This matches the existing
    /// `try_read_from_worker` per-peer-attempt loop, where each peer
    /// attempt either succeeds (writer fully written + terminated) or
    /// fails (writer untouched), and the loop's caller terminates the
    /// writer at the end.
    async fn try_batched_read_from_endpoint(
        self: Pin<&Self>,
        endpoint: &str,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
    ) -> Result<(), Error> {
        // Promote `&self` to `Arc<Self>` so the coalescer's BatchFn
        // can keep its Weak<Self> capture alive across submissions.
        // `Pin::into_inner_unchecked` would let us cast, but we don't
        // need the Pin to take an Arc — we need an Arc<Self> the same
        // way as_any_arc returns one. Alternative: use `arc_self_any`
        // (we have one through StoreDriver::as_any_arc). The simplest
        // is to obtain the Arc from `self.as_any_arc` chain. But the
        // method only takes `&Self`. Instead we make `coalescer` work
        // off a stored Arc<BatchReadCoalescer> that was eagerly
        // initialized at the first call — which means we need an
        // Arc<Self> *here* to bootstrap. We solve this by having
        // every call site that may invoke this method already hold
        // `Arc<Self>` via `WorkerProxyStore` construction; we re-
        // upgrade through `register_item_callback`'s `as_any_arc`
        // pattern is overkill. Simplest: stash the Arc<Self> at
        // construction.
        //
        // Actually the simplest: callers of try_batched_read_from_endpoint
        // wrap us with `Arc<Self>` via `Self::new`, so `as_any_arc()`
        // exists. We can pass the bootstrap Arc<Self> at coalescer-
        // init time once, via the parent's `Arc::clone` known at
        // wire-up time.
        //
        // For the impl below we use the `coalescer_handle()` helper
        // that takes `Arc<Self>` and lazy-inits.
        let coalescer = self
            .coalescer_handle()
            .err_tip(|| "try_batched_read_from_endpoint: coalescer not initialized")?;
        // M1 (code-reviewer): pass `&str` — the coalescer allocates
        // an `Arc<str>` only on the first submission per endpoint
        // (insert-miss path inside `submit`). Steady-state hits reuse
        // the cached key.
        let bytes = coalescer.submit(endpoint, digest).await?;
        // Forward bytes to caller's writer first, then tee to inner
        // store. On writer-send error, surface immediately — DO NOT
        // call `writer.send_error()` because this method's contract
        // is "writer untouched on Err" (mirrors per-peer-attempt
        // handling in try_read_from_worker).
        writer
            .send(bytes.clone())
            .await
            .err_tip(|| {
                format!(
                    "try_batched_read_from_endpoint: forwarding chunk for digest {digest}"
                )
            })?;
        writer.send_eof().err_tip(|| {
            format!("try_batched_read_from_endpoint: sending EOF for digest {digest}")
        })?;
        // Tee to inner cache (best-effort; matches get_part_and_cache).
        // `update_oneshot` constructs its own UploadSizeInfo internally
        // from the Bytes length — no need to pass one explicitly.
        let inner = self.inner.clone();
        let cache_key: StoreKey<'static> = digest.into();
        let bytes_for_cache = bytes;
        // Spawn so the caller's get_part returns as soon as their
        // writer is satisfied — the cache write is async-tee.
        tokio::spawn(async move {
            match inner
                .update_oneshot(cache_key, bytes_for_cache)
                .await
            {
                Ok(()) => {
                    info!(
                        %digest,
                        size_bytes = digest.size_bytes(),
                        "proxy_cache: cached batched-proxied blob in inner store"
                    );
                }
                Err(e) => {
                    warn!(
                        %digest,
                        size_bytes = digest.size_bytes(),
                        ?e,
                        "proxy_cache: failed to cache batched-proxied blob"
                    );
                }
            }
        });
        Ok(())
    }

    /// Bootstrap helper to obtain an `Arc<BatchReadCoalescer>` from
    /// `&self`. Reads from the `OnceLock`; if uninitialized, returns
    /// an error. Callers MUST initialize the coalescer via the public
    /// `coalescer_init_from_arc(self_arc)` once construction is done.
    /// This split is necessary because the coalescer's `BatchFn`
    /// captures `Weak<Self>`, which we cannot construct from `&Self`
    /// alone.
    fn coalescer_handle(&self) -> Result<Arc<BatchReadCoalescer>, Error> {
        self.batch_read_coalescer
            .get()
            .cloned()
            .ok_or_else(|| {
                make_err!(
                    Code::FailedPrecondition,
                    "WorkerProxyStore: BatchReadCoalescer not initialized — \
                     caller must invoke `init_batch_read_coalescer(self_arc)` \
                     after construction; falling back to per-blob path"
                )
            })
    }

    /// Initialize the `BatchReadCoalescer` with a `Weak<Self>` capture
    /// for the per-endpoint `BatchReadBlobs` lookup. Idempotent — a
    /// second call is a no-op. Callers MUST invoke this immediately
    /// after `Self::new` / `Self::new_with_tls` if they intend to ever
    /// call `enable_batch_small_blob_reads()`.
    pub fn init_batch_read_coalescer(self: &Arc<Self>) {
        let _ = self.coalescer();
    }

    /// Try to read a blob from a specific list of peer endpoints (e.g. from
    /// a redirect response). Same logic as `try_read_from_worker` but uses
    /// the caller-provided endpoints instead of consulting the locality map.
    async fn try_read_from_endpoints(
        &self,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
        endpoints: &[String],
    ) -> Result<bool, Error> {
        let digest = key.borrow().into_digest();
        debug!(
            ?digest,
            endpoint_count = endpoints.len(),
            "WorkerProxyStore: following redirect to peer endpoints"
        );

        // B3 (perf-optimizer): the batched fast path sends WHOLE-blob
        // bytes via `writer.send(...)` — if any prior iteration of this
        // loop already wrote partial bytes to the writer (e.g. endpoint
        // A's streaming `get_part_and_cache` succeeded mid-stream then
        // errored), firing the batched path against endpoint B would
        // produce `partial_A_bytes ++ full_B_bytes`, silent corruption.
        // Capture the writer's byte-written count BEFORE the loop and
        // gate the batched path on `writer.get_bytes_written() ==
        // bytes_before_proxy` per the streaming-path pattern in
        // `Self::try_read_from_worker`.
        let bytes_before_proxy = writer.get_bytes_written();

        for endpoint in endpoints {
            let Some(store) = self.get_or_create_connection(endpoint).await else {
                continue;
            };

            // #88: opportunistic batching of small-blob proxy reads.
            // When the flag is on AND eligibility passes (whole-blob
            // read, ≤ SMALL_BLOB_THRESHOLD), route this digest through
            // the per-endpoint coalescer. Concurrent same-target reads
            // collapse into one BatchReadBlobs RPC. On Err, fall
            // through to the existing get_part_and_cache path
            // (preserves the writer-termination contract — the
            // batched helper guarantees the writer is untouched on
            // Err).
            //
            // B3 guard: same `bytes_before_proxy` guard as the
            // `try_read_from_worker` streaming-path callsite — refuse
            // the batched whole-blob send if a previous endpoint
            // already wrote partial bytes to the writer.
            if self.batch_small_blob_reads.load(Ordering::Relaxed)
                && writer.get_bytes_written() == bytes_before_proxy
                && BatchReadCoalescer::is_eligible(digest, offset, length)
            {
                // SAFETY: `Pin::new(self)` is the same upgrade the
                // StoreDriver entry-point performs; this method is
                // called from `try_read_from_endpoints` which itself
                // is reachable only via `get_part`'s Pin<&Self>.
                let pinned: Pin<&Self> = Pin::new(self);
                match pinned
                    .try_batched_read_from_endpoint(endpoint, digest, &mut *writer)
                    .await
                {
                    Ok(()) => {
                        debug!(
                            ?digest,
                            endpoint = endpoint.as_str(),
                            "WorkerProxyStore: batched read from redirected peer succeeded"
                        );
                        return Ok(true);
                    }
                    Err(e) => {
                        // Surface the per-digest failure and fall
                        // through to the streaming path. Eviction
                        // policy mirrors the streaming path below.
                        let evict = should_evict_locality_on_peer_error(&e);
                        if evict {
                            self.locality_map
                                .write()
                                .evict_blobs(endpoint, &[digest]);
                        }
                        warn!(
                            ?digest,
                            endpoint = endpoint.as_str(),
                            code = ?e.code,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: batched redirected read failed; \
                             falling through to per-blob streaming path"
                        );
                        // Fall through to streaming get_part_and_cache.
                    }
                }
            }

            // Capture writer position immediately before this peer's
            // attempt so the post-Ok 0-byte guard observes ONLY this
            // peer's contribution (not accumulated bytes from earlier
            // endpoints in the loop).
            let bytes_before_attempt = writer.get_bytes_written();
            match self
                .get_part_and_cache(&store, key.borrow(), &mut *writer, offset, length)
                .await
            {
                Ok(()) => {
                    // #500 sibling: same defensive guard as the
                    // streaming-path 0-byte-Ok check in
                    // `Self::try_read_from_worker`. Prior to this guard,
                    // a redirected peer returning Ok+0-bytes for a
                    // non-zero range silently propagated as canonical
                    // EOF, regardless of `length=None` vs
                    // `length=Some(N)`. Note: `try_read_from_endpoints`
                    // does NOT carry resume state across endpoints
                    // (offset/length are constant per call), so the
                    // owed-bytes computation uses the caller's
                    // (offset, length) unmodified.
                    let bytes_written_in_attempt =
                        writer.get_bytes_written() - bytes_before_attempt;
                    let expected_size = digest.size_bytes();
                    let expected_bytes_this_attempt =
                        owed_bytes(expected_size, offset, length);
                    if bytes_written_in_attempt == 0 && expected_bytes_this_attempt > 0 {
                        warn!(
                            ?digest,
                            endpoint = endpoint.as_str(),
                            expected_size,
                            offset,
                            length = ?length,
                            expected_bytes_this_attempt,
                            "WorkerProxyStore: redirected peer returned \
                             Ok+0-bytes for non-zero range — treating as \
                             stale-positive, evicting locality and trying \
                             next endpoint (#500)"
                        );
                        self.locality_map
                            .write()
                            .evict_blobs(endpoint, &[digest]);
                        continue;
                    }
                    debug!(
                        ?digest,
                        endpoint = endpoint.as_str(),
                        bytes_written_in_attempt,
                        "WorkerProxyStore: successfully read blob from redirected peer"
                    );
                    return Ok(true);
                }
                Err(e) => {
                    // Same locality-eviction policy as `try_read_from_worker`:
                    // see `should_evict_locality_on_peer_error` — narrow to
                    // NotFound / DataLoss only, so a transient blip doesn't
                    // permanently destroy the locality entry.
                    let is_conn_err = is_connection_error(&e);
                    if is_conn_err {
                        self.remove_worker_endpoint(endpoint);
                    }
                    let evict = should_evict_locality_on_peer_error(&e);
                    if evict {
                        self.locality_map
                            .write()
                            .evict_blobs(endpoint, &[digest]);
                    }
                    // Gate the error-level log on writer-still-open: when
                    // the outer consumer has dropped the reader (or a
                    // prior eviction `continue` left the writer in a
                    // closed state), `get_part_and_cache` returns
                    // "Failed to write to data, receiver disconnected" /
                    // similar derivative errors. Those are NOT a peer
                    // fault and should not surface as fleet-level
                    // `error!` noise. N healthy peers × stale-positive
                    // READ produced N-1 spurious errors per request.
                    if writer.is_pipe_broken() {
                        debug!(
                            ?digest,
                            endpoint = endpoint.as_str(),
                            code = ?e.code,
                            connection_error = is_conn_err,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: redirected peer fetch \
                             failed after writer pipe broken (derivative)"
                        );
                    } else {
                        error!(
                            ?digest,
                            endpoint = endpoint.as_str(),
                            code = ?e.code,
                            connection_error = is_conn_err,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: redirected peer fetch failed"
                        );
                    }
                    warn!(
                        ?digest,
                        endpoint = endpoint.as_str(),
                        ?e,
                        "WorkerProxyStore: read from redirected peer failed, trying next"
                    );
                    continue;
                }
            }
        }

        Ok(false)
    }

    /// Try to read a blob from a worker that has it, according to the locality map.
    ///
    /// Streams from the peer to the caller's writer via `get_part_and_cache()`,
    /// which tees the data to both the caller and the inner store for caching
    /// (for full-blob reads within the size limit). If a peer fails mid-stream,
    /// we resume from the next peer at the byte offset where the previous one
    /// left off (content-addressed blobs are identical across peers).
    async fn try_read_from_worker(
        &self,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<bool, Error> {
        let digest = key.borrow().into_digest();
        info!(?digest, "try_read_from_worker: locality lookup entered");
        let workers = self.locality_map.read().lookup_workers(&digest);
        info!(
            ?digest,
            worker_count = workers.len(),
            "try_read_from_worker: locality lookup returned"
        );

        if workers.is_empty() {
            info!(
                ?digest,
                "try_read_from_worker: no peers in locality map (server-only fetch path)",
            );
            return Ok(false);
        }

        // Diagnostic: capture the caller's intent so we can correlate
        // weird per-attempt offsets in the loop below against what was
        // originally requested. The "0-byte success for non-zero blob"
        // warns we've been chasing show offsets >> digest.size_bytes(),
        // which can only originate from the resume-from-offset logic
        // accumulating bytes_written_total wrongly across peer attempts.
        let digest_size = digest.size_bytes();
        info!(
            ?digest,
            digest_size,
            caller_offset = offset,
            caller_length = ?length,
            worker_count = workers.len(),
            "WorkerProxyStore: attempting to proxy blob from workers"
        );

        // Track how many bytes have been written so we can resume from the
        // correct offset if a streaming peer fails mid-transfer.
        let bytes_before_proxy = writer.get_bytes_written();
        let mut current_offset = offset;
        let mut remaining_length = length;

        for endpoint in &workers {
            // Diagnostic: log the per-attempt offset and flag the
            // smoking-gun pattern (offset already past EOF before the
            // RPC even fires). When this warn triggers, the bug is
            // upstream of the peer — bytes_written_total accumulated
            // wrongly in a previous attempt.
            if current_offset >= digest_size && digest_size > 0 {
                warn!(
                    ?digest,
                    endpoint = %endpoint,
                    digest_size,
                    current_offset,
                    bytes_before_proxy,
                    bytes_written_so_far = writer.get_bytes_written() - bytes_before_proxy,
                    "WorkerProxyStore: about to issue peer read with offset >= digest_size \
                     — bytes_written_total accumulated wrongly in a prior attempt; \
                     peer will return Ok+EOF (the 0-byte-success warn we've been chasing)"
                );
            }
            info!(
                ?digest,
                endpoint = %endpoint,
                current_offset,
                remaining_length = ?remaining_length,
                "worker_proxy: peer attempt entered"
            );
            let Some(store) = self.get_or_create_connection(endpoint).await else {
                info!(?digest, endpoint = %endpoint, "worker_proxy: peer attempt skipped (no connection)");
                continue;
            };

            // #88: opportunistic batching of small-blob proxy reads.
            // Eligibility uses the ORIGINAL caller offset/length (not
            // current_offset / remaining_length) because the batched
            // path is only safe for whole-blob reads — the
            // resume-from-mid-stream logic above never engages once
            // we've taken the batched path. If a previous peer already
            // wrote partial bytes (`bytes_before_proxy` <
            // `writer.get_bytes_written()`), the batched path would
            // double-write — DON'T fire the batched path in that case.
            if self.batch_small_blob_reads.load(Ordering::Relaxed)
                && current_offset == offset
                && remaining_length == length
                && writer.get_bytes_written() == bytes_before_proxy
                && BatchReadCoalescer::is_eligible(digest, offset, length)
            {
                let pinned: Pin<&Self> = Pin::new(self);
                match pinned
                    .try_batched_read_from_endpoint(endpoint, digest, &mut *writer)
                    .await
                {
                    Ok(()) => {
                        info!(
                            ?digest,
                            endpoint = %endpoint,
                            "WorkerProxyStore: batched read from peer succeeded"
                        );
                        return Ok(true);
                    }
                    Err(e) => {
                        // Eviction policy mirrors the streaming path
                        // below (NotFound / DataLoss → evict). All
                        // other codes are transient — keep the
                        // locality entry. The batched helper guarantees
                        // the writer is untouched on Err, so falling
                        // through to get_part_and_cache is safe.
                        let evict = should_evict_locality_on_peer_error(&e);
                        if evict {
                            self.locality_map
                                .write()
                                .evict_blobs(endpoint, &[digest]);
                        }
                        warn!(
                            ?digest,
                            endpoint = %endpoint,
                            code = ?e.code,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: batched peer read failed; \
                             falling through to per-blob streaming path"
                        );
                        // Fall through to streaming get_part_and_cache.
                    }
                }
            }

            // Stream from the peer, caching in the inner store when possible.
            // On failure, compute how many bytes were written and resume
            // from the next peer at the correct offset.
            //
            // Capture writer position immediately before THIS peer's
            // attempt so the post-Ok 0-byte guard observes ONLY this
            // peer's contribution (not accumulated bytes from earlier
            // endpoints in the loop). Pre-fix the guard subtracted from
            // `bytes_before_proxy` (pre-loop), which produced a stale
            // non-zero delta when an earlier peer had partial-Err'd and
            // the resume math advanced past its bytes — masking a
            // subsequent peer's silent Ok+0-bytes for the remaining
            // chunked range.
            let bytes_before_attempt = writer.get_bytes_written();
            let attempt_res = self
                .get_part_and_cache(&store, key.borrow(), &mut *writer, current_offset, remaining_length)
                .await;
            info!(
                ?digest,
                endpoint = %endpoint,
                ok = attempt_res.is_ok(),
                "worker_proxy: peer attempt complete"
            );
            match attempt_res {
                Ok(()) => {
                    // Defensive guard: a peer can finish a Read RPC with
                    // Ok+EOF and zero bytes for a non-zero digest (see the
                    // bug class previously caught by grpc_store.rs:1453,
                    // now removed in favor of source-side fixes in
                    // fast_slow_store.rs::insert_mirror_blob + get_part
                    // size guards). If THIS peer is on an old build that
                    // still has the bug — or some other path produces an
                    // empty stream — accepting Ok+0-bytes here would
                    // pollute the consumer with a silent empty response
                    // and leave the locality_map pointing at the broken
                    // peer. Treat 0-bytes-on-any-non-empty-range as a
                    // peer failure: evict locality and try the next peer.
                    //
                    // #500: the pre-fix `was_full_read` predicate
                    // included `length.is_none()`, so Bazel's parallel-
                    // chunk reads (which carry `length=Some(chunk_size)`)
                    // bypassed the guard entirely and propagated Ok+0
                    // bytes silently. Reformulated: compute the bytes
                    // the peer would have OWED for this chunked range
                    // (clamped to blob size), and trigger on zero-vs-
                    // non-zero owed.
                    let bytes_written_in_attempt =
                        writer.get_bytes_written() - bytes_before_attempt;
                    let expected_size = digest.size_bytes();
                    let expected_bytes_this_attempt =
                        owed_bytes(expected_size, current_offset, remaining_length);
                    if bytes_written_in_attempt == 0 && expected_bytes_this_attempt > 0 {
                        warn!(
                            ?digest,
                            endpoint = %endpoint,
                            expected_size,
                            current_offset,
                            remaining_length = ?remaining_length,
                            expected_bytes_this_attempt,
                            "WorkerProxyStore: peer returned Ok+0-bytes for \
                             non-zero range — treating as stale-positive, \
                             evicting locality and trying next peer (#500)"
                        );
                        self.locality_map
                            .write()
                            .evict_blobs(endpoint, &[digest]);
                        continue;
                    }
                    info!(
                        ?digest,
                        endpoint = %endpoint,
                        bytes_written_in_attempt,
                        "WorkerProxyStore: successfully proxied blob from worker"
                    );
                    return Ok(true);
                }
                Err(e) => {
                    // Locality-eviction policy: see
                    // `should_evict_locality_on_peer_error` doc — narrow to
                    // NotFound / DataLoss only.
                    let is_conn_err = is_connection_error(&e);
                    if is_conn_err {
                        self.remove_worker_endpoint(endpoint);
                    }
                    let evict = should_evict_locality_on_peer_error(&e);
                    if evict {
                        self.locality_map
                            .write()
                            .evict_blobs(endpoint, &[digest]);
                    }
                    // Gate the error-level log on writer-still-open: when
                    // the outer consumer has dropped the reader (or a
                    // prior eviction `continue` left the writer in a
                    // closed state), `get_part_and_cache` returns
                    // "Failed to write to data, receiver disconnected" /
                    // similar derivative errors. Those are NOT a peer
                    // fault and should not surface as fleet-level
                    // `error!` noise. N healthy peers × stale-positive
                    // READ produced N-1 spurious errors per request.
                    if writer.is_pipe_broken() {
                        debug!(
                            ?digest,
                            endpoint = %endpoint,
                            code = ?e.code,
                            connection_error = is_conn_err,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: peer fetch failed after writer \
                             pipe broken (derivative)"
                        );
                    } else {
                        self.worker_proxy_peer_fetch_notfound_total.inc();
                        error!(
                            ?digest,
                            endpoint = %endpoint,
                            code = ?e.code,
                            connection_error = is_conn_err,
                            evicted_locality = evict,
                            ?e,
                            "WorkerProxyStore: peer fetch failed"
                        );
                    }
                    let bytes_written_total =
                        writer.get_bytes_written() - bytes_before_proxy;
                    let next_offset = offset + bytes_written_total;
                    // Diagnostic: if the resume math produces an offset that
                    // exceeds digest_size, the bytes_written_total has gone
                    // wrong (peer responded with bytes from a DIFFERENT blob,
                    // OR the writer inherited bytes from a prior call). This
                    // is the upstream cause of the "0-byte success for
                    // non-zero blob" warns we've been chasing — the next
                    // peer attempt issues a Read at offset > size, gets EOF.
                    if digest_size > 0 && next_offset > digest_size {
                        warn!(
                            ?digest,
                            endpoint = %endpoint,
                            digest_size,
                            caller_offset = offset,
                            bytes_written_total,
                            next_offset,
                            overshoot = next_offset - digest_size,
                            "WorkerProxyStore: resume bump produced offset > digest_size — \
                             bytes_written_total is wrong (writer accumulated unrelated bytes); \
                             next peer will be asked for offset past EOF"
                        );
                    }
                    warn!(
                        ?digest,
                        endpoint = %endpoint,
                        bytes_written_total,
                        ?e,
                        "WorkerProxyStore: streaming get_part from peer failed, \
                         will resume from next peer at offset {}",
                        next_offset,
                    );
                    // Advance offset so the next peer picks up where this one left off.
                    current_offset = next_offset;
                    if let Some(len) = remaining_length {
                        remaining_length =
                            Some(len.saturating_sub(bytes_written_total));
                    }
                    continue;
                }
            }
        }

        Ok(false)
    }

    /// Maximum blob size to buffer and cache in the inner store after a
    /// successful proxy read. Blobs larger than this are streamed directly
    /// without caching, to avoid excessive memory usage.
    /// Public so integration tests (`tests/worker_proxy_singleflight_test`)
    /// can compute oversized-blob digests against the same constant
    /// the production code gates on. Pinning a hard-coded literal in
    /// tests would silently desync if this size ever changed.
    pub const MAX_CACHE_BLOB_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB

    /// Wrapper around a peer's `get_part` that tees the data to both the
    /// caller's writer and a background write to the inner store, with
    /// #130 singleflight dedup of concurrent same-digest peer-fetches
    /// layered on top.
    ///
    /// For full-blob reads (`offset == 0 && length.is_none()`) of blobs
    /// within `MAX_CACHE_BLOB_SIZE`, the bytes are forwarded chunk-by-chunk
    /// to the caller AND fanned out to a spawned cache task that writes
    /// the blob to `self.inner`. For partial reads or oversized blobs,
    /// streams directly without caching AND without singleflight.
    ///
    /// # #130 SingleflightMap wire-up (Option C, 2026-05-07)
    ///
    /// When SF dedup is eligible (full-blob read ≤ MAX_CACHE_BLOB_SIZE,
    /// digest size > 0), the peer-fetch routes through
    /// `self.singleflight.singleflight(...)`. Three roles emerge:
    ///
    /// * **Leader / Bypass** (the role's `fetcher` closure runs): does
    ///   the existing #230 detached-cache flow via
    ///   `get_part_and_cache_inner`, IMMEDIATELY detaches the cache
    ///   `JoinHandle` (preserving the #230 contract), and signals SF
    ///   based on the FORWARD outcome alone (Ok if bytes reached
    ///   Bazel, Err if the peer fetch failed). The leader's CALLER
    ///   receives the same forward outcome via a side-channel
    ///   `Arc<parking_lot::Mutex<Option<...>>>` — kept identical to
    ///   the SF signal here for symmetry, but routed via the side-
    ///   channel because the SF API ties leader return = waiter
    ///   broadcast.
    /// * **Waiter** (the closure does NOT run): waits for the leader's
    ///   SF signal. On `Ok`: tries `self.inner.get_part(...)` — the
    ///   leader's spawned cache task is detached and may or may not
    ///   have completed by now. On hit: cheap CAS read (no peer
    ///   round-trip). On `NotFound` with zero bytes written: falls
    ///   back to a direct (non-SF) `get_part_and_cache_inner` call.
    ///   This is the race-window safety net — when the leader's cache
    ///   hasn't landed yet (or was abandoned via `try_send` Full),
    ///   the waiter pays peer-fetch cost. This degrades gracefully to
    ///   the pre-#130 behavior (each waiter does its own peer-fetch);
    ///   no waiter is ever wedged.
    /// * **Bypass** (cap exceeded): runs the closure too — same code
    ///   path as Leader. Cache is detached either way.
    ///
    /// This is the "streaming-required" wire-up from
    /// `project_sf_wireup_design_streaming_required` — there is NEVER
    /// any `Vec<Bytes>` buffering of payload at the SF layer. The SF
    /// payload is always `Vec::new()` (empty) and acts purely as a
    /// barrier; bytes flow chunk-by-chunk through the leader's writer
    /// (forward path) and the leader's cache task → CAS → waiter's
    /// `inner.get_part` (waiter path).
    ///
    /// ## Choice (α): leader detaches cache and signals on forward only
    ///
    /// The Leader/Bypass closure DETACHES `cache_handle` (drops it
    /// without awaiting) and signals SF based on `forward_result`
    /// alone. Rationale: awaiting `cache_handle` would couple the
    /// leader's outer return latency to the cache task's completion
    /// time. With the existing slow-cache test
    /// (`cdn_tee_slow_cache_abandons_does_not_block_bazel` —
    /// 5 s/update inner store, 4 s assertion), an `await` would
    /// regress the #229 contract that "Bazel rate is decoupled from
    /// slow cache" — the leader's `proxy.get_part_unchunked` (which
    /// `join!`s `rx.consume` AND `get_part`) would wait for `get_part`
    /// to return, which would wait for cache.
    ///
    /// The cost of choice (α): a race window between leader-signal-Ok
    /// and cache-task-complete. A waiter that wakes during this window
    /// finds CAS empty and falls back to direct peer-fetch. In the
    /// pathological "5 s slow cache" case the entire waiter cohort
    /// falls back — equivalent to NO singleflight, which is the
    /// pre-#130 baseline. In the COMMON case (cache write rate >>
    /// peer fetch rate, e.g. 100s of MB/s SSD vs 10s of MB/s peer
    /// gRPC), cache completes WHILE the leader's forward is still
    /// streaming, so by the time the waiter reaches `inner.get_part`
    /// the CAS already has the blob — full SF dedup engages.
    ///
    /// (β) — leader awaits cache before signaling — was prototyped
    /// and rejected: it red-tripped the slow-cache decoupling test
    /// and introduced architectural sign-off concerns under CLAUDE.md
    /// "async↔sync coupling" trip-wire.
    ///
    /// # Architecture (#230 — user-approved 2026-05-02)
    ///
    /// The forward path is the SOURCE OF TRUTH. The cache fan-out is a
    /// best-effort tee that never propagates back-pressure into the
    /// forward path. Concretely, the per-chunk loop is:
    ///
    /// 1. `bazel_writer.send(chunk).await` — awaits ONLY Bazel-side
    ///    back-pressure. The caller's writer is the only thing that can
    ///    legitimately make the peer-reader wait.
    /// 2. `cache_tx.try_send(chunk)` — non-blocking. If the cache mpsc
    ///    is full, take and drop `cache_tx` (the `Option::take()`
    ///    move-out leaves None), and continue forwarding-only. The
    ///    next reader that needs this digest triggers another
    ///    peer-fetch — user-explicit: "if you have to re-read chunks
    ///    from the worker again, so be it."
    ///
    /// The cache task is spawned BEFORE the forward loop and runs
    /// `inner.update(digest, cache_rx, ExactSize(size))` to completion
    /// (or `CDN_TEE_CACHE_TASK_TIMEOUT`, whichever comes first). On
    /// abandon, the cache task sees an EOF before all bytes arrive,
    /// `inner.update`'s `ExactSize` check trips, and the temp file is
    /// unlinked via `EncodedFilePath::Drop`'s background spawn when
    /// `update_file` fails before reaching `emplace_file` (see
    /// `filesystem_store.rs:145-191`).
    ///
    /// # Why this is NOT `tokio::join!(forward, cache)` per chunk
    ///
    /// A per-chunk join would couple Bazel back-pressure to cache
    /// back-pressure: the chunk loop would not advance until BOTH halves
    /// drained. The pre-#230 implementation had this shape (a 3-way
    /// `tokio::join!` over the entire stream) and silently lost ~88 % of
    /// peer-fetched blobs because the bytestream consumer's `unfold`
    /// returned `None` on EOF, which dropped the `cache_write_fut`
    /// mid-`inner.update`. Per-#229 audit at
    /// `.claude/audits/229-peer-fetch-outcome-coverage.md`.
    ///
    /// # Asymmetric contract coverage (per CLAUDE.md §Tests)
    ///
    /// State-mutating side effects on the borrowed `&mut writer` here:
    /// - Under-action: forward-loop fails to send EOF on Bazel-EOF path.
    ///   Caller's `tokio::join!` over a paired rx would deadlock.
    /// - Over-action: forward-loop fires `send_error` on the writer in a
    ///   path where the peer succeeded — wrapping callers (e.g.
    ///   try_read_from_worker's resume logic) would mistake a cache
    ///   abandon for a transport failure and trigger a peer cycle.
    /// Both directions are covered by tests in
    /// `nativelink-store/tests/worker_proxy_store_test.rs` (`cdn_*`).
    async fn get_part_and_cache(
        &self,
        peer_store: &Store,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let digest = key.borrow().into_digest();

        // Only cache full-blob reads for blobs within the size limit.
        let should_cache = offset == 0
            && length.is_none()
            && digest.size_bytes() <= Self::MAX_CACHE_BLOB_SIZE;

        // SF dedup gate: same predicate as `should_cache`, plus reject
        // empty digests (they're handled trivially elsewhere and dedup
        // adds zero value). Per the design doc Option B: full-blob
        // reads only.
        let should_use_sf = should_cache && digest.size_bytes() > 0;

        if !should_cache {
            // Loop-terminator propagation: set IS_WORKER_REQUEST=true on
            // peer→peer calls so the receiving worker enters responder
            // mode (race_peers + IS_WORKER_REQUEST gate at top of
            // `get_part`) and refuses to chain externally. This makes
            // the user's invariant "workers NEVER try to satisfy a read
            // from another host by issuing an external RPC" enforced
            // at depth 1, not just bounded at depth 2.
            return IS_WORKER_REQUEST
                .scope(true, peer_store.get_part(key, &mut *writer, offset, length))
                .await;
        }

        if !should_use_sf {
            // Eligible for cache but not SF (e.g., zero-length blob).
            // Run the inner detached-cache path directly without SF
            // coordination.
            let (forward_result, cache_handle) = self
                .get_part_and_cache_inner(peer_store, key, writer, offset, length)
                .await;
            // Detach (existing behavior — no bytes-in-flight latency cost).
            drop(cache_handle);
            return forward_result;
        }

        // ====================================================================
        // SF wire-up — see fn-level docs for design rationale.
        // ====================================================================
        //
        // Side-channel: the leader's closure populates this with the
        // forward result so the leader's CALLER (this stack frame) can
        // return the real outcome instead of the SF signal (which
        // encodes cache outcome, not forward outcome).
        let leader_forward_outcome: Arc<parking_lot::Mutex<Option<Result<(), Error>>>> =
            Arc::new(parking_lot::Mutex::new(None));
        let was_leader_or_bypass: Arc<core::sync::atomic::AtomicBool> =
            Arc::new(core::sync::atomic::AtomicBool::new(false));

        let outcome_for_closure = leader_forward_outcome.clone();
        let was_leader_clone = was_leader_or_bypass.clone();

        // The SF closure borrows `&mut writer` and `self`, and copies
        // `peer_store` (`&Store` is Copy — it wraps Arc<dyn StoreDriver
        // + Send + Sync>, and `&T` of any T is Copy).
        // Send: `&mut DropCloserWriteHalf` is Send (Bytes/sender are Send),
        //       `&Self` is Send (WorkerProxyStore is Sync), `&Store` is
        //       Copy and Send.
        // Lifetime: the closure / future is bounded by the
        // `singleflight().await` call's lifetime — the SF API does NOT
        // 'static-bound the fetcher (see `singleflight.rs:236-244`), it
        // only requires Send. We await singleflight() inline below, so
        // the borrows are valid.
        //
        // We re-borrow `writer` and clone `key` for the closure so the
        // original bindings remain available on the waiter path after
        // SF returns (FnOnce consumes the captures unconditionally,
        // even on the waiter path where the closure body never runs).
        let writer_for_closure: &mut DropCloserWriteHalf = &mut *writer;
        let key_for_closure: StoreKey<'_> = key.borrow();
        let sf_signal = self
            .singleflight
            .singleflight(
                key.borrow().into_owned(),
                digest.size_bytes(),
                move || async move {
                    // Release: pairs with the Acquire load below to
                    // publish "we ran the closure body" before the
                    // outer `singleflight().await` completes. SeqCst
                    // was overspec'd — there's no cross-flag total
                    // order requirement; only happens-before between
                    // this store and the post-await load.
                    was_leader_clone.store(true, Ordering::Release);
                    let (forward_result, cache_handle) = self
                        .get_part_and_cache_inner(
                            peer_store,
                            key_for_closure,
                            writer_for_closure,
                            offset,
                            length,
                        )
                        .await;

                    // CRITICAL (#229 invariant — see fn-level docs choice
                    // (α) section): DETACH `cache_handle` here. Do NOT
                    // await — that would couple the leader's outer return
                    // latency to the cache task and red-trip
                    // `cdn_tee_slow_cache_abandons_does_not_block_bazel`.
                    drop(cache_handle);

                    // Stash forward result for the leader's caller path.
                    // The SF signal IS the forward result here (no cache
                    // outcome involved per choice (α)), but we route via
                    // side-channel so the caller path is structurally
                    // identical to a hypothetical (β) future redesign
                    // and so the SF signal type remains decoupled from
                    // the leader's caller-facing return type.
                    let forward_for_caller = match &forward_result {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e.clone()),
                    };
                    *outcome_for_closure.lock() = Some(forward_for_caller);

                    // SF signal:
                    // - forward Err → broadcast peer error to waiters
                    //   (they'd hit the same peer; surface it cleanly).
                    // - forward Ok → broadcast Ok(empty); waiters try
                    //   `inner.get_part` (CAS may or may not have it
                    //   yet — race window handled by waiter path).
                    match forward_result {
                        Err(e) => Err(e),
                        Ok(()) => Ok(Vec::new()),
                    }
                },
            )
            .await;

        if was_leader_or_bypass.load(Ordering::Acquire) {
            // Leader/Bypass path: closure ran; writer was populated by
            // the inner forward loop. Return the actual forward result
            // (NOT the SF signal — that encodes cache outcome).
            return leader_forward_outcome
                .lock()
                .take()
                .unwrap_or_else(|| {
                    Err(make_err!(
                        Code::Internal,
                        "singleflight: leader closure ran but did not populate \
                         forward outcome side-channel — bug"
                    ))
                });
        }

        // Waiter path: SF signal Ok ⇒ leader's forward succeeded.
        // CAS may or may not contain the blob yet (race window — see
        // choice (α) docs above). Try CAS; fall back to direct fetch
        // if missed.
        match sf_signal {
            Ok(_payload) => {
                // Capture writer's byte position so we can detect any
                // partial-write before falling back (avoid stream
                // corruption per #284 part 2 pattern).
                let bytes_before = writer.get_bytes_written();
                let inner_result = self
                    .inner
                    .get_part(key.borrow(), &mut *writer, offset, length)
                    .await;
                match inner_result {
                    Ok(()) => Ok(()),
                    Err(e)
                        if e.code == Code::NotFound
                            && writer.get_bytes_written() == bytes_before
                            && !writer.is_pipe_broken() =>
                    {
                        // CAS race: leader's cache hasn't landed yet
                        // (or was abandoned via try_send Full /
                        // consumer EOF). Safe to fall back to direct
                        // fetch — no bytes were written AND the writer
                        // is still usable (not closed via send_error
                        // by some upstream layer). The waiter pays
                        // peer-fetch cost; SF dedup-amplification
                        // protection is lost for this caller in this
                        // race window, but no waiter is wedged.
                        //
                        // The `!writer.is_pipe_broken()` guard is a
                        // DEFENSIVE check against a hypothetical future
                        // wrapper layer above WPS that closes the writer
                        // on Err. Today, no such caller exists: WPS is
                        // the outermost CAS wrapper, and no layer between
                        // WPS and the leaf calls `send_error` on NotFound
                        // (verified at verify_store.rs ~:208-276 +
                        // memory_store.rs ~:503), so the byte-counter
                        // alone catches the live race. If a future
                        // `WriteHalfGuard`-style defensive wrapper lands
                        // above WPS and starts closing the writer on
                        // Err, this guard prevents `get_part_and_cache_inner`
                        // from re-entering and tripping "Tried to send
                        // while stream is closed" on the first chunk.
                        // NOT a sibling of #171 (#171 was a real bug; this
                        // is forward-compat hardening only).
                        self.singleflight.record_waiter_fallback();
                        debug!(
                            %digest,
                            "singleflight: waiter NotFound in CAS despite leader \
                             signal-Ok — cache write task hasn't completed \
                             yet (or was abandoned); falling back to direct \
                             peer fetch"
                        );
                        let (forward_result, cache_handle) = self
                            .get_part_and_cache_inner(
                                peer_store, key, writer, offset, length,
                            )
                            .await;
                        drop(cache_handle); // detach — no SF cohort here
                        forward_result
                    }
                    Err(e) => {
                        // Either non-NotFound error, OR partial bytes
                        // written (stream now corrupt), OR the writer
                        // is already closed (pipe broken — fall-back
                        // would just trip "Tried to send while stream
                        // is closed" on its first chunk). Surface as-is.
                        Err(e).err_tip(|| {
                            "singleflight waiter: inner.get_part error after \
                             leader-signal-Ok"
                        })
                    }
                }
            }
            Err(e) => {
                // Leader's peer fetch failed — propagate to caller. The
                // leader's caller already saw this error too; the
                // waiter's upstream caller (try_read_from_worker) may
                // try the next peer.
                Err(e).err_tip(|| {
                    "singleflight waiter: leader peer-fetch failed; surface \
                     error to upstream peer-fallback loop"
                })
            }
        }
    }

    /// Existing #230 detached-cache flow, refactored to:
    /// * Return the spawned cache task's `JoinHandle<Result<(), Error>>`
    ///   so the caller can choose to await it (Leader path under #130
    ///   singleflight) or detach it (Bypass / pre-#130 behavior).
    /// * Keep all the per-chunk forward-loop semantics from #230 intact.
    ///
    /// The cache task now returns `Result<(), Error>` (was `()`) — the
    /// outcome flows into the SF signal computation in
    /// `get_part_and_cache`.
    async fn get_part_and_cache_inner(
        &self,
        peer_store: &Store,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> (Result<(), Error>, JoinHandle<Result<(), Error>>) {
        let digest = key.borrow().into_digest();

        // Intermediate buf_channel that the peer's `get_part` writes into;
        // we then fan the bytes out to (a) the caller's writer and
        // (b) the cache mpsc.
        let (mut proxy_tx, mut proxy_rx) = make_buf_channel_pair();

        // Cache mpsc: tight cap so a slow cache task triggers abandon
        // fast (#230). NOT shared with the caller's writer — these are
        // two independent flow-control regimes by design.
        //
        // `cache_tx` is wrapped in an Option so the abandon paths can
        // `take()` it (move-out + None in one step) without fighting
        // the borrow checker over the `loop` boundary. `is_some()`
        // means the cache fan-out is still live; once None, never
        // re-arm.
        let (cache_tx_init, cache_rx) =
            make_buf_channel_pair_with_size(CDN_TEE_CACHE_MPSC_CAP);
        let mut cache_tx: Option<DropCloserWriteHalf> = Some(cache_tx_init);

        // Spawn the peer-reader. The peer keeps writing into `proxy_tx`
        // bounded only by `proxy_tx`'s own mpsc capacity; the forward
        // loop below drives `proxy_rx`.
        let owned_key = key.borrow().into_owned();
        let peer = peer_store.clone();
        let peer_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            IS_WORKER_REQUEST
                .scope(
                    true,
                    peer.get_part(owned_key.borrow(), &mut proxy_tx, offset, length),
                )
                .await
        });

        // Spawn the cache-write task. It owns `cache_rx` for its full
        // lifetime; the forward loop owns `cache_tx` and either feeds it
        // (happy path) or drops it (abandon path). Either way the cache
        // task observes a finite stream and runs to completion or
        // abandons cleanly via the inner store's ExactSize check.
        //
        // Counter increment is BEFORE the spawn so the attempt count
        // covers every code path that intends to cache (including ones
        // where the cache task is immediately dropped because the very
        // first try_send returns Full).
        self.cdn_tee_cache_attempts_total.fetch_add(1, Ordering::Relaxed);
        let inner = self.inner.clone();
        let cache_size = UploadSizeInfo::ExactSize(digest.size_bytes());
        let cache_key: StoreKey<'static> = digest.into();
        let completed_counter = self.cdn_tee_cache_completed_total.clone();
        // Wrap inner.update in a task-level timeout so a wedged inner
        // store cannot leak the spawned task (and its in-flight tracker
        // entry) indefinitely.
        let cache_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            match tokio::time::timeout(
                CDN_TEE_CACHE_TASK_TIMEOUT,
                inner.update(cache_key, cache_rx, cache_size),
            )
            .await
            {
                Ok(Ok(())) => {
                    completed_counter.fetch_add(1, Ordering::Relaxed);
                    info!(
                        %digest,
                        size_bytes = digest.size_bytes(),
                        "proxy_cache: cached proxied blob in inner store"
                    );
                    Ok(())
                }
                Ok(Err(e)) => {
                    // FU-6: best-effort cache fan-out abandonments are
                    // benign — the Bazel read already succeeded and the
                    // blob is durable via the worker's own upload.  Demote
                    // to debug! so operators are not alarmed.  Genuine
                    // cache-write failures (disk error, OOM, connection
                    // drop) keep warn! so the signal is not lost.
                    if is_cache_fanout_abandonment(&e) {
                        debug!(
                            %digest,
                            size_bytes = digest.size_bytes(),
                            ?e,
                            "proxy_cache: cache fan-out abandoned (best-effort, \
                             not a genuine failure — FU-6)",
                        );
                    } else {
                        warn!(
                            %digest,
                            size_bytes = digest.size_bytes(),
                            ?e,
                            "proxy_cache: failed to cache proxied blob in inner store"
                        );
                    }
                    Err(e)
                }
                Err(_elapsed) => {
                    error!(
                        %digest,
                        size_bytes = digest.size_bytes(),
                        timeout_s = CDN_TEE_CACHE_TASK_TIMEOUT.as_secs(),
                        "proxy_cache: cache write task exceeded timeout — abandoning \
                         (temp file unlinked by EncodedFilePath::Drop background \
                         spawn since update_file did not reach emplace_file)"
                    );
                    Err(make_err!(
                        Code::DeadlineExceeded,
                        "cache write task exceeded {}s timeout",
                        CDN_TEE_CACHE_TASK_TIMEOUT.as_secs()
                    ))
                }
            }
        });

        // Per-chunk forward loop. `cache_tx` is `Some` while the
        // cache fan-out is active; `take()` on every abandon path
        // drops it in one move. Once None, never re-arm.
        let mut total_bytes: u64 = 0;
        let mut forward_result: Result<(), Error> = Ok(());
        loop {
            match proxy_rx.recv().await {
                Ok(chunk) if chunk.is_empty() => {
                    // Peer EOF. Forward EOF to Bazel; finalize cache.
                    if let Err(e) = writer
                        .send_eof()
                        .err_tip(|| "get_part_and_cache: forwarding EOF")
                    {
                        forward_result = Err(e);
                    }
                    if let Some(mut tx) = cache_tx.take() {
                        // Best-effort cache EOF. If it errors (cache task
                        // already gone — unusual but not fatal), log and
                        // proceed; the dropped tx still tidies up.
                        if let Err(e) = tx.send_eof() {
                            warn!(
                                %digest,
                                ?e,
                                "get_part_and_cache: cache_tx send_eof failed \
                                 (cache task may have errored already)"
                            );
                        }
                    }
                    break;
                }
                Ok(chunk) => {
                    total_bytes += chunk.len() as u64;
                    // (1) Forward to Bazel — awaits ONLY Bazel-side back-
                    // pressure. If this errors, the consumer is gone:
                    // abandon cache, propagate to the caller.
                    if let Err(e) = writer
                        .send(chunk.clone())
                        .await
                        .err_tip(|| "get_part_and_cache: forwarding chunk")
                    {
                        if let Some(ref mut tx) = cache_tx {
                            // Signal intentional abandonment before dropping
                            // so inner stores demote update-failure from
                            // error! to debug! (FU-6). Consumer disconnect
                            // is benign — Bazel is gone, blob will be
                            // re-fetched by the next reader.
                            tx.send_error(make_err!(
                                Code::Aborted,
                                "{CACHE_FANOUT_ABANDONED_MARKER}: \
                                 bazel consumer disconnected at {total_bytes}B"
                            ));
                        }
                        if cache_tx.take().is_some() {
                            self.cdn_tee_cache_abandoned_consumer_eof_total
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(
                                %digest,
                                bytes_sent = total_bytes,
                                "get_part_and_cache: bazel consumer disconnected \
                                 mid-blob — abandoning cache fan-out"
                            );
                            // take() above already dropped cache_tx; the
                            // cache task observes EOF before all bytes
                            // (ExactSize mismatch triggers
                            // FilesystemStore in-flight discard).
                        }
                        forward_result = Err(e);
                        break;
                    }
                    // (2) Best-effort cache fan-out — non-blocking. NEVER
                    // .await here; the user-explicit contract is "Bazel
                    // reader NEVER blocks on cache".
                    if let Some(tx) = cache_tx.as_mut() {
                        match tx.try_send(chunk) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                self.cdn_tee_cache_abandoned_full_total
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    %digest,
                                    bytes_sent = total_bytes,
                                    cap = CDN_TEE_CACHE_MPSC_CAP,
                                    "get_part_and_cache: cache mpsc full — \
                                     abandoning cache fan-out (Bazel reader \
                                     unaffected; next reader will re-fetch)"
                                );
                                // Signal the cache task that this is an
                                // intentional abandonment (mpsc full, not a
                                // producer crash). Inner stores
                                // (`fast_slow_store`, `existence_cache_store`)
                                // detect CACHE_FANOUT_ABANDONED_MARKER and
                                // demote their update-failure log from
                                // `error!` to `debug!`. Without this signal
                                // they see a generic "Sender dropped before
                                // sending EOF" (Code::Internal) and log
                                // error! — benign but operationally
                                // misleading (FU-6).
                                if let Some(ref mut tx) = cache_tx {
                                    tx.send_error(make_err!(
                                        Code::Aborted,
                                        "{CACHE_FANOUT_ABANDONED_MARKER}: \
                                         cache mpsc full at {}/{} slots",
                                        total_bytes,
                                        CDN_TEE_CACHE_MPSC_CAP
                                    ));
                                }
                                cache_tx.take();
                            }
                            Err(TrySendError::Closed(_)) => {
                                // Cache task already returned (errored or
                                // wedged-and-timed-out). Accounting: NOT
                                // a Bazel-disconnect; NOT a fill-up.
                                // Signal-class is "task exited unexpectedly"
                                // and is already covered by the cache
                                // task's own warn!/error! log paths.
                                warn!(
                                    %digest,
                                    bytes_sent = total_bytes,
                                    "get_part_and_cache: cache_rx closed before \
                                     forward EOF (cache task ended) — abandoning \
                                     cache fan-out"
                                );
                                cache_tx.take();
                            }
                        }
                    }
                }
                Err(e) => {
                    // Peer side errored. Drop cache_tx so the cache task
                    // sees a short stream and aborts via ExactSize. The
                    // outer caller observes the peer-driven error via
                    // peer_handle below.
                    cache_tx.take();
                    forward_result = Err(e)
                        .err_tip(|| "get_part_and_cache: reading from proxy channel");
                    break;
                }
            }
        }

        // Defensive: ensure cache_tx is dropped before we await the peer
        // task. Each branch above already takes() it on its own exit
        // path; this is a no-op when reached via EOF (already None).
        drop(cache_tx);

        // CRITICAL (#230 perf-optimizer M1 fix): drop `proxy_rx` BEFORE
        // awaiting `peer_handle`. The peer task writes into `proxy_tx`,
        // whose default capacity is `DEFAULT_BUF_CHANNEL_CAPACITY = 1024`
        // slots. On the consumer-disconnect path (bazel writer.send Err),
        // the forward loop `break`s but `proxy_rx` is still alive in the
        // function frame; the peer task fills proxy_tx and blocks
        // indefinitely on `proxy_tx.send().await` — `peer_handle.await`
        // below would then hang forever, pinning a worker on every
        // mid-blob Bazel disconnect of a >1024-chunk peer stream.
        //
        // Dropping proxy_rx here makes the next peer `send()` fail with
        // a closed-channel error; the peer task then exits and
        // peer_handle.await resolves to an Err that we treat as
        // expected on the abandon path.
        //
        // Belt-and-braces: also `peer_handle.abort()` whenever the
        // forward loop terminated with an error. abort + drop both
        // unblock the peer task; either alone suffices on this path,
        // but together they bound recovery time and avoid a window
        // where the peer task races between an in-flight syscall and
        // the dropped channel.
        if forward_result.is_err() {
            peer_handle.abort();
        }
        drop(proxy_rx);

        // Resolve the peer-reader task. Error preference: the producer
        // (peer) is the source of truth — surface its structured upstream
        // code (NotFound, DataLoss, Unavailable, …) before the forward
        // loop's derivative artifacts. Sibling pattern to commit 8674bc19
        // (populate path) and 01b68015 (spawn-detach producer path).
        //
        // Cancellation note: in tokio 1.x, `JoinHandle::drop` DETACHES
        // the spawned task — it does NOT abort or cancel it. So if the
        // outer caller is dropped while we're awaiting `peer_handle`
        // here, dropping `peer_handle` alone does not stop the peer
        // task. The actual wake mechanism that lets the peer task wind
        // down on outer-future drop is structural: when this future is
        // dropped, `proxy_rx` (owned by this frame) drops with it; the
        // peer task's next `proxy_tx.send().await` then returns Err and
        // the task observes that its consumer is gone and exits.
        //
        // The M1 explicit `peer_handle.abort()` + `drop(proxy_rx)` on
        // the abandon path (above, gated on `forward_result.is_err()`)
        // is belt-and-suspenders: either alone is sufficient on its
        // own — `drop(proxy_rx)` produces a closed-channel error on the
        // peer's next send, and `peer_handle.abort()` cancels the task
        // at the next await point. Together they bound recovery to one
        // scheduler tick instead of waiting for the next scheduled send.
        //
        // Cancellation can also originate externally — runtime shutdown,
        // a parent future being cancelled, etc. — not just from our own
        // abort path. In all such cases the cache task is detached and
        // proceeds independently (bounded by `CDN_TEE_CACHE_TASK_TIMEOUT`).
        let get_part_result = match peer_handle.await {
            Ok(res) => res,
            // Cancelled is the expected path when we asked for the abort
            // above (forward_result.is_err()); treat it as Ok so the
            // caller surfaces the forward-side error instead of a
            // derivative join error.
            Err(join_err) if join_err.is_cancelled() => Ok(()),
            Err(join_err) if join_err.is_panic() => Err(make_err!(
                Code::Internal,
                "get_part_and_cache: peer-reader task panicked: {join_err:?}"
            )),
            Err(join_err) => Err(make_err!(
                Code::Internal,
                "get_part_and_cache: peer-reader task join failed: {join_err:?}"
            )),
        };

        // Cache task handle is RETURNED to the caller. ALL callers
        // (Bypass / SF-disabled / SF Leader / SF Waiter fall-back)
        // currently `drop(cache_handle)` to detach the cache task —
        // see choice (α) in `get_part_and_cache`'s fn-level docs.
        // Awaiting `cache_handle` here would couple Bazel back-pressure
        // to slow-cache writes, regressing the
        // `cdn_tee_slow_cache_abandons_does_not_block_bazel` test
        // (rejected; see #229 audit + the choice-α design doc). A
        // future redesign that needs leader-await-cache (choice β)
        // would change ONLY the SF-Leader closure inside
        // `get_part_and_cache`; this inner function stays choice-α
        // by always returning the handle for the caller to drop.
        let final_forward_result = if let Err(get_err) = get_part_result {
            // Peer's get_part errored — surface that. forward result is
            // derivative.
            Err(get_err)
        } else if let Err(fwd_err) = forward_result {
            Err(fwd_err)
        } else {
            debug!(
                %digest,
                size_bytes = total_bytes,
                "get_part_and_cache: forward loop completed successfully"
            );
            Ok(())
        };

        (final_forward_result, cache_handle)
    }

    /// The original sequential get_part logic: try inner store, then parse
    /// redirects, then fall back to locality map / peer proxying.
    /// This is used as the fallback when no peers are known for racing.
    async fn get_part_sequential(
        &self,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let mut redirect_endpoints: Option<Vec<String>> = None;
        // Capture the writer's byte position BEFORE calling the inner store.
        // If the inner store streams partial bytes and then errors with a
        // peer-fallback-eligible code (e.g. mid-stream Internal/DataLoss/
        // Unavailable from a verifier or connection drop), we MUST NOT fall
        // through to the peer fetch — the peer would write the full blob
        // again, producing a corrupt prefix-from-inner + full-peer-copy
        // stream. Surface the original error instead. This mirrors the
        // post-peer-fallback bytes-written guard a few hundred lines below
        // (search for `bytes_written_by_workers`).
        let bytes_before_inner = writer.get_bytes_written();
        let inner_await_start = std::time::Instant::now();
        let _digest_for_log = key.borrow().into_digest();
        debug!(
            digest = ?_digest_for_log,
            offset,
            length = ?length,
            "WorkerProxyStore::get_part_sequential: awaiting inner.get_part (will reveal whether NotFound returns or stream hangs)"
        );
        // #171: set INNER_MISS_NO_TERMINATE so the wrapped FastSlowStore
        // does NOT close the OUTER writer on populate-NotFound. We re-use
        // this same writer for peer-fetch fallback below (try_read_from_worker
        // → get_part_and_cache); a closed writer there means peer bytes
        // arrive but cannot be delivered. WorkerProxyStore owns the writer-
        // termination contract from this point on.
        let inner_result = INNER_MISS_NO_TERMINATE
            .scope(
                true,
                IS_WORKER_REQUEST.scope(
                    true,
                    self.inner.get_part(key.borrow(), &mut *writer, offset, length),
                ),
            )
            .await;
        let inner_elapsed_ms = inner_await_start.elapsed().as_millis() as u64;
        debug!(
            digest = ?_digest_for_log,
            inner_elapsed_ms,
            ok = inner_result.is_ok(),
            code = ?inner_result.as_ref().err().map(|e| e.code),
            bytes_written_by_inner = writer.get_bytes_written() - bytes_before_inner,
            "WorkerProxyStore::get_part_sequential: inner.get_part returned"
        );
        match inner_result {
            Ok(()) => return Ok(()),
            Err(e) if should_try_peers(e.code) => {
                let bytes_written_by_inner =
                    writer.get_bytes_written() - bytes_before_inner;
                if bytes_written_by_inner > 0 {
                    // Inner wrote partial bytes before erroring; peer-fetch
                    // would corrupt the consumer stream. Surface the
                    // original error.
                    //
                    // #284 part 2 defensive belt-and-suspenders: detect
                    // at-cap (`Code::ResourceExhausted` carrying a
                    // `BackpressureSignal`) and warn loudly. The
                    // populator-side fix
                    // (`fast_slow_store::run_producer`'s `cache_tee_at_cap`
                    // demotion) prevents this code path from being reached
                    // for MemoryStore at-cap events; if this branch ever
                    // fires WITH the at-cap discriminator, a populator
                    // regression has reintroduced mid-stream poisoning and
                    // the consumer stream is being aborted exactly as the
                    // 2026-05-06 read-cascade-abort did. Scream so the
                    // regression is visible in production logs.
                    if e.code == Code::ResourceExhausted
                        && error_has_backpressure_signal(&e)
                    {
                        warn!(
                            key = ?key.borrow().into_digest(),
                            bytes_written_by_inner,
                            err = %e,
                            "WorkerProxyStore: inner store wrote partial bytes then \
                             returned at-cap (ResourceExhausted+BackpressureSignal) — \
                             populator-side warn-and-continue regressed; consumer stream \
                             will be aborted (#284 part 2 invariant violated)"
                        );
                    }
                    // #336 P1 sibling fix: inner store wrote partial bytes
                    // then errored without terminating the writer (leaf
                    // contract: inner does NOT terminate on Err). Wrapping
                    // callers that join on this writer's tx/rx pair would
                    // deadlock. Terminate explicitly with the constructed
                    // wrapping error so the paired reader observes it.
                    let err = make_err!(
                        e.code,
                        "WorkerProxyStore: inner store wrote {bytes_written_by_inner} bytes \
                         then failed with {:?} ({}); cannot peer-fetch without corrupting \
                         consumer stream",
                        e.code,
                        e.message_string()
                    );
                    writer.send_error(err.clone());
                    return Err(err);
                }
                // Promoted to info! to verify the client-cancellation hypothesis
                // for digests that show inner-NotFound but never reach
                // try_read_from_worker (e.g. a7fd12e4...-242504 across 11 reads
                // / 12 hours: producer logs but no worker_proxy_store logs).
                // If this line ALSO doesn't appear post-deploy for those
                // digests, the get_part future is being cancelled by the
                // gRPC client before the inner-await returns. If it DOES
                // appear, look at why try_read_from_worker is then skipped.
                info!(
                    key = ?key.borrow().into_digest(),
                    code = ?e.code,
                    "WorkerProxyStore: inner store miss, consulting locality map"
                );
            }
            Err(e) if e.code == Code::FailedPrecondition => {
                let msg = e.message_string();
                if let Some(start) = msg.find(REDIRECT_PREFIX) {
                    let endpoints_str = &msg[start + REDIRECT_PREFIX.len()..];
                    let endpoints_str = endpoints_str
                        .split('|')
                        .next()
                        .unwrap_or(endpoints_str);
                    let endpoints: Vec<String> = endpoints_str
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                    if !endpoints.is_empty() {
                        debug!(
                            key = ?key.borrow().into_digest(),
                            ?endpoints,
                            "WorkerProxyStore: received redirect from inner store"
                        );
                        redirect_endpoints = Some(endpoints);
                    }
                }
                if redirect_endpoints.is_none() {
                    // #336 P1 sibling fix: inner returned FailedPrecondition
                    // without a parseable redirect. Inner leaf stores do not
                    // terminate the writer on Err; wrapper layer is the
                    // load-bearing guard. Idempotent if inner already did.
                    writer.send_error(e.clone());
                    return Err(e);
                }
            }
            Err(e) => {
                // #336 P1 sibling fix: catch-all inner Err. Inner leaf
                // stores do not terminate the writer on Err. Terminate
                // here so wrapping callers' rx unblocks. Idempotent.
                writer.send_error(e.clone());
                return Err(e);
            }
        }

        let is_worker = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);

        if let Some(endpoints) = redirect_endpoints {
            // For worker requests, pass the redirect through instead of
            // following it — workers should fetch from peers directly.
            if is_worker {
                let digest = key.borrow().into_digest();
                let ep_str = endpoints.join(",");
                debug!(
                    ?digest,
                    endpoints = ep_str.as_str(),
                    "WorkerProxyStore: passing redirect through to worker"
                );
                // #336 P1 sibling fix: redirect-passthrough is constructed
                // by us; inner did not terminate the writer (it returned
                // FailedPrecondition+redirect with no bytes). Terminate
                // so wrapping callers' rx unblocks. Bazel-facing
                // classifier still observes the structured redirect.
                let err = make_err!(
                    Code::FailedPrecondition,
                    "{REDIRECT_PREFIX}{ep_str}|"
                );
                writer.send_error(err.clone());
                return Err(err);
            }
            // `try_read_from_endpoints` is a network helper that may
            // partially-write before erroring; on `?` propagation the
            // writer may be in any state. Defense-in-depth: surface the
            // error to the paired reader. Idempotent if helper already did.
            match self
                .try_read_from_endpoints(key.borrow(), writer, offset, length, &endpoints)
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e) => {
                    writer.send_error(e.clone());
                    return Err(e);
                }
            }
        }

        if is_worker {
            // The reader's role splits two ways at this point:
            //
            // - **Server-side WorkerProxyStore** (`race_peers=false`): the
            //   server is responding to a worker's incoming Read.
            //   Consult the server-side locality_map: if any peer worker
            //   has the blob, return a `REDIRECT_PREFIX` error so the
            //   requesting worker fetches the bytes directly from those
            //   peers (saving server bandwidth + RAM). If no peer has it,
            //   fall through to NotFound.
            //
            // - **Worker-side WorkerProxyStore** (`race_peers=true`): the
            //   worker is responding to an incoming external Read (from
            //   another host, e.g. server's proxy or a peer following a
            //   redirect). A worker MUST NEVER chain external RPCs while
            //   responding to someone else's request — that's the loop-
            //   terminator invariant: chains can never propagate past the
            //   first hop because responders refuse to chain. Just return
            //   NotFound; do not generate a redirect (workers don't have
            //   authority to redirect anyone), and do not try peers
            //   (race_peers is for INITIATOR-mode reads when the worker
            //   is satisfying its own action-input needs).
            let digest = key.borrow().into_digest();
            if self.race_peers.load(Ordering::Relaxed) {
                // Worker side, responder mode. No chain.
                debug!(
                    ?digest,
                    "WorkerProxyStore (worker side): incoming Read for missing blob — \
                     returning NotFound without chaining (responder mode never RPCs out)"
                );
                let err = Error::not_found_with_detail(
                    format!(
                        "Blob {digest:?} not found in this worker's inner store \
                         (responder mode, no external RPCs)"
                    ),
                    make_precondition_failure_any(digest),
                );
                // #336 P1: terminate the borrowed writer so any wrapping
                // caller that joins on the writer's tx/rx pair sees the
                // structured NotFound instead of deadlocking on the
                // un-EOF'd writer. Inner WAS consulted at the top of
                // `get_part_sequential` (the `self.inner.get_part(...)`
                // await above) and returned NotFound with zero bytes
                // written; per the wrapper-layer contract its NotFound
                // does NOT terminate the writer (leaf stores leave that
                // to the wrapper). WorkerProxyStore IS that wrapper.
                // Idempotent — safe even if some earlier call did.
                writer.send_error(err.clone());
                return Err(err);
            }
            // Server side: generate a single-hop redirect to peers in
            // locality_map. The receiving worker handles the redirect by
            // calling `try_read_from_endpoints` → `get_part_and_cache` →
            // `peer.get_part(...)` WITH `IS_WORKER_REQUEST.scope(true, ...)`
            // wrapping (added so the propagation reaches the next peer via
            // `grpc_store.rs:827`'s `x-nativelink-worker` header). The
            // peer's `WorkerProxyStore.get_part` top-of-function gate
            // (race_peers && IS_WORKER_REQUEST) then enters responder
            // mode and returns inner-only — no further peer chaining.
            // Loop terminates at depth 1: server → worker.
            let peers: Vec<String> = self
                .locality_map
                .read()
                .lookup_workers(&digest)
                .iter()
                .map(|p| p.to_string())
                .collect();
            if !peers.is_empty() {
                let ep_str = peers.join(",");
                debug!(
                    ?digest,
                    endpoints = ep_str.as_str(),
                    "WorkerProxyStore: returning redirect to is_worker caller"
                );
                // #336 P1 sibling fix: we constructed this redirect Err;
                // no inner/peer terminated the writer (inner returned
                // NotFound with zero bytes per the guarded branch above,
                // and we did not consult any peer here). Terminate
                // explicitly so wrapping callers' rx unblocks. Idempotent.
                let err = make_err!(
                    Code::FailedPrecondition,
                    "{REDIRECT_PREFIX}{ep_str}|"
                );
                writer.send_error(err.clone());
                return Err(err);
            }
            let err = Error::not_found_with_detail(
                format!(
                    "Blob {digest:?} not found in inner store or any peer (worker request)"
                ),
                make_precondition_failure_any(digest),
            );
            // #336 P1: writer never written by inner (the inner.get_part
            // upstream only ran in worker-side responder mode, which we
            // ARE NOT in here — we're in worker-side server-fetch mode).
            // Terminate explicitly so any wrapping caller's reader
            // unblocks. Idempotent.
            writer.send_error(err.clone());
            return Err(err);
        }

        let bytes_before_workers = writer.get_bytes_written();
        // `try_read_from_worker` is a network helper that may
        // partially-write before erroring. Surface its Err to the paired
        // reader on `?` propagation. Idempotent if helper already did.
        let worker_outcome = match self
            .try_read_from_worker(key.borrow(), writer, offset, length)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                writer.send_error(e.clone());
                return Err(e);
            }
        };
        if worker_outcome {
            return Ok(());
        }

        // All workers failed. The blob may have arrived in the inner store
        // while we were trying workers (e.g. another client uploaded it, or
        // a backfill completed). Re-check before giving up.
        //
        // Only safe to retry if no bytes were written to the writer by any
        // worker — otherwise the consumer would receive overlapping data.
        let bytes_written_by_workers = writer.get_bytes_written() - bytes_before_workers;
        if bytes_written_by_workers > 0 {
            // #336 P1 sibling fix: worker wrote partial bytes then failed.
            // We constructed this Internal Err; neither inner nor worker
            // terminated the writer (workers stream and don't synthesize
            // termination on caller-side abort). Terminate explicitly.
            let err = make_err!(
                Code::Internal,
                "Blob {:?} worker transfer wrote {} bytes then failed, \
                 cannot retry inner store without data corruption",
                key.borrow().into_digest(),
                bytes_written_by_workers
            );
            writer.send_error(err.clone());
            return Err(err);
        }
        match self
            .inner
            .get_part(key.borrow(), &mut *writer, offset, length)
            .await
        {
            Ok(()) => {
                info!(
                    digest = ?key.borrow().into_digest(),
                    "WorkerProxyStore: inner store retry succeeded after all workers failed"
                );
                return Ok(());
            }
            Err(e) if e.code == Code::NotFound => {
                // Still not found — fall through to the final error.
            }
            Err(e) => {
                // #336 P1 sibling fix: inner retry returned non-NotFound
                // Err. Leaf stores do not terminate the writer; wrapper is
                // load-bearing. Terminate so wrapping callers' rx unblocks.
                writer.send_error(e.clone());
                return Err(e);
            }
        }

        let digest = key.borrow().into_digest();
        // CCS-drop §3.3 stale-AC signal: both the inner CAS and all workers
        // returned NotFound. This is the stale-AC symptom — Bazel received a
        // cache-hit AC entry referencing this blob but the blob is genuinely
        // gone from every source. Increment the operator-facing counter so
        // stale-AC accumulation is observable even when CCS's own
        // `ccs_incomplete_entries_counter` is unavailable (e.g. during the
        // soak with `disable_completeness_check = true`).
        self.wps_cas_and_peer_notfound_total.inc();
        let err = Error::not_found_with_detail(
            format!("Blob {digest:?} not found in inner store or any worker"),
            make_precondition_failure_any(digest),
        );
        // #336 P1: terminate the borrowed writer so any wrapping caller
        // that joins on the writer's tx/rx pair sees the structured
        // NotFound instead of deadlocking on the un-EOF'd writer.
        // Inner leaf stores (e.g. MemoryStore) do NOT terminate the
        // writer on NotFound — the wrapper layer is the load-bearing
        // guard. WorkerProxyStore IS that wrapper here. Idempotent.
        writer.send_error(err.clone());
        Err(err)
    }

    /// Cooperatively cancel a losing racer: drop its receive half (which
    /// causes the spawned `get_part`'s next `tx.send` to return
    /// `Err(disconnected)` and the task to exit naturally, dropping its
    /// inner `tonic::Streaming` from the await path rather than from
    /// mid-poll). Falls back to `abort()` if the task doesn't exit
    /// within `LOSER_GRACE`.
    ///
    /// NOTE: server-streaming Read RPCs still emit RST_STREAM when their
    /// `Streaming<ReadResponse>` is dropped (no `END_STREAM` send path
    /// on one-way receive). The win is that cooperative cancellation
    /// converts an active `abort()` into a passive `Drop` from the
    /// producer's normal exit path, which tonic handles more cleanly.
    /// Combined with the `parallel_chunk_count: 64 → 16` reduction
    /// (#147 producer-side), post-burst RST rate stays under hyper's
    /// `max_local_error_reset_streams = 1024` budget.
    fn cancel_loser_racer(
        loser_rx: DropCloserReadHalf,
        loser_handle: JoinHandle<Result<(), Error>>,
    ) {
        const LOSER_GRACE: Duration = Duration::from_millis(50);
        // Dropping the rx causes the producer's next `tx.send` to fail.
        drop(loser_rx);
        // Capture an abort_handle BEFORE moving handle into the timeout
        // future, so we can fall back to abort() on grace-window expiry.
        let abort_handle = loser_handle.abort_handle();
        tokio::spawn(async move {
            if tokio::time::timeout(LOSER_GRACE, loser_handle).await.is_err() {
                abort_handle.abort();
            }
        });
    }

    /// Forward remaining data from a racer's read half to the caller's writer,
    /// then wait for the spawned task to complete.
    async fn forward_racer(
        winner_name: &str,
        writer: &mut DropCloserWriteHalf,
        rx: &mut DropCloserReadHalf,
        handle: JoinHandle<Result<(), Error>>,
    ) -> Result<(), Error> {
        // Forward all remaining chunks from the racer's channel to the
        // caller's writer. bind_buffered handles EOF propagation.
        //
        // CRITICAL (#244 — sibling of #230 M1 BLOCK fix at line 1788-1791):
        // on `bind_buffered` Err early-return, the spawned racer task
        // (`handle`) is leaked because `JoinHandle::drop` DETACHES rather
        // than aborts. The racer is producing into the matching `tx` of
        // the caller-owned `rx`; on consumer-disconnect (writer.send Err
        // mid-stream) the caller drops `rx` only after we return, so the
        // racer can wedge on `tx.send().await` for an unbounded window
        // (default buf_channel cap = 1024 slots; large blobs / many
        // chunks fill it well before the rx drops). Aborting before
        // returning bounds recovery to one scheduler tick.
        //
        // Mirrors the #230 M1 idiom: abort the spawned handle on the
        // forward-error path. Belt-and-suspenders with the natural
        // rx-drop the caller performs after we return — either alone is
        // sufficient, but `abort()` here cancels at the next await point
        // even if the rx-drop is delayed (e.g. the caller's frame holds
        // the rx in scope past additional `.await` points). Production
        // path: worker-side `race_peers=true` parallel-race fetch on
        // any peer-winner OR server-winner branch where the consumer
        // disconnects after the first chunk.
        if let Err(e) = writer.bind_buffered(rx).await {
            handle.abort();
            return Err(e).err_tip(|| {
                format!("WorkerProxyStore: {winner_name} racer bind_buffered")
            });
        }

        // Wait for the spawned get_part to confirm it finished successfully.
        // If the task was already done (sent EOF), this returns immediately.
        handle
            .await
            .map_err(|e| make_err!(Code::Internal, "WorkerProxyStore: {winner_name} task join error: {e}"))?
            .err_tip(|| format!("WorkerProxyStore: {winner_name} get_part failed after winning race"))
    }

    /// Server racer either errored or returned an empty-EOF for a non-zero
    /// digest (stale-positive). Wait for the peer racer instead. If the
    /// peer also produces an empty-EOF for a non-zero digest, surface
    /// `Code::NotFound` — the blob is unavailable from either source.
    async fn await_peer_after_empty_server(
        writer: &mut DropCloserWriteHalf,
        peer_rx: &mut DropCloserReadHalf,
        peer_handle: JoinHandle<Result<(), Error>>,
        digest: &DigestInfo,
        peer_endpoint: &Arc<str>,
        is_zero_blob: bool,
    ) -> Result<(), Error> {
        let peer_chunk = match peer_rx.recv().await {
            Ok(c) => c,
            Err(e) => {
                // #336 P1 sibling fix (parallel-race path): peer racer
                // failed on the second recv. We're returning Err via `?`-
                // shaped propagation; the outer `get_part` joins on this
                // writer's tx/rx pair (when wrapped). Terminate so the
                // paired reader unblocks. Idempotent.
                let err = e.append("WorkerProxyStore: peer recv after server failure/empty");
                writer.send_error(err.clone());
                return Err(err);
            }
        };
        if peer_chunk.is_empty() {
            if is_zero_blob {
                writer.send_eof()
                    .err_tip(|| "WorkerProxyStore: peer EOF for zero-length blob")?;
                return peer_handle.await
                    .map_err(|e| make_err!(Code::Internal, "peer task join: {e}"))?;
            }
            // Non-zero digest, no data from either racer — surface NotFound.
            // #336 P1 sibling fix (parallel-race path): we constructed
            // this NotFound; neither racer wrote bytes nor terminated the
            // outer writer. Wrapping caller's rx must observe it.
            let err = Error::not_found_with_detail(
                format!(
                    "WorkerProxyStore: both server and peer {} returned empty EOF for non-zero digest {:?} (size_bytes={})",
                    peer_endpoint,
                    digest,
                    digest.size_bytes(),
                ),
                make_precondition_failure_any(*digest),
            );
            writer.send_error(err.clone());
            return Err(err);
        }
        debug!(
            ?digest,
            endpoint = %peer_endpoint,
            "WorkerProxyStore: peer won race (server empty/failed)"
        );
        writer.send(peer_chunk).await
            .err_tip(|| "WorkerProxyStore: sending peer fallback chunk")?;
        Self::forward_racer("peer", writer, peer_rx, peer_handle).await
    }

    /// Peer racer either errored or returned an empty-EOF for a non-zero
    /// digest (stale-positive — locality already evicted by caller). Wait
    /// for the server racer instead. If the server also returns empty for
    /// a non-zero digest, surface `Code::NotFound`.
    async fn await_server_after_empty_peer(
        writer: &mut DropCloserWriteHalf,
        server_rx: &mut DropCloserReadHalf,
        server_handle: JoinHandle<Result<(), Error>>,
        digest: &DigestInfo,
        is_zero_blob: bool,
    ) -> Result<(), Error> {
        let server_chunk = match server_rx.recv().await {
            Ok(c) => c,
            Err(e) => {
                // #336 P1 sibling fix (parallel-race path): server racer
                // failed on the second recv. Terminate the outer writer
                // so wrapping callers' rx unblocks. Idempotent.
                let err = e.append("WorkerProxyStore: server recv after peer failure/empty");
                writer.send_error(err.clone());
                return Err(err);
            }
        };
        if server_chunk.is_empty() {
            if is_zero_blob {
                writer.send_eof()
                    .err_tip(|| "WorkerProxyStore: server EOF for zero-length blob")?;
                return server_handle.await
                    .map_err(|e| make_err!(Code::Internal, "server task join: {e}"))?;
            }
            // #336 P1 sibling fix (parallel-race path): we constructed
            // this NotFound; neither racer wrote bytes nor terminated the
            // outer writer. Wrapping caller's rx must observe it.
            let err = Error::not_found_with_detail(
                format!(
                    "WorkerProxyStore: both peer and server returned empty EOF for non-zero digest {:?} (size_bytes={})",
                    digest,
                    digest.size_bytes(),
                ),
                make_precondition_failure_any(*digest),
            );
            writer.send_error(err.clone());
            return Err(err);
        }
        debug!(
            ?digest,
            "WorkerProxyStore: server won race (peer empty/failed)"
        );
        writer.send(server_chunk).await
            .err_tip(|| "WorkerProxyStore: sending server fallback chunk")?;
        Self::forward_racer("server", writer, server_rx, server_handle).await
    }

    /// Mirror a blob to a random connected worker for OOM redundancy.
    /// Fire-and-forget: errors are logged but do not propagate.
    /// The blob data is passed as `Bytes` to avoid re-reading from the store.
    /// Threshold above which mirror uses streaming `update()` instead of
    /// `update_oneshot()`. 4 MiB is well under the 64 MiB gRPC max message
    /// size, giving headroom for framing overhead.
    const MIRROR_CHUNK_THRESHOLD: usize = 4 * 1024 * 1024;

    /// Chunk size for the streaming mirror path. 3 MiB matches the
    /// `max_bytes_per_stream` default used by ByteStream configs.
    const MIRROR_CHUNK_SIZE: usize = 3 * 1024 * 1024;

    /// Pick the next mirror endpoint, skipping anything currently quarantined
    /// and the optional `exclude` (used by retry to pick a different worker).
    /// Returns the endpoint string and a permit clone for that endpoint.
    /// `None` if there are no eligible workers.
    ///
    /// Falls back to ignoring the quarantine list if every endpoint is
    /// quarantined — a degraded mirror is better than no mirror at all when
    /// the quarantine itself may be the result of a transient cluster-wide
    /// problem.
    ///
    /// Locking: the steady-state path (every endpoint already in the map,
    /// no quarantine to clear) takes only the read lock. The write lock is
    /// taken only on (a) first-ever sighting of an endpoint or (b) cleanup
    /// of an expired quarantine. Mirrors are called per blob during write
    /// bursts, so keeping the hot path read-only avoids serializing fan-out.
    /// Pick a mirror endpoint, filtering out peers whose last-reported
    /// `(mirror_used_bytes + size_bytes) > mirror_max_bytes`. The
    /// capacity check is the review #1 fix: BEFORE the source stream is
    /// consumed, we exclude peers we know cannot accept the blob.
    /// Saturated peers that slip through (e.g. capacity report is stale
    /// because the worker just inserted a different blob) still surface
    /// the cap-exceeded `Err(ResourceExhausted)` from
    /// `insert_mirror_blob`, which `record_mirror_failure(Saturated)`
    /// handles without quarantining.
    ///
    /// `size_bytes = 0` skips the filter (used by callers that don't
    /// know the size — preserves pre-fix behavior).
    fn pick_mirror_endpoint(
        &self,
        endpoints: &[Arc<str>],
        exclude: Option<&str>,
        size_bytes: u64,
    ) -> Option<(Arc<str>, Arc<Semaphore>)> {
        if endpoints.is_empty() {
            return None;
        }
        let now = Instant::now();

        // Try the read-only fast path. We can serve the request without a
        // write lock if every endpoint we'd consider has an existing entry
        // with no quarantine that needs clearing.
        if let Some(pick) =
            self.pick_mirror_endpoint_read(endpoints, exclude, now, size_bytes)
        {
            return Some(pick);
        }
        // Slow path: missing entries or expired quarantines need cleanup.
        self.pick_mirror_endpoint_write(endpoints, exclude, now, size_bytes)
    }

    /// Filter `peers` to drop any endpoint currently quarantined
    /// (`quarantined_until > now`). Used by the read-side peer-fetch
    /// selector (#67) to avoid hammering a persistently-failing peer on
    /// every same-digest read until the locality entry is reactively
    /// evicted. The same `EndpointStateRegistry` (here: `mirror_state`)
    /// the mirror-write picker consults is consulted here.
    ///
    /// If every candidate is quarantined the returned vec is empty;
    /// callers fall through to whatever the existing fallback is (here:
    /// `get_part_sequential`).
    ///
    /// Endpoints with no entry in `mirror_state` (never recorded a
    /// failure) are kept — they are healthy by definition.
    fn filter_quarantined_peers(
        &self,
        peers: Vec<Arc<str>>,
    ) -> Vec<Arc<str>> {
        if peers.is_empty() {
            return peers;
        }
        let now = Instant::now();
        let state = self.mirror_state.read();
        peers
            .into_iter()
            .filter(|ep| match state.get(ep) {
                Some(entry) => entry
                    .quarantined_until
                    .is_none_or(|t| t <= now),
                None => true,
            })
            .collect()
    }

    /// Read-lock fast path. Returns `None` if any endpoint we'd consider is
    /// missing from the state map or has an expired quarantine that should
    /// be cleared — both require a write lock to fix.
    fn pick_mirror_endpoint_read(
        &self,
        endpoints: &[Arc<str>],
        exclude: Option<&str>,
        now: Instant,
        size_bytes: u64,
    ) -> Option<(Arc<str>, Arc<Semaphore>)> {
        let state = self.mirror_state.read();
        let mut eligible_count = 0usize;
        let mut considered_count = 0usize;
        for ep in endpoints {
            if exclude.is_some_and(|x| x == ep.as_ref()) {
                continue;
            }
            let Some(entry) = state.get(ep) else {
                // Need write lock to insert.
                return None;
            };
            considered_count += 1;
            match entry.quarantined_until {
                Some(t) if t > now => continue, // still quarantined, skip
                Some(_) => return None,         // expired — clear under write lock
                None => {
                    if size_bytes == 0 || entry.fits(size_bytes) {
                        eligible_count += 1;
                    }
                    // Capacity-rejected peers count toward considered_count
                    // (so the caller knows there ARE peers, just full)
                    // but NOT toward eligible_count (so the picker
                    // prefers a fits-able peer).
                }
            }
        }
        if considered_count == 0 {
            return None;
        }
        let pool_size = if eligible_count > 0 {
            eligible_count
        } else {
            considered_count
        };
        let idx =
            self.mirror_counter.fetch_add(1, Ordering::Relaxed) as usize % pool_size;
        // Walk the endpoints again to find the idx-th match without
        // allocating a Vec.
        let mut seen = 0usize;
        for ep in endpoints {
            if exclude.is_some_and(|x| x == ep.as_ref()) {
                continue;
            }
            // We already checked all endpoints exist with no expired
            // quarantine, so this lookup must succeed.
            let entry = state.get(ep)?;
            let active = entry.quarantined_until.is_some_and(|t| t > now);
            let fits = size_bytes == 0 || entry.fits(size_bytes);
            let in_pool = if eligible_count > 0 {
                !active && fits
            } else {
                // Degraded mode: every peer is either quarantined or
                // saturated. Pick anyway (better to try than to drop).
                true
            };
            if !in_pool {
                continue;
            }
            if seen == idx {
                return Some((ep.clone(), entry.permits.clone()));
            }
            seen += 1;
        }
        None
    }

    /// Slow path: takes the write lock to insert missing entries and clear
    /// expired quarantines, then picks an endpoint.
    fn pick_mirror_endpoint_write(
        &self,
        endpoints: &[Arc<str>],
        exclude: Option<&str>,
        now: Instant,
        size_bytes: u64,
    ) -> Option<(Arc<str>, Arc<Semaphore>)> {
        let mut state = self.mirror_state.write();
        let mut eligible_count = 0usize;
        let mut considered_count = 0usize;
        for ep in endpoints {
            if exclude.is_some_and(|x| x == ep.as_ref()) {
                continue;
            }
            let entry = state
                .entry(ep.clone())
                .or_insert_with(MirrorEndpointState::new);
            considered_count += 1;
            if entry.quarantined_until.is_some_and(|t| t > now) {
                continue;
            }
            // Either never quarantined or quarantine expired — clear.
            entry.quarantined_until = None;
            // Apply the same capacity filter as the read path.
            if size_bytes == 0 || entry.fits(size_bytes) {
                eligible_count += 1;
            }
        }
        if considered_count == 0 {
            return None;
        }
        let pool_size = if eligible_count > 0 {
            eligible_count
        } else {
            considered_count
        };
        let idx =
            self.mirror_counter.fetch_add(1, Ordering::Relaxed) as usize % pool_size;
        let mut seen = 0usize;
        for ep in endpoints {
            if exclude.is_some_and(|x| x == ep.as_ref()) {
                continue;
            }
            let entry = state.get(ep)?;
            let active = entry.quarantined_until.is_some_and(|t| t > now);
            let fits = size_bytes == 0 || entry.fits(size_bytes);
            let in_pool = if eligible_count > 0 {
                !active && fits
            } else {
                true
            };
            if !in_pool {
                continue;
            }
            if seen == idx {
                return Some((ep.clone(), entry.permits.clone()));
            }
            seen += 1;
        }
        None
    }

    fn record_mirror_success(&self, endpoint: &str) {
        let mut state = self.mirror_state.write();
        if let Some(entry) = state.get_mut(endpoint) {
            entry.consecutive_failures = 0;
            entry.first_failure_at = None;
            entry.quarantined_until = None;
        }
    }

    /// Record the last-reported mirror-blob capacity for `endpoint`.
    /// Called from the worker_api_server when `BlobsAvailableNotification`
    /// includes mirror-bytes fields. Used by `pick_mirror_endpoint_*`
    /// to filter peers whose `(used + size_bytes) > max` BEFORE the
    /// source stream is consumed (review #1).
    ///
    /// Short-circuits when `(used_bytes, max_bytes)` is unchanged from the
    /// last report (review #10) — workers tick every ~100ms and a
    /// long-running cluster can spin record_mirror_capacity at ~100/s/peer
    /// taking the RwLock::write each time. The early-return uses the
    /// read lock and `RwLock` upgrades only on a real change.
    pub fn record_mirror_capacity(
        &self,
        endpoint: &str,
        used_bytes: u64,
        max_bytes: u64,
    ) {
        // Read-only fast path: skip the write lock when the value hasn't
        // changed since the last tick. Most ticks are no-ops.
        {
            let state = self.mirror_state.read();
            if let Some(entry) = state.get(endpoint) {
                if entry.mirror_used_bytes == Some(used_bytes)
                    && entry.mirror_max_bytes == Some(max_bytes)
                {
                    return;
                }
            }
        }
        let mut state = self.mirror_state.write();
        let entry = state
            .entry(Arc::from(endpoint))
            .or_insert_with(MirrorEndpointState::new);
        entry.mirror_used_bytes = Some(used_bytes);
        entry.mirror_max_bytes = Some(max_bytes);
    }

    /// Test-only accessor: returns the last-reported `(used, max)` for
    /// `endpoint`, or `None` if no capacity report has been recorded.
    /// Used by integration tests to assert that `BlobsAvailable`
    /// capacity fields are plumbed end-to-end through
    /// `WorkerApiServer::handle_blobs_available`.
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn mirror_capacity_for_test(
        &self,
        endpoint: &str,
    ) -> Option<(u64, u64)> {
        let state = self.mirror_state.read();
        state.get(endpoint).and_then(|entry| {
            match (entry.mirror_used_bytes, entry.mirror_max_bytes) {
                (Some(u), Some(m)) => Some((u, m)),
                _ => None,
            }
        })
    }

    /// Record a mirror-write failure against `endpoint` according to its
    /// classification:
    ///   * [`MirrorFailureKind::Generic`] — quarantine fires only after
    ///     `MIRROR_FAILURE_THRESHOLD` failures inside `MIRROR_FAILURE_WINDOW`.
    ///   * [`MirrorFailureKind::DefinitiveUnreachable`] — quarantine fires
    ///     immediately. A transport-level `ConnectionRefused` /
    ///     `NetworkUnreachable` / `HostUnreachable` is sufficient evidence
    ///     that the peer is gone; we want to stop round-robin routing to
    ///     the dead worker rather than burn the ~1s it would take to
    ///     accumulate 5 failures.
    ///   * [`MirrorFailureKind::Saturated`] — the peer's mirror cap is full.
    ///     This is NOT evidence the peer is broken — it is healthy and
    ///     responding, just out of room. Skip the streak update entirely
    ///     so a peer that fills up first does not get quarantined out of
    ///     the rotation; the picker filters saturated peers via the
    ///     capacity pre-check (review #1) so this path is a fallback for
    ///     the small remaining race window only.
    /// In all cases the quarantine itself lasts `MIRROR_QUARANTINE_DURATION`.
    fn record_mirror_failure(&self, endpoint: &str, kind: MirrorFailureKind) {
        let now = Instant::now();
        let mut state = self.mirror_state.write();
        let entry = state
            .entry(Arc::from(endpoint))
            .or_insert_with(MirrorEndpointState::new);
        match kind {
            MirrorFailureKind::Saturated => {
                // Healthy peer, just full. Do not perturb the failure
                // streak — quarantining a full peer would compound a
                // transient memory-pressure issue into a hard outage.
                debug!(
                    endpoint,
                    "mirror: peer saturated (ResourceExhausted); skipping quarantine streak update"
                );
                return;
            }
            MirrorFailureKind::DefinitiveUnreachable => {
                entry.consecutive_failures = MIRROR_FAILURE_THRESHOLD;
                if entry.first_failure_at.is_none() {
                    entry.first_failure_at = Some(now);
                }
            }
            MirrorFailureKind::Generic => {
                // Reset the streak if the previous failure was outside the window —
                // a slow drip of unrelated failures shouldn't trigger quarantine.
                match entry.first_failure_at {
                    Some(t) if now.duration_since(t) > MIRROR_FAILURE_WINDOW => {
                        entry.consecutive_failures = 1;
                        entry.first_failure_at = Some(now);
                    }
                    None => {
                        entry.consecutive_failures = 1;
                        entry.first_failure_at = Some(now);
                    }
                    _ => {
                        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                    }
                }
            }
        }
        if entry.consecutive_failures >= MIRROR_FAILURE_THRESHOLD
            && entry.quarantined_until.is_none_or(|t| t <= now)
        {
            entry.quarantined_until = Some(now + MIRROR_QUARANTINE_DURATION);
            warn!(
                endpoint,
                consecutive_failures = entry.consecutive_failures,
                quarantine_secs = MIRROR_QUARANTINE_DURATION.as_secs(),
                ?kind,
                "mirror: quarantining endpoint after consecutive failures"
            );
        }
    }

    pub async fn mirror_blob_to_random_worker(
        &self,
        digest: DigestInfo,
        data: Bytes,
    ) {
        let endpoints = self.locality_map.read().all_endpoints();
        if endpoints.is_empty() {
            return;
        }

        self.mirror_total_attempted.fetch_add(1, Ordering::Relaxed);
        let blob_size = data.len() as u64;

        // Try once on a healthy endpoint. On a connection-level failure,
        // try once more on a different endpoint — most mirror failures
        // are connection-level (KeepAliveTimedOut, ConnectionReset, EOF
        // without close_notify) and recover on a second attempt.
        let mut last_endpoint: Option<Arc<str>> = None;
        for attempt in 0..2 {
            let exclude = last_endpoint.as_deref();
            // Capacity pre-check (review #1): filter out peers we know
            // can't fit this blob BEFORE we consume the source. The
            // ResourceExhausted Err from `insert_mirror_blob` remains
            // as a racy fallback for stale capacity reports.
            let Some((endpoint, permits)) = self
                .pick_mirror_endpoint(&endpoints, exclude, blob_size)
            else {
                self.mirror_dropped_quarantined.fetch_add(1, Ordering::Relaxed);
                return;
            };
            // Per-worker permit: prevents one slow worker from starving
            // mirror capacity for healthier ones. 50ms cap so a saturated
            // worker doesn't queue up cloned `Bytes` (each waiter pins
            // ~size_bytes of memory until the permit drops). On timeout
            // we move to the next endpoint instead of giving up — that
            // preserves the dual-endpoint resilience of this path.
            let _permit = match tokio::time::timeout(
                Duration::from_millis(50),
                permits.acquire(),
            )
            .await
            {
                Ok(Ok(p)) => p,
                Ok(Err(_)) | Err(_) => {
                    self.mirror_dropped_no_permit.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        %digest,
                        endpoint = endpoint.as_ref(),
                        attempt,
                        "mirror: permit busy, trying next endpoint"
                    );
                    last_endpoint = Some(endpoint);
                    continue;
                }
            };

            let Some(store) = self.get_or_create_connection(&endpoint).await else {
                warn!(
                    %digest,
                    endpoint = endpoint.as_ref(),
                    attempt,
                    "mirror: failed to connect to worker"
                );
                self.record_mirror_failure(
                    &endpoint,
                    MirrorFailureKind::DefinitiveUnreachable,
                );
                last_endpoint = Some(endpoint);
                continue;
            };

            let size_bytes = data.len();
            let data_clone = data.clone();
            let result = IS_MIRROR_REQUEST.scope(true, async {
                if size_bytes > Self::MIRROR_CHUNK_THRESHOLD {
                    // Large blob: stream in chunks to stay under gRPC max message size.
                    let (mut tx, rx) = make_buf_channel_pair();
                    let chunk_size = Self::MIRROR_CHUNK_SIZE;
                    tokio::spawn(async move {
                        let mut offset = 0;
                        while offset < data_clone.len() {
                            let end = (offset + chunk_size).min(data_clone.len());
                            let chunk = data_clone.slice(offset..end);
                            if tx.send(chunk).await.is_err() {
                                return;
                            }
                            offset = end;
                        }
                        drop(tx.send_eof());
                    });
                    let key: StoreKey<'_> = digest.into();
                    store
                        .update(key, rx, UploadSizeInfo::ExactSize(size_bytes as u64))
                        .await
                } else {
                    // Small blob: single-message oneshot is more efficient.
                    store.update_oneshot(digest, data_clone).await
                }
            })
            .await;

            match result {
                Ok(()) => {
                    self.record_mirror_success(&endpoint);
                    self.mirror_total_succeeded.fetch_add(1, Ordering::Relaxed);
                    info!(
                        %digest,
                        size_bytes,
                        endpoint = endpoint.as_ref(),
                        attempt,
                        "mirror: blob sent to worker"
                    );
                    return;
                }
                Err(e) => {
                    self.record_mirror_failure(&endpoint, classify_mirror_failure(&e));
                    let retry = attempt == 0 && is_connection_error(&e);
                    warn!(
                        %digest,
                        size_bytes,
                        endpoint = endpoint.as_ref(),
                        attempt,
                        retry,
                        ?e,
                        "mirror: failed to send blob to worker"
                    );
                    if !retry {
                        return;
                    }
                    last_endpoint = Some(endpoint);
                }
            }
        }
    }

    /// Mirror a blob to a random connected worker via a streaming channel.
    /// The caller provides a `DropCloserReadHalf` that produces the blob data.
    /// Fire-and-forget semantics: errors are logged but do not propagate.
    pub async fn mirror_blob_via_stream(
        &self,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
    ) {
        let endpoints = self.locality_map.read().all_endpoints();
        if endpoints.is_empty() {
            // No workers — drain the reader so the sender doesn't block.
            drop(reader);
            return;
        }

        self.mirror_total_attempted.fetch_add(1, Ordering::Relaxed);

        // Streaming path can't retry: bytes from `reader` are consumed once.
        // We still benefit from the per-worker permit (fair fan-out), the
        // health quarantine (skip dead workers), and the capacity
        // pre-check (review #1): the digest's size is known up front, so
        // pick a peer that has room BEFORE we start consuming the
        // source stream.
        let blob_size = digest.size_bytes();
        let Some((endpoint, permits)) = self
            .pick_mirror_endpoint(&endpoints, None, blob_size)
        else {
            self.mirror_dropped_quarantined.fetch_add(1, Ordering::Relaxed);
            drop(reader);
            return;
        };
        // Streaming path can't wait on permits: the reader is already
        // buffering up to 72 MiB of producer chunks (3 MiB × 24 slots) so
        // pinning that memory while we queue is the worst case for OOM.
        // Drop instead — the streaming mirror has no retry semantic anyway.
        let _permit = match permits.try_acquire() {
            Ok(p) => p,
            Err(_) => {
                self.mirror_dropped_no_permit.fetch_add(1, Ordering::Relaxed);
                debug!(
                    %digest,
                    endpoint = endpoint.as_ref(),
                    "mirror_stream: skipped, all permits busy"
                );
                drop(reader);
                return;
            }
        };

        let Some(store) = self.get_or_create_connection(&endpoint).await else {
            warn!(
                %digest,
                endpoint = endpoint.as_ref(),
                "mirror_stream: failed to connect to worker"
            );
            self.record_mirror_failure(
                &endpoint,
                MirrorFailureKind::DefinitiveUnreachable,
            );
            drop(reader);
            return;
        };

        let size_bytes = digest.size_bytes();
        let key: StoreKey<'_> = digest.into();
        let result = IS_MIRROR_REQUEST
            .scope(true, async {
                store
                    .update(key, reader, UploadSizeInfo::ExactSize(size_bytes))
                    .await
            })
            .await;

        match &result {
            Ok(()) => {
                self.record_mirror_success(&endpoint);
                self.mirror_total_succeeded.fetch_add(1, Ordering::Relaxed);
                debug!(
                    %digest,
                    size_bytes,
                    endpoint = endpoint.as_ref(),
                    "mirror_stream: blob streamed to worker"
                );
            }
            Err(e) => {
                self.record_mirror_failure(&endpoint, classify_mirror_failure(&e));
                // #344: when the error is the typed signal from the
                // bytestream tee producer ("chunks dropped to backpressure"),
                // the failure is by-design — the producer already logged a
                // single per-blob INFO ("receiver will re-fetch on demand")
                // and bumped `mirror_chunks_dropped_backpressure`. Logging
                // a WARN here double-counts an expected event. Demote to
                // DEBUG so genuine worker failures (h2 GOAWAY, TCP RST,
                // peer disconnect — different Codes / no marker) remain
                // visible at WARN.
                if is_mirror_tee_backpressure_error(&e) {
                    debug!(
                        %digest,
                        size_bytes,
                        endpoint = endpoint.as_ref(),
                        ?e,
                        "mirror_stream: tee backpressure dropped chunks; receiver will re-fetch on demand"
                    );
                } else {
                    warn!(
                        %digest,
                        size_bytes,
                        endpoint = endpoint.as_ref(),
                        ?e,
                        "mirror_stream: failed to stream blob to worker"
                    );
                }
            }
        }
    }
}

#[async_trait]
impl StoreDriver for WorkerProxyStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Inner store first.
        self.inner.has_with_results(digests, results).await?;

        // For digests still missing from the server CAS, consult the
        // locality_map and report `Some` if any worker reports holding
        // the blob. This is what makes the worker-side fast path
        // coherent end-to-end: the bytestream upload path short-circuits
        // when `has` returns Some without storing on the server (the
        // blob is only on the worker), and Bazel's next FindMissingBlobs
        // would otherwise see "missing" and re-upload, defeating the
        // optimization.
        //
        // Stale-Some safety (v2 lost-eviction invariant). Workers send
        // explicit `BlobsAvailable.evicted_digests` on every eviction;
        // worker disconnect triggers `remove_endpoint` cleanup at
        // worker_api_server.rs:407 within ~5s; `try_read_from_worker`
        // self-heal evicts the locality entry on per-digest NotFound.
        // The worst-case stale-Some manifests as a single proxy-fetch
        // attempt that NotFounds and self-heals — same risk class as
        // server CAS evicting between FMB and Read (which we already
        // accept). Task #155 deleted the bytestream sync-confirm
        // safety-net that previously belt-and-suspendered this contract.
        if !self.consult_locality_in_has.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut missing: Vec<(usize, DigestInfo)> = Vec::new();
        for (idx, (key, slot)) in digests.iter().zip(results.iter()).enumerate() {
            if slot.is_some() {
                continue;
            }
            if let StoreKey::Digest(d) = key.borrow() {
                missing.push((idx, d));
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let only_digests: Vec<DigestInfo> = missing.iter().map(|(_, d)| *d).collect();
        let lookups = self.locality_map.read().lookup_many(&only_digests);
        for ((idx, digest), endpoints) in missing.iter().zip(lookups.iter()) {
            if endpoints.is_empty() {
                continue;
            }
            // Any endpoint is equally valid — timestamps are gone from
            // EndpointList, and the worker's own moka cache is the real
            // tiebreaker on the read path.
            results[*idx] = Some(digest.size_bytes());
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Pass through to inner store.
        self.inner.update(key, reader, upload_size).await
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        // Report LazyExistenceOnSync so that FastSlowStore skips the has()
        // check before get_part(). get_part() handles redirect/proxy logic
        // via the locality map that has_with_results() intentionally skips.
        if optimization == StoreOptimizations::LazyExistenceOnSync {
            return true;
        }
        self.inner
            .inner_store(None::<StoreKey<'_>>)
            .optimized_for(optimization)
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Responder-mode short-circuit (worker side serving an incoming
        // external Read for another host).
        //
        // Loop terminator invariant: a worker MUST NEVER chain external
        // RPCs while responding to someone else's request. Chains can
        // never propagate past the first hop because responders refuse
        // to chain. race_peers / try_read_from_worker / redirect-
        // generation are all INITIATOR-mode behavior — only when the
        // worker is satisfying its OWN action-input needs.
        //
        // This gate fires when both:
        //   - race_peers=true (worker side)
        //   - IS_WORKER_REQUEST=true (incoming external Read; set by the
        //     worker's bytestream_server when servicing a remote caller)
        //
        // Pass straight through to inner — local stores only, no
        // network. inner returns Ok if local has it, NotFound otherwise.
        // The matching gate at line ~1057 in get_part_sequential is
        // defense in depth for the "no peers in locality_map" path.
        if self.race_peers.load(Ordering::Relaxed) {
            let is_responder = IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false);
            if is_responder {
                // Writer-termination contract (#336 P1 fix): pre-fix this
                // delegated directly to `self.inner.get_part(...)` with the
                // borrowed writer. Inner leaf stores (e.g. MemoryStore,
                // FilesystemStore — verified at `memory_store.rs:567`,
                // `filesystem_store.rs:2275-2281`) do NOT terminate the
                // writer on NotFound — they `?`-propagate the structured
                // Err without `send_error`. Wrapping callers that join on
                // this writer's tx/rx pair would deadlock. WorkerProxyStore
                // is the load-bearing wrapper at this seam — terminate
                // explicitly on Err so the paired reader unblocks.
                // Idempotent — safe even if some sub-store terminates too.
                let res = self
                    .inner
                    .get_part(key, &mut *writer, offset, length)
                    .await;
                if let Err(ref e) = res {
                    writer.send_error(e.clone());
                }
                return res;
            }
        }

        // Only race when explicitly enabled (worker side). Server-side
        // WorkerProxyStore uses the sequential path which generates
        // redirects for workers and proxies for non-worker callers.
        let digest = key.borrow().into_digest();
        let peers = if self.race_peers.load(Ordering::Relaxed) {
            // #67: read-side circuit breaker. Filter out peers currently
            // quarantined by `mirror_state` so a persistently-failing
            // peer is not hammered on every same-digest read until the
            // locality entry is reactively evicted. Falls through to the
            // sequential path (server fetch) if all candidates are
            // quarantined.
            let raw = self.locality_map.read().lookup_workers(&digest);
            self.filter_quarantined_peers(raw)
        } else {
            Vec::new()
        };

        if peers.is_empty() {
            // No peers known (or server side) — use the sequential path.
            return self
                .get_part_sequential(key, writer, offset, length)
                .await;
        }

        // Try to get a connection to the first peer.
        let peer_store = match self.get_or_create_connection(&peers[0]).await {
            Some(store) => store,
            None => {
                return self
                    .get_part_sequential(key, writer, offset, length)
                    .await;
            }
        };
        let peer_endpoint: Arc<str> = peers[0].clone();

        // Create buf_channel pairs for each racer. Each spawned task writes
        // into its own tx; we read from the rx to see who produces data first.
        let (mut server_tx, mut server_rx) = make_buf_channel_pair();
        let (mut peer_tx, mut peer_rx) = make_buf_channel_pair();

        // We need owned keys for the spawned tasks.
        let server_key = key.borrow().into_owned();
        let peer_key = key.borrow().into_owned();

        // Clone inner store for the server task.
        let inner = self.inner.clone();

        // Spawn server fetch. Do NOT set IS_WORKER_REQUEST — we want the
        // server to actually serve the blob data, not return a redirect.
        let server_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            inner
                .get_part(server_key.borrow(), &mut server_tx, offset, length)
                .await
        });

        // Spawn peer fetch with IS_WORKER_REQUEST=true so the peer enters
        // responder mode and refuses to chain. Note: tokio::spawn does NOT
        // inherit task-locals, so the scope must be set INSIDE the spawned
        // task — outside the spawn is a no-op.
        //
        // TODO(cdn-tee-parallel-path): when the peer wins this race the
        // forwarded bytes are sent straight to `writer` and never tee'd
        // into local CAS, so the next read of the same digest re-races
        // (or re-fetches if the peer disappears). The sequential path
        // `get_part_and_cache` (line 883-1056) already tees; this parallel
        // path needs a similar tee on the peer-winner branches in the
        // tokio::select! below (lines ~2267 and ~2275). Tracked in
        // `.claude/agent-memory/.../project_cdn_tee_already_implemented.md`.
        let peer_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            IS_WORKER_REQUEST
                .scope(
                    true,
                    peer_store.get_part(peer_key.borrow(), &mut peer_tx, offset, length),
                )
                .await
        });

        // Whether an empty initial chunk is a legitimate zero-length-blob
        // success. For non-zero digests, an empty first chunk is a
        // stale-positive: the racer claimed it had the blob but produced
        // no bytes (e.g. peer's BatchReadBlobs returned `data: vec![]`
        // for an evicted blob). Must NOT be treated as success.
        let is_zero_blob = digest.size_bytes() == 0;

        // Race: wait for the first racer to produce a data chunk (or error).
        tokio::select! {
            server_result = server_rx.recv() => {
                match server_result {
                    Ok(chunk) if !chunk.is_empty() => {
                        // Server produced data first — it wins.
                        // #147: cooperative cancel — drop peer_rx + brief
                        // grace window before falling back to abort().
                        Self::cancel_loser_racer(peer_rx, peer_handle);
                        debug!(
                            ?digest,
                            "WorkerProxyStore: server won race against peer"
                        );
                        writer.send(chunk).await
                            .err_tip(|| "WorkerProxyStore: sending server winner chunk")?;
                        Self::forward_racer("server", writer, &mut server_rx, server_handle).await
                    }
                    Ok(_empty) if is_zero_blob => {
                        // Legitimate zero-length blob — server won the race.
                        Self::cancel_loser_racer(peer_rx, peer_handle);
                        debug!(
                            ?digest,
                            "WorkerProxyStore: server won race (zero-length blob)"
                        );
                        writer.send_eof()
                            .err_tip(|| "WorkerProxyStore: sending EOF for zero-length blob")?;
                        server_handle.await
                            .map_err(|e| make_err!(Code::Internal, "server task join: {e}"))?
                    }
                    Ok(_empty) => {
                        // Stale-positive: server reported EOF with no bytes
                        // for a non-zero digest. Wait for the peer instead.
                        warn!(
                            ?digest,
                            size_bytes = digest.size_bytes(),
                            "WorkerProxyStore: server returned empty EOF for non-zero digest, waiting for peer"
                        );
                        Self::await_peer_after_empty_server(
                            writer, &mut peer_rx, peer_handle, &digest, &peer_endpoint, is_zero_blob,
                        ).await
                    }
                    Err(_server_err) => {
                        // Server racer failed — wait for peer.
                        warn!(
                            ?digest,
                            "WorkerProxyStore: server racer failed, waiting for peer"
                        );
                        Self::await_peer_after_empty_server(
                            writer, &mut peer_rx, peer_handle, &digest, &peer_endpoint, is_zero_blob,
                        ).await
                    }
                }
            }
            peer_result = peer_rx.recv() => {
                match peer_result {
                    Ok(chunk) if !chunk.is_empty() => {
                        // Peer produced data first — it wins.
                        Self::cancel_loser_racer(server_rx, server_handle);
                        debug!(
                            ?digest,
                            endpoint = %peer_endpoint,
                            "WorkerProxyStore: peer won race against server"
                        );
                        writer.send(chunk).await
                            .err_tip(|| "WorkerProxyStore: sending peer winner chunk")?;
                        Self::forward_racer("peer", writer, &mut peer_rx, peer_handle).await
                    }
                    Ok(_empty) if is_zero_blob => {
                        // Legitimate zero-length blob — peer won the race.
                        Self::cancel_loser_racer(server_rx, server_handle);
                        debug!(
                            ?digest,
                            endpoint = %peer_endpoint,
                            "WorkerProxyStore: peer won race (zero-length blob)"
                        );
                        writer.send_eof()
                            .err_tip(|| "WorkerProxyStore: sending EOF for zero-length blob from peer")?;
                        peer_handle.await
                            .map_err(|e| make_err!(Code::Internal, "peer task join: {e}"))?
                    }
                    Ok(_empty) => {
                        // Stale-positive: peer reported EOF with no bytes
                        // for a non-zero digest. Evict the locality entry
                        // (peer claimed it had it, but lied) and wait for
                        // the server instead.
                        warn!(
                            ?digest,
                            size_bytes = digest.size_bytes(),
                            endpoint = %peer_endpoint,
                            "WorkerProxyStore: peer returned empty EOF for non-zero digest, evicting locality and waiting for server"
                        );
                        self.locality_map
                            .write()
                            .evict_blobs(&peer_endpoint, &[digest]);
                        Self::await_server_after_empty_peer(
                            writer, &mut server_rx, server_handle, &digest, is_zero_blob,
                        ).await
                    }
                    Err(_peer_err) => {
                        // Peer racer failed — wait for server.
                        warn!(
                            ?digest,
                            endpoint = %peer_endpoint,
                            "WorkerProxyStore: peer racer failed, waiting for server"
                        );
                        Self::await_server_after_empty_peer(
                            writer, &mut server_rx, server_handle, &digest, is_zero_blob,
                        ).await
                    }
                }
            }
        }
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &dyn StoreDriver {
        // Delegate to inner store so that callers can downcast through
        // the chain (e.g. worker finding FastSlowStore via downcast_ref).
        // WorkerProxyStore's optimized_for override is independent of this.
        self.inner.inner_store(key)
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.inner.register_item_callback(callback)
    }

    /// Forward to inner so that wrappers querying the slow tier of an
    /// FSS see the underlying store's true capability (the proxy does
    /// not synthesize its own removal events — it only forwards).
    fn supports_removal_callbacks(&self) -> bool {
        self.inner.supports_removal_callbacks()
    }

    /// WorkerProxyStore is a single-inner wrapper. The proxy adds locality
    /// + mirror routing on top of the inner store but does not own the BIS
    /// or pin chain — both forward unchanged via the trait defaults.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    /// `mark_stable` forwards unchanged to `inner` via the trait
    /// default's `Inner` arm (task #157 / C+D folded mark_stable into the
    /// forced-delegation enum mechanism).
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

#[async_trait]
impl HealthStatusIndicator for WorkerProxyStore {
    fn get_name(&self) -> &'static str {
        "WorkerProxyStore"
    }

    async fn check_health(
        &self,
        namespace: Cow<'static, str>,
    ) -> HealthStatus {
        self.inner.check_health(namespace).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nativelink_config::stores::MemorySpec;
    use nativelink_error::{Code, Error, make_err};
    use nativelink_macro::nativelink_test;
    use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::{
        IS_WORKER_REQUEST, REDIRECT_PREFIX, StoreLike, StoreKey, StoreOptimizations,
    };
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::memory_store::MemoryStore;

    const VALID_HASH1: &str =
        "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const VALID_HASH2: &str =
        "0123456789abcdef000000000000000000020000000000000123456789abcdef";

    /// Helper: create a WorkerProxyStore backed by a fresh MemoryStore.
    fn make_proxy_store() -> (Store, SharedBlobLocalityMap) {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map.clone());
        (Store::new(proxy), locality_map)
    }

    // ---------------------------------------------------------------
    // Locality-eviction policy: evict ONLY on NotFound / DataLoss.
    // Transient failures (DeadlineExceeded, Unavailable, Internal,
    // Aborted, Unknown transport blips) must KEEP the locality entry —
    // otherwise a single network hiccup permanently loses the only
    // routing record for blobs held by a single peer.
    //
    // Spec source: bug report — single peer-fetch failure (e.g. one
    // missed deadline) currently nukes the locality entry forever, so
    // subsequent has_with_results / FMB miss the fast path even though
    // the peer still holds the blob.
    // ---------------------------------------------------------------
    #[test]
    fn test_should_evict_on_not_found() {
        let e = make_err!(Code::NotFound, "blob not present on peer");
        assert!(
            should_evict_locality_on_peer_error(&e),
            "NotFound is a definitive 'peer no longer holds blob' signal — must evict"
        );
    }

    #[test]
    fn test_should_evict_on_data_loss() {
        let e = make_err!(Code::DataLoss, "peer delivered truncated bytes");
        assert!(
            should_evict_locality_on_peer_error(&e),
            "DataLoss means the peer's stored copy is unusable — must evict"
        );
    }

    #[test]
    fn test_should_keep_on_deadline_exceeded() {
        let e = make_err!(Code::DeadlineExceeded, "peer hung past deadline");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "DeadlineExceeded is transient — peer may still hold the blob; \
             evicting would permanently lose locality after one slow fetch"
        );
    }

    #[test]
    fn test_should_keep_on_unavailable() {
        let e = make_err!(Code::Unavailable, "transient unavailable");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "Unavailable is transient (KeepAliveTimedOut, RST_STREAM, etc.) \
             — must keep the locality entry"
        );
    }

    #[test]
    fn test_should_keep_on_internal() {
        let e = make_err!(Code::Internal, "peer hit an internal error");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "Internal does not prove the peer lost the blob"
        );
    }

    #[test]
    fn test_should_keep_on_aborted() {
        let e = make_err!(Code::Aborted, "peer aborted the stream");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "Aborted is transient — must keep locality entry"
        );
    }

    #[test]
    fn test_should_keep_on_unknown_transport_blip() {
        let e = make_err!(Code::Unknown, "h2 transport blip");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "Unknown / transport blip is handled by connection drop, \
             not locality eviction"
        );
    }

    #[test]
    fn test_should_keep_on_cancelled() {
        let e = make_err!(Code::Cancelled, "client cancelled the rpc");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "Cancelled is transient (caller dropped) — does not prove \
             the peer lost the blob"
        );
    }

    #[test]
    fn test_should_keep_on_failed_precondition() {
        let e = make_err!(Code::FailedPrecondition, "peer in unexpected state");
        assert!(
            !should_evict_locality_on_peer_error(&e),
            "FailedPrecondition is transient — does not prove the peer \
             lost the blob"
        );
    }

    /// Exhaustive table covering every `tonic::Code` variant in the
    /// canonical 0..=16 range. Any change to the helper that reclassifies
    /// an existing variant fails this test, so a future maintainer who
    /// widens eviction to a transient code without updating the table
    /// will see it go red. Pair with the per-case tests above for
    /// prose-level intent on the high-traffic codes.
    #[test]
    fn test_locality_eviction_policy_for_all_grpc_codes() {
        // (code, expected_evict). Spec: evict ONLY on NotFound + DataLoss;
        // every other code (including Ok, which never actually appears as
        // an error but is included as a sanity row) must keep locality.
        let cases: &[(Code, bool)] = &[
            (Code::Ok, false),
            (Code::Cancelled, false),
            (Code::Unknown, false),
            (Code::InvalidArgument, false),
            (Code::DeadlineExceeded, false),
            (Code::NotFound, true),
            (Code::AlreadyExists, false),
            (Code::PermissionDenied, false),
            (Code::ResourceExhausted, false),
            (Code::FailedPrecondition, false),
            (Code::Aborted, false),
            (Code::OutOfRange, false),
            (Code::Unimplemented, false),
            (Code::Internal, false),
            (Code::Unavailable, false),
            (Code::DataLoss, true),
            (Code::Unauthenticated, false),
        ];
        for (code, expected) in cases {
            let e = make_err!(*code, "table-driven test for {:?}", code);
            let actual = should_evict_locality_on_peer_error(&e);
            assert_eq!(
                actual, *expected,
                "Code::{code:?}: expected evict={expected}, got evict={actual}"
            );
        }
    }

    /// Exhaustive table for `should_try_peers`. The architectural invariant
    /// is "if the local chain failed to serve, ALWAYS consult peers before
    /// giving up", so the predicate must include every code that means
    /// "blob not served by local chain": NotFound, DataLoss, Internal,
    /// OutOfRange, Unavailable, Unknown, ResourceExhausted. Every other
    /// code (Cancelled/DeadlineExceeded — caller gone; Aborted — AC ABA;
    /// authn/authz; client-side validation; redirect protocol) MUST return
    /// false so the inner store's error reaches the caller untouched.
    #[test]
    fn test_should_try_peers_for_all_grpc_codes() {
        let cases: &[(Code, bool)] = &[
            (Code::Ok, false),
            (Code::Cancelled, false),
            (Code::Unknown, true),
            (Code::InvalidArgument, false),
            (Code::DeadlineExceeded, false),
            (Code::NotFound, true),
            (Code::AlreadyExists, false),
            (Code::PermissionDenied, false),
            (Code::ResourceExhausted, true),
            (Code::FailedPrecondition, false),
            (Code::Aborted, false),
            (Code::OutOfRange, true),
            (Code::Unimplemented, false),
            (Code::Internal, true),
            (Code::Unavailable, true),
            (Code::DataLoss, true),
            (Code::Unauthenticated, false),
        ];
        for (code, expected) in cases {
            let actual = should_try_peers(*code);
            assert_eq!(
                actual, *expected,
                "Code::{code:?}: expected try_peers={expected}, got try_peers={actual}"
            );
        }
    }

    #[test]
    fn test_is_definitive_unreachable_classifies_only_definitive_strings() {
        // ConnectionRefused / NetworkUnreachable / HostUnreachable on
        // Code::Unavailable must trigger fast quarantine.
        for msg in [
            "tcp connect error: ConnectionRefused (os error 111)",
            "NetworkUnreachable: no route to host",
            "HostUnreachable: target down",
        ] {
            let e = make_err!(Code::Unavailable, "{msg}");
            assert!(
                is_definitive_unreachable(&e),
                "expected definitive: {msg}"
            );
        }

        // Bare Code::Unavailable / Unknown / KeepAliveTimedOut /
        // close_notify / RST_STREAM must NOT fast-quarantine — those
        // legitimately recover on retry.
        for msg in [
            "transient unavailable",
            "KeepAliveTimedOut",
            "EOF without close_notify",
            "RST_STREAM received",
        ] {
            let e = make_err!(Code::Unavailable, "{msg}");
            assert!(
                !is_definitive_unreachable(&e),
                "did not expect definitive: {msg}"
            );
        }

        // Wrong code class must not match even with definitive substring.
        let e = make_err!(Code::NotFound, "ConnectionRefused but wrong code");
        assert!(!is_definitive_unreachable(&e));
    }

    // ---------------------------------------------------------------
    // Review #4: ResourceExhausted classifies as Saturated, NOT as a
    // quarantine-eligible failure. A peer that fills up first must not
    // get pulled out of rotation as if it were broken.
    // ---------------------------------------------------------------
    #[test]
    fn test_classify_mirror_failure_resource_exhausted_is_saturated() {
        let e = make_err!(Code::ResourceExhausted, "mirror cap exceeded");
        assert_eq!(classify_mirror_failure(&e), MirrorFailureKind::Saturated);
    }

    #[test]
    fn test_classify_mirror_failure_definitive_takes_precedence_over_generic() {
        let e = make_err!(
            Code::Unavailable,
            "tcp connect error: ConnectionRefused (os error 111)"
        );
        assert_eq!(
            classify_mirror_failure(&e),
            MirrorFailureKind::DefinitiveUnreachable
        );
    }

    #[test]
    fn test_classify_mirror_failure_other_codes_are_generic() {
        for code in [
            Code::Unknown,
            Code::Unavailable, // bare, no transport substring
            Code::Internal,
            Code::DeadlineExceeded,
        ] {
            let e = make_err!(code, "generic transient");
            assert_eq!(
                classify_mirror_failure(&e),
                MirrorFailureKind::Generic,
                "unexpected classification for {code:?}"
            );
        }
    }

    // ---------------------------------------------------------------
    // Review #1: capacity pre-check filters out peers that cannot fit
    // the next mirror write before the source stream is consumed.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_capacity_aware_picker_filters_full_peers() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into()];

        // Initialize entries (the picker write-path inserts default state).
        let _ = proxy.pick_mirror_endpoint(&endpoints, None, 0);

        // 'a' has 100 bytes free, 'b' has 0 bytes free.
        proxy.record_mirror_capacity("a", 0, 100);
        proxy.record_mirror_capacity("b", 100, 100);

        // For a 10-byte write, picker MUST always pick 'a' (b is full).
        for _ in 0..50 {
            let (chosen, _) =
                proxy.pick_mirror_endpoint(&endpoints, None, 10).unwrap();
            assert_eq!(
                chosen.as_ref(),
                "a",
                "10-byte write must skip the full peer"
            );
        }

        // For size_bytes = 0 (unknown size), filter is disabled — both
        // peers are eligible.
        let mut saw_a = false;
        let mut saw_b = false;
        for _ in 0..50 {
            let (chosen, _) =
                proxy.pick_mirror_endpoint(&endpoints, None, 0).unwrap();
            match chosen.as_ref() {
                "a" => saw_a = true,
                "b" => saw_b = true,
                other => panic!("unexpected endpoint: {other}"),
            }
        }
        assert!(saw_a && saw_b, "size=0 must round-robin both peers");

        Ok(())
    }

    /// Capacity is unknown for a peer that has never reported (older
    /// worker). The picker treats unknown capacity as "fits" so we
    /// don't filter out workers we have no information about.
    #[nativelink_test]
    async fn test_unknown_capacity_treated_as_fits() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into()];

        // No record_mirror_capacity call — capacity stays unknown.
        let pick = proxy.pick_mirror_endpoint(&endpoints, None, 1_000_000);
        assert!(
            pick.is_some(),
            "unknown capacity must not block the picker"
        );
        assert_eq!(pick.unwrap().0.as_ref(), "a");
        Ok(())
    }

    /// Degraded mode: every peer is over capacity. The picker still
    /// returns SOMETHING rather than dropping the write — the
    /// downstream Saturated Err is the safety net.
    #[nativelink_test]
    async fn test_all_peers_full_falls_back_to_full_set() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into()];

        let _ = proxy.pick_mirror_endpoint(&endpoints, None, 0);
        proxy.record_mirror_capacity("a", 100, 100);
        proxy.record_mirror_capacity("b", 100, 100);

        let pick = proxy.pick_mirror_endpoint(&endpoints, None, 50);
        assert!(
            pick.is_some(),
            "all-full case must still return a peer (degraded > nothing)"
        );
        Ok(())
    }

    /// Boundary check (review #13): `used + size_bytes == max` must
    /// return true. Guards against an off-by-one regression where the
    /// `<=` check were tightened to `<` (which would silently reject
    /// the very last byte of capacity and shunt traffic to a
    /// less-saturated peer for no reason).
    ///
    /// The test uses TWO peers — "a" exactly at boundary, "b" with
    /// plenty of room — and asserts the picker round-robins both. With
    /// the mutation (`<` instead of `<=`), "a" would be excluded from
    /// the eligible pool and the picker would always return "b".
    #[nativelink_test]
    async fn test_fits_at_exact_boundary_returns_true() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into()];

        // Force state entries to exist before we record capacity.
        let _ = proxy.pick_mirror_endpoint(&endpoints, None, 0);
        // "a" has exactly 900 bytes free; "b" has 100_000.
        proxy.record_mirror_capacity("a", 100, 1000);
        proxy.record_mirror_capacity("b", 0, 100_000);

        // For a 900-byte write (exactly fills "a"), both must be eligible
        // — round-robin should hit "a" at least once.
        let mut saw_a = false;
        let mut saw_b = false;
        for _ in 0..200 {
            let (chosen, _) = proxy
                .pick_mirror_endpoint(&endpoints, None, 900)
                .expect("at least one peer eligible");
            match chosen.as_ref() {
                "a" => saw_a = true,
                "b" => saw_b = true,
                other => panic!("unexpected endpoint: {other}"),
            }
            if saw_a && saw_b {
                break;
            }
        }
        assert!(
            saw_a,
            "fits at exact capacity boundary (used + size == max) must \
             include the boundary peer in the eligible pool — pre-fix \
             `<` instead of `<=` would exclude 'a' and only return 'b'"
        );
        assert!(saw_b, "non-boundary peer must also be eligible");

        Ok(())
    }

    // ---------------------------------------------------------------
    // Review #4: a stream of Saturated failures must NOT quarantine the
    // endpoint, regardless of count — saturation is not evidence of a
    // broken peer. With the pre-fix bool API every failure (including
    // the cap-exceeded Err returned by `insert_mirror_blob`) bumped the
    // streak; this would quarantine a healthy peer that just filled up.
    //
    // We assert the underlying state rather than just the picker outcome
    // — `pick_mirror_endpoint` falls back to the full set when every
    // endpoint is quarantined (degraded > nothing), so a single-endpoint
    // pool would still return `a` even if it were quarantined.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_saturated_failures_do_not_quarantine() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into()];

        // First, quarantine "b" so the picker has a real preference path.
        for _ in 0..MIRROR_FAILURE_THRESHOLD {
            proxy.record_mirror_failure("b", MirrorFailureKind::Generic);
        }

        // Hammer "a" with saturated failures.
        for _ in 0..(MIRROR_FAILURE_THRESHOLD * 4) {
            proxy.record_mirror_failure("a", MirrorFailureKind::Saturated);
        }

        // Direct state check: "a" must NOT be quarantined and must NOT
        // have accumulated any consecutive_failures.
        {
            let st = proxy.mirror_state.read();
            match st.get("a") {
                Some(entry) => {
                    assert!(
                        entry.quarantined_until.is_none(),
                        "saturated peer must not be quarantined; quarantined_until={:?}",
                        entry.quarantined_until
                    );
                    assert_eq!(
                        entry.consecutive_failures, 0,
                        "saturated failures must not bump consecutive_failures, got {}",
                        entry.consecutive_failures
                    );
                }
                None => {
                    // Either no entry was ever inserted (also acceptable —
                    // proves we did not perturb the streak) or the impl
                    // chose to record under a different key. Both are
                    // fine for this assertion's intent.
                }
            }
        }

        // Picker must prefer "a" (the only non-quarantined eligible peer).
        let (chosen, _) = proxy
            .pick_mirror_endpoint(&endpoints, None, 0)
            .expect("at least one eligible endpoint");
        assert_eq!(
            chosen.as_ref(),
            "a",
            "saturated 'a' must be preferred over quarantined 'b'"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 1. Inner store hit returns data without consulting locality map.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_inner_store_hit_skips_locality() -> Result<(), Error> {
        let (store, locality_map) = make_proxy_store();

        let value = b"hello world";
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Write the blob into the inner store via the proxy.
        store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;

        // Register a fake worker in the locality map so we can verify
        // it is NOT contacted when the inner store already has the blob.
        locality_map
            .write()
            .register_blobs("fake-worker:50081", &[digest]);

        // Read the blob back — should succeed from the inner store.
        let result = store
            .get_part_unchunked(digest, 0, None)
            .await?;
        assert_eq!(result.as_ref(), value);

        Ok(())
    }

    // ---------------------------------------------------------------
    // 2. Inner store miss + empty locality map => NotFound.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_inner_store_miss_no_peers_returns_not_found() -> Result<(), Error> {
        let (store, _locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // The inner store is empty and the locality map has no entries.
        let result = store.get_part_unchunked(digest, 0, None).await;

        assert!(result.is_err(), "Expected NotFound error");
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::NotFound,
            "Expected NotFound code, got: {err:?}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 3. Inner store miss + locality has peers but no gRPC connections
    //    => falls through gracefully and returns NotFound.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_inner_store_miss_locality_has_peers_but_no_connections()
        -> Result<(), Error>
    {
        let (store, locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Use an invalid URI that fails during GrpcStore::new(). The
        // space character is illegal in URIs, so Uri::try_from() fails
        // and create_worker_connection returns Err. try_read_from_worker
        // will `continue` past this endpoint and return Ok(false),
        // resulting in the final NotFound error.
        locality_map
            .write()
            .register_blobs("not a valid uri", &[digest]);

        let result = store.get_part_unchunked(digest, 0, None).await;

        assert!(result.is_err(), "Expected NotFound error");
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::NotFound,
            "Expected NotFound, got: {err:?}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 4. has_with_results: locality fallback ON (default) reports
    //     worker-only blobs as present (canonical size from digest).
    //     Required for end-to-end coherence: the bytestream short-
    //     circuit returns success without storing on the server, so
    //     the next FMB must agree the blob is present or Bazel
    //     re-uploads it.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_has_with_results_locality_fallback_when_enabled() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner.clone(), locality_map.clone());
        proxy.enable_locality_in_has();
        let store = Store::new(proxy);

        let d_inner = DigestInfo::try_new(VALID_HASH1, 9)?;
        let d_worker_only = DigestInfo::try_new(VALID_HASH2, 999)?;

        store
            .update_oneshot(d_inner, Bytes::from_static(b"test data"))
            .await?;
        locality_map
            .write()
            .register_blobs("worker-a:50081", &[d_worker_only]);

        let keys: Vec<StoreKey<'_>> = vec![d_inner.into(), d_worker_only.into()];
        let mut results = vec![None; 2];
        store.has_with_results(&keys, &mut results).await?;

        assert_eq!(results[0], Some(9));
        assert_eq!(
            results[1],
            Some(999),
            "locality fallback should report worker-only blob as present with canonical size"
        );
        Ok(())
    }

    // ---------------------------------------------------------------
    // 4b. has_with_results: no locality entry => still None.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_has_with_results_no_locality_returns_none() -> Result<(), Error> {
        let (store, _locality_map) = make_proxy_store();

        let d1 = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Neither inner store nor locality map has d1.
        let keys: Vec<StoreKey<'_>> = vec![d1.into()];
        let mut results = vec![None; 1];
        store.has_with_results(&keys, &mut results).await?;

        assert_eq!(
            results[0], None,
            "d1 should not be found when absent from both inner store and locality map"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 5. update() passes through to inner store.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_update_passes_through() -> Result<(), Error> {
        let (store, _locality_map) = make_proxy_store();

        let value = b"upload me";
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Upload via the proxy store.
        store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;

        // Verify the blob is retrievable (proving it went into the inner store).
        let data = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(data.as_ref(), value);

        // Also verify via has().
        let size = store.has(digest).await?;
        assert_eq!(size, Some(value.len() as u64));

        Ok(())
    }

    // ---------------------------------------------------------------
    // 6. get_part with offset and length returns correct subset.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_get_part_with_offset_and_length() -> Result<(), Error> {
        let (store, _locality_map) = make_proxy_store();

        let value = b"0123456789abcdefghij"; // 20 bytes
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;

        // Read bytes [5..15) — 10 bytes starting at offset 5.
        let data = store
            .get_part_unchunked(digest, 5, Some(10))
            .await?;
        assert_eq!(
            data.as_ref(),
            b"56789abcde",
            "Expected subset at offset=5, length=10"
        );

        // Read from offset 15 to end (no length limit).
        let data = store.get_part_unchunked(digest, 15, None).await?;
        assert_eq!(
            data.as_ref(),
            b"fghij",
            "Expected tail from offset=15"
        );

        // Read 0 bytes from offset 0 with length 0.
        let data = store
            .get_part_unchunked(digest, 0, Some(0))
            .await?;
        assert_eq!(data.as_ref(), b"", "Expected empty result for length=0");

        Ok(())
    }

    // ---------------------------------------------------------------
    // 7. Redirect parsing: well-formed redirect error.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_redirect_well_formed() -> Result<(), Error> {
        let err = make_err!(
            Code::FailedPrecondition,
            "{REDIRECT_PREFIX}grpc://w1:50071,grpc://w2:50071|"
        );
        let msg = err.message_string();
        let start = msg.find(REDIRECT_PREFIX).expect("prefix missing");
        let endpoints_str = &msg[start + REDIRECT_PREFIX.len()..];
        let endpoints_str = endpoints_str.split('|').next().unwrap_or(endpoints_str);
        let endpoints: Vec<String> = endpoints_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0], "grpc://w1:50071");
        assert_eq!(endpoints[1], "grpc://w2:50071");
        Ok(())
    }

    // ---------------------------------------------------------------
    // 8. Redirect parsing: trailing noise after pipe is ignored.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_redirect_trailing_noise_after_pipe() -> Result<(), Error> {
        let err = make_err!(
            Code::FailedPrecondition,
            "{REDIRECT_PREFIX}grpc://w1:50071|some extra noise"
        );
        let msg = err.message_string();
        let start = msg.find(REDIRECT_PREFIX).expect("prefix missing");
        let endpoints_str = &msg[start + REDIRECT_PREFIX.len()..];
        let endpoints_str = endpoints_str.split('|').next().unwrap_or(endpoints_str);
        let endpoints: Vec<String> = endpoints_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0], "grpc://w1:50071");
        Ok(())
    }

    // ---------------------------------------------------------------
    // 9. Redirect parsing: empty segments filtered out.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_redirect_empty_segments_filtered() -> Result<(), Error> {
        let err = make_err!(
            Code::FailedPrecondition,
            "{REDIRECT_PREFIX}a,,b,|"
        );
        let msg = err.message_string();
        let start = msg.find(REDIRECT_PREFIX).expect("prefix missing");
        let endpoints_str = &msg[start + REDIRECT_PREFIX.len()..];
        let endpoints_str = endpoints_str.split('|').next().unwrap_or(endpoints_str);
        let endpoints: Vec<String> = endpoints_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        assert_eq!(endpoints, vec!["a", "b"]);
        Ok(())
    }

    // ---------------------------------------------------------------
    // 10. IS_WORKER_REQUEST=true with a peer in locality => redirect.
    //     Server returns `Code::FailedPrecondition` carrying
    //     `REDIRECT_PREFIX{peer-endpoint}|` so the worker fetches the
    //     blob directly from peers. Loop safety: see comment in
    //     `get_part_sequential` near the redirect-generation site.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_worker_request_returns_redirect_with_peer() -> Result<(), Error> {
        let (store, locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
        let peer_endpoint = "grpc://peer-worker:50071";

        locality_map
            .write()
            .register_blobs(peer_endpoint, &[digest]);

        let result = IS_WORKER_REQUEST
            .scope(true, store.get_part_unchunked(digest, 0, None))
            .await;

        assert!(result.is_err(), "Expected redirect error");
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::FailedPrecondition,
            "Worker request with a peer in locality should get FailedPrecondition redirect, got: {err:?}"
        );
        let msg = err.message_string();
        assert!(
            msg.contains(REDIRECT_PREFIX),
            "Worker redirect message should contain REDIRECT_PREFIX: {msg}"
        );
        assert!(
            msg.contains(peer_endpoint),
            "Worker redirect message should contain peer endpoint: {msg}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 10b. IS_WORKER_REQUEST=true with NO peer in locality => NotFound.
    //      Confirms the redirect-or-NotFound branch falls back to the
    //      NotFound path when locality has nothing to offer.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_worker_request_with_no_peers_gets_not_found() -> Result<(), Error> {
        let (store, _locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        let result = IS_WORKER_REQUEST
            .scope(true, store.get_part_unchunked(digest, 0, None))
            .await;

        assert!(result.is_err(), "Expected NotFound error");
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::NotFound,
            "Worker request with no peers should get NotFound, got: {err:?}"
        );
        let msg = err.message_string();
        assert!(
            !msg.contains(REDIRECT_PREFIX),
            "NotFound path must NOT contain redirect prefix: {msg}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 10c. IS_WORKER_REQUEST=true with MULTIPLE peers => redirect lists ALL.
    //      Mutation guard: if the implementation only includes the first
    //      peer (e.g. via `.next()` instead of `.collect()`), this test
    //      goes red.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_worker_request_redirect_includes_all_peers() -> Result<(), Error> {
        let (store, locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
        let endpoints = [
            "grpc://peer-a:50071",
            "grpc://peer-b:50071",
            "grpc://peer-c:50071",
        ];

        for ep in endpoints {
            locality_map
                .write()
                .register_blobs(ep, &[digest]);
        }

        let result = IS_WORKER_REQUEST
            .scope(true, store.get_part_unchunked(digest, 0, None))
            .await;

        let err = result.expect_err("Expected redirect error");
        assert_eq!(err.code, Code::FailedPrecondition);
        let msg = err.message_string();
        for ep in endpoints {
            assert!(
                msg.contains(ep),
                "Redirect should include peer endpoint {ep}: got {msg}"
            );
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // 10d. Worker-side responder mode (race_peers=true) MUST NOT generate
    //      a redirect on incoming external Reads — only servers can
    //      redirect. The worker gets the same is_worker=true signal when
    //      its bytestream_server receives a remote Read, but its job is
    //      to serve from local stores or return NotFound. NEVER chain.
    //      This is the loop-terminator invariant.
    //
    //      Mutation guard: removing the `if self.race_peers.load(...)`
    //      gate (i.e. the worker generates a redirect like a server)
    //      makes this test go red.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_worker_responder_mode_returns_notfound_no_redirect() -> Result<(), Error> {
        // Build the proxy by hand so we can flip race_peers BEFORE
        // wrapping in a Store. `enable_race_peers` requires the
        // Arc<WorkerProxyStore>, not the trait-object Store.
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
        proxy_arc.enable_race_peers();
        let store = Store::new(proxy_arc);

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
        let peer_endpoint = "grpc://peer-worker:40081";

        // Locality_map HAS a peer for this digest. On the SERVER side
        // this would generate a redirect. On the WORKER side
        // (race_peers=true), the worker is in RESPONDER mode and MUST
        // return NotFound without any external RPC or redirect.
        locality_map.write().register_blobs(peer_endpoint, &[digest]);

        let result = IS_WORKER_REQUEST
            .scope(true, store.get_part_unchunked(digest, 0, None))
            .await;

        let err = result.expect_err("Expected NotFound, not Ok or redirect");
        assert_eq!(
            err.code,
            Code::NotFound,
            "Worker-side responder MUST return NotFound, not redirect or other code; \
             chaining responders into external RPCs would form loops. Got: {err:?}"
        );
        let msg = err.message_string();
        assert!(
            !msg.contains(REDIRECT_PREFIX),
            "Worker-side responder MUST NOT generate REDIRECT_PREFIX (only servers redirect). \
             Got msg: {msg}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 11. IS_WORKER_REQUEST=false gets NotFound (no proxy to invalid peer).
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_non_worker_request_gets_not_found() -> Result<(), Error> {
        let (store, locality_map) = make_proxy_store();

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Use an invalid URI so the proxy attempt fails gracefully.
        locality_map
            .write()
            .register_blobs("not a valid uri", &[digest]);

        let result = IS_WORKER_REQUEST
            .scope(false, store.get_part_unchunked(digest, 0, None))
            .await;

        assert!(result.is_err(), "Expected NotFound error");
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::NotFound,
            "Non-worker should get NotFound, got: {err:?}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 12. optimized_for(LazyExistenceOnSync) returns true.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_optimized_for_lazy_existence() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);

        assert!(
            StoreDriver::optimized_for(&*proxy, StoreOptimizations::LazyExistenceOnSync),
            "WorkerProxyStore should report LazyExistenceOnSync"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 13. optimized_for(other) delegates to inner store.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_optimized_for_other_delegates_to_inner() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);

        assert!(
            !StoreDriver::optimized_for(&*proxy, StoreOptimizations::NoopUpdates),
            "Should delegate non-LazyExistence optimizations to inner store"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 14. Race: inner store has blob, peer registered — server wins race.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_race_server_wins_when_inner_has_blob() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let mut proxy = WorkerProxyStore::new(inner.clone(), locality_map.clone());
        proxy.enable_race_peers();
        let store = Store::new(proxy.clone());

        let value = b"race test data";
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Put blob in inner store.
        inner
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;

        // Inject a peer that also has the blob (MemoryStore with same data).
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;
        proxy.inject_worker_connection("grpc://peer:50071", peer_store);

        locality_map
            .write()
            .register_blobs("grpc://peer:50071", &[digest]);

        // NOT in IS_WORKER_REQUEST scope, so racing path is taken.
        let result = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(result.as_ref(), value);

        Ok(())
    }

    // ---------------------------------------------------------------
    // 15. Race: inner store miss, peer has blob — peer wins race.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_race_peer_wins_when_inner_misses() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let mut proxy = WorkerProxyStore::new(inner, locality_map.clone());
        proxy.enable_race_peers();
        let store = Store::new(proxy.clone());

        let value = b"peer only data";
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        // Inner store is empty. Peer has the blob.
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;
        proxy.inject_worker_connection("grpc://peer:50071", peer_store);

        locality_map
            .write()
            .register_blobs("grpc://peer:50071", &[digest]);

        let result = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(result.as_ref(), value);

        Ok(())
    }

    // ---------------------------------------------------------------
    // #58 (2026-06-07 mutation stamp): peer-fetch NotFound counter
    //
    // Production composition: server-side WorkerProxyStore (race_peers=OFF,
    // non-IS_WORKER_REQUEST caller). Inner store empty, peer registered in
    // locality but its MemoryStore is empty so peer.get_part returns
    // NotFound. The non-derivative error! branch at the `:1650-style` site
    // (now `WorkerProxyStore: peer fetch failed` with writer still open)
    // must fire and increment `worker_proxy_peer_fetch_notfound_total`.
    //
    // Per #35 OQ-8 preflight (`.claude/audits/35-phase5-blobs-available-
    // design-2026-06-04.md`): production 4 events / 7d, all
    // `code: NotFound` + `evicted_locality: true`. Counter lets future
    // preflights skip journal grep.
    //
    // Mutation guard: comment out the `self.worker_proxy_peer_fetch_
    // notfound_total.inc();` line; this test red-fails with
    //   "counter must increment when peer-fetch error site fires".
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_peer_fetch_notfound_counter_increments_on_error_site()
        -> Result<(), Error>
    {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc =
            WorkerProxyStore::new(inner, locality_map.clone());
        // race_peers OFF (server-side); IS_WORKER_REQUEST is false by default.
        let store = Store::new(proxy_arc.clone());

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Empty peer MemoryStore: peer.get_part will return NotFound,
        // taking the non-derivative error! branch in try_read_from_worker.
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        proxy_arc
            .inject_worker_connection("grpc://peer:50071", peer_store);
        locality_map
            .write()
            .register_blobs("grpc://peer:50071", &[digest]);

        let before = proxy_arc
            .worker_proxy_peer_fetch_notfound_total
            .counter
            .load(Ordering::Relaxed);
        assert_eq!(before, 0, "counter must start at zero");

        // Bound the call with a deadlock detector; if the writer-
        // termination contract regresses, this surfaces as Elapsed
        // rather than a hang.
        let result = tokio::time::timeout(
            core::time::Duration::from_secs(5),
            store.get_part_unchunked(digest, 0, None),
        )
        .await
        .expect("must not deadlock — get_part_unchunked completes within 5s");

        assert!(
            result.is_err(),
            "expected NotFound when inner+peer both miss"
        );

        let after = proxy_arc
            .worker_proxy_peer_fetch_notfound_total
            .counter
            .load(Ordering::Relaxed);
        assert!(
            after >= 1,
            "counter must increment when peer-fetch error site fires; got {after}"
        );

        Ok(())
    }

    // ---------------------------------------------------------------
    // 16. Race: both inner and peer miss — returns error.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_race_both_miss_returns_error() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let mut proxy = WorkerProxyStore::new(inner, locality_map.clone());
        proxy.enable_race_peers();
        let store = Store::new(proxy.clone());

        let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

        // Both inner and peer are empty.
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        proxy.inject_worker_connection("grpc://peer:50071", peer_store);

        locality_map
            .write()
            .register_blobs("grpc://peer:50071", &[digest]);

        let result = store.get_part_unchunked(digest, 0, None).await;
        assert!(result.is_err(), "Expected error when both miss");

        Ok(())
    }

    // ---------------------------------------------------------------
    // 17. Quarantine: 5 consecutive failures within window quarantine
    //     the endpoint, after which pick_mirror_endpoint skips it.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_quarantine_after_threshold_failures() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> =
            vec!["a".into(), "b".into(), "c".into()];

        // Drive endpoint "a" past the failure threshold.
        for _ in 0..MIRROR_FAILURE_THRESHOLD {
            proxy.record_mirror_failure("a", MirrorFailureKind::Generic);
        }

        // pick_mirror_endpoint must skip "a" while it's quarantined.
        for _ in 0..20 {
            let (chosen, _) = proxy.pick_mirror_endpoint(&endpoints, None, 0).unwrap();
            assert_ne!(
                chosen.as_ref(),
                "a",
                "quarantined endpoint should be skipped"
            );
        }

        Ok(())
    }

    // ---------------------------------------------------------------
    // 18. Quarantine: success below threshold resets the streak so the
    //     endpoint stays eligible.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_success_resets_failure_streak() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into()];

        // Accumulate failures, then succeed before crossing the threshold.
        for _ in 0..(MIRROR_FAILURE_THRESHOLD - 1) {
            proxy.record_mirror_failure("a", MirrorFailureKind::Generic);
        }
        proxy.record_mirror_success("a");

        // One more failure must NOT trigger quarantine because the streak
        // was cleared.
        proxy.record_mirror_failure("a", MirrorFailureKind::Generic);
        let (chosen, _) = proxy
            .pick_mirror_endpoint(&endpoints, None, 0)
            .expect("endpoint should be eligible");
        assert_eq!(chosen.as_ref(), "a");

        Ok(())
    }

    // ---------------------------------------------------------------
    // 19. Quarantine: when every endpoint is quarantined, fall back to
    //     the full set rather than returning None (degraded > nothing).
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_all_quarantined_falls_back_to_full_set() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into()];

        for ep in ["a", "b"] {
            for _ in 0..MIRROR_FAILURE_THRESHOLD {
                proxy.record_mirror_failure(ep, MirrorFailureKind::Generic);
            }
        }

        let pick = proxy.pick_mirror_endpoint(&endpoints, None, 0);
        assert!(pick.is_some(), "should fall back to full set when all quarantined");

        Ok(())
    }

    // ---------------------------------------------------------------
    // 20. Exclude argument: pick_mirror_endpoint never returns the
    //     excluded endpoint (used by the retry path).
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_exclude_endpoint_for_retry() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["a".into(), "b".into(), "c".into()];

        for _ in 0..50 {
            let (chosen, _) = proxy
                .pick_mirror_endpoint(&endpoints, Some("a"), 0)
                .expect("eligible endpoints exist");
            assert_ne!(chosen.as_ref(), "a", "excluded endpoint must be skipped");
        }

        Ok(())
    }

    // ---------------------------------------------------------------
    // 21. Per-worker permits: pick_mirror_endpoint returns the same
    //     Semaphore Arc for repeated picks of the same endpoint.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_per_worker_permits_are_shared_across_picks() -> Result<(), Error> {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy = WorkerProxyStore::new(inner, locality_map);
        let endpoints: Vec<Arc<str>> = vec!["only".into()];

        let (_, sem1) = proxy.pick_mirror_endpoint(&endpoints, None, 0).unwrap();
        let (_, sem2) = proxy.pick_mirror_endpoint(&endpoints, None, 0).unwrap();
        assert!(
            Arc::ptr_eq(&sem1, &sem2),
            "permit semaphore must be shared across picks"
        );
        assert_eq!(sem1.available_permits(), MIRROR_PERMITS_PER_WORKER);

        Ok(())
    }

    // ---------------------------------------------------------------
    // B3 regression (perf-optimizer): the batched fast-path inside
    // `try_read_from_endpoints` MUST be skipped on iterations after
    // the FIRST when a previous endpoint's streaming attempt wrote
    // partial bytes before erroring. Without the
    // `bytes_before_proxy == writer.get_bytes_written()` guard, the
    // batched whole-blob send to endpoint B fires on top of A's
    // partial bytes — silent data corruption.
    //
    // The test:
    // - Endpoint A: a fake peer that writes some bytes via streaming
    //   `get_part`, THEN errors. This forces the loop's first
    //   iteration to grow `writer.get_bytes_written()` past
    //   `bytes_before_proxy`.
    // - Endpoint B: a healthy MemoryStore with the full blob bytes.
    //   On loop iteration 2, the batched-path eligibility check
    //   sees `writer.get_bytes_written() != bytes_before_proxy` and
    //   MUST skip the batched fast path.
    //
    // We assert `coalescer.batches_dispatched() == 0` — without the
    // guard, B's batched path would fire and the counter would be 1.
    //
    // Mutation step: remove the
    // `&& writer.get_bytes_written() == bytes_before_proxy`
    // condition from the eligibility check at
    // `try_read_from_endpoints` and verify this test fails.
    // ---------------------------------------------------------------

    /// Test fixture: writes a partial chunk to the caller's writer,
    /// then returns Err. Mirrors a real peer that started streaming
    /// then died mid-stream.
    #[derive(MetricsComponent)]
    struct PartialThenErrorPeer {
        partial: Bytes,
    }

    #[async_trait]
    impl StoreDriver for PartialThenErrorPeer {
        async fn has_with_results(
            self: Pin<&Self>,
            _keys: &[StoreKey<'_>],
            _results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            _rx: DropCloserReadHalf,
            _size: UploadSizeInfo,
        ) -> Result<(), Error> {
            Err(make_err!(Code::Unimplemented, "test fixture: no update"))
        }

        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            // Write some bytes (so writer.get_bytes_written() grows
            // past bytes_before_proxy), then return an Err that the
            // outer try_read_from_endpoints treats as "try next
            // endpoint".
            writer.send(self.partial.clone()).await?;
            Err(make_err!(
                Code::Internal,
                "PartialThenErrorPeer: simulated mid-stream peer death"
            ))
        }

        fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
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
            Err(make_err!(Code::Unimplemented, "no callbacks"))
        }
        fn stable_delegation(&self) -> StableDigestDelegation<'_> {
            StableDigestDelegation::Leaf
        }
        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Leaf
        }
        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Leaf
        }
    }

    #[async_trait]
    impl HealthStatusIndicator for PartialThenErrorPeer {
        fn get_name(&self) -> &'static str {
            "PartialThenErrorPeer"
        }
        async fn check_health(
            &self,
            namespace: std::borrow::Cow<'static, str>,
        ) -> HealthStatus {
            StoreDriver::check_health(Pin::new(self), namespace).await
        }
    }

    #[nativelink_test]
    async fn redirect_path_skips_batched_after_partial_write_to_writer()
    -> Result<(), Error> {
        use nativelink_util::buf_channel::make_buf_channel_pair;

        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map);
        proxy_arc.init_batch_read_coalescer();
        proxy_arc.enable_batch_small_blob_reads();

        // Endpoint A: a peer that writes 5 bytes then errors.
        let peer_a = Store::new(Arc::new(PartialThenErrorPeer {
            partial: Bytes::from_static(b"abcde"),
        }));
        let endpoint_a = "grpc://peer-a-partial:50081";
        proxy_arc.inject_worker_connection(endpoint_a, peer_a);

        // Endpoint B: healthy MemoryStore with the full blob.
        let peer_b = Store::new(MemoryStore::new(&MemorySpec::default()));
        let value_b =
            Bytes::from_static(b"endpoint-B whole-blob bytes (must NOT be batched)");
        let digest = DigestInfo::try_new(VALID_HASH1, value_b.len() as u64)?;
        peer_b.update_oneshot(digest, value_b.clone()).await?;
        let endpoint_b = "grpc://peer-b-healthy:50081";
        proxy_arc.inject_worker_connection(endpoint_b, peer_b);

        // Build a writer + spawn a reader to drain so backpressure
        // doesn't block the producer.
        let (mut writer, mut reader) = make_buf_channel_pair();
        let drain = tokio::spawn(async move {
            let mut total = 0usize;
            while let Ok(chunk) = reader.recv().await {
                if chunk.is_empty() {
                    break;
                }
                total += chunk.len();
            }
            total
        });

        let endpoints = vec![endpoint_a.to_string(), endpoint_b.to_string()];
        let _ = proxy_arc
            .try_read_from_endpoints(
                digest.into(),
                &mut writer,
                0,
                None,
                &endpoints,
            )
            .await;

        drop(writer);
        let _bytes_drained = drain.await.expect("drain task must finish");

        // B3: at most 1 batched RPC may dispatch (the one for endpoint
        // A's first attempt — which itself fails). Endpoint A's
        // streaming-fallback writes partial bytes BEFORE the loop
        // advances to endpoint B. With the guard,
        // `writer.get_bytes_written() != bytes_before_proxy` on
        // iteration B → batched path is skipped → counter stays at 1.
        // Without the guard, B's batched RPC would also fire (sending
        // the WHOLE blob) on top of A's partial bytes — corruption,
        // and the counter would be 2.
        let coalescer = proxy_arc
            .coalescer_handle()
            .expect("coalescer must be initialized for this test");
        let dispatched = coalescer.batches_dispatched();
        assert!(
            dispatched <= 1,
            "B3 regression: at most 1 batched RPC may dispatch \
             (endpoint A's first try); endpoint B's batched path MUST \
             be skipped because the writer already has partial bytes \
             from A's streaming-fallback. Got {dispatched} batched \
             dispatches. Mutate by removing the \
             `writer.get_bytes_written() == bytes_before_proxy` guard \
             at try_read_from_endpoints to verify this assertion fires."
        );
        Ok(())
    }

    // ---------------------------------------------------------------
    // #67: read-side circuit breaker for persistently-failing peers.
    //
    // The mirror-write picker filters quarantined endpoints (
    // `pick_mirror_endpoint_*` paths). The read-side peer-fetch
    // selector at `get_part` SHOULD do the same — otherwise a
    // persistently-failing peer is hammered on every same-digest read
    // until reactive eviction kicks in.
    //
    // Production composition: real `WorkerProxyStore`, `race_peers=true`,
    // two registered peers (one quarantined, one healthy). Both peers
    // claim the digest (locality_map). The filter MUST drop the
    // quarantined peer so the healthy peer is the one consulted.
    //
    // Counting fake peer: `peer_quarantined` is a MemoryStore holding
    // the digest with poisoned bytes. If the filter is removed, the
    // race selector picks `peers[0]` (the quarantined one) and returns
    // poisoned bytes; the assertion catches the mismatch. The healthy
    // peer holds the correct bytes; with the filter applied it is
    // consulted and returns correct bytes.
    //
    // Mutation guard: removing the `filter_quarantined_peers` call in
    // `get_part` (line ~3022) must produce the bespoke message via the
    // returned-bytes mismatch.
    // ---------------------------------------------------------------
    #[nativelink_test]
    async fn test_67_read_side_filters_quarantined_peer() -> Result<(), Error> {
        // Inner empty — force the race path to consult a peer.
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let locality_map = new_shared_blob_locality_map();
        let mut proxy = WorkerProxyStore::new(inner, locality_map.clone());
        proxy.enable_race_peers();
        let store = Store::new(proxy.clone());

        let healthy_value = b"healthy-peer-bytes";
        let poisoned_value = b"quarantined-peer-bytes-must-not-appear";
        // Same logical key — use the healthy size to match what the
        // healthy peer stores. The quarantined peer's value differs in
        // length, so without the filter the race path will instead try
        // to satisfy a 18-byte read from a peer holding a different-
        // length blob — the bytes returned would diverge from the
        // expected `healthy_value`.
        let digest = DigestInfo::try_new(VALID_HASH1, healthy_value.len() as u64)?;

        // Quarantined peer: fill with poisoned data of *matching* size
        // so it WOULD return bytes if consulted.
        let peer_quarantined =
            Store::new(MemoryStore::new(&MemorySpec::default()));
        let poisoned_same_size: Vec<u8> = poisoned_value
            .iter()
            .copied()
            .cycle()
            .take(healthy_value.len())
            .collect();
        peer_quarantined
            .update_oneshot(digest, Bytes::from(poisoned_same_size.clone()))
            .await?;
        proxy.inject_worker_connection(
            "grpc://peer-quarantined:50071",
            peer_quarantined,
        );

        // Healthy peer: holds the correct bytes.
        let peer_healthy = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_healthy
            .update_oneshot(digest, Bytes::from_static(healthy_value))
            .await?;
        proxy.inject_worker_connection(
            "grpc://peer-healthy:50071",
            peer_healthy,
        );

        // Register quarantined first (so it appears at peers[0] in
        // insertion order); then healthy. Both claim the digest.
        locality_map.write().register_blobs(
            "grpc://peer-quarantined:50071",
            &[digest],
        );
        locality_map.write().register_blobs(
            "grpc://peer-healthy:50071",
            &[digest],
        );

        // Quarantine the bad peer by driving it past the failure
        // threshold. This populates `mirror_state` exactly as a real
        // mirror-write would.
        for _ in 0..MIRROR_FAILURE_THRESHOLD {
            proxy.record_mirror_failure(
                "grpc://peer-quarantined:50071",
                MirrorFailureKind::Generic,
            );
        }

        // Wrap in a deadlock-detector timeout. The race path itself is
        // bounded, but if the filter selects an empty peer set and the
        // sequential fallback hangs, the timeout will surface the bug
        // with a deterministic message instead of hanging the runner.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.get_part_unchunked(digest, 0, None),
        )
        .await
        .expect(
            "must filter quarantined peers — #67 read-side circuit \
             breaker (timeout indicates the filter routed to a dead \
             peer or dropped both)",
        )?;

        assert_eq!(
            result.as_ref(),
            healthy_value,
            "must filter quarantined peers — #67 read-side circuit \
             breaker (got bytes from quarantined peer instead of \
             healthy peer)"
        );

        Ok(())
    }
}
