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
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, WriteHalfGuard, make_buf_channel_pair,
};
use nativelink_util::common::{DigestInfo, make_precondition_failure_any};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    IS_MIRROR_REQUEST, IS_WORKER_REQUEST, ItemCallback, REDIRECT_PREFIX, Store, StoreDriver,
    StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};

use crate::grpc_store::GrpcStore;

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
        })
    }

    /// Enable racing peer fetches against server fetches.
    /// Only workers should call this — servers should leave it disabled.
    pub fn enable_race_peers(&self) {
        self.race_peers.store(true, Ordering::Relaxed);
    }

    /// Enable the locality-aware fast paths in `has_with_results` and the
    /// bytestream_write fast-path. On by default; this is the kill-switch
    /// re-arm.
    pub fn enable_locality_in_has(&self) {
        self.consult_locality_in_has.store(true, Ordering::Relaxed);
    }

    /// Disable the locality-aware fast paths. Operator kill-switch — flips
    /// `has_with_results` back to inner-store-only and disables the
    /// bytestream_write sync-confirm fast path.
    pub fn disable_locality_in_has(&self) {
        self.consult_locality_in_has.store(false, Ordering::Relaxed);
    }

    /// Inspector for the bytestream fast-path; returns whether locality
    /// consultation is enabled.
    pub fn locality_in_has_enabled(&self) -> bool {
        self.consult_locality_in_has.load(Ordering::Relaxed)
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
        };
        let store = GrpcStore::new(&spec)
            .await
            .err_tip(|| format!("Creating worker proxy connection to {endpoint}"))?;
        Ok(Store::new(store))
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

        for endpoint in endpoints {
            let Some(store) = self.get_or_create_connection(endpoint).await else {
                continue;
            };

            match self
                .get_part_and_cache(&store, key.borrow(), &mut *writer, offset, length)
                .await
            {
                Ok(()) => {
                    debug!(
                        ?digest,
                        endpoint = endpoint.as_str(),
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
                    error!(
                        ?digest,
                        endpoint = endpoint.as_str(),
                        code = ?e.code,
                        connection_error = is_conn_err,
                        evicted_locality = evict,
                        ?e,
                        "WorkerProxyStore: redirected peer fetch failed"
                    );
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

            // Stream from the peer, caching in the inner store when possible.
            // On failure, compute how many bytes were written and resume
            // from the next peer at the correct offset.
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
                    // peer. Treat 0-bytes-on-full-read as a peer failure:
                    // evict locality and try the next peer.
                    let bytes_written_this_peer =
                        writer.get_bytes_written() - bytes_before_proxy;
                    let expected_size = digest.size_bytes();
                    let was_full_read = current_offset == offset
                        && remaining_length == length
                        && length.is_none();
                    if bytes_written_this_peer == 0
                        && expected_size > 0
                        && was_full_read
                    {
                        warn!(
                            ?digest,
                            endpoint = %endpoint,
                            expected_size,
                            "WorkerProxyStore: peer returned Ok+0-bytes for \
                             non-zero digest — treating as stale-positive, \
                             evicting locality and trying next peer"
                        );
                        self.locality_map
                            .write()
                            .evict_blobs(endpoint, &[digest]);
                        continue;
                    }
                    info!(
                        ?digest,
                        endpoint = %endpoint,
                        bytes_written_this_peer,
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
                    error!(
                        ?digest,
                        endpoint = %endpoint,
                        code = ?e.code,
                        connection_error = is_conn_err,
                        evicted_locality = evict,
                        ?e,
                        "WorkerProxyStore: peer fetch failed"
                    );
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
    const MAX_CACHE_BLOB_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB

    /// Wrapper around a peer's `get_part` that tees the data to both the
    /// caller's writer and a background write to the inner store.
    ///
    /// For full-blob reads (offset=0, length=None) of blobs within the
    /// size limit, the data is collected during streaming and written to
    /// `self.inner` in a background task after success. For partial reads
    /// or oversized blobs, streams directly without caching.
    async fn get_part_and_cache(
        &self,
        peer_store: &Store,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Subordinate guard: WorkerProxyStore is a wrapper that may itself
        // be wrapped (e.g. as the slow store of a FastSlowStore on the
        // server CAS chain). Active Drop / fail would `send_error` on
        // Err and poison the outer wrapper's fall-through. The outer
        // wrapper's own active guard catches contract violations via
        // `commit_delegated_if_ok(&res)`.
        let mut guard = WriteHalfGuard::new_subordinate(writer);

        let digest = key.borrow().into_digest();

        // Only cache full-blob reads for blobs within the size limit.
        let should_cache = offset == 0
            && length.is_none()
            && digest.size_bytes() <= Self::MAX_CACHE_BLOB_SIZE;

        if !should_cache {
            // Loop-terminator propagation: set IS_WORKER_REQUEST=true on
            // peer→peer calls so the receiving worker enters responder
            // mode (race_peers + IS_WORKER_REQUEST gate at top of
            // `get_part`) and refuses to chain externally. This makes
            // the user's invariant "workers NEVER try to satisfy a read
            // from another host by issuing an external RPC" enforced
            // at depth 1, not just bounded at depth 2.
            let res = IS_WORKER_REQUEST
                .scope(true, peer_store.get_part(key, &mut *guard, offset, length))
                .await;
            guard.commit_delegated_if_ok(&res);
            return res;
        }

        // Create an intermediate channel so we can tee the data to both the
        // caller's writer and a concurrent inner store write.
        let (mut proxy_tx, mut proxy_rx) = make_buf_channel_pair();
        let (mut cache_tx, cache_rx) = make_buf_channel_pair();

        // Run the peer's get_part concurrently with forwarding, because the
        // buf_channel has limited capacity and the producer will block if
        // we don't consume data as it arrives. IS_WORKER_REQUEST=true
        // propagates to the peer (loop-terminator: peer enters responder
        // mode and won't chain).
        let owned_key = key.borrow().into_owned();
        let peer = peer_store.clone();
        let get_part_fut = async move {
            IS_WORKER_REQUEST
                .scope(
                    true,
                    peer.get_part(owned_key.borrow(), &mut proxy_tx, offset, length),
                )
                .await
        };

        // Start the inner store write concurrently. If the blob size is known
        // from the digest, use ExactSize; otherwise MaxSize.
        let inner = self.inner.clone();
        let cache_size = UploadSizeInfo::ExactSize(digest.size_bytes());
        let cache_key: StoreKey<'static> = digest.into();
        let cache_write_fut = async move {
            inner.update(cache_key, cache_rx, cache_size).await
        };

        let mut total_bytes: u64 = 0;
        // Reborrow the guard's writer for the forward_fut closure. The
        // subordinate guard's Drop is no-op so the outer wrapper's
        // fall-through (if any) is preserved when forward_fut returns Err.
        let writer_for_forward = &mut *guard;
        let forward_fut = async {
            loop {
                match proxy_rx.recv().await {
                    Ok(chunk) if chunk.is_empty() => {
                        writer_for_forward
                            .send_eof()
                            .err_tip(|| "get_part_and_cache: forwarding EOF")?;
                        cache_tx
                            .send_eof()
                            .err_tip(|| "get_part_and_cache: cache EOF")?;
                        break;
                    }
                    Ok(chunk) => {
                        total_bytes += chunk.len() as u64;
                        // Send to inner store write (clone is O(1) refcount bump).
                        if let Err(e) = cache_tx.send(chunk.clone()).await {
                            // Cache write failed; log but continue serving the caller.
                            warn!(
                                %digest,
                                ?e,
                                "get_part_and_cache: cache channel send failed, \
                                 skipping cache"
                            );
                            // Drop the cache writer so the cache_write_fut finishes.
                            drop(cache_tx);
                            // Forward remaining data without caching.
                            writer_for_forward
                                .send(chunk)
                                .await
                                .err_tip(|| "get_part_and_cache: forwarding chunk")?;
                            loop {
                                match proxy_rx.recv().await {
                                    Ok(c) if c.is_empty() => {
                                        writer_for_forward.send_eof().err_tip(
                                            || "get_part_and_cache: forwarding EOF (no cache)",
                                        )?;
                                        return Ok::<(), Error>(());
                                    }
                                    Ok(c) => {
                                        writer_for_forward.send(c).await.err_tip(
                                            || "get_part_and_cache: forwarding chunk (no cache)",
                                        )?;
                                    }
                                    Err(e) => {
                                        return Err(e).err_tip(
                                            || "get_part_and_cache: proxy channel (no cache)",
                                        );
                                    }
                                }
                            }
                        }
                        writer_for_forward
                            .send(chunk)
                            .await
                            .err_tip(|| "get_part_and_cache: forwarding chunk")?;
                    }
                    Err(e) => {
                        return Err(e)
                            .err_tip(|| "get_part_and_cache: reading from proxy channel");
                    }
                }
            }
            Ok::<(), Error>(())
        };

        let (get_part_result, forward_result, cache_result) =
            tokio::join!(get_part_fut, forward_fut, cache_write_fut);

        // Error preference: surface the structured upstream code from the
        // peer's get_part BEFORE the forward path's "Sender dropped before
        // sending EOF" artifact. When the peer errors mid-stream it drops
        // its writer (proxy_tx) without EOF, which makes `forward_fut`
        // observe a generic `Code::Internal` from `proxy_rx.recv()` —
        // masking the structured code (NotFound, Unavailable, DataLoss,
        // etc.) that callers up the chain need for connection-pool /
        // locality / retry decisions. Sibling fix to commit 8674bc19 (the
        // populate path) and 01b68015 (the spawn-detach producer path):
        // the producer is the source of truth, the forward channel error
        // is a secondary symptom.
        //
        // Cancellation note: if the outer caller is dropped, all three
        // futures here drop together via `tokio::join!`. There is no
        // observer for any of the results, so the ordering is irrelevant
        // for cancellation; this only changes behavior when the join!
        // completes naturally with at least one Err.
        if let Err(get_err) = get_part_result {
            // Peer's get_part errored — surface that. forward/cache
            // results are derivative and would only confuse the caller.
            // `guard.fail` is no-op for subordinate guard (does not
            // poison the outer wrapper's fall-through writer); it just
            // marks committed and returns the err unchanged.
            return Err(guard.fail(get_err));
        }
        // Peer's get_part returned Ok. If forwarding failed (e.g.
        // caller's writer broken), propagate that error.
        if let Err(forward_err) = forward_result {
            return Err(guard.fail(forward_err));
        }

        // Log cache write result (non-fatal).
        match cache_result {
            Ok(()) => {
                debug!(
                    %digest,
                    size_bytes = total_bytes,
                    "proxy_cache: cached proxied blob in inner store"
                );
            }
            Err(e) => {
                warn!(
                    %digest,
                    size_bytes = total_bytes,
                    ?e,
                    "proxy_cache: failed to cache proxied blob in inner store"
                );
            }
        }

        // Happy path: forward_fut already sent EOF on the writer; signal
        // delegated termination to the guard so the cosmetic-only Drop
        // path stays consistent with the active-guard idiom used at
        // wrapper layers.
        guard.commit_delegated();
        Ok(())
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
        info!(
            digest = ?_digest_for_log,
            offset,
            length = ?length,
            "WorkerProxyStore::get_part_sequential: awaiting inner.get_part (will reveal whether NotFound returns or stream hangs)"
        );
        let inner_result = IS_WORKER_REQUEST
            .scope(
                true,
                self.inner.get_part(key.borrow(), &mut *writer, offset, length),
            )
            .await;
        let inner_elapsed_ms = inner_await_start.elapsed().as_millis() as u64;
        info!(
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
                    return Err(make_err!(
                        e.code,
                        "WorkerProxyStore: inner store wrote {bytes_written_by_inner} bytes \
                         then failed with {:?} ({}); cannot peer-fetch without corrupting \
                         consumer stream",
                        e.code,
                        e.message_string()
                    ));
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
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
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
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "{REDIRECT_PREFIX}{ep_str}|"
                ));
            }
            if self
                .try_read_from_endpoints(key.borrow(), writer, offset, length, &endpoints)
                .await?
            {
                return Ok(());
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
                return Err(Error::not_found_with_detail(
                    format!(
                        "Blob {digest:?} not found in this worker's inner store \
                         (responder mode, no external RPCs)"
                    ),
                    make_precondition_failure_any(digest),
                ));
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
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "{REDIRECT_PREFIX}{ep_str}|"
                ));
            }
            return Err(Error::not_found_with_detail(
                format!(
                    "Blob {digest:?} not found in inner store or any peer (worker request)"
                ),
                make_precondition_failure_any(digest),
            ));
        }

        let bytes_before_workers = writer.get_bytes_written();
        if self
            .try_read_from_worker(key.borrow(), writer, offset, length)
            .await?
        {
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
            return Err(make_err!(
                Code::Internal,
                "Blob {:?} worker transfer wrote {} bytes then failed, \
                 cannot retry inner store without data corruption",
                key.borrow().into_digest(),
                bytes_written_by_workers
            ));
        }
        match self
            .inner
            .get_part(key.borrow(), writer, offset, length)
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
            Err(e) => return Err(e),
        }

        let digest = key.borrow().into_digest();
        Err(Error::not_found_with_detail(
            format!("Blob {digest:?} not found in inner store or any worker"),
            make_precondition_failure_any(digest),
        ))
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
        writer
            .bind_buffered(rx)
            .await
            .err_tip(|| format!("WorkerProxyStore: {winner_name} racer bind_buffered"))?;

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
        // Subordinate guard: this helper writes to a borrowed writer that
        // a wrapping layer owns; on Err, the wrapper may legitimately
        // fall through. Active `send_error` would poison that path.
        let mut guard = WriteHalfGuard::new_subordinate(writer);

        let peer_chunk = peer_rx.recv().await
            .err_tip(|| "WorkerProxyStore: peer recv after server failure/empty")?;
        if peer_chunk.is_empty() {
            if is_zero_blob {
                guard.commit_eof()
                    .err_tip(|| "WorkerProxyStore: peer EOF for zero-length blob")?;
                return peer_handle.await
                    .map_err(|e| make_err!(Code::Internal, "peer task join: {e}"))?;
            }
            // Non-zero digest, no data from either racer — surface NotFound.
            return Err(guard.fail(Error::not_found_with_detail(
                format!(
                    "WorkerProxyStore: both server and peer {} returned empty EOF for non-zero digest {:?} (size_bytes={})",
                    peer_endpoint,
                    digest,
                    digest.size_bytes(),
                ),
                make_precondition_failure_any(*digest),
            )));
        }
        debug!(
            ?digest,
            endpoint = %peer_endpoint,
            "WorkerProxyStore: peer won race (server empty/failed)"
        );
        guard.send(peer_chunk).await
            .err_tip(|| "WorkerProxyStore: sending peer fallback chunk")?;
        let res = Self::forward_racer("peer", &mut *guard, peer_rx, peer_handle).await;
        guard.commit_delegated_if_ok(&res);
        res
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
        // Subordinate guard: same fall-through-preservation rationale as
        // `await_peer_after_empty_server`.
        let mut guard = WriteHalfGuard::new_subordinate(writer);

        let server_chunk = server_rx.recv().await
            .err_tip(|| "WorkerProxyStore: server recv after peer failure/empty")?;
        if server_chunk.is_empty() {
            if is_zero_blob {
                guard.commit_eof()
                    .err_tip(|| "WorkerProxyStore: server EOF for zero-length blob")?;
                return server_handle.await
                    .map_err(|e| make_err!(Code::Internal, "server task join: {e}"))?;
            }
            return Err(guard.fail(Error::not_found_with_detail(
                format!(
                    "WorkerProxyStore: both peer and server returned empty EOF for non-zero digest {:?} (size_bytes={})",
                    digest,
                    digest.size_bytes(),
                ),
                make_precondition_failure_any(*digest),
            )));
        }
        debug!(
            ?digest,
            "WorkerProxyStore: server won race (peer empty/failed)"
        );
        guard.send(server_chunk).await
            .err_tip(|| "WorkerProxyStore: sending server fallback chunk")?;
        let res = Self::forward_racer("server", &mut *guard, server_rx, server_handle).await;
        guard.commit_delegated_if_ok(&res);
        res
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
        // the blob. This is what makes the bytestream sync-confirm
        // optimization coherent end-to-end: that path returns success
        // to Bazel without storing on the server (the blob is only on
        // the worker), and Bazel's next FindMissingBlobs would otherwise
        // see "missing" and re-upload, defeating the optimization.
        //
        // Stale-Some safety. Workers send explicit
        // `BlobsAvailable.evicted_digests` on every eviction; worker
        // disconnect triggers `remove_endpoint` cleanup at
        // worker_api_server.rs:407 within ~5s; `try_read_from_worker`
        // self-heal evicts the locality entry on per-digest NotFound;
        // and the bytestream fast-path's `worker.has(digest)` is the
        // last-mile sync verification before we drop Bazel's bytes.
        // The worst-case stale-Some manifests as a single proxy-fetch
        // attempt that NotFounds and self-heals — same risk class as
        // server CAS evicting between FMB and Read (which we already
        // accept).
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
                return self.inner.get_part(key, writer, offset, length).await;
            }
        }

        // Only race when explicitly enabled (worker side). Server-side
        // WorkerProxyStore uses the sequential path which generates
        // redirects for workers and proxies for non-worker callers.
        let digest = key.borrow().into_digest();
        let peers = if self.race_peers.load(Ordering::Relaxed) {
            self.locality_map.read().lookup_workers(&digest)
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

    fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        self.inner.drain_stable_digests()
    }

    fn stable_notify(&self) -> Arc<Notify> {
        self.inner.stable_notify()
    }

    fn pin_digests(&self, digests: &[DigestInfo]) {
        self.inner.pin_digests(digests);
    }

    fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        self.inner.drain_failed_digests()
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
    //     Required for bytestream sync-confirm coherence: that path
    //     returns success without storing on the server, so the next
    //     FMB must agree the blob is present or Bazel re-uploads.
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
}
