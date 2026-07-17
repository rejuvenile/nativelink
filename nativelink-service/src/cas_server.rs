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
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::task::{Context, Poll};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use bytes::Bytes;
use fastcdc::v2020::{AsyncStreamCDC, Normalization};
use futures::stream::{FuturesUnordered, Stream};
use futures::{StreamExt, TryStreamExt};
use nativelink_config::cas_server::{CasStoreConfig, WithInstanceName};
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetTreeRequest, GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest,
    SplitBlobResponse, batch_read_blobs_response, batch_update_blobs_response, chunking_function,
    compressor, digest_function,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_store::ac_utils::batch_get_and_decode_digest;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::small_blob_dispatcher::{SMALL_BLOB_THRESHOLD, SmallBlobDispatcher};
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::{WorkerProxyStore, ack_gate_budget_singleton};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::log_utils::throughput_mbps;
use nativelink_util::metrics_publisher::MetricsRegistry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::stall_detector::StallGuard;
use nativelink_util::store_trait::{
    IS_MIRROR_REQUEST, IS_WORKER_REQUEST, Store, StoreKey, StoreLike, UploadSizeInfo,
};
use nativelink_util::zero_copy_codec::{
    GrpcUnaryBody, decode_unary_request, encode_grpc_unary_response,
};
use opentelemetry::context::FutureExt;
use prost::Message;
use tokio::sync::watch;
use tokio_util::io::StreamReader;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, debug, error, error_span, info, instrument, warn};

/// Maximum per-blob size for BatchReadBlobs batch reads (64 MiB).
/// Bounds memory usage per blob when reading through the store chain.
const MAX_BATCH_READ_BLOB_SIZE: u64 = 64 << 20;

/// Maximum total encoded size of cached GetTree results (512 MiB).
const TREE_CACHE_MAX_BYTES: usize = 512 << 20;

/// Maximum number of cached GetTree results.
const TREE_CACHE_MAX_COUNT: u64 = 10_000;

/// TTL for cached GetTree results (5 minutes). CAS trees are immutable
/// (content-addressed), but we expire entries to bound memory usage
/// for trees that aren't re-requested.
const TREE_CACHE_TTL_SECS: u32 = 300;

/// Maximum total encoded size of cached individual directory protos (256 MiB).
/// This cache is populated as a side effect of BFS traversal, so future
/// GetTree calls with overlapping subtrees can skip store fetches for
/// directories already seen.
const SUBTREE_CACHE_MAX_BYTES: usize = 256 << 20;

/// Maximum number of cached individual directory protos.
const SUBTREE_CACHE_MAX_COUNT: u64 = 50_000;

/// TTL for cached individual directory protos (5 minutes).
const SUBTREE_CACHE_TTL_SECS: u32 = 300;

/// A cached GetTree result: the full list of directories for a given
/// root digest. Keyed by `DigestInfo` in the tree cache.
///
/// `directories` is wrapped in `Arc` so cache hits return a cheap
/// reference-count bump instead of deep-cloning every `Directory`.
#[derive(Clone, Debug)]
struct CachedTree {
    directories: Arc<Vec<Directory>>,
    /// Pre-computed total protobuf encoded size for LenEntry.
    encoded_size: u64,
    /// The next_page_token from the full BFS traversal (empty string
    /// when the tree is complete).
    next_page_token: String,
}

impl LenEntry for CachedTree {
    #[inline]
    fn len(&self) -> u64 {
        self.encoded_size
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.directories.is_empty()
    }
}

/// A cached individual `Directory` proto, populated as a side effect of
/// GetTree BFS traversal. When a future BFS encounters a directory
/// digest that's already cached here, it uses the cached proto instead
/// of reading from the store. This avoids redundant fetches for
/// overlapping subtrees across concurrent or sequential GetTree calls
/// (very common in Bazel builds within the same repository).
#[derive(Clone, Debug)]
struct CachedDirectory {
    directory: Directory,
    /// Pre-computed protobuf encoded size for LenEntry.
    encoded_size: u64,
}

impl LenEntry for CachedDirectory {
    #[inline]
    fn len(&self) -> u64 {
        self.encoded_size
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded_size == 0
    }
}

/// Spawn a background task to mirror a blob (with data already in hand)
/// to a random connected worker for OOM redundancy. Fire-and-forget.
fn mirror_blob_to_worker_with_data(store: &Store, digest: DigestInfo, data: Bytes) {
    let Some(_proxy) = store
        .as_store_driver()
        .as_any()
        .downcast_ref::<WorkerProxyStore>()
    else {
        return;
    };

    if digest.size_bytes() == 0 {
        return;
    }

    // Clone the store so the spawned task can access WorkerProxyStore.
    let store = store.clone();
    nativelink_util::background_spawn!("mirror_blob_to_worker", async move {
        let Some(proxy) = store
            .as_store_driver()
            .as_any()
            .downcast_ref::<WorkerProxyStore>()
        else {
            return;
        };
        proxy.mirror_blob_to_random_worker(digest, data).await;
    });
}

/// Maximum size of a single chunk accepted in a `SpliceBlob` request.
/// Deliberately looser than the largest chunk the server ever advertises
/// (4x the maximum allowed average = 4 MiB) so clients using their own
/// chunking function are still accepted. Together with `CHUNK_CONCURRENCY`
/// this bounds the memory a single splice request can pin.
// CAPPED AT 16 MiB: per-chunk ceiling for SpliceBlob; combined with
// CHUNK_CONCURRENCY (10) this bounds in-flight splice memory to ~160 MiB and
// rejects (never buffers) oversized chunks.
const MAX_SPLICE_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// Generous upper bound for the serialized size of one chunk entry in a
/// stored layout (hash string of up to 128 hex characters plus varints and
/// field tags). Multiplied by the configured `max_chunk_count` this caps
/// layout reads from the index store; a larger entry is corrupt. A truncated
/// read is detected (and treated as no layout) by the size consistency check
/// in `read_chunk_layout`.
const MAX_LAYOUT_BYTES_PER_CHUNK: u64 = 160;

/// Number of chunk reads/writes kept in flight while re-assembling or
/// chunking a blob. Matches the `DedupStore` concurrency default.
const CHUNK_CONCURRENCY: usize = 10;

/// Metrics for the experimental `SplitBlob`/`SpliceBlob` chunking RPCs.
/// The split hit rate (`split_hits` / `split_requests_total`) indicates how
/// often chunked downloads could be served; the spliced/split byte totals
/// bound the transfer volume flowing through the chunked paths.
#[derive(Debug, Default)]
pub struct ChunkingMetrics {
    /// Total `SpliceBlob` requests received on chunking-enabled instances.
    pub splice_requests_total: AtomicU64,
    /// `SpliceBlob` requests that were no-ops because the blob and its chunk
    /// layout were already registered.
    pub splice_already_exists: AtomicU64,
    /// `SpliceBlob` requests rejected because the re-assembled blob did not
    /// match the expected digest or size.
    pub splice_verification_failures: AtomicU64,
    /// Total bytes of blobs successfully re-assembled by `SpliceBlob`.
    pub splice_bytes_total: AtomicU64,
    /// Total `SplitBlob` requests received on chunking-enabled instances.
    pub split_requests_total: AtomicU64,
    /// `SplitBlob` requests served from a stored chunk layout.
    pub split_hits: AtomicU64,
    /// `SplitBlob` requests that could not be served because the blob was
    /// not present in the CAS.
    pub split_misses: AtomicU64,
    /// `SplitBlob` requests served by chunking the blob on demand because
    /// no stored layout was available (or its chunks were evicted).
    pub split_chunked_on_demand: AtomicU64,
    /// Total bytes of blobs served as chunk layouts by `SplitBlob`.
    pub split_bytes_total: AtomicU64,
}

impl MetricsComponent for ChunkingMetrics {
    fn publish(
        &self,
        _kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let _enter = group!(field_metadata.name).entered();

        publish!(
            "splice_requests_total",
            &self.splice_requests_total,
            MetricKind::Counter,
            "Total SpliceBlob requests received"
        );
        publish!(
            "splice_already_exists",
            &self.splice_already_exists,
            MetricKind::Counter,
            "SpliceBlob requests that were no-ops because blob and layout already existed"
        );
        publish!(
            "splice_verification_failures",
            &self.splice_verification_failures,
            MetricKind::Counter,
            "SpliceBlob requests rejected due to digest or size mismatch"
        );
        publish!(
            "splice_bytes_total",
            &self.splice_bytes_total,
            MetricKind::Counter,
            "Total bytes of blobs re-assembled by SpliceBlob"
        );
        publish!(
            "split_requests_total",
            &self.split_requests_total,
            MetricKind::Counter,
            "Total SplitBlob requests received"
        );
        publish!(
            "split_hits",
            &self.split_hits,
            MetricKind::Counter,
            "SplitBlob requests served from a stored chunk layout"
        );
        publish!(
            "split_misses",
            &self.split_misses,
            MetricKind::Counter,
            "SplitBlob requests where the blob was not present"
        );
        publish!(
            "split_chunked_on_demand",
            &self.split_chunked_on_demand,
            MetricKind::Counter,
            "SplitBlob requests served by chunking the blob on demand"
        );
        publish!(
            "split_bytes_total",
            &self.split_bytes_total,
            MetricKind::Counter,
            "Total bytes of blobs served as chunk layouts by SplitBlob"
        );

        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Prometheus root prefix for the chunking counters. Rendered names are
/// `cas_<field>` (e.g. `cas_splice_verification_failures`) — the
/// `ChunkingMetrics::publish` `group!` receives an empty root name from the
/// registry render path, so no segment is doubled.
const CHUNKING_METRICS_PREFIX: &str = "cas";

/// Process-wide `ChunkingMetrics` singleton. Production `CasServer::new`
/// wires every CAS instance's counters to THIS Arc (via
/// [`chunking_metrics_singleton`]) and [`register_chunking_metrics`]
/// registers the SAME Arc with the process metrics registry — so the
/// `splice_verification_failures` (CAS-poisoning-rejection) signal and its
/// siblings are reachable on `/metrics` rather than dark on a per-instance
/// tree the binary never sees (the worker-metrics-exposure trap). Tests
/// inject a fresh per-server Arc via
/// [`CasServer::new_with_chunking_metrics`] for isolation.
static CHUNKING_METRICS: OnceLock<Arc<ChunkingMetrics>> = OnceLock::new();

/// Returns a clone of the process-wide [`ChunkingMetrics`] Arc. All calls
/// within the process observe the same atomic state.
#[must_use]
pub fn chunking_metrics_singleton() -> Arc<ChunkingMetrics> {
    Arc::clone(CHUNKING_METRICS.get_or_init(|| Arc::new(ChunkingMetrics::default())))
}

/// Register the process-wide chunking counters with `registry` so the
/// `cas_split_*` / `cas_splice_*` metrics (including
/// `cas_splice_verification_failures`, the CAS-poisoning-attempt signal)
/// render on `/metrics`. Call ONCE at binary startup — the counters are
/// process-global; double-registration would publish duplicate lines. The
/// registered Arc is the SAME one production `CasServer::new` increments.
pub fn register_chunking_metrics(registry: &MetricsRegistry) {
    registry.register(CHUNKING_METRICS_PREFIX, chunking_metrics_singleton());
}

/// Per-instance state for the experimental chunking RPCs.
#[derive(Debug, Clone)]
struct ChunkingInstance {
    /// Store holding blob-digest -> chunk-layout mappings.
    index_store: Store,
    /// Average chunk size used for server-side `FastCDC` 2020 chunking.
    avg_chunk_size_bytes: u32,
    /// Maximum number of chunks accepted in a `SpliceBlob` request or
    /// produced by on-demand chunking.
    max_chunk_count: usize,
}

impl ChunkingInstance {
    /// Maximum serialized layout size consistent with `max_chunk_count`.
    const fn max_layout_size(&self) -> u64 {
        self.max_chunk_count as u64 * MAX_LAYOUT_BYTES_PER_CHUNK
    }
}

/// Per-instance plumbing for the CAS service. Carries the configured
/// `cas_store` name (e.g. `"cas_STORE"`) so we can pass the same value
/// to `SmallBlobDispatcher::schedule_dispatch_to_all_workers` as the
/// `store_id` the dispatcher's per-store `EphemeralServerSidePin` is
/// registered under (see `nativelink.rs:499-525`). Pre-allocated as
/// `Arc<str>` so the per-blob hot path only does an O(1) refcount bump
/// (perf-optimizer #168 NIT-1 — avoids `Arc::from(&str)` allocation
/// per dispatch).
#[derive(Debug, Clone)]
struct CasInstance {
    store: Store,
    cas_store_name_arc: Arc<str>,
    /// #168 producer-side hook. `None` when no worker scheduler is
    /// configured (the dispatcher is not constructed at startup —
    /// `nativelink.rs:409-534`).
    small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
}

#[derive(Debug)]
pub struct CasServer {
    stores: HashMap<String, CasInstance>,
    /// Per-instance state for the experimental `SplitBlob`/`SpliceBlob`
    /// content-defined chunking RPCs (#2497). Only populated for CAS
    /// instances that opt in via `experimental_chunking` and are NOT backed
    /// by a grpc proxy store (those forward the RPCs to the backend). Empty
    /// when no instance enables chunking, in which case the handlers return
    /// `Unimplemented` and behavior is unchanged.
    chunking_instances: HashMap<String, ChunkingInstance>,
    /// Names of grpc-store-backed instances that opted into
    /// `experimental_chunking`. Only these forward SplitBlob/SpliceBlob to
    /// the backend; a grpc-backed instance that did NOT opt in returns
    /// `Unimplemented` (matching its advertised `split/splice = false`)
    /// rather than forwarding an RPC the operator never enabled.
    grpc_chunking_instances: HashSet<String>,
    /// Counters for the experimental chunking RPCs. Shared with the process
    /// metrics registry in production (see [`chunking_metrics_singleton`]);
    /// a fresh per-server Arc in tests.
    chunking_metrics: Arc<ChunkingMetrics>,
    /// Cache of GetTree results keyed by root digest. CAS trees are
    /// immutable (content-addressed), so a cache hit avoids re-running
    /// the full BFS traversal. Bounded by size and TTL.
    tree_cache: MokaEvictingMap<DigestInfo, DigestInfo, CachedTree, SystemTime>,
    /// Cache of individual directory digests -> their resolved Directory
    /// proto. Populated as a side effect of GetTree BFS. When a future
    /// BFS encounters a directory that's already cached here, it can use
    /// the cached proto instead of reading from the store. This covers
    /// the common case of overlapping subtrees across GetTree calls
    /// (e.g., multiple Bazel targets in the same repo share identical
    /// third_party/ or generated code directories).
    ///
    /// Level 3 optimization (Tree proto lookup) is deferred: GetTree is
    /// keyed by a root Directory digest, but Tree protos are stored
    /// under their own separate digest in the CAS. There is no mapping
    /// from root_directory_digest -> tree_digest in the CAS protocol,
    /// so the server cannot look up a pre-assembled Tree proto given
    /// only the root digest. Supporting this would require either:
    ///   (a) A side index populated from ActionResult output_directories,
    ///       requiring hooks into the AC write path, or
    ///   (b) A separate mapping store (root_digest -> tree_digest).
    /// The subtree cache already covers the main performance win
    /// (avoiding redundant fetches for shared subdirectories), so the
    /// Tree proto lookup is not needed at this time.
    subtree_cache: MokaEvictingMap<DigestInfo, DigestInfo, CachedDirectory, SystemTime>,
    /// In-flight GetTree BFS operations, keyed by root digest. When
    /// multiple concurrent GetTree calls arrive for the same tree,
    /// only the first performs the BFS traversal. Others subscribe to
    /// the watch channel and wait for the result to appear in
    /// `tree_cache`, avoiding thundering-herd redundant traversals.
    tree_inflight: parking_lot::Mutex<HashMap<DigestInfo, watch::Receiver<bool>>>,
}

type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send + 'static>>;

impl CasServer {
    /// Production entry point. Wires every CAS instance's chunking counters
    /// to the process-wide [`chunking_metrics_singleton`] so they render on
    /// `/metrics` once [`register_chunking_metrics`] runs at startup.
    pub fn new(
        configs: &[WithInstanceName<CasStoreConfig>],
        store_manager: &StoreManager,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
    ) -> Result<Self, Error> {
        Self::new_with_chunking_metrics(
            configs,
            store_manager,
            small_blob_dispatcher,
            chunking_metrics_singleton(),
        )
    }

    /// Like [`Self::new`] but takes the [`ChunkingMetrics`] Arc explicitly.
    /// Production passes the process-wide singleton (so `/metrics` reflects
    /// live counters); tests pass a fresh Arc for per-server isolation.
    pub fn new_with_chunking_metrics(
        configs: &[WithInstanceName<CasStoreConfig>],
        store_manager: &StoreManager,
        small_blob_dispatcher: Option<Arc<SmallBlobDispatcher>>,
        chunking_metrics: Arc<ChunkingMetrics>,
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(configs.len());
        let mut chunking_instances = HashMap::new();
        let mut grpc_chunking_instances = HashSet::new();
        for config in configs {
            let store = store_manager.get_store(&config.cas_store).ok_or_else(|| {
                make_input_err!("'cas_store': '{}' does not exist", config.cas_store)
            })?;
            if let Some(chunking_config) = &config.experimental_chunking {
                let avg_chunk_size_bytes = chunking_config
                    .validated_avg_chunk_size_bytes()
                    .err_tip(|| {
                        format!(
                            "In 'experimental_chunking' of instance '{}'",
                            config.instance_name
                        )
                    })?;
                // #2497 D1: SpliceBlob performs a SERVER-ORIGINATED
                // `store.update` reassembly. On a `WorkerProxyStore`-wrapped
                // CAS (the production `cas_STORE` topology) that write flows
                // through the FL-688 ack-gate, which was designed and tested
                // ONLY for WORKER mirror uploads (local_worker → mirror_blobs
                // → BIS ack), never for server-originated streams — an
                // unvalidated interaction. Fail fast at startup rather than
                // splice through an untested path, mirroring the same-store
                // and grpc-index foot-gun rejections below. Detection: the
                // WorkerProxyStore is the OUTERMOST driver in the prod chain,
                // reached via `as_store_driver().as_any()` (NOT
                // `Store::downcast_ref`, which steps INTO `inner_store`).
                error_if!(
                    store
                        .as_store_driver()
                        .as_any()
                        .downcast_ref::<WorkerProxyStore>()
                        .is_some(),
                    "'experimental_chunking' of instance '{}' is not supported on a WorkerProxyStore-wrapped 'cas_store': SpliceBlob's server-originated reassembly is unvalidated against the FL-688 ack-gate (designed for worker mirror uploads only). Restrict chunking to a non-WorkerProxyStore CAS instance.",
                    config.instance_name
                );
                if store.downcast_ref::<GrpcStore>(None).is_some() {
                    // SplitBlob/SpliceBlob for grpc-store-backed instances
                    // are forwarded to the backend, which owns the chunk
                    // layouts; a local index store is meaningless there.
                    error_if!(
                        chunking_config.index_store.is_some(),
                        "'experimental_chunking.index_store' of instance '{}' must not be set when 'cas_store' is a grpc store: SplitBlob/SpliceBlob are forwarded to the backend",
                        config.instance_name
                    );
                    // Record that THIS grpc instance opted in; only opted-in
                    // grpc instances forward (D2). No ChunkingInstance: the
                    // forwarding shortcut in the handlers takes over before
                    // local chunking is reached.
                    grpc_chunking_instances.insert(config.instance_name.to_string());
                } else {
                    let index_store_name =
                        chunking_config.index_store.as_ref().ok_or_else(|| {
                            make_input_err!(
                                "'experimental_chunking.index_store' of instance '{}' is required",
                                config.instance_name
                            )
                        })?;
                    // Chunk layouts are stored under the digests of the blobs
                    // they describe but do not hash to them, so writing them
                    // into the CAS itself would overwrite blob content.
                    error_if!(
                        index_store_name == &config.cas_store,
                        "'experimental_chunking.index_store' of instance '{}' must not be the same store as 'cas_store'",
                        config.instance_name
                    );
                    let index_store =
                        store_manager.get_store(index_store_name).ok_or_else(|| {
                            make_input_err!(
                                "'experimental_chunking.index_store': '{index_store_name}' does not exist"
                            )
                        })?;
                    let avg_chunk_size_bytes = u32::try_from(avg_chunk_size_bytes)
                        .err_tip(|| "avg_chunk_size_bytes did not fit in u32")?;
                    let max_chunk_count =
                        usize::try_from(chunking_config.resolved_max_chunk_count())
                            .err_tip(|| "max_chunk_count did not fit in usize")?;
                    chunking_instances.insert(
                        config.instance_name.to_string(),
                        ChunkingInstance {
                            index_store,
                            avg_chunk_size_bytes,
                            max_chunk_count,
                        },
                    );
                }
            }
            stores.insert(
                config.instance_name.to_string(),
                CasInstance {
                    store,
                    cas_store_name_arc: Arc::from(config.cas_store.as_str()),
                    small_blob_dispatcher: small_blob_dispatcher.clone(),
                },
            );
        }
        let tree_cache_policy = EvictionPolicy {
            max_bytes: TREE_CACHE_MAX_BYTES,
            max_count: TREE_CACHE_MAX_COUNT,
            max_seconds: TREE_CACHE_TTL_SECS,
            ..Default::default()
        };
        let tree_cache = MokaEvictingMap::with_anchor(&tree_cache_policy, SystemTime::now());
        let subtree_cache_policy = EvictionPolicy {
            max_bytes: SUBTREE_CACHE_MAX_BYTES,
            max_count: SUBTREE_CACHE_MAX_COUNT,
            max_seconds: SUBTREE_CACHE_TTL_SECS,
            ..Default::default()
        };
        let subtree_cache =
            MokaEvictingMap::with_anchor(&subtree_cache_policy, SystemTime::now());
        Ok(Self {
            stores,
            chunking_instances,
            grpc_chunking_instances,
            chunking_metrics,
            tree_cache,
            subtree_cache,
            tree_inflight: parking_lot::Mutex::new(HashMap::new()),
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    /// Metrics for the experimental `SplitBlob`/`SpliceBlob` RPCs.
    pub fn chunking_metrics(&self) -> &ChunkingMetrics {
        &self.chunking_metrics
    }

    /// Returns the number of entries in the tree cache. Exposed for
    /// integration tests to verify caching behavior.
    #[doc(hidden)]
    pub async fn tree_cache_len(&self) -> usize {
        self.tree_cache.len_for_test().await
    }

    /// Returns the number of entries in the subtree cache. Exposed for
    /// integration tests to verify caching behavior.
    #[doc(hidden)]
    pub async fn subtree_cache_len(&self) -> usize {
        self.subtree_cache.len_for_test().await
    }

    /// Returns the number of in-flight GetTree BFS operations. Exposed
    /// for integration tests to verify coalescing behavior.
    #[doc(hidden)]
    pub fn tree_inflight_len(&self) -> usize {
        self.tree_inflight.lock().len()
    }

    /// Wrap this server in a `ZeroCopyCasService` that intercepts
    /// `BatchUpdateBlobs` RPCs and decodes the request directly from HTTP
    /// body frames, bypassing tonic's `BytesMut` reassembly buffer.
    ///
    /// All other CAS RPCs (FindMissingBlobs, BatchReadBlobs, GetTree)
    /// delegate to the standard tonic path.
    pub fn into_zero_copy_service(
        self,
        max_decoding_message_size: usize,
        max_encoding_message_size: usize,
    ) -> ZeroCopyCasService {
        let inner = Arc::new(self);
        ZeroCopyCasService {
            inner: inner.clone(),
            tonic_service: Server::from_arc(inner)
                .max_decoding_message_size(max_decoding_message_size)
                .max_encoding_message_size(max_encoding_message_size),
        }
    }

    async fn inner_find_missing_blobs(
        &self,
        request: FindMissingBlobsRequest,
    ) -> Result<Response<FindMissingBlobsResponse>, Error> {
        let instance_name = &request.instance_name;
        let instance = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .clone();
        let store = instance.store.clone();

        let mut requested_blobs = Vec::with_capacity(request.blob_digests.len());
        for digest in &request.blob_digests {
            requested_blobs.push(DigestInfo::try_from(digest.clone())?.into());
        }
        let sizes = store
            .has_many(&requested_blobs)
            .await
            .err_tip(|| "In find_missing_blobs")?;
        let missing_blob_digests: Vec<_> = sizes
            .into_iter()
            .zip(request.blob_digests)
            .filter_map(|(maybe_size, digest)| maybe_size.map_or_else(|| Some(digest), |_| None))
            .collect();

        debug!(
            requested = requested_blobs.len(),
            missing = missing_blob_digests.len(),
            "FindMissingBlobs",
        );
        if !missing_blob_digests.is_empty() {
            debug!(
                digests = ?missing_blob_digests.iter().map(|d| format!("{}-{}", d.hash, d.size_bytes)).collect::<Vec<_>>(),
                "FindMissingBlobs: missing digests",
            );
        }

        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn inner_batch_update_blobs(
        &self,
        request: BatchUpdateBlobsRequest,
        is_mirror: bool,
        is_worker: bool,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Error> {
        let instance_name = &request.instance_name;

        let instance = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .clone();
        let store = instance.store.clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            return grpc_store.batch_update_blobs(Request::new(request)).await;
        }

        let store_ref = &store;
        let blob_count = request.requests.len();
        let batch_start = std::time::Instant::now();
        // Pre-resolve dispatcher + store_name into local refs the
        // FuturesUnordered closures can capture cheaply. #168
        // producer-side hook.
        let dispatcher_ref = instance.small_blob_dispatcher.as_ref();
        let cas_store_name_arc = &instance.cas_store_name_arc;

        // Pre-parse all digests and validate sizes upfront so we can do a
        // single batch has() check instead of N individual checks inside
        // ExistenceCacheStore::update().
        let mut parsed: Vec<(DigestInfo, usize)> = Vec::with_capacity(blob_count);
        for req in &request.requests {
            let digest = req
                .digest
                .clone()
                .err_tip(|| "Digest not found in request")?;
            let digest_info = DigestInfo::try_from(digest)?;
            let size_bytes = usize::try_from(digest_info.size_bytes())
                .err_tip(|| "Digest size_bytes was not convertible to usize")?;
            error_if!(
                size_bytes != req.data.len(),
                "Digest for upload had mismatching sizes, digest said {} data  said {}",
                size_bytes,
                req.data.len()
            );
            parsed.push((digest_info, size_bytes));
        }

        // Batch has() check: skip writes for blobs the store already has.
        let keys: Vec<StoreKey<'_>> = parsed
            .iter()
            .map(|(d, _)| (*d).into())
            .collect();
        let mut has_results = vec![None; keys.len()];
        store_ref
            .has_with_results(&keys, &mut has_results)
            .await
            .err_tip(|| "BatchUpdateBlobs: has_with_results failed")?;
        let skipped = has_results.iter().filter(|r| r.is_some()).count();
        if skipped > 0 {
            info!(
                blob_count,
                skipped,
                "BatchUpdateBlobs: skipping blobs that already exist",
            );
        }

        let update_futures: FuturesUnordered<_> = request
            .requests
            .into_iter()
            .zip(parsed.iter())
            .zip(has_results.iter())
            .map(|((request, &(digest_info, size_bytes)), has_result)| async move {
                // Skip blobs the store already has.
                if has_result.is_some() {
                    return Ok::<batch_update_blobs_response::Response, Error>(
                        batch_update_blobs_response::Response {
                            digest: Some(digest_info.into()),
                            status: Some(GrpcStatus {
                                code: 0, // OK
                                ..Default::default()
                            }),
                        },
                    );
                }
                let request_data = request.data;
                debug!(
                    %digest_info,
                    size_bytes,
                    "BatchUpdateBlobs: blob received",
                );
                // Clone data for mirroring (Bytes clone is O(1) refcount bump).
                let mirror_data = request_data.clone();
                let upload_start = std::time::Instant::now();

                // #FL-688: is this a mirror/worker write that must NOT be
                // ack-gated? Mirror/worker writes ARE themselves the 2nd
                // replica push — gating them would deadlock (a mirror waiting
                // on its own mirror) and loop. They keep the plain write path.
                let proxy_ref = if is_mirror || is_worker {
                    None
                } else {
                    // The WorkerProxyStore is the OUTERMOST driver; reach it via
                    // `as_store_driver()` (NOT `Store::downcast_ref`, which calls
                    // `inner_store()` and delegates PAST the proxy).
                    store_ref
                        .as_store_driver()
                        .as_any()
                        .downcast_ref::<WorkerProxyStore>()
                };

                let result = if let Some(proxy) = proxy_ref {
                    // #FL-688 HARD ≥2-replica ack-gate. Acquire a global
                    // byte-budget permit (BACKPRESSURE if the budget is full —
                    // never silent buffering) so concurrent ack-gated writes
                    // cannot grow in-flight memory without bound, then drive
                    // the local write CONCURRENTLY with the 2nd-replica
                    // confirmation and gate the ack on local-Ok AND the
                    // confirmation. `_ack_permit` releases at the end of this
                    // blob's future.
                    //
                    // SMALL blobs (≤ SMALL_BLOB_THRESHOLD) confirm via the SLOW
                    // TIER (production durable replica = Redis SMALL_CAS_CACHED;
                    // the SmallBlobDispatcher handles worker read-locality
                    // separately). LARGE blobs confirm via the worker mirror.
                    let confirm_via_slow_tier = size_bytes <= SMALL_BLOB_THRESHOLD;
                    let _ack_permit = ack_gate_budget_singleton()
                        .acquire(size_bytes)
                        .await;
                    let local_write = IS_MIRROR_REQUEST.scope(is_mirror, async {
                        store_ref
                            .update_oneshot(digest_info, request_data)
                            .await
                            .err_tip(|| "Error writing to store")
                    });
                    proxy
                        .ack_gated_write(
                            digest_info,
                            mirror_data.clone(),
                            confirm_via_slow_tier,
                            local_write,
                        )
                        .await
                } else {
                    // No WorkerProxyStore in this composition (or a
                    // mirror/worker write): preserve the plain write path.
                    IS_MIRROR_REQUEST.scope(is_mirror, async {
                        store_ref
                            .update_oneshot(digest_info, request_data)
                            .await
                            .err_tip(|| "Error writing to store")
                    }).await
                };

                match &result {
                    Ok(()) => {
                        let elapsed = upload_start.elapsed();
                        debug!(
                            %digest_info,
                            size_bytes,
                            elapsed_ms = elapsed.as_millis() as u64,
                            throughput_mbps = format!("{:.1}", throughput_mbps(size_bytes as u64, elapsed)),
                            ack_gated = proxy_ref.is_some(),
                            "BatchUpdateBlobs: CAS write completed",
                        );
                        // #168 producer-side: fan out small CAS blobs
                        // to every connected worker via the dispatcher.
                        // Sits ALONGSIDE the ack-gate mirror above. Skip for
                        // `is_mirror` AND `is_worker` to avoid feedback loops:
                        //   - is_mirror: server-to-worker mirror push
                        //     re-arrived via cas_server (rare).
                        //   - is_worker: worker uploaded action results
                        //     to the server; we'd loop them back
                        //     uselessly to the originating worker.
                        // Both gates close the over-action sibling
                        // contract (#168 testing-czar M1 / USER
                        // DIRECTIVE on loop prevention).
                        //
                        // Fire-and-forget — do not .await; see
                        // `SmallBlobDispatcher::schedule_dispatch_to_all_workers` doc.
                        // #FL-688: the random-single mirror is now subsumed by
                        // the ack-gated `mirror_and_confirm_data` above
                        // (proxy_ref.is_some()); only the dispatcher fan-out
                        // (proactive read-locality to EVERY worker, distinct
                        // from the single ack-gating replica) remains here.
                        if !is_mirror
                            && !is_worker
                            && size_bytes <= SMALL_BLOB_THRESHOLD
                        {
                            if let Some(dispatcher) = dispatcher_ref {
                                dispatcher.schedule_dispatch_to_all_workers(
                                    cas_store_name_arc.clone(),
                                    digest_info,
                                    mirror_data.clone(),
                                );
                            }
                        }
                        // #FL-688: when there is NO WorkerProxyStore in the
                        // composition (proxy_ref is None for a non-mirror,
                        // non-worker write), fall back to the pre-FL-688
                        // fire-and-forget random mirror so test/edge
                        // compositions still get read-locality redundancy.
                        // With a proxy present, the ack-gate already placed
                        // (and confirmed) the single mirror replica.
                        if !is_mirror && !is_worker && proxy_ref.is_none() {
                            mirror_blob_to_worker_with_data(store_ref, digest_info, mirror_data);
                        }
                    }
                    Err(e) => {
                        let elapsed = upload_start.elapsed();
                        warn!(
                            %digest_info,
                            size_bytes,
                            elapsed_ms = elapsed.as_millis() as u64,
                            ?e,
                            "BatchUpdateBlobs: blob upload failed",
                        );
                    }
                }
                Ok::<_, Error>(batch_update_blobs_response::Response {
                    digest: Some(digest_info.into()),
                    status: Some(result.map_or_else(Into::into, |()| GrpcStatus::default())),
                })
            })
            .collect();
        let responses = update_futures
            .try_collect::<Vec<batch_update_blobs_response::Response>>()
            .await?;

        let batch_elapsed = batch_start.elapsed();
        let total_bytes: usize = responses
            .iter()
            .filter_map(|r| r.digest.as_ref())
            .map(|d| d.size_bytes as usize)
            .sum();
        info!(
            blob_count,
            total_bytes,
            elapsed_ms = batch_elapsed.as_millis() as u64,
            "BatchUpdateBlobs: batch completed",
        );

        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    /// Zero-copy BatchUpdateBlobs handler called from `ZeroCopyCasService`.
    ///
    /// The request has already been decoded from the raw HTTP body frames
    /// without copying through tonic's BytesMut reassembly buffer.
    async fn zero_copy_batch_update_blobs(
        &self,
        request: BatchUpdateBlobsRequest,
        is_mirror: bool,
        is_worker: bool,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let digest_function = request.digest_function;

        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "BatchUpdateBlobs",
        );
        IS_WORKER_REQUEST
            .scope(
                is_worker,
                self.inner_batch_update_blobs(request, is_mirror, is_worker)
                    .instrument(error_span!("cas_server_batch_update_blobs"))
                    .with_context(
                        make_ctx_for_hash_func(digest_function)
                            .err_tip(|| "In CasServer::batch_update_blobs")?,
                    ),
            )
            .await
            .err_tip(|| "Failed on batch_update_blobs() command")
            .map_err(Into::into)
    }

    async fn inner_batch_read_blobs(
        &self,
        request: BatchReadBlobsRequest,
    ) -> Result<Response<BatchReadBlobsResponse>, Error> {
        let instance_name = &request.instance_name;

        let instance = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .clone();
        let store = instance.store.clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            return grpc_store.batch_read_blobs(Request::new(request)).await;
        }

        // Parse all digests upfront so we can do a single pipelined batch read.
        let mut parsed_digests: Vec<DigestInfo> = Vec::with_capacity(request.digests.len());
        for digest in &request.digests {
            parsed_digests.push(DigestInfo::try_from(digest.clone())?);
        }

        // Use batch_get_part_unchunked which pipelines the underlying I/O
        // (e.g. a single Redis round-trip for all keys instead of N individual ones).
        // Cap per-blob size to bound memory usage across the batch.
        let keys: Vec<_> = parsed_digests.iter().map(|d| StoreKey::Digest(*d)).collect();
        let read_start = std::time::Instant::now();
        let batch_results = store
            .batch_get_part_unchunked(keys, Some(MAX_BATCH_READ_BLOB_SIZE))
            .await;
        let batch_elapsed = read_start.elapsed();

        let mut total_bytes: u64 = 0;
        let responses: Vec<batch_read_blobs_response::Response> = request
            .digests
            .into_iter()
            .zip(parsed_digests.iter())
            .zip(batch_results)
            .map(|((digest, &digest_info), result)| {
                let (status, data) = match result {
                    Err(mut e) => {
                        if e.code != Code::NotFound {
                            error!(
                                %digest_info,
                                elapsed_ms = batch_elapsed.as_millis() as u64,
                                ?e,
                                "BatchReadBlobs: CAS read failed",
                            );
                        }
                        if e.code == Code::NotFound {
                            // Trim the error code. Not Found is quite common and we don't want to send a large
                            // error (debug) message for something that is common. We resize to just the last
                            // message as it will be the most relevant.
                            e.messages.resize_with(1, String::new);
                        }
                        (e.into(), Bytes::new())
                    }
                    Ok(v) => {
                        total_bytes += v.len() as u64;
                        (GrpcStatus::default(), v)
                    }
                };
                batch_read_blobs_response::Response {
                    status: Some(status),
                    digest: Some(digest),
                    compressor: compressor::Value::Identity.into(),
                    data,
                }
            })
            .collect();

        debug!(
            blob_count = responses.len(),
            total_bytes,
            elapsed_ms = batch_elapsed.as_millis() as u64,
            throughput_mbps = format!("{:.1}", throughput_mbps(total_bytes, batch_elapsed)),
            "BatchReadBlobs: batch completed",
        );

        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    async fn inner_get_tree(
        &self,
        request: GetTreeRequest,
    ) -> Result<impl Stream<Item = Result<GetTreeResponse, Status>> + Send + use<>, Error> {
        let instance_name = &request.instance_name;

        let instance = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .clone();
        let store = instance.store.clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        // Note: We don't know the digests here, so we try perform a very shallow
        // check to see if it's a grpc store.
        if let Some(grpc_store) = store.downcast_ref::<GrpcStore>(None) {
            let stream = grpc_store
                .get_tree(Request::new(request))
                .await?
                .into_inner();
            return Ok(stream.left_stream());
        }
        let tree_start = std::time::Instant::now();
        let root_digest: DigestInfo = request
            .root_digest
            .err_tip(|| "Expected root_digest to exist in GetTreeRequest")?
            .try_into()
            .err_tip(|| "In GetTreeRequest::root_digest")?;

        // Cache check: for non-paginated requests (the common case from
        // Bazel), serve from the tree cache to avoid redundant BFS
        // traversals. CAS trees are immutable (content-addressed), so
        // the cached result is always valid.
        let is_unpaginated = request.page_token.is_empty() && request.page_size == 0;

        // For unpaginated requests, coalesce concurrent GetTree calls
        // for the same root digest. Only one request performs the BFS
        // traversal; others wait for it to populate the tree_cache.
        // This prevents thundering-herd when many workers request the
        // same tree simultaneously.
        //
        // `inflight_tx` is Some when we are the "leader" — the first
        // request that registered for this root_digest. On all exit
        // paths (success, error, early return) we must send on it to
        // wake waiters, and remove the entry from `tree_inflight`.
        let mut inflight_tx: Option<watch::Sender<bool>> = None;

        if is_unpaginated {
            if let Some(cached) = self.tree_cache.get(&root_digest).await {
                let elapsed = tree_start.elapsed();
                info!(
                    ?root_digest,
                    dir_count = cached.directories.len(),
                    encoded_size = cached.encoded_size,
                    elapsed_us = elapsed.as_micros() as u64,
                    "GetTree: cache hit",
                );
                return Ok(futures::stream::once(futures::future::ready(
                    Ok(GetTreeResponse {
                        directories: cached.directories.as_ref().clone(),
                        next_page_token: cached.next_page_token,
                    }),
                ))
                .right_stream());
            }

            // Check-and-register in a single lock scope to prevent
            // TOCTOU race where two requests both see no inflight entry
            // and both register as leader.
            let maybe_rx = {
                use std::collections::hash_map::Entry;
                let mut inflight = self.tree_inflight.lock();
                match inflight.entry(root_digest) {
                    Entry::Occupied(entry) => {
                        // Another request is already doing BFS.
                        Some(entry.get().clone())
                    }
                    Entry::Vacant(entry) => {
                        // We are the first — register as leader.
                        let (tx, rx) = watch::channel(false);
                        entry.insert(rx);
                        inflight_tx = Some(tx);
                        None
                    }
                }
            };
            if let Some(mut rx) = maybe_rx {
                // Wait for the leader to complete BFS.
                info!(
                    ?root_digest,
                    "GetTree: coalescing with in-flight BFS traversal",
                );
                // Ignore errors (sender dropped = leader failed/panicked).
                let _ = rx.changed().await;
                // Re-check cache — the leader should have populated it.
                if let Some(cached) = self.tree_cache.get(&root_digest).await {
                    let elapsed = tree_start.elapsed();
                    info!(
                        ?root_digest,
                        dir_count = cached.directories.len(),
                        encoded_size = cached.encoded_size,
                        elapsed_us = elapsed.as_micros() as u64,
                        "GetTree: coalesced cache hit",
                    );
                    return Ok(futures::stream::once(futures::future::ready(
                        Ok(GetTreeResponse {
                            directories: cached.directories.as_ref().clone(),
                            next_page_token: cached.next_page_token,
                        }),
                    ))
                    .right_stream());
                }
                // Leader failed (missing dirs, error, etc.). Fall through
                // and do our own BFS as a non-leader (no inflight_tx).
                warn!(
                    ?root_digest,
                    "GetTree: coalesced request found no cache entry, performing own BFS",
                );
            }
        }

        // BFS traversal. Runs for:
        // - The inflight leader (inflight_tx is Some)
        // - A waiter whose leader failed (inflight_tx is None, is_unpaginated)
        // - Paginated requests (inflight_tx is None, !is_unpaginated)
        let result = self
            .bfs_get_tree(
                &store,
                root_digest,
                &request.page_token,
                request.page_size,
                tree_start,
                is_unpaginated,
            )
            .await;

        // Cleanup: if we are the inflight leader, notify waiters and
        // remove ourselves from the inflight map regardless of outcome.
        if let Some(tx) = inflight_tx {
            // Send wakes all receivers waiting on changed().
            let _ = tx.send(true);
            self.tree_inflight.lock().remove(&root_digest);
        }

        let response = result?;
        Ok(futures::stream::once(futures::future::ready(Ok(response))).right_stream())
    }

    /// Perform the BFS traversal for GetTree. Factored out so the
    /// coalescing logic in `inner_get_tree` can wrap it with inflight
    /// tracking and cleanup.
    async fn bfs_get_tree(
        &self,
        store: &Store,
        root_digest: DigestInfo,
        page_token: &str,
        page_size: i32,
        tree_start: std::time::Instant,
        is_unpaginated: bool,
    ) -> Result<GetTreeResponse, Error> {
        let mut deque: VecDeque<DigestInfo> = VecDeque::with_capacity(64);
        // Track all digests we have ever enqueued to avoid fetching/processing
        // the same directory twice. In a Merkle tree, identical subdirectory
        // structures share the same digest, so multiple parents at the same BFS
        // level can reference the same child digest. Without deduplication:
        //   1. We fetch the same blob N times concurrently (wasteful).
        //   2. `level_results.remove()` succeeds for the first occurrence but
        //      returns None for duplicates, causing a spurious
        //      "Directory missing from level results" error.
        let mut seen: HashSet<DigestInfo> = HashSet::with_capacity(256);
        let mut directories: Vec<Directory> = Vec::with_capacity(256);
        // `page_token` will return the `{hash_str}-{size_bytes}` of the current request's first directory digest.
        let page_token_digest = if page_token.is_empty() {
            root_digest
        } else {
            let mut page_token_parts = page_token.split('-');
            DigestInfo::try_new(
                page_token_parts
                    .next()
                    .err_tip(|| "Failed to parse `hash_str` in `page_token`")?,
                page_token_parts
                    .next()
                    .err_tip(|| "Failed to parse `size_bytes` in `page_token`")?
                    .parse::<i64>()
                    .err_tip(|| "Failed to parse `size_bytes` as i64")?,
            )
            .err_tip(|| "Failed to parse `page_token` as `Digest` in `GetTreeRequest`")?
        };
        // If `page_size` is 0, paging is not necessary — return all directories.
        let page_size_limit = if page_size == 0 {
            usize::MAX
        } else {
            usize::try_from(page_size).unwrap_or(usize::MAX)
        };
        let mut page_token_matched = page_size == 0;
        seen.insert(root_digest);
        deque.push_back(root_digest);
        let mut page_filled = false;

        // Per-level timing and dedup tracking for diagnostics.
        let mut bfs_level: u32 = 0;
        let mut total_duplicates_skipped: u64 = 0;
        let mut total_missing_skipped: u64 = 0;
        let mut total_subtree_cache_hits: u64 = 0;
        let mut level_timings: Vec<(u32, usize, u64, u64, u64)> = Vec::with_capacity(16); // (level, dirs_fetched, children_discovered, elapsed_ms, cache_hits)

        while !deque.is_empty() && !page_filled {
            let level_start = std::time::Instant::now();
            let level: Vec<DigestInfo> = deque.drain(..).collect();

            // Subtree cache lookup: check which directories we already have
            // cached from previous GetTree calls. Only fetch uncached ones
            // from the store (avoids redundant I/O for overlapping subtrees).
            let mut level_results: HashMap<DigestInfo, Directory> =
                HashMap::with_capacity(level.len());
            let mut uncached_digests: Vec<DigestInfo> = Vec::with_capacity(level.len());
            let mut level_cache_hits: u64 = 0;

            for &digest in &level {
                if let Some(cached_dir) = self.subtree_cache.get(&digest).await {
                    level_results.insert(digest, cached_dir.directory);
                    level_cache_hits += 1;
                } else {
                    uncached_digests.push(digest);
                }
            }
            total_subtree_cache_hits += level_cache_hits;

            // Batch-fetch uncached directories using a single pipelined
            // store operation (one Redis round-trip instead of N).
            // Tolerant: missing or corrupt directories are skipped rather
            // than failing the entire GetTree response. The client can
            // fill in gaps via individual directory fetches.
            let mut level_missing: u64 = 0;
            if !uncached_digests.is_empty() {
                let batch_results =
                    batch_get_and_decode_digest::<Directory>(store, &uncached_digests).await;
                for (digest, result) in batch_results {
                    match result {
                        Ok(directory) => {
                            // Populate the subtree cache for future GetTree calls.
                            let encoded_size = directory.encoded_len() as u64;
                            let cached = CachedDirectory {
                                directory: directory.clone(),
                                encoded_size,
                            };
                            drop(self.subtree_cache.insert(digest, cached).await);
                            level_results.insert(digest, directory);
                        }
                        Err(e) => {
                            warn!(
                                ?root_digest,
                                missing_digest = %digest,
                                bfs_level,
                                err = ?e,
                                "GetTree: skipping missing/corrupt directory, client will fetch individually"
                            );
                            level_missing += 1;
                        }
                    }
                }
            }
            total_missing_skipped += level_missing;
            // Process directories in the order they appeared in the deque (BFS discovery order).
            // Missing directories are skipped — the client's parallel BFS fallback
            // will detect gaps and fetch them individually.
            let mut level_new_children: u64 = 0;
            let mut level_duplicates: u64 = 0;
            for (i, digest) in level.iter().enumerate() {
                let Some(directory) = level_results.get(digest).cloned() else {
                    // This directory was missing/corrupt — skip it.
                    // Its children won't be enqueued, but the client will
                    // discover and fetch them via its own tree walk.
                    continue;
                };
                if *digest == page_token_digest {
                    page_token_matched = true;
                }
                // Always enqueue children so BFS traversal finds the page token
                // even when it's deeper in the tree.
                for child in &directory.directories {
                    let child_digest: DigestInfo = child
                        .digest
                        .clone()
                        .err_tip(|| {
                            "Expected Digest to exist in Directory::directories::digest"
                        })?
                        .try_into()
                        .err_tip(|| "In Directory::file::digest")?;
                    // Only enqueue children we haven't seen before to avoid
                    // duplicate fetches and processing.
                    if seen.insert(child_digest) {
                        deque.push_back(child_digest);
                        level_new_children += 1;
                    } else {
                        level_duplicates += 1;
                    }
                }
                if page_token_matched {
                    directories.push(directory);
                    if directories.len() >= page_size_limit {
                        // Put remaining unprocessed items from this level back
                        // into the front of the deque for the next page token.
                        let remaining: Vec<DigestInfo> =
                            level[i + 1..].iter().copied().collect();
                        // Prepend remaining items before any children already in deque.
                        for (j, rem) in remaining.into_iter().enumerate() {
                            deque.insert(j, rem);
                        }
                        page_filled = true;
                        break;
                    }
                }
            }

            let level_elapsed_ms = level_start.elapsed().as_millis() as u64;
            total_duplicates_skipped += level_duplicates;

            if level_duplicates > 0 {
                debug!(
                    ?root_digest,
                    bfs_level,
                    duplicates_skipped = level_duplicates,
                    "GetTree: deduplication skipped children at this level",
                );
            }

            debug!(
                ?root_digest,
                bfs_level,
                dirs_in_level = level.len(),
                subtree_cache_hits = level_cache_hits,
                store_fetched = uncached_digests.len(),
                new_children = level_new_children,
                duplicates_skipped = level_duplicates,
                elapsed_ms = level_elapsed_ms,
                "GetTree: BFS level completed",
            );

            if level_elapsed_ms > 100 {
                debug!(
                    ?root_digest,
                    bfs_level,
                    dirs_in_level = level.len(),
                    subtree_cache_hits = level_cache_hits,
                    store_fetched = uncached_digests.len(),
                    new_children = level_new_children,
                    elapsed_ms = level_elapsed_ms,
                    "GetTree: slow BFS level (>100ms)",
                );
            }

            level_timings.push((bfs_level, level.len(), level_new_children, level_elapsed_ms, level_cache_hits));
            bfs_level += 1;
        }
        // `next_page_token` will return the `{hash_str}-{size_bytes}` of the next request's first directory digest.
        // It will be an empty string when it reached the end of the directory tree.
        let next_page_token: String = deque
            .front()
            .map_or_else(String::new, |value| format!("{value}"));

        let elapsed = tree_start.elapsed();
        let total_bytes: u64 = directories.iter().map(|d| d.encoded_len() as u64).sum();

        // Build per-level timing breakdown string for the summary log.
        let level_breakdown: String = level_timings
            .iter()
            .map(|(lvl, dirs, children, ms, cache_hits)| {
                format!("L{lvl}:{dirs}dirs/{cache_hits}cached/{children}children/{ms}ms")
            })
            .collect::<Vec<_>>()
            .join(", ");

        if total_missing_skipped > 0 {
            warn!(
                ?root_digest,
                dir_count = directories.len(),
                total_bytes,
                total_missing_skipped,
                total_duplicates_skipped,
                total_subtree_cache_hits,
                bfs_levels = bfs_level,
                elapsed_ms = elapsed.as_millis() as u64,
                level_breakdown = %level_breakdown,
                "GetTree: resolved directory tree (partial — some directories missing)",
            );
        } else {
            debug!(
                ?root_digest,
                dir_count = directories.len(),
                total_bytes,
                total_duplicates_skipped,
                total_subtree_cache_hits,
                bfs_levels = bfs_level,
                elapsed_ms = elapsed.as_millis() as u64,
                level_breakdown = %level_breakdown,
                "GetTree: resolved directory tree",
            );
        }

        // Cache the result for future GetTree calls with the same root
        // digest. Only cache complete, non-paginated results with no
        // missing directories (partial trees could be stale).
        if is_unpaginated && total_missing_skipped == 0 {
            // Move directories into Arc first (zero-copy), give cache a
            // cheap Arc clone, then clone out for the response. Avoids
            // the old Arc::new(directories.clone()) which briefly doubled
            // the directory list in memory.
            let dirs_arc = Arc::new(directories);
            let cached = CachedTree {
                directories: Arc::clone(&dirs_arc),
                encoded_size: total_bytes,
                next_page_token: next_page_token.clone(),
            };
            drop(self.tree_cache.insert(root_digest, cached).await);
            Ok(GetTreeResponse {
                directories: dirs_arc.as_ref().clone(),
                next_page_token,
            })
        } else {
            Ok(GetTreeResponse {
                directories,
                next_page_token,
            })
        }
    }

    /// Returns the CAS store for an instance and its chunking state, or
    /// `Unimplemented` when chunking is not enabled for it. Grpc-store-backed
    /// instances never reach this: their handlers forward the RPC to the
    /// backend first.
    fn chunking_instance(&self, instance_name: &str) -> Result<(Store, ChunkingInstance), Error> {
        let store = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?
            .store
            .clone();
        let chunking_instance = self
            .chunking_instances
            .get(instance_name)
            .ok_or_else(|| {
                make_err!(
                    Code::Unimplemented,
                    "Blob chunking is not enabled for instance '{instance_name}'"
                )
            })?
            .clone();
        Ok((store, chunking_instance))
    }

    /// Returns the backend `GrpcStore` when the instance's CAS is a grpc
    /// proxy store AND the instance opted into `experimental_chunking`, in
    /// which case chunking RPCs are forwarded verbatim. A grpc-backed
    /// instance that did NOT opt in returns `None` here so the caller falls
    /// through to `chunking_instance()` and answers `Unimplemented` (#2497
    /// D2) — matching its advertised `split/splice = false`, rather than
    /// forwarding an RPC the operator never enabled.
    fn grpc_store_for_instance(&self, instance_name: &str) -> Option<&GrpcStore> {
        if !self.grpc_chunking_instances.contains(instance_name) {
            return None;
        }
        self.stores
            .get(instance_name)
            .and_then(|instance| instance.store.downcast_ref::<GrpcStore>(None))
    }

    /// Returns the digest function explicitly requested by the client, or
    /// `None` when the field was left unset. REAPI's length-based inference
    /// cannot be used as a fallback here: SHA256 and BLAKE3 digests are both
    /// 32 bytes, and `NativeLink` announces support for both. Notably Bazel
    /// (9.1.1) leaves this field unset even when running with
    /// `--digest_function=blake3`.
    fn explicit_hasher_func(digest_function_value: i32) -> Option<DigestHasherFunc> {
        digest_function::Value::try_from(digest_function_value)
            .ok()
            .and_then(|value| DigestHasherFunc::try_from(value).ok())
    }

    /// Determines the digest function of a blob already present in the CAS
    /// by hashing its content with each supported function and returning the
    /// one that reproduces `blob_digest`.
    async fn infer_blob_hasher_func(
        store: &Store,
        blob_digest: DigestInfo,
    ) -> Result<DigestHasherFunc, Error> {
        const CANDIDATES: [DigestHasherFunc; 2] =
            [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3];
        let (tx, rx) = make_buf_channel_pair();
        let read_store = store.clone();
        let read_fut = async move {
            let mut tx = tx;
            read_store
                .get_part(blob_digest, &mut tx, 0, None)
                .await
                .err_tip(|| "Failed to read blob in infer_blob_hasher_func")
        };
        let hash_fut = async move {
            let mut rx = rx;
            let mut hashers = CANDIDATES.map(|func| func.hasher());
            loop {
                let data = rx
                    .recv()
                    .await
                    .err_tip(|| "In infer_blob_hasher_func::recv")?;
                if data.is_empty() {
                    break; // EOF.
                }
                for hasher in &mut hashers {
                    hasher.update(&data);
                }
            }
            Ok::<_, Error>(hashers.map(|mut hasher| hasher.finalize_digest()))
        };
        let (read_res, hash_res) = futures::join!(read_fut, hash_fut);
        let computed_digests = read_res.merge(hash_res)?;
        CANDIDATES
            .iter()
            .zip(computed_digests)
            .find(|(_, computed)| *computed == blob_digest)
            .map(|(func, _)| *func)
            .ok_or_else(|| {
                make_err!(
                    Code::NotFound,
                    "Blob {blob_digest} does not match any supported digest function; no split information available"
                )
            })
    }

    /// Returns the display names of the chunks missing from the CAS. The
    /// existence check also touches present chunks, which extends their
    /// lifetimes on a best-effort basis (stores that answer existence from a
    /// cache may not promote the underlying entries).
    async fn missing_chunks(store: &Store, chunk_digests: &[Digest]) -> Result<Vec<String>, Error> {
        let mut digest_infos = Vec::with_capacity(chunk_digests.len());
        for digest in chunk_digests {
            digest_infos
                .push(DigestInfo::try_from(digest.clone()).err_tip(|| "Invalid chunk digest")?);
        }
        let chunk_keys: Vec<_> = digest_infos.iter().map(|digest| (*digest).into()).collect();
        let sizes = store
            .has_many(&chunk_keys)
            .await
            .err_tip(|| "In missing_chunks")?;
        Ok(sizes
            .iter()
            .zip(&digest_infos)
            .filter(|(maybe_size, _)| maybe_size.is_none())
            .map(|(_, digest)| digest.to_string())
            .collect())
    }

    /// Reads the chunk layout registered for a blob. Returns `None` when no
    /// usable layout exists: not registered, undecodable, or inconsistent
    /// with the blob size (which also rejects entries truncated by the read
    /// cap below).
    async fn read_chunk_layout(
        chunking_instance: &ChunkingInstance,
        blob_digest: DigestInfo,
    ) -> Option<SplitBlobResponse> {
        let layout_bytes = chunking_instance
            .index_store
            .get_part_unchunked(blob_digest, 0, Some(chunking_instance.max_layout_size()))
            .await
            .ok()?;
        let layout = SplitBlobResponse::decode(layout_bytes).ok()?;
        // A usable layout must reproduce the blob exactly, so the chunk
        // sizes have to add up to the blob size.
        let mut total_size: u64 = 0;
        for digest in &layout.chunk_digests {
            total_size = total_size.checked_add(u64::try_from(digest.size_bytes).ok()?)?;
        }
        (total_size == blob_digest.size_bytes()).then_some(layout)
    }

    /// Writes the chunk layout for a blob to the index store. This is the
    /// write side of the format `read_chunk_layout` expects.
    async fn write_chunk_layout(
        index_store: &Store,
        blob_digest: DigestInfo,
        layout: &SplitBlobResponse,
    ) -> Result<(), Error> {
        index_store
            .update_oneshot(blob_digest, layout.encode_to_vec().into())
            .await
            .err_tip(|| "Failed to write chunk layout to index store")
    }

    async fn inner_split_blob(
        &self,
        request: SplitBlobRequest,
    ) -> Result<Response<SplitBlobResponse>, Error> {
        // If we are a GrpcStore we forward the RPC to the backend, which
        // owns chunking and the layout index for proxied instances.
        if let Some(grpc_store) = self.grpc_store_for_instance(&request.instance_name) {
            return grpc_store.split_blob(Request::new(request)).await;
        }
        let (store, chunking_instance) = self.chunking_instance(&request.instance_name)?;
        self.chunking_metrics
            .split_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let blob_digest: DigestInfo = request
            .blob_digest
            .err_tip(|| "Expected blob_digest to exist in SplitBlobRequest")?
            .try_into()
            .err_tip(|| "In SplitBlobRequest::blob_digest")?;

        // The existence check also touches the blob, extending its lifetime
        // (best effort) as suggested by the REAPI spec for SplitBlob.
        let (blob_exists, maybe_layout) = futures::join!(
            store.has(blob_digest),
            Self::read_chunk_layout(&chunking_instance, blob_digest),
        );
        if blob_exists.err_tip(|| "In split_blob")?.is_none() {
            self.chunking_metrics
                .split_misses
                .fetch_add(1, Ordering::Relaxed);
            return Err(make_err!(
                Code::NotFound,
                "Blob {blob_digest} not present in the CAS in split_blob"
            ));
        }

        // Serve the registered layout if it is still fully backed by chunks
        // in the CAS. Any problem with it (missing, corrupt, evicted chunks,
        // or a transient chunk existence-check failure) falls back to
        // re-chunking the blob below.
        if let Some(layout) = maybe_layout
            && matches!(
                Self::missing_chunks(&store, &layout.chunk_digests).await,
                Ok(missing) if missing.is_empty()
            )
        {
            self.chunking_metrics
                .split_hits
                .fetch_add(1, Ordering::Relaxed);
            self.chunking_metrics
                .split_bytes_total
                .fetch_add(blob_digest.size_bytes(), Ordering::Relaxed);
            return Ok(Response::new(layout));
        }

        // No usable layout: chunk the blob on demand with FastCDC 2020,
        // store the chunks and the layout, and serve the result. This is the
        // path taken for blobs that were uploaded whole (e.g. outputs
        // produced by remote execution workers).
        let split_response = self
            .chunk_blob_on_demand(
                &store,
                &chunking_instance,
                blob_digest,
                request.digest_function,
            )
            .await?;
        self.chunking_metrics
            .split_chunked_on_demand
            .fetch_add(1, Ordering::Relaxed);
        self.chunking_metrics
            .split_bytes_total
            .fetch_add(blob_digest.size_bytes(), Ordering::Relaxed);
        Ok(Response::new(split_response))
    }

    /// Chunks the blob with `FastCDC` 2020 (normalization level 2, parameters
    /// derived from the configured average chunk size per the REAPI spec),
    /// uploads any missing chunks to the CAS, registers the layout in the
    /// index store, and returns it.
    async fn chunk_blob_on_demand(
        &self,
        store: &Store,
        chunking_instance: &ChunkingInstance,
        blob_digest: DigestInfo,
        digest_function_value: i32,
    ) -> Result<SplitBlobResponse, Error> {
        let avg_size = chunking_instance.avg_chunk_size_bytes;
        let (min_size, max_size) = (avg_size / 4, avg_size * 4);
        let max_chunk_count = chunking_instance.max_chunk_count;
        // Chunk digests MUST use the blob's digest function. When the client
        // leaves the field unset it has to be inferred from the blob content
        // (an extra read pass) since the hash length alone is ambiguous.
        let hasher_func = match Self::explicit_hasher_func(digest_function_value) {
            Some(hasher_func) => hasher_func,
            None => Self::infer_blob_hasher_func(store, blob_digest).await?,
        };

        // Set by the in-stream cap guard below when it aborts an over-cap
        // blob. A genuinely over-cap blob is usually larger than the buf
        // channel, so the guard drops `rx` while the store read is still in
        // flight; that read then fails with `Code::Internal`. `Error::merge`
        // prefers the read's code, so without this flag the RPC would return
        // `Internal` instead of the REAPI-correct `NotFound`. After the join
        // we consult the flag and force `NotFound` regardless of the merged
        // code (see below). Mirrors the `verification_failed` pattern in
        // `inner_splice_blob`.
        let over_cap = AtomicBool::new(false);
        let over_cap_ref = &over_cap;
        let (tx, rx) = make_buf_channel_pair();
        let read_store = store.clone();
        // `tx` is moved into the future so that when the read finishes or
        // fails it is dropped, which terminates the chunking stream.
        let read_fut = async move {
            let mut tx = tx;
            read_store
                .get_part(blob_digest, &mut tx, 0, None)
                .await
                .err_tip(|| format!("Failed to read blob {blob_digest} in chunk_blob_on_demand"))
        };
        // `rx` is owned by this future so an early error return drops it,
        // which aborts the in-flight read instead of leaving it blocked.
        let chunk_fut = async move {
            let mut bytes_reader = StreamReader::new(rx);
            let mut cdc = AsyncStreamCDC::with_level(
                &mut bytes_reader,
                min_size,
                avg_size,
                max_size,
                Normalization::Level2,
            );
            // Chunks are hashed and stored CHUNK_CONCURRENCY at a time while
            // the blob keeps streaming; `buffered` preserves chunk order.
            let chunk_digests: Vec<Digest> = pin!(cdc.as_stream())
                .enumerate()
                .map(|(chunk_index, chunk_result)| async move {
                    // #2497 D4: refuse ONCE the cap is exceeded, BEFORE
                    // hashing or storing this chunk. Bounds work at
                    // ~max_chunk_count chunks instead of chunking AND writing
                    // the entire over-cap blob and leaving orphan chunks
                    // (the previous post-`try_collect` check did both). The
                    // early `Err` aborts the stream, which drops `rx` and
                    // cancels the in-flight blob read (O(blob_size) saved).
                    // `buffered` may have started a few later chunks
                    // concurrently; they hit this same guard and store nothing.
                    if chunk_index >= max_chunk_count {
                        over_cap_ref.store(true, Ordering::Relaxed);
                        return Err(make_err!(
                            Code::NotFound,
                            "Blob {blob_digest} exceeds the configured max_chunk_count of {max_chunk_count}; no split information available"
                        ));
                    }
                    let chunk = chunk_result
                        .map_err(|e| make_err!(Code::Internal, "Failed to chunk blob: {e:?}"))
                        .err_tip(|| "In chunk_blob_on_demand")?;
                    let mut hasher = hasher_func.hasher();
                    hasher.update(&chunk.data);
                    let chunk_digest = hasher.finalize_digest();
                    // The existence check also touches pre-existing chunks,
                    // extending their lifetimes (best effort). FastCDC is
                    // deterministic, so repeated splits of similar blobs
                    // mostly find their chunks present.
                    if store
                        .has(chunk_digest)
                        .await
                        .err_tip(|| "In chunk_blob_on_demand")?
                        .is_none()
                    {
                        store
                            .update_oneshot(chunk_digest, chunk.data.into())
                            .await
                            .err_tip(|| {
                                format!(
                                    "Failed to store chunk {chunk_digest} in chunk_blob_on_demand"
                                )
                            })?;
                    }
                    Ok::<Digest, Error>(chunk_digest.into())
                })
                .buffered(CHUNK_CONCURRENCY)
                .try_collect()
                .await?;
            Ok::<Vec<Digest>, Error>(chunk_digests)
        };
        let (read_res, chunk_res) = futures::join!(read_fut, chunk_fut);
        // #2497 D4: over-cap outcome must be deterministically `NotFound`.
        // When the guard fired it aborted the read mid-stream, so `read_res`
        // is `Err(Internal)` and `Error::merge` below would surface that code
        // instead of the guard's `NotFound`. Force `NotFound` here regardless
        // of which future's error `merge` prefers — the observable contract
        // (over-cap -> NotFound, "no split information available") holds for
        // large streaming-store blobs too, not only buffer-fitting ones.
        if over_cap.load(Ordering::Relaxed) {
            return Err(make_err!(
                Code::NotFound,
                "Blob {blob_digest} exceeds the configured max_chunk_count of {max_chunk_count}; no split information available"
            ));
        }
        // Not over-cap: prefer the read error (the chunker error is usually a
        // consequence of it); merge keeps both messages when both fail.
        let chunk_digests = read_res
            .merge(chunk_res)
            .err_tip(|| "Failed to chunk blob in chunk_blob_on_demand")?;

        let split_response = SplitBlobResponse {
            chunk_digests,
            chunking_function: chunking_function::Value::FastCdc2020.into(),
        };
        Self::write_chunk_layout(&chunking_instance.index_store, blob_digest, &split_response)
            .await?;
        Ok(split_response)
    }

    async fn inner_splice_blob(
        &self,
        request: SpliceBlobRequest,
    ) -> Result<Response<SpliceBlobResponse>, Error> {
        // If we are a GrpcStore we forward the RPC to the backend, which
        // owns chunking and the layout index for proxied instances.
        if let Some(grpc_store) = self.grpc_store_for_instance(&request.instance_name) {
            return grpc_store.splice_blob(Request::new(request)).await;
        }
        let (store, chunking_instance) = self.chunking_instance(&request.instance_name)?;
        let index_store = chunking_instance.index_store;
        self.chunking_metrics
            .splice_requests_total
            .fetch_add(1, Ordering::Relaxed);

        let blob_digest: DigestInfo = request
            .blob_digest
            .err_tip(|| "Expected blob_digest to exist in SpliceBlobRequest")?
            .try_into()
            .err_tip(|| "In SpliceBlobRequest::blob_digest")?;

        error_if!(
            request.chunk_digests.is_empty(),
            "chunk_digests must not be empty in splice_blob"
        );
        error_if!(
            request.chunk_digests.len() > chunking_instance.max_chunk_count,
            "Request has {} chunk_digests, expected at most {} in splice_blob",
            request.chunk_digests.len(),
            chunking_instance.max_chunk_count
        );
        let mut chunk_digests = Vec::with_capacity(request.chunk_digests.len());
        let mut total_size: u64 = 0;
        for digest in &request.chunk_digests {
            let digest_info = DigestInfo::try_from(digest.clone())
                .err_tip(|| "In SpliceBlobRequest::chunk_digests")?;
            error_if!(
                digest_info.size_bytes() == 0 || digest_info.size_bytes() > MAX_SPLICE_CHUNK_SIZE,
                "Chunk {digest_info} has invalid size, expected to be in range (0, {MAX_SPLICE_CHUNK_SIZE}] in splice_blob"
            );
            total_size += digest_info.size_bytes();
            chunk_digests.push(digest_info);
        }
        if total_size != blob_digest.size_bytes() {
            self.chunking_metrics
                .splice_verification_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(make_err!(
                Code::InvalidArgument,
                "Sum of chunk sizes ({total_size}) does not match the expected blob size ({}) in splice_blob",
                blob_digest.size_bytes()
            ));
        }

        // One round of existence checks: the chunks (which also touches
        // them, best-effort extending their lifetimes), the blob, and the
        // registered layout.
        let (missing_chunks, blob_exists, layout_exists) = futures::join!(
            Self::missing_chunks(&store, &request.chunk_digests),
            store.has(blob_digest),
            index_store.has(blob_digest),
        );
        let missing_chunks = missing_chunks.err_tip(|| "In splice_blob")?;
        if !missing_chunks.is_empty() {
            return Err(make_err!(
                Code::NotFound,
                "Chunk(s) [{}] not present in the CAS in splice_blob",
                missing_chunks.join(", ")
            ));
        }
        // Fast path: if the blob and its chunk layout are already registered
        // this request is a no-op.
        if blob_exists.err_tip(|| "In splice_blob")?.is_some()
            && layout_exists.err_tip(|| "In splice_blob")?.is_some()
        {
            self.chunking_metrics
                .splice_already_exists
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(SpliceBlobResponse {
                blob_digest: Some(blob_digest.into()),
            }));
        }

        // Re-assemble the blob into the store: chunk reads are pipelined
        // CHUNK_CONCURRENCY at a time while hashing and channel writes stay
        // in chunk order. The digest is verified before the final EOF is
        // sent, so a digest mismatch aborts the upload before the store
        // commits it.
        // When the client sets the digest function, verify with exactly that
        // function. When it is unset the hash length is ambiguous (SHA256
        // and BLAKE3 are both 32 bytes), so hash with both candidates and
        // accept whichever reproduces the expected digest.
        let candidate_hasher_funcs: Vec<DigestHasherFunc> =
            match Self::explicit_hasher_func(request.digest_function) {
                Some(hasher_func) => vec![hasher_func],
                None => vec![DigestHasherFunc::Sha256, DigestHasherFunc::Blake3],
            };
        let verification_failed = AtomicBool::new(false);
        let verification_failed_ref = &verification_failed;
        let (tx, rx) = make_buf_channel_pair();
        let send_store = store.clone();
        // `tx` is moved into the future so that an early error return drops
        // it without an EOF, which aborts the in-flight store update instead
        // of leaving it waiting for more data.
        let send_fut = async move {
            let mut tx = tx;
            let mut hashers: Vec<_> = candidate_hasher_funcs
                .iter()
                .map(DigestHasherFunc::hasher)
                .collect();
            let mut fetch_stream = futures::stream::iter(chunk_digests.into_iter().map(
                move |chunk_digest| {
                    let store = send_store.clone();
                    async move {
                        let data = store
                            .get_part_unchunked(chunk_digest, 0, None)
                            .await
                            .err_tip(|| {
                                format!("Failed to read chunk {chunk_digest} in splice_blob")
                            })?;
                        if u64::try_from(data.len()).unwrap_or(0) != chunk_digest.size_bytes() {
                            return Err(make_err!(
                                Code::Internal,
                                "Chunk {chunk_digest} content has length {}, expected {}, in splice_blob",
                                data.len(),
                                chunk_digest.size_bytes()
                            ));
                        }
                        Ok::<Bytes, Error>(data)
                    }
                },
            ))
            .buffered(CHUNK_CONCURRENCY);
            while let Some(data) = fetch_stream.next().await {
                let data = data?;
                for hasher in &mut hashers {
                    hasher.update(&data);
                }
                tx.send(data)
                    .await
                    .err_tip(|| "Failed to send chunk data in splice_blob")?;
            }
            drop(fetch_stream);
            let computed_digests: Vec<DigestInfo> = hashers
                .iter_mut()
                .map(DigestHasher::finalize_digest)
                .collect();
            if !computed_digests
                .iter()
                .any(|computed| *computed == blob_digest)
            {
                verification_failed_ref.store(true, Ordering::Relaxed);
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Digest of spliced blob ({}) does not match the expected digest ({blob_digest}) in splice_blob",
                    computed_digests
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" / ")
                ));
            }
            tx.send_eof()
                .err_tip(|| "Failed to send EOF in splice_blob")?;
            Ok::<(), Error>(())
        };
        let update_fut = store.update(
            blob_digest,
            rx,
            UploadSizeInfo::ExactSize(blob_digest.size_bytes()),
        );
        let (send_res, update_res) = futures::join!(send_fut, update_fut);
        if verification_failed.load(Ordering::Relaxed) {
            self.chunking_metrics
                .splice_verification_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        // Prefer the sender error: it carries the reason the upload was
        // aborted (e.g. the digest mismatch), the store error is usually a
        // consequence; merge keeps both messages when both fail.
        send_res
            .merge(update_res)
            .err_tip(|| "Failed to write spliced blob to store in splice_blob")?;

        // Persist the chunk layout so SplitBlob can serve it later.
        let split_response = SplitBlobResponse {
            chunk_digests: request.chunk_digests,
            chunking_function: request.chunking_function,
        };
        Self::write_chunk_layout(&index_store, blob_digest, &split_response).await?;

        self.chunking_metrics
            .splice_bytes_total
            .fetch_add(blob_digest.size_bytes(), Ordering::Relaxed);
        Ok(Response::new(SpliceBlobResponse {
            blob_digest: Some(blob_digest.into()),
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasServer {
    type GetTreeStream = GetTreeStream;

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(
            // Mostly to skip request.blob_digests which is sometimes enormous
            request.instance_name = ?grpc_request.get_ref().instance_name,
            request.digest_function = ?grpc_request.get_ref().digest_function
        )
    )]
    async fn find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_find_missing_blobs(request)
            .instrument(error_span!("cas_server_find_missing_blobs"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In CasServer::find_missing_blobs")?,
            )
            .await
            .err_tip(|| "Failed on find_missing_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn batch_update_blobs(
        &self,
        grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let is_mirror = grpc_request
            .metadata()
            .contains_key("x-nativelink-mirror");
        // #168 producer-side: extract `is_worker` so the dispatcher hook
        // in `inner_batch_update_blobs` skips fan-out for worker uploads
        // (workers already hold the blob locally; dispatching back would
        // loop). Symmetric to the `is_worker` extraction in
        // `batch_read_blobs` below.
        let is_worker = grpc_request
            .metadata()
            .contains_key("x-nativelink-worker");
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "BatchUpdateBlobs",
        );
        IS_WORKER_REQUEST
            .scope(
                is_worker,
                self.inner_batch_update_blobs(request, is_mirror, is_worker)
                    .instrument(error_span!("cas_server_batch_update_blobs"))
                    .with_context(
                        make_ctx_for_hash_func(digest_function)
                            .err_tip(|| "In CasServer::batch_update_blobs")?,
                    ),
            )
            .await
            .err_tip(|| "Failed on batch_update_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn batch_read_blobs(
        &self,
        grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let is_worker = grpc_request
            .metadata()
            .contains_key("x-nativelink-worker");
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        let _stall_guard = StallGuard::new(
            nativelink_util::stall_detector::DEFAULT_STALL_THRESHOLD,
            "BatchReadBlobs",
        );
        IS_WORKER_REQUEST
            .scope(
                is_worker,
                self.inner_batch_read_blobs(request)
                    .instrument(error_span!("cas_server_batch_read_blobs"))
                    .with_context(
                        make_ctx_for_hash_func(digest_function)
                            .err_tip(|| "In CasServer::batch_read_blobs")?,
                    ),
            )
            .await
            .err_tip(|| "Failed on batch_read_blobs() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn get_tree(
        &self,
        grpc_request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;

        let resp = self
            .inner_get_tree(request)
            .instrument(error_span!("cas_server_get_tree"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::get_tree")?,
            )
            .await
            .err_tip(|| "Failed on get_tree() command")
            .map(|stream| -> Response<Self::GetTreeStream> { Response::new(Box::pin(stream)) })
            .map_err(Into::into);

        if resp.is_ok() {
            debug!(return = "Ok(<stream>)");
        }
        resp
    }

    // REAPI content-defined chunking (SplitBlob/SpliceBlob, upstream #2497).
    // Real handlers are wired to the instance's `cas_store` chain and its
    // configured `experimental_chunking.index_store`. When an instance has no
    // `experimental_chunking` block the inner handlers return `Unimplemented`
    // (via `chunking_instance`), and the capabilities server advertises
    // `split_blob_support`/`splice_blob_support = false` for it, so behavior
    // is unchanged for CAS instances that do not opt in.
    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(
            request.instance_name = ?grpc_request.get_ref().instance_name,
            request.blob_digest = ?grpc_request.get_ref().blob_digest,
            request.digest_function = ?grpc_request.get_ref().digest_function,
        )
    )]
    async fn split_blob(
        &self,
        grpc_request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_split_blob(request)
            .instrument(error_span!("cas_server_split_blob"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::split_blob")?,
            )
            .await
            .err_tip(|| "Failed on split_blob() command")
            .map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(
            // Skip request.chunk_digests which is sometimes enormous.
            request.instance_name = ?grpc_request.get_ref().instance_name,
            request.blob_digest = ?grpc_request.get_ref().blob_digest,
            request.digest_function = ?grpc_request.get_ref().digest_function,
        )
    )]
    async fn splice_blob(
        &self,
        grpc_request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_splice_blob(request)
            .instrument(error_span!("cas_server_splice_blob"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In CasServer::splice_blob")?,
            )
            .await
            .err_tip(|| "Failed on splice_blob() command")
            .map_err(Into::into)
    }
}

/// A tower `Service` wrapper around `CasServer` that intercepts
/// `BatchUpdateBlobs` RPCs and decodes the `BatchUpdateBlobsRequest`
/// directly from raw HTTP body frames, bypassing tonic's `BytesMut`
/// reassembly buffer.
///
/// This preserves zero-copy semantics for `Bytes` fields in the request
/// (specifically `BatchUpdateBlobsRequest.requests[].data`), eliminating
/// one full copy of every blob byte on the inbound path.
///
/// All other CAS RPCs pass through to the inner tonic service unchanged.
#[derive(Clone, Debug)]
pub struct ZeroCopyCasService {
    inner: Arc<CasServer>,
    tonic_service: Server<CasServer>,
}

impl ZeroCopyCasService {
    /// Apply compression settings to the inner tonic service
    /// (for non-BatchUpdateBlobs RPCs).
    pub fn accept_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.tonic_service = self.tonic_service.accept_compressed(encoding);
        self
    }

    /// Apply compression settings to the inner tonic service
    /// (for non-BatchUpdateBlobs RPCs).
    pub fn send_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.tonic_service = self.tonic_service.send_compressed(encoding);
        self
    }
}

impl tonic::server::NamedService for ZeroCopyCasService {
    const NAME: &'static str =
        "build.bazel.remote.execution.v2.ContentAddressableStorage";
}

impl tower::Service<http::Request<tonic::body::Body>> for ZeroCopyCasService {
    type Response = http::Response<tonic::body::Body>;
    type Error = core::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path();
        if path
            == "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs"
        {
            let inner = self.inner.clone();
            Box::pin(async move {
                let (parts, body) = req.into_parts();
                let is_mirror = parts.headers.contains_key("x-nativelink-mirror");
                let is_worker = parts.headers.contains_key("x-nativelink-worker");

                // Decode the unary request directly from body frames.
                let request: BatchUpdateBlobsRequest =
                    match decode_unary_request(body).await {
                        Ok(req) => req,
                        Err(status) => return Ok(status.into_http()),
                    };

                let result = inner
                    .zero_copy_batch_update_blobs(request, is_mirror, is_worker)
                    .await;

                match result {
                    Ok(response) => {
                        let (resp_metadata, update_response, _extensions) =
                            response.into_parts();
                        let body_bytes =
                            encode_grpc_unary_response(&update_response);
                        let body = GrpcUnaryBody::new(body_bytes);
                        let mut http_response = http::Response::new(
                            tonic::body::Body::new(body),
                        );
                        *http_response.headers_mut() =
                            resp_metadata.into_headers();
                        http_response.headers_mut().insert(
                            http::header::CONTENT_TYPE,
                            tonic::metadata::GRPC_CONTENT_TYPE,
                        );
                        Ok(http_response)
                    }
                    Err(status) => Ok(status.into_http()),
                }
            })
        } else {
            // Delegate all other RPCs to the standard tonic path.
            self.tonic_service.call(req)
        }
    }
}
