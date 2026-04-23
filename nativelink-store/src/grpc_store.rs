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
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::connection_manager::ConnectionManager;
use nativelink_util::digest_hasher::{DigestHasherFunc, default_digest_hasher_func};
use nativelink_util::health_utils::HealthStatusIndicator;
use nativelink_util::proto_stream_utils::{
    FirstStream, WriteRequestStreamWrapper, WriteState, WriteStateWrapper,
};
use nativelink_util::resource_info::ResourceInfo;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{
    IS_MIRROR_REQUEST, IS_WORKER_REQUEST, ItemCallback, StoreDriver, StoreKey, StoreOptimizations,
    UploadSizeInfo,
};
use nativelink_util::{default_health_status_indicator, tls_utils};
use opentelemetry::context::Context;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tonic::{Code, IntoRequest, Request, Response, Status, Streaming};
use tracing::{error, info, trace, warn};
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
                        let endpoint = tls_utils::endpoint(endpoint_config)
                            .map_err(|e| make_input_err!("Invalid URI for GrpcStore endpoint (dual/tcp): {e:?}"))?;
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
                    Transport::Dual { tcp: tcp_cm, quic: quic_channel }
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

        let (batch_tx, batch_rx) =
            if batch_update_threshold > 0 {
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
        });

        if let Some(rx) = batch_rx {
            let weak = Arc::downgrade(&store);
            let max_concurrent = spec.max_concurrent_batch_rpcs.max(1) as usize;
            let semaphore = Arc::new(Semaphore::new(max_concurrent));
            tokio::spawn(Self::batch_flush_loop(weak, rx, semaphore));
            info!(
                batch_update_threshold,
                max_concurrent,
                "GrpcStore: BatchUpdateBlobs opportunistic batching enabled",
            );
        }

        Ok(store)
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
        let mut client = ByteStreamClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_DECODING_SIZE);
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
        let mut client = ActionCacheClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_DECODING_SIZE);
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
                return digests
                    .iter()
                    .map(|d| (*d, Err(err.clone())))
                    .collect();
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
            results
                .entry(*d)
                .or_insert_with(|| Err(make_input_err!("BatchUpdateBlobs: no response for digest")));
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
                        if new_total > Self::MAX_BATCH_TOTAL_SIZE && !batch.is_empty()
                        {
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
            trace!(
                count = num,
                total_size,
                "GrpcStore: flushing batch",
            );

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
                    let channel = cm.connection("find_missing_blobs".into()).await.err_tip(|| "in find_missing_blobs")?;
                    self.cas_client(channel)
                        .find_missing_blobs(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::find_missing_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.cas_client(ch.clone())
                        .find_missing_blobs(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::find_missing_blobs (quic)")
                }
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
        self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_mirror {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-mirror",
                    tonic::metadata::MetadataValue::from_static("1"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm.connection("batch_update_blobs".into()).await.err_tip(|| "in batch_update_blobs")?;
                    self.cas_client(channel)
                        .batch_update_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_update_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.cas_client(ch.clone())
                        .batch_update_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_update_blobs (quic)")
                }
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
        self.perform_request(request, |request| async move {
            let mut grpc_request = Request::new(request);
            if is_worker {
                grpc_request.metadata_mut().insert(
                    "x-nativelink-worker",
                    tonic::metadata::MetadataValue::from_static("true"),
                );
            }
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm.connection("batch_read_blobs".into()).await.err_tip(|| "in batch_read_blobs")?;
                    self.cas_client(channel)
                        .batch_read_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_read_blobs")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.cas_client(ch.clone())
                        .batch_read_blobs(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::batch_read_blobs (quic)")
                }
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
        .await
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
        self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm.connection("get_tree".into()).await.err_tip(|| "in get_tree")?;
                    self.cas_client(channel)
                        .get_tree(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_tree")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.cas_client(ch.clone())
                        .get_tree(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_tree (quic)")
                }
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
        .await
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

    async fn read_internal(
        &self,
        request: ReadRequest,
        prefer_tcp: bool,
    ) -> Result<impl Stream<Item = Result<ReadResponse, Status>> + use<>, Error> {
        let _ = prefer_tcp; // Used only in the Dual transport arm (quic feature)
        let mut grpc_request = Request::new(request);
        if IS_WORKER_REQUEST.try_with(|v| *v).unwrap_or(false) {
            grpc_request.metadata_mut().insert(
                "x-nativelink-worker",
                tonic::metadata::MetadataValue::from_static("true"),
            );
        }
        let mut response = match &self.transport {
            Transport::Tcp(cm) => {
                let channel = cm.connection("bytestream_read".into()).await.err_tip(|| "in read_internal")?;
                self.bs_client(channel)
                    .read(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::read")?
                    .into_inner()
            }
            #[cfg(feature = "quic")]
            Transport::Quic(ch) => {
                self.bs_client(ch.clone())
                    .read(grpc_request)
                    .await
                    .err_tip(|| "in GrpcStore::read (quic)")?
                    .into_inner()
            }
            #[cfg(feature = "quic")]
            Transport::Dual { tcp, quic } => {
                if prefer_tcp {
                    // Parallel chunked reads: prefer TCP (2x faster at
                    // high concurrency)
                    let channel = tcp.connection("bytestream_read".into()).await.err_tip(|| "in read_internal (dual/tcp)")?;
                    self.bs_client(channel)
                        .read(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::read (dual/tcp)")?
                        .into_inner()
                } else {
                    // Single-stream reads: prefer QUIC (2.6x faster)
                    self.bs_client(quic.clone())
                        .read(grpc_request)
                        .await
                        .err_tip(|| "in GrpcStore::read (dual/quic)")?
                        .into_inner()
                }
            }
        };
        let first_response = response
            .message()
            .await
            .err_tip(|| "Fetching first chunk in GrpcStore::read()")?;
        Ok(FirstStream::new(first_response, response))
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
            self.read_internal(request, false).await
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

        // Per-chunk no-progress timeout. Configured via `rpc_timeout_s`
        // but applied per-chunk: each WriteRequest delivered from the
        // upstream stream resets the timer. If no chunk arrives for
        // `rpc_timeout`, the wrapper aborts the RPC with DeadlineExceeded.
        //
        // The previous whole-RPC `tokio::time::timeout` killed legitimate
        // slow-but-progressing mirror writes (a 50 MB blob through a slow
        // Bazel client at 2 MB/s legitimately takes 25s end-to-end). Each
        // such kill broke the >=2-replica durability invariant for the
        // affected blob (641 events on 2026-04-23). Per-chunk progress
        // detects stuck transports without aborting in-flight work.
        let rpc_timeout = self.rpc_timeout;
        let local_state = Arc::new(Mutex::new(WriteState::with_progress_timeout(
            self.instance_name.clone(),
            stream,
            rpc_timeout,
        )));

        let write_start = std::time::Instant::now();
        let instance_name = self.instance_name.clone();
        trace!(
            instance_name = %instance_name,
            rpc_timeout_s = rpc_timeout.as_secs(),
            is_mirror,
            "GrpcStore::write: starting ByteStream write",
        );
        let mut attempt: u32 = 0;
        let result = self
            .retrier
            .retry(unfold(local_state, move |local_state| {
                attempt += 1;
                let instance_name = instance_name.clone();
                async move {
                    // The client write may occur on a separate thread and
                    // therefore in order to share the state with it we have to
                    // wrap it in a Mutex and retrieve it after the write
                    // has completed.  There is no way to get the value back
                    // from the client.
                    trace!(
                        instance_name = %instance_name,
                        attempt,
                        "GrpcStore::write: requesting connection from pool",
                    );
                    let conn_start = std::time::Instant::now();
                    let instance_for_rpc = instance_name.clone();
                    let local_state_for_rpc = local_state.clone();

                    /// Helper: build the tonic Request for a ByteStream write,
                    /// attaching the `x-nativelink-mirror` header when the
                    /// write originates from a server-side mirror operation.
                    fn make_write_request<T, E>(
                        state: Arc<Mutex<WriteState<T, E>>>,
                        is_mirror: bool,
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
                                trace!(
                                    instance_name = %instance_for_rpc,
                                    conn_elapsed_ms,
                                    "GrpcStore::write: got connection, starting ByteStream.Write RPC",
                                );
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(channel)
                                    .write(make_write_request(local_state_for_rpc, is_mirror))
                                    .await
                                    .err_tip(|| "in GrpcStore::write");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                trace!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    "GrpcStore::write: ByteStream.Write RPC returned",
                                );
                                res
                            }
                            #[cfg(feature = "quic")]
                            Transport::Quic(ch) => {
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(ch.clone())
                                    .write(make_write_request(local_state_for_rpc, is_mirror))
                                    .await
                                    .err_tip(|| "in GrpcStore::write (quic)");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                trace!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    "GrpcStore::write: ByteStream.Write RPC returned (quic)",
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
                                trace!(
                                    instance_name = %instance_for_rpc,
                                    conn_elapsed_ms,
                                    "GrpcStore::write: got connection, starting ByteStream.Write RPC (dual/tcp)",
                                );
                                let rpc_start = std::time::Instant::now();
                                let res = self.bs_client(channel)
                                    .write(make_write_request(local_state_for_rpc, is_mirror))
                                    .await
                                    .err_tip(|| "in GrpcStore::write (dual/tcp)");
                                let rpc_elapsed_ms = u64::try_from(
                                    rpc_start.elapsed().as_millis(),
                                )
                                .unwrap_or(u64::MAX);
                                trace!(
                                    instance_name = %instance_for_rpc,
                                    rpc_elapsed_ms,
                                    success = res.is_ok(),
                                    "GrpcStore::write: ByteStream.Write RPC returned (dual/tcp)",
                                );
                                res
                            }
                        }
                    };

                    // Per-chunk progress timeout is enforced inside
                    // WriteStateWrapper::poll_next via WriteState's
                    // progress_deadline; it aborts the RPC by ending the
                    // stream with `read_stream_error` set. No outer
                    // whole-RPC deadline here — that was killing
                    // slow-but-progressing mirror writes and breaking the
                    // >=2-replica durability invariant.
                    let result = rpc_fut.await;

                    // Get the state back from StateWrapper, this should be
                    // uncontended since write has returned.
                    let mut local_state_locked = local_state.lock();

                    let result = local_state_locked
                        .take_read_stream_error()
                        .map(|err| RetryResult::Err(err.append("Where read_stream_error was set")))
                        .unwrap_or_else(|| {
                            // No stream error, handle the original result
                            match result {
                                Ok(response) => RetryResult::Ok(response),
                                Err(ref err)
                                    if err.code == Code::AlreadyExists =>
                                {
                                    RetryResult::Ok(Response::new(WriteResponse {
                                        committed_size: 0,
                                    }))
                                }
                                Err(ref err) => {
                                    warn!(
                                        instance_name = %instance_name,
                                        attempt,
                                        ?err,
                                        can_resume = local_state_locked.can_resume(),
                                        "GrpcStore::write: RPC failed",
                                    );
                                    if local_state_locked.can_resume() {
                                        local_state_locked.resume();
                                        RetryResult::Retry(err.clone())
                                    } else {
                                        RetryResult::Err(
                                            err.clone().append("Retry is not possible"),
                                        )
                                    }
                                }
                            }
                        });

                    drop(local_state_locked);
                    Some((result, local_state))
                }
            }))
            .await?;

        let total_elapsed_ms = u64::try_from(write_start.elapsed().as_millis()).unwrap_or(u64::MAX);
        trace!(
            instance_name = %self.instance_name,
            total_elapsed_ms,
            "GrpcStore::write: completed successfully",
        );
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
                    let channel = cm.connection("query_write_status".into()).await.err_tip(|| "in query_write_status")?;
                    self.bs_client(channel)
                        .query_write_status(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::query_write_status")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.bs_client(ch.clone())
                        .query_write_status(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::query_write_status (quic)")
                }
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
        self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm.connection("get_action_result".into()).await.err_tip(|| "in get_action_result")?;
                    self.ac_client(channel)
                        .get_action_result(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_action_result")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.ac_client(ch.clone())
                        .get_action_result(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::get_action_result (quic)")
                }
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // AC lookup: prefer QUIC
                    self.ac_client(quic.clone())
                        .get_action_result(Request::new(request))
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
        self.perform_request(request, |request| async move {
            match &self.transport {
                Transport::Tcp(cm) => {
                    let channel = cm.connection("update_action_result".into()).await.err_tip(|| "in update_action_result")?;
                    self.ac_client(channel)
                        .update_action_result(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::update_action_result")
                }
                #[cfg(feature = "quic")]
                Transport::Quic(ch) => {
                    self.ac_client(ch.clone())
                        .update_action_result(Request::new(request))
                        .await
                        .err_tip(|| "in GrpcStore::update_action_result (quic)")
                }
                #[cfg(feature = "quic")]
                Transport::Dual { quic, .. } => {
                    // Small AC update: prefer QUIC
                    self.ac_client(quic.clone())
                        .update_action_result(Request::new(request))
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
        }

        let local_state = LocalState {
            resource_name,
            writer,
            read_offset: i64::try_from(offset)
                .err_tip(|| "Could not convert offset to i64")?,
            read_limit: i64::try_from(length.unwrap_or(0))
                .err_tip(|| "Could not convert length to i64")?,
            bytes_received_this_stream: 0,
            last_frame_at: std::time::Instant::now(),
            attempt: 0,
        };

        let result = self.retrier
            .retry(unfold(local_state, move |mut local_state| async move {
                local_state.attempt += 1;
                let attempt_start = std::time::Instant::now();
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
                let mut stream = match self
                    .read_internal(request, false)
                    .await
                    .err_tip(|| "in GrpcStore::get_part()")
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        info!(
                            resource_name = %local_state.resource_name,
                            attempt = local_state.attempt,
                            attempt_elapsed_ms = attempt_start.elapsed().as_millis() as u64,
                            code = ?err.code,
                            "GrpcStore::get_part_single_stream read_internal failed",
                        );
                        return Some((RetryResult::Retry(err), local_state))
                    }
                };

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
                            return Some((
                                RetryResult::Retry(
                                    Into::<Error>::into(status).append(
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
                        // BUG NOTE: 0-byte successful responses from workers
                        //
                        // When a worker's store layer has a digest in its
                        // existence cache but the actual blob data was evicted,
                        // get_part() may send EOF without any data. The
                        // ByteStream server produces a successful empty gRPC
                        // stream (0 ReadResponse messages). On the client side,
                        // read_internal() calls message().await which returns
                        // Ok(None), and FirstStream yields an empty stream.
                        // We land here having written 0 bytes in this stream
                        // attempt — a silent data loss.
                        //
                        // If no bytes were received in this stream attempt,
                        // this is almost certainly a stale worker response,
                        // not a legitimate empty blob. Return a retryable
                        // error. This correctly handles retries at offset > 0.
                        if local_state.bytes_received_this_stream == 0 {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::NotFound,
                                    "GrpcStore: ByteStream returned 0 bytes \
                                     for non-empty blob (stale worker data?) — \
                                     not found in remote store"
                                )),
                                local_state,
                            ));
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
                        return Some((RetryResult::Err(err), local_state));
                    }
                    local_state.read_offset += length;
                    local_state.bytes_received_this_stream += length;
                }
            }))
            .await;
        let elapsed_ms = entry_at.elapsed().as_millis() as u64;
        match &result {
            Ok(()) => info!(
                elapsed_ms,
                "GrpcStore::get_part_single_stream exit Ok",
            ),
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
    async fn get_part_parallel(
        &self,
        resource_name: &str,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        total_length: u64,
    ) -> Result<(), Error> {
        let chunk_count = self.parallel_chunk_count;
        let base_chunk_size = total_length / chunk_count;
        let remainder = total_length % chunk_count;
        let read_start = std::time::Instant::now();
        info!(
            %resource_name,
            offset,
            total_length,
            chunk_count,
            "GrpcStore::get_part_parallel entry",
        );

        // Build chunk descriptors: (chunk_offset, chunk_length).
        let mut chunks: Vec<(u64, u64)> =
            Vec::with_capacity(chunk_count as usize);
        let mut current_offset = offset;
        for i in 0..chunk_count {
            let this_chunk =
                base_chunk_size + if i < remainder { 1 } else { 0 };
            if this_chunk == 0 {
                break;
            }
            chunks.push((current_offset, this_chunk));
            current_offset += this_chunk;
        }

        let actual_chunk_count = chunks.len();

        // Create a bounded channel per chunk. Fetch tasks push data
        // into their channel as it arrives from the gRPC stream;
        // the writer drains channels sequentially (ch0 then ch1 …).
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..actual_chunk_count)
                .map(|_| {
                    tokio::sync::mpsc::channel::<Bytes>(
                        Self::PARALLEL_CHUNK_CHANNEL_SIZE,
                    )
                })
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
                                            let mut stream = match self
                                                .read_internal(
                                                    request, true,
                                                )
                                                .await
                                            {
                                                Ok(s) => s,
                                                Err(err) => {
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
                    writer.send(data).await.err_tip(|| {
                        "while writing parallel chunk data"
                    })?;
                }
            }
            Result::<u64, Error>::Ok(total_bytes)
        };

        let (fetch_result, write_result) =
            tokio::join!(fetch_all, write_all);
        // Check both — fetch errors take priority since they indicate
        // upstream data issues; write errors indicate downstream
        // backpressure or client disconnect.
        fetch_result
            .err_tip(|| "in GrpcStore::get_part_parallel fetch")?;
        let total_bytes = write_result
            .err_tip(|| "in GrpcStore::get_part_parallel write")?;

        writer
            .send_eof()
            .err_tip(|| "could not send eof in get_part_parallel")?;

        let elapsed = read_start.elapsed();
        let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
            (total_bytes as f64 / (1024.0 * 1024.0))
                / elapsed.as_secs_f64()
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

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
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
            let mut results =
                self.do_batch_update(&digests, vec![(digest, data)]).await;
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
            let offset = usize::try_from(offset)
                .err_tip(|| "Could not convert offset to usize")?;
            let length = length
                .map(|v| {
                    usize::try_from(v)
                        .err_tip(|| "Could not convert length to usize")
                })
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
        let effective_length = length.unwrap_or_else(|| {
            digest.size_bytes().saturating_sub(offset)
        });

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
                )
                .await;
        }

        // Single-stream path for small blobs or when parallel reads
        // are disabled.
        self.get_part_single_stream(
            resource_name,
            writer,
            offset,
            length,
        )
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
}

default_health_status_indicator!(GrpcStore);

#[cfg(test)]
mod tests {
    use super::{ChunkAttemptOutcome, classify_chunk_attempt};

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
            /*clean_eof=*/ true,
            /*bytes_received=*/ 183,
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
            /*clean_eof=*/ false,
            /*bytes_received=*/ 100,
            /*chunk_length=*/ 1024,
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
}
