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

//! AC peer-fetch read-side wrapper.
//!
//! Server-side store that wraps the AC chain with a NotFound→peer-fetch
//! redirect: when the inner AC chain returns `Code::NotFound`, the wrapper
//! consults the [`AcPinRegistry`] for workers that have advertised the
//! same digest under any AC store, opens a `StoreType::Ac` gRPC connection
//! to that worker, and streams the bytes back through the original
//! writer.
//!
//! # Why this is a separate wrapper from [`crate::worker_proxy_store::WorkerProxyStore`]
//!
//! The CAS [`crate::worker_proxy_store::WorkerProxyStore`] consults
//! [`nativelink_util::blob_locality_map::BlobLocalityMap`]. AC pins are
//! held in [`nativelink_util::ac_pin_registry::AcPinRegistry`] —
//! intentionally a separate data structure (see
//! [`nativelink_util::ac_pin_registry`] module docs). Routing AC-pin
//! lookups through the CAS locality map would weaponize the upload
//! short-circuits in `bytestream_server::write` and
//! `cas_server::batch_update_blobs`, because Bazel re-uses the same
//! digest for an Action proto in CAS that the server registered as an
//! AC pin. A short-circuit on AC-pin presence would silently swallow
//! the CAS upload of the Action proto bytes — permanent data loss.
//!
//! The peer-fetch wrapper exists ONLY on the AC chain. On the AC chain
//! the digest is the AC entry's `action_digest`, which is just an
//! address — there is no CAS-upload short-circuit on the AC path, so
//! AC-pin presence cannot weaponize anything.
//!
//! # Has-pass-through (no AC-pin consultation)
//!
//! `has_with_results` for the AC chain DOES NOT consult the AC pin
//! registry. AC has-checks have no production caller that benefits from
//! the locality fast path the way Bazel's `FindMissingBlobs` does for
//! CAS, and a stale-positive on a borrowed digest could (per the spec
//! reasoning above) feed back into a CAS-side path through misuse. The
//! wrapper deliberately reports only what the inner AC chain reports.
//!
//! # Update-pass-through (no AC-pin consultation)
//!
//! `update` always writes to the inner store. Even though some upload
//! short-circuit MIGHT be safe to add in the future, the current code
//! intentionally never short-circuits AC writes against AC pins.

use core::pin::Pin;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, trace, warn};

use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::ac_pin_registry::SharedAcPinRegistry;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};

use crate::grpc_store::GrpcStore;

/// Server-side AC peer-fetch wrapper. See module docs for design
/// rationale and isolation contract relative to the CAS path.
#[derive(MetricsComponent)]
pub struct AcProxyStore {
    /// Inner AC chain (typically `CompletenessCheckingStore →
    /// FastSlowStore { fast: Memory, slow: RefStore→Redis }` in
    /// production, or a `FilesystemStore` for the AC-on-disk
    /// configuration).
    #[metric(group = "inner_store")]
    inner: Store,
    /// Read-side index into worker AC pin advertisements. Populated by
    /// [`nativelink_service::worker_api_server::WorkerApiServer::handle_blobs_available`]
    /// from `BlobsAvailableNotification.pinned_ac_mirror_entries`
    /// (proto field 17).
    registry: SharedAcPinRegistry,
    /// Cached `StoreType::Ac` gRPC connections to worker endpoints.
    /// Populated lazily on the first AC redirect to that endpoint.
    /// Boot-epoch flips require the consumer to call
    /// [`Self::remove_worker_endpoint`] to drop the stale connection.
    worker_connections: RwLock<HashMap<Arc<str>, Store>>,
    /// Optional TLS config for connecting to worker AC endpoints.
    /// `None` means plaintext (`grpc://`).
    worker_tls_config: Option<ClientTlsConfig>,
}

impl core::fmt::Debug for AcProxyStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcProxyStore")
            .field("inner", &self.inner)
            .field("worker_connections", &self.worker_connections.read().len())
            .finish()
    }
}

impl AcProxyStore {
    /// Construct a new wrapper around `inner` consulting `registry` on
    /// inner-NotFound. Plaintext worker connections.
    pub fn new(inner: Store, registry: SharedAcPinRegistry) -> Arc<Self> {
        Arc::new(Self {
            inner,
            registry,
            worker_connections: RwLock::new(HashMap::new()),
            worker_tls_config: None,
        })
    }

    /// Variant of [`Self::new`] that connects to worker AC endpoints
    /// over `grpcs://` using `tls_config`.
    pub fn new_with_tls(
        inner: Store,
        registry: SharedAcPinRegistry,
        tls_config: ClientTlsConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            registry,
            worker_connections: RwLock::new(HashMap::new()),
            worker_tls_config: Some(tls_config),
        })
    }

    /// Inner AC chain accessor (used by the bin's wiring to verify the
    /// wrapper preserves the chain it was given).
    pub fn inner_store_handle(&self) -> &Store {
        &self.inner
    }

    /// Drop a cached connection — used by the parent on boot-epoch
    /// flip to ensure the next AC peer-fetch creates a fresh channel.
    pub fn remove_worker_endpoint(&self, endpoint: &str) {
        let mut conns = self.worker_connections.write();
        if conns.remove(endpoint).is_some() {
            debug!(endpoint, "AcProxyStore: removed worker connection");
        }
    }

    /// Inject a pre-built [`Store`] as a worker connection (test seam).
    /// Production calls construct connections lazily via
    /// [`Self::get_or_create_connection`].
    pub fn inject_worker_connection(&self, endpoint: &str, store: Store) {
        self.worker_connections
            .write()
            .insert(Arc::from(endpoint), store);
    }

    fn get_worker_connection(&self, endpoint: &str) -> Option<Store> {
        self.worker_connections.read().get(endpoint).cloned()
    }

    /// Look up (or create) a `StoreType::Ac` gRPC connection to
    /// `endpoint`. Returns `None` if the connection couldn't be
    /// established — caller treats this the same as "no peer
    /// available" and falls through to NotFound.
    async fn get_or_create_connection(&self, endpoint: &str) -> Option<Store> {
        if let Some(store) = self.get_worker_connection(endpoint) {
            return Some(store);
        }
        match self.create_worker_connection(endpoint).await {
            Ok(store) => {
                let mut conns = self.worker_connections.write();
                Some(
                    conns
                        .entry(Arc::from(endpoint))
                        .or_insert_with(|| store.clone())
                        .clone(),
                )
            }
            Err(e) => {
                trace!(endpoint, ?e, "AcProxyStore: failed to connect to peer");
                None
            }
        }
    }

    /// Build a `StoreType::Ac` GrpcStore against `endpoint`. The AC
    /// peer-fetch path is read-only from this side, but the underlying
    /// `GrpcStore` impl gates AC-vs-CAS routing on `store_type` so we
    /// MUST configure it correctly — otherwise the read would be
    /// dispatched against the worker's CAS server, which is the very
    /// digest-collision footgun this whole module exists to avoid.
    async fn create_worker_connection(&self, endpoint: &str) -> Result<Store, Error> {
        let spec = GrpcSpec {
            instance_name: String::new(),
            endpoints: vec![GrpcEndpoint {
                address: endpoint.to_string(),
                tls_config: self.worker_tls_config.clone(),
                concurrency_limit: None,
                connect_timeout_s: 5,
                tcp_keepalive_s: 30,
                http2_keepalive_interval_s: 30,
                http2_keepalive_timeout_s: 60,
                tcp_nodelay: true,
                use_http3: false,
            }],
            store_type: StoreType::Ac,
            retry: Retry::default(),
            max_concurrent_requests: 0,
            connections_per_endpoint: 8,
            // 15s, matching the CAS sibling's worker connection timeout
            // (`worker_proxy_store::create_worker_connection`). AC reads
            // are tiny (<<1 MiB Action proto / ActionResult proto) so a
            // wider timeout would only mask a wedged peer.
            rpc_timeout_s: 15,
            // AC entries are point reads of small protos — none of the
            // CAS hot-path knobs apply. Defaults disable batching /
            // chunked / dual transport / compression.
            batch_update_threshold_bytes: 0,
            max_concurrent_batch_rpcs: 0,
            parallel_chunk_read_threshold: 0,
            parallel_chunk_count: 0,
            dual_transport: false,
            zstd_compression: false,
            connection_acquire_timeout_ms: Some(3000),
            chunked_writes_enabled: false,
        };
        let store = GrpcStore::new(&spec)
            .await
            .err_tip(|| format!("Creating AC peer connection to {endpoint}"))?;
        Ok(Store::new(store))
    }

    /// Iterate registered endpoints, returning every worker that has
    /// advertised `digest` in its AC pin set under any `store_id`.
    /// Iteration cost is O(workers × pins-per-worker) per call — for
    /// the production fleet (~10 workers × ~100K pins) this snapshots
    /// per worker but the snapshot is only consulted on the
    /// inner-NotFound slow path, so the cost is paid only when the
    /// fast path already missed.
    fn endpoints_holding(&self, digest: &DigestInfo) -> Vec<Arc<str>> {
        let counts = self.registry.endpoint_counts();
        let mut hits: Vec<Arc<str>> = Vec::new();
        for endpoint in counts.keys() {
            if let Some(snapshot) = self.registry.snapshot_endpoint(endpoint) {
                if snapshot.iter().any(|(_, d)| d == digest) {
                    hits.push(Arc::from(endpoint.as_str()));
                }
            }
        }
        hits
    }

    /// Try every worker the registry says holds this digest, in
    /// registry-iteration order. Returns `Ok(true)` on the first
    /// successful peer-fetch, `Ok(false)` if no peer was reachable
    /// or held the bytes, and `Err` only if a peer succeeded with a
    /// streaming error after partial bytes (in which case the
    /// caller's writer is poisoned and we cannot retry the next peer
    /// without corrupting the consumer stream).
    ///
    /// On per-peer NotFound the registry entry is removed so the next
    /// AC read for the same digest skips that peer. Other peer errors
    /// (transport blip, transient) leave the registry unchanged — the
    /// peer may still hold the bytes; we don't penalise locality on a
    /// network hiccup.
    async fn try_read_from_peer(
        &self,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<bool, Error> {
        let digest = key.borrow().into_digest();
        let endpoints = self.endpoints_holding(&digest);
        if endpoints.is_empty() {
            trace!(?digest, "AcProxyStore: no peers in AC pin registry");
            return Ok(false);
        }

        let bytes_before_peers = writer.get_bytes_written();

        for endpoint in &endpoints {
            let Some(store) = self.get_or_create_connection(endpoint).await else {
                debug!(?digest, endpoint = %endpoint, "AcProxyStore: peer connect failed; trying next");
                continue;
            };

            let bytes_before_this_peer = writer.get_bytes_written();
            let peer_result = store
                .get_part(key.borrow(), &mut *writer, offset, length)
                .await;
            match peer_result {
                Ok(()) => {
                    debug!(
                        ?digest,
                        endpoint = %endpoint,
                        bytes_written = writer.get_bytes_written() - bytes_before_this_peer,
                        "AcProxyStore: AC peer fetch succeeded"
                    );
                    return Ok(true);
                }
                Err(e) => {
                    let bytes_written_this_peer =
                        writer.get_bytes_written() - bytes_before_this_peer;
                    if bytes_written_this_peer > 0 {
                        // Mid-stream failure after partial bytes —
                        // surfacing the original error is the only
                        // safe action; trying the next peer would
                        // overlap bytes onto the consumer stream.
                        return Err(make_err!(
                            e.code,
                            "AcProxyStore: peer {} wrote {} bytes then failed with {:?} ({}); \
                             cannot retry next peer without corrupting consumer stream",
                            endpoint,
                            bytes_written_this_peer,
                            e.code,
                            e.message_string()
                        ));
                    }

                    if e.code == Code::NotFound {
                        // Peer evicted / never had it. Drop the
                        // registry entry across all store_ids on
                        // this endpoint so subsequent AC reads skip
                        // straight to the next holder.
                        self.registry
                            .remove_digests_for_endpoint(endpoint, &[digest]);
                        debug!(
                            ?digest,
                            endpoint = %endpoint,
                            "AcProxyStore: peer NotFound — evicted AC pin entry"
                        );
                    } else {
                        warn!(
                            ?digest,
                            endpoint = %endpoint,
                            code = ?e.code,
                            ?e,
                            "AcProxyStore: peer fetch failed transiently; trying next peer"
                        );
                    }
                    continue;
                }
            }
        }

        // No peer held the bytes. If we wrote ANYTHING the caller's
        // writer is no longer pristine; surface a defensive error.
        // (In practice the partial-bytes branch above returned earlier
        // when this was true, so this is belt-and-suspenders.)
        if writer.get_bytes_written() > bytes_before_peers {
            return Err(make_err!(
                Code::Internal,
                "AcProxyStore: every peer failed but writer was already partially written; \
                 cannot fall through to NotFound without corrupting consumer stream"
            ));
        }
        Ok(false)
    }
}

#[async_trait]
impl StoreDriver for AcProxyStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Pass through. Deliberately does NOT consult the AC pin
        // registry — see module docs.
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Pass through. Writes never short-circuit against AC pins.
        self.inner.update(key, reader, upload_size).await
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
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
        // Capture the writer's byte position so we can detect
        // mid-stream failures from the inner store and refuse to
        // peer-fetch (peer would write the full blob again, producing
        // a corrupt prefix-from-inner + full-peer-copy stream).
        let bytes_before_inner = writer.get_bytes_written();
        let inner_result = self.inner.get_part(key.borrow(), &mut *writer, offset, length).await;
        match inner_result {
            Ok(()) => return Ok(()),
            Err(e) if e.code == Code::NotFound => {
                let bytes_written_by_inner = writer.get_bytes_written() - bytes_before_inner;
                if bytes_written_by_inner > 0 {
                    // Partial bytes already on the wire — the inner
                    // store violated its own contract by sending
                    // bytes before NotFound, but we cannot recover
                    // by trying peers now.
                    return Err(make_err!(
                        e.code,
                        "AcProxyStore: inner store wrote {bytes_written_by_inner} bytes \
                         then returned NotFound; cannot peer-fetch without corrupting \
                         consumer stream",
                    ));
                }
                trace!(
                    digest = ?key.borrow().into_digest(),
                    "AcProxyStore: inner NotFound — consulting AC pin registry"
                );
            }
            Err(e) => return Err(e),
        }

        // Inner returned NotFound with no bytes written. Try each
        // peer that has advertised the digest.
        if self
            .try_read_from_peer(key.borrow(), writer, offset, length)
            .await?
        {
            return Ok(());
        }

        // No peer held it either — surface a clean NotFound. The
        // wrapper does NOT re-issue the inner call here (no race
        // window benefit on AC).
        let digest = key.borrow().into_digest();
        Err(make_err!(
            Code::NotFound,
            "AcProxyStore: AC entry {digest:?} not found in inner store or any peer"
        ))
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &dyn StoreDriver {
        // Delegate to inner so callers can downcast through the chain
        // (e.g. `ac_server` reaching through to `GrpcStore` for the
        // `get_action_result`-shortcut codepath).
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

    /// Single-inner wrapper: the proxy adds peer-fetch on top of the
    /// inner AC chain but does not own the BIS / pin chain.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

#[async_trait]
impl HealthStatusIndicator for AcProxyStore {
    fn get_name(&self) -> &'static str {
        "AcProxyStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        self.inner.check_health(namespace).await
    }
}
