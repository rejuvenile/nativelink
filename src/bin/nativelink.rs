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

use core::net::SocketAddr;
use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_lock::Mutex as AsyncMutex;
use axum::Router;
use axum::http::Uri;
use clap::Parser;
use futures::FutureExt;
use futures::future::{BoxFuture, OptionFuture, TryFutureExt, try_join_all};
use hyper::StatusCode;
use hyper_util::rt::TokioTimer;
use hyper_util::rt::tokio::TokioIo;
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use mimalloc::MiMalloc;
use nativelink_config::cas_server::{
    CasConfig, GlobalConfig, HttpCompressionAlgorithm, ListenerConfig, SchedulerConfig,
    ServerConfig, StoreConfig, WorkerConfig,
};
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_service::ac_server::AcServer;
use nativelink_service::bep_server::BepServer;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_service::fetch_server::FetchServer;
use nativelink_service::health_server::HealthServer;
use nativelink_service::push_server::PushServer;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_util::blob_locality_map;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::fs::set_open_file_limit;
use nativelink_util::digest_hasher::{DigestHasherFunc, set_default_digest_hasher_func};
use nativelink_util::health_utils::HealthRegistryBuilder;
use nativelink_util::origin_event_publisher::OriginEventPublisher;
#[cfg(target_family = "unix")]
use nativelink_util::shutdown_guard::Priority;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::store_trait::{
    DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG, set_default_digest_size_health_check,
};
use nativelink_util::task::TaskExecutor;
use nativelink_util::telemetry::init_tracing;
use nativelink_util::{background_spawn, fs, spawn};

/// Global store manager reference for graceful shutdown flush.
static STORE_MANAGER: std::sync::OnceLock<Arc<StoreManager>> = std::sync::OnceLock::new();
use nativelink_worker::local_worker::new_local_worker;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateRevocationListDer, PrivateKeyDer};
use socket2::SockRef;
use tokio::net::TcpListener;
use tokio::select;
#[cfg(target_family = "unix")]
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot::Sender;
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig as TlsServerConfig};
use tonic::codec::CompressionEncoding;
use tonic::service::Routes;
#[cfg(feature = "quic")]
use {quinn, tonic_h3};
use tracing::{debug, error, error_span, info, trace_span, warn};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Note: This must be kept in sync with the documentation in `AdminConfig::path`.
const DEFAULT_ADMIN_API_PATH: &str = "/admin";

// Note: This must be kept in sync with the documentation in `HealthConfig::path`.
const DEFAULT_HEALTH_STATUS_CHECK_PATH: &str = "/status";

// Note: This must be kept in sync with the documentation in
// `OriginEventsConfig::max_event_queue_size`.
const DEFAULT_MAX_QUEUE_EVENTS: usize = 0x0001_0000;

/// Broadcast Channel Capacity
/// Note: The actual capacity may be greater than the provided capacity.
const BROADCAST_CAPACITY: usize = 1;

/// Backend for bazel remote execution / cache API.
#[derive(Parser, Debug)]
#[clap(
    author = "Trace Machina, Inc. <nativelink@tracemachina.com>",
    version,
    about,
    long_about = None
)]
struct Args {
    /// Config file to use.
    #[clap(value_parser)]
    config_file: String,
}

trait RoutesExt {
    fn add_optional_service<S>(self, svc: Option<S>) -> Self
    where
        S: tower::Service<
                axum::http::Request<tonic::body::Body>,
                Error = core::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static;
}

impl RoutesExt for Routes {
    fn add_optional_service<S>(mut self, svc: Option<S>) -> Self
    where
        S: tower::Service<
                axum::http::Request<tonic::body::Body>,
                Error = core::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        if let Some(svc) = svc {
            self = self.add_service(svc);
        }
        self
    }
}

/// If this value changes update the documentation in the config definition.
const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Server-side encoding (response) limit.  Bazel's Java gRPC client defaults
/// to 4 MiB max inbound message size, so we default to 4 MiB.  Workers that
/// need larger responses should use a separate listener with a higher
/// `max_encoding_message_size` in the config.
const DEFAULT_MAX_ENCODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

async fn inner_main(
    cfg: CasConfig,
    shutdown_tx: broadcast::Sender<ShutdownGuard>,
    scheduler_shutdown_tx: Sender<()>,
    #[cfg(target_family = "unix")] scheduler_shutdown_rx: oneshot::Receiver<()>,
    #[cfg(target_family = "unix")] mut shutdown_guard: ShutdownGuard,
) -> Result<(), Error> {
    const fn into_encoding(from: HttpCompressionAlgorithm) -> Option<CompressionEncoding> {
        match from {
            HttpCompressionAlgorithm::Gzip => Some(CompressionEncoding::Gzip),
            HttpCompressionAlgorithm::Zstd => Some(CompressionEncoding::Zstd),
            HttpCompressionAlgorithm::None => None,
        }
    }

    let health_registry_builder =
        Arc::new(AsyncMutex::new(HealthRegistryBuilder::new("nativelink")));

    let store_manager = Arc::new(StoreManager::new());
    {
        let mut health_registry_lock = health_registry_builder.lock().await;

        for StoreConfig { name, spec } in cfg.stores {
            let health_component_name = format!("stores/{name}");
            let mut health_register_store =
                health_registry_lock.sub_builder(&health_component_name);
            let store = store_factory(&spec, &store_manager, Some(&mut health_register_store))
                .await
                .err_tip(|| format!("Failed to create store '{name}'"))?;
            store_manager.add_store(&name, store);
        }
    }
    STORE_MANAGER.set(store_manager.clone()).ok();

    let mut root_futures: Vec<BoxFuture<Result<(), Error>>> = Vec::new();

    let maybe_origin_event_tx = cfg
        .experimental_origin_events
        .as_ref()
        .map(|origin_events_cfg| {
            let mut max_queued_events = origin_events_cfg.max_event_queue_size;
            if max_queued_events == 0 {
                max_queued_events = DEFAULT_MAX_QUEUE_EVENTS;
            }
            let (tx, rx) = mpsc::channel(max_queued_events);
            let store_name = origin_events_cfg.publisher.store.as_str();
            let store = store_manager.get_store(store_name).err_tip(|| {
                format!("Could not get store {store_name} for origin event publisher")
            })?;

            root_futures.push(Box::pin(
                OriginEventPublisher::new(store, rx, shutdown_tx.clone())
                    .run()
                    .map(Ok),
            ));

            Ok::<_, Error>(tx)
        })
        .transpose()?;

    // Create a shared blob locality map for peer-to-peer blob sharing.
    // This map is shared between the scheduler (for locality scoring and
    // peer hint generation) and WorkerApiServer (for receiving
    // BlobsAvailable updates from workers).
    let locality_map = blob_locality_map::new_shared_blob_locality_map();

    // Server-side AC pin registry — completely separate from the
    // CAS-shared `BlobLocalityMap` so AC pin advertisements (proto
    // field 17 `pinned_ac_mirror_entries`) cannot weaponize the CAS
    // upload short-circuits in `bytestream_server::write` /
    // `cas_server::batch_update_blobs` even on action_digest
    // collisions. No read-side consumer in this commit (advertisement
    // channel only).
    let ac_pin_registry =
        nativelink_util::ac_pin_registry::new_shared_ac_pin_registry();

    // Build TLS config for server-to-worker connections (used by both the
    // scheduler's prefetch path and WorkerProxyStore).
    let worker_proxy_tls: Option<nativelink_config::stores::ClientTlsConfig> =
        cfg.global.as_ref().and_then(|g| {
            g.worker_proxy_tls_ca_file.as_ref().map(|ca| {
                nativelink_config::stores::ClientTlsConfig {
                    ca_file: Some(ca.clone()),
                    cert_file: g.worker_proxy_tls_cert_file.clone(),
                    key_file: g.worker_proxy_tls_key_file.clone(),
                    use_native_roots: Some(false),
                }
            })
        });

    // #261 fix: the WorkerProxyStore wrap MUST happen BEFORE
    // `scheduler_factory` runs so that the scheduler captures a clone of
    // the WRAPPED `cas_store` (with peer-fetch fallback) rather than the
    // raw `unwrapped_cas_stores` entry. Otherwise tree-resolution in
    // `ApiWorkerScheduler::resolve_directory_for_input_root` walks
    // `SizePartitioning → Memory → Filesystem` only and surfaces NotFound
    // for tiny Directory blobs that live on a peer worker (because
    // `bytestream_server`'s fast-path skipped the server-side persist on
    // the strength of `WorkerProxyStore::has() = Some`).
    //
    // The wrap REPLACES the entry in `store_manager` (HashMap insert),
    // so the next `store_manager.get_store(name)` call inside
    // `scheduler_factory` resolves to the wrapped Arc.
    let server_cfgs: Vec<ServerConfig> = cfg.servers.into_iter().collect();

    // Wrap CAS stores with WorkerProxyStore so the server can proxy reads
    // to workers that have the blob (discovered via BlobsAvailable reports).
    // Save the original (unwrapped) CAS store for backfill existence checks
    // so that has_with_results goes directly to the real store, not through
    // WorkerProxyStore which would consider blobs on workers as "present".
    let mut unwrapped_cas_stores: HashMap<String, nativelink_util::store_trait::Store> =
        HashMap::new();
    // Per-store WorkerProxyStore Arcs so WorkerApiServer can call
    // `record_mirror_capacity` (review #1).
    let mut worker_proxy_stores: HashMap<
        String,
        Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>,
    > = HashMap::new();
    // #212 v4.5: per-CAS-store ChunkedWriteHandler instances keyed by
    // store_name. Populated below in the chunked-dispatcher wiring
    // block when the slow tier is a direct
    // FilesystemStore<FileEntryImpl>. Consumed by the per-listener
    // loop to register a `CasExtensionsServer` on every listener that
    // hosts that CAS store. Empty when the `chunked_fast_slow`
    // feature is OFF (the type is still declared so the per-listener
    // loop can be feature-uniform).
    #[cfg(feature = "chunked_fast_slow")]
    let mut chunked_write_handlers: HashMap<
        String,
        Arc<
            nativelink_service::chunked_write_handler::ChunkedWriteHandler<
                nativelink_store::filesystem_store::FileEntryImpl,
            >,
        >,
    > = HashMap::new();
    let cas_store_names: HashSet<String> = {
        let mut names: HashSet<String> = HashSet::new();
        for server_cfg in &server_cfgs {
            if let Some(ref services) = server_cfg.services {
                if let Some(ref cas_cfgs) = services.cas {
                    for c in cas_cfgs {
                        names.insert(c.config.cas_store.clone());
                    }
                }
                if let Some(ref bs_cfgs) = services.bytestream {
                    for c in bs_cfgs {
                        names.insert(c.config.cas_store.clone());
                    }
                }
            }
        }
        for store_name in &names {
            if let Some(original_store) = store_manager.get_store(store_name) {
                // Save the unwrapped store before replacing it with
                // the WorkerProxyStore wrapper.
                unwrapped_cas_stores.insert(store_name.clone(), original_store.clone());
                let proxy_arc = if let Some(ref tls) = worker_proxy_tls {
                    nativelink_store::worker_proxy_store::WorkerProxyStore::new_with_tls(
                        original_store,
                        locality_map.clone(),
                        tls.clone(),
                    )
                } else {
                    nativelink_store::worker_proxy_store::WorkerProxyStore::new(
                        original_store,
                        locality_map.clone(),
                    )
                };
                // #88: pre-initialize the BatchReadCoalescer so the
                // operator-flippable batch-small-blob-reads kill-switch
                // can engage at runtime without first triggering
                // construction. Default-OFF — only active when
                // `enable_batch_small_blob_reads` is called.
                proxy_arc.init_batch_read_coalescer();
                // #88: enable opportunistic BatchReadBlobs coalescing for
                // small-blob server→worker proxy reads. Concurrent
                // same-target small-blob fetches collapse into one
                // BatchReadBlobs RPC per coalesce window per endpoint
                // instead of N individual ByteStream Read RPCs. User
                // sign-off 2026-05-05 (operator authorization).
                proxy_arc.enable_batch_small_blob_reads();
                worker_proxy_stores.insert(store_name.clone(), proxy_arc.clone());
                let proxy_store = nativelink_util::store_trait::Store::new(proxy_arc);
                store_manager.add_store(store_name, proxy_store);
                info!(
                    store_name,
                    worker_proxy_tls = worker_proxy_tls.is_some(),
                    "wrapped CAS store with WorkerProxyStore for peer blob sharing"
                );
            }
        }
        names
    };

    // Collect AC store names from `services.ac` configs so we can
    // include them in the BIS broadcast loop's drain (Option A2 drain
    // channel for the AC pin lifecycle). When the AC store is itself a
    // `FastSlowStore` (production: `AC_STORE → CompletenessChecking →
    // FastSlow{ fast: Memory, slow: RefStore→Redis }`), its slow-tier
    // write completion produces a `stable_digest` push, the BIS loop
    // drains it, and the broadcast triggers `remove_local_ac_pins` on
    // matching workers via `handle_blobs_in_stable_storage`.
    //
    // We harvest the `services.ac` set rather than re-scanning the
    // whole `store_manager` so we only iterate stores the operator
    // explicitly exposed as AC services.
    let ac_store_names: HashSet<String> = {
        let mut names: HashSet<String> = HashSet::new();
        for server_cfg in &server_cfgs {
            if let Some(ref services) = server_cfg.services {
                if let Some(ref ac_cfgs) = services.ac {
                    for c in ac_cfgs {
                        names.insert(c.config.ac_store.clone());
                    }
                }
            }
        }
        names
    };

    // Wrap each AC store with AcProxyStore so an inner-NotFound on
    // the AC chain transparently consults the AcPinRegistry and
    // peer-fetches the AC entry bytes from the worker that pinned
    // them. The wrap MUST happen here — after the CAS wrap loop and
    // after `ac_pin_registry` is constructed — so the wrapper is in
    // place before any service handler binds to the store via
    // `store_manager.get_store(...)`. AcServer is opaque to the
    // wrapper (it consumes a `Store`); no handler change is needed.
    //
    // The wrapper is a no-op when no AC pin registry is wired (tests
    // / standalone). Production always supplies one.
    for store_name in &ac_store_names {
        if let Some(original_store) = store_manager.get_store(store_name) {
            let proxy_arc = if let Some(ref tls) = worker_proxy_tls {
                nativelink_store::ac_proxy_store::AcProxyStore::new_with_tls(
                    original_store,
                    ac_pin_registry.clone(),
                    tls.clone(),
                )
            } else {
                nativelink_store::ac_proxy_store::AcProxyStore::new(
                    original_store,
                    ac_pin_registry.clone(),
                )
            };
            // Register a wipe callback so the proxy's worker_connections
            // cache is cleared adjacent to the registry wipe on
            // boot-epoch flip. Captures a Weak<AcProxyStore> so the
            // callback (held by the registry) does NOT keep the proxy
            // alive past store_manager's lifetime.
            let proxy_weak = std::sync::Arc::downgrade(&proxy_arc);
            ac_pin_registry.on_endpoint_wipe(std::sync::Arc::new(
                move |endpoint: &str| {
                    if let Some(p) = proxy_weak.upgrade() {
                        p.remove_worker_endpoint(endpoint);
                    }
                },
            ));
            let proxy_store = nativelink_util::store_trait::Store::new(proxy_arc);
            store_manager.add_store(store_name, proxy_store);
            info!(
                store_name,
                worker_proxy_tls = worker_proxy_tls.is_some(),
                "wrapped AC store with AcProxyStore for peer AC entry sharing"
            );
        }
    }

    let mut action_schedulers = HashMap::new();
    let mut worker_schedulers = HashMap::new();
    for SchedulerConfig { name, spec } in cfg.schedulers.iter().flatten() {
        let (maybe_action_scheduler, maybe_worker_scheduler) =
            scheduler_factory(spec, &store_manager, maybe_origin_event_tx.as_ref(), Some(locality_map.clone()), worker_proxy_tls.clone())
                .await
                .err_tip(|| format!("Failed to create scheduler '{name}'"))?;
        if let Some(action_scheduler) = maybe_action_scheduler {
            action_schedulers.insert(name.clone(), action_scheduler.clone());
        }
        if let Some(worker_scheduler) = maybe_worker_scheduler {
            worker_schedulers.insert(name.clone(), worker_scheduler.clone());
        }
    }

    // #160 Phase 1: process-wide registry of MetricsComponent roots.
    // Components register themselves at construction; per-server admin
    // routes mount `/metrics` backed by this single registry so every
    // listener exposes the same view.
    let metrics_registry = nativelink_util::metrics_publisher::MetricsRegistry::new();

    // Register the entire `StoreManager` as a single root. `StoreManager`
    // derives `MetricsComponent` and exposes its `stores` HashMap via
    // `#[metric]`, so this one registration walks every store the
    // operator configured (cas_STORE, cas_FAST_SLOW_STORE, AC_STORE,
    // AC_BACKEND_CACHED, etc.). The per-store metric tree (including
    // `cas_FAST_SLOW_STORE.fast.memory.pinned_bytes` for #332
    // falsifiability) is reachable under the `stores.<name>.…` path.
    metrics_registry.register("nativelink", store_manager.clone());

    // Register every scheduler that has been constructed. Schedulers
    // implement `RootMetricsComponent` (which has `MetricsComponent` as
    // a supertrait); we use `register_dyn` plus stable trait upcasting
    // (Rust 1.86+) to register the trait object directly without
    // demanding a concrete type per scheduler kind.
    for (name, scheduler) in &action_schedulers {
        metrics_registry.register_dyn(
            format!("scheduler.{name}.action"),
            scheduler.clone() as Arc<
                dyn nativelink_util::metrics_publisher::MetricsComponentTrait
                    + Send
                    + Sync,
            >,
        );
    }
    for (name, scheduler) in &worker_schedulers {
        metrics_registry.register_dyn(
            format!("scheduler.{name}.worker"),
            scheduler.clone() as Arc<
                dyn nativelink_util::metrics_publisher::MetricsComponentTrait
                    + Send
                    + Sync,
            >,
        );
    }

    // #436 measurement gate: register the global PinBudget and
    // ChunkBudget singletons so every `/metrics` listener exposes
    // `chunked_pin_budget.pinned_bytes_used`,
    // `chunked_pin_budget.pinned_bytes_capacity`,
    // `chunked_pin_budget.pin_budget_rejections_total`,
    // `chunked_chunk_budget.chunk_budget_used_bytes`, and
    // `chunked_chunk_budget.chunk_resource_exhausted_rejections_total`.
    //
    // The two `MetricsComponent` impls on `PinBudget`/`ChunkBudget`
    // exist (`pin_budget.rs`, `chunk_budget.rs`) but were not reachable
    // from the registry without an `Arc` accessor — admissions read
    // from `pin_budget_singleton()` / `chunk_budget_singleton()`, which
    // return `&'static`, while `register_dyn` demands
    // `Arc<dyn MetricsComponent + Send + Sync>`. The `*_arc()`
    // accessors return an `Arc` pointing at the SAME instance, so the
    // gauges published here are the live values admissions consume
    // from — not a separate copy.
    //
    // Registration happens ONCE here (not in the per-store
    // `wire_bazel_chunked_dispatcher` loop) because the budgets are
    // process-global; double-registration would publish duplicate
    // lines.
    //
    // Cfg-gated on `chunked_fast_slow` because the `nativelink_store::
    // chunked` module is only compiled in under that feature; the same
    // gate is applied at every other call-site in this file.
    #[cfg(feature = "chunked_fast_slow")]
    {
        metrics_registry.register(
            "chunked_pin_budget",
            nativelink_store::chunked::pin_budget::pin_budget_arc(),
        );
        metrics_registry.register(
            "chunked_chunk_budget",
            nativelink_store::chunked::chunk_budget::chunk_budget_arc(),
        );
    }

    // First-listener-wins guard: when more than one ServerConfig hosts
    // a worker_api block, the SECOND construction would register the
    // same metrics tree under the same prefix and double every line.
    let mut worker_api_metrics_registered = false;

    // Periodically log tokio runtime metrics to detect thread pool exhaustion.
    // Requires tokio_unstable cfg for blocking thread metrics.
    #[cfg(tokio_unstable)]
    {
        let metrics_handle = tokio::runtime::Handle::current();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let metrics = metrics_handle.metrics();
                let workers = metrics.num_workers();
                let blocking_threads = metrics.num_blocking_threads();
                let idle_blocking = metrics.num_idle_blocking_threads();
                let blocking_depth = metrics.blocking_queue_depth();
                if blocking_depth > 0 || (blocking_threads > 0 && idle_blocking == 0) {
                    warn!(
                        workers,
                        blocking_threads,
                        idle_blocking,
                        blocking_queue_depth = blocking_depth,
                        "tokio thread pool pressure detected"
                    );
                }
            }
        });
    }

    // task #168 item 8: build the SmallBlobDispatcher singleton for
    // Bug A small-CAS peer-mirror push. The dispatcher is plumbed into
    // the WorkerApiServer (register/unregister_worker on connect, broadcast
    // on BlobsAvailable) and into the upload-completion hook sites
    // (item 3) when the feature flag is on. Today the feature flag is OFF
    // by default (`SmallBlobDispatcherConfig::default`), so the
    // dispatcher's `enqueue` is an inert no-op until the operator
    // explicitly enables it for canary rollout. See plan §"Decisions"
    // and `SmallBlobDispatcherConfig::small_blob_mirror_enabled`.
    //
    // Item 7: register `EphemeralServerSidePin` for every FastSlowStore
    // that backs a CAS instance. We walk the (potentially wrapped:
    // ExistenceCache → Verify → SizePartitioning → FastSlowStore) chain
    // for each `cas_store_names` entry, find the FastSlowStore via
    // `as_any().downcast_ref` (matches the existing `find_fast_slow`
    // pattern in `store_manager.rs:81-108`), and register the pin set
    // keyed by the store's CAS instance name. Stores not backed by a
    // FastSlowStore (e.g., direct GrpcStore for testing) are skipped.
    let small_blob_dispatcher: Option<Arc<nativelink_store::small_blob_dispatcher::SmallBlobDispatcher>> = {
        if worker_schedulers.is_empty() {
            None
        } else {
            use nativelink_store::small_blob_dispatcher::{
                EphemeralServerSidePin, SmallBlobDispatcher, SmallBlobDispatcherConfig,
                find_fast_slow_for_pin,
            };
            use nativelink_util::store_trait::StoreDriver;

            // Walk the store wrapper chain to find the underlying
            // FastSlowStore. Mirrors `store_manager.rs:81-108` and the
            // sibling `find_fast_slow_chunked` walker below.
            //
            // SmallBlobDispatcher targets SMALL blobs (≤16 KiB per plan
            // C9), so when traversing a `SizePartitioningStore` we pass a
            // synthetic SMALL-digest key (size 0). That routes through
            // SizePartitioning's `inner_store(Some(key))` to its
            // `lower_store` — the side that holds small CAS blobs in
            // production (`SMALL_CAS_CACHED = FSS { fast: MemoryStore,
            // slow: RefStore→Redis }`). Without this, SizePartitioning's
            // `inner_store(None)` returns `self` and the walker bails;
            // the dispatcher's pin set never registers, and the dispatcher
            // is silently disabled (the second silent-failure mode of
            // #168, masked by the regex bug until it was fixed).
            //
            // ExistenceCacheStore + VerifyStore are the two production
            // wrappers that return `self` from `inner_store(None)`, so we
            // downcast and recurse manually. Stops on the first wrapper
            // that is not recognized + does not unwrap further.
            // Synthetic small-key for routing through `SizePartitioning`
            // wrappers (size 0 routes to `lower_store`). Mirrors the
            // helper inside `find_fast_slow_for_pin`; defined locally
            // because the registration loops below also call
            // `inner_store(Some(synthetic_small_key()))` on the OUTER
            // store before handing the result to the walker.
            fn synthetic_small_key() -> nativelink_util::store_trait::StoreKey<'static> {
                nativelink_util::store_trait::StoreKey::Digest(
                    nativelink_util::common::DigestInfo::new([0u8; 32], 0),
                )
            }

            // #168: SmallBlobDispatcher master feature flag is sourced
            // from `GlobalConfig.small_blob_mirror_enabled` (defaults
            // false). Operator flips via JSON5 config — no rebuild
            // required. The dispatcher is constructed regardless so
            // that the WorkerApiServer wire-up + per-store pin-set
            // registration stay consistent across reconfigs; with the
            // flag off, `enqueue` (and the new sync
            // `schedule_dispatch_to_all_workers`) are inert no-ops.
            let small_blob_mirror_enabled = cfg
                .global
                .as_ref()
                .map(|g| g.small_blob_mirror_enabled)
                .unwrap_or(false);
            let dispatcher_cfg = SmallBlobDispatcherConfig {
                small_blob_mirror_enabled,
                ..Default::default()
            };
            let pin_max_bytes = dispatcher_cfg.pin_max_bytes;
            let dispatcher = Arc::new(SmallBlobDispatcher::new(dispatcher_cfg));
            info!(
                small_blob_mirror_enabled,
                pin_max_bytes,
                "small_blob_dispatcher: constructed (#168)"
            );

            // Register a per-store `EphemeralServerSidePin` for every
            // FastSlowStore backing a CAS instance. The `store_id`
            // matches the CAS instance name so the dispatcher's
            // (store_id, digest) keying is unambiguous.
            //
            // We use `unwrapped_cas_stores` (the stores BEFORE
            // WorkerProxyStore wrapping) so the find walks straight to
            // the FastSlowStore without the WorkerProxyStore layer
            // adding another inner_store hop.
            //
            // **Walker miss = silent disable** (#278C visibility fix).
            // Pre-#278C, when `find_fast_slow_for_pin` returned None
            // (e.g. a future wrapper that returns `self` from
            // `inner_store(None)` without a recognised drill-through),
            // the else branch silently skipped pin-set registration and
            // SmallBlobDispatcher was effectively disabled for that
            // store. Operators had no log line to diagnose by. Now we:
            //   - emit one `warn!` per missed CAS store at startup
            //     naming the store_name (concrete wrapper type-name is
            //     not exposed via the StoreDriver trait, so the
            //     warn names the store_name as the actionable handle);
            //   - count resolved targets and emit
            //     `worker_ac_mirror_target_resolved=<n>` at startup so
            //     operators can see the dispatcher's effective coverage;
            //   - escalate to `error!` when the operator has explicitly
            //     enabled `small_blob_mirror_enabled=true` AND no
            //     targets resolved. The dispatcher is still
            //     constructed (`Some(dispatcher)`) and `enqueue` will
            //     no-op without registered pin sets — the `error!`
            //     surfaces the misconfiguration without fail-stopping
            //     a partial deploy. Future fail-stop policy (return
            //     `None` here) is a separate operator decision.
            let mut resolved: usize = 0;
            let mut missed_stores: Vec<String> = Vec::new();
            let mut skipped_invalid_id: usize = 0;
            for store_name in &cas_store_names {
                let Some(store) = unwrapped_cas_stores.get(store_name) else {
                    continue;
                };
                // Skip stores whose names don't match the dispatcher
                // store_id format (Rust-ident-like `[a-zA-Z_][a-zA-Z0-9_]*`);
                // pin sets won't match and we'd just waste a registration.
                if !nativelink_store::small_blob_dispatcher::is_valid_store_id(store_name) {
                    info!(
                        store_name,
                        "small_blob_dispatcher: skipping pin-set registration; \
                         store_name does not match `[a-zA-Z_][a-zA-Z0-9_]*` (per plan C11)"
                    );
                    skipped_invalid_id += 1;
                    continue;
                }
                let driver: &dyn StoreDriver =
                    store.inner_store(Some(synthetic_small_key()));
                if find_fast_slow_for_pin(driver).is_some() {
                    let pin = Arc::new(EphemeralServerSidePin::new(pin_max_bytes));
                    dispatcher.register_pin_set(store_name, pin);
                    resolved += 1;
                    info!(
                        store_name,
                        pin_max_bytes,
                        "small_blob_dispatcher: registered EphemeralServerSidePin"
                    );
                } else {
                    // Walker bailed: chain bottoms out at a wrapper
                    // that returns `self` from `inner_store(None)` and
                    // is not recognised by `find_fast_slow_for_pin`.
                    // Centralised emission so production + tests share
                    // the message shape (#278C).
                    nativelink_store::small_blob_dispatcher::emit_walker_miss_warn(store_name);
                    missed_stores.push(store_name.clone());
                }
            }
            // Emit the resolved-target count at startup so operators
            // have a single line to grep for SmallBlobDispatcher
            // effective coverage.
            info!(
                worker_ac_mirror_target_resolved = resolved,
                cas_store_count = cas_store_names.len(),
                skipped_invalid_id,
                missed = missed_stores.len(),
                "small_blob_dispatcher: walker resolution summary"
            );
            // Refuse to enable the dispatcher when explicitly opted-in
            // but every CAS store missed: the operator's intent
            // (`small_blob_mirror_enabled=true`) cannot be honoured by
            // a no-op dispatcher. Surface the misconfiguration loudly
            // rather than wedging the dispatcher silently.
            if small_blob_mirror_enabled && resolved == 0 {
                error!(
                    cas_store_count = cas_store_names.len(),
                    missed = missed_stores.len(),
                    "small_blob_dispatcher: small_blob_mirror_enabled=true but \
                     ZERO CAS stores resolved a FastSlowStore via walker — \
                     dispatcher constructed in inert state (every enqueue \
                     no-ops). Audit the cas_stores chain and \
                     find_fast_slow_for_pin's recognised wrappers."
                );
            }
            // #168 item I: spawn the periodic activity-metrics logger
            // (1 line / minute). Provides operator visibility into
            // pin-set capacity headroom + queue-full / pin-full counters
            // without standing up a separate metrics endpoint. The
            // join handle is intentionally dropped — the task observes
            // the dispatcher via `Weak`, so it self-exits if the
            // dispatcher Arc is ever dropped.
            let _metrics_handle = dispatcher.spawn_periodic_metrics(
                core::time::Duration::from_secs(60),
            );
            Some(dispatcher)
        }
    };

    // #212 Phase 2.7 production wire-up: honor the
    // `bazel_facing_internal_chunking_enabled` kill-switch from
    // `GlobalConfig` (production sign-off 2026-05-02). When the
    // feature is compiled in AND the operator has set the field to
    // true, flip the process-wide AtomicBool here so subsequent
    // `FastSlowStore::update` calls dispatch through the per-blob
    // ChunkedDriver machinery once the dispatcher is installed below.
    // The runtime setters
    // `chunked::enable_bazel_facing_internal_chunking()` /
    // `chunked::disable_bazel_facing_internal_chunking()` remain
    // available for tests and admin tooling.
    #[cfg(feature = "chunked_fast_slow")]
    if cfg
        .global
        .as_ref()
        .is_some_and(|g| g.bazel_facing_internal_chunking_enabled)
    {
        nativelink_store::chunked::enable_bazel_facing_internal_chunking();
        info!(
            "GlobalConfig: bazel_facing_internal_chunking_enabled=true \
             (process-wide chunked-dispatch ON; #212 Phase 2.7)"
        );
    }

    // #212 Phase 2.5/2.7 fixup S1: wire the chunked-read registry +
    // Bazel-facing chunked dispatcher into every CAS-backing
    // FastSlowStore whose slow tier is a FilesystemStore. The kill
    // switches are now driven by JSON config (production sign-off
    // 2026-05-02): per-FastSlowStore `chunked_reads_enabled` (Phase
    // 2.5) and process-wide `bazel_facing_internal_chunking_enabled`
    // (Phase 2.7, set above). The runtime APIs
    // (`FastSlowStore::enable_chunked_reads()`,
    // `chunked::enable_bazel_facing_internal_chunking()`) remain
    // available for tests and admin tooling.
    #[cfg(feature = "chunked_fast_slow")]
    {
        use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
        use nativelink_store::wrapper_walker::{
            find_fast_slow_via_chain, synthetic_large_key,
        };
        use nativelink_util::store_trait::StoreDriver;

        // Walker + synthetic-large-key sentinel live in
        // `nativelink_store::wrapper_walker` so the
        // `failed_writes_drain` V3 self-retry path
        // (`nativelink-service`) and this chunked-dispatcher wiring
        // share one canonical implementation. Both must use
        // `synthetic_large_key()` to descend `SizePartitioningStore`
        // into its upper arm (the side that holds the >16KiB
        // FilesystemStore-backed FSS in production); without it,
        // `SizePartitioningStore::inner_store(None)` returns `self` and
        // the walker bails before reaching the FSS.

        for store_name in &cas_store_names {
            let Some(store) = unwrapped_cas_stores.get(store_name) else {
                continue;
            };
            let driver: &dyn StoreDriver = store.inner_store(Some(synthetic_large_key()));
            let Some(fss) = find_fast_slow_via_chain(driver) else {
                info!(
                    store_name,
                    "chunked-dispatcher wiring: no FastSlowStore found in chain; \
                     skipping (#212 v4.5 routing fix; production chains with \
                     RefStore/SizePartitioning are walked via large-key descent)"
                );
                continue;
            };
            // Try to get the slow-tier as Arc<FilesystemStore<FileEntryImpl>>.
            // Walk through SizePartitioning if present; the production
            // FSS layout has the slow tier as a direct FilesystemStore
            // OR wrapped by SizePartitioningStore (size-based routing).
            let slow_arc: std::sync::Arc<dyn StoreDriver> =
                fss.slow_store_clone().into_inner();
            // Try direct FilesystemStore downcast first.
            let Ok(fs_arc) = std::sync::Arc::clone(&slow_arc)
                .as_any_arc()
                .downcast::<FilesystemStore<FileEntryImpl>>()
            else {
                info!(
                    store_name,
                    "chunked-dispatcher wiring: slow tier is not a direct \
                     FilesystemStore<FileEntryImpl>; skipping (#212 fixup S1) — \
                     non-FilesystemStore slow tiers (e.g. SizePartitioning, \
                     GrpcStore) are out of scope for the v1 wiring",
                );
                continue;
            };
            let _dispatcher =
                nativelink_service::chunked_write_handler::wire_bazel_chunked_dispatcher(
                    fss,
                    Arc::clone(&fs_arc),
                );
            // #212 v4.5: also build the worker→server WriteChunked
            // handler keyed by this store_name so the per-listener
            // wiring below can register `CasExtensionsServer` on any
            // listener that hosts this CAS store (port 50071 in
            // production). Without this registration the worker's
            // outbound chunked stream lands on a Routes builder that
            // has no CasExtensions service and gets Code::Unimplemented
            // — the production bug this commit fixes.
            chunked_write_handlers.insert(
                store_name.clone(),
                Arc::new(
                    nativelink_service::chunked_write_handler::ChunkedWriteHandler::<FileEntryImpl>::new(
                        fs_arc,
                    ),
                ),
            );
            info!(
                store_name,
                "chunked-dispatcher wiring: installed registry + dispatcher + \
                 ChunkedWriteHandler (#212 fixup S1; #212 v4.5 routing fix; \
                 kill-switches default OFF — read: enable_chunked_reads(); \
                 write: enable_bazel_facing_internal_chunking())"
            );
        }
    }

    // Spawn the BlobsInStableStorage drain-then-fire loop. When any CAS
    // (or AC) FastSlowStore completes a background slow write it pushes
    // the digest and notifies us. We drain all queued digests and
    // broadcast immediately, so workers can unpin blobs with minimal
    // latency.
    //
    // CAS vs AC chunks are HARD-PARTITIONED on the wire by the chunk's
    // `store_id` field (proto3 field 6 of `BlobsInStableStorageChunk`):
    // - CAS chunks carry `store_id = ""` (forward-compatible default),
    //   routed by the worker to `cas_fss.remove_mirror_blobs`.
    // - AC chunks carry `store_id = "<AC store name>"`, routed by the
    //   worker to its `ac_fss.remove_local_ac_pins` for the matching
    //   store. CAS readers (`bytestream_server::write` short-circuit,
    //   `cas_server::batch_update_blobs` short-circuit) NEVER consume
    //   these via `BlobLocalityMap`; the AC channel is end-to-end
    //   isolated from CAS-side data plane.
    if !worker_schedulers.is_empty() {
        // CAS stores: drained together → one broadcast tagged store_id="".
        let cas_bis_stores: Vec<(String, nativelink_util::store_trait::Store)> = cas_store_names
            .iter()
            .filter_map(|name| store_manager.get_store(name).map(|s| (name.clone(), s)))
            .collect();
        // AC stores: drained per-store → one broadcast PER AC store
        // tagged with that store's name, so workers can route the
        // unpin to the matching FSS via `store_id` lookup.
        let ac_bis_stores: Vec<(String, nativelink_util::store_trait::Store)> = ac_store_names
            .iter()
            // Avoid double-broadcasting if a name appears in both sets
            // (a store wired as both CAS and AC service is pathological,
            // but the dedupe is cheap insurance).
            .filter(|name| !cas_store_names.contains(name.as_str()))
            .filter_map(|name| store_manager.get_store(name).map(|s| (name.clone(), s)))
            .collect();
        let cas_store_count = cas_bis_stores.len();
        let ac_store_count = ac_bis_stores.len();
        let schedulers: Vec<Arc<dyn nativelink_scheduler::worker_scheduler::WorkerScheduler>> =
            worker_schedulers.values().cloned().collect();

        if cas_store_count + ac_store_count > 0 {
            let scheduler_count = schedulers.len();

            // Merge per-store notifies into a single wakeup signal so the
            // broadcast loop wakes when *any* store has new stable digests.
            let merged_notify = Arc::new(Notify::new());
            for (_name, store) in cas_bis_stores.iter().chain(ac_bis_stores.iter()) {
                let store_notify = store.stable_notify();
                let merged = merged_notify.clone();
                tokio::spawn(async move {
                    loop {
                        store_notify.notified().await;
                        debug!(
                            target: "nativelink::stable_notify_fire",
                            "stable_notify fired by a BIS-tracked store (CAS or AC)"
                        );
                        merged.notify_one();
                    }
                });
            }

            // Capture an AC pin registry handle so the broadcast loop
            // can also drop server-side AC pin entries for the
            // newly-stable digests. The drain is symmetric with the
            // worker-side `remove_local_ac_pins`: both fire on the same
            // BIS broadcast event, so by the time the next
            // `BlobsAvailable` tick arrives the registry and the
            // worker's pin map agree.
            let registry_for_loop = ac_pin_registry.clone();

            background_spawn!("blobs_in_stable_storage_loop", async move {
                loop {
                    tokio::select! {
                        () = merged_notify.notified() => {}
                        () = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                    // Build broadcast batches in a single list:
                    //   - CAS first: all CAS-store drains merged into one
                    //     bucket tagged store_id="" (CAS share locality_map;
                    //     one broadcast covers them all).
                    //   - Then one entry per AC store with its own store_id
                    //     so workers can route the unpin to the matching
                    //     FSS via store_id lookup.
                    // An empty store_id distinguishes the CAS batch from
                    // AC batches downstream (pin-sweep + log message).
                    let mut batches: Vec<(String, Vec<nativelink_util::common::DigestInfo>)> =
                        Vec::new();
                    // #334 Fix C: per-CAS-store drain captured separately so
                    // we can call `unpin_digests` on the originating store
                    // post-broadcast. Without per-store accounting we'd have
                    // to fan an unpin out across every CAS store, which
                    // would either no-op cheaply (digest absent — fine) or
                    // race with a fresh pin (digest just-rewritten — bad,
                    // releases the pin held for a NEW upload). Tracking
                    // origin keeps unpin scoped to the BIS-acked write.
                    let mut cas_drains_per_store: Vec<(
                        &nativelink_util::store_trait::Store,
                        Vec<nativelink_util::common::DigestInfo>,
                    )> = Vec::with_capacity(cas_bis_stores.len());
                    let mut cas_digests = Vec::new();
                    for (_name, store) in &cas_bis_stores {
                        let drained = store.drain_stable_digests();
                        if !drained.is_empty() {
                            cas_digests.extend_from_slice(&drained);
                            cas_drains_per_store.push((store, drained));
                        }
                    }
                    if !cas_digests.is_empty() {
                        batches.push((String::new(), cas_digests));
                    }
                    // #334 Fix C extended (dsr MAJOR-1): symmetric per-AC-
                    // store drain capture so we can call `unpin_digests`
                    // on the originating AC store post-broadcast. AC
                    // backends in production (`AC_BACKEND_CACHED` =
                    // `FastSlowStore { fast: MemoryStore(4 GB), slow:
                    // ref(REDIS_AC_STORE) }`) take the same fast-tier
                    // pin at write time as CAS (`fast_slow_store.rs:4103`
                    // / `:3805`). Without this AC-side unpin the 1 GB
                    // pin budget (4 GB cap × 25%) fills after ~1 GB of
                    // AC writes and `pin_keys: pin cap exceeded` floods.
                    let mut ac_drains_per_store: Vec<(
                        &nativelink_util::store_trait::Store,
                        Vec<nativelink_util::common::DigestInfo>,
                    )> = Vec::with_capacity(ac_bis_stores.len());
                    for (name, store) in &ac_bis_stores {
                        let drained = store.drain_stable_digests();
                        if !drained.is_empty() {
                            batches.push((name.clone(), drained.clone()));
                            ac_drains_per_store.push((store, drained));
                        }
                    }
                    if batches.is_empty() {
                        continue;
                    }

                    // Server-side AC pin sweep: previously this fanned
                    // out per-AC-store, then per-endpoint, with ONE
                    // `inner.write()` lock acquisition per endpoint
                    // per AC store (#278B perf MAJOR). Refactor: group
                    // ALL AC drains by endpoint and apply one
                    // `remove_digests_for_endpoint_batch` per endpoint
                    // — N endpoints × M AC stores collapses from N*M
                    // lock cycles to N. CAS has no analogous registry.
                    let mut drains_for_batch: Vec<(std::sync::Arc<str>, &[nativelink_util::common::DigestInfo])> =
                        Vec::with_capacity(batches.len());
                    for (store_id, digests) in &batches {
                        if store_id.is_empty() {
                            continue; // CAS batch — no AC pin sweep
                        }
                        drains_for_batch.push((
                            std::sync::Arc::<str>::from(store_id.as_str()),
                            digests.as_slice(),
                        ));
                    }
                    if !drains_for_batch.is_empty() {
                        let endpoints: Vec<String> =
                            registry_for_loop.endpoint_counts().keys().cloned().collect();
                        for endpoint in &endpoints {
                            registry_for_loop
                                .remove_digests_for_endpoint_batch(endpoint, &drains_for_batch);
                        }
                    }

                    for (store_id, digests) in &batches {
                        let is_ac = !store_id.is_empty();
                        let kind = if is_ac { "AC" } else { "CAS" };
                        debug!(
                            target: "nativelink::stable_storage_broadcast",
                            digest_count = digests.len(),
                            ac_store = store_id.as_str(),
                            scheduler_count = schedulers.len(),
                            kind,
                            "BlobsInStableStorage {kind}: broadcasting drained digests"
                        );
                        for (scheduler_idx, scheduler) in schedulers.iter().enumerate() {
                            scheduler
                                .broadcast_blobs_in_stable_storage_chunked(
                                    digests.clone(),
                                    store_id,
                                )
                                .await;
                            debug!(
                                target: "nativelink::stable_storage_broadcast",
                                scheduler_idx,
                                ac_store = store_id.as_str(),
                                kind,
                                "BlobsInStableStorage {kind} chunked: broadcast returned"
                            );
                        }
                    }

                    // #334 Fix C: release server-side fast-tier pins for
                    // every CAS and AC digest just broadcast. The
                    // broadcast told workers the blob is durably
                    // mirrored (≥2 replicas); the server's fast-tier
                    // pin acquired at write time (`FastSlowStore::update`)
                    // is no longer load-bearing past this point. Without
                    // this unpin, every write accumulates a permanent
                    // pin entry in the fast tier's 25%-of-cap pin budget
                    // — for CAS that's 48 GB × 25% = ~12 GB, for AC
                    // that's 4 GB × 25% = ~1 GB. After the cap fills
                    // `pin_keys: pin cap exceeded` would warn-and-skip
                    // every subsequent pin, silently re-opening the
                    // durability gap this fix closes.
                    //
                    // Routed through `Store::unpin_digests` so the call
                    // walks the same `pin_delegation` chain `pin_digests`
                    // used at write time:
                    //   - CAS: ExistenceCacheStore → VerifyStore →
                    //     SizePartitioning → FastSlowStore → MemoryStore
                    //     + FilesystemStore.
                    //   - AC: AcProxyStore → CompletenessCheckingStore →
                    //     FastSlowStore → MemoryStore + ref(REDIS_AC_STORE)
                    //     (production `AC_BACKEND_CACHED`).
                    //
                    // Race safety. `MokaEvictingMap::unpin_key` is wholesale
                    // `pinned.remove(key)` (no per-write epoch tracking), so a
                    // concurrent re-pin between drain and unpin DOES lose
                    // its pin here. That race is benign — but for a
                    // different reason than per-write scoping. The actual
                    // safety:
                    //   - BIS-ack implies the SLOW TIER (FilesystemStore for
                    //     CAS, Redis for AC) already has the bytes. The
                    //     fast-tier pin is no longer load-bearing past this
                    //     point — it only existed to hold the in-memory
                    //     replica across the ack window.
                    //   - For CAS specifically, content-addressing means a
                    //     re-uploaded same-key blob has identical bytes; the
                    //     slow tier already has them, so even an unpinned-
                    //     then-evicted entry is recoverable on the next
                    //     read via the slow-tier fallback.
                    //   - For AC, an unpinned entry that's evicted before
                    //     a follow-up read just means the next reader pays
                    //     a Redis round-trip (the slow tier holds it).
                    // The comment is intentionally specific because future
                    // readers might assume per-write epoch tracking exists
                    // (the prior version of this comment did make that
                    // claim incorrectly).
                    for (store, drained) in &cas_drains_per_store {
                        store.unpin_digests(drained);
                    }
                    for (store, drained) in &ac_drains_per_store {
                        store.unpin_digests(drained);
                    }
                }
            });
            info!(
                cas_store_count,
                ac_store_count,
                scheduler_count,
                "started BlobsInStableStorage drain-then-fire loop (CAS + AC)"
            );
        }

        // #287: Spawn the server-side `failed_slow_writes` drain loop.
        //
        // Background. The server's cas_STORE FastSlowStore tracks digests
        // whose background slow-tier write failed in a `failed_slow_writes`
        // set, populated by `failed_writes_inserter()` (chunked-commit Err
        // arms), the legacy `update`/`update_oneshot` Err arms, the
        // `PinExpireFailedWritesListener` 120 s pin auto-expire, and the
        // streaming-write watchdog. The companion side-effect at insert
        // time is `fast_store.pin_digests(&[digest])` — keeps the
        // in-memory replica alive during the 120 s pin TTL so a worker
        // can still re-upload the bytes.
        //
        // Pre-#287, the only consumer was the WORKER's own
        // `LocalWorker::on_reconnect` calling `cas_store.drain_failed_digests()`
        // — but that's the WORKER's FSS instance, not the SERVER's. The
        // server-side set was dead-letter: filled forever, never drained,
        // pinning blobs in MemoryStore until the 120 s TTL expired and
        // the mirror protocol theoretically re-uploaded them on the next
        // BlobsAvailable tick.
        //
        // Fix. Periodically drain the server's `failed_slow_writes` and
        // dispatch `UploadMissingBlobs` to a worker that has the digest
        // (per `BlobLocalityMap`). Idempotent: if the upload fails, the
        // slow-tier write Err arm re-inserts the digest naturally.
        //
        // Edge cases:
        //   - No worker in `locality_map` for a digest → log warn (via
        //     metric counter) and re-insert with throttling (worker may
        //     report it on the next BlobsAvailable tick).
        //   - Multiple workers per digest → pick the first connected one.
        //   - Recently-dispatched digests are filtered via a per-digest
        //     cooldown so we don't re-spam the same worker.
        //
        // No fsync, no async/sync architectural change — purely additive
        // periodic drain task.
        if !worker_schedulers.is_empty() {
            let cas_drain_stores: Vec<(String, nativelink_util::store_trait::Store)> =
                cas_store_names
                    .iter()
                    .filter_map(|name| store_manager.get_store(name).map(|s| (name.clone(), s)))
                    .collect();
            if !cas_drain_stores.is_empty() {
                if let Some(dispatcher_for_drain) = small_blob_dispatcher.clone() {
                    let locality_map_for_drain = locality_map.clone();
                    let drain_store_count = cas_drain_stores.len();
                    background_spawn!("failed_slow_writes_drain_loop", async move {
                        use nativelink_service::failed_writes_drain::{
                            DEFAULT_DRAIN_BATCH_SIZE, DEFAULT_DRAIN_COOLDOWN,
                            DEFAULT_DRAIN_INFLIGHT_CAP, DEFAULT_DRAIN_INTERVAL, drain_tick,
                        };
                        let mut inflight: HashMap<
                            nativelink_util::common::DigestInfo,
                            std::time::Instant,
                        > = HashMap::new();
                        loop {
                            tokio::time::sleep(DEFAULT_DRAIN_INTERVAL).await;
                            // The drain logic is in
                            // `nativelink_service::failed_writes_drain`
                            // so integration tests can drive it
                            // directly. The binary's responsibility is
                            // (a) the spawn, (b) the periodic wakeup.
                            let _stats = drain_tick(
                                &cas_drain_stores,
                                &locality_map_for_drain,
                                &dispatcher_for_drain,
                                &mut inflight,
                                DEFAULT_DRAIN_COOLDOWN,
                                DEFAULT_DRAIN_BATCH_SIZE,
                                DEFAULT_DRAIN_INFLIGHT_CAP,
                            )
                            .await;
                        }
                    });
                    info!(
                        drain_store_count,
                        "started failed_slow_writes drain loop (CAS, #287)"
                    );
                }
            }
        }
    }

    // Graceful shutdown: accept_stop signals HTTP accept loops to stop,
    // drain_receivers lets the SIGTERM handler wait for connection drain.
    let (accept_stop_tx, _accept_stop_rx) = tokio::sync::watch::channel(false);
    #[cfg(target_family = "unix")]
    let mut drain_receivers: Vec<oneshot::Receiver<()>> = Vec::new();

    for server_cfg in server_cfgs {
        let services = server_cfg
            .services
            .err_tip(|| "'services' must be configured")?;

        // Extract message size limits from the listener config.
        // Both HTTP and HTTP3 listeners support these; HTTP also has compression.
        let (max_decode, max_encode) = match &server_cfg.listener {
            ListenerConfig::Http(http) => (http.max_decoding_message_size, http.max_encoding_message_size),
            ListenerConfig::Http3(h3) => (h3.max_decoding_message_size, h3.max_encoding_message_size),
        };
        let max_decoding = if max_decode == 0 { DEFAULT_MAX_DECODING_MESSAGE_SIZE } else { max_decode };
        let max_encoding = if max_encode == 0 { DEFAULT_MAX_ENCODING_MESSAGE_SIZE } else { max_encode };

        // Helper to configure a tonic service with message size limits and
        // optional compression from the HTTP listener config.
        macro_rules! svc_setup {
            ($v:expr) => {{
                let mut service = $v.into_service();
                service = service.max_decoding_message_size(max_decoding);
                service = service.max_encoding_message_size(max_encoding);
                if let ListenerConfig::Http(ref http_config) = server_cfg.listener {
                    let send_algo = &http_config.compression.send_compression_algorithm;
                    if let Some(encoding) = into_encoding(send_algo.unwrap_or(HttpCompressionAlgorithm::None)) {
                        service = service.send_compressed(encoding);
                    }
                    for encoding in http_config.compression.accepted_compression_algorithms.iter()
                        .filter_map(|from: &HttpCompressionAlgorithm| into_encoding(*from))
                    {
                        service = service.accept_compressed(encoding);
                    }
                }
                service
            }};
        }

        // #212 v4.5: precompute the optional CasExtensionsServer for
        // this listener. We register it whenever the listener hosts a
        // `cas` service AND a ChunkedWriteHandler exists for that
        // listener's CAS store name. Multiple CAS configs per listener
        // share a handler when they share a store_name; if the
        // handlers differ across configs we pick the first match
        // (this reflects today's production layout — one CAS store per
        // listener). The handler is `Arc`-shared so registering the
        // same handler on multiple listeners is correct (only one
        // in-flight tracker / budget across the process).
        #[cfg(feature = "chunked_fast_slow")]
        let cas_extensions_handler: Option<
            Arc<
                nativelink_service::chunked_write_handler::ChunkedWriteHandler<
                    nativelink_store::filesystem_store::FileEntryImpl,
                >,
            >,
        > = services
            .cas
            .as_ref()
            .and_then(|cas_cfgs| {
                cas_cfgs
                    .iter()
                    .find_map(|c| chunked_write_handlers.get(&c.config.cas_store).cloned())
            });

        // Builder helper for the CasExtensions service (#212 v4.5).
        // Returns Some(service) only when the feature is on AND a
        // handler exists for this listener; otherwise None so the
        // Routes builder skips it cleanly.
        #[cfg(feature = "chunked_fast_slow")]
        let make_cas_extensions_service = |handler: Option<Arc<nativelink_service::chunked_write_handler::ChunkedWriteHandler<nativelink_store::filesystem_store::FileEntryImpl>>>| -> Option<
            nativelink_proto::com::github::trace_machina::nativelink::remote_execution::cas_extensions_server::CasExtensionsServer<
                nativelink_service::chunked_write_handler::ChunkedWriteHandler<
                    nativelink_store::filesystem_store::FileEntryImpl,
                >,
            >,
        > {
            let handler = handler?;
            let mut service = nativelink_proto::com::github::trace_machina::nativelink::remote_execution::cas_extensions_server::CasExtensionsServer::from_arc(handler);
            service = service.max_decoding_message_size(max_decoding);
            service = service.max_encoding_message_size(max_encoding);
            if let ListenerConfig::Http(ref http_config) = server_cfg.listener {
                let send_algo = &http_config.compression.send_compression_algorithm;
                if let Some(encoding) = into_encoding(send_algo.unwrap_or(HttpCompressionAlgorithm::None)) {
                    service = service.send_compressed(encoding);
                }
                for encoding in http_config.compression.accepted_compression_algorithms.iter()
                    .filter_map(|from: &HttpCompressionAlgorithm| into_encoding(*from))
                {
                    service = service.accept_compressed(encoding);
                }
            }
            Some(service)
        };

        let execution_server = services
            .execution
            .as_ref()
            .map(|cfg| ExecutionServer::new(cfg, &action_schedulers, &store_manager))
            .transpose()
            .err_tip(|| "Could not create Execution service")?;

        let tonic_services = Routes::builder()
            .routes()
            .add_optional_service(
                services
                    .ac
                    .map_or(Ok(None), |cfg| {
                        AcServer::new(&cfg, &store_manager)
                            .map(|v| Some(svc_setup!(v)))
                    })
                    .err_tip(|| "Could not create AC service")?,
            )
            .add_optional_service(
                services
                    .cas
                    .map_or(Ok(None), |cfg| {
                        CasServer::new(&cfg, &store_manager, small_blob_dispatcher.clone())
                            .map(|v| {
                                let mut service = v.into_zero_copy_service(max_decoding, max_encoding);
                                if let ListenerConfig::Http(ref http_config) = server_cfg.listener {
                                    let send_algo = &http_config.compression.send_compression_algorithm;
                                    if let Some(encoding) = into_encoding(send_algo.unwrap_or(HttpCompressionAlgorithm::None)) {
                                        service = service.send_compressed(encoding);
                                    }
                                    for encoding in http_config.compression.accepted_compression_algorithms.iter()
                                        .filter_map(|from: &HttpCompressionAlgorithm| into_encoding(*from))
                                    {
                                        service = service.accept_compressed(encoding);
                                    }
                                }
                                Some(service)
                            })
                    })
                    .err_tip(|| "Could not create CAS service")?,
            )
            .add_optional_service(
                execution_server
                    .clone()
                    .map(|v| svc_setup!(v)),
            )
            .add_optional_service(
                execution_server.map(|v| {
                    let mut service = v.into_operations_service();
                    service = service.max_decoding_message_size(max_decoding);
                    service = service.max_encoding_message_size(max_encoding);
                    if let ListenerConfig::Http(ref http_config) = server_cfg.listener {
                        let send_algo = &http_config.compression.send_compression_algorithm;
                        if let Some(encoding) = into_encoding(send_algo.unwrap_or(HttpCompressionAlgorithm::None)) {
                            service = service.send_compressed(encoding);
                        }
                        for encoding in http_config.compression.accepted_compression_algorithms.iter()
                            .filter_map(|from: &HttpCompressionAlgorithm| into_encoding(*from))
                        {
                            service = service.accept_compressed(encoding);
                        }
                    }
                    service
                }),
            )
            .add_optional_service(
                services
                    .fetch
                    .map_or(Ok(None), |cfg| {
                        FetchServer::new(&cfg, &store_manager)
                            .map(|v| Some(svc_setup!(v)))
                    })
                    .err_tip(|| "Could not create Fetch service")?,
            )
            .add_optional_service(
                services
                    .push
                    .map_or(Ok(None), |cfg| {
                        PushServer::new(&cfg, &store_manager)
                            .map(|v| Some(svc_setup!(v)))
                    })
                    .err_tip(|| "Could not create Push service")?,
            )
            .add_optional_service(
                services
                    .bytestream
                    .map_or(Ok(None), |cfg| {
                        ByteStreamServer::new(&cfg, &store_manager, small_blob_dispatcher.clone())
                            .map(|v| {
                                let mut service = v.into_zero_copy_service(max_decoding, max_encoding);
                                if let ListenerConfig::Http(ref http_config) = server_cfg.listener {
                                    let send_algo = &http_config.compression.send_compression_algorithm;
                                    if let Some(encoding) = into_encoding(send_algo.unwrap_or(HttpCompressionAlgorithm::None)) {
                                        service = service.send_compressed(encoding);
                                    }
                                    for encoding in http_config.compression.accepted_compression_algorithms.iter()
                                        .filter_map(|from: &HttpCompressionAlgorithm| into_encoding(*from))
                                    {
                                        service = service.accept_compressed(encoding);
                                    }
                                }
                                Some(service)
                            })
                    })
                    .err_tip(|| "Could not create ByteStream service")?,
            )
            .add_optional_service(
                OptionFuture::from(
                    services
                        .capabilities
                        .as_ref()
                        .map(|cfg| CapabilitiesServer::new(cfg, &action_schedulers)),
                )
                .await
                .map_or(Ok::<Option<CapabilitiesServer>, Error>(None), |server| {
                    Ok(Some(server?))
                })
                .err_tip(|| "Could not create Capabilities service")?
                .map(|v| svc_setup!(v)),
            )
            .add_optional_service(
                services
                    .worker_api
                    .map_or(Ok(None), |cfg| {
                        // Pick the first unwrapped CAS store for backfill existence
                        // checks. Using the unwrapped store ensures has_with_results
                        // goes directly to the real store, bypassing WorkerProxyStore
                        // which would report blobs on other workers as "present".
                        let backfill_cas = cas_store_names
                            .iter()
                            .next()
                            .and_then(|name| unwrapped_cas_stores.get(name).cloned());
                        // Pass the matching WorkerProxyStore Arc so the
                        // server can plumb mirror capacity reports
                        // (review #1) into the picker's pre-check.
                        let worker_proxy = cas_store_names
                            .iter()
                            .next()
                            .and_then(|name| worker_proxy_stores.get(name).cloned());
                        WorkerApiServer::new(
                            &cfg,
                            &worker_schedulers,
                            Some(locality_map.clone()),
                            backfill_cas,
                            worker_proxy,
                            small_blob_dispatcher.clone(),
                            Some(ac_pin_registry.clone()),
                        )
                        .map(|v| {
                            // #160 Phase 1: register the WorkerApiMetrics
                            // tree (which transitively walks
                            // ChunkDropCounts via #99-fixup-2) before
                            // `into_service()` consumes the server.
                            // First-listener wins per registry semantics
                            // (the metrics tree is process-wide).
                            if !worker_api_metrics_registered {
                                metrics_registry
                                    .register("worker_api", v.metrics());
                                worker_api_metrics_registered = true;
                            }
                            Some(svc_setup!(v))
                        })
                    })
                    .err_tip(|| "Could not create WorkerApi service")?,
            )
            .add_optional_service(
                services
                    .experimental_bep
                    .map_or(Ok(None), |cfg| {
                        BepServer::new(&cfg, &store_manager)
                            .map(|v| Some(svc_setup!(v)))
                    })
                    .err_tip(|| "Could not create BEP service")?,
            );

        // #212 v4.5: register CasExtensions on the same Routes builder
        // as `cas` / `bytestream` so worker→server WriteChunked lands
        // on the CAS-port listener (port 50071 in production) where
        // the worker's GrpcStore-backed outbound channel can actually
        // reach it. Without this registration the request hits the
        // Routes builder's fallback handler and gets
        // Code::Unimplemented; every >=1 MiB chunked write fails.
        // Cfg-gated on `chunked_fast_slow` because the handler type
        // itself is gated on that feature; in non-feature builds the
        // shadowed binding is omitted entirely so the chain remains
        // identical to the pre-fix layout.
        #[cfg(feature = "chunked_fast_slow")]
        let tonic_services = tonic_services
            .add_optional_service(make_cas_extensions_service(cas_extensions_handler));

        let health_registry = health_registry_builder.lock().await.build();

        match server_cfg.listener {
        ListenerConfig::Http(http_config) => {
        let mut svc =
            tonic_services
                .into_axum_router()
                .layer(nativelink_util::telemetry::OtlpLayer::new(
                    server_cfg.experimental_identity_header.required,
                ));

        if let Some(health_cfg) = services.health {
            let path = if health_cfg.path.is_empty() {
                DEFAULT_HEALTH_STATUS_CHECK_PATH
            } else {
                &health_cfg.path
            };
            svc = svc.route_service(path, HealthServer::new(health_registry, &health_cfg));
        }

        if let Some(admin_config) = services.admin {
            let path = if admin_config.path.is_empty() {
                DEFAULT_ADMIN_API_PATH
            } else {
                &admin_config.path
            };
            let worker_schedulers = Arc::new(worker_schedulers.clone());
            svc = svc.nest_service(
                path,
                Router::new().route(
                    "/scheduler/{instance_name}/set_drain_worker/{worker_id}/{is_draining}",
                    axum::routing::post(
                        move |params: axum::extract::Path<(String, String, String)>| async move {
                            let (instance_name, worker_id, is_draining) = params.0;
                            (async move {
                                let is_draining = match is_draining.as_str() {
                                    "0" => false,
                                    "1" => true,
                                    _ => {
                                        return Err(make_err!(
                                            Code::Internal,
                                            "{} is neither 0 nor 1",
                                            is_draining
                                        ));
                                    }
                                };
                                worker_schedulers
                                    .get(&instance_name)
                                    .err_tip(|| {
                                        format!(
                                            "Can not get an instance with the name of '{}'",
                                            &instance_name
                                        )
                                    })?
                                    .clone()
                                    .set_drain_worker(&worker_id.clone().into(), is_draining)
                                    .await?;
                                Ok::<_, Error>(format!("Draining worker {worker_id}"))
                            })
                            .await
                            .map_err(|e| {
                                Err::<String, _>((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("Error: {e:?}"),
                                ))
                            })
                        },
                    ),
                ),
            );
        }

        // #160 Phase 1: mount the metrics publisher at `/metrics` on
        // any HTTP listener whose `services` block opts in via
        // `"metrics": {}` (security FIX-FIRST, see [`MetricsConfig`]).
        // Each listener serves a snapshot of the SAME process-wide
        // registry (the registry holds Arc handles, so the per-listener
        // route receives a cheap clone). Cfg-gated on the `pprof`
        // feature because that gates `axum` in `nativelink-util`; the
        // production binary is always built with `--features pprof`.
        // Operators must opt in PER LISTENER — typically only the
        // internal-network listener should expose `/metrics`. The
        // public-facing Bazel listener should leave `metrics` unset
        // unless an upstream auth/ACL is in place.
        // NOTE: HTTP/3 listeners do NOT mount `/metrics` even when
        // `metrics` is set; the Http3 arm builds a `tonic_h3::H3Router`
        // (not an axum `Router`), so axum-merge composition is not
        // a one-line change. Operators wanting `/metrics` must
        // configure at least one HTTP/1+2 listener with the opt-in
        // flag set. Tracked as a follow-up to #160.
        #[cfg(feature = "pprof")]
        if services.metrics.is_some() {
            svc = svc.merge(
                nativelink_util::metrics_publisher::metrics_router(
                    metrics_registry.clone(),
                ),
            );
        }

        // This is the default service that executes if no other endpoint matches.
        svc = svc.fallback(|uri: Uri| async move {
            warn!("No route for {uri}");
            (StatusCode::NOT_FOUND, format!("No route for {uri}"))
        });
        // Reject startup if require_tls is set but no TLS config is provided.
        if http_config.require_tls && http_config.tls.is_none() {
            return Err(make_input_err!(
                "Listener '{}' on {} has require_tls=true but no TLS configuration. \
                 Either add a tls block or set require_tls to false",
                server_cfg.name,
                http_config.socket_address
            ));
        }

        // Configure our TLS acceptor if we have TLS configured.
        let maybe_tls_acceptor = http_config.tls.map_or(Ok(None), |tls_config| {
            fn read_cert(cert_file: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
                let mut cert_reader = std::io::BufReader::new(
                    std::fs::File::open(cert_file)
                        .err_tip(|| format!("Could not open cert file {cert_file}"))?,
                );
                let certs = CertificateDer::pem_reader_iter(&mut cert_reader)
                    .collect::<Result<Vec<CertificateDer<'_>>, _>>()
                    .err_tip(|| format!("Could not extract certs from file {cert_file}"))?;
                Ok(certs)
            }
            let certs = read_cert(&tls_config.cert_file)?;
            let mut key_reader = std::io::BufReader::new(
                std::fs::File::open(&tls_config.key_file)
                    .err_tip(|| format!("Could not open key file {}", tls_config.key_file))?,
            );
            let key = match PrivateKeyDer::from_pem_reader(&mut key_reader)
                .err_tip(|| format!("Could not extract key(s) from file {}", tls_config.key_file))?
            {
                PrivateKeyDer::Pkcs8(key) => key.into(),
                PrivateKeyDer::Sec1(key) => key.into(),
                PrivateKeyDer::Pkcs1(key) => key.into(),
                _ => {
                    return Err(make_err!(
                        Code::Internal,
                        "No keys found in file {}",
                        tls_config.key_file
                    ));
                }
            };
            if PrivateKeyDer::from_pem_reader(&mut key_reader).is_ok() {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Expected 1 key in file {}",
                    tls_config.key_file
                ));
            }
            let verifier = if let Some(client_ca_file) = &tls_config.client_ca_file {
                let mut client_auth_roots = RootCertStore::empty();
                for cert in read_cert(client_ca_file)? {
                    client_auth_roots.add(cert).map_err(|e| {
                        make_err!(Code::Internal, "Could not read client CA: {e:?}")
                    })?;
                }
                let crls = if let Some(client_crl_file) = &tls_config.client_crl_file {
                    let mut crl_reader = std::io::BufReader::new(
                        std::fs::File::open(client_crl_file)
                            .err_tip(|| format!("Could not open CRL file {client_crl_file}"))?,
                    );
                    CertificateRevocationListDer::pem_reader_iter(&mut crl_reader)
                        .collect::<Result<_, _>>()
                        .err_tip(|| format!("Could not extract CRLs from file {client_crl_file}"))?
                } else {
                    Vec::new()
                };
                WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                    .with_crls(crls)
                    .build()
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Could not create WebPkiClientVerifier: {e:?}"
                        )
                    })?
            } else {
                WebPkiClientVerifier::no_client_auth()
            };
            let mut config = TlsServerConfig::builder_with_provider(
                    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().into(),
                )
                .with_safe_default_protocol_versions()
                .map_err(|e| make_err!(Code::Internal, "TLS version error: {e:?}"))?
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| {
                    make_err!(Code::Internal, "Could not create TlsServerConfig : {e:?}")
                })?;

            config.alpn_protocols.push("h2".into());
            Ok(Some(TlsAcceptor::from(Arc::new(config))))
        })?;

        let socket_addr = http_config
            .socket_address
            .parse::<SocketAddr>()
            .map_err(|e| {
                make_input_err!("Invalid address '{}' - {e:?}", http_config.socket_address)
            })?;
        let tcp_listener = TcpListener::bind(&socket_addr).await?;
        let mut http = auto::Builder::new(TaskExecutor::default());
        // hyper 1.x requires a Timer on the http2 builder; the keepalive
        // PingPong, header-read, and stream-idle paths panic with
        // "You must supply a timer." on the first poll otherwise. The
        // panic fires regardless of whether keep_alive_interval is set
        // because http2's idle-stream tracker also schedules timer
        // events. Always install TokioTimer; the cost is one allocation
        // per connection and the panic surface is total: every gRPC
        // request fails with ClosedChannelException at the client.
        // (Production outage 2026-05-12: panic on every connection at
        // port 50051/50061 after infra commit db1ac5bf enabled
        // http2_keep_alive_interval=30 in prod-server.json5.)
        http.http2().timer(TokioTimer::new());

        let http_config = &http_config.advanced_http;
        if let Some(value) = http_config.http2_keep_alive_interval {
            http.http2()
                .keep_alive_interval(Duration::from_secs(u64::from(value)));
        }

        if let Some(value) = http_config.experimental_http2_max_pending_accept_reset_streams {
            http.http2()
                .max_pending_accept_reset_streams(usize::try_from(value).err_tip(
                    || "Could not convert experimental_http2_max_pending_accept_reset_streams",
                )?);
        }
        // Default to 16 MiB stream window and 128 MiB connection window
        // to avoid capping per-stream throughput at ~64 MB/s with 1ms RTT
        // (hyper's default of 64 KiB is too small for high-bandwidth links).
        http.http2().initial_stream_window_size(
            http_config
                .experimental_http2_initial_stream_window_size
                .unwrap_or(16 * 1024 * 1024),
        );
        http.http2().initial_connection_window_size(
            http_config
                .experimental_http2_initial_connection_window_size
                .unwrap_or(128 * 1024 * 1024),
        );
        if let Some(value) = http_config.experimental_http2_adaptive_window {
            http.http2().adaptive_window(value);
        }
        http.http2().max_frame_size(
            http_config
                .experimental_http2_max_frame_size
                .unwrap_or(4 * 1024 * 1024),
        );
        if let Some(value) = http_config.experimental_http2_max_concurrent_streams {
            http.http2().max_concurrent_streams(value);
        }
        if let Some(value) = http_config.experimental_http2_keep_alive_timeout {
            http.http2()
                .keep_alive_timeout(Duration::from_secs(u64::from(value)));
        }
        http.http2().max_send_buf_size(
            usize::try_from(
                http_config
                    .experimental_http2_max_send_buf_size
                    .unwrap_or(2 * 1024 * 1024),
            )
            .err_tip(|| "Could not convert http2_max_send_buf_size")?,
        );
        if http_config.experimental_http2_enable_connect_protocol == Some(true) {
            http.http2().enable_connect_protocol();
        }
        if let Some(value) = http_config.experimental_http2_max_header_list_size {
            http.http2().max_header_list_size(value);
        }
        info!("Ready, listening on {socket_addr}",);
        let graceful = GracefulShutdown::new();
        let mut accept_stop_rx = accept_stop_tx.subscribe();
        let (drain_tx, drain_rx) = oneshot::channel::<()>();
        #[cfg(target_family = "unix")]
        drain_receivers.push(drain_rx);
        #[cfg(not(target_family = "unix"))]
        drop(drain_rx);

        root_futures.push(Box::pin(async move {
            loop {
                select! {
                    accept_result = tcp_listener.accept() => {
                        match accept_result {
                            Ok((tcp_stream, remote_addr)) => {
                                // Disable Nagle's algorithm to reduce latency
                                // on small writes (e.g., gRPC frames).
                                if let Err(err) = tcp_stream.set_nodelay(true) {
                                    error!(
                                        target: "nativelink::services",
                                        ?err,
                                        "Failed to set TCP_NODELAY"
                                    );
                                }
                                // Enable TCP keepalive to detect dead connections.
                                // Uses system defaults (tcp_keepalive_time/intvl/probes).
                                let sock_ref = SockRef::from(&tcp_stream);
                                if let Err(err) = sock_ref.set_keepalive(true) {
                                    error!(
                                        target: "nativelink::services",
                                        ?err,
                                        "Failed to set SO_KEEPALIVE"
                                    );
                                }
                                // Set large socket buffers for 10 GbE throughput.
                                // BDP = 1.25 GB/s × 0.5ms RTT = 625 KB; 4 MiB
                                // provides headroom for bursts. Linux doubles the
                                // value internally for bookkeeping.
                                const SOCKET_BUF_SIZE: usize = 8 * 1024 * 1024;
                                if let Err(err) = sock_ref.set_send_buffer_size(SOCKET_BUF_SIZE) {
                                    error!(
                                        target: "nativelink::services",
                                        ?err,
                                        "Failed to set SO_SNDBUF"
                                    );
                                }
                                if let Err(err) = sock_ref.set_recv_buffer_size(SOCKET_BUF_SIZE) {
                                    error!(
                                        target: "nativelink::services",
                                        ?err,
                                        "Failed to set SO_RCVBUF"
                                    );
                                }
                                info!(
                                    target: "nativelink::services",
                                    ?remote_addr,
                                    ?socket_addr,
                                    "Client connected"
                                );

                                let (http, svc, maybe_tls_acceptor) =
                                    (http.clone(), svc.clone(), maybe_tls_acceptor.clone());
                                let watcher = graceful.watcher();

                                background_spawn!(
                                    name: "http_connection",
                                    fut: error_span!(
                                        "http_connection",
                                        remote_addr = %remote_addr,
                                        socket_addr = %socket_addr,
                                    ).in_scope(|| async move {
                                        // Serve the connection wrapped with graceful
                                        // shutdown. On SIGTERM, GracefulShutdown sends
                                        // HTTP/2 GOAWAY, letting in-flight RPCs finish.
                                        let result = if let Some(tls_acceptor) = maybe_tls_acceptor {
                                            match tls_acceptor.accept(tcp_stream).await {
                                                Ok(tls_stream) => {
                                                    let conn = http.serve_connection(
                                                        TokioIo::new(tls_stream),
                                                        TowerToHyperService::new(svc),
                                                    );
                                                    watcher.watch(conn).await
                                                }
                                                Err(err) => {
                                                    error!(?err, "Failed to accept tls stream");
                                                    return;
                                                }
                                            }
                                        } else {
                                            let conn = http.serve_connection(
                                                TokioIo::new(tcp_stream),
                                                TowerToHyperService::new(svc),
                                            );
                                            watcher.watch(conn).await
                                        };

                                        if let Err(err) = result {
                                            // Walk the error source chain looking
                                            // for a std::io::Error so we can
                                            // downgrade normal connection-close
                                            // events to info level.
                                            let is_conn_close = {
                                                let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err.as_ref());
                                                let mut found = false;
                                                while let Some(e) = cur {
                                                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                                                        found = matches!(
                                                            io_err.kind(),
                                                            std::io::ErrorKind::BrokenPipe
                                                            | std::io::ErrorKind::ConnectionReset
                                                            | std::io::ErrorKind::ConnectionAborted
                                                        );
                                                        break;
                                                    }
                                                    cur = e.source();
                                                }
                                                found
                                            };
                                            if is_conn_close {
                                                info!(
                                                    target: "nativelink::services",
                                                    ?err,
                                                    "client disconnected"
                                                );
                                            } else {
                                                error!(
                                                    target: "nativelink::services",
                                                    ?err,
                                                    "Failed running service"
                                                );
                                            }
                                        }
                                    }),
                                    target: "nativelink::services",
                                    ?remote_addr,
                                    ?socket_addr,
                                );
                            },
                            Err(err) => {
                                error!(?err, "Failed to accept tcp connection");
                            }
                        }
                    },
                    _ = accept_stop_rx.changed() => {
                        let count = graceful.count();
                        info!(
                            %socket_addr,
                            count,
                            "SIGTERM: listener stopping, draining in-flight connections"
                        );
                        // Send HTTP/2 GOAWAY to all connections and wait for
                        // in-flight RPCs to complete. Timeout ensures we don't
                        // block shutdown indefinitely.
                        if count > 0 {
                            tokio::select! {
                                _ = graceful.shutdown() => {
                                    info!(%socket_addr, "all connections drained");
                                }
                                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                                    warn!(%socket_addr, "connection drain timed out after 30s");
                                }
                            }
                        }
                        let _ = drain_tx.send(());
                        break;
                    },
                }
            }
            Ok(())
        }));
        } // end ListenerConfig::Http

        #[cfg(feature = "quic")]
        ListenerConfig::Http3(h3_config) => {
            let socket_addr = h3_config
                .socket_address
                .parse::<SocketAddr>()
                .map_err(|e| {
                    make_input_err!("Invalid address '{}' - {e:?}", h3_config.socket_address)
                })?;

            // Load TLS cert + key for QUIC (TLS 1.3 is mandatory).
            let cert_pem = std::fs::read(&h3_config.cert_file)
                .err_tip(|| format!("Could not read cert file {}", h3_config.cert_file))?;
            let key_pem = std::fs::read(&h3_config.key_file)
                .err_tip(|| format!("Could not read key file {}", h3_config.key_file))?;

            let certs: Vec<CertificateDer<'static>> =
                CertificateDer::pem_reader_iter(&mut &cert_pem[..])
                    .collect::<Result<_, _>>()
                    .err_tip(|| "Could not parse PEM certs for QUIC")?;
            let key = PrivateKeyDer::from_pem_reader(&mut &key_pem[..])
                .err_tip(|| "Could not parse PEM key for QUIC")?;

            use tokio_rustls::rustls as rustls;

            fn read_cert_quic(cert_file: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
                let mut cert_reader = std::io::BufReader::new(
                    std::fs::File::open(cert_file)
                        .err_tip(|| format!("Could not open cert file {cert_file}"))?,
                );
                let certs = CertificateDer::pem_reader_iter(&mut cert_reader)
                    .collect::<Result<Vec<CertificateDer<'_>>, _>>()
                    .err_tip(|| format!("Could not extract certs from file {cert_file}"))?;
                Ok(certs)
            }

            // WebPkiClientVerifier::builder() needs a process-level crypto provider.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let verifier = if let Some(client_ca_file) = &h3_config.client_ca_file {
                let mut client_auth_roots = RootCertStore::empty();
                for cert in read_cert_quic(client_ca_file)? {
                    client_auth_roots.add(cert).map_err(|e| {
                        make_err!(Code::Internal, "Could not read QUIC client CA: {e:?}")
                    })?;
                }
                WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                    .build()
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Could not create QUIC WebPkiClientVerifier: {e:?}"
                        )
                    })?
            } else {
                WebPkiClientVerifier::no_client_auth()
            };

            let mut tls_config = rustls::ServerConfig::builder_with_provider(
                rustls::crypto::aws_lc_rs::default_provider().into(),
            )
            .with_safe_default_protocol_versions()
            .map_err(|e| make_err!(Code::Internal, "QUIC TLS version error: {e:?}"))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| make_err!(Code::Internal, "QUIC TLS config error: {e:?}"))?;
            tls_config.alpn_protocols = vec![b"h3".to_vec()];
            tls_config.max_early_data_size = u32::MAX;

            let mut quic_server_config = quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
                    .map_err(|e| make_err!(Code::Internal, "Quinn server config error: {e:?}"))?,
            ));

            // Tune QUIC transport for 10 GbE LAN (~0.5ms RTT).
            // BDP = 1.25 GB/s × 0.5ms ≈ 625 KB. Use generous windows to
            // handle bursts and multiple concurrent streams.
            let mut transport = quinn::TransportConfig::default();
            transport.stream_receive_window((16 * 1024 * 1024u32).into()); // 16 MiB per stream (vs 1 MiB)
            transport.receive_window((128 * 1024 * 1024u32).into()); // 128 MiB connection (vs 24 MiB)
            transport.send_window(128 * 1024 * 1024); // 128 MiB (vs 24 MiB)
            transport.max_concurrent_bidi_streams(1024u32.into()); // vs 256
            transport.max_concurrent_uni_streams(1024u32.into());
            transport.initial_rtt(Duration::from_micros(500)); // 0.5ms LAN RTT (vs 333ms)
            // Reduce ACK delay from default 25ms to 5ms for LAN.
            // 1ms caused H3_FRAME_ERROR from BBR pacing instability.
            let mut ack_freq = quinn::AckFrequencyConfig::default();
            ack_freq.max_ack_delay(Some(Duration::from_millis(5)));
            transport.ack_frequency_config(Some(ack_freq));
            transport.max_idle_timeout(Some(Duration::from_secs(60).try_into().unwrap()));
            // Server-side keepalives prevent idle timeout when clients stall
            // mid-upload (flow control, network congestion, CPU load).
            transport.keep_alive_interval(Some(Duration::from_secs(5)));
            // BBR handles bursty workloads better than Cubic on high-BDP LAN.
            transport.congestion_controller_factory(Arc::new(
                quinn::congestion::BbrConfig::default(),
            ));
            // Enable QUIC MTU discovery for jumbo frames. Start at the
            // QUIC minimum (1200) and probe up to 8952 bytes (9000-byte
            // jumbo Ethernet MTU minus 40 IPv6 + 8 UDP headers).
            // Reduces packet rate by ~6x vs default 1452, making AES-GCM
            // and per-packet processing proportionally cheaper.
            transport.initial_mtu(1200);
            let mut mtu_config = quinn::MtuDiscoveryConfig::default();
            mtu_config.upper_bound(8952);
            transport.mtu_discovery_config(Some(mtu_config));
            quic_server_config.transport_config(Arc::new(transport));

            // Pre-create UDP socket with large buffers and SO_REUSEPORT.
            // SO_REUSEPORT allows multiple sockets on the same port so the
            // kernel distributes incoming packets across them in parallel.
            let udp_socket = {
                let sock = socket2::Socket::new(
                    match socket_addr {
                        std::net::SocketAddr::V4(_) => socket2::Domain::IPV4,
                        std::net::SocketAddr::V6(_) => socket2::Domain::IPV6,
                    },
                    socket2::Type::DGRAM,
                    Some(socket2::Protocol::UDP),
                )
                .map_err(|e| make_err!(Code::Internal, "QUIC UDP socket: {e:?}"))?;
                sock.set_reuse_port(true)
                    .map_err(|e| make_err!(Code::Internal, "QUIC SO_REUSEPORT: {e:?}"))?;
                sock.set_nonblocking(true)
                    .map_err(|e| make_err!(Code::Internal, "QUIC nonblocking: {e:?}"))?;
                let bufs = nativelink_util::tls_utils::tune_quic_udp_buffers(
                    socket2::SockRef::from(&sock),
                    "server",
                );
                nativelink_util::tls_utils::warn_if_quic_udp_buffer_capped(bufs, "server");
                sock.bind(&socket_addr.into())
                    .map_err(|e| make_err!(Code::Internal, "QUIC UDP bind on {socket_addr}: {e:?}"))?;
                std::net::UdpSocket::from(sock)
            };

            let quinn_endpoint = quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(quic_server_config),
                udp_socket,
                quinn::default_runtime().ok_or_else(|| {
                    make_err!(Code::Internal, "No async runtime for QUIC endpoint")
                })?,
            )
            .map_err(|e| make_err!(Code::Internal, "Failed to create QUIC endpoint: {e:?}"))?;

            // Build tonic Routes from the same services.
            let routes = tonic_services;
            let acceptor = tonic_h3::quinn::H3QuinnAcceptor::new(quinn_endpoint.clone());
            let h3_router = tonic_h3::server::H3Router::new(routes);

            info!("Ready, listening on {socket_addr} (QUIC/HTTP3)");
            let mut quic_stop_rx = accept_stop_tx.subscribe();
            let (quic_drain_tx, quic_drain_rx) = oneshot::channel::<()>();
            #[cfg(target_family = "unix")]
            drain_receivers.push(quic_drain_rx);
            #[cfg(not(target_family = "unix"))]
            drop(quic_drain_rx);
            root_futures.push(Box::pin(async move {
                if let Err(err) = h3_router
                    .serve_with_shutdown(acceptor, async move {
                        let _ = quic_stop_rx.changed().await;
                        info!(%socket_addr, "QUIC/HTTP3 listener shutting down");
                    })
                    .await
                {
                    error!(?err, "QUIC/HTTP3 server error");
                }
                let _ = quic_drain_tx.send(());
                Ok(())
            }));
        }

        #[cfg(not(feature = "quic"))]
        ListenerConfig::Http3(_) => {
            return Err(make_err!(
                Code::InvalidArgument,
                "HTTP3/QUIC listener configured but the 'quic' feature is not enabled. \
                 Rebuild with: cargo build --features quic"
            ));
        }
        } // end match server_cfg.listener
    }

    {
        // We start workers after our TcpListener is setup so if our worker connects to one
        // of these services it will be able to connect.
        let worker_cfgs = cfg.workers.unwrap_or_default();
        let mut worker_names = HashSet::with_capacity(worker_cfgs.len());
        for (i, worker_cfg) in worker_cfgs.into_iter().enumerate() {
            let spawn_fut = match worker_cfg {
                WorkerConfig::Local(local_worker_cfg) => {
                    let fast_slow_store = store_manager
                        .get_store(&local_worker_cfg.cas_fast_slow_store)
                        .err_tip(|| {
                            format!(
                                "Failed to find store for cas_store_ref in worker config : {}",
                                local_worker_cfg.cas_fast_slow_store
                            )
                        })?;

                    let maybe_ac_store_ref =
                        local_worker_cfg.upload_action_result.ac_store.clone();
                    let maybe_ac_store = if let Some(ac_store_ref) = &maybe_ac_store_ref
                    {
                        Some(store_manager.get_store(ac_store_ref).err_tip(|| {
                            format!("Failed to find store for ac_store in worker config : {ac_store_ref}")
                        })?)
                    } else {
                        None
                    };
                    // Note: Defaults to fast_slow_store if not specified. If this ever changes it must
                    // be updated in config documentation for the `historical_results_store` the field.
                    let historical_store = if let Some(cas_store_ref) = &local_worker_cfg
                        .upload_action_result
                        .historical_results_store
                    {
                        store_manager.get_store(cas_store_ref).err_tip(|| {
                                format!(
                                "Failed to find store for historical_results_store in worker config : {cas_store_ref}"
                            )
                            })?
                    } else {
                        fast_slow_store.clone()
                    };
                    let local_worker = new_local_worker(
                        Arc::new(local_worker_cfg),
                        fast_slow_store,
                        maybe_ac_store,
                        maybe_ac_store_ref,
                        historical_store,
                    )
                    .await
                    .err_tip(|| "Could not make LocalWorker")?;

                    let name = if local_worker.name().is_empty() {
                        format!("worker_{i}")
                    } else {
                        local_worker.name().clone()
                    };

                    if worker_names.contains(&name) {
                        Err(make_input_err!(
                            "Duplicate worker name '{}' found in config",
                            name
                        ))?;
                    }
                    worker_names.insert(name.clone());
                    let shutdown_rx = shutdown_tx.subscribe();
                    let fut = trace_span!("worker_ctx", worker_name = %name)
                        .in_scope(|| local_worker.run(shutdown_rx));
                    spawn!("worker", fut, ?name)
                }
            };
            root_futures.push(Box::pin(spawn_fut.map_ok_or_else(|e| Err(e.into()), |v| v)));
        }
    }

    // Graceful SIGTERM handler: evict workers → stop accepting →
    // drain connections → flush writes → shut down local workers → exit.
    #[cfg(target_family = "unix")]
    {
        let shutdown_tx_clone = shutdown_tx.clone();
        // Clone schedulers so SIGTERM handler can evict workers before
        // draining connections. This ensures ConnectWorker streams close
        // promptly (no 30s timeout) and no new work is assigned during drain.
        let schedulers_for_shutdown: Vec<_> = worker_schedulers.values().cloned().collect();
        #[expect(clippy::disallowed_methods, reason = "signal handler spawned in inner_main")]
        tokio::spawn(async move {
            signal(SignalKind::terminate())
                .expect("Failed to listen to SIGTERM")
                .recv()
                .await;
            warn!("SIGTERM received, starting graceful shutdown");

            // Step 1: Evict all remote workers from schedulers. This closes
            // their ConnectWorker streams so port 50061 drains promptly,
            // and prevents the scheduler from assigning new work during drain.
            // Per-scheduler 10s timeout so a wedged backend can't stall SIGTERM.
            if !schedulers_for_shutdown.is_empty() {
                info!(
                    count = schedulers_for_shutdown.len(),
                    "evicting workers from schedulers"
                );
                let evict_start = std::time::Instant::now();
                let evict_guard = shutdown_guard.clone();
                for scheduler in &schedulers_for_shutdown {
                    if tokio::time::timeout(
                        Duration::from_secs(10),
                        scheduler.shutdown(evict_guard.clone()),
                    )
                    .await
                    .is_err()
                    {
                        warn!("scheduler shutdown timed out after 10s, continuing");
                    }
                }
                info!(
                    elapsed_ms = u64::try_from(evict_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "all workers evicted from schedulers"
                );
            }

            // Step 2: Stop accepting new connections. Each HTTP listener
            // sees this in its select! and starts draining via GOAWAY.
            let _ = accept_stop_tx.send(true);

            // Step 3: Wait for all listeners to finish draining in-flight
            // connections. Each listener has its own 30s drain timeout.
            // With workers already evicted, ConnectWorker streams should
            // close quickly so this should complete well under 30s.
            info!(
                listeners = drain_receivers.len(),
                "waiting for listeners to drain"
            );
            let drain_all = futures::future::join_all(drain_receivers);
            tokio::select! {
                _ = drain_all => {
                    info!("all listeners drained");
                }
                _ = tokio::time::sleep(Duration::from_secs(35)) => {
                    warn!("listener drain wait timed out after 35s");
                }
            }

            // Step 4: Flush in-flight background slow writes. All RPCs
            // have completed (or timed out), so all writes are queued.
            //
            // CRITICAL: skipping this step causes production
            // "Lost inputs no longer available remotely" Bazel failures.
            // The fast tier of cas_FAST_SLOW_STORE is a MemoryStore that
            // dies with the process. AC entries get written to Redis the
            // moment the fast write succeeds — if we exit before the
            // background slow write durably persists the blob to the
            // FilesystemStore, AC references a blob that no longer exists
            // anywhere. Bazel asks for the action result, the CAS doesn't
            // have it, and the build dies across many crates.
            //
            // We wrap with a wall-clock timeout so a wedged backend cannot
            // stall SIGTERM forever. The inner flush_slow_writes already
            // honors the same budget; this outer guard is belt-and-braces.
            if let Some(sm) = STORE_MANAGER.get() {
                let flush_budget = Duration::from_secs(30);
                let flush_start = std::time::Instant::now();
                info!(
                    timeout_secs = flush_budget.as_secs(),
                    "flushing in-flight slow writes before shutdown",
                );
                // The outer guard accommodates BOTH phases of
                // `StoreManager::flush_slow_writes`:
                //   Phase 1 (in-flight drain) ≤ flush_budget,
                //   Phase 2 (#210 MemoryStore→slow) ≥ 1s floor when
                //                                      Phase 1 used the full
                //                                      budget, plus its own
                //                                      2s outer-guard slack.
                // 8s of headroom keeps the SIGTERM-to-exit budget tight
                // (well under listener-drain's 35s) while leaving enough
                // room that the inner timeouts always fire first.
                match tokio::time::timeout(
                    flush_budget + Duration::from_secs(8),
                    sm.flush_slow_writes(flush_budget),
                )
                .await
                {
                    Ok(()) => info!(
                        elapsed_ms = u64::try_from(flush_start.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        "slow-write flush returned",
                    ),
                    Err(_) => warn!(
                        elapsed_ms = u64::try_from(flush_start.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        "slow-write flush wall-clock timeout fired; \
                         continuing with shutdown",
                    ),
                }
            } else {
                warn!(
                    "STORE_MANAGER not initialized at shutdown; \
                     skipping slow-write flush",
                );
            }

            // Step 5: Shut down local workers (20s budget). Remote workers
            // were already evicted in Step 1; this handles local workers
            // and the ShutdownGuard coordination.
            drop(shutdown_tx_clone.send(shutdown_guard.clone()));
            tokio::select! {
                result = async {
                    // Use .ok() instead of .expect() — if the scheduler
                    // handler panics, we still want process::exit to run.
                    let _ = scheduler_shutdown_rx.await;
                    let () = shutdown_guard.wait_for(Priority::P0).await;
                } => { let _ = result; }
                _ = tokio::time::sleep(Duration::from_secs(20)) => {
                    warn!("scheduler/worker shutdown timed out after 20s");
                }
            }

            warn!("graceful shutdown complete");
            std::process::exit(143);
        });
    }

    // Set up a shutdown handler for the worker schedulers.
    let mut shutdown_rx = shutdown_tx.subscribe();
    root_futures.push(Box::pin(async move {
        if shutdown_rx.recv().await.is_ok() {
            // Remote workers were already evicted in SIGTERM Step 1.
            // Signal Step 5 to proceed with ShutdownGuard coordination.
            let _ = scheduler_shutdown_tx.send(());
            drop(worker_schedulers);
        }
        Ok(())
    }));

    if let Err(e) = try_join_all(root_futures).await {
        panic!("{e:?}");
    }

    Ok(())
}

fn get_config() -> Result<CasConfig, Error> {
    let args = Args::parse();
    CasConfig::try_from_json5_file(&args.config_file)
}

/// Path to the runtime watchdog heartbeat file.
///
/// On Linux this lives in `/dev/shm` (tmpfs RAM-disk) so the heartbeat
/// write bypasses the tracing-appender backlog, the journald socket, and
/// any disk-tier stall — making it a reliable forensic signal during
/// journald-pressure events or txg sync stalls.
///
/// macOS has no `/dev/shm`; workers fall back to `/tmp/`. The macOS
/// workers don't run under journald pressure, so the disk-backed path
/// is acceptable. (Without this fallback the watchdog's `open()` fails
/// at startup on every Mac worker and runtime-stall stack dumps are
/// never captured — see task #101.)
#[cfg(target_os = "linux")]
const HEARTBEAT_FILE: &str = "/dev/shm/nativelink-heartbeat";
#[cfg(target_os = "macos")]
const HEARTBEAT_FILE: &str = "/tmp/nativelink-heartbeat";

/// Dump all thread stacks to a timestamped file for post-mortem analysis.
/// Reads /proc/self/task/*/comm, status, wchan, and stack (if permitted).
fn dump_thread_stacks() {
    nativelink_util::stall_detector::dump_thread_stacks("runtime-watchdog");
}

/// Write one line to the watchdog heartbeat file.
/// On Linux this lives in `/dev/shm` (tmpfs RAM-disk) so the write
/// bypasses the tracing-appender backlog and any disk-tier stall.
/// On macOS there is no `/dev/shm`; we fall back to `/tmp`.
/// Errors are ignored — the heartbeat is a best-effort forensic signal.
fn write_heartbeat(
    file: Option<&mut std::fs::File>,
    tick: u64,
    uptime_secs: u64,
    stall_count: u64,
    counter: u64,
    stall_event: Option<(&str, f64)>,
) {
    let Some(file) = file else { return };
    use std::io::Write;
    let wall_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let result = match stall_event {
        Some((state, secs)) => writeln!(
            file,
            "ts={wall_ts} tick={tick} uptime_secs={uptime_secs} stall_count={stall_count} counter={counter} stall={state} stall_secs={secs:.1}"
        ),
        None => writeln!(
            file,
            "ts={wall_ts} tick={tick} uptime_secs={uptime_secs} stall_count={stall_count} counter={counter}"
        ),
    };
    if result.is_ok() {
        let _ = file.flush();
    }
}

/// Sets the current thread's QoS class to USER_INITIATED on macOS so the
/// kernel prefers scheduling on performance cores instead of efficiency cores.
#[cfg(target_os = "macos")]
fn set_qos_user_initiated() {
    const QOS_CLASS_USER_INITIATED: u32 = 0x19;
    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    let ret = unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INITIATED, 0) };
    if ret != 0 {
        eprintln!("warning: failed to set QoS to USER_INITIATED: {ret}");
    }
}

#[cfg(not(target_os = "macos"))]
fn set_qos_user_initiated() {}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    // Install the rustls crypto provider early so WebPkiClientVerifier::builder()
    // and other rustls APIs that need a process-level provider can find it.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Install the per-thread backtrace signal handler BEFORE the tokio
    // runtime spawns worker threads. The default disposition for the
    // dump signal (SIGUSR2 on macOS, SIGRTMIN+1 on Linux) is process
    // termination — without this eager install, an external
    // `kill -USR2 $pid` (or any errant SIGUSR2) would kill the
    // process. We also need to install before
    // `spawn_external_dump_listener` so tokio's signal-hook chains our
    // sigaction as `prev` and invokes it on signal arrival; if tokio
    // installs first, signal-hook captures `SIG_DFL` as `prev` and our
    // slot-based capture is never called.
    nativelink_util::stall_detector::install_dump_signal_handler();

    // (#216) Warm the build-SHA cache BEFORE the tokio runtime starts.
    // `build_sha()` performs ~67 MiB of streaming I/O + SHA-256 on a
    // ~67 MiB release binary (~270 ms on a modern CPU). Done lazily
    // inside an async context (e.g. from
    // `make_connect_worker_request`) it would block one tokio worker
    // for the entire hash duration on the first connect; doing it
    // here makes every later call a nanosecond `OnceLock` hit. The
    // binary is the same on both server and worker startup paths
    // (same `nativelink` binary, role chosen by config) so the warm
    // is correct on both sides.
    let _ = nativelink_util::build_sha::build_sha();

    // Set QoS before runtime creation so tokio worker threads inherit
    // P-core scheduling preference via pthread_create QoS inheritance.
    set_qos_user_initiated();

    #[expect(clippy::disallowed_methods, reason = "starting main runtime")]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .on_thread_start(set_qos_user_initiated)
        // Large async state machines (especially in debug builds) need more
        // stack space than the default 2 MiB per worker thread.
        .thread_stack_size(8 * 1024 * 1024)
        // All file I/O uses spawn_blocking (benchmark showed 18-25x faster
        // than io_uring for reads, 2.4-3.3x for writes). 1024 blocking
        // threads allows high concurrent file I/O throughput.
        .max_blocking_threads(1024)
        .enable_all()
        .build()?;

    // Initialize the global rayon pool with a tokio handle bridge. This
    // must run before any rayon::spawn or blake3 mmap call so every rayon
    // worker thread carries the tokio runtime and any code (or Drop) that
    // touches a tokio API does not panic-abort the process.
    if let Err(e) = nativelink_util::rayon_pool::init_rayon_pool(runtime.handle().clone()) {
        eprintln!("failed to initialize rayon global pool: {e:?}");
        return Err(Box::new(e));
    }

    // Parse config before tracing init so we can read disable_otlp.
    let mut cfg = get_config()?;

    let global_cfg = if let Some(global_cfg) = &mut cfg.global {
        if global_cfg.max_open_files == 0 {
            global_cfg.max_open_files = fs::DEFAULT_OPEN_FILE_LIMIT;
        }
        if global_cfg.default_digest_size_health_check == 0 {
            global_cfg.default_digest_size_health_check = DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG;
        }

        global_cfg.clone()
    } else {
        GlobalConfig {
            max_open_files: fs::DEFAULT_OPEN_FILE_LIMIT,
            default_digest_hash_function: None,
            default_digest_size_health_check: DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG,
            pprof_port: 0,
            disable_otlp: true,
            nonblocking_log: true,
            worker_proxy_tls_ca_file: None,
            worker_proxy_tls_cert_file: None,
            worker_proxy_tls_key_file: None,
            bazel_facing_internal_chunking_enabled: false,
            small_blob_mirror_enabled: false,
        }
    };

    // The OTLP exporters need to run in a Tokio context
    // Do this first so all the other logging works
    let disable_otlp = global_cfg.disable_otlp;
    let nonblocking_log = global_cfg.nonblocking_log;
    #[expect(clippy::disallowed_methods, reason = "tracing init on main runtime")]
    runtime.block_on(async { tokio::spawn(async move { init_tracing(disable_otlp, nonblocking_log) }).await? })?;
    set_open_file_limit(global_cfg.max_open_files);
    set_default_digest_hasher_func(DigestHasherFunc::from(
        global_cfg
            .default_digest_hash_function
            .unwrap_or(ConfigDigestHashFunction::Sha256),
    ))?;
    set_default_digest_size_health_check(global_cfg.default_digest_size_health_check)?;

    // Start pprof HTTP server if configured and the feature is enabled.
    // Must enter the runtime context since start_pprof_server spawns a tokio task.
    #[cfg(feature = "pprof")]
    if global_cfg.pprof_port != 0 {
        let _guard = runtime.enter();
        match nativelink_util::pprof_server::start_pprof_server(global_cfg.pprof_port) {
            Ok(guard) => {
                // Leak the guard so the server lives for the process lifetime.
                std::mem::forget(guard);
                info!(port = global_cfg.pprof_port, "pprof HTTP server started");
            }
            Err(e) => {
                warn!(?e, port = global_cfg.pprof_port, "failed to start pprof HTTP server");
            }
        }
    }

    // Initiates the shutdown process by broadcasting the shutdown signal via the `oneshot::Sender` to all listeners.
    // Each listener will perform its cleanup and then drop its `oneshot::Sender`, signaling completion.
    // Once all `oneshot::Sender` instances are dropped, the worker knows it can safely terminate.
    let (shutdown_tx, _) = broadcast::channel::<ShutdownGuard>(BROADCAST_CAPACITY);

    #[expect(clippy::disallowed_methods, reason = "signal handler on main runtime")]
    runtime.spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen to SIGINT");
        eprintln!("User terminated process via SIGINT");
        std::process::exit(130);
    });

    #[allow(unused_variables)]
    let (scheduler_shutdown_tx, scheduler_shutdown_rx) = oneshot::channel();
    #[cfg(target_family = "unix")]
    let shutdown_guard = ShutdownGuard::default();

    // Spawn a heartbeat task inside the tokio runtime and an external
    // watchdog OS thread that detects when the runtime stalls.
    let heartbeat_counter = Arc::new(AtomicU64::new(0));
    let heartbeat_counter_task = heartbeat_counter.clone();
    #[expect(clippy::disallowed_methods, reason = "runtime watchdog heartbeat")]
    runtime.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            ticker.tick().await;
            heartbeat_counter_task.fetch_add(1, Ordering::Relaxed);
        }
    });
    std::thread::Builder::new()
        .name("runtime-watchdog".to_string())
        .spawn(move || {
            let stall_threshold = Duration::from_secs(2);
            let check_interval = Duration::from_secs(1);
            // Heartbeat written to HEARTBEAT_FILE bypasses tracing-appender,
            // systemd-journald, and stdio. During a journald-pressure event
            // (e.g. a co-tenant filling the journal socket) the in-band
            // logs go silent for minutes; this file's mtime + last line
            // tell post-mortem whether the runtime itself was alive:
            // - heartbeat updating, app logs missing -> appender blocked
            // - heartbeat frozen too -> OS thread starvation / process
            //   reclaim / mimalloc lockup
            // See module-level HEARTBEAT_FILE for the per-OS path choice.
            let mut heartbeat_file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(HEARTBEAT_FILE)
                .map_err(|e| {
                    eprintln!("watchdog: failed to open {HEARTBEAT_FILE}: {e}");
                    e
                })
                .ok();
            let watchdog_start = std::time::Instant::now();
            let heartbeat_interval = Duration::from_secs(30);
            let mut last_heartbeat = std::time::Instant::now();
            let mut heartbeat_tick: u64 = 0;
            let mut stall_count: u64 = 0;
            // Emit an immediate baseline tick so post-mortems can confirm
            // the heartbeat was wired up at startup.
            write_heartbeat(
                heartbeat_file.as_mut(),
                0,
                0,
                0,
                heartbeat_counter.load(Ordering::Relaxed),
                None,
            );
            loop {
                let before = heartbeat_counter.load(Ordering::Relaxed);
                std::thread::sleep(check_interval);
                let after = heartbeat_counter.load(Ordering::Relaxed);
                if before == after {
                    stall_count = stall_count.saturating_add(1);
                    let stall_start = std::time::Instant::now();
                    let mut stall_logged = false;
                    // Confirmed stall — wait until it resolves to measure duration.
                    loop {
                        std::thread::sleep(Duration::from_millis(100));
                        let now = heartbeat_counter.load(Ordering::Relaxed);
                        if now != after {
                            let stall_duration = stall_start.elapsed();
                            let total_secs = stall_duration.as_secs_f64()
                                + check_interval.as_secs_f64();
                            eprintln!(
                                "RUNTIME STALL RESOLVED: tokio runtime was unresponsive for {total_secs:.1}s (heartbeat stuck at {after})",
                            );
                            write_heartbeat(
                                heartbeat_file.as_mut(),
                                heartbeat_tick,
                                watchdog_start.elapsed().as_secs(),
                                stall_count,
                                now,
                                Some(("RESOLVED", total_secs)),
                            );
                            break;
                        }
                        if !stall_logged && stall_start.elapsed() > stall_threshold {
                            stall_logged = true;
                            let total = stall_threshold.as_secs_f64()
                                + check_interval.as_secs_f64();
                            eprintln!(
                                "RUNTIME STALL IN PROGRESS: tokio runtime unresponsive for >{total:.1}s (heartbeat stuck at {after})",
                            );
                            write_heartbeat(
                                heartbeat_file.as_mut(),
                                heartbeat_tick,
                                watchdog_start.elapsed().as_secs(),
                                stall_count,
                                after,
                                Some(("IN_PROGRESS", total)),
                            );
                            dump_thread_stacks();
                        }
                    }
                }
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    heartbeat_tick = heartbeat_tick.saturating_add(1);
                    last_heartbeat = std::time::Instant::now();
                    write_heartbeat(
                        heartbeat_file.as_mut(),
                        heartbeat_tick,
                        watchdog_start.elapsed().as_secs(),
                        stall_count,
                        after,
                        None,
                    );
                }
            }
        })
        .expect("Failed to spawn runtime watchdog thread");

    #[expect(clippy::disallowed_methods, reason = "waiting on everything to finish")]
    runtime
        .block_on(async {
            // Spawn the external SIGUSR2 listener inside the runtime
            // (tokio::signal::unix needs a tokio signal driver). The
            // sigaction was already installed pre-runtime; tokio's
            // signal-hook chains it as `prev`, so external SIGUSR2
            // wakes the listener AND lets our slot-based capture run
            // in the handler when an internal pthread_kill round is
            // active.
            nativelink_util::stall_detector::spawn_external_dump_listener();

            trace_span!("main")
                .in_scope(|| async {
                    inner_main(
                        cfg,
                        shutdown_tx,
                        scheduler_shutdown_tx,
                        #[cfg(target_family = "unix")]
                        scheduler_shutdown_rx,
                        #[cfg(target_family = "unix")]
                        shutdown_guard,
                    )
                    .await
                })
                .await
        })
        .err_tip(|| "main() function failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::HEARTBEAT_FILE;

    /// Guards against regressing the macOS watchdog fix from task #101.
    /// Linux must keep the `/dev/shm` tmpfs path so the heartbeat write
    /// bypasses tracing-appender; macOS has no `/dev/shm` and must use
    /// `/tmp` so `open()` does not fail at startup.
    #[test]
    fn heartbeat_file_path_is_per_os() {
        #[cfg(target_os = "linux")]
        assert!(
            HEARTBEAT_FILE.starts_with("/dev/shm/"),
            "Linux heartbeat path must live on tmpfs (/dev/shm/...), got {HEARTBEAT_FILE}"
        );
        #[cfg(target_os = "macos")]
        assert!(
            HEARTBEAT_FILE.starts_with("/tmp/"),
            "macOS heartbeat path must live under /tmp/ (no /dev/shm on macOS), got {HEARTBEAT_FILE}"
        );
        // Sanity: never empty, never relative.
        assert!(HEARTBEAT_FILE.starts_with('/'), "must be absolute");
    }
}
