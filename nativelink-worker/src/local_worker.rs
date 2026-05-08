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

use core::hash::BuildHasher;
use core::pin::Pin;
use core::str;
use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::env;
use std::process::Stdio;
use std::sync::{Arc, Weak};

use futures::future::{BoxFuture, OptionFuture};
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt, TryFutureExt, select};
use nativelink_config::cas_server::{EnvironmentSource, LocalWorkerConfig};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BisAck, BlobDigestInfo, BlobsAvailableNotification, BlobsInStableStorageChunk, ExecuteComplete,
    ExecuteResult, GoingAwayRequest, KeepAliveRequest, MirrorPinEntry, PeerHintsChunk,
    UpdateForWorker, chunked_message, execute_result,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_util::action_messages::{ActionResult, ActionStage, OperationId};
use nativelink_util::blob_locality_map::SharedBlobLocalityMap;
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::metrics_utils::{AsyncCounterWrapper, CounterWithTime};
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::store_trait::{ItemCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{spawn, tls_utils};
use opentelemetry::context::Context;
use parking_lot::Mutex;
use tokio::process;
use tokio::sync::{Notify, Semaphore, broadcast, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;
use tracing::{Level, debug, error, event, info, info_span, instrument, trace, warn};

use crate::running_actions_manager::{
    ExecutionConfiguration, Metrics as RunningActionManagerMetrics, RunningAction,
    RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use crate::worker_api_client_wrapper::{WorkerApiClientTrait, WorkerApiClientWrapper};
use crate::worker_utils::make_connect_worker_request;

/// Maximum backstop interval for BlobsAvailable reports (milliseconds).
/// The send loop normally wakes immediately on blob changes via `Notify`,
/// but this backstop ensures subtree-only changes (which don't fire the
/// tracker notify) are still reported within a bounded time.
/// At 100ms with 10 workers the server sees ~100 msgs/s worst case, each
/// coalesced via drain-then-fire. Empty ticks are skipped (no send when
/// there are no changes), so idle workers generate zero traffic.
const BLOBS_AVAILABLE_MAX_INTERVAL_MS: u64 = 100;

/// Platform-specific cumulative CPU time reading.
#[cfg(target_os = "linux")]
mod cpu_impl {
    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        let contents = std::fs::read_to_string("/proc/stat").ok()?;
        let line = contents.lines().next()?;
        if !line.starts_with("cpu ") {
            return None;
        }
        // fields: user(0) nice(1) system(2) idle(3) iowait(4) irq(5) softirq(6) steal(7)
        let fields: Vec<u64> = line[4..]
            .split_whitespace()
            .filter_map(|s| s.parse().ok())
            .collect();
        if fields.len() < 8 {
            return None;
        }
        let busy = fields[0] + fields[1] + fields[2] + fields[5] + fields[6] + fields[7];
        let total = busy + fields[3] + fields[4];
        Some(CpuTimes { busy, total })
    }
}

#[cfg(target_os = "macos")]
mod cpu_impl {
    const CPU_STATE_USER: usize = 0;
    const CPU_STATE_SYSTEM: usize = 1;
    const CPU_STATE_IDLE: usize = 2;
    const CPU_STATE_NICE: usize = 3;
    const CPU_STATE_MAX: usize = 4;
    const PROCESSOR_CPU_LOAD_INFO: i32 = 2;

    unsafe extern "C" {
        fn mach_host_self() -> u32;
        fn mach_task_self() -> u32;
        fn host_processor_info(
            host: u32,
            flavor: i32,
            out_processor_count: *mut u32,
            out_processor_info: *mut *mut i32,
            out_processor_info_cnt: *mut u32,
        ) -> i32;
        fn vm_deallocate(target_task: u32, address: usize, size: usize) -> i32;
    }

    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) struct PerTypeCpuTimes {
        pub(super) aggregate: CpuTimes,
        pub(super) p_core: CpuTimes,
        pub(super) e_core: CpuTimes,
        pub(super) has_e_cores: bool,
    }

    /// Returns the number of P-cores on Apple Silicon via sysctl.
    /// Returns 0 on Intel Macs (sysctl key doesn't exist).
    fn p_core_count() -> u32 {
        use std::sync::OnceLock;
        static COUNT: OnceLock<u32> = OnceLock::new();
        *COUNT.get_or_init(|| sysctl_u32("hw.perflevel0.logicalcpu").unwrap_or(0))
    }

    /// Returns the number of E-cores on Apple Silicon via sysctl.
    /// Returns 0 on Intel Macs or P-core-only Apple Silicon.
    fn e_core_count() -> u32 {
        use std::sync::OnceLock;
        static COUNT: OnceLock<u32> = OnceLock::new();
        *COUNT.get_or_init(|| sysctl_u32("hw.perflevel1.logicalcpu").unwrap_or(0))
    }

    fn sysctl_u32(name: &str) -> Option<u32> {
        use std::ffi::CString;
        let cname = CString::new(name).ok()?;
        let mut val: u32 = 0;
        let mut len = core::mem::size_of::<u32>();
        // SAFETY: sysctlbyname is a stable POSIX API on macOS.
        let ret = unsafe {
            libc::sysctlbyname(
                cname.as_ptr(),
                &raw mut val as *mut _,
                &mut len,
                core::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 { Some(val) } else { None }
    }

    /// Reads per-logical-CPU tick data via host_processor_info and splits
    /// into aggregate, P-core, and E-core buckets.
    pub(super) fn read_per_type_cpu_times() -> Option<PerTypeCpuTimes> {
        use std::sync::OnceLock;
        static HOST_PORT: OnceLock<u32> = OnceLock::new();

        let p_count = p_core_count();
        let e_count = e_core_count();

        // SAFETY: host_processor_info is a stable macOS kernel API.
        // We check the return code and deallocate the kernel-allocated buffer.
        unsafe {
            let host = *HOST_PORT.get_or_init(|| mach_host_self());
            let mut cpu_count: u32 = 0;
            let mut info_array: *mut i32 = core::ptr::null_mut();
            let mut info_count: u32 = 0;
            let ret = host_processor_info(
                host,
                PROCESSOR_CPU_LOAD_INFO,
                &mut cpu_count,
                &mut info_array,
                &mut info_count,
            );
            if ret != 0 || info_array.is_null() {
                return None;
            }

            // On Intel Macs, perflevel sysctl doesn't exist → p_count == 0.
            // Also guard against future chips where the counts don't add up
            // (e.g. a third core type) — fall back to treating all as P-cores.
            let is_heterogeneous = p_count > 0 && (p_count + e_count == cpu_count);

            let mut agg_busy = 0u64;
            let mut agg_total = 0u64;
            let mut p_busy = 0u64;
            let mut p_total = 0u64;
            let mut e_busy = 0u64;
            let mut e_total = 0u64;

            for i in 0..cpu_count {
                let base = (i as usize) * CPU_STATE_MAX;
                let user = *info_array.add(base + CPU_STATE_USER) as u64;
                let system = *info_array.add(base + CPU_STATE_SYSTEM) as u64;
                let idle = *info_array.add(base + CPU_STATE_IDLE) as u64;
                let nice = *info_array.add(base + CPU_STATE_NICE) as u64;
                let busy = user + system + nice;
                let total = busy + idle;
                agg_busy += busy;
                agg_total += total;
                if is_heterogeneous && i < p_count {
                    p_busy += busy;
                    p_total += total;
                } else if is_heterogeneous {
                    e_busy += busy;
                    e_total += total;
                }
            }

            // If not heterogeneous, all cores are P-cores.
            if !is_heterogeneous {
                p_busy = agg_busy;
                p_total = agg_total;
            }

            let kr = vm_deallocate(
                mach_task_self(),
                info_array as usize,
                (info_count as usize) * core::mem::size_of::<i32>(),
            );
            debug_assert_eq!(kr, 0, "vm_deallocate failed: {kr}");

            Some(PerTypeCpuTimes {
                aggregate: CpuTimes { busy: agg_busy, total: agg_total },
                p_core: CpuTimes { busy: p_busy, total: p_total },
                e_core: CpuTimes { busy: e_busy, total: e_total },
                has_e_cores: e_count > 0,
            })
        }
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        read_per_type_cpu_times().map(|t| t.aggregate)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod cpu_impl {
    pub(super) struct CpuTimes {
        pub(super) busy: u64,
        pub(super) total: u64,
    }

    pub(super) fn read_cpu_times() -> Option<CpuTimes> {
        None
    }
}

static CPU_PCT: AtomicU32 = AtomicU32::new(0);
static P_CORE_PCT: AtomicU32 = AtomicU32::new(0);
static E_CORE_PCT: AtomicU32 = AtomicU32::new(0);
static SAMPLER_STARTED: AtomicBool = AtomicBool::new(false);

/// Starts a dedicated OS thread that samples system-wide CPU utilization
/// every 100ms. Idempotent — only the first call spawns the thread.
fn start_cpu_sampler() -> Result<(), Error> {
    if SAMPLER_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        return Ok(());
    }
    std::thread::Builder::new()
        .name("cpu-sampler".into())
        .spawn(cpu_sample_loop)
        .map_err(|e| make_err!(Code::Internal, "failed to spawn cpu-sampler thread: {:?}", e))?;
    Ok(())
}

fn compute_pct(prev: &cpu_impl::CpuTimes, curr: &cpu_impl::CpuTimes) -> u32 {
    let total_delta = curr.total.wrapping_sub(prev.total);
    let busy_delta = curr.busy.wrapping_sub(prev.busy);
    if total_delta > 0 {
        ((busy_delta as f64 / total_delta as f64) * 100.0).round() as u32
    } else {
        0
    }
}

fn cpu_sample_loop() {
    // Monitoring thread — downgrade to UTILITY QoS so it doesn't
    // compete with real work for P-cores.
    #[cfg(target_os = "macos")]
    {
        const QOS_CLASS_UTILITY: u32 = 0x11;
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
        }
        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0) };
    }

    // Try per-type sampling first (macOS with host_processor_info).
    #[cfg(target_os = "macos")]
    {
        if let Some(initial) = cpu_impl::read_per_type_cpu_times() {
            per_type_sample_loop(initial);
            return; // unreachable — loop is infinite
        }
    }

    // Fallback: aggregate-only sampling (Linux, non-macOS, or Intel Mac
    // where host_processor_info failed).
    let mut prev = cpu_impl::read_cpu_times();
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let curr = cpu_impl::read_cpu_times();
        match (&prev, &curr) {
            (Some(p), Some(c)) => {
                CPU_PCT.store(compute_pct(p, c).min(100), Ordering::Relaxed);
            }
            _ => CPU_PCT.store(0, Ordering::Relaxed),
        }
        prev = curr;
    }
}

#[cfg(target_os = "macos")]
fn per_type_sample_loop(initial: cpu_impl::PerTypeCpuTimes) {
    let mut prev = initial;
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let Some(curr) = cpu_impl::read_per_type_cpu_times() else {
            CPU_PCT.store(0, Ordering::Relaxed);
            P_CORE_PCT.store(0, Ordering::Relaxed);
            E_CORE_PCT.store(0, Ordering::Relaxed);
            continue;
        };
        CPU_PCT.store(compute_pct(&prev.aggregate, &curr.aggregate).min(100), Ordering::Relaxed);
        P_CORE_PCT.store(compute_pct(&prev.p_core, &curr.p_core).min(100), Ordering::Relaxed);
        if curr.has_e_cores {
            E_CORE_PCT.store(compute_pct(&prev.e_core, &curr.e_core).min(100), Ordering::Relaxed);
        } else {
            // No E-cores → report as fully saturated so scheduler
            // doesn't think idle E-cores are available.
            E_CORE_PCT.store(100, Ordering::Relaxed);
        }
        prev = curr;
    }
}

/// Returns the current system-wide CPU utilization as a percentage (0-100),
/// sampled every 100ms by a dedicated OS thread.
fn get_cpu_load_pct() -> u32 {
    CPU_PCT.load(Ordering::Relaxed)
}

/// Returns the P-core CPU utilization (0-100). 0 means unknown (Linux or
/// non-heterogeneous CPU where per-core-type data is unavailable).
fn get_p_core_load_pct() -> u32 {
    P_CORE_PCT.load(Ordering::Relaxed)
}

/// Returns the E-core CPU utilization (0-100). 0 means unknown.
/// 100 on CPUs without E-cores (all cores are P-cores).
fn get_e_core_load_pct() -> u32 {
    E_CORE_PCT.load(Ordering::Relaxed)
}


/// Build the advertised gRPC endpoint for peer blob sharing.
/// Uses the machine's hostname so a single config works across all workers.
/// The hostname is resolved once and cached for the lifetime of the process.
/// When `use_tls` is true, advertises `grpcs://` so the server connects with TLS.
fn cas_advertised_endpoint(port: u16, use_tls: bool) -> String {
    use std::sync::OnceLock;
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    let hostname = HOSTNAME.get_or_init(|| {
        match hostname::get() {
            Ok(h) => {
                let name = h.to_string_lossy().into_owned();
                // Append .local for mDNS resolution if the hostname is bare
                // (no dots), so the server can resolve it via multicast DNS.
                if name.contains('.') {
                    name
                } else {
                    format!("{name}.local")
                }
            }
            Err(err) => {
                error!(
                    ?err,
                    "hostname::get() failed, using 'localhost' — peer blob sharing will not work across machines"
                );
                "localhost".to_string()
            }
        }
    });
    let scheme = if use_tls { "grpcs" } else { "grpc" };
    format!("{scheme}://{hostname}:{port}")
}

/// Start a QUIC/H3 server for the worker CAS, alongside the TCP server.
///
/// Generates a self-signed TLS certificate at startup (QUIC mandates TLS 1.3)
/// and binds a UDP socket on the same port as the TCP server. Peer workers
/// connecting with `use_http3: true` will use this QUIC endpoint for blob
/// fetches, benefiting from QUIC's built-in stream multiplexing.
#[cfg(feature = "quic")]
fn start_worker_quic_server(
    port: u16,
    worker_name: &str,
    routes: tonic::service::Routes,
) -> Result<JoinHandleDropGuard<Result<(), Error>>, Error> {
    use std::sync::Arc;
    use h3_quinn as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    // Generate self-signed certificate for this worker.
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        worker_name.to_string(),
    ])
    .map_err(|e| make_err!(Code::Internal, "Failed to generate self-signed cert: {e:?}"))?;

    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        cert.signing_key.serialize_der(),
    ));

    let mut tls_config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .map_err(|e| make_err!(Code::Internal, "Worker QUIC TLS version error: {e:?}"))?
    .with_no_client_auth()
    .with_single_cert(vec![cert_der], key_der)
    .map_err(|e| make_err!(Code::Internal, "Worker QUIC TLS config error: {e:?}"))?;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];
    tls_config.max_early_data_size = u32::MAX;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
            .map_err(|e| make_err!(Code::Internal, "Worker Quinn server config error: {e:?}"))?,
    ));

    // Tune QUIC transport for LAN usage.
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window((16 * 1024 * 1024u32).into());
    transport.receive_window((128 * 1024 * 1024u32).into());
    transport.send_window(128 * 1024 * 1024);
    transport.max_concurrent_bidi_streams(1024u32.into());
    transport.max_concurrent_uni_streams(1024u32.into());
    transport.initial_rtt(Duration::from_micros(500));
    // Match server/client idle timeout for consistent behavior.
    transport.max_idle_timeout(Some(Duration::from_secs(60).try_into().unwrap()));
    // Send QUIC keepalives every 5s to detect dead connections and
    // prevent NAT/firewall timeouts on the server→worker path.
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    // Enable QUIC MTU discovery for jumbo frames on LAN.
    transport.initial_mtu(1200);
    let mut mtu_config = quinn::MtuDiscoveryConfig::default();
    mtu_config.upper_bound(8952);
    transport.mtu_discovery_config(Some(mtu_config));
    server_config.transport_config(Arc::new(transport));

    // Bind UDP socket with large buffers.
    let socket_addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
    let udp_socket = std::net::UdpSocket::bind(socket_addr)
        .map_err(|e| make_err!(Code::Internal, "Worker QUIC UDP bind on {socket_addr}: {e:?}"))?;
    let bufs = nativelink_util::tls_utils::tune_quic_udp_buffers(
        socket2::SockRef::from(&udp_socket),
        "worker_peer",
    );
    nativelink_util::tls_utils::warn_if_quic_udp_buffer_capped(bufs, "worker_peer");

    let quinn_endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        udp_socket,
        quinn::default_runtime()
            .ok_or_else(|| make_err!(Code::Internal, "No async runtime for worker QUIC"))?,
    )
    .map_err(|e| make_err!(Code::Internal, "Failed to create worker QUIC endpoint: {e:?}"))?;

    let acceptor = tonic_h3::quinn::H3QuinnAcceptor::new(quinn_endpoint);
    let h3_router = tonic_h3::server::H3Router::new(routes);

    let worker_name = worker_name.to_string();
    info!(
        worker_name = %worker_name,
        %socket_addr,
        "Starting worker CAS QUIC/H3 server for peer blob sharing"
    );

    Ok(spawn!("worker_cas_quic", async move {
        if let Err(err) = h3_router.serve(acceptor).await {
            error!(?err, "Worker CAS QUIC/H3 server error");
            return Err(make_err!(Code::Internal, "Worker CAS QUIC server: {err:?}"));
        }
        Ok(())
    }))
}

/// Accumulated blob changes between BlobsAvailable ticks.
///
/// `added` and `touched` are reported in the same outgoing
/// `digest_infos` slice (the server's locality_map upserts both as
/// "present"); separating them lets the tracker maintain the invariant
/// that no digest is in more than one set at a time.
///
/// `touched` (cache hits via on_get) flow into the same slice as
/// `added` so the server's existing per-broadcast backfill check
/// (`request_missing_blob_uploads`) can pull hot blobs back into the
/// server CAS even if it had previously evicted them — without this,
/// hot-read-cold-write blobs silently age out of the server CAS.
#[derive(Debug, Default)]
pub struct BlobChanges {
    pub added: HashSet<DigestInfo>,
    pub evicted: HashSet<DigestInfo>,
    pub touched: HashSet<DigestInfo>,
}

/// Tracks inserts, evictions, and reads of the FilesystemStore between ticks.
/// Registered as a callback on the FilesystemStore's evicting map.
///
/// Contains a `Notify` that is signalled on every state transition so
/// the BlobsAvailable send loop can wake immediately instead of polling
/// on a fixed interval.
#[derive(Debug)]
pub struct BlobChangeTracker {
    pending: Mutex<BlobChanges>,
    /// Wakes the BlobsAvailable send loop when changes accumulate.
    notify: Arc<Notify>,
}

impl BlobChangeTracker {
    pub fn new(notify: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(BlobChanges::default()),
            notify,
        })
    }

    /// Atomically swap out accumulated changes, returning them.
    /// The internal state is replaced with an empty BlobChanges.
    pub fn swap(&self) -> BlobChanges {
        let mut pending = self.pending.lock();
        std::mem::take(&mut *pending)
    }
}

impl ItemCallback for BlobChangeTracker {
    // On evict: add to evicted, remove from added/touched.
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            pending.added.remove(&digest);
            pending.touched.remove(&digest);
            pending.evicted.insert(digest);
            self.notify.notify_one();
        }
        Box::pin(core::future::ready(()))
    }

    // On insert: add to added, remove from evicted/touched.
    fn on_insert(&self, store_key: StoreKey<'_>, _size: u64) {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            pending.evicted.remove(&digest);
            pending.touched.remove(&digest);
            pending.added.insert(digest);
            self.notify.notify_one();
        }
    }

    // On read (cache hit): record in touched IF the digest isn't already
    // accounted for in this window's added or evicted sets. This
    // surfaces blobs the worker is actively reading so the server's
    // backfill picks them up if its CAS evicted them.
    fn on_get(&self, store_key: StoreKey<'_>) {
        if let StoreKey::Digest(digest) = store_key {
            let mut pending = self.pending.lock();
            if pending.added.contains(&digest) || pending.evicted.contains(&digest) {
                return;
            }
            // Only wake the broadcast loop when this is a NEW touched
            // entry — repeat-read on the same digest between swaps would
            // otherwise pointlessly wake the loop.
            if pending.touched.insert(digest) {
                self.notify.notify_one();
            }
        }
    }
}

/// Amount of time to wait if we have actions in transit before we try to
/// consider an error to have occurred.
const ACTIONS_IN_TRANSIT_TIMEOUT_S: f32 = 10.;

/// If we lose connection to the worker api server we will wait this many seconds
/// before trying to connect.
const CONNECTION_RETRY_DELAY_S: f32 = 0.5;

/// Default endpoint timeout. If this value gets modified the documentation in
/// `cas_server.rs` must also be updated.
const DEFAULT_ENDPOINT_TIMEOUT_S: f32 = 5.;

/// Maximum decoded message size for the scheduler→worker `WorkerApi` stream.
///
/// Tonic's generated client default is 4 MiB. The worker receives the
/// `UpdateForWorker` oneof which today carries:
///   * `StartExecute` with pre-resolved directory trees up to 32 MiB
///     (`api_worker_scheduler::MAX_TREE_PROTO_BYTES`). Peer hints used to
///     ride here under a `MAX_PEER_HINTS = 16384` cap; #98 moved them to
///     `Update::ChunkedMessage(PeerHintsChunk)` so `StartExecute` no
///     longer balloons under high-locality workloads.
///   * `BlobsInStableStorage` with an unbounded `repeated Digest` list
///     (one entry per blob the server just persisted; a write burst of
///     thousands of blobs in a single message is plausible). #97 will
///     chunk this similarly.
///   * `Update::ChunkedMessage` payloads — capped per chunk by their
///     producer (e.g. `PEER_HINTS_PER_CHUNK = 256` ≈ 64 KiB), so the
///     decoder limit is not the bottleneck for chunked streams.
///
/// At the default 4 MiB limit, a large `StartExecute` or
/// `BlobsInStableStorage` would be silently rejected by the worker's tonic
/// decoder, breaking the connect_worker stream and forcing reconnection
/// (which in turn delays mirror unpinning and stalls dispatches).
///
/// 64 MiB matches the server-side listener default
/// (`DEFAULT_MAX_DECODING_MESSAGE_SIZE` in `src/bin/nativelink.rs`) and the
/// worker's CAS server (`WORKER_CAS_MAX_DECODING_MESSAGE_SIZE`), keeping
/// the cross-tier ceiling consistent.
pub const WORKER_API_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Default maximum amount of time a task is allowed to run for.
/// If this value gets modified the documentation in `cas_server.rs` must also be updated.
const DEFAULT_MAX_ACTION_TIMEOUT: Duration = Duration::from_secs(1200); // 20 mins.
const DEFAULT_MAX_UPLOAD_TIMEOUT: Duration = Duration::from_secs(600); // 10 mins.

/// Couples the worker's AC `FastSlowStore` handle with its configured
/// store-id name. The Some-iff-Some invariant — both fields are present
/// only when the worker's AC store is wired as a `FastSlowStore` — is
/// encoded by the type itself rather than spread across paired
/// `Option<...>` fields, so callers can't introduce a half-Some shape
/// by accident.
#[derive(Clone, Debug)]
pub struct AcMirrorTarget {
    /// The AC store's `FastSlowStore` instance. Pin advertisement
    /// inserts go via `fss.insert_local_ac_pin`; the BIS-ack drain
    /// goes via `fss.remove_local_ac_pins`.
    pub fss: Arc<FastSlowStore>,
    /// The AC store's configured name (e.g. `"AC_MAIN_STORE"`). Used
    /// as the `store_id` field in `MirrorPinEntry` so the server-side
    /// AC pin registry keys correctly.
    pub store_id: Arc<str>,
}

/// Holds the FilesystemStore reference and change tracker needed for
/// BlobsAvailable reporting with drain-then-fire semantics.
#[derive(Clone, Debug)]
pub struct BlobsAvailableState {
    /// Reference to the worker's local FilesystemStore (the fast store in FastSlowStore).
    fs_store: Arc<FilesystemStore>,
    /// Tracks inserted and evicted digests between sends.
    tracker: Arc<BlobChangeTracker>,
    /// The worker's CAS endpoint for peer serving (e.g. "grpc://192.168.100.5:50081").
    cas_endpoint: String,
    /// Woken by the tracker on every insert/eviction so the send loop fires
    /// immediately instead of sleeping for a fixed interval.
    notify: Arc<Notify>,
    /// Backstop interval: even without blob changes, wake periodically to
    /// pick up subtree-only deltas that bypass the tracker notify.
    max_interval: Duration,
    /// The FastSlowStore backing the worker's CAS server. Used to clean up
    /// mirror blobs when `BlobsInStableStorage` is received.
    cas_server_fss: Option<Arc<FastSlowStore>>,
    /// The worker's AC store wired as a `FastSlowStore`, when configured
    /// that way. Source of `pinned_ac_mirror_entries` (proto field 17)
    /// in the `BlobsAvailable` snapshot, and target of the AC-pin
    /// removal on `BlobsInStableStorage` ack. `None` when the worker
    /// has no AC store, or its AC store is a direct GrpcStore (no FSS
    /// wrap), or any wrapper hides the FSS that the
    /// `find_fast_slow_for_pin` walker can't see through.
    ac_mirror_target: Option<AcMirrorTarget>,
}

/// Test-only builder for [`BlobsAvailableState`]. Lets each test set only
/// the fields it cares about and rely on `Default` for the rest.
///
/// Replaces the previous `new_for_test` / `new_for_test_with_ac` factory
/// pair (#281 simplifier MAJOR-2): adding new optional state fields no
/// longer requires another constructor — extend this struct with a
/// sensible `Default` and existing callers stay green via
/// `..Default::default()`.
///
/// `fs_store` has no sensible default (every test needs its own
/// tempdir-backed store) so it's a required argument to
/// [`BlobsAvailableState::from_test_args`]; everything else defaults.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default)]
pub struct BlobsAvailableTestArgs {
    /// CAS-server `FastSlowStore` for tests that exercise the
    /// CAS-mirror cleanup path. `None` for tests that only need a
    /// `BlobsAvailableState` to drive non-CAS code paths.
    pub cas_server_fss: Option<Arc<FastSlowStore>>,
    /// AC-mirror target for tests that exercise AC-pin advertisement /
    /// unpin behavior.
    pub ac_mirror_target: Option<AcMirrorTarget>,
}

impl BlobsAvailableState {
    /// Test-only: build a `BlobsAvailableState` from a
    /// [`BlobsAvailableTestArgs`] builder. The non-test path
    /// constructs this inline inside `new_local_worker`.
    ///
    /// `fs_store` is the only required argument (no sensible default).
    /// All other fields default via [`BlobsAvailableTestArgs::default`];
    /// override only the ones the test cares about, e.g.
    ///
    /// ```ignore
    /// BlobsAvailableState::from_test_args(
    ///     fs_store,
    ///     BlobsAvailableTestArgs {
    ///         ac_mirror_target: Some(target),
    ///         ..Default::default()
    ///     },
    /// )
    /// ```
    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn from_test_args(
        fs_store: Arc<FilesystemStore>,
        args: BlobsAvailableTestArgs,
    ) -> Self {
        let BlobsAvailableTestArgs {
            cas_server_fss,
            ac_mirror_target,
        } = args;
        Self {
            fs_store,
            tracker: BlobChangeTracker::new(Arc::new(Notify::new())),
            cas_endpoint: String::new(),
            notify: Arc::new(Notify::new()),
            max_interval: Duration::from_secs(60),
            cas_server_fss,
            ac_mirror_target,
        }
    }
}

/// Process a `BatchWriteSmallBlobs` push from the server's
/// `SmallBlobDispatcher` (Bug A small-CAS peer-mirror; task #153).
///
/// For each `SmallBlobEntry`:
///   * Decode the proto digest into `DigestInfo` (skip + warn on
///     malformed).
///   * Validate `data.len() == digest.size_bytes()` (preserves the
///     load-bearing invariant from `fast_slow_store.rs:771-784`).
///   * Call `cas_server_fss.insert_dispatched_mirror_blob(store_id,
///     digest, data)`. Errors (cap exceeded, etc.) are logged but the
///     batch continues — partial-batch acceptance is OK because the
///     server's per-store `EphemeralServerSidePin` TTL will reclaim
///     the unacked entries.
///
/// Extracted from the `Update::BatchWriteSmallBlobs` match arm in
/// `LocalWorkerImpl::run` so the handler is unit-testable without
/// standing up the full scheduler/worker stream stack. The dispatch
/// arm is a thin call site; all behavior lives here.
pub fn handle_batch_write_small_blobs(
    cas_server_fss: Option<&Arc<FastSlowStore>>,
    blobs: &[nativelink_proto::com::github::trace_machina::nativelink::remote_execution::SmallBlobEntry],
) {
    let blob_count = blobs.len();
    let total_bytes: usize = blobs.iter().map(|b| b.data.len()).sum();
    let Some(fss) = cas_server_fss else {
        warn!(
            blob_count,
            total_bytes,
            "BatchWriteSmallBlobs: no cas_server_fss on this worker; dropping batch \
             (worker has no CAS server / mirror store — server should not have \
             dispatched here; check locality registration)"
        );
        return;
    };
    let mut inserted = 0usize;
    let mut skipped = 0usize;
    for entry in blobs {
        // Wire-side store_id validation (#168 producer wire-up review):
        // the worker writes `dispatched_mirror_pins[(store_id, digest)]`
        // which is later iterated to populate
        // `BlobsAvailableNotification.pinned_mirror_entries` (proto
        // field 16). A malformed `store_id` from a buggy or untrusted
        // server would (a) leak unbounded keys into the BTreeMap, and
        // (b) propagate to the wire ack, where the server's
        // `is_valid_store_id`-keyed pin-set lookup would silently fail
        // to unpin — creating a memory-leak path on the server. Reject
        // here with the same regex `enqueue` enforces (Rust-ident
        // shape per plan C11).
        if !nativelink_store::small_blob_dispatcher::is_valid_store_id(&entry.store_id) {
            warn!(
                store_id = entry.store_id,
                "BatchWriteSmallBlobs: invalid store_id (must match \
                 `[a-zA-Z_][a-zA-Z0-9_]*` per plan C11); skipping"
            );
            skipped += 1;
            continue;
        }
        let Some(proto_digest) = entry.digest.as_ref() else {
            warn!(
                store_id = entry.store_id,
                "BatchWriteSmallBlobs: entry has no digest; skipping"
            );
            skipped += 1;
            continue;
        };
        let digest = match DigestInfo::try_from(proto_digest.clone()) {
            Ok(d) => d,
            Err(err) => {
                warn!(
                    ?err,
                    store_id = entry.store_id,
                    "BatchWriteSmallBlobs: invalid digest, skipping"
                );
                skipped += 1;
                continue;
            }
        };
        if let Err(err) = fss.insert_dispatched_mirror_blob(
            &entry.store_id,
            digest,
            entry.data.clone(),
        ) {
            // insert_dispatched_mirror_blob already warns; bump the
            // skip counter and move on. Partial batches are fine
            // because the server's pin TTL recovers.
            warn!(
                ?err,
                store_id = entry.store_id,
                %digest,
                "BatchWriteSmallBlobs: insert_dispatched_mirror_blob failed; skipping"
            );
            skipped += 1;
            continue;
        }
        inserted += 1;
    }
    info!(
        blob_count,
        inserted,
        skipped,
        total_bytes,
        "BatchWriteSmallBlobs: batch processed"
    );
}

/// Outcome of one BIS unpin pass: how many digests were successfully
/// unpinned and how many failed (currently only digest-decode errors;
/// `unpin_digest` itself is infallible). Returned by
/// [`handle_blobs_in_stable_storage`] so the chunked caller
/// ([`handle_bis_chunk`]) can gate the ack on per-digest success — see
/// the doc comment on `handle_bis_chunk` for why partial failure must
/// suppress the ack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BisUnpinOutcome {
    /// Number of digests where the proto decoded AND every per-digest
    /// side effect (FilesystemStore::unpin_digest, mirror cleanup,
    /// failed_slow_writes ack) ran without error.
    pub unpinned: usize,
    /// Number of digests where the proto failed `DigestInfo::try_from`.
    /// All other unpin operations on this layer are infallible today,
    /// so this is the only failure mode currently observable; the count
    /// is exposed as a struct field to make future failure-mode growth
    /// non-breaking.
    pub failed: usize,
}

impl BisUnpinOutcome {
    /// True iff every digest in the input batch was processed without
    /// error. Used by `handle_bis_chunk` as the ack gate.
    #[inline]
    pub const fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

/// Process a `BlobsInStableStorage` notification from the server:
///   * Unpin the digests on the local FilesystemStore so they become
///     eligible for eviction.
///   * Drop them from the pending-upload (`failed_slow_writes`) set
///     so a reconnect doesn't re-upload them.
///   * Drop the in-memory mirror copies from the CAS server's
///     FastSlowStore — the server now has its own durable copy and
///     the worker no longer needs to hold one.
///
/// Returns a [`BisUnpinOutcome`] reporting how many digests
/// succeeded vs. failed. Today the only failure mode is
/// `DigestInfo::try_from` returning Err on a malformed proto digest;
/// `FilesystemStore::unpin_digest` and the mirror cleanup are both
/// infallible. The `Result`-shaped return type is preserved so future
/// failure-mode growth (e.g. disk-IO-backed unpin) does not require a
/// breaking signature change at every call-site.
///
/// Extracted from the `Update::BlobsInStableStorage` match arm in
/// `LocalWorkerImpl::run` so the handler is unit-testable without
/// standing up the full scheduler/worker stream stack. The dispatch
/// arm is a thin call site; all behavior lives here.
pub fn handle_blobs_in_stable_storage(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    proto_digests: &[nativelink_proto::build::bazel::remote::execution::v2::Digest],
) -> BisUnpinOutcome {
    handle_blobs_in_stable_storage_for_store(state, cas_store, "", proto_digests)
}

/// Variant of [`handle_blobs_in_stable_storage`] that takes the
/// chunk's `store_id` and dispatches to:
///
/// - Empty `store_id` (`""`): the historic CAS path — unpins from the
///   FilesystemStore, calls `cas_store.ack_digests`, drops `mirror_blobs`
///   from the CAS FSS. **Forward-compatible default for pre-AC-BIS
///   servers.**
/// - `store_id` matching this worker's configured AC store name: the
///   AC pin drain path — calls `remove_local_ac_pins` ONLY. Does NOT
///   touch the FilesystemStore (AC entries never lived there) and
///   does NOT touch `mirror_blobs` (same — AC pins never registered
///   there in this Option-A design).
/// - Unknown non-empty `store_id`: `warn!` and treat as a no-op (the
///   chunk is still acked so the server's resend buffer drains). Being
///   asked to unpin against a store this worker doesn't know about is
///   benign on the worker side; the registry mismatch is a server-side
///   config drift problem and surfaces in the warn log.
pub fn handle_blobs_in_stable_storage_for_store(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    store_id: &str,
    proto_digests: &[nativelink_proto::build::bazel::remote::execution::v2::Digest],
) -> BisUnpinOutcome {
    let digest_count = proto_digests.len();
    let mut decoded = 0usize;
    let mut failed = 0usize;
    let mut acked_digests: Vec<DigestInfo> = Vec::with_capacity(digest_count);
    for proto_digest in proto_digests {
        if let Ok(digest) = DigestInfo::try_from(proto_digest.clone()) {
            acked_digests.push(digest);
            decoded += 1;
        } else {
            failed += 1;
            warn!(
                ?proto_digest,
                "BlobsInStableStorage: invalid digest, skipping unpin"
            );
        }
    }

    // Dispatch on store_id. CRITICAL: AC chunks (non-empty store_id
    // matching the configured AC store) MUST NOT walk the CAS path;
    // routing AC digests through `cas_fss.remove_mirror_blobs` would
    // (a) walk the wrong byte map (zero overlap with AC entries), and
    // (b) walk `dispatched_mirror_pins` removing matches keyed by
    // digest only — collateral damage to CAS pins for the same digest.
    //
    // `unpinned` is set ONLY in the branches that actually mutate pin
    // state. The unknown-store_id and no-AC-target branches return
    // `unpinned = 0` so observability accurately reflects "did we do
    // anything" — the `warn!` is the only signal that the chunk was
    // received but unrouted, and the metric must not contradict it.
    let unpinned = if store_id.is_empty() {
        // CAS path — historic shape. Every decoded digest is unpinned
        // and acked; `unpinned` equals `decoded` here by construction.
        let fs_store = &state.fs_store;
        for digest in &acked_digests {
            fs_store.unpin_digest(digest);
        }
        if let Some(cas_store) = cas_store {
            cas_store.ack_digests(&acked_digests);
        }
        if let Some(cas_fss) = state.cas_server_fss.as_ref() {
            let before = cas_fss.mirror_blob_count();
            cas_fss.remove_mirror_blobs(&acked_digests);
            let removed = before - cas_fss.mirror_blob_count();
            if removed > 0 {
                info!(
                    removed,
                    remaining = cas_fss.mirror_blob_count(),
                    "BlobsInStableStorage CAS: removed mirror blobs from memory"
                );
            }
        }
        info!(
            unpinned = decoded,
            failed,
            digest_count,
            store_id = "",
            "BlobsInStableStorage CAS: unpinned digests from local CAS"
        );
        decoded
    } else if let Some(target) = state.ac_mirror_target.as_ref() {
        if target.store_id.as_ref() == store_id {
            target.fss.remove_local_ac_pins(&acked_digests);
            info!(
                unpinned = decoded,
                failed,
                digest_count,
                store_id,
                "BlobsInStableStorage AC: dropped local AC pins"
            );
            decoded
        } else {
            warn!(
                store_id,
                ac_store_id = %target.store_id,
                digest_count,
                "BlobsInStableStorage: store_id does not match this worker's \
                 configured AC store; treating as no-op (chunk will still be \
                 acked so server resend buffer drains)"
            );
            0
        }
    } else {
        warn!(
            store_id,
            digest_count,
            "BlobsInStableStorage: chunk carries non-empty store_id but this \
             worker has no AC mirror target; treating as no-op"
        );
        0
    };

    BisUnpinOutcome { unpinned, failed }
}

/// (#97) Process one `BlobsInStableStorageChunk` arriving on the
/// scheduler→worker stream and emit the matching `BisAck` IFF every
/// digest in the chunk was unpinned without error.
///
/// **Ack-on-success-only.** Per red-team finding #3 on the original
/// #97 PR: previously the ack fired unconditionally after the unpin
/// pass. If `handle_blobs_in_stable_storage` failed on any digest
/// (today: malformed proto), the ack still went out → the server
/// dropped the chunk from the resend buffer → on the next ConnectWorker
/// the worker never saw a replay → the failed digests stayed pinned
/// forever. Same outcome as the original #89 bug, different mechanism,
/// less detectable. The fix: the ack fires only when every digest in
/// the chunk was processed successfully (`outcome.all_succeeded()`).
/// On partial failure, an `error!` log records the chunk identity and
/// the failure count; the server's resend buffer keeps the chunk and
/// the next reconnect replays it.
///
/// **Empty-terminal still acks.** A chunk with zero digests
/// (`chunk_iter`'s empty-terminal contract) trivially succeeds — there
/// is nothing to fail on — so the ack fires and the server's resend
/// buffer slot is released. Without this, a broadcast whose final
/// chunk lands on the chunk-size boundary would leak its slot forever.
///
/// **Duplicate chunks ack on every delivery.** When a server resend
/// crosses an in-flight ack, the worker sees the same chunk twice;
/// every digest decodes again, every unpin is idempotent → outcome
/// is success → ack fires. The server's per-chunk slot is keyed on
/// `(broadcast_id, sequence)` so the second ack is a harmless
/// `HashMap::remove` on a missing key.
///
/// Returns the [`BisUnpinOutcome`] reporting per-digest success/failure
/// counts. Callers can use the failed-count for observability;
/// production callers MUST NOT bypass the ack-gate by calling
/// `ack_sink` themselves on partial failure.
pub fn handle_bis_chunk(
    state: &BlobsAvailableState,
    cas_store: Option<&Arc<FastSlowStore>>,
    chunk: &BlobsInStableStorageChunk,
    ack_sink: impl FnOnce(BisAck),
) -> BisUnpinOutcome {
    let outcome = handle_blobs_in_stable_storage_for_store(
        state,
        cas_store,
        &chunk.store_id,
        &chunk.digests,
    );
    if outcome.all_succeeded() {
        // Echo the server_instance_token from the chunk into the ack
        // (red-team #5: scheduler validates the token to drop acks
        // across server-bounces).
        (ack_sink)(BisAck {
            broadcast_id: chunk.broadcast_id,
            sequence: chunk.sequence,
            server_instance_token: chunk.server_instance_token,
        });
    } else {
        // Loud-log so operators see the unpin-failure rate. The server
        // will retain the chunk in its per-worker resend buffer and
        // replay on the next ConnectWorker.
        error!(
            target: "nativelink::bis_chunk_unpin_failure",
            broadcast_id = chunk.broadcast_id,
            sequence = chunk.sequence,
            unpinned = outcome.unpinned,
            failed = outcome.failed,
            digest_count = chunk.digests.len(),
            "BIS chunk had unpin failures; SUPPRESSING ack so server replays \
             chunk on next reconnect — without this gate a single malformed \
             digest in the chunk would leak the chunk's pin state forever"
        );
    }
    outcome
}

/// Process one `PeerHintsChunk` arriving on the scheduler→worker stream:
/// register every (digest, endpoints) pair into the worker's global
/// `peer_locality_map` so subsequent `WorkerProxyStore` reads can route
/// to peer workers.
///
/// Direct-merge design: NO buffer keyed on `operation_id`, NO wait for a
/// chunk-count predicate, NO race-elimination machinery. Each chunk's
/// hints are independently meaningful — a chunk that arrives BEFORE the
/// matching `StartAction` works fine (the worker has the hints early); a
/// chunk that arrives AFTER `input_fetch` started works fine too (the
/// hints simply aren't consulted; the worker falls back to the server CAS
/// or whatever locality state was already present).
///
/// Logged at `info!` so the chunk arrival cadence is visible in
/// production journals; the per-chunk count + sequence + is_last let
/// reviewers reconstruct the scheduler's emit pattern from logs alone.
pub fn handle_peer_hints_chunk(
    peer_locality_map: Option<&SharedBlobLocalityMap>,
    chunk: &PeerHintsChunk,
) {
    let Some(locality_map) = peer_locality_map else {
        // Worker built without peer-blob sharing (no `cas_server_port`).
        // Hints would be unused even if registered; drop them silently
        // at trace level.
        trace!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            is_last = chunk.is_last,
            hint_count = chunk.peer_hints.len(),
            "PeerHintsChunk received but worker has no peer_locality_map (peer sharing disabled)"
        );
        return;
    };
    let mut total_registered = 0usize;
    {
        // Single locked region per chunk so we don't pay N times the
        // contention cost for a 256-hint payload. The bottleneck of the
        // worker's read path is `WorkerProxyStore::lookup_workers`, which
        // takes a read lock; bursts of writes don't starve it because
        // parking_lot RwLock is fair.
        let mut map = locality_map.write();
        for hint in &chunk.peer_hints {
            let Some(ref digest_proto) = hint.digest else {
                continue;
            };
            let Ok(digest) = DigestInfo::try_from(digest_proto) else {
                continue;
            };
            for endpoint in &hint.peer_endpoints {
                map.register_blobs(endpoint, &[digest]);
                total_registered += 1;
            }
        }
    }
    // Per-chunk events are repetitive in the hot path (a 1M-hint dispatch
    // = ~3908 chunks). Demote to debug! for the per-chunk cadence; emit
    // an info! once per dispatch on the terminal chunk so journals still
    // record the state-transition "all hints for op_id are in".
    if chunk.is_last {
        info!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            hint_count = chunk.peer_hints.len(),
            registrations = total_registered,
            "PeerHintsChunk: terminal chunk applied; locality registrations complete"
        );
    } else {
        debug!(
            operation_id = %chunk.operation_id,
            sequence = chunk.sequence,
            hint_count = chunk.peer_hints.len(),
            registrations = total_registered,
            "PeerHintsChunk: registered hints into worker locality map"
        );
    }
}

struct LocalWorkerImpl<'a, T: WorkerApiClientTrait + 'static, U: RunningActionsManager> {
    config: &'a LocalWorkerConfig,
    // According to the tonic documentation it is a cheap operation to clone this.
    grpc_client: T,
    worker_id: String,
    running_actions_manager: Arc<U>,
    // Number of actions that have been received in `Update::StartAction`, but
    // not yet processed by running_actions_manager's spawn. This number should
    // always be zero if there are no actions running and no actions being waited
    // on by the scheduler.
    actions_in_transit: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    /// State for periodic BlobsAvailable reporting. None if disabled (no CAS endpoint).
    blobs_available_state: Option<BlobsAvailableState>,
    /// Worker-global locality map shared with `WorkerProxyStore`. When
    /// present, `Update::ChunkedMessage(PeerHints)` arms register hints
    /// directly into this map. None if peer-blob sharing is disabled
    /// (no `cas_server_port`).
    peer_locality_map: Option<SharedBlobLocalityMap>,
    /// Reference to the CAS server shutdown signal for graceful shutdown.
    cas_shutdown_tx: &'a Option<tokio::sync::watch::Sender<bool>>,
}

pub async fn preconditions_met<H: BuildHasher + Sync>(
    precondition_script: Option<String>,
    extra_envs: &HashMap<String, String, H>,
) -> Result<(), Error> {
    let Some(precondition_script) = &precondition_script else {
        // No script means we are always ok to proceed.
        return Ok(());
    };
    // TODO: Might want to pass some information about the command to the
    //       script, but at this point it's not even been downloaded yet,
    //       so that's not currently possible.  Perhaps we'll move this in
    //       future to pass useful information through?  Or perhaps we'll
    //       have a pre-condition and a pre-execute script instead, although
    //       arguably entrypoint already gives us that.

    let maybe_split_cmd = shlex::split(precondition_script);
    let (command, args) = match &maybe_split_cmd {
        Some(split_cmd) => (&split_cmd[0], &split_cmd[1..]),
        None => {
            return Err(make_input_err!(
                "Could not parse the value of precondition_script: '{}'",
                precondition_script,
            ));
        }
    };

    let precondition_process = process::Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env_clear()
        .envs(extra_envs)
        .spawn()
        .err_tip(|| format!("Could not execute precondition command {precondition_script:?}"))?;
    let output = precondition_process.wait_with_output().await?;
    let stdout = str::from_utf8(&output.stdout).unwrap_or("");
    trace!(status = %output.status, %stdout, "Preconditions script returned");
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        Err(make_err!(
            Code::ResourceExhausted,
            "Preconditions script returned status {} - {}",
            output.status,
            stdout
        ))
    }
}

impl<'a, T: WorkerApiClientTrait + 'static, U: RunningActionsManager> LocalWorkerImpl<'a, T, U> {
    fn new(
        config: &'a LocalWorkerConfig,
        grpc_client: T,
        worker_id: String,
        running_actions_manager: Arc<U>,
        metrics: Arc<Metrics>,
        blobs_available_state: Option<BlobsAvailableState>,
        peer_locality_map: Option<SharedBlobLocalityMap>,
        cas_shutdown_tx: &'a Option<tokio::sync::watch::Sender<bool>>,
    ) -> Self {
        Self {
            config,
            grpc_client,
            worker_id,
            running_actions_manager,
            // Number of actions that have been received in `Update::StartAction`, but
            // not yet processed by running_actions_manager's spawn. This number should
            // always be zero if there are no actions running and no actions being waited
            // on by the scheduler.
            actions_in_transit: Arc::new(AtomicU64::new(0)),
            metrics,
            blobs_available_state,
            peer_locality_map,
            cas_shutdown_tx,
        }
    }

    /// Upload blobs requested by the server's UploadMissingBlobs message.
    /// Reads from the local fast store and writes to the slow store (server CAS).
    async fn handle_upload_missing_blobs(
        running_actions_manager: &Arc<U>,
        digests: Vec<DigestInfo>,
    ) {
        let Some(cas_store) = running_actions_manager.get_cas_store() else {
            warn!("UploadMissingBlobs: no CAS store available, ignoring");
            return;
        };
        let slow_store = cas_store.slow_store();
        if slow_store
            .inner_store(None::<StoreKey<'_>>)
            .optimized_for(nativelink_util::store_trait::StoreOptimizations::NoopUpdates)
        {
            return;
        }
        // Use the FastSlowStore wrapper (not just `fast_store()`) so reads
        // transparently see mirror_blobs entries — the worker may hold a
        // pinned mirror copy that never landed on disk, and that is the
        // very copy the server is asking us to upload back.
        let cas_store_wrapped: Store = Store::new(cas_store.clone());

        // Check which blobs we actually have locally (disk OR mirror) before
        // uploading. FastSlowStore::has_with_results checks fast_store, the
        // in_flight_slow_writes map, and mirror_blobs.
        let keys: Vec<StoreKey<'_>> = digests
            .iter()
            .map(|d| StoreKey::from(*d))
            .collect();
        let mut results = vec![None; keys.len()];
        if let Err(err) = cas_store_wrapped.has_with_results(&keys, &mut results).await {
            warn!(?err, "UploadMissingBlobs: failed to check local store");
            return;
        }

        let present: Vec<DigestInfo> = digests
            .iter()
            .zip(results.iter())
            .filter_map(|(d, r)| if r.is_some() { Some(*d) } else { None })
            .collect();

        if present.is_empty() {
            info!(
                requested = digests.len(),
                "UploadMissingBlobs: none of the requested blobs found locally"
            );
            return;
        }

        info!(
            requested = digests.len(),
            found = present.len(),
            "UploadMissingBlobs: uploading blobs to server"
        );

        const MAX_CONCURRENT_UPLOADS: usize = 32;
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));

        let mut uploads: FuturesUnordered<_> = present
            .iter()
            .map(|&digest| {
                let cas_store_wrapped = cas_store_wrapped.clone();
                let slow_store = slow_store.clone();
                let semaphore = semaphore.clone();
                async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .expect("semaphore should not be closed");
                    // Use in-memory transfer for small blobs, streaming for
                    // large ones to avoid OOM on multi-GB blobs. Reads go
                    // through the FastSlowStore wrapper so mirror_blobs
                    // entries are visible.
                    const STREAMING_THRESHOLD: u64 = 1024 * 1024; // 1 MiB
                    let result = if digest.size_bytes() <= STREAMING_THRESHOLD {
                        match cas_store_wrapped.get_part_unchunked(digest, 0, None).await {
                            Ok(data) => slow_store.update_oneshot(digest, data).await,
                            Err(err) => Err(err),
                        }
                    } else {
                        let (tx, rx) = make_buf_channel_pair();
                        // Phase-tagged tracing — same instrumentation pattern
                        // as RunningActionsManagerImpl::spawn_upload_to_remote.
                        // Names which half of the streaming upload wedges so a
                        // 30s+ stall on UploadMissingBlobs surfaces the
                        // specific phase (fast read vs. gRPC send) in the log.
                        const SLOW_PHASE_WARN: Duration = Duration::from_secs(5);
                        let upload_phase_start = std::time::Instant::now();
                        let read_fut = async {
                            let phase_start = std::time::Instant::now();
                            let res = cas_store_wrapped.get(digest, tx).await;
                            let elapsed = phase_start.elapsed();
                            if elapsed >= SLOW_PHASE_WARN {
                                warn!(
                                    ?digest,
                                    size_bytes = digest.size_bytes(),
                                    elapsed_ms = elapsed.as_millis() as u64,
                                    "UploadMissingBlobs: slow fast-store read phase",
                                );
                            }
                            res
                        };
                        let write_fut = async {
                            let phase_start = std::time::Instant::now();
                            let res = slow_store
                                .update(
                                    digest,
                                    rx,
                                    UploadSizeInfo::ExactSize(digest.size_bytes()),
                                )
                                .await;
                            let elapsed = phase_start.elapsed();
                            if elapsed >= SLOW_PHASE_WARN {
                                warn!(
                                    ?digest,
                                    size_bytes = digest.size_bytes(),
                                    elapsed_ms = elapsed.as_millis() as u64,
                                    "UploadMissingBlobs: slow slow-store write phase (gRPC send)",
                                );
                            }
                            res
                        };
                        let (read_res, write_res) = tokio::join!(read_fut, write_fut);
                        let total_elapsed = upload_phase_start.elapsed();
                        if total_elapsed >= SLOW_PHASE_WARN {
                            warn!(
                                ?digest,
                                size_bytes = digest.size_bytes(),
                                total_elapsed_ms = total_elapsed.as_millis() as u64,
                                read_ok = read_res.is_ok(),
                                write_ok = write_res.is_ok(),
                                "UploadMissingBlobs: slow streaming upload (combined)",
                            );
                        }
                        if write_res.is_ok() {
                            Ok(())
                        } else {
                            read_res.merge(write_res)
                        }
                    };
                    match result {
                        Ok(()) => true,
                        Err(err) => {
                            warn!(
                                ?digest,
                                ?err,
                                "UploadMissingBlobs: failed to transfer blob"
                            );
                            false
                        }
                    }
                }
            })
            .collect();

        let mut uploaded = 0usize;
        let mut failed = 0usize;
        while let Some(ok) = uploads.next().await {
            if ok {
                uploaded += 1;
            } else {
                failed += 1;
            }
        }

        info!(
            uploaded,
            failed,
            total = present.len(),
            "UploadMissingBlobs: backfill complete"
        );
    }

    /// Starts a background spawn/thread that will send a message to the server every `timeout / 2`.
    async fn start_keep_alive(&self) -> Result<(), Error> {
        // According to tonic's documentation this call should be cheap and is the same stream.
        let mut grpc_client = self.grpc_client.clone();

        loop {
            let timeout = self
                .config
                .worker_api_endpoint
                .timeout
                .unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
            // We always send 2 keep alive requests per timeout. Http2 should manage most of our
            // timeout issues, this is a secondary check to ensure we can still send data.
            sleep(Duration::from_secs_f32(timeout / 2.)).await;
            let load = get_cpu_load_pct();
            let p_load = get_p_core_load_pct();
            let e_load = get_e_core_load_pct();
            debug!("KeepAlive cpu_load_pct={load} p_core={p_load} e_core={e_load}");
            if let Err(e) = grpc_client.keep_alive(KeepAliveRequest {
                cpu_load_pct: load,
                p_core_load_pct: p_load,
                e_core_load_pct: e_load,
            }).await {
                return Err(make_err!(
                    Code::Internal,
                    "Failed to send KeepAlive in LocalWorker : {:?}",
                    e
                ));
            }
        }
    }

    /// Sends a periodic BlobsAvailable notification.
    /// - First tick: full snapshot of all digests with timestamps (scans store once).
    ///   Also sends a full subtree snapshot with ALL subtree digests.
    /// - Subsequent ticks: delta from callback-accumulated changes (no scan).
    ///   Sends delta-encoded subtree changes (added/removed).
    async fn send_periodic_blobs_available(
        grpc_client: &mut T,
        state: &BlobsAvailableState,
        running_actions_manager: &Arc<U>,
        is_first: bool,
    ) -> Result<(), Error> {
        let (digest_infos, evicted_digests, pinned_mirror_digests) = if is_first {
            // Full snapshot: scan everything once.
            let all = state.fs_store.get_all_digests_with_timestamps();
            // Drain any changes that accumulated during startup.
            drop(state.tracker.swap());

            let infos: Vec<BlobDigestInfo> = all
                .iter()
                .map(|(digest, _ts)| BlobDigestInfo {
                    digest: Some((*digest).into()),
                })
                .collect();

            // Mirror digests: drain deltas FIRST, then take the snapshot
            // (atomically, under both mirror locks). If we snapshotted first
            // and then drained, a concurrent `remove_mirror_blobs` could land
            // between the two calls — its `removed` delta would be discarded
            // by the snapshot reset and the digest would never reach the
            // server's locality map cleanup. The snapshot covers all live
            // pins at the post-drain moment; drained `removed` deltas are
            // merged into `evicted_digests` so the locality map is cleaned.
            let (mirror_evicted_protos, mirror_pinned_protos) =
                if let Some(ref fss) = state.cas_server_fss {
                    let (mc, snap) = fss.snapshot_and_reset_mirror_changes();
                    let evicted: Vec<_> = mc.removed.into_iter().map(|d| d.into()).collect();
                    let pinned: Vec<_> = snap.into_iter().map(|d| d.into()).collect();
                    (evicted, pinned)
                } else {
                    (Vec::new(), Vec::new())
                };

            (infos, mirror_evicted_protos, mirror_pinned_protos)
        } else {
            // Delta: swap out accumulated changes. Touched digests (from
            // on_get cache hits) are merged with `added` so the server's
            // backfill check sees them; the proto carries no timestamps,
            // entries persist in the locality map until explicit eviction.
            let changes = state.tracker.swap();
            let mut all_present: HashSet<DigestInfo> =
                changes.added.into_iter().collect();
            all_present.extend(changes.touched.into_iter());

            let infos: Vec<BlobDigestInfo> = all_present
                .iter()
                .map(|digest| BlobDigestInfo {
                    digest: Some((*digest).into()),
                })
                .collect();
            let mut evicted_protos: Vec<_> =
                changes.evicted.iter().map(|d| (*d).into()).collect();

            // Mirror delta: drain → send `added` as `pinned_mirror_digests`
            // and merge `removed` into `evicted_digests` so the server cleans
            // up locality entries for blobs we no longer hold.
            let mirror_added_protos: Vec<_> =
                if let Some(ref fss) = state.cas_server_fss {
                    let mc = fss.drain_mirror_changes();
                    for d in mc.removed {
                        evicted_protos.push(d.into());
                    }
                    mc.added.into_iter().map(|d| d.into()).collect()
                } else {
                    Vec::new()
                };

            (infos, evicted_protos, mirror_added_protos)
        };

        // Collect subtree delta or full snapshot.
        let (cached_directory_digests, added_subtree_digests, removed_subtree_digests, is_full_subtree_snapshot) = if is_first {
            // Full subtree snapshot: send ALL subtree digests in cached_directory_digests.
            // Also drain any pending changes accumulated during startup.
            drop(running_actions_manager.take_pending_subtree_changes().await);
            let all_subtrees = running_actions_manager.all_subtree_digests().await;
            let all_subtree_protos = all_subtrees.into_iter().map(|d| d.into()).collect();
            (all_subtree_protos, Vec::new(), Vec::new(), true)
        } else {
            // Delta: take pending subtree changes.
            let (added, removed) = running_actions_manager.take_pending_subtree_changes().await;
            let added_protos = added.into_iter().map(|d| d.into()).collect();
            let removed_protos = removed.into_iter().map(|d| d.into()).collect();
            (Vec::new(), added_protos, removed_protos, false)
        };

        let new_or_touched_count = digest_infos.len();
        let evicted_count = evicted_digests.len();
        let cached_dir_count = cached_directory_digests.len();
        let added_subtree_count = added_subtree_digests.len();
        let removed_subtree_count = removed_subtree_digests.len();
        let pinned_mirror_count = pinned_mirror_digests.len();

        // Build the AC pin slice (proto field 17). Hard-partitioned from
        // the CAS pin slice (field 16): the AC entries flow into a
        // dedicated server-side `AcPinRegistry`, NOT the CAS-shared
        // `BlobLocalityMap`, so they cannot weaponize CAS upload-skip
        // short-circuits even on action_digest collisions. The snapshot
        // method filters by store_id; AC entries on a different store_id
        // would never reach this loop anyway under the type-system
        // invariants on `AcMirrorTarget`.
        let pinned_ac_mirror_entries: Vec<MirrorPinEntry> = state
            .ac_mirror_target
            .as_ref()
            .map(|target| {
                target
                    .fss
                    .dispatched_ac_pin_snapshot_for_store(target.store_id.as_ref())
                    .into_iter()
                    .map(|digest| MirrorPinEntry {
                        digest: Some(digest.into()),
                        store_id: target.store_id.to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let pinned_ac_mirror_count = pinned_ac_mirror_entries.len();

        // Skip sending if there are truly no changes at all.
        if !is_first
            && new_or_touched_count == 0
            && evicted_count == 0
            && added_subtree_count == 0
            && removed_subtree_count == 0
            && pinned_mirror_count == 0
            && pinned_ac_mirror_count == 0
        {
            trace!("BlobsAvailable: no changes since last tick, skipping");
            return Ok(());
        }

        let load = get_cpu_load_pct();
        let p_load = get_p_core_load_pct();
        let e_load = get_e_core_load_pct();
        debug!("BlobsAvailable cpu_load_pct={load} p_core={p_load} e_core={e_load}");
        let notification = BlobsAvailableNotification {
            worker_cas_endpoint: state.cas_endpoint.clone(),
            digests: Vec::new(),
            is_full_snapshot: is_first,
            evicted_digests,
            digest_infos,
            cpu_load_pct: load,
            cached_directory_digests,
            added_subtree_digests,
            removed_subtree_digests,
            is_full_subtree_snapshot,
            p_core_load_pct: p_load,
            e_core_load_pct: e_load,
            pinned_mirror_digests,
            // Mirror capacity report (review #1): server's picker uses
            // these to filter peers that cannot fit a blob BEFORE
            // consuming the source stream. `(0, 0)` for workers with
            // no CAS server / mirror store ⇒ picker treats as unknown
            // and disables the filter for this endpoint.
            mirror_used_bytes: state
                .cas_server_fss
                .as_ref()
                .map_or(0, |fss| fss.mirror_blobs_used_bytes()),
            mirror_max_bytes: state
                .cas_server_fss
                .as_ref()
                .map_or(0, |fss| fss.mirror_blobs_max_bytes()),
            // Field 16 (task #168 item 5): the dispatcher-pushed
            // pin snapshot keyed by (store_id, digest). Iterates the
            // FastSlowStore's `dispatched_mirror_pins` BTreeMap so the
            // order is sorted by `store_id` ASCII (then by DigestInfo)
            // — the precondition for the server's binary-search
            // self-filter in `EphemeralServerSidePin::observe_pinned_mirror_ack`.
            // Empty when the dispatcher has pushed nothing OR when
            // there is no `cas_server_fss` on this worker.
            //
            // CAS-only by construction: the snapshot iterates the CAS
            // FSS's pin map, which contains exactly the CAS dispatcher
            // pins (`insert_dispatched_mirror_blob`). AC pins live on
            // a different `FastSlowStore` instance (the AC FSS) and
            // ride field 17 below; the two slices CANNOT overlap and
            // CAS readers consuming this field cannot see AC entries.
            pinned_mirror_entries: state
                .cas_server_fss
                .as_ref()
                .map(|fss| {
                    fss.dispatched_mirror_pin_snapshot()
                        .into_iter()
                        .map(|(store_id, digest)| MirrorPinEntry {
                            digest: Some(digest.into()),
                            store_id: store_id.to_string(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            // AC pin snapshot, fed from a SEPARATE `FastSlowStore`
            // instance (the AC FSS). HARD-PARTITIONED from
            // `pinned_mirror_entries`: the server registers it in the
            // dedicated `AcPinRegistry` — never the CAS-shared
            // `BlobLocalityMap` — because `action_digest` IS by REAPI
            // design the same digest as the Action proto in CAS, so
            // routing AC pins through the locality map would cause CAS
            // upload short-circuits to silently skip uploads of the
            // Action proto bytes.
            pinned_ac_mirror_entries,
        };

        if let Err(err) = grpc_client.blobs_available(notification).await {
            warn!(
                ?err,
                new_or_touched_count,
                evicted_count,
                cached_dir_count,
                added_subtree_count,
                removed_subtree_count,
                pinned_mirror_count,
                is_first,
                "Failed to send periodic BlobsAvailable"
            );
            // Channel closed means the server dropped us — propagate to
            // trigger reconnect. The server also sends Update::Disconnect
            // when it detects "Worker not found", which is handled in run().
            return Err(err);
        } else {
            info!(
                new_or_touched_count,
                evicted_count,
                cached_dir_count,
                added_subtree_count,
                removed_subtree_count,
                pinned_mirror_count,
                is_first,
                "Sent periodic BlobsAvailable"
            );
        }
        Ok(())
    }

    async fn run(
        &self,
        update_for_worker_stream: Streaming<UpdateForWorker>,
        shutdown_rx: &mut broadcast::Receiver<ShutdownGuard>,
    ) -> Result<(), Error> {
        // This big block of logic is designed to help simplify upstream components. Upstream
        // components can write standard futures that return a `Result<(), Error>` and this block
        // will forward the error up to the client and disconnect from the scheduler.
        // It is a common use case that an item sent through update_for_worker_stream will always
        // have a response but the response will be triggered through a callback to the scheduler.
        // This can be quite tricky to manage, so what we have done here is given access to a
        // `futures` variable which because this is in a single thread as well as a channel that you
        // send a future into that makes it into the `futures` variable.
        // This means that if you want to perform an action based on the result of the future
        // you use the `.map()` method and the new action will always come to live in this spawn,
        // giving mutable access to stuff in this struct.
        // NOTE: If you ever return from this function it will disconnect from the scheduler.
        let mut futures = FuturesUnordered::new();
        futures.push(self.start_keep_alive().boxed());

        // Start BlobsAvailable reporting with drain-then-fire semantics.
        // The loop wakes immediately when blob changes are detected (via
        // Notify) and drains all accumulated changes in one send. Under
        // high load, changes accumulate while the previous send is in
        // flight and are picked up by the next iteration.
        if let Some(ref state) = self.blobs_available_state {
            let mut grpc_client = self.grpc_client.clone();
            let state = state.clone();
            // Pull a notify handle for mirror-blob inserts/removes so the
            // BlobsAvailable loop wakes promptly when the server pushes a
            // mirror copy to us. Pre-fix the loop only woke on FilesystemStore
            // changes — mirror writes were invisible until the next backstop
            // tick, and the mirror-TTL sweeper would sometimes drop the only
            // copy of a blob if the server was slow to ack stable storage.
            let mirror_notify =
                state.cas_server_fss.as_ref().map(|f| f.mirror_changes_notify());
            // Sibling notify on the AC FSS: wake when an AC entry is
            // newly written (insert_local_ac_pin) or BIS-acked
            // (remove_local_ac_pins). The AC FSS is a DIFFERENT
            // FastSlowStore instance than the CAS FSS, so each Notify
            // has its own waiter (single-consumer invariant preserved).
            let ac_notify = state
                .ac_mirror_target
                .as_ref()
                .map(|t| t.fss.mirror_changes_notify());
            let ram = self.running_actions_manager.clone();
            futures.push(
                async move {
                    // Send full snapshot immediately on connect so the
                    // server has an accurate locality map right away.
                    Self::send_periodic_blobs_available(
                        &mut grpc_client,
                        &state,
                        &ram,
                        true,
                    )
                    .await?;
                    loop {
                        // Wait for any of:
                        // 1. A FilesystemStore blob insert/eviction (immediate wake)
                        // 2. A CAS mirror-blob insert/remove (immediate wake — only
                        //    armed if a CAS server FastSlowStore exists)
                        // 3. An AC FSS pin insert/remove (immediate wake — only
                        //    armed if the worker's AC store is a FastSlowStore)
                        // 4. The backstop interval (catches subtree-only changes)
                        //
                        // Stack-pinned Notified instead of `Box::pin` per
                        // iteration — saves one heap allocation per
                        // BlobsAvailable wakeup. A fresh `Notified` is
                        // semantically required each iteration (it consumes
                        // exactly one notification permit), so the future
                        // itself must be re-created; `tokio::pin!` keeps it
                        // on the stack.
                        let mirror_wait = OptionFuture::from(
                            mirror_notify.as_deref().map(Notify::notified),
                        );
                        tokio::pin!(mirror_wait);
                        let ac_wait = OptionFuture::from(
                            ac_notify.as_deref().map(Notify::notified),
                        );
                        tokio::pin!(ac_wait);
                        tokio::select! {
                            () = state.notify.notified() => {}
                            Some(()) = &mut mirror_wait => {}
                            Some(()) = &mut ac_wait => {}
                            () = sleep(state.max_interval) => {}
                        }
                        Self::send_periodic_blobs_available(
                            &mut grpc_client,
                            &state,
                            &ram,
                            false,
                        )
                        .await?;
                    }
                }
                .boxed(),
            );

            // NOTE: The mirror-TTL sweeper that previously expired pinned
            // mirror blobs after 120s has been REMOVED. Mirror blobs are
            // pinned indefinitely and only released when the server sends
            // `BlobsInStableStorage` for the digest. During a server
            // restart the worker holds the only durable copy; an aggressive
            // TTL would drop that copy and lose data. The 2 GiB
            // `MIRROR_BLOBS_MAX_BYTES` cap is the only bound, and silent
            // drops at the cap are now logged at warn! level.
        }

        // On (re)connect, retry any failed background slow-store writes
        // so blobs that couldn't reach the server are re-uploaded.
        {
            let ram = self.running_actions_manager.clone();
            if let Some(cas_store) = ram.get_cas_store() {
                let failed = cas_store.drain_failed_digests();
                if !failed.is_empty() {
                    let count = failed.len();
                    info!(
                        count,
                        "retrying failed slow-store uploads on reconnect"
                    );
                    // Re-pin to refresh the pin timeout before uploading. We
                    // pin on the inner fast (FilesystemStore) directly because
                    // that is the store whose eviction we are guarding against;
                    // pinning through the wrapper would also forward to the
                    // slow store, which is meaningless for a remote GrpcStore.
                    #[allow(clippy::disallowed_methods)]
                    cas_store.fast_store().pin_digests(&failed);
                    tokio::spawn(async move {
                        Self::handle_upload_missing_blobs(&ram, failed).await;
                        info!(
                            count,
                            "reconnect: failed upload retry complete"
                        );
                    });
                }
            }
        }

        let (add_future_channel, add_future_rx) = mpsc::unbounded_channel();
        let mut add_future_rx = UnboundedReceiverStream::new(add_future_rx).fuse();

        let mut update_for_worker_stream = update_for_worker_stream.fuse();
        // A notify which is triggered every time actions_in_flight is subtracted.
        let actions_notify = Arc::new(Notify::new());
        // A counter of actions that are in-flight, this is similar to actions_in_transit but
        // includes the AC upload and notification to the scheduler.
        let actions_in_flight = Arc::new(AtomicU64::new(0));
        // Set to true when shutting down, this stops any new StartAction.
        let mut shutting_down = false;

        loop {
            select! {
                maybe_update = update_for_worker_stream.next() => if !shutting_down || maybe_update.is_some() {
                    let proto_update = maybe_update
                        .err_tip(|| "UpdateForWorker stream closed early")?
                        .err_tip(|| "Got error in UpdateForWorker stream")?
                        .update;
                    // Per plan B2 (USER OVERRIDE: no capability flag): when
                    // the server sends a NEW oneof variant that this worker
                    // does not know about, prost decodes the variant
                    // INSIDE the oneof but leaves the outer `update` as
                    // `None` (proto3 unknown-field skip). Pre-fix this
                    // path `?`-propagated "Expected update to exist in
                    // UpdateForWorker" and exited the connection task,
                    // creating an offline-worker-wakeup hot loop on
                    // server-side rollouts of new variants. Now we
                    // gracefully `warn!` + continue so old workers
                    // survive a rolling deploy of `BatchWriteSmallBlobs`
                    // (and any future variant added at the same site).
                    let Some(update) = proto_update else {
                        warn!(
                            "received UpdateForWorker with no recognized update variant; \
                             skipping (server may be running a newer build with a new oneof tag)"
                        );
                        continue;
                    };
                    match update {
                        Update::ConnectionResult(_) => {
                            return Err(make_input_err!(
                                "Got ConnectionResult in LocalWorker::run which should never happen"
                            ));
                        }
                        Update::Disconnect(()) => {
                            self.metrics.disconnects_received.inc();
                            return Err(make_err!(
                                Code::Internal,
                                "received disconnect from scheduler, will reconnect"
                            ));
                        }
                        Update::KeepAlive(()) => {
                            self.metrics.keep_alives_received.inc();
                        }
                        Update::KillOperationRequest(kill_operation_request) => {
                            let operation_id = OperationId::from(kill_operation_request.operation_id);
                            if let Err(err) = self.running_actions_manager.kill_operation(&operation_id).await {
                                error!(
                                    %operation_id,
                                    ?err,
                                    "Failed to send kill request for operation"
                                );
                            }
                        }
                        Update::TouchBlobs(touch_request) => {
                            // Touch blobs in the local store to update access times
                            // and prevent premature eviction of referenced blobs.
                            let digest_count = touch_request.digests.len();
                            trace!(digest_count, "Received TouchBlobs request");
                            if let Some(ref state) = self.blobs_available_state {
                                let fs_store = state.fs_store.clone();
                                let digests: Vec<DigestInfo> = touch_request
                                    .digests
                                    .into_iter()
                                    .filter_map(|d| DigestInfo::try_from(d).ok())
                                    .collect();
                                // Best-effort: call has() on each digest to update
                                // the EvictingMap's LRU access time.
                                let keys: Vec<StoreKey<'_>> = digests
                                    .iter()
                                    .map(|d| StoreKey::from(*d))
                                    .collect();
                                let mut results = vec![None; keys.len()];
                                if let Err(err) = Pin::new(fs_store.as_ref())
                                    .has_with_results(&keys, &mut results)
                                    .await
                                {
                                    warn!(
                                        ?err,
                                        digest_count,
                                        "TouchBlobs: failed to touch digests in FilesystemStore"
                                    );
                                } else {
                                    let found = results.iter().filter(|r| r.is_some()).count();
                                    trace!(
                                        digest_count,
                                        found,
                                        "TouchBlobs: touched digests in FilesystemStore"
                                    );
                                }
                            }
                        }
                        Update::BlobsInStableStorage(blobs) => {
                            let digest_count = blobs.digests.len();
                            info!(
                                target: "nativelink::stable_storage_received",
                                digest_count,
                                "BlobsInStableStorage: arm entered (BEFORE any gate)"
                            );
                            if let Some(ref state) = self.blobs_available_state {
                                info!(
                                    target: "nativelink::stable_storage_gate",
                                    digest_count,
                                    "blobs_available_state present, processing"
                                );
                                let cas_store_for_ack =
                                    self.running_actions_manager.get_cas_store();
                                handle_blobs_in_stable_storage(
                                    state,
                                    cas_store_for_ack.as_ref(),
                                    &blobs.digests,
                                );
                            } else {
                                warn!(
                                    target: "nativelink::stable_storage_gate",
                                    digest_count,
                                    "blobs_available_state is None, dropping unpin (BUG?)"
                                );
                                trace!(
                                    digest_count,
                                    "BlobsInStableStorage: no FilesystemStore available, ignoring"
                                );
                            }
                        }
                        Update::ChunkedMessage(chunked) => {
                            // (#98 / #97) Streaming protocol envelope. PeerHints
                            // chunks register into the worker's peer_locality_map
                            // (#98); BlobsInStableStorage chunks unpin local CAS
                            // entries + emit a BisAck so the server's per-worker
                            // resend buffer can drop the matching slot (#97).
                            match chunked.payload {
                                Some(chunked_message::Payload::PeerHints(chunk)) => {
                                    handle_peer_hints_chunk(
                                        self.peer_locality_map.as_ref(),
                                        &chunk,
                                    );
                                }
                                Some(chunked_message::Payload::BlobsInStableStorage(chunk)) => {
                                    let digest_count = chunk.digests.len();
                                    let broadcast_id = chunk.broadcast_id;
                                    let sequence = chunk.sequence;
                                    info!(
                                        target: "nativelink::stable_storage_chunked_received",
                                        broadcast_id,
                                        sequence,
                                        digest_count,
                                        is_last = chunk.is_last,
                                        "BIS chunk arm entered"
                                    );
                                    if let Some(ref state) = self.blobs_available_state {
                                        let cas_store_for_ack =
                                            self.running_actions_manager.get_cas_store();
                                        let mut grpc_client = self.grpc_client.clone();
                                        // Send the ack inline so the resend
                                        // buffer is released as soon as the
                                        // unpins land. The async send is
                                        // spawned to avoid blocking the
                                        // dispatch loop on a slow ack.
                                        handle_bis_chunk(
                                            state,
                                            cas_store_for_ack.as_ref(),
                                            &chunk,
                                            move |ack| {
                                                tokio::spawn(async move {
                                                    if let Err(err) = grpc_client.bis_ack(ack).await {
                                                        warn!(
                                                            ?err,
                                                            broadcast_id,
                                                            sequence,
                                                            "BIS ack send failed; server will resend on reconnect"
                                                        );
                                                    }
                                                });
                                            },
                                        );
                                    } else {
                                        warn!(
                                            target: "nativelink::stable_storage_chunked_gate",
                                            broadcast_id,
                                            sequence,
                                            digest_count,
                                            "blobs_available_state is None, dropping BIS chunk + ack (BUG?)"
                                        );
                                    }
                                }
                                None => {
                                    warn!(
                                        "Update::ChunkedMessage with empty payload from scheduler; ignoring"
                                    );
                                }
                            }
                        }
                        Update::UploadMissingBlobs(request) => {
                            // Server is requesting we upload blobs it doesn't
                            // have. Read from local fast store and upload to
                            // the slow store (server CAS) in the background.
                            let digest_count = request.digests.len();
                            let digests: Vec<DigestInfo> = request
                                .digests
                                .into_iter()
                                .filter_map(|d| DigestInfo::try_from(d).ok())
                                .collect();
                            info!(
                                digest_count,
                                valid_count = digests.len(),
                                "UploadMissingBlobs: server requests blob backfill"
                            );
                            let ram = self.running_actions_manager.clone();
                            tokio::spawn(async move {
                                Self::handle_upload_missing_blobs(&ram, digests).await;
                            });
                        }
                        Update::BatchWriteSmallBlobs(batch) => {
                            // Per plan §"Architecture summary": the server's
                            // SmallBlobDispatcher pushes a batch of small
                            // CAS/AC blobs (≤ SMALL_BLOB_THRESHOLD = 16 KiB)
                            // for the worker to hold in `mirror_blobs`. The
                            // worker advertises the snapshot via field 16
                            // `pinned_mirror_entries` on the next
                            // BlobsAvailableNotification, and the server's
                            // per-store `EphemeralServerSidePin` releases
                            // matching pins.
                            //
                            // The (store_id, digest) keying is INFORMATIONAL
                            // for now — per plan B5 the BTreeMap refactor
                            // is a follow-up; today the underlying mirror_blobs
                            // is keyed by DigestInfo. Multi-store collisions
                            // on the same digest will overwrite (last-writer
                            // wins). The dispatcher's feature flag is OFF in
                            // canary, so production is not yet exposed.
                            let cas_server_fss =
                                self.blobs_available_state.as_ref()
                                    .and_then(|s| s.cas_server_fss.as_ref());
                            handle_batch_write_small_blobs(cas_server_fss, &batch.blobs);
                        }
                        Update::StartAction(start_execute) => {
                            // Don't accept any new requests if we're shutting down.
                            if shutting_down {
                                if let Some(instance_name) = start_execute.execute_request.map(|request| request.instance_name) {
                                    self.grpc_client.clone().execution_response(
                                        ExecuteResult{
                                            instance_name,
                                            operation_id: start_execute.operation_id,
                                            result: Some(execute_result::Result::InternalError(make_err!(Code::ResourceExhausted, "Worker shutting down").into())),
                                        }
                                    ).await?;
                                }
                                continue;
                            }

                            self.metrics.start_actions_received.inc();

                            let execute_request = start_execute.execute_request.as_ref();
                            let operation_id = start_execute.operation_id.clone();
                            let operation_id_to_log = operation_id.clone();
                            let maybe_instance_name = execute_request.map(|v| v.instance_name.clone());
                            let action_digest = execute_request.and_then(|v| v.action_digest.clone());
                            let digest_hasher = execute_request
                                .ok_or_else(|| make_input_err!("Expected execute_request to be set"))
                                .and_then(|v| DigestHasherFunc::try_from(v.digest_function))
                                .err_tip(|| "In LocalWorkerImpl::new()")?;

                            let start_action_fut = {
                                let precondition_script_cfg = self.config.experimental_precondition_script.clone();
                                let mut extra_envs: HashMap<String, String> = HashMap::new();
                                if let Some(ref additional_environment) = self.config.additional_environment {
                                    for (name, source) in additional_environment {
                                        let value = match source {
                                            EnvironmentSource::Property(property) => start_execute
                                                .platform.as_ref().and_then(|p|p.properties.iter().find(|pr| &pr.name == property))
                                                .map_or_else(|| Cow::Borrowed(""), |v| Cow::Borrowed(v.value.as_str())),
                                            EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                                            EnvironmentSource::FromEnvironment => Cow::Owned(env::var(name).unwrap_or_default()),
                                            other => {
                                                debug!(?other, "Worker doesn't support this type of additional environment");
                                                continue;
                                            }
                                        };
                                        extra_envs.insert(name.clone(), value.into_owned());
                                    }
                                }
                                let actions_in_transit = self.actions_in_transit.clone();
                                let worker_id = self.worker_id.clone();
                                let running_actions_manager = self.running_actions_manager.clone();
                                self.metrics.clone().wrap(move |metrics| async move {
                                    metrics.preconditions.wrap(preconditions_met(precondition_script_cfg, &extra_envs))
                                    .and_then(|()| running_actions_manager.create_and_add_action(worker_id, start_execute))
                                    .map(move |r| {
                                        // Now that we either failed or registered our action, we can
                                        // consider the action to no longer be in transit.
                                        actions_in_transit.fetch_sub(1, Ordering::Release);
                                        r
                                    })
                                    .and_then(|action| {
                                        debug!(
                                            operation_id = %action.get_operation_id(),
                                            "Received request to run action"
                                        );
                                        // Box each phase to heap-allocate its future state
                                        // separately. Without this, the compiler generates a
                                        // single monolithic state machine for the entire
                                        // AndThen chain, which overflows the 8 MiB stack in
                                        // debug builds.
                                        Box::pin(action.clone().prepare_action())
                                            .and_then(|a| Box::pin(RunningAction::execute(a)))
                                            // upload_results now only uploads to the local fast store
                                            // (FilesystemStore). The remote CAS upload is deferred to
                                            // the background after the result is reported.
                                            .and_then(|a| Box::pin(RunningAction::upload_results(a)))
                                            .and_then(|a| Box::pin(RunningAction::get_finished_result(a)))
                                            .then(|result| async move {
                                                // Spawn cleanup in the background — it only removes
                                                // the work directory (files already renamed into CAS).
                                                // The cleaning_up_operations + wait_for_cleanup mechanism
                                                // handles the race if the same action is retried.
                                                tokio::spawn(async move {
                                                    if let Err(e) = action.cleanup().await {
                                                        error!(?e, "Background cleanup failed");
                                                    }
                                                });
                                                result
                                            })
                                    }).await
                                })
                            };

                            let make_publish_future = {
                                let mut grpc_client = self.grpc_client.clone();
                                let use_tls = self.config.cas_server_tls.is_some();
                                let cas_endpoint_for_notify = self.config.cas_server_port
                                    .map(|port| cas_advertised_endpoint(port, use_tls))
                                    .unwrap_or_default();

                                let running_actions_manager = self.running_actions_manager.clone();
                                move |res: Result<ActionResult, Error>| async move {
                                    // Sample CPU at completion time, not action start time.
                                    let exec_load = get_cpu_load_pct();
                                    let exec_p_load = get_p_core_load_pct();
                                    let exec_e_load = get_e_core_load_pct();
                                    debug!("ExecuteComplete cpu_load_pct={exec_load} p_core={exec_p_load} e_core={exec_e_load}");
                                    let complete = ExecuteComplete {
                                        operation_id: operation_id.clone(),
                                        cpu_load_pct: exec_load,
                                        p_core_load_pct: exec_p_load,
                                        e_core_load_pct: exec_e_load,
                                    };
                                    let instance_name = maybe_instance_name
                                        .err_tip(|| "`instance_name` could not be resolved; this is likely an internal error in local_worker.")?;
                                    match res {
                                        Ok(mut action_result) => {
                                            // External-consistency invariant (#129): every
                                            // blob the action produced MUST be observable
                                            // from the server (in CAS or via locality_map
                                            // peer-fetch) BEFORE the client sees the
                                            // ExecuteResult. The server populates the
                                            // locality_map from BlobsAvailable; the
                                            // worker→scheduler stream is processed in
                                            // arrival order, so sending BlobsAvailable
                                            // FIRST guarantees the locality_map is
                                            // populated by the time the server processes
                                            // ExecuteResult and forwards it to the client.
                                            //
                                            // Without this ordering, Bazel reads of any
                                            // tree-internal file digest in the race window
                                            // (between ExecuteResult delivery and
                                            // BlobsAvailable processing) hit NotFound:
                                            // the slow-tier upload is fire-and-forget
                                            // (step 5) and may not have completed, AND
                                            // the locality_map has no peer registered.
                                            //
                                            // 1. Tree expansion + BlobsAvailable on the
                                            //    critical path. Tree expansion reads Tree
                                            //    blobs from local CAS (just produced by
                                            //    upload_results, almost always hot in OS
                                            //    cache).
                                            if !cas_endpoint_for_notify.is_empty() {
                                                let mut output_digests = Vec::new();
                                                for file in &action_result.output_files {
                                                    output_digests.push(file.digest.into());
                                                }
                                                for folder in &action_result.output_folders {
                                                    output_digests.push(folder.tree_digest.into());
                                                }
                                                if action_result.stdout_digest.size_bytes() > 0 {
                                                    output_digests.push(action_result.stdout_digest.into());
                                                }
                                                if action_result.stderr_digest.size_bytes() > 0 {
                                                    output_digests.push(action_result.stderr_digest.into());
                                                }
                                                // Expand Tree protos to include individual
                                                // file digests in the locality map. Without
                                                // this, the server can't proxy reads for
                                                // tree file blobs until the background
                                                // upload completes.
                                                let tree_file_digests = running_actions_manager
                                                    .expand_tree_file_digests(&action_result)
                                                    .await;
                                                output_digests.extend(tree_file_digests.into_iter().map(Into::into));

                                                if !output_digests.is_empty() {
                                                    let load = get_cpu_load_pct();
                                                    let p_load = get_p_core_load_pct();
                                                    let e_load = get_e_core_load_pct();
                                                    debug!("BlobsAvailable cpu_load_pct={load} p_core={p_load} e_core={e_load}");
                                                    if let Err(err) = grpc_client.blobs_available(
                                                        BlobsAvailableNotification {
                                                            worker_cas_endpoint: cas_endpoint_for_notify.clone(),
                                                            digests: output_digests,
                                                            is_full_snapshot: false,
                                                            evicted_digests: Vec::new(),
                                                            digest_infos: Vec::new(),
                                                            cpu_load_pct: load,
                                                            cached_directory_digests: Vec::new(),
                                                            added_subtree_digests: Vec::new(),
                                                            removed_subtree_digests: Vec::new(),
                                                            is_full_subtree_snapshot: false,
                                                            p_core_load_pct: p_load,
                                                            e_core_load_pct: e_load,
                                                            pinned_mirror_digests: Vec::new(),
                                                            mirror_used_bytes: 0,
                                                            mirror_max_bytes: 0,
                                                            pinned_mirror_entries: Vec::new(),
                                                            pinned_ac_mirror_entries: Vec::new(),
                                                        }
                                                    ).await {
                                                        // Failure to send BlobsAvailable
                                                        // breaks external-consistency
                                                        // (server has no peer-locality for
                                                        // these blobs) but the action did
                                                        // succeed and the slow-tier upload
                                                        // is still scheduled. Log and
                                                        // continue — this is no worse than
                                                        // the pre-fix behaviour where
                                                        // BlobsAvailable was always
                                                        // best-effort.
                                                        warn!(?err, "Failed to send blobs_available notification");
                                                    }
                                                }
                                            }

                                            // 2. Send execution response. The server
                                            //    processes worker stream messages in
                                            //    arrival order; BlobsAvailable above is
                                            //    already enqueued so the locality_map will
                                            //    be populated before this ExecuteResult is
                                            //    forwarded to the client.
                                            //
                                            //    The server's inner_execution_response()
                                            //    also re-registers the result's top-level
                                            //    output digests as a redundant safety net
                                            //    (worker_api_server.rs:518).
                                            let action_stage = ActionStage::Completed(action_result.clone());
                                            grpc_client.execution_response(
                                                ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::ExecuteResponse(action_stage.into())),
                                                }
                                            )
                                            .await
                                            .err_tip(|| "Error while calling execution_response")?;

                                            // 3. Free the worker for new actions.
                                            drop(grpc_client.execution_complete(complete).await);

                                            // 4. AC write — needs &mut action_result so
                                            //    runs after the tree expansion (which
                                            //    borrows immutably) and after the
                                            //    locality-critical sends.
                                            if let Some(digest_info) = action_digest.clone().and_then(|action_digest| action_digest.try_into().ok()) {
                                                if let Err(err) = running_actions_manager.cache_action_result(digest_info, &mut action_result, digest_hasher).await {
                                                    error!(
                                                        ?err,
                                                        ?action_digest,
                                                        "Error saving action in store",
                                                    );
                                                }
                                            }

                                            // 5. Upload output blobs from local CAS to remote
                                            //    CAS in the background. This is fire-and-forget;
                                            //    peers can already serve the blobs directly.
                                            running_actions_manager.spawn_upload_to_remote(&action_result);
                                        },
                                        Err(e) => {
                                            // Still notify completion on error so the worker
                                            // is freed for new work.
                                            drop(grpc_client.execution_complete(complete).await);

                                            // Only convert to FAILED_PRECONDITION if this
                                            // is a CAS blob miss (from FastSlowStore). Other
                                            // NotFound errors (e.g., command binary not found,
                                            // missing output files) should propagate as-is.
                                            let err_msg = format!("{e:?}");
                                            if e.code == Code::NotFound
                                                && err_msg.contains("not found in")
                                            {
                                                // Per REAPI spec, missing inputs should return
                                                // FAILED_PRECONDITION so the client re-uploads.
                                                warn!(
                                                    ?e,
                                                    "Missing CAS inputs, returning FAILED_PRECONDITION"
                                                );
                                                // Re-stamp the code without losing the
                                                // attached PreconditionFailure details —
                                                // `make_err!` would drop them, breaking
                                                // Bazel's REAPI v2 §2.2.4 recovery path.
                                                let mut translated = e;
                                                translated.code = Code::FailedPrecondition;
                                                let action_result = ActionResult {
                                                    error: Some(translated),
                                                    ..ActionResult::default()
                                                };
                                                let action_stage = ActionStage::Completed(action_result);
                                                grpc_client.execution_response(ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::ExecuteResponse(action_stage.into())),
                                                }).await.err_tip(|| "Error calling execution_response with missing inputs")?;
                                            } else {
                                                grpc_client.execution_response(ExecuteResult{
                                                    instance_name,
                                                    operation_id,
                                                    result: Some(execute_result::Result::InternalError(e.into())),
                                                }).await.err_tip(|| "Error calling execution_response with error")?;
                                            }
                                        },
                                    }
                                    Ok(())
                                }
                            };

                            self.actions_in_transit.fetch_add(1, Ordering::Release);

                            let add_future_channel = add_future_channel.clone();

                            info_span!(
                                "worker_start_action_ctx",
                                operation_id = operation_id_to_log,
                                digest_function = %digest_hasher.to_string(),
                            ).in_scope(|| {
                                let _guard = Context::current_with_value(digest_hasher)
                                    .attach();

                                let actions_in_flight = actions_in_flight.clone();
                                let actions_notify = actions_notify.clone();
                                let actions_in_flight_fail = actions_in_flight.clone();
                                let actions_notify_fail = actions_notify.clone();
                                actions_in_flight.fetch_add(1, Ordering::Release);

                                futures.push(
                                    spawn!("worker_start_action", start_action_fut).map(move |res| {
                                        let res = res.err_tip(|| "Failed to launch spawn")?;
                                        if let Err(err) = &res {
                                            error!(?err, "Error executing action");
                                        }
                                        add_future_channel
                                            .send(make_publish_future(res).then(move |res| {
                                                actions_in_flight.fetch_sub(1, Ordering::Release);
                                                actions_notify.notify_one();
                                                core::future::ready(res)
                                            }).boxed())
                                            .map_err(|_| make_err!(Code::Internal, "LocalWorker could not send future"))?;
                                        Ok(())
                                    })
                                    .or_else(move |err| {
                                        // If the make_publish_future is not run we still need to notify.
                                        actions_in_flight_fail.fetch_sub(1, Ordering::Release);
                                        actions_notify_fail.notify_one();
                                        core::future::ready(Err(err))
                                    })
                                    .boxed()
                                );
                            });
                        }
                    }
                },
                res = add_future_rx.next() => {
                    let fut = res.err_tip(|| "New future stream receives should never be closed")?;
                    futures.push(fut);
                },
                res = futures.next() => res.err_tip(|| "Keep-alive should always pending. Likely unable to send data to scheduler")??,
                complete_msg = shutdown_rx.recv().fuse() => {
                    warn!("Worker loop received shutdown signal. Shutting down worker...",);
                    // Signal the worker CAS server to stop accepting new
                    // connections and drain in-flight blob transfers.
                    if let Some(tx) = self.cas_shutdown_tx {
                        let _ = tx.send(true);
                    }
                    let mut grpc_client = self.grpc_client.clone();
                    let shutdown_guard = complete_msg.map_err(|e| make_err!(Code::Internal, "Failed to receive shutdown message: {e:?}"))?;
                    let actions_in_flight = actions_in_flight.clone();
                    let actions_notify = actions_notify.clone();
                    let shutdown_future = async move {
                        // Wait for in-flight operations to be fully completed.
                        // #95: subscribe-before-predicate. Construct the
                        // `notified()` future and arm it via `enable()`
                        // BEFORE loading `actions_in_flight`. Any decrement
                        // (and accompanying `notify_one()` from the
                        // running-action completion sites at line ~2455
                        // and ~2464) issued from this point on is
                        // captured by the pre-armed Notified, even if it
                        // fires between the load and the await. Same
                        // shape as the cleanup_wait_notify reference in
                        // running_actions_manager.rs (~line 5720-5759)
                        // and #92.
                        loop {
                            let notified = actions_notify.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if actions_in_flight.load(Ordering::Acquire) == 0 {
                                break;
                            }
                            notified.as_mut().await;
                        }
                        // Sending this message immediately evicts all jobs from
                        // this worker, of which there should be none.
                        if let Err(e) = grpc_client.going_away(GoingAwayRequest {}).await {
                            error!("Failed to send GoingAwayRequest: {e}",);
                            return Err(e);
                        }
                        // Allow shutdown to occur now.
                        drop(shutdown_guard);
                        Ok::<(), Error>(())
                    };
                    futures.push(shutdown_future.boxed());
                    shutting_down = true;
                },
            };
        }
        // Unreachable.
    }
}

type ConnectionFactory<T> = Box<dyn Fn() -> BoxFuture<'static, Result<T, Error>> + Send + Sync>;

pub struct LocalWorker<T: WorkerApiClientTrait + 'static, U: RunningActionsManager> {
    config: Arc<LocalWorkerConfig>,
    running_actions_manager: Arc<U>,
    connection_factory: ConnectionFactory<T>,
    sleep_fn: Option<Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>>,
    metrics: Arc<Metrics>,
    /// State for periodic BlobsAvailable reporting.
    blobs_available_state: Option<BlobsAvailableState>,
    /// Worker-global locality map shared with `WorkerProxyStore`. Forwarded
    /// to `LocalWorkerImpl` so `Update::ChunkedMessage(PeerHints)` chunks
    /// can register hints directly without going through the action
    /// manager (#98 — peer-hints chunking, direct-merge design).
    peer_locality_map: Option<SharedBlobLocalityMap>,
    /// Guards for the worker CAS server tasks (TCP + QUIC). Keeps the tasks
    /// alive as long as the `LocalWorker` is alive. When dropped, servers abort.
    _cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
    /// Signals the worker CAS server to stop accepting connections during
    /// graceful shutdown. Sent `true` when the worker receives SIGTERM.
    cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl<
    T: WorkerApiClientTrait + core::fmt::Debug + 'static,
    U: RunningActionsManager + core::fmt::Debug,
> core::fmt::Debug for LocalWorker<T, U>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LocalWorker")
            .field("config", &self.config)
            .field("running_actions_manager", &self.running_actions_manager)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

/// Creates a new `LocalWorker`. The `cas_store` must be an instance of
/// `FastSlowStore` and will be checked at runtime.
///
/// `ac_store_name` is the configured store name (e.g. `"AC_MAIN_STORE"`)
/// for the AC store. When the AC store wraps a `FastSlowStore` (the
/// production shape), the worker registers AC pin entries in the FSS's
/// `dispatched_mirror_pins` index after each `upload_ac_results` so the
/// next `BlobsAvailable` tick advertises them via the dedicated
/// `pinned_ac_mirror_entries` field (proto field 17). `None` ⇒ no AC
/// store configured / no name to advertise.
pub async fn new_local_worker(
    config: Arc<LocalWorkerConfig>,
    cas_store: Store,
    ac_store: Option<Store>,
    ac_store_name: Option<String>,
    historical_store: Store,
) -> Result<LocalWorker<WorkerApiClientWrapper, RunningActionsManagerImpl>, Error> {
    start_cpu_sampler()?;

    let fast_slow_store = cas_store
        .downcast_ref::<FastSlowStore>(None)
        .err_tip(|| "Expected store for LocalWorker's store to be a FastSlowStore")?
        .get_arc()
        .err_tip(|| "FastSlowStore's Arc doesn't exist")?;

    // Log warning about CAS configuration for multi-worker setups
    event!(
        Level::INFO,
        worker_name = %config.name,
        "Starting worker '{}'. IMPORTANT: If running multiple workers, all workers \
        must share the same CAS storage path to avoid 'Object not found' errors.",
        config.name
    );

    if let Ok(path) = fs::canonicalize(&config.work_directory).await {
        fs::remove_dir_all(&path).await.err_tip(|| {
            format!(
                "Could not remove work_directory '{}' in LocalWorker",
                &path.as_path().to_str().unwrap_or("bad path")
            )
        })?;
    }

    fs::create_dir_all(&config.work_directory)
        .await
        .err_tip(|| format!("Could not make work_directory : {}", config.work_directory))?;
    let entrypoint = if config.entrypoint.is_empty() {
        None
    } else {
        Some(config.entrypoint.clone())
    };
    let max_action_timeout = if config.max_action_timeout == 0 {
        DEFAULT_MAX_ACTION_TIMEOUT
    } else {
        Duration::from_secs(config.max_action_timeout as u64)
    };
    let max_upload_timeout = if config.max_upload_timeout == 0 {
        DEFAULT_MAX_UPLOAD_TIMEOUT
    } else {
        Duration::from_secs(config.max_upload_timeout as u64)
    };

    // Whether the worker CAS server uses TLS (determines grpc:// vs grpcs:// in
    // the advertised endpoint).
    let use_tls = config.cas_server_tls.is_some();

    // If peer blob sharing is configured (cas_server_port is set), create a
    // worker-local locality map and wrap the slow store with WorkerProxyStore.
    // This enables workers to fetch blobs from peers instead of the central CAS.
    let (effective_cas_store, peer_locality_map) = if config.cas_server_port.is_some() {
        let locality_map = nativelink_util::blob_locality_map::new_shared_blob_locality_map();

        // Wrap the slow store (central CAS) with WorkerProxyStore.
        // Enable racing so the worker races peer fetches against server fetches.
        let slow_store = fast_slow_store.slow_store().clone();
        let mut proxy_arc =
            nativelink_store::worker_proxy_store::WorkerProxyStore::new(
                slow_store,
                locality_map.clone(),
            );
        Arc::get_mut(&mut proxy_arc)
            .expect("WorkerProxyStore just created, no other refs")
            .enable_race_peers();
        let proxy_store = Store::new(proxy_arc);

        // Build a new FastSlowStore: fast=local disk, slow=WorkerProxyStore(central CAS).
        // Preserve the original store's direction config so that e.g.
        // slow_direction=get prevents uploads from propagating to the server.
        //
        // Sibling-bug audit (review #7): `.fast_store()` here is store
        // *construction*, not a `has_with_results` lookup. We are wrapping
        // the on-disk `FilesystemStore` into a NEW `FastSlowStore` that
        // gets its own empty `mirror_blobs` map. There is no missed-mirror
        // hit risk because the new wrapper has no mirror state yet.
        // Construction-time wiring: extract the existing fast/slow handles to
        // re-wrap them in a new FastSlowStore — there is no wrapper to route
        // through here because the new wrapper does not exist yet.
        #[allow(clippy::disallowed_methods)]
        let fast_store = fast_slow_store.fast_store().clone();
        let fss_spec = nativelink_config::stores::FastSlowSpec {
            fast: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            slow: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            fast_direction: fast_slow_store.fast_direction(),
            slow_direction: fast_slow_store.slow_direction(),
            // Worker-side wrapper FSS; the chunked-read cascade lives
            // on the server, never on the worker, so leave OFF.
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        };
        let new_fss = FastSlowStore::new(&fss_spec, fast_store, proxy_store);
        info!(
            "Peer blob sharing enabled: wrapping slow store with WorkerProxyStore"
        );

        (new_fss, Some(locality_map))
    } else {
        (fast_slow_store.clone(), None)
    };

    // Initialize directory cache if configured.
    // This is done after effective_cas_store is created so the cache can use
    // the same FastSlowStore (with WorkerProxyStore) for batch downloads.
    let directory_cache = if let Some(cache_config) = &config.directory_cache {
        use std::path::PathBuf;

        use crate::directory_cache::{
            DirectoryCache, DirectoryCacheConfig as WorkerDirCacheConfig,
        };

        let cache_root = if cache_config.cache_root.is_empty() {
            PathBuf::from(&config.work_directory).parent().map_or_else(
                || PathBuf::from("/tmp/nativelink_directory_cache"),
                |p| p.join("directory_cache"),
            )
        } else {
            PathBuf::from(&cache_config.cache_root)
        };

        let worker_cache_config = WorkerDirCacheConfig {
            max_entries: cache_config.max_entries,
            max_size_bytes: cache_config.max_size_bytes,
            cache_root,
            direct_use_mode: cache_config.direct_use_mode,
        };

        match DirectoryCache::new(
            worker_cache_config,
            Store::new(effective_cas_store.clone()),
            Some(effective_cas_store.clone()),
        ).await {
            Ok(cache) => {
                tracing::info!("Directory cache initialized successfully");
                Some(Arc::new(cache))
            }
            Err(e) => {
                tracing::warn!("Failed to initialize directory cache: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    // The worker CAS server (which receives mirror writes from the server)
    // uses a separate FastSlowStore with slow_direction=ReadOnly. This
    // prevents mirror writes from being uploaded back to the server —
    // the blob is written to the local FilesystemStore only and pinned.
    // The server will ack via BlobsInStableStorage to unpin, or request
    // re-upload via UploadMissingBlobs on reconnect if it lost the blob.
    //
    // Both stores share the same failed_slow_writes set so that the
    // reconnect retry (which drains from the RunningActionsManager's
    // store) also picks up unacked mirror digests.
    //
    // `with_local_only_reads()` hard-codes p2p-source-only mode: the
    // public CAS server's reads MUST never fall through to the slow tier
    // (`GrpcStore`→server). On local miss we return NotFound so the
    // asking server routes to a different peer or serves from its own
    // CAS, instead of bouncing the request back through this worker's
    // slow tier — which would loop straight back to the same worker via
    // the locality map and wedge both ends. The regular `effective_cas_store`
    // above keeps its slow tier active for action input fetches inside
    // `RunningActionsManager`.
    let effective_cas_store_for_cas_server = {
        // Sibling-bug audit (review #7): `.fast_store()` here is store
        // *construction*. We rebuild a sibling FastSlowStore with the
        // same on-disk fast tier but ReadOnly slow direction. The new
        // wrapper has its own empty `mirror_blobs` map and is the one
        // that subsequently receives `IS_MIRROR_REQUEST` writes via the
        // CAS server, so the empty start state is correct.
        // Construction-time wiring: building a sibling FastSlowStore that
        // shares the same fast/slow store handles but flips slow_direction to
        // ReadOnly. We need the underlying Store handles, not a wrapper, so
        // there is nothing to route through.
        #[allow(clippy::disallowed_methods)]
        let fast_store = effective_cas_store.fast_store().clone();
        let slow_store = effective_cas_store.slow_store().clone();
        // `slow_direction = ReadOnly` is defensive only: with
        // `local_only_reads = true` the read paths short-circuit before
        // touching the slow tier, and `update()` early-returns under
        // `IS_MIRROR_REQUEST` before consulting `slow_direction`. Cost
        // is nil and the original 354 GB / 30 min bounce-loop is bad
        // enough to justify defense-in-depth.
        let fss_spec = nativelink_config::stores::FastSlowSpec {
            fast: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            slow: nativelink_config::stores::StoreSpec::Noop(Default::default()),
            fast_direction: effective_cas_store.fast_direction(),
            slow_direction: nativelink_config::stores::StoreDirection::ReadOnly,
            // Worker-side wrapper FSS; the chunked-read cascade lives
            // on the server, never on the worker, so leave OFF.
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        };
        FastSlowStore::new_with_shared_failed_writes(
            &fss_spec,
            fast_store,
            slow_store,
            &effective_cas_store,
        )
        .with_local_only_reads()
    };
    // Keep a reference for mirror blob cleanup in BlobsInStableStorage.
    let cas_server_fss = effective_cas_store_for_cas_server.clone();

    // Walk the AC store wrapper chain to find its underlying
    // `FastSlowStore`. Uses the same `find_fast_slow_for_pin` walker
    // that the CAS pin path uses, NOT a single-level downcast — a bare
    // downcast silently disables AC pin advertisement the moment any
    // wrapper (ExistenceCacheStore, VerifyStore, etc.) lands above the
    // FSS, since each wrapper presents its own Arc and a one-level
    // downcast misses the layered chain.
    //
    // Per the type-system invariant on `AcMirrorTarget`, both `fss`
    // and `store_id` are produced together — there is no "have one,
    // missing the other" half-Some shape.
    let ac_mirror_target: Option<AcMirrorTarget> = match (
        ac_store.as_ref(),
        ac_store_name.as_deref(),
    ) {
        (Some(store), Some(name)) => {
            // The walker borrows `&dyn StoreDriver` from the store
            // it's given; call `.inner_store(None)` to obtain a
            // borrow without requiring the store to clone its inner.
            let driver = store.inner_store(None::<StoreKey<'_>>);
            let fss_borrow =
                nativelink_store::small_blob_dispatcher::find_fast_slow_for_pin(driver);
            match fss_borrow.and_then(|fss| fss.get_arc()) {
                Some(fss) => {
                    info!(
                        ac_store_name = name,
                        "AC pin advertisement enabled — found FastSlowStore in AC chain"
                    );
                    Some(AcMirrorTarget {
                        fss,
                        store_id: Arc::from(name),
                    })
                }
                None => {
                    warn!(
                        ac_store_name = name,
                        "AC pin advertisement DISABLED — no FastSlowStore found in AC chain. \
                         AC writes still complete normally, but BlobsAvailable will not \
                         carry AC pins for this worker. If the production AC chain has \
                         changed shape (new wrapper above the FSS), extend \
                         `find_fast_slow_for_pin` to recurse through it."
                    );
                    None
                }
            }
        }
        _ => None,
    };

    let running_actions_manager =
        Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: config.work_directory.clone(),
            execution_configuration: ExecutionConfiguration {
                entrypoint,
                additional_environment: config.additional_environment.clone(),
            },
            cas_store: effective_cas_store,
            ac_store,
            ac_mirror_target: ac_mirror_target.clone(),
            historical_store,
            upload_action_result_config: &config.upload_action_result,
            max_action_timeout,
            max_upload_timeout,
            timeout_handled_externally: config.timeout_handled_externally,
            directory_cache,
        })?);

    // Set up BlobsAvailable reporting with drain-then-fire semantics.
    // The send loop wakes immediately on blob insert/eviction via Notify,
    // with a backstop interval to catch subtree-only changes.
    let blobs_available_state = if config.cas_server_port.is_some() {
        // Sibling-bug audit (review #7): fast-store-only is intentional.
        // BlobsAvailable advertises ON-DISK digests so peer workers can
        // fetch them. Mirror-blob digests are reported via a separate
        // `pinned_mirror_digests` field on the same proto, populated
        // from `cas_server_fss.snapshot_and_reset_mirror_changes()` —
        // the two snapshots have different lifetimes and routing
        // semantics on the server side and must NOT be merged here.
        // Concrete FilesystemStore needed for BlobChangeTracker registration;
        // the wrapper hides the concrete type so the downcast must read the
        // inner store directly.
        #[allow(clippy::disallowed_methods)]
        let fs_store_opt: Option<Arc<FilesystemStore>> = fast_slow_store
            .fast_store()
            .downcast_ref::<FilesystemStore>(None)
            .and_then(|fs| fs.get_arc());

        if let Some(fs_store) = fs_store_opt {
            let max_interval_ms = if config.blobs_available_interval_ms == 0 {
                BLOBS_AVAILABLE_MAX_INTERVAL_MS
            } else {
                config.blobs_available_interval_ms
            };
            let cas_endpoint = config
                .cas_server_port
                .map(|port| cas_advertised_endpoint(port, use_tls))
                .unwrap_or_default();

            // Shared notify: tracker fires it on insert/eviction, send loop
            // awaits it to wake immediately.
            let notify = Arc::new(Notify::new());

            // Create change tracker and register it on the FilesystemStore.
            let tracker = BlobChangeTracker::new(notify.clone());
            if let Err(err) = fs_store
                .clone()
                .register_item_callback(tracker.clone())
            {
                warn!(?err, "Failed to register blob change tracker on FilesystemStore");
            } else {
                info!(
                    max_interval_ms,
                    "Registered BlobsAvailable drain-then-fire reporting with callback-based change tracking"
                );
            }

            Some(BlobsAvailableState {
                fs_store,
                tracker,
                cas_endpoint,
                notify,
                max_interval: Duration::from_millis(max_interval_ms),
                cas_server_fss: Some(cas_server_fss.clone()),
                ac_mirror_target: ac_mirror_target.clone(),
            })
        } else {
            warn!("FastSlowStore's fast store is not a FilesystemStore; BlobsAvailable reporting disabled");
            None
        }
    } else {
        None
    };

    // Start a CAS + ByteStream gRPC server for peer blob sharing if configured.
    // Serves the effective_cas_store (which includes WorkerProxyStore) so that
    // reads can be proxied to peers when the local store doesn't have the blob.
    let cas_server_guard = if let Some(cas_port) = config.cas_server_port {
        let cas_store = Store::new(effective_cas_store_for_cas_server);
        let store_manager = Arc::new(nativelink_store::store_manager::StoreManager::new());
        store_manager.add_store("worker_cas", cas_store);

        let cas_configs = vec![nativelink_config::cas_server::WithInstanceName {
            instance_name: String::new(),
            config: nativelink_config::cas_server::CasStoreConfig {
                cas_store: "worker_cas".to_string(),
            },
        }];
        let bytestream_configs = vec![nativelink_config::cas_server::WithInstanceName {
            instance_name: String::new(),
            config: nativelink_config::cas_server::ByteStreamConfig {
                cas_store: "worker_cas".to_string(),
                ..Default::default()
            },
        }];

        // Workers do NOT participate in the SmallBlobDispatcher producer
        // path — workers RECEIVE dispatched bytes; they never push to
        // other workers. Pass `None` here so the dispatcher hook is
        // entirely inert on the worker side. Server-side wire-up lives
        // in `src/bin/nativelink.rs:957-973`.
        let cas_server = nativelink_service::cas_server::CasServer::new(&cas_configs, &store_manager, None)
            .err_tip(|| "Failed to create worker CAS server")?;
        let bytestream_server =
            nativelink_service::bytestream_server::ByteStreamServer::new(&bytestream_configs, &store_manager, None)
                .err_tip(|| "Failed to create worker ByteStream server")?;

        let addr: std::net::SocketAddr = ([0, 0, 0, 0, 0, 0, 0, 0], cas_port).into();
        let advertised = cas_advertised_endpoint(cas_port, use_tls);

        let worker_name = config.name.clone();

        // Match the main server's message size limits so that mirror writes
        // from WorkerProxyStore (which may send BatchUpdateBlobs >4MiB) are
        // not rejected by tonic's default 4MiB limit.
        const WORKER_CAS_MAX_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
        const WORKER_CAS_MAX_ENCODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

        // Build tonic service wrappers first (they wrap in Arc internally
        // and implement Clone), so we can share them between TCP and QUIC.
        let cas_svc = cas_server
            .into_service()
            .max_decoding_message_size(WORKER_CAS_MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(WORKER_CAS_MAX_ENCODING_MESSAGE_SIZE);
        let bs_svc = bytestream_server
            .into_service()
            .max_decoding_message_size(WORKER_CAS_MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(WORKER_CAS_MAX_ENCODING_MESSAGE_SIZE);

        // Start TCP server (with TLS if cas_server_tls is configured).
        let tcp_cas_svc = cas_svc.clone();
        let tcp_bs_svc = bs_svc.clone();
        let tcp_worker_name = worker_name.clone();
        let tls_server_config = if let Some(ref tls_cfg) = config.cas_server_tls {
            let cert = std::fs::read_to_string(&tls_cfg.cert_file)
                .err_tip(|| format!("Could not read CAS server cert: {}", tls_cfg.cert_file))?;
            let key = std::fs::read_to_string(&tls_cfg.key_file)
                .err_tip(|| format!("Could not read CAS server key: {}", tls_cfg.key_file))?;
            let identity = tonic::transport::Identity::from_pem(cert, key);
            let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            if let Some(ref ca_file) = tls_cfg.client_ca_file {
                let ca_cert = std::fs::read_to_string(ca_file)
                    .err_tip(|| format!("Could not read CAS server client CA: {ca_file}"))?;
                tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca_cert));
            }
            Some(tls)
        } else {
            None
        };
        // Shutdown signal for the worker CAS server. On SIGTERM, the worker
        // sends `true` so the CAS server stops accepting new connections and
        // drains in-flight requests before the process exits.
        let (cas_shutdown_tx, cas_shutdown_rx) = tokio::sync::watch::channel(false);
        let mut tcp_shutdown_rx = cas_shutdown_rx.clone();
        let tcp_guard = spawn!("worker_cas_tcp", async move {
            info!(
                worker_name = %tcp_worker_name,
                %addr,
                %advertised,
                tls = tls_server_config.is_some(),
                "Starting worker CAS TCP server for peer blob sharing"
            );
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls) = tls_server_config {
                builder = builder.tls_config(tls)
                    .map_err(|e| make_err!(Code::Internal, "Worker CAS TCP TLS config failed: {e:?}"))?;
            }
            let result = builder
                .add_service(tcp_cas_svc)
                .add_service(tcp_bs_svc)
                .serve_with_shutdown(addr, async move {
                    let _ = tcp_shutdown_rx.changed().await;
                    info!(%addr, "worker CAS server shutting down gracefully");
                })
                .await
                .map_err(|e| make_err!(Code::Internal, "Worker CAS TCP server failed: {e:?}"));
            if let Err(ref e) = result {
                error!(%addr, ?e, "Worker CAS TCP server exited with error");
            }
            result
        });

        // Start QUIC/H3 server on the same port (UDP) for peer blob sharing.
        #[cfg(feature = "quic")]
        let _quic_guard = {
            let quic_routes = tonic::service::Routes::new(cas_svc).add_service(bs_svc);
            match start_worker_quic_server(cas_port, &worker_name, quic_routes) {
                Ok(guard) => Some(guard),
                Err(e) => {
                    warn!(?e, "Failed to start worker QUIC CAS server, falling back to TCP only");
                    None
                }
            }
        };

        #[allow(unused_mut)]
        let mut guards = vec![tcp_guard];
        #[cfg(feature = "quic")]
        if let Some(quic_guard) = _quic_guard {
            guards.push(quic_guard);
        }
        (guards, Some(cas_shutdown_tx))
    } else {
        (Vec::new(), None)
    };
    let (cas_server_guard, cas_shutdown_tx) = cas_server_guard;

    // Start pprof HTTP server if configured and the feature is enabled.
    #[cfg(feature = "pprof")]
    if config.pprof_port != 0 {
        match nativelink_util::pprof_server::start_pprof_server(config.pprof_port) {
            Ok(guard) => {
                // Leak the guard so the server lives for the process lifetime.
                // The pprof server is a diagnostic tool that should outlive any
                // individual worker reconnection cycle.
                std::mem::forget(guard);
                info!(port = config.pprof_port, "pprof HTTP server started");
            }
            Err(e) => {
                warn!(?e, port = config.pprof_port, "failed to start pprof HTTP server");
            }
        }
    }

    let local_worker = LocalWorker::new_with_connection_factory_actions_manager_and_locality(
        config.clone(),
        running_actions_manager,
        Box::new(move || {
            let config = config.clone();
            Box::pin(async move {
                // Check if QUIC/HTTP3 is requested for the worker API endpoint.
                #[cfg(feature = "quic")]
                if config.worker_api_endpoint.use_http3 {
                    let grpc_endpoint = nativelink_config::stores::GrpcEndpoint {
                        address: config.worker_api_endpoint.uri.clone(),
                        tls_config: None,
                        concurrency_limit: None,
                        connect_timeout_s: 0,
                        tcp_keepalive_s: 0,
                        http2_keepalive_interval_s: 0,
                        http2_keepalive_timeout_s: 0,
                        tcp_nodelay: true,
                        use_http3: true,
                    };
                    let quic_channel = tls_utils::h3_channel(&grpc_endpoint, 1)
                        .map_err(|e| make_err!(
                            Code::Internal,
                            "Failed to create QUIC channel for worker API: {e:?}"
                        ))?;
                    info!(
                        uri = %config.worker_api_endpoint.uri,
                        decode_limit_mib = WORKER_API_MAX_DECODING_MESSAGE_SIZE / (1024 * 1024),
                        "Worker API: using QUIC/HTTP3 transport with explicit decode limit"
                    );
                    return Ok(WorkerApiClient::new(quic_channel)
                        .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE)
                        .into());
                }

                let timeout = config
                    .worker_api_endpoint
                    .timeout
                    .unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
                let timeout_duration = Duration::from_secs_f32(timeout);
                let tls_config =
                    tls_utils::load_client_config(&config.worker_api_endpoint.tls_config)
                        .err_tip(|| "Parsing local worker TLS configuration")?;
                let endpoint =
                    tls_utils::endpoint_from(&config.worker_api_endpoint.uri, tls_config)
                        .map_err(|e| make_input_err!("Invalid URI for worker endpoint : {e:?}"))?
                        .connect_timeout(timeout_duration)
                        .timeout(timeout_duration);

                let transport = endpoint.connect().await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Could not connect to endpoint {}: {e:?}",
                        config.worker_api_endpoint.uri
                    )
                })?;
                info!(
                    uri = %config.worker_api_endpoint.uri,
                    decode_limit_mib = WORKER_API_MAX_DECODING_MESSAGE_SIZE / (1024 * 1024),
                    "Worker API: using TCP/HTTP2 transport with explicit decode limit"
                );
                Ok(WorkerApiClient::new(transport)
                    .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE)
                    .into())
            })
        }),
        Box::new(move |d| Box::pin(sleep(d))),
        blobs_available_state,
        peer_locality_map,
        cas_server_guard,
        cas_shutdown_tx,
    );
    Ok(local_worker)
}

impl<T: WorkerApiClientTrait + 'static, U: RunningActionsManager> LocalWorker<T, U> {
    pub fn new_with_connection_factory_and_actions_manager(
        config: Arc<LocalWorkerConfig>,
        running_actions_manager: Arc<U>,
        connection_factory: ConnectionFactory<T>,
        sleep_fn: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
        blobs_available_state: Option<BlobsAvailableState>,
        cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
        cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    ) -> Self {
        Self::new_with_connection_factory_actions_manager_and_locality(
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn,
            blobs_available_state,
            None,
            cas_server_guards,
            cas_shutdown_tx,
        )
    }

    /// Same as `new_with_connection_factory_and_actions_manager` but plumbs
    /// through an optional `peer_locality_map` so the worker's
    /// `Update::ChunkedMessage(PeerHints)` arm can register hints
    /// directly. The legacy constructor preserved as a thin wrapper so
    /// existing test setups (which never enable peer sharing) compile
    /// unchanged.
    pub fn new_with_connection_factory_actions_manager_and_locality(
        config: Arc<LocalWorkerConfig>,
        running_actions_manager: Arc<U>,
        connection_factory: ConnectionFactory<T>,
        sleep_fn: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
        blobs_available_state: Option<BlobsAvailableState>,
        peer_locality_map: Option<SharedBlobLocalityMap>,
        cas_server_guards: Vec<JoinHandleDropGuard<Result<(), Error>>>,
        cas_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    ) -> Self {
        let metrics = Arc::new(Metrics::new(Arc::downgrade(
            running_actions_manager.metrics(),
        )));
        Self {
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn: Some(sleep_fn),
            metrics,
            blobs_available_state,
            peer_locality_map,
            _cas_server_guards: cas_server_guards,
            cas_shutdown_tx,
        }
    }

    #[allow(
        clippy::missing_const_for_fn,
        reason = "False positive on stable, but not on nightly"
    )]
    pub fn name(&self) -> &String {
        &self.config.name
    }

    async fn register_worker(
        &self,
        client: &mut T,
    ) -> Result<(String, Streaming<UpdateForWorker>), Error> {
        let mut extra_envs: HashMap<String, String> = HashMap::new();
        if let Some(ref additional_environment) = self.config.additional_environment {
            for (name, source) in additional_environment {
                let value = match source {
                    EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                    EnvironmentSource::FromEnvironment => {
                        Cow::Owned(env::var(name).unwrap_or_default())
                    }
                    other => {
                        debug!(
                            ?other,
                            "Worker registration doesn't support this type of additional environment"
                        );
                        continue;
                    }
                };
                extra_envs.insert(name.clone(), value.into_owned());
            }
        }

        let use_tls = self.config.cas_server_tls.is_some();
        let cas_endpoint = self
            .config
            .cas_server_port
            .map_or_else(String::new, |port| cas_advertised_endpoint(port, use_tls));
        let connect_worker_request = make_connect_worker_request(
            self.config.name.clone(),
            &self.config.platform_properties,
            &extra_envs,
            self.config.max_inflight_tasks,
            cas_endpoint,
        )
        .await?;
        let mut update_for_worker_stream = client
            .connect_worker(connect_worker_request)
            .await
            .err_tip(|| "Could not call connect_worker() in worker")?
            .into_inner();

        let first_msg_update = update_for_worker_stream
            .next()
            .await
            .err_tip(|| "Got EOF expected UpdateForWorker")?
            .err_tip(|| "Got error when receiving UpdateForWorker")?
            .update;

        let worker_id = match first_msg_update {
            Some(Update::ConnectionResult(connection_result)) => connection_result.worker_id,
            other => {
                return Err(make_input_err!(
                    "Expected first response from scheduler to be a ConnectResult got : {:?}",
                    other
                ));
            }
        };
        Ok((worker_id, update_for_worker_stream))
    }

    #[instrument(skip(self), level = Level::INFO)]
    pub async fn run(
        mut self,
        mut shutdown_rx: broadcast::Receiver<ShutdownGuard>,
    ) -> Result<(), Error> {
        let sleep_fn = self
            .sleep_fn
            .take()
            .err_tip(|| "Could not unwrap sleep_fn in LocalWorker::run")?;
        let sleep_fn_pin = Pin::new(&sleep_fn);
        let error_handler = Box::pin(move |err| async move {
            error!(?err, "Error");
            (sleep_fn_pin)(Duration::from_secs_f32(CONNECTION_RETRY_DELAY_S)).await;
        });

        loop {
            // First connect to our endpoint.
            let mut client = match (self.connection_factory)().await {
                Ok(client) => client,
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
            };

            // Next register our worker with the scheduler.
            let (inner, update_for_worker_stream) = match self.register_worker(&mut client).await {
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
                Ok((worker_id, update_for_worker_stream)) => (
                    LocalWorkerImpl::new(
                        &self.config,
                        client,
                        worker_id,
                        self.running_actions_manager.clone(),
                        self.metrics.clone(),
                        self.blobs_available_state.clone(),
                        self.peer_locality_map.clone(),
                        &self.cas_shutdown_tx,
                    ),
                    update_for_worker_stream,
                ),
            };
            info!(
                worker_id = %inner.worker_id,
                "Worker registered with scheduler"
            );

            // Now listen for connections and run all other services.
            if let Err(err) = inner.run(update_for_worker_stream, &mut shutdown_rx).await {
                'no_more_actions: {
                    // Ensure there are no actions in transit before we try to kill
                    // all our actions.
                    const ITERATIONS: usize = 1_000;

                    const ERROR_MSG: &str = "Actions in transit did not reach zero before we disconnected from the scheduler";

                    let sleep_duration = ACTIONS_IN_TRANSIT_TIMEOUT_S / ITERATIONS as f32;
                    for _ in 0..ITERATIONS {
                        if inner.actions_in_transit.load(Ordering::Acquire) == 0 {
                            break 'no_more_actions;
                        }
                        (sleep_fn_pin)(Duration::from_secs_f32(sleep_duration)).await;
                    }
                    // Don't terminate the worker process — fall through to
                    // kill_all + reconnect. The stuck create_and_add_action
                    // futures will be cancelled when kill_all drops them.
                    warn!(ERROR_MSG);
                }
                error!(?err, "Worker disconnected from scheduler");
                // Kill off any existing actions because if we re-connect, we'll
                // get some more and it might resource lock us.
                self.running_actions_manager.kill_all().await;

                (error_handler)(err).await; // Try to connect again.
            }
        }
        // Unreachable.
    }
}

#[derive(Debug, MetricsComponent)]
pub struct Metrics {
    #[metric(
        help = "Total number of actions sent to this worker to process. This does not mean it started them, it just means it received a request to execute it."
    )]
    start_actions_received: CounterWithTime,
    #[metric(help = "Total number of disconnects received from the scheduler.")]
    disconnects_received: CounterWithTime,
    #[metric(help = "Total number of keep-alives received from the scheduler.")]
    keep_alives_received: CounterWithTime,
    #[metric(
        help = "Stats about the calls to check if an action satisfies the config supplied script."
    )]
    preconditions: AsyncCounterWrapper,
    #[metric]
    #[allow(
        clippy::struct_field_names,
        reason = "TODO Fix this. Triggers on nightly"
    )]
    running_actions_manager_metrics: Weak<RunningActionManagerMetrics>,
}

impl RootMetricsComponent for Metrics {}

impl Metrics {
    fn new(running_actions_manager_metrics: Weak<RunningActionManagerMetrics>) -> Self {
        Self {
            start_actions_received: CounterWithTime::default(),
            disconnects_received: CounterWithTime::default(),
            keep_alives_received: CounterWithTime::default(),
            preconditions: AsyncCounterWrapper::default(),
            running_actions_manager_metrics,
        }
    }
}

impl Metrics {
    async fn wrap<U, T: Future<Output = U>, F: FnOnce(Arc<Self>) -> T>(
        self: Arc<Self>,
        fut: F,
    ) -> U {
        fut(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreKey;

    #[test]
    fn test_blob_change_tracker_eviction_collects_and_swaps() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        // Evict two digests via the callback.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1)));
        rt.block_on(tracker.callback(StoreKey::Digest(d2)));

        // Swap should return both as evicted.
        let changes = tracker.swap();
        assert!(changes.added.is_empty(), "Expected no added digests");
        assert_eq!(changes.evicted.len(), 2, "Expected 2 evicted digests");
        assert!(changes.evicted.contains(&d1), "Expected d1 in evicted set");
        assert!(changes.evicted.contains(&d2), "Expected d2 in evicted set");

        // Second swap should return empty.
        let changes2 = tracker.swap();
        assert!(changes2.added.is_empty());
        assert!(changes2.evicted.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_ignores_non_digest_keys() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));

        // Evict callback with a string key.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Str(Cow::Borrowed("some_key"))));

        // Insert callback with a string key.
        tracker.on_insert(StoreKey::Str(Cow::Borrowed("other_key")), 42);

        let changes = tracker.swap();
        assert!(changes.added.is_empty());
        assert!(changes.evicted.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_insert_callback() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        tracker.on_insert(StoreKey::Digest(d1), 100);
        tracker.on_insert(StoreKey::Digest(d2), 200);

        let changes = tracker.swap();
        assert_eq!(changes.added.len(), 2, "Expected 2 added digests");
        assert!(changes.added.contains(&d1));
        assert!(changes.added.contains(&d2));
        assert!(changes.evicted.is_empty());
        assert!(changes.touched.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_swap_returns_and_clears() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);
        let d2 = DigestInfo::new([2u8; 32], 200);

        // Accumulate an insert and an eviction.
        tracker.on_insert(StoreKey::Digest(d1), 100);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d2)));

        // First swap returns the accumulated changes.
        let changes = tracker.swap();
        assert_eq!(changes.added.len(), 1);
        assert!(changes.added.contains(&d1));
        assert_eq!(changes.evicted.len(), 1);
        assert!(changes.evicted.contains(&d2));

        // Second swap should be empty.
        let changes2 = tracker.swap();
        assert!(changes2.added.is_empty());
        assert!(changes2.evicted.is_empty());
    }

    #[test]
    fn test_blob_change_tracker_insert_then_evict_records_eviction() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);

        // Insert then evict the same digest — the eviction must still be
        // recorded so the server knows the blob is no longer available.
        tracker.on_insert(StoreKey::Digest(d1), 100);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1)));

        let changes = tracker.swap();
        // The digest was inserted then evicted within the same tick.
        // It should be removed from `added` (no longer available) and
        // appear in `evicted` so the server is notified.
        assert!(
            !changes.added.contains(&d1),
            "Expected d1 to NOT be in added after insert+evict"
        );
        assert!(
            changes.evicted.contains(&d1),
            "Expected d1 in evicted (it was evicted, removing it from added)"
        );
    }

    #[test]
    fn test_blob_change_tracker_evict_then_reinsert_cancels_out() {
        let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
        let d1 = DigestInfo::new([1u8; 32], 100);

        // Evict then reinsert the same digest — should show as added only.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(tracker.callback(StoreKey::Digest(d1)));
        tracker.on_insert(StoreKey::Digest(d1), 100);

        let changes = tracker.swap();
        assert!(
            changes.added.contains(&d1),
            "Expected d1 in added after evict+reinsert"
        );
        assert!(
            !changes.evicted.contains(&d1),
            "Expected d1 NOT in evicted after evict+reinsert"
        );
    }

    // ---------------------------------------------------------------
    // Gap 4: BlobChangeTracker <-> MokaEvictingMap integration test
    // ---------------------------------------------------------------
    // Wires: MokaEvictingMap -> ItemCallbackHolder -> BlobChangeTracker
    // and verifies that inserts and evictions flow through correctly.
    #[test]
    fn test_blob_change_tracker_evicting_map_integration() {
        use std::time::SystemTime;

        use nativelink_config::stores::EvictionPolicy;
        use nativelink_store::callback_utils::ItemCallbackHolder;
        use nativelink_util::evicting_map::LenEntry;
        use nativelink_util::moka_evicting_map::MokaEvictingMap;
        use nativelink_util::store_trait::StoreKeyBorrow;

        // Simple value type for the MokaEvictingMap.
        #[derive(Clone, Debug)]
        struct TestValue(u64);

        impl LenEntry for TestValue {
            fn len(&self) -> u64 {
                self.0
            }
            fn is_empty(&self) -> bool {
                self.0 == 0
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            // Create a MokaEvictingMap with max_count = 2 so the third
            // insert deterministically evicts the LRU. We avoid max_bytes
            // here because moka divides by an internal SCALE factor and
            // sub-1KB budgets are unstable across moka versions; max_count
            // is the predictable knob for unit tests.
            let evicting_map = std::sync::Arc::new(MokaEvictingMap::<
                StoreKeyBorrow,
                StoreKey<'static>,
                TestValue,
                SystemTime,
                ItemCallbackHolder,
            >::with_anchor(
                &EvictionPolicy {
                    max_count: 2,
                    max_seconds: 0,
                    max_bytes: 0,
                    evict_bytes: 0,
                },
                SystemTime::now(),
            ));
            // Drain pending eviction events on a background task so the
            // tracker actually sees the eviction callback for d1 below.
            evicting_map.start_background_eviction();

            // Create a BlobChangeTracker and register it.
            let tracker = BlobChangeTracker::new(Arc::new(Notify::new()));
            let holder = ItemCallbackHolder::new(tracker.clone());
            evicting_map.add_item_callback(holder);

            let d1 = DigestInfo::new([1u8; 32], 30);
            let d2 = DigestInfo::new([2u8; 32], 40);

            // Insert two items at capacity for max_count=2.
            let key1: StoreKeyBorrow = StoreKey::Digest(d1).into();
            let key2: StoreKeyBorrow = StoreKey::Digest(d2).into();
            evicting_map.insert(key1, TestValue(30)).await;
            evicting_map.insert(key2, TestValue(40)).await;

            // Swap and verify both digests appear in `added`.
            let changes = tracker.swap();
            assert_eq!(
                changes.added.len(),
                2,
                "Expected 2 added digests after initial inserts"
            );
            assert!(
                changes.added.contains(&d1),
                "Expected d1 in added set"
            );
            assert!(
                changes.added.contains(&d2),
                "Expected d2 in added set"
            );
            assert!(
                changes.evicted.is_empty(),
                "Expected no evictions yet"
            );

            // Now insert a third item — exceeds max_count=2 so the LRU
            // entry (d1) must be evicted. Promote d2 explicitly via get
            // so LRU order makes d1 the eviction victim.
            let d2_key = StoreKey::Digest(d2);
            let _ = evicting_map.get(&d2_key).await;
            let d3 = DigestInfo::new([3u8; 32], 50);
            let key3: StoreKeyBorrow = StoreKey::Digest(d3).into();
            evicting_map.insert(key3, TestValue(50)).await;

            // Wait for the background drainer to fire the eviction
            // callback. start_background_eviction owns the drain task; a
            // few yields are usually enough but give it generous slack
            // since current_thread runtime serializes.
            for _ in 0..50 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let changes = tracker.swap();
            assert!(
                changes.added.contains(&d3),
                "Expected d3 in added set after third insert"
            );
            assert!(
                changes.evicted.contains(&d1),
                "Expected d1 in evicted set (LRU eviction)"
            );
            assert!(
                !changes.evicted.contains(&d2),
                "Expected d2 to NOT be evicted (most recently used)"
            );
        });
    }

    #[test]
    fn test_cas_advertised_endpoint_format() {
        let endpoint = cas_advertised_endpoint(50081, false);
        assert!(
            endpoint.starts_with("grpc://"),
            "Expected endpoint to start with 'grpc://', got: {endpoint}"
        );
        assert!(
            endpoint.ends_with(":50081"),
            "Expected endpoint to end with ':50081', got: {endpoint}"
        );

        // Extract hostname and verify it's non-empty.
        let without_prefix = endpoint.strip_prefix("grpc://").unwrap();
        let hostname = without_prefix.strip_suffix(":50081").unwrap();
        assert!(
            !hostname.is_empty(),
            "Expected non-empty hostname in endpoint: {endpoint}"
        );
    }

    #[test]
    fn test_cas_advertised_endpoint_tls() {
        let endpoint = cas_advertised_endpoint(40081, true);
        assert!(
            endpoint.starts_with("grpcs://"),
            "Expected endpoint to start with 'grpcs://', got: {endpoint}"
        );
        assert!(
            endpoint.ends_with(":40081"),
            "Expected endpoint to end with ':40081', got: {endpoint}"
        );
    }
}

#[cfg(test)]
mod actions_notify_subscribe_before_predicate_tests {
    //! Regression test for #95: lost-wakeup window in the
    //! shutdown-drain loop in the worker loop body
    //! (`local_worker.rs` near line 2491-2493).
    //!
    //! The pre-fix loop loaded `actions_in_flight` and then awaited
    //! `actions_notify.notified()`. Subscribe-after-predicate has the
    //! standard lost-wakeup race: a producer fired between the load
    //! and the await would be missed.
    //!
    //! The producer (running-action completion sites) uses
    //! `notify_one()` which DOES store one permit, so today's
    //! immediate symptom is at-most-one-extra loop iteration during
    //! shutdown drain rather than a hard deadlock. This test
    //! documents the contract — defense-in-depth for any future
    //! producer change toward broadcast-style `notify_waiters`. Same
    //! shape and rationale as the cleanup_wait_notify_parity_tests
    //! reference in `running_actions_manager.rs:5715-5807` and the
    //! `fetched_notify_subscribe_before_predicate_tests` for #92.
    //!
    //! NOTE on test discipline (CLAUDE.md
    //! `feedback_lost_wakeup_test_theatre`): we use a
    //! `tokio::sync::Barrier` for deterministic ordering and a
    //! `tokio::time::timeout` deadlock detector — NEVER `sleep` as
    //! synchronization.
    use core::time::Duration;
    use std::sync::Arc;

    use tokio::sync::{Barrier, Notify};

    /// Subscribe-before-predicate (the fix at #95): the Notified
    /// future is constructed BEFORE the predicate window, so a
    /// `notify_waiters` issued during that window is delivered. With
    /// the contract violated (subscribe AFTER predicate), the
    /// `notify_waiters` evaporates and the await blocks forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_before_predicate_captures_wakeup() {
        let notify = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(2));

        // Producer: wait at the barrier, then fire notify_waiters().
        // notify_waiters() does NOT store a permit — it only wakes
        // waiters currently registered.
        let prod_notify = notify.clone();
        let prod_barrier = barrier.clone();
        tokio::spawn(async move {
            prod_barrier.wait().await;
            prod_notify.notify_waiters();
        });

        // Consumer mirrors the production loop body shape (#95 fix):
        // subscribe FIRST, enable, then enter the predicate window.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Predicate window: release the producer to fire its
        // notify_waiters(). The barrier acts as a happens-before
        // synchronization point: the producer's notify is emitted
        // strictly after this point, while we are still in the
        // predicate window — strictly before we reach the await
        // below.
        barrier.wait().await;

        // Await: the pre-enabled Notified must observe the wakeup
        // issued during the predicate window. 2s real-wall-clock is
        // a deadlock detector, NOT synchronization.
        tokio::time::timeout(Duration::from_secs(2), notified.as_mut())
            .await
            .expect(
                "lost-wakeup race — must subscribe before predicate (#95): \
                 notify_waiters() fired during the predicate window was lost",
            );
    }
}
