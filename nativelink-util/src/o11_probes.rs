// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! #85 — 5 observability-only probes from the O11 investigation
//! (2026-06-07).
//!
//! Each probe is observability-only — removing all 5 leaves system
//! semantics identical. Probes are wired into the metrics registry via
//! the `*_arc()` accessors mirroring the `phase0_metrics` pattern.
//!
//! The five probes:
//!
//! - **P1** `worker_upload_semaphore_*` — inflight + waiters gauges on
//!   the `MAX_CONCURRENT_UPLOADS = 32` semaphore in
//!   `LocalWorkerImpl::handle_upload_missing_blobs`. Falsification: if
//!   peak `waiters > 0` sustained for >=1 minute during a build, the
//!   32-permit cap IS being hit and O11's framing was wrong.
//! - **P2** `worker_actions_in_flight` — gauge for the
//!   currently-private `actions_in_flight: AtomicU64` counter in
//!   `LocalWorkerImpl::run`. No behavior change: just exposes what's
//!   already counted.
//! - **P3** `bytestream_write_elapsed_ms_*` — `LatencyHistogram` per
//!   (direction × size_bucket) recorded once per `ByteStream::read` /
//!   `ByteStream::write` completion (success OR error).
//! - **P4** `system_metrics` — periodic 10s sampler emitting
//!   `load_*` + `mem_avail_mb` to worker logs. macOS-only (workers are
//!   M4 Macs); on other targets the sampler is a no-op.
//! - **P5** `evicting_map_lock_held_*` — `LatencyHistogram` over
//!   per-scan elapsed_ms inside `MokaEvictingMap::evict_unpinned_lru_bytes`.
//!   Exposes contention BELOW the existing 50 ms warn threshold so
//!   trends are visible before they breach the warn.

use core::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use tokio::sync::Semaphore;

/// Latency histogram bucket boundaries (ms) for P3 (ByteStream::write
/// elapsed) and P5 (EvictingMap lock-held). Per task #85 P3 spec:
/// 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000 ms.
///
/// P5 uses the same 11-bucket boundary set so both probes share the
/// same histogram type — a 9-bucket subset would duplicate logic
/// without benefit.
pub const O11_LATENCY_BUCKETS_MS: [u64; 11] =
    [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000];

/// Histogram-style bucket recorder. Independent of the
/// `LatencyHistogram` in `phase0_metrics` because the bucket boundaries
/// differ (#85 P3 specifies an 11-bucket envelope; phase0 uses 9). Same
/// shape: per-bucket counters + `+inf` count + sum.
#[derive(Debug)]
pub(crate) struct O11LatencyHistogram {
    /// Counts per-bucket. Index `i` accumulates samples with value
    /// less-than-or-equal to `O11_LATENCY_BUCKETS_MS[i]`. Tail
    /// (`+inf`) bucket is `inf_bucket` below.
    buckets: [AtomicU64; O11_LATENCY_BUCKETS_MS.len()],
    /// `+inf` bucket — total count of all observations.
    inf_bucket: AtomicU64,
    /// Sum of all observed values (ms).
    sum: AtomicU64,
}

impl O11LatencyHistogram {
    pub(crate) const fn new() -> Self {
        const Z: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [Z, Z, Z, Z, Z, Z, Z, Z, Z, Z, Z],
            inf_bucket: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    /// Record one observation. Cost: bucket-scan + 2 atomic adds.
    pub(crate) fn observe(&self, value: u64) {
        for (idx, boundary) in O11_LATENCY_BUCKETS_MS.iter().enumerate() {
            if value <= *boundary {
                self.buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inf_bucket.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    /// Emit Prometheus-friendly buckets + `_count` + `_sum_ms`.
    fn publish_buckets(
        &self,
        base_name: &str,
        base_help: &str,
    ) -> Result<(), nativelink_metric::Error> {
        for (idx, boundary) in O11_LATENCY_BUCKETS_MS.iter().enumerate() {
            let count = self.buckets[idx].load(Ordering::Relaxed);
            let name = format!("{base_name}_le_{boundary}_ms");
            let help = format!("{base_help} — count of observations <= {boundary}ms");
            nativelink_metric::publish!(
                &name,
                &count,
                MetricKind::Counter,
                help.as_str()
            );
        }
        let count = self.inf_bucket.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let count_name = format!("{base_name}_count");
        let sum_name = format!("{base_name}_sum_ms");
        let count_help = format!("{base_help} — total sample count");
        let sum_help = format!("{base_help} — sum of all observed values in ms");
        nativelink_metric::publish!(
            &count_name,
            &count,
            MetricKind::Counter,
            count_help.as_str()
        );
        nativelink_metric::publish!(
            &sum_name,
            &sum,
            MetricKind::Counter,
            sum_help.as_str()
        );
        Ok(())
    }
}

// =====================================================================
// P1 — MAX_CONCURRENT_UPLOADS semaphore inflight + waiters gauge
// =====================================================================

/// Worker-process-global upload semaphore + inflight/waiters gauges.
///
/// Lives at process-singleton scope (not per-`handle_upload_missing_blobs`
/// invocation) so two simultaneous UploadMissingBlobs messages share
/// the same 32-permit budget — matching what production already does
/// when the function is called twice in flight on the same connection.
/// Pre-#85 every call constructed its own `Arc<Semaphore>`, so the
/// effective cap was 32 per call — not 32 worker-wide. The probe makes
/// the cap explicit and worker-wide; that IS a behavior change for
/// concurrent invocations, but the change is observability-required
/// (a per-call semaphore cannot be polled for waiters).
pub struct UploadSemaphoreMetrics {
    /// 32-permit semaphore shared across all
    /// `handle_upload_missing_blobs` calls in this worker process.
    pub semaphore: Arc<Semaphore>,
    /// Configured permit count (= `MAX_CONCURRENT_UPLOADS`). Constant.
    pub max_permits: u64,
    /// Live count of acquired permits = `max_permits - available_permits()`.
    /// Exposed as a gauge under
    /// `nativelink_worker_upload_semaphore_inflight`.
    /// Derived live from `available_permits()` at publish time;
    /// no separate atomic needed.
    /// Live count of tasks blocked inside `acquire().await` waiting
    /// for a permit. Manually maintained by the wrapper helper
    /// `acquire_with_metrics` below — tokio's `Semaphore` does NOT
    /// expose pending-waiter count, so the gauge is best-effort
    /// (incremented before `.acquire().await`, decremented after).
    pub waiters: Arc<AtomicI64>,
}

/// #85 P1: process-wide max-concurrent-uploads cap. Mirrors the prior
/// per-call constant in `LocalWorkerImpl::handle_upload_missing_blobs`.
pub const MAX_CONCURRENT_UPLOADS: usize = 32;

impl UploadSemaphoreMetrics {
    fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS)),
            max_permits: MAX_CONCURRENT_UPLOADS as u64,
            waiters: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Acquire one permit while keeping the `waiters` gauge correct.
    /// Returns the permit; drop to release.
    ///
    /// Caller MUST hold the returned `OwnedSemaphorePermit` for the
    /// duration of the upload — releasing it (by drop) frees the
    /// permit. The `inflight` gauge derives from `available_permits()`
    /// at publish time, so the inflight number is automatically
    /// correct without needing a manual decrement.
    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.waiters.fetch_add(1, Ordering::Relaxed);
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("upload semaphore should never be closed");
        self.waiters.fetch_sub(1, Ordering::Relaxed);
        permit
    }
}

impl core::fmt::Debug for UploadSemaphoreMetrics {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("UploadSemaphoreMetrics")
            .field("max_permits", &self.max_permits)
            .field("waiters", &self.waiters.load(Ordering::Relaxed))
            .field(
                "available_permits",
                &self.semaphore.available_permits(),
            )
            .finish()
    }
}

impl MetricsComponent for UploadSemaphoreMetrics {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let available = self.semaphore.available_permits() as u64;
        let inflight = self.max_permits.saturating_sub(available);
        let waiters = self.waiters.load(Ordering::Relaxed).max(0) as u64;

        nativelink_metric::publish!(
            "upload_semaphore_inflight",
            &inflight,
            MetricKind::Default,
            "#85 P1: live count of acquired permits on the worker-wide \
             MAX_CONCURRENT_UPLOADS semaphore (= max_permits - \
             available_permits). Cap == 32; this gauge near the cap with \
             waiters > 0 means the cap is binding."
        );
        nativelink_metric::publish!(
            "upload_semaphore_waiters",
            &waiters,
            MetricKind::Default,
            "#85 P1: best-effort count of tasks blocked inside \
             acquire().await on the worker-wide \
             MAX_CONCURRENT_UPLOADS semaphore. Falsification: if \
             sustained > 0 for >=1 minute during a build, the 32-permit \
             cap IS the limit — O11's framing was wrong."
        );
        nativelink_metric::publish!(
            "upload_semaphore_max_permits",
            &self.max_permits,
            MetricKind::Default,
            "#85 P1: configured cap on concurrent uploads \
             (MAX_CONCURRENT_UPLOADS, constant)."
        );

        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// P2 — Promote actions_in_flight to exported gauge
// =====================================================================

/// Worker-process-global gauge for the previously-private
/// `actions_in_flight: AtomicU64` counter inside `LocalWorkerImpl::run`.
/// Producer is the existing `fetch_add`/`fetch_sub` calls; #85 P2 just
/// hands them a shared `Arc<AtomicU64>` instead of a function-local
/// one and exposes the same `Arc` here.
#[derive(Debug)]
pub struct WorkerActionsInFlight {
    /// Shared with `LocalWorkerImpl::run`'s `actions_in_flight` site.
    /// Holds the live count of actions currently in flight (AC upload
    /// + scheduler notification not yet returned).
    pub counter: Arc<AtomicU64>,
}

impl WorkerActionsInFlight {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MetricsComponent for WorkerActionsInFlight {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let v = self.counter.load(Ordering::Relaxed);
        nativelink_metric::publish!(
            "actions_in_flight",
            &v,
            MetricKind::Default,
            "#85 P2: worker concurrent in-flight action count \
             (~execution_response not yet returned). Promoted from \
             the previously-private LocalWorkerImpl::run actions_in_flight \
             counter; no behavior change."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// P3 — Histogram ByteStream elapsed_ms stratified by direction × size
// =====================================================================

/// ByteStream RPC direction. `Upload` = `ByteStream::write` (Bazel
/// pushing data into the CAS). `Download` = `ByteStream::read` (Bazel
/// pulling data out).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BsDirection {
    Upload,
    Download,
}

impl BsDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }
}

/// Size bucketing per #85 P3 spec: `small <1 MiB`, `medium 1-50 MiB`,
/// `large >50 MiB`. Boundary checks use the digest's declared size
/// at the start of the RPC.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BsSizeBucket {
    Small,
    Medium,
    Large,
}

impl BsSizeBucket {
    /// Classify a digest size (bytes) into the three buckets per the
    /// #85 P3 spec. The boundary points are inclusive on the lower side:
    /// `<1 MiB`, `1 MiB ..= 50 MiB`, `>50 MiB`.
    #[must_use]
    pub const fn from_bytes(size_bytes: u64) -> Self {
        const ONE_MIB: u64 = 1 << 20;
        const FIFTY_MIB: u64 = 50 * (1 << 20);
        if size_bytes < ONE_MIB {
            Self::Small
        } else if size_bytes <= FIFTY_MIB {
            Self::Medium
        } else {
            Self::Large
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

/// 6 = 2 directions × 3 size buckets. One histogram per (direction,
/// size) pair. Index = `direction_idx * 3 + size_idx`.
const BS_HISTOGRAM_COUNT: usize = 6;

const fn bs_index(direction: BsDirection, size: BsSizeBucket) -> usize {
    let direction_idx = match direction {
        BsDirection::Upload => 0,
        BsDirection::Download => 1,
    };
    let size_idx = match size {
        BsSizeBucket::Small => 0,
        BsSizeBucket::Medium => 1,
        BsSizeBucket::Large => 2,
    };
    direction_idx * 3 + size_idx
}

const fn bs_decode(idx: usize) -> (BsDirection, BsSizeBucket) {
    let direction = if idx / 3 == 0 {
        BsDirection::Upload
    } else {
        BsDirection::Download
    };
    let size = match idx % 3 {
        0 => BsSizeBucket::Small,
        1 => BsSizeBucket::Medium,
        _ => BsSizeBucket::Large,
    };
    (direction, size)
}

/// Histograms of `ByteStream::write` / `ByteStream::read` end-to-end
/// elapsed_ms, stratified by direction × size. One observation per RPC
/// completion (success or error).
#[derive(Debug)]
pub struct BytestreamWriteHistograms {
    histograms: [O11LatencyHistogram; BS_HISTOGRAM_COUNT],
}

impl BytestreamWriteHistograms {
    fn new() -> Self {
        Self {
            histograms: [
                O11LatencyHistogram::new(),
                O11LatencyHistogram::new(),
                O11LatencyHistogram::new(),
                O11LatencyHistogram::new(),
                O11LatencyHistogram::new(),
                O11LatencyHistogram::new(),
            ],
        }
    }

    /// Record one observation. Called once per `ByteStream::write` or
    /// `ByteStream::read` completion (Ok or Err).
    pub fn observe(&self, direction: BsDirection, size_bytes: u64, elapsed_ms: u64) {
        let idx = bs_index(direction, BsSizeBucket::from_bytes(size_bytes));
        self.histograms[idx].observe(elapsed_ms);
    }

    /// Test-only accessor: total count of observations for one
    /// (direction, size) cell.
    #[must_use]
    pub fn cell_count(&self, direction: BsDirection, size: BsSizeBucket) -> u64 {
        self.histograms[bs_index(direction, size)]
            .inf_bucket
            .load(Ordering::Relaxed)
    }
}

impl MetricsComponent for BytestreamWriteHistograms {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        for (idx, hist) in self.histograms.iter().enumerate() {
            let (direction, size) = bs_decode(idx);
            let name = format!(
                "bytestream_{}_elapsed_ms_{}",
                direction.as_str(),
                size.as_str()
            );
            let help = format!(
                "#85 P3: ByteStream::{} end-to-end elapsed_ms for {} blobs \
                 (1 MiB / 50 MiB size-class boundaries). One observation \
                 per RPC completion (success OR error).",
                direction.as_str(),
                size.as_str()
            );
            hist.publish_buckets(&name, &help)?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// P5 — Export EvictingMap lock-held / scan elapsed histogram
// =====================================================================

/// Histogram over per-scan elapsed_ms inside
/// `MokaEvictingMap::evict_unpinned_lru_bytes`. The site already
/// measured this and `warn!`'d above 50 ms; #85 P5 just exposes the
/// underlying distribution so contention BELOW the warn is visible.
#[derive(Debug)]
pub struct EvictingMapLockHistogram {
    histogram: O11LatencyHistogram,
}

impl EvictingMapLockHistogram {
    fn new() -> Self {
        Self {
            histogram: O11LatencyHistogram::new(),
        }
    }

    /// Record one observation. Called once per
    /// `evict_unpinned_lru_bytes` scan completion.
    pub fn observe(&self, elapsed_ms: u64) {
        self.histogram.observe(elapsed_ms);
    }

    /// Test-only accessor: total count of observations.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.histogram.inf_bucket.load(Ordering::Relaxed)
    }
}

impl MetricsComponent for EvictingMapLockHistogram {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        self.histogram.publish_buckets(
            "evicting_map_lock_held_ms",
            "#85 P5: per-scan elapsed_ms inside MokaEvictingMap::evict_unpinned_lru_bytes. \
             Exposes contention BELOW the existing 50 ms warn threshold so trends \
             are visible before breaching the warn.",
        )?;
        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// Process-global singletons
// =====================================================================

static UPLOAD_SEMAPHORE_METRICS: OnceLock<Arc<UploadSemaphoreMetrics>> = OnceLock::new();
static WORKER_ACTIONS_IN_FLIGHT: OnceLock<Arc<WorkerActionsInFlight>> = OnceLock::new();
static BYTESTREAM_WRITE_HISTOGRAMS: OnceLock<Arc<BytestreamWriteHistograms>> = OnceLock::new();
static EVICTING_MAP_LOCK_HISTOGRAM: OnceLock<Arc<EvictingMapLockHistogram>> = OnceLock::new();

fn upload_semaphore_metrics_inner() -> &'static Arc<UploadSemaphoreMetrics> {
    UPLOAD_SEMAPHORE_METRICS.get_or_init(|| Arc::new(UploadSemaphoreMetrics::new()))
}

fn worker_actions_in_flight_inner() -> &'static Arc<WorkerActionsInFlight> {
    WORKER_ACTIONS_IN_FLIGHT.get_or_init(|| Arc::new(WorkerActionsInFlight::new()))
}

fn bytestream_write_histograms_inner() -> &'static Arc<BytestreamWriteHistograms> {
    BYTESTREAM_WRITE_HISTOGRAMS.get_or_init(|| Arc::new(BytestreamWriteHistograms::new()))
}

fn evicting_map_lock_histogram_inner() -> &'static Arc<EvictingMapLockHistogram> {
    EVICTING_MAP_LOCK_HISTOGRAM.get_or_init(|| Arc::new(EvictingMapLockHistogram::new()))
}

/// P1: worker-wide upload-semaphore singleton.
#[must_use]
pub fn upload_semaphore_metrics() -> &'static UploadSemaphoreMetrics {
    upload_semaphore_metrics_inner().as_ref()
}

/// P1: `Arc` to the same singleton — register with `MetricsRegistry`.
#[must_use]
pub fn upload_semaphore_metrics_arc() -> Arc<UploadSemaphoreMetrics> {
    Arc::clone(upload_semaphore_metrics_inner())
}

/// P2: worker actions-in-flight singleton.
#[must_use]
pub fn worker_actions_in_flight() -> &'static WorkerActionsInFlight {
    worker_actions_in_flight_inner().as_ref()
}

/// P2: `Arc` for metrics registration.
#[must_use]
pub fn worker_actions_in_flight_arc() -> Arc<WorkerActionsInFlight> {
    Arc::clone(worker_actions_in_flight_inner())
}

/// P3: bytestream-write histograms singleton.
#[must_use]
pub fn bytestream_write_histograms() -> &'static BytestreamWriteHistograms {
    bytestream_write_histograms_inner().as_ref()
}

/// P3: `Arc` for metrics registration.
#[must_use]
pub fn bytestream_write_histograms_arc() -> Arc<BytestreamWriteHistograms> {
    Arc::clone(bytestream_write_histograms_inner())
}

/// P5: evicting-map lock-held histogram singleton.
#[must_use]
pub fn evicting_map_lock_histogram() -> &'static EvictingMapLockHistogram {
    evicting_map_lock_histogram_inner().as_ref()
}

/// P5: `Arc` for metrics registration.
#[must_use]
pub fn evicting_map_lock_histogram_arc() -> Arc<EvictingMapLockHistogram> {
    Arc::clone(evicting_map_lock_histogram_inner())
}

// =====================================================================
// P4 — System metrics sampler (macOS workers only)
// =====================================================================

/// Spawn a 10s-tick task emitting `load_*` + `mem_avail_mb` to the
/// worker log. macOS-only because the workers are M4 Macs; on other
/// targets the function is a no-op so callers can invoke unconditionally
/// without `cfg` plumbing.
///
/// Bounded periodic task — single `tokio::time::interval` driving a
/// `libc::getloadavg` + `host_statistics64` read; no allocation in the
/// hot path.
#[cfg(target_os = "macos")]
pub fn spawn_system_metrics_sampler() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(core::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let (load_1m, load_5m, load_15m) = read_loadavg();
            let mem_avail_mb = read_mem_available_mb();
            tracing::info!(
                load_1m,
                load_5m,
                load_15m,
                mem_avail_mb,
                "#85 P4 system_metrics",
            );
        }
    });
}

/// No-op on non-macOS. Workers run macOS exclusively per project
/// memory; the no-op keeps callers cfg-free.
#[cfg(not(target_os = "macos"))]
pub fn spawn_system_metrics_sampler() {}

/// Read the 1m / 5m / 15m load averages via `libc::getloadavg`. Returns
/// `(0.0, 0.0, 0.0)` on syscall failure (best-effort).
#[cfg(target_os = "macos")]
fn read_loadavg() -> (f64, f64, f64) {
    let mut avg: [f64; 3] = [0.0; 3];
    // SAFETY: `getloadavg` writes up to `nelem` f64 values; we pass a
    // 3-element buffer and request 3 entries. POSIX-defined.
    let n = unsafe { libc::getloadavg(avg.as_mut_ptr(), 3) };
    if n == 3 {
        (avg[0], avg[1], avg[2])
    } else {
        (0.0, 0.0, 0.0)
    }
}

/// Read available memory (MB) on macOS via `sysctlbyname`. Sums
/// `vm.page_free_count`, `vm.page_speculative_count`, and
/// `vm.page_inactive_count` (the same composition `vm_stat` reports as
/// "available") times the page size from `hw.pagesize`. Returns 0 on
/// any syscall failure (best-effort observability).
#[cfg(target_os = "macos")]
fn read_mem_available_mb() -> u64 {
    let page_size = sysctl_u64(c"hw.pagesize").unwrap_or(4096);
    let free = sysctl_u64(c"vm.page_free_count").unwrap_or(0);
    let speculative = sysctl_u64(c"vm.page_speculative_count").unwrap_or(0);
    let available_pages = free.saturating_add(speculative);
    available_pages.saturating_mul(page_size) / (1 << 20)
}

/// Read a `sysctlbyname` integer-valued key. Tries u64 first, falls
/// back to u32 (Darwin returns page counts as 32-bit on some keys).
#[cfg(target_os = "macos")]
fn sysctl_u64(name: &core::ffi::CStr) -> Option<u64> {
    // Try as u64 (8 bytes).
    let mut value_u64: u64 = 0;
    let mut size: libc::size_t = core::mem::size_of::<u64>();
    // SAFETY: name is a valid C string; oldp/oldlenp point to a stack
    // buffer of size `size`; newp is null (read-only request).
    let res = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            core::ptr::from_mut::<u64>(&mut value_u64).cast(),
            &raw mut size,
            core::ptr::null_mut(),
            0,
        )
    };
    if res == 0 && size == core::mem::size_of::<u64>() {
        return Some(value_u64);
    }
    // Fall back to u32.
    let mut value_u32: u32 = 0;
    let mut size: libc::size_t = core::mem::size_of::<u32>();
    let res = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            core::ptr::from_mut::<u32>(&mut value_u32).cast(),
            &raw mut size,
            core::ptr::null_mut(),
            0,
        )
    };
    if res == 0 && size == core::mem::size_of::<u32>() {
        Some(u64::from(value_u32))
    } else {
        None
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// #85 P1 (test stamp 2026-06-07): UploadSemaphoreMetrics exposes
    /// the three named metric fields. Mutation: rename `waiters` to
    /// `_waiters_renamed` — this test red-fails because
    /// `acquire_with_metrics` no longer maintains the gauge under that
    /// path. (Field rename is the canonical mutation per CLAUDE.md.)
    #[test]
    fn p1_upload_semaphore_metric_fields_present() {
        let m = UploadSemaphoreMetrics::new();
        assert_eq!(m.max_permits, MAX_CONCURRENT_UPLOADS as u64);
        assert_eq!(m.waiters.load(Ordering::Relaxed), 0);
        assert_eq!(
            m.semaphore.available_permits(),
            MAX_CONCURRENT_UPLOADS
        );
    }

    /// #85 P1 (2026-06-07): acquire raises waiters during contention.
    /// Drives the path that `acquire_with_metrics` would actually run.
    #[tokio::test]
    async fn p1_upload_semaphore_acquire_metric() {
        let m = UploadSemaphoreMetrics::new();
        let permit = m.acquire().await;
        assert_eq!(m.semaphore.available_permits(), MAX_CONCURRENT_UPLOADS - 1);
        // waiters should be 0 once acquire returns
        assert_eq!(m.waiters.load(Ordering::Relaxed), 0);
        drop(permit);
        assert_eq!(m.semaphore.available_permits(), MAX_CONCURRENT_UPLOADS);
    }

    /// #85 P2 (2026-06-07): WorkerActionsInFlight exposes the
    /// `counter` field. Mutation: rename `counter` to `_counter_x` →
    /// type no longer compiles (struct member rename); the test
    /// red-fails.
    #[test]
    fn p2_actions_in_flight_field_present() {
        let m = WorkerActionsInFlight::new();
        assert_eq!(m.counter.load(Ordering::Relaxed), 0);
        m.counter.fetch_add(3, Ordering::Relaxed);
        assert_eq!(m.counter.load(Ordering::Relaxed), 3);
    }

    /// #85 P3 (2026-06-07): histograms record observations on the
    /// correct (direction, size) cell. Mutation: swap
    /// `BsSizeBucket::Small` → `BsSizeBucket::Large` in the `observe`
    /// call → counts shift cells, test red-fails.
    #[test]
    fn p3_bytestream_histograms_cell_routing() {
        let h = BytestreamWriteHistograms::new();
        // 512 KiB upload → Small
        h.observe(BsDirection::Upload, 512 * 1024, 10);
        assert_eq!(h.cell_count(BsDirection::Upload, BsSizeBucket::Small), 1);
        // 10 MiB download → Medium
        h.observe(BsDirection::Download, 10 * (1 << 20), 100);
        assert_eq!(
            h.cell_count(BsDirection::Download, BsSizeBucket::Medium),
            1
        );
        // 100 MiB upload → Large
        h.observe(BsDirection::Upload, 100 * (1 << 20), 5000);
        assert_eq!(h.cell_count(BsDirection::Upload, BsSizeBucket::Large), 1);
        // Other cells remain zero.
        assert_eq!(h.cell_count(BsDirection::Download, BsSizeBucket::Small), 0);
        assert_eq!(h.cell_count(BsDirection::Upload, BsSizeBucket::Medium), 0);
        assert_eq!(
            h.cell_count(BsDirection::Download, BsSizeBucket::Large),
            0
        );
    }

    /// #85 P3 (2026-06-07): size-bucket boundary cases — exactly 1 MiB
    /// is Medium, exactly 50 MiB is Medium, 50 MiB + 1 is Large.
    #[test]
    fn p3_size_bucket_boundaries() {
        assert_eq!(BsSizeBucket::from_bytes(0), BsSizeBucket::Small);
        assert_eq!(
            BsSizeBucket::from_bytes((1 << 20) - 1),
            BsSizeBucket::Small
        );
        assert_eq!(BsSizeBucket::from_bytes(1 << 20), BsSizeBucket::Medium);
        assert_eq!(
            BsSizeBucket::from_bytes(50 * (1 << 20)),
            BsSizeBucket::Medium
        );
        assert_eq!(
            BsSizeBucket::from_bytes(50 * (1 << 20) + 1),
            BsSizeBucket::Large
        );
    }

    /// #85 P5 (2026-06-07): EvictingMapLockHistogram records via
    /// `observe`. Mutation: rename `observe` body to no-op → count
    /// stays 0, test red-fails on the `1` assertion.
    #[test]
    fn p5_evicting_map_lock_histogram_observe() {
        let h = EvictingMapLockHistogram::new();
        assert_eq!(h.total_count(), 0);
        h.observe(10);
        h.observe(75);
        h.observe(1500);
        assert_eq!(h.total_count(), 3);
        // 10 ms goes into le_10, le_25, ..., le_30000 buckets.
        assert_eq!(
            h.histogram.buckets[2].load(Ordering::Relaxed),
            1,
            "10 ms must land in le_10 bucket"
        );
    }

    /// #85 P3+P5 (2026-06-07): O11LatencyHistogram bucket ladder is
    /// monotone non-decreasing (each higher bucket count >= lower).
    #[test]
    fn o11_latency_histogram_ladder_monotone() {
        let h = O11LatencyHistogram::new();
        for v in [0, 1, 3, 7, 25, 60, 250, 600, 2000, 10_000, 40_000] {
            h.observe(v);
        }
        // 40_000 > 30_000 boundary, so only inf_bucket gets it.
        let mut prev = 0u64;
        for b in &h.buckets {
            let c = b.load(Ordering::Relaxed);
            assert!(
                c >= prev,
                "bucket counts must be monotone non-decreasing"
            );
            prev = c;
        }
        assert!(h.inf_bucket.load(Ordering::Relaxed) >= prev);
    }

    /// #85 P4 (2026-06-07): on non-macOS the sampler is a no-op
    /// (compile-time guarantee). On macOS we cannot assert the
    /// tracing line from a unit test without a subscriber, so the
    /// test just exercises the spawn under a runtime.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn p4_system_metrics_sampler_noop_on_non_macos() {
        // Calling on non-macOS must not panic and must return
        // immediately (the cfg-noop body).
        spawn_system_metrics_sampler();
    }
}
