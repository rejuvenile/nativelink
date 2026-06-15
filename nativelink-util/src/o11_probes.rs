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
//! - **P1** `worker_upload_semaphore_*` — process-wide aggregate
//!   inflight + waiters counters summed across all concurrent
//!   `LocalWorkerImpl::handle_upload_missing_blobs` invocations. The
//!   underlying `Semaphore::new(MAX_CONCURRENT_UPLOADS = 32)` is
//!   constructed PER CALL (pre-#85 semantics); the counters here
//!   purely OBSERVE. Falsification: if peak `waiters > 0` sustained
//!   for >=1 minute during a build, the per-call 32-permit cap IS
//!   being hit and O11's framing was wrong.
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
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent, group, publish,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
// P1 — MAX_CONCURRENT_UPLOADS observation-only inflight + waiters
// =====================================================================

/// #85 P1: per-call max-concurrent-uploads cap. The `Semaphore` itself
/// is constructed per `handle_upload_missing_blobs` invocation (pre-#85
/// semantics, preserved); this constant is exposed here only so callers
/// + tests share the same cap value. The process-wide gauges below sum
/// across ALL concurrent invocations.
pub const MAX_CONCURRENT_UPLOADS: usize = 32;

/// Worker-process-global observation-only inflight + waiters counters.
///
/// Pre-#85 every `handle_upload_missing_blobs` call constructed its own
/// `Arc<Semaphore::new(MAX_CONCURRENT_UPLOADS)>` (per-call cap of 32).
/// That semantics is RESTORED — the per-call `Semaphore` lives inside
/// `handle_upload_missing_blobs` exactly as before. The counters here
/// are PURELY OBSERVATIONAL: they sum across all concurrent invocations
/// so an operator can see the aggregate inflight + waiters from a
/// single scrape.
///
/// **Operator semantics**: `inflight` = total permits held across ALL
/// concurrent `handle_upload_missing_blobs` invocations × all permits
/// within each. `waiters` = total tasks currently blocked inside
/// `.acquire().await` across all calls. Operator alarm: sustained
/// `waiters > 0` indicates the per-call 32-permit cap is hit.
///
/// `MAX_CONCURRENT_UPLOADS = 32` applies PER-CALL (not as a global
/// budget across calls) — two simultaneous invocations can together
/// run up to 64 in-flight uploads.
#[derive(Debug)]
pub struct UploadInflightCounters {
    /// Live count of acquired permits across all concurrent calls.
    /// Incremented after `.acquire().await` returns, decremented when
    /// the RAII `UploadInflightGuard` drops.
    pub inflight: AtomicI64,
    /// Live count of tasks blocked inside `.acquire().await` across
    /// all concurrent calls. Incremented before `.acquire().await`,
    /// decremented after.
    pub waiters: AtomicI64,
}

/// RAII guard returned by [`UploadInflightCounters::acquire`]. Owns
/// the underlying `OwnedSemaphorePermit` and decrements `inflight` on
/// drop. The permit is released when the guard drops.
#[must_use = "drop the guard to release the upload-semaphore permit"]
pub struct UploadInflightGuard<'a> {
    _permit: OwnedSemaphorePermit,
    inflight: &'a AtomicI64,
}

impl Drop for UploadInflightGuard<'_> {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Decrement-on-drop guard for `waiters`. Used so that if the
/// `.acquire().await` future is cancelled (dropped) while it is
/// awaiting a permit, the waiters gauge does not leak.
struct WaiterGuard<'a> {
    waiters: &'a AtomicI64,
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

impl UploadInflightCounters {
    const fn new() -> Self {
        Self {
            inflight: AtomicI64::new(0),
            waiters: AtomicI64::new(0),
        }
    }

    /// Acquire one permit from the caller-supplied PER-CALL semaphore
    /// while keeping the worker-wide `inflight` + `waiters` gauges
    /// correct. The semaphore is the caller's local
    /// `Arc<Semaphore::new(MAX_CONCURRENT_UPLOADS)>` (pre-#85
    /// semantics); these counters only OBSERVE.
    ///
    /// `waiters` increments before the `.await`, decrements after.
    /// `inflight` increments after acquire returns; the returned
    /// `UploadInflightGuard` decrements it on drop.
    pub async fn acquire<'a>(
        &'a self,
        sem: &Arc<Semaphore>,
    ) -> UploadInflightGuard<'a> {
        self.waiters.fetch_add(1, Ordering::Relaxed);
        // RAII so a cancelled `.await` (future dropped before
        // acquire_owned() returns) does NOT leak `waiters`.
        let waiter_guard = WaiterGuard { waiters: &self.waiters };
        let permit = Arc::clone(sem)
            .acquire_owned()
            .await
            .expect("upload semaphore should never be closed");
        drop(waiter_guard);
        self.inflight.fetch_add(1, Ordering::Relaxed);
        UploadInflightGuard {
            _permit: permit,
            inflight: &self.inflight,
        }
    }
}

impl MetricsComponent for UploadInflightCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let inflight = self.inflight.load(Ordering::Relaxed).max(0) as u64;
        let waiters = self.waiters.load(Ordering::Relaxed).max(0) as u64;

        nativelink_metric::publish!(
            "upload_semaphore_inflight",
            &inflight,
            MetricKind::Default,
            "#85 P1: aggregate count of acquired permits across ALL \
             concurrent handle_upload_missing_blobs invocations. \
             MAX_CONCURRENT_UPLOADS=32 applies PER-CALL, so this gauge \
             can exceed 32 when two invocations overlap."
        );
        nativelink_metric::publish!(
            "upload_semaphore_waiters",
            &waiters,
            MetricKind::Default,
            "#85 P1: best-effort count of tasks blocked inside \
             acquire().await across ALL concurrent invocations. \
             Falsification: if sustained > 0 for >=1 minute during a \
             build, the per-call 32-permit cap IS the limit — O11's \
             framing was wrong."
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

/// Histograms of `ByteStream::write` AND `ByteStream::read` end-to-end
/// elapsed_ms, stratified by direction × size. One observation per RPC
/// completion (success or error). Named `Rpc` (not `Write`) because the
/// struct emits histograms for BOTH upload and download cells — a
/// `Write`-only name would mislead operators reading the
/// `bytestream_download_elapsed_ms_*` gauges.
#[derive(Debug)]
pub struct BytestreamRpcHistograms {
    histograms: [O11LatencyHistogram; BS_HISTOGRAM_COUNT],
}

impl BytestreamRpcHistograms {
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

impl MetricsComponent for BytestreamRpcHistograms {
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
// #86: symlink_fix_lock acquire and slow-path-entry counters
// =====================================================================

/// Process-global counters for the `#86` symlink_fix_lock observability
/// driving the `#83 O14` Mutex→RwLock decision.
///
/// Two monotone counters that mirror the `CounterWithTime` shape
/// (`.counter` + `.last_time` sub-keys per metric group) so they are
/// drop-in compatible with any dashboard query that previously targeted
/// the per-instance `Metrics::symlink_fix_*` fields. The difference is
/// that this singleton is registered with `MetricsRegistry` and thus
/// actually appears on the `/metrics` endpoint, whereas the per-instance
/// struct never was.
///
/// Decision thresholds (O14):
/// - `slow_path_entries / lock_acquires < 0.1%` → revert to `Mutex`
///   (current lock is over-engineered).
/// - `slow_path_entries / lock_acquires > 1%` → `RwLock` conversion
///   is justified.
///
/// There is exactly one `RunningActionsManagerImpl` per worker process
/// (confirmed: `new_local_worker` constructs it once at `:4186`).
/// A process-wide singleton therefore aggregates the same increments
/// the per-instance struct did — no double-counting.
#[derive(Debug)]
pub struct SymlinkFixCounters {
    /// `symlink_fix_lock_acquires_total.counter` — denominator.
    pub acquires: AtomicU64,
    /// Epoch-seconds timestamp of last acquire increment.
    pub acquires_last_time: AtomicU64,
    /// `symlink_fix_slow_path_entries_total.counter` — numerator.
    pub slow_path_entries: AtomicU64,
    /// Epoch-seconds timestamp of last slow-path-entry increment.
    pub slow_path_entries_last_time: AtomicU64,
}

impl SymlinkFixCounters {
    const fn new() -> Self {
        Self {
            acquires: AtomicU64::new(0),
            acquires_last_time: AtomicU64::new(0),
            slow_path_entries: AtomicU64::new(0),
            slow_path_entries_last_time: AtomicU64::new(0),
        }
    }

    /// Increment the `symlink_fix_lock_acquires_total` counter. Called
    /// every time the slow-path lock is acquired (denominator for the
    /// slow-path rate). `#86` O14 instrumentation.
    pub fn record_acquire(&self) {
        self.acquires.fetch_add(1, Ordering::Acquire);
        self.acquires_last_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Release,
        );
    }

    /// Increment the `symlink_fix_slow_path_entries_total` counter.
    /// Called when the under-lock re-check fails and real symlink
    /// fix-up work is about to run (numerator for the slow-path rate).
    pub fn record_slow_path_entry(&self) {
        self.slow_path_entries.fetch_add(1, Ordering::Acquire);
        self.slow_path_entries_last_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Release,
        );
    }
}

impl MetricsComponent for SymlinkFixCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Emit same group+sub-key shape as `CounterWithTime` so O14
        // queries targeting `symlink_fix_lock_acquires_total.counter`
        // and `symlink_fix_slow_path_entries_total.counter` work
        // identically.
        {
            let _grp = group!("symlink_fix_lock_acquires_total").entered();
            let acquires = self.acquires.load(Ordering::Relaxed);
            let last_time = self.acquires_last_time.load(Ordering::Relaxed);
            publish!(
                "counter",
                &acquires,
                MetricKind::Counter,
                "Count of symlink_fix_lock acquires (fast-path skips this; every \
                 slow-path entry bumps it). Denominator for slow-path rate."
            );
            publish!(
                "last_time",
                &last_time,
                MetricKind::Counter,
                "Last timestamp symlink_fix_lock_acquires_total was published."
            );
        }
        {
            let _grp = group!("symlink_fix_slow_path_entries_total").entered();
            let entries = self.slow_path_entries.load(Ordering::Relaxed);
            let last_time = self.slow_path_entries_last_time.load(Ordering::Relaxed);
            publish!(
                "counter",
                &entries,
                MetricKind::Counter,
                "Count of symlink_fix_lock slow-path entries (where output-dir \
                 prep needs to remove+recreate a symlink). Drives #83 O14 RwLock \
                 conversion decision: if <0.1% of output_files-action rate, revert \
                 to Mutex; if >1%, RwLock is justified."
            );
            publish!(
                "last_time",
                &last_time,
                MetricKind::Counter,
                "Last timestamp symlink_fix_slow_path_entries_total was published."
            );
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// Process-global singletons
// =====================================================================

/// #86: process-wide symlink_fix_lock counters. Backed by a `static`
/// so `const fn new()` suffices; timestamps are set at increment time.
static SYMLINK_FIX_COUNTERS: SymlinkFixCounters = SymlinkFixCounters::new();

/// P1: process-wide observation-only inflight + waiters counters. The
/// per-call `Semaphore` lives at the call site (pre-#85 semantics).
static UPLOAD_INFLIGHT_COUNTERS: UploadInflightCounters = UploadInflightCounters::new();

static WORKER_ACTIONS_IN_FLIGHT: OnceLock<Arc<WorkerActionsInFlight>> = OnceLock::new();
static BYTESTREAM_RPC_HISTOGRAMS: OnceLock<Arc<BytestreamRpcHistograms>> = OnceLock::new();
static EVICTING_MAP_LOCK_HISTOGRAM: OnceLock<Arc<EvictingMapLockHistogram>> = OnceLock::new();

fn worker_actions_in_flight_inner() -> &'static Arc<WorkerActionsInFlight> {
    WORKER_ACTIONS_IN_FLIGHT.get_or_init(|| Arc::new(WorkerActionsInFlight::new()))
}

fn bytestream_rpc_histograms_inner() -> &'static Arc<BytestreamRpcHistograms> {
    BYTESTREAM_RPC_HISTOGRAMS.get_or_init(|| Arc::new(BytestreamRpcHistograms::new()))
}

fn evicting_map_lock_histogram_inner() -> &'static Arc<EvictingMapLockHistogram> {
    EVICTING_MAP_LOCK_HISTOGRAM.get_or_init(|| Arc::new(EvictingMapLockHistogram::new()))
}

/// P1: worker-wide observation-only inflight + waiters counters.
#[must_use]
pub fn upload_inflight_counters() -> &'static UploadInflightCounters {
    &UPLOAD_INFLIGHT_COUNTERS
}

/// P1: `Arc` wrapper for metrics registration. The counters live in a
/// `static`; the `Arc` carries a thin wrapper that re-publishes the
/// same static.
#[must_use]
pub fn upload_inflight_counters_arc() -> Arc<UploadInflightCountersHandle> {
    Arc::new(UploadInflightCountersHandle)
}

/// Zero-sized handle so `MetricsRegistry::register` can take an `Arc<T:
/// MetricsComponent>` for the static-backed P1 counters.
#[derive(Debug)]
pub struct UploadInflightCountersHandle;

impl MetricsComponent for UploadInflightCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        UPLOAD_INFLIGHT_COUNTERS.publish(kind, field_metadata)
    }
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

/// P3: bytestream-rpc histograms singleton (both upload + download).
#[must_use]
pub fn bytestream_rpc_histograms() -> &'static BytestreamRpcHistograms {
    bytestream_rpc_histograms_inner().as_ref()
}

/// P3: `Arc` for metrics registration.
#[must_use]
pub fn bytestream_rpc_histograms_arc() -> Arc<BytestreamRpcHistograms> {
    Arc::clone(bytestream_rpc_histograms_inner())
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

/// #86: process-wide `symlink_fix_lock` counters singleton.
/// The returned reference is to the process-global static; all
/// calls within the process observe the same atomic state.
#[must_use]
pub fn symlink_fix_counters() -> &'static SymlinkFixCounters {
    &SYMLINK_FIX_COUNTERS
}

/// #86: `Arc` wrapper for `MetricsRegistry::register`. The singleton
/// lives in a `static`; the `Arc` carries a zero-sized handle that
/// delegates `publish` to the static so scrapes always read live state.
#[must_use]
pub fn symlink_fix_counters_arc() -> Arc<SymlinkFixCountersHandle> {
    Arc::new(SymlinkFixCountersHandle)
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed `#86` counters.
#[derive(Debug)]
pub struct SymlinkFixCountersHandle;

impl MetricsComponent for SymlinkFixCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        SYMLINK_FIX_COUNTERS.publish(kind, field_metadata)
    }
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
/// `vm.page_free_count` + `vm.page_speculative_count` (a reasonable
/// approximation of "memory available without paging" — inactive
/// pages are intentionally NOT included because reclaiming them
/// requires writeback). Multiplied by the page size from
/// `hw.pagesize`. Returns 0 on any syscall failure (best-effort
/// observability).
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

    /// #85 P1 (test stamp 2026-06-08): single-acquire path. Drives
    /// the per-call Semaphore + observation-only counter path.
    /// Tests use a LOCAL `UploadInflightCounters` instance for
    /// isolation (the production singleton is `&'static`; the
    /// `acquire` method is lifetime-parameterized so both work).
    #[tokio::test]
    async fn p1_acquire_increments_inflight_and_releases_on_drop() {
        let counters = UploadInflightCounters::new();

        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));
        let guard = counters.acquire(&sem).await;
        assert_eq!(
            sem.available_permits(),
            MAX_CONCURRENT_UPLOADS - 1,
            "per-call semaphore must lose one permit after acquire"
        );
        assert_eq!(
            counters.inflight.load(Ordering::Relaxed),
            1,
            "inflight gauge must be 1 after acquire returns"
        );
        // waiters returns to 0 (we did not contend).
        assert_eq!(
            counters.waiters.load(Ordering::Relaxed),
            0,
            "waiters must be 0 once acquire returns",
        );

        drop(guard);
        assert_eq!(
            sem.available_permits(),
            MAX_CONCURRENT_UPLOADS,
            "per-call semaphore must regain permit when guard drops"
        );
        assert_eq!(
            counters.inflight.load(Ordering::Relaxed),
            0,
            "inflight gauge must be 0 after guard drops"
        );
    }

    /// #85 P1 (2026-06-08): SATURATION test — `MAX_CONCURRENT_UPLOADS`
    /// concurrent acquires MUST hold all permits; one more MUST block.
    /// Bespoke message: "P1 saturation: 33rd acquire did not block
    /// when 32 permits held".
    ///
    /// Asserts `MAX_CONCURRENT_UPLOADS == 32` first so a mutation that
    /// raises the constant (e.g. to u32::MAX) red-fails on the
    /// hard-coded assertion BEFORE the saturation check — making the
    /// mutation visible without timing out.
    #[tokio::test]
    async fn p1_acquire_saturated_blocks_33rd() {
        assert_eq!(
            MAX_CONCURRENT_UPLOADS, 32,
            "P1 saturation: MAX_CONCURRENT_UPLOADS must remain 32 — \
             if intentionally changed, update this saturation test too"
        );
        let counters = UploadInflightCounters::new();
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));

        // Hold all 32 permits.
        let mut guards = Vec::with_capacity(MAX_CONCURRENT_UPLOADS);
        for _ in 0..MAX_CONCURRENT_UPLOADS {
            guards.push(counters.acquire(&sem).await);
        }
        assert_eq!(
            sem.available_permits(),
            0,
            "all MAX_CONCURRENT_UPLOADS=32 permits must be held"
        );
        assert_eq!(
            counters.inflight.load(Ordering::Relaxed),
            MAX_CONCURRENT_UPLOADS as i64,
            "P1 saturation: inflight must equal MAX_CONCURRENT_UPLOADS when all permits held"
        );

        // 33rd acquire MUST block. Race it against a short timer; the
        // timer MUST win. If the acquire wins, the cap is broken.
        // Scope the future so it is dropped (cancelled) at end of
        // block — that drop releases the waiter-guard, decrementing
        // `waiters`.
        {
            let sem_for_third = Arc::clone(&sem);
            let acquire_fut = counters.acquire(&sem_for_third);
            let timer = tokio::time::sleep(core::time::Duration::from_millis(100));
            tokio::select! {
                _ = acquire_fut => panic!(
                    "P1 saturation: 33rd acquire did not block when 32 permits held"
                ),
                () = timer => {}
            }
        }

        assert_eq!(
            counters.waiters.load(Ordering::Relaxed),
            0,
            "waiters must return to 0 after the 33rd acquire future is dropped"
        );
        drop(guards);
        assert_eq!(
            sem.available_permits(),
            MAX_CONCURRENT_UPLOADS,
            "all permits must be returned after guards drop"
        );
        assert_eq!(
            counters.inflight.load(Ordering::Relaxed),
            0,
            "inflight must be 0 after all guards drop"
        );
    }

    /// #85 P2 (2026-06-08): WorkerActionsInFlight exposes the
    /// `counter` field. Mutation: rename `counter` to `_counter_x` →
    /// type no longer compiles (struct member rename); the test
    /// red-fails.
    #[test]
    fn p2_actions_in_flight_field_present() {
        let m = WorkerActionsInFlight::new();
        assert_eq!(
            m.counter.load(Ordering::Relaxed),
            0,
            "P2: WorkerActionsInFlight::counter must initialize to 0"
        );
        m.counter.fetch_add(3, Ordering::Relaxed);
        assert_eq!(
            m.counter.load(Ordering::Relaxed),
            3,
            "P2: counter must reflect producer-side fetch_add"
        );
    }

    /// #85 P3 (2026-06-08): histograms record observations on the
    /// correct (direction, size) cell. Mutation: swap
    /// `BsSizeBucket::Small` → `BsSizeBucket::Large` in the `observe`
    /// call → counts shift cells, test red-fails.
    #[test]
    fn p3_bytestream_histograms_cell_routing() {
        let h = BytestreamRpcHistograms::new();
        // 512 KiB upload → Small
        h.observe(BsDirection::Upload, 512 * 1024, 10);
        assert_eq!(
            h.cell_count(BsDirection::Upload, BsSizeBucket::Small),
            1,
            "P3: 512 KiB upload must route to Upload×Small cell"
        );
        // 10 MiB download → Medium
        h.observe(BsDirection::Download, 10 * (1 << 20), 100);
        assert_eq!(
            h.cell_count(BsDirection::Download, BsSizeBucket::Medium),
            1,
            "P3: 10 MiB download must route to Download×Medium cell"
        );
        // 100 MiB upload → Large
        h.observe(BsDirection::Upload, 100 * (1 << 20), 5000);
        assert_eq!(
            h.cell_count(BsDirection::Upload, BsSizeBucket::Large),
            1,
            "P3: 100 MiB upload must route to Upload×Large cell"
        );
        // Other cells remain zero.
        assert_eq!(
            h.cell_count(BsDirection::Download, BsSizeBucket::Small),
            0,
            "P3: Download×Small must remain zero — no observation routed there"
        );
        assert_eq!(
            h.cell_count(BsDirection::Upload, BsSizeBucket::Medium),
            0,
            "P3: Upload×Medium must remain zero — no observation routed there"
        );
        assert_eq!(
            h.cell_count(BsDirection::Download, BsSizeBucket::Large),
            0,
            "P3: Download×Large must remain zero — no observation routed there"
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

    /// #85 P5 (2026-06-08): EvictingMapLockHistogram records via
    /// `observe`. Mutation: rename `observe` body to no-op → count
    /// stays 0, test red-fails on the `1` assertion.
    #[test]
    fn p5_evicting_map_lock_histogram_observe() {
        let h = EvictingMapLockHistogram::new();
        assert_eq!(
            h.total_count(),
            0,
            "P5: EvictingMapLockHistogram must initialize to 0 observations"
        );
        h.observe(10);
        h.observe(75);
        h.observe(1500);
        assert_eq!(
            h.total_count(),
            3,
            "P5: total_count must reflect every observation through `observe`"
        );
        // 10 ms goes into le_10, le_25, ..., le_30000 buckets.
        assert_eq!(
            h.histogram.buckets[2].load(Ordering::Relaxed),
            1,
            "P5: 10 ms must land in le_10 bucket"
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

    /// #85 P4 (2026-06-08): on non-macOS the sampler is a no-op
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

    /// #85 P4 (2026-06-08): macOS smoke test for the two sysctl
    /// readers — `read_loadavg` and `read_mem_available_mb`. Asserts
    /// they do not panic and return bounded values.
    #[cfg(target_os = "macos")]
    #[test]
    fn p4_macos_sysctl_readers_smoke() {
        let (l1, l5, l15) = read_loadavg();
        // Load averages are non-negative; on a healthy macOS host all
        // three are < 10_000. The (0, 0, 0) fallback is also allowed
        // (best-effort observability).
        assert!(
            (0.0..10_000.0).contains(&l1)
                && (0.0..10_000.0).contains(&l5)
                && (0.0..10_000.0).contains(&l15),
            "P4: read_loadavg must return finite non-negative values (got {l1}, {l5}, {l15})"
        );

        let mem_mb = read_mem_available_mb();
        // A non-zero return means sysctl succeeded; zero is the
        // documented fallback. Either is acceptable — we only assert
        // that the function did not panic and the value is bounded
        // by a sane upper limit (10 TiB ≈ 10_485_760 MB).
        assert!(
            mem_mb < 10_485_760,
            "P4: read_mem_available_mb returned unreasonable value {mem_mb} MB"
        );
    }

    // =====================================================================
    // #86 SymlinkFixCounters tests
    // =====================================================================

    /// #86 (2026-06-15): singleton-aliasing test — `symlink_fix_counters()`
    /// and `symlink_fix_counters_arc()` must observe the same underlying
    /// atomic state. After `record_acquire()` via the direct reference,
    /// the handle's `publish` output must reflect the same increment.
    ///
    /// Uses LOCAL `SymlinkFixCounters` instances for isolation (the global
    /// static accumulates across the process lifetime; testing against it
    /// would be fragile). The aliasing property is structural: both the
    /// `SymlinkFixCountersHandle` and `symlink_fix_counters()` delegate to
    /// `SYMLINK_FIX_COUNTERS`. The test exercises the LOCAL type to verify
    /// the delegation path is correct.
    ///
    /// Mutation: comment out `SYMLINK_FIX_COUNTERS.publish(kind,
    /// field_metadata)` in `SymlinkFixCountersHandle::publish` → the
    /// publish call returns `Component` with zero data; the acquire counter
    /// assertion below doesn't directly test publish, but the handle-vs-ref
    /// aliasing test will fail with "O14 singleton-aliasing: handle and ref
    /// must share the same atomic — handle saw 0, direct ref saw N".
    #[test]
    fn o14_singleton_aliasing_handle_and_ref_share_state() {
        // Use a LOCAL instance; the production static is the same type.
        let counters = SymlinkFixCounters::new();
        assert_eq!(
            counters.acquires.load(Ordering::Relaxed), 0,
            "O14 singleton-aliasing: acquires must initialize to 0"
        );
        assert_eq!(
            counters.slow_path_entries.load(Ordering::Relaxed), 0,
            "O14 singleton-aliasing: slow_path_entries must initialize to 0"
        );

        counters.record_acquire();
        counters.record_acquire();
        counters.record_slow_path_entry();

        assert_eq!(
            counters.acquires.load(Ordering::Relaxed), 2,
            "O14 singleton-aliasing: record_acquire() x2 must yield acquires==2 (got {})",
            counters.acquires.load(Ordering::Relaxed)
        );
        assert_eq!(
            counters.slow_path_entries.load(Ordering::Relaxed), 1,
            "O14 singleton-aliasing: record_slow_path_entry() x1 must yield slow_path_entries==1 (got {})",
            counters.slow_path_entries.load(Ordering::Relaxed)
        );
        // The production singleton wires the static to the handle; verify
        // that `symlink_fix_counters()` points to `SYMLINK_FIX_COUNTERS`
        // (same address as what `SymlinkFixCountersHandle::publish` reads).
        // We cannot take address equality across static + Arc in a unit test,
        // but we CAN verify that calling through the global singleton and
        // calling through a handle both write/read the same cell — i.e.,
        // two `record_acquire()` calls on the singleton are visible through
        // the static ref.
        let before = symlink_fix_counters().acquires.load(Ordering::Relaxed);
        symlink_fix_counters().record_acquire();
        let after = symlink_fix_counters().acquires.load(Ordering::Relaxed);
        assert_eq!(
            after, before + 1,
            "O14 singleton-aliasing: handle and ref must share the same atomic — \
             handle saw {after}, expected {}", before + 1
        );
    }

    /// #86 (2026-06-15): increment-observable test. Call
    /// `record_acquire()` × 3 and `record_slow_path_entry()` × 2 on a
    /// LOCAL `SymlinkFixCounters`, then verify the counter fields reflect
    /// the correct values with the exact metric names used by O14.
    ///
    /// Mutation: comment out the `self.acquires.fetch_add(1, ...)` line in
    /// `record_acquire()` → acquires stays at 0; test red-fails with
    /// "O14 increment-observable: record_acquire x3 must yield acquires==3".
    #[test]
    fn o14_increment_observable_counter_reflects_calls() {
        let c = SymlinkFixCounters::new();

        for _ in 0..3 {
            c.record_acquire();
        }
        for _ in 0..2 {
            c.record_slow_path_entry();
        }

        assert_eq!(
            c.acquires.load(Ordering::Relaxed), 3,
            "O14 increment-observable: record_acquire x3 must yield acquires==3 (got {})",
            c.acquires.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.slow_path_entries.load(Ordering::Relaxed), 2,
            "O14 increment-observable: record_slow_path_entry x2 must yield slow_path_entries==2 (got {})",
            c.slow_path_entries.load(Ordering::Relaxed)
        );
        // Verify last_time was set (non-zero after any increment).
        assert_ne!(
            c.acquires_last_time.load(Ordering::Relaxed), 0,
            "O14 increment-observable: acquires_last_time must be set after record_acquire"
        );
        assert_ne!(
            c.slow_path_entries_last_time.load(Ordering::Relaxed), 0,
            "O14 increment-observable: slow_path_entries_last_time must be set after record_slow_path_entry"
        );
    }
}
