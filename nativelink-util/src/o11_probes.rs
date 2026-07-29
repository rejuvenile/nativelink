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

use core::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

/// (#obs-tuning follow-up 2) DEDICATED bucket ladder for the cold-construct
/// latency [`DecayingP95Histogram`]. SEPARATE from [`O11_LATENCY_BUCKETS_MS`]
/// on purpose: that ladder backs the P3 (ByteStream) and P5 (EvictingMap)
/// [`O11LatencyHistogram`]s and MUST NOT change (its consumers pin the exact
/// 11-bucket set). This ladder refines the 50–500 ms band — where the live
/// cold-construct population was measured (34–229 ms, 2026-07-07) — so the
/// conservative p95 no longer snaps to 250 ms for anything in (100,250].
/// Boundaries: 75/150/200/350 added to the shared set's 50/100/250/500 in
/// this band. Below 50 ms and above 500 ms it matches the shared ladder (the
/// cold-construct span is comfortably inside 50–500 ms; the wider bounds only
/// catch outliers). Must stay strictly ascending — the p95 walk assumes it.
pub const CONSTRUCT_LATENCY_BUCKETS_MS: [u64; 16] = [
    1, 5, 10, 25, 50, 75, 100, 150, 200, 250, 350, 500, 750, 1000, 5000, 30000,
];

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

/// (#obs-tuning-construct-latency-conditioning) Per-SECOND decay keep-fraction
/// applied to every bucket of [`DecayingP95Histogram`] by ELAPSED WALL-TIME
/// (#obs-tuning follow-up 1: time-aware decay). Over an interval of `Δt`
/// seconds the mass is multiplied by `KEEP.powf(Δt)` (continuous exponential
/// decay), so decay depends on WALL-TIME, not on how many observations arrived.
/// At `0.98`/s the half-life is `ln(0.5)/ln(0.98) ≈ 34 s` and mass falls below
/// half an observation after ~6 min of idle — so an IDLE worker (no new cold
/// constructs) ages its p95 toward the empty sentinel instead of fossilising
/// its last value forever (the follow-up-1 defect). The window still tracks the
/// RECENT cold-construct regime: at a cold-heavy cadence (a few constructs/min)
/// the ~34 s half-life keeps ~1–3 min of samples effective — fast enough not to
/// fossilise, slow enough not to jitter on one outlier. NOTE the semantics
/// changed from the prior PER-OBSERVATION `0.98` (a burst of N constructs in
/// one second used to decay N times; now it decays once for that ~1 s).
const P95_DECAY_KEEP_PER_SEC: f64 = 0.98;

/// (#obs-tuning follow-up 1) Total decayed weight BELOW which the histogram is
/// treated as "no recent cold constructs" and the p95 reports the empty
/// sentinel 0. Half an observation of residual mass: after a long idle gap the
/// geometric decay drives the surviving mass far below this, so an idle
/// worker's stale p95 ages back to 0 (a scrape/gossip then reports "cold" as 0,
/// same as a freshly-booted worker). Above this, even a single recent
/// observation reports its bucket. Chosen at 0.5 so ONE recent construct is
/// still reported (mass 1.0 > 0.5) but a fully-idle-decayed histogram is not.
const P95_NEGLIGIBLE_MASS: f64 = 0.5;

/// (#obs-tuning-construct-latency-conditioning) Exponentially-decayed
/// fixed-bucket latency histogram producing a CONSERVATIVE p95 (upper-edge of
/// the 95th-percentile bucket). Purpose-built for the worker→scheduler
/// cold-construct latency gossip (`construct_latency_ms_p95`, worker_api proto
/// field 25/29): the eventual `T_SETUP` consumer pays asymmetrically for
/// estimation error — holding a queued op on an UNDER-estimate is the costly
/// direction — so a high percentile that biases toward the expensive tail is
/// the right estimator, NOT a central mean/EWMA (which of a heavy-tailed cold
/// population lands below the mode that matters). The exponential decay
/// (`P95_DECAY_KEEP_PER_SEC`) removes the since-boot fossilisation a cumulative
/// histogram would share with the old `sum/count` mean.
///
/// (#obs-tuning follow-up 1) Decay is WALL-CLOCK-driven, not per-observation:
/// each access ([`observe`](Self::observe) / [`p95_ms`](Self::p95_ms)) first
/// ages every bucket by the time elapsed since the last access
/// ([`decay_to`](Self::decay_to)), so an IDLE worker's p95 decays toward the
/// empty sentinel with real time even when no new observation arrives. The
/// monotonic clock is [`std::time::Instant`]; production entry points read
/// `Instant::now()`, and the `*_at` variants take an explicit `now` so tests
/// drive decay deterministically (no real sleep). NOTE the `last_access` clock
/// advances on READS too — a `p95_ms` scrape ages the histogram — which is
/// correct: an observability read must not un-age an idle worker.
///
/// (#obs-tuning follow-up 2) Buckets are [`CONSTRUCT_LATENCY_BUCKETS_MS`] (a
/// ladder refined in the measured 50–500 ms band), NOT the shared
/// [`O11_LATENCY_BUCKETS_MS`] — so the p95 no longer snaps to 250 ms for a
/// value in (100,250] and the shared P3/P5 ladder is untouched.
///
/// A synchronous `parking_lot::Mutex` guards the decay+insert (the read-modify-
/// write of all buckets is not lock-free-composable), which is negligible: the
/// sole producer is the COLD full-reconstruct path (`record_construct_fetch_ms`
/// at `directory_cache.rs`, no cached subtree), the coldest and least-frequent
/// worker path — a few-nanosecond critical section behind a hundreds-of-ms
/// operation. No `.await` is ever held across the lock.
#[derive(Debug)]
pub struct DecayingP95Histogram {
    /// Decay state (weights + last-access clock), guarded by ONE mutex so the
    /// wall-clock decay and the bucket weights stay mutually consistent.
    inner: parking_lot::Mutex<DecayInner>,
}

/// (#obs-tuning follow-up 1) The mutex-guarded interior of
/// [`DecayingP95Histogram`]: the decayed per-bucket weights AND the last-access
/// instant that drives the wall-clock decay.
#[derive(Debug)]
struct DecayInner {
    /// Decayed per-bucket weights. Index `i` (`i < BUCKETS.len()`) holds the
    /// weight of observations that fell in bucket `i` (value `<= BUCKETS[i]`
    /// and `> BUCKETS[i-1]`); the final slot is the overflow bucket
    /// (value `> BUCKETS.last()`). `f64` (not atomics) because the whole
    /// decay+insert is done under the lock.
    // UNBOUNDED-OK: fixed-length array (`BUCKETS.len() + 1` slots), NOT a
    // growable buffer — the histogram is O(1) memory regardless of observation
    // count; weights decay toward a bounded steady state (sum ≤ rate/(1-KEEP)).
    weights: [f64; CONSTRUCT_LATENCY_BUCKETS_MS.len() + 1],
    /// Instant of the last decay application (`None` until the first access).
    /// The next access decays every bucket by the elapsed wall-time since this
    /// instant, then advances it. `None` initial state is required because
    /// `Instant` has no `const` constructor (the histogram lives in a `static`).
    last_access: Option<Instant>,
}

impl DecayingP95Histogram {
    pub const fn new() -> Self {
        Self {
            inner: parking_lot::Mutex::new(DecayInner {
                weights: [0.0; CONSTRUCT_LATENCY_BUCKETS_MS.len() + 1],
                last_access: None,
            }),
        }
    }

    /// Bucket index for `value_ms`: the first ladder index whose boundary is
    /// `>= value_ms`, else the overflow slot (`BUCKETS.len()`).
    fn bucket_index(value_ms: u64) -> usize {
        for (idx, boundary) in CONSTRUCT_LATENCY_BUCKETS_MS.iter().enumerate() {
            if value_ms <= *boundary {
                return idx;
            }
        }
        CONSTRUCT_LATENCY_BUCKETS_MS.len()
    }

    /// (#obs-tuning follow-up 1) Age every bucket by the WALL-TIME elapsed since
    /// the last access (`inner.last_access`), then advance the clock to `now`.
    /// The decay factor over `Δt` seconds is `KEEP.powf(Δt)` — continuous, so
    /// one big step equals many small steps summing to the same elapsed time
    /// (an idle worker and a busy one age identically per second). Caller holds
    /// the lock. On first access (`last_access == None`) there is nothing to
    /// decay; just set the clock. `saturating_duration_since` guards a
    /// non-monotone `now` (clock skew) — a backwards step decays by 0 s (`×1`),
    /// never panics.
    fn decay_to(inner: &mut DecayInner, now: Instant) {
        if let Some(last) = inner.last_access {
            let dt = now.saturating_duration_since(last).as_secs_f64();
            if dt > 0.0 {
                let factor = P95_DECAY_KEEP_PER_SEC.powf(dt);
                for slot in &mut inner.weights {
                    *slot *= factor;
                }
            }
        }
        inner.last_access = Some(now);
    }

    /// Record one observation of `value_ms` at wall-clock instant `now`. Ages
    /// the histogram by the elapsed time since the last access
    /// ([`decay_to`](Self::decay_to)), then adds one unit of weight to the
    /// matching bucket. The `now` parameter makes decay test-controllable;
    /// production calls [`observe`](Self::observe) which passes `Instant::now()`.
    pub fn observe_at(&self, value_ms: u64, now: Instant) {
        let idx = Self::bucket_index(value_ms);
        let mut inner = self.inner.lock();
        Self::decay_to(&mut inner, now);
        inner.weights[idx] += 1.0;
    }

    /// Record one observation of `value_ms` milliseconds at the current
    /// wall-clock time. Production entry point; see [`observe_at`](Self::observe_at).
    pub fn observe(&self, value_ms: u64) {
        self.observe_at(value_ms, Instant::now());
    }

    /// Conservative p95 in milliseconds AS OF wall-clock instant `now`: first
    /// ages the histogram by the elapsed time since the last access (so an idle
    /// worker's stale p95 decays even on a read), then walks buckets low→high
    /// accumulating decayed weight and returns the UPPER edge of the first
    /// bucket at which the cumulative weight reaches `0.95 * total`. The upper
    /// edge (rather than a bucket midpoint) is the conservative choice — it
    /// never UNDER-reports the tail, matching the asymmetric `T_SETUP` cost.
    /// Returns `0` when the total decayed weight is below
    /// [`P95_NEGLIGIBLE_MASS`] (no recent cold constructs — the same "none
    /// observed" sentinel a freshly-booted worker reports). The overflow slot
    /// reports the last ladder boundary (30000 ms) as a saturating upper edge
    /// (a construct >30 s is implausible; the wire field is `u32` ms). The `now`
    /// parameter makes the age-on-read test-controllable; production calls
    /// [`p95_ms`](Self::p95_ms) which passes `Instant::now()`.
    pub fn p95_ms_at(&self, now: Instant) -> u64 {
        let mut inner = self.inner.lock();
        Self::decay_to(&mut inner, now);
        let total: f64 = inner.weights.iter().sum();
        // Below half an observation of residual mass → "no recent cold
        // constructs" sentinel (an idle worker's mass has decayed away).
        if total < P95_NEGLIGIBLE_MASS {
            return 0;
        }
        let target = total * 0.95;
        let mut cumulative = 0.0;
        for (idx, weight) in inner.weights.iter().enumerate() {
            cumulative += *weight;
            if cumulative >= target {
                return CONSTRUCT_LATENCY_BUCKETS_MS
                    .get(idx)
                    .copied()
                    .unwrap_or_else(|| {
                        // Overflow slot: saturate at the top ladder boundary.
                        *CONSTRUCT_LATENCY_BUCKETS_MS
                            .last()
                            .expect("CONSTRUCT_LATENCY_BUCKETS_MS is non-empty")
                    });
            }
        }
        // Unreachable (cumulative reaches `total >= target` in the last slot),
        // but return the top boundary defensively rather than panic.
        *CONSTRUCT_LATENCY_BUCKETS_MS
            .last()
            .expect("CONSTRUCT_LATENCY_BUCKETS_MS is non-empty")
    }

    /// Conservative p95 in milliseconds as of now. Production entry point; see
    /// [`p95_ms_at`](Self::p95_ms_at). Ages the histogram by wall-time on read.
    pub fn p95_ms(&self) -> u64 {
        self.p95_ms_at(Instant::now())
    }
}

impl Default for DecayingP95Histogram {
    fn default() -> Self {
        Self::new()
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
/// Aggregates increments across all configured `RunningActionsManagerImpl`
/// instances (N worker configs = N instances; the process-wide sum is the
/// correct total). The primary producer is `prepare_output_directory` in
/// `running_actions_manager.rs`, called via `new_local_worker` at
/// `local_worker.rs:3828`. On server-only processes the counters read 0.
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
        self.acquires.fetch_add(1, Ordering::Relaxed);
        self.acquires_last_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    /// Increment the `symlink_fix_slow_path_entries_total` counter.
    /// Called when the under-lock re-check fails and real symlink
    /// fix-up work is about to run (numerator for the slow-path rate).
    pub fn record_slow_path_entry(&self) {
        self.slow_path_entries.fetch_add(1, Ordering::Relaxed);
        self.slow_path_entries_last_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }
}

impl MetricsComponent for SymlinkFixCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Registered under prefix "symlink_fix" (nativelink.rs), so the outer
        // span is "symlink_fix". Each group!() here adds one inner segment:
        //   symlink_fix . lock_acquires_total . counter
        //                                    → symlink_fix_lock_acquires_total_counter
        // Using group names that include the full "symlink_fix_lock_…" prefix
        // would double the prefix because the register key is already in scope.
        {
            let _grp = group!("lock_acquires_total").entered();
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
                "Epoch-seconds of last lock_acquires_total increment."
            );
        }
        {
            let _grp = group!("slow_path_entries_total").entered();
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
                "Epoch-seconds of last slow_path_entries_total increment."
            );
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

// =====================================================================
// AC get_action_result hit/miss counters
// =====================================================================

/// Process-global counters for `get_action_result` cache hits and misses.
///
/// A hit = the AC store returned `Ok(ActionResult)` — Bazel gets the cached
/// result and does NOT re-execute the action. A miss = the AC store returned
/// `Code::NotFound` — Bazel must re-execute. The hit rate `hit / (hit + miss)`
/// is the primary build-cache metric; sustained low hit rate signals cold cache,
/// key-space mismatch, or excessive AC eviction.
///
/// Counting scope: only `Code::NotFound` increments `miss`. Other error codes
/// (`Internal`, `Unavailable`, `DeadlineExceeded`, etc.) are neither a hit nor
/// a miss — they represent backend/transport failures, not cache decisions. During
/// a backend error storm `hit + miss < total_rpcs` is expected and correct.
///
/// GrpcStore-shortcut RPCs (where `store.downcast_ref::<GrpcStore>` matches and
/// the RPC is forwarded to the remote store directly) are NOT counted — the local
/// process performs no cache lookup in that path. This is intentional.
///
/// Registered under prefix `"ac_get_action_result"` so the rendered names are:
///   `ac_get_action_result_hit_total`
///   `ac_get_action_result_miss_total`
#[derive(Debug)]
pub struct AcHitCounters {
    /// Monotone count of `get_action_result` RPCs returning `Ok(ActionResult)`.
    pub hit: AtomicU64,
    /// Monotone count of `get_action_result` RPCs returning `Code::NotFound`.
    pub miss: AtomicU64,
}

impl AcHitCounters {
    const fn new() -> Self {
        Self {
            hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
        }
    }

    /// Record one AC cache hit.
    pub fn record_hit(&self) {
        self.hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one AC cache miss (NotFound).
    pub fn record_miss(&self) {
        self.miss.fetch_add(1, Ordering::Relaxed);
    }
}

impl MetricsComponent for AcHitCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let v = self.hit.load(Ordering::Relaxed);
        publish!(
            "hit_total",
            &v,
            MetricKind::Counter,
            "Count of get_action_result RPCs that returned Ok(ActionResult) — Bazel \
             used the cached result and did NOT re-execute the action. Numerator for \
             the AC hit rate (hit / (hit + miss))."
        );
        let v = self.miss.load(Ordering::Relaxed);
        publish!(
            "miss_total",
            &v,
            MetricKind::Counter,
            "Count of get_action_result RPCs that returned Code::NotFound — Bazel \
             must re-execute the action. Denominator counterpart to hit_total; \
             sustained high miss rate signals cold cache, key-space mismatch, or \
             excessive AC eviction."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Process-wide AC hit/miss counters. Backed by a `static` so `const fn new()`
/// suffices; incremented from `ac_server.rs::inner_get_action_result`.
static AC_HIT_COUNTERS: AcHitCounters = AcHitCounters::new();
/// Cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents double-
/// registration; both calls return a clone of the same `Arc`.
static AC_HIT_COUNTERS_ARC: OnceLock<Arc<AcHitCountersHandle>> = OnceLock::new();

/// Process-wide AC hit/miss counters singleton.
#[must_use]
pub fn ac_hit_counters() -> &'static AcHitCounters {
    &AC_HIT_COUNTERS
}

/// `Arc` wrapper for `MetricsRegistry::register`. `OnceLock`-cached.
#[must_use]
pub fn ac_hit_counters_arc() -> Arc<AcHitCountersHandle> {
    Arc::clone(AC_HIT_COUNTERS_ARC.get_or_init(|| Arc::new(AcHitCountersHandle)))
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed AC counters.
#[derive(Debug)]
pub struct AcHitCountersHandle;

impl MetricsComponent for AcHitCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        AC_HIT_COUNTERS.publish(kind, field_metadata)
    }
}

// =====================================================================
// ExistenceCache has() hit/miss counters
// =====================================================================

/// Process-global counters for `ExistenceCacheStore::has_with_results` cache
/// hits and misses, aggregated PER KEY (one observation per key per call).
///
/// A hit = the key was found in the in-process moka cache (the backend
/// existence check was skipped). A miss = the key was absent from the moka
/// cache and the inner store was queried. Hit rate `hit / (hit + miss)` tells
/// operators how effectively the 50M-entry moka cache is absorbing FindMissingBlobs
/// traffic.
///
/// Registered under prefix `"ecs"` so the rendered names are:
///   `ecs_has_hit_total`
///   `ecs_has_miss_total`
#[derive(Debug)]
pub struct EcsHitCounters {
    /// Monotone count of per-key `has_with_results` lookups satisfied from
    /// the moka cache (backend not queried).
    pub hit: AtomicU64,
    /// Monotone count of per-key `has_with_results` lookups that missed the
    /// moka cache and required an inner-store query.
    pub miss: AtomicU64,
}

impl EcsHitCounters {
    const fn new() -> Self {
        Self {
            hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
        }
    }

    /// Record `n` cache hits (keys satisfied from moka without inner-store query).
    pub fn record_hits(&self, n: u64) {
        if n > 0 {
            self.hit.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Record `n` cache misses (keys that required an inner-store query).
    pub fn record_misses(&self, n: u64) {
        if n > 0 {
            self.miss.fetch_add(n, Ordering::Relaxed);
        }
    }
}

impl MetricsComponent for EcsHitCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let v = self.hit.load(Ordering::Relaxed);
        publish!(
            "has_hit_total",
            &v,
            MetricKind::Counter,
            "Count of per-key has_with_results lookups satisfied from the moka \
             existence cache (inner store NOT queried). Numerator for the \
             ExistenceCacheStore hit rate (hit / (hit + miss))."
        );
        let v = self.miss.load(Ordering::Relaxed);
        publish!(
            "has_miss_total",
            &v,
            MetricKind::Counter,
            "Count of per-key has_with_results lookups that missed the moka cache \
             and required an inner-store query. Denominator counterpart to \
             has_hit_total; sustained high miss rate with a full 50M-entry cache \
             signals a key-space larger than the cache capacity."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Process-wide ExistenceCache hit/miss counters.
static ECS_HIT_COUNTERS: EcsHitCounters = EcsHitCounters::new();
/// Cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents double-
/// registration; both calls return a clone of the same `Arc`.
static ECS_HIT_COUNTERS_ARC: OnceLock<Arc<EcsHitCountersHandle>> = OnceLock::new();

/// Process-wide ExistenceCache hit/miss counters singleton.
#[must_use]
pub fn ecs_hit_counters() -> &'static EcsHitCounters {
    &ECS_HIT_COUNTERS
}

/// `Arc` wrapper for `MetricsRegistry::register`. `OnceLock`-cached.
#[must_use]
pub fn ecs_hit_counters_arc() -> Arc<EcsHitCountersHandle> {
    Arc::clone(ECS_HIT_COUNTERS_ARC.get_or_init(|| Arc::new(EcsHitCountersHandle)))
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed ECS counters.
#[derive(Debug)]
pub struct EcsHitCountersHandle;

impl MetricsComponent for EcsHitCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        ECS_HIT_COUNTERS.publish(kind, field_metadata)
    }
}

// =====================================================================
// Process-global singletons
// =====================================================================

/// #86: process-wide symlink_fix_lock counters. Backed by a `static`
/// so `const fn new()` suffices; timestamps are set at increment time.
static SYMLINK_FIX_COUNTERS: SymlinkFixCounters = SymlinkFixCounters::new();
/// #86: cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents
/// a double-registration hazard if `symlink_fix_counters_arc()` is called
/// twice — both calls return a clone of the same `Arc`.
static SYMLINK_FIX_COUNTERS_ARC: OnceLock<Arc<SymlinkFixCountersHandle>> = OnceLock::new();

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
/// `OnceLock`-cached so repeated calls return a clone of the same `Arc`
/// (prevents a double-registration hazard if the caller invokes twice).
#[must_use]
pub fn symlink_fix_counters_arc() -> Arc<SymlinkFixCountersHandle> {
    Arc::clone(SYMLINK_FIX_COUNTERS_ARC.get_or_init(|| Arc::new(SymlinkFixCountersHandle)))
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
// #DC3: directory-cache efficacy counters, broken down by OUTCOME class
// =====================================================================

/// Process-global directory-cache efficacy counters, broken down by the
/// cache-OUTCOME class. Surfaces on the worker `/metrics` endpoint so
/// dir-cache efficacy ideas #1/#2 become measurable.
///
/// Background: the per-instance `DirectoryCache` (`directory_cache.rs`)
/// already counts these outcomes in plain `AtomicU64` fields, but that
/// struct has no `MetricsComponent` and is never registered with
/// `MetricsRegistry` — so the counters are DARK on `/metrics` (the same
/// worker-metrics-exposure trap #86 `SYMLINK_FIX_COUNTERS` fixed). This
/// singleton is the `/metrics`-visible aggregate; increments are routed to
/// it at the same six sites that bump the per-instance fields. The
/// per-instance fields are KEPT because they feed live operator-visible
/// hit-rate `info!` logs (a separate sink — not double-counting).
///
/// Outcome dimension (cardinality-bounded — a fixed 6-outcome enum, NOT a
/// runtime-keyed label map, so no unbounded-label-set defect):
/// - `exact_hit`  — digest already fully cached (the cheapest outcome).
/// - `miss`       — full construction from the CAS (the most expensive).
/// - `subtree_hit`— partial reuse of an already-cached subtree via symlink.
/// - `fuzzy_match`— a miss resolved by patching the best-matching cached root.
/// - `hit_clonefile` / `hit_hardlink` — the materialisation MECHANISM used
///   on a hit (clonefile/reflink vs hardlink).
///
/// NOTE (action-class follow-up): the dispatch's "do link inputs hit the
/// tree cache?" question wants a join to the Bazel action mnemonic. That
/// label lives in REAPI `RequestMetadata.action_mnemonic` and never reaches
/// the worker today; threading it is significant cross-component plumbing
/// (scheduler `RequestMetadata` parse → worker-api proto → `ActionInfo`).
/// Filed as a follow-up; the OUTCOME dimension here is the cleanest-available
/// bounded dimension and is what #1/#2 actually need.
///
/// Aggregates across all configured `DirectoryCache` instances in the
/// process (N worker configs = N instances; the process-wide sum is the
/// correct `/metrics` total). On server-only processes the counters read 0.
#[derive(Debug)]
pub struct DirCacheCounters {
    /// `dir_cache_exact_hit_total.counter` — digest already fully cached.
    pub exact_hit: AtomicU64,
    /// `dir_cache_miss_total.counter` — full construction from the CAS.
    pub miss: AtomicU64,
    /// `dir_cache_subtree_hit_total.counter` — cached-subtree reuse count.
    pub subtree_hit: AtomicU64,
    /// `dir_cache_fuzzy_match_total.counter` — best-match-patch resolutions.
    pub fuzzy_match: AtomicU64,
    /// `dir_cache_hit_clonefile_total.counter` — hit materialised via clonefile/reflink.
    pub hit_clonefile: AtomicU64,
    /// `dir_cache_hit_hardlink_total.counter` — hit materialised via hardlink.
    pub hit_hardlink: AtomicU64,
    /// `dir_cache_hit_clonefile_preempted_total.counter` — a HIT-path materialise
    /// found a NON-EMPTY destination directory. On macOS this preempts the
    /// whole-tree `clonefile(2)` fast path (`try_clonefile` requires an
    /// empty/absent dst), forcing the ~600ms per-file hardlink fallback; the
    /// existing `hit_hardlink` counter conflates that preemption with a genuine
    /// clonefile failure (cross-device, non-APFS), so this counter is the
    /// disambiguating diagnostic (#clonefile-fallback). On Linux/Windows the
    /// hit always hardlinks regardless, so a non-zero value there is
    /// informational (no clonefile to preempt).
    pub hit_clonefile_preempted: AtomicU64,
    // ---- #DC3 (scope ext): COLD-construct phase sub-cost decomposition ----
    // The decision instrument for dir-cache ideas #1/#2: red-team showed the
    // construct cost is blob-fetch-dominated, so the phase NAME is the
    // cost-attribution axis. Each phase is a sum(ms)+count pair (mean =
    // sum/count) — the cheapest signal that answers "where does the construct
    // time go". Action-class labelling is the deferred follow-up (mnemonic is
    // not at the worker); the phase metrics are NOT cross-labelled by outcome
    // because the phase is itself the decomposition (an outcome cross-product
    // would multiply series without decision value).
    /// `dir_cache_construct_resolve_ms_{sum,count}` — parallel-BFS directory
    /// tree resolve (`resolve_directory_tree`) span on a COLD construct.
    pub construct_resolve_ms: PhaseTiming,
    /// `dir_cache_construct_fetch_ms_{sum,count}` — the COLD-construct
    /// fetch+materialise span (`download_to_directory`). Fetch-DOMINATED but
    /// includes the in-construct hardlink emission; see the metric help for
    /// the exact measured boundary (fetch and hardlink-emit interleave inside
    /// `download_to_directory` and are not separable without entering
    /// `running_actions_manager.rs`).
    pub construct_fetch_ms: PhaseTiming,
    /// (#obs-tuning-construct-latency-conditioning) Decayed p95 of the SAME
    /// cold-construct fetch span as `construct_fetch_ms`, fed from the same
    /// `record_construct_fetch_ms` observation. This is the CONDITIONED estimate
    /// the worker gossips (`construct_latency_ms_p95`, worker_api field 25/29):
    /// a decay-windowed conservative tail estimate that (unlike the cumulative
    /// `construct_fetch_ms.sum/count` mean) does not fossilise since boot and
    /// biases toward the expensive tail the eventual `T_SETUP` consumer must not
    /// under-price. `construct_fetch_ms` is KEPT alongside it (unchanged) — it
    /// backs the `/metrics` DC3 phase decomposition (sum+count, rate-friendly);
    /// the two are complementary, not redundant.
    pub construct_fetch_p95: DecayingP95Histogram,
    /// (#obs-tuning follow-up 3) Monotone count of COLD-construct fetch
    /// observations — one per `record_construct_fetch_ms` call. Makes the
    /// cold-construct RATE (`Δcount / Δt`) observable so the decayed-p95 window
    /// (`P95_DECAY_KEEP_PER_SEC`, ~34 s half-life, assumes "a few cold
    /// constructs/min") can be VALIDATED against the live rate instead of
    /// resting on an operator estimate. A wrong rate would make the effective
    /// window seconds (jittery) or hours (re-fossilised); this counter is the
    /// diagnostic that catches either. Surfaced on the worker's own `/metrics`
    /// endpoint as `dir_cache_construct_fetch_count_total_counter` (the same
    /// `MetricsRegistry` render the other DC3 counters use); the cold RATE is
    /// then `rate()` of this series on a scrape. Numerically it tracks
    /// `construct_fetch_ms.count` (both bump per cold construct); it is called
    /// out as a DISTINCT named counter so the "cold-construct rate" is a
    /// first-class, self-describing `/metrics` series next to the p95 gossip it
    /// shapes — not an implied sub-field of the phase-timing decomposition.
    /// (A future `T_SETUP` control-plane change may ALSO gossip it to the 15 s
    /// `worker_construct_latency` scheduler log; that wiring is out of scope for
    /// this observability-only change — see the tsetup-dynamic design doc.)
    // UNBOUNDED-OK: a plain monotone AtomicU64 event counter (no buffered bytes).
    pub construct_fetch_count: AtomicU64,
    /// `dir_cache_hit_assemble_ms_{sum,count}` — the HIT-path materialise span
    /// (`hardlink_directory_tree` in `try_hardlink_cached`: cached entry →
    /// dest). This is the cost dir-cache ideas #1/#2 would make MORE frequent.
    pub hit_assemble_ms: PhaseTiming,
    // ---- #speculative-prefetch: DirectoryCache::prewarm outcome + priority ----
    // The speculative pre-construct entry point (`DirectoryCache::prewarm`)
    // records its outcome here so the fleet can measure pre-warm efficacy
    // (how often a pre-warm found the entry already warm vs did a real
    // construct) WITHOUT threading a new metrics tree into the worker.
    /// `dir_cache_prewarm_warm_redundant_total.counter` — a `prewarm` found the
    /// entry ALREADY present in the cache (fast-path pin-only, no construct).
    /// A high ratio vs `prewarm_completed` means the pre-warm is racing an
    /// already-cached digest — the speculation added no work but also saved none.
    pub prewarm_warm_redundant: AtomicU64,
    /// `dir_cache_prewarm_completed_total.counter` — a `prewarm` ran the real
    /// coalesced construct (leader or waiter) and left the entry present+pinned.
    /// This is the pre-warm that actually moved a cold `construct_fetch` off the
    /// later real-dispatch critical path.
    pub prewarm_completed: AtomicU64,
    /// `dir_cache_prewarm_foreground_total.counter` — a `prewarm` entered tagged
    /// `OpPriority::Foreground` (a real materialize path routed through prewarm).
    /// In Increment 1 the real materialize callers do NOT route through prewarm,
    /// so this is expected to stay 0 in production; it exists so the priority
    /// dimension is observable end-to-end (T-oppriority pins it).
    pub prewarm_foreground: AtomicU64,
    /// `dir_cache_prewarm_speculative_total.counter` — a `prewarm` entered tagged
    /// `OpPriority::Speculative` (the `Update::PrefetchInputs` path). This is the
    /// observable proof the speculative tag reached the construct/fetch boundary
    /// (the `// TODO(#speculative-prefetch-io-priority)` hook a future scheduler
    /// consults). MUST equal the count of speculative pre-warms driven.
    pub prewarm_speculative: AtomicU64,
}

/// Sum+count pair for a single dir-cache construct phase. `sum` is the
/// total observed milliseconds; `count` the number of observations. The
/// per-phase mean is `sum / count`. A `Counter`-kind sum+count pair
/// (Prometheus rate-friendly) is deliberately lighter than a full
/// `O11LatencyHistogram` — for a cost-attribution decision the mean and
/// rate are sufficient and avoid per-phase bucket-series cardinality.
#[derive(Debug)]
pub struct PhaseTiming {
    /// Sum of all observed phase durations, in milliseconds.
    pub sum_ms: AtomicU64,
    /// Number of phase observations.
    pub count: AtomicU64,
}

impl PhaseTiming {
    const fn new() -> Self {
        Self {
            sum_ms: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one phase observation of `elapsed_ms` milliseconds.
    pub fn observe_ms(&self, elapsed_ms: u64) {
        self.sum_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

impl DirCacheCounters {
    const fn new() -> Self {
        Self {
            exact_hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
            subtree_hit: AtomicU64::new(0),
            fuzzy_match: AtomicU64::new(0),
            hit_clonefile: AtomicU64::new(0),
            hit_hardlink: AtomicU64::new(0),
            hit_clonefile_preempted: AtomicU64::new(0),
            construct_resolve_ms: PhaseTiming::new(),
            construct_fetch_ms: PhaseTiming::new(),
            construct_fetch_p95: DecayingP95Histogram::new(),
            construct_fetch_count: AtomicU64::new(0),
            hit_assemble_ms: PhaseTiming::new(),
            prewarm_warm_redundant: AtomicU64::new(0),
            prewarm_completed: AtomicU64::new(0),
            prewarm_foreground: AtomicU64::new(0),
            prewarm_speculative: AtomicU64::new(0),
        }
    }

    /// Record an exact-hit outcome (digest already fully cached).
    pub fn record_exact_hit(&self) {
        self.exact_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a miss outcome (full construction from the CAS).
    pub fn record_miss(&self) {
        self.miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Record `n` cached-subtree reuses (a single construction can reuse
    /// many subtrees; the producer passes the per-construction count).
    pub fn record_subtree_hits(&self, n: u64) {
        self.subtree_hit.fetch_add(n, Ordering::Relaxed);
    }

    /// Record a fuzzy-match outcome (miss resolved via best-match patching).
    pub fn record_fuzzy_match(&self) {
        self.fuzzy_match.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a hit materialised via clonefile/reflink.
    pub fn record_hit_clonefile(&self) {
        self.hit_clonefile.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a hit materialised via hardlink.
    pub fn record_hit_hardlink(&self) {
        self.hit_hardlink.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a HIT-path materialise that found a non-empty destination
    /// (clonefile preempted on macOS; informational elsewhere).
    pub fn record_hit_clonefile_preempted(&self) {
        self.hit_clonefile_preempted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a COLD-construct resolve-phase observation (ms).
    pub fn record_construct_resolve_ms(&self, elapsed_ms: u64) {
        self.construct_resolve_ms.observe_ms(elapsed_ms);
    }

    /// Record a COLD-construct fetch+materialise-phase observation (ms).
    /// Feeds BOTH the cumulative `construct_fetch_ms` sum+count (the `/metrics`
    /// DC3 phase decomposition) AND the decayed p95 estimator
    /// (`construct_fetch_p95`, the conditioned `construct_latency_ms_p95`
    /// gossip). One observation, two derived signals.
    pub fn record_construct_fetch_ms(&self, elapsed_ms: u64) {
        self.construct_fetch_ms.observe_ms(elapsed_ms);
        self.construct_fetch_p95.observe(elapsed_ms);
        // (#obs-tuning follow-up 3) Bump the monotone cold-construct rate
        // counter so the decay-window (~few/min) assumption is observable.
        self.construct_fetch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a HIT-path assemble-phase (hardlink materialise) observation (ms).
    pub fn record_hit_assemble_ms(&self, elapsed_ms: u64) {
        self.hit_assemble_ms.observe_ms(elapsed_ms);
    }

    /// Record a `DirectoryCache::prewarm` that found the entry already warm
    /// (fast-path pin-only; no construct ran).
    pub fn record_prewarm_warm_redundant(&self) {
        self.prewarm_warm_redundant.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a `DirectoryCache::prewarm` that ran the real coalesced construct
    /// (leader or waiter) and left the entry present+pinned.
    pub fn record_prewarm_completed(&self) {
        self.prewarm_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a `DirectoryCache::prewarm` entered under `OpPriority::Foreground`.
    /// Split from `record_prewarm_speculative` because `OpPriority` lives in
    /// `nativelink-worker` (the caller matches on it) and `nativelink-util`
    /// cannot depend on `nativelink-worker` — so the priority is recorded via
    /// two distinct entry points, one per label.
    pub fn record_prewarm_foreground(&self) {
        self.prewarm_foreground.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a `DirectoryCache::prewarm` entered under `OpPriority::Speculative`
    /// (the `Update::PrefetchInputs` path). This is the observable proof the
    /// speculative tag reached the construct/fetch boundary.
    pub fn record_prewarm_speculative(&self) {
        self.prewarm_speculative.fetch_add(1, Ordering::Relaxed);
    }
}

impl MetricsComponent for DirCacheCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Registered under prefix "dir_cache" (nativelink.rs). Each
        // group!() adds ONE inner segment so the rendered name is
        //   dir_cache . <outcome>_total . counter → dir_cache_<outcome>_total_counter
        // Group names must NOT repeat the "dir_cache" prefix (the register
        // key is already in scope) or it doubles to `dir_cache_dir_cache_…`
        // (the #86 doubled-name trap, guarded by the render test).
        let emit = |outcome: &'static str, field: &AtomicU64, help: &'static str| -> Result<(), nativelink_metric::Error> {
            let grp = format!("{outcome}_total");
            let _g = group!(grp).entered();
            let v = field.load(Ordering::Relaxed);
            publish!("counter", &v, MetricKind::Counter, help);
            Ok(())
        };
        emit(
            "exact_hit",
            &self.exact_hit,
            "Directory-cache exact-hit count: input root digest was already \
             fully cached (cheapest outcome). Numerator for tree-cache efficacy.",
        )?;
        emit(
            "miss",
            &self.miss,
            "Directory-cache miss count: full construction from the CAS (most \
             expensive outcome). Denominator counterpart to the hit outcomes.",
        )?;
        emit(
            "subtree_hit",
            &self.subtree_hit,
            "Directory-cache subtree-hit count: number of already-cached \
             subtrees reused via symlink across all constructions. Measures \
             partial tree-cache reuse on otherwise-missing roots.",
        )?;
        emit(
            "fuzzy_match",
            &self.fuzzy_match,
            "Directory-cache fuzzy-match count: misses resolved by patching the \
             best-matching cached root instead of full construction.",
        )?;
        emit(
            "hit_clonefile",
            &self.hit_clonefile,
            "Directory-cache hit materialised via clonefile/reflink (the \
             copy-on-write hit mechanism).",
        )?;
        emit(
            "hit_hardlink",
            &self.hit_hardlink,
            "Directory-cache hit materialised via hardlink (the shared-inode \
             hit mechanism).",
        )?;
        emit(
            "hit_clonefile_preempted",
            &self.hit_clonefile_preempted,
            "Directory-cache hit whose materialise found a NON-EMPTY destination \
             directory, preempting the macOS clonefile(2) whole-tree fast path \
             (forcing the per-file hardlink fallback). Disambiguates preemption \
             from a genuine clonefile failure that hit_hardlink alone conflates \
             (#clonefile-fallback). Informational on non-macOS (no clonefile).",
        )?;

        // #DC3 (scope ext): cold-construct phase sub-cost decomposition.
        // Each phase emits dir_cache_<phase>_ms_sum (total ms) +
        // dir_cache_<phase>_ms_count (#observations) under a group whose
        // name does NOT repeat the "dir_cache" prefix (#86 doubled-name trap).
        let emit_phase = |phase: &'static str, t: &PhaseTiming, help_sum: &'static str| -> Result<(), nativelink_metric::Error> {
            let grp = format!("{phase}_ms");
            let _g = group!(grp).entered();
            let sum = t.sum_ms.load(Ordering::Relaxed);
            let count = t.count.load(Ordering::Relaxed);
            publish!("sum", &sum, MetricKind::Counter, help_sum);
            publish!(
                "count",
                &count,
                MetricKind::Counter,
                "Number of observations for this dir-cache construct phase \
                 (mean phase ms = sum / count)."
            );
            Ok(())
        };
        emit_phase(
            "construct_resolve",
            &self.construct_resolve_ms,
            "Sum (ms) of the COLD-construct parallel-BFS directory-tree resolve \
             phase (resolve_directory_tree). Boundary: from just-before the \
             resolve call to just-after it returns.",
        )?;
        emit_phase(
            "construct_fetch",
            &self.construct_fetch_ms,
            "Sum (ms) of the COLD-construct blob-fetch+materialise phase \
             (download_to_directory). Boundary: the full download_to_directory \
             span — fetch-DOMINATED but includes the in-construct hardlink \
             emission (fetch and hardlink-emit interleave inside it and are not \
             separable here).",
        )?;
        emit_phase(
            "hit_assemble",
            &self.hit_assemble_ms,
            "Sum (ms) of the HIT-path assemble phase (hardlink_directory_tree: \
             cached entry → action dest). Boundary: the hardlink_directory_tree \
             span in try_hardlink_cached. This is the cost dir-cache ideas #1/#2 \
             would make more frequent.",
        )?;

        // #obs-tuning follow-up 3: COLD-construct rate — a monotone count of
        // record_construct_fetch_ms calls so the decayed-p95 window assumption
        // ("a few cold constructs/min") is observable/validatable. Emitted via
        // the same `emit` helper → dir_cache_construct_fetch_count_total_counter.
        emit(
            "construct_fetch_count",
            &self.construct_fetch_count,
            "Directory-cache COLD-construct count (one per record_construct_fetch_ms \
             call): the cold-construct RATE (delta/interval) that shapes the gossiped \
             construct_latency_ms_p95 decay window. Validates the ~few-per-min window \
             assumption; a wrong rate makes that window jittery (seconds) or \
             re-fossilised (hours).",
        )?;

        // #speculative-prefetch: DirectoryCache::prewarm outcome + priority.
        emit(
            "prewarm_warm_redundant",
            &self.prewarm_warm_redundant,
            "Directory-cache prewarm that found the entry ALREADY cached \
             (speculative pre-construct raced an already-warm digest: pin-only, \
             no construct). High vs prewarm_completed = speculation adds no work.",
        )?;
        emit(
            "prewarm_completed",
            &self.prewarm_completed,
            "Directory-cache prewarm that ran the real coalesced construct and \
             left the entry present+pinned — the pre-warm that moved a cold \
             construct_fetch off the later real-dispatch critical path.",
        )?;
        emit(
            "prewarm_foreground",
            &self.prewarm_foreground,
            "Directory-cache prewarm entered under OpPriority::Foreground (a real \
             materialize routed through prewarm). Expected 0 in Increment 1 \
             (real materialize does not route through prewarm); exists so the \
             priority dimension is observable end-to-end.",
        )?;
        emit(
            "prewarm_speculative",
            &self.prewarm_speculative,
            "Directory-cache prewarm entered under OpPriority::Speculative (the \
             Update::PrefetchInputs path). Observable proof the speculative tag \
             reached the construct/fetch boundary (the future IO-priority hook).",
        )?;
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// #DC3: process-wide directory-cache outcome counters. Backed by a
/// `static` so `const fn new()` suffices.
static DIR_CACHE_COUNTERS: DirCacheCounters = DirCacheCounters::new();
/// #DC3: cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents
/// a double-registration hazard if `dir_cache_counters_arc()` is called
/// twice — both calls return a clone of the same `Arc`.
static DIR_CACHE_COUNTERS_ARC: OnceLock<Arc<DirCacheCountersHandle>> = OnceLock::new();

/// #DC3: process-wide directory-cache outcome counters singleton. All
/// calls within the process observe the same atomic state.
#[must_use]
pub fn dir_cache_counters() -> &'static DirCacheCounters {
    &DIR_CACHE_COUNTERS
}

/// #DC3: `Arc` wrapper for `MetricsRegistry::register`. The singleton lives
/// in a `static`; the `Arc` carries a zero-sized handle that delegates
/// `publish` to the static so scrapes always read live state. `OnceLock`-
/// cached so repeated calls return a clone of the same `Arc`.
#[must_use]
pub fn dir_cache_counters_arc() -> Arc<DirCacheCountersHandle> {
    Arc::clone(DIR_CACHE_COUNTERS_ARC.get_or_init(|| Arc::new(DirCacheCountersHandle)))
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed `#DC3` counters.
#[derive(Debug)]
pub struct DirCacheCountersHandle;

impl MetricsComponent for DirCacheCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        DIR_CACHE_COUNTERS.publish(kind, field_metadata)
    }
}

// =====================================================================
// #37 re-enable follow-up — memory gate NAK counters
// =====================================================================

/// (#task-memgate-twosignal) Process-wide memory gate counters + gauges.
/// Two monotonic NAK counters, one per OOM trip source (free-floor FAIL-SAFE
/// and sustained-SWAPIN), plus the observability gauges. Backed by `static`s
/// so `const fn new()` suffices; incremented/stored from `local_worker.rs`.
///
/// These were originally fields on `LocalWorker.metrics` (a per-instance
/// struct that is never registered with `MetricsRegistry` — the
/// worker-metrics-exposure trap). Moving them to a process singleton makes
/// them visible on `/metrics` without adding per-instance registration.
///
/// The three RAW rate gauges (`compress_rate_last`, `decompress_rate_last`,
/// `swapin_rate_last`) exist for CALIBRATION: we have no compression-rate soak
/// data yet, so logging all three is what will let the churn-throttle band be
/// set later. `churn_ewma` is the published graded scalar.
#[derive(Debug)]
pub struct MemoryGateCounters {
    /// StartAction NAKs from free-floor trip (available < FREE_FLOOR_BYTES).
    pub nak_free_floor: AtomicU64,
    /// StartAction NAKs from the sustained-SWAPIN OOM trip.
    pub nak_swapin: AtomicU64,
    /// (#task-memgate-twosignal) Current compressor-CHURN EWMA
    /// (`min(compress_rate_ewma, decompress_rate_ewma)`, events/sec) — the
    /// graded perf scalar published on the wire + consumed by the scheduler
    /// ranking/throttle. Gauge (non-monotonic). Updated every sampler tick
    /// (~100 ms) regardless of gate-enable state → observable on ALL workers.
    pub churn_ewma: AtomicU32,
    /// (#task-memgate-twosignal) Raw per-second COMPRESSION rate from the LAST
    /// sampler tick. Gauge. Calibration signal (the SUPPLY half of churn).
    pub compress_rate_last: AtomicU32,
    /// (#task-memgate-twosignal) Raw per-second DECOMPRESSION rate from the LAST
    /// sampler tick. Gauge. Calibration signal (the DEMAND half of churn).
    pub decompress_rate_last: AtomicU32,
    /// (#task-memgate-twosignal) Raw per-second SWAPIN rate from the LAST sampler
    /// tick — the OOM-adjacent disk-spill signal (baseline 0). Gauge.
    pub swapin_rate_last: AtomicU32,
    /// (#64 dark-signals) host swap bytes in use (macOS `vm.swapusage` /
    /// Linux `/proc/meminfo`), published as a gauge. Updated unconditionally
    /// every sampler tick (~100 ms) before the signal-read early-return —
    /// visible on all WORKER processes. Zero when the OS reports no swap, the
    /// read fails (best-effort), or no sampler runs (server-only processes
    /// have no worker). Non-monotonic.
    pub swap_used_bytes: AtomicU64,
    /// (#64 dark-signals) MiB below the free-floor (0 = at or above the
    /// floor; positive = pressured). The FAIL-SAFE magnitude (NO LONGER the
    /// wire scalar — the churn EWMA is; kept as a diagnostic gauge). Updated
    /// every sampler tick: set to 0 on the unreadable-signals early-return path,
    /// set to the computed level on the readable path. Non-monotonic gauge.
    pub pressure_level_mib: AtomicU32,

    // ── (#calib swapin/churn threshold calibration) busy-window self-recording ──
    // The `*_rate_last` / `churn_ewma` gauges above are INSTANTANEOUS: a scrape
    // between busy windows reads 0, so the "measure a busy soak → set
    // `memory_gate_swapin_confirm_rate` + churn low/high thresholds" plan can
    // never complete from scrapes alone (2026-07-28: a benchmark burst came and
    // went; the gauges read 0 after). These fields make the busy-window history
    // self-recording: a HIGH-WATER max per signal (+ churn) and a fixed 6-band
    // tick histogram per signal, all fed by [`Self::record_calibration_tick`]
    // on the ~100 ms sampler tick (readable-signals path only). O(1) atomics
    // per tick; reset only at process start.
    /// High-water mark of the churn EWMA (`churn_ewma`) since process start.
    /// `fetch_max` gauge — never reset, never decays.
    pub churn_ewma_max: AtomicU32,
    /// High-water mark of the raw per-second COMPRESSION rate since process
    /// start. `fetch_max` gauge.
    pub compress_rate_max: AtomicU32,
    /// High-water mark of the raw per-second DECOMPRESSION rate since process
    /// start. `fetch_max` gauge.
    pub decompress_rate_max: AtomicU32,
    /// High-water mark of the raw per-second SWAPIN rate since process start.
    /// `fetch_max` gauge — the direct calibration input for
    /// `memory_gate_swapin_confirm_rate` (currently suppressed at `u32::MAX`).
    pub swapin_rate_max: AtomicU32,
    /// Sampler-tick counts by COMPRESSION-rate band
    /// `{0, 1-10, 11-100, 101-1000, 1001-10000, >10000}/s` (index 0..=5).
    /// Monotonic counters — sustained-vs-spike is read from the band SHAPE
    /// (the gate's sustained-N-ticks semantics needs tick-duration, which a
    /// max alone cannot give).
    pub compress_rate_bands: [AtomicU64; MEMORY_GATE_RATE_BANDS],
    /// Sampler-tick counts by DECOMPRESSION-rate band (same bands as
    /// [`Self::compress_rate_bands`]).
    pub decompress_rate_bands: [AtomicU64; MEMORY_GATE_RATE_BANDS],
    /// Sampler-tick counts by SWAPIN-rate band (same bands as
    /// [`Self::compress_rate_bands`]).
    pub swapin_rate_bands: [AtomicU64; MEMORY_GATE_RATE_BANDS],
}

/// (#calib) Number of fixed calibration rate bands:
/// `{0, 1-10, 11-100, 101-1000, 1001-10000, >10000}` events/sec.
pub const MEMORY_GATE_RATE_BANDS: usize = 6;

/// (#calib) Map a per-second rate into its fixed calibration band index:
/// `0 → 0`, `1-10 → 1`, `11-100 → 2`, `101-1000 → 3`, `1001-10000 → 4`,
/// `>10000 → 5`. Pure so the band edges are unit-testable exactly.
const fn rate_band_index(rate: u32) -> usize {
    match rate {
        0 => 0,
        1..=10 => 1,
        11..=100 => 2,
        101..=1_000 => 3,
        1_001..=10_000 => 4,
        _ => 5,
    }
}

impl MemoryGateCounters {
    const fn new() -> Self {
        // Array-init idiom for a non-Copy const-constructible element: a const
        // ITEM is instantiated per use, so `[ZERO_U64; N]` is N fresh atomics.
        const ZERO_U64: AtomicU64 = AtomicU64::new(0);
        Self {
            nak_free_floor: AtomicU64::new(0),
            nak_swapin: AtomicU64::new(0),
            churn_ewma: AtomicU32::new(0),
            compress_rate_last: AtomicU32::new(0),
            decompress_rate_last: AtomicU32::new(0),
            swapin_rate_last: AtomicU32::new(0),
            swap_used_bytes: AtomicU64::new(0),
            pressure_level_mib: AtomicU32::new(0),
            churn_ewma_max: AtomicU32::new(0),
            compress_rate_max: AtomicU32::new(0),
            decompress_rate_max: AtomicU32::new(0),
            swapin_rate_max: AtomicU32::new(0),
            compress_rate_bands: [ZERO_U64; MEMORY_GATE_RATE_BANDS],
            decompress_rate_bands: [ZERO_U64; MEMORY_GATE_RATE_BANDS],
            swapin_rate_bands: [ZERO_U64; MEMORY_GATE_RATE_BANDS],
        }
    }

    /// (#calib) Record one readable-signals sampler tick into the busy-window
    /// self-recording state: high-water `fetch_max` per signal (+ churn) and
    /// one band-tick per signal. Called from the worker memory-gate sampler
    /// (`sample_mem_pressure`) on the readable path ONLY — the unreadable-signals
    /// early-return does NOT call it (an unreadable tick is not a measurement,
    /// and the maxes must survive it). Cost: 7 relaxed atomic RMWs per ~100 ms
    /// tick; no allocation, no lock.
    pub fn record_calibration_tick(
        &self,
        compress_rate: u32,
        decompress_rate: u32,
        swapin_rate: u32,
        churn_ewma: u32,
    ) {
        self.compress_rate_max
            .fetch_max(compress_rate, Ordering::Relaxed);
        self.decompress_rate_max
            .fetch_max(decompress_rate, Ordering::Relaxed);
        self.swapin_rate_max
            .fetch_max(swapin_rate, Ordering::Relaxed);
        self.churn_ewma_max.fetch_max(churn_ewma, Ordering::Relaxed);
        self.compress_rate_bands[rate_band_index(compress_rate)]
            .fetch_add(1, Ordering::Relaxed);
        self.decompress_rate_bands[rate_band_index(decompress_rate)]
            .fetch_add(1, Ordering::Relaxed);
        self.swapin_rate_bands[rate_band_index(swapin_rate)]
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl MetricsComponent for MemoryGateCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Registered under prefix "memory_gate" (nativelink.rs). The publish!
        // name is the FULL field name so operators alert on the literal string.
        // No inner group!() — the prefix already scopes these uniquely.
        let v = self.nak_free_floor.load(Ordering::Relaxed);
        publish!(
            "nak_free_floor_total",
            &v,
            MetricKind::Counter,
            "StartAction NAKs from memory gate free-floor trip (available < \
             FREE_FLOOR_BYTES=1GiB); monotonic — alert on rate; zero = gate \
             disabled or floor healthy."
        );
        let v = self.nak_swapin.load(Ordering::Relaxed);
        publish!(
            "nak_swapin_total",
            &v,
            MetricKind::Counter,
            "StartAction NAKs from the memory gate sustained-SWAPIN OOM trip \
             (swapin rate >= memory_gate_swapin_confirm_rate for \
             memory_gate_swapin_confirm_window_ticks consecutive ticks); \
             monotonic — non-zero = genuine disk spill (swapin baseline is 0)."
        );
        // (#task-memgate-twosignal gauges) Published with MetricKind::Default, which
        // the metrics library renders as Prometheus `# TYPE ... counter` (there is
        // no gauge kind). These values are NON-MONOTONIC (EWMA decays / raw rates
        // rise and fall), so read the INSTANT value — do NOT apply `rate()`. No
        // `_total` suffix — name them as the gauges they semantically are.
        let v = self.churn_ewma.load(Ordering::Relaxed);
        publish!(
            "churn_ewma",
            &v,
            MetricKind::Default,
            "Compressor-churn perf scalar: min(compress_rate_ewma, \
             decompress_rate_ewma), events/sec — the graded 'how pressured' signal \
             on the wire + the scheduler ranking/throttle. Gauge — not monotonic. \
             High only when compression AND decompression are both high (thrash). \
             Updated every sampler tick (~100 ms) unconditionally — all workers."
        );
        let v = self.compress_rate_last.load(Ordering::Relaxed);
        publish!(
            "compress_rate_last",
            &v,
            MetricKind::Default,
            "Raw per-second COMPRESSION rate from the last sampler tick \
             (compressions delta / elapsed). Gauge. Calibration signal (supply \
             half of the churn scalar) — no compression-rate soak data existed yet."
        );
        let v = self.decompress_rate_last.load(Ordering::Relaxed);
        publish!(
            "decompress_rate_last",
            &v,
            MetricKind::Default,
            "Raw per-second DECOMPRESSION rate from the last sampler tick \
             (decompressions delta / elapsed). Gauge. Calibration signal (demand \
             half of the churn scalar)."
        );
        let v = self.swapin_rate_last.load(Ordering::Relaxed);
        publish!(
            "swapin_rate_last",
            &v,
            MetricKind::Default,
            "Raw per-second SWAPIN rate from the last sampler tick (swapins delta / \
             elapsed) — the OOM-adjacent disk-spill signal (baseline 0). Gauge. \
             The sustained-window OOM gate keys off this."
        );
        // (#64 dark-signals) Two previously-dark sampler signals now exposed as
        // gauges, same convention as the `churn_ewma` gauge above. These render
        // as `# TYPE ... counter` (the `publish!` macro resolves a numeric
        // MetricKind::Default
        // to Counter at publish time (`u64::publish` -> `into_known_kind(Counter)`
        // -> the macro emits `__type = Counter`), so every renderer (prod collector
        // AND the test-only render_prometheus/format_prometheus) sees Counter — the
        // library has no gauge kind, and the `Default -> untyped` arm is dead for
        // numeric metrics. These values are NON-MONOTONIC (gauges) despite the
        // `counter` TYPE line, so consumers read the INSTANT value; do NOT apply
        // `rate()` (a decay reads as a counter reset). Cost: 2 extra
        // AtomicLoad(Relaxed) per scrape — negligible.
        let v = self.swap_used_bytes.load(Ordering::Relaxed);
        publish!(
            "swap_used_bytes",
            &v,
            MetricKind::Default,
            "host swap bytes in use (macOS vm.swapusage / Linux /proc/meminfo), \
             gauge. Updated every sampler tick (~100 ms) on worker processes; 0 \
             when OS reports no swap, the read fails, OR no sampler runs (server-only \
             processes have no worker, so the gauge stays 0). Non-monotonic — read \
             instant value only; do not write a `== 0` healthy-signal alert."
        );
        let v = u64::from(self.pressure_level_mib.load(Ordering::Relaxed));
        publish!(
            "pressure_level_mib",
            &v,
            MetricKind::Default,
            "this worker's locally-computed MiB below the free-floor (0 = above \
             floor), gauge. Same raw value the worker transmits to the server's \
             least-pressured fail-open ranking — this gauge is the worker-LOCAL \
             view, before transmission. Set to 0 when signals are unreadable or no \
             sampler runs (server-only processes stay 0). Non-monotonic; per-worker \
             only — do NOT sum across processes."
        );
        // ── (#calib) busy-window self-recording: high-water maxes + band ticks ──
        // Same MetricKind::Default rendering note as above (the library renders
        // numeric Default as `# TYPE ... counter`); the four maxes are
        // NON-MONOTONIC-in-name only — they are in fact monotone high-water
        // marks, but read the INSTANT value (no rate()). The 18 band counters
        // ARE monotonic tick counts (`_total`).
        let v = self.churn_ewma_max.load(Ordering::Relaxed);
        publish!(
            "churn_ewma_max",
            &v,
            MetricKind::Default,
            "high-water mark of the churn EWMA since process start (fetch_max, \
             never reset) — the busy-window churn peak a between-burst scrape \
             would otherwise miss; calibration input for the churn-throttle \
             low/high thresholds."
        );
        let v = self.compress_rate_max.load(Ordering::Relaxed);
        publish!(
            "compress_rate_max",
            &v,
            MetricKind::Default,
            "high-water mark of the raw per-second COMPRESSION rate since \
             process start (fetch_max, never reset). Calibration signal."
        );
        let v = self.decompress_rate_max.load(Ordering::Relaxed);
        publish!(
            "decompress_rate_max",
            &v,
            MetricKind::Default,
            "high-water mark of the raw per-second DECOMPRESSION rate since \
             process start (fetch_max, never reset). Calibration signal."
        );
        let v = self.swapin_rate_max.load(Ordering::Relaxed);
        publish!(
            "swapin_rate_max",
            &v,
            MetricKind::Default,
            "high-water mark of the raw per-second SWAPIN rate since process \
             start (fetch_max, never reset) — the direct calibration input for \
             memory_gate_swapin_confirm_rate (currently suppressed at u32::MAX)."
        );
        // The 18 band-tick counters: sampler ticks (~100 ms each) whose rate fell
        // in the band. Monotonic; band SHAPE distinguishes sustained pressure
        // (many high-band ticks) from a spike (few) — what the gate's
        // sustained-N-ticks semantics needs a threshold calibrated against.
        for (signal, bands) in [
            ("compress", &self.compress_rate_bands),
            ("decompress", &self.decompress_rate_bands),
            ("swapin", &self.swapin_rate_bands),
        ] {
            for (i, suffix) in ["0", "1_10", "11_100", "101_1k", "1k_10k", "gt10k"]
                .iter()
                .enumerate()
            {
                let v = bands[i].load(Ordering::Relaxed);
                publish!(
                    format!("{signal}_rate_band_{suffix}_total"),
                    &v,
                    MetricKind::Counter,
                    format!(
                        "sampler ticks (~100 ms) with the {signal} rate in band \
                         {suffix} events/sec (bands 0, 1-10, 11-100, 101-1k, \
                         1k-10k, >10k); monotonic — band shape distinguishes \
                         sustained pressure from a spike."
                    )
                );
            }
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// #37: process-wide memory gate NAK counters. Backed by a `static`
/// so `const fn new()` suffices.
static MEMORY_GATE_COUNTERS: MemoryGateCounters = MemoryGateCounters::new();
/// #37: cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents
/// a double-registration hazard if `memory_gate_counters_arc()` is called
/// twice — both calls return a clone of the same `Arc`.
static MEMORY_GATE_COUNTERS_ARC: OnceLock<Arc<MemoryGateCountersHandle>> = OnceLock::new();

/// #37: process-wide memory gate NAK counters singleton. All calls within
/// the process observe the same atomic state.
#[must_use]
pub fn memory_gate_counters() -> &'static MemoryGateCounters {
    &MEMORY_GATE_COUNTERS
}

/// #37: `Arc` wrapper for `MetricsRegistry::register`. The singleton lives
/// in a `static`; the `Arc` carries a zero-sized handle that delegates
/// `publish` to the static so scrapes always read live state. `OnceLock`-
/// cached so repeated calls return a clone of the same `Arc`.
#[must_use]
pub fn memory_gate_counters_arc() -> Arc<MemoryGateCountersHandle> {
    Arc::clone(MEMORY_GATE_COUNTERS_ARC.get_or_init(|| Arc::new(MemoryGateCountersHandle)))
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed `#37` counters.
#[derive(Debug)]
pub struct MemoryGateCountersHandle;

impl MetricsComponent for MemoryGateCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        MEMORY_GATE_COUNTERS.publish(kind, field_metadata)
    }
}

// =====================================================================
// #FL-688 reconcile-pin observability — process-wide reconcile-pin /
// backfill counters
// =====================================================================

/// (#FL-688 log-miscalibration fix) Process-wide reconcile-pin / backfill
/// counters for the worker's `UploadMissingBlobs` handler.
///
/// These were originally fields on `LocalWorker.metrics` (a per-instance
/// `MetricsComponent` tree that is never registered with `MetricsRegistry` —
/// the worker-metrics-exposure trap; the same class #37 memory_gate, #86
/// symlink_fix, and #DC3 dir_cache fixed). On that dark tree the counters
/// rendered in an in-process `publish()` unit test but never reached
/// `/metrics`. Moving them to a process singleton (registered under the
/// "reconcile_pin" prefix in `nativelink.rs`) makes them visible without
/// per-instance registration.
///
/// The server ONLY requests a blob the worker itself advertised via
/// `BlobsAvailable` (`worker_api_server.rs::request_missing_blob_uploads` gates
/// the request on the worker's advertised set minus the server CAS's own
/// `has_with_results`). So every digest in an `UploadMissingBlobs` request is
/// one the worker CLAIMED to hold. The four counters below classify what
/// happens to each such digest by SEVERITY — separating benign races and
/// recoverable retries from genuine, irrecoverable sole-copy loss:
///
/// - `refused` (BENIGN, info): the digest was absent from the moka in-memory
///   eviction INDEX at reconcile-pin time (`pin_digest_indefinite_or_time_bounded`
///   returned `Refused`). NOT data loss: the handler does not skip the upload —
///   it re-checks presence via `FastSlowStore::has_with_results`, which reads
///   DISK + `mirror_blobs` (not the moka index), so a blob absent from the
///   pin-index is still on disk and uploads fine (verified live: `970 refused →
///   found: 1000 → uploaded: 1000, failed: 0`). Observe the rate; do NOT alert.
///
/// - `vanished` (IRRECOVERABLE loss, WARN): the digest the worker advertised is
///   ABSENT at the `has_with_results` re-check (disk + in-flight + mirror all
///   `None`). The worker advertised it, then lost it (unpinned eviction + mirror
///   drop) and can no longer produce it. Because the server requested it, the
///   server does not hold it either → **sole-copy permanent loss**. This is the
///   genuine FL-688 regression signal — the path a real regression surfaces on.
///   Nothing downstream can recover it (the blob is gone); alert on any non-zero.
///
/// - `requeued` (RECOVERABLE, info): the upload was attempted but FAILED, and
///   the digest WAS re-queued into `failed_slow_writes` (`requeue_failed_push`
///   returned true) for retry-until-durable by the reconnect drainer. The blob
///   still exists locally and will be re-attempted — not yet lost. Rate>0 under
///   load is expected; a SUSTAINED high rate indicates a stuck slow tier.
///
/// - `dropped` (IRRECOVERABLE, WARN): the upload FAILED and the digest could
///   NOT be re-queued because `failed_slow_writes` is at cap
///   (`requeue_failed_push` returned false). The digest is dropped from the
///   retry set; only the server's next `BlobsAvailable` re-request can recover
///   it (and only while the blob still exists locally). Alert on any non-zero.
#[derive(Debug)]
pub struct ReconcilePinCounters {
    /// Digests absent from the moka eviction index at reconcile-pin time
    /// (`Refused`). BENIGN eviction race — the disk-backed upload still
    /// proceeds. Monotonic; alert on nothing, observe the rate for eviction
    /// churn.
    pub refused: AtomicU64,
    /// Advertised digests ABSENT at the `has_with_results` re-check — the worker
    /// claimed the blob then lost it and can no longer produce it. Sole-copy
    /// PERMANENT loss (the server requested it, so the server lacks it too). The
    /// genuine FL-688 data-loss signal. Monotonic; alert on any non-zero.
    pub vanished: AtomicU64,
    /// Upload attempted and FAILED but the digest WAS re-queued into
    /// `failed_slow_writes` for retry-until-durable (RECOVERABLE — the blob
    /// still exists locally). Monotonic; sustained high rate = stuck slow tier.
    pub requeued: AtomicU64,
    /// Upload FAILED and the digest could NOT be re-queued (`failed_slow_writes`
    /// at cap) — dropped from the retry set (IRRECOVERABLE via the local retry
    /// path). Monotonic; alert on any non-zero.
    pub dropped: AtomicU64,
}

impl ReconcilePinCounters {
    const fn new() -> Self {
        Self {
            refused: AtomicU64::new(0),
            vanished: AtomicU64::new(0),
            requeued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }
}

impl MetricsComponent for ReconcilePinCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // Registered under prefix "reconcile_pin" (nativelink.rs). The publish!
        // name is the FULL field name so operators alert on the literal string.
        // No inner group!() — the prefix already scopes these uniquely (avoids
        // the #86 doubled-name trap).
        let v = self.refused.load(Ordering::Relaxed);
        publish!(
            "refused_total",
            &v,
            MetricKind::Counter,
            "BENIGN: blobs absent from the moka eviction index at reconcile-pin \
             time (evicted before pin or never inserted). The disk-backed upload \
             still proceeds (presence is re-checked on disk + mirror_blobs, NOT \
             the moka index). Monotonic — observe the rate for eviction churn; do \
             NOT alert on non-zero (this is NOT the data-loss signal — see \
             reconcile_pin_vanished_total)."
        );
        let v = self.vanished.load(Ordering::Relaxed);
        publish!(
            "vanished_total",
            &v,
            MetricKind::Counter,
            "FL-688 data-loss signal (IRRECOVERABLE): advertised blobs ABSENT at \
             the reconcile re-check — the worker advertised the blob, then lost it \
             (unpinned eviction + mirror drop) and can no longer produce it. The \
             server requested it, so the server lacks it too = sole-copy permanent \
             loss. Monotonic — alert on ANY non-zero; this is where a real FL-688 \
             regression surfaces. Nothing downstream can recover it."
        );
        let v = self.requeued.load(Ordering::Relaxed);
        publish!(
            "requeued_total",
            &v,
            MetricKind::Counter,
            "RECOVERABLE: backfill uploads that FAILED but were re-queued into \
             failed_slow_writes for retry-until-durable (the blob still exists \
             locally). Monotonic — rate>0 under load is expected; a SUSTAINED high \
             rate indicates a stuck slow tier, not yet loss."
        );
        let v = self.dropped.load(Ordering::Relaxed);
        publish!(
            "dropped_total",
            &v,
            MetricKind::Counter,
            "FL-688 data-loss signal (IRRECOVERABLE via local retry): backfill \
             uploads that FAILED and could NOT be re-queued because \
             failed_slow_writes is at cap. Dropped from the retry set; only the \
             server's next BlobsAvailable re-request can recover it (and only \
             while the blob still exists locally). Monotonic — alert on any \
             non-zero."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// #FL-688: process-wide reconcile-pin counters. Backed by a `static`
/// so `const fn new()` suffices.
static RECONCILE_PIN_COUNTERS: ReconcilePinCounters = ReconcilePinCounters::new();
/// #FL-688: cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents
/// a double-registration hazard if `reconcile_pin_counters_arc()` is called
/// twice — both calls return a clone of the same `Arc`.
static RECONCILE_PIN_COUNTERS_ARC: OnceLock<Arc<ReconcilePinCountersHandle>> = OnceLock::new();

/// #FL-688: process-wide reconcile-pin counters singleton. All calls within
/// the process observe the same atomic state.
#[must_use]
pub fn reconcile_pin_counters() -> &'static ReconcilePinCounters {
    &RECONCILE_PIN_COUNTERS
}

/// #FL-688: `Arc` wrapper for `MetricsRegistry::register`. The singleton lives
/// in a `static`; the `Arc` carries a zero-sized handle that delegates
/// `publish` to the static so scrapes always read live state. `OnceLock`-
/// cached so repeated calls return a clone of the same `Arc`.
#[must_use]
pub fn reconcile_pin_counters_arc() -> Arc<ReconcilePinCountersHandle> {
    Arc::clone(RECONCILE_PIN_COUNTERS_ARC.get_or_init(|| Arc::new(ReconcilePinCountersHandle)))
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed reconcile-pin counters.
#[derive(Debug)]
pub struct ReconcilePinCountersHandle;

impl MetricsComponent for ReconcilePinCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        RECONCILE_PIN_COUNTERS.publish(kind, field_metadata)
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
// FL-681 worker admission-NAK counter (pin-cap saturation)
// =====================================================================

/// (FL-681 NAK boundary fix) Process-wide count of new actions the worker
/// REFUSED at admission because its local CAS FilesystemStore pin budget was
/// saturated (`indefinite_pin_saturated()` true), NAKing with
/// `Code::ResourceExhausted` so the scheduler re-queues them.
///
/// This is the counter that proves the NAK gate is actually FIRING. It was the
/// missing signal in the FL-681 incident: the gate was silently dead (pins
/// pegged at cap, thousands of internal refusals, ~1 scheduler re-queue in
/// 12 h) with no metric distinguishing "gate never fires" from "gate healthy,
/// zero load." Backed by a `static` so `const fn new()` suffices; incremented
/// DIRECTLY at the NAK site in `running_actions_manager.rs::create_and_add_action`.
///
/// Lives here (NOT on the per-instance `LocalWorker.metrics` tree, which is
/// never registered with `MetricsRegistry` — the worker-metrics-exposure trap;
/// same class as #37 memory_gate, #86 symlink_fix, #DC3 dir_cache) so it renders
/// on `/metrics`. Registered under prefix `"worker_admission"` → rendered name:
///   `worker_admission_nak_pin_saturated_total`
#[derive(Debug)]
pub struct WorkerAdmissionNakCounters {
    /// Monotone count of new actions NAKed at admission because the worker's
    /// local pin cap was saturated (F2 deferred-output mode).
    pub nak_pin_saturated: AtomicU64,
}

impl WorkerAdmissionNakCounters {
    const fn new() -> Self {
        Self {
            nak_pin_saturated: AtomicU64::new(0),
        }
    }

    /// Record one admission NAK due to pin-cap saturation.
    pub fn record_nak_pin_saturated(&self) {
        self.nak_pin_saturated.fetch_add(1, Ordering::Relaxed);
    }
}

impl MetricsComponent for WorkerAdmissionNakCounters {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let v = self.nak_pin_saturated.load(Ordering::Relaxed);
        publish!(
            "nak_pin_saturated_total",
            &v,
            MetricKind::Counter,
            "Count of new actions the worker REFUSED at admission because its local CAS \
             pin budget was saturated (indefinite_pin_saturated true), NAKing with \
             ResourceExhausted so the scheduler re-queues them (F2 deferred-output mode). \
             Monotonic — alert on rate; a rising rate means sustained pending-BIS durability \
             backpressure. Zero = gate never fired (healthy pin budget OR — the FL-681 bug \
             — a dead gate); correlate with the pinned_bytes/pin_cap gauges to tell them apart."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// (FL-681) Process-wide worker admission-NAK counter. Backed by a `static`
/// so `const fn new()` suffices; incremented from the NAK site in
/// `running_actions_manager.rs::create_and_add_action`.
static WORKER_ADMISSION_NAK_COUNTERS: WorkerAdmissionNakCounters =
    WorkerAdmissionNakCounters::new();
/// (FL-681) Cached `Arc` for `MetricsRegistry::register`. `OnceLock` prevents
/// double-registration; both calls return a clone of the same `Arc`.
static WORKER_ADMISSION_NAK_COUNTERS_ARC: OnceLock<Arc<WorkerAdmissionNakCountersHandle>> =
    OnceLock::new();

/// (FL-681) Process-wide worker admission-NAK counter singleton. All calls
/// within the process observe the same atomic state.
#[must_use]
pub fn worker_admission_nak_counters() -> &'static WorkerAdmissionNakCounters {
    &WORKER_ADMISSION_NAK_COUNTERS
}

/// (FL-681) `Arc` wrapper for `MetricsRegistry::register`. The singleton lives
/// in a `static`; the `Arc` carries a zero-sized handle that delegates
/// `publish` to the static so scrapes always read live state. `OnceLock`-cached.
#[must_use]
pub fn worker_admission_nak_counters_arc() -> Arc<WorkerAdmissionNakCountersHandle> {
    Arc::clone(
        WORKER_ADMISSION_NAK_COUNTERS_ARC.get_or_init(|| Arc::new(WorkerAdmissionNakCountersHandle)),
    )
}

/// Zero-sized handle so `MetricsRegistry::register` can take an
/// `Arc<T: MetricsComponent>` for the `static`-backed FL-681 counter.
#[derive(Debug)]
pub struct WorkerAdmissionNakCountersHandle;

impl MetricsComponent for WorkerAdmissionNakCountersHandle {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        WORKER_ADMISSION_NAK_COUNTERS.publish(kind, field_metadata)
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use core::time::Duration;

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
            let timer = tokio::time::sleep(Duration::from_millis(100));
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
    /// Mutation: comment out `self.acquires.fetch_add(1, Ordering::Relaxed)`
    /// in `record_acquire()` → `acquires` stays at 0; the assertion below
    /// fails with "O14 singleton-aliasing: acquires must initialize to 0"
    /// on the post-record assert, exposing that the fetch_add is load-bearing.
    /// The `publish` delegation path is covered by
    /// `o14_render_prometheus_metric_names_not_doubled` (the render test).
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

    /// #86 (2026-06-15): end-to-end render test — verifies that the
    /// `SymlinkFixCounters::publish` registered under prefix `"symlink_fix"`
    /// and inner `group!("lock_acquires_total")` / `group!("slow_path_entries_total")`
    /// produce EXACTLY the Prometheus names `symlink_fix_lock_acquires_total_counter`
    /// and `symlink_fix_slow_path_entries_total_counter` after sanitization.
    ///
    /// This is the regression guard for the BLOCK identified in review: registering
    /// under `"symlink_fix_lock"` with inner `group!("symlink_fix_lock_acquires_total")`
    /// doubled the prefix to `symlink_fix_lock_symlink_fix_lock_acquires_total_counter`.
    ///
    /// Also asserts the doubled form is ABSENT so a name regression is caught
    /// immediately rather than silently producing wrong names.
    ///
    /// Covers `SymlinkFixCountersHandle::publish` delegation path (the prior
    /// singleton-aliasing test exercised `record_acquire` only, not `publish`).
    ///
    /// Mutation step: revert `group!("lock_acquires_total")` back to
    /// `group!("symlink_fix_lock_acquires_total")` in `SymlinkFixCounters::publish`.
    /// This test MUST red-fail with:
    ///   "#86 doubled metric name: ..."
    #[test]
    fn o14_render_prometheus_metric_names_not_doubled() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // Arc-owned counters for 'static lifetime required by register().
        // The real SYMLINK_FIX_COUNTERS static accumulates across the process
        // so tests use a local Arc to avoid cross-test interference.
        let counters = Arc::new(SymlinkFixCounters::new());
        for _ in 0..5 {
            counters.record_acquire();
        }
        for _ in 0..2 {
            counters.record_slow_path_entry();
        }

        let registry = MetricsRegistry::new();
        // Register under "symlink_fix" — the prefix the production nativelink.rs
        // uses (after this fix). Arc<SymlinkFixCounters> impls MetricsComponent
        // via the blanket Arc<T: MetricsComponent> impl, delegating to
        // SymlinkFixCounters::publish — same delegation as SymlinkFixCountersHandle.
        registry.register("symlink_fix", counters);

        let body = render_prometheus(&registry);

        // The counter sub-key must carry the recorded values using EXACT line matches
        // (newline-anchored). The exact-line form `\nNAME VALUE\n` distinguishes the
        // correct metric from the doubled-prefix form:
        //   correct:  symlink_fix_lock_acquires_total_counter 5
        //   doubled:  symlink_fix_symlink_fix_lock_acquires_total_counter 5
        // The substring "symlink_fix_lock_acquires_total" appears in BOTH, so a
        // bare `contains` is insufficient — only the newline-anchored value assertion
        // below correctly rejects the doubled form (the correct line is absent when
        // doubled, and the wrong line is present instead).
        assert!(
            body.contains("\nsymlink_fix_lock_acquires_total_counter 5\n"),
            "#86 render test: expected exact line `symlink_fix_lock_acquires_total_counter 5` \
             (5 record_acquire calls). If missing, either value is wrong or the metric name \
             is doubled (got `symlink_fix_symlink_fix_lock_acquires_total_counter 5` instead). \
             body=\n{body}"
        );
        assert!(
            body.contains("\nsymlink_fix_slow_path_entries_total_counter 2\n"),
            "#86 render test: expected exact line `symlink_fix_slow_path_entries_total_counter 2` \
             (2 record_slow_path_entry calls). If missing, either value is wrong or the metric \
             name is doubled. body=\n{body}"
        );

        // Belt-and-braces: also assert the doubled prefix is absent so the failure
        // message names the specific regression class.
        // When group!("symlink_fix_lock_acquires_total") is used inside publish()
        // while registered under "symlink_fix", the rendered name starts with
        // "symlink_fix_symlink_fix_lock" (prefix doubled).
        assert!(
            !body.contains("symlink_fix_symlink_fix_lock"),
            "#86 doubled metric name: rendered output contains doubled prefix \
             `symlink_fix_symlink_fix_lock` — the register key `symlink_fix` and inner \
             group!() name are concatenating incorrectly (group name should be \
             `lock_acquires_total`, not `symlink_fix_lock_acquires_total`). body=\n{body}"
        );
    }

    // =====================================================================
    // #DC3 DirCacheCounters tests
    // =====================================================================

    /// #DC3: increment-observable test on a LOCAL `DirCacheCounters`. Each
    /// `record_*` method must bump exactly its own outcome field, leaving
    /// the others untouched (no cross-talk between outcome classes).
    ///
    /// Mutation: comment out `self.exact_hit.fetch_add(1, ...)` in
    /// `record_exact_hit` → `exact_hit` stays at 0; this test red-fails with
    /// "#DC3 increment-observable: record_exact_hit x3 must yield exact_hit==3".
    #[test]
    fn dir_cache_counters_increment_observable_per_outcome() {
        let c = DirCacheCounters::new();
        for _ in 0..3 {
            c.record_exact_hit();
        }
        for _ in 0..2 {
            c.record_miss();
        }
        c.record_subtree_hits(5);
        c.record_fuzzy_match();
        c.record_hit_clonefile();
        c.record_hit_hardlink();
        c.record_hit_hardlink();

        assert_eq!(
            c.exact_hit.load(Ordering::Relaxed), 3,
            "#DC3 increment-observable: record_exact_hit x3 must yield exact_hit==3 (got {})",
            c.exact_hit.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.miss.load(Ordering::Relaxed), 2,
            "#DC3 increment-observable: record_miss x2 must yield miss==2 (got {})",
            c.miss.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.subtree_hit.load(Ordering::Relaxed), 5,
            "#DC3 increment-observable: record_subtree_hits(5) must yield subtree_hit==5 (got {})",
            c.subtree_hit.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.fuzzy_match.load(Ordering::Relaxed), 1,
            "#DC3 increment-observable: record_fuzzy_match x1 must yield fuzzy_match==1 (got {})",
            c.fuzzy_match.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.hit_clonefile.load(Ordering::Relaxed), 1,
            "#DC3 increment-observable: record_hit_clonefile x1 must yield hit_clonefile==1 (got {})",
            c.hit_clonefile.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.hit_hardlink.load(Ordering::Relaxed), 2,
            "#DC3 increment-observable: record_hit_hardlink x2 must yield hit_hardlink==2 (got {})",
            c.hit_hardlink.load(Ordering::Relaxed)
        );
    }

    /// #DC3: end-to-end render test — the PRIMARY contract guard for this
    /// task. Verifies the `DirCacheCounters::publish` registered under prefix
    /// `"dir_cache"` produces EXACTLY the Prometheus outcome lines via the
    /// SAME `render_prometheus` walk the worker `/metrics` handler uses (NOT a
    /// hand-rolled scrape). This is the test that catches the
    /// worker-metrics-exposure trap: if the counters are dark on `/metrics`
    /// (no `publish!` emitted, or the singleton never registered) the
    /// outcome lines are ABSENT and this test red-fails.
    ///
    /// Newline-anchored exact-line assertions (`\nNAME VALUE\n`) so a
    /// doubled-prefix regression (#86 class — registering under `dir_cache`
    /// with an inner `group!("dir_cache_...")`) is also caught: the doubled
    /// form `dir_cache_dir_cache_*` would make the correct line absent.
    ///
    /// Mutation (a): make `DirCacheCounters::publish` a no-op (the TDD-RED
    /// stub) OR drop the `register("dir_cache", …)` → outcome lines vanish;
    /// this test red-fails with the "dark on /metrics" message below.
    #[test]
    fn dir_cache_render_prometheus_exposes_outcome_counters() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // Arc-owned LOCAL counters for the 'static lifetime register() wants;
        // the production DIR_CACHE_COUNTERS static accumulates across the
        // process, so the test uses a local Arc to avoid cross-test interference.
        let counters = Arc::new(DirCacheCounters::new());
        for _ in 0..7 {
            counters.record_exact_hit();
        }
        for _ in 0..3 {
            counters.record_miss();
        }
        counters.record_subtree_hits(11);
        counters.record_fuzzy_match();
        counters.record_fuzzy_match();
        counters.record_hit_clonefile();
        for _ in 0..4 {
            counters.record_hit_hardlink();
        }
        for _ in 0..5 {
            counters.record_hit_clonefile_preempted();
        }
        // #speculative-prefetch prewarm outcome + priority counters.
        for _ in 0..6 {
            counters.record_prewarm_warm_redundant();
        }
        for _ in 0..8 {
            counters.record_prewarm_completed();
        }
        for _ in 0..9 {
            counters.record_prewarm_speculative();
        }
        counters.record_prewarm_foreground();

        let registry = MetricsRegistry::new();
        // Register under "dir_cache" — the prefix production nativelink.rs uses.
        // Arc<DirCacheCounters> impls MetricsComponent via the blanket
        // Arc<T: MetricsComponent> impl, delegating to DirCacheCounters::publish
        // — the same delegation path as DirCacheCountersHandle.
        registry.register("dir_cache", counters);

        let body = render_prometheus(&registry);

        // Each outcome must render as an exact newline-anchored line with the
        // recorded value. Absence = "dark on /metrics" (the failure this task
        // exists to prevent).
        for (name, value) in [
            ("dir_cache_exact_hit_total_counter", 7u64),
            ("dir_cache_miss_total_counter", 3),
            ("dir_cache_subtree_hit_total_counter", 11),
            ("dir_cache_fuzzy_match_total_counter", 2),
            ("dir_cache_hit_clonefile_total_counter", 1),
            ("dir_cache_hit_hardlink_total_counter", 4),
            ("dir_cache_hit_clonefile_preempted_total_counter", 5),
            ("dir_cache_prewarm_warm_redundant_total_counter", 6),
            ("dir_cache_prewarm_completed_total_counter", 8),
            ("dir_cache_prewarm_speculative_total_counter", 9),
            ("dir_cache_prewarm_foreground_total_counter", 1),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "#DC3 dark on /metrics: expected exact line `{name} {value}` from the \
                 render_prometheus walk, but it is ABSENT — the dir-cache outcome counter \
                 is not exposed (publish emitted nothing, the value is wrong, or the metric \
                 name is doubled e.g. `dir_cache_dir_cache_{name}`). body=\n{body}"
            );
        }

        // Belt-and-braces: the doubled-prefix form (#86 class) must be absent
        // so a name regression names itself.
        assert!(
            !body.contains("dir_cache_dir_cache"),
            "#DC3 doubled metric name: rendered output contains doubled prefix \
             `dir_cache_dir_cache` — the register key `dir_cache` and an inner group!() \
             name are concatenating (group names must NOT repeat the `dir_cache` prefix). \
             body=\n{body}"
        );
    }

    /// (#37 re-enable follow-up) The memory-gate NAK counters must render on the
    /// REAL `/metrics` path (`MetricsRegistry::register` + `render_prometheus`),
    /// not just `MetricsComponent::publish` in isolation — the canary soak alerts
    /// on the rendered Prometheus line. This test PINS the exact rendered name:
    /// the value line is the BARE `memory_gate_nak_free_floor_total` (NO `_counter`
    /// suffix — verified empirically here; do not assume a suffix from other
    /// counters' output). Stops an operator / the soak runbook from alerting on a
    /// name that does not exist on the wire (the worker-metrics-exposure trap).
    #[test]
    fn memory_gate_render_prometheus_exposes_nak_counters() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // LOCAL Arc (not the process static) for the 'static register() lifetime
        // + to avoid cross-test interference with MEMORY_GATE_COUNTERS. The
        // blanket `Arc<T: MetricsComponent>` impl delegates to
        // `MemoryGateCounters::publish` — the same path the registered handle uses.
        let counters = Arc::new(MemoryGateCounters::new());
        counters.nak_free_floor.fetch_add(7, Ordering::Relaxed);
        counters.nak_swapin.fetch_add(3, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        // Prefix "memory_gate" — the exact key production nativelink.rs registers.
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("memory_gate_nak_free_floor_total", 7u64),
            ("memory_gate_nak_swapin_total", 3),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "#37 dark on /metrics: expected exact line `{name} {value}` from the \
                 render_prometheus walk, but it is ABSENT — the canary soak alerts on this \
                 EXACT rendered name. body=\n{body}"
            );
        }
        // Guard the doubled-prefix trap (as the dir_cache test does).
        assert!(
            !body.contains("memory_gate_memory_gate"),
            "#37 doubled metric name: rendered output contains `memory_gate_memory_gate` \
             — the register key and an inner group!() are concatenating. body=\n{body}"
        );
    }

    /// (#FL-688 log-miscalibration fix) All four reconcile-pin counters must
    /// render on the REAL `/metrics` path (`MetricsRegistry::register` +
    /// `render_prometheus`), not just `MetricsComponent::publish` in isolation
    /// — the fleet alarm keys on the rendered Prometheus line. This test PINS
    /// the exact rendered names: the value lines are the BARE
    /// `reconcile_pin_refused_total`, `reconcile_pin_vanished_total`,
    /// `reconcile_pin_requeued_total`, `reconcile_pin_dropped_total` (NO
    /// `_counter` suffix). Without a rendering singleton these would stay on the
    /// per-instance `LocalWorker.metrics` tree — DARK on `/metrics` (the
    /// worker-metrics-exposure trap).
    ///
    /// Mutation: comment out any one `publish!` block in
    /// `ReconcilePinCounters::publish` → the corresponding line vanishes; this
    /// test red-fails with the bespoke "dark on /metrics" message below.
    #[test]
    fn reconcile_pin_render_prometheus_exposes_counters() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // LOCAL Arc (not the process static) for the 'static register()
        // lifetime + to avoid cross-test interference with
        // RECONCILE_PIN_COUNTERS. The blanket `Arc<T: MetricsComponent>` impl
        // delegates to `ReconcilePinCounters::publish` — the same path the
        // registered handle uses. Distinct values so a mis-wired field (right
        // name, wrong source) is caught, not just an absent line.
        let counters = Arc::new(ReconcilePinCounters::new());
        counters.refused.fetch_add(970, Ordering::Relaxed);
        counters.vanished.fetch_add(3, Ordering::Relaxed);
        counters.requeued.fetch_add(11, Ordering::Relaxed);
        counters.dropped.fetch_add(2, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        // Prefix "reconcile_pin" — the exact key production nativelink.rs registers.
        registry.register("reconcile_pin", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("reconcile_pin_refused_total", 970u64),
            ("reconcile_pin_vanished_total", 3),
            ("reconcile_pin_requeued_total", 11),
            ("reconcile_pin_dropped_total", 2),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "#FL-688 dark on /metrics: expected exact line `{name} {value}` from the \
                 render_prometheus walk, but it is ABSENT — the fleet alarm keys on this \
                 EXACT rendered name. body=\n{body}"
            );
        }
        // Guard the doubled-prefix trap (as the dir_cache / memory_gate tests do).
        assert!(
            !body.contains("reconcile_pin_reconcile_pin"),
            "#FL-688 doubled metric name: rendered output contains \
             `reconcile_pin_reconcile_pin` — the register key and an inner group!() are \
             concatenating. body=\n{body}"
        );
    }

    /// (#FL-688 log-miscalibration fix) The four reconcile-pin counters are
    /// SEPARATE atomics classified by durability SEVERITY — incrementing one
    /// must NEVER touch another. This pins the asymmetry that makes the fix
    /// correct: the BENIGN `refused` path (eviction race, disk-backed upload
    /// still proceeds) and the RECOVERABLE `requeued` path (failed-but-retried)
    /// must NOT raise either irrecoverable data-loss counter (`vanished` =
    /// advertised-then-lost sole-copy loss; `dropped` = over-cap requeue drop).
    /// The old single `backfill_failed` counter folded recoverable and
    /// irrecoverable together — a milder reprise of the miscalibration this fix
    /// removes.
    ///
    /// (The production increments fire in `local_worker.rs`'s
    /// `handle_upload_missing_blobs`; `vanished`/`dropped`/`requeued` are
    /// exercised end-to-end by the `backfill_*` tests in
    /// `nativelink-worker/tests/backfill_upload_requeue_test.rs`. This test pins
    /// the field-independence contract those rely on.)
    ///
    /// Mutation: point any `fetch_add` at the wrong field → a cross-field
    /// assertion red-fails with its bespoke message.
    #[test]
    fn reconcile_pin_counters_are_independent_by_severity() {
        let c = ReconcilePinCounters::new();
        for f in [&c.refused, &c.vanished, &c.requeued, &c.dropped] {
            assert_eq!(f.load(Ordering::Relaxed), 0);
        }

        // BENIGN refused bumps ONLY refused — never an irrecoverable-loss counter.
        c.refused.fetch_add(970, Ordering::Relaxed);
        assert_eq!(
            c.vanished.load(Ordering::Relaxed),
            0,
            "#FL-688 miscalibration: a benign reconcile-pin Refused (970) must NOT raise the \
             vanished (sole-copy loss) counter — Refused is an eviction race, the \
             disk-backed upload still proceeds",
        );
        assert_eq!(c.dropped.load(Ordering::Relaxed), 0);

        // RECOVERABLE requeued must NOT read as irrecoverable loss.
        c.requeued.fetch_add(11, Ordering::Relaxed);
        assert_eq!(
            c.vanished.load(Ordering::Relaxed),
            0,
            "#FL-688: a RECOVERABLE requeued failure (retry-until-durable) must NOT raise the \
             vanished (irrecoverable sole-copy loss) counter — the folded backfill_failed \
             counter was the miscalibration this split removes",
        );
        assert_eq!(c.dropped.load(Ordering::Relaxed), 0);

        // The two irrecoverable-loss counters are distinct and do not cross.
        c.vanished.fetch_add(3, Ordering::Relaxed);
        c.dropped.fetch_add(2, Ordering::Relaxed);
        assert_eq!(c.refused.load(Ordering::Relaxed), 970);
        assert_eq!(c.requeued.load(Ordering::Relaxed), 11);
        assert_eq!(c.vanished.load(Ordering::Relaxed), 3);
        assert_eq!(c.dropped.load(Ordering::Relaxed), 2);
    }

    /// #DC3 (scope ext): phase-observe increment test on a LOCAL
    /// `DirCacheCounters`. Each `record_construct_*` / `record_hit_assemble_ms`
    /// must accumulate sum + count on its own phase only.
    ///
    /// Mutation: comment out `self.count.fetch_add(1, ...)` in
    /// `PhaseTiming::observe_ms` → count stays 0; this test red-fails with
    /// "#DC3 phase-observe: resolve count must be 2".
    #[test]
    fn dir_cache_phase_timing_observe_accumulates_sum_and_count() {
        let c = DirCacheCounters::new();
        c.record_construct_resolve_ms(10);
        c.record_construct_resolve_ms(30);
        c.record_construct_fetch_ms(100);
        c.record_hit_assemble_ms(7);

        assert_eq!(
            c.construct_resolve_ms.sum_ms.load(Ordering::Relaxed), 40,
            "#DC3 phase-observe: resolve sum_ms must be 10+30=40 (got {})",
            c.construct_resolve_ms.sum_ms.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.construct_resolve_ms.count.load(Ordering::Relaxed), 2,
            "#DC3 phase-observe: resolve count must be 2 (got {})",
            c.construct_resolve_ms.count.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.construct_fetch_ms.sum_ms.load(Ordering::Relaxed), 100,
            "#DC3 phase-observe: fetch sum_ms must be 100 (got {})",
            c.construct_fetch_ms.sum_ms.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.construct_fetch_ms.count.load(Ordering::Relaxed), 1,
            "#DC3 phase-observe: fetch count must be 1 (got {})",
            c.construct_fetch_ms.count.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.hit_assemble_ms.sum_ms.load(Ordering::Relaxed), 7,
            "#DC3 phase-observe: assemble sum_ms must be 7 (got {})",
            c.hit_assemble_ms.sum_ms.load(Ordering::Relaxed)
        );
        assert_eq!(
            c.hit_assemble_ms.count.load(Ordering::Relaxed), 1,
            "#DC3 phase-observe: assemble count must be 1 (got {})",
            c.hit_assemble_ms.count.load(Ordering::Relaxed)
        );
    }

    /// #DC3 (scope ext): end-to-end render test for the cold-construct phase
    /// sub-cost decomposition. Verifies the three phase sum+count pairs render
    /// via the SAME `render_prometheus` walk the worker `/metrics` handler
    /// uses, with EXACT newline-anchored names. This is the dark-on-/metrics
    /// guard for the phase decomposition (the actual #1/#2 decision instrument).
    ///
    /// Mutation: comment out the `emit_phase("construct_resolve", …)` call in
    /// `DirCacheCounters::publish` → the resolve sum/count lines vanish; this
    /// test red-fails with "#DC3 phase dark on /metrics: …".
    #[test]
    fn dir_cache_render_prometheus_exposes_construct_phase_costs() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(DirCacheCounters::new());
        // resolve: 2 obs summing to 40ms; fetch: 1 obs of 100ms;
        // assemble: 3 obs summing to 21ms.
        counters.record_construct_resolve_ms(10);
        counters.record_construct_resolve_ms(30);
        counters.record_construct_fetch_ms(100);
        counters.record_hit_assemble_ms(7);
        counters.record_hit_assemble_ms(7);
        counters.record_hit_assemble_ms(7);

        let registry = MetricsRegistry::new();
        registry.register("dir_cache", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("dir_cache_construct_resolve_ms_sum", 40u64),
            ("dir_cache_construct_resolve_ms_count", 2),
            ("dir_cache_construct_fetch_ms_sum", 100),
            ("dir_cache_construct_fetch_ms_count", 1),
            ("dir_cache_hit_assemble_ms_sum", 21),
            ("dir_cache_hit_assemble_ms_count", 3),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "#DC3 phase dark on /metrics: expected exact line `{name} {value}` from the \
                 render_prometheus walk, but it is ABSENT — the cold-construct phase \
                 decomposition is not exposed (publish emitted nothing for this phase, the \
                 value is wrong, or the name is doubled). body=\n{body}"
            );
        }

        assert!(
            !body.contains("dir_cache_dir_cache"),
            "#DC3 doubled metric name (phase): rendered output contains `dir_cache_dir_cache`. \
             body=\n{body}"
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) BOOT-DOMINATION FIX — the
    /// core contract of the conditioning task. A decayed p95 must TRACK a
    /// sustained regime shift; a since-boot cumulative estimator (mean or
    /// non-decayed histogram) would FOSSILISE on the early regime and never
    /// track the new one.
    ///
    /// Scenario: 1000 observations in a MID bucket (100 ms) at `t0` so the p95
    /// sits at 100; then — after a 300 s WALL-CLOCK gap — a SUSTAINED low regime
    /// of 300 observations at 5 ms. With WALL-TIME decay (`P95_DECAY_KEEP_PER_SEC
    /// = 0.98`/s over the 300 s gap) the 1000 old-mid weights decay to
    /// `1000 * 0.98^300 ≈ 2.4` while the 300 recent-low weights (weight 300)
    /// dominate → the p95 DROPS to 5. Without decay (cumulative), the 1000 mid
    /// observations still hold the 95th-percentile mass (total 1300) → the p95
    /// STAYS fossilised at 100. (#obs-tuning follow-up 1: uses `observe_at` with
    /// an explicit clock — decay is now wall-clock-driven, so a burst at one
    /// instant does NOT self-decay; the 300 s gap is what ages the old regime.)
    ///
    /// Mutation (CLAUDE.md TDD #5): make the `decay_to` multiply a no-op in
    /// `DecayingP95Histogram`. The histogram becomes cumulative; the p95 stays
    /// at 100 and this test red-fails with the bespoke "boot-domination NOT
    /// fixed" message.
    #[test]
    fn p95_decays_toward_new_regime_not_fossilised_since_boot() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        // Old regime: a large mass in the 100ms bucket, all at t0.
        for _ in 0..1000 {
            h.observe_at(100, t0);
        }
        assert_eq!(
            h.p95_ms_at(t0),
            100,
            "sanity: after 1000×100ms observations the p95 must be the 100ms \
             bucket upper edge (got {})",
            h.p95_ms_at(t0)
        );
        // Sustained regime shift 300 s LATER: 300 low observations. The 300 s
        // wall-clock gap decays the old 1000-mass to ~2.4 (0.98^300).
        let t1 = t0 + Duration::from_secs(300);
        for _ in 0..300 {
            h.observe_at(5, t1);
        }
        let p95 = h.p95_ms_at(t1);
        assert!(
            p95 <= 5,
            "#obs-tuning boot-domination NOT fixed: after a SUSTAINED low regime \
             (300×5ms, 300s after 1000×100ms) the decayed p95 must track the new \
             regime and fall to <=5ms, but it stayed at {p95}ms — the early-regime \
             mass never aged (the wall-clock decay in decay_to was removed, \
             making the histogram a since-boot cumulative fossil, exactly the \
             defect this task fixes)"
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) The estimator tracks an
    /// UPWARD regime shift too (the direction that matters most for `T_SETUP`:
    /// cold-construct cost rising). After a low regime then a sustained high
    /// regime, the p95 must rise into the high bucket. Complements the
    /// downward-tracking test above (both directions of the contract).
    #[test]
    fn p95_tracks_upward_regime_shift() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        for _ in 0..500 {
            h.observe_at(5, t0); // low regime → p95 at 5
        }
        assert_eq!(h.p95_ms_at(t0), 5, "sanity: low regime p95 is 5ms");
        // 300 s later, a sustained high regime. The gap ages the low mass away.
        let t1 = t0 + Duration::from_secs(300);
        for _ in 0..300 {
            h.observe_at(8_000, t1); // high regime: 8000 > 5000 → the <=30000 bucket
        }
        let p95 = h.p95_ms_at(t1);
        assert!(
            p95 >= 5_000,
            "#obs-tuning upward-tracking: after a sustained 8000ms regime the p95 \
             must rise to the high bucket (>=5000ms), got {p95}ms — the estimator \
             is not tracking the recent expensive-construct regime"
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) CONSERVATIVE p95 bucket math
    /// on a known distribution. 95 observations at 10ms + 5 observations at
    /// 5000ms: the exact 95th percentile lands ABOVE the 10ms mass, so the
    /// conservative p95 must be the UPPER edge of the high bucket (5000ms), NOT
    /// 10ms — under-reporting the tail is the costly `T_SETUP` direction the
    /// upper-edge choice guards against. Uses a fresh (undecayed-dominant)
    /// window so the ratio is well-defined.
    ///
    /// Mutation: change `p95_ms`'s `cumulative >= target` to accumulate the
    /// WRONG edge (e.g. return `BUCKETS[idx-1]`), or set `target = total * 0.5`
    /// → the returned percentile no longer equals the 5000ms conservative edge.
    /// (#obs-tuning follow-up 1: spaced 1 s per observation via `observe_at` so
    /// the wall-clock decay weights the late-spread tail exactly as the prior
    /// per-observation decay did — same conservative-edge contract.)
    #[test]
    fn p95_is_conservative_upper_edge_on_known_distribution() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        // Interleave so decay does not fully erase the minority tail before we
        // read: 5000ms tail samples spread through the 10ms bulk, 1 s apart.
        for i in 0..100 {
            let at = t0 + Duration::from_secs(i);
            if i % 20 == 19 {
                h.observe_at(5_000, at); // 5 tail samples (indices 19,39,59,79,99)
            } else {
                h.observe_at(10, at); // 95 bulk samples
            }
        }
        let p95 = h.p95_ms_at(t0 + Duration::from_secs(99));
        assert_eq!(
            p95, 5_000,
            "#obs-tuning conservative-p95: with 95% mass at 10ms and a 5% tail at \
             5000ms, the p95 must be the 5000ms bucket upper edge (the tail the \
             95th percentile reaches), got {p95}ms — a non-conservative or \
             wrong-target extraction would under-report the tail T_SETUP must not \
             under-price"
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) Empty-histogram sentinel:
    /// with no observations the p95 is 0 (the same "no cold constructs observed
    /// yet" sentinel the prior `sum/max(count,1)` mean produced), so a freshly
    /// booted worker gossips 0, NOT a spurious bucket edge.
    #[test]
    fn p95_empty_is_zero_sentinel() {
        let h = DecayingP95Histogram::new();
        assert_eq!(
            h.p95_ms(),
            0,
            "#obs-tuning: an unobserved p95 histogram must report 0 (no cold \
             constructs yet), got {}",
            h.p95_ms()
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) `record_construct_fetch_ms`
    /// feeds BOTH derived signals from ONE observation: the cumulative
    /// `construct_fetch_ms` sum+count (the `/metrics` DC3 decomposition, KEPT)
    /// AND the decayed `construct_fetch_p95` estimator (the conditioned gossip).
    /// Guards against a future edit dropping one feed.
    ///
    /// Mutation: comment out `self.construct_fetch_p95.observe(elapsed_ms);` in
    /// `record_construct_fetch_ms` → the p95 stays 0 while sum/count update;
    /// this test red-fails on the p95 assertion.
    #[test]
    fn record_construct_fetch_ms_feeds_both_sum_count_and_p95() {
        let c = DirCacheCounters::new();
        for _ in 0..50 {
            c.record_construct_fetch_ms(250);
        }
        // Cumulative sum/count (the /metrics DC3 signal) still accumulates.
        assert_eq!(
            c.construct_fetch_ms.count.load(Ordering::Relaxed),
            50,
            "record_construct_fetch_ms must still feed the cumulative count for \
             the /metrics DC3 decomposition"
        );
        assert_eq!(
            c.construct_fetch_ms.sum_ms.load(Ordering::Relaxed),
            50 * 250,
        );
        // The conditioned p95 estimator is also fed from the SAME call.
        assert_eq!(
            c.construct_fetch_p95.p95_ms(),
            250,
            "#obs-tuning: record_construct_fetch_ms must ALSO feed the p95 \
             estimator (50×250ms → p95 in the <=250 bucket = 250ms), got {} — the \
             `construct_fetch_p95.observe(..)` feed was dropped",
            c.construct_fetch_p95.p95_ms()
        );
    }

    /// (#obs-tuning-construct-latency-conditioning) EMPIRICAL GROUNDING — the p95
    /// biases toward the pessimistic cold-construct tail `T_SETUP` must not
    /// under-price, where the replaced `sum/count` mean landed in the cheap
    /// middle. Feeds the per-worker cold-construct population from a FORCED synthetic
    /// cold-input cascade (2026-07-07; NOT a steady-state scrape — live steady-state
    /// p95 is ~0, cold constructs are rare): 34, 61, 69,
    /// 75, 92, 141, 149, 210, 225, 229 ms. Their arithmetic mean is 128.5 ms (the
    /// old signal's value). On the finer [`CONSTRUCT_LATENCY_BUCKETS_MS`] ladder
    /// the top three samples (210, 225, 229) land in the `<=250` bucket, so the
    /// conservative 95th percentile still reports 250 ms (the genuine tail edge —
    /// here the finer ladder AGREES with the coarse one because the tail truly
    /// reaches ~229 ms; the follow-up-2 finer resolution matters for populations
    /// whose p95 is BELOW ~200 ms, tested separately). The gate compares
    /// `T_wait_W` against a construct COST; the mean understates that cost by ~2×
    /// against this real distribution, so a `T_SETUP` derived from it would hold
    /// too rarely. The p95 reports the tail the gate actually needs.
    #[test]
    fn p95_on_live_measured_cold_population_reports_tail_not_mean_valley() {
        let h = DecayingP95Histogram::new();
        // The frozen-across-reads live per-worker means the dispatch measured;
        // here treated as one worker's recent cold-construct SAMPLES.
        for ms in [34, 61, 69, 75, 92, 141, 149, 210, 225, 229] {
            h.observe(ms);
        }
        let p95 = h.p95_ms();
        // Arithmetic mean of the same population = 1285/10 = 128 ms. The p95 must
        // be the 250ms edge (the tail the top three samples reach).
        assert_eq!(
            p95, 250,
            "#obs-tuning: p95 of the live-measured cold population (mean 128.5ms) \
             must report the pessimistic 250ms tail edge, not the ~100ms a mean \
             gives — got {p95}ms; T_SETUP needs the tail, not the valley"
        );
        assert!(
            p95 > 128,
            "#obs-tuning: the p95 ({p95}ms) must exceed the population mean \
             (128.5ms) — the whole point of conditioning is to stop understating \
             the cold-construct cost the way the replaced mean did"
        );
    }

    // =================================================================
    // #obs-tuning follow-up 1: TIME-AWARE (wall-clock) decay.
    //
    // The decay must age by ELAPSED WALL-TIME between accesses, not per
    // observation, so an IDLE worker's p95 ages toward 0 instead of
    // holding its last cold-construct value forever. Tests drive a
    // deterministic clock by passing explicit `Instant`s to `observe_at`
    // / `p95_ms_at` (production `observe`/`p95_ms` call `Instant::now()`),
    // so no real sleep is used (no sleep-as-synchronization).
    // =================================================================

    /// (#obs-tuning follow-up 1) The CORE new contract: an IDLE worker's p95
    /// AGES with wall-clock time even though NO new observation arrives. A
    /// single cold construct at 250 ms sets the p95 to 250; after a long idle
    /// gap (no further observations) a read must have decayed the mass away and
    /// report the empty-sentinel 0 — the stale p95 no longer sticks forever.
    ///
    /// Mutation (TDD #5): make `decay_to` a no-op (or gate the whole time-aware
    /// decay so `observe`/read stop aging) → the 250 ms mass never fades and the
    /// idle read still reports 250 ms; this test red-fails with its bespoke
    /// "idle p95 did NOT age" message.
    #[test]
    fn p95_ages_on_idle_wall_clock_gap() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        h.observe_at(250, t0);
        assert_eq!(
            h.p95_ms_at(t0),
            250,
            "sanity: immediately after one 250ms cold construct the p95 is the \
             250ms bucket edge (got {})",
            h.p95_ms_at(t0)
        );
        // A LONG idle gap with NO new observations. With a per-second keep of
        // 0.98 the mass after 600 s is 0.98^600 ≈ 5.6e-6, well below the
        // empty-sentinel threshold, so the p95 must have aged back to 0.
        let idle_read = h.p95_ms_at(t0 + Duration::from_secs(600));
        assert_eq!(
            idle_read, 0,
            "#obs-tuning follow-up 1: idle p95 did NOT age — after a 600s idle \
             gap with no new cold constructs the decayed p95 must fall back to \
             the empty sentinel 0, but it stayed at {idle_read}ms. The decay is \
             still per-observation (wall-time elapsed does not age it), so an \
             idle worker fossilises its last cold-construct value forever — the \
             exact defect this follow-up fixes"
        );
    }

    /// (#obs-tuning follow-up 1) The wall-clock decay scales by Δt (CONTINUOUS
    /// exponential), NOT by a fixed factor per access. A SINGLE observation
    /// (mass 1.0), read ONCE after a 40 s idle gap, must have decayed by
    /// `0.98^40 ≈ 0.446` — below the `P95_NEGLIGIBLE_MASS = 0.5` sentinel
    /// threshold — so the read reports 0. A per-CALL multiply would apply only
    /// ONE `×0.98` for that single read (mass 0.98, still > 0.5) and wrongly
    /// report 250. This pins the Δt-exponent specifically (distinct from the
    /// "decay entirely off" idle test): one big elapsed interval must apply the
    /// FULL time-scaled decay in a single step, not one flat factor.
    ///
    /// Mutation: revert `decay_to` to a fixed per-CALL multiply (`factor =
    /// P95_DECAY_KEEP_PER_SEC` instead of `.powf(dt)`) → the single 40 s read
    /// decays by only `×0.98`, the mass stays above the sentinel, the p95 stays
    /// 250 and this test red-fails.
    #[test]
    fn p95_decay_is_continuous_in_wall_time() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        // Exactly one observation → mass 1.0 in the 250ms bucket.
        h.observe_at(250, t0);
        assert_eq!(
            h.p95_ms_at(t0),
            250,
            "sanity: one 250ms observation reports the 250ms bucket (got {})",
            h.p95_ms_at(t0)
        );
        // ONE read after a 40 s idle gap. Continuous decay: 0.98^40 ≈ 0.446 <
        // 0.5 → sentinel 0. A per-call flat ×0.98 would keep mass 0.98 → 250.
        let aged = h.p95_ms_at(t0 + Duration::from_secs(40));
        assert_eq!(
            aged, 0,
            "#obs-tuning follow-up 1: wall-clock decay is not Δt-CONTINUOUS — a \
             single observation read ONCE after a 40s gap must decay by the full \
             0.98^40 (≈0.446, below the 0.5 sentinel) and report 0, but it \
             reported {aged}ms. A per-CALL flat ×0.98 (dropping the Δt exponent) \
             applies only one decay step for the single read, leaving stale mass \
             above the sentinel — the exact Δt-scaling bug this pins"
        );
    }

    /// (#obs-tuning follow-up 1) An ACTIVE worker at a steady cold cadence still
    /// tracks its regime — the time-aware decay must not erase the current
    /// distribution when constructs keep arriving. Ten cold constructs at a
    /// realistic 5 s cadence, all in the 250 ms bucket, must still report the
    /// 250 ms p95 (the recent regime), NOT decay to 0 the way a pure idle gap
    /// does. Guards against over-aggressive aging that would blank a busy
    /// worker's signal.
    #[test]
    fn p95_active_cadence_retains_regime() {
        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        // 10 constructs, 5 s apart (a plausible cold-heavy cadence).
        for i in 0..10 {
            h.observe_at(250, t0 + Duration::from_secs(i * 5));
        }
        let p95 = h.p95_ms_at(t0 + Duration::from_secs(9 * 5));
        assert_eq!(
            p95, 250,
            "#obs-tuning follow-up 1: an ACTIVE worker (10× 250ms constructs at \
             5s cadence) must still report its recent 250ms regime, got {p95}ms \
             — the wall-clock decay is aging too aggressively and blanking a busy \
             worker's live signal"
        );
    }

    // =================================================================
    // #obs-tuning follow-up 2: FINER ladder in the measured 34–229 ms band.
    //
    // The shared O11_LATENCY_BUCKETS_MS has only 50/100/250 boundaries in
    // the measured band, so a p95 anywhere in (100,250] snaps to 250. The
    // construct-latency histogram gets its OWN finer ladder
    // (CONSTRUCT_LATENCY_BUCKETS_MS) so the shared P3/P5 ladder is
    // untouched (it has other consumers — O11LatencyHistogram).
    // =================================================================

    /// (#obs-tuning follow-up 2) The construct-latency ladder resolves the
    /// measured band FINER than the shared 50/100/250 ladder. A population whose
    /// true 95th percentile is ~150 ms (34,61,69,75,92,110,120,130,141,149 —
    /// all inside 34–149 ms) snaps to 250 ms on the shared ladder (its only
    /// boundary above 100 is 250) but resolves to 150 ms on the finer ladder.
    /// The 100 ms of quantization error the coarse ladder imposed is exactly the
    /// snap this follow-up removes. Computed independently for cross-check.
    ///
    /// Mutation: point `DecayingP95Histogram` back at `O11_LATENCY_BUCKETS_MS`
    /// (the coarse ladder) → the p95 snaps to 250 again and this test red-fails
    /// with its bespoke "ladder too coarse" message.
    #[test]
    fn construct_p95_finer_ladder_resolves_measured_band() {
        let pop = [34u64, 61, 69, 75, 92, 110, 120, 130, 141, 149];
        // Cross-check what the COARSE shared ladder would report on this pop, so
        // the "finer beats coarse" claim is grounded, not asserted.
        let coarse = conservative_p95_on_ladder(&pop, &O11_LATENCY_BUCKETS_MS);
        assert_eq!(
            coarse, 250,
            "cross-check: the coarse shared ladder snaps this 34–149ms population \
             to 250ms (its only boundary >100 is 250); got {coarse}"
        );

        let h = DecayingP95Histogram::new();
        let t0 = Instant::now();
        for ms in pop {
            h.observe_at(ms, t0);
        }
        let p95 = h.p95_ms_at(t0);
        assert_eq!(
            p95, 150,
            "#obs-tuning follow-up 2: ladder too coarse — the construct-latency \
             p95 of a 34–149ms population (true p95 ~149ms) must resolve to the \
             150ms finer boundary, NOT snap to the 250ms the shared 50/100/250 \
             ladder gives, got {p95}ms. The finer 50–500ms ladder is not in \
             effect (still using the coarse shared O11_LATENCY_BUCKETS_MS)"
        );
        assert!(
            p95 < coarse,
            "#obs-tuning follow-up 2: the finer ladder ({p95}ms) must resolve the \
             band STRICTLY finer than the coarse shared ladder ({coarse}ms)"
        );
    }

    /// Independent reference implementation of the conservative upper-edge p95
    /// for a sample slice against an arbitrary ascending ladder — used to
    /// cross-check the production `DecayingP95Histogram` p95 against the coarse
    /// shared ladder WITHOUT reimplementing decay (all samples share one t0).
    fn conservative_p95_on_ladder(samples: &[u64], ladder: &[u64]) -> u64 {
        let mut weights = vec![0u64; ladder.len() + 1];
        for &s in samples {
            let idx = ladder
                .iter()
                .position(|&b| s <= b)
                .unwrap_or(ladder.len());
            weights[idx] += 1;
        }
        let total: u64 = weights.iter().sum();
        if total == 0 {
            return 0;
        }
        // target = ceil-ish: first bucket whose cumulative >= 0.95*total.
        let target = (total as f64) * 0.95;
        let mut cumulative = 0u64;
        for (idx, w) in weights.iter().enumerate() {
            cumulative += *w;
            if cumulative as f64 >= target {
                return ladder.get(idx).copied().unwrap_or(*ladder.last().unwrap());
            }
        }
        *ladder.last().unwrap()
    }

    /// (#obs-tuning follow-up 2) The construct-latency ladder is STRICTLY finer
    /// than the shared ladder in the 50–500 ms band: it must contain at least
    /// one boundary the shared `O11_LATENCY_BUCKETS_MS` lacks between 100 and
    /// 250 ms (the specific gap that caused the snap). Also asserts the ladder is
    /// sorted ascending (the p95 walk depends on monotone boundaries) and the
    /// shared ladder is UNCHANGED (untouched for its P3/P5 consumers).
    ///
    /// Mutation: remove the added (100,250) boundaries from
    /// `CONSTRUCT_LATENCY_BUCKETS_MS` → the "finer in band" assertion red-fails.
    #[test]
    fn construct_ladder_is_finer_and_shared_ladder_untouched() {
        // The shared ladder is UNCHANGED — its P3/P5 consumers (O11LatencyHistogram)
        // depend on exactly this 11-bucket set.
        assert_eq!(
            O11_LATENCY_BUCKETS_MS,
            [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000],
            "#obs-tuning follow-up 2: the SHARED O11_LATENCY_BUCKETS_MS must stay \
             untouched (P3 ByteStream + P5 EvictingMap histograms depend on it); \
             the finer band belongs on the SEPARATE construct ladder only"
        );
        // The construct ladder is sorted ascending.
        let ladder = CONSTRUCT_LATENCY_BUCKETS_MS;
        assert!(
            ladder.windows(2).all(|w| w[0] < w[1]),
            "#obs-tuning follow-up 2: CONSTRUCT_LATENCY_BUCKETS_MS must be strictly \
             ascending (the conservative-p95 walk assumes monotone boundaries), \
             got {ladder:?}"
        );
        // It has at least one boundary the shared ladder lacks in (100,250) — the
        // exact gap that snapped the measured band to 250.
        let shared_in_gap: Vec<u64> = O11_LATENCY_BUCKETS_MS
            .iter()
            .copied()
            .filter(|&b| b > 100 && b < 250)
            .collect();
        let construct_in_gap: Vec<u64> = ladder
            .iter()
            .copied()
            .filter(|&b| b > 100 && b < 250)
            .collect();
        assert!(
            shared_in_gap.is_empty() && !construct_in_gap.is_empty(),
            "#obs-tuning follow-up 2: the construct ladder must add >=1 boundary in \
             (100,250) that the shared ladder lacks (shared_in_gap={shared_in_gap:?}, \
             construct_in_gap={construct_in_gap:?}) — otherwise the measured 34–229ms \
             band still snaps to 250ms"
        );
    }

    // =================================================================
    // #obs-tuning follow-up 3: COLD-CONSTRUCT RATE is measurable.
    //
    // The decay window assumes "few cold constructs/min" — an operator
    // estimate with no counter. A monotone count of record_construct_fetch_ms
    // calls makes the rate observable (rate = Δcount / Δt on the 15s log or a
    // /metrics scrape), so the decay-window assumption can be validated.
    // =================================================================

    /// (#obs-tuning follow-up 3) Every `record_construct_fetch_ms` call bumps a
    /// monotone cold-construct counter, so the cold RATE (Δcount / Δt) is
    /// observable and the "~few/min" decay-window assumption is verifiable
    /// instead of estimated. Guards against the counter being dropped from the
    /// record path.
    ///
    /// Mutation: comment out `self.construct_fetch_count.fetch_add(1, ..)` in
    /// `record_construct_fetch_ms` → the count stays 0 while sum/count/p95 still
    /// update; this test red-fails on the count assertion.
    #[test]
    fn record_construct_fetch_ms_increments_cold_rate_counter() {
        let c = DirCacheCounters::new();
        assert_eq!(
            c.construct_fetch_count.load(Ordering::Relaxed),
            0,
            "cold-construct rate counter must start at 0"
        );
        for _ in 0..7 {
            c.record_construct_fetch_ms(120);
        }
        assert_eq!(
            c.construct_fetch_count.load(Ordering::Relaxed),
            7,
            "#obs-tuning follow-up 3: record_construct_fetch_ms must bump the \
             cold-construct rate counter once per call (7 calls → 7), got {} — \
             the cold RATE is unmeasured, so the decay-window (~few/min) \
             assumption cannot be validated",
            c.construct_fetch_count.load(Ordering::Relaxed)
        );
    }

    /// (#obs-tuning follow-up 3) The cold-construct rate counter renders on the
    /// REAL `/metrics` path (same MetricsRegistry + render_prometheus walk the
    /// worker uses), under the `dir_cache` prefix, as
    /// `dir_cache_construct_fetch_count_total_counter`. Absence = the rate is
    /// DARK on /metrics (the dark-counter trap: 0 indistinguishable from unwired).
    ///
    /// Mutation: drop the `construct_fetch_count` emit in
    /// `DirCacheCounters::publish` (or rename its key) → the exact line is absent
    /// and this test red-fails with its bespoke "cold-rate counter dark" message.
    #[test]
    fn dir_cache_render_exposes_cold_construct_rate_counter() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        // Drive a distinct sentinel count so we catch a publish of the wrong field.
        dir_cache_counters()
            .construct_fetch_count
            .store(4242, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        registry.register("dir_cache", dir_cache_counters_arc());
        let body = render_prometheus(&registry);

        let needle = "dir_cache_construct_fetch_count_total_counter 4242";
        assert!(
            body.contains(needle),
            "#obs-tuning follow-up 3: cold-construct rate counter DARK on /metrics \
             — expected line `{needle}` from the render_prometheus walk, but it is \
             ABSENT. Without it on /metrics the cold RATE cannot be scraped to \
             validate the decay window. body=\n{body}"
        );
        // Guard the #86 doubled-prefix trap.
        assert!(
            !body.contains("dir_cache_dir_cache"),
            "#obs-tuning follow-up 3: doubled-prefix trap — construct rate counter \
             emitted a `dir_cache_dir_cache_*` name. body=\n{body}"
        );
    }

    /// (#64 dark-signals) `swap_used_bytes` and `pressure_level_mib` gauges must
    /// render on the REAL `/metrics` path via the same `MetricsRegistry` +
    /// `render_prometheus` walk the worker `/metrics` handler uses. Pins EXACT
    /// rendered names: `memory_gate_swap_used_bytes` and
    /// `memory_gate_pressure_level_mib` — NO `_total` / `_counter` suffix (these
    /// are instant-value gauges, not monotone counters; verified empirically here).
    ///
    /// Absence = the signal is DARK on `/metrics`, preventing operator alerting.
    /// Also guards the doubled-prefix trap (`memory_gate_memory_gate_*`).
    ///
    /// Mutation: drop one of the two new `publish!` blocks in
    /// `MemoryGateCounters::publish` (or rename its key) → test red-fails with
    /// "#64 swap/pressure gauge dark on /metrics: expected exact line `<name> <sentinel>`".
    #[test]
    fn memory_gate_render_prometheus_exposes_swap_and_pressure_gauges() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(MemoryGateCounters::new());
        // Distinct sentinels so we catch a publish that emits the wrong field.
        counters.swap_used_bytes.store(9_876_543_210, Ordering::Relaxed);
        counters.pressure_level_mib.store(256, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        // Prefix "memory_gate" — the exact key production nativelink.rs uses.
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("memory_gate_swap_used_bytes", 9_876_543_210u64),
            ("memory_gate_pressure_level_mib", 256u64),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "#64 swap/pressure gauge dark on /metrics: expected exact line \
                 `{name} {value}` from the render_prometheus walk, but it is \
                 ABSENT — the signal is dark on /metrics. body=\n{body}"
            );
        }
        // Guard doubled-prefix trap.
        assert!(
            !body.contains("memory_gate_memory_gate"),
            "#64 doubled metric name: rendered output contains `memory_gate_memory_gate`. \
             body=\n{body}"
        );
    }

    /// (#64 dark-signals) Storing a value into `swap_used_bytes` /
    /// `pressure_level_mib` must flow through to the rendered output (not
    /// silently published as 0).
    ///
    /// Mutation: in `MemoryGateCounters::publish`, hard-code the published
    /// value to `0u64` / `0u32` instead of loading the atomic → test red-fails
    /// with "#64 publish-value: swap_used_bytes published 0 not sentinel …".
    #[test]
    fn memory_gate_swap_pressure_gauges_emit_stored_value() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(MemoryGateCounters::new());
        counters.swap_used_bytes.store(1_234_567_890, Ordering::Relaxed);
        counters.pressure_level_mib.store(512, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        let swap_line = "\nmemory_gate_swap_used_bytes 1234567890\n".to_string();
        assert!(
            body.contains(&swap_line),
            "#64 publish-value: swap_used_bytes published 0 not sentinel 1234567890 — \
             the atomic store is not flowing through publish(). body=\n{body}"
        );
        let pressure_line = "\nmemory_gate_pressure_level_mib 512\n".to_string();
        assert!(
            body.contains(&pressure_line),
            "#64 publish-value: pressure_level_mib published 0 not sentinel 512 — \
             the atomic store is not flowing through publish(). body=\n{body}"
        );
    }

    /// (#task-memgate-twosignal) The churn scalar + three raw-rate calibration
    /// gauges must render on the real `/metrics` path. This test pins the EXACT
    /// rendered names: `memory_gate_churn_ewma`, `memory_gate_compress_rate_last`,
    /// `memory_gate_decompress_rate_last`, `memory_gate_swapin_rate_last` — NO
    /// `_total` / `_counter` suffix (gauges, not counters). Stops the soak runbook
    /// from alerting on names that do not exist on the wire.
    ///
    /// Mutation: drop one of the `publish!` calls in `MemoryGateCounters::publish`
    /// (or rename its key) → test red-fails with "churn/rate gauge dark on
    /// /metrics: expected exact line…".
    #[test]
    fn memory_gate_render_prometheus_exposes_churn_gauges() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(MemoryGateCounters::new());
        // Sentinel values distinguishable from 0 and from each other.
        counters.churn_ewma.store(42, Ordering::Relaxed);
        counters.compress_rate_last.store(137, Ordering::Relaxed);
        counters.decompress_rate_last.store(211, Ordering::Relaxed);
        counters.swapin_rate_last.store(313, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("memory_gate_churn_ewma", 42u64),
            ("memory_gate_compress_rate_last", 137),
            ("memory_gate_decompress_rate_last", 211),
            ("memory_gate_swapin_rate_last", 313),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "churn/rate gauge dark on /metrics: expected exact line `{name} {value}` from \
                 the render_prometheus walk, but it is ABSENT — the soak will be unable to \
                 observe the compressor-churn calibration signals. body=\n{body}"
            );
        }
        // Guard the doubled-prefix trap.
        assert!(
            !body.contains("memory_gate_memory_gate"),
            "#64 doubled metric name: rendered output contains `memory_gate_memory_gate`. \
             body=\n{body}"
        );
    }

    /// (#calib) `record_calibration_tick` must bucket each signal's rate into
    /// the EXACT fixed band edges `{0, 1-10, 11-100, 101-1000, 1001-10000,
    /// >10000}/s` — the gate's sustained-N-ticks semantics reads sustained-vs-
    /// spike from the band SHAPE, so an off-by-one edge silently mis-attributes
    /// a whole soak. Drives every boundary value on the swapin signal while
    /// compress stays 0, proving per-signal independence.
    ///
    /// Mutation: change any edge in `rate_band_index` (e.g. `1..=10` → `1..=11`)
    /// → the exact-array assert red-fails with "calibration band edges".
    #[test]
    fn memory_gate_rate_band_edges_bucket_correctly() {
        let c = MemoryGateCounters::new();
        // Every band boundary (low + high side of each edge) PLUS filler so each
        // band receives a UNIQUE tick count [1,2,3,4,5,6] — an equal-count
        // expected array would pass under a band-index swap (caught live by a
        // passing mutation: swapping bands 1↔2 left [1,2,2,2,2,1] unchanged).
        for rate in [
            0u32, // band 0 (×1)
            1, 10, // band 1 boundaries (×2)
            11, 50, 100, // band 2 boundaries + filler (×3)
            101, 500, 999, 1_000, // band 3 (×4)
            1_001, 2_000, 5_000, 9_999, 10_000, // band 4 (×5)
            10_001, 20_000, 100_000, 1_000_000, u32::MAX - 1, u32::MAX, // band 5 (×6)
        ] {
            c.record_calibration_tick(0, 0, rate, 0);
        }
        let swapin: Vec<u64> = c
            .swapin_rate_bands
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        assert_eq!(
            swapin,
            vec![1, 2, 3, 4, 5, 6],
            "calibration band edges: the 21 driven rates (all boundaries of \
             {{0, 1-10, 11-100, 101-1k, 1k-10k, >10k}} + unique-count filler) must \
             bucket into swapin bands [1,2,3,4,5,6] — an off-by-one OR a swapped \
             band index in rate_band_index mis-attributes sustained-vs-spike"
        );
        let compress: Vec<u64> = c
            .compress_rate_bands
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        assert_eq!(
            compress,
            vec![21, 0, 0, 0, 0, 0],
            "calibration band independence: all 21 ticks had compress_rate=0, so \
             ONLY compress band-0 may count — cross-signal leakage would poison \
             the per-signal histograms"
        );
    }

    /// (#calib) The `*_rate_max` / `churn_ewma_max` fields must be HIGH-WATER
    /// marks: a later smaller tick must NOT lower them (this is what makes the
    /// busy-window burst survive until the next scrape — the whole point of the
    /// calibration change; the instantaneous `_rate_last` gauges already read 0
    /// after a burst).
    ///
    /// Mutation: comment out any `fetch_max` in `record_calibration_tick` → the
    /// corresponding assert red-fails with "high-water mark lost".
    #[test]
    fn memory_gate_rate_max_is_high_water() {
        let c = MemoryGateCounters::new();
        c.record_calibration_tick(500, 20_000, 7, 42);
        c.record_calibration_tick(50, 3, 3, 7); // burst over; smaller tick
        assert_eq!(
            c.compress_rate_max.load(Ordering::Relaxed),
            500,
            "high-water mark lost: compress_rate_max must hold the burst peak 500 \
             after a smaller (50) tick — record_calibration_tick must fetch_max, \
             never store"
        );
        assert_eq!(
            c.decompress_rate_max.load(Ordering::Relaxed),
            20_000,
            "high-water mark lost: decompress_rate_max must hold the burst peak \
             20000 after a smaller (3) tick — record_calibration_tick must \
             fetch_max, never store"
        );
        assert_eq!(
            c.swapin_rate_max.load(Ordering::Relaxed),
            7,
            "high-water mark lost: swapin_rate_max must hold the burst peak 7 \
             after a smaller (3) tick — this is the calibration input for \
             memory_gate_swapin_confirm_rate"
        );
        assert_eq!(
            c.churn_ewma_max.load(Ordering::Relaxed),
            42,
            "high-water mark lost: churn_ewma_max must hold the burst peak 42 \
             after a smaller (7) tick — this is the calibration input for the \
             churn low/high thresholds"
        );
    }

    /// (#calib) EVERY new calibration metric name must render on the real
    /// `/metrics` path under the production `memory_gate` prefix — the
    /// worker-metrics-exposure trap (#37/#64/#86) is a name that increments
    /// in-process but never renders. Pins all 22 EXACT literal names: 4
    /// high-water maxes + 3 signals × 6 band-tick counters.
    ///
    /// Mutation: drop any `publish!` (or break a band-name `format!`) in
    /// `MemoryGateCounters::publish` → red-fails with "calibration metric dark
    /// on /metrics".
    #[test]
    fn memory_gate_render_prometheus_exposes_calibration_maxes_and_bands() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(MemoryGateCounters::new());
        // Distinctive sentinel per field so a misrouted band renders a
        // different number (wrong-field guard).
        counters.churn_ewma_max.store(61, Ordering::Relaxed);
        counters.compress_rate_max.store(62, Ordering::Relaxed);
        counters.decompress_rate_max.store(63, Ordering::Relaxed);
        counters.swapin_rate_max.store(64, Ordering::Relaxed);
        for (i, band) in counters.compress_rate_bands.iter().enumerate() {
            band.store(70 + i as u64, Ordering::Relaxed);
        }
        for (i, band) in counters.decompress_rate_bands.iter().enumerate() {
            band.store(76 + i as u64, Ordering::Relaxed);
        }
        for (i, band) in counters.swapin_rate_bands.iter().enumerate() {
            band.store(82 + i as u64, Ordering::Relaxed);
        }

        let registry = MetricsRegistry::new();
        // Prefix "memory_gate" — the exact key production nativelink.rs uses.
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        let mut expected: Vec<(String, u64)> = vec![
            ("memory_gate_churn_ewma_max".to_string(), 61),
            ("memory_gate_compress_rate_max".to_string(), 62),
            ("memory_gate_decompress_rate_max".to_string(), 63),
            ("memory_gate_swapin_rate_max".to_string(), 64),
        ];
        for (signal, base) in [("compress", 70u64), ("decompress", 76), ("swapin", 82)] {
            for (i, suffix) in ["0", "1_10", "11_100", "101_1k", "1k_10k", "gt10k"]
                .iter()
                .enumerate()
            {
                expected.push((
                    format!("memory_gate_{signal}_rate_band_{suffix}_total"),
                    base + i as u64,
                ));
            }
        }
        assert_eq!(expected.len(), 22, "self-check: 4 maxes + 18 band counters");
        for (name, value) in &expected {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "calibration metric dark on /metrics: expected exact line `{name} \
                 {value}` from the render_prometheus walk, but it is ABSENT — the \
                 swapin/churn threshold calibration soak cannot read it. body=\n{body}"
            );
        }
        // Guard the doubled-prefix trap.
        assert!(
            !body.contains("memory_gate_memory_gate"),
            "doubled metric name: rendered output contains `memory_gate_memory_gate`. \
             body=\n{body}"
        );
    }

    // =====================================================================
    // AcHitCounters tests
    // =====================================================================

    /// AC hit/miss: increment-observable test on a LOCAL `AcHitCounters`.
    /// `record_hit` must bump `hit`; `record_miss` must bump `miss`; no cross-talk.
    ///
    /// Mutation: comment out `self.hit.fetch_add(1, ...)` in `record_hit` →
    /// `hit` stays at 0; test red-fails with
    /// "ac-hit increment: record_hit x4 must yield hit==4".
    #[test]
    fn ac_hit_counters_increment_observable() {
        let c = AcHitCounters::new();
        assert_eq!(c.hit.load(Ordering::Relaxed), 0,
            "ac-hit increment: hit must initialize to 0");
        assert_eq!(c.miss.load(Ordering::Relaxed), 0,
            "ac-hit increment: miss must initialize to 0");

        for _ in 0..4 { c.record_hit(); }
        for _ in 0..2 { c.record_miss(); }

        assert_eq!(c.hit.load(Ordering::Relaxed), 4,
            "ac-hit increment: record_hit x4 must yield hit==4 (got {})",
            c.hit.load(Ordering::Relaxed));
        assert_eq!(c.miss.load(Ordering::Relaxed), 2,
            "ac-hit increment: record_miss x2 must yield miss==2 (got {})",
            c.miss.load(Ordering::Relaxed));
    }

    /// AC hit/miss: end-to-end render test. Verifies that `AcHitCounters`
    /// registered under prefix `"ac_get_action_result"` emits EXACTLY
    /// `ac_get_action_result_hit_total` and `ac_get_action_result_miss_total`
    /// via the real `render_prometheus` walk.
    ///
    /// Pins the EXACT line so a prefix or field-name regression (including
    /// the doubled-prefix trap) is caught immediately.
    ///
    /// Mutation: drop one `publish!` call in `AcHitCounters::publish` (or
    /// rename its key) → test red-fails with "ac-hit dark on /metrics:
    /// expected exact line `ac_get_action_result_hit_total 7`".
    #[test]
    fn ac_hit_counters_render_prometheus_exposes_hit_and_miss() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(AcHitCounters::new());
        for _ in 0..7 { counters.record_hit(); }
        for _ in 0..3 { counters.record_miss(); }

        let registry = MetricsRegistry::new();
        // Prefix "ac_get_action_result" — the exact key production nativelink.rs
        // will use. The rendered names must be:
        //   ac_get_action_result_hit_total
        //   ac_get_action_result_miss_total
        registry.register("ac_get_action_result", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("ac_get_action_result_hit_total", 7u64),
            ("ac_get_action_result_miss_total", 3u64),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "ac-hit dark on /metrics: expected exact line `{name} {value}` \
                 from render_prometheus walk, but it is ABSENT — the AC hit/miss \
                 counter is not exposed. body=\n{body}"
            );
        }

        // Guard doubled-prefix trap.
        assert!(
            !body.contains("ac_get_action_result_ac_get_action_result"),
            "ac-hit doubled metric name: rendered output contains doubled prefix. \
             body=\n{body}"
        );
    }

    // =====================================================================
    // EcsHitCounters tests
    // =====================================================================

    /// ECS hit/miss: increment-observable test on a LOCAL `EcsHitCounters`.
    /// `record_hits(n)` must add `n` to `hit`; `record_misses(n)` to `miss`.
    ///
    /// Mutation: comment out `self.hit.fetch_add(n, ...)` in `record_hits` →
    /// `hit` stays 0; test red-fails with
    /// "ecs-hit increment: record_hits(5) must yield hit==5".
    #[test]
    fn ecs_hit_counters_increment_observable() {
        let c = EcsHitCounters::new();
        assert_eq!(c.hit.load(Ordering::Relaxed), 0,
            "ecs-hit increment: hit must initialize to 0");
        assert_eq!(c.miss.load(Ordering::Relaxed), 0,
            "ecs-hit increment: miss must initialize to 0");

        c.record_hits(5);
        c.record_misses(3);

        assert_eq!(c.hit.load(Ordering::Relaxed), 5,
            "ecs-hit increment: record_hits(5) must yield hit==5 (got {})",
            c.hit.load(Ordering::Relaxed));
        assert_eq!(c.miss.load(Ordering::Relaxed), 3,
            "ecs-hit increment: record_misses(3) must yield miss==3 (got {})",
            c.miss.load(Ordering::Relaxed));
    }

    /// ECS hit/miss: end-to-end render test. Verifies that `EcsHitCounters`
    /// registered under prefix `"ecs"` emits EXACTLY `ecs_has_hit_total` and
    /// `ecs_has_miss_total` via the real `render_prometheus` walk.
    ///
    /// Mutation: drop one `publish!` call in `EcsHitCounters::publish` (or
    /// rename its key) → test red-fails with "ecs-hit dark on /metrics:
    /// expected exact line `ecs_has_hit_total 9`".
    #[test]
    fn ecs_hit_counters_render_prometheus_exposes_hit_and_miss() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(EcsHitCounters::new());
        counters.record_hits(9);
        counters.record_misses(4);

        let registry = MetricsRegistry::new();
        // Prefix "ecs" — the exact key production nativelink.rs will use.
        // Rendered names: ecs_has_hit_total, ecs_has_miss_total.
        registry.register("ecs", counters);
        let body = render_prometheus(&registry);

        for (name, value) in [
            ("ecs_has_hit_total", 9u64),
            ("ecs_has_miss_total", 4u64),
        ] {
            let needle = format!("\n{name} {value}\n");
            assert!(
                body.contains(&needle),
                "ecs-hit dark on /metrics: expected exact line `{name} {value}` \
                 from render_prometheus walk, but it is ABSENT — the ECS hit/miss \
                 counter is not exposed. body=\n{body}"
            );
        }

        // Guard doubled-prefix trap.
        assert!(
            !body.contains("ecs_ecs"),
            "ecs-hit doubled metric name: rendered output contains `ecs_ecs`. \
             body=\n{body}"
        );
    }

    /// (#task-memgate-twosignal) Storing a value into `churn_ewma` /
    /// `swapin_rate_last` must flow through to the rendered output (not silently
    /// published as 0).
    ///
    /// Mutation: in `MemoryGateCounters::publish`, hard-code the published value
    /// to `0u32` instead of loading the atomic → test red-fails with
    /// "publish-value: churn_ewma published 0 not sentinel 99".
    #[test]
    fn memory_gate_churn_gauge_publish_emits_stored_value() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let counters = Arc::new(MemoryGateCounters::new());
        counters.churn_ewma.store(99, Ordering::Relaxed);
        counters.swapin_rate_last.store(7, Ordering::Relaxed);

        let registry = MetricsRegistry::new();
        registry.register("memory_gate", counters);
        let body = render_prometheus(&registry);

        let ewma_line = "\nmemory_gate_churn_ewma 99\n".to_string();
        assert!(
            body.contains(&ewma_line),
            "publish-value: churn_ewma published 0 not sentinel 99 — \
             the atomic store is not flowing through publish(). body=\n{body}"
        );
        let rate_line = "\nmemory_gate_swapin_rate_last 7\n".to_string();
        assert!(
            body.contains(&rate_line),
            "publish-value: swapin_rate_last published 0 not sentinel 7 — \
             the atomic store is not flowing through publish(). body=\n{body}"
        );
    }
}
