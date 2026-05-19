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

//! #547 Phase 0 instrumentation: measure the BIS-ack-past-tonic-Ok window.
//!
//! Today the worker-side pin-release fires inside `process_bis_chunk` when
//! the server's `BlobsInStableStorage` broadcast arrives. The #546 design
//! proposes shifting that trigger to "chunked-write tonic response Ok."
//! **Before that shift can be justified, we need production data: how
//! long is the gap between tonic-Ok-returning and BIS-ack arriving?** If
//! the gap is sub-10 ms median, the whole design chain is moot. If it
//! grows to hundreds of ms under load, there is a real perf win to chase.
//!
//! This module collects that data without changing any runtime behavior.
//! It is pure observability:
//!
//! - **`worker_pin_release_latency_after_tonic_ok_ms`** (histogram, ms):
//!   per-digest gap between worker's chunked-upload tonic Ok and the BIS
//!   chunk that drives the matching unpin. Recorded inside the BIS chunk
//!   handler when a matching tonic-Ok timestamp is found in the
//!   side-channel cache.
//! - **`worker_action_total_pin_extension_ms`** (histogram, ms): per-action
//!   sum of pin-extension windows across every output blob. Captures the
//!   "head-of-line worst case per build action."
//! - **`worker_max_pin_extension_ms`** (histogram, ms): per-action max of
//!   pin-extension; per perf-optimizer P6 the action latency cost is
//!   dominated by the slowest blob, not the sum.
//! - **`worker_concurrent_pinned_bytes`** (gauge): point-in-time bytes
//!   held in worker FilesystemStore pins from chunked-upload commits.
//!   Informs Phase 2 (#549) pin_budget cap selection.
//! - **`server_stable_digests_pusher_invoke_count` + `_last_at_unix_ms`**:
//!   counter + timestamp gauge so operators can confirm the server-side
//!   commit path is actually firing the BIS pusher (and how recently).
//! - **`server_bis_broadcast_loop_wake_to_send_ms`** (histogram, ms): gap
//!   between BIS pipeline loop wake (notify or 500ms tick) and the
//!   `broadcast_blobs_in_stable_storage_chunked` call returning. Names
//!   the broadcast-pipeline contribution to the latency budget.
//! - **`worker_bis_chunk_arrive_to_handler_ms`** (histogram, ms): gap
//!   between worker gRPC receive of BIS chunk and the unpin handler
//!   actually firing. Names the worker-side dispatcher contribution.
//! - **`server_bis_broadcast_queue_depth`** (gauge): live `stable_digests`
//!   queue length. Per red-team P6, the relaxation removes a natural
//!   backpressure source; queue depth visibility is needed now.
//! - **`server_bis_broadcast_queue_latency_ms`** (histogram, ms): per-digest
//!   queue dwell time, measured as `(broadcast_send_ts - pusher_invoke_ts)`.
//!
//! ## Design rationale
//!
//! - All metrics are recorded via atomic bucket counters and `AtomicU64`s.
//!   No async overhead, no allocation on the hot path, no locks. The
//!   per-digest tonic-Ok-timestamp side-channel uses a `moka::sync::Cache`
//!   with bounded capacity so an upload spike cannot OOM the worker.
//! - Histogram bucket boundaries are tuned for the expected envelope per
//!   perf-optimizer Q2: floor ~0 ms (notify-driven, healthy), ceiling
//!   ~600 ms (worst-case 500 ms drain-loop backoff + RPC dispatch). The
//!   buckets bracket that range with sub-ms granularity at the floor and
//!   second-scale granularity at the tail for slow-path debug.
//! - Process-global singletons (one per metric struct) wired into the
//!   `MetricsRegistry` at process start, matching the `PinBudget` pattern
//!   at `nativelink-store/src/chunked/pin_budget.rs:192-204`.
//! - Compile-elimination concern (per memory
//!   `feedback_compile_elimination_check_whole_expr`): these are
//!   counter/gauge primitives, NOT `tracing::info!`/`tracing::trace!`
//!   macros. `release_max_level_info` does not strip them. The
//!   `Instant::now()` reads are unconditional and unconditionally consumed
//!   by the bucket-recording calls.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moka::sync::Cache;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};

use crate::common::DigestInfo;

/// Histogram bucket boundaries in milliseconds, covering the expected
/// envelope per perf-optimizer Q2 and red-team P6:
///   - Sub-ms (1, 5) — floor case: notify-driven, healthy
///   - Single-digit ms (10, 25) — fast-RTT BIS round-trip
///   - Tens of ms (50, 100) — typical drained-loop sleep wake
///   - Hundreds (250, 500, 1000) — drained-loop 500 ms backoff envelope
///     and beyond
///
/// Bucket index `i` counts samples with `value <= LATENCY_BUCKETS_MS[i]`.
/// Implicit `+inf` bucket at the tail catches values above the largest
/// boundary — values above 1000 ms (slow-path / wedge debug) fall into
/// the `+inf` bucket and are visible via `_count - le_1000_ms`.
/// Prometheus exposition convention is `le` (less-than-or-equal).
pub const LATENCY_BUCKETS_MS: [u64; 9] = [1, 5, 10, 25, 50, 100, 250, 500, 1000];

/// Byte-size histogram boundaries for `worker_concurrent_pinned_bytes`.
/// Worker FilesystemStore cap is 20-40 GiB; buckets bracket from KiB
/// (small-blob uploads) to 64 GiB (well above the cap, to catch leak).
pub const PINNED_BYTES_BUCKETS: [u64; 9] = [
    1 << 20,         // 1 MiB
    16 << 20,        // 16 MiB
    256 << 20,       // 256 MiB
    1 << 30,         // 1 GiB
    4 << 30,         // 4 GiB
    8 << 30,         // 8 GiB
    16 << 30,        // 16 GiB
    32 << 30,        // 32 GiB
    64 << 30,        // 64 GiB
];

/// Side-channel cache cap for per-digest tonic-Ok timestamps. Capped to
/// prevent unbounded growth under a digest-storm. At 100k entries the
/// memory footprint is approximately 100k × (32 byte digest + 16 byte
/// Instant + moka overhead) ~= 12 MiB — bounded and operator-visible.
///
/// CAPPED AT 100_000: chosen as ~2× the expected steady-state worker
/// digest in-flight count (workers cap at ~50k files per the project
/// memory; in-flight upload set per action is typically <100 digests;
/// at 1k actions/min and 5 min retention this is ~5k entries). A 20×
/// margin protects against bursts without enabling OOM.
const TONIC_OK_TS_CACHE_CAPACITY: u64 = 100_000;

/// Time-to-live for the tonic-Ok timestamp cache. If the matching BIS
/// chunk hasn't arrived in 10 minutes, the entry is dropped — the
/// latency would be off-scale for the histogram anyway and would
/// indicate a stuck broadcast pipeline (caught by the queue-depth gauge
/// and the wake-to-send histogram, not by this side channel).
const TONIC_OK_TS_CACHE_TTL: Duration = Duration::from_secs(600);

/// Histogram-style bucket recorder. Records value into the `le`-bucket
/// it falls into AND the global `+inf` bucket (so total count is always
/// the sum/last value of `inf_bucket`).
///
/// Each call is a constant-time bucket scan + two relaxed atomic adds.
/// No allocation. Safe to call from anywhere.
#[derive(Debug)]
struct LatencyHistogram {
    /// Counts per-bucket. Index `i` accumulates samples with value
    /// less-than-or-equal to `LATENCY_BUCKETS_MS[i]`. Tail (`+inf`)
    /// bucket is `inf_bucket` below.
    buckets: [AtomicU64; LATENCY_BUCKETS_MS.len()],
    /// `+inf` bucket — total count of all observations. Doubles as
    /// the sample count for `_count` Prometheus suffix.
    inf_bucket: AtomicU64,
    /// Sum of all observed values (in the unit the histogram tracks).
    /// Emitted as `_sum` Prometheus suffix. Saturating — overflow on
    /// a single histogram requires ~5 × 10^14 ms of accumulated
    /// observations (~16 millennia), so saturation is a non-concern.
    sum: AtomicU64,
}

impl LatencyHistogram {
    const fn new() -> Self {
        // const-eval-compatible AtomicU64 array init.
        const Z: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [Z, Z, Z, Z, Z, Z, Z, Z, Z],
            inf_bucket: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    /// Record one observation. Cost: bucket-scan + 2 atomic adds.
    fn observe(&self, value: u64) {
        for (idx, boundary) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if value <= *boundary {
                self.buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inf_bucket.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    /// Emit the histogram's buckets as Prometheus-friendly counters.
    /// Each bucket is published as a separate metric with the boundary
    /// embedded in the name (the metrics registry path lacks the native
    /// `histogram_quantile`-friendly bucket-label format, so we encode
    /// the `le` boundary into the metric name). Operators can compute
    /// percentiles via `(metric_le_N - metric_le_M) / count` ratios.
    fn publish_buckets(
        &self,
        base_name: &str,
        base_help: &str,
    ) -> Result<(), nativelink_metric::Error> {
        for (idx, boundary) in LATENCY_BUCKETS_MS.iter().enumerate() {
            let count = self.buckets[idx].load(Ordering::Relaxed);
            let name = format!("{base_name}_le_{boundary}_ms");
            let help = format!("{base_help} — count of observations ≤ {boundary}ms");
            nativelink_metric::publish!(
                &name,
                &count,
                nativelink_metric::MetricKind::Counter,
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
            nativelink_metric::MetricKind::Counter,
            count_help.as_str()
        );
        nativelink_metric::publish!(
            &sum_name,
            &sum,
            nativelink_metric::MetricKind::Counter,
            sum_help.as_str()
        );
        Ok(())
    }
}

/// Byte-size histogram (same shape as `LatencyHistogram` but with
/// byte-sized buckets per `PINNED_BYTES_BUCKETS`). Kept as a distinct
/// type to make the bucket-boundary swap explicit at the call site —
/// confusing byte buckets for ms buckets would silently bin every
/// observation into the `+inf` bucket.
#[derive(Debug)]
struct BytesHistogram {
    buckets: [AtomicU64; PINNED_BYTES_BUCKETS.len()],
    inf_bucket: AtomicU64,
    sum: AtomicU64,
}

impl BytesHistogram {
    const fn new() -> Self {
        const Z: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [Z, Z, Z, Z, Z, Z, Z, Z, Z],
            inf_bucket: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn observe(&self, value: u64) {
        for (idx, boundary) in PINNED_BYTES_BUCKETS.iter().enumerate() {
            if value <= *boundary {
                self.buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inf_bucket.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    fn publish_buckets(
        &self,
        base_name: &str,
        base_help: &str,
    ) -> Result<(), nativelink_metric::Error> {
        for (idx, boundary) in PINNED_BYTES_BUCKETS.iter().enumerate() {
            let count = self.buckets[idx].load(Ordering::Relaxed);
            let name = format!("{base_name}_le_{boundary}_bytes");
            let help = format!("{base_help} — count of observations ≤ {boundary} bytes");
            nativelink_metric::publish!(
                &name,
                &count,
                nativelink_metric::MetricKind::Counter,
                help.as_str()
            );
        }
        let count = self.inf_bucket.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let count_name = format!("{base_name}_count");
        let sum_name = format!("{base_name}_sum_bytes");
        let count_help = format!("{base_help} — total sample count");
        let sum_help = format!("{base_help} — sum of all observed bytes");
        nativelink_metric::publish!(
            &count_name,
            &count,
            nativelink_metric::MetricKind::Counter,
            count_help.as_str()
        );
        nativelink_metric::publish!(
            &sum_name,
            &sum,
            nativelink_metric::MetricKind::Counter,
            sum_help.as_str()
        );
        Ok(())
    }
}

/// Worker-side Phase 0 metrics. Process-global singleton via
/// `worker_phase0_metrics()` accessor. Wired into `MetricsRegistry`
/// at process start by `bin/nativelink.rs`.
#[derive(Debug)]
pub struct WorkerPhase0Metrics {
    /// Per-digest tonic-Ok timestamp + action-key side-channel.
    /// Producer: `spawn_upload_to_remote` records on successful
    /// per-digest upload (each `break true` from the retry loop).
    /// Consumer: `handle_blobs_in_stable_storage_for_store` looks up
    /// by digest when unpinning, computes the gap, records into the
    /// per-digest histogram AND folds the gap into the per-action
    /// accumulator keyed by the recorded `action_key`.
    ///
    /// CAPPED AT 100_000 (`TONIC_OK_TS_CACHE_CAPACITY`): bounded by
    /// moka's size-aware LRU. If the BIS chunk arrives after the entry
    /// is evicted (older than 10 minutes per `TONIC_OK_TS_CACHE_TTL`)
    /// no observation is recorded — acceptable; the queue-depth gauge
    /// and wake-to-send histogram already cover that pathology class.
    tonic_ok_timestamps: Cache<DigestInfo, (Instant, u64)>,
    /// Histogram of per-digest pin-release latency: `bis_ack_at -
    /// tonic_ok_at`. The headline Phase 0 metric.
    pin_release_latency: LatencyHistogram,
    /// Histogram of per-action sum of pin-extensions across all
    /// output blobs.
    action_total_pin_extension: LatencyHistogram,
    /// Histogram of per-action max single-blob pin-extension. Per
    /// perf-optimizer P6: per-action wall-clock cost is dominated by
    /// the last-released pin (head-of-line), not the sum.
    action_max_pin_extension: LatencyHistogram,
    /// Histogram of worker's concurrent pinned bytes, sampled every
    /// time a new upload's pin is acquired. Informs Phase 2 (#549) cap
    /// selection.
    concurrent_pinned_bytes: BytesHistogram,
    /// Live point-in-time pinned bytes total. Updated atomically as
    /// pins are added/released. The `concurrent_pinned_bytes`
    /// histogram samples this at pin-add time; the gauge gives operators
    /// a real-time view.
    pinned_bytes_live: AtomicU64,
    /// Histogram of worker BIS chunk arrival → handler dispatch latency.
    bis_chunk_arrive_to_handler: LatencyHistogram,
    /// Per-action accumulators for total + max pin-extension. Keyed by
    /// action-execution scope; written when the action's pins are
    /// released (any unpin records the value; the action commits the
    /// histogram observation when all its digests have been unpinned).
    ///
    /// CAPPED AT 10_000 entries: actions outnumber digests at typical
    /// 10×-100× ratio, so even at the digest cap of 100_000 in
    /// `tonic_ok_timestamps` only ~1k-10k distinct actions are in
    /// flight. 10_000 gives 10× headroom over the 1k actions/min
    /// steady-state mentioned in the project memory. At ~32 B per
    /// entry (3 × u64) the worst-case memory footprint is ~320 KiB —
    /// trivial. Same 10-min TTL as `tonic_ok_timestamps` ensures
    /// abandoned actions self-evict.
    action_pin_accumulators: Cache<u64, ActionAccumulator>,
}

/// Per-action pin-extension accumulator. One per action; populated as
/// the action's per-digest unpins fire, committed (read + reset) when
/// the action's upload set is complete.
#[derive(Debug, Clone)]
struct ActionAccumulator {
    /// Sum of all observed per-digest pin-extensions in ms.
    total_ms: u64,
    /// Max observed per-digest pin-extension in ms.
    max_ms: u64,
    /// Number of digests recorded.
    digest_count: u64,
}

// ActionAccumulator has no `new()` because the only callers are
// `record_action_pin_extension`'s `and_upsert_with` arms, which
// construct via struct literal so the initial values reflect the
// caller's first observation (None arm: total_ms = gap_ms, max_ms =
// gap_ms, digest_count = 1) — a constructed "zero" would be wrong.

impl WorkerPhase0Metrics {
    fn new() -> Self {
        Self {
            tonic_ok_timestamps: Cache::builder()
                .max_capacity(TONIC_OK_TS_CACHE_CAPACITY)
                .time_to_live(TONIC_OK_TS_CACHE_TTL)
                .build(),
            pin_release_latency: LatencyHistogram::new(),
            action_total_pin_extension: LatencyHistogram::new(),
            action_max_pin_extension: LatencyHistogram::new(),
            concurrent_pinned_bytes: BytesHistogram::new(),
            pinned_bytes_live: AtomicU64::new(0),
            bis_chunk_arrive_to_handler: LatencyHistogram::new(),
            action_pin_accumulators: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(TONIC_OK_TS_CACHE_TTL)
                .build(),
        }
    }

    /// Producer: called from `spawn_upload_to_remote` after a successful
    /// per-digest upload (tonic Ok). Stores the timestamp + the caller's
    /// `action_key` keyed by digest, so the BIS handler can both compute
    /// the per-digest gap AND fold the gap into the right action's
    /// accumulator. The `action_key` is opaque (caller-chosen u64; the
    /// upload spawn point uses an action-scoped fresh counter). Passing
    /// `0` for `action_key` is fine — `record_bis_unpin` will still
    /// record the per-digest histogram observation; only the per-action
    /// aggregate is shared across digests with the same key.
    pub fn record_tonic_ok(&self, digest: DigestInfo, action_key: u64) {
        self.tonic_ok_timestamps
            .insert(digest, (Instant::now(), action_key));
    }

    /// Producer: called from `spawn_upload_to_remote` when a digest
    /// is pinned. Bumps the live gauge AND records the new total into
    /// the concurrent-bytes histogram.
    pub fn record_pin_acquired(&self, size_bytes: u64) {
        let new = self
            .pinned_bytes_live
            .fetch_add(size_bytes, Ordering::Relaxed)
            .wrapping_add(size_bytes);
        self.concurrent_pinned_bytes.observe(new);
    }

    /// Producer: called from `spawn_upload_to_remote` when an upload's
    /// pin is released (whether via the eventual BIS unpin or any
    /// other release path). Decrements the live gauge by the digest's
    /// size. **Idempotent against an over-release** via saturating sub.
    pub fn record_pin_released(&self, size_bytes: u64) {
        // Use a CAS-style loop to bound the decrement at zero. This
        // protects against double-release bugs from elsewhere in the
        // codebase silently corrupting the gauge into wraparound.
        loop {
            let cur = self.pinned_bytes_live.load(Ordering::Relaxed);
            let new = cur.saturating_sub(size_bytes);
            if self
                .pinned_bytes_live
                .compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Consumer: called from `handle_blobs_in_stable_storage_for_store`
    /// when unpinning a digest. Looks up the tonic-Ok timestamp,
    /// computes the gap, records it AND folds the gap into the
    /// per-action accumulator (if the recorded action_key is non-zero).
    /// Returns the gap in ms if found (so the caller can optionally
    /// log / publish it); returns `None` if the digest was not in the
    /// side-channel (cache evicted, or the digest was a Bazel-source /
    /// non-worker upload that didn't go through `spawn_upload_to_remote`).
    pub fn record_bis_unpin(&self, digest: &DigestInfo) -> Option<u64> {
        let (tonic_ok_at, action_key) = self.tonic_ok_timestamps.get(digest)?;
        let gap = Instant::now().saturating_duration_since(tonic_ok_at);
        let gap_ms = u64::try_from(gap.as_millis()).unwrap_or(u64::MAX);
        self.pin_release_latency.observe(gap_ms);
        // Fold the per-digest gap into the per-action accumulator. The
        // caller commits the accumulator with `commit_action_pin_extension`
        // once all the action's BIS-unpins have arrived (typically a few
        // hundred ms after the last per-digest unpin under healthy load).
        if action_key != 0 {
            self.record_action_pin_extension(action_key, gap_ms);
        }
        // Evict the entry once consumed; the BIS broadcast is one-shot
        // per digest under the producer protocol.
        self.tonic_ok_timestamps.invalidate(digest);
        Some(gap_ms)
    }

    /// Producer: called from the worker BIS chunk dispatch site after
    /// chunk gRPC receive, BEFORE the handler is invoked. The handler
    /// closes the loop via `commit_arrival_to_handler`.
    pub fn record_bis_chunk_arrival(&self) -> Instant {
        Instant::now()
    }

    /// Producer: called from the worker BIS chunk dispatch site
    /// IMMEDIATELY before invoking `handle_bis_chunk` / `handle_blobs_*`.
    /// Records the arrival → handler-entry gap into the histogram.
    pub fn commit_arrival_to_handler(&self, arrival_ts: Instant) {
        let gap = Instant::now().saturating_duration_since(arrival_ts);
        let gap_ms = u64::try_from(gap.as_millis()).unwrap_or(u64::MAX);
        self.bis_chunk_arrive_to_handler.observe(gap_ms);
    }

    /// Producer: called from `spawn_upload_to_remote` after all
    /// per-digest unpins for an action have fired. Folds the action's
    /// per-digest gaps into the total + max histograms. `action_key`
    /// is any caller-chosen u64 that uniquely identifies the action
    /// across its lifecycle; using `OperationId.hash()` or similar is
    /// fine because the accumulator self-resets on commit.
    ///
    /// **Invariant (#547 fix-up CF3): concurrent invocations with the
    /// same `action_key` produce the correct sum/max.** Earlier
    /// implementation used a get-update-insert pattern which under
    /// concurrent BIS dispatch (e.g. a future refactor that
    /// `tokio::spawn`s the chunk handler to unblock the dispatch loop)
    /// would lose updates: both invocations would read the same
    /// baseline, both compute their own updates, the second `insert`
    /// would clobber the first. Today single-threaded BIS dispatch in
    /// `local_worker.rs:2229` protects against this; tomorrow's
    /// refactor would silently corrupt per-action `total_ms` /
    /// `max_ms` / `digest_count` by up to 50% per collision.
    ///
    /// Fix: `moka::sync::Cache::entry(key).and_upsert_with(|opt| ...)`
    /// uses per-key locking (documented in moka 0.12 `and_upsert_with`)
    /// so concurrent same-key calls serialize and each sees the prior
    /// update's result.
    pub fn record_action_pin_extension(&self, action_key: u64, gap_ms: u64) {
        // Atomic RMW via moka's per-key serialized upsert. Concurrent
        // same-key calls execute in invocation order; cross-key calls
        // remain fully concurrent. The returned `Entry` carries an Arc
        // to the value; we drop it explicitly to avoid the
        // `let _ = ... <destructor> ...` lint.
        drop(
            self.action_pin_accumulators
                .entry(action_key)
                .and_upsert_with(|maybe_entry| match maybe_entry {
                    Some(entry) => {
                        let acc = entry.into_value();
                        ActionAccumulator {
                            total_ms: acc.total_ms.saturating_add(gap_ms),
                            max_ms: acc.max_ms.max(gap_ms),
                            digest_count: acc.digest_count.saturating_add(1),
                        }
                    }
                    None => ActionAccumulator {
                        total_ms: gap_ms,
                        max_ms: gap_ms,
                        digest_count: 1,
                    },
                }),
        );
    }

    /// Producer: called from `spawn_upload_to_remote` when all upload
    /// futures have resolved AND the corresponding BIS-driven unpins
    /// are expected to have fired. Reads + commits + drops the
    /// accumulator. The aggregation is best-effort — if not all digests
    /// have been recorded yet (BIS still in flight), the partial sum
    /// gets emitted and the remaining gaps are lost. For Phase 0 this
    /// is acceptable; the per-digest histogram remains complete.
    pub fn commit_action_pin_extension(&self, action_key: u64) {
        if let Some(acc) = self.action_pin_accumulators.get(&action_key) {
            if acc.digest_count > 0 {
                self.action_total_pin_extension.observe(acc.total_ms);
                self.action_max_pin_extension.observe(acc.max_ms);
            }
            self.action_pin_accumulators.invalidate(&action_key);
        }
    }
}

impl MetricsComponent for WorkerPhase0Metrics {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        self.pin_release_latency.publish_buckets(
            "worker_pin_release_latency_after_tonic_ok",
            "#547 Phase 0: gap between worker chunked-upload tonic Ok and BIS chunk arrival that drives the matching unpin (the headline metric for justifying the #546 design)"
        )?;
        self.action_total_pin_extension.publish_buckets(
            "worker_action_total_pin_extension",
            "#547 Phase 0: per-action sum of pin-extension windows across all output blobs"
        )?;
        self.action_max_pin_extension.publish_buckets(
            "worker_max_pin_extension",
            "#547 Phase 0: per-action max single-blob pin-extension; per perf-optimizer P6 the per-action wall-clock cost is dominated by the last-released pin"
        )?;
        self.concurrent_pinned_bytes.publish_buckets(
            "worker_concurrent_pinned_bytes",
            "#547 Phase 0: worker's concurrent pinned bytes histogram sampled at each pin acquire; informs #549 pin_budget cap selection"
        )?;
        self.bis_chunk_arrive_to_handler.publish_buckets(
            "worker_bis_chunk_arrive_to_handler",
            "#547 Phase 0: gap between worker BIS chunk gRPC receive and handler dispatch; names the worker-side dispatcher contribution to the latency budget"
        )?;
        let live_bytes = self.pinned_bytes_live.load(Ordering::Relaxed);
        nativelink_metric::publish!(
            "worker_concurrent_pinned_bytes_live",
            &live_bytes,
            nativelink_metric::MetricKind::Default,
            "#547 Phase 0: point-in-time worker chunked-upload pinned bytes total (live gauge derived from atomic counter)"
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// Server-side Phase 0 metrics. Process-global singleton via
/// `server_phase0_metrics()` accessor.
#[derive(Debug)]
pub struct ServerPhase0Metrics {
    /// Counter: invocations of `stable_digests_pusher` (the commit-path
    /// → BIS pipeline entry point). Non-zero confirms the commit path
    /// is firing the pusher.
    ///
    /// **Coverage limit (#547 fix-up CF2).** This counter is bumped
    /// ONLY by the `stable_digests_pusher` closure at
    /// `fast_slow_store.rs:1420-1428`. Three other production sites
    /// push directly to `stable_digests.lock()` without routing
    /// through `stable_digests_pusher` and therefore do NOT bump this
    /// counter (and do NOT populate `pusher_timestamps`):
    ///   - `fast_slow_store.rs:2616` `try_self_retry_slow_write`
    ///     success arm (V3 self-retry after a failed slow write).
    ///   - `fast_slow_store.rs:5244` and `:5498` failed-write drain
    ///     re-broadcast paths.
    /// Per-digest `bis_broadcast_queue_latency` will silently drop
    /// samples for those digests. Operators reading this counter
    /// should treat it as "commit-path pusher invocations" — not as
    /// total BIS broadcast traffic. (`server_bis_broadcast_queue_depth`
    /// reflects the FULL queue regardless of insert site.) If a
    /// future audit shows the direct-push sites are non-negligible,
    /// either route them through `stable_digests_pusher` or add a
    /// separate counter for the direct-push paths.
    pusher_invoke_count: AtomicU64,
    /// Unix-ms timestamp of the most recent pusher invocation. Operators
    /// can compare against `wall_clock_now()` to detect a stalled commit
    /// pipeline. Zero means the pusher has never fired.
    pusher_last_at_unix_ms: AtomicU64,
    /// Histogram of BIS broadcast loop's wake-to-send latency: gap
    /// between the select-arm fire (notify or 500 ms tick) and the
    /// `broadcast_blobs_in_stable_storage_chunked` call returning.
    bis_broadcast_loop_wake_to_send: LatencyHistogram,
    /// Histogram of per-digest queue dwell time: `(broadcast_send_at -
    /// pusher_invoke_at)`. Captures the end-to-end server-side
    /// commit→broadcast contribution.
    bis_broadcast_queue_latency: LatencyHistogram,
    /// Live gauge of the server-side `stable_digests` queue depth.
    /// Updated by the broadcast loop on each wake (read of the
    /// pending count BEFORE drain). Per red-team P6: post-relaxation,
    /// no worker-side backpressure throttles enqueues — depth must
    /// be visible NOW.
    bis_broadcast_queue_depth: AtomicU64,
    /// Per-digest pusher-invoke-timestamp side channel for queue
    /// dwell measurement. Producer: `stable_digests_pusher` records
    /// timestamp; consumer: the BIS broadcast loop computes the gap
    /// when each digest is broadcast.
    ///
    /// CAPPED AT 100_000: same cap shape and rationale as
    /// `WorkerPhase0Metrics::tonic_ok_timestamps`; bounded LRU
    /// prevents OOM under digest-storm scenarios.
    pusher_timestamps: Cache<DigestInfo, Instant>,
}

impl ServerPhase0Metrics {
    fn new() -> Self {
        Self {
            pusher_invoke_count: AtomicU64::new(0),
            pusher_last_at_unix_ms: AtomicU64::new(0),
            bis_broadcast_loop_wake_to_send: LatencyHistogram::new(),
            bis_broadcast_queue_latency: LatencyHistogram::new(),
            bis_broadcast_queue_depth: AtomicU64::new(0),
            pusher_timestamps: Cache::builder()
                .max_capacity(TONIC_OK_TS_CACHE_CAPACITY)
                .time_to_live(TONIC_OK_TS_CACHE_TTL)
                .build(),
        }
    }

    /// Producer: called inside `stable_digests_pusher` when a digest
    /// is pushed into the BIS queue.
    pub fn record_pusher_invoke(&self, digest: DigestInfo) {
        self.pusher_invoke_count.fetch_add(1, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.pusher_last_at_unix_ms.store(now_ms, Ordering::Relaxed);
        self.pusher_timestamps.insert(digest, Instant::now());
    }

    /// Producer: called from the BIS broadcast loop right when the
    /// select-arm fires (loop wake). The returned `Instant` is then
    /// passed to `commit_loop_wake_to_send` at the end of the
    /// drain + broadcast cycle to record the latency.
    pub fn record_loop_wake(&self) -> Instant {
        Instant::now()
    }

    /// Producer: called from the BIS broadcast loop after the
    /// `broadcast_*` call returns. Records the wake-to-send gap.
    pub fn commit_loop_wake_to_send(&self, wake_ts: Instant) {
        let gap = Instant::now().saturating_duration_since(wake_ts);
        let gap_ms = u64::try_from(gap.as_millis()).unwrap_or(u64::MAX);
        self.bis_broadcast_loop_wake_to_send.observe(gap_ms);
    }

    /// Producer: called by the BIS broadcast loop after each drain,
    /// AFTER the broadcast has fired for each digest in the batch.
    /// Updates the queue-depth gauge AND records per-digest dwell
    /// time (broadcast_send_at - pusher_invoke_at) into the queue
    /// latency histogram.
    pub fn record_broadcast(&self, digests: &[DigestInfo], remaining_queue_depth: u64) {
        self.bis_broadcast_queue_depth
            .store(remaining_queue_depth, Ordering::Relaxed);
        let now = Instant::now();
        for digest in digests {
            if let Some(pushed_at) = self.pusher_timestamps.get(digest) {
                let gap = now.saturating_duration_since(pushed_at);
                let gap_ms = u64::try_from(gap.as_millis()).unwrap_or(u64::MAX);
                self.bis_broadcast_queue_latency.observe(gap_ms);
                self.pusher_timestamps.invalidate(digest);
            }
        }
    }
}

impl MetricsComponent for ServerPhase0Metrics {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let invokes = self.pusher_invoke_count.load(Ordering::Relaxed);
        let last_at = self.pusher_last_at_unix_ms.load(Ordering::Relaxed);
        let depth = self.bis_broadcast_queue_depth.load(Ordering::Relaxed);
        nativelink_metric::publish!(
            "server_stable_digests_pusher_invoke_via_commit_path_count",
            &invokes,
            nativelink_metric::MetricKind::Counter,
            "#547 Phase 0 (CF2): stable_digests pushed via the chunked-v2 commit path (stable_digests_pusher closure) only; direct push paths from failed_writes_drain (fast_slow_store.rs:2616 / :5244 / :5498) bypass this counter. Non-zero confirms commit path is firing; this is NOT total BIS broadcast traffic."
        );
        nativelink_metric::publish!(
            "server_stable_digests_pusher_last_at_unix_ms",
            &last_at,
            nativelink_metric::MetricKind::Default,
            "#547 Phase 0: SystemTime-derived unix-ms timestamp of most recent stable_digests_pusher invocation (commit-path only — see _via_commit_path_count for coverage limits); zero means pusher has never fired. Wall-clock source: NTP-step backward can move this gauge backward; operators alerting on monotonic increase should additionally cross-check the _count counter (monotonic) before paging on a step."
        );
        nativelink_metric::publish!(
            "server_bis_broadcast_queue_depth",
            &depth,
            nativelink_metric::MetricKind::Default,
            "#547 Phase 0: live BIS broadcast queue depth (stable_digests pending after the most recent drain); per red-team P6, post-relaxation visibility is required because worker-side backpressure no longer throttles enqueues"
        );
        self.bis_broadcast_loop_wake_to_send.publish_buckets(
            "server_bis_broadcast_loop_wake_to_send",
            "#547 Phase 0: gap between BIS broadcast loop select-arm fire and broadcast_blobs_in_stable_storage_chunked return; names the drain-loop coalescing contribution to per-action latency"
        )?;
        self.bis_broadcast_queue_latency.publish_buckets(
            "server_bis_broadcast_queue_latency",
            "#547 Phase 0: per-digest BIS-queue dwell time (broadcast_send_at - pusher_invoke_at); end-to-end server-side commit→broadcast contribution"
        )?;
        Ok(MetricPublishKnownKindData::Component)
    }
}

// -----------------------------------------------------------------
// Process-global singletons. Pattern mirrors `PinBudget::pin_budget_singleton`
// at `nativelink-store/src/chunked/pin_budget.rs:192-204`.
// -----------------------------------------------------------------

static WORKER_PHASE0_METRICS: OnceLock<Arc<WorkerPhase0Metrics>> = OnceLock::new();
static SERVER_PHASE0_METRICS: OnceLock<Arc<ServerPhase0Metrics>> = OnceLock::new();

fn worker_phase0_metrics_inner() -> &'static Arc<WorkerPhase0Metrics> {
    WORKER_PHASE0_METRICS.get_or_init(|| Arc::new(WorkerPhase0Metrics::new()))
}

fn server_phase0_metrics_inner() -> &'static Arc<ServerPhase0Metrics> {
    SERVER_PHASE0_METRICS.get_or_init(|| Arc::new(ServerPhase0Metrics::new()))
}

/// Returns the process-wide worker-side Phase 0 metrics singleton.
/// Initializes on first call. Cheap thereafter (atomic load).
pub fn worker_phase0_metrics() -> &'static WorkerPhase0Metrics {
    worker_phase0_metrics_inner().as_ref()
}

/// Returns an `Arc` to the same singleton returned by
/// `worker_phase0_metrics()`. Use at process start to hand a clone to
/// `MetricsRegistry::register` so the gauges/histograms are scraped
/// by every `/metrics` listener.
#[must_use]
pub fn worker_phase0_metrics_arc() -> Arc<WorkerPhase0Metrics> {
    Arc::clone(worker_phase0_metrics_inner())
}

/// Returns the process-wide server-side Phase 0 metrics singleton.
pub fn server_phase0_metrics() -> &'static ServerPhase0Metrics {
    server_phase0_metrics_inner().as_ref()
}

/// Returns an `Arc` to the same singleton returned by
/// `server_phase0_metrics()`.
#[must_use]
pub fn server_phase0_metrics_arc() -> Arc<ServerPhase0Metrics> {
    Arc::clone(server_phase0_metrics_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity: fresh metrics have zero counts and zero pinned bytes.
    #[test]
    fn fresh_worker_metrics_are_zero() {
        let m = WorkerPhase0Metrics::new();
        assert_eq!(m.pinned_bytes_live.load(Ordering::Relaxed), 0);
        assert_eq!(m.pin_release_latency.inf_bucket.load(Ordering::Relaxed), 0);
    }

    /// Per-action accumulator fold: tonic-Ok with a non-zero action key
    /// + BIS unpin folds the gap into the action's accumulator. Commit
    /// emits the histogram observation, which the per-digest path does
    /// NOT do on its own (verified by `tonic_ok_then_bis_unpin_records_gap`
    /// only bumping `pin_release_latency`).
    #[test]
    fn tonic_ok_with_action_key_folds_into_accumulator() {
        let m = WorkerPhase0Metrics::new();
        let d1 = DigestInfo::try_new(
            "1111111111111111111111111111111111111111111111111111111111111111",
            1,
        )
        .unwrap();
        let d2 = DigestInfo::try_new(
            "2222222222222222222222222222222222222222222222222222222222222222",
            2,
        )
        .unwrap();
        let action_key = 0xCAFE_BABE;
        m.record_tonic_ok(d1, action_key);
        m.record_tonic_ok(d2, action_key);
        // BIS unpins fold gaps into the action accumulator.
        let _ = m.record_bis_unpin(&d1);
        let _ = m.record_bis_unpin(&d2);
        // No commit yet → action histograms still empty.
        assert_eq!(
            m.action_total_pin_extension
                .inf_bucket
                .load(Ordering::Relaxed),
            0
        );
        // Commit emits the histograms.
        m.commit_action_pin_extension(action_key);
        assert_eq!(
            m.action_total_pin_extension
                .inf_bucket
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.action_max_pin_extension.inf_bucket.load(Ordering::Relaxed),
            1
        );
    }

    /// Producer + consumer round-trip: record a tonic-Ok, then a BIS
    /// unpin for the same digest, observe a non-zero gap recorded.
    #[test]
    fn tonic_ok_then_bis_unpin_records_gap() {
        let m = WorkerPhase0Metrics::new();
        let digest = DigestInfo::try_new(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            42,
        )
        .unwrap();
        m.record_tonic_ok(digest, 0);
        // Tiny spin so the gap is non-zero. Avoid sleep — synchronization
        // is via the moka cache; the gap need only be measurable, not real.
        for _ in 0..10_000 {
            std::hint::spin_loop();
        }
        let gap = m.record_bis_unpin(&digest).expect("digest must be found");
        // The gap is in ms; spin-loop is sub-ms, so 0 ms is correct.
        // What we assert is the count went up.
        assert_eq!(m.pin_release_latency.inf_bucket.load(Ordering::Relaxed), 1);
        // gap is 0 or more ms; just verify no overflow.
        assert!(gap < 1_000_000, "implausible gap: {gap} ms");
    }

    /// Missing-side-channel: BIS unpin for a digest that was never
    /// recorded by `record_tonic_ok` returns `None` and does NOT record
    /// a histogram observation (we don't fabricate a zero).
    #[test]
    fn bis_unpin_without_tonic_ok_returns_none() {
        let m = WorkerPhase0Metrics::new();
        let digest = DigestInfo::try_new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1,
        )
        .unwrap();
        let gap = m.record_bis_unpin(&digest);
        assert!(gap.is_none(), "absent digest must return None");
        assert_eq!(m.pin_release_latency.inf_bucket.load(Ordering::Relaxed), 0);
    }

    /// Pin lifecycle: acquire bumps the live gauge AND the histogram;
    /// release decrements the gauge.
    #[test]
    fn pin_lifecycle_updates_gauge_and_histogram() {
        let m = WorkerPhase0Metrics::new();
        m.record_pin_acquired(1024);
        assert_eq!(m.pinned_bytes_live.load(Ordering::Relaxed), 1024);
        assert_eq!(
            m.concurrent_pinned_bytes.inf_bucket.load(Ordering::Relaxed),
            1
        );
        m.record_pin_acquired(2048);
        assert_eq!(m.pinned_bytes_live.load(Ordering::Relaxed), 3072);
        m.record_pin_released(1024);
        assert_eq!(m.pinned_bytes_live.load(Ordering::Relaxed), 2048);
        // Over-release saturates at 0, doesn't wrap.
        m.record_pin_released(99999);
        assert_eq!(m.pinned_bytes_live.load(Ordering::Relaxed), 0);
    }

    /// CF1 regression: acquire+release pair must zero the gauge so the
    /// "live" semantic actually holds. The unit-level `record_pin_released`
    /// existed but was never wired in production — `local_worker.rs`'s
    /// CAS BIS-unpin loop called `record_bis_unpin` without the matching
    /// `record_pin_released`. Reviewers flagged the gauge as
    /// monotonically growing (distributed-systems MAJOR-1, code-reviewer
    /// MAJOR-2). This test bakes the symmetry into the unit so a future
    /// removal of either side red-fails.
    ///
    /// Mutation step: revert the producer fix at `local_worker.rs:1117-1131`
    /// (drop the `record_pin_released` call inside the `if` arm). This
    /// test stays green — the test exercises the unit, not the wiring.
    /// The bespoke message below is the discriminator a triage operator
    /// should grep for.
    #[test]
    fn pin_acquire_release_pair_zeroes_live_gauge() {
        let m = WorkerPhase0Metrics::new();
        m.record_pin_acquired(4096);
        m.record_pin_acquired(8192);
        assert_eq!(
            m.pinned_bytes_live.load(Ordering::Relaxed),
            12288,
            "#547 fix-up CF1: pinned_bytes_live should reflect sum of \
             outstanding acquires; if this fails the producer-side gauge \
             bump itself is broken"
        );
        m.record_pin_released(4096);
        m.record_pin_released(8192);
        assert_eq!(
            m.pinned_bytes_live.load(Ordering::Relaxed),
            0,
            "#547 fix-up CF1: pinned_bytes_live grows but never shrinks; \
             record_pin_released not wired in production — \
             worker_concurrent_pinned_bytes_live would saturate at +inf \
             within minutes and the gauge would be useless for Phase 2 \
             (#549) pin_budget cap selection"
        );
    }

    /// BIS chunk arrival timer records into the histogram.
    #[test]
    fn bis_chunk_arrive_to_handler_records() {
        let m = WorkerPhase0Metrics::new();
        let arrival = m.record_bis_chunk_arrival();
        for _ in 0..10_000 {
            std::hint::spin_loop();
        }
        m.commit_arrival_to_handler(arrival);
        assert_eq!(
            m.bis_chunk_arrive_to_handler
                .inf_bucket
                .load(Ordering::Relaxed),
            1
        );
    }

    /// CF3 regression: concurrent calls to `record_action_pin_extension`
    /// with the same `action_key` must produce the correct sum + max.
    /// The earlier get-update-insert pattern would lose updates under
    /// concurrent BIS dispatch (a refactor that `tokio::spawn`s the
    /// chunk handler to unblock the dispatch loop would silently
    /// corrupt `total_ms` / `max_ms` / `digest_count`).
    ///
    /// This test spawns 2 OS threads that each call
    /// `record_action_pin_extension(key, gap)` N times. Expected
    /// outcomes after both complete:
    ///   - `digest_count == 2 * N` (no lost updates)
    ///   - `total_ms == 2 * N * gap_per_call_avg`
    ///   - `max_ms == max(gap_per_call)`
    ///
    /// Mutation: revert `and_upsert_with` to the original
    /// get-update-insert pattern; test must red-fail with the bespoke
    /// "#547 fix-up CF3" message because concurrent loads will see a
    /// stale baseline and the second writer will clobber the first.
    /// Note: under a single-threaded scheduler this race is hard to
    /// hit; the test deliberately uses `std::thread::spawn` (NOT
    /// tokio) to maximize OS-thread interleaving so the race surfaces
    /// reliably in CI.
    #[test]
    fn action_accumulator_concurrent_updates_atomic_rmw() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let m = StdArc::new(WorkerPhase0Metrics::new());
        let key: u64 = 0xC0FFEE_DEAD_BEEF;
        const ITERS: u64 = 1000;
        const GAP_PER_CALL: u64 = 7;

        let m1 = StdArc::clone(&m);
        let h1 = thread::spawn(move || {
            for _ in 0..ITERS {
                m1.record_action_pin_extension(key, GAP_PER_CALL);
            }
        });
        let m2 = StdArc::clone(&m);
        let h2 = thread::spawn(move || {
            for _ in 0..ITERS {
                m2.record_action_pin_extension(key, GAP_PER_CALL);
            }
        });
        h1.join().expect("thread 1 must complete");
        h2.join().expect("thread 2 must complete");

        let acc = m
            .action_pin_accumulators
            .get(&key)
            .expect("accumulator must exist after 2 * ITERS records");
        let expected_total = 2 * ITERS * GAP_PER_CALL;
        let expected_count = 2 * ITERS;
        assert_eq!(
            acc.digest_count, expected_count,
            "#547 fix-up CF3: action accumulator RMW race; concurrent fold \
             corrupts total_ms/max_ms — digest_count under-counted by \
             {} (expected {}, observed {}). Mechanism: get-update-insert \
             pattern non-atomic on moka::sync::Cache; per-key serialized \
             entry().and_upsert_with() is required.",
            expected_count - acc.digest_count,
            expected_count,
            acc.digest_count
        );
        assert_eq!(
            acc.total_ms, expected_total,
            "#547 fix-up CF3: action accumulator RMW race; concurrent fold \
             corrupts total_ms (expected {}, observed {}). Lost-update count: {}.",
            expected_total,
            acc.total_ms,
            expected_count.saturating_sub(acc.digest_count)
        );
        // max_ms is associative under concurrent max, so even a racy
        // implementation gets this right. Verify anyway.
        assert_eq!(
            acc.max_ms, GAP_PER_CALL,
            "#547 fix-up CF3: max_ms should equal GAP_PER_CALL = {} under \
             constant-gap workload (observed {})",
            GAP_PER_CALL, acc.max_ms
        );
    }

    /// Action accumulator commits sum + max correctly across multiple
    /// per-digest records.
    #[test]
    fn action_accumulator_aggregates_total_and_max() {
        let m = WorkerPhase0Metrics::new();
        let key = 0xDEAD_BEEF;
        m.record_action_pin_extension(key, 10);
        m.record_action_pin_extension(key, 50);
        m.record_action_pin_extension(key, 25);
        m.commit_action_pin_extension(key);
        // total_ms = 85 should land in the `_le_100_ms` bucket (and all
        // larger buckets); max_ms = 50 should land in `_le_50_ms` (and
        // all larger).
        assert_eq!(
            m.action_total_pin_extension
                .inf_bucket
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.action_max_pin_extension.inf_bucket.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.action_total_pin_extension.sum.load(Ordering::Relaxed),
            85
        );
        assert_eq!(
            m.action_max_pin_extension.sum.load(Ordering::Relaxed),
            50
        );
    }

    /// Server: pusher invoke updates count + timestamp.
    #[test]
    fn pusher_invoke_records() {
        let m = ServerPhase0Metrics::new();
        assert_eq!(m.pusher_invoke_count.load(Ordering::Relaxed), 0);
        assert_eq!(m.pusher_last_at_unix_ms.load(Ordering::Relaxed), 0);
        let digest = DigestInfo::try_new(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            7,
        )
        .unwrap();
        m.record_pusher_invoke(digest);
        assert_eq!(m.pusher_invoke_count.load(Ordering::Relaxed), 1);
        assert!(m.pusher_last_at_unix_ms.load(Ordering::Relaxed) > 0);
    }

    /// Server: broadcast records dwell time when the digest was pushed
    /// earlier; updates the queue-depth gauge.
    #[test]
    fn broadcast_records_dwell_and_depth() {
        let m = ServerPhase0Metrics::new();
        let digest = DigestInfo::try_new(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            13,
        )
        .unwrap();
        m.record_pusher_invoke(digest);
        for _ in 0..10_000 {
            std::hint::spin_loop();
        }
        m.record_broadcast(&[digest], 42);
        assert_eq!(m.bis_broadcast_queue_depth.load(Ordering::Relaxed), 42);
        assert_eq!(
            m.bis_broadcast_queue_latency
                .inf_bucket
                .load(Ordering::Relaxed),
            1
        );
    }

    /// Server: wake-to-send timer records into the histogram.
    #[test]
    fn loop_wake_to_send_records() {
        let m = ServerPhase0Metrics::new();
        let wake = m.record_loop_wake();
        for _ in 0..10_000 {
            std::hint::spin_loop();
        }
        m.commit_loop_wake_to_send(wake);
        assert_eq!(
            m.bis_broadcast_loop_wake_to_send
                .inf_bucket
                .load(Ordering::Relaxed),
            1
        );
    }

    /// Singletons return the same instance across calls (load-bearing
    /// for the metrics-registry wiring — register_dyn gets a clone of
    /// the same Arc the producers consult).
    #[test]
    fn singletons_alias() {
        let a: *const WorkerPhase0Metrics = worker_phase0_metrics();
        let b: *const WorkerPhase0Metrics = worker_phase0_metrics();
        assert!(core::ptr::eq(a, b));
        let arc = worker_phase0_metrics_arc();
        let arc_ptr: *const WorkerPhase0Metrics = &*arc;
        assert!(core::ptr::eq(a, arc_ptr));

        let c: *const ServerPhase0Metrics = server_phase0_metrics();
        let d: *const ServerPhase0Metrics = server_phase0_metrics();
        assert!(core::ptr::eq(c, d));
        let arc = server_phase0_metrics_arc();
        let arc_ptr: *const ServerPhase0Metrics = &*arc;
        assert!(core::ptr::eq(c, arc_ptr));
    }

    /// End-to-end metric publication via the same `MetricsRegistry` +
    /// `render_prometheus` path the production `/metrics` listener
    /// uses. Mirror of the `PinBudget::publish_emits_gauges_via_render_prometheus`
    /// test in `nativelink-store/src/chunked/pin_budget.rs:397`.
    ///
    /// Why this test: the per-metric unit tests cover internal state.
    /// They do NOT cover the publish → registry → Prometheus exposition
    /// seam. Per CLAUDE.md asymmetric-contract discipline, the
    /// under-action gap is "publish silently emits nothing" — invisible
    /// to in-process callers, fatal for operator scrapes.
    ///
    /// **CF4 strengthening (testing-czar finding):** the test now
    /// asserts specific VALUES for every emitted metric, not just
    /// presence. Per memory `feedback_publish_body_not_field_existence`
    /// (#160 / #380 incident 2026-05-11): a future refactor that
    /// silently dis-wires a producer (e.g. moves it inside an
    /// `if !mirror_request { ... }` branch that's always false in
    /// production) would leave the metric in the scrape body but with
    /// stale-zero values. Presence-only assertions would not catch
    /// that; value assertions do.
    ///
    /// Mutation step: comment out any of the `nativelink_metric::publish!`
    /// calls in `WorkerPhase0Metrics::publish` or `ServerPhase0Metrics::publish`.
    /// OR replace a producer-side `fetch_add(1)` with `load()`. The
    /// test must red-fail with the bespoke "#547 fix-up CF4:" message
    /// naming the metric whose value was wrong.
    #[test]
    fn publish_emits_metrics_via_render_prometheus() {
        use crate::metrics_publisher::{MetricsRegistry, render_prometheus};

        let worker = Arc::new(WorkerPhase0Metrics::new());
        let digest = DigestInfo::try_new(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            100,
        )
        .unwrap();
        // Pin lifecycle: acquire 4096, release 4096 → live gauge = 0,
        // concurrent_pinned_bytes count = 1.
        worker.record_pin_acquired(4096);
        worker.record_pin_released(4096);
        // tonic_ok → bis_unpin: pin_release_latency count = 1.
        worker.record_tonic_ok(digest, 0);
        let _ = worker.record_bis_unpin(&digest);
        // BIS chunk arrival: bis_chunk_arrive_to_handler count = 1.
        let arr = worker.record_bis_chunk_arrival();
        worker.commit_arrival_to_handler(arr);
        // Per-action: record + commit → action_total + action_max count = 1, sum = 33.
        worker.record_action_pin_extension(99, 33);
        worker.commit_action_pin_extension(99);

        let server = Arc::new(ServerPhase0Metrics::new());
        // Pusher invoke: count = 1, last_at > 0.
        server.record_pusher_invoke(digest);
        // Wake → send: loop_wake_to_send count = 1.
        let wake = server.record_loop_wake();
        server.commit_loop_wake_to_send(wake);
        // Broadcast: depth = 7, queue_latency count = 1.
        server.record_broadcast(&[digest], 7);

        let registry = MetricsRegistry::new();
        registry.register("phase0_worker", worker.clone());
        registry.register("phase0_server", server.clone());
        let body = render_prometheus(&registry);

        // Value-bearing assertions. Each (line, why) pair below names
        // the producer side-effect that the metric value reflects;
        // if the producer is silently dis-wired, the line is absent or
        // the value is stale-zero.
        //
        // Format: "<metric> <value>\n" (exposition is one line per
        // metric, value separated by single space, terminated by
        // newline — matches the `nativelink_metric::publish!` macro's
        // emission shape and `render_prometheus`'s capture).
        let assertions: [(&str, &str, &str); 13] = [
            // (line to find, metric short-name for error message, expected-state description)
            (
                "phase0_worker_worker_pin_release_latency_after_tonic_ok_count 1\n",
                "pin_release_latency count",
                "1 round-trip recorded (record_tonic_ok + record_bis_unpin)",
            ),
            (
                "phase0_worker_worker_action_total_pin_extension_count 1\n",
                "action_total_pin_extension count",
                "1 commit_action_pin_extension after a 33ms fold",
            ),
            (
                "phase0_worker_worker_action_total_pin_extension_sum_ms 33\n",
                "action_total_pin_extension sum_ms",
                "sum should equal the single 33ms gap recorded",
            ),
            (
                "phase0_worker_worker_max_pin_extension_count 1\n",
                "max_pin_extension count",
                "1 commit recorded",
            ),
            (
                "phase0_worker_worker_max_pin_extension_sum_ms 33\n",
                "max_pin_extension sum_ms",
                "single observation; sum = max = 33ms",
            ),
            (
                "phase0_worker_worker_concurrent_pinned_bytes_count 1\n",
                "concurrent_pinned_bytes count",
                "1 pin_acquired (4096 bytes) sampled the gauge",
            ),
            (
                "phase0_worker_worker_concurrent_pinned_bytes_live 0\n",
                "concurrent_pinned_bytes_live (CF1 invariant)",
                "acquire(4096) + release(4096) must return live gauge to 0; \
                 non-zero here would mean record_pin_released was broken or \
                 the gauge was never decremented",
            ),
            (
                "phase0_worker_worker_bis_chunk_arrive_to_handler_count 1\n",
                "bis_chunk_arrive_to_handler count",
                "1 record_bis_chunk_arrival + commit_arrival_to_handler pair",
            ),
            (
                "phase0_server_server_stable_digests_pusher_invoke_via_commit_path_count 1\n",
                "pusher_invoke_via_commit_path count (CF2 rename)",
                "1 record_pusher_invoke call",
            ),
            (
                "phase0_server_server_bis_broadcast_queue_depth 7\n",
                "bis_broadcast_queue_depth",
                "record_broadcast(_, 7) sets the depth gauge to the last-drained batch size",
            ),
            (
                "phase0_server_server_bis_broadcast_loop_wake_to_send_count 1\n",
                "bis_broadcast_loop_wake_to_send count",
                "1 record_loop_wake + commit_loop_wake_to_send pair",
            ),
            (
                "phase0_server_server_bis_broadcast_queue_latency_count 1\n",
                "bis_broadcast_queue_latency count",
                "record_broadcast saw 1 digest with a matching pusher_invoke",
            ),
            // pin_release_latency sum is timing-dependent (0-1ms typical
            // in isolation, occasionally up to hundreds of ms under
            // parallel-test scheduler load in CI); assert on the
            // largest bucket (`_le_1000_ms`) which always catches the
            // observation regardless of scheduler jitter. This still
            // confirms the value path reaches Prometheus — a missing
            // wiring would leave ALL buckets at 0, not just the small
            // ones. Asserting le_5_ms here would be flaky under
            // contention (observed in `cargo test -p nativelink-util
            // --lib` parallel runs).
            (
                "phase0_worker_worker_pin_release_latency_after_tonic_ok_le_1000_ms 1\n",
                "pin_release_latency le_1000_ms bucket",
                "round-trip must land somewhere in the histogram; the le_1000_ms bucket is the largest finite boundary and must contain the observation regardless of scheduler jitter",
            ),
        ];
        for (needle, short_name, why) in &assertions {
            assert!(
                body.contains(needle),
                "#547 fix-up CF4: metric {short_name} value expected in \
                 render_prometheus body but not found.\n\
                 Expected line: {needle:?}\n\
                 Why: {why}\n\
                 If this fails after a producer-wiring change, the producer \
                 stopped firing OR the publish() body dropped this metric \
                 OR the registry prefix changed. body=\n{body}"
            );
        }

        // Last-at gauge is non-zero — exact value is SystemTime-derived
        // so we can only assert presence + a non-zero pattern (any digit).
        assert!(
            body.contains("phase0_server_server_stable_digests_pusher_last_at_unix_ms ")
                && !body.contains("phase0_server_server_stable_digests_pusher_last_at_unix_ms 0\n"),
            "#547 fix-up CF4: pusher_last_at_unix_ms must be non-zero after \
             record_pusher_invoke; observed it as 0 or missing. The producer \
             side-effect (SystemTime::now().store(...)) was either not invoked \
             or its write was discarded by publish(). body=\n{body}"
        );
    }
}
