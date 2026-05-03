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

//! Always-on probe to record `spawn_blocking` inter-arrival rates at
//! the chunked-path hot call sites. Used to confirm/refute the #239
//! root-cause hypothesis (chunked path's 7-133× spawn_blocking
//! amplification saturating tokio 1.49's single-mutex blocking pool).
//!
//! Three sites are wrapped (each via a one-line `record(...)` placed
//! immediately before the `tokio::task::spawn_blocking` call):
//!
//! 1. [`SpawnSite::ChunkedShaAdmit`] —
//!    `nativelink-service/src/chunked_write_handler.rs` `compute_sha256_blocking`
//!    on the per-incoming `WriteChunk` admit path (one call per
//!    Bazel-side chunk). Highest-frequency site.
//! 2. [`SpawnSite::ChunkedShaCommit`] —
//!    `nativelink-service/src/chunked_write_handler.rs` `compute_sha256_blocking`
//!    on the bazel-facing internal-chunking driver path that re-chunks
//!    a wrapped non-chunked stream into `ChunkedFastSlowStore` units.
//! 3. [`SpawnSite::ChunkedPwrite`] —
//!    `nativelink-store/src/chunked/chunked_filesystem.rs`
//!    `write_chunk_at_offset` (per-chunk `pwrite(2)`).
//!
//! Storage: a single global `parking_lot::Mutex<VecDeque<Sample>>`
//! ring of [`RING_CAPACITY`] entries. Each sample is 16 bytes
//! `Instant` + 1 byte site id (with padding ≈ 24 bytes); total
//! resident ≈ 96 KiB. Eviction is FIFO (`pop_front` when at capacity).
//!
//! Overhead per [`record`]: one `parking_lot::Mutex` acquire (μs hold)
//! + one `push_back` + at most one `pop_front`. Well below the cost of
//! the `spawn_blocking` it is measuring (and its goal is precisely to
//! quantify queueing into the blocking pool, so a μs of probe time is
//! immaterial relative to ms of pool-queue stall).
//!
//! **Concurrency contract.** [`record`] takes no future and returns
//! synchronously; the lock is therefore never held across `.await`
//! (CLAUDE.md hard rule). [`snapshot`] briefly locks, clones the
//! ring contents into a local `Vec`, drops the lock, and computes
//! percentiles outside the critical section.
//!
//! Read via the HTTP debug endpoint at `GET /debug/spawn_rate_probe`
//! exposed by `pprof_server.rs`. Returns JSON; see [`SpawnRateSnapshot`].

use std::collections::VecDeque;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;

/// Maximum number of samples retained in the ring. At ~24 bytes per
/// sample, total resident ≈ 96 KiB. Sized to capture roughly one
/// full Bazel build's burst at peak (>10K spawn_blocking per second
/// observed in #239 stalls; 4096 ≈ 0.4 s window — enough to
/// characterize p99 inter-arrival, not enough to bloat memory).
pub const RING_CAPACITY: usize = 4096;

/// Identifies which of the 3 chunked-path call sites a sample came
/// from. Encoded as `u8` for compact storage in [`Sample`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SpawnSite {
    /// `chunked_write_handler.rs` `compute_sha256_blocking` on the
    /// per-`WriteChunk` Bazel-side admit path. Fired once per chunk
    /// the producer hands us.
    ChunkedShaAdmit = 1,
    /// `chunked_write_handler.rs` `compute_sha256_blocking` on the
    /// bazel-facing internal-chunking driver path that re-chunks a
    /// wrapped non-chunked stream.
    ChunkedShaCommit = 2,
    /// `chunked/chunked_filesystem.rs` `write_chunk_at_offset`
    /// (per-chunk `pwrite(2)`). Fired once per chunk written to the
    /// fast tier's chunked partial.
    ChunkedPwrite = 3,
}

impl SpawnSite {
    /// Stable string label used in the JSON snapshot output.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::ChunkedShaAdmit => "ChunkedShaAdmit",
            Self::ChunkedShaCommit => "ChunkedShaCommit",
            Self::ChunkedPwrite => "ChunkedPwrite",
        }
    }

    /// Decode a `u8` previously stored via `as u8`. Returns `None` for
    /// unknown values (defensive — the ring only ever stores the three
    /// valid variants, so this is a soundness check).
    #[must_use]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::ChunkedShaAdmit),
            2 => Some(Self::ChunkedShaCommit),
            3 => Some(Self::ChunkedPwrite),
            _ => None,
        }
    }
}

/// One ring entry: when the call happened (relative to process
/// `Instant` epoch) and which site emitted it.
#[derive(Debug, Clone, Copy)]
struct Sample {
    ts: Instant,
    site: u8,
}

static RING: LazyLock<Mutex<VecDeque<Sample>>> =
    LazyLock::new(|| Mutex::new(VecDeque::with_capacity(RING_CAPACITY)));

/// Record one `spawn_blocking` invocation at the given site. Call
/// this on the ASYNC side immediately before the `spawn_blocking`
/// call — never inside the closure, so we measure inter-arrival of
/// SUBMISSIONS, not of completions.
///
/// # Concurrency
///
/// Acquires a single `parking_lot::Mutex` for the duration of one
/// `push_back` (and at most one `pop_front`). The function is sync
/// and contains no `.await`, so the CLAUDE.md "never hold a lock
/// across `.await`" rule is upheld by construction.
///
/// # Performance
///
/// On an uncontended Mutex acquire is ≈ 25 ns; under contention the
/// `parking_lot` adaptive spin keeps the hot path well under 1 μs.
/// Total expected overhead per call: < 1 μs, vs ≥ 100 μs typical
/// `spawn_blocking` queue + thread wake. Always-on is intentional.
pub fn record(site: SpawnSite) {
    let now = Instant::now();
    let mut ring = RING.lock();
    if ring.len() >= RING_CAPACITY {
        ring.pop_front();
    }
    ring.push_back(Sample {
        ts: now,
        site: site as u8,
    });
}

/// One sample emitted in the JSON `last_n_raw` payload. `ts_us_ago`
/// is the microseconds elapsed between the sample's `Instant` and
/// the moment [`snapshot`] was called (always non-negative; older
/// samples have larger values).
#[derive(Debug, Clone, Serialize)]
pub struct RawSample {
    pub site: &'static str,
    pub ts_us_ago: u64,
}

/// Per-site sample count breakdown. Stable JSON field names keep
/// downstream dashboards / scripts robust across refactors.
#[derive(Debug, Clone, Serialize, Default)]
pub struct BySiteCount {
    #[serde(rename = "ChunkedShaAdmit")]
    pub chunked_sha_admit: usize,
    #[serde(rename = "ChunkedShaCommit")]
    pub chunked_sha_commit: usize,
    #[serde(rename = "ChunkedPwrite")]
    pub chunked_pwrite: usize,
}

/// Tokio blocking-pool health captured at snapshot time. None if
/// `snapshot()` was called outside a tokio runtime (e.g. unit tests).
/// All fields source from `tokio::runtime::Handle::current().metrics()`
/// (tokio_unstable, already enabled in the workspace).
///
/// Together with the per-site inter-arrival deltas this answers the
/// #239 hypothesis directly: H1 predicts p99 inter-arrival < ~100 μs
/// AND `num_blocking_threads()` healthy (no slot exhaustion). If both
/// hold, chunked-path amplification is saturating tokio's single-mutex
/// blocking pool. If `num_blocking_threads` is at the cap, the answer
/// is slot exhaustion (different fix).
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub num_blocking_threads: usize,
    pub num_idle_blocking_threads: usize,
    pub num_workers: usize,
    pub num_alive_tasks: usize,
}

/// JSON payload returned by `GET /debug/spawn_rate_probe`. All
/// percentiles are inter-arrival deltas (the gap between consecutive
/// `record` calls, ANY site) measured in microseconds. A small
/// inter-arrival (e.g. p99 < 100 μs) means the chunked path is
/// firing into `spawn_blocking` faster than the blocking pool can
/// drain — the #239 hypothesis.
#[derive(Debug, Clone, Serialize)]
pub struct SpawnRateSnapshot {
    pub samples_in_ring: usize,
    pub ring_capacity: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub by_site: BySiteCount,
    pub last_n_raw: Vec<RawSample>,
    /// None if snapshot() was called outside a tokio runtime context
    /// (e.g. unit tests). Required to disambiguate H1 (mutex
    /// contention) from slot exhaustion — see `RuntimeMetricsSnapshot`
    /// docs.
    pub runtime_metrics: Option<RuntimeMetricsSnapshot>,
}

/// Take a snapshot of the ring. Acquires the lock briefly, clones
/// out the entries, drops the lock, then computes percentiles +
/// per-site counts outside the critical section.
///
/// `last_n` caps the size of the `last_n_raw` field in the returned
/// snapshot (most-recent samples kept). 0 returns no raw samples.
#[must_use]
pub fn snapshot(last_n: usize) -> SpawnRateSnapshot {
    let entries: Vec<Sample> = {
        let ring = RING.lock();
        ring.iter().copied().collect()
    };

    let now = Instant::now();
    let samples_in_ring = entries.len();

    // Inter-arrival deltas in microseconds. Computed in submission
    // order (push_back appends, so iter is oldest-first).
    let mut deltas_us: Vec<u64> = Vec::with_capacity(samples_in_ring.saturating_sub(1));
    for window in entries.windows(2) {
        let delta = window[1].ts.saturating_duration_since(window[0].ts);
        deltas_us.push(delta_to_us(delta));
    }

    let (p50_us, p95_us, p99_us) = percentiles(&mut deltas_us);

    // By-site totals.
    let mut by_site = BySiteCount::default();
    for s in &entries {
        match SpawnSite::from_u8(s.site) {
            Some(SpawnSite::ChunkedShaAdmit) => by_site.chunked_sha_admit += 1,
            Some(SpawnSite::ChunkedShaCommit) => by_site.chunked_sha_commit += 1,
            Some(SpawnSite::ChunkedPwrite) => by_site.chunked_pwrite += 1,
            None => {}
        }
    }

    // Last-N raw, most-recent-first.
    let take = last_n.min(samples_in_ring);
    let last_n_raw: Vec<RawSample> = entries
        .iter()
        .rev()
        .take(take)
        .map(|s| RawSample {
            site: SpawnSite::from_u8(s.site)
                .map_or("Unknown", SpawnSite::label),
            ts_us_ago: delta_to_us(now.saturating_duration_since(s.ts)),
        })
        .collect();

    let runtime_metrics = capture_runtime_metrics();

    SpawnRateSnapshot {
        samples_in_ring,
        ring_capacity: RING_CAPACITY,
        p50_us,
        p95_us,
        p99_us,
        by_site,
        last_n_raw,
        runtime_metrics,
    }
}

/// Capture `tokio::runtime::RuntimeMetrics` if a runtime is current,
/// otherwise return None. `Handle::try_current()` is the documented
/// way to detect "are we in a tokio context"; we never panic on no-rt.
///
/// `num_blocking_threads` and `num_idle_blocking_threads` are gated
/// by `tokio_unstable` in tokio 1.49 — the workspace
/// `.cargo/config.toml` sets `--cfg tokio_unstable` so production
/// builds expose them. Builds without that cfg will fail to compile;
/// see also the precedent at `src/bin/nativelink.rs:264-280` which
/// uses the same gated methods directly.
fn capture_runtime_metrics() -> Option<RuntimeMetricsSnapshot> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let m = handle.metrics();
    Some(RuntimeMetricsSnapshot {
        num_blocking_threads: m.num_blocking_threads(),
        num_idle_blocking_threads: m.num_idle_blocking_threads(),
        num_workers: m.num_workers(),
        num_alive_tasks: m.num_alive_tasks(),
    })
}

/// Saturating cast of a `Duration` to microseconds as `u64`. A
/// `Duration` exceeding `u64::MAX` μs (≈ 584 thousand years) clamps
/// to `u64::MAX` rather than panicking.
fn delta_to_us(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Compute (p50, p95, p99) of a slice of `u64` samples. Sorts the
/// slice in place. Returns `(0, 0, 0)` for an empty input.
///
/// Percentiles are computed via the simple "nearest-rank" method on
/// the sorted vector: `idx = ceil(p · N) − 1`, clamped to
/// `[0, N − 1]`. Suitable for diagnostic instrumentation; avoids
/// the cost of interpolation while remaining correct on the order
/// of magnitude that matters here (sub-100 μs vs sub-ms vs ms).
fn percentiles(samples: &mut [u64]) -> (u64, u64, u64) {
    if samples.is_empty() {
        return (0, 0, 0);
    }
    samples.sort_unstable();
    let n = samples.len();
    let pick = |p: f64| -> u64 {
        // ceil(p · N) − 1, clamped to [0, N-1].
        let idx_f = (p * n as f64).ceil() as i64 - 1;
        let idx = idx_f.clamp(0, n as i64 - 1) as usize;
        samples[idx]
    };
    (pick(0.50), pick(0.95), pick(0.99))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reset the global ring for test isolation. The ring is a
    /// process-global singleton, so tests that depend on its contents
    /// must run serially; this is enforced by each test that needs
    /// it via [`reset_ring`].
    fn reset_ring() {
        RING.lock().clear();
    }

    /// Tests share the global ring and therefore must run serially
    /// behind this lock to prevent interleaved record() calls
    /// from one test polluting another's snapshot.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn record_then_snapshot_returns_n_samples() {
        let _guard = TEST_LOCK.lock();
        reset_ring();
        for _ in 0..10 {
            record(SpawnSite::ChunkedShaAdmit);
        }
        let snap = snapshot(20);
        assert_eq!(snap.samples_in_ring, 10);
        assert_eq!(snap.last_n_raw.len(), 10);
        assert_eq!(snap.by_site.chunked_sha_admit, 10);
        assert_eq!(snap.by_site.chunked_sha_commit, 0);
        assert_eq!(snap.by_site.chunked_pwrite, 0);
    }

    #[test]
    fn snapshot_caps_last_n_to_samples_in_ring() {
        let _guard = TEST_LOCK.lock();
        reset_ring();
        for _ in 0..3 {
            record(SpawnSite::ChunkedPwrite);
        }
        let snap = snapshot(100);
        // Only 3 in ring; last_n_raw must NOT exceed that.
        assert_eq!(snap.last_n_raw.len(), 3);
    }

    #[test]
    fn snapshot_empty_ring_is_all_zeros() {
        let _guard = TEST_LOCK.lock();
        reset_ring();
        let snap = snapshot(10);
        assert_eq!(snap.samples_in_ring, 0);
        assert_eq!(snap.p50_us, 0);
        assert_eq!(snap.p95_us, 0);
        assert_eq!(snap.p99_us, 0);
        assert!(snap.last_n_raw.is_empty());
    }

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let _guard = TEST_LOCK.lock();
        reset_ring();
        // Push capacity + extras; each extra must evict one from front.
        for _ in 0..(RING_CAPACITY + 50) {
            record(SpawnSite::ChunkedShaAdmit);
        }
        let snap = snapshot(0);
        assert_eq!(snap.samples_in_ring, RING_CAPACITY);
        assert_eq!(snap.by_site.chunked_sha_admit, RING_CAPACITY);
    }

    #[test]
    fn multiple_sites_distribute_into_by_site() {
        let _guard = TEST_LOCK.lock();
        reset_ring();
        for _ in 0..5 {
            record(SpawnSite::ChunkedShaAdmit);
        }
        for _ in 0..3 {
            record(SpawnSite::ChunkedShaCommit);
        }
        for _ in 0..7 {
            record(SpawnSite::ChunkedPwrite);
        }
        let snap = snapshot(0);
        assert_eq!(snap.samples_in_ring, 15);
        assert_eq!(snap.by_site.chunked_sha_admit, 5);
        assert_eq!(snap.by_site.chunked_sha_commit, 3);
        assert_eq!(snap.by_site.chunked_pwrite, 7);
    }

    #[test]
    fn percentiles_nearest_rank_on_synthetic_data() {
        // 100 samples 1..=100; nearest-rank percentiles:
        //  p50 = ceil(0.50·100) = 50, idx 49 → value 50
        //  p95 = ceil(0.95·100) = 95, idx 94 → value 95
        //  p99 = ceil(0.99·100) = 99, idx 98 → value 99
        let mut data: Vec<u64> = (1..=100).collect();
        let (p50, p95, p99) = percentiles(&mut data);
        assert_eq!(p50, 50);
        assert_eq!(p95, 95);
        assert_eq!(p99, 99);
    }

    #[test]
    fn percentiles_single_value() {
        let mut data = vec![42u64];
        let (p50, p95, p99) = percentiles(&mut data);
        assert_eq!(p50, 42);
        assert_eq!(p95, 42);
        assert_eq!(p99, 42);
    }

    #[test]
    fn percentiles_empty_returns_zeros() {
        let mut data: Vec<u64> = Vec::new();
        let (p50, p95, p99) = percentiles(&mut data);
        assert_eq!(p50, 0);
        assert_eq!(p95, 0);
        assert_eq!(p99, 0);
    }

    #[test]
    fn snapshot_inter_arrival_deltas_are_monotonic_nonneg() {
        // Verifies the windows(2) inter-arrival math doesn't
        // produce wrap-around if Instants are emitted in order.
        let _guard = TEST_LOCK.lock();
        reset_ring();
        for _ in 0..50 {
            record(SpawnSite::ChunkedShaAdmit);
            // Tiny busy-wait to guarantee Instant monotonic increase
            // on systems where consecutive Instant::now() can return
            // identical values. We're testing the math, not timing
            // resolution, so cap the loop.
            let start = Instant::now();
            while Instant::now() == start { /* spin briefly */ }
        }
        let snap = snapshot(0);
        // Every percentile must be representable; nothing panics or
        // saturates to MAX on a sub-second test run.
        assert!(snap.p50_us < 1_000_000);
        assert!(snap.p99_us < 1_000_000);
        assert!(snap.p95_us >= snap.p50_us);
        assert!(snap.p99_us >= snap.p95_us);
    }

    #[test]
    fn site_label_round_trip() {
        for site in [
            SpawnSite::ChunkedShaAdmit,
            SpawnSite::ChunkedShaCommit,
            SpawnSite::ChunkedPwrite,
        ] {
            let v = site as u8;
            let back = SpawnSite::from_u8(v).expect("known site");
            assert_eq!(back, site);
            assert!(!site.label().is_empty());
        }
        assert!(SpawnSite::from_u8(0).is_none());
        assert!(SpawnSite::from_u8(99).is_none());
    }
}
