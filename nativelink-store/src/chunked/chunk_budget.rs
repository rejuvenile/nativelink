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

//! Global byte-budget Semaphore for the #212 chunked architecture.
//!
//! Per design §13.1.1 (multi-axis backpressure spec):
//!
//! 1. Each in-flight chunk holds exactly one permit. One permit = one
//!    `CHUNK_SIZE` (1 MiB). The 4 GiB cap from Q4 maps to `4 GiB /
//!    CHUNK_SIZE = 4096 permits` total.
//! 2. The admission path is `try_acquire` ONLY — never `await` on a
//!    permit. Awaiting a permit while holding upstream-blocking state
//!    is the #203 OOM trap; a fresh upstream RPC must be rejected with
//!    `Code::ResourceExhausted` instead. Phase 2 admission code is the
//!    sole call-site; see `BackpressureSignal` in nativelink-proto for
//!    the wire-stable detail message.
//! 3. The permit is owned by the per-blob `ChunkWork` until the chunk
//!    completes (success or abandon). Drop releases the permit; there
//!    is no separate "driver-level" permit pool.
//!
//! NOT yet wired into any data path. Phase 2 will plumb the singleton
//! into `FastSlowStore::update` (admission point) and into the per-blob
//! driver task. The struct lives behind the `chunked_fast_slow`
//! feature.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::CHUNK_SIZE;

/// Total in-flight chunk permits. `4 GiB / CHUNK_SIZE` per Q4
/// (aggressive concurrency). The constant lives here (not on
/// `ChunkBudget::new`) so the metric formula
/// `chunk_budget_used_bytes = (TOTAL - available) * CHUNK_SIZE` has
/// a single source of truth.
pub(crate) const TOTAL_CHUNK_PERMITS: usize = (4 * 1024 * 1024 * 1024) / CHUNK_SIZE;

/// Global per-process byte budget for in-flight chunked traffic.
///
/// Wraps a `tokio::sync::Semaphore` with `TOTAL_CHUNK_PERMITS` initial
/// permits. Construction is via `ChunkBudget::new()` (cheap; a single
/// `Arc<Semaphore>` allocation). Every `try_acquire_chunk` returns an
/// owned permit on success, or `None` on exhaustion — the caller MUST
/// translate `None` into a `Code::ResourceExhausted` rejection
/// carrying a `BackpressureSignal` detail (see Phase 2).
///
/// Why an `OwnedSemaphorePermit` and not a borrowed `SemaphorePermit`:
/// the permit must be moved into a `ChunkWork` that travels through
/// the per-blob mpsc into the spawned driver task (`'static` bound on
/// `tokio::spawn`), so a borrowed permit cannot work. Cost: one
/// `Arc::clone` per admission, negligible vs the chunk's wire/disk
/// I/O.
#[derive(Debug)]
pub(crate) struct ChunkBudget {
    /// `Arc` because every successful `try_acquire_chunk` clones the
    /// semaphore handle to mint an `OwnedSemaphorePermit` (which holds
    /// its own `Arc` to the underlying semaphore).
    sem: Arc<Semaphore>,
    /// Cumulative count of admission rejections (no permit available).
    /// Phase 2 will increment this from the admission path; today
    /// `try_acquire_chunk` increments on its own `None` return so the
    /// metric is meaningful from the moment the budget is wired.
    rejections_total: AtomicU64,
}

impl ChunkBudget {
    /// Construct a fresh chunk budget with `TOTAL_CHUNK_PERMITS` permits
    /// available. Singletons are created via `ChunkBudget::new()` on
    /// the constructor of whichever owner first registers the budget;
    /// see Phase 2 wiring for the chosen ownership model.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            sem: Arc::new(Semaphore::new(TOTAL_CHUNK_PERMITS)),
            rejections_total: AtomicU64::new(0),
        }
    }

    /// Try to acquire one chunk-worth of budget. Returns `Some(permit)`
    /// on success, `None` on exhaustion. **NEVER blocks.** The caller
    /// MUST translate a `None` into a `Code::ResourceExhausted`
    /// rejection (with a `BackpressureSignal` detail) per §13.1.1
    /// point 1 — awaiting a permit while holding upstream-blocking
    /// state reproduces the #203 OOM cascade.
    ///
    /// The returned `OwnedSemaphorePermit` releases the permit when
    /// dropped, including via the `ChunkWork` that owns it being
    /// dropped on driver-task panic — this is what makes the budget
    /// safe under arbitrary task death (see §6.7 termination triggers).
    #[must_use = "the permit must be held by the ChunkWork or dropped explicitly to release budget"]
    pub(crate) fn try_acquire_chunk(&self) -> Option<OwnedSemaphorePermit> {
        match Arc::clone(&self.sem).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                self.rejections_total.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Number of permits currently available (i.e. chunks the budget
    /// could admit right now). Used for observability and tests; NOT
    /// race-free vs concurrent admission — a value of `N` does NOT
    /// guarantee the next `N` `try_acquire_chunk` calls all succeed.
    #[must_use]
    pub(crate) fn available_chunks(&self) -> usize {
        self.sem.available_permits()
    }

    /// Cumulative rejection count for tests + the metrics gauge wiring
    /// in Phase 2. Phase 2's admission path will also increment this
    /// when the per-blob mpsc rejects (after releasing the permit per
    /// §13.1.1 point 1 step 2).
    #[must_use]
    #[allow(dead_code, reason = "wired in Phase 2 admission path")]
    pub(crate) fn rejections_total(&self) -> u64 {
        self.rejections_total.load(Ordering::Relaxed)
    }
}

impl Default for ChunkBudget {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-wide singleton holder. `OnceLock` chosen over constructor
/// injection because:
/// - The budget is a process-global resource (4 GiB cap is per-process,
///   not per-store).
/// - Phase 2 wires admission from MULTIPLE owners (FastSlowStore::update,
///   WorkerProxyStore::get_part_and_cache, the WriteChunked RPC handler);
///   constructor injection would require threading the budget through
///   every call-site.
/// - The metric publication path (Phase 2) uses
///   `chunk_budget_singleton().publish(...)` from a parent
///   `MetricsComponent`-deriving struct that holds an `&'static
///   ChunkBudget` field.
///
/// `OnceLock::get_or_init` is lock-free after first init and
/// thread-safe; the contract matches the per-process singleton
/// semantics.
static CHUNK_BUDGET_SINGLETON: OnceLock<ChunkBudget> = OnceLock::new();

/// Returns the process-wide `ChunkBudget` singleton, initializing it
/// on first call. Phase 2 admission code calls this once per chunk
/// arrival; the cost is one `OnceLock::get_or_init` (atomic load + a
/// branch in the hot path after first call).
#[allow(dead_code, reason = "Phase 1 SKELETON; consumers land in Phase 2 (#212)")]
pub(crate) fn chunk_budget_singleton() -> &'static ChunkBudget {
    CHUNK_BUDGET_SINGLETON.get_or_init(ChunkBudget::new)
}

/// Manual `MetricsComponent` impl: the two metrics published are NOT
/// stored fields (one is derived from the live Semaphore, the other is
/// the live atomic), so the derive macro cannot generate them.
///
/// - `chunk_budget_used_bytes` — gauge derived as
///   `(TOTAL_CHUNK_PERMITS - available_chunks()) * CHUNK_SIZE`.
///   This is the live in-flight byte consumption attributable to the
///   chunked path. Required to verify the §4.13 implication 2 memory
///   bound (12-16 GiB worst-case server RSS) holds in production.
/// - `chunk_resource_exhausted_rejections_total` — counter from the
///   atomic. Non-zero means the global budget is being hit; high
///   signal for capacity planning.
impl MetricsComponent for ChunkBudget {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        // The wrapping `MetricsComponent` derive on the parent struct
        // (Phase 2 will pick the host struct) will recurse into this
        // impl and publish each named field separately via
        // `nativelink_metric::publish!` macros. For now we expose the
        // budget as a Component so a parent derive can group it.
        let used_chunks = TOTAL_CHUNK_PERMITS.saturating_sub(self.available_chunks());
        let used_bytes = used_chunks as u64 * CHUNK_SIZE as u64;
        let rejections = self.rejections_total();
        // Publish as nested fields under the parent group.
        nativelink_metric::publish!(
            "chunk_budget_used_bytes",
            &used_bytes,
            // Gauge — the value goes UP and DOWN with permit
            // acquire/release. `nativelink_metric` has no dedicated
            // `Gauge` variant; `Default` is the convention for
            // non-monotone numeric metrics (the sibling
            // `chunk_resource_exhausted_rejections_total` IS a
            // monotone counter and uses `Counter`).
            nativelink_metric::MetricKind::Default,
            "Bytes currently held by in-flight chunked transfers (gauge derived from the global Semaphore; upper bound = 4 GiB per #212 Q4)"
        );
        nativelink_metric::publish!(
            "chunk_resource_exhausted_rejections_total",
            &rejections,
            nativelink_metric::MetricKind::Counter,
            "Cumulative count of chunk admissions rejected because the global byte budget was exhausted (#212 Q8 backpressure)"
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[cfg(test)]
mod tests {
    use super::{CHUNK_SIZE, ChunkBudget, TOTAL_CHUNK_PERMITS, chunk_budget_singleton};

    /// Sanity: the budget is 4096 permits at construction.
    #[test]
    fn fresh_budget_has_full_permits() {
        let b = ChunkBudget::new();
        assert_eq!(b.available_chunks(), TOTAL_CHUNK_PERMITS);
        assert_eq!(b.rejections_total(), 0);
    }

    /// Per Q3=(a) and Q4: 4 GiB / 1 MiB = 4096 permits.
    #[test]
    fn total_permits_match_design() {
        assert_eq!(TOTAL_CHUNK_PERMITS, 4096);
        assert_eq!(TOTAL_CHUNK_PERMITS * CHUNK_SIZE, 4 * 1024 * 1024 * 1024);
    }

    /// `try_acquire_chunk` succeeds N times, fails N+1th, and
    /// `available_chunks` reports correctly throughout. This is the
    /// load-bearing contract for Phase 2 admission code.
    #[test]
    fn try_acquire_succeeds_until_exhaustion() {
        let b = ChunkBudget::new();
        let mut permits = Vec::with_capacity(TOTAL_CHUNK_PERMITS);
        for i in 0..TOTAL_CHUNK_PERMITS {
            let p = b
                .try_acquire_chunk()
                .unwrap_or_else(|| panic!("permit {i}/{TOTAL_CHUNK_PERMITS} must be available"));
            permits.push(p);
        }
        assert_eq!(b.available_chunks(), 0);
        assert_eq!(b.rejections_total(), 0);
        // The N+1th attempt must fail and bump the rejection counter.
        assert!(
            b.try_acquire_chunk().is_none(),
            "budget exhausted; admission must reject"
        );
        assert_eq!(b.rejections_total(), 1);
        // Drop one permit; one slot becomes available again.
        drop(permits.pop());
        assert_eq!(b.available_chunks(), 1);
        // Re-acquire succeeds without bumping the rejection counter.
        let _re = b.try_acquire_chunk().expect("permit must be re-acquired");
        assert_eq!(b.rejections_total(), 1);
        // Drop the rest to release.
        drop(permits);
    }

    /// The rejection counter is monotone. After 100 failed admissions
    /// it reads exactly 100. This is the signal Phase 2 will export
    /// for `chunk_resource_exhausted_rejections_total`.
    #[test]
    fn rejections_counter_is_monotone() {
        let b = ChunkBudget::new();
        // Drain all permits.
        let _holders: Vec<_> = (0..TOTAL_CHUNK_PERMITS)
            .map(|_| b.try_acquire_chunk().expect("drain must succeed"))
            .collect();
        for _ in 0..100 {
            assert!(b.try_acquire_chunk().is_none());
        }
        assert_eq!(b.rejections_total(), 100);
    }

    /// Singleton accessor returns the same `&'static ChunkBudget`
    /// across calls — the load-bearing property the metric wiring
    /// depends on (parent struct holds `&'static ChunkBudget`, so a
    /// fresh budget per get would surface zero-permit metrics).
    #[test]
    fn singleton_returns_same_reference_across_calls() {
        let a = chunk_budget_singleton();
        let b = chunk_budget_singleton();
        assert!(
            core::ptr::eq(a, b),
            "OnceLock singleton must return the same reference",
        );
        // And that reference behaves like a fresh budget (or one
        // already partially consumed by another test — we don't
        // assume the count, only that the contract holds).
        let _hold = a.try_acquire_chunk();
        // Drop happens at end-of-test.
    }

    /// Smaller-scale variant of the above so the test file documents
    /// the API at-a-glance without a 4096-iteration loop dominating
    /// reading time.
    #[test]
    fn small_scale_admission_lifecycle() {
        let b = ChunkBudget::new();
        let initial = b.available_chunks();
        let p1 = b.try_acquire_chunk().expect("first permit");
        assert_eq!(b.available_chunks(), initial - 1);
        let p2 = b.try_acquire_chunk().expect("second permit");
        assert_eq!(b.available_chunks(), initial - 2);
        drop(p1);
        assert_eq!(b.available_chunks(), initial - 1);
        drop(p2);
        assert_eq!(b.available_chunks(), initial);
    }
}
