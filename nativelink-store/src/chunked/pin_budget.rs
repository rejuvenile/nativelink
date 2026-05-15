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

//! #212 Phase 2.5/2.7 fixup B1: global pinned-bytes Semaphore.
//!
//! The existing `ChunkBudget` (`chunk_budget.rs`) caps the IN-FLIGHT
//! arrival side at 4 GiB. Once a chunk lands at the per-blob driver and
//! the chunk's `OwnedSemaphorePermit` releases, the bytes still live in
//! the per-blob pin (`ChunkPin`) until the chunked driver's slow-tier
//! commit completes. **The pin itself has no global cap.**
//!
//! Under a slow-tier pause (ZFS txg pressure, FS busy, server-restart
//! drain), uploads continue arriving at line-rate while commits stall.
//! Pinned bytes grow at upload-rate × pause-duration. At 10 Gbps × 30s
//! that's ~37 GiB on top of the 4 GiB chunk budget — the same OOM-shape
//! as #203 if the Phase 2.7 kill-switch flips at non-trivial concurrency.
//!
//! `PinBudget` adds a SECOND tokio Semaphore alongside `ChunkBudget`,
//! capping the post-arrival pinned bytes globally. Each chunked-dispatch
//! admission acquires N permits (one permit = one byte) before adding
//! the chunk's bytes to the per-blob pin; the permit is released when
//! the chunked driver's commit completes (success OR failure → pin
//! cleared in `run_driver`'s post-loop block).
//!
//! Wire-format: a permit is one byte. Default cap = 8 GiB
//! (`DEFAULT_PIN_BUDGET_BYTES`); see the constant's doc-comment for the
//! 4→8 GiB bump rationale (#493). Operators can override at process
//! start; the cap is process-global, not per-store.
//!
//! Why a Semaphore not a counter+CAS:
//! - We need ATOMIC reservation: if the cap is N and two concurrent
//!   admissions each request N/2 + 1 bytes, exactly one must fail.
//!   Semaphore::try_acquire_many provides this; a counter + compare-
//!   and-swap loop with backoff would be more fragile.
//! - The same `try_acquire` (NEVER `acquire().await`) discipline as
//!   `ChunkBudget`: holding upstream-blocking state while awaiting a
//!   permit reproduces the #203 OOM trap. Admission either succeeds
//!   immediately or rejects with `Code::ResourceExhausted` +
//!   `BackpressureSignal::PinnedBytesExhausted`.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default global pin-budget cap: 8 GiB of pinned bytes.
///
/// Bumped from 4 GiB → 8 GiB per #493 after production hit the prior cap
/// (16 distinct digests rejected with `Code::ResourceExhausted: global
/// PinBudget exhausted` in a 2h window, surfacing as ByteStream::write
/// upload failures to Bazel). The pin budget covers the post-arrival
/// (waiting-for-slow-tier-commit) phase, which under slow-tier pauses
/// grows at upload-rate × pause-duration; the prior 4 GiB matched the
/// `ChunkBudget` arrival-side cap from Q4 but turned out too tight for
/// observed slow-tier hiccups under sustained line-rate ingest.
///
/// The two budgets still cover disjoint phases of a chunk's lifecycle
/// (in-flight arrival vs post-arrival pin), so the worst-case combined
/// memory budget for the chunked path is now ~12 GiB (4 GiB chunk + 8
/// GiB pin) plus the per-driver fast-tier MemoryStore pin.
pub const DEFAULT_PIN_BUDGET_BYTES: usize = 8 * 1024 * 1024 * 1024;

/// `tokio::sync::Semaphore::MAX_PERMITS` is `usize::MAX >> 3`, giving
/// us plenty of headroom even for 256 MiB blobs (one byte = one permit).
/// Documented for clarity; not load-bearing on this path.
const _: () = assert!(DEFAULT_PIN_BUDGET_BYTES < (usize::MAX >> 3));

/// Global per-process byte budget for pinned chunk bytes after
/// admission to a `ChunkedDriver`'s in-memory pin.
///
/// Wraps a `tokio::sync::Semaphore` whose total permits = byte cap
/// (one permit = one byte). Construction is via `PinBudget::new()`
/// (cheap; one `Arc<Semaphore>` allocation); production uses the
/// `pin_budget_singleton()` accessor.
///
/// Each successful `try_acquire(n_bytes)` returns an
/// `OwnedSemaphorePermit` representing exactly `n_bytes` of headroom.
/// Drop releases the permits back to the pool.
#[derive(Debug)]
pub struct PinBudget {
    /// `Arc` because every successful `try_acquire` clones the semaphore
    /// handle to mint an `OwnedSemaphorePermit` (which holds its own
    /// `Arc` to the underlying semaphore).
    sem: Arc<Semaphore>,
    /// Total bytes available at construction; used to compute the
    /// `pinned_bytes_used` gauge from the live `available_permits()`.
    capacity_bytes: usize,
    /// Cumulative count of admission rejections (insufficient permits).
    /// Incremented on every `try_acquire` `None` return; published as
    /// the `pin_budget_rejections_total` counter.
    rejections_total: AtomicU64,
}

impl PinBudget {
    /// Construct a fresh pin budget with `capacity_bytes` permits
    /// available. Production wiring uses `pin_budget_singleton()` which
    /// initializes with `DEFAULT_PIN_BUDGET_BYTES`.
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(capacity_bytes)),
            capacity_bytes,
            rejections_total: AtomicU64::new(0),
        }
    }

    /// Try to acquire `n_bytes` of pin-budget headroom. Returns
    /// `Some(permit)` on success, `None` on exhaustion. **NEVER blocks.**
    ///
    /// The caller MUST translate `None` into a `Code::ResourceExhausted`
    /// rejection carrying a `BackpressureSignal::PinnedBytesExhausted`
    /// detail — awaiting a permit while holding upstream-blocking state
    /// reproduces the #203 OOM cascade.
    ///
    /// `n_bytes == 0` returns a no-op permit (zero bytes acquired); this
    /// is defensive — production callers always pass `chunk_bytes.len()`
    /// which is `>= 1` for any non-empty chunk.
    #[must_use = "the permit must be held by the chunked driver / dropped on cleanup to release the budget"]
    pub fn try_acquire(&self, n_bytes: usize) -> Option<OwnedSemaphorePermit> {
        let n = u32::try_from(n_bytes).ok()?;
        match Arc::clone(&self.sem).try_acquire_many_owned(n) {
            Ok(permit) => Some(permit),
            Err(_) => {
                self.rejections_total.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Bytes currently available (cap minus held permits). Race-prone
    /// vs concurrent `try_acquire`; for observability + tests only.
    #[must_use]
    pub fn available_bytes(&self) -> usize {
        self.sem.available_permits()
    }

    /// Bytes currently used (cap minus available). Published as the
    /// `pinned_bytes_used` gauge.
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.capacity_bytes
            .saturating_sub(self.sem.available_permits())
    }

    /// Total cap in bytes. Pinned at construction.
    #[must_use]
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Cumulative rejection count. Published as the
    /// `pin_budget_rejections_total` counter.
    #[must_use]
    pub fn rejections_total(&self) -> u64 {
        self.rejections_total.load(Ordering::Relaxed)
    }
}

impl Default for PinBudget {
    fn default() -> Self {
        Self::new(DEFAULT_PIN_BUDGET_BYTES)
    }
}

/// Process-wide singleton holder. `OnceLock` chosen for the same
/// reasons as `chunk_budget_singleton`: the budget is a process-global
/// resource (8 GiB cap per #493 is per-process, not per-store), and
/// Phase 2.7 admission code wires from one site (the chunked dispatcher).
///
/// Storage is `Arc<PinBudget>` (not bare `PinBudget`) so the same
/// instance can be returned BOTH as a `&'static PinBudget` (for the
/// admission hot path) AND as an `Arc<PinBudget>` (for
/// `MetricsRegistry::register_dyn` at process start, which requires
/// `Arc<dyn MetricsComponent + Send + Sync>`). Without this dual
/// accessor the registry would either need a manual wrapper or end
/// up tracking a *different* `PinBudget` instance than the one
/// admissions consult — a silent measurement bug.
static PIN_BUDGET_SINGLETON: OnceLock<Arc<PinBudget>> = OnceLock::new();

fn pin_budget_arc_inner() -> &'static Arc<PinBudget> {
    PIN_BUDGET_SINGLETON.get_or_init(|| Arc::new(PinBudget::default()))
}

/// Returns the process-wide `PinBudget` singleton, initializing it on
/// first call with `DEFAULT_PIN_BUDGET_BYTES`. The cost is one
/// `OnceLock::get_or_init` (atomic load + branch in the hot path after
/// first call).
pub fn pin_budget_singleton() -> &'static PinBudget {
    pin_budget_arc_inner().as_ref()
}

/// Returns a clonable `Arc` to the same process-wide `PinBudget`
/// singleton returned by `pin_budget_singleton()`. The clone is one
/// atomic increment; use at process start to hand a clone to
/// `MetricsRegistry::register` so the `pinned_bytes_used`,
/// `pinned_bytes_capacity`, and `pin_budget_rejections_total` gauges
/// are scraped by every `/metrics` listener.
#[must_use]
pub fn pin_budget_arc() -> Arc<PinBudget> {
    Arc::clone(pin_budget_arc_inner())
}

/// Manual `MetricsComponent` impl: the metrics published are NOT
/// stored fields (one is derived from the live Semaphore), so the
/// derive macro cannot generate them.
///
/// - `pinned_bytes_used` — gauge derived from `used_bytes()`. Live
///   in-flight pin consumption; combined with `chunk_budget_used_bytes`
///   gives the total chunked-path memory footprint.
/// - `pinned_bytes_capacity` — pinned cap (constant after construction;
///   exported for the operator dashboard).
/// - `pin_budget_rejections_total` — monotone counter from the atomic.
///   Non-zero means the pin cap is being hit; signal for capacity
///   planning AND for the #203-class OOM defense ("we kept growth
///   bounded by rejecting admission, not by OOM").
impl MetricsComponent for PinBudget {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let used = self.used_bytes() as u64;
        let cap = self.capacity_bytes as u64;
        let rejections = self.rejections_total();
        nativelink_metric::publish!(
            "pinned_bytes_used",
            &used,
            nativelink_metric::MetricKind::Default,
            "Bytes currently held by chunked-driver pins (gauge derived from the global PinBudget Semaphore; cap = DEFAULT_PIN_BUDGET_BYTES, default 8 GiB)"
        );
        nativelink_metric::publish!(
            "pinned_bytes_capacity",
            &cap,
            nativelink_metric::MetricKind::Default,
            "Total pinned-bytes cap (constant after process start; default DEFAULT_PIN_BUDGET_BYTES = 8 GiB)"
        );
        nativelink_metric::publish!(
            "pin_budget_rejections_total",
            &rejections,
            nativelink_metric::MetricKind::Counter,
            "Cumulative count of chunked-dispatch admissions rejected because the global pinned-bytes budget was exhausted (#212 fixup B1; anti-#203 OOM defense)"
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PIN_BUDGET_BYTES, PinBudget, pin_budget_arc, pin_budget_singleton};

    /// Sanity: a fresh budget has the requested capacity and zero
    /// rejections.
    #[test]
    fn fresh_budget_has_full_capacity() {
        let b = PinBudget::new(1024);
        assert_eq!(b.available_bytes(), 1024);
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(b.capacity_bytes(), 1024);
        assert_eq!(b.rejections_total(), 0);
    }

    /// `try_acquire(n)` succeeds when `n <= available`, fails when
    /// `n > available`, and bumps the rejection counter on failure.
    #[test]
    fn try_acquire_succeeds_until_exhaustion() {
        let b = PinBudget::new(100);
        let p1 = b.try_acquire(60).expect("first 60 bytes");
        assert_eq!(b.used_bytes(), 60);
        let p2 = b.try_acquire(40).expect("remaining 40 bytes");
        assert_eq!(b.used_bytes(), 100);
        // Cap exhausted; next request fails.
        assert!(
            b.try_acquire(1).is_none(),
            "1 byte beyond cap must reject",
        );
        assert_eq!(b.rejections_total(), 1);
        // Drop one permit; capacity returns.
        drop(p1);
        assert_eq!(b.used_bytes(), 40);
        // Re-acquire succeeds without bumping the rejection counter.
        let _re = b.try_acquire(50).expect("50 bytes back available");
        assert_eq!(b.rejections_total(), 1);
        drop(p2);
        drop(_re);
    }

    /// Even a request that exceeds the entire capacity rejects cleanly
    /// (instead of e.g. clamping or wrapping) — the caller is expected
    /// to back off and retry once existing pins drain.
    #[test]
    fn try_acquire_above_capacity_rejects_cleanly() {
        let b = PinBudget::new(100);
        assert!(b.try_acquire(101).is_none(), "above-cap request rejects");
        assert_eq!(b.rejections_total(), 1);
        // Subsequent within-cap request still works.
        let _p = b.try_acquire(50).expect("within-cap still works");
    }

    /// Singleton accessor returns the same `&'static PinBudget` across
    /// calls — the load-bearing property the metric wiring depends on.
    #[test]
    fn singleton_returns_same_reference_across_calls() {
        let a = pin_budget_singleton();
        let b = pin_budget_singleton();
        assert!(
            core::ptr::eq(a, b),
            "OnceLock singleton must return the same reference",
        );
        assert_eq!(a.capacity_bytes(), DEFAULT_PIN_BUDGET_BYTES);
    }

    /// `pin_budget_arc()` returns an `Arc` pointing at the SAME
    /// `PinBudget` instance the `&'static` accessor returns. Without
    /// this property, `MetricsRegistry::register_dyn` would publish
    /// gauges from a different `Semaphore` than the one admissions
    /// consume from — a silent measurement bug.
    #[test]
    fn singleton_arc_and_ref_point_to_same_instance() {
        let ref_ptr: *const PinBudget = pin_budget_singleton();
        let arc = pin_budget_arc();
        let arc_ptr: *const PinBudget = &*arc;
        assert!(
            core::ptr::eq(ref_ptr, arc_ptr),
            "pin_budget_arc() and pin_budget_singleton() must alias the same PinBudget instance",
        );
    }

    /// Zero-byte acquire is a no-op permit (zero bytes consumed).
    /// Defensive — production callers pass `chunk_bytes.len() >= 1`.
    #[test]
    fn zero_byte_acquire_is_noop() {
        let b = PinBudget::new(100);
        let p = b.try_acquire(0).expect("zero-byte permit");
        assert_eq!(b.used_bytes(), 0);
        drop(p);
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(b.rejections_total(), 0);
    }

    /// The default-derived budget matches `DEFAULT_PIN_BUDGET_BYTES`.
    #[test]
    fn default_matches_constant() {
        let b = PinBudget::default();
        assert_eq!(b.capacity_bytes(), DEFAULT_PIN_BUDGET_BYTES);
        assert_eq!(b.available_bytes(), DEFAULT_PIN_BUDGET_BYTES);
    }

    /// Regression: `DEFAULT_PIN_BUDGET_BYTES` MUST be 8 GiB per #493 (the
    /// 2026-05-12 attempt at this same bump silently failed because only
    /// doc-comments were updated, not the actual constant; reviewers
    /// trusted the rewritten doc and missed it). This test pins the
    /// LITERAL EXPRESSION at the declaration site so doc-only drift can
    /// no longer mask a stale constant.
    #[test]
    fn default_cap_is_8_gib() {
        assert_eq!(
            DEFAULT_PIN_BUDGET_BYTES,
            8 * 1024 * 1024 * 1024,
            "DEFAULT_PIN_BUDGET_BYTES regression: must remain 8 GiB per #493 (2026-05-12 incident: prior bump landed in doc-comments only, not constant)"
        );
    }

    /// #463-sibling-B (memory at-capacity diagnosis) under-action gate:
    /// `PinBudget::publish` must emit `pinned_bytes_used`,
    /// `pinned_bytes_capacity`, and `pin_budget_rejections_total`
    /// END-TO-END through the same `MetricsRegistry` + `render_prometheus`
    /// path the production `/metrics` listener uses.
    ///
    /// Why this test, given the unit-level publish coverage above:
    /// the existing tests cover the budget's INTERNAL state transitions
    /// (acquire/release, cap exhaustion, rejection counter). They do NOT
    /// cover the `publish()` -> registry -> Prometheus exposition seam.
    /// Per CLAUDE.md asymmetric-contract discipline, the under-action
    /// gap is "publish silently emits nothing" — invisible to in-process
    /// callers, fatal for operator scrapes. This test crosses the same
    /// seam an operator scrapes.
    ///
    /// Mutation step: comment out the `nativelink_metric::publish!` for
    /// `pinned_bytes_used` in the `MetricsComponent for PinBudget` impl
    /// (~`pin_budget.rs:230`). This test must red-fail with the bespoke
    /// "#463-sibling-B" message so a future regression triage points
    /// straight at the contract.
    #[test]
    fn publish_emits_gauges_via_render_prometheus() {
        use std::sync::Arc;

        use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};

        let budget = Arc::new(PinBudget::new(1024));
        // Consume some bytes so `pinned_bytes_used` has a non-zero value
        // we can assert on (separates "publish emitted nothing" from
        // "publish emitted zero").
        let _hold = budget.try_acquire(256).expect("256-byte permit");

        let registry = MetricsRegistry::new();
        registry.register("chunked_pin_budget", budget.clone());
        let body = render_prometheus(&registry);

        assert!(
            body.contains("chunked_pin_budget_pinned_bytes_used"),
            "#463-sibling-B: PinBudget::publish must emit pinned_bytes_used \
             so the memory at-capacity diagnosis can verify the global pinned-bytes \
             gauge end-to-end via /metrics. body=\n{body}"
        );
        assert!(
            body.contains("chunked_pin_budget_pinned_bytes_capacity"),
            "#463-sibling-B: PinBudget::publish must emit pinned_bytes_capacity \
             so the memory at-capacity diagnosis can verify the cap end-to-end \
             via /metrics. body=\n{body}"
        );
        assert!(
            body.contains("chunked_pin_budget_pin_budget_rejections_total"),
            "#463-sibling-B: PinBudget::publish must emit pin_budget_rejections_total \
             so the memory at-capacity diagnosis can verify rejection-pressure \
             end-to-end via /metrics. body=\n{body}"
        );

        // Belt-and-braces: the gauge value reflects live state, not a
        // snapshot taken at registration. 256 was acquired above; the
        // capacity is 1024.
        assert!(
            body.contains("\nchunked_pin_budget_pinned_bytes_used 256\n"),
            "#463-sibling-B: pinned_bytes_used gauge must reflect live used state \
             (expected 256 of 1024). body=\n{body}"
        );
        assert!(
            body.contains("\nchunked_pin_budget_pinned_bytes_capacity 1024\n"),
            "#463-sibling-B: pinned_bytes_capacity gauge must reflect configured cap \
             (expected 1024). body=\n{body}"
        );
    }
}
