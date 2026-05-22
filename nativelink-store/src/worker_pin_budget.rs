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

//! #549 Phase 2: worker-side pinned-bytes Semaphore (observation-only).
//!
//! ## Why this exists
//!
//! #549 is Phase 2 of the architectural shift that relaxes worker pin
//! lifetime from BIS-ack-bound (today's ~46.6s mean per #547 24h data) to
//! tonic-Ok-bound (Phase 4 / #551). Once Phase 4 lands the steady-state
//! pin lifetime collapses to microseconds; if that path is correct the
//! pin set never grows large. If it is NOT correct (server hang after
//! tonic-Ok, network loss between worker and server, bug in the
//! tonic-Ok-bound release trigger), pins accumulate indefinitely because
//! the worker's `FilesystemStore` pins are unbounded.
//!
//! `WorkerPinBudget` adds a defense-in-depth `tokio::sync::Semaphore`
//! sized in bytes, sitting alongside every worker-side
//! `pin_digests` / `pin_digest` call. The existing 120s pin TTL at
//! `nativelink-util/src/moka_evicting_map.rs:50` stays as a separate
//! backstop; the LRU-cap (`FilesystemStore::max_bytes`, 20 GiB default)
//! stays as the on-disk cap. The pin-budget Semaphore caps the
//! IN-MEMORY pin-bookkeeping that protects those bytes from LRU until
//! the BIS-ack (today) or tonic-Ok (Phase 4) releases them.
//!
//! ## BUILD + OBSERVE mode (today)
//!
//! Per user authorization 2026-05-21, this lands in OBSERVATION-ONLY mode:
//!
//! - The default cap (`DEFAULT_WORKER_PIN_BUDGET_BYTES = 128 GiB`) is
//!   well above the worst-case observed per-worker pin set so
//!   production today CANNOT trigger rejections. The 2026-05-21
//!   10-worker live scrape of `worker_pin_inflight_admission_bytes`
//!   showed 76.3% of samples ≤ 32 GiB and 100% ≤ 64 GiB (worst case
//!   landed in the 32-64 GiB bucket). 128 GiB = 2× that worst-case
//!   bucket boundary — at 64 GiB the gate would have fired during
//!   BIS-ack tails; 128 GiB gives clean observation-only headroom
//!   until Phase 4 (#551) lowers the cap.
//! - The guard returned by `try_acquire` is dropped immediately at every
//!   wired call site (the worker code does NOT yet hold the guard for
//!   the pin lifetime). This is intentional: Phase 4 (#551) will flip
//!   the lifecycle by holding the guard alongside the pin.
//! - To make the metric useful in observation-only mode (the
//!   Semaphore's `available_permits()` would always be at-cap if guards
//!   drop immediately), a separate `inflight_admission_bytes`
//!   `AtomicU64` is incremented at acquire and decremented on guard
//!   drop. Today this gauge oscillates near zero because guards drop
//!   immediately; once Phase 4 holds guards across the pin lifetime the
//!   gauge becomes "currently-pinned bytes the worker tracked."
//! - The `worker_pin_admission_bytes_total` counter monotonically tracks
//!   total bytes acquired (admission throughput); the
//!   `worker_pin_budget_rejections_total` counter monotonically tracks
//!   over-cap rejection attempts. Both are observable today even with
//!   immediate-drop semantics.
//!
//! ## Composite invariant (Admission/Eviction/Pin triangle)
//!
//! - **Admission gate**: `WorkerPinBudget::try_acquire` (this file).
//!   Today: never rejects (128 GiB cap >> 64 GiB worst-case bucket
//!   observed). Phase 4 may tighten.
//! - **Eviction**: `FilesystemStore` LRU at `max_bytes` (20 GiB default)
//!   plus the 120s `PIN_TIMEOUT_SECS` backstop in
//!   `nativelink-util/src/moka_evicting_map.rs:50`.
//! - **Pin**: `FilesystemStore::pin_digests` (the wrapped call). Today
//!   the pin extends to BIS-ack; Phase 4 trims to tonic-Ok.
//!
//! Composite: `gate-active ⇒ pin-works AND ttl-fires AND lru-evicts`.
//! In observation-only mode the gate is never active, so the composite
//! is vacuously satisfied.
//!
//! ## Wire-format mirror of `chunked::pin_budget`
//!
//! Structure mirrors `nativelink-store/src/chunked/pin_budget.rs`
//! exactly (Semaphore + OnceLock singleton + MetricsComponent impl);
//! the differences are (a) the singleton is a SEPARATE process-global
//! from the server-side `pin_budget_singleton()` (workers and servers
//! have disjoint pin pressures), (b) the metric names are
//! `worker_*`-prefixed for operator disambiguation, (c) one extra
//! atomic + counter for the observation-only mode, and (d) the
//! `try_acquire` path splits requests > `u32::MAX` into multiple
//! `OwnedSemaphorePermit`s held inside the guard (`chunked::pin_budget`
//! does not — its callers are bounded by `CHUNK_SIZE`).

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default worker pinned-bytes cap: 128 GiB.
///
/// Chosen 2026-05-22 from a 10-worker live scrape of
/// `worker_pin_inflight_admission_bytes` after the Phase 2 wiring
/// landed: 76.3% of samples ≤ 32 GiB and 100% ≤ 64 GiB (worst-case
/// landed inside the 32-64 GiB bucket). The cap is set to 2× the
/// worst-case bucket boundary so the gate remains vacuously satisfied
/// in observation-only mode: at 64 GiB the gate would have fired
/// during BIS-ack tails right at the observed edge. Phase 4 (#551)
/// may revisit once the tonic-Ok-bound release trigger lands and the
/// steady-state pin set drops by orders of magnitude.
///
/// Per CLAUDE.md "Reviewer prompt for any diff that claims to change a
/// numeric constant": the regression test `default_cap_is_128_gib`
/// pins the literal expression on the line below so doc-only drift
/// cannot mask a stale constant (2026-05-12 lesson: prior
/// `DEFAULT_PIN_BUDGET_BYTES` bump landed in doc-comments only;
/// reviewers trusted the rewritten doc and missed the bare constant).
pub const DEFAULT_WORKER_PIN_BUDGET_BYTES: usize = 128 * 1024 * 1024 * 1024;

/// `tokio::sync::Semaphore::MAX_PERMITS` is `usize::MAX >> 3`, giving
/// us plenty of headroom even at the 128 GiB cap (one byte = one
/// permit).
const _: () = assert!(DEFAULT_WORKER_PIN_BUDGET_BYTES < (usize::MAX >> 3));

/// Worker-side per-process byte budget for pinned bytes in the
/// `FilesystemStore` LRU.
///
/// Wraps a `tokio::sync::Semaphore` whose total permits = byte cap
/// (one permit = one byte). Construction is via `WorkerPinBudget::new()`
/// (cheap; one `Arc<Semaphore>` allocation); production uses the
/// `worker_pin_budget_singleton()` accessor.
///
/// **Phase 4 enforcement prerequisite (multi-GB digests):**
/// `try_acquire(n_bytes)` splits `n_bytes` across multiple
/// `OwnedSemaphorePermit` chunks of up to `u32::MAX` permits each
/// (`tokio::sync::Semaphore::try_acquire_many_owned` takes a `u32`).
/// Today (observation-only) a digest > `u32::MAX` would be ~4 GiB,
/// which is rare and the gate's None return is harmless (the pin
/// proceeds anyway). Once Phase 4 (#551) lands and the cap binds,
/// callers will hold the guard for the pin lifetime and a partial
/// failure mid-split must drop already-acquired chunks before
/// returning None — the loop in `try_acquire` does this by dropping
/// `acquired` on the early return.
///
/// Each successful `try_acquire(n_bytes)` returns a
/// `WorkerPinBudgetGuard` representing exactly `n_bytes` of headroom.
/// Drop releases the permits back to the pool AND decrements
/// `inflight_admission_bytes`.
#[derive(Debug)]
pub struct WorkerPinBudget {
    /// `Arc` because every successful `try_acquire` clones the semaphore
    /// handle to mint an `OwnedSemaphorePermit` (held inside the guard).
    sem: Arc<Semaphore>,
    /// Total bytes available at construction; used to compute the
    /// `worker_pinned_bytes_used` gauge from the live
    /// `available_permits()`.
    capacity_bytes: usize,
    /// Cumulative count of admission rejections (insufficient permits).
    /// Incremented on every `try_acquire` `None` return; published as
    /// the `worker_pin_budget_rejections_total` counter.
    rejections_total: AtomicU64,
    /// Cumulative bytes successfully admitted. Published as the
    /// `worker_pin_admission_bytes_total` counter. Useful in
    /// observation-only mode where the Semaphore's
    /// `available_permits()` oscillates because guards drop immediately
    /// at the call sites.
    admission_bytes_total: AtomicU64,
    /// Live count of bytes held by outstanding guards (acquire +=,
    /// drop -=). In observation-only mode this is near-zero because
    /// guards drop immediately; once Phase 4 holds guards across the
    /// pin lifetime this becomes the "live tracked pinned bytes"
    /// gauge.
    inflight_admission_bytes: Arc<AtomicU64>,
}

/// RAII guard returned by [`WorkerPinBudget::try_acquire`]. Holds the
/// underlying `OwnedSemaphorePermit`(s) for the duration of the pin
/// and decrements `inflight_admission_bytes` on drop.
///
/// In #549 BUILD + OBSERVE mode, callers drop the guard immediately
/// (observation-only). In Phase 4 (#551), callers will hold the guard
/// for the pin lifetime so the gauge reflects live pinned bytes.
///
/// **Multi-permit storage (Phase 4 prerequisite, #549 fix-up A-2,
/// 2026-05-22):** `tokio::sync::Semaphore::try_acquire_many_owned`
/// takes a `u32`, but a single worker-pin digest can exceed `u32::MAX`
/// bytes (4 GiB) under chunked uploads. The guard stores a `Vec` of
/// permits so a digest of `n_bytes` is split into
/// `ceil(n_bytes / u32::MAX as usize)` chunks at acquire and all
/// chunks release together at drop. In observation-only mode the cap
/// is so far above any plausible digest size that this matters only
/// for correctness (no silent rejection); Phase 4 (#551) makes it
/// load-bearing — a multi-GB digest that today rejected silently via
/// the prior `u32::try_from(...).ok()?` would have leaked admission
/// rejections without ever bumping `rejections_total`.
#[derive(Debug)]
pub struct WorkerPinBudgetGuard {
    /// Held for the pin lifetime; drop releases all Semaphore permits.
    /// `Vec` because a single `n_bytes` request may exceed `u32::MAX`
    /// and `try_acquire_many_owned` takes a `u32` — see struct-level
    /// doc-comment.
    _permits: Vec<OwnedSemaphorePermit>,
    /// Shared with the budget; decremented on drop by `n_bytes`.
    inflight_admission_bytes: Arc<AtomicU64>,
    /// Bytes this guard reserved; subtracted from the live gauge on
    /// drop.
    n_bytes: u64,
}

impl Drop for WorkerPinBudgetGuard {
    fn drop(&mut self) {
        self.inflight_admission_bytes
            .fetch_sub(self.n_bytes, Ordering::Relaxed);
    }
}

impl WorkerPinBudget {
    /// Construct a fresh worker pin budget with `capacity_bytes`
    /// permits available. Production wiring uses
    /// `worker_pin_budget_singleton()` which initializes with
    /// `DEFAULT_WORKER_PIN_BUDGET_BYTES`.
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(capacity_bytes)),
            capacity_bytes,
            rejections_total: AtomicU64::new(0),
            admission_bytes_total: AtomicU64::new(0),
            inflight_admission_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Try to acquire `n_bytes` of pin-budget headroom. Returns
    /// `Some(guard)` on success, `None` on exhaustion. **NEVER blocks.**
    ///
    /// On success: increments `admission_bytes_total` AND
    /// `inflight_admission_bytes` by `n_bytes`. The returned guard's
    /// `Drop` decrements `inflight_admission_bytes` again.
    ///
    /// `n_bytes == 0` returns a no-op guard (zero bytes acquired); this
    /// is defensive — production callers always pass `digest.size_bytes()`
    /// which is `>= 1` for any non-empty blob.
    ///
    /// **Multi-permit splitting (#549 fix-up A-2):**
    /// `tokio::sync::Semaphore::try_acquire_many_owned` takes a `u32`,
    /// so for `n_bytes > u32::MAX` we acquire multiple permits of up
    /// to `u32::MAX` permits each and store them all in the guard.
    /// If any chunk acquisition fails partway through, the already-
    /// acquired permits are dropped (releasing them back to the pool)
    /// and `rejections_total` is bumped exactly once. Today (observation-
    /// only, 128 GiB cap) this matters only for correctness; Phase 4
    /// (#551) makes it load-bearing because the cap will bind.
    #[must_use = "the guard must be held for the pin lifetime; dropping it releases the budget"]
    pub fn try_acquire(&self, n_bytes: usize) -> Option<WorkerPinBudgetGuard> {
        let n_u64 = n_bytes as u64;
        let mut remaining = n_bytes;
        let mut acquired: Vec<OwnedSemaphorePermit> = Vec::new();
        while remaining > 0 {
            // Cast is safe: `chunk` is `min(remaining, u32::MAX as usize)`,
            // and `u32::MAX as usize` fits `u32` by construction.
            let chunk = core::cmp::min(remaining, u32::MAX as usize) as u32;
            match Arc::clone(&self.sem).try_acquire_many_owned(chunk) {
                Ok(permit) => {
                    acquired.push(permit);
                    remaining -= chunk as usize;
                }
                Err(_) => {
                    // Drop already-acquired permits (releases them back
                    // to the pool) before reporting rejection.
                    drop(acquired);
                    self.rejections_total.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        }
        self.admission_bytes_total
            .fetch_add(n_u64, Ordering::Relaxed);
        self.inflight_admission_bytes
            .fetch_add(n_u64, Ordering::Relaxed);
        Some(WorkerPinBudgetGuard {
            _permits: acquired,
            inflight_admission_bytes: Arc::clone(&self.inflight_admission_bytes),
            n_bytes: n_u64,
        })
    }

    /// Bytes currently available in the Semaphore (cap minus held
    /// permits). Race-prone vs concurrent `try_acquire`; for
    /// observability + tests only.
    #[must_use]
    pub fn available_bytes(&self) -> usize {
        self.sem.available_permits()
    }

    /// Semaphore-derived used bytes (cap minus available permits).
    /// Published as the `worker_pinned_bytes_used` gauge. In
    /// observation-only mode this is near-zero because guards drop
    /// immediately; Phase 4 makes it useful.
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

    /// Cumulative rejection count. Published as
    /// `worker_pin_budget_rejections_total`.
    #[must_use]
    pub fn rejections_total(&self) -> u64 {
        self.rejections_total.load(Ordering::Relaxed)
    }

    /// Cumulative bytes successfully admitted. Published as
    /// `worker_pin_admission_bytes_total`. Tracks admission throughput
    /// even in observation-only mode.
    #[must_use]
    pub fn admission_bytes_total(&self) -> u64 {
        self.admission_bytes_total.load(Ordering::Relaxed)
    }

    /// Live bytes held by outstanding guards. Near-zero in
    /// observation-only mode (guards drop immediately); becomes the
    /// live "tracked pinned bytes" gauge once Phase 4 (#551) holds
    /// guards across the pin lifetime.
    #[must_use]
    pub fn inflight_admission_bytes(&self) -> u64 {
        self.inflight_admission_bytes.load(Ordering::Relaxed)
    }
}

impl Default for WorkerPinBudget {
    fn default() -> Self {
        Self::new(DEFAULT_WORKER_PIN_BUDGET_BYTES)
    }
}

/// Process-wide singleton holder. `OnceLock` chosen for the same
/// reasons as `pin_budget_singleton`: the budget is a process-global
/// resource (128 GiB cap is per-worker-process, not per-store), and
/// the wire-up code lives at process start in `src/bin/nativelink.rs`.
///
/// Storage is `Arc<WorkerPinBudget>` (not bare `WorkerPinBudget`) so
/// the same instance can be returned BOTH as a `&'static
/// WorkerPinBudget` (for the admission hot path) AND as an
/// `Arc<WorkerPinBudget>` (for `MetricsRegistry::register` at process
/// start). Without this dual accessor the registry would either need
/// a manual wrapper or end up tracking a *different* `WorkerPinBudget`
/// instance than the one admissions consult — a silent measurement
/// bug.
static WORKER_PIN_BUDGET_SINGLETON: OnceLock<Arc<WorkerPinBudget>> = OnceLock::new();

fn worker_pin_budget_arc_inner() -> &'static Arc<WorkerPinBudget> {
    WORKER_PIN_BUDGET_SINGLETON.get_or_init(|| Arc::new(WorkerPinBudget::default()))
}

/// Returns the process-wide `WorkerPinBudget` singleton, initializing
/// it on first call with `DEFAULT_WORKER_PIN_BUDGET_BYTES`. The cost
/// is one `OnceLock::get_or_init` (atomic load + branch in the hot
/// path after first call).
pub fn worker_pin_budget_singleton() -> &'static WorkerPinBudget {
    worker_pin_budget_arc_inner().as_ref()
}

/// Returns a clonable `Arc` to the same process-wide
/// `WorkerPinBudget` singleton returned by
/// `worker_pin_budget_singleton()`. The clone is one atomic
/// increment; use at process start to hand a clone to
/// `MetricsRegistry::register` so the `worker_pinned_bytes_used`,
/// `worker_pinned_bytes_capacity`, etc. gauges are scraped by every
/// `/metrics` listener.
#[must_use]
pub fn worker_pin_budget_arc() -> Arc<WorkerPinBudget> {
    Arc::clone(worker_pin_budget_arc_inner())
}

/// Manual `MetricsComponent` impl: the metrics published are NOT
/// stored fields (one is derived from the live Semaphore), so the
/// derive macro cannot generate them.
///
/// **Observation-only mode lit/dark map** (per red-team P-1
/// 2026-05-22 fix-up): of the five metrics below, only
/// `worker_pin_admission_bytes_total` carries real signal today —
/// the others are pre-wired for Phase 4 (#551) when guards will be
/// held across the pin lifetime and the cap will bind. No metric is
/// dropped; the lit/dark distinction is documented inline so the
/// operator dashboard can render the dark ones as "Phase 4 awaiting"
/// instead of "broken probe".
///
/// - `worker_pinned_bytes_used` — DARK in observation-only mode.
///   Gauge derived from `used_bytes()`. Structurally near-zero today
///   because guards drop immediately at the call sites; the live
///   Semaphore consumption never grows. Becomes meaningful once
///   Phase 4 (#551) holds guards across the BIS-ack window.
/// - `worker_pinned_bytes_capacity` — LIT (gauge, constant).
///   Configured cap published every scrape so the operator dashboard
///   has a denominator for `used` and `inflight` gauges.
/// - `worker_pin_budget_rejections_total` — DARK in observation-only
///   mode. Monotone counter from the atomic. Structurally zero today
///   because the 128 GiB cap is far above the 64 GiB worst-case
///   observed pin set; becomes meaningful once Phase 4 (#551) lowers
///   the cap toward the steady-state pin set size.
/// - `worker_pin_admission_bytes_total` — **LIT / PRIMARY**
///   observation-only signal. Monotone counter of total bytes
///   admitted through `try_acquire`. Useful TODAY for measuring
///   per-process admission throughput (bytes/sec rate) without
///   depending on guard lifetime; this is the metric to chart for
///   worker-pin pressure under the current observation-only wiring.
/// - `worker_pin_inflight_admission_bytes` — DARK in observation-only
///   mode. Live gauge of bytes held by outstanding guards.
///   Structurally near-zero today because guards drop immediately;
///   becomes the "tracked pinned bytes" gauge once Phase 4 (#551)
///   holds guards across the pin lifetime.
impl MetricsComponent for WorkerPinBudget {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let used = self.used_bytes() as u64;
        let cap = self.capacity_bytes as u64;
        let rejections = self.rejections_total();
        let admission_total = self.admission_bytes_total();
        let inflight = self.inflight_admission_bytes();
        nativelink_metric::publish!(
            "worker_pinned_bytes_used",
            &used,
            nativelink_metric::MetricKind::Default,
            "DARK in #549 observation-only mode. Bytes currently held by worker pin-budget Semaphore (gauge derived from cap - available_permits; cap = DEFAULT_WORKER_PIN_BUDGET_BYTES, default 128 GiB). Structurally near-zero today because guards drop immediately at call sites; meaningful post Phase 4 (#551)."
        );
        nativelink_metric::publish!(
            "worker_pinned_bytes_capacity",
            &cap,
            nativelink_metric::MetricKind::Default,
            "LIT (gauge, constant). Total worker pinned-bytes cap (constant after process start; default DEFAULT_WORKER_PIN_BUDGET_BYTES = 128 GiB). Operator dashboard denominator for `used` and `inflight` gauges."
        );
        nativelink_metric::publish!(
            "worker_pin_budget_rejections_total",
            &rejections,
            nativelink_metric::MetricKind::Counter,
            "DARK in #549 observation-only mode. Cumulative count of worker pin admissions rejected because the per-process pinned-bytes budget was exhausted (#549 defense-in-depth). Structurally zero today (128 GiB cap >> 64 GiB worst-case observed); meaningful once Phase 4 (#551) lowers the cap toward the steady-state pin set."
        );
        nativelink_metric::publish!(
            "worker_pin_admission_bytes_total",
            &admission_total,
            nativelink_metric::MetricKind::Counter,
            "LIT / PRIMARY observation-only signal. Cumulative bytes successfully admitted via WorkerPinBudget::try_acquire. Useful today for per-process pin-admission throughput (bytes/sec rate) — does NOT depend on guard lifetime."
        );
        nativelink_metric::publish!(
            "worker_pin_inflight_admission_bytes",
            &inflight,
            nativelink_metric::MetricKind::Default,
            "DARK in #549 observation-only mode. Live bytes held by outstanding WorkerPinBudgetGuard instances. Structurally near-zero today because guards drop immediately at call sites; becomes 'tracked pinned bytes' gauge once Phase 4 (#551) holds guards across the pin lifetime."
        );
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_WORKER_PIN_BUDGET_BYTES, WorkerPinBudget, worker_pin_budget_arc,
        worker_pin_budget_singleton,
    };

    /// Sanity: a fresh budget has the requested capacity and zero
    /// rejections.
    #[test]
    fn fresh_budget_has_full_capacity() {
        let b = WorkerPinBudget::new(1024);
        assert_eq!(b.available_bytes(), 1024);
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(b.capacity_bytes(), 1024);
        assert_eq!(b.rejections_total(), 0);
        assert_eq!(b.admission_bytes_total(), 0);
        assert_eq!(b.inflight_admission_bytes(), 0);
    }

    /// `try_acquire(n)` succeeds when `n <= available`, fails when
    /// `n > available`, and bumps the rejection counter on failure.
    /// Also covers the new `admission_bytes_total` +
    /// `inflight_admission_bytes` accounting.
    #[test]
    fn try_acquire_succeeds_until_exhaustion() {
        let b = WorkerPinBudget::new(100);
        let g1 = b.try_acquire(60).expect("first 60 bytes");
        assert_eq!(b.used_bytes(), 60);
        assert_eq!(b.admission_bytes_total(), 60);
        assert_eq!(b.inflight_admission_bytes(), 60);
        let g2 = b.try_acquire(40).expect("remaining 40 bytes");
        assert_eq!(b.used_bytes(), 100);
        assert_eq!(b.admission_bytes_total(), 100);
        assert_eq!(b.inflight_admission_bytes(), 100);
        // Cap exhausted; next request fails.
        assert!(b.try_acquire(1).is_none(), "1 byte beyond cap must reject");
        assert_eq!(b.rejections_total(), 1);
        // Drop one guard; capacity returns AND inflight drops.
        drop(g1);
        assert_eq!(b.used_bytes(), 40);
        assert_eq!(b.inflight_admission_bytes(), 40);
        // admission_bytes_total is monotone — it does NOT decrement.
        assert_eq!(b.admission_bytes_total(), 100);
        // Re-acquire succeeds without bumping the rejection counter.
        let _re = b.try_acquire(50).expect("50 bytes back available");
        assert_eq!(b.rejections_total(), 1);
        assert_eq!(b.admission_bytes_total(), 150);
        drop(g2);
        drop(_re);
        assert_eq!(b.inflight_admission_bytes(), 0);
    }

    /// Even a request that exceeds the entire capacity rejects cleanly
    /// (instead of e.g. clamping or wrapping) — the caller is expected
    /// to back off and retry once existing pins drain.
    #[test]
    fn try_acquire_above_capacity_rejects_cleanly() {
        let b = WorkerPinBudget::new(100);
        assert!(b.try_acquire(101).is_none(), "above-cap request rejects");
        assert_eq!(b.rejections_total(), 1);
        // Subsequent within-cap request still works.
        let _g = b.try_acquire(50).expect("within-cap still works");
        assert_eq!(b.admission_bytes_total(), 50);
    }

    /// Singleton accessor returns the same `&'static WorkerPinBudget`
    /// across calls — the load-bearing property the metric wiring
    /// depends on.
    #[test]
    fn singleton_returns_same_reference_across_calls() {
        let a = worker_pin_budget_singleton();
        let b = worker_pin_budget_singleton();
        assert!(
            core::ptr::eq(a, b),
            "OnceLock singleton must return the same reference",
        );
        assert_eq!(a.capacity_bytes(), DEFAULT_WORKER_PIN_BUDGET_BYTES);
    }

    /// `worker_pin_budget_arc()` returns an `Arc` pointing at the SAME
    /// `WorkerPinBudget` instance the `&'static` accessor returns.
    /// Without this property, `MetricsRegistry::register` would
    /// publish gauges from a different `Semaphore` than the one
    /// admissions consume from — a silent measurement bug.
    #[test]
    fn singleton_arc_and_ref_point_to_same_instance() {
        let ref_ptr: *const WorkerPinBudget = worker_pin_budget_singleton();
        let arc = worker_pin_budget_arc();
        let arc_ptr: *const WorkerPinBudget = &*arc;
        assert!(
            core::ptr::eq(ref_ptr, arc_ptr),
            "worker_pin_budget_arc() and worker_pin_budget_singleton() must alias the same WorkerPinBudget instance",
        );
    }

    /// Zero-byte acquire is a no-op guard (zero bytes consumed).
    /// Defensive — production callers pass `digest.size_bytes() >= 1`.
    #[test]
    fn zero_byte_acquire_is_noop() {
        let b = WorkerPinBudget::new(100);
        let g = b.try_acquire(0).expect("zero-byte guard");
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(b.admission_bytes_total(), 0);
        assert_eq!(b.inflight_admission_bytes(), 0);
        drop(g);
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(b.rejections_total(), 0);
    }

    /// The default-derived budget matches `DEFAULT_WORKER_PIN_BUDGET_BYTES`.
    #[test]
    fn default_matches_constant() {
        let b = WorkerPinBudget::default();
        assert_eq!(b.capacity_bytes(), DEFAULT_WORKER_PIN_BUDGET_BYTES);
        assert_eq!(b.available_bytes(), DEFAULT_WORKER_PIN_BUDGET_BYTES);
    }

    /// Regression on the literal expression at the declaration site —
    /// per CLAUDE.md: doc-comments, metric help text, and renamed test
    /// functions can all drift while the actual constant stays old.
    /// Only the `assert_eq!` on the literal at the declaration line is
    /// authoritative.
    #[test]
    fn default_cap_is_128_gib() {
        assert_eq!(
            DEFAULT_WORKER_PIN_BUDGET_BYTES,
            128 * 1024 * 1024 * 1024,
            "DEFAULT_WORKER_PIN_BUDGET_BYTES regression: must remain 128 GiB per #549 fix-up A-1 (2× the 32-64 GiB worst-case bucket observed in the 2026-05-21 10-worker live scrape; 64 GiB cap would have fired right at the observed edge)"
        );
    }

    /// Guard drop releases BOTH the Semaphore permit AND decrements
    /// `inflight_admission_bytes`. Mutation: remove the
    /// `fetch_sub` in `WorkerPinBudgetGuard::Drop`; this test must
    /// red-fail with the bespoke "guard drop must decrement inflight"
    /// message.
    #[test]
    fn guard_drop_releases_inflight_bytes() {
        let b = WorkerPinBudget::new(1024);
        {
            let _g = b.try_acquire(256).expect("256-byte guard");
            assert_eq!(b.inflight_admission_bytes(), 256);
            assert_eq!(b.used_bytes(), 256);
        }
        assert_eq!(
            b.inflight_admission_bytes(),
            0,
            "guard drop must decrement inflight: live tracking is the post-Phase-4 (#551) gauge semantic; without it operators cannot tell whether the worker pin set is growing"
        );
        assert_eq!(b.used_bytes(), 0);
        assert_eq!(
            b.admission_bytes_total(),
            256,
            "admission_bytes_total is monotone; guard drop does NOT undo throughput accounting"
        );
    }

    /// #549 fix-up A-2 (2026-05-22): a digest larger than `u32::MAX`
    /// bytes must split into multiple `OwnedSemaphorePermit`s
    /// transparently instead of silently rejecting via the prior
    /// `u32::try_from(...).ok()?` early-return. Today this is a
    /// correctness fix (observation-only mode never depends on the
    /// admission outcome); Phase 4 (#551) makes it load-bearing.
    ///
    /// Mutation: change the `while remaining > 0` loop to a single
    /// `try_acquire_many_owned(u32::try_from(n_bytes).unwrap_or(u32::MAX))`
    /// call. This test must red-fail with the bespoke "multi-permit
    /// splitting required" message (the larger of the two acquisitions
    /// would not happen, so `used_bytes` would not match `n_bytes`).
    #[test]
    fn acquire_above_u32_max_bytes_splits_correctly() {
        // 5 GiB request = u32::MAX (~4 GiB) + ~1 GiB; needs 2 permits.
        let n_bytes: usize = 5 * 1024 * 1024 * 1024;
        // 16 GiB budget — large enough for 4 such requests.
        let b = WorkerPinBudget::new(16 * 1024 * 1024 * 1024);
        let g = b
            .try_acquire(n_bytes)
            .expect("multi-permit splitting required — single try_acquire_many_owned(u32) would silently reject a >4 GiB request before #549 fix-up A-2");
        assert_eq!(
            b.used_bytes(),
            n_bytes,
            "multi-permit splitting required: used_bytes must reflect the full multi-GB acquisition, not just the first u32-chunk"
        );
        assert_eq!(b.admission_bytes_total(), n_bytes as u64);
        assert_eq!(b.inflight_admission_bytes(), n_bytes as u64);
        assert_eq!(b.rejections_total(), 0);
        drop(g);
        // Single drop releases ALL permits in the Vec, returning the
        // full multi-GB budget to the pool.
        assert_eq!(
            b.used_bytes(),
            0,
            "single guard drop must release ALL split permits — the multi-permit Vec is the storage that makes this work"
        );
        assert_eq!(b.inflight_admission_bytes(), 0);
        // Subsequent acquisition can re-use the full budget.
        let _g2 = b
            .try_acquire(n_bytes)
            .expect("post-drop, all 5 GiB must be re-acquirable — the prior drop released both split permits");
    }

    /// #549 fix-up testing-czar MAJOR 2 (over-action coverage,
    /// 2026-05-22): when `try_acquire` is invoked with a request larger
    /// than the cap, the rejection bumps `rejections_total` BUT the
    /// caller (in observation-only mode) STILL invokes the underlying
    /// pin call. This test asserts the gate behaves as a strict
    /// "rejection signal only" contract — it does NOT panic, abort, or
    /// otherwise produce side effects that would prevent the caller
    /// from proceeding with the pin.
    ///
    /// In observation-only mode the gate is decoupled from the pin
    /// call (see wired callers in nativelink-worker/src/{local_worker,
    /// directory_cache,running_actions_manager}.rs — each treats a
    /// `None` as best-effort and proceeds with `pin_digests`). This is
    /// the "over-action contract": callers can rely on the gate NOT
    /// firing any side effect that prevents the pin.
    ///
    /// Mutation: add `panic!("rejection")` or `std::process::abort()`
    /// in the `try_acquire` rejection arm. This test must red-fail (or
    /// the harness will surface the panic).
    #[test]
    fn rejection_only_bumps_counter_no_other_side_effects() {
        let b = WorkerPinBudget::new(100);
        // Request well above the cap.
        let result = b.try_acquire(1000);
        assert!(
            result.is_none(),
            "above-cap request must return None so the caller can decide whether to proceed (observation-only mode: proceed regardless)"
        );
        assert_eq!(
            b.rejections_total(),
            1,
            "rejection-only contract: the only side effect of a rejection is bumping rejections_total"
        );
        // Capacity is fully restored (no leaked permits).
        assert_eq!(
            b.used_bytes(),
            0,
            "rejection-only contract: a rejected acquisition leaks zero permits"
        );
        assert_eq!(b.admission_bytes_total(), 0);
        assert_eq!(b.inflight_admission_bytes(), 0);
        // Subsequent within-cap acquisition still works (no poisoning).
        let _g = b
            .try_acquire(50)
            .expect("rejection-only contract: a prior rejection does NOT poison subsequent acquisitions");
    }

    /// #549 end-to-end seam: `WorkerPinBudget::publish` must emit
    /// `worker_pinned_bytes_used`, `worker_pinned_bytes_capacity`,
    /// `worker_pin_budget_rejections_total`,
    /// `worker_pin_admission_bytes_total`, and
    /// `worker_pin_inflight_admission_bytes` END-TO-END through the
    /// same `MetricsRegistry` + `render_prometheus` path the
    /// production `/metrics` listener uses.
    ///
    /// Per CLAUDE.md `feedback_publish_body_not_field_existence`:
    /// verify metric exposure via end-to-end scrape, not field-existence
    /// in the source struct. This test crosses the same seam an
    /// operator scrapes.
    ///
    /// Mutation step: comment out the `nativelink_metric::publish!`
    /// for `worker_pinned_bytes_used` in `MetricsComponent for
    /// WorkerPinBudget`. This test must red-fail with the bespoke
    /// "#549 seam" message so a future regression triage points
    /// straight at the contract.
    #[test]
    fn publish_emits_gauges_via_render_prometheus() {
        use std::sync::Arc;

        use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};

        let budget = Arc::new(WorkerPinBudget::new(1024));
        // Consume some bytes so the live gauges have a non-zero value
        // we can assert on (separates "publish emitted nothing" from
        // "publish emitted zero").
        let _hold = budget.try_acquire(256).expect("256-byte guard");

        let registry = MetricsRegistry::new();
        registry.register("worker_pin_budget", budget.clone());
        let body = render_prometheus(&registry);

        assert!(
            body.contains("worker_pin_budget_worker_pinned_bytes_used"),
            "#549 seam: WorkerPinBudget::publish must emit worker_pinned_bytes_used \
             so the operator can verify the live cap-derived gauge end-to-end via \
             /metrics. body=\n{body}"
        );
        assert!(
            body.contains("worker_pin_budget_worker_pinned_bytes_capacity"),
            "#549 seam: WorkerPinBudget::publish must emit worker_pinned_bytes_capacity \
             so the operator can verify the configured cap end-to-end via /metrics. \
             body=\n{body}"
        );
        assert!(
            body.contains("worker_pin_budget_worker_pin_budget_rejections_total"),
            "#549 seam: WorkerPinBudget::publish must emit \
             worker_pin_budget_rejections_total so the operator can verify \
             defense-in-depth rejection pressure end-to-end via /metrics. body=\n{body}"
        );
        assert!(
            body.contains("worker_pin_budget_worker_pin_admission_bytes_total"),
            "#549 seam: WorkerPinBudget::publish must emit \
             worker_pin_admission_bytes_total so admission throughput is observable \
             end-to-end via /metrics. body=\n{body}"
        );
        assert!(
            body.contains("worker_pin_budget_worker_pin_inflight_admission_bytes"),
            "#549 seam: WorkerPinBudget::publish must emit \
             worker_pin_inflight_admission_bytes so live guard-held bytes are \
             observable end-to-end via /metrics. body=\n{body}"
        );

        // Belt-and-braces: gauge values reflect live state, not a
        // snapshot taken at registration. 256 was acquired above; the
        // capacity is 1024.
        assert!(
            body.contains("\nworker_pin_budget_worker_pinned_bytes_used 256\n"),
            "#549 seam: worker_pinned_bytes_used gauge must reflect live used state \
             (expected 256 of 1024). body=\n{body}"
        );
        assert!(
            body.contains("\nworker_pin_budget_worker_pinned_bytes_capacity 1024\n"),
            "#549 seam: worker_pinned_bytes_capacity gauge must reflect configured \
             cap (expected 1024). body=\n{body}"
        );
        assert!(
            body.contains("\nworker_pin_budget_worker_pin_inflight_admission_bytes 256\n"),
            "#549 seam: worker_pin_inflight_admission_bytes gauge must reflect live \
             guard-held bytes (expected 256 with one outstanding guard). body=\n{body}"
        );
        assert!(
            body.contains("\nworker_pin_budget_worker_pin_admission_bytes_total 256\n"),
            "#549 seam: worker_pin_admission_bytes_total counter must reflect total \
             bytes admitted (expected 256 after one successful acquire). body=\n{body}"
        );
    }
}
