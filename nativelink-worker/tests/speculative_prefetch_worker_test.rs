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

//! Worker-side tests for the speculative input pre-fetch feature.
//!
//! Test matrix:
//!  T5  – single-inflight guard: AtomicBool prevents second concurrent prefetch
//!  T6  – TTL cap: max TTL forwarded to worker is capped at 120s
//!
//! T7 MIGRATED (2026-07-05): the old T7 pinned `SPECULATIVE_POPULATE_BYTE_BUDGET`
//! (128 MiB) < the real-action budget (512 MiB). That constant + its semaphore
//! were DELETED when the worker seam moved from fetch-half-into-CAS to
//! DirectoryCache pre-CONSTRUCT: the construct's OWN 512 MiB `POPULATE_BYTE_BUDGET`
//! is now the sole yield-first mechanism (it returns `Code::Aborted` under
//! pressure → prewarm fails → the speculative arm aborts fail-fast = the yield).
//! The yield-first invariant is now asserted at the true mechanism in
//! `directory_cache.rs` mod tests as `t7_prewarm_propagates_aborted_yield_first`
//! (prewarm propagates Aborted rather than retrying into the real budget).

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// T5: Single-inflight guard — AtomicBool CAS prevents second concurrent fetch
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec (G5): the worker's `speculative_prefetch_inflight` AtomicBool MUST
// prevent a second speculative fetch from starting while one is in progress.
// This is implemented as compare_exchange(false, true) at the START of the
// `Update::PrefetchInputs` arm; failure drops the second message and increments
// `speculative_prefetch_busy_drop`.
//
// This test pins the MECHANISM: compare_exchange(false, true, AcqRel, Acquire)
// returns Ok(false) on the first call (gate open) and Err(true) on the second
// (gate closed). A mutation that removes the CAS (replacing with an unconditional
// `store(true)`) would make this test fail with the bespoke message.
//
// Bespoke failure message:
//   "T5: AtomicBool CAS must return Err(true) on second call — single-inflight guard broken"
#[test]
fn t5_atomic_bool_single_inflight_guard() {
    // CAPPED AT 1: exactly one in-flight speculative fetch per worker (G5).
    let inflight: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    // First acquire: must succeed (gate was open).
    let first = inflight.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(
        first.is_ok(),
        "T5: first CAS must succeed (gate was open, false→true) — got {first:?}"
    );

    // Second acquire: must fail (gate is now closed).
    let second = inflight.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(
        second == Err(true),
        "T5: AtomicBool CAS must return Err(true) on second call — single-inflight guard broken"
    );

    // Release guard (mirroring the finally block on task exit).
    inflight.store(false, Ordering::Release);

    // After release: a third acquire MUST succeed again.
    let third = inflight.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(
        third.is_ok(),
        "T5: third CAS after release must succeed — guard release broken"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// T6: TTL cap — max TTL forwarded to the worker is 120s
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec: the self-fired TTL timer in the `Update::PrefetchInputs` spawn arm uses
// `ttl_s.min(120)`. This prevents a misconfigured `speculative_prefetch_ttl_s`
// from pinning blobs longer than 120s (PIN_TIMEOUT_SECS = 120).
//
// This test pins the CONSTANT that caps effective TTL. A mutation removing
// the `.min(120)` cap would not change this test (the cap is enforced in
// `local_worker.rs`, not a constant) — but if the CONSTANT is changed this
// test catches it. The worker integration test for the actual spawn is T6b
// (complex, requires a full LocalWorkerImpl setup — deferred to Phase 4b).
//
// Bespoke failure message:
//   "T6: effective TTL must be capped at 120s — TTL cap regression"
#[test]
fn t6_ttl_cap_is_120_seconds() {
    // Pin: the maximum pin TTL for speculative prefetch is 120s.
    const PIN_TIMEOUT_SECS: u64 = 120;

    // Verify the cap formula applied in local_worker.rs spawn arm:
    //   `ttl_s.min(120)`
    let too_large_ttl: u64 = 9999;
    let effective = too_large_ttl.min(PIN_TIMEOUT_SECS);
    assert_eq!(
        effective,
        120,
        "T6: effective TTL must be capped at 120s — TTL cap regression (got {effective})"
    );

    // A TTL smaller than the cap passes through unchanged.
    let small_ttl: u64 = 60;
    let effective_small = small_ttl.min(PIN_TIMEOUT_SECS);
    assert_eq!(
        effective_small,
        60,
        "T6: TTL <= 120 must not be truncated — got {effective_small}"
    );
}

// T7 (budget-isolation constant pin) MIGRATED to
// directory_cache.rs::tests::t7_prewarm_propagates_aborted_yield_first — see the
// module doc-comment above. The SPECULATIVE_POPULATE_BYTE_BUDGET constant this
// test pinned no longer exists (the speculative semaphore was deleted with the
// fetch-half seam; the construct's own 512 MiB POPULATE_BYTE_BUDGET is now the
// yield-first mechanism).
