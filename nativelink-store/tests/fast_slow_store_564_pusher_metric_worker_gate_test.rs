// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! #564 — over-action regression for `record_pusher_invoke` on
//! worker-process `FastSlowStore` instances.
//!
//! Workers also reach `push_stable_digests_via_arcs` (via
//! `nativelink-worker/src/local_worker.rs:2986,:3094` +
//! `directory_cache.rs:5373` constructing FSS instances), but the
//! metric is named `server_stable_digests_pusher_invoke_count` and is
//! consumed by server-side dashboards only. Workers paying ~hundreds
//! of ns per digest (via the moka `pusher_timestamps.insert` call
//! inside `record_pusher_invoke`) produces an unobservable signal at
//! non-trivial steady-state cost.
//!
//! Pre-#554 the closure-only path had this same asymmetry; #554
//! widened it to the four direct-push arms. #564 gates the per-digest
//! bump inside the shared helper so the cost is paid ONLY on the
//! server process (set by `set_is_server_process(true)` from
//! `src/bin/nativelink.rs` early in `inner_main`).
//!
//! ## Invariant under test (asymmetric contract coverage)
//!
//! Per CLAUDE.md "Asymmetric contract coverage":
//!   - Under-action (gate-active, server context): existing #554
//!     tests in `fast_slow_store_554_*.rs` cover this; the gate
//!     defaults to true and the existing tests still pass.
//!   - **Over-action (this file): gate-active in worker context.**
//!     A worker-context FSS push MUST NOT bump
//!     `pusher_invoke_count`. Pre-#564 it did, paying unobservable
//!     cost. Post-#564 it does not.
//!
//! ## Seams crossed
//!
//! `FastSlowStore::mark_stable` → `push_stable_digests` →
//! `push_stable_digests_via_arcs` → (gate: `is_server_process()`
//! false) → no `record_pusher_invoke` call.
//!
//! We use `mark_stable` (not `update_oneshot`) because:
//!   1. `mark_stable` is synchronous — no spawn-drain ceremony, the
//!      counter assertion is immediate.
//!   2. `mark_stable` is one of the arms #554 added; if any future
//!      refactor strips the `is_server_process()` gate from this
//!      path, this test catches it.
//!   3. The streaming/oneshot/V3 arms are exercised in
//!      `fast_slow_store_554_*.rs` under the default-server context;
//!      duplicating the fixture here would not add coverage.
//!
//! ## Process isolation
//!
//! Each `tests/*.rs` integration test file is a SEPARATE cargo test
//! binary, so this file's `set_is_server_process_for_test(false)`
//! call is isolated from the `fast_slow_store_554_*.rs` binary's
//! state (which relies on the default `true`). Within THIS binary
//! every test must set the flag explicitly at entry.
//!
//! ## Intra-binary test isolation (`#[serial]`)
//!
//! Tests in this file mutate the process-global
//! `IS_SERVER_PROCESS_RUNTIME` `AtomicBool`. Cargo's default parallel
//! test scheduler would let one test flip the flag false while
//! another reads it true (or vice versa), producing flake. All tests
//! are tagged `#[serial(is_server_process_gate)]` so `serial_test`
//! serializes them — only one at a time runs, so the flag's state at
//! test entry is exactly what the test set it to.
//!
//! ## Mutation step (CLAUDE.md mandatory)
//!
//! 1. Open `nativelink-store/src/fast_slow_store.rs`.
//! 2. Remove the `if is_server_process() {` guard around the
//!    `for digest in digests { metrics.record_pusher_invoke(*digest); }`
//!    loop inside `push_stable_digests_via_arcs` (un-gate so worker
//!    context also bumps).
//! 3. Re-run this test. It MUST red-fail with the bespoke
//!    "#564 worker bumped server-only metric" message.
//!
//! Without the mutation step the test proves only "counter equals
//! before-snapshot in worker context", which is also true if the
//! whole metric system is broken. The mutation step proves the gate
//! itself is what suppresses the bump.

use core::time::Duration;
use std::sync::Arc;

use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::phase0_metrics::{
    is_server_process, server_phase0_metrics, set_is_server_process_for_test,
};
use nativelink_util::store_trait::Store;
use serial_test::serial;

/// Distinct 64-hex hashes per test so concurrent test runs in this
/// binary don't collide on FSS-instance state. Hashes use a `564`
/// prefix matching the tracker.
const HASH_MARK_STABLE_WORKER_1: &str =
    "5640000000000000000000000000000000000000000000000000000000000001";
const HASH_MARK_STABLE_WORKER_2: &str =
    "5640000000000000000000000000000000000000000000000000000000000002";
const HASH_MARK_STABLE_SERVER_1: &str =
    "5640000000000000000000000000000000000000000000000000000000000003";

/// Build a real `FastSlowStore` (mirrors `fast_slow_store_554_*.rs`'s
/// `build_fast_slow_with_memory_slow`).
fn build_fast_slow_with_memory_slow() -> (Store, Arc<FastSlowStore>) {
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let slow_inner = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(fast_inner.clone());
    let slow_store = Store::new(slow_inner);
    let fss: Arc<FastSlowStore> = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow_store,
    );
    let wrapped = Store::new(fss.clone());
    (wrapped, fss)
}

/// #564 over-action regression: worker-process `mark_stable` MUST NOT
/// bump `server_stable_digests_pusher_invoke_count`.
///
/// Mutation step (CLAUDE.md mandatory): remove the
/// `if is_server_process() {` guard in
/// `nativelink-store/src/fast_slow_store.rs::push_stable_digests_via_arcs`.
/// This test must red-fail with the bespoke
/// "#564 worker bumped server-only metric" message.
#[serial(is_server_process_gate)]
#[nativelink_test]
async fn worker_context_mark_stable_does_not_bump_pusher_invoke_count() -> Result<(), Error>
{
    // Flip the gate off — simulate worker-process FSS context.
    set_is_server_process_for_test(false);
    assert!(
        !is_server_process(),
        "test precondition: set_is_server_process_for_test(false) must \
         flip the runtime flag to false. If this fails, the test \
         helper is broken; investigate phase0_metrics.rs."
    );

    let (wrapped, fss) = build_fast_slow_with_memory_slow();
    let d1 = DigestInfo::try_new(HASH_MARK_STABLE_WORKER_1, 11).unwrap();
    let d2 = DigestInfo::try_new(HASH_MARK_STABLE_WORKER_2, 13).unwrap();

    let before = server_phase0_metrics().pusher_invoke_count_for_test();

    wrapped.mark_stable(&[d1, d2]);

    let after = server_phase0_metrics().pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    // SPECIFIC message (CLAUDE.md asymmetric contract coverage:
    // "Assertion message must be specific, not generic is_ok()/is_err()").
    assert_eq!(
        delta, 0,
        "#564 worker bumped server-only metric: worker-context FSS \
         mark_stable incremented server_stable_digests_pusher_invoke_count \
         by {delta} (before={before}, after={after}). The gate at \
         push_stable_digests_via_arcs MUST skip record_pusher_invoke when \
         is_server_process() is false; workers reach the helper but the \
         metric subtree (phase0_server_*) is consumed only by server-side \
         dashboards. Pre-#564 workers paid ~hundreds of ns per digest \
         (moka pusher_timestamps cache insert) for an unobservable signal."
    );

    // Composite check: the queue push (the BENIGN side of the helper) MUST
    // still fire — we only gated the metric bump, not the queue mechanics.
    // A regression that gated EVERYTHING (e.g. `if is_server_process() {
    // return; }` at the top of the helper) would skip the push too;
    // workers would then never accumulate `stable_digests` on their FSS
    // instance, which would mask the cost of the (worker-side benign)
    // accumulator. We assert the digests landed on the queue regardless
    // of the gate so a "gate-everything" regression is caught.
    let drained = fss.drain_stable_digests();
    assert!(
        drained.contains(&d1) && drained.contains(&d2),
        "#564 gate scope contract: only the record_pusher_invoke bump \
         is gated on is_server_process(); the stable_digests queue \
         push must fire regardless. A regression that early-returned \
         the entire helper would skip the queue push too. drained={drained:?}, \
         expected to contain {d1:?} and {d2:?}."
    );

    // Restore default for clean tear-down in case future tests are
    // appended to this binary.
    set_is_server_process_for_test(true);

    Ok(())
}

/// #564 under-action regression: server-process `mark_stable` MUST
/// bump `server_stable_digests_pusher_invoke_count` (mirrors the
/// `mark_stable_bumps_pusher_invoke_count_once_per_digest` test in
/// `fast_slow_store_554_*.rs`, but pinned here so this binary's
/// `set_is_server_process_for_test(true)` path is independently
/// covered AND so the gate's TRUE branch is exercised in the same
/// test binary as the FALSE branch).
///
/// This test is the "two of three corners" composite per CLAUDE.md:
/// the gate's TRUE branch (server) and FALSE branch (worker) must
/// both fire correctly within the same binary. Without this test the
/// worker-context test could pass vacuously if the entire gate
/// short-circuited regardless of `is_server_process()` value.
///
/// Mutation step: invert the gate (`if !is_server_process()`) in
/// `push_stable_digests_via_arcs`. This test must red-fail (counter
/// not bumped in server context) AND the worker-context test above
/// must red-fail (counter bumped in worker context). One mutation,
/// two red-fails — that's the composite-coverage discipline.
#[serial(is_server_process_gate)]
#[nativelink_test]
async fn server_context_mark_stable_does_bump_pusher_invoke_count() -> Result<(), Error> {
    // Flip the gate on — simulate server-process FSS context.
    set_is_server_process_for_test(true);
    assert!(
        is_server_process(),
        "test precondition: set_is_server_process_for_test(true) must \
         flip the runtime flag to true."
    );

    let (wrapped, _fss) = build_fast_slow_with_memory_slow();
    let d = DigestInfo::try_new(HASH_MARK_STABLE_SERVER_1, 17).unwrap();

    let before = server_phase0_metrics().pusher_invoke_count_for_test();

    wrapped.mark_stable(&[d]);

    let after = server_phase0_metrics().pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    assert!(
        delta >= 1,
        "#564 gate TRUE branch missed bump: server-context FSS \
         mark_stable did NOT increment \
         server_stable_digests_pusher_invoke_count (before={before}, \
         after={after}, delta={delta}, expected >= 1). If the gate's \
         TRUE branch is broken, the under-action regression test in \
         fast_slow_store_554_*.rs will also red-fail in the next CI \
         run; the redundancy is intentional (binary isolation)."
    );

    Ok(())
}

/// Sanity test for the worker-context-set state-transition path.
/// Confirms `set_is_server_process_for_test(false)` actually persists
/// across the `await` boundaries inside `#[nativelink_test]` (the
/// `AtomicBool` lives in process-global static storage so it should,
/// but the test makes the assumption explicit).
///
/// Without this, a regression that put `set_is_server_process_for_test`
/// into thread-local storage (or some other state with limited
/// lifetime) would silently make the worker-context test above
/// vacuously pass.
#[serial(is_server_process_gate)]
#[nativelink_test]
async fn worker_context_state_persists_across_await() -> Result<(), Error> {
    set_is_server_process_for_test(false);
    assert!(!is_server_process(), "pre-await read");

    // Yield to the runtime — if the flag were thread-local, this
    // would lose it on a subsequent task hop.
    tokio::time::sleep(Duration::from_millis(1)).await;

    assert!(
        !is_server_process(),
        "#564 state-storage contract violated: \
         set_is_server_process_for_test(false) did not persist across \
         a tokio::time::sleep yield. The runtime flag MUST live in \
         process-global static storage so the gate decision is \
         consistent regardless of which tokio worker thread executes \
         push_stable_digests_via_arcs (this is the production case \
         too — FSS calls span tokio tasks)."
    );

    // Restore default.
    set_is_server_process_for_test(true);

    Ok(())
}
