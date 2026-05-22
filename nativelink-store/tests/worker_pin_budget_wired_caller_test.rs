// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
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

//! #549 fix-up testing-czar MAJOR 1 (2026-05-22): integration test
//! that crosses the production seam from a wired-caller invocation
//! through the `worker_pin_budget_singleton()` accessor to the
//! `MetricsRegistry::render_prometheus` body the operator scrapes.
//!
//! ## Seams crossed
//!
//! 1. **Wired caller pattern** — the exact `worker_pin_budget_singleton()
//!    .try_acquire(n_bytes)` call shape used at:
//!    - `nativelink-worker/src/directory_cache.rs:2562` (DirectoryCache
//!      populate-tail re-pin path)
//!    - `nativelink-worker/src/local_worker.rs:2110` (reconnect-retry
//!      `failed` set re-pin path)
//!    - `nativelink-worker/src/running_actions_manager.rs:3785-3812`
//!      (post-upload output digest pin path — 4 call sites: output_files,
//!      output_folders, stdout, stderr)
//!    - `nativelink-worker/src/running_actions_manager.rs:4758`
//!      (spawn_upload_to_remote pre-upload pin path)
//!    - `nativelink-worker/src/running_actions_manager.rs:4947`
//!      (tree-extracted file_digests pin path)
//! 2. **Singleton accessor** — `worker_pin_budget_singleton()` returns
//!    the same `&'static WorkerPinBudget` the `worker_pin_budget_arc()`
//!    accessor wraps in `Arc` for `MetricsRegistry::register`.
//! 3. **MetricsComponent::publish** — the `nativelink_metric::publish!`
//!    calls inside `impl MetricsComponent for WorkerPinBudget`.
//! 4. **`render_prometheus` Prometheus formatting** — the same
//!    `MetricsRegistry::register` → `render_prometheus` path
//!    `src/bin/nativelink.rs:596-599` wires for `/metrics`.
//!
//! ## Mutation step
//!
//! Comment out the `singleton().try_acquire(n)` line in
//! `nativelink-store/src/worker_pin_budget.rs::try_acquire`
//! (so the function early-returns `None` regardless of capacity), OR
//! comment out the `nativelink_metric::publish!` for
//! `worker_pin_admission_bytes_total`. Either mutation must red-fail
//! this test with the bespoke "#549 wired-caller seam: …" message.
//!
//! ## Why this is an integration test, not a unit test
//!
//! The existing `publish_emits_gauges_via_render_prometheus` unit test
//! in `worker_pin_budget.rs` exercises the publish path against a
//! locally-constructed `WorkerPinBudget`. This test exercises the
//! singleton + render path that the WIRED CALLERS use — a different
//! seam. Any future refactor that decouples the singleton from the
//! per-instance `WorkerPinBudget` (e.g. process-pool reset, test-only
//! override) will fail this test even if the unit test passes.

use core::time::Duration;
use std::sync::Arc;

use nativelink_macro::nativelink_test;
use nativelink_store::worker_pin_budget::{worker_pin_budget_arc, worker_pin_budget_singleton};
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use serial_test::serial;

/// Verify the wired-caller pattern: `worker_pin_budget_singleton()
/// .try_acquire(n)` bumps `worker_pin_admission_bytes_total` and the
/// counter is observable end-to-end via `render_prometheus`.
///
/// `#[serial]` because every test in this file mutates the process-
/// global singleton — running in parallel would race the counter
/// assertions.
#[nativelink_test]
#[serial]
async fn wired_caller_admission_bumps_metric_endtoend() {
    // The `tokio::time::timeout` is the deadlock detector — without it,
    // a future regression that wedges the singleton accessor (e.g. an
    // accidental `Mutex` introduced in front of the OnceLock) would
    // hang CI instead of producing a meaningful failure.
    tokio::time::timeout(Duration::from_secs(5), async {
        // Snapshot the singleton's counter BEFORE the wired-caller
        // pattern. Other tests may run sequentially against the same
        // singleton; assert via delta, not absolute.
        let before = worker_pin_budget_singleton().admission_bytes_total();

        // ===== Wired-caller seam =====
        // This is the exact shape used at every wired call site
        // (directory_cache.rs:2562, local_worker.rs:2110,
        // running_actions_manager.rs:3785+/4758/4947). The guard is
        // dropped immediately at end of scope (observation-only mode).
        const N_BYTES: usize = 4 * 1024 * 1024; // 4 MiB — representative output blob
        {
            let _guard = worker_pin_budget_singleton()
                .try_acquire(N_BYTES)
                .expect(
                    "#549 wired-caller seam: under the 128 GiB default cap, a 4 MiB \
                     acquisition MUST succeed. None return implies the singleton \
                     try_acquire path is broken (the wired callers' guards would \
                     also be None — observation-only mode wastes the metric)",
                );
            // Guard drops at end of scope — mirrors the
            // observation-only-mode pattern at every wired site.
        }

        // ===== render_prometheus seam =====
        // Wire the SAME Arc the production registry uses. The
        // singleton must report the new counter value via the
        // `MetricsComponent` impl rendered into Prometheus.
        let registry = MetricsRegistry::new();
        registry.register("worker_pin_budget", worker_pin_budget_arc());
        let body = render_prometheus(&registry);

        let after = worker_pin_budget_singleton().admission_bytes_total();
        let delta = after - before;
        assert_eq!(
            delta, N_BYTES as u64,
            "#549 wired-caller seam: admission_bytes_total delta must equal the \
             try_acquire size (expected {N_BYTES} bytes admitted, got delta {delta}). \
             A non-matching delta means the wired-caller pattern is double-counting \
             or under-counting at the singleton accessor — production observability \
             would be a lie.",
        );

        assert!(
            body.contains("worker_pin_budget_worker_pin_admission_bytes_total"),
            "#549 wired-caller seam: render_prometheus body must contain \
             `worker_pin_admission_bytes_total` (the LIT primary observation-only \
             signal). Absence means the MetricsComponent::publish call for this \
             metric is broken — the wired callers' admissions would be invisible \
             to /metrics scrapes. body=\n{body}",
        );

        // Belt-and-braces: the counter VALUE published equals `after`.
        // This catches a regression where `publish!` emits the metric
        // name but a stale snapshot of the counter (e.g. lazily-bound
        // closure capturing the value at registration instead of
        // scrape time).
        let expected_line = format!(
            "\nworker_pin_budget_worker_pin_admission_bytes_total {after}\n"
        );
        assert!(
            body.contains(&expected_line),
            "#549 wired-caller seam: rendered counter must reflect LIVE \
             admission_bytes_total ({after}), not a snapshot. Missing line: \
             {expected_line:?}. body=\n{body}",
        );
    })
    .await
    .expect(
        "#549 wired-caller seam: deadlock detector tripped — \
         worker_pin_budget_singleton() must NEVER block (the OnceLock + Semaphore \
         path is non-blocking by construction; a 5s timeout indicates a regression \
         that introduced a Mutex or async dependency in the accessor)",
    );
}

/// Rejection-side wired-caller seam: when the singleton is at
/// capacity (forced via a separately-constructed local `WorkerPinBudget`
/// matching the singleton's pattern), the rendered Prometheus body
/// exposes `worker_pin_budget_rejections_total` so an operator can
/// detect over-cap pressure end-to-end.
///
/// This complements `wired_caller_admission_bumps_metric_endtoend` by
/// exercising the over-action path: the gate must report rejection
/// via the metric without preventing the caller from proceeding with
/// the pin (the caller, in observation-only mode, ignores `None` and
/// proceeds anyway — see the wired callsites).
///
/// Note: we cannot exhaust the process-global singleton from inside a
/// test (it has a 128 GiB cap — 137+ billion permits — and another
/// test running in parallel would observe the leaked acquisition).
/// Instead we construct a local `WorkerPinBudget::new(small_cap)` and
/// assert the same MetricsComponent path renders rejection counts
/// correctly. This mirrors the wired-caller pattern's expectation:
/// rejection signal is operator-observable via /metrics.
#[nativelink_test]
#[serial]
async fn rejection_path_renders_via_prometheus() {
    use nativelink_store::worker_pin_budget::WorkerPinBudget;

    tokio::time::timeout(Duration::from_secs(5), async {
        let budget = Arc::new(WorkerPinBudget::new(100));
        // Over-cap acquire → None + bump rejections_total.
        let result = budget.try_acquire(1000);
        assert!(
            result.is_none(),
            "#549 wired-caller seam (rejection): try_acquire over cap must return \
             None so the caller can decide to proceed (observation-only mode: \
             proceed anyway). Some implies the gate is broken.",
        );
        assert_eq!(
            budget.rejections_total(),
            1,
            "#549 wired-caller seam (rejection): rejection MUST bump \
             rejections_total — the only operator-observable signal of over-cap \
             pressure before Phase 4 (#551) flips the gate to load-bearing",
        );

        let registry = MetricsRegistry::new();
        registry.register("rejection_test", budget.clone());
        let body = render_prometheus(&registry);

        assert!(
            body.contains(
                "\nrejection_test_worker_pin_budget_rejections_total 1\n"
            ),
            "#549 wired-caller seam (rejection): rendered Prometheus body must \
             expose worker_pin_budget_rejections_total = 1 after one over-cap \
             try_acquire. Absence means the rejection counter is invisible to \
             operator scrapes — over-cap pressure would go undetected until Phase 4. \
             body=\n{body}",
        );
    })
    .await
    .expect(
        "#549 wired-caller seam (rejection): deadlock detector tripped — \
         try_acquire + publish path must be non-blocking",
    );
}
