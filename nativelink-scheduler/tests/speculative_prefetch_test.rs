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

//! Tests for the speculative input pre-fetch feature (tag-15 `PrefetchInputs`).
//!
//! Test matrix (numbers match what each test ACTUALLY asserts, not the spec's
//! original T-matrix — see distsys F5 on decorative numbering):
//!  T1   – feature gate OFF (default): no PrefetchInputs emitted (config + scheduling)
//!  T1b  – feature-OFF inertness: do_try_match leaves the coalesce guard empty
//!  T2   – emission: send_prefetch_inputs DELIVERS a PrefetchInputs to an idle
//!         worker (direct-call; vacuity probe = disable emission → red-fail)
//!  T3   – coalesce guard: a 2nd emit for the same op is SUPPRESSED + counter bumps
//!  T4   – coalesce guard is REAPED on worker evict so a rerouted op re-prefetches
//!         (the distsys/red-team/testing-czar convergent must-fix)
//!  (renamed) send_prefetch_inputs_no_eligible_idle_worker_no_emit – platform
//!         mismatch → no emit (was the mislabeled "t4_platform_mismatch")
//!  T8   – config deserialization: enable_speculative_prefetch defaults false
//!  T9   – proto roundtrip: PrefetchInputs serializes/deserializes correctly
//!
//! Worker-side arm coverage (Update::PrefetchInputs) lives in
//! nativelink-worker/tests/speculative_prefetch_worker_test.rs.

use core::sync::atomic::Ordering;
use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    PrefetchInputs, UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::{ActionInfoWithProps, Worker};
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::platform_properties::PlatformProperties;
use prost::Message;
use tokio::sync::{Notify, mpsc};

mod utils {
    pub(crate) mod scheduler_utils;
}

use utils::scheduler_utils::make_base_action_info;

const NOW_TIME: u64 = 10000;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

/// Add a worker to the scheduler and drain the ConnectionResult message.
async fn add_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    add_worker_with_slots(scheduler, worker_id, props, 0).await
}

/// Add a worker with a specific `max_inflight_tasks` (0 = unlimited).
/// Drains the initial ConnectionResult message from the returned receiver.
async fn add_worker_with_slots(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
    max_inflight_tasks: u64,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(worker_id.clone(), props, tx, NOW_TIME, max_inflight_tasks);
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    // Drain the ConnectionResult.
    let _ = rx.recv().await;
    Ok(rx)
}

/// Queue an action with the given input_root_digest and return the channel.
async fn queue_action(
    scheduler: &SimpleScheduler,
    action_digest: DigestInfo,
    input_root_digest: DigestInfo,
    platform_props: HashMap<String, String>,
) -> Result<(), Error> {
    let mut action_info = make_base_action_info(make_system_time(1), action_digest);
    Arc::make_mut(&mut action_info).platform_properties = platform_props;
    Arc::make_mut(&mut action_info).input_root_digest = input_root_digest;
    let client_id = OperationId::default();
    scheduler
        .add_action(client_id, action_info)
        .await
        .err_tip(|| "Failed to add action")?;
    tokio::task::yield_now().await;
    Ok(())
}

/// Build a `SimpleSpec` with speculative prefetch enabled.
fn spec_with_prefetch(
    threshold: u64,
    ttl_s: u64,
) -> SimpleSpec {
    SimpleSpec {
        enable_speculative_prefetch: true,
        speculative_prefetch_backlog_threshold: threshold,
        speculative_prefetch_ttl_s: ttl_s,
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// T1: Feature gate OFF — no PrefetchInputs emitted
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec: with `enable_speculative_prefetch = false`, the backlog trigger block
// in `do_try_match` MUST NOT execute. Verified at two observable layers:
//
// Layer A (config default): `SimpleSpec::default()` now has
//   `enable_speculative_prefetch = true` — the feature is ENABLED by default,
//   drift-proof (per user 2026-07-07: don't default features to OFF, it delays
//   fixing their bugs). This layer pins the shipped default; T8 also covers it.
//   Config `false` is the operational kill-switch, and the OFF path itself is
//   proven by Layer B below (which SETS the flag `false` explicitly).
//
// Layer B (scheduling, OFF path): with the flag SET `false`, threshold=1, ALL
//   workers saturated PLUS a platform-mismatched idle worker, do_try_match MUST
//   NOT emit PrefetchInputs. The mismatch worker is present so that IF the gate
//   were removed (mutation), the code would reach `inner_find_worker_for_action`,
//   fail on mismatch, and return None → still no PrefetchInputs. Gate-removal is
//   therefore only PARTIALLY catchable via scheduling output (the gate guards the
//   entire block, but when `inner_find_worker_for_action` returns None the end
//   result is identical).
//
// The AUTHORITATIVE OFF-path test is now Layer B (explicit flag `false`); Layer A
// pins the drift-proof ON default.
//
// Bespoke failure messages:
//   Layer A: "T1: enable_speculative_prefetch MUST default to TRUE — drift-proof ON default"
//   Layer B: "T1: PrefetchInputs emitted with gate OFF — MUST NOT emit when feature disabled"
#[nativelink_test]
async fn t1_feature_gate_off_no_prefetch_inputs() -> Result<(), Error> {
    // ── Layer A: config default assertion (default is now ON, drift-proof) ──────
    let spec = SimpleSpec::default();
    assert!(
        spec.enable_speculative_prefetch,
        "T1: enable_speculative_prefetch MUST default to TRUE — drift-proof ON default \
         (per user 2026-07-07; config `false` is the kill-switch, off-path pinned by Layer B)"
    );

    // ── Layer B: scheduling output — gate=OFF, all matching workers saturated, ──
    //    one platform-mismatch idle worker present (ensures code would reach
    //    inner_find_worker_for_action if gate were removed).
    // ──────────────────────────────────────────────────────────────────────────
    let task_change_notify = Arc::new(Notify::new());
    let spec_gate_off = SimpleSpec {
        enable_speculative_prefetch: false,
        speculative_prefetch_backlog_threshold: 1,
        speculative_prefetch_ttl_s: 60,
        supported_platform_properties: Some({
            let mut m = std::collections::HashMap::new();
            m.insert("env".to_string(), nativelink_config::schedulers::PropertyType::Exact);
            m
        }),
        ..Default::default()
    };
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec_gate_off,
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Worker A: matching platform (env=prod), max_inflight_tasks=1 → saturates.
    let mut props_prod = PlatformProperties::default();
    props_prod.properties.insert(
        "env".to_string(),
        nativelink_util::platform_properties::PlatformPropertyValue::Exact("prod".to_string()),
    );
    let worker_a = WorkerId("t1_worker_prod".to_string());
    let mut rx_a = add_worker_with_slots(&scheduler, worker_a, props_prod.clone(), 1).await?;

    // Worker B: idle but MISMATCHED platform (env=dev).
    let mut props_dev = PlatformProperties::default();
    props_dev.properties.insert(
        "env".to_string(),
        nativelink_util::platform_properties::PlatformPropertyValue::Exact("dev".to_string()),
    );
    let worker_b = WorkerId("t1_worker_dev".to_string());
    let mut rx_b = add_worker_with_slots(&scheduler, worker_b, props_dev, 0).await?;

    // Queue action requiring env=prod: dispatched to A (A now saturated).
    let mut action_props = HashMap::new();
    action_props.insert("env".to_string(), "prod".to_string());
    queue_action(
        &scheduler, DigestInfo::new([1u8; 32], 1), DigestInfo::new([2u8; 32], 2),
        action_props.clone(),
    ).await?;
    scheduler.do_try_match_for_test().await?;
    for _ in 0..3 { tokio::task::yield_now().await; }
    while rx_a.try_recv().is_ok() {}
    while rx_b.try_recv().is_ok() {}

    // Queue action 2 (env=prod): A is saturated, B mismatches → stays queued.
    // Depth=1 >= threshold=1. Gate=OFF → backlog block MUST NOT execute.
    queue_action(
        &scheduler, DigestInfo::new([3u8; 32], 3), DigestInfo::new([4u8; 32], 4),
        action_props,
    ).await?;
    scheduler.do_try_match_for_test().await?;
    for _ in 0..5 { tokio::task::yield_now().await; }

    // Neither A nor B must have received PrefetchInputs.
    while let Ok(m) = rx_a.try_recv() {
        assert!(
            !matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))),
            "T1: PrefetchInputs emitted to worker A with gate OFF — MUST NOT emit when feature disabled"
        );
    }
    while let Ok(m) = rx_b.try_recv() {
        assert!(
            !matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))),
            "T1: PrefetchInputs emitted with gate OFF — MUST NOT emit when feature disabled"
        );
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T1b: feature-OFF path inertness — do_try_match leaves the coalesce guard EMPTY
// ─────────────────────────────────────────────────────────────────────────────
//
// testing-czar G-C. Two halves:
//  (1) NON-VACUITY: the guard CAN be non-empty — a direct send_prefetch_inputs
//      populates it (proves prefetch_coalesce_guard_len observes a real value,
//      not a constant 0). This is the same production fn the backlog block calls.
//  (2) CONTRACT: a gate-OFF SimpleScheduler running do_try_match over a real
//      backlog (more queued actions than idle-worker slots) leaves ITS guard
//      EMPTY and emits NO PrefetchInputs — byte-identical to pre-feature.
//
// Coverage limitation (recorded in deferred_tasks, #speculative-prefetch-t1b-
// gate-mutation): the greedy normal match drains the backlog before the trigger
// ever sees a still-queued action WITH an idle worker in a single do_try_match
// cycle, so the gate-ON path cannot be driven to POPULATE the guard through
// do_try_match in this harness; the trigger's guts are covered by T2/T3/T4's
// direct calls instead. This test therefore pins the OFF contract + non-vacuity
// of the inspection, not a do_try_match-scope gate-removal mutation.
#[nativelink_test]
async fn t1b_feature_off_leaves_coalesce_guard_empty() -> Result<(), Error> {
    // (1) NON-VACUITY via a direct emit on a gate-ON scheduler.
    {
        let task_change_notify = Arc::new(Notify::new());
        let spec_on = spec_with_prefetch(1, 60);
        let (scheduler_on, _ws) = SimpleScheduler::new_with_callback(
            &spec_on,
            memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None, None, None, None,
        );
        let _rx = add_worker(&scheduler_on, WorkerId("t1b_on".to_string()), PlatformProperties::default()).await?;
        let ws = scheduler_on.worker_scheduler_for_test();
        assert_eq!(ws.prefetch_coalesce_guard_len().await, 0, "guard starts empty");
        assert!(
            ws.send_prefetch_inputs(
                &PlatformProperties::default(), &OperationId::default(),
                DigestInfo::new([1u8; 32], 1), vec![], 60, &mut HashMap::new(),
            ).await,
            "non-vacuity precondition: a direct emit to an idle worker must succeed"
        );
        assert_eq!(
            ws.prefetch_coalesce_guard_len().await, 1,
            "non-vacuity: prefetch_coalesce_guard_len must observe the recorded entry (proves the \
             OFF assertion below is not comparing against a constant 0)"
        );
    }

    // (2) CONTRACT: gate OFF + a backlog → guard stays empty, no PrefetchInputs.
    let task_change_notify = Arc::new(Notify::new());
    let spec_off = SimpleSpec {
        enable_speculative_prefetch: false,
        speculative_prefetch_backlog_threshold: 1,
        speculative_prefetch_ttl_s: 60,
        ..Default::default()
    };
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec_off,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    // One worker, max_inflight=1; queue THREE actions → 1 dispatched, 2 remain
    // queued (backlog=2 >= threshold=1). Gate OFF ⇒ backlog block never runs.
    let mut rx = add_worker_with_slots(
        &scheduler, WorkerId("t1b_off".to_string()), PlatformProperties::default(), 1,
    ).await?;
    for i in 0..3u8 {
        queue_action(
            &scheduler,
            DigestInfo::new([i + 1; 32], u64::from(i) + 1),
            DigestInfo::new([i + 100; 32], u64::from(i) + 100),
            HashMap::new(),
        ).await?;
    }
    scheduler.do_try_match_for_test().await?;
    for _ in 0..5 { tokio::task::yield_now().await; }

    assert_eq!(
        scheduler.worker_scheduler_for_test().prefetch_coalesce_guard_len().await,
        0,
        "T1b (2026-07-05): feature-OFF is NOT inert — the coalesce guard is non-empty after \
         do_try_match with the gate OFF. The `if self.enable_speculative_prefetch` gate must \
         skip the entire backlog block."
    );
    while let Ok(m) = rx.try_recv() {
        assert!(
            !matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))),
            "T1b: PrefetchInputs emitted with the gate OFF — the backlog block must not run"
        );
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T2: emission — send_prefetch_inputs delivers a PrefetchInputs to an idle worker
// ─────────────────────────────────────────────────────────────────────────────
//
// testing-czar C1: the prior T2 was VACUOUS (it passed with emission fully
// disabled). This drives the emission path DETERMINISTICALLY by calling the
// production `ApiWorkerScheduler::send_prefetch_inputs` directly (the same fn
// `do_try_match`'s backlog trigger calls) with one idle worker, and asserts the
// worker's rx receives exactly one `PrefetchInputs` carrying the op_id +
// input_root_digest. Vacuity probe: disabling `send_prefetch_inputs`
// (unconditional early `return false`) makes this red-fail.
//
// Bespoke failure message:
//   "T2: idle worker did not receive PrefetchInputs — send_prefetch_inputs emission broken"
#[nativelink_test]
async fn t2_send_prefetch_inputs_delivers_to_idle_worker() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(3, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    let worker_id = WorkerId("t2_idle_worker".to_string());
    let mut rx = add_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    let op_id = OperationId::default();
    let input_root = DigestInfo::new([7u8; 32], 4242);
    let sent = scheduler
        .worker_scheduler_for_test()
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
        .await;
    assert!(
        sent,
        "T2: send_prefetch_inputs returned false with one idle matching worker present — \
         emission should have found the worker and sent"
    );
    tokio::task::yield_now().await;

    // The idle worker must have received exactly one PrefetchInputs matching
    // the op_id + input_root_digest we asked for.
    let mut got: Option<PrefetchInputs> = None;
    while let Ok(m) = rx.try_recv() {
        if let Some(update_for_worker::Update::PrefetchInputs(p)) = m.update {
            got = Some(p);
        }
    }
    let p = got.expect(
        "T2: idle worker did not receive PrefetchInputs — send_prefetch_inputs emission broken",
    );
    assert_eq!(
        p.operation_id,
        op_id.to_string(),
        "T2: PrefetchInputs carried the wrong operation_id"
    );
    let got_root: DigestInfo = p
        .input_root_digest
        .as_ref()
        .expect("T2: PrefetchInputs missing input_root_digest")
        .try_into()
        .expect("T2: PrefetchInputs input_root_digest not a valid DigestInfo");
    assert_eq!(
        got_root, input_root,
        "T2: PrefetchInputs carried the wrong input_root_digest"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T3: coalesce guard — a second emit for the SAME op is suppressed (G5 fan-out=1)
// ─────────────────────────────────────────────────────────────────────────────
//
// testing-czar C2: the prior T3's `total_prefetch <= 1` bound was satisfied by
// ZERO (no emit ever fired). This drives the real dedup: two `send_prefetch_inputs`
// calls for the SAME op → the worker receives EXACTLY ONE message, the second
// call returns `false`, and `speculative_prefetch_coalesce_suppressed` increments
// by exactly one. Vacuity probe: remove the `prefetch_coalesce_guard.contains`
// check → the second emit fires (worker gets 2) and the counter stays 0.
//
// Bespoke failure message baked into the asserts.
#[nativelink_test]
async fn t3_coalesce_guard_suppresses_duplicate_emit() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(3, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    let worker_id = WorkerId("t3_idle_worker".to_string());
    let mut rx = add_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    let op_id = OperationId::default();
    let input_root = DigestInfo::new([8u8; 32], 900);

    let suppressed_before = ws
        .get_metrics()
        .speculative_prefetch_coalesce_suppressed
        .load(core::sync::atomic::Ordering::Relaxed);

    let first = ws
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
        .await;
    assert!(first, "T3: first emit for a fresh op must succeed");

    let second = ws
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
        .await;
    assert!(
        !second,
        "T3: second emit for the SAME op must be coalesced (return false) — G5 fan-out=1 broken"
    );

    let suppressed_after = ws
        .get_metrics()
        .speculative_prefetch_coalesce_suppressed
        .load(core::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        suppressed_after - suppressed_before,
        1,
        "T3: speculative_prefetch_coalesce_suppressed must increment by exactly 1 on the \
         coalesced second emit (the observability the reviewers demanded)"
    );

    tokio::task::yield_now().await;
    let prefetch_count = {
        let mut n = 0;
        while let Ok(m) = rx.try_recv() {
            if matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))) {
                n += 1;
            }
        }
        n
    };
    assert_eq!(
        prefetch_count, 1,
        "T3: worker received {prefetch_count} PrefetchInputs for the same op — coalesce guard \
         must deliver EXACTLY one (dedup broken)"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T4 (reap): coalesce guard is REAPED on worker eviction so a rerouted op can
// re-prefetch
// ─────────────────────────────────────────────────────────────────────────────
//
// distsys BLOCK-1 / testing-czar C5 / red-team A-F1 (convergent): a stale
// coalesce entry for an op whose worker died must NOT permanently suppress a
// fresh prefetch to a HEALTHY worker. `immediate_evict_worker` (reached via the
// public `remove_worker`) reaps the coalesce entry for every op the dead worker
// held (keyed on client_operation_id, matching the map). This test: emit for an
// op to worker A (succeeds), a second emit is coalesced (suppressed), remove
// worker A (reap), then emit AGAIN for the SAME op to idle worker B — it MUST
// succeed because the reap cleared the stale entry.
//
// Vacuity/mutation probe: remove the `prefetch_coalesce_guard.pop(&operation_id)`
// reap in immediate_evict_worker → the third emit is still coalesced (returns
// false) and this red-fails.
#[nativelink_test]
async fn t4_coalesce_guard_reaped_on_worker_evict() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(3, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    let worker_a = WorkerId("t4_worker_a".to_string());
    let _rx_a = add_worker(&scheduler, worker_a.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    let op_id = OperationId::default();
    let input_root = DigestInfo::new([9u8; 32], 555);

    // 1) First emit for the op → recorded in the coalesce guard (goes to A, the
    //    only idle worker).
    assert!(
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "T4: first emit must succeed"
    );
    // 2) A second emit for the same op is coalesced (proves the entry exists).
    assert!(
        !ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "T4: second emit must be coalesced (entry present)"
    );

    // 3) Simulate worker A holding then dropping the op: reserve it to A (so the
    //    evict-drain reaps it), then remove A. `find_and_reserve_worker` records
    //    the op in A's running_action_infos under client_operation_id — the same
    //    key immediate_evict_worker drains and reaps.
    let action_info = {
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([1u8; 32], 1));
        Arc::make_mut(&mut ai).input_root_digest = input_root;
        ActionInfoWithProps {
            inner: ai,
            platform_properties: PlatformProperties::default(),
        }
    };
    let reserved = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op_id, &action_info, false)
        .await;
    assert!(
        reserved.is_some(),
        "T4: precondition — the op must reserve onto worker A so the evict-drain reaps it"
    );

    // Evict worker A → immediate_evict_worker drains A's held ops and reaps their
    // coalesce entries.
    ws.remove_worker(&worker_a)
        .await
        .err_tip(|| "T4: remove_worker A failed")?;
    tokio::task::yield_now().await;

    // 4) A fresh idle worker B is available; re-emitting for the SAME op MUST now
    //    succeed because the reap cleared the stale entry.
    let worker_b = WorkerId("t4_worker_b".to_string());
    let _rx_b = add_worker(&scheduler, worker_b.clone(), PlatformProperties::default()).await?;
    assert!(
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "distsys BLOCK-1 (2026-07-05): coalesce guard NOT reaped on worker evict — a rerouted \
         op's stale entry permanently suppressed its re-prefetch to a healthy worker. \
         immediate_evict_worker must pop the coalesce entry for every op the dead worker held."
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// D2 (reap on reroute): coalesce guard is REAPED on unreserve/reroute, not only
// on worker eviction
// ─────────────────────────────────────────────────────────────────────────────
//
// distsys MINOR-1 (D2): the coalesce-guard reap previously fired ONLY in
// immediate_evict_worker (dead-worker drain). An assign-Aborted reroute goes
// through unreserve_worker (a re-queue that is NOT a worker death), so its stale
// coalesce entry persisted until the LRU cap evicted it — meanwhile a legitimate
// re-prefetch of the rerouted op was suppressed by the dedup `contains` check
// (hit-rate loss). The reap is now mirrored in inner_unreserve_worker, keyed on
// the SAME client_operation_id the guard was inserted with. This test: emit for
// an op (guard populated), a second emit is coalesced, reserve the op onto the
// worker, then unreserve it (reroute) — the guard MUST be reaped so a fresh emit
// for the SAME op succeeds.
//
// Vacuity/mutation probe: remove the `prefetch_coalesce_guard.pop(operation_id)`
// reap in inner_unreserve_worker → the guard len stays 1 and the re-emit is still
// coalesced (returns false) → both asserts red-fail.
#[nativelink_test]
async fn d2_coalesce_guard_reaped_on_unreserve_reroute() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(3, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    let worker_a = WorkerId("d2_worker_a".to_string());
    let _rx_a = add_worker(&scheduler, worker_a.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    let op_id = OperationId::default();
    let input_root = DigestInfo::new([9u8; 32], 555);

    // 1) First emit for the op → recorded in the coalesce guard (goes to A, the
    //    only idle worker).
    assert!(
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "D2: first emit must succeed"
    );
    assert_eq!(
        ws.prefetch_coalesce_guard_len().await,
        1,
        "D2: precondition — the coalesce guard must hold exactly the one emitted op"
    );
    // 2) A second emit for the same op is coalesced (proves the entry exists).
    assert!(
        !ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "D2: second emit must be coalesced (entry present)"
    );

    // 3) Reserve the op onto worker A (so unreserve_worker has a live reservation
    //    to release — find_and_reserve_worker records it under
    //    client_operation_id, the same key the reap pops).
    let action_info = {
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([1u8; 32], 1));
        Arc::make_mut(&mut ai).input_root_digest = input_root;
        ActionInfoWithProps {
            inner: ai,
            platform_properties: PlatformProperties::default(),
        }
    };
    let reserved = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op_id, &action_info, false)
        .await;
    assert!(
        reserved.is_some(),
        "D2: precondition — the op must reserve onto worker A so the reroute can unreserve it"
    );

    // 4) Unreserve the op from worker A (the reroute path, NOT a worker death).
    //    inner_unreserve_worker must reap the op's coalesce entry.
    ws.unreserve_worker(&worker_a, &op_id).await;
    tokio::task::yield_now().await;

    // The guard must now be empty (reaped) — the direct observable.
    assert_eq!(
        ws.prefetch_coalesce_guard_len().await,
        0,
        "D2 (2026-07-06): coalesce guard NOT reaped on unreserve/reroute — the rerouted \
         op's stale entry persisted (only immediate_evict_worker reaped, not \
         inner_unreserve_worker). It must pop the coalesce entry for the unreserved op."
    );

    // 5) Worker A is idle again; re-emitting for the SAME op MUST now succeed
    //    because the reap cleared the stale entry (the end-to-end consequence).
    assert!(
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut HashMap::new())
            .await,
        "D2 (2026-07-06): re-prefetch of a rerouted op was suppressed by the stale coalesce \
         entry — inner_unreserve_worker must reap it so the op can re-prefetch after reroute."
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// send_prefetch_inputs with NO eligible idle worker → no emit (renamed from the
// mislabeled "t4_platform_mismatch"; it tests the no-idle-worker path, not a
// spec-T4 reap — distsys F5 numbering fix)
// ─────────────────────────────────────────────────────────────────────────────
//
// `send_prefetch_inputs` peeks for an idle worker matching the platform
// properties; if none matches it returns false and emits nothing. Here the only
// worker lacks the required "gpu" property, so a gpu-requiring prefetch finds no
// eligible worker and must NOT emit.
#[nativelink_test]
async fn send_prefetch_inputs_no_eligible_idle_worker_no_emit() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = SimpleSpec {
        enable_speculative_prefetch: true,
        speculative_prefetch_backlog_threshold: 1,
        speculative_prefetch_ttl_s: 60,
        supported_platform_properties: Some({
            let mut m = std::collections::HashMap::new();
            m.insert("gpu".to_string(), nativelink_config::schedulers::PropertyType::Exact);
            m
        }),
        ..Default::default()
    };
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    // Worker supports NO "gpu" property.
    let worker_id = WorkerId("mismatch_worker_no_gpu".to_string());
    let mut rx = add_worker(&scheduler, worker_id, PlatformProperties::default()).await?;

    // A prefetch that REQUIRES gpu=required has no eligible idle worker.
    let mut gpu_props = PlatformProperties::default();
    gpu_props.properties.insert(
        "gpu".to_string(),
        nativelink_util::platform_properties::PlatformPropertyValue::Exact("required".to_string()),
    );
    let op_id = OperationId::default();
    let sent = scheduler
        .worker_scheduler_for_test()
        .send_prefetch_inputs(
            &gpu_props,
            &op_id,
            DigestInfo::new([91u8; 32], 900),
            vec![],
            60,
            &mut HashMap::new(),
        )
        .await;
    assert!(
        !sent,
        "send_prefetch_inputs must return false when no idle worker satisfies the platform \
         properties — must not emit to an ineligible worker"
    );
    tokio::task::yield_now().await;
    while let Ok(m) = rx.try_recv() {
        assert!(
            !matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))),
            "PrefetchInputs emitted despite platform mismatch — must not emit to ineligible worker"
        );
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T8: Config deserialization — enable_speculative_prefetch defaults TRUE
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec: `SimpleSpec::default()` must have `enable_speculative_prefetch = true`
// (feature ENABLED by default, drift-proof — per user 2026-07-07: don't default
// features to OFF, it delays fixing their bugs). Deserializing `{}` must produce
// the same value, so config drift (an omitted flag) can no longer dark the
// feature; config `false` is the operational kill-switch.
//
// This is the registry's serde contract test: it pins that the manual
// `impl Default` and the `#[serde(default = "default_true")]` agree on the
// shipped default. The backlog-threshold (3) and TTL (60) defaults are NOT
// flipped and are asserted unchanged.
//
// Bespoke failure message baked into assert_eq!.
#[nativelink_test]
async fn t8_config_defaults_gate_off() -> Result<(), Error> {
    let default_spec = SimpleSpec::default();
    assert_eq!(
        default_spec.enable_speculative_prefetch,
        true,
        "T8: SimpleSpec::default() must have enable_speculative_prefetch=true (ENABLED \
         by default, drift-proof, per user 2026-07-07); found false — config drift or a \
         reverted default would silently dark the feature"
    );
    assert_eq!(
        default_spec.speculative_prefetch_backlog_threshold,
        3,
        "T8: default backlog threshold must be 3; found {}",
        default_spec.speculative_prefetch_backlog_threshold
    );
    assert_eq!(
        default_spec.speculative_prefetch_ttl_s,
        60,
        "T8: default TTL must be 60s; found {}",
        default_spec.speculative_prefetch_ttl_s
    );

    // Deserialize from empty JSON to verify serde(default) wiring.
    let from_json: SimpleSpec = serde_json::from_str("{}")
        .expect("T8: deserialize SimpleSpec from empty JSON object failed");
    assert_eq!(
        from_json.enable_speculative_prefetch,
        true,
        "T8: serde-deserialized SimpleSpec{{}} must have enable_speculative_prefetch=true \
         (the #[serde(default = \"default_true\")] must fire on an omitted flag)"
    );
    assert_eq!(
        from_json.speculative_prefetch_backlog_threshold,
        3,
        "T8: serde-deserialized SimpleSpec{{}} must have backlog_threshold=3"
    );
    assert_eq!(
        from_json.speculative_prefetch_ttl_s,
        60,
        "T8: serde-deserialized SimpleSpec{{}} must have ttl_s=60"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// T9: Proto roundtrip — PrefetchInputs serializes/deserializes correctly
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec: `PrefetchInputs` with a known `operation_id` and `input_root_digest`
// must survive a prost encode → decode roundtrip with identical fields.
//
// Bespoke failure message: assert_eq! on operation_id and digest fields.
#[nativelink_test]
async fn t9_proto_roundtrip_prefetch_inputs() -> Result<(), Error> {
    let digest = DigestInfo::new([0xAB_u8; 32], 12345);
    let proto_digest: Digest = digest.into();

    let original = PrefetchInputs {
        operation_id: "op-roundtrip-test".to_string(),
        input_root_digest: Some(proto_digest.clone()),
        missing_digest_peers: vec![],
        ttl_s: 90,
    };

    let mut buf = Vec::new();
    original.encode(&mut buf).expect("T9: PrefetchInputs::encode failed");

    let decoded = PrefetchInputs::decode(buf.as_slice())
        .expect("T9: PrefetchInputs::decode failed after encode");

    assert_eq!(
        decoded.operation_id, "op-roundtrip-test",
        "T9: operation_id field corrupted in proto roundtrip"
    );
    assert_eq!(
        decoded.input_root_digest.as_ref().map(|d| d.hash.as_str()),
        Some(proto_digest.hash.as_str()),
        "T9: input_root_digest.hash corrupted in proto roundtrip"
    );
    assert_eq!(
        decoded.input_root_digest.as_ref().map(|d| d.size_bytes),
        Some(12345_i64),
        "T9: input_root_digest.size_bytes corrupted in proto roundtrip"
    );
    assert_eq!(
        decoded.ttl_s, 90,
        "T9: ttl_s field corrupted in proto roundtrip (the forwarded operator TTL)"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage A helpers (§2.1 capacity-agnostic selector + §2.4 per-worker cap)
// ─────────────────────────────────────────────────────────────────────────────

/// Add a worker with `max_inflight_tasks = 1` and reserve one dummy op onto it,
/// so `can_accept_work() == false` (BUSY) while the worker stays HEALTHY (no
/// quarantine / pressure). This is the exact regime the boondoggle failed on:
/// a backlog means every capable worker is busy. Returns the worker's rx (with
/// the ConnectionResult already drained; no StartAction is sent because
/// `find_and_reserve_worker` returns the tx/msg to the caller unsent).
async fn add_busy_healthy_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let rx = add_worker_with_slots(scheduler, worker_id.clone(), props.clone(), 1).await?;
    // Reserve a filler op so running_action_infos.len() (1) == max_inflight (1)
    // → can_accept_work() is false. A UNIQUE digest per worker so the filler
    // reserves onto THIS worker (the reserve is capacity-gated + locality-aware;
    // a shared digest could pile all fillers onto one holder).
    let filler_root = DigestInfo::new(
        {
            let mut h = [0u8; 32];
            h[0] = 0xF0;
            // Cheap uniqueness from the worker id bytes.
            for (i, b) in worker_id.0.bytes().take(31).enumerate() {
                h[i + 1] = b;
            }
            h
        },
        7,
    );
    let filler_op = OperationId::default();
    let filler_ai = {
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xEE; 32], 7));
        // Carry the worker's platform properties as the HashMap<String,String>
        // the action model expects (only used by restore on unreserve; the
        // capability match uses the `props` passed to find_and_reserve below).
        Arc::make_mut(&mut ai).platform_properties = props
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().into_owned()))
            .collect();
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        ActionInfoWithProps {
            inner: ai,
            platform_properties: props.clone(),
        }
    };
    let reserved = scheduler
        .worker_scheduler_for_test()
        .find_and_reserve_worker(&props, &filler_op, &filler_ai, false)
        .await;
    assert!(
        reserved.is_some(),
        "add_busy_healthy_worker precondition: filler op must reserve onto the fresh worker"
    );
    Ok(rx)
}

/// Drain the worker rx and return the number of `PrefetchInputs` messages it
/// received.
fn count_prefetch_inputs(rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>) -> usize {
    let mut n = 0;
    while let Ok(m) = rx.try_recv() {
        if matches!(m.update, Some(update_for_worker::Update::PrefetchInputs(_))) {
            n += 1;
        }
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// A1 (the anti-regression test for the whole redesign): prefetch FIRES under a
// backlog where EVERY capable worker is BUSY but HEALTHY.
// ─────────────────────────────────────────────────────────────────────────────
//
// The boondoggle (design §0): `send_prefetch_inputs` targeted
// `inner_find_worker_for_action`, whose FIRST line returns None unless some
// worker `can_accept_work()` (IDLE). The trigger fires only on a BACKLOG (all
// workers busy). Backlog ∧ idle are mutually exclusive → 0 emission under load.
//
// This test sets up exactly that regime: ONE capable worker, BUSY (max_inflight=1
// with a reserved filler op → can_accept_work()==false) but HEALTHY. The Stage-A
// capacity-agnostic selector MUST target it and deliver a PrefetchInputs.
//
// MUTATION (the anti-regression guarantee): revert the selector to the old
// idle-only `inner_find_worker_for_action` (change
// `find_prefetch_target_worker(...)` back to
// `inner_find_worker_for_action(platform_properties, false)`) → this red-fails
// with 0 emission because no worker can_accept_work() under the backlog.
#[nativelink_test]
async fn prefetch_fires_under_backlog_with_all_workers_busy() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    // The ONLY capable worker is busy (can_accept_work()==false) but healthy.
    let worker_busy = WorkerId("a1_busy_healthy".to_string());
    let mut rx = add_busy_healthy_worker(&scheduler, worker_busy.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    // Sanity: the OLD idle-only matcher finds NOTHING here (this is what made the
    // feature never fire). This pins the boondoggle precondition.
    assert!(
        ws.find_worker_for_action(&PlatformProperties::default(), false).await.is_none(),
        "A1 precondition: the idle-only matcher must return None under the backlog \
         (every capable worker is busy) — this is the boondoggle regime"
    );

    // Stage-A selector: a PrefetchInputs MUST be emitted to the BUSY worker.
    let op_id = OperationId::default();
    let input_root = DigestInfo::new([0xA1; 32], 4242);
    let mut per_cycle = HashMap::new();
    let sent = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut per_cycle,
        )
        .await;
    assert!(
        sent,
        "A1 (boondoggle): send_prefetch_inputs returned false under a backlog where the only \
         capable worker is BUSY-but-HEALTHY. The capacity-agnostic selector must target the \
         predicted (busy) worker — the idle-only inner_find_worker_for_action NEVER fires here."
    );

    let got = count_prefetch_inputs(&mut rx);
    assert_eq!(
        got, 1,
        "A1 (boondoggle): the busy-but-healthy worker received {got} PrefetchInputs; expected \
         exactly 1. Reverting the selector to the idle-only inner_find_worker_for_action makes \
         this 0 (0-emission-under-load — the whole redesign's anti-regression contract)."
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// A2: prefetch SKIPS a busy-but-HEALTH-PRESSURED worker even when it is the best
// locality holder.
// ─────────────────────────────────────────────────────────────────────────────
//
// §2.1: the selector DROPS the capacity gates but KEEPS the health gates
// (`swap_pressured`/`disk_pressured`/`indefinite_pin_saturated`/`quarantined_at`).
// Prefetching to a pressured worker wastes its scarce resource. Here the sole
// capable worker holds the input_root (perfect Tier-1 locality) but is
// swap-pressured → the selector must decline (no emit), NOT target it.
//
// MUTATION: drop the health gate in `find_prefetch_target_worker` (remove the
// `swap_pressured` skip from its viability predicate) → the pressured holder is
// selected, `sent` becomes true, and this red-fails.
#[nativelink_test]
async fn prefetch_skips_health_pressured_worker() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    let worker = WorkerId("a2_pressured_holder".to_string());
    let mut rx = add_busy_healthy_worker(&scheduler, worker.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xA2; 32], 555);
    // Give the worker PERFECT locality (holds the exact input_root) …
    ws.update_cached_directories(&worker, HashSet::from([input_root]))
        .await
        .err_tip(|| "A2: update_cached_directories failed")?;
    // … then mark it swap-pressured (a health gate the selector MUST honor).
    ws.update_worker_swap_pressure(&worker, true, 50_000)
        .await
        .err_tip(|| "A2: update_worker_swap_pressure failed")?;

    let op_id = OperationId::default();
    let mut per_cycle = HashMap::new();
    let sent = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut per_cycle,
        )
        .await;
    assert!(
        !sent,
        "A2 (health gate): send_prefetch_inputs targeted a swap-pressured worker (its best \
         locality holder). The selector KEEPS the health gates — a pressured worker is NOT a \
         valid prefetch target (prefetch would waste its scarce resource)."
    );

    let got = count_prefetch_inputs(&mut rx);
    assert_eq!(
        got, 0,
        "A2 (health gate): the swap-pressured worker received {got} PrefetchInputs; expected 0. \
         Dropping the swap_pressured skip from the selector's viability predicate makes this 1."
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// A3: same-input fan-out is CAPPED per worker per cycle — prewarms SPREAD across
// the top-K locality workers instead of all piling on one.
// ─────────────────────────────────────────────────────────────────────────────
//
// §2.4: same-input fan-out (common Bazel shape) makes §2.1 return the SAME W for
// all N queued ops → all prewarm on W. A per-cycle per-worker cap (K, small)
// spreads the prewarms across the top-K locality workers. Here THREE busy-healthy
// workers all hold the same input_root (equal Tier-1 locality); N=6 distinct ops
// prefetch through ONE shared per-cycle counter. With cap K, no single worker
// may receive more than K prefetches this cycle.
//
// MUTATION: remove the per-worker cap check in the selector (stop consulting /
// bumping `per_cycle_targets`) → the selector returns the same (first-ranked)
// worker for all 6 ops → that worker receives all 6 → the ≤K assertion red-fails.
#[nativelink_test]
async fn prefetch_fanout_capped_per_worker() -> Result<(), Error> {
    const PREFETCH_PER_WORKER_PER_CYCLE_CAP: usize = 2;

    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xA3; 32], 999);
    // Three busy-healthy workers, ALL holding the exact input_root (equal Tier-1
    // locality) → without the cap the selector would return the same worker for
    // every op.
    let mut rxs = Vec::new();
    let worker_names = ["a3_w0", "a3_w1", "a3_w2"];
    for name in worker_names {
        let wid = WorkerId(name.to_string());
        let rx = add_busy_healthy_worker(&scheduler, wid.clone(), PlatformProperties::default()).await?;
        ws.update_cached_directories(&wid, HashSet::from([input_root]))
            .await
            .err_tip(|| "A3: update_cached_directories failed")?;
        rxs.push((wid, rx));
    }

    // Fan out N=6 DISTINCT ops (same input_root) through ONE shared per-cycle
    // counter — the do_try_match loop shape.
    let mut per_cycle: HashMap<WorkerId, usize> = HashMap::new();
    let n_ops = 6;
    let mut total_sent = 0;
    for _ in 0..n_ops {
        let op_id = OperationId::default();
        if ws
            .send_prefetch_inputs(
                &PlatformProperties::default(), &op_id, input_root, vec![], 60, &mut per_cycle,
            )
            .await
        {
            total_sent += 1;
        }
    }

    // Per-worker delivered counts.
    let mut per_worker: Vec<(String, usize)> = Vec::new();
    for (wid, rx) in &mut rxs {
        per_worker.push((wid.0.clone(), count_prefetch_inputs(rx)));
    }
    let max_on_one = per_worker.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let total_delivered: usize = per_worker.iter().map(|(_, c)| *c).sum();

    assert!(
        max_on_one <= PREFETCH_PER_WORKER_PER_CYCLE_CAP,
        "A3 (placement cap): one worker received {max_on_one} prefetches in a single cycle \
         (cap = {PREFETCH_PER_WORKER_PER_CYCLE_CAP}); per-worker counts {per_worker:?}. Removing \
         the per-cycle per-worker cap in the selector piles the whole same-input fan-out onto one \
         worker instead of spreading across the top-K locality holders."
    );
    // Consistency: delivered count matches the number of successful sends (no
    // ghost emits) and every emit landed on some worker.
    assert_eq!(
        total_delivered, total_sent,
        "A3: delivered PrefetchInputs ({total_delivered}) must equal successful send count \
         ({total_sent})"
    );
    // With 3 workers × cap 2 = 6 slots and 6 ops, the fan-out should SPREAD onto
    // more than one worker (the whole point of the cap).
    let workers_hit = per_worker.iter().filter(|(_, c)| *c > 0).count();
    assert!(
        workers_hit >= 2,
        "A3 (spread): the same-input fan-out landed on only {workers_hit} worker(s); the cap must \
         spread prewarms across multiple locality holders. Per-worker counts {per_worker:?}"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// A4: the selector is PEEK-ONLY — it reserves nothing and does not perturb real
// assignment. A prefetch peek followed by a real find_and_reserve produces the
// SAME assignment it would without the peek.
// ─────────────────────────────────────────────────────────────────────────────
//
// §2.1: "peek-only, reserves nothing". A prefetch selection must not mutate
// worker state (no running_action_infos change, no LRU promotion that flips the
// matcher's pick). Here an IDLE worker is the sole capable worker; we run a
// prefetch peek for an op, then a real find_and_reserve_worker for a different
// op, and assert the real assignment still lands on that worker AND the prefetch
// peek left running_action_infos untouched (only the real reserve added an op).
#[nativelink_test]
async fn prefetch_selector_is_peek_only_assignment_unchanged() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );

    // Idle worker, unlimited slots (so the real reserve succeeds).
    let worker = WorkerId("a4_worker".to_string());
    let _rx = add_worker(&scheduler, worker.clone(), PlatformProperties::default()).await?;
    let ws = scheduler.worker_scheduler_for_test();

    // Baseline: no ops running.
    assert_eq!(
        ws.worker_running_action_count_for_test(&worker).await,
        Some(0),
        "A4 precondition: worker starts with 0 running actions"
    );

    // A prefetch peek for op-P (peek-only; reserves nothing).
    let op_p = OperationId::default();
    let mut per_cycle = HashMap::new();
    let peeked = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &op_p, DigestInfo::new([0xA4; 32], 1), vec![], 60,
            &mut per_cycle,
        )
        .await;
    assert!(peeked, "A4: prefetch peek to the idle worker should emit");

    // The peek reserved NOTHING — running_action_infos is still empty.
    assert_eq!(
        ws.worker_running_action_count_for_test(&worker).await,
        Some(0),
        "A4 (peek-only): the prefetch selector RESERVED a slot — running_action_infos changed \
         after a peek-only prefetch. The selector must not mutate worker reservation state."
    );

    // A real find_and_reserve for a DIFFERENT op lands on the same (only) worker.
    let op_real = OperationId::default();
    let real_ai = {
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0x11; 32], 1));
        Arc::make_mut(&mut ai).input_root_digest = DigestInfo::new([0x22; 32], 2);
        ActionInfoWithProps {
            inner: ai,
            platform_properties: PlatformProperties::default(),
        }
    };
    let reserved = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op_real, &real_ai, false)
        .await;
    let (assigned, _, _) = reserved.expect("A4: the real op must reserve onto the idle worker");
    assert_eq!(
        assigned, worker,
        "A4: real assignment must still land on the sole capable worker after a peek-only prefetch"
    );
    // Exactly ONE running op now (the real reserve), confirming the peek added none.
    assert_eq!(
        ws.worker_running_action_count_for_test(&worker).await,
        Some(1),
        "A4 (peek-only): after one real reserve the worker must have exactly 1 running op — the \
         prefetch peek must have added 0"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// A4-LRU (pair-b T-1): the selector is LRU-NEUTRAL — a prefetch peek does NOT
// promote the chosen worker in the workers LRU, so a subsequent real match's
// LRU tiebreak still sees the SAME order.
// ─────────────────────────────────────────────────────────────────────────────
//
// A4 above uses a SINGLE worker, so an LRU-promotion mutation (`.peek()`→`.get()`
// on the chosen worker) is unobservable — there is no second worker to reorder
// relative to, and A4 asserts only `running_action_infos.len()` (type-enforced).
// This variant adds a SECOND idle worker so the workers LRU has an observable
// MRU→LRU order, prefetches to one of them, and asserts the order is UNCHANGED.
//
// MUTATION: change the chosen-worker `peek` in `send_prefetch_inputs`
// (`inner.workers.0.peek(&worker_id)`) or a `peek` inside
// `find_prefetch_target_worker` to `.get()` / `.get_mut()` (both promote the
// entry to MRU in `lru::LruCache`) → the peeked worker jumps to the front of the
// order and this assertion red-fails. That promotion would silently bias the
// NEXT real match's LRU-fallback tiebreak toward the prewarmed worker — a routing
// side effect the peek-only contract (§2.1) forbids.
#[nativelink_test]
async fn prefetch_selector_lru_neutral_two_workers() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    // Two idle workers. `add_worker` inserts (put) → each becomes MRU, so after
    // both inserts the LRU iteration order (MRU→LRU) is [w1, w0].
    let w0 = WorkerId("a4lru_w0".to_string());
    let w1 = WorkerId("a4lru_w1".to_string());
    let _rx0 = add_worker(&scheduler, w0.clone(), PlatformProperties::default()).await?;
    let _rx1 = add_worker(&scheduler, w1.clone(), PlatformProperties::default()).await?;

    let order_before = ws.worker_lru_order_for_test().await;
    assert_eq!(
        order_before,
        vec![w1.clone(), w0.clone()],
        "A4-LRU precondition: two workers added w0 then w1 must be ordered [w1(MRU), w0(LRU)] — \
         the fixture cannot observe promotion otherwise. Got {order_before:?}"
    );

    // Give w0 (the LRU/back entry) locality so the selector chooses IT — a
    // `.get()` mutation on the chosen worker would promote w0 to the FRONT,
    // flipping the order to [w0, w1] and making this test fail.
    let input_root = DigestInfo::new([0xB4; 32], 42);
    ws.update_cached_directories(&w0, HashSet::from([input_root]))
        .await
        .err_tip(|| "A4-LRU: update_cached_directories failed")?;

    let op_p = OperationId::default();
    let mut per_cycle = HashMap::new();
    let peeked = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &op_p, input_root, vec![], 60, &mut per_cycle,
        )
        .await;
    assert!(
        peeked,
        "A4-LRU: prefetch peek to the locality-holder idle worker should emit (non-vacuity: an \
         empty peek would never exercise the promotion the mutation targets)"
    );

    let order_after = ws.worker_lru_order_for_test().await;
    assert_eq!(
        order_after, order_before,
        "A4-LRU (peek-only / no LRU promotion): the prefetch selector PROMOTED the chosen worker \
         in the workers LRU — order changed from {order_before:?} to {order_after:?}. The selector \
         must use `peek` (non-promoting); a `.get()`/`.get_mut()` on the chosen worker biases the \
         NEXT real match's LRU-fallback tiebreak toward the prewarmed worker (§2.1 forbids this)."
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// EMIT-COUNTER: `speculative_prefetch_emitted` bumps by exactly 1 on a DELIVERED
// PrefetchInputs send (the positive-emit instrument the soak asserts `> 0`).
// ─────────────────────────────────────────────────────────────────────────────
//
// distsys MAJOR-1 / red-team convergent: the feature's defining incident (§0)
// was SILENT zero-emission; the soak needs a positive-emit counter to prove
// Stage A fires under load. One idle worker; a direct `send_prefetch_inputs`
// delivers a PrefetchInputs → `speculative_prefetch_emitted` goes 0→1 and
// `speculative_prefetch_no_target` stays 0 (a delivered send is neither a
// no-target nor a coalesce skip).
//
// MUTATION: remove the `speculative_prefetch_emitted.fetch_add(1, ...)` on the
// delivered-send path (`send_prefetch_inputs` `true` return) → the counter stays
// 0 and this red-fails with its bespoke message.
#[nativelink_test]
async fn prefetch_emitted_counter_increments_on_delivered_send() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();
    let _rx = add_worker(&scheduler, WorkerId("emit_w".to_string()), PlatformProperties::default()).await?;

    let emitted_before = ws.get_metrics().speculative_prefetch_emitted.load(Ordering::Relaxed);
    let no_target_before = ws.get_metrics().speculative_prefetch_no_target.load(Ordering::Relaxed);

    let sent = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &OperationId::default(),
            DigestInfo::new([0xE1; 32], 1), vec![], 60, &mut HashMap::new(),
        )
        .await;
    assert!(sent, "EMIT precondition: a direct emit to an idle worker must deliver");

    let emitted_after = ws.get_metrics().speculative_prefetch_emitted.load(Ordering::Relaxed);
    let no_target_after = ws.get_metrics().speculative_prefetch_no_target.load(Ordering::Relaxed);
    assert_eq!(
        emitted_after - emitted_before,
        1,
        "EMIT: speculative_prefetch_emitted did not increment by 1 on a DELIVERED PrefetchInputs \
         send. Without this positive-emit counter the soak cannot prove Stage A fires (the \
         boondoggle's silent-zero-emission signature). before={emitted_before} after={emitted_after}"
    );
    assert_eq!(
        no_target_after, no_target_before,
        "EMIT: speculative_prefetch_no_target moved on a SUCCESSFUL delivery — a delivered send is \
         not a no-target case. before={no_target_before} after={no_target_after}"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// NO-TARGET-COUNTER: `speculative_prefetch_no_target` bumps when EVERY capable
// worker is health-pressured → the selector returns None (the silent no-op path
// that hid the original boondoggle).
// ─────────────────────────────────────────────────────────────────────────────
//
// distsys MAJOR-1 / red-team pre-mortem: a fleet-wide pressure event drives
// `find_prefetch_target_worker` to None for the whole backlog and the feature
// silently emits nothing. This test makes that observable: the ONLY capable
// worker holds the input_root but is swap-pressured → selector None →
// `speculative_prefetch_no_target` goes 0→1 while `speculative_prefetch_emitted`
// stays flat (fired, found no viable target, delivered nothing).
//
// MUTATION: bump `speculative_prefetch_emitted` instead of
// `speculative_prefetch_no_target` on the `None` arm of the selector call in
// `send_prefetch_inputs` → `no_target` stays 0 and this red-fails.
#[nativelink_test]
async fn prefetch_no_target_counter_increments_when_all_workers_pressured() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_prefetch(1, 60);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    // The ONLY capable worker: perfect locality, but swap-pressured (a health
    // gate the selector honors) → no viable target for the op.
    let worker = WorkerId("notarget_pressured".to_string());
    let _rx = add_busy_healthy_worker(&scheduler, worker.clone(), PlatformProperties::default()).await?;
    let input_root = DigestInfo::new([0xE2; 32], 2);
    ws.update_cached_directories(&worker, HashSet::from([input_root]))
        .await
        .err_tip(|| "NO-TARGET: update_cached_directories failed")?;
    ws.update_worker_swap_pressure(&worker, true, 50_000)
        .await
        .err_tip(|| "NO-TARGET: update_worker_swap_pressure failed")?;

    let emitted_before = ws.get_metrics().speculative_prefetch_emitted.load(Ordering::Relaxed);
    let no_target_before = ws.get_metrics().speculative_prefetch_no_target.load(Ordering::Relaxed);

    let sent = ws
        .send_prefetch_inputs(
            &PlatformProperties::default(), &OperationId::default(), input_root, vec![], 60,
            &mut HashMap::new(),
        )
        .await;
    assert!(
        !sent,
        "NO-TARGET precondition: with the sole capable worker swap-pressured the selector must \
         return None and send_prefetch_inputs must return false"
    );

    let emitted_after = ws.get_metrics().speculative_prefetch_emitted.load(Ordering::Relaxed);
    let no_target_after = ws.get_metrics().speculative_prefetch_no_target.load(Ordering::Relaxed);
    assert_eq!(
        no_target_after - no_target_before,
        1,
        "NO-TARGET: speculative_prefetch_no_target did not increment when every capable worker was \
         health-pressured (selector None). This is the silent zero-benefit path that hid the \
         original boondoggle — the soak must be able to distinguish 'fired but no target' from a \
         real emit. before={no_target_before} after={no_target_after}"
    );
    assert_eq!(
        emitted_after, emitted_before,
        "NO-TARGET: speculative_prefetch_emitted moved on a no-target case — nothing was delivered. \
         Bumping emitted instead of no_target on the selector's None arm makes this red-fail. \
         before={emitted_before} after={emitted_after}"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HIT-VS-MISS: at ASSIGNMENT, when the coalesce guard holds a recorded prefetch
// target `op→W`, `speculative_prefetch_hit` bumps if the op is assigned to W and
// `speculative_prefetch_miss` bumps if it is assigned elsewhere. MEASUREMENT
// ONLY — the matcher must NOT route/bias on the recorded target.
// ─────────────────────────────────────────────────────────────────────────────
//
// red-team A1 (gossip-window): Stage A ships no instrument for how often a
// prewarm actually LANDS on the run worker vs is wasted by a reroute. This test
// drives both arms:
//   HIT : one idle worker W holds R → prefetch op_P→W → reserve op_P lands on W
//         (Tier-1 holder) → assigned == recorded → hit.
//   MISS: W (busy holder of R) + X (idle non-holder) → prefetch op_M→W (selector
//         is capacity-agnostic, picks the holder W) → reserve op_M: the real
//         matcher SKIPS busy W (worker_is_viable requires can_accept_work) and
//         lands op_M on X → assigned X != recorded W → miss.
// The MISS arm ALSO proves the matcher did NOT route by the recorded target: an
// IDENTICAL reserve with NO coalesce record lands on the SAME worker X (the
// record changes the metric, not the assignment).
//
// MUTATION: swap the hit/miss increments at the assignment read
// (`prepare_worker_run_action`) — the HIT case then bumps miss and the MISS case
// bumps hit → both assertions red-fail.
#[nativelink_test]
async fn prefetch_hit_vs_miss() -> Result<(), Error> {
    // ── HIT: prewarm lands on the run worker ──────────────────────────────────
    {
        let task_change_notify = Arc::new(Notify::new());
        let spec = spec_with_prefetch(1, 60);
        let (scheduler, _ws) = SimpleScheduler::new_with_callback(
            &spec,
            memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None, None, None, None,
        );
        let ws = scheduler.worker_scheduler_for_test();

        // One IDLE worker holding R (so the real reserve can land on it).
        let w = WorkerId("hit_w".to_string());
        let _rx = add_worker(&scheduler, w.clone(), PlatformProperties::default()).await?;
        let input_root = DigestInfo::new([0xC1; 32], 11);
        ws.update_cached_directories(&w, HashSet::from([input_root]))
            .await
            .err_tip(|| "HIT: update_cached_directories failed")?;

        let op_p = OperationId::default();
        let sent = ws
            .send_prefetch_inputs(
                &PlatformProperties::default(), &op_p, input_root, vec![], 60, &mut HashMap::new(),
            )
            .await;
        assert!(sent, "HIT precondition: prefetch to the idle holder must emit (records op_p→W)");

        let hit_before = ws.get_metrics().speculative_prefetch_hit.load(Ordering::Relaxed);
        let miss_before = ws.get_metrics().speculative_prefetch_miss.load(Ordering::Relaxed);

        // Reserve the SAME op (op_p) carrying input_root R → Tier-1 holder match
        // lands it on W == the recorded prefetch target.
        let ai = {
            let mut inner = make_base_action_info(make_system_time(1), DigestInfo::new([0x31; 32], 1));
            Arc::make_mut(&mut inner).input_root_digest = input_root;
            ActionInfoWithProps { inner, platform_properties: PlatformProperties::default() }
        };
        let reserved = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &op_p, &ai, false)
            .await;
        let (assigned, _, _) = reserved.expect("HIT: op_p must reserve onto the sole idle worker");
        assert_eq!(assigned, w, "HIT precondition: op_p must land on the holder W");

        let hit_after = ws.get_metrics().speculative_prefetch_hit.load(Ordering::Relaxed);
        let miss_after = ws.get_metrics().speculative_prefetch_miss.load(Ordering::Relaxed);
        assert_eq!(
            hit_after - hit_before,
            1,
            "HIT: op assigned to the SAME worker its prefetch targeted, but speculative_prefetch_hit \
             did not increment. before={hit_before} after={hit_after}"
        );
        assert_eq!(
            miss_after, miss_before,
            "HIT: speculative_prefetch_miss moved on a prewarm that LANDED on the run worker — \
             swapping the hit/miss increments makes this red-fail. before={miss_before} after={miss_after}"
        );
    }

    // ── MISS: prewarm target rerouted (op lands elsewhere) ────────────────────
    {
        let task_change_notify = Arc::new(Notify::new());
        let spec = spec_with_prefetch(1, 60);
        let (scheduler, _ws) = SimpleScheduler::new_with_callback(
            &spec,
            memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None, None, None, None,
        );
        let ws = scheduler.worker_scheduler_for_test();

        // W: BUSY holder of R (selector picks it; real matcher skips it as busy).
        // X: idle non-holder (where the op actually lands).
        let w = WorkerId("miss_w_busy_holder".to_string());
        let x = WorkerId("miss_x_idle".to_string());
        let _rxw = add_busy_healthy_worker(&scheduler, w.clone(), PlatformProperties::default()).await?;
        let _rxx = add_worker(&scheduler, x.clone(), PlatformProperties::default()).await?;
        let input_root = DigestInfo::new([0xC2; 32], 22);
        ws.update_cached_directories(&w, HashSet::from([input_root]))
            .await
            .err_tip(|| "MISS: update_cached_directories failed")?;

        // Prefetch op_M: the capacity-agnostic selector picks the best-locality
        // holder W (even though W is busy) → records op_M→W.
        let op_m = OperationId::default();
        let sent = ws
            .send_prefetch_inputs(
                &PlatformProperties::default(), &op_m, input_root, vec![], 60, &mut HashMap::new(),
            )
            .await;
        assert!(sent, "MISS precondition: prefetch to the busy holder W must emit (records op_m→W)");

        let hit_before = ws.get_metrics().speculative_prefetch_hit.load(Ordering::Relaxed);
        let miss_before = ws.get_metrics().speculative_prefetch_miss.load(Ordering::Relaxed);

        // Reserve op_M carrying R: the real matcher SKIPS busy W (worker_is_viable
        // requires can_accept_work) and lands op_M on idle X → assigned != recorded.
        let ai = {
            let mut inner = make_base_action_info(make_system_time(1), DigestInfo::new([0x32; 32], 2));
            Arc::make_mut(&mut inner).input_root_digest = input_root;
            ActionInfoWithProps { inner, platform_properties: PlatformProperties::default() }
        };
        let reserved = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &op_m, &ai, false)
            .await;
        let (assigned, _, _) = reserved.expect("MISS: op_m must reserve onto the idle worker X");
        assert_eq!(
            assigned, x,
            "MISS precondition: op_m must land on idle X (the real matcher skips busy holder W). \
             If it landed on W the matcher illegally routed by the coalesce record."
        );

        let hit_after = ws.get_metrics().speculative_prefetch_hit.load(Ordering::Relaxed);
        let miss_after = ws.get_metrics().speculative_prefetch_miss.load(Ordering::Relaxed);
        assert_eq!(
            miss_after - miss_before,
            1,
            "MISS: op assigned to a DIFFERENT worker than its prefetch targeted, but \
             speculative_prefetch_miss did not increment (the wasted-prewarm case the soak needs \
             to size the gossip window). before={miss_before} after={miss_after}"
        );
        assert_eq!(
            hit_after, hit_before,
            "MISS: speculative_prefetch_hit moved on a rerouted prewarm — swapping the hit/miss \
             increments makes this red-fail. before={hit_before} after={hit_after}"
        );

        // The matcher did NOT route by the recorded target: an IDENTICAL reserve
        // for a DIFFERENT op with NO coalesce record lands on the SAME worker X.
        let op_control = OperationId::default();
        let control_ai = {
            let mut inner = make_base_action_info(make_system_time(1), DigestInfo::new([0x33; 32], 3));
            Arc::make_mut(&mut inner).input_root_digest = input_root;
            ActionInfoWithProps { inner, platform_properties: PlatformProperties::default() }
        };
        let control = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &op_control, &control_ai, false)
            .await;
        let (control_assigned, _, _) =
            control.expect("MISS control: the no-record op must also reserve onto X");
        assert_eq!(
            control_assigned, x,
            "MISS (measurement-only): an identical reserve with NO coalesce record landed on \
             {control_assigned:?}, not X — the assignment must be IDENTICAL with and without the \
             prefetch record. The hit/miss read must READ the record, never route on it (§2.2)."
        );
    }

    Ok(())
}
