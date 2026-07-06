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

use core::time::Duration;
use std::collections::HashMap;
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
// Layer A (config): `SimpleSpec::default()` must have `enable_speculative_prefetch = false`.
//   Mutation: change the default to `true` → T1 fails with bespoke message.
//   (T8 also covers this; T1 is the primary gate-contract test.)
//
// Layer B (scheduling): with gate=OFF, threshold=1, ALL workers saturated PLUS
//   a platform-mismatched idle worker, do_try_match MUST NOT emit PrefetchInputs.
//   The mismatch worker is present so that IF the gate were removed (mutation),
//   the code would reach `inner_find_worker_for_action`, fail on mismatch, and
//   return None → still no PrefetchInputs. Gate-removal is therefore only
//   PARTIALLY catchable via scheduling output (the gate guards the entire block,
//   but when `inner_find_worker_for_action` returns None the end result is identical).
//
// The AUTHORITATIVE gate test is Layer A: the config default.
//
// Bespoke failure messages:
//   Layer A: "T1: enable_speculative_prefetch MUST default to false — gate-OFF regression"
//   Layer B: "T1: PrefetchInputs emitted with gate OFF — MUST NOT emit when feature disabled"
#[nativelink_test]
async fn t1_feature_gate_off_no_prefetch_inputs() -> Result<(), Error> {
    // ── Layer A: config default assertion ──────────────────────────────────────
    let spec = SimpleSpec::default();
    assert!(
        !spec.enable_speculative_prefetch,
        "T1: enable_speculative_prefetch MUST default to false — gate-OFF regression"
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
                DigestInfo::new([1u8; 32], 1), vec![], 60,
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
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
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
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
        .await;
    assert!(first, "T3: first emit for a fresh op must succeed");

    let second = ws
        .send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
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
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
            .await,
        "T4: first emit must succeed"
    );
    // 2) A second emit for the same op is coalesced (proves the entry exists).
    assert!(
        !ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
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
        ws.send_prefetch_inputs(&PlatformProperties::default(), &op_id, input_root, vec![], 60)
            .await,
        "distsys BLOCK-1 (2026-07-05): coalesce guard NOT reaped on worker evict — a rerouted \
         op's stale entry permanently suppressed its re-prefetch to a healthy worker. \
         immediate_evict_worker must pop the coalesce entry for every op the dead worker held."
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
// T8: Config deserialization — enable_speculative_prefetch defaults false
// ─────────────────────────────────────────────────────────────────────────────
//
// Spec: `SimpleSpec::default()` must have `enable_speculative_prefetch = false`
// (feature gate OFF). Deserializing `{}` must produce the same value.
//
// This is the registry's serde contract test. If this fails, the production
// deployment would have the feature accidentally ON.
//
// Bespoke failure message baked into assert_eq!.
#[nativelink_test]
async fn t8_config_defaults_gate_off() -> Result<(), Error> {
    let default_spec = SimpleSpec::default();
    assert_eq!(
        default_spec.enable_speculative_prefetch,
        false,
        "T8: SimpleSpec::default() must have enable_speculative_prefetch=false (gate OFF); \
         found true — production would activate the feature on every deploy"
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
        false,
        "T8: serde-deserialized SimpleSpec{{}} must have enable_speculative_prefetch=false"
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
