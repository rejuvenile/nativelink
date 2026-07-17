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

//! Render test for queue-depth + worker-saturation gauge metrics.
//!
//! These gauges answer the fleet-level question: are actions QUEUING because
//! workers are saturated (→ add workers), or dispatching instantly because
//! workers are idle (→ distribution problem)?
//!
//! The test exercises the REAL metrics path: SimpleScheduler constructed with
//! a MemoryAwaitedActionDb, registered under MetricsRegistry, rendered via
//! render_prometheus — the same path the production /metrics listener uses.
//!
//! Metric paths under the two registration prefixes used in nativelink.rs:
//!
//!   Action scheduler ("scheduler.{name}.action"):
//!     sorted_action_infos.queued_count   — actions in Queued state
//!     sorted_action_infos.executing_count — actions in Executing state
//!
//!   Worker scheduler ("scheduler.{name}.worker"):
//!     scheduler_metrics.workers_total        — total connected workers
//!     scheduler_metrics.workers_at_capacity  — workers that cannot accept more actions
//!     scheduler_metrics.total_running_actions — in-flight actions across fleet
//!
//! Mutation rule: comment out any of the five scalar publishes. The test
//! must red-fail with the bespoke "#schedmetric" message naming the missing gauge.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

const INSTANCE_NAME: &str = "test_instance";
const NOW_TIME: u64 = 10_000;

fn make_digest(b: u8) -> DigestInfo {
    DigestInfo::new([b; 32], 0)
}

fn make_action_info(digest: DigestInfo, ts_offset: u64) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH
            .checked_add(Duration::from_secs(NOW_TIME + ts_offset))
            .unwrap(),
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest,
        }),
        targetkey: None,
    })
}

fn make_and_register(
    scheduler: Arc<SimpleScheduler>,
    worker_scheduler: Arc<dyn WorkerScheduler>,
) -> MetricsRegistry {
    let registry = MetricsRegistry::new();
    // Register exactly as nativelink.rs does:
    //   action scheduler → "scheduler.{name}.action"
    //   worker scheduler → "scheduler.{name}.worker"
    registry.register("scheduler.test.action", scheduler);
    // Upcast via coercion: WorkerScheduler: RootMetricsComponent: MetricsComponent.
    let ws_component: Arc<dyn nativelink_util::metrics_publisher::MetricsComponentTrait + Send + Sync> =
        worker_scheduler;
    registry.register_dyn("scheduler.test.worker".to_string(), ws_component);
    registry
}

/// #schedmetric: queue-depth + worker-saturation gauges appear on /metrics
/// with correct values after N actions queued and M workers connected.
///
/// Scenario:
///   - 1 worker registered with max_inflight_tasks=1 (capacity=1).
///   - 2 actions added; the matcher dispatches 1 to the worker, 1 stays queued.
///   - Worker is now at capacity (running=1, max=1).
///
/// Uses `new_with_callback(|| async {})` so the matching loop yields via
/// `yield_now()` between cycles (no sleep(Duration::ZERO) timer dependency).
/// This is the same pattern used by `basic_add_action_with_one_worker_test`.
///
/// Expected gauges:
///   queued_count            = 1
///   executing_count         = 1
///   workers_total           = 1
///   workers_at_capacity     = 1
///   total_running_actions   = 1
#[nativelink_test]
async fn schedmetric_queue_depth_and_worker_saturation_render_prometheus() -> Result<(), Error> {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db = memory_awaited_action_db_factory(
        0,
        &task_notify.clone(),
        MockInstantWrapped::default,
    );
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    // Use new_with_callback with a no-op inter-cycle callback so the
    // matching loop cooperates with yield_now() rather than sleeping via
    // the timer. This is the same approach used by simple_scheduler_test.rs.
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &spec,
        awaited_action_db,
        || async move {},
        task_notify.clone(),
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Register worker first (max_inflight_tasks=1 → capacity=1).
    let (tx1, mut rx1) = mpsc::unbounded_channel::<UpdateForWorker>();
    let worker1 = Worker::new(
        WorkerId("w1".to_string()),
        PlatformProperties::default(),
        tx1,
        NOW_TIME,
        1, // max_inflight_tasks = 1 → saturated after 1 running action
    );
    worker_scheduler
        .add_worker(worker1)
        .await
        .expect("add worker1 must succeed");
    tokio::task::yield_now().await; // allow matcher to process new worker
    // Drain ConnectionResult (sync-sent during add_worker).
    let conn1 = rx1.recv().await.expect("worker1 ConnectionResult must arrive");
    assert!(
        matches!(
            conn1.update,
            Some(update_for_worker::Update::ConnectionResult(ConnectionResult { .. }))
        ),
        "#schedmetric setup: worker1 initial message must be ConnectionResult"
    );

    // Add 2 actions with no workers busy yet → both land in Queued state.
    // The matcher will dispatch exactly 1 (worker capacity=1), leaving 1 queued.
    scheduler
        .add_action(OperationId::default(), make_action_info(make_digest(1), 0))
        .await
        .expect("add_action 1 must succeed");
    tokio::task::yield_now().await; // let matcher dispatch action 1
    scheduler
        .add_action(OperationId::default(), make_action_info(make_digest(2), 1))
        .await
        .expect("add_action 2 must succeed");
    tokio::task::yield_now().await; // let matcher try action 2 (worker full → stays queued)

    // Consume the StartAction for action 1. This confirms dispatch happened
    // and that exactly 1 action is executing.  The recv() suspends the test
    // task if the StartAction is not yet in the channel, giving the matcher
    // background task time to run.
    let start1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
        .await
        .expect(
            "#schedmetric setup: timed out waiting for worker1 StartAction — \
             matching engine did not dispatch within 5 s",
        )
        .expect(
            "#schedmetric setup: worker1 channel closed before StartAction arrived",
        );
    assert!(
        matches!(start1.update, Some(update_for_worker::Update::StartAction(_))),
        "#schedmetric setup: worker1 must receive StartAction, got: {:?}",
        start1.update
    );

    // Register & render.
    let registry = make_and_register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    // ── Queue-depth gauges ──────────────────────────────────────────────────
    //
    // Published by SortedAwaitedActions::publish under the group path:
    //   scheduler.test.action
    //     .matching_engine_state_manager   (SimpleScheduler field group)
    //     .action_db                        (SimpleSchedulerStateManager field group)
    //     .sorted_action_infos              (AwaitedActionDbImpl field group)
    //     .queued_count                     (scalar published by custom MetricsComponent)
    //
    // Prometheus name (dots → underscores, _counter suffix from MetricKind::Counter):
    //   scheduler_test_action_matching_engine_state_manager_action_db_sorted_action_infos_queued_count
    //
    // After 1 dispatch and worker at capacity: 1 queued, 1 executing.
    assert!(
        body.contains(
            "\nscheduler_test_action_matching_engine_state_manager_action_db_sorted_action_infos_queued_count 1\n"
        ),
        "#schedmetric MISSING or WRONG VALUE: `queued_count` must be 1 \
         (2 actions added, 1 dispatched to worker, 1 still awaiting assignment \
         because worker is now at max capacity). \
         This is the distribution-vs-capacity diagnostic: if workers are idle but \
         queued_count > 0, the problem is scheduling distribution, not capacity. \
         body=\n{body}"
    );
    assert!(
        body.contains(
            "\nscheduler_test_action_matching_engine_state_manager_action_db_sorted_action_infos_executing_count 1\n"
        ),
        "#schedmetric MISSING or WRONG VALUE: `executing_count` must be 1 \
         (exactly 1 action dispatched to worker1). \
         body=\n{body}"
    );

    // ── Worker-saturation gauges ────────────────────────────────────────────
    //
    // Published by ApiWorkerScheduler via SchedulerMetrics fields under:
    //   scheduler.test.worker
    //     .scheduler_metrics              (#[metric(group = "scheduler_metrics")])
    //     .workers_total                  (AtomicU64 field)
    //     .workers_at_capacity            (AtomicU64 field)
    //     .total_running_actions          (AtomicU64 field)
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_workers_total 1\n"),
        "#schedmetric MISSING or WRONG VALUE: `workers_total` must be 1 \
         (only worker1 registered). \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_workers_at_capacity 1\n"),
        "#schedmetric MISSING or WRONG VALUE: `workers_at_capacity` must be 1 \
         (worker1 has max_inflight_tasks=1 and is running 1 action → \
         can_accept_work() returns false). \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_total_running_actions 1\n"),
        "#schedmetric MISSING or WRONG VALUE: `total_running_actions` must be 1 \
         (one action running on worker1). \
         body=\n{body}"
    );

    Ok(())
}

/// #schedmetric: baseline (no actions, no workers) — all five gauges are 0.
/// Ensures the metrics are PRESENT (not silently absent) even when the
/// scheduler is idle, so Prometheus scrapes yield a complete time series
/// rather than gaps that make dashboards look like "metric not found".
#[nativelink_test]
async fn schedmetric_all_gauges_zero_when_idle() -> Result<(), Error> {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db = memory_awaited_action_db_factory(
        0,
        &task_notify.clone(),
        MockInstantWrapped::default,
    );
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler) = SimpleScheduler::new(
        &spec,
        awaited_action_db,
        task_notify.clone(),
        None,
    );

    let registry = make_and_register(scheduler, worker_scheduler);
    let body = render_prometheus(&registry);

    assert!(
        body.contains(
            "\nscheduler_test_action_matching_engine_state_manager_action_db_sorted_action_infos_queued_count 0\n"
        ),
        "#schedmetric idle: `queued_count` must emit 0 even when no actions exist \
         (absence would cause Prometheus to report `no data` on idle scheduler, \
         masking whether the metric is misconfigured vs truly idle). \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_workers_total 0\n"),
        "#schedmetric idle: `workers_total` must emit 0 even when no workers registered. \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_total_running_actions 0\n"),
        "#schedmetric idle: `total_running_actions` must emit 0 when idle. \
         body=\n{body}"
    );

    Ok(())
}

/// #schedmetric: draining a worker updates `workers_at_capacity` immediately.
///
/// `set_drain_worker` sets `is_draining`, which is a direct input to
/// `can_accept_work()`. Drain is precisely when an operator watches the
/// saturation gauge (rolling deploy / fleet drain). The gauge must recompute at
/// drain time, not lag until the next unrelated pool mutation.
///
/// Scenario:
///   - 1 idle worker (unlimited capacity, no actions) → at_capacity=0.
///   - Drain the worker → can_accept_work()=false → at_capacity=1, with NO
///     intervening add/remove/dispatch mutation to mask a missing recompute.
#[nativelink_test]
async fn schedmetric_drain_updates_workers_at_capacity_immediately() -> Result<(), Error> {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db = memory_awaited_action_db_factory(
        0,
        &task_notify.clone(),
        MockInstantWrapped::default,
    );
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler) = SimpleScheduler::new(
        &spec,
        awaited_action_db,
        task_notify.clone(),
        None,
    );

    // One idle worker, unlimited capacity, no actions → can_accept_work()=true.
    let (tx1, mut rx1) = mpsc::unbounded_channel::<UpdateForWorker>();
    let worker_id = WorkerId("w1".to_string());
    let worker1 = Worker::new(
        worker_id.clone(),
        PlatformProperties::default(),
        tx1,
        NOW_TIME,
        0, // unlimited
    );
    worker_scheduler
        .add_worker(worker1)
        .await
        .expect("add worker1 must succeed");
    let conn1 = rx1.recv().await.expect("worker1 ConnectionResult must arrive");
    assert!(
        matches!(
            conn1.update,
            Some(update_for_worker::Update::ConnectionResult(ConnectionResult { .. }))
        ),
        "#schedmetric drain setup: worker1 initial message must be ConnectionResult"
    );

    // Before drain: idle worker is NOT at capacity.
    {
        let registry = make_and_register(scheduler.clone(), worker_scheduler.clone());
        let body = render_prometheus(&registry);
        assert!(
            body.contains("\nscheduler_test_worker_scheduler_metrics_workers_at_capacity 0\n"),
            "#schedmetric drain: pre-drain `workers_at_capacity` must be 0 \
             (idle unlimited-capacity worker can accept work). \
             body=\n{body}"
        );
    }

    // Drain the worker — this is the ONLY mutation between the two renders, so
    // it MUST be what flips workers_at_capacity to 1.
    worker_scheduler
        .set_drain_worker(&worker_id, true)
        .await
        .expect("set_drain_worker must succeed");

    let registry = make_and_register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_workers_at_capacity 1\n"),
        "#schedmetric drain: `workers_at_capacity` must be 1 immediately after \
         set_drain_worker (is_draining → can_accept_work()=false). If this is 0, \
         set_drain_worker did not call recompute_capacity_gauges() and the gauge \
         is stale during exactly the fleet-drain window an operator watches it. \
         body=\n{body}"
    );
    // workers_total is unchanged (worker still registered, just draining).
    assert!(
        body.contains("\nscheduler_test_worker_scheduler_metrics_workers_total 1\n"),
        "#schedmetric drain: `workers_total` must remain 1 (draining ≠ removed). \
         body=\n{body}"
    );

    Ok(())
}

/// (#task-resource-profile Phase-3 §4) RENDER TEST — pins the LITERAL Phase-3
/// observe-metric names on the rendered `/metrics` surface, at 0, so the
/// dark-counter trap cannot hide them: an absent name (dropped `#[metric]`, a
/// typo, or a rename that misses the emit) would fail here. Covers BOTH the NEW
/// `down_opportunity_*` DOWN-headroom counters (§4) AND the RELABELED
/// `predicted_tail_over_actual_*` counters (cadre C4 — formerly the misread
/// `accuracy_over_ratio_*`), so a future rename must update the pinned name here.
///
/// MUTATION: rename any of the pinned `#[metric]` fields (e.g.
/// `down_opportunity_sum_x100`) without updating this test → the corresponding
/// `body.contains(...)` red-fails with its bespoke message.
#[nativelink_test]
async fn schedmetric_phase3_observe_metric_names_render() -> Result<(), Error> {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db = memory_awaited_action_db_factory(
        0,
        &task_notify.clone(),
        MockInstantWrapped::default,
    );
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler) = SimpleScheduler::new(
        &spec,
        awaited_action_db,
        task_notify.clone(),
        None,
    );

    let registry = make_and_register(scheduler, worker_scheduler);
    let body = render_prometheus(&registry);

    // ── §4 DOWN-opportunity counters (declared/p50 headroom) ──
    for name in [
        "down_opportunity_sum_x100",
        "down_opportunity_max_x100",
        "down_opportunity_samples",
        "down_opportunity_fine_samples",
        "down_opportunity_coarse_samples",
    ] {
        assert!(
            body.contains(&format!(
                "\nscheduler_test_worker_scheduler_metrics_{name} 0\n"
            )),
            "#phase3-observe: the §4 DOWN-opportunity metric `{name}` must render at 0 on \
             an idle scheduler (dark-counter trap: an absent name would make the DOWN \
             headroom invisible). body=\n{body}"
        );
    }

    // ── C4 relabel: predicted_tail_over_actual_* (formerly accuracy_over_ratio_*) ──
    for name in [
        "predicted_tail_over_actual_max_x100",
        "predicted_tail_over_actual_sum_x100",
        "predicted_tail_over_actual_samples",
    ] {
        assert!(
            body.contains(&format!(
                "\nscheduler_test_worker_scheduler_metrics_{name} 0\n"
            )),
            "#phase3-observe: the RELABELED metric `{name}` (cadre C4 — the old \
             `accuracy_over_ratio_*` misread declared/actual) must render at 0; a stale \
             `accuracy_over_ratio_*` name here means the relabel was incomplete. body=\n{body}"
        );
    }
    // The OLD mislabeled names must be GONE (the relabel must be complete).
    assert!(
        !body.contains("accuracy_over_ratio_max_x100")
            && !body.contains("accuracy_over_samples"),
        "#phase3-observe: the old mislabeled `accuracy_over_*` metric names must NOT render \
         after the C4 relabel. body=\n{body}"
    );

    Ok(())
}
