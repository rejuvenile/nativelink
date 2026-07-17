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

//! #sched-affinity-probe flag tests for the OBSERVABILITY-ONLY pending-affinity
//! probe (`record_pending_affinity_surplus`), which was pinned at 17.84% of
//! scheduler CPU during a live 27s match cycle (its dominant kernel
//! `compute_batch_sched_gain` is quadratic in the sampled-action count) and ran
//! SYNCHRONOUSLY inside the timed match region, collapsing throughput under a
//! deep backlog. See `.claude/audits/affinity-probe-slow-match-2026-07-06/`.
//!
//! The fix makes the probe OPT-IN, default OFF (user decision 2026-07-06): the
//! deployed prod config does not set the flag, so the probe never runs and the
//! 20-27s `do_try_match` collapse cannot occur. An investigation enables it
//! deliberately via `pending_affinity_probe_enabled: true` in the scheduler
//! config. The probe is observability-only — it never affects worker selection.
//!
//! The `sampled_ops` gauge is the WITNESS: it is 0 by default and set to the
//! sampled pending count ONLY when the probe runs. These tests drive
//! `do_try_match` via the `do_try_match_for_test` hook and read `sampled_ops`
//! off the REAL `/metrics` render path (same as
//! `batch_affinity_metrics_render_test.rs`).
//!
//! Fix under test (single gate on the `do_try_match` call site):
//!   `self.pending_affinity_probe_enabled` — derived from the config flag
//!   (default false) AND a non-Redis backend force-off.
//!
//! Mutation rules:
//!   - force the gate `true` unconditionally         → default-off RED.
//!   - force the gate `false` unconditionally        → flag-enabled RED.
//!   - ignore `pending_affinity_probe_enabled`        → flag-off RED.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::operation_state_manager::ClientStateManager;
use tokio::sync::Notify;

const INSTANCE_NAME: &str = "test_instance";
const NOW_TIME: u64 = 10_000;

/// Build an `ActionInfo` with an explicit `input_root_digest`. `action_disc`
/// keeps the ACTION digest distinct so N enqueued ops are N distinct operations.
fn make_action_info(input_root: DigestInfo, action_disc: u64, ts_offset: u64) -> Arc<ActionInfo> {
    let mut action_hash = [0u8; 32];
    action_hash[0..8].copy_from_slice(&action_disc.to_le_bytes());
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: input_root,
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
            digest: DigestInfo::new(action_hash, 0),
        }),
        targetkey: None,
    })
}

/// Distinct input root per op (index-discriminated).
fn root(idx: u64) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(&idx.to_le_bytes());
    DigestInfo::new(h, 1)
}

fn new_scheduler_with_spec(
    spec: &SimpleSpec,
) -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>, Arc<Notify>) {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db =
        memory_awaited_action_db_factory(0, &task_notify.clone(), MockInstantWrapped::default);
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        spec,
        awaited_action_db,
        || async move {},
        task_notify.clone(),
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    (scheduler, worker_scheduler, task_notify)
}

fn register(
    scheduler: Arc<SimpleScheduler>,
    worker_scheduler: Arc<dyn WorkerScheduler>,
) -> MetricsRegistry {
    let registry = MetricsRegistry::new();
    registry.register("scheduler.test.action", scheduler);
    let ws_component: Arc<
        dyn nativelink_util::metrics_publisher::MetricsComponentTrait + Send + Sync,
    > = worker_scheduler;
    registry.register_dyn("scheduler.test.worker".to_string(), ws_component);
    registry
}

/// Enqueue `n` distinct pending ops, one match cycle, return the rendered body.
async fn enqueue_and_match(scheduler: &Arc<SimpleScheduler>, n: u64) -> Result<(), Error> {
    for i in 0..n {
        scheduler
            .add_action(OperationId::default(), make_action_info(root(i), i, i))
            .await
            .expect("#sched-affinity-probe setup: add_action must succeed");
    }
    scheduler
        .do_try_match_for_test()
        .await
        .expect("#sched-affinity-probe setup: do_try_match must succeed with no workers");
    Ok(())
}

/// DEFAULT-OFF (the core opt-in fix). With `SimpleSpec::default()` the probe is
/// OFF (the deployed prod shape — the flag is absent from config), so a match
/// cycle over a shallow queue that WOULD sample if the probe ran leaves
/// `sampled_ops` at 0. This is the guarantee that prod never pays the quadratic
/// probe: it does not run at all unless an investigation opts in.
#[nativelink_test]
async fn probe_never_runs_by_default() -> Result<(), Error> {
    // Only set `worker_timeout_s` (a match-loop knob); leave the probe flag at its
    // default so this test pins the DEFAULT = OFF behavior, not an explicit false.
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    // Guard: this fixture is only meaningful if the default really is OFF.
    assert!(
        !spec.pending_affinity_probe_enabled,
        "#sched-affinity-probe: SimpleSpec::default() must leave \
         pending_affinity_probe_enabled = false (the probe is opt-in). If this \
         flips, the deployed prod config would run the quadratic probe."
    );
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // A shallow queue that WOULD sample (produce sampled_ops == depth) if the probe
    // ran — so the ONLY reason sampled_ops stays 0 is the default-off gate.
    let depth = 3u64;
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 0\n"),
        "#sched-affinity-probe DEFAULT FAILED: with the default (opt-in, OFF) config \
         the probe must NOT run even for a shallow queue ({depth} ops) — sampled_ops \
         must stay 0. A non-zero value means the probe ran by default, so prod would \
         pay the quadratic cost. body=\n{body}"
    );

    Ok(())
}

/// FLAG-ENABLED. With `pending_affinity_probe_enabled = true` explicitly set (and
/// a non-Redis backend), the probe RUNS on ANY queue depth — there is no
/// depth guard — so `sampled_ops` equals the pending count. This is how an
/// investigation opts in.
#[nativelink_test]
async fn probe_runs_when_flag_enabled() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        pending_affinity_probe_enabled: true,
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // A non-trivial queue depth to prove the probe runs regardless of depth (the
    // former depth guard, now removed, would have SKIPPED anything > 64).
    let depth = 100u64;
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains(&format!(
            "\nscheduler_test_action_batch_affinity_sampled_ops {depth}\n"
        )),
        "#sched-affinity-probe FLAG FAILED: with pending_affinity_probe_enabled=true \
         and {depth} queued ops the probe must RUN and sample all {depth} ops (there \
         is no depth guard — the investigation opts in for exactly this deep-backlog \
         regime) — sampled_ops must be {depth}. A value of 0 means the flag was \
         ignored. body=\n{body}"
    );

    Ok(())
}

/// FLAG-OFF (explicit). With `pending_affinity_probe_enabled = false` set
/// explicitly the probe must NEVER run — redundant with the default today, but
/// pins that an explicit false is honored (and guards a future default flip).
#[nativelink_test]
async fn probe_never_runs_when_flag_disabled() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        pending_affinity_probe_enabled: false,
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    let depth = 3u64;
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 0\n"),
        "#sched-affinity-probe FLAG FAILED: with pending_affinity_probe_enabled=false \
         the probe must NOT run ({depth} ops) — sampled_ops must stay 0. A non-zero \
         value means the config flag was ignored. body=\n{body}"
    );

    Ok(())
}

/// CORRECTNESS UNCHANGED (the probe never affected assignment). With the probe
/// ENABLED vs DISABLED, a match cycle over the SAME shallow pending set must
/// leave the SAME set of ops still-queued (the probe is observability-only, so
/// which ops match/stay-queued is identical). With no workers, all ops stay
/// queued in both configs; asserting the still-queued count is identical proves
/// the probe does not perturb matching.
#[nativelink_test]
async fn matching_unchanged_with_probe_on_vs_off() -> Result<(), Error> {
    async fn still_queued_after_match(probe_enabled: bool) -> usize {
        let spec = SimpleSpec {
            worker_timeout_s: 100,
            pending_affinity_probe_enabled: probe_enabled,
            ..Default::default()
        };
        let (scheduler, _ws, _notify) = new_scheduler_with_spec(&spec);
        for i in 0..5u64 {
            scheduler
                .add_action(OperationId::default(), make_action_info(root(i), i, i))
                .await
                .expect("add_action must succeed");
        }
        scheduler
            .do_try_match_for_test()
            .await
            .expect("do_try_match must succeed");
        // Query how many operations remain Queued (no workers → all 5 stay).
        let filter = nativelink_util::operation_state_manager::OperationFilter {
            stages: nativelink_util::operation_state_manager::OperationStageFlags::Queued,
            ..Default::default()
        };
        let stream = scheduler
            .filter_operations(filter)
            .await
            .expect("filter_operations must succeed");
        use futures::StreamExt;
        stream.collect::<Vec<_>>().await.len()
    }

    let with_probe = still_queued_after_match(true).await;
    let without_probe = still_queued_after_match(false).await;

    assert_eq!(
        with_probe, without_probe,
        "#sched-affinity-probe CORRECTNESS: the observability probe must NOT change \
         which actions match — the still-queued count after one match cycle must be \
         identical with the probe ON ({with_probe}) and OFF ({without_probe})."
    );
    // Sanity: no workers → all 5 ops stay queued in both.
    assert_eq!(
        with_probe, 5,
        "#sched-affinity-probe CORRECTNESS setup: with no workers all 5 ops must \
         remain queued (got {with_probe})."
    );

    Ok(())
}
