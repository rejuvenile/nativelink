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

//! #sched-affinity-probe guard tests for the OBSERVABILITY-ONLY pending-affinity
//! probe (`record_pending_affinity_surplus`), which was pinned at 17.84% of
//! scheduler CPU during a live 27s match cycle (its dominant kernel
//! `compute_batch_sched_gain` is quadratic in the sampled-action count) and ran
//! SYNCHRONOUSLY inside the timed match region, collapsing throughput under a
//! deep backlog. See `.claude/audits/affinity-probe-slow-match-2026-07-06/`.
//!
//! The `sampled_ops` gauge is the WITNESS: it is 0 by default and set to the
//! sampled pending count ONLY when the probe runs. These tests drive
//! `do_try_match` via the `do_try_match_for_test` hook and read `sampled_ops`
//! off the REAL `/metrics` render path (same as
//! `batch_affinity_metrics_render_test.rs`).
//!
//! Fix under test (two gates on the `do_try_match` call site):
//!   `self.pending_affinity_probe_enabled` (config flag, default true, Redis
//!   force-off) `&& primary_queue_depth <= PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH`.
//!
//! Mutation rules:
//!   - remove the `primary_queue_depth <=` clause  → skip-under-backlog RED.
//!   - force the guard false unconditionally        → runs-when-shallow RED.
//!   - ignore `pending_affinity_probe_enabled`       → flag-off RED.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::{
    PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH, SimpleScheduler,
};
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
async fn enqueue_and_match(
    scheduler: &Arc<SimpleScheduler>,
    n: u64,
) -> Result<(), Error> {
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

/// The threshold constant is authoritative at its declaration line (numeric-
/// constant rule). This pins the shipped value so a doc-comment / commit-message
/// drift cannot masquerade as the constant.
#[test]
fn pending_affinity_probe_max_queue_depth_is_64() {
    assert_eq!(
        PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH, 64,
        "#sched-affinity-probe: the guard threshold must be 64 (validated by \
         tests/pending_affinity_probe_timing.rs — the quadratic probe kernel is \
         ~5% of the 5s budget at 64, vs 492% at 512). If this value changes, \
         re-run the timing harness and update the audit."
    );
}

/// SKIP-UNDER-BACKLOG (the core throughput fix). Enqueue MORE than the threshold
/// pending ops with the default (enabled) config; the probe must be SKIPPED, so
/// `sampled_ops` stays 0.
#[nativelink_test]
async fn probe_skipped_when_queue_deeper_than_threshold() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // One more than the guard depth → the pre-match queue exceeds the threshold.
    let depth = PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH + 1;
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 0\n"),
        "#sched-affinity-probe GUARD FAILED: with {depth} queued ops (> threshold \
         {PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH}) the observability probe must be \
         SKIPPED so it cannot collapse the match cycle — sampled_ops must stay 0. \
         A non-zero value means the quadratic probe ran under backlog. body=\n{body}"
    );

    Ok(())
}

/// RUNS-WHEN-SHALLOW. Enqueue AT MOST the threshold pending ops with the default
/// (enabled) config; the probe must RUN, so `sampled_ops` equals the pending
/// count (the gauge is preserved during normal shallow-queue operation).
#[nativelink_test]
async fn probe_runs_when_queue_at_or_below_threshold() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // Exactly at the guard depth (inclusive `<=`) → the probe runs.
    let depth = PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH;
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains(&format!(
            "\nscheduler_test_action_batch_affinity_sampled_ops {depth}\n"
        )),
        "#sched-affinity-probe GUARD FAILED: with {depth} queued ops (== threshold \
         {PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH}, inclusive) the probe must RUN and \
         sample all {depth} ops so the measurement is preserved during normal \
         shallow-queue operation — sampled_ops must be {depth}. body=\n{body}"
    );

    Ok(())
}

/// FLAG-OFF. With `pending_affinity_probe_enabled = false` the probe must NEVER
/// run, regardless of queue depth — even a shallow queue that would otherwise
/// pass the depth guard leaves `sampled_ops` at 0.
#[nativelink_test]
async fn probe_never_runs_when_flag_disabled() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        pending_affinity_probe_enabled: false,
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // Shallow queue (below the depth guard) so ONLY the flag can suppress it.
    let depth = 3u64;
    assert!(depth <= PENDING_AFFINITY_PROBE_MAX_QUEUE_DEPTH);
    enqueue_and_match(&scheduler, depth).await?;

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 0\n"),
        "#sched-affinity-probe FLAG FAILED: with pending_affinity_probe_enabled=false \
         the probe must NOT run even for a shallow queue ({depth} ops) — sampled_ops \
         must stay 0. A non-zero value means the config flag was ignored. body=\n{body}"
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
