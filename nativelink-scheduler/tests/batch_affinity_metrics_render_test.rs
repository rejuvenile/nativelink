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

//! Render test for the OBSERVABILITY-ONLY batch-affinity probe gauges/counter
//! (`#batch-affinity`).
//!
//! Exercises the REAL metrics path: a `SimpleScheduler` built with a
//! `MemoryAwaitedActionDb`, actions added, `do_try_match` run with NO workers
//! (so every op stays queued and the pending-set surplus is measurable),
//! rendered via `render_prometheus` — the same path the production `/metrics`
//! listener uses. Metric paths under the action-scheduler prefix
//! (`scheduler.{name}.action`, see `src/bin/nativelink.rs:591`):
//!
//!   scheduler.test.action.batch_affinity.colocation_surplus
//!   scheduler.test.action.batch_affinity.max_group
//!   scheduler.test.action.batch_affinity.sampled_ops
//!   scheduler.test.action.batch_affinity.arrival_within_250ms_total
//!
//! The dim-B counter is driven by the scheduler's INJECTABLE clock
//! (`MockInstantWrapped` → thread-local `MockClock`), so this file advances the
//! mock clock to place arrivals inside / outside the 250ms window
//! DETERMINISTICALLY — no wall-clock dependence (FIX 1: was previously
//! real-clock-flaky under a loaded build box).
//!
//! Mutation rule: comment out either compute site
//! (`record_pending_affinity_surplus` for A, the `record_arrival` +
//! `fetch_add` block in `inner_add_action` for B). The relevant test must
//! red-fail with its bespoke "#batch-affinity" message.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use mock_instant::thread_local::MockClock;
use nativelink_config::schedulers::{
    ExperimentalRedisSchedulerBackend, ExperimentalSimpleSchedulerBackend, SimpleSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::{MAX_PENDING_AFFINITY_SAMPLE, SimpleScheduler};
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

/// Build an `ActionInfo` with an explicit `input_root_digest` (the affinity
/// key). `action_disc` differentiates the ACTION digest so distinct ops with
/// the SAME input root are distinct operations (not deduped/joined), while
/// `input_root_disc` sets the co-location key.
fn make_action_info(input_root: DigestInfo, action_disc: u8, ts_offset: u64) -> Arc<ActionInfo> {
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
            digest: DigestInfo::new([action_disc; 32], 0),
        }),
    })
}

/// Single-byte-discriminated input root (readable [A,A,B,C] fixtures).
fn root(disc: u8) -> DigestInfo {
    DigestInfo::new([disc; 32], 1)
}

/// Build a scheduler over the given `SimpleSpec` with a memory awaited-action DB
/// and the mock clock. The spec's `experimental_backend` decides the dim-A gate.
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

fn new_scheduler() -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>, Arc<Notify>) {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    new_scheduler_with_spec(&spec)
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

/// (A) After adding 4 pending ops with input roots [A, A, B, C] and running one
/// match cycle with NO workers (all stay queued), the pending co-location
/// surplus gauge must be 1 and max_group 2.
#[nativelink_test]
async fn batch_affinity_colocation_surplus_render() -> Result<(), Error> {
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    // Roots: A, A, B, C → 4 ops, 3 distinct roots → surplus 1, max_group 2.
    // Distinct ACTION digests (1,2,3,4) so the ops are 4 separate operations.
    for (input_root, action, off) in [(b'A', 1u8, 0u64), (b'A', 2, 1), (b'B', 3, 2), (b'C', 4, 3)] {
        scheduler
            .add_action(OperationId::default(), make_action_info(root(input_root), action, off))
            .await
            .expect("#batch-affinity setup: add_action must succeed");
    }

    // Run one match cycle. With no workers registered, every op stays queued;
    // the dimension-A probe samples the pending set and stores the gauges.
    scheduler
        .do_try_match_for_test()
        .await
        .expect("#batch-affinity setup: do_try_match must succeed with no workers");

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_colocation_surplus 1\n"),
        "#batch-affinity MISSING or WRONG VALUE: colocation_surplus must be 1 \
         (4 pending ops [A,A,B,C], 3 distinct input roots → surplus 1). \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_max_group 2\n"),
        "#batch-affinity MISSING or WRONG VALUE: max_group must be 2 \
         (largest same-input-root pending group is the two A's). \
         body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 4\n"),
        "#batch-affinity MISSING or WRONG VALUE: sampled_ops must be 4 \
         (all 4 pending ops sampled, below the 512 cap). \
         body=\n{body}"
    );

    Ok(())
}

/// (A) All-distinct pending roots → surplus 0, max_group 1. Ensures the gauge
/// is present (not absent) at its floor, so dashboards don't read a gap.
#[nativelink_test]
async fn batch_affinity_all_distinct_surplus_zero_render() -> Result<(), Error> {
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    for (input_root, action, off) in [(b'A', 1u8, 0u64), (b'B', 2, 1), (b'C', 3, 2)] {
        scheduler
            .add_action(OperationId::default(), make_action_info(root(input_root), action, off))
            .await
            .expect("#batch-affinity setup: add_action must succeed");
    }
    scheduler
        .do_try_match_for_test()
        .await
        .expect("#batch-affinity setup: do_try_match must succeed");

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_colocation_surplus 0\n"),
        "#batch-affinity MISSING or WRONG VALUE: colocation_surplus must be 0 \
         for all-distinct pending roots. body=\n{body}"
    );
    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_max_group 1\n"),
        "#batch-affinity MISSING or WRONG VALUE: max_group must be 1 \
         for all-distinct pending roots. body=\n{body}"
    );

    Ok(())
}

/// (FIX 4 / F3) When the pending set exceeds `MAX_PENDING_AFFINITY_SAMPLE`, the
/// dim-A pass samples only the cap and `sampled_ops` SATURATES at the cap — so
/// operators can see the surplus is a lower bound. Adds `cap + 8` all-DISTINCT
/// pending ops → sampled_ops == 512, and (because the sampled prefix is all
/// distinct) surplus 0 / max_group 1.
#[nativelink_test]
async fn batch_affinity_sampled_ops_saturates_at_cap_render() -> Result<(), Error> {
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    let total = MAX_PENDING_AFFINITY_SAMPLE + 8;
    for i in 0..total {
        // Distinct input root AND distinct action per op.
        let mut root_hash = [0u8; 32];
        root_hash[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        let mut action_hash = [0u8; 32];
        action_hash[0..8].copy_from_slice(&(0xFFFF_0000u64 + i as u64).to_le_bytes());
        let action_info = Arc::new(ActionInfo {
            command_digest: DigestInfo::new([0u8; 32], 0),
            input_root_digest: DigestInfo::new(root_hash, 1),
            timeout: Duration::MAX,
            platform_properties: HashMap::new(),
            priority: 0,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH
                .checked_add(Duration::from_secs(NOW_TIME + i as u64))
                .unwrap(),
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                instance_name: INSTANCE_NAME.to_string(),
                digest_function: DigestHasherFunc::Sha256,
                digest: DigestInfo::new(action_hash, 0),
            }),
        });
        scheduler
            .add_action(OperationId::default(), action_info)
            .await
            .expect("#batch-affinity setup: add_action must succeed");
    }

    scheduler
        .do_try_match_for_test()
        .await
        .expect("#batch-affinity setup: do_try_match must succeed");

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains(&format!(
            "\nscheduler_test_action_batch_affinity_sampled_ops {MAX_PENDING_AFFINITY_SAMPLE}\n"
        )),
        "#batch-affinity MISSING or WRONG VALUE: sampled_ops must SATURATE at the sample cap \
         {MAX_PENDING_AFFINITY_SAMPLE} when the pending set ({total}) exceeds it — the surplus \
         gauge is then a lower bound and operators must be able to see the saturation. body=\n{body}"
    );

    Ok(())
}

/// (FIX 2) On the Redis/store backend the dim-A pass is GATED OFF (each
/// `as_action_info()` would be a store round-trip). Verify a scheduler built
/// with a Redis `experimental_backend` does NOT run the surplus pass:
/// `sampled_ops` stays at its default 0 even after adding co-located pending
/// ops and running a match cycle. (The awaited-action DB is still the in-memory
/// fake — the gate keys on the SPEC, which is what the factory has.)
#[nativelink_test]
async fn batch_affinity_dim_a_gated_off_on_redis_backend() -> Result<(), Error> {
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        experimental_backend: Some(ExperimentalSimpleSchedulerBackend::Redis(
            ExperimentalRedisSchedulerBackend {
                redis_store: "unused_in_test".to_string(),
            },
        )),
        ..Default::default()
    };
    let (scheduler, worker_scheduler, _notify) = new_scheduler_with_spec(&spec);

    // Add co-located pending ops that WOULD produce surplus 1 if the pass ran.
    for (input_root, action, off) in [(b'A', 1u8, 0u64), (b'A', 2, 1), (b'B', 3, 2)] {
        scheduler
            .add_action(OperationId::default(), make_action_info(root(input_root), action, off))
            .await
            .expect("#batch-affinity setup: add_action must succeed");
    }
    scheduler
        .do_try_match_for_test()
        .await
        .expect("#batch-affinity setup: do_try_match must succeed");

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    // The gauge is still PRESENT (BatchAffinityMetrics is always published) but
    // never written → stays at default 0, proving the dim-A pass did not run.
    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_sampled_ops 0\n"),
        "#batch-affinity GATE FAILED: on the Redis backend the dim-A pass must be skipped, \
         so sampled_ops must stay 0 even with 3 co-located pending ops. A non-zero value means \
         the store-round-trip pass ran on the Redis critical path. body=\n{body}"
    );
    // Corroborate: surplus also stays 0 (would be 1 if the pass had run).
    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_colocation_surplus 0\n"),
        "#batch-affinity GATE FAILED: colocation_surplus must stay 0 on the gated Redis backend. \
         body=\n{body}"
    );

    Ok(())
}

/// (B, FIX 1) Deterministic mock-clock arrival-window test. Two same-root
/// arrivals placed 100ms apart (via `MockClock::advance`) fall INSIDE the 250ms
/// window → counter 1. A third same-root arrival placed 300ms later falls
/// OUTSIDE → counter stays 1. Fully deterministic: no wall-clock dependence.
#[nativelink_test]
async fn batch_affinity_arrival_window_counter_render() -> Result<(), Error> {
    // Anchor the mock clock at a fixed base so arrivals are deterministic.
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let (scheduler, worker_scheduler, _notify) = new_scheduler();

    // First arrival of root A at t=0: no peer → counter 0.
    scheduler
        .add_action(OperationId::default(), make_action_info(root(b'A'), 1, 0))
        .await
        .expect("#batch-affinity setup: add_action A1 must succeed");

    // Advance 100ms (< 250ms window) and add root A again → within window → +1.
    MockClock::advance(Duration::from_millis(100));
    scheduler
        .add_action(OperationId::default(), make_action_info(root(b'A'), 2, 1))
        .await
        .expect("#batch-affinity setup: add_action A2 must succeed");

    // Advance 300ms (> 250ms window) and add root A a third time → stale peer →
    // NO increment. Proves the window boundary is honored via the mock clock.
    MockClock::advance(Duration::from_millis(300));
    scheduler
        .add_action(OperationId::default(), make_action_info(root(b'A'), 3, 2))
        .await
        .expect("#batch-affinity setup: add_action A3 must succeed");

    let registry = register(scheduler.clone(), worker_scheduler.clone());
    let body = render_prometheus(&registry);

    assert!(
        body.contains("\nscheduler_test_action_batch_affinity_arrival_within_250ms_total 1\n"),
        "#batch-affinity MISSING or WRONG VALUE: arrival_within_250ms_total must be exactly 1 \
         (A@0 and A@100ms are within the 250ms window → +1; A@400ms is 300ms after its peer → \
         outside the window → no increment). Driven by the mock clock, so this is deterministic. \
         body=\n{body}"
    );

    Ok(())
}
