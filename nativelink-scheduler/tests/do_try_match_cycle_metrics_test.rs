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

//! (#matchcycle) Render + behavior tests for the matcher-cost
//! `do_try_match` cycle telemetry on `SchedulerMetrics`.
//!
//! The counters answer "is the matcher ever the bottleneck under burst"
//! from a scrape: per-cycle count, cycle/query duration sum+max, a fixed
//! 6-band cycle-duration histogram, and the per-cycle matched-action
//! sum+max. Before this, the only signal was the >5s `Slow do_try_match
//! cycle` WARN (not scrapeable) and `match_latency_ms` (conflates
//! queue+match).
//!
//! Dark-counter trap coverage: `matchcycle_render_pins_all_metric_names`
//! exercises the REAL metrics path — `SimpleScheduler` constructed with a
//! `MemoryAwaitedActionDb`, the worker scheduler registered under
//! `MetricsRegistry` EXACTLY as `nativelink.rs` registers it
//! (`scheduler.{name}.worker` via `register_dyn`), rendered via
//! `render_prometheus` — the same walk the production `/metrics` listener
//! uses (verified live 2026-08-04: `scheduler_MAIN_SCHEDULER_worker_
//! scheduler_metrics_*` renders on buildcache:50061). A name that fails the
//! literal-string pin here would be a silently-dark or doubled-name
//! counter in production; the rendered-VALUE assertion additionally
//! proves the REGISTERED instance is the RECORDING instance (the
//! second-instance dark-counter trap).
//!
//! Band-edge coverage: `matchcycle_band_edges_bucket_uniquely` drives
//! `record_do_try_match_cycle` directly with a UNIQUE sample count per
//! band (1,2,3,4,5,7) probing every boundary value, so ANY band-boundary
//! swap or off-by-one relabels a band to the wrong count and fails
//! exactly (the #calib band-swap lesson). A trailing DESCENDING sample
//! separates `fetch_max` high-water semantics from last-value `store` for
//! all three `_max` counters. The cycle/query wiring INSIDE
//! `record_do_try_match_cycle` is caught by the `query = cycle + 1`
//! schedule; a cycle/query swap AT THE PRODUCTION CALL SITE is
//! uncompilable by construction (`DoTryMatchCycleSample` named fields),
//! not merely tested.

use core::sync::atomic::Ordering;
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
use nativelink_scheduler::api_worker_scheduler::{DoTryMatchCycleSample, SchedulerMetrics};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
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

/// Every metric name this change adds, as rendered under the production
/// registration prefix (`scheduler.{name}.worker` + the
/// `#[metric(group = "scheduler_metrics")]` walk). In production these
/// render as `scheduler_MAIN_SCHEDULER_worker_scheduler_metrics_<name>`.
const NEW_METRIC_NAMES: [&str; 13] = [
    "do_try_match_cycles_total",
    "do_try_match_cycle_ms_sum",
    "do_try_match_cycle_ms_max",
    "do_try_match_query_ms_sum",
    "do_try_match_query_ms_max",
    "do_try_match_actions_matched_sum",
    "do_try_match_actions_matched_max",
    "do_try_match_cycle_ms_band_lt1_total",
    "do_try_match_cycle_ms_band_1_10_total",
    "do_try_match_cycle_ms_band_11_100_total",
    "do_try_match_cycle_ms_band_101_1k_total",
    "do_try_match_cycle_ms_band_1k_5k_total",
    "do_try_match_cycle_ms_band_gt5k_total",
];

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

type SchedulerPair = (
    Arc<SimpleScheduler>,
    Arc<dyn nativelink_scheduler::worker_scheduler::WorkerScheduler>,
);

fn make_scheduler() -> SchedulerPair {
    let task_notify = Arc::new(Notify::new());
    let awaited_action_db =
        memory_awaited_action_db_factory(0, &task_notify.clone(), MockInstantWrapped::default);
    let spec = SimpleSpec {
        worker_timeout_s: 100,
        ..Default::default()
    };
    SimpleScheduler::new_with_callback(
        &spec,
        awaited_action_db,
        || async move {},
        task_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    )
}

/// Register EXACTLY as nativelink.rs does: worker scheduler under
/// "scheduler.{name}.worker" via `register_dyn` — the production walk.
fn make_and_register(
    worker_scheduler: Arc<dyn nativelink_scheduler::worker_scheduler::WorkerScheduler>,
) -> MetricsRegistry {
    let registry = MetricsRegistry::new();
    let ws_component: Arc<
        dyn nativelink_util::metrics_publisher::MetricsComponentTrait + Send + Sync,
    > = worker_scheduler;
    registry.register_dyn("scheduler.test.worker".to_string(), ws_component);
    registry
}

/// (#matchcycle) Every new metric name renders on the production
/// registration path, AND the registered tree is the RECORDING tree.
///
/// Name pins: exact-value pinning is racy (the background matching loop
/// may already have recorded cycles by render time), so the literal
/// `\n<name> ` prefix pins the exact rendered name value-agnostically.
///
/// Value assertion: after an explicitly AWAITED cycle, the RENDERED
/// `cycles_total` line must parse to >= 1 (monotonic — background cycles
/// only push it higher, so this is race-free). A name-only pin cannot
/// catch a second never-incremented `SchedulerMetrics` instance being the
/// registered one (the `WORKER_EXEC_FAST_SLOW_STORE` second-instance
/// dark-counter trap); this closes it.
#[nativelink_test]
async fn matchcycle_render_pins_all_metric_names() -> Result<(), Error> {
    let (scheduler, worker_scheduler) = make_scheduler();
    let registry = make_and_register(worker_scheduler);
    let body = render_prometheus(&registry);

    for name in NEW_METRIC_NAMES {
        let expected = format!("\nscheduler_test_worker_scheduler_metrics_{name} ");
        assert!(
            body.contains(&expected),
            "#matchcycle DARK COUNTER: `{name}` did not render under the \
             production registration path (expected a line starting with \
             `scheduler_test_worker_scheduler_metrics_{name} `). In production \
             this metric would be silently invisible on /metrics — the exact \
             dark-counter trap this test exists to catch. body=\n{body}"
        );
    }

    // Registered tree == recording tree: drive one awaited cycle, then the
    // RENDERED value must reflect it.
    tokio::time::timeout(Duration::from_secs(5), scheduler.do_try_match_for_test())
        .await
        .expect(
            "#matchcycle DEADLOCK: do_try_match_for_test did not complete \
             within 5 s — the match cycle wedged",
        )
        .expect("do_try_match_for_test must succeed");
    let body = render_prometheus(&registry);
    let prefix = "\nscheduler_test_worker_scheduler_metrics_do_try_match_cycles_total ";
    let idx = body
        .find(prefix)
        .expect("#matchcycle: cycles_total line vanished from the re-render");
    let rendered_value: u64 = body[idx + prefix.len()..]
        .split_whitespace()
        .next()
        .expect("#matchcycle: cycles_total line has no value token")
        .parse()
        .expect("#matchcycle: cycles_total rendered a non-numeric value");
    assert!(
        rendered_value >= 1,
        "#matchcycle WRONG-INSTANCE DARK COUNTER: an awaited do_try_match \
         cycle completed but the RENDERED cycles_total is still \
         {rendered_value} — the SchedulerMetrics instance registered on the \
         metrics tree is not the instance the scheduler records into (the \
         second-instance dark-counter trap)"
    );
    Ok(())
}

/// (#matchcycle) Band edges bucket EXACTLY, with a unique sample count per
/// band probing every boundary value {0,1,10,11,100,101,1000,1001,4999,
/// 5000}, plus sum/max wiring for cycle, query, and matched.
///
/// `query_ms` is fed as `cycle_ms + 1` so crossed cycle/query wiring
/// INSIDE `record_do_try_match_cycle` changes the asserted sums/maxes (a
/// swap at the production call site is uncompilable — named-field
/// `DoTryMatchCycleSample`); `actions_matched` is the 1-based call index
/// so sum and max both differ from every other family.
///
/// The trailing DESCENDING sample (cycle 6000 < max 10000, query 6001 <
/// max 10001, matched 1 < max 21) separates `fetch_max` high-water
/// semantics from a last-value `store`: with `store`, all three `_max`
/// assertions read the final sample's values and fail. An ascending-only
/// schedule cannot tell the two apart (review 94a2ae37 T1).
#[nativelink_test]
async fn matchcycle_band_edges_bucket_uniquely() -> Result<(), Error> {
    let metrics = SchedulerMetrics::default();

    // Unique count per band: band0 gets 1 sample, band1 gets 2, ...
    // band5 gets 6 ascending samples + the descending sample = 7. Every
    // band boundary value appears; 5000 lands in gt5k (WARN alignment,
    // review 94a2ae37 C4).
    let samples_per_band: [&[u64]; 6] = [
        &[0],
        &[1, 10],
        &[11, 55, 100],
        &[101, 500, 750, 1000],
        &[1001, 2000, 3000, 4000, 4999],
        &[5000, 5001, 7000, 8000, 9000, 10000],
    ];

    let mut call_index: u64 = 0;
    let mut expected_cycle_sum: u64 = 0;
    for band_samples in samples_per_band {
        for &cycle_ms in band_samples {
            call_index += 1;
            expected_cycle_sum += cycle_ms;
            metrics.record_do_try_match_cycle(DoTryMatchCycleSample {
                cycle_ms,
                query_ms: cycle_ms + 1,
                actions_matched: call_index,
            });
        }
    }
    assert_eq!(call_index, 21, "#matchcycle self-check: 21 samples fed");

    // The descending sample (T1): every component strictly below its
    // family's running max. Lands in gt5k → its count becomes 7 (still
    // unique across bands).
    metrics.record_do_try_match_cycle(DoTryMatchCycleSample {
        cycle_ms: 6000,
        query_ms: 6001,
        actions_matched: 1,
    });
    expected_cycle_sum += 6000;

    let band_counters = [
        ("lt1", &metrics.do_try_match_cycle_ms_band_lt1_total, 1u64),
        ("1_10", &metrics.do_try_match_cycle_ms_band_1_10_total, 2),
        (
            "11_100",
            &metrics.do_try_match_cycle_ms_band_11_100_total,
            3,
        ),
        (
            "101_1k",
            &metrics.do_try_match_cycle_ms_band_101_1k_total,
            4,
        ),
        ("1k_5k", &metrics.do_try_match_cycle_ms_band_1k_5k_total, 5),
        ("gt5k", &metrics.do_try_match_cycle_ms_band_gt5k_total, 7),
    ];
    for (name, counter, expected) in band_counters {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            expected,
            "#matchcycle BAND MISBUCKET: band `{name}` must hold exactly \
             {expected} samples (unique per-band counts {{1,2,3,4,5,7}} \
             probing every boundary value incl. 4999/5000 — a band-boundary \
             swap or off-by-one relabels a band to the WRONG unique count \
             and fails here)"
        );
    }

    assert_eq!(
        metrics.do_try_match_cycles_total.load(Ordering::Relaxed),
        22,
        "#matchcycle CYCLE COUNTER UNWIRED: cycles_total must equal the 22 \
         record_do_try_match_cycle calls (one increment per cycle)"
    );
    assert_eq!(
        metrics.do_try_match_cycle_ms_sum.load(Ordering::Relaxed),
        expected_cycle_sum,
        "#matchcycle CYCLE SUM UNWIRED: cycle_ms_sum must accumulate every \
         cycle duration (mean derivation input)"
    );
    assert_eq!(
        metrics.do_try_match_cycle_ms_max.load(Ordering::Relaxed),
        10_000,
        "#matchcycle CYCLE MAX NOT HIGH-WATER: cycle_ms_max must stay at the \
         peak (10000ms) after the later DESCENDING 6000ms sample — a \
         last-value store instead of fetch_max reads 6000 and fails here"
    );
    assert_eq!(
        metrics.do_try_match_query_ms_sum.load(Ordering::Relaxed),
        expected_cycle_sum + 22,
        "#matchcycle QUERY SUM UNWIRED: query_ms_sum must accumulate every \
         query duration (fed as cycle_ms+1 for all 22 calls, so sum = \
         cycle_sum + 22; crossed cycle/query wiring inside the record \
         method fails here)"
    );
    assert_eq!(
        metrics.do_try_match_query_ms_max.load(Ordering::Relaxed),
        10_001,
        "#matchcycle QUERY MAX NOT HIGH-WATER: query_ms_max must stay at the \
         peak (10001ms) after the later DESCENDING 6001ms sample — a \
         last-value store instead of fetch_max reads 6001 and fails here"
    );
    assert_eq!(
        metrics
            .do_try_match_actions_matched_sum
            .load(Ordering::Relaxed),
        232,
        "#matchcycle MATCHED SUM UNWIRED: actions_matched_sum must accumulate \
         the per-cycle matched counts (1..=21 fed then a trailing 1, sum 232)"
    );
    assert_eq!(
        metrics
            .do_try_match_actions_matched_max
            .load(Ordering::Relaxed),
        21,
        "#matchcycle MATCHED MAX NOT HIGH-WATER: actions_matched_max must \
         stay at the peak (21) after the later DESCENDING matched=1 sample — \
         a last-value store instead of fetch_max reads 1 and fails here"
    );
    Ok(())
}

/// (#matchcycle) End-to-end behavior on the production composition: a real
/// worker + a real action through the REAL matching path. Proves the
/// `do_try_match` call site is wired (not just that the record method
/// works): the cycle counter advances across an explicit
/// `do_try_match_for_test` call, a duration band increments, and
/// `actions_matched_sum` advances when a match actually dispatches a
/// StartAction.
///
/// Lower-bound assertions are `>=`: the background matching loop runs
/// concurrently and may record additional cycles — that only moves
/// counters FURTHER past the threshold, so they are race-free. The final
/// OVER-count guard is an exact `==` delta and is still race-free: with
/// the only worker at capacity, EVERY concurrent cycle records matched=0,
/// so no interleaving can move the sum.
#[nativelink_test]
async fn matchcycle_behavior_cycle_and_matched_counters_advance() -> Result<(), Error> {
    let (scheduler, worker_scheduler) = make_scheduler();
    let metrics = scheduler.worker_scheduler_for_test().get_metrics().clone();

    // Register a worker with capacity 1.
    let (tx1, mut rx1) = mpsc::unbounded_channel::<UpdateForWorker>();
    let worker1 = Worker::new(
        WorkerId("w1".to_string()),
        PlatformProperties::default(),
        tx1,
        NOW_TIME,
        1,
    );
    worker_scheduler
        .add_worker(worker1)
        .await
        .expect("add worker1 must succeed");
    tokio::task::yield_now().await;
    let conn1 = rx1
        .recv()
        .await
        .expect("worker1 ConnectionResult must arrive");
    assert!(
        matches!(
            conn1.update,
            Some(update_for_worker::Update::ConnectionResult(
                ConnectionResult { .. }
            ))
        ),
        "#matchcycle setup: worker1 initial message must be ConnectionResult"
    );

    // Add one action; the matcher must dispatch it to worker1.
    scheduler
        .add_action(OperationId::default(), make_action_info(make_digest(1), 0))
        .await
        .expect("add_action must succeed");

    let start1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
        .await
        .expect(
            "#matchcycle setup: timed out waiting for worker1 StartAction — \
             matching engine did not dispatch within 5 s",
        )
        .expect("#matchcycle setup: worker1 channel closed before StartAction arrived");
    assert!(
        matches!(
            start1.update,
            Some(update_for_worker::Update::StartAction(_))
        ),
        "#matchcycle setup: worker1 must receive StartAction, got: {:?}",
        start1.update
    );

    // A match dispatched → the per-cycle matched telemetry must count it.
    // The StartAction is sent MID-cycle (inside the match future) while the
    // record happens at cycle END, so poll until the recording cycle
    // completes, bounded by a 5s deadline (a TIME bound, not an iteration
    // bound — 10k yields can burn in ms on a loaded runtime while the
    // cycle is still in flight; review 94a2ae37 T5). An unwired counter
    // fails via the timeout's bespoke message rather than hanging.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let matched_sum = metrics
                .do_try_match_actions_matched_sum
                .load(Ordering::Relaxed);
            let matched_max = metrics
                .do_try_match_actions_matched_max
                .load(Ordering::Relaxed);
            if matched_sum >= 1 && matched_max >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#matchcycle MATCHED COUNT UNWIRED: a StartAction was dispatched but \
         actions_matched_{sum,max} did not both reach >= 1 within 5 s — the \
         per-cycle matched count from the match_action_to_worker merge is \
         not reaching record_do_try_match_cycle",
    );

    // An explicit cycle must advance cycles_total by at least 1.
    let cycles_before = metrics.do_try_match_cycles_total.load(Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), scheduler.do_try_match_for_test())
        .await
        .expect(
            "#matchcycle DEADLOCK: do_try_match_for_test did not complete \
             within 5 s — the match cycle wedged",
        )
        .expect("do_try_match_for_test must succeed");
    let cycles_after = metrics.do_try_match_cycles_total.load(Ordering::Relaxed);
    assert!(
        cycles_after >= cycles_before + 1,
        "#matchcycle CYCLE COUNTER UNWIRED: an awaited do_try_match cycle \
         completed but cycles_total did not advance (before={cycles_before}, \
         after={cycles_after}) — the record call at the do_try_match \
         total_elapsed site is missing"
    );

    // Every recorded cycle lands in exactly one duration band.
    let band_sum: u64 = [
        &metrics.do_try_match_cycle_ms_band_lt1_total,
        &metrics.do_try_match_cycle_ms_band_1_10_total,
        &metrics.do_try_match_cycle_ms_band_11_100_total,
        &metrics.do_try_match_cycle_ms_band_101_1k_total,
        &metrics.do_try_match_cycle_ms_band_1k_5k_total,
        &metrics.do_try_match_cycle_ms_band_gt5k_total,
    ]
    .iter()
    .map(|c| c.load(Ordering::Relaxed))
    .sum();
    assert!(
        band_sum >= 1,
        "#matchcycle BAND BUCKETING UNWIRED: at least one cycle completed but \
         no duration band incremented — the band fetch_add in \
         record_do_try_match_cycle is missing"
    );

    // OVER-count guard (review 94a2ae37 T3): every prior matched assertion
    // is `>=`, so counting NON-matches as matches (e.g. `Ok(false)` bumping
    // the counter) would stay green above. Deterministic upper bound:
    // worker1 (capacity 1) is still busy with action 1 — it never
    // completed — so a second queued action CANNOT match
    // (find_and_reserve_worker returns None → Ok(false)), and every cycle,
    // ours or the background loop's, records matched=0. The sum must
    // advance by EXACTLY zero across an awaited cycle.
    scheduler
        .add_action(OperationId::default(), make_action_info(make_digest(2), 1))
        .await
        .expect("add_action 2 must succeed");
    let matched_before_unmatchable = metrics
        .do_try_match_actions_matched_sum
        .load(Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), scheduler.do_try_match_for_test())
        .await
        .expect(
            "#matchcycle DEADLOCK: do_try_match_for_test (unmatchable-action \
             cycle) did not complete within 5 s",
        )
        .expect("do_try_match_for_test (unmatchable-action cycle) must succeed");
    let matched_after_unmatchable = metrics
        .do_try_match_actions_matched_sum
        .load(Ordering::Relaxed);
    assert_eq!(
        matched_after_unmatchable, matched_before_unmatchable,
        "#matchcycle MATCHED OVER-COUNT: a queued action with NO available \
         worker (capacity-1 worker still busy) must contribute ZERO to \
         actions_matched_sum — a non-match path (per-client skip / reject / \
         no-worker / benign-Aborted) is being counted as a match"
    );
    Ok(())
}
