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

//! (#matchcycle) Render + behavior tests for the pure matcher-work
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
//! counter in production.
//!
//! Band-edge coverage: `matchcycle_band_edges_bucket_uniquely` drives
//! `record_do_try_match_cycle` directly with a UNIQUE sample count per
//! band (1,2,3,4,5,6) probing every boundary value, so ANY band-boundary
//! swap or off-by-one relabels a band to the wrong count and fails
//! exactly (the #calib band-swap lesson).

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
use nativelink_scheduler::api_worker_scheduler::SchedulerMetrics;
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
/// registration path. This is the dark-counter guard: a field that
/// increments in-process but never renders (unregistered tree, doubled
/// name, wrong group) fails HERE, not in a 3-week-later soak.
///
/// Values are NOT pinned (the background matching loop may already have
/// recorded cycles by render time); the literal `\n<name> ` prefix pins
/// the exact rendered name while staying value-agnostic.
#[nativelink_test]
async fn matchcycle_render_pins_all_metric_names() -> Result<(), Error> {
    let (_scheduler, worker_scheduler) = make_scheduler();
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
    Ok(())
}

/// (#matchcycle) Band edges bucket EXACTLY, with a unique sample count per
/// band (1..=6) probing every boundary value {0,1,10,11,100,101,1000,
/// 1001,5000,5001}, plus sum/max wiring for cycle, query, and matched.
///
/// `query_ms` is fed as `cycle_ms + 1` so a crossed cycle/query wiring
/// mutation changes the asserted sums/maxes; `actions_matched` is the
/// 1-based call index so sum (231) and max (21) both differ from every
/// other family.
#[nativelink_test]
async fn matchcycle_band_edges_bucket_uniquely() -> Result<(), Error> {
    let metrics = SchedulerMetrics::default();

    // Unique count per band: band0 gets 1 sample, band1 gets 2, ... band5
    // gets 6. Every band boundary value appears.
    let samples_per_band: [&[u64]; 6] = [
        &[0],
        &[1, 10],
        &[11, 55, 100],
        &[101, 500, 750, 1000],
        &[1001, 2000, 3000, 4000, 5000],
        &[5001, 6000, 7000, 8000, 9000, 10000],
    ];

    let mut call_index: u64 = 0;
    let mut expected_cycle_sum: u64 = 0;
    for band_samples in samples_per_band {
        for &cycle_ms in band_samples {
            call_index += 1;
            expected_cycle_sum += cycle_ms;
            metrics.record_do_try_match_cycle(cycle_ms, cycle_ms + 1, call_index);
        }
    }
    assert_eq!(call_index, 21, "#matchcycle self-check: 21 samples fed");

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
        ("gt5k", &metrics.do_try_match_cycle_ms_band_gt5k_total, 6),
    ];
    for (name, counter, expected) in band_counters {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            expected,
            "#matchcycle BAND MISBUCKET: band `{name}` must hold exactly \
             {expected} samples (unique per-band counts 1..=6 probing every \
             boundary value — a band-boundary swap or off-by-one relabels a \
             band to the WRONG unique count and fails here)"
        );
    }

    assert_eq!(
        metrics.do_try_match_cycles_total.load(Ordering::Relaxed),
        21,
        "#matchcycle CYCLE COUNTER UNWIRED: cycles_total must equal the 21 \
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
        "#matchcycle CYCLE MAX UNWIRED: cycle_ms_max must fetch_max to the \
         worst cycle (10000ms fed)"
    );
    assert_eq!(
        metrics.do_try_match_query_ms_sum.load(Ordering::Relaxed),
        expected_cycle_sum + 21,
        "#matchcycle QUERY SUM UNWIRED: query_ms_sum must accumulate every \
         query duration (fed as cycle_ms+1, so sum = cycle_sum + 21; a \
         crossed cycle/query wiring fails here)"
    );
    assert_eq!(
        metrics.do_try_match_query_ms_max.load(Ordering::Relaxed),
        10_001,
        "#matchcycle QUERY MAX UNWIRED: query_ms_max must fetch_max to the \
         worst query (10001ms fed)"
    );
    assert_eq!(
        metrics
            .do_try_match_actions_matched_sum
            .load(Ordering::Relaxed),
        231,
        "#matchcycle MATCHED SUM UNWIRED: actions_matched_sum must accumulate \
         the per-cycle matched counts (1..=21 fed, sum 231)"
    );
    assert_eq!(
        metrics
            .do_try_match_actions_matched_max
            .load(Ordering::Relaxed),
        21,
        "#matchcycle MATCHED MAX UNWIRED: actions_matched_max must fetch_max \
         to the largest per-cycle matched count (21 fed)"
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
/// Assertions are `>=` (never `==`): the background matching loop runs
/// concurrently and may record additional cycles — that only moves
/// counters FURTHER past the threshold, so the assertions are race-free.
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
    // record happens at cycle END, so give the matching cycle a bounded
    // number of yields to complete — an iteration bound, not wall-clock
    // sleep, so an unwired counter still fails with the bespoke message
    // below rather than hanging.
    let mut matched_sum = 0;
    let mut matched_max = 0;
    for _ in 0..10_000 {
        matched_sum = metrics
            .do_try_match_actions_matched_sum
            .load(Ordering::Relaxed);
        matched_max = metrics
            .do_try_match_actions_matched_max
            .load(Ordering::Relaxed);
        if matched_sum >= 1 && matched_max >= 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        matched_sum >= 1,
        "#matchcycle MATCHED COUNT UNWIRED: a StartAction was dispatched but \
         actions_matched_sum stayed 0 across 10000 yields — the per-cycle \
         matched count from the match_action_to_worker merge is not reaching \
         record_do_try_match_cycle"
    );
    assert!(
        matched_max >= 1,
        "#matchcycle MATCHED MAX UNWIRED: a StartAction was dispatched but \
         actions_matched_max stayed 0 across 10000 yields"
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
    Ok(())
}
