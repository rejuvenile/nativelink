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

//! P1/P2 scheduler completeness coverage (the wave after the P0 gap-matrix,
//! TLA+ model, and Group B state-machine tests). Each test drives the
//! production `SimpleScheduler` composition (matcher + worker pool + state
//! manager) and pins the ACTUAL behavior verified against current source, not
//! aspirational behavior. Items already covered by a P0 test are SKIPPED (see
//! the module tail for the skip ledger).
//!
//! Load discipline: every worker gets real P/E core counts (`> 0`) via
//! `setup_new_worker_with_core_counts` AND an explicit `update_worker_load`
//! call. A never-reported worker is treated as SATURATED by the selector
//! (#sched-zeroload) — so a test that forgets to report load exercises the
//! fall-through path, not the load-aware blend. `load_byte_cost` is set
//! EXPLICITLY in every `SimpleSpec` (not left to `::default()`, whose
//! `#[derive(Default)]` yields `0` and disables the load blend) so the tests
//! pass regardless of which `SimpleSpec::default()` fidelity fix merges first.

use core::ops::Bound;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::Stream;
use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, SortedAwaitedAction, SortedAwaitedActionState,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::memory_awaited_action_db::MemoryAwaitedActionDb;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{ClientStateManager, UpdateOperationType};
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc, oneshot};

mod utils {
    pub(crate) mod scheduler_utils;
}
use utils::scheduler_utils::INSTANCE_NAME;

const NOW_TIME: u64 = 10000;
const WORKER_TIMEOUT_S: u64 = 100;
/// `load_byte_cost` used by every spec in this file. Set EXPLICITLY (never via
/// `SimpleSpec::default()`, which is `0` and disables the load blend). Matches
/// the serde `default_load_byte_cost` = 512 KiB the deployed config resolves.
const LOAD_BYTE_COST: u64 = 512 * 1024;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

/// Build an `ActionInfo` with a specific `instance_name`, `priority`, digest
/// seed, and insert timestamp. The `instance_name` lives inside the
/// `ActionUniqueKey` (which the fair-scheduling counter keys on); a distinct
/// digest keeps two actions from the same client from deduplicating.
fn make_action_info(
    instance_name: &str,
    priority: i32,
    seed: u8,
    insert_timestamp: SystemTime,
) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: instance_name.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new([seed; 32], 512),
        }),
        targetkey: None,
    })
}

async fn verify_initial_connection_message(
    worker_id: WorkerId,
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) {
    let expected = UpdateForWorker {
        update: Some(update_for_worker::Update::ConnectionResult(
            ConnectionResult {
                worker_id: worker_id.into(),
            },
        )),
    };
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg, expected);
}

/// Add a worker with realistic P/E logical-CPU counts so the continuous
/// cache-vs-load blend (`capacity_score`) has a real per-core denominator. In
/// production the counts ride the connect hello frame; the `#[cfg(test)]`
/// scheduler helper is only visible to the src crate, so an integration test
/// builds the worker here via `new_with_cas_endpoint` with an empty CAS
/// endpoint (counts only, no locality wiring).
async fn setup_new_worker_with_core_counts(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
    p_core_count: u32,
    e_core_count: u32,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new_with_cas_endpoint(
        worker_id.clone(),
        props,
        tx,
        NOW_TIME,
        0,
        String::new(),
        p_core_count,
        e_core_count,
        0, // total_memory_kb (Phase-3 off in this test)
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

/// Build a scheduler whose BACKGROUND matcher task is INERT: the DB is wired to
/// a `db_notify` (fired on `add_action`), but the scheduler's own matcher loop
/// is handed a SEPARATE `sched_notify` that nothing ever fires — so the loop
/// parks at its top-of-loop `notified().await` and never runs a cycle on its
/// own. `do_try_match_for_test()` then becomes the SOLE driver of match cycles,
/// which is what makes the per-cycle / single-cycle assertions in this file
/// deterministic (a free-running background matcher would race, running an
/// unbounded number of extra cycles with fresh per-cycle counters). The
/// `do_try_match` path reads queued actions from the DB directly, independent of
/// either notify, so the queue is fully visible to the explicit driver.
fn make_scheduler(spec: SimpleSpec) -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>) {
    let db_notify = Arc::new(Notify::new());
    let sched_notify = Arc::new(Notify::new()); // never fired -> matcher loop inert
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &db_notify, MockInstantWrapped::default),
        || async move {},
        sched_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    (scheduler, worker_scheduler)
}

/// Receive the next StartAction for a dispatched action and return its
/// operation id. No draining: peer-hint chunks only ride when hints are
/// non-empty (never in these tests — no locality wiring), so exactly one
/// message arrives per dispatch and draining would risk eating the NEXT
/// dispatch/completion signal.
async fn recv_start_execute(rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>) -> String {
    let msg = rx.recv().await.expect("worker channel closed");
    match msg.update {
        Some(update_for_worker::Update::StartAction(se)) => se.operation_id,
        v => panic!("expected StartAction, got: {v:?}"),
    }
}

// ───────────────────────────── P1-3 fair scheduling ─────────────────────────
//
// `max_matches_per_client_per_cycle` (simple_scheduler.rs:572, config
// schedulers.rs:180). One client's N queued actions are capped at K matches in
// a single `do_try_match` cycle; a different client's action is still matched
// in the SAME cycle; the skipped actions stay Queued (matched on a later
// cycle). The counter keys on `instance_name` (match_action_to_worker_cached
// :724-733) and skipped actions are NOT rejected — they `return Ok(())` while
// leaving the action queued (:728-731).

/// K=1: client A submits TWO actions, client B submits ONE. With 3 idle
/// workers and `max_matches_per_client_per_cycle = 1`, a single cycle matches
/// AT MOST one of A's actions plus B's action — A's second action must NOT be
/// dispatched in that cycle (it stays Queued). Proves the per-client cap fires
/// AND does not starve the other client.
#[nativelink_test]
async fn fair_scheduling_caps_one_client_but_schedules_other_same_cycle_test()
-> Result<(), Error> {
    let (scheduler, _ws) = make_scheduler(SimpleSpec {
        worker_timeout_s: WORKER_TIMEOUT_S,
        load_byte_cost: LOAD_BYTE_COST,
        max_matches_per_client_per_cycle: 1,
        ..Default::default()
    });

    // Three idle workers so worker availability is NEVER the limiter — the cap
    // is the only reason an action can stay Queued this cycle.
    let mut rxs = Vec::new();
    for (i, id) in ["fair_w0", "fair_w1", "fair_w2"].iter().enumerate() {
        let wid = WorkerId((*id).to_string());
        let mut rx = setup_new_worker_with_core_counts(
            &scheduler,
            wid.clone(),
            PlatformProperties::default(),
            4,
            6,
        )
        .await?;
        scheduler.update_worker_load(&wid, 30, 30, 30).await?;
        // Drain nothing yet; keep the rx.
        let _ = &mut rx;
        rxs.push((wid, rx, i));
    }

    // Client A: two actions (distinct digests, same instance_name).
    let a_op1 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info("client_a", 0, 0xA1, make_system_time(1)),
        )
        .await?;
    let a_op2 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info("client_a", 0, 0xA2, make_system_time(2)),
        )
        .await?;
    // Client B: one action (different instance_name).
    let b_op1 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info("client_b", 0, 0xB1, make_system_time(3)),
        )
        .await?;
    let _ = (&a_op1, &a_op2, &b_op1);

    // Run EXACTLY one match cycle.
    scheduler.do_try_match_for_test().await?;

    // Count how many StartActions were dispatched across the whole fleet this
    // cycle, and track how many carried each instance_name via the client
    // subscribers' terminal stage. We read dispatch counts from the worker
    // channels (fleet-wide): the cap limits client A to 1 dispatch, client B
    // gets its 1 — so EXACTLY 2 StartActions total this cycle, not 3.
    let mut dispatched = 0usize;
    for (_wid, rx, _i) in &mut rxs {
        while let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            if matches!(msg.update, Some(update_for_worker::Update::StartAction(_))) {
                dispatched += 1;
            }
        }
    }
    assert_eq!(
        dispatched, 2,
        "with max_matches_per_client_per_cycle=1 and 3 idle workers, exactly \
         ONE of client A's two actions plus client B's one action must dispatch \
         in a single cycle (2 total) — got {dispatched}. More than 2 means the \
         per-client cap did not fire; fewer means the other client was starved."
    );

    // The remaining client-A action must still be Queued (skipped, not
    // rejected). It is dispatched on the NEXT cycle. Run another cycle and
    // confirm the third StartAction finally lands (cap resets per cycle).
    scheduler.do_try_match_for_test().await?;
    let mut dispatched_cycle2 = 0usize;
    for (_wid, rx, _i) in &mut rxs {
        while let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            if matches!(msg.update, Some(update_for_worker::Update::StartAction(_))) {
                dispatched_cycle2 += 1;
            }
        }
    }
    assert_eq!(
        dispatched_cycle2, 1,
        "the client-A action skipped by the per-cycle cap must stay Queued and \
         dispatch on the NEXT cycle (1 more StartAction) — got \
         {dispatched_cycle2}; a skipped action must be re-schedulable, not \
         dropped or rejected"
    );

    Ok(())
}

// ────────────── P1-7 paused_due_to_backpressure NOT cleared by capacity ──────
//
// A worker paused because it reported ResourceExhausted (worker backpressure)
// stays paused even with free capacity — the capacity-based unpause gate
// (api_worker_scheduler.rs:1040) explicitly skips `paused_due_to_backpressure`
// workers ("Workers that reported ResourceExhausted should remain paused until
// they complete an action"). It clears ONLY when the worker completes an action
// (worker.rs:462,472-473 `complete_action` unconditionally clears both flags).
//
// TOPOLOGY NOTE: the unpause gate is only REACHABLE when at least one OTHER
// worker can accept work — `inner_find_and_reserve_worker` early-returns
// (api_worker_scheduler.rs:1013) if NO worker `can_accept_work()`, which is the
// case when the ONLY worker is paused. So this test uses TWO workers: the
// backpressure-paused `bp_worker` (LIGHT load, so it would WIN selection if the
// gate wrongly unpaused it) and an available `helper` (HEAVY load, the correct
// fallback). Correct behavior routes the new action to `helper`; a mutation
// that drops the `!paused_due_to_backpressure` guard would unpause `bp_worker`
// and, because it is lighter, steer the action to it instead.

/// `bp_worker` runs two actions; one reports `UpdateWithError(ResourceExhausted)`
/// so `bp_worker` becomes `paused_due_to_backpressure` while the other action is
/// still in flight (free capacity: 1 running, `max_inflight_tasks` unlimited). A
/// new action is then queued while `helper` is also available. The action must
/// dispatch to `helper` — the backpressure-paused `bp_worker` must NOT be
/// unpaused by the capacity gate despite having free capacity AND lighter load.
#[nativelink_test]
async fn backpressure_paused_worker_not_unpaused_by_capacity_gate_test()
-> Result<(), Error> {
    let (scheduler, _ws) = make_scheduler(SimpleSpec {
        worker_timeout_s: WORKER_TIMEOUT_S,
        load_byte_cost: LOAD_BYTE_COST,
        ..Default::default()
    });

    let bp_worker = WorkerId("bp_worker".to_string());
    let mut rx_bp = setup_new_worker_with_core_counts(
        &scheduler,
        bp_worker.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;
    // bp_worker reports LIGHT load: if the gate wrongly unpauses it, its lower
    // load_penalty would make it WIN over helper.
    scheduler.update_worker_load(&bp_worker, 5, 5, 5).await?;

    // Dispatch two actions onto bp_worker and hold both in-flight.
    let _l1 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info(INSTANCE_NAME, 0, 0x01, make_system_time(1)),
        )
        .await?;
    scheduler.do_try_match_for_test().await?;
    let op1 = recv_start_execute(&mut rx_bp).await;

    let _l2 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info(INSTANCE_NAME, 0, 0x02, make_system_time(2)),
        )
        .await?;
    scheduler.do_try_match_for_test().await?;
    let _op2 = recv_start_execute(&mut rx_bp).await;
    assert_ne!(op1, _op2, "the two held actions must be distinct operations");

    // op1 reports ResourceExhausted (worker backpressure). This frees op1's slot
    // (op2 still running -> has_actions()==true), setting is_paused +
    // paused_due_to_backpressure (update_action_cs2, api_worker_scheduler.rs:1958).
    scheduler
        .update_action(
            &bp_worker,
            &OperationId::from(op1.as_str()),
            UpdateOperationType::UpdateWithError(make_err!(
                Code::ResourceExhausted,
                "worker backpressure NAK"
            )),
        )
        .await?;
    // op1 is re-queued by the ResourceExhausted (retryable) path; drain that
    // re-dispatch attempt if any lands (it must NOT, since bp_worker is now
    // paused and there is no other worker yet). We add helper next.

    // Add helper AFTER the pause is established so the unpause gate is reachable
    // (a second worker can accept work). helper reports HEAVY load -> it is the
    // LESS-preferred worker; it wins ONLY because bp_worker is (correctly) still
    // excluded by its backpressure pause.
    let helper = WorkerId("helper_worker".to_string());
    let mut rx_helper = setup_new_worker_with_core_counts(
        &scheduler,
        helper.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;
    scheduler.update_worker_load(&helper, 95, 95, 95).await?;

    // Queue a fresh action and match. It must go to helper (bp_worker stays
    // backpressure-paused). op1's re-queued action is also pending; either way,
    // NONE of the pending actions may land on bp_worker.
    let _l3 = scheduler
        .add_action(
            OperationId::default(),
            make_action_info(INSTANCE_NAME, 0, 0x03, make_system_time(3)),
        )
        .await?;
    scheduler.do_try_match_for_test().await?;

    // helper must receive a StartAction; bp_worker must receive NOTHING.
    let helper_msg = tokio::time::timeout(Duration::from_secs(2), rx_helper.recv())
        .await
        .expect("helper did not receive the dispatched action within 2s — the \
                 backpressure-paused bp_worker was wrongly selected, or the \
                 action wedged")
        .expect("helper channel closed");
    assert!(
        matches!(helper_msg.update, Some(update_for_worker::Update::StartAction(_))),
        "helper must receive the StartAction (bp_worker is backpressure-paused)"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), rx_bp.recv())
            .await
            .is_err(),
        "bp_worker (backpressure-paused, free capacity, LIGHTER load) was \
         selected for new work — the capacity-based unpause gate must NOT clear \
         paused_due_to_backpressure (api_worker_scheduler.rs:1040); the action \
         must route to the available helper instead"
    );

    Ok(())
}

// ─────────────── P1-10 stale load: selection uses last-reported value ────────
//
// `update_worker_load` (api_worker_scheduler.rs:5537-5561) stores the reported
// load with NO timestamp / TTL / freshness field; `has_reported_load` is set
// true and NEVER reset. So once a worker reports load, the selector keeps using
// that LAST value indefinitely (until the worker times out and is removed) even
// if no further keepalive/load report arrives. This test pins that documented
// behavior.

/// Two reporting workers. worker_light reports LOW load, worker_heavy reports
/// HIGH load. Time then advances (short of the worker timeout, no worker
/// removed) and NO further load reports arrive. A new action is dispatched: it
/// must go to worker_light — the selector is still using the last-reported
/// (now stale) values. The pick does not decay to a tie or flip.
#[nativelink_test]
async fn selection_uses_last_reported_load_after_reports_stop_test() -> Result<(), Error> {
    let (scheduler, _ws) = make_scheduler(SimpleSpec {
        worker_timeout_s: WORKER_TIMEOUT_S,
        load_byte_cost: LOAD_BYTE_COST,
        ..Default::default()
    });

    let light = WorkerId("stale_light".to_string());
    let heavy = WorkerId("stale_heavy".to_string());
    // Add HEAVY first so it is the LRU-OLDEST. This isolates the STALE-LOAD
    // mechanism: under a load-blind selection the LRU tiebreak returns the
    // oldest (heavy), so a load-blind regression red-fails; only a genuine
    // stale-load ranking picks the lighter (newer) worker. If `light` were
    // added first it would win on BOTH lower load AND LRU-oldest, so the
    // assertion couldn't distinguish stale-load from LRU (testing-czar 0c299807).
    let mut rx_heavy = setup_new_worker_with_core_counts(
        &scheduler,
        heavy.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;
    let mut rx_light = setup_new_worker_with_core_counts(
        &scheduler,
        light.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;

    // Report load ONCE each, then never again. light is much lighter.
    scheduler.update_worker_load(&light, 5, 5, 5).await?;
    scheduler.update_worker_load(&heavy, 90, 90, 90).await?;

    // Keep both workers alive so neither is removed/quarantined, but do NOT
    // re-report load — the reported values are now stale.
    scheduler
        .worker_keep_alive_received(&light, NOW_TIME + WORKER_TIMEOUT_S)
        .await?;
    scheduler
        .worker_keep_alive_received(&heavy, NOW_TIME + WORKER_TIMEOUT_S)
        .await?;

    // Dispatch an action. It must go to the worker whose LAST-reported load was
    // lightest, using the stale value.
    let _l = scheduler
        .add_action(
            OperationId::default(),
            make_action_info(INSTANCE_NAME, 0, 0x77, make_system_time(1)),
        )
        .await?;
    scheduler.do_try_match_for_test().await?;

    let selected = tokio::select! {
        msg = rx_light.recv() => {
            assert!(matches!(msg.unwrap().update, Some(update_for_worker::Update::StartAction(_))));
            light.clone()
        }
        msg = rx_heavy.recv() => {
            assert!(matches!(msg.unwrap().update, Some(update_for_worker::Update::StartAction(_))));
            heavy.clone()
        }
    };
    assert_eq!(
        selected, light,
        "selection must use the LAST-reported load even after reports stop \
         (no freshness/TTL on worker load) — the lighter last-reported worker \
         must win; a flip to worker_heavy would mean the stale value was \
         discarded"
    );

    Ok(())
}

// ─────────────── P1-9 consecutive_match_errors corruption alert at 10 ────────
//
// The matcher's background loop (simple_scheduler.rs:1118,1355-1368) counts
// CONSECUTIVE `do_try_match` errors and, at `>= 10`, emits a distinctive
// "possible scheduler data structure corruption" error log; a single SUCCESS
// resets the counter to 0. Input-validation rejections (FailedPrecondition,
// re-tagged from InvalidArgument at :796,836) return `Ok` from the matcher and
// so do NOT count. The counter is a bare `u32` local to the spawn closure — no
// metric, no accessor — so the only observable is the alert log. We drive the
// REAL background loop and use the injected `on_matching_engine_run` callback
// as a per-iteration barrier (a oneshot handshake, NOT a sleep) to count cycles
// deterministically; a `TestDb` seam forces `get_range_of_actions(Queued)` to
// error so every cycle fails genuinely.

/// Test-only `AwaitedActionDb` wrapper that (optionally) fails
/// `get_range_of_actions` for the `Queued` state — the exact call
/// `SimpleScheduler::get_queued_operations` makes — so `do_try_match` returns a
/// genuine (non-input-validation) `Err` every cycle. Every other method
/// delegates to the real `MemoryAwaitedActionDb`.
type NowFnT = fn() -> MockInstantWrapped;
type MemDb = MemoryAwaitedActionDb<MockInstantWrapped, NowFnT>;

#[derive(MetricsComponent)]
struct QueueErrDb {
    #[metric]
    inner: Arc<MemDb>,
    fail_queued: Arc<AtomicBool>,
}

impl AwaitedActionDb for QueueErrDb {
    type Subscriber = <MemDb as AwaitedActionDb>::Subscriber;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        self.inner.get_awaited_action_by_id(client_operation_id).await
    }

    async fn get_all_awaited_actions(
        &self,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        self.inner.get_all_awaited_actions().await
    }

    async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        self.inner.get_by_operation_id(operation_id).await
    }

    async fn get_range_of_actions(
        &self,
        state: SortedAwaitedActionState,
        start: Bound<SortedAwaitedAction>,
        end: Bound<SortedAwaitedAction>,
        desc: bool,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        if matches!(state, SortedAwaitedActionState::Queued)
            && self.fail_queued.load(Ordering::SeqCst)
        {
            return Err(make_err!(
                Code::Internal,
                "injected get_range_of_actions(Queued) failure (test seam)"
            ));
        }
        self.inner.get_range_of_actions(state, start, end, desc).await
    }

    async fn update_awaited_action(
        &self,
        new_awaited_action: AwaitedAction,
    ) -> Result<(), Error> {
        self.inner.update_awaited_action(new_awaited_action).await
    }

    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        self.inner
            .add_action(client_operation_id, action_info, no_event_action_timeout)
            .await
    }
}

/// Ten CONSECUTIVE genuine matcher errors must trip the corruption alert, and a
/// success in between must reset the counter (so the alert does NOT trip until
/// ten UNBROKEN failures follow the last success). Drives the real background
/// loop; each cycle is gated by the `on_matching_engine_run` barrier so the
/// count is deterministic (no sleeps, no lost-wakeup races).
#[nativelink_test]
async fn ten_consecutive_matcher_errors_trip_corruption_alert_reset_on_success_test()
-> Result<(), Error> {
    let fail_queued = Arc::new(AtomicBool::new(true));
    let notify = Arc::new(Notify::new());
    let now_fn: NowFnT = MockInstantWrapped::default;
    let inner: Arc<MemDb> = Arc::new(memory_awaited_action_db_factory(0, &notify, now_fn));
    let db = QueueErrDb {
        inner,
        fail_queued: fail_queued.clone(),
    };

    // Per-iteration barrier: the callback sends a signal and blocks until the
    // test releases it, so the test steps the loop exactly N times with no
    // timing coupling. Bounded channels of one in-flight step at a time.
    let (step_tx, mut step_rx) = mpsc::channel::<oneshot::Sender<()>>(1);
    let task_change_notify = Arc::new(Notify::new());
    let callback_notify = task_change_notify.clone();
    let step_tx_for_cb = step_tx.clone();
    let on_run = move || {
        let step_tx = step_tx_for_cb.clone();
        let callback_notify = callback_notify.clone();
        async move {
            // Re-arm the loop's top-of-loop select BEFORE we block, so the next
            // iteration proceeds immediately once we release.
            callback_notify.notify_one();
            let (ack_tx, ack_rx) = oneshot::channel();
            // If the receiver is gone the scheduler is shutting down; ignore.
            if step_tx.send(ack_tx).await.is_ok() {
                let _ = ack_rx.await;
            }
        }
    };

    let (_scheduler, _ws) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            load_byte_cost: LOAD_BYTE_COST,
            ..Default::default()
        },
        db,
        on_run,
        task_change_notify.clone(),
        now_fn,
        None,
        None,
        None,
        None,
    );

    // Kick the loop.
    task_change_notify.notify_one();

    // Step the loop N times, releasing each barrier. Between steps, assert the
    // alert has/has-not fired as expected. A oneshot handshake per step makes
    // the cadence deterministic.
    async fn step(step_rx: &mut mpsc::Receiver<oneshot::Sender<()>>) {
        let ack = tokio::time::timeout(Duration::from_secs(5), step_rx.recv())
            .await
            .expect("matcher loop did not reach the on_matching_engine_run barrier within 5s")
            .expect("matcher loop callback channel closed unexpectedly");
        ack.send(()).expect("failed to release matcher loop barrier");
    }

    // Phase 1 — nine failures, alert MUST NOT have fired yet (threshold is
    // >= 10). Each `step()` returns AFTER the loop body (do_try_match + counter
    // update) has run and the callback barrier is reached, so after N steps the
    // counter equals N.
    for _ in 0..9 {
        step(&mut step_rx).await;
    }
    assert!(
        !logs_contain("possible scheduler data structure corruption"),
        "the corruption alert fired before 10 consecutive errors — the \
         threshold is >= 10 (simple_scheduler.rs:1356)"
    );

    // Phase 2 — RESET-ON-SUCCESS. Let the NEXT cycle SUCCEED (queued query
    // stops failing) so the loop resets `consecutive_match_errors = 0`
    // (simple_scheduler.rs:1368). Without this reset the counter would carry 9
    // forward and the very next failure below would be the 10th overall,
    // tripping the alert prematurely.
    fail_queued.store(false, Ordering::SeqCst);
    step(&mut step_rx).await; // cycle 10 overall, but a SUCCESS -> counter := 0
    fail_queued.store(true, Ordering::SeqCst);

    // Phase 3 — nine MORE failures. If the counter were NOT reset by the
    // success, this run would have accumulated to 18 and fired long ago; with a
    // correct reset the counter is only at 9 here, so the alert MUST still be
    // silent.
    for _ in 0..9 {
        step(&mut step_rx).await;
    }
    assert!(
        !logs_contain("possible scheduler data structure corruption"),
        "the corruption alert fired after only 9 consecutive errors following a \
         SUCCESS — a single success must reset consecutive_match_errors to 0 \
         (simple_scheduler.rs:1368); it was not reset"
    );

    // Phase 4 — the tenth consecutive failure (post-reset) trips the alert. The
    // alert log is emitted inside the loop body BEFORE the callback barrier, so
    // by the time this `step()` returns the log exists.
    step(&mut step_rx).await;
    assert!(
        logs_contain("possible scheduler data structure corruption"),
        "10 consecutive genuine matcher errors (after the reset) must trip the \
         corruption alert (simple_scheduler.rs:1356-1362) — it did not fire at 10"
    );

    Ok(())
}

// ── P1 skip ledger (verified already covered against current source) ─────────
//
// * P1-2 SIGKILL retry + boost_priority — COVERED by Group B
//   `sigkill_exit9_increments_attempts_and_boosts_priority`
//   (simple_scheduler_state_manager_test.rs). Skipped.
// * P1-5 two-worker tie determinism — COVERED by
//   `identical_workers_tie_break_is_deterministic_lru_test`
//   (simple_scheduler_test.rs, 8-iteration stable LRU pick). Skipped.
// * P1-6 quarantine gate skip + clear-on-keepalive — COVERED by
//   `quarantined_worker_skipped_then_selectable_after_keepalive_test`
//   (simple_scheduler_test.rs). Skipped.
// * P1-1 priority ordering + FIFO tiebreak — implemented in
//   `simple_scheduler_state_manager_test.rs` (the production sort seam
//   `filter_operations(Queued, Desc)`), NOT here.
