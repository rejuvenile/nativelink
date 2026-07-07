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

//! (#specprefetch-rebind Stage B v3) Tests for the TEMPORAL hold-vs-rebind gate
//! (`inner_find_and_reserve_worker`, design §2.3-v3).
//!
//! v3 re-derivation: "busy" W is now **P-SATURATED** (`!worker_has_p_headroom`,
//! `running >= p_core_count` at the default threshold), NOT slot-full — the live
//! fleet runs `max_inflight_tasks=0` (unlimited slots), so the v2 slot-full gate
//! could never fire. The hold is the TEMPORAL exception to pcore-first's SPATIAL
//! route-away: when `p_gate_active` EXCLUDES a P-saturated holder W from the cache
//! tiers, the op would route to a cold X that re-constructs; the gate HOLDS the op
//! for W iff W REGAINS a P-slot (`T_wait_W = k-th soonest completion < T_setup`)
//! before X could re-construct. Gated on `p_gate_active` (design §2.3-v3.7-#1) so
//! it does NOT hold on a fully-P-saturated Phase-2 fleet (where pcore-first spreads
//! work). Behind `enable_speculative_hold` (default OFF).
//!
//! Test matrix:
//!  hold_no_deadlock_under_write_lock – the FATAL lock-ordering regression guard,
//!      FIXED: the reserve runs on a SEPARATE task whose JoinHandle is `timeout`ed,
//!      so a self-deadlock (re-acquiring the inner read lock under the held write
//!      lock) fires the bespoke DEADLOCK message instead of wedging the runtime.
//!  hold_fires_for_p_saturated_holder – the core positive: P-saturated W (k=1),
//!      T_wait_W < T_setup, not overdue, p_gate_active, cold X → None + count +1.
//!  no_hold_when_p_gate_inactive – the distsys BLOCK-2 regression guard: with
//!      `p_gate_active=false` (flag off / Phase-2 lift) the gate must NOT hold even
//!      with a P-saturated holder (it would violate pcore-first spread-under-sat).
//!  no_hold_over_partial_locality_x – C1: X has subtree/blob locality → no hold.
//!  deep_backlog_declines – W running=352, p_core_count=4 → k≈349 → T_wait ≫ T_setup
//!      → no hold (proves the just-saturated-band reachability boundary).
//!  k_th_completion_frees_one_p_slot – the k-th order-statistic correctness.
//!  no_hold_on_overdue_w – W's k-soonest action past its estimate → NO hold.
//!  no_hold_when_x_is_holder – X itself holds the root (idle) → no hold (X wins).
//!  overdue_boundary_elapsed_equals_estimate – elapsed == estimate is NOT overdue.
//!  subtree_holder_disjunct – a P-saturated SUBTREE holder is a valid hold target.
//!  max_hold_cap_abandons / hold_max_cycles_arm – the two starvation-cap arms.
//!  defensive_reap_on_reroute / defensive_reap_on_evict – both defensive reaps.
//!  do_try_match_none_requeue_seam – the production `do_try_match` None re-queue.
//!  flag_off_no_hold – `enable_speculative_hold=false` → identical to today.
//!  p_core_count_zero_no_candidate – A5 legacy worker is never a hold candidate.
//!  hold_paid_off_counter / hold_regret_counter – the two landing-outcome counters.

use core::sync::atomic::Ordering;
use core::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mock_instant::thread_local::MockClock;
use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::{ActionInfoWithProps, Worker};
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

mod utils {
    pub(crate) mod scheduler_utils;
}

use utils::scheduler_utils::make_base_action_info;

/// Mock-clock base so exec-start stamps (`UNIX_EPOCH + MockClock::time()`) are
/// coherent with the worker keepalive timestamp passed to `Worker::new*`.
const NOW_TIME: u64 = 10_000;

/// `DEFAULT_DURATION_ESTIMATE` (api_worker_scheduler) = 30 s, the per-action
/// estimate before any completion. `T_SETUP` = 3 s. On a just-P-saturated W
/// (`running == p_core_count` → k=1) `T_wait_W = 30 s − elapsed` (the MIN
/// remaining); advancing the clock to `elapsed = 28 s` yields `T_wait_W = 2 s <
/// T_SETUP` while NOT overdue (28 s < 30 s). These constants mirror the production
/// values under test (asserted at their declaration site by the numeric-const
/// block, not here).
const DEFAULT_ESTIMATE_SECS: u64 = 30;
const ELAPSED_UNDER_ESTIMATE_SECS: u64 = 28; // → T_wait_W = 2 s (< T_SETUP = 3 s)

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

/// Build a `SimpleSpec` with the temporal hold gate enabled AND the pcore-first
/// P-headroom gate enabled — the v3 enablement (design §2.3-v3.6: Stage B rides
/// pcore-first, gated on `p_gate_active`). Stage-A prefetch stays off (the hold
/// gate is exercised directly via `find_and_reserve_worker`).
fn spec_with_hold(enable_hold: bool) -> SimpleSpec {
    SimpleSpec {
        enable_speculative_hold: enable_hold,
        // v3: the hold gates on `p_gate_active`, which requires the pcore-first
        // gate ON (design §2.3-v3.7-#1). With it OFF, W is not excluded from the
        // cache tiers and no hold is ever needed.
        p_headroom_gate_enabled: enable_hold,
        ..Default::default()
    }
}

/// Add a worker with a specific `p_core_count` (via `new_with_cas_endpoint`, whose
/// counts ride the connect frame in prod) and unlimited slots (`max_inflight=0`,
/// the live-fleet shape). Reports a low load so it is a viable, non-saturated
/// candidate (a never-reported worker is treated as saturated by the
/// `#sched-zeroload` gate, which would make the cache tiers fall through to
/// LRU/MRU — masking the P-saturation distinction the v3 gate depends on).
async fn add_worker_pcores(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    p_core_count: u32,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new_with_cas_endpoint(
        worker_id.clone(),
        PlatformProperties::default(),
        tx,
        NOW_TIME,
        0, // unlimited slots — the live fleet runs max_inflight_tasks=0
        String::new(),
        p_core_count,
        p_core_count, // e_core_count mirrors; irrelevant to the P gate
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    drop(rx.recv().await); // drain ConnectionResult
    scheduler
        .update_worker_load(&worker_id, 10, 10, 10)
        .await
        .err_tip(|| "add_worker_pcores: update_worker_load failed")?;
    Ok(rx)
}

/// Add an IDLE worker (`p_core_count=4`, unlimited slots, no cached digests,
/// running 0 → always has P-headroom) reporting a low load so it is a viable,
/// non-saturated candidate with P-headroom (so `p_gate_active` can be true and W
/// is the one excluded, not X).
async fn add_idle_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    add_worker_pcores(scheduler, worker_id, 4).await
}

/// A per-(worker,index) UNIQUE filler input-root, so each filler lands on its
/// intended worker via Tier-1 exact match (only that worker holds this digest)
/// regardless of which other idle workers exist.
fn unique_filler_root(worker_id: &WorkerId, idx: u32) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = 0xF0;
    h[1] = idx as u8;
    for (i, b) in worker_id.0.bytes().take(30).enumerate() {
        h[i + 2] = b;
    }
    DigestInfo::new(h, 7)
}

/// Make W P-SATURATED by reserving exactly `p_core_count` fillers onto it (so
/// `running == p_core_count` → k=1 → `!worker_has_p_headroom` at the default
/// threshold), and give it the op's `input_root` in its directory cache (the
/// Tier-1 holder signal the v3 `find_p_saturated_holder` reads). Each filler
/// carries a per-index UNIQUE root that only W holds → Tier-1 exact match lands it
/// on W (W keeps P-headroom until the last filler, so it wins each one) without
/// disturbing any idle peer. Fillers' exec-starts are stamped at the current
/// `MockClock::time()`, so advancing the clock drives `T_wait_W`.
///
/// `also_subtree`: additionally register `input_root` as a SUBTREE digest (for the
/// subtree-disjunct test); `false` for the common directory-holder case.
async fn add_p_saturated_holder(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    input_root: DigestInfo,
    p_core_count: u32,
    also_subtree: bool,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let props = PlatformProperties::default();
    let rx = add_worker_pcores(scheduler, worker_id.clone(), p_core_count).await?;
    // W's directory cache: the op root + every unique filler root.
    let mut dirs: HashSet<DigestInfo> = HashSet::new();
    if !also_subtree {
        dirs.insert(input_root);
    }
    for i in 0..p_core_count {
        dirs.insert(unique_filler_root(&worker_id, i));
    }
    scheduler
        .worker_scheduler_for_test()
        .update_cached_directories(&worker_id, dirs)
        .await
        .err_tip(|| "add_p_saturated_holder: update_cached_directories failed")?;
    if also_subtree {
        scheduler
            .worker_scheduler_for_test()
            .update_cached_subtrees(&worker_id, false, vec![], vec![input_root], vec![])
            .await
            .err_tip(|| "add_p_saturated_holder: update_cached_subtrees failed")?;
    }
    // Reserve p_core_count fillers → running climbs to p_core_count (P-saturated).
    for i in 0..p_core_count {
        let filler_root = unique_filler_root(&worker_id, i);
        let filler_op = OperationId::default();
        let filler_ai = {
            let mut ai =
                make_base_action_info(make_system_time(1), DigestInfo::new([0xE0 + i as u8; 32], 7));
            Arc::make_mut(&mut ai).input_root_digest = filler_root;
            ActionInfoWithProps {
                inner: ai,
                platform_properties: props.clone(),
            }
        };
        let (assigned, _, _) = scheduler
            .worker_scheduler_for_test()
            .find_and_reserve_worker(&props, &filler_op, &filler_ai, false)
            .await
            .expect(
                "add_p_saturated_holder precondition: each filler (carrying a unique root) must \
                 reserve onto W via Tier-1 exact match (W still has P-headroom during the fill)",
            );
        assert_eq!(
            assigned, worker_id,
            "add_p_saturated_holder precondition: filler {i} must land on W via its unique root, \
             not an idle peer — got {assigned:?}"
        );
    }
    // W is now P-saturated (running == p_core_count).
    assert_eq!(
        scheduler
            .worker_scheduler_for_test()
            .worker_running_action_count_for_test(&worker_id)
            .await,
        Some(p_core_count as usize),
        "add_p_saturated_holder postcondition: W must hold exactly p_core_count running actions \
         (P-saturated, k=1)"
    );
    Ok(rx)
}

/// Build an `ActionInfoWithProps` carrying `input_root`.
fn action_with_root(input_root: DigestInfo, action_hash: u8) -> ActionInfoWithProps {
    let mut inner = make_base_action_info(make_system_time(1), DigestInfo::new([action_hash; 32], 1));
    Arc::make_mut(&mut inner).input_root_digest = input_root;
    ActionInfoWithProps {
        inner,
        platform_properties: PlatformProperties::default(),
    }
}

/// Construct a scheduler with the hold-gate spec, mock-clock-anchored.
fn build_hold_scheduler(enable_hold: bool) -> Arc<SimpleScheduler> {
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(enable_hold);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );
    scheduler
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_no_deadlock_under_write_lock — the FATAL lock-ordering regression guard.
// ─────────────────────────────────────────────────────────────────────────────
//
// The design's fatal finding (§2.3.1): the gate runs INSIDE
// `inner_find_and_reserve_worker` under the `self.inner.write()` held by
// `find_and_reserve_worker`. It MUST compute `T_wait_W` via the LOCK-FREE
// `t_wait_w_locked` core on the already-borrowed `inner` — NOT via the async
// `worker_time_to_free` (whose first line re-acquires `self.inner.read()` on the
// SAME non-reentrant tokio `RwLock`, self-deadlocking the WHOLE match cycle: a
// silent HANG, not a panic).
//
// The v2 form of this test `.await`ed the reserve INLINE inside a `timeout`. That
// wedges under the mutation: when the reserve self-deadlocks (holds write, blocks
// on read) the current-thread runtime cannot make progress on the timeout's own
// Sleep (empirically confirmed 2026-07-06 — a mutated inline-await reserve did not
// return within 2 minutes despite a 5 s internal `timeout`). The FIX runs the
// reserve on a SEPARATE spawned task and `timeout`s its `JoinHandle`: the
// deadlocked task parks holding the lock, but the MAIN task's `timeout(handle)` is
// an independent future the runtime CAN drive, so its Sleep fires and the bespoke
// DEADLOCK message below is what red-fails on the mutation.
//
// MUTATION (reproduces the fatal finding, GATE-PATH-SCOPED so it fires here and not
// in un-timed setup): in `find_and_reserve_worker`, right after the
// `inner.inner_find_and_reserve_worker(...)` call, insert
// `if result.is_none() { let _ = self.worker_time_to_free(&W).await; }` — re-acquire
// the read lock while the write lock (`inner`) is held, ONLY on the hold path (None).
// (An UNCONDITIONAL insert after `self.inner.write().await` instead deadlocks the
// setup fillers, which are NOT timeout-wrapped, so the test wedges before reaching
// the assertion — the conditional keeps the deadlock on the timeout-wrapped reserve.)
// Verified 2026-07-06: with this mutation the JoinHandle timeout fires at 5.01 s and
// the `expect("DEADLOCK …")` red-fails; the gate's lock-free t_wait_w_locked keeps it
// green.
#[nativelink_test]
async fn hold_no_deadlock_under_write_lock() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);

    let input_root = DigestInfo::new([0xB1; 32], 100);
    // W: P-saturated holder of R (stamps filler exec-starts at MockClock base).
    let w = WorkerId("hold_deadlock_w".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    // X: idle, cold (does not hold R), has P-headroom → dir_cache_winner is None
    // and p_gate_active is true → the gate reaches find_p_saturated_holder →
    // t_wait_w_locked(W) under the write lock.
    let x = WorkerId("hold_deadlock_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // Advance so T_wait_W(W) = 30 − 28 = 2 s < T_SETUP (3 s) → the gate takes the
    // HOLD branch (return None), exercising the full gate path under the lock.
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x11);

    // Run the reserve on a SEPARATE task and timeout its JoinHandle: if the gate
    // self-deadlocks the spawned task parks holding the lock, and THIS task's
    // timeout still fires (independent future) → the bespoke message below.
    let sched_c = Arc::clone(&scheduler);
    let handle = tokio::spawn(async move {
        let ws = sched_c.worker_scheduler_for_test();
        ws.find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
            .await
    });
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect(
            "DEADLOCK: find_and_reserve_worker did not complete within 5 s — the temporal hold gate \
             re-acquired the inner RwLock (read) while the write lock was held, self-deadlocking the \
             match cycle. The gate MUST use the lock-free t_wait_w_locked core, NEVER \
             worker_time_to_free (design §2.3.1 fatal finding).",
        )
        .expect("reserve task panicked");

    // With T_wait_W < T_SETUP the gate HOLDS → None (the op re-queues). The point
    // of this test is that it RETURNED at all; the None is the expected outcome.
    assert!(
        result.is_none(),
        "hold_no_deadlock: with a P-saturated holder and T_wait_W < T_setup the gate must HOLD \
         (return None). Got a reservation instead — the hold branch did not fire."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_fires_for_p_saturated_holder — the core positive (v3).
// ─────────────────────────────────────────────────────────────────────────────
//
// §2.3-v3: P-saturated holder W (running == p_core_count → k=1) with T_wait_W (the
// MIN remaining) < T_setup and not overdue, p_gate_active true (X has headroom),
// and the best available X is cold (dir/subtree/locality winners all None) → the
// gate HOLDS: reserve returns None (op re-queues) and `speculative_hold_count` +1.
//
// MUTATION: skip the gate (comment out the whole `if self.enable_speculative_hold
// && p_gate_active …` block in `inner_find_and_reserve_worker`) → the op is
// assigned to X → `result` is Some and `speculative_hold_count` stays 0 → both
// assertions red-fail.
#[nativelink_test]
async fn hold_fires_for_p_saturated_holder() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB2; 32], 200);
    let w = WorkerId("hold_fire_w".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let x = WorkerId("hold_fire_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x22);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    assert!(
        result.is_none(),
        "hold_fires: a P-saturated holder W (k=1) with T_wait_W (2 s) < T_setup (3 s), not overdue, \
         p_gate_active, and a cold X available → the gate MUST HOLD (return None so the op \
         re-queues). Got a reservation — the gate did not fire (skipping the gate makes this Some)."
    );
    let hold_after = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    assert_eq!(
        hold_after - hold_before,
        1,
        "hold_fires: speculative_hold_count did not increment by 1 on a hold — the hold-count \
         instrument (soak `hold_count`) is dark. before={hold_before} after={hold_after}"
    );
    // The op is still queued: X did NOT receive it (peek-only gate).
    assert_eq!(
        ws.worker_running_action_count_for_test(&x).await,
        Some(0),
        "hold_fires: idle X gained a running action — a hold must NOT reserve any worker (it \
         returns the existing no-match None so the op re-queues)."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// no_hold_when_p_gate_inactive — the distsys BLOCK-2 regression guard (v3.7-#1).
// ─────────────────────────────────────────────────────────────────────────────
//
// The hold is the TEMPORAL exception to pcore-first's SPATIAL route-away, so it may
// only fire when `p_gate_active` (flag ON AND some viable worker has P-headroom →
// W is genuinely being routed-away-from). On a FULLY-P-saturated fleet the gate
// LIFTS (`p_gate_active=false`) and pcore-first SPREADS work — the hold must NOT
// fire then, or it violates the spread-under-saturation invariant.
//
// To make `p_gate_active` the SOLE discriminator (isolating it from the C1
// dir/subtree/locality guards, which — when p_gate_active is false — would ALSO
// block the hold because a P-saturated root-holder is no longer cache-excluded and
// wins its tier): drive `saturation_fall_through` by making EVERY viable worker
// report SATURATED LOAD (100%). Then ALL cache tiers decline (all three winners are
// None → C1 passes trivially), the fleet is P-saturated (`p_gate_active=false`), and
// a P-saturated root-holder W sits in the already-materialized `p_gated_excluded`
// set. The op must be assigned via the LRU/MRU fallback (unlimited slots accept
// work), NOT held. Loads are reported LOW during the filler fill (so the fillers
// land via Tier-1) and bumped to 100 AFTER, so only the op-under-test sees the
// fall-through.
//
// MUTATION (distsys BLOCK-2): remove the `&& p_gate_active` term from the gate
// condition. With C1 passing (winners None) the gate then finds W in
// `p_gated_excluded` and HOLDS on this Phase-2-lifted fleet → this red-fails with
// the bespoke spread-under-saturation message below. (This is the ONE composition
// where p_gate_active is the sole guard — verified 2026-07-06 that the mutation
// red-fails here and that C1 alone does NOT catch it.)
#[nativelink_test]
async fn no_hold_when_p_gate_inactive() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB9; 32], 900);
    // W: P-saturated holder of R (fillers land at low load).
    let w = WorkerId("pgate_w_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    // Z: the ONLY alternative, ALSO P-saturated (cold, does not hold R).
    let z = WorkerId("pgate_z_saturated_cold".to_string());
    let z_cold_root = DigestInfo::new([0xC9; 32], 909);
    let _rxz = add_p_saturated_holder(&scheduler, z.clone(), z_cold_root, 4, false).await?;

    // Now saturate BOTH workers' reported load → cap_score saturated → the op's
    // reserve takes `saturation_fall_through` (all cache tiers decline → winners
    // None). The workers stay P-saturated (running≥p_core_count) → p_gate_active
    // false → the hold must be blocked by p_gate_active alone.
    ws.update_worker_load(&w, 100, 100, 100)
        .await
        .err_tip(|| "pgate: saturate W load failed")?;
    ws.update_worker_load(&z, 100, 100, 100)
        .await
        .err_tip(|| "pgate: saturate Z load failed")?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x99);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    assert!(
        result.is_some(),
        "no_hold_when_p_gate_inactive: on a FULLY-P-saturated fleet (no viable worker has \
         P-headroom) p_gate_active is false, so pcore-first LIFTS and spreads work — the hold gate \
         must NOT fire (it would violate the pcore-first spread-under-saturation invariant). Got \
         None: the gate was mistakenly gated on the flag, not p_gate_active (distsys BLOCK-2)."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "no_hold_when_p_gate_inactive: speculative_hold_count moved with p_gate_active=false — the \
         gate held on a Phase-2-lifted fleet, violating spread-under-saturation (distsys BLOCK-2). \
         before={hold_before}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// no_hold_when_gossip_subtree_holder_x — C1 via the dir_cache_winner subtree arm.
// ─────────────────────────────────────────────────────────────────────────────
//
// The hold only pays when X is a genuinely COLD reconstruct. A worker holding the
// op's root in its GOSSIPED `cached_subtree_digests` is a Tier-1 `has_subtree_match`
// → it is a `dir_cache_winner` (Tier-1 fires on `has_root_match || has_subtree_
// match`) and can hardlink the tree as a subtree of an already-cached tree — a cheap
// rebind, NOT a cold reconstruct. So the gate must NOT hold for a P-saturated holder
// W when such an X is available: the `dir_cache_winner.is_none()` C1 guard blocks it.
// Here X is idle with R in `cached_subtree_digests` (→ dir_cache_winner = X) while a
// P-saturated holder W also exists → no hold: X wins Tier-1.
//
// NOTE: the SEPARATE Tier-1.5 `subtree_coverage_winner` and Tier-2 `locality_winner`
// C1 guards need a `resolved_tree`/`endpoint_scores` to be reachable (the integration
// `find_and_reserve_worker` path derives those from a locality_map that is not wired
// here). They are covered by the lib test `sched_blend::c1_locality_winner_blocks_
// hold`, which drives `inner_find_and_reserve_worker` with an explicit
// `endpoint_scores`. This test covers the `has_subtree_match` → dir_cache_winner arm.
//
// MUTATION: drop the `dir_cache_winner.is_none()` term from the gate condition → the
// op is HELD despite a gossip-subtree holder X being available (a cheap rebind) →
// this red-fails (None instead of assigning X).
#[nativelink_test]
async fn no_hold_when_gossip_subtree_holder_x() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB4; 32], 400);
    // W: P-saturated DIRECTORY holder of R (excluded from Tier-1 when p_gate_active).
    let w = WorkerId("c1_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    // X: IDLE with R in cached_subtree_digests → Tier-1 has_subtree_match →
    // dir_cache_winner = X (a cheap subtree hardlink, not a cold reconstruct).
    let x = WorkerId("c1_x_idle_subtree".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    ws.update_cached_subtrees(&x, false, vec![], vec![input_root], vec![])
        .await
        .err_tip(|| "no_hold_when_gossip_subtree_holder_x: update_cached_subtrees(X) failed")?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x44);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "no_hold_when_gossip_subtree_holder_x: an IDLE worker X holds R in its gossiped \
         cached_subtree_digests → a Tier-1 has_subtree_match (dir_cache_winner = X), a cheap \
         subtree hardlink, not a cold reconstruct → the gate must NOT hold (C1). Got None: the \
         dir_cache_winner.is_none() guard was dropped, so the op was needlessly held.",
    );
    assert_eq!(
        assigned, x,
        "no_hold_when_gossip_subtree_holder_x: the op landed on {assigned:?}, expected the idle \
         gossip-subtree holder X."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "no_hold_when_gossip_subtree_holder_x: the gate held despite a gossip-subtree holder X \
         (C1 dir_cache_winner arm violated)."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// deep_backlog_declines — the just-saturated-band reachability boundary (v3.7-#4).
// ─────────────────────────────────────────────────────────────────────────────
//
// `running` is UNCAPPED (Phase-2 lift + max_inflight=0 → running up to 352, the
// domino incident). k = running − p_core_count + 1 grows with the backlog; the k-th
// SOONEST completion of a deeply-backlogged W is ≫ T_setup, so the hold DECLINES
// (a deep-backlog W is NOT worth waiting for). Here W runs 8 actions at
// p_core_count=4 → k = 5 → the 5th-soonest completion. With all 8 fillers stamped
// at the SAME clock and elapsed 28 s, every remaining is 2 s, so k-th = 2 s < T_setup
// — that would still HOLD. To prove the DECLINE we need the k-th to exceed T_setup:
// stamp the first 4 fillers LONG ago (nearly done, remaining ~0) and the last 4
// FRESH (remaining ~full estimate); then the 4 soonest are the old ones and the
// 5th-soonest (k=5) is a fresh one (remaining ≫ T_setup) → decline.
//
// MUTATION: use MIN-remaining (k=1) instead of the k-th → the 1st-soonest is a
// nearly-done old filler (remaining ~0 < T_setup) → the gate would HOLD → this
// red-fails (Some expected, None seen).
#[nativelink_test]
async fn deep_backlog_declines() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBA; 32], 1000);
    let w = WorkerId("deep_w".to_string());
    let p_core_count = 4u32;
    // Manually build a DEEP backlog: 4 OLD fillers (stamped now, will age to nearly
    // done) + 4 FRESH fillers (stamped after the age advance). Bypass the
    // add_p_saturated_holder helper (which stamps all at one instant) so the
    // remainings are bimodal — 4 near-0 and 4 near-full.
    let _rxw = add_worker_pcores(&scheduler, w.clone(), p_core_count).await?;
    // W's directory cache: the op root + 8 unique filler roots.
    let mut dirs: HashSet<DigestInfo> = HashSet::from([input_root]);
    for i in 0..8u32 {
        dirs.insert(unique_filler_root(&w, i));
    }
    ws.update_cached_directories(&w, dirs)
        .await
        .err_tip(|| "deep_backlog: update_cached_directories failed")?;
    // Reserve the first 4 fillers (OLD) at the base clock.
    for i in 0..4u32 {
        let filler_root = unique_filler_root(&w, i);
        let filler_op = OperationId::default();
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xD0 + i as u8; 32], 7));
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        let filler_ai = ActionInfoWithProps { inner: ai, platform_properties: PlatformProperties::default() };
        let (assigned, _, _) = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &filler_op, &filler_ai, false)
            .await
            .expect("deep_backlog: old filler must reserve onto W");
        assert_eq!(assigned, w, "deep_backlog: old filler {i} must land on W");
    }
    // Age the 4 old fillers to nearly done (remaining ~2 s, well < T_setup).
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));
    // Reserve 4 more (FRESH) fillers — remaining ~full estimate (≫ T_setup).
    for i in 4..8u32 {
        let filler_root = unique_filler_root(&w, i);
        let filler_op = OperationId::default();
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xD0 + i as u8; 32], 7));
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        let filler_ai = ActionInfoWithProps { inner: ai, platform_properties: PlatformProperties::default() };
        let (assigned, _, _) = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &filler_op, &filler_ai, false)
            .await
            .expect("deep_backlog: fresh filler must reserve onto W");
        assert_eq!(assigned, w, "deep_backlog: fresh filler {i} must land on W");
    }
    // W now runs 8 → k = 8 − 4 + 1 = 5. The 4 soonest are the OLD (remaining ~2 s);
    // the 5th-soonest (k) is a FRESH one (remaining ~full ≫ T_setup) → decline.
    assert_eq!(
        ws.worker_running_action_count_for_test(&w).await,
        Some(8),
        "deep_backlog: W must run 8 actions (k = 8 − 4 + 1 = 5)"
    );
    let x = WorkerId("deep_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xAA);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "deep_backlog_declines: W runs a DEEP backlog (running=8, p_core_count=4 → k=5); the \
         5th-soonest completion is ≫ T_setup, so W will NOT regain a P-slot before X could \
         reconstruct → the gate must NOT hold (it must assign the cold X). Got None: T_wait_W used \
         MIN-remaining (k=1, a nearly-done old filler) instead of the k-th order statistic.",
    );
    assert_eq!(
        assigned, x,
        "deep_backlog_declines: the op must land on the idle X, got {assigned:?}."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "deep_backlog_declines: the gate held on a deep-backlog W whose k-th completion ≫ T_setup."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// k_th_completion_frees_one_p_slot — the k-th order-statistic correctness (v3.3).
// ─────────────────────────────────────────────────────────────────────────────
//
// `T_wait_W` = the k-th SOONEST completion (k = running − p_core_count + 1) = the
// time until ONE P-slot frees (the moment `running` drops back to `p_core_count −
// 1` < p_core_count → W regains headroom). Here W runs 6 at p_core_count=4 → k = 3.
// Fillers are stamped in two waves so their remainings are DISTINCT and ordered:
// 2 OLD (remaining ~2 s) + 4 FRESH (remaining ~30 s). Sorted ascending the
// remainings are [~2, ~2, ~30, ~30, ~30, ~30]; the 3rd-soonest (k=3) is ~30 s ≥
// T_setup → the hold DECLINES. A SUM would be ~124 s (also ≥ T_setup, decline — not
// discriminating); a MIN (k=1) would be ~2 s < T_setup → HOLD. So this asserts the
// gate does NOT hold, and the discriminating mutation is MIN.
//
// MUTATION: compute T_wait_W as MIN-remaining (k=1) → the 1st-soonest (~2 s) <
// T_setup → HOLD (None) → red-fails (Some/X expected). (The SUM mutation is caught
// by the Stage-C `worker_time_to_free` unit test migrated to k-th semantics.)
#[nativelink_test]
async fn k_th_completion_frees_one_p_slot() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBB; 32], 1100);
    let w = WorkerId("kth_w".to_string());
    let p_core_count = 4u32;
    let _rxw = add_worker_pcores(&scheduler, w.clone(), p_core_count).await?;
    let mut dirs: HashSet<DigestInfo> = HashSet::from([input_root]);
    for i in 0..6u32 {
        dirs.insert(unique_filler_root(&w, i));
    }
    ws.update_cached_directories(&w, dirs)
        .await
        .err_tip(|| "kth: update_cached_directories failed")?;
    // 2 OLD fillers at base.
    for i in 0..2u32 {
        let filler_root = unique_filler_root(&w, i);
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xC0 + i as u8; 32], 7));
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        let filler_ai = ActionInfoWithProps { inner: ai, platform_properties: PlatformProperties::default() };
        let (assigned, _, _) = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &OperationId::default(), &filler_ai, false)
            .await
            .expect("kth: old filler must reserve onto W");
        assert_eq!(assigned, w);
    }
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));
    // 4 FRESH fillers.
    for i in 2..6u32 {
        let filler_root = unique_filler_root(&w, i);
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xC0 + i as u8; 32], 7));
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        let filler_ai = ActionInfoWithProps { inner: ai, platform_properties: PlatformProperties::default() };
        let (assigned, _, _) = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &OperationId::default(), &filler_ai, false)
            .await
            .expect("kth: fresh filler must reserve onto W");
        assert_eq!(assigned, w);
    }
    assert_eq!(
        ws.worker_running_action_count_for_test(&w).await,
        Some(6),
        "kth: W must run 6 (k = 6 − 4 + 1 = 3)"
    );
    let x = WorkerId("kth_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xBC);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "k_th_completion_frees_one_p_slot: W runs 6 at p_core_count=4 → k=3; sorted remainings are \
         [~2,~2,~30,~30,~30,~30], so the 3rd-soonest (the k-th, = time until ONE P-slot frees) is \
         ~30 s ≥ T_setup → the gate must NOT hold. Got None: T_wait_W used MIN-remaining (k=1, ~2 s) \
         instead of the k-th order statistic — it mis-modelled a deep-backlog W as about to free a \
         P-slot.",
    );
    assert_eq!(assigned, x, "kth: the op must land on the idle X, got {assigned:?}.");
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "kth: the gate held when the k-th (not the min) completion is ≥ T_setup."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// no_hold_on_overdue_w — the bimodal-risk refusal (§2.3.4), v3 over the k-soonest.
// ─────────────────────────────────────────────────────────────────────────────
//
// The single GLOBAL duration EWMA mis-estimates on a bimodal fleet. The gate
// REFUSES to hold on an OVERDUE W (any of the k-SOONEST inflight actions has
// `elapsed > estimate`): ambiguous (about-to-finish → take X, OR a mis-estimated
// long action → holding is wrong and regresses p99), so it does not bet. Here W is
// just-P-saturated (k=1) and its soonest filler has run PAST the 30 s estimate
// (elapsed 31 s) → overdue → NO hold → the op is assigned to idle X.
//
// MUTATION: drop the `!overdue` guard in the gate (accept a hold on an overdue W)
// → the op is HELD (None) instead of assigned to X → this red-fails with the
// bespoke bimodal-risk message below.
#[nativelink_test]
async fn no_hold_on_overdue_w() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB3; 32], 300);
    let w = WorkerId("overdue_w".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let x = WorkerId("overdue_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // Advance PAST the estimate: elapsed = 31 s > 30 s → W's k-soonest is OVERDUE.
    MockClock::advance(Duration::from_secs(DEFAULT_ESTIMATE_SECS + 1));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x33);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "no_hold_on_overdue_w: W's k-soonest action is OVERDUE (elapsed 31 s > estimate 30 s), so \
         the gate must NOT hold — it must assign the op to the idle X. Got None: the overdue guard \
         was dropped, so the op was held on a worker whose estimate is unreliable (the bimodal \
         p99-regression risk the guard exists to prevent).",
    );
    assert_eq!(
        assigned, x,
        "no_hold_on_overdue_w: the op landed on {assigned:?}, expected idle X — an overdue W must \
         not capture the op."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "no_hold_on_overdue_w: speculative_hold_count moved on an OVERDUE holder — the gate held \
         when it must have refused. before={hold_before}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// overdue_boundary_elapsed_equals_estimate — the overdue boundary (v3.7-#4).
// ─────────────────────────────────────────────────────────────────────────────
//
// `overdue` is `elapsed > estimate` (STRICT), so `elapsed == estimate` is NOT
// overdue and `T_wait_W = estimate − elapsed = 0 < T_setup` → the gate HOLDS. This
// pins the boundary: at exactly the estimate the action is treated as about-to-free
// (remaining 0), not as a mis-estimated overdue action. Here W is just-P-saturated
// (k=1) and its soonest filler's elapsed is EXACTLY 30 s.
//
// MUTATION: change the overdue test from `elapsed > estimate` to `elapsed >=
// estimate` → the boundary action becomes overdue → NO hold → this red-fails (None
// expected, Some seen).
#[nativelink_test]
async fn overdue_boundary_elapsed_equals_estimate() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBD; 32], 1300);
    let w = WorkerId("boundary_w".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let x = WorkerId("boundary_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // Advance to EXACTLY the estimate: elapsed = 30 s == estimate → NOT overdue,
    // remaining = 0 < T_setup → HOLD.
    MockClock::advance(Duration::from_secs(DEFAULT_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xDE);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    assert!(
        result.is_none(),
        "overdue_boundary: at elapsed == estimate (30 s) the action is NOT overdue (overdue is the \
         STRICT elapsed > estimate) and its remaining is 0 < T_setup → the gate must HOLD. Got a \
         reservation: the overdue test was widened to `elapsed >= estimate`, wrongly refusing a \
         boundary action that is about to free."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed) - hold_before,
        1,
        "overdue_boundary: the boundary hold did not bump speculative_hold_count."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// subtree_holder_disjunct — a P-saturated SUBTREE holder is a valid target.
// ─────────────────────────────────────────────────────────────────────────────
//
// `find_p_saturated_holder` matches a root held as a DIRECTORY *or* as a SUBTREE
// (`cached_directory_digests.contains || cached_subtree_digests.contains`). Here W
// holds R ONLY as a subtree (not a directory) and is P-saturated → it is still a
// valid hold target → the gate HOLDS.
//
// MUTATION: drop the `|| cached_subtree_digests.contains(...)` disjunct from the
// holder finder → W is no longer recognized as a holder → NO hold → this red-fails
// (None expected, Some seen).
#[nativelink_test]
async fn subtree_holder_disjunct() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBE; 32], 1400);
    let w = WorkerId("subtree_w".to_string());
    // also_subtree=true: W holds R as a SUBTREE digest, NOT a directory digest.
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, true).await?;
    let x = WorkerId("subtree_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xEF);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    assert!(
        result.is_none(),
        "subtree_holder_disjunct: W holds R as a SUBTREE digest and is P-saturated, so it is a \
         valid hold target (the holder finder matches directory OR subtree) → the gate must HOLD. \
         Got a reservation: the subtree disjunct was dropped from find_p_saturated_holder."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed) - hold_before,
        1,
        "subtree_holder_disjunct: the subtree-holder hold did not bump speculative_hold_count."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// no_hold_when_x_is_holder — condition (a) (§2.3.1).
// ─────────────────────────────────────────────────────────────────────────────
//
// The gate holds only when the best available X is NOT itself a good-locality
// holder of the root (`dir_cache_winner.is_none()`). If a VIABLE (idle) worker
// already holds the root as a DIRECTORY, it wins Tier-1 and can hardlink now —
// there is no reason to wait. Here X is idle AND holds R (so dir_cache_winner = X)
// while a separate P-saturated holder W also exists → no hold: X wins.
//
// MUTATION: remove the `dir_cache_winner.is_none()` guard from the gate condition
// → the op is HELD even though an idle holder was available → this red-fails (the
// reserve returns None instead of assigning X).
#[nativelink_test]
async fn no_hold_when_x_is_holder() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB7; 32], 700);
    // A P-saturated holder W of R also exists (so the gate WOULD have a W to hold for).
    let w = WorkerId("xh_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    // X: IDLE and holds R as a directory → it is the Tier-1 dir_cache_winner.
    let x = WorkerId("xh_x_idle_holder".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    ws.update_cached_directories(&x, HashSet::from([input_root]))
        .await
        .err_tip(|| "no_hold_when_x_is_holder: update_cached_directories(X) failed")?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x77);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "no_hold_when_x_is_holder: an IDLE worker X holds the root as a directory, so it wins \
         Tier-1 and can hardlink now — the gate must NOT hold (condition (a): X is a good-locality \
         holder). Got None: the dir_cache_winner.is_none() guard was dropped, so the op was \
         needlessly held.",
    );
    assert_eq!(
        assigned, x,
        "no_hold_when_x_is_holder: the op landed on {assigned:?}, expected the idle holder X."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "no_hold_when_x_is_holder: the gate held despite an idle root-holder being available."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// max_hold_cap_abandons — the starvation bound, WALL-TIME arm (§2.3.4).
// ─────────────────────────────────────────────────────────────────────────────
//
// A held op must not hold INDEFINITELY. The max-hold cap trips on wall-time
// (`HOLD_MAX_WALL`, 10 s); on trip the gate ABANDONS the hold and reserves the best
// available (cold) X, bumping `hold_expired`. Cycle 1 holds the op (records
// first_hold_at); the clock is advanced past HOLD_MAX_WALL; cycle 2 uses a FRESH
// just-P-saturated holder W2 with T_wait_W < T_setup so the ONLY reason the second
// reserve does not hold is the wall-time cap. The op's hold record persists across
// the W→W2 swap (keyed on the op, not the worker).
//
// MUTATION: remove the wall-time arm (`held_for >= HOLD_MAX_WALL`) from the cap
// check → the op is HELD again (None on the second reserve) → this red-fails
// (expected Some/X).
#[nativelink_test]
async fn max_hold_cap_abandons() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB5; 32], 500);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x55);

    // ── Cycle 1: hold the op (records first_hold_at at the current clock) ──
    let w1 = WorkerId("cap_w1_psat_holder".to_string());
    let _rxw1 = add_p_saturated_holder(&scheduler, w1.clone(), input_root, 4, false).await?;
    let x = WorkerId("cap_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));
    let first = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    assert!(
        first.is_none(),
        "max_hold_cap: cycle-1 reserve must HOLD (record the op's first_hold_at) — got a \
         reservation, so the setup did not produce a hold."
    );
    let expired_before = ws.get_metrics().hold_expired.load(Ordering::Relaxed);

    // ── Advance the wall past HOLD_MAX_WALL (10 s) since first_hold_at ──
    MockClock::advance(Duration::from_secs(20));

    // ── Cycle 2: a FRESH just-P-saturated holder W2 with T_wait_W < T_setup, so the
    // ONLY reason not to hold is the cap. W2's fillers are stamped NOW; advance so
    // their elapsed = 28 s → T_wait_W(W2) = 2 s < T_setup. ──
    let w2 = WorkerId("cap_w2_psat_holder".to_string());
    let _rxw2 = add_p_saturated_holder(&scheduler, w2.clone(), input_root, 4, false).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let second = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    let (assigned, _, _) = second.expect(
        "max_hold_cap: after HOLD_MAX_WALL of wall-time held, the gate must ABANDON the hold and \
         reserve the best available worker (the starvation bound). Got None: the wall-time cap arm \
         was removed, so a held op holds indefinitely under sustained backlog.",
    );
    assert_eq!(
        assigned, x,
        "max_hold_cap: on cap expiry the op must reserve the idle X (the best available), got \
         {assigned:?}."
    );
    let expired_after = ws.get_metrics().hold_expired.load(Ordering::Relaxed);
    assert_eq!(
        expired_after - expired_before,
        1,
        "max_hold_cap: hold_expired did not increment on cap expiry — the starvation-bound \
         instrument is dark. before={expired_before} after={expired_after}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_max_cycles_arm — the starvation bound, CYCLE-COUNT arm (§2.3.4).
// ─────────────────────────────────────────────────────────────────────────────
//
// The SECOND, independent cap arm: `cycles >= HOLD_MAX_CYCLES` (20). Even if the
// wall-time never reaches HOLD_MAX_WALL, an op held for HOLD_MAX_CYCLES cycles is
// abandoned. Here the SAME op is re-served HOLD_MAX_CYCLES+1 times against a FRESH
// just-P-saturated holder each cycle (T_wait_W < T_setup every time, wall advanced
// by only a small amount per cycle so the WALL arm never trips) — the first
// HOLD_MAX_CYCLES reserves HOLD (bumping `cycles`), and the (HOLD_MAX_CYCLES+1)-th
// trips the cycle arm → abandons onto X + `hold_expired` +1.
//
// MUTATION: remove the cycle arm (`rec.cycles >= HOLD_MAX_CYCLES`) from the cap
// check → the op keeps holding past 20 cycles → this red-fails (Some/X expected on
// the 21st reserve, None seen).
#[nativelink_test]
async fn hold_max_cycles_arm() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBF; 32], 1500);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x5C);
    let x = WorkerId("cyc_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // HOLD_MAX_CYCLES = 20 (asserted at the declaration site by the numeric block).
    const HOLD_MAX_CYCLES: u32 = 20;
    // Drive HOLD_MAX_CYCLES holds. Each cycle: a FRESH just-P-saturated holder Wi
    // stamped at the current clock, then advance only 1 s BEFORE the reserve so
    // T_wait_W(Wi) = 30 − 1 = 29 s ≥ T_setup?? — no: we need T_wait_W < T_setup, so
    // instead advance 28 s to make it 2 s, then advance only a TINY amount for the
    // next cycle's wall so the WALL arm (10 s) never trips across cycles. But 28 s
    // per cycle × 20 ≫ 10 s. To keep the wall arm untripped we instead re-stamp a
    // fresh holder EACH cycle at (base + 28 s of that cycle's local clock) but keep
    // first_hold_at's wall-delta small — impossible on one monotonic clock.
    //
    // Resolution: this arm is about the CYCLE COUNT, not wall-time. We keep the wall
    // arm from tripping by NOT advancing the wall between the per-cycle holder setup
    // and reserve beyond what the FIRST cycle already consumed. Concretely: advance
    // 28 s ONCE (so every fresh holder created afterwards, whose fillers are stamped
    // at the then-current clock, has elapsed 0 at creation), then for each cycle add
    // a fresh holder and advance a small 0-second delta — its fillers were stamped
    // at the current clock, so we advance a per-cycle small amount to age them into
    // the < T_setup band while keeping the total wall < HOLD_MAX_WALL is impossible
    // for 20 cycles. So we DISABLE the wall arm's reach by giving the op a fresh
    // holder each cycle stamped at NOW and advancing exactly 28 s per cycle, and we
    // rely on the CYCLE arm tripping FIRST at cycle 20 — but the wall arm would trip
    // at cycle 1 (28 s ≥ 10 s). Therefore this test can only isolate the CYCLE arm
    // if HOLD_MAX_CYCLES-worth of holds accrue BEFORE 10 s of wall passes, which
    // requires per-cycle wall advance < 0.5 s AND T_wait_W < T_setup. We get both by
    // stamping each fresh holder's fillers at (clock − 28 s) via a holder whose
    // fillers we age with a one-time 28 s advance, then per cycle advance 0 s: the
    // fillers stay at elapsed 28 s (T_wait_W = 2 s) and the wall does not move, so
    // the WALL arm never trips and only the CYCLE arm can abandon.
    //
    // Implement that: one advance of 28 s up-front; per cycle, a fresh holder whose
    // fillers are back-dated by the helper's stamp-at-current-clock is at elapsed 0
    // → T_wait_W = 30 s ≥ T_setup (would NOT hold). So instead reuse a SINGLE
    // long-lived holder W whose fillers age once, and advance 0 between cycles: its
    // T_wait_W stays 2 s and its running stays p_core_count across cycles (holds
    // don't consume W's slots). first_hold_at is set on cycle 1; the wall never
    // advances after, so ONLY the cycle counter grows.
    let w = WorkerId("cyc_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS)); // age W's fillers once
    let expired_before = ws.get_metrics().hold_expired.load(Ordering::Relaxed);

    for c in 0..HOLD_MAX_CYCLES {
        let r = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
            .await;
        assert!(
            r.is_none(),
            "hold_max_cycles: reserve #{c} must HOLD (cycles below the cap, wall not advanced so \
             the wall arm cannot trip) — got a reservation."
        );
    }
    // The (HOLD_MAX_CYCLES+1)-th reserve trips the CYCLE arm (cycles == 20 ≥ 20).
    let tripped = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    let (assigned, _, _) = tripped.expect(
        "hold_max_cycles: after HOLD_MAX_CYCLES (20) holds — with the wall never advanced so the \
         wall-time arm cannot trip — the CYCLE-count cap arm must ABANDON the hold and reserve the \
         best available worker. Got None: the cycle arm was removed, so a held op spins past 20 \
         cycles under a fast match cadence.",
    );
    assert_eq!(assigned, x, "hold_max_cycles: cap expiry must reserve idle X, got {assigned:?}.");
    assert_eq!(
        ws.get_metrics().hold_expired.load(Ordering::Relaxed) - expired_before,
        1,
        "hold_max_cycles: hold_expired did not increment on cycle-cap expiry."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// defensive_reap_on_reroute — the inner_unreserve_worker defensive reap.
// ─────────────────────────────────────────────────────────────────────────────
//
// A held op that is later reserved onto W, then UNRESERVED (assign-Aborted reroute)
// must not leak its hold record. `inner_unreserve_worker` defensively pops the
// hold-state record. Here: hold the op (cycle 1), then reserve it onto an idle
// holder Y (record reaped at assignment — paid_off), unreserve it (defensive reap
// is a no-op since assignment already reaped), then re-hold and re-reserve to
// confirm the op is treated as a FRESH hold (hold record was not carrying stale
// cycles). The direct observable: after unreserve, a fresh hold cycle starts at
// cycles=1 (not carried over), so a subsequent cap cannot trip early.
//
// COVERAGE + no-panic test. The `inner_unreserve_worker` hold-state reap is
// DEFENSIVE: in the normal flow a held op (returned None, never reserved) is not in
// any worker's `running_action_infos`, and an op that WAS reserved had its hold
// record reaped at assignment (`prepare_worker_run_action`), so by the time
// `inner_unreserve_worker` runs there is normally NO record to pop — removing the pop
// has no observable effect on the reachable normal flow (it guards odd
// reserved-then-aborted interleavings). This test drives the reap SITE via a real
// reserve→unreserve and asserts the path runs cleanly (no panic) and frees the
// worker's slot — the reachable, verifiable property. (The assignment-path reap +
// classification is mutation-verified by hold_paid_off_counter / hold_regret_counter.)
#[nativelink_test]
async fn defensive_reap_on_reroute() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xC1; 32], 1600);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xC1);

    // Reserve the op onto an idle holder Y directly (no hold needed — Y holds R).
    let y = WorkerId("reroute_y_idle_holder".to_string());
    let _rxy = add_idle_worker(&scheduler, y.clone()).await?;
    ws.update_cached_directories(&y, HashSet::from([input_root]))
        .await
        .err_tip(|| "reroute: update_cached_directories(Y) failed")?;
    let (assigned, _, _) = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await
        .expect("reroute: op must reserve onto idle holder Y");
    assert_eq!(assigned, y);
    assert_eq!(
        ws.worker_running_action_count_for_test(&y).await,
        Some(1),
        "reroute: Y must hold the reserved op before unreserve"
    );

    // Unreserve (the reroute path): the op is removed from Y and the defensive hold
    // reap runs (a no-op if no record, but MUST not panic and MUST free Y's slot).
    ws.unreserve_worker(&y, &op).await;
    assert_eq!(
        ws.worker_running_action_count_for_test(&y).await,
        Some(0),
        "defensive_reap_on_reroute: unreserve must remove the op from Y (freeing its slot) and \
         defensively reap any hold record — Y still holds the op, so the reroute path did not run \
         cleanly."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// defensive_reap_on_evict — the immediate_evict_worker defensive reap.
// ─────────────────────────────────────────────────────────────────────────────
//
// COVERAGE + no-panic test. When a worker holding a reserved op is EVICTED
// (disconnect / death), its running ops are drained and re-queued; the hold-state
// record for each is DEFENSIVELY popped in `immediate_evict_worker`'s drain loop.
// This reap is defensive: a HELD op is queued (not in `running_action_infos`), so it
// is never in the drain set, and a reserved op had its record reaped at assignment —
// so in the normal flow there is no record to pop (the reap guards odd
// reserved-then-evicted interleavings). This test drives the drain SITE via a real
// reserve→evict and asserts the loop runs cleanly (no panic) and removes the worker —
// the reachable, verifiable property.
#[nativelink_test]
async fn defensive_reap_on_evict() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xC2; 32], 1700);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xC2);

    let y = WorkerId("evict_y_idle_holder".to_string());
    let _rxy = add_idle_worker(&scheduler, y.clone()).await?;
    ws.update_cached_directories(&y, HashSet::from([input_root]))
        .await
        .err_tip(|| "evict: update_cached_directories(Y) failed")?;
    let (assigned, _, _) = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await
        .expect("evict: op must reserve onto idle holder Y");
    assert_eq!(assigned, y);

    // Evict Y (disconnect): drains its running ops and defensively reaps hold records.
    ws.remove_worker(&y)
        .await
        .err_tip(|| "evict: remove_worker failed")?;
    assert_eq!(
        ws.worker_running_action_count_for_test(&y).await,
        None,
        "defensive_reap_on_evict: Y must be gone after eviction (its drain loop ran, defensively \
         reaping any hold record for its drained ops)."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// do_try_match_none_requeue_seam — the production do_try_match None re-queue.
// ─────────────────────────────────────────────────────────────────────────────
//
// A hold returns `None` from the reserve; in the PRODUCTION `do_try_match` path
// that `None` is the existing no-match outcome (`return Ok(())`), leaving the action
// Queued (re-served next cycle), and it must NOT bump match errors. This drives the
// FULL `do_try_match_for_test()` (not just the reserve) with a held op and asserts
// the op stays Queued (not assigned, not errored) while `speculative_hold_count`
// bumps — the production-composition seam.
//
// MUTATION: return `Err` from the hold instead of `None` → do_try_match would bump
// `consecutive_match_errors` and could reject the action → this red-fails (the op
// gets assigned to X or the hold counter stays 0).
#[nativelink_test]
async fn do_try_match_none_requeue_seam() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xC3; 32], 1800);
    let w = WorkerId("seam_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let mut rxx = {
        let x = WorkerId("seam_x_idle".to_string());
        add_idle_worker(&scheduler, x.clone()).await?
    };

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    // Enqueue the op through the real client path so do_try_match sees it.
    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let mut ai = make_base_action_info(make_system_time(2), DigestInfo::new([0xC3; 32], 1));
    Arc::make_mut(&mut ai).input_root_digest = input_root;
    let _client_rx = scheduler
        .add_action(OperationId::default(), ai)
        .await
        .err_tip(|| "seam: add_action failed")?;

    // Run ONE production match cycle. The op should HOLD (None) → stay Queued.
    scheduler
        .do_try_match_for_test()
        .await
        .err_tip(|| "seam: do_try_match must return Ok even when the op is held")?;

    // The hold fired (counter bumped) …
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed) - hold_before,
        1,
        "do_try_match_none_requeue_seam: the held op did not bump speculative_hold_count through \
         the production do_try_match path — the None re-queue seam did not reach the gate."
    );
    // … and idle X did NOT receive a StartAction (the op stayed Queued).
    assert!(
        rxx.try_recv().is_err(),
        "do_try_match_none_requeue_seam: idle X received a StartAction — a held op must stay Queued \
         (None re-queue), not be assigned. If the hold returned Err instead of None, do_try_match \
         would mishandle the action."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// flag_off_no_hold — default-OFF inertness (§2.3.7).
// ─────────────────────────────────────────────────────────────────────────────
//
// With `enable_speculative_hold=false` (the DEFAULT) the whole gate is skipped, even
// when the pcore-first gate is ON. This uses the DISCRIMINATING config
// (`enable_speculative_hold=false` BUT `p_headroom_gate_enabled=true` — the realistic
// "hold dark, pcore-first live" state), so the `self.enable_speculative_hold` flag
// guard is the SOLE thing preventing the hold: W is P-excluded (p_gate_active true
// via idle X), so a cold X is available and the setup would HOLD were the flag on.
// The op must instead be assigned via the LRU/MRU fallback (W excluded from the cache
// tiers, X cold) with no hold counter movement.
//
// MUTATION: remove the `self.enable_speculative_hold &&` guard from the gate → with
// the flag off but pcore-first on, the gate fires (p_gate_active true, cold X, W a
// P-saturated holder) and HOLDS → this red-fails (None instead of a reservation, and
// the hold counter moves).
#[nativelink_test]
async fn flag_off_no_hold() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    // DISCRIMINATING config: hold flag OFF, pcore-first gate ON.
    let task_change_notify = Arc::new(Notify::new());
    let spec = SimpleSpec {
        enable_speculative_hold: false,
        p_headroom_gate_enabled: true,
        ..Default::default()
    };
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB6; 32], 600);
    let w = WorkerId("flagoff_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let x = WorkerId("flagoff_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x66);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    assert!(
        result.is_some(),
        "flag_off_no_hold: with enable_speculative_hold=false the gate must be inert even with \
         pcore-first ON — the op is assigned via the LRU/MRU fallback (never held). Got None: the \
         `self.enable_speculative_hold` flag guard was removed, so the gate ran with the flag off \
         (a default-behavior change)."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        0,
        "flag_off_no_hold: speculative_hold_count is non-zero with the flag OFF — the gate ran."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// p_core_count_zero_no_candidate — A5 legacy worker is never a hold candidate.
// ─────────────────────────────────────────────────────────────────────────────
//
// A worker with `p_core_count == 0` (legacy / Linux / Intel-Mac) is UNGATED (always
// has P-headroom, A5), so it is never P-saturated and never in `p_gated_excluded` →
// it is never a hold candidate. Here W holds R with p_core_count=0 and several
// running fillers → it still has A5 headroom → not excluded → wins Tier-1 → the op
// assigns to W, no hold. This proves the v3 hold candidacy uses the P-headroom
// function (which treats p_core_count=0 as ungated), not a raw running-count test.
//
// A REAL-core idle X is present (p_core_count=4, headroom) so `p_gate_active` is TRUE
// — that isolates the A5 clause as the SOLE reason W is not held-for (if W were
// wrongly P-saturated, p_gate_active being true would let the gate hold for it over
// the cold X).
//
// MUTATION: drop the `w.p_core_count == 0 ||` A5 clause from `worker_has_p_headroom`
// → a legacy W with running>0 reads as having NO P-headroom → it is excluded from
// Tier-1 AND pushed into `p_gated_excluded` → the gate (p_gate_active true via X)
// finds W (holds R, t_wait small) and HOLDS instead of assigning W → this red-fails
// (None / not-W).
#[nativelink_test]
async fn p_core_count_zero_no_candidate() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xC0; 32], 1900);
    // W: p_core_count=0 (legacy), holds R, with several running fillers. A5 →
    // ALWAYS has headroom → not P-saturated → wins Tier-1 (not excluded).
    let w = WorkerId("legacy_w_holder".to_string());
    let _rxw = add_worker_pcores(&scheduler, w.clone(), 0).await?;
    let mut dirs: HashSet<DigestInfo> = HashSet::from([input_root]);
    for i in 0..3u32 {
        dirs.insert(unique_filler_root(&w, i));
    }
    ws.update_cached_directories(&w, dirs)
        .await
        .err_tip(|| "legacy: update_cached_directories failed")?;
    for i in 0..3u32 {
        let filler_root = unique_filler_root(&w, i);
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xA0 + i as u8; 32], 7));
        Arc::make_mut(&mut ai).input_root_digest = filler_root;
        let filler_ai = ActionInfoWithProps { inner: ai, platform_properties: PlatformProperties::default() };
        let (assigned, _, _) = ws
            .find_and_reserve_worker(&PlatformProperties::default(), &OperationId::default(), &filler_ai, false)
            .await
            .expect("legacy: filler must reserve onto W");
        assert_eq!(assigned, w);
    }
    // A REAL-core idle X (p_core_count=4, headroom) → p_gate_active is TRUE, so the
    // A5 clause is the sole reason W (legacy) is not a hold candidate.
    let x = WorkerId("legacy_x_idle_realcores".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x0C);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "p_core_count_zero_no_candidate: a legacy worker (p_core_count=0) is UNGATED (A5, always \
         has P-headroom), so it is never P-saturated and never in p_gated_excluded — it wins Tier-1 \
         and the op assigns to it (no hold). Got None: the A5 `p_core_count == 0` clause was dropped \
         from worker_has_p_headroom, so the legacy W was wrongly P-saturated and held-for.",
    );
    assert_eq!(
        assigned, w,
        "p_core_count_zero_no_candidate: the op must land on the legacy holder W (ungated, wins \
         Tier-1), got {assigned:?}."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        hold_before,
        "p_core_count_zero_no_candidate: the gate held for a legacy (p_core_count=0) worker — the \
         A5 ungated worker must never be a hold candidate."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_paid_off_counter — the bet-realized landing outcome (§2.3.6).
// ─────────────────────────────────────────────────────────────────────────────
//
// When a held op is later ASSIGNED to a worker that HOLDS its input_root, the
// locality bet paid off → `hold_paid_off` bumps (and the hold record is reaped).
// Here: hold the op (cycle 1, P-saturated holder W + a cold idle X so p_gate_active
// is true and X is the cold rebind the hold declines), then make an IDLE holder Y of
// R available and re-serve → the op lands on Y (a holder, wins Tier-1 over cold X) →
// paid off.
//
// MUTATION: swap the paid-off/regret branches in `prepare_worker_run_action` (bump
// `hold_regret` when assigned_holds_root) → `hold_paid_off` stays 0 → red-fail.
#[nativelink_test]
async fn hold_paid_off_counter() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB8; 32], 800);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x88);

    // Cycle 1: hold for P-saturated holder W. A cold idle X provides the P-headroom
    // that makes `p_gate_active` true (v3) and is the cold rebind the hold declines.
    let w = WorkerId("paid_w_psat_holder".to_string());
    let _rxw = add_p_saturated_holder(&scheduler, w.clone(), input_root, 4, false).await?;
    let x = WorkerId("paid_x_idle_cold".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));
    let first = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    assert!(first.is_none(), "hold_paid_off: cycle-1 must hold (record the op).");

    let paid_before = ws.get_metrics().hold_paid_off.load(Ordering::Relaxed);
    let regret_before = ws.get_metrics().hold_regret.load(Ordering::Relaxed);

    // Cycle 2: an IDLE holder Y of R appears → the op lands on Y (a holder).
    let y = WorkerId("paid_y_idle_holder".to_string());
    let _rxy = add_idle_worker(&scheduler, y.clone()).await?;
    ws.update_cached_directories(&y, HashSet::from([input_root]))
        .await
        .err_tip(|| "hold_paid_off: update_cached_directories(Y) failed")?;

    let second = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    let (assigned, _, _) =
        second.expect("hold_paid_off: cycle-2 must assign the op to the idle holder Y.");
    assert_eq!(assigned, y, "hold_paid_off: the op must land on the idle holder Y, got {assigned:?}.");

    let paid_after = ws.get_metrics().hold_paid_off.load(Ordering::Relaxed);
    let regret_after = ws.get_metrics().hold_regret.load(Ordering::Relaxed);
    assert_eq!(
        paid_after - paid_before,
        1,
        "hold_paid_off: a held op assigned to a worker that HOLDS its input_root must bump \
         hold_paid_off — the bet-realized instrument is dark. before={paid_before} after={paid_after}"
    );
    assert_eq!(
        regret_after, regret_before,
        "hold_paid_off: hold_regret moved on a bet that PAID OFF (assigned to a holder) — swapping \
         the paid-off/regret branches makes this red-fail. before={regret_before} after={regret_after}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_regret_counter — the BIMODAL-RISK detector (§2.3.6).
// ─────────────────────────────────────────────────────────────────────────────
//
// When a held op is later assigned to a worker that does NOT hold its input_root
// AND it had been held ≥ T_setup of wall-time, an immediate reconstruct would have
// been faster → `hold_regret` bumps (the detector that sizes EWMA mis-estimation
// during the soak). Mirrors `max_hold_cap_abandons`: hold once (cycle 1, holder W1),
// advance the wall past HOLD_MAX_WALL (so held_for ≥ T_setup), add a FRESH
// non-overdue P-saturated holder W2 (so the cap — not an overdue W — is the abandon
// cause), then re-serve → the op is abandoned (cap) onto the COLD idle X (a
// non-holder). held_for at assignment (≥ T_setup) + landed on a non-holder → regret.
//
// MUTATION: swap the paid-off/regret branches in `prepare_worker_run_action` (bump
// `hold_paid_off` when NOT assigned_holds_root) → `hold_regret` stays 0 → red-fail.
#[nativelink_test]
async fn hold_regret_counter() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let scheduler = build_hold_scheduler(true);
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xBC; 32], 1200);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0xC8);

    // Cycle 1: hold for P-saturated holder W1.
    let w1 = WorkerId("regret_w1_psat_holder".to_string());
    let _rxw1 = add_p_saturated_holder(&scheduler, w1.clone(), input_root, 4, false).await?;
    let x = WorkerId("regret_x_idle_cold".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?; // cold non-holder
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));
    let first = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    assert!(first.is_none(), "hold_regret: cycle-1 must hold (record first_hold_at).");

    let paid_before = ws.get_metrics().hold_paid_off.load(Ordering::Relaxed);
    let regret_before = ws.get_metrics().hold_regret.load(Ordering::Relaxed);

    // Advance past HOLD_MAX_WALL since first_hold_at, then add a FRESH non-overdue
    // holder W2 (T_wait_W2 < T_setup) so the CAP — not an overdue W — is the abandon
    // cause. The op is then assigned to cold X; held_for (≥ 20 s) ≥ T_setup and X is
    // a non-holder → regret.
    MockClock::advance(Duration::from_secs(20));
    let w2 = WorkerId("regret_w2_psat_holder".to_string());
    let _rxw2 = add_p_saturated_holder(&scheduler, w2.clone(), input_root, 4, false).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let second = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    let (assigned, _, _) =
        second.expect("hold_regret: cycle-2 must abandon (cap) and assign the op to cold X.");
    assert_eq!(assigned, x, "hold_regret: the op must land on cold X, got {assigned:?}.");

    let paid_after = ws.get_metrics().hold_paid_off.load(Ordering::Relaxed);
    let regret_after = ws.get_metrics().hold_regret.load(Ordering::Relaxed);
    assert_eq!(
        regret_after - regret_before,
        1,
        "hold_regret: a held op assigned to a NON-holder after being held ≥ T_setup must bump \
         hold_regret — the bimodal-EWMA detector (the soak's flag-enable gate) is dark. \
         before={regret_before} after={regret_after}"
    );
    assert_eq!(
        paid_after, paid_before,
        "hold_regret: hold_paid_off moved on a bet that did NOT pay off (assigned to a non-holder) \
         — swapping the branches makes this red-fail. before={paid_before} after={paid_after}"
    );
    Ok(())
}
