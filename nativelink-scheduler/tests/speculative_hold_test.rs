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

//! (#specprefetch-rebind Stage B) Tests for the TEMPORAL hold-vs-rebind gate
//! (`inner_find_and_reserve_worker`, design §2.3).
//!
//! The gate HOLDS a queued op (returns `None` from the reserve → the op re-queues)
//! for a BUSY-because-FULL holder W of the op's `input_root_digest` when W is
//! expected to free (`T_wait_W < T_setup`) SOONER than a free-but-cold worker X
//! could re-construct the input tree — trading a bounded queue wait for a saved
//! construct on the critical path. Behind `enable_speculative_hold` (default OFF).
//!
//! Test matrix:
//!  hold_no_deadlock_under_write_lock – the FATAL lock-ordering regression guard:
//!      the gate computes `T_wait_W` under the held `inner.write()`; the reserve
//!      MUST COMPLETE (a `timeout`-wrapped test that HANGS if the gate ever
//!      re-acquires the `inner` read lock — the design's fatal finding).
//!  hold_fires_for_busy_full_holder – the core positive: busy-full W, T_wait_W <
//!      T_setup, not overdue → reserve returns `None` + `speculative_hold_count` +1.
//!  no_hold_on_overdue_w – W with an inflight action past its estimate → NO hold
//!      (assigns X) — the bimodal-risk refusal.
//!  no_hold_when_x_is_holder – X itself holds the root (idle) → no hold (X wins).
//!  max_hold_cap_abandons – after the wall-time cap, the op reserves X +
//!      `hold_expired` +1 (the starvation bound).
//!  flag_off_no_hold – `enable_speculative_hold=false` → identical to today.
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
/// coherent with the worker keepalive timestamp passed to `Worker::new`.
const NOW_TIME: u64 = 10_000;

/// `DEFAULT_DURATION_ESTIMATE` (api_worker_scheduler) = 30 s, the per-action
/// estimate before any completion. `T_SETUP` = 3 s. So on a single-slot W,
/// `T_wait_W = 30 s − elapsed`; advancing the clock to `elapsed = 28 s` yields
/// `T_wait_W = 2 s < T_SETUP` while NOT overdue (28 s < 30 s). These constants
/// mirror the production values under test (not asserted here — the numeric-const
/// block asserts them at the declaration site).
const DEFAULT_ESTIMATE_SECS: u64 = 30;
const ELAPSED_UNDER_ESTIMATE_SECS: u64 = 28; // → T_wait_W = 2 s (< T_SETUP = 3 s)

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

/// Build a `SimpleSpec` with the temporal hold gate enabled (Stage-A prefetch off:
/// the hold gate is exercised directly via `find_and_reserve_worker`).
fn spec_with_hold(enable_hold: bool) -> SimpleSpec {
    SimpleSpec {
        enable_speculative_hold: enable_hold,
        ..Default::default()
    }
}

/// Add a worker with a specific `max_inflight_tasks` (0 = unlimited).
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
    drop(rx.recv().await); // drain ConnectionResult
    Ok(rx)
}

/// Add an IDLE worker (unlimited slots), no cached digests, reporting a low load
/// so it is a viable, non-saturated candidate (a never-reported worker is treated
/// as saturated by the #sched-zeroload gate, which would make it lose Tier-1 to
/// the LRU/MRU fall-through — masking the holder-vs-cold distinction the gate
/// depends on).
async fn add_idle_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let rx =
        add_worker_with_slots(scheduler, worker_id.clone(), PlatformProperties::default(), 0).await?;
    scheduler
        .update_worker_load(&worker_id, 10, 10, 10)
        .await
        .err_tip(|| "add_idle_worker: update_worker_load failed")?;
    Ok(rx)
}

/// Per-worker UNIQUE filler input-root, derived from the worker id bytes, so a
/// filler reserve lands on its intended worker via Tier-1 exact match (only that
/// worker will hold this digest) regardless of which other idle workers exist.
fn unique_filler_root(worker_id: &WorkerId) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = 0xF0;
    for (i, b) in worker_id.0.bytes().take(31).enumerate() {
        h[i + 1] = b;
    }
    DigestInfo::new(h, 7)
}

/// Add a worker with `max_inflight=1`, mark it as HOLDING BOTH a UNIQUE filler root
/// AND the op's `input_root` (the Tier-1 holder signal Stage B's
/// `find_busy_full_holder` reads), report a LOW load so the cache tiers do not fall
/// through to LRU/MRU (a never-reported worker is treated as saturated →
/// `saturation_fall_through` declines Tier-1, so the filler would miss its holder),
/// then reserve a filler CARRYING the unique root → Tier-1 exact match lands it on
/// THIS worker (only it holds the unique root), making it busy-because-full without
/// disturbing any idle peer. The filler's exec-start is stamped at the current
/// `MockClock::time()`, so advancing the clock drives `T_wait_W` for this worker.
async fn add_busy_full_holder(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    input_root: DigestInfo,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let props = PlatformProperties::default();
    let rx = add_worker_with_slots(scheduler, worker_id.clone(), props.clone(), 1).await?;
    // Report a low load so the worker is NOT treated as saturated → Tier-1 fires.
    scheduler
        .update_worker_load(&worker_id, 10, 10, 10)
        .await
        .err_tip(|| "add_busy_full_holder: update_worker_load failed")?;
    let filler_root = unique_filler_root(&worker_id);
    // W holds BOTH the unique filler root (for the filler's Tier-1 landing) AND the
    // op's input_root (the gate's holder signal).
    scheduler
        .worker_scheduler_for_test()
        .update_cached_directories(&worker_id, HashSet::from([filler_root, input_root]))
        .await
        .err_tip(|| "add_busy_full_holder: update_cached_directories failed")?;
    let filler_op = OperationId::default();
    let filler_ai = {
        let mut ai = make_base_action_info(make_system_time(1), DigestInfo::new([0xEE; 32], 7));
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
            "add_busy_full_holder precondition: filler op must reserve onto a worker (making it \
             busy-because-full)",
        );
    assert_eq!(
        assigned, worker_id,
        "add_busy_full_holder precondition: the filler (carrying the unique root) must reserve \
         onto W via Tier-1 exact match, not an idle peer — got {assigned:?}"
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
// This test drives a reserve that MUST reach the gate's `T_wait_W` computation
// under the held write lock (flag ON, a busy-full holder W, X cold so
// `dir_cache_winner` is None), wrapped in a `tokio::time::timeout`. If the gate
// ever re-acquires the read lock the reserve NEVER returns and the timeout fires.
//
// MUTATION (reproduces the fatal finding): in `find_and_reserve_worker`, after
// `let mut inner = self.inner.write().await;`, insert
// `let _ = self.worker_time_to_free(&W).await;` (the design's exact anti-pattern
// — re-acquire the read lock while the write lock is held). This test then HANGS
// and red-fails on the timeout expect below; the gate's use of the lock-free core
// is what keeps it green.
#[nativelink_test]
async fn hold_no_deadlock_under_write_lock() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB1; 32], 100);
    // W: busy-full holder of R (stamps filler exec-start at MockClock base).
    let w = WorkerId("hold_deadlock_w".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
    // X: idle, cold (does not hold R) → dir_cache_winner is None → gate reaches
    // find_busy_full_holder → t_wait_w_locked(W) under the write lock.
    let x = WorkerId("hold_deadlock_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // Advance so T_wait_W(W) = 30 − 28 = 2 s < T_SETUP (3 s) → the gate takes the
    // HOLD branch (return None), exercising the full gate path under the lock.
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x11);

    // The reserve MUST complete (not hang) under the held write lock.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ws.find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false),
    )
    .await
    .expect(
        "DEADLOCK: find_and_reserve_worker did not complete within 5 s — the temporal hold gate \
         re-acquired the inner RwLock (read) while the write lock was held, self-deadlocking the \
         match cycle. The gate MUST use the lock-free t_wait_w_locked core, NEVER worker_time_to_free \
         (design §2.3.1 fatal finding).",
    );

    // With T_wait_W < T_SETUP the gate HOLDS → None (the op re-queues). The point
    // of this test is that it RETURNED at all; the None is the expected outcome.
    assert!(
        result.is_none(),
        "hold_no_deadlock: with a busy-full holder and T_wait_W < T_setup the gate must HOLD \
         (return None). Got a reservation instead — the hold branch did not fire."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_fires_for_busy_full_holder — the core positive.
// ─────────────────────────────────────────────────────────────────────────────
//
// §2.3: busy-full holder W with T_wait_W < T_setup and not overdue, and the best
// available X is not itself a holder → the gate HOLDS: reserve returns None (op
// re-queues) and `speculative_hold_count` bumps by 1.
//
// MUTATION: skip the gate (comment out the whole `if self.enable_speculative_hold
// …` block in `inner_find_and_reserve_worker`) → the op is assigned to X → `result`
// is Some and `speculative_hold_count` stays 0 → both assertions red-fail.
#[nativelink_test]
async fn hold_fires_for_busy_full_holder() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB2; 32], 200);
    let w = WorkerId("hold_fire_w".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
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
        "hold_fires: a busy-full holder W with T_wait_W (2 s) < T_setup (3 s), not overdue, and \
         a cold X available → the gate MUST HOLD (return None so the op re-queues). Got a \
         reservation — the gate did not fire (skipping the gate makes this Some)."
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
// no_hold_on_overdue_w — the bimodal-risk refusal (§2.3.4).
// ─────────────────────────────────────────────────────────────────────────────
//
// The single GLOBAL duration EWMA mis-estimates on a bimodal fleet: a long-compile
// W reads "about to free" when the truth is minutes. The gate REFUSES to hold on
// an OVERDUE W (any inflight action with `elapsed > estimate`): ambiguous
// (about-to-finish → take X, OR a mis-estimated long action → holding is wrong and
// regresses p99), so it does not bet. Here W's filler has run PAST the 30 s
// estimate (elapsed 31 s) → overdue → NO hold → the op is assigned to idle X.
//
// MUTATION: drop the `&& !overdue` guard in the gate (accept a hold on an overdue
// W) → the op is HELD (None) instead of assigned to X → this red-fails with the
// bespoke bimodal-risk message below.
#[nativelink_test]
async fn no_hold_on_overdue_w() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB3; 32], 300);
    let w = WorkerId("overdue_w".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
    let x = WorkerId("overdue_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    // Advance PAST the estimate: elapsed = 31 s > 30 s → W is OVERDUE.
    MockClock::advance(Duration::from_secs(DEFAULT_ESTIMATE_SECS + 1));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x33);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "no_hold_on_overdue_w: W is OVERDUE (elapsed 31 s > estimate 30 s), so the gate must NOT \
         hold — it must assign the op to the idle X. Got None: the overdue guard was dropped, so \
         the op was held on a worker whose estimate is unreliable (the bimodal p99-regression \
         risk the guard exists to prevent).",
    );
    assert_eq!(
        assigned, x,
        "no_hold_on_overdue_w: the op landed on {assigned:?}, expected idle X — an overdue W must \
         not capture the op."
    );
    let hold_after = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    assert_eq!(
        hold_after, hold_before,
        "no_hold_on_overdue_w: speculative_hold_count moved on an OVERDUE holder — the gate held \
         when it must have refused. before={hold_before} after={hold_after}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// no_hold_when_x_is_holder — condition (a) (§2.3.1).
// ─────────────────────────────────────────────────────────────────────────────
//
// The gate holds only when the best available X is NOT itself a good-locality
// holder of the root (`dir_cache_winner.is_none()`). If a VIABLE (idle) worker
// already holds the root, it wins Tier-1 and can hardlink now — there is no reason
// to wait for a busy holder. Here X is idle AND holds R (so dir_cache_winner = X)
// while a separate busy-full holder W also exists → no hold: X wins.
//
// MUTATION: remove the `dir_cache_winner.is_none()` guard from the gate condition
// → the op is HELD even though an idle holder was available → this red-fails (the
// reserve returns None instead of assigning X).
#[nativelink_test]
async fn no_hold_when_x_is_holder() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB4; 32], 400);
    // A busy-full holder W of R also exists (so the gate WOULD have a W to hold for).
    let w = WorkerId("xh_w_busy_holder".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
    // X: IDLE and holds R → it is the Tier-1 dir_cache_winner.
    let x = WorkerId("xh_x_idle_holder".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    ws.update_cached_directories(&x, HashSet::from([input_root]))
        .await
        .err_tip(|| "no_hold_when_x_is_holder: update_cached_directories(X) failed")?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let hold_before = ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x44);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "no_hold_when_x_is_holder: an IDLE worker X holds the root, so it wins Tier-1 and can \
         hardlink now — the gate must NOT hold (condition (a): X is a good-locality holder). Got \
         None: the dir_cache_winner.is_none() guard was dropped, so the op was needlessly held.",
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
// max_hold_cap_abandons — the starvation bound (§2.3.4).
// ─────────────────────────────────────────────────────────────────────────────
//
// A held op must not hold INDEFINITELY. The max-hold cap trips on wall-time
// (`HOLD_MAX_WALL`) OR cycle count; on trip the gate ABANDONS the hold and
// reserves the best available (cold) X, bumping `hold_expired`. Here the SAME op is
// held once (recording first_hold_at), then the clock is advanced past
// `HOLD_MAX_WALL` (10 s), and the op is re-served: the cap trips → it reserves X.
//
// To keep W's T_wait_W < T_setup across BOTH cycles (so the ONLY reason the second
// reserve does not hold is the cap, not a T_wait_W change), W's filler is
// re-stamped between cycles by advancing only a little for the first hold and then
// jumping the wall past the cap while keeping the holder's remaining time small is
// not possible on one filler — so instead the second cycle uses a FRESH busy-full
// holder W2 whose filler was stamped just before the second reserve (T_wait_W ≈
// 2 s), isolating the cap as the sole cause. The op's hold record persists across
// the W→W2 swap (it is keyed on the op, not the worker).
//
// MUTATION: remove the cap check (the `cap_hit` branch) from the gate → the op is
// HELD forever (None on the second reserve) → this red-fails (expected Some/X).
#[nativelink_test]
async fn max_hold_cap_abandons() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB5; 32], 500);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x55);

    // ── Cycle 1: hold the op (records first_hold_at at the current clock) ──
    let w1 = WorkerId("cap_w1_busy_holder".to_string());
    let _rxw1 = add_busy_full_holder(&scheduler, w1.clone(), input_root).await?;
    let x = WorkerId("cap_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;
    // Make W1's filler nearly-done so T_wait_W < T_setup → the first reserve HOLDS.
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
    // First hold was at wall = NOW_TIME + 28 s. Jump +20 s → 48 s of wall have
    // passed since NOW_TIME; held_for = 20 s ≥ HOLD_MAX_WALL (10 s) → cap trips.
    MockClock::advance(Duration::from_secs(20));

    // ── Cycle 2: a FRESH busy-full holder W2 with T_wait_W < T_setup, so the ONLY
    // reason not to hold is the cap. W2's filler is stamped NOW; advance so its
    // elapsed = 28 s → T_wait_W(W2) = 2 s < T_setup. ──
    let w2 = WorkerId("cap_w2_busy_holder".to_string());
    let _rxw2 = add_busy_full_holder(&scheduler, w2.clone(), input_root).await?;
    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let second = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;
    let (assigned, _, _) = second.expect(
        "max_hold_cap: after HOLD_MAX_WALL of wall-time held, the gate must ABANDON the hold and \
         reserve the best available worker (the starvation bound). Got None: the cap check was \
         removed, so a held op holds indefinitely under sustained backlog.",
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
// flag_off_no_hold — default-OFF inertness (§2.3.7).
// ─────────────────────────────────────────────────────────────────────────────
//
// With `enable_speculative_hold=false` (the DEFAULT) the whole gate is skipped:
// the reserve is byte-identical to the pre-Stage-B path. The SAME setup that HOLDS
// under the flag (busy-full holder W, cold X, T_wait_W < T_setup) must instead
// ASSIGN the op to X, with no hold counter movement.
//
// MUTATION: this is the inertness guard for the flag itself — if the gate ever
// runs unconditionally (the `self.enable_speculative_hold &&` guard removed) this
// red-fails (the op is held → None instead of assigned to X).
#[nativelink_test]
async fn flag_off_no_hold() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(false); // gate OFF
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB6; 32], 600);
    let w = WorkerId("flagoff_w_busy_holder".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
    let x = WorkerId("flagoff_x_idle".to_string());
    let _rxx = add_idle_worker(&scheduler, x.clone()).await?;

    MockClock::advance(Duration::from_secs(ELAPSED_UNDER_ESTIMATE_SECS));

    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x66);
    let result = ws
        .find_and_reserve_worker(&PlatformProperties::default(), &op, &ai, false)
        .await;

    let (assigned, _, _) = result.expect(
        "flag_off_no_hold: with enable_speculative_hold=false the gate must be inert — the op is \
         assigned to the idle X exactly as today. Got None: the flag guard was removed, so the \
         gate ran with the flag off (a default-behavior change).",
    );
    assert_eq!(
        assigned, x,
        "flag_off_no_hold: the op landed on {assigned:?}, expected idle X (flag-off = today's \
         assignment)."
    );
    assert_eq!(
        ws.get_metrics().speculative_hold_count.load(Ordering::Relaxed),
        0,
        "flag_off_no_hold: speculative_hold_count is non-zero with the flag OFF — the gate ran."
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// hold_paid_off_counter — the bet-realized landing outcome (§2.3.6).
// ─────────────────────────────────────────────────────────────────────────────
//
// When a held op is later ASSIGNED to a worker that HOLDS its input_root, the
// locality bet paid off → `hold_paid_off` bumps (and the hold record is reaped).
// Here: hold the op (cycle 1, busy-full holder W), then make an IDLE holder Y of R
// available and re-serve → the op lands on Y (a holder) → paid off.
//
// MUTATION: swap the paid-off/regret branches in `prepare_worker_run_action` (bump
// `hold_regret` when assigned_holds_root) → `hold_paid_off` stays 0 → red-fail.
#[nativelink_test]
async fn hold_paid_off_counter() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB7; 32], 700);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x77);

    // Cycle 1: hold for busy-full holder W (no assignable holder yet).
    let w = WorkerId("paid_w_busy_holder".to_string());
    let _rxw = add_busy_full_holder(&scheduler, w.clone(), input_root).await?;
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
// during the soak). Mirrors `max_hold_cap_abandons`: hold once (cycle 1, holder
// W1), advance the wall past HOLD_MAX_WALL (so held_for ≥ T_setup), add a FRESH
// non-overdue busy-full holder W2 (so the cap — not an overdue W — is the abandon
// cause), then re-serve → the op is abandoned (cap) onto the COLD idle X (a
// non-holder). held_for at assignment (≥ T_setup) + landed on a non-holder →
// regret (and NOT paid_off).
//
// MUTATION: swap the paid-off/regret branches in `prepare_worker_run_action` (bump
// `hold_paid_off` when NOT assigned_holds_root) → `hold_regret` stays 0 → red-fail.
#[nativelink_test]
async fn hold_regret_counter() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let task_change_notify = Arc::new(Notify::new());
    let spec = spec_with_hold(true);
    let (scheduler, _ws) = SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None, None, None, None,
    );
    let ws = scheduler.worker_scheduler_for_test();

    let input_root = DigestInfo::new([0xB8; 32], 800);
    let op = OperationId::default();
    let ai = action_with_root(input_root, 0x88);

    // Cycle 1: hold for busy-full holder W1.
    let w1 = WorkerId("regret_w1_busy_holder".to_string());
    let _rxw1 = add_busy_full_holder(&scheduler, w1.clone(), input_root).await?;
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
    // holder W2 (T_wait_W2 < T_setup) so the CAP — not an overdue W — is the
    // abandon cause. The op is then assigned to cold X; held_for (≥ 20 s) ≥ T_setup
    // and X is a non-holder → regret.
    MockClock::advance(Duration::from_secs(20));
    let w2 = WorkerId("regret_w2_busy_holder".to_string());
    let _rxw2 = add_busy_full_holder(&scheduler, w2.clone(), input_root).await?;
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
