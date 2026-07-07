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

//! (#sched M1 rebalance v2) Black-box, production-composition coverage of the
//! ranker-aware p_load-refinement config surface.
//!
//! The FINE-GRAINED ranker behavior (I6 magnet on Tier-1 / Tier-1.5 / fallback,
//! the ceiling bound, PrefMonotone, the predicate truth-table) is proven by the
//! INLINE `#[cfg(test)]` tests in `api_worker_scheduler.rs`: those need to set a
//! worker's `p_core_count` and inject a precise `running_action_infos` count,
//! neither of which is reachable from this external crate (`set_worker_core_counts`
//! is `#[cfg(test)]`-only; there is no public in-flight-count setter). So this
//! file owns the seam the inline tests DON'T cross: the `SimpleSpec` → `SimpleScheduler`
//! → `ApiWorkerScheduler::new_with_locality_map` config-plumbing path, verifying
//! that
//!   (a) the two new tunables (`p_idle_threshold_pct`, `p_headroom_override_factor`)
//!       flow through the whole constructor chain, and
//!   (b) a gate-ON, threshold-SET scheduler still dispatches an action to its
//!       only worker — no wedge (I2), the load-bearing no-wedge property at the
//!       full production composition.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{ActionInfo, OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

mod utils {
    pub(crate) mod scheduler_utils;
}
use utils::scheduler_utils::INSTANCE_NAME;

const NOW_TIME: u64 = 10000;
const WORKER_TIMEOUT_S: u64 = 100;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

/// Add a worker and drain its initial ConnectionResult frame.
async fn setup_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    // Real P/E core counts (4,4) so the P-headroom gate is actually engaged
    // (a `p_core_count == 0` worker is A5-ungated → the gate would be inert).
    let worker = Worker::new_with_cas_endpoint(
        worker_id.clone(),
        PlatformProperties::default(),
        tx,
        NOW_TIME,
        0,
        String::new(),
        4,
        4,
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    // Drain the ConnectionResult hello frame.
    let msg = rx.recv().await.expect("connection frame");
    assert!(
        matches!(
            msg.update,
            Some(update_for_worker::Update::ConnectionResult(_))
        ),
        "first frame must be the ConnectionResult hello"
    );
    Ok(rx)
}

fn make_action_info(action_digest: DigestInfo, insert_timestamp: SystemTime) -> Arc<ActionInfo> {
    use nativelink_util::action_messages::{ActionUniqueKey, ActionUniqueQualifier};
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: action_digest,
        }),
    })
}

/// (§13, config-plumbing seam) A gate-ON scheduler with a NON-ZERO idle
/// threshold and factor 2 — set via `SimpleSpec`, the exact shape prod
/// deserializes — must dispatch an action to its only worker. This crosses the
/// `SimpleSpec` → `SimpleScheduler::new_with_callback` → `ApiWorkerScheduler::
/// new_with_locality_map` plumbing (the config path the inline tests bypass by
/// constructing `ApiWorkerScheduler` directly), and proves the gate does NOT
/// wedge at the full production composition (I2 no-wedge). If either new field
/// failed to plumb, the constructor arity would not compile; if the gate
/// wrongly excluded the only worker, this dispatch would hang the deadlock
/// detector below.
#[nativelink_test]
async fn gate_on_with_threshold_config_plumbs_and_dispatches() -> Result<(), Error> {
    let worker_id = WorkerId("w1".to_string());
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            // The v2 surface, set to NON-default so the plumbing is exercised
            // with the override LIVE (threshold 50 > 0).
            p_headroom_gate_enabled: true,
            p_idle_threshold_pct: 50,
            p_headroom_override_factor: 2,
            ..Default::default()
        },
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

    let mut rx = setup_worker(&scheduler, worker_id.clone()).await?;

    let action_digest = DigestInfo::new([99u8; 32], 512);
    let client_id = OperationId::default();
    let _listener = scheduler
        .add_action(client_id, make_action_info(action_digest, make_system_time(1)))
        .await
        .expect("add_action");
    tokio::task::yield_now().await;

    // The worker MUST receive a StartAction — a gate-on-with-threshold config
    // that wedged (excluded the only worker) would time out here (the deadlock
    // detector). This is the no-wedge (I2) property at the SimpleScheduler
    // composition, plus proof the two new config fields plumbed through the
    // whole constructor chain.
    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("gate-on-with-threshold must not wedge — the only worker must be dispatched to")
        .expect("worker channel open");
    assert!(
        matches!(msg.update, Some(update_for_worker::Update::StartAction(_))),
        "the gate-ON, threshold-SET scheduler must dispatch the action to its only \
         worker (StartAction) — the override/ranker config must not wedge the fleet (I2)"
    );
    Ok(())
}

/// (§13 test 6, black-box) `SimpleSpec::default()` carries the v2 tunables at
/// their intended defaults — threshold 0 (override OFF → exact v1) and factor 2
/// (NOT the u32 type default 0, which would disable the override even with a
/// threshold set). The field-by-field `simple_spec_default_test.rs` in
/// `nativelink-config` is the canonical pin; this restates the two v2 values in
/// the scheduler crate so a `SimpleSpec` change that skipped the config test is
/// still caught where the scheduler consumes them.
#[nativelink_test]
async fn simple_spec_v2_defaults_are_override_off_factor_two() -> Result<(), Error> {
    let spec = SimpleSpec::default();
    assert_eq!(
        spec.p_idle_threshold_pct, 0,
        "default p_idle_threshold_pct must be 0 (override OFF → exact v1)"
    );
    assert_eq!(
        spec.p_headroom_override_factor, 2,
        "default p_headroom_override_factor must be 2 (a bare serde default of 0 \
         would make the ceiling p_count*0 == 0 and silently disable the override)"
    );
    assert!(
        spec.p_headroom_gate_enabled,
        "default p_headroom_gate_enabled must be TRUE (ENABLED by default, \
         drift-proof, per user 2026-07-07; config `false` is the kill-switch)"
    );
    Ok(())
}
