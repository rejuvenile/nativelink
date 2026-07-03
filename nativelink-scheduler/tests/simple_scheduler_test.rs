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

use core::future::Future;
use core::ops::Bound;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_lock::Mutex;
use bytes::Bytes;
use futures::task::Poll;
use futures::{Stream, StreamExt, poll};
use mock_instant::thread_local::{MockClock, SystemTime as MockSystemTime};
use nativelink_config::schedulers::{PropertyType, SimpleSpec};
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory, ExecuteRequest, FileNode, Platform, digest_function,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, StartExecute, UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, DirectoryInfo, ExecutionMetadata, FileInfo,
    INTERNAL_ERROR_EXIT_CODE, NameOrPath, OperationId, SymlinkInfo, WorkerId,
};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, OperationFilter, OperationStageFlags,
    UpdateOperationType,
};
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, mpsc};
use utils::scheduler_utils::{INSTANCE_NAME, make_base_action_info, update_eq};

mod utils {
    pub(crate) mod scheduler_utils;
}

async fn verify_initial_connection_message(
    worker_id: WorkerId,
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) {
    // Worker should have been sent an execute command.
    let expected_msg_for_worker = UpdateForWorker {
        update: Some(update_for_worker::Update::ConnectionResult(
            ConnectionResult {
                worker_id: worker_id.into(),
            },
        )),
    };
    let msg_for_worker = rx.recv().await.unwrap();
    assert_eq!(msg_for_worker, expected_msg_for_worker);
}

const NOW_TIME: u64 = 10000;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

async fn setup_new_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(worker_id.clone(), props, tx, NOW_TIME, 0);
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

/// Like `setup_new_worker`, but constructs the worker with realistic P/E
/// logical-CPU counts so the continuous cache-vs-load blend
/// (`capacity_score`) has a real per-core denominator instead of the
/// `assume_core_count` fallback. In production the counts ride the connect
/// hello frame (`Worker::new_with_cas_endpoint`); the `#[cfg(test)]`
/// `set_worker_core_counts` scheduler helper is only visible to the src
/// crate's own unit tests, NOT to this integration test — so integration
/// tests that need a prod-shaped worker (reported load + real core counts)
/// build one here via the public `new_with_cas_endpoint` with an empty CAS
/// endpoint (no locality wiring, just the counts).
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
        String::new(), // no CAS endpoint — counts only, no locality wiring
        p_core_count,
        e_core_count,
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

async fn setup_action(
    scheduler: &SimpleScheduler,
    action_digest: DigestInfo,
    platform_properties: HashMap<String, String>,
    insert_timestamp: SystemTime,
) -> Result<Box<dyn ActionStateResult>, Error> {
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    Arc::make_mut(&mut action_info).platform_properties = platform_properties;
    let client_id = OperationId::default();
    let result = scheduler.add_action(client_id, action_info).await;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    result
}

const WORKER_TIMEOUT_S: u64 = 100;

#[nativelink_test]
async fn basic_add_action_with_one_worker_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
            .await
            .unwrap();

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
                platform: Some(Platform::default()),
                worker_id: worker_id.into(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Executing,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn bad_worker_match_logging_interval() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let (_scheduler, _worker_scheduler) = SimpleScheduler::new(
        &SimpleSpec {
            worker_match_logging_interval_s: -2,
            ..Default::default()
        },
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        task_change_notify,
        None,
    );
    assert!(logs_contain(
        "nativelink_scheduler::simple_scheduler: Valid values for worker_match_logging_interval_s are -1, 0, or a positive integer, setting to disabled worker_match_logging_interval_s=-2"
    ));
    Ok(())
}

#[nativelink_test]
async fn client_does_not_receive_update_timeout() -> Result<(), Error> {
    async fn advance_time<T>(duration: Duration, poll_fut: &mut Pin<&mut impl Future<Output = T>>) {
        const STEP_AMOUNT: Duration = Duration::from_millis(100);
        for _ in 0..(duration.as_millis() / STEP_AMOUNT.as_millis()) {
            MockClock::advance(STEP_AMOUNT);
            tokio::task::yield_now().await;
            assert!(poll!(&mut *poll_fut).is_pending());
        }
    }

    MockClock::set_time(Duration::from_secs(NOW_TIME));

    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            worker_match_logging_interval_s: 1,
            ..Default::default()
        },
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify.clone(),
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let _rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        HashMap::new(),
        make_system_time(1),
    )
    .await
    .unwrap();

    // Trigger a do_try_match to ensure we get a state change.
    scheduler.do_try_match_for_test().await?;
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    let changed_fut = action_listener.changed();
    tokio::pin!(changed_fut);

    {
        // No update should have been received yet.
        assert_eq!(poll!(&mut changed_fut).is_ready(), false);
    }
    // Advance our time by just under the timeout.
    advance_time(Duration::from_secs(WORKER_TIMEOUT_S - 1), &mut changed_fut).await;
    {
        // Still no update should have been received yet.
        assert_eq!(poll!(&mut changed_fut).is_ready(), false);
    }
    // Advance it by just over the timeout.
    MockClock::advance(Duration::from_secs(2));
    {
        // Now we should have received a timeout and the action should have been
        // put back in the queue.
        assert_eq!(changed_fut.await.unwrap().0.stage, ActionStage::Queued);
    }

    Ok(())
}

#[nativelink_test]
async fn find_executing_action() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let action_listener = setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
        .await
        .unwrap();

    let client_operation_id = action_listener
        .as_state()
        .await
        .unwrap()
        .0
        .client_operation_id
        .clone();
    // Drop our receiver and look up a new one.
    drop(action_listener);
    let mut action_listener = scheduler
        .filter_operations(OperationFilter {
            client_operation_id: Some(client_operation_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .next()
        .await
        .expect("Action not found");

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
                platform: Some(Platform::default()),
                worker_id: worker_id.into(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Executing,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn remove_worker_reschedules_multiple_running_job_test() -> Result<(), Error> {
    let worker_id1 = WorkerId("worker1".to_string());
    let worker_id2 = WorkerId("worker2".to_string());
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest1 = DigestInfo::new([99u8; 32], 512);
    let action_digest2 = DigestInfo::new([88u8; 32], 512);

    let mut rx_from_worker1 = setup_new_worker(
        &scheduler,
        worker_id1.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest1,
        HashMap::new(),
        insert_timestamp1,
    )
    .await?;
    let insert_timestamp2 = make_system_time(2);
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest2,
        HashMap::new(),
        insert_timestamp2,
    )
    .await?;

    let mut expected_start_execute_for_worker1 = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest1.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "WILL BE SET BELOW".to_string(),
        queued_timestamp: Some(insert_timestamp1.into()),
        platform: Some(Platform::default()),
        worker_id: worker_id1.to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
    };

    let mut expected_start_execute_for_worker2 = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest2.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "WILL BE SET BELOW".to_string(),
        queued_timestamp: Some(insert_timestamp2.into()),
        platform: Some(Platform::default()),
        worker_id: worker_id1.to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
    };
    let operation_id1 = {
        // Worker1 should now see first execution request.
        let update_for_worker = rx_from_worker1
            .recv()
            .await
            .expect("Worker terminated stream")
            .update
            .expect("`update` should be set on UpdateForWorker");
        let (operation_id, rx_start_execute) = match update_for_worker {
            update_for_worker::Update::StartAction(start_execute) => (
                OperationId::from(start_execute.operation_id.as_str()),
                start_execute,
            ),
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        expected_start_execute_for_worker1.operation_id = operation_id.to_string();
        assert_eq!(expected_start_execute_for_worker1, rx_start_execute);
        operation_id
    };
    let operation_id2 = {
        // Worker1 should now see second execution request.
        let update_for_worker = rx_from_worker1
            .recv()
            .await
            .expect("Worker terminated stream")
            .update
            .expect("`update` should be set on UpdateForWorker");
        let (operation_id, rx_start_execute) = match update_for_worker {
            update_for_worker::Update::StartAction(start_execute) => (
                OperationId::from(start_execute.operation_id.as_str()),
                start_execute,
            ),
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        expected_start_execute_for_worker2.operation_id = operation_id.to_string();
        assert_eq!(expected_start_execute_for_worker2, rx_start_execute);
        operation_id
    };

    // Add a second worker that can take jobs if the first dies.
    let mut rx_from_worker2 = setup_new_worker(
        &scheduler,
        worker_id2.clone(),
        PlatformProperties::default(),
    )
    .await?;

    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) =
            client1_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) =
            client2_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }

    // Now remove worker.
    drop(scheduler.remove_worker(&worker_id1).await);
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    {
        // Worker1 should have received a disconnect message.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::Disconnect(()))
            }
        );
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) =
            client1_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) =
            client2_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        expected_start_execute_for_worker1.operation_id = operation_id1.to_string();
        expected_start_execute_for_worker1.worker_id = worker_id2.to_string();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    expected_start_execute_for_worker1
                )),
            }
        );
    }
    {
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        expected_start_execute_for_worker2.operation_id = operation_id2.to_string();
        expected_start_execute_for_worker2.worker_id = worker_id2.to_string();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    expected_start_execute_for_worker2
                )),
            }
        );
    }

    Ok(())
}

/// FL-681 re-saturation gate: a worker whose indefinite-pin cap is reported
/// saturated MUST NOT be selected by the matcher for a new action — even when
/// it has no other in-flight action (the regime the admission-NAK pause misses,
/// because the scheduler's `update_action` pause is conditional on
/// `worker.has_actions()`). Without the matcher gate, the re-queued action
/// re-dispatches to the same saturated worker → re-NAK → an RPC-rate
/// re-dispatch spin between scheduler and worker. With the gate, the action
/// stays Queued until the worker reports headroom again.
///
/// This drives the production `SimpleScheduler` composition (matcher +
/// worker pool + state manager), not the matcher in isolation, so the
/// `update_worker_indefinite_pin_saturation` → `Worker` → `inner_find_and_reserve_worker`
/// path is exercised end to end.
#[nativelink_test]
async fn indefinite_pin_saturated_worker_not_rematched_until_headroom_test()
-> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    // Report the worker's indefinite-pin cap as saturated BEFORE any action is
    // queued. This is the single-in-flight regime: the worker has ZERO actions
    // in `running_action_infos` (its saturating backlog lives in the pin set),
    // so the admission-NAK conditional pause would never fire — only the
    // matcher gate keeps the worker undispatchable.
    scheduler
        .update_worker_indefinite_pin_saturation(&worker_id, true)
        .await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([88u8; 32], 512);
    let insert_timestamp = make_system_time(14);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    {
        // The action MUST be parked in Queued: the only worker is saturated, so
        // the matcher must skip it rather than dispatch → NAK → re-dispatch.
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before first state");
        assert_eq!(
            action_state.stage,
            ActionStage::Queued,
            "FL-681 re-saturation spin: a saturated worker was selected by the \
             matcher and the action was dispatched (expected Queued — the \
             matcher must skip an indefinite-pin-saturated worker so it is not \
             re-NAK-spun)"
        );
    }

    // Worker drains below cap (a BIS-ack freed pin headroom) → reports
    // not-saturated. The matcher must now select it and the action executes.
    scheduler
        .update_worker_indefinite_pin_saturation(&worker_id, false)
        .await?;
    tokio::task::yield_now().await;

    {
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before second state");
        assert_eq!(
            action_state.stage,
            ActionStage::Executing,
            "a worker reporting indefinite-pin headroom must be re-selectable \
             by the matcher (action stuck in Queued after saturation cleared)"
        );
    }

    // Sanity: the worker actually received the StartAction once unblocked —
    // confirms the matcher SELECTED the worker (not merely that the state
    // manager flipped to Executing).
    match rx_from_worker
        .recv()
        .await
        .expect("worker channel closed")
        .update
    {
        Some(update_for_worker::Update::StartAction(_)) => {}
        v => panic!("Expected StartAction after headroom restored, got: {v:?}"),
    }

    Ok(())
}

/// FL-681 re-saturation gate, cache-affinity path: the matcher has THREE
/// eligibility predicates. The LRU/MRU fallback runs `worker_matches`; the
/// directory-cache and subtree-coverage tiers run `worker_is_viable`. A worker
/// that has the action's `input_root_digest` cached is selected by the
/// `dir_cache_winner` tier through `worker_is_viable` — a SEPARATE code path
/// from the fallback. This test drives that path (worker has the input_root in
/// its directory cache) and asserts the saturation gate fires there too, so a
/// saturated worker is not selected even when it is the cache-affinity winner.
#[nativelink_test]
async fn indefinite_pin_saturated_worker_skipped_on_cache_affinity_path_test()
-> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_from_worker = setup_new_worker_with_cas_endpoint(
        &scheduler,
        worker_id.clone(),
        PlatformProperties::default(),
        "worker:50081",
    )
    .await?;

    // Give the worker the action's input_root in its directory cache so the
    // `dir_cache_winner` tier (which evaluates `worker_is_viable`) would select
    // it absent the saturation gate.
    let input_root_digest = DigestInfo::new([55u8; 32], 4096);
    let mut cached_dirs = std::collections::HashSet::new();
    cached_dirs.insert(input_root_digest);
    scheduler
        .update_cached_directories(&worker_id, cached_dirs)
        .await?;

    // Report saturation BEFORE queueing the action (single-in-flight regime).
    scheduler
        .update_worker_indefinite_pin_saturation(&worker_id, true)
        .await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([56u8; 32], 512);
    let insert_timestamp = make_system_time(20);
    let mut action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    {
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before first state");
        assert_eq!(
            action_state.stage,
            ActionStage::Queued,
            "FL-681 re-saturation spin (cache-affinity path): a saturated worker \
             was selected by the directory-cache tier (worker_is_viable) and the \
             action was dispatched (expected Queued — the matcher must skip an \
             indefinite-pin-saturated worker on EVERY selection tier)"
        );
    }

    // Clear saturation → the cache-affinity winner is selectable again.
    scheduler
        .update_worker_indefinite_pin_saturation(&worker_id, false)
        .await?;
    tokio::task::yield_now().await;

    {
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before second state");
        assert_eq!(
            action_state.stage,
            ActionStage::Executing,
            "cache-affinity winner must be re-selectable once saturation clears"
        );
    }

    // Confirm the worker received the dispatch (StartAction may interleave with
    // a PeerHints ChunkedMessage on the cache-affinity path — accept either as
    // the first message and require StartAction within a small drain budget).
    let mut saw_start = false;
    for _ in 0..4 {
        match rx_from_worker
            .recv()
            .await
            .expect("worker channel closed")
            .update
        {
            Some(update_for_worker::Update::StartAction(_)) => {
                saw_start = true;
                break;
            }
            Some(_) => continue,
            None => panic!("worker channel produced empty update"),
        }
    }
    assert!(
        saw_start,
        "worker did not receive StartAction after saturation cleared"
    );

    Ok(())
}

/// (F4) T4 — disk-pressure routing. The matcher MUST route a new action AWAY
/// from a `disk_pressured` worker to a healthy peer, so the action is not
/// dispatched into a worker that would NAK ResourceExhausted (re-queue → re-
/// dispatch spin) or ENOSPC at make_action_directory.
///
/// Topology note (vs the indefinite-pin sibling test): the disk gate has a
/// FLEET FAIL-OPEN, so a SINGLE disk-pressured worker would be re-admitted via
/// the fail-open (correct — better one NAK than a wedge). The "routes away"
/// contract therefore needs a HEALTHY alternative: with one pressured + one
/// healthy worker, the action must land on the HEALTHY one. This proves the
/// `worker_matches` skip steers selection, distinct from the all-pressured
/// fail-open (the sibling `all_disk_pressured_fleet_fail_open` test).
///
/// Production composition: drives the real `SimpleScheduler` (matcher + worker
/// pool + state manager), exercising `update_worker_disk_pressure` →
/// `Worker.disk_pressured` → `inner_find_and_reserve_worker` (`worker_matches`
/// skip) end to end.
///
/// Mutation step (CLAUDE.md TDD #5): in `api_worker_scheduler.rs`, delete the
/// `if w.disk_pressured { return false; }` skip in `worker_matches`. This test
/// red-fails: the disk-pressured worker is no longer steered-around, so the
/// action may dispatch to it (the pressured worker receives the StartAction).
#[nativelink_test]
async fn disk_pressured_worker_routed_away_test() -> Result<(), Error> {
    let pressured_worker = WorkerId("disk_pressured_worker".to_string());
    let healthy_worker = WorkerId("healthy_worker".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_pressured =
        setup_new_worker(&scheduler, pressured_worker.clone(), PlatformProperties::default()).await?;
    let mut rx_healthy =
        setup_new_worker(&scheduler, healthy_worker.clone(), PlatformProperties::default()).await?;

    // One worker disk-pressured (volume full), one healthy. The matcher must
    // steer AROUND the pressured one.
    scheduler
        .update_worker_disk_pressure(&pressured_worker, true, 0)
        .await?;
    scheduler
        .update_worker_disk_pressure(&healthy_worker, false, 100 * (1 << 30))
        .await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([77u8; 32], 512);
    let insert_timestamp = make_system_time(15);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    {
        // The action dispatches (a healthy worker exists) → Executing.
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before first state");
        assert_eq!(
            action_state.stage,
            ActionStage::Executing,
            "with a healthy worker available the action must dispatch"
        );
    }

    // The load-bearing assertion: the HEALTHY worker received the StartAction,
    // and the disk-pressured worker did NOT. Drain the healthy worker's channel
    // with a budget (StartAction may interleave with a PeerHints ChunkedMessage)
    // under a `tokio::time::timeout` deadlock detector.
    let mut healthy_saw_start = false;
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(5), rx_healthy.recv()).await {
            Ok(Some(msg)) => match msg.update {
                Some(update_for_worker::Update::StartAction(_)) => {
                    healthy_saw_start = true;
                    break;
                }
                _ => continue,
            },
            Ok(None) => panic!("healthy worker channel closed"),
            Err(_) => break,
        }
    }
    assert!(
        healthy_saw_start,
        "F4 disk-pressure gate: the HEALTHY worker did NOT receive the \
         StartAction — the matcher must steer the action to the healthy worker \
         and away from the disk-pressured one"
    );
    // The pressured worker must NOT have been dispatched to. By the time the
    // healthy worker has the StartAction, the matcher has resolved this action,
    // so the pressured channel holding any StartAction is a routing bug.
    let mut pressured_saw_start = false;
    while let Ok(msg) = rx_pressured.try_recv() {
        if let Some(update_for_worker::Update::StartAction(_)) = msg.update {
            pressured_saw_start = true;
            break;
        }
    }
    assert!(
        !pressured_saw_start,
        "F4 disk-pressure gate: the disk-pressured worker was dispatched to \
         (matcher did not route around it)"
    );

    Ok(())
}

/// (F4) T4 — fleet fail-open MUST NOT wedge a fully disk-pressured fleet. The
/// cadre's load-bearing correction: if `disk_pressured` is added to the
/// `worker_matches` skip but the fleet fail-open predicate
/// (`worker_matches_ignoring_pressure`) does NOT ignore it, then when EVERY
/// candidate is disk-gated the matcher finds nothing and the action wedges in
/// Queued forever (capability-class wedge). The worker-local statvfs fallback
/// (rejecting only a TRULY-full disk) is the backstop, so placing on the
/// least-pressured (most-free) worker is safe — better a worker-NAK re-queue
/// than a permanent stall.
///
/// Production composition: a TWO-worker fleet, BOTH reporting disk-pressured.
/// The action must still be DISPATCHED (the fleet fail-open at
/// `api_worker_scheduler.rs` places on the most-free worker), NOT stuck in
/// Queued.
///
/// Mutation step: in `worker_matches_ignoring_pressure`, add
/// `&& !w.disk_pressured` (re-introduce the disk skip in the fail-open
/// predicate). This test red-fails: with no fail-open target, the action stays
/// Queued and the `recv()` of a StartAction times out / the stage assertion
/// fails.
#[nativelink_test]
async fn all_disk_pressured_fleet_fail_open_does_not_wedge_test() -> Result<(), Error> {
    let worker_id_1 = WorkerId("worker_1".to_string());
    let worker_id_2 = WorkerId("worker_2".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_from_worker_1 =
        setup_new_worker(&scheduler, worker_id_1.clone(), PlatformProperties::default()).await?;
    let mut rx_from_worker_2 =
        setup_new_worker(&scheduler, worker_id_2.clone(), PlatformProperties::default()).await?;

    // BOTH workers disk-pressured. Worker 2 has MORE free bytes, so the
    // most-free fail-open ranking should prefer it — but the load-bearing
    // assertion is only that SOMETHING is dispatched (no wedge).
    scheduler
        .update_worker_disk_pressure(&worker_id_1, true, 1 << 30) // 1 GiB free
        .await?;
    scheduler
        .update_worker_disk_pressure(&worker_id_2, true, 4 * (1 << 30)) // 4 GiB free
        .await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([78u8; 32], 512);
    let insert_timestamp = make_system_time(16);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    {
        // The fleet fail-open MUST place the action despite every worker being
        // disk-gated — else the capability class wedges.
        let (action_state, _maybe_origin_metadata) = action_listener
            .changed()
            .await
            .expect("action listener closed before first state");
        assert_eq!(
            action_state.stage,
            ActionStage::Executing,
            "F4 fleet fail-open: an action wedged in Queued with EVERY worker \
             disk-pressured — the fail-open predicate must ignore disk pressure \
             and place on the least-pressured (most-free) worker, else a fully \
             disk-pressured fleet deadlocks its capability class (cadre correction)"
        );
    }

    // Confirm one of the two workers actually received the StartAction (the
    // fail-open SELECTED a worker, not merely flipped the stage). The ranking
    // prefers the most-free worker (worker 2), but accept either to keep the
    // test about the no-wedge contract rather than the tie-break.
    let mut saw_start = false;
    for _ in 0..4 {
        tokio::select! {
            biased;
            msg = rx_from_worker_2.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) =
                    msg.expect("worker 2 channel closed").update
                {
                    saw_start = true;
                    break;
                }
            }
            msg = rx_from_worker_1.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) =
                    msg.expect("worker 1 channel closed").update
                {
                    saw_start = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_start,
        "fleet fail-open selected no worker for dispatch on a fully \
         disk-pressured fleet (the matcher must degrade to least-pressured \
         placement, not wedge)"
    );

    Ok(())
}

#[nativelink_test]
async fn set_drain_worker_pauses_and_resumes_worker_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let _operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                OperationId::from(start_execute.operation_id)
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        operation_id
    };

    // Set the worker draining.
    scheduler.set_drain_worker(&worker_id, true).await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([88u8; 32], 512);
    let insert_timestamp = make_system_time(14);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    {
        // Client should get notification saying it's been queued.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Queued,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Set the worker not draining.
    scheduler.set_drain_worker(&worker_id, false).await?;
    tokio::task::yield_now().await;

    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Executing,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn worker_should_not_queue_if_properties_dont_match_test() -> Result<(), Error> {
    let worker_id1 = WorkerId("worker1".to_string());
    let worker_id2 = WorkerId("worker2".to_string());

    let mut prop_defs = HashMap::new();
    prop_defs.insert("prop".to_string(), PropertyType::Exact);

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(prop_defs),
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);
    let mut platform_properties = HashMap::new();
    platform_properties.insert("prop".to_string(), "1".to_string());
    let mut worker1_properties = PlatformProperties::default();
    worker1_properties.properties.insert(
        "prop".to_string(),
        PlatformPropertyValue::Exact("2".to_string()),
    );

    let mut rx_from_worker1 =
        setup_new_worker(&scheduler, worker_id1, worker1_properties.clone()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        platform_properties,
        insert_timestamp,
    )
    .await?;

    {
        // Client should get notification saying it's been queued.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Queued,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }
    let mut worker2_properties = PlatformProperties::default();
    worker2_properties.properties.insert(
        "prop".to_string(),
        PlatformPropertyValue::Exact("1".to_string()),
    );
    let mut rx_from_worker2 =
        setup_new_worker(&scheduler, worker_id2.clone(), worker2_properties.clone()).await?;
    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
                platform: Some((&worker2_properties).into()),
                worker_id: worker_id2.to_string(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
            })),
        };
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Executing,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Our first worker should have no updates over this test.
    assert_eq!(
        rx_from_worker1.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

#[nativelink_test]
async fn cacheable_items_join_same_action_queued_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let client_operation_id = OperationId::default();
    let mut expected_action_state = ActionState {
        client_operation_id,
        stage: ActionStage::Queued,
        action_digest,
        last_transition_timestamp: SystemTime::now(),
    };

    let insert_timestamp1 = make_system_time(1);
    let insert_timestamp2 = make_system_time(2);
    let mut client1_action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp1).await?;
    let mut client2_action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp2).await?;

    let (operation_id1, operation_id2) = {
        // Clients should get notification saying it's been queued.
        let (action_state1, _maybe_origin_metadata) =
            client1_action_listener.changed().await.unwrap();
        let (action_state2, _maybe_origin_metadata) =
            client2_action_listener.changed().await.unwrap();
        let operation_id1 = action_state1.client_operation_id.clone();
        let operation_id2 = action_state2.client_operation_id.clone();
        // Name is random so we set force it to be the same.
        expected_action_state.client_operation_id = operation_id1.clone();
        assert_eq!(action_state1.as_ref(), &expected_action_state);
        expected_action_state.client_operation_id = operation_id2.clone();
        assert_eq!(action_state2.as_ref(), &expected_action_state);
        // Both clients should have unique operation ID.
        assert_ne!(
            action_state2.client_operation_id,
            action_state1.client_operation_id
        );
        (operation_id1, operation_id2)
    };

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp1.into()),
                platform: Some(Platform::default()),
                worker_id: worker_id.into(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }

    // Action should now be executing.
    expected_action_state.stage = ActionStage::Executing;
    expected_action_state.last_transition_timestamp = SystemTime::now();
    {
        // Both client1 and client2 should be receiving the same updates.
        // Most importantly the `name` (which is random) will be the same.
        expected_action_state.client_operation_id = operation_id1.clone();
        assert_eq!(
            client1_action_listener.changed().await.unwrap().0.as_ref(),
            &expected_action_state
        );
        expected_action_state.client_operation_id = operation_id2.clone();
        assert_eq!(
            client2_action_listener.changed().await.unwrap().0.as_ref(),
            &expected_action_state
        );
    }

    {
        // Now if another action is requested it should also join with executing action.
        let insert_timestamp3 = make_system_time(2);
        let mut client3_action_listener =
            setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp3).await?;
        let (action_state, _maybe_origin_metadata) =
            client3_action_listener.changed().await.unwrap();
        expected_action_state.client_operation_id = action_state.client_operation_id.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn worker_disconnects_does_not_schedule_for_execution_test() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let worker_id = WorkerId("worker_id".to_string());
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    // Now act like the worker disconnected.
    drop(rx_from_worker);

    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;
    {
        // Client should get notification saying it's being queued not executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Queued,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

// TODO(palfrey) These should be gneralized and expanded for more tests.
struct MockAwaitedActionSubscriber {}
impl AwaitedActionSubscriber for MockAwaitedActionSubscriber {
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        unreachable!();
    }

    async fn borrow(&self) -> Result<AwaitedAction, Error> {
        Ok(AwaitedAction::new(
            OperationId::default(),
            make_base_action_info(SystemTime::UNIX_EPOCH, DigestInfo::zero_digest()),
            MockSystemTime::now().into(),
        ))
    }
}

struct TxMockSenders {
    get_awaited_action_by_id:
        mpsc::UnboundedSender<Result<Option<MockAwaitedActionSubscriber>, Error>>,
    get_by_operation_id: mpsc::UnboundedSender<Result<Option<MockAwaitedActionSubscriber>, Error>>,
    get_range_of_actions: mpsc::UnboundedSender<Vec<Result<MockAwaitedActionSubscriber, Error>>>,
    update_awaited_action: mpsc::UnboundedSender<Result<(), Error>>,
}

#[derive(MetricsComponent)]
struct RxMockAwaitedAction {
    get_awaited_action_by_id:
        Mutex<mpsc::UnboundedReceiver<Result<Option<MockAwaitedActionSubscriber>, Error>>>,
    get_by_operation_id:
        Mutex<mpsc::UnboundedReceiver<Result<Option<MockAwaitedActionSubscriber>, Error>>>,
    get_range_of_actions:
        Mutex<mpsc::UnboundedReceiver<Vec<Result<MockAwaitedActionSubscriber, Error>>>>,
    update_awaited_action: Mutex<mpsc::UnboundedReceiver<Result<(), Error>>>,
}
impl RxMockAwaitedAction {
    fn new() -> (TxMockSenders, Self) {
        let (tx_get_awaited_action_by_id, rx_get_awaited_action_by_id) = mpsc::unbounded_channel();
        let (tx_get_by_operation_id, rx_get_by_operation_id) = mpsc::unbounded_channel();
        let (tx_get_range_of_actions, rx_get_range_of_actions) = mpsc::unbounded_channel();
        let (tx_update_awaited_action, rx_update_awaited_action) = mpsc::unbounded_channel();
        (
            TxMockSenders {
                get_awaited_action_by_id: tx_get_awaited_action_by_id,
                get_by_operation_id: tx_get_by_operation_id,
                get_range_of_actions: tx_get_range_of_actions,
                update_awaited_action: tx_update_awaited_action,
            },
            Self {
                get_awaited_action_by_id: Mutex::new(rx_get_awaited_action_by_id),
                get_by_operation_id: Mutex::new(rx_get_by_operation_id),
                get_range_of_actions: Mutex::new(rx_get_range_of_actions),
                update_awaited_action: Mutex::new(rx_update_awaited_action),
            },
        )
    }
}
impl AwaitedActionDb for RxMockAwaitedAction {
    type Subscriber = MockAwaitedActionSubscriber;

    async fn get_awaited_action_by_id(
        &self,
        _client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        let mut rx_get_awaited_action_by_id = self.get_awaited_action_by_id.lock().await;
        rx_get_awaited_action_by_id
            .try_recv()
            .expect("Could not receive msg in mpsc")
    }

    async fn get_all_awaited_actions(
        &self,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        Ok(futures::stream::empty())
    }

    async fn get_by_operation_id(
        &self,
        _operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        let mut rx_get_by_operation_id = self.get_by_operation_id.lock().await;
        rx_get_by_operation_id
            .try_recv()
            .expect("Could not receive msg in mpsc")
    }

    async fn get_range_of_actions(
        &self,
        _state: SortedAwaitedActionState,
        _start: Bound<SortedAwaitedAction>,
        _end: Bound<SortedAwaitedAction>,
        _desc: bool,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        let mut rx_get_range_of_actions = self.get_range_of_actions.lock().await;
        let items = rx_get_range_of_actions
            .try_recv()
            .expect("Could not receive msg in mpsc");
        Ok(futures::stream::iter(items))
    }

    async fn update_awaited_action(&self, _new_awaited_action: AwaitedAction) -> Result<(), Error> {
        let mut rx_update_awaited_action = self.update_awaited_action.lock().await;
        rx_update_awaited_action
            .try_recv()
            .expect("Could not receive msg in mpsc")
    }

    async fn add_action(
        &self,
        _client_operation_id: OperationId,
        _action_info: Arc<ActionInfo>,
        _no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        unreachable!();
    }
}

#[nativelink_test]
async fn matching_engine_fails_sends_abort() -> Result<(), Error> {
    {
        let task_change_notify = Arc::new(Notify::new());
        let (senders, awaited_action) = RxMockAwaitedAction::new();

        let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
            &SimpleSpec::default(),
            awaited_action,
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None,
            None, // cas_store
            None, // locality_map
            None, // worker_tls_config
        );
        // Initial worker calls do_try_match, so send it no items.
        senders.get_range_of_actions.send(vec![]).unwrap();
        let _worker_rx = setup_new_worker(
            &scheduler,
            WorkerId("worker_id".to_string()),
            PlatformProperties::default(),
        )
        .await
        .unwrap();

        senders
            .get_awaited_action_by_id
            .send(Ok(Some(MockAwaitedActionSubscriber {})))
            .unwrap();
        senders
            .get_by_operation_id
            .send(Ok(Some(MockAwaitedActionSubscriber {})))
            .unwrap();
        // This one gets called twice because of Abort triggers retry, just return item not exist on retry.
        senders.get_by_operation_id.send(Ok(None)).unwrap();
        senders
            .get_range_of_actions
            .send(vec![Ok(MockAwaitedActionSubscriber {})])
            .unwrap();
        senders
            .update_awaited_action
            .send(Err(make_err!(
                Code::Aborted,
                "This means data version did not match."
            )))
            .unwrap();

        assert_eq!(scheduler.do_try_match_for_test().await, Ok(()));
    }
    {
        let task_change_notify = Arc::new(Notify::new());
        let (senders, awaited_action) = RxMockAwaitedAction::new();

        let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
            &SimpleSpec::default(),
            awaited_action,
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None,
            None, // cas_store
            None, // locality_map
            None, // worker_tls_config
        );
        // senders.tx_get_awaited_action_by_id.send(Ok(None)).unwrap();
        senders.get_range_of_actions.send(vec![]).unwrap();
        let _worker_rx = setup_new_worker(
            &scheduler,
            WorkerId("worker_id".to_string()),
            PlatformProperties::default(),
        )
        .await
        .unwrap();

        senders
            .get_awaited_action_by_id
            .send(Ok(Some(MockAwaitedActionSubscriber {})))
            .unwrap();
        senders
            .get_by_operation_id
            .send(Ok(Some(MockAwaitedActionSubscriber {})))
            .unwrap();
        senders
            .get_range_of_actions
            .send(vec![Ok(MockAwaitedActionSubscriber {})])
            .unwrap();
        senders
            .update_awaited_action
            .send(Err(make_err!(
                Code::Internal,
                "This means an internal error happened."
            )))
            .unwrap();

        assert_eq!(
            scheduler.do_try_match_for_test().await.unwrap_err().code,
            Code::Internal
        );
    }

    Ok(())
}

#[nativelink_test]
async fn worker_timesout_reschedules_running_job_test() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));

    let worker_id1 = WorkerId("worker1".to_string());
    let worker_id2 = WorkerId("worker2".to_string());
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    // Note: This needs to stay in scope or a disconnect will trigger.
    let mut rx_from_worker1 = setup_new_worker(
        &scheduler,
        worker_id1.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Note: This needs to stay in scope or a disconnect will trigger.
    let mut rx_from_worker2 = setup_new_worker(
        &scheduler,
        worker_id2.clone(),
        PlatformProperties::default(),
    )
    .await?;

    let mut start_execute = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "UNKNOWN HERE, WE WILL SET IT LATER".to_string(),
        queued_timestamp: Some(insert_timestamp.into()),
        platform: Some(Platform::default()),
        worker_id: worker_id1.to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
    };

    {
        // Worker1 should now see execution request.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        let operation_id = if let update_for_worker::Update::StartAction(start_execute) =
            msg_for_worker.update.as_ref().unwrap()
        {
            start_execute.operation_id.clone()
        } else {
            panic!("Expected StartAction, got : {msg_for_worker:?}");
        };
        start_execute.operation_id.clone_from(&operation_id);
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    start_execute.clone()
                )),
            }
        );
    }

    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        assert_eq!(
            action_state.as_ref(),
            &ActionState {
                client_operation_id: action_state.client_operation_id.clone(),
                stage: ActionStage::Executing,
                action_digest: action_state.action_digest,
                last_transition_timestamp: SystemTime::now(),
            }
        );
    }

    // Keep worker 2 alive at 2x timeout so it survives both phases.
    scheduler
        .worker_keep_alive_received(&worker_id2, NOW_TIME + 2 * WORKER_TIMEOUT_S)
        .await?;
    // Phase 1: quarantine worker 1 at 1x timeout (stops receiving new work).
    scheduler
        .remove_timedout_workers(NOW_TIME + WORKER_TIMEOUT_S)
        .await?;
    tokio::task::yield_now().await;
    // Phase 2: evict worker 1 at 2x timeout (fully removed, job rescheduled).
    scheduler
        .remove_timedout_workers(NOW_TIME + 2 * WORKER_TIMEOUT_S)
        .await?;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    {
        // Worker1 should have received a disconnect message.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::Disconnect(()))
            }
        );
    }
    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        assert_eq!(
            action_state.as_ref(),
            &ActionState {
                client_operation_id: action_state.client_operation_id.clone(),
                stage: ActionStage::Executing,
                action_digest: action_state.action_digest,
                last_transition_timestamp: SystemTime::now(),
            }
        );
    }
    {
        start_execute.worker_id = worker_id2.to_string();
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(start_execute)),
            }
        );
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_sends_completed_result_to_client_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                // Other tests check full data. We only care if client thinks we are Executing.
                assert_eq!(
                    action_listener.changed().await.unwrap().0.stage,
                    ActionStage::Executing
                );
                start_execute.operation_id
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
    };

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("hello".to_string()),
            digest: DigestInfo::new([5u8; 32], 18),
            is_executable: true,
        }],
        output_folders: vec![DirectoryInfo {
            path: "123".to_string(),
            tree_digest: DigestInfo::new([9u8; 32], 100),
        }],
        output_file_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo".to_string()),
            target: "bar".to_string(),
        }],
        output_directory_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo2".to_string()),
            target: "bar2".to_string(),
        }],
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    scheduler
        .update_action(
            &worker_id,
            &OperationId::from(operation_id),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await?;

    {
        // Client should get notification saying it has been completed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(action_result),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_sends_completed_result_after_disconnect() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let client_id = action_listener
        .as_state()
        .await
        .unwrap()
        .0
        .client_operation_id
        .clone();

    // Drop our receiver and don't reconnect until completed.
    drop(action_listener);

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        OperationId::from(operation_id)
    };

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("hello".to_string()),
            digest: DigestInfo::new([5u8; 32], 18),
            is_executable: true,
        }],
        output_folders: vec![DirectoryInfo {
            path: "123".to_string(),
            tree_digest: DigestInfo::new([9u8; 32], 100),
        }],
        output_file_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo".to_string()),
            target: "bar".to_string(),
        }],
        output_directory_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo2".to_string()),
            target: "bar2".to_string(),
        }],
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    scheduler
        .update_action(
            &worker_id,
            &operation_id,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await?;

    // Now look up a channel after the action has completed.
    let mut action_listener = scheduler
        .filter_operations(OperationFilter {
            client_operation_id: Some(client_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .next()
        .await
        .expect("Action not found");
    {
        // Client should get notification saying it has been completed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(action_result),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_with_wrong_worker_id_errors_test() -> Result<(), Error> {
    let good_worker_id = WorkerId("good_worker_id".to_string());
    let rogue_worker_id = WorkerId("rogue_worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker = setup_new_worker(
        &scheduler,
        good_worker_id.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
    }
    drop(
        setup_new_worker(
            &scheduler,
            rogue_worker_id.clone(),
            PlatformProperties::default(),
        )
        .await?,
    );

    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_file_symlinks: Vec::default(),
        output_directory_symlinks: Vec::default(),
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: good_worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    let update_action_result = scheduler
        .update_action(
            &rogue_worker_id,
            &OperationId::default(),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await;

    {
        const EXPECTED_ERR: &str = "should not be running on worker";
        // Our request should have sent an error back.
        assert!(
            update_action_result.is_err(),
            "Expected error, got: {:?}",
            &update_action_result
        );
        let err = update_action_result.unwrap_err();
        assert!(
            err.to_string().contains(EXPECTED_ERR),
            "Error should contain '{EXPECTED_ERR}', got: {err:?}",
        );
    }
    {
        // Ensure client did not get notified.
        assert_eq!(
            poll!(action_listener.changed()),
            Poll::Pending,
            "Client should not have been notified of event"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn does_not_crash_if_operation_joined_then_relaunched() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let client_operation_id = OperationId::default();
    let mut expected_action_state = ActionState {
        client_operation_id,
        stage: ActionStage::Executing,
        action_digest,
        last_transition_timestamp: SystemTime::now(),
    };

    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
            .await
            .unwrap();
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default())
            .await
            .unwrap();

    let operation_id = {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
                platform: Some(Platform::default()),
                worker_id: worker_id.clone().into(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                        missing_digest_peers: Vec::new(),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(
            expected_msg_for_worker,
            msg_for_worker.clone(),
            true
        ));
        match msg_for_worker.update.unwrap() {
            update_for_worker::Update::StartAction(start_execute) => {
                OperationId::from(start_execute.operation_id)
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
    };

    {
        // Client should get notification saying it's being executed.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        expected_action_state.client_operation_id = action_state.client_operation_id.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_directory_symlinks: Vec::default(),
        output_file_symlinks: Vec::default(),
        exit_code: Default::default(),
        stdout_digest: DigestInfo::new([1u8; 32], 512),
        stderr_digest: DigestInfo::new([2u8; 32], 512),
        execution_metadata: ExecutionMetadata {
            worker: String::new(),
            queued_timestamp: SystemTime::UNIX_EPOCH,
            worker_start_timestamp: SystemTime::UNIX_EPOCH,
            worker_completed_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
            execution_start_timestamp: SystemTime::UNIX_EPOCH,
            execution_completed_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };

    scheduler
        .update_action(
            &worker_id,
            &operation_id,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await
        .unwrap();

    {
        // Action should now be executing.
        expected_action_state.stage = ActionStage::Completed(action_result.clone());
        expected_action_state.last_transition_timestamp = SystemTime::now();
        assert_eq!(
            action_listener.changed().await.unwrap().0.as_ref(),
            &expected_action_state
        );
    }

    // Now we need to ensure that if we schedule another execution of the same job it doesn't
    // fail.

    {
        let insert_timestamp = make_system_time(1);
        let mut action_listener =
            setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
                .await
                .unwrap();
        // We didn't disconnect our worker, so it will have scheduled it to the worker.
        expected_action_state.stage = ActionStage::Executing;
        expected_action_state.last_transition_timestamp = SystemTime::now();
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        // The name of the action changed (since it's a new action), so update it.
        expected_action_state.client_operation_id = action_state.client_operation_id.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

/// This tests to ensure that platform property restrictions allow jobs to continue to run after
/// a job finished on a specific worker (eg: restore platform properties).
#[nativelink_test]
async fn run_two_jobs_on_same_worker_with_platform_properties_restrictions() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let mut supported_props = HashMap::new();
    supported_props.insert("prop1".to_string(), PropertyType::Minimum);
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(supported_props),
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest1 = DigestInfo::new([11u8; 32], 512);
    let action_digest2 = DigestInfo::new([99u8; 32], 512);

    let mut properties = HashMap::new();
    properties.insert("prop1".to_string(), PlatformPropertyValue::Minimum(1.0));
    let platform_properties = PlatformProperties {
        properties: properties.clone(),
    };
    let action_props: HashMap<String, String> = properties
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().into_owned()))
        .collect();
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), platform_properties.clone())
            .await
            .unwrap();
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest1,
        action_props.clone(),
        insert_timestamp1,
    )
    .await
    .unwrap();
    let insert_timestamp2 = make_system_time(1);
    let mut client2_action_listener =
        setup_action(&scheduler, action_digest2, action_props, insert_timestamp2)
            .await
            .unwrap();

    let operation_id1 = match rx_from_worker.recv().await.unwrap().update {
        Some(update_for_worker::Update::StartAction(start_execute)) => {
            OperationId::from(start_execute.operation_id)
        }
        v => panic!("Expected StartAction, got : {v:?}"),
    };
    {
        let (state_1, _maybe_origin_metadata) = client1_action_listener.changed().await.unwrap();
        let (state_2, _maybe_origin_metadata) = client2_action_listener.changed().await.unwrap();
        // First client should be in an Executing state.
        assert_eq!(state_1.stage, ActionStage::Executing);
        // Second client should be in a queued state.
        assert_eq!(state_2.stage, ActionStage::Queued);
    }

    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_file_symlinks: Vec::default(),
        output_directory_symlinks: Vec::default(),
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };

    // Tell scheduler our first task is completed.
    scheduler
        .update_action(
            &worker_id,
            &operation_id1,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await
        .unwrap();

    {
        // First action should now be completed.
        let (action_state, _maybe_origin_metadata) =
            client1_action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(action_result.clone()),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // At this stage it should have added back any platform_properties and the next
    // task should be executing on the same worker.

    let operation_id2 = {
        // Our second client should now executing.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                OperationId::from(start_execute.operation_id)
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            client2_action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        operation_id
    };

    // Tell scheduler our second task is completed.
    scheduler
        .update_action(
            &worker_id,
            &operation_id2,
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await
        .unwrap();

    {
        // Our second client should be notified it completed.
        let (action_state, _maybe_origin_metadata) =
            client2_action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(action_result.clone()),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

/// This tests that actions are performed in the order they were queued.
#[nativelink_test]
async fn run_jobs_in_the_order_they_were_queued() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let mut supported_props = HashMap::new();
    supported_props.insert("prop1".to_string(), PropertyType::Minimum);
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(supported_props),
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest1 = DigestInfo::new([11u8; 32], 512);
    let action_digest2 = DigestInfo::new([99u8; 32], 512);

    // Use property to restrict the worker to a single action at a time.
    let mut properties = HashMap::new();
    properties.insert("prop1".to_string(), PlatformPropertyValue::Minimum(1.0));
    let action_props: HashMap<String, String> = properties
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().into_owned()))
        .collect();
    let platform_properties = PlatformProperties { properties };
    // This is queued after the next one (even though it's placed in the map
    // first), so it should execute second.
    let insert_timestamp2 = make_system_time(2);
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest2,
        action_props.clone(),
        insert_timestamp2,
    )
    .await?;
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener =
        setup_action(&scheduler, action_digest1, action_props, insert_timestamp1).await?;

    // Add the worker after the queue has been set up.
    let mut rx_from_worker = setup_new_worker(&scheduler, worker_id, platform_properties).await?;

    match rx_from_worker.recv().await.unwrap().update {
        Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
        v => panic!("Expected StartAction, got : {v:?}"),
    }
    {
        // First client should be in an Executing state.
        assert_eq!(
            client1_action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        // Second client should be in a queued state.
        assert_eq!(
            client2_action_listener.changed().await.unwrap().0.stage,
            ActionStage::Queued
        );
    }

    Ok(())
}

#[nativelink_test]
async fn worker_retries_on_internal_error_and_fails_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            max_job_retries: 1,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        OperationId::from(operation_id.as_str())
    };

    drop(
        scheduler
            .update_action(
                &worker_id,
                &operation_id,
                UpdateOperationType::UpdateWithError(make_err!(Code::Internal, "Some error")),
            )
            .await,
    );

    {
        // Client should get notification saying it has been queued again.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Queued,
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Now connect a new worker and it should pickup the action.
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
    }

    let err = make_err!(Code::Internal, "Some error");
    // Send internal error from worker again.
    drop(
        scheduler
            .update_action(
                &worker_id,
                &operation_id,
                UpdateOperationType::UpdateWithError(err.clone()),
            )
            .await,
    );

    {
        // Client should get notification saying it has been queued again.
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(ActionResult {
                output_files: Vec::default(),
                output_folders: Vec::default(),
                output_file_symlinks: Vec::default(),
                output_directory_symlinks: Vec::default(),
                exit_code: INTERNAL_ERROR_EXIT_CODE,
                stdout_digest: DigestInfo::zero_digest(),
                stderr_digest: DigestInfo::zero_digest(),
                execution_metadata: ExecutionMetadata {
                    worker: worker_id.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                },
                server_logs: HashMap::default(),
                error: Some(err.clone()),
                message: String::new(),
            }),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        let mut received_state = action_state.as_ref().clone();
        if let ActionStage::Completed(stage) = &mut received_state.stage {
            if let Some(real_err) = &mut stage.error {
                assert!(
                    real_err
                        .to_string()
                        .contains("Job cancelled because it attempted to execute too many times"),
                    "{real_err} did not contain 'Job cancelled because it attempted to execute too many times'",
                );
                *real_err = err;
            }
        } else {
            panic!("Expected Completed, got : {:?}", action_state.stage);
        }
        assert_eq!(received_state, expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn ensure_scheduler_drops_inner_spawn() -> Result<(), Error> {
    struct DropChecker {
        dropped: Arc<AtomicBool>,
    }
    impl Drop for DropChecker {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let drop_checker = Arc::new(DropChecker {
        dropped: dropped.clone(),
    });

    // Since the inner spawn owns this callback, we can use the callback to know if the
    // inner spawn was dropped because our callback would be dropped, which dropps our
    // DropChecker.
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        move || {
            // This will ensure dropping happens if this function is ever dropped.
            let _drop_checker = drop_checker.clone();
            async move {}
        },
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    assert_eq!(dropped.load(Ordering::Relaxed), false);

    drop(scheduler);
    tokio::task::yield_now().await; // The drop may happen in a different task.

    // Ensure our callback was dropped.
    assert_eq!(dropped.load(Ordering::Relaxed), true);

    Ok(())
}

/// Regression test for: <https://github.com/TraceMachina/nativelink/issues/257>.
#[nativelink_test]
async fn ensure_task_or_worker_change_notification_received_test() -> Result<(), Error> {
    let worker_id1 = WorkerId("worker1".to_string());
    let worker_id2 = WorkerId("worker2".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker1 = setup_new_worker(
        &scheduler,
        worker_id1.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        HashMap::new(),
        make_system_time(1),
    )
    .await?;

    let mut rx_from_worker2 = setup_new_worker(
        &scheduler,
        worker_id2.clone(),
        PlatformProperties::default(),
    )
    .await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker1.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        OperationId::from(operation_id.as_str())
    };

    drop(
        scheduler
            .update_action(
                &worker_id1,
                &operation_id,
                UpdateOperationType::UpdateWithError(make_err!(Code::NotFound, "Some error")),
            )
            .await,
    );

    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    // Now connect a new worker and it should pickup the action.
    {
        // Other tests check full data. We only care if we got StartAction.
        rx_from_worker2
            .recv()
            .await
            .err_tip(|| "worker went away")?;
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
    }

    Ok(())
}

// Note: This is a regression test for:
// https://github.com/TraceMachina/nativelink/issues/1197
#[nativelink_test]
async fn client_reconnect_keeps_action_alive() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let insert_timestamp = make_system_time(1);
    let action_listener = setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
        .await
        .unwrap();

    let client_id = action_listener
        .as_state()
        .await
        .unwrap()
        .0
        .client_operation_id
        .clone();

    // Simulate client disconnecting.
    drop(action_listener);

    let mut new_action_listener = scheduler
        .filter_operations(OperationFilter {
            client_operation_id: Some(client_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .next()
        .await
        .expect("Action not found");

    // We should get one notification saying it's queued.
    assert_eq!(
        new_action_listener.changed().await.unwrap().0.stage,
        ActionStage::Queued
    );

    let changed_fut = new_action_listener.changed();
    tokio::pin!(changed_fut);

    // Now increment time and ensure the action does not get evicted.
    for _ in 0..500 {
        MockClock::advance(Duration::from_secs(2));
        // All others should be pending.
        assert_eq!(poll!(&mut changed_fut), Poll::Pending);
        tokio::task::yield_now().await;
        // Eviction happens when someone touches the internal
        // evicting map.  So we constantly ask for all queued actions.
        // Regression: https://github.com/TraceMachina/nativelink/issues/1579
        let mut stream = scheduler
            .filter_operations(OperationFilter {
                stages: OperationStageFlags::Queued,
                ..Default::default()
            })
            .await?;
        while stream.next().await.is_some() {}
    }

    Ok(())
}

#[nativelink_test]
async fn client_timesout_job_then_same_action_requested() -> Result<(), Error> {
    const CLIENT_ACTION_TIMEOUT_S: u64 = 60;
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            client_action_timeout_s: CLIENT_ACTION_TIMEOUT_S,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    {
        let insert_timestamp = make_system_time(1);
        let mut action_listener =
            setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
                .await
                .unwrap();

        // We should get one notification saying it's queued.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Queued
        );

        let changed_fut = action_listener.changed();
        tokio::pin!(changed_fut);

        MockClock::advance(Duration::from_secs(2));
        scheduler.do_try_match_for_test().await.unwrap();
        assert_eq!(poll!(&mut changed_fut), Poll::Pending);
    }

    MockClock::advance(Duration::from_secs(CLIENT_ACTION_TIMEOUT_S + 1));

    {
        let insert_timestamp = make_system_time(1);
        let mut action_listener =
            setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
                .await
                .unwrap();

        // We should get one notification saying it's queued.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Queued
        );

        let changed_fut = action_listener.changed();
        tokio::pin!(changed_fut);

        MockClock::advance(Duration::from_secs(2));
        tokio::task::yield_now().await;
        assert_eq!(poll!(&mut changed_fut), Poll::Pending);
    }

    Ok(())
}

#[nativelink_test]
async fn logs_when_no_workers_match() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let mut prop_defs = HashMap::new();
    prop_defs.insert("prop".to_string(), PropertyType::Minimum);

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_match_logging_interval_s: 1,
            supported_platform_properties: Some(prop_defs),
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut required_platform_properties = HashMap::new();
    required_platform_properties.insert("prop".to_string(), "1".to_string());

    let mut worker_properties = PlatformProperties::default();
    worker_properties
        .properties
        .insert("prop".to_string(), PlatformPropertyValue::Minimum(0.0));

    setup_new_worker(&scheduler, worker_id.clone(), worker_properties).await?;

    setup_action(
        &scheduler,
        action_digest,
        required_platform_properties,
        make_system_time(1),
    )
    .await
    .unwrap();

    scheduler.do_try_match_for_test().await?;

    assert!(logs_contain(
        "Property mismatch on worker property prop. Minimum(0.0) < Minimum(1.0)"
    ));
    assert!(logs_contain("No workers matched"));

    Ok(())
}

#[nativelink_test]
async fn worker_fails_precondition_completes_immediately_test() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            max_job_retries: 5,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        OperationId::from(operation_id.as_str())
    };

    let err = make_err!(Code::FailedPrecondition, "Missing input blobs");
    // Send FailedPrecondition error from worker. This should NOT be retried
    // even though max_job_retries is 5.
    drop(
        scheduler
            .update_action(
                &worker_id,
                &operation_id,
                UpdateOperationType::UpdateWithError(err.clone()),
            )
            .await,
    );

    {
        // Client should get notification saying the action completed (not re-queued).
        let (action_state, _maybe_origin_metadata) = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            client_operation_id: action_state.client_operation_id.clone(),
            stage: ActionStage::Completed(ActionResult {
                output_files: Vec::default(),
                output_folders: Vec::default(),
                output_file_symlinks: Vec::default(),
                output_directory_symlinks: Vec::default(),
                exit_code: INTERNAL_ERROR_EXIT_CODE,
                stdout_digest: DigestInfo::zero_digest(),
                stderr_digest: DigestInfo::zero_digest(),
                execution_metadata: ExecutionMetadata {
                    worker: worker_id.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                },
                server_logs: HashMap::default(),
                error: Some(err.clone()),
                message: String::new(),
            }),
            action_digest: action_state.action_digest,
            last_transition_timestamp: SystemTime::now(),
        };
        let mut received_state = action_state.as_ref().clone();
        if let ActionStage::Completed(stage) = &mut received_state.stage {
            if let Some(real_err) = &mut stage.error {
                // Verify the error contains the FailedPrecondition message.
                assert!(
                    real_err.to_string().contains("Missing input blobs"),
                    "{real_err} did not contain 'Missing input blobs'",
                );
                assert!(
                    real_err
                        .to_string()
                        .contains("Job cancelled because it attempted to execute too many times"),
                    "{real_err} did not contain 'Job cancelled because it attempted to execute too many times'",
                );
                *real_err = err;
            }
        } else {
            panic!(
                "Expected Completed (not re-queued), got : {:?}",
                action_state.stage
            );
        }
        assert_eq!(received_state, expected_action_state);
    }

    Ok(())
}

// ============================================================================
// Locality-aware scheduling tests
// ============================================================================

/// Helper: adds a worker with a specific CAS endpoint (for locality mapping).
async fn setup_new_worker_with_cas_endpoint(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
    cas_endpoint: &str,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new_with_cas_endpoint(
        worker_id.clone(),
        props,
        tx,
        NOW_TIME,
        0,
        cas_endpoint.to_string(),
        0, // p_core_count (#sched-blend; unknown in this test)
        0, // e_core_count
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

/// Like `setup_new_worker_with_cas_endpoint`, but also sets realistic P/E
/// core counts. Locality (Tier-2) tests need the CAS endpoint for the
/// `endpoint_to_worker` map AND real core counts so the worker is not
/// treated as `assume_core_count`-only. A worker built this way still needs
/// an `update_worker_load(...)` call to become a viable (non-saturated)
/// candidate — a never-reported worker is treated as 100% busy per
/// #sched-zeroload regardless of its core counts.
async fn setup_new_worker_with_cas_endpoint_and_cores(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
    cas_endpoint: &str,
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
        cas_endpoint.to_string(),
        p_core_count,
        e_core_count,
    );
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

/// Helper: schedules an action with a custom `input_root_digest`.
async fn setup_action_with_input_root(
    scheduler: &SimpleScheduler,
    action_digest: DigestInfo,
    input_root_digest: DigestInfo,
    platform_properties: HashMap<String, String>,
    insert_timestamp: SystemTime,
) -> Result<Box<dyn ActionStateResult>, Error> {
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    Arc::make_mut(&mut action_info).platform_properties = platform_properties;
    Arc::make_mut(&mut action_info).input_root_digest = input_root_digest;
    let client_id = OperationId::default();
    let result = scheduler.add_action(client_id, action_info).await;
    tokio::task::yield_now().await;
    result
}

/// Helper: extracts the StartExecute from a worker receiver, returning
/// (operation_id, start_execute, peer_hints). The dispatch path emits
/// `Update::ChunkedMessage(PeerHints)` messages alongside (typically just
/// before) `Update::StartAction`. We drain whatever's in the queue,
/// concatenating peer-hint chunks until we see the matching StartAction
/// (and one terminal chunk OR end-of-stream). Cap the drain at 64
/// messages so a buggy emitter can't hang the test forever.
async fn recv_start_execute(
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) -> (String, StartExecute) {
    let (op_id, se, _hints) = recv_start_execute_with_hints(rx).await;
    (op_id, se)
}

/// As `recv_start_execute` but ALSO returns the concatenated hints from
/// every `PeerHintsChunk` for the matching operation_id. The order of
/// arrival between chunks and `StartAction` is not guaranteed by the
/// protocol — this helper handles either ordering.
async fn recv_start_execute_with_hints(
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) -> (String, StartExecute, Vec<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::PeerHint>) {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        chunked_message, PeerHint,
    };
    let mut start_action: Option<(String, StartExecute)> = None;
    let mut hints: Vec<PeerHint> = Vec::new();
    let mut saw_terminal_chunk = false;
    // The scheduler now SKIPS chunk emission entirely when hints is empty
    // (post-fixup pass for #98). So the exit condition is:
    //   * StartAction received, AND
    //   * either (a) we've seen the terminal chunk, OR (b) the queue is
    //     empty and no more messages are pending.
    // We do a blocking recv() for the StartAction, then a non-blocking
    // try_recv() drain for any chunks the scheduler chose to send.
    for _ in 0..256 {
        let msg = if start_action.is_none() {
            match rx.recv().await {
                Some(m) => m,
                None => break,
            }
        } else {
            // Once StartAction has arrived, do a short bounded wait for
            // any chunks. The scheduler emits chunks before StartAction
            // for non-empty hint lists, but we accept either ordering.
            // For the empty-hint case there will be NO chunks at all —
            // the loop must terminate without hanging.
            match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) | Err(_) => break,
            }
        };
        match msg.update {
            Some(update_for_worker::Update::StartAction(se)) => {
                start_action = Some((se.operation_id.clone(), se));
            }
            Some(update_for_worker::Update::ChunkedMessage(cm)) => match cm.payload {
                Some(chunked_message::Payload::PeerHints(chunk)) => {
                    if chunk.is_last {
                        saw_terminal_chunk = true;
                    }
                    hints.extend(chunk.peer_hints);
                }
                // BIS chunks (#97) are produced by the production
                // broadcast loop in `src/bin/nativelink.rs`, not by the
                // scheduler under test here. Tolerate them defensively in
                // case a future test composition wires the loop in — they
                // are not what this helper asserts on.
                Some(chunked_message::Payload::BlobsInStableStorage(_)) => {}
                // BlobsAvailable chunks share the same property — emitted by
                // a production broadcast loop, not the scheduler-under-test.
                Some(chunked_message::Payload::BlobsAvailable(_)) => {}
                None => panic!("ChunkedMessage with empty payload"),
            },
            v => panic!("Expected StartAction or ChunkedMessage, got: {v:?}"),
        }
        if start_action.is_some() && saw_terminal_chunk {
            break;
        }
    }
    let (op_id, se) = start_action.expect("did not receive StartAction within drain budget");
    (op_id, se, hints)
}

#[nativelink_test]
async fn locality_scoring_selects_best_worker_test() -> Result<(), Error> {
    // Test: When a locality map is populated and CAS store has Directory protos,
    // the worker with the most cached input bytes should be preferred.
    let worker_id_a = WorkerId("worker_a".to_string());
    let worker_id_b = WorkerId("worker_b".to_string());
    let cas_endpoint_a = "worker-a:50081";
    let cas_endpoint_b = "worker-b:50081";

    // Create file digests that will be in the input tree.
    let file_digest1 = DigestInfo::new([1u8; 32], 5000); // 5000 bytes
    let file_digest2 = DigestInfo::new([2u8; 32], 3000); // 3000 bytes
    let file_digest3 = DigestInfo::new([3u8; 32], 2000); // 2000 bytes

    // Build a Directory proto with these files as the input root.
    let input_root_dir = Directory {
        files: vec![
            FileNode {
                name: "file1.txt".to_string(),
                digest: Some(file_digest1.into()),
                is_executable: false,
                ..Default::default()
            },
            FileNode {
                name: "file2.txt".to_string(),
                digest: Some(file_digest2.into()),
                is_executable: false,
                ..Default::default()
            },
            FileNode {
                name: "file3.txt".to_string(),
                digest: Some(file_digest3.into()),
                is_executable: false,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let dir_bytes = input_root_dir.encode_to_vec();
    let input_root_digest = DigestInfo::new(
        {
            use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&dir_bytes);
            let digest_info = hasher.finalize_digest();
            **digest_info.packed_hash()
        },
        dir_bytes.len() as u64,
    );

    // Create a CAS store and populate it with the directory proto.
    let cas_store_inner = MemoryStore::new(&MemorySpec::default());
    let cas_store = Store::new(cas_store_inner.clone());
    let key: nativelink_util::store_trait::StoreKey<'_> = input_root_digest.into();
    cas_store
        .update_oneshot(key, Bytes::from(dir_bytes))
        .await?;

    // Create and populate the locality map.
    // Worker A has file1 (5000) and file3 (2000) = 7000 total.
    // Worker B has file2 (3000) = 3000 total.
    // Worker A should win.
    let locality_map = new_shared_blob_locality_map();
    {
        let mut map = locality_map.write();
        map.register_blobs(cas_endpoint_a, &[file_digest1, file_digest3]);
        map.register_blobs(cas_endpoint_b, &[file_digest2]);
    }

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        Some(cas_store),
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([99u8; 32], 512);

    // Add workers WITH cas_endpoints so the endpoint_to_worker map is populated.
    let mut rx_a = setup_new_worker_with_cas_endpoint(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        cas_endpoint_a,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_cas_endpoint(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        cas_endpoint_b,
    )
    .await?;

    // Schedule the action.
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    // Worker A should get the action because it has the highest locality score (7000 > 3000).
    // The PeerHints chunk + StartAction may interleave; drain whichever rx
    // produces first via `recv_start_execute_with_hints`.
    let (selected_worker_id, _se, _hints) = tokio::select! {
        v = recv_start_execute_with_hints(&mut rx_a) => {
            (worker_id_a.clone(), v.1, v.2)
        }
        v = recv_start_execute_with_hints(&mut rx_b) => {
            (worker_id_b.clone(), v.1, v.2)
        }
    };

    assert_eq!(
        selected_worker_id, worker_id_a,
        "Locality scoring should select worker_a (7000 cached bytes > worker_b's 3000)"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

/// (#p1p2 enqueue-hook wiring) Proves the PRODUCTION seam
/// `SimpleScheduler::inner_add_action → worker_scheduler.prefetch_input_tree`
/// is live: adding an action whose `input_root_digest` names a real tree in a
/// configured `cas_store` must trigger an enqueue-time prefetch. The prefetch
/// counter `tree_prefetch_issued` is bumped SYNCHRONOUSLY inside
/// `prefetch_input_tree` (before the spawn), so after `add_action` returns it
/// is deterministically 1 — no wait needed.
///
/// The existing `simple_scheduler_test.rs` harness always passed
/// `cas_store: None` (so the prefetch early-returns on the no-CAS guard and
/// the seam was UNTESTED). This variant wires `Some(cas_store)` — the same
/// shape `locality_scoring_selects_best_worker_test` proves is drivable — and
/// observes the counter through the PRODUCTION `/metrics` render path (the
/// returned `Arc<dyn WorkerScheduler>` is `RootMetricsComponent`, registered
/// exactly as `src/bin/nativelink.rs` registers it).
///
/// Mutation step (CLAUDE.md TDD #5): comment out the
/// `self.worker_scheduler.prefetch_input_tree(...).await;` call in
/// `SimpleScheduler::inner_add_action` — `tree_prefetch_issued` stays 0 and
/// this test red-fails with its bespoke "enqueue seam did not fire" message.
#[nativelink_test]
async fn enqueue_triggers_input_tree_prefetch_test() -> Result<(), Error> {
    use nativelink_util::metrics_publisher::{
        MetricsComponentTrait, MetricsRegistry, render_prometheus,
    };

    // Build a real single-directory input tree and store it in the CAS.
    let input_root_dir = Directory {
        files: vec![FileNode {
            name: "prefetch_seam.txt".to_string(),
            digest: Some(DigestInfo::new([7u8; 32], 1234).into()),
            is_executable: false,
            ..Default::default()
        }],
        ..Default::default()
    };
    let dir_bytes = input_root_dir.encode_to_vec();
    let input_root_digest = DigestInfo::new(
        {
            use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&dir_bytes);
            let digest_info = hasher.finalize_digest();
            **digest_info.packed_hash()
        },
        dir_bytes.len() as u64,
    );

    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let key: nativelink_util::store_trait::StoreKey<'_> = input_root_digest.into();
    cas_store
        .update_oneshot(key, Bytes::from(dir_bytes))
        .await?;

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        Some(cas_store), // (#p1p2) CAS store present — the prefetch is not no-op'd
        Some(new_shared_blob_locality_map()),
        None, // worker_tls_config
    );

    // Add an action whose input root names the stored tree. No worker is
    // registered — this isolates the ENQUEUE-time prefetch from any
    // match-time lazy resolution (no worker ⇒ no match ⇒ no lazy resolve).
    let action_digest = DigestInfo::new([99u8; 32], 512);
    let _action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        make_system_time(1),
    )
    .await?;

    // Render the worker scheduler's metrics exactly as production does and
    // assert the enqueue seam fired. `tree_prefetch_issued` is incremented
    // synchronously in `prefetch_input_tree`, so it is already 1 here.
    let registry = MetricsRegistry::new();
    registry.register_dyn(
        "scheduler.testsched.worker",
        worker_scheduler as Arc<dyn MetricsComponentTrait + Send + Sync>,
    );
    // Warm the lazy span-thread-local path once and discard (same de-flake the
    // unit render test uses; avoids the 1/47 cold whole-group-vanishes flake).
    let _warm = render_prometheus(&registry);
    let body = render_prometheus(&registry);

    assert!(
        body.contains(
            "\nscheduler_testsched_worker_scheduler_metrics_tree_prefetch_issued 1\n"
        ),
        "enqueue seam did not fire: SimpleScheduler::inner_add_action must call \
         worker_scheduler.prefetch_input_tree for an action with a cas_store set, \
         bumping tree_prefetch_issued to 1. body=\n{body}"
    );
    // Neither skip counter should have fired: the root is cold (not cached) and
    // a fresh scheduler has all prefetch permits.
    assert!(
        body.contains(
            "\nscheduler_testsched_worker_scheduler_metrics_tree_prefetch_skipped_cached 0\n"
        ),
        "a cold (uncached) root must not count as skipped_cached. body=\n{body}"
    );
    assert!(
        body.contains(
            "\nscheduler_testsched_worker_scheduler_metrics_tree_prefetch_skipped_nopermit 0\n"
        ),
        "a fresh scheduler has permits — must not count as skipped_nopermit. body=\n{body}"
    );

    // (#p1p2, assumption-auditor) Pin the NINE cold-resolution latency histogram
    // bucket names on the REAL /metrics render path (dashboards key on them; the
    // sibling prefetch counters above have render pins, these did not). All nine
    // are `SchedulerMetrics` fields under the same worker_scheduler_metrics group,
    // so they render (at their default 0 here) whenever the group renders. This
    // pins the NAME + boundary set; a bucket rename or a boundary-constant edit
    // that changes the emitted name red-fails here.
    for bucket in [
        "tree_resolution_ms_le_50",
        "tree_resolution_ms_le_100",
        "tree_resolution_ms_le_250",
        "tree_resolution_ms_le_500",
        "tree_resolution_ms_le_1000",
        "tree_resolution_ms_le_2000",
        "tree_resolution_ms_le_5000",
        "tree_resolution_ms_le_30000",
        "tree_resolution_ms_gt_30000",
    ] {
        assert!(
            body.contains(&format!(
                "\nscheduler_testsched_worker_scheduler_metrics_{bucket} "
            )),
            "#p1p2 MISSING histogram bucket: \
             scheduler_testsched_worker_scheduler_metrics_{bucket} must render on the real \
             /metrics path (dashboards key on the 9-bucket cold-resolution latency histogram). \
             body=\n{body}"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn no_peer_hints_without_resolved_tree_test() -> Result<(), Error> {
    // Test: When a locality map has entries for the input_root_digest itself
    // but there is no CAS store / no resolved tree, peer hints should be
    // empty. The old fallback that generated a single hint for
    // input_root_digest never worked because workers register individual
    // file digests, not directory digests.
    let worker_id = WorkerId("worker_recv".to_string());
    let peer_endpoint = "peer-worker:50081";

    let input_root = DigestInfo::new([77u8; 32], 4096);

    // Create locality map and register the input_root_digest on a peer endpoint.
    let locality_map = new_shared_blob_locality_map();
    {
        let mut map = locality_map.write();
        map.register_blobs(peer_endpoint, &[input_root]);
    }

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // no CAS store -- no resolved tree available
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([88u8; 32], 256);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    // Schedule action with a specific input_root.
    let insert_timestamp = make_system_time(1);
    let _action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    // Worker should receive StartAction with no peer-hints chunks (no resolved tree).
    let (_, _start_execute, peer_hints) =
        recv_start_execute_with_hints(&mut rx_from_worker).await;

    assert!(
        peer_hints.is_empty(),
        "peer-hints chunks should be empty without a resolved tree (directory digests are not useful)"
    );

    Ok(())
}

#[nativelink_test]
async fn peer_hints_from_resolved_tree_test() -> Result<(), Error> {
    // Test: When a CAS store has a Directory proto for the input root, and
    // the locality map has entries for individual file digests, the
    // StartExecute message should contain per-file peer hints sorted by
    // size descending.
    let worker_id = WorkerId("worker_recv".to_string());
    let peer_endpoint = "peer-worker:50081";

    // Create file digests.
    let file_large = DigestInfo::new([10u8; 32], 10000);
    let file_small = DigestInfo::new([11u8; 32], 500);

    // Build Directory proto.
    let input_root_dir = Directory {
        files: vec![
            FileNode {
                name: "large.bin".to_string(),
                digest: Some(file_large.into()),
                is_executable: false,
                ..Default::default()
            },
            FileNode {
                name: "small.txt".to_string(),
                digest: Some(file_small.into()),
                is_executable: false,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let dir_bytes = input_root_dir.encode_to_vec();
    let input_root_digest = DigestInfo::new(
        {
            use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&dir_bytes);
            let digest_info = hasher.finalize_digest();
            **digest_info.packed_hash()
        },
        dir_bytes.len() as u64,
    );

    // Create and populate CAS store.
    let cas_store_inner = MemoryStore::new(&MemorySpec::default());
    let cas_store = Store::new(cas_store_inner);
    let key: nativelink_util::store_trait::StoreKey<'_> = input_root_digest.into();
    cas_store
        .update_oneshot(key, Bytes::from(dir_bytes))
        .await?;

    // Create locality map with file blobs registered on a peer.
    let locality_map = new_shared_blob_locality_map();
    {
        let mut map = locality_map.write();
        map.register_blobs(peer_endpoint, &[file_large, file_small]);
    }

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        Some(cas_store),
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    let insert_timestamp = make_system_time(1);
    let _action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    let (_, _start_execute, peer_hints) =
        recv_start_execute_with_hints(&mut rx_from_worker).await;

    // Should have per-file peer hints (one per file in the tree).
    assert_eq!(
        peer_hints.len(),
        2,
        "Should have 2 peer hints (one per file in the input tree)"
    );

    // Hints should be sorted by size descending (large first).
    let first_hint_digest = DigestInfo::try_from(
        peer_hints[0]
            .digest
            .as_ref()
            .expect("hint should have digest"),
    )
    .unwrap();
    let second_hint_digest = DigestInfo::try_from(
        peer_hints[1]
            .digest
            .as_ref()
            .expect("hint should have digest"),
    )
    .unwrap();

    assert_eq!(
        first_hint_digest, file_large,
        "First hint should be the largest file"
    );
    assert_eq!(
        second_hint_digest, file_small,
        "Second hint should be the smaller file"
    );

    // Both hints should reference the peer endpoint.
    for hint in &peer_hints {
        assert!(
            hint.peer_endpoints.contains(&peer_endpoint.to_string()),
            "Each hint should reference the peer endpoint"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn fallback_to_lru_when_no_locality_data_test() -> Result<(), Error> {
    // Test: When a locality map and CAS store are configured but contain NO
    // blob data for the action's input tree, the scheduler should fall back
    // to the normal LRU worker selection without errors.
    let worker_id_a = WorkerId("worker_a".to_string());
    let worker_id_b = WorkerId("worker_b".to_string());
    let cas_endpoint_a = "worker-a:50081";
    let cas_endpoint_b = "worker-b:50081";

    // Build a Directory proto with files, but do NOT register those files
    // in the locality map -- simulating a fresh deployment or cold start.
    let file_digest1 = DigestInfo::new([30u8; 32], 4000);
    let file_digest2 = DigestInfo::new([31u8; 32], 2000);

    let input_root_dir = Directory {
        files: vec![
            FileNode {
                name: "cold_file1.bin".to_string(),
                digest: Some(file_digest1.into()),
                is_executable: false,
                ..Default::default()
            },
            FileNode {
                name: "cold_file2.bin".to_string(),
                digest: Some(file_digest2.into()),
                is_executable: false,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let dir_bytes = input_root_dir.encode_to_vec();
    let input_root_digest = DigestInfo::new(
        {
            use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&dir_bytes);
            let digest_info = hasher.finalize_digest();
            **digest_info.packed_hash()
        },
        dir_bytes.len() as u64,
    );

    // Create CAS store with the directory proto so tree resolution succeeds.
    let cas_store_inner = MemoryStore::new(&MemorySpec::default());
    let cas_store = Store::new(cas_store_inner);
    let key: nativelink_util::store_trait::StoreKey<'_> = input_root_digest.into();
    cas_store
        .update_oneshot(key, Bytes::from(dir_bytes))
        .await?;

    // Create an EMPTY locality map -- no blobs registered on any endpoint.
    let locality_map = new_shared_blob_locality_map();

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        Some(cas_store),
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([99u8; 32], 512);

    // Add two workers with CAS endpoints.
    let mut rx_a = setup_new_worker_with_cas_endpoint(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        cas_endpoint_a,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_cas_endpoint(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        cas_endpoint_b,
    )
    .await?;

    // Schedule action with the input root.
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    // One of the workers should receive the action (LRU fallback). The
    // PeerHints chunk + StartAction may interleave; drain whichever rx
    // produces first via `recv_start_execute_with_hints`.
    let (selected_worker_id, _start_execute, peer_hints) = tokio::select! {
        v = recv_start_execute_with_hints(&mut rx_a) => {
            (worker_id_a.clone(), v.1, v.2)
        }
        v = recv_start_execute_with_hints(&mut rx_b) => {
            (worker_id_b.clone(), v.1, v.2)
        }
    };

    // Verify the action was dispatched to one of the two workers.
    assert!(
        selected_worker_id == worker_id_a || selected_worker_id == worker_id_b,
        "Action should be dispatched to one of the available workers via LRU fallback"
    );

    // With no locality data, there should be no peer hints (no blobs are registered).
    assert!(
        peer_hints.is_empty(),
        "peer-hints chunks should be empty when locality map has no data for input files, got {} hints",
        peer_hints.len()
    );

    // Client should see the Executing state.
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn locality_scoring_with_empty_map_and_no_cas_store_test() -> Result<(), Error> {
    // Test: When locality_map is provided but cas_store is None (tree
    // resolution impossible), scheduling should still work via LRU fallback.
    // This covers the path where resolve_input_tree returns None.
    let worker_id = WorkerId("worker_solo".to_string());

    // Create locality map but don't populate it.
    let locality_map = new_shared_blob_locality_map();

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // No CAS store -- tree resolution returns None
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([55u8; 32], 256);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Worker should receive the action via normal LRU selection.
    let (_, _start_execute, peer_hints) =
        recv_start_execute_with_hints(&mut rx_from_worker).await;

    // No peer hints should be generated (no tree, no locality data).
    assert!(
        peer_hints.is_empty(),
        "peer-hints chunks should be empty when no CAS store is configured"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn locality_scoring_partial_data_still_selects_best_worker_test() -> Result<(), Error> {
    // Test: When only SOME workers have locality data, Tier-2 blob-level
    // locality scoring picks the worker holding the most cached input bytes,
    // and the worker with no cached data (score 0) falls behind.
    //
    // (#sched-zeroload) BOTH workers must report load AND carry real core
    // counts so they are NOT treated as never-reported (100% busy → saturated
    // → the cache tiers decline and the cascade falls through to LRU/MRU,
    // which would pick worker_a by LRU order and defeat the point of this
    // test). With reported sub-100 load + real (P,E) counts, both are viable
    // non-saturated candidates, so Tier 2 fires and worker_b (sole holder of
    // file_digest1 = 8000 bytes) wins on cached bytes.
    let worker_id_a = WorkerId("worker_a".to_string());
    let worker_id_b = WorkerId("worker_b".to_string());
    let cas_endpoint_a = "worker-a:50081";
    let cas_endpoint_b = "worker-b:50081";

    // Files in the input tree.
    let file_digest1 = DigestInfo::new([40u8; 32], 8000);
    let file_digest2 = DigestInfo::new([41u8; 32], 1000);

    let input_root_dir = Directory {
        files: vec![
            FileNode {
                name: "big.dat".to_string(),
                digest: Some(file_digest1.into()),
                is_executable: false,
                ..Default::default()
            },
            FileNode {
                name: "small.dat".to_string(),
                digest: Some(file_digest2.into()),
                is_executable: false,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let dir_bytes = input_root_dir.encode_to_vec();
    let input_root_digest = DigestInfo::new(
        {
            use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&dir_bytes);
            let digest_info = hasher.finalize_digest();
            **digest_info.packed_hash()
        },
        dir_bytes.len() as u64,
    );

    // Create CAS store with directory proto.
    let cas_store_inner = MemoryStore::new(&MemorySpec::default());
    let cas_store = Store::new(cas_store_inner);
    let key: nativelink_util::store_trait::StoreKey<'_> = input_root_digest.into();
    cas_store
        .update_oneshot(key, Bytes::from(dir_bytes))
        .await?;

    // Only worker B has file_digest1 (8000 bytes). Worker A has nothing.
    let locality_map = new_shared_blob_locality_map();
    {
        let mut map = locality_map.write();
        map.register_blobs(cas_endpoint_b, &[file_digest1]);
    }

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        Some(cas_store),
        Some(locality_map),
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([99u8; 32], 512);

    // Prod-shaped: CAS endpoint (for the endpoint→worker map) + real (P,E)
    // core counts. p=4, e=6 chosen to match the other prod-shaped tests.
    let mut rx_a = setup_new_worker_with_cas_endpoint_and_cores(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        cas_endpoint_a,
        4,
        6,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_cas_endpoint_and_cores(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        cas_endpoint_b,
        4,
        6,
    )
    .await?;

    // Report a sub-100 load on BOTH so `has_reported_load = true` and
    // `weighted_free > 0` (not saturated): load(30,30,30) with (4,6) counts
    // gives weighted_free = 2*(4*70) + (6*70) = 980. Without this, both are
    // never-reported → saturated → cache tiers decline (see the header
    // comment). Identical load on both keeps the decision purely on cached
    // bytes.
    scheduler.update_worker_load(&worker_id_a, 30, 30, 30).await?;
    scheduler.update_worker_load(&worker_id_b, 30, 30, 30).await?;

    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root_digest,
        HashMap::new(),
        insert_timestamp,
    )
    .await?;

    // Worker B should be selected (8000 cached bytes vs. 0 for worker A).
    // The PeerHints chunk + StartAction may interleave; drain whichever rx
    // produces first via `recv_start_execute_with_hints`.
    let (selected_worker_id, _se, _hints) = tokio::select! {
        v = recv_start_execute_with_hints(&mut rx_a) => {
            (worker_id_a.clone(), v.1, v.2)
        }
        v = recv_start_execute_with_hints(&mut rx_b) => {
            (worker_id_b.clone(), v.1, v.2)
        }
    };

    assert_eq!(
        selected_worker_id, worker_id_b,
        "Tier-2 locality scoring should select worker_b (8000 cached bytes vs. worker_a's 0)"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

// ---------------------------------------------------------------
// CPU-load-aware scheduling tests
// ---------------------------------------------------------------

#[nativelink_test]
async fn cpu_load_update_worker_load_stores_correctly() -> Result<(), Error> {
    // Verify that update_worker_load stores the load on the worker and
    // influences scheduling. We set load on a single worker, submit an
    // action, and confirm the worker still receives it (proving the
    // update didn't break anything and the worker is still viable).
    let worker_id = WorkerId("worker_load_test".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx = setup_new_worker(
        &scheduler,
        worker_id.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // Update the worker's CPU load.
    scheduler.update_worker_load(&worker_id, 42, 0, 0).await?;

    // Submit an action — the single worker should still be selected.
    let action_digest = DigestInfo::new([10u8; 32], 256);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Worker should receive the action.
    let (_op_id, _se) = recv_start_execute(&mut rx).await;

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn cpu_load_lightest_loaded_worker_gets_picked() -> Result<(), Error> {
    // Create 3 workers with different cpu_load_pct values.
    // Worker A=80, Worker B=20, Worker C=50.
    // Worker B (lightest load) should be selected for the action.
    let worker_id_a = WorkerId("worker_a".to_string());
    let worker_id_b = WorkerId("worker_b".to_string());
    let worker_id_c = WorkerId("worker_c".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Add all 3 workers (no queued actions yet, so no matching happens).
    let mut rx_a = setup_new_worker(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_b = setup_new_worker(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_c = setup_new_worker(
        &scheduler,
        worker_id_c.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // Set CPU loads: A=80, B=20, C=50.
    scheduler.update_worker_load(&worker_id_a, 80, 0, 0).await?;
    scheduler.update_worker_load(&worker_id_b, 20, 0, 0).await?;
    scheduler.update_worker_load(&worker_id_c, 50, 0, 0).await?;

    // Submit an action.
    let action_digest = DigestInfo::new([20u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Determine which worker received the action.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_a.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_a, got: {v:?}"),
            };
            (worker_id_a.clone(), se)
        }
        msg = rx_b.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_b, got: {v:?}"),
            };
            (worker_id_b.clone(), se)
        }
        msg = rx_c.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_c, got: {v:?}"),
            };
            (worker_id_c.clone(), se)
        }
    };

    assert_eq!(
        selected_worker_id, worker_id_b,
        "Worker B (cpu_load_pct=20) should be selected as lightest-loaded"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn cpu_load_unknown_zero_sorted_last() -> Result<(), Error> {
    // Create 2 workers: one with cpu_load_pct=60 (known) and one with
    // cpu_load_pct=0 (unknown). The worker with known load should be
    // selected over the unknown one, even though 0 < 60 numerically.
    let worker_id_known = WorkerId("worker_known".to_string());
    let worker_id_unknown = WorkerId("worker_unknown".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_known = setup_new_worker(
        &scheduler,
        worker_id_known.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_unknown = setup_new_worker(
        &scheduler,
        worker_id_unknown.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // Set only one worker's load; the other stays at default 0 (unknown).
    scheduler.update_worker_load(&worker_id_known, 60, 0, 0).await?;
    // worker_unknown stays at cpu_load_pct=0.

    // Submit an action.
    let action_digest = DigestInfo::new([30u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Determine which worker received the action.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_known.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_known, got: {v:?}"),
            };
            (worker_id_known.clone(), se)
        }
        msg = rx_unknown.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_unknown, got: {v:?}"),
            };
            (worker_id_unknown.clone(), se)
        }
    };

    assert_eq!(
        selected_worker_id, worker_id_known,
        "Worker with known load (60) should be preferred over unknown (0)"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn cpu_load_falls_back_to_lru_when_no_load_data() -> Result<(), Error> {
    // Create 2 workers with cpu_load_pct=0 on both (no load data).
    // Scheduling should still work via LRU/MRU fallback.
    let worker_id_1 = WorkerId("worker_1".to_string());
    let worker_id_2 = WorkerId("worker_2".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Add both workers (both have cpu_load_pct=0 by default).
    let mut rx_1 = setup_new_worker(
        &scheduler,
        worker_id_1.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_2 = setup_new_worker(
        &scheduler,
        worker_id_2.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // Neither worker has load data — cpu_load_pct stays at 0.

    // Submit an action. It should be assigned to one of the workers
    // via LRU fallback (the first in LRU order).
    let action_digest = DigestInfo::new([40u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Either worker is acceptable — just verify one was selected.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_1.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_1, got: {v:?}"),
            };
            (worker_id_1.clone(), se)
        }
        msg = rx_2.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_2, got: {v:?}"),
            };
            (worker_id_2.clone(), se)
        }
    };

    // Verify a worker was actually selected (the assert_eq on stage below
    // also proves this, but let's be explicit).
    assert!(
        selected_worker_id == worker_id_1 || selected_worker_id == worker_id_2,
        "One of the workers should have been selected via LRU fallback"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

// ---------------------------------------------------------------
// P/E core scheduling preference tests
// ---------------------------------------------------------------

#[nativelink_test]
async fn p_core_preference_test() -> Result<(), Error> {
    // Two workers with per-core-type load data.
    // Worker A: p=30, e=80, aggregate=50 -> effective_load_score = 30 (P-cores available, score = p_load)
    // Worker B: p=80, e=10, aggregate=40 -> effective_load_score = 80 (P-cores available, score = p_load)
    // Despite Worker B having lower aggregate load (40 < 50), Worker A should be
    // preferred because its P-core load is lower (30 < 80).
    let worker_id_a = WorkerId("worker_pcore_a".to_string());
    let worker_id_b = WorkerId("worker_pcore_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_a = setup_new_worker(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_b = setup_new_worker(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // Set per-core-type loads: (cpu_load_pct, p_core_load_pct, e_core_load_pct)
    // Worker A: aggregate=50, p=30, e=80 -> effective_load_score = 30
    scheduler
        .update_worker_load(&worker_id_a, 50, 30, 80)
        .await?;
    // Worker B: aggregate=40, p=80, e=10 -> effective_load_score = 80
    scheduler
        .update_worker_load(&worker_id_b, 40, 80, 10)
        .await?;

    // Submit an action.
    let action_digest = DigestInfo::new([40u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // Determine which worker received the action.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_a.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_a, got: {v:?}"),
            };
            (worker_id_a.clone(), se)
        }
        msg = rx_b.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_b, got: {v:?}"),
            };
            (worker_id_b.clone(), se)
        }
    };

    assert_eq!(
        selected_worker_id, worker_id_a,
        "Worker A (p_core_load=30, effective=30) should be preferred over Worker B (p_core_load=80, effective=80) despite B having lower aggregate load"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

// ---------------------------------------------------------------
// Cache affinity load cutoff tests
// ---------------------------------------------------------------

#[nativelink_test]
async fn cache_affinity_load_cutoff_test() -> Result<(), Error> {
    // Worker A: has the action's input_root_digest cached but is overloaded
    //           (P-cores saturated, effective_load_score > 99).
    // Worker B: no cache hit, low load (effective_load_score = 20).
    //
    // Worker A's effective_load_score(100, 20, 95) = 100 + 20 = 120 which
    // exceeds the CACHE_AFFINITY_LOAD_CUTOFF of 99. Since A is the only
    // cache match, the soft fallback picks A (an overloaded cache-hot worker
    // is still preferred over a completely cache-cold worker). This validates
    // that the soft-fallback path is exercised when all cache matches are
    // above the cutoff.
    let worker_id_a = WorkerId("worker_cache_a".to_string());
    let worker_id_b = WorkerId("worker_cache_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_a = setup_new_worker(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
    )
    .await?;
    let mut rx_b = setup_new_worker(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
    )
    .await?;

    // The action's input_root_digest.
    let input_root = DigestInfo::new([50u8; 32], 1024);

    // Worker A: cache hit for input_root, but P-cores saturated.
    // effective_load_score(100, 20, 95) = 100 + 20 = 120 (> 99 cutoff)
    scheduler
        .update_worker_load(&worker_id_a, 95, 100, 20)
        .await?;
    scheduler
        .update_cached_subtrees(&worker_id_a, true, vec![input_root], vec![], vec![])
        .await?;

    // Worker B: no cache hit, low load.
    // effective_load_score(0, 0, 20) = 20 (aggregate only, P-core tier)
    scheduler
        .update_worker_load(&worker_id_b, 20, 0, 0)
        .await?;

    // Submit an action whose input_root_digest matches Worker A's cache.
    let action_digest = DigestInfo::new([51u8; 32], 512);
    let insert_timestamp = make_system_time(2);
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let client_id = OperationId::default();
    let mut action_listener = scheduler.add_action(client_id, action_info).await?;
    tokio::task::yield_now().await;

    // Determine which worker received the action.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_a.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_a, got: {v:?}"),
            };
            (worker_id_a.clone(), se)
        }
        msg = rx_b.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_b, got: {v:?}"),
            };
            (worker_id_b.clone(), se)
        }
    };

    // Worker A has a cache hit but is overloaded (score 120 > cutoff 99).
    // The soft fallback picks A anyway because a cache-hot overloaded worker
    // is still preferred over a completely cache-cold worker in the current
    // implementation. This validates the soft-fallback path: overloaded
    // cache matches are used when no under-cutoff cache match exists.
    assert_eq!(
        selected_worker_id, worker_id_a,
        "Worker A (overloaded but cache-hot) should still be selected via soft fallback over cache-cold Worker B"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

#[nativelink_test]
async fn cache_affinity_least_loaded_holder_wins_tier1_test() -> Result<(), Error> {
    // (#sched-blend) Tier-1 exact-root cache affinity, continuous-blend
    // semantics. BOTH workers have the action's `input_root_digest` cached
    // (both are viable Tier-1 holders), and BOTH are genuinely busy (in
    // weighted-free DEFICIT, `weighted_free < REF_FREE`). Tier 1 picks the
    // holder with the SMALLEST `load_penalty` == the MOST free capacity.
    //
    // This REPLACES the old binary `CACHE_AFFINITY_LOAD_CUTOFF` /
    // `best_overloaded` soft-fallback framing (removed in #sched-blend): there
    // is no cutoff and no "overloaded" bucket now — Tier 1 is a pure
    // continuous min-`load_penalty` among viable holders.
    //
    // DETERMINISM ROOT-CAUSE (prior flake): the old test built the scheduler
    // with `SimpleSpec::default()`, whose `#[derive(Default)]` yields
    // `load_byte_cost == 0` (the serde `default = "default_load_byte_cost"`
    // fires only on DEserialization, not on `Default::default()`). With
    // `load_byte_cost == 0`, `capacity_score.load_penalty == 0` for EVERY
    // worker, so both Tier-1 holders tied at penalty 0 and the winner was
    // decided by `candidates` (a `HashSet`) iteration order — nondeterministic
    // per process, hence ~2/3 failure. Fix: a NON-ZERO `load_byte_cost` plus
    // REAL (P,E) core counts so the least-loaded holder pays a strictly-lower
    // penalty and wins deterministically.
    //
    // Arithmetic (p_count=4, e_count=6, load_byte_cost=512 KiB, REF_FREE=200):
    //   Worker A load(98,98,98): p_free=4*2=8, e_free=6*2=12,
    //     weighted_free = 2*8 + 12 = 28 (>0, not saturated),
    //     busy = (200-28) = 172, load_penalty = 512Ki*172/200 (larger).
    //   Worker B load(90,90,90): p_free=4*10=40, e_free=6*10=60,
    //     weighted_free = 2*40 + 60 = 140 (>0, not saturated),
    //     busy = (200-140) = 60, load_penalty = 512Ki*60/200 (smaller).
    // B has strictly more free capacity → strictly lower penalty → Tier 1
    // selects B deterministically. Neither is saturated, so the cascade does
    // NOT fall through to LRU/MRU; the cache tier owns the decision.
    let worker_id_a = WorkerId("worker_fallback_a".to_string());
    let worker_id_b = WorkerId("worker_fallback_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            // Non-zero so the continuous load penalty DISCRIMINATES between the
            // two holders (default-derived spec has load_byte_cost == 0).
            load_byte_cost: 512 * 1024,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Prod-shaped workers: real (P,E) core counts so `capacity_score` uses a
    // real denominator, not the `assume_core_count` fallback.
    let mut rx_a = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        4, // p_core_count
        6, // e_core_count
    )
    .await?;
    let mut rx_b = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;

    // The action's input_root_digest — both workers hold it (Tier-1 viable).
    let input_root = DigestInfo::new([60u8; 32], 2048);

    // Worker A: cache hit, more loaded (weighted_free 28).
    scheduler
        .update_worker_load(&worker_id_a, 98, 98, 98)
        .await?;
    scheduler
        .update_cached_subtrees(&worker_id_a, true, vec![input_root], vec![], vec![])
        .await?;

    // Worker B: cache hit, less loaded (weighted_free 140 → lower penalty).
    scheduler
        .update_worker_load(&worker_id_b, 90, 90, 90)
        .await?;
    scheduler
        .update_cached_subtrees(&worker_id_b, true, vec![input_root], vec![], vec![])
        .await?;

    // Submit an action whose input_root_digest matches both workers' caches.
    let action_digest = DigestInfo::new([61u8; 32], 512);
    let insert_timestamp = make_system_time(3);
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let client_id = OperationId::default();
    let mut action_listener = scheduler.add_action(client_id, action_info).await?;

    // Deterministic match: drive one match cycle inline rather than racing the
    // spawned matcher via `yield_now`. The dispatch StartAction then already
    // sits in exactly one worker's channel.
    scheduler.do_try_match_for_test().await?;

    // Determine which worker received the action.
    let (selected_worker_id, _se) = tokio::select! {
        msg = rx_a.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_a, got: {v:?}"),
            };
            (worker_id_a.clone(), se)
        }
        msg = rx_b.recv() => {
            let se = match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(se)) => se,
                v => panic!("Expected StartAction on worker_b, got: {v:?}"),
            };
            (worker_id_b.clone(), se)
        }
    };

    // Tier 1 (continuous min-load_penalty among viable holders): Worker B has
    // strictly more free capacity (weighted_free 140 vs 28) → strictly lower
    // load_penalty → B wins. Deterministic now that the penalty is non-zero.
    assert_eq!(
        selected_worker_id, worker_id_b,
        "Tier-1 cache affinity must pick the least-loaded holder: Worker B \
         (weighted_free 140) has more free capacity than Worker A \
         (weighted_free 28), so its load_penalty is strictly lower"
    );

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

/// Regression test: ExecutionComplete arriving after ExecuteResult(Completed)
/// must not trigger "should not be running on worker" and must not evict the
/// worker. Previously, the Completed update called complete_action() which
/// removed the operation from running_action_infos, causing the subsequent
/// ExecutionComplete to fail the contains_key check and evict the worker,
/// killing all its other in-flight actions.
#[nativelink_test]
async fn execution_complete_after_completed_does_not_evict_worker() -> Result<(), Error> {
    let worker_id = WorkerId("worker_id".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let action_digest = DigestInfo::new([99u8; 32], 512);
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    let operation_id = {
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                assert_eq!(
                    action_listener.changed().await.unwrap().0.stage,
                    ActionStage::Executing
                );
                start_execute.operation_id
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
    };

    let action_result = ActionResult {
        exit_code: 0,
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            ..ExecutionMetadata::default()
        },
        ..ActionResult::default()
    };

    // Step 1: Worker sends ExecuteResult(Completed) — this removes the
    // operation from running_action_infos via complete_action().
    scheduler
        .update_action(
            &worker_id,
            &OperationId::from(operation_id.clone()),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                action_result.clone(),
            )),
        )
        .await?;

    // Step 2: Worker sends ExecutionComplete. Before the fix, this would
    // trigger "should not be running on worker" and evict the worker.
    let execution_complete_result = scheduler
        .update_action(
            &worker_id,
            &OperationId::from(operation_id),
            UpdateOperationType::ExecutionComplete,
        )
        .await;

    assert!(
        execution_complete_result.is_ok(),
        "ExecutionComplete after Completed should succeed, got: {:?}",
        execution_complete_result.unwrap_err()
    );

    // Verify the worker is still alive by sending a keepalive — this would
    // fail with "Worker does not exist" if the worker was evicted.
    let keepalive_result = scheduler
        .worker_keep_alive_received(&worker_id, NOW_TIME + 1)
        .await;
    assert!(
        keepalive_result.is_ok(),
        "Worker should still be in the pool after ExecutionComplete, got: {:?}",
        keepalive_result.unwrap_err()
    );

    Ok(())
}

/// Regression test for "Unknown platform property re-trying every poll" bug.
///
/// An action submitted with a platform property the scheduler does NOT know
/// about (i.e. not in `supported_platform_properties`) used to:
///   - cause `make_platform_properties` to return `Code::InvalidArgument`,
///   - bubble that error out of `do_try_match`,
///   - increment the `consecutive_match_errors` counter (intended for
///     scheduler-state corruption, NOT input validation),
///   - emit a misleading "scheduler data structure corruption — restart may
///     be required" ERROR after 10 consecutive failures,
///   - leave the action queued so it would re-trigger on every poll cycle.
///
/// In production this surfaced as 7,010 consecutive `do_try_match` failures
/// in 10 min, all `InvalidArgument: Unknown platform property '…'`, on the
/// same ~110 stalled actions.
///
/// The fix: the matcher catches the producer's `InvalidArgument` and
/// rejects the action back to the originating client as a terminal
/// `FailedPrecondition` (re-tagged so it routes through the state-manager's
/// existing `missing_inputs` terminal-completion gate; no re-queue, no
/// `consecutive_match_errors` bump, no corruption alert), and the matcher
/// keeps draining the queue. The re-tag is deliberate: keeping the original
/// `Code::InvalidArgument` would have required either a state-manager gate
/// keyed on InvalidArgument (which would silently mute corruption-class
/// InvalidArgument from sibling sources like `awaited_action_decode` serde
/// failures, `ClientIdToOperationId::decode`, `WorkerId not in workers map`
/// desync — all of which legitimately signal corruption and SHOULD
/// retry/alert), or naked retry until max-retries exhaust.
///
/// This test asserts the **client-observable** outcome: the action's stage
/// transitions to `Completed` with a `Code::FailedPrecondition` error whose
/// message names the offending property — within a `tokio::time::timeout`
/// deadlock detector. Form (real scheduler wired) AND substance (state
/// transition observed via the same `ActionStateResult` subscription that
/// production callers read).
#[nativelink_test]
async fn unknown_platform_property_rejects_action_without_corruption_counter()
-> Result<(), Error> {
    // Scheduler is configured with a single known property "prop"; the action
    // we submit will require "persistentWorkerProtocol" which is NOT declared,
    // mirroring the production wedge.
    let mut prop_defs = HashMap::new();
    prop_defs.insert("prop".to_string(), PropertyType::Exact);

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(prop_defs),
            max_job_retries: 5,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Register a worker that DOES declare the known property, so the matcher
    // has a candidate to consider. The mismatch fires on the action side
    // (unknown key in action_info.platform_properties), not on the worker
    // capabilities side.
    let worker_id = WorkerId("worker_id".to_string());
    let mut worker_props = PlatformProperties::default();
    worker_props
        .properties
        .insert("prop".to_string(), PlatformPropertyValue::Exact("v".to_string()));
    let _rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), worker_props).await?;

    // Submit an action that requires an unknown property.
    let action_digest = DigestInfo::new([42u8; 32], 256);
    let mut bad_props = HashMap::new();
    bad_props.insert(
        "persistentWorkerProtocol".to_string(),
        "json".to_string(),
    );
    let mut action_listener =
        setup_action(&scheduler, action_digest, bad_props, make_system_time(1)).await?;

    // Run the matcher (no-op if the background task already drained it on
    // task_change_notify; either way, the action is rejected exactly once).
    // Before the fix this returns Err and would re-fire on every subsequent
    // poll; after the fix it returns Ok because the unknown-property action
    // is rejected to the client and the matcher continues draining work.
    let match_result = scheduler.do_try_match_for_test().await;
    assert!(
        match_result.is_ok(),
        "do_try_match must NOT propagate InvalidArgument as a matcher \
         failure (would bump consecutive_match_errors and trigger \
         'scheduler corruption' alert); got: {match_result:?}"
    );

    // The client should observe the action transitioning to a terminal
    // Completed stage with an InvalidArgument error naming the offending
    // property. The matcher runs on a background task, so the action may
    // skip past Queued before we read; drain state changes until we either
    // see Completed or time out.
    //
    // Wrap in a short timeout — if the action is silently re-queued (the
    // bug), `changed()` may keep flapping between Queued ↔ Queued and we
    // never reach Completed; Elapsed surfaces with a specific message.
    let action_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (state, _origin) = action_listener.changed().await.unwrap();
            match &state.stage {
                ActionStage::Completed(result) => break result.clone(),
                ActionStage::Queued | ActionStage::CacheCheck => continue,
                other => panic!(
                    "unexpected intermediate stage for unknown-platform-property \
                     action: {other:?}"
                ),
            }
        }
    })
    .await
    .expect(
        "action must reach terminal Completed within 5s — \
         unknown platform property must reject action to client, \
         not silently re-queue (the regression we are guarding against)",
    );
    let err = action_result
        .error
        .as_ref()
        .expect("Completed stage must carry the rejection error");
    assert_eq!(
        err.code,
        Code::FailedPrecondition,
        "rejection error code must be FailedPrecondition (client \
         precondition not met by scheduler config); we deliberately do NOT \
         use Code::InvalidArgument here because the state-manager retry \
         gate distinguishes the two, and InvalidArgument is also produced \
         by genuine corruption-class errors (awaited_action serde decode) \
         that MUST keep retrying. Got: {err:?}"
    );
    assert!(
        err.to_string().contains("persistentWorkerProtocol"),
        "rejection error message must name the offending property so the \
         client can fix their request; got: {err}"
    );

    // The matcher MUST NOT have logged the scheduler-corruption alert that
    // would page a human and (falsely) suggest a restart. The single
    // rejection above already exercises the matcher's input-validation
    // path; the corruption alert only fires after >=10 consecutive
    // matcher-loop errors, which one rejection cannot trigger.
    assert!(
        !logs_contain("possible scheduler data structure corruption"),
        "unknown platform property must NOT trigger the scheduler-corruption \
         alert — that alert is reserved for actual data-structure damage \
         and falsely suggests a server restart"
    );

    Ok(())
}

/// Sibling regression test: an action with a KNOWN platform property name
/// but a malformed VALUE (e.g. `cpu_count = "not_a_number"`) is rejected
/// terminally to the client, not silently re-queued.
///
/// Mirrors `unknown_platform_property_rejects_action_without_corruption_counter`
/// but exercises the second `make_platform_properties` failure mode —
/// `PropertyType::Minimum` parsing — to ensure both code paths through
/// `make_platform_properties` route through the same FailedPrecondition
/// rejection. Without this test, a future change that broke the malformed-
/// value path (e.g. by returning a different code) could regress without
/// the test suite catching it.
#[nativelink_test]
async fn malformed_minimum_value_rejects_action() -> Result<(), Error> {
    // Scheduler declares "cpu_count" as a Minimum-typed property. A
    // value of "not_a_number" is a parse failure inside
    // `make_platform_properties` → `PropertyType::Minimum::try_from`.
    let mut prop_defs = HashMap::new();
    prop_defs.insert("cpu_count".to_string(), PropertyType::Minimum);

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(prop_defs),
            max_job_retries: 5,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Register a worker advertising a valid "cpu_count" so the matcher
    // is willing to consider the action; the failure fires on action-side
    // value parsing.
    let worker_id = WorkerId("worker_id".to_string());
    let mut worker_props = PlatformProperties::default();
    worker_props
        .properties
        .insert("cpu_count".to_string(), PlatformPropertyValue::Minimum(4.0));
    let _rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), worker_props).await?;

    // Action declares cpu_count with a non-numeric value.
    let action_digest = DigestInfo::new([43u8; 32], 256);
    let mut bad_props = HashMap::new();
    bad_props.insert("cpu_count".to_string(), "not_a_number".to_string());
    let mut action_listener =
        setup_action(&scheduler, action_digest, bad_props, make_system_time(2)).await?;

    let match_result = scheduler.do_try_match_for_test().await;
    assert!(
        match_result.is_ok(),
        "do_try_match must NOT propagate malformed-value error as a matcher \
         failure; got: {match_result:?}"
    );

    let action_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (state, _origin) = action_listener.changed().await.unwrap();
            match &state.stage {
                ActionStage::Completed(result) => break result.clone(),
                ActionStage::Queued | ActionStage::CacheCheck => continue,
                other => panic!(
                    "unexpected intermediate stage for malformed-value action: \
                     {other:?}"
                ),
            }
        }
    })
    .await
    .expect(
        "action must reach terminal Completed within 5s — malformed property \
         value must reject action to client, not silently re-queue",
    );
    let err = action_result
        .error
        .as_ref()
        .expect("Completed stage must carry the rejection error");
    assert_eq!(
        err.code,
        Code::FailedPrecondition,
        "malformed property value rejection code must be FailedPrecondition \
         (same as unknown property); got {err:?}"
    );

    Ok(())
}

// #36 Phase 6 §6 Phase 0 probe TB2: P-SCHED-DISPATCH fires when the
// scheduler hands an action to a worker via prepare_worker_run_action.
//
// The probe is wired at api_worker_scheduler.rs near the `Some((tx, msg))`
// return so it captures every successful dispatch. This test exercises
// the public scheduler path: add worker → add action → do_try_match →
// assert the probe's structured log fired.
//
// Mutation: remove the `info!(tag = "phase6_scheduler_dispatch", ...)` from
// api_worker_scheduler.rs. This test must red-fail with the bespoke
// "phase6 scheduler dispatch probe absent 2026-06-07" panic message.
#[nativelink_test]
async fn phase6_probe_p_sched_dispatch_fires_on_action_assignment() -> Result<(), Error> {
    let worker_id = WorkerId("phase6_dispatch_worker".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([7u8; 32], 256);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let _action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
            .await
            .unwrap();

    // Drain the StartAction message to ensure dispatch actually happened.
    // (The probe fires inside prepare_worker_run_action on the same path that
    // produces this message — receiving it guarantees the probe code ran.)
    let _msg_for_worker = rx_from_worker
        .recv()
        .await
        .expect("worker must receive StartAction — Phase 6 dispatch probe is on this path");

    assert!(
        logs_contain("phase6_scheduler_dispatch"),
        "phase6 scheduler dispatch probe absent 2026-06-07"
    );

    Ok(())
}

// #queue-attrib: the scheduler emits an INFO line at the moment it assigns
// an action to a worker, carrying `match_latency_ms` = (dispatch wall-clock −
// queued/insert timestamp). This is the accept→worker-assigned sub-interval of
// the worker-side `queue_ms` (running_actions_manager.rs Action-phase-timing),
// letting an operator split queue delay into scheduler-match vs delivery+accept.
//
// CRITICAL: this assertion checks the emit LEVEL, not just the message text.
// `logs_contain` cannot distinguish `info!` from `debug!` (both fire under the
// debug-profile test subscriber), which is exactly how the sibling
// phase6_scheduler_dispatch probe silently became invisible in prod when a
// log-demotion sweep flipped it info!→debug! on 2026-06-13 — a `release_max_level_info`
// release build compiles `debug!` out entirely. `logs_assert` hands us the
// level-prefixed formatted lines (tracing-test's FmtSubscriber sets
// `.with_level(true)`), so we assert the line is INFO-level — the only level
// that survives `release_max_level_info`.
//
// Mutation A: change `info!` → `debug!` at the new dispatch-attribution site in
// api_worker_scheduler.rs. This test must red-fail with the bespoke
// "match_latency_ms dispatch-attribution line not emitted at INFO" message,
// because a DEBUG line is no longer at INFO level.
// Mutation B: remove the `match_latency_ms` field. Same red-fail (field absent).
#[nativelink_test]
async fn dispatch_attribution_match_latency_emitted_at_info() -> Result<(), Error> {
    let worker_id = WorkerId("dispatch_attrib_worker".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(
            0,
            &task_change_notify.clone(),
            MockInstantWrapped::default,
        ),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([9u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let _action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp)
            .await
            .unwrap();

    // Draining StartAction guarantees the dispatch path (prepare_worker_run_action)
    // ran — the attribution line is emitted on that same path.
    let _msg_for_worker = rx_from_worker
        .recv()
        .await
        .expect("worker must receive StartAction — dispatch-attribution line is on this path");

    // Assert the attribution line fired AT INFO level. logs_assert gives us the
    // level-prefixed formatted lines (`.with_level(true)`); a single line must
    // carry both the INFO token and the structured `match_latency_ms=` field.
    // Match the structured-field form (`match_latency_ms=`, no surrounding
    // spaces) rather than the bare token so the assertion cannot be satisfied by
    // a message string that merely mentions the field name — only the emitted
    // structured field produces `match_latency_ms=<n>`.
    logs_assert(|lines: &[&str]| {
        if lines
            .iter()
            .any(|line| line.contains("INFO") && line.contains("match_latency_ms="))
        {
            Ok(())
        } else {
            Err(
                "match_latency_ms dispatch-attribution line not emitted at INFO"
                    .to_string(),
            )
        }
    });

    Ok(())
}

/// (FL-688 v3 Stage C follow-up — BLOCK-3 NAK-seam e2e test)
///
/// Verifies the full NAK→scheduler seam: when a worker returns
/// `Code::ResourceExhausted` via `UpdateWithError`, the scheduler's
/// `due_to_backpressure` path at `simple_scheduler_state_manager.rs:817`
/// must NOT increment `attempts`, so the action stays `Queued` and
/// re-matchable indefinitely — regardless of `max_job_retries`.
///
/// Seam covered: `UpdateWithError(ResourceExhausted)` →
/// `SimpleSchedulerStateManager::update_operation` →
/// `due_to_backpressure = true` → `attempts` unchanged → `ActionStage::Queued`.
///
/// Mutation: change `Code::ResourceExhausted` to `Code::Unavailable` at BOTH
/// NAK send-sites (the two `Code::ResourceExhausted,` args in the test body) —
/// mutating only ONE is a false-green: with `max_job_retries = 1`, one
/// `Unavailable` NAK takes `attempts` to 1 (not `> 1`) so the action still
/// re-queues; both must flip to drive `attempts` to 2 (`> max_job_retries`)
/// and trip the hard-fail. Then `due_to_backpressure` becomes `false` →
/// `attempts` increments → action hard-fails after `max_job_retries`. Panics:
/// "NAK-seam BLOCK-3: action reached Completed after ResourceExhausted NAK —
///  attempts must NOT be incremented for Code::ResourceExhausted backpressure NAKs
///  (simple_scheduler_state_manager.rs:817: due_to_backpressure check)"
///
/// Production seam: `local_worker.rs:4834` sends `Code::ResourceExhausted`
/// for the startup-reconcile gate NAK; the scheduler at `:817` maps it to
/// `due_to_backpressure = true` → re-queues without consuming the retry budget.
/// Using `Code::Unavailable` instead burns the budget and hard-fails the action
/// after `max_job_retries` NAKs — the regression BLOCK-3 fixed.
#[nativelink_test]
async fn v3c_block3_nak_resource_exhausted_does_not_increment_attempts_test()
-> Result<(), Error> {
    const NAK_TIMEOUT: Duration = Duration::from_secs(5);
    let worker_id = WorkerId("nak_seam_worker".to_string());

    // max_job_retries: 1 — a Code::Unavailable NAK would exhaust retries after
    // 2 UpdateWithErrors; Code::ResourceExhausted must never exhaust them.
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            max_job_retries: 1,
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
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );
    let action_digest = DigestInfo::new([0xABu8; 32], 256);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // ----- StartAction: wait for worker to receive it -----
    let operation_id = {
        let op_id = match tokio::time::timeout(NAK_TIMEOUT, rx_from_worker.recv())
            .await
            .expect(
                "NAK-seam BLOCK-3: timed out waiting for StartAction — \
                 action was not dispatched to worker within 5s",
            )
            .unwrap()
            .update
        {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("NAK-seam BLOCK-3: expected StartAction, got: {v:?}"),
        };
        // Consume the Executing transition.
        assert_eq!(
            tokio::time::timeout(NAK_TIMEOUT, action_listener.changed())
                .await
                .expect(
                    "NAK-seam BLOCK-3: timed out waiting for Executing transition — \
                     listener did not fire within 5s",
                )
                .unwrap()
                .0
                .stage,
            ActionStage::Executing,
            "NAK-seam BLOCK-3: expected Executing after StartAction"
        );
        OperationId::from(op_id.as_str())
    };

    // ----- NAK #1 with Code::ResourceExhausted — must NOT burn attempts -----
    scheduler
        .update_action(
            &worker_id,
            &operation_id,
            UpdateOperationType::UpdateWithError(make_err!(
                Code::ResourceExhausted,
                "Worker startup reconcile in progress"
            )),
        )
        .await
        .err_tip(|| "update_action NAK#1 failed")?;

    {
        // Action must return to Queued — ResourceExhausted does not count as attempt.
        assert_eq!(
            tokio::time::timeout(NAK_TIMEOUT, action_listener.changed())
                .await
                .expect(
                    "NAK-seam BLOCK-3: timed out waiting for Queued after NAK#1 — \
                     scheduler did not re-queue the action within 5s",
                )
                .unwrap()
                .0
                .stage,
            ActionStage::Queued,
            "NAK-seam BLOCK-3: action reached Completed after ResourceExhausted NAK — \
             attempts must NOT be incremented for Code::ResourceExhausted backpressure NAKs \
             (simple_scheduler_state_manager.rs:817: due_to_backpressure check)"
        );
    }

    // ----- Re-dispatch to the same worker; NAK #2 — still must not burn attempts -----
    // (max_job_retries=1; if attempts were incremented, the second NAK would hard-fail)
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;

    let operation_id_2 = {
        let op_id = match tokio::time::timeout(NAK_TIMEOUT, rx_from_worker.recv())
            .await
            .expect(
                "NAK-seam BLOCK-3: timed out waiting for StartAction re-dispatch \
                 after NAK#1 — action not re-dispatched within 5s",
            )
            .unwrap()
            .update
        {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("NAK-seam BLOCK-3: expected StartAction on re-dispatch, got: {v:?}"),
        };
        assert_eq!(
            tokio::time::timeout(NAK_TIMEOUT, action_listener.changed())
                .await
                .expect(
                    "NAK-seam BLOCK-3: timed out waiting for Executing transition (re-dispatch)",
                )
                .unwrap()
                .0
                .stage,
            ActionStage::Executing,
            "NAK-seam BLOCK-3: expected Executing on re-dispatch"
        );
        OperationId::from(op_id.as_str())
    };

    scheduler
        .update_action(
            &worker_id,
            &operation_id_2,
            UpdateOperationType::UpdateWithError(make_err!(
                Code::ResourceExhausted,
                "Worker startup reconcile in progress"
            )),
        )
        .await
        .err_tip(|| "update_action NAK#2 failed")?;

    {
        // Still Queued — two ResourceExhausted NAKs with max_job_retries=1 must
        // NOT hard-fail. If attempts were incremented, this would be Completed.
        assert_eq!(
            tokio::time::timeout(NAK_TIMEOUT, action_listener.changed())
                .await
                .expect(
                    "NAK-seam BLOCK-3: timed out waiting for Queued after NAK#2 — \
                     scheduler did not re-queue the action within 5s (second NAK)",
                )
                .unwrap()
                .0
                .stage,
            ActionStage::Queued,
            "NAK-seam BLOCK-3: action reached Completed after second ResourceExhausted NAK — \
             attempts must NOT be incremented for Code::ResourceExhausted backpressure NAKs \
             (simple_scheduler_state_manager.rs:817: due_to_backpressure check). \
             Mutation target: change Code::ResourceExhausted to Code::Unavailable at \
             local_worker.rs:4834 to reproduce — Unavailable DOES increment attempts."
        );
    }

    Ok(())
}

// ===============================================================
// P0 fleet edge-case coverage (worker-selection permutations).
// Each test uses prod-shaped workers (reported load + real (P,E) core
// counts) UNLESS it is specifically about the never-reported path.
// Synchronization is via `do_try_match_for_test()` (an inline, awaited
// match cycle) or via a blocking `recv()` of the dispatch — never
// sleep-as-synchronization.
// ===============================================================

/// #1 (#sched-zeroload) A fleet where EVERY worker has NEVER reported load
/// is intentionally treated as 100%-busy (`weighted_free == 0` → saturated),
/// so the cache-affinity tiers DECLINE and the cascade falls through to
/// LRU/MRU. The action must still be DISPATCHED (no wedge). This pins the
/// behavior that (incorrectly) surprised the old locality test: a
/// never-reported worker holding a cached blob does NOT get a locality/cache
/// placement — it competes only in the LRU fallback like any other
/// never-reported worker. Over-selecting an unknown-load worker on a cache
/// hit is exactly what #sched-zeroload prevents.
#[nativelink_test]
async fn never_reported_fleet_falls_through_to_lru_no_wedge_test() -> Result<(), Error> {
    let worker_id_a = WorkerId("nr_worker_a".to_string());
    let worker_id_b = WorkerId("nr_worker_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            // Non-zero so, IF the workers were viable, a cache hit COULD win —
            // making the point that they still don't (they are saturated).
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Real core counts, but NEITHER worker calls `update_worker_load` → both
    // stay `has_reported_load == false` → treated as 100% busy.
    let mut rx_a = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;

    // Worker B holds a cached subtree matching the action's input_root. If the
    // workers were viable this would give B a Tier-1 cache win — but because B
    // is never-reported (saturated), the cache tier declines and B gets NO
    // preferential placement.
    let input_root = DigestInfo::new([70u8; 32], 1024);
    scheduler
        .update_cached_subtrees(&worker_id_b, true, vec![input_root], vec![], vec![])
        .await?;

    let action_digest = DigestInfo::new([71u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let client_id = OperationId::default();
    let mut action_listener = scheduler.add_action(client_id, action_info).await?;

    scheduler.do_try_match_for_test().await?;

    // Load-bearing assertion: the action is DISPATCHED (no wedge) despite every
    // candidate being never-reported/saturated — the cascade degrades to
    // LRU/MRU rather than declining to place anything.
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "never-reported fleet wedged the action in Queued — the saturation \
         fall-through must degrade to LRU/MRU placement, not decline to place"
    );

    // Confirm exactly one worker received a StartAction (a real dispatch, not
    // just a stage flip). Do NOT assert WHICH worker: the point is that the
    // cache hit on B did NOT steer the placement — LRU order owns it.
    let mut saw_start = false;
    for _ in 0..4 {
        tokio::select! {
            biased;
            msg = rx_a.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) = msg.expect("a closed").update {
                    saw_start = true;
                    break;
                }
            }
            msg = rx_b.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) = msg.expect("b closed").update {
                    saw_start = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_start,
        "no worker received a StartAction on the never-reported fleet (LRU \
         fall-through selected nothing)"
    );

    Ok(())
}

/// #2 An empty fleet (0 workers) does not panic or wedge on `add_action`:
/// the action stays Queued. When a viable worker later appears, the matcher
/// dispatches it (Queued → Executing).
#[nativelink_test]
async fn empty_fleet_queues_then_dispatches_when_worker_appears_test() -> Result<(), Error> {
    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec::default(),
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // No workers yet. Submit an action.
    let action_digest = DigestInfo::new([72u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    // A match cycle over an empty fleet must be a benign no-op (no panic).
    scheduler.do_try_match_for_test().await?;

    // The action stays Queued: nothing to dispatch to.
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Queued,
        "action on an empty fleet must remain Queued (no viable worker), not \
         wedge or error"
    );

    // Now a worker appears. The Queued action must transition to Executing.
    let worker_id = WorkerId("late_worker".to_string());
    let mut rx = setup_new_worker(&scheduler, worker_id.clone(), PlatformProperties::default()).await?;
    scheduler.do_try_match_for_test().await?;

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "a Queued action must be dispatched once a viable worker appears \
         (Queued → Executing)"
    );

    // And the worker actually received the StartAction.
    let msg = rx.recv().await.expect("worker channel closed");
    assert!(
        matches!(msg.update, Some(update_for_worker::Update::StartAction(_))),
        "the newly-added worker did not receive the queued action's StartAction"
    );

    Ok(())
}

/// #3 A fleet where EVERY worker genuinely reports ~100% load (real core
/// counts, `weighted_free == 0` → saturated) must still make progress: the
/// cache tiers decline via the saturation fall-through and the LRU/MRU path
/// places the action. No permanent wedge under a fully-saturated fleet.
#[nativelink_test]
async fn all_saturated_fleet_still_dispatches_no_wedge_test() -> Result<(), Error> {
    let worker_id_a = WorkerId("sat_worker_a".to_string());
    let worker_id_b = WorkerId("sat_worker_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_a = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_a.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_core_counts(
        &scheduler,
        worker_id_b.clone(),
        PlatformProperties::default(),
        4,
        6,
    )
    .await?;

    // Both report 100% on every axis: p_free = 4*(100-100) = 0,
    // e_free = 6*(100-100) = 0, weighted_free = 0 → saturated (but
    // has_reported_load == true, so this is a GENUINE saturation, distinct
    // from the never-reported case in #1).
    scheduler.update_worker_load(&worker_id_a, 100, 100, 100).await?;
    scheduler.update_worker_load(&worker_id_b, 100, 100, 100).await?;

    let action_digest = DigestInfo::new([73u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;

    scheduler.do_try_match_for_test().await?;

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "a fully-saturated fleet wedged the action — the saturation \
         fall-through must place on the LRU/MRU worker (spread the \
         unavoidable work), not stall forever"
    );

    let mut saw_start = false;
    for _ in 0..4 {
        tokio::select! {
            biased;
            msg = rx_a.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) = msg.expect("a closed").update {
                    saw_start = true;
                    break;
                }
            }
            msg = rx_b.recv() => {
                if let Some(update_for_worker::Update::StartAction(_)) = msg.expect("b closed").update {
                    saw_start = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_start,
        "no worker received a StartAction on the fully-saturated fleet"
    );

    Ok(())
}

/// #4 A platform-partitioned fleet: only ONE of three workers carries the
/// required Exact platform property. The action requires it, so only that
/// worker is a candidate — the other two are filtered out of the capability
/// index and are NEVER chosen.
#[nativelink_test]
async fn platform_partitioned_fleet_selects_only_matching_worker_test() -> Result<(), Error> {
    let worker_match = WorkerId("gpu_worker".to_string());
    let worker_no1 = WorkerId("cpu_worker_1".to_string());
    let worker_no2 = WorkerId("cpu_worker_2".to_string());

    let mut prop_defs = HashMap::new();
    prop_defs.insert("gpu".to_string(), PropertyType::Exact);

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            supported_platform_properties: Some(prop_defs),
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Only `worker_match` advertises gpu=true.
    let mut gpu_props = PlatformProperties::default();
    gpu_props.properties.insert(
        "gpu".to_string(),
        PlatformPropertyValue::Exact("true".to_string()),
    );
    let mut rx_match = setup_new_worker(&scheduler, worker_match.clone(), gpu_props).await?;
    let mut rx_no1 =
        setup_new_worker(&scheduler, worker_no1.clone(), PlatformProperties::default()).await?;
    let mut rx_no2 =
        setup_new_worker(&scheduler, worker_no2.clone(), PlatformProperties::default()).await?;

    // The action requires gpu=true.
    let action_digest = DigestInfo::new([74u8; 32], 512);
    let mut required = HashMap::new();
    required.insert("gpu".to_string(), "true".to_string());
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, required, insert_timestamp).await?;

    scheduler.do_try_match_for_test().await?;

    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "the gpu-requiring action was not dispatched to the sole gpu worker"
    );

    // The matching worker got the StartAction.
    let msg = rx_match.recv().await.expect("gpu worker channel closed");
    assert!(
        matches!(msg.update, Some(update_for_worker::Update::StartAction(_))),
        "the sole capability-matching worker did not receive the action"
    );

    // The two non-matching workers got NOTHING (they were never candidates).
    assert_eq!(
        rx_no1.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "a non-gpu worker received work for a gpu-requiring action"
    );
    assert_eq!(
        rx_no2.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "a non-gpu worker received work for a gpu-requiring action"
    );

    Ok(())
}

/// #5 Tie determinism: two workers identical on every axis (same core
/// counts, same reported load, no cache) must yield a DETERMINISTIC, stable
/// pick. The default `LeastRecentlyUsed` allocation strategy iterates workers
/// LRU-first and `min_by_key` keeps the first element on an equal-score tie,
/// so the first-added worker (the LRU one on a fresh fleet) wins every time.
/// We rebuild a fresh scheduler each iteration to prove the pick is stable
/// across process/HashSet-seed variation (the same nondeterminism that
/// flaked the cache-affinity test would surface here if the tiebreak were
/// unstable).
#[nativelink_test]
async fn identical_workers_tie_break_is_deterministic_lru_test() -> Result<(), Error> {
    for iteration in 0..8 {
        let worker_id_a = WorkerId("tie_worker_a".to_string());
        let worker_id_b = WorkerId("tie_worker_b".to_string());

        let task_change_notify = Arc::new(Notify::new());
        let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
            &SimpleSpec {
                load_byte_cost: 512 * 1024,
                ..Default::default()
            },
            memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None,
            None, // cas_store
            None, // locality_map
            None, // worker_tls_config
        );

        // worker_a added FIRST → it is the least-recently-used on a fresh
        // fleet. Both are otherwise identical (same counts, same load, no
        // cache).
        let mut rx_a =
            setup_new_worker_with_core_counts(&scheduler, worker_id_a.clone(), PlatformProperties::default(), 4, 6)
                .await?;
        let mut rx_b =
            setup_new_worker_with_core_counts(&scheduler, worker_id_b.clone(), PlatformProperties::default(), 4, 6)
                .await?;
        scheduler.update_worker_load(&worker_id_a, 40, 40, 40).await?;
        scheduler.update_worker_load(&worker_id_b, 40, 40, 40).await?;

        let action_digest = DigestInfo::new([75u8; 32], 512);
        let insert_timestamp = make_system_time(1);
        let mut action_listener =
            setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;
        scheduler.do_try_match_for_test().await?;

        let selected = tokio::select! {
            msg = rx_a.recv() => {
                assert!(matches!(msg.unwrap().update, Some(update_for_worker::Update::StartAction(_))));
                worker_id_a.clone()
            }
            msg = rx_b.recv() => {
                assert!(matches!(msg.unwrap().update, Some(update_for_worker::Update::StartAction(_))));
                worker_id_b.clone()
            }
        };

        assert_eq!(
            action_listener.changed().await.unwrap().0.stage,
            ActionStage::Executing
        );
        // Stable tiebreak: the LRU (first-added) worker wins EVERY iteration.
        assert_eq!(
            selected, worker_id_a,
            "identical-worker tiebreak was not stable on iteration {iteration}: \
             expected the LRU (first-added) worker to win deterministically, \
             but the pick varied — the tiebreak must not depend on HashSet \
             iteration order"
        );
    }

    Ok(())
}

/// #6 Eviction during dispatch: a worker is selected and running an action,
/// then it is removed before the action completes. The scheduler must
/// re-queue the action and re-dispatch it to another worker — no panic, no
/// double-dispatch (the evicted worker gets exactly one StartAction then a
/// Disconnect; the action ends up Executing on the surviving worker).
#[nativelink_test]
async fn worker_eviction_during_dispatch_requeues_to_other_worker_test() -> Result<(), Error> {
    let worker_id_a = WorkerId("evict_worker_a".to_string());
    let worker_id_b = WorkerId("evict_worker_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Only worker_a is present when the action is dispatched, so it is the
    // unambiguous first recipient. worker_b is added afterward to receive the
    // requeued action.
    let mut rx_a =
        setup_new_worker_with_core_counts(&scheduler, worker_id_a.clone(), PlatformProperties::default(), 4, 6)
            .await?;
    scheduler.update_worker_load(&worker_id_a, 30, 30, 30).await?;

    let action_digest = DigestInfo::new([76u8; 32], 512);
    let insert_timestamp = make_system_time(1);
    let mut action_listener =
        setup_action(&scheduler, action_digest, HashMap::new(), insert_timestamp).await?;
    scheduler.do_try_match_for_test().await?;

    // worker_a receives exactly one StartAction and the action is Executing.
    let first = rx_a.recv().await.expect("worker_a channel closed");
    assert!(
        matches!(first.update, Some(update_for_worker::Update::StartAction(_))),
        "worker_a did not receive the initial StartAction"
    );
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    // Add a second worker that can take the job once worker_a is evicted.
    let mut rx_b =
        setup_new_worker_with_core_counts(&scheduler, worker_id_b.clone(), PlatformProperties::default(), 4, 6)
            .await?;
    scheduler.update_worker_load(&worker_id_b, 30, 30, 30).await?;

    // Evict worker_a mid-dispatch. Its in-flight action must be re-queued.
    drop(scheduler.remove_worker(&worker_id_a).await);
    scheduler.do_try_match_for_test().await?;

    // worker_a should have received a Disconnect (and NOT a second
    // StartAction — no double-dispatch to the evicted worker).
    let disc = rx_a.recv().await.expect("worker_a channel closed before disconnect");
    assert_eq!(
        disc.update,
        Some(update_for_worker::Update::Disconnect(())),
        "evicted worker_a did not receive a Disconnect (or got an unexpected \
         second dispatch — possible double-dispatch)"
    );
    assert_eq!(
        rx_a.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected),
        "evicted worker_a received a message after Disconnect (double-dispatch \
         to an evicted worker)"
    );

    // The action is re-dispatched to worker_b and ends up Executing there.
    let requeued = rx_b.recv().await.expect("worker_b channel closed");
    let se = match requeued.update {
        Some(update_for_worker::Update::StartAction(se)) => se,
        v => panic!("worker_b expected the requeued StartAction, got: {v:?}"),
    };
    assert_eq!(
        se.worker_id,
        worker_id_b.to_string(),
        "the requeued action's StartAction was not addressed to worker_b"
    );
    assert_eq!(
        action_listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "after eviction the action did not resume Executing on the surviving \
         worker"
    );

    Ok(())
}

/// #7 Quarantine skip + clear: a quarantined worker is excluded from
/// selection; after it re-checks in (keepalive), it becomes selectable again.
/// Quarantine is reached via the EXISTING seam (`remove_timedout_workers` at
/// 1x the timeout quarantines a worker that has not checked in, while a
/// kept-alive peer survives); clearing is via `worker_keep_alive_received`
/// (refresh_lifetime takes the quarantine flag). No production change needed.
#[nativelink_test]
async fn quarantined_worker_skipped_then_selectable_after_keepalive_test() -> Result<(), Error> {
    let worker_id_a = WorkerId("quar_worker_a".to_string());
    let worker_id_b = WorkerId("quar_worker_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            worker_timeout_s: WORKER_TIMEOUT_S,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    let mut rx_a =
        setup_new_worker_with_core_counts(&scheduler, worker_id_a.clone(), PlatformProperties::default(), 4, 6)
            .await?;
    let mut rx_b =
        setup_new_worker_with_core_counts(&scheduler, worker_id_b.clone(), PlatformProperties::default(), 4, 6)
            .await?;
    scheduler.update_worker_load(&worker_id_a, 30, 30, 30).await?;
    scheduler.update_worker_load(&worker_id_b, 30, 30, 30).await?;

    // Keep worker_b alive at 2x timeout so it survives the timeout sweep;
    // worker_a is NOT kept alive → it will be quarantined at 1x timeout.
    scheduler
        .worker_keep_alive_received(&worker_id_b, NOW_TIME + 2 * WORKER_TIMEOUT_S)
        .await?;
    scheduler
        .remove_timedout_workers(NOW_TIME + WORKER_TIMEOUT_S)
        .await?;

    // First action: worker_a is quarantined → it must go to worker_b.
    let action_digest_1 = DigestInfo::new([77u8; 32], 512);
    let insert_timestamp_1 = make_system_time(1);
    let mut listener_1 =
        setup_action(&scheduler, action_digest_1, HashMap::new(), insert_timestamp_1).await?;
    scheduler.do_try_match_for_test().await?;

    let msg_b = rx_b.recv().await.expect("worker_b channel closed");
    assert!(
        matches!(msg_b.update, Some(update_for_worker::Update::StartAction(_))),
        "the action was not routed to the non-quarantined worker_b"
    );
    assert_eq!(
        listener_1.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );
    // worker_a (quarantined) must NOT have received work.
    assert_eq!(
        rx_a.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "a quarantined worker was selected for new work"
    );

    // Clear worker_a's quarantine via keepalive, then remove worker_b so
    // worker_a is the ONLY viable worker for the next action.
    scheduler
        .worker_keep_alive_received(&worker_id_a, NOW_TIME + WORKER_TIMEOUT_S)
        .await?;
    drop(scheduler.remove_worker(&worker_id_b).await);

    // Second action: worker_a is un-quarantined and is now selectable.
    let action_digest_2 = DigestInfo::new([78u8; 32], 512);
    let insert_timestamp_2 = make_system_time(2);
    let mut listener_2 =
        setup_action(&scheduler, action_digest_2, HashMap::new(), insert_timestamp_2).await?;
    scheduler.do_try_match_for_test().await?;

    let msg_a = rx_a.recv().await.expect("worker_a channel closed");
    assert!(
        matches!(msg_a.update, Some(update_for_worker::Update::StartAction(_))),
        "worker_a was not selectable after its quarantine was cleared by \
         keepalive"
    );
    assert_eq!(
        listener_2.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "the second action did not dispatch to the un-quarantined worker_a"
    );

    Ok(())
}

// ────────────────────────── (#sched M1 rebalance) ──────────────────────────
// Dispatch-count P-headroom overflow gate. `SimpleSpec::p_headroom_gate_enabled`
// (default OFF) gates the cache-affinity tiers: while ANY viable worker has
// P-headroom (`running_action_infos.len() < p_core_count`), a worker WITHOUT
// P-headroom is excluded from those tiers so its cached-input surplus overflows
// to a P-headroom peer instead of piling onto full P cores. Design v2.1
// (`.claude/audits/scheduler-pcore-first-rebalance-design-v2-2026-06-30.md`),
// mechanisms M1 (gate in `worker_is_viable`) + M2 (fold the pre-scan into the
// existing `all_viable_saturated` loop) + A5 (`p_core_count == 0` ungated).
//
// Helper: drive one action HELD in-flight (never completed) onto a specific
// worker so its `running_action_infos` count climbs toward `p_core_count`. The
// action is dispatched via `do_try_match_for_test` and left executing; the
// worker rx is drained so a later assertion sees the NEXT dispatch cleanly.
async fn dispatch_and_hold_on_worker(
    scheduler: &SimpleScheduler,
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
    input_root: DigestInfo,
    action_hash: [u8; 32],
    ts: u64,
) -> Result<(), Error> {
    let action_digest = DigestInfo::new(action_hash, 512);
    let mut action_info = make_base_action_info(make_system_time(ts), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let client_id = OperationId::default();
    let _listener = scheduler.add_action(client_id, action_info).await?;
    scheduler.do_try_match_for_test().await?;
    // Consume the StartAction so the channel is empty for the next assertion.
    // The action is intentionally NOT completed — it stays in-flight in the
    // worker's `running_action_infos`, consuming a P-headroom slot.
    match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
        Ok(Some(msg)) => match msg.update {
            Some(update_for_worker::Update::StartAction(_)) => Ok(()),
            v => panic!("dispatch_and_hold: expected StartAction, got: {v:?}"),
        },
        Ok(None) => panic!("dispatch_and_hold: worker channel closed"),
        Err(_) => panic!(
            "dispatch_and_hold: action did not dispatch to the intended worker \
             (it was routed elsewhere or parked)"
        ),
    }
}

/// Test 2 (§7): the P-headroom gate (flag ON). A worker HOLDING the cached input
/// root but AT its dispatch-count P-headroom limit (`running == p_core_count`)
/// LOSES the next input-sharing action to a P-headroom NON-holder. Without the
/// gate the saturated holder wins on cache affinity (Tier-1) — the 2026-06-30
/// sole-holder domino.
///
/// Topology: holder H has `p_core_count = 1` and one in-flight action (so it has
/// NO P-headroom), holds the input root, and reports HIGH p_load (so the LRU
/// backstop — which is load-based, not dispatch-count, per design A3 — ranks it
/// below the peer once the cache tiers are gated off). Peer P has
/// `p_core_count = 8`, zero in-flight (P-headroom), no cache, and reports LOW
/// p_load. Flag ON.
///
/// Mutation (TDD #5): comment out the `has_p_headroom`/`p_gate_active` exclusion
/// in `inner_find_and_reserve_worker` so the cache tiers no longer skip a
/// no-headroom worker. This red-fails: H (Tier-1 cache holder) wins the second
/// action and P never receives it.
#[nativelink_test]
async fn p_headroom_gate_overflows_saturated_holder_to_peer_test() -> Result<(), Error> {
    let holder = WorkerId("holder_no_headroom".to_string());
    let peer = WorkerId("peer_with_headroom".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            // Flag ON — enable the dispatch-count P-headroom overflow gate.
            p_headroom_gate_enabled: true,
            // Non-zero so the continuous load penalty / LRU load score
            // discriminate between the workers (default-derived spec is 0).
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None, // cas_store
        None, // locality_map
        None, // worker_tls_config
    );

    // Holder: 1 P-core (saturated after a single in-flight action), e_count>0 so
    // it stays non-saturated (weighted_free>0) and remains a viable candidate.
    let mut rx_h = setup_new_worker_with_core_counts(
        &scheduler,
        holder.clone(),
        PlatformProperties::default(),
        1, // p_core_count — one in-flight action removes all P-headroom
        6, // e_core_count
    )
    .await?;
    // Peer: 8 P-cores → ample P-headroom.
    let mut rx_p = setup_new_worker_with_core_counts(
        &scheduler,
        peer.clone(),
        PlatformProperties::default(),
        8,
        6,
    )
    .await?;

    let input_root = DigestInfo::new([70u8; 32], 2048);

    // Holder caches the input root and reports a moderate load so it is the
    // viable Tier-1 winner for the FIRST (priming) action.
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;
    scheduler.update_worker_load(&holder, 40, 40, 40).await?;
    // Peer reports moderate load too; it does NOT hold the root.
    scheduler.update_worker_load(&peer, 40, 40, 40).await?;

    // Prime: dispatch one action (input_root) — holder is the sole Tier-1 cache
    // holder → it lands on the holder, held in-flight. Holder now has
    // running_action_infos.len() == 1 == p_core_count → NO P-headroom.
    dispatch_and_hold_on_worker(&scheduler, &mut rx_h, input_root, [71u8; 32], 1).await?;

    // Now the holder is P-saturated (dispatch-count). Report it as heavily loaded
    // and the peer as idle so that once the cache tiers are gated off, the
    // load-based LRU backstop prefers the peer (design A3: Phase-fallthrough
    // ranking is load-based). Holder still non-saturated (e-cores free) so it
    // remains a *viable* candidate the gate must actively exclude.
    scheduler.update_worker_load(&holder, 100, 20, 60).await?;
    scheduler.update_worker_load(&peer, 5, 5, 5).await?;

    // Second action, SAME input root. Gate ON: holder has no P-headroom, peer
    // does → holder is excluded from the cache tiers → cache tiers decline (peer
    // is cache-cold) → LRU backstop selects the lighter-loaded peer.
    let action_digest = DigestInfo::new([72u8; 32], 512);
    let mut action_info = make_base_action_info(make_system_time(2), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let mut listener = scheduler.add_action(OperationId::default(), action_info).await?;
    scheduler.do_try_match_for_test().await?;

    let winner = tokio::select! {
        msg = rx_h.recv() => {
            match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => holder.clone(),
                v => panic!("holder produced non-StartAction: {v:?}"),
            }
        }
        msg = rx_p.recv() => {
            match msg.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => peer.clone(),
                v => panic!("peer produced non-StartAction: {v:?}"),
            }
        }
    };

    assert_eq!(
        winner, peer,
        "P-headroom gate (flag ON): a cache-holding worker AT its dispatch-count \
         P-headroom limit (running == p_core_count) must LOSE the next \
         input-sharing action to a P-headroom peer — the surplus overflowed to \
         the saturated holder instead (2026-06-30 sole-holder domino)"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

/// Test 3 (§7): Phase-2 lift + no-wedge (flag ON). When EVERY viable worker is at
/// its dispatch-count P-headroom limit, the gate LIFTS (Phase 2): the
/// cache-affinity tiers are re-enabled over the fully-saturated fleet so a cache
/// holder still wins (locality is free when every worker is equally P-full), and
/// dispatch always proceeds (no wedge).
///
/// Topology: both workers are P-saturated (`p_core_count = 1`, one in-flight
/// each). Holder H caches the input root. Peer P is cache-cold but reports a
/// LIGHTER load. With the gate ACTIVE, H (cache-holding, no headroom) would be
/// excluded and the action would fall to the load-ranked LRU → the lighter peer
/// P. The Phase-1 condition (`any_viable_has_p_headroom == false` here) LIFTS the
/// gate so the cache tier fires and H wins on locality instead.
///
/// Mutation (TDD #5): drop the `any_viable_has_p_headroom` term from
/// `p_gate_active` (gate unconditionally while the flag is on). This red-fails:
/// the gate never lifts, H is excluded from the cache tiers even when the whole
/// fleet is saturated, and the action falls to the lighter-loaded cache-cold
/// peer P — losing Phase-2 locality.
#[nativelink_test]
async fn p_headroom_gate_phase2_lift_dispatches_when_all_saturated_test() -> Result<(), Error> {
    let holder = WorkerId("phase2_holder".to_string());
    let peer = WorkerId("phase2_peer".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Both workers: p_core_count = 1 (P-headroom gone after one in-flight
    // action), e_core_count = 6 (stays non-saturated → viable).
    let mut rx_h = setup_new_worker_with_core_counts(
        &scheduler,
        holder.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;
    let mut rx_p = setup_new_worker_with_core_counts(
        &scheduler,
        peer.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;

    let input_root = DigestInfo::new([80u8; 32], 2048);
    // Holder caches the root; report a moderate load so it is the priming
    // action's Tier-1 winner.
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;
    scheduler.update_worker_load(&holder, 40, 40, 40).await?;
    scheduler.update_worker_load(&peer, 40, 40, 40).await?;

    // Saturate BOTH workers' P-headroom (one held in-flight action each). The
    // holder's priming action shares the cached root so Tier-1 places it on the
    // holder; the peer is primed with a cacheless action via the LRU path.
    dispatch_and_hold_on_worker(&scheduler, &mut rx_h, input_root, [81u8; 32], 1).await?;
    // Prime the peer: a cacheless action. With the holder now P-saturated and
    // the gate active, this action is gated off the holder and lands on the
    // (P-headroom-at-this-instant) peer via the fallback.
    let prime_peer = DigestInfo::new([84u8; 32], 512);
    let _pl = setup_action(&scheduler, prime_peer, HashMap::new(), make_system_time(2)).await?;
    scheduler.do_try_match_for_test().await?;
    match tokio::time::timeout(Duration::from_millis(200), rx_p.recv()).await {
        Ok(Some(msg)) => assert!(
            matches!(msg.update, Some(update_for_worker::Update::StartAction(_))),
            "priming action did not land on the peer"
        ),
        _ => panic!("peer was not primed to P-saturation"),
    }

    // Now BOTH workers are P-saturated (running == p_core_count == 1). Make the
    // peer strictly lighter-loaded than the holder so that IF the gate failed to
    // lift, the load-ranked LRU fallback would pick the peer, not the holder.
    scheduler.update_worker_load(&holder, 90, 20, 60).await?;
    scheduler.update_worker_load(&peer, 5, 5, 5).await?;

    // Third action, SAME cached root. No viable worker has P-headroom → the gate
    // LIFTS → the cache tier fires → the holder wins on locality (Phase-2 locality
    // preserved). And of course it dispatches (no wedge).
    let action_digest = DigestInfo::new([83u8; 32], 512);
    let mut action_info = make_base_action_info(make_system_time(3), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let mut listener = scheduler.add_action(OperationId::default(), action_info).await?;
    scheduler.do_try_match_for_test().await?;

    let winner = tokio::select! {
        msg = rx_h.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(holder.clone()),
            _ => None,
        },
        msg = rx_p.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(peer.clone()),
            _ => None,
        },
        () = tokio::time::sleep(Duration::from_millis(300)) => None,
    };

    assert_eq!(
        winner,
        Some(holder.clone()),
        "Phase-2 lift (flag ON): every viable worker is P-saturated, so the gate \
         must LIFT and re-enable the cache tiers → the cache holder wins on \
         locality. Instead the action was wedged or fell to the lighter-loaded \
         cache-cold peer (the gate did not lift on a fully-saturated fleet)"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "Phase-2 action stuck in a non-Executing stage — the gate wedged a \
         fully-P-saturated fleet"
    );

    Ok(())
}

/// Test 4 (§7 + A5): `p_core_count == 0` guard. A worker advertising
/// `p_core_count == 0` must be treated as UNGATED (always has_p_headroom) so it
/// degrades to current behavior rather than being frozen out — otherwise
/// `0 < 0 == false` would permanently exclude it from Phase-1 cache-tier work.
///
/// Topology (makes the A5 guard load-bearing): a count-0 cache HOLDER H plus a
/// count>0 cache-cold peer P WITH P-headroom. P's headroom keeps the gate ACTIVE
/// (`any_viable_has_p_headroom == true`), so the gate does NOT lift — the A5
/// short-circuit is the ONLY thing keeping H (count-0) in the cache tiers. P is
/// strictly lighter-loaded than H so that IF H were gated out, the action would
/// fall to the load-ranked LRU and land on P (not H).
///
/// Mutation (TDD #5): drop the `w.p_core_count == 0` short-circuit in
/// `has_p_headroom` so a count-0 worker computes `0 < 0 == false` (no headroom).
/// This red-fails: with the gate still active (P has headroom), H is excluded
/// from the cache tiers and the cached action falls to the lighter-loaded peer P
/// instead of the count-0 holder H.
#[nativelink_test]
async fn p_headroom_gate_p_core_count_zero_is_ungated_test() -> Result<(), Error> {
    let holder = WorkerId("count_zero_holder".to_string());
    let peer = WorkerId("count_nonzero_peer".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Holder H: p_core_count == 0 (legacy / Linux / Intel-Mac shape), some
    // e-cores so it is non-saturated (viable). Holds the cached input root.
    let mut rx_h = setup_new_worker_with_core_counts(
        &scheduler,
        holder.clone(),
        PlatformProperties::default(),
        0, // p_core_count == 0 → must be treated as UNGATED (A5)
        6,
    )
    .await?;
    // Peer P: real P cores with headroom → keeps the gate ACTIVE. Cache-cold.
    let mut rx_p = setup_new_worker_with_core_counts(
        &scheduler,
        peer.clone(),
        PlatformProperties::default(),
        8,
        6,
    )
    .await?;

    let input_root = DigestInfo::new([90u8; 32], 2048);
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;
    // H heavier, P lighter: if H were (wrongly) gated out, the LRU fallback would
    // pick the lighter-loaded P, not H.
    scheduler.update_worker_load(&holder, 80, 20, 60).await?;
    scheduler.update_worker_load(&peer, 5, 5, 5).await?;

    let action_digest = DigestInfo::new([91u8; 32], 512);
    let mut action_info = make_base_action_info(make_system_time(1), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let mut listener = scheduler.add_action(OperationId::default(), action_info).await?;
    scheduler.do_try_match_for_test().await?;

    let winner = tokio::select! {
        msg = rx_h.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(holder.clone()),
            _ => None,
        },
        msg = rx_p.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(peer.clone()),
            _ => None,
        },
        () = tokio::time::sleep(Duration::from_millis(300)) => None,
    };

    assert_eq!(
        winner,
        Some(holder.clone()),
        "p_core_count == 0 guard (A5): a count-0 worker must be treated as \
         UNGATED (always has P-headroom) so it stays in the cache tiers even \
         while the gate is active — the count-0 cache holder was frozen out and \
         the action fell to the cache-cold peer instead"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

/// Test 5 (§7): production-composition — a 10-worker synthetic fleet with one
/// cache holder and a burst exceeding its `p_core_count`. Asserts the burst
/// spreads to P-headroom peers rather than piling onto the (P-saturated) holder
/// (the incident scenario). Flag ON.
///
/// Mutation (TDD #5): remove the gate exclusion → every burst action lands on
/// the holder (Tier-1 cache) and the peers receive none, red-failing the "spread"
/// assertion.
#[nativelink_test]
async fn p_headroom_gate_burst_spreads_across_fleet_test() -> Result<(), Error> {
    const FLEET: usize = 10;
    const HOLDER_P_CORES: u32 = 2;
    // Burst larger than the holder's P capacity so the surplus MUST overflow.
    const BURST: usize = 6;

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Worker 0 is the cache holder; workers 1..10 are cache-cold P-headroom
    // peers. All report a light load so all are viable (non-saturated).
    let mut rxs: Vec<(WorkerId, mpsc::UnboundedReceiver<UpdateForWorker>)> = Vec::new();
    for i in 0..FLEET {
        let wid = WorkerId(format!("fleet_worker_{i}"));
        // Holder: small P count so its P-headroom is exhausted mid-burst.
        // Peers: ample P headroom.
        let p_cores = if i == 0 { HOLDER_P_CORES } else { 8 };
        let rx = setup_new_worker_with_core_counts(
            &scheduler,
            wid.clone(),
            PlatformProperties::default(),
            p_cores,
            6,
        )
        .await?;
        scheduler.update_worker_load(&wid, 20, 20, 20).await?;
        rxs.push((wid, rx));
    }

    let holder = rxs[0].0.clone();
    let input_root = DigestInfo::new([100u8; 32], 4096);
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;

    // Fire a burst of BURST actions all sharing the holder's cached input root.
    let mut listeners = Vec::new();
    for j in 0..BURST {
        let mut hash = [0u8; 32];
        hash[0] = 200;
        hash[1] = j as u8;
        let action_digest = DigestInfo::new(hash, 512);
        let mut action_info =
            make_base_action_info(make_system_time(10 + j as u64), action_digest);
        Arc::make_mut(&mut action_info).input_root_digest = input_root;
        listeners.push(scheduler.add_action(OperationId::default(), action_info).await?);
    }
    // Drive matching until the whole burst is placed. Each cycle places up to
    // MATCH_CONCURRENCY actions; a few cycles cover the burst as holder headroom
    // is consumed and later actions overflow.
    for _ in 0..(BURST + 2) {
        scheduler.do_try_match_for_test().await?;
    }

    // Count how many StartActions each worker received.
    let mut holder_count = 0usize;
    let mut peer_count = 0usize;
    for (idx, (_wid, rx)) in rxs.iter_mut().enumerate() {
        let mut n = 0usize;
        while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            if matches!(msg.update, Some(update_for_worker::Update::StartAction(_))) {
                n += 1;
            }
        }
        if idx == 0 {
            holder_count = n;
        } else {
            peer_count += n;
        }
    }

    // The holder can absorb at most its P-headroom worth of the burst; the rest
    // MUST spread to P-headroom peers.
    assert!(
        holder_count <= HOLDER_P_CORES as usize,
        "burst spread (flag ON): the cache holder absorbed {holder_count} of the \
         {BURST}-action burst but its P-headroom is only {HOLDER_P_CORES} — the \
         gate failed to cap the holder at its P capacity"
    );
    assert!(
        peer_count > 0,
        "burst spread (flag ON): no burst action overflowed to a P-headroom peer \
         (holder absorbed {holder_count}) — the surplus piled onto the saturated \
         holder instead of spreading (the 2026-06-30 incident scenario)"
    );
    assert_eq!(
        holder_count + peer_count,
        BURST,
        "burst spread (flag ON): {} of {BURST} burst actions were placed \
         (holder {holder_count} + peers {peer_count}) — some action wedged",
        holder_count + peer_count
    );

    Ok(())
}

/// Test 6 (§7): flag ON must NOT regress the FL-681 indefinite-pin gate. A worker
/// reporting indefinite-pin saturation is skipped on the cache-affinity path
/// regardless of the P-headroom gate — the two predicates compose (both exclude).
/// (The zero-load and FL-681 OFF-path behaviors are already covered by the
/// existing suite, which runs with the flag defaulting OFF — the flag-OFF
/// zero-change safety property. This test adds the flag-ON leg for FL-681.)
///
/// Mutation (TDD #5): removing the FL-681 `indefinite_pin_saturated` skip in
/// `worker_is_viable` red-fails this (the saturated worker is selected and the
/// action dispatches instead of parking in Queued).
#[nativelink_test]
async fn p_headroom_gate_on_preserves_fl681_indefinite_pin_skip_test() -> Result<(), Error> {
    let worker = WorkerId("fl681_with_gate_on".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    let mut rx = setup_new_worker_with_cas_endpoint_and_cores(
        &scheduler,
        worker.clone(),
        PlatformProperties::default(),
        "fl681-gate:50081",
        4, // ample P-headroom so the gate itself would NOT exclude it
        6,
    )
    .await?;

    let input_root = DigestInfo::new([110u8; 32], 4096);
    let mut cached_dirs = std::collections::HashSet::new();
    cached_dirs.insert(input_root);
    scheduler.update_cached_directories(&worker, cached_dirs).await?;
    scheduler.update_worker_load(&worker, 10, 10, 10).await?;

    // Report indefinite-pin saturation → the worker must be skipped on the
    // cache-affinity path even with the P-headroom gate ON (it has P-headroom,
    // so ONLY the FL-681 predicate keeps it out).
    scheduler
        .update_worker_indefinite_pin_saturation(&worker, true)
        .await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([111u8; 32], 512);
    let mut listener = setup_action_with_input_root(
        &scheduler,
        action_digest,
        input_root,
        HashMap::new(),
        make_system_time(1),
    )
    .await?;
    scheduler.do_try_match_for_test().await?;

    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Queued,
        "FL-681 with P-gate ON: an indefinite-pin-saturated worker must still be \
         skipped on the cache-affinity path (the P-headroom gate must not shadow \
         or bypass the FL-681 re-saturation gate) — the action dispatched to the \
         saturated worker instead of parking in Queued"
    );

    // Clearing saturation makes the (P-headroom) cache holder selectable again.
    scheduler
        .update_worker_indefinite_pin_saturation(&worker, false)
        .await?;
    tokio::task::yield_now().await;
    scheduler.do_try_match_for_test().await?;

    let mut saw_start = false;
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(msg)) => match msg.update {
                Some(update_for_worker::Update::StartAction(_)) => {
                    saw_start = true;
                    break;
                }
                Some(_) => continue,
                None => break,
            },
            Ok(None) | Err(_) => break,
        }
    }
    assert!(
        saw_start,
        "FL-681 with P-gate ON: the cache holder must be re-selectable once \
         indefinite-pin saturation clears (it has P-headroom, so the P-gate does \
         not exclude it)"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

// ─────────────────────── (#sched M1 rebalance §11 v2.2) ───────────────────────
// SOFT P-headroom-first fallback ranking. The M1 cache-tier gate excludes a
// no-headroom holder on FRESH dispatch-count, but the overflow is PLACED by the
// LRU/MRU fallback, which ranked purely on STALE p_load — so in the signal-
// disagreement case (holder P-saturated by dispatch-count yet reporting LOWER
// p_load than a P-headroom peer) the overflow routed BACK to the excluded holder
// (red-team RECONSIDER-PREMISE). v2.2 makes the fallback sort key a tuple
// `(no_p_headroom_tier, effective_load_score)` — a SOFT preference: P-headroom
// workers rank first, but no-headroom workers stay eligible (win when the fleet
// is fully P-saturated → no wedge). Flag OFF → tier always false → byte-identical.

/// v2.2 Test 1 — THE disagreement test. Holder H holds the cached root, is
/// P-saturated (running >= p_core_count), and reports a LOWER p_load than a
/// P-headroom peer P. Flag ON. The M1 gate excludes H from the cache tiers; the
/// overflow must land on the P-headroom peer P (tier 0), NOT route back to the
/// lower-p_load holder H (tier 1) via the fallback's stale-p_load ranking.
///
/// Mutation (TDD #5): revert the fallback sort key to single-tier
/// (`effective_load_score` only — drop the `no_p_headroom_tier` element). This
/// red-fails: the fallback then prefers H's lower p_load and the overflow routes
/// back to the excluded, dispatch-count-saturated holder.
#[nativelink_test]
async fn v22_fallback_prefers_p_headroom_peer_over_lower_pload_holder_test()
-> Result<(), Error> {
    let holder = WorkerId("v22_holder_low_pload".to_string());
    let peer = WorkerId("v22_peer_high_pload".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Holder H: 1 P-core (saturated after one in-flight action), e_count>0 so it
    // stays non-saturated (viable). Peer P: ample P-headroom.
    let mut rx_h = setup_new_worker_with_core_counts(
        &scheduler,
        holder.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;
    let mut rx_p = setup_new_worker_with_core_counts(
        &scheduler,
        peer.clone(),
        PlatformProperties::default(),
        8,
        6,
    )
    .await?;

    let input_root = DigestInfo::new([120u8; 32], 2048);
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;
    // Prime: dispatch one cached action → lands on H (sole Tier-1 holder), held
    // in-flight → H now running==1==p_core_count → NO P-headroom.
    scheduler.update_worker_load(&holder, 40, 40, 40).await?;
    scheduler.update_worker_load(&peer, 40, 40, 40).await?;
    dispatch_and_hold_on_worker(&scheduler, &mut rx_h, input_root, [121u8; 32], 1).await?;

    // THE disagreement: holder reports LOWER p_load (20) than the P-headroom peer
    // (60). Fresh dispatch-count says H is saturated; stale p_load says H is
    // lighter. The M1 gate excludes H from the cache tiers; the fallback must
    // steer the overflow to the P-headroom PEER (tier 0) despite its higher
    // p_load — NOT back to H (tier 1) on its lower p_load.
    // update_worker_load args are (cpu, p_core, e_core): set p_core so
    // effective_load_score = holder 20 < peer 60 (the stale-p_load "H is lighter"
    // signal the tier must override).
    scheduler.update_worker_load(&holder, 20, 20, 0).await?;
    scheduler.update_worker_load(&peer, 60, 60, 0).await?;

    let action_digest = DigestInfo::new([122u8; 32], 512);
    let mut action_info = make_base_action_info(make_system_time(2), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let mut listener = scheduler.add_action(OperationId::default(), action_info).await?;
    scheduler.do_try_match_for_test().await?;

    let winner = tokio::select! {
        msg = rx_h.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(holder.clone()),
            _ => None,
        },
        msg = rx_p.recv() => match msg.and_then(|m| m.update) {
            Some(update_for_worker::Update::StartAction(_)) => Some(peer.clone()),
            _ => None,
        },
        () = tokio::time::sleep(Duration::from_millis(300)) => None,
    };

    assert_eq!(
        winner,
        Some(peer.clone()),
        "v2.2 disagreement (flag ON): the M1 gate excludes the P-saturated cache \
         holder on FRESH dispatch-count, but the holder reports a LOWER (stale) \
         p_load than the P-headroom peer. The soft P-headroom-first fallback must \
         steer the overflow to the P-headroom PEER (tier 0), not route it back to \
         the excluded holder on its stale-lower p_load (tier 1)"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing
    );

    Ok(())
}

/// v2.2 Test 2 — no-wedge under the refinement. Every viable worker is
/// P-saturated (running >= p_core_count), flag ON. The soft tier is NOT a filter:
/// no-headroom workers stay eligible (all in tier 1), so the fallback still
/// dispatches. This is the load-bearing constraint — a HARD filter here would
/// reintroduce the TLC-proven Phase2NoLift wedge.
///
/// Mutation (TDD #5): turn the soft tier into a HARD filter (drop no-headroom
/// workers from the fallback candidate `Vec` when the flag is on). This red-fails:
/// the fully-saturated fleet yields zero fallback candidates → the action wedges.
#[nativelink_test]
async fn v22_fallback_no_wedge_when_all_p_saturated_test() -> Result<(), Error> {
    let worker_a = WorkerId("v22_wedge_a".to_string());
    let worker_b = WorkerId("v22_wedge_b".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    // Both p_core_count=1, e_count=6: one in-flight action removes P-headroom
    // while keeping them non-saturated (viable). No cache → pure fallback.
    let mut rx_a = setup_new_worker_with_core_counts(
        &scheduler,
        worker_a.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;
    let mut rx_b = setup_new_worker_with_core_counts(
        &scheduler,
        worker_b.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;
    scheduler.update_worker_load(&worker_a, 40, 40, 40).await?;
    scheduler.update_worker_load(&worker_b, 40, 40, 40).await?;

    let dummy_root = DigestInfo::new([130u8; 32], 1024);
    dispatch_and_hold_on_worker(&scheduler, &mut rx_a, dummy_root, [131u8; 32], 1).await?;
    dispatch_and_hold_on_worker(&scheduler, &mut rx_b, dummy_root, [132u8; 32], 2).await?;

    // Both are now P-saturated (running==1==p_core_count). A cacheless action
    // must STILL dispatch via the soft tier-1 (no-headroom stays eligible).
    let action_digest = DigestInfo::new([133u8; 32], 512);
    let mut listener =
        setup_action(&scheduler, action_digest, HashMap::new(), make_system_time(3)).await?;
    scheduler.do_try_match_for_test().await?;

    let dispatched = tokio::select! {
        msg = rx_a.recv() => matches!(
            msg.and_then(|m| m.update),
            Some(update_for_worker::Update::StartAction(_))
        ),
        msg = rx_b.recv() => matches!(
            msg.and_then(|m| m.update),
            Some(update_for_worker::Update::StartAction(_))
        ),
        () = tokio::time::sleep(Duration::from_millis(300)) => false,
    };

    assert!(
        dispatched,
        "v2.2 no-wedge (flag ON): every viable worker is P-saturated, so the soft \
         P-headroom-first tier must keep them ELIGIBLE (tier 1) and the fallback \
         must still dispatch — a HARD filter would wedge the fully-saturated fleet"
    );
    assert_eq!(
        listener.changed().await.unwrap().0.stage,
        ActionStage::Executing,
        "v2.2 no-wedge: the fully-P-saturated fallback action stuck in a \
         non-Executing stage"
    );

    Ok(())
}

/// v2.2 Test 3 (G1, testing-czar) — the A2 exclusion log actually EMITS at INFO
/// with the `p_load` field, so a future info!→debug! demotion (which
/// `release_max_level_info` would compile out of prod) is caught. Drives an M1
/// exclusion (holder P-saturated + cache-holding + a P-headroom peer), then
/// asserts the tagged line fired at INFO carrying `p_load=`.
///
/// Mutation (TDD #5): change the A2 `info!` to `debug!`, OR drop the `p_load`
/// field. Either red-fails (no INFO-level line with the tag + p_load field).
#[nativelink_test]
async fn v22_a2_exclusion_log_emits_at_info_with_pload_test() -> Result<(), Error> {
    let holder = WorkerId("v22_a2_holder".to_string());
    let peer = WorkerId("v22_a2_peer".to_string());

    let task_change_notify = Arc::new(Notify::new());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &SimpleSpec {
            p_headroom_gate_enabled: true,
            load_byte_cost: 512 * 1024,
            ..Default::default()
        },
        memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
        || async move {},
        task_change_notify,
        MockInstantWrapped::default,
        None,
        None,
        None,
        None,
    );

    let mut rx_h = setup_new_worker_with_core_counts(
        &scheduler,
        holder.clone(),
        PlatformProperties::default(),
        1,
        6,
    )
    .await?;
    let _rx_p = setup_new_worker_with_core_counts(
        &scheduler,
        peer.clone(),
        PlatformProperties::default(),
        8,
        6,
    )
    .await?;

    let input_root = DigestInfo::new([140u8; 32], 2048);
    scheduler
        .update_cached_subtrees(&holder, true, vec![input_root], vec![], vec![])
        .await?;
    scheduler.update_worker_load(&holder, 40, 40, 40).await?;
    scheduler.update_worker_load(&peer, 40, 40, 40).await?;
    // Saturate H's P-headroom (holds the cached root).
    dispatch_and_hold_on_worker(&scheduler, &mut rx_h, input_root, [141u8; 32], 1).await?;
    // Keep peer with P-headroom (gate active), holder now excluded on next match.
    scheduler.update_worker_load(&holder, 25, 0, 25).await?;
    scheduler.update_worker_load(&peer, 5, 5, 5).await?;

    // Second cached action → the gate is active and excludes H → A2 log emits.
    let action_digest = DigestInfo::new([142u8; 32], 512);
    let mut action_info = make_base_action_info(make_system_time(2), action_digest);
    Arc::make_mut(&mut action_info).input_root_digest = input_root;
    let _listener = scheduler.add_action(OperationId::default(), action_info).await?;
    scheduler.do_try_match_for_test().await?;

    // Assert the A2 exclusion line fired AT INFO carrying the tag + p_load field.
    // logs_assert hands us level-prefixed formatted lines; a single line must
    // carry INFO, the tag, and the structured `p_load=` field (not just the
    // message text) — so an info!→debug! demotion OR a dropped p_load field is
    // caught.
    logs_assert(|lines: &[&str]| {
        if lines.iter().any(|line| {
            line.contains("INFO")
                && line.contains("p_headroom_gate_exclusion")
                && line.contains("p_load=")
        }) {
            Ok(())
        } else {
            Err(
                "A2 p_headroom_gate_exclusion line not emitted at INFO with p_load= field"
                    .to_string(),
            )
        }
    });

    Ok(())
}

/// v2.2 Test 4 (G2, testing-czar) — flag-ON-but-Phase-2 == flag-OFF byte-
/// identical. On a fully-P-saturated fleet (Phase 2, gate lifted) the SAME worker
/// must be picked with the flag ON as with the flag OFF for an identical topology
/// — proving the refinement collapses to the shipped behavior once no worker has
/// P-headroom (the soft tier is uniform → key reduces to load only).
///
/// Mutation (TDD #5): none needed as a red-fail here — this is an equivalence
/// assertion (ON == OFF under Phase 2). It is protected by the v2.2 Test-1
/// disagreement mutation (which proves the tier is load-bearing in Phase 1) and
/// the flag-OFF byte-identical property (the whole existing suite).
#[nativelink_test]
async fn v22_phase2_flag_on_equals_flag_off_test() -> Result<(), Error> {
    // Identical topology, run once with flag ON and once with flag OFF; assert
    // the same worker wins. Both workers P-saturated (Phase 2), distinct loads so
    // the load ranking (not iteration order) decides deterministically.
    async fn run(flag: bool) -> Result<WorkerId, Error> {
        let light = WorkerId("v22_p2_light".to_string());
        let heavy = WorkerId("v22_p2_heavy".to_string());
        let task_change_notify = Arc::new(Notify::new());
        let (scheduler, _ws) = SimpleScheduler::new_with_callback(
            &SimpleSpec {
                p_headroom_gate_enabled: flag,
                load_byte_cost: 512 * 1024,
                ..Default::default()
            },
            memory_awaited_action_db_factory(0, &task_change_notify.clone(), MockInstantWrapped::default),
            || async move {},
            task_change_notify,
            MockInstantWrapped::default,
            None,
            None,
            None,
            None,
        );

        let mut rx_light = setup_new_worker_with_core_counts(
            &scheduler,
            light.clone(),
            PlatformProperties::default(),
            1,
            6,
        )
        .await?;
        let mut rx_heavy = setup_new_worker_with_core_counts(
            &scheduler,
            heavy.clone(),
            PlatformProperties::default(),
            1,
            6,
        )
        .await?;
        scheduler.update_worker_load(&light, 40, 40, 40).await?;
        scheduler.update_worker_load(&heavy, 40, 40, 40).await?;

        // Saturate BOTH P-headroom (Phase 2 for the flag-ON run).
        let dummy_root = DigestInfo::new([150u8; 32], 1024);
        dispatch_and_hold_on_worker(&scheduler, &mut rx_light, dummy_root, [151u8; 32], 1).await?;
        dispatch_and_hold_on_worker(&scheduler, &mut rx_heavy, dummy_root, [152u8; 32], 2).await?;

        // Distinct P-CORE loads (`update_worker_load` args are cpu, p_core,
        // e_core): `light` p_load=10 < `heavy` p_load=90 → `light` wins on
        // `effective_load_score` in BOTH runs (ON: both tier 1, ranked by load;
        // OFF: single tier, ranked by load) — the LOAD decides, not iteration
        // order. (A prior draft set p_core=0 for both → both scored 0 → the
        // winner was iteration order, so the parity was only degenerate-load;
        // testing-czar R1.)
        scheduler.update_worker_load(&light, 10, 10, 0).await?;
        scheduler.update_worker_load(&heavy, 90, 90, 0).await?;

        let action_digest = DigestInfo::new([153u8; 32], 512);
        let _l = setup_action(&scheduler, action_digest, HashMap::new(), make_system_time(3)).await?;
        scheduler.do_try_match_for_test().await?;

        let winner = tokio::select! {
            msg = rx_light.recv() => match msg.and_then(|m| m.update) {
                Some(update_for_worker::Update::StartAction(_)) => light.clone(),
                v => panic!("light produced non-StartAction: {v:?}"),
            },
            msg = rx_heavy.recv() => match msg.and_then(|m| m.update) {
                Some(update_for_worker::Update::StartAction(_)) => heavy.clone(),
                v => panic!("heavy produced non-StartAction: {v:?}"),
            },
            () = tokio::time::sleep(Duration::from_millis(300)) => {
                panic!("Phase-2 fallback wedged (no worker received the action)")
            }
        };
        Ok(winner)
    }

    let winner_on = run(true).await?;
    let winner_off = run(false).await?;
    assert_eq!(
        winner_on, winner_off,
        "v2.2 G2 equivalence: on a fully-P-saturated fleet (Phase 2) the flag-ON \
         fallback must pick the SAME worker as flag-OFF — the soft tier is uniform \
         when no worker has P-headroom, so the key reduces to load only (both runs \
         must pick the lighter-loaded worker)"
    );

    Ok(())
}
