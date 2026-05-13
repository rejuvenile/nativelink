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

//! AC-poisoning fix tests:
//! - C.1: `cancel_operation_internal` routes `KillOperationRequest`
//!   to the worker assigned to the operation.
//! - C.1b: cancel for unknown operation is `Ok` (idempotent).

use core::time::Duration;
use std::sync::Arc;

use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler; // for add_worker
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::ClientStateManager;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

mod utils {
    pub(crate) mod scheduler_utils;
}

use utils::scheduler_utils::make_base_action_info;

const NOW_TIME: u64 = 10000;

async fn setup_new_worker(
    scheduler: &SimpleScheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(worker_id.clone(), props, tx, NOW_TIME, 0);
    scheduler.add_worker(worker).await?;
    tokio::task::yield_now().await;
    // Drain the initial ConnectionResult message.
    let _connection = rx.recv().await.expect("expected ConnectionResult");
    Ok(rx)
}

/// C.1: cancel_operation_internal sends KillOperationRequest to the
/// worker that the operation was assigned to. Mutation: comment out
/// the `tx.send(msg)` in `cancel_operation_internal` — this test
/// must red-fail with bespoke "expected target_worker.tx to receive
/// KillOperationRequest, got nothing".
#[nativelink_test]
async fn cancel_operation_internal_sends_kill_to_assigned_worker() -> Result<(), Error> {
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
        None,
        None,
        None,
    );

    // Add two workers; only the worker that gets the StartAction
    // should receive the kill.
    let worker_a = WorkerId("worker_a".to_string());
    let worker_b = WorkerId("worker_b".to_string());
    let mut rx_a =
        setup_new_worker(&scheduler, worker_a.clone(), PlatformProperties::default()).await?;
    let mut rx_b =
        setup_new_worker(&scheduler, worker_b.clone(), PlatformProperties::default()).await?;

    // Submit an action so it gets dispatched to one of the workers.
    let action_digest = DigestInfo::new([99u8; 32], 512);
    let action_info = make_base_action_info(
        std::time::UNIX_EPOCH + Duration::from_secs(NOW_TIME + 1),
        action_digest,
    );
    let _action_listener = scheduler
        .add_action(OperationId::default(), action_info)
        .await?;
    tokio::task::yield_now().await;

    // Probe both rx in parallel; whichever wins is the target.
    let target_op_id = tokio::select! {
        msg = rx_a.recv() => {
            let msg = msg.expect("rx_a closed");
            if let Some(update_for_worker::Update::StartAction(start)) = msg.update {
                OperationId::from(start.operation_id)
            } else {
                panic!("expected StartAction on rx_a, got {:?}", msg.update);
            }
        }
        msg = rx_b.recv() => {
            let msg = msg.expect("rx_b closed");
            if let Some(update_for_worker::Update::StartAction(start)) = msg.update {
                // Move the rx to the canonical position so the
                // assertion below checks the same channel.
                core::mem::swap(&mut rx_a, &mut rx_b);
                OperationId::from(start.operation_id)
            } else {
                panic!("expected StartAction on rx_b, got {:?}", msg.update);
            }
        }
        () = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("must receive StartAction within timeout — scheduler dispatch wedged");
        }
    };

    // After the swap, rx_a is always the target worker's rx.
    // Issue cancel through the canonical client path
    // (ClientStateManager::cancel_operation → SimpleScheduler →
    // ApiWorkerScheduler::cancel_operation_internal).
    scheduler.cancel_operation(&target_op_id).await?;

    // Target worker MUST receive the KillOperationRequest.
    let kill_msg = tokio::time::timeout(Duration::from_secs(5), rx_a.recv())
        .await
        .expect(
            "expected target_worker.tx to receive KillOperationRequest within 5s — got timeout",
        )
        .expect("expected target_worker.tx to receive KillOperationRequest, got nothing");

    match kill_msg.update {
        Some(update_for_worker::Update::KillOperationRequest(req)) => {
            assert_eq!(
                req.operation_id,
                target_op_id.to_string(),
                "expected kill to carry the target operation_id"
            );
        }
        other => panic!("expected KillOperationRequest, got {:?}", other),
    }

    // Other worker MUST NOT receive any kill.
    match tokio::time::timeout(Duration::from_millis(200), rx_b.recv()).await {
        Err(_) => {} // expected: timeout = no message
        Ok(None) => {} // channel closed is fine
        Ok(Some(msg)) => match msg.update {
            Some(update_for_worker::Update::KillOperationRequest(_)) => {
                panic!("expected non-target worker.tx to receive NO KillOperationRequest");
            }
            _ => {} // other unrelated messages are ignored
        },
    }

    Ok(())
}

/// C.1b: cancel for an operation that no worker is running returns
/// Ok(()) — idempotent. Mutation: change the `Ok(())` in the
/// no-target branch of `cancel_operation_internal` to an
/// `Err(make_input_err!())` — this test must red-fail.
#[nativelink_test]
async fn cancel_operation_internal_unknown_op_is_ok() -> Result<(), Error> {
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
        None,
        None,
        None,
    );

    // No workers added, no actions submitted; arbitrary op_id.
    let op_id = OperationId::from("unknown-op-id");
    let result = scheduler.cancel_operation(&op_id).await;
    result.expect(
        "cancel_operation MUST return Ok for unknown op (idempotent) — got Err",
    );
    Ok(())
}
