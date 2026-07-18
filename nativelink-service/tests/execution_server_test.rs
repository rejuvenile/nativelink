// Copyright 2025 The NativeLink Authors. All rights reserved.
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

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::stream;
use nativelink_config::cas_server::{ExecutionConfig, PortableIncrConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
use nativelink_proto::build::bazel::remote::execution::v2::{ExecuteRequest, digest_function};
use nativelink_proto::google::longrunning::operations_server::Operations;
use nativelink_proto::google::longrunning::{
    CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest,
    WaitOperationRequest,
};
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::mock_scheduler::MockActionScheduler;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{ActionStateResult, ActionStateResultStream};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::targetkey::TargetKey;
use tonic::Request;

const INSTANCE_NAME: &str = "instance_name";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_execution_server(
    store_manager: &StoreManager,
) -> Result<(ExecutionServer, Arc<MockActionScheduler>), Error> {
    make_execution_server_with(store_manager, PortableIncrConfig::default())
}

fn make_execution_server_with(
    store_manager: &StoreManager,
    portable_incr: PortableIncrConfig,
) -> Result<(ExecutionServer, Arc<MockActionScheduler>), Error> {
    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let mut action_schedulers: HashMap<String, Arc<dyn KnownPlatformPropertyProvider>> =
        HashMap::new();
    action_schedulers.insert("main_scheduler".to_string(), mock_scheduler.clone());
    let server = ExecutionServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: ExecutionConfig {
                cas_store: "main_cas".to_string(),
                scheduler: "main_scheduler".to_string(),
                portable_incr,
            },
        }],
        &action_schedulers,
        store_manager,
    )?;
    Ok((server, mock_scheduler))
}

#[nativelink_test]
async fn instance_name_fail() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, _) = make_execution_server(&store_manager)?;

    let raw_response = execution_server
        .execute(Request::new(ExecuteRequest {
            instance_name: "foo".to_string(),
            digest_function: digest_function::Value::Sha256.into(),
            skip_cache_lookup: false,
            action_digest: None,
            execution_policy: None,
            results_cache_policy: None,
        }))
        .await;

    match raw_response {
        Err(response_err) => {
            assert_eq!(
                response_err.message(),
                "'instance_name' not configured for 'foo' : Failed on execute() command"
            );
        }
        Ok(_) => {
            panic!("Not expecting ok!");
        }
    }
    Ok(())
}

#[nativelink_test]
async fn operations_list_operations_unimplemented() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, _) = make_execution_server(&store_manager)?;

    let err = execution_server
        .list_operations(Request::new(ListOperationsRequest::default()))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unimplemented);
    assert_eq!(err.message(), "list_operations not implemented");
    Ok(())
}

#[nativelink_test]
async fn operations_delete_operation_unimplemented() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, _) = make_execution_server(&store_manager)?;

    let err = execution_server
        .delete_operation(Request::new(DeleteOperationRequest::default()))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unimplemented);
    assert_eq!(err.message(), "delete_operation not implemented");
    Ok(())
}

/// AC-poisoning fix C.2 (RPC path): the explicit `cancel_operation`
/// RPC now routes via `ClientStateManager::cancel_operation` (was
/// `Status::unimplemented`). Mutation: revert the
/// `cancel_operation` body to `Err(Status::unimplemented(...))` —
/// this test must red-fail because the mock's
/// `expect_cancel_operation` will never receive its call.
#[nativelink_test]
async fn operations_cancel_operation_routes_to_scheduler()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");
    let request_fut = execution_server
        .cancel_operation(Request::new(CancelOperationRequest {
            name: operation_name.clone(),
        }));

    let (request_res, observed_op_id) = tokio::join!(
        request_fut,
        mock_scheduler.expect_cancel_operation(Ok(())),
    );

    request_res
        .expect("cancel_operation MUST succeed when routed to scheduler — got Err");
    assert_eq!(
        observed_op_id,
        OperationId::from("some_operation_id"),
        "cancel_operation must forward the parsed operation_id to the scheduler"
    );
    Ok(())
}

/// AC-poisoning fix C.2 (RPC path): unknown instance returns NotFound,
/// not Unimplemented. Validates routing-side error mapping.
#[nativelink_test]
async fn operations_cancel_operation_unknown_instance_returns_not_found()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, _) = make_execution_server(&store_manager)?;

    let err = execution_server
        .cancel_operation(Request::new(CancelOperationRequest {
            name: "nonexistent_instance/some_op".to_string(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::NotFound);
    Ok(())
}

struct MockActionStateResult {
    states: Vec<Arc<ActionState>>,
}

#[async_trait]
impl ActionStateResult for MockActionStateResult {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        Ok((self.states.first().unwrap().clone(), None))
    }

    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        if self.states.is_empty() {
            return Err(make_err!(Code::Internal, "No more states"));
        }
        let state = self.states.remove(0);
        Ok((state, None))
    }

    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "as_action_info not implemented"
        ))
    }
}

#[nativelink_test]
async fn operations_get_operation() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    let action_state = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });

    let mock_action_state_result = MockActionStateResult {
        states: vec![action_state.clone()],
    };

    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.get_operation(Request::new(GetOperationRequest {
        name: operation_name.clone(),
    }));

    let (request_res, filter) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );

    assert_eq!(
        filter.client_operation_id,
        Some(OperationId::from("some_operation_id"))
    );

    let operation = request_res?.into_inner();
    assert_eq!(operation.name, operation_name);
    assert!(!operation.done);

    Ok(())
}

#[nativelink_test]
async fn operations_get_operation_not_found() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    let stream: ActionStateResultStream = Box::pin(stream::empty());

    let request_fut = execution_server.get_operation(Request::new(GetOperationRequest {
        name: operation_name.clone(),
    }));

    let (request_res, filter) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );

    assert_eq!(
        filter.client_operation_id,
        Some(OperationId::from("some_operation_id"))
    );

    let err = request_res.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(err.message(), "Failed to find existing task");

    Ok(())
}

#[nativelink_test]
async fn operations_wait_operation_finishes() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    let state1 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });

    let state2 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Completed(ActionResult::default()),
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });

    let mock_action_state_result = MockActionStateResult {
        states: vec![state1.clone(), state2.clone()],
    };

    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.wait_operation(Request::new(WaitOperationRequest {
        name: operation_name.clone(),
        timeout: None,
    }));

    let (request_res, _) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );

    let operation = request_res?.into_inner();
    assert_eq!(operation.name, operation_name);
    assert!(operation.done);

    Ok(())
}

struct TimeoutActionStateResult {
    state: Arc<ActionState>,
    first_called: bool,
}

#[async_trait]
impl ActionStateResult for TimeoutActionStateResult {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        Ok((self.state.clone(), None))
    }

    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        if !self.first_called {
            self.first_called = true;
            return Ok((self.state.clone(), None));
        }
        tokio::time::sleep(core::time::Duration::from_secs(1)).await;
        Ok((self.state.clone(), None))
    }

    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "as_action_info not implemented"
        ))
    }
}

/// AC-poisoning fix C.2: stream-drop on the streaming `Execute` RPC
/// fires `cancel_operation` on the wrapping scheduler. After BUG-2
/// hoist (the `cancel_guard` is installed ONLY by `inner_execute` —
/// see `cancel_routing_e2e_test`), the only path that should
/// cancel-on-drop is the streaming Execute. `inner_wait_execution`
/// (used by `get_operation`/`wait_operation`/`WaitExecution`) is
/// guard-free.
///
/// This test drives `execute()` end-to-end through the mock
/// scheduler, reads the first state, drops the response stream,
/// and asserts `cancel_operation` was forwarded. The BARE
/// OperationId form (BUG-1 fix) is asserted via
/// `cancel_routing_e2e_test`'s production-seam test (real
/// SimpleScheduler + real Worker map), which observes the kill at
/// the worker rx; this test asserts the guard fires AT ALL with the
/// MockActionScheduler.
///
/// Mutation: comment out the `background_spawn!` in
/// `ExecuteStreamCancelGuard::drop` — this test must red-fail with
/// "must observe ExecuteStreamCancelGuard::drop fire".
#[nativelink_test]
async fn stream_drop_calls_cancel_operation() -> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Action, Command, Directory, ExecuteRequest, digest_function,
    };
    use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
    use nativelink_store::ac_utils::serialize_and_upload_message;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::store_trait::StoreLike;

    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    // execute() requires a real Action proto in the CAS store
    // (decoded via get_and_decode_digest). Upload Command, Directory,
    // and Action.
    let cas_store = store_manager
        .get_store("main_cas")
        .expect("main_cas registered");
    let command_digest = serialize_and_upload_message(
        &Command {
            arguments: vec!["true".to_string()],
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![],
            ..Default::default()
        },
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;
    let input_root_digest = serialize_and_upload_message(
        &Directory::default(),
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(input_root_digest.into()),
        ..Default::default()
    };
    let action_digest = serialize_and_upload_message(
        &action,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    // Single Queued state — execute reads it, then is_finished=false
    // means more is expected. Dropping the response triggers the
    // guard.
    let action_state = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });
    let mock_action_state_result = MockActionStateResult {
        states: vec![action_state.clone()],
    };

    // Drive execute(). Mock will receive add_action and return the
    // action_state_result; we then drop the response stream.
    let request_fut = execution_server.execute(Request::new(ExecuteRequest {
        instance_name: INSTANCE_NAME.to_string(),
        digest_function: digest_function::Value::Sha256.into(),
        skip_cache_lookup: true,
        action_digest: Some(action_digest.into()),
        execution_policy: None,
        results_cache_policy: None,
    }));

    let (response, _add_action_call) = tokio::join!(
        request_fut,
        mock_scheduler.expect_add_action(Ok(Box::new(mock_action_state_result))),
    );
    let response = response.expect("execute must succeed");

    // Drop the response stream — this triggers the guard's Drop
    // (with `completed=false` because the Queued state is not
    // finished).
    drop(response);

    // The guard's Drop spawns the cancel via background_spawn!. Wait
    // for the mock to receive it within the deadlock-detector timeout.
    let _observed_op_id = tokio::time::timeout(
        Duration::from_secs(5),
        mock_scheduler.expect_cancel_operation(Ok(())),
    )
    .await
    .expect(
        "must observe ExecuteStreamCancelGuard::drop fire — Tonic stream-drop not propagating",
    );
    // The exact OperationId shape forwarded to cancel is asserted
    // end-to-end by `cancel_routing_e2e_test`; here we only assert the
    // guard fires.
    Ok(())
}

/// C.2 (over-action positive control): wait_operation that drives
/// the stream to a finished state must NOT trigger the cancel guard.
/// `completed=true` short-circuits Drop. Mutation: remove the
/// `completed.store(true, Ordering::Release)` write at the
/// `is_finished` branch in `to_execute_stream` — this test must
/// red-fail because cancel_operation will be observed on the mock.
#[nativelink_test]
async fn stream_natural_completion_does_not_call_cancel_operation()
-> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    // Two states; the second is Completed → is_finished()=true →
    // completed.store(true) → guard's Drop is a no-op.
    let state1 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });
    let state2 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Completed(ActionResult::default()),
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });
    let mock_action_state_result = MockActionStateResult {
        states: vec![state1.clone(), state2.clone()],
    };
    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.wait_operation(Request::new(WaitOperationRequest {
        name: operation_name.clone(),
        timeout: None,
    }));

    let (request_res, _) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );
    let operation = request_res?.into_inner();
    assert!(operation.done, "operation must be done");

    // The mock should NOT receive a cancel_operation call. Use a
    // short timeout — if cancel WAS called, the mock would have
    // received it by now (the background_spawn fires immediately
    // on Drop).
    let cancel_observed = tokio::time::timeout(
        Duration::from_millis(200),
        mock_scheduler.expect_cancel_operation(Ok(())),
    )
    .await;
    assert!(
        cancel_observed.is_err(),
        "must complete normally — cancel guard fired against non-cancelled action; \
         expected NO cancel_operation call when stream completes naturally, but got one"
    );

    Ok(())
}

#[nativelink_test]
async fn operations_wait_operation_timeout() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    let state1 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });

    let mock_action_state_result = TimeoutActionStateResult {
        state: state1.clone(),
        first_called: false,
    };

    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.wait_operation(Request::new(WaitOperationRequest {
        name: operation_name.clone(),
        timeout: Some(prost_types::Duration {
            seconds: 0,
            nanos: 10_000_000, // 10ms
        }),
    }));

    let (request_res, _) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );

    let operation = request_res?.into_inner();
    assert_eq!(operation.name, operation_name);
    assert!(!operation.done);

    Ok(())
}

/// AC-poisoning fix BUG-2 over-action positive control: status-poll RPCs
/// (`get_operation`) MUST NOT trigger the cancel guard when the
/// underlying action is still running. `get_operation` reads ONE state
/// from the stream and returns; dropping the stream is its NORMAL exit
/// path, NOT a cancel signal. If the guard fires here, every Bazel
/// status poll cancels the action it is polling.
///
/// Mutation: revert the BUG-2 hoist (re-add `ExecuteStreamCancelGuard`
/// to `to_execute_stream` so `get_operation`/`wait_operation`/`WaitExecution`
/// callers all get the guard). This test MUST red-fail with the
/// bespoke message because cancel WILL be observed on the mock.
#[nativelink_test]
async fn get_operation_unfinished_does_not_cancel() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    // Single Queued state — action is still running. get_operation
    // reads it, returns. Dropping the stream MUST NOT cancel.
    let action_state = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });
    let mock_action_state_result = MockActionStateResult {
        states: vec![action_state.clone()],
    };
    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.get_operation(Request::new(GetOperationRequest {
        name: operation_name.clone(),
    }));

    let (request_res, _filter) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );
    let operation = request_res?.into_inner();
    assert_eq!(operation.name, operation_name);
    assert!(!operation.done, "Queued action is not done");

    // After get_operation returns, the stream is dropped. If the
    // guard fired (BUG-2), the mock receives a cancel within the
    // background_spawn timing window. Wait long enough that a
    // legitimately-fired cancel would have arrived; assert mock did
    // NOT see one.
    let cancel_observed = tokio::time::timeout(
        Duration::from_millis(200),
        mock_scheduler.expect_cancel_operation(Ok(())),
    )
    .await;
    assert!(
        cancel_observed.is_err(),
        "must NOT cancel on get_operation poll — \
         guard fired despite unary RPC; see distributed-systems R2 BLOCK"
    );

    Ok(())
}

/// AC-poisoning fix BUG-2 over-action positive control: `wait_operation`
/// with a short client timeout that elapses before `is_done` becomes
/// true MUST NOT trigger the cancel guard. Bazel `wait_operation` polls
/// in a loop with client-supplied timeouts; expiring the timeout is
/// natural return, not cancel.
///
/// Mutation: revert the BUG-2 hoist. This test MUST red-fail because
/// cancel WILL be observed on the mock.
#[nativelink_test]
async fn wait_operation_timeout_does_not_cancel() -> Result<(), Box<dyn core::error::Error>> {
    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) = make_execution_server(&store_manager)?;

    let operation_name = format!("{INSTANCE_NAME}/some_operation_id");

    let state1 = Arc::new(ActionState {
        client_operation_id: OperationId::from("some_operation_id"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });

    // TimeoutActionStateResult yields the same Queued state on the
    // first call, then sleeps 1s on subsequent calls — wait_operation
    // with 10ms client-timeout will hit the timeout branch and return
    // an unfinished operation. Dropping the stream MUST NOT cancel.
    let mock_action_state_result = TimeoutActionStateResult {
        state: state1.clone(),
        first_called: false,
    };
    let stream: ActionStateResultStream = Box::pin(stream::once(async move {
        let result: Box<dyn ActionStateResult> = Box::new(mock_action_state_result);
        result
    }));

    let request_fut = execution_server.wait_operation(Request::new(WaitOperationRequest {
        name: operation_name.clone(),
        timeout: Some(prost_types::Duration {
            seconds: 0,
            nanos: 10_000_000, // 10ms
        }),
    }));

    let (request_res, _) = tokio::join!(
        request_fut,
        mock_scheduler.expect_filter_operations(Ok(stream)),
    );
    let operation = request_res?.into_inner();
    assert_eq!(operation.name, operation_name);
    assert!(!operation.done, "must time out before is_done");

    let cancel_observed = tokio::time::timeout(
        Duration::from_millis(200),
        mock_scheduler.expect_cancel_operation(Ok(())),
    )
    .await;
    assert!(
        cancel_observed.is_err(),
        "must NOT cancel on wait_operation timeout — \
         guard fires on legitimate poll-then-drop"
    );

    Ok(())
}

/// A distinctive Goma-path marker placed on the uploaded `Command.platform`. It
/// lands in the resulting `ActionInfo.platform_properties` ONLY if ingestion
/// fetched+merged the Command (the Goma path, taken only when the Action carries
/// no inline platform properties). Its ABSENCE is how the carrier-path tests
/// prove the portable_incr path performed ZERO `Command` fetch.
const GOMA_PROBE_PROPERTY: &str = "nl_goma_probe";
const GOMA_PROBE_VALUE: &str = "from_command";

/// The exact primary-output the shared cross-repo KAT hashes (config-stripped as
/// the FL build emits it), and its byte-verified blake3 key. The Bazel client
/// attaches these two as the `nl_incr_targetkey` / `nl_incr_primary_output`
/// Action Platform carrier for allowlisted rustc actions.
const KAT_PRIMARY_OUTPUT: &str = "bazel-out/darwin_arm64-fastbuild/bin/pkg/libfoo.rlib";
const KAT_KEY: &str = "a17b9c22c639c19f2951ad4d0cb28df41dbd33113bbe7ead51ac0dd577998567";

/// FL-1383 (§10) ingestion-threading shared harness: uploads a Command (bearing
/// the Goma probe marker on its platform) plus an Action whose
/// `platform.properties` carry `action_platform_properties` (the client carrier
/// lives here), drives `execute()` through the mock scheduler, and returns the
/// `ActionInfo` the scheduler received — so a caller can inspect `targetkey` and
/// (via the probe marker) whether the Command was fetched.
async fn drive_execute_and_capture_action_info(
    action_platform_properties: Vec<(String, String)>,
    portable_incr: PortableIncrConfig,
) -> Result<ActionInfo, Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
    use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Action, Command, Directory, Platform,
    };
    use nativelink_store::ac_utils::serialize_and_upload_message;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::store_trait::StoreLike;

    let store_manager = make_store_manager().await?;
    let (execution_server, mock_scheduler) =
        make_execution_server_with(&store_manager, portable_incr)?;

    let cas_store = store_manager
        .get_store("main_cas")
        .expect("main_cas registered");
    // The Command carries the Goma probe on its platform. If ingestion fetches +
    // merges the Command, the probe leaks into ActionInfo.platform_properties;
    // if not (the carrier path), it does not — the ZERO-fetch discriminator.
    let command_digest = serialize_and_upload_message(
        &Command {
            arguments: vec!["true".to_string()],
            working_directory: ".".to_string(),
            platform: Some(Platform {
                properties: vec![Property {
                    name: GOMA_PROBE_PROPERTY.to_string(),
                    value: GOMA_PROBE_VALUE.to_string(),
                }],
            }),
            ..Default::default()
        },
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;
    let input_root_digest = serialize_and_upload_message(
        &Directory::default(),
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;
    let platform = if action_platform_properties.is_empty() {
        None
    } else {
        Some(Platform {
            properties: action_platform_properties
                .into_iter()
                .map(|(name, value)| Property { name, value })
                .collect(),
        })
    };
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(input_root_digest.into()),
        platform,
        ..Default::default()
    };
    let action_digest = serialize_and_upload_message(
        &action,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    let action_state = Arc::new(ActionState {
        client_operation_id: OperationId::from("targetkey_probe_op"),
        stage: ActionStage::Queued,
        action_digest: DigestInfo::new([0u8; 32], 0),
        last_transition_timestamp: SystemTime::UNIX_EPOCH,
    });
    let mock_action_state_result = MockActionStateResult {
        states: vec![action_state],
    };

    let request_fut = execution_server.execute(Request::new(ExecuteRequest {
        instance_name: INSTANCE_NAME.to_string(),
        digest_function: digest_function::Value::Sha256.into(),
        skip_cache_lookup: true,
        action_digest: Some(action_digest.into()),
        execution_policy: None,
        results_cache_policy: None,
    }));

    let (response, add_action_call) = tokio::join!(
        request_fut,
        mock_scheduler.expect_add_action(Ok(Box::new(mock_action_state_result))),
    );
    // Drop the response stream (Queued is not finished; nothing else to drive).
    drop(response.expect("execute must succeed"));

    let (_operation_id, action_info) = add_action_call;
    Ok(action_info)
}

/// Build the client carrier as it lands on `Action.platform.properties`:
/// `nl_incr_targetkey` = `key`, `nl_incr_primary_output` = `primary_output`.
fn carrier(key: &str, primary_output: &str) -> Vec<(String, String)> {
    vec![
        ("nl_incr_targetkey".to_string(), key.to_string()),
        (
            "nl_incr_primary_output".to_string(),
            primary_output.to_string(),
        ),
    ]
}

/// FL-1383 (§10) BLOCK fix: when the feature is ENABLED and the client carrier
/// is present + allowlisted, the `targetkey` is READ from the Action Platform
/// carrier (NOT derived) and threaded through `ActionInfo` — and the portable_
/// incr path performs ZERO `Command` fetch (the Goma probe on the Command MUST
/// NOT appear in `platform_properties`, proving no fetch+merge occurred).
/// Mutation: comment out the `targetkey = TargetKey::from_carrier(...)`
/// assignment in `execution_server::build_action_info` — the targetkey assertion
/// must red-fail.
#[nativelink_test]
async fn targetkey_from_carrier_populated_and_zero_command_fetch()
-> Result<(), Box<dyn core::error::Error>> {
    let portable_incr = PortableIncrConfig {
        enabled: true,
        action_output_allowlist: vec!["bazel-out/".to_string()],
    };

    let action_info =
        drive_execute_and_capture_action_info(carrier(KAT_KEY, KAT_PRIMARY_OUTPUT), portable_incr)
            .await?;

    assert_eq!(
        action_info.targetkey.as_ref().map(TargetKey::key),
        Some(KAT_KEY),
        "targetkey must be read from the carrier when enabled + allowlisted"
    );
    assert_eq!(
        action_info.targetkey.as_ref().map(TargetKey::primary_output),
        Some(KAT_PRIMARY_OUTPUT),
        "primary output must be the carrier-supplied string"
    );
    // ZERO Command fetch: the Goma probe embedded on the (present-but-unfetched)
    // Command must NOT have leaked into platform_properties.
    assert!(
        !action_info
            .platform_properties
            .contains_key(GOMA_PROBE_PROPERTY),
        "portable_incr path must NOT fetch the Command — the Goma probe leaked, so a Command fetch+merge occurred"
    );
    Ok(())
}

/// FL-1383 inert-by-default: with the feature DISABLED (the fleet default), the
/// carrier is not read even when present — `ActionInfo.targetkey` stays `None`.
/// Mutation: force `enabled = true` here — the assertion must red-fail, proving
/// the disabled flag is what keeps it inert.
#[nativelink_test]
async fn targetkey_absent_when_feature_disabled() -> Result<(), Box<dyn core::error::Error>> {
    let action_info = drive_execute_and_capture_action_info(
        carrier(KAT_KEY, KAT_PRIMARY_OUTPUT),
        PortableIncrConfig::default(),
    )
    .await?;

    assert_eq!(
        action_info.targetkey, None,
        "targetkey must be absent when the portable_incr feature is disabled (inert default)"
    );
    Ok(())
}

/// FL-1383 defense-in-depth (§13): the client only attaches the carrier for
/// allowlisted actions, but the server RE-CHECKS the carrier-supplied primary
/// output server-side. With the feature ENABLED, a carrier present, but the
/// primary output NOT matching any allowlist prefix, no `targetkey` is threaded.
/// Mutation: make the allowlist match (e.g. `vec!["bazel-out/".into()]`) — the
/// assertion must red-fail.
#[nativelink_test]
async fn targetkey_absent_when_enabled_but_not_allowlisted()
-> Result<(), Box<dyn core::error::Error>> {
    let portable_incr = PortableIncrConfig {
        enabled: true,
        // Prefix that does NOT match KAT_PRIMARY_OUTPUT.
        action_output_allowlist: vec!["bazel-out/some-other-config/".to_string()],
    };

    let action_info = drive_execute_and_capture_action_info(
        carrier(KAT_KEY, KAT_PRIMARY_OUTPUT),
        portable_incr,
    )
    .await?;

    assert_eq!(
        action_info.targetkey, None,
        "targetkey must stay None when enabled but the carrier primary output is not allowlisted"
    );
    Ok(())
}

/// FL-1383: with the feature ENABLED + allowlist set but NO carrier on the
/// action (a non-rustc / non-allowlisted action the client left un-tagged),
/// no `targetkey` is threaded. A non-carrier platform property is present so the
/// Goma fetch is not forced — the carrier-absence alone yields `None`. Mutation:
/// read a hardcoded key instead of the carrier in `build_action_info` — this
/// must red-fail.
#[nativelink_test]
async fn targetkey_absent_when_carrier_missing() -> Result<(), Box<dyn core::error::Error>> {
    let portable_incr = PortableIncrConfig {
        enabled: true,
        action_output_allowlist: vec!["bazel-out/".to_string()],
    };

    // A benign inline platform property, but NO nl_incr_* carrier.
    let action_info = drive_execute_and_capture_action_info(
        vec![("OSFamily".to_string(), "linux".to_string())],
        portable_incr,
    )
    .await?;

    assert_eq!(
        action_info.targetkey, None,
        "targetkey must be absent when the client carrier is not present on the action"
    );
    Ok(())
}

/// FL-1383 cheap integrity guard (§10): a carrier whose key does NOT equal
/// `blake3(primary_output)` is client/contract drift and must fall back to cold
/// (`None`) — never seed against a wrong key. The recompute is a short-string
/// hash, NO CAS fetch. Mutation: drop the `!= key` guard in
/// `TargetKey::from_carrier` — this must red-fail.
#[nativelink_test]
async fn targetkey_absent_when_carrier_key_mismatched()
-> Result<(), Box<dyn core::error::Error>> {
    let portable_incr = PortableIncrConfig {
        enabled: true,
        action_output_allowlist: vec!["bazel-out/".to_string()],
    };

    // Allowlisted primary output, but a key that is NOT blake3(primary_output).
    let wrong_key = "0".repeat(64);
    let action_info = drive_execute_and_capture_action_info(
        carrier(&wrong_key, KAT_PRIMARY_OUTPUT),
        portable_incr,
    )
    .await?;

    assert_eq!(
        action_info.targetkey, None,
        "targetkey must be absent (cold) when the carrier key does not match blake3(primary_output)"
    );
    Ok(())
}
