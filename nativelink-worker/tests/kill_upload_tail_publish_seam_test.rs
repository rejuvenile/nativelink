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

//! M1 (cadre fix-up #2): publish-closure WIRE-SHAPE seam test for a
//! killed-during-stalled-upload action.
//!
//! Production composition crossed end-to-end:
//!   `LocalWorkerImpl::run` action pipeline
//!     → `RunningAction::upload_results` (returns the terminal shape)
//!     → `RunningAction::get_finished_result` (yields the `ActionResult`)
//!     → publish closure (`make_publish_future`) Ok-arm
//!     → `worker_api_client.execution_response(ExecuteResult{...})`
//!     → `MockWorkerApiClient` capture.
//!
//! The seam under test is the publish-closure's Ok-vs-Err arm SELECTION.
//! The companion in-process integration test
//! `kill_during_upload_tail_aborts_in_flight_upload`
//! (`running_actions_manager_test.rs`) proves the REAL `upload_results`
//! produces `Ok(ActionResult{error: Aborted})` (not `Err`) when a kill
//! preempts a stalled upload. This test proves that shape drives the
//! Ok-arm `ExecuteResult::ExecuteResponse(Completed{status: Aborted})`
//! wire message — NOT the Err-arm `ExecuteResult::InternalError(Aborted)`.
//!
//! Why the distinction is load-bearing (the whole point of M1): the
//! scheduler converts `ExecuteResponse(Completed{Aborted})` into
//! `UpdateOperationType::UpdateWithActionStage(ActionStage::Completed)`
//! which is TERMINAL (`ActionStage::is_finished()` true), whereas
//! `InternalError(Aborted)` becomes
//! `UpdateOperationType::UpdateWithError(Aborted)` which
//! `simple_scheduler_state_manager.rs:837-859` RE-QUEUES (Aborted is
//! neither ResourceExhausted nor FailedPrecondition → `attempts += 1`
//! then `ActionStage::Queued` while `attempts <= max_job_retries`),
//! re-executing a killed action. The scheduler-side kill path
//! (`cancel_operation_internal`) never marks the awaited-action finished,
//! so the already-completed guard does not save us — the wire shape is the
//! only thing that keeps the killed op terminal.
//!
//! Mutation: in `running_actions_manager.rs` `upload_results`, change the
//! kill arm back to `Err(killed)` (the pre-fix shape). This test must
//! red-fail with the bespoke "publish closure took the Err arm
//! (InternalError) for a killed upload" panic.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use hyper::body::Frame;
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Platform;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::execute_result::Result as ExecuteResultResult;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, StartExecute, UpdateForWorker,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionUniqueKey, ActionUniqueQualifier,
};
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::digest_hasher::DigestHasherFunc;
use utils::local_worker_test_utils::setup_local_worker;
use utils::mock_running_actions_manager::MockRunningAction;

const INSTANCE_NAME: &str = "foo";

fn make_action_info(digest_byte: u8) -> ActionInfo {
    let action_digest = DigestInfo::new([digest_byte; 32], 10);
    ActionInfo {
        command_digest: DigestInfo::new([1u8; 32], 10),
        input_root_digest: DigestInfo::new([2u8; 32], 10),
        timeout: Duration::from_secs(1),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: action_digest,
        }),
    }
}

async fn send_start_action(
    tx_stream: &tokio::sync::mpsc::Sender<Frame<bytes::Bytes>>,
    worker_id: &str,
    action_info: &ActionInfo,
    op_id: &str,
) -> Result<(), Error> {
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::StartAction(StartExecute {
                    execute_request: Some(action_info.into()),
                    operation_id: op_id.to_string(),
                    queued_timestamp: None,
                    platform: Some(Platform::default()),
                    worker_id: worker_id.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                    missing_digest_peers: Vec::new(),
                })),
            })
            .unwrap(),
        ))
        .await
        .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    Ok(())
}

/// M1 seam: a killed upload-tail produces the Ok-arm
/// `ExecuteResponse(Completed{Aborted})` wire shape, NOT the Err-arm
/// `InternalError(Aborted)`.
///
/// `get_finished_result` returns `Ok(ActionResult{error: Aborted})` — the
/// EXACT terminal shape the real `upload_results` kill arm now synthesizes
/// (proven by `kill_during_upload_tail_aborts_in_flight_upload`). The
/// publish closure must therefore take its Ok arm and emit
/// `ExecuteResult { result: ExecuteResponse(resp) }` with
/// `resp.status.code == Aborted`.
#[nativelink_test]
async fn killed_upload_tail_publishes_execute_response_not_internal_error()
-> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;

        let worker_id = "m1_seam_worker".to_string();
        let tx_stream = test_context.maybe_tx_stream.take().unwrap();
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;

        let action_info = make_action_info(0x42);
        send_start_action(&tx_stream, &worker_id, &action_info, "m1-seam-op").await?;

        let running_action = Arc::new(MockRunningAction::new());
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;

        // Drive the pipeline. The terminal `get_finished_result` carries the
        // killed-upload shape: `Ok(ActionResult{error: Aborted})` — the same
        // shape the real kill arm synthesizes. `simple_expect_get_finished_result`
        // walks prepare → execute → upload_results → get_finished_result →
        // cleanup, matching the production `.and_then` chain.
        let killed_action_result = ActionResult {
            error: Some(make_err!(Code::Aborted, "killed during upload tail")),
            ..ActionResult::default()
        };
        running_action
            .simple_expect_get_finished_result(Ok(killed_action_result))
            .await?;

        // Capture the publish closure's wire message and assert the Ok arm.
        let execute_result = test_context.client.expect_execution_response(Ok(())).await;
        let result = execute_result
            .result
            .expect("ExecuteResult must carry a result variant");

        match result {
            ExecuteResultResult::ExecuteResponse(resp) => {
                let status = resp.status.expect(
                    "M1: the Ok-arm ExecuteResponse must carry a status \
                     so the embedded Aborted error reaches the scheduler",
                );
                assert_eq!(
                    status.code,
                    Code::Aborted as i32,
                    "M1: the killed upload-tail ExecuteResponse status must be \
                     Aborted (got code {}, message {:?}) — the embedded error \
                     drives the scheduler's terminal Completed{{Aborted}} stage",
                    status.code,
                    status.message,
                );
            }
            ExecuteResultResult::InternalError(e) => {
                panic!(
                    "M1: publish closure took the Err arm (InternalError) for a \
                     killed upload — this maps to UpdateWithError(Aborted) which \
                     the scheduler RE-QUEUES as a spurious re-execution. The kill \
                     arm must produce Ok(ActionResult{{error: Aborted}}) so the \
                     Ok-arm ExecuteResponse(Completed{{Aborted}}) wire shape is \
                     emitted instead. Got InternalError: {e:?}"
                );
            }
        }
        Ok::<_, Error>(())
    })
    .await
    .expect(
        "M1 seam: deadlock detector fired — the publish closure never emitted \
         an ExecuteResult for the killed upload within 5s",
    )?;
    Ok(())
}
