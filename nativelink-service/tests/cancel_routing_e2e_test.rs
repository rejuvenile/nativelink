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

//! AC-poisoning fix — production-seam integration test.
//!
//! Composes the real `ExecutionServer` over a real `SimpleScheduler` over
//! a real `ApiWorkerScheduler` with a real `Worker` whose `tx` is captured
//! via an mpsc channel. Drives the streaming `Execute` RPC, drops the
//! stream, and asserts that the worker actually receives a
//! `KillOperationRequest` with the bare `OperationId::Uuid(uuid)` shape
//! (NOT the Display-formatted "instance/uuid" `OperationId::String(_)`
//! shape). The latter is what the broken code constructed at
//! `execution_server.rs:223` via `OperationId::from(nl_client_operation_id.to_string())`,
//! and the worker's `running_action_infos` HashMap (keyed by Uuid) would
//! miss → silent no-op → no kill delivered → AC poisoning persists.
//!
//! Seams crossed end-to-end:
//!   1. `ExecutionServer::execute` (Tonic streaming entrypoint)
//!   2. `inner_execute` (action_info build + scheduler.add_action)
//!   3. `to_execute_stream` (constructs `ExecuteStreamCancelGuard`)
//!   4. `ExecuteStreamCancelGuard::drop` (background_spawn cancel)
//!   5. `SimpleScheduler::cancel_operation` (ClientStateManager forward)
//!   6. `ApiWorkerScheduler::cancel_operation_internal` (worker scan)
//!   7. `worker.tx.send(UpdateForWorker::KillOperationRequest)` (terminal)
//!
//! Mutation: revert the BUG-1 fix (re-introduce
//! `OperationId::from(nl_client_operation_id.to_string())` at the guard
//! construction site). This test MUST red-fail with the bespoke message
//! "must observe KillOperationRequest at worker".

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use nativelink_config::cas_server::{ExecutionConfig, WithInstanceName};
use nativelink_config::schedulers::SimpleSpec;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, ExecuteRequest, digest_function,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    UpdateForWorker, update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::platform_properties::PlatformProperties;
use nativelink_util::store_trait::StoreLike;
use tokio::sync::{Notify, mpsc};
use tonic::Request;

const INSTANCE_NAME: &str = "instance_name";
const NOW_TIME: u64 = 10_000;

#[nativelink_test]
async fn stream_drop_routes_kill_to_assigned_worker_via_real_apiworkerscheduler()
-> Result<(), Box<dyn core::error::Error>> {
    // ---------------------------------------------------------------
    // 1. Real CAS store with a real Action proto (so inner_execute can
    //    decode action_digest → Action and build action_info).
    // ---------------------------------------------------------------
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    let cas_store = store_manager
        .get_store("cas")
        .expect("cas store registered");

    // Upload Command, Directory (input root), and Action protos via the
    // CAS store. The execute() handler decodes Action by digest.
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

    // ---------------------------------------------------------------
    // 2. Real SimpleScheduler over real ApiWorkerScheduler.
    // ---------------------------------------------------------------
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
    let scheduler_dyn: Arc<dyn KnownPlatformPropertyProvider> = scheduler.clone();

    // ---------------------------------------------------------------
    // 3. Real Worker whose tx is captured by the test. The worker's
    //    `running_action_infos` HashMap is what the cancel-routing
    //    code path scans; the bug is that the wrong OperationId shape
    //    misses the lookup.
    // ---------------------------------------------------------------
    let worker_id = WorkerId("worker_a".to_string());
    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
    let worker = Worker::new(
        worker_id.clone(),
        PlatformProperties::default(),
        worker_tx,
        NOW_TIME,
        0,
    );
    scheduler.add_worker(worker).await?;
    tokio::task::yield_now().await;
    // Drain the initial ConnectionResult message so subsequent recvs
    // see only StartAction / KillOperationRequest.
    let _connection = worker_rx.recv().await.expect("expected ConnectionResult");

    // ---------------------------------------------------------------
    // 4. Build ExecutionServer over the real scheduler + real cas_store.
    // ---------------------------------------------------------------
    let mut scheduler_map: HashMap<String, Arc<dyn KnownPlatformPropertyProvider>> = HashMap::new();
    scheduler_map.insert("main_scheduler".to_string(), scheduler_dyn);
    let execution_server = ExecutionServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE_NAME.to_string(),
            config: ExecutionConfig {
                cas_store: "cas".to_string(),
                scheduler: "main_scheduler".to_string(),
                portable_incr: Default::default(),
            },
        }],
        &scheduler_map,
        &store_manager,
    )?;

    // ---------------------------------------------------------------
    // 5. Drive the production streaming Execute path. We invoke
    //    `execute()` then drop the resulting Tonic stream on the
    //    next yield. The guard's Drop must fire and cancel must
    //    route through the entire chain to the worker rx.
    // ---------------------------------------------------------------
    let response = execution_server
        .execute(Request::new(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: digest_function::Value::Sha256.into(),
            skip_cache_lookup: true,
            action_digest: Some(action_digest.into()),
            execution_policy: None,
            results_cache_policy: None,
        }))
        .await
        .expect("execute() must succeed");

    // Wait for the matching engine to dispatch the action to the
    // worker. The worker's `running_action_infos` MUST be populated
    // before the cancel can find it; otherwise the test would have
    // to disambiguate "stream-drop didn't fire" from "match didn't
    // happen yet" — those are different bugs.
    let start_action_msg = tokio::time::timeout(Duration::from_secs(5), worker_rx.recv())
        .await
        .expect("worker must receive StartAction within 5s — matching engine wedged")
        .expect("worker_rx closed unexpectedly");
    let dispatched_op_id = match start_action_msg.update {
        Some(update_for_worker::Update::StartAction(start)) => {
            // The matching engine populates the worker's
            // running_action_infos with `OperationId::Uuid(uuid)`. The
            // start.operation_id wire form re-parses through
            // `OperationId::from(String)` which roundtrips Uuid
            // correctly — the cancel-side bug was that
            // `to_execute_stream` rebuilt from the Display-formatted
            // composite "instance/uuid" string, NOT from the bare
            // operation_id.
            OperationId::from(start.operation_id)
        }
        other => panic!("expected StartAction first, got {other:?}"),
    };

    // Now DROP the response stream. This is what Tonic does when the
    // Bazel client RST_STREAMs / TCP-closes / drops on cancel.
    drop(response);

    // ---------------------------------------------------------------
    // 6. Assert: worker MUST receive KillOperationRequest within 5s.
    //    Bespoke message names the seam: BUG-1 (OperationId shape
    //    mismatch in to_execute_stream's guard construction) is the
    //    only known mechanism that makes this fail.
    // ---------------------------------------------------------------
    let kill_msg = tokio::time::timeout(Duration::from_secs(5), worker_rx.recv())
        .await
        .expect(
            "must observe KillOperationRequest at worker — \
             OperationId shape mismatch on stream-drop path; see red-team R2",
        )
        .expect("worker_rx closed before KillOperationRequest delivered");

    match kill_msg.update {
        Some(update_for_worker::Update::KillOperationRequest(req)) => {
            // The kill MUST carry the same OperationId the worker is
            // tracking (the bare uuid form). The bug shipped a
            // Display-formatted "instance/uuid" string here, which
            // round-tripped through OperationId::from(String) into
            // OperationId::String("instance/uuid"), missing the
            // worker's HashMap keyed by OperationId::Uuid(uuid).
            assert_eq!(
                req.operation_id,
                dispatched_op_id.to_string(),
                "kill operation_id must match dispatched operation_id (bare uuid form); \
                 BUG-1 regression: guard re-routed via Display+From and produced wrong shape"
            );
        }
        other => panic!(
            "expected KillOperationRequest after stream drop, got {other:?} — \
             stream-drop guard either fired wrong message or skipped cancel routing"
        ),
    }

    Ok(())
}
