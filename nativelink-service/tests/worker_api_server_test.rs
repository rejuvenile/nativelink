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

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_lock::Mutex as AsyncMutex;
use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, ExecuteResponse, ExecutedActionMetadata, LogFile,
    OutputDirectory, OutputFile, OutputSymlink,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    execute_result, update_for_worker, BlobsAvailableNotification, BlobsEvictedNotification,
    ConnectWorkerRequest, ExecuteResult, KeepAliveRequest, MirrorPinEntry, UpdateForScheduler,
};
use nativelink_proto::google::rpc::Status as ProtoStatus;
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::ActionInfoWithProps;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::{
    ConnectWorkerStream, NowFn, WorkerApiMetrics, WorkerApiServer,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, OperationId, WorkerId,
};
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::platform_properties::PlatformProperties;
use pretty_assertions::assert_eq;
use tokio::join;
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;
use nativelink_scheduler::worker_registry::WorkerRegistry;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;

#[derive(Debug)]
enum WorkerStateManagerCalls {
    UpdateOperation((OperationId, WorkerId, UpdateOperationType)),
}

#[derive(Debug)]
enum WorkerStateManagerReturns {
    UpdateOperation(Result<(), Error>),
}

#[derive(MetricsComponent)]
struct MockWorkerStateManager {
    rx_call: Arc<AsyncMutex<mpsc::UnboundedReceiver<WorkerStateManagerCalls>>>,
    tx_call: mpsc::UnboundedSender<WorkerStateManagerCalls>,
    rx_resp: Arc<AsyncMutex<mpsc::UnboundedReceiver<WorkerStateManagerReturns>>>,
    tx_resp: mpsc::UnboundedSender<WorkerStateManagerReturns>,
}

impl MockWorkerStateManager {
    pub(crate) fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Arc::new(AsyncMutex::new(rx_call)),
            tx_call,
            rx_resp: Arc::new(AsyncMutex::new(rx_resp)),
            tx_resp,
        }
    }

    pub(crate) async fn expect_update_operation(
        &self,
        result: Result<(), Error>,
    ) -> (OperationId, WorkerId, UpdateOperationType) {
        let mut rx_call_lock = self.rx_call.lock().await;
        let recv = rx_call_lock.recv();
        let WorkerStateManagerCalls::UpdateOperation(req) =
            recv.await.expect("Could not receive msg in mpsc");
        self.tx_resp
            .send(WorkerStateManagerReturns::UpdateOperation(result))
            .expect("Could not send request to mpsc");
        req
    }
}

#[async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        self.tx_call
            .send(WorkerStateManagerCalls::UpdateOperation((
                operation_id.clone(),
                worker_id.clone(),
                update,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerStateManagerReturns::UpdateOperation(result) => result,
        }
    }
}

struct TestContext {
    scheduler: Arc<ApiWorkerScheduler>,
    state_manager: Arc<MockWorkerStateManager>,
    _worker_api_server: WorkerApiServer,
    connection_worker_stream: ConnectWorkerStream,
    worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "`setup_api_server` requires a method that returns a `Result`"
)]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

async fn setup_api_server(worker_timeout: u64, now_fn: NowFn) -> Result<TestContext, Error> {
    setup_api_server_with_task_limit(worker_timeout, now_fn, 0).await
}

async fn setup_api_server_with_task_limit(
    worker_timeout: u64,
    now_fn: NowFn,
    max_worker_tasks: u64,
) -> Result<TestContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";

    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        worker_timeout,
        worker_registry,
    );

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        now_fn,
        [1u8; 6],
        None,
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        max_inflight_tasks: max_worker_tasks,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let maybe_first_message = connection_worker_stream.next().await;
    assert!(
        maybe_first_message.is_some(),
        "Expected first message from stream"
    );
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field to be populated")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };

    assert_eq!(
        worker_id.len(),
        UUID_SIZE,
        "Worker ID should be 36 characters"
    );

    Ok(TestContext {
        scheduler,
        state_manager,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        worker_id: worker_id.into(),
        worker_stream: tx,
    })
}

#[nativelink_test]
pub async fn connect_worker_adds_worker_to_scheduler_test()
-> Result<(), Box<dyn core::error::Error>> {
    let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

    let worker_exists = test_context
        .scheduler
        .contains_worker_for_test(&test_context.worker_id)
        .await;
    assert!(worker_exists, "Expected worker to exist in worker map");

    Ok(())
}

#[nativelink_test]
pub async fn server_times_out_workers_test() -> Result<(), Box<dyn core::error::Error>> {
    let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

    let mut now_timestamp = BASE_NOW_S;
    {
        // Now change time to 1 second before timeout and ensure the worker is still in the pool.
        now_timestamp += BASE_WORKER_TIMEOUT_S - 1;
        test_context
            .scheduler
            .remove_timedout_workers(now_timestamp)
            .await?;
        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(worker_exists, "Expected worker to exist in worker map");
    }
    {
        // At exactly 1x timeout the worker is quarantined (stops receiving
        // new work) but still exists in the map.
        now_timestamp += 1;
        test_context
            .scheduler
            .remove_timedout_workers(now_timestamp)
            .await?;
        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(
            worker_exists,
            "Expected worker to still exist (quarantined, not yet evicted)"
        );
    }
    {
        // At 2x timeout the worker is fully evicted from the pool.
        now_timestamp += BASE_WORKER_TIMEOUT_S;
        test_context
            .scheduler
            .remove_timedout_workers(now_timestamp)
            .await?;
        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(!worker_exists, "Expected worker to not exist in map");
    }

    Ok(())
}

#[nativelink_test]
pub async fn server_does_not_timeout_if_keep_alive_test() -> Result<(), Box<dyn core::error::Error>>
{
    let now_timestamp = Arc::new(Mutex::new(BASE_NOW_S));
    let now_timestamp_clone = now_timestamp.clone();
    let add_and_return_timestamp = move |add_amount: u64| -> u64 {
        let mut locked_now_timestamp = now_timestamp.lock().unwrap();
        *locked_now_timestamp += add_amount;
        *locked_now_timestamp
    };

    let test_context = setup_api_server(
        BASE_WORKER_TIMEOUT_S,
        Box::new(move || Ok(Duration::from_secs(*now_timestamp_clone.lock().unwrap()))),
    )
    .await?;
    {
        // Now change time to 1 second before timeout and ensure the worker is still in the pool.
        let timestamp = add_and_return_timestamp(BASE_WORKER_TIMEOUT_S - 1);
        test_context
            .scheduler
            .remove_timedout_workers(timestamp)
            .await?;
        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(worker_exists, "Expected worker to exist in worker map");
    }
    {
        // Now send keep alive.
        test_context
            .worker_stream
            .send(Update::KeepAliveRequest(KeepAliveRequest { cpu_load_pct: 0, p_core_load_pct: 0, e_core_load_pct: 0 }))
            .await
            .map_err(|e| make_err!(tonic::Code::Internal, "Error sending keep alive {e}"))?;
        // Wait for a moment to allow it to be processed.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    {
        // Now add 1 second and our worker should still exist in our map.
        let timestamp = add_and_return_timestamp(1);
        test_context
            .scheduler
            .remove_timedout_workers(timestamp)
            .await?;
        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(worker_exists, "Expected worker to exist in map");
    }

    Ok(())
}

#[nativelink_test]
pub async fn worker_receives_keep_alive_request_test() -> Result<(), Box<dyn core::error::Error>> {
    let mut test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

    // Send keep alive to client.
    test_context
        .scheduler
        .send_keep_alive_to_worker_for_test(&test_context.worker_id)
        .await
        .err_tip(|| "Could not send keep alive to worker")?;

    {
        // Read stream and ensure it was a keep alive message.
        let maybe_message = test_context.connection_worker_stream.next().await;
        assert!(
            maybe_message.is_some(),
            "Expected next message in stream to exist"
        );
        let update_message = maybe_message
            .unwrap()
            .err_tip(|| "Expected success result")?
            .update
            .err_tip(|| "Expected update field to be populated")?;
        assert_eq!(
            update_message,
            update_for_worker::Update::KeepAlive(()),
            "Expected KeepAlive message"
        );
    }

    Ok(())
}

#[nativelink_test]
pub async fn going_away_removes_worker_test() -> Result<(), Box<dyn core::error::Error>> {
    let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

    let worker_exists = test_context
        .scheduler
        .contains_worker_for_test(&test_context.worker_id)
        .await;
    assert!(worker_exists, "Expected worker to exist in worker map");

    test_context
        .scheduler
        .remove_worker(&test_context.worker_id)
        .await
        .unwrap();

    let worker_exists = test_context
        .scheduler
        .contains_worker_for_test(&test_context.worker_id)
        .await;
    assert!(
        !worker_exists,
        "Expected worker to be removed from worker map"
    );

    Ok(())
}

fn make_system_time(time: u64) -> SystemTime {
    UNIX_EPOCH.checked_add(Duration::from_secs(time)).unwrap()
}

#[nativelink_test]
pub async fn execution_response_success_test() -> Result<(), Box<dyn core::error::Error>> {
    let mut test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

    let action_digest = DigestInfo::new([7u8; 32], 123);
    let instance_name = "instance_name".to_string();

    let unique_qualifier = ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
        instance_name: instance_name.clone(),
        digest_function: DigestHasherFunc::Sha256,
        digest: action_digest,
    });
    let action_info = Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: make_system_time(0),
        insert_timestamp: make_system_time(0),
        unique_qualifier,
    });
    let expected_operation_id = OperationId::default();

    let platform_properties = test_context
        .scheduler
        .get_platform_property_manager()
        .make_platform_properties(action_info.platform_properties.clone())
        .err_tip(|| "Failed to make platform properties in SimpleScheduler::do_try_match")?;

    test_context
        .scheduler
        .worker_notify_run_action(
            test_context.worker_id.clone(),
            expected_operation_id.clone(),
            ActionInfoWithProps {
                inner: action_info,
                platform_properties,
            },
        )
        .await
        .unwrap();

    let mut server_logs = HashMap::new();
    server_logs.insert(
        "log_name".to_string(),
        LogFile {
            digest: Some(DigestInfo::new([9u8; 32], 124).into()),
            human_readable: false, // We only support non-human readable.
        },
    );
    let execute_response = ExecuteResponse {
        result: Some(ProtoActionResult {
            output_files: vec![OutputFile {
                path: "some path1".to_string(),
                digest: Some(DigestInfo::new([8u8; 32], 124).into()),
                is_executable: true,
                contents: Bytes::default(), // We don't implement this.
                node_properties: None,
            }],
            output_file_symlinks: vec![OutputSymlink {
                path: "some path3".to_string(),
                target: "some target3".to_string(),
                node_properties: None,
            }],
            output_symlinks: vec![OutputSymlink {
                path: "some path3".to_string(),
                target: "some target3".to_string(),
                node_properties: None,
            }],
            output_directories: vec![OutputDirectory {
                path: "some path4".to_string(),
                tree_digest: Some(DigestInfo::new([12u8; 32], 124).into()),
                is_topologically_sorted: false,
            }],
            output_directory_symlinks: Vec::default(), // Bazel deprecated this.
            exit_code: 5,
            stdout_raw: Bytes::default(), // We don't implement this.
            stdout_digest: Some(DigestInfo::new([10u8; 32], 124).into()),
            stderr_raw: Bytes::default(), // We don't implement this.
            stderr_digest: Some(DigestInfo::new([11u8; 32], 124).into()),
            execution_metadata: Some(ExecutedActionMetadata {
                worker: test_context.worker_id.to_string(),
                queued_timestamp: Some(make_system_time(1).into()),
                worker_start_timestamp: Some(make_system_time(2).into()),
                worker_completed_timestamp: Some(make_system_time(3).into()),
                input_fetch_start_timestamp: Some(make_system_time(4).into()),
                input_fetch_completed_timestamp: Some(make_system_time(5).into()),
                execution_start_timestamp: Some(make_system_time(6).into()),
                execution_completed_timestamp: Some(make_system_time(7).into()),
                output_upload_start_timestamp: Some(make_system_time(8).into()),
                output_upload_completed_timestamp: Some(make_system_time(9).into()),
                virtual_execution_duration: Some(prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                }),
                auxiliary_metadata: vec![],
            }),
        }),
        cached_result: false,
        status: Some(ProtoStatus {
            code: 9,
            message: "foo".to_string(),
            details: Vec::default(),
        }),
        server_logs,
        message: "TODO(palfrey) We should put a reference something like bb_browser".to_string(),
    };
    let result = ExecuteResult {
        instance_name,
        operation_id: expected_operation_id.to_string(),
        result: Some(execute_result::Result::ExecuteResponse(
            execute_response.clone(),
        )),
    };

    let update_for_worker = test_context
        .connection_worker_stream
        .next()
        .await
        .expect("Worker stream ended early")?
        .update
        .expect("Expected update field to be populated");
    let update_for_worker::Update::StartAction(start_execute) = update_for_worker else {
        panic!("Expected StartAction message");
    };
    assert_eq!(result.operation_id, start_execute.operation_id);

    {
        // Ensure our state manager got the same result as the server.
        let (execution_response_result, (operation_id, worker_id, client_given_update)) = join!(
            test_context
                .worker_stream
                .send(Update::ExecuteResult(result.clone())),
            test_context.state_manager.expect_update_operation(Ok(())),
        );
        execution_response_result?;

        assert_eq!(operation_id, expected_operation_id);
        assert_eq!(worker_id, test_context.worker_id);
        assert_eq!(
            client_given_update,
            UpdateOperationType::UpdateWithActionStage(execute_response.clone().try_into()?)
        );
        let UpdateOperationType::UpdateWithActionStage(client_given_state) = client_given_update
        else {
            unreachable!()
        };
        assert_eq!(execute_response, client_given_state.into());
    }
    Ok(())
}

#[nativelink_test]
pub async fn workers_only_allow_max_tasks() -> Result<(), Box<dyn core::error::Error>> {
    let test_context =
        setup_api_server_with_task_limit(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn), 1).await?;

    let selected_worker = test_context
        .scheduler
        .find_worker_for_action(&PlatformProperties::new(HashMap::new()), true)
        .await;
    assert_eq!(
        selected_worker,
        Some(test_context.worker_id.clone()),
        "Expected worker to permit tasks to begin with"
    );

    let action_digest = DigestInfo::new([7u8; 32], 123);
    let instance_name = "instance_name".to_string();

    let unique_qualifier = ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
        instance_name: instance_name.clone(),
        digest_function: DigestHasherFunc::Sha256,
        digest: action_digest,
    });

    let action_info = Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: make_system_time(0),
        insert_timestamp: make_system_time(0),
        unique_qualifier,
    });

    let platform_properties = test_context
        .scheduler
        .get_platform_property_manager()
        .make_platform_properties(action_info.platform_properties.clone())
        .err_tip(|| "Failed to make platform properties in SimpleScheduler::do_try_match")?;

    let expected_operation_id = OperationId::default();

    test_context
        .scheduler
        .worker_notify_run_action(
            test_context.worker_id.clone(),
            expected_operation_id,
            ActionInfoWithProps {
                inner: action_info,
                platform_properties,
            },
        )
        .await
        .unwrap();

    let selected_worker = test_context
        .scheduler
        .find_worker_for_action(&PlatformProperties::new(HashMap::new()), true)
        .await;
    assert_eq!(
        selected_worker, None,
        "Expected not to be able to give worker a second task"
    );

    assert!(logs_contain("All workers are fully allocated"));

    Ok(())
}

// --- Blob locality map tests ---

struct LocalityTestContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    locality_map: SharedBlobLocalityMap,
}

/// Sets up a WorkerApiServer with a real SharedBlobLocalityMap and a worker
/// that has a CAS endpoint set. Returns the context needed to send updates
/// and verify the locality map.
async fn setup_api_server_with_locality(
    cas_endpoint: &str,
) -> Result<LocalityTestContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let maybe_first_message = connection_worker_stream.next().await;
    assert!(
        maybe_first_message.is_some(),
        "Expected first message from stream"
    );
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field to be populated")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };

    assert_eq!(
        worker_id.len(),
        UUID_SIZE,
        "Worker ID should be 36 characters"
    );

    Ok(LocalityTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        locality_map,
    })
}

// ----- Capacity-plumbing test (review #7) -----------------------------
//
// Verifies that mirror_used_bytes / mirror_max_bytes from a
// `BlobsAvailableNotification` reach `WorkerProxyStore::record_mirror_capacity`
// and end up in the picker's per-endpoint state. Pre-fix the existing
// tests just wrote the literals `mirror_used_bytes: 0, mirror_max_bytes: 0`
// to compile; nothing asserted that a non-zero report propagates.

#[cfg(feature = "test-utils")]
struct MirrorCapacityTestContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    _connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    worker_proxy: Arc<nativelink_store::worker_proxy_store::WorkerProxyStore>,
}

#[cfg(feature = "test-utils")]
async fn setup_api_server_with_mirror_proxy(
    cas_endpoint: &str,
) -> Result<MirrorCapacityTestContext, Error> {
    use nativelink_config::stores::MemorySpec;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_store::worker_proxy_store::WorkerProxyStore;
    use nativelink_util::store_trait::Store;

    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let worker_proxy = WorkerProxyStore::new(inner, locality_map.clone());

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        None,
        Some(worker_proxy.clone()),
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let maybe_first_message = connection_worker_stream.next().await;
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field to be populated")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(MirrorCapacityTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        _connection_worker_stream: connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        worker_proxy,
    })
}

/// Send a `BlobsAvailable` with `mirror_used_bytes` / `mirror_max_bytes`
/// set and verify the picker's per-endpoint state was updated.
/// Mutate-test guidance: comment out the `proxy.record_mirror_capacity(...)`
/// block in `worker_api_server.rs` (around the `if notification.mirror_max_bytes > 0`
/// guard); this test must fail.
#[cfg(feature = "test-utils")]
#[nativelink_test]
pub async fn mirror_capacity_report_plumbed_to_picker_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.20:50081";
    let test_context = setup_api_server_with_mirror_proxy(cas_endpoint).await?;

    // Pre-condition: no capacity recorded yet.
    assert_eq!(
        test_context.worker_proxy.mirror_capacity_for_test(cas_endpoint),
        None,
        "no capacity report yet"
    );

    // Worker reports: 1MiB used, 2GiB cap.
    const REPORTED_USED: u64 = 1_000_000;
    const REPORTED_MAX: u64 = 2_000_000_000;
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: cas_endpoint.to_string(),
            digests: vec![],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: REPORTED_USED,
            mirror_max_bytes: REPORTED_MAX,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;

    // Poll until the picker sees the report — bounded so a regression
    // (no plumbing) surfaces as a clean failure rather than a hang.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let observed = loop {
        if let Some(cap) =
            test_context.worker_proxy.mirror_capacity_for_test(cas_endpoint)
        {
            break cap;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "mirror_used_bytes/mirror_max_bytes did not reach the picker \
                 within 5s — `record_mirror_capacity` plumbing is broken"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    assert_eq!(
        observed,
        (REPORTED_USED, REPORTED_MAX),
        "picker must see the exact bytes the worker reported"
    );

    Ok(())
}

/// Capacity report with `mirror_max_bytes == 0` is suppressed by the
/// dispatch arm — workers without a CAS server (and thus no mirror_blobs
/// map) report zeroes that should NOT be stored as `(used=0, max=0)` because
/// `fits()` would then always succeed for them.
#[cfg(feature = "test-utils")]
#[nativelink_test]
pub async fn zero_mirror_max_does_not_record_capacity_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.21:50081";
    let test_context = setup_api_server_with_mirror_proxy(cas_endpoint).await?;

    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: cas_endpoint.to_string(),
            digests: vec![],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;

    // Wait long enough that any plumbing would have fired, then confirm
    // nothing was recorded. Poll-and-fail-on-presence rather than just
    // a single sleep: if the suppression IS broken, we want a deterministic
    // catch on the first iteration.
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        if test_context
            .worker_proxy
            .mirror_capacity_for_test(cas_endpoint)
            .is_some()
        {
            panic!(
                "mirror_max_bytes == 0 was recorded as capacity; this would \
                 make `fits()` always return true and mask saturated peers"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

#[nativelink_test]
pub async fn handle_blobs_available_populates_locality_map_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);

    // Send a BlobsAvailable notification with two digests.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(), // Empty means use the worker's registered endpoint.
            digests: vec![d1.into(), d2.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending blobs available: {e}"))?;

    // Allow background task to process the update.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify the locality map has both digests registered to the endpoint.
    let map = test_context.locality_map.read();
    let workers_d1 = map.lookup_workers(&d1);
    assert_eq!(
        workers_d1.len(),
        1,
        "Expected d1 to have 1 endpoint, got {workers_d1:?}"
    );
    assert_eq!(&*workers_d1[0], cas_endpoint);

    let workers_d2 = map.lookup_workers(&d2);
    assert_eq!(
        workers_d2.len(),
        1,
        "Expected d2 to have 1 endpoint, got {workers_d2:?}"
    );
    assert_eq!(&*workers_d2[0], cas_endpoint);

    assert_eq!(map.digest_count(), 2);
    assert_eq!(map.endpoint_count(), 1);

    Ok(())
}

#[nativelink_test]
pub async fn full_snapshot_replaces_endpoint_view_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);
    let d3 = DigestInfo::new([3u8; 32], 300);

    // First, register d1 and d2 with an incremental update.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into(), d2.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Confirm d1 and d2 are present.
    {
        let map = test_context.locality_map.read();
        assert_eq!(map.digest_count(), 2);
        assert!(!map.lookup_workers(&d1).is_empty());
        assert!(!map.lookup_workers(&d2).is_empty());
    }

    // Now send a full snapshot containing only d3.
    // This should clear d1 and d2 and only have d3.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d3.into()],
            is_full_snapshot: true,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify: d1 and d2 should be gone, only d3 remains.
    let map = test_context.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "d1 should have been cleared by full snapshot"
    );
    assert!(
        map.lookup_workers(&d2).is_empty(),
        "d2 should have been cleared by full snapshot"
    );
    let workers_d3 = map.lookup_workers(&d3);
    assert_eq!(
        workers_d3.len(),
        1,
        "d3 should be registered after full snapshot"
    );
    assert_eq!(&*workers_d3[0], cas_endpoint);
    assert_eq!(map.digest_count(), 1);

    Ok(())
}

#[nativelink_test]
pub async fn incremental_update_preserves_existing_blobs_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);
    let d3 = DigestInfo::new([3u8; 32], 300);

    // First update: register d1 and d2.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into(), d2.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second update (incremental): register d3 only.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d3.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // All three digests should be present.
    let map = test_context.locality_map.read();
    assert_eq!(
        map.digest_count(),
        3,
        "All three digests should be present after incremental update"
    );
    assert!(!map.lookup_workers(&d1).is_empty(), "d1 should still exist");
    assert!(!map.lookup_workers(&d2).is_empty(), "d2 should still exist");
    assert!(!map.lookup_workers(&d3).is_empty(), "d3 should be added");

    Ok(())
}

#[nativelink_test]
pub async fn eviction_removes_digests_from_locality_map_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);
    let d3 = DigestInfo::new([3u8; 32], 300);

    // Register d1, d2, d3.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into(), d2.into(), d3.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now send an incremental update with evicted_digests containing d1 and d2.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![],
            is_full_snapshot: false,
            evicted_digests: vec![d1.into(), d2.into()],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // d1 and d2 should be evicted, d3 remains.
    let map = test_context.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "d1 should have been evicted"
    );
    assert!(
        map.lookup_workers(&d2).is_empty(),
        "d2 should have been evicted"
    );
    assert_eq!(
        map.lookup_workers(&d3).len(),
        1,
        "d3 should still be present"
    );
    assert_eq!(map.digest_count(), 1);

    Ok(())
}

#[nativelink_test]
pub async fn worker_disconnect_cleans_up_locality_map_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);

    // Register d1 and d2.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into(), d2.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Confirm blobs are present.
    {
        let map = test_context.locality_map.read();
        assert_eq!(map.digest_count(), 2);
        assert_eq!(map.endpoint_count(), 1);
    }

    // Drop the worker stream sender to simulate disconnect.
    // The background task in WorkerConnection will see the stream end
    // and call remove_endpoint on the locality map.
    drop(test_context.worker_stream);
    drop(test_context.connection_worker_stream);

    // Allow the background cleanup task to run.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // All entries for this endpoint should be removed.
    let map = test_context.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "d1 should be removed after worker disconnect"
    );
    assert!(
        map.lookup_workers(&d2).is_empty(),
        "d2 should be removed after worker disconnect"
    );
    assert_eq!(
        map.endpoint_count(),
        0,
        "No endpoints should remain after disconnect"
    );
    assert_eq!(
        map.digest_count(),
        0,
        "No digests should remain after disconnect"
    );

    Ok(())
}

#[nativelink_test]
pub async fn blobs_available_with_malformed_digests_test()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;

    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);
    let d2 = DigestInfo::new([2u8; 32], 200);

    // Build the digests list: 2 valid + 1 malformed (hash too short).
    let valid1: ProtoDigest = d1.into();
    let valid2: ProtoDigest = d2.into();
    let malformed = ProtoDigest {
        hash: "deadbeef".to_string(), // Only 8 hex chars, not 64.
        size_bytes: 999,
        ..Default::default()
    };

    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![valid1, malformed, valid2],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Only the 2 valid digests should appear in the locality map.
    let map = test_context.locality_map.read();
    assert_eq!(
        map.digest_count(),
        2,
        "Expected exactly 2 valid digests in locality map, got {}",
        map.digest_count()
    );
    assert!(
        !map.lookup_workers(&d1).is_empty(),
        "Expected d1 to be registered"
    );
    assert!(
        !map.lookup_workers(&d2).is_empty(),
        "Expected d2 to be registered"
    );

    Ok(())
}

#[nativelink_test]
pub async fn blobs_evicted_is_noop_for_wire_compat_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let test_context = setup_api_server_with_locality(cas_endpoint).await?;

    let d1 = DigestInfo::new([1u8; 32], 100);

    // Register d1.
    test_context
        .worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send BlobsEvicted -- should be a no-op (handler returns Ok(())).
    // The old BlobsEvicted RPC is kept for wire compatibility but ignored.
    test_context
        .worker_stream
        .send(Update::BlobsEvicted(BlobsEvictedNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d1.into()],
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // d1 should STILL be present because BlobsEvicted is now a no-op.
    let map = test_context.locality_map.read();
    assert_eq!(
        map.lookup_workers(&d1).len(),
        1,
        "d1 should still be present -- BlobsEvicted is a no-op for wire compat"
    );

    Ok(())
}

// ----- #141 boot_epoch_id one-way wipe tests --------------------------
//
// Background: when a worker process dies (OOM / kill -9 / panic) and a
// fresh process restarts on the same CAS endpoint, locality_map entries
// from the old process point to `mirror_blobs` that lived only in the
// dead process's memory. A new worker that has none of those blobs
// must NOT inherit the old entries — otherwise the server will route
// reads to the new worker for blobs it doesn't have.
//
// boot_epoch_id is generated fresh at process start. The scheduler:
//   - same epoch on reconnect (transient stream drop) → preserve entries
//   - different epoch on reconnect (fresh process) → wipe entries
//   - missing/zero epoch (legacy worker) → wipe (conservative)

/// Helper that creates a `WorkerApiServer` once and lets tests open
/// multiple consecutive `connect_worker` streams against it.
struct MultiConnectContext {
    worker_api_server: WorkerApiServer,
    locality_map: SharedBlobLocalityMap,
}

async fn setup_multi_connect() -> Result<MultiConnectContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    Ok(MultiConnectContext {
        worker_api_server,
        locality_map,
    })
}

/// Open one connect_worker stream with the given endpoint+boot_epoch and
/// drive it to the point where the ConnectionResult has been received.
async fn open_worker_connection(
    server: &WorkerApiServer,
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<(mpsc::Sender<Update>, ConnectWorkerStream), Error> {
    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();
    // Consume the ConnectionResult so callers see post-handshake state.
    let first = connection_worker_stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    assert!(
        matches!(first, update_for_worker::Update::ConnectionResult(_)),
        "first update must be ConnectionResult, got {first:?}"
    );
    Ok((tx, connection_worker_stream))
}

/// Send `BlobsAvailable` with the given digests on a connected worker
/// stream. Uses a polling loop bounded by `deadline` to wait until at
/// least `expected_total` digests are visible in the locality map.
async fn send_blobs_and_wait(
    worker_stream: &mpsc::Sender<Update>,
    locality_map: &SharedBlobLocalityMap,
    cas_endpoint: &str,
    digests: Vec<DigestInfo>,
    is_full_snapshot: bool,
    expected_for_endpoint: usize,
) -> Result<(), Error> {
    worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: cas_endpoint.to_string(),
            digests: digests.iter().copied().map(Into::into).collect(),
            is_full_snapshot,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending: {e}"))?;

    // Poll until the digests appear (or fail loudly after a deadline).
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let count = {
            let map = locality_map.read();
            digests
                .iter()
                .filter(|d| {
                    map.lookup_workers(d)
                        .iter()
                        .any(|ep| &**ep == cas_endpoint)
                })
                .count()
        };
        if count >= expected_for_endpoint {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(make_err!(
        tonic::Code::DeadlineExceeded,
        "BlobsAvailable did not propagate to locality_map within 2s"
    ))
}

/// Reconnecting with the SAME boot_epoch_id (transient drop, same
/// process) MUST preserve locality_map entries from the prior connection.
///
/// Production race scenario: a transient stream drop fires the OLD
/// WorkerConnection's disconnect cleanup task, which BLINDLY calls
/// `remove_endpoint` and wipes the entries. After the fix, the cleanup
/// task must skip the wipe when the current epoch entry no longer
/// belongs to its own connection (the new connection re-registered with
/// the same epoch, so the cleanup task should be a no-op).
#[nativelink_test]
pub async fn boot_epoch_same_preserves_locality_entries_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.30:50081";
    let ctx = setup_multi_connect().await?;
    let d1 = DigestInfo::new([0xA1u8; 32], 100);
    let d2 = DigestInfo::new([0xA2u8; 32], 200);

    // First boot.
    let (tx1, stream1) = open_worker_connection(&ctx.worker_api_server, cas_endpoint, 7777)
        .await?;
    send_blobs_and_wait(&tx1, &ctx.locality_map, cas_endpoint, vec![d1, d2], true, 2)
        .await?;

    // Disconnect: drop both ends so WorkerConnection cleanup task fires.
    drop(tx1);
    drop(stream1);

    // Reconnect with the SAME epoch — registration MUST NOT wipe, AND
    // the now-fired cleanup task from the old connection MUST be
    // suppressed (it sees the endpoint's epoch is still 7777 = its own
    // epoch, but the connection identity is no longer this one).
    let (_tx2, _stream2) =
        open_worker_connection(&ctx.worker_api_server, cas_endpoint, 7777).await?;

    // Give the disconnect-cleanup background task generous time to
    // run AFTER reconnect (this is the race window).
    tokio::time::sleep(Duration::from_millis(200)).await;

    let map = ctx.locality_map.read();
    assert_eq!(
        map.lookup_workers(&d1).len(),
        1,
        "d1 must survive same-epoch reconnect"
    );
    assert_eq!(
        map.lookup_workers(&d2).len(),
        1,
        "d2 must survive same-epoch reconnect"
    );
    assert_eq!(&*map.lookup_workers(&d1)[0], cas_endpoint);
    Ok(())
}

/// Reconnecting with a DIFFERENT boot_epoch_id (fresh process after
/// crash) MUST wipe the prior locality_map entries on registration —
/// they referenced blobs only the dead process held in memory.
///
/// Test mechanic: hold the OLD stream alive across the reconnect to
/// prevent the disconnect cleanup task from contributing to the wipe.
/// If entries disappear, it is the registration path that wiped them,
/// not the cleanup task. Without the fix, the entries persist (the
/// wipe-on-register isn't there) and this test fails.
#[nativelink_test]
pub async fn boot_epoch_different_wipes_locality_entries_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.31:50081";
    let ctx = setup_multi_connect().await?;
    let d1 = DigestInfo::new([0xB1u8; 32], 100);
    let d2 = DigestInfo::new([0xB2u8; 32], 200);

    let (tx1, stream1) = open_worker_connection(&ctx.worker_api_server, cas_endpoint, 1111)
        .await?;
    send_blobs_and_wait(&tx1, &ctx.locality_map, cas_endpoint, vec![d1, d2], true, 2)
        .await?;

    // Reconnect with a NEW epoch WHILE the old stream is still open.
    // The wipe in this scenario can ONLY come from the registration
    // path — it would falsely pass if we let the old cleanup run.
    let (_tx2, _stream2) =
        open_worker_connection(&ctx.worker_api_server, cas_endpoint, 2222).await?;

    let map = ctx.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "d1 must be wiped on different-epoch reconnect (d1 -> {:?})",
        map.lookup_workers(&d1)
    );
    assert!(
        map.lookup_workers(&d2).is_empty(),
        "d2 must be wiped on different-epoch reconnect (d2 -> {:?})",
        map.lookup_workers(&d2)
    );

    // Tidy up the still-open old stream.
    drop(tx1);
    drop(stream1);
    Ok(())
}

/// A legacy worker that omits boot_epoch_id (proto3 default 0) MUST
/// trigger the conservative wipe path on every reconnect — we cannot
/// distinguish a legacy reconnect from a fresh process.
///
/// Same mechanic: hold the old stream alive so the wipe must come from
/// the registration path, not from the disconnect cleanup.
#[nativelink_test]
pub async fn boot_epoch_zero_legacy_wipes_locality_entries_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.32:50081";
    let ctx = setup_multi_connect().await?;
    let d1 = DigestInfo::new([0xC1u8; 32], 100);

    // First boot with a real epoch (e.g. the new binary).
    let (tx1, stream1) = open_worker_connection(&ctx.worker_api_server, cas_endpoint, 99)
        .await?;
    send_blobs_and_wait(&tx1, &ctx.locality_map, cas_endpoint, vec![d1], true, 1)
        .await?;

    // Reconnect with epoch 0 (legacy / unset) WHILE the old stream is
    // still open — wipe must come from registration.
    let (_tx2, _stream2) =
        open_worker_connection(&ctx.worker_api_server, cas_endpoint, 0).await?;

    let map = ctx.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "legacy boot_epoch_id (0) must trigger wipe (d1 -> {:?})",
        map.lookup_workers(&d1)
    );

    drop(tx1);
    drop(stream1);
    Ok(())
}

/// New worker after wipe must be able to register its own blobs on the
/// same endpoint without the OLD WorkerConnection's disconnect cleanup
/// task wiping them. The OLD task fires at a non-deterministic time
/// after the new connection lands; if its `remove_endpoint` runs
/// unguarded it will erase the new worker's just-registered entries.
///
/// This test exercises the disconnect-suppression contract: after a
/// different-epoch reconnect, the new worker registers a new digest
/// and we wait long enough that any straggling cleanup from the old
/// connection would have run, then assert the new digest is still
/// present.
#[nativelink_test]
pub async fn boot_epoch_new_blobs_survive_old_disconnect_cleanup_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.33:50081";
    let ctx = setup_multi_connect().await?;
    let d_old = DigestInfo::new([0xD1u8; 32], 100);
    let d_new = DigestInfo::new([0xD2u8; 32], 200);

    let (tx1, stream1) = open_worker_connection(&ctx.worker_api_server, cas_endpoint, 100)
        .await?;
    send_blobs_and_wait(&tx1, &ctx.locality_map, cas_endpoint, vec![d_old], true, 1)
        .await?;

    // Connect new worker BEFORE dropping the old one — establishes the
    // new boot_epoch entry, so the OLD cleanup task (when it fires)
    // sees a stale epoch and must skip its remove_endpoint.
    let (tx2, _stream2) =
        open_worker_connection(&ctx.worker_api_server, cas_endpoint, 200).await?;
    send_blobs_and_wait(&tx2, &ctx.locality_map, cas_endpoint, vec![d_new], true, 1)
        .await?;

    // NOW drop the old connection — its cleanup task should be a
    // no-op because the endpoint's current epoch (200) is not its
    // own (100).
    drop(tx1);
    drop(stream1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let map = ctx.locality_map.read();
    assert!(
        map.lookup_workers(&d_old).is_empty(),
        "d_old must remain wiped"
    );
    assert_eq!(
        map.lookup_workers(&d_new).len(),
        1,
        "d_new must survive — old cleanup must not wipe new entries"
    );
    Ok(())
}

// =====================================================================
// BLOCK-2 / task #157: WorkerApiMetrics is wired into the metrics tree
// =====================================================================
//
// Asserts the `mark_stable_has_with_results_failures` AtomicU64 actually
// reaches the registered metric tree (the bug red-team caught: prior to
// the `#[derive(MetricsComponent)]` + `#[metric(group = "worker_api")]`
// wiring, the counter lived only as a private AtomicU64 — operators had
// no way to alert on it). The test exercises the increment path AND
// asserts the publish output emits the counter name + value via a
// custom tracing layer (the `metric` macro emits via `tracing::info!`
// to the `nativelink_metric` target).

/// In-memory layer that captures events targeting `nativelink_metric`.
/// One row per published metric: (name, value, help).
#[derive(Debug, Default, Clone)]
struct CapturedMetric {
    name: String,
    value: String,
    help: String,
}

#[derive(Default)]
struct MetricCaptureLayer {
    events: Arc<Mutex<Vec<CapturedMetric>>>,
}

impl<S> tracing_subscriber::Layer<S> for MetricCaptureLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "nativelink_metric" {
            return;
        }
        let mut visitor = FieldGrabber::default();
        event.record(&mut visitor);
        // Empty-name events are the group-span enters from `group!(...)`,
        // not actual metric publishes. Skip them.
        if visitor.name.is_empty() {
            return;
        }
        self.events.lock().unwrap().push(CapturedMetric {
            name: visitor.name,
            value: visitor.value,
            help: visitor.help,
        });
    }
}

#[derive(Default)]
struct FieldGrabber {
    name: String,
    value: String,
    help: String,
}

impl tracing::field::Visit for FieldGrabber {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
        let s = format!("{value:?}");
        // Strip surrounding quotes for fields that come through as Debug-of-String.
        let trimmed = s.trim_matches('"').to_string();
        match field.name() {
            "__name" => self.name = trimmed,
            "__value" => self.value = trimmed,
            "__help" => self.help = trimmed,
            _ => {}
        }
    }
}

#[nativelink_test]
async fn worker_api_metrics_mark_stable_failures_visible_in_metric_tree() {
    use core::sync::atomic::Ordering;

    use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    let layer = MetricCaptureLayer::default();
    let events = layer.events.clone();
    let subscriber = Registry::default().with(layer);

    let _guard = tracing::subscriber::set_default(subscriber);

    let metrics = WorkerApiMetrics::default();

    // Drive the increment path the production code path uses.
    metrics
        .mark_stable_has_with_results_failures
        .fetch_add(3, Ordering::Relaxed);

    // Walk the metric tree the same way the metric exporter does.
    metrics
        .publish(MetricKind::Component, MetricFieldData::default())
        .expect("publish must succeed for derived MetricsComponent");

    drop(_guard);

    let captured = events.lock().unwrap().clone();
    let metric = captured
        .iter()
        .find(|m| m.name == "mark_stable_has_with_results_failures")
        .unwrap_or_else(|| {
            panic!(
                "expected metric `mark_stable_has_with_results_failures` to be \
                 published when WorkerApiMetrics::publish() walks the tree. \
                 Captured events: {captured:#?}. Without #[derive(MetricsComponent)] \
                 + #[metric(help = ...)] on the field, the AtomicU64 stays invisible \
                 to operators (BLOCK-2)."
            )
        });
    assert_eq!(
        metric.value, "3",
        "expected counter value 3 to flow through the publish chain — got {:?}. \
         If publish() returned Component without emitting Counter, the derive \
         output is silently mis-routing the AtomicU64.",
        metric.value
    );
    assert!(
        !metric.help.is_empty(),
        "expected non-empty help text for `mark_stable_has_with_results_failures` \
         so operators have a description in the metric stream — found empty help. \
         Add `#[metric(help = \"...\")]` to the field."
    );
}

// =====================================================================
// task #168: SmallBlobDispatcher production-composition disconnect test
// =====================================================================
//
// testing-czar #168 MAJOR-1: every existing dispatcher test owns the
// SmallBlobDispatcher in isolation and exercises its API directly. The
// PRODUCTION wireup contract — "WorkerApiServer's disconnect-cleanup
// task calls dispatcher.unpin_on_disconnect when the worker stream is
// dropped" — is invisible to those tests. A regression that deletes
// the unpin_on_disconnect call (or accidentally moves it outside the
// ownership-check guard) would leak every server-side pin entry on
// every worker disconnect, drifting `pin_max_bytes` toward
// ResourceExhausted with no test signal until production.
//
// This test wraps the dispatcher in its production composition
// (`WorkerApiServer::new_with_now_fn(... Some(dispatcher) ...)`),
// connects a worker, populates the dispatcher's pin set directly via
// the pin-set API, then drops the worker stream. The disconnect-cleanup
// task in `WorkerConnection::start` MUST call
// `dispatcher.unpin_on_disconnect(endpoint, boot_epoch)` from inside
// the ownership-check guard, draining the pin set. The assertion
// is wrapped in `tokio::time::timeout(5s, ...)` per the CLAUDE.md
// production-composition deadlock-detector rule.
//
// Mutation step (per CLAUDE.md TDD step 5): in
// `nativelink-service/src/worker_api_server.rs`, comment out the
// `dispatcher.unpin_on_disconnect(...)` line inside the
// `if let Some(ref dispatcher) = instance.small_blob_dispatcher` block
// (around line 674). The test MUST then panic with
// "disconnect must drain dispatcher pin set within 5s — wireup
// contract violated".
async fn setup_api_server_with_dispatcher(
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<DispatcherTestContext, Error> {
    use nativelink_store::small_blob_dispatcher::{
        EphemeralServerSidePin, SmallBlobDispatcher, SmallBlobDispatcherConfig,
    };

    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    // Production-realistic dispatcher: feature flag is OFF (matches
    // production today; the test exercises the disconnect-cleanup path
    // which fires regardless of the flag because pin-set membership is
    // the property under test). Pin set is registered for "cas" and
    // populated directly via the public API.
    let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));
    let cas_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024 * 1024));
    dispatcher.register_pin_set("cas", cas_pin.clone());

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        None, // no locality_map needed
        None, // no cas_store
        None, // no worker_proxy
        Some(dispatcher.clone()),
        None, // no ac_pin_registry
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let first = connection_worker_stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    let worker_id = match first {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(DispatcherTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        dispatcher,
        cas_pin,
    })
}

#[expect(dead_code, reason = "fields kept alive for the duration of the test")]
struct DispatcherTestContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    dispatcher: Arc<nativelink_store::small_blob_dispatcher::SmallBlobDispatcher>,
    cas_pin: Arc<nativelink_store::small_blob_dispatcher::EphemeralServerSidePin>,
}

#[nativelink_test]
pub async fn dispatcher_unpin_on_worker_disconnect_drains_pin_set_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.40:50081";
    let boot_epoch = 4242u64;
    let ctx = setup_api_server_with_dispatcher(cas_endpoint, boot_epoch).await?;

    // Populate the dispatcher's "cas" pin set directly with two
    // entries — these simulate the in-flight push tracker for blobs
    // the dispatcher pushed to the worker that have not yet been
    // ack'd via BlobsAvailable.pinned_mirror_entries. When the worker
    // disconnects without sending the matching ack, the cleanup task
    // MUST drain the pin set so the bytes do not leak the
    // server-side push tracker forever.
    let d1 = DigestInfo::new([0xE1u8; 32], 100);
    let d2 = DigestInfo::new([0xE2u8; 32], 200);
    ctx.cas_pin
        .insert(d1, Bytes::from(vec![0u8; 100]))
        .err_tip(|| "pin insert d1")?;
    ctx.cas_pin
        .insert(d2, Bytes::from(vec![0u8; 200]))
        .err_tip(|| "pin insert d2")?;
    assert_eq!(ctx.cas_pin.len(), 2, "pre-disconnect: pin set populated");
    assert_eq!(ctx.cas_pin.total_bytes(), 300);

    // Drop the worker stream — both ends — to fire the
    // WorkerConnection cleanup task.
    drop(ctx.worker_stream);
    drop(ctx.connection_worker_stream);

    // Wait — bounded — for the cleanup task to drain the pin set.
    // The 5s deadlock detector exists per the CLAUDE.md
    // production-composition rule: a regression that deletes the
    // dispatcher.unpin_on_disconnect call (or accidentally moves it
    // outside the ownership-check guard) would leave the pin set
    // populated forever; without the timeout the CI runner would hang
    // instead of failing.
    let pin = ctx.cas_pin.clone();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            if pin.is_empty() && pin.total_bytes() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect must drain dispatcher pin set within 5s — wireup contract violated");

    // Belt-and-braces post-condition checks: cleanly drained.
    assert!(
        ctx.cas_pin.is_empty(),
        "post-disconnect: pin set MUST be empty (got len={})",
        ctx.cas_pin.len()
    );
    assert_eq!(
        ctx.cas_pin.total_bytes(),
        0,
        "post-disconnect: total_bytes MUST be 0 (got {})",
        ctx.cas_pin.total_bytes()
    );
    Ok(())
}

// #174: boot-epoch wipe dispatcher leak. When a worker reconnects with a
// new boot_epoch BEFORE the OLD's disconnect-cleanup task runs (or when
// the OLD's cleanup runs after the wipe and is suppressed by the
// ownership-check guard), the OLD epoch's `dispatcher.worker_txs[(endpoint,
// OLD_epoch)]` and per-(worker, OLD_epoch) queues leak forever — the new
// owner has overwritten `endpoint_state[endpoint].owner_worker_id` so
// the OLD cleanup task SKIPS its `unregister_worker` call. Symmetric to
// the locality_map wipe (#141) that the boot-epoch wipe block already
// performs. Currently dormant in production behind
// `small_blob_mirror_enabled=false`; becomes an active leak the moment
// the flag flips. The fix: while `endpoint_state` lock is held in the
// boot-epoch wipe block, the wipe MUST also call
// `dispatcher.unregister_worker(endpoint, prev_epoch)` and
// `dispatcher.unpin_on_disconnect(endpoint, prev_epoch)`.
//
// Builds a fresh dispatcher with `small_blob_mirror_enabled=true` and
// `pin_max_bytes` high enough that the precondition gates pass. Connects
// a worker at (endpoint=E, epoch=A); seeds `worker_txs[(E,A)]` (via the
// connect path) and a per-(worker, store) queue (via `enqueue`).
// Reconnects the SAME endpoint with a new boot_epoch B (epoch flip).
// The wipe block is the only synchronization point that can clear
// `(E, A)` state atomically with the `endpoint_state` ownership flip;
// after the second connect returns, `(E, A)` MUST be cleared.
async fn setup_dispatcher_with_mirror_enabled(
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<DispatcherTestContext, Error> {
    use nativelink_store::small_blob_dispatcher::{
        EphemeralServerSidePin, SmallBlobDispatcher, SmallBlobDispatcherConfig,
    };

    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    // Mirror flag ON so `enqueue` actually populates the queues map —
    // we need real per-(endpoint, boot_epoch_id, store_id) queue state
    // to assert the boot-epoch wipe clears it. In production today the
    // flag is OFF, but the leak this test guards against fires the
    // moment the flag flips.
    let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        ..Default::default()
    }));
    let cas_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024 * 1024));
    dispatcher.register_pin_set("cas", cas_pin.clone());

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        None, // no locality_map needed
        None, // no cas_store
        None, // no worker_proxy
        Some(dispatcher.clone()),
        None, // no ac_pin_registry
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let first = connection_worker_stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    let worker_id = match first {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(DispatcherTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        dispatcher,
        cas_pin,
    })
}

// Connect a SECOND worker stream on the same WorkerApiServer with the
// given boot_epoch_id. Returns the new (sender, stream) pair plus the
// allocated worker_id. Mirrors the second half of
// setup_dispatcher_with_mirror_enabled.
async fn connect_second_worker(
    server: &WorkerApiServer,
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<(mpsc::Sender<Update>, ConnectWorkerStream, String), Error> {
    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();
    let first = connection_worker_stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    let worker_id = match first {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    Ok((tx, connection_worker_stream, worker_id))
}

#[nativelink_test]
pub async fn boot_epoch_wipe_clears_dispatcher_state()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.41:50081";
    let old_epoch = 1111u64;
    let new_epoch = 2222u64;
    let ctx = setup_dispatcher_with_mirror_enabled(cas_endpoint, old_epoch).await?;

    // Sanity: register_worker fired during the connect path, so
    // worker_txs[(endpoint, old_epoch)] is populated.
    assert!(
        ctx.dispatcher.has_worker_tx_for_test(cas_endpoint, old_epoch),
        "pre-reconnect: dispatcher MUST have worker_tx for (endpoint, OLD)"
    );

    // Populate per-(endpoint, OLD_epoch, store_id) queue state by
    // calling enqueue. With small_blob_mirror_enabled=true and the
    // worker_tx + pin_set both registered, enqueue spawns a drainer
    // and inserts a queue entry for (endpoint, OLD_epoch, "cas").
    let d = DigestInfo::new([0xAA; 32], 64);
    ctx.dispatcher
        .enqueue(cas_endpoint, old_epoch, "cas", d, Bytes::from(vec![0u8; 64]))
        .await
        .err_tip(|| "enqueue OLD")?;
    assert_eq!(
        ctx.dispatcher
            .queue_count_for_worker_for_test(cas_endpoint, old_epoch),
        1,
        "pre-reconnect: per-(worker, OLD_epoch) queue MUST exist after enqueue"
    );

    // Reconnect the SAME endpoint with a NEW boot_epoch BEFORE the OLD
    // stream is dropped. This is the production race: the worker process
    // restarted, opened a new gRPC stream, and the new ConnectWorker
    // request landed before the OLD cleanup task ran. The boot-epoch
    // wipe block in inner_connect_worker is the ONLY synchronization
    // point that can clear (E, OLD) state atomically with the ownership
    // flip — once the new connect returns, the OLD cleanup task will be
    // suppressed by the ownership-check guard and any (E, OLD) state
    // that the wipe didn't clear leaks forever.
    let (_new_tx, _new_stream, _new_worker_id) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_second_worker(&ctx._worker_api_server, cas_endpoint, new_epoch),
    )
    .await
    .expect("second connect must complete within 5s — wipe path stalled")
    .err_tip(|| "second connect failed")?;

    // After the wipe completes (synchronously within the new connect):
    //   - worker_txs[(endpoint, OLD)] MUST be cleared
    //   - queues[(endpoint, OLD, *)] MUST be empty
    //   - pin set MUST be drained (unpin_on_disconnect was called)
    assert!(
        !ctx.dispatcher.has_worker_tx_for_test(cas_endpoint, old_epoch),
        "post-reconnect: dispatcher MUST NOT retain worker_tx for (endpoint, OLD) — \
         #174 leak: boot-epoch wipe failed to call unregister_worker"
    );
    assert_eq!(
        ctx.dispatcher
            .queue_count_for_worker_for_test(cas_endpoint, old_epoch),
        0,
        "post-reconnect: dispatcher MUST NOT retain per-(worker, OLD_epoch) queues — \
         #174 leak: boot-epoch wipe failed to call unregister_worker"
    );
    assert!(
        ctx.cas_pin.is_empty(),
        "post-reconnect: pin set MUST be drained (got len={}) — \
         #174 leak: boot-epoch wipe failed to call unpin_on_disconnect",
        ctx.cas_pin.len()
    );
    Ok(())
}

// Asymmetric contract coverage (CLAUDE.md §Tests): the wipe of
// endpoint A's `(endpoint_A, OLD_A)` state MUST NOT touch endpoint B's
// `(endpoint_B, ANY_B)` state. A regression that nukes BOTH (e.g. a
// `retain` filter that forgets to gate on `endpoint`, or a blunt
// `dispatcher.queues.lock().clear()`) would still pass the previous
// "NEW epoch on the SAME endpoint is registered" assertion (because
// register_worker runs unconditionally AFTER the wipe block, repopulating
// (endpoint_A, NEW_A) regardless of regression). This test pins the
// real over-action contract: a per-endpoint wipe must be SCOPED to that
// endpoint and must not collateral-damage other endpoints' state.
//
// **v1 limitation note.** `unpin_on_disconnect` is documented v1 broad-
// clearing — it clears EVERY registered store's pin set regardless of
// which `endpoint`/`boot_epoch_id` invoked it (see the doc comment on
// `SmallBlobDispatcher::unpin_on_disconnect` and `TODO(#168 follow-up)`).
// Therefore endpoint B's pin entry IS expected to be cleared by
// endpoint A's wipe today; we deliberately do NOT assert pin-set
// survival here. The per-endpoint contract DOES hold for `worker_tx`
// and `queues` (both are keyed by `(endpoint, boot_epoch_id)`), so
// those are the assertions we make. When per-attribution lands
// (#168/#190), update this test to also require that B's pin set
// survives A's wipe.
#[nativelink_test]
pub async fn boot_epoch_wipe_does_not_clear_other_endpoint_state()
-> Result<(), Box<dyn core::error::Error>> {
    let endpoint_a = "grpc://192.168.1.42:50081";
    let endpoint_b = "grpc://192.168.1.43:50081";
    let old_epoch_a = 3333u64;
    let new_epoch_a = 4444u64;
    let epoch_b = 5555u64;

    // (1) Connect worker A on endpoint_A with OLD_A. Seeds A's
    // worker_tx + queue (via enqueue) + pin entry.
    let ctx = setup_dispatcher_with_mirror_enabled(endpoint_a, old_epoch_a).await?;
    let d_a = DigestInfo::new([0xAA; 32], 32);
    ctx.dispatcher
        .enqueue(endpoint_a, old_epoch_a, "cas", d_a, Bytes::from(vec![0u8; 32]))
        .await
        .err_tip(|| "enqueue A")?;

    // (2) Connect worker B on endpoint_B with epoch_B (DIFFERENT
    // endpoint). Seeds B's worker_tx + queue (via enqueue) + pin entry.
    let (_b_tx, _b_stream, _b_worker_id) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_second_worker(&ctx._worker_api_server, endpoint_b, epoch_b),
    )
    .await
    .expect("worker B connect must complete within 5s")
    .err_tip(|| "worker B connect failed")?;
    let d_b = DigestInfo::new([0xBB; 32], 32);
    ctx.dispatcher
        .enqueue(endpoint_b, epoch_b, "cas", d_b, Bytes::from(vec![0u8; 32]))
        .await
        .err_tip(|| "enqueue B")?;

    // Sanity: B's per-endpoint state is populated before the wipe.
    assert!(
        ctx.dispatcher.has_worker_tx_for_test(endpoint_b, epoch_b),
        "pre-reconnect: dispatcher MUST have worker_tx for (endpoint_B, epoch_B)"
    );
    assert_eq!(
        ctx.dispatcher
            .queue_count_for_worker_for_test(endpoint_b, epoch_b),
        1,
        "pre-reconnect: per-(endpoint_B, epoch_B) queue MUST exist after enqueue"
    );

    // (3) Reconnect endpoint_A with NEW_A — triggers wipe of
    // (endpoint_A, OLD_A) state. The wipe is scoped to endpoint_A;
    // endpoint_B's state must survive.
    let (_a_new_tx, _a_new_stream, _a_new_worker_id) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_second_worker(&ctx._worker_api_server, endpoint_a, new_epoch_a),
    )
    .await
    .expect("endpoint_A reconnect must complete within 5s")
    .err_tip(|| "endpoint_A reconnect failed")?;

    // (4) Existing assertion: A's NEW_A state present (register_worker
    // ran post-wipe). This guards against a regression that nukes
    // (endpoint_A, *) including the NEW entry.
    assert!(
        ctx.dispatcher.has_worker_tx_for_test(endpoint_a, new_epoch_a),
        "post-reconnect: dispatcher MUST retain worker_tx for (endpoint_A, NEW_A) — \
         over-action regression: wipe cleared NEW epoch's state on the same endpoint"
    );

    // (5) THE OVER-ACTION CONTRACT: endpoint_B's state UNTOUCHED.
    // worker_tx and queues are keyed by (endpoint, boot_epoch_id), so a
    // correctly-scoped per-endpoint wipe leaves B's entries alone. A
    // regression that over-clears (e.g.
    // `dispatcher.queues.lock().clear()` or
    // `worker_txs.retain(|(_, e), _| *e != prev_epoch)`) would nuke B's
    // entries too.
    assert!(
        ctx.dispatcher.has_worker_tx_for_test(endpoint_b, epoch_b),
        "post-reconnect: endpoint_B's state must not be cleared by endpoint_A's wipe — \
         over-action contract: dispatcher.worker_tx for (endpoint_B, epoch_B) was over-cleared"
    );
    assert_eq!(
        ctx.dispatcher
            .queue_count_for_worker_for_test(endpoint_b, epoch_b),
        1,
        "post-reconnect: endpoint_B's state must not be cleared by endpoint_A's wipe — \
         over-action contract: dispatcher.queues for (endpoint_B, epoch_B) was over-cleared"
    );

    // Note: B's pin entry IS expected to be cleared by A's wipe under
    // the v1 limitation (see test header comment + #168/#190). The
    // per-endpoint contract above is the load-bearing assertion.
    Ok(())
}

// =====================================================================
// #168 item K — eager locality_map update on dispatch ack.
//
// Production composition: WorkerApiServer wired with BOTH a
// `SharedBlobLocalityMap` AND a `SmallBlobDispatcher`. When the worker
// reports `pinned_mirror_entries` (proto field 16) on a
// `BlobsAvailableNotification`, the server MUST register those digests
// in the locality_map keyed by the worker's CAS endpoint.
//
// USER DIRECTIVE (#168): "when the server mirrors small blobs in batch
// to workers ... add those blobs to the locality map server-side, after
// the ack. That way there is no delay in the server's knowledge; an
// action which references that blob could come sooner than a
// blobsavailable broadcast."
//
// Without this, the server's knowledge that worker W now holds digest
// D would lag the next periodic field-13 (`digests`) tick — meaning an
// action referencing D scheduled in the meantime would (a) trigger a
// redundant peer-fetch from another worker, (b) potentially be
// re-dispatched by the dispatcher because the server doesn't know W
// has it, OR (c) be scheduled away from W, missing locality affinity.
//
// Mutation step: comment out the
// `locality_map.write().register_blobs(endpoint, &digests)` call in
// `worker_api_server::handle_blobs_available` (next to the
// `broadcast_pinned_mirror_ack` call). This test MUST red-fail with
// the bespoke "locality_map MUST be updated within 2s of dispatch"
// assertion message.
async fn setup_api_server_with_locality_and_dispatcher(
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<DispatcherWithLocalityContext, Error> {
    use nativelink_store::small_blob_dispatcher::{
        EphemeralServerSidePin, SmallBlobDispatcher, SmallBlobDispatcherConfig,
    };

    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();
    let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));
    let cas_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024 * 1024));
    dispatcher.register_pin_set("cas", cas_pin.clone());

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        None, // no cas_store
        None, // no worker_proxy
        Some(dispatcher.clone()),
        None, // no ac_pin_registry
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(8);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let first = connection_worker_stream
        .next()
        .await
        .err_tip(|| "expected ConnectionResult")?
        .err_tip(|| "stream error before ConnectionResult")?
        .update
        .err_tip(|| "ConnectionResult update missing")?;
    let worker_id = match first {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(DispatcherWithLocalityContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        _connection_worker_stream: connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        _dispatcher: dispatcher,
        _cas_pin: cas_pin,
        locality_map,
    })
}

#[expect(dead_code, reason = "fields kept alive for the duration of the test")]
struct DispatcherWithLocalityContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    _connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    _dispatcher: Arc<nativelink_store::small_blob_dispatcher::SmallBlobDispatcher>,
    _cas_pin: Arc<nativelink_store::small_blob_dispatcher::EphemeralServerSidePin>,
    locality_map: SharedBlobLocalityMap,
}

#[nativelink_test]
pub async fn handle_blobs_available_pinned_mirror_entries_register_in_locality_map_test()
-> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.50:50081";
    let ctx = setup_api_server_with_locality_and_dispatcher(cas_endpoint, 4242u64).await?;

    // Two distinct dispatcher-pushed digests. The worker reports them
    // in `pinned_mirror_entries` (proto field 16, MirrorPinEntry) —
    // this is the wire form of "the worker now holds these bytes
    // because the server pushed them via the dispatcher."
    let d1 = DigestInfo::new([0xC1u8; 32], 1024);
    let d2 = DigestInfo::new([0xC2u8; 32], 2048);

    ctx.worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(), // empty ⇒ use registered endpoint
            digests: vec![],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            // The load-bearing field for #168 item K. Each entry is
            // an (store_id, digest) pair sorted by store_id ASCII —
            // the server must extract the digests and register them
            // in the locality_map keyed by the worker's endpoint.
            pinned_mirror_entries: vec![
                MirrorPinEntry {
                    digest: Some(d1.into()),
                    store_id: "cas".to_string(),
                },
                MirrorPinEntry {
                    digest: Some(d2.into()),
                    store_id: "cas".to_string(),
                },
            ],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending blobs available: {e}"))?;

    // Bounded poll for the locality_map update. 2s deadline per
    // user directive: "an action which references that blob could
    // come sooner than a blobsavailable broadcast" — the latency
    // window between dispatch and locality_map update must be tight
    // enough to outpace action arrival. We poll instead of sleeping
    // because the background BlobsAvailable handler is async and we
    // want fail-fast on a positive observation.
    let locality_map = ctx.locality_map.clone();
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let map = locality_map.read();
            let workers_d1 = map.lookup_workers(&d1);
            let workers_d2 = map.lookup_workers(&d2);
            if !workers_d1.is_empty() && !workers_d2.is_empty() {
                return (workers_d1, workers_d2);
            }
            drop(map);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#168 item K: locality_map MUST be updated within 2s of dispatch — \
         broadcast latency cannot exceed action arrival window. The server's \
         handle_blobs_available MUST register pinned_mirror_entries digests in \
         the locality_map BEFORE acking the dispatcher (broadcast_pinned_mirror_ack); \
         without this, an action referencing the dispatched digest scheduled \
         between dispatch and the next field-13 BlobsAvailable tick will not \
         see worker locality and either (a) trigger a redundant peer-fetch, \
         (b) be re-dispatched, or (c) be scheduled away from this worker.",
    );

    assert_eq!(
        observed.0.len(),
        1,
        "#168 item K: d1 must be registered against exactly one endpoint \
         (the dispatching worker); got {:?}",
        observed.0,
    );
    assert_eq!(
        &*observed.0[0],
        cas_endpoint,
        "#168 item K: d1 must be registered against the dispatching worker's \
         endpoint ({cas_endpoint}); got {:?}",
        observed.0,
    );
    assert_eq!(
        observed.1.len(),
        1,
        "#168 item K: d2 must be registered against exactly one endpoint \
         (the dispatching worker); got {:?}",
        observed.1,
    );
    assert_eq!(
        &*observed.1[0],
        cas_endpoint,
        "#168 item K: d2 must be registered against the dispatching worker's \
         endpoint ({cas_endpoint}); got {:?}",
        observed.1,
    );

    Ok(())
}

// =====================================================================
// #168 follow-up A2: field-16 fold into consolidated locality_map.write()
// =====================================================================
//
// Pre-fold, `handle_blobs_available` took `locality_map.write()` twice
// when a BlobsAvailable carried BOTH field-13 (`digests` /
// `pinned_mirror_digests`) AND field-16 (`pinned_mirror_entries`)
// payloads. The standalone field-16 block also fired
// `broadcast_pinned_mirror_ack` BEFORE the consolidated block ran, so
// a reader racing the ack could observe (a) pin released without
// locality entry, OR (b) locality entry written while pin still held —
// the "register BEFORE ack" invariant was structurally OK because the
// standalone block ordered field-16-locality before its own ack, but
// the FIELD-13 register-blobs-iter ran AFTER the field-16 ack, leaving
// a window where field-13 digests were absent from locality while the
// dispatcher believed every advertised peer had stable view.
//
// Post-fold, a SINGLE consolidated `register_blobs_iter` chains
// field-13 digests + field-13 `pinned_mirror_digests` + field-16
// entry digests; `broadcast_pinned_mirror_ack` fires strictly AFTER
// `drop(map)`. This test exercises the merged path with a notification
// carrying BOTH field-13 and field-16 payloads simultaneously and
// asserts:
//
//   (T1) Field-16 digests are registered in locality_map (under-action
//        of the locality side: same coverage as the pre-existing
//        `handle_blobs_available_pinned_mirror_entries_register_in_locality_map_test`
//        but additionally validates the chained iterator preserves
//        field-16 registration when field-13 is ALSO present — the
//        merged-iterator boundary case).
//
//   (T2) The dispatcher pin set's pre-pinned entries are removed by
//        the ack call (ack fired). Together with (T1) this exercises
//        BOTH side-effects of the merged exit path.
//
//   (T3) Field-13 digests are ALSO registered in locality_map (the
//        chained iterator preserves the field-13 registration when
//        field-16 is also present).
//
// Mutation falsification:
//   - Comment out the
//     `.chain(pinned_mirror_field16_digests.iter().copied())` clause
//     in `worker_api_server.rs` → (T1) red-fails with bespoke
//     "field-16 fold: merged register_blobs_iter dropped field-16".
//   - Comment out EITHER of the two
//     `dispatcher.broadcast_pinned_mirror_ack(entries)` calls inside
//     the consolidated-block exits → (T2) red-fails with bespoke
//     "field-16 fold: ack not fired post-fold; pin entries still held".
#[nativelink_test]
pub async fn handle_blobs_available_a2_fold_merged_field13_and_field16_test()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_store::small_blob_dispatcher::EphemeralServerSidePin;

    let cas_endpoint = "grpc://192.168.1.51:50081";
    let ctx = setup_api_server_with_locality_and_dispatcher(cas_endpoint, 4243u64).await?;

    // Field-16 (pinned_mirror_entries) digests. Pre-pin them so we can
    // observe ack removing them.
    let d16_a = DigestInfo::new([0xA1u8; 32], 1024);
    let d16_b = DigestInfo::new([0xA2u8; 32], 2048);
    // Field-13 (digests) — chained into the same consolidated
    // register_blobs_iter call post-fold.
    let d13_a = DigestInfo::new([0xB1u8; 32], 512);

    // Pre-pin the field-16 entries in the cas pin set so we can detect
    // when the ack call removes them.
    let cas_pin: Arc<EphemeralServerSidePin> = ctx._cas_pin.clone();
    cas_pin.insert(d16_a, Bytes::from_static(&[0u8; 1024]))?;
    cas_pin.insert(d16_b, Bytes::from_static(&[0u8; 2048]))?;
    assert!(cas_pin.contains(&d16_a), "pre-condition: d16_a pinned");
    assert!(cas_pin.contains(&d16_b), "pre-condition: d16_b pinned");

    // Send ONE BlobsAvailable carrying BOTH field-13 digests AND
    // field-16 pinned_mirror_entries. This is the merged path the
    // A2 fold collapses into a single locality_map.write().
    ctx.worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: String::new(),
            digests: vec![d13_a.into()],
            is_full_snapshot: false,
            evicted_digests: vec![],
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![
                MirrorPinEntry {
                    digest: Some(d16_a.into()),
                    store_id: "cas".to_string(),
                },
                MirrorPinEntry {
                    digest: Some(d16_b.into()),
                    store_id: "cas".to_string(),
                },
            ],
            pinned_ac_mirror_entries: Vec::new(),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "Error sending blobs available: {e}"))?;

    // Bounded poll: BOTH the locality_map MUST contain all three
    // digests (d13_a + d16_a + d16_b) AND the pin set MUST have
    // released d16_a, d16_b (ack fired). 2s deadline shared with the
    // sibling under-action test below.
    let locality_map = ctx.locality_map.clone();
    let cas_pin_poll = cas_pin.clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let map = locality_map.read();
            let has_d13 = !map.lookup_workers(&d13_a).is_empty();
            let has_d16a = !map.lookup_workers(&d16_a).is_empty();
            let has_d16b = !map.lookup_workers(&d16_b).is_empty();
            drop(map);
            let pin_released =
                !cas_pin_poll.contains(&d16_a) && !cas_pin_poll.contains(&d16_b);
            if has_d13 && has_d16a && has_d16b && pin_released {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#168 A2 fold: merged register_blobs_iter dropped field-16 \
         OR ack not fired post-fold; pin entries still held. \
         Within 2s the consolidated locality_map.write() must register \
         BOTH field-13 (`digests`) AND field-16 (`pinned_mirror_entries`) \
         digests; broadcast_pinned_mirror_ack must fire strictly AFTER \
         drop(map) so the pin set releases the pre-pinned field-16 \
         entries.",
    );

    Ok(())
}

// =====================================================================
// #387: worker-flap detection on rapid boot_epoch reconnects
// =====================================================================
//
// `worker_api_server.rs:520-660` already wipes per-endpoint state when
// a worker reconnects with a fresh `boot_epoch_id` (new process took
// over the same `cas_endpoint`). That tells the server "the old
// process died" but does NOT distinguish a one-off crash from a tight
// restart loop — and an OOM-restart-looping worker keeps flipping
// epochs while the operator only sees per-action SIGKILL noise.
//
// #387 adds a sliding-window flap counter (3 epoch changes within
// 5 min) held in a SEPARATE `flap_history` map keyed by
// `cas_endpoint` (its own `parking_lot::Mutex`, distinct from
// `endpoint_state`'s lock — the distributed-systems-reviewer
// `.claude/reviews/387-first-pass/...` BLOCK-FIX-FIRST showed that
// folding flap state into `EndpointState` made the detector silent
// in the OOM-loop pattern because disconnect-cleanup wipes
// `endpoint_state` between every reconnect). The `flap_history` map
// is NEVER touched by disconnect cleanup, so the deque accumulates
// across reconnects regardless of `endpoint_state` lifecycle.
// Counter `worker_flap_warns_total` rides on `WorkerApiMetrics`; a
// 60-second cooldown suppresses re-warn so a sustained 1-per-minute
// flap doesn't drown the log.
//
// Invariant: when the same `cas_endpoint` undergoes
// `FLAP_THRESHOLD` boot_epoch changes within `FLAP_WINDOW`, exactly
// one `worker_flap_warns_total` increment fires. The next change
// within `FLAP_COOLDOWN` MUST NOT re-increment; a change after the
// cooldown MUST re-increment.
//
// Mutation step (per CLAUDE.md TDD step 5): comment out the
// `self.metrics.worker_flap_warns_total.fetch_add(1, Ordering::Relaxed)`
// line in `worker_api_server.rs` — this test MUST then panic with
// the specific assertion message
// "worker-flap warn missing — operator-blind to whole-process restart loop".

/// Mutable-time `NowFn` factory for #387 flap tests. Test calls
/// `set_now_secs(N)` to advance the clock between connect_worker
/// invocations so we can exercise the sliding window AND the cooldown
/// gate deterministically (no `tokio::time::sleep`, no real-time
/// dependency — per CLAUDE.md "no thread::sleep as synchronization").
#[derive(Clone)]
struct MockClock {
    now_secs: Arc<core::sync::atomic::AtomicU64>,
}

impl MockClock {
    fn new(initial: u64) -> Self {
        Self {
            now_secs: Arc::new(core::sync::atomic::AtomicU64::new(initial)),
        }
    }

    fn set_now_secs(&self, secs: u64) {
        self.now_secs
            .store(secs, core::sync::atomic::Ordering::Relaxed);
    }

    fn now_fn(&self) -> NowFn {
        let clk = self.now_secs.clone();
        Box::new(move || {
            Ok(Duration::from_secs(
                clk.load(core::sync::atomic::Ordering::Relaxed),
            ))
        })
    }
}

/// Build a `WorkerApiServer` whose `now_fn` returns a clock value the
/// test can drive. Returns the server + the clock handle. Mirrors
/// `setup_multi_connect()` but injects a controllable clock so the
/// flap window + cooldown can be exercised without real time.
async fn setup_multi_connect_with_clock(
    clock: MockClock,
) -> Result<(WorkerApiServer, MockClock), Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        clock.now_fn(),
        [1u8; 6],
        Some(locality_map),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;
    Ok((worker_api_server, clock))
}

/// Three boot_epoch flips within `FLAP_WINDOW` (300s) MUST fire
/// exactly one `worker_flap_warns_total` increment. A fourth flip
/// inside the 60s cooldown MUST NOT re-fire. Advancing past the
/// cooldown and flipping again MUST re-fire.
///
/// Seams crossed: `inner_connect_worker` epoch-wipe branch
/// (`worker_api_server.rs:580-660`) → `EndpointState.epoch_changes`
/// deque → cooldown check → `WorkerApiMetrics.worker_flap_warns_total`
/// AtomicU64. The metric counter is the operator-visible signal
/// (warn line is the load-bearing signal in journald per
/// `.claude/audits/384-exec-log-2026-05-10/server-worker-oom-detection.md`
/// Q2; counter is belt-and-suspenders for when the metrics exporter
/// scrapes worker_api group).
#[nativelink_test]
pub async fn worker_flap_detection_fires_warn_and_respects_cooldown_test()
-> Result<(), Box<dyn core::error::Error>> {
    use core::sync::atomic::Ordering;

    let cas_endpoint = "grpc://192.168.1.99:50081";
    let clock = MockClock::new(1_000_000);
    let (server, clock) = setup_multi_connect_with_clock(clock).await?;
    let metrics = server.metrics();

    // Helper: open a stream with the given epoch at the current mock
    // time. Drops both ends immediately so connection state is left
    // for the next call without holding background tasks open.
    async fn flip(
        server: &WorkerApiServer,
        cas_endpoint: &str,
        epoch: u64,
    ) -> Result<(), Error> {
        let (tx, stream) = open_worker_connection(server, cas_endpoint, epoch).await?;
        drop(tx);
        drop(stream);
        Ok(())
    }

    // First connect — establishes baseline; needs_wipe == false on
    // first-ever connect, so no flap entry recorded yet.
    clock.set_now_secs(1_000_000);
    flip(&server, cas_endpoint, 1).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "first connect must not fire flap warn — no prior epoch to differ from"
    );

    // Two epoch changes — still below threshold (FLAP_THRESHOLD = 3).
    clock.set_now_secs(1_000_010);
    flip(&server, cas_endpoint, 2).await?;
    clock.set_now_secs(1_000_020);
    flip(&server, cas_endpoint, 3).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "two flips within window must not yet fire — threshold is 3, observed 2"
    );

    // Third epoch change — len == 3 == FLAP_THRESHOLD, fire.
    clock.set_now_secs(1_000_030);
    flip(&server, cas_endpoint, 4).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        1,
        "worker-flap warn missing — operator-blind to whole-process restart loop \
         (3 epoch flips within FLAP_WINDOW must fire exactly one warn at threshold \
         crossover)"
    );

    // Fourth flip inside cooldown — MUST NOT re-fire.
    clock.set_now_secs(1_000_050); // 20s after last warn, well below 60s cooldown
    flip(&server, cas_endpoint, 5).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        1,
        "flap warn re-fired inside FLAP_COOLDOWN — cooldown gate broken; \
         a 1/min sustained flap would drown the log"
    );

    // Advance past the cooldown and flip again — MUST re-fire.
    // last_flap_warn_at was set at 1_000_030; cooldown is 60s, so any
    // now >= 1_000_090 lifts the gate. Use 1_000_100 for headroom.
    // All four prior flips are still inside the 300s window
    // (oldest at 1_000_000 + 1_000_010, both within 300s of 1_000_100
    // — but the first one at t=1_000_010 falls out at t > 1_000_310).
    // So at t=1_000_100 the deque carries [1_000_010, 1_000_020,
    // 1_000_030, 1_000_050, 1_000_100] -> 5 entries, well above
    // threshold; cooldown now lifted -> warn fires.
    clock.set_now_secs(1_000_100);
    flip(&server, cas_endpoint, 6).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        2,
        "flap warn must re-fire after FLAP_COOLDOWN elapses — sustained flapping \
         deserves periodic re-notification (otherwise a stuck-in-loop worker would \
         go silent after the first 60s)"
    );

    Ok(())
}

/// Sliding-window eviction: a flip that's older than `FLAP_WINDOW`
/// (300s) MUST fall out of the deque so a slow drumbeat of epoch
/// changes (one every 200s) never accumulates to threshold.
#[nativelink_test]
pub async fn worker_flap_window_evicts_stale_entries_test()
-> Result<(), Box<dyn core::error::Error>> {
    use core::sync::atomic::Ordering;

    let cas_endpoint = "grpc://192.168.1.100:50081";
    let clock = MockClock::new(2_000_000);
    let (server, clock) = setup_multi_connect_with_clock(clock).await?;
    let metrics = server.metrics();

    async fn flip(
        server: &WorkerApiServer,
        cas_endpoint: &str,
        epoch: u64,
    ) -> Result<(), Error> {
        let (tx, stream) = open_worker_connection(server, cas_endpoint, epoch).await?;
        drop(tx);
        drop(stream);
        Ok(())
    }

    // First connect — baseline, no flap entry.
    clock.set_now_secs(2_000_000);
    flip(&server, cas_endpoint, 1).await?;

    // Flip every 400s (well past 300s window). Each push evicts the
    // prior entry, so deque len stays at 1 -> never reaches threshold.
    for i in 0..10u64 {
        clock.set_now_secs(2_000_000 + (i + 1) * 400);
        flip(&server, cas_endpoint, 2 + i).await?;
    }
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "slow-drumbeat epoch flips (one every 400s, beyond FLAP_WINDOW=300s) must \
         never accumulate to threshold — sliding-window eviction broken"
    );
    Ok(())
}

/// Same-epoch reconnects (transient stream drops, not whole-process
/// restarts) MUST NOT count against the flap threshold. Otherwise
/// network jitter would falsely trigger flap warns.
#[nativelink_test]
pub async fn worker_flap_same_epoch_reconnects_dont_count_test()
-> Result<(), Box<dyn core::error::Error>> {
    use core::sync::atomic::Ordering;

    let cas_endpoint = "grpc://192.168.1.101:50081";
    let clock = MockClock::new(3_000_000);
    let (server, clock) = setup_multi_connect_with_clock(clock).await?;
    let metrics = server.metrics();

    // First connect.
    clock.set_now_secs(3_000_000);
    let (tx1, stream1) = open_worker_connection(&server, cas_endpoint, 42).await?;
    drop(tx1);
    drop(stream1);

    // Five reconnects at the SAME epoch — these are transient stream
    // drops; needs_wipe == false, so flap deque stays empty.
    for i in 0..5u64 {
        clock.set_now_secs(3_000_001 + i);
        let (tx, stream) = open_worker_connection(&server, cas_endpoint, 42).await?;
        drop(tx);
        drop(stream);
    }
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "same-epoch reconnects must not count as flaps — those are transient \
         stream drops, not whole-process restarts"
    );
    Ok(())
}

/// BLOCK-FIX regression for distributed-systems-reviewer's
/// `.claude/reviews/387-first-pass/distributed-systems-reviewer.md`
/// finding: the OOM-loop pattern wipes `endpoint_state` between every
/// reconnect (worker dies → kernel RST → server disconnect-cleanup
/// runs `state.remove(&cas_endpoint)` in ms while no new connection
/// has arrived). If the flap deque lives inside `EndpointState`,
/// every disconnect-cleanup wipes it and the detector is silent on
/// the exact pattern it was built to surface.
///
/// This test FORCES the disconnect-cleanup task to completion between
/// every connect (by polling `endpoint_state_is_empty_for_testing`
/// inside a `tokio::time::timeout` deadlock-detector) before the next
/// connect. After three such fully-cleaned-up flips with rising
/// boot_epoch values, the flap detector MUST still observe the
/// threshold crossing and fire exactly one warn — because the new
/// `flap_history` map is held in a SEPARATE `Mutex` from
/// `endpoint_state` and is NOT touched by disconnect cleanup.
///
/// Mutation step: in `worker_api_server.rs`, fold the
/// `last_seen_epoch / epoch_changes / last_flap_warn_at` fields back
/// into `EndpointState` and source them via `prev.as_ref().map(...)`
/// in `inner_connect_worker`. This test MUST then panic with
/// "flap detector wiped by disconnect cleanup — operator-blind to
/// OOM-loop pattern".
#[nativelink_test]
pub async fn worker_flap_detection_survives_disconnect_cleanup_test()
-> Result<(), Box<dyn core::error::Error>> {
    use core::sync::atomic::Ordering;

    let cas_endpoint = "grpc://192.168.1.102:50081";
    let clock = MockClock::new(4_000_000);
    let (server, clock) = setup_multi_connect_with_clock(clock).await?;
    let metrics = server.metrics();

    // Flip helper that closes the stream and drives the per-connection
    // background task's disconnect-cleanup to completion BEFORE
    // returning. Production OOM-loop pattern: worker dies, kernel RST,
    // disconnect cleanup runs, only THEN the worker relaunches. By
    // polling `endpoint_state_is_empty_for_testing` we force this
    // ordering deterministically — no `tokio::time::sleep` as
    // synchronization (per CLAUDE.md). 5s timeout doubles as a
    // deadlock detector if the background task never runs.
    async fn flip_with_cleanup(
        server: &WorkerApiServer,
        cas_endpoint: &str,
        epoch: u64,
    ) -> Result<(), Error> {
        let (tx, stream) = open_worker_connection(server, cas_endpoint, epoch).await?;
        drop(tx);
        drop(stream);
        // Wait until the per-connection background task has run its
        // `state.remove(&cas_endpoint)` at the bottom of
        // `WorkerConnection::start`'s async block.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if server.endpoint_state_is_empty_for_testing(cas_endpoint) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            make_err!(
                tonic::Code::DeadlineExceeded,
                "disconnect-cleanup background task did not run within 5s — \
                 production OOM-loop pattern cannot be reproduced"
            )
        })?;
        Ok(())
    }

    // First connect: establishes `last_seen_epoch` in `flap_history`.
    // No flap push yet (first ever).
    clock.set_now_secs(4_000_000);
    flip_with_cleanup(&server, cas_endpoint, 1).await?;
    assert!(
        server.endpoint_state_is_empty_for_testing(cas_endpoint),
        "endpoint_state must be empty after first cleanup — test invariant"
    );
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "first connect must not fire (no prior epoch in flap_history yet)"
    );

    // Second connect: epoch flips 1 → 2. `flap_history.last_seen_epoch`
    // sourced from the (separate) map, NOT from `endpoint_state`
    // which was wiped by the cleanup above. Push #1.
    clock.set_now_secs(4_000_010);
    flip_with_cleanup(&server, cas_endpoint, 2).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "below threshold (1/3) — must not fire"
    );

    // Third connect: epoch 2 → 3. Push #2.
    clock.set_now_secs(4_000_020);
    flip_with_cleanup(&server, cas_endpoint, 3).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        0,
        "below threshold (2/3) — must not fire"
    );

    // Fourth connect: epoch 3 → 4. Push #3 → threshold crossover.
    // BEFORE the fix this would NOT fire: each prior connect's
    // disconnect-cleanup wiped the deque, so the new connect sees
    // `prev = None` → `needs_wipe = false` → push gated out.
    clock.set_now_secs(4_000_030);
    flip_with_cleanup(&server, cas_endpoint, 4).await?;
    assert_eq!(
        metrics.worker_flap_warns_total.load(Ordering::Relaxed),
        1,
        "flap detector wiped by disconnect cleanup — operator-blind to OOM-loop \
         pattern (3 epoch flips with full disconnect-cleanup between each must \
         still fire one warn at threshold crossover; the production OOM-loop \
         pattern hits the disconnect-cleanup path BEFORE the next connect \
         arrives, so flap state held in EndpointState — wiped by the cleanup's \
         state.remove — leaves the detector silent in the exact scenario it \
         was built to surface)"
    );

    Ok(())
}
