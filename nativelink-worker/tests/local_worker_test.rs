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
use std::env;
use std::ffi::OsString;
use std::io::Write;
#[cfg(target_family = "unix")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

mod utils {
    pub(crate) mod local_worker_test_utils;
    pub(crate) mod mock_running_actions_manager;
}

use hyper::body::Frame;
use nativelink_config::cas_server::{EndpointConfig, LocalWorkerConfig, WorkerProperty};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err, make_input_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Platform;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectWorkerRequest, ConnectionResult, ExecuteResult, KillOperationRequest, StartExecute,
    UpdateForWorker, execute_result,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionUniqueKey, ActionUniqueQualifier,
    ExecutionMetadata, OperationId,
};
use nativelink_util::common::{DigestInfo, encode_stream_proto, fs};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::Store;
use nativelink_worker::local_worker::new_local_worker;
#[cfg(target_family = "unix")]
use nativelink_worker::local_worker::preconditions_met;
use pretty_assertions::assert_eq;
use prost::Message;
use rand::Rng;
use utils::local_worker_test_utils::{
    setup_grpc_stream, setup_local_worker, setup_local_worker_with_config,
};
use utils::mock_running_actions_manager::MockRunningAction;

const INSTANCE_NAME: &str = "foo";

/// Asserts that the captured `ConnectWorkerRequest` matches the default
/// shape (no properties, empty endpoint, etc.) IGNORING the worker's
/// `boot_epoch_id`. The epoch is process-lifetime random per #141 so
/// it cannot be hard-coded; the test still verifies it is populated
/// (non-zero).
fn assert_default_connect_request(actual: ConnectWorkerRequest) {
    assert_ne!(
        actual.boot_epoch_id, 0,
        "worker must populate boot_epoch_id (#141)"
    );
    let stripped = ConnectWorkerRequest {
        boot_epoch_id: 0,
        ..actual
    };
    assert_eq!(stripped, ConnectWorkerRequest::default());
}

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

#[nativelink_test]
#[cfg_attr(feature = "nix", ignore)]
async fn platform_properties_smoke_test() -> Result<(), Error> {
    let mut platform_properties = HashMap::new();
    platform_properties.insert(
        "foo".to_string(),
        WorkerProperty::Values(vec!["bar1".to_string(), "bar2".to_string()]),
    );
    platform_properties.insert(
        "baz".to_string(),
        // Note: new lines will result in two entries for same key.
        #[cfg(target_family = "unix")]
        WorkerProperty::QueryCmd("printf 'hello\ngoodbye'".to_string()),
        #[cfg(target_family = "windows")]
        WorkerProperty::QueryCmd("cmd /C \"echo hello && echo goodbye\"".to_string()),
    );
    let mut test_context = setup_local_worker(platform_properties).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    // Now wait for our client to send `.connect_worker()` (which has our platform properties).
    let mut connect_worker_request = test_context
        .client
        .expect_connect_worker(Ok(streaming_response))
        .await;
    // It is undefined which order these will be returned in, so we sort it.
    connect_worker_request
        .properties
        .sort_by_key(Message::encode_to_vec);
    // boot_epoch_id is generated lazily at process start (#141) and
    // therefore non-zero. Verify it then strip it before comparing to
    // the structural expected request.
    assert_ne!(
        connect_worker_request.boot_epoch_id, 0,
        "worker must populate boot_epoch_id (#141)"
    );
    connect_worker_request.boot_epoch_id = 0;
    assert_eq!(
        connect_worker_request,
        ConnectWorkerRequest {
            worker_id_prefix: String::new(),
            properties: vec![
                Property {
                    name: "baz".to_string(),
                    value: "hello".to_string(),
                },
                Property {
                    name: "baz".to_string(),
                    value: "goodbye".to_string(),
                },
                Property {
                    name: "foo".to_string(),
                    value: "bar1".to_string(),
                },
                Property {
                    name: "foo".to_string(),
                    value: "bar2".to_string(),
                }
            ],
            max_inflight_tasks: 0,
            cas_endpoint: String::new(),
            boot_epoch_id: 0,
        }
    );

    Ok(())
}

#[nativelink_test]
async fn reconnect_on_server_disconnect_test() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    // Disconnect our grpc stream.
    drop(test_context.maybe_tx_stream.take().unwrap());

    {
        // Client should try to auto reconnect and check our properties again.
        let (_, streaming_response) = setup_grpc_stream();
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    Ok(())
}

#[nativelink_test]
async fn kill_all_called_on_disconnect() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    // Handle registration (kill_all not called unless registered).
    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: "foobar".to_string(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    // Disconnect our grpc stream.
    drop(tx_stream);

    // Check that kill_all is called.
    test_context.actions_manager.expect_kill_all().await;

    Ok(())
}

#[nativelink_test]
async fn blake3_digest_function_registered_properly() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        // First initialize our worker by sending the response to the connection request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
        command_digest: DigestInfo::new([1u8; 32], 10),
        input_root_digest: DigestInfo::new([2u8; 32], 10),
        timeout: Duration::from_secs(1),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Blake3,
            digest: action_digest,
        }),
    };

    {
        // Send execution request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }
    let running_action = Arc::new(MockRunningAction::new());

    // Send and wait for response from create_and_add_action to RunningActionsManager.
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Now the RunningAction needs to send a series of state updates. This shortcuts them
    // into a single call (shortcut for prepare, execute, upload, collect_results, cleanup).
    running_action
        .simple_expect_get_finished_result(Ok(ActionResult::default()))
        .await?;

    // Expect the action to be updated in the action cache.
    let (_stored_digest, _stored_result, digest_hasher) = test_context
        .actions_manager
        .expect_cache_action_result()
        .await;
    assert_eq!(digest_hasher, DigestHasherFunc::Blake3);

    Ok(())
}

#[nativelink_test]
async fn simple_worker_start_action_test() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        // First initialize our worker by sending the response to the connection request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        // Send execution request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }
    let action_result = ActionResult {
        output_files: vec![],
        output_folders: vec![],
        output_file_symlinks: vec![],
        output_directory_symlinks: vec![],
        exit_code: 5,
        stdout_digest: DigestInfo::new([21u8; 32], 10),
        stderr_digest: DigestInfo::new([22u8; 32], 10),
        execution_metadata: ExecutionMetadata {
            worker: expected_worker_id.clone(),
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
        server_logs: HashMap::new(),
        error: None,
        message: String::new(),
    };
    let running_action = Arc::new(MockRunningAction::new());

    // Send and wait for response from create_and_add_action to RunningActionsManager.
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Now the RunningAction needs to send a series of state updates. This shortcuts them
    // into a single call (shortcut for prepare, execute, upload, collect_results, cleanup).
    running_action
        .simple_expect_get_finished_result(Ok(action_result.clone()))
        .await?;

    // Expect the action to be updated in the action cache.
    let (stored_digest, stored_result, digest_hasher) = test_context
        .actions_manager
        .expect_cache_action_result()
        .await;
    assert_eq!(stored_digest, action_digest);
    assert_eq!(stored_result, action_result.clone());
    assert_eq!(digest_hasher, DigestHasherFunc::Sha256);

    // Now our client should be notified that our runner finished.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // Now ensure the final results match our expectations.
    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::ExecuteResponse(
                ActionStage::Completed(action_result).into()
            )),
        }
    );

    Ok(())
}

/// External-consistency invariant: by the time the client sees an action's
/// `ExecuteResult`, every blob digest the action produced MUST already be
/// observable from the server (either in the server's CAS or registered in
/// the locality_map so the server can proxy-fetch from the worker).
///
/// The server populates its `locality_map` from `BlobsAvailableNotification`
/// messages from the worker. The worker→scheduler stream is a single
/// bidirectional gRPC channel that the server processes in arrival order
/// (`worker_api_server.rs` per-stream `while let Some(maybe_update)
/// connection.next().await` loop). So if the worker sends `ExecuteResult`
/// BEFORE `BlobsAvailable`, the server forwards the result to the client
/// and STILL has not registered the worker as holder for those digests.
/// A client reading any output blob inside that race window can hit a
/// `NotFound` because (a) the slow-tier upload is fire-and-forget and may
/// not have completed yet, and (b) the locality_map has no peer yet.
///
/// This test asserts the worker sends `BlobsAvailable` BEFORE
/// `ExecuteResult` on the same stream, so server-side sequential
/// processing guarantees the locality_map is populated before the
/// client sees the result.
#[nativelink_test]
async fn worker_sends_blobs_available_before_execute_result_test() -> Result<(), Error> {
    const ARBITRARY_LARGE_TIMEOUT: f32 = 10000.;
    // cas_server_port must be Some(_) so the worker has an advertised CAS
    // endpoint and actually sends BlobsAvailable. With None the worker
    // skips the BlobsAvailable send entirely (peer-fetch disabled) and
    // there is nothing to order.
    let local_worker_config = LocalWorkerConfig {
        worker_api_endpoint: EndpointConfig {
            timeout: Some(ARBITRARY_LARGE_TIMEOUT),
            ..Default::default()
        },
        cas_server_port: Some(50081),
        ..Default::default()
    };
    let mut test_context = setup_local_worker_with_config(local_worker_config).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        let _props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
    }

    let expected_worker_id = "ordering_test_worker".to_string();
    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    // Action result with stdout/stderr so that BlobsAvailable's output_digests
    // is non-empty and the worker actually sends the notification.
    let action_result = ActionResult {
        output_files: vec![],
        output_folders: vec![],
        output_file_symlinks: vec![],
        output_directory_symlinks: vec![],
        exit_code: 0,
        stdout_digest: DigestInfo::new([21u8; 32], 10),
        stderr_digest: DigestInfo::new([22u8; 32], 10),
        execution_metadata: ExecutionMetadata {
            worker: expected_worker_id.clone(),
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
        server_logs: HashMap::new(),
        error: None,
        message: String::new(),
    };
    let running_action = Arc::new(MockRunningAction::new());

    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    running_action
        .simple_expect_get_finished_result(Ok(action_result.clone()))
        .await?;

    // The KEY assertion: BlobsAvailable must be the FIRST call seen by the
    // grpc client mock — before ExecuteResult. The mock's call channel is
    // ordered (FIFO mpsc), so calling `expect_blobs_available` first will
    // receive the first call made; if ExecuteResult was made first, the
    // expect_blobs_available helper will panic with "expected
    // BlobsAvailable, got: ExecutionResponse(...)".
    let notification = test_context.client.expect_blobs_available(Ok(())).await;
    // Sanity: contains the stdout + stderr digests.
    assert!(
        notification
            .digests
            .iter()
            .any(|d| d.hash == DigestInfo::new([21u8; 32], 10).packed_hash().to_string()),
        "Expected BlobsAvailable to include stdout digest, got: {:?}",
        notification.digests,
    );
    assert!(
        notification
            .digests
            .iter()
            .any(|d| d.hash == DigestInfo::new([22u8; 32], 10).packed_hash().to_string()),
        "Expected BlobsAvailable to include stderr digest, got: {:?}",
        notification.digests,
    );

    // After the ordering invariant: ExecuteResult comes second.
    // cache_action_result is also called somewhere in this flow, but goes
    // through the running_actions_manager mock channel, not the gRPC mock.
    // We don't assert ordering against it here.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;
    assert_eq!(
        execution_response.operation_id,
        String::new(),
    );

    // Drain cache_action_result so the test cleans up gracefully.
    drop(
        test_context
            .actions_manager
            .expect_cache_action_result()
            .await,
    );

    Ok(())
}

#[nativelink_test]
async fn new_local_worker_creates_work_directory_test() -> Result<(), Error> {
    let cas_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            // Note: These are not needed for this test, so we put dummy memory stores here.
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(
            <FilesystemStore>::new(&FilesystemSpec {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                ..Default::default()
            })
            .await?,
        ),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let work_directory = make_temp_path("foo");
    new_local_worker(
        Arc::new(LocalWorkerConfig {
            work_directory: work_directory.clone(),
            ..Default::default()
        }),
        cas_store.clone(),
        Some(ac_store),
        None,
        cas_store,
    )
    .await?;

    assert!(
        fs::metadata(work_directory).await.is_ok(),
        "Expected work_directory to be created"
    );

    Ok(())
}

#[nativelink_test]
async fn new_local_worker_removes_work_directory_before_start_test() -> Result<(), Error> {
    let cas_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            // Note: These are not needed for this test, so we put dummy memory stores here.
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(
            <FilesystemStore>::new(&FilesystemSpec {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                ..Default::default()
            })
            .await?,
        ),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let work_directory = make_temp_path("foo");
    fs::create_dir_all(format!("{}/{}", work_directory, "another_dir")).await?;
    let mut file =
        fs::create_file(OsString::from(format!("{}/{}", work_directory, "foo.txt"))).await?;
    Write::write_all(file.as_std_mut(), b"Hello, world!")
        .map_err(|e| Into::<Error>::into(e))?;
    file.as_std().sync_all()
        .map_err(|e| Into::<Error>::into(e))?;
    drop(file);
    new_local_worker(
        Arc::new(LocalWorkerConfig {
            work_directory: work_directory.clone(),
            ..Default::default()
        }),
        cas_store.clone(),
        Some(ac_store),
        None,
        cas_store,
    )
    .await?;

    let work_directory_path_buf = PathBuf::from(work_directory);

    assert!(
        work_directory_path_buf.read_dir()?.next().is_none(),
        "Expected work_directory to have removed all files and to be empty"
    );

    Ok(())
}

#[nativelink_test]
async fn experimental_precondition_script_fails() -> Result<(), Error> {
    #[cfg(target_family = "unix")]
    const EXPECTED_MSG: &str = "Preconditions script returned status exit status: 1 - ";
    #[cfg(target_family = "windows")]
    const EXPECTED_MSG: &str = "Preconditions script returned status exit code: 1 - ";

    let temp_path = make_temp_path("scripts");
    fs::create_dir_all(temp_path.clone()).await?;
    #[cfg(target_family = "unix")]
    let precondition_script = {
        let precondition_script = format!("{temp_path}/precondition.sh");
        let precondition_script_tmp = format!("{precondition_script}.tmp");

        // We use std::fs::File here because we sometimes get strange bugs here
        // that result in: "Text file busy (os error 26)" if it is an executable.
        // It is likely because somewhere the file descriptor does not get closed
        // in tokio's async context.
        {
            // We write to a temporary file and then rename it to force the kernel
            // to flush all related file descriptors fully before we use it.
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o777)
                .open(OsString::from(&precondition_script_tmp))
                .unwrap();
            file.write_all(b"#!/bin/sh\nexit 1\n").unwrap();
            file.sync_all().unwrap();
            // Note: Github runners appear to use some kind of filesystem driver
            // that does not sync data as expected. This is the easiest solution.
            // See: https://github.com/pantsbuild/pants/issues/10507
            // See: https://github.com/moby/moby/issues/9547
            std::process::Command::new("sync").output().unwrap();
        }
        std::fs::rename(&precondition_script_tmp, &precondition_script).unwrap();
        // Add a small delay to ensure the file system has fully released the file
        // This helps avoid "Text file busy" errors on some Linux environments
        tokio::time::sleep(Duration::from_millis(100)).await;
        precondition_script
    };
    #[cfg(target_family = "windows")]
    let precondition_script = {
        let precondition_script = format!("{}/precondition.bat", temp_path);
        let mut file = std::fs::File::create(OsString::from(&precondition_script))?;
        file.write_all(b"@echo off\r\nexit 1")?;
        file.sync_all().unwrap();
        precondition_script
    };

    let local_worker_config = LocalWorkerConfig {
        experimental_precondition_script: Some(precondition_script),
        ..Default::default()
    };

    let mut test_context = setup_local_worker_with_config(local_worker_config).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        // First initialize our worker by sending the response to the connection request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        // Send execution request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    // Now our client should be notified that our runner finished.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // Now ensure the final results match our expectations.
    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::InternalError(
                make_err!(Code::ResourceExhausted, "{}", EXPECTED_MSG,).into()
            )),
        }
    );

    Ok(())
}

#[nativelink_test]
async fn kill_action_request_kills_action() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;

    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    // Handle registration (kill_all not called unless registered).
    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
        command_digest: DigestInfo::new([1u8; 32], 10),
        input_root_digest: DigestInfo::new([2u8; 32], 10),
        timeout: Duration::from_secs(1),
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            instance_name: INSTANCE_NAME.to_string(),
            digest_function: DigestHasherFunc::Blake3,
            digest: action_digest,
        }),
    };

    let operation_id = OperationId::default();
    {
        // Send execution request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: operation_id.to_string(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }
    let running_action = Arc::new(MockRunningAction::new());

    // Send and wait for response from create_and_add_action to RunningActionsManager.
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    {
        // Send kill request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::KillOperationRequest(KillOperationRequest {
                        operation_id: operation_id.to_string(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let killed_operation_id = test_context.actions_manager.expect_kill_operation().await;

    // Make sure that the killed action is the one we intended
    assert_eq!(killed_operation_id, operation_id);

    Ok(())
}

#[nativelink_test]
async fn cas_not_found_returns_failed_precondition_test() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let running_action = Arc::new(MockRunningAction::new());

    // Send and wait for response from create_and_add_action.
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Simulate prepare_action failing with a CAS NotFound error containing the
    // specific "not found in either fast or slow store" message. This is the exact
    // condition that the code checks to decide whether to return FailedPrecondition.
    running_action
        .expect_prepare_action(Err(make_err!(
            Code::NotFound,
            "Hash 0123456789abcdef not found in either fast or slow store"
        )))
        .await?;

    // Cleanup is still called even when prepare_action fails.
    running_action.cleanup(Ok(())).await?;

    // The worker should respond with FailedPrecondition wrapped in an ExecuteResponse,
    // NOT an InternalError. This allows Bazel to re-upload the missing artifacts.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    let expected_action_result = ActionResult {
        error: Some(make_err!(
            Code::FailedPrecondition,
            "Hash 0123456789abcdef not found in either fast or slow store"
        )),
        ..ActionResult::default()
    };
    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::ExecuteResponse(
                ActionStage::Completed(expected_action_result).into()
            )),
        }
    );

    Ok(())
}

#[nativelink_test]
async fn cas_not_found_translation_preserves_details_test() -> Result<(), Error> {
    // REAPI v2 §2.2.4: a FAILED_PRECONDITION returned to Bazel for a missing
    // blob MUST carry the corresponding google.rpc.PreconditionFailure detail
    // (with a MISSING violation) so Bazel can re-upload the blob and recover.
    // The worker's NotFound→FailedPrecondition translation must preserve any
    // details attached to the source error rather than dropping them on the
    // floor by going through `make_err!`.
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let running_action = Arc::new(MockRunningAction::new());
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Build a PreconditionFailure detail (MISSING violation) and attach it
    // to a NotFound error — same shape produced by the input-fetch path.
    // The proto types are defined locally to mirror the worker's helper
    // (`make_precondition_failure_any` in running_actions_manager.rs).
    #[derive(prost::Message)]
    struct PfViolation {
        #[prost(string, tag = "1")]
        r#type: String,
        #[prost(string, tag = "2")]
        subject: String,
        #[prost(string, tag = "3")]
        description: String,
    }
    #[derive(prost::Message)]
    struct PfFailure {
        #[prost(message, repeated, tag = "1")]
        violations: Vec<PfViolation>,
    }

    let missing_digest = DigestInfo::new([0xAB; 32], 42);
    let detail = PfFailure {
        violations: vec![PfViolation {
            r#type: "MISSING".into(),
            subject: format!(
                "blobs/{}/{}",
                missing_digest.packed_hash(),
                missing_digest.size_bytes()
            ),
            description: String::new(),
        }],
    };
    let any = prost_types::Any {
        type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
        value: detail.encode_to_vec(),
    };
    let mut source_err = make_err!(
        Code::NotFound,
        "Hash abababab not found in either fast or slow store"
    );
    source_err.details.push(any.clone());

    running_action.expect_prepare_action(Err(source_err)).await?;
    running_action.cleanup(Ok(())).await?;

    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    let response = match execution_response.result {
        Some(execute_result::Result::ExecuteResponse(resp)) => resp,
        other => panic!("expected ExecuteResponse, got {other:?}"),
    };
    // ExecuteResponse.status is the google.rpc.Status that carries the
    // PreconditionFailure detail to Bazel (REAPI v2 §2.2.4).
    let status = response.status.expect("ExecuteResponse missing status");
    assert_eq!(
        status.code,
        Code::FailedPrecondition as i32,
        "expected FAILED_PRECONDITION, got {}",
        status.code,
    );
    assert_eq!(
        status.details.len(),
        1,
        "PreconditionFailure detail must survive NotFound→FailedPrecondition translation",
    );
    assert_eq!(status.details[0].type_url, any.type_url);
    assert_eq!(status.details[0].value, any.value);

    Ok(())
}

#[nativelink_test]
async fn non_cas_not_found_returns_internal_error_test() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let running_action = Arc::new(MockRunningAction::new());

    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Simulate prepare_action failing with a NotFound error that does NOT contain
    // the CAS-specific message. This should result in an InternalError, not
    // FailedPrecondition.
    let other_not_found_error = make_err!(Code::NotFound, "Some other resource was not found");
    running_action
        .expect_prepare_action(Err(other_not_found_error.clone()))
        .await?;

    // Cleanup is still called even when prepare_action fails.
    running_action.cleanup(Ok(())).await?;

    // The worker should respond with InternalError since this is not a CAS blob miss.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::InternalError(
                other_not_found_error.into()
            )),
        }
    );

    Ok(())
}

#[cfg(target_family = "unix")]
#[nativelink_test]
async fn preconditions_met_extra_envs() -> Result<(), Error> {
    let mut extra_envs = HashMap::new();
    extra_envs.insert("DEMO_ENV".into(), "test_value_for_demo_env".into());

    // So we have bash for nix cases, because the PATH gets reset
    extra_envs.insert("PATH".into(), env::var("PATH").unwrap());

    preconditions_met(Some("bash -c \"echo $DEMO_ENV\"".to_string()), &extra_envs).await?;
    assert!(logs_contain("test_value_for_demo_env"));
    Ok(())
}

#[nativelink_test]
async fn worker_translates_not_found_to_failed_precondition_test() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        // Ensure our worker connects and properties were sent.
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "foobar".to_string();

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();
    {
        // First initialize our worker by sending the response to the connection request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let action_digest = DigestInfo::new([3u8; 32], 10);
    let action_info = ActionInfo {
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
    };

    {
        // Send execution request.
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: String::new(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),

                        missing_digests: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    let running_action = Arc::new(MockRunningAction::new());

    // Send and wait for response from create_and_add_action to RunningActionsManager.
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;

    // Build a PreconditionFailure detail (MISSING violation) to attach to
    // the source NotFound — same shape produced by store-layer NotFound
    // returns. Reviewer Finding 3: assert that the worker's
    // NotFound→FailedPrecondition translation does NOT drop these
    // details (REAPI v2 §2.2.4 — Bazel needs the violation to know
    // which blob to re-upload).
    #[derive(prost::Message)]
    struct PfViolation {
        #[prost(string, tag = "1")]
        r#type: String,
        #[prost(string, tag = "2")]
        subject: String,
        #[prost(string, tag = "3")]
        description: String,
    }
    #[derive(prost::Message)]
    struct PfFailure {
        #[prost(message, repeated, tag = "1")]
        violations: Vec<PfViolation>,
    }

    let missing_digest = DigestInfo::new([0xCD; 32], 99);
    let detail = PfFailure {
        violations: vec![PfViolation {
            r#type: "MISSING".into(),
            subject: format!(
                "blobs/{}/{}",
                missing_digest.packed_hash(),
                missing_digest.size_bytes(),
            ),
            description: String::new(),
        }],
    };
    let any = prost_types::Any {
        type_url: "type.googleapis.com/google.rpc.PreconditionFailure".into(),
        value: detail.encode_to_vec(),
    };

    // Make the action fail with a NotFound error during get_finished_result.
    // The "not found in" substring matches what production CAS-miss errors
    // look like (e.g. "Blob ... not found in inner store or any worker") and
    // is what `local_worker.rs` looks for to trigger REAPI translation.
    let mut source_err = make_err!(
        Code::NotFound,
        "Blob abc not found in inner store or any worker"
    );
    source_err.details.push(any.clone());
    running_action
        .simple_expect_get_finished_result(Err(source_err))
        .await?;

    // Now our client should be notified that our runner finished.
    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // The worker should have translated NotFound into FailedPrecondition per
    // the REAPI spec. Translation produces an ExecuteResponse whose status
    // carries the re-stamped code so Bazel's recovery path can re-upload.
    let execute_response = match execution_response.result {
        Some(execute_result::Result::ExecuteResponse(resp)) => resp,
        other => panic!("Expected ExecuteResponse result, got: {other:?}"),
    };

    let status = execute_response
        .status
        .expect("translated ExecuteResponse must carry a status");
    assert_eq!(
        status.code,
        Code::FailedPrecondition as i32,
        "Expected NotFound to be translated to FailedPrecondition, got message: {}",
        status.message
    );
    assert!(
        status.message.contains("not found in"),
        "Expected status message to preserve original 'not found in' context, got: {}",
        status.message
    );
    // Reviewer Finding 3: NotFound→FailedPrecondition translation in
    // `local_worker.rs` (≈line 1683) MUST preserve `e.details` rather
    // than rebuilding the error via `make_err!`. Translation re-stamps
    // the code in-place; this guards against a future regression that
    // drops details on the floor.
    assert_eq!(
        status.details.len(),
        1,
        "PreconditionFailure detail must survive NotFound→FailedPrecondition translation",
    );
    assert_eq!(status.details[0].type_url, any.type_url);
    assert_eq!(status.details[0].value, any.value);

    Ok(())
}


// ----------------------------------------------------------------------
// (#97) Production-composition test for the BIS chunked dispatch arm.
//
// This test addresses testing-czar requirement (a) from the #97 fixup
// pass: the existing `bis_chunk_handler_test.rs` calls
// `handle_bis_chunk` with a hand-rolled `ack_sink` closure, which
// bypasses two production layers:
//   1. The `Update::ChunkedMessage(BlobsInStableStorageChunk)`
//      envelope-decode arm in `LocalWorkerImpl::run`.
//   2. The `worker_api_client_wrapper::bis_ack` send.
//
// A bug in either layer (wrong oneof tag handling, dropped ack on the
// async-spawn boundary, missing token echo in the wire-level wrapper)
// would not surface from the bis_chunk_handler_test. This test feeds a
// real `BlobsInStableStorageChunk` through the live `Streaming<...>`
// channel and asserts the ack flows back through
// `worker_api_client_wrapper::bis_ack` with the correct
// (broadcast_id, sequence, server_instance_token) round-trip.
//
// Mutation guidance:
//   * Replace the `Some(chunked_message::Payload::BlobsInStableStorage(chunk))`
//     match arm in `local_worker.rs:1813-1864` with `_ => {}`. This test
//     MUST then time out at the `expect_bis_ack` step (no ack ever
//     fires) — the timeout's `expect("...")` message is the SPECIFIC
//     guard.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn bis_chunked_dispatch_arm_round_trips_ack() -> Result<(), Error> {
    use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        BlobsInStableStorageChunk, ChunkedMessage, chunked_message,
    };
    use nativelink_store::filesystem_store::FileEntryImpl;
    use nativelink_worker::local_worker::{BlobsAvailableState, BlobsAvailableTestArgs};
    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};
    use utils::local_worker_test_utils::setup_local_worker_with_blobs_state;

    // Set up a real FilesystemStore so the BlobsAvailableState has a
    // legit fs_store reference. The unpin path is exercised but the
    // stores have no entries — that's fine, unpin_digest is a no-op
    // on absent keys (FilesystemStore drops the lookup if nothing is
    // pinned at that key).
    let content_dir: TempDir = tempfile::Builder::new()
        .prefix("nl_bis_dispatch_content_")
        .tempdir()
        .map_err(|e| make_input_err!("tempdir: {e:?}"))?;
    let temp_dir: TempDir = tempfile::Builder::new()
        .prefix("nl_bis_dispatch_temp_")
        .tempdir()
        .map_err(|e| make_input_err!("tempdir: {e:?}"))?;
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        ..Default::default()
    })
    .await?;
    let blobs_state =
        BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    let mut test_context = setup_local_worker_with_blobs_state(blobs_state).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();
    let _ = test_context
        .client
        .expect_connect_worker(Ok(streaming_response))
        .await;

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();

    // Initialize via ConnectionResult so the worker's run loop
    // is in the dispatch state.
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::ConnectionResult(ConnectionResult {
                    worker_id: "bis-disp-worker".to_string(),
                })),
            })
            .map_err(|e| make_input_err!("encode connection result: {e:?}"))?,
        ))
        .await
        .map_err(|e| make_input_err!("send connection result: {e:?}"))?;

    // Real BlobsInStableStorageChunk through the live stream.
    let chunk = BlobsInStableStorageChunk {
        digests: vec![ProtoDigest {
            hash: "0".repeat(64),
            size_bytes: 0,
        }],
        broadcast_id: 0xCAFE_F00D,
        sequence: 7,
        is_last: true,
        server_instance_token: 0xDEAD_BEEF_DEAD_BEEF,
        store_id: String::new(),
    };
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::ChunkedMessage(ChunkedMessage {
                    payload: Some(chunked_message::Payload::BlobsInStableStorage(
                        chunk.clone(),
                    )),
                })),
            })
            .map_err(|e| make_input_err!("encode chunked: {e:?}"))?,
        ))
        .await
        .map_err(|e| make_input_err!("send chunked: {e:?}"))?;

    // Ack should appear on the mock client's recorded calls. The
    // dispatch arm fires the ack from a `tokio::spawn`'d task, so
    // wrap in a generous timeout — without this, a missing ack would
    // hang the test indefinitely instead of failing. The worker's
    // periodic BlobsAvailable loop also fires while we wait — the
    // helper auto-acks those so the test is robust to the
    // worker-loop's first-wakeup behavior.
    let ack = timeout(
        Duration::from_secs(5),
        test_context.client.expect_bis_ack_skipping_blobs_available(),
    )
    .await
    .expect(
        "must NOT time out waiting for BisAck — the worker's \
         Update::ChunkedMessage(BlobsInStableStorage) dispatch arm \
         must wire through worker_api_client_wrapper::bis_ack so \
         the server's per-worker resend buffer can release the slot",
    );

    assert_eq!(ack.broadcast_id, chunk.broadcast_id);
    assert_eq!(ack.sequence, chunk.sequence);
    assert_eq!(
        ack.server_instance_token, chunk.server_instance_token,
        "wrapper must round-trip the chunk's server_instance_token \
         into the ack (red-team #5: token validation requires the \
         exact value back)"
    );

    Ok(())
}
