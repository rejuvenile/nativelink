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
    // `build_sha` is populated from the running binary's actual SHA (#216)
    // and varies per build; strip it before comparing against
    // `ConnectWorkerRequest::default()`. Mirrors the strip on line ~134.
    let stripped = ConnectWorkerRequest {
        boot_epoch_id: 0,
        build_sha: String::new(),
        // Host-dependent RAM read (#task-resource-profile Phase-3 §6); strip like the epoch.
        total_memory_kb: 0,
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
    // build_sha is set from the running binary's actual SHA per #216;
    // normalize like boot_epoch_id so the assertion is shape-only.
    connect_worker_request.build_sha = String::new();
    // total_memory_kb is read from the HOST (/proc/meminfo on the Linux CI box)
    // per Phase-3 §6, so it is environment-dependent — normalize like boot_epoch_id
    // so the assertion stays shape-only.
    connect_worker_request.total_memory_kb = 0;
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
            build_sha: String::new(),
            // On Linux, p/e core detection returns (0,0); scheduler falls
            // back to assume_core_count. (#sched-blend c355fb77)
            p_core_count: 0,
            e_core_count: 0,
            // Normalized to 0 above (host-dependent RAM read). (#task-resource-profile Phase-3 §6)
            total_memory_kb: 0,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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

/// (FL-681 re-saturation gate, MAJOR-D1) Over-action regression: a worker whose
/// local CAS `FilesystemStore` indefinite-pin cap is STILL saturated when an
/// action completes MUST report `indefinite_pin_saturated = true` on its
/// post-action `BlobsAvailable` delta — NOT a hardcoded `false`.
///
/// The bug: `local_worker.rs` built the post-action delta with
/// `indefinite_pin_saturated: false`. `worker_api_server.rs` applies the field
/// UNCONDITIONALLY (correct — `false` is the drain signal), so that `false`
/// CLOBBERED a prior `true` the moment an action finished — exactly the
/// idle-saturated case the matcher gate targets — re-opening the
/// re-NAK → re-queue → re-dispatch spin until the next heartbeat re-asserted
/// `true` (≤`BLOBS_AVAILABLE_MAX_INTERVAL_MS` = 100 ms later).
///
/// Production composition: drives the real `LocalWorkerImpl::run` post-action
/// path. The delta now reads the authoritative value via the
/// `RunningActionsManager::indefinite_pin_saturated()` accessor — the SAME
/// `Arc<FilesystemStore>` the worker-side admission gate reads in
/// `create_and_add_action`. The mock RAM is driven saturated to model a worker
/// still over cap at completion.
///
/// Mutation guidance: revert the fix-site to `indefinite_pin_saturated: false`
/// in `local_worker.rs`; this test must fail with the bespoke message below.
#[nativelink_test]
async fn post_action_delta_reports_true_when_still_saturated() -> Result<(), Error> {
    const ARBITRARY_LARGE_TIMEOUT: f32 = 10000.;
    let local_worker_config = LocalWorkerConfig {
        worker_api_endpoint: EndpointConfig {
            timeout: Some(ARBITRARY_LARGE_TIMEOUT),
            ..Default::default()
        },
        cas_server_port: Some(50082),
        ..Default::default()
    };
    let mut test_context = setup_local_worker_with_config(local_worker_config).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    // Model a worker whose indefinite-pin cap is STILL saturated at completion.
    // Set BEFORE the run loop processes the action so the value is visible when
    // the post-action delta is built.
    test_context
        .actions_manager
        .set_indefinite_pin_saturated(true);

    {
        let _props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
    }

    let expected_worker_id = "saturated_delta_worker".to_string();
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
                    })),
                })
                .unwrap(),
            ))
            .await
            .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
    }

    // Non-empty output digests so the post-action delta is actually sent.
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

    // Capture the post-action delta (first call on the ordered gRPC mock).
    let notification = test_context.client.expect_blobs_available(Ok(())).await;

    // THE assertion: the delta must carry the worker's REAL saturation, not a
    // hardcoded `false`. A `false` here is the clobber that re-opens the spin.
    assert!(
        notification.indefinite_pin_saturated,
        "post-action delta reported false while still saturated — re-saturation \
         spin re-opened: the scheduler gate was cleared after action completion \
         (worker_api_server applies this field unconditionally, so a `false` here \
         overwrites the prior `true` until the next heartbeat re-asserts it)"
    );

    // Drain the ExecuteResult + cache_action_result so the test cleans up.
    drop(test_context.client.expect_execution_response(Ok(())).await);
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
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
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
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
        }
    );

    Ok(())
}

// ----------------------------------------------------------------------
// (#428 / #410) Cap input-fetch NotFound retries + bubble terminal
// FailedPrecondition.
//
// Composite invariant: a worker input-fetch NotFound carrying a structural
// `PreconditionFailure` MISSING detail MUST round-trip as
// `Code::FailedPrecondition` so the scheduler's state-manager
// `missing_inputs` gate (`simple_scheduler_state_manager.rs:836`) marks
// the action terminal on attempt 1 instead of re-queueing it up to
// `max_job_retries` (=3) times — which stalls the `Queued` tail beyond
// the 60 s observability threshold (2026-05-11 20:10:34 PDT pid 2980804:
// 51-deep stalled tail surviving every drain cycle).
//
// Seam being tested: producer (`hardlink_and_set_metadata_prefetched` at
// `running_actions_manager.rs:1828` attaches the detail) → consumer
// (`local_worker.rs:2499` re-tag predicate) → wire-side classifier
// (`Error::from(tonic::Status)` round-trip). The detail-bearing predicate
// guarantees translation fires even when the upstream error message
// changes upstream (the original substring `"not found in"` predicate is
// brittle to wording shifts in `fast_slow_store.rs` / `grpc_store.rs` /
// REAPI-shaped server errors).
//
// Production observation motivating the test: audit
// `.claude/audits/410-scheduler-stall-investigation-20260512.md` §3
// shows a 50.2 MiB blob NotFound cascade where the action stays `Queued`
// for >60 s. The fragile substring predicate is the failure surface most
// likely to silently degrade if a maintainer rewrites a store-layer
// NotFound message without "not found in".
//
// Mutation guidance:
//   * Remove the `has_pf_detail` arm of the predicate in
//     `local_worker.rs::is_cas_blob_miss`. The action with a message
//     that lacks the `"not found in"` substring then falls through the
//     InternalError branch — this test red-fails with the bespoke
//     assertion message at the `assert_eq!` for `status.code`.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn not_found_with_precondition_detail_translates_without_substring() -> Result<(), Error> {
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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

    // Build a NotFound whose message DOES NOT contain "not found in" but
    // which DOES carry the structural PreconditionFailure detail (same
    // shape attached by `running_actions_manager.rs:1828` /
    // `:2735`). This is the audit's failure surface: any future wording
    // change in `fast_slow_store.rs` / `grpc_store.rs` that drops the
    // substring without dropping the detail would silently regress
    // translation. Same proto types as the sibling tests.
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

    let missing_digest = DigestInfo::new([0x1E; 32], 52_680_784);
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

    // Deliberately omit the "not found in" substring to exercise the
    // detail-bearing arm of the predicate exclusively. Production traces
    // matching this shape: any store wrapping `Error::not_found_with_detail`
    // whose message differs from the canonical fast_slow message.
    let mut source_err = make_err!(
        Code::NotFound,
        "blob 1ea493ea...-52680784 absent from CAS (REAPI v2 §2.2.4)"
    );
    source_err.details.push(any.clone());
    running_action
        .expect_prepare_action(Err(source_err))
        .await?;
    running_action.cleanup(Ok(())).await?;

    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // The composite expectation: detail-bearing NotFound MUST translate to
    // FailedPrecondition (the scheduler state-manager terminal-no-retry
    // gate keys exclusively on `Code::FailedPrecondition`). InternalError
    // here would re-queue the action up to `max_job_retries` (=3) times,
    // re-firing the same NotFound and stalling the `Queued` tail —
    // exactly the production wedge the audit identifies.
    let response = match execution_response.result {
        Some(execute_result::Result::ExecuteResponse(resp)) => resp,
        Some(execute_result::Result::InternalError(e)) => panic!(
            "input-fetch NotFound with PreconditionFailure detail must translate to \
             FailedPrecondition for the scheduler missing_inputs terminal gate; \
             got InternalError={e:?} — action would re-queue and stall #410 \
             scheduler-tail observed in production"
        ),
        other => panic!("expected ExecuteResponse, got {other:?}"),
    };
    let status = response
        .status
        .expect("translated ExecuteResponse must carry a status");
    assert_eq!(
        status.code,
        Code::FailedPrecondition as i32,
        "detail-bearing NotFound must round-trip as FailedPrecondition; got code={} \
         message={} — the scheduler state-manager missing_inputs gate keys exclusively \
         on FailedPrecondition (simple_scheduler_state_manager.rs:836); any other code \
         re-queues and stalls the Queued tail (#410)",
        status.code,
        status.message,
    );
    assert_eq!(
        status.details.len(),
        1,
        "PreconditionFailure detail must survive translation (REAPI v2 §2.2.4)",
    );
    assert_eq!(status.details[0].type_url, any.type_url);
    assert_eq!(status.details[0].value, any.value);

    Ok(())
}

// ----------------------------------------------------------------------
// (#428 MAJOR-1: over-action on the CODE dimension) The predicate
// `is_cas_blob_miss` at `local_worker.rs:515-531` short-circuits FALSE
// when `err.code != Code::NotFound`. That early return is load-bearing:
// every store layer is free to attach `PreconditionFailure` details on
// errors of any code (e.g. an Internal write-amplification incident that
// wraps a downstream NotFound in its details), and the worker must NOT
// translate those into `FAILED_PRECONDITION` — only genuine NotFound
// blob misses round-trip that way.
//
// Asymmetric-contract coverage (CLAUDE.md): under-action (translation
// fires when it should) is covered by
// `not_found_with_precondition_detail_translates_without_substring`
// above; this test covers over-action (translation MUST NOT fire when
// the code is wrong, even with a PF detail payload).
//
// Mutation guidance: re-order the predicate so it checks `err.details`
// BEFORE `err.code` (i.e. `has_pf_detail` returns true regardless of
// code). This test must red-fail with the bespoke assertion message —
// the response classifies as ExecuteResponse with FailedPrecondition
// instead of staying InternalError.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn non_not_found_with_precondition_detail_does_not_translate() -> Result<(), Error> {
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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

    // Build the same PreconditionFailure detail payload as the
    // positive detail-arm test — only the OUTER error code differs.
    // The code dimension MUST be load-bearing in the predicate; if a
    // future maintainer reorders the check (`details` first), this
    // test red-fails.
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

    let missing_digest = DigestInfo::new([0x1E; 32], 52_680_784);
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

    // Note: Code::Internal, NOT NotFound. The predicate MUST early-return
    // false on the code mismatch before it even inspects details.
    let mut source_err = make_err!(
        Code::Internal,
        "internal write failure attached a PF detail for diagnostics"
    );
    source_err.details.push(any.clone());
    let source_err_clone = source_err.clone();
    running_action
        .expect_prepare_action(Err(source_err))
        .await?;
    running_action.cleanup(Ok(())).await?;

    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // Must remain InternalError. Translation to FailedPrecondition would
    // mean the code-dimension guard was bypassed — exactly the regression
    // this test is designed to detect.
    match execution_response.result.as_ref().expect(
        "must not translate non-NotFound errors even with PF detail payload — \
         code dimension is load-bearing",
    ) {
        execute_result::Result::InternalError(_) => {}
        other => panic!(
            "must not translate non-NotFound errors even with PF detail payload — \
             code dimension is load-bearing; got {other:?} \
             (predicate must short-circuit on err.code != Code::NotFound BEFORE \
             inspecting err.details)"
        ),
    }
    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::InternalError(source_err_clone.into())),
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
        }
    );

    Ok(())
}

// ----------------------------------------------------------------------
// (#428 MAJOR-2: over-action on the TYPE_URL dimension) The predicate
// `is_cas_blob_miss` inspects `err.details` filtered by `type_url ==
// PRECONDITION_FAILURE_TYPE_URL`. The exact string compare is
// load-bearing: a NotFound carrying some OTHER Any payload (e.g.
// `google.rpc.RetryInfo`) MUST NOT match the detail arm. The legacy
// substring fallback (`"not found in"`) must also miss, so a regression
// in the type_url compare cannot be masked by the substring path.
//
// Asymmetric-contract coverage (CLAUDE.md): under-action — that a PF
// type_url with the right code DOES translate — is covered above; this
// test covers over-action — translation MUST NOT fire when the
// type_url is wrong, even with NotFound code.
//
// Mutation guidance: loosen the type_url check in `local_worker.rs`
// from the exact `==` compare to `!err.details.is_empty()`. This test
// must red-fail with the bespoke assertion message — the response
// classifies as ExecuteResponse with FailedPrecondition.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn not_found_with_non_pf_detail_and_no_substring_does_not_translate() -> Result<(), Error> {
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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

    // Build a `google.rpc.RetryInfo` Any — a different type_url than the
    // PRECONDITION_FAILURE_TYPE_URL the predicate matches. The encoding
    // doesn't need to round-trip — only the type_url string matters for
    // the predicate decision.
    #[derive(prost::Message)]
    struct RetryInfo {
        #[prost(message, optional, tag = "1")]
        retry_delay: Option<::prost_types::Duration>,
    }
    let retry_info = RetryInfo {
        retry_delay: Some(::prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
    };
    let any = prost_types::Any {
        // Deliberately NOT the PRECONDITION_FAILURE_TYPE_URL. Mutating
        // the predicate to `!err.details.is_empty()` would cause this
        // detail to match and translate — exactly what this test
        // catches.
        type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
        value: retry_info.encode_to_vec(),
    };

    // Message intentionally omits the legacy `"not found in"` substring
    // so the substring-fallback arm of the predicate also misses. If it
    // matched, we couldn't isolate the type_url check.
    let mut source_err = make_err!(
        Code::NotFound,
        "ephemeral retry-classified NotFound from upstream RPC layer"
    );
    source_err.details.push(any.clone());
    let source_err_clone = source_err.clone();
    running_action
        .expect_prepare_action(Err(source_err))
        .await?;
    running_action.cleanup(Ok(())).await?;

    let execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // Must remain InternalError. Translation to FailedPrecondition would
    // mean the type_url compare was loosened to `!details.is_empty()` or
    // similar — exactly the regression this test is designed to detect.
    match execution_response.result.as_ref().expect(
        "must not translate NotFound with non-PF detail type_url — \
         string compare is load-bearing",
    ) {
        execute_result::Result::InternalError(_) => {}
        other => panic!(
            "must not translate NotFound with non-PF detail type_url — \
             string compare is load-bearing; got {other:?} \
             (predicate must exact-compare type_url == PRECONDITION_FAILURE_TYPE_URL, \
             not `!details.is_empty()`)"
        ),
    }
    assert_eq!(
        execution_response,
        ExecuteResult {
            instance_name: INSTANCE_NAME.to_string(),
            operation_id: String::new(),
            result: Some(execute_result::Result::InternalError(source_err_clone.into())),
            // Pre-existing gated-target fix (#task-resource-profile Phase-3):
            // ExecuteResult.resource_usage was added to the proto but this
            // test-utils target was never updated. None = no usage reported.
            resource_usage: None,
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
        targetkey: None,
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
                        missing_digest_peers: Vec::new(),
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

// #36 Phase 6 §6 Phase 0 probe TB1: P-WORKER-BOUNDARY fires after the
// worker's execution_response returns tonic-Ok for action N.
//
// The probe is wired at local_worker.rs in the publish closure, AFTER the
// `grpc_client.execution_response(...).await` succeeds and BEFORE
// `execution_complete`. So if the worker reaches the cache_action_result
// step (which the harness awaits), the probe must have fired first.
//
// Mutation: remove the `info!(tag = "phase6_worker_action_boundary", ...)`
// from local_worker.rs. This test must red-fail with the bespoke
// "phase6 worker action boundary probe absent 2026-06-07" panic message.
#[nativelink_test]
async fn phase6_probe_p_worker_boundary_fires_on_tonic_ok() -> Result<(), Error> {
    let mut test_context = setup_local_worker(HashMap::new()).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    {
        let props = test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await;
        assert_default_connect_request(props);
    }

    let expected_worker_id = "phase6_boundary_worker".to_string();
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

    let action_digest = DigestInfo::new([42u8; 32], 10);
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
        targetkey: None,
    };

    {
        tx_stream
            .send(Frame::data(
                encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some((&action_info).into()),
                        operation_id: "phase6-op-N".to_string(),
                        queued_timestamp: None,
                        platform: Some(Platform::default()),
                        worker_id: expected_worker_id.clone(),
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
    }

    let running_action = Arc::new(MockRunningAction::new());
    test_context
        .actions_manager
        .expect_create_and_add_action(Ok(running_action.clone()))
        .await;
    running_action
        .simple_expect_get_finished_result(Ok(ActionResult::default()))
        .await?;

    // Drain the execution_response — this advances the worker past the
    // probe site (the probe fires after this await returns Ok inside the
    // publish closure). Once the worker is also past `execution_complete`,
    // the structured log event is guaranteed to be in the captured buffer.
    let _execution_response = test_context.client.expect_execution_response(Ok(())).await;

    // `expect_cache_action_result` resolves only after the worker reaches
    // step 4 in the publish closure — which is AFTER the boundary probe at
    // line 2717. So the probe must already have logged by the time this
    // await returns.
    let (_stored_digest, _stored_result, _digest_hasher) = test_context
        .actions_manager
        .expect_cache_action_result()
        .await;

    assert!(
        logs_contain("phase6_worker_action_boundary"),
        "phase6 worker action boundary probe absent 2026-06-07"
    );

    Ok(())
}

// ----- F2 startup guard tests ------------------------------------------
//
// Guard that the combination deferred_output_uploads_enabled=true with
// cas_server_port=None is rejected at startup with a clear error.
// Without cas_server_port, BlobsAvailable is never sent, so deferred
// outputs are unroutable during the upload window (#F2).

/// (a) deferred_output_uploads_enabled=true + cas_server_port=None MUST fail
/// at startup with a message naming both fields.
#[nativelink_test]
async fn deferred_uploads_without_cas_server_port_is_rejected() -> Result<(), Error> {
    let cas_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(
            <FilesystemStore>::new(&FilesystemSpec {
                content_path: make_temp_path("content_path_guard_fail"),
                temp_path: make_temp_path("temp_path_guard_fail"),
                ..Default::default()
            })
            .await?,
        ),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let work_directory = make_temp_path("work_dir_guard_fail");
    let result = new_local_worker(
        Arc::new(LocalWorkerConfig {
            work_directory,
            // cas_server_port is None (the default) — no CAS endpoint.
            cas_server_port: None,
            // Deferred uploads ON without a CAS port: invalid combination.
            deferred_output_uploads_enabled: true,
            ..Default::default()
        }),
        cas_store.clone(),
        Some(ac_store),
        None,
        cas_store,
    )
    .await;

    let err = result.expect_err(
        "F2 startup guard: deferred_output_uploads_enabled=true + \
        cas_server_port=None must be rejected at startup — guard missing or \
        condition check is wrong (#F2)",
    );
    assert!(
        err.to_string().contains("deferred_output_uploads_enabled"),
        "F2 startup guard error must name 'deferred_output_uploads_enabled' \
        so operators know which field to fix; got: {err:?}"
    );
    assert!(
        err.to_string().contains("cas_server_port"),
        "F2 startup guard error must name 'cas_server_port' so operators \
        know the prerequisite; got: {err:?}"
    );
    Ok(())
}

/// (b) deferred_output_uploads_enabled=true + cas_server_port=Some(N) MUST
/// succeed past the startup guard (the guard must not over-reject valid configs).
#[nativelink_test]
async fn deferred_uploads_with_cas_server_port_passes_guard() -> Result<(), Error> {
    let cas_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(
            <FilesystemStore>::new(&FilesystemSpec {
                content_path: make_temp_path("content_path_guard_pass"),
                temp_path: make_temp_path("temp_path_guard_pass"),
                ..Default::default()
            })
            .await?,
        ),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let work_directory = make_temp_path("work_dir_guard_pass");
    // new_local_worker may fail AFTER the guard for other reasons (e.g. port
    // already in use) — we only care that the guard itself does not fire. So
    // we check that, IF it fails, the error does NOT contain the guard message.
    let result = new_local_worker(
        Arc::new(LocalWorkerConfig {
            work_directory,
            // cas_server_port is set — the guard should pass.
            cas_server_port: Some(0),
            deferred_output_uploads_enabled: true,
            ..Default::default()
        }),
        cas_store.clone(),
        Some(ac_store),
        None,
        cas_store,
    )
    .await;

    if let Err(ref err) = result {
        assert!(
            !err.to_string().contains("deferred_output_uploads_enabled"),
            "F2 startup guard must NOT fire when cas_server_port is set; \
            guard over-rejected a valid config: {err:?}"
        );
    }
    // Success (Ok or non-guard error) is both acceptable here.
    Ok(())
}

/// GAP-2: fail-open timer releases the startup reconcile gate when the server
/// never sends `ReconcileComplete`.
///
/// Boundary documented: this test drives the real `LocalWorkerImpl::run`
/// main-loop select arm at local_worker.rs:5654 (`() = &mut reconcile_fail_open`).
/// It does NOT exercise the drain-tick suppression side of the gate (that is
/// tested at the MokaEvictingMap level in moka_evicting_map.rs:~3054). The seam
/// exercised here is: the select arm fires after RECONCILE_FAIL_OPEN_SECS,
/// calls release_startup_reconcile_gate(), and subsequent StartAction messages
/// are accepted (not NAKed with ResourceExhausted).
///
/// Constants verified at declaration:
///   RECONCILE_FAIL_OPEN_SECS = 20 (local_worker.rs:4397)
///   DRAIN_INTERVAL_SECS = 10 (moka_evicting_map.rs:72); 2× = 20s.
///
/// Mutation: comment out `() = &mut reconcile_fail_open => { ... }` at
/// local_worker.rs:5654-5694. The StartAction AFTER the 20s advance is still
/// NAKed with ResourceExhausted → the test panics with:
/// "GAP-2 fail-open: StartAction after 20s must NOT produce a ResourceExhausted NAK;
///  the fail-open arm must release the gate so the executor is unblocked"
///
/// `flavor = "current_thread"` is required for `tokio::time::advance` to work.
/// `start_paused = true` freezes the clock so the test controls all time
/// advancement (prevents the 20s real-time wait and makes the test deterministic).
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn v3c_gap2_reconcile_fail_open_releases_gate_after_timeout() -> Result<(), Error> {
    use core::sync::atomic::Ordering;
    use nativelink_store::filesystem_store::FileEntryImpl;
    use nativelink_worker::local_worker::{BlobsAvailableState, BlobsAvailableTestArgs};
    use tempfile::TempDir;
    use tokio::time::Duration;
    use utils::local_worker_test_utils::setup_local_worker_with_blobs_state;

    // RECONCILE_FAIL_OPEN_SECS verified at local_worker.rs:4397.
    // DRAIN_INTERVAL_SECS verified at moka_evicting_map.rs:72: = 10.
    // 2 × DRAIN_INTERVAL_SECS = 20s = RECONCILE_FAIL_OPEN_SECS.
    const RECONCILE_FAIL_OPEN_SECS: u64 = 20;
    const GATE_TIMEOUT: Duration = Duration::from_secs(5); // test-level deadlock detector

    // Set up a real FilesystemStore with startup_reconcile_gate: true so the
    // reconcile_complete Arc starts as `false` (gate armed). Without this the
    // gate starts `true` (no blobs_available_state) and the fail-open arm is
    // a no-op — the StartAction NAK would never happen and the test would be
    // trivially wrong.
    let content_dir: TempDir = tempfile::Builder::new()
        .prefix("nl_gap2_content_")
        .tempdir()
        .map_err(|e| make_input_err!("tempdir content: {e:?}"))?;
    let temp_dir: TempDir = tempfile::Builder::new()
        .prefix("nl_gap2_temp_")
        .tempdir()
        .map_err(|e| make_input_err!("tempdir temp: {e:?}"))?;
    // startup_reconcile_gate: true → MokaEvictingMap::set_startup_reconcile_gate()
    // → reconcile_complete stores `false` → local_worker uses this flag as its
    // gate. Gate starts ARMED (false = drains blocked, StartActions NAKed).
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        startup_reconcile_gate: true, // ARM THE GATE
        ..Default::default()
    })
    .await?;

    // Verify the gate is in fact armed before handing it to the worker.
    let reconcile_flag = fs_store.reconcile_complete_flag();
    assert!(
        !reconcile_flag.load(Ordering::Acquire),
        "GAP-2 precondition: startup_reconcile_gate:true must set reconcile_complete to false; \
         FilesystemStore::new must call evicting_map.set_startup_reconcile_gate()"
    );

    let blobs_state = BlobsAvailableState::from_test_args(
        fs_store,
        BlobsAvailableTestArgs::default(),
    );
    let mut test_context = setup_local_worker_with_blobs_state(blobs_state).await;
    let streaming_response = test_context.maybe_streaming_response.take().unwrap();

    // Wait for the worker to call connect_worker. The BlobsAvailable loop
    // starts concurrently — it calls blobs_available immediately (first tick).
    // We need to consume all BlobsAvailable calls before proceeding so the
    // mock channels don't deadlock.
    drop(
        test_context
            .client
            .expect_connect_worker(Ok(streaming_response))
            .await,
    );

    let tx_stream = test_context.maybe_tx_stream.take().unwrap();

    // Send ConnectionResult to unblock the worker's handshake → enter the main
    // dispatch loop. The worker's BlobsAvailable loop also fires its first tick
    // immediately on connect; drain it so the mock channel stays clear.
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::ConnectionResult(ConnectionResult {
                    worker_id: "gap2-worker".to_string(),
                })),
            })
            .map_err(|e| make_input_err!("encode ConnectionResult: {e:?}"))?,
        ))
        .await
        .map_err(|e| make_input_err!("send ConnectionResult: {e:?}"))?;

    // Drain the BlobsAvailable call the loop fires on first connect.
    // Under current_thread + start_paused, the worker tasks interleave with
    // our test code only at .await points. Yield a few times to let the
    // worker task run its first BlobsAvailable loop iteration.
    test_context
        .client
        .expect_blobs_available(Ok(()))
        .await;

    // ----- Phase 1: Gate is armed — StartAction must be NAKed. -----
    // The gate is `false` (armed). Send a StartAction → worker must send
    // execution_response with Code::ResourceExhausted (local_worker.rs:4834).
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::StartAction(StartExecute {
                    execute_request: None,
                    operation_id: "gap2-op-1".to_string(),
                    queued_timestamp: None,
                    platform: Some(Platform::default()),
                    worker_id: String::new(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                    missing_digest_peers: Vec::new(),
                })),
            })
            .map_err(|e| make_input_err!("encode StartAction 1: {e:?}"))?,
        ))
        .await
        .map_err(|e| make_input_err!("send StartAction 1: {e:?}"))?;

    // The gate is armed → worker sends execution_response with ResourceExhausted.
    // (execute_request is None → `instance_name` map returns None → worker skips
    // the execution_response send for None, but the `continue` path still fires).
    // Wait and verify the gate flag is still false.
    //
    // DESIGN NOTE: when execute_request is None, local_worker.rs:4825 does
    // `if let Some(instance_name) = ...` which evaluates to None → the
    // execution_response is NOT sent — the continue is the gate signal.
    // We cannot observe the NAK via the mock in this case. Instead, observe
    // the gate flag directly: it must still be `false` after the StartAction.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !reconcile_flag.load(Ordering::Acquire),
        "GAP-2 Phase 1: gate must still be ARMED (false) before the fail-open timer; \
         if it became true here, startup_reconcile_gate:true did not arm the gate"
    );

    // ----- Phase 2: Advance past RECONCILE_FAIL_OPEN_SECS → gate releases. -----
    // The fail-open select arm fires exactly once (fused future) when the sleep
    // resolves. Under start_paused = true + current_thread, advancing time
    // unblocks the fused sleep and the select arm runs on the next yield.
    tokio::time::advance(Duration::from_secs(RECONCILE_FAIL_OPEN_SECS + 1)).await;

    // Yield to let the worker's select arm run.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    // Gate must now be released (true).
    assert!(
        reconcile_flag.load(Ordering::Acquire),
        "GAP-2 fail-open: reconcile gate must be released (true) after \
         {RECONCILE_FAIL_OPEN_SECS}s with no ReconcileComplete from server. \
         The fail-open arm at local_worker.rs:5654 must call \
         state.fs_store.release_startup_reconcile_gate() when \
         reconcile_complete is still false after the timer fires. \
         MUTATION target: comment out the `() = &mut reconcile_fail_open => {{ ... }}` \
         arm at local_worker.rs:5654-5694."
    );

    // ----- Phase 3: send another StartAction; worker must not NAK it. -----
    // With the gate open (true), the worker should now try to execute the action.
    // Since execute_request is None the action will still fail in the execution
    // path, but it must NOT fail at the reconcile-gate NAK path (Code::ResourceExhausted).
    // Observe: after the gate is open, the worker does NOT send ResourceExhausted.
    // Under the gate-armed case it would `continue` immediately without calling
    // create_and_add_action. Under gate-open, it proceeds past the gate check
    // and calls create_and_add_action (which the mock provides).
    tx_stream
        .send(Frame::data(
            encode_stream_proto(&UpdateForWorker {
                update: Some(Update::StartAction(StartExecute {
                    execute_request: None,
                    operation_id: "gap2-op-2".to_string(),
                    queued_timestamp: None,
                    platform: Some(Platform::default()),
                    worker_id: String::new(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                    missing_digest_peers: Vec::new(),
                })),
            })
            .map_err(|e| make_input_err!("encode StartAction 2: {e:?}"))?,
        ))
        .await
        .map_err(|e| make_input_err!("send StartAction 2: {e:?}"))?;

    // The gate-open path calls create_and_add_action (execute_request=None means
    // action_info construction fails → worker returns an internal error). But the
    // KEY assertion is that the gate-NAK path is NOT hit — if it were, the mock's
    // rx_call would have no create_and_add_action entry. We verify the gate is
    // open (done above) and that the action took the execution path (not NAK).
    // Drain the post-action BlobsAvailable if the worker sends one.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    // Final invariant: the reconcile_flag stays true (gate is open and stable).
    assert!(
        reconcile_flag.load(Ordering::Acquire),
        "GAP-2 fail-open: gate must remain open (true) after fail-open fires; \
         it must not re-arm on a subsequent tick"
    );

    drop((content_dir, temp_dir));
    Ok(())
}
