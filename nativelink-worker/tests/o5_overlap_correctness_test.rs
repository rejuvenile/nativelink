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

//! O5 overlap correctness tests: verifies that running output-directory
//! preparation [C] concurrently with input download [B2] produces a correct
//! result when input-tree directories and output-path parents overlap.
//!
//! Design invariant: `inner_prepare_action` must correctly materialise both
//! input files and output parent directories even when a directory name
//! appears both in the input tree (`input_root_digest` sub-directories) and
//! as the parent of a declared `output_paths` or `output_files` entry.
//!
//! Contract:
//! - Success path (T1): shared-dir-in-input-tree-and-output-parent — both
//!   input files and the output parent directory exist after `prepare_action`.
//!   Verified by `prepare_action_shared_dir_in_input_and_output_succeeds`.
//! - Non-regression (T2): direct-use mode guard — when direct_use=true, the
//!   overlap must NOT be applied (tested by asserting prepare_action succeeds
//!   regardless; but we cannot easily test direct_use=true without a full
//!   DirectoryCache, so T2 is a normal-mode sanity check with no overlap
//!   assertions). Verified by `prepare_action_output_dirs_created_correctly`.
//!
//! Mutation verification (commit 2):
//! - Mutant: comment out the [C]∥[B2] restructure (revert to sequential) →
//!   T1 must STILL PASS (because the prereq fix makes it safe in both
//!   sequential and concurrent modes). The overlap is a correctness-AND-perf
//!   change; T1 verifies the correctness claim (shared parent is handled).
//!   A timing-based test would be needed to verify the concurrency gain, but
//!   that is excluded per task spec (no bench cell exists).

use core::time::Duration;
use std::env;
use std::sync::Arc;

use bytes::Bytes;

use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, DirectoryNode, ExecuteRequest, FileNode,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::OperationId;
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningAction, RunningActionsManager, RunningActionsManagerArgs,
    RunningActionsManagerImpl,
};
use rand::Rng;

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data,
    )
}

async fn setup_stores() -> Result<
    (
        Arc<FilesystemStore>,
        Arc<MemoryStore>,
        Arc<FastSlowStore>,
        Arc<MemoryStore>,
    ),
    Error,
> {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = MemorySpec::default();
    let fast_store = FilesystemStore::new(&fast_config).await?;
    let slow_store = MemoryStore::new(&slow_config);
    let ac_store = MemoryStore::new(&slow_config);
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config),
            slow: StoreSpec::Memory(slow_config),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast_store.clone()),
        Store::new(slow_store.clone()),
    );
    Ok((fast_store, slow_store, cas_store, ac_store))
}

/// T1: When the input tree has a subdirectory `src/` and the command declares
/// an output path `src/lib.rs`, both the input file materialisation AND the
/// output parent directory creation must succeed — even though both operations
/// try to create/access `work_dir/src/` concurrently under the O5 overlap.
///
/// This is the primary correctness regression test for O5's [C]∥[B2] restructure.
/// With the prereq fix (BFS mkdir AlreadyExists tolerance), the test passes
/// regardless of whether [C] or [B2] creates `work_dir/src/` first.
///
/// Mutation 2026-06-15: revert the BFS mkdir AlreadyExists tolerance (use
/// plain `fs::create_dir` without the AlreadyExists match) → this test MUST
/// red-fail with "prepare_action must succeed with shared-parent input/output dir".
#[nativelink_test]
async fn prepare_action_shared_dir_in_input_and_output_succeeds()
-> Result<(), Box<dyn core::error::Error>> {
    let (_fast_store, _slow_store, cas_store, ac_store) = setup_stores().await?;
    let root_action_directory = make_temp_path("root_action_dir");
    fs::create_dir_all(&root_action_directory).await?;

    // Build the CAS content: input tree has a `src/` subdirectory with a file.
    // Command declares output_paths = ["src/lib.rs"] — src/ is the shared parent.
    let file_content = b"hello from src";
    // Upload the file content.
    let file_content_digest = DigestInfo::new([0xABu8; 32], file_content.len() as u64);
    cas_store
        .as_ref()
        .update_oneshot(file_content_digest, Bytes::from_static(file_content))
        .await
        .err_tip(|| "uploading file content")?;

    // src/ subdirectory with one file.
    let src_dir = Directory {
        files: vec![FileNode {
            name: "input_file.c".to_string(),
            digest: Some(file_content_digest.into()),
            is_executable: false,
            node_properties: None,
        }],
        ..Default::default()
    };
    let src_digest = serialize_and_upload_message(
        &src_dir,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    // Root input directory: just the `src/` subdirectory.
    let root_dir = Directory {
        directories: vec![DirectoryNode {
            name: "src".to_string(),
            digest: Some(src_digest.into()),
        }],
        ..Default::default()
    };
    let input_root_digest = serialize_and_upload_message(
        &root_dir,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    // Command: output_paths includes "src/lib.rs" — src/ is also in the input tree.
    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: vec!["src/lib.rs".to_string()],
        ..Default::default()
    };
    let command_digest = serialize_and_upload_message(
        &command,
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

    let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
        RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: cas_store.clone(),
            ac_store: Some(Store::new(ac_store.clone())),
            ac_mirror_target: None,
            historical_store: Store::new(cas_store.clone()),
            upload_action_result_config:
                &nativelink_config::cas_server::UploadActionResultConfig {
                    upload_ac_results_strategy:
                        nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::from_secs(600),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            deferred_output_uploads_enabled: false,
        },
    )?);

    let execute_request = ExecuteRequest {
        action_digest: Some(action_digest.into()),
        ..Default::default()
    };
    let operation_id = OperationId::default().to_string();

    let running_action = running_actions_manager
        .create_and_add_action(
            "test_worker".to_string(),
            StartExecute {
                execute_request: Some(execute_request),
                operation_id,
                queued_timestamp: None,
                platform: action.platform.clone(),
                worker_id: "test_worker".to_string(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                missing_digests: Vec::new(),
                missing_digest_peers: Vec::new(),
            },
        )
        .await?;

    // prepare_action runs inner_prepare_action, which includes the O5 overlap.
    // With the BFS prereq fix, this must succeed even when [C] and [B2] both
    // try to create work_dir/src/ (the shared parent).
    let prepared = tokio::time::timeout(
        Duration::from_secs(30),
        running_action.clone().prepare_action(),
    )
    .await
    .expect("prepare_action must not deadlock (30s detector)")
    .map_err(|e| {
        format!("prepare_action must succeed with shared-parent input/output dir: {e:?}")
    })
    .unwrap();

    let work_dir = prepared.get_work_directory().to_string();

    // Verify input file was materialised inside src/.
    let input_file = format!("{work_dir}/src/input_file.c");
    assert!(
        fs::metadata(&input_file).await.is_ok(),
        "input file src/input_file.c was not materialised: \
         [B2] failed or was disrupted by [C]'s create_dir_all",
    );

    // Verify output parent directory exists (created by [C]).
    let output_parent = format!("{work_dir}/src");
    assert!(
        fs::metadata(&output_parent).await.is_ok(),
        "output parent dir src/ does not exist after prepare_action: \
         [C] did not create the shared parent directory",
    );

    prepared.cleanup().await?;
    Ok(())
}

/// T2: Sanity check that output directories are created correctly when there
/// is no overlap between input tree and output paths.
///
/// This is a non-regression test that ensures the O5 restructure does not
/// break the common case where input tree and output paths are disjoint.
#[nativelink_test]
async fn prepare_action_output_dirs_created_correctly()
-> Result<(), Box<dyn core::error::Error>> {
    let (_fast_store, _slow_store, cas_store, ac_store) = setup_stores().await?;
    let root_action_directory = make_temp_path("root_action_dir_t2");
    fs::create_dir_all(&root_action_directory).await?;

    // Simple empty input root, output path in a separate directory.
    let empty_dir = Directory::default();
    let input_root_digest = serialize_and_upload_message(
        &empty_dir,
        cas_store.as_pin(),
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: vec!["build/output/result.o".to_string()],
        ..Default::default()
    };
    let command_digest = serialize_and_upload_message(
        &command,
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

    let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
        RunningActionsManagerArgs {
            root_action_directory,
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: cas_store.clone(),
            ac_store: Some(Store::new(ac_store.clone())),
            ac_mirror_target: None,
            historical_store: Store::new(cas_store.clone()),
            upload_action_result_config:
                &nativelink_config::cas_server::UploadActionResultConfig {
                    upload_ac_results_strategy:
                        nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
                    ..Default::default()
                },
            max_action_timeout: Duration::MAX,
            max_upload_timeout: Duration::from_secs(600),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
            deferred_output_uploads_enabled: false,
        },
    )?);

    let execute_request = ExecuteRequest {
        action_digest: Some(action_digest.into()),
        ..Default::default()
    };
    let operation_id = OperationId::default().to_string();

    let running_action = running_actions_manager
        .create_and_add_action(
            "test_worker".to_string(),
            StartExecute {
                execute_request: Some(execute_request),
                operation_id,
                queued_timestamp: None,
                platform: action.platform.clone(),
                worker_id: "test_worker".to_string(),
                resolved_directories: Vec::new(),
                resolved_directory_digests: Vec::new(),
                missing_digests: Vec::new(),
                missing_digest_peers: Vec::new(),
            },
        )
        .await?;

    let prepared = tokio::time::timeout(
        Duration::from_secs(30),
        running_action.clone().prepare_action(),
    )
    .await
    .expect("prepare_action must not deadlock (30s detector)")
    .map_err(|e| format!("prepare_action must succeed for disjoint input/output: {e:?}"))
    .unwrap();

    let work_dir = prepared.get_work_directory().to_string();

    // Verify output parent directory was created.
    let output_parent = format!("{work_dir}/build/output");
    assert!(
        fs::metadata(&output_parent).await.is_ok(),
        "output parent dir build/output was not created by [C]",
    );

    prepared.cleanup().await?;
    Ok(())
}
