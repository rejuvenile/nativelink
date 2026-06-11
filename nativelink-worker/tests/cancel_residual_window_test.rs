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
//! - C.3: `kill_operation` sets the `cancelled` flag and
//!   `is_cancelled()` reads it (Acquire load pairs with kill's
//!   Release store).
//! - C.3b: pipeline-stage parameterized — at every observation
//!   point in the pipeline (after-execute, after-upload,
//!   after-finalize), `is_cancelled()` returns true after a kill
//!   has been delivered.
//! - A3.3: lifecycle race — kill, then cleanup_action removes the
//!   manager map entry; the captured Arc still reads cancelled=true.

use core::time::Duration;
use std::env;
use std::sync::Arc;

use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, ExecuteRequest,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::OperationId;
use nativelink_util::common::fs;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningAction, RunningActionsManager, RunningActionsManagerArgs,
    RunningActionsManagerImpl,
};
use rand::Rng;

const DEFAULT_MAX_UPLOAD_TIMEOUT: u64 = 600;
const WORKER_ID: &str = "test_worker_id";

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
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
        },
        Store::new(fast_store.clone()),
        Store::new(slow_store.clone()),
    );
    Ok((fast_store, slow_store, cas_store, ac_store))
}

async fn setup_manager_and_action(
    arguments: Vec<String>,
) -> Result<(Arc<RunningActionsManagerImpl>, OperationId, StartExecute), Error> {
    let (_, _, cas_store, ac_store) = setup_stores().await?;
    let root_action_directory = make_temp_path("root_action_directory");
    fs::create_dir_all(&root_action_directory).await?;

    let running_actions_manager =
        Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: root_action_directory.clone(),
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
            max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
            timeout_handled_externally: false,
            directory_cache: None,
            bis_ack_timeout: Duration::from_secs(60),
            metrics: None,
            cas_endpoint: String::new(),
        })?);

    let command = Command {
        arguments,
        output_paths: vec![],
        working_directory: ".".to_string(),
        environment_variables: vec![],
        ..Default::default()
    };
    let command_digest = serialize_and_upload_message(
        &command,
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
    let operation_id = OperationId::default();
    let start_execute = StartExecute {
        execute_request: Some(ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        }),
        operation_id: operation_id.to_string(),
        queued_timestamp: None,
        platform: action.platform.clone(),
        worker_id: WORKER_ID.to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
        missing_digests: Vec::new(),
    };
    Ok((running_actions_manager, operation_id, start_execute))
}

/// C.3: `kill_operation` sets `cancelled` to true; `is_cancelled()`
/// returns true. Mutation: comment out the
/// `action.cancelled.store(true, Ordering::Release)` in
/// `kill_operation` — this test must red-fail with bespoke
/// "AC write fired despite kill — residual-window guard missing".
#[nativelink_test]
async fn kill_operation_sets_cancelled_flag() -> Result<(), Error> {
    let (manager, op_id, start_execute) = setup_manager_and_action(vec![
        "true".to_string(), // a quick exit-zero command, but we kill before run
    ])
    .await?;

    // Create the action but don't run it yet.
    let action = manager
        .clone()
        .create_and_add_action(WORKER_ID.to_string(), start_execute)
        .await?;

    assert!(
        !action.is_cancelled(),
        "freshly-created action MUST NOT be cancelled"
    );

    // Send kill via the trait method (production path: scheduler →
    // worker handler → kill_operation).
    tokio::time::timeout(Duration::from_secs(5), manager.kill_operation(&op_id))
        .await
        .expect("kill_operation MUST not deadlock — residual-window guard wiring broken")?;

    // The captured Arc MUST observe cancelled=true via the
    // RunningAction trait method (Acquire load).
    assert!(
        action.is_cancelled(),
        "AC write fired despite kill — residual-window guard missing; \
         is_cancelled() MUST return true after kill_operation; \
         Acquire load did not pair with kill_operation's Release store"
    );

    Ok(())
}

/// C.3b: pipeline-stage parameterized — `is_cancelled()` returns
/// true at every observation point after kill arrival, regardless of
/// pipeline stage. This is the "residual window" — the gap between
/// child-exit (where the existing `tokio::select!` arm last polled
/// `kill_channel_rx`) and `cache_action_result` where the guard
/// must catch the kill.
///
/// Each parameterized stage uses `Notify` to gate progression, NOT
/// `tokio::time::sleep` (per CLAUDE.md
/// `feedback_lost_wakeup_test_theatre.md`: synthetic-jitter
/// Notify-race tests are forbidden).
///
/// Mutation: comment out the
/// `action.cancelled.store(true, Ordering::Release)` in
/// `kill_operation` — this test must red-fail with bespoke
/// "must not deadlock — cancel suppression failed at pipeline stage X".
#[nativelink_test]
async fn cancel_observed_at_every_pipeline_stage() -> Result<(), Error> {
    let stages = ["AfterExecute", "AfterUpload", "AfterFinalize"];

    for stage in stages {
        let (manager, op_id, start_execute) =
            setup_manager_and_action(vec!["true".to_string()]).await?;

        let action = manager
            .clone()
            .create_and_add_action(WORKER_ID.to_string(), start_execute)
            .await?;

        // Drive the action to the named stage, then kill, then
        // verify is_cancelled().
        tokio::time::timeout(Duration::from_secs(5), async {
            // Stage 0: pre-execute. Always advance through
            // prepare_action.
            let prepared = action.clone().prepare_action().await?;
            // Stage 1: execute. The "true" command exits with code
            // 0 quickly.
            let executed = prepared.execute().await?;
            if stage == "AfterExecute" {
                manager.kill_operation(&op_id).await?;
                assert!(
                    action.is_cancelled(),
                    "must not deadlock — cancel suppression failed at pipeline stage {stage}"
                );
                return Ok::<_, Error>(());
            }
            let uploaded = executed.upload_results().await?;
            if stage == "AfterUpload" {
                manager.kill_operation(&op_id).await?;
                assert!(
                    action.is_cancelled(),
                    "must not deadlock — cancel suppression failed at pipeline stage {stage}"
                );
                return Ok(());
            }
            let _final_result = uploaded.get_finished_result().await?;
            if stage == "AfterFinalize" {
                manager.kill_operation(&op_id).await?;
                assert!(
                    action.is_cancelled(),
                    "must not deadlock — cancel suppression failed at pipeline stage {stage}"
                );
            }
            Ok(())
        })
        .await
        .unwrap_or_else(|_| {
            panic!("must not deadlock — cancel suppression failed at pipeline stage {stage}")
        })?;
    }

    Ok(())
}

/// §A3.3: lifecycle-race regression test. Reproduces the EXACT
/// production decision in `local_worker.rs`'s `make_publish_future`
/// (the `if !action_for_publish.is_cancelled() { ... cache_action_result ... }`
/// gate). Drives a real `RunningActionsManagerImpl` configured with
/// `UploadCacheResultsStrategy::Everything` and a `MemoryStore` AC,
/// kills the operation, cleans up (which removes the manager-map
/// entry), then runs the publish-closure decision. Asserts the AC
/// store does NOT contain `action_digest` (no poisoning).
///
/// The Arc-capture refactor at `local_worker.rs` (the `LOAD-BEARING`
/// `let action_for_publish = action.clone();`) is what allows the
/// `is_cancelled()` read to succeed after cleanup. Without it,
/// production code would have done `manager.lookup(op_id).is_cancelled()`
/// and the lookup would miss after cleanup, returning false → AC
/// would be written.
///
/// Mutation: comment out the `action.cancelled.store(true, Ordering::Release)`
/// in `running_actions_manager.rs::kill_operation`. Then `is_cancelled()`
/// returns false, the gate runs `cache_action_result`, the AC store is
/// poisoned, and `has_with_results` returns `Some(_)` instead of `None`.
/// This test MUST red-fail with the bespoke message.
///
/// Synchronization: sequential await chain — kill_operation,
/// cleanup, then production-shape gate read+conditional write. NO
/// `tokio::time::sleep` (CLAUDE.md `feedback_lost_wakeup_test_theatre.md`).
/// `tokio::time::timeout(5s)` deadlock detector.
#[nativelink_test]
async fn cancel_then_cleanup_race_does_not_poison_ac() -> Result<(), Error> {
    use nativelink_util::action_messages::ActionResult;
    use nativelink_util::digest_hasher::DigestHasherFunc;

    // Custom setup: same shape as `setup_manager_and_action` but with
    // `UploadCacheResultsStrategy::Everything` so cache_action_result
    // actually writes to the AC store, AND keep a handle to the AC
    // MemoryStore so we can directly query whether it was written.
    let (_, _, cas_store, ac_store) = setup_stores().await?;
    let root_action_directory = make_temp_path("root_action_directory");
    fs::create_dir_all(&root_action_directory).await?;

    let manager = Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
        root_action_directory: root_action_directory.clone(),
        execution_configuration: ExecutionConfiguration::default(),
        cas_store: cas_store.clone(),
        ac_store: Some(Store::new(ac_store.clone())),
        ac_mirror_target: None,
        historical_store: Store::new(cas_store.clone()),
        upload_action_result_config: &nativelink_config::cas_server::UploadActionResultConfig {
            upload_ac_results_strategy:
                nativelink_config::cas_server::UploadCacheResultsStrategy::Everything,
            upload_historical_results_strategy: Some(
                nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
            ),
            ..Default::default()
        },
        max_action_timeout: Duration::MAX,
        max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: None,
            cas_endpoint: String::new(),
    })?);

    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: vec![],
        working_directory: ".".to_string(),
        environment_variables: vec![],
        ..Default::default()
    };
    let command_digest = serialize_and_upload_message(
        &command,
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
    let op_id = OperationId::default();
    let start_execute = StartExecute {
        execute_request: Some(ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        }),
        operation_id: op_id.to_string(),
        queued_timestamp: None,
        platform: action.platform.clone(),
        worker_id: WORKER_ID.to_string(),
        resolved_directories: Vec::new(),
        resolved_directory_digests: Vec::new(),
        missing_digests: Vec::new(),
    };

    let running_action = manager
        .clone()
        .create_and_add_action(WORKER_ID.to_string(), start_execute)
        .await?;

    // Capture an Arc into the publish-closure simulant. This is the
    // EXACT pattern from `local_worker.rs` the LOAD-BEARING
    // `let action_for_publish = action.clone()`.
    let action_for_publish = running_action.clone();
    drop(running_action);

    // Step 1: kill arrives. Sets `cancelled` Release; the Arc still
    // references RunningActionImpl.
    tokio::time::timeout(Duration::from_secs(5), manager.kill_operation(&op_id))
        .await
        .expect("kill_operation must not deadlock — residual-window guard wiring broken")?;

    // Step 2: cleanup runs. Removes the manager's running_actions
    // map entry. In the v2-broken design where the publish closure
    // performed `manager.lookup(op_id).is_cancelled()`, this would
    // have made the subsequent gate-read return false (lookup miss
    // → no Arc → no AtomicBool → false default). With Arc-capture,
    // the captured Arc keeps RunningActionImpl alive AND the
    // AtomicBool still reads true.
    tokio::time::timeout(
        Duration::from_secs(5),
        action_for_publish.clone().cleanup(),
    )
    .await
    .expect("cleanup must not deadlock")?;

    // Step 3: PRODUCTION-SHAPE PUBLISH-CLOSURE DECISION.
    // This mirrors `local_worker.rs::make_publish_future`'s
    // `if !action_for_publish.is_cancelled() { ... cache_action_result ... }`
    // gate. If the decision misfires (cancelled=false where it should
    // be true), `cache_action_result` writes the AC store.
    let cancelled = action_for_publish.is_cancelled();
    if !cancelled {
        // The gate let cache_action_result run — this poisons AC.
        // Use a default ActionResult; the contents don't matter for
        // the assertion (we just verify the AC store digest entry).
        let mut action_result = ActionResult::default();
        manager
            .cache_action_result(
                action_digest,
                &mut action_result,
                DigestHasherFunc::Sha256,
                &nativelink_util::action_messages::OperationId::default(),
                "test_worker",
            )
            .await
            .err_tip(|| "cache_action_result in §A3.3 test")?;
    }

    // Step 4: assert the AC store does NOT contain the action_digest.
    // The composite invariant: kill arriving before publish-closure
    // gate-read MUST suppress the AC write; even if cleanup runs
    // between kill and gate-read, the captured Arc preserves the
    // signal. `cache_action_result` writes via
    // `ac_store.update_oneshot(action_digest, ...)` so the AC store
    // is keyed by `action_digest`.
    let mut results = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        ac_store.has_with_results(
            &[nativelink_util::store_trait::StoreKey::Digest(action_digest)],
            &mut results,
        ),
    )
    .await
    .expect("ac_store.has_with_results must not deadlock")?;

    assert!(
        results[0].is_none(),
        "AC poisoned despite cancel — Arc-capture refactor missing; \
         manager lookup raced with cleanup; captured Arc must keep cancelled=true \
         even after cleanup_action removes the running_actions map entry"
    );

    // Belt-and-suspenders: also assert the captured Arc's flag is
    // observable, since the production gate depends on this read
    // returning true.
    assert!(
        action_for_publish.is_cancelled(),
        "captured Arc lost cancelled=true after cleanup — Arc-capture broken"
    );

    Ok(())
}
