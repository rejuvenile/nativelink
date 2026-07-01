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

use serial_test::serial;

#[serial]
mod tests {
    use core::pin::Pin;
    use core::str::from_utf8;
    use core::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
    #[cfg(target_family = "unix")]
    use core::task::Poll;
    use core::time::Duration;
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::ffi::OsString;
    use std::io::{Cursor, Write};
    #[cfg(target_family = "unix")]
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::prelude::*;
    use nativelink_config::cas_server::EnvironmentSource;
    use nativelink_config::stores::{
        EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
    };
    use nativelink_metric::{
        MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    };
    use nativelink_error::{Code, Error, ResultExt, make_input_err};
    use nativelink_macro::nativelink_test;
    use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
    #[cfg_attr(target_family = "windows", allow(unused_imports))]
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Action, ActionResult as ProtoActionResult, Command, Directory, DirectoryNode,
        ExecuteRequest, ExecuteResponse, FileNode, NodeProperties, Platform, SymlinkNode, Tree,
        digest_function::Value as ProtoDigestFunction, platform::Property,
    };
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        HistoricalExecuteResponse, StartExecute,
    };
    use nativelink_proto::google::rpc::Status;
    use nativelink_store::ac_utils::{get_and_decode_digest, serialize_and_upload_message};
    use nativelink_store::fast_slow_store::FastSlowStore;
    use nativelink_store::filesystem_store::FilesystemStore;
    use nativelink_store::memory_store::MemoryStore;
    #[cfg(target_family = "unix")]
    use nativelink_util::action_messages::DirectoryInfo;
    #[cfg_attr(target_family = "windows", allow(unused_imports))]
    use nativelink_util::action_messages::SymlinkInfo;
    use nativelink_util::action_messages::{
        ActionResult, ExecutionMetadata, FileInfo, NameOrPath, OperationId,
    };
    use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
    use nativelink_util::common::{DigestInfo, fs};
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
    use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
    use nativelink_util::store_trait::{
        ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store,
        StoreDriver, StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
    };
    use nativelink_worker::local_worker::AcMirrorTarget;
    use nativelink_worker::running_actions_manager::{
        Callbacks, ExecutionConfiguration, RunningAction, RunningActionImpl, RunningActionsManager,
        RunningActionsManagerArgs, RunningActionsManagerImpl, download_to_directory,
    };
    use pretty_assertions::assert_eq;
    use prost::Message;
    use rand::Rng;
    use tokio::sync::{Notify, oneshot};

    const DEFAULT_MAX_UPLOAD_TIMEOUT: u64 = 600;

    /// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
    /// not set.
    fn make_temp_path(data: &str) -> String {
        #[cfg(target_family = "unix")]
        return format!(
            "{}/{}/{}",
            env::var("TEST_TMPDIR")
                .unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
            rand::rng().random::<u64>(),
            data
        );
        #[cfg(target_family = "windows")]
        return format!(
            "{}\\{}\\{}",
            env::var("TEST_TMPDIR")
                .unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
            rand::rng().random::<u64>(),
            data
        );
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

    /// FL-681 Follow-up A: worker id used by the admission-gate tests.
    const ADMISSION_GATE_WORKER_ID: &str = "fl681_admission_gate_worker";

    /// FL-681 Follow-up A: build a CAS `FastSlowStore` whose fast tier is a
    /// `FilesystemStore` with an explicit indefinite-pin byte cap
    /// (`pending_bis_pin_max_bytes`) and a non-zero eviction `max_bytes` (so
    /// the saturation gate is governed — `max_bytes == 0` never gates). Returns
    /// the concrete `FilesystemStore` handle alongside so the test can drive
    /// the indefinite-pin set directly to saturate the cap.
    async fn setup_capped_stores(
        indefinite_pin_cap_bytes: u64,
        max_bytes: usize,
    ) -> Result<(Arc<FilesystemStore>, Arc<FastSlowStore>, Arc<MemoryStore>), Error> {
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: Some(EvictionPolicy {
                max_bytes,
                ..Default::default()
            }),
            pending_bis_pin_max_bytes: indefinite_pin_cap_bytes,
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
        Ok((fast_store, cas_store, ac_store))
    }

    /// FL-681 Follow-up A: construct a `RunningActionsManagerImpl` over a given
    /// CAS store with the F2 deferred-output kill-switch in a chosen state.
    /// Shared by the admission-gate tests so the large `RunningActionsManagerArgs`
    /// shape is written once.
    async fn build_running_actions_manager(
        cas_store: Arc<FastSlowStore>,
        ac_store: Arc<MemoryStore>,
        deferred_output_uploads_enabled: bool,
    ) -> Result<Arc<RunningActionsManagerImpl>, Error> {
        fn test_now() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        Ok(Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store)),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store),
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
                deferred_output_uploads_enabled,
            },
            Callbacks {
                now_fn: test_now,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?))
    }

    /// FL-681 Follow-up A: build a minimal valid `StartExecute` whose Action /
    /// Command / input-root protos are uploaded to `cas_store`, returning the
    /// `StartExecute` ready to feed to `create_and_add_action`.
    async fn make_start_execute(cas_store: &Arc<FastSlowStore>) -> Result<StartExecute, Error> {
        let command = Command {
            arguments: vec!["true".to_string()],
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
        Ok(StartExecute {
            execute_request: Some(ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            }),
            operation_id: OperationId::default().to_string(),
            queued_timestamp: None,
            platform: action.platform.clone(),
            worker_id: ADMISSION_GATE_WORKER_ID.to_string(),
            resolved_directories: Vec::new(),
            resolved_directory_digests: Vec::new(),
            missing_digests: Vec::new(),
        })
    }

    /// FL-681 Follow-up A (MAJOR-1b true close-out): drive the fast store's
    /// indefinite-pin set to exactly its `pending_bis_pin_max_bytes` cap so
    /// `indefinite_pin_saturated()` reports `true`. Inserts a `cap`-byte blob
    /// and pins it indefinitely. Returns the saturating digest.
    async fn saturate_indefinite_pin_cap(
        fast_store: &Arc<FilesystemStore>,
        cap_bytes: u64,
    ) -> Result<DigestInfo, Error> {
        let payload = vec![0u8; cap_bytes as usize];
        let digest = DigestInfo::try_new(
            "0000000000000000000000000000000000000000000000000000000000000001",
            cap_bytes,
        )?;
        fast_store
            .as_pin()
            .update_oneshot(digest.into(), payload.into())
            .await?;
        assert!(
            fast_store.pin_digest_indefinite_with_result(&digest),
            "saturating indefinite pin must succeed (the blob fills the cap exactly)"
        );
        assert!(
            fast_store.indefinite_pin_saturated(),
            "test precondition: indefinite-pin cap must read saturated after filling it"
        );
        Ok(digest)
    }

    // FL-681 Follow-up A (MAJOR-1b true close-out): when the F2 indefinite-pin
    // cap is saturated, the worker's action-acceptance path MUST NAK a fresh
    // action with `Code::ResourceExhausted` — producer backpressure that the
    // scheduler re-queues (verified: simple_scheduler_state_manager.rs treats a
    // worker ResourceExhausted as re-queue without consuming a retry attempt)
    // — rather than admitting an action whose outputs cannot be pinned-until-
    // BIS-durable (the accept-then-time-bound-then-lose residual).
    //
    // Composite invariant (admission/eviction/pin triangle):
    //   gate-active ⇒ (indefinite-pin works AND BIS-ack release fires) OR the
    //   time-bounded TTL fallback compensates.
    // Here the gate (this NAK) fires when the indefinite-pin corner is
    // saturated; the BIS-ack `unpin_digest` corner drains the cap to clear it.
    #[nativelink_test]
    async fn f2_admission_gate_naks_when_indefinite_pin_saturated()
    -> Result<(), Box<dyn core::error::Error>> {
        const CAP: u64 = 4096;
        let (fast_store, cas_store, ac_store) = setup_capped_stores(CAP, 1024 * 1024).await?;
        let running_actions_manager =
            build_running_actions_manager(cas_store.clone(), ac_store, true).await?;

        saturate_indefinite_pin_cap(&fast_store, CAP).await?;

        let start_execute = make_start_execute(&cas_store).await?;
        let result = running_actions_manager
            .create_and_add_action(ADMISSION_GATE_WORKER_ID.to_string(), start_execute)
            .await;

        let err = result.err().expect(
            "saturated indefinite-pin cap must NAK the action — without the admission gate a fresh \
             F2 output is admitted, executed, and then lost when its pin falls back to the 120s TTL",
        );
        assert_eq!(
            err.code,
            Code::ResourceExhausted,
            "the admission NAK MUST carry Code::ResourceExhausted so the scheduler re-queues it as \
             backpressure (any other code fails the action and churns instead of throttling): {err:?}"
        );
        Ok(())
    }

    // FL-681 Follow-up A: with indefinite-pin headroom, admission proceeds
    // normally — the gate must not throttle a worker that can still durably
    // hold new pending-BIS outputs.
    #[nativelink_test]
    async fn f2_admission_gate_admits_when_indefinite_pin_has_headroom()
    -> Result<(), Box<dyn core::error::Error>> {
        const CAP: u64 = 4096;
        let (fast_store, cas_store, ac_store) = setup_capped_stores(CAP, 1024 * 1024).await?;
        let running_actions_manager =
            build_running_actions_manager(cas_store.clone(), ac_store, true).await?;

        assert!(
            !fast_store.indefinite_pin_saturated(),
            "test precondition: a fresh capped store must have indefinite-pin headroom"
        );

        let start_execute = make_start_execute(&cas_store).await?;
        running_actions_manager
            .create_and_add_action(ADMISSION_GATE_WORKER_ID.to_string(), start_execute)
            .await
            .expect(
                "with indefinite-pin headroom the admission gate must NOT fire — gating a worker \
                 that can still hold pending-BIS outputs needlessly churns the scheduler",
            );
        Ok(())
    }

    // FL-681 Follow-up A: the gate is F2-only. With deferred output uploads
    // DISABLED (synchronous path), the worker takes NO indefinite pins, so a
    // saturated indefinite-pin cap is irrelevant and the gate MUST stay inert
    // — gating the synchronous path would refuse admission for a constraint it
    // does not impose.
    #[nativelink_test]
    async fn f2_admission_gate_inert_when_deferred_uploads_disabled()
    -> Result<(), Box<dyn core::error::Error>> {
        const CAP: u64 = 4096;
        let (fast_store, cas_store, ac_store) = setup_capped_stores(CAP, 1024 * 1024).await?;
        let running_actions_manager =
            build_running_actions_manager(cas_store.clone(), ac_store, false).await?;

        saturate_indefinite_pin_cap(&fast_store, CAP).await?;

        let start_execute = make_start_execute(&cas_store).await?;
        running_actions_manager
            .create_and_add_action(ADMISSION_GATE_WORKER_ID.to_string(), start_execute)
            .await
            .expect(
                "with deferred_output_uploads_enabled=false the synchronous path takes no \
                 indefinite pins; a saturated indefinite-pin cap MUST NOT gate admission",
            );
        Ok(())
    }

    async fn run_action(action: Arc<RunningActionImpl>) -> Result<ActionResult, Error> {
        let result = action
            .clone()
            .prepare_action()
            .await?
            .execute()
            .await?
            .upload_results()
            .await?
            .get_finished_result()
            .await;
        action.cleanup().await?;
        result
    }

    const NOW_TIME: u64 = 10000;

    fn make_system_time(add_time: u64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(NOW_TIME + add_time))
            .unwrap()
    }

    fn monotonic_clock(counter: &AtomicU64) -> SystemTime {
        let count = counter.fetch_add(1, Ordering::Relaxed);
        make_system_time(count)
    }

    fn increment_clock(time: &mut SystemTime) -> SystemTime {
        let previous_time = *time;
        *time = previous_time.checked_add(Duration::from_secs(1)).unwrap();
        previous_time
    }

    #[nativelink_test]
    async fn download_to_directory_file_download_test() -> Result<(), Box<dyn core::error::Error>> {
        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const FILE2_NAME: &str = "file2.exec";
        const FILE2_CONTENT: &str = "HELLOFILE2";
        const FILE2_MODE: u32 = 0o710;
        const FILE2_MTIME: u64 = 5;

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file1_content_digest = DigestInfo::new([2u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
                .await?;
            let file2_content_digest = DigestInfo::new([3u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file2_content_digest, FILE2_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([1u8; 32], 32);
            let root_directory = Directory {
                files: vec![
                    FileNode {
                        name: FILE1_NAME.to_string(),
                        digest: Some(file1_content_digest.into()),
                        is_executable: false,
                        node_properties: None,
                    },
                    FileNode {
                        name: FILE2_NAME.to_string(),
                        digest: Some(file2_content_digest.into()),
                        is_executable: true,
                        node_properties: Some(NodeProperties {
                            properties: vec![],
                            mtime: Some(
                                SystemTime::UNIX_EPOCH
                                    .checked_add(Duration::from_secs(FILE2_MTIME))
                                    .unwrap()
                                    .into(),
                            ),
                            unix_mode: Some(FILE2_MODE),
                        }),
                    },
                ],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_pin(),
                &root_directory_digest,
                &download_dir,
                None,
                None,
                None,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let file1_content = fs::read(format!("{download_dir}/{FILE1_NAME}")).await?;
            assert_eq!(from_utf8(&file1_content)?, FILE1_CONTENT);

            let file2_path = format!("{download_dir}/{FILE2_NAME}");
            let file2_content = fs::read(&file2_path).await?;
            assert_eq!(from_utf8(&file2_content)?, FILE2_CONTENT);

            let file2_metadata = fs::metadata(&file2_path).await?;
            // Note: We sent 0o710, but because is_executable was set it turns into 0o711.
            #[cfg(target_family = "unix")]
            assert_eq!(file2_metadata.mode() & 0o777, FILE2_MODE | 0o111);
            assert_eq!(
                file2_metadata
                    .modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
                FILE2_MTIME
            );
        }
        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_folder_download_test() -> Result<(), Box<dyn core::error::Error>>
    {
        const DIRECTORY1_NAME: &str = "folder1";
        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const DIRECTORY2_NAME: &str = "folder2";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let directory1_digest = DigestInfo::new([1u8; 32], 32);
            {
                let file1_content_digest = DigestInfo::new([2u8; 32], 32);
                slow_store
                    .as_ref()
                    .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
                    .await?;
                let directory1 = Directory {
                    files: vec![FileNode {
                        name: FILE1_NAME.to_string(),
                        digest: Some(file1_content_digest.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                slow_store
                    .as_ref()
                    .update_oneshot(directory1_digest, directory1.encode_to_vec().into())
                    .await?;
            }
            let directory2_digest = DigestInfo::new([3u8; 32], 32);
            {
                // Now upload an empty directory.
                slow_store
                    .as_ref()
                    .update_oneshot(
                        directory2_digest,
                        Directory::default().encode_to_vec().into(),
                    )
                    .await?;
            }
            let root_directory_digest = DigestInfo::new([5u8; 32], 32);
            {
                let root_directory = Directory {
                    directories: vec![
                        DirectoryNode {
                            name: DIRECTORY1_NAME.to_string(),
                            digest: Some(directory1_digest.into()),
                        },
                        DirectoryNode {
                            name: DIRECTORY2_NAME.to_string(),
                            digest: Some(directory2_digest.into()),
                        },
                    ],
                    ..Default::default()
                };
                slow_store
                    .as_ref()
                    .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                    .await?;
            }
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_pin(),
                &root_directory_digest,
                &download_dir,
                None,
                None,
                None,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let file1_content = fs::read(format!("{download_dir}/{DIRECTORY1_NAME}/{FILE1_NAME}"))
                .await
                .err_tip(|| "On file_1 read")?;
            assert_eq!(from_utf8(&file1_content)?, FILE1_CONTENT);

            let folder2_path = format!("{download_dir}/{DIRECTORY2_NAME}");
            let folder2_metadata = fs::metadata(&folder2_path)
                .await
                .err_tip(|| "On folder2_metadata metadata")?;
            assert_eq!(folder2_metadata.is_dir(), true);
        }
        Ok(())
    }

    // Windows does not support symlinks.
    #[cfg(not(target_family = "windows"))]
    #[nativelink_test]
    async fn download_to_directory_symlink_download_test() -> Result<(), Box<dyn core::error::Error>>
    {
        const FILE_NAME: &str = "file.txt";
        const FILE_CONTENT: &str = "HELLOFILE";
        const SYMLINK_NAME: &str = "symlink_file.txt";
        const SYMLINK_TARGET: &str = "file.txt";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file_content_digest = DigestInfo::new([1u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file_content_digest, FILE_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([2u8; 32], 32);
            let root_directory = Directory {
                files: vec![FileNode {
                    name: FILE_NAME.to_string(),
                    digest: Some(file_content_digest.into()),
                    is_executable: false,
                    node_properties: None,
                }],
                symlinks: vec![SymlinkNode {
                    name: SYMLINK_NAME.to_string(),
                    target: SYMLINK_TARGET.to_string(),
                    node_properties: None,
                }],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_pin(),
                &root_directory_digest,
                &download_dir,
                None,
                None,
                None,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let symlink_path = format!("{download_dir}/{SYMLINK_NAME}");
            let symlink_content = fs::read(&symlink_path)
                .await
                .err_tip(|| "On symlink read")?;
            assert_eq!(from_utf8(&symlink_content)?, FILE_CONTENT);

            let symlink_metadata = fs::symlink_metadata(&symlink_path)
                .await
                .err_tip(|| "On symlink symlink_metadata")?;
            assert_eq!(symlink_metadata.is_symlink(), true);
        }
        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_batch_existence_check_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Verifies that files already in the fast store are hardlinked
        // without being re-fetched from the slow store.
        const FILE1_NAME: &str = "cached_file.txt";
        const FILE1_CONTENT: &str = "ALREADY_IN_FAST";
        const FILE2_NAME: &str = "uncached_file.txt";
        const FILE2_CONTENT: &str = "ONLY_IN_SLOW";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let file1_content_digest = DigestInfo::new([10u8; 32], FILE1_CONTENT.len() as u64);
            let file2_content_digest = DigestInfo::new([11u8; 32], FILE2_CONTENT.len() as u64);

            // Put file1 in BOTH slow and fast store (simulates a cached blob).
            slow_store
                .as_ref()
                .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
                .await?;
            fast_store
                .as_ref()
                .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
                .await?;

            // Put file2 ONLY in slow store (simulates a cache miss).
            slow_store
                .as_ref()
                .update_oneshot(file2_content_digest, FILE2_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([12u8; 32], 32);
            let root_directory = Directory {
                files: vec![
                    FileNode {
                        name: FILE1_NAME.to_string(),
                        digest: Some(file1_content_digest.into()),
                        ..Default::default()
                    },
                    FileNode {
                        name: FILE2_NAME.to_string(),
                        digest: Some(file2_content_digest.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = make_temp_path("download_dir_batch_check");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // Both files should be present with correct content.
        let file1_content = fs::read(format!("{download_dir}/{FILE1_NAME}")).await?;
        assert_eq!(from_utf8(&file1_content)?, FILE1_CONTENT);

        let file2_content = fs::read(format!("{download_dir}/{FILE2_NAME}")).await?;
        assert_eq!(from_utf8(&file2_content)?, FILE2_CONTENT);

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_dedup_digests_test() -> Result<(), Box<dyn core::error::Error>> {
        // Verifies that multiple files sharing the same digest content
        // are all materialized correctly (the digest is only downloaded once
        // but hardlinked to multiple destinations).
        const SHARED_CONTENT: &str = "SHARED_CONTENT_DATA";
        const FILE_A_NAME: &str = "file_a.txt";
        const FILE_B_NAME: &str = "file_b.txt";
        const FILE_C_NAME: &str = "file_c.txt";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let shared_digest = DigestInfo::new([20u8; 32], SHARED_CONTENT.len() as u64);
            slow_store
                .as_ref()
                .update_oneshot(shared_digest, SHARED_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([21u8; 32], 32);
            let root_directory = Directory {
                files: vec![
                    FileNode {
                        name: FILE_A_NAME.to_string(),
                        digest: Some(shared_digest.into()),
                        ..Default::default()
                    },
                    FileNode {
                        name: FILE_B_NAME.to_string(),
                        digest: Some(shared_digest.into()),
                        ..Default::default()
                    },
                    FileNode {
                        name: FILE_C_NAME.to_string(),
                        digest: Some(shared_digest.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = make_temp_path("download_dir_dedup");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // All three files should exist with the same content.
        for name in &[FILE_A_NAME, FILE_B_NAME, FILE_C_NAME] {
            let content = fs::read(format!("{download_dir}/{name}")).await?;
            assert_eq!(from_utf8(&content)?, SHARED_CONTENT, "Mismatch for {name}");
        }

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_deep_nested_tree_test() -> Result<(), Box<dyn core::error::Error>>
    {
        // Verifies that deeply nested directory trees (3 levels) are resolved
        // correctly via the recursive fallback path (MemoryStore).
        const LEAF_FILE_NAME: &str = "leaf.txt";
        const LEAF_CONTENT: &str = "DEEP_LEAF_DATA";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let leaf_content_digest = DigestInfo::new([30u8; 32], LEAF_CONTENT.len() as u64);
            slow_store
                .as_ref()
                .update_oneshot(leaf_content_digest, LEAF_CONTENT.into())
                .await?;

            // Level 3 (deepest): directory containing a file
            let level3_digest = DigestInfo::new([31u8; 32], 32);
            let level3_dir = Directory {
                files: vec![FileNode {
                    name: LEAF_FILE_NAME.to_string(),
                    digest: Some(leaf_content_digest.into()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(level3_digest, level3_dir.encode_to_vec().into())
                .await?;

            // Level 2: directory containing level3
            let level2_digest = DigestInfo::new([32u8; 32], 32);
            let level2_dir = Directory {
                directories: vec![DirectoryNode {
                    name: "level3".to_string(),
                    digest: Some(level3_digest.into()),
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(level2_digest, level2_dir.encode_to_vec().into())
                .await?;

            // Level 1 (root): directory containing level2
            let root_digest = DigestInfo::new([33u8; 32], 32);
            let root_dir = Directory {
                directories: vec![DirectoryNode {
                    name: "level2".to_string(),
                    digest: Some(level2_digest.into()),
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_dir.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_deep");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // Verify the deeply nested file exists with correct content.
        let leaf_path = format!("{download_dir}/level2/level3/{LEAF_FILE_NAME}");
        let leaf_content = fs::read(&leaf_path).await?;
        assert_eq!(from_utf8(&leaf_content)?, LEAF_CONTENT);

        // Verify intermediate directories exist.
        let level2_meta = fs::metadata(format!("{download_dir}/level2")).await?;
        assert!(level2_meta.is_dir());
        let level3_meta = fs::metadata(format!("{download_dir}/level2/level3")).await?;
        assert!(level3_meta.is_dir());

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_empty_directory_test() -> Result<(), Box<dyn core::error::Error>>
    {
        // Verifies that an empty root directory is handled correctly.
        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let root_digest = DigestInfo::new([40u8; 32], 32);
            let root_dir = Directory::default();
            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_dir.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_empty");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // Directory should exist and be empty.
        let meta = fs::metadata(&download_dir).await?;
        assert!(meta.is_dir());

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_many_files_test() -> Result<(), Box<dyn core::error::Error>> {
        // Verifies that a directory with many files (simulating a real build
        // with many inputs) is handled correctly by the batch existence check
        // and parallel download paths.
        const FILE_COUNT: usize = 50;

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let mut file_nodes = Vec::with_capacity(FILE_COUNT);
            for i in 0..FILE_COUNT {
                let content = format!("content_of_file_{i}");
                // Create unique digests using the index.
                let mut hash = [0u8; 32];
                hash[0] = 50;
                hash[1] = (i >> 8) as u8;
                hash[2] = (i & 0xff) as u8;
                let digest = DigestInfo::new(hash, content.len() as u64);

                slow_store
                    .as_ref()
                    .update_oneshot(digest, content.into())
                    .await?;

                // Pre-populate every 3rd file in the fast store to test
                // the mixed cached/uncached path.
                if i % 3 == 0 {
                    let content_again = format!("content_of_file_{i}");
                    fast_store
                        .as_ref()
                        .update_oneshot(digest, content_again.into())
                        .await?;
                }

                file_nodes.push(FileNode {
                    name: format!("file_{i:04}.txt"),
                    digest: Some(digest.into()),
                    ..Default::default()
                });
            }

            let root_digest = DigestInfo::new([51u8; 32], 32);
            let root_dir = Directory {
                files: file_nodes,
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_dir.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_many");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // Verify all files.
        for i in 0..FILE_COUNT {
            let expected = format!("content_of_file_{i}");
            let path = format!("{download_dir}/file_{i:04}.txt");
            let content = fs::read(&path).await?;
            assert_eq!(
                from_utf8(&content)?,
                expected,
                "Content mismatch for file {i}"
            );
        }

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_missing_blob_returns_error_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Verifies that a reference to a missing blob in the slow store
        // propagates an error (not silently ignored).
        const FILE_NAME: &str = "missing.txt";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Reference a file content digest that does NOT exist in any store.
            let missing_content_digest = DigestInfo::new([60u8; 32], 100);

            let root_digest = DigestInfo::new([61u8; 32], 32);
            let root_directory = Directory {
                files: vec![FileNode {
                    name: FILE_NAME.to_string(),
                    digest: Some(missing_content_digest.into()),
                    ..Default::default()
                }],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_directory.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_missing_blob");
        fs::create_dir_all(&download_dir).await?;
        let result = download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await;

        assert!(result.is_err(), "Expected error for missing blob");
        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_missing_directory_digest_returns_error_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Verifies that a DirectoryNode referencing a non-existent directory
        // digest propagates an error during tree resolution.
        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Reference a child directory digest that does NOT exist.
            let missing_child_digest = DigestInfo::new([70u8; 32], 32);

            let root_digest = DigestInfo::new([71u8; 32], 32);
            let root_directory = Directory {
                directories: vec![DirectoryNode {
                    name: "missing_dir".to_string(),
                    digest: Some(missing_child_digest.into()),
                }],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_directory.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_missing_dir");
        fs::create_dir_all(&download_dir).await?;
        let result = download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "Expected error for missing directory digest"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_zero_digest_file_test() -> Result<(), Box<dyn core::error::Error>>
    {
        // Verifies that zero-digest (empty) files are created correctly.
        // Zero-digest files have special handling and skip batch existence checks.
        const EMPTY_FILE_NAME: &str = "empty.txt";
        const NORMAL_FILE_NAME: &str = "normal.txt";
        const NORMAL_CONTENT: &str = "NORMAL_DATA";

        // SHA-256 of zero bytes.
        const ZERO_HASH: [u8; 32] = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            let zero_digest = DigestInfo::new(ZERO_HASH, 0);
            let normal_digest = DigestInfo::new([80u8; 32], NORMAL_CONTENT.len() as u64);
            slow_store
                .as_ref()
                .update_oneshot(normal_digest, NORMAL_CONTENT.into())
                .await?;

            let root_digest = DigestInfo::new([81u8; 32], 32);
            let root_directory = Directory {
                files: vec![
                    FileNode {
                        name: EMPTY_FILE_NAME.to_string(),
                        digest: Some(zero_digest.into()),
                        ..Default::default()
                    },
                    FileNode {
                        name: NORMAL_FILE_NAME.to_string(),
                        digest: Some(normal_digest.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_directory.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_zero");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // Zero-digest file should exist and be empty.
        let empty_path = format!("{download_dir}/{EMPTY_FILE_NAME}");
        let empty_content = fs::read(&empty_path).await?;
        assert_eq!(empty_content.len(), 0, "Zero-digest file should be empty");

        // Normal file should also exist.
        let normal_content = fs::read(format!("{download_dir}/{NORMAL_FILE_NAME}")).await?;
        assert_eq!(from_utf8(&normal_content)?, NORMAL_CONTENT);

        Ok(())
    }

    #[nativelink_test]
    async fn ensure_output_files_full_directories_are_created_no_working_directory_test()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        {
            let command = Command {
                arguments: vec!["touch".to_string(), "./some/path/test.txt".to_string()],
                output_files: vec!["some/path/test.txt".to_string()],
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: "some_cwd".to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_pin(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            let running_action = running_action.clone().prepare_action().await?;

            // The folder should have been created for our output file.
            assert_eq!(
                fs::metadata(format!(
                    "{}/{}",
                    running_action.get_work_directory(),
                    "some/path"
                ))
                .await
                .is_ok(),
                true,
                "Expected path to exist"
            );

            running_action.cleanup().await?;
        };
        Ok(())
    }

    #[nativelink_test]
    async fn ensure_output_files_full_directories_are_created_test()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        {
            let working_directory = "some_cwd";
            let command = Command {
                arguments: vec!["touch".to_string(), "./some/path/test.txt".to_string()],
                output_files: vec!["some/path/test.txt".to_string()],
                working_directory: working_directory.to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: "some_cwd".to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_pin(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            let running_action = running_action.clone().prepare_action().await?;

            // The folder should have been created for our output file.
            assert_eq!(
                fs::metadata(format!(
                    "{}/{}/{}",
                    running_action.get_work_directory(),
                    working_directory,
                    "some/path"
                ))
                .await
                .is_ok(),
                true,
                "Expected path to exist"
            );

            running_action.cleanup().await?;
        };
        Ok(())
    }

    #[nativelink_test]
    async fn blake3_upload_files() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        let action_result = {
            #[cfg(target_family = "unix")]
            let arguments = vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '123 ' > ./test.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                    .to_string(),
            ];
            #[cfg(target_family = "windows")]
            let arguments = vec![
                "cmd".to_string(),
                "/C".to_string(),
                // Note: Windows adds two spaces after 'set /p=XXX'.
                "echo | set /p=123> ./test.txt & echo | set /p=foo-stdout & echo | set /p=bar-stderr 1>&2 & exit 0"
                    .to_string(),
            ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string()],
                working_directory: working_directory.to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Blake3.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_pin(),
                                &mut DigestHasherFunc::Blake3.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_pin(),
                &mut DigestHasherFunc::Blake3.hasher(),
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
                &mut DigestHasherFunc::Blake3.hasher(),
            )
            .await?;

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                digest_function: ProtoDigestFunction::Blake3.into(),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "3f488ba478fc6716c756922c9f34ebd7e84b85c3e03e33e22e7a3736cafdc6d8",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "af1720193ae81515067a3ef39f0dfda3ad54a1a9d216e55d32fe5c1e178c6a7d",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "65e0abbae32a3aedaf040b654c6f02ace03c7690c17a8415a90fc2ec9c809a16",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn upload_files_from_above_cwd_test() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        let action_result = {
            #[cfg(target_family = "unix")]
            let arguments = vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '123 ' > ./test.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                    .to_string(),
            ];
            #[cfg(target_family = "windows")]
            let arguments = vec![
                "cmd".to_string(),
                "/C".to_string(),
                // Note: Windows adds two spaces after 'set /p=XXX'.
                "echo | set /p=123> ./test.txt & echo | set /p=foo-stdout & echo | set /p=bar-stderr 1>&2 & exit 0"
                    .to_string(),
            ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string()],
                working_directory: working_directory.to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_pin(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "c69e10a5f54f4e28e33897fbd4f8701595443fa8c3004aeaa20dd4d9a463483b",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "15019a676f057d97d1ad3af86f3cc1e623cb33b18ff28422bbe3248d2471cc94",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "2375ab8a01ca11e1ea7606dfb58756c153d49733cde1dbfb5a1e00f39afacf06",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    // Windows does not support symlinks.
    #[cfg(not(target_family = "windows"))]
    #[nativelink_test]
    async fn upload_dir_and_symlink_test() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        let queued_timestamp = make_system_time(1000);
        let action_result = {
            let command = Command {
                arguments: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    concat!(
                        "mkdir -p dir1/dir2 && ",
                        "echo foo > dir1/file && ",
                        "touch dir1/file2 && ",
                        "ln -s ../file dir1/dir2/sym &&",
                        "ln -s /dev/null empty_sym",
                    )
                    .to_string(),
                ],
                output_paths: vec!["dir1".to_string(), "empty_sym".to_string()],
                working_directory: ".".to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: Some(queued_timestamp.into()),
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let tree = get_and_decode_digest::<Tree>(
            cas_store.as_ref(),
            action_result.output_folders[0].tree_digest.into(),
        )
        .await?;
        let root_directory = Directory {
            files: vec![
                FileNode {
                    name: "file".to_string(),
                    digest: Some(
                        DigestInfo::try_new(
                            "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c",
                            4,
                        )?
                        .into(),
                    ),
                    ..Default::default()
                },
                FileNode {
                    name: "file2".to_string(),
                    digest: Some(
                        DigestInfo::try_new(
                            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                            0,
                        )?
                        .into(),
                    ),
                    ..Default::default()
                },
            ],
            directories: vec![DirectoryNode {
                name: "dir2".to_string(),
                digest: Some(
                    DigestInfo::try_new(
                        "cce0098e0b0f1d785edb0da50beedb13e27dcd459b091b2f8f82543cb7cd0527",
                        16,
                    )?
                    .into(),
                ),
            }],
            ..Default::default()
        };
        assert_eq!(
            tree,
            Tree {
                root: Some(root_directory.clone()),
                children: vec![
                    Directory {
                        symlinks: vec![SymlinkNode {
                            name: "sym".to_string(),
                            target: "../file".to_string(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    root_directory
                ],
            }
        );
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![],
                stdout_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                stderr_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                exit_code: 0,
                output_folders: vec![DirectoryInfo {
                    path: "dir1".to_string(),
                    tree_digest: DigestInfo::try_new(
                        "adbb04fa6e166e663c1310bbf8ba494e468b1b6c33e1e5346e2216b6904c9917",
                        490
                    )?,
                }],
                output_file_symlinks: vec![SymlinkInfo {
                    name_or_path: NameOrPath::Path("empty_sym".to_string()),
                    target: "/dev/null".to_string(),
                }],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn cleanup_happens_on_job_failure() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
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
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        let queued_timestamp = make_system_time(1000);

        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "exit 33".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["cmd".to_string(), "/C".to_string(), "exit 33".to_string()];

        let action_result = {
            let command = Command {
                arguments,
                output_paths: vec![],
                working_directory: ".".to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: Some(queued_timestamp.into()),
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![],
                stdout_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                stderr_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                exit_code: 33,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        let mut dir_stream = fs::read_dir(&root_action_directory).await?;
        assert!(
            dir_stream.as_mut().next_entry().await?.is_none(),
            "Expected empty directory at {root_action_directory}"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn kill_ends_action() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

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
                deferred_output_uploads_enabled: false,
            })?);

        #[cfg(target_family = "unix")]
        let (arguments, process_started_file) = {
            let process_started_file = {
                let tmp_dir = make_temp_path("root_action_directory");
                fs::create_dir_all(&tmp_dir).await.unwrap();
                format!("{tmp_dir}/process_started")
            };
            (
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("touch {process_started_file} && sleep infinity"),
                ],
                process_started_file,
            )
        };
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let run_action_fut = run_action(running_action_impl);
        tokio::pin!(run_action_fut);

        #[cfg(target_family = "unix")]
        loop {
            assert_eq!(futures::poll!(&mut run_action_fut), Poll::Pending);
            tokio::task::yield_now().await;
            match fs::metadata(&process_started_file).await {
                Ok(_) => break,
                Err(err) => {
                    assert_eq!(err.code, Code::NotFound, "Unknown error {err:?}");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        let result = futures::join!(run_action_fut, running_actions_manager.kill_all())
            .0
            .unwrap();

        // Check that the action was killed.
        #[cfg(all(target_family = "unix", not(target_os = "macos")))]
        assert_eq!(9, result.exit_code, "Wrong exit_code - {result:?}");
        // Mac for some reason sometimes returns 1 and 9.
        #[cfg(all(target_family = "unix", target_os = "macos"))]
        assert!(
            9 == result.exit_code || 1 == result.exit_code,
            "Wrong exit_code - {result:?}"
        );
        // Note: Windows kill command returns exit code 1.
        #[cfg(target_family = "windows")]
        assert_eq!(1, result.exit_code);

        Ok(())
    }

    // This script runs a command under a wrapper script set in a config.
    // The wrapper script will print a constant string to stderr, and the test itself will
    // print to stdout. We then check the results of both to make sure the shell script was
    // invoked and the actual command was invoked under the shell script.
    #[cfg_attr(feature = "nix", ignore)]
    #[nativelink_test]
    async fn entrypoint_does_invoke_if_set() -> Result<(), Box<dyn core::error::Error>> {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/usr/bin/env bash
# Print some static text to stderr. This is what the test uses to
# make sure the script did run.
>&2 printf \"Wrapper script did run\"

# Now run the real command.
exec \"$@\"
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
:: Print some static text to stderr. This is what the test uses to
:: make sure the script did run.
echo | set /p=\"Wrapper script did run\" 1>&2

:: Run command, but morph the echo to ensure it doesn't
:: add a new line to the end of the output.
%1 | set /p=%2
exit 0
";
        const WORKER_ID: &str = "foo_worker_id";
        const EXPECTED_STDOUT: &str = "Action did run";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");
            {
                let mut file_options = std::fs::OpenOptions::new();
                file_options.create(true);
                file_options.truncate(true);
                file_options.write(true);
                #[cfg(target_family = "unix")]
                file_options.mode(0o777);
                let mut test_wrapper_script_handle = file_options
                    .open(OsString::from(&test_wrapper_script))
                    .unwrap();
                test_wrapper_script_handle
                    .write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())
                    .unwrap();
                test_wrapper_script_handle.sync_all().unwrap();
                // Note: Github runners appear to use some kind of filesystem driver
                // that does not sync data as expected. This is the easiest solution.
                // See: https://github.com/pantsbuild/pants/issues/10507
                // See: https://github.com/moby/moby/issues/9547
                std::process::Command::new("sync").output().unwrap();
            }
            test_wrapper_script
        };

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: None,
                },
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
                deferred_output_uploads_enabled: false,
            })?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["printf".to_string(), EXPECTED_STDOUT.to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["echo".to_string(), EXPECTED_STDOUT.to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 0, "Exit code should be 0");

        let expected_stdout = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(EXPECTED_STDOUT))
            .await?;
        // Note: This string should match what is in worker_for_test.sh
        let expected_stderr = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new("Wrapper script did run"))
            .await?;
        assert_eq!(expected_stdout, result.stdout_digest);
        assert_eq!(expected_stderr, result.stderr_digest);

        Ok(())
    }

    #[cfg_attr(feature = "nix", ignore)]
    #[nativelink_test]
    async fn entrypoint_injects_properties() -> Result<(), Box<dyn core::error::Error>> {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/usr/bin/env bash
# Print some static text to stderr. This is what the test uses to
# make sure the script did run.
>&2 printf \"Wrapper script did run with property $PROPERTY $VALUE $INNER_TIMEOUT\"

# Now run the real command.
exec \"$@\"
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
:: Print some static text to stderr. This is what the test uses to
:: make sure the script did run.
echo | set /p=\"Wrapper script did run with property %PROPERTY% %VALUE% %INNER_TIMEOUT%\" 1>&2

:: Run command, but morph the echo to ensure it doesn't
:: add a new line to the end of the output.
%1 | set /p=%2
exit 0
";
        const WORKER_ID: &str = "foo_worker_id";
        const EXPECTED_STDOUT: &str = "Action did run";
        const TASK_TIMEOUT: Duration = Duration::from_secs(122);

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");
            {
                let mut file_options = std::fs::OpenOptions::new();
                file_options.create(true);
                file_options.truncate(true);
                file_options.write(true);
                #[cfg(target_family = "unix")]
                file_options.mode(0o777);
                let mut test_wrapper_script_handle = file_options
                    .open(OsString::from(&test_wrapper_script))
                    .unwrap();
                test_wrapper_script_handle
                    .write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())
                    .unwrap();
                test_wrapper_script_handle.sync_all().unwrap();
                // Note: Github runners appear to use some kind of filesystem driver
                // that does not sync data as expected. This is the easiest solution.
                // See: https://github.com/pantsbuild/pants/issues/10507
                // See: https://github.com/moby/moby/issues/9547
                std::process::Command::new("sync").output().unwrap();
            }
            test_wrapper_script
        };

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: Some(HashMap::from([
                        (
                            "PROPERTY".to_string(),
                            EnvironmentSource::Property("property_name".to_string()),
                        ),
                        (
                            "VALUE".to_string(),
                            EnvironmentSource::Value("raw_value".to_string()),
                        ),
                        (
                            "INNER_TIMEOUT".to_string(),
                            EnvironmentSource::TimeoutMillis,
                        ),
                        (
                            "PATH".to_string(),
                            EnvironmentSource::Value(env::var("PATH").unwrap()),
                        ),
                    ])),
                },
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
                deferred_output_uploads_enabled: false,
            })?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["printf".to_string(), EXPECTED_STDOUT.to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["echo".to_string(), EXPECTED_STDOUT.to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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
            platform: Some(Platform {
                properties: vec![Property {
                    name: "property_name".into(),
                    value: "property_value".into(),
                }],
            }),
            timeout: Some(prost_types::Duration {
                seconds: TASK_TIMEOUT.as_secs() as i64,
                nanos: 0,
            }),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 0, "Exit code should be 0");

        let expected_stdout = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(EXPECTED_STDOUT))
            .await?;
        // Note: This string should match what is in worker_for_test.sh
        let expected_stderr =
            "Wrapper script did run with property property_value raw_value 122000";
        let expected_stderr_digest = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(expected_stderr))
            .await?;

        let actual_stderr: Bytes = cas_store
            .as_ref()
            .get_part_unchunked(result.stderr_digest, 0, None)
            .await?;
        let actual_stderr_decoded = from_utf8(&actual_stderr)?;
        assert_eq!(expected_stderr, actual_stderr_decoded);
        assert_eq!(expected_stdout, result.stdout_digest);
        assert_eq!(expected_stderr_digest, result.stderr_digest);

        Ok(())
    }

    #[cfg_attr(feature = "nix", ignore)]
    #[nativelink_test]
    async fn entrypoint_sends_timeout_via_side_channel() -> Result<(), Box<dyn core::error::Error>>
    {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/bin/bash
echo '{\"failure\":\"timeout\"}' > \"$SIDE_CHANNEL_FILE\"
exit 1
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
echo | set /p={\"failure\":\"timeout\"} 1>&2 > %SIDE_CHANNEL_FILE%
exit 1
";
        const WORKER_ID: &str = "foo_worker_id";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");
            {
                let mut file_options = std::fs::OpenOptions::new();
                file_options.create(true);
                file_options.truncate(true);
                file_options.write(true);
                #[cfg(target_family = "unix")]
                file_options.mode(0o777);
                let mut test_wrapper_script_handle = file_options
                    .open(OsString::from(&test_wrapper_script))
                    .unwrap();
                test_wrapper_script_handle
                    .write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())
                    .unwrap();
                test_wrapper_script_handle.sync_all().unwrap();
                // Note: Github runners appear to use some kind of filesystem driver
                // that does not sync data as expected. This is the easiest solution.
                // See: https://github.com/pantsbuild/pants/issues/10507
                // See: https://github.com/moby/moby/issues/9547
                std::process::Command::new("sync").output().unwrap();
            }
            test_wrapper_script
        };

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: Some(HashMap::from([(
                        "SIDE_CHANNEL_FILE".to_string(),
                        EnvironmentSource::SideChannelFile,
                    )])),
                },
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
                deferred_output_uploads_enabled: false,
            })?);
        let arguments = vec!["true".to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 1, "Exit code should be 1");
        assert_eq!(
            result.error.err_tip(|| "Error should exist")?.code,
            Code::DeadlineExceeded
        );
        Ok(())
    }

    #[nativelink_test]
    async fn caches_results_in_action_cache_store() -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::SuccessOnly,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 0,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        let retrieved_result =
            get_and_decode_digest::<ProtoActionResult>(ac_store.as_ref(), action_digest.into())
                .await?;

        let proto_result: ProtoActionResult = action_result.try_into()?;
        assert_eq!(proto_result, retrieved_result);

        Ok(())
    }

    #[nativelink_test]
    async fn failed_action_does_not_cache_in_action_cache()
    -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::Everything,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 1,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        let retrieved_result =
            get_and_decode_digest::<ProtoActionResult>(ac_store.as_ref(), action_digest.into())
                .await?;

        let proto_result: ProtoActionResult = action_result.try_into()?;
        assert_eq!(proto_result, retrieved_result);

        Ok(())
    }

    #[nativelink_test]
    async fn success_does_cache_in_historical_results() -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_historical_results_strategy: Some(
                            nativelink_config::cas_server::UploadCacheResultsStrategy::SuccessOnly,
                        ),
                        #[expect(
                            clippy::literal_string_with_formatting_args,
                            reason = "passed to `formatx` crate for runtime interpretation"
                        )]
                        success_message_template:
                            "{historical_results_hash}-{historical_results_size}".to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 0,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let historical_digest = {
            let (historical_results_hash, historical_results_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(
                historical_results_hash,
                historical_results_size.parse::<i64>()?,
            )?
        };
        let retrieved_result = get_and_decode_digest::<HistoricalExecuteResponse>(
            cas_store.as_ref(),
            historical_digest.into(),
        )
        .await?;

        assert_eq!(
            HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(ExecuteResponse {
                    result: Some(action_result.try_into()?),
                    status: Some(Status::default()),
                    ..Default::default()
                }),
            },
            retrieved_result
        );

        Ok(())
    }

    #[nativelink_test]
    async fn failure_does_not_cache_in_historical_results()
    -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_historical_results_strategy: Some(
                            nativelink_config::cas_server::UploadCacheResultsStrategy::SuccessOnly,
                        ),
                        success_message_template:
                            "{historical_results_hash}-{historical_results_size}".to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 1,
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        assert!(
            action_result.message.is_empty(),
            "Message should not be set"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn infra_failure_does_cache_in_historical_results()
    -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_historical_results_strategy: Some(
                            nativelink_config::cas_server::UploadCacheResultsStrategy::FailuresOnly,
                        ),
                        #[expect(
                            clippy::literal_string_with_formatting_args,
                            reason = "passed to `formatx` crate for runtime interpretation"
                        )]
                        failure_message_template:
                            "{historical_results_hash}-{historical_results_size}".to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 0,
            error: Some(make_input_err!("test error")),
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let historical_digest = {
            let (historical_results_hash, historical_results_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(
                historical_results_hash,
                historical_results_size.parse::<i64>()?,
            )?
        };

        let retrieved_result = get_and_decode_digest::<HistoricalExecuteResponse>(
            cas_store.as_ref(),
            historical_digest.into(),
        )
        .await?;

        assert_eq!(
            HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(ExecuteResponse {
                    result: Some(action_result.try_into()?),
                    status: Some(make_input_err!("test error").into()),
                    ..Default::default()
                }),
            },
            retrieved_result
        );
        Ok(())
    }

    #[nativelink_test]
    async fn action_result_has_used_in_message() -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::SuccessOnly,
                        success_message_template: "{action_digest_hash}-{action_digest_size}"
                            .to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 0,
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256, &nativelink_util::action_messages::OperationId::default(), "test_worker")
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let action_result_digest = {
            let (action_result_hash, action_result_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(action_result_hash, action_result_size.parse::<i64>()?)?
        };

        let retrieved_result = get_and_decode_digest::<ProtoActionResult>(
            ac_store.as_ref(),
            action_result_digest.into(),
        )
        .await?;

        let proto_result: ProtoActionResult = action_result.try_into()?;
        assert_eq!(proto_result, retrieved_result);
        Ok(())
    }

    #[nativelink_test]
    async fn ensure_worker_timeout_chooses_correct_values()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;

        #[cfg(target_family = "unix")]
        let arguments = vec!["true".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "exit".to_string(),
            "0".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        {
            // Test to ensure that the task timeout is chosen if it is less than the max timeout.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(10);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
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
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(
                            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                            Ordering::Relaxed,
                        );
                        Box::pin(future::pending())
                    },
                },
            )?);

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: Some(make_system_time(1000).into()),
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await?;
            assert_eq!(
                SENT_TIMEOUT.load(Ordering::Relaxed),
                i64::try_from(TASK_TIMEOUT.as_millis())
                    .expect("TASK_TIMEOUT.as_millis() exceeds i64::MAX")
            );
        }
        {
            // Ensure if no timeout is set use max timeout.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(0);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
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
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(
                            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                            Ordering::Relaxed,
                        );
                        Box::pin(future::pending())
                    },
                },
            )?);

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: Some(make_system_time(1000).into()),
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await?;
            assert_eq!(
                SENT_TIMEOUT.load(Ordering::Relaxed),
                i64::try_from(MAX_TIMEOUT_DURATION.as_millis())
                    .expect("MAX_TIMEOUT_DURATION.as_millis() exceeds i64::MAX")
            );
        }
        {
            // Ensure we reject tasks that have a timeout set too high.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(200);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
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
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(
                            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
                            Ordering::Relaxed,
                        );
                        Box::pin(future::pending())
                    },
                },
            )?);

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let result = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: Some(make_system_time(1000).into()),
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await;
            assert_eq!(SENT_TIMEOUT.load(Ordering::Relaxed), -1);
            assert_eq!(result.err().unwrap().code, Code::InvalidArgument);
        }
        Ok(())
    }

    #[nativelink_test]
    async fn worker_times_out() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        type StaticOneshotTuple =
            Mutex<(Option<oneshot::Sender<()>>, Option<oneshot::Receiver<()>>)>;
        static TIMEOUT_ONESHOT: LazyLock<StaticOneshotTuple> = LazyLock::new(|| {
            let (tx, rx) = oneshot::channel();
            Mutex::new((Some(tx), Some(rx)))
        });
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
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
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| {
                    Box::pin(async move {
                        let rx = TIMEOUT_ONESHOT.lock().unwrap().1.take().unwrap();
                        rx.await.expect("Could not receive timeout signal");
                    })
                },
            },
        )?);

        #[cfg(target_family = "unix")]
        let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep infinity".to_string(),
        ];
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let execute_results_fut = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .and_then(|action| async move {
                let result = action
                    .clone()
                    .prepare_action()
                    .await?
                    .execute()
                    .await?
                    .upload_results()
                    .await?
                    .get_finished_result()
                    .await;
                if let Err(e) = action.cleanup().await {
                    return Result::<ActionResult, Error>::Err(e).merge(result);
                }
                result
            });

        let (results, ()) = tokio::join!(execute_results_fut, async move {
            tokio::task::yield_now().await;
            let tx = TIMEOUT_ONESHOT.lock().unwrap().0.take().unwrap();
            tx.send(()).expect("Could not send timeout signal");
        });
        assert_eq!(results?.error.unwrap().code, Code::DeadlineExceeded);

        #[cfg(target_family = "unix")]
        let command = "[\"sh\", \"-c\", \"sleep infinity\"]";
        #[cfg(target_family = "windows")]
        let command = "[\"cmd\", \"/C\", \"ping -n 99999 127.0.0.1\"]";

        assert!(logs_contain(&format!("Executing command args={command}")));
        assert!(logs_contain(&format!("Command complete args={command}")));

        assert!(!logs_contain(
            "Child process was not cleaned up before dropping the call to execute(), killing in background spawn"
        ));
        #[cfg(target_family = "unix")]
        assert!(logs_contain(
            "Command timed out seconds=0.0 command=sh -c sleep infinity"
        ));
        #[cfg(target_family = "windows")]
        assert!(logs_contain(
            "Command timed out seconds=0.0 command=cmd /C ping -n 99999 127.0.0.1"
        ));

        Ok(())
    }

    #[nativelink_test]
    async fn kill_all_waits_for_all_tasks_to_finish() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
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
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        #[cfg(target_family = "unix")]
        let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep infinity".to_string(),
        ];
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let (cleanup_tx, cleanup_rx) = oneshot::channel();
        let cleanup_was_requested = Arc::new(AtomicBool::new(false));
        let action = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;
        let execute_results_fut = {
            let action = action.clone();
            let cleanup_was_requested = cleanup_was_requested.clone();
            async move {
                let result = action
                    .clone()
                    .prepare_action()
                    .await?
                    .execute()
                    .await?
                    .upload_results()
                    .await?
                    .get_finished_result()
                    .await;
                cleanup_was_requested.store(true, Ordering::Release);
                cleanup_rx.await.expect("Could not receive cleanup signal");
                if let Err(e) = action.cleanup().await {
                    return Result::<ActionResult, Error>::Err(e).merge(result);
                }
                result
            }
        };

        tokio::pin!(execute_results_fut);
        {
            // Advance the action as far as possible and ensure we are not waiting on cleanup.
            for _ in 0..100 {
                assert!(futures::poll!(&mut execute_results_fut).is_pending());
                tokio::task::yield_now().await;
            }
            assert_eq!(cleanup_was_requested.load(Ordering::Acquire), false);
        }

        let kill_all_fut = running_actions_manager.kill_all();
        tokio::pin!(kill_all_fut);

        {
            // * Advance the action as far as possible.
            // * Ensure we are now waiting on cleanup.
            // * Ensure our kill_action is still pending.
            while !cleanup_was_requested.load(Ordering::Acquire) {
                // Wait for cleanup to be triggered.
                tokio::task::yield_now().await;
                assert!(futures::poll!(&mut execute_results_fut).is_pending());
                assert!(futures::poll!(&mut kill_all_fut).is_pending());
            }
        }
        // Allow cleanup, which allows execute_results_fut to advance.
        cleanup_tx.send(()).expect("Could not send cleanup signal");
        // Advance our two futures to completion now.
        let result = execute_results_fut.await;
        kill_all_fut.await;
        {
            // Ensure our results are correct.
            let action_result = result?;
            let err = action_result
                .error
                .as_ref()
                .err_tip(|| format!("No error exists in result : {action_result:?}"))?;
            assert_eq!(
                err.code,
                Code::Aborted,
                "Expected Aborted : {action_result:?}"
            );
        }

        Ok(())
    }

    /// Regression Test for Issue #675
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn unix_executable_file_test() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";
        const FILE_1_NAME: &str = "file1";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store.clone())),
                ac_mirror_target: None,
                execution_configuration: ExecutionConfiguration::default(),
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
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        // Create and run an action which
        // creates a file with owner executable permissions.
        let action_result = {
            let command = Command {
                arguments: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("touch {FILE_1_NAME} && chmod 700 {FILE_1_NAME}"),
                ],
                output_paths: vec![FILE_1_NAME.to_string()],
                working_directory: ".".to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        ..Default::default()
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        // Ensure the file copied from worker to CAS is executable.
        assert!(
            action_result.output_files[0].is_executable,
            "Expected output file to be executable"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn action_directory_contents_are_cleaned() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        let temp_action_directory = make_temp_path("root_action_directory/temp");
        fs::create_dir_all(&temp_action_directory).await?;

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
                deferred_output_uploads_enabled: false,
            })?);
        let queued_timestamp = make_system_time(1000);

        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["cmd".to_string(), "/C".to_string(), "exit 0".to_string()];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(queued_timestamp.into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        run_action(running_action_impl.clone()).await?;

        let mut dir_stream = fs::read_dir(&root_action_directory).await?;
        assert!(
            dir_stream.as_mut().next_entry().await?.is_none(),
            "Expected empty directory at {root_action_directory}"
        );
        Ok(())
    }

    // We've experienced deadlocks when uploading, so make only a single permit available and
    // check it's able to handle uploading some directories with some files in.

    // TODO(palfrey) This is unix only only because I was lazy and didn't spend the time to
    // build the bash-like commands in windows as well.

    #[nativelink_test]
    #[cfg(target_family = "unix")]
    async fn upload_with_single_permit() -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        // Take all but one FD permit away.
        let _permits = stream::iter(1..fs::OPEN_FILE_SEMAPHORE.available_permits())
            .then(|_| fs::OPEN_FILE_SEMAPHORE.acquire())
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(1, fs::OPEN_FILE_SEMAPHORE.available_permits());

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);
        let action_result = {
            let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "printf '123 ' > ./test.txt; mkdir ./tst; printf '456 ' > ./tst/tst.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                .to_string(),
        ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string(), "tst".to_string()],
                working_directory: working_directory.to_string(),
                environment_variables: vec![EnvironmentVariable {
                    name: "PATH".to_string(),
                    value: env::var("PATH").unwrap(),
                }],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_pin(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
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

            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = cas_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "c69e10a5f54f4e28e33897fbd4f8701595443fa8c3004aeaa20dd4d9a463483b",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "15019a676f057d97d1ad3af86f3cc1e623cb33b18ff28422bbe3248d2471cc94",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "2375ab8a01ca11e1ea7606dfb58756c153d49733cde1dbfb5a1e00f39afacf06",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![DirectoryInfo {
                    path: "tst".to_string(),
                    tree_digest: DigestInfo::try_new(
                        "95711c1905d4898a70209dd6e98241dcafb479c00241a1ea4ed8415710d706f3",
                        166,
                    )?,
                },],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn running_actions_manager_respects_action_timeout()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        // Ignore the sleep and immediately timeout.
        static ACTION_TIMEOUT: i64 = 1;
        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_work_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                // If action_timeout is the passed duration then return immediately,
                // which will cause the action to be killed and pass the test,
                // otherwise return pending and fail the test.
                sleep_fn: |duration| {
                    assert_eq!(duration.as_secs(), ACTION_TIMEOUT as u64);
                    Box::pin(future::ready(()))
                },
            },
        )?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "sleep 2".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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
            platform: Some(Platform {
                properties: vec![Property {
                    name: "property_name".into(),
                    value: "property_value".into(),
                }],
            }),
            timeout: Some(prost_types::Duration {
                seconds: ACTION_TIMEOUT,
                nanos: 0,
            }),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: Some(make_system_time(1000).into()),
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;

        #[cfg(target_family = "unix")]
        assert_eq!(result.exit_code, 9, "Action process should be been killed");
        #[cfg(target_family = "windows")]
        assert_eq!(result.exit_code, 1, "Action process should be been killed");
        Ok(())
    }

    #[nativelink_test]
    async fn test_handles_stale_directory_on_retry() -> Result<(), Error> {
        const WORKER_ID: &str = "foo_worker_id";
        let (_, ac_store, cas_store, _) = setup_stores().await?;
        let root_action_directory = make_temp_path("retry_work_directory");

        // Ensure root directory exists
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: None,
                    additional_environment: None,
                },
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
                deferred_output_uploads_enabled: false,
            })?);

        // Create a simple action
        let command = Command {
            arguments: vec!["echo".to_string(), "test".to_string()],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };

        // Use a fixed operation ID to simulate retry with same ID
        let operation_id = "test-retry-operation-fixed-id".to_string();

        // Create the directory manually to simulate a previous failed action
        let action_directory = format!("{root_action_directory}/{operation_id}");
        eprintln!("Creating directory: {action_directory}");
        fs::create_dir_all(&action_directory).await?;

        // Also create the work subdirectory to ensure conflict
        let work_directory = format!("{action_directory}/work");
        fs::create_dir_all(&work_directory).await?;

        // Add a marker file to detect if directory is deleted and recreated
        let marker_file = format!("{action_directory}/marker.txt");
        tokio::fs::write(&marker_file, "test").await?;

        // Verify the directory was created
        assert!(
            tokio::fs::metadata(&action_directory).await.is_ok(),
            "Directory should exist"
        );
        assert!(
            tokio::fs::metadata(&work_directory).await.is_ok(),
            "Work directory should exist"
        );
        assert!(
            tokio::fs::metadata(&marker_file).await.is_ok(),
            "Marker file should exist"
        );

        // Now try to create an action with the same operation ID
        // This should fail with "File exists" error
        eprintln!("Attempting to create action with existing directory...");
        let result = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id: operation_id.clone(),
                    queued_timestamp: Some(SystemTime::now().into()),
                    platform: None,
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await;

        // Verify the behavior - with the fix, it should succeed after removing stale directory
        match result {
            Ok(_) => {
                // Check if the directory still exists and if marker file is gone
                let dir_exists = tokio::fs::metadata(&action_directory).await.is_ok();
                let marker_exists = tokio::fs::metadata(&marker_file).await.is_ok();
                eprintln!(
                    "SUCCESS: Directory collision handled gracefully. Directory exists: {dir_exists}, Marker exists: {marker_exists}"
                );
                assert!(
                    dir_exists,
                    "Directory should exist after successful creation"
                );
                assert!(
                    !marker_exists,
                    "Marker file should be gone - stale directory was cleaned up"
                );
                eprintln!(
                    "PASSED: The fix is working - stale directory was removed and action proceeded"
                );
            }
            Err(err) => {
                panic!("Expected success after fix, but got error: {err}");
            }
        }

        // Clean up
        fs::remove_dir_all(&root_action_directory).await?;
        Ok(())
    }

    #[nativelink_test]
    async fn test_retry_after_cleanup_succeeds() -> Result<(), Error> {
        const WORKER_ID: &str = "foo_worker_id";
        let (_, ac_store, cas_store, _) = setup_stores().await?;
        let root_action_directory = make_temp_path("retry_after_cleanup_work_directory");

        // Ensure root directory exists
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: None,
                    additional_environment: None,
                },
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
                deferred_output_uploads_enabled: false,
            })?);

        // Create a simple action
        let command = Command {
            arguments: vec!["echo".to_string(), "test".to_string()],
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

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };

        let operation_id = "test-retry-after-cleanup-fixed-id".to_string();

        // First, create and execute an action
        let action1 = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request.clone()),
                    operation_id: operation_id.clone(),
                    queued_timestamp: Some(SystemTime::now().into()),
                    platform: None,
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        // Clean up the action
        action1.cleanup().await?;

        // Give cleanup a moment to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now try to create another action with the same operation ID
        // This should succeed because the directory has been cleaned up
        let result = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id: operation_id.clone(),
                    queued_timestamp: Some(SystemTime::now().into()),
                    platform: None,
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await;

        assert!(
            result.is_ok(),
            "Expected success when creating action after cleanup, got: {:?}",
            result.err()
        );

        // Clean up
        if let Ok(action2) = result {
            action2.cleanup().await?;
        }
        fs::remove_dir_all(&root_action_directory).await?;
        Ok(())
    }

    #[nativelink_test]
    async fn parse_get_tree_response_with_missing_directory_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Regression test: when the server's GetTree response skips a missing
        // directory (tolerant mode), the digest-based parsing must still
        // correctly identify each directory. The tree structure is:
        //   root → [A, B]    (server skips B because it's missing)
        //   A → [std]
        //   std → (leaf file)
        //
        // With the old position-based parser, skipping B would shift positions
        // and assign std's content to B's digest, losing std entirely.
        use nativelink_worker::running_actions_manager::parse_get_tree_response;

        // Build directories bottom-up so digests are content-based.
        let file_digest = DigestInfo::new([5u8; 32], 10);

        // std/ directory — contains mod.rs
        let std_dir = Directory {
            files: vec![FileNode {
                name: "mod.rs".to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let std_encoded = std_dir.encode_to_vec();
        let std_digest = {
            let mut hasher = nativelink_util::digest_hasher::default_digest_hasher_func().hasher();
            hasher.update(&std_encoded);
            hasher.finalize_digest()
        };

        // A/ directory — contains std/
        let a_dir = Directory {
            directories: vec![DirectoryNode {
                name: "std".to_string(),
                digest: Some(std_digest.into()),
            }],
            ..Default::default()
        };
        let a_encoded = a_dir.encode_to_vec();
        let a_digest = {
            let mut hasher = nativelink_util::digest_hasher::default_digest_hasher_func().hasher();
            hasher.update(&a_encoded);
            hasher.finalize_digest()
        };

        // B/ directory — this one will be MISSING from the response.
        let b_digest = DigestInfo::new([99u8; 32], 50);

        // root/ directory — contains A/ and B/
        let root_dir = Directory {
            directories: vec![
                DirectoryNode {
                    name: "A".to_string(),
                    digest: Some(a_digest.into()),
                },
                DirectoryNode {
                    name: "B".to_string(),
                    digest: Some(b_digest.into()),
                },
            ],
            ..Default::default()
        };
        let root_encoded = root_dir.encode_to_vec();
        let root_digest = {
            let mut hasher = nativelink_util::digest_hasher::default_digest_hasher_func().hasher();
            hasher.update(&root_encoded);
            hasher.finalize_digest()
        };

        // Server sends BFS order but SKIPS B (missing from CAS).
        // Full BFS would be: [root, A, B, std]
        // Tolerant response: [root, A, std]  (B omitted)
        let response_dirs = vec![root_dir, a_dir, std_dir];

        let tree = parse_get_tree_response(response_dirs, &root_digest);

        // Root should be in the tree.
        assert!(tree.contains_key(&root_digest), "root should be in tree");

        // A should be in the tree.
        assert!(tree.contains_key(&a_digest), "A should be in tree");

        // std should be in the tree under its correct digest.
        assert!(
            tree.contains_key(&std_digest),
            "std directory should be in tree under its correct digest"
        );

        // B should NOT be in the tree (it was skipped).
        assert!(
            !tree.contains_key(&b_digest),
            "B should not be in tree (it was missing)"
        );

        // Verify std has the right content.
        let std_entry = tree.get(&std_digest).unwrap();
        assert_eq!(std_entry.files.len(), 1);
        assert_eq!(std_entry.files[0].name, "mod.rs");

        // Verify the tree validation would detect the gap (B is missing).
        let all_children_present = tree.values().all(|dir| {
            dir.directories.iter().all(|node| {
                node.digest
                    .as_ref()
                    .and_then(|d| DigestInfo::try_from(d).ok())
                    .is_some_and(|d| tree.contains_key(&d))
            })
        });
        assert!(
            !all_children_present,
            "tree validation should detect B is missing"
        );

        Ok(())
    }

    #[nativelink_test]
    async fn parse_get_tree_response_orphan_root_fallback_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Test the orphan-detection fallback: when the caller's root_digest
        // doesn't match the computed digest of any directory (e.g., due to
        // protobuf serialization differences), the function identifies the
        // root as the unique "orphan" — a directory not referenced as a child
        // by any other directory — and re-keys it under root_digest.
        use nativelink_worker::running_actions_manager::parse_get_tree_response;

        let file_digest = DigestInfo::new([7u8; 32], 20);

        // child/ directory
        let child_dir = Directory {
            files: vec![FileNode {
                name: "data.bin".to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let child_encoded = child_dir.encode_to_vec();
        let child_digest = {
            let mut hasher = nativelink_util::digest_hasher::default_digest_hasher_func().hasher();
            hasher.update(&child_encoded);
            hasher.finalize_digest()
        };

        // root/ directory — contains child/
        let root_dir = Directory {
            directories: vec![DirectoryNode {
                name: "child".to_string(),
                digest: Some(child_digest.into()),
            }],
            ..Default::default()
        };

        // Simulate a root_digest that differs from the computed digest
        // (as if the server serialized the proto differently).
        let fake_root_digest = DigestInfo::new([42u8; 32], 999);

        let response_dirs = vec![root_dir.clone(), child_dir.clone()];
        let tree = parse_get_tree_response(response_dirs, &fake_root_digest);

        // The root should be re-keyed under fake_root_digest.
        assert!(
            tree.contains_key(&fake_root_digest),
            "root should be re-keyed under the caller's root_digest"
        );
        let root_entry = tree.get(&fake_root_digest).unwrap();
        assert_eq!(root_entry.directories.len(), 1);
        assert_eq!(root_entry.directories[0].name, "child");

        // The child should still be accessible under its computed digest.
        assert!(
            tree.contains_key(&child_digest),
            "child should remain under its computed digest"
        );
        let child_entry = tree.get(&child_digest).unwrap();
        assert_eq!(child_entry.files.len(), 1);
        assert_eq!(child_entry.files[0].name, "data.bin");

        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_nested_std_directory_test()
    -> Result<(), Box<dyn core::error::Error>> {
        // Regression test for the rustix `maybe_polyfill/std/mod.rs` bug.
        // Verifies that a directory literally named "std" (which collides with
        // Rust's standard library name) is materialized correctly during
        // remote execution input fetch. The tree structure mimics:
        //   root/
        //     src/
        //       maybe_polyfill/
        //         std/
        //           mod.rs
        //       lib.rs
        const MOD_RS_CONTENT: &str = "// std polyfill module";
        const LIB_RS_CONTENT: &str = "pub mod maybe_polyfill;";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Upload file contents.
            let mod_rs_digest = DigestInfo::new([80u8; 32], MOD_RS_CONTENT.len() as u64);
            slow_store
                .as_ref()
                .update_oneshot(mod_rs_digest, MOD_RS_CONTENT.into())
                .await?;

            let lib_rs_digest = DigestInfo::new([81u8; 32], LIB_RS_CONTENT.len() as u64);
            slow_store
                .as_ref()
                .update_oneshot(lib_rs_digest, LIB_RS_CONTENT.into())
                .await?;

            // std/ directory (deepest) — contains mod.rs
            let std_digest = DigestInfo::new([82u8; 32], 32);
            let std_dir = Directory {
                files: vec![FileNode {
                    name: "mod.rs".to_string(),
                    digest: Some(mod_rs_digest.into()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(std_digest, std_dir.encode_to_vec().into())
                .await?;

            // maybe_polyfill/ directory — contains std/
            let maybe_polyfill_digest = DigestInfo::new([83u8; 32], 32);
            let maybe_polyfill_dir = Directory {
                directories: vec![DirectoryNode {
                    name: "std".to_string(),
                    digest: Some(std_digest.into()),
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(
                    maybe_polyfill_digest,
                    maybe_polyfill_dir.encode_to_vec().into(),
                )
                .await?;

            // src/ directory — contains maybe_polyfill/ and lib.rs
            let src_digest = DigestInfo::new([84u8; 32], 32);
            let src_dir = Directory {
                files: vec![FileNode {
                    name: "lib.rs".to_string(),
                    digest: Some(lib_rs_digest.into()),
                    ..Default::default()
                }],
                directories: vec![DirectoryNode {
                    name: "maybe_polyfill".to_string(),
                    digest: Some(maybe_polyfill_digest.into()),
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(src_digest, src_dir.encode_to_vec().into())
                .await?;

            // root directory — contains src/
            let root_digest = DigestInfo::new([85u8; 32], 32);
            let root_dir = Directory {
                directories: vec![DirectoryNode {
                    name: "src".to_string(),
                    digest: Some(src_digest.into()),
                }],
                ..Default::default()
            };
            slow_store
                .as_ref()
                .update_oneshot(root_digest, root_dir.encode_to_vec().into())
                .await?;
            root_digest
        };

        let download_dir = make_temp_path("download_dir_std");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        // The critical assertion: std/mod.rs must exist.
        let mod_rs_path = format!("{download_dir}/src/maybe_polyfill/std/mod.rs");
        let content = fs::read(&mod_rs_path).await?;
        assert_eq!(
            from_utf8(&content)?,
            MOD_RS_CONTENT,
            "maybe_polyfill/std/mod.rs should have correct content"
        );

        // Verify the directory named "std" exists as a directory.
        let std_meta = fs::metadata(format!("{download_dir}/src/maybe_polyfill/std")).await?;
        assert!(std_meta.is_dir(), "std should be a directory");

        // Verify lib.rs also exists.
        let lib_rs_path = format!("{download_dir}/src/lib.rs");
        let lib_content = fs::read(&lib_rs_path).await?;
        assert_eq!(from_utf8(&lib_content)?, LIB_RS_CONTENT);

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────
    // Server missing digest hints tests
    // ─────────────────────────────────────────────────────────────────────

    /// When server_missing_digests is provided, download_to_directory
    /// should skip the has_with_results check and treat the hinted
    /// digests as missing (to be fetched from the slow store).
    #[nativelink_test]
    async fn download_to_directory_with_server_missing_hints()
    -> Result<(), Box<dyn core::error::Error>> {
        const FILE1_NAME: &str = "cached.txt";
        const FILE1_CONTENT: &str = "ALREADY_CACHED";
        const FILE2_NAME: &str = "missing.txt";
        const FILE2_CONTENT: &str = "NEEDS_FETCH";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let file1_digest = DigestInfo::new([20u8; 32], FILE1_CONTENT.len() as u64);
        let file2_digest = DigestInfo::new([21u8; 32], FILE2_CONTENT.len() as u64);

        // Put file1 in both stores (cached).
        slow_store
            .as_ref()
            .update_oneshot(file1_digest, FILE1_CONTENT.into())
            .await?;
        fast_store
            .as_ref()
            .update_oneshot(file1_digest, FILE1_CONTENT.into())
            .await?;

        // Put file2 only in slow store (not cached on fast).
        slow_store
            .as_ref()
            .update_oneshot(file2_digest, FILE2_CONTENT.into())
            .await?;

        let root_directory_digest = DigestInfo::new([22u8; 32], 32);
        let root_directory = Directory {
            files: vec![
                FileNode {
                    name: FILE1_NAME.to_string(),
                    digest: Some(file1_digest.into()),
                    ..Default::default()
                },
                FileNode {
                    name: FILE2_NAME.to_string(),
                    digest: Some(file2_digest.into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        // Provide server hints saying file2 is missing.
        let mut missing = HashSet::new();
        missing.insert(file2_digest);

        let download_dir = make_temp_path("download_dir_hints");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            Some(missing),
            None,
        )
        .await?;

        // Both files should be present with correct content.
        let content1 = fs::read(format!("{download_dir}/{FILE1_NAME}")).await?;
        assert_eq!(from_utf8(&content1)?, FILE1_CONTENT);

        let content2 = fs::read(format!("{download_dir}/{FILE2_NAME}")).await?;
        assert_eq!(from_utf8(&content2)?, FILE2_CONTENT);

        Ok(())
    }

    /// Verify that stale hints (marking a blob as missing when it's
    /// actually cached) still work -- the blob gets re-fetched from
    /// the slow store even though it was already in the fast store.
    #[nativelink_test]
    async fn download_to_directory_stale_missing_hints() -> Result<(), Box<dyn core::error::Error>>
    {
        const FILE_NAME: &str = "stale.txt";
        const FILE_CONTENT: &str = "STALE_HINT_FILE";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let file_digest = DigestInfo::new([30u8; 32], FILE_CONTENT.len() as u64);

        // Put the file in BOTH stores.
        slow_store
            .as_ref()
            .update_oneshot(file_digest, FILE_CONTENT.into())
            .await?;
        fast_store
            .as_ref()
            .update_oneshot(file_digest, FILE_CONTENT.into())
            .await?;

        let root_directory_digest = DigestInfo::new([31u8; 32], 32);
        let root_directory = Directory {
            files: vec![FileNode {
                name: FILE_NAME.to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        // Provide stale hints: claim the file is missing even though
        // it's actually cached.
        let mut missing = HashSet::new();
        missing.insert(file_digest);

        let download_dir = make_temp_path("download_dir_stale_hints");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            Some(missing),
            None,
        )
        .await?;

        // The file should still be present (re-fetched via FastSlowStore).
        let content = fs::read(format!("{download_dir}/{FILE_NAME}")).await?;
        assert_eq!(from_utf8(&content)?, FILE_CONTENT);

        Ok(())
    }

    /// Verify that an empty server_missing_digests set (all blobs
    /// hinted as cached) still downloads correctly.
    #[nativelink_test]
    async fn download_to_directory_empty_missing_hints() -> Result<(), Box<dyn core::error::Error>>
    {
        const FILE_NAME: &str = "all_cached.txt";
        const FILE_CONTENT: &str = "ALL_CACHED_FILE";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let file_digest = DigestInfo::new([40u8; 32], FILE_CONTENT.len() as u64);

        // Put the file in both stores.
        slow_store
            .as_ref()
            .update_oneshot(file_digest, FILE_CONTENT.into())
            .await?;
        fast_store
            .as_ref()
            .update_oneshot(file_digest, FILE_CONTENT.into())
            .await?;

        let root_directory_digest = DigestInfo::new([41u8; 32], 32);
        let root_directory = Directory {
            files: vec![FileNode {
                name: FILE_NAME.to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        // Empty hints set: everything is "cached" (nothing missing).
        let missing = HashSet::new();

        let download_dir = make_temp_path("download_dir_empty_hints");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            Some(missing),
            None,
        )
        .await?;

        // File should be present via hardlink from fast store.
        let content = fs::read(format!("{download_dir}/{FILE_NAME}")).await?;
        assert_eq!(from_utf8(&content)?, FILE_CONTENT);

        Ok(())
    }

    /// Verify the None path (no server hints) still does the
    /// has_with_results check as before.
    #[nativelink_test]
    async fn download_to_directory_no_hints_uses_has_check()
    -> Result<(), Box<dyn core::error::Error>> {
        const FILE_NAME: &str = "no_hints.txt";
        const FILE_CONTENT: &str = "NO_HINTS_FILE";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let file_digest = DigestInfo::new([50u8; 32], FILE_CONTENT.len() as u64);

        // Only in slow store (fast store miss).
        slow_store
            .as_ref()
            .update_oneshot(file_digest, FILE_CONTENT.into())
            .await?;

        let root_directory_digest = DigestInfo::new([51u8; 32], 32);
        let root_directory = Directory {
            files: vec![FileNode {
                name: FILE_NAME.to_string(),
                digest: Some(file_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        let download_dir = make_temp_path("download_dir_no_hints");
        fs::create_dir_all(&download_dir).await?;
        // Pass None for server_missing_digests: uses the fallback
        // has_with_results path.
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            None,
            None,
        )
        .await?;

        let content = fs::read(format!("{download_dir}/{FILE_NAME}")).await?;
        assert_eq!(from_utf8(&content)?, FILE_CONTENT);

        Ok(())
    }

    /// When server_missing_digests marks blobs as missing, verify
    /// populate_fast_store_unchecked is used (has() is skipped) by
    /// confirming blobs NOT in the fast store are fetched from slow.
    #[nativelink_test]
    async fn download_to_directory_missing_hints_skip_has_check()
    -> Result<(), Box<dyn core::error::Error>> {
        const CACHED_NAME: &str = "cached_blob.txt";
        const CACHED_CONTENT: &str = "I_AM_CACHED";
        const MISSING_NAME: &str = "missing_blob.txt";
        const MISSING_CONTENT: &str = "I_NEED_FETCH";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let cached_digest = DigestInfo::new([60u8; 32], CACHED_CONTENT.len() as u64);
        let missing_digest = DigestInfo::new([61u8; 32], MISSING_CONTENT.len() as u64);

        // cached_blob: present in both stores.
        slow_store
            .as_ref()
            .update_oneshot(cached_digest, CACHED_CONTENT.into())
            .await?;
        fast_store
            .as_ref()
            .update_oneshot(cached_digest, CACHED_CONTENT.into())
            .await?;

        // missing_blob: only in slow store (will be fetched via
        // populate_fast_store_unchecked when hints say it's missing).
        slow_store
            .as_ref()
            .update_oneshot(missing_digest, MISSING_CONTENT.into())
            .await?;

        // Confirm the missing blob is NOT in fast store before the test.
        let key: StoreKey<'_> = missing_digest.into();
        let has = fast_store.as_ref().has(key).await?;
        assert!(
            has.is_none(),
            "missing_blob should not be in fast store yet"
        );

        let root_directory_digest = DigestInfo::new([62u8; 32], 32);
        let root_directory = Directory {
            files: vec![
                FileNode {
                    name: CACHED_NAME.to_string(),
                    digest: Some(cached_digest.into()),
                    ..Default::default()
                },
                FileNode {
                    name: MISSING_NAME.to_string(),
                    digest: Some(missing_digest.into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        let mut missing_set = HashSet::new();
        missing_set.insert(missing_digest);

        let download_dir = make_temp_path("download_dir_skip_has");
        fs::create_dir_all(&download_dir).await?;
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            Some(missing_set),
            None,
        )
        .await?;

        // Both files should be materialized correctly.
        let cached_content = fs::read(format!("{download_dir}/{CACHED_NAME}")).await?;
        assert_eq!(from_utf8(&cached_content)?, CACHED_CONTENT);

        let missing_content = fs::read(format!("{download_dir}/{MISSING_NAME}")).await?;
        assert_eq!(from_utf8(&missing_content)?, MISSING_CONTENT);

        // The missing blob should now be in the fast store (populated
        // via populate_fast_store_unchecked).
        let key: StoreKey<'_> = missing_digest.into();
        let has_after = fast_store.as_ref().has(key).await?;
        assert!(
            has_after.is_some(),
            "missing blob should be in fast store after download"
        );

        Ok(())
    }

    /// Large missing_digests list (100+ entries) — verify no performance
    /// regression and all files are materialized correctly.
    #[nativelink_test]
    async fn download_to_directory_large_missing_digests_list()
    -> Result<(), Box<dyn core::error::Error>> {
        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        const NUM_FILES: usize = 150;

        let mut file_nodes = Vec::with_capacity(NUM_FILES);
        let mut missing_set = HashSet::new();
        let mut file_digests = Vec::with_capacity(NUM_FILES);

        for i in 0..NUM_FILES {
            let content = format!("file-content-{i:04}");
            // Generate unique hash: first two bytes encode the index.
            let mut hash = [0u8; 32];
            hash[0] = (i >> 8) as u8;
            hash[1] = (i & 0xff) as u8;
            hash[2] = 0xAA; // sentinel to distinguish from other tests
            let digest = DigestInfo::new(hash, content.len() as u64);

            // Put in slow store only (missing from fast).
            slow_store
                .as_ref()
                .update_oneshot(digest, content.clone().into())
                .await?;

            file_nodes.push(FileNode {
                name: format!("file_{i:04}.txt"),
                digest: Some(digest.into()),
                ..Default::default()
            });

            // Mark all as missing.
            missing_set.insert(digest);
            file_digests.push((digest, content));
        }

        let root_directory_digest = DigestInfo::new([70u8; 32], 32);
        let root_directory = Directory {
            files: file_nodes,
            ..Default::default()
        };
        slow_store
            .as_ref()
            .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
            .await?;

        let download_dir = make_temp_path("download_dir_large_missing");
        fs::create_dir_all(&download_dir).await?;

        let start = std::time::Instant::now();
        download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_directory_digest,
            &download_dir,
            None,
            Some(missing_set),
            None,
        )
        .await?;
        let elapsed = start.elapsed();

        // Verify all 150 files are present with correct content.
        for (i, (_digest, expected_content)) in file_digests.iter().enumerate() {
            let path = format!("{download_dir}/file_{i:04}.txt");
            let actual = fs::read(&path).await?;
            assert_eq!(
                from_utf8(&actual)?,
                expected_content.as_str(),
                "Content mismatch for file_{i:04}.txt"
            );
        }

        // Performance sanity check: 150 small in-memory blobs should complete
        // in well under 30 seconds, even on slow CI.
        assert!(
            elapsed < Duration::from_secs(30),
            "150-file download took {elapsed:?}, expected < 30s"
        );

        Ok(())
    }

    #[nativelink_test]
    async fn missing_command_attaches_precondition_failure_detail()
    -> Result<(), Box<dyn core::error::Error>> {
        // REAPI v2 §2.2.4: when the worker fails to fetch the action's
        // Command from CAS, the resulting NotFound error MUST carry a
        // PreconditionFailure detail (MISSING violation for the command
        // digest) so Bazel can re-upload and recover. The original code
        // only attached the detail for missing input files, leaving the
        // command-fetch path bare.
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Synthesize a command_digest that is NOT uploaded to CAS so the
        // command-fetch path returns NotFound.
        let missing_command_digest = DigestInfo::new([0xCD; 32], 256);

        // Upload an empty input root so input fetch succeeds (we only want
        // the command-fetch branch to fail).
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(missing_command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepare_err = running_action
            .clone()
            .prepare_action()
            .await
            .err()
            .expect("prepare_action must fail when command is missing from CAS");

        assert_eq!(
            prepare_err.code,
            Code::NotFound,
            "missing-command path should surface as NotFound, got {:?}: {}",
            prepare_err.code,
            prepare_err.message_string(),
        );
        assert!(
            !prepare_err.details.is_empty(),
            "command-fetch NotFound must attach a PreconditionFailure detail (REAPI §2.2.4); details was empty (messages={:?})",
            prepare_err.messages,
        );
        let any = prepare_err
            .details
            .iter()
            .find(|d| d.type_url == "type.googleapis.com/google.rpc.PreconditionFailure")
            .expect("expected a PreconditionFailure detail");

        // Decode and verify the MISSING violation references the missing
        // command digest in canonical `blobs/<hash>/<size>` form.
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
        let decoded =
            PfFailure::decode(any.value.as_slice()).expect("PreconditionFailure must decode");
        assert!(
            !decoded.violations.is_empty(),
            "PreconditionFailure must contain at least one violation",
        );
        let v = &decoded.violations[0];
        assert_eq!(v.r#type, "MISSING");
        assert_eq!(
            v.subject,
            format!(
                "blobs/{}/{}",
                missing_command_digest.packed_hash(),
                missing_command_digest.size_bytes()
            ),
            "MISSING violation subject must reference the missing command digest",
        );

        running_action.cleanup().await?;
        Ok(())
    }

    /// Production-composition regression: when `upload_ac_results`
    /// completes successfully on a `FastSlowStore`-backed AC store, the
    /// `ac_mirror_target.fss.insert_local_ac_pin(...)` MUST run so the
    /// worker's BlobsAvailable loop advertises the AC entry on proto
    /// field 17 (`pinned_ac_mirror_entries`) during the slow-write
    /// window. This is the under-action half of the contract.
    ///
    /// The test wires the SAME AC `FastSlowStore` instance as both
    /// `ac_store` (so `update_oneshot` runs through it on the real path
    /// `cache_action_result → upload_ac_results`) AND as
    /// `ac_mirror_target.fss` (so the post-write pin insert mutates the
    /// observable index). Asserting via `dispatched_mirror_pin_snapshot`
    /// crosses the same in-process seam the production
    /// `send_periodic_blobs_available` loop reads from
    /// (`dispatched_ac_pin_snapshot_for_store`), satisfying production
    /// composition in substance — not just form.
    ///
    /// Mutation step: comment out the `if let Some(target) =
    /// self.ac_mirror_target.as_ref()` block in
    /// `running_actions_manager.rs::upload_ac_results`. The test must
    /// red-fail with the bespoke "AC pin must be registered after
    /// upload_ac_results — production-composition contract violated"
    /// message — not a generic `is_err()` / `assert_eq` mismatch.
    #[nativelink_test]
    async fn upload_ac_results_registers_pin_in_fss() -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, _) = setup_stores().await?;

        // Build a REAL `FastSlowStore` for AC: memory-fast over
        // memory-slow. This is the production shape on which
        // `insert_local_ac_pin` is meaningful — a bare `MemoryStore` AC
        // would not even support pin tracking.
        let ac_fast_spec = MemorySpec::default();
        let ac_slow_spec = MemorySpec::default();
        let ac_fast = MemoryStore::new(&ac_fast_spec);
        let ac_slow = MemoryStore::new(&ac_slow_spec);
        let ac_fss = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Memory(ac_fast_spec),
                slow: StoreSpec::Memory(ac_slow_spec),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                slow_writes_in_flight_max_bytes: 0,
            },
            Store::new(ac_fast),
            Store::new(ac_slow),
        );

        let ac_store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                // The same AC FSS that is the pin-target also serves as
                // `ac_store` so the production update path actually runs.
                ac_store: Some(Store::new(ac_fss.clone())),
                ac_mirror_target: Some(AcMirrorTarget {
                    fss: ac_fss.clone(),
                    store_id: ac_store_id.clone(),
                    ac_publish_pending_acks: std::sync::Arc::new(parking_lot::Mutex::new(
                        std::collections::HashMap::new(),
                    )),
                    metrics: std::sync::Arc::new(
                        nativelink_worker::running_actions_manager::Metrics::default(),
                    ),
                }),
                historical_store: Store::new(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::SuccessOnly,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            })?);

        let action_digest = DigestInfo::new([0xACu8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 0,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };

        // 5s deadlock-detector: if any layer above the AC FSS borrows
        // a writer that the pin insert path doesn't terminate, this
        // timeout fires with a SPECIFIC message instead of hanging
        // CI. Per CLAUDE.md, the timeout-message specificity is the
        // signal that distinguishes a contract violation from a generic
        // test-infra hang.
        tokio::time::timeout(
            Duration::from_secs(5),
            running_actions_manager.cache_action_result(
                action_digest,
                &mut action_result,
                DigestHasherFunc::Sha256,
                &nativelink_util::action_messages::OperationId::default(),
                "test_worker",
            ),
        )
        .await
        .expect(
            "must not deadlock — cache_action_result borrowed-state \
             contract on AC FSS pin insert",
        )?;

        // Production-composition assertion: the AC FSS instance held
        // by `ac_mirror_target` must contain a pin entry for the
        // action_digest under the configured store_id, observable via
        // the SAME accessor the production `send_periodic_blobs_available`
        // loop uses.
        let snapshot = ac_fss.dispatched_ac_pin_snapshot_for_store(ac_store_id.as_ref());
        assert!(
            snapshot.iter().any(|d| *d == action_digest),
            "AC pin must be registered after upload_ac_results — \
             production-composition contract violated. \
             Snapshot under store_id {ac_store_id:?}: {snapshot:?}",
        );
        Ok(())
    }

    /// (#O1 + #A4 2026-06-07) Helper: build and upload N Tree protos
    /// to the cas_store. Each tree has `files_per_tree` file_nodes with
    /// deterministic digests. Returns `(folders, expected_digests)`.
    async fn upload_n_trees(
        cas_store: &Arc<FastSlowStore>,
        n: usize,
        files_per_tree: usize,
    ) -> Result<(Vec<DirectoryInfo>, HashSet<DigestInfo>), Error> {
        let mut folders = Vec::with_capacity(n);
        let mut expected = HashSet::new();
        for tree_idx in 0..n {
            let files: Vec<FileNode> = (0..files_per_tree)
                .map(|file_idx| {
                    // Deterministic, distinct, non-zero-size digests per
                    // (tree_idx, file_idx). 32 bytes = sha256-shaped.
                    let mut hash = [0u8; 32];
                    hash[0] = tree_idx as u8;
                    hash[1] = file_idx as u8;
                    // Size > 0 so the filter keeps it.
                    let digest = DigestInfo::new(hash, ((tree_idx * 100 + file_idx) as u64) + 1);
                    expected.insert(digest);
                    FileNode {
                        name: format!("f{file_idx}"),
                        digest: Some(digest.into()),
                        ..Default::default()
                    }
                })
                .collect();
            let tree = Tree {
                root: Some(Directory {
                    files,
                    ..Default::default()
                }),
                children: vec![],
            };
            let tree_digest = serialize_and_upload_message(
                &tree,
                cas_store.as_pin(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            folders.push(DirectoryInfo {
                path: format!("dir{tree_idx}"),
                tree_digest,
            });
        }
        Ok((folders, expected))
    }

    /// T1 (Part A — parallel decode correctness, #A4 2026-06-07).
    /// `expand_tree_file_digests` must return every file_node digest from
    /// every output_folders' Tree, irrespective of decode order. The
    /// pre-fix sequential loop and the post-fix `FuturesUnordered` parallel
    /// decode share this correctness contract; the test guards both. The
    /// SEPARATE parallel-timing test (`t1_parallel_concurrency`) below
    /// proves the post-fix is also concurrent.
    ///
    /// Mutation: revert to the sequential `for folder in &output_folders`
    /// loop — this correctness test still passes (sequential is still
    /// correct). That is the point: this test guards correctness across
    /// the surgery; `t1_parallel_concurrency` separately guards parallelism.
    #[nativelink_test]
    async fn expand_tree_file_digests_returns_all_file_node_digests()
    -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store)),
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
                deferred_output_uploads_enabled: false,
            })?);

        const N_TREES: usize = 5;
        const FILES_PER_TREE: usize = 4;
        let (folders, expected) =
            upload_n_trees(&cas_store, N_TREES, FILES_PER_TREE).await?;
        let action_result = ActionResult {
            output_folders: folders,
            ..ActionResult::default()
        };

        // 5s deadlock-detector. Bounded to detect a FuturesUnordered
        // misuse hang rather than passing on `tokio::time::Elapsed`.
        let got = tokio::time::timeout(
            Duration::from_secs(5),
            running_actions_manager.expand_tree_file_digests(&action_result, None),
        )
        .await
        .expect(
            "expand_tree_file_digests must not deadlock — \
             FuturesUnordered drive-to-completion contract violated",
        );

        assert_eq!(
            got.len(),
            N_TREES * FILES_PER_TREE,
            "expand_tree_file_digests must return every file_node digest \
             across all trees; got {} of {N_TREES}×{FILES_PER_TREE}={}",
            got.len(),
            N_TREES * FILES_PER_TREE,
        );
        let got_set: HashSet<DigestInfo> = got.into_iter().collect();
        assert_eq!(
            got_set, expected,
            "expand_tree_file_digests returned wrong digest set",
        );
        Ok(())
    }

    /// T1-parallel (Part A — concurrent decode, #A4 2026-06-07).
    /// The post-fix `FuturesUnordered` issues all per-folder
    /// `get_and_decode_digest` futures concurrently. The pre-fix
    /// sequential loop awaits each before issuing the next.
    ///
    /// Proof technique: time the function with N folders, then time
    /// `expand_tree_file_digests` calls in sequence with single-folder
    /// ActionResults summing the per-folder wall-clock. The parallel
    /// implementation should complete in less than the summed
    /// single-folder time. We use a generous margin because the per-decode
    /// cost over MemoryStore-backed FilesystemStore is small; the test
    /// asserts that the PARALLEL run is bounded by ~1× the wall-clock of
    /// the slowest single decode plus overhead, NOT the sum.
    ///
    /// Mutation: revert to sequential `for folder in &output_folders` —
    /// the parallel wall-clock will rise to approximately the sum of
    /// per-folder times. The assertion red-fails with the specific
    /// "parallel run wall-clock exceeded sequential-bound" message.
    #[nativelink_test]
    async fn expand_tree_file_digests_runs_decodes_concurrently()
    -> Result<(), Box<dyn core::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: cas_store.clone(),
                ac_store: Some(Store::new(ac_store)),
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
                deferred_output_uploads_enabled: false,
            })?);

        // Use larger trees so each decode has a measurable cost. 64
        // file_nodes per tree, 10 trees — N_TREES decodes done in
        // parallel should be ~1× the cost of a single decode.
        const N_TREES: usize = 10;
        const FILES_PER_TREE: usize = 64;
        let (folders, _expected) =
            upload_n_trees(&cas_store, N_TREES, FILES_PER_TREE).await?;
        let action_result_all = ActionResult {
            output_folders: folders.clone(),
            ..ActionResult::default()
        };

        // Warm caches: one untimed run so OS page-cache + EvictingMap
        // state stabilises.
        let _ = running_actions_manager
            .expand_tree_file_digests(&action_result_all, None)
            .await;

        // Time the parallel (production) call: all N trees in one
        // invocation. Median of 3 to reduce noise.
        let mut parallel_runs: Vec<Duration> = Vec::with_capacity(3);
        for _ in 0..3 {
            let t0 = Instant::now();
            let _ = running_actions_manager
                .expand_tree_file_digests(&action_result_all, None)
                .await;
            parallel_runs.push(t0.elapsed());
        }
        parallel_runs.sort();
        let parallel_median = parallel_runs[1];

        // Time per-folder calls, summed: each call has a single-folder
        // ActionResult, so the function still walks the same code path
        // but with concurrency=1 per call. The SUM is what the pre-fix
        // sequential loop would wall-clock to.
        let mut sequential_total = Duration::ZERO;
        for folder in &folders {
            let single = ActionResult {
                output_folders: vec![folder.clone()],
                ..ActionResult::default()
            };
            // Median of 3.
            let mut runs: Vec<Duration> = Vec::with_capacity(3);
            for _ in 0..3 {
                let t0 = Instant::now();
                let _ = running_actions_manager.expand_tree_file_digests(&single, None).await;
                runs.push(t0.elapsed());
            }
            runs.sort();
            sequential_total += runs[1];
        }

        // Bound: parallel must be strictly less than the sequential
        // sum. A sequential `for folder in &output_folders { ... await ... }`
        // loop would wall-clock to ~= sequential_total because each
        // future is fully driven before the next is polled. A parallel
        // `FuturesUnordered` driver overlaps the awaits and so completes
        // in less than the sum.
        //
        // Over a MemoryStore-backed FilesystemStore the per-decode
        // wall-clock is sub-millisecond and dominated by task-scheduling
        // overhead; the speedup margin is small (~20-30% in CI). We
        // therefore assert `parallel < sequential_total` rather than
        // `< sequential_total / 2`. The intent is to detect the
        // sequential-vs-parallel REGRESSION (where parallel >=
        // sequential_total), not to assert a specific speedup ratio.
        //
        // Mutation: revert the loop to sequential `for folder in
        // &action_result.output_folders { ... .await }` — parallel run
        // becomes ~= sequential_total (within scheduling noise), and
        // this assertion red-fails.
        assert!(
            parallel_median < sequential_total,
            "parallel run wall-clock exceeded sequential-bound — \
             FuturesUnordered concurrency contract violated. \
             parallel_median={parallel_median:?} sequential_total={sequential_total:?}",
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // O5: output-dir overlap concurrency test (FU-8)
    //
    // The O5 change wires `try_join([B2], [C])` so that output-directory
    // prep ([C]) runs concurrently with input download ([B2]).  The SAFETY
    // contract — [C]'s create_dir_all tolerating AlreadyExists when [B2]'s
    // BFS mkdir pre-created the same path — is exercised by the existing
    // action-execution tests (e.g. `simple_worker_executes_action`).
    //
    // The CONCURRENCY contract — that [C] makes forward progress while [B2]
    // is blocked — is tested by `o5_overlap_c_runs_while_b2_blocked` below.
    //
    // FU-8 seam: FU-8 added a `#[cfg(feature = "test-utils")]` Notify gate to
    // `batch_read_small_blobs` (`BATCH_READ_TEST_GATE` static).  When armed,
    // the gate blocks [B2] at the entry of `batch_read_small_blobs` before
    // any slow-store interaction, so the test can assert [C] completes while
    // [B2] is parked.  Production builds see zero overhead (the static and
    // the gate check are absent from the default build).
    //
    // Note: `batch_read_small_blobs` only fires when the input tree contains
    // files that are not yet in the fast store — i.e. the fetcher identifies
    // at least one "small" blob to batch-read.  The test therefore supplies a
    // non-trivial input root with a file blob that exists in the CAS store
    // but NOT in the fast (FilesystemStore) tier, forcing the fetcher to
    // call `batch_read_small_blobs`.
    //
    // Mutation guard: reverting the `try_join(inputs_fut, output_dirs_fut)`
    // at `running_actions_manager.rs` ~line 3258 to sequential
    // (`inputs_fut.await?; output_dirs_fut.await?;`) must red-fail this
    // test with the bespoke "O5 concurrency invariant violated" message.
    // -----------------------------------------------------------------------

    /// FU-8: proves [C] (output-dir prep) runs to completion while [B2]
    /// (input download) is frozen at `batch_read_small_blobs` — the O5
    /// overlap concurrency contract.
    ///
    /// # Setup
    /// - Input root: one small file blob whose content is NOT in the CAS
    ///   store (so the existence check finds it "missing") — this forces
    ///   `batch_read_small_blobs` to be called by the fetcher.
    /// - Declared output: `output_dir/output.txt` — the parent `output_dir`
    ///   must be created by [C].
    /// - After gate release, prepare_action may fail (blob not fetchable
    ///   from MemoryStore via GrpcStore downcast); that is expected.
    ///
    /// # Protocol
    /// 1. Install the `BATCH_READ_TEST_GATE` via `install_batch_read_test_gate()`.
    /// 2. Spawn `prepare_action()` as a task.
    /// 3. Await `gate.entered` — confirms [B2] is FROZEN inside
    ///    `batch_read_small_blobs` (parked on `gate.release.notified()`).
    /// 4. Wait (bounded, via `yield_now` loop) for `work_directory/output_dir`
    ///    to appear.  With [B2] frozen, only [C] can advance, so the loop is
    ///    purely waiting on [C]'s `spawn_blocking`-backed `create_dir_all`.
    /// 5. Release the gate; await task completion.
    ///
    /// # Why this is a happens-before guarantee (not a timing race)
    /// [C] is NOT synchronous: `prepare_output_directory` →
    /// `fs::create_dir_all` → `call_with_permit` → `spawn_blocking` (always
    /// yields).  But once [B2] is frozen at the gate, the only future in the
    /// try_join that can make progress is [C].  In the OVERLAP case [C] runs
    /// to completion while [B2] is frozen (loop terminates).  In the
    /// SEQUENTIAL mutation [C] never STARTS while [B2] is blocked (loop times
    /// out).  The cases are separated structurally — "[C] runs while [B2]
    /// frozen" vs "[C] cannot run while [B2] frozen" — not by a timing margin.
    ///
    /// # Mutation falsifier
    /// Revert `try_join(inputs_fut, output_dirs_fut)` to sequential
    /// (`inputs_fut.await?; output_dirs_fut.await?;`) at ~line 3258 in
    /// `running_actions_manager.rs`.  [C] is sequenced AFTER [B2], so it never
    /// starts while [B2] is frozen at the gate.  `output_dir` never appears,
    /// the `tokio::time::timeout` on the wait-for-[C] loop fires with
    /// "O5 concurrency invariant violated: [C] did not create output_dir
    /// while [B2] was frozen...".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn o5_overlap_c_runs_while_b2_blocked()
    -> Result<(), Box<dyn core::error::Error>> {
        use nativelink_worker::running_actions_manager::install_batch_read_test_gate;
        use tokio::time::timeout;

        const WORKER_ID: &str = "o5_overlap_worker";
        // Generous enough not to flap under CI load, tight enough to catch
        // a deadlock or sequentialisation regression quickly.
        const ASSERT_DEADLINE: Duration = Duration::from_secs(10);

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory_o5_overlap");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Build a minimal Action with:
        //   - one small input file blob (forces batch_read_small_blobs to run)
        //   - one declared output file under a subdirectory (forces [C] to mkdir)
        //
        // The file content blob is NOT uploaded to the store.  The existence
        // check in download_to_directory will find it "missing" and call
        // batch_read_small_blobs.  The gate fires at the ENTRY of
        // batch_read_small_blobs before any slow-store lookup, so [B2] blocks
        // regardless of what the store contains.
        //
        // After the gate releases, batch_read_small_blobs returns
        // Ok(HashSet::new()) (slow store is MemoryStore, not GrpcStore — the
        // downcast misses), and the fallback populate_fast_store also fails
        // (blob not present).  The action fails with an error after the gate
        // releases — that is expected and acceptable for this test; we only
        // need to assert [C] ran while [B2] was blocked.
        let file_content = Bytes::from_static(b"hello from o5 overlap test");
        let file_digest: DigestInfo = {
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&file_content);
            hasher.finalize_digest()
        };
        // Intentionally NOT uploading file_content — we want the blob missing.

        let file_node = FileNode {
            name: "input.txt".to_string(),
            digest: Some(file_digest.into()),
            is_executable: false,
            node_properties: None,
        };
        let root_dir = Directory {
            files: vec![file_node],
            ..Default::default()
        };
        let input_root_digest = serialize_and_upload_message(
            &root_dir,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        // Declared output under a subdirectory — [C] must create `output_dir/`.
        let command = Command {
            arguments: vec!["/bin/true".to_string()],
            output_files: vec!["output_dir/output.txt".to_string()],
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

        let running_action = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        // Arm the gate BEFORE spawning so there's no race between spawn
        // and install.
        let gate = install_batch_read_test_gate();

        // Spawn prepare_action as a background task.  It will run [A] →
        // [B1] → try_join([B2], [C]).  [B2] will block at the gate.
        let work_dir = running_action.get_work_directory().clone();
        let prepare_task = tokio::spawn(async move {
            running_action.prepare_action().await
        });

        // Wait until [B2] has entered batch_read_small_blobs and is parked
        // at the gate.  This confirms [B2] is frozen.
        //
        // No lost-wakeup hazard: if the spawned task reaches the gate and
        // calls `entered.notify_one()` BEFORE this `notified()` future is
        // first polled, tokio::sync::Notify stores the permit, and the first
        // poll of `notified()` consumes it and completes immediately. The
        // `.notified()`-without-`.enable()` shape is safe here because we are
        // not racing a drop of the notified future before the notify.
        timeout(ASSERT_DEADLINE, gate.entered.notified())
            .await
            .expect("timed out waiting for batch_read_small_blobs to be entered — \
                     check that the input file triggers the fetcher path");

        // [B2] is now frozen at `gate.release.notified().await` and cannot
        // make any further progress until the test releases it.  This is the
        // load-bearing fact: with [B2] frozen, the ONLY future in the
        // try_join that can advance is [C].
        //
        // [C] is NOT synchronous — `prepare_output_directory` →
        // `fs::create_dir_all` → `call_with_permit` → `spawn_blocking`, which
        // always yields and completes on a blocking thread.  So [C]'s
        // directory does not necessarily exist the instant `gate.entered`
        // fires; [C]'s spawn_blocking may still be in flight.  We therefore
        // wait for [C]'s side effect (the directory) with a bounded loop that
        // yields to the runtime so [C]'s spawn_blocking can complete.
        //
        // Why this is a happens-before guarantee and not a flaky race:
        //   - OVERLAP (try_join): [C] is unblocked and runs to completion
        //     while [B2] is frozen.  The loop terminates as soon as [C]'s
        //     spawn_blocking lands the directory — bounded by spawn_blocking
        //     latency, not by [B2].
        //   - SEQUENTIAL (mutation `[B2].await?; [C].await?;`): [C] never
        //     STARTS, because [B2] is frozen first and [C] is sequenced
        //     after it.  The directory NEVER appears.  The loop exhausts its
        //     deadline and the test fails with the bespoke message.
        // The two cases are separated by "[C] runs while [B2] is frozen"
        // (overlap) vs "[C] cannot run at all while [B2] is frozen"
        // (sequential) — a structural difference, not a timing margin.
        let output_dir_path = format!("{work_dir}/output_dir");
        let wait_for_c = async {
            loop {
                if fs::metadata(&output_dir_path).await.is_ok() {
                    break;
                }
                // Yield (not sleep) so [C]'s spawn_blocking can make progress.
                // [B2] is frozen, so this loop is purely waiting on [C].
                tokio::task::yield_now().await;
            }
        };
        timeout(ASSERT_DEADLINE, wait_for_c).await.expect(
            "O5 concurrency invariant violated: [C] did not create output_dir \
             while [B2] was frozen at batch_read_small_blobs — \
             try_join([B2],[C]) may have been reverted to sequential, in which \
             case [C] never starts while [B2] is blocked",
        );

        // Release [B2] and await the task.
        // Note: prepare_action is expected to fail after gate release — the
        // file content blob is not in any store, so batch_read_small_blobs
        // returns Ok(HashSet::new()) and the fallback populate also fails.
        // The failure is expected; we only assert it didn't hang.
        gate.release.notify_one();
        let join_result = timeout(ASSERT_DEADLINE, prepare_task)
            .await
            .expect("prepare_action task timed out after gate release — task hung after gate release");
        // The task must not have panicked (join_result is Ok(task_result)).
        assert!(
            join_result.is_ok(),
            "prepare_action task panicked after gate release: {:?}",
            join_result.err()
        );
        // The action result may be Ok or Err (blob not in store → fetch fails);
        // both are acceptable — we only care that the task completed, not that
        // the action succeeded.

        Ok(())
    }

    // -----------------------------------------------------------------------
    // F2: deferred_output_uploads_enabled kill-switch tests
    //
    // These two tests exercise both states of the kill-switch introduced by
    // commit worktree-agent-af6887b4d19344d5e (F2 task).
    //
    // `BlockingFakeSlowStore` — a `StoreDriver` whose `update()` and
    // `update_with_whole_file()` park indefinitely until `release()` is
    // called.  This makes it possible to assert that `upload_results`
    // either completes without waiting (deferred=true) or blocks until the
    // slow tier is released (deferred=false).
    // -----------------------------------------------------------------------

    struct BlockingFakeSlowStore {
        inner: Arc<MemoryStore>,
        /// Notified (permit-1) each time `update()` is entered and starts
        /// blocking. Used in tests to confirm the slow store is actually
        /// being awaited before asserting `task.is_finished() == false`.
        entered: Notify,
        gate: Notify,
        block_updates: AtomicBool,
        update_attempts: AtomicUsize,
    }

    impl BlockingFakeSlowStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryStore::new(&Default::default()),
                entered: Notify::new(),
                gate: Notify::new(),
                block_updates: AtomicBool::new(true),
                update_attempts: AtomicUsize::new(0),
            })
        }

        fn release(&self) {
            self.block_updates.store(false, Ordering::SeqCst);
            // notify_waiters wakes ALL currently parked waiters in one
            // call (versus notify_one which would require N calls for N
            // waiters and risks deadlock if call ordering varies).
            self.gate.notify_waiters();
        }

        fn update_attempts_count(&self) -> usize {
            self.update_attempts.load(Ordering::SeqCst)
        }
    }

    impl MetricsComponent for BlockingFakeSlowStore {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl StoreDriver for BlockingFakeSlowStore {
        async fn has_with_results(
            self: Pin<&Self>,
            keys: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(keys, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<(), Error> {
            self.update_attempts.fetch_add(1, Ordering::SeqCst);
            if self.block_updates.load(Ordering::SeqCst) {
                // Signal the test that we have been entered and are about
                // to block. This allows the test to confirm the slow store
                // is actually being awaited without relying on time-based
                // polling (which suffers from timer-starvation in tokio's
                // current-thread runtime under tight yield loops).
                self.entered.notify_one();
                // Park indefinitely — this is the hook that proves whether
                // upload_results waits on the slow store or not.
                self.gate.notified().await;
            }
            Pin::new(self.inner.as_ref())
                .update(key, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn inner_store(&self, _key: Option<StoreKey<'_>>) -> &dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
            self
        }

        fn register_item_callback(
            self: Arc<Self>,
            _callback: Arc<dyn ItemCallback>,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn stable_delegation(&self) -> StableDigestDelegation<'_> {
            StableDigestDelegation::Leaf
        }

        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Leaf
        }

        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Leaf
        }
        fn durable_delegation(&self) -> DurableDelegation<'_> {
            DurableDelegation::Leaf
        }

        fn optimized_for(&self, _optimization: StoreOptimizations) -> bool {
            false
        }
    }

    default_health_status_indicator!(BlockingFakeSlowStore);

    /// Build a real `FastSlowStore` (FilesystemStore fast + BlockingFakeSlowStore
    /// slow) so we can control when the slow tier becomes available.
    async fn setup_stores_with_blocking_slow() -> Result<
        (
            Arc<FilesystemStore>,
            Arc<BlockingFakeSlowStore>,
            Arc<FastSlowStore>,
            Arc<MemoryStore>,
        ),
        Error,
    > {
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path_blocking"),
            temp_path: make_temp_path("temp_path_blocking"),
            eviction_policy: None,
            ..Default::default()
        };
        let fast_store = FilesystemStore::new(&fast_config).await?;
        let slow_store = BlockingFakeSlowStore::new();
        let ac_store = MemoryStore::new(&Default::default());
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(Default::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                // BlockingFakeSlowStore.requires_in_flight_buffer_cap() == false
                // (default), so 0 is valid here.
                slow_writes_in_flight_max_bytes: 0,
            },
            Store::new(fast_store.clone()),
            Store::new(slow_store.clone()),
        );
        Ok((fast_store, slow_store, cas_store, ac_store))
    }

    /// F2 kill-switch ENABLED: `upload_results` must complete without
    /// waiting for the remote slow store.
    ///
    /// The slow store blocks all `update()` calls until `release()`.
    /// Under `deferred_output_uploads_enabled = true`, `upload_results`
    /// writes to the fast store only and returns immediately — the slow
    /// store gate is never reached.
    ///
    /// Mutation-verify: set `deferred_output_uploads_enabled: true` →
    /// `false` in the manager constructor. `upload_results` now goes
    /// through `FastSlowStore::update_with_whole_file` which calls
    /// `join!(slow_fut, fast_fut)`, parking on the still-blocked
    /// `BlockingFakeSlowStore`. The outer `tokio::time::timeout(FAST_DEADLINE)`
    /// fires with "F2 kill-switch ENABLED: upload_results must not block on
    /// the remote slow store — deferred contract violated".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn deferred_upload_enabled_completes_before_slow_store()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "deferred_enabled_worker";
        // Generous enough to not flap in CI, tight enough to catch a
        // regression where upload_results waits on the blocked slow store.
        const FAST_DEADLINE: Duration = Duration::from_secs(10);

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (fast_store, slow_store, cas_store, ac_store) =
            setup_stores_with_blocking_slow().await?;
        let root_action_directory = make_temp_path("root_action_directory_deferred_enabled");
        fs::create_dir_all(&root_action_directory).await?;

        // Slow store starts blocked — release() is never called in this
        // test; if upload_results waits for it, the outer timeout fires.
        assert!(
            slow_store.block_updates.load(Ordering::SeqCst),
            "fixture invariant: slow store must start blocked"
        );

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                    cas_endpoint: String::new(),
                    // F2 kill-switch ON: deferred path — write fast store
                    // only, do not wait for slow store.
                    deferred_output_uploads_enabled: true,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |_duration| Box::pin(future::pending()),
                },
            )?);

        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'deferred-content' > ./out.txt".to_string(),
            ],
            output_paths: vec!["out.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
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

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        // Drive the action through prepare + execute + upload_results.
        // The outer timeout IS the test assertion — if upload_results
        // blocks on the slow store (regression), it fires first with the
        // bespoke "deferred contract violated" message.
        let action_result = tokio::time::timeout(FAST_DEADLINE, async {
            running_action_impl
                .clone()
                .prepare_action()
                .await?
                .execute()
                .await?
                .upload_results()
                .await?
                .get_finished_result()
                .await
        })
        .await
        .expect(
            "F2 kill-switch ENABLED: upload_results must not block on the \
             remote slow store — deferred contract violated",
        )?;

        // Verify the output blob landed in the fast store (FilesystemStore).
        // This proves that deferred mode actually wrote something locally,
        // not that it silently skipped the upload entirely.
        assert_eq!(
            action_result.output_files.len(),
            1,
            "expected exactly one output file"
        );
        let output_digest = action_result.output_files[0].digest;
        let key: StoreKey<'_> = output_digest.into();
        let has_in_fast = tokio::time::timeout(
            Duration::from_secs(5),
            fast_store.as_ref().has(key),
        )
        .await
        .expect("fast-store has() must not hang — FilesystemStore index contract violated")?;
        assert!(
            has_in_fast.is_some(),
            "F2 deferred path: output blob must be present in the fast store \
             (FilesystemStore) immediately after upload_results — \
             fast-write skipped or indexed incorrectly"
        );

        running_action_impl.cleanup().await?;
        Ok(())
    }

    /// F2 kill-switch DISABLED: `upload_results` must block on the slow
    /// store (via `FastSlowStore::update_with_whole_file`'s `join!`) and
    /// not complete until the slow tier is released.
    ///
    /// This guards against accidental default change: if someone sets
    /// `deferred_output_uploads_enabled` to `true` by default, the slow
    /// store is bypassed and `upload_results` completes before the slow
    /// store is entered — the `update_attempts_count() > 0` timeout fires
    /// with the bespoke "slow store was never entered" message, proving the
    /// synchronous path was skipped.
    ///
    /// The test spawns `upload_results()` in a background task so the
    /// main test can detect blocking without consuming the `executed` Arc:
    /// 1. Capture baseline `before_count = update_attempts_count()`.
    /// 2. Spawn task → task parks on `BlockingFakeSlowStore.gate.notified()`.
    /// 3. Wait until `update_attempts_count() > before_count` (using
    ///    `tokio::time::sleep(1ms)` between checks to avoid timer starvation).
    ///    Then assert `task.is_finished() == false`.
    /// 4. Release slow store → task unblocks and completes.
    ///
    /// Mutation-verify: set `deferred_output_uploads_enabled: false` →
    /// `true` in the manager constructor. In deferred mode, `upload_results`
    /// writes the fast store only and returns immediately — the slow store's
    /// `update()` is never called by the task, so `update_attempts_count()`
    /// stays at `before_count`. The `tokio::time::timeout(SHORT_DEADLINE, ...)`
    /// fires with bespoke message "F2 kill-switch DISABLED: slow store was
    /// never entered within SHORT_DEADLINE — synchronous upload path not
    /// reached; default changed to deferred?".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn deferred_upload_disabled_blocks_on_slow_store()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "deferred_disabled_worker";
        // Short deadline: if upload_results completes before this, the
        // synchronous invariant is violated.
        const SHORT_DEADLINE: Duration = Duration::from_millis(500);
        // Long deadline for the release path: after releasing the slow
        // store, upload_results must complete within this.
        const LONG_DEADLINE: Duration = Duration::from_secs(15);

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (fast_store, slow_store, cas_store, ac_store) =
            setup_stores_with_blocking_slow().await?;
        let root_action_directory = make_temp_path("root_action_directory_deferred_disabled");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                    cas_endpoint: String::new(),
                    // F2 kill-switch OFF: synchronous path — upload_results
                    // blocks until the slow store accepts the write.
                    deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |_duration| Box::pin(future::pending()),
                },
            )?);

        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'sync-content' > ./out.txt".to_string(),
            ],
            output_paths: vec!["out.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };
        // Write setup protos to the fast store directly (FilesystemStore) to
        // avoid spawning background FSS slow-write tasks that would increment
        // update_attempts_count() before before_count is captured, adding
        // noise to the baseline. Workers read command/action protos from the
        // fast store; no slow-store write is needed for setup. (distsys F3)
        let command_digest = serialize_and_upload_message(
            &command,
            fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            fast_store.as_pin(),
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
            fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        // Capture how many times the slow store has already been called
        // by the FSS background writes from the test setup (uploading
        // command/action protos). The task assertion waits for the count
        // to EXCEED this baseline, proving `upload_results` itself entered
        // the slow store.
        let before_count = slow_store.update_attempts_count();

        // Spawn upload_results into a background task so we can race it
        // against the blocking slow store without consuming `executed`.
        // The blocking store's `gate.notified()` parks this task until
        // `release()` is called.
        let task = tokio::spawn(async move { executed.upload_results().await });

        // --- Phase 1: assert blocking ---
        // Wait until the slow store has been entered by `upload_results`
        // (count > before_count). Use `tokio::time::sleep` (a real timer
        // sleep) between checks to avoid timer starvation in tokio's
        // current-thread runtime — unlike `yield_now()`, `sleep(1ms)`
        // actually advances the timer wheel.
        //
        // In the mutation case (deferred=true), `upload_results` writes the
        // fast store only — `update()` is never called by the task — so
        // the count stays at `before_count`. The `tokio::time::timeout`
        // fires and the `.expect(...)` panics with the bespoke message.
        tokio::time::timeout(SHORT_DEADLINE, async {
            loop {
                if slow_store.update_attempts_count() > before_count {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect(
            "F2 kill-switch DISABLED: slow store was never entered within \
             SHORT_DEADLINE — synchronous upload path not reached; \
             default changed to deferred?",
        );
        assert!(
            !task.is_finished(),
            "F2 kill-switch DISABLED: upload_results must NOT complete while \
             slow store is still blocked — synchronous contract violated; \
             default changed to deferred?"
        );

        // --- Phase 2: release and verify completion ---
        // Unblock the slow store; upload_results must now complete.
        slow_store.release();
        tokio::time::timeout(LONG_DEADLINE, task)
            .await
            .expect(
                "F2 kill-switch DISABLED: upload_results must complete after \
                 slow store is released — wedged or deadlocked after release",
            )
            .expect("task join error")?;

        running_action_impl.cleanup().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Gap 2: a kill that arrives DURING the upload tail must preempt the
    // in-flight upload so the action reaches cleanup.
    //
    // Before the fix `inner_upload_results` / `upload_results` had no kill
    // arm in its `select!` (`running_actions_manager.rs:~4592`): the
    // `kill_channel_rx` is consumed by `inner_execute`, so once the child
    // has exited the only kill signal is the `cancelled` AtomicBool — which
    // nothing in the upload path awaits. A 600s upload therefore ignored a
    // kill that arrived after the child exited, wedging the action until the
    // `max_upload_timeout` (600s) fired.
    //
    // This test wires the production composition `FastSlowStore { fast:
    // FilesystemStore, slow: BlockingFakeSlowStore }` with the slow tier
    // parked indefinitely (deferred OFF → synchronous upload blocks on the
    // slow tier's `update`). It drives a real action through
    // prepare → execute, spawns the REAL `upload_results` (which parks
    // in-flight on the blocked slow store), confirms it is genuinely
    // in-flight, then fires `kill_operation` — WITHOUT ever releasing the
    // slow store. The kill must preempt the in-flight upload and the task
    // must return `Code::Aborted` within a tight bound.
    //
    // Mutation-verify: comment out the `kill_fut` arm in `upload_results`
    // (`running_actions_manager.rs`). The kill notify is then ignored, the
    // upload stays parked on the never-released slow store, and the
    // `tokio::time::timeout(ABORT_DEADLINE, task)` fires with the bespoke
    // "kill during upload tail must preempt the in-flight upload" message.
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn kill_during_upload_tail_aborts_in_flight_upload()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "kill_upload_tail_worker";
        // Time to confirm the upload is genuinely parked on the slow store.
        const ENTERED_DEADLINE: Duration = Duration::from_secs(5);
        // After the kill, the in-flight upload must abort within this bound.
        // It is FAR below the 600s max_upload_timeout: a regression (no kill
        // arm) keeps the task parked on the never-released slow store and
        // only the 600s upload-timeout would eventually fire, so this short
        // deadline cleanly separates "kill preempted" from "kill ignored".
        const ABORT_DEADLINE: Duration = Duration::from_secs(5);

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_fast_store, slow_store, cas_store, ac_store) =
            setup_stores_with_blocking_slow().await?;
        let root_action_directory = make_temp_path("root_action_directory_kill_upload_tail");
        fs::create_dir_all(&root_action_directory).await?;

        assert!(
            slow_store.block_updates.load(Ordering::SeqCst),
            "fixture invariant: slow store must start blocked"
        );

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                    // Full production default. The point of the test is that
                    // the kill aborts FAR sooner than this would.
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                    cas_endpoint: String::new(),
                    // Synchronous upload path: upload_results blocks on the
                    // slow store via FastSlowStore::update_with_whole_file.
                    deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |_duration| Box::pin(future::pending()),
                },
            )?);

        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'kill-tail-content' > ./out.txt".to_string(),
            ],
            output_paths: vec!["out.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };
        // Write setup protos to the fast store directly so the FSS background
        // slow-write tasks don't park on the blocked slow tier during setup.
        let command_digest = serialize_and_upload_message(
            &command,
            _fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            _fast_store.as_pin(),
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
            _fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        // Operation id used to fire the per-action kill below.
        let operation_id = running_action_impl.get_operation_id().clone();

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        let before_count = slow_store.update_attempts_count();

        // Spawn the REAL upload_results into a background task. It parks
        // in-flight on the blocked slow store (deferred OFF).
        let task = tokio::spawn(async move { executed.upload_results().await });

        // Confirm the upload is genuinely in-flight: await the fixture's
        // `entered` Notify, which `BlockingFakeSlowStore::update` fires the
        // moment it enters and starts blocking. This is a deterministic
        // synchronization edge (vs a timer poll); `notify_one` stores a
        // permit if the slow store entered before we subscribe, so the await
        // resolves regardless of ordering.
        tokio::time::timeout(ENTERED_DEADLINE, slow_store.entered.notified())
            .await
            .expect(
                "fixture invariant: upload_results must enter the slow store \
                 before the kill — synchronous upload path not reached",
            );
        // `update` was entered at least once past the setup baseline.
        assert!(
            slow_store.update_attempts_count() > before_count,
            "fixture invariant: slow-store update must have been entered"
        );
        assert!(
            !task.is_finished(),
            "fixture invariant: upload_results must be parked in-flight on \
             the blocked slow store before the kill is fired"
        );

        // Fire the kill DURING the in-flight upload. The slow store is NEVER
        // released, so the only way the task can complete is the kill arm
        // preempting the upload.
        running_actions_manager
            .kill_operation(&operation_id)
            .await?;

        let upload_outcome = tokio::time::timeout(ABORT_DEADLINE, task)
            .await
            .expect(
                "Gap 2: kill during the upload tail must preempt the in-flight \
                 upload — task still parked on the never-released slow store \
                 after the kill, so the kill arm is missing from upload_results",
            )
            .expect("upload_results task join error");

        // M1 (cadre fix-up #2): the kill arm must produce the SAME terminal
        // shape as #1899's kill-during-execute — `Ok(self)` carrying a
        // terminal `ActionResult{error: Aborted}` — NOT `Err(Aborted)`.
        // `upload_results` returns `Arc<RunningActionImpl>`; the real
        // `get_finished_result()` (the exact seam the publish closure uses)
        // then yields the embedded `ActionResult`, which routes the Ok-arm
        // `ExecuteResponse(Completed{Aborted})` to the scheduler. The Err-arm
        // `InternalError(Aborted)` is RE-QUEUED by
        // `simple_scheduler_state_manager.rs:837-859` (Aborted is neither
        // ResourceExhausted nor FailedPrecondition, so attempts++ then
        // `ActionStage::Queued` while attempts <= max_job_retries), causing a
        // spurious re-execution of a killed action. The Ok-arm
        // `Completed{Aborted}` is terminal (`ActionStage::is_finished()`), so
        // it is not retried. The publish-closure wire shape is asserted by
        // the seam test
        // `killed_upload_tail_publishes_execute_response_not_internal_error`
        // in `kill_upload_tail_publish_seam_test.rs`.
        let killed_action = upload_outcome.expect(
            "Gap 2 / M1: a killed upload-tail must return Ok(self) carrying a \
             terminal ActionResult{error: Aborted} (the #1899 Ok-arm shape), \
             NOT Err — the Err-arm InternalError(Aborted) is re-queued by the \
             scheduler as a spurious re-execution",
        );
        // Drive the real `get_finished_result()` seam — the same call the
        // publish pipeline (`.and_then(RunningAction::get_finished_result)`)
        // makes — to extract the synthesized terminal ActionResult.
        let action_result = killed_action.get_finished_result().await.expect(
            "M1: the kill arm must synthesize a terminal action_result so \
             get_finished_result yields Ok(ActionResult{error: Aborted}) — \
             an Err here means the killed upload produced no terminal result",
        );
        let err = action_result.error.expect(
            "M1: the killed upload-tail ActionResult must carry error: Aborted",
        );
        assert_eq!(
            err.code,
            Code::Aborted,
            "Gap 2 / M1: a kill during the upload tail must carry Code::Aborted \
             in the terminal ActionResult (got {err:?})",
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // testing-czar MAJOR (cadre fix-up #2): the `kill_fut` durable-flag
    // FAST PATH (`if !kill_action.cancelled.load(Acquire)`) is the
    // belt-and-suspenders guard for an action that was ALREADY cancelled
    // before `upload_results` first polls — e.g. a kill landed during
    // `execute` (child exited, `inner_execute` set `cancelled=true`), and the
    // pipeline drives the already-cancelled action straight into
    // `upload_results`. In that case the fast-path must preempt the upload
    // IMMEDIATELY, BEFORE it ever touches the slow store, without relying on
    // a post-start `kill_notify` wakeup.
    //
    // This test sets `cancelled=true` directly (`set_cancelled_for_test`,
    // which does NOT fire `kill_notify` — exactly UNLIKE `kill_operation`,
    // which stores a permit) BEFORE spawning `upload_results`. It then
    // asserts the upload returns the terminal `Ok(ActionResult{error:
    // Aborted})` shape immediately AND never entered the slow store
    // (`update_attempts_count() == 0`).
    //
    // Mutation: change `if !kill_action.cancelled.load(...)` to `if false`
    // in `upload_results`. The kill arm then ALWAYS awaits
    // `kill_notify.notified()`; because no `kill_operation` was called there
    // is no stored permit, so the notify never fires, the `biased` select
    // polls `upload_fut` first, the upload enters the blocked slow store, and
    // the `ABORT_DEADLINE` timeout fires with the bespoke message below.
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn kill_fut_fast_path_aborts_precancelled_upload()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "kill_fast_path_worker";
        // A pre-cancelled upload must abort within this bound WITHOUT ever
        // entering the slow store. Far below the 600s max_upload_timeout.
        const ABORT_DEADLINE: Duration = Duration::from_secs(5);

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_fast_store, slow_store, cas_store, ac_store) =
            setup_stores_with_blocking_slow().await?;
        let root_action_directory = make_temp_path("root_action_directory_kill_fast_path");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                    max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                    timeout_handled_externally: false,
                    directory_cache: None,
                    bis_ack_timeout: Duration::from_secs(60),
                    metrics: None,
                    cas_endpoint: String::new(),
                    // Synchronous upload path: upload_results blocks on the
                    // slow store unless the kill fast-path preempts it.
                    deferred_output_uploads_enabled: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |_duration| Box::pin(future::pending()),
                },
            )?);

        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'fast-path-content' > ./out.txt".to_string(),
            ],
            output_paths: vec!["out.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            _fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            _fast_store.as_pin(),
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
            _fast_store.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        ..Default::default()
                    }),
                    operation_id: OperationId::default().to_string(),
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        // Pre-cancel BEFORE upload_results starts — no kill_notify permit is
        // stored (unlike kill_operation). The only thing that can preempt the
        // upload is the fast-path durable-flag check.
        executed.set_cancelled_for_test();

        let before_count = slow_store.update_attempts_count();
        assert_eq!(
            before_count, 0,
            "fixture invariant: no slow-store update before upload_results starts"
        );

        let upload_outcome = tokio::time::timeout(ABORT_DEADLINE, async move {
            executed.upload_results().await
        })
        .await
        .expect(
            "kill_fut fast-path: a pre-cancelled action must abort upload_results \
             via the durable `cancelled` flag WITHOUT a post-start kill_notify — \
             the upload entered the blocked slow store, so the `if !cancelled` \
             fast-path check is missing from kill_fut",
        );

        // The fast-path preempts BEFORE the upload touches the slow store.
        assert_eq!(
            slow_store.update_attempts_count(),
            0,
            "kill_fut fast-path: a pre-cancelled upload must be preempted before \
             entering the slow store (update_attempts must stay 0)"
        );

        let killed_action = upload_outcome.expect(
            "kill_fut fast-path: a pre-cancelled upload must return Ok(self) \
             carrying a terminal ActionResult{error: Aborted}, NOT Err",
        );
        let action_result = killed_action.get_finished_result().await.expect(
            "kill_fut fast-path: the synthesized terminal action_result must let \
             get_finished_result yield Ok(ActionResult{error: Aborted})",
        );
        let err = action_result.error.expect(
            "kill_fut fast-path: the pre-cancelled ActionResult must carry error: Aborted",
        );
        assert_eq!(
            err.code,
            Code::Aborted,
            "kill_fut fast-path: a pre-cancelled upload must abort with Code::Aborted \
             (got {err:?})",
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // N2: output_directories prehash batch tests
    //
    // These tests verify that Phase 1 of `inner_upload_results` walks
    // declared `output_directories` trees and includes their file digests in
    // the single batch `has_with_results()` call, eliminating one individual
    // `has()` RPC per directory-interior file.
    //
    // `HasCountingStore` wraps a `MemoryStore` and counts:
    //   - `single_has_count`: `has_with_results` invocations where `keys.len() == 1`
    //     (these are individual `has()` calls from `upload_file`'s fallback path).
    //   - `batch_has_key_count`: total keys submitted across all multi-key
    //     `has_with_results` invocations (these are the Phase 1 batch calls).
    // -----------------------------------------------------------------------

    struct HasCountingStore {
        inner: Arc<MemoryStore>,
        /// Count of `has_with_results` calls that arrived with exactly one key —
        /// these are individual per-file `has()` RPCs from the upload_file
        /// fallback path at running_actions_manager.rs (upload_file: `has()`).
        single_has_count: AtomicUsize,
        /// Total keys submitted across all multi-key `has_with_results` calls —
        /// these are the Phase 1 batch calls.
        batch_has_key_count: AtomicUsize,
    }

    impl HasCountingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryStore::new(&Default::default()),
                single_has_count: AtomicUsize::new(0),
                batch_has_key_count: AtomicUsize::new(0),
            })
        }

        fn single_has_count(&self) -> usize {
            self.single_has_count.load(Ordering::SeqCst)
        }

        fn batch_has_key_count(&self) -> usize {
            self.batch_has_key_count.load(Ordering::SeqCst)
        }
    }

    impl MetricsComponent for HasCountingStore {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl StoreDriver for HasCountingStore {
        async fn has_with_results(
            self: Pin<&Self>,
            keys: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            if keys.len() == 1 {
                self.single_has_count.fetch_add(1, Ordering::SeqCst);
            } else {
                self.batch_has_key_count
                    .fetch_add(keys.len(), Ordering::SeqCst);
            }
            Pin::new(self.inner.as_ref())
                .has_with_results(keys, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(key, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn inner_store(&self, _key: Option<StoreKey<'_>>) -> &dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
            self
        }

        fn register_item_callback(
            self: Arc<Self>,
            _callback: Arc<dyn ItemCallback>,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn stable_delegation(&self) -> StableDigestDelegation<'_> {
            StableDigestDelegation::Leaf
        }

        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Leaf
        }

        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Leaf
        }
        fn durable_delegation(&self) -> DurableDelegation<'_> {
            DurableDelegation::Leaf
        }

        fn optimized_for(&self, _optimization: StoreOptimizations) -> bool {
            false
        }
    }

    default_health_status_indicator!(HasCountingStore);

    /// Build a `FastSlowStore` with `HasCountingStore` as the slow tier.
    async fn setup_stores_with_counting_slow() -> Result<
        (
            Arc<FilesystemStore>,
            Arc<HasCountingStore>,
            Arc<FastSlowStore>,
            Arc<MemoryStore>,
        ),
        Error,
    > {
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path_counting"),
            temp_path: make_temp_path("temp_path_counting"),
            eviction_policy: None,
            ..Default::default()
        };
        let fast_store = FilesystemStore::new(&fast_config).await?;
        let slow_store = HasCountingStore::new();
        let ac_store = MemoryStore::new(&Default::default());
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(Default::default()),
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

    /// N2: files inside output_directories must be batched into Phase 1
    /// `has_with_results()`, not checked via individual `has()` RPCs.
    ///
    /// Action creates 3 files inside `output_dir/`. Before the fix, each
    /// file triggers one single-key `has_with_results` call (individual
    /// `has()` from upload_file fallback). After the fix, all 3 digests
    /// are included in the Phase 1 batch `has_with_results` call and no
    /// individual `has()` call fires for them.
    ///
    /// The `HasCountingStore` sitting at the slow tier counts:
    ///   - `single_has_count` — individual per-file `has()` calls
    ///   - `batch_has_key_count` — keys submitted in the single batch call
    ///
    /// Mutation-verify: remove the directory-walk from Phase 1 (revert to
    /// top-level-only prehash) → `single_has_count` climbs to 3 and
    /// `batch_has_key_count` drops to 0 for directory-interior files.
    /// The test fails with
    /// "N2 invariant violated: directory-interior files produced N
    ///  individual has() RPC calls; expected 0 (must be covered by batch)"
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n2_output_dir_files_use_batch_has_not_individual()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n2_test_worker";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, counting_store, cas_store, ac_store) =
            setup_stores_with_counting_slow().await?;

        let root_action_directory = make_temp_path("root_action_directory_n2");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Action: create 3 files inside output_dir/ — no top-level output
        // files so all file digests MUST come from the directory walk.
        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "mkdir -p output_dir && \
                 printf 'alpha' > output_dir/a.txt && \
                 printf 'bravo' > output_dir/b.txt && \
                 printf 'charlie' > output_dir/c.txt"
                    .to_string(),
            ],
            // Use output_directories only; no output_files.
            output_directories: vec!["output_dir".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: ProtoDigestFunction::Blake3.into(),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        // Snapshot the store's has() counts before upload so we can
        // measure only what upload_results() itself triggered.
        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        let single_before = counting_store.single_has_count();
        let batch_before = counting_store.batch_has_key_count();

        executed.upload_results().await?;

        let single_after = counting_store.single_has_count();
        let batch_after = counting_store.batch_has_key_count();

        let new_single = single_after - single_before;
        let new_batch_keys = batch_after - batch_before;

        // The 3 directory-interior files must appear in the batch call.
        // (The batch call also covers any directory-proto blobs, but the
        // minimum is the 3 file digests.)
        assert!(
            new_batch_keys >= 3,
            "N2 invariant violated: expected ≥3 file digests in the Phase 1 \
             batch has_with_results call; got {new_batch_keys} batch keys — \
             directory-interior files are not being walked in Phase 1",
        );

        // No individual single-key has() for the directory files.
        // After the fix, the batch covered them — no fallback individual RPC.
        assert_eq!(
            new_single, 0,
            "N2 invariant violated: directory-interior files produced {new_single} \
             individual has() RPC calls; expected 0 (must be covered by batch)",
        );

        running_action_impl.cleanup().await?;
        Ok(())
    }

    /// T2: recursive prehash must reach files nested 2+ levels deep.
    ///
    /// Action creates `output_dir/subdir/nested.txt`. The batch Phase 1
    /// must include nested.txt's digest so that `single_has_count` stays 0
    /// and `batch_has_key_count` >= 1.
    ///
    /// Mutation-verify: comment out the recursive `prehash_directory_tree`
    /// push inside `prehash_directory_tree` → the nested file is not walked
    /// → `single_has_count` rises to ≥1.
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n2_nested_dir_files_use_batch_has_not_individual()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n2_nested_test_worker";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, counting_store, cas_store, ac_store) =
            setup_stores_with_counting_slow().await?;

        let root_action_directory = make_temp_path("root_action_directory_n2_nested");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Action: two files — one at depth 1 and one at depth 2.
        // output_dir/top.txt (depth-1) + output_dir/subdir/nested.txt (depth-2)
        // exercises the recursive branch of prehash_directory_tree.
        // Two files guarantee batch_has_key_count > 0 (HasCountingStore counts
        // multi-key batch calls only; a single-key call increments single_has_count).
        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "mkdir -p output_dir/subdir && \
                 printf 'top' > output_dir/top.txt && \
                 printf 'nested' > output_dir/subdir/nested.txt"
                    .to_string(),
            ],
            output_directories: vec!["output_dir".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: ProtoDigestFunction::Blake3.into(),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        let single_before = counting_store.single_has_count();
        let batch_before = counting_store.batch_has_key_count();

        executed.upload_results().await?;

        let single_after = counting_store.single_has_count();
        let batch_after = counting_store.batch_has_key_count();

        let new_single = single_after - single_before;
        let new_batch_keys = batch_after - batch_before;

        // Both files (top.txt at depth-1 + nested.txt at depth-2) must appear
        // in the Phase 1 batch. HasCountingStore counts multi-key calls only;
        // a single-key Phase 1 call would be classified as single_has_count —
        // the two-file design ensures the batch has ≥2 keys.
        assert!(
            new_batch_keys >= 2,
            "T2 invariant violated: expected ≥2 file digests in the Phase 1 batch \
             (top.txt + nested.txt); got {new_batch_keys} batch keys — \
             recursive prehash_directory_tree is not reaching depth-2 files",
        );

        // No individual has() for either file.
        assert_eq!(
            new_single, 0,
            "T2 invariant violated: nested directory files produced {new_single} \
             individual has() RPC calls; expected 0 (must be covered by batch)",
        );

        running_action_impl.cleanup().await?;
        Ok(())
    }

    /// T1: a symlink inside an output directory is skipped by prehash
    /// (not hashed, not batch-checked) and still uploaded as a SymlinkNode.
    ///
    /// This guards against `prehash_directory_tree` accidentally following
    /// symlinks inside the directory tree. If a symlink were prehashed,
    /// its "file content" digest would enter `batch_checked`; `upload_file`
    /// would then skip the individual has() for a digest that no `upload_file`
    /// call will ever actually upload (symlinks go through `upload_symlink`,
    /// not `upload_file`). This is harmless in current code but wrong in
    /// principle, and a future refactor could make it load-bearing.
    ///
    /// Invariant: the total batch key count equals the number of regular
    /// files only (1 in this test), not the number of regular files + symlinks.
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n2_symlink_inside_output_dir_is_skipped_by_prehash()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n2_symlink_test_worker";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, counting_store, cas_store, ac_store) =
            setup_stores_with_counting_slow().await?;

        let root_action_directory = make_temp_path("root_action_directory_n2_symlink");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Action: output_dir/ contains two regular files and one symlink.
        // Two regular files ensure the batch has ≥2 keys (HasCountingStore
        // counts multi-key calls; a single-key Phase 1 call is counted as
        // single_has_count instead of batch_has_key_count).
        // The symlink should be skipped by prehash_directory_tree (which uses
        // entry.file_type() — lstat semantics — so is_symlink() returns true
        // and neither the is_file() nor is_dir() branch fires).
        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "mkdir -p output_dir && \
                 printf 'real' > output_dir/real.txt && \
                 printf 'also' > output_dir/also_real.txt && \
                 ln -s real.txt output_dir/link.txt"
                    .to_string(),
            ],
            output_directories: vec!["output_dir".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: ProtoDigestFunction::Blake3.into(),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        let single_before = counting_store.single_has_count();
        let batch_before = counting_store.batch_has_key_count();

        executed.upload_results().await?;

        let single_after = counting_store.single_has_count();
        let batch_after = counting_store.batch_has_key_count();

        let new_single = single_after - single_before;
        let new_batch_keys = batch_after - batch_before;

        // Exactly 2 regular files (real.txt + also_real.txt) should be in the batch.
        // If the symlink (link.txt) were accidentally prehashed by prehash_directory_tree,
        // batch_has_key_count would be 3. Dedup collapses same-content digests, so
        // link.txt (same content as real.txt) would dedup to 1 unique digest with
        // real.txt → batch_keys == 2 still. We use >= 2 to accommodate dedup.
        // The load-bearing check is new_single == 0: both regular files are covered
        // by the batch so no upload_file fallback has() fires.
        assert!(
            new_batch_keys >= 2,
            "T1 invariant violated: expected ≥2 batch keys (real.txt + also_real.txt); \
             got {new_batch_keys} — regular files inside output_dir were not prehashed",
        );

        // No individual has() calls: both regular files are covered by the batch.
        // The symlink goes through upload_directory's symlink_futures path (upload_symlink),
        // which never calls upload_file and therefore never fires individual has().
        assert_eq!(
            new_single, 0,
            "T1 invariant violated: {new_single} individual has() RPC calls fired; \
             expected 0 — regular files must be covered by the Phase 1 batch",
        );

        // upload_results must succeed (symlink uploaded as SymlinkNode via
        // upload_directory's symlink_futures path, not upload_file).
        running_action_impl.cleanup().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // N3: F2 (deferred_output_uploads_enabled) skips the Phase 1 batch
    // has_with_results RPC (empty for novel outputs; prior-present digests dedup
    // via emplace_file) while preserving individual-has() suppression.
    //
    // In F2 mode `inner_upload_results` writes outputs to the local fast store
    // (FilesystemStore in production). The Phase 1 batch has_with_results query
    // checks whether the JUST-PRODUCED output-file digests already exist in
    // that fast store — for NOVEL outputs (the common case) they do not, so the
    // batch returns empty and delivers ZERO skips. (A prior action's identical
    // digest CAN still be present — see the rare-case test below.) The batch's
    // ONLY load-bearing side effect was populating `batch_checked`, which
    // suppresses the per-file individual has() inside upload_file
    // (`if !batch_checked.contains(&digest)`).
    //
    // N3 skips the RPC but STILL populates `batch_checked` from the prehashed
    // digests directly (and leaves `known_existing` empty), so downstream
    // behavior is IDENTICAL minus one RPC: individual has() is still suppressed
    // and every fresh file still uploads.
    //
    // The NON-F2 path is unchanged — there the batch hits the remote CAS (slow
    // tier) and delivers REAL skips, so it must still run.
    //
    // To observe the fast-store has() calls in F2 mode the counter must sit at
    // the FAST tier (F2 queries `cas_store.fast_store()`), unlike the N2 tests
    // which place `HasCountingStore` at the slow tier (non-F2 queries the slow
    // tier). But `RunningActionsManagerImpl::new` downcasts `fast_store()` to a
    // concrete `FilesystemStore` (hardlink/pin on the action sandbox), so the
    // fast tier MUST be a real FilesystemStore — a raw `MemoryStore` wrapper
    // fails the downcast with "Expected FilesystemStore store for .fast_store()".
    // `CountingFsStore` therefore WRAPS a real FilesystemStore: it counts
    // `has_with_results` calls and returns the inner FilesystemStore from
    // `inner_store()` so the production downcast still succeeds.
    // -----------------------------------------------------------------------

    /// Counting wrapper around a real `FilesystemStore` for the FAST tier.
    ///
    /// Counts `has_with_results` invocations (single-key vs multi-key, same
    /// convention as `HasCountingStore`) while delegating everything to the
    /// inner FilesystemStore. `inner_store()` returns the FilesystemStore so
    /// the `RunningActionsManagerImpl::new` downcast to `FilesystemStore`
    /// (required for hardlink/pin) succeeds, and the default
    /// `update_with_whole_file` delegates straight to the FilesystemStore
    /// (which is `optimized_for(FileUpdates)`).
    struct CountingFsStore {
        inner: Arc<FilesystemStore>,
        /// `has_with_results` calls with exactly one key — individual per-file
        /// has() RPCs from upload_file's fallback path.
        single_has_count: AtomicUsize,
        /// Total keys across all multi-key `has_with_results` calls — the
        /// Phase 1 batch calls.
        batch_has_key_count: AtomicUsize,
    }

    impl CountingFsStore {
        fn new(inner: Arc<FilesystemStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                single_has_count: AtomicUsize::new(0),
                batch_has_key_count: AtomicUsize::new(0),
            })
        }

        fn single_has_count(&self) -> usize {
            self.single_has_count.load(Ordering::SeqCst)
        }

        fn batch_has_key_count(&self) -> usize {
            self.batch_has_key_count.load(Ordering::SeqCst)
        }
    }

    impl MetricsComponent for CountingFsStore {
        fn publish(
            &self,
            _kind: MetricKind,
            _field_metadata: MetricFieldData,
        ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
            Ok(MetricPublishKnownKindData::Component)
        }
    }

    #[async_trait]
    impl StoreDriver for CountingFsStore {
        async fn has_with_results(
            self: Pin<&Self>,
            keys: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            if keys.len() == 1 {
                self.single_has_count.fetch_add(1, Ordering::SeqCst);
            } else {
                self.batch_has_key_count
                    .fetch_add(keys.len(), Ordering::SeqCst);
            }
            Pin::new(self.inner.as_ref())
                .has_with_results(keys, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(key, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        // Return the inner FilesystemStore so the production downcast in
        // RunningActionsManagerImpl::new finds a concrete FilesystemStore, and
        // the default update_with_whole_file delegates the real file write to
        // it (FilesystemStore is optimized_for(FileUpdates)).
        fn inner_store(&self, _key: Option<StoreKey<'_>>) -> &dyn StoreDriver {
            self.inner.as_ref()
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
            self
        }

        fn register_item_callback(
            self: Arc<Self>,
            _callback: Arc<dyn ItemCallback>,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn stable_delegation(&self) -> StableDigestDelegation<'_> {
            StableDigestDelegation::Leaf
        }

        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Leaf
        }

        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Leaf
        }
        fn durable_delegation(&self) -> DurableDelegation<'_> {
            DurableDelegation::Leaf
        }

        fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
            // Mirror the inner FilesystemStore so callers route file uploads
            // through update_with_whole_file (which delegates to the inner FS).
            StoreDriver::optimized_for(self.inner.as_ref(), optimization)
        }
    }

    default_health_status_indicator!(CountingFsStore);

    /// Build a `FastSlowStore` with a real `FilesystemStore` (wrapped in
    /// `CountingFsStore`) as the FAST tier so F2-mode
    /// (`deferred_output_uploads_enabled`) has()/has_with_results calls — which
    /// target the fast store — are counted while the production downcast to a
    /// concrete FilesystemStore still succeeds.
    async fn setup_stores_with_counting_fast() -> Result<
        (
            Arc<CountingFsStore>,
            Arc<MemoryStore>,
            Arc<FastSlowStore>,
            Arc<MemoryStore>,
        ),
        Error,
    > {
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path_counting_fast"),
            temp_path: make_temp_path("temp_path_counting_fast"),
            eviction_policy: None,
            ..Default::default()
        };
        let fs_store = FilesystemStore::new(&fast_config).await?;
        let counting_fast_store = CountingFsStore::new(fs_store);
        let slow_store = MemoryStore::new(&Default::default());
        let ac_store = MemoryStore::new(&Default::default());
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(Default::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                slow_writes_in_flight_max_bytes: 0,
            },
            Store::new(counting_fast_store.clone()),
            Store::new(slow_store.clone()),
        );
        Ok((counting_fast_store, slow_store, cas_store, ac_store))
    }

    /// N3: in F2 mode the Phase 1 batch `has_with_results` RPC is skipped
    /// (it is always empty for fresh outputs on the local fast store), but
    /// `batch_checked` is still populated so the per-file individual `has()`
    /// inside `upload_file` stays suppressed and every file still uploads.
    ///
    /// Action produces 3 top-level output files. With `CountingFsStore` (a
    /// real FilesystemStore plus has()-counters) at the FAST tier (the store F2
    /// writes/queries), after `upload_results()`:
    ///   (a) `batch_has_key_count == 0` — the multi-key batch RPC was skipped.
    ///   (b) `single_has_count == 0`    — `batch_checked` suppression holds; no
    ///       individual per-file has() RPC fired (NO N×has regression).
    ///   (c) all 3 output blobs are present in the fast store (still uploaded).
    ///
    /// Against unchanged (pre-N3) source this fails at (a): the batch RPC runs,
    /// so `batch_has_key_count == 3`, not 0 — the right reason (batch not yet
    /// skipped).
    ///
    /// Mutation-verify: drop the F2 `batch_checked` population (so `checked`
    /// stays empty in F2). Then every file falls into upload_file's
    /// `!batch_checked.contains(&digest)` branch → individual has() → assertion
    /// (b) fails with "N3 regressed to per-file has() — batch_checked not
    /// populated in F2".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n3_f2_skips_batch_has_but_suppresses_individual_has()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n3_f2_test_worker";
        const OUTPUT_FILE_COUNT: usize = 3;

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (counting_fast_store, _slow_store, cas_store, ac_store) =
            setup_stores_with_counting_fast().await?;

        let root_action_directory = make_temp_path("root_action_directory_n3_f2");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                // F2 kill-switch ON: outputs written to the fast store only;
                // Phase 1 batch has() is the always-empty RPC N3 removes.
                deferred_output_uploads_enabled: true,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Action: three top-level output FILES (no directories) — each is
        // prehashed via prehash_single_file and would, pre-N3, be submitted to
        // the Phase 1 batch has_with_results call.
        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'alpha' > a.txt && \
                 printf 'bravo' > b.txt && \
                 printf 'charlie' > c.txt"
                    .to_string(),
            ],
            output_paths: vec!["a.txt".to_string(), "b.txt".to_string(), "c.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: ProtoDigestFunction::Blake3.into(),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        // Snapshot AFTER execute so only upload_results()'s own calls count.
        let single_before = counting_fast_store.single_has_count();
        let batch_before = counting_fast_store.batch_has_key_count();

        executed.upload_results().await?;

        let new_single = counting_fast_store.single_has_count() - single_before;
        let new_batch_keys = counting_fast_store.batch_has_key_count() - batch_before;

        // (a) Batch has_with_results RPC skipped in F2 — zero batch keys.
        assert_eq!(
            new_batch_keys, 0,
            "N3 invariant violated: F2 mode must skip the always-empty Phase 1 \
             batch has_with_results RPC; got {new_batch_keys} batch keys — \
             the batch RPC is still firing on the fast store",
        );

        // (b) batch_checked suppression holds — NO individual per-file has().
        // This is the anti-regression assertion: skipping the batch WITHOUT
        // populating batch_checked would push every file into upload_file's
        // individual-has() fallback (N×has, worse than today).
        assert_eq!(
            new_single, 0,
            "N3 regressed to per-file has() — batch_checked not populated in F2; \
             got {new_single} individual has() RPC calls (expected 0): skipping \
             the batch left batch_checked empty so upload_file fell back to N×has()",
        );

        // (c) All 3 output blobs still uploaded to the fast store.
        let action_result = running_action_impl.clone().get_finished_result().await?;
        assert_eq!(
            action_result.output_files.len(),
            OUTPUT_FILE_COUNT,
            "N3: all {OUTPUT_FILE_COUNT} output files must still be produced",
        );
        for output_file in &action_result.output_files {
            let key: StoreKey<'_> = output_file.digest.into();
            let present = tokio::time::timeout(
                Duration::from_secs(5),
                Pin::new(counting_fast_store.as_ref()).has(key),
            )
            .await
            .expect("fast-store has() must not hang — FilesystemStore index contract")?;
            assert!(
                present.is_some(),
                "N3: output blob {:?} must be present in the fast store after \
                 upload_results — file was not uploaded",
                output_file.digest,
            );
        }

        running_action_impl.cleanup().await?;
        Ok(())
    }

    /// N3 non-regression: the NON-F2 (synchronous) path MUST still run the
    /// Phase 1 batch `has_with_results` RPC. There the batch hits the remote
    /// CAS (the slow tier in the FastSlowStore) and delivers REAL upload skips,
    /// so N3 must NOT touch it.
    ///
    /// With `HasCountingStore` at the SLOW tier and `deferred_output_uploads_enabled
    /// = false`, after `upload_results()` for 3 top-level output files:
    ///   `batch_has_key_count >= 3` — the batch RPC still fires and covers the
    ///   3 output-file digests.
    ///
    /// Mutation-verify: gate the batch on `!deferred` AND extend that skip to
    /// the non-F2 arm (i.e. skip the batch unconditionally) → `batch_has_key_count`
    /// drops to 0 and this test fails with "N3 over-reached: non-F2 batch
    /// has_with_results was skipped — remote-CAS skips lost".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n3_non_f2_still_runs_batch_has()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n3_non_f2_test_worker";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, counting_store, cas_store, ac_store) =
            setup_stores_with_counting_slow().await?;

        let root_action_directory = make_temp_path("root_action_directory_n3_non_f2");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                // NON-F2: synchronous path — batch has() hits the remote CAS
                // (slow tier) and delivers real skips, so it must still run.
                deferred_output_uploads_enabled: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'alpha' > a.txt && \
                 printf 'bravo' > b.txt && \
                 printf 'charlie' > c.txt"
                    .to_string(),
            ],
            output_paths: vec!["a.txt".to_string(), "b.txt".to_string(), "c.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        let execute_request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: ProtoDigestFunction::Blake3.into(),
            ..Default::default()
        };
        let operation_id = OperationId::default().to_string();

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(execute_request),
                    operation_id,
                    queued_timestamp: None,
                    platform: action.platform.clone(),
                    worker_id: WORKER_ID.to_string(),
                    resolved_directories: Vec::new(),
                    resolved_directory_digests: Vec::new(),
                    missing_digests: Vec::new(),
                },
            )
            .await?;

        let prepared = running_action_impl.clone().prepare_action().await?;
        let executed = prepared.execute().await?;

        let batch_before = counting_store.batch_has_key_count();

        executed.upload_results().await?;

        let new_batch_keys = counting_store.batch_has_key_count() - batch_before;

        // The batch must still fire and cover the 3 output-file digests.
        assert!(
            new_batch_keys >= 3,
            "N3 over-reached: non-F2 batch has_with_results was skipped — \
             remote-CAS skips lost; got {new_batch_keys} batch keys (expected ≥3). \
             N3 must only skip the batch in F2 mode.",
        );

        running_action_impl.cleanup().await?;
        Ok(())
    }

    // N3 rare-case: in F2, a prior action's identical output digest is ALREADY
    // present in the local fast FilesystemStore. N3 leaves `known_existing`
    // empty, so upload_file does NOT skip the upload via the `known_existing`
    // gate (upload_file:2482); it proceeds to upload-then-dedup. The duplicate
    // write is absorbed by the FilesystemStore `emplace_file` content_is_immutable
    // short-circuit (filesystem_store.rs:1274-1280): when the key already exists
    // and the store is immutable, emplace_file returns Ok BEFORE the rename, so
    // NO second rename / NO duplicate file write occurs and the action still
    // succeeds. This is the config-contingent dedup path the green N3 tests skip
    // (they use FilesystemSpec::default() → content_is_immutable: false + novel
    // outputs, so emplace always renames). The deployed worker config sets
    // `content_is_immutable: true` on the fast tier (worker.json5:68), so this
    // test pins the PRODUCTION precondition that makes the rare-case dedup free.
    //
    // Observability seam: the inner FilesystemStore is built with a custom
    // `rename_fn` that counts invocations. `emplace_file` calls rename exactly
    // once per write that reaches the rename (filesystem_store.rs:1333); the
    // dedup short-circuit returns before it. So the rename count is a direct
    // observable for "no second rename / no duplicate write".

    /// Module-level rename counter for the immutable-dedup test. `rename_fn` is
    /// a bare `fn` pointer (no closure capture), so the counter must be a
    /// `static`. Only the immutable-dedup test installs this `rename_fn`, and
    /// the whole `tests` module is `#[serial]` (file top), so no other test runs
    /// concurrently to perturb the count.
    static IMMUTABLE_DEDUP_RENAME_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn counting_rename_fn(from: &std::ffi::OsStr, to: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        IMMUTABLE_DEDUP_RENAME_COUNT.fetch_add(1, Ordering::SeqCst);
        std::fs::rename(from, to)
    }

    /// Build a `FastSlowStore` whose FAST tier is a real `FilesystemStore` with
    /// `content_is_immutable: true` (matching deployed `worker.json5:68`) and a
    /// rename-counting `rename_fn`, so the F2 upload-then-dedup path can be
    /// observed via the rename count. Mirrors `setup_stores_with_counting_fast`
    /// otherwise (CountingFsStore wrapper for the production downcast).
    async fn setup_stores_with_immutable_counting_fast() -> Result<
        (
            Arc<CountingFsStore>,
            Arc<FastSlowStore>,
            Arc<MemoryStore>,
        ),
        Error,
    > {
        let fast_config = FilesystemSpec {
            content_path: make_temp_path("content_path_immutable_fast"),
            temp_path: make_temp_path("temp_path_immutable_fast"),
            eviction_policy: None,
            // The PRODUCTION precondition: deployed worker.json5:68 sets this
            // true on the fast tier. With it false (the schema default) the
            // emplace dedup at filesystem_store.rs:1274 never fires and the
            // rare-case duplicate write re-renames — exactly the regression
            // this test guards.
            content_is_immutable: true,
            ..Default::default()
        };
        let fs_store = FilesystemStore::new_with_timeout_and_rename_fn(
            &fast_config,
            counting_rename_fn,
        )
        .await?;
        let counting_fast_store = CountingFsStore::new(fs_store);
        let slow_store = MemoryStore::new(&Default::default());
        let ac_store = MemoryStore::new(&Default::default());
        let cas_store = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(Default::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
                slow_writes_in_flight_max_bytes: 0,
            },
            Store::new(counting_fast_store.clone()),
            Store::new(slow_store.clone()),
        );
        Ok((counting_fast_store, cas_store, ac_store))
    }

    /// N3 rare-case dedup: when a prior action's identical output digest is
    /// already in the fast store, the second F2 `upload_results()` absorbs the
    /// duplicate write via the `content_is_immutable` emplace short-circuit —
    /// NO second rename, and the action still succeeds.
    ///
    /// Two runs of the SAME action (identical output content ⇒ identical
    /// digests) against the SAME fast FilesystemStore:
    ///   Run 1 (novel): each output emplaces + renames → 3 renames recorded.
    ///   Run 2 (prior-present): each digest is already in the fast store's
    ///     evicting_map, so emplace_file returns Ok before the rename →
    ///     ZERO additional renames, yet all 3 outputs still report present.
    ///
    /// Asserts:
    ///   (a) run 1 performed ≥ OUTPUT_FILE_COUNT renames (outputs were novel);
    ///   (b) run 2 performed ZERO additional renames (dedup absorbed the
    ///       duplicate write — no second rename / no duplicate file write);
    ///   (c) run 2's action result still lists all OUTPUT_FILE_COUNT outputs,
    ///       all present in the fast store (the action still succeeds).
    ///
    /// Mutation-verify: flip the fast tier's `content_is_immutable` to false
    /// (or comment out the emplace dedup at filesystem_store.rs:1274-1280).
    /// Then run 2 re-renames every output and assertion (b) red-fails with
    /// "N3 rare-case regression: prior-present digest was re-written".
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn n3_f2_prior_present_digest_dedups_no_second_rename()
    -> Result<(), Box<dyn core::error::Error>> {
        const WORKER_ID: &str = "n3_immutable_dedup_worker";
        const OUTPUT_FILE_COUNT: usize = 3;

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        // Reset the module-level rename counter (the static persists across
        // tests in one process; the `tests` module is #[serial] so no other
        // test perturbs it during this run).
        IMMUTABLE_DEDUP_RENAME_COUNT.store(0, Ordering::SeqCst);

        let (counting_fast_store, cas_store, ac_store) =
            setup_stores_with_immutable_counting_fast().await?;

        let root_action_directory =
            make_temp_path("root_action_directory_n3_immutable_dedup");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
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
                max_upload_timeout: Duration::from_secs(DEFAULT_MAX_UPLOAD_TIMEOUT),
                timeout_handled_externally: false,
                directory_cache: None,
                bis_ack_timeout: Duration::from_secs(60),
                metrics: None,
                cas_endpoint: String::new(),
                // F2 ON: outputs written to the fast store; in F2 known_existing
                // is left empty so a prior-present digest takes upload-then-dedup.
                deferred_output_uploads_enabled: true,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(future::pending()),
            },
        )?);

        // Deterministic output content so both runs produce identical digests.
        let command = Command {
            arguments: vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 'alpha' > a.txt && \
                 printf 'bravo' > b.txt && \
                 printf 'charlie' > c.txt"
                    .to_string(),
            ],
            output_paths: vec!["a.txt".to_string(), "b.txt".to_string(), "c.txt".to_string()],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".to_string(),
                value: env::var("PATH").unwrap(),
            }],
            ..Default::default()
        };

        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_pin(),
            &mut DigestHasherFunc::Blake3.hasher(),
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
            &mut DigestHasherFunc::Blake3.hasher(),
        )
        .await?;

        // Run the same action twice; each run gets its own operation id /
        // sandbox but produces byte-identical outputs ⇒ identical digests.
        let run_once = async |run_label: &str| -> Result<Vec<DigestInfo>, Error> {
            let execute_request = ExecuteRequest {
                action_digest: Some(action_digest.into()),
                digest_function: ProtoDigestFunction::Blake3.into(),
                ..Default::default()
            };
            let operation_id = OperationId::default().to_string();
            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(execute_request),
                        operation_id,
                        queued_timestamp: None,
                        platform: action.platform.clone(),
                        worker_id: WORKER_ID.to_string(),
                        resolved_directories: Vec::new(),
                        resolved_directory_digests: Vec::new(),
                        missing_digests: Vec::new(),
                    },
                )
                .await
                .err_tip(|| format!("create_and_add_action failed for {run_label}"))?;
            let prepared = running_action_impl.clone().prepare_action().await?;
            let executed = prepared.execute().await?;
            executed.upload_results().await?;
            let action_result = running_action_impl.clone().get_finished_result().await?;
            let digests: Vec<DigestInfo> = action_result
                .output_files
                .iter()
                .map(|f| f.digest)
                .collect();
            running_action_impl.cleanup().await?;
            Ok(digests)
        };

        // Run 1: novel outputs — emplace renames each into place.
        let run1_digests = run_once("run1").await?;
        let renames_after_run1 = IMMUTABLE_DEDUP_RENAME_COUNT.load(Ordering::SeqCst);

        // (a) Run 1 must have actually renamed the novel outputs into place,
        // otherwise run 2's "no second rename" is vacuous (nothing was ever
        // present to dedup against).
        assert!(
            renames_after_run1 >= OUTPUT_FILE_COUNT,
            "N3 rare-case test setup invalid: run 1 performed only \
             {renames_after_run1} renames (expected ≥{OUTPUT_FILE_COUNT}); the \
             novel outputs were not emplaced, so the dedup precondition (digest \
             already present) is not established",
        );

        // Run 2: identical outputs — every digest is now ALREADY present in the
        // fast store, so the F2 upload-then-dedup path must absorb each write.
        let run2_digests = run_once("run2").await?;
        let renames_after_run2 = IMMUTABLE_DEDUP_RENAME_COUNT.load(Ordering::SeqCst);
        let renames_during_run2 = renames_after_run2 - renames_after_run1;

        // Sanity: both runs produced the same output digests (so run 2 really
        // re-uploads the SAME digests run 1 emplaced — the prior-present case).
        assert_eq!(
            run1_digests, run2_digests,
            "N3 rare-case test invalid: the two runs produced different output \
             digests, so run 2 is not exercising the prior-present dedup path",
        );

        // (b) THE CONTRACT: run 2 performed ZERO additional renames — the
        // content_is_immutable emplace short-circuit absorbed every duplicate
        // write. With content_is_immutable false (or the dedup removed) each
        // prior-present digest re-renames and this fails.
        assert_eq!(
            renames_during_run2, 0,
            "N3 rare-case regression: prior-present digest was re-written — run 2 \
             performed {renames_during_run2} renames (expected 0). The F2 \
             upload-then-dedup path did NOT absorb the duplicate write; the \
             content_is_immutable emplace short-circuit \
             (filesystem_store.rs:1274-1280) did not fire. This is the \
             production precondition (worker.json5:68 content_is_immutable=true) \
             that makes the rare-case dedup free",
        );

        // (c) The action still succeeds — all outputs present in the fast store
        // after the deduped run.
        assert_eq!(
            run2_digests.len(),
            OUTPUT_FILE_COUNT,
            "N3 rare-case: run 2 must still produce all {OUTPUT_FILE_COUNT} \
             outputs after the dedup",
        );
        for digest in &run2_digests {
            let key: StoreKey<'_> = (*digest).into();
            let present = tokio::time::timeout(
                Duration::from_secs(5),
                Pin::new(counting_fast_store.as_ref()).has(key),
            )
            .await
            .expect("fast-store has() must not hang — FilesystemStore index contract")?;
            assert!(
                present.is_some(),
                "N3 rare-case: output blob {digest:?} must be present in the fast \
                 store after the deduped run 2",
            );
        }

        Ok(())
    }

}
