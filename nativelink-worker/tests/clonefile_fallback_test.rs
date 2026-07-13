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

//! #clonefile-fallback regression suite.
//!
//! Bug: the DirectoryCache whole-tree APFS `clonefile(2)` fast path (one ~1ms
//! CoW syscall) NEVER fires in production (`dir_cache_hit_clonefile_total = 0`
//! fleet-wide) because `inner_prepare_action` ran the input-tree materialise
//! `[B2]` (which ends in `hardlink_directory_tree` → `try_clonefile`)
//! CONCURRENTLY with output-dir prep `[C]` into the SAME `work_directory`
//! (the "O5 overlap", commit 333ce15e). `[C]` pre-creates output dirs
//! (`bazel-out/…`) → the work dir is non-empty when `[B2]`'s clonefile runs
//! → `try_clonefile` (`fs_util.rs`) returns Err on a non-empty dst → the hit
//! falls back to the per-file `hard_link_batch` (~600ms on the critical path,
//! every action).
//!
//! Fix: `inner_prepare_action` normal mode now runs `[B2]` (materialise into
//! the EMPTY work dir) FIRST, then creates the output-only directories `[C]`
//! (idempotent `create_dir_all`, so the input-tree dirs the clone already
//! brought are no-ops). The materialise therefore sees an empty dst and the
//! clonefile fires.
//!
//! Invariant proven: input-tree materialisation into a cache-HIT work dir
//! must run against an EMPTY dst so the APFS `clonefile(2)` fast path can be
//! taken.
//!
//! Production composition: `RunningActionImpl::inner_prepare_action` (driven
//! through `RunningActionsManager::create_and_add_action` +
//! `RunningAction::prepare_action`) over a real `DirectoryCache` whose backing
//! CAS is a `FastSlowStore{ fast: FilesystemStore, slow: MemoryStore }` — the
//! `has_fast_path=true` shape the production worker runs (a `MemoryStore`-only
//! fast tier would `downcast` to `None` and take the serial construct path,
//! not the hardlink/clonefile path this bug lives on).
//!
//! Two coverage layers (the bug is macOS-specific — `try_clonefile` is
//! `#[cfg(target_os = "macos")]`, and Linux always hardlinks regardless):
//!  - `clonefile_fires_on_cache_hit_into_work_dir` (macOS-gated): the DIRECT
//!    dispatch-named signal — `dir_cache_hit_clonefile_total` increments on a
//!    hit and `dir_cache_hit_hardlink_total` does NOT (clonefile fired, not
//!    fell back). This is the platform where the fleet observed the bug.
//!  - `cache_hit_materialise_sees_empty_dst` (portable / CI): the ordering
//!    invariant — a cache-hit materialise finds an EMPTY dst
//!    (`dir_cache_hit_clonefile_preempted_total` stays 0). Runs on the
//!    Linux+Windows CI matrix (no macOS runner exists) so an ordering
//!    regression that re-populates the work dir before materialise is caught.

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
use nativelink_util::o11_probes::dir_cache_counters;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::directory_cache::{DirectoryCache, DirectoryCacheConfig};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, RunningAction, RunningActionImpl, RunningActionsManager,
    RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use rand::Rng;

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data,
    )
}

/// Build the production-shaped CAS: `FastSlowStore{ fast: FilesystemStore,
/// slow: MemoryStore }`. The `FilesystemStore` fast tier is load-bearing —
/// `DirectoryCache::new` downcasts the fast store to `FilesystemStore` to get
/// `has_fast_path=true`; a `MemoryStore` fast tier downcasts to `None` and the
/// cache falls back to the serial construct path (NOT the hardlink/clonefile
/// materialise path this bug lives on).
async fn setup_cas() -> Result<(Arc<FastSlowStore>, Arc<MemoryStore>), Error> {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = MemorySpec::default();
    let fast_store: Arc<FilesystemStore> = FilesystemStore::new(&fast_config).await?;
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
    Ok((cas_store, ac_store))
}

/// Uploads an action whose input tree contains a `src/` subdirectory (with a
/// file) and whose command declares an output path in an OUTPUT-ONLY directory
/// (`outputs/`) that is NOT part of the input tree. The output-only dir is the
/// one `[C]` would create in the work dir; if `[C]` runs before/with the
/// materialise the work dir is non-empty and clonefile is preempted.
///
/// Returns the action digest.
async fn upload_action_with_output_only_dir(
    cas_store: &Arc<FastSlowStore>,
) -> Result<DigestInfo, Error> {
    let file_content = b"hello from src";
    let file_content_digest = DigestInfo::new([0xABu8; 32], file_content.len() as u64);
    cas_store
        .as_ref()
        .update_oneshot(file_content_digest, Bytes::from_static(file_content))
        .await
        .err_tip(|| "uploading file content")?;

    // Input tree: src/ subdir with one file.
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

    // Command: output path lives in `outputs/`, an OUTPUT-ONLY directory that
    // is NOT in the input tree — this is the directory [C] pre-creates.
    let command = Command {
        arguments: vec!["true".to_string()],
        output_paths: vec!["outputs/result.o".to_string()],
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
    Ok(action_digest)
}

fn make_manager(
    cas_store: Arc<FastSlowStore>,
    ac_store: Arc<MemoryStore>,
    root_action_directory: String,
    directory_cache: Arc<DirectoryCache>,
) -> Result<Arc<RunningActionsManagerImpl>, Error> {
    RunningActionsManagerImpl::new(RunningActionsManagerArgs {
        root_action_directory,
        execution_configuration: ExecutionConfiguration::default(),
        cas_store: cas_store.clone(),
        ac_store: Some(Store::new(ac_store)),
        ac_mirror_target: None,
        historical_store: Store::new(cas_store),
        upload_action_result_config: &nativelink_config::cas_server::UploadActionResultConfig {
            upload_ac_results_strategy: nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
            ..Default::default()
        },
        max_action_timeout: Duration::MAX,
        max_upload_timeout: Duration::from_secs(600),
        timeout_handled_externally: false,
        directory_cache: Some(directory_cache),
        bis_ack_timeout: Duration::from_secs(60),
        metrics: None,
        cas_endpoint: String::new(),
        deferred_output_uploads_enabled: false,
    })
    .map(Arc::new)
}

/// Runs one action through the full `prepare_action` (= `inner_prepare_action`)
/// production path, returning the PREPARED action so the caller can inspect the
/// materialised work directory on disk BEFORE `cleanup()` removes it (cleanup
/// deletes the work dir, so any `fs::metadata` check must happen first).
async fn prepare_one_action(
    manager: &Arc<RunningActionsManagerImpl>,
    action: &Action,
    action_digest: DigestInfo,
) -> Result<Arc<RunningActionImpl>, Box<dyn core::error::Error>> {
    let execute_request = ExecuteRequest {
        action_digest: Some(action_digest.into()),
        ..Default::default()
    };
    let operation_id = OperationId::default().to_string();

    let running_action = manager
        .clone()
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
    .map_err(|e| format!("prepare_action must succeed: {e:?}"))
    .unwrap();

    Ok(prepared)
}

/// macOS-gated DIRECT signal (the dispatch's primary contract). On a
/// DirectoryCache HIT, `inner_prepare_action` must materialise the cached tree
/// into the work dir via APFS `clonefile(2)` — so `dir_cache_hit_clonefile`
/// increments and `dir_cache_hit_hardlink` does NOT (clonefile fired, it did
/// not fall back). This is the platform where the fleet observed
/// `dir_cache_hit_clonefile_total = 0`.
///
/// FAIL on pre-fix code: `[C]` (running concurrently under the O5 overlap)
/// pre-populates the work dir with `outputs/`; `try_clonefile` finds a
/// non-empty dst → Err → the hit falls back to hardlink → clonefile delta 0,
/// hardlink delta >= 1 → this test red-fails.
///
/// Mutation (re-introduce the pre-populate): edit `inner_prepare_action` to
/// create an output dir in the work dir BEFORE the materialise → clonefile
/// stays 0 → this test red-fails with the bespoke message below.
#[cfg(target_os = "macos")]
#[nativelink_test]
async fn clonefile_fires_on_cache_hit_into_work_dir()
-> Result<(), Box<dyn core::error::Error>> {
    let (cas_store, ac_store) = setup_cas().await?;
    let action_digest = upload_action_with_output_only_dir(&cas_store).await?;
    // Re-decode the Action so callers get platform (empty here); simplest is to
    // reconstruct the same shape used by the uploader.
    let action = Action {
        ..Default::default()
    };

    let cache = Arc::new(
        DirectoryCache::new(
            DirectoryCacheConfig {
                max_entries: 100,
                max_size_bytes: 64 * 1024 * 1024,
                cache_root: make_temp_path("dir_cache_root").into(),
                direct_use_mode: false,
            },
            Store::new(cas_store.clone()),
            Some(cas_store.clone()),
        )
        .await?,
    );

    let root_action_directory = make_temp_path("root_action_dir_macos");
    fs::create_dir_all(&root_action_directory).await?;
    let manager = make_manager(cas_store, ac_store, root_action_directory, cache)?;

    // Action 1: cache MISS — constructs + caches the tree. (Also materialises,
    // but into a fresh work dir; the clonefile/hardlink split we assert is on
    // the HIT below.)
    let prepared_miss = prepare_one_action(&manager, &action, action_digest).await?;
    prepared_miss.cleanup().await?;

    // Snapshot the process-global counters, then run action 2 (a HIT).
    let clonefile_before = dir_cache_counters()
        .hit_clonefile
        .load(core::sync::atomic::Ordering::Relaxed);
    let hardlink_before = dir_cache_counters()
        .hit_hardlink
        .load(core::sync::atomic::Ordering::Relaxed);

    let prepared_hit = prepare_one_action(&manager, &action, action_digest).await?;
    prepared_hit.cleanup().await?;

    let clonefile_delta = dir_cache_counters()
        .hit_clonefile
        .load(core::sync::atomic::Ordering::Relaxed)
        - clonefile_before;
    let hardlink_delta = dir_cache_counters()
        .hit_hardlink
        .load(core::sync::atomic::Ordering::Relaxed)
        - hardlink_before;

    assert!(
        clonefile_delta >= 1,
        "#clonefile-fallback: DirectoryCache HIT did NOT materialise via clonefile — \
         dir_cache_hit_clonefile_total advanced by {clonefile_delta}, expected >= 1. \
         The APFS clonefile(2) fast path was preempted: the work dir was non-empty when \
         the input-tree materialise ran (output-dir prep [C] pre-populated it before [B2]), \
         so try_clonefile returned Err and the hit fell back to per-file hardlink."
    );
    assert_eq!(
        hardlink_delta, 0,
        "#clonefile-fallback: DirectoryCache HIT fell back to hardlink \
         (dir_cache_hit_hardlink_total advanced by {hardlink_delta}) instead of using \
         clonefile — the materialise saw a non-empty dst (clonefile preempted by [C])."
    );

    Ok(())
}

/// Portable / CI ordering invariant (Linux + Windows matrix; no macOS runner
/// exists). A DirectoryCache HIT's materialise must run against an EMPTY work
/// dir — i.e. output-dir prep `[C]` must NOT pre-populate the work dir before
/// the input-tree materialise `[B2]`. Observed via the
/// `dir_cache_hit_clonefile_preempted_total` counter, which increments in
/// `try_hardlink_cached` whenever the materialise finds a non-empty dst
/// (clonefile would be preempted). After the fix the hit materialise sees an
/// empty dst → the counter stays 0.
///
/// This is the CI mutation anchor: the macOS clonefile assertion above does
/// NOT run on the Linux+Windows CI matrix, so this counter carries the
/// ordering regression guard on the platform CI actually runs.
///
/// FAIL on pre-fix code / mutation: `[C]` pre-populates `work_dir/outputs/`
/// before/with the materialise → the materialise sees a non-empty dst →
/// `dir_cache_hit_clonefile_preempted_total` increments → this test red-fails
/// with the bespoke message below.
#[nativelink_test]
async fn cache_hit_materialise_sees_empty_dst()
-> Result<(), Box<dyn core::error::Error>> {
    let (cas_store, ac_store) = setup_cas().await?;
    let action_digest = upload_action_with_output_only_dir(&cas_store).await?;
    let action = Action {
        ..Default::default()
    };

    let cache = Arc::new(
        DirectoryCache::new(
            DirectoryCacheConfig {
                max_entries: 100,
                max_size_bytes: 64 * 1024 * 1024,
                cache_root: make_temp_path("dir_cache_root_portable").into(),
                direct_use_mode: false,
            },
            Store::new(cas_store.clone()),
            Some(cas_store.clone()),
        )
        .await?,
    );

    let root_action_directory = make_temp_path("root_action_dir_portable");
    fs::create_dir_all(&root_action_directory).await?;
    let manager = make_manager(cas_store, ac_store, root_action_directory, cache)?;

    // Action 1: cache MISS — constructs + caches the tree.
    let prepared_miss = prepare_one_action(&manager, &action, action_digest).await?;
    // Sanity: the input tree materialised (check BEFORE cleanup removes the dir).
    let work_dir_miss = prepared_miss.get_work_directory().to_string();
    assert!(
        fs::metadata(format!("{work_dir_miss}/src/input_file.c")).await.is_ok(),
        "miss-path input file src/input_file.c was not materialised",
    );
    prepared_miss.cleanup().await?;

    // Snapshot the preempted counter, then run action 2 (a HIT).
    let preempted_before = dir_cache_counters()
        .hit_clonefile_preempted
        .load(core::sync::atomic::Ordering::Relaxed);

    let prepared_hit = prepare_one_action(&manager, &action, action_digest).await?;
    let work_dir_hit = prepared_hit.get_work_directory().to_string();

    let preempted_delta = dir_cache_counters()
        .hit_clonefile_preempted
        .load(core::sync::atomic::Ordering::Relaxed)
        - preempted_before;

    // The HIT materialise must have seen an EMPTY dst (output dirs created AFTER
    // it). If [C] pre-populated the work dir the counter fires.
    assert_eq!(
        preempted_delta, 0,
        "#clonefile-fallback: DirectoryCache HIT materialise ran against a NON-EMPTY dst \
         (dir_cache_hit_clonefile_preempted_total advanced by {preempted_delta}) — output-dir \
         prep [C] pre-populated the work dir before the input-tree materialise [B2], which on \
         macOS preempts the clonefile(2) fast path. inner_prepare_action must materialise into \
         the empty work dir FIRST, then create the output-only directories."
    );

    // Both input and output-only dirs must exist after prepare_action (the
    // reorder must not drop [C]'s work). Check BEFORE cleanup.
    assert!(
        fs::metadata(format!("{work_dir_hit}/src/input_file.c")).await.is_ok(),
        "hit-path input file src/input_file.c was not materialised after the reorder",
    );
    assert!(
        fs::metadata(format!("{work_dir_hit}/outputs")).await.is_ok(),
        "hit-path output-only parent dir outputs/ was not created by [C] after the reorder",
    );
    prepared_hit.cleanup().await?;

    Ok(())
}
