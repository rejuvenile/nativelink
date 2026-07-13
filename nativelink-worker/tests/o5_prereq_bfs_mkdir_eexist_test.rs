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

//! #O5 prerequisite: `download_to_directory` BFS mkdir must tolerate EEXIST.
//!
//! This is the safety prerequisite for the O5 overlap restructure: when
//! `prepare_output_directory` [C] runs concurrently with
//! `prepare_action_inputs` [B2], [C] may call `create_dir_all` on a path
//! that [B2]'s BFS also tries to `create_dir`. The BFS must not fail with
//! EEXIST when the directory was pre-created by [C].
//!
//! Contract (asymmetric):
//! - Under-action (T1): if [C] pre-creates a shared parent directory before
//!   [B2]'s BFS runs, `download_to_directory` MUST succeed — not fail with
//!   "Could not create directory". Verified by
//!   `bfs_mkdir_succeeds_when_dir_pre_created_by_output_prep`.
//! - Over-action (T2): if [C] pre-creates a *file* (not a directory) at the
//!   same path, `download_to_directory` MUST still fail with an error
//!   (existing file at a directory path is a real conflict). Verified by
//!   `bfs_mkdir_fails_when_file_pre_exists_at_dir_path`.
//!
//! Mutation verification (commit 1 prereq):
//! - Mutant A: remove the AlreadyExists tolerance from the BFS mkdir →
//!   T1 MUST red-fail with "BFS mkdir unexpectedly failed with AlreadyExists
//!   on pre-created shared parent".
//! - Mutant B: widen tolerance to ignore AlreadyExists even for a file →
//!   T2 MUST red-fail with "BFS mkdir should have failed but returned Ok".

use std::sync::Arc;

use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use rand::Rng;

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        std::env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data,
    )
}

async fn setup_stores() -> Result<
    (
        Arc<FilesystemStore>,
        Arc<MemoryStore>,
        Arc<FastSlowStore>,
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
    Ok((fast_store, slow_store, cas_store))
}

/// Build a two-level tree: root → subdir (empty), root has one file.
/// Returns (root_digest, subdir_name).
///
/// CAS layout:
///   root_digest → ProtoDirectory { directories: [subdir_digest → "subdir"], files: [file1] }
///   subdir_digest → ProtoDirectory {} (empty leaf)
///   file1_digest → b"hello"
async fn build_two_level_tree(
    slow_store: &MemoryStore,
) -> Result<(DigestInfo, String), Error> {
    let subdir_name = "shared_parent";

    // Empty sub-directory (leaf)
    let subdir_dir = ProtoDirectory::default();
    let subdir_digest = DigestInfo::new([0x10u8; 32], subdir_dir.encoded_len() as u64);
    slow_store
        .update_oneshot(subdir_digest, subdir_dir.encode_to_vec().into())
        .await
        .err_tip(|| "uploading subdir")?;

    // Root directory with a file + the subdir
    let file_content = b"hello";
    let file_digest = DigestInfo::new([0x20u8; 32], file_content.len() as u64);
    slow_store
        .update_oneshot(file_digest, file_content[..].into())
        .await
        .err_tip(|| "uploading file content")?;

    let root_dir = ProtoDirectory {
        files: vec![FileNode {
            name: "file1.txt".to_string(),
            digest: Some(file_digest.into()),
            is_executable: false,
            node_properties: None,
        }],
        directories: vec![DirectoryNode {
            name: subdir_name.to_string(),
            digest: Some(subdir_digest.into()),
        }],
        ..Default::default()
    };
    let root_digest = DigestInfo::new([0x01u8; 32], root_dir.encoded_len() as u64);
    slow_store
        .update_oneshot(root_digest, root_dir.encode_to_vec().into())
        .await
        .err_tip(|| "uploading root directory")?;

    Ok((root_digest, subdir_name.to_string()))
}

/// T1 (under-action): `download_to_directory` MUST succeed when the shared
/// parent directory was pre-created (as a real directory) by a concurrent
/// `prepare_output_directory` [C].
///
/// This validates the fix: BFS mkdir treats `AlreadyExists` on a pre-existing
/// *directory* as success (not an error).
///
/// Mutation 2026-06-15: remove the AlreadyExists tolerance from the BFS mkdir
/// → MUST red-fail with bespoke message:
/// "BFS mkdir unexpectedly failed with AlreadyExists on pre-created shared parent".
#[nativelink_test]
async fn bfs_mkdir_succeeds_when_dir_pre_created_by_output_prep()
-> Result<(), Box<dyn core::error::Error>> {
    let (fast_store, slow_store, cas_store) = setup_stores().await?;
    let work_dir = make_temp_path("work_dir_bfs_t1");
    fs::create_dir_all(&work_dir).await?;

    let (root_digest, subdir_name) = build_two_level_tree(slow_store.as_ref()).await?;

    // Simulate what prepare_output_directory [C] does: pre-create the directory
    // that [B2]'s BFS will also try to create.
    let shared_parent = format!("{work_dir}/{subdir_name}");
    fs::create_dir(&shared_parent)
        .await
        .err_tip(|| "pre-creating shared_parent")?;

    // The BFS must tolerate the pre-existing directory and succeed.
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(30),
        nativelink_worker::running_actions_manager::download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_digest,
            &work_dir,
            None,
            None,
            None,
        ),
    )
    .await
    .expect("download_to_directory must not hang (30s deadlock detector)");

    assert!(
        result.is_ok(),
        "BFS mkdir unexpectedly failed with AlreadyExists on pre-created shared parent: {result:?}",
    );

    // Verify the file was materialized correctly despite the pre-existing dir.
    let file_path = format!("{work_dir}/file1.txt");
    assert!(
        fs::metadata(&file_path).await.is_ok(),
        "file1.txt was not materialized after BFS tolerated pre-created dir",
    );

    Ok(())
}

/// T1b (direct-use mode precondition): verifies that `fs::symlink` fails when
/// the destination path already exists as a real directory.
///
/// This is the direct-use mode precondition documented in the O5 design:
/// `work_directory` MUST NOT exist as a real directory before
/// `DirectoryCache::get_or_create_direct` creates it as a symlink. If [C]
/// pre-creates `work_directory` before `get_or_create_direct` runs (as would
/// happen in an unsafe overlap), the symlink creation fails with AlreadyExists.
///
/// The O5 commit 2 overlap guard: in direct-use mode, do NOT run [C]
/// concurrently with [B]. Only overlap [C] with [B2] when `!is_direct_use`.
///
/// Mutation: change `assert!(result.is_err(), ...)` to `assert!(result.is_ok(),
/// ...)` → MUST red-fail with "symlink over pre-existing dir should have failed".
#[cfg(target_family = "unix")]
#[nativelink_test]
async fn symlink_fails_when_dest_already_exists_as_directory()
-> Result<(), Box<dyn core::error::Error>> {
    let work_dir = make_temp_path("work_dir_symlink_precond");
    fs::create_dir_all(&work_dir).await?;

    // Simulate [C] pre-creating work_directory as a real directory.
    let work_directory = format!("{work_dir}/action_work");
    fs::create_dir(&work_directory).await?;

    // Simulate get_or_create_direct trying to create a symlink at work_directory.
    let cache_dir = format!("{work_dir}/cache_entry");
    fs::create_dir(&cache_dir).await?;

    let result = fs::symlink(&cache_dir, &work_directory).await;

    assert!(
        result.is_err(),
        "symlink over pre-existing dir should have failed: this documents the direct-use \
         mode precondition — [C] must not create work_directory before get_or_create_direct \
         runs. O5 commit 2 guard: only overlap [C] with [B2] when !is_direct_use.",
    );

    Ok(())
}

/// T2 (over-action): `download_to_directory` MUST fail when a *file* (not a
/// directory) occupies the path where the BFS tries to create a directory.
///
/// An existing file at a directory path is a genuine conflict — the BFS must
/// NOT silently succeed by ignoring any AlreadyExists.
///
/// Mutation 2026-06-15: widen tolerance to ignore AlreadyExists even when the
/// existing entry is a file → MUST red-fail with bespoke message:
/// "BFS mkdir should have failed when a file occupies a directory path".
#[nativelink_test]
async fn bfs_mkdir_fails_when_file_pre_exists_at_dir_path()
-> Result<(), Box<dyn core::error::Error>> {
    let (fast_store, slow_store, cas_store) = setup_stores().await?;
    let work_dir = make_temp_path("work_dir_bfs_t2");
    fs::create_dir_all(&work_dir).await?;

    let (root_digest, subdir_name) = build_two_level_tree(slow_store.as_ref()).await?;

    // Place a FILE at the path where BFS expects to create a directory.
    // This is a genuine conflict (different from the [C] pre-create case).
    let blocking_file_path = format!("{work_dir}/{subdir_name}");
    tokio::fs::File::create(&blocking_file_path)
        .await
        .err_tip(|| "creating blocking file")?;

    let result = tokio::time::timeout(
        core::time::Duration::from_secs(30),
        nativelink_worker::running_actions_manager::download_to_directory(
            cas_store.as_ref(),
            fast_store.as_pin(),
            &root_digest,
            &work_dir,
            None,
            None,
            None,
        ),
    )
    .await
    .expect("download_to_directory must not hang");

    assert!(
        result.is_err(),
        "BFS mkdir should have failed when a file occupies a directory path, but returned Ok",
    );

    Ok(())
}
