// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! Integration test (review #6): a worker FastSlowStore with an empty
//! slow store but a populated `mirror_blobs` map must be able to
//! materialize a mirror-only blob during `download_to_directory`. Pre-fix
//! the directory_cache and ram has-checks were fast-store-only AND
//! `populate_fast_store_unchecked` did not consult `mirror_blobs`, so a
//! mirror-only blob was re-fetched from the slow store and failed with
//! NotFound when the slow store was empty (server down or had lost the
//! blob).
//!
//! Mutate-test guidance: revert the `materialize_mirror_to_fast` call
//! inside `populate_fast_store_unchecked` (replace with `Ok(false)`); the
//! `mirror_only_blob_materialized_via_download_to_directory` test must
//! fail with NotFound on the populate step.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Digest as ProtoDigest, Directory as ProtoDirectory, FileNode,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreLike};
use nativelink_worker::running_actions_manager::download_to_directory;
use pretty_assertions::assert_eq;

fn temp_path(suffix: &str) -> String {
    // Use `tempfile::Builder` for race-free unique-name generation; `.keep()`
    // disarms the auto-cleanup so the FilesystemStore (which lives past the
    // test body inside Arcs) doesn't see its content_path vanish mid-run.
    // Tradeoff: tmp files leak; the OS reclaims them on next /tmp sweep.
    tempfile::Builder::new()
        .prefix(&format!("nl_dl_to_dir_{suffix}_"))
        .tempdir()
        .expect("tempdir")
        .keep()
        .to_string_lossy()
        .into_owned()
}

async fn make_filesystem_store() -> Arc<FilesystemStore> {
    let content = temp_path("content");
    let temp = temp_path("temp");
    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content,
        temp_path: temp,
        eviction_policy: Some(EvictionPolicy::default()),
        ..Default::default()
    })
    .await
    .expect("create filesystem store")
}

fn make_fss(
    fast: Store,
    slow: Store,
) -> Arc<FastSlowStore> {
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast,
        slow,
    )
}

async fn write_mirror(fss: &Arc<FastSlowStore>, digest: DigestInfo, data: Bytes) {
    let store = Store::new(fss.clone());
    IS_MIRROR_REQUEST
        .scope(true, async move {
            store
                .update_oneshot(digest, data)
                .await
                .expect("mirror write");
        })
        .await;
}

fn digest_of_bytes(bytes: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}

/// Mirror-only blob: present in `mirror_blobs`, absent from slow store.
/// Must materialize successfully during `download_to_directory` and end
/// up as a real on-disk file in the work directory.
#[nativelink_test]
async fn mirror_only_blob_materialized_via_download_to_directory() {
    // Single FilesystemStore shared as the worker's fast tier AND the
    // hardlink source. This matches production wiring: in
    // `nativelink::main`, `filesystem_store` is the same Arc as
    // `cas_store.fast_store()`. Using two separate stores would write
    // to one and read from the other and the hardlink would fail
    // independent of the mirror logic under test.
    let shared_fs = make_filesystem_store().await;

    // Empty slow store — represents "server is down" or "server has
    // lost the blob". Any populate request for the test blob MUST fail
    // unless the FastSlowStore consults `mirror_blobs` first.
    let empty_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cas_store = make_fss(Store::new(shared_fs.clone()), empty_slow);

    // Insert the test blob into mirror_blobs ONLY.
    let blob_bytes = Bytes::from_static(b"mirror_only_payload_42");
    let blob_digest = digest_of_bytes(&blob_bytes);
    write_mirror(&cas_store, blob_digest, blob_bytes.clone()).await;
    assert_eq!(
        cas_store.mirror_blob_count(),
        1,
        "test setup: blob should be in mirror_blobs only"
    );

    // Build a one-file Directory whose only entry references the
    // mirror-only blob.
    let proto_blob_digest = ProtoDigest::from(blob_digest);
    let root_dir = ProtoDirectory {
        files: vec![FileNode {
            name: "mirror_file.bin".to_string(),
            digest: Some(proto_blob_digest),
            is_executable: false,
            node_properties: None,
        }],
        ..Default::default()
    };
    let root_dir_bytes: Vec<u8> = {
        use prost::Message;
        let mut buf = Vec::new();
        root_dir.encode(&mut buf).unwrap();
        buf
    };
    let root_dir_digest = digest_of_bytes(&root_dir_bytes);

    // Pre-resolved tree skips the GetTree RPC; we hand the directory in
    // directly. The blob digest is what download_to_directory will try
    // to populate via `populate_fast_store_unchecked`.
    let mut pre_resolved = HashMap::new();
    pre_resolved.insert(root_dir_digest, root_dir);

    let work_dir = temp_path("work");
    tokio::fs::create_dir_all(&work_dir).await.expect("create work dir");

    let fs_pin: Pin<&FilesystemStore> = Pin::new(shared_fs.as_ref());
    download_to_directory(
        cas_store.as_ref(),
        fs_pin,
        &root_dir_digest,
        &work_dir,
        Some(pre_resolved),
        None,
    )
    .await
    .expect("download_to_directory must succeed for mirror-only blob");

    // Assert the file exists and its bytes match.
    let dest = format!("{work_dir}/mirror_file.bin");
    let on_disk = tokio::fs::read(&dest)
        .await
        .expect("expected mirror_file.bin to exist on disk");
    assert_eq!(
        on_disk,
        blob_bytes.as_ref(),
        "materialized file bytes must match the mirror copy"
    );

    // Cleanup.
    tokio::fs::remove_dir_all(&work_dir).await.ok();
}
