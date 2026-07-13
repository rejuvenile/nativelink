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

//! (#mapgap) Production-composition test for the worker input-materialization
//! FALSE-MISSING probe wired into `download_to_directory`.
//!
//! ## Gap being measured
//!
//! When the SERVER supplies `missing_digests` hints (derived from the
//! routing `blob_locality_map`), `download_to_directory` TRUSTS them and
//! fetches them via `populate_fast_store_unchecked`. If a hinted-missing
//! digest is actually a MIRROR replica the worker already holds in its
//! in-memory `mirror_blobs` buffer (a ≥2-replica durability copy the map
//! is blind to — mirror writes skip disk + the tracker), that is THE
//! held-but-unreported mirror gap.
//!
//! ## Invariant under test
//!
//! Exercising `download_to_directory` with `server_missing_digests`
//! containing a MIRROR-ONLY blob MUST increment
//! `input_server_missing_hit_mirror_count` (the mirror-gap counter) — the
//! probe runs at the real call site, gated on the server-hints branch.
//!
//! ## Seam crossed
//!
//! Real worker composition: `FastSlowStore` with a `FilesystemStore` fast
//! tier (shared as the hardlink source, matching production) + an empty
//! slow store + a populated `mirror_blobs` buffer, driven through the
//! actual `download_to_directory` entry point (mirrors
//! `download_to_directory_mirror_test.rs`). The counter is read via the
//! store's public accessor after the call.
//!
//! ## Mutation step (CLAUDE.md mandatory)
//!
//! In `running_actions_manager.rs::download_to_directory`, comment out the
//! `cas_store.probe_input_missing_sources(...)` call (the whole
//! `if server_missing_digests.is_some() && ...` block). This test MUST
//! red-fail with its bespoke "#mapgap: ... probe did not fire" message —
//! the mirror-gap counter stays 0.

use std::collections::{HashMap, HashSet};
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
use nativelink_util::metrics_publisher::{MetricsRegistry, render_prometheus};
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreLike};
use nativelink_worker::running_actions_manager::download_to_directory;
use tempfile::TempDir;

fn make_temp_dir(suffix: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("nl_mapgap_probe_{suffix}_"))
        .tempdir()
        .expect("tempdir")
}

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = make_temp_dir("content");
    let temp_dir = make_temp_dir("temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy::default()),
        ..Default::default()
    })
    .await
    .expect("create filesystem store");
    (store, content_dir, temp_dir)
}

fn make_fss(fast: Store, slow: Store) -> Arc<FastSlowStore> {
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
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

/// (#mapgap) A server-flagged-missing blob that the worker actually holds
/// ONLY in its in-memory mirror buffer must bump
/// `input_server_missing_hit_mirror_count` when materialized through
/// `download_to_directory` with server hints.
#[nativelink_test]
async fn server_missing_mirror_only_blob_bumps_mirror_gap_counter() {
    let (shared_fs, _content_dir, _temp_dir) = make_filesystem_store().await;
    let empty_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cas_store = make_fss(Store::new(shared_fs.clone()), empty_slow);

    // Blob lives in mirror_blobs ONLY (not on disk, not in slow store).
    let blob_bytes = Bytes::from_static(b"mirror_gap_probe_payload");
    let blob_digest = digest_of_bytes(&blob_bytes);
    write_mirror(&cas_store, blob_digest, blob_bytes.clone()).await;
    assert_eq!(
        cas_store.mirror_blob_count(),
        1,
        "test setup: blob should be in mirror_blobs only"
    );

    // One-file Directory referencing the mirror-only blob.
    let root_dir = ProtoDirectory {
        files: vec![FileNode {
            name: "mirror_file.bin".to_string(),
            digest: Some(ProtoDigest::from(blob_digest)),
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
    let mut pre_resolved = HashMap::new();
    pre_resolved.insert(root_dir_digest, root_dir);

    // THE server-missing hints: flag the mirror-only blob as "missing".
    let mut server_missing = HashSet::new();
    server_missing.insert(blob_digest);

    let work_dir_handle = make_temp_dir("work");
    let work_dir = work_dir_handle.path().to_string_lossy().into_owned();

    let fs_pin: Pin<&FilesystemStore> = Pin::new(shared_fs.as_ref());
    download_to_directory(
        cas_store.as_ref(),
        fs_pin,
        &root_dir_digest,
        &work_dir,
        Some(pre_resolved),
        Some(server_missing),
        None,
    )
    .await
    .expect("download_to_directory must succeed for mirror-only blob");

    // The probe must have fired and classified the blob as a MIRROR hit.
    let registry = MetricsRegistry::new();
    registry.register("nativelink.WORKER_FAST_SLOW_STORE", cas_store.clone());
    let _warm = render_prometheus(&registry);
    let body = render_prometheus(&registry);

    assert!(
        body.contains(
            "\nnativelink_WORKER_FAST_SLOW_STORE_input_server_missing_hit_mirror_count 1\n"
        ),
        "#mapgap: the download_to_directory false-missing probe did not fire \
         (or misclassified) — the mirror-gap counter is not 1. A \
         server-flagged-missing blob the worker held in the mirror buffer is \
         THE held-but-unreported mirror gap; if this is 0 the smoking-gun \
         signal is invisible in production. body=\n{body}"
    );
    // The file still materialized correctly (behavior unchanged by the probe).
    let dest = format!("{work_dir}/mirror_file.bin");
    let on_disk = tokio::fs::read(&dest)
        .await
        .expect("expected mirror_file.bin to exist on disk");
    assert_eq!(
        on_disk,
        blob_bytes.as_ref(),
        "materialized file bytes must match the mirror copy (probe must not \
         alter materialization behavior)"
    );
}
