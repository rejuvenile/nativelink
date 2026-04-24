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

//! Worker-side handler test for `Update::BlobsInStableStorage` (review #5).
//!
//! Covers the contract that the worker:
//!   1. Converts proto digests into `DigestInfo`,
//!   2. Calls `remove_mirror_blobs` on the CAS server's FastSlowStore so
//!      the in-memory mirror copy is freed, and
//!   3. Leaves unrelated mirror blobs untouched.
//!
//! Mutate-test guidance: comment out the
//! `cas_fss.remove_mirror_blobs(&acked_digests)` call inside
//! `handle_blobs_in_stable_storage`; the
//! `mirror_blob_removed_when_proto_received` test below must fail.

use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreLike};
use nativelink_worker::local_worker::{
    BlobsAvailableState, handle_blobs_in_stable_storage,
};
use pretty_assertions::assert_eq;

fn temp_path(suffix: &str) -> String {
    let dir = std::env::temp_dir();
    let nonce = format!(
        "{}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        suffix,
    );
    dir.join(nonce).to_string_lossy().into_owned()
}

async fn make_filesystem_store() -> Arc<FilesystemStore> {
    let content_path = temp_path("content");
    let temp = temp_path("temp");
    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path: temp,
        eviction_policy: Some(EvictionPolicy::default()),
        ..Default::default()
    })
    .await
    .expect("create filesystem store")
}

fn make_fss_for_mirror() -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
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
            store.update_oneshot(digest, data).await.expect("mirror write");
        })
        .await;
}

fn proto_digest_for(d: &DigestInfo) -> ProtoDigest {
    ProtoDigest::from(*d)
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// The handler must remove the matching mirror blob and leave unrelated
/// pinned mirror blobs in place.
#[nativelink_test]
async fn mirror_blob_removed_when_proto_received() {
    let cas_fss = make_fss_for_mirror();
    let kept_digest = mk_digest(1, 5);
    let acked_digest = mk_digest(2, 5);

    write_mirror(&cas_fss, kept_digest, Bytes::from_static(b"hello")).await;
    write_mirror(&cas_fss, acked_digest, Bytes::from_static(b"world")).await;
    assert_eq!(
        cas_fss.mirror_blob_count(),
        2,
        "both mirror blobs pinned before handler"
    );

    let fs_store = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test(fs_store, Some(cas_fss.clone()));

    // Construct the proto exactly the way the dispatch arm receives it.
    let proto = vec![proto_digest_for(&acked_digest)];

    handle_blobs_in_stable_storage(&state, None, &proto);

    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "acked digest must be removed from mirror_blobs"
    );
    let remaining = cas_fss.mirror_blob_digests();
    assert_eq!(
        remaining,
        vec![kept_digest],
        "only the un-acked digest must remain"
    );
}

/// Invalid (malformed hex) proto digests must NOT crash the handler and
/// must be skipped silently — the rest of the batch must still be acked.
#[nativelink_test]
async fn invalid_proto_digest_does_not_crash_handler() {
    let cas_fss = make_fss_for_mirror();
    let valid_digest = mk_digest(3, 4);
    write_mirror(&cas_fss, valid_digest, Bytes::from_static(b"abcd")).await;

    let fs_store = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test(fs_store, Some(cas_fss.clone()));

    let proto = vec![
        ProtoDigest {
            hash: "not_valid_hex_at_all".into(),
            size_bytes: 7,
        },
        proto_digest_for(&valid_digest),
    ];
    handle_blobs_in_stable_storage(&state, None, &proto);

    assert_eq!(
        cas_fss.mirror_blob_count(),
        0,
        "valid digest must be acked despite malformed sibling"
    );
}

/// When `cas_server_fss` is None, the handler must not panic and must
/// still process the FilesystemStore unpin path. We assert this by
/// passing None and verifying the call returns without panicking.
#[nativelink_test]
async fn handler_no_op_when_cas_server_fss_absent() {
    let fs_store = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test(fs_store, None);
    let digest = mk_digest(4, 4);
    let proto = vec![proto_digest_for(&digest)];
    // Must complete without panic.
    handle_blobs_in_stable_storage(&state, None, &proto);
}
