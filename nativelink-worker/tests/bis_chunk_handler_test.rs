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

//! Worker-side handler tests for #97 — `Update::ChunkedMessage(
//! BlobsInStableStorageChunk)`.
//!
//! These tests cover:
//!   1. **`bis_chunked_full_burst`**: a single chunk of N digests
//!      causes N unpins on the worker's FilesystemStore AND the worker
//!      emits exactly one `BisAck { broadcast_id, sequence }` matching
//!      the chunk it processed.
//!   2. **`bis_chunked_empty_terminal`**: a terminal chunk with zero
//!      digests still produces an ack (the empty-input case from
//!      `chunk_iter`'s contract — without an ack the server's resend
//!      buffer would never release that broadcast).
//!   3. **`bis_chunked_idempotent_unpin`**: applying the same chunk
//!      twice (simulating a resend after a missed ack) is safe; both
//!      applications produce an ack and neither double-decrements pin
//!      state into negative territory.
//!
//! Production-composition: each test uses a real `FilesystemStore` +
//! `FastSlowStore` with a mirror-blob fixture (mirrors the existing
//! `blobs_in_stable_storage_handler_test.rs` setup), so the chunked
//! path traverses the same code as the legacy single-message arm —
//! plus the ack emission.
//!
//! Mutation guidance:
//!   * Comment out `(ack_sink)(BisAck { ... })` inside
//!     `handle_bis_chunk` — both `bis_chunked_full_burst` and
//!     `bis_chunked_empty_terminal` MUST fail with the
//!     "must emit ack" assertion.
//!   * Comment out the per-digest `fs_store.unpin_digest(&digest)`
//!     inside `handle_blobs_in_stable_storage` — the
//!     `bis_chunked_full_burst` "unpin must drop pin count" assertion
//!     MUST fail.

use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BisAck, BlobsInStableStorageChunk,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreLike};
use nativelink_worker::local_worker::{
    BlobsAvailableState, handle_bis_chunk,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

/// Build a Filesystem store and the two `TempDir`s backing it. Caller
/// MUST keep the `TempDir`s alive for the lifetime of every store
/// reference (avoids the `.keep()` leak documented in the existing
/// blobs_in_stable_storage_handler_test).
async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_bis_chunk_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_bis_chunk_temp_")
        .tempdir()
        .expect("tempdir");
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

/// Insert `n` mirror blobs (4-byte payloads) and return their digests.
async fn populate_mirror_blobs(fss: &Arc<FastSlowStore>, n: usize) -> Vec<DigestInfo> {
    let mut digests = Vec::with_capacity(n);
    for i in 0..n {
        // Distinct SHA256-shaped digest: pack i into the first 8 bytes.
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let digest = DigestInfo::new(hash, 4);
        let payload = Bytes::from(vec![i as u8, (i >> 8) as u8, (i >> 16) as u8, (i >> 24) as u8]);
        IS_MIRROR_REQUEST
            .scope(true, fss.update_oneshot(digest, payload))
            .await
            .expect("mirror update");
        digests.push(digest);
    }
    digests
}

fn proto_from(digests: &[DigestInfo]) -> Vec<ProtoDigest> {
    digests
        .iter()
        .map(|d| ProtoDigest {
            hash: hex::encode(**d.packed_hash()),
            size_bytes: i64::try_from(d.size_bytes()).unwrap_or(0),
        })
        .collect()
}

/// 1. Direct chunk delivery unpins every digest AND emits exactly one ack.
#[nativelink_test]
async fn bis_chunked_full_burst() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let fss = make_fss_for_mirror();
    let digests = populate_mirror_blobs(&fss, 32).await;
    let before = fss.mirror_blob_count();
    assert_eq!(before, 32, "mirror fixture must seat 32 blobs");

    let state = BlobsAvailableState::new_for_test(fs_store.clone(), Some(fss.clone()));

    let chunk = BlobsInStableStorageChunk {
        digests: proto_from(&digests),
        broadcast_id: 7,
        sequence: 0,
        is_last: true,
    };

    let mut acks: Vec<BisAck> = Vec::new();
    let unpinned = handle_bis_chunk(&state, None, &chunk, |a| acks.push(a));

    assert_eq!(
        unpinned, 32,
        "every digest in the chunk must unpin"
    );
    assert_eq!(
        fss.mirror_blob_count(),
        0,
        "mirror blobs must be removed after BIS chunk handling"
    );
    assert_eq!(
        acks.len(),
        1,
        "must emit exactly one ack per chunk processed"
    );
    assert_eq!(acks[0].broadcast_id, 7, "ack must carry the chunk's broadcast_id");
    assert_eq!(acks[0].sequence, 0, "ack must carry the chunk's sequence");
    Ok(())
}

/// 2. The empty terminal chunk (chunk_iter contract) still acks.
#[nativelink_test]
async fn bis_chunked_empty_terminal_acks() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let fss = make_fss_for_mirror();
    let state = BlobsAvailableState::new_for_test(fs_store.clone(), Some(fss.clone()));

    let chunk = BlobsInStableStorageChunk {
        digests: vec![],
        broadcast_id: 42,
        sequence: 5,
        is_last: true,
    };

    let mut acks: Vec<BisAck> = Vec::new();
    let unpinned = handle_bis_chunk(&state, None, &chunk, |a| acks.push(a));

    assert_eq!(unpinned, 0, "no digests means no unpins");
    assert_eq!(
        acks.len(),
        1,
        "empty-terminal chunk must still emit an ack so the server's \
         resend buffer can release the matching slot — without this, a \
         broadcast of zero digests (or one whose final chunk is the \
         empty terminal) would leak into the resend buffer forever"
    );
    assert_eq!(acks[0].broadcast_id, 42);
    assert_eq!(acks[0].sequence, 5);
    Ok(())
}

/// 3. Re-applying the same chunk (e.g. after a server-side resend that
///    crossed an in-flight ack) is safe: the second application sees no
///    additional state to drop, but it MUST still emit an ack so the
///    server's retry path eventually releases the slot.
#[nativelink_test]
async fn bis_chunked_idempotent_unpin() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content, _temp) = make_filesystem_store().await;
    let fss = make_fss_for_mirror();
    let digests = populate_mirror_blobs(&fss, 8).await;
    let state = BlobsAvailableState::new_for_test(fs_store.clone(), Some(fss.clone()));

    let chunk = BlobsInStableStorageChunk {
        digests: proto_from(&digests),
        broadcast_id: 11,
        sequence: 2,
        is_last: false,
    };

    let mut acks: Vec<BisAck> = Vec::new();
    handle_bis_chunk(&state, None, &chunk, |a| acks.push(a));
    assert_eq!(fss.mirror_blob_count(), 0, "first apply removes mirror blobs");

    handle_bis_chunk(&state, None, &chunk, |a| acks.push(a));
    assert_eq!(
        fss.mirror_blob_count(),
        0,
        "second apply must not undo or corrupt mirror state"
    );
    assert_eq!(
        acks.len(),
        2,
        "every chunk delivery emits an ack, even the duplicate"
    );
    assert_eq!(acks[0].broadcast_id, 11);
    assert_eq!(acks[0].sequence, 2);
    assert_eq!(acks[1].broadcast_id, 11);
    assert_eq!(acks[1].sequence, 2);
    Ok(())
}
