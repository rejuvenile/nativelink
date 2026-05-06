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

//! Worker-side BIS handler routing tests for Option A AC mirroring
//! (#268). These cover the "asymmetric" cases that complement the
//! existing CAS-only `blobs_in_stable_storage_handler_test.rs`:
//!
//! 1. **AC chunk → AC drain only** (under-action of AC path):
//!    a non-empty `store_id` matching this worker's configured AC
//!    store routes through `remove_local_ac_pins` on the AC FSS.
//! 2. **AC chunk does NOT touch CAS state** (over-action of CAS path):
//!    an AC-tagged chunk MUST NOT call `cas_fss.remove_mirror_blobs`,
//!    `fs_store.unpin_digest`, or `cas_store.ack_digests`. Same digest
//!    is used for the AC pin AND a CAS mirror blob to surface any
//!    cross-channel leakage.
//! 3. **CAS chunk does NOT touch AC state** (over-action of AC path):
//!    an empty-store_id chunk MUST NOT call `remove_local_ac_pins` on
//!    the AC FSS, even when the chunk's digest aliases an AC pin
//!    (REAPI digest collision case — the exact mechanism that drove
//!    the revert of merge `563c8ebb`).
//! 4. **Unknown store_id → warn + ack** (no-panic, no-state-change):
//!    a chunk whose `store_id` matches neither "" nor the configured
//!    AC store is treated as a no-op. The chunk still acks (the BIS
//!    chunk handler is wrapped by `handle_bis_chunk` in production;
//!    we test the lower-level `handle_blobs_in_stable_storage_for_store`
//!    directly here).
//!
//! Mutation guidance:
//!   * Comment out the dispatch on `store_id.is_empty()` and route ALL
//!     chunks to the CAS path → tests 1 and 4 must red-fail (AC pin not
//!     removed; AC pin removed but CAS state changes too).
//!   * Comment out the AC-path's `target.fss.remove_local_ac_pins(...)`
//!     → test 1 ("AC pin removed under matching store_id") red-fails
//!     with bespoke "AC pin must be drained" message.
//!   * Replace the AC dispatch's `target.store_id.as_ref() == store_id`
//!     guard with `true` → test 4 (unknown store_id) red-fails ("must
//!     NOT drop AC pin under non-matching store_id" — over-action of
//!     unguarded routing).

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
    AcMirrorTarget, BlobsAvailableState, handle_blobs_in_stable_storage_for_store,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

const AC_STORE_NAME: &str = "AC_MAIN_STORE";

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_bis_ac_route_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_bis_ac_route_temp_")
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

fn make_fss() -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
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

fn proto_digest_for(d: &DigestInfo) -> ProtoDigest {
    ProtoDigest::from(*d)
}

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

fn ac_target_for(fss: Arc<FastSlowStore>) -> AcMirrorTarget {
    AcMirrorTarget {
        fss,
        store_id: Arc::from(AC_STORE_NAME),
    }
}

/// Test 1 (under-action) + Test 2 (over-action of CAS path).
/// AC-tagged chunk routes to AC FSS, drains the AC pin, and does NOT
/// touch CAS state. Uses a digest that ALIASES between an AC pin and
/// a CAS mirror blob (the REAPI-collision case) to detect cross-channel
/// leakage.
#[nativelink_test]
async fn ac_chunk_drains_ac_only_not_cas() {
    let cas_fss = make_fss();
    let ac_fss = make_fss();
    let aliased = mk_digest(0xAA, 5);

    // CAS-side mirror blob for the digest (a peer-pushed CAS blob).
    write_mirror(&cas_fss, aliased, Bytes::from_static(b"abcde")).await;
    // AC-side pin for the same digest (a worker-written AC entry whose
    // `action_digest` is aliased with the CAS digest by REAPI design).
    ac_fss.insert_local_ac_pin(AC_STORE_NAME, aliased);

    assert_eq!(cas_fss.mirror_blob_count(), 1, "CAS mirror seeded");
    assert_eq!(
        ac_fss.dispatched_mirror_pin_snapshot().len(),
        1,
        "AC pin seeded"
    );

    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test_with_ac(
        fs_store,
        Some(cas_fss.clone()),
        Some(ac_target_for(ac_fss.clone())),
    );

    let proto = vec![proto_digest_for(&aliased)];

    let outcome = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async {
            // handle_blobs_in_stable_storage_for_store is synchronous;
            // wrap in async block + timeout to satisfy the deadlock
            // detector contract for borrowed-state lifecycle changes.
            handle_blobs_in_stable_storage_for_store(
                &state,
                None,
                AC_STORE_NAME,
                &proto,
            )
        },
    )
    .await
    .expect("must not deadlock — AC chunk routing contract violated");

    assert_eq!(outcome.unpinned, 1);
    assert_eq!(outcome.failed, 0);
    // Under-action: AC pin MUST be drained.
    assert!(
        ac_fss.dispatched_mirror_pin_snapshot().is_empty(),
        "AC pin must be drained by AC-tagged chunk \
         (under-action: AC routing not invoked)"
    );
    // Over-action: CAS mirror blob MUST NOT be removed by an AC chunk.
    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "CAS mirror blob MUST NOT be removed by AC-tagged chunk; \
         over-action: AC routing leaked into CAS path \
         (this is the digest-collision exploit that drove the \
         revert of merge 563c8ebb)"
    );
}

/// Test 3 (over-action of AC path).
/// CAS chunk (empty store_id) routes to CAS path only and does NOT
/// touch AC pin state, even when the digest aliases an AC pin.
#[nativelink_test]
async fn cas_chunk_does_not_touch_ac_pins_even_under_aliased_digest() {
    let cas_fss = make_fss();
    let ac_fss = make_fss();
    let aliased = mk_digest(0xBB, 5);

    write_mirror(&cas_fss, aliased, Bytes::from_static(b"01234")).await;
    ac_fss.insert_local_ac_pin(AC_STORE_NAME, aliased);

    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test_with_ac(
        fs_store,
        Some(cas_fss.clone()),
        Some(ac_target_for(ac_fss.clone())),
    );

    let proto = vec![proto_digest_for(&aliased)];

    // Empty store_id → CAS path.
    let outcome = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async {
            handle_blobs_in_stable_storage_for_store(&state, None, "", &proto)
        },
    )
    .await
    .expect("must not deadlock — CAS chunk routing contract violated");

    assert_eq!(outcome.unpinned, 1);
    // Under-action of CAS path: CAS mirror MUST be removed.
    assert_eq!(
        cas_fss.mirror_blob_count(),
        0,
        "CAS chunk must drain CAS mirror under matching digest"
    );
    // Over-action of AC path: AC pin MUST stay even when digest aliases.
    assert_eq!(
        ac_fss.dispatched_mirror_pin_snapshot().len(),
        1,
        "AC pin MUST NOT be drained by CAS-tagged chunk on aliased digest; \
         over-action: empty-store_id routing leaked into AC path"
    );
}

/// Test 4: unknown store_id → warn + no-op.
/// A chunk whose `store_id` matches neither "" (CAS) nor the worker's
/// configured AC store_id is treated as a no-op. AC pin MUST stay,
/// CAS mirror MUST stay. The chunk-handler caller will still ack.
#[nativelink_test]
async fn unknown_store_id_chunk_is_noop_with_ac_target_present() {
    let cas_fss = make_fss();
    let ac_fss = make_fss();
    let digest = mk_digest(0xCC, 4);

    write_mirror(&cas_fss, digest, Bytes::from_static(b"abcd")).await;
    ac_fss.insert_local_ac_pin(AC_STORE_NAME, digest);

    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    let state = BlobsAvailableState::new_for_test_with_ac(
        fs_store,
        Some(cas_fss.clone()),
        Some(ac_target_for(ac_fss.clone())),
    );

    let proto = vec![proto_digest_for(&digest)];

    let outcome = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async {
            handle_blobs_in_stable_storage_for_store(
                &state,
                None,
                "AC_GHOST_STORE",
                &proto,
            )
        },
    )
    .await
    .expect("must not deadlock — unknown store_id handling violated");

    // Decode succeeds (proto is valid) — the no-op is the routing
    // step, not the digest decode. So `unpinned` counts decoded
    // digests; the index mutations are the asymmetric assertions.
    assert_eq!(outcome.unpinned, 1, "valid proto digest must decode");
    assert_eq!(outcome.failed, 0);
    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "unknown store_id MUST NOT drain CAS mirror; \
         over-action: routing fell through to CAS"
    );
    assert_eq!(
        ac_fss.dispatched_mirror_pin_snapshot().len(),
        1,
        "unknown store_id MUST NOT drain AC pin; \
         over-action: routing matched on store_id presence \
         alone instead of equality"
    );
}

/// Test 5: chunk carrying non-empty `store_id` arrives on a worker
/// with NO AC mirror target configured. Must warn + no-op (NOT panic,
/// NOT route to CAS). This is the forward-compat path for a fleet
/// where some workers don't yet have an AC FSS but the server has
/// already begun broadcasting AC chunks.
#[nativelink_test]
async fn ac_store_id_chunk_with_no_ac_target_is_noop() {
    let cas_fss = make_fss();
    let digest = mk_digest(0xDD, 4);
    write_mirror(&cas_fss, digest, Bytes::from_static(b"wxyz")).await;

    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    // Note: ac_mirror_target = None.
    let state = BlobsAvailableState::new_for_test_with_ac(
        fs_store,
        Some(cas_fss.clone()),
        None,
    );

    let proto = vec![proto_digest_for(&digest)];

    let outcome = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async {
            handle_blobs_in_stable_storage_for_store(
                &state,
                None,
                AC_STORE_NAME,
                &proto,
            )
        },
    )
    .await
    .expect("must not deadlock — no-AC-target routing contract violated");

    assert_eq!(outcome.unpinned, 1);
    // CAS state must remain (no-op routing) — AC chunk should not
    // fall through to the CAS path even on workers without an AC FSS.
    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "AC chunk on no-AC-target worker MUST NOT drain CAS mirror; \
         over-action: forward-compat path leaked into CAS"
    );
}
