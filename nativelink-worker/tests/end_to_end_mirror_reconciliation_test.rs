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

//! End-to-end mirror reconciliation test (review #6).
//!
//! Walks the headline server-restart-resilience scenario at the worker
//! boundary:
//!
//!   1. Worker holds digest D pinned in `mirror_blobs` (pushed by the
//!      server via `x-nativelink-mirror`); the slow store does NOT have
//!      D (modeling "server lost the blob during restart").
//!   2. The worker's `pinned_mirror_digests` snapshot must include D —
//!      this is what the worker advertises on the next BlobsAvailable
//!      after reconnect.
//!   3. Server discovers D missing → sends `UploadMissingBlobs`. We
//!      simulate this by reading D through the FastSlowStore wrapper
//!      (the production path in `handle_upload_missing_blobs`) and
//!      writing it to the slow store.
//!   4. Slow store now durably holds D.
//!   5. Server sends `BlobsInStableStorage(D)` → worker invokes
//!      `handle_blobs_in_stable_storage`, which calls
//!      `remove_mirror_blobs(&[D])`.
//!   6. Worker's mirror pin for D is dropped; subsequent
//!      `pinned_mirror_digests` snapshots do NOT include D.
//!
//! Scope limitation: a fully-wired test would stand up a fake
//! `WorkerApiServer` + `LocalWorkerImpl` connected by an in-memory
//! gRPC channel. That harness lives in
//! `nativelink-service/tests/worker_api_server_test.rs` and would
//! require non-trivial fixtures (`MockWorkerStateManager`,
//! `ApiWorkerScheduler`, etc.). The test below scopes down to the
//! contract that the parent agents touch:
//!
//!     [worker FSS state, BlobsAvailable inputs] ⇒
//!     [server-side request], then
//!     [BlobsInStableStorage proto] ⇒ [worker FSS state]
//!
//! and pins the round-trip at the data-flow seams the dispatch arms in
//! `WorkerApiServer::handle_blobs_available` and
//! `LocalWorkerImpl::run` cross. The full transport-layer E2E remains
//! deferred (see TODO in the test body).

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
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreKey, StoreLike};
use nativelink_worker::local_worker::{
    BlobsAvailableState, BlobsAvailableTestArgs, handle_blobs_in_stable_storage,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

/// Returns the FilesystemStore plus the two `TempDir` handles backing its
/// `content_path` and `temp_path`. The caller MUST bind both `TempDir`s
/// to locals so they outlive every Arc'd reference to the store —
/// otherwise the on-disk dirs leak (same anti-pattern as the mongo_runner
/// `.keep()` leak fixed in commit 086d0d31).
async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_e2e_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_e2e_temp_")
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
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
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

fn mk_digest(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Headline scenario: worker survives a server restart and reconciles a
/// mirror-only blob back to durable storage.
#[nativelink_test]
async fn server_restart_reconciliation_full_flow() {
    let blob_payload = Bytes::from_static(b"reconcile_me_please");
    let blob = mk_digest(7, blob_payload.len());

    // -------- Step 1: worker holds blob pinned in mirror_blobs ------
    // Slow store is empty (the server "lost" the blob).
    let cas_fss = make_fss_for_mirror();
    write_mirror(&cas_fss, blob, blob_payload.clone()).await;
    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "step 1: blob pinned in mirror_blobs"
    );
    let slow_has = cas_fss
        .slow_store()
        .has(StoreKey::from(blob))
        .await
        .expect("slow has");
    assert_eq!(
        slow_has, None,
        "step 1: slow store must be empty (server lost the blob)"
    );

    // -------- Step 2: pinned digest visible in advertise snapshot ----
    let pinned_advertised = cas_fss.mirror_blob_digests();
    assert!(
        pinned_advertised.contains(&blob),
        "step 2: blob must appear in pinned_mirror_digests snapshot \
         (this is what the worker sends to the server on reconnect)"
    );

    // -------- Step 3: server requests UploadMissingBlobs -------------
    // Production code: `LocalWorkerImpl::handle_upload_missing_blobs`
    // reads through the FastSlowStore wrapper so mirror_blobs entries
    // are visible, then writes to slow_store. We replicate that exact
    // sequence here without the LocalWorkerImpl scaffolding.
    let cas_store_wrapped = Store::new(cas_fss.clone());
    let read_back = cas_store_wrapped
        .get_part_unchunked(blob, 0, None)
        .await
        .expect("step 3: must read mirror-only blob via wrapper");
    assert_eq!(
        read_back, blob_payload,
        "step 3: bytes from wrapper must match mirror copy"
    );
    cas_fss
        .slow_store()
        .update_oneshot(blob, read_back)
        .await
        .expect("step 3: upload to slow store");

    // -------- Step 4: slow store now has the blob durably ------------
    let slow_has_after = cas_fss
        .slow_store()
        .has(StoreKey::from(blob))
        .await
        .expect("slow has");
    assert_eq!(
        slow_has_after,
        Some(blob_payload.len() as u64),
        "step 4: slow store must hold the blob durably"
    );

    // -------- Step 5: server sends BlobsInStableStorage --------------
    // Production code: the dispatch arm calls
    // `handle_blobs_in_stable_storage(state, cas_store, &proto)`.
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(
        fs_store,
        BlobsAvailableTestArgs {
            cas_server_fss: Some(cas_fss.clone()),
            ..Default::default()
        },
    );
    let proto = vec![ProtoDigest::from(blob)];
    handle_blobs_in_stable_storage(&state, None, &proto);

    // -------- Step 6: pin dropped --------------------------------
    assert_eq!(
        cas_fss.mirror_blob_count(),
        0,
        "step 6: pin must be dropped after BlobsInStableStorage"
    );
    let pinned_after = cas_fss.mirror_blob_digests();
    assert!(
        !pinned_after.contains(&blob),
        "step 6: blob must NOT appear in subsequent pinned_mirror_digests"
    );
    let changes = cas_fss.drain_mirror_changes();
    assert!(
        changes.removed.contains(&blob),
        "step 6: ack must surface as a `removed` delta so the server can \
         clean up its locality entry on the next BlobsAvailable"
    );

    // TODO(deferred): full transport-layer E2E with WorkerApiServer +
    // LocalWorkerImpl + an in-memory gRPC pair is not yet wired here.
    // The two sides are exercised independently in:
    //   - nativelink-service/tests/worker_api_server_test.rs
    //   - nativelink-worker/tests/blobs_in_stable_storage_handler_test.rs
    //   - this test (the data-flow contract)
}

/// Negative path: if the server NEVER sends `BlobsInStableStorage`,
/// the worker's pin survives indefinitely. This pins the
/// "no automatic timer" property — the only valid pin removal is
/// an explicit ack.
#[nativelink_test]
async fn pin_survives_indefinitely_without_ack() {
    let blob_payload = Bytes::from_static(b"unacked");
    let blob = mk_digest(8, blob_payload.len());
    let cas_fss = make_fss_for_mirror();
    write_mirror(&cas_fss, blob, blob_payload.clone()).await;
    assert_eq!(cas_fss.mirror_blob_count(), 1);

    // Drain advertise deltas many times to model many BlobsAvailable
    // ticks across what would be the old TTL (120s) window.
    for _ in 0..50 {
        drop(cas_fss.snapshot_and_reset_mirror_changes());
    }
    assert_eq!(
        cas_fss.mirror_blob_count(),
        1,
        "drain cycles must NOT remove the pin"
    );
}
