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

//! (FINDING 2 moka-eviction-wedge piece 3) Disk-NAK → eviction-drain kick
//! seam tests.
//!
//! The worker's disk-pressure NAK site (the `DiskGateDecision::Nak` arm in
//! `LocalWorker::run`) calls `kick_fs_eviction_on_disk_nak` so the
//! admission gate actively drives eviction (`gate ⇒ evict`,
//! admission-eviction-pin composite) instead of only refusing work on the
//! premise "eviction catches up" — a premise FINDING 2 falsified (moka
//! stale-probation-front livelock: eviction permanently dead at 3.3× over
//! budget for 4 days while the gate NAK'd 3K actions).
//!
//! Production-composition: drives the EXACT production helper against a
//! real tempdir-backed `FilesystemStore` inside a real
//! `BlobsAvailableState` — the kick crosses the
//! `FilesystemStore::kick_eviction_drain` → `MokaEvictingMap::kick_drain`
//! seam, so the rate-limit verdicts asserted here can only come from the
//! real map's limiter. The kick-wakes-the-drain-arm behavior itself is
//! proven at the map level (`drain_kick_wakes_drain_arm_without_tick` in
//! `moka_evicting_map.rs`), against the same `kick_drain` this test
//! reaches. `wedge_selfheal_deletes_real_file_from_disk` (pair-b T5)
//! closes the remaining composition half: the heal against a REAL
//! `FileEntryImpl` store, asserting both the index (`has`) and the
//! on-disk file removal.
//!
//! Mutation guidance (bespoke messages each mutation must re-trip):
//!   * Comment the `s.fs_store.kick_eviction_drain()` call in
//!     `kick_fs_eviction_on_disk_nak` (return `false`) →
//!     `disk_nak_kick_reaches_real_filesystem_store_map` MUST fail with
//!     "first disk-NAK kick must reach the store's eviction map".
//!   * Remove the rate-limit in `MokaEvictingMap::kick_drain` → the
//!     second-kick assertion MUST fail with "immediate second disk-NAK
//!     kick must be rate-limited".

use std::path::Path;
use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::moka_evicting_map::WedgeSelfHealOutcome;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use nativelink_worker::local_worker::{
    BlobsAvailableState, BlobsAvailableTestArgs, kick_fs_eviction_on_disk_nak,
};
use tempfile::TempDir;

async fn make_filesystem_store() -> (Arc<FilesystemStore>, TempDir, TempDir) {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_nak_kick_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_nak_kick_temp_")
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

/// The disk-NAK kick helper must (1) reach the real store's eviction map
/// through the `FilesystemStore::kick_eviction_drain` passthrough (first
/// kick accepted), (2) be rate-limited on an immediate repeat (a NAK storm
/// must not become a busy drain loop), and (3) be a safe no-op on a worker
/// with no FilesystemStore fast tier.
#[nativelink_test]
async fn disk_nak_kick_reaches_real_filesystem_store_map() -> Result<(), nativelink_error::Error> {
    let (fs_store, _content_dir, _temp_dir) = make_filesystem_store().await;
    let state = BlobsAvailableState::from_test_args(fs_store, BlobsAvailableTestArgs::default());

    assert!(
        kick_fs_eviction_on_disk_nak(Some(&state)),
        "gate => evict: the first disk-NAK kick must reach the store's eviction map \
         (FilesystemStore::kick_eviction_drain -> MokaEvictingMap::kick_drain) and be \
         accepted — without it the disk gate only refuses work while a wedged evictor \
         never catches up (FINDING 2)"
    );
    assert!(
        !kick_fs_eviction_on_disk_nak(Some(&state)),
        "an immediate second disk-NAK kick must be rate-limited by the real map's \
         limiter (once per few seconds) — a NAK storm must not become a busy drain loop"
    );
    assert!(
        !kick_fs_eviction_on_disk_nak(None),
        "a worker with no FilesystemStore fast tier must treat the kick as a safe no-op"
    );
    Ok(())
}

/// Recursively count regular files under `dir` (the FilesystemStore
/// content layout may shard into subdirectories).
fn count_files_recursive(dir: &Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_files_recursive(&path);
            } else {
                count += 1;
            }
        }
    }
    count
}

/// (review 9fd52fc0 pair-b T5) The `gate ⇒ evict` composite crossed
/// end-to-end with a REAL `FileEntryImpl`: the wedge self-heal must
/// remove an actual on-disk file through the NORMAL eviction pipeline
/// (listener → event → `unref` [file unlink] + index update), with the
/// removal observed through the store's own index primitive
/// (`has`, per the index-visibility contract) AND on disk. The
/// map-level heal tests prove the pipeline for `BytesEntry`, whose
/// `unref` is a no-op — this is the file-deleting half.
///
/// Mutation step: comment out `drain_pending_evictions().await` in the
/// heal loop (`maybe_selfheal_wedged_eviction`) → the invalidations
/// stay queued, `unref` never runs, and the on-disk assertion red-fails
/// with the bespoke "file must be GONE from disk" message.
#[nativelink_test]
async fn wedge_selfheal_deletes_real_file_from_disk() -> Result<(), nativelink_error::Error> {
    let content_dir = tempfile::Builder::new()
        .prefix("nl_wedge_heal_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_wedge_heal_temp_")
        .tempdir()
        .expect("tempdir");
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 100 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await
    .expect("create filesystem store");

    // One real 4 KiB blob → one real file in the content dir.
    let digest = DigestInfo::new([0x42u8; 32], 4096);
    let store = Store::new(fs_store.clone());
    store
        .update_oneshot(digest, vec![0x42u8; 4096].into())
        .await?;
    assert_eq!(
        count_files_recursive(content_dir.path()),
        1,
        "fixture: the 4 KiB blob must exist as exactly one file on disk before the heal"
    );
    assert!(
        store.has(StoreKey::from(digest)).await?.is_some(),
        "fixture: the store index must see the blob before the heal"
    );

    // Inflate the wedge observation strictly over the 100 KiB budget and
    // drive the trigger to firing (2 frozen evaluations + the firing) —
    // the same evaluation the production drain arms run.
    fs_store.test_inflate_wedge_observation(200 * 1024);
    let mut fired = None;
    for _ in 0..3 {
        fired = Some(fs_store.test_maybe_selfheal_wedged_eviction().await);
    }
    assert!(
        matches!(fired, Some(WedgeSelfHealOutcome::Fired { evicted_count: 1, .. })),
        "the heal must fire and evict the single unpinned blob (got {fired:?})"
    );

    // Index-visibility: the store's own lookup primitive must observe
    // the eviction (never substitute fs metadata for this half).
    let has_after = tokio::time::timeout(core::time::Duration::from_secs(5), async {
        store.has(StoreKey::from(digest)).await
    })
    .await
    .expect("must not deadlock — index lookup after wedge heal")?;
    assert!(
        has_after.is_none(),
        "index-visibility: after the wedge heal the store index must report the \
         evicted blob ABSENT (gate => evict composite, index half)"
    );

    // Disk half (the T5 gap): the eviction pipeline's `unref` must have
    // DELETED the real file — an index-only removal would leak disk
    // bytes forever, which is the exact resource the disk-NAK gate is
    // protecting.
    assert_eq!(
        count_files_recursive(content_dir.path()),
        0,
        "gate => evict composite, disk half: the healed blob's backing file must be \
         GONE from disk (FileEntryImpl::unref unlink through the normal eviction \
         pipeline) — an index-only removal leaks the very disk bytes the NAK gate \
         protects"
    );
    Ok(())
}
