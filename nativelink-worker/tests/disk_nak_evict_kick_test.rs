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
//! reaches.
//!
//! Mutation guidance (bespoke messages each mutation must re-trip):
//!   * Comment the `s.fs_store.kick_eviction_drain()` call in
//!     `kick_fs_eviction_on_disk_nak` (return `false`) →
//!     `disk_nak_kick_reaches_real_filesystem_store_map` MUST fail with
//!     "first disk-NAK kick must reach the store's eviction map".
//!   * Remove the rate-limit in `MokaEvictingMap::kick_drain` → the
//!     second-kick assertion MUST fail with "immediate second disk-NAK
//!     kick must be rate-limited".

use std::sync::Arc;

use nativelink_config::stores::{EvictionPolicy, FilesystemSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
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
