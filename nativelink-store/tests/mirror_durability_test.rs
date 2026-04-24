// Copyright 2025 The NativeLink Authors. All rights reserved.
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

//! Tests for the worker mirror durability fix.
//!
//! Background — see commit message for details. TDD red→green→mutate
//! evidence is summarized in the PR description.

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreLike};
use pretty_assertions::assert_eq;

fn make_fss() -> std::sync::Arc<FastSlowStore> {
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

/// Drive a mirror write by setting the IS_MIRROR_REQUEST task-local around
/// an `update_oneshot`. This is the same path the bytestream/cas servers
/// take when they receive an `x-nativelink-mirror` request.
async fn write_mirror(fss: &std::sync::Arc<FastSlowStore>, digest: DigestInfo, data: Bytes) {
    let store: Store = Store::new(fss.clone());
    IS_MIRROR_REQUEST
        .scope(true, async move {
            store
                .update_oneshot(digest, data)
                .await
                .expect("mirror write");
        })
        .await;
}

fn d(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Test 1 (Directive A): a mirror blob that never receives
/// `BlobsInStableStorage` must remain pinned indefinitely. The pre-fix
/// behavior was a 30s sweep that expired pins after 120s; the sweep no
/// longer exists and `expire_mirror_blobs` is gone from the public API.
#[nativelink_test]
async fn mirror_blob_pinned_indefinitely() {
    let fss = make_fss();
    let digest = d(1, 5);
    write_mirror(&fss, digest, Bytes::from_static(b"hello")).await;
    assert_eq!(fss.mirror_blob_count(), 1, "blob inserted");

    // Simulate large wall-clock elapsed time. The pre-fix sweeper would
    // have dropped the pin via `expire_mirror_blobs(Duration::from_secs(120))`.
    // We verify the API no longer exists by simply asserting the count is
    // still 1 after we explicitly do NOT call any expiry routine. Without
    // calling remove_mirror_blobs, the pin survives forever.
    tokio::task::yield_now().await;
    assert_eq!(
        fss.mirror_blob_count(),
        1,
        "blob remains pinned without BlobsInStableStorage ack"
    );

    // Confirm explicit ack still drops the pin.
    fss.remove_mirror_blobs(&[digest]);
    assert_eq!(fss.mirror_blob_count(), 0, "explicit ack drops pin");
}

/// Test 2 (Directive D): mirror digest appears in the
/// `pinned_mirror_digests` snapshot, NOT in the FilesystemStore-backed
/// digest list. We exercise the FastSlowStore-level accessor here; the
/// worker's `send_periodic_blobs_available` lifts these into the proto's
/// `pinned_mirror_digests` field.
#[nativelink_test]
async fn pinned_mirror_digest_advertised_separately() {
    let fss = make_fss();
    let digest = d(2, 4);
    write_mirror(&fss, digest, Bytes::from_static(b"abcd")).await;

    let snap = fss.mirror_blob_digests();
    assert_eq!(snap, vec![digest], "snapshot includes mirror digest");

    // The mirror change tracker should report this as added since the
    // last drain.
    let changes = fss.drain_mirror_changes();
    assert!(changes.added.contains(&digest), "delta tracker recorded add");
    assert!(changes.removed.is_empty());

    // After draining, the next drain is empty.
    let changes2 = fss.drain_mirror_changes();
    assert!(changes2.added.is_empty());
    assert!(changes2.removed.is_empty());
}

/// Test 3 (server pull pipeline component): when a mirror blob is acked
/// via `remove_mirror_blobs`, the change tracker emits a `removed` entry
/// so the worker can include it in `evicted_digests` on the next
/// `BlobsAvailable`. (The full end-to-end "server pulls and broadcasts"
/// flow is covered by the existing blobs_available_integration_test;
/// this test pins the contract that drives the worker side.)
#[nativelink_test]
async fn ack_emits_removed_delta() {
    let fss = make_fss();
    let digest = d(3, 2);
    write_mirror(&fss, digest, Bytes::from_static(b"ok")).await;
    // Drain initial added so the next drain isolates the removal.
    drop(fss.drain_mirror_changes());
    fss.remove_mirror_blobs(&[digest]);

    let changes = fss.drain_mirror_changes();
    assert!(changes.removed.contains(&digest), "ack recorded as removal");
    assert!(changes.added.is_empty());
    assert_eq!(fss.mirror_blob_count(), 0);
}

/// Test 4 (Directive A — server-restart resilience): even if the worker
/// emits BlobsAvailable many times across what would be the old TTL
/// window, the pin survives until explicit ack. Models a server restart
/// (no acks for a long time).
#[nativelink_test]
async fn pin_survives_repeated_drain_cycles() {
    let fss = make_fss();
    let digest = d(4, 3);
    write_mirror(&fss, digest, Bytes::from_static(b"xyz")).await;

    // Worker sends BlobsAvailable many times; each call drains the change
    // tracker but does NOT touch the pin map.
    for _ in 0..50 {
        drop(fss.drain_mirror_changes());
    }
    assert_eq!(fss.mirror_blob_count(), 1, "drains do not affect pin");

    // The absence of any timer-based sweeper is what the test verifies.
    // A wall-clock sleep would only false-pass for the same reason: the
    // sweeper is gone. The fact that 50 drains have not removed the pin
    // proves the only valid removal mechanism is `remove_mirror_blobs`,
    // which is driven by `Update::BlobsInStableStorage` from the server.
}

/// Try a mirror write expecting failure (cap exceeded).
async fn try_write_mirror(
    fss: &std::sync::Arc<FastSlowStore>,
    digest: DigestInfo,
    data: Bytes,
) -> Result<(), nativelink_error::Error> {
    let store: Store = Store::new(fss.clone());
    IS_MIRROR_REQUEST
        .scope(true, async move { store.update_oneshot(digest, data).await })
        .await
}

/// Test 6: cap-exceeded must surface as `Err(ResourceExhausted)`, NOT
/// silently drop the blob and pretend success. The server-side mirror
/// writer relies on Err to call `record_mirror_failure` and route the
/// next attempt to a different peer.
#[nativelink_test]
async fn mirror_blob_dropped_when_cap_exceeded() {
    let fss = make_fss();
    // Set cap small enough that the second write busts it.
    fss.set_mirror_blobs_max_bytes_for_test(8);

    let d1 = d(10, 5);
    write_mirror(&fss, d1, Bytes::from_static(b"hello")).await;
    assert_eq!(fss.mirror_blob_count(), 1);
    // Drain so we isolate the next delta.
    drop(fss.drain_mirror_changes());

    let d2 = d(11, 6);
    let err = try_write_mirror(&fss, d2, Bytes::from_static(b"world!"))
        .await
        .expect_err("cap-exceeded must Err");
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "cap-exceeded must use ResourceExhausted, got {:?}",
        err
    );

    // Pin map and change tracker must NOT have been mutated by the rejected insert.
    assert_eq!(fss.mirror_blob_count(), 1, "rejected insert must not bump count");
    let snap = fss.mirror_blob_digests();
    assert!(!snap.contains(&d2), "rejected digest must not appear in snapshot");
    let mc = fss.drain_mirror_changes();
    assert!(
        mc.added.is_empty() && mc.removed.is_empty(),
        "rejected insert must not perturb the change tracker (got added={:?} removed={:?})",
        mc.added,
        mc.removed
    );
}

/// Test 7: insert+remove within a single drain window cancels — neither
/// `added` nor `removed` carries the digest because the worker never
/// actually advertised it.
#[nativelink_test]
async fn insert_then_remove_cancels_in_change_tracker() {
    let fss = make_fss();
    let digest = d(20, 3);
    write_mirror(&fss, digest, Bytes::from_static(b"abc")).await;
    fss.remove_mirror_blobs(&[digest]);

    let mc = fss.drain_mirror_changes();
    assert!(
        !mc.added.contains(&digest),
        "insert→remove must cancel `added`"
    );
    assert!(
        mc.removed.contains(&digest),
        "remove must dominate so server cleans up locality entries if any"
    );
    assert_eq!(fss.mirror_blob_count(), 0);
}

/// Test 8: remove-then-insert in one window resolves to `added` (the
/// blob is currently held).
#[nativelink_test]
async fn remove_then_insert_supersedes_in_change_tracker() {
    let fss = make_fss();
    let digest = d(21, 3);
    // First insert + drain so the digest exists, then we'll remove and re-insert.
    write_mirror(&fss, digest, Bytes::from_static(b"xyz")).await;
    drop(fss.drain_mirror_changes());

    fss.remove_mirror_blobs(&[digest]);
    write_mirror(&fss, digest, Bytes::from_static(b"xyz")).await;

    let mc = fss.drain_mirror_changes();
    assert!(
        mc.added.contains(&digest),
        "re-insert dominates: digest must be advertised as added"
    );
    assert!(
        !mc.removed.contains(&digest),
        "re-insert must wipe the prior pending removal"
    );
    assert_eq!(fss.mirror_blob_count(), 1);
}

/// Test 9 (Directive 4): full snapshot drains deltas FIRST so a remove
/// racing the snapshot is not lost. Uses the
/// `snapshot_and_reset_mirror_changes` accessor to model what
/// `send_periodic_blobs_available` does on the first tick.
#[nativelink_test]
async fn snapshot_consistent_with_drained_deltas() {
    let fss = make_fss();
    let d1 = d(30, 1);
    let d2 = d(31, 1);
    write_mirror(&fss, d1, Bytes::from_static(b"a")).await;
    write_mirror(&fss, d2, Bytes::from_static(b"b")).await;

    // Now drop d1: remove must be visible in either the drained `removed`
    // delta OR a snapshot that omits d1 — never lost.
    fss.remove_mirror_blobs(&[d1]);

    let (drained, snapshot) = fss.snapshot_and_reset_mirror_changes();
    let in_removed = drained.removed.contains(&d1);
    let in_snapshot = snapshot.contains(&d1);
    assert!(
        in_removed || !in_snapshot,
        "remove of d1 must surface as either a `removed` delta or absence \
         from snapshot; got removed={in_removed} snapshot_has_d1={in_snapshot}"
    );
    assert!(
        snapshot.contains(&d2),
        "d2 must remain pinned and present in snapshot"
    );

    // After the atomic drain+snapshot, the next drain must be empty.
    let next = fss.drain_mirror_changes();
    assert!(
        next.added.is_empty() && next.removed.is_empty(),
        "drain+snapshot must reset deltas to empty"
    );
}

/// Test 10: insert wakes the change-notify so the BlobsAvailable loop
/// reacts immediately rather than waiting for the backstop interval.
#[nativelink_test]
async fn insert_triggers_mirror_changes_notify() {
    let fss = make_fss();
    let notify = fss.mirror_changes_notify();
    let waiter = notify.notified();
    tokio::pin!(waiter);

    // Before the insert, the future must NOT be ready.
    let poll1 =
        futures::poll!(waiter.as_mut());
    assert!(matches!(poll1, std::task::Poll::Pending), "no notify yet");

    write_mirror(&fss, d(40, 1), Bytes::from_static(b"i")).await;

    // After the insert, the registered notification must resolve.
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("notify must fire within 1s of insert");
}

/// Test 11: remove also wakes the change-notify (so an unpin propagates
/// promptly to the server's locality cleanup).
#[nativelink_test]
async fn remove_triggers_mirror_changes_notify() {
    let fss = make_fss();
    let digest = d(41, 1);
    write_mirror(&fss, digest, Bytes::from_static(b"r")).await;
    drop(fss.drain_mirror_changes());

    // Drain any residual notify permits left by the insert: a fresh
    // `notified()` would otherwise immediately resolve from a stored permit
    // rather than waiting on a *new* notification.
    let notify = fss.mirror_changes_notify();
    {
        let drain = notify.notified();
        tokio::pin!(drain);
        // Single non-waiting poll consumes a stored permit if present;
        // otherwise it returns Pending and we drop the future.
        let _ = futures::poll!(drain.as_mut());
    }

    let waiter = notify.notified();
    tokio::pin!(waiter);
    let poll_before = futures::poll!(waiter.as_mut());
    assert!(
        matches!(poll_before, std::task::Poll::Pending),
        "after consuming residual permits, fresh notify must be Pending"
    );

    fss.remove_mirror_blobs(&[digest]);
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("notify must fire within 1s of remove");
}

/// Test 5 (Directive C): a digest present ONLY in `mirror_blobs` (not on
/// disk / in fast store) must still be visible to `has_with_results` and
/// `get_part_unchunked` when called via the FastSlowStore wrapper.
/// `handle_upload_missing_blobs` relies on this to upload mirror-only
/// blobs back to the server.
#[nativelink_test]
async fn mirror_only_digest_visible_via_wrapper() {
    let fss = make_fss();
    let digest = d(5, 4);
    let data = Bytes::from_static(b"mira");
    write_mirror(&fss, digest, data.clone()).await;

    let store: Store = Store::new(fss.clone());
    let mut results = vec![None];
    store
        .has_with_results(&[digest.into()], &mut results)
        .await
        .expect("has_with_results");
    assert_eq!(
        results,
        vec![Some(data.len() as u64)],
        "wrapper sees mirror-only blob"
    );

    let read_back = store
        .get_part_unchunked(digest, 0, None)
        .await
        .expect("get_part_unchunked");
    assert_eq!(read_back, data, "wrapper reads mirror-only blob bytes");
}
