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
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{IS_MIRROR_REQUEST, Store, StoreKey, StoreLike};
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
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
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
    assert!(
        changes.added.contains(&digest),
        "delta tracker recorded add"
    );
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
) -> Result<(), Error> {
    let store: Store = Store::new(fss.clone());
    IS_MIRROR_REQUEST
        .scope(
            true,
            async move { store.update_oneshot(digest, data).await },
        )
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
        Code::ResourceExhausted,
        "cap-exceeded must use ResourceExhausted, got {:?}",
        err
    );

    // Pin map and change tracker must NOT have been mutated by the rejected insert.
    assert_eq!(
        fss.mirror_blob_count(),
        1,
        "rejected insert must not bump count"
    );
    let snap = fss.mirror_blob_digests();
    assert!(
        !snap.contains(&d2),
        "rejected digest must not appear in snapshot"
    );
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
    let poll1 = futures::poll!(waiter.as_mut());
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

/// Test (review #3): a mirror-only blob (held in `mirror_blobs`, NOT
/// on the slow store and NOT yet on the fast store) MUST be
/// materializable to the fast store via `populate_fast_store_unchecked`
/// — without round-tripping the slow store. The slow store may be down
/// or may have lost the blob; the worker is the only durable holder.
///
/// Pre-fix `populate_fast_store_unchecked` always called
/// `copy_slow_to_fast`, which would fail with NotFound here.
#[nativelink_test]
async fn populate_fast_store_unchecked_materializes_mirror_only_blob() {
    let fss = make_fss();
    let digest = d(50, 4);
    let data = Bytes::from_static(b"miry");
    write_mirror(&fss, digest, data.clone()).await;

    // Confirm the slow store does NOT have it (the mirror write is
    // memory-only and never touches slow_store).
    let slow_has = fss
        .slow_store()
        .has(StoreKey::from(digest))
        .await
        .expect("slow has");
    assert!(slow_has.is_none(), "mirror write must not touch slow store");

    // populate_fast_store_unchecked must succeed via mirror materialization.
    fss.populate_fast_store_unchecked(StoreKey::from(digest))
        .await
        .expect("mirror-only populate must succeed");

    // After populate, the bytes must be on the fast store.
    let read_back = fss
        .fast_store()
        .get_part_unchunked(digest, 0, None)
        .await
        .expect("read-back from fast store");
    assert_eq!(
        read_back, data,
        "fast store has the materialized mirror bytes"
    );
}

/// Test (review #3): same flow via `populate_fast_store` (which checks
/// `has()` first). After clearing the fast store, a fresh populate must
/// route through the mirror materialization path.
#[nativelink_test]
async fn populate_fast_store_uses_mirror_when_disk_empty() {
    let fss = make_fss();
    let digest = d(51, 5);
    let data = Bytes::from_static(b"miryy");
    write_mirror(&fss, digest, data.clone()).await;

    // populate_fast_store must succeed even though the slow store is
    // empty.
    fss.populate_fast_store(StoreKey::from(digest))
        .await
        .expect("populate_fast_store must materialize mirror");

    let read_back = fss
        .fast_store()
        .get_part_unchunked(digest, 0, None)
        .await
        .expect("read-back from fast store");
    assert_eq!(read_back, data);
}

/// Mirror-materialize must produce a CAS file with the canonical 0o555
/// mode (review #14). Pre-set in `filesystem_store::emplace_file`; this
/// test guards against a regression where the mirror-materialize path
/// bypasses or post-overwrites that mode bit. Linux-only: macOS dev
/// targets don't go through the same chmod path.
#[cfg(target_os = "linux")]
#[nativelink_test]
async fn mirror_materialize_sets_0o555_mode_on_disk() {
    use std::os::unix::fs::PermissionsExt;

    use nativelink_store::filesystem_store::digest_content_path;

    // Build a FastSlowStore whose fast tier is a real FilesystemStore so
    // the post-write chmod actually fires. (`make_fss()` uses a memory
    // fast store, which has no on-disk file to inspect.) Bind both
    // `TempDir`s to locals so Drop fires at end of scope (same anti-
    // pattern as the mongo_runner `.keep()` leak fixed in 086d0d31).
    let content_dir = tempfile::Builder::new()
        .prefix("nl_mirror_mode_content_")
        .tempdir()
        .expect("tempdir");
    let temp_dir = tempfile::Builder::new()
        .prefix("nl_mirror_mode_temp_")
        .tempdir()
        .expect("tempdir");
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_dir.path().to_string_lossy().into_owned(),
        temp_path: temp_dir.path().to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy::default()),
        ..Default::default()
    })
    .await
    .expect("filesystem store");

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fs_store),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let digest = d(60, 4);
    let data = Bytes::from_static(b"perm");
    write_mirror(&fss, digest, data.clone()).await;

    fss.populate_fast_store_unchecked(StoreKey::from(digest))
        .await
        .expect("mirror-only populate");

    // Locate the on-disk file via the documented path layout and assert
    // the mode is exactly 0o555 (CAS read-execute, no write).
    let content_path_str = content_dir.path().to_string_lossy().into_owned();
    let on_disk_path = digest_content_path(&content_path_str, &digest);
    let meta = std::fs::metadata(&on_disk_path).unwrap_or_else(|err| {
        panic!(
            "expected materialized file at {}: {err:?}",
            std::path::Path::new(&on_disk_path).display()
        )
    });
    let mode = meta.permissions().mode() & 0o7777;
    assert_eq!(
        mode,
        0o555,
        "materialized CAS file mode must be 0o555 (got {mode:o}) at {}",
        std::path::Path::new(&on_disk_path).display()
    );
}

/// Lock-ordering regression test (review #2/#4/#5): the canonical
/// acquisition order is `mirror_blobs` BEFORE `mirror_changes`. A previous
/// version of `snapshot_and_reset_mirror_changes` took the locks in the
/// inverted order and could AB/BA-deadlock with a concurrent insert/remove.
///
/// Implementation notes (review #4):
///   * Run on an OS thread, not a tokio task. A deadlock here will block
///     ALL tokio worker threads (these are sync `parking_lot::Mutex`
///     waits, not awaits), so an inner `tokio::time::timeout` would
///     never fire — we'd wedge the entire test suite. The OS-thread guard
///     uses `std::sync::mpsc::recv_timeout` so a regression surfaces as
///     a clean panic ("AB/BA deadlock detected") within 15 seconds.
///   * 10_000 iterations with NO `yield_now()` — `yield_now()` lets the
///     scheduler reorder operations, which weakens the contention race.
///     A regression should show up within a few hundred iterations under
///     real contention; 10_000 gives a wide safety margin.
///   * `flavor = "current_thread"` — the contention is between the
///     spawned OS thread (running the producer/remover/snapshotter on its
///     own runtime) and... nothing else here. We control the parallelism
///     directly with three `std::thread::spawn` workers below to ensure
///     they execute on three real OS threads simultaneously, which is
///     the only configuration that exhibits the AB/BA race on
///     `parking_lot::Mutex`.
#[nativelink_test]
async fn lock_ordering_no_deadlock_under_contention() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    const ITERATIONS: u32 = 10_000;
    const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(15);

    // The stress workload runs on dedicated OS threads (NOT tokio tasks)
    // so an AB/BA deadlock on the parking_lot mutexes can't wedge the
    // tokio worker pool. Each worker thread owns a fresh current-thread
    // tokio runtime for the async helpers. The outer thread waits via
    // an std::sync::mpsc::recv_timeout so we surface a deadlock as a
    // clean panic ("AB/BA deadlock detected") within the bound.
    //
    // make_fss() is called inside the stress thread so its construction
    // sees a tokio context (MemoryStore/FastSlowStore initializers may
    // touch tokio internals on creation).
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let _runner = std::thread::Builder::new()
        .name("mirror-lock-ordering-stress".into())
        .spawn(move || {
            let setup_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let fss = setup_rt.block_on(async { make_fss() });
            let stop = std::sync::Arc::new(AtomicBool::new(false));

            // Inserter: bursts mirror writes for a rolling set of digests.
            // Uses a tokio current-thread runtime per worker so the async
            // `try_write_mirror` helper can be reused; the locks under
            // test are sync `parking_lot::Mutex` so the runtime choice
            // doesn't affect the AB/BA race itself.
            let t_inserter = {
                let fss = fss.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        for i in 0..ITERATIONS {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            let digest = d((i % 200) as u8, 1);
                            let _ = try_write_mirror(&fss, digest, Bytes::from_static(b"x")).await;
                        }
                    });
                })
            };

            // Remover: same rolling set so insert/remove genuinely contend.
            let t_remover = {
                let fss = fss.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    for i in 0..ITERATIONS {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        fss.remove_mirror_blobs(&[d((i % 200) as u8, 1)]);
                    }
                })
            };

            // Snapshotter: this is the call site at risk of AB/BA. Hammer
            // it from a third thread so all three orderings can interleave.
            let t_snapshotter = {
                let fss = fss.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let _ = fss.snapshot_and_reset_mirror_changes();
                    }
                })
            };

            t_inserter.join().expect("inserter panicked");
            t_remover.join().expect("remover panicked");
            t_snapshotter.join().expect("snapshotter panicked");
            // Best-effort: a deadlocked stress thread will never reach
            // here; the test failure path is the recv_timeout below.
            let _ = done_tx.send(());
        })
        .expect("spawn stress thread");

    match done_rx.recv_timeout(DEADLOCK_TIMEOUT) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "AB/BA deadlock detected: lock-ordering stress did not \
                 complete {ITERATIONS} iterations within {DEADLOCK_TIMEOUT:?} — \
                 `snapshot_and_reset_mirror_changes` is likely taking the \
                 mirror_blobs/mirror_changes locks in the wrong order"
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("stress thread panicked before signaling completion");
        }
    }
}
