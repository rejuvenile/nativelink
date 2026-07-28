// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #F3 sibling (2026-07-28): idle-TTL reap of abandoned
//! `ChunkInProgress::SpawnBlocking` entries in the FilesystemStore's
//! `chunked_partials` map.
//!
//! Leak class under test: a WriteChunkedV2 session abort deliberately
//! leaves the SpawnBlocking entry (+ open fd + on-disk `.partial`) so
//! the NEXT retry resumes the same partial — that reuse is load-bearing
//! and must be preserved. But a digest whose writers never return
//! (client gone for good) held entry + fd + `.partial` FOREVER (the map
//! is process-memory; only restart reclaimed it) and poisoned Path-A
//! dispatch with `AlreadyExists`.
//!
//! Invariant re-established: bounded resource holding — an abandoned
//! entry is reclaimed within `idle_ttl` (+ one reap tick) once no
//! writer session holds it; entries with an ACTIVE writer session, and
//! idle entries younger than the TTL, are never touched.
//!
//! All tests run in production composition (real `FilesystemStore`,
//! real session-guard + reap APIs); deterministic time via
//! `start_paused` + `tokio::time::advance`; the race test serializes
//! interleavings with a notify/semaphore gate — no sleep-as-sync, no
//! jitter loops.

#![cfg(all(feature = "chunked_fast_slow", feature = "test-utils"))]

use core::time::Duration;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;

/// 4 KiB test chunks (production `CHUNK_SIZE` is 1 MiB; the reap logic
/// is size-agnostic and small chunks keep the tests fast).
const CHUNK: usize = 4 * 1024;

/// The idle TTL used by the direct-reap tests (as a `Duration`, driven
/// through `reap_idle_chunked_partials_for_test`).
const TEST_TTL: Duration = Duration::from_secs(600);

/// Build a fresh on-disk `FilesystemStore` with the given
/// `chunked_idle_partial_reap_ttl_s` config value. Returns
/// `(store, content_path)` so tests can inspect the canonical CAS file
/// after a commit. `ttl_s = 0` disables the BACKGROUND reaper task so
/// tests can drive `reap_idle_chunked_partials_for_test` directly with
/// full control of the clock.
async fn make_store(ttl_s: u64) -> (Arc<FilesystemStore<FileEntryImpl>>, String) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/sbreap/content");
    let temp_path = format!("{base}/{nonce}/sbreap/temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path,
        eviction_policy: None,
        block_size: 1,
        chunked_idle_partial_reap_ttl_s: ttl_s,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed for sbreap tests");
    (store, content_path)
}

/// Unique 2-chunk digest per `seed`; declared size = 2 * CHUNK.
fn make_digest(seed: u8) -> DigestInfo {
    let mut hash = [0u8; 32];
    hash[0] = seed;
    hash[31] = seed;
    DigestInfo::new(hash, (2 * CHUNK) as u64)
}

/// Canonical CAS path for a digest under `content_path` (mirrors the
/// `<content>/d/<XX>/<digest>` layout `finalize_holding` renames into).
/// `seed` is the digest's first hash byte (the shard), as passed to
/// [`make_digest`].
fn canonical_path(content_path: &str, digest: &DigestInfo, seed: u8) -> PathBuf {
    PathBuf::from(format!("{content_path}/d/{seed:02x}/{digest}"))
}

/// Linux-only: returns true if any fd of THIS process currently refers
/// to `path` (via /proc/self/fd readlink). Used to prove the reaped
/// entry's `std::fs::File` was actually closed, not just unmapped.
#[cfg(target_os = "linux")]
fn process_holds_fd_for(path: &std::path::Path) -> bool {
    let Ok(dir) = std::fs::read_dir("/proc/self/fd") else {
        return false;
    };
    for entry in dir.flatten() {
        if let Ok(target) = std::fs::read_link(entry.path()) {
            if target == path {
                return true;
            }
        }
    }
    false
}

/// T1 — the invariant test: an entry whose last writer session ended
/// (guard dropped) and that has been idle past the TTL is reaped —
/// entry gone from the map, fd closed, on-disk `.partial` deleted.
///
/// MUTATION M1 (comment out the map-removal in the reap loop): this
/// test red-fails at "SpawnBlocking entry must be REAPED".
/// MUTATION M3 (comment out the session-guard decrement): the writer
/// count never returns to zero, the reap never fires, and this test
/// red-fails at the same assert — the dedicated inverse-catcher the
/// dispatch requires.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn spawn_blocking_entry_reaped_after_idle_ttl() {
    let (store, _content) = make_store(0).await;
    let digest = make_digest(0x01);
    let partial_path = store.partial_path_for_digest(&digest);

    // A writer session writes chunk 0 and then goes away for good
    // (client gone) — the production leak shape.
    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xAA; CHUNK]))
            .await
            .expect("chunk 0 write must succeed");
    }
    assert!(
        store.has_in_flight_chunked_partial(&digest),
        "fixture guard: entry must exist after the session aborts (retry-reuse \
         contract — if this fails the fixture is not modeling the leak class)",
    );
    assert!(
        partial_path.exists(),
        "fixture guard: .partial must exist on disk after the aborted session",
    );

    // Idle past the TTL (virtual clock).
    tokio::time::advance(TEST_TTL + Duration::from_secs(1)).await;

    let reaped = store.reap_idle_chunked_partials_for_test(TEST_TTL).await;
    assert_eq!(
        reaped, 1,
        "idle-TTL reap must reap exactly the one abandoned SpawnBlocking entry \
         (0 = the reap mechanism did not fire: the 2026-07-28 leak class — entry+fd+\
         partial held until process restart — is back)",
    );
    assert!(
        !store.has_in_flight_chunked_partial(&digest),
        "SpawnBlocking entry must be REAPED from chunked_partials after idle TTL \
         with zero active writers — a resident entry means the abandoned-entry \
         leak (fd + map memory + Path-A AlreadyExists poisoning) persists",
    );
    assert!(
        !partial_path.exists(),
        "on-disk .partial must be deleted by the idle-TTL reap (disk leak: \
         abandoned partials previously survived until the next process restart's \
         prune_temp_path sweep)",
    );
    #[cfg(target_os = "linux")]
    assert!(
        !process_holds_fd_for(&partial_path),
        "no fd may still refer to the reaped .partial — the entry Arc drop must \
         close the SpawnBlocking file handle (fd leak class)",
    );
}

/// T1b — plumbing test: the BACKGROUND reaper task spawned by
/// `FilesystemStore::new` (config `chunked_idle_partial_reap_ttl_s`,
/// default ON) fires on its own tick — no manual reap call.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn background_reaper_task_reaps_after_ttl_tick() {
    // TTL 120 s → tick = min(120, 60) = 60 s; entry expires at the
    // t=120 s tick.
    let (store, _content) = make_store(120).await;
    let digest = make_digest(0x02);
    let partial_path = store.partial_path_for_digest(&digest);

    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xBB; CHUNK]))
            .await
            .expect("chunk 0 write must succeed");
    }
    assert!(
        store.has_in_flight_chunked_partial(&digest),
        "fixture guard: entry must exist after the session aborts",
    );

    // Advance past TTL + one tick so the background task's sleep fires
    // and the reap runs. The unlink happens on the real blocking pool,
    // so resume the clock and poll with a REAL-time deadlock detector.
    tokio::time::advance(Duration::from_secs(200)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.has_in_flight_chunked_partial(&digest) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "background chunked-partials reaper task must reap the abandoned \
         SpawnBlocking entry after the idle TTL elapses — if this hangs, the \
         config-spawned reaper (chunked_idle_partial_reap_ttl_s, default ON) \
         is not wired to FilesystemStore::new",
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while partial_path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background reaper must also delete the on-disk .partial");
}

/// T2 — under-TTL retention + retry-reuse: an entry idle for LESS than
/// the TTL is untouched, and a second writer session resumes the SAME
/// partial (chunk 0's bytes survive into the committed CAS file
/// without being rewritten).
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn entry_survives_idle_below_ttl_and_retry_reuses_partial() {
    let (store, content_path) = make_store(0).await;
    let digest = make_digest(0x03);
    let partial_path = store.partial_path_for_digest(&digest);

    // Session A writes chunk 0, then aborts.
    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xC0; CHUNK]))
            .await
            .expect("session A chunk 0 write must succeed");
    }

    // Idle for TTL/2 — must survive a reap pass.
    tokio::time::advance(TEST_TTL / 2).await;
    let reaped = store.reap_idle_chunked_partials_for_test(TEST_TTL).await;
    assert_eq!(
        reaped, 0,
        "an entry idle BELOW the TTL must NOT be reaped — reaping it would \
         destroy the retry-reuse contract (the next retry within the worker's \
         ~41 s deferred-upload cadence must resume the same partial)",
    );
    assert!(
        store.has_in_flight_chunked_partial(&digest) && partial_path.exists(),
        "entry + .partial must survive an under-TTL reap pass (retry-reuse)",
    );

    // Session B (the retry) resumes the SAME partial: writes only the
    // MISSING chunk 1, then commits. Success proves chunk 0's bytes
    // were still there — production composition, not a mock map.
    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, CHUNK as u64, Bytes::from(vec![0xC1; CHUNK]))
            .await
            .expect("session B chunk 1 write must succeed");
        store
            .commit_chunked(&digest, (2 * CHUNK) as u64)
            .await
            .expect(
                "retry session's commit must succeed against the REUSED partial — \
                 a length mismatch here means chunk 0's bytes were lost, i.e. the \
                 under-TTL entry was not actually reused",
            );
        store
            .finalize_holding(&digest)
            .await
            .expect("finalize_holding must succeed for the reused partial");
    }

    let canonical = canonical_path(&content_path, &digest, 0x03);
    let committed = std::fs::read(&canonical).expect(
        "committed CAS file must exist at the canonical path after finalize",
    );
    assert_eq!(committed.len(), 2 * CHUNK, "committed length must be full blob");
    assert!(
        committed[..CHUNK].iter().all(|b| *b == 0xC0),
        "chunk 0 bytes (written by aborted session A) must survive into the \
         committed file — session B never rewrote them, so the partial was \
         genuinely REUSED (retry-reuse is load-bearing)",
    );
    assert!(
        committed[CHUNK..].iter().all(|b| *b == 0xC1),
        "chunk 1 bytes (written by retry session B) must be present",
    );
}

/// T3 — over-action direction: an ACTIVE writer session pins the entry
/// past ANY idle age; only after the guard drops (+ TTL idle) may the
/// reap fire.
///
/// MUTATION M2 (comment out the active_writers check in the reap
/// filter): this test red-fails at "active writer session must PIN".
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn entry_not_reaped_while_writer_active() {
    let (store, _content) = make_store(0).await;
    let digest = make_digest(0x04);
    let partial_path = store.partial_path_for_digest(&digest);

    let session = store.begin_chunked_write_session(digest);
    store
        .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xD0; CHUNK]))
        .await
        .expect("chunk 0 write must succeed");

    // Way past the TTL — but the session is still alive.
    tokio::time::advance(TEST_TTL * 10).await;
    let reaped = store.reap_idle_chunked_partials_for_test(TEST_TTL).await;
    assert_eq!(
        reaped, 0,
        "an entry with an ACTIVE writer session (active_writers > 0) must NEVER \
         be reaped regardless of idle age — reaping it would yank the partial \
         out from under a live writer (#494 double-writer protection direction)",
    );
    assert!(
        store.has_in_flight_chunked_partial(&digest) && partial_path.exists(),
        "active writer session must PIN entry + .partial past the TTL",
    );

    // Differential: once the guard drops and the TTL passes, the SAME
    // composition reaps it.
    drop(session);
    tokio::time::advance(TEST_TTL + Duration::from_secs(1)).await;
    let reaped = store.reap_idle_chunked_partials_for_test(TEST_TTL).await;
    assert_eq!(
        reaped, 1,
        "after the last guard drops and the TTL elapses the entry must be \
         reaped (differential half of the writer-active pin test)",
    );
    assert!(
        !store.has_in_flight_chunked_partial(&digest) && !partial_path.exists(),
        "post-drop + post-TTL: entry and .partial must be gone",
    );
}

/// T-EX — the F3 interlock: `IoUringMarker` entries are EXEMPT from the
/// idle-TTL reap in BOTH liveness states.
///
/// - fd ALIVE (the F1 abort-drain window: driver gone, guard left the
///   entry resident, detached writer may still write the inode): idle-
///   reaping it would drop the #494 presence-keyed gate mid-drain —
///   exactly the corruption seam the F3 fix-up closes.
/// - fd DEAD (stale marker): owned by F3's liveness-gated reapers
///   (Path-B takeover / Path-A stale-reap), which REUSE the partial;
///   idle-reaping would race them and delete a partial a takeover is
///   about to resume.
///
/// The exemption is the `ChunkInProgress::IoUringMarker { .. } => false`
/// arm of the reap's victim filter (`reap_idle_spawn_blocking_partials`).
/// MUTATION M-EX (flip that arm to `true`): this test red-fails at
/// "must NEVER idle-reap an IoUringMarker".
#[cfg(all(feature = "io-uring", target_os = "linux"))]
#[nativelink_test]
async fn io_uring_marker_exempt_from_idle_reap_alive_and_dead() {
    let (store, _content) = make_store(0).await;
    let digest = make_digest(0x06);
    let partial_path = store.partial_path_for_digest(&digest);

    // Insert a marker; drop the driver guard while the writer fd is
    // ALIVE — the F3-F1 leave-resident path keeps the entry in the map
    // (production shape: driver aborted mid-drain).
    let (fd, marker_guard) = store
        .open_chunked_partial_marker(digest)
        .await
        .expect("marker insert must succeed on a fresh store");
    drop(marker_guard);
    assert!(
        store.has_in_flight_chunked_partial(&digest),
        "fixture guard: F3-F1 must leave the marker resident while the writer \
         fd is alive (if this fails the fixture no longer models the drain window)",
    );

    // fd ALIVE: zero-TTL reap must skip the marker.
    let reaped = store.reap_idle_chunked_partials_for_test(Duration::ZERO).await;
    assert_eq!(
        reaped, 0,
        "the idle reap must NEVER idle-reap an IoUringMarker (fd ALIVE — the F1 \
         abort-drain window): removing it drops the #494 presence-keyed gate \
         while the detached writer can still write the inode",
    );
    assert!(
        store.has_in_flight_chunked_partial(&digest) && partial_path.exists(),
        "live-fd marker entry + .partial must survive the idle reap",
    );

    // fd DEAD (writer drained): the marker is now STALE — still exempt;
    // F3's liveness-gated takeover/stale-reap owns it (and REUSES the
    // partial), so the idle reap must not delete the file out from
    // under a takeover.
    drop(fd);
    let reaped = store.reap_idle_chunked_partials_for_test(Duration::ZERO).await;
    assert_eq!(
        reaped, 0,
        "the idle reap must NEVER idle-reap an IoUringMarker (fd DEAD — stale \
         marker): F3's Path-B takeover / Path-A stale-reap own that variant and \
         resume the same partial; idle-reaping would race the takeover",
    );
    assert!(
        store.has_in_flight_chunked_partial(&digest) && partial_path.exists(),
        "stale marker entry + .partial must survive the idle reap (F3 owns them)",
    );
}

/// T4 — reap vs new-session race, deterministic: a new session that
/// arrives while the reaper is BETWEEN map-removal and file-unlink must
/// not get a torn state. The map-side `reaping` mark makes the new
/// session's file-open WAIT until the unlink completes, so the session
/// creates a genuinely FRESH partial (never a file the in-flight unlink
/// is about to delete).
///
/// Interleaving is pinned with the test-only pre-unlink gate (notify +
/// semaphore) — no jitter loops, no sleeps.
#[nativelink_test]
async fn reap_vs_new_session_race_no_torn_state() {
    let (store, content_path) = make_store(0).await;
    let digest = make_digest(0x05);
    let partial_path = store.partial_path_for_digest(&digest);

    // Abandoned entry with BOTH chunks written → old file length is
    // 2*CHUNK. The fresh file created by the racing session writes only
    // chunk 0 → length CHUNK. Length is the discriminator between
    // "fresh file" (correct), "old file survived" (unlink skipped), and
    // "file deleted after re-create" (torn state → NotFound).
    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xE0; CHUNK]))
            .await
            .expect("old session chunk 0 write must succeed");
        store
            .write_chunk_at_offset(&digest, CHUNK as u64, Bytes::from(vec![0xE0; CHUNK]))
            .await
            .expect("old session chunk 1 write must succeed");
    }

    let gate = store.set_test_reap_pre_unlink_gate(&digest);

    // Task A: the reaper. Parks at the gate AFTER removing the entry
    // from the map (and marking the digest as reaping), BEFORE the
    // unlink.
    let store_a = Arc::clone(&store);
    let reap_handle = tokio::spawn(async move {
        store_a
            .reap_idle_chunked_partials_for_test(Duration::ZERO)
            .await
    });

    // Wait until the reaper is parked at the gate.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = gate.reached.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if gate.reached_flag.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("reaper must reach the pre-unlink gate (deadlock detector)");

    // Task B: a new writer session racing the in-flight reap. Its
    // first write must WAIT for the unlink (reaping mark) rather than
    // re-creating a file the reaper is about to delete.
    let store_b = Arc::clone(&store);
    let mut write_handle = tokio::spawn(async move {
        let _session = store_b.begin_chunked_write_session(digest);
        let res = store_b
            .write_chunk_at_offset(&digest, 0, Bytes::from(vec![0xE1; CHUNK]))
            .await;
        // Keep holding the session guard until the test observed disk
        // state? No — the assertions below only need the write result;
        // the guard may drop here (entry stays per retry-reuse).
        res
    });

    // The racing write must be PENDING while the unlink is in flight.
    let parked = tokio::time::timeout(Duration::from_millis(200), &mut write_handle).await;
    assert!(
        parked.is_err(),
        "a new session's first write must WAIT while the reaper is between \
         map-removal and unlink (reaping mark) — completing here means the \
         write re-created a file the in-flight unlink will delete (torn state)",
    );

    // Release the reaper; both sides must now complete cleanly.
    gate.proceed.add_permits(1);
    let reaped = tokio::time::timeout(Duration::from_secs(5), reap_handle)
        .await
        .expect("reaper must complete after gate release (deadlock detector)")
        .expect("reaper task must not panic");
    assert_eq!(reaped, 1, "the racing reap must have reaped the old entry");
    tokio::time::timeout(Duration::from_secs(5), &mut write_handle)
        .await
        .expect("racing write must complete after the unlink finishes (deadlock detector)")
        .expect("racing write task must not panic")
        .expect("racing write must succeed once the reap completes");

    // No torn state: the new session's FRESH file exists with exactly
    // the new session's bytes (length CHUNK — not the old 2*CHUNK file,
    // and not deleted).
    let meta = std::fs::metadata(&partial_path).expect(
        "the racing session's fresh .partial must exist after the reap — \
         NotFound here is the torn state (reaper unlinked the file the new \
         session had just created)",
    );
    assert_eq!(
        meta.len(),
        CHUNK as u64,
        "fresh .partial must contain exactly the new session's chunk 0 — \
         length 2*CHUNK means the OLD file survived the reap (unlink skipped) \
         and the new session resumed a file the reaper owned",
    );
    assert!(
        store.has_in_flight_chunked_partial(&digest),
        "the racing session must own a fresh map entry after the reap",
    );
    store.clear_test_reap_pre_unlink_gate(&digest);

    // And the fresh state is fully usable: finish the blob + commit.
    {
        let _session = store.begin_chunked_write_session(digest);
        store
            .write_chunk_at_offset(&digest, CHUNK as u64, Bytes::from(vec![0xE1; CHUNK]))
            .await
            .expect("chunk 1 write on the fresh partial must succeed");
        store
            .commit_chunked(&digest, (2 * CHUNK) as u64)
            .await
            .expect("commit on the fresh partial must succeed (clean restart-from-0)");
        store
            .finalize_holding(&digest)
            .await
            .expect("finalize on the fresh partial must succeed");
    }
    let canonical = canonical_path(&content_path, &digest, 0x05);
    let committed = std::fs::read(&canonical)
        .expect("committed CAS file must exist after the post-race commit");
    assert!(
        committed.iter().all(|b| *b == 0xE1),
        "committed content must be entirely the NEW session's bytes — any 0xE0 \
         byte means old-file state leaked across the reap boundary",
    );
}
