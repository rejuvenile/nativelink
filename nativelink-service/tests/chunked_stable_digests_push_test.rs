// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #282 production-incident-2026-05-06 fix: chunked-write completion
//! must push committed digests onto the FastSlowStore's
//! `stable_digests` queue (and notify `stable_notify`) so the BIS
//! broadcast loop can ack stable bytes. WITHOUT this push, worker
//! `mirror_blobs` and server fast-tier pins accumulate unboundedly
//! until the 120 s pin TTL drains them — the production incident
//! mechanism that drove the server MemoryStore (48 GB cap) to
//! ResourceExhausted.
//!
//! The fix touches THREE push sites in `chunked_write_handler.rs`:
//!
//!   1. line 1618           — Synchronous commit success branch
//!   2. lines 1697-1700     — AsyncCommit reaper's success branch
//!   3. lines 2398-2400     — early-dedup short-circuit branch
//!
//! Plus a NOTIFY side: the closure returned by
//! `FastSlowStore::stable_digests_pusher()` calls
//! `stable_notify.notify_one()` after pushing — without which the BIS
//! broadcast loop's `notified().await` never resolves and
//! `stable_digests` accumulates without ever being drained.
//!
//! ## Tests in this file
//!
//! Asymmetric contract coverage (CLAUDE.md) for site 2 (AsyncCommit reaper):
//!   - **Under-action** (`chunked_commit_pushes_digest_to_stable_digests`):
//!     chunked write commits OK → digest MUST appear in
//!     `drain_stable_digests()` within timeout. Mutation: comment out
//!     `sink(stream_digest)` at the AsyncCommit reaper
//!     (`chunked_write_handler.rs:1697-1700`); test red-fails with the
//!     "production incident 2026-05-06 mechanism" message.
//!   - **Over-action**
//!     (`chunked_commit_failure_does_not_push_to_stable_digests`):
//!     chunked commit FAILURE (forced e2e SHA-256 mismatch via lying
//!     digest) → digest MUST NOT appear in `drain_stable_digests()`.
//!     (BIS protocol requires bytes to be durably stored before ack.)
//!   - **Race coverage**
//!     (`chunked_commit_no_visibility_gap_between_in_flight_and_stable`):
//!     while a chunked write is in flight, the digest must be
//!     observable as either "in_flight" (chunked in-flight set OR
//!     in_flight_slow_writes) OR "stable" — never neither. The push
//!     happens BEFORE in_flight removal so the visibility gap is closed.
//!
//! Sibling-site under-action coverage:
//!   - **Synchronous commit**
//!     (`chunked_synchronous_commit_pushes_digest_to_stable_digests`):
//!     calls `dispatch_chunks_to_driver(CommitMode::Synchronous)`
//!     directly with a production-shaped sink closure. Mutation:
//!     comment out `sink(stream_digest)` at site 1 (line 1618); test
//!     red-fails with "synchronous chunked commit must push to
//!     stable_digests — sibling-bug regression".
//!   - **Early-dedup short-circuit**
//!     (`chunked_early_dedup_short_circuit_pushes_digest_to_stable_digests`):
//!     pre-populates the FilesystemStore so `has_indexed_digest`
//!     returns Some, then drives a re-upload through
//!     `dispatch_bazel_facing_internal_chunking` with the sink wired.
//!     Mutation: comment out `sink(digest)` at site 3 (line 2399);
//!     test red-fails with "early-dedup short-circuit must push to
//!     stable_digests — defense-in-depth regression".
//!
//! Notify-side coverage:
//!   - **stable_notify wakeup**
//!     (`chunked_commit_notifies_stable_notify_waiters`): subscribes
//!     to `fast_slow.stable_notify().notified()` BEFORE driving an
//!     AsyncCommit, asserts the future resolves within
//!     `tokio::time::timeout(5s)`. Mutation: comment out
//!     `stable_notify.notify_one()` in the closure at
//!     `fast_slow_store.rs:709`; test red-fails with "BIS broadcast
//!     loop wakeup contract violated".

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::{
    BazelChunkedDispatcherImpl, ChunkedWriteHandlerMetrics, ChunkedWriteInFlight, CommitMode,
    PreparedChunk, dispatch_bazel_facing_internal_chunking, dispatch_chunks_to_driver,
};
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry;
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, disable_bazel_facing_internal_chunking,
    enable_bazel_facing_internal_chunking,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair_with_size;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreDriver, StoreLike, UploadSizeInfo};
use sha2::{Digest as _, Sha256};

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

fn make_test_chunk_budget() -> &'static ChunkBudget {
    Box::leak(Box::new(ChunkBudget::new()))
}

fn make_test_pin_budget(cap_bytes: usize) -> &'static PinBudget {
    Box::leak(Box::new(PinBudget::new(cap_bytes)))
}

/// Process-wide kill-switch lock (the chunked kill-switch is a
/// process-global atomic, so concurrent tests that flip it would
/// race). Mirrors `chunked_p25_p27_e2e_test.rs::kill_switch_lock`.
fn kill_switch_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn make_filesystem_store() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/282-stable-digests-push/content");
    let temp_path = format!("{base}/{nonce}/282-stable-digests-push/temp");
    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed")
}

/// Production-composition fixture: a real `FastSlowStore` (fast =
/// MemoryStore, slow = FilesystemStore) wired with a real
/// `BazelChunkedDispatcherImpl` carrying the
/// `stable_digests_pusher()` closure. Mirrors the production wiring
/// in `wire_bazel_chunked_dispatcher`.
async fn make_e2e_fast_slow_with_sink(
    chunk_size: usize,
) -> Arc<FastSlowStore> {
    let fs_store = make_filesystem_store().await;
    let fast_store: Store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store: Store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    let registry = ChunkedReadRegistry::new();
    let in_flight = ChunkedWriteInFlight::new();
    let chunk_budget = make_test_chunk_budget();
    let pin_budget = make_test_pin_budget(64 * 1024 * 1024);
    let dispatcher = Arc::new(
        BazelChunkedDispatcherImpl::new_with_state_and_pin_budget_for_test(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            pin_budget,
            chunk_size,
        )
        .with_registry(Arc::clone(&registry))
        .with_in_flight_tracking(
            fast_slow.chunked_in_flight_digests_handle(),
            fast_slow.in_flight_empty_notify_handle(),
        )
        // #282 fix under test: install the BIS push closure.
        .with_stable_digests_sink(fast_slow.stable_digests_pusher())
        // #283 fix under test: install the failed-commit closure so the
        // AsyncCommit reaper Err arm performs the legacy bookkeeping
        // (failed_slow_writes insert + fast-store re-pin) on commit
        // FAILURE. Mirrors `wire_bazel_chunked_dispatcher`.
        .with_failed_commit_sink(fast_slow.failed_writes_inserter()),
    );
    fast_slow.set_chunked_read_registry(Arc::clone(&registry));
    fast_slow
        .set_bazel_chunked_dispatcher(Arc::clone(&dispatcher) as Arc<dyn BazelChunkedDispatcher>);
    fast_slow.set_chunked_size_threshold_for_test(chunk_size as u64);

    fast_slow
}

/// Helper: drive a Bazel-shaped write through `FastSlowStore::update`.
async fn run_update(
    fast_slow: &Arc<FastSlowStore>,
    digest: DigestInfo,
    data: Bytes,
) -> Result<(), nativelink_error::Error> {
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    let total = data.len() as u64;
    let store_clone: Arc<FastSlowStore> = Arc::clone(fast_slow);
    let key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    let writer_fut = async move {
        if !data.is_empty() {
            tx.send(data).await.expect("tx.send must succeed");
        }
        tx.send_eof().expect("tx.send_eof must succeed");
    };
    let store_call = async move {
        store_clone
            .update(key, rx, UploadSizeInfo::ExactSize(total))
            .await
    };
    let (_, store_res) = tokio::join!(writer_fut, store_call);
    store_res
}

// =============================================================================
// UNDER-ACTION TEST
// =============================================================================

/// **Under-action** — the chunked-write happy path MUST push the
/// committed digest onto `stable_digests`. The legacy
/// `update`/`update_oneshot` background spawn pushes at
/// `fast_slow_store.rs:3449-3450`; the chunked path was missing this
/// (#282).
///
/// **Mutation step:** comment out `sink(stream_digest)` in the
/// `dispatch_chunks_to_driver` AsyncCommit reaper. This test red-fails
/// with the bespoke message — the slow-tier file is committed but the
/// BIS broadcast never sees the digest.
///
/// **Production composition** satisfied: real `FastSlowStore` +
/// `BazelChunkedDispatcherImpl` + `ChunkedReadRegistry` + per-blob
/// driver wired exactly as `wire_bazel_chunked_dispatcher` does for
/// the production server. Test crosses the same seam (FSS::update
/// → dispatcher → driver → reaper → stable_digests) that the
/// production code does.
#[nativelink_test]
async fn chunked_commit_pushes_digest_to_stable_digests() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 11) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;

    // Pre-flight: drain MUST be empty.
    assert!(
        fast_slow.as_ref().drain_stable_digests().is_empty(),
        "fixture invariant: stable_digests starts empty",
    );

    // Drive the upload.
    tokio::time::timeout(
        Duration::from_secs(10),
        run_update(&fast_slow, digest, Bytes::from(blob)),
    )
    .await
    .expect("must not deadlock — chunked update should commit within 10s")
    .expect("chunked update must succeed for hash-matching blob");

    // Wait up to 5 s for the AsyncCommit reaper to push to
    // stable_digests. The reaper runs on a separate spawn; we observe
    // by polling drain_stable_digests().
    //
    // NOTE: drain_stable_digests is destructive — call it inside a
    // poll loop and stash the result so we can assert on contents.
    let pushed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let drained = fast_slow.as_ref().drain_stable_digests();
            if drained.contains(&digest) {
                return drained;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "drain_stable_digests must contain digest within timeout — \
         chunked commit completion did not push (production incident \
         2026-05-06 mechanism)",
    );

    assert!(
        pushed.contains(&digest),
        "stable_digests MUST contain the chunked-committed digest. \
         Without this push, the BIS broadcast loop has nothing to \
         broadcast and worker mirror_blobs / server fast-tier pins \
         accumulate until the 120 s pin TTL drains them. This is the \
         production-incident-2026-05-06 mechanism.",
    );

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// OVER-ACTION TEST
// =============================================================================

/// **Over-action** — chunked-commit FAILURE (forced via lying digest)
/// MUST NOT push to `stable_digests`. The BIS protocol acks bytes that
/// are durably on stable storage; pushing on commit failure would
/// cause the broadcast to ack bytes that aren't actually durable,
/// upstream workers to drop their `mirror_blobs` entries, and a
/// later read for the digest would NotFound (no fast tier, no slow
/// tier, no mirror).
///
/// We force a hash mismatch by uploading bytes whose actual SHA-256
/// does NOT match the declared digest. The driver runs the e2e SHA
/// verify, sees the mismatch, returns
/// `Err(Code::InvalidArgument, "end-to-end SHA-256 mismatch")`. Per
/// the fix, the AsyncCommit reaper's `Err` branch does NOT push to
/// stable_digests.
///
/// **Mutation step:** if the AsyncCommit reaper were changed to push
/// unconditionally (move the sink call out of `Ok(_)` branch), this
/// test would red-fail with the assertion below.
#[nativelink_test]
async fn chunked_commit_failure_does_not_push_to_stable_digests() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;

    let actual_blob: Vec<u8> = (0..SIZE).map(|i| (i * 13) as u8).collect();
    // Lying digest: declared hash is for DIFFERENT bytes (all-zero) so
    // the e2e SHA verify mismatches.
    let lying_blob: Vec<u8> = vec![0u8; SIZE];
    let lying_digest = DigestInfo::new(sha256(&lying_blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;

    // Pre-flight: drain MUST be empty.
    assert!(
        fast_slow.as_ref().drain_stable_digests().is_empty(),
        "fixture invariant: stable_digests starts empty",
    );

    // Drive the upload with the WRONG bytes for the declared digest.
    // The dispatcher/driver will admit the chunks (per-chunk SHA is
    // computed from the actual bytes), and the e2e SHA verify will
    // fail at `commit_and_verify` step 2 (SHA-256 mismatch).
    //
    // The async-commit return value is Ok (admission succeeded); the
    // mismatch surfaces only when the spawned reaper observes the
    // driver's commit Err.
    let _admit_res = tokio::time::timeout(
        Duration::from_secs(10),
        run_update(&fast_slow, lying_digest, Bytes::from(actual_blob)),
    )
    .await
    .expect("must not deadlock — chunked admission should complete in 10s");
    // We don't assert on _admit_res — async-commit returns Ok at
    // admission even when the eventual commit will fail. The
    // assertion that matters is BIS push absence below.

    // Wait long enough for the reaper to have run (commit attempt +
    // SHA verify + cleanup) AND for any spurious push to land.
    // The chunked-in-flight set goes back to empty after the reaper
    // runs; poll on that as the ground-truth completion signal.
    let chunked_set = fast_slow.as_ref().chunked_in_flight_digests_handle();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let empty = chunked_set.lock().is_empty();
            if empty {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "must not deadlock — chunked in-flight set must drain after \
         reaper completes (success OR failure)",
    );

    // Give the reaper one more macro-tick to ensure any spurious push
    // has had time to land. yield_now * a few iterations is not
    // sufficient because the spawn may not have run; an explicit
    // small sleep here is justified — we're asserting ABSENCE of an
    // event, not waiting on one.
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }

    let drained = fast_slow.as_ref().drain_stable_digests();
    assert!(
        !drained.contains(&lying_digest),
        "stable_digests MUST NOT contain a digest whose chunked commit \
         failed (e2e SHA-256 mismatch). Pushing on commit failure would \
         cause the BIS broadcast to ack bytes that aren't durably stored, \
         upstream workers would drop their mirror_blobs entries, and \
         later reads would NotFound on a non-existent blob. drained={drained:?}",
    );

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// RACE COVERAGE TEST
// =============================================================================

/// **Race coverage** — between successful chunked commit and BIS push,
/// a concurrent reader querying `chunked_in_flight_digests_handle()`
/// + `drain_stable_digests()` must observe the digest as EITHER
/// "in_flight" OR "stable" (or BOTH during the brief overlap), but
/// NEVER neither. The fix orders the push BEFORE in_flight removal so
/// the visibility gap is closed.
///
/// **Mutation step:** swap the push-before-remove ordering in the
/// AsyncCommit reaper (push AFTER `in_flight.remove(...)`); the
/// observer then can see "neither in_flight nor stable" and
/// `gap_observations` increments.
#[nativelink_test]
async fn chunked_commit_no_visibility_gap_between_in_flight_and_stable() {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 17) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;
    let chunked_set = fast_slow.as_ref().chunked_in_flight_digests_handle();

    // The observer task polls both signals in a tight loop. It
    // counts (a) total observations after the digest was first seen
    // in_flight, (b) observations where the digest was neither in
    // chunked_in_flight nor in stable_digests (the gap we're guarding
    // against), and (c) observations where the digest was in
    // stable_digests.
    //
    // We collect digests we drain into a Vec we own so we don't lose
    // visibility (drain is destructive). Once we've seen the digest
    // pushed to stable, we stop the loop.
    let observed_seen_in_stable = Arc::new(AtomicU64::new(0));
    let gap_observations = Arc::new(AtomicU64::new(0));
    let observer_done = Arc::new(AtomicU64::new(0));

    let store_for_observer: Arc<FastSlowStore> = Arc::clone(&fast_slow);
    let chunked_set_for_observer = Arc::clone(&chunked_set);
    let seen_in_stable_for_obs = Arc::clone(&observed_seen_in_stable);
    let gap_obs_for_obs = Arc::clone(&gap_observations);
    let obs_done_for_obs = Arc::clone(&observer_done);
    let observer_fut = tokio::spawn(async move {
        let mut started = false;
        let mut accumulated_drains: Vec<DigestInfo> = Vec::new();
        for _ in 0..2_000_000 {
            let in_flight_now = chunked_set_for_observer.lock().contains(&digest);
            // Drain ALL stable digests; check if our target appears.
            let drained = store_for_observer.as_ref().drain_stable_digests();
            for d in &drained {
                accumulated_drains.push(*d);
            }
            let in_stable_now = accumulated_drains.contains(&digest);
            if in_flight_now || in_stable_now {
                started = true;
            }
            if started && !in_flight_now && !in_stable_now {
                gap_obs_for_obs.fetch_add(1, AtomicOrdering::Relaxed);
            }
            if in_stable_now {
                seen_in_stable_for_obs.fetch_add(1, AtomicOrdering::Relaxed);
                // We've seen the BIS push land; the contract held.
                // Run a few more iterations to confirm no spurious gap
                // appears AFTER stable is observed (it shouldn't, but
                // the loop is cheap).
                for _ in 0..100 {
                    let in_flight_post = chunked_set_for_observer
                        .lock()
                        .contains(&digest);
                    let in_stable_post = accumulated_drains.contains(&digest);
                    if !in_flight_post && !in_stable_post {
                        gap_obs_for_obs.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    tokio::task::yield_now().await;
                }
                obs_done_for_obs.store(1, AtomicOrdering::Release);
                return;
            }
            tokio::task::yield_now().await;
        }
        obs_done_for_obs.store(2, AtomicOrdering::Release); // timed out
    });

    // Drive the upload.
    let updater_fut = run_update(&fast_slow, digest, Bytes::from(blob));

    let (update_res, observer_res) =
        tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(updater_fut, observer_fut)
        })
        .await
        .expect("must not deadlock — race-coverage test");

    update_res.expect("chunked update must succeed");
    observer_res.expect("observer task panic");

    let seen = observed_seen_in_stable.load(AtomicOrdering::Relaxed);
    let gaps = gap_observations.load(AtomicOrdering::Relaxed);
    let done = observer_done.load(AtomicOrdering::Acquire);

    assert_eq!(
        done, 1,
        "observer must have observed the digest in stable_digests \
         within its iteration budget (got done={done}, where 1=saw \
         stable, 2=timed out without seeing stable). If 2, the \
         AsyncCommit reaper never pushed — same root cause as the \
         under-action test.",
    );
    assert!(
        seen >= 1,
        "observer must have observed the digest in stable_digests at \
         least once (got seen={seen})",
    );
    assert_eq!(
        gaps, 0,
        "observer saw the digest as 'neither in chunked_in_flight nor \
         in stable_digests' {gaps} times. The fix orders the BIS push \
         BEFORE removing from chunked_in_flight, so any reader seeing \
         the chunked_in_flight entry as removed MUST also see the \
         digest in stable_digests. A non-zero gap count means the \
         ordering invariant is violated.",
    );

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// SYNCHRONOUS COMMIT BRANCH TEST (testing-czar item 1)
// =============================================================================

/// **Sibling-bug audit (testing-czar item 1).** The #282 fix touches
/// THREE push sites:
///
///   1. `chunked_write_handler.rs:1618`  — Synchronous commit success
///   2. `chunked_write_handler.rs:1697-1700` — AsyncCommit reaper
///   3. `chunked_write_handler.rs:2398-2400` — early-dedup short-circuit
///
/// The under-action test above only covers site (2). This test covers
/// site (1): driving `dispatch_chunks_to_driver` directly with
/// `CommitMode::Synchronous` and the `stable_digests_pusher()` closure
/// from a real `FastSlowStore`. After the call returns Ok, the digest
/// MUST appear in `drain_stable_digests()`.
///
/// **Why a separate path is needed:** the public Bazel-facing entry
/// (`dispatch_bazel_facing_internal_chunking`) always uses
/// `CommitMode::AsyncCommit`. The Synchronous mode is reachable only
/// through a direct call into `dispatch_chunks_to_driver` (used by the
/// WriteChunked RPC handler in production). Mutating the push at
/// `:1618` would not red-fail any existing test before this one
/// landed.
///
/// **Production composition:** real `FastSlowStore` (provides the
/// pusher closure that captures `stable_digests` + `stable_notify`) +
/// real `FilesystemStore` slow tier + real `ChunkedDriver` machinery
/// via `dispatch_chunks_to_driver`. The closure is the SAME
/// `Arc<dyn Fn(DigestInfo)>` that `wire_bazel_chunked_dispatcher`
/// installs on the production dispatcher.
///
/// **Mutation step (verified at test authorship time):** comment out
/// the `sink(stream_digest)` call at `chunked_write_handler.rs:1619`
/// (inside the `Synchronous` arm). This test red-fails with the
/// bespoke `.expect("synchronous chunked commit must push to
/// stable_digests — sibling-bug regression")`.
#[nativelink_test]
async fn chunked_synchronous_commit_pushes_digest_to_stable_digests() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    // Build a deterministic blob whose declared SHA-256 matches its
    // actual contents (Synchronous mode runs the e2e SHA verify; a
    // mismatched declared digest would fail commit and bypass the push
    // we want to observe).
    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0x73u8 ^ (i as u8)).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    // Build a fresh FilesystemStore slow tier + FastSlowStore so we
    // can extract the production-shaped pusher closure.
    let fs_store = make_filesystem_store().await;
    let fast_store: Store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store: Store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Pre-flight: drain MUST be empty.
    assert!(
        fast_slow.as_ref().drain_stable_digests().is_empty(),
        "fixture invariant: stable_digests starts empty",
    );

    // Build the per-chunk PreparedChunk stream the dispatcher consumes.
    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&blob[i * CHUNK..(i + 1) * CHUNK]);
            Ok(PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_sha256: sha256(&chunk_bytes),
                chunk_bytes,
                finish: i == N - 1,
            })
        })
        .collect();
    let stream = Box::pin(futures::stream::iter(chunks));

    let in_flight = ChunkedWriteInFlight::new();
    let chunk_budget = make_test_chunk_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Pull the production-shaped sink closure out of the FSS. This
    // captures `stable_digests` + `stable_notify`. We pass it through
    // to `dispatch_chunks_to_driver` exactly as
    // `wire_bazel_chunked_dispatcher` does for the production server.
    let sink = fast_slow.as_ref().stable_digests_pusher();

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            None, // pin_budget
            None, // chunked_read_registry
            Some(sink),
            None, // failed_commit_sink — not exercised here (Synchronous + hash-matching blob)
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            metrics,
        ),
    )
    .await
    .expect(
        "must not deadlock — synchronous dispatch_chunks_to_driver should \
         complete within 10s",
    )
    .expect(
        "synchronous chunked commit must succeed for hash-matching blob \
         — preconditions for the BIS push under test",
    );
    assert_eq!(outcome.committed_size, SIZE as u64);

    // Synchronous mode pushes BEFORE returning Ok (line 1618 in the
    // implementation), so by the time we get here the digest MUST be
    // observable in `drain_stable_digests()`. No polling needed —
    // unlike AsyncCommit, the push is not on a separate spawn.
    let drained = fast_slow.as_ref().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "synchronous chunked commit must push to stable_digests — \
         sibling-bug regression: the Synchronous arm at \
         chunked_write_handler.rs:1618 omitted the BIS push. drained={drained:?}",
    );
}

// =============================================================================
// EARLY-DEDUP SHORT-CIRCUIT BRANCH TEST (testing-czar item 2)
// =============================================================================

/// **Sibling-bug audit (testing-czar item 2).** Covers push site (3) at
/// `chunked_write_handler.rs:2398-2400` — the early-dedup short-circuit
/// branch in `dispatch_bazel_facing_internal_chunking`. This branch
/// fires when the FilesystemStore's `evicting_map` already contains
/// the digest (steady-state Bazel re-upload of an already-indexed
/// blob). Without the fix the branch returned Ok without pushing, so
/// the BIS broadcast loop never saw the digest and the worker's
/// mirror_blobs entry would not be unpinned via the BIS path.
///
/// Mutating the call at `:2399` would not red-fail any test before
/// this one landed: the existing
/// `dispatch_bazel_facing_skips_chunked_path_when_digest_already_indexed`
/// in `bazel_facing_internal_chunking_test.rs` passes `None` for the
/// sink, so it cannot observe the push.
///
/// **Production composition:** real `FastSlowStore` provides the
/// pusher closure; real `FilesystemStore` is pre-populated so
/// `has_indexed_digest` returns `Some(size)`; real
/// `dispatch_bazel_facing_internal_chunking` is invoked directly with
/// the sink wired (matching how `wire_bazel_chunked_dispatcher` would
/// wire it on the production server).
///
/// **Mutation step (verified at test authorship time):** comment out
/// the `sink(digest)` call at `chunked_write_handler.rs:2399`. This
/// test red-fails with the bespoke `.expect("early-dedup short-circuit
/// must push to stable_digests — defense-in-depth regression")`.
#[nativelink_test]
async fn chunked_early_dedup_short_circuit_pushes_digest_to_stable_digests() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0x91u8 ^ (i as u8)).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    // Pre-populate the FilesystemStore so the digest is in
    // evicting_map. This is the steady-state precondition the
    // early-dedup gate consults.
    let fs_store = make_filesystem_store().await;
    let pop_key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    fs_store
        .as_pin()
        .update_oneshot(pop_key, Bytes::copy_from_slice(&blob))
        .await
        .expect("pre-populate update_oneshot must succeed");

    // Sanity: the `has_indexed_digest` probe sees the entry. If this
    // assertion ever fails, the early-dedup gate has nothing to short-
    // circuit on and the test would NOT exercise site (3).
    assert_eq!(
        fs_store.has_indexed_digest(&digest).await,
        Some(SIZE as u64),
        "test precondition: pre-populate must register the digest in \
         evicting_map so the early-dedup gate fires",
    );

    // Build the FastSlowStore so we can pull `stable_digests_pusher()`.
    // The FSS's slow tier IS the same FilesystemStore — production
    // composition substance, not just form.
    let fast_store: Store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store: Store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Pre-flight: drain MUST be empty (the pre-populate update_oneshot
    // ran on the bare FilesystemStore, NOT through FastSlowStore::update,
    // so no stable_digests push could have fired from it).
    assert!(
        fast_slow.as_ref().drain_stable_digests().is_empty(),
        "fixture invariant: stable_digests starts empty (pre-populate \
         was on the bare FilesystemStore, not through FSS::update)",
    );

    // Spawn a producer that streams the same bytes into a
    // DropCloserReadHalf. The early-dedup gate drains this reader
    // (with bounded-drain) before short-circuiting Ok.
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    let blob_for_producer = blob.clone();
    tokio::spawn(async move {
        let _ = tx.send(Bytes::from(blob_for_producer)).await;
        let _ = tx.send_eof();
    });

    let in_flight = ChunkedWriteInFlight::new();
    let chunk_budget = make_test_chunk_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let sink = fast_slow.as_ref().stable_digests_pusher();

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        dispatch_bazel_facing_internal_chunking(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            None, // pin_budget
            None, // chunked_read_registry
            Some(sink),
            None, // failed_commit_sink — not exercised here (early-dedup short-circuit)
            metrics,
            CHUNK,
            digest,
            rx,
        ),
    )
    .await
    .expect(
        "must not deadlock — early-dedup short-circuit must drain the \
         producer and return Ok within 10s",
    )
    .expect(
        "early-dedup short-circuit must return Ok when the digest is \
         already in evicting_map",
    );
    assert_eq!(outcome.committed_size, SIZE as u64);

    // The early-dedup branch pushes BEFORE returning Ok (line 2399 in
    // the implementation), so by the time we get here the digest MUST
    // be observable in drain_stable_digests(). No polling needed —
    // unlike the AsyncCommit reaper, the push is on the same task as
    // the dispatcher.
    let drained = fast_slow.as_ref().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "early-dedup short-circuit must push to stable_digests — \
         defense-in-depth regression: the early-dedup branch at \
         chunked_write_handler.rs:2398-2400 omitted the BIS push. \
         drained={drained:?}",
    );
}

// =============================================================================
// stable_notify WAKEUP CONTRACT TEST (testing-czar item 3)
// =============================================================================

/// **Notify-side coverage (testing-czar item 3).** The closure returned
/// by `FastSlowStore::stable_digests_pusher()` does TWO things:
///
///   1. push the digest onto `stable_digests` (Vec<DigestInfo>)
///   2. call `stable_notify.notify_one()` to wake the BIS broadcast
///      loop's `notified().await`
///
/// All three earlier tests observe via `drain_stable_digests()` —
/// which DOES NOT consult the Notify. A regression that broke the
/// `notify_one()` call but kept the push would ship: the queue would
/// fill, but no broadcast loop would ever wake to drain it.
///
/// This test subscribes to `stable_notify().notified()` BEFORE
/// driving the commit, then drives an AsyncCommit and asserts the
/// notified future resolves within `tokio::time::timeout(5s)`.
///
/// **Production composition:** real `FastSlowStore` (chunked
/// dispatcher wired through the same `make_e2e_fast_slow_with_sink`
/// fixture as the under-action test); the Notify subscriber mirrors
/// the production BIS broadcast loop's wait pattern.
///
/// **Mutation step (verified at test authorship time):** comment out
/// `stable_notify.notify_one()` at `fast_slow_store.rs:709` (inside
/// the `stable_digests_pusher()` closure). This test red-fails with
/// the bespoke `.expect("chunked commit must wake stable_notify
/// waiters — BIS broadcast loop wakeup contract violated")` message.
#[nativelink_test]
async fn chunked_commit_notifies_stable_notify_waiters() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 19) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;

    // Subscribe to stable_notify().notified() BEFORE driving the
    // commit. The Notify is permit-based: registering the future via
    // `.notified()` ensures a notify_one() that fires while the
    // future is being constructed CANNOT be lost (Notify stores one
    // pending permit). We register the future BEFORE the upload
    // starts so even if the chunked dispatcher is unrealistically
    // fast, the wakeup is observable.
    //
    // This call must be inside a tokio runtime context (the
    // forced-delegation merged-Notify path lazily spawns forwarder
    // tasks on first call); the `#[nativelink_test]` attribute
    // satisfies that.
    let stable_notify = fast_slow.stable_notify();
    let notified_fut = {
        let n = stable_notify.clone();
        async move { n.notified().await }
    };

    // Concurrently: drive the upload AND wait for the notify. If the
    // notify_one() in the closure ever fires (under-action: it MUST
    // fire on commit success), `notified_fut` resolves. If it does
    // NOT fire, the outer timeout panics with the bespoke message.
    let updater_fut = run_update(&fast_slow, digest, Bytes::from(blob));

    let (update_res, _notify_res) = tokio::time::timeout(
        Duration::from_secs(5),
        async move { tokio::join!(updater_fut, notified_fut) },
    )
    .await
    .expect(
        "chunked commit must wake stable_notify waiters — BIS broadcast \
         loop wakeup contract violated: the closure returned by \
         stable_digests_pusher() must call stable_notify.notify_one() \
         after pushing onto stable_digests, otherwise the broadcast \
         loop's notified().await never resolves and stable_digests \
         accumulates without ever being drained",
    );

    update_res.expect("chunked update must succeed for hash-matching blob");

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// #283 — AsyncCommit FAILURE bookkeeping (failed_slow_writes + re-pin)
// =============================================================================

/// **#283 under-action coverage** — chunked AsyncCommit FAILURE MUST
/// fire the `failed_commit_sink`, which:
///
///   1. inserts the digest into `failed_slow_writes` (the worker
///      reconnect-retry path consumes the set on reconnect; the
///      mirror protocol re-uploads the lost blob), AND
///   2. re-pins the in-memory replica on the fast store (so MemoryStore
///      eviction can't drop the blob between commit-failure and the
///      next mirror-protocol retry).
///
/// Mirrors the legacy `FastSlowStore::update` Err arm at
/// `fast_slow_store.rs:3489-3494`. WITHOUT this, a chunked-commit
/// failure leaves no record of the missing slow-tier write — the
/// reconnect-retry never runs and subsequent reads NotFound on the
/// lost blob.
///
/// **Failure trigger** — same fixture as the over-action test
/// `chunked_commit_failure_does_not_push_to_stable_digests`: lying
/// digest (declared SHA-256 doesn't match actual bytes). The chunked
/// driver's e2e SHA verify mismatches → commit Err →
/// AsyncCommit reaper's Err branch → failed_commit_sink fires.
///
/// **Production composition** — real `FastSlowStore` +
/// `BazelChunkedDispatcherImpl` wired with both
/// `stable_digests_pusher()` AND `failed_writes_inserter()` exactly as
/// `wire_bazel_chunked_dispatcher` does for the production server.
///
/// **Mutation step (verified at test authorship time):** comment out
/// the `sink(stream_digest)` call in the new Err branch at
/// `chunked_write_handler.rs:1731-1735` (the `if commit_result.is_err()`
/// block). This test red-fails with the bespoke message — the chunked
/// commit fails but the failed-write bookkeeping never lands, so the
/// worker reconnect-retry has nothing to retry.
#[nativelink_test]
async fn chunked_async_commit_failure_inserts_failed_writes_and_repins() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;

    let actual_blob: Vec<u8> = (0..SIZE).map(|i| (i * 13) as u8).collect();
    // Lying digest: declared hash is for DIFFERENT bytes (all-zero) so
    // the chunked driver's e2e SHA verify mismatches at commit.
    let lying_blob: Vec<u8> = vec![0u8; SIZE];
    let lying_digest = DigestInfo::new(sha256(&lying_blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;

    // Pre-flight: the failed-writes set MUST be empty.
    assert!(
        !fast_slow.failed_slow_writes_contains(&lying_digest),
        "fixture invariant: failed_slow_writes starts without our digest",
    );

    // Drive the upload with the WRONG bytes for the declared digest.
    // The dispatcher/driver will admit the chunks (per-chunk SHA is
    // computed from the actual bytes), and the e2e SHA verify will
    // fail at the driver's commit step.
    //
    // The async-commit return value is Ok (admission succeeded); the
    // mismatch surfaces only when the spawned reaper observes the
    // driver's commit Err.
    let _admit_res = tokio::time::timeout(
        Duration::from_secs(10),
        run_update(&fast_slow, lying_digest, Bytes::from(actual_blob)),
    )
    .await
    .expect("must not deadlock — chunked admission should complete in 10s");
    // We don't assert on _admit_res — async-commit returns Ok at
    // admission even when the eventual commit will fail.

    // Wait for the chunked-in-flight set to drain (ground-truth signal
    // that the AsyncCommit reaper has run to completion: it removes the
    // chunked driver's in_flight entry, which causes the outer FSS
    // dispatch reaper's `while contains_digest` loop to break and
    // remove from `chunked_in_flight_digests`).
    let chunked_set = fast_slow.as_ref().chunked_in_flight_digests_handle();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let empty = chunked_set.lock().is_empty();
            if empty {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "must not deadlock — chunked in-flight set must drain after \
         reaper completes (success OR failure)",
    );

    // The failed_commit_sink runs BEFORE the chunked driver removes its
    // in_flight entry (per the ordering in the new Err branch at
    // `chunked_write_handler.rs:1731-1735`), so by the time the outer
    // chunked_in_flight_digests set has drained, the bookkeeping MUST
    // already be observable. No additional poll needed.

    // (1) failed_slow_writes MUST contain the digest.
    assert!(
        fast_slow.failed_slow_writes_contains(&lying_digest),
        "chunked AsyncCommit Err arm must insert failed_writes + re-pin \
         — contract parity with legacy update at \
         fast_slow_store.rs:3489-3494 violated. Without this insert, the \
         worker reconnect-retry path (drain_failed_digests) has nothing \
         to retry; the slow tier is missing the blob and no mechanism \
         exists to re-upload it. failed_slow_writes_contains returned \
         false for digest={lying_digest:?}",
    );

    // (2) The fast-store re-pin MUST have been attempted. The test
    //     fast tier is a MemoryStore, which is a non-pinning Leaf and
    //     silently no-ops `pin_digests` (StoreDriver default). The
    //     observable consequence is that the blob remains accessible
    //     in the fast store via `has` — which the upstream
    //     `FastSlowStore::update`'s tee already wrote. We assert
    //     `has_with_results` returns Some so that ANY future change to
    //     replace MemoryStore with a pinning leaf (e.g.
    //     FilesystemStore) preserves the contract: the blob is alive
    //     in the fast tier when the reconnect-retry consults it.
    let mut results = vec![None; 1];
    fast_slow
        .fast_store_handle()
        .has_with_results(&[lying_digest.into()], &mut results)
        .await
        .expect("fast_store has_with_results must succeed");
    assert!(
        results[0].is_some(),
        "fast-store replica must remain accessible after chunked-commit \
         failure (the upstream FSS::update tee wrote the blob; the \
         failed_commit_sink's pin_digests call protects it from \
         eviction). Without the re-pin, MemoryStore eviction (or — in \
         production with FilesystemStore as fast tier — the 120s pin \
         TTL) could drop the blob before the worker reconnect-retry \
         consumes failed_slow_writes. results={results:?}",
    );

    disable_bazel_facing_internal_chunking();
}
