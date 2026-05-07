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

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreSpec};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_service::chunked_write_handler::{
    BazelChunkedDispatcherImpl, CHUNKED_COMMIT_WATCHDOG_SECS, ChunkedWriteHandlerMetrics,
    ChunkedWriteInFlight, CommitMode, PreparedChunk, dispatch_bazel_facing_internal_chunking,
    dispatch_chunks_to_driver, run_async_commit_reaper,
};
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{
    ChunkedCommitResult, ChunkedDriver, PER_BLOB_MPSC_CAP,
};
use nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry;
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, disable_bazel_facing_internal_chunking,
    enable_bazel_facing_internal_chunking,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, make_buf_channel_pair_with_size};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
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

// =============================================================================
// #283 fixup MAJOR-3 — pin-observable fast tier
// =============================================================================
//
// `PinCountingFastStore` wraps a `MemoryStore` and counts every
// `pin_digests` invocation. The earlier `chunked_async_commit_failure_*`
// test asserted re-pin via `has_with_results`, which is satisfied by
// the upstream `FastSlowStore::update`'s tee into the fast tier
// REGARDLESS of whether `failed_commit_sink` (and therefore the
// `pin_digests` call inside `failed_writes_inserter`) ran. Commenting
// out `fast_store.pin_digests(&[d])` in `failed_writes_inserter`
// (`fast_slow_store.rs:750`) would NOT red-fail that test — the re-pin
// half of the contract was unguarded. The wrapper below directly
// observes the pin call so the mutation step actually red-fails when
// the sink call is removed.
//
// MemoryStore declares `PinDelegation::Leaf`; default `pin_digests`
// is a silent no-op. We override it to increment the counter; the
// rest of the trait is forwarded to the inner `MemoryStore`.

#[derive(MetricsComponent)]
struct PinCountingFastStore {
    inner: Arc<MemoryStore>,
    pin_calls: Arc<AtomicU64>,
}

#[async_trait]
impl StoreDriver for PinCountingFastStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(digests, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .update(digest, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        // Leaf — we provide our own (counting) `pin_digests` impl
        // below. Declaring `Leaf` matches MemoryStore's classification
        // and routes `Store::pin_digests` straight into our override
        // (rather than recursing into the inner store).
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }

    /// Override of the default `pin_digests`: count every call so the
    /// test can assert the `failed_commit_sink` actually invoked it.
    /// Without this override, `PinDelegation::Leaf` would route to the
    /// trait default (no-op for non-pinning leaves) and we'd be no
    /// better than the bare MemoryStore.
    fn pin_digests(&self, digests: &[DigestInfo]) {
        self.pin_calls
            .fetch_add(digests.len() as u64, AtomicOrdering::Relaxed);
    }
}

default_health_status_indicator!(PinCountingFastStore);

/// Production-composition fixture variant: identical to
/// `make_e2e_fast_slow_with_sink` except the fast tier is a
/// `PinCountingFastStore` instead of a bare `MemoryStore`. Returns
/// the `(FastSlowStore, pin_calls counter)` so tests can observe pin
/// invocations directly.
async fn make_e2e_fast_slow_with_sink_and_pin_counter(
    chunk_size: usize,
) -> (Arc<FastSlowStore>, Arc<AtomicU64>) {
    let fs_store = make_filesystem_store().await;
    let pin_calls = Arc::new(AtomicU64::new(0));
    let counting_fast = Arc::new(PinCountingFastStore {
        inner: MemoryStore::new(&MemorySpec::default()),
        pin_calls: Arc::clone(&pin_calls),
    });
    let fast_store: Store = Store::new(counting_fast);
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
        .with_stable_digests_sink(fast_slow.stable_digests_pusher())
        .with_failed_commit_sink(fast_slow.failed_writes_inserter()),
    );
    fast_slow.set_chunked_read_registry(Arc::clone(&registry));
    fast_slow
        .set_bazel_chunked_dispatcher(Arc::clone(&dispatcher) as Arc<dyn BazelChunkedDispatcher>);
    fast_slow.set_chunked_size_threshold_for_test(chunk_size as u64);

    (fast_slow, pin_calls)
}

/// **#283 fixup MAJOR-3** — direct re-pin observation for the
/// AsyncCommit failure path. Replaces the indirect `has_with_results`
/// assertion that the original test used (which was satisfied by the
/// FSS::update tee regardless of whether `pin_digests` ran).
///
/// **Mutation step (verified at test authorship time):** comment out
/// the `fast_store.pin_digests(&[digest])` call in
/// `FastSlowStore::failed_writes_inserter` (`fast_slow_store.rs:750`).
/// This test red-fails with the bespoke `pin_calls == 0` message; the
/// `failed_slow_writes` insert still fires (so the previous test still
/// passes), confirming this assertion guards the re-pin half of the
/// contract specifically.
#[nativelink_test]
async fn chunked_async_commit_failure_actually_calls_pin_digests() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;

    let actual_blob: Vec<u8> = (0..SIZE).map(|i| (i * 13) as u8).collect();
    let lying_blob: Vec<u8> = vec![0u8; SIZE];
    let lying_digest = DigestInfo::new(sha256(&lying_blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let (fast_slow, pin_calls) = make_e2e_fast_slow_with_sink_and_pin_counter(CHUNK).await;

    let _admit_res = tokio::time::timeout(
        Duration::from_secs(10),
        run_update(&fast_slow, lying_digest, Bytes::from(actual_blob)),
    )
    .await
    .expect("must not deadlock — chunked admission should complete in 10s");

    // Wait for chunked in-flight set to drain (reaper completion signal).
    let chunked_set = fast_slow.as_ref().chunked_in_flight_digests_handle();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if chunked_set.lock().is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — chunked in-flight set must drain after reaper");

    // failed_slow_writes insert fired → upstream `failed_writes_inserter`
    // closure ran. The closure also calls `fast_store.pin_digests(&[d])`;
    // PinCountingFastStore counts every such invocation.
    assert!(
        fast_slow.failed_slow_writes_contains(&lying_digest),
        "precondition for the pin-call assertion: the failed_commit_sink \
         closure must have run (insert must be observable). If THIS \
         assertion fails, the AsyncCommit reaper Err arm regressed — \
         see chunked_async_commit_failure_inserts_failed_writes_and_repins.",
    );

    // Expected pin_calls breakdown for AsyncCommit FAILURE path:
    //   1. update_via_chunked_dispatcher post-admission pin
    //      (fast_slow_store.rs:904) — fires once after admission.
    //   2. failed_commit_sink's pin_digests inside
    //      failed_writes_inserter (fast_slow_store.rs:750) — fires
    //      from the reaper Err arm. THIS is the call the test guards.
    //
    // Total >= 2 with the fix; == 1 if the sink's pin_digests is
    // removed (mutation step), so the assertion red-fails directly.
    let pin_calls_after_reaper = pin_calls.load(AtomicOrdering::Relaxed);
    assert!(
        pin_calls_after_reaper >= 2,
        "fast_store.pin_digests MUST be called BOTH by the post-admission \
         path in update_via_chunked_dispatcher (fast_slow_store.rs:904) \
         AND by the failed_commit_sink closure when the AsyncCommit \
         reaper observes commit FAILURE (fast_slow_store.rs:750 inside \
         failed_writes_inserter). Total expected: >= 2. Got: \
         {pin_calls_after_reaper}. If == 1, only the post-admission pin \
         fired — the failed_commit_sink's re-pin (which protects the \
         in-memory replica from eviction between commit-failure and the \
         next reconnect-retry) is missing. Without it, MemoryStore \
         eviction (or, in production with FilesystemStore fast tier, \
         the 120s pin TTL) could drop the blob before \
         drain_failed_digests fires, undoing the failed_slow_writes \
         insert's recovery purpose.",
    );

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// #283 fixup MAJOR-1 — Synchronous-arm failure bookkeeping
// =============================================================================

/// **#283 fixup MAJOR-1** — chunked Synchronous commit FAILURE MUST
/// fire the `failed_commit_sink`. The Synchronous arm is reachable
/// from production via the `WriteChunked` RPC handler. Pre-fix, the
/// Synchronous Err branch returned the error WITHOUT firing the sink
/// — same parity gap that #283 closed for AsyncCommit, just on the
/// sibling code path.
///
/// **Drive path:** `dispatch_chunks_to_driver(CommitMode::Synchronous)`
/// directly with a lying digest (declared SHA-256 ≠ actual bytes).
/// The driver's `await_completion()` returns `Err(InvalidArgument,
/// "end-to-end SHA-256 mismatch")`. Per the fix, the Synchronous arm
/// invokes `failed_commit_sink(stream_digest)` BEFORE the in_flight
/// removal, mirroring the AsyncCommit reaper's ordering.
///
/// **Production composition:** real `FastSlowStore` provides the
/// `failed_writes_inserter()` closure that captures `failed_slow_writes`
/// AND `fast_store` (a `PinCountingFastStore` so we can count
/// `pin_digests` invocations directly). The closure is the SAME shape
/// that `wire_bazel_chunked_dispatcher` installs in production.
///
/// **Mutation step (verified at test authorship time):** comment out
/// the `failed_commit_sink` call inside the new `if commit_result.is_err()`
/// block in the `Synchronous` arm of `dispatch_chunks_to_driver`. This
/// test red-fails with the `failed_slow_writes_contains` assertion AND
/// the `pin_calls > 0` assertion — both halves of the contract are
/// guarded.
#[nativelink_test]
async fn chunked_synchronous_commit_failure_inserts_failed_writes_and_repins() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    // Lying digest: declared hash is for all-zero bytes; we'll feed
    // chunks whose actual content is non-zero. Per-chunk SHA is
    // computed from the actual bytes (so admission succeeds); the
    // driver's e2e SHA verify mismatches at commit.
    let actual_blob: Vec<u8> = (0..SIZE).map(|i| (i * 19) as u8).collect();
    let lying_blob: Vec<u8> = vec![0u8; SIZE];
    let lying_digest = DigestInfo::new(sha256(&lying_blob), SIZE as u64);

    // Build a fresh FilesystemStore slow tier + FastSlowStore (with
    // the PinCountingFastStore as the fast tier) so we can pull the
    // production-shaped `failed_writes_inserter()` closure.
    let fs_store = make_filesystem_store().await;
    let pin_calls = Arc::new(AtomicU64::new(0));
    let counting_fast = Arc::new(PinCountingFastStore {
        inner: MemoryStore::new(&MemorySpec::default()),
        pin_calls: Arc::clone(&pin_calls),
    });
    let fast_store: Store = Store::new(counting_fast);
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

    // Pre-flight: failed-writes empty, no pin calls yet.
    assert!(
        !fast_slow.failed_slow_writes_contains(&lying_digest),
        "fixture invariant: failed_slow_writes starts empty",
    );
    assert_eq!(
        pin_calls.load(AtomicOrdering::Relaxed),
        0,
        "fixture invariant: no pin_digests calls before the commit attempt",
    );

    // Build per-chunk PreparedChunks from the ACTUAL bytes (so each
    // chunk's per-chunk SHA matches what the driver computes and
    // admits). Only the e2e SHA verify at commit will mismatch.
    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&actual_blob[i * CHUNK..(i + 1) * CHUNK]);
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

    // Pull the production-shaped failed-commit closure out of the FSS.
    // Same shape `wire_bazel_chunked_dispatcher` installs.
    let failed_sink = fast_slow.as_ref().failed_writes_inserter();
    let stable_sink = fast_slow.as_ref().stable_digests_pusher();

    // Drive Synchronous commit. The driver's e2e verify mismatches and
    // returns Err; the Synchronous arm's new `if commit_result.is_err()`
    // block fires `failed_sink(lying_digest)` BEFORE the in_flight
    // removal, then propagates the Err via `return Err(err)`.
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            None, // pin_budget
            None, // chunked_read_registry
            Some(stable_sink),
            Some(failed_sink),
            CHUNK,
            lying_digest,
            stream,
            CommitMode::Synchronous,
            metrics,
        ),
    )
    .await
    .expect(
        "must not deadlock — Synchronous dispatch_chunks_to_driver must \
         complete (with Err) within 10s",
    );

    assert!(
        res.is_err(),
        "Synchronous commit MUST return Err for a lying digest; got \
         Ok({res:?}) — the e2e SHA-256 verify failed to fire",
    );

    // (1) failed_slow_writes MUST contain the digest (under-action of
    //     the `failed_slow_writes.insert(...)` half of the closure).
    assert!(
        fast_slow.failed_slow_writes_contains(&lying_digest),
        "Synchronous chunked-commit Err arm MUST insert into \
         failed_slow_writes — sibling-bug parity with the AsyncCommit \
         Err arm at chunked_write_handler.rs:1745-1749 violated. The \
         Synchronous arm at :1604-1620 returned Err WITHOUT firing \
         failed_commit_sink before this fix landed; the WriteChunked \
         RPC path would lose track of failed slow-tier writes and the \
         worker reconnect-retry (drain_failed_digests) would have \
         nothing to retry.",
    );

    // (2) fast_store.pin_digests MUST have been called at least once
    //     (under-action of the re-pin half of the closure).
    let pin_calls_after = pin_calls.load(AtomicOrdering::Relaxed);
    assert!(
        pin_calls_after > 0,
        "Synchronous chunked-commit Err arm MUST invoke fast_store.\
         pin_digests via failed_writes_inserter (re-pin half of the \
         contract). Without this, the in-memory replica can be evicted \
         between commit-failure and the next reconnect-retry. \
         pin_calls_after={pin_calls_after}",
    );

    // Ensure the cleanup_guard / in_flight removal happened: the entry
    // for our digest must NOT still be present (the Sync arm removes
    // in_flight AFTER firing the sink in the new code).
    assert!(
        !in_flight.contains_digest(&lying_digest),
        "Synchronous arm MUST remove the in_flight entry after firing \
         the failed_commit_sink (cleanup ordering bug — sink fires, \
         then in_flight is removed, then Err is returned).",
    );
}

// =============================================================================
// #283 SUB-ITEM 3 (WATCHDOG) TEST
// =============================================================================
//
// Red-team finding for the 2026-05-06 production cap-exhaustion: even
// after #283 sub-items 1+2 close the missing-`failed_commit_sink` path
// for natural commit-Err, a wedged slow tier (ZFS lock-up, kernel I/O
// hang) can still reach the same end-state — pins past the 120 s
// `chunked_in_flight_digests` TTL — by stalling
// `ChunkedDriver::await_completion()` indefinitely. The legacy
// `update`/`update_oneshot` background spawn at
// `fast_slow_store.rs:3491-3510` guards against this with
// `SLOW_WRITE_WATCHDOG_SECS=60`; sub-item 3 mirrors that guard onto
// the chunked path via `CHUNKED_COMMIT_WATCHDOG_SECS=60`.
//
// The test exercises `run_async_commit_reaper` (extracted from the
// dispatcher's AsyncCommit branch) directly, with a deliberately-
// wedged `ChunkedDriver`: the test holds the per-blob mpsc sender
// alive for the duration, so the driver's `rx.recv().await` blocks
// forever and `await_completion()` never returns. Without the
// watchdog wrapper, the reaper task would also block forever — a
// `tokio::time::timeout` outer guard would catch the deadlock.
//
// We use `tokio::time::pause()` + `start_paused = true` to advance
// virtual time past the watchdog deadline without burning real
// wall-clock seconds.

/// **#283 sub-item 3 (watchdog) — under-action.** When
/// `await_completion()` does not return within
/// `CHUNKED_COMMIT_WATCHDOG_SECS`, the watchdog arm of the
/// AsyncCommit reaper MUST fire `failed_commit_sink(stream_digest)`
/// so the digest is observable in `failed_slow_writes` and the
/// worker's reconnect-retry path picks it up. The
/// `Arc<ChunkedDriver>` parameter goes out of scope at the end of
/// the reaper future, so the `JoinHandleDropGuard` aborts the
/// stalled inner driver task.
///
/// **Drive path:** call `run_async_commit_reaper` directly with a
/// real `ChunkedDriver` (constructed via `spawn_driver`) whose
/// per-blob mpsc sender is held alive by the test, so the driver's
/// `rx.recv().await` never returns and `await_completion()` blocks
/// forever. The reaper's watchdog `tokio::time::timeout` is the
/// only thing that can unblock the future.
///
/// **Production composition:** real `FastSlowStore` (fast =
/// MemoryStore, slow = FilesystemStore) provides the
/// `failed_writes_inserter()` closure that's wired in production
/// via `wire_bazel_chunked_dispatcher`. The watchdog arm fires that
/// EXACT closure shape; we observe via
/// `fast_slow.failed_slow_writes_contains(&digest)`.
///
/// **Mutation step (verified at test authorship time):** revert the
/// `tokio::time::timeout(watchdog, driver.await_completion())` in
/// `run_async_commit_reaper` to a bare `driver.await_completion().await`.
/// This test then red-fails — the outer `tokio::time::timeout` deadlock
/// detector trips (the reaper hangs forever waiting on a blocked
/// receiver) — with the bespoke
/// `"chunked commit watchdog must fire on stalled await_completion —
/// pin TTL leak class regression"` message.
///
/// Outer wall-clock guard: even though the test uses paused virtual
/// time internally, a regression that causes a real-time stall
/// (e.g., the watchdog gets compiled-out in a feature combo we
/// didn't anticipate) would manifest as the test running forever in
/// CI. Wrapping the whole test body in a generous wall-clock
/// `tokio::time::timeout` is not possible because tokio's paused
/// timers also drive `tokio::time::timeout`. We rely instead on the
/// poll loop having a virtual-time bound (5 s of virtual time
/// post-watchdog) and the test runner's external bound (`timeout 60`
/// in the cargo invocation).
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn chunked_async_commit_watchdog_fires_on_stalled_completion() {
    // Use a small declared size; the actual size doesn't matter — we
    // never admit any chunks, the recv loop blocks on its very first
    // iteration.
    const SIZE: u64 = 1024;
    let digest = DigestInfo::new(sha256(b"watchdog-test-blob"), SIZE);

    // Production composition: real FilesystemStore + FastSlowStore so
    // the `failed_writes_inserter()` closure goes into the genuine
    // `FastSlowStore::failed_slow_writes` set + invokes pin_digests on
    // the genuine fast store. The closure shape matches what
    // `wire_bazel_chunked_dispatcher` wires in production.
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

    // Pre-flight: failed-set empty.
    assert!(
        !fast_slow.failed_slow_writes_contains(&digest),
        "fixture invariant: failed_slow_writes starts empty",
    );

    // Construct a real ChunkedDriver. We deliberately KEEP the sender
    // alive: the driver's `rx.recv().await` will block on the very
    // first iteration because no work has been admitted, and there is
    // still at least one Sender (us) keeping the mpsc open.
    // Consequently `await_completion()` never returns; the reaper's
    // watchdog wrapper is the only thing that can unblock it.
    let (driver, _sender_held_alive) = ChunkedDriver::spawn_driver(
        Arc::clone(&fs_store),
        digest,
        SIZE,
        // CHUNK size: any reasonable value; we never admit a chunk.
        4 * 1024,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);

    // Empty in_flight map. The reaper's `inner.lock().remove(...)`
    // returns None on a missing entry, which is harmless. The watchdog
    // arm is independent of in_flight entry presence — it fires on
    // timeout regardless.
    let in_flight = ChunkedWriteInFlight::new();

    // Production-shaped failed-commit sink. Closure captures
    // `failed_slow_writes` + the fast store's `pin_digests`; identical
    // shape to `wire_bazel_chunked_dispatcher`.
    let failed_sink = fast_slow.as_ref().failed_writes_inserter();
    let stable_sink = fast_slow.as_ref().stable_digests_pusher();

    // Spawn the reaper. With `start_paused = true`, virtual time is
    // frozen until we explicitly advance it. The reaper enters its
    // `tokio::time::timeout(WATCHDOG, await_completion())` and
    // immediately yields awaiting the inner future + the timer.
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let reaper_handle = tokio::spawn(run_async_commit_reaper(
        Arc::clone(&driver_arc),
        digest,
        Arc::clone(&in_flight),
        None, // chunked_read_registry
        Some(stable_sink),
        Some(failed_sink),
        Arc::clone(&metrics),
        "async",
        None, // result_relay — Async test asserts via in_flight + failed_sink
    ));

    // Yield once so the spawned reaper makes progress past the spawn
    // boundary into the timeout future.
    tokio::task::yield_now().await;

    // Advance virtual time PAST the watchdog deadline. The +5s buffer
    // ensures we cross the boundary cleanly (the inner timeout+driver
    // await race resolves to the timeout side).
    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    // Wait for the reaper to finish, with a generous virtual-time
    // outer bound. The deadlock detector: if the watchdog wrapper is
    // missing, the reaper task hangs forever and this `timeout` trips
    // — virtual time can be advanced past it, OR (if the test runner
    // races us to advance again) the test will simply never exit; the
    // outer cargo `timeout 60` is the wall-clock backstop.
    tokio::time::timeout(Duration::from_secs(10), reaper_handle)
        .await
        .expect(
            "chunked commit watchdog must fire on stalled await_completion — \
             pin TTL leak class regression",
        )
        .expect("reaper task must not panic");

    // Contract: the watchdog arm fires `failed_commit_sink`, which
    // inserts into `failed_slow_writes`. Without the watchdog, the
    // reaper hangs forever and the `await` above trips the deadlock
    // detector instead.
    assert!(
        fast_slow.failed_slow_writes_contains(&digest),
        "watchdog arm MUST insert into failed_slow_writes via the \
         failed_commit_sink closure. Without this, a stalled slow tier \
         leaves no record of the failed commit and the worker's \
         reconnect-retry path never picks up the digest — recreating \
         the 2026-05-06 cap-exhaustion shape via stall instead of via \
         missing-push.",
    );

    // The in_flight set should be empty (it was already empty pre-
    // reaper; the assertion documents that the watchdog arm doesn't
    // accidentally insert anything).
    assert!(
        !in_flight.contains_digest(&digest),
        "watchdog arm MUST NOT leave any residual entry in the chunked \
         in-flight set. Observed in_flight entry post-watchdog suggests \
         the reaper inserted instead of removing.",
    );

    // The commit-failures metric should have been incremented (the
    // watchdog Err arm goes through the same metrics increment as a
    // natural commit-Err).
    let failures =
        metrics.commit_failures_total.load(AtomicOrdering::Relaxed);
    assert!(
        failures >= 1,
        "watchdog arm MUST increment commit_failures_total (natural \
         Err path parity). got={failures}",
    );

    // Drop the driver Arc so the JoinHandleDropGuard inside
    // ChunkedDriver aborts the still-blocked inner driver task. The
    // production reaper does this via the closure's variable scope
    // ending; the test does it explicitly to ensure the test process
    // doesn't leak the driver task into other tests.
    drop(driver_arc);

    // Drop the held sender so the driver's mpsc closes (in case the
    // JoinHandleDropGuard didn't fully tear down before this point).
    drop(_sender_held_alive);
}

// =============================================================================
// #283 SUB-ITEM 3 (WATCHDOG) — OVER-ACTION TEST (testing-czar MAJOR-2 fixup)
// =============================================================================
//
// Asymmetric contract coverage (CLAUDE.md #171 lesson). The under-action
// test (`chunked_async_commit_watchdog_fires_on_stalled_completion`)
// proves the watchdog FIRES on a wedged driver. This test proves it
// DOES NOT fire on a healthy driver that completes inside the budget.
//
// Without this test, a regression that swapped `timeout(WATCHDOG_SECS,
// ...)` for `timeout(0, ...)`, that mis-mapped the `Ok(r) =>` and
// `Err(_) =>` arms, or that fired the watchdog speculatively would
// corrupt healthy commits into `failed_slow_writes`, retriggering the
// 2026-05-06 cap-exhaustion class via spurious-failure inflation
// instead of via stall.

/// **#283 sub-item 3 (watchdog) — over-action.** When the chunked
/// commit completes successfully BEFORE `CHUNKED_COMMIT_WATCHDOG_SECS`,
/// the watchdog arm MUST NOT fire. Specifically:
///
///   1. `failed_commit_sink` MUST NOT be invoked (the digest MUST NOT
///      appear in `failed_slow_writes`).
///   2. `stable_digests_sink` MUST be invoked (the digest MUST appear
///      in `drain_stable_digests`).
///
/// **Drive path:** drive a real chunked update through the production
/// composition (`make_e2e_fast_slow_with_sink` + `run_update`) and
/// observe both sinks within a generous 5s timeout. The chunked
/// dispatcher spawns the AsyncCommit reaper, which awaits
/// `await_completion()` under `tokio::time::timeout(WATCHDOG, ..)`;
/// for a healthy slow tier the inner future resolves Ok long before
/// the watchdog fires, the success arm runs, and the digest lands in
/// `stable_digests`.
///
/// **Production composition:** identical to
/// `chunked_commit_pushes_digest_to_stable_digests` (the under-action
/// test for #282 BIS push) — same `make_e2e_fast_slow_with_sink`
/// helper, same `run_update` driver, same drain-polling pattern. The
/// only difference: this test additionally asserts the over-action
/// invariant (failed_slow_writes EMPTY) — which the under-action test
/// did not bother to verify.
///
/// **Mutation step (verified at test authorship time):** change the
/// `CHUNKED_COMMIT_WATCHDOG_SECS` constant in `chunked_write_handler.rs`
/// from `60` to `0`. The watchdog now fires immediately, treating the
/// healthy commit as a Deadline-Exceeded failure: the digest lands in
/// `failed_slow_writes` (over-action!) and NOT in `stable_digests`.
/// This test red-fails on the `failed_slow_writes_contains == false`
/// assertion with the bespoke `"watchdog MUST NOT fire on a healthy
/// commit"` message. The under-action #282 push test
/// (`chunked_commit_pushes_digest_to_stable_digests`) ALSO red-fails
/// (the success-path push doesn't run because the reaper takes the
/// Err arm), confirming the mutation is the right one.
#[nativelink_test]
async fn chunked_async_commit_watchdog_does_not_fire_on_healthy_commit() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 13) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let fast_slow = make_e2e_fast_slow_with_sink(CHUNK).await;

    // Pre-flight: both sinks empty.
    assert!(
        !fast_slow.failed_slow_writes_contains(&digest),
        "fixture invariant: failed_slow_writes starts empty",
    );
    assert!(
        fast_slow.as_ref().drain_stable_digests().is_empty(),
        "fixture invariant: stable_digests starts empty",
    );

    // Drive the upload through the dispatcher — same path as
    // `chunked_commit_pushes_digest_to_stable_digests` (#282 under-action).
    tokio::time::timeout(
        Duration::from_secs(10),
        run_update(&fast_slow, digest, Bytes::from(blob)),
    )
    .await
    .expect("must not deadlock — chunked update should commit within 10s")
    .expect("chunked update must succeed for hash-matching blob");

    // Wait up to 5s for the AsyncCommit reaper to push the success
    // signal. The reaper completes on a separate spawn; we observe
    // via drain_stable_digests as the under-action test does.
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
         setup invariant: the healthy commit must reach stable_digests, \
         otherwise the test isn't actually exercising the success arm",
    );

    // Contract part 1: stable_digests fires (success path).
    assert!(
        pushed.contains(&digest),
        "healthy commit MUST push to stable_digests (under-action \
         mirror invariant — confirms the reaper actually took the Ok \
         arm before we assert the over-action). pushed={pushed:?}",
    );

    // Contract part 2 (the over-action assertion): failed_slow_writes
    // MUST NOT contain the digest. A regression that fires the watchdog
    // on healthy completion would tag this digest as failed and
    // trigger spurious worker reconnect-retry — recreating the
    // 2026-05-06 cap-exhaustion class via spurious-failure inflation
    // instead of via missing-push. CLAUDE.md asymmetric-coverage rule
    // (#171 lesson): the under-action push-fires test alone does NOT
    // catch a watchdog that ALSO over-fires on success — both
    // directions of the contract must be tested.
    assert!(
        !fast_slow.failed_slow_writes_contains(&digest),
        "watchdog MUST NOT fire on a healthy commit (over-action \
         contract). The digest was tagged in failed_slow_writes even \
         though the driver completed successfully — a regression that \
         would convert healthy commits into spurious worker \
         reconnect-retry storms. Most likely cause: \
         CHUNKED_COMMIT_WATCHDOG_SECS reduced to 0, the timeout's \
         Ok/Err arms swapped, or the watchdog firing speculatively.",
    );

    disable_bazel_facing_internal_chunking();
}

// =============================================================================
// #283 SUB-ITEM 3 (WATCHDOG) — SYNC ARM SIBLING TEST (testing-czar MAJOR-1 fixup)
// =============================================================================
//
// Sibling-bug parity coverage for the Synchronous arm of
// `dispatch_chunks_to_driver`. After the #283-fixup MAJOR-1 detach, the
// Synchronous arm's production code path is:
//
//     tokio::spawn(run_async_commit_reaper(driver, ..., "synchronous", Some(relay_tx)));
//     match relay_rx.await { ... }
//
// — IDENTICAL composition to what this test exercises. This satisfies
// CLAUDE.md "Test in production composition, not in isolation": the
// reaper spawn + relay-await pattern IS the Sync arm's production
// shape now that the watchdog runs on a detached task.
//
// Why a separate test from the Async one: the under-action contract
// for the Sync arm has TWO halves the Async arm doesn't have:
//   1. The relay (`oneshot::Sender<Result<ChunkedCommitResult, Error>>`)
//      MUST forward the watchdog's `Err(DeadlineExceeded)` to the
//      caller's RPC return path. Without this the WriteChunked RPC
//      would hang on `relay_rx.await` forever even though the reaper
//      fired the failed-commit sink correctly.
//   2. The `mode_label="synchronous"` MUST be threaded to the
//      tracing fields (operator dashboards distinguish chunked-watchdog
//      fires by mode; conflating async + sync hides which path is
//      degrading).

/// **#283 sub-item 3 (watchdog) — Sync-arm sibling.** When the
/// Synchronous arm of `dispatch_chunks_to_driver` is wedged on a
/// stalled `await_completion()`, the reaper's watchdog MUST:
///
/// 1. Fire `failed_commit_sink(stream_digest)` so `failed_slow_writes`
///    contains the digest (the WriteChunked RPC's reconnect-retry
///    path requires it).
/// 2. Relay an `Err(Code::DeadlineExceeded)` over the
///    `result_relay` oneshot so the WriteChunked RPC future returns a
///    deterministic Err to the worker (instead of hanging on
///    `relay_rx.await`).
/// 3. Remove the digest from `in_flight` (parity with the Async arm's
///    bookkeeping).
/// 4. Increment `commit_failures_total` (so chunked-watchdog fires
///    are visible in the same metric the Async arm increments).
///
/// **Drive path:** mirror the Sync arm's exact production composition
/// — `tokio::spawn(run_async_commit_reaper(..., "synchronous",
/// Some(relay_tx)))` followed by `relay_rx.await`. The driver's
/// per-blob mpsc sender is held alive by the test, so the driver's
/// `rx.recv().await` never returns and `await_completion()` blocks
/// forever; the reaper's watchdog is the only thing that can unblock
/// the future.
///
/// **Production composition:** real `FastSlowStore` (fast =
/// MemoryStore, slow = FilesystemStore) provides the
/// `failed_writes_inserter()` closure that's wired in production via
/// `wire_bazel_chunked_dispatcher`. The watchdog arm fires that EXACT
/// closure shape; we observe via
/// `fast_slow.failed_slow_writes_contains(&digest)`.
///
/// **Mutation step (verified at test authorship time):** revert the
/// `tokio::time::timeout(watchdog, driver.await_completion())` in
/// `run_async_commit_reaper` to a bare `driver.await_completion().await`.
/// This test then red-fails — the outer `tokio::time::timeout` deadlock
/// detector trips (the reaper's spawn hangs forever on a blocked
/// receiver, the relay never sends, `relay_rx.await` hangs in turn) —
/// with the bespoke
/// `"sync-arm chunked commit watchdog must fire on stalled
/// await_completion — pin TTL leak class regression"` message.
///
/// Additional Sync-only mutation: comment out the
/// `tx.send(commit_result.clone())` relay-firing line in the reaper.
/// The bookkeeping still fires (Async test stays green) but THIS test
/// red-fails on the relay-await deadlock-detector — the relay-fire
/// half of the contract is uniquely guarded here.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn chunked_synchronous_commit_watchdog_fires_on_stalled_completion() {
    const SIZE: u64 = 1024;
    let digest = DigestInfo::new(sha256(b"sync-watchdog-test-blob"), SIZE);

    // Production composition: real FilesystemStore + FastSlowStore so
    // the `failed_writes_inserter()` closure goes into the genuine
    // `FastSlowStore::failed_slow_writes` set + invokes pin_digests on
    // the genuine fast store. The closure shape matches what
    // `wire_bazel_chunked_dispatcher` wires in production.
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

    // Pre-flight: failed-set empty.
    assert!(
        !fast_slow.failed_slow_writes_contains(&digest),
        "fixture invariant: failed_slow_writes starts empty",
    );

    // Construct a real ChunkedDriver with sender held alive — the
    // driver's `rx.recv().await` blocks forever, `await_completion()`
    // never returns, the reaper's watchdog is the only thing that
    // unblocks. Mirrors the Async test's wedge mechanism exactly.
    let (driver, _sender_held_alive) = ChunkedDriver::spawn_driver(
        Arc::clone(&fs_store),
        digest,
        SIZE,
        4 * 1024,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);

    let in_flight = ChunkedWriteInFlight::new();

    // Production-shaped sinks. Identical shape to
    // `wire_bazel_chunked_dispatcher`.
    let failed_sink = fast_slow.as_ref().failed_writes_inserter();
    let stable_sink = fast_slow.as_ref().stable_digests_pusher();

    // Build the relay channel exactly as the Sync arm's production
    // code path does. The Sync arm spawns the reaper with
    // `mode_label="synchronous"` and `result_relay=Some(tx)`, then
    // awaits `rx`.
    let (relay_tx, relay_rx) =
        tokio::sync::oneshot::channel::<Result<ChunkedCommitResult, Error>>();

    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let reaper_handle = tokio::spawn(run_async_commit_reaper(
        Arc::clone(&driver_arc),
        digest,
        Arc::clone(&in_flight),
        None, // chunked_read_registry
        Some(stable_sink),
        Some(failed_sink),
        Arc::clone(&metrics),
        // The under-test mode label: distinguishes the Sync arm's
        // watchdog firing from the Async arm's in operator
        // dashboards. Mutating to "async" would make this test pass
        // (the test does not assert on log fields) — log-field
        // coverage is left to the existing AsyncCommit test's
        // mutation step ("async" → "synchronous" similarly silent).
        "synchronous",
        // The under-test relay: the Sync arm's RPC future awaits
        // this. Mutating to `None` would make `relay_rx.await` below
        // hang (never receive); the outer 10s virtual-time timeout
        // would trip on the deadlock-detector.
        Some(relay_tx),
    ));

    // Yield once so the spawned reaper makes progress past the spawn
    // boundary into the timeout future.
    tokio::task::yield_now().await;

    // Advance virtual time PAST the watchdog deadline. Same +5s
    // buffer as the Async test.
    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    // Await the relay — this is what the Sync arm's RPC future does.
    // The reaper's watchdog fires, synthesises an Err, sends it
    // through the relay BEFORE bookkeeping. Without the relay-fire,
    // this `relay_rx.await` would hang forever (the deadlock-detector
    // catches it via the outer 10s virtual-time timeout below).
    let relay_result = tokio::time::timeout(Duration::from_secs(10), relay_rx)
        .await
        .expect(
            "sync-arm chunked commit watchdog must fire on stalled \
             await_completion — pin TTL leak class regression",
        )
        .expect(
            "reaper task must relay the commit result — without this \
             the WriteChunked RPC future hangs on relay_rx.await even \
             though the watchdog correctly fired the failed-commit sink",
        );

    // Contract part 1: the relayed result is `Err(DeadlineExceeded)`.
    let relay_err = relay_result.expect_err(
        "sync-arm watchdog MUST relay an Err — the WriteChunked RPC's \
         caller relies on the Err to know the commit failed (and to \
         feed the worker reconnect-retry path)",
    );
    assert_eq!(
        relay_err.code,
        Code::DeadlineExceeded,
        "sync-arm watchdog MUST relay Code::DeadlineExceeded (not a \
         generic Err) so the WriteChunked classifier can distinguish \
         the watchdog-fire from a natural commit-Err. got code={:?}, \
         msg={:?}",
        relay_err.code,
        relay_err.message_string(),
    );

    // Wait for the reaper to fully complete its bookkeeping (the
    // relay fired BEFORE bookkeeping; we need to await the spawn to
    // observe the bookkeeping post-conditions).
    tokio::time::timeout(Duration::from_secs(10), reaper_handle)
        .await
        .expect(
            "reaper task must complete bookkeeping after firing the \
             relay — without this the failed_commit_sink + in_flight \
             removal observability gaps remain open",
        )
        .expect("reaper task must not panic");

    // Contract part 2: failed_commit_sink fired. Without this the
    // WriteChunked worker reconnect-retry path never picks up the
    // digest.
    assert!(
        fast_slow.failed_slow_writes_contains(&digest),
        "sync-arm watchdog MUST insert into failed_slow_writes via \
         the failed_commit_sink closure. Without this, a stalled slow \
         tier on the Sync (WriteChunked RPC) path leaves no record of \
         the failed commit and the worker reconnect-retry path never \
         picks up the digest — sibling-bug regression of the Async \
         arm's contract guarded by \
         chunked_async_commit_watchdog_fires_on_stalled_completion.",
    );

    // Contract part 3: in_flight cleared.
    assert!(
        !in_flight.contains_digest(&digest),
        "sync-arm watchdog MUST NOT leave any residual entry in the \
         chunked in-flight set. Observed in_flight entry post-watchdog \
         suggests the reaper's bookkeeping was skipped on the relay \
         path (parity gap with Async arm).",
    );

    // Contract part 4: commit_failures_total incremented.
    let failures = metrics.commit_failures_total.load(AtomicOrdering::Relaxed);
    assert!(
        failures >= 1,
        "sync-arm watchdog MUST increment commit_failures_total \
         (natural Err path parity with the Async arm). got={failures}",
    );

    // Drop the driver Arc so the JoinHandleDropGuard inside
    // ChunkedDriver aborts the still-blocked inner driver task.
    drop(driver_arc);
    drop(_sender_held_alive);
}
