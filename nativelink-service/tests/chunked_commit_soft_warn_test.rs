// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! #501 (narrow scope): per-site soft-warn observability tests.
//!
//! 6 tests, 2 per commit-watchdog site, exercising the under-action
//! (counter bumps on slow commit) AND over-action (counter does NOT
//! bump on fast commit) contracts. All paused-time. Each test reads
//! the REAL `commit_watchdog_soft_warn_total` counter — not a constant
//! tautology (`assert!(CONST > 10)`), which the prior #501 reviewer
//! sweep flagged as theatrical.
//!
//! Mutation falsification (per CLAUDE.md TDD step 5) is documented on
//! each test with the exact source line to comment out and the bespoke
//! failure message naming "#501 narrow-scope" so a regression red-fails
//! distinctly from unrelated v1/v2/BazelDispatcher test breakage.

#![cfg(feature = "chunked_fast_slow")]

use core::sync::atomic::Ordering;
use core::time::Duration;
use std::path::PathBuf;
use std::sync::Arc;

use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreSpec};
use nativelink_error::Code;
use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::{
    AWAIT_INFLIGHT_SOFT_WARN_SEEN, BAZEL_AWAIT_COMMIT_SOFT_WARN_SEEN, BazelChunkedDispatcherImpl,
    CHUNKED_COMMIT_SOFT_WARN_SECS, CHUNKED_COMMIT_WATCHDOG_SECS, ChunkedWriteHandlerMetrics,
    ChunkedWriteInFlight, V1_REAPER_SOFT_WARN_SEEN, V2_AWAITER_SOFT_WARN_SEEN,
    await_inflight_commit_with_watchdog_for_test, run_async_commit_reaper,
};
use nativelink_service::chunked_write_handler_v2::v2_await_commit_result_for_test;
use nativelink_store::chunked::BazelChunkedDispatcher;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{ChunkedDriver, PER_BLOB_MPSC_CAP};
use nativelink_store::chunked::chunked_race_state::{
    ChunkRaceState, RaceCommitResult, SingleStreamAttachOutcome, WriterId,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use sha2::{Digest as _, Sha256};

// -----------------------------------------------------------------------------
// Shared helpers (mirrors chunked_stable_digests_push_test fixture shape;
// inlined here to keep this file self-contained).
// -----------------------------------------------------------------------------

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

async fn make_filesystem_store() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/501-soft-warn/content");
    let temp_path = format!("{base}/{nonce}/501-soft-warn/temp");
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

async fn make_fast_slow() -> (Arc<FilesystemStore<FileEntryImpl>>, Arc<FastSlowStore>) {
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
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow_store,
    );
    (fs_store, fast_slow)
}

fn make_test_budget() -> &'static ChunkBudget {
    Box::leak(Box::new(ChunkBudget::new()))
}

// =============================================================================
// SITE 1 — v1 reaper (`run_async_commit_reaper`)
// =============================================================================

/// **#501 (narrow scope) under-action — v1 reaper site.** A wedged
/// driver (sender held alive → `await_completion()` blocks forever)
/// MUST trigger `commit_watchdog_soft_warn_total.fetch_add(1)` at the
/// 30 s soft-warn deadline, BEFORE the destructive 60 s infra-integrity
/// watchdog fires. The infra-integrity watchdog's behavior is
/// byte-identical to today (this test does not assert on its 4
/// side effects — those are covered by the #283 sub-item 3 suite);
/// we observe ONLY the new soft-warn counter.
///
/// **Mutation step (per CLAUDE.md TDD step 5):** comment out the
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Ordering::Relaxed)`
/// inside `run_async_commit_reaper`'s soft-warn select arm. This test
/// then red-fails with the bespoke
/// "#501 narrow-scope: v1 reaper soft-warn counter did not bump at 30 s"
/// message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn v1_reaper_soft_warn_fires_once_per_digest_at_30s() {
    let digest = DigestInfo::new(sha256(b"#501-narrow-v1-soft-warn-under-action"), 1024);

    // Clear the static dedup set so a prior test (or this test on rerun)
    // doesn't pre-populate the digest and mask the expected bump.
    V1_REAPER_SOFT_WARN_SEEN.clear();

    let (fs_store, fast_slow) = make_fast_slow().await;
    let failed_sink = fast_slow.as_ref().failed_writes_inserter();
    let stable_sink = fast_slow.as_ref().stable_digests_pusher();

    // Construct a real ChunkedDriver. Keep the sender alive so
    // `rx.recv()` blocks on the first iteration; await_completion()
    // never resolves on its own.
    let (driver, _sender_held_alive) = ChunkedDriver::spawn_driver(
        Arc::clone(&fs_store),
        digest,
        1024,
        4 * 1024,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    let in_flight = ChunkedWriteInFlight::new();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    let reaper_handle = tokio::spawn(run_async_commit_reaper(
        Arc::clone(&fs_store),
        Arc::clone(&driver_arc),
        digest,
        Arc::clone(&in_flight),
        None,
        Some(stable_sink),
        Some(failed_sink),
        Arc::clone(&metrics),
        "async",
        None,
    ));

    tokio::task::yield_now().await;
    let pre = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        pre, 0,
        "#501 narrow-scope pre-flight: counter must be 0 before the soft-warn \
         deadline is crossed; got {pre}",
    );

    // Advance virtual time PAST the soft-warn deadline (30 s) but NOT
    // past the infra-integrity watchdog (60 s).
    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_SOFT_WARN_SECS + 2)).await;
    tokio::task::yield_now().await;

    let post = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        post, 1,
        "#501 narrow-scope: v1 reaper soft-warn counter did not bump at 30 s. \
         Expected commit_watchdog_soft_warn_total == 1 after advancing past \
         CHUNKED_COMMIT_SOFT_WARN_SECS={CHUNKED_COMMIT_SOFT_WARN_SECS}; got \
         {post}. The select! arm in run_async_commit_reaper that calls \
         `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Relaxed)` \
         either did not fire or was elided. The infra-integrity watchdog \
         path is unchanged; only the new soft-warn layer can produce this \
         counter bump.",
    );

    // Drive past infra-integrity so the reaper resolves cleanly.
    tokio::time::advance(Duration::from_secs(
        CHUNKED_COMMIT_WATCHDOG_SECS - CHUNKED_COMMIT_SOFT_WARN_SECS + 2,
    ))
    .await;
    tokio::time::timeout(Duration::from_secs(10), reaper_handle)
        .await
        .expect(
            "#501 narrow-scope: reaper must complete after infra-integrity \
             watchdog (60 s) fires post-soft-warn",
        )
        .expect("reaper task must not panic");

    // Remove-on-completion contract: dedup set must drain.
    assert_eq!(
        V1_REAPER_SOFT_WARN_SEEN.len(),
        0,
        "#501 narrow-scope: v1 reaper soft-warn dedup set MUST drain the \
         digest after the reaper finishes (remove-on-completion contract). \
         Observed len={}; expected 0.",
        V1_REAPER_SOFT_WARN_SEEN.len(),
    );

    drop(driver_arc);
    drop(_sender_held_alive);
}

/// **#501 (narrow scope) over-action — v1 reaper site.** When the
/// driver completes BEFORE the 30 s soft-warn deadline, the soft-warn
/// counter MUST stay at 0. Guards against a regression where the
/// soft-warn select arm fires unconditionally.
///
/// **Real counter read, not constant tautology:** per CLAUDE.md TDD
/// asymmetric-contract guidance, this test asserts on
/// `metrics.commit_watchdog_soft_warn_total.load(Relaxed) == 0`, not
/// `assert!(CHUNKED_COMMIT_SOFT_WARN_SECS > 10)` (the prior #501
/// reviewer caught this exact theatrical pattern).
///
/// **Mutation step:** move the
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1,
/// Ordering::Relaxed)` call OUT of the soft-warn select arm body to
/// the top of the reaper body (so it fires unconditionally on EVERY
/// reaper invocation, regardless of whether the soft-warn deadline
/// was actually crossed). The over-action test then red-fails with
/// the bespoke "#501 narrow-scope over-action: soft-warn fired on
/// fast-commit path" message. Verified at test-authorship time.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn v1_reaper_soft_warn_does_not_fire_on_fast_commit() {
    let digest = DigestInfo::new(sha256(b"#501-narrow-v1-soft-warn-over-action"), 1024);

    V1_REAPER_SOFT_WARN_SEEN.clear();

    let (fs_store, fast_slow) = make_fast_slow().await;
    let failed_sink = fast_slow.as_ref().failed_writes_inserter();
    let stable_sink = fast_slow.as_ref().stable_digests_pusher();

    // Driver returns promptly: no sender retained → recv loop exits
    // → commit returns Err well inside the soft-warn deadline.
    let (driver, sender_dropped) = ChunkedDriver::spawn_driver(
        Arc::clone(&fs_store),
        digest,
        1024,
        4 * 1024,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    drop(sender_dropped);

    let in_flight = ChunkedWriteInFlight::new();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    let reaper_handle = tokio::spawn(run_async_commit_reaper(
        Arc::clone(&fs_store),
        Arc::clone(&driver_arc),
        digest,
        Arc::clone(&in_flight),
        None,
        Some(stable_sink),
        Some(failed_sink),
        Arc::clone(&metrics),
        "async",
        None,
    ));

    tokio::time::timeout(Duration::from_secs(10), reaper_handle)
        .await
        .expect(
            "#501 narrow-scope over-action: reaper with promptly-completing \
             driver must finish without needing the watchdog to fire",
        )
        .expect("reaper task must not panic");

    // Defensive: advance past both deadlines.
    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    let count = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        count, 0,
        "#501 narrow-scope over-action: soft-warn fired on fast-commit path \
         (commit_watchdog_soft_warn_total = {count}, expected 0). The \
         driver returned in well under CHUNKED_COMMIT_SOFT_WARN_SECS \
         ({CHUNKED_COMMIT_SOFT_WARN_SECS} s), so the soft-warn select arm \
         should have been cancelled by the watchdog branch resolving \
         first. A non-zero counter means the soft-warn fired spuriously — \
         likely the `if !soft_warned` gate was removed OR the select! \
         ordering changed so the soft-warn arm wins on a same-poll tie.",
    );

    drop(driver_arc);
}

// =============================================================================
// SITE 2 — v2 sibling (`v2_await_commit_result`)
// =============================================================================

/// **#501 (narrow scope) under-action — v2 sibling site.** When no
/// commit_runner publishes a result, the awaiter parks on the Notify.
/// At the 30 s soft-warn deadline, `commit_watchdog_soft_warn_total`
/// MUST bump. The infra-integrity watchdog (60 s) is byte-identical
/// to today — including the #508 `WatchdogTimeoutSignal` discriminator
/// attachment.
///
/// **Mutation step:** comment out
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Ordering::Relaxed)`
/// inside the soft-warn select arm in `v2_await_commit_result`. This
/// test then red-fails with the bespoke "#501 narrow-scope: v2 awaiter
/// soft-warn counter did not bump at 30 s" message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn v2_awaiter_soft_warn_fires_once_per_digest_at_30s() {
    let mut hash = [0u8; 32];
    hash[0] = 0x50;
    hash[1] = 0x01;
    hash[2] = 0xA0; // distinguishes from sibling tests
    let digest = DigestInfo::new(hash, 2048);

    V2_AWAITER_SOFT_WARN_SEEN.clear();

    let race_state = Arc::new(ChunkRaceState::new(
        digest,
        2048,
        PathBuf::from("/tmp/501-narrow-v2-under-action.partial"),
    ));
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let metrics_for_awaiter = Arc::clone(&metrics);
    let race_state_for_awaiter = Arc::clone(&race_state);

    let awaiter = tokio::spawn(async move {
        v2_await_commit_result_for_test(&race_state_for_awaiter, digest, &metrics_for_awaiter)
            .await
    });

    tokio::task::yield_now().await;
    let pre = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        pre, 0,
        "#501 narrow-scope pre-flight: v2 counter must be 0 before soft-warn \
         deadline; got {pre}",
    );

    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_SOFT_WARN_SECS + 2)).await;
    tokio::task::yield_now().await;

    let post = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        post, 1,
        "#501 narrow-scope: v2 awaiter soft-warn counter did not bump at \
         30 s. Expected commit_watchdog_soft_warn_total == 1 after \
         advancing past CHUNKED_COMMIT_SOFT_WARN_SECS=\
         {CHUNKED_COMMIT_SOFT_WARN_SECS}; got {post}. The select! arm in \
         v2_await_commit_result that calls \
         `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Relaxed)` \
         either did not fire or was elided. The infra-integrity watchdog \
         (COMMIT_WAIT_WATCHDOG = 60 s) is unchanged; only the new \
         soft-warn layer can produce this counter bump.",
    );

    tokio::time::advance(Duration::from_secs(
        CHUNKED_COMMIT_WATCHDOG_SECS - CHUNKED_COMMIT_SOFT_WARN_SECS + 2,
    ))
    .await;
    let result = tokio::time::timeout(Duration::from_secs(10), awaiter)
        .await
        .expect(
            "#501 narrow-scope: awaiter must finish after infra-integrity \
             watchdog fires post-soft-warn",
        )
        .expect("awaiter task must not panic");
    let err = result.expect_err(
        "v2 awaiter with no publisher MUST return Err on infra-integrity fire",
    );
    assert_eq!(
        err.code,
        Code::DeadlineExceeded,
        "#501 narrow-scope: infra-integrity Err arm contract preserved \
         (DeadlineExceeded); got {err:?}",
    );

    assert_eq!(
        V2_AWAITER_SOFT_WARN_SEEN.len(),
        0,
        "#501 narrow-scope: v2 awaiter soft-warn dedup set MUST drain the \
         digest after the awaiter finishes (remove-on-completion contract). \
         Observed len={}; expected 0.",
        V2_AWAITER_SOFT_WARN_SEEN.len(),
    );
}

/// **#501 (narrow scope) over-action — v2 sibling site.** A publisher
/// firing BEFORE the 30 s soft-warn deadline MUST keep the soft-warn
/// counter at 0. Same regression-guard class as v1 over-action.
///
/// **Mutation step:** move the
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1,
/// Ordering::Relaxed)` call OUT of the soft-warn select arm body to
/// the top of `v2_await_commit_result` (so it fires unconditionally
/// on every awaiter invocation). The over-action test then red-fails
/// with the bespoke "#501 narrow-scope over-action: v2 awaiter
/// soft-warn fired on fast-commit path" message. Verified at
/// test-authorship time.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn v2_awaiter_soft_warn_does_not_fire_on_fast_commit() {
    let mut hash = [0u8; 32];
    hash[0] = 0x50;
    hash[1] = 0x01;
    hash[2] = 0xB0;
    let digest = DigestInfo::new(hash, 4096);

    V2_AWAITER_SOFT_WARN_SEEN.clear();

    let race_state = Arc::new(ChunkRaceState::new(
        digest,
        4096,
        PathBuf::from("/tmp/501-narrow-v2-over-action.partial"),
    ));
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let metrics_for_awaiter = Arc::clone(&metrics);
    let race_state_for_awaiter = Arc::clone(&race_state);

    let awaiter = tokio::spawn(async move {
        v2_await_commit_result_for_test(&race_state_for_awaiter, digest, &metrics_for_awaiter)
            .await
    });

    tokio::task::yield_now().await;
    race_state.publish_commit_result(Ok(RaceCommitResult {
        committed_size: 4096,
    }));

    let outcome = tokio::time::timeout(Duration::from_secs(10), awaiter)
        .await
        .expect(
            "#501 narrow-scope over-action: awaiter must resolve immediately \
             after publish_commit_result(Ok); test hung — possibly a \
             regression where the select! now favors the soft-warn branch \
             over the watchdog/notify branch (biased ordering must put \
             watchdog first)",
        )
        .expect("awaiter task must not panic");
    let commit = outcome.expect(
        "#501 narrow-scope over-action sanity: publish was Ok so awaiter \
         must return Ok",
    );
    assert_eq!(commit.committed_size, 4096);

    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    let count = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        count, 0,
        "#501 narrow-scope over-action: v2 awaiter soft-warn fired on \
         fast-commit path (commit_watchdog_soft_warn_total = {count}, \
         expected 0). The publisher fired BEFORE \
         CHUNKED_COMMIT_SOFT_WARN_SECS ({CHUNKED_COMMIT_SOFT_WARN_SECS} s), \
         so the soft-warn select arm should have been cancelled by the \
         notify branch resolving first. A non-zero counter means the \
         soft-warn fired spuriously — likely the `if !soft_warned` gate \
         was removed.",
    );
}

// =============================================================================
// SITE 3 — BazelChunkedDispatcher AwaitCommit
// =============================================================================

/// **#501 (narrow scope) under-action — BazelChunkedDispatcher AwaitCommit
/// site.** When another writer holds the single-stream owner gate (and
/// never publishes a result), the `BazelChunkedDispatcherImpl::dispatch`
/// AwaitCommit branch parks on the per-digest Notify. At the 30 s
/// soft-warn deadline, `commit_watchdog_soft_warn_total` MUST bump.
/// The infra-integrity watchdog (now derived from
/// `CHUNKED_COMMIT_WATCHDOG_SECS` per the #509 fold-in) fires at 60 s.
///
/// This test also implicitly covers the #509 fold-in (the
/// `Duration::from_secs(60)` literal → `Duration::from_secs(
/// CHUNKED_COMMIT_WATCHDOG_SECS)`) — the test relies on the named
/// constant for the watchdog timing.
///
/// **Mutation step:** comment out the
/// `self.metrics.commit_watchdog_soft_warn_total.fetch_add(1, Ordering::Relaxed)`
/// inside the dispatcher's AwaitCommit soft-warn select arm. This test
/// then red-fails with the bespoke "#501 narrow-scope: BazelDispatcher
/// AwaitCommit soft-warn counter did not bump at 30 s" message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn bazel_dispatch_await_commit_soft_warn_fires_once_per_digest_at_30s() {
    const CHUNK: usize = 4 * 1024;
    const SIZE: u64 = 1024;
    let payload: Vec<u8> = (0..SIZE as usize).map(|i| (i as u8).wrapping_add(0x10)).collect();
    let digest = DigestInfo::new(sha256(&payload), SIZE);

    BAZEL_AWAIT_COMMIT_SOFT_WARN_SEEN.clear();

    let fs_store = make_filesystem_store().await;

    // Pre-attach a fake single-stream owner so our dispatcher's
    // attach call returns `AwaitCommit`. The holder guard pins
    // attached_writer_count for the test's lifetime.
    let (_race_state_for_holder, _holder_guard, holder_outcome) = fs_store
        .race_state_for_digest_and_attach_single_stream(&digest, CHUNK as u32, WriterId(99));
    match holder_outcome {
        SingleStreamAttachOutcome::Owner => { /* expected */ }
        SingleStreamAttachOutcome::AwaitCommit { reason } => panic!(
            "fixture invariant: first attach must be Owner (got \
             AwaitCommit reason={reason:?})"
        ),
    }

    let chunk_budget = make_test_budget();
    let in_flight = ChunkedWriteInFlight::new();
    let dispatcher_impl = BazelChunkedDispatcherImpl::new_with_state_for_test(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        chunk_budget,
        CHUNK,
    );
    // Capture metrics handle BEFORE wrapping the impl into Arc<dyn>.
    let metrics = Arc::clone(dispatcher_impl.metrics());
    let dispatcher: Arc<dyn BazelChunkedDispatcher> = Arc::new(dispatcher_impl);

    let (mut tx, rx) = make_buf_channel_pair();
    let payload_for_producer = payload.clone();
    let producer = tokio::spawn(async move {
        tx.send(bytes::Bytes::copy_from_slice(&payload_for_producer))
            .await
            .expect("producer send must succeed");
        tx.send_eof().expect("producer eof must succeed");
    });
    let dispatch_handle = tokio::spawn(async move { dispatcher.dispatch(digest, rx).await });

    tokio::task::yield_now().await;
    let pre = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        pre, 0,
        "#501 narrow-scope pre-flight: BazelDispatcher counter must be 0 \
         before soft-warn deadline; got {pre}",
    );

    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_SOFT_WARN_SECS + 2)).await;
    tokio::task::yield_now().await;

    let post = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        post, 1,
        "#501 narrow-scope: BazelDispatcher AwaitCommit soft-warn counter \
         did not bump at 30 s. Expected commit_watchdog_soft_warn_total \
         == 1 after advancing past CHUNKED_COMMIT_SOFT_WARN_SECS=\
         {CHUNKED_COMMIT_SOFT_WARN_SECS}; got {post}. The select! arm in \
         BazelChunkedDispatcherImpl::dispatch's AwaitCommit branch that \
         calls `self.metrics.commit_watchdog_soft_warn_total.fetch_add(1, \
         Relaxed)` either did not fire or was elided. The \
         infra-integrity 60 s watchdog (now named \
         CHUNKED_COMMIT_WATCHDOG_SECS per #509 fold-in) is unchanged; \
         only the new soft-warn layer can produce this counter bump.",
    );

    tokio::time::advance(Duration::from_secs(
        CHUNKED_COMMIT_WATCHDOG_SECS - CHUNKED_COMMIT_SOFT_WARN_SECS + 2,
    ))
    .await;
    let result = tokio::time::timeout(Duration::from_secs(10), dispatch_handle)
        .await
        .expect(
            "#501 narrow-scope: dispatch must complete after infra-integrity \
             watchdog (60 s) fires post-soft-warn",
        )
        .expect("dispatch task must not panic");
    let err = result.expect_err(
        "BazelDispatcher AwaitCommit with no publisher MUST return Err on \
         infra-integrity fire",
    );
    assert_eq!(
        err.code,
        Code::DeadlineExceeded,
        "#501 narrow-scope: BazelDispatcher AwaitCommit infra-integrity \
         Err arm contract preserved (DeadlineExceeded); got {err:?}",
    );

    let _ = producer.await;

    assert_eq!(
        BAZEL_AWAIT_COMMIT_SOFT_WARN_SEEN.len(),
        0,
        "#501 narrow-scope: BazelDispatcher AwaitCommit soft-warn dedup \
         set MUST drain the digest after dispatch finishes \
         (remove-on-completion contract). Observed len={}; expected 0.",
        BAZEL_AWAIT_COMMIT_SOFT_WARN_SEEN.len(),
    );

    drop(_holder_guard);
}

/// **#501 (narrow scope) over-action — BazelChunkedDispatcher AwaitCommit
/// site.** When the single-stream owner publishes a result BEFORE the
/// 30 s soft-warn deadline, the soft-warn counter MUST stay at 0.
///
/// **Mutation step:** move the
/// `self.metrics.commit_watchdog_soft_warn_total.fetch_add(1,
/// Ordering::Relaxed)` call OUT of the soft-warn select arm body to
/// the top of the AwaitCommit branch (so it fires unconditionally on
/// every AwaitCommit invocation). The over-action test then red-fails
/// with the bespoke "#501 narrow-scope over-action: BazelDispatcher
/// AwaitCommit soft-warn fired on fast-commit path" message. Verified
/// at test-authorship time.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn bazel_dispatch_await_commit_soft_warn_does_not_fire_on_fast_commit() {
    const CHUNK: usize = 4 * 1024;
    const SIZE: u64 = 1024;
    let payload: Vec<u8> = (0..SIZE as usize).map(|i| (i as u8).wrapping_add(0x20)).collect();
    let digest = DigestInfo::new(sha256(&payload), SIZE);

    BAZEL_AWAIT_COMMIT_SOFT_WARN_SEEN.clear();

    let fs_store = make_filesystem_store().await;

    let (race_state_for_holder, _holder_guard, holder_outcome) = fs_store
        .race_state_for_digest_and_attach_single_stream(&digest, CHUNK as u32, WriterId(99));
    match holder_outcome {
        SingleStreamAttachOutcome::Owner => { /* expected */ }
        SingleStreamAttachOutcome::AwaitCommit { reason } => panic!(
            "fixture invariant: first attach must be Owner (got \
             AwaitCommit reason={reason:?})"
        ),
    }

    let chunk_budget = make_test_budget();
    let in_flight = ChunkedWriteInFlight::new();
    let dispatcher_impl = BazelChunkedDispatcherImpl::new_with_state_for_test(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        chunk_budget,
        CHUNK,
    );
    let metrics = Arc::clone(dispatcher_impl.metrics());
    let dispatcher: Arc<dyn BazelChunkedDispatcher> = Arc::new(dispatcher_impl);

    let (mut tx, rx) = make_buf_channel_pair();
    let payload_for_producer = payload.clone();
    let producer = tokio::spawn(async move {
        tx.send(bytes::Bytes::copy_from_slice(&payload_for_producer))
            .await
            .expect("producer send must succeed");
        tx.send_eof().expect("producer eof must succeed");
    });
    let dispatch_handle = tokio::spawn(async move { dispatcher.dispatch(digest, rx).await });

    tokio::task::yield_now().await;
    race_state_for_holder.publish_commit_result(Ok(RaceCommitResult {
        committed_size: SIZE,
    }));

    let outcome = tokio::time::timeout(Duration::from_secs(10), dispatch_handle)
        .await
        .expect(
            "#501 narrow-scope over-action: dispatch must resolve \
             immediately after publish_commit_result(Ok); test hung — \
             possibly a regression where the dispatcher's AwaitCommit \
             select! favors the soft-warn arm over the watchdog (biased \
             order regression)",
        )
        .expect("dispatch task must not panic");
    let committed = outcome.expect(
        "#501 narrow-scope over-action sanity: publish was Ok so dispatch \
         must return Ok",
    );
    assert_eq!(committed, SIZE);

    let _ = producer.await;

    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    let count = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        count, 0,
        "#501 narrow-scope over-action: BazelDispatcher AwaitCommit \
         soft-warn fired on fast-commit path \
         (commit_watchdog_soft_warn_total = {count}, expected 0). The \
         publisher fired BEFORE CHUNKED_COMMIT_SOFT_WARN_SECS \
         ({CHUNKED_COMMIT_SOFT_WARN_SECS} s), so the soft-warn select \
         arm should have been cancelled by the notify branch. A non-zero \
         counter means the soft-warn fired spuriously.",
    );

    drop(_holder_guard);
}

// =============================================================================
// SITE 4 — `await_inflight_commit_with_watchdog` (#510)
// =============================================================================
//
// This site is the v1 worker `WriteChunked` cross-version coordination
// path: a worker arriving while another writer holds the per-digest
// single-stream gate drains its stream and parks on the per-digest
// Notify via `await_inflight_commit_with_watchdog`. Added by #447 AFTER
// the #501 narrow-scope design was sketched, hence deferred to #510.

/// **#510 under-action — `await_inflight_commit_with_watchdog` site.**
/// When no commit_runner publishes a result, the awaiter parks on the
/// Notify. At the 30 s soft-warn deadline,
/// `commit_watchdog_soft_warn_total` MUST bump. The infra-integrity
/// watchdog (60 s) is byte-identical to today — including the
/// `WatchdogTimeoutSignal` discriminator attachment that #447 / #508
/// added.
///
/// **Real counter read, not constant tautology:** asserts on
/// `metrics.commit_watchdog_soft_warn_total.load(Relaxed) == 1`.
///
/// **Mutation step (per CLAUDE.md TDD step 5):** comment out
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Ordering::Relaxed)`
/// inside the soft-warn select arm in
/// `await_inflight_commit_with_watchdog`. This test then red-fails with
/// the bespoke "#510: await_inflight soft-warn did not fire at 30s"
/// message naming the site.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn await_inflight_commit_soft_warn_fires_once_per_digest_at_30s() {
    let mut hash = [0u8; 32];
    hash[0] = 0x51;
    hash[1] = 0x10;
    hash[2] = 0xA0; // distinguishes from sibling tests
    let digest = DigestInfo::new(hash, 2048);

    // Clear the static dedup set so a prior test (or this test on rerun)
    // doesn't pre-populate the digest and mask the expected bump.
    AWAIT_INFLIGHT_SOFT_WARN_SEEN.clear();

    let race_state = Arc::new(ChunkRaceState::new(
        digest,
        2048,
        PathBuf::from("/tmp/510-await-inflight-under-action.partial"),
    ));
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let metrics_for_awaiter = Arc::clone(&metrics);
    let race_state_for_awaiter = Arc::clone(&race_state);

    let awaiter = tokio::spawn(async move {
        await_inflight_commit_with_watchdog_for_test(
            &race_state_for_awaiter,
            digest,
            &metrics_for_awaiter,
        )
        .await
    });

    tokio::task::yield_now().await;
    let pre = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        pre, 0,
        "#510 pre-flight: await_inflight counter must be 0 before \
         soft-warn deadline; got {pre}",
    );

    // Advance virtual time PAST the soft-warn deadline (30 s) but NOT
    // past the infra-integrity watchdog (60 s).
    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_SOFT_WARN_SECS + 2)).await;
    tokio::task::yield_now().await;

    let post = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        post, 1,
        "#510: await_inflight soft-warn did not fire at 30s. Expected \
         commit_watchdog_soft_warn_total == 1 after advancing past \
         CHUNKED_COMMIT_SOFT_WARN_SECS={CHUNKED_COMMIT_SOFT_WARN_SECS}; \
         got {post}. The select! arm in await_inflight_commit_with_watchdog \
         that calls `metrics.commit_watchdog_soft_warn_total.fetch_add(1, \
         Relaxed)` either did not fire or was elided. The infra-integrity \
         60 s watchdog (CHUNKED_COMMIT_WATCHDOG_SECS) is unchanged; only \
         the new #510 4th-site soft-warn layer can produce this counter \
         bump.",
    );

    // Drive past infra-integrity so the awaiter resolves cleanly via
    // the watchdog Err arm.
    tokio::time::advance(Duration::from_secs(
        CHUNKED_COMMIT_WATCHDOG_SECS - CHUNKED_COMMIT_SOFT_WARN_SECS + 2,
    ))
    .await;
    let result = tokio::time::timeout(Duration::from_secs(10), awaiter)
        .await
        .expect(
            "#510: await_inflight must complete after infra-integrity \
             watchdog (60 s) fires post-soft-warn",
        )
        .expect("await_inflight task must not panic");
    let err = result.expect_err(
        "await_inflight with no publisher MUST return Err on \
         infra-integrity fire",
    );
    assert_eq!(
        err.code,
        Code::DeadlineExceeded,
        "#510: await_inflight infra-integrity Err arm contract preserved \
         (DeadlineExceeded); got {err:?}",
    );

    // Remove-on-completion contract: dedup set must drain.
    assert_eq!(
        AWAIT_INFLIGHT_SOFT_WARN_SEEN.len(),
        0,
        "#510: await_inflight soft-warn dedup set MUST drain the digest \
         after the awaiter finishes (remove-on-completion contract). \
         Observed len={}; expected 0.",
        AWAIT_INFLIGHT_SOFT_WARN_SEEN.len(),
    );
}

/// **#510 over-action — `await_inflight_commit_with_watchdog` site.**
/// A publisher firing BEFORE the 30 s soft-warn deadline MUST keep the
/// soft-warn counter at 0. Same regression-guard class as the v1 / v2 /
/// BazelDispatcher over-action tests.
///
/// **Mutation step:** move the
/// `metrics.commit_watchdog_soft_warn_total.fetch_add(1, Ordering::Relaxed)`
/// call OUT of the soft-warn select arm body to the top of
/// `await_inflight_commit_with_watchdog` (so it fires unconditionally on
/// every invocation). The over-action test then red-fails with the
/// bespoke "#510 over-action: await_inflight soft-warn fired on
/// fast-commit path" message. Verified at test-authorship time.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn await_inflight_commit_soft_warn_does_not_fire_on_fast_resolve() {
    let mut hash = [0u8; 32];
    hash[0] = 0x51;
    hash[1] = 0x10;
    hash[2] = 0xB0;
    let digest = DigestInfo::new(hash, 4096);

    AWAIT_INFLIGHT_SOFT_WARN_SEEN.clear();

    let race_state = Arc::new(ChunkRaceState::new(
        digest,
        4096,
        PathBuf::from("/tmp/510-await-inflight-over-action.partial"),
    ));
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
    let metrics_for_awaiter = Arc::clone(&metrics);
    let race_state_for_awaiter = Arc::clone(&race_state);

    let awaiter = tokio::spawn(async move {
        await_inflight_commit_with_watchdog_for_test(
            &race_state_for_awaiter,
            digest,
            &metrics_for_awaiter,
        )
        .await
    });

    tokio::task::yield_now().await;
    race_state.publish_commit_result(Ok(RaceCommitResult {
        committed_size: 4096,
    }));

    let outcome = tokio::time::timeout(Duration::from_secs(10), awaiter)
        .await
        .expect(
            "#510 over-action: await_inflight must resolve immediately \
             after publish_commit_result(Ok); test hung — possibly a \
             regression where the select! now favors the soft-warn branch \
             over the watchdog/notify branch (biased ordering must put \
             watchdog first)",
        )
        .expect("await_inflight task must not panic");
    let committed_size = outcome.expect(
        "#510 over-action sanity: publish was Ok so await_inflight must \
         return Ok",
    );
    assert_eq!(committed_size, 4096);

    tokio::time::advance(Duration::from_secs(CHUNKED_COMMIT_WATCHDOG_SECS + 5)).await;

    let count = metrics
        .commit_watchdog_soft_warn_total
        .load(Ordering::Relaxed);
    assert_eq!(
        count, 0,
        "#510 over-action: await_inflight soft-warn fired on fast-commit \
         path (commit_watchdog_soft_warn_total = {count}, expected 0). \
         The publisher fired BEFORE CHUNKED_COMMIT_SOFT_WARN_SECS \
         ({CHUNKED_COMMIT_SOFT_WARN_SECS} s), so the soft-warn select \
         arm should have been cancelled by the notify branch resolving \
         first. A non-zero counter means the soft-warn fired spuriously \
         — likely the `if !soft_warned` gate was removed OR the select! \
         ordering changed so the soft-warn arm wins on a same-poll tie.",
    );
}
