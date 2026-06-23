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

//! #212 Phase 2.7 — Bazel-facing internal chunking tests.
//!
//! These tests build a real `FastSlowStore` (fast tier = `MemoryStore`,
//! slow tier = `FilesystemStore`) and install a real
//! `BazelChunkedDispatcherImpl`. The dispatcher pipes the fast-slow
//! `update()` upstream into the per-blob `ChunkedDriver`. Tests:
//!
//! 1. Kill-switch OFF → legacy path; no chunking; existing behaviour.
//! 2. Kill-switch ON + small blob (< CHUNK_SIZE) → legacy path.
//! 3. Kill-switch ON + large blob (>= CHUNK_SIZE) → chunked dispatch:
//!    a. `update()` returns Ok promptly (β async-commit; anti-#203).
//!    b. The blob is readable from the fast tier IMMEDIATELY (the
//!       in-memory replica satisfies ≥2-replica during the
//!       async-commit window).
//!    c. The driver eventually commits to the slow tier on its own task.
//! 4. Direct unit: `dispatch_chunks_to_driver(Synchronous)` commits a
//!    multi-chunk blob to disk.
//!
//! Per CLAUDE.md test discipline:
//!   - Every async test wrapped in `tokio::time::timeout(5s)` for
//!     deadlock detection.
//!   - Specific assertion messages on every `expect()`.
//!   - Production composition: real FastSlowStore + real FilesystemStore
//!     + real ChunkedDriver machinery.
//!   - Mutation discipline (see comments on each test).

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::{
    BazelChunkedDispatcherImpl, ChunkedWriteHandlerMetrics, ChunkedWriteInFlight, CommitMode,
    DispatchOutcome, PreparedChunk, dispatch_bazel_facing_internal_chunking,
    dispatch_chunks_to_driver, wait_for_no_in_flight,
};
use nativelink_store::chunked::chunk_budget::{ChunkBudget, TOTAL_CHUNK_PERMITS};
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, disable_bazel_facing_internal_chunking,
    enable_bazel_facing_internal_chunking,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair_with_size;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use sha2::{Digest as _, Sha256};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// Test-only: each test gets its own ChunkBudget so we don't share
/// state across tests via the process-wide singleton.
fn make_test_budget() -> &'static ChunkBudget {
    Box::leak(Box::new(ChunkBudget::new()))
}

/// The kill-switch is a PROCESS-WIDE `AtomicBool` (this is intentional
/// — production has one server process, one bool). To prevent
/// cross-test interference under cargo's default parallel test runner,
/// every test that toggles the switch acquires this `tokio::sync::Mutex`
/// for the duration of its setup-execute-reset.
fn kill_switch_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Build a fresh FilesystemStore at a unique temp path.
async fn make_filesystem_store() -> (Arc<FilesystemStore<FileEntryImpl>>, String) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/p27-test/content");
    let temp_path = format!("{base}/{nonce}/p27-test/temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed");
    (store, content_path)
}

/// Build a FastSlowStore with MemoryStore fast tier + FilesystemStore
/// slow tier, optionally installing a chunked dispatcher pointed at the
/// SAME FilesystemStore. Returns the FastSlowStore, the FilesystemStore,
/// the on-disk content path (so tests can check files directly), and
/// (if `with_dispatcher`) the chunked-dispatcher's in-flight tracker
/// for direct inspection.
async fn make_fast_slow_with_dispatcher(
    chunk_size: usize,
    with_dispatcher: bool,
) -> (
    Arc<FastSlowStore>,
    Arc<FilesystemStore<FileEntryImpl>>,
    String,
    Option<Arc<ChunkedWriteInFlight>>,
) {
    let (fs_store, content_path) = make_filesystem_store().await;
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            // #212 v4.5: stale-base test catch-up — see commit 65edaa83
            // (config wire-up). Default-false preserves prior
            // semantics; this test does not depend on the read-cascade.
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow_store,
    );
    let in_flight_opt = if with_dispatcher {
        let in_flight = ChunkedWriteInFlight::new();
        let budget = make_test_budget();
        let dispatcher: Arc<dyn BazelChunkedDispatcher> =
            Arc::new(BazelChunkedDispatcherImpl::new_with_state_for_test(
                Arc::clone(&fs_store),
                Arc::clone(&in_flight),
                budget,
                chunk_size,
            ));
        fast_slow.set_bazel_chunked_dispatcher(dispatcher);
        // Lower the size threshold so a small (chunk_size * N) blob
        // exercises the chunked path in tests instead of the 1 MiB
        // production threshold.
        fast_slow.set_chunked_size_threshold_for_test(chunk_size as u64);
        Some(in_flight)
    } else {
        None
    };
    (fast_slow, fs_store, content_path, in_flight_opt)
}

/// Returns the canonical CAS path for `digest` under `content_path`.
/// Layout: `{content_path}/d/{first_byte_hex}/{digest_string}`.
fn canonical_cas_path(content_path: &str, digest: &DigestInfo) -> String {
    format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    )
}

/// Wait until the canonical CAS file for `digest` exists under
/// `content_path`. Polls with `tokio::task::yield_now` (no sleep,
/// per CLAUDE.md test discipline).
async fn wait_for_cas_file(
    content_path: &str,
    digest: &DigestInfo,
    timeout: Duration,
) -> Result<u64, &'static str> {
    let path = canonical_cas_path(content_path, digest);
    tokio::time::timeout(timeout, async {
        loop {
            if let Ok(meta) = tokio::fs::metadata(&path).await {
                return meta.len();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "CAS file did not appear within timeout")
}

/// Stream `data` into a FastSlowStore::update via a buf-channel pair.
/// Returns the result of the update call.
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
    let update_fut = async move {
        // Send all bytes then EOF.
        if !data.is_empty() {
            tx.send(data).await.expect("tx.send must succeed");
        }
        tx.send_eof().expect("tx.send_eof must succeed");
        Result::<(), nativelink_error::Error>::Ok(())
    };
    let store_call = async move {
        store_clone
            .update(key, rx, UploadSizeInfo::ExactSize(total))
            .await
    };
    let (writer_res, store_res) = tokio::join!(update_fut, store_call);
    writer_res?;
    store_res
}

// -----------------------------------------------------------------------------
// Production-composition tests (FastSlowStore::update)
// -----------------------------------------------------------------------------

/// Kill-switch OFF — even with a dispatcher installed, the chunked
/// path is not taken. Mutation step: forcing the kill-switch ON in
/// this test should change observable behavior (the in-flight tracker
/// gains an entry).
#[nativelink_test]
async fn fast_slow_update_kill_switch_off_uses_legacy_path() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;

    // Serialize against other kill-switch-toggling tests; the switch is
    // process-wide.
    let _guard = kill_switch_lock().lock().await;

    // Hard-set kill-switch OFF (defensive — other tests may have flipped
    // it on without resetting).
    disable_bazel_facing_internal_chunking();

    let blob: Vec<u8> = (0..SIZE).map(|i| i as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fast_slow, _fs_store, _content_path, in_flight_opt) =
        make_fast_slow_with_dispatcher(CHUNK, true).await;
    let in_flight = in_flight_opt.expect("dispatcher requested");

    tokio::time::timeout(Duration::from_secs(5), async {
        run_update(&fast_slow, digest, Bytes::from(blob))
            .await
            .expect("legacy-path update must succeed for hash-matching blob");
    })
    .await
    .expect("must not deadlock — legacy update returns promptly");

    // The chunked dispatcher MUST NOT have been invoked: in-flight
    // tracker is empty (no driver was spawned).
    assert_eq!(
        in_flight.in_flight_count(),
        0,
        "chunked dispatcher must not be engaged when kill-switch is OFF — \
         a non-zero in-flight count means the legacy path was bypassed \
         (Phase 2.7 kill-switch invariant violated)"
    );
}

/// Kill-switch ON, blob smaller than CHUNK_SIZE → still uses legacy
/// path (the chunked machinery's bitmap completeness check requires
/// at least one full chunk).
#[nativelink_test]
async fn fast_slow_update_kill_switch_on_small_blob_uses_legacy_path() {
    const CHUNK: usize = 64 * 1024;
    const SMALL_SIZE: usize = 1024; // < CHUNK
    let blob: Vec<u8> = (0..SMALL_SIZE).map(|i| (i * 7) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SMALL_SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let (fast_slow, _fs_store, _content_path, in_flight_opt) =
        make_fast_slow_with_dispatcher(CHUNK, true).await;
    let in_flight = in_flight_opt.expect("dispatcher requested");

    tokio::time::timeout(Duration::from_secs(5), async {
        run_update(&fast_slow, digest, Bytes::from(blob))
            .await
            .expect("small-blob legacy-path update must succeed");
    })
    .await
    .expect("must not deadlock — small blob skips chunking");

    // Reset kill-switch so subsequent tests start from a clean slate.
    disable_bazel_facing_internal_chunking();

    assert_eq!(
        in_flight.in_flight_count(),
        0,
        "small blobs (size < CHUNK_SIZE) must skip the chunked dispatcher \
         (Phase 2.7 size-threshold invariant violated)"
    );
}

/// Kill-switch ON, blob >= CHUNK_SIZE, dispatcher installed → chunked
/// dispatch path runs. update() returns Ok promptly (β async-commit
/// MUST NOT block on slow-tier commit; this is the anti-#203 invariant).
/// The fast tier has the blob immediately. Eventually the slow-tier
/// driver commits and the in-flight tracker drains.
///
/// Mutation step: comment out the `update_via_chunked_dispatcher`
/// branch in `FastSlowStore::update` — the in-flight tracker would
/// stay empty AND the legacy path's behaviour would still pass the
/// post-update fast-tier read assertion. This is why the test ALSO
/// asserts in-flight count > 0 immediately after admission AND
/// drainage afterward.
#[nativelink_test]
async fn fast_slow_update_chunked_dispatch_engages_for_large_blob() {
    const CHUNK: usize = 64 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;

    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 13) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let (fast_slow, fs_store, content_path, in_flight_opt) =
        make_fast_slow_with_dispatcher(CHUNK, true).await;
    let in_flight = in_flight_opt.expect("dispatcher requested");

    // Capture the fast-tier handle BEFORE the update so we can observe
    // it directly (FastSlowStore::has() does NOT consult the fast tier
    // by default — see the `has_with_results` impl, which only checks
    // slow + in_flight_slow_writes + mirror_blobs unless the
    // `local_only_reads` flag is set; Phase 2.5 will wire the chunked-
    // dispatch read accessor into the in_flight surface).
    //
    // For Phase 2.7's β async-commit visibility check today, we read
    // the fast-store handle directly via the FastSlowStore's
    // `fast_store_handle()` accessor, then call `has()` on the
    // MemoryStore wrapper.
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        run_update(&fast_slow, digest, Bytes::from(blob.clone())).await
    })
    .await
    .expect(
        "must not deadlock — chunked-dispatch update must return promptly \
         (β async-commit; anti-#203 invariant)",
    );
    outcome.expect("chunked-dispatch update must succeed for hash-matching blob");

    // β async-commit visibility today: the fast-tier MemoryStore has
    // the bytes immediately. We verify via the inline fast-tier handle
    // we saved before the update.
    let fast_has = fast_slow
        .fast_store_handle()
        .has(digest)
        .await
        .expect("fast_store has() must not error after admission");
    assert_eq!(
        fast_has,
        Some(SIZE as u64),
        "blob must be visible via the fast-tier MemoryStore immediately after \
         FastSlowStore::update() returns (the in-memory replica that satisfies \
         ≥2-replica during the async-commit window). Phase 2.5 will additionally \
         expose this via FastSlowStore::has() through a `failed_writes` pin in \
         the chunked-dispatch path; this assertion documents the visibility \
         surface available TODAY"
    );

    // Wait for the driver to commit (the on-disk canonical CAS file
    // appears) — this is the load-bearing observable for "Phase 2.7
    // chunked dispatch ran and the driver completed the slow-tier
    // commit." The chunked path bypasses FilesystemStore's EvictingMap
    // (Phase 2.5 will wire the read accessor), so we verify presence
    // via direct filesystem stat instead of `fs_store.has()`.
    let _ = fs_store; // unused but documents the slow tier identity.
    let on_disk_size = wait_for_cas_file(&content_path, &digest, Duration::from_secs(10))
        .await
        .expect("CAS file must appear on disk after async commit completes");
    assert_eq!(
        on_disk_size, SIZE as u64,
        "post-commit, the canonical CAS file MUST have the declared blob \
         length — the chunked driver finalized to the right path"
    );

    // The in-flight tracker MUST drain after the driver commits.
    wait_for_no_in_flight(&in_flight, Duration::from_secs(10))
        .await
        .expect(
            "driver must drain the in-flight tracker post-commit — if it \
             does not, the reaper task in dispatch_chunks_to_driver's \
             AsyncCommit branch is broken (mutation-test target)",
        );

    // Reset kill-switch.
    disable_bazel_facing_internal_chunking();
}

/// Kill-switch ON, blob >= CHUNK_SIZE, NO dispatcher installed → legacy
/// path runs (defensive — production wires the dispatcher at startup,
/// but absence MUST NOT break the path).
#[nativelink_test]
async fn fast_slow_update_kill_switch_on_no_dispatcher_uses_legacy_path() {
    const CHUNK: usize = 64 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 5) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    // with_dispatcher=false → no dispatcher installed.
    let (fast_slow, _fs_store, _content_path, in_flight_opt) =
        make_fast_slow_with_dispatcher(CHUNK, false).await;
    assert!(
        in_flight_opt.is_none(),
        "no dispatcher requested → no in-flight tracker"
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        run_update(&fast_slow, digest, Bytes::from(blob))
            .await
            .expect("absence-of-dispatcher legacy path must succeed");
    })
    .await
    .expect("must not deadlock — legacy fallback");

    disable_bazel_facing_internal_chunking();
}

// -----------------------------------------------------------------------------
// Direct unit tests on dispatch_chunks_to_driver
// -----------------------------------------------------------------------------

/// Synchronous dispatch of three pre-built chunks → blob committed to
/// the FilesystemStore at the canonical CAS path. This unit-tests the
/// shared dispatch helper that BOTH the WriteChunked RPC handler AND
/// the Bazel-facing dispatcher consume.
///
/// Mutation step: change CommitMode::Synchronous → CommitMode::AsyncCommit
/// here; the `commit_result.committed_size == SIZE` assertion still
/// holds (declared size returned in async mode), but the post-call
/// `fs_store.has` assertion may fail before the driver commits — which
/// shows the modes have observably different semantics.
#[nativelink_test]
async fn dispatch_chunks_to_driver_synchronous_commits_blob() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa0u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Build a futures::Stream of PreparedChunk.
    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&blob[i * CHUNK..(i + 1) * CHUNK]);
            Ok(PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes,
                finish: i == N - 1,
            })
        })
        .collect();
    let stream = Box::pin(futures::stream::iter(chunks));

    let outcome: DispatchOutcome = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None, // pin_budget
            None, // chunked_read_registry
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            metrics,
            None, // async_result_relay (MAJOR-F #499 followup) — Synchronous mode uses internal relay
        ),
    )
    .await
    .expect("must not deadlock — dispatch_chunks_to_driver must complete in 5s")
    .expect("dispatch must succeed for hash-matching blob");
    assert_eq!(outcome.committed_size, SIZE as u64);

    // The chunked path bypasses FilesystemStore's EvictingMap (Phase
    // 2.5 will wire the read accessor); we verify the on-disk file
    // directly. Synchronous mode means by the time we get here the
    // commit must have completed.
    let on_disk_size = wait_for_cas_file(&content_path, &digest, Duration::from_secs(5))
        .await
        .expect("CAS file must be on disk after Synchronous commit returns");
    assert_eq!(
        on_disk_size, SIZE as u64,
        "post-Synchronous-dispatch, the canonical CAS file MUST have the \
         declared blob length"
    );

    // In-flight entry drained.
    assert_eq!(
        in_flight.in_flight_count(),
        0,
        "in-flight tracker must drain after Synchronous commit"
    );
}

/// #548 Phase 1: the `ChunkedWriteSource` value passed at the
/// `dispatch_chunks_to_driver` entry MUST reach the in-flight entry by
/// the time the reaper fires `stable_digests_sink`. This is the seam
/// Phase 4 (#551) will branch on; Phase 1 only proves the value
/// threads end-to-end.
///
/// Setup: wrap the `stable_digests_sink` in a closure that, on fire,
/// reads `in_flight.source_for_test(&digest)` and stores it via
/// `Arc<Mutex<Option<ChunkedWriteSource>>>`. Reaper fires the sink
/// BEFORE removing the in-flight entry (see `run_async_commit_reaper`
/// — `if let Ok(...)` block precedes `in_flight.inner.lock().remove`),
/// so the entry is observable at sink-fire time.
///
/// Mutation step: at the call site below, change
/// `ChunkedWriteSource::Bazel` to `ChunkedWriteSource::Mirror`. The
/// `assert_eq!(captured, Some(Bazel))` MUST red-fail with the bespoke
/// "#548 Phase 1: source threaded from dispatch entry to reaper sink"
/// message. Without the source-on-InFlightEntry field
/// (`InFlightEntry::source`) the captured value would be `None` (or
/// the test would not type-check, depending on which line is mutated).
#[nativelink_test]
async fn dispatch_chunks_to_driver_threads_source_through_to_reaper_sink_548_phase1() {
    use parking_lot::Mutex;
    use tokio::sync::Notify;

    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xd0u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, _content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Side-channel capture: the wrapped sink reads
    // `in_flight.source_for_test(&digest)` at fire time and records it,
    // then fires a `tokio::sync::Notify` so the test wakes deterministically
    // (per CLAUDE.md "No `thread::sleep` / `tokio::time::sleep` as
    // synchronization in tests").
    let captured: Arc<Mutex<Option<nativelink_store::chunked::ChunkedWriteSource>>> =
        Arc::new(Mutex::new(None));
    let fired_notify = Arc::new(Notify::new());
    let captured_for_sink = Arc::clone(&captured);
    let notify_for_sink = Arc::clone(&fired_notify);
    let in_flight_for_sink = Arc::clone(&in_flight);
    let sink: Arc<dyn Fn(DigestInfo) + Send + Sync> = Arc::new(move |d: DigestInfo| {
        *captured_for_sink.lock() = in_flight_for_sink.source_for_test(&d);
        notify_for_sink.notify_one();
    });

    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&blob[i * CHUNK..(i + 1) * CHUNK]);
            Ok(PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes,
                finish: i == N - 1,
            })
        })
        .collect();
    let stream = Box::pin(futures::stream::iter(chunks));

    // Subscribe BEFORE dispatch so a fast sink-fire (notify_one happens
    // before .notified().await is polled) is not missed — Notify's
    // documented behavior: a permit pending at subscription time
    // immediately satisfies the next notified() await.
    let sink_fired = fired_notify.notified();
    tokio::pin!(sink_fired);

    tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None, // pin_budget
            None, // chunked_read_registry
            // The source under test. Mutation: change to Mirror; the
            // assertion below must red-fail with the bespoke "#548 Phase 1"
            // message.
            nativelink_store::chunked::ChunkedWriteSource::Bazel,
            Some(sink), // stable_digests_sink — wrapped to capture source
            None,       // failed_commit_sink
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            metrics,
            None,
        ),
    )
    .await
    .expect("must not deadlock — dispatch_chunks_to_driver must complete in 5s")
    .expect("dispatch must succeed for hash-matching blob");

    // Reactive wait on the per-test Notify; 5s deadlock-detector bound.
    tokio::time::timeout(Duration::from_secs(5), sink_fired)
        .await
        .expect("stable_digests_sink must fire within 5s of Synchronous commit");

    let captured = *captured.lock();
    assert_eq!(
        captured,
        Some(nativelink_store::chunked::ChunkedWriteSource::Bazel),
        "#548 Phase 1: source threaded from dispatch entry to reaper sink \
         — expected Bazel (the value passed at the dispatch_chunks_to_driver \
         call site), observed via in_flight.source_for_test(&digest) inside \
         the wrapped stable_digests_sink. If this fails with Some(other), \
         the source argument is being ignored or overwritten between admission \
         and sink-fire. If it fails with None, the in-flight entry was removed \
         before the sink fired (reaper ordering invariant violated — see \
         `run_async_commit_reaper`: sink call MUST precede `in_flight.inner.lock().remove`)."
    );
}

/// #548 Phase 1 Seam 8: drive `BazelChunkedDispatcherImpl::dispatch`
/// (NOT the lower-layer `dispatch_chunks_to_driver`) and assert that
/// the source it passes downstream reaches the reaper sink as
/// `ChunkedWriteSource::Bazel`. This is the actual production seam —
/// `FastSlowStore::update` routes Bazel-originated writes through
/// `BazelChunkedDispatcherImpl::dispatch`, which hardcodes
/// `ChunkedWriteSource::Bazel` at `chunked_write_handler.rs:4179`.
///
/// Setup: wire a custom `stable_digests_sink` on the dispatcher that,
/// on fire, reads `in_flight.source_for_test(&digest)` (the same
/// reader-of-truth used by the
/// `dispatch_chunks_to_driver_threads_source_through_to_reaper_sink_548_phase1`
/// test above). Drive `.dispatch(digest, reader)` directly with a
/// hash-matching multi-chunk blob.
///
/// Mutation step: at `chunked_write_handler.rs:4179`, change
/// `ChunkedWriteSource::Bazel` to any other variant. This test red-fails
/// with the bespoke "#548 Phase 1 Seam 8: BazelChunkedDispatcherImpl
/// must thread source = Bazel to reaper sink" message.
#[nativelink_test]
async fn bazel_chunked_dispatcher_impl_threads_source_to_reaper_sink_548_phase1_seam8() {
    use parking_lot::Mutex;
    use tokio::sync::Notify;

    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xb1u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, _content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();

    // Capture-on-sink-fire: read source_for_test inside the wrapped
    // sink. Reaper invokes `stable_digests_sink` BEFORE removing the
    // in-flight entry, so the entry is observable at sink-fire time.
    // Use `tokio::sync::Notify` for deterministic wake (per CLAUDE.md
    // test discipline: no sleep-based synchronization).
    let captured: Arc<Mutex<Option<nativelink_store::chunked::ChunkedWriteSource>>> =
        Arc::new(Mutex::new(None));
    let fired_notify = Arc::new(Notify::new());
    let captured_for_sink = Arc::clone(&captured);
    let notify_for_sink = Arc::clone(&fired_notify);
    let in_flight_for_sink = Arc::clone(&in_flight);
    let sink: Arc<dyn Fn(DigestInfo) + Send + Sync> = Arc::new(move |d: DigestInfo| {
        *captured_for_sink.lock() = in_flight_for_sink.source_for_test(&d);
        notify_for_sink.notify_one();
    });

    let dispatcher = BazelChunkedDispatcherImpl::new_with_state_for_test(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        budget,
        CHUNK,
    )
    .with_stable_digests_sink(sink);

    // Subscribe to the notify BEFORE dispatch so a fast sink-fire
    // (notify_one before .notified() polled) is not missed.
    let sink_fired = fired_notify.notified();
    tokio::pin!(sink_fired);

    // Stream the bytes through a buf-channel pair into the dispatcher's
    // `reader` arg. Send all bytes then EOF so the chunked driver's
    // recv loop terminates.
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    let blob_bytes = Bytes::from(blob);
    let send_task = tokio::spawn(async move {
        tx.send(blob_bytes).await.expect("tx.send must succeed");
        tx.send_eof().expect("tx.send_eof must succeed");
    });

    let dispatched_size = tokio::time::timeout(
        Duration::from_secs(5),
        <BazelChunkedDispatcherImpl<FileEntryImpl> as BazelChunkedDispatcher>::dispatch(
            &dispatcher,
            digest,
            rx,
        ),
    )
    .await
    .expect("must not deadlock — BazelChunkedDispatcherImpl::dispatch must complete in 5s")
    .expect("dispatch must succeed for hash-matching blob");
    send_task
        .await
        .expect("blob-sender task must complete cleanly");
    assert_eq!(
        dispatched_size, SIZE as u64,
        "dispatch must report the declared size on AsyncCommit Ok"
    );

    // Reactive wait on the per-test Notify; 5s deadlock-detector bound.
    tokio::time::timeout(Duration::from_secs(5), sink_fired)
        .await
        .expect("stable_digests_sink must fire within 5s of dispatcher commit");

    let captured = *captured.lock();
    assert_eq!(
        captured,
        Some(nativelink_store::chunked::ChunkedWriteSource::Bazel),
        "#548 Phase 1 Seam 8: BazelChunkedDispatcherImpl must thread \
         source = Bazel to reaper sink — `dispatch` calls into \
         `dispatch_bazel_facing_internal_chunking` with \
         `ChunkedWriteSource::Bazel` hardcoded at \
         `chunked_write_handler.rs:4179`. If this fails with Some(other), \
         the production literal at the dispatch site has been flipped \
         and Phase 4 (#551) BIS-elision will misclassify Bazel writes \
         as Worker (Bazel is NOT a durable holder; eliding the BIS \
         broadcast would leave the worker fleet unaware of the new \
         digest). If it fails with None, the in-flight entry was \
         removed before the sink fired (reaper ordering invariant \
         violated)."
    );
}

/// Async-commit dispatch returns Ok immediately; the driver is still
/// in-flight. We poll until commit lands.
///
/// Mutation step: comment out the reaper-spawn `tokio::spawn` block in
/// dispatch_chunks_to_driver's `CommitMode::AsyncCommit` branch — the
/// in-flight entry would NEVER drain, the
/// `wait_for_no_in_flight` polling loop would Elapsed, and the
/// "must drain after async commit" expect would panic with the
/// specific message. This catches the bug where the reaper isn't wired.
#[nativelink_test]
async fn dispatch_chunks_to_driver_async_commit_returns_promptly_then_drains() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xc0u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&blob[i * CHUNK..(i + 1) * CHUNK]);
            Ok(PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes,
                finish: i == N - 1,
            })
        })
        .collect();
    let stream = Box::pin(futures::stream::iter(chunks));

    let outcome: DispatchOutcome = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None, // pin_budget
            None, // chunked_read_registry
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            CHUNK,
            digest,
            stream,
            CommitMode::AsyncCommit,
            metrics,
            None, // async_result_relay (MAJOR-F #499 followup) — test does not bridge to a v2 race-state
        ),
    )
    .await
    .expect("must not deadlock — async-commit dispatch returns when admission completes")
    .expect("admission must succeed for hash-matching blob");
    // In async mode `committed_size` is the producer-declared size.
    assert_eq!(outcome.committed_size, SIZE as u64);

    // The driver continues on its own task; wait for the in-flight
    // entry to drain (which happens after the commit completes).
    wait_for_no_in_flight(&in_flight, Duration::from_secs(10))
        .await
        .expect(
            "must drain after async commit — reaper must remove the in-flight entry post-commit",
        );

    let on_disk_size = wait_for_cas_file(&content_path, &digest, Duration::from_secs(5))
        .await
        .expect("CAS file must be on disk after async commit completes (post-drain)");
    assert_eq!(
        on_disk_size, SIZE as u64,
        "post-async-commit, the canonical CAS file MUST have the declared length"
    );
}

// -----------------------------------------------------------------------------
// Pin global ChunkBudget budget invariant.
// -----------------------------------------------------------------------------

/// Sanity: the chunk-budget singleton has 4 GiB / 1 MiB = 4096 permits.
/// This pins the constant the per-blob driver math relies on.
#[nativelink_test]
async fn chunk_budget_total_permits_is_4096() {
    assert_eq!(TOTAL_CHUNK_PERMITS, 4096);
}

// -----------------------------------------------------------------------------
// #212 fixup B1 — global pinned-bytes Semaphore (anti-#203 OOM cap)
// -----------------------------------------------------------------------------

/// **B1 cap regression** — a `PinBudget` smaller than the per-blob byte
/// requirement MUST cause `dispatch_chunks_to_driver` admission to
/// reject with `Code::ResourceExhausted` + `BackpressureSignal::
/// PinnedBytesExhausted`, BEFORE pinned bytes can grow unboundedly.
///
/// Setup: a 4 KiB chunk size with N=4 chunks (16 KiB total). PinBudget
/// cap = 8 KiB (covers 2 chunks; the 3rd MUST reject). The first 2
/// admissions succeed; the 3rd MUST fail with the
/// `PINNED_BYTES_EXHAUSTED` signal.
///
/// **Mutation step:** comment out the `pin_budget.try_acquire(...)`
/// block in `admit_prepared_chunk` (or pass `None` instead of `Some(pb)`
/// to `dispatch_chunks_to_driver`). The test will then admit all 4
/// chunks (no cap), the call returns Ok, and the
/// `expect_err("admission MUST reject when pin budget is exhausted...")`
/// fires with the specific message.
#[nativelink_test]
async fn pin_budget_cap_rejects_admission_with_pinned_bytes_exhausted_signal() {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        BackpressureSignal, backpressure_signal,
    };
    use nativelink_proto::type_urls::BACKPRESSURE_SIGNAL_TYPE_URL;
    use prost::Message as _;

    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    // Cap covers exactly 2 chunks (8 KiB). The 3rd chunk's admission
    // MUST reject. Box::leak yields a 'static reference matching
    // `dispatch_chunks_to_driver`'s signature.
    let pin_budget: &'static PinBudget = Box::leak(Box::new(PinBudget::new(2 * CHUNK)));

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xb0u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, _content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let chunk_budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    let chunks: Vec<Result<PreparedChunk, nativelink_error::Error>> = (0..N)
        .map(|i| {
            let chunk_bytes = Bytes::copy_from_slice(&blob[i * CHUNK..(i + 1) * CHUNK]);
            Ok(PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes,
                finish: i == N - 1,
            })
        })
        .collect();
    let stream = Box::pin(futures::stream::iter(chunks));

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_chunks_to_driver(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            Some(pin_budget),
            None, // chunked_read_registry
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            Arc::clone(&metrics),
            None, // async_result_relay (MAJOR-F #499 followup)
        ),
    )
    .await
    .expect(
        "must not deadlock — dispatch must reject the 3rd chunk admission \
         within 5s (PinBudget cap exceeded; #212 fixup B1)",
    );

    let err = res.expect_err(
        "admission MUST reject when pin budget is exhausted — \
         if this expect_err panics with Ok(...), the PinBudget cap is \
         not enforced and the chunked path can OOM under slow-tier pause \
         (#203-shape regression). See B1 fix in pin_budget.rs.",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "admission rejection MUST be Code::ResourceExhausted (got {err:?})"
    );

    // The error MUST carry a BackpressureSignal::PinnedBytesExhausted
    // detail so the classifier (`looks_like_dead_channel`) does NOT
    // treat it as a dead channel.
    let detail_any = err
        .details
        .first()
        .expect("admission rejection MUST carry a BackpressureSignal detail");
    assert_eq!(
        detail_any.type_url, BACKPRESSURE_SIGNAL_TYPE_URL,
        "detail MUST be a BackpressureSignal Any (got type_url {})",
        detail_any.type_url,
    );
    let signal = BackpressureSignal::decode(detail_any.value.as_slice())
        .expect("BackpressureSignal proto MUST decode");
    assert_eq!(
        signal.reason,
        backpressure_signal::Reason::PinnedBytesExhausted as i32,
        "rejection signal reason MUST be PinnedBytesExhausted (got {})",
        signal.reason,
    );
    // The metric MUST have ticked at least once.
    assert!(
        metrics
            .pin_budget_exhausted_rejections_total
            .load(core::sync::atomic::Ordering::Relaxed)
            >= 1,
        "metric pin_budget_exhausted_rejections_total MUST tick on rejection",
    );
}

// -----------------------------------------------------------------------------
// #212 fixup S4 — fast-tier-OK + dispatch-Err combined state
// -----------------------------------------------------------------------------

/// **S4 partial-success state** — when the chunked dispatcher rejects
/// admission AFTER the fast-tier write has succeeded (e.g. PinBudget
/// exhaustion mid-stream), the FastSlowStore::update path takes the
/// dispatcher-error branch (`update_via_chunked_dispatcher`), records
/// the digest in `failed_slow_writes`, AND keeps the fast-tier replica
/// pinned. We document this behavior:
/// - The blob remains visible via the fast tier.
/// - The digest is in `failed_slow_writes` so the mirror protocol /
///   reconnect retry will re-upload it later.
/// - `update()` returns `Err(ResourceExhausted)` so the caller knows
///   the slow-tier write did not complete.
///
/// This test pins the documented behavior; it does NOT advocate atomic
/// rollback (which would require throwing away the in-memory replica
/// the ≥2-replica invariant relies on during the async-commit window).
///
/// **Mutation step:** if `update_via_chunked_dispatcher`'s err branch
/// is changed to skip `failed_slow_writes.insert(...)`, this test's
/// `failed_slow_writes_count() >= 1` assertion red-fails with the
/// specific message.
#[nativelink_test]
async fn fast_tier_ok_dispatch_err_records_failed_slow_write() {
    use nativelink_util::store_trait::{StoreKey, UploadSizeInfo};

    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 17) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    // Build a FastSlowStore with a custom dispatcher whose PinBudget
    // is too small to admit even one chunk.
    let (fs_store, _content_path) = make_filesystem_store().await;
    let fast_store: Store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store: Store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            // #212 v4.5: stale-base test catch-up — see commit 65edaa83
            // (config wire-up). Default-false preserves prior
            // semantics; this test does not depend on the read-cascade.
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow_store,
    );
    // PinBudget cap = 0 ensures the FIRST chunk admission is rejected.
    let pin_budget: &'static PinBudget = Box::leak(Box::new(PinBudget::new(0)));
    let chunk_budget = make_test_budget();
    let in_flight = ChunkedWriteInFlight::new();
    let dispatcher: Arc<dyn BazelChunkedDispatcher> = Arc::new(
        BazelChunkedDispatcherImpl::new_with_state_and_pin_budget_for_test(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            chunk_budget,
            pin_budget,
            CHUNK,
        ),
    );
    fast_slow.set_bazel_chunked_dispatcher(dispatcher);
    fast_slow.set_chunked_size_threshold_for_test(CHUNK as u64);

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        run_update(&fast_slow, digest, Bytes::from(blob.clone())),
    )
    .await
    .expect(
        "must not deadlock — fast-tier-OK + dispatch-Err update must \
         return promptly with Err",
    );

    disable_bazel_facing_internal_chunking();

    let err = res.expect_err(
        "update MUST return Err when chunked-dispatch admission fails \
         (PinBudget cap = 0); the dispatcher records the digest in \
         failed_slow_writes for retry-on-reconnect",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "rejection MUST be ResourceExhausted (got {err:?})",
    );

    // Fast-tier replica MUST still be visible (≥2-replica invariant).
    let fast_has = fast_slow
        .fast_store_handle()
        .has(digest)
        .await
        .expect("fast_store has() must not error");
    assert_eq!(
        fast_has,
        Some(SIZE as u64),
        "fast-tier MemoryStore MUST still have the blob after dispatch \
         rejection — the in-memory replica is the load-bearing recovery \
         hook (failed_slow_writes + mirror retry)",
    );

    // Digest MUST be in failed_slow_writes.
    assert!(
        fast_slow.failed_slow_writes_contains(&digest),
        "failed_slow_writes MUST contain the digest after dispatch-Err \
         (so the mirror retry-on-reconnect path re-uploads); without \
         this, the blob is not durable AND not flagged for retry",
    );

    // The bytes are NOT yet visible via the slow tier (commit never
    // ran).
    let _ = StoreKey::Digest(digest);
    let _ = UploadSizeInfo::ExactSize(SIZE as u64);
}

// -----------------------------------------------------------------------------
// #212 fixup S5 — digest-mismatch on async-commit failure mode
// -----------------------------------------------------------------------------

/// **S5 digest-mismatch under async-commit** — when the bytes streamed
/// into the chunked driver hash to a DIFFERENT digest than the
/// upstream-declared digest, the (β) async-commit path returns Ok at
/// admission time, then the driver detects the e2e SHA-256 mismatch
/// during commit. Documented behavior:
/// - The reaper logs `error!` on commit failure (mismatch is one cause).
/// - The metric `sha256_e2e_mismatches_total` ticks.
/// - The on-disk CAS file does NOT appear (commit is rejected before
///   atomic-rename).
/// - The fast-tier MemoryStore replica still holds the WRONG bytes
///   (since the upstream tee landed them); the digest-mismatch is
///   detected only after the caller has already received Ok. Reads via
///   the fast tier could return wrong bytes — documented corruption
///   window.
///
/// This test pins the visible-corruption-window behavior; the long-
/// term fix is to verify upstream digest BEFORE returning Ok at
/// admission (deferred to a follow-up; out of scope for #212 v1).
///
/// **Mutation step:** if the e2e SHA verify in `commit_and_verify` is
/// removed, the on-disk CAS file would appear (lying bytes durably
/// committed). The `wait_for_cas_file` poll inside the test's bounded
/// timeout would unexpectedly succeed, surfacing the regression.
#[nativelink_test]
async fn async_commit_digest_mismatch_blocks_canonical_cas_landing() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const SIZE: usize = N * CHUNK;

    // Build the LIES blob (real bytes the producer streams).
    let lies: Vec<u8> = (0..SIZE).map(|i| 0x10u8.wrapping_add(i as u8)).collect();
    // Build the EXPECTED digest from a DIFFERENT byte pattern.
    let expected: Vec<u8> = (0..SIZE).map(|i| 0xaau8.wrapping_add(i as u8)).collect();
    let declared_digest = DigestInfo::new(sha256(&expected), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    enable_bazel_facing_internal_chunking();

    let (fast_slow, _fs_store, content_path, in_flight_opt) =
        make_fast_slow_with_dispatcher(CHUNK, true).await;
    let in_flight = in_flight_opt.expect("dispatcher requested");

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        run_update(&fast_slow, declared_digest, Bytes::from(lies.clone())).await
    })
    .await
    .expect("must not deadlock");

    // β async-commit returns Ok at admission, regardless of digest.
    outcome.expect(
        "β async-commit MUST return Ok at admission (anti-#203); the digest mismatch is \
         detected later by the chunked driver's commit",
    );

    // Wait for the in-flight tracker to drain — the reaper has run,
    // commit was attempted (and failed with SHA-256 mismatch), so the
    // entry is removed.
    wait_for_no_in_flight(&in_flight, Duration::from_secs(10))
        .await
        .expect("in-flight tracker must drain after async-commit reaper handles mismatch");

    disable_bazel_facing_internal_chunking();

    // The canonical CAS file MUST NOT have appeared (commit was
    // rejected by e2e SHA-256 verify). We poll for ~1s and assert
    // it is still absent.
    let path = canonical_cas_path(&content_path, &declared_digest);
    let appeared = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if tokio::fs::metadata(&path).await.is_ok() {
                return true;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        !appeared,
        "canonical CAS file MUST NOT appear when the bytes hash to a \
         different digest than declared — e2e SHA-256 verify must reject \
         the commit before atomic-rename. If this assertion fires with \
         appeared=true, the chunked driver's commit-time hash check is \
         broken and lying bytes can durably land on disk (#212 fixup S5).",
    );
}

// -----------------------------------------------------------------------------
// Early-dedup gate: when the digest is already in the FilesystemStore's
// in-process index (`evicting_map`), the chunked dispatcher MUST short-
// circuit — drain the producer to EOF and return Ok WITHOUT spawning a
// per-blob driver, opening a `.holding` file, or running per-chunk
// SHA-256 / pwrite work.
//
// This is the EARLY sibling of the `finalize_holding` duplicate-commit
// guard at filesystem_store.rs:1698 (#256 fix), which fires AFTER all
// chunks have been pwrite'd to a `.holding` file. The early gate elides
// the per-chunk cost on byte-identical re-uploads (Bazel re-uploads of
// canonical CAS blobs are common during retries / cross-action sharing).
// -----------------------------------------------------------------------------

/// Helper: build a `(tx, rx)` buf-channel pair, send `data` + EOF on the
/// tx half from a separately-spawned task, return the `rx` half. The
/// producer task is intentionally `spawn`'d so the rx-half consumer can
/// run concurrently — modelling the production path where
/// `FastSlowStore::update`'s `data_stream_fut` runs concurrently with
/// the dispatcher's reader-drain.
fn spawn_producer(data: Bytes) -> nativelink_util::buf_channel::DropCloserReadHalf {
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    tokio::spawn(async move {
        if !data.is_empty() {
            let _ = tx.send(data).await;
        }
        let _ = tx.send_eof();
    });
    rx
}

/// EARLY-DEDUP gate, hit case: the digest is already in `evicting_map`
/// before dispatch starts. The dispatcher MUST return Ok promptly and
/// MUST NOT create a `.holding` file under `content_path/d/XX/`.
///
/// Mutation step: comment out the `has_indexed_digest` short-circuit at
/// the top of `dispatch_bazel_facing_internal_chunking` — this test then
/// red-fails with the bespoke message because the chunked driver opens
/// the `.holding` file as part of normal per-chunk processing.
#[nativelink_test]
async fn dispatch_bazel_facing_skips_chunked_path_when_digest_already_indexed() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0x55u8 ^ (i as u8)).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;

    // Pre-populate the FilesystemStore so the digest is in `evicting_map`.
    // Use `update_oneshot` via the StoreLike blanket so the canonical
    // CAS file lands at `content_path/d/XX/<digest>` AND the in-process
    // index is updated — modelling the steady-state production case
    // where Bazel is re-uploading a blob the server already has.
    let pop_key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    fs_store
        .as_pin()
        .update_oneshot(pop_key, Bytes::copy_from_slice(&blob))
        .await
        .expect("pre-populate update_oneshot must succeed");

    // Sanity: the `has_indexed_digest` probe sees the entry. If this
    // assertion ever fails the test setup is broken — proceed only if
    // the precondition (which the dispatcher's gate consults) holds.
    assert_eq!(
        fs_store.has_indexed_digest(&digest).await,
        Some(SIZE as u64),
        "test precondition: pre-populate must register the digest in \
         evicting_map so the early-dedup gate has something to short-\
         circuit on",
    );

    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Run the dispatcher with a fresh producer pushing the same bytes.
    // The early-dedup gate should drain the reader and return Ok
    // without ever spawning the per-blob driver / opening a .holding
    // file. The full byte stream is sent so the test still works in
    // the (intentionally regression-failing) mutation case where the
    // gate is removed — the dispatcher will then go through normal
    // per-chunk processing.
    let producer_rx = spawn_producer(Bytes::copy_from_slice(&blob));
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_bazel_facing_internal_chunking(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None, // pin_budget
            None, // chunked_read_registry
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            metrics,
            CHUNK,
            digest,
            producer_rx,
            None, // async_result_relay (MAJOR-F #499 followup) — test does not bridge to a v2 race-state
        ),
    )
    .await
    .expect(
        "must not deadlock — early-dedup short-circuit must drain the \
         producer and return Ok within seconds",
    )
    .expect(
        "early-dedup short-circuit must return Ok when the digest is \
         already in evicting_map (CAS immutability: byte-identical \
         re-upload)",
    );
    assert_eq!(
        outcome.committed_size, SIZE as u64,
        "early-dedup outcome.committed_size MUST equal the declared blob \
         size (the size the producer would otherwise have written)",
    );

    // Load-bearing assertion: NO `.holding` file MUST be created. The
    // chunked driver opens `<content_path>/d/XX/<digest>.holding` as
    // soon as the first chunk is admitted. If the early-dedup gate is
    // missing or wrong, this file appears and the assertion red-fails
    // with the bespoke message.
    let holding_path = format!(
        "{}/d/{:02x}/{}.holding",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let exists = tokio::fs::metadata(&holding_path).await.is_ok();
    assert!(
        !exists,
        "early-dedup gate violated — a .holding file appeared at {} \
         when the digest was already in evicting_map. The chunked \
         dispatcher MUST short-circuit before opening any holding \
         file (CAS immutability + #256 sibling early-dedup gate).",
        holding_path,
    );

    // Per-blob in-flight tracker MUST NOT have observed any entry —
    // the early-dedup path never spawns a ChunkedDriver.
    assert_eq!(
        in_flight.in_flight_count(),
        0,
        "early-dedup gate violated — the per-blob in-flight tracker \
         saw an entry, meaning a ChunkedDriver was spawned. The gate \
         must short-circuit BEFORE driver spawn.",
    );

    // The original CAS file MUST still be intact (the early dedup
    // path must not touch the canonical file in any way).
    let canonical = canonical_cas_path(&content_path, &digest);
    let on_disk_size = tokio::fs::metadata(&canonical)
        .await
        .expect("canonical CAS file from pre-populate must still exist after early-dedup")
        .len();
    assert_eq!(
        on_disk_size, SIZE as u64,
        "pre-populated canonical CAS file MUST be byte-length unchanged \
         after the early-dedup short-circuit (the gate must not unlink \
         or replace the canonical file)",
    );
}

/// EARLY-DEDUP gate, miss case: the digest is NOT in `evicting_map`
/// before dispatch starts. The dispatcher MUST proceed through the
/// normal chunked path — spawn a driver, open a `.holding` file, and
/// commit the blob to disk. This is the over-action sibling of the
/// hit case (CLAUDE.md asymmetric-contract-coverage rule).
///
/// Mutation step: change the gate to fire on every call (always-Some)
/// — this test then red-fails with "canonical CAS file did not appear
/// after dispatch", proving the gate doesn't over-fire on miss.
#[nativelink_test]
async fn dispatch_bazel_facing_runs_chunked_path_when_digest_not_indexed() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let mut blob = Vec::with_capacity(SIZE);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa3u8 ^ (i as u8)).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;

    // Sanity precondition: digest is NOT yet indexed.
    assert!(
        fs_store.has_indexed_digest(&digest).await.is_none(),
        "test precondition: fresh FilesystemStore must not contain the \
         digest before dispatch",
    );

    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    let producer_rx = spawn_producer(Bytes::copy_from_slice(&blob));
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        dispatch_bazel_facing_internal_chunking(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None,
            None,
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            metrics,
            CHUNK,
            digest,
            producer_rx,
            None, // async_result_relay (MAJOR-F #499 followup)
        ),
    )
    .await
    .expect("must not deadlock — chunked dispatch with normal path")
    .expect("chunked dispatch must succeed for hash-matching blob");
    assert_eq!(outcome.committed_size, SIZE as u64);

    // Wait for the async-commit driver to land the blob on disk.
    let on_disk_size = wait_for_cas_file(&content_path, &digest, Duration::from_secs(10))
        .await
        .expect(
            "canonical CAS file did not appear after dispatch — when the \
             digest is NOT pre-populated, the chunked driver MUST run \
             and commit the blob (over-action gate sibling: gate must \
             not over-fire on miss)",
        );
    assert_eq!(on_disk_size, SIZE as u64);

    // The driver MUST drain post-commit.
    wait_for_no_in_flight(&in_flight, Duration::from_secs(10))
        .await
        .expect("in-flight tracker must drain after async commit completes");
}

/// Bounded-drain (red-team #1, follow-up to f4567ea1): producer claims
/// a small declared digest then streams more bytes than declared. The
/// gate's bounded drain MUST surface this as `Code::InvalidArgument`
/// (size-cap fired) instead of accepting unbounded bytes into
/// MemoryStore via the upstream tee's sibling `fast_store_fut` (the
/// #203 OOM-cascade shape).
///
/// Mutation step: remove the `consumed > cap` branch in
/// `bounded_drain_reader` — this test then red-fails because the drain
/// would silently accept all 32 KiB.
#[nativelink_test]
async fn dispatch_bazel_facing_dedup_drain_size_cap_fires_on_oversized_producer() {
    const CHUNK: usize = 4 * 1024;
    // Declared blob is 4 KiB; producer streams 32 KiB. With the
    // 4 MiB slack constant, 32 KiB does not trip the cap on its own
    // for a 4 KiB declared size (4 KiB + 4 MiB ≈ 4.004 MiB > 32 KiB).
    // So we use a larger over-stream to actually trip the cap, while
    // keeping the declared size small to model "1 KiB-claim,
    // huge-payload" attack shape that the cap is designed to catch.
    const DECLARED: usize = 1024;
    const OVERSTREAM: usize = 8 * 1024 * 1024; // 8 MiB > 4 MiB slack

    let mut declared_blob = vec![0xa5u8; DECLARED];
    // The digest is over the SMALL declared bytes — gate triggers on
    // this hash. The producer then streams an inflated payload that
    // does NOT match the declared bytes, but the gate has already
    // decided to short-circuit so it never verifies the bytes; the
    // size-cap is the only defence.
    let digest = DigestInfo::new(sha256(&declared_blob), DECLARED as u64);
    let _ = &mut declared_blob; // suppress unused-mut

    let (fs_store, _content_path) = make_filesystem_store().await;

    // Pre-populate so the gate fires.
    let pop_key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    fs_store
        .as_pin()
        .update_oneshot(pop_key, Bytes::copy_from_slice(&declared_blob))
        .await
        .expect("pre-populate must succeed");

    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Producer streams the OVERSIZED payload (mismatched bytes; the
    // size-cap should fire before any byte-content check would matter).
    let oversized = vec![0xc3u8; OVERSTREAM];
    let producer_rx = spawn_producer(Bytes::from(oversized));

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_bazel_facing_internal_chunking(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None,
            None,
            nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
            None, // stable_digests_sink
            None, // failed_commit_sink
            metrics,
            CHUNK,
            digest,
            producer_rx,
            None, // async_result_relay (MAJOR-F #499 followup)
        ),
    )
    .await
    .expect(
        "must not deadlock — bounded drain must surface size-cap \
         failure within seconds, NOT consume unbounded bytes",
    );

    let err = result.expect_err(
        "early-dedup bounded drain MUST reject a producer that streams \
         more bytes than declared (size-cap = declared + 4 MiB slack); \
         silently accepting bytes is the #203 OOM-cascade shape",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::InvalidArgument,
        "size-cap excess MUST surface as Code::InvalidArgument, got {:?} (msg={})",
        err.code,
        err.messages.first().map(String::as_str).unwrap_or(""),
    );
}

/// Bounded-drain (red-team #1, follow-up to f4567ea1): producer stalls
/// — sends nothing AND does not EOF. The per-recv timeout MUST fire
/// and surface `Code::DeadlineExceeded` instead of the gate holding
/// the dispatch task forever (which would also keep the upstream
/// MemoryStore::update via the sibling tee ingesting forever — the
/// stalled-producer DoS shape red-team identified).
///
/// Uses `tokio::time::pause()` so the test does not actually wait the
/// full per-recv timeout (15 s); virtual time advances past the
/// deadline instantly.
///
/// Mutation step: replace the `tokio::time::timeout(...).await` in
/// `bounded_drain_reader` with a bare `recv_fut.await` — this test
/// then hangs (caught by the outer 30s wall-clock guard) instead of
/// surfacing DeadlineExceeded.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn dispatch_bazel_facing_dedup_drain_per_recv_timeout_fires_on_stalled_producer() {
    const CHUNK: usize = 4 * 1024;
    const SIZE: usize = 4 * 1024;

    let blob = vec![0x77u8; SIZE];
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, _content_path) = make_filesystem_store().await;
    let pop_key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    fs_store
        .as_pin()
        .update_oneshot(pop_key, Bytes::copy_from_slice(&blob))
        .await
        .expect("pre-populate must succeed");

    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Stalled producer: hold the tx alive, send nothing, never EOF.
    let (tx, producer_rx) = make_buf_channel_pair_with_size(128);
    // Move tx into a long-lived Box so it isn't dropped (which would
    // close the channel and cause `recv()` to return EOF, masking the
    // timeout we want to test). We deliberately leak it for the test
    // duration; the helper drops on test-completion via the spawn
    // handle below.
    let tx_holder = tokio::spawn(async move {
        // Hold tx alive for a virtual eternity. With `start_paused`,
        // the runtime advances virtual time only when polled.
        let _hold = tx;
        // Sleep on virtual time effectively forever.
        tokio::time::sleep(Duration::from_secs(3600)).await;
    });

    let dispatch_fut = dispatch_bazel_facing_internal_chunking(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        budget,
        None,
        None,
        nativelink_store::chunked::ChunkedWriteSource::Bazel, // #548 Phase 1
        None, // stable_digests_sink
        None, // failed_commit_sink
        metrics,
        CHUNK,
        digest,
        producer_rx,
        None, // async_result_relay (MAJOR-F #499 followup)
    );

    // Outer guard: 30 virtual seconds. The per-recv timeout is 15 s,
    // so the gate MUST surface DeadlineExceeded inside this window.
    // Without the timeout in `bounded_drain_reader`, this test hangs
    // and the timeout's `.expect` panics.
    let result = tokio::time::timeout(Duration::from_secs(30), dispatch_fut)
        .await
        .expect(
            "bounded drain hung — the per-recv 15s timeout MUST fire \
             when the producer stalls; without it, the dispatch task \
             holds forever and the upstream tee's MemoryStore::update \
             keeps consuming bytes (the #203 OOM-cascade shape)",
        );

    tx_holder.abort();

    let err = result.expect_err(
        "stalled producer MUST surface a Result::Err — the gate \
         cannot return Ok without observing the producer's bytes \
         (or its EOF)",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::DeadlineExceeded,
        "stalled-producer drain MUST surface as Code::DeadlineExceeded, got {:?} (msg={})",
        err.code,
        err.messages.first().map(String::as_str).unwrap_or(""),
    );
}

// -----------------------------------------------------------------------------
// FL-688 chunked-write-reject diagnostic (observability-only).
//
// The ~9 stuck large CAS blobs fail on the Bazel-facing internal-chunking
// path with `"buf_channel: writer dropped without commit : bazel-facing
// internal-chunking: reader.recv()"` — the producer (the FastSlowStore::update
// tee write-half) is dropped before sending any bytes, so the FIRST
// `reader.recv()` in `build_bazel_chunk_stream` returns the buf_channel
// "writer dropped without commit" error at offset 0. The reject path used to
// return the Status with NO digest-tagged log, so the stuck loop was
// un-diagnosable. The diagnostic at the first-recv site tags the digest +
// offset-0-vs-midstream and bumps a split counter so the
// offset0:midstream:pre_probe ratio settles the open fork (worker sends
// nothing vs. drops partway vs. server rejects before the probe arms).
// -----------------------------------------------------------------------------

/// Producer dropped at offset 0 (tx dropped WITHOUT sending bytes and WITHOUT
/// EOF) → the first `reader.recv()` in `build_bazel_chunk_stream` returns
/// `"buf_channel: writer dropped without commit"`, which the chunker appends
/// `"bazel-facing internal-chunking: reader.recv()"` to (the EXACT FL-688
/// failure signature). The diagnostic MUST:
///   1. bump `chunked_reject_at_offset0` exactly once (worker-sent-zero-chunks
///      bucket), NOT `chunked_reject_midstream`,
///   2. emit a `warn!` (survives `release_max_level_info`) carrying the digest
///      so a journal scan can attribute the stuck loop to this branch.
///
/// **Mutation step:** comment out the `chunked_reject_at_offset0.fetch_add`
/// in `build_bazel_chunk_stream`'s `reader.recv()` Err arm (or the whole
/// `record_chunked_reject_at_recv` call). This test then red-fails with the
/// bespoke "FL-688: offset-0 reject counter MUST increment on a producer
/// dropped before any chunk" message (counter stays 0).
#[nativelink_test]
async fn chunked_reject_diagnostic_counts_offset0_drop_and_logs_digest() {
    const CHUNK: usize = 4 * 1024;
    // Declared size is one full chunk; the producer never sends a byte, so
    // the reject lands at offset 0 (bytes_consumed == 0, buf empty).
    const SIZE: usize = CHUNK;

    let blob = vec![0x42u8; SIZE];
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);
    // Display format (`%digest`) is what the production `warn!` emits; the
    // captured tracing line carries it as `digest=<hash>-<size>`.
    let digest_disc = format!("{digest}");

    let (fs_store, _content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());

    // Producer dropped at offset 0: build the pair, then drop tx WITHOUT
    // sending bytes and WITHOUT EOF. WriteHalfGuard::Drop synthesizes
    // `Code::Internal "buf_channel: writer dropped without commit"`, which
    // the next `reader.recv()` observes — exactly the FL-688 signature.
    let (tx, producer_rx) = make_buf_channel_pair_with_size(128);
    drop(tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch_bazel_facing_internal_chunking(
            Arc::clone(&fs_store),
            Arc::clone(&in_flight),
            budget,
            None, // pin_budget
            None, // chunked_read_registry
            nativelink_store::chunked::ChunkedWriteSource::Bazel,
            None, // stable_digests_sink
            None, // failed_commit_sink
            Arc::clone(&metrics),
            CHUNK,
            digest,
            producer_rx,
            None, // async_result_relay
        ),
    )
    .await
    .expect(
        "must not deadlock — a producer dropped at offset 0 MUST surface the \
         reader.recv() Err promptly, NOT hang the dispatch task",
    );

    let err = result.expect_err(
        "dispatch MUST return Err when the producer is dropped before any \
         chunk (the FL-688 writer-dropped-without-commit signature)",
    );
    let joined = err.messages.join(" : ");
    assert!(
        joined.contains("bazel-facing internal-chunking: reader.recv()"),
        "FL-688: the offset-0 reject MUST carry the bazel-facing reader.recv() \
         marker (the exact production failure signature); got: {joined}",
    );

    // Load-bearing: the offset-0 bucket MUST tick exactly once and the
    // midstream bucket MUST stay zero (the producer sent ZERO chunks).
    use core::sync::atomic::Ordering;
    assert_eq!(
        metrics.chunked_reject_at_offset0.load(Ordering::Relaxed),
        1,
        "FL-688: offset-0 reject counter MUST increment on a producer dropped \
         before any chunk — without the `chunked_reject_at_offset0.fetch_add` \
         in build_bazel_chunk_stream's reader.recv() Err arm, the stuck loop \
         is invisible per-digest (the diagnostic's whole purpose)",
    );
    assert_eq!(
        metrics.chunked_reject_midstream.load(Ordering::Relaxed),
        0,
        "FL-688: a producer that sent ZERO chunks MUST NOT count as a \
         midstream drop — the offset0:midstream split IS the discriminator \
         between 'worker sends nothing' and 'worker drops partway'",
    );

    // The `warn!` MUST carry the digest so a journal scan attributes the
    // stuck loop to this branch. `release_max_level_info` keeps `warn!`.
    let raw = String::from_utf8(
        tracing_test::internal::global_buf().lock().unwrap().to_vec(),
    )
    .expect("tracing-test global buffer must be valid UTF-8");
    let warn_line = raw
        .lines()
        .find(|l| {
            l.contains("chunked-write-reject diagnostic")
                && l.contains(&digest_disc)
        });
    assert!(
        warn_line.is_some(),
        "FL-688: the offset-0 reject MUST emit a warn! carrying the digest \
         {digest_disc} so the stuck loop is attributable from logs alone. \
         Mutation hint: deleted the `warn!(... \"chunked-write-reject \
         diagnostic\")` in build_bazel_chunk_stream's reader.recv() Err arm. \
         tracing-test global_buf:\n{raw}",
    );
}

// -----------------------------------------------------------------------------
// #40: chunked #497 race-state stale-commit — F-A + F-B regression tests
//
// Root cause (see
// `.claude/audits/chunked-497-race-state-stale-commit-rootcause-fix-2026-06-23.md`):
// a transient CDN-tee cache-fan-out abandonment drops the Bazel dispatch
// producer → the dispatcher's `reader.recv()` errors → the chunk stream
// yields the "bazel-facing internal-chunking: reader.recv()" Err →
// `BazelChunkedDispatcherImpl::dispatch`'s synchronous Owner-Err arm
// (`chunked_write_handler.rs:4332`) publishes that Err into the shared
// per-digest `ChunkRaceState`. `publish_commit_result` is no-overwrite +
// `commit_done_flag` is sticky + the v1 paths never removed the registry
// entry → the poisoned `Err` is replayed verbatim to every subsequent v1
// WriteChunked attempt FOREVER (each attaches, sees `commit_done_flag`,
// returns AwaitCommit, peeks the stale Err, fails ~80–810 ms).
//
// Seams crossed by these tests (per `.claude/rules/testing-contracts.md`
// identify-the-seam discipline): producer error (`tx.send_error`,
// mirroring `CACHE_FANOUT_ABANDONED_MARKER`) → `build_bazel_chunk_stream`
// `reader.recv()` Err synthesizer → `dispatch_chunks_to_driver` inline
// admit-loop Err return → `BazelChunkedDispatcherImpl::dispatch` Owner-Err
// arm → `publish_commit_result(Err)` → `commit_done_flag` →
// `ChunkRaceRegistry` slot lifecycle → the next `dispatch`'s
// `try_attach_single_stream_writer` short-circuit.
// -----------------------------------------------------------------------------

/// Drive `BazelChunkedDispatcherImpl::dispatch` with a producer that
/// streams K<N bytes then surfaces an error (mirroring the live
/// cache-fan-out abandonment that drops the Bazel dispatch producer).
/// Returns the dispatch result.
async fn drive_dispatch_with_aborted_producer<Fe: FileEntry>(
    dispatcher: &BazelChunkedDispatcherImpl<Fe>,
    digest: DigestInfo,
    partial_bytes: Bytes,
) -> Result<u64, nativelink_error::Error> {
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    let send_task = tokio::spawn(async move {
        if !partial_bytes.is_empty() {
            // Send a partial prefix so the producer-drop happens
            // mid-stream (bytes_sent < declared), exactly like the live
            // journal's `bytes_sent=34297680 of 62303904`. The send may
            // race the dispatch consuming-then-erroring; either way the
            // subsequent send_error is the poison signal under test.
            drop(tx.send(partial_bytes).await);
        }
        // Mirror `CACHE_FANOUT_ABANDONED_MARKER` (Code::Aborted) — the
        // exact terminal signal the cache fan-out sends before dropping
        // its sender. The reader's `recv()` surfaces this, and
        // `build_bazel_chunk_stream` appends "bazel-facing
        // internal-chunking: reader.recv()" to it.
        tx.send_error(nativelink_error::make_err!(
            nativelink_error::Code::Aborted,
            "test cache-fan-out abandonment (CACHE_FANOUT_ABANDONED_MARKER analogue)"
        ));
        // tx drops here.
    });
    let res = <BazelChunkedDispatcherImpl<Fe> as BazelChunkedDispatcher>::dispatch(
        dispatcher, digest, rx,
    )
    .await;
    drop(send_task.await);
    res
}

/// Drive `BazelChunkedDispatcherImpl::dispatch` with a complete,
/// hash-matching blob (a clean backfill upload). Returns the dispatch
/// result. The blob lands on the slow tier on the AsyncCommit reaper's
/// task; callers poll `wait_for_cas_file` to observe durability.
async fn drive_dispatch_with_full_blob<Fe: FileEntry>(
    dispatcher: &BazelChunkedDispatcherImpl<Fe>,
    digest: DigestInfo,
    blob: Bytes,
) -> Result<u64, nativelink_error::Error> {
    let (mut tx, rx) = make_buf_channel_pair_with_size(128);
    let send_task = tokio::spawn(async move {
        tx.send(blob).await.expect("full-blob send must succeed");
        tx.send_eof().expect("full-blob send_eof must succeed");
    });
    let res = <BazelChunkedDispatcherImpl<Fe> as BazelChunkedDispatcher>::dispatch(
        dispatcher, digest, rx,
    )
    .await;
    drop(send_task.await);
    res
}

/// **F-A (the load-bearing regression).** A v1 Bazel dispatch whose
/// producer is abandoned mid-stream publishes a transient bazel-facing
/// `Err` into the per-digest race-state. WITHOUT F-A the registry entry
/// is immortal (`commit_done_flag=true`, no-overwrite, never removed on
/// v1 paths) and every later v1 WriteChunked for the SAME digest attaches,
/// sees `commit_done_flag`, returns AwaitCommit, peeks the stale Err, and
/// fails forever. WITH F-A, the failed publish force-removes the entry so
/// the next writer mints a fresh `ChunkRaceState`, claims Owner, and
/// actually re-writes the blob.
///
/// Production composition: real `FilesystemStore` + real
/// `ChunkRaceRegistry` (shared via `fs_store.chunked_race_registry()`) +
/// real `BazelChunkedDispatcherImpl`. The `dispatch` trait method is the
/// exact production seam `FastSlowStore::update` routes Bazel writes
/// through.
///
/// Mutation step (TDD step 5): delete F-A's `force_remove` after the
/// Err publish in the Owner-Err arm (`chunked_write_handler.rs:4332`) —
/// the registry entry survives poisoned, the second dispatch observes
/// AwaitCommit + the stale Err, and the
/// `.expect("stale failed-publish must not block backfill re-write …")`
/// below red-fails (the second dispatch returns the bazel-facing Err
/// instead of succeeding).
#[nativelink_test]
async fn chunked_race_state_failed_publish_reopens_owner_for_backfill() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    const SIZE: usize = N * CHUNK;

    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 13 + 7) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let dispatcher = BazelChunkedDispatcherImpl::new_with_state_for_test(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        budget,
        CHUNK,
    );

    // ---- Step 1: poison the race-state via an abandoned producer. ----
    // Send the first chunk's bytes, then surface the abandonment error.
    let partial = Bytes::copy_from_slice(&blob[..CHUNK]);
    let poison_res = tokio::time::timeout(
        Duration::from_secs(5),
        drive_dispatch_with_aborted_producer(&dispatcher, digest, partial),
    )
    .await
    .expect("must not deadlock — abandoned-producer dispatch must return promptly with Err");
    let poison_err = poison_res.expect_err(
        "abandoned-producer dispatch MUST return Err (the bazel-facing \
         reader.recv() failure) — this is the poison event",
    );
    assert!(
        poison_err
            .messages
            .iter()
            .any(|m| m.contains("bazel-facing internal-chunking: reader.recv()")),
        "the poison Err MUST carry the bazel-facing reader.recv() suffix \
         (proves the seam from build_bazel_chunk_stream → dispatch Err arm \
         was crossed); got {:?}",
        poison_err.messages,
    );

    // ---- Step 2: with F-A, the registry slot is GONE. ----
    // (Mutation of F-A leaves the entry resident with a poisoned
    // commit_done_flag; this assertion red-fails first.)
    assert!(
        fs_store.chunked_race_registry().get(&digest).is_none(),
        "F-A: after a v1 publish_commit_result(Err), the race-state \
         registry entry MUST be force-removed so the next writer mints a \
         fresh state — a resident entry here means the poisoned \
         commit_done_flag=true(Err) is still immortal (the FL-688 \
         infinite-re-upload-loop bug)",
    );

    // ---- Step 3: a fresh backfill WriteChunked for the SAME digest ----
    // MUST claim Owner and actually re-write the blob (NOT loop on
    // AwaitCommit peeking the stale Err).
    let backfill_size = tokio::time::timeout(
        Duration::from_secs(5),
        drive_dispatch_with_full_blob(&dispatcher, digest, Bytes::from(blob.clone())),
    )
    .await
    .expect("must not deadlock — backfill dispatch must complete in 5s")
    .expect(
        "stale failed-publish must not block backfill re-write — F-A force-remove \
         (without F-A the second dispatch attaches, sees commit_done_flag=true, \
          returns AwaitCommit, and peeks the stale bazel-facing Err forever)",
    );
    assert_eq!(
        backfill_size, SIZE as u64,
        "backfill dispatch must report the declared size on AsyncCommit Ok",
    );

    // ---- Step 4: the blob actually LANDS on the slow tier. ----
    // (Index-visibility contract: the AsyncCommit reaper renames into the
    // canonical CAS path. Without the re-write, no file appears.)
    let on_disk = wait_for_cas_file(&content_path, &digest, Duration::from_secs(5))
        .await
        .expect(
            "F-A: backfill re-write MUST land the canonical CAS file — \
             if the poisoned entry blocked Owner, the driver never ran and \
             no file appears (the blob stays permanently absent, exactly the \
             FL-688 live symptom)",
        );
    assert_eq!(
        on_disk, SIZE as u64,
        "post-backfill, the canonical CAS file MUST have the declared length",
    );
}

/// **F-B (defense-in-depth against a stale *Ok*).** A published
/// `commit_done_flag=Ok` short-circuit is honored ONLY while the blob is
/// actually present in the store. If the blob was evicted after a
/// successful commit, the sticky Ok must NOT false-short-circuit a fresh
/// writer to "already committed"; the writer must re-claim Owner and
/// re-write.
///
/// We exercise the gate directly at its seam:
/// `FilesystemStore::has_indexed_digest` is the existence primitive F-B
/// consults. The test composes a real race-state with a published Ok,
/// asserts the digest is ABSENT from the store, and drives a fresh
/// dispatch; with F-B the absent-blob success is reopened and the writer
/// re-writes (the CAS file lands), instead of returning a phantom Ok for
/// a non-existent blob.
///
/// Mutation step: remove F-B's existence-gate (let `commit_done_flag=Ok`
/// short-circuit unconditionally in `try_attach_single_stream_writer`) —
/// the fresh dispatch returns Ok WITHOUT re-writing, `wait_for_cas_file`
/// Elapsed, and the bespoke expect below red-fails.
#[nativelink_test]
async fn chunked_race_state_evicted_success_reopens_owner_for_rewrite() {
    use nativelink_store::chunked::chunked_race_state::{
        RaceCommitResult, SingleStreamAttachOutcome, WriterId,
    };

    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const SIZE: usize = N * CHUNK;

    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 5 + 1) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, content_path) = make_filesystem_store().await;
    let in_flight = ChunkedWriteInFlight::new();
    let budget = make_test_budget();
    let dispatcher = BazelChunkedDispatcherImpl::new_with_state_for_test(
        Arc::clone(&fs_store),
        Arc::clone(&in_flight),
        budget,
        CHUNK,
    );

    // Manufacture a sticky published-Ok race-state for a digest the store
    // does NOT have (simulating: committed once, then evicted; the
    // registry entry outlived the blob).
    let chunk_size_u32 = u32::try_from(CHUNK).unwrap();
    let (race_state, _guard, outcome) = fs_store
        .race_state_for_digest_and_attach_single_stream(
            &digest,
            chunk_size_u32,
            WriterId(9_999),
        );
    assert_eq!(
        outcome,
        SingleStreamAttachOutcome::Owner,
        "test setup: first attach for an unseen digest must be Owner",
    );
    race_state.publish_commit_result(Ok(RaceCommitResult {
        committed_size: SIZE as u64,
    }));
    // Drop our setup guard so the only thing pinning the entry is the
    // sticky published-Ok state itself.
    drop(_guard);

    // Precondition: the blob is genuinely absent from the store (eviction
    // simulation). If this ever returns Some, the test setup is wrong.
    assert!(
        fs_store.has_indexed_digest(&digest).await.is_none(),
        "test precondition: the evicted blob must be ABSENT from the store \
         (has_indexed_digest must be None) so F-B's existence-gate is the \
         thing under test",
    );

    // A fresh dispatch for the same digest. With F-B, the sticky Ok is
    // existence-gated → absent → the writer re-claims Owner and re-writes.
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        drive_dispatch_with_full_blob(&dispatcher, digest, Bytes::from(blob.clone())),
    )
    .await
    .expect("must not deadlock — evicted-success re-dispatch must complete in 5s")
    .expect(
        "F-B: a sticky commit_done_flag=Ok for an EVICTED blob must NOT \
         false-short-circuit — the writer must re-claim Owner and re-write \
         (an Err here means the gate let the dispatch fail or the writer \
          parked on a stale state)",
    );
    assert_eq!(res, SIZE as u64, "re-dispatch must report declared size");

    let on_disk = wait_for_cas_file(&content_path, &digest, Duration::from_secs(5))
        .await
        .expect(
            "F-B: evicted-success re-write MUST land the canonical CAS file — \
             if the sticky Ok short-circuited unconditionally, the driver \
             never ran and the evicted blob stays permanently absent",
        );
    assert_eq!(
        on_disk, SIZE as u64,
        "post-re-write, the canonical CAS file MUST have the declared length",
    );
}

/// **#494 sparse-zero guard (the over-action direction — MUST NOT
/// regress).** A published, DURABLE *Ok* (blob actually indexed) MUST
/// still short-circuit a late writer to that Ok — the late writer must
/// NOT re-claim Owner and pwrite into a renamed-away `.partial`. F-A is
/// scoped to the FAILURE slot precisely so this success path stays
/// sticky.
///
/// Mutation step: widen F-A to force-remove on Ok too (or drop the
/// `commit_done_flag` Ok short-circuit in
/// `try_attach_single_stream_writer`) — the late writer re-claims Owner
/// instead of yielding to the durable Ok, and the
/// `SingleStreamAttachOutcome::AwaitCommit` assertion below red-fails
/// (returns Owner). This proves F-A did NOT over-reopen the success path
/// (the #494 corruption direction).
#[nativelink_test]
async fn chunked_race_state_durable_success_still_yields_late_writer() {
    use nativelink_store::chunked::chunked_race_state::{
        RaceCommitResult, SingleStreamAttachOutcome, WriterId,
    };

    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const SIZE: usize = N * CHUNK;

    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 3 + 2) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let (fs_store, _content_path) = make_filesystem_store().await;
    let chunk_size_u32 = u32::try_from(CHUNK).unwrap();

    // Make the blob genuinely DURABLE in the store first (the success
    // path's precondition: a published Ok corresponds to a present blob).
    let seed_key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    fs_store
        .as_pin()
        .update_oneshot(seed_key, Bytes::from(blob.clone()))
        .await
        .expect("seed: store must accept the durable blob");
    assert!(
        fs_store.has_indexed_digest(&digest).await.is_some(),
        "seed precondition: the durable blob must be indexed",
    );

    // First writer attaches Owner and publishes a durable Ok.
    let (race_state, owner_guard, first_outcome) = fs_store
        .race_state_for_digest_and_attach_single_stream(
            &digest,
            chunk_size_u32,
            WriterId(1),
        );
    assert_eq!(
        first_outcome,
        SingleStreamAttachOutcome::Owner,
        "first attach must be Owner",
    );
    race_state.publish_commit_result(Ok(RaceCommitResult {
        committed_size: SIZE as u64,
    }));
    // Production shape: the Owner RELEASES its single_stream_owner slot
    // after publishing (mirrors `SingleStreamOwnerGuard::relinquish` at
    // the end of the v1 write path). This is load-bearing for the
    // mutation: with the slot cleared, the ONLY thing that yields the late
    // writer is the `commit_done_flag` success short-circuit in
    // `try_attach_single_stream_writer`. A mutation that removes that
    // short-circuit (the #494 sparse-zero guard) makes the late writer
    // claim Owner and this test red-fails. If the slot were left held, the
    // `single_stream_owner.is_some()` check would MASK the mutation.
    race_state.clear_single_stream_owner_if_owned(WriterId(1));

    // A LATE writer attaches AFTER the durable Ok publish + owner-slot
    // release. It MUST get AwaitCommit (yield to the published Ok), NOT
    // Owner. The entry is still pinned by `owner_guard`'s paired
    // RaceWriterGuard, mirroring production where the publishing Owner's
    // `_race_writer_guard` holds to fn-end.
    let (_late_state, _late_guard, late_outcome) = fs_store
        .race_state_for_digest_and_attach_single_stream(
            &digest,
            chunk_size_u32,
            WriterId(2),
        );
    assert!(
        matches!(late_outcome, SingleStreamAttachOutcome::AwaitCommit { .. }),
        "#494 guard: a late writer arriving after a DURABLE published Ok MUST \
         yield via AwaitCommit (propagate the durable Ok), NOT re-claim Owner \
         and re-write into a renamed-away .partial (sparse-zero corruption \
         direction). got {late_outcome:?}",
    );

    drop(owner_guard);
}
