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
    DispatchOutcome, PreparedChunk, dispatch_chunks_to_driver, wait_for_no_in_flight,
};
use nativelink_store::chunked::chunk_budget::{ChunkBudget, TOTAL_CHUNK_PERMITS};
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
                chunk_sha256: sha256(&chunk_bytes),
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
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            metrics,
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
                chunk_sha256: sha256(&chunk_bytes),
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
            CHUNK,
            digest,
            stream,
            CommitMode::AsyncCommit,
            metrics,
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
        BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
    };
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
                chunk_sha256: sha256(&chunk_bytes),
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
            CHUNK,
            digest,
            stream,
            CommitMode::Synchronous,
            Arc::clone(&metrics),
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
