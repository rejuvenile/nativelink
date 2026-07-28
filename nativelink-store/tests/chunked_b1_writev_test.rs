// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #47 b1 Phase 2 Step 5: integration tests for the io_uring writev
//! coalescer wire-up.
//!
//! Design source of truth:
//! `.claude/audits/47-b1-chunked-coalescer-design-2026-06-03.md` §9.
//!
//! Each test runs in production composition (real `FilesystemStore` +
//! real `ChunkedDriver::spawn_driver`) and wraps the operation in
//! `tokio::time::timeout(Duration::from_secs(5), ...)` with a SPECIFIC
//! error message (deadlock detector). T1 and T_multi also assert
//! `has_with_results(&[digest]) = Some(size)` within the same 5 s
//! window per the index-visibility contract (CLAUDE.md
//! `feedback_index_visibility_contract`, 2026-05-04).
//!
//! ## io_uring skip-guard (cadre fix-up B3)
//!
//! Every test that exercises Path A (the io_uring writer) checks
//! `is_io_uring_available()` at entry and SKIPs (`return`) on hosts
//! where the runtime probe returns false. Without this guard, Path B
//! (spawn_blocking) is silently exercised and the test "passes" for
//! the wrong reason — invariants like "two parallel writers don't
//! serialize" or "writev coalesces chunks" become tautologies on the
//! spawn_blocking path. The unit test `b1_writev_pick_path_unit_*` in
//! `chunked_writer.rs` is the only test that intentionally exercises
//! the path-decision branch without the runtime probe.
//!
//! Mutation steps named per CLAUDE.md TDD discipline.

#![cfg(all(feature = "chunked_fast_slow", feature = "test-utils"))]

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP};
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreKey;
use sha2::{Digest as _, Sha256};

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

/// Cadre fix-up B3: probe runtime io_uring availability. Skips the
/// caller test (logging on stderr) when Path A would not actually be
/// exercised — i.e. on non-Linux hosts, on Linux kernels too old, OR
/// when the `io-uring` feature is compiled out. Returns `true` when
/// the test should proceed.
async fn skip_if_no_io_uring(test_name: &str) -> bool {
    // `is_io_uring_available` only exists when the `io-uring` feature is
    // compiled in (gated `#[cfg(all(feature = "io-uring", target_os =
    // "linux"))]` in `nativelink_util::fs`). With the feature compiled out
    // there is no io_uring path to exercise, so treat it as unavailable and
    // let the caller SKIP — matching this guard's documented intent.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    let available = nativelink_util::fs::is_io_uring_available().await;
    #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
    let available = false;
    if !available {
        eprintln!(
            "SKIP {test_name}: io_uring not available on this host — Path A not exercised; \
             #47 b1 fix-up B3 skip-guard fired",
        );
    }
    available
}

/// Build a fresh on-disk `FilesystemStore`. The whole chunked path
/// (`open_or_create_partial_marker` → writer task → commit) runs against
/// real disk so the index-visibility assertion at the end actually
/// exercises `evicting_map.insert`.
async fn make_fs_store() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/47-b1/content");
    let temp_path = format!("{base}/{nonce}/47-b1/temp");
    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed for #47 b1 tests")
}

/// Build `(blob_bytes, digest, total_size)` for a `n_chunks`-of-1-MiB
/// blob. Uses `CHUNK_SIZE` so the test exercises the production chunk
/// size (matches design §10 Step 5 T2 expectations).
fn make_blob_mib(n_chunks: usize, fill_byte: u8) -> (Vec<u8>, DigestInfo, u64) {
    let total = (n_chunks * CHUNK_SIZE) as u64;
    let mut blob = Vec::with_capacity(n_chunks * CHUNK_SIZE);
    for i in 0..n_chunks {
        blob.extend(std::iter::repeat(fill_byte.wrapping_add(i as u8)).take(CHUNK_SIZE));
    }
    let h = sha256(&blob);
    (blob, DigestInfo::new(h, total), total)
}

/// Cadre fix-up B4: assert has_with_results returns Some(size) for the
/// freshly-committed digest within a 5 s window. Replaces the prior
/// `let _ = results` discard which left the index-visibility contract
/// unenforced. Bespoke assertion message names the contract per
/// CLAUDE.md `feedback_index_visibility_contract`.
async fn assert_index_visible<Fe: nativelink_store::filesystem_store::FileEntry>(
    store: &Arc<FilesystemStore<Fe>>,
    digest: DigestInfo,
    expected_size: u64,
    test_name: &str,
) {
    let key: StoreKey<'_> = digest.into();
    let mut results: [Option<u64>; 1] = [None];
    tokio::time::timeout(
        Duration::from_secs(5),
        nativelink_util::store_trait::StoreDriver::has_with_results(
            Pin::new(store.as_ref()),
            &[key],
            &mut results,
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("{test_name}: has_with_results must complete within 5 s window"))
    .unwrap_or_else(|e| panic!("{test_name}: has_with_results must succeed: {e:?}"));
    let Some(actual_size) = results[0] else {
        panic!(
            "{test_name}: stale negative — index not updated post-rename \
             (digest committed but has_with_results returned None; \
             evicting_map.insert in finalize_holding broken)",
        );
    };
    assert_eq!(
        actual_size, expected_size,
        "{test_name}: index returned wrong size for committed digest \
         (got {actual_size}, expected {expected_size})",
    );
}

/// =====================================================================
/// T1 — single small-blob commits + has_with_results visibility.
/// =====================================================================
///
/// Sends ONE 1 MiB chunk (smallest multiple of `CHUNK_SIZE` so chunk
/// coverage is `expected_chunk_count = 1`). Asserts:
/// 1. `await_completion()` returns Ok with size == 1 MiB.
/// 2. Within the same 5 s window, `has_with_results(&[digest]) =
///    Some(size)` (index-visibility contract).
///
/// Mutation target: in `chunked_driver.rs::run_driver`'s `ready_to_commit`
/// branch, comment out `writer_handle.await??`. The rename then races
/// the still-in-flight writev CQE; `commit_chunked_to_holding`'s
/// length-check fires with `actual_len < expected_size` and the test
/// fails with a length-mismatch error (LOAD-BEARING ORDERING per
/// design §3 / §10 Step 2).
#[nativelink_test]
async fn b1_writev_single_chunk_small_blob_commits() {
    if !skip_if_no_io_uring("b1_writev_single_chunk_small_blob_commits").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(1, 0x41);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let permit = budget
        .try_acquire_chunk()
        .expect("ChunkBudget must have permits available");

    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(blob.clone()),
            finish: true,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("ChunkWork send must succeed on a fresh driver");
        drop(tx);
        driver
            .await_completion()
            .await
            .expect("commit_and_verify must succeed for valid blob")
    })
    .await
    .expect(
        "T1: must not deadlock — single-chunk commit on Path A must complete \
         within 5 s; LOAD-BEARING ORDERING (writer_handle.await before commit \
         stat) violation would surface as length-mismatch in commit_chunked_to_holding",
    );

    // Index-visibility contract: rename-into-canonical-path must
    // populate the evicting_map index synchronously, so a
    // has_with_results call within the test window must observe the
    // newly-committed blob. Mutation target (per
    // feedback_index_visibility_contract): comment out the
    // background_spawn evicting_map.insert in filesystem_store's
    // finalize_holding hook; this assertion red-fails with
    // "stale negative — index not updated post-rename".
    assert_index_visible(&store, digest, total, "T1").await;
}

/// =====================================================================
/// T5 — zero-byte / empty-chunk fast-path skips writer spawn.
/// =====================================================================
///
/// Sends a single empty `ChunkWork(finish=true, offset=0,
/// chunk_bytes=Bytes::new())` for a NON-zero-size digest (1 MiB
/// declared). The driver-side empty-chunk skip on Path A means the
/// writer task is NOT spawned for this chunk — so no IoUringMarker
/// entry is inserted in the chunked_partials map.
///
/// Test asserts: after the recv loop exits with no chunks of bytes
/// admitted, the FilesystemStore has NO chunked_partials entry for the
/// digest. The driver's `await_completion` returns Err (length
/// mismatch / aborted) because we did not actually deliver 1 MiB —
/// that is expected; the test focuses on the writer-spawn skip
/// invariant, not on the commit outcome.
///
/// Mutation target: in `chunked_driver.rs::run_driver`'s Path A
/// branch, remove the `if bytes_for_write.is_empty() { Ok(()) }`
/// short-circuit. The writer task is spawned even for the empty chunk,
/// the IoUringMarker entry is inserted, and
/// `has_in_flight_chunked(&digest)` returns true → test red-fails with
/// the bespoke "marker entry inserted for zero-byte blob" message.
#[nativelink_test]
async fn b1_writev_empty_chunk_does_not_spawn_writer() {
    if !skip_if_no_io_uring("b1_writev_empty_chunk_does_not_spawn_writer").await {
        return;
    }
    let store = make_fs_store().await;
    // Use a 1-byte digest declared but send only an empty chunk. The
    // empty-chunk skip on Path A must not spawn a writer task; the
    // commit then fails (since no bytes were actually written), which
    // is the expected behavior — we're checking the WRITER-SPAWN
    // skip, not the commit outcome.
    let payload = [0u8; 1];
    let digest = DigestInfo::new(sha256(&payload), 1);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        1,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let permit = budget
        .try_acquire_chunk()
        .expect("ChunkBudget must have permits available");

    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::new(),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("ChunkWork send must succeed for empty chunk");
        // Drop tx so the driver's recv loop terminates.
        drop(tx);
        // We don't care about the commit outcome here — the writer
        // spawn skip is the test target.
        let _ = driver.await_completion().await;
    })
    .await
    .expect(
        "T5: must not deadlock — empty-chunk on Path A MUST be a no-op \
         that does not spawn the writer task; if the writer task is \
         spawned for an empty chunk, the IoUringMarker entry is \
         inserted and the chunked_partials map shows in-flight state \
         for this digest",
    );

    // Verify NO in-flight chunked partial was ever registered. The
    // writer-spawn skip on Path A means `open_chunked_partial_marker`
    // was never called, so the chunked_partials map is empty for this
    // digest. If the mutation removed the empty-chunk skip, the
    // writer task spawns → marker entry inserted → this assertion
    // fires with the bespoke message.
    assert!(
        !store.has_in_flight_chunked_partial(&digest),
        "marker entry inserted for zero-byte blob — empty-chunk Path A skip broken \
         (chunked_partials should be empty after empty-chunk arrival + driver exit)"
    );
}

/// =====================================================================
/// T_multi — multi-chunk commit completes and is index-visible.
/// =====================================================================
///
/// Sends 4 contiguous 1 MiB chunks (total 4 MiB) IN-ORDER through the
/// driver's mpsc. Asserts:
/// 1. `await_completion()` returns Ok with size == 4 MiB.
/// 2. `has_with_results(&[digest]) = Some(size)` within the same 5 s
///    window (index-visibility).
///
/// The io_uring writer task is exercised end-to-end: writer spawn on
/// first chunk, four contiguous WriteJobs cross the mpsc, the writev
/// CQEs land, the writer drops on chunk_tx-drop, the driver awaits the
/// writer handle, and `commit_chunked_to_holding` sees the full 4 MiB
/// file.
///
/// Mutation target: comment out the `writer_handle.await??` in
/// `chunked_driver.rs::run_driver`'s `ready_to_commit` branch. The
/// commit's stat sees a short file → length-mismatch.
#[nativelink_test]
async fn b1_writev_multi_chunk_in_order_commits() {
    if !skip_if_no_io_uring("b1_writev_multi_chunk_in_order_commits").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(4, 0x71);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..4 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must have permits available");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK_SIZE) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec()),
                finish: i == 3,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("ChunkWork send must succeed");
        }
        drop(tx);
        driver
            .await_completion()
            .await
            .expect("commit_and_verify must succeed for valid 4 MiB blob")
    })
    .await
    .expect(
        "T_multi: must not deadlock — 4-chunk commit on Path A must \
         complete within 5 s; LOAD-BEARING ORDERING violation would \
         surface as length-mismatch in commit_chunked_to_holding",
    );

    // Index-visibility within the same window (cadre fix-up B4).
    assert_index_visible(&store, digest, total, "T_multi").await;
}

/// =====================================================================
/// T_drop — channel-close-on-driver-drop releases writer task.
/// =====================================================================
///
/// Send 2 chunks (NO `finish=true`) then drop the sender. Path A's
/// writer task MUST observe the mpsc close, drain in-flight, and exit
/// within `tokio::time::timeout(Duration::from_secs(5), ...)`. The
/// driver's recv loop returns None and the existing "no finish observed"
/// arm fires with Code::Aborted.
///
/// Mutation target: in `chunked_writer::writer_task`, replace the
/// `while let Some(job) = chunk_rx.recv().await` loop with a non-
/// terminating `loop { let _ = chunk_rx.recv().await; }` that never
/// returns when rx closes. The driver's `writer_handle.await` then
/// blocks indefinitely → tokio::time::timeout fires with the bespoke
/// message below.
#[nativelink_test]
async fn b1_writev_channel_close_on_driver_drop_releases_writer() {
    if !skip_if_no_io_uring("b1_writev_channel_close_on_driver_drop_releases_writer").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(4, 0xa1);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..2 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must have permits available");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK_SIZE) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec()),
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("ChunkWork send must succeed");
        }
        drop(tx);
        // Drop the driver — JoinHandleDropGuard aborts the spawned
        // driver task, which drops `chunk_tx` inside, which closes the
        // writer task's mpsc. The writer task MUST exit (drain
        // remaining + return Ok) so the inner JoinHandle resolves.
        // await_completion observes the driver task's exit via the
        // completion oneshot.
        let result = driver.await_completion().await;
        // The "no finish observed" arm returns Code::Aborted; that's
        // fine — what matters is the writer task did NOT wedge.
        let _ = result;
    })
    .await
    .expect(
        "T_drop: must not deadlock — writer task did not drain — \
         channel-close-on-drop broken",
    );
}

/// =====================================================================
/// T2 — multi-chunk coalescing histogram bounds.
/// =====================================================================
///
/// Sends 16 contiguous 1 MiB chunks (total 16 MiB) IN-ORDER. After
/// `await_completion`, reads `COALESCE_HISTOGRAM_BY_DIGEST[digest]`
/// and asserts:
///   1. `sum == 16` — every chunk accounted for in some writev SQE.
///   2. `len <= 16` — coalescing should not produce MORE SQEs than chunks
///      (sanity).
///
/// Spec note (cadre fix-up B5 — design §1 / §4): with
/// `COALESCE_TARGET == CHUNK_SIZE == 1 MiB`, the writer pops one job
/// from `pending`, hits the byte-target, and submits the writev in one
/// shot — so the steady-state observation is `coalesce_count = 1` per
/// SQE (len = 16). That is EXPECTED. The load-bearing win on Path A is
/// the io_uring bypass of the spawn_blocking pool mutex (#449 mutex
/// contention), NOT coalescing amortization. A strict `<` form of the
/// bound cannot fire without a pre-queueing setup (multiple jobs in
/// `pending` before any writev submits) — those tests are reserved for
/// a Phase 3 burst-load harness.
///
/// Mutation target: in `chunked_writer.rs`'s writer-task body, remove
/// the `COALESCE_HISTOGRAM_BY_DIGEST.push(...)` call. `sum` becomes 0;
/// the first assertion fires with bespoke "every chunk must be
/// accounted for in some writev". Alternative mutation: make the
/// histogram push fire twice per writev — `sum` becomes 32; same
/// assertion fires with the inverse direction.
///
/// MUTATION VERIFIED (2026-06-03): in chunked_writer.rs (~line 647-652),
/// replaced
///   `super::COALESCE_HISTOGRAM_BY_DIGEST.lock().entry(digest)
///        .or_insert_with(Vec::new).push(coalesce_count as u32);`
/// with a no-op (`let _ = digest; let _ = coalesce_count;`). The
/// histogram stayed empty → `sum == 0` → this assertion fired with
/// bespoke "T2: coalesce histogram sum mismatch — every chunk must
/// be accounted for in some writev: got sum=0, histogram=[]".
/// Reverting the mutation restored green.
#[nativelink_test]
async fn b1_writev_multi_chunk_coalesces_pwritev_count() {
    if !skip_if_no_io_uring("b1_writev_multi_chunk_coalesces_pwritev_count").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(16, 0xc1);

    // Clear any prior histogram entry for this digest (random fill +
    // 16 chunks ≈ unique digest; this is defensive against re-runs).
    nativelink_store::chunked::chunked_writer::COALESCE_HISTOGRAM_BY_DIGEST
        .lock()
        .remove(&digest);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        for i in 0..16 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must have permits available");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK_SIZE) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec()),
                finish: i == 15,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("ChunkWork send must succeed");
        }
        drop(tx);
        driver
            .await_completion()
            .await
            .expect("commit_and_verify must succeed for valid 16 MiB blob")
    })
    .await
    .expect(
        "T2: must not deadlock — 16-chunk commit on Path A must \
         complete within 10 s",
    );

    let histogram = nativelink_store::chunked::chunked_writer::COALESCE_HISTOGRAM_BY_DIGEST
        .lock()
        .get(&digest)
        .cloned()
        .unwrap_or_default();
    let sum: u32 = histogram.iter().sum();
    let len = histogram.len();
    // Load-bearing invariant #1: the writer accounts for EVERY chunk in
    // some writev SQE. A missed `COALESCE_HISTOGRAM_BY_DIGEST.push(...)`
    // call would show sum < 16; a duplicated push would show sum > 16.
    assert_eq!(
        sum, 16,
        "T2: coalesce histogram sum mismatch — every chunk must be \
         accounted for in some writev: got sum={sum}, histogram={histogram:?}",
    );
    // Load-bearing invariant #2 (design §9 T2 + B5 framing): the
    // writer never produces MORE SQEs than chunks. With
    // CHUNK_SIZE == COALESCE_TARGET == 1 MiB and 16 chunks, the
    // ceiling is 16 and the steady-state observation is 16
    // (one writev per chunk — the bypass-the-mutex win, not the
    // amortize-via-coalescing win). Histogram inversion (more SQEs
    // than chunks) would be a contract bug — the bespoke message
    // names it.
    assert!(
        len <= 16,
        "T2: histogram length = {len} (expected ≤ 16 with \
         COALESCE_TARGET=1MiB and a 16 MiB blob); writer produced MORE \
         SQEs than chunks — histogram inversion, contract bug; \
         histogram={histogram:?}",
    );
}

/// =====================================================================
/// T4 — different-digest parallelism (no global serialization).
/// =====================================================================
///
/// Launches TWO 4 MiB blobs in parallel via `tokio::join!`. Each blob
/// runs through a distinct `ChunkedDriver` with its own writer task.
/// After both complete, reads `WRITER_START_AT_BY_DIGEST` for both
/// digests and asserts the gap is < 100 ms — proving no global
/// serialization wraps the writer-task spawn or its first writev SQE.
///
/// Mutation target (per design §9 T4): wrap the writer-task spawn (or
/// its body) in a global `tokio::sync::Mutex` so all writers serialize.
/// The second blob's first-writev timestamp then trails the first's by
/// at LEAST the wall-clock cost of the first blob's writev pipeline
/// (≫ 100 ms with 4 × 1 MiB chunks at low load); the < 100 ms
/// assertion fires with bespoke "different digests serialized".
#[nativelink_test]
async fn b1_writev_different_digests_parallelize() {
    if !skip_if_no_io_uring("b1_writev_different_digests_parallelize").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob_a, digest_a, total_a) = make_blob_mib(4, 0xa1);
    let (blob_b, digest_b, total_b) = make_blob_mib(4, 0xb1);
    assert_ne!(
        digest_a, digest_b,
        "T4: digests must differ to exercise per-blob parallelism",
    );

    // Clear any prior probe state for both digests.
    {
        let mut starts = nativelink_store::chunked::chunked_writer::WRITER_START_AT_BY_DIGEST.lock();
        starts.remove(&digest_a);
        starts.remove(&digest_b);
    }

    let budget = ChunkBudget::new();
    let (driver_a, tx_a) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest_a,
        total_a,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );
    let (driver_b, tx_b) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest_b,
        total_b,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let send_all = |tx: tokio::sync::mpsc::Sender<ChunkWork>,
                    blob: Vec<u8>,
                    budget: &ChunkBudget|
     -> futures::future::BoxFuture<'static, ()> {
        let permits: Vec<_> = (0..4)
            .map(|_| {
                budget
                    .try_acquire_chunk()
                    .expect("T4: ChunkBudget permits must be available")
            })
            .collect();
        Box::pin(async move {
            let mut permits = permits;
            for i in 0..4 {
                tx.send(ChunkWork {
                    chunk_offset: (i * CHUNK_SIZE) as u64,
                    chunk_bytes: Bytes::from(
                        blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec(),
                    ),
                    finish: i == 3,
                    _permit: permits.remove(0),
                    _pin_permit: None,
                })
                .await
                .expect("ChunkWork send must succeed");
            }
            drop(tx);
        })
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        let send_a = send_all(tx_a, blob_a, &budget);
        let send_b = send_all(tx_b, blob_b, &budget);
        let (_, _, res_a, res_b) = tokio::join!(
            send_a,
            send_b,
            driver_a.await_completion(),
            driver_b.await_completion(),
        );
        res_a.expect("T4: blob A commit must succeed");
        res_b.expect("T4: blob B commit must succeed");
    })
    .await
    .expect("T4: must not deadlock — two parallel writers must complete within 10 s");

    // Read both start timestamps and assert the gap < 100 ms.
    let (start_a, start_b) = {
        let starts = nativelink_store::chunked::chunked_writer::WRITER_START_AT_BY_DIGEST.lock();
        let a = starts
            .get(&digest_a)
            .copied()
            .expect("T4: blob A must have recorded first-writev start time");
        let b = starts
            .get(&digest_b)
            .copied()
            .expect("T4: blob B must have recorded first-writev start time");
        (a, b)
    };
    let gap = if start_b > start_a {
        start_b.duration_since(start_a)
    } else {
        start_a.duration_since(start_b)
    };
    // MUTATION VERIFIED (2026-06-03): in chunked_driver.rs (~line
    // 1018-1025), wrapped the writer-task spawn in
    //   `static GLOBAL_WRITER_LOCK: tokio::sync::Mutex<()> =
    //       tokio::sync::Mutex::const_new(());`
    // acquired at the top of the spawned future and held across the
    // writer_task body + a 300 ms post-completion sleep. Two parallel
    // writers serialized → blob B's first-writev start lagged blob A's
    // by ~303 ms → this assertion fired with bespoke "T4: different
    // digests serialized: |start_b - start_a| = 303 ms (> 100 ms
    // threshold) — per-blob isolation broken, global mutex around
    // writer-task spawn or body introduced". Reverting the mutation
    // restored green (gap < 100 ms).
    assert!(
        gap.as_millis() < 100,
        "T4: different digests serialized: |start_b - start_a| = {} ms (> 100 ms threshold) — \
         per-blob isolation broken, global mutex around writer-task spawn or body \
         introduced",
        gap.as_millis(),
    );
}

/// =====================================================================
/// T6 — real writev error surfaces verbatim through driver expect_err.
/// =====================================================================
///
/// Installs `WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST[digest] = 2` so the
/// writer task synthesizes
/// `"test-inject: writer error at writev count 2"` on its (2+1)th
/// writev submission. Driver sends 4 × 1 MiB chunks. The writer errors
/// before the third writev completes → on a subsequent
/// `chunk_tx.send` the driver hits the `Err(SendError)` branch and
/// surfaces the writer's real error via `drop(chunk_tx); writer_handle.
/// await??.expect_err(...)` (design §6 / §10 Step 2 LOAD-BEARING
/// ORDERING).
///
/// The test asserts the surfaced error's display contains the literal
/// substring `"test-inject:"` — that's the load-bearing verification
/// that the driver did NOT manufacture a synthetic "writer dropped"
/// Code::Internal.
///
/// Mutation target: replace the `drop(chunk_tx); writer_handle.await??`
/// two-step in chunked_driver.rs with `return Err(make_err!(...,
/// "writer dropped"))`. The substring assertion fires with bespoke
/// "synthetic error surfaced: expected 'test-inject:' substring".
#[nativelink_test]
async fn b1_writev_error_propagates_real_writev_error() {
    if !skip_if_no_io_uring("b1_writev_error_propagates_real_writev_error").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(4, 0xd1);

    // Install the injection for THIS digest.
    nativelink_store::chunked::chunked_writer::WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST
        .lock()
        .insert(digest, 2);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        // Best-effort send: some of these may fail when the writer
        // has already dropped chunk_rx; those are the SendErr branches
        // the driver propagates via `writer_handle.await??.expect_err`.
        for i in 0..4 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must have permits available");
            // The driver-side recv loop will fail-out via the chunk_tx
            // send failure branch once the writer errors; that branch
            // returns the writer's REAL err. Subsequent sends from
            // here also fail because tx is dropped by the driver task.
            let send_result = tx
                .send(ChunkWork {
                    chunk_offset: (i * CHUNK_SIZE) as u64,
                    chunk_bytes: Bytes::from(
                        blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec(),
                    ),
                    finish: i == 3,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await;
            if send_result.is_err() {
                break;
            }
        }
        drop(tx);
        driver.await_completion().await
    })
    .await
    .expect("T6: must not deadlock — error path must complete within 5 s");

    // Clean up injection state.
    nativelink_store::chunked::chunked_writer::WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST
        .lock()
        .remove(&digest);

    let err = result.expect_err("T6: writer-injected error must surface as Err from await_completion");
    let msg = err.to_string();
    // MUTATION VERIFIED (2026-06-03): in chunked_driver.rs run_driver's
    // post-recv-loop branch (~line 1207-1217), replace
    //   `writer_handle.await.map_err(...)??;`
    // with
    //   `let _ = writer_handle; return Err(make_err!(Code::Internal, "writer dropped"));`
    // The substring "test-inject:" never reaches the test → this
    // assertion fires with the bespoke "synthetic 'writer dropped'"
    // message. Test went from green to red with exactly that message;
    // reverting the mutation made it green again.
    assert!(
        msg.contains("test-inject:"),
        "T6: synthetic error surfaced: expected 'test-inject:' substring, \
         got '{msg}' — LOAD-BEARING ORDERING violated; driver manufactured \
         a synthetic 'writer dropped' instead of awaiting writer_handle to \
         surface the REAL writev error",
    );
}

/// =====================================================================
/// T8 — asymmetric over-action: post-error drain returns ChunkBudget permits.
/// =====================================================================
///
/// Installs the writer error injection at writev count 1, then sends up
/// to 100 × 1 MiB chunks. The driver may complete sending some chunks
/// before the writer errors and closes its mpsc — those that arrive
/// before the writer's post-error drain go into `pending`/in_flight
/// (where the permits ride along), the rest are blocked at
/// `chunk_tx.send`. In ALL cases, every permit eventually returns to
/// `ChunkBudget` because either:
///   (a) the writer's post-error chunk_rx drain loop releases them
///       (design §6 S1 step 2), OR
///   (b) the driver's send-fail branch drops the permit-carrying
///       ChunkWork on the floor.
///
/// Test asserts: after the error surfaces, `ChunkBudget::available_chunks()`
/// returns to `TOTAL_CHUNK_PERMITS` within the 5 s window.
///
/// Mutation target (design §9 T8): remove the post-error drain loop at
/// the end of `writer_task` (the `while let Some(_job) =
/// chunk_rx.recv().await` that drops queued WriteJobs to release
/// permits). With the drain removed, permits queued past the
/// writer's failure point are stranded; the assertion fires with
/// bespoke "permits leaked".
#[nativelink_test]
async fn b1_writev_error_mid_stream_drains_and_returns_permits() {
    use nativelink_store::chunked::chunk_budget::TOTAL_CHUNK_PERMITS;

    if !skip_if_no_io_uring("b1_writev_error_mid_stream_drains_and_returns_permits").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(100, 0xe1);

    let budget = ChunkBudget::new();
    let initial_available = budget.available_chunks();
    assert_eq!(
        initial_available, TOTAL_CHUNK_PERMITS,
        "T8: precondition — fresh ChunkBudget must start at full permits",
    );

    // Inject after writev #1 — error fires very early so most permits
    // are queued in the per-blob mpsc when the writer fails.
    nativelink_store::chunked::chunked_writer::WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST
        .lock()
        .insert(digest, 1);

    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..100 {
            let Some(permit) = budget.try_acquire_chunk() else {
                // Budget exhausted (would not occur with TOTAL_CHUNK_PERMITS
                // == 4096 ≫ 100) — break out so the assertion below
                // measures whatever permits we DID acquire.
                break;
            };
            let send_result = tx
                .send(ChunkWork {
                    chunk_offset: (i * CHUNK_SIZE) as u64,
                    chunk_bytes: Bytes::from(
                        blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec(),
                    ),
                    finish: i == 99,
                    _permit: permit,
                    _pin_permit: None,
                })
                .await;
            if send_result.is_err() {
                // Driver dropped tx after surfacing the writer error.
                break;
            }
        }
        drop(tx);
        driver.await_completion().await
    })
    .await
    .expect("T8: must not deadlock — error + drain must complete within 5 s");

    // Cleanup.
    nativelink_store::chunked::chunked_writer::WRITER_INJECT_ERROR_AFTER_N_BY_DIGEST
        .lock()
        .remove(&digest);

    // The error MUST surface — verified separately by T6, but reassert
    // here so a regression in T6 doesn't hide a T8 silent-success bug.
    let err = result.expect_err("T8: writer-injected error must surface");
    assert!(
        err.to_string().contains("test-inject:"),
        "T8: surfaced error must carry the test-inject substring \
         (regression-shared with T6); got {err}",
    );

    // The load-bearing assertion: every permit returned. Polling loop
    // because the writer's post-error drain races the test thread —
    // give the runtime a chance to flush the chunk_rx.recv loop.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let available = budget.available_chunks();
        if available == TOTAL_CHUNK_PERMITS {
            break;
        }
        if std::time::Instant::now() > deadline {
            // MUTATION VERIFIED (2026-06-03): in chunked_writer.rs
            // writer_task, both (a) replace step 4's
            //   `drop(pending); if hit_eof { break; } continue;`
            // with `std::mem::forget(pending); break;` AND (b) replace
            // step 7's
            //   `while let Some(_job) = chunk_rx.recv().await { }`
            // with a no-op. Permits queued in pending + the mpsc stay
            // held → available stays below TOTAL_CHUNK_PERMITS → this
            // assertion fires with the bespoke "permits leaked"
            // message (observed available = 4095 in mutation run).
            // Reverting both restored available = 4096 (green).
            panic!(
                "T8: permits leaked: available = {available}, expected {TOTAL_CHUNK_PERMITS} — \
                 writer did not drain chunk_rx on error; post-error drain loop \
                 (design §6 S1 step 2) broken",
            );
        }
        tokio::task::yield_now().await;
    }
}

/// =====================================================================
/// T9 — pin populate fires AFTER writev CQE (cadre fix-up P1).
/// =====================================================================
///
/// LOAD-BEARING ORDERING (design §6.2 / §6.3 + cadre red-team P1):
/// the in-memory pin must never advertise bytes whose writev has not
/// yet completed. Pre-fix, pin populate ran AFTER
/// `chunk_tx.send().await` returned Ok but BEFORE the writev CQE
/// landed — a concurrent reader's `try_get_chunk_from_pin` could
/// surface bytes for chunks whose writev was still in-flight (or had
/// errored).
///
/// Test setup uses `TEST_PRE_WRITE_DELAY_MS_BY_DIGEST` to wedge the
/// writer's writev for 800 ms BEFORE submission. We observe the pin
/// DURING that window (i.e. AFTER `chunk_tx.send` returned Ok but
/// BEFORE the writev CQE could possibly have landed). The driver's
/// post-loop `pin_state.chunks.clear()` runs only AFTER the driver
/// task exits, so during the in-flight window we can probe pin state.
///
/// Sequence:
///   1. Install 800 ms pre-write delay for `digest`.
///   2. Send chunk 0; `tx.send` returns Ok the moment the writer's
///      mpsc accepts the WriteJob (pre-CQE).
///   3. Sleep 200 ms (between the writer task's pre-delay start and
///      its writev submission). Observe pin state via
///      `driver.pinned_chunk_count()` — MUST be 0 (writev has not
///      submitted yet, let alone landed).
///   4. Send chunks 1-3 (finish=true on the last) so the commit
///      completes after the pre-delay elapses.
///   5. `await_completion` returns Ok; final assertion on commit
///      success.
///
/// MUTATION VERIFIED (2026-06-03): in chunked_driver.rs (~line 1318),
/// removed the `if !pin_populated_by_writer` gate around
///   `let mut pin_state = pin.lock();
///    pin_state.populate(chunk_offset, chunk_bytes.clone(),
///                       pin_permit_for_path_b_or_empty);`
/// so the driver always populates pin synchronously with `tx.send Ok`
/// (pre-CQE). The 200 ms observation window then saw pin populated
/// (pinned_chunk_count = 1, pinned_bytes = 1048576) and this
/// assertion fired with bespoke "T9: pin advertised bytes before
/// writev completed — pinned_chunk_count = 1 (expected 0 during the
/// pre-write delay window); LOAD-BEARING ORDERING violated;
/// pinned_bytes = 1048576". Reverting (restoring the gate) restored
/// green.
#[nativelink_test]
async fn b1_writev_pin_only_advertises_post_cqe_bytes() {
    if !skip_if_no_io_uring("b1_writev_pin_only_advertises_post_cqe_bytes").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(4, 0xf2);

    // Install pre-write delay so the writer's writev SQE waits 800 ms
    // BEFORE submitting. The test thread observes pin state during
    // that window. Same probe used by `driver_per_chunk_pwrite_*`
    // tests — single source of truth for the delay map.
    nativelink_store::chunked::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
        .lock()
        .insert(digest, 800);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let send_first = async {
        let permit = budget
            .try_acquire_chunk()
            .expect("ChunkBudget must have permits available");
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(blob[0..CHUNK_SIZE].to_vec()),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("T9: first ChunkWork send must succeed");
    };

    // Step 1+2: send chunk 0; tx.send returns Ok pre-CQE.
    tokio::time::timeout(Duration::from_secs(5), send_first)
        .await
        .expect("T9: send-first must complete within 5 s");

    // Step 3: observe pin state. With the writer task wedged in its
    // 800 ms pre-write delay, NO writev CQE has landed. The pin MUST
    // be empty (post-fix); pre-fix the driver-site populate fired
    // synchronously with `tx.send` Ok and would show `pinned_count
    // >= 1` here.
    //
    // 200 ms sleep is well INSIDE the writer's 800 ms pre-delay
    // window: the writer has popped chunk 0 from its mpsc and is
    // waiting in `tokio::time::sleep(800ms)` at the top of step 5;
    // the CQE has not happened. Tested at 200 ms vs 800 ms with the
    // mutation: pin is consistently populated pre-fix (driver-side
    // populate is synchronous with tx.send) and consistently empty
    // post-fix (writer's pre-delay > test's sleep).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pinned_count = driver.pinned_chunk_count();
    let pinned_bytes = driver.pinned_bytes();
    // LOAD-BEARING ASSERTION (cadre fix-up P1):
    // Mutation: delete the `if !pin_populated_by_writer` gate in
    // chunked_driver.rs so the driver always populates the pin
    // synchronously with tx.send Ok. The 200 ms sleep then observes
    // pinned_count == 1, pinned_bytes == 1 MiB; this assertion fires
    // with the bespoke "pin advertised bytes before writev completed"
    // message.
    assert_eq!(
        pinned_count, 0,
        "T9: pin advertised bytes before writev completed — \
         pinned_chunk_count = {pinned_count} (expected 0 during the \
         pre-write delay window); LOAD-BEARING ORDERING violated; \
         pinned_bytes = {pinned_bytes}",
    );

    // Step 4+5: send remaining chunks (must succeed AFTER the
    // pre-delay elapses — the writev for chunk 0 completes, the
    // writer drains the next chunks, the commit fires).
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        for i in 1..4 {
            let permit = budget
                .try_acquire_chunk()
                .expect("ChunkBudget must have permits available");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK_SIZE) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE].to_vec()),
                finish: i == 3,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("T9: ChunkWork send must succeed");
        }
        drop(tx);
        driver.await_completion().await
    })
    .await
    .expect("T9: commit must complete within 10 s window (pre-delay 800 ms × 4 = 3.2 s)");

    // Cleanup.
    nativelink_store::chunked::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
        .lock()
        .remove(&digest);

    result.expect("T9: blob commit must succeed after pre-delay elapses");
}

/// =====================================================================
/// T10 — warn! fields include `cqe_kernel_ms` and `cqe_dispatch_ms` (#3).
/// =====================================================================
///
/// Verifies that when the slow-write threshold is crossed the warn! line
/// emitted by `ChunkedWriter::process_completion` contains BOTH the new
/// `cqe_kernel_ms` (SQE-submit → CQE-reap) and `cqe_dispatch_ms`
/// (CQE-reap → future-resume) structured fields introduced by #3
/// (cqe-reap-timestamp).
///
/// A 100 ms pre-write delay is injected via
/// `TEST_PRE_WRITE_DELAY_MS_BY_DIGEST` so that `enqueue_ms ≥ 100` and
/// `total_inner_ms > 50` — the condition that gates the `warn!`.
///
/// Mutation: if the `warn!` macro invocation is edited to remove either
/// `cqe_kernel_ms` or `cqe_dispatch_ms` (or if the fields are computed
/// but not passed to `warn!`), this test red-fails with bespoke message.
///
/// Skips when io_uring is unavailable (Path B / spawn_blocking does not
/// use the writev timestamp path and would not emit these fields).
#[nativelink_test]
async fn b1_writev_warn_contains_cqe_split_fields() {
    if !skip_if_no_io_uring("b1_writev_warn_contains_cqe_split_fields").await {
        return;
    }
    let store = make_fs_store().await;
    // One chunk: minimal blob that completes in a single writev.
    let (blob, digest, total) = make_blob_mib(1, 0xd3);

    // Install 100 ms pre-write delay so total_inner_ms > 50 → warn fires.
    nativelink_store::chunked::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
        .lock()
        .insert(digest, 100);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );

    let permit = budget
        .try_acquire_chunk()
        .expect("T10: ChunkBudget must have permits available");
    tx.send(ChunkWork {
        chunk_offset: 0,
        chunk_bytes: Bytes::from(blob),
        finish: true,
        _permit: permit,
        _pin_permit: None,
    })
    .await
    .expect("T10: ChunkWork send must succeed");
    drop(tx);

    tokio::time::timeout(Duration::from_secs(10), driver.await_completion())
        .await
        .expect("T10: commit must complete within 10 s (deadlock detector)")
        .expect("T10: blob commit must succeed");

    // Cleanup.
    nativelink_store::chunked::chunked_filesystem::TEST_PRE_WRITE_DELAY_MS_BY_DIGEST
        .lock()
        .remove(&digest);

    // The warn! is emitted inside `process_completion` when total_inner_ms > 50.
    // Both new fields must appear in the structured warn output.
    logs_assert(|lines: &[&str]| {
        let has_kernel = lines.iter().any(|l| {
            l.contains(" WARN ") && l.contains("cqe_kernel_ms")
        });
        let has_dispatch = lines.iter().any(|l| {
            // NOTE (testing-czar, cqe-split review): verifies field NAME
            // presence only. A mutation swapping the kernel/dispatch
            // computation FORMULAS (keeping both names) survives this test;
            // the fork's dispatch_window_guard covers the capture-point
            // semantics but not the warn labeling. Closing this needs a
            // structured-log VALUE assertion — follow-up filed.
            l.contains(" WARN ") && l.contains("cqe_dispatch_ms")
        });
        match (has_kernel, has_dispatch) {
            (true, true) => Ok(()),
            (false, _) => Err(
                "T10: cqe_kernel_ms missing from warn! (#3 cqe-reap-timestamp field not emitted) \
                 — mutation: remove cqe_kernel_ms from process_completion warn! to reproduce"
                    .to_string(),
            ),
            (true, false) => Err(
                "T10: cqe_dispatch_ms missing from warn! (#3 cqe-reap-timestamp field not emitted) \
                 — mutation: remove cqe_dispatch_ms from process_completion warn! to reproduce"
                    .to_string(),
            ),
        }
    });
}

/// #F3 (2026-07-28 stale-marker wedge) T3: ABORTING the driver task
/// (the production leak path — bytestream idle-stream sweeper cancels
/// `FastSlowStore::update (chunked)` mid-dispatch → `InFlightCleanup`
/// drops the last `Arc<ChunkedDriver>` → `JoinHandleDropGuard` aborts
/// `run_driver` at an await point) must NOT leak the `IoUringMarker`
/// entry in the `chunked_partials` map. A leaked entry wedges every
/// subsequent Path-B (`write_chunk_at_offset`) session for the digest
/// forever: 41 digests, ~3.6K aborts/hour in production.
///
/// Mutation step: replace `_marker_guard = Some(guard)` in
/// `run_driver` (chunked_driver.rs Path A lazy-init) with
/// `core::mem::forget(guard)` — this test must fail with the
/// "aborted io_uring driver must remove its IoUringMarker entry"
/// message below.
#[nativelink_test]
async fn f3_driver_abort_removes_iouring_marker_entry() {
    if !skip_if_no_io_uring("f3_driver_abort_removes_iouring_marker_entry").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(2, 0x53);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );
    let permit = budget
        .try_acquire_chunk()
        .expect("ChunkBudget must have permits available");

    // First (non-finish) chunk: triggers Path A lazy-init → marker
    // inserted + writer task spawned. The blob is NOT completed.
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(blob[..CHUNK_SIZE].to_vec()),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("ChunkWork send must succeed on a fresh driver");
        // Wait until the driver has actually inserted the marker.
        while !store.has_in_flight_chunked_partial(&digest) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("marker entry must appear after first chunk (deadlock detector)");

    // Abort the driver task mid-blob: drop the ChunkedDriver while the
    // sender is still alive — `JoinHandleDropGuard` aborts `run_driver`
    // between awaits, exactly like the production sweeper-cancel path.
    drop(driver);

    tokio::time::timeout(Duration::from_secs(5), async {
        while store.has_in_flight_chunked_partial(&digest) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "aborted io_uring driver must remove its IoUringMarker entry \
         (IoUringMarkerGuard::drop) — a leaked entry is the permanent Path-B \
         wedge of 2026-07-28 (41 digests retry-forever on the contract-bug error)",
    );
    drop(tx);
}

/// #F3 T3b: the driver's NATURAL error exit "upstream dropped without
/// finish" (`run_driver` returns `Err(Code::Aborted)` WITHOUT calling
/// `discard_chunked` — by design the partial file stays for retry
/// reuse) must also not leak the `IoUringMarker` entry. Unlike the
/// abort path this exit is deterministic: the guard drops before
/// `await_completion` resolves, so the map must be clean immediately
/// after the completion error is observed.
#[nativelink_test]
async fn f3_driver_upstream_drop_removes_iouring_marker_entry() {
    if !skip_if_no_io_uring("f3_driver_upstream_drop_removes_iouring_marker_entry").await {
        return;
    }
    let store = make_fs_store().await;
    let (blob, digest, total) = make_blob_mib(2, 0x54);

    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&store),
        digest,
        total,
        CHUNK_SIZE,
        PER_BLOB_MPSC_CAP,
    );
    let permit = budget
        .try_acquire_chunk()
        .expect("ChunkBudget must have permits available");

    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(ChunkWork {
            chunk_offset: 0,
            chunk_bytes: Bytes::from(blob[..CHUNK_SIZE].to_vec()),
            finish: false,
            _permit: permit,
            _pin_permit: None,
        })
        .await
        .expect("ChunkWork send must succeed on a fresh driver");
        while !store.has_in_flight_chunked_partial(&digest) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("marker entry must appear after first chunk (deadlock detector)");

    // Upstream drop WITHOUT finish: driver exits via the
    // "mpsc closed without commit" arm (case i) — Err(Aborted), no
    // discard by design.
    drop(tx);
    let completion = tokio::time::timeout(Duration::from_secs(5), driver.await_completion())
        .await
        .expect("driver must terminate after upstream drop (deadlock detector)");
    let err = completion.expect_err(
        "upstream-drop-without-finish must surface Err from the driver (fixture guard: \
         if this is Ok the test is not exercising the leak arm)",
    );
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("upstream dropped without finish")),
        "fixture guard: expected the case-(i) driver error, got {err:?}",
    );
    assert!(
        !store.has_in_flight_chunked_partial(&digest),
        "driver's natural 'upstream dropped without finish' exit must remove the \
         IoUringMarker entry (IoUringMarkerGuard::drop) — a leaked entry is the \
         permanent Path-B wedge of 2026-07-28",
    );
}
