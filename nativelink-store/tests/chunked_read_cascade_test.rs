// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #212 Phase 2.5: production-composition tests for the chunked-pin
//! read cascade.
//!
//! Verifies the design §6.3 step 2 wire-up in
//! `FastSlowStore::get_part`. The unit-level pin accessor tests live in
//! `chunked_driver.rs::tests` (`pin_accessor_full_range_hits_after_*`,
//! `pin_accessor_gap_returns_none`, `pin_accessor_overrun_returns_none`,
//! `pin_accessor_cleared_after_successful_commit`); these tests
//! exercise the wrapped composition (VerifyStore wrapping
//! FastSlowStore) so the cascade's writer-termination contract is
//! validated AS the production-config caller observes it (CLAUDE.md
//! "Test in production composition, not in isolation").
//!
//! Two contract directions covered (CLAUDE.md "Asymmetric contract
//! coverage"):
//! - **Under-action:** with kill-switch ON + driver in-flight + range
//!   covered, get_part MUST consult the registry and serve from the
//!   pin (instead of NotFound from the empty slow store).
//! - **Over-action:** with kill-switch OFF, get_part MUST NOT consult
//!   the registry (proved by a panicking probe driver that would fire
//!   if reached).

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP};
use nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use sha2::{Digest as _, Sha256};

const CHUNK: usize = 4 * 1024;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

/// Build a fresh on-disk FilesystemStore for the chunked driver to
/// write its sparse partial into. The chunked-pin test does not read
/// from this store directly — it only needs the driver to have a real
/// `pwrite` target. The slow-store side of the wrapping FastSlowStore
/// is a separate empty MemoryStore so the `failed_writes`-pin fallback
/// is the ONLY place the bytes can come from.
async fn make_chunked_filesystem() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/p2.5-chunked-fs/content");
    let temp_path = format!("{base}/{nonce}/p2.5-chunked-fs/temp");
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

/// Build a 3-chunk blob (12 KiB) + matching digest. Returns
/// `(blob_bytes, digest, total_size)`. CHUNK is 4 KiB so the driver
/// can be passed `chunk_size = CHUNK` as a test-only override of the
/// production `CHUNK_SIZE = 1 MiB` (per the chunked_driver tests'
/// established pattern).
fn make_blob(n: usize, fill_byte: u8) -> (Vec<u8>, DigestInfo, u64) {
    let total = (n * CHUNK) as u64;
    let mut blob = Vec::with_capacity(n * CHUNK);
    for i in 0..n {
        blob.extend(std::iter::repeat(fill_byte + i as u8).take(CHUNK));
    }
    let h = sha256(&blob);
    (blob, DigestInfo::new(h, total), total)
}

/// Wrap an `Arc<FastSlowStore>` in a real VerifyStore so the
/// production composition is exercised end-to-end. VerifyStore's
/// `tokio::join!(get_fut, check_fut)` is the historical writer-
/// termination contract carrier — if the chunked-pin step left the
/// writer open without EOF, the join would deadlock and the test
/// would `tokio::time::timeout` panic with the assertion message.
fn wrap_in_verify(fs: Arc<FastSlowStore>) -> Store {
    Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        Store::new(fs),
    ))
}

/// Phase 2.5 production-composition test: kill-switch ON + registry
/// installed + driver in-flight with all chunks landed → wrapped
/// get_part MUST return the assembled Bytes from the pin (NOT a
/// NotFound from the empty slow store).
///
/// Mutation step: in `fast_slow_store.rs::get_part`, comment out the
/// `try_get_chunk_from_pin` send branch. The slow store is empty
/// MemoryStore → FastSlowStore returns NotFound → VerifyStore
/// surfaces it → assertion below fires with the specific message
/// "must serve assembled bytes from chunked pin".
#[nativelink_test]
async fn pin_serves_request_under_verify_when_enabled() {
    const N: usize = 3;
    let (blob, digest, total) = make_blob(N, 0x80);

    // Build the FastSlowStore: empty MemoryStore fast + empty
    // MemoryStore slow. The chunked-pin path is the ONLY source of
    // the bytes.
    let fs_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    // Install a fresh registry and flip the kill-switch ON.
    let registry = ChunkedReadRegistry::new();
    let installed = fs_arc.set_chunked_read_registry(Arc::clone(&registry));
    assert!(installed, "registry must be a fresh install");
    fs_arc.enable_chunked_reads();
    assert!(
        fs_arc.chunked_reads_enabled(),
        "kill-switch must read back ON"
    );

    // Spin up a real chunked driver, ship all N chunks (no finish so
    // commit doesn't fire and clear the pin).
    let chunked_fs = make_chunked_filesystem().await;
    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&chunked_fs),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    registry.register(digest, Arc::clone(&driver_arc));

    // Send all chunks (no finish — keeps the pin populated).
    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }
        // Wait for all N chunks pin-resident.
        loop {
            if driver_arc.chunks_committed() == N as u64 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — driver setup");

    // Wrap in VerifyStore (production composition). VerifyStore
    // verifies size + hash via tokio::join!(get_fut, check_fut) — if
    // the cascade fails to terminate the inner writer, the join
    // deadlocks and the timeout below fires with our specific message.
    let wrapped = wrap_in_verify(fs_arc);

    let got = tokio::time::timeout(
        Duration::from_secs(5),
        wrapped.get_part_unchunked(digest, 0, Some(total)),
    )
    .await
    .expect(
        "must not deadlock — chunked-pin cascade step must terminate the inner writer \
         (VerifyStore's join is the load-bearing observer of the writer-termination contract)",
    )
    .expect("must serve assembled bytes from chunked pin (cascade step 2)");
    assert_eq!(
        got.len(),
        blob.len(),
        "served byte length must equal blob length",
    );
    assert_eq!(
        &got[..],
        &blob[..],
        "served bytes must equal the originally-pinned blob bytes",
    );

    // Cleanup: drop the driver (so the spawned task can exit) so the
    // test doesn't leave background work running past the test exit.
    drop(tx);
    registry.deregister(&digest);
}

/// Phase 2.5 over-action test: kill-switch OFF + registry installed +
/// driver in-flight → get_part MUST NOT consult the registry.
///
/// Setup: pin holds bytes; slow store is empty. Expected OFF
/// behavior: cascade skips the pin → slow-store NotFound surfaces.
/// If the kill-switch guard is broken, the pin is consulted → bytes
/// served → call succeeds with `Ok(...)`. The test asserts `Err` AND
/// the specific `Code::NotFound` from the slow-store fall-through.
///
/// Mutation step: in `fast_slow_store.rs::get_part`, REMOVE the
/// `if self.chunked_reads_enabled.load(...)` guard. The pin is
/// consulted unconditionally → bytes served → call succeeds → the
/// `expect_err("kill-switch OFF must NOT serve from chunked pin ...")`
/// below panics with that specific message.
#[nativelink_test]
async fn pin_skipped_when_kill_switch_off() {
    const N: usize = 2;
    let (blob, digest, total) = make_blob(N, 0x90);

    let fs_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let registry = ChunkedReadRegistry::new();
    fs_arc.set_chunked_read_registry(Arc::clone(&registry));
    // KILL-SWITCH IS OFF — the cascade MUST skip the registry entirely.
    assert!(
        !fs_arc.chunked_reads_enabled(),
        "kill-switch default must be OFF",
    );

    let chunked_fs = make_chunked_filesystem().await;
    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&chunked_fs),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    registry.register(digest, Arc::clone(&driver_arc));

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }
        loop {
            if driver_arc.chunks_committed() == N as u64 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — driver setup (off-switch test)");

    let wrapped = wrap_in_verify(fs_arc);

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        wrapped.get_part_unchunked(digest, 0, Some(total)),
    )
    .await
    .expect("must not deadlock — kill-switch-OFF cascade still terminates writer");

    // OFF behavior: the pin is NOT consulted; the empty slow store
    // returns NotFound. If the kill-switch is broken, the call would
    // succeed (bytes from the pin) and the expect_err below panics
    // with the specific "must NOT serve from chunked pin" message.
    let err = res.expect_err(
        "kill-switch OFF must NOT serve from chunked pin — \
         the slow-store fallthrough is empty so the call MUST error \
         (NotFound). If this expect_err panics with `Ok(...)`, the \
         chunked_reads_enabled guard in FastSlowStore::get_part has \
         been removed or is not gating the registry consultation.",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::NotFound,
        "kill-switch OFF cascade must terminate at the empty slow store \
         (NotFound). Got: {err:?}"
    );

    drop(tx);
    registry.deregister(&digest);
}

/// Phase 2.5 cascade ordering test: when both the slow store HAS the
/// blob AND a chunked driver is in-flight with different (lying)
/// pinned bytes for the same digest, the cascade MUST consult the pin
/// FIRST (per design §6.3 step 2 BEFORE step 3 slow-store). This is
/// the load-bearing per-blob ordering claim.
///
/// We assert the ordering by byte-comparison: if pin-first, the
/// returned bytes equal `lying_blob` (sourced from the pin); if
/// (incorrectly) slow-store-first, the returned bytes equal
/// `real_blob`. VerifyStore is wired with `verify_size=true,
/// verify_hash=false` here because we deliberately serve bytes whose
/// hash does not match the digest (so verify_hash would just produce
/// a DataLoss error that hides the SOURCE-of-bytes question we're
/// actually asking).
#[nativelink_test]
async fn pin_consulted_before_slow_store_when_enabled() {
    const N: usize = 2;
    let (real_blob, digest, total) = make_blob(N, 0xa0);
    let mut lying_blob = vec![0u8; N * CHUNK];
    for (i, byte) in lying_blob.iter_mut().enumerate() {
        *byte = ((i + 1) & 0xFF) as u8;
    }

    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    // Slow store gets the REAL blob — if the cascade consults slow
    // before pin, the served bytes equal real_blob; if pin first, the
    // served bytes equal lying_blob.
    slow_store
        .update_oneshot(digest, Bytes::from(real_blob.clone()))
        .await
        .expect("seed slow store with real blob");

    let fs_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        slow_store,
    );

    let registry = ChunkedReadRegistry::new();
    fs_arc.set_chunked_read_registry(Arc::clone(&registry));
    fs_arc.enable_chunked_reads();

    let chunked_fs = make_chunked_filesystem().await;
    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&chunked_fs),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    registry.register(digest, Arc::clone(&driver_arc));

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(lying_blob[i * CHUNK..(i + 1) * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }
        loop {
            if driver_arc.chunks_committed() == N as u64 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — order-test driver setup");

    // Wrap with size-only VerifyStore (verify_hash=false). Both the
    // lying bytes and the real bytes are the same length so size
    // verification passes either way; the byte-comparison below is
    // the load-bearing assertion of the cascade ORDER.
    let wrapped = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fs_arc),
    ));

    let got = tokio::time::timeout(
        Duration::from_secs(5),
        wrapped.get_part_unchunked(digest, 0, Some(total)),
    )
    .await
    .expect("must not deadlock — order-test cascade")
    .expect("get_part must succeed (verify_hash=false; both byte sources have correct length)");

    // The cascade consults the pin FIRST per design §6.3 step 2. The
    // returned bytes MUST equal lying_blob (sourced from the pin),
    // NOT real_blob (which would mean slow-store-first ordering).
    assert_eq!(
        got.len(),
        lying_blob.len(),
        "served byte length matches blob length",
    );
    assert_eq!(
        &got[..],
        &lying_blob[..],
        "pin-first cascade ordering: bytes MUST come from the pin (lying_blob), \
         not from the slow store (real_blob). If this assertion fails, the \
         cascade is consulting the slow store BEFORE the chunked-driver pin.",
    );
    assert_ne!(
        &got[..],
        &real_blob[..],
        "served bytes equal real_blob — cascade ORDER inverted (slow before pin)",
    );

    drop(tx);
    registry.deregister(&digest);
}

/// #254 P5 follow-up regression test. The #252 fix returned
/// `Code::Unavailable` immediately on partial-coverage. Production
/// shipped on 2026-05-05 and the new branch fired ~6/sec because
/// chunked writes are the dominant write path with ~4 KiB chunks
/// (hundreds per blob); any concurrent get_part during the multi-second
/// commit window hits partial-miss. Bazel translates Unavailable →
/// FailedPrecondition and aborts builds.
///
/// The #254 fix replaces the immediate Err with a bounded poll-loop:
/// up to 500 ms waiting for missing chunks to land. If they DO land,
/// serve from the pin (cascade hit, no Unavailable). Three regression
/// tests cover the contract:
///   1. Chunk lands within the wait window → serve from pin (this test).
///   2. Driver completes within the wait window → fall through to slow.
///   3. Wait times out → fall through to slow (pre-#252 behavior).
///
/// Mutation step: in `fast_slow_store.rs::get_part`, comment out the
/// `tokio::time::sleep(...)` inside the partial-miss poll loop. The
/// loop spins without yielding control to the chunk-sending future →
/// chunk 1 never lands → loop exits on deadline → fall-through to
/// slow store → assertion fails.
#[nativelink_test]
async fn pin_partial_coverage_waits_then_serves_from_pin_when_chunk_lands_254() {
    const N: usize = 3;
    let (real_blob, digest, total) = make_blob(N, 0xb0);

    // Slow store is EMPTY. The pin is the only source of bytes.
    // If the wait-loop falls through (broken implementation), the
    // assertion below fires with NotFound or empty bytes.
    let fs_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let registry = ChunkedReadRegistry::new();
    fs_arc.set_chunked_read_registry(Arc::clone(&registry));
    fs_arc.enable_chunked_reads();

    let chunked_fs = make_chunked_filesystem().await;
    let budget = Arc::new(ChunkBudget::new());
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&chunked_fs),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    registry.register(digest, Arc::clone(&driver_arc));

    // Send chunks 0 and 2 first. Chunk 1 is the gap that will land
    // partway through the wait window.
    tokio::time::timeout(Duration::from_secs(5), async {
        for &i in &[0usize, 2usize] {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(real_blob[i * CHUNK..(i + 1) * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }
        loop {
            if driver_arc.chunks_committed() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — initial partial-coverage setup");

    // Spawn a task that sends chunk 1 after a short delay. The
    // delay must be SHORTER than the wait-loop budget (500 ms) so
    // the pin gets covered before the loop deadline. 50 ms is well
    // within budget and gives the read path time to enter the loop.
    let blob_for_send = real_blob.clone();
    let tx_for_send = tx.clone();
    let budget_for_send = Arc::clone(&budget);
    let late_send = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let permit = budget_for_send.try_acquire_chunk().expect("permit");
        tx_for_send
            .send(ChunkWork {
                chunk_offset: CHUNK as u64,
                chunk_bytes: Bytes::from(blob_for_send[CHUNK..2 * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("late send");
    });

    let wrapped = wrap_in_verify(fs_arc);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        wrapped.get_part_unchunked(digest, 0, Some(total)),
    )
    .await
    .expect("must not deadlock — partial-coverage wait-loop");

    let got = result.expect(
        "partial-cover wait-loop must serve assembled bytes from the pin \
         once chunk 1 lands within the 500ms wait budget. If this returns \
         Err, the wait loop is missing or the late chunk-arrival is not \
         polled (mutation: tokio::time::sleep removed → busy-loop starves \
         the late-send task → fall-through to empty slow store → NotFound)",
    );
    assert_eq!(
        got.len(),
        real_blob.len(),
        "served byte length matches blob length (full pin coverage after wait)",
    );
    assert_eq!(
        &got[..],
        &real_blob[..],
        "served bytes match real_blob (sourced from pin after wait)",
    );

    late_send.await.expect("late-send task");
    drop(tx);
    registry.deregister(&digest);
}

/// #254 partial-coverage timeout test: the wait budget elapses without
/// the missing chunk landing AND without the driver completing. Per
/// design the cascade falls through to the slow store (pre-#252
/// behavior). The slow store IS seeded so the get_part returns Ok
/// rather than Code::NotFound — we assert the path is fall-through,
/// NOT the #252-era immediate Code::Unavailable.
///
/// Mutation step: replace the wait-loop's deadline-break with
/// `return Err(make_err!(Code::Unavailable, ...))`. The fall-through
/// is suppressed → result is Err(Unavailable) → assertion fails.
#[nativelink_test]
async fn pin_partial_coverage_falls_through_on_timeout_254() {
    const N: usize = 3;
    let (real_blob, digest, total) = make_blob(N, 0xc0);

    // Slow store IS seeded with the real blob. After the wait-budget
    // elapses, fall-through must serve real_blob from slow tier.
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    slow_store
        .update_oneshot(digest, Bytes::from(real_blob.clone()))
        .await
        .expect("seed slow store");

    let fs_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        slow_store,
    );

    let registry = ChunkedReadRegistry::new();
    fs_arc.set_chunked_read_registry(Arc::clone(&registry));
    fs_arc.enable_chunked_reads();

    let chunked_fs = make_chunked_filesystem().await;
    let budget = ChunkBudget::new();
    let (driver, tx) = ChunkedDriver::spawn_driver(
        Arc::clone(&chunked_fs),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );
    let driver_arc = Arc::new(driver);
    registry.register(digest, Arc::clone(&driver_arc));

    // Send only chunks 0 and 2 — chunk 1 is the gap. NEVER send
    // chunk 1 → wait-budget MUST elapse → fall-through.
    tokio::time::timeout(Duration::from_secs(5), async {
        for &i in &[0usize, 2usize] {
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(real_blob[i * CHUNK..(i + 1) * CHUNK].to_vec()),
                chunk_sha256: [0u8; 32],
                finish: false,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("send");
        }
        loop {
            if driver_arc.chunks_committed() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must not deadlock — partial-coverage setup");

    let wrapped = wrap_in_verify(fs_arc);

    // 5s timeout safely covers the 500ms wait-budget plus slow-tier
    // serve. If wait-budget is missing entirely, this still completes
    // quickly via fall-through.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        wrapped.get_part_unchunked(digest, 0, Some(total)),
    )
    .await
    .expect("must not deadlock — partial-coverage timeout cascade");

    let got = result.expect(
        "partial-cover wait-loop timeout MUST fall through to slow store \
         (NOT return Code::Unavailable as the #252 fix did). Bazel sees \
         Unavailable → FailedPrecondition and aborts; production cannot \
         tolerate ~6/sec of these. The slow store is seeded so fall-through \
         must succeed.",
    );
    assert_eq!(
        got.len(),
        real_blob.len(),
        "fall-through served byte length matches real_blob length",
    );
    assert_eq!(
        &got[..],
        &real_blob[..],
        "fall-through served bytes match the seeded slow-store blob",
    );

    // Suppress unused-warning for CHUNK_SIZE import — kept for forward
    // reference: production CHUNK_SIZE is 1 MiB; tests use 4 KiB.
    let _ = CHUNK_SIZE;

    drop(tx);
    registry.deregister(&digest);
}
