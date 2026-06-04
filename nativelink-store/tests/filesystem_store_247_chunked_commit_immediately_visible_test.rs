// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #247 regression: `FilesystemStore::has_with_results` MUST return
//! `Some(size)` for a digest immediately after the chunked driver
//! reports a successful commit. Before the fix, `finalize_holding`
//! renamed the holding file to the canonical CAS path but never
//! inserted the resulting `FileEntry` into the `evicting_map`, so the
//! file was on disk yet invisible to `has()` until the next process
//! startup walked `add_files_to_cache` over `content_path/d/`.
//!
//! Production symptom (2026-05-04): ~4012 "not found in either fast or
//! slow store" + 80 chunked-mismatch warns per 30 min. Server fell back
//! to peer-fetch, peers replied partial bytes, Bazel hashed the partial
//! bytes and declared mismatch. Bazel builds wedged.
//!
//! This test is the production-composition regression detector
//! (CLAUDE.md "Test in production composition, not in isolation").
//! `FilesystemStore::has_with_results` is exercised AS the slow-tier
//! consumer (`FastSlowStore::run_producer` → `head_result`) sees it,
//! immediately after the chunked driver's `commit_and_verify` Ok arm.
//!
//! Mutation step (CLAUDE.md TDD): comment out the
//! `evicting_map.insert(...)` call in
//! `FilesystemStore::finalize_holding` (filesystem_store.rs:~1571);
//! this test must FAIL with the bespoke message
//! `"FilesystemStore::has() must return Some(size) immediately after
//! chunked commit — file on disk but evicting_map not updated (#247)"`.

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    ExistenceCacheSpec, FilesystemSpec, MemorySpec, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{
    ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP,
};
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey};
use sha2::{Digest as _, Sha256};

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

/// Mirror of the chunked_driver in-file test helper. Builds a real
/// `FilesystemStore` rooted at a fresh per-test temp directory.
async fn make_test_store() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/247-immediate-has/content");
    let temp_path = format!("{base}/{nonce}/247-immediate-has/temp");
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

/// Production composition: drive a chunked write end-to-end via the
/// real `ChunkedDriver` (the production caller of `commit_chunked` +
/// `finalize_holding`), then immediately call
/// `FilesystemStore::has_with_results` and assert the freshly-committed
/// digest is visible.
///
/// The 5 s outer `tokio::time::timeout` is the deadlock detector
/// (CLAUDE.md "writer-termination class") — without it, a deadlock or
/// regression that re-introduces a bypass would hang the CI runner
/// instead of failing.
#[nativelink_test]
async fn chunked_commit_makes_blob_visible_to_has_immediately() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    let total = (N * CHUNK) as u64;

    // Real blob bytes + real matching digest so the e2e SHA-256 verify
    // arm in commit_and_verify takes the success path (not the unlink
    // path). The default digest hasher is Sha256 (digest_hasher.rs:53).
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa0u8 + i as u8).take(CHUNK));
    }
    let blob_hash = sha256(&blob);
    let digest = DigestInfo::new(blob_hash, total);

    let store = make_test_store().await;
    let budget = ChunkBudget::new();

    let (driver, tx) = ChunkedDriver::spawn_driver(
        store.clone(),
        digest,
        total,
        CHUNK,
        PER_BLOB_MPSC_CAP,
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        // Send all chunks in order; mark the last as `finish` to trigger
        // commit_and_verify inside the driver task.
        for i in 0..N {
            let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: bytes,
                finish: i == N - 1,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("driver mpsc still alive");
        }
        drop(tx);

        // Await the driver's commit signal. After this returns Ok, the
        // canonical CAS file is on disk (chunked_driver.rs:1239
        // `finalize_holding` Ok arm has run).
        let result = driver
            .await_completion()
            .await
            .expect("commit must succeed for hash-matching blob");
        assert_eq!(result.committed_size, total);

        // Production composition: this is the call that
        // `FastSlowStore::run_producer` makes via `head_result` to
        // decide whether to populate from slow → fast. Before the
        // #247 fix it returned None for ~seconds (until the next
        // FilesystemStore::new startup walk re-emplaced the file),
        // sending the producer down the WorkerProxyStore peer-fetch
        // path which would partial-respond and trigger a Bazel
        // hash-mismatch wedge.
        let mut results = [None];
        Pin::new(store.as_ref())
            .has_with_results(&[StoreKey::Digest(digest)], &mut results)
            .await
            .expect("has_with_results must succeed");
        assert_eq!(
            results[0],
            Some(total),
            "FilesystemStore::has() must return Some(size) immediately after \
             chunked commit — file on disk but evicting_map not updated (#247)",
        );
    })
    .await
    .expect(
        "must not deadlock — chunked commit + immediate has() must complete in 5s (#247)",
    );
}

/// #247 follow-up (commit `8ec47b35`) cancellation-safety regression
/// guard.
///
/// The post-rename `evicting_map.insert` was moved into a
/// `background_spawn!` so a caller-cancellation between `chunked_
/// finalize_holding` (the rename) and the insert cannot leave the
/// file on disk without an index entry. A future maintainer who
/// "simplifies" the spawn back to an inline `.await` would
/// re-introduce the narrower-window #247 — visible only at server-
/// shutdown / panic / abort time, undetectable in normal test runs.
///
/// **Why this is a structural test rather than a behavioral one:**
/// the cancellation-safety property is fundamentally hard to test
/// dynamically without production-code instrumentation. The window
/// between rename-complete and insert-complete is microseconds; a
/// `tokio::spawn(...).abort()` either wins the race against the
/// rename (cancels too early — both forms fail) or loses to the
/// inline insert (cancels too late — both forms pass). No
/// `tokio::time::sleep` duration reliably hits the post-rename /
/// pre-insert window. A structural test pinning the source pattern
/// catches the specific regression direction the reviewer
/// identified ("inline the spawn") with zero timing dependency.
///
/// Mutation step: replace `background_spawn!(...)` in
/// `finalize_holding` with the inline body — this test FAILS with
/// the bespoke `#247 cancellation-safety` message.
#[test]
fn finalize_holding_uses_background_spawn_for_cancellation_safety_247() {
    let source = include_str!("../src/filesystem_store.rs");
    let fn_start = source
        .find("pub async fn finalize_holding")
        .expect("FilesystemStore::finalize_holding fn must exist");
    // The function body ends before the next `pub async fn` or `pub fn`
    // declaration in the impl block (the next public method is
    // `unlink_holding`).
    let after_signature = &source[fn_start + 1..];
    let body_end_offset = after_signature
        .find("\n    pub async fn ")
        .or_else(|| after_signature.find("\n    pub fn "))
        .expect("a subsequent pub fn must follow finalize_holding");
    let body = &source[fn_start..fn_start + 1 + body_end_offset];
    assert!(
        body.contains("background_spawn!"),
        "FilesystemStore::finalize_holding MUST wrap the post-rename \
         evicting_map.insert in background_spawn! for cancellation-safety \
         (#247 follow-up; commit 8ec47b35). If this fires, an inline \
         `.await` form was reintroduced and an aborted parent task could \
         leave the file on disk without an evicting_map entry — the \
         narrower-window #247 re-introduction. Restore the background_spawn \
         wrap; see filesystem_store.rs:1208 (emplace_file) for the canonical \
         pattern, citing nativelink#495.",
    );
}

/// #247 production-composition lock-in. Wraps `FilesystemStore` in
/// `ExistenceCacheStore` (matches the production CAS chain at
/// `src/bin/nativelink.rs:393-447`) and asserts that a freshly-
/// committed chunked blob is visible THROUGH the cache layer too.
/// Today this is functionally equivalent to the FilesystemStore-only
/// test (because `ExistenceCacheStore` does not cache negative
/// results), but the equivalence is fragile to a future "negative
/// caching" change. This test pins the composition contract so any
/// such change has to update this test alongside the production code.
#[nativelink_test]
async fn chunked_commit_visible_through_existence_cache_store_247() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    let total = (N * CHUNK) as u64;

    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xc0u8 + i as u8).take(CHUNK));
    }
    let blob_hash = sha256(&blob);
    let digest = DigestInfo::new(blob_hash, total);

    let store = make_test_store().await;
    let budget = ChunkBudget::new();
    let (driver, tx) =
        ChunkedDriver::spawn_driver(store.clone(), digest, total, CHUNK, PER_BLOB_MPSC_CAP);

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let bytes = Bytes::from(blob[i * CHUNK..(i + 1) * CHUNK].to_vec());
            let permit = budget.try_acquire_chunk().expect("permit");
            tx.send(ChunkWork {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: bytes,
                finish: i == N - 1,
                _permit: permit,
                _pin_permit: None,
            })
            .await
            .expect("driver mpsc still alive");
        }
        drop(tx);
        let result = driver
            .await_completion()
            .await
            .expect("commit must succeed");
        assert_eq!(result.committed_size, total);

        // Wrap the FilesystemStore in ExistenceCacheStore. The `spec.
        // backend` field is unused by `ExistenceCacheStore::new` (it
        // takes the inner_store directly), so any placeholder spec
        // works.
        let cache = ExistenceCacheStore::new(
            &ExistenceCacheSpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                eviction_policy: None,
                log_not_found_at_info: false,
            },
            Store::new(store.clone()),
        );

        let mut results = [None];
        Pin::new(cache.as_ref())
            .has_with_results(&[StoreKey::Digest(digest)], &mut results)
            .await
            .expect("has_with_results must succeed");
        assert_eq!(
            results[0],
            Some(total),
            "ExistenceCacheStore must NOT mask FilesystemStore's post-#247 \
             index visibility. If this fires, ExistenceCacheStore added \
             negative caching (or some other change inverted the composition \
             behavior) and silently re-introduced #247 at the layer boundary.",
        );
    })
    .await
    .expect(
        "must not deadlock — chunked commit + ExistenceCacheStore-wrapped \
         has() must complete in 5s (#247 composition)",
    );
}
