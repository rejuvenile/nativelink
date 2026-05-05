// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #256 regression: a SECOND chunked commit for a digest that is
//! already in `FilesystemStore::evicting_map` MUST NOT cause the
//! canonical CAS file to disappear.
//!
//! Root cause (production trace 2026-05-05, PID 570543, ~575 PHANTOM
//! BLOB events between 06:23 and 08:59 PDT, 89% of phantom digests
//! had >=2 chunked-commit logs preceding the phantom):
//!
//! `FilesystemStore::finalize_holding` (filesystem_store.rs:1629-1693)
//! does, per call:
//! 1. `chunked_finalize_holding` renames `<digest>.holding` ->
//!    canonical CAS path. On a duplicate (key already present in the
//!    `evicting_map`), this overwrites the byte-identical canonical
//!    file (CAS-immutable so byte-identical content is OK).
//! 2. `background_spawn!` runs `evicting_map.insert(key, new_arc)`.
//!
//! Step 2 calls `MokaEvictingMap::insert` (moka_evicting_map.rs:391),
//! which:
//! - Captures the OLD `Arc<FileEntry>` via `cache.get(key)` (line 489).
//! - Replaces it via `cache.insert(key, new_data)` (line 490).
//! - Returns the OLD Arc to the caller; `insert` then calls
//!   `old.unref().await` (line 400).
//!
//! `FileEntryImpl::unref` (filesystem_store.rs:471) renames the OLD
//! entry's `from_path` (= canonical CAS path; `path_type ==
//! PathType::Content`) to a temp_path under `temp_path-cas/`.
//!
//! After both rename + unref complete, the on-disk state is:
//!   - canonical path: NO file (renamed away by old.unref())
//!   - temp_path: the file (orphan; will be deleted on Arc drop)
//!   - evicting_map[key]: NEW FileEntry pointing at PathType::Content
//!     (canonical) -- BUT THE FILE IS GONE.
//!
//! Subsequent `has_with_results(key)` returns Some(size) (entry is in
//! map), but `get_part(key)` opens the canonical path, gets ENOENT,
//! logs "Stale filesystem cache entry" and removes the entry.
//! `FastSlowStore::run_producer` sees `head_result = Some(size)` then
//! a `slow_store.get` Err NotFound -> emits the PHANTOM BLOB warn at
//! `fast_slow_store.rs`, falls back to `WorkerProxyStore` peer fetch,
//! peers reply partial bytes, Bazel hashes the partial bytes and
//! declares OutputDigestMismatchException -> Bazel build wedges.
//!
//! The CAS-immutable shortcut in `finalize_holding` (line 1657-1660)
//! checks `content_is_immutable && size_for_key.is_some()` and skips
//! the insert when both hold, but production deploys (per
//! `buildcache-native.json5`) do NOT set `content_is_immutable: true` for
//! the slow `FilesystemStore` (default is false at
//! `nativelink-config/src/stores.rs:717`), so the shortcut never fires
//! in production and every duplicate commit walks into the
//! insert+unref-the-old trap.
//!
//! Production composition: real `ChunkedDriver` x2 against a single
//! real `FilesystemStore`. 5s deadlock-detector timeout.
//!
//! Mutation step (CLAUDE.md TDD): if the fix is removed (i.e.
//! `finalize_holding` always replaces and triggers unref of the old
//! Arc), this test FAILS with the bespoke `#256` message. The fix
//! itself short-circuits the insert when an entry for `key` already
//! exists pointing at the SAME on-disk path -- because in CAS the
//! same digest = byte-identical content and the existing entry's
//! `path_type` is already `Content`, replacing it would only delete
//! the file we just re-renamed into place.

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_driver::{
    ChunkWork, ChunkedDriver, PER_BLOB_MPSC_CAP,
};
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreDriver, StoreKey};
use sha2::{Digest as _, Sha256};

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

async fn make_test_store() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/256-dup-commit/content");
    let temp_path = format!("{base}/{nonce}/256-dup-commit/temp");
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

async fn run_one_chunked_commit(
    store: Arc<FilesystemStore<FileEntryImpl>>,
    digest: DigestInfo,
    blob: &[u8],
    chunk: usize,
) {
    let n = (blob.len() + chunk - 1) / chunk;
    let total = blob.len() as u64;
    let budget = ChunkBudget::new();
    let (driver, tx) =
        ChunkedDriver::spawn_driver(store, digest, total, chunk, PER_BLOB_MPSC_CAP);

    for i in 0..n {
        let lo = i * chunk;
        let hi = std::cmp::min(lo + chunk, blob.len());
        let bytes = Bytes::from(blob[lo..hi].to_vec());
        let permit = budget.try_acquire_chunk().expect("permit");
        let chunk_sha = sha256(&bytes);
        tx.send(ChunkWork {
            chunk_offset: lo as u64,
            chunk_bytes: bytes,
            chunk_sha256: chunk_sha,
            finish: i == n - 1,
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
        .expect("commit must succeed for hash-matching blob");
    assert_eq!(result.committed_size, total);
}

/// Production trace summary:
/// - 06:38:35.761 PDT: chunked driver commit #1 for digest D
/// - 06:38:35.801 PDT: chunked driver commit #2 for digest D (40ms later)
/// - 08:58:35.454 PDT: ByteStream::inner_read for D (2h20m later)
///   - has() returns Some(size)
///   - get_part opens canonical path -> ENOENT
///   - "Failed to rename file" warn (unref tries content_path-cas/.../D ->
///     tmp_path-cas/.../D-with-counter and gets ENOENT)
///   - PHANTOM BLOB warn fires
///
/// This test:
/// 1. Drives chunked commit #1 -> file lands at canonical, entry in map.
/// 2. Drives chunked commit #2 (same digest, same content) -> the bug
///    triggers `unref` on the old Arc which renames the canonical file
///    to a temp path, leaving the new entry pointing at a file that
///    isn't there.
/// 3. Calls `get_part` (the call that `FastSlowStore::run_producer` makes
///    after `head_result = Some`). Asserts the bytes come back.
///
/// `get_part` is the right assertion (not `has_with_results`) because
/// the bug presents as has-says-Some-but-get-says-NotFound (PHANTOM
/// BLOB).
#[nativelink_test]
async fn duplicate_chunked_commit_must_not_unref_canonical_file_256() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    let total = (N * CHUNK) as u64;

    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xb0u8 + i as u8).take(CHUNK));
    }
    let blob_hash = sha256(&blob);
    let digest = DigestInfo::new(blob_hash, total);

    let store = make_test_store().await;

    tokio::time::timeout(Duration::from_secs(5), async {
        // First chunked commit: file lands at canonical path, entry
        // gets inserted into evicting_map.
        run_one_chunked_commit(store.clone(), digest, &blob, CHUNK).await;

        // Second chunked commit, same digest, byte-identical content.
        // Production hits this path when two parallel writers race for
        // the same blob (e.g. mirror_blobs + Bazel concurrent uploads).
        // Pre-fix: this triggers `evicting_map.insert(key, new_arc)`,
        // which captures the OLD Arc, replaces it, then `unref()`s the
        // OLD Arc. `unref` on an entry whose `path_type == Content`
        // renames the canonical CAS file to a temp path -> the file
        // is no longer at the canonical path the new entry points at.
        run_one_chunked_commit(store.clone(), digest, &blob, CHUNK).await;

        // Production composition: this is the call that
        // `FastSlowStore::run_producer` makes (`slow_store.get_part`)
        // immediately after `head_result = Some(size)`. Pre-fix, this
        // fails with NotFound because the canonical file was renamed
        // to a temp path by the second commit's insert+unref.
        //
        // Concurrent reader/producer pattern: spawn the producer side
        // (get_part writes into tx); concurrently drain rx in this task
        // until EOF. Both join via try_join so an error in either side
        // (the bug-side: NotFound from get_part) surfaces.
        let (mut tx, mut rx) = make_buf_channel_pair();
        let store_for_get = store.clone();
        let producer = async move {
            Pin::new(store_for_get.as_ref())
                .get_part(StoreKey::Digest(digest), &mut tx, 0, None)
                .await
        };
        let consumer = async {
            let mut received = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(chunk_bytes) => {
                        if chunk_bytes.is_empty() {
                            // EOF marker.
                            break Ok::<Vec<u8>, nativelink_error::Error>(received);
                        }
                        received.extend_from_slice(&chunk_bytes);
                    }
                    Err(e) => break Err(e),
                }
            }
        };

        let (producer_res, consumer_res) = tokio::join!(producer, consumer);
        producer_res.expect(
            "FilesystemStore::get_part must succeed after a duplicate \
             chunked commit -- the second commit's evicting_map.insert \
             must NOT trigger unref of the prior entry's canonical CAS \
             file (#256). If this fires, the duplicate-commit insert+unref \
             chain renamed the byte-identical canonical file out from \
             under the new entry, leaving the in-memory index pointing \
             at a missing file -> production PHANTOM BLOB at \
             FastSlowStore::run_producer + Bazel build wedge.",
        );
        let received = consumer_res.expect(
            "rx consumer must not error -- on the bug, get_part returns \
             NotFound and the consumer sees the channel closed with the \
             error before any bytes arrive (#256)",
        );
        assert_eq!(
            received.len() as u64,
            total,
            "FilesystemStore::get_part must return the full blob bytes \
             after a duplicate chunked commit (#256). Got {} bytes, \
             expected {}.",
            received.len(),
            total,
        );
    })
    .await
    .expect(
        "must not deadlock -- duplicate chunked commit + get_part must \
         complete in 5s (#256)",
    );
}
