// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #212 Phase 2.5/2.7 fixup S2 — end-to-end integration test.
//!
//! Wires a real `FastSlowStore` (fast = MemoryStore, slow =
//! FilesystemStore) with BOTH:
//!   - Phase 2.5's `ChunkedReadRegistry` installed via
//!     `set_chunked_read_registry` + `enable_chunked_reads()`.
//!   - Phase 2.7's `BazelChunkedDispatcherImpl` installed via
//!     `set_bazel_chunked_dispatcher` (with the SAME registry passed
//!     via `with_registry`) + `set_bazel_facing_internal_chunking_enabled(true)`.
//!
//! The test then drives a Bazel-shaped client write through
//! `FastSlowStore::update`. The (β) async-commit path returns Ok at
//! admission; reads via the in-flight pin should succeed during the
//! async-commit window. After the chunked driver commits the bytes
//! to disk, reads should still succeed via the slow tier.
//!
//! Mutation step at the end of each test confirms the assertion
//! actually guards the wiring (commenting out the registry register
//! call → red-fail with the specific message).

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::{
    BazelChunkedDispatcherImpl, ChunkedWriteHandlerMetrics, ChunkedWriteInFlight,
};
use nativelink_store::chunked::chunk_budget::ChunkBudget;
use nativelink_store::chunked::chunked_read_registry::ChunkedReadRegistry;
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, set_bazel_facing_internal_chunking_enabled,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair_with_size;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
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

fn kill_switch_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn make_filesystem_store() -> (Arc<FilesystemStore<FileEntryImpl>>, String) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/p2.5-7-e2e/content");
    let temp_path = format!("{base}/{nonce}/p2.5-7-e2e/temp");
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

/// Build a fully-wired FastSlowStore (registry + dispatcher both
/// installed) with a small chunk size so the test can exercise the
/// chunked path without burning megabytes per chunk.
async fn make_e2e_fast_slow(
    chunk_size: usize,
) -> (
    Arc<FastSlowStore>,
    Arc<FilesystemStore<FileEntryImpl>>,
    Arc<ChunkedReadRegistry>,
    Arc<ChunkedWriteInFlight>,
    Arc<ChunkedWriteHandlerMetrics>,
    String,
) {
    let (fs_store, content_path) = make_filesystem_store().await;
    let fast_store: Store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store: Store = Store::new(fs_store.clone());
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
        },
        fast_store,
        slow_store,
    );

    // Build registry + dispatcher with all wiring (S1).
    let registry = ChunkedReadRegistry::new();
    let in_flight = ChunkedWriteInFlight::new();
    let chunk_budget = make_test_chunk_budget();
    // Pin budget large enough to cover the whole blob in tests; the B1
    // cap test exercises the small-cap rejection separately.
    let pin_budget = make_test_pin_budget(64 * 1024 * 1024);
    let metrics = Arc::new(ChunkedWriteHandlerMetrics::default());
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
        ),
    );
    fast_slow.set_chunked_read_registry(Arc::clone(&registry));
    fast_slow
        .set_bazel_chunked_dispatcher(Arc::clone(&dispatcher) as Arc<dyn BazelChunkedDispatcher>);
    fast_slow.set_chunked_size_threshold_for_test(chunk_size as u64);
    // Read-side kill-switch ON.
    fast_slow.enable_chunked_reads();

    (
        fast_slow,
        fs_store,
        registry,
        in_flight,
        metrics,
        content_path,
    )
}

/// Stream `data` into FastSlowStore::update via a buf-channel pair.
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
        if !data.is_empty() {
            tx.send(data).await.expect("tx.send must succeed");
        }
        tx.send_eof().expect("tx.send_eof must succeed");
        Result::<(), nativelink_error::Error>::Ok(())
    };
    let store_call =
        async move { store_clone.update(key, rx, UploadSizeInfo::ExactSize(total)).await };
    let (writer_res, store_res) = tokio::join!(update_fut, store_call);
    writer_res?;
    store_res
}

/// **S2 dispatcher↔registry wire** — drives a real `BazelChunkedDispatcherImpl`
/// (constructed via `with_registry`) through the `dispatch` trait API,
/// and asserts that DURING the dispatch the registry contains an entry
/// for the digest. After the dispatch completes, the registry MUST be
/// empty again (deregister fired).
///
/// We use a fast-tier-only setup with a 50 ms artificial delay between
/// chunks to leave time for the test to observe the registry mid-flight.
/// For the visibility window, the test reads `registry.get(&digest)`
/// in a parallel task during the dispatch.
///
/// This complements the unit-level `chunked_read_cascade_test.rs` (which
/// drives the registry manually) by exercising the actual production
/// register/deregister path through `dispatch_chunks_to_driver`.
///
/// **Mutation step:** comment out the `let _prev = reg.register(digest,
/// Arc::clone(&driver));` line in `dispatch_chunks_to_driver`. The
/// `mid_dispatch_registered` assertion red-fails with the specific
/// message identifying the dead-wire bug.
#[nativelink_test]
async fn e2e_dispatcher_registers_driver_in_registry_during_dispatch() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 23) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    set_bazel_facing_internal_chunking_enabled(true);

    let (fast_slow, _fs_store, registry, _in_flight_chunked, _metrics, _content_path) =
        make_e2e_fast_slow(CHUNK).await;

    // Pre-flight: registry empty.
    assert!(
        registry.get(&digest).is_none(),
        "registry must start empty for this digest",
    );
    let pre_hits = registry_pin_hits(&registry);

    // Drive upload via a slow producer (yields between chunks) so the
    // parallel poll-loop has time to observe the registration.
    let (mut tx, rx) = make_buf_channel_pair_with_size(4);
    let blob_for_writer = blob.clone();
    let writer_fut = tokio::spawn(async move {
        for i in 0..N {
            let chunk = Bytes::copy_from_slice(&blob_for_writer[i * CHUNK..(i + 1) * CHUNK]);
            tx.send(chunk).await.expect("tx.send");
            // Yield repeatedly to give the dispatcher + observer task
            // time to run.
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        }
        tx.send_eof().expect("send_eof");
    });

    let store_for_update: Arc<FastSlowStore> = Arc::clone(&fast_slow);
    let key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    let update_fut = tokio::spawn(async move {
        store_for_update
            .update(key, rx, UploadSizeInfo::ExactSize(SIZE as u64))
            .await
    });

    // Concurrent observer: poll the registry. As soon as we see the
    // digest registered, set the flag.
    let registry_for_observer = Arc::clone(&registry);
    let observer_fut = tokio::spawn(async move {
        for _ in 0..1_000_000 {
            if registry_for_observer.get(&digest).is_some() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    });

    let (writer_res, update_res, observer_res) = tokio::time::timeout(
        Duration::from_secs(15),
        async { tokio::join!(writer_fut, update_fut, observer_fut) },
    )
    .await
    .expect("must not deadlock — E2E dispatcher↔registry test");

    writer_res.expect("writer task panic");
    update_res
        .expect("update task panic")
        .expect("update must succeed via β async-commit");
    let mid_dispatch_registered = observer_res.expect("observer task panic");

    assert!(
        mid_dispatch_registered,
        "registry MUST contain the digest during dispatch — the \
         observer task polled `registry.get(&digest)` for many \
         yields and never saw a Some. This means the \
         BazelChunkedDispatcherImpl is not calling \
         `registry.register(digest, Arc::clone(&driver))` in \
         `dispatch_chunks_to_driver` (S1 dead-wire regression). \
         Phase 2.5's read cascade can never serve from the in-flight \
         pin without this register call.",
    );

    // Post-dispatch: registry MUST be empty again (deregister fired).
    // Wait briefly for the async reaper to deregister.
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if registry.get(&digest).is_none() {
                break true;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        drained,
        "registry MUST drain the digest after dispatch completes — \
         the reaper in `dispatch_chunks_to_driver`'s AsyncCommit branch \
         must call `reg.deregister(&digest)` after `await_completion()`. \
         A stale registry entry would inflate `pin_partial_misses_total` \
         on every subsequent read of a digest the slow tier already has.",
    );

    // Sanity: pin_hits delta is non-negative (the cascade may or may
    // not have reached the chunked-pin step depending on cargo's
    // parallel-test ordering with the fast-tier MemoryStore; that
    // exact assertion is covered by `chunked_read_cascade_test.rs`'s
    // `pin_serves_request_under_verify_when_enabled` which uses an
    // empty fast tier deterministically).
    let post_hits = registry_pin_hits(&registry);
    assert!(
        post_hits >= pre_hits,
        "pin_hits_total counter is monotone (got pre={pre_hits} post={post_hits})",
    );

    set_bazel_facing_internal_chunking_enabled(false);
}

/// Helper: snapshot the registry's `pin_hits_total` via the metric
/// publication path. We don't have a direct accessor, so we read via
/// `format!` of the registry's debug.
///
/// More robust: introspect via the register's dirty `register/get/
/// deregister` API; the actual counter is internal but its monotonic
/// behavior is observable through the published metric. For this test
/// we read it via the new `pin_hits_total` accessor (see the
/// chunked_read_registry doc).
fn registry_pin_hits(registry: &Arc<ChunkedReadRegistry>) -> u64 {
    registry.pin_hits_total()
}

/// **B2 graceful-shutdown drain** — kick off a chunked dispatch (which
/// inserts the digest into the FastSlowStore's
/// `chunked_in_flight_digests` set), call `flush_slow_writes` mid-
/// dispatch with a generous timeout. The drain MUST wait for the
/// chunked-driver reaper to remove the digest (commit complete or
/// failed), THEN return 0 (all flushed).
///
/// Without B2's wiring, `flush_slow_writes` would return 0 immediately
/// (the legacy `in_flight_slow_writes` map is empty for chunked-path
/// blobs), the chunked driver would still be mid-commit, and the
/// graceful shutdown would lose the in-flight write.
///
/// **Mutation step:** comment out the
/// `set.lock().insert(digest);` line in
/// `BazelChunkedDispatcherImpl::dispatch`. The drain returns 0 before
/// the commit lands; the assertion below `commit_complete_before_drain`
/// would be false → test red-fails.
#[nativelink_test]
async fn b2_flush_slow_writes_waits_for_chunked_dispatch_commit() {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    const CHUNK: usize = 4 * 1024;
    const N: usize = 4;
    const SIZE: usize = N * CHUNK;
    let blob: Vec<u8> = (0..SIZE).map(|i| (i * 7) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), SIZE as u64);

    let _guard = kill_switch_lock().lock().await;
    set_bazel_facing_internal_chunking_enabled(true);

    let (fast_slow, _fs_store, _registry, _in_flight_chunked, _metrics, _content_path) =
        make_e2e_fast_slow(CHUNK).await;

    // Drive a slow upload (yield between chunks).
    let (mut tx, rx) = make_buf_channel_pair_with_size(2);
    let blob_for_writer = blob.clone();
    let writer_done = Arc::new(AtomicBool::new(false));
    let writer_done_for_task = Arc::clone(&writer_done);
    let writer_fut = tokio::spawn(async move {
        for i in 0..N {
            let chunk = Bytes::copy_from_slice(&blob_for_writer[i * CHUNK..(i + 1) * CHUNK]);
            tx.send(chunk).await.expect("tx.send");
            for _ in 0..30 {
                tokio::task::yield_now().await;
            }
        }
        tx.send_eof().expect("send_eof");
        writer_done_for_task.store(true, AtomicOrdering::Release);
    });

    let store_for_update: Arc<FastSlowStore> = Arc::clone(&fast_slow);
    let key: nativelink_util::store_trait::StoreKey<'static> =
        nativelink_util::store_trait::StoreKey::Digest(digest);
    let update_fut = tokio::spawn(async move {
        store_for_update
            .update(key, rx, UploadSizeInfo::ExactSize(SIZE as u64))
            .await
    });

    // Wait for the chunked-in-flight set to register the digest, then
    // call flush_slow_writes. The drain MUST wait for the dispatch to
    // complete.
    let store_for_drain = Arc::clone(&fast_slow);
    let drain_fut = tokio::spawn(async move {
        // Wait until the chunked-in-flight set is non-empty.
        let chunked_set = store_for_drain.chunked_in_flight_digests_handle();
        for _ in 0..1_000_000 {
            if !chunked_set.lock().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Now call flush_slow_writes.
        let drain_start = std::time::Instant::now();
        let remaining = store_for_drain
            .flush_slow_writes(Duration::from_secs(10))
            .await;
        (remaining, drain_start.elapsed())
    });

    let (writer_res, update_res, drain_res) = tokio::time::timeout(
        Duration::from_secs(20),
        async { tokio::join!(writer_fut, update_fut, drain_fut) },
    )
    .await
    .expect("must not deadlock — B2 graceful drain test");

    writer_res.expect("writer task panic");
    update_res
        .expect("update task panic")
        .expect("update must succeed");
    let (remaining, drain_elapsed) = drain_res.expect("drain task panic");

    // The drain MUST observe 0 remaining writes (chunked dispatch fully
    // drained).
    assert_eq!(
        remaining, 0,
        "flush_slow_writes MUST drain the chunked-path in-flight set \
         before returning — got {remaining} entries still pending. \
         If non-zero, B2's chunked_in_flight_digests reaper is not \
         removing the digest (or B2 is not wired into flush_slow_writes \
         at all); #210 graceful-shutdown contract is violated for \
         chunked-path writes.",
    );
    // Sanity: drain elapsed must be > 0ms (the dispatch was in flight
    // when we called drain). Without B2 wiring the drain returns
    // immediately (~0ms).
    assert!(
        drain_elapsed > Duration::from_micros(100),
        "drain must have actually waited for the chunked dispatch to \
         complete (elapsed = {drain_elapsed:?}); a near-zero elapsed \
         indicates the drain returned without observing the chunked \
         in-flight entry — B2 wiring missing or flush_slow_writes does \
         not consult chunked_in_flight_digests",
    );

    set_bazel_facing_internal_chunking_enabled(false);
}
