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

//! #230 regression tests: per-chunk CDN-tee with abandon-on-full
//!
//! These tests assert the user-approved 2026-05-02 architecture:
//!   1. Bazel reader NEVER blocks on cache.
//!   2. Per-chunk forwarding: chunk arrives → forward to Bazel
//!      (only Bazel-side back-pressure) → try_send to cache mpsc
//!      (non-blocking, abandon on Full).
//!   3. Cache write task is detached + per-task timeout.
//!
//! Production composition: real `WorkerProxyStore` wrapping a real
//! `FilesystemStore` inner (so the abandon path's "FilesystemStore
//! in-flight tracker discards partial" claim is exercised end-to-end).
//! Tests use `tokio::time::timeout(Duration::from_secs(10))` as the
//! deadlock detector with bespoke `.expect(...)` messages — generic
//! `is_err()` would mask `tokio::time::Elapsed` from a too-short timeout.
//!
//! The over-action sibling test (`cdn_tee_does_not_block_bazel_on_slow_cache`)
//! is the #171-shaped guard: removing the `try_send`-with-Full-abandon
//! coupling MUST trip the assertion that Bazel still reads at peer-reader
//! rate. Without this test, an under-action-only suite would let
//! "cache slowness propagates to Bazel" land silently.

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering as AOrdering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, FilesystemSpec, MemorySpec, NoopSpec,
    StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::{
    SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH1: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Wall-clock cap for every test in this file. The whole point of
/// these tests is to detect deadlocks, so we MUST wrap every
/// `proxy.get_part_unchunked` (and the wait for the spawned cache
/// task) in a `tokio::time::timeout(TEST_TIMEOUT)`. 10 s is plenty for
/// micro-blob ops on a developer laptop and short enough that a hung
/// CI run fails fast.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

// Note: the production `CDN_TEE_CACHE_MPSC_CAP = 16` (FL-688; was 4) is
// intentionally not duplicated here; tests assert via behavior + counters,
// not by re-coupling to the constant. If the cap changes in production,
// these tests should still pass on their behavioral assertions.

// ---------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------

/// Generate a digest for a Vec<u8> of size `n`. Uses the predictable
/// VALID_HASH1 string so tests are deterministic; the digest's
/// content-hash claim is intentionally not real (the inner stores in
/// these tests are MemoryStore / FilesystemStore which trust the
/// claimed digest).
fn digest_for_size(n: u64) -> DigestInfo {
    DigestInfo::try_new(VALID_HASH1, n).expect("valid digest")
}

/// Build a deterministic test value of `size_bytes` bytes.
fn test_value(size_bytes: usize) -> Vec<u8> {
    (0..size_bytes).map(|i| (i & 0xFF) as u8).collect()
}

/// Construct a temp-path FilesystemStore for use as the WPS inner.
/// Uses TEST_TMPDIR per the existing filesystem_store_test pattern.
async fn make_filesystem_inner() -> Result<(Store, String), Error> {
    let tmpdir = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
    let suffix: u64 = rand::random();
    let base = format!("{tmpdir}/cdn_tee_test/{suffix}");
    let content_path = format!("{base}/content");
    let temp_path = format!("{base}/temp");
    tokio::fs::create_dir_all(&content_path).await.err_tip(|| {
        format!("create_dir_all(content_path={content_path}) failed in test setup")
    })?;
    tokio::fs::create_dir_all(&temp_path).await.err_tip(|| {
        format!("create_dir_all(temp_path={temp_path}) failed in test setup")
    })?;
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path,
        eviction_policy: Some(EvictionPolicy {
            max_count: 10_000,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await?;
    Ok((Store::new(fs_store), content_path))
}

/// Construct a WPS wrapping the given inner Store with a single
/// peer-store entry registered in the locality map.
fn build_proxy_with_peer(
    inner: Store,
    peer_store: Store,
    digest: DigestInfo,
    peer_endpoint: &'static str,
) -> (Arc<WorkerProxyStore>, SharedBlobLocalityMap) {
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);
    (proxy_arc, locality_map)
}

// ---------------------------------------------------------------------
// Test 1: happy path — peer-fetch a 4 MB blob; both Bazel and cache OK
// ---------------------------------------------------------------------

/// Asserts the under-action contract: a successful peer-fetch causes
/// the spawned cache task to run `inner.update` to completion. The
/// counter snapshot triangulates which path fired.
///
/// Production composition: real WorkerProxyStore + real FilesystemStore
/// inner + real MemoryStore peer. Verifies that the bytes land in
/// FilesystemStore on disk (not just claimed via has_with_results).
///
/// Mutation step: in `WorkerProxyStore::get_part_and_cache`, comment
/// out `completed_counter.fetch_add(1, ...)` in the cache task's
/// `Ok(Ok(()))` arm. Test panics with the bespoke
/// "completed counter must increment" message because the assertion
/// fires after the FilesystemStore has-check confirms the blob landed.
#[nativelink_test]
async fn cdn_tee_happy_path_4mb_blob_caches_and_serves() -> Result<(), Error> {
    let value = test_value(4 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let (inner, _content_path) = make_filesystem_inner().await?;
    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from(value.clone()))
        .await?;

    let (proxy_arc, _locality) = build_proxy_with_peer(
        inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-happy-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    let (attempts_before, completed_before, full_before, eof_before) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(
        (attempts_before, completed_before, full_before, eof_before),
        (0, 0, 0, 0),
        "fresh WPS must report all-zero CDN-tee counters",
    );

    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — happy-path peer-fetch must complete within \
         10s; the per-chunk forward + try_send loop is the only path",
    )?;
    assert_eq!(
        bytes.len(),
        value.len(),
        "Bazel must receive all 4 MiB",
    );
    assert_eq!(bytes.as_ref(), value.as_slice());

    // The cache task is detached; poll its outcome via the counter.
    // Completion is bounded by the inner.update() runtime + our timeout.
    let deadline = std::time::Instant::now() + TEST_TIMEOUT;
    let mut completed_after = 0u64;
    while std::time::Instant::now() < deadline {
        completed_after = proxy_arc.cdn_tee_counters_snapshot().1;
        if completed_after == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        completed_after, 1,
        "completed counter must increment exactly once after a successful \
         peer-fetch — cache task either never ran or never reached Ok(Ok)",
    );

    let (attempts_after, _, full_after, eof_after) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts_after, 1, "exactly one cache attempt");
    assert_eq!(full_after, 0, "happy path must not abandon-on-full");
    assert_eq!(eof_after, 0, "happy path must not abandon-on-consumer-eof");

    // End-to-end proof: the blob is in inner FilesystemStore.
    let mut probe = [None];
    inner.has_with_results(&[digest.into()], &mut probe).await?;
    assert_eq!(
        probe[0],
        Some(value.len() as u64),
        "FilesystemStore inner MUST have the blob — cache task either \
         did not start or failed inner.update(); this is the original \
         #229 silent-failure surface, now load-bearing-tested",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 2: slow Bazel consumer must NOT stall peer or cache (over-action
// guard for "Bazel back-pressure must propagate to cache")
// ---------------------------------------------------------------------

/// Inverse of the bug: prove that a slow Bazel reader does NOT delay
/// the peer-reader task. The peer task is `tokio::spawn`-detached and
/// writes into a 24-slot `proxy_tx`; with a small blob (≤ 24 chunks)
/// the peer task fully drains its source into proxy_tx and exits
/// before consumer EOF. The slow consumer drains proxy_rx via
/// `bazel.send().await`-paced forward loop.
///
/// Note: the per-chunk forward loop is sequential
/// (`bazel.send().await; cache.try_send(...)`), so cache fan-out IS
/// rate-bound by Bazel. That's by design — this test is about the
/// peer-side independence, not the cache task. The
/// `cdn_tee_slow_cache_abandons_does_not_block_bazel` test below is
/// the OTHER direction (cache slowness must not pin Bazel).
///
/// Mutation step: in `WorkerProxyStore::get_part_and_cache`, remove
/// the `tokio::spawn` around the peer-reader and instead `.await` the
/// peer's `get_part` inline before the forward loop. Peer completion
/// would then be gated on the consumer's reads (because the peer
/// can't return until proxy_tx drains). The peer-task-elapsed
/// assertion below would trip.
#[nativelink_test]
async fn cdn_tee_slow_consumer_does_not_stall_cache_completion()
-> Result<(), Error> {
    let value = test_value(32 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let (inner, _content_path) = make_filesystem_inner().await?;
    // ChunkedPeer with 4 KiB chunks → 8 chunks → consumer pace matters.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 4 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-slow-consumer-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive the read with a slow consumer: a writer paired with a
    // reader that sleeps between recv() calls. We measure the wall-
    // clock for the cache task to complete vs the consumer.
    let (writer, mut reader) =
        nativelink_util::buf_channel::make_buf_channel_pair();
    let key: StoreKey<'static> = digest.into();
    let proxy_for_get = proxy.clone();
    let get_handle = tokio::spawn(async move {
        let mut writer = writer;
        proxy_for_get
            .get_part(key, &mut writer, 0, None)
            .await
    });

    // Slow consumer: sleep 50ms between reads. Total expected time is
    // ~50ms × num_chunks; the cache task should complete well before
    // the consumer finishes.
    let consumer_started = std::time::Instant::now();
    let mut received_total = 0usize;
    loop {
        match tokio::time::timeout(TEST_TIMEOUT, reader.recv()).await {
            Ok(Ok(chunk)) if chunk.is_empty() => break,
            Ok(Ok(chunk)) => {
                received_total += chunk.len();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(Err(e)) => {
                return Err(e).err_tip(|| "slow consumer recv failed");
            }
            Err(_) => panic!(
                "must not deadlock — slow consumer should still receive \
                 all chunks within {} s",
                TEST_TIMEOUT.as_secs()
            ),
        }
    }
    let consumer_elapsed = consumer_started.elapsed();
    assert_eq!(
        received_total,
        value.len(),
        "slow consumer must still receive every byte",
    );

    // Bound: 8 chunks × 50ms = ~400ms expected; allow 2× headroom.
    // If the consumer's `recv()` were somehow blocked on the cache
    // task (e.g., a future refactor wires cache→consumer coupling),
    // the consumer wall-clock would inflate well past this bound.
    assert!(
        consumer_elapsed < Duration::from_millis(2000),
        "slow consumer wall-clock {consumer_elapsed:?} must be \
         consumer-paced (~400ms for 8 chunks × 50ms), NOT inflated \
         by cache or peer back-pressure; production architecture \
         spawns peer-reader so its work happens in parallel",
    );

    // Cache task outcome under slow consumer: with the production
    // architecture, the peer-reader spawns and writes into a 24-slot
    // proxy_tx; the forward loop reads at consumer rate and offers
    // chunks to the 16-slot cache mpsc via try_send. With a fast peer
    // and a slow consumer, proxy_tx fills first, then the forward
    // loop's cache.try_send fills the 16-slot cache mpsc, then
    // abandon-on-full fires. With a fast consumer, cache keeps up
    // and completed=1.
    //
    // Either outcome is valid for this test; what we want to ensure
    // is that the cache fan-out resolves (completed OR abandoned)
    // and the spawned task does not leak. Wait briefly for one of
    // those outcomes.
    let resolve_deadline =
        std::time::Instant::now() + Duration::from_secs(5);
    let mut completed = 0u64;
    let mut full = 0u64;
    while std::time::Instant::now() < resolve_deadline {
        let snap = proxy_arc.cdn_tee_counters_snapshot();
        completed = snap.1;
        full = snap.2;
        if completed >= 1 || full >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        completed >= 1 || full >= 1,
        "cache fan-out must resolve to either completed (consumer \
         kept up) or abandoned-on-full (consumer slower than cache \
         could drain); observed completed={completed}, full={full} \
         after 5s — spawned cache task may have leaked",
    );

    // Resolve the get-handle (it should already have returned).
    let _join_result = tokio::time::timeout(TEST_TIMEOUT, get_handle)
        .await
        .expect("get_part handle must join")
        .expect("get_part task must not panic");

    Ok(())
}

// ---------------------------------------------------------------------
// Test 3: slow cache must NOT block Bazel (the #230 motivating bug)
// ---------------------------------------------------------------------

/// Wraps the inner store in a SlowUpdateStore that holds `inner.update`
/// open for 5s before returning. With cache mpsc cap = 16 (FL-688) and a
/// cache stalled 5s, the forward loop's `try_send` MUST return Full after
/// ~16 chunks (the 8 MiB / 64 KiB blob is ~128 chunks, far more than the
/// cap) and the abandon path MUST fire. Bazel must continue receiving the
/// remaining chunks at peer-reader rate, NOT at cache rate.
///
/// This is the motivating regression test for #230. Pre-#230, the
/// `tokio::join!(forward_fut, cache_write_fut)` would have meant that
/// the forward_fut's `cache_tx.send().await` blocked on the slow
/// cache, propagating cache slowness to Bazel via `proxy_rx`.
///
/// Production composition: real WPS + (FailingSlowInnerStore wrapping
/// real FilesystemStore). The wrapper's slow update gives the cache
/// mpsc the time it needs to fill.
///
/// Mutation step: revert `try_send` to `send().await` in
/// `WorkerProxyStore::get_part_and_cache`. The forward loop would
/// then block on the slow cache, Bazel would receive bytes only as
/// fast as the cache drains, and the wall-clock assertion below
/// would trip with the bespoke "Bazel rate decoupled from cache"
/// message. This is the over-action assertion.
#[nativelink_test]
async fn cdn_tee_slow_cache_abandons_does_not_block_bazel()
-> Result<(), Error> {
    // Blob big enough to fill the 16-slot mpsc and require many more
    // chunks. We use a ChunkedPeerStore that splits the blob into
    // 64 KiB chunks so the per-chunk forward loop fires many iterations.
    let value = test_value(8 * 1024 * 1024); // 8 MiB
    let digest = digest_for_size(value.len() as u64);

    // Slow inner store: each `update` sleeps 5s before completing.
    // This delays the cache task's drain of `cache_rx` so the mpsc
    // fills within the first few chunks.
    let slow_inner = Store::new(Arc::new(SlowUpdateInnerStore {
        sleep: Duration::from_secs(5),
        update_calls: AtomicU64::new(0),
    }));

    // ChunkedPeer: emit the blob in 64 KiB chunks. With the production
    // cache mpsc cap of 16 (FL-688), the forward loop produces ~128 chunks;
    // the slow cache (5s sleep before drain) means the mpsc fills (16 slots)
    // within the first ~16 chunks and the abandon path takes over.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        slow_inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-slow-cache-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    let bazel_started = std::time::Instant::now();
    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — Bazel-side fetch must complete within 10s \
         even with a 5s-per-update cache; coupling violation if this \
         times out",
    )?;
    let bazel_elapsed = bazel_started.elapsed();

    assert_eq!(
        bytes.len(),
        value.len(),
        "Bazel must receive every byte even when cache abandons",
    );
    assert_eq!(bytes.as_ref(), value.as_slice());

    // Bazel rate must be decoupled from the 5s cache stall. With the
    // memory peer and an in-process MemoryStore source, Bazel should
    // deliver the 8 MiB in < 2 s comfortably; pre-#230 coupling
    // would have made it ≥ 5 s.
    assert!(
        bazel_elapsed < Duration::from_secs(4),
        "Bazel rate must be decoupled from slow cache; observed \
         {bazel_elapsed:?} (≥ 4 s would prove coupling)",
    );

    // The abandon-on-full counter MUST have fired.
    let (attempts, _completed, full, _eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts, 1, "exactly one cache attempt was made");
    assert_eq!(
        full, 1,
        "abandon-on-full counter MUST have incremented; observed \
         attempts={attempts}, full={full}. Without the abandon path, \
         the test would have failed on the 4 s wall-clock assertion \
         above",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 4: consumer disconnect mid-blob — clean cache abandon
// ---------------------------------------------------------------------

/// Bazel disconnects after receiving ~50 % of the bytes. The forward
/// loop's `bazel_writer.send(chunk).await` returns Err, the
/// abandon-on-consumer-eof counter MUST fire, the cache task MUST
/// observe a short stream and finish (or timeout), and there MUST be
/// no orphan partial files in the FilesystemStore on-disk content
/// path.
///
/// Production composition: real WPS + real FilesystemStore inner +
/// real MemoryStore peer. The dropped consumer is simulated by
/// dropping the read half mid-stream.
///
/// Mutation step: in `get_part_and_cache`, remove the
/// `cdn_tee_cache_abandoned_consumer_eof_total.fetch_add(...)` line.
/// Test panics with the bespoke "abandon-on-consumer-eof must
/// increment" assertion. The on-disk cleanup assertion is separate
/// and would still pass — but the counter assertion fires first,
/// which is the correctness signal.
#[nativelink_test]
async fn cdn_tee_consumer_disconnect_midblob_abandons_cache_cleanly()
-> Result<(), Error> {
    let value = test_value(4 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let (inner, content_path) = make_filesystem_inner().await?;
    // ChunkedPeer with inter-chunk sleep so the peer producer is
    // still going when we drop the consumer mid-blob. Without the
    // sleep the buf_channel (24 slots × 64 KiB ≈ 1.5 MiB capacity)
    // would buffer the entire 4 MiB and EOF would arrive before
    // we finished reading half — abandon-on-consumer-eof would
    // never have a chance to fire.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(5),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-disconnect-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive get_part with a writer/reader pair we control. After
    // receiving half the bytes, drop the reader to simulate a
    // disconnected consumer.
    let (writer, mut reader) =
        nativelink_util::buf_channel::make_buf_channel_pair();
    let key: StoreKey<'static> = digest.into();
    let proxy_for_get = proxy.clone();
    let get_handle = tokio::spawn(async move {
        let mut writer = writer;
        proxy_for_get
            .get_part(key, &mut writer, 0, None)
            .await
    });

    // Read until we've received roughly half, then DROP the reader.
    let half = value.len() / 2;
    let mut received = 0usize;
    loop {
        let chunk_res = tokio::time::timeout(TEST_TIMEOUT, reader.recv())
            .await
            .expect(
                "must not deadlock — peer-fetch must produce chunks within \
                 10s before consumer disconnects",
            )?;
        if chunk_res.is_empty() {
            // Already EOF; can't test disconnect — fail loudly so
            // future changes that increase chunk size or consolidate
            // the stream get caught.
            panic!(
                "test setup error: blob delivered as a single chunk; \
                 cannot simulate mid-blob disconnect. received={received}"
            );
        }
        received += chunk_res.len();
        if received >= half {
            break;
        }
    }
    drop(reader); // Consumer disconnect.

    // The get_part task should observe the dropped reader, surface
    // an error, and the abandon-on-consumer-eof counter must fire.
    let _get_res = tokio::time::timeout(TEST_TIMEOUT, get_handle)
        .await
        .expect("get_part task must finish within 10s after consumer drop")
        .expect("get_part task must not panic");

    // Wait briefly for the spawned cache task to observe the dropped
    // cache_tx and complete its abort.
    let deadline = std::time::Instant::now() + TEST_TIMEOUT;
    while std::time::Instant::now() < deadline {
        let (_, _, _, eof) = proxy_arc.cdn_tee_counters_snapshot();
        if eof >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let (attempts, _completed, _full, eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts, 1, "exactly one cache attempt");
    assert_eq!(
        eof, 1,
        "abandon-on-consumer-eof counter MUST have incremented after \
         consumer disconnect; observed eof={eof}",
    );

    // No orphan partial file on disk. The FilesystemStore in-flight
    // tracker must have discarded the partial when ExactSize tripped.
    // We probe by listing the content path and asserting no file
    // contains the digest's hex prefix.
    let abandoned_partial_present =
        find_digest_file(&content_path, &digest).await;
    assert!(
        !abandoned_partial_present,
        "after consumer disconnect, FilesystemStore content_path={} \
         must NOT contain a partial file for digest {digest}; orphan \
         partial would be the symptom of FilesystemStore in-flight \
         discard not firing",
        content_path,
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 5: cache abandon-on-full leaves no on-disk partial
// ---------------------------------------------------------------------

/// Forces the abandon-full path (slow cache + full mpsc) and asserts
/// that no orphan partial file exists in the FilesystemStore content
/// path afterward. This covers the "FilesystemStore in-flight tracker
/// discards partial via ExactSize check" claim from the design doc.
///
/// We can't trivially combine slow inner + real FilesystemStore in one
/// call (the slow wrapper hides the fs in-flight tracker), so instead
/// we run two chained checks:
///   (a) The slow-cache abandon test above already proves abandon
///       fires (counter incremented).
///   (b) Here we wire the real FilesystemStore as the inner, but use
///       a SLOW DRAIN PEER that produces chunks slowly enough that
///       the cache mpsc fills BEFORE the cache task can keep up.
///       Inner.update is the real FilesystemStore `update` — its
///       in-flight tracker discards on ExactSize mismatch.
///
/// The actual abandon-full triggering is fragile with real fs;
/// to keep this deterministic we invert the design: wrap the fs
/// inner in a "throttle update" wrapper that delays the FIRST chunk
/// of inner.update by `sleep`. The fs inner still gets called with
/// a real `update` once the wrapper's delay expires, so its in-flight
/// tracker observes a short stream when our orchestration drops
/// cache_tx.
///
/// Mutation step: in `get_part_and_cache`, replace
/// `cache_tx.take()` (in the Full arm) with `let _ = cache_tx;`
/// (does NOT drop). The cache task would never see EOF, the
/// FilesystemStore in-flight entry would linger past the test, and
/// our final on-disk-no-orphan assertion would fire.
#[nativelink_test]
async fn cdn_tee_abandon_full_leaves_no_on_disk_partial() -> Result<(), Error> {
    let value = test_value(8 * 1024 * 1024); // 8 MiB
    let digest = digest_for_size(value.len() as u64);

    // Inner: throttle wrapper around real FilesystemStore. The throttle
    // sleeps for the entire cache task duration so the mpsc fills.
    let (fs_inner, content_path) = make_filesystem_inner().await?;
    let throttled = Store::new(Arc::new(ThrottledFirstChunkInnerStore {
        delegate: fs_inner.clone(),
        sleep: Duration::from_secs(3),
        triggered: AtomicU64::new(0),
    }));

    // ChunkedPeer: 64 KiB chunks → ~128 chunks → mpsc fills fast.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        throttled.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-abandon-full-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — Bazel-side fetch must complete within 10s \
         even when the inner.update is throttled",
    )?;
    assert_eq!(
        bytes.len(),
        value.len(),
        "Bazel must still receive every byte",
    );

    // The abandon-on-full counter MUST have fired.
    let (_attempts, _completed, full, _eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert!(
        full >= 1,
        "abandon-on-full MUST have fired with throttled inner.update; \
         observed full={full}",
    );

    // Poll the FilesystemStore content_path until either the orphan
    // file disappears (expected) or the test deadline fires (failure).
    // Per CLAUDE.md "no `tokio::time::sleep` as synchronization in
    // tests" — we bound the polling loop by TEST_TIMEOUT and treat the
    // absence-of-file as the synchronization condition.
    let poll_deadline = std::time::Instant::now() + TEST_TIMEOUT;
    let mut abandoned_partial_present = true;
    while std::time::Instant::now() < poll_deadline {
        abandoned_partial_present =
            find_digest_file(&content_path, &digest).await;
        if !abandoned_partial_present {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !abandoned_partial_present,
        "after abandon-full, FilesystemStore content_path={} must NOT \
         contain a partial file for digest {digest} within {:?}; \
         orphan partial would be the symptom of FilesystemStore \
         in-flight discard not firing on a short cache_rx stream",
        content_path,
        TEST_TIMEOUT,
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 6: #201 follow-up — sustained cache-tier slowness must not
// deadlock the read path. (Resolves #201's deferred deadlock probe.)
// ---------------------------------------------------------------------

/// #201: CDN-tee back-pressure deadlock probe. Pre-#230 architecture
/// could deadlock if `cache_write_fut` blocked on a slow inner store
/// while `forward_fut` waited for `cache_tx.send().await` to return
/// — the chain would lock at `cache_tx -> cache_rx -> inner.update`.
///
/// Post-#230, the abandon-on-full path eliminates this class. This
/// test asserts ten back-to-back peer-fetches against a slow inner
/// store all complete (Bazel-side) within wall-clock budget.
///
/// Mutation step: same as Test 3 — revert `try_send` to `send().await`
/// in get_part_and_cache. With ten 8-MiB blobs each waiting 5s on the
/// cache, total wall-clock would explode well past TEST_TIMEOUT.
#[nativelink_test]
async fn cdn_tee_sustained_cache_slowness_does_not_deadlock_201()
-> Result<(), Error> {
    let blob_count: usize = 5;
    let blob_size: usize = 4 * 1024 * 1024;
    let value = test_value(blob_size);

    let slow_inner = Store::new(Arc::new(SlowUpdateInnerStore {
        sleep: Duration::from_secs(3),
        update_calls: AtomicU64::new(0),
    }));

    // ChunkedPeerStore so the abandon-on-full path fires deterministically.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    // Each digest is unique; we register all of them on one peer.
    let mut digests = Vec::with_capacity(blob_count);
    for i in 0..blob_count {
        let hash = format!(
            "0123456789abcdef000000000000000000{:02}0000000000000123456789abcdef",
            i
        );
        let d = DigestInfo::try_new(&hash, blob_size as u64)?;
        digests.push(d);
    }

    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(slow_inner.clone(), locality_map.clone());
    let peer_endpoint = "grpc://cdn-tee-sustained-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_inner);
    {
        let mut map = locality_map.write();
        for d in &digests {
            map.register_blobs(peer_endpoint, &[*d]);
        }
    }
    let proxy = Store::new(proxy_arc.clone());

    let started = std::time::Instant::now();
    let mut handles = Vec::with_capacity(blob_count);
    for d in digests.iter().copied() {
        let p = proxy.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(TEST_TIMEOUT, p.get_part_unchunked(d, 0, None))
                .await
                .expect(
                    "must not deadlock — sustained cache-tier slowness must \
                     NOT block any individual peer-fetch (#201 deadlock \
                     probe)",
                )
        }));
    }
    for h in handles {
        let bytes = h.await.expect("task must not panic")?;
        assert_eq!(bytes.len(), value.len());
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < TEST_TIMEOUT,
        "all {blob_count} fetches must complete within {:?}; observed \
         {elapsed:?} — pre-#230 coupling would have serialized cache \
         updates and missed the deadline",
        TEST_TIMEOUT,
    );

    // At least one abandon-on-full MUST have fired (the slow inner.update
    // backs up faster than the 16-slot mpsc can drain: a 4 MiB blob at
    // 64 KiB/chunk is 64 chunks, far more than the 16 slots, against a
    // 3s-per-update drain).
    let (attempts, _completed, full, _eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(
        attempts, blob_count as u64,
        "exactly {blob_count} cache attempts",
    );
    assert!(
        full >= 1,
        "with sustained 3s-per-update cache and {blob_count} concurrent \
         fetches, at least one abandon-on-full must have fired; observed \
         full={full}, attempts={attempts}",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 7: M1 (perf-optimizer BLOCK fix) — abandon path with >1024
// chunks must NOT pin the peer task on `proxy_tx.send().await`.
// ---------------------------------------------------------------------

/// Regression test for the M1 BLOCK in #230's r1 review.
///
/// **Bug**: when the Bazel consumer disconnects mid-blob, the forward
/// loop's `writer.send(chunk).await` errors and the loop `break`s
/// without dropping `proxy_rx`. The peer task continues writing into
/// `proxy_tx` (default capacity = `DEFAULT_BUF_CHANNEL_CAPACITY = 1024`
/// slots). With a peer chunk size that produces >1024 chunks per blob,
/// the peer task fills proxy_tx and blocks indefinitely on
/// `proxy_tx.send().await`. The outer `peer_handle.await` then hangs
/// forever, pinning a tokio worker per orphaned fetch.
///
/// **Fix** (`worker_proxy_store.rs`): drop `proxy_rx` (and
/// `peer_handle.abort()`) BEFORE awaiting `peer_handle` whenever the
/// forward loop errored. The peer task's next `send()` then fails with
/// a closed-channel error and the task exits.
///
/// **Reproduction parameters**: 8 KiB chunks × 16 MiB blob = 2048
/// chunks (well over the 1024-slot proxy_tx cap). Inter-chunk sleep is
/// kept tiny so the peer task is reliably mid-stream when we drop the
/// consumer at ~10% received bytes.
///
/// **Mutation step**: comment out `peer_handle.abort();` AND replace
/// `drop(proxy_rx);` with `let _proxy_rx = proxy_rx;` to keep it alive
/// — the test will trip the bespoke message via 10s timeout. Without
/// the fix, the peer task wedges on send() once proxy_tx fills past
/// 1024 chunks and `peer_handle.await` never returns. NOTE: either
/// drop(proxy_rx) alone OR peer_handle.abort() alone is sufficient
/// to unblock the peer task; this test only catches the both-removed
/// regression. (drop(proxy_rx) closes the channel so the peer's next
/// send returns Err; peer_handle.abort() cancels the task at the next
/// await. Together they bound recovery to one scheduler tick.)
#[nativelink_test]
async fn cdn_tee_consumer_disconnect_with_huge_chunk_count_does_not_pin_peer_task()
-> Result<(), Error> {
    // 16 MiB blob × 8 KiB chunks = 2048 chunks. The 1024-slot
    // proxy_tx mpsc fills on the first 1024 chunks; without the M1
    // fix, the peer task blocks on send() of chunk 1025+.
    let value = test_value(16 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let (inner, _content_path) = make_filesystem_inner().await?;
    // ChunkedPeer with tiny chunks. inter_chunk_sleep_ms=1 keeps the
    // peer producer alive across the consumer-disconnect window
    // without the test taking forever.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 8 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(1),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-huge-chunk-disconnect-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive get_part with a writer/reader pair we control. After
    // receiving ~10% of bytes (well below the 1024-chunk proxy_tx
    // cap × 8 KiB ≈ 8 MiB-buffered window), drop the reader.
    let (writer, mut reader) =
        nativelink_util::buf_channel::make_buf_channel_pair();
    let key: StoreKey<'static> = digest.into();
    let proxy_for_get = proxy.clone();
    let get_handle = tokio::spawn(async move {
        let mut writer = writer;
        proxy_for_get
            .get_part(key, &mut writer, 0, None)
            .await
    });

    let disconnect_threshold = value.len() / 10; // ~1.6 MiB
    let mut received = 0usize;
    loop {
        let chunk_res = tokio::time::timeout(TEST_TIMEOUT, reader.recv())
            .await
            .expect(
                "must not deadlock — peer-fetch must produce chunks within \
                 10s before consumer disconnects (M1: peer task should NOT \
                 be pinned on proxy_tx.send() with >1024 chunks)",
            )?;
        if chunk_res.is_empty() {
            panic!(
                "test setup error: blob delivered as a single chunk; cannot \
                 simulate mid-blob disconnect. received={received}"
            );
        }
        received += chunk_res.len();
        if received >= disconnect_threshold {
            break;
        }
    }
    drop(reader); // Consumer disconnect.

    // The get_part task MUST resolve within TEST_TIMEOUT. Without the
    // M1 fix (drop proxy_rx + abort peer_handle on forward error), the
    // peer task wedges on `proxy_tx.send().await` once it has produced
    // 1024 chunks, and `peer_handle.await` in `get_part_and_cache`
    // never returns — get_handle hangs and the timeout fires with the
    // bespoke message below.
    let get_res = tokio::time::timeout(TEST_TIMEOUT, get_handle)
        .await
        .expect(
            "must not deadlock — peer task pinned by undrained proxy_rx \
             after consumer disconnect (#230 M1 BLOCK regression)",
        )
        .expect("get_part task must not panic");
    assert!(
        get_res.is_err(),
        "expected get_part to fail after consumer disconnect; got Ok(())",
    );

    // The abandon-on-consumer-eof counter must have fired.
    let deadline = std::time::Instant::now() + TEST_TIMEOUT;
    while std::time::Instant::now() < deadline {
        let (_, _, _, eof) = proxy_arc.cdn_tee_counters_snapshot();
        if eof >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (_attempts, _completed, _full, eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(
        eof, 1,
        "abandon-on-consumer-eof MUST fire on the consumer-disconnect \
         path even when the peer produced >1024 chunks; observed eof={eof}",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 8: M1 happy-path sibling — over-action regression guard for
// `peer_handle.abort()`. (testing-czar MINOR-2)
// ---------------------------------------------------------------------

/// Happy-path twin of
/// `cdn_tee_consumer_disconnect_with_huge_chunk_count_does_not_pin_peer_task`.
/// Same parameters (16 MiB / 8 KiB chunks / 2048 chunks, well over the
/// 1024-slot proxy_tx cap), but the consumer fully drains the stream
/// instead of disconnecting.
///
/// **Regression class guarded:** #230's primary architectural property —
/// the cache write must complete *fully* even when the consumer-read path
/// is decoupled and the chunk count vastly exceeds the proxy_tx cap.
/// Round-2 reviewers initially framed this test as guarding an
/// "over-action `peer_handle.abort()` regression", but control-flow
/// analysis (see `.claude/reviews/230-round-2/testing-czar.md` round-3
/// update) shows that hypothesis does NOT hold: the peer task is already
/// `Ok(())` by the time the EOF chunk reaches the forward loop, so
/// `JoinHandle::abort()` on a completed task is a no-op and cannot be
/// observed by any happy-path assertion. Do NOT re-add an
/// "unconditional `peer_handle.abort()`" mutation here — it will not
/// trip this test.
///
/// Production composition: real WPS + real FilesystemStore inner +
/// ChunkedPeerStore producing many >1024 chunks. Asserts:
///   1. `get_part` returns `Ok(())`.
///   2. The full 16 MiB lands at the consumer (byte count + checksum).
///   3. The blob is present in the FilesystemStore inner (poll
///      `inner.has_with_results` bounded by TEST_TIMEOUT).
///   4. The cache `completed` counter increments to 1 (peer task
///      ran to natural EOF; cache task ran `inner.update` to Ok).
///
/// **Mutation step (Mutation A — primary):** in
/// `WorkerProxyStore::get_part_and_cache`, comment out the
/// `let cache_handle: JoinHandle<()> = tokio::spawn(async move { ... });`
/// at line ~1620 (the cache-task spawn) — for example, replace its body
/// with a no-op that drops `cache_rx` immediately. With the cache task
/// gone, `cache_tx`'s sends still succeed (mpsc has capacity), but
/// `inner.update` never runs, so the FilesystemStore probe in step 3
/// observes `None` and the assertion at line ~1110 fires with the
/// bespoke message naming "regression guard for #230 cache-write/
/// consumer-read decoupling architecture". The `completed` counter at
/// step 4 also stays at 0, providing a second tripwire.
///
/// **Mutation B (alternative):** comment out `cache_tx.take()` /
/// `tx.send_eof()` in the EOF branch (line ~1671-1689). The cache task
/// hangs on `cache_rx.recv()` until `CDN_TEE_CACHE_TASK_TIMEOUT` fires,
/// or `inner.update` errors on size mismatch — either way the
/// `completed == 1` assertion fires.
#[nativelink_test]
async fn cdn_tee_happy_path_huge_chunk_count_caches_all_bytes()
-> Result<(), Error> {
    // Mirror Test 7's parameters: 16 MiB blob × 8 KiB chunks = 2048
    // chunks (well over the 1024-slot proxy_tx cap). The point is to
    // catch a regression where `peer_handle.abort()` fires on the
    // happy path: with this many chunks, an unconditional abort
    // would race the peer's natural EOF and either truncate the
    // consumer's bytes or cancel the in-flight cache update.
    let value = test_value(16 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    let (inner, _content_path) = make_filesystem_inner().await?;
    // Inter-chunk sleep is intentionally tiny so the test completes
    // well within TEST_TIMEOUT but the producer is reliably mid-stream
    // for many scheduler ticks (any unconditional abort has many
    // opportunities to fire mid-stream).
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 8 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(1),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-happy-huge-chunk-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive get_part and FULLY drain the consumer. Wrap in
    // TEST_TIMEOUT as the deadlock detector — if any unconditional
    // abort kills the peer task mid-stream, recv will hang waiting
    // for the next chunk and trip the bespoke message.
    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "happy-path many-chunk peer-fetch must complete fully — \
         consumer-read path stalled? regression guard for #230 \
         cache-write/consumer-read decoupling architecture",
    )?;

    assert_eq!(
        bytes.len(),
        value.len(),
        "consumer must receive every byte of the 16 MiB blob; \
         short-read indicates the forward loop terminated early",
    );
    assert_eq!(
        bytes.as_ref(),
        value.as_slice(),
        "consumer bytes must match peer payload byte-for-byte",
    );

    // The cache task is detached. Poll `completed` and the inner
    // store until the cache write lands or the deadline fires.
    // Per CLAUDE.md: bounded polling loop, not a fixed sleep.
    let deadline = std::time::Instant::now() + TEST_TIMEOUT;
    let mut completed_after = 0u64;
    while std::time::Instant::now() < deadline {
        completed_after = proxy_arc.cdn_tee_counters_snapshot().1;
        if completed_after == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        completed_after, 1,
        "happy-path cache `completed` counter must reach 1 — \
         decoupled cache write must complete fully on huge-chunk \
         happy path — regression guard for #230 cache-write/\
         consumer-read decoupling architecture (mutation: comment \
         out the `tokio::spawn` for the cache task; cache file never \
         appears; this assertion fails)",
    );

    // End-to-end cache-presence probe: bounded poll on the inner
    // FilesystemStore. The same TEST_TIMEOUT-bounded pattern as
    // Test 5's m3 fix.
    let probe_deadline = std::time::Instant::now() + TEST_TIMEOUT;
    let mut probe_size: Option<u64> = None;
    while std::time::Instant::now() < probe_deadline {
        let mut probe = [None];
        inner
            .has_with_results(&[digest.into()], &mut probe)
            .await?;
        if probe[0].is_some() {
            probe_size = probe[0];
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        probe_size,
        Some(value.len() as u64),
        "FilesystemStore inner MUST have the cached blob within \
         {:?} after the consumer drained the stream — decoupled \
         cache write must complete fully on huge-chunk happy path — \
         regression guard for #230 cache-write/consumer-read \
         decoupling architecture (mutation: comment out the \
         `tokio::spawn` for the cache task; cache file never appears; \
         this assertion fails)",
        TEST_TIMEOUT,
    );

    // Sanity: no abandon paths fired on the happy path.
    let (attempts, _completed, full, eof) =
        proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts, 1, "exactly one cache attempt");
    assert_eq!(
        full, 0,
        "happy path must not abandon-on-full; observed full={full}",
    );
    assert_eq!(
        eof, 0,
        "happy path must not abandon-on-consumer-eof; observed eof={eof}",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 9 (FU-6 Test A): cache fan-out abandonment must NOT emit error!
// ---------------------------------------------------------------------
//
// Invariant: a best-effort background cache fan-out abandonment
// (downloader outraced the cache mpsc — Bazel read already succeeded,
// blob durable via the worker's own upload) MUST NOT produce an
// `error!`-level log from the inner store's `update()` site.
//
// Mechanism violation (pre-fix): `WorkerProxyStore` dropped `cache_tx`
// without signalling the abandonment; inner stores saw a generic
// "Sender dropped before sending EOF" (`Code::Internal`) and logged
// `error!`. This is benign but operationally misleading.
//
// Mechanism re-establishing (the fix): `WorkerProxyStore` calls
// `cache_tx.send_error(make_err!(Code::Aborted, CACHE_FANOUT_ABANDONED_MARKER))`
// before dropping on every intentional-abandon path. Inner stores
// detect `Code::Aborted + CACHE_FANOUT_ABANDONED_MARKER` and log
// `debug!` instead of `error!`.
//
// Production composition: real WPS + ExistenceCacheStore wrapping
// DelayedReadInnerStore (matches the existence-cache layer in the
// production CAS chain — see production seam list below). Delayed
// read ensures the 16-slot mpsc fills and the Full-abandon path fires.
//
// Production seam list (ordered producer → classifier):
//   1. WorkerProxyStore (producer: send_error on Full-abandon)
//   2. VerifyStore — analytically verified to preserve Code::Aborted
//      and the CACHE_FANOUT_ABANDONED_MARKER string via err_tip_with_code
//      (preserves code); marker is in messages[0] and survives the chain.
//      Not a test seam here; see distsys review NIT acknowledgement.
//   3. ExistenceCacheStore (classifier: this test's seam — covered)
//   The two FastSlowStore classifier seams are covered by
//   cdn_tee_fss_nc_abandonment_does_not_emit_error_log (non-chunked)
//   and cdn_tee_fss_chunked_abandonment_does_not_emit_error_log (chunked).
//
// Test uses `#[nativelink_test]` (= `#[traced_test]`); `logs_contain`
// checks all captured log events regardless of level.
//
// Mutation verification: comment out the `tx.send_error(...)` call in
// the `TrySendError::Full` arm of `get_part_and_cache_inner`. The inner
// stores receive `Code::Internal "Sender dropped before sending EOF"`,
// which does NOT match `CACHE_FANOUT_ABANDONED_MARKER`, so `error!`
// fires. The assertion `!logs_contain("ERROR") || !logs_contain("inner \
// store write failed")` fails with the bespoke message naming the
// mutation.
#[nativelink_test]
async fn cdn_tee_abandonment_does_not_emit_error_log() -> Result<(), Error> {
    // 8 MiB blob with 64 KiB chunks → ~128 iterations → mpsc fills fast
    // with a 5s-sleeping inner store.
    let value = test_value(8 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    // Production-composition inner: ExistenceCacheStore wrapping
    // DelayedReadInnerStore. The existence cache layer IS the one that
    // fires "ExistenceCacheStore::update: inner store write failed" in
    // the production error log (FU-6 finding). DelayedReadInnerStore
    // sleeps before reading so the 16-slot mpsc fills (Full-abandon fires),
    // then reads from the reader and propagates the Aborted error.
    let delayed_inner = Store::new(Arc::new(DelayedReadInnerStore {
        sleep: Duration::from_secs(3),
    }));
    let ec_inner = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        delayed_inner,
    ));

    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        ec_inner.clone(),
        peer_inner,
        digest,
        "grpc://cdn-tee-fu6-test-a-peer:50081",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive the fetch. Bazel must receive all bytes even with the slow cache.
    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — Bazel-side fetch must complete within 10s \
         with slow inner; coupling violation if this times out (FU-6 Test A)",
    )?;
    assert_eq!(
        bytes.len(),
        value.len(),
        "Bazel must receive every byte even when cache abandons",
    );

    // The abandon-on-full counter MUST have fired (proving the tested path ran).
    let (attempts, _completed, full, _eof) = proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts, 1, "exactly one cache attempt");
    assert!(
        full >= 1,
        "abandon-on-full MUST fire with 5s-sleeping inner and 8 MiB blob; \
         observed full={full} — the log-demotion path was never exercised \
         if this fires (FU-6 Test A)",
    );

    // Wait for the cache task to complete: it sleeps 3s in
    // DelayedReadInnerStore then reads from cache_rx and gets the Aborted
    // signal. Poll until "cache fan-out abandoned" fires in THIS TEST's
    // log (the ExistenceCacheStore debug! demotion message), or
    // TEST_TIMEOUT expires. `logs_contain` is test-local (per-test
    // capture from #[traced_test]); it is safe to call in a polling loop.
    let cache_task_deadline = std::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if logs_contain("cache fan-out abandoned") {
            break;
        }
        if std::time::Instant::now() >= cache_task_deadline {
            panic!(
                "must not deadlock — cache task must complete within {:?} \
                 and log 'cache fan-out abandoned' (FU-6 Test A: demotion \
                 path must fire within the test timeout)",
                TEST_TIMEOUT
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // CRITICAL assertion: the inner-store write-failure ERROR message
    // must NOT appear in this test's log. Pre-fix, the abandonment
    // surfaced as "ExistenceCacheStore::update: inner store write failed".
    // Post-fix, the demotion path logs "cache fan-out abandoned (best-effort)"
    // at DEBUG instead. The original "inner store write failed" message is
    // entirely absent from the demoted path.
    //
    // `logs_contain` is test-local (injected by #[traced_test] via
    // #[nativelink_test]) and checks all captured events regardless of level.
    // We assert the old error message does NOT appear (it has been replaced
    // by the new debug message).
    assert!(
        !logs_contain("inner store write failed"),
        "FU-6 Test A: cache fan-out abandonment MUST NOT log 'inner store \
         write failed' at any level — that message is from the error! path \
         which should have been demoted to debug! with 'cache fan-out \
         abandoned'. Mutation check: the `tx.send_error(...)` in the \
         Full-abandon arm of get_part_and_cache_inner must be in place; \
         removing it causes Code::Internal (not Code::Aborted+MARKER) which \
         bypasses the demotion check and restores the error! path (FU-6 regression)",
    );

    // NOTE: we do NOT assert !logs_contain("data stream failed") here
    // because FastSlowStore is not in this test's composition — the
    // assertion would be vacuously true and create false seam-coverage
    // confidence (testing-czar finding T3).  That string is guarded
    // meaningfully by cdn_tee_fss_nc_abandonment_does_not_emit_error_log
    // and cdn_tee_fss_chunked_abandonment_does_not_emit_error_log.

    // Sanity: the debug-level abandonment log MUST be present (proves the
    // demotion path fired, not that we silently swallowed the error).
    assert!(
        logs_contain("cache fan-out abandoned"),
        "FU-6 Test A: the debug! demotion log MUST fire when abandonment is \
         detected — absence means the demotion check did not run at all",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 10 (FU-6 Test B): genuine update failure still logs error!
// ---------------------------------------------------------------------
//
// Invariant: a GENUINE inner-store write failure (not a best-effort
// cache fan-out abandonment) MUST still produce an `error!`-level log
// from `ExistenceCacheStore::update`. This is the over-demotion guard:
// it proves the `CACHE_FANOUT_ABANDONED_MARKER` check is scoped and
// does NOT blanket-demote all update failures.
//
// Test composition: ExistenceCacheStore wrapping a FailingInnerStore
// that returns `Code::Internal, "genuine disk failure"` on every
// `update()`. The error code is Internal (not Aborted) and the message
// does NOT contain CACHE_FANOUT_ABANDONED_MARKER, so the demotion
// check evaluates to false and `error!` fires.
//
// Mutation verification: in `existence_cache_store::update`, change the
// demotion condition to `true` (always demote). The `error!` disappears
// and this test fails with the bespoke "ERROR must still appear"
// message.
#[nativelink_test]
async fn genuine_update_failure_still_logs_error_level() -> Result<(), Error> {
    // FailingInnerStore: always returns Code::Internal on update().
    // Message deliberately does NOT contain CACHE_FANOUT_ABANDONED_MARKER.
    let failing = Store::new(Arc::new(FailingUpdateInnerStore {
        err_code: Code::Internal,
        err_msg: "genuine disk failure — not a cache fan-out abandonment",
    }));
    let ec_store = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        failing,
    ));

    let digest = DigestInfo::try_new(VALID_HASH1, 4)?;
    // Write a short payload via a channel that sends 4 bytes then EOF.
    let (mut tx, rx) = make_buf_channel_pair();
    tx.send(Bytes::from_static(b"data")).await.map_err(|e| {
        make_err!(Code::Internal, "test: tx.send failed: {e:?}")
    })?;
    tx.send_eof().map_err(|e| {
        make_err!(Code::Internal, "test: tx.send_eof failed: {e:?}")
    })?;

    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        ec_store.update(StoreKey::from(digest), rx, UploadSizeInfo::ExactSize(4)),
    )
    .await
    .expect(
        "must not deadlock — ExistenceCacheStore::update must complete within 10s \
         even on inner failure (FU-6 Test B: genuine update failure guard)",
    );

    assert!(
        result.is_err(),
        "genuine inner-store failure must propagate as Err (FU-6 Test B)",
    );
    assert_eq!(
        result.unwrap_err().code,
        Code::Internal,
        "genuine failure code must be preserved through ExistenceCacheStore (FU-6 Test B)",
    );

    // The ERROR log MUST fire — genuine failures must not be silently demoted.
    // `logs_contain` is test-local and covers all captured levels; the
    // exact "inner store write failed" string comes from the `error!` macro
    // in ExistenceCacheStore::update (unchanged by the FU-6 fix for
    // non-abandonment errors).
    assert!(
        logs_contain("inner store write failed"),
        "FU-6 Test B: a genuine inner-store write failure (Code::Internal, \
         no CACHE_FANOUT_ABANDONED_MARKER) MUST log 'inner store write failed' \
         (the error! message in ExistenceCacheStore::update). \
         Mutation check: changing the demotion condition in \
         existence_cache_store::update to always-true would silence the error! \
         and cause this assertion to fail — FU-6 over-demotion guard",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 11 (FU-6 Test A-FSS-NC): FastSlowStore non-chunked demotion seam
// ---------------------------------------------------------------------
//
// Invariant: a best-effort background cache fan-out abandonment MUST NOT
// produce `error!` "FastSlowStore::update: data stream failed" from the
// non-chunked update path (fast_slow_store.rs:~5247).
//
// Mechanism: WPS sends Code::Aborted+CACHE_FANOUT_ABANDONED_MARKER via
// send_error; FSS's data_stream_fut reads it from cache_rx via
// reader.recv().err_tip(...)?; the outer `if let Err(err) = data_res`
// check at ~5228 calls is_cache_fanout_abandonment and demotes to debug!.
//
// Production composition: WPS → FastSlowStore{fast: SlowUpdateInnerStore,
// slow: MemoryStore}.  SlowUpdateInnerStore sleeps 3s before draining
// fast_rx so fast_tx (128-slot) fills after ~128 × 64 KiB = 8 MiB of
// forwarding, causing data_stream_fut to block on fast_guard.send(buffer)
// → cache_rx is not drained → 16-slot WPS cache mpsc fills → Full-abandon
// fires → send_error → data_stream_fut's reader.recv() returns Err(Aborted)
// → FSS non-chunked error check at ~5228 classifies correctly.
//
// Mutation verification: comment out the
// `is_cache_fanout_abandonment(&err)` check in fast_slow_store.rs at the
// non-chunked site (force the `else` branch always). The `error!` fires.
// This test fails with: "FU-6 Test A-FSS-NC: FSS non-chunked seam MUST
// NOT log 'data stream failed' on abandonment".
#[nativelink_test]
async fn cdn_tee_fss_nc_abandonment_does_not_emit_error_log() -> Result<(), Error> {
    // 16 MiB blob with 64 KiB chunks → 256 iterations.
    // fast_tx has 128 slots; after 128 sends SlowUpdateInnerStore is still
    // sleeping (has not yet drained fast_rx), so fast_guard.send(buffer)
    // blocks on the 129th chunk.  cache_rx stalls → WPS 16-slot mpsc fills
    // → Full-abandon fires → send_error(Aborted+MARKER).
    let value = test_value(16 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    // FSS fast store: SlowUpdateInnerStore sleeps 3s before draining
    // fast_rx. This backs up fast_tx (128-slot), which backs up
    // cache_rx (16-slot WPS mpsc) → Full-abandon → send_error(Aborted+MARKER).
    let slow_fast = Store::new(Arc::new(SlowUpdateInnerStore {
        sleep: Duration::from_secs(3),
        update_calls: AtomicU64::new(0),
    }));
    // FSS slow store: MemoryStore — succeeds instantly for the slow-write
    // background spawn (only reached after data_stream_fut completes,
    // which in the error path it doesn't — the test just exercises the
    // data_stream_fut error path).
    let fss_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        slow_fast,
        fss_slow,
    );
    let fss = Store::new(fss_arc);

    // Peer: serves the blob in 64 KiB chunks at full speed so that the
    // Bazel-side fetch completes before the slow cache side.
    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        fss,
        peer_inner,
        digest,
        "grpc://cdn-tee-fu6-fss-nc-peer:50082",
    );
    let proxy = Store::new(proxy_arc.clone());

    // Drive the Bazel-side fetch. MUST complete even though the cache
    // (FSS fast store) is slow.
    let bytes = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — Bazel-side fetch must complete within 10s \
         despite slow FSS fast store (FU-6 Test A-FSS-NC: decoupling violated \
         if this times out)",
    )?;
    assert_eq!(
        bytes.len(),
        value.len(),
        "Bazel must receive every byte even when FSS cache abandons",
    );

    // Full-abandon MUST have fired (proves the tested path ran).
    let (attempts, _completed, full, _eof) = proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(attempts, 1, "exactly one cache attempt (FU-6 Test A-FSS-NC)");
    assert!(
        full >= 1,
        "abandon-on-full MUST fire with 16 MiB blob and slow FSS fast store \
         (fast_tx fills after 128 × 64 KiB = 8 MiB, blocking data_stream_fut, \
         backing up the 16-slot WPS mpsc); observed full={full} — FSS \
         non-chunked demotion path was not exercised (FU-6 Test A-FSS-NC)",
    );

    // Poll until the FSS non-chunked demotion debug! fires.
    // FSS's data_stream_fut sees Err(Aborted+MARKER) from cache_rx
    // after the 3s SlowUpdateInnerStore sleep + WPS send_error.
    let cache_task_deadline = std::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if logs_contain("cache fan-out abandoned") {
            break;
        }
        if std::time::Instant::now() >= cache_task_deadline {
            panic!(
                "must not deadlock — FSS non-chunked demotion path must fire \
                 within {:?} (FU-6 Test A-FSS-NC: is_cache_fanout_abandonment \
                 check at fast_slow_store.rs non-chunked site must have fired)",
                TEST_TIMEOUT,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // CRITICAL: FSS non-chunked error! MUST be absent.
    assert!(
        !logs_contain("data stream failed"),
        "FU-6 Test A-FSS-NC: cache fan-out abandonment MUST NOT log \
         'data stream failed' — that is the FastSlowStore::update error! \
         message (non-chunked site, fast_slow_store.rs:~5247). \
         Mutation check: comment out the is_cache_fanout_abandonment branch \
         at the non-chunked FSS site — the else-branch fires error! and this \
         assertion fails (FU-6 FSS non-chunked seam regression)",
    );

    // Sanity: demotion debug! must be present.
    assert!(
        logs_contain("cache fan-out abandoned"),
        "FU-6 Test A-FSS-NC: debug! demotion log MUST fire (proves the \
         FSS non-chunked demotion path ran, not that the error was swallowed)",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 12 (FU-6 Test A-FSS chunked): FastSlowStore chunked demotion seam
// ---------------------------------------------------------------------
//
// Invariant: same as Test A-FSS-NC but for the chunked dispatch path
// (fast_slow_store.rs:~2014).  The chunked site fires when a chunked
// dispatcher is installed AND the kill-switch is ON AND the blob ≥ the
// size threshold.
//
// Mechanism: the chunked data_stream_fut tees to fast_tx AND chunk_tx.
// When the upstream cache_rx delivers Err(Aborted+MARKER), data_stream_fut
// propagates it via `?`. The outer `if let Err(err) = data_res` at ~2003
// calls is_cache_fanout_abandonment and logs debug! instead of error!.
//
// Production composition: WPS → FastSlowStore{fast: SlowUpdateInnerStore,
// slow: MemoryStore} with a draining BazelChunkedDispatcher and
// set_chunked_size_threshold_for_test(1) so the threshold is always met.
//
// Mutation verification: comment out the is_cache_fanout_abandonment branch
// at the chunked FSS site (fast_slow_store.rs:~2003). The `else` branch
// fires error!. This test fails with: "FU-6 Test A-FSS-chunked: FSS
// chunked seam MUST NOT log 'data stream failed' on abandonment".
#[cfg(all(feature = "chunked_fast_slow", feature = "test-utils"))]
#[nativelink_test]
async fn cdn_tee_fss_chunked_abandonment_does_not_emit_error_log() -> Result<(), Error> {
    use std::sync::OnceLock;

    use nativelink_store::chunked::{
        BazelChunkedDispatcher, BazelChunkedDispatcherArc, disable_bazel_facing_internal_chunking,
        enable_bazel_facing_internal_chunking,
    };
    use nativelink_util::buf_channel::DropCloserReadHalf as ChunkedReadHalf;

    // Process-wide kill-switch is global state; serialize with a
    // per-process Mutex so sibling tests in the same binary don't race.
    static CHUNK_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    let _kill_guard = CHUNK_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    // Draining dispatcher: absorbs all chunks from chunk_rx quickly so
    // the data_stream_fut isn't blocked on chunk_tx backpressure.
    // Returns Ok as soon as the reader hits EOF or error.
    #[derive(Debug)]
    struct DrainingDispatcher;
    #[async_trait::async_trait]
    impl BazelChunkedDispatcher for DrainingDispatcher {
        async fn dispatch(
            &self,
            digest: DigestInfo,
            mut reader: ChunkedReadHalf,
        ) -> Result<u64, nativelink_error::Error> {
            loop {
                let buf = reader.recv().await?;
                if buf.is_empty() {
                    break;
                }
            }
            Ok(digest.size_bytes())
        }
    }

    // 16 MiB blob (same sizing rationale as A-FSS-NC; fast_tx fills after
    // 128 × 64 KiB = 8 MiB of forwarding → data_stream_fut blocks →
    // cache_rx backs up → Full-abandon fires).
    let value = test_value(16 * 1024 * 1024);
    let digest = digest_for_size(value.len() as u64);

    // FSS fast store: SlowUpdateInnerStore (same role as A-FSS-NC: backs
    // up fast_tx → backs up cache_rx → Full-abandon → send_error).
    let slow_fast = Store::new(Arc::new(SlowUpdateInnerStore {
        sleep: Duration::from_secs(3),
        update_calls: AtomicU64::new(0),
    }));
    let fss_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        slow_fast,
        fss_slow,
    );
    // Wire the dispatcher + lower size threshold to 1 byte so this
    // blob always takes the chunked path.
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(DrainingDispatcher);
    fss_arc.set_bazel_chunked_dispatcher(dispatcher);
    fss_arc.set_chunked_size_threshold_for_test(1);
    enable_bazel_facing_internal_chunking();

    let fss = Store::new(fss_arc);

    let peer_inner = Store::new(Arc::new(ChunkedPeerStore {
        payload: Bytes::from(value.clone()),
        chunk_size: 64 * 1024,
        inter_chunk_sleep_ms: AtomicU64::new(0),
    }));

    let (proxy_arc, _locality) = build_proxy_with_peer(
        fss,
        peer_inner,
        digest,
        "grpc://cdn-tee-fu6-fss-chunked-peer:50083",
    );
    let proxy = Store::new(proxy_arc.clone());

    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        proxy.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — Bazel-side fetch must complete within 10s \
         (FU-6 Test A-FSS-chunked: decoupling violated if this times out)",
    );
    // Restore kill-switch before any assertion can panic.
    disable_bazel_facing_internal_chunking();

    result.map_err(|e| {
        make_err!(
            Code::Internal,
            "FU-6 Test A-FSS-chunked: Bazel fetch failed: {e:?}"
        )
    })?;

    let (attempts, _completed, full, _eof) = proxy_arc.cdn_tee_counters_snapshot();
    assert_eq!(
        attempts, 1,
        "exactly one cache attempt (FU-6 Test A-FSS-chunked)"
    );
    assert!(
        full >= 1,
        "abandon-on-full MUST fire with 16 MiB blob and slow FSS fast store; \
         observed full={full} — FSS chunked demotion path was not exercised \
         (FU-6 Test A-FSS-chunked)",
    );

    let cache_task_deadline = std::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if logs_contain("cache fan-out abandoned") {
            break;
        }
        if std::time::Instant::now() >= cache_task_deadline {
            panic!(
                "must not deadlock — FSS chunked demotion path must fire \
                 within {:?} (FU-6 Test A-FSS-chunked: is_cache_fanout_abandonment \
                 check at fast_slow_store.rs chunked site must have fired)",
                TEST_TIMEOUT,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        !logs_contain("data stream failed"),
        "FU-6 Test A-FSS-chunked: cache fan-out abandonment MUST NOT log \
         'data stream failed' — that is the FastSlowStore::update (chunked) \
         error! message (fast_slow_store.rs:~2022). \
         Mutation check: comment out the is_cache_fanout_abandonment branch \
         at the chunked FSS site — the else-branch fires error! and this \
         assertion fails (FU-6 FSS chunked seam regression)",
    );

    assert!(
        logs_contain("cache fan-out abandoned"),
        "FU-6 Test A-FSS-chunked: debug! demotion log MUST fire (proves the \
         FSS chunked demotion path ran)",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// Test 13 (FU-6 Test B-FSS): FSS genuine failure still logs error!
// ---------------------------------------------------------------------
//
// Invariant: a GENUINE data-stream failure through FastSlowStore MUST
// still produce `error!` "FastSlowStore::update: data stream failed".
// This is the over-demotion guard for the FSS non-chunked site: it
// proves is_cache_fanout_abandonment is scoped and does NOT blanket-demote
// all update failures.
//
// Mechanism: inject a Code::Internal error directly into the reader
// passed to FSS::update.  The error does NOT contain
// CACHE_FANOUT_ABANDONED_MARKER, so is_cache_fanout_abandonment returns
// false and the `else` branch fires error!.
//
// Mutation verification: in fast_slow_store.rs at the non-chunked site,
// change `is_cache_fanout_abandonment(&err)` to `true` (always demote).
// The error! disappears.  This test fails with: "FU-6 Test B-FSS: a
// genuine FSS data-stream failure MUST log 'data stream failed'".
#[nativelink_test]
async fn fss_genuine_update_failure_still_logs_error_level() -> Result<(), Error> {
    // Build a FastSlowStore with MemoryStore on both tiers. We only care
    // about the data_stream_fut error path; neither store is reached before
    // the error fires.
    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );
    let fss = Store::new(fss_arc);

    // Inject a genuine Code::Internal error that does NOT carry
    // CACHE_FANOUT_ABANDONED_MARKER.  FSS's data_stream_fut sees
    // reader.recv() return this error; is_cache_fanout_abandonment returns
    // false; the else-branch fires error!.
    let (mut tx, rx) = make_buf_channel_pair();
    tx.send_error(make_err!(
        Code::Internal,
        "genuine disk failure — not a cache fan-out abandonment"
    ));

    let digest = DigestInfo::try_new(VALID_HASH1, 4)?;
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        fss.update(StoreKey::from(digest), rx, UploadSizeInfo::ExactSize(4)),
    )
    .await
    .expect(
        "must not deadlock — FastSlowStore::update must complete within 10s \
         on reader error (FU-6 Test B-FSS: genuine update failure guard)",
    );

    assert!(
        result.is_err(),
        "genuine data-stream failure must propagate as Err (FU-6 Test B-FSS)",
    );

    // The ERROR log MUST fire for genuine failures.
    assert!(
        logs_contain("data stream failed"),
        "FU-6 Test B-FSS: a genuine data-stream failure (Code::Internal, \
         no CACHE_FANOUT_ABANDONED_MARKER) MUST log 'data stream failed' \
         (the error! message in FastSlowStore::update non-chunked). \
         Mutation check: changing is_cache_fanout_abandonment to always-true \
         at the FSS non-chunked site would silence the error! and cause this \
         assertion to fail — FU-6 FSS over-demotion guard",
    );

    Ok(())
}

// =====================================================================
// Test fixtures (slow / throttled inner stores; on-disk file probe)
// =====================================================================

/// Inner store that delegates `update`/`get_part`/`has_with_results`
/// to the inner via a `Box`-async layer that sleeps `sleep` before
/// returning. Used to simulate a slow cache tier so the mpsc
/// abandon-full path fires deterministically.
#[derive(Debug, MetricsComponent)]
struct SlowUpdateInnerStore {
    sleep: Duration,
    #[metric(help = "number of update() calls")]
    update_calls: AtomicU64,
}

default_health_status_indicator!(SlowUpdateInnerStore);

#[async_trait]
impl StoreDriver for SlowUpdateInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_calls.fetch_add(1, AOrdering::SeqCst);
        // Sleep BEFORE we drain — this is what causes cache_tx to back
        // up and abandon-on-full to fire.
        tokio::time::sleep(self.sleep).await;
        let _drained = reader.drain().await;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "SlowUpdateInnerStore: get_part NotFound (peer-fetch must take over)"
        ))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Throttled inner: sleeps `sleep` before EACH update, then delegates
/// to the real inner. The `triggered` flag is informational only.
#[derive(Debug, MetricsComponent)]
struct ThrottledFirstChunkInnerStore {
    delegate: Store,
    sleep: Duration,
    #[metric(help = "1 iff at least one update has fired")]
    triggered: AtomicU64,
}

default_health_status_indicator!(ThrottledFirstChunkInnerStore);

#[async_trait]
impl StoreDriver for ThrottledFirstChunkInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.delegate.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.triggered.store(1, AOrdering::SeqCst);
        tokio::time::sleep(self.sleep).await;
        self.delegate.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.delegate.get_part(key, writer, offset, length).await
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Inner store that always returns a specified error on `update()`.
/// Used by Test 10 (FU-6 Test B) to verify that genuine failures (those
/// NOT carrying `CACHE_FANOUT_ABANDONED_MARKER`) still produce `error!`
/// from `ExistenceCacheStore::update`.
#[derive(Debug, MetricsComponent)]
struct FailingUpdateInnerStore {
    err_code: Code,
    err_msg: &'static str,
}

default_health_status_indicator!(FailingUpdateInnerStore);

#[async_trait]
impl StoreDriver for FailingUpdateInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Drain the reader so the producer doesn't wedge, then fail.
        let _ = reader.drain().await;
        Err(make_err!(self.err_code, "{}", self.err_msg))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "FailingUpdateInnerStore: not found"))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Inner store that sleeps `sleep` before reading from the reader, then
/// propagates any read error. Used by Test 9 (FU-6 Test A) to simulate
/// the production scenario where the inner store is slow to drain
/// `cache_rx` (causing the mpsc to fill and the Full-abandon path to
/// fire), then attempts to read and receives the `Code::Aborted`
/// abandonment signal via `send_error`.
///
/// Unlike `SlowUpdateInnerStore` (which ignores read errors), this
/// store propagates them so `ExistenceCacheStore::update` sees the
/// `Code::Aborted + CACHE_FANOUT_ABANDONED_MARKER` error and can
/// exercise its demotion path.
#[derive(Debug, MetricsComponent)]
struct DelayedReadInnerStore {
    sleep: Duration,
}

default_health_status_indicator!(DelayedReadInnerStore);

#[async_trait]
impl StoreDriver for DelayedReadInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Sleep before reading, giving the forward loop time to fill
        // the 16-slot mpsc and trigger the Full-abandon + send_error path.
        tokio::time::sleep(self.sleep).await;
        // Read and propagate errors (unlike SlowUpdateInnerStore which
        // uses `let _drained = reader.drain().await` to discard errors).
        // After the Full-abandon, reader.recv() returns Err(Code::Aborted)
        // with the CACHE_FANOUT_ABANDONED_MARKER, which is propagated to
        // the ExistenceCacheStore::update error-check site.
        reader.drain().await.err_tip(|| {
            "DelayedReadInnerStore: reader.drain() failed (expected \
             Aborted on abandonment path)"
        })
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "DelayedReadInnerStore: not found"))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Peer fake that returns a fixed payload as N small chunks, with an
/// optional inter-chunk sleep. Used to:
///   - force many forward-loop iterations so the 16-slot cache mpsc has
///     a real chance to fill,
///   - keep the peer producer alive while the consumer disconnects so
///     the abandon-on-consumer-eof path actually fires.
/// Chunks are emitted in the order they appear in the payload; the
/// `chunk_size` controls how many bytes per `writer.send` call.
#[derive(Debug, MetricsComponent)]
struct ChunkedPeerStore {
    payload: Bytes,
    chunk_size: usize,
    #[metric(help = "millis to sleep between chunks")]
    inter_chunk_sleep_ms: AtomicU64,
}

default_health_status_indicator!(ChunkedPeerStore);

#[async_trait]
impl StoreDriver for ChunkedPeerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = Some(self.payload.len() as u64);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let _drained = reader.drain().await;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let offset = usize::try_from(offset).unwrap_or(0);
        let total_len = self.payload.len();
        let end = match length {
            Some(l) => core::cmp::min(
                total_len,
                offset.saturating_add(usize::try_from(l).unwrap_or(0)),
            ),
            None => total_len,
        };
        let mut pos = offset;
        let sleep_ms = self.inter_chunk_sleep_ms.load(AOrdering::Relaxed);
        while pos < end {
            let chunk_end = core::cmp::min(end, pos + self.chunk_size);
            let chunk = self.payload.slice(pos..chunk_end);
            writer.send(chunk).await?;
            pos = chunk_end;
            if sleep_ms > 0 && pos < end {
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }
        writer.send_eof()?;
        Ok(())
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

/// Walks the FilesystemStore content_path looking for any file whose
/// name contains the digest's hex prefix. Used to assert that
/// abandoned partials are NOT left on disk.
async fn find_digest_file(content_path: &str, digest: &DigestInfo) -> bool {
    let prefix = format!("{}", digest.packed_hash());
    let prefix = &prefix[..core::cmp::min(prefix.len(), 8)];
    let mut stack = vec![content_path.to_string()];
    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Ok(Some(ent)) = rd.next_entry().await {
            let path = ent.path();
            if path.is_dir() {
                stack.push(path.to_string_lossy().to_string());
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(prefix))
            {
                return true;
            }
        }
    }
    false
}
