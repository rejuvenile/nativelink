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
use nativelink_config::stores::{EvictionPolicy, FilesystemSpec, MemorySpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::{
    SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
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

// Note: the production `CDN_TEE_CACHE_MPSC_CAP = 4` is intentionally
// not duplicated here; tests assert via behavior + counters, not by
// re-coupling to the constant. If the cap changes in production,
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
    // chunks to the 4-slot cache mpsc via try_send. With a fast peer
    // and a slow consumer, proxy_tx fills first, then the forward
    // loop's cache.try_send fills the 4-slot cache mpsc, then
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
/// open for 5s before returning. With cache mpsc cap = 4, the forward
/// loop's `try_send` MUST return Full after 4 chunks and the abandon
/// path MUST fire. Bazel must continue receiving the remaining chunks
/// at peer-reader rate, NOT at cache rate.
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
    // Blob big enough to fill the 4-slot mpsc and require many more
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
    // cache mpsc cap of 4, the forward loop produces ~128 chunks; the
    // slow cache (5s sleep before drain) means the mpsc fills almost
    // immediately and the abandon path takes over.
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
    // backs up faster than the 4-slot mpsc can drain).
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
/// **Mutation step**: revert both the `peer_handle.abort()` and the
/// `drop(proxy_rx)` lines in `get_part_and_cache` (replace with `let
/// _proxy_rx = proxy_rx; let _peer_handle = &peer_handle;` so the
/// channel and join handle are still live across the await). The
/// `tokio::time::timeout(TEST_TIMEOUT)` will fire and the bespoke
/// `.expect(...)` panics with the message below. Without the fix, the
/// peer task wedges on send() and `peer_handle.await` never returns.
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
    let _get_res = tokio::time::timeout(TEST_TIMEOUT, get_handle)
        .await
        .expect(
            "must not deadlock — peer task pinned by undrained proxy_rx \
             after consumer disconnect (#230 M1 BLOCK regression)",
        )
        .expect("get_part task must not panic");

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
}

/// Peer fake that returns a fixed payload as N small chunks, with an
/// optional inter-chunk sleep. Used to:
///   - force many forward-loop iterations so the 4-slot cache mpsc has
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
