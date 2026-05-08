// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! #325 (option D.1) regression suite: per-reader fallback to a fresh
//! `slow_store.get_part` when a streaming-buffer reader trips
//! `Code::Unavailable: reader fell behind sliding window`.
//!
//! Mechanism (production failure being guarded):
//! 1. Producer fills the 64 MiB streaming buffer at line rate.
//! 2. A slow consumer (gRPC egress + VerifyStore re-hashing every byte)
//!    drains it slower than the producer fills.
//! 3. The sliding window evicts past the consumer's cursor.
//! 4. `next_chunk()` returns `Code::Unavailable: reader fell behind
//!    sliding window`.
//! 5. WorkerProxyStore correctly refuses peer-fetch when bytes have
//!    already flowed (would corrupt the prefix); returns partial bytes
//!    → Bazel digest mismatch → build break.
//!
//! Fix (option D.1): on the sliding-window error, splice in a fresh
//! `slow_store.get_part(key, offset=bytes_already_sent, length=remaining)`.
//! Producer and other readers unaffected. The slow reader transparently
//! resumes against the slow tier; consumer sees a single continuous
//! byte stream.
//!
//! These tests use the `streaming_blob_next_chunk_fail` failpoint, whose
//! synthetic message contains `"reader fell behind sliding window"` so
//! the production D.1 predicate trips.

use core::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use serial_test::serial;
use tokio::sync::Notify;
use tokio::try_join;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// sha256 of vec![0x42; 4096]. Verified by `python3 -c
/// "import hashlib; print(hashlib.sha256(b'\x42'*4096).hexdigest())"`.
/// We use sha256 because `default_digest_hasher_func()` defaults to
/// sha256 in tests (production overrides to blake3); see
/// `digest_hasher.rs:53`.
const SHA256_HASH_4096_42S: &str =
    "725bcd6c66d02acf6ebeab9c92410e010ea22e336876256aaf05a211f4ce1902";

/// Insert `chunks` into `store` as separate `Bytes` sends so the slow
/// tier (MemoryStore) preserves the chunk boundary on read. `update_oneshot`
/// always sends a single chunk; this helper bypasses it so we can stage
/// multi-chunk blobs needed by the splice-arithmetic test below (the
/// failpoint must fire AFTER at least one chunk has been forwarded so
/// `bytes_already_sent > 0` actually exercises the splice arithmetic).
async fn update_multi_chunk(
    store: &Store,
    digest: DigestInfo,
    chunks: &[Bytes],
) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let total_size: u64 = chunks.iter().map(|b| b.len() as u64).sum();
    let chunks_owned: Vec<Bytes> = chunks.to_vec();
    let send_fut = async move {
        for chunk in chunks_owned {
            tx.send(chunk)
                .await
                .err_tip(|| "update_multi_chunk: send chunk")?;
        }
        tx.send_eof()
            .err_tip(|| "update_multi_chunk: send_eof")?;
        Ok::<(), Error>(())
    };
    let update_fut = store.update(digest, rx, UploadSizeInfo::ExactSize(total_size));
    try_join!(send_fut, update_fut)?;
    Ok(())
}

/// Build a FastSlowStore wrapping a MemoryStore fast tier and a separate
/// MemoryStore slow tier, returning the FastSlowStore Arc so we can read
/// its metric counters.
fn make_fast_slow_arc() -> (Arc<FastSlowStore>, Store, Store) {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    (fss, fast_store, slow_store)
}

// -------------------------------------------------------------------------
// 1. UNDER-ACTION (the bug-shape): reader hits sliding-window eviction →
//    D.1 fallback fires → blob delivered intact → metric incremented.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_populator_caller_falls_back_on_sliding_window_eviction() -> Result<(), Error> {
    let (fss, _fast_store, slow_store) = make_fast_slow_arc();
    let store = Store::new(fss.clone());

    let data = Bytes::from(vec![0x42; 4096]);
    let digest = DigestInfo::try_new(VALID_HASH, 4096).unwrap();

    // Stage data into slow tier so the populator-caller path will reach
    // the streaming-buffer read loop (rather than short-circuiting on
    // empty slow tier).
    slow_store
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to slow store")?;

    let metric_before = fss.streaming_buffer_reader_fallback_to_direct_total();

    // Enable failpoint so the populator-caller's `next_chunk()` returns
    // the production-shape `Code::Unavailable` carrying "reader fell
    // behind sliding window".
    fail::cfg("streaming_blob_next_chunk_fail", "return").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — D.1 fallback contract violated; full blob \
         must be delivered via slow-store splice within 10s",
    );

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes = result.expect(
        "must not return Code::Unavailable — D.1 fallback contract \
         violated; full blob must be delivered via slow-store splice \
         instead of propagating the sliding-window error to the caller",
    );

    assert_eq!(
        bytes.len(),
        4096,
        "full blob must be delivered via slow-store splice; got {} bytes",
        bytes.len()
    );
    assert_eq!(
        bytes,
        data,
        "spliced bytes must match the original blob byte-for-byte"
    );

    let metric_after = fss.streaming_buffer_reader_fallback_to_direct_total();
    assert_eq!(
        metric_after - metric_before,
        1,
        "D.1 fallback metric must increment exactly once on sliding-window \
         eviction; got delta = {}",
        metric_after - metric_before
    );

    Ok(())
}

// -------------------------------------------------------------------------
// 2. OVER-ACTION (asymmetric coverage): reader does NOT hit sliding-window
//    eviction → fallback does NOT fire → metric NOT incremented. Guards
//    against an over-broad predicate that would slow-store-fall-back on
//    every read.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_populator_caller_does_not_fall_back_on_clean_read() -> Result<(), Error> {
    let (fss, _fast_store, slow_store) = make_fast_slow_arc();
    let store = Store::new(fss.clone());

    let data = Bytes::from(vec![0x77; 4096]);
    let digest = DigestInfo::try_new(VALID_HASH, 4096).unwrap();

    slow_store
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to slow store")?;

    // Failpoint OFF — production fast-path. Failpoint must be reset
    // explicitly because the serial(failpoints) group may have left
    // it set by a prior test.
    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let metric_before = fss.streaming_buffer_reader_fallback_to_direct_total();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect("clean read must complete within 10s");

    let bytes = result.expect("clean read must succeed");
    assert_eq!(bytes, data, "clean read must return the original blob");

    let metric_after = fss.streaming_buffer_reader_fallback_to_direct_total();
    assert_eq!(
        metric_after - metric_before,
        0,
        "D.1 fallback metric must NOT increment on a clean (no-eviction) \
         read; got delta = {} (over-broad predicate would over-bump)",
        metric_after - metric_before
    );

    Ok(())
}

// -------------------------------------------------------------------------
// 3. PARTIAL READ correctness: the splice must respect the (offset, length)
//    requested by the caller. Asserts the byte-range arithmetic in the
//    fallback (new_offset = offset + bytes_already_sent;
//    new_length = length - bytes_already_sent).
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_populator_caller_partial_read_after_fallback() -> Result<(), Error> {
    let (fss, _fast_store, slow_store) = make_fast_slow_arc();
    let store = Store::new(fss.clone());

    let mut payload = vec![0u8; 8192];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i & 0xFF) as u8;
    }
    let data = Bytes::from(payload);
    let digest = DigestInfo::try_new(VALID_HASH, 8192).unwrap();

    slow_store
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to slow store")?;

    fail::cfg("streaming_blob_next_chunk_fail", "return").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 1024, Some(2048)),
    )
    .await
    .expect("partial read must complete via fallback within 10s");

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes = result.expect(
        "partial read through D.1 fallback must succeed — splice byte-range \
         arithmetic is broken if this returns Err",
    );
    assert_eq!(
        bytes.len(),
        2048,
        "partial read must return exactly 2048 bytes (length parameter); \
         got {}",
        bytes.len()
    );
    assert_eq!(
        bytes,
        data.slice(1024..3072),
        "partial read bytes must match the original byte range [1024..3072) \
         — splice offset arithmetic is wrong if this fails"
    );

    Ok(())
}

// -------------------------------------------------------------------------
// 3b. SPLICE ARITHMETIC: the `1*off->return` failpoint action lets the
//     FIRST `next_chunk()` succeed (so the producer-loop forwards bytes to
//     the outer writer), then trips on the SECOND call. By that point
//     `bytes_already_sent > 0`, so the splice computes
//     `new_offset = offset + bytes_already_sent` (NOT `new_offset =
//     offset` — the testing-czar T1 mutation finding). The earlier
//     "return" failpoint fires immediately on the first call when
//     `bytes_already_sent == 0`, so mutation 2 (drop the
//     `+ bytes_already_sent` term) silently passes that test family.
//     This test pins the splice arithmetic for the non-zero case.
//
//     Mutation step (run manually): in `fast_slow_store.rs` populator-
//     caller branch, change `let new_offset = offset + bytes_already_sent;`
//     to `let new_offset = offset;`. This test must red-fail with the
//     bespoke `splice offset MUST equal caller_offset + bytes_already_sent`
//     message because the slow-store splice would replay the prefix bytes
//     the streaming buffer already delivered, and the assembled blob
//     would NOT match the original.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_populator_caller_splice_after_partial_consumption() -> Result<(), Error> {
    let (fss, _fast_store, slow_store) = make_fast_slow_arc();
    let store = Store::new(fss.clone());

    // Two distinct chunks so the slow tier emits >1 streaming-buffer
    // chunk; that lets us trip the failpoint AFTER one chunk has been
    // forwarded by the populator-caller's read loop, so the splice
    // arithmetic test runs against a non-zero `bytes_already_sent`.
    let chunk0 = Bytes::from(vec![0xAAu8; 1024]);
    let chunk1 = Bytes::from(vec![0xBBu8; 1024]);
    let mut combined = Vec::with_capacity(2048);
    combined.extend_from_slice(&chunk0);
    combined.extend_from_slice(&chunk1);
    let combined = Bytes::from(combined);
    let digest = DigestInfo::try_new(VALID_HASH, 2048).unwrap();

    update_multi_chunk(&slow_store, digest, &[chunk0.clone(), chunk1.clone()])
        .await
        .err_tip(|| "setup: writing multi-chunk to slow store")?;

    let metric_before = fss.streaming_buffer_reader_fallback_to_direct_total();

    // `1*off->return`: first invocation = no-op (the read-loop reads
    // chunk0 normally), second invocation = synthetic
    // sliding-window-eviction error. By the time the failpoint fires,
    // chunk0's bytes are already in the outer writer
    // (bytes_already_sent = 1024 > 0).
    fail::cfg("streaming_blob_next_chunk_fail", "1*off->return").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — D.1 splice contract violated; full blob \
         must be delivered via mid-stream slow-store splice within 10s",
    );

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes = result.expect(
        "splice offset MUST equal caller_offset + bytes_already_sent — \
         D.1 byte-range arithmetic broken",
    );

    // Reconstructed blob must equal the original concatenated bytes.
    // If the splice used `new_offset = offset` (mutation 2), the
    // slow-store get_part(offset=0) would replay chunk0's bytes, the
    // outer writer would receive `chunk0 || chunk0 || chunk1` =
    // 3072 bytes, NOT 2048 — the assertion below would red-fail with
    // the bespoke message.
    assert_eq!(
        bytes.len(),
        2048,
        "splice offset MUST equal caller_offset + bytes_already_sent \
         — D.1 byte-range arithmetic broken; got {} bytes (expected 2048)",
        bytes.len()
    );
    assert_eq!(
        bytes, combined,
        "splice offset MUST equal caller_offset + bytes_already_sent \
         — D.1 byte-range arithmetic broken; reassembled blob bytes \
         do not match original"
    );

    let metric_after = fss.streaming_buffer_reader_fallback_to_direct_total();
    assert_eq!(
        metric_after - metric_before,
        1,
        "D.1 fallback metric must increment exactly once when failpoint \
         fires AFTER one chunk has been consumed; got delta = {}",
        metric_after - metric_before
    );

    Ok(())
}

// -------------------------------------------------------------------------
// 4. PRODUCTION-COMPOSITION: VerifyStore wrapping FastSlowStore.
//
//    The production CAS server stack is:
//      ExistenceCacheStore → VerifyStore { verify_size, verify_hash } →
//      FastSlowStore.
//
//    When the populator-caller's reader falls behind the sliding window,
//    the D.1 fallback splices in a fresh slow-store read at the cursor.
//    For VerifyStore re-hashing every byte, this MUST be safe:
//    `hash([0..N) || [N..end]) == hash([0..end])`. If the splice replays
//    bytes or skips bytes, hash-mismatch fires and Bazel rejects the
//    blob — the bug-shape we are fixing.
//
//    This test uses verify_hash=true with a known-good blake3 hash so a
//    splice off-by-one would manifest as a `verify_hash` mismatch.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_verify_store_around_fast_slow_survives_fallback_with_correct_hash()
-> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    let fss_for_metric = fss.clone();

    // Production composition: VerifyStore around FastSlowStore. We use a
    // dummy `backend` in the spec because the constructor consults it
    // only for metrics naming; the real backend is the explicit
    // `Store::new(fss)` arg.
    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: true,
        },
        Store::new(fss),
    );
    let store = Store::new(verify_store);

    let data = Bytes::from(vec![0x42u8; 4096]);
    let digest = DigestInfo::try_new(SHA256_HASH_4096_42S, 4096).unwrap();

    // Stage data directly into the slow tier (NOT through VerifyStore,
    // so the staging step doesn't trip a hash-mismatch on its own).
    slow_store
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to slow store")?;

    let metric_before = fss_for_metric.streaming_buffer_reader_fallback_to_direct_total();

    fail::cfg("streaming_blob_next_chunk_fail", "return").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — VerifyStore-wrapped FastSlowStore D.1 fallback \
         contract violated; full blob must be delivered via slow-store \
         splice within 10s",
    );

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes = result.expect(
        "must not return error — VerifyStore + D.1 splice must deliver \
         the full blob; if this fails with a hash-mismatch the splice \
         either replayed or dropped bytes (the bug-shape from production \
         build break 2026-05-08 09:00:07 PDT)",
    );

    assert_eq!(
        bytes.len(),
        4096,
        "VerifyStore-wrapped fallback must deliver full 4096 bytes"
    );
    assert_eq!(
        bytes, data,
        "splice bytes must match the original blob byte-for-byte through \
         VerifyStore (hash + size verified)"
    );

    let metric_after = fss_for_metric.streaming_buffer_reader_fallback_to_direct_total();
    assert_eq!(
        metric_after - metric_before,
        1,
        "D.1 fallback metric must increment under production composition \
         (VerifyStore + FastSlowStore); got delta = {}",
        metric_after - metric_before
    );

    Ok(())
}

// -------------------------------------------------------------------------
// 5. CACHE-TEE preserved: when the populator-caller's reader hits the
//    sliding-window fallback, the producer task is INDEPENDENT and keeps
//    running. The fast-tier population (cache-tee) writes complete in the
//    background regardless of any single reader's fallback — this is the
//    "fast readers go as fast as possible; slow readers fall back
//    transparently" guarantee.
//
//    After the fallback completes, a follow-up `has()` on the fast store
//    SHOULD see the blob if the producer's tee was unaffected.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_cache_tee_survives_reader_fallback() -> Result<(), Error> {
    let (fss, fast_store, slow_store) = make_fast_slow_arc();
    let store = Store::new(fss.clone());

    let data = Bytes::from(vec![0xAAu8; 4096]);
    let digest = DigestInfo::try_new(VALID_HASH, 4096).unwrap();

    slow_store
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to slow store")?;

    fail::cfg("streaming_blob_next_chunk_fail", "return").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect("get_part must complete via fallback within 10s");

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes = result.expect("get_part via fallback must succeed");
    assert_eq!(bytes, data, "delivered bytes must match original");

    // Producer is detached — give the cache-tee a brief grace window to
    // complete. The streaming buffer continues to receive bytes from the
    // producer regardless of the populator-caller's fallback (the producer
    // doesn't observe the reader's failpoint at all).
    //
    // We deliberately poll `has` rather than sleeping: poll-with-timeout
    // is the CLAUDE.md-prescribed sync primitive for tests.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut found = false;
    while std::time::Instant::now() < deadline {
        if fast_store.has(digest).await?.is_some() {
            found = true;
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(
        found,
        "cache-tee must have populated the fast tier — D.1 fallback should \
         not perturb the producer-side cache-tee path; if this fails the \
         producer is being blocked or dropped by the reader's fallback"
    );

    Ok(())
}

// -------------------------------------------------------------------------
// GatedSlowStore: a slow-store wrapper that delegates everything to an
// inner MemoryStore but blocks `get_part` on a `Notify` until the test
// explicitly releases it. Used by the waiter-path test below to keep the
// producer task in `populating_digests` so a second concurrent caller
// arrives as a WAITER instead of being a fresh populator-caller for a
// completed digest.
// -------------------------------------------------------------------------

#[derive(Debug, MetricsComponent)]
struct GatedSlowStore {
    inner: Arc<MemoryStore>,
    release_get_part: Arc<Notify>,
    get_part_arrived: Arc<Notify>,
}

impl GatedSlowStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        Self {
            inner,
            release_get_part: Arc::new(Notify::new()),
            get_part_arrived: Arc::new(Notify::new()),
        }
    }
}

default_health_status_indicator!(GatedSlowStore);

#[async_trait]
impl StoreDriver for GatedSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(&*self.inner).has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Pin::new(&*self.inner).update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Signal arrival so the test driver knows the populator-caller
        // has reached the producer entry and the entry is registered in
        // populating_digests. Then wait for the test to explicitly
        // release us before serving any bytes.
        self.get_part_arrived.notify_one();
        self.release_get_part.notified().await;
        Pin::new(&*self.inner).get_part(key, writer, offset, length).await
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

// -------------------------------------------------------------------------
// 6. WAITER-PATH coverage: the existing 5 tests trigger only the
//    populator-caller path (single caller per test). The waiter-path
//    metric, log, and splice arithmetic at `fast_slow_store.rs:5197-5230`
//    are untested — testing-czar finding.
//
// Design choice (documented per task instruction): "If a single failpoint
// can't distinguish populator vs waiter, this may require splitting the
// metric into two atomics OR using a per-caller injection."
//
// We split the metric. `streaming_buffer_reader_fallback_to_direct_total`
// now counts only the populator-caller path; the new
// `streaming_buffer_reader_fallback_to_direct_waiter_total` counts only
// the waiter path. This is more useful production observability AND
// removes the need for per-caller failpoint injection.
//
// To force a waiter to actually exist (rather than caller B becoming
// a fresh populator-caller for a completed digest), we use a
// `GatedSlowStore` that blocks the producer task in `slow.get_part`
// until the test releases it. While the producer is blocked, the
// streaming-buffer entry is registered in `populating_digests`, so the
// second caller deterministically arrives as a waiter.
//
// Mutation step: in `fast_slow_store.rs` waiter-path, comment out the
// `.streaming_buffer_reader_fallback_to_direct_waiter_total
// .fetch_add(1, ...)` line. This test must red-fail with the bespoke
// "waiter must fall back" message.
// -------------------------------------------------------------------------
#[serial(failpoints)]
#[nativelink_test]
async fn d1_waiter_path_falls_back_on_sliding_window_eviction() -> Result<(), Error> {
    // Build a FastSlowStore around a GatedSlowStore so we can hold the
    // producer task open while caller B arrives as a waiter.
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let inner_slow = MemoryStore::new(&MemorySpec::default());
    let gated = Arc::new(GatedSlowStore::new(inner_slow.clone()));
    let release = gated.release_get_part.clone();
    let arrived = gated.get_part_arrived.clone();
    let slow_store = Store::new(gated);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    let store = Store::new(fss.clone());

    let data = Bytes::from(vec![0xAAu8; 4096]);
    let digest = DigestInfo::try_new(VALID_HASH, 4096).unwrap();

    // Stage data into the inner MemoryStore (NOT through the gated
    // wrapper) so when we eventually release the gate, the inner
    // get_part returns the bytes. The gate only blocks the wrapper's
    // get_part call, not the staging update.
    inner_slow
        .clone()
        .update_oneshot(digest, data.clone())
        .await
        .err_tip(|| "setup: writing to inner slow store")?;

    let metric_pop_before = fss.streaming_buffer_reader_fallback_to_direct_total();
    let metric_waiter_before = fss.streaming_buffer_reader_fallback_to_direct_waiter_total();

    // Failpoint ON before either caller starts so both readers' first
    // `next_chunk()` call trips the synthetic sliding-window-eviction
    // error. Caller A (populator-caller) → populator-path; caller B
    // (waiter) → waiter-path.
    fail::cfg("streaming_blob_next_chunk_fail", "return").unwrap();

    // Caller A: spawn so it owns its task. The producer task it spawns
    // calls `slow_store.get_part`, which is gated — so the
    // populating_digests entry stays alive until we release.
    let store_a = store.clone();
    let task_a = tokio::spawn(async move {
        store_a.get_part_unchunked(digest, 0, None).await
    });

    // Wait until the producer task has reached `slow.get_part` (its
    // entry is now registered in populating_digests AND the producer is
    // blocked on `release_get_part.notified()`). At this point caller A
    // is the populator-caller; any concurrent caller for the same digest
    // arriving NOW will be a waiter.
    tokio::time::timeout(Duration::from_secs(5), arrived.notified())
        .await
        .expect(
            "GatedSlowStore.get_part never reached — producer didn't \
             enter the slow store within 5s; test setup wedged",
        );

    // Caller B: now arrives. spawn_populate_producer_with_role finds an
    // existing entry → returns is_populator_caller=false → caller B is
    // a WAITER. Both A and B's readers will call `next_chunk()` on the
    // streaming buffer; the failpoint trips on both.
    let store_b = store.clone();
    let task_b = tokio::spawn(async move {
        store_b.get_part_unchunked(digest, 0, None).await
    });

    // Brief grace for caller B to attach its reader before we unblock
    // the producer. Without this, caller B might still be inside
    // `populate_and_maybe_stream` setup when the producer exits and
    // cleans up populating_digests — producing a fresh populator-caller
    // path instead of the waiter path. Yield-loop with explicit timeout.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && !task_b.is_finished() {
        tokio::task::yield_now().await;
        // 200µs sleep is the established yield-fairness primitive for
        // tests (see chunked_filesystem_serialization_test). Not used
        // as synchronization — we have a deadline + finish-check.
        // TODO(D.1) replace with Notify hook on waiter attach if a
        // future refactor exposes one.
        if !task_b.is_finished() {
            // Brief yield to let task_b's spawn_populate_producer_with_role
            // run; we don't sleep on the wall clock.
            tokio::task::yield_now().await;
        }
    }

    // Release the producer. The gate-released slow.get_part returns the
    // bytes; the producer pumps them into the streaming buffer; both
    // readers' `next_chunk()` calls fire the failpoint and fall back to
    // direct slow-store reads (which are NOT gated for caller A nor B
    // since they call `slow_store.get_part` directly via the splice,
    // NOT through the producer task).
    //
    // Note: the splice fallback also goes through `slow_store.get_part`
    // (the GatedSlowStore wrapper), so the splice ITSELF is also gated.
    // Release multiple times so each caller's splice + producer can
    // proceed. The gate fires once per `notify_one` — we'll need 3
    // notifies (1 for producer, 1 for caller A's splice, 1 for caller B's
    // splice). Use `notify_waiters` to release ALL waiting receivers.
    release.notify_waiters();
    // Drain in case more arrive after notify_waiters fires.
    for _ in 0..5 {
        tokio::task::yield_now().await;
        release.notify_waiters();
    }

    let res_a = tokio::time::timeout(Duration::from_secs(10), task_a)
        .await
        .expect("caller A must complete within 10s")
        .expect("caller A task must not panic");
    let res_b = tokio::time::timeout(Duration::from_secs(10), task_b)
        .await
        .expect("caller B must complete within 10s")
        .expect("caller B task must not panic");

    fail::cfg("streaming_blob_next_chunk_fail", "off").unwrap();

    let bytes_a = res_a.expect(
        "caller A (populator-caller) must deliver full blob via D.1 \
         splice — populator-path D.1 contract violated",
    );
    let bytes_b = res_b.expect(
        "waiter must fall back to direct slow-store on sliding-window \
         eviction — D.1 contract violated for waiter path",
    );

    assert_eq!(bytes_a.len(), 4096, "caller A bytes length mismatch");
    assert_eq!(bytes_b.len(), 4096, "caller B bytes length mismatch");
    assert_eq!(bytes_a, data, "caller A bytes mismatch original");
    assert_eq!(bytes_b, data, "caller B bytes mismatch original");

    let metric_pop_after = fss.streaming_buffer_reader_fallback_to_direct_total();
    let metric_waiter_after = fss.streaming_buffer_reader_fallback_to_direct_waiter_total();

    // Populator-path metric must increment exactly once for caller A.
    assert_eq!(
        metric_pop_after - metric_pop_before,
        1,
        "populator-caller metric must increment exactly once for caller A; \
         got delta = {} (metric split is broken if this fails)",
        metric_pop_after - metric_pop_before
    );

    // The KEY assertion: waiter-path metric must increment for caller B.
    // Without the metric split this would be ambiguous; with the split
    // (Fix 4 production change), the waiter-path increment is observable
    // independently.
    assert_eq!(
        metric_waiter_after - metric_waiter_before,
        1,
        "waiter must fall back to direct slow-store on sliding-window \
         eviction — D.1 contract violated for waiter path; got waiter \
         metric delta = {} (expected 1)",
        metric_waiter_after - metric_waiter_before,
    );

    Ok(())
}
