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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nativelink_config::stores::{
    FastSlowSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use serial_test::serial;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// sha256 of vec![0x42; 4096]. Verified by `python3 -c
/// "import hashlib; print(hashlib.sha256(b'\x42'*4096).hexdigest())"`.
/// We use sha256 because `default_digest_hasher_func()` defaults to
/// sha256 in tests (production overrides to blake3); see
/// `digest_hasher.rs:53`.
const SHA256_HASH_4096_42S: &str =
    "725bcd6c66d02acf6ebeab9c92410e010ea22e336876256aaf05a211f4ce1902";

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
