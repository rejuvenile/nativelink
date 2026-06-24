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

//! Regression tests for #210: graceful shutdown loses MemoryStore-only blobs.
//!
//! Production CAS chain wraps a MemoryStore in front of a slow tier (Redis or
//! FilesystemStore). The pre-existing `flush_slow_writes` only drains the
//! `in_flight_slow_writes` map of fire-and-forget writes that have already
//! been spawned. Any blob that lives in the fast-tier MemoryStore but does
//! NOT have a corresponding in-flight write entry — for example, a blob
//! READ from the slow tier and back-populated into the fast tier, a blob
//! that was originally written through the fast store directly (bypassing
//! `FastSlowStore::update_oneshot`'s spawn), or any blob whose background
//! write was reported as failed via `failed_slow_writes` — does not get
//! flushed to the slow tier on SIGTERM. The MemoryStore vanishes with the
//! process and the blob is permanently lost. Workers that hold the blob's
//! AC entry then report it missing for hours as Bazel "Lost inputs" errors
//! after restart (see #206 investigation: 9904/66174 blobs missing for 6+
//! hours, 4 of 5 small-blob digests missing from Redis).
//!
//! This file asserts the new `FastSlowStore::flush_fast_to_slow_at_shutdown`
//! method (invoked from `StoreManager::flush_slow_writes` AFTER the
//! existing in-flight drain) enumerates the fast-tier MemoryStore content
//! and writes each entry to the slow tier before returning.
//!
//! Tests are written in production composition per CLAUDE.md: each test
//! wraps the unit in the deployed wrapper chain and uses
//! `tokio::time::timeout` as a deadlock detector with specific assertion
//! messages.
//!
//! Mutation step (CLAUDE.md mandatory): comment out the body of
//! `FastSlowStore::flush_fast_to_slow_at_shutdown` so it returns 0
//! immediately without copying anything; rerun this file. Both
//! `shutdown_flushes_memory_only_blobs_to_slow_tier` and the
//! over-action test must panic with their specific messages.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, SizePartitioningSpec,
    StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::Notify;

/// Generous deadlock-detector timeout. A correctly-wired flush completes in
/// milliseconds; 5 seconds protects against slow CI runners without masking
/// real wedges. `tokio::time::Elapsed` from a too-short timeout would be
/// indistinguishable from a real hang and mask the bug per CLAUDE.md.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Each blob is unique by digest so a synthetic SHA-like hex distinguishes
/// them. The hash is just a unique label here — VerifyStore is intentionally
/// NOT in the chain for this set of tests because we want to write directly
/// into the fast tier, which `verify_size: false`/`verify_hash: false` would
/// permit but is needless complexity for a flush-only test.
fn unique_digest(idx: u64, size: u64) -> DigestInfo {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&idx.to_le_bytes());
    DigestInfo::new(bytes, size)
}

/// Build a `FastSlowStore { fast: MemoryStore, slow: <user-supplied probe> }`
/// matching the production-fast-tier shape (no upper Verify/ExistenceCache
/// wrapping — those are exercised in the production-chain test below). The
/// caller-supplied slow store gets wrapped into a `Store` so the test can
/// reach into it after the flush to assert what landed.
fn build_fast_slow_with_slow_probe(slow: Store) -> (Arc<FastSlowStore>, Store) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast.clone(),
        slow.clone(),
    );
    (fast_slow, fast)
}

/// Test 1 (under-action): a FastSlowStore with N blobs in its fast-tier
/// MemoryStore but ZERO in its slow tier MUST have all N blobs in the slow
/// tier after `flush_fast_to_slow_at_shutdown` returns within the deadline.
///
/// The "blobs in fast tier but not slow tier" condition is set up by writing
/// directly to `fast_store` (bypassing `FastSlowStore::update_oneshot`'s
/// background-write spawn). This models the production case where a blob
/// landed in MemoryStore via a back-populated read or an
/// in-flight-write-marked-failed path, leaving nothing in the slow tier.
#[nativelink_test]
async fn shutdown_flushes_memory_only_blobs_to_slow_tier() -> Result<(), Error> {
    let slow_probe = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(slow_probe.clone());

    const N: usize = 5;
    let mut digests = Vec::with_capacity(N);
    let mut payloads = Vec::with_capacity(N);
    for i in 0..N {
        let payload = vec![0xA5u8; 64 + i * 16];
        let digest = unique_digest(i as u64, payload.len() as u64);
        // Direct write to the fast tier ONLY — bypasses FastSlowStore's
        // background slow-write spawn so we can simulate the
        // MemoryStore-only state.
        fast_store
            .update_oneshot(digest, Bytes::from(payload.clone()))
            .await?;
        // durability-ack v3 Change A: register at-risk (not-yet-durable) so
        // the not-yet-durable filter flushes it (models production state).
        assert!(fast_slow.requeue_failed_push(digest));
        digests.push(digest);
        payloads.push(payload);
    }

    // Sanity: slow tier really has zero entries for these digests pre-flush.
    for digest in &digests {
        let exists = slow_probe.has(*digest).await?;
        assert!(
            exists.is_none(),
            "test setup error: slow tier already has digest {digest:?} before flush"
        );
    }

    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK DETECTED: flush_fast_to_slow_at_shutdown did not return within 5s. \
         #210 graceful-shutdown MUST flush MemoryStore-only blobs to slow tier; \
         the call hung instead of completing.",
    );

    assert_eq!(
        unflushed, 0,
        "#210 graceful-shutdown MUST flush MemoryStore-only blobs to slow tier; \
         got {} blobs not flushed (expected 0)",
        unflushed,
    );

    // Verify each blob is now in the slow tier with correct contents.
    for (digest, payload) in digests.iter().zip(payloads.iter()) {
        let stored = slow_probe
            .get_part_unchunked(*digest, 0, None)
            .await
            .err_tip(|| format!("blob {digest:?} missing from slow tier post-flush"))?;
        assert_eq!(
            stored.as_ref(),
            payload.as_slice(),
            "#210 graceful-shutdown MUST flush MemoryStore-only blobs to slow tier; \
             slow-tier digest {digest:?} content does not match what was in fast tier",
        );
    }

    Ok(())
}

/// Test 2 (under-action through production composition): the same flush
/// invariant must hold when the FastSlowStore is wrapped under VerifyStore +
/// ExistenceCacheStore + SizePartitioningStore — the actual deployed shape.
///
/// Per CLAUDE.md "test in production composition" rule: a wrapper layer that
/// fails to forward the new flush call (e.g. SizePartitioningStore not
/// invoking flush on both children) would be invisible at the FastSlowStore
/// unit boundary but break in production. This test catches that.
#[nativelink_test]
async fn shutdown_flush_propagates_through_production_chain() -> Result<(), Error> {
    // Build a small production-shaped chain. We use two FastSlowStore tiers
    // (lower for ≤16 KiB, upper for >16 KiB) per the deployed
    // SizePartitioning shape, both with MemoryStore probes for the slow side
    // so the test can assert post-flush state without hitting Redis or disk.
    const PARTITION_SIZE: u64 = 16 * 1024;

    let upper_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        upper_fast.clone(),
        upper_slow.clone(),
    );

    let lower_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        lower_fast.clone(),
        lower_slow.clone(),
    );

    let size_part = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: PARTITION_SIZE,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        Store::new(lower_fss.clone()),
        Store::new(upper_fss.clone()),
    );

    let _cache = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1024,
                ..Default::default()
            }),
            log_not_found_at_info: false,
        },
        Store::new(size_part),
    );
    let _verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        Store::new(_cache),
    );

    // Pre-load each FastSlowStore's fast tier with a unique digest. We
    // bypass the wrapper chain on the WRITE side (writing directly to the
    // fast store) to deterministically install the
    // MemoryStore-only-no-in-flight state.
    let upper_payload = vec![0x55u8; PARTITION_SIZE as usize + 32];
    let upper_digest = unique_digest(1001, upper_payload.len() as u64);
    upper_fast
        .update_oneshot(upper_digest, Bytes::from(upper_payload.clone()))
        .await?;
    assert!(upper_fss.requeue_failed_push(upper_digest)); // v3 Change A: at-risk

    let lower_payload = vec![0xAAu8; 256];
    let lower_digest = unique_digest(1002, lower_payload.len() as u64);
    lower_fast
        .update_oneshot(lower_digest, Bytes::from(lower_payload.clone()))
        .await?;
    assert!(lower_fss.requeue_failed_push(lower_digest)); // v3 Change A: at-risk

    // Sanity: the slow tiers really start empty for these digests.
    assert!(upper_slow.has(upper_digest).await?.is_none());
    assert!(lower_slow.has(lower_digest).await?.is_none());

    // Flush each FastSlowStore. (The StoreManager.flush_slow_writes
    // wrapper is what production calls; this test exercises the per-store
    // primitive directly so a failure points at FastSlowStore, not at the
    // walker.)
    let upper_remaining = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        upper_fss.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect("DEADLOCK: upper-tier flush did not return");
    let lower_remaining = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        lower_fss.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect("DEADLOCK: lower-tier flush did not return");

    assert_eq!(
        upper_remaining, 0,
        "#210 production-chain flush MUST propagate to inner upper-tier \
         FastSlowStore; got {upper_remaining} unflushed",
    );
    assert_eq!(
        lower_remaining, 0,
        "#210 production-chain flush MUST propagate to inner lower-tier \
         FastSlowStore; got {lower_remaining} unflushed",
    );

    // Both blobs are present in their slow tier post-flush.
    let upper_seen = upper_slow.get_part_unchunked(upper_digest, 0, None).await?;
    assert_eq!(
        upper_seen.as_ref(),
        upper_payload.as_slice(),
        "#210 upper-tier slow store missing the flushed blob",
    );
    let lower_seen = lower_slow.get_part_unchunked(lower_digest, 0, None).await?;
    assert_eq!(
        lower_seen.as_ref(),
        lower_payload.as_slice(),
        "#210 lower-tier slow store missing the flushed blob",
    );

    Ok(())
}

// Test 3 (deadline-bound over-action) REMOVED by durability-ack v3 Change A:
// the `deadline: Option<Duration>` parameter is gone (R2 — flush is
// unconditionally unbounded). The 'drain to completion despite a slow tier'
// half of the contract is now covered by
// `shutdown_flush_unbounded_drains_despite_slow_tier` (test 7).

/// Test 6 (#210 UNBOUNDED — the production cure): with NO deadline (`None`),
/// the flush MUST drain EVERY MemoryStore-only blob to the slow tier and
/// return `0` unflushed. This is the operator directive: "give shutdown
/// unlimited time to flush blobs to redis and to disk." The returned count is
/// `errored + deadline_exceeded`; with no deadline and no errors it MUST be 0,
/// which directly guards the production `flushed=0 deadline_exceeded=827204`
/// data-loss (827,204 SMALL_CAS_CACHED blobs lost at the 2026-06-23 17:38
/// restart because the 30 s deadline fired mid-drain).
///
/// Mutation step (CLAUDE.md mandate): re-introduce a finite per-iter deadline
/// in `flush_fast_to_slow_at_shutdown` (e.g. hardcode `Some(Duration::ZERO)`
/// in place of the `None` branch, or `break` the loop early) — the
/// `assert_eq!(unflushed, 0, ...)` MUST red-fail with the bespoke message
/// `"#210 UNBOUNDED flush must drain ALL memory-only blobs — data-loss
/// regression"`.
#[nativelink_test]
async fn shutdown_flush_unbounded_drains_all_blobs() -> Result<(), Error> {
    let slow_probe = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(slow_probe.clone());

    // More blobs than any bounded flush concurrency, so a correct unbounded
    // drain must process multiple waves — not just the first.
    const N: usize = 250;
    let mut digests = Vec::with_capacity(N);
    let mut payloads = Vec::with_capacity(N);
    for i in 0..N {
        let payload = vec![(i & 0xFF) as u8; 48 + (i % 7)];
        let digest = unique_digest(5000 + i as u64, payload.len() as u64);
        fast_store
            .update_oneshot(digest, Bytes::from(payload.clone()))
            .await?;
        // durability-ack v3 Change A: register at-risk (not-yet-durable) so
        // the not-yet-durable filter flushes it (models production state).
        assert!(fast_slow.requeue_failed_push(digest));
        digests.push(digest);
        payloads.push(payload);
    }

    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK DETECTED: unbounded flush_fast_to_slow_at_shutdown() did \
         not return within 5s for 250 trivial blobs; a correct unbounded drain \
         of a handful of in-memory blobs finishes in milliseconds.",
    );

    // unflushed == 0 ⇒ flushed == N (skipped_already == 0 here because the
    // slow tier started empty). This is the counter the production log
    // reported as `flushed=0`; here it MUST account for every blob.
    assert_eq!(
        unflushed, 0,
        "#210 UNBOUNDED flush must drain ALL memory-only blobs — data-loss \
         regression: {unflushed} of {N} blobs left unflushed with no deadline",
    );

    // Every blob is present in the slow tier with correct content.
    for (digest, payload) in digests.iter().zip(payloads.iter()) {
        let stored = slow_probe
            .get_part_unchunked(*digest, 0, None)
            .await
            .err_tip(|| format!("blob {digest:?} missing from slow tier post-unbounded-flush"))?;
        assert_eq!(
            stored.as_ref(),
            payload.as_slice(),
            "#210 UNBOUNDED flush: slow-tier digest {digest:?} content mismatch",
        );
    }
    Ok(())
}

/// Test 7 (#210 UNBOUNDED vs SLOW TIER — proves the DEADLINE, not a structural
/// cap, bounded the production drain): inject a per-`update_oneshot` latency on
/// the slow tier and flush with NO deadline (`None`). ALL N blobs MUST still
/// land. If a finite deadline (or any internal structural cap on processed
/// count) survived, a slow tier would strand blobs exactly as production did.
/// Because the flush is unbounded, the only thing that can stop it is running
/// out of blobs — so all N drain regardless of how slow each write is.
///
/// The injected latency is small (8 ms) and N modest (40) so the test stays
/// well under the 5 s deadlock-detector while still forcing many serialized
/// slow-tier round-trips that a finite deadline would have truncated.
#[nativelink_test]
async fn shutdown_flush_unbounded_drains_despite_slow_tier() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_call_count = Arc::new(AtomicUsize::new(0));
    let probe_arc: Arc<dyn StoreDriver> = Arc::new(SlowSlowProbe {
        inner: inner.clone(),
        per_update_delay: Duration::from_millis(8),
        update_count: slow_call_count.clone(),
    });
    let slow_probe = Store::new(probe_arc);
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(slow_probe.clone());

    const N: usize = 40;
    let mut digests = Vec::with_capacity(N);
    for i in 0..N {
        let payload = vec![(i & 0xFF) as u8; 64];
        let digest = unique_digest(6000 + i as u64, payload.len() as u64);
        fast_store
            .update_oneshot(digest, Bytes::from(payload))
            .await?;
        // durability-ack v3 Change A: register at-risk (not-yet-durable) so
        // the not-yet-durable filter flushes it (models production state).
        assert!(fast_slow.requeue_failed_push(digest));
        digests.push(digest);
    }

    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK DETECTED: unbounded flush did not return within 5s despite a \
         slow (8 ms/write) slow tier; unbounded flush must still terminate once \
         all blobs are drained.",
    );

    assert_eq!(
        unflushed, 0,
        "#210 UNBOUNDED-vs-slow-tier: a slow tier MUST NOT strand blobs when \
         the deadline is unbounded; {unflushed} of {N} left unflushed — this is \
         the production `deadline_exceeded` data-loss the directive removes",
    );

    // Directly guard the `flushed` counter that production logged as `0`:
    // EXACTLY N slow-tier writes must have been issued (the slow tier started
    // empty, so none are skip-existing). A counter/loop bug that under-counts
    // or short-circuits writes shows up here as != N.
    let writes = slow_call_count.load(Ordering::SeqCst);
    assert_eq!(
        writes, N,
        "#210 flushed-counter guard: unbounded flush must issue exactly {N} \
         slow-tier writes (the production log showed flushed=0); got {writes}",
    );

    // Confirm the slow tier physically received every blob.
    for digest in &digests {
        let exists = inner
            .has(*digest)
            .await?
            .ok_or_else(|| make_err!(Code::NotFound, "blob missing post-flush: {digest:?}"))?;
        let _ = exists;
    }
    Ok(())
}

/// Slow probe: an in-process slow store whose `update`/`update_oneshot`
/// wait `per_update_delay` before delegating to `inner`. Used by the
/// over-action test to drive the flush past its deadline.
#[derive(Debug, MetricsComponent)]
struct SlowSlowProbe {
    inner: Store,
    per_update_delay: Duration,
    update_count: Arc<AtomicUsize>,
}

default_health_status_indicator!(SlowSlowProbe);

#[async_trait]
impl StoreDriver for SlowSlowProbe {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        tokio::time::sleep(self.per_update_delay).await;
        self.update_count.fetch_add(1, Ordering::SeqCst);
        self.inner.update(key, reader, upload_size).await
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        tokio::time::sleep(self.per_update_delay).await;
        self.update_count.fetch_add(1, Ordering::SeqCst);
        self.inner.update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
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
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_store_driver())
    }
}

/// Test 4 (over-action / per-entry tolerance): one failing slow-tier write
/// MUST NOT abort the flush. The remaining entries should still be flushed.
///
/// The probe rejects every Nth write with an error; the flush must log and
/// continue. We assert (a) flush returns successfully (no panic), (b) the
/// non-failing entries DID land in the slow tier.
#[nativelink_test]
async fn shutdown_flush_continues_past_per_entry_errors() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let probe_arc: Arc<dyn StoreDriver> = Arc::new(EveryOtherFailsProbe {
        inner: inner.clone(),
        call_count: Arc::new(AtomicUsize::new(0)),
    });
    let probe = Store::new(probe_arc);
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(probe.clone());

    const N: usize = 6;
    let mut digests = Vec::with_capacity(N);
    let mut payloads = Vec::with_capacity(N);
    for i in 0..N {
        let payload = vec![(i & 0xFF) as u8; 32];
        let digest = unique_digest(3000 + i as u64, payload.len() as u64);
        fast_store
            .update_oneshot(digest, Bytes::from(payload.clone()))
            .await?;
        // durability-ack v3 Change A: register at-risk (not-yet-durable) so
        // the not-yet-durable filter flushes it (models production state).
        assert!(fast_slow.requeue_failed_push(digest));
        digests.push(digest);
        payloads.push(payload);
    }

    let _ = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK: flush_fast_to_slow_at_shutdown did not return when slow \
         tier returns errors. #210 contract: per-entry errors MUST be logged \
         and skipped, never propagated to abort the flush.",
    );

    // At least half of the entries must have landed (the non-failing ones).
    // We check the inner MemoryStore directly so the assertion measures
    // post-flush slow-tier state, not whether the failing probe happened
    // to write before failing.
    let mut succeeded = 0usize;
    for digest in &digests {
        if inner.has(*digest).await?.is_some() {
            succeeded += 1;
        }
    }
    assert!(
        succeeded >= N / 2,
        "#210 per-entry tolerance: flush should have written ≥{} blobs to \
         slow tier despite per-entry errors; only {succeeded} landed",
        N / 2,
    );
    Ok(())
}

/// Probe whose `update_oneshot` fails on every other call. Used to verify
/// per-entry error tolerance in the flush loop.
#[derive(Debug, MetricsComponent)]
struct EveryOtherFailsProbe {
    inner: Store,
    call_count: Arc<AtomicUsize>,
}

default_health_status_indicator!(EveryOtherFailsProbe);

#[async_trait]
impl StoreDriver for EveryOtherFailsProbe {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);
        if n % 2 == 0 {
            return Err(make_err!(
                Code::Internal,
                "EveryOtherFailsProbe: synthetic per-entry failure #{n}"
            ));
        }
        self.inner.update(key, reader, upload_size).await
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);
        if n % 2 == 0 {
            return Err(make_err!(
                Code::Internal,
                "EveryOtherFailsProbe: synthetic per-entry failure #{n}"
            ));
        }
        self.inner.update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
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
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_store_driver())
    }
}

// Test 5 (`shutdown_flush_skips_blobs_already_in_slow_tier`) + its
// `NoUpdateExpectedProbe` were DELETED by durability-ack v3 Stage 1 fix-up
// (pair-b T1). They are dead guards under the Change A filter: the v3 flush
// skips a durable resident via AT-RISK-ABSENCE (`if !at_risk.contains(&digest)`
// in `flush_fast_to_slow_at_shutdown`), NOT via a `slow.has()` pre-check (which
// Change A removed). Test 5 pre-loaded the slow tier and wrote the SAME digest
// to the fast tier but NEVER registered it at-risk, so the flush skipped it on
// at-risk-absence and the probe's `update_oneshot` panic guard was unreachable
// — its mutation (drop the filter) would not red-fail. The genuine "skip a
// durable resident" semantics are covered by
// `shutdown_flush_chunked_safe_test::durable_resident_skipped_via_in_memory_state`,
// which registers a SECOND at-risk blob so the per-key drain actually runs past
// the filter, making its mutation (`if false && at_risk.contains(..)`) red-fail
// with a bespoke `NoUpdateExpected` panic. Verified that replacement covers the
// semantics before deleting (fix-up report, item 3).

/// BLOCK-1 regression test (#335 follow-up): `StoreManager::flush_slow_writes`
/// MUST descend the production wrapper chain to find the inner
/// `FastSlowStore` and propagate the Phase-2 flush to it. Production
/// composition is `ExistenceCacheStore → VerifyStore →
/// SizePartitioningStore(16384) → FastSlowStore`. The previous local
/// walker descended `inner_store(None)`, which terminates at
/// `SizePartitioningStore` (its `inner_store(None)` returns `self`),
/// so `flush_slow_writes` silently logged "no FastSlowStore registered;
/// skipping" on every SIGTERM — defeating the #210 graceful-shutdown
/// fix at the walker layer.
///
/// This test seeds the fast tier with a unique blob, registers the
/// fully-wrapped chain via `StoreManager::add_store`, and calls
/// `StoreManager::flush_slow_writes`. The Phase-2 `MemoryStore`-only
/// drain MUST land the blob in the slow tier — and that only happens
/// if the walker successfully descends through ECS → VS → SP → FSS.
///
/// Mutation step (CLAUDE.md mandate): change the
/// `store.inner_store(Some(synthetic_large_key()))` argument back to
/// `Option::<StoreKey<'_>>::None` in `store_manager.rs::flush_slow_writes`
/// (the `find_fast_slow_via_chain` callsites). With `None`,
/// `SizePartitioningStore::inner_store` returns `self`, the walker
/// returns None for the upper-arm FSS, the Phase-2 drain is a silent
/// no-op, the slow tier never sees the blob, and the `seen.expect(...)`
/// below red-fails with the bespoke message
/// `"BLOCK-1 regression: StoreManager walker failed to descend
/// production composition — Phase-2 flush did not propagate; slow tier
/// is missing the blob"`.
#[nativelink_test]
async fn store_manager_flush_descends_production_composition() -> Result<(), Error> {
    const PARTITION_SIZE: u64 = 16 * 1024;

    // Build the production-shaped upper-arm FastSlowStore (the one the
    // walker MUST find).
    let upper_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            // 0 = uncapped (test default); MemoryStore slow tier is
            // exempt from Path C's required-cap check.
            slow_writes_in_flight_max_bytes: 0,
        },
        upper_fast.clone(),
        upper_slow.clone(),
    );

    // Build a lower-arm FastSlowStore (production's lower SizePartitioning
    // arm is itself a FastSlowStore — see `prod-server.json5`). Required so
    // SizePartitioning's `stable_delegation = Many { children: [lower,
    // upper] }` doesn't trip a debug_assert when the StoreManager walks
    // the chain.
    let lower_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            // 0 = uncapped (test default); MemoryStore slow tier is
            // exempt from Path C's required-cap check.
            slow_writes_in_flight_max_bytes: 0,
        },
        lower_fast,
        lower_slow,
    );

    let size_part = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: PARTITION_SIZE,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        Store::new(lower_fss),
        Store::new(upper_fss.clone()),
    );

    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        Store::new(size_part),
    );

    let cache = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1024,
                ..Default::default()
            }),
            log_not_found_at_info: false,
        },
        Store::new(verify),
    );

    // Seed the fast tier with a unique upper-arm blob (size > partition
    // threshold so SizePartitioning routes upper). We bypass the wrapper
    // chain on the WRITE side (writing directly to the upper-arm
    // MemoryStore) to install the deterministic fast-only state that
    // Phase-2 of `StoreManager::flush_slow_writes` is supposed to
    // discover via the walker.
    let payload = vec![0x42u8; PARTITION_SIZE as usize + 64];
    let digest = unique_digest(2026, payload.len() as u64);
    upper_fast
        .update_oneshot(digest, Bytes::from(payload.clone()))
        .await?;
    assert!(upper_fss.requeue_failed_push(digest)); // v3 Change A: at-risk

    // Sanity: slow tier starts empty for this digest.
    assert!(upper_slow.has(digest).await?.is_none());

    // Wire the production-shaped chain into a StoreManager (matches
    // `default_store_factory.rs`'s `add_store` calls at startup).
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("cas_STORE", Store::new(cache));

    // Drive the StoreManager's flush — this is the SIGTERM-time path
    // (`nativelink.rs:2079`). Wrapped in a tokio timeout as the
    // deadlock detector per CLAUDE.md.
    tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        store_manager.flush_slow_writes(NO_DEADLOCK_TIMEOUT),
    )
    .await
    .expect(
        "StoreManager::flush_slow_writes must not deadlock — \
         walker descent contract violated (see BLOCK-1 regression doc)",
    );

    // The load-bearing assertion: post-flush the slow tier MUST hold
    // the blob. If the walker bottomed out at SizePartitioningStore
    // (the BLOCK-1 bug) Phase-2 was a silent no-op and the slow tier
    // is empty. Bespoke message names the exact failure mode per
    // CLAUDE.md "specific .expect" rule.
    let seen = upper_slow.get_part_unchunked(digest, 0, None).await;
    let bytes = seen.expect(
        "BLOCK-1 regression: StoreManager walker failed to descend \
         production composition — Phase-2 flush did not propagate; \
         slow tier is missing the blob",
    );
    assert_eq!(
        bytes.as_ref(),
        payload.as_slice(),
        "BLOCK-1 regression: slow-tier bytes after StoreManager \
         flush do not match the seeded fast-tier bytes"
    );

    Ok(())
}

/// FORWARD-GUARANTEE — `in_flight_slow_writes`-only at-risk member, exercised
/// through the REAL `FastSlowStore::update` spawn path (durability-ack v3
/// Stage 1, pair-a F1 / red-team / assumption-auditor convergent).
///
/// The shutdown flush filters the whole-tier scan through the 3-set
/// acked-not-durable union
/// (`in_flight_slow_writes` ∪ `chunked_in_flight_digests` ∪
/// `failed_slow_writes`). The existing flush tests only register at-risk via
/// `requeue_failed_push` (→ `failed_slow_writes`) or the chunked map; NONE
/// exercised an `in_flight_slow_writes`-ONLY member produced by the real
/// `update`-spawn path. That is the path-1 the keystone proof names: the
/// legacy `update` inserts `in_flight_slow_writes` BEFORE the bg slow-write
/// spawn and BEFORE `update()` returns Ok (the ack), removing it only in the
/// bg task's Ok arm (= durable). This test closes pair-b's named gap.
///
/// Setup drives a real streaming `update()` whose slow-tier bg write is
/// BLOCKED on a `Notify`, so at flush time the digest is registered in
/// `in_flight_slow_writes` ONLY (not in `failed_slow_writes` or
/// `chunked_in_flight_digests`) and its bytes are in the fast tier. The flush
/// MUST drain it. The slow tier's bg `update` is blocked; the flush's
/// `update_oneshot` is allowed through (it is the only path that lands the
/// blob during the test).
///
/// Mutation step (CLAUDE.md mandate): in
/// `FastSlowStore::flush_fast_to_slow_at_shutdown`'s at-risk-union
/// construction, drop the `in_flight_slow_writes` member (comment out the
/// `for k in self.in_flight_slow_writes.lock().keys() { ... }` loop). With
/// that member gone the digest is no longer at-risk, the flush SKIPS it, and
/// the `assert_eq!(unflushed, 0, ...)` / slow-tier presence assertion
/// red-fails with the bespoke message
/// `"FORWARD-GUARANTEE: an in_flight_slow_writes-only blob (real update-spawn
/// path) MUST be flushed at shutdown — the in_flight_slow_writes at-risk
/// member was dropped"`.
#[nativelink_test]
async fn in_flight_slow_writes_only_blob_flushed_via_real_update_path() -> Result<(), Error> {
    let slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let release = Arc::new(Notify::new());
    let bg_update_entered = Arc::new(Notify::new());
    let probe_arc: Arc<dyn StoreDriver> = Arc::new(BlockingUpdateProbe {
        inner: slow_inner.clone(),
        release: release.clone(),
        update_entered: bg_update_entered.clone(),
    });
    let slow_probe = Store::new(probe_arc);
    // The fast tier is written via the real `update()` below, not directly.
    let (fast_slow, _fast_store) = build_fast_slow_with_slow_probe(slow_probe.clone());

    let payload = vec![0xB6u8; 4096];
    let digest = unique_digest(11_001, payload.len() as u64);

    // Drive a REAL streaming `FastSlowStore::update`: feed the bytes through a
    // buf_channel producer while the FSS consumes. `update()` writes the fast
    // tier, inserts `in_flight_slow_writes`, spawns the (blocked) bg slow
    // write, and returns Ok — modelling an acked-not-durable blob.
    let (mut tx, rx) = make_buf_channel_pair();
    let payload_for_send = payload.clone();
    let send_handle = tokio::spawn(async move {
        tx.send(Bytes::from(payload_for_send)).await?;
        tx.send_eof()?;
        Result::<(), Error>::Ok(())
    });
    tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        Pin::new(fast_slow.as_ref()).update(
            digest.into(),
            rx,
            UploadSizeInfo::ExactSize(payload.len() as u64),
        ),
    )
    .await
    .expect("DEADLOCK: FastSlowStore::update did not return")?;
    send_handle
        .await
        .expect("producer task panicked")
        .err_tip(|| "producer send failed")?;

    // Wait until the bg slow-write task has actually entered the probe's
    // `update` (so it is parked on `release`, holding the digest in-flight)
    // — channel/notify synchronization, NOT a sleep.
    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, bg_update_entered.notified())
        .await
        .expect("bg slow-write task never entered the blocking probe update");

    // The digest must be at-risk via `in_flight_slow_writes` ONLY.
    assert!(
        fast_slow.in_flight_contains_for_test(&digest),
        "setup: digest must be registered in in_flight_slow_writes by the real \
         update-spawn path"
    );
    assert!(
        !fast_slow.failed_slow_writes_contains(&digest),
        "setup: digest must NOT be in failed_slow_writes (this test exercises \
         the in_flight_slow_writes-only at-risk class)"
    );

    // Sanity: slow tier (inner) is still empty (bg write is blocked).
    assert!(
        slow_inner.has(digest).await?.is_none(),
        "setup: slow tier must be empty before flush (bg write is blocked)"
    );

    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK: flush_fast_to_slow_at_shutdown did not return for a single \
         in_flight_slow_writes-only blob; a correct flush finishes in milliseconds.",
    );

    assert_eq!(
        unflushed, 0,
        "FORWARD-GUARANTEE: an in_flight_slow_writes-only blob (real update-spawn \
         path) MUST be flushed at shutdown — the in_flight_slow_writes at-risk \
         member was dropped; got {unflushed} unflushed (expected 0)",
    );

    let stored = slow_inner
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| {
            "FORWARD-GUARANTEE: an in_flight_slow_writes-only blob (real \
             update-spawn path) MUST be flushed at shutdown — the \
             in_flight_slow_writes at-risk member was dropped; blob missing from \
             slow tier post-flush"
        })?;
    assert_eq!(
        stored.as_ref(),
        payload.as_slice(),
        "in_flight_slow_writes-only flush: slow-tier content mismatch",
    );

    // Release the blocked bg write so the spawned task does not leak.
    release.notify_waiters();
    Ok(())
}

/// Slow-tier probe whose streaming `update` (the bg slow-write path) BLOCKS on
/// `release` after announcing entry via `update_entered`, while `update_oneshot`
/// (the shutdown-flush path) delegates immediately. Lets a test hold a digest
/// in `in_flight_slow_writes` across the flush without a sleep.
#[derive(Debug, MetricsComponent)]
struct BlockingUpdateProbe {
    inner: Store,
    // No `#[metric]` attribute: the derive only publishes annotated fields, so
    // these test sync primitives (which do not implement MetricsComponent) are
    // correctly left out of the metrics tree.
    release: Arc<Notify>,
    update_entered: Arc<Notify>,
}

default_health_status_indicator!(BlockingUpdateProbe);

#[async_trait]
impl StoreDriver for BlockingUpdateProbe {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Announce entry, then park until the test releases us. We do NOT
        // consume the reader or write to `inner`: the test only needs the
        // digest to stay registered in `in_flight_slow_writes` (which the FSS
        // already did before spawning this bg task) across the flush. The
        // flush itself lands the bytes via `update_oneshot`.
        self.update_entered.notify_waiters();
        self.release.notified().await;
        Ok(())
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        self.inner.update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
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
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_store_driver())
    }
}
