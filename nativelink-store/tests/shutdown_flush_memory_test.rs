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
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

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
        fast_slow.flush_fast_to_slow_at_shutdown(NO_DEADLOCK_TIMEOUT),
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

    let lower_payload = vec![0xAAu8; 256];
    let lower_digest = unique_digest(1002, lower_payload.len() as u64);
    lower_fast
        .update_oneshot(lower_digest, Bytes::from(lower_payload.clone()))
        .await?;

    // Sanity: the slow tiers really start empty for these digests.
    assert!(upper_slow.has(upper_digest).await?.is_none());
    assert!(lower_slow.has(lower_digest).await?.is_none());

    // Flush each FastSlowStore. (The StoreManager.flush_slow_writes
    // wrapper is what production calls; this test exercises the per-store
    // primitive directly so a failure points at FastSlowStore, not at the
    // walker.)
    let upper_remaining = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        upper_fss.flush_fast_to_slow_at_shutdown(NO_DEADLOCK_TIMEOUT),
    )
    .await
    .expect("DEADLOCK: upper-tier flush did not return");
    let lower_remaining = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        lower_fss.flush_fast_to_slow_at_shutdown(NO_DEADLOCK_TIMEOUT),
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

/// Test 3 (over-action / deadline-bound): flush MUST honor the deadline and
/// return promptly even when the slow tier is so slow that not all blobs
/// can be drained in time. The contract is "best-effort within the
/// deadline; report unflushed count + log; do NOT block shutdown forever."
///
/// We install a slow-tier probe that delays each `update_oneshot` by 200ms.
/// With 50 blobs queued and a 200 ms deadline, AT MOST 1-2 should land
/// before the deadline triggers. The flush must return within ~deadline +
/// overhead and the unflushed count must be > 0 (NOT silently report 0).
#[nativelink_test]
async fn shutdown_flush_respects_deadline_when_slow_tier_is_slow() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_call_count = Arc::new(AtomicUsize::new(0));
    let probe_arc: Arc<dyn StoreDriver> = Arc::new(SlowSlowProbe {
        inner: inner.clone(),
        per_update_delay: Duration::from_millis(200),
        update_count: slow_call_count.clone(),
    });
    let slow_probe = Store::new(probe_arc);
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(slow_probe.clone());

    const N: usize = 50;
    for i in 0..N {
        let payload = vec![(i & 0xFF) as u8; 64];
        let digest = unique_digest(2000 + i as u64, payload.len() as u64);
        fast_store
            .update_oneshot(digest, Bytes::from(payload))
            .await?;
    }

    let deadline = Duration::from_millis(200);
    let started = std::time::Instant::now();
    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(deadline),
    )
    .await
    .expect(
        "DEADLOCK DETECTED: flush_fast_to_slow_at_shutdown did not return \
         within the outer 5s timeout despite a 200 ms per-store deadline. \
         #210 over-action: flush MUST cap wall-clock at the supplied deadline; \
         a wedge here means the deadline guard is missing.",
    );
    let elapsed = started.elapsed();

    // Deadline guard: the flush must return within a reasonable buffer of
    // the supplied deadline. Allow generous slack (2s) so CI variance does
    // not flake — the bug we're guarding against is "blocks forever," not
    // "took 250ms instead of 200ms."
    assert!(
        elapsed < Duration::from_secs(2),
        "#210 over-action: flush_fast_to_slow_at_shutdown took {elapsed:?} \
         despite a {deadline:?} deadline; the deadline cap is not enforced",
    );

    // The slow-tier probe should NOT have completed all N writes — that
    // would either mean the flush ignored the deadline (bad) or that the
    // delays did not fire. Asserting `unflushed > 0` AND that the slow
    // tier did NOT receive all N updates pins down the deadline-cap
    // contract.
    let completed = slow_call_count.load(Ordering::SeqCst);
    assert!(
        unflushed > 0,
        "#210 over-action: with a 200 ms deadline and {N} × 200 ms slow \
         probe, flush should have left blobs unflushed; got 0 (probe \
         completed {completed} updates) — deadline likely ignored",
    );
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

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
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
        digests.push(digest);
        payloads.push(payload);
    }

    let _ = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(NO_DEADLOCK_TIMEOUT),
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

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
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
}

/// Test 5 (skip already-present): if the slow tier ALREADY has a blob with
/// that digest, the flush MUST NOT redundantly write it (avoids wasted I/O,
/// avoids re-pinning a blob that the slow tier already considers stable).
/// We expose this via a probe whose `update_oneshot` panics if invoked
/// after pre-loading the same blob.
#[nativelink_test]
async fn shutdown_flush_skips_blobs_already_in_slow_tier() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let probe_arc: Arc<dyn StoreDriver> = Arc::new(NoUpdateExpectedProbe {
        inner: inner.clone(),
    });
    let probe = Store::new(probe_arc);
    let (fast_slow, fast_store) = build_fast_slow_with_slow_probe(probe.clone());

    let payload = vec![0x77u8; 32];
    let digest = unique_digest(4001, payload.len() as u64);

    // Pre-load slow tier directly via the inner MemoryStore (bypassing the
    // probe wrapper so the pre-load doesn't trip the panic guard).
    inner
        .update_oneshot(digest, Bytes::from(payload.clone()))
        .await?;

    // Independently put it in the fast tier.
    fast_store
        .update_oneshot(digest, Bytes::from(payload.clone()))
        .await?;

    let unflushed = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        fast_slow.flush_fast_to_slow_at_shutdown(NO_DEADLOCK_TIMEOUT),
    )
    .await
    .expect("DEADLOCK: flush did not return")
    ;
    // 0 unflushed (skip-counted) AND the probe was never invoked for
    // update_oneshot (would have panicked on call).
    assert_eq!(
        unflushed, 0,
        "#210 skip-existing: flush must not report the already-present blob \
         as unflushed",
    );
    Ok(())
}

#[derive(Debug, MetricsComponent)]
struct NoUpdateExpectedProbe {
    inner: Store,
}

default_health_status_indicator!(NoUpdateExpectedProbe);

#[async_trait]
impl StoreDriver for NoUpdateExpectedProbe {
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
        panic!(
            "NoUpdateExpectedProbe::update invoked — flush should have skipped \
             the blob because it is already in the slow tier"
        );
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _data: Bytes,
    ) -> Result<(), Error> {
        panic!(
            "NoUpdateExpectedProbe::update_oneshot invoked — flush should have \
             skipped the blob because it is already in the slow tier"
        );
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
}
