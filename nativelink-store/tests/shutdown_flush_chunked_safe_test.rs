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

//! Durability-ack v3 Stage 1 — CHANGE A: chunked-safe shutdown flush.
//!
//! `FastSlowStore::flush_fast_to_slow_at_shutdown` is the graceful-shutdown
//! drain that copies not-yet-durable fast-tier bytes to the slow tier so an
//! acked-not-durable blob is not lost when the process exits. v3 Change A:
//!
//!   1. KEEP the whole-tier `fast_store.list()` scan as the byte source
//!      (#210's "drain ALL memory-only blobs" superset — and the ONLY source
//!      that reaches CHUNKED bytes, which live in the fast tier, not in any
//!      bytes-carrying in-flight map: `chunked_in_flight_digests` holds only
//!      `(NonZeroU32, Arc<Notify>)`).
//!   2. DROP the per-key `slow.has()` pre-check (the RCA budget-burner that
//!      spent the whole deadline probing already-durable blobs).
//!   3. DROP the deadline (R2 — the param is gone; the flush runs to
//!      completion).
//!   4. FILTER to not-yet-durable via the in-memory at-risk DIGEST SET
//!      (`in_flight_slow_writes` ∪ `chunked_in_flight_digests` ∪
//!      `failed_slow_writes`) so already-durable residents are skipped
//!      WITHOUT a slow probe.
//!
//! ## Tests
//!
//! * `chunked_inflight_blob_flushed_without_has_probe` (under-action +
//!   no-probe): a blob whose digest is registered in
//!   `chunked_in_flight_digests` (bytes in the fast tier) MUST be flushed,
//!   and the slow tier's `has()` MUST NOT be probed (the probe panics).
//! * `durable_resident_skipped_via_in_memory_state` (over-action / perf
//!   win): a fast-tier resident NOT in any at-risk set (models a durable
//!   back-populated read) MUST be skipped WITHOUT a redundant write (the
//!   probe's `update_oneshot` panics if called).
//!
//! ## Mutation (TDD step 5)
//!
//! * Re-add the `slow.has()` pre-check in `flush_fast_to_slow_at_shutdown`
//!   → `chunked_inflight_blob_flushed_without_has_probe` red-fails with the
//!   `NoHasProbe` panic "slow.has() probed during shutdown flush — the RCA
//!   budget-burner is back".
//! * Drop the not-yet-durable filter (flush every resident) →
//!   `durable_resident_skipped_via_in_memory_state` red-fails with the
//!   `NoUpdateExpected` panic "already-durable resident re-written during
//!   shutdown flush — the not-yet-durable filter was dropped".

use core::num::NonZeroU32;
use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::Notify;

const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

fn unique_digest(idx: u64, size: u64) -> DigestInfo {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&idx.to_le_bytes());
    DigestInfo::new(bytes, size)
}

/// Build `FastSlowStore { fast: Memory, slow: <probe> }`.
fn build_fss(slow: Store) -> Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    )
}

// ----- NoHasProbe: a slow tier whose has() panics -----
//
// Proves the v3 flush issues NO `slow.has()` pre-check. `update`/
// `update_oneshot` delegate so flushed bytes still land.
#[derive(Debug, MetricsComponent)]
struct NoHasProbe {
    inner: Store,
}
default_health_status_indicator!(NoHasProbe);

#[async_trait]
impl StoreDriver for NoHasProbe {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        panic!(
            "slow.has() probed during shutdown flush — the RCA budget-burner is back. \
             v3 Change A DROPS the per-key slow.has() pre-check; the not-yet-durable \
             filter is the in-memory at-risk set, never a slow-tier probe."
        );
    }
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
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
    fn register_item_callback(self: Arc<Self>, _cb: Arc<dyn ItemCallback>) -> Result<(), Error> {
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

// ----- NoUpdateExpected: a slow tier whose writes panic -----
//
// Proves the not-yet-durable filter SKIPS an already-durable resident (no
// redundant write). `has()` delegates (the v3 flush should not call it, but
// delegating keeps the probe honest if a mutation re-adds the has() skip).
#[derive(Debug, MetricsComponent)]
struct NoUpdateExpected {
    inner: Store,
    /// The flush MUST NOT write this digest (the already-durable resident).
    /// Writes of OTHER digests (the at-risk blob that keeps the drain alive)
    /// are delegated to `inner`.
    forbidden: DigestInfo,
}
default_health_status_indicator!(NoUpdateExpected);

impl NoUpdateExpected {
    fn forbid(&self, key: &StoreKey<'_>) {
        if let StoreKey::Digest(d) = key.borrow() {
            assert!(
                d != self.forbidden,
                "already-durable resident re-written during shutdown flush — the \
                 not-yet-durable filter was dropped. A fast-tier resident NOT in any \
                 at-risk set (in_flight ∪ chunked ∪ failed) must be SKIPPED. \
                 Offending digest: {d:?}"
            );
        }
    }
}

#[async_trait]
impl StoreDriver for NoUpdateExpected {
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
        self.forbid(&key);
        self.inner.update(key, reader, upload_size).await
    }
    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        self.forbid(&key);
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
    fn register_item_callback(self: Arc<Self>, _cb: Arc<dyn ItemCallback>) -> Result<(), Error> {
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

/// UNDER-ACTION + NO-PROBE: a CHUNKED-in-flight blob (digest registered in
/// `chunked_in_flight_digests`, bytes in the fast tier, NOT in any
/// bytes-carrying map) MUST be flushed to the slow tier at shutdown, and the
/// flush MUST NOT issue a `slow.has()` pre-check. The slow tier panics on
/// `has()` so a surviving probe is caught.
#[nativelink_test]
async fn chunked_inflight_blob_flushed_without_has_probe() -> Result<(), Error> {
    let slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let probe = Store::new(Arc::new(NoHasProbe {
        inner: slow_inner.clone(),
    }) as Arc<dyn StoreDriver>);
    let fss = build_fss(probe);

    // Bytes live in the fast tier (where chunked acked-not-durable bytes
    // actually live). We write to the FSS's fast tier directly to model the
    // chunked tee without spawning a real chunked driver.
    let payload = Bytes::from(vec![0xC4u8; 4096]);
    let digest = unique_digest(7001, payload.len() as u64);
    fss.fast_store()
        .update_oneshot(digest, payload.clone())
        .await
        .err_tip(|| "seed fast tier")?;

    // Register the digest in chunked_in_flight_digests (the at-risk set). The
    // entry holds only a refcount + Notify (NO bytes) — exactly the
    // production shape that v2's "drain the maps' bytes" could not source.
    {
        let map = fss.chunked_in_flight_digests_handle();
        let mut guard = map.lock();
        guard.insert(
            digest,
            (NonZeroU32::new(1).unwrap(), Arc::new(Notify::new())),
        );
    }
    assert!(
        fss.is_chunked_in_flight(&digest),
        "setup: digest must be registered chunked-in-flight"
    );

    let unflushed = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fss.flush_fast_to_slow_at_shutdown())
        .await
        .expect(
            "DEADLOCK: flush_fast_to_slow_at_shutdown did not return; the chunked-safe \
         flush of a single in-memory blob must finish in milliseconds.",
        );

    assert_eq!(
        unflushed, 0,
        "chunked-safe flush MUST drain the at-risk chunked-in-flight blob; got \
         {unflushed} unflushed (expected 0)"
    );

    // The blob physically landed in the slow tier.
    let stored = slow_inner
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "chunked-in-flight blob missing from slow tier post-flush")?;
    assert_eq!(
        stored.as_ref(),
        payload.as_ref(),
        "chunked-safe flush: slow-tier content mismatch for the at-risk blob"
    );
    Ok(())
}

/// OVER-ACTION / PERF WIN: a fast-tier resident NOT in any at-risk set
/// (models a durable back-populated read) MUST be SKIPPED — no redundant
/// write. CRUCIAL: a SECOND blob IS registered at-risk so the at-risk
/// snapshot is NON-empty and the per-key drain actually runs (otherwise the
/// `at_risk.is_empty()` early-return would mask a dropped per-key filter —
/// the mutation `if false && at_risk.contains(..)` must turn this RED, which
/// requires reaching the drain). The slow probe panics ONLY for the durable
/// resident's digest, so a flush that re-writes it is caught; the at-risk
/// blob's write is delegated and must land.
#[nativelink_test]
async fn durable_resident_skipped_via_in_memory_state() -> Result<(), Error> {
    let slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));

    // The durable resident (NOT at-risk) — its re-write must never happen.
    let durable_payload = Bytes::from(vec![0xD0u8; 2048]);
    let durable_digest = unique_digest(8001, durable_payload.len() as u64);

    let probe = Store::new(Arc::new(NoUpdateExpected {
        inner: slow_inner.clone(),
        forbidden: durable_digest,
    }) as Arc<dyn StoreDriver>);
    let fss = build_fss(probe);

    // (a) durable resident in the fast tier, NOT in any at-risk set.
    fss.fast_store()
        .update_oneshot(durable_digest, durable_payload.clone())
        .await
        .err_tip(|| "seed durable resident")?;

    // (b) an AT-RISK blob (failed_slow_writes) so the at-risk snapshot is
    // non-empty and the per-key drain runs. Its write IS allowed.
    let at_risk_payload = Bytes::from(vec![0xA7u8; 1024]);
    let at_risk_digest = unique_digest(8002, at_risk_payload.len() as u64);
    fss.fast_store()
        .update_oneshot(at_risk_digest, at_risk_payload.clone())
        .await
        .err_tip(|| "seed at-risk blob")?;
    assert!(fss.requeue_failed_push(at_risk_digest));

    let unflushed = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fss.flush_fast_to_slow_at_shutdown())
        .await
        .expect("DEADLOCK: flush did not return");

    // The at-risk blob landed (so the drain definitely ran past the filter);
    // the durable resident's write never fired (probe would have panicked).
    assert_eq!(
        unflushed, 0,
        "the at-risk blob must flush cleanly; got {unflushed} unflushed"
    );
    let landed = slow_inner
        .get_part_unchunked(at_risk_digest, 0, None)
        .await
        .err_tip(|| "at-risk blob missing from slow tier — the drain did not run")?;
    assert_eq!(landed.as_ref(), at_risk_payload.as_ref());
    // The durable resident must NOT have been written to the slow tier by the
    // flush. (It was never written there at all in this test.)
    assert!(
        slow_inner.has(durable_digest).await?.is_none(),
        "the already-durable resident must NOT be (re-)written by the flush"
    );
    Ok(())
}

/// SIBLING: a `failed_slow_writes` blob (the other at-risk class) MUST also
/// be flushed. Same byte source (fast tier), different at-risk-set member.
#[nativelink_test]
async fn failed_write_blob_flushed_at_shutdown() -> Result<(), Error> {
    let slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let probe = Store::new(Arc::new(NoHasProbe {
        inner: slow_inner.clone(),
    }) as Arc<dyn StoreDriver>);
    let fss = build_fss(probe);

    let payload = Bytes::from(vec![0xF1u8; 1500]);
    let digest = unique_digest(9001, payload.len() as u64);
    fss.fast_store()
        .update_oneshot(digest, payload.clone())
        .await
        .err_tip(|| "seed fast tier")?;
    // Register in the failed-slow-writes at-risk set.
    assert!(
        fss.requeue_failed_push(digest),
        "setup: requeue_failed_push should accept the digest"
    );

    let unflushed = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, fss.flush_fast_to_slow_at_shutdown())
        .await
        .expect("DEADLOCK: flush did not return");
    assert_eq!(
        unflushed, 0,
        "failed-slow-writes (at-risk) blob MUST be flushed; got {unflushed} unflushed"
    );
    let stored = slow_inner
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "failed-write blob missing from slow tier post-flush")?;
    assert_eq!(stored.as_ref(), payload.as_ref());
    Ok(())
}
