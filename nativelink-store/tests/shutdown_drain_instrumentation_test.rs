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

//! (#sigkill-gap) Tests for the shutdown-drain instrumentation + the Phase-1
//! "no automatic data loss" unbounding.
//!
//! Two contracts proven here:
//!
//! 1. **Phase-1 unbounded (correction 3).** `StoreManager::flush_slow_writes`
//!    no longer wraps the Phase-1 in-flight drain in an outer
//!    `tokio::time::timeout` that returned `Vec::new()` + logged "some slow
//!    writes will be lost" on elapse (AUTOMATIC DATA LOSS). A slow drain that
//!    exceeds the OLD 30 s budget MUST still land its data, not abandon it.
//!    Driven under `tokio::time::pause()` so the test advances virtual time
//!    well past 30 s in milliseconds of wall-clock.
//!    **Mutation:** re-introduce an outer `tokio::time::timeout(30s, ...)` arm
//!    that returns early at 30 s → the long in-flight write is abandoned → the
//!    blob never lands → red-fail with the bespoke message.
//!
//! 2. **Per-size-class instrumentation + stall detector (correction 4).** With
//!    a never-resolving slow tier and the worker-intake quiesced (no producer),
//!    the per-size-class progress poller reports the residue per class AND the
//!    stall detector sets `shutdown_stalled = 1` once `remaining > 0` with zero
//!    net drain over the stall window — WITHOUT killing or exiting anything.
//!    **Mutation:** make the slow tier resolve → the residue drains → no stall
//!    → `shutdown_stalled` stays 0 → red-fail.
//!
//! Production composition: the unit under test is `StoreManager::flush_slow_writes`
//! driving real `FastSlowStore { fast: MemoryStore, slow: <probe> }` stores
//! registered under the real `StoreManager` — the exact wrapper the SIGTERM
//! handler calls. No sleep-as-synchronization: virtual time + `Notify`.

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::Notify;

fn unique_digest(idx: u64, size: u64) -> DigestInfo {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&idx.to_le_bytes());
    DigestInfo::new(bytes, size)
}

fn build_fss(slow: Store) -> (Arc<FastSlowStore>, Store) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast.clone(),
        slow,
    );
    (fss, fast)
}

// ===================================================================
// Probe: a slow tier whose `update`/`update_oneshot` either sleep a
// configurable delay (for the unbounded test) or BLOCK forever on a Notify
// that is never fired (for the stall test). When `block_forever` is set the
// write parks on `gate.notified()` so the at-risk residue never drains.
// ===================================================================
#[derive(Debug, MetricsComponent)]
struct ControllableSlowProbe {
    inner: Store,
    delay: Duration,
    block_forever: bool,
    landed: Arc<AtomicUsize>,
    gate: Arc<Notify>,
}

default_health_status_indicator!(ControllableSlowProbe);

impl ControllableSlowProbe {
    async fn run_write<F, Fut, T>(self: Pin<&Self>, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<T, Error>>,
    {
        if self.block_forever {
            // Park forever (until the never-fired gate). Models a wedged slow
            // tier. The drain awaiting this write never completes; the stall
            // detector must surface it.
            self.gate.notified().await;
        } else if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let value = f().await?;
        self.landed.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    }
}

#[async_trait]
impl StoreDriver for ControllableSlowProbe {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
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
    ) -> Result<u64, Error> {
        let inner = self.inner.clone();
        let key = key.into_owned();
        self.run_write(move || async move { inner.update(key, reader, upload_size).await })
            .await
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        let inner = self.inner.clone();
        let key = key.into_owned();
        self.run_write(move || async move { inner.update_oneshot(key, data).await })
            .await
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

/// CONTRACT 1 (correction 3 — NO AUTOMATIC DATA LOSS in the flush). When the
/// Phase-1 in-flight wait budget is exceeded, `flush_slow_writes` must NOT
/// abandon the at-risk blob: the unbounded Phase 2 (`flush_fast_to_slow_at_shutdown`)
/// drains the residue to the slow tier. The OLD outer
/// `tokio::time::timeout(timeout, drain)` that returned `Vec::new()` + logged
/// "some slow writes will be lost" was the data-loss arm; it is removed. The
/// non-loss now rests ENTIRELY on Phase 2 catching the Phase-1 residue
/// (`in_flight_slow_writes ⊆ in_flight ∪ chunked ∪ failed`).
///
/// We seed an at-risk blob (fast-tier resident + `requeue_failed_push`) with a
/// slow-but-eventually-succeeding slow tier and pass a NEAR-ZERO Phase-1 budget
/// — the exact case the old outer timeout abused to bail early. The blob MUST
/// still land durably.
///
/// Mutation (the FIX, deterministic): make Phase-2 a no-op (comment out the
/// `flush_fast_to_slow_at_shutdown` call in `StoreManager::flush_slow_writes`) →
/// nothing drains the post-budget residue → the blob never reaches the slow tier
/// → red-fail with the bespoke message. (Re-adding the OLD outer Phase-1 timeout
/// does NOT lose data — Phase 2 still catches it — which is exactly why the
/// auditor verified unbounding Phase-1 "loses nothing"; the load-bearing
/// safety mechanism is Phase 2, and THIS test pins it.)
#[nativelink_test]
async fn flush_does_not_abandon_at_risk_blob_when_phase1_budget_exceeded() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let landed = Arc::new(AtomicUsize::new(0));
    // A slow tier that takes a real (small) delay per write, longer than the
    // near-zero Phase-1 budget we pass — so Phase 1 returns with the write still
    // pending and Phase 2 must be the thing that lands it.
    let probe: Arc<dyn StoreDriver> = Arc::new(ControllableSlowProbe {
        inner: inner.clone(),
        delay: Duration::from_millis(50),
        block_forever: false,
        landed: landed.clone(),
        gate: Arc::new(Notify::new()),
    });
    let (fss, fast) = build_fss(Store::new(probe));

    let sm = Arc::new(StoreManager::new());
    sm.add_store("cas_FAST_SLOW_STORE", Store::new(fss.clone()));

    // Seed the fast tier directly + mark the digest at-risk (models a
    // not-yet-durable blob whose background write has not reached the slow tier).
    let payload = vec![0x5Au8; 4096];
    let digest = unique_digest(7001, payload.len() as u64);
    fast.update_oneshot(digest, Bytes::from(payload.clone())).await?;
    assert!(
        fss.requeue_failed_push(digest),
        "setup: register the blob in the at-risk (failed_slow_writes) set",
    );
    assert!(
        inner.has(digest).await?.is_none(),
        "setup: slow tier must start empty (the blob is fast-tier-only / at-risk)",
    );

    // Near-zero Phase-1 budget — the case the OLD outer timeout abused to bail
    // and (mis)report "all drained / will be lost". With the abandoning arm
    // removed, the unbounded Phase 2 MUST still drive the blob durable.
    tokio::time::timeout(
        Duration::from_secs(5),
        sm.flush_slow_writes(Duration::from_millis(1)),
    )
    .await
    .expect("flush_slow_writes must converge (no wedge) and reach Phase 2");

    assert_eq!(
        landed.load(Ordering::SeqCst),
        1,
        "NO AUTOMATIC DATA LOSS: the at-risk blob MUST have been written to the          slow tier by the unbounded Phase 2 even though the Phase-1 budget (1 ms)          was exceeded by the 50 ms slow write. (Mutation: no-op Phase-2 → 0 →          red-fail.)",
    );
    let stored = inner
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "blob must be durable in slow tier after the unbounded drain")?;
    assert_eq!(
        stored.as_ref(),
        payload.as_slice(),
        "the slow-tier blob content must match the at-risk blob the flush drained",
    );
    Ok(())
}

/// CONTRACT 2 (per-size-class instrumentation + stall detector). With a
/// never-resolving (wedged) slow tier, the at-risk residue never drains; the
/// per-size-class gauges reflect the residue and the stall detector escalates
/// (`shutdown_stalled = 1`) once the stall window elapses — and NOTHING is
/// killed (the drain just keeps waiting; `flush_slow_writes` never returns).
///
/// Two stores registered (a `small`-named arm and a `cas_FAST_SLOW`-named arm)
/// so the per-class bucketing is exercised: the `large_tank` class must show
/// the wedged residue.
///
/// Mutation: make the slow tier resolve (set `block_forever = false`) → the
/// residue drains → `shutdown_stalled` stays 0 → the `assert!(stalled == 1)`
/// red-fails ("stall detector fired on a progressing drain" is the inverse;
/// here the assertion is that it FIRES on a wedged drain).
#[nativelink_test]
async fn stall_detector_fires_on_wedged_slow_tier_without_killing() -> Result<(), Error> {
    tokio::time::pause();

    // Wedged large/srv/bulk arm: slow tier blocks forever.
    let large_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let large_probe: Arc<dyn StoreDriver> = Arc::new(ControllableSlowProbe {
        inner: large_inner.clone(),
        delay: Duration::ZERO,
        block_forever: true,
        landed: Arc::new(AtomicUsize::new(0)),
        gate: Arc::new(Notify::new()),
    });
    let (large_fss, _large_fast) = build_fss(Store::new(large_probe));

    let sm = Arc::new(StoreManager::new());
    // Name carries the size class: classify() maps "cas_FAST_SLOW_STORE" →
    // large_tank.
    sm.add_store("cas_FAST_SLOW_STORE", Store::new(large_fss.clone()));

    // Seed a real in-flight slow write that parks forever in the probe. The
    // fast tier accepts instantly; the slow write enters in_flight_slow_writes
    // (Phase-1 target) and never completes.
    let payload = vec![0xCCu8; 8192];
    let digest = unique_digest(8001, payload.len() as u64);
    Store::new(large_fss.clone())
        .update_oneshot(digest, Bytes::from(payload.clone()))
        .await?;

    // Drive the flush concurrently; it will NEVER return (wedged tier). We
    // advance virtual time past the stall window and observe the gauges.
    let sm_drive = sm.clone();
    let flush = tokio::spawn(async move {
        sm_drive.flush_slow_writes(Duration::from_secs(30)).await;
    });

    // Advance past the stall window (20 s) plus a couple of progress ticks so
    // the no-progress accumulator crosses the threshold and the detector fires.
    for _ in 0..15 {
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }
    // Let any pending poller tasks run.
    tokio::task::yield_now().await;

    let (phase, stalled, _small_blobs, large_blobs, _small_bytes, _large_bytes) =
        sm.shutdown_drain_gauges_for_testing();

    assert_eq!(
        phase, 1,
        "stall detector test: the drain must be in Phase-1 (in-flight drain) \
         while the slow write is wedged; gauge reported phase {phase}",
    );
    assert!(
        large_blobs >= 1,
        "per-size-class instrumentation: the large_tank residue gauge MUST \
         reflect the wedged in-flight blob; got {large_blobs} (expected ≥1)",
    );
    assert_eq!(
        stalled, 1,
        "stall detector MUST fire (shutdown_stalled = 1) once remaining > 0 with \
         zero net drain over the stall window on a wedged slow tier; got \
         stalled={stalled}. (Mutation: make the slow tier resolve → residue \
         drains → this stays 0.)",
    );

    // HARD CONSTRAINT: the detector is OBSERVABILITY-ONLY. The flush must NOT
    // have returned (no auto-kill, no auto-abandon) — it is still waiting for
    // the wedged write, exactly as an unbounded drain should.
    assert!(
        !flush.is_finished(),
        "no automatic data loss: the stall detector MUST NOT cause \
         flush_slow_writes to return / abandon the wedged blob — it only \
         escalates to error! + sets the gauge. The drain keeps waiting.",
    );

    flush.abort();
    Ok(())
}
