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

//! Durability-ack v3 Stage 1 — `has_durably` SEAM tests.
//!
//! `has_durably` is a DURABLE-tier presence query threaded through every CAS
//! chain wrapper. Unlike `has_with_results` (which the FastSlowStore
//! satisfies from the fast tier + in-flight maps + mirror), `has_durably`
//! routes the FSS arm to `slow_store().has_with_results` ONLY — so a blob
//! present only in RAM reports absent.
//!
//! The seam guarantee (v3 §6.8): `durable_delegation()` is a no-default-body
//! forced method, so a new wrapper that omits its arm is a COMPILE ERROR.
//! There is no `has_with_results`-collapsing default — a wrapper that
//! forgets durability propagation reports absent (strictly-safer), never a
//! false-durable.
//!
//! ## Mutation (TDD step 5)
//!
//! Make one chain wrapper forward `has_durably` to `has_with_results` (the
//! collapse the seam forbids). With the FSS slow write held, the chain-top
//! `has_durably` then returns `Some` for the RAM-only blob and
//! `has_durably_absent_for_ram_only_blob_through_chain` red-fails with
//! "has_durably collapsed to has_with_results (durable check defeated)".

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    ExistenceCacheSpec, FastSlowSpec, MemorySpec, SizePartitioningSpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::{Error, ResultExt};
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
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::Notify;

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

// ----- HoldableSlowStore: slow tier whose update blocks until released -----
#[derive(Debug, MetricsComponent)]
struct HoldableSlowStore {
    inner: Store,
    #[metric(help = "unused")]
    _unused: u64,
    release: Arc<Notify>,
    hold: std::sync::atomic::AtomicBool,
}

impl HoldableSlowStore {
    fn new(inner: Store) -> Arc<Self> {
        Arc::new(Self {
            inner,
            _unused: 0,
            release: Arc::new(Notify::new()),
            hold: std::sync::atomic::AtomicBool::new(true),
        })
    }
    fn release(&self) {
        self.hold.store(false, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_waiters();
    }
    async fn wait_if_held(&self) {
        while self.hold.load(std::sync::atomic::Ordering::SeqCst) {
            self.release.notified().await;
        }
    }
}

default_health_status_indicator!(HoldableSlowStore);

#[async_trait]
impl StoreDriver for HoldableSlowStore {
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
        self.wait_if_held().await;
        self.inner.update(key, reader, upload_size).await
    }
    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        self.wait_if_held().await;
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

/// Build `Verify(ExistenceCache(SizePartitioning(Memory→Memory FSS,
/// Memory→Holdable FSS)))` and return `(chain_top, hold)`. >16 KiB blobs
/// route to the holdable upper arm.
fn build_chain() -> (Store, Arc<HoldableSlowStore>) {
    const PARTITION: u64 = 16 * 1024;

    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let hold = HoldableSlowStore::new(upper_slow_inner);
    let upper_slow = Store::new(hold.clone());
    let upper_fss = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        upper_fast,
        upper_slow,
    ));

    let lower_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fss = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        lower_fast,
        lower_slow,
    ));

    let size_part = Store::new(SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: PARTITION,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        lower_fss,
        upper_fss,
    ));
    let cache = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        size_part,
    ));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        cache,
    ));
    (verify, hold)
}

/// SEAM test: a blob written through the chain into the holdable upper FSS is
/// present (non-durable has) but `has_durably` at the CHAIN TOP returns
/// absent while the slow write is held. After release + the durable write
/// lands, `has_durably` returns Some.
///
/// This crosses every wrapper: Verify → ExistenceCache → SizePartitioning →
/// FastSlowStore → slow tier. A wrapper that collapses `has_durably` to
/// `has_with_results` would report the RAM-only blob as durable and fail the
/// first assertion.
#[nativelink_test]
async fn has_durably_absent_for_ram_only_blob_through_chain() -> Result<(), Error> {
    let (chain, hold) = build_chain();

    let data = Bytes::from(vec![0x5Au8; 32 * 1024]); // >16 KiB → upper arm
    let digest = DigestInfo::new([3u8; 32], data.len() as u64);

    // Write through the chain. FSS `update_oneshot` lands the bytes in the
    // fast tier + `in_flight_slow_writes` and spawns the slow write, which
    // blocks in HoldableSlowStore::update_oneshot — so the blob is present in
    // RAM but NOT durable.
    tokio::time::timeout(DEADLOCK_TIMEOUT, chain.update_oneshot(digest, data.clone()))
        .await
        .expect("DEADLOCK: chain.update_oneshot did not return")?;

    // Non-durable presence: Some (fast tier / in-flight map).
    let present = chain.has(digest).await?;
    assert!(
        present.is_some(),
        "blob should be present via the non-durable has() path (fast tier / in-flight)"
    );

    // Durable presence at the chain top: None (slow write is held).
    let mut durable = [None];
    chain.has_durably(&[digest.into()], &mut durable).await?;
    assert!(
        durable[0].is_none(),
        "has_durably collapsed to has_with_results (durable check defeated): the \
         chain-top has_durably returned {durable:?} for a blob present ONLY in the \
         fast tier (RAM) while the slow write was held. Every wrapper must route \
         has_durably to the durable tier, NOT satisfy it from fast/in-flight/mirror."
    );

    // Release the held slow write; wait for the durable write to land.
    hold.release();
    let deadline = std::time::Instant::now() + DEADLOCK_TIMEOUT;
    loop {
        let mut durable = [None];
        chain.has_durably(&[digest.into()], &mut durable).await?;
        if durable[0].is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "after releasing the slow write, has_durably never reported the blob \
             durable within {DEADLOCK_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}

/// Companion: `has_durably` returns absent for a digest never written at all
/// (basic absent semantics, no holding required).
#[nativelink_test]
async fn has_durably_absent_for_unknown_digest() -> Result<(), Error> {
    let (chain, _hold) = build_chain();
    let digest = DigestInfo::new([0xEEu8; 32], 123);
    let mut durable = [None];
    chain
        .has_durably(&[digest.into()], &mut durable)
        .await
        .err_tip(|| "has_durably on unknown digest")?;
    assert!(
        durable[0].is_none(),
        "has_durably must report absent for a digest never written; got {durable:?}"
    );
    Ok(())
}
