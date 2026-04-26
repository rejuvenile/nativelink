use core::pin::Pin;
use std::sync::Arc;

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::default_health_status_indicator;
use nativelink_util::health_utils::HealthStatusIndicator;
use nativelink_util::store_trait::{
    ItemCallback, PinDelegation, StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike,
    UploadSizeInfo,
};
use tonic::async_trait;

#[derive(Debug, MetricsComponent)]
struct FakeStore {}

#[async_trait]
#[allow(clippy::todo)]
impl StoreDriver for FakeStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        todo!();
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        todo!();
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        todo!();
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        todo!();
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }
}

default_health_status_indicator!(FakeStore);

#[nativelink_test]
async fn fast_has_with_results() -> Result<(), Error> {
    let store = Store::new(Arc::new(FakeStore {}));
    let mut results: [Option<u64>; 0] = [];
    store.has_with_results(&[], &mut results).await?;

    Ok(())
}

#[nativelink_test]
async fn fast_has_many() -> Result<(), Error> {
    let store = Store::new(Arc::new(FakeStore {}));
    let res = store.has_many(&[]).await?;
    assert!(res.is_empty());

    Ok(())
}

/// A `Leaf`-declared store that does NOT actually pin (simulating
/// MemoryStore, NoopStore, S3, etc). The trait default for
/// `pin_digests_with_results` MUST report `false` for every digest so
/// callers do not receive a silent-success that masks "this store
/// cannot pin."
///
/// **Bug class (CRIT-1 from c-plus-d/testing-czar.md and F3 from
/// c-plus-d/red-team.md).** Prior default returned `vec![true; n]` —
/// indistinguishable from an actually-pinned digest. When fanned out
/// via `PinDelegation::Many` (e.g. `FastSlowStore { fast: Memory,
/// slow: Filesystem }`), the OR-merge with FilesystemStore's real
/// `false` yielded `true`, hiding that the digest had been evicted.
/// The forced-delegation enums fix the `Many` mechanism but the
/// `Leaf` default body itself was the silent-success leak.
///
/// Tests in this module pin a fake leaf (no override) and assert the
/// returned vec is all-false. Pre-fix this test FAILS — vec is all-true.
#[derive(Debug, MetricsComponent)]
struct NonPinningLeafStore {}

#[async_trait]
#[allow(clippy::todo)]
impl StoreDriver for NonPinningLeafStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        todo!();
    }
    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        todo!();
    }
    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        todo!();
    }
    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }
    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }
    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        todo!();
    }
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }
}

default_health_status_indicator!(NonPinningLeafStore);

#[nativelink_test]
async fn pin_digests_with_results_non_pinning_leaf_reports_false() {
    let store = NonPinningLeafStore {};
    let digests = [
        DigestInfo::try_new(
            "0000000000000000000000000000000000000000000000000000000000000001",
            10,
        )
        .unwrap(),
        DigestInfo::try_new(
            "0000000000000000000000000000000000000000000000000000000000000002",
            20,
        )
        .unwrap(),
    ];

    let results = store.pin_digests_with_results(&digests);

    assert_eq!(
        results,
        vec![false, false],
        "PinDelegation::Leaf default body must report false for stores that do not pin. \
         Reporting true silently lies to callers that use the result to detect \
         eviction races (CRIT-1 / F3). Fix in nativelink-util/src/store_trait.rs \
         pin_digests_with_results trait default."
    );
}
