use core::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::default_health_status_indicator;
use nativelink_util::health_utils::HealthStatusIndicator;
use nativelink_util::store_trait::{
    DelegationChildren, ItemCallback, MarkStableDelegation, MergedNotifyState, PinDelegation,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::Notify;
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

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
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
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
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

/// A leaf-position fake store that returns its own per-instance
/// `Arc<Notify>` (not the shared static) so the test can probe
/// `Weak::strong_count` to detect whether the spawned forwarder
/// task is still holding a reference.
#[derive(Debug, MetricsComponent)]
struct CustomNotifyLeafStore {
    notify: Arc<Notify>,
}

impl CustomNotifyLeafStore {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
#[allow(clippy::todo)]
impl StoreDriver for CustomNotifyLeafStore {
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
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn stable_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

default_health_status_indicator!(CustomNotifyLeafStore);

/// A `Many`-position fake wrapper that owns two leaf children and a
/// `OnceLock<MergedNotifyState>`. Mirrors the production shape of
/// SizePartitioningStore / ShardStore / DedupStore minus the size
/// routing / sharding logic.
#[derive(Debug, MetricsComponent)]
struct ManyTestWrapper {
    lower: Arc<CustomNotifyLeafStore>,
    upper: Arc<CustomNotifyLeafStore>,
    merged_state: OnceLock<MergedNotifyState>,
}

#[async_trait]
#[allow(clippy::todo)]
impl StoreDriver for ManyTestWrapper {
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
        let lower: &dyn StoreDriver = self.lower.as_ref();
        let upper: &dyn StoreDriver = self.upper.as_ref();
        let mut children = DelegationChildren::new();
        children.push(lower);
        children.push(upper);
        StableDigestDelegation::Many {
            children,
            merged_state: &self.merged_state,
        }
    }
    fn pin_delegation(&self) -> PinDelegation<'_> {
        let lower: &dyn StoreDriver = self.lower.as_ref();
        let upper: &dyn StoreDriver = self.upper.as_ref();
        let mut children = DelegationChildren::new();
        children.push(lower);
        children.push(upper);
        PinDelegation::Many(children)
    }
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        let lower: &dyn StoreDriver = self.lower.as_ref();
        let upper: &dyn StoreDriver = self.upper.as_ref();
        let mut children = DelegationChildren::new();
        children.push(lower);
        children.push(upper);
        MarkStableDelegation::Many(children)
    }
}

default_health_status_indicator!(ManyTestWrapper);

/// Regression test for F2 (forwarder task leak) closure via `AbortOnDrop`.
///
/// **Bug class (IMPORTANT-2 from c-plus-d/testing-czar.md).** The whole
/// reason commit `669dabb9` exists is `MergedNotifyState`'s `Vec<AbortOnDrop>`
/// — when the wrapper drops, the OnceLock drops the state, each
/// `AbortOnDrop::drop` calls `JoinHandle::abort`, the forwarder task ends,
/// and its `Arc<Notify>` clones are released.
///
/// Without this test, an accidental change to `AbortOnDrop::drop`, the
/// `OnceLock` field placement, or `MergedNotifyState`'s `aborters` field
/// (e.g. someone removing `#[allow(dead_code)]` and "cleaning up" the
/// "unused" field) re-introduces F2 with zero test pressure.
///
/// Test shape:
///   1. Build a `Many` wrapper over two leaf stores, each owning its own
///      `Arc<Notify>` (not the shared static, so we can probe it).
///   2. Capture `Weak<Notify>` on each child's notify BEFORE the wrapper
///      acquires references via `stable_notify()`.
///   3. Call `wrapper.stable_notify()` once — this spawns N forwarder
///      tasks, each holding an `Arc<Notify>` clone of one child.
///   4. After yield, observe `Weak::strong_count` increased to baseline+1.
///   5. Drop the wrapper. The OnceLock drops MergedNotifyState, each
///      AbortOnDrop fires JoinHandle::abort, forwarders unwind and release
///      their child-Notify clones.
///   6. Yield + poll `Weak::strong_count` under timeout, assert it returns
///      to baseline (only the test's own Arc remaining).
#[nativelink_test]
async fn merged_notify_aborters_release_child_notify_arcs_on_wrapper_drop() {
    let lower = Arc::new(CustomNotifyLeafStore::new());
    let upper = Arc::new(CustomNotifyLeafStore::new());

    // Capture weak references BEFORE the wrapper acquires Arcs.
    let lower_weak: Weak<Notify> = Arc::downgrade(&lower.notify);
    let upper_weak: Weak<Notify> = Arc::downgrade(&upper.notify);

    // Baseline strong count: each leaf owns its notify (1) + this test's
    // local clone via the leaf field (the child `lower`/`upper` Arcs
    // still hold the only Arc<Notify>).
    let baseline_lower = Arc::strong_count(&lower.notify);
    let baseline_upper = Arc::strong_count(&upper.notify);
    assert_eq!(baseline_lower, 1, "baseline assumed: only leaf owns its notify");
    assert_eq!(baseline_upper, 1, "baseline assumed: only leaf owns its notify");

    let wrapper = Arc::new(ManyTestWrapper {
        lower: lower.clone(),
        upper: upper.clone(),
        merged_state: OnceLock::new(),
    });

    // Trigger the lazy init: spawns N forwarder tasks, each owning an
    // Arc<Notify> clone of one child.
    let merged = wrapper.stable_notify();
    // Hold the merged Notify alive locally to prove that releasing the
    // forwarders' Arcs is what changes the strong count, not just dropping
    // the merged side.
    let _merged = merged;

    // Yield so the spawned tasks can run their first `child_notify.notified()`
    // registration. (The Arc clone happens at spawn time, not at first
    // notified, so this isn't strictly needed for the strong-count check —
    // but a yield avoids any test-runner ordering surprises.)
    tokio::task::yield_now().await;

    let after_spawn_lower = Arc::strong_count(&lower.notify);
    let after_spawn_upper = Arc::strong_count(&upper.notify);
    assert!(
        after_spawn_lower > baseline_lower,
        "forwarder did not acquire an Arc clone of the lower child notify (strong_count {after_spawn_lower} <= baseline {baseline_lower}). \
         If MergedNotifyState's spawn loop is broken, this assertion misfires."
    );
    assert!(
        after_spawn_upper > baseline_upper,
        "forwarder did not acquire an Arc clone of the upper child notify (strong_count {after_spawn_upper} <= baseline {baseline_upper})."
    );

    // Drop wrapper -> OnceLock drop -> MergedNotifyState drop -> AbortOnDrop
    // drop -> JoinHandle::abort -> task unwind -> Arc<Notify> release.
    drop(wrapper);

    // Poll under a timeout: tokio's task abort + unwind isn't synchronous,
    // so we need to yield repeatedly until the strong count drops back to
    // baseline. The 1-second cap is the deadlock detector — without it a
    // regression would hang the test runner instead of failing fast.
    let waited = tokio::time::timeout(core::time::Duration::from_secs(1), async {
        loop {
            tokio::task::yield_now().await;
            let lower_now = lower_weak.strong_count();
            let upper_now = upper_weak.strong_count();
            if lower_now == baseline_lower && upper_now == baseline_upper {
                return (lower_now, upper_now);
            }
        }
    })
    .await;

    let (final_lower, final_upper) = waited.expect(
        "F2 regression: forwarder tasks did not release their Arc<Notify> clones \
         within 1s of wrapper drop. AbortOnDrop did not abort the spawned \
         forwarders, OR MergedNotifyState's aborters field was dropped without \
         firing JoinHandle::abort. Verify nativelink-util/src/store_trait.rs \
         AbortOnDrop::drop and MergedNotifyState's #[allow(dead_code)] \
         aborters field. CLAUDE.md note: this is the entire reason commit \
         669dabb9 exists.",
    );

    assert_eq!(final_lower, baseline_lower);
    assert_eq!(final_upper, baseline_upper);
}

// ----------------------------------------------------------------------
// task #168 item 2 (per plan C10): StoreDriver::observe_pinned_mirror_ack
// has a default no-op body. Stores that don't override (FakeStore here,
// MemoryStore / FilesystemStore in production) inherit the no-op so they
// can be safely included in the WorkerApiServer broadcast loop without
// special-casing.
//
// Per CLAUDE.md TDD: this test was written first to drive the trait
// addition, then verified to PASS once the default body landed. To
// mutate: change the default body to `panic!` and verify this test
// panics.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn observe_pinned_mirror_ack_default_is_noop() -> Result<(), Error> {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MirrorPinEntry;

    let store = Arc::new(FakeStore {});
    // Default no-op MUST accept an empty slice.
    StoreDriver::observe_pinned_mirror_ack(store.as_ref(), &[]);
    // Default no-op MUST also accept a non-empty slice (a wrapper that
    // ignores acks should not panic).
    let entry = MirrorPinEntry {
        digest: None,
        store_id: "cas".to_string(),
    };
    StoreDriver::observe_pinned_mirror_ack(store.as_ref(), &[entry]);

    Ok(())
}
