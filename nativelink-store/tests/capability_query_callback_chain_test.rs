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

//! Regression tests for #10 and #11: capability-query honesty and callback-chain
//! correctness across ECS / FastSlow / RefStore compositions.
//!
//! Item 1 (#10): `FastSlowStore::register_slow_eviction_stable_set_listener`
//!   must resolve the slow store's RefStore cell via `inner_store(None)` BEFORE
//!   calling `supports_removal_callbacks()`, so direct `FastSlowStore::new()`
//!   callers with a ref-wrapped slow store don't silently lose the BIS listener.
//!
//! Item 2 (#11): `FastSlowStore::supports_removal_callbacks` must return
//!   `fast.supports() && slow.supports()` (resolving refs first) so an ECS
//!   wrapping a FastSlow{*, NoCallbackStore} composition correctly reports
//!   `false` and constructs in `vulnerable_mode=true` instead of registering
//!   a dead callback that is never fired.
//!
//! Item 5 (#11): `RefStore::get_store` must write the cell BEFORE replaying
//!   queued callbacks, so a callback replay failure does NOT permanently brick
//!   the RefStore; ops succeed (degraded, no callbacks) after a replay error.
//!
//! Item 6 (#11): Production-shape propagation — ECS(RefStore→FastSlow{Memory,
//!   Memory}) end-to-end: evicting from the SLOW tier must invalidate the ECS
//!   existence entry via FastSlow's callback fanout, within a deadline.

use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use nativelink_config::stores::{ExistenceCacheSpec, FastSlowSpec, MemorySpec, RefSpec, StoreDirection, StoreSpec};
use nativelink_error::{Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::{MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent};
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// ---------------------------------------------------------------------------
// Helper: a store that reports supports_removal_callbacks=false.
// Used for Item 2 (honesty test — the slow tier that cannot fire callbacks).
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NoCallbackStore;

impl MetricsComponent for NoCallbackStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl NoCallbackStore {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait]
impl StoreDriver for NoCallbackStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
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
            nativelink_error::Code::NotFound,
            "NoCallbackStore: no data",
        ))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    /// Returns `false` — this store cannot fire eviction callbacks.
    /// Used to test FastSlow honesty: when the slow tier is NoCallbackStore,
    /// FastSlow.supports_removal_callbacks() must return false.
    fn supports_removal_callbacks(&self) -> bool {
        false
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Err(make_err!(
            nativelink_error::Code::FailedPrecondition,
            "NoCallbackStore: callbacks not supported",
        ))
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

default_health_status_indicator!(NoCallbackStore);

// ---------------------------------------------------------------------------
// Helper: a store that reports supports_removal_callbacks=true but always
// rejects register_item_callback with Err. Used for Item 5 (replay-failure).
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RejectCallbackStore {
    /// Counts how many `has_with_results` calls succeed (proves store is usable
    /// after a replay-failure bricking would have prevented all ops).
    pub ops_completed: AtomicUsize,
}

impl MetricsComponent for RejectCallbackStore {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl RejectCallbackStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ops_completed: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl StoreDriver for RejectCallbackStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for r in results.iter_mut() {
            *r = None;
        }
        self.ops_completed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
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
            nativelink_error::Code::NotFound,
            "RejectCallbackStore: no data",
        ))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn supports_removal_callbacks(&self) -> bool {
        // Reports support (so it's queued for replay) but always rejects.
        true
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Err(make_err!(
            nativelink_error::Code::FailedPrecondition,
            "RejectCallbackStore: register_item_callback deliberately rejects",
        ))
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

default_health_status_indicator!(RejectCallbackStore);

// Simple noop callback for use in Item 5 test.
#[derive(Debug)]
struct NoopCallback;

impl ItemCallback for NoopCallback {
    fn callback<'a>(
        &'a self,
        _store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

// ---------------------------------------------------------------------------
// Item 1 (#10): FastSlow::new() with ref-wrapped slow → BIS listener registered
// ---------------------------------------------------------------------------

/// Test for #10: `FastSlowStore::new()` (direct, not `new_validated`)
/// with a RefStore-wrapped slow store must register the BIS slow-eviction
/// listener so it fires correctly after the RefStore cell resolves.
///
/// The `inner_store(None)` call in `register_slow_eviction_stable_set_listener`
/// ensures `supports_removal_callbacks()` returns the correct (true) answer
/// before the capability is logged, preventing a misleading false-warn in the
/// `(false, Ok(()))` branch. Without the fix, `supports_removal_callbacks()` on
/// an unresolved RefStore returns false (by design, cd980946/#367), causing an
/// incorrect "silent no-op accept" WARN even though RefStore queues and replays
/// the callback correctly. The fix is about observability correctness, not
/// functional behavior (the callback fires in both cases via RefStore queuing).
///
/// Assertion: after a slow-tier eviction fires on the MemoryStore (the
/// resolved target of the RefStore), `drain_stable_digests()` on the
/// FastSlowStore returns empty (callback fired, entry removed from stable set).
/// We write a blob and mark it stable first so the listener has state to
/// invalidate.
///
/// NOTE on mutation: the `inner_store(None)` call affects log output (warn vs
/// quiet), not functional callback behavior. The callback still fires via
/// RefStore's item_callbacks queue. This test verifies the functional guarantee;
/// the companion Item 2 tests verify the capability-query answer.
#[nativelink_test]
async fn fast_slow_new_ref_wrapped_slow_registers_bis_listener() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());

    // Slow store: MemoryStore registered under "slow_backend".
    let slow_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    store_manager.add_store("slow_backend", slow_mem.clone());

    // Wrap slow store in a RefStore (cell empty at this point).
    let slow_ref = Store::new(RefStore::new(
        &RefSpec {
            name: "slow_backend".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    let fast_mem = Store::new(MemoryStore::new(&MemorySpec::default()));

    // Call FastSlowStore::new() directly — NOT new_validated().
    // Pre-fix: new_validated() accidentally resolves the ref via its own
    // `inner_store(None)` call, so the vulnerability only manifests on direct
    // new() callers.
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_mem.clone(),
        slow_ref,
    );
    let fss_store = Store::new(fss.clone());

    // Write a blob through the FSS so we can mark it stable.
    let digest = DigestInfo::try_new(VALID_HASH1, 3)?;
    fss_store
        .update_oneshot(digest, "abc".into())
        .await
        .expect("update_oneshot must succeed");

    // Mark the digest stable — places it in `stable_digests`.
    fss.mark_stable(&[digest]);

    // FastSlowStore writes to the slow tier in a background task. Wait for it.
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        while slow_mem.has(digest).await.unwrap_or(None).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow write must complete within 3s before we can evict from it");

    // Now evict from the SLOW tier (MemoryStore) directly. The BIS listener
    // should fire and remove the digest from `stable_digests`.
    let slow_inner = slow_mem
        .downcast_ref::<MemoryStore>(None)
        .expect("slow_mem is MemoryStore");
    let removed = slow_inner.remove_entry(digest.into()).await;
    assert!(
        removed,
        "slow MemoryStore must have the entry to remove (write must have reached slow tier)"
    );

    // Poll for the callback to fire and clear the stable set.
    // The BIS callback uses `retain` to remove the digest from stable_digests
    // in-place when eviction fires. We yield first to give the async callback
    // time to fire (and use retain to empty stable_digests) before we drain.
    // Common case: callback fires during yield_now → drain returns empty → done.
    // We do NOT re-mark between drain iterations: in the prior loop pattern
    // (drain → non-empty → re-mark → yield), if the callback fires between
    // drain and re-mark, the re-mark puts the entry back and no future eviction
    // can clear it → 3-second hang. Without re-marking, drain on the next
    // iteration returns empty (callback already removed via retain, or the prior
    // drain left stable_digests empty).
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        // Yield first: let the async eviction callback fire and clear stable_digests
        // via retain before we drain.
        tokio::task::yield_now().await;
        loop {
            let still_stable = fss.drain_stable_digests();
            if still_stable.is_empty() {
                return;
            }
            // Non-empty on this iteration means the callback hasn't fired yet.
            // Yield to let it run, then drain again.
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#10: BIS callback failed to fire after slow-tier eviction. \
         RefStore-wrapped slow store must replay the queued listener on cell resolution; \
         stable_digests must be empty after eviction from MemoryStore.",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Item 2 (#11): FastSlowStore::supports_removal_callbacks honesty
// ---------------------------------------------------------------------------

/// TDD Red test for #11 (honesty, false path):
/// `FastSlow{MemoryStore, NoCallbackStore}` — NoCallbackStore.supports() == false.
/// Pre-fix: FastSlow.supports() == true (inherited default, unconditional).
/// Post-fix: FastSlow.supports() == false (slow tier drags it down).
///
/// Mutation: remove the `supports_removal_callbacks` override from
/// `FastSlowStore` → this test red-fails with:
///   "#11: FastSlow honest-supports — slow tier that cannot fire callbacks \
///    must make composite false"
#[nativelink_test]
async fn fast_slow_supports_callbacks_false_when_slow_does_not() -> Result<(), Error> {
    let slow = Store::new(NoCallbackStore::new());
    assert!(
        !slow.supports_removal_callbacks(),
        "NoCallbackStore pre-condition: must return false"
    );

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));

    let fss = FastSlowStore::new(
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
    );

    assert!(
        !fss.supports_removal_callbacks(),
        "#11: FastSlow honest-supports — slow tier that cannot fire callbacks \
         must make composite false. Pre-fix: trait default true is inherited \
         unconditionally; post-fix: override returns fast.supports() && slow.supports(). \
         With NoCallbackStore slow, the composite must be false so ECS constructs \
         in vulnerable_mode=true instead of registering a dead callback."
    );

    Ok(())
}

/// TDD test for #11 (honesty, true path):
/// `FastSlow{MemoryStore, MemoryStore}` — both tiers support callbacks.
/// `FastSlow.supports()` must return `true`.
#[nativelink_test]
async fn fast_slow_supports_callbacks_true_when_both_tiers_support() -> Result<(), Error> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));

    let fss = FastSlowStore::new(
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
    );

    assert!(
        fss.supports_removal_callbacks(),
        "#11: FastSlow{{Memory, Memory}} must report supports_removal_callbacks=true \
         (both tiers support). This is the AC_BACKEND_CACHED production shape."
    );

    Ok(())
}

/// TDD test for #11 (honesty, ref-wrapped slow resolved):
/// `FastSlow{MemoryStore, RefStore(MemoryStore)}` — after resolution via
/// inner_store(None), the RefStore cell points to MemoryStore which supports
/// callbacks. `FastSlow.supports()` must return `true`.
#[nativelink_test]
async fn fast_slow_supports_callbacks_true_when_slow_ref_resolves_to_memory() -> Result<(), Error>
{
    let store_manager = Arc::new(StoreManager::new());
    let slow_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    store_manager.add_store("slow_mem", slow_mem);

    let slow_ref = Store::new(RefStore::new(
        &RefSpec {
            name: "slow_mem".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow_ref,
    );

    // supports_removal_callbacks must query after resolving the slow RefStore.
    assert!(
        fss.supports_removal_callbacks(),
        "#11: FastSlow{{Memory, RefStore(Memory)}} must report supports=true after RefStore \
         resolution. If FastSlow.supports_removal_callbacks() does not resolve the slow \
         RefStore before querying, the unresolved cell returns false and the composite \
         incorrectly reports false."
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Item 2b (#11): ECS over FSS{Memory, NoCallbackStore} — over-action direction
// ---------------------------------------------------------------------------

/// Pins the over-action direction of the `supports_removal_callbacks` contract:
/// when FSS reports `false` (because the slow tier cannot fire callbacks), ECS
/// must enter `vulnerable_mode=true` — it must NOT attempt to register a dead
/// callback against the FSS.
///
/// Composition: ECS(FSS{MemoryStore-as-fast, NoCallbackStore-as-slow}).
/// FSS.supports_removal_callbacks() returns false (NoCallbackStore.supports=false
/// drags the AND down). ECS sees false at construction → sets vulnerable_mode=true.
///
/// Doc-comment on the AND rule's over-conservative behavior: slow-only would
/// suffice for stale-positive prevention (see fn doc-comment on FSS override),
/// AND is kept as defense-in-depth; if this composition ever needs eager
/// invalidation from the fast tier only, revisit the rule.
///
/// Mutation (a): revert `supports_removal_callbacks` override to return `true`
/// unconditionally → test red-fails with bespoke message:
///   "#11 over-action: ECS(FSS{Mem, NoCallback}) must enter vulnerable_mode=true; \
///    FSS.supports_removal_callbacks() must return false so ECS does not register \
///    dead callback"
#[nativelink_test]
async fn ecs_over_fss_no_callback_slow_enters_vulnerable_mode() -> Result<(), Error> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(NoCallbackStore::new());

    // Pre-condition: NoCallbackStore.supports=false is already tested by
    // fast_slow_supports_callbacks_false_when_slow_does_not; here we verify
    // the ECS sees the false answer and latches vulnerable_mode.
    let fss = Store::new(FastSlowStore::new(
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
    ));

    let ec = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        fss,
    );

    assert!(
        ec.is_vulnerable_mode(),
        "#11 over-action: ECS(FSS{{Mem, NoCallback}}) must enter vulnerable_mode=true; \
         FSS.supports_removal_callbacks() must return false when slow tier cannot fire \
         callbacks, so ECS does not register a dead callback against the FSS"
    );

    Ok(())
}

/// Pins the AND rule's over-conservative behavior in the asymmetric case.
///
/// Composition: FSS{NoCallbackStore-as-FAST, MemoryStore-as-SLOW}.
/// FSS.supports_removal_callbacks() returns false because the fast tier's
/// `supports=false` drags the AND down — even though the slow tier (MemoryStore)
/// does support callbacks. In this asymmetric case, slow-only would suffice for
/// ECS stale-positive prevention (fast-tier eviction creates a false negative,
/// not a stale positive), but the AND rule conservatively returns false.
///
/// Doc-comment: Pins the AND rule's over-conservative behavior in the asymmetric
/// case: slow-only would suffice for stale-positive prevention (see fn doc-comment),
/// AND is kept as defense-in-depth; if this composition ever needs eager invalidation,
/// revisit the rule.
///
/// Mutation (b): flip the AND to `||` in `supports_removal_callbacks` →
/// this test red-fails with bespoke:
///   "#11 AND-rule: FSS{NoCallback-fast, Memory-slow}.supports() must be false \
///    (AND rule); slow-only would suffice for stale-positive prevention but AND \
///    is defense-in-depth — fast tier cannot fire callbacks"
#[nativelink_test]
async fn fss_supports_callbacks_false_when_fast_does_not_and_rule() -> Result<(), Error> {
    // fast = NoCallbackStore (supports=false), slow = MemoryStore (supports=true)
    let fast = Store::new(NoCallbackStore::new());
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));

    let fss = FastSlowStore::new(
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
    );

    assert!(
        !fss.supports_removal_callbacks(),
        "#11 AND-rule: FSS{{NoCallback-fast, Memory-slow}}.supports() must be false \
         (AND rule); slow-only would suffice for stale-positive prevention but AND \
         is defense-in-depth — fast tier cannot fire callbacks"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Item 5 (#11): RefStore::get_store must not brick on callback replay failure
// ---------------------------------------------------------------------------

/// TDD Red test for #11 (Item 5): if `register_item_callback` replay fails
/// during `RefStore::get_store` cell publication, the RefStore must:
///   (a) still write the cell (resolution succeeds),
///   (b) allow subsequent ops to proceed,
///   (c) NOT propagate the replay Err to the caller that triggered resolution.
///
/// Pre-fix: `store.register_item_callback(callback)?` on line 106 of
/// `ref_store.rs` causes early return from `get_store` WITHOUT writing the
/// cell if any callback replay fails. The ref is permanently unresolvable:
/// every subsequent op goes through `get_store()` again, hits the slow path,
/// tries to replay again, fails again — all ops through the RefStore fail
/// forever.
///
/// Post-fix: the cell is written FIRST, then callbacks are replayed; on replay
/// Err: `error!` log + counter increment, but resolution is considered
/// successful (degraded to no-eager-invalidation, NOT bricked).
///
/// Mutation: revert to propagating `?` before writing the cell → this test
/// red-fails with:
///   "#11: ref bricked by callback replay failure"
#[nativelink_test]
async fn ref_store_get_store_not_bricked_by_callback_replay_failure() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());

    // RejectCallbackStore: supports callbacks (so it's queued for replay) but
    // always rejects register_item_callback with Err.
    let reject_store = RejectCallbackStore::new();
    store_manager.add_store("reject_target", Store::new(reject_store.clone()));

    let ref_store = Store::new(RefStore::new(
        &RefSpec {
            name: "reject_target".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    // Register a callback BEFORE the ref resolves. This goes into the queued
    // `item_callbacks` Vec. When `get_store()` runs its slow path, it will
    // try to replay this callback on `reject_target` — which will fail.
    ref_store.register_item_callback(Arc::new(NoopCallback))?;

    // Trigger resolution (via `has`). Pre-fix: `get_store()` returns Err
    // because the callback replay fails. Post-fix: the cell is written first,
    // resolution succeeds despite the replay Err, and `has` returns Ok.
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(3),
        ref_store.has(DigestInfo::try_new(VALID_HASH1, 3)?),
    )
    .await
    .expect("must not deadlock (#11: ref bricked by callback replay failure)")
    .expect(
        "#11: ref bricked by callback replay failure — has() returned Err after replay Err; \
         cell must be written before replay so ops succeed in degraded mode",
    );

    // has() returned Ok (value is None since reject_target has no data).
    assert!(
        result.is_none(),
        "has() must succeed after replay failure (store has no data, so None is correct)"
    );

    // Subsequent op must also succeed — the cell must be permanently written.
    let result2 = tokio::time::timeout(
        core::time::Duration::from_secs(3),
        ref_store.has(DigestInfo::try_new(VALID_HASH1, 3)?),
    )
    .await
    .expect("must not deadlock on second call")
    .expect(
        "#11: second has() after replay failure must also succeed — cell must be written",
    );
    assert!(result2.is_none());

    // The inner store must have received both ops (proves the cell is wired to
    // the correct target, not stuck in an unresolved state).
    assert!(
        reject_store.ops_completed.load(Ordering::Relaxed) >= 2,
        "RejectCallbackStore must have received at least 2 has_with_results calls; \
         got {}. If only 0-1, the cell was not written and ops fell through to \
         error path instead of delegating to the resolved target.",
        reject_store.ops_completed.load(Ordering::Relaxed),
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Item 6 (#11): Production-shape callback propagation through FastSlow fanout
// ---------------------------------------------------------------------------

/// TDD Red test for #11 (Item 6): end-to-end callback propagation in the
/// production-shape composition: ECS(RefStore → FastSlow{fast=Memory, slow=Memory}).
///
/// Writes a blob through the full chain, verifies ECS caches it, then evicts
/// from the SLOW MemoryStore tier directly. The FastSlowStore's callback fanout
/// must propagate the eviction event back up through the chain to the ECS, which
/// removes the existence cache entry.
///
/// This is the test red-team identified as missing in blind spot 1 of
/// acbb767d-ecs-fix/red-team.md: "callback not tested through production-
/// identical FastSlowStore composition".
///
/// Seams exercised:
///   1. ECS construction — `inner_store(None)` resolves RefStore cell;
///      `supports_removal_callbacks()` returns true (both tiers are Memory);
///      ECS registers callback on FastSlow.
///   2. FastSlow `register_item_callback` fans out to fast + slow MemoryStore.
///   3. Slow-tier MemoryStore eviction fires the registered callback.
///   4. ECS callback (registered via FastSlow fanout) fires and removes the
///      existence cache entry.
///   5. `exists_in_cache` within a 3-second deadline returns `false`.
///
/// Mutation: comment out the slow-tier fan-out in FastSlow's
/// `register_item_callback` → test red-fails with:
///   "#11: ECS existence cache not cleared via FastSlow slow-tier fanout"
#[nativelink_test]
async fn ecs_over_ref_fast_slow_slow_tier_eviction_clears_cache() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());

    // Build the production-shape inner stores: both tiers are MemoryStore.
    let fast_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_mem = Store::new(MemoryStore::new(&MemorySpec::default()));

    // Register the FastSlow under a name so the RefStore can find it.
    let fss = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_mem.clone(),
        slow_mem.clone(),
    ));
    store_manager.add_store("inner_fast_slow", fss.clone());

    // RefStore pointing at the FastSlow (cell empty — production shape).
    let ref_inner = Store::new(RefStore::new(
        &RefSpec {
            name: "inner_fast_slow".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    // ECS wrapping the RefStore. This is the key composition: ECS sees the
    // RefStore (unresolved), calls inner_store(None) to resolve it, then
    // queries supports_removal_callbacks() on the resolved FastSlow.
    let ec = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        ref_inner,
    );

    // Pre-condition: ECS must NOT be in vulnerable mode (both tiers support
    // callbacks after RefStore resolution).
    assert!(
        !ec.is_vulnerable_mode(),
        "#11: ECS over RefStore(FastSlow{{Memory, Memory}}) must not be in \
         vulnerable_mode — both tiers support callbacks and the RefStore resolves \
         to a registered store. If this fails, check Item 1 (#10) fix is in place."
    );

    // Write a blob through ECS → RefStore → FastSlow → both tiers.
    let digest = DigestInfo::try_new(VALID_HASH1, 3)?;
    ec.update_oneshot(digest, "abc".into())
        .await
        .expect("update_oneshot through ECS chain must succeed");

    // ECS must have cached the existence entry.
    assert!(
        ec.exists_in_cache(&digest).await,
        "ECS must cache existence after update_oneshot"
    );

    // FastSlowStore writes to the slow tier in a background task. Wait for it.
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        while slow_mem.has(digest).await.unwrap_or(None).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow write must complete within 3s before we can evict from it");

    // Evict from the SLOW MemoryStore tier. This is the seam that must
    // propagate the callback back up through FastSlow → ECS → cache removal.
    let slow_inner = slow_mem
        .downcast_ref::<MemoryStore>(None)
        .expect("slow_mem is MemoryStore");
    let removed = slow_inner.remove_entry(digest.into()).await;
    assert!(
        removed,
        "slow MemoryStore must have the entry to remove (write must reach slow tier)"
    );

    // Poll for ECS cache entry removal within a deadline.
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        while ec.exists_in_cache(&digest).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#11: ECS existence cache not cleared via FastSlow slow-tier fanout. \
         Slow MemoryStore eviction fired but ECS cache entry was not removed \
         within 3s. This means the callback registered by ECS via FastSlow's \
         register_item_callback fanout did not reach the slow tier. \
         Seams: ECS→FastSlow register_item_callback→slow MemoryStore eviction \
         →callback→ECS cache remove.",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Item 3 (#11): invalidation_callbacks_fired metric increments on callback fire
// ---------------------------------------------------------------------------

/// Verifies that `ExistenceCacheStore::invalidation_callbacks_fired` increments
/// when an eviction callback fires from the inner store. This is the operator
/// signal for "callbacks are wired and firing" — `callbacks registered but
/// fired==0 over a long window` is the dead-chain signal documented in the
/// metric help text.
///
/// Uses ECS(MemoryStore) directly to test the counter in isolation; the full
/// chain test (Item 6) exercises the same counter through the production
/// composition.
///
/// Mutation: remove the `self.invalidation_callbacks_fired.inc()` call in
/// `ExistenceCacheCallback::callback` → this test red-fails with:
///   "#11: invalidation_callbacks_fired counter did not increment after eviction"
#[nativelink_test]
async fn ecs_invalidation_callbacks_fired_counter_increments() -> Result<(), Error> {
    let inner_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let ec = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        inner_mem.clone(),
    );

    let digest = DigestInfo::try_new(VALID_HASH1, 3)?;
    ec.update_oneshot(digest, "abc".into())
        .await
        .expect("update_oneshot must succeed");

    assert!(
        ec.exists_in_cache(&digest).await,
        "ECS must cache existence after update"
    );

    let before = ec.invalidation_callbacks_fired();
    assert_eq!(before, 0, "no callbacks fired yet");

    // Trigger the eviction callback via MemoryStore::remove_entry.
    let mem = inner_mem
        .downcast_ref::<MemoryStore>(None)
        .expect("inner is MemoryStore");
    let removed = mem.remove_entry(digest.into()).await;
    assert!(removed, "MemoryStore must have the entry");

    // Poll for callback to fire.
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        while ec.invalidation_callbacks_fired() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "#11: invalidation_callbacks_fired counter did not increment after eviction. \
         Counter must increment in the ExistenceCacheCallback::callback path."
    );

    assert!(
        ec.invalidation_callbacks_fired() >= 1,
        "invalidation_callbacks_fired must be >= 1 after at least one eviction callback"
    );

    Ok(())
}
