// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use core::cell::UnsafeCell;
use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Weak;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::error;

use nativelink_config::stores::RefSpec;
use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

use crate::store_manager::StoreManager;

#[repr(C, align(8))]
#[derive(Debug)]
struct AlignedStoreCell(UnsafeCell<Option<Store>>);

#[derive(Debug)]
struct StoreReference {
    cell: AlignedStoreCell,
    mux: Mutex<()>,
}

unsafe impl Sync for StoreReference {}

#[derive(Debug, MetricsComponent)]
pub struct RefStore {
    #[metric(help = "The store we are referencing")]
    name: String,
    store_manager: Weak<StoreManager>,
    inner: StoreReference,
    // UNBOUNDED-OK: bounded O(wrapper-depth) at construction; not attacker-controlled
    item_callbacks: Mutex<Vec<Arc<dyn ItemCallback>>>,
    /// #11 (2026-06-10): counts register_item_callback replay failures during
    /// get_store() cell publication. A non-zero value means this RefStore was
    /// resolved successfully but at least one pre-queued callback could not be
    /// forwarded to the inner store. The store is usable (cell is written,
    /// all ops succeed) but eager-invalidation callbacks from those registrations
    /// will not fire. Operator-visible via `error!` log at replay time and via
    /// this metric. (#11 Item 5 — cell-write-before-replay contract.)
    #[metric(help = "register_item_callback replay failures during cell publication; \
                     store resolved but pre-queued callbacks lost (degraded, no panic)")]
    callback_replay_failures: AtomicU64,
}

impl RefStore {
    pub fn new(spec: &RefSpec, store_manager: Weak<StoreManager>) -> Arc<Self> {
        Arc::new(Self {
            name: spec.name.clone(),
            store_manager,
            inner: StoreReference {
                mux: Mutex::new(()),
                cell: AlignedStoreCell(UnsafeCell::new(None)),
            },
            item_callbacks: Mutex::new(vec![]),
            callback_replay_failures: AtomicU64::new(0),
        })
    }

    #[inline]
    fn get_store(&self) -> Result<&Store, Error> {
        let ref_store = self.inner.cell.0.get();
        unsafe {
            if let Some(ref store) = *ref_store {
                return Ok(store);
            }
        }
        // This should protect us against multiple writers writing the same location at the same
        // time. We must also hold this lock across the callback snapshot + publish below so that
        // a concurrent `register_item_callback` cannot observe `*ref_store == None`, push its
        // callback, and have us publish a snapshot taken before that push (silently dropping the
        // callback). See also `register_item_callback`.
        let _lock = self.inner.mux.lock();
        // Re-check after taking the lock: another caller may have populated `ref_store`
        // between our fast-path read and acquiring the mutex.
        unsafe {
            if let Some(ref store) = *ref_store {
                return Ok(store);
            }
        }
        let store_manager = self
            .store_manager
            .upgrade()
            .err_tip(|| "Store manager is gone")?;
        if let Some(store) = store_manager.get_store(&self.name) {
            let item_callbacks = self.item_callbacks.lock().clone();
            // #11 Item 5 (2026-06-10): write the cell BEFORE replaying callbacks.
            // Pre-fix: `store.register_item_callback(callback)?` inside the loop
            // returned Err WITHOUT writing the cell, permanently bricking the
            // RefStore — every subsequent op re-entered the slow path, tried to
            // replay again, failed again, and all ops through this RefStore failed
            // forever. The fix: publish the cell first, then replay; on replay Err:
            // log + increment counter, but treat resolution as succeeded (degraded
            // to no-eager-invalidation, NOT bricked). Callers that triggered
            // `get_store()` via a normal op (has/update/get_part) will proceed
            // normally even if some callbacks failed to register.
            unsafe {
                *ref_store = Some(store);
            }
            // Replay queued callbacks against the newly resolved store. Errors are
            // degradations, not failures: the resolution already succeeded above.
            let resolved_store = unsafe { (*ref_store).as_ref().unwrap() };
            for callback in item_callbacks {
                if let Err(err) = resolved_store.register_item_callback(callback) {
                    self.callback_replay_failures.fetch_add(1, Ordering::Relaxed);
                    error!(
                        ?err,
                        name = %self.name,
                        "RefStore: register_item_callback replay failed during cell publication; \
                         resolution succeeded but this callback will not fire on evictions. \
                         Store is usable in degraded mode (no-eager-invalidation for this \
                         callback). (#11 Item 5)"
                    );
                }
            }
            unsafe {
                return Ok((*ref_store).as_ref().unwrap());
            }
        }
        Err(make_input_err!(
            "Failed to find store '{}' in StoreManager in RefStore",
            self.name
        ))
    }
}

#[async_trait]
impl StoreDriver for RefStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        // RefStore resolves its inner store lazily on first access via
        // `get_store()` (AlignedStoreCell + queued-callback replay); there is
        // no eager binding to do at post-init time.
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.get_store()?.has_with_results(keys, results).await
    }

    /// Resolve the ref target and forward the DURABLE-presence query
    /// (durability-ack v3 §3.0). Mirrors `has_with_results`'s lazy resolve.
    async fn has_durably(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.get_store()?.has_durably(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.get_store()?.update(key, reader, size_info).await
    }

    async fn remove(self: Pin<&Self>, key: StoreKey<'_>) -> Result<(), Error> {
        self.get_store()?.remove(key).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Writer-termination contract: `self.get_store()?` early-returns Err
        // without first sending the error through the writer. If a caller
        // wraps this RefStore in `VerifyStore` (or any composer that joins
        // a paired reader on this writer's other half), the missing
        // termination deadlocks `check_fut` forever on `rx.recv().await`.
        // Resolve the inner store explicitly and `send_error` BEFORE the
        // early return so the paired reader unblocks with the structured Err.
        let store = match self.get_store() {
            Ok(store) => store,
            Err(err) => {
                writer.send_error(err.clone());
                return Err(err);
            }
        };
        store.get_part(key, writer, offset, length).await
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        match self.get_store() {
            Ok(store) => store.inner_store(key),
            Err(err) => {
                error!(?key, ?err, "Failed to get store for key");
                self
            }
        }
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        // Hold `inner.mux` across the push + read-of-ref_store so that we cannot interleave
        // with `get_store()`'s slow path between the snapshot of `item_callbacks` and the
        // publish of `*ref_store`. Without this, a slow-path init could publish an inner store
        // that is missing this callback, and we would see `*ref_store == None` here and skip
        // direct propagation — silently dropping the callback forever.
        let _lock = self.inner.mux.lock();
        self.item_callbacks.lock().push(callback.clone());
        let ref_store = self.inner.cell.0.get();
        unsafe {
            if let Some(ref store) = *ref_store {
                store.register_item_callback(callback)?;
            }
        }
        Ok(())
    }

    /// Forward to the resolved inner if available; otherwise return
    /// `false` so wrappers that consult this flag at registration time
    /// emit an operator-visible warn for silent-no-op slow tiers.
    ///
    /// **Unresolved-cell behavior**: production wires `RefStore` for
    /// late-binding (e.g. `AC_BACKEND_CACHED.slow = ref(REDIS_AC_STORE)`),
    /// and the cell IS empty when `FastSlowStore::new` calls
    /// `register_slow_eviction_stable_set_listener` (#367) at
    /// construction. Returning `false` for the unresolved case
    /// surfaces the warn at startup, alerting the operator that the
    /// listener will be silent until the inner resolves AND supports
    /// callbacks. Returning `true` here would silently suppress the
    /// warn for the most common production composition (DSR M2 finding,
    /// 2026-05-11).
    // GUARD (#9, 2026-06-10): do NOT "clean this up" to auto-resolve the cell
    // via get_store() — false-for-unresolved is a deliberate contract
    // (cd980946/#367): a ref to a store missing from the StoreManager must
    // degrade to vulnerable_mode with an operator-visible error, never panic
    // or self-resolve at query time. Callers that can guarantee the target is
    // registered must resolve explicitly (inner_store(None)) BEFORE querying,
    // as ExistenceCacheStore::new_with_time now does.
    fn supports_removal_callbacks(&self) -> bool {
        let ref_store = self.inner.cell.0.get();
        unsafe {
            if let Some(ref store) = *ref_store {
                return store.supports_removal_callbacks();
            }
        }
        // Inner not resolved yet; conservative `false` ensures the
        // operator-visible warn fires for late-binding compositions.
        false
    }

    /// RefStore resolves its inner store lazily. We cannot safely return a
    /// `Passthrough(s)` borrow at trait-dispatch time because `get_store()`
    /// can fail (returns Err if the named store is missing from the
    /// manager) and the borrow's lifetime would have to outlive the
    /// transient `Arc` `get_store()` returns.
    ///
    /// **Why `Leaf` instead of adding a `Lazy` variant:** every method that
    /// would dispatch via the delegation enum (`drain_stable_digests`,
    /// `stable_notify`, `pin_digests`, `pin_digests_with_results`) is
    /// already overridden below with the same `match self.get_store()`
    /// pattern. Declaring `Leaf` makes the trait default body a no-op, and
    /// the explicit overrides do the lazy resolution AND the missing-store
    /// degradation (empty drains, never-woken notify, silent pin no-op —
    /// matching prior semantics). A dedicated `Lazy` enum variant would
    /// add API surface to `StableDigestDelegation` / `PinDelegation` for
    /// exactly one wrapper; the override pattern is the right tradeoff.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    /// See [`Self::stable_delegation`] — same `Leaf`-plus-explicit-override
    /// pattern; `pin_digests` below is the override.
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    /// See [`Self::stable_delegation`] — same `Leaf`-plus-explicit-override
    /// pattern; the `mark_stable` override below lazily resolves the inner
    /// store via `get_store()` and forwards (matching the existing
    /// drain/notify lazy-resolve semantics). Declaring `Leaf` here makes
    /// the trait default a no-op; the override owns dispatch. (Task #157.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }

    /// Same `Leaf`-plus-explicit-override pattern: the `has_durably`
    /// override (in the StoreDriver impl above) lazily resolves the ref
    /// target via `get_store()` and forwards the durable-presence query, so
    /// a RefStore-mediated chain (e.g. `Verify(Ref(cas_INNER))`) routes
    /// durability correctly. `Leaf` makes the trait default report ABSENT;
    /// the override owns dispatch. (durability-ack v3 §3.0.)
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }

    fn drain_stable_digests(&self) -> Vec<DigestInfo> {
        match self.get_store() {
            Ok(store) => store.drain_stable_digests(),
            Err(_) => Vec::new(),
        }
    }

    fn drain_failed_digests(&self) -> Vec<DigestInfo> {
        match self.get_store() {
            Ok(store) => store.drain_failed_digests(),
            Err(_) => Vec::new(),
        }
    }

    fn stable_notify(&self) -> Arc<Notify> {
        match self.get_store() {
            Ok(store) => store.stable_notify(),
            Err(_) => {
                // Fall back to default (never-woken) notify.
                static NOOP_NOTIFY: std::sync::OnceLock<Arc<Notify>> = std::sync::OnceLock::new();
                NOOP_NOTIFY
                    .get_or_init(|| Arc::new(Notify::new()))
                    .clone()
            }
        }
    }

    fn mark_stable(&self, digests: &[DigestInfo]) {
        if let Ok(store) = self.get_store() {
            store.mark_stable(digests);
        }
        // Err: ref target unresolved — no-op, matches drain_stable_digests semantics.
    }

    fn pin_digests(&self, digests: &[DigestInfo]) {
        if let Ok(store) = self.get_store() {
            store.pin_digests(digests);
        }
    }

    /// #334 Fix C: forward unpin to the resolved ref target so the BIS
    /// broadcast loop can release server-side fast-tier pins through
    /// any RefStore-mediated chain. Mirrors `pin_digests` above.
    fn unpin_digests(&self, digests: &[DigestInfo]) {
        if let Ok(store) = self.get_store() {
            store.unpin_digests(digests);
        }
    }
}

default_health_status_indicator!(RefStore);
