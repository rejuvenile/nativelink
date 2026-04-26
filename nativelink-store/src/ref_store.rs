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
use std::sync::{Arc, Weak};

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
    ItemCallback, PinDelegation, StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike,
    UploadSizeInfo,
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
    item_callbacks: Mutex<Vec<Arc<dyn ItemCallback>>>,
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
        })
    }

    // This will get the store or populate it if needed. It is designed to be quite fast on the
    // common path, but slow on the uncommon path. It does use some unsafe functions because we
    // wanted it to be fast. It is technically possible on some platforms for this function to
    // create a data race here is the reason I do not believe it is an issue:
    // 1. It would only happen on the very first call of the function (after first call we are safe)
    // 2. It should only happen on platforms that are < 64 bit address space
    // 3. It is likely that the internals of how Option work protect us anyway.
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
            for callback in item_callbacks {
                store.register_item_callback(callback)?;
            }
            unsafe {
                *ref_store = Some(store);
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
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.get_store()?.has_with_results(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.get_store()?.update(key, reader, size_info).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_store()?
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        match self.get_store() {
            Ok(store) => store.inner_store(key),
            Err(err) => {
                error!(?key, ?err, "Failed to get store for key",);
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
}

default_health_status_indicator!(RefStore);
