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

use core::fmt::Debug;
use core::future::Future;
use core::pin::Pin;
use std::sync::Arc;

/// Trait for entries that report their byte length, used by evicting map
/// implementations (`MokaEvictingMap`) to track total stored size and
/// enforce eviction policies.
pub trait LenEntry: 'static {
    /// Length of referenced data.
    fn len(&self) -> u64;

    /// Returns `true` if `self` has zero length.
    fn is_empty(&self) -> bool;

    /// (#locality-map-drift) Read this value's per-mutation logical LWW
    /// counter, frozen into the value AT INSERT by
    /// [`crate::moka_evicting_map::MokaEvictingMap`] (`set_stamp`). The
    /// eviction listener reads it off the EVICTED value so the holdings
    /// eviction delta carries the evicted value's frozen counter — NOT a
    /// fresh mint — which is what makes `ts_evict(V) < ts_reinsert` hold under
    /// out-of-order async callback delivery. Default `0`: value types whose
    /// map has no holdings tracker (e.g. `MemoryStore`'s) never stamp, and
    /// their `ItemCallback` consumes the ts as a no-op.
    #[inline]
    fn stamp(&self) -> u64 {
        0
    }

    /// (#locality-map-drift) Freeze this value's per-mutation logical LWW
    /// counter. Called by `MokaEvictingMap` under the insert path (before
    /// `cache.insert`) so the counter travels with the value through moka's
    /// cache slot and is readable off the evicted value later. Default no-op
    /// for value types that don't participate in holdings tracking. Requires
    /// interior mutability (values are shared behind `Arc`); the production
    /// value `FileEntryImpl` uses an `AtomicU64`.
    #[inline]
    fn set_stamp(&self, _stamp: u64) {}

    /// This will be called when object is removed from map.
    /// Note: There may still be a reference to it held somewhere else, which
    /// is why it can't be mutable. This is a good place to mark the item
    /// to be deleted and then in the Drop call actually do the deleting.
    /// This will ensure nowhere else in the program still holds a reference
    /// to this object.
    /// You should not rely only on the Drop trait. Doing so might result in the
    /// program safely shutting down and calling the Drop method on each object,
    /// which if you are deleting items you may not want to do.
    /// It is undefined behavior to have `unref()` called more than once.
    /// During the execution of `unref()` no items can be added or removed to/from
    /// the evicting map globally (including inside `unref()`).
    #[inline]
    fn unref(&self) -> impl Future<Output = ()> + Send {
        core::future::ready(())
    }
}

impl<T: LenEntry + Send + Sync> LenEntry for Arc<T> {
    #[inline]
    fn len(&self) -> u64 {
        T::len(self.as_ref())
    }

    #[inline]
    fn is_empty(&self) -> bool {
        T::is_empty(self.as_ref())
    }

    #[inline]
    fn stamp(&self) -> u64 {
        T::stamp(self.as_ref())
    }

    #[inline]
    fn set_stamp(&self, stamp: u64) {
        T::set_stamp(self.as_ref(), stamp);
    }

    #[inline]
    async fn unref(&self) {
        self.as_ref().unref().await;
    }
}

/// Callback invoked when an evicting map inserts or removes an item.
///
/// (#locality-map-drift) `callback`/`on_insert`/`on_get` carry a per-mutation
/// logical LWW timestamp `(ts_boot_epoch, ts_counter)`. `on_insert` gets a
/// FRESHLY-minted counter (the value's insert ts); `callback` (eviction) gets
/// the EVICTED value's FROZEN counter (read off the value, never re-minted);
/// `on_get` gets a fresh counter (a read is a fresh PRESENT transition that
/// must be able to supersede a stale evict). `ts_boot_epoch` is the map's
/// per-process boot epoch (constant for the map's lifetime).
pub trait ItemCallback<Q>: Debug + Send + Sync {
    fn callback(
        &self,
        store_key: &Q,
        ts_boot_epoch: u64,
        ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Called synchronously when a new item is inserted.
    /// Default is a no-op.
    fn on_insert(&self, _store_key: &Q, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {}

    /// Fired when a key is read (cache hit) via the public `get` /
    /// `get_many` paths of the evicting map. Intentionally NOT fired
    /// from existence-check paths such as `sizes_for_keys`, nor from
    /// the internal `cache.get` used to capture replaced values inside
    /// `insert_inner`. Default is a no-op.
    fn on_get(&self, _store_key: &Q, _ts_boot_epoch: u64, _ts_counter: u64) {}

    /// Fired when a pin auto-expires (the pinned entry crossed the
    /// `PIN_TIMEOUT_SECS` deadline without being explicitly unpinned).
    /// Distinct from `callback` (eviction): the blob is NOT removed
    /// from the map, just demoted from "pinned" back to LRU-evictable.
    /// `FastSlowStore` listens on this hook so a slow-write that hangs
    /// past the pin deadline still gets recorded in `failed_slow_writes`
    /// for retry-on-reconnect — closing the durability gap that an
    /// auto-unpin would otherwise silently open. Default is a no-op.
    fn on_pin_expired(&self, _store_key: &Q, _size: u64) {}

    /// FL-688 advertise-on-pin: fired when a key crosses into an INDEFINITE
    /// (held-until-BIS-ack) pin. Distinct from `on_insert`: the pin path moves
    /// the entry into the `pinned` map without an insert/get, so this is the
    /// only holdings signal for a re-produced already-resident indefinitely-
    /// pinned blob. Carries a FRESHLY-minted `(ts_boot_epoch, ts_counter)`
    /// (frozen into the value) so the holdings tracker's PRESENT delta strictly
    /// out-ranks any prior evict of this key. Fired once per pin lifecycle.
    /// Default is a no-op.
    fn on_pin(&self, _store_key: &Q, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {}
}

#[derive(Debug, Clone, Copy)]
pub struct NoopCallback;

impl<Q> ItemCallback<Q> for NoopCallback {
    fn callback(
        &self,
        _store_key: &Q,
        _ts_boot_epoch: u64,
        _ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn on_insert(&self, _store_key: &Q, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {}

    fn on_get(&self, _store_key: &Q, _ts_boot_epoch: u64, _ts_counter: u64) {}

    fn on_pin_expired(&self, _store_key: &Q, _size: u64) {}

    fn on_pin(&self, _store_key: &Q, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {}
}
