// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Canonical wrapper-chain walker for finding a [`FastSlowStore`] inside
//! the production CAS wrapper chain
//! (`WorkerProxyStore` → `ExistenceCacheStore` → `VerifyStore` →
//! `SizePartitioningStore(16 KiB)` → `FastSlowStore`).
//!
//! ## Why this exists
//!
//! Two consumers (the `chunked_fast_slow` dispatcher wiring in
//! `src/bin/nativelink.rs` and the `failed_writes_drain::drain_tick`
//! V3 self-retry path in `nativelink-service`) need to find the
//! `FastSlowStore` inside whatever production composition the operator
//! configured for `cas_STORE`. Both used to maintain near-identical
//! ad-hoc walkers; that meant any fix to one (e.g., `synthetic_large_key()`
//! to descend `SizePartitioningStore`) had to be remembered for the
//! other. This module centralises the upper-arm walker so both consumers
//! share a single implementation that handles the production composition
//! correctly.
//!
//! Note: a third walker variant lives in
//! [`crate::small_blob_dispatcher::find_fast_slow_for_pin`] which uses
//! a synthetic *small* key (size `0`) to descend
//! `SizePartitioningStore`'s lower arm (the small-blob FSS); that
//! consumer has different semantics and stays separate.
//! TODO(#303): once chunked-dispatcher consolidation lands, evaluate
//! whether the small-key walker can also fold into this module
//! parameterised by the hint size.
//!
//! ## The size-partitioning trap
//!
//! `SizePartitioningStore::inner_store(None)` returns `self`. A walker
//! that calls `inner_store(None)` therefore terminates at the
//! partitioning boundary without descending — even though the
//! `FastSlowStore` is one level below the upper-arm side of the
//! partition. Passing [`synthetic_large_key()`] (a digest with
//! `size_bytes == u64::MAX`) routes the walker into the upper arm,
//! which is the side that holds the `FilesystemStore`-backed
//! `FastSlowStore` in production (`size_partitioning.size = 16384`
//! in `prod-server.json5`).
//!
//! Wrappers that do NOT shadow `inner_store` are traversed by
//! delegation; wrappers that shadow `inner_store` to return `self`
//! (e.g. `ExistenceCacheStore`, `VerifyStore`) are special-cased via
//! direct downcast and recursion through their typed `inner_store()`
//! accessor (which returns the wrapped `Store`, not `self`).
//!
//! ## Failure mode of an absent walk
//!
//! If the chain doesn't terminate in a `FastSlowStore`, the walker
//! returns `None`. Callers must treat `None` as "feature unavailable
//! for this composition" — not as an error. The `failed_writes_drain`
//! drainer falls back to its pre-#335 worker-only retry behaviour;
//! the chunked dispatcher logs and skips wiring for that store.

use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{StoreDriver, StoreKey};

use crate::existence_cache_store::ExistenceCacheStore;
use crate::fast_slow_store::FastSlowStore;
use crate::size_partitioning_store::SizePartitioningStore;
use crate::verify_store::VerifyStore;

/// A digest with `size_bytes == u64::MAX` used to descend
/// `SizePartitioningStore` into its `upper_store` arm.
///
/// Production `cas_INNER` partitions on `16384` bytes
/// (`prod-server.json5`); `u64::MAX` is greater than any plausible
/// partition threshold, so the walker reliably lands in the
/// upper-arm `FastSlowStore`. RefStore / ExistenceCacheStore /
/// VerifyStore are key-agnostic for the descent so threading the
/// key through them is a no-op.
#[must_use]
pub fn synthetic_large_key() -> StoreKey<'static> {
    StoreKey::Digest(DigestInfo::new([0u8; 32], u64::MAX))
}

/// Walk the production CAS wrapper chain to find the underlying
/// upper-arm [`FastSlowStore`].
///
/// Returns `None` if the chain doesn't terminate in an FSS (e.g.
/// pure-test compositions wrapping a `MemoryStore` directly with
/// no FSS in the path) — caller treats `None` as "feature
/// unavailable for this composition" and falls back to its pre-feature
/// behaviour.
///
/// See module-level docs for the descent strategy and the
/// size-partitioning trap.
#[must_use]
pub fn find_fast_slow_via_chain(store: &dyn StoreDriver) -> Option<&FastSlowStore> {
    if let Some(fss) = store.as_any().downcast_ref::<FastSlowStore>() {
        return Some(fss);
    }
    if let Some(ecs) = store
        .as_any()
        .downcast_ref::<ExistenceCacheStore<std::time::SystemTime>>()
    {
        // ExistenceCacheStore::inner_store (the typed accessor)
        // returns the wrapped `Store`; recurse through its driver
        // with the synthetic key so any inner SizePartitioningStore
        // descends into its upper arm.
        return find_fast_slow_via_chain(
            ecs.inner_store().inner_store(Some(synthetic_large_key())),
        );
    }
    if let Some(vs) = store.as_any().downcast_ref::<VerifyStore>() {
        return find_fast_slow_via_chain(
            vs.inner_store().inner_store(Some(synthetic_large_key())),
        );
    }
    // Generic fallback: most wrappers (e.g. WorkerProxyStore,
    // RefStore) implement `inner_store` to delegate; SizePartitioning
    // implements it to dispatch on the key (hence the synthetic
    // large key). Wrappers that shadow `inner_store` to return
    // `self` terminate the walk via the ptr-eq guard below.
    let inner = store.inner_store(Some(synthetic_large_key()));
    if core::ptr::eq(
        inner as *const dyn StoreDriver,
        store as *const dyn StoreDriver,
    ) {
        return None;
    }
    find_fast_slow_via_chain(inner)
}

/// #FL-688: read the live `SizePartitioning` threshold for the CAS chain
/// starting at `store`, descending the same wrappers as
/// [`find_fast_slow_via_chain`].
///
/// The no-peer degraded ack-gate path needs to route a blob's confirming
/// slow-tier write to the SAME `SizePartitioning` arm that reads will route
/// to — otherwise the 2nd replica lands where no read looks (the lower arm is
/// Redis-backed, the upper is `FilesystemStore`-backed in production). Rather
/// than hardcode the production `16384` (config-driven, may drift), this reads
/// the store's own `partition_size()`.
///
/// Returns `None` when the chain has no `SizePartitioningStore` (e.g. a flat
/// test composition), in which case the caller routes through the single FSS
/// the chain DOES expose. `SizePartitioningStore::inner_store(None)` returns
/// `self`, so it is found by direct downcast, not delegation.
#[must_use]
pub fn find_partition_size(store: &dyn StoreDriver) -> Option<u64> {
    if let Some(sp) = store.as_any().downcast_ref::<SizePartitioningStore>() {
        return Some(sp.partition_size());
    }
    if let Some(ecs) = store
        .as_any()
        .downcast_ref::<ExistenceCacheStore<std::time::SystemTime>>()
    {
        // `Store::inner_store(None)` returns the wrapped driver (these typed
        // accessors hand back a `&Store`; the `None` key steps one layer in).
        return find_partition_size(ecs.inner_store().inner_store(None::<StoreKey<'_>>));
    }
    if let Some(vs) = store.as_any().downcast_ref::<VerifyStore>() {
        return find_partition_size(vs.inner_store().inner_store(None::<StoreKey<'_>>));
    }
    // Generic single-step delegation (e.g. WorkerProxyStore). Use a
    // size-agnostic `None` key: we are looking for the partition store
    // itself, not descending one of its arms. The ptr-eq guard stops at any
    // wrapper that shadows `inner_store` to return `self` and is NOT a
    // SizePartitioningStore.
    let inner = store.inner_store(None);
    if core::ptr::eq(
        inner as *const dyn StoreDriver,
        store as *const dyn StoreDriver,
    ) {
        return None;
    }
    find_partition_size(inner)
}
