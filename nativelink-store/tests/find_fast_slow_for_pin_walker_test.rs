// Copyright 2024-2026 The NativeLink Authors. All rights reserved.
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

//! Regression tests for the `find_fast_slow_for_pin` startup walker
//! (`nativelink-store::small_blob_dispatcher::find_fast_slow_for_pin`).
//!
//! Background: at server startup the SmallBlobDispatcher wire-up
//! registers a per-store `EphemeralServerSidePin` for every CAS-backing
//! chain that bottoms out at a `FastSlowStore`. The walker drills
//! through wrapper layers (`ExistenceCacheStore`, `VerifyStore`,
//! `CompletenessCheckingStore`) by recognising each wrapper type and
//! following its concrete inner accessor. If the walker fails to
//! recognise a wrapper that returns `self` from `inner_store(None)`,
//! registration silently no-ops and the dispatcher is inert in
//! production with no alarm.
//!
//! These tests cover both (a) the recursion path through the
//! `CompletenessCheckingStore` wrapper into the BACKEND-side
//! `FastSlowStore` Arc, and (b) the short-circuit on a non-FSS chain
//! returning `None`.
//!
//! ### Mutation step
//!
//! Empirical mutation: commenting out the
//! `CompletenessCheckingStore` arm in `find_fast_slow_for_pin` causes
//! `walker_recurses_through_completeness_checking_store_to_inner_fss`
//! to red-fail with the bespoke
//! `"walker must recurse through CompletenessCheckingStore to find
//! inner FSS — single-level downcast regression"` message. With the
//! arm in place the test passes. The bespoke message distinguishes a
//! real recursion regression from a `tokio::time::Elapsed` from the
//! 5-second deadlock detector.
//!
//! ### Asymmetric coverage
//!
//! The pair covers both directions of the contract: (a) the walker
//! MUST recurse through CCS when present (under-action: arm missing →
//! returns None even though chain bottoms at FSS); (b) the walker MUST
//! return None when no FSS exists (over-action: spurious downcast
//! widened or fall-through hit a stub FSS). Today's prod failure mode
//! is (a) — the AC-side dispatcher pin-set silently does not register
//! when the wrapper is not recognised — but (b) guards against a
//! future change that loosens the type-check.

use core::time::Duration;

use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::find_fast_slow_for_pin;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey};

/// Build a vanilla CAS-side `FastSlowStore` over Memory tiers.
fn fresh_fss() -> std::sync::Arc<FastSlowStore> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let spec = FastSlowSpec {
        fast: StoreSpec::Memory(MemorySpec::default()),
        slow: StoreSpec::Memory(MemorySpec::default()),
        fast_direction: StoreDirection::Both,
        slow_direction: StoreDirection::Both,
        chunked_reads_enabled: false,
    };
    FastSlowStore::new(&spec, fast, slow)
}

/// Asserts that the walker drills through the production-shaped CAS
/// chain
/// `CompletenessCheckingStore { backend: <ECS → VS → FSS>, cas_store: <FSS> }`
/// and returns the BACKEND-side FSS (the AC chain in production
/// naming, but reused here as a generic FSS-bottomed chain).
///
/// The recursion arm under test sits inside
/// [`find_fast_slow_for_pin`] and follows
/// `CompletenessCheckingStore::ac_store()` through any further
/// recognised wrappers (`ExistenceCacheStore`, `VerifyStore`) until
/// the underlying `FastSlowStore` Arc surfaces. The doc-comment on
/// `CompletenessCheckingStore::ac_store` promises this recursion; the
/// production wire-up depends on it for the dispatcher's AC pin-set
/// registration.
///
/// ### Bespoke assertion message
///
/// `expect("walker must recurse through CompletenessCheckingStore to find inner FSS — single-level downcast regression")`
///
/// is intentionally specific so a `tokio::time::Elapsed` from the
/// 5-second deadlock detector cannot be confused with a contract
/// violation. Production-composition mandate: `CompletenessCheckingStore`
/// holds two `Store` arms — only the BACKEND arm is what the AC pin
/// walker pursues, so the chain wrapping must look like production
/// (CCS over ECS/VS over FSS), not a bare FSS.
#[nativelink_test]
async fn walker_recurses_through_completeness_checking_store_to_inner_fss()
-> Result<(), Error> {
    // The backend chain: FastSlowStore wrapped in VerifyStore wrapped
    // in ExistenceCacheStore — mirrors the production
    // `AC_BACKEND_CACHED = ExistenceCache(Verify(FastSlow(MemoryStore,
    // RefStore→Redis)))` layout. Walker must descend through each
    // wrapper's concrete inner accessor.
    let inner_fss = fresh_fss();
    let inner_fss_ptr: *const FastSlowStore = std::sync::Arc::as_ptr(&inner_fss);

    let inner_store_for_verify = Store::new(inner_fss.clone());
    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        inner_store_for_verify,
    );
    let verify_store = Store::new(verify);
    let existence_cache = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy::default()),
        },
        verify_store,
    );
    let backend_chain = Store::new(existence_cache);

    // CAS-side arm: a separate FSS — the walker must NOT pick this one
    // when starting from CCS (the AC walker drills BACKEND-side only).
    let cas_fss = fresh_fss();
    let cas_chain = Store::new(cas_fss);

    let ccs = CompletenessCheckingStore::new(backend_chain, cas_chain);
    let outer: &dyn StoreDriver = ccs.as_ref();

    let walker_fut = async move {
        find_fast_slow_for_pin(outer).map(core::ptr::from_ref)
    };

    let found_ptr = tokio::time::timeout(Duration::from_secs(5), walker_fut)
        .await
        .expect("walker must complete within 5s — synchronous downcast cannot legitimately block")
        .expect(
            "walker must recurse through CompletenessCheckingStore to find inner FSS — \
             single-level downcast regression",
        );

    assert_eq!(
        found_ptr, inner_fss_ptr,
        "walker returned a different FastSlowStore than the BACKEND arm — \
         CompletenessCheckingStore recursion picked the wrong arm or stopped early"
    );
    Ok(())
}

/// Asserts that the walker returns `None` when handed a chain that
/// never bottoms out at a `FastSlowStore`. The ALL-MemoryStore case is
/// the canonical "no FSS" production-shaped scenario (e.g. a
/// development override that bypasses the slow tier).
///
/// The bespoke failure message distinguishes a real wrong-result
/// failure from a deadlock-detector elapsed.
#[nativelink_test]
async fn walker_returns_none_for_non_fss_chain() -> Result<(), Error> {
    let mem = MemoryStore::new(&MemorySpec::default());
    // Touch StoreKey/DigestInfo so the import lives even if a future
    // refactor removes references inside the helper modules. Provides
    // a stable compile-time guard the test stays wired to the public
    // store_trait surface.
    let _key: StoreKey<'_> = StoreKey::Digest(DigestInfo::new([0u8; 32], 0));
    let driver: &dyn StoreDriver = mem.as_ref();

    let walker_fut = async move {
        find_fast_slow_for_pin(driver).is_some()
    };

    let found = tokio::time::timeout(Duration::from_secs(5), walker_fut)
        .await
        .expect(
            "walker must complete within 5s — synchronous downcast cannot legitimately \
             block on a non-FSS chain",
        );

    assert!(
        !found,
        "walker must return None for a chain with no FastSlowStore — \
         spurious hit on a MemoryStore-only chain indicates the as_any \
         downcast accidentally widened or a recursion arm fell through \
         to a wrapper that returned a stub FSS"
    );
    Ok(())
}
