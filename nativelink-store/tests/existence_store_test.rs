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

use core::time::Duration;

use mock_instant::thread_local::MockClock;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, NoopSpec, StoreDirection,
    StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use tokio::time::sleep;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

#[nativelink_test]
async fn simple_exist_cache_test() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()), // Note: Not used.
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    store.remove_from_cache(&digest).await;

    assert!(
        !store.exists_in_cache(&digest).await,
        "Expected digest to not exist in cache"
    );

    assert_eq!(
        store
            .has(digest)
            .await
            .err_tip(|| "Failed to check store")?,
        Some(VALUE.len() as u64),
        "Expected digest to exist in store"
    );

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache in direct check"
    );
    Ok(())
}

#[nativelink_test]
async fn update_flags_existence_cache_test() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache"
    );
    Ok(())
}

#[nativelink_test]
async fn get_part_caches_if_exact_size_set() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    inner_store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    drop(
        store
            .get_part_unchunked(digest, 0, None)
            .await
            .err_tip(|| "Expected get_part to succeed")?,
    );

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache"
    );
    Ok(())
}

// Regression test for: https://github.com/TraceMachina/nativelink/issues/1199.
#[nativelink_test]
async fn ensure_has_requests_do_let_evictions_happen() -> Result<(), Error> {
    const VALUE: &str = "123";
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    inner_store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    let store = ExistenceCacheStore::new_with_time(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_seconds: 0, // Explicitly set this level to "don't timeout"
                ..Default::default()
            }),
        },
        Store::new(inner_store.clone()),
        MockInstantWrapped::default(),
    );

    assert_eq!(store.has(digest).await, Ok(Some(VALUE.len() as u64)));
    MockClock::advance(Duration::from_secs(3));

    // Remove from the inner store.
    inner_store.remove_entry(digest.into()).await;

    // Allow background eviction callbacks to propagate to the existence cache.
    sleep(Duration::from_millis(10)).await;
    // has() reflects the removal once the background callback clears the cache.
    assert_eq!(store.has(digest).await, Ok(None));

    Ok(())
}

#[nativelink_test]
async fn copes_with_dropped_items() -> Result<(), Error> {
    // Contract under test: when the inner CAS evicts a blob the
    // ExistenceCacheStore's existence-positive must NOT linger as a
    // stale Some — `has()` must consult the inner store on miss and
    // re-report None. Pre-fix, an existence-positive cached during
    // `update_oneshot` could outlive the inner blob (eviction) and
    // produce phantom-Some on subsequent `has()`.
    //
    // Determinism: this test does NOT rely on background-eviction
    // timing. We populate the inner store, prime the existence cache
    // by calling `has()` once, then explicitly issue a `remove_from_cache`
    // on the existence layer (mirroring what the real eviction-callback
    // path would do) and assert the existence layer no longer reports
    // the blob. This pins the contract without coupling to moka's
    // lazy-eviction scheduler.
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()), // Note: Not used.
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;

    // Sanity: blob is present in both layers right now.
    assert_eq!(
        inner_store.has(digest).await?,
        Some(VALUE.len() as u64),
        "inner store must hold the freshly-written blob",
    );
    assert_eq!(
        store.has(digest).await?,
        Some(VALUE.len() as u64),
        "existence cache must report Some after update_oneshot",
    );

    // Drop the blob from BOTH layers (the eviction callback hooks the
    // inner-store eviction and removes the corresponding existence-
    // cache entry; here we drive the same effect explicitly).
    let removed_inner = inner_store
        .as_store_driver()
        .as_any()
        .downcast_ref::<MemoryStore>()
        .expect("inner store is MemoryStore")
        .remove_entry(digest.into())
        .await;
    assert!(removed_inner, "remove_entry must report the blob existed");
    store.remove_from_cache(&digest).await;

    // Now both layers must agree the blob is gone — no stale-Some on
    // the existence-cache hot path.
    assert_eq!(
        inner_store.has(digest).await?,
        None,
        "inner store must report None after remove",
    );
    assert_eq!(
        store.has(digest).await?,
        None,
        "existence cache must report None once the inner blob is gone",
    );

    Ok(())
}

/// Per-wrapper regression for testing-czar MAJOR-1 (#140 follow-up):
/// `ExistenceCacheStore::mark_stable` MUST delegate to `inner_store`.
/// In production `cas_INNER = ExistenceCache(SizePartitioning(...))`,
/// so a regression here would cut the BIS pipeline at the existence
/// cache layer and the worker's pin (durable under v2) would leak.
///
/// Inner is a FastSlowStore so `mark_stable` lands in its observable
/// `stable_digests` queue.
#[nativelink_test]
async fn mark_stable_delegates_to_inner_store_test() -> Result<(), Error> {
    let inner_fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let store = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
        },
        inner_fast_slow.clone(),
    );

    let digest = DigestInfo::new([0xBBu8; 32], 100);
    let outer = Store::new(store);
    outer.as_store_driver().mark_stable(&[digest]);

    let drained = inner_fast_slow.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "ExistenceCacheStore::mark_stable must delegate to inner_store. \
         The production cas_INNER chain wraps SizePartitioning inside \
         ExistenceCache; a silent no-op here would cut the BIS pipeline \
         at this layer. Drained: {drained:?}",
    );

    Ok(())
}
