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

//! Regression tests for #9: `ExistenceCacheStore::new_with_time` must
//! resolve a `RefStore` inner before querying `supports_removal_callbacks()`.
//!
//! Root cause: `supports_removal_callbacks()` on an UNRESOLVED `RefStore`
//! returns `false` by design (commit cd980946, #367 — preserves the
//! operator-visible warn for genuinely-unregistered refs). For an ECS
//! whose inner is `RefStore { name: "AC_BACKEND_CACHED", cell: None }` this
//! means `vulnerable_mode` latches `true` at every boot, losing eager
//! invalidation. Fix: resolve the ref via `inner_store(None)` (which calls
//! `RefStore::get_store()` and populates the cell) BEFORE the capability
//! query.
//!
//! Invariant: an ECS over a registered-but-ref-wrapped backend MUST register
//! its eviction callback (the "eviction corner" of the gate/eviction/pin
//! triangle for the ECS stale-positive contract). `vulnerable_mode=true` is
//! the symptom; a latched `false` on the capability query before the ref
//! resolves is the mechanism.
//!
//! cd980946's deliberate `false`-for-unresolved contract is preserved: when
//! the ref target name is NOT registered in the `StoreManager` the ECS still
//! constructs in `vulnerable_mode=true` WITHOUT panicking.

use std::sync::Arc;

use nativelink_config::stores::{ExistenceCacheSpec, MemorySpec, NoopSpec, RefSpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Test 1 (TDD RED against unmodified code, GREEN after fix):
///
/// Build a `StoreManager` with a real `MemoryStore` registered under
/// "ac_backend_cached". Wrap it in a `RefStore` (unresolved at construction —
/// the cell is empty). Wrap that in an `ExistenceCacheStore`.
///
/// Pre-fix: `supports_removal_callbacks()` is called on the unresolved
/// `RefStore`, returns `false`, and ECS sets `vulnerable_mode=true`.
///
/// Post-fix: `inner_store(None)` is called first, which resolves the ref cell;
/// subsequent `supports_removal_callbacks()` returns `true`, ECS sets
/// `vulnerable_mode=false` and registers the callback.
///
/// Assertions:
///   (a) `is_vulnerable_mode()` returns `false` (eager invalidation active)
///   (b) the callback actually reached the backend (verified via a downstream
///       eviction that removes the ECS cache entry)
///
/// Mutation test (step 5 of TDD): commenting out the `inner_store(None)` call
/// in `new_with_time` causes this test to red-fail with:
///   "ECS queried capability on unresolved RefStore — eager invalidation lost (#9)"
#[nativelink_test]
async fn ecs_over_resolved_ref_is_not_vulnerable() -> Result<(), Error> {
    // Build StoreManager with a real MemoryStore registered under a name.
    let store_manager = Arc::new(StoreManager::new());
    let backend = Store::new(MemoryStore::new(&MemorySpec::default()));
    store_manager.add_store("ac_backend_cached", backend.clone());

    // Wrap in a RefStore (cell is None at this point — unresolved).
    let ref_store = Store::new(RefStore::new(
        &RefSpec {
            name: "ac_backend_cached".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    // Wrap in ExistenceCacheStore. This is the call site being fixed.
    let ec = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        ref_store,
    );

    // (a) ECS must NOT be in vulnerable_mode — eager invalidation must be on.
    assert!(
        !ec.is_vulnerable_mode(),
        "ECS queried capability on unresolved RefStore — eager invalidation lost (#9). \
         vulnerable_mode=true means register_item_callback was not called. The fix \
         requires calling inner_store(None) to resolve the RefStore BEFORE \
         supports_removal_callbacks() in ECS::new_with_time.",
    );

    // (b) The callback chain is live: write a blob, verify the ECS caches it,
    //     evict it from the MemoryStore backend (which fires the callback via
    //     MemoryStore's evicting_map → ItemCallbackHolder chain), and confirm
    //     the ECS existence entry was cleared.
    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    ec.update_oneshot(digest, "abc".into())
        .await
        .expect("update_oneshot must succeed");

    // Cache should hold the positive after a successful write.
    assert!(
        ec.exists_in_cache(&digest).await,
        "existence cache must hold positive after update_oneshot",
    );

    // Evict from the backend MemoryStore. The registered callback fires and
    // removes the ECS entry. We use the MemoryStore directly to trigger the
    // eviction path that calls registered ItemCallbacks.
    let memory = backend
        .downcast_ref::<MemoryStore>(None)
        .expect("backend is MemoryStore");
    let removed = memory.remove_entry(digest.into()).await;
    assert!(removed, "MemoryStore must have the entry to remove");

    // Give the async eviction callback a moment to propagate — it's delivered
    // via moka's background async task, not inline. We poll with a deadline
    // instead of a flat sleep (CLAUDE.md: no sleep-as-sync).
    tokio::time::timeout(core::time::Duration::from_secs(3), async {
        while ec.exists_in_cache(&digest).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "callback must clear the ECS entry within 3s after MemoryStore eviction; \
         stale positive still present — callback was not registered (#9)",
    );

    Ok(())
}

/// Test 2 (negative contract — cd980946 semantics preserved):
///
/// When the RefStore's named target is NOT registered in the `StoreManager`,
/// `inner_store(None)` silently returns `self` (the RefStore logs an error but
/// does NOT panic). The subsequent `supports_removal_callbacks()` call still
/// returns `false` (the cell is still empty), so ECS constructs in
/// `vulnerable_mode=true`.
///
/// This test guards the "no panic on unresolvable ref" invariant from #367 /
/// cd980946: the prior `.expect("Register item callback should work")` at the
/// ECS construction site converted any misconfiguration into a systemd restart
/// loop. The current design degrades gracefully.
#[nativelink_test]
async fn ecs_over_unresolvable_ref_constructs_in_vulnerable_mode() -> Result<(), Error> {
    // StoreManager with NO "missing_target" key registered.
    let store_manager = Arc::new(StoreManager::new());

    let ref_store = Store::new(RefStore::new(
        &RefSpec {
            name: "missing_target".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    // Construction MUST NOT panic even though the ref target is unresolvable.
    let ec = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        ref_store,
    );

    // Must be in vulnerable_mode — cd980946's contract: unresolvable ref ⇒
    // false for capability query ⇒ vulnerable_mode. Operator sees the loud
    // error! at construction time.
    assert!(
        ec.is_vulnerable_mode(),
        "ECS over an unresolvable RefStore must enter vulnerable_mode=true \
         WITHOUT panicking (cd980946 / #367 contract). The prior .expect() \
         at construction converted misconfiguration into a restart loop; \
         graceful degradation is required.",
    );

    Ok(())
}
