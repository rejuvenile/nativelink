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

//! Regression test for the `find_fast_slow_for_pin` startup walker
//! (`nativelink-store::small_blob_dispatcher::find_fast_slow_for_pin`).
//!
//! Background: at server startup the SmallBlobDispatcher wire-up
//! registers a per-store `EphemeralServerSidePin` for every CAS-backing
//! chain that bottoms out at a `FastSlowStore`. The walker drills
//! through wrapper layers (`ExistenceCacheStore`, `VerifyStore`) by
//! recognising each wrapper type and following its concrete inner
//! accessor. If the walker fails to recognise a wrapper that returns
//! `self` from `inner_store(None)`, registration silently no-ops and
//! the dispatcher is inert in production with no alarm.
//!
//! AC stores are intentionally NOT walked — see `8e6d1323` (#276)
//! for the architectural rationale (workers never read `ac_store`).
//!
//! Coverage: short-circuit on a non-FSS chain returning `None`. The
//! bespoke failure message distinguishes a real wrong-result failure
//! from a deadlock-detector elapsed.

use core::time::Duration;

use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::find_fast_slow_for_pin;
use nativelink_util::store_trait::StoreDriver;

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
    let driver: &dyn StoreDriver = mem.as_ref();

    let walker_fut = async move { find_fast_slow_for_pin(driver).is_some() };

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
