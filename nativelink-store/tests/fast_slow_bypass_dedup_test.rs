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

//! Focused regression coverage for #2415 (`bypass_dedup_threshold_bytes`).
//!
//! Standalone file because the merge-v1.6.1 `fast_slow_store_test.rs` still
//! carries conflict markers (UU) and would not compile; this file compiles
//! and runs independently via
//! `cargo test -p nativelink-store --test fast_slow_bypass_dedup_test`.

use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Builds a `FastSlowStore` over two `MemoryStore`s with the given
/// `bypass_dedup_threshold_bytes`. Returns `(fast_slow, fast, slow)`.
fn make_stores(bypass_dedup_threshold_bytes: u64) -> (Store, Store, Store) {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes,
        },
        fast_store.clone(),
        slow_store.clone(),
    ));
    (fast_slow_store, fast_store, slow_store)
}

/// #2415: a read of a blob at/above the threshold streams straight from the
/// slow store and NEVER populates the fast tier. This is fully deterministic
/// because the bypass path spawns no producer and tees nothing — so a
/// `fast.has()` immediately after the read is guaranteed `None`.
#[nativelink_test]
async fn bypass_reads_slow_and_does_not_populate_fast() -> Result<(), Error> {
    // Threshold 100; digest size 200 (>= threshold) => bypass engaged.
    let (fast_slow_store, fast_store, slow_store) = make_stores(100);

    let data = vec![7u8; 200];
    let digest = DigestInfo::try_new(VALID_HASH, 200).unwrap();

    // Blob lives ONLY in the slow store.
    slow_store
        .update_oneshot(digest, data.clone().into())
        .await?;

    // Read through the fast-slow store: the huge-blob bypass serves it from
    // the slow tier.
    let got = fast_slow_store
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(got, data, "bypass read must return the slow-tier bytes");

    // The bypass must NOT have populated the fast tier. Deterministic: the
    // bypass path performs a direct `slow_store.get_part` with no
    // producer-spawn / cache-tee, so nothing ever writes to `fast`.
    assert!(
        fast_store.has(digest).await?.is_none(),
        "huge-blob bypass must not populate the fast tier"
    );

    Ok(())
}

/// #2415 gating: with the SAME threshold, a blob BELOW it is NOT bypassed —
/// it takes the normal streaming-populate path, which DOES tee into the fast
/// tier. Proves `bypass_dedup_threshold_bytes` gates the behavior on size.
#[nativelink_test]
async fn below_threshold_still_populates_fast_tier() -> Result<(), Error> {
    // Threshold 100; digest size 50 (< threshold) => bypass NOT engaged.
    let (fast_slow_store, fast_store, slow_store) = make_stores(100);

    let data = vec![9u8; 50];
    let digest = DigestInfo::try_new(VALID_HASH, 50).unwrap();

    slow_store
        .update_oneshot(digest, data.clone().into())
        .await?;

    let got = fast_slow_store
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(got, data, "non-bypassed read must return the correct bytes");

    // The normal populate path tees into the fast tier on a spawned producer;
    // observe that async side-effect via a bounded cooperative-yield poll
    // (no fixed sleep — yields let the spawned producer make progress).
    let mut populated = false;
    for _ in 0..10_000 {
        if fast_store.has(digest).await?.is_some() {
            populated = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        populated,
        "a blob below the bypass threshold must populate the fast tier \
         (normal streaming-populate path)"
    );

    Ok(())
}

/// #2415 default: threshold `0` disables the bypass entirely, so even a large
/// blob takes the normal populate path (behavior unchanged from pre-#2415).
#[nativelink_test]
async fn threshold_zero_disables_bypass() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores(0);

    let data = vec![3u8; 200];
    let digest = DigestInfo::try_new(VALID_HASH, 200).unwrap();

    slow_store
        .update_oneshot(digest, data.clone().into())
        .await?;

    let got = fast_slow_store
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(got, data);

    // With the bypass disabled, the large blob is populated into the fast
    // tier just like any other read.
    let mut populated = false;
    for _ in 0..10_000 {
        if fast_store.has(digest).await?.is_some() {
            populated = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        populated,
        "threshold 0 must leave the populate path intact (bypass disabled)"
    );

    Ok(())
}
