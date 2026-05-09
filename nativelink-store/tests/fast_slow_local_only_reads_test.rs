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

//! Tests for the worker public CAS server's `local_only_reads` mode of
//! `FastSlowStore`. When set, reads must NEVER fall through to the slow
//! tier on local miss — the worker would otherwise loop the request back
//! to the asking server (via `GrpcStore` → server) which then bounces it
//! straight back to the same worker via the locality map, producing 8+
//! minute mutual stream-blocking wedges in production.
//!
//! Coverage:
//!   1. local-miss returns `NotFound` (slow tier untouched) on `get_part`
//!   2. fast tier hit serves data
//!   3. mirror-blob hit serves data
//!   4. `has_with_results` reports `None` for blobs only on slow tier
//!   5. `batch_get_part_unchunked` routes per-key (fast/mirror/missing)

use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use pretty_assertions::assert_eq;

/// Build a `FastSlowStore` with separate in-memory fast/slow tiers, then
/// flip on `local_only_reads` so the read path skips slow-tier fallthrough.
/// Returns the store along with handles to the inner fast and slow tiers
/// so tests can pre-populate either side independently.
fn make_local_only_fss() -> (Arc<FastSlowStore>, Store, Store) {
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
        fast.clone(),
        slow.clone(),
    )
    .with_local_only_reads();
    (fss, fast, slow)
}

fn d(seed: u8, size: usize) -> DigestInfo {
    let mut h = [0u8; 32];
    h[0] = seed;
    DigestInfo::new(h, size as u64)
}

/// Test 1: slow tier holds the blob, fast tier is empty → must return
/// `NotFound` (the slow tier MUST NOT be consulted under
/// `local_only_reads`).
#[nativelink_test]
async fn local_only_reads_returns_not_found_on_local_miss() -> Result<(), Error> {
    let (fss, _fast, slow) = make_local_only_fss();
    let digest = d(1, 5);
    // Pre-populate ONLY the slow tier with the blob.
    slow.update_oneshot(digest, Bytes::from_static(b"hello"))
        .await?;

    let store: Store = Store::new(fss);
    let result = store.get_part_unchunked(digest, 0, None).await;
    let err = result.expect_err("expected NotFound — slow tier must not be consulted");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got {err:?}");
    Ok(())
}

/// Test 2: fast tier holds the blob → returns data normally.
#[nativelink_test]
async fn local_only_reads_serves_from_fast_store() -> Result<(), Error> {
    let (fss, fast, _slow) = make_local_only_fss();
    let digest = d(2, 5);
    fast.update_oneshot(digest, Bytes::from_static(b"world"))
        .await?;

    let store: Store = Store::new(fss);
    let data = store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(&data[..], b"world");
    Ok(())
}

/// Test 3: blob lives in `mirror_blobs` (server-pushed mirror), fast/slow
/// both empty → returns from the in-memory mirror map.
#[nativelink_test]
async fn local_only_reads_serves_from_mirror_blobs() -> Result<(), Error> {
    let (fss, _fast, _slow) = make_local_only_fss();
    let digest = d(3, 6);
    // Drive a mirror write via the public IS_MIRROR_REQUEST path so the
    // blob lands in `mirror_blobs` (not the fast or slow tier).
    {
        let store: Store = Store::new(fss.clone());
        nativelink_util::store_trait::IS_MIRROR_REQUEST
            .scope(true, async move {
                store
                    .update_oneshot(digest, Bytes::from_static(b"mirror"))
                    .await
                    .expect("mirror write");
            })
            .await;
    }
    assert_eq!(fss.mirror_blob_count(), 1, "mirror blob inserted");

    let store: Store = Store::new(fss);
    let data = store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(&data[..], b"mirror");
    Ok(())
}

/// Test 4: `has_with_results` for a blob present only on the slow tier
/// must report `None` (the slow tier MUST NOT be consulted). Fast and
/// mirror are empty.
#[nativelink_test]
async fn local_only_reads_has_with_results_does_not_consult_slow() -> Result<(), Error> {
    let (fss, _fast, slow) = make_local_only_fss();
    let digest = d(4, 7);
    slow.update_oneshot(digest, Bytes::from_static(b"slowonly"))
        .await?;

    let keys: Vec<StoreKey<'_>> = vec![StoreKey::from(digest)];
    let mut results: Vec<Option<u64>> = vec![None];
    let store: Store = Store::new(fss);
    store.has_with_results(&keys, &mut results).await?;
    assert_eq!(
        results,
        vec![None],
        "has_with_results must NOT report a hit for slow-only blobs in local_only_reads mode"
    );
    Ok(())
}

/// Test 5: a multi-key batch covering one fast hit, one mirror hit, and
/// one blob present only on the slow tier — the slow blob must come back
/// as `NotFound`, the others must return correct data.
#[nativelink_test]
async fn local_only_reads_batch_get_part_routes_per_key() -> Result<(), Error> {
    let (fss, fast, slow) = make_local_only_fss();
    let d_fast = d(5, 4);
    let d_mirror = d(6, 6);
    let d_missing = d(7, 8);

    fast.update_oneshot(d_fast, Bytes::from_static(b"FAST"))
        .await?;
    {
        let store: Store = Store::new(fss.clone());
        nativelink_util::store_trait::IS_MIRROR_REQUEST
            .scope(true, async move {
                store
                    .update_oneshot(d_mirror, Bytes::from_static(b"MIRROR"))
                    .await
                    .expect("mirror write");
            })
            .await;
    }
    // Slow has the third blob, but local_only_reads MUST NOT consult it.
    slow.update_oneshot(d_missing, Bytes::from_static(b"SLOWBLOB"))
        .await?;

    let keys: Vec<StoreKey<'_>> = vec![
        StoreKey::from(d_fast),
        StoreKey::from(d_mirror),
        StoreKey::from(d_missing),
    ];
    // Use the inner driver so we can invoke batch_get_part_unchunked directly
    // and observe per-slot results.
    let driver = fss.clone();
    let results = std::pin::Pin::new(driver.as_ref())
        .batch_get_part_unchunked(keys, None)
        .await;
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].as_ref().expect("fast slot succeeded"),
        &Bytes::from_static(b"FAST"),
    );
    assert_eq!(
        results[1].as_ref().expect("mirror slot succeeded"),
        &Bytes::from_static(b"MIRROR"),
    );
    let err = results[2].as_ref().expect_err("missing slot must NotFound");
    assert_eq!(
        err.code,
        Code::NotFound,
        "missing slot must be NotFound, got {err:?}"
    );
    Ok(())
}
