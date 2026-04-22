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

//! Tests for the directory_cache populate hot loop's pin-after-populate
//! behavior. The loop in `nativelink-worker/src/directory_cache.rs` calls
//! `populate_fast_store_unchecked` and immediately `pin_digests`. Without
//! the pin, sibling populates in the same batch can LRU-evict already-
//! landed blobs (the cache is sized like 19/20 GB and many parallel
//! populates exceed the free headroom).
//!
//! These tests reproduce the populate+pin sequence directly against a
//! `FastSlowStore { fast: FilesystemStore, slow: MemoryStore }` so the
//! pinning behavior on the production-shaped EvictingMap is exercised
//! without requiring a full action sandbox.

use std::env;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use rand::Rng;

fn temp_dir(suffix: &str) -> String {
    format!(
        "{}/{}/{suffix}",
        env::var("TEST_TMPDIR")
            .unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
    )
}

fn digest_for(seed: u64, size: u64) -> DigestInfo {
    // Deterministic sha256 of (seed, size) tuple.
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    hasher.update(size.to_le_bytes());
    let bytes = hasher.finalize();
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    DigestInfo::try_new(&hex, size).unwrap()
}

async fn make_fss(
    fast_max_bytes: usize,
) -> Result<(Arc<FastSlowStore>, Store, Store), Error> {
    let content_path = temp_dir("populate_pin_content");
    let temp_path = temp_dir("populate_pin_temp");
    tokio::fs::create_dir_all(&content_path).await.unwrap();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let fs_arc = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: Some(EvictionPolicy {
            max_bytes: fast_max_bytes,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await?;
    let fast_store = Store::new(fs_arc);
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    Ok((fss, fast_store, slow_store))
}

// -------------------------------------------------------------------------
// (d) self-cannibalization without pinning would fail; with pin, the
//     tracked blobs survive sibling eviction pressure. Scaled from
//     production (300 * 10 MiB on 200 MiB, pin cap 50 MiB) to a
//     unit-test footprint: 8 tracked blobs * 16 KiB (128 KiB total
//     working set, fits in 256 KiB pin cap) plus 32 sibling blobs of
//     32 KiB each (1 MiB of pressure) written into the same fast store
//     to drive LRU eviction. Fast store 1 MiB, pin cap 256 KiB (25%).
// -------------------------------------------------------------------------
#[nativelink_test]
async fn populate_pin_prevents_self_cannibalization() -> Result<(), Error> {
    const N: usize = 8;
    const BLOB_SIZE: usize = 16 * 1024;
    const FAST_BYTES: usize = 1024 * 1024;
    const PRESSURE_BLOBS: usize = 32;
    const PRESSURE_SIZE: usize = 32 * 1024;

    let (fss, fast_store, slow_store) = make_fss(FAST_BYTES).await?;

    // Seed N "tracked" blobs and PRESSURE_BLOBS sibling blobs in slow.
    let mut digests = Vec::with_capacity(N);
    for i in 0..N {
        let d = digest_for(i as u64, BLOB_SIZE as u64);
        digests.push(d);
        slow_store
            .update_oneshot(d, Bytes::from(vec![(i % 256) as u8; BLOB_SIZE]))
            .await?;
    }
    let mut pressure_digests = Vec::with_capacity(PRESSURE_BLOBS);
    for i in 0..PRESSURE_BLOBS {
        let d = digest_for(0xC0DE + i as u64, PRESSURE_SIZE as u64);
        pressure_digests.push(d);
        slow_store
            .update_oneshot(d, Bytes::from(vec![((i + 64) % 256) as u8; PRESSURE_SIZE]))
            .await?;
    }

    // Populate + pin each tracked blob, then push pressure blobs
    // (unpinned) through the same store. The pressure writes drive the
    // fast tier over capacity; only pin_digests protects tracked blobs.
    for (i, d) in digests.iter().enumerate() {
        fss.populate_fast_store_unchecked((*d).into())
            .await
            .err_tip(|| format!("populate tracked {d:?}"))?;
        fss.fast_store().pin_digests(&[*d]);
        for j in 0..(PRESSURE_BLOBS / N) {
            let pd = pressure_digests[i * (PRESSURE_BLOBS / N) + j];
            fss.populate_fast_store_unchecked(pd.into())
                .await
                .err_tip(|| format!("populate pressure {pd:?}"))?;
        }
    }

    // Let moka's async eviction thread drain — cache accounting lags.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut missing = Vec::new();
    for d in &digests {
        if fast_store.has(*d).await?.is_none() {
            missing.push(*d);
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {N} pinned blobs missing from fast store under sibling eviction pressure",
        missing.len(),
    );
    Ok(())
}

// -------------------------------------------------------------------------
// (d-control) Demonstrate that WITHOUT the pin, tracked blobs ARE
//     evicted under the same sibling pressure. Guards the pin contract.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn populate_without_pin_loses_blobs_to_eviction() -> Result<(), Error> {
    const N: usize = 8;
    const BLOB_SIZE: usize = 16 * 1024;
    const FAST_BYTES: usize = 1024 * 1024;
    const PRESSURE_BLOBS: usize = 32;
    const PRESSURE_SIZE: usize = 32 * 1024;

    let (fss, fast_store, slow_store) = make_fss(FAST_BYTES).await?;

    let mut digests = Vec::with_capacity(N);
    for i in 0..N {
        let d = digest_for(i as u64, BLOB_SIZE as u64);
        digests.push(d);
        slow_store
            .update_oneshot(d, Bytes::from(vec![(i % 256) as u8; BLOB_SIZE]))
            .await?;
    }
    let mut pressure_digests = Vec::with_capacity(PRESSURE_BLOBS);
    for i in 0..PRESSURE_BLOBS {
        let d = digest_for(0xC0DE + i as u64, PRESSURE_SIZE as u64);
        pressure_digests.push(d);
        slow_store
            .update_oneshot(d, Bytes::from(vec![((i + 64) % 256) as u8; PRESSURE_SIZE]))
            .await?;
    }

    for (i, d) in digests.iter().enumerate() {
        fss.populate_fast_store_unchecked((*d).into())
            .await
            .err_tip(|| format!("populate tracked {d:?}"))?;
        // NOTE: no pin_digests — negative control.
        for j in 0..(PRESSURE_BLOBS / N) {
            let pd = pressure_digests[i * (PRESSURE_BLOBS / N) + j];
            fss.populate_fast_store_unchecked(pd.into())
                .await
                .err_tip(|| format!("populate pressure {pd:?}"))?;
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut present = 0;
    for d in &digests {
        if fast_store.has(*d).await?.is_some() {
            present += 1;
        }
    }
    // Without pin, sibling pressure should evict at least one tracked
    // blob. In practice ~all are evicted because FAST_BYTES=1 MiB and
    // total working set = 1.125 MiB.
    assert!(
        present < N,
        "expected at least some eviction without pinning, but all {N} survived",
    );
    Ok(())
}

// -------------------------------------------------------------------------
// (e) pin-cap exhaustion: pinning beyond the 25% cap warns and the
//     verify-and-retry safety net catches the unpinned tail.
//
//     pin_cap = max_bytes * 0.25 = 32 KiB on a 128 KiB cache. With 8
//     blobs of 8 KiB each, total = 64 KiB > 32 KiB pin_cap, so the cap
//     trips partway through.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn populate_pin_cap_exhaustion_warns() -> Result<(), Error> {
    const N: usize = 8;
    const BLOB_SIZE: usize = 8 * 1024;
    const FAST_BYTES: usize = 128 * 1024;

    let (fss, fast_store, slow_store) = make_fss(FAST_BYTES).await?;

    let mut digests = Vec::with_capacity(N);
    for i in 0..N {
        let d = digest_for(0xCAFE + i as u64, BLOB_SIZE as u64);
        digests.push(d);
        slow_store
            .update_oneshot(d, Bytes::from(vec![(i % 256) as u8; BLOB_SIZE]))
            .await?;
    }

    // Run the populate+pin loop. Some populates may surface Aborted from
    // the verify-and-retry safety net once the pin cap is exhausted and
    // sibling populates evict the unpinned tail. We accept either Ok or
    // Aborted on each blob — what we are asserting is that the WARN
    // message about pin cap exhaustion was emitted.
    let mut populated = 0;
    let mut aborted = 0;
    for d in &digests {
        let res = fss.populate_fast_store_unchecked((*d).into()).await;
        match res {
            Ok(()) => populated += 1,
            Err(e) if e.code == nativelink_error::Code::Aborted => aborted += 1,
            Err(e) => return Err(e).err_tip(|| format!("unexpected error for {d:?}")),
        }
        fss.fast_store().pin_digests(&[*d]);
    }

    assert!(
        populated + aborted == N,
        "every populate must end Ok or Aborted; got populated={populated} aborted={aborted}"
    );
    // Sanity check: at least the first few should have populated cleanly.
    assert!(populated >= 1, "at least one populate must succeed");

    // The pin cap warn must have fired at least once when the cap was hit.
    assert!(
        logs_contain("pin cap exceeded"),
        "expected pin_keys to warn about pin cap exhaustion"
    );

    // And the still-present count should be bounded by what fits in the
    // fast store (cap + free headroom). We just check the store is not
    // empty.
    let mut present = 0;
    for d in &digests {
        if fast_store.has(*d).await?.is_some() {
            present += 1;
        }
    }
    assert!(present > 0, "fast store should still have some pinned blobs");
    Ok(())
}
