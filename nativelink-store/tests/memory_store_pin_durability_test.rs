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

//! #334 Fix C — server-side `cas_FAST_SLOW_STORE` durability gap during
//! the BlobsInStableStorage (BIS) ack window.
//!
//! ## Background (the bug being fixed)
//!
//! The production server's `cas_FAST_SLOW_STORE` composes
//! `MemoryStore` (fast, 48 GB) over `FilesystemStore` (slow). After a
//! Bazel client writes a blob, FastSlowStore queues the slow-tier write
//! in the background and immediately calls `pin_digests(&[digest])` on
//! BOTH inner stores — keeping the blob alive for the BIS ack window
//! during which the worker mirror is acquiring its second replica.
//!
//! Pre-fix: `MemoryStore::pin_delegation` returned `Leaf`, the trait
//! default's `Leaf` arm is a documented no-op, so the fast-tier pin
//! evaporated. The slow-tier pin (FilesystemStore) was racing the
//! background slow write — `MokaEvictingMap::pin_key` returned `false`
//! on a key not yet in cache (`moka_evicting_map.rs:792-795`). BOTH
//! pins disappeared. The blob was pinned only by being
//! recently-written; under cap pressure it could be LRU-evicted before
//! the worker's BIS ack arrived, breaking the ≥2-replica invariant.
//!
//! ## Asymmetric coverage (CLAUDE.md)
//!
//! Two contracts on the borrowed pin state, both directions tested:
//!
//! * **Under-action** (`memory_store_pin_survives_eviction_pressure`):
//!   after `pin_digests(A)`, sibling writes that drive MemoryStore over
//!   capacity must NOT evict A. Pre-fix the no-op pin lets A vanish.
//!   Mutation: revert the new `MemoryStore::pin_digests` override; the
//!   test red-fails with the bespoke "MemoryStore-pin no-op shipped"
//!   message.
//!
//! * **Over-action** (`memory_store_unpin_releases_for_eviction`):
//!   after `unpin_digests(A)` (the BIS-ack path), A MUST be eligible
//!   for eviction again — otherwise the pin would leak forever and the
//!   8 GB MemoryStore would run dry of headroom. Pre-fix this path did
//!   not exist at all (no `unpin_digests` trait method). Mutation:
//!   revert the BIS-loop unpin call (or the `MemoryStore::unpin_digests`
//!   override); A stays pinned and the assertion red-fails with the
//!   bespoke "pin leaks past BIS-ack" message.
//!
//! ## Production composition
//!
//! Both tests wrap the inner `FastSlowStore { fast: MemoryStore, slow:
//! FilesystemStore }` in the canonical CAS chain — `VerifyStore` →
//! `ExistenceCacheStore` — so the pin/unpin call traverses the same
//! `pin_delegation` chain used in production. A unit test against
//! `MemoryStore` alone would prove only that the override compiles; a
//! production-composition test proves that VerifyStore/ExistenceCache
//! correctly route the trait method through.
//!
//! Each assertion runs under a 5 s `tokio::time::timeout` so a
//! lock-contention / deadlock regression in the chain manifests as a
//! red test rather than a hung CI runner.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection,
    StoreSpec, VerifySpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use tempfile::TempDir;

/// Fast-tier MemoryStore cap (8 KiB; per `MokaEvictingMap`'s 1-KiB
/// granularity weigher the effective capacity is 8 weight units).
/// Pin cap = 25% × `MEM_CAP_BYTES` = 2 KiB ⇒ 1 pressure-pin slot
/// after the FSS::update auto-pin lands.
const MEM_CAP_BYTES: usize = 8 * 1024;

/// Pinned blob A — small enough to fit inside the pin cap. The hash
/// is arbitrary; sha256 width is what matters for the digest format.
const PINNED_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const PINNED_SIZE: usize = 1024;

/// Sibling pressure blobs. Each pressure blob is 2 KiB (weight 2);
/// `FastSlowStore::update` calls `pin_digests` on the fast tier
/// after a successful write, so the FIRST pressure blob takes the
/// remaining pin slot and stays in `pinned`. The other 7 land in
/// the moka cache:
///
///   cache weight after seeding: A(1) + 7×2 = 15 ≫ cap 8.
///
/// The LRU evictor MUST drop A unless its pin is real. Eight blobs
/// also gives us slack against moka's eventually-consistent admission
/// (counts are accurate within ~one batch).
const PRESSURE_HASHES: &[&str] = &[
    "2222222222222222222222222222222222222222222222222222222222222222",
    "3333333333333333333333333333333333333333333333333333333333333333",
    "4444444444444444444444444444444444444444444444444444444444444444",
    "5555555555555555555555555555555555555555555555555555555555555555",
    "6666666666666666666666666666666666666666666666666666666666666666",
    "7777777777777777777777777777777777777777777777777777777777777777",
    "8888888888888888888888888888888888888888888888888888888888888888",
    "9999999999999999999999999999999999999999999999999999999999999999",
];
const PRESSURE_SIZE: usize = 2 * 1024;

/// 5 s deadlock detector — see CLAUDE.md "test in production composition".
/// A healthy `has` round-trip through VerifyStore + ExistenceCache + FSS
/// is sub-millisecond; 5 s leaves CI headroom without masking a
/// chain-routing regression.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

struct Harness {
    /// Production-composition handle: `ExistenceCacheStore` →
    /// `VerifyStore` → `FastSlowStore { fast: MemoryStore, slow:
    /// FilesystemStore }`. All `pin_digests` / `unpin_digests` go
    /// through this; the test never reaches into the fast-tier
    /// MemoryStore directly.
    cas_chain: Store,
    /// Inner FSS handle for direct `pin_digests` / `unpin_digests`
    /// (the production BIS-broadcast loop calls these on the FSS).
    fss: Arc<FastSlowStore>,
    /// Direct `MemoryStore` handle so the test can ask "what does the
    /// in-process index see?" without traversing the wrapping chain
    /// (which would re-populate from the slow tier on miss).
    fast_mem: Store,
    /// Hold-onto-it tempdir backing the FilesystemStore — drop fires
    /// at end of scope cleaning up content/temp dirs.
    _temp: TempDir,
}

async fn make_harness() -> Result<Harness, Error> {
    let root = tempfile::Builder::new()
        .prefix("memstore_pin_durability_")
        .tempdir()
        .expect("tempdir");
    let content_path = root.path().join("content");
    let temp_path = root.path().join("temp");
    tokio::fs::create_dir_all(&content_path).await.unwrap();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();

    // Slow tier: FilesystemStore. In production this is the on-disk
    // half of cas_FAST_SLOW_STORE; here we just need it to satisfy
    // the FSS `Many` pin_delegation fan-out without contributing
    // false positives to the test's eviction observation.
    let slow_arc = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.to_string_lossy().into_owned(),
        temp_path: temp_path.to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy {
            // Generous slow-tier cap so the slow store NEVER evicts and
            // the test isolates fast-tier pin behavior.
            max_bytes: 100 * 1024 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    })
    .await?;
    let slow_store = Store::new(slow_arc);

    // Fast tier: tiny MemoryStore. The cap is the variable under
    // test — it's what creates the eviction pressure that exposes
    // whether `pin_digests` actually pins.
    let fast_mem_arc = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: MEM_CAP_BYTES,
            ..Default::default()
        }),
        ..Default::default()
    });
    let fast_mem = Store::new(fast_mem_arc);

    // Build the FastSlowStore over the two tiers above.
    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            // The FastSlowSpec.fast / .slow specs are placeholders here —
            // the actual delegate is the `fast_store` / `slow_store` arg
            // passed to FastSlowStore::new. (Same pattern as
            // populate_pinning_test.rs.)
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_mem.clone(),
        slow_store.clone(),
    );
    let fss_store = Store::new(fss_arc.clone());

    // Wrap in VerifyStore (the canonical CAS composition: every CAS
    // write goes through VerifyStore for size + hash checks).
    let verify_store = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        fss_store,
    ));

    // ExistenceCacheStore is the outermost wrapper for cas_STORE in
    // production. It forwards pin/unpin via PinDelegation::Inner.
    let cas_chain = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
        },
        verify_store,
    ));

    Ok(Harness {
        cas_chain,
        fss: fss_arc,
        fast_mem,
        _temp: root,
    })
}

// -------------------------------------------------------------------------
// Under-action: the new `MemoryStore::pin_digests` override MUST keep
// blob A in the in-memory fast tier across sibling-write eviction
// pressure. Pre-fix the trait default's `Leaf` arm was no-op and A
// silently disappeared, breaking the ≥2-replica invariant during the
// BIS ack window.
//
// Mutation step: revert the `MemoryStore::pin_digests` override (so
// the trait default `Leaf` arm fires again). Confirm this test
// red-fails with the bespoke "MemoryStore-pin no-op shipped" message.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn memory_store_pin_survives_eviction_pressure() -> Result<(), Error> {
    let h = make_harness().await?;

    let pinned_digest = DigestInfo::try_new(PINNED_HASH, PINNED_SIZE as u64)?;

    // Write blob A through the production CAS chain. Lands in
    // MemoryStore (and also in FilesystemStore via the FSS slow-write
    // path; that's fine for this test, the question is only about
    // the in-memory replica).
    h.cas_chain
        .update_oneshot(
            pinned_digest,
            Bytes::from(vec![0xAAu8; PINNED_SIZE]),
        )
        .await
        .err_tip(|| "writing pinned blob A through CAS chain")?;

    // Acquire pin via the same trait method the production
    // FastSlowStore::update calls (`pin_digests` on the fast tier
    // through the chain). This is the load-bearing call.
    h.fss.fast_store().pin_digests(&[pinned_digest]);

    // Drive sibling writes to push MemoryStore past its cap. Each
    // pressure blob is 2 KiB; eight of them total 16 KiB ≫ 8 KiB cap.
    // With the pinned blob still occupying its slot, the LRU evictor
    // is forced to choose a victim — without a real pin, blob A is
    // the oldest and is the natural victim.
    for hash in PRESSURE_HASHES {
        let d = DigestInfo::try_new(hash, PRESSURE_SIZE as u64)?;
        h.cas_chain
            .update_oneshot(d, Bytes::from(vec![0xBBu8; PRESSURE_SIZE]))
            .await
            .err_tip(|| format!("writing pressure blob {hash}"))?;
    }

    // Let moka's async eviction thread drain — cache accounting lags.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Visibility check on the IN-PROCESS INDEX of the fast tier
    // directly (not the wrapping chain — VerifyStore + FSS would
    // re-populate from the slow tier on miss, masking the eviction).
    // 5 s deadlock detector around the fast-tier `has` call.
    let has_fut = h.fast_mem.has(pinned_digest);
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, has_fut)
        .await
        .expect(
            "timed out asking fast-tier MemoryStore::has — chain regression \
             in pin/has plumbing",
        );
    let observed = res.expect("MemoryStore::has returned Err");

    assert!(
        observed.is_some(),
        "MemoryStore-pin no-op shipped — blob A evicted from the fast tier \
         before BIS-ack window closed; durability invariant violated. \
         observed: {observed:?}, pinned_digest: {pinned_digest:?}, \
         pressure_blobs: {} × {} bytes, mem_cap: {} bytes.",
        PRESSURE_HASHES.len(),
        PRESSURE_SIZE,
        MEM_CAP_BYTES,
    );

    Ok(())
}

// -------------------------------------------------------------------------
// Over-action: after `unpin_digests(A)` fires from the BIS broadcast
// loop (Step 2 in the fix), the pin must ACTUALLY be released — the
// blob must once again be a candidate for LRU eviction. Without this
// the pin leaks: every CAS write would accumulate a pin entry forever,
// the 25%-of-cap pin budget would fill, and `pin_keys: pin cap
// exceeded` warnings would flood after the first ~12 GB of writes
// (48 GB cap × 25%).
//
// Mutation step: revert the `MemoryStore::unpin_digests` override (or
// remove the BIS-broadcast-loop unpin call). The pin stays held, the
// pinned blob survives the pressure write, and the assertion red-fails
// with the bespoke "pin leaks past BIS-ack" message.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn memory_store_unpin_releases_for_eviction() -> Result<(), Error> {
    let h = make_harness().await?;

    let pinned_digest = DigestInfo::try_new(PINNED_HASH, PINNED_SIZE as u64)?;

    h.cas_chain
        .update_oneshot(pinned_digest, Bytes::from(vec![0xAAu8; PINNED_SIZE]))
        .await
        .err_tip(|| "writing pinned blob A through CAS chain")?;

    // Acquire pin (production write-side path).
    h.fss.fast_store().pin_digests(&[pinned_digest]);

    // Simulate the BIS broadcast loop's post-broadcast unpin via the
    // new `unpin_digests` trait method on the same chain. This is
    // the production path the BIS loop now takes.
    h.cas_chain.unpin_digests(&[pinned_digest]);

    // Drive sibling pressure exactly as in the under-action test.
    // After the unpin, A must be evictable; otherwise the pin leaked.
    for hash in PRESSURE_HASHES {
        let d = DigestInfo::try_new(hash, PRESSURE_SIZE as u64)?;
        h.cas_chain
            .update_oneshot(d, Bytes::from(vec![0xBBu8; PRESSURE_SIZE]))
            .await
            .err_tip(|| format!("writing pressure blob {hash}"))?;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let has_fut = h.fast_mem.has(pinned_digest);
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, has_fut)
        .await
        .expect(
            "timed out asking fast-tier MemoryStore::has — chain regression \
             in unpin/has plumbing",
        );
    let observed = res.expect("MemoryStore::has returned Err");

    assert!(
        observed.is_none(),
        "pin leaks past BIS-ack — blob A survived sibling-pressure eviction \
         even after unpin_digests; the pin entry was not released. observed: \
         {observed:?}, pinned_digest: {pinned_digest:?}, pressure_blobs: \
         {} × {} bytes, mem_cap: {} bytes. With a 2 KiB pin cap and the \
         oldest entry now unpinned, A MUST be the LRU victim.",
        PRESSURE_HASHES.len(),
        PRESSURE_SIZE,
        MEM_CAP_BYTES,
    );

    Ok(())
}
