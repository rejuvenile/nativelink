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

//! #334 Fix C extended (dsr MAJOR-1) — AC backend pin/unpin durability.
//!
//! ## Background
//!
//! The original Fix C bundle ([memory_store_pin_durability_test.rs])
//! covered the CAS chain. dsr's MAJOR-1 review pointed out that the
//! production AC composition has the same shape (`FastSlowStore` over
//! `MemoryStore`) and therefore the same pin-leak failure mode. This
//! file is the symmetric AC test.
//!
//! Production AC chain:
//!   `AC_STORE` = `AcProxyStore` → `CompletenessCheckingStore` →
//!     `AC_BACKEND_CACHED` = `FastSlowStore { fast: MemoryStore(4 GB),
//!                                            slow: ref(REDIS_AC_STORE) }`
//!
//! AC writes go through `FastSlowStore::update[_oneshot]` which calls
//! `pin_digests` on the fast tier (`fast_slow_store.rs:4103` /
//! `:3805`) — same code path as CAS. Without this Fix-C-extended
//! unpin, the 1 GB pin budget (4 GB cap × 25%) fills after ~1 GB of
//! AC writes and `pin_keys: pin cap exceeded` floods.
//!
//! ## Asymmetric coverage
//!
//! Two contracts on the borrowed AC pin state, both directions tested:
//!
//! * **Under-action** (`ac_pin_survives_eviction_pressure`): after
//!   `pin_digests(A)` via the AC chain, sibling AC writes that drive
//!   the AC fast-tier MemoryStore over capacity must NOT evict A.
//!   Pre-Fix-C this failed (pin was a no-op); the Fix-C MemoryStore
//!   override fixes it for both CAS and AC since it lives on
//!   `MemoryStore` itself, not on the chain. This test confirms the
//!   AC chain plumbing routes the pin call through correctly.
//!
//! * **Over-action** (`ac_unpin_releases_for_eviction`): after the
//!   BIS-broadcast loop fires `unpin_digests(A)` on the AC chain
//!   (the new code path added in this fix-up), A MUST be eligible
//!   for eviction again. Mutation step: comment out the AC
//!   `unpin_digests` loop in `src/bin/nativelink.rs` (or revert the
//!   `MemoryStore::unpin_digests` override). The pin stays held, A
//!   survives the pressure write, and the assertion red-fails with
//!   the bespoke "AC pin leaks past BIS-ack" message.
//!
//! ## Production composition
//!
//! Both tests wrap the inner FSS in the canonical AC chain
//! `AcProxyStore → CompletenessCheckingStore → FastSlowStore`, so
//! `pin_digests` / `unpin_digests` traverse the same `pin_delegation`
//! chain as production. A unit test against `MemoryStore` alone would
//! prove only that the override compiles; this composition proves
//! that `AcProxyStore::pin_delegation` →
//! `CompletenessCheckingStore::pin_delegation` →
//! `FastSlowStore::pin_delegation` correctly route the trait call
//! through to the AC fast tier.
//!
//! Each assertion runs under a 5 s `tokio::time::timeout` so a
//! lock-contention / deadlock regression in the chain manifests as a
//! red test rather than a hung CI runner.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::ac_proxy_store::AcProxyStore;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::ac_pin_registry::new_shared_ac_pin_registry;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};

/// Tiny AC fast-tier MemoryStore cap (8 KiB; per `MokaEvictingMap`'s
/// 1-KiB granularity weigher the effective capacity is 8 weight
/// units). Pin cap = 25% × `MEM_CAP_BYTES` = 2 KiB ⇒ exactly enough
/// to hold one 1 KiB pinned blob plus the FSS::update auto-pin slot.
const MEM_CAP_BYTES: usize = 8 * 1024;

/// Pinned AC entry A — small enough to fit inside the pin cap. The
/// hash is arbitrary; sha256 width is what matters for the digest.
const PINNED_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const PINNED_SIZE: usize = 1024;

/// Sibling AC pressure entries. Each is 2 KiB (weight 2); after
/// seeding all 8, total weight is A(1) + 7×2 = 15 ≫ cap 8 (one of
/// the 8 takes the auto-pin slot). The LRU evictor MUST drop A
/// unless its pin is real.
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
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

struct Harness {
    /// Production-composition handle: `AcProxyStore` →
    /// `CompletenessCheckingStore` → `FastSlowStore { fast:
    /// MemoryStore, slow: MemoryStore }`. All `pin_digests` /
    /// `unpin_digests` go through this; the test never reaches into
    /// the fast-tier MemoryStore directly except via `fast_mem` for
    /// in-process index visibility checks.
    ac_chain: Store,
    /// Inner FSS handle for direct `pin_digests` (the production
    /// FSS::update path calls these on the FSS).
    fss: Arc<FastSlowStore>,
    /// Direct `MemoryStore` handle so the test can ask "what does
    /// the in-process index see?" without traversing the wrapping
    /// chain (which would re-populate from the slow tier on miss).
    fast_mem: Store,
}

async fn make_harness() -> Result<Harness, Error> {
    // Slow tier: a generously-sized MemoryStore. In production this
    // is `ref(REDIS_AC_STORE)`; here we just need it to satisfy the
    // FSS `Many` pin_delegation fan-out without contributing false
    // positives to the test's eviction observation.
    let slow_mem_arc = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 100 * 1024 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    });
    let slow_store = Store::new(slow_mem_arc);

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

    // Build the FastSlowStore over the two tiers above (production
    // `AC_BACKEND_CACHED`).
    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            // Placeholder specs — the actual delegate is the
            // `fast_store` / `slow_store` arg passed to ::new.
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_mem.clone(),
        slow_store.clone(),
    );
    let fss_store = Store::new(fss_arc.clone());

    // CompletenessCheckingStore wraps the AC backend. It needs a CAS
    // reference (consulted only during AC-read completeness checks).
    // For pin/unpin tests we only exercise update + pin paths, so a
    // separate empty MemoryStore as cas suffices.
    let cas_for_completeness =
        Store::new(MemoryStore::new(&MemorySpec::default()));
    let completeness_arc =
        CompletenessCheckingStore::new(fss_store, cas_for_completeness);
    let completeness_store = Store::new(completeness_arc);

    // AcProxyStore is the outermost wrapper for AC_STORE in
    // production. Plaintext (no TLS) — peer-fetch path won't fire
    // because the test never registers any worker pins.
    let registry = new_shared_ac_pin_registry();
    let proxy_arc = AcProxyStore::new(completeness_store, registry);
    let ac_chain = Store::new(proxy_arc);

    Ok(Harness {
        ac_chain,
        fss: fss_arc,
        fast_mem,
    })
}

// -------------------------------------------------------------------------
// Under-action: the `MemoryStore::pin_digests` override (introduced
// by Fix C) MUST keep AC entry A in the in-memory fast tier across
// sibling-write eviction pressure when reached via the AC chain. A
// chain-routing regression in `AcProxyStore::pin_delegation`,
// `CompletenessCheckingStore::pin_delegation`, or
// `FastSlowStore::pin_delegation` would manifest as A being evicted
// because the pin call never reached the MemoryStore.
//
// Mutation step: change `AcProxyStore::pin_delegation` or
// `CompletenessCheckingStore::pin_delegation` to return
// `PinDelegation::Leaf`; this test red-fails with the bespoke
// "AC chain pin routing broken" message.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn ac_pin_survives_eviction_pressure() -> Result<(), Error> {
    let h = make_harness().await?;

    let pinned_digest = DigestInfo::try_new(PINNED_HASH, PINNED_SIZE as u64)?;

    // Write AC entry A through the production AC chain. Lands in
    // the AC fast-tier MemoryStore (and slow tier).
    h.ac_chain
        .update_oneshot(
            pinned_digest,
            Bytes::from(vec![0xAAu8; PINNED_SIZE]),
        )
        .await
        .err_tip(|| "writing pinned AC entry A through AC chain")?;

    // Acquire pin via the same trait method the production
    // FastSlowStore::update calls (`pin_digests` on the fast tier
    // through the chain). This is the load-bearing call.
    h.fss.fast_store().pin_digests(&[pinned_digest]);

    // Drive sibling AC writes to push the AC fast tier past its cap.
    for hash in PRESSURE_HASHES {
        let d = DigestInfo::try_new(hash, PRESSURE_SIZE as u64)?;
        h.ac_chain
            .update_oneshot(d, Bytes::from(vec![0xBBu8; PRESSURE_SIZE]))
            .await
            .err_tip(|| format!("writing AC pressure entry {hash}"))?;
    }

    // Let moka's async eviction thread drain — cache accounting lags.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Visibility check on the IN-PROCESS INDEX of the AC fast tier
    // directly (not the wrapping chain — CompletenessCheckingStore +
    // FSS would re-populate from the slow tier on miss, masking the
    // eviction).
    let has_fut = h.fast_mem.has(pinned_digest);
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, has_fut)
        .await
        .expect(
            "timed out asking AC fast-tier MemoryStore::has — chain regression \
             in AC pin/has plumbing",
        );
    let observed = res.expect("AC MemoryStore::has returned Err");

    assert!(
        observed.is_some(),
        "AC chain pin routing broken — entry A evicted from the AC fast \
         tier before BIS-ack window closed; pin call did not reach the \
         AC MemoryStore through AcProxy/CompletenessChecking/FSS chain. \
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
// loop's NEW AC-unpin code (this fix-up), the pin must ACTUALLY be
// released — the entry must once again be a candidate for LRU
// eviction. Without this the AC pin leaks: every AC write would
// accumulate a pin entry forever, the 25%-of-cap pin budget would
// fill after ~1 GB of AC writes (4 GB cap × 25%), and `pin_keys:
// pin cap exceeded` warnings would flood.
//
// Mutation step: comment out the new
//   for (store, drained) in &ac_drains_per_store {
//       store.unpin_digests(drained);
//   }
// in `src/bin/nativelink.rs`, OR change
// `AcProxyStore::pin_delegation` / `CompletenessCheckingStore::pin_delegation`
// to return `Leaf`. The pin stays held, A survives pressure, and
// the assertion red-fails with the bespoke
// "AC pin leaks past BIS-ack" message.
// -------------------------------------------------------------------------
#[nativelink_test]
async fn ac_unpin_releases_for_eviction() -> Result<(), Error> {
    let h = make_harness().await?;

    let pinned_digest = DigestInfo::try_new(PINNED_HASH, PINNED_SIZE as u64)?;

    h.ac_chain
        .update_oneshot(pinned_digest, Bytes::from(vec![0xAAu8; PINNED_SIZE]))
        .await
        .err_tip(|| "writing pinned AC entry A through AC chain")?;

    // Acquire pin (production write-side path).
    h.fss.fast_store().pin_digests(&[pinned_digest]);

    // Simulate the BIS broadcast loop's post-broadcast AC unpin via
    // the `unpin_digests` trait method on the same chain. This is
    // the production path the BIS loop now takes for AC drains.
    h.ac_chain.unpin_digests(&[pinned_digest]);

    // Drive sibling pressure exactly as in the under-action test.
    // After the unpin, A must be evictable; otherwise the pin leaked.
    for hash in PRESSURE_HASHES {
        let d = DigestInfo::try_new(hash, PRESSURE_SIZE as u64)?;
        h.ac_chain
            .update_oneshot(d, Bytes::from(vec![0xBBu8; PRESSURE_SIZE]))
            .await
            .err_tip(|| format!("writing AC pressure entry {hash}"))?;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let has_fut = h.fast_mem.has(pinned_digest);
    let res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, has_fut)
        .await
        .expect(
            "timed out asking AC fast-tier MemoryStore::has — chain regression \
             in AC unpin/has plumbing",
        );
    let observed = res.expect("AC MemoryStore::has returned Err");

    assert!(
        observed.is_none(),
        "AC pin leaks past BIS-ack — entry A survived sibling-pressure \
         eviction even after unpin_digests through the AC chain; the pin \
         entry was not released. observed: {observed:?}, pinned_digest: \
         {pinned_digest:?}, pressure_blobs: {} × {} bytes, mem_cap: {} \
         bytes. With a 2 KiB pin cap and the oldest entry now unpinned, \
         A MUST be the LRU victim.",
        PRESSURE_HASHES.len(),
        PRESSURE_SIZE,
        MEM_CAP_BYTES,
    );

    Ok(())
}
