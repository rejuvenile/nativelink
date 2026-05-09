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

//! #334 Fix C eviction extension — `MemoryStore::check_backpressure_gate`
//! must attempt LRU eviction of UNPINNED entries before emitting the
//! typed `BackpressureSignal::MemoryStoreAtCapacity` signal. Without
//! this, the production server's `cas_FAST_SLOW_STORE.fast = MemoryStore`
//! becomes write-once-evict-never the moment it reaches cap (the gate
//! refuses inserts before moka's natural admission-driven LRU can fire).
//!
//! ## Asymmetric coverage (CLAUDE.md)
//!
//! - **Under-action: eviction frees room.** Cache full of UNPINNED
//!   entries; gate-driven eviction evicts to make room; the new write
//!   is admitted (not rejected). Mutation step: comment out the
//!   `evict_unpinned_lru_bytes` call in `check_backpressure_gate`;
//!   the test red-fails because the new write is rejected with the
//!   typed signal even though there were evictable entries.
//!
//! - **Over-action: pinned entries are protected.** Cache full of
//!   PINNED entries; gate-driven eviction finds nothing evictable;
//!   the new write IS rejected with the typed signal. Mutation step:
//!   remove the pinned-entry skip in `evict_unpinned_lru_bytes`; the
//!   test red-fails because the pinned entries are evicted (durability
//!   invariant broken) and the new write is admitted.
//!
//! - **Mixed (partial eviction can still succeed when freed bytes are
//!   enough).** Cache contains mixed pinned + unpinned entries; gate-
//!   driven eviction frees enough unpinned bytes to admit the new
//!   write while pinned entries survive intact. This is the production
//!   steady state during a BIS ack window — some bytes load-bearing,
//!   the rest evictable on demand.
//!
//! Each test runs under a `tokio::time::timeout(5s)` deadlock detector
//! so a regression that wedges the gate (e.g. holding a lock across the
//! eviction loop) red-fails as a panic with a bespoke message instead
//! of hanging the CI runner.
//!
//! ## Why a 4 KiB cap
//!
//! With cap = 4 KiB and pin_cap = 25% × cap = 1 KiB, a single 1 KiB
//! entry can be pinned (the largest pin that fits). Smaller caps
//! either prevent pinning (`pin_keys: pin cap exceeded` no-op) or
//! prevent the gate from firing on realistic write sizes. 4 KiB is
//! the smallest cap that supports both pinning and a triggerable gate.

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{EvictionPolicy, MemorySpec};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};

const VALID_HASH1: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const VALID_HASH2: &str = "0000000000000000000000000000000000000000000000000000000000000002";
const VALID_HASH3: &str = "0000000000000000000000000000000000000000000000000000000000000003";
const VALID_HASH4: &str = "0000000000000000000000000000000000000000000000000000000000000004";
const VALID_HASH5: &str = "0000000000000000000000000000000000000000000000000000000000000005";

/// 4 KiB — the smallest cap that supports both a real 1 KiB pin
/// (pin_cap = 25% × cap = 1 KiB) and an over-cap incoming write to
/// trip the gate. See module docs for the cap-sizing rationale.
const CAP_BYTES: usize = 4096;
/// 1 KiB — fits inside pin_cap so `pin_digests` actually pins the
/// entry instead of silently no-op'ing.
const ENTRY_BYTES: usize = 1024;

/// 5 s deadlock detector — the gate eviction loop is sub-millisecond
/// in healthy operation; a 5 s timeout catches lock-contention or
/// blocking-await regressions without masking real bugs as Elapsed.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

fn make_store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: CAP_BYTES,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    store.enable_emit_backpressure();
    store
}

// ---------------------------------------------------------------------------
// Test 1 (under-action): eviction extension frees room for unpinned entries.
//
// Setup:
//   - 4 KiB cap MemoryStore with backpressure ENABLED.
//   - Fill cap with 4 × 1 KiB UNPINNED entries (cache_bytes ≈ 4 KiB).
//   - Issue a 1 KiB write that would otherwise trip the gate
//     (4096 + 1024 > 4096).
//
// Expected post-fix:
//   - The gate detects over-cap, calls `evict_unpinned_lru_bytes(1024)`,
//     which frees ≥ 1 KiB by evicting one of the unpinned entries,
//     then re-checks `would_exceed_capacity` — now FALSE — and admits
//     the new write.
//   - Result: `update_oneshot` returns Ok; the new entry is present
//     via `has`.
//
// Mutation step: comment out the
// `let report = self.evicting_map.evict_unpinned_lru_bytes(...)` call
// (and the subsequent re-check) in `check_backpressure_gate`. The
// gate then immediately emits the typed signal even though there are
// evictable entries; this test red-fails on the bespoke
// "Fix C eviction extension regressed — gate refused admission \
//  with evictable unpinned entries available" message.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn eviction_extension_admits_when_unpinned_entries_can_be_freed(
) -> Result<(), Error> {
    let store = make_store();

    // Fill the cap with 4 unpinned 1 KiB entries.
    let unpinned_hashes = [VALID_HASH1, VALID_HASH2, VALID_HASH3, VALID_HASH4];
    for hash in &unpinned_hashes {
        let d = DigestInfo::try_new(hash, ENTRY_BYTES as u64)?;
        store
            .update_oneshot(d, Bytes::from(vec![0xAAu8; ENTRY_BYTES]))
            .await
            .expect("filling cap with unpinned entries");
    }

    // The under-test write — would be over cap without eviction.
    let new_digest = DigestInfo::try_new(VALID_HASH5, ENTRY_BYTES as u64)?;
    let new_payload = Bytes::from(vec![0xBBu8; ENTRY_BYTES]);

    let result = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        store.update_oneshot(new_digest, new_payload),
    )
    .await
    .expect(
        "must not deadlock — gate-driven eviction must complete promptly \
         (sub-second). A timeout here means the eviction loop is holding \
         a lock across an await or otherwise wedging the gate.",
    );

    result.expect(
        "Fix C eviction extension regressed — gate refused admission with \
         evictable unpinned entries available. With cap = 4 KiB, \
         4 unpinned entries × 1 KiB filling the cache, and a 1 KiB \
         incoming write, the gate MUST evict ≥ 1 KiB of unpinned LRU \
         entries and admit the new write. A failure here means the \
         eviction extension call in check_backpressure_gate is missing, \
         broken, or the re-check after eviction is wrong.",
    );

    // The new entry must actually be present in the store.
    let landed = Store::new(store.clone()).has(new_digest).await?;
    assert_eq!(
        landed,
        Some(ENTRY_BYTES as u64),
        "post-eviction the new entry MUST be visible via has() — \
         got {landed:?}",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 2 (over-action): pinned entries are PROTECTED — gate emits the
// typed signal when nothing evictable remains.
//
// Setup:
//   - 4 KiB cap MemoryStore with backpressure ENABLED.
//   - Insert 1 × 1 KiB entry, then PIN it via the production
//     `pin_digests` trait method (the BIS-ack-window analogue).
//   - Issue a 4 KiB write that would over-cap even after evicting
//     anything in the cache (which is empty — pinned entry lives
//     outside moka's `cache`).
//
// Expected post-fix:
//   - Gate detects over-cap, calls `evict_unpinned_lru_bytes(4096)`,
//     which finds NOTHING in `cache` (the 1 KiB pinned entry is in
//     `pinned` DashMap, not in `cache`). Re-check: still over cap.
//     Emits the typed `BackpressureSignal::MemoryStoreAtCapacity`.
//   - The pinned entry MUST still be present via `has` (durability
//     invariant — the BIS-ack pin is the whole reason the gate
//     refuses to silent-evict).
//
// Mutation step (corrected per #334 bundle fixup #8c): the original
// docstring claimed the skip in `evict_unpinned_lru_bytes`'s iter
// loop (`if check_pinned && self.pinned.contains_key(q) { continue;
// }`) was the load-bearing site. That's misleading: `pin_keys` calls
// `self.cache.invalidate(q)` at `moka_evicting_map.rs:895` so pinned
// entries are MOVED OUT of the cache (they live in the `pinned`
// DashMap exclusively, with `pinned_bytes` tracked separately). The
// iter loop's `continue` is therefore dead — pinned keys never appear
// in `cache.iter()` to skip in the first place.
//
// The actual guard is `pinned_bytes` accounting in
// `would_exceed_capacity`: it reads `cache.weighted_size() +
// pinned_bytes` so the over-cap check stays honest after entries are
// pinned-into / unpinned-out. To mutate, replace the
// `cache.weighted_size().saturating_add(pinned_bytes_now)` in
// `would_exceed_capacity` with just `cache.weighted_size()`. The
// pinned 1 KiB entry then becomes invisible to the cap check; the
// gate believes it has 4 KiB free, attempts to admit, succeeds, and
// the durability invariant is broken — `has(pinned_digest)` returns
// None because the pinned entry was overwritten by the new admission.
// This test red-fails on the `has(pinned_digest)` assertion with the
// bespoke message below.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn eviction_extension_emits_signal_when_all_bytes_pinned(
) -> Result<(), Error> {
    let store = make_store();

    // Insert + pin a 1 KiB entry. After this, cache is empty; the
    // 1 KiB lives in the `pinned` DashMap. (Use Store wrapper to
    // dispatch the StoreDriver trait method without bringing
    // StoreDriver into scope and shadowing StoreLike's method names.)
    let pinned_digest = DigestInfo::try_new(VALID_HASH1, ENTRY_BYTES as u64)?;
    store
        .update_oneshot(pinned_digest, Bytes::from(vec![0xAAu8; ENTRY_BYTES]))
        .await
        .expect("inserting the to-be-pinned entry");
    Store::new(store.clone()).pin_digests(&[pinned_digest]);

    // Verify the pin actually took (pin_cap = 1 KiB ≥ 1 KiB entry).
    // This is a setup precondition — if it fails, the test wouldn't
    // be exercising the contract we claim to be testing.
    let setup_check = Store::new(store.clone()).has(pinned_digest).await?;
    assert_eq!(
        setup_check,
        Some(ENTRY_BYTES as u64),
        "test setup invariant — pinned entry must be present before the \
         gate test runs (got {setup_check:?})",
    );

    // The under-test write — exceeds cap even after evicting cache
    // (which is empty). 0 cache + 1024 pinned + 4096 incoming = 5120 \
    // > 4096 cap. Eviction frees 0 bytes (cache empty). Re-check:
    // still over cap. Emits.
    let new_digest = DigestInfo::try_new(VALID_HASH2, CAP_BYTES as u64)?;
    let new_payload = Bytes::from(vec![0xBBu8; CAP_BYTES]);

    let result = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        store.update_oneshot(new_digest, new_payload),
    )
    .await
    .expect(
        "must not deadlock — gate must emit signal promptly even when \
         every byte of cap is pinned. A timeout here means the eviction \
         loop is wedged or the re-check is blocking.",
    );

    let err = result.expect_err(
        "Fix C eviction extension MUST emit the typed signal when no \
         unpinned entries are evictable. With cap = 4 KiB, 1 KiB pinned, \
         and a 4 KiB incoming write, the gate's eviction call finds \
         nothing in cache (the pinned entry lives outside moka's cache), \
         re-checks would_exceed_capacity, and MUST return \
         ResourceExhausted+BackpressureSignal::MemoryStoreAtCapacity. A \
         failure here means either the re-check is wrong (admitting \
         despite over-cap) OR the pinned entry was illegally evicted \
         (durability invariant broken).",
    );
    assert_eq!(
        err.code,
        Code::ResourceExhausted,
        "expected ResourceExhausted, got code={:?} messages={:?}",
        err.code,
        err.messages,
    );

    // The pinned entry MUST still be present — the eviction extension
    // is FORBIDDEN from touching pinned entries. This is the over-
    // action half of the contract.
    let post = Store::new(store.clone()).has(pinned_digest).await?;
    assert_eq!(
        post,
        Some(ENTRY_BYTES as u64),
        "pinned entry was illegally evicted — Fix C eviction extension \
         must NEVER touch keys present in `pinned`. Eviction observed \
         on the pinned entry breaks the BIS-ack durability invariant. \
         Got {post:?} for the pinned digest.",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3 (mixed): partial eviction admits the write when freed bytes
// suffice, while pinned entries survive intact.
//
// Setup:
//   - 4 KiB cap MemoryStore with backpressure ENABLED.
//   - Insert 1 × 1 KiB entry and pin it (consumes 1 KiB of the cap
//     via the pinned DashMap).
//   - Insert 3 × 1 KiB UNPINNED entries (cache_bytes ≈ 3 KiB).
//     Total: 4 KiB pinned + cache → exactly at cap.
//   - Issue a 1 KiB write. would_exceed_capacity:
//     3072 + 1024 + 1024 = 5120 > 4096 → true.
//
// Expected post-fix:
//   - Eviction walks cache, finds the 3 unpinned entries (skips the
//     pinned one — but it's not in cache anyway), evicts ONE
//     (~1 KiB) to free room, re-checks (2048 + 1024 + 1024 = 4096
//     > 4096? FALSE — equals not exceeds), admits the new write.
//   - The pinned entry is untouched (still present via `has`).
//   - The new entry is present via `has`.
//   - Some unpinned entries may or may not be present (eviction order
//     is moka's iteration order which is arbitrary — we only assert
//     that AT LEAST ONE was evicted, not WHICH).
//
// Mutation step: same as test 2 (remove the pinned-skip) — this test
// red-fails on the pinned-entry survival assertion. Or comment out
// the `evict_unpinned_lru_bytes` call entirely — this test red-fails
// on the new-entry admission assertion (the gate refuses admission
// even though eviction could have freed room).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn eviction_extension_partial_eviction_admits_and_preserves_pin(
) -> Result<(), Error> {
    let store = make_store();

    // Pin the first entry — this is the BIS-ack-window analogue.
    let pinned_digest = DigestInfo::try_new(VALID_HASH1, ENTRY_BYTES as u64)?;
    store
        .update_oneshot(pinned_digest, Bytes::from(vec![0xAAu8; ENTRY_BYTES]))
        .await
        .expect("inserting to-be-pinned entry");
    Store::new(store.clone()).pin_digests(&[pinned_digest]);

    // Fill the rest of the cap with 3 unpinned entries. Total bytes
    // accounted: 1 KiB pinned + 3 KiB cache = 4 KiB (at cap).
    let unpinned_hashes = [VALID_HASH2, VALID_HASH3, VALID_HASH4];
    for hash in &unpinned_hashes {
        let d = DigestInfo::try_new(hash, ENTRY_BYTES as u64)?;
        store
            .update_oneshot(d, Bytes::from(vec![0xBBu8; ENTRY_BYTES]))
            .await
            .expect("filling rest of cap with unpinned entries");
    }

    // Under-test write — 1 KiB. Would over-cap without eviction.
    let new_digest = DigestInfo::try_new(VALID_HASH5, ENTRY_BYTES as u64)?;
    let new_payload = Bytes::from(vec![0xCCu8; ENTRY_BYTES]);

    let result = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        store.update_oneshot(new_digest, new_payload),
    )
    .await
    .expect(
        "must not deadlock — partial eviction in mixed pinned/unpinned \
         scenario must complete promptly",
    );

    result.expect(
        "Fix C eviction extension regressed — gate refused admission in \
         mixed pinned/unpinned scenario where eviction of unpinned LRU \
         entries SHOULD free enough room. With cap = 4 KiB, 1 KiB pinned, \
         3 × 1 KiB unpinned, and a 1 KiB incoming write, evicting one \
         unpinned entry frees enough (2048 + 1024 + 1024 = 4096 ≤ 4096) \
         to admit. A failure here means the eviction extension is \
         under-evicting or the re-check predicate is wrong.",
    );

    // The pinned entry MUST survive — this is the over-action half of
    // the contract (eviction must skip pinned).
    let pinned_post = Store::new(store.clone()).has(pinned_digest).await?;
    assert_eq!(
        pinned_post,
        Some(ENTRY_BYTES as u64),
        "pinned entry was illegally evicted by the partial-eviction path \
         — Fix C eviction extension must NEVER touch keys present in \
         `pinned` even when freeing room for a competing write. Got \
         {pinned_post:?} for the pinned digest.",
    );

    // The new entry MUST be present.
    let new_post = Store::new(store.clone()).has(new_digest).await?;
    assert_eq!(
        new_post,
        Some(ENTRY_BYTES as u64),
        "new entry MUST be visible after partial-eviction-driven \
         admission — got {new_post:?}",
    );

    // AT LEAST ONE of the unpinned entries must have been evicted.
    // Moka's iteration order is arbitrary, so we don't assert WHICH;
    // we assert the count of survivors is < 3.
    let mut survivor_count = 0u32;
    for hash in &unpinned_hashes {
        let d = DigestInfo::try_new(hash, ENTRY_BYTES as u64)?;
        if Store::new(store.clone()).has(d).await?.is_some() {
            survivor_count += 1;
        }
    }
    assert!(
        survivor_count < 3,
        "Fix C eviction extension must evict ≥ 1 unpinned entry to free \
         room for the new write. With 3 unpinned entries × 1 KiB filling \
         the cache and a 1 KiB incoming write, at least one MUST have \
         been invalidated. survivor_count = {survivor_count} (≥ 3 means \
         no eviction happened, contradicting the new entry's admission).",
    );
    Ok(())
}
