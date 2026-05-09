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

//! Production-composition test for the BIS / pin chain.
//!
//! Builds the actual deployed `cas_STORE` chain from `buildcache-native.json5`
//! and asserts all three contracts that depend on outermost-wrapper
//! behavior. The audit at
//! `.claude/reviews/why-bis-bug-not-caught/audit.md` flagged that the
//! 31-day BIS broadcast wedge happened because every existing test built
//! a bare `FastSlowStore` (`make_fss()`) — none ever wrapped it in the
//! production composition. This file is the regression / sentinel.
//!
//! Production CAS chain (server-side, from prod-server.json5):
//!
//! ```text
//! cas_STORE
//!   = VerifyStore { backend: cas_INNER }
//!   = ExistenceCacheStore { backend: SizePartitioningStore {
//!       lower: SMALL_CAS_CACHED  (FastSlowStore { Memory, slow_inner })
//!       upper: cas_FAST_SLOW_STORE (FastSlowStore { Memory, FilesystemStore })
//!   }}
//! ```
//!
//! For the unit test we substitute:
//! - the slow tier of `SMALL_CAS_CACHED` with a MemoryStore (Redis is out
//!   of scope; what we're testing is delegation, not Redis semantics)
//! - the slow tier of `cas_FAST_SLOW_STORE` with a real FilesystemStore
//!   (so we can exercise pin-survives-eviction with actual on-disk state).
//!
//! Each test wraps the entire write or read under
//! `tokio::time::timeout(...)` per CLAUDE.md. The timeout is the
//! deadlock detector — without it, the test would hang the CI runner
//! instead of producing a meaningful failure.
//!
//! To produce the mutation-step "kill" on the SizePartitioningStore
//! delegation override (CLAUDE.md mandatory): comment out
//! `stable_delegation` in `nativelink-store/src/size_partitioning_store.rs`
//! (or replace the body with `StableDigestDelegation::Leaf`) and rerun
//! `outer_stable_notify_fires_when_inner_fastslow_completes_slow_write`.
//! It must panic with the contract-violated message specifying which
//! property failed.

use core::time::Duration;
use std::env;

use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, FilesystemSpec, MemorySpec,
    SizePartitioningSpec, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use rand::Rng;

/// Generous deadlock-detector timeout. A correctly-wired BIS chain
/// notifies in milliseconds; 5 seconds protects against slow CI runners
/// without masking real wedges. Per CLAUDE.md: an `Elapsed` from a too-
/// short timeout would mask the real bug.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Threshold dividing small (Redis-backed) and large (Filesystem-backed)
/// CAS blobs in production. We use the same value so the production-chain
/// shape is preserved.
const SIZE_PARTITION_THRESHOLD: u64 = 16 * 1024;

/// SHA256("hello world\n"). Used as the precomputed digest for tests
/// that go through VerifyStore with `verify_hash: true`.
const HELLO_WORLD_SHA256: &str = "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447";

fn make_temp_path(data: &str) -> String {
    format!(
        "{}/production-composition-{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
        data
    )
}

/// Build the actual deployed CAS chain (modulo Redis substitution
/// described in the file header). Returns the outermost `Store` handle
/// (cas_STORE = VerifyStore) plus the underlying FilesystemStore that
/// backs the upper tier (so pin-survives-eviction can exercise the real
/// on-disk path).
async fn build_cas_chain() -> Result<(Store, std::sync::Arc<FilesystemStore<FileEntryImpl>>), Error>
{
    // Upper tier — large blobs land here. Memory + Filesystem.
    let fs_content = make_temp_path("fs-content");
    let fs_temp = make_temp_path("fs-temp");
    let fs_store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: fs_content,
        temp_path: fs_temp,
        eviction_policy: Some(EvictionPolicy {
            // pin_cap is 25% of max_bytes (PIN_CAP_FRACTION in
            // moka_evicting_map.rs:42). Pinned blob is ~16448 bytes so
            // pin_cap must be ≥ 16448 → max_bytes ≥ 65792. We use 80 KiB
            // so pin_cap = 20480 (fits the pinned blob) and the cache
            // capacity (80 KiB minus what gets pinned-out) cannot hold
            // 8 pad blobs of 16448 each — eviction must fire.
            max_bytes: 80 * 1024,
            max_count: 64,
            ..Default::default()
        }),
        block_size: 1,
        ..Default::default()
    })
    .await?;
    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_slow = Store::new(fs_store.clone());
    let upper_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        upper_fast,
        upper_slow,
    );

    // Lower tier — small blobs land here. Memory + Memory (Redis-substitute).
    let lower_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        lower_fast,
        lower_slow,
    );

    // SizePartitioningStore — the load-bearing wrapper that the audit
    // identified as the silent-default-trap source.
    let size_part = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: SIZE_PARTITION_THRESHOLD,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        Store::new(lower_fss),
        Store::new(upper_fss),
    );

    // ExistenceCacheStore wraps SizePartitioningStore.
    let cache = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1024,
                ..Default::default()
            }),
        },
        Store::new(size_part),
    );

    // VerifyStore wraps ExistenceCacheStore = cas_STORE outermost.
    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(cache),
    );

    Ok((Store::new(verify), fs_store))
}

/// Property 1: outermost `stable_notify().notified()` MUST fire when an
/// inner FastSlowStore completes its slow-store write.
///
/// **This is the exact scenario that wedged for 31 days in production.**
/// SizePartitioningStore inherited the no-op `stable_notify` default and
/// returned a never-woken Notify, breaking the
/// `BlobsInStableStorage` broadcast loop in `nativelink.rs:367-380`.
///
/// Pre-fix this test would fire the `tokio::time::timeout` and panic
/// with the "must not deadlock" message. With the forced-delegation
/// design, the SizePartitioningStore.stable_delegation() == Many wires
/// the merged Notify automatically.
#[nativelink_test]
async fn outer_stable_notify_fires_when_inner_fastslow_completes_slow_write() -> Result<(), Error> {
    let (cas_store, _fs_backend) = build_cas_chain().await?;

    // Subscribe to the OUTERMOST Notify before writing. This must reach
    // through:
    //   VerifyStore::stable_notify()  (Inner ⇒ ExistenceCacheStore)
    //   ExistenceCacheStore::stable_notify()  (Inner ⇒ SizePartitioning)
    //   SizePartitioningStore::stable_notify() (Many ⇒ merged of {lower, upper})
    //   each FastSlowStore::stable_notify()   (Leaf override returning own Notify)
    let outer_notify = cas_store.as_store_driver().stable_notify();
    let notified_fut = {
        let n = outer_notify.clone();
        async move { n.notified().await }
    };

    // Write a digest BIGGER than the partition threshold so it routes to
    // the upper FastSlowStore (Filesystem-backed). The slow write has to
    // complete before stable_notify fires.
    let payload = vec![0xABu8; SIZE_PARTITION_THRESHOLD as usize + 64];
    let payload_size = payload.len() as u64;
    // verify_size=true so size must match the digest's claim.
    let digest = DigestInfo::try_new(HELLO_WORLD_SHA256, payload_size)?;

    let write_fut = cas_store.update_oneshot(digest, Bytes::from(payload));

    // Run the write and the notify-wait concurrently. Whichever direction
    // breaks would dead-end the test.
    let combined = async move {
        write_fut.await?;
        notified_fut.await;
        Ok::<(), Error>(())
    };

    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, combined)
        .await
        .expect(
            "DEADLOCK DETECTED: outer stable_notify did not fire within timeout. \
             Production-composition contract violated — SizePartitioningStore is \
             not forwarding stable_notify from its inner FastSlowStore. Verify \
             stable_delegation() returns Many { children, merged_state } and that \
             the wrapper owns OnceLock<MergedNotifyState> field.",
        )?;

    Ok(())
}

/// Property 2: outermost `drain_stable_digests()` MUST return the digest
/// after a write completes through the chain.
///
/// Same bug class as Property 1 but verified via the drain side of the
/// drain-then-fire pattern.
#[nativelink_test]
async fn outer_drain_stable_digests_returns_inner_fastslow_completed_digest() -> Result<(), Error> {
    let (cas_store, _fs_backend) = build_cas_chain().await?;

    let outer_notify = cas_store.as_store_driver().stable_notify();
    let payload = vec![0x42u8; SIZE_PARTITION_THRESHOLD as usize + 128];
    let payload_size = payload.len() as u64;
    let digest = DigestInfo::try_new(HELLO_WORLD_SHA256, payload_size)?;

    let combined = async {
        cas_store
            .update_oneshot(digest, Bytes::from(payload))
            .await?;
        // Wait for the slow-write notify to fire so the digest is in
        // the chain's drain queue.
        outer_notify.notified().await;
        // Drain at the OUTERMOST layer (cas_STORE) — what
        // `nativelink.rs:391` does in production.
        let drained = cas_store.as_store_driver().drain_stable_digests();
        assert!(
            drained.iter().any(|d| *d == digest),
            "DEADLOCK / DRAIN FAILURE: drain_stable_digests at the OUTERMOST chain \
             layer did not include the just-written digest. Drained: {drained:?}; \
             expected: {digest:?}. SizePartitioningStore is not concatenating \
             drains from its inner FastSlowStore children. Verify Many's \
             drain_stable_digests trait default body."
        );
        Ok::<(), Error>(())
    };

    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, combined)
        .await
        .expect(
            "DEADLOCK DETECTED: outer drain_stable_digests did not return within \
             timeout. Stable-notify chain or write itself is wedged.",
        )?;

    Ok(())
}

/// Property 3: a digest pinned at the outermost layer MUST survive
/// eviction pressure on the FilesystemStore that backs the upper tier.
///
/// This validates that `pin_digests` traverses the chain unchanged:
///   VerifyStore::pin_digests       (Inner)
///   ExistenceCacheStore::pin_digests  (Inner)
///   SizePartitioningStore::pin_digests (Many — fans out to both inner FSS)
///   FastSlowStore::pin_digests     (Many — fans to fast + slow)
///   FilesystemStore::pin_digests   (Leaf — actually pins via MokaEvictingMap)
///
/// If any link in this chain returns the silent no-op default
/// `pin_digests {}`, the pin won't reach FilesystemStore and the
/// blob-survives-eviction assertion below will fail.
///
/// To exercise the FilesystemStore eviction cap (max_bytes = 64 KiB,
/// max_count = 64 from `build_cas_chain`) we write the pinned blob first,
/// pin it, then write enough additional blobs to push past the cap. The
/// pinned blob must still be readable; an unpinned blob from the same
/// burst is allowed to be evicted.
#[nativelink_test]
async fn outer_pin_digests_survives_eviction_pressure_on_fs_backend() -> Result<(), Error> {
    let (cas_store, fs_backend) = build_cas_chain().await?;

    // Write a UPPER-tier blob (above partition) so it lands in the FS-backed
    // FastSlowStore. Use distinct hashes so each write is a separate digest.
    let pinned_payload = vec![0x77u8; (SIZE_PARTITION_THRESHOLD + 32) as usize];
    let pinned_size = pinned_payload.len() as u64;
    let pinned_hash = "a000000000000000000000000000000000000000000000000000000000000001";
    let pinned_digest = DigestInfo::try_new(pinned_hash, pinned_size)?;

    let outer_notify = cas_store.as_store_driver().stable_notify();
    cas_store
        .update_oneshot(pinned_digest, Bytes::from(pinned_payload))
        .await?;
    // Wait for slow-write completion so the blob is on disk before pinning.
    tokio::time::timeout(NO_DEADLOCK_TIMEOUT, outer_notify.notified())
        .await
        .expect("write notify wedged before pin");
    let _ = cas_store.as_store_driver().drain_stable_digests();

    // Pin at the OUTERMOST layer. Per the chain trace above, this MUST
    // reach FilesystemStore::pin_digests for the pin to land.
    cas_store.as_store_driver().pin_digests(&[pinned_digest]);

    // Verify the pin landed on the FS backend by checking the evicting map
    // sees the blob as resident.
    assert_eq!(
        fs_backend.has(pinned_digest).await?,
        Some(pinned_size),
        "PIN ROUTING BROKEN: pinned digest was not resident in the FilesystemStore \
         backend after a successful write through the chain."
    );

    // Now apply eviction pressure: write enough OTHER upper-tier blobs to
    // push the FS cap (64 KiB / 64 entries). Each ~16 KiB+, 8 of them =
    // ~128 KiB, well above the cap.
    for seed in 0u8..8 {
        let pad_payload = vec![seed; (SIZE_PARTITION_THRESHOLD + 64) as usize];
        let pad_size = pad_payload.len() as u64;
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0] = 0xB0;
        hash_bytes[1] = seed;
        let hash_str = hex_encode(&hash_bytes);
        let pad_digest = DigestInfo::try_new(&hash_str, pad_size)?;
        cas_store
            .update_oneshot(pad_digest, Bytes::from(pad_payload))
            .await?;
        // Wait for slow notify so the blob is durably present on disk.
        let _ = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, outer_notify.notified()).await;
        let _ = cas_store.as_store_driver().drain_stable_digests();
    }

    // Pinned blob MUST still be present on disk — even though we pushed
    // 8 × 16 KiB+ blobs through a 64-KiB-cap FS. If `pin_digests` was
    // dropped at any layer, the LRU-evicting map would have reclaimed
    // the pinned blob's slot.
    let after_eviction = fs_backend.has(pinned_digest).await?;
    assert_eq!(
        after_eviction,
        Some(pinned_size),
        "PIN DID NOT SURVIVE EVICTION: blob {pinned_digest:?} was reclaimed by the \
         LRU evicting map despite being pinned at the OUTERMOST chain layer. The \
         pin_digests call did not propagate through the wrapper chain. Verify \
         pin_delegation() on every layer (VerifyStore=Inner, \
         ExistenceCacheStore=Inner, SizePartitioningStore=Many, FastSlowStore=Many)."
    );

    Ok(())
}

/// Property 4: Many-position wrappers must EAGERLY initialize their
/// `merged_stable_notify` `OnceLock` during `new()`, not lazily on first
/// `stable_notify()` call.
///
/// **Bug class (perf-optimizer Finding 1, CRITICAL).** With lazy
/// initialization, a writer between `new()` and the first external
/// `stable_notify()` call can fire `notify_one` on an inner child's
/// Notify before the wrapper's forwarder task is registered. The first
/// call to `stable_notify()` finally spawns the forwarder, which on
/// first poll consumes the permit and fires the merged Notify — but
/// permits don't accumulate (Notify caps at 1 permit), so multi-fire
/// pre-spawn races collapse silently.
///
/// Eager init in `SizePartitioningStore::new` / `ShardStore::new` /
/// `DedupStore::new` ensures the forwarder is running before any
/// writer can fire, moving the invariant out of caller etiquette and
/// into the type system.
///
/// To produce the mutation-step "kill": comment out the
/// `let _ = StoreDriver::stable_notify(result.as_ref());` line in any
/// of the three Many constructors and rerun this test.
#[nativelink_test]
async fn many_wrappers_eagerly_initialize_merged_stable_notify_on_new() -> Result<(), Error> {
    use nativelink_config::stores::{DedupSpec, MemorySpec, ShardConfig, ShardSpec};
    use nativelink_store::dedup_store::DedupStore;
    use nativelink_store::shard_store::ShardStore;

    // SizePartitioningStore — uses build_cas_chain's wrapper construction
    // path (the wrapper's new() must eagerly init).
    let lower = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper = Store::new(MemoryStore::new(&MemorySpec::default()));
    let size_part = SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: SIZE_PARTITION_THRESHOLD,
            lower_store: StoreSpec::Memory(MemorySpec::default()),
            upper_store: StoreSpec::Memory(MemorySpec::default()),
        },
        lower,
        upper,
    );
    assert!(
        size_part.merged_stable_notify_initialized(),
        "SizePartitioningStore::new must eagerly initialize merged_stable_notify (perf-optimizer F1). \
         Lazy init opens a cold-path lost-wakeup window between construction and first external stable_notify() call."
    );

    // ShardStore — at least 2 shards so the Many fan-out applies.
    let shard_lower = Store::new(MemoryStore::new(&MemorySpec::default()));
    let shard_upper = Store::new(MemoryStore::new(&MemorySpec::default()));
    let shard = ShardStore::new(
        &ShardSpec {
            stores: vec![
                ShardConfig {
                    store: StoreSpec::Memory(MemorySpec::default()),
                    weight: Some(1),
                },
                ShardConfig {
                    store: StoreSpec::Memory(MemorySpec::default()),
                    weight: Some(1),
                },
            ],
        },
        vec![shard_lower, shard_upper],
    )?;
    assert!(
        shard.merged_stable_notify_initialized(),
        "ShardStore::new must eagerly initialize merged_stable_notify (perf-optimizer F1)."
    );

    // DedupStore — index + content child.
    let dedup_index = Store::new(MemoryStore::new(&MemorySpec::default()));
    let dedup_content = Store::new(MemoryStore::new(&MemorySpec::default()));
    let dedup = DedupStore::new(
        &DedupSpec {
            index_store: StoreSpec::Memory(MemorySpec::default()),
            content_store: StoreSpec::Memory(MemorySpec::default()),
            min_size: 0,
            normal_size: 0,
            max_size: 0,
            max_concurrent_fetch_per_get: 0,
        },
        dedup_index,
        dedup_content,
    )?;
    assert!(
        dedup.merged_stable_notify_initialized(),
        "DedupStore::new must eagerly initialize merged_stable_notify (perf-optimizer F1)."
    );

    Ok(())
}

/// Hex-encode 32 bytes to a 64-char lowercase string (SHA256 digest format).
/// Local helper — no `hex` crate dep needed for this one usage.
fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}
