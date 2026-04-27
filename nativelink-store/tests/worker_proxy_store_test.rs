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

use core::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    IS_WORKER_REQUEST, ItemCallback, MarkStableDelegation, PinDelegation, REDIRECT_PREFIX,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, StoreOptimizations,
    UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";

/// Helper: create a WorkerProxyStore backed by a fresh MemoryStore.
/// Returns (proxy_store_as_Store, inner_memory_store, locality_map).
fn make_proxy_store() -> (Store, Store, SharedBlobLocalityMap) {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner.clone(), locality_map.clone());
    (Store::new(proxy), inner, locality_map)
}

// -------------------------------------------------------------------
// 1. get_part delegates to inner store on hit
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_returns_data_from_inner_store_on_hit() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let value = b"hello from inner store";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Write directly through the proxy (which delegates update to inner).
    proxy
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    // Register a fake worker in the locality map. If get_part were to
    // consult it, it would try to connect and potentially fail or return
    // different data. We verify the inner store data is returned instead.
    locality_map
        .write()
        .register_blobs("fake-worker:9999", &[digest]);

    let result = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        value,
        "Expected data from inner store, not from worker"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 2. get_part returns NotFound when inner misses and no peers
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_returns_not_found_when_inner_misses_and_no_peers() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 42)?;

    let result = proxy.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "Expected an error for missing blob");

    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::NotFound,
        "Expected NotFound error code, got: {err:?}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 3. has delegates to inner store (returns Some on hit)
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_returns_size_when_inner_has_blob() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let value = b"test data for has";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    proxy
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    let size = proxy.has(digest).await?;
    assert_eq!(
        size,
        Some(value.len() as u64),
        "has() should return the blob size from inner store"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 4. has falls back to the locality map when inner misses
//    (locality-aware FMB; commit d3399e48, 2026-04-22).
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_falls_back_to_locality_map_when_inner_missing() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // Register the digest on a worker endpoint.
    locality_map
        .write()
        .register_blobs("worker-a:50081", &[digest]);

    // has() consults the locality_map after the inner store misses and
    // reports `Some(digest.size_bytes())` if any worker holds the blob.
    // This is what allows Bazel's FindMissingBlobs to recognize that a
    // blob "lives only on the worker" and skip re-uploading.
    let size = proxy.has(digest).await?;
    assert_eq!(
        size,
        Some(100),
        "has() should fall back to locality map and report the digest \
         size_bytes when a worker is registered for the blob"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 5. has_with_results delegates to inner store and then falls back to
//    the locality map for digests still missing after the inner check
//    (locality-aware FMB; commit d3399e48, 2026-04-22).
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_with_results_falls_back_to_locality_map_when_inner_missing()
-> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let value = b"test data";
    let d1 = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;
    let d2 = DigestInfo::try_new(VALID_HASH2, 999)?;
    let d3 = DigestInfo::try_new(VALID_HASH3, 50)?;

    // Only d1 is in the inner store.
    proxy
        .update_oneshot(d1, Bytes::from_static(value))
        .await?;

    // Register d2 and d3 on workers — has_with_results consults the
    // locality_map for digests still missing after the inner check and
    // reports them as `Some(digest.size_bytes())`. This is how Bazel's
    // FindMissingBlobs sees a blob that lives only on a worker and
    // skips re-uploading it.
    {
        let mut map = locality_map.write();
        map.register_blobs("worker-a:50081", &[d2]);
        map.register_blobs("worker-b:50081", &[d3]);
    }

    let keys: Vec<StoreKey<'_>> = vec![d1.into(), d2.into(), d3.into()];
    let mut results = vec![None; 3];
    proxy.has_with_results(&keys, &mut results).await?;

    assert_eq!(
        results[0],
        Some(value.len() as u64),
        "d1 should be found in inner store"
    );
    assert_eq!(
        results[1],
        Some(999),
        "d2 should be found via locality_map fallback (digest size_bytes)"
    );
    assert_eq!(
        results[2],
        Some(50),
        "d3 should be found via locality_map fallback (digest size_bytes)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 5b. Kill-switch: disable_locality_in_has() bypasses the locality_map
//     fallback in has_with_results, returning inner-store-only results.
//     Guards the operator escape hatch at worker_proxy_store.rs:454.
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_with_results_skips_locality_after_disable_locality_in_has()
-> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // Register the digest on a worker — by default this would make
    // has_with_results return Some(100) via the locality fallback.
    locality_map
        .write()
        .register_blobs("worker-a:50081", &[digest]);

    // Operator kill-switch: disables the locality-aware fast path so
    // has_with_results reflects only what the inner (server) store holds.
    proxy_arc.disable_locality_in_has();

    let keys: Vec<StoreKey<'_>> = vec![digest.into()];
    let mut results = vec![None; 1];
    proxy.has_with_results(&keys, &mut results).await?;

    assert_eq!(
        results[0],
        None,
        "disable_locality_in_has() must bypass the locality_map fallback; \
         the inner store is empty so has_with_results must report None"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 6. has_with_results on empty digest list succeeds
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_with_results_empty_digests_succeeds() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let keys: Vec<StoreKey<'_>> = vec![];
    let mut results: Vec<Option<u64>> = vec![];
    proxy.has_with_results(&keys, &mut results).await?;

    // No assertions needed beyond not panicking.
    Ok(())
}

// -------------------------------------------------------------------
// 7. update_oneshot delegates to inner store
// -------------------------------------------------------------------
#[nativelink_test]
async fn update_oneshot_stores_in_inner() -> Result<(), Error> {
    let (proxy, inner, _locality_map) = make_proxy_store();

    let value = b"upload via proxy";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    proxy
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    // Verify the blob landed in the inner store directly.
    let inner_data = inner.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        inner_data.as_ref(),
        value,
        "Data should be present in the inner store after update_oneshot"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 8. get_part with offset and length on inner hit
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_with_offset_and_length_from_inner() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let value = b"0123456789abcdefghij"; // 20 bytes
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    proxy
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    // Read bytes [5..15) — 10 bytes at offset 5.
    let data = proxy.get_part_unchunked(digest, 5, Some(10)).await?;
    assert_eq!(
        data.as_ref(),
        b"56789abcde",
        "Expected subset at offset=5, length=10"
    );

    // Read from offset 15 to end.
    let data = proxy.get_part_unchunked(digest, 15, None).await?;
    assert_eq!(data.as_ref(), b"fghij", "Expected tail from offset=15");

    // Read 0 bytes.
    let data = proxy.get_part_unchunked(digest, 0, Some(0)).await?;
    assert_eq!(data.as_ref(), b"", "Expected empty result for length=0");

    Ok(())
}

// -------------------------------------------------------------------
// 9. Inner miss + locality has peers for a DIFFERENT digest
//    => the queried digest is still NotFound (locality map miss)
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_inner_miss_locality_has_different_digest_returns_not_found() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let d1 = DigestInfo::try_new(VALID_HASH1, 100)?;
    let d2 = DigestInfo::try_new(VALID_HASH2, 200)?;

    // Register d2 on a worker, but NOT d1.
    locality_map
        .write()
        .register_blobs("worker-a:50081", &[d2]);

    // Query d1 — not in inner store, not in locality map.
    let result = proxy.get_part_unchunked(d1, 0, None).await;
    assert!(result.is_err(), "Expected NotFound for d1");

    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::NotFound,
        "Expected NotFound since d1 has no locality entries, got: {err:?}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 10. Locality map returns empty workers list after eviction
//     => NotFound (no peers to try)
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_inner_miss_locality_evicted_returns_not_found() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // Register then evict the digest.
    {
        let mut map = locality_map.write();
        map.register_blobs("worker-a:50081", &[digest]);
        map.evict_blobs("worker-a:50081", &[digest]);
    }

    // Now there are no workers for this digest.
    let result = proxy.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "Expected NotFound after eviction");

    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::NotFound,
        "Expected NotFound since locality was evicted, got: {err:?}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 11. update followed by get_part roundtrip
// -------------------------------------------------------------------
#[nativelink_test]
async fn update_then_get_roundtrip() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let value = b"roundtrip data payload";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Upload via proxy.
    proxy
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    // Verify has() works.
    let size = proxy.has(digest).await?;
    assert_eq!(size, Some(value.len() as u64));

    // Verify get_part returns the correct data.
    let data = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(data.as_ref(), value);

    Ok(())
}

// -------------------------------------------------------------------
// 12. Multiple blobs: has_with_results shows correct presence
// -------------------------------------------------------------------
#[nativelink_test]
async fn has_with_results_multiple_blobs_mixed() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let v1 = b"first blob";
    let v3 = b"third blob";
    let d1 = DigestInfo::try_new(VALID_HASH1, v1.len() as u64)?;
    let d2 = DigestInfo::try_new(VALID_HASH2, 999)?; // not stored
    let d3 = DigestInfo::try_new(VALID_HASH3, v3.len() as u64)?;

    proxy
        .update_oneshot(d1, Bytes::from_static(v1))
        .await?;
    proxy
        .update_oneshot(d3, Bytes::from_static(v3))
        .await?;

    let keys: Vec<StoreKey<'_>> = vec![d1.into(), d2.into(), d3.into()];
    let mut results = vec![None; 3];
    proxy.has_with_results(&keys, &mut results).await?;

    assert_eq!(results[0], Some(v1.len() as u64), "d1 should be found");
    assert_eq!(results[1], None, "d2 should not be found");
    assert_eq!(results[2], Some(v3.len() as u64), "d3 should be found");

    Ok(())
}

// -------------------------------------------------------------------
// 13. get_part for a blob that was never stored and has no locality
//     entries returns NotFound (different digest, not in map at all)
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_completely_unknown_digest_returns_not_found() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    // Register a DIFFERENT digest on a worker (not the one we query).
    let other_digest = DigestInfo::try_new(VALID_HASH2, 50)?;
    locality_map
        .write()
        .register_blobs("worker-x:50081", &[other_digest]);

    // Query a digest that is not in the inner store and not in the
    // locality map at all.
    let query_digest = DigestInfo::try_new(VALID_HASH1, 100)?;
    let result = proxy.get_part_unchunked(query_digest, 0, None).await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    Ok(())
}

// -------------------------------------------------------------------
// 14. Overwrite a blob via update and verify new data is returned
// -------------------------------------------------------------------
#[nativelink_test]
async fn update_overwrites_existing_blob() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 5)?;

    proxy
        .update_oneshot(digest, Bytes::from_static(b"first"))
        .await?;

    let data = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(data.as_ref(), b"first");

    // Overwrite with new data (same digest key, different content for
    // MemoryStore which doesn't validate content hash).
    proxy
        .update_oneshot(digest, Bytes::from_static(b"secnd"))
        .await?;

    let data = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(data.as_ref(), b"secnd");

    Ok(())
}

// -------------------------------------------------------------------
// 15. Non-NotFound errors from inner store propagate directly
//     (no locality map fallback)
// -------------------------------------------------------------------
// Note: This is difficult to test without a custom mock store that
// returns a non-NotFound error. The inline tests cover this via the
// match arm in get_part(). We verify the pattern indirectly: a
// successful inner read never consults the locality map (test 1),
// and NotFound triggers the locality path (tests 2, 9, 10).

// -------------------------------------------------------------------
// 16. Large blob roundtrip through the proxy
// -------------------------------------------------------------------
#[nativelink_test]
async fn large_blob_roundtrip() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();

    // 1 MiB of repeated bytes
    let size: usize = 1024 * 1024;
    let value: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH1, size as u64)?;

    proxy
        .update_oneshot(digest, Bytes::from(value.clone()))
        .await?;

    let data = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(data.len(), size, "Returned blob size should match");
    assert_eq!(data.as_ref(), value.as_slice());

    Ok(())
}

// ===================================================================
// Gap 1: Successful peer proxy read — inject a MemoryStore as a peer
// ===================================================================

/// Helper: create a WorkerProxyStore and return the underlying Arc so we
/// can call inject_worker_connection().
fn make_proxy_store_with_arc() -> (Arc<WorkerProxyStore>, Store, SharedBlobLocalityMap) {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner.clone(), locality_map.clone());
    (proxy_arc, inner, locality_map)
}

// -------------------------------------------------------------------
// 17. Successful peer proxy read: inner miss, peer has the blob
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_proxies_from_injected_peer() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"data from the peer worker";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Create a "peer" MemoryStore and populate it with the blob.
    let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    // Inject the peer store as a worker connection.
    let peer_endpoint = "grpc://peer-worker:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);

    // Register the digest on the peer in the locality map.
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // The inner store is empty, so get_part should proxy from the peer.
    let result = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        value,
        "Expected blob data from the injected peer store"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 18. Peer proxy read with offset and length
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_proxies_from_peer_with_offset() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"0123456789abcdef"; // 16 bytes
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    let peer_endpoint = "grpc://peer-worker:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Read bytes [4..12) from the peer.
    let result = proxy.get_part_unchunked(digest, 4, Some(8)).await?;
    assert_eq!(
        result.as_ref(),
        b"456789ab",
        "Expected subset from peer at offset=4, length=8"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 19. Peer proxy: first peer doesn't have blob, second peer does
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_skips_peer_without_blob_and_reads_from_next() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"only on peer-b";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Peer A: empty store (has() returns None).
    let peer_a_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let peer_a_endpoint = "grpc://peer-a:50081";
    proxy_arc.inject_worker_connection(peer_a_endpoint, peer_a_store);

    // Peer B: has the blob.
    let peer_b_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_b_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_b_endpoint = "grpc://peer-b:50081";
    proxy_arc.inject_worker_connection(peer_b_endpoint, peer_b_store);

    // Register the digest on both peers.
    {
        let mut map = locality_map.write();
        map.register_blobs(peer_a_endpoint, &[digest]);
        map.register_blobs(peer_b_endpoint, &[digest]);
    }

    let result = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        value,
        "Expected data from peer-b after peer-a returned None for has()"
    );

    Ok(())
}

// ===================================================================
// Gap 2: Resume-from-offset — PartialFailStore + next peer
// ===================================================================

/// A store wrapper that delegates to an inner store but fails `get_part`
/// after writing a configured number of bytes. Used to test streaming
/// resume logic in WorkerProxyStore.
#[derive(Debug, MetricsComponent)]
struct PartialFailStore {
    inner: Store,
    /// Number of bytes to successfully write before returning an error.
    fail_after_bytes: u64,
}

default_health_status_indicator!(PartialFailStore);

#[async_trait]
impl StoreDriver for PartialFailStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Read the full blob from the inner store.
        let data = self.inner.get_part_unchunked(key.borrow(), offset, length).await?;

        // Write up to `fail_after_bytes` bytes, then return an error.
        let write_len = core::cmp::min(data.len() as u64, self.fail_after_bytes) as usize;
        if write_len > 0 {
            writer
                .send(data.slice(..write_len))
                .await
                .map_err(|e| make_err!(Code::Internal, "PartialFailStore write error: {e:?}"))?;
        }

        Err(make_err!(
            Code::Internal,
            "PartialFailStore: simulated failure after {} bytes",
            write_len
        ))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

// -------------------------------------------------------------------
// 20. Resume from offset: first peer fails mid-stream, second succeeds
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_resumes_from_next_peer_after_mid_stream_failure() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"0123456789abcdef"; // 16 bytes
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Peer A: a PartialFailStore that writes 5 bytes then fails.
    let peer_a_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_a_inner
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_a_store = Store::new(Arc::new(PartialFailStore {
        inner: peer_a_inner,
        fail_after_bytes: 5,
    }));
    let peer_a_endpoint = "grpc://peer-a:50081";
    proxy_arc.inject_worker_connection(peer_a_endpoint, peer_a_store);

    // Peer B: has the full blob (normal MemoryStore).
    let peer_b_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_b_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_b_endpoint = "grpc://peer-b:50081";
    proxy_arc.inject_worker_connection(peer_b_endpoint, peer_b_store);

    // Register the digest on both peers. The order in the locality map
    // determines which peer is tried first. We register A first.
    {
        let mut map = locality_map.write();
        map.register_blobs(peer_a_endpoint, &[digest]);
        map.register_blobs(peer_b_endpoint, &[digest]);
    }

    // The proxy should: try peer A, get 5 bytes, fail, then resume from
    // peer B at offset 5. The final result should be the complete blob.
    let result = proxy.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        value,
        "Expected complete blob after resume from second peer"
    );

    Ok(())
}

// ===================================================================
// Gap 3: IS_WORKER_REQUEST branching tests
// ===================================================================

// -------------------------------------------------------------------
// 21. IS_WORKER_REQUEST=true: inner miss + locality has peer
//     => FailedPrecondition redirect with peer endpoint
// -------------------------------------------------------------------
#[nativelink_test]
async fn worker_request_returns_redirect_with_peer_endpoints() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
    let peer_endpoint = "grpc://peer-worker:50071";

    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let result = IS_WORKER_REQUEST
        .scope(true, proxy.get_part_unchunked(digest, 0, None))
        .await;

    assert!(result.is_err(), "Expected redirect error for worker request");
    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::FailedPrecondition,
        "Redirect should use FailedPrecondition, got: {err:?}"
    );
    let msg = err.message_string();
    assert!(
        msg.contains(REDIRECT_PREFIX),
        "Error message should contain redirect prefix: {msg}"
    );
    assert!(
        msg.contains(peer_endpoint),
        "Error message should contain peer endpoint: {msg}"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 22. IS_WORKER_REQUEST=false: inner miss + locality has peer with
//     invalid URI => NotFound (proxy attempt fails gracefully)
// -------------------------------------------------------------------
#[nativelink_test]
async fn non_worker_request_returns_not_found_when_peer_unreachable() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();

    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // Invalid URI fails during create_worker_connection.
    locality_map
        .write()
        .register_blobs("not a valid uri", &[digest]);

    let result = IS_WORKER_REQUEST
        .scope(false, proxy.get_part_unchunked(digest, 0, None))
        .await;

    assert!(result.is_err(), "Expected NotFound error");
    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::NotFound,
        "Non-worker request should get NotFound, got: {err:?}"
    );

    Ok(())
}

// ===================================================================
// Gap 4: optimized_for tests
// ===================================================================

// -------------------------------------------------------------------
// 23. optimized_for(LazyExistenceOnSync) returns true
// -------------------------------------------------------------------
#[nativelink_test]
async fn optimized_for_lazy_existence_returns_true() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner, locality_map);

    assert!(
        StoreDriver::optimized_for(&*proxy, StoreOptimizations::LazyExistenceOnSync),
        "WorkerProxyStore should report LazyExistenceOnSync"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 24. optimized_for(other) delegates to inner store
// -------------------------------------------------------------------
#[nativelink_test]
async fn optimized_for_other_delegates_to_inner() -> Result<(), Error> {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner, locality_map);

    assert!(
        !StoreDriver::optimized_for(&*proxy, StoreOptimizations::NoopUpdates),
        "Should delegate non-LazyExistence optimizations to inner store"
    );

    Ok(())
}

// ===================================================================
// Gap 5: Cancellation-survives-requester / structured-error-preference
// (Sibling to populate_and_maybe_stream cancellation-drop fix in
// fast_slow_store.rs, commits 8674bc19 + 01b68015.)
// ===================================================================

/// A peer-store wrapper that writes a configurable number of bytes,
/// then drops the writer (without sending EOF) and returns a structured
/// upstream `Error` with a caller-chosen `Code` and message. This mimics
/// the production-observed pattern where a peer's `get_part` errors
/// mid-stream — closing its writer half without EOF — and the cache-tee
/// proxy in `WorkerProxyStore::get_part_and_cache` would surface the
/// generic `Code::Internal "Sender dropped before sending EOF"` instead
/// of the structured upstream code.
#[derive(Debug, MetricsComponent)]
struct StructuredFailStore {
    inner: Store,
    /// Bytes to write before erroring.
    fail_after_bytes: u64,
    /// Code to return after writing `fail_after_bytes`.
    err_code: Code,
    /// Sentinel substring that must appear in the surfaced error.
    err_marker: String,
}

default_health_status_indicator!(StructuredFailStore);

#[async_trait]
impl StoreDriver for StructuredFailStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let data = self
            .inner
            .get_part_unchunked(key.borrow(), offset, length)
            .await?;
        let write_len = core::cmp::min(data.len() as u64, self.fail_after_bytes) as usize;
        if write_len > 0 {
            writer
                .send(data.slice(..write_len))
                .await
                .map_err(|e| make_err!(Code::Internal, "StructuredFailStore send: {e:?}"))?;
        }
        // Return a structured error WITHOUT calling `writer.send_eof()`.
        // The caller's `proxy_rx.recv()` will see "Sender dropped before
        // sending EOF" — but the *true* failure cause is this Err which
        // must not be masked.
        Err(make_err!(self.err_code, "{}", self.err_marker))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

// -------------------------------------------------------------------
// 25. Mid-stream peer Code::Unavailable triggers connection drop
//
// When the peer's get_part errors mid-stream with `Code::Unavailable`,
// `WorkerProxyStore::try_read_from_worker` MUST classify it as a
// connection error and call `remove_worker_endpoint`. Before the fix,
// `get_part_and_cache` returned the generic `Code::Internal "Sender
// dropped before sending EOF"` from the forward path, which masked
// the upstream code and caused the connection drop check to miss.
// -------------------------------------------------------------------
#[nativelink_test]
async fn peer_unavailable_mid_stream_drops_cached_connection() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    // 16-byte blob, large enough to engage the cache-tee path
    // (offset=0, length=None, size <= 64MiB).
    let value = b"0123456789abcdef";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Single peer that writes 4 bytes, then errors with Code::Unavailable.
    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_store = Store::new(Arc::new(StructuredFailStore {
        inner: peer_inner,
        fail_after_bytes: 4,
        err_code: Code::Unavailable,
        err_marker: "STRUCTURED_PEER_UNAVAILABLE".to_string(),
    }));
    let peer_endpoint = "grpc://flaky-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Sanity: connection is in the pool before the call.
    assert!(
        proxy_arc.peer_stores().contains_key(&Arc::from(peer_endpoint)),
        "Expected injected connection to be present before fetch"
    );

    // The proxy should: try the only peer, get 4 bytes, see an
    // upstream Code::Unavailable, classify as connection error, remove
    // the cached connection. Final outer error is "cannot retry inner
    // store" because partial bytes were forwarded — that's expected
    // and unrelated to the structured-error-preference fix.
    let result = proxy.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "Expected error after the only peer failed");

    // Connection MUST have been removed because the surfaced error
    // code was Code::Unavailable. With the pre-fix forward-first
    // ordering, the surfaced code would have been Code::Internal
    // ("Sender dropped before sending EOF") and the connection would
    // have remained cached.
    assert!(
        !proxy_arc.peer_stores().contains_key(&Arc::from(peer_endpoint)),
        "Expected cached connection to be removed after upstream Unavailable; \
         peer_stores={:?}",
        proxy_arc.peer_stores().keys().collect::<Vec<_>>()
    );

    Ok(())
}

// -------------------------------------------------------------------
// 26. Mid-stream peer Code::DataLoss does NOT drop the connection
//
// The structured peer error must be visible to the locality eviction
// code path so it can decide to evict locality entries (always) but
// only drop the cached connection on Unavailable | Unknown. Before
// the fix, ALL peer errors were masked as Code::Internal so the
// connection-drop discrimination was impossible.
// -------------------------------------------------------------------
#[nativelink_test]
async fn peer_dataloss_mid_stream_keeps_cached_connection() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"0123456789abcdef";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_store = Store::new(Arc::new(StructuredFailStore {
        inner: peer_inner,
        fail_after_bytes: 4,
        err_code: Code::DataLoss,
        err_marker: "STRUCTURED_PEER_DATALOSS".to_string(),
    }));
    let peer_endpoint = "grpc://lossy-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let _unused = proxy.get_part_unchunked(digest, 0, None).await;

    // DataLoss is NOT a connection error, so the cached connection
    // remains. Locality entry IS evicted (always-on under 2fe4b1cb),
    // but the connection pool entry must persist.
    assert!(
        proxy_arc.peer_stores().contains_key(&Arc::from(peer_endpoint)),
        "Expected cached connection to remain after non-connection upstream error"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 27. Pre-EOF peer error (no bytes written) surfaces structured code
//
// When the peer's get_part errors before writing any bytes, no data
// reaches the caller's writer. With no bytes written, the outer
// "cannot retry inner store" guard does not trigger and the inner
// store retry path runs. The locality map and connection pool MUST
// reflect the structured peer code, not the generic forward artifact.
// -------------------------------------------------------------------
#[nativelink_test]
async fn peer_unavailable_pre_eof_drops_cached_connection() -> Result<(), Error> {
    let (proxy_arc, _inner, locality_map) = make_proxy_store_with_arc();
    let proxy = Store::new(proxy_arc.clone());

    let value = b"0123456789abcdef";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;
    let peer_store = Store::new(Arc::new(StructuredFailStore {
        inner: peer_inner,
        fail_after_bytes: 0, // no data forwarded before the error
        err_code: Code::Unavailable,
        err_marker: "STRUCTURED_PEER_PRE_EOF".to_string(),
    }));
    let peer_endpoint = "grpc://prefail-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let _unused = proxy.get_part_unchunked(digest, 0, None).await;

    assert!(
        !proxy_arc.peer_stores().contains_key(&Arc::from(peer_endpoint)),
        "Expected cached connection to be removed after pre-EOF Unavailable"
    );

    Ok(())
}

// ===================================================================
// Reviewer Finding 2 (testing-czar Gap 2): construction-site coverage
// for the REAPI v2 §2.2.4 PreconditionFailure detail attachment in
// `worker_proxy_store.rs:985` (worker request, no redirect path) and
// `worker_proxy_store.rs:1033` (inner store + all workers miss path).
// ===================================================================

/// Helper to assert that an error carries a single `PreconditionFailure`
/// MISSING violation whose `subject` is `"blobs/<hash>/<size>"`.
fn assert_precondition_failure_for_digest(err: &Error, digest: DigestInfo) {
    use prost::Message;
    use nativelink_util::common::PreconditionFailure;

    assert_eq!(
        err.details.len(),
        1,
        "expected exactly one PreconditionFailure detail, got {}: {err:?}",
        err.details.len(),
    );
    let detail = &err.details[0];
    assert!(
        detail.type_url.ends_with("PreconditionFailure"),
        "detail type_url should end with 'PreconditionFailure', got: {}",
        detail.type_url,
    );
    let pf = PreconditionFailure::decode(detail.value.as_slice())
        .expect("detail value must decode as PreconditionFailure");
    assert_eq!(pf.violations.len(), 1, "expected one violation");
    assert_eq!(pf.violations[0].r#type, "MISSING");
    let expected_subject = format!(
        "blobs/{}/{}",
        digest.packed_hash(),
        digest.size_bytes(),
    );
    assert_eq!(
        pf.violations[0].subject, expected_subject,
        "violation subject must be 'blobs/<hash>/<size>', got: {}",
        pf.violations[0].subject,
    );
}

/// Asserts that the worker-request NotFound path attaches a
/// `PreconditionFailure` detail with the missing digest as `subject`.
/// This is the path Bazel observes when a worker asks the server-side
/// proxy for a blob that neither the inner store nor any peer in the
/// locality map has — the server returns NotFound (instead of the
/// REDIRECT_PREFIX response that fires when locality has at least one
/// peer).
#[nativelink_test]
async fn worker_request_no_redirect_not_found_carries_precondition_detail() -> Result<(), Error> {
    let (proxy, _inner, _locality_map) = make_proxy_store();
    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // IS_WORKER_REQUEST=true with no locality entries → falls through
    // the redirect-or-NotFound branch into the NotFound construction.
    let result = IS_WORKER_REQUEST
        .scope(true, proxy.get_part_unchunked(digest, 0, None))
        .await;

    let err = result.err().expect("expected NotFound for worker request with no peers");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got: {err:?}");
    assert_precondition_failure_for_digest(&err, digest);
    Ok(())
}

/// Asserts that the "inner store + all workers miss" NotFound path at
/// `worker_proxy_store.rs:1033` attaches a `PreconditionFailure` detail.
/// This is the path Bazel observes when the proxy attempted peer
/// fetches that all failed AND the inner-store retry also returned
/// NotFound. Triggered by registering an unreachable peer in the
/// locality map (so worker fetch is attempted and fails) with
/// `IS_WORKER_REQUEST=false`.
#[nativelink_test]
async fn inner_store_and_all_workers_miss_carries_precondition_detail() -> Result<(), Error> {
    let (proxy, _inner, locality_map) = make_proxy_store();
    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;

    // Register a peer whose URI is invalid — peer attempt will fail
    // immediately during create_worker_connection, then the inner
    // store retry runs, sees nothing, and falls through to the line
    // 1033 construction site.
    locality_map
        .write()
        .register_blobs("not a valid uri", &[digest]);

    let result = IS_WORKER_REQUEST
        .scope(false, proxy.get_part_unchunked(digest, 0, None))
        .await;

    let err = result.err().expect("expected NotFound after all workers fail and inner misses");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got: {err:?}");
    assert_precondition_failure_for_digest(&err, digest);
    Ok(())
}

// ===================================================================
// Issue #117: bytestream Read RPCs that fail with non-NotFound codes
// (e.g. Code::Internal "writer dropped without sending EOF" produced
// by `StreamingBlobWriter::Drop` when a populator task is cancelled)
// MUST still consult peers. Pre-fix the gate in
// `WorkerProxyStore::get_part_sequential` only matched NotFound,
// so the bypass returned the inner-store error directly without
// invoking `try_read_from_worker` — even though the blob existed
// on a peer worker.
// ===================================================================

/// Helper: build a WorkerProxyStore wrapping an inner
/// `PartialWriteThenErrorStore` whose `get_part` errors immediately
/// (empty prefix → no bytes streamed) with `(err_code, err_marker)`.
/// Returns the proxy Arc, the proxy as a Store, and the locality map.
///
/// The unified `PartialWriteThenErrorStore` fake is defined below the
/// red-team Finding 4 tests; it covers both "error before any bytes" (when
/// `prefix.is_empty()`) and "error after partial bytes" (non-empty prefix).
fn make_proxy_with_failing_inner(
    err_code: Code,
    err_marker: &str,
) -> (Arc<WorkerProxyStore>, Store, SharedBlobLocalityMap) {
    let inner = Store::new(Arc::new(PartialWriteThenErrorStore {
        prefix: Bytes::new(),
        err_code,
        err_marker: err_marker.to_string(),
    }));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
    let proxy = Store::new(proxy_arc.clone());
    (proxy_arc, proxy, locality_map)
}

// -------------------------------------------------------------------
// 28. Table-driven: every code in `should_try_peers`'s allowlist must
//     fall through to peer-fetch when the inner store errors with that
//     code BEFORE writing any bytes. Production case for `Internal` is
//     `StreamingBlobWriter::Drop` ("writer dropped without sending EOF");
//     `DataLoss` is verifier hash-mismatch; `Unavailable` is inner-store
//     transient; `OutOfRange` is truncated stored blob; `Unknown` is
//     tonic mapping unrecognized HTTP/2 status; `ResourceExhausted` is
//     local capacity saturated.
//
// Mutation guard: change one row's code to a NON-fallthrough code (e.g.
// `Code::PermissionDenied`) — that row's iteration must fail because
// peer-fetch is not invoked and the inner-store error propagates.
// -------------------------------------------------------------------
#[nativelink_test]
async fn inner_fallthrough_codes_invoke_peer_fetch() -> Result<(), Error> {
    let cases: &[(Code, &str)] = &[
        (Code::Internal, "writer dropped without sending EOF"),
        (Code::DataLoss, "verify_store: hash mismatch"),
        (Code::Unavailable, "inner store transiently unavailable"),
        (Code::OutOfRange, "requested range exceeds blob length"),
        (Code::Unknown, "tonic: unmapped HTTP/2 status"),
        (Code::ResourceExhausted, "local connection cap reached"),
    ];

    for (code, marker) in cases {
        let (proxy_arc, proxy, locality_map) =
            make_proxy_with_failing_inner(*code, marker);

        let value = b"data only on the peer worker";
        let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;

        let peer_endpoint = "grpc://peer-worker:50081";
        proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
        locality_map
            .write()
            .register_blobs(peer_endpoint, &[digest]);

        let result = proxy.get_part_unchunked(digest, 0, None).await;
        let data = result.unwrap_or_else(|e| {
            panic!("Code::{code:?} ({marker}): expected peer fetch to succeed, got err: {e:?}")
        });
        assert_eq!(
            data.as_ref(),
            value,
            "Code::{code:?} ({marker}): peer-fetch returned wrong bytes",
        );
    }

    Ok(())
}

// -------------------------------------------------------------------
// 29. Code::PermissionDenied is NOT in the fallthrough allowlist.
//     Peer-fetch must NOT be invoked; the original authz refusal must
//     propagate to the caller untouched.
// -------------------------------------------------------------------
#[nativelink_test]
async fn inner_permission_denied_does_not_invoke_peer_fetch() -> Result<(), Error> {
    let (proxy_arc, proxy, locality_map) = make_proxy_with_failing_inner(
        Code::PermissionDenied,
        "INNER_PERMISSION_DENIED_MARKER",
    );

    let value = b"this peer data must NOT be returned";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    let peer_endpoint = "grpc://peer-worker:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let result = proxy.get_part_unchunked(digest, 0, None).await;
    let err = result.expect_err("Expected PermissionDenied to propagate");
    assert_eq!(
        err.code,
        Code::PermissionDenied,
        "PermissionDenied must propagate; peer-fetch must NOT be invoked. err={err:?}"
    );
    assert!(
        err.message_string().contains("INNER_PERMISSION_DENIED_MARKER"),
        "Expected the original inner-store error message to surface, got: {}",
        err.message_string()
    );

    Ok(())
}

// ===================================================================
// Sibling-bug coverage (testing-czar round 2): the racing-fetch path
// in `WorkerProxyStore::get_part` has two more NotFound construction
// sites that surface to Bazel without the REAPI v2 §2.2.4
// PreconditionFailure detail when *both* server and peer return an
// empty EOF for a non-zero digest:
//   - `worker_proxy_store.rs:1085` (await_peer_after_empty_server,
//     server's empty EOF wins the race; peer also empty)
//   - `worker_proxy_store.rs:1124` (await_server_after_empty_peer,
//     peer's empty EOF wins the race; server also empty)
//
// These are reachable in production whenever a stale-positive locality
// entry races a stale slow-store result. Both must attach the same
// MISSING violation detail so Bazel can re-upload.
// ===================================================================

/// A minimal `StoreDriver` whose `get_part` emits an empty EOF without
/// writing any chunks. Used to simulate the empty-EOF stale-positive
/// that `await_*_after_empty_*` is designed to handle. Each instance
/// can be optionally gated on an external `Notify` so the test can
/// pin which side wins the racing-fetch `tokio::select!`.
/// `has_with_results` reports the blob present so the racing path is
/// engaged on either side.
#[derive(Debug, MetricsComponent)]
struct EmptyEofStore {
    /// Reported size for any digest queried via `has_with_results`.
    declared_size: u64,
    /// Optional gate: if `Some`, `get_part` awaits a notification on
    /// this `Notify` before emitting EOF. Used to make the race
    /// outcome deterministic in the unit tests below.
    release: Option<Arc<tokio::sync::Notify>>,
}

impl EmptyEofStore {
    fn ungated(declared_size: u64) -> Arc<Self> {
        Arc::new(Self {
            declared_size,
            release: None,
        })
    }

    fn gated(declared_size: u64, gate: Arc<tokio::sync::Notify>) -> Arc<Self> {
        Arc::new(Self {
            declared_size,
            release: Some(gate),
        })
    }
}

default_health_status_indicator!(EmptyEofStore);

#[async_trait]
impl StoreDriver for EmptyEofStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut().take(digests.len()) {
            *slot = Some(self.declared_size);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unimplemented, "EmptyEofStore does not support update"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // If this side is gated (the "loser"), wait for the test to
        // notify after the proxy has had time to consume the winner's
        // EOF and enter the appropriate `await_*_after_empty_*` arm.
        if let Some(notify) = self.release.as_ref() {
            notify.notified().await;
        }
        writer
            .send_eof()
            .err_tip(|| "EmptyEofStore: send_eof failed")?;
        Ok(())
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

// ===================================================================
// Red-team Finding 4: partial-write corruption guard
//
// If the inner store streams K bytes successfully and THEN errors with
// a peer-fallback-eligible code (Internal/DataLoss/Unavailable/etc.),
// falling through to a peer fetch would write the FULL blob from the
// peer to the same writer — producing a corrupt prefix-from-inner +
// full-peer-copy stream. The bytes-written guard added in
// `get_part_sequential` MUST surface the original error instead.
// ===================================================================

/// A store whose `get_part` writes a configurable prefix of bytes from
/// a backing buffer, then returns a configurable error code/marker.
/// Differs from `PartialFailStore` (peer-side helper) by being keyed to
/// the inner-store path: it owns its data inline rather than delegating
/// to a child Store, so we can construct it without a separate inner
/// MemoryStore population step.
#[derive(Debug, MetricsComponent)]
struct PartialWriteThenErrorStore {
    /// Bytes to write to the writer before erroring.
    prefix: Bytes,
    err_code: Code,
    err_marker: String,
}

default_health_status_indicator!(PartialWriteThenErrorStore);

#[async_trait]
impl StoreDriver for PartialWriteThenErrorStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut() {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::Unimplemented,
            "PartialWriteThenErrorStore: update not supported"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        if !self.prefix.is_empty() {
            writer
                .send(self.prefix.clone())
                .await
                .map_err(|e| make_err!(Code::Internal, "PartialWriteThenErrorStore send: {e:?}"))?;
        }
        Err(make_err!(self.err_code, "{}", self.err_marker))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

/// Drives the proxy's racing fetch with one ungated side (the
/// "winner") and one gated side (the "loser"). Returns the final
/// proxy result. The loser stays blocked on `loser_gate` until the
/// caller fires it AFTER the proxy has had time to consume the
/// winner's EOF and enter the corresponding `await_*_after_empty_*`
/// arm — pinning the race outcome.
async fn drive_empty_eof_race(
    server_inner: Arc<EmptyEofStore>,
    peer_store: Arc<EmptyEofStore>,
    loser_gate: Arc<tokio::sync::Notify>,
    digest: DigestInfo,
    peer_endpoint: &str,
) -> Result<Bytes, Error> {
    let server_inner_store = Store::new(server_inner);
    let peer_store_handle = Store::new(peer_store);

    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(server_inner_store, locality_map.clone());
    proxy_arc.enable_race_peers();
    let proxy = Store::new(proxy_arc.clone());

    proxy_arc.inject_worker_connection(peer_endpoint, peer_store_handle);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Spawn the proxy call so the test task can fire the loser gate
    // concurrently. Yield several times first so the proxy task gets
    // scheduled, spawns its racer tasks, drains the winner's EOF, and
    // enters the relevant `await_*_after_empty_*` arm before the
    // loser produces its EOF.
    let handle = tokio::spawn(async move {
        proxy.get_part_unchunked(digest, 0, None).await
    });
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    loser_gate.notify_one();
    handle
        .await
        .map_err(|e| make_err!(Code::Internal, "test task join: {e}"))?
}

/// Asserts that when the server racer wins with an empty EOF and the
/// peer racer also produces an empty EOF, the resulting NotFound at
/// `worker_proxy_store.rs:1085` carries a `PreconditionFailure`
/// MISSING-violation detail keyed to the requested digest.
#[nativelink_test]
async fn await_peer_after_empty_server_carries_precondition_detail() -> Result<(), Error> {
    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
    let peer_gate = Arc::new(tokio::sync::Notify::new());

    let server_inner = EmptyEofStore::ungated(digest.size_bytes());
    let peer_store = EmptyEofStore::gated(digest.size_bytes(), peer_gate.clone());

    let result = drive_empty_eof_race(
        server_inner,
        peer_store,
        peer_gate,
        digest,
        "grpc://gated-peer:50081",
    )
    .await;
    let err = result
        .err()
        .expect("expected NotFound when both server and peer return empty EOF");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got: {err:?}");
    let msg = err.message_string();
    assert!(
        msg.contains("both server and peer"),
        "expected await_peer_after_empty_server message (line 1085 path), got: {msg}",
    );
    assert_precondition_failure_for_digest(&err, digest);
    Ok(())
}

/// Symmetric to the above: when the peer racer wins with an empty EOF
/// and the server racer also produces an empty EOF, the resulting
/// NotFound at `worker_proxy_store.rs:1124` must also carry the
/// `PreconditionFailure` MISSING-violation detail.
#[nativelink_test]
async fn await_server_after_empty_peer_carries_precondition_detail() -> Result<(), Error> {
    let digest = DigestInfo::try_new(VALID_HASH1, 100)?;
    let server_gate = Arc::new(tokio::sync::Notify::new());

    let server_inner = EmptyEofStore::gated(digest.size_bytes(), server_gate.clone());
    let peer_store = EmptyEofStore::ungated(digest.size_bytes());

    let result = drive_empty_eof_race(
        server_inner,
        peer_store,
        server_gate,
        digest,
        "grpc://winner-peer:50081",
    )
    .await;
    let err = result
        .err()
        .expect("expected NotFound when both peer and server return empty EOF");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got: {err:?}");
    let msg = err.message_string();
    assert!(
        msg.contains("both peer and server"),
        "expected await_server_after_empty_peer message (line 1124 path), got: {msg}",
    );
    assert_precondition_failure_for_digest(&err, digest);
    Ok(())
}

// -------------------------------------------------------------------
// 30. Table-driven: for EVERY code in `should_try_peers`'s allowlist,
//     if the inner store writes 5 bytes THEN errors with that code while
//     a peer holds the full 16-byte blob, the proxy MUST surface the
//     inner-store error (NOT silently produce 5 + 16 = 21 corrupt bytes).
//
// The corruption-guard logic in `get_part_sequential` is code-agnostic:
// any of the 6 fall-through codes (Internal, DataLoss, Unavailable,
// OutOfRange, Unknown, ResourceExhausted) can fire mid-stream and
// trigger the same partial-write hazard. Treating only `Internal` would
// leave the other 5 codes silently corrupting consumers.
//
// We assert (per code):
//   * caller gets `Err` with the original code (not Ok with mixed bytes),
//   * the surfaced error preserves the inner-store marker substring,
//   * `bytes_written` matches the inner's partial write (5), NOT the
//     5+16=21 "papered-over" shape.
//
// Mutation guard: comment out the `bytes_written_by_inner > 0` block in
// `get_part_sequential` — every row of this table must turn RED with
// `Ok(21 bytes)` instead of `Err`.
// -------------------------------------------------------------------
#[nativelink_test]
async fn inner_partial_write_then_error_is_not_papered_over_by_peer_fetch()
-> Result<(), Error> {
    // Every code in `should_try_peers`'s allowlist; all 6 must be
    // guarded — keeping the guard code-agnostic keeps the corruption
    // surface zero regardless of which code the inner store emits.
    let cases: &[(Code, &str)] = &[
        (Code::Internal, "INNER_PARTIAL_THEN_INTERNAL"),
        (Code::DataLoss, "INNER_PARTIAL_THEN_DATALOSS"),
        (Code::Unavailable, "INNER_PARTIAL_THEN_UNAVAILABLE"),
        (Code::OutOfRange, "INNER_PARTIAL_THEN_OUT_OF_RANGE"),
        (Code::Unknown, "INNER_PARTIAL_THEN_UNKNOWN"),
        (Code::ResourceExhausted, "INNER_PARTIAL_THEN_RESOURCE_EXHAUSTED"),
    ];

    // Inner partial-prefix length and peer blob length differ on
    // purpose — 5 vs 16 — so a "papered-over" failure mode would
    // produce a 21-byte corrupt stream that the assertion catches.
    const PARTIAL_PREFIX: &[u8] = b"AAAAA"; // 5 bytes
    let value: &[u8] = b"0123456789abcdef"; // 16 bytes
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    for (code, marker) in cases {
        // Inner: write 5 bytes "AAAAA" then error with `code`.
        let inner = Store::new(Arc::new(PartialWriteThenErrorStore {
            prefix: Bytes::from_static(PARTIAL_PREFIX),
            err_code: *code,
            err_marker: (*marker).to_string(),
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
        let proxy = Store::new(proxy_arc.clone());

        // Peer holds the full blob for the same digest.
        let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_store
            .update_oneshot(digest, Bytes::from_static(value))
            .await?;
        let peer_endpoint = "grpc://peer-worker:50081";
        proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
        locality_map
            .write()
            .register_blobs(peer_endpoint, &[digest]);

        let result = proxy.get_part_unchunked(digest, 0, None).await;
        let err = match result {
            Ok(data) => panic!(
                "Code::{code:?} ({marker}): expected Err (corruption guard), \
                 got Ok with {} bytes (papered-over shape would be 5+16=21)",
                data.len()
            ),
            Err(e) => e,
        };
        assert_eq!(
            err.code, *code,
            "Code::{code:?} ({marker}): expected inner-store code to surface, got: {err:?}"
        );
        let msg = err.message_string();
        assert!(
            msg.contains(*marker),
            "Code::{code:?} ({marker}): expected inner-store marker in surfaced error, got: {msg}"
        );
        // The error message also documents the partial-write shape so
        // operators can recognize the corruption-guard fire.
        assert!(
            msg.contains("5 bytes"),
            "Code::{code:?} ({marker}): expected guard to report bytes_written=5, got: {msg}"
        );
    }

    Ok(())
}

// -------------------------------------------------------------------
// 31. `Code::Aborted` must NOT invoke the peer-fetch path.
//     Aborted is reserved for AC ABA (write/write conflict) signalling
//     and is not a "blob can't be served" signal. The original error
//     must propagate untouched; peer-fetch must NOT be consulted (which
//     we verify by registering peer data that, if returned, would
//     overwrite the marker — and asserting it does not).
// -------------------------------------------------------------------
#[nativelink_test]
async fn inner_aborted_does_not_invoke_peer_fetch() -> Result<(), Error> {
    let (proxy_arc, proxy, locality_map) = make_proxy_with_failing_inner(
        Code::Aborted,
        "INNER_ABORTED_MARKER",
    );

    let value = b"this peer data must NOT be returned for Aborted";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    let peer_endpoint = "grpc://peer-worker:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let result = proxy.get_part_unchunked(digest, 0, None).await;
    let err = result.expect_err("Expected Aborted to propagate");
    assert_eq!(
        err.code,
        Code::Aborted,
        "Aborted must propagate; peer-fetch must NOT be invoked. err={err:?}"
    );
    assert!(
        err.message_string().contains("INNER_ABORTED_MARKER"),
        "Expected the original inner-store error message to surface, got: {}",
        err.message_string()
    );

    Ok(())
}

// -------------------------------------------------------------------
// 32. `Code::DeadlineExceeded` must NOT invoke the peer-fetch path.
//     The caller's deadline has already passed; spending more wall-clock
//     on peer RPCs against a stream the caller has stopped reading is
//     wasted work. The original DeadlineExceeded error must propagate
//     untouched, and the peer-registered bytes must NOT be returned.
// -------------------------------------------------------------------
#[nativelink_test]
async fn inner_deadline_exceeded_does_not_invoke_peer_fetch() -> Result<(), Error> {
    let (proxy_arc, proxy, locality_map) = make_proxy_with_failing_inner(
        Code::DeadlineExceeded,
        "INNER_DEADLINE_EXCEEDED_MARKER",
    );

    let value = b"this peer data must NOT be returned for DeadlineExceeded";
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    let peer_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_store
        .update_oneshot(digest, Bytes::from_static(value))
        .await?;

    let peer_endpoint = "grpc://peer-worker:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, peer_store);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    let result = proxy.get_part_unchunked(digest, 0, None).await;
    let err = result.expect_err("Expected DeadlineExceeded to propagate");
    assert_eq!(
        err.code,
        Code::DeadlineExceeded,
        "DeadlineExceeded must propagate; peer-fetch must NOT be invoked. err={err:?}"
    );
    assert!(
        err.message_string().contains("INNER_DEADLINE_EXCEEDED_MARKER"),
        "Expected the original inner-store error message to surface, got: {}",
        err.message_string()
    );

    Ok(())
}

/// Per-wrapper regression for testing-czar MAJOR-1 (#140 follow-up):
/// `WorkerProxyStore::mark_stable` MUST delegate to its inner store.
/// WorkerProxyStore wraps the worker-side fast tier in some
/// compositions; without delegation the BIS pipeline breaks at this
/// layer. Inner is a FastSlowStore so `mark_stable` lands in its
/// observable `stable_digests` queue.
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
    let locality_map = new_shared_blob_locality_map();
    let proxy = WorkerProxyStore::new(inner_fast_slow.clone(), locality_map);

    let digest = DigestInfo::new([0xCCu8; 32], 100);
    let outer = Store::new(proxy);
    outer.as_store_driver().mark_stable(&[digest]);

    let drained = inner_fast_slow.as_store_driver().drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "WorkerProxyStore::mark_stable must delegate to inner. Without \
         this delegation the BIS pipeline breaks at the worker proxy \
         layer and worker pins (durable under v2) leak. Drained: {drained:?}",
    );

    Ok(())
}

// =====================================================================
// #171 — inner-miss writer-termination race vs WorkerProxyStore peer-fetch
// =====================================================================

/// A peer-side store that delays before serving any chunk. Models a
/// real network peer that takes ~200ms to respond. The delay opens
/// the race window that production exhibits: the local
/// notify_waiters of `streaming_writer.send_error` always fires
/// first, so any writer-termination there propagates BEFORE the
/// peer's bytes can be delivered.
#[derive(Debug, MetricsComponent)]
struct DelayedPeerStore {
    inner: Store,
    delay: core::time::Duration,
}

default_health_status_indicator!(DelayedPeerStore);

#[async_trait]
impl StoreDriver for DelayedPeerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.inner.update(key, reader, upload_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // Wait BEFORE issuing the read so the local-NotFound notify
        // wins the race deterministically.
        tokio::time::sleep(self.delay).await;
        self.inner
            .get_part(key.borrow(), writer, offset, length)
            .await
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
}

/// Reproducer for the #171 inner-miss writer-termination race.
///
/// Production composition (server side):
///
/// ```text
///   bytestream tx
///     │
///     ▼
///   WorkerProxyStore  (race_peers=false)
///     │ inner.get_part(&mut tx, ...)
///     ▼
///   ExistenceCacheStore
///     │
///     ▼
///   VerifyStore (verify_size=true)  — when should_verify is FALSE
///     │ passes outer tx through unchanged
///     ▼
///   FastSlowStore  (fast=Memory, slow=Memory) — both empty
///     │
///     ▼ run_producer.head=NotFound → streaming_writer.send_error
///     │
///   guard.fail(producer_err)  ← terminates outer tx (THE BUG)
/// ```
///
/// The OUTER tx is terminated before WorkerProxyStore's
/// `get_part_sequential` can fall through to `try_read_from_worker`
/// → `get_part_and_cache`. The peer DOES return all bytes but
/// `writer.send(chunk)` fails with
/// `Code::Internal "Tried to send while stream is closed"`.
///
/// Production trace: ~38 events/sec since 2026-04-27 11:28 deploy.
///
/// To force the bug to surface even when `should_verify=true` (which
/// otherwise inserts a separate internal tx between the OUTER writer
/// and FastSlowStore), this test issues a partial read
/// (`offset=0, length=Some(_)`) so VerifyStore's `should_verify`
/// gate fails and FastSlowStore wraps the OUTER tx directly. This
/// matches the production scenario where the outer tx is fast_slow's
/// guard target.
#[nativelink_test]
async fn inner_miss_with_peer_fallback_does_not_lose_peer_bytes_to_writer_termination_race()
-> Result<(), Error> {
    use core::time::Duration;

    let value: Vec<u8> = (0..108_944u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH1, value.len() as u64)?;

    // Inner store chain mirrors production CAS:
    //   ExistenceCache → Verify(verify_size=true) → FastSlow(Memory, Memory)
    // All tiers EMPTY for the test digest.
    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    ));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        fast_slow,
    ));
    let existence_cache = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1_000_000,
                ..Default::default()
            }),
        },
        verify,
    ));

    // Wrap with WorkerProxyStore — the production outer layer.
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(existence_cache.clone(), locality_map.clone());
    let proxy = Store::new(proxy_arc.clone());

    // Peer holds the blob and serves with a 200ms artificial delay so
    // the local-NotFound notify always wins the race.
    let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    peer_inner
        .update_oneshot(digest, Bytes::from(value.clone()))
        .await?;
    let delayed_peer = Store::new(Arc::new(DelayedPeerStore {
        inner: peer_inner,
        delay: Duration::from_millis(200),
    }));

    let peer_endpoint = "grpc://delayed-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, delayed_peer);
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    // Read the blob with a length hint that pushes VerifyStore's
    // `should_verify` gate to false (length.is_some()), so VerifyStore
    // delegates directly to the inner store with the OUTER writer
    // (rather than inserting its own internal tx). This makes
    // FastSlowStore's `guard.fail(producer_err)` close the OUTER writer
    // — the exact production failure mode.
    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        proxy.get_part_unchunked(digest, 0, Some(value.len() as u64)),
    )
    .await
    .expect(
        "must not deadlock — inner-miss + peer-fetch with delayed peer \
         must deliver bytes within 5s; race-loser writer-termination \
         bug at fast_slow_store::populate (line ~3178) closed the outer \
         writer before the peer's bytes could be forwarded",
    );

    let bytes = timed.unwrap_or_else(|err| {
        panic!(
            "inner-miss + peer-fetch must succeed; race-loser writer-\
             termination bug at fast_slow_store::populate dropped the \
             peer's bytes: got Err {err:?} expected {} bytes from peer",
            value.len()
        )
    });

    assert_eq!(
        bytes.len(),
        value.len(),
        "inner-miss + peer-fetch with delayed peer must deliver bytes; \
         race-loser writer-termination bug at fast_slow_store::populate \
         dropped them: got {} vs expected {}",
        bytes.len(),
        value.len(),
    );
    assert_eq!(
        bytes.as_ref(),
        value.as_slice(),
        "peer bytes must round-trip identically through the outer \
         WorkerProxyStore writer; mismatched bytes indicate the writer \
         was partially terminated then re-opened",
    );

    Ok(())
}
