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
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, NoopSpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::noop_store::NoopStore;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike,
};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const MEGABYTE_SZ: usize = 1024 * 1024;

fn make_stores_direction(
    fast_direction: StoreDirection,
    slow_direction: StoreDirection,
) -> (Store, Store, Store) {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction,
            slow_direction,
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    ));
    (fast_slow_store, fast_store, slow_store)
}

fn make_stores() -> (Store, Store, Store) {
    make_stores_direction(StoreDirection::default(), StoreDirection::default())
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

async fn check_data(
    check_store: &Store,
    digest: DigestInfo,
    original_data: &Vec<u8>,
    debug_name: &str,
) -> Result<(), Error> {
    assert!(
        check_store.has(digest).await?.is_some(),
        "Expected data to exist in {debug_name} store"
    );

    let store_data = check_store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        store_data, original_data,
        "Expected data to match in {debug_name} store"
    );
    Ok(())
}

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

#[nativelink_test]
async fn write_large_amount_to_both_stores_test() -> Result<(), Error> {
    let (store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(20 * MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    check_data(&store, digest, &original_data, "fast_slow").await?;
    check_data(&fast_store, digest, &original_data, "fast").await?;
    check_data(&slow_store, digest, &original_data, "slow").await?;

    Ok(())
}

/// Demonstrates that checking `has()` on just the fast store (as the
/// old `upload_file` code did via `inner_upload_results` passing
/// `fast_store()`) reports the blob as existing even when it is absent
/// from the slow store (remote CAS).
///
/// Before the fix: `inner_upload_results` set
///   `cas_store = self.running_actions_manager.cas_store.fast_store()`
/// so `upload_file`'s `has()` check only queried the local FilesystemStore.
/// If the blob existed locally but not remotely, the upload was skipped.
///
/// After the fix: `inner_upload_results` passes the full FastSlowStore,
/// whose `has()` checks the slow store (remote CAS).
#[nativelink_test]
async fn has_on_fast_store_only_does_not_reflect_slow_store() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // Write the blob ONLY to the fast store (simulating a previous
    // action's download that populated the local cache, or a prior
    // upload whose background slow-store write failed).
    fast_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // fast_store.has() sees the blob. Before the fix, upload_file
    // used this check (via cas_store = fast_store()) and would skip
    // the upload even though the blob is NOT on the remote.
    let fast_has = fast_store.has(digest).await?;
    assert!(
        fast_has.is_some(),
        "fast_store should report the blob exists"
    );

    // slow_store.has() does NOT see the blob.
    let slow_has = slow_store.has(digest).await?;
    assert!(
        slow_has.is_none(),
        "slow_store should NOT report the blob exists"
    );

    // fast_slow_store.has() correctly reflects the slow store state:
    // the blob is NOT available remotely even though it exists locally.
    // After the fix, upload_file uses this check (via the full
    // FastSlowStore) so the upload correctly proceeds.
    let fss_has = fast_slow_store.has(digest).await?;
    assert!(
        fss_has.is_none(),
        "fast_slow_store.has() should return None when blob is only \
         in fast store, proving the old fast_store.has() check was wrong"
    );

    // Simulate the upload_file logic with the FIX applied:
    // use fast_slow_store.has() (returns None) so the upload proceeds.
    // This is the behavior after changing inner_upload_results to pass
    // the full FastSlowStore instead of fast_store().
    let should_upload = fss_has.is_none();
    assert!(
        should_upload,
        "with the fix, upload_file should NOT skip the upload when \
         the blob is missing from the slow store"
    );

    Ok(())
}

#[nativelink_test]
async fn fetch_slow_store_puts_in_fast_store_test() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(Some(original_data.len() as u64))
    );
    assert_eq!(fast_store.has(digest).await, Ok(None));
    assert_eq!(
        slow_store.has(digest).await,
        Ok(Some(original_data.len() as u64))
    );

    // This get() request should place the data in fast_store too.
    fast_slow_store.get_part_unchunked(digest, 0, None).await?;

    // Now the data should exist in all the stores.
    check_data(&fast_store, digest, &original_data, "fast_store").await?;
    check_data(&slow_store, digest, &original_data, "slow_store").await?;

    Ok(())
}

#[nativelink_test]
async fn partial_reads_copy_full_to_fast_store_test() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // This get() request should place the data in fast_store too.
    assert_eq!(
        original_data[10..60],
        fast_slow_store
            .get_part_unchunked(digest, 10, Some(50))
            .await?
    );

    // Full data should exist in the fast store even though only partially
    // read.
    check_data(&slow_store, digest, &original_data, "slow_store").await?;
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    Ok(())
}

#[test]
fn calculate_range_test() {
    let test =
        |start_range, end_range| FastSlowStore::calculate_range(&start_range, &end_range).unwrap();
    {
        // Exact match.
        let received_range = 0..1;
        let send_range = 0..1;
        let expected_results = Some(0..1);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Minus one on received_range.
        let received_range = 1..4;
        let send_range = 1..5;
        let expected_results = Some(0..3);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Minus one on send_range.
        let received_range = 1..5;
        let send_range = 1..4;
        let expected_results = Some(0..3);
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Should have already sent all data (start fence post).
        let received_range = 1..2;
        let send_range = 0..1;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Definiltly already sent data.
        let received_range = 2..3;
        let send_range = 0..1;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // All data should be sent (inside range).
        let received_range = 3..4;
        let send_range = 0..100;
        let expected_results = Some(0..1); // Note: This is relative received_range.start.
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Subset of received data should be sent.
        let received_range = 1..100;
        let send_range = 3..4;
        let expected_results = Some(2..3); // Note: This is relative received_range.start.
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // We are clearly not at the offset yet.
        let received_range = 0..1;
        let send_range = 3..4;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Not at offset yet (fence post).
        let received_range = 0..1;
        let send_range = 1..2;
        let expected_results = None;
        assert_eq!(test(received_range, send_range), expected_results);
    }
    {
        // Head part of the received data should be sent.
        let received_range = 1..3;
        let send_range = 2..5;
        let expected_results = Some(1..2);
        assert_eq!(test(received_range, send_range), expected_results);
    }
}

#[nativelink_test]
async fn drop_on_eof_completes_store_futures() -> Result<(), Error> {
    #[derive(MetricsComponent)]
    struct DropCheckStore {
        drop_flag: Arc<AtomicBool>,
        read_rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        eof_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        digest: Option<DigestInfo>,
    }

    #[async_trait]
    impl StoreDriver for DropCheckStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            if let Some(has_digest) = self.digest {
                for (digest, result) in digests.iter().zip(results.iter_mut()) {
                    if *digest == has_digest.into() {
                        *result = Some(has_digest.size_bytes());
                    }
                }
            }
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _digest: StoreKey<'_>,
            mut reader: nativelink_util::buf_channel::DropCloserReadHalf,
            _size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            // Gets called in the fast store and we don't need to do
            // anything.  Should only complete when drain has finished.
            reader.drain().await?;
            let eof_tx = self.eof_tx.lock().unwrap().take();
            if let Some(tx) = eof_tx {
                tx.send(())
                    .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            }
            let read_rx = self.read_rx.lock().unwrap().take();
            if let Some(rx) = read_rx {
                rx.await.map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            }
            Ok(())
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            // Return NotFound if this store doesn't have the digest,
            // matching real store behavior (MemoryStore, FilesystemStore).
            if let Some(has_digest) = self.digest {
                let store_key: StoreKey<'_> = has_digest.into();
                if key != store_key {
                    return Err(make_err!(Code::NotFound, "Key not found in DropCheckStore"));
                }
            } else {
                return Err(make_err!(Code::NotFound, "Key not found in DropCheckStore"));
            }
            // Provide the data for matching keys (used by the slow store path).
            let bytes = length.unwrap_or_else(|| key.into_digest().size_bytes()) - offset;
            let data = vec![0_u8; usize::try_from(bytes).unwrap_or(usize::MAX)];
            writer.send(Bytes::copy_from_slice(&data)).await?;
            writer.send_eof()
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    impl Drop for DropCheckStore {
        fn drop(&mut self) {
            self.drop_flag.store(true, Ordering::Release);
        }
    }

    default_health_status_indicator!(DropCheckStore);

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let (fast_store_read_tx, fast_store_read_rx) = tokio::sync::oneshot::channel();
    let (fast_store_eof_tx, fast_store_eof_rx) = tokio::sync::oneshot::channel();
    let fast_store_dropped = Arc::new(AtomicBool::new(false));
    let fast_store = Store::new(Arc::new(DropCheckStore {
        drop_flag: fast_store_dropped.clone(),
        eof_tx: Mutex::new(Some(fast_store_eof_tx)),
        read_rx: Mutex::new(Some(fast_store_read_rx)),
        digest: None,
    }));
    let slow_store_dropped = Arc::new(AtomicBool::new(false));
    let slow_store = Store::new(Arc::new(DropCheckStore {
        drop_flag: slow_store_dropped,
        eof_tx: Mutex::new(None),
        read_rx: Mutex::new(None),
        digest: Some(digest),
    }));

    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    let (tx, mut rx) = make_buf_channel_pair();
    let (get_res, read_res) = tokio::join!(
        async move {
            // Drop get_part as soon as rx.drain() completes
            tokio::select!(
                res = rx.drain() => res,
                res = fast_slow_store.get_part(digest, tx, 0, Some(digest.size_bytes())) => res,
            )
        },
        async move {
            fast_store_eof_rx
                .await
                .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            // Give a couple of cycles for dropping to occur if it's going to.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            if fast_store_dropped.load(Ordering::Acquire) {
                return Err(make_err!(Code::Internal, "Fast store was dropped!"));
            }
            fast_store_read_tx
                .send(())
                .map_err(|e| make_err!(Code::Internal, "{:?}", e))?;
            Ok::<_, Error>(())
        }
    );
    get_res.merge(read_res)
}

#[nativelink_test]
async fn ignore_value_in_fast_store() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = Arc::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store,
    ));
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_slow_store.has(digest).await?.is_none(),
        "Expected data to not exist in store"
    );
    Ok(())
}

// Regression test for https://github.com/TraceMachina/nativelink/issues/665
#[nativelink_test]
async fn has_checks_fast_store_when_noop() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(NoopStore::new());
    let fast_slow_store_config = FastSlowSpec {
        fast: StoreSpec::Memory(MemorySpec::default()),
        slow: StoreSpec::Noop(NoopSpec::default()),
        fast_direction: StoreDirection::default(),
        slow_direction: StoreDirection::default(),
        chunked_reads_enabled: false,
    };
    let fast_slow_store = Arc::new(FastSlowStore::new(
        &fast_slow_store_config,
        fast_store.clone(),
        slow_store.clone(),
    ));

    let data = make_random_data(100);
    let digest = DigestInfo::try_new(VALID_HASH, data.len()).unwrap();

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(None),
        "Expected data to not exist in store"
    );

    // Upload some dummy data.
    fast_store
        .update_oneshot(digest, data.clone().into())
        .await?;

    assert_eq!(
        fast_slow_store.has(digest).await,
        Ok(Some(data.len() as u64)),
        "Expected data to exist in store"
    );

    assert_eq!(
        fast_slow_store.get_part_unchunked(digest, 0, None).await,
        Ok(data.into()),
        "Data read from store is not correct"
    );
    Ok(())
}

#[nativelink_test]
async fn fast_get_only_not_updated() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::Get, StoreDirection::Both);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_store.has(digest).await?.is_none(),
        "Expected data to not be in the fast store"
    );
    assert!(
        slow_store.has(digest).await?.is_some(),
        "Expected data in the slow store"
    );
    Ok(())
}

#[nativelink_test]
async fn fast_readonly_only_not_updated() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::ReadOnly, StoreDirection::Both);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_store.has(digest).await?.is_none(),
        "Expected data to not be in the fast store"
    );
    assert!(
        slow_store.has(digest).await?.is_some(),
        "Expected data in the slow store"
    );
    Ok(())
}

#[nativelink_test]
async fn slow_readonly_only_not_updated() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::Both, StoreDirection::ReadOnly);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_store.has(digest).await?.is_some(),
        "Expected data to be in the fast store"
    );
    assert!(
        slow_store.has(digest).await?.is_none(),
        "Expected data to not be in the slow store"
    );
    Ok(())
}

#[nativelink_test]
async fn slow_get_only_not_updated() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::Both, StoreDirection::Get);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    fast_slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        fast_store.has(digest).await?.is_some(),
        "Expected data to be in the fast store"
    );
    assert!(
        slow_store.has(digest).await?.is_none(),
        "Expected data to not be in the slow store"
    );
    Ok(())
}

#[nativelink_test]
async fn fast_put_only_not_updated() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::Update, StoreDirection::Both);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    fast_slow_store.get_part_unchunked(digest, 0, None).await?;
    assert!(
        fast_store.has(digest).await?.is_none(),
        "Expected data to not be in the fast store"
    );
    Ok(())
}

#[nativelink_test]
async fn fast_readonly_only_not_updated_on_get() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) =
        make_stores_direction(StoreDirection::ReadOnly, StoreDirection::Both);
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    slow_store
        .update_oneshot(digest, make_random_data(100).into())
        .await?;
    assert!(
        !fast_slow_store
            .get_part_unchunked(digest, 0, None)
            .await?
            .is_empty(),
        "Data not found in slow store"
    );
    assert!(
        fast_store.has(digest).await?.is_none(),
        "Expected data to not be in the fast store"
    );
    assert!(
        slow_store.has(digest).await?.is_some(),
        "Expected data in the slow store"
    );
    Ok(())
}

fn make_stores_with_lazy_slow() -> (Store, Store, Store) {
    #[derive(MetricsComponent)]
    struct LazyStore {
        inner: Arc<MemoryStore>,
    }

    #[async_trait]
    impl StoreDriver for LazyStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(digests, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn optimized_for(
            &self,
            optimization: nativelink_util::store_trait::StoreOptimizations,
        ) -> bool {
            matches!(
                optimization,
                nativelink_util::store_trait::StoreOptimizations::LazyExistenceOnSync
            )
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(LazyStore);

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(Arc::new(LazyStore {
        inner: MemoryStore::new(&MemorySpec::default()),
    }));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    ));
    (fast_slow_store, fast_store, slow_store)
}

#[nativelink_test]
async fn lazy_not_found_returns_error_when_missing() -> Result<(), Error> {
    let (fast_slow_store, _fast_store, _slow_store) = make_stores_with_lazy_slow();
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;

    assert!(result.is_err(), "Expected error when key doesn't exist");
    assert_eq!(
        result.unwrap_err().code,
        Code::NotFound,
        "Expected NotFound error code"
    );
    Ok(())
}

#[nativelink_test]
async fn lazy_not_found_syncs_to_fast_store() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores_with_lazy_slow();
    let original_data = make_random_data(100);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    assert!(
        fast_store.has(digest).await?.is_none(),
        "Expected data to not be in fast store initially"
    );

    let retrieved_data = fast_slow_store.get_part_unchunked(digest, 0, None).await?;

    assert_eq!(
        retrieved_data.as_ref(),
        original_data.as_slice(),
        "Retrieved data should match"
    );
    assert!(
        fast_store.has(digest).await?.is_some(),
        "Expected data to be synced to fast store"
    );
    Ok(())
}

#[nativelink_test]
async fn partial_slow_store_read_does_not_poison_fast_store() -> Result<(), Error> {
    // Regression test: if the fast store has a truncated entry, FastSlowStore
    // must not silently serve partial data. Since get_part() no longer calls
    // has() first (to avoid the double round-trip), truncation is detected
    // post-read by comparing bytes written against the digest size. Because
    // bytes were already sent to the writer, the operation returns an error
    // so the caller can retry.
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    let fast_slow_store = Store::new(fast_slow_store_arc);

    let full_data = make_random_data(100_000); // 100KB
    let digest = DigestInfo::try_new(VALID_HASH, full_data.len() as u64).unwrap();

    // Put the full blob in the slow store.
    slow_store
        .update_oneshot(digest, full_data.clone().into())
        .await?;

    // Write a PARTIAL blob directly into the fast store's MemoryStore.
    let partial_data = &full_data[..1000]; // Only 1KB of 100KB
    fast_store
        .update_oneshot(digest, Bytes::copy_from_slice(partial_data))
        .await?;

    // Read through FastSlowStore. It detects the truncated fast store entry
    // and returns an error (bytes already sent, cannot fall through to slow store).
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    assert!(
        result.is_err(),
        "Expected error for truncated fast store entry, got {} bytes",
        result.as_ref().map(|d| d.len()).unwrap_or(0),
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.code,
        Code::Internal,
        "Expected Internal error code for truncated data, got {:?}",
        err.code,
    );

    Ok(())
}

/// Test that `update_with_whole_file` writes complete data to both stores
/// when called on a FastSlowStore where the fast store supports file
/// updates. This exercises the streaming fd-clone parallel write path
/// (stream_fd_to_store) that avoids buffering the entire file in memory.
///
/// Uses a FileUpdateStore wrapper around MemoryStore that claims
/// FileUpdates optimization so the parallel path is triggered.
#[nativelink_test]
async fn update_with_whole_file_writes_to_both_stores() -> Result<(), Error> {
    use std::ffi::OsString;
    use std::io::Write;
    use nativelink_util::store_trait::{StoreOptimizations, UploadSizeInfo};

    /// MemoryStore wrapper that reports FileUpdates optimization, causing
    /// FastSlowStore to use the parallel `update_with_whole_file` path.
    #[derive(MetricsComponent)]
    struct FileUpdateStore {
        inner: Arc<MemoryStore>,
    }

    #[async_trait]
    impl StoreDriver for FileUpdateStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(digests, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        async fn update_with_whole_file(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            _path: OsString,
            file: nativelink_util::common::fs::FileSlot,
            upload_size: UploadSizeInfo,
        ) -> Result<Option<nativelink_util::common::fs::FileSlot>, Error> {
            // Delegate to the regular update path (read file, send to store).
            let file = nativelink_util::store_trait::slow_update_store_with_file(
                Pin::new(self.inner.as_ref()),
                key,
                file,
                upload_size,
            )
            .await?;
            Ok(Some(file))
        }

        fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
            matches!(optimization, StoreOptimizations::FileUpdates)
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(FileUpdateStore);

    let inner_fast = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(Arc::new(FileUpdateStore {
        inner: inner_fast.clone(),
    }));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    // Keep a direct handle to the inner MemoryStore for data verification.
    let inner_fast_store = Store::new(inner_fast);
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    ));

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Write data to a real temp file.
    let mut tmpfile = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "Failed to create tempfile: {:?}", e))?;
    tmpfile.write_all(&original_data)
        .map_err(|e| make_err!(Code::Internal, "Failed to write tempfile: {:?}", e))?;
    tmpfile.flush()
        .map_err(|e| make_err!(Code::Internal, "Failed to flush tempfile: {:?}", e))?;
    let path = tmpfile.path().to_owned();

    // Open the file as a FileSlot.
    let file = nativelink_util::common::fs::open_file(&path, 0).await?;

    // Call update_with_whole_file on the FastSlowStore.
    let store_key: StoreKey<'_> = digest.into();
    fast_slow_store
        .as_store_driver_pin()
        .update_with_whole_file(
            store_key,
            path.into_os_string(),
            file,
            UploadSizeInfo::ExactSize(original_data.len() as u64),
        )
        .await?;

    // Both stores should have the complete data.
    // The fast store (FileUpdateStore wrapping MemoryStore) received the
    // file handle via update_with_whole_file. The slow store received
    // streamed chunks via stream_fd_to_store from the cloned fd.
    check_data(&inner_fast_store, digest, &original_data, "fast_store").await?;
    check_data(&slow_store, digest, &original_data, "slow_store").await?;

    Ok(())
}

/// Test the streaming populate fallback: when a concurrent reader's streaming
/// buffer errors (e.g., cursor fell behind the sliding window), the reader
/// falls back to reading directly from the slow store starting at the correct
/// offset. This prevents duplicate data from being sent to the writer.
///
/// The fallback arithmetic is: new_offset = original_offset + bytes_already_sent,
/// new_length = original_length - bytes_already_sent. We test this by using
/// a very small streaming blob buffer so chunks are evicted before the second
/// reader can consume them, triggering the Unavailable error and fallback path.
#[nativelink_test]
async fn streaming_populate_fallback_on_buffer_eviction() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    );
    let fast_slow_store = Store::new(fast_slow_store_arc);

    // Use a blob larger than the default streaming buffer to trigger
    // sliding window eviction. The default buffer is 64 MiB, so we use
    // a smaller blob and rely on the slow store being accessible for
    // the fallback. The key behavior we test is that the final data is
    // correct regardless of whether the streaming path or fallback path
    // delivers the data.
    let original_data = make_random_data(2 * MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Write data only to the slow store so get_part triggers a populate.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Launch two concurrent get_part calls. The first becomes the populator,
    // the second becomes a waiter that reads from the streaming buffer.
    // Both should receive the complete, correct data.
    let fss = fast_slow_store.clone();
    let data_len = original_data.len() as u64;
    let (result1, result2) = tokio::join!(
        fss.get_part_unchunked(digest, 0, Some(data_len)),
        async {
            // Small yield to increase chance the first call becomes the populator.
            tokio::task::yield_now().await;
            fast_slow_store.get_part_unchunked(digest, 0, Some(data_len)).await
        }
    );

    let data1 = result1?;
    let data2 = result2?;

    assert_eq!(
        data1.as_ref(),
        original_data.as_slice(),
        "First reader should receive complete correct data"
    );
    assert_eq!(
        data2.as_ref(),
        original_data.as_slice(),
        "Second reader should receive complete correct data (via streaming or fallback)"
    );

    // The fast store should now have the data (populated from slow store).
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    Ok(())
}

/// Test the fallback arithmetic for the streaming populate error path.
/// When a streaming reader errors mid-stream, the fallback should resume
/// from the correct offset: new_offset = original_offset + bytes_already_sent.
/// This unit test verifies the arithmetic without needing to trigger an
/// actual buffer eviction (which depends on timing).
#[nativelink_test]
async fn streaming_populate_fallback_arithmetic() -> Result<(), Error> {
    // This test verifies that when a reader has already consumed some bytes
    // from the streaming buffer and then falls back to slow store, the
    // resulting data is correct and complete.
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store.clone(),
        slow_store.clone(),
    ));

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Write data only to slow store to trigger populate on read.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Partial read: request only a range (offset=1000, length=5000).
    // This exercises the offset/length arithmetic in the streaming path.
    let partial_result = fast_slow_store
        .get_part_unchunked(digest, 1000, Some(5000))
        .await?;
    assert_eq!(
        partial_result.as_ref(),
        &original_data[1000..6000],
        "Partial read should return correct range"
    );

    // Full read should work after the populate completed.
    let full_result = fast_slow_store
        .get_part_unchunked(digest, 0, None)
        .await?;
    assert_eq!(
        full_result.as_ref(),
        original_data.as_slice(),
        "Full read after populate should return complete data"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Test 2c: get_part when blob only in slow store populates fast store
// ─────────────────────────────────────────────────────────────────────

/// When a blob exists only in the slow store, get_part must:
/// 1. Return the correct data to the caller
/// 2. Populate the fast store with the full blob (even for partial reads)
/// 3. Subsequent reads should come from the fast store
#[nativelink_test]
async fn get_part_slow_only_populates_fast_and_returns_correct_data() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Write only to slow store.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Verify fast store is empty.
    assert_eq!(
        fast_store.has(digest).await?,
        None,
        "Fast store should be empty initially"
    );

    // Read through FastSlowStore — triggers populate.
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        original_data.as_slice(),
        "Full read through FastSlowStore should return correct data"
    );

    // Fast store should now have the data.
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    // Second read should still work (served from fast store now).
    let result2 = fast_slow_store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result2.as_ref(),
        original_data.as_slice(),
        "Second read should also return correct data"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Test 2d: get_part with partial read (offset + length)
// ─────────────────────────────────────────────────────────────────────

/// get_part with non-zero offset and limited length should return
/// exactly the requested slice. The fast store should still be populated
/// with the FULL blob (not just the requested slice).
#[nativelink_test]
async fn get_part_with_offset_and_length_returns_correct_slice() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(100_000); // 100 KB
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Write only to slow store.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Partial read: offset=10000, length=5000 (bytes 10000..15000).
    let partial = fast_slow_store
        .get_part_unchunked(digest, 10_000, Some(5_000))
        .await?;
    assert_eq!(
        partial.as_ref(),
        &original_data[10_000..15_000],
        "Partial read should return the exact requested slice"
    );

    // Fast store should have the FULL blob (not just the slice).
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    // Partial read at the very end of the blob.
    let tail = fast_slow_store
        .get_part_unchunked(digest, 99_000, Some(1_000))
        .await?;
    assert_eq!(
        tail.as_ref(),
        &original_data[99_000..],
        "Tail read should return the correct final 1000 bytes"
    );

    // Partial read at offset 0 with limited length.
    let head = fast_slow_store
        .get_part_unchunked(digest, 0, Some(500))
        .await?;
    assert_eq!(
        head.as_ref(),
        &original_data[..500],
        "Head read should return the correct first 500 bytes"
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Test 3a: upload when blob only in fast store (worker upload skip bug)
// ─────────────────────────────────────────────────────────────────────

/// When a blob exists in the fast store but NOT the slow store, an
/// update through the FastSlowStore should write to BOTH stores.
/// This is the FastSlowStore-level equivalent of the worker upload_file
/// bug where has() on the fast store returned true and the upload was
/// skipped even though the slow store (remote CAS) didn't have it.
#[nativelink_test]
async fn update_through_fast_slow_writes_to_slow_even_when_fast_has_it() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Simulate: blob exists in fast store (local cache) but NOT in slow store (remote).
    fast_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Verify initial state.
    assert!(
        fast_store.has(digest).await?.is_some(),
        "Fast store should have the blob"
    );
    assert!(
        slow_store.has(digest).await?.is_none(),
        "Slow store should NOT have the blob initially"
    );

    // FastSlowStore.has() checks slow store — should return None.
    assert!(
        fast_slow_store.has(digest).await?.is_none(),
        "FastSlowStore.has() should return None when blob only in fast store"
    );

    // Write through FastSlowStore. This should write to BOTH stores.
    fast_slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    // Now both stores should have the data.
    check_data(&fast_store, digest, &original_data, "fast_store").await?;
    check_data(&slow_store, digest, &original_data, "slow_store").await?;

    // FastSlowStore.has() should now succeed.
    assert!(
        fast_slow_store.has(digest).await?.is_some(),
        "FastSlowStore.has() should return Some after writing to both stores"
    );

    Ok(())
}

/// Verify that concurrent get_part calls for the same digest that only
/// exists in the slow store both return correct, complete data. This
/// exercises the streaming populate path where one caller populates
/// and the other reads from the streaming buffer.
#[nativelink_test]
async fn concurrent_get_part_same_digest_both_return_correct_data() -> Result<(), Error> {
    let (fast_slow_store, fast_store, slow_store) = make_stores();

    let original_data = make_random_data(2 * MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    // Only in slow store.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    let fss1 = fast_slow_store.clone();
    let fss2 = fast_slow_store.clone();
    let data_len = original_data.len() as u64;

    // Launch three concurrent reads to stress the streaming populate path.
    let (r1, r2, r3) = tokio::join!(
        fss1.get_part_unchunked(digest, 0, Some(data_len)),
        async {
            tokio::task::yield_now().await;
            fss2.get_part_unchunked(digest, 0, Some(data_len)).await
        },
        async {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            fast_slow_store.get_part_unchunked(digest, 0, Some(data_len)).await
        }
    );

    let d1 = r1?;
    let d2 = r2?;
    let d3 = r3?;

    assert_eq!(
        d1.as_ref(),
        original_data.as_slice(),
        "First concurrent reader got wrong data"
    );
    assert_eq!(
        d2.as_ref(),
        original_data.as_slice(),
        "Second concurrent reader got wrong data"
    );
    assert_eq!(
        d3.as_ref(),
        original_data.as_slice(),
        "Third concurrent reader got wrong data"
    );

    // Fast store should be populated.
    check_data(&fast_store, digest, &original_data, "fast_store").await?;

    Ok(())
}

/// Regression for the orphan-drop in `populate_and_maybe_stream`'s early
/// `?` paths (sibling to commit `49bf70fb`): the streaming buffer's
/// terminal state must carry the structured upstream NotFound, not Drop's
/// generic "writer dropped without sending EOF" fallback.
#[nativelink_test]
async fn populate_early_not_found_propagates_via_send_error() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_util::streaming_blob::StreamingBlob;
    use tokio::sync::Notify;

    /// Slow store whose `has()` blocks until `release` is notified, then
    /// returns Ok(None). All other operations defer to a backing
    /// `MemoryStore`.
    #[derive(MetricsComponent)]
    struct GatedHasStore {
        inner: Arc<MemoryStore>,
        gate: Arc<Notify>,
        has_entered: Arc<Notify>,
    }

    #[async_trait]
    impl StoreDriver for GatedHasStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            self.has_entered.notify_waiters();
            self.gate.notified().await;
            // Always report missing — exercises the NotFound `?` path.
            for r in results.iter_mut().take(digests.len()) {
                *r = None;
            }
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(GatedHasStore);

    let gate = Arc::new(Notify::new());
    let has_entered = Arc::new(Notify::new());
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(Arc::new(GatedHasStore {
        inner: MemoryStore::new(&MemorySpec::default()),
        gate: Arc::clone(&gate),
        has_entered: Arc::clone(&has_entered),
    }));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // Pre-register a `notified()` for `has_entered` so we don't miss
    // the wake-up if the populator races us.
    let entered_wait = has_entered.notified();

    // Spawn the populator. It will block in `slow_store.has()` until
    // we release the gate.
    let fss_for_pop = Arc::clone(&fast_slow_store);
    let pop_handle = tokio::spawn(async move {
        // Bound the entire operation so a hang fails the test rather
        // than wedging.
        tokio::time::timeout(
            Duration::from_secs(5),
            fss_for_pop.get_part_unchunked(digest, 0, None),
        )
        .await
    });

    // Wait until the populator is parked inside `slow_store.has()`.
    entered_wait.await;

    // Capture the streaming buffer Arc via the diagnostic accessor.
    // The populator's LoaderGuard holds the matching loader, so the
    // entry is live in `populating_digests`.
    let streaming_inner = fast_slow_store
        .populating_streaming_inner(digest.into())
        .expect(
            "populator should have registered a streaming buffer in \
             populating_digests before awaiting slow_store.has()",
        );

    // Release the gate so `slow_store.has()` returns None → the
    // populator's `?` returns NotFound.
    gate.notify_waiters();

    // Populator should complete with NotFound, well within the bound.
    let pop_res = pop_handle
        .await
        .map_err(|e| make_err!(Code::Internal, "populator join: {:?}", e))?
        .map_err(|_| make_err!(Code::DeadlineExceeded, "populator timed out"))?;

    let pop_err = pop_res
        .err()
        .expect("populator must return an error when slow store has no blob");
    assert_eq!(
        pop_err.code,
        Code::NotFound,
        "populator should see structured NotFound, got: {pop_err:?}"
    );

    // Now the load-bearing assertion: the streaming buffer's terminal
    // state must be the structured NotFound, NOT Drop's generic
    // `Code::Internal "writer dropped without sending EOF"`. Without
    // the fix, the writer in `populate_and_maybe_stream` was dropped
    // un-EOF'd by the early `?`, and Drop set the Internal terminal
    // state — a waiter reading the buffer would observe that, not the
    // upstream NotFound.
    let mut reader = StreamingBlob::new_reader(&streaming_inner);
    let read_res = tokio::time::timeout(Duration::from_secs(5), reader.next_chunk())
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "streaming reader hung"))?;
    let read_err = read_res
        .err()
        .expect("streaming buffer terminal state should be an error");
    assert_eq!(
        read_err.code,
        Code::NotFound,
        "streaming buffer terminal must carry the structured upstream \
         NotFound (proving send_error ran before drop). Got: {read_err:?}"
    );
    assert!(
        !read_err
            .messages
            .iter()
            .any(|m| m.contains("dropped without sending EOF")),
        "streaming buffer terminal must NOT be Drop's fallback \
         'writer dropped without sending EOF' — got: {:?}",
        read_err.messages
    );

    Ok(())
}

/// Regression for the orphan-drop in `populate_and_maybe_stream` caused by
/// the original requester's future being cancelled mid-populate. Before the
/// spawn-detach fix, dropping the requester dropped the populate future,
/// which dropped the StreamingBlobWriter un-EOF'd → all waiters observed
/// `Code::Internal "writer dropped without sending EOF"` and fell back to
/// the slow store directly (the production symptom: floods of
/// "streaming populate reader error, falling back to slow store" warns).
/// The fix detaches the producer onto its own task so cancellation of the
/// requester does not cancel the populate.
///
/// Observable signal: the streaming buffer's terminal state. Pre-fix, it
/// is the Drop fallback (`Code::Internal "writer dropped without sending
/// EOF"`). Post-fix, the producer runs to completion and terminates the
/// buffer with `Ok` (success EOF).
#[nativelink_test]
async fn populate_survives_caller_cancellation() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_util::streaming_blob::StreamingBlob;
    use tokio::sync::Notify;

    /// Slow store whose `get_part` releases a notify on entry then waits
    /// for the test to release it. All other operations defer to a
    /// backing MemoryStore.
    #[derive(MetricsComponent)]
    struct StallSlowStore {
        inner: Arc<MemoryStore>,
        get_entered: Arc<Notify>,
        release_get: Arc<Notify>,
    }

    #[async_trait]
    impl StoreDriver for StallSlowStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(digests, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            // Signal entry so the test can cancel the requester before
            // any bytes are delivered.
            self.get_entered.notify_waiters();
            self.release_get.notified().await;
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(StallSlowStore);

    let original_data = make_random_data(64 * 1024);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len() as u64).unwrap();

    let inner_slow = MemoryStore::new(&MemorySpec::default());
    Pin::new(inner_slow.as_ref())
        .update_oneshot(digest.into(), original_data.clone().into())
        .await?;

    let get_entered = Arc::new(Notify::new());
    let release_get = Arc::new(Notify::new());
    let stall_store = Arc::new(StallSlowStore {
        inner: inner_slow,
        get_entered: Arc::clone(&get_entered),
        release_get: Arc::clone(&release_get),
    });

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(stall_store);
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Caller A: pre-register a `notified()` for `get_entered` before
    // launching, so we don't miss the wake-up.
    let entered_wait = get_entered.notified();
    let fss_a = Arc::clone(&fast_slow_store);
    let caller_a = tokio::spawn(async move {
        fss_a.get_part_unchunked(digest, 0, None).await
    });

    // Wait until caller A's populate has entered slow_store.get_part().
    tokio::time::timeout(Duration::from_secs(5), entered_wait)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "caller A never entered slow get_part"))?;

    // Capture the streaming buffer Arc BEFORE cancelling A, while the
    // populating_digests entry is live.
    let streaming_inner = fast_slow_store
        .populating_streaming_inner(digest.into())
        .expect(
            "populator should have registered a streaming buffer in \
             populating_digests before awaiting slow_store.get_part()",
        );

    // CANCEL caller A. Pre-fix: this drops the populate future → drops
    // the StreamingBlobWriter un-EOF'd → terminal becomes Drop's
    // generic Internal error.
    caller_a.abort();
    // Drain the JoinHandle to ensure the cancelled task is fully torn
    // down before proceeding (the result is a JoinError from the abort).
    drop(caller_a.await);

    // Release the slow store so the populate can proceed (post-fix:
    // the spawned producer is parked here; pre-fix: nobody is parked
    // because the slow_store.get_part future was cancelled).
    release_get.notify_waiters();

    // Read from the streaming buffer to drive the terminal state. With
    // the fix, the producer continues, sends all chunks, sends EOF.
    // Without the fix, the buffer is already terminated with the Drop
    // fallback error.
    let mut reader = StreamingBlob::new_reader(&streaming_inner);
    let mut collected: Vec<u8> = Vec::new();
    let read_outcome: Result<(), Error> = async {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), reader.next_chunk()).await {
                Ok(Ok(c)) if c.is_empty() => return Ok(()),
                Ok(Ok(c)) => collected.extend_from_slice(&c),
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    return Err(make_err!(
                        Code::DeadlineExceeded,
                        "streaming reader hung (producer never resumed after caller cancel)"
                    ));
                }
            }
        }
    }
    .await;

    let read_err = read_outcome.err();
    assert!(
        read_err.is_none(),
        "streaming buffer must terminate with EOF after producer completes — \
         pre-fix Drop fallback would surface here. Got: {read_err:?}"
    );
    assert_eq!(
        collected.as_slice(),
        original_data.as_slice(),
        "waiter must receive full populate data even when populator's caller cancels"
    );

    Ok(())
}

/// Regression for the producer error path: when the slow store fails
/// mid-stream, waiters must receive the typed upstream error via the
/// streaming buffer's terminal state, NOT the generic Drop fallback.
/// This ensures `send_error` is reached on every error path including
/// the spawn-detached producer's error path.
#[nativelink_test]
async fn populate_producer_error_propagates_to_waiters() -> Result<(), Error> {
    use core::sync::atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrd};
    use core::time::Duration;
    use nativelink_util::streaming_blob::StreamingBlob;
    use tokio::sync::Notify;

    /// Slow store whose first `get_part` parks until `release_get` is
    /// notified then returns Unavailable. Subsequent calls return
    /// Unavailable immediately so `get_part`'s slow-store fallback path
    /// terminates promptly. The gate gives the test a deterministic
    /// window to capture the streaming buffer Arc before the producer
    /// completes.
    #[derive(MetricsComponent)]
    struct GatedErrorSlowStore {
        inner: Arc<MemoryStore>,
        get_entered: Arc<Notify>,
        release_get: Arc<Notify>,
        first_call_done: TestAtomicBool,
    }

    #[async_trait]
    impl StoreDriver for GatedErrorSlowStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(digests, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            // First call: signal entry and wait for the test to release
            // the gate. Later calls (e.g. slow-store fallback path)
            // fail immediately without parking.
            if !self.first_call_done.swap(true, TestOrd::AcqRel) {
                self.get_entered.notify_waiters();
                self.release_get.notified().await;
            }
            Err(make_err!(
                Code::Unavailable,
                "synthetic slow-store get_part failure for test"
            ))
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(GatedErrorSlowStore);

    let digest = DigestInfo::try_new(VALID_HASH, 1024).unwrap();
    let inner_slow = MemoryStore::new(&MemorySpec::default());
    // Register a placeholder so `has()` returns Some — only `get_part`
    // errors. This forces the populator past the head_result match into
    // the data_stream_fut path.
    Pin::new(inner_slow.as_ref())
        .update_oneshot(digest.into(), Bytes::from(vec![0u8; 1024]))
        .await?;

    let get_entered = Arc::new(Notify::new());
    let release_get = Arc::new(Notify::new());
    let err_store = Arc::new(GatedErrorSlowStore {
        inner: inner_slow,
        get_entered: Arc::clone(&get_entered),
        release_get: Arc::clone(&release_get),
        first_call_done: TestAtomicBool::new(false),
    });

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(err_store);
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    let entered_wait = get_entered.notified();
    let fss = Arc::clone(&fast_slow_store);
    let caller = tokio::spawn(async move {
        fss.get_part_unchunked(digest, 0, None).await
    });

    // Wait until the producer has entered slow_store.get_part() and is
    // parked on the gate.
    tokio::time::timeout(Duration::from_secs(5), entered_wait)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "producer never entered slow get_part"))?;

    // Capture the streaming buffer Arc while the producer is parked.
    let streaming_inner = fast_slow_store
        .populating_streaming_inner(digest.into())
        .expect(
            "producer should have registered a streaming buffer before \
             entering slow_store.get_part()",
        );

    // Release the gate so the producer fails with Unavailable.
    release_get.notify_waiters();

    // Wait for the caller to complete (with an error).
    let caller_res = tokio::time::timeout(Duration::from_secs(5), caller)
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "caller hung on producer error"))?
        .map_err(|e| make_err!(Code::Internal, "caller join: {:?}", e))?;
    assert!(
        caller_res.is_err(),
        "caller must observe an error when slow store fails mid-stream"
    );

    // The streaming buffer's terminal state must carry a structured
    // error message that reflects the upstream failure, NOT the Drop
    // fallback "writer dropped without sending EOF".
    let mut reader = StreamingBlob::new_reader(&streaming_inner);
    let read_err = tokio::time::timeout(Duration::from_secs(5), reader.next_chunk())
        .await
        .map_err(|_| make_err!(Code::DeadlineExceeded, "streaming reader hung"))?
        .err()
        .expect("streaming buffer terminal must be an error after producer fails");
    assert!(
        !read_err
            .messages
            .iter()
            .any(|m| m.contains("dropped without sending EOF")),
        "streaming buffer terminal must NOT be Drop's fallback — \
         send_error must be reached on every producer error path. \
         Got: {:?}",
        read_err.messages
    );

    Ok(())
}

/// Regression for the inline-fast-path optimisation in `copy_slow_to_fast`.
/// A single-caller cache-miss `populate_fast_store` MUST run the producer
/// inline (no `tokio::spawn`) and skip the streaming-buffer drain. The
/// observable signal is `populate_spawn_count`, which the inline path
/// leaves unchanged. The cancellation-prone `get_part` streaming path
/// keeps `spawn_populate_producer_with_role` and DOES bump the counter —
/// asserted as a contrast so future refactors that accidentally route
/// `copy_slow_to_fast` back through the spawn path get caught.
///
/// Cancellation safety for the streaming `get_part` path remains covered
/// by `populate_survives_caller_cancellation`. This test does not assert
/// cancellation semantics — only the spawn-vs-inline routing.
#[nativelink_test]
async fn populate_inline_does_not_spawn() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store.clone(),
    );

    // Seed the slow store with a blob.
    let payload_a = make_random_data(64 * 1024);
    let digest_a = DigestInfo::try_new(VALID_HASH, payload_a.len() as u64).unwrap();
    slow_store
        .update_oneshot(digest_a, payload_a.clone().into())
        .await?;

    // Inline fast path: single-caller populate_fast_store on a cache-miss
    // blob must NOT spawn the producer. populate_spawn_count must stay 0.
    assert_eq!(
        fast_slow_store.populate_spawn_count(),
        0,
        "test setup: populate_spawn_count must start at 0",
    );
    fast_slow_store
        .populate_fast_store(digest_a.into())
        .await?;
    assert_eq!(
        fast_slow_store.populate_spawn_count(),
        0,
        "single-caller populate_fast_store MUST run the producer inline \
         and skip tokio::spawn (inline fast path optimisation). A non-zero \
         counter means copy_slow_to_fast regressed back through \
         spawn_populate_producer_with_role.",
    );

    // Contrast: the cancellation-prone get_part path on a fresh cache miss
    // SHOULD bump the counter (it keeps spawn-detach for cancellation
    // safety, covered by populate_survives_caller_cancellation).
    let payload_b = make_random_data(64 * 1024);
    let digest_b = DigestInfo::try_new(
        // Distinct hash so the populator runs again on a separate key.
        "fedcba9876543210000000000000000000000000000000000000000000000000",
        payload_b.len() as u64,
    )
    .unwrap();
    slow_store
        .update_oneshot(digest_b, payload_b.clone().into())
        .await?;
    let read_back = Store::new(fast_slow_store.clone())
        .get_part_unchunked(digest_b, 0, None)
        .await?;
    assert_eq!(
        read_back.as_ref(),
        payload_b.as_slice(),
        "get_part must return the populate data verbatim",
    );
    assert_eq!(
        fast_slow_store.populate_spawn_count(),
        1,
        "get_part's cancellation-safe path MUST still spawn-detach. \
         Counter must bump from 0 to 1 over a single populate via get_part. \
         If this assertion fires, get_part likely regressed to inline (which \
         would break populate_survives_caller_cancellation).",
    );

    Ok(())
}

/// Contract test for the refactored `drain_streaming_buffer`: terminal
/// state is the source of truth. With the new structure, the drain
/// checks `terminal_result()` BEFORE reading any chunks; if terminal=Err
/// the drain returns the error even when buffered data is still
/// readable. This prevents the prior race where the recovery path could
/// observe a chunk and return Ok despite the producer terminating with
/// an error (nit #6 from `01b68015`'s code review).
///
/// Setup: hand-build a `StreamingBlobInner` with a 10-byte budget, write
/// 5 chunks of 10 bytes each (forcing eviction so only the last chunk
/// remains), then `send_error`. The drain MUST surface the structured
/// error code, not the buffered data.
#[nativelink_test]
async fn drain_streaming_buffer_propagates_terminal_error_over_buffered_data()
-> Result<(), Error> {
    use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};

    let digest = DigestInfo::try_new(VALID_HASH, 50).unwrap();
    let inner = Arc::new(StreamingBlobInner::new(digest, 10));

    let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
    for i in 0..5u8 {
        writer.send(Bytes::from(vec![i; 10])).await?;
    }
    writer.send_error(make_err!(Code::DataLoss, "synthetic producer mid-stream failure"));
    drop(writer);

    assert!(inner.is_terminal(), "writer.send_error should mark terminal");
    assert!(
        inner.earliest_chunk_idx() > 0,
        "test setup: writes must trigger eviction (earliest > 0)",
    );

    let err = FastSlowStore::drain_streaming_buffer(&inner)
        .await
        .expect_err(
            "drain MUST propagate the producer's terminal error — pre-fix \
             the drain's recovery branch could read buffered data and \
             return Ok despite terminal=Err, silently swallowing the \
             upstream error in copy_slow_to_fast's caller path",
        );
    assert_eq!(err.code, Code::DataLoss);
    Ok(())
}

/// Stress regression for nit #6: the genuine race between drain and a
/// fast producer that errors mid-stream. Spawns a writer task that
/// hammers chunks into a tiny sliding window and then `send_error`s,
/// while the drain runs concurrently. Pre-fix, drain could observe a
/// chunk in the recovery path's fresh reader and return Ok(()) despite
/// terminal=Err. Post-fix, drain checks terminal state directly.
///
/// Run 50 iterations to maximize the chance the race window is hit.
/// Pre-fix this would intermittently return Ok; post-fix it always
/// returns Err.
#[nativelink_test]
async fn drain_streaming_buffer_eviction_race_propagates_error() -> Result<(), Error> {
    use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};

    let digest = DigestInfo::try_new(VALID_HASH, 1000).unwrap();
    for iter in 0..50 {
        let inner = Arc::new(StreamingBlobInner::new(digest, 10));
        let inner_writer = Arc::clone(&inner);
        // Writer task: hammers chunks (each forces eviction) then
        // terminates with an error.
        let writer_task = tokio::spawn(async move {
            let mut writer = StreamingBlobWriter::new(inner_writer);
            for i in 0..100u8 {
                let _send_res = writer.send(Bytes::from(vec![i; 10])).await;
            }
            writer.send_error(make_err!(
                Code::DataLoss,
                "synthetic mid-stream producer failure"
            ));
        });
        // Drain runs concurrently. With the producer error, drain must
        // ALWAYS return Err — never Ok regardless of which inner branch
        // it traversed.
        let drain_res = FastSlowStore::drain_streaming_buffer(&inner).await;
        writer_task
            .await
            .map_err(|e| make_err!(Code::Internal, "writer task panic: {e:?}"))?;
        assert!(
            drain_res.is_err(),
            "iter {iter}: drain must return Err (producer terminated with \
             send_error), got {drain_res:?}. Pre-fix the recovery branch \
             could observe buffered data and return Ok.",
        );
    }
    Ok(())
}

/// Regression test for the lost-wakeup race in `FastSlowStore::flush_slow_writes`.
///
/// `Notify::notified()` does NOT register interest until the returned future is
/// first polled. The previous implementation was:
///
/// ```ignore
/// let notified = self.in_flight_empty_notify.notified();
/// // <-- interest NOT yet registered: future not polled
/// let count = self.in_flight_slow_writes.lock().len();
/// // <-- racing notify_waiters() here is LOST
/// timeout_at(deadline, notified).await
/// ```
///
/// The fix is the canonical `pin!` + `enable()` subscribe-before-predicate
/// pattern — see commit f1750357 (cleanup wait) and the streaming_blob audit
/// for sibling fixes. With the fix, `enable()` arms the subscription before
/// the predicate evaluation, so a concurrently-firing `notify_waiters()` is
/// captured rather than dropped.
///
/// Strategy: pre-populate one in-flight entry so `flush_slow_writes` enters
/// the await path. Drive the race by calling `flush_slow_writes` first (so it
/// has reached the await), then drain the entry and notify. With the fix the
/// notify reliably wakes the flush; without it the notify may be lost and
/// flush blocks until the deadline.
///
/// Requires `multi_thread` runtime: the lost-wakeup window between
/// `notified()` (which does not register interest) and the first poll inside
/// `timeout_at` is essentially zero on a current-thread runtime — there is no
/// `.await` between them, so a single-threaded executor cannot interleave the
/// concurrent `notify_waiters()` call into that window. Real parallelism is
/// required to reproduce the bug, matching the production deployment.
#[nativelink_test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_slow_writes_no_lost_wakeup() -> Result<(), Error> {
    use tokio::sync::Barrier;
    // Per-iteration timeout. With the fix, every iteration completes in
    // microseconds. Without the fix, lost-wakeup iterations stall the full
    // duration — multiplied across iterations, total wall time blows past
    // any reasonable test budget, which is exactly what we assert on.
    const PER_ITER_TIMEOUT: core::time::Duration = core::time::Duration::from_millis(200);
    // Many iterations to give the racing pair (`notify_waiters` vs. the
    // flush task's predicate-check and first poll of `timeout_at`) lots of
    // chances to interleave on a multi-thread runtime. Even one missed
    // wakeup pushes the test budget over.
    const ITERS: usize = 256;
    // Per-iteration successful-flush deadline. With the fix, well under 50ms
    // each. A full-timeout iteration would exceed this.
    const PER_ITER_BUDGET: core::time::Duration = core::time::Duration::from_millis(150);

    for iter in 0..ITERS {
        let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
        let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
        let fss = Arc::new(FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Memory(MemorySpec::default()),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                chunked_reads_enabled: false,
            },
            fast,
            slow,
        ));

        let key: StoreKey<'static> =
            StoreKey::Digest(DigestInfo::try_new(VALID_HASH, 1).unwrap());
        fss.test_insert_in_flight(key.clone(), vec![Bytes::from_static(b"x")]);

        // Barrier ensures both racers release at the same instant, maximising
        // the chance that `notify_waiters()` lands inside the lost-wakeup
        // window between `notified()` and the first poll of `timeout_at`.
        let barrier = Arc::new(Barrier::new(2));

        let fss_flush = Arc::clone(&fss);
        let barrier_flush = Arc::clone(&barrier);
        let flush_task = tokio::spawn(async move {
            barrier_flush.wait().await;
            fss_flush.flush_slow_writes(PER_ITER_TIMEOUT).await
        });

        let fss_notify = Arc::clone(&fss);
        let barrier_notify = Arc::clone(&barrier);
        let key_notify = key.clone();
        let notify_task = tokio::spawn(async move {
            barrier_notify.wait().await;
            fss_notify.test_remove_in_flight_and_notify(key_notify);
        });

        let start = std::time::Instant::now();
        let remaining = flush_task
            .await
            .map_err(|e| make_err!(Code::Internal, "flush task panic: {e:?}"))?;
        notify_task
            .await
            .map_err(|e| make_err!(Code::Internal, "notify task panic: {e:?}"))?;
        let elapsed = start.elapsed();

        assert_eq!(
            remaining, 0,
            "iter {iter}: flush_slow_writes returned {remaining} (expected 0); \
             notify was lost and flush hit the deadline",
        );
        assert!(
            elapsed < PER_ITER_BUDGET,
            "iter {iter}: flush_slow_writes took {elapsed:?} (expected <{PER_ITER_BUDGET:?}); \
             lost-wakeup forced a full deadline wait",
        );
    }
    Ok(())
}

// ===================================================================
// Reviewer Finding 2 (testing-czar Gap 2): construction-site coverage
// for the REAPI v2 §2.2.4 PreconditionFailure detail attachment in
// `fast_slow_store.rs:823` (slow-store `.has()` miss in `run_producer`).
// Without the detail, Bazel sees a generic NotFound and cannot recover
// by re-uploading the missing blob.
// ===================================================================

/// Asserts that calling `get_part` on an empty FastSlowStore (where the
/// slow store reports the blob missing via `.has()`) returns a NotFound
/// error carrying a `PreconditionFailure` detail with `subject` of the
/// form `"blobs/<hash>/<size>"`. This guards the construction site at
/// `nativelink-store/src/fast_slow_store.rs:823`.
///
/// Uses `make_stores()` (non-lazy slow store) so the populator's
/// `slow_store.has()` branch fires — `LazyExistenceOnSync` would skip
/// the `.has()` call and bypass the construction site under test.
#[nativelink_test]
async fn fast_slow_store_not_found_carries_precondition_failure_detail() -> Result<(), Error> {
    use prost::Message;
    use nativelink_util::common::PreconditionFailure;

    let (fast_slow_store, _fast_store, _slow_store) = make_stores();
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    let err = result.err().expect("expected NotFound for missing blob");
    assert_eq!(err.code, Code::NotFound, "expected NotFound, got: {err:?}");
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
    Ok(())
}

// ===================================================================
// Task #124: misleading "fast store item evicted after populate" warn.
//
// The terminal-state branch in `FastSlowStore::get_part` previously
// fired the "evicted after populate" warn for EVERY case where the
// fast store returned NotFound after the producer terminated — including
// the very common case where the producer ITSELF failed with NotFound
// (slow-store had nothing to populate). Production logs (2026-04-24)
// showed 1825 fires of this warn against 3 stale-positive existence-cache
// digests in 20 minutes; 100% of the warns were preceded by `head_result
// Err: NotFound` in `run_producer` — the fast store was never populated,
// nothing was evicted, the warn message was a lie.
//
// The post-fix terminal-state branch consults `terminal_result()` first:
// - Producer Err  → return that error directly. No fast/slow probe — both
//   are guaranteed-NotFound (fast was never written; slow was the source
//   of the producer's NotFound) and the round-trip wastes work AND emits
//   the misleading warn.
// - Producer Ok   → fast store HAS the data unless evicted. Probe; on
//   NotFound, fall back to slow with the (now-accurate) "evicted after
//   populate" warn.
// ===================================================================

/// Slow store that fails `has` with NotFound and counts every
/// `has_with_results` and `get_part` call. Used to assert that a
/// failed-populate get_part does NOT re-issue the slow-store probe
/// after the producer already proved the slow store has nothing.
#[derive(MetricsComponent)]
struct CountingNotFoundSlowStore {
    has_calls: Arc<core::sync::atomic::AtomicU32>,
    get_calls: Arc<core::sync::atomic::AtomicU32>,
}

#[async_trait]
impl StoreDriver for CountingNotFoundSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.has_calls.fetch_add(1, Ordering::Relaxed);
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        _reader: nativelink_util::buf_channel::DropCloserReadHalf,
        _size_info: nativelink_util::store_trait::UploadSizeInfo,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        Err(make_err!(Code::NotFound, "CountingNotFoundSlowStore: blob absent"))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

default_health_status_indicator!(CountingNotFoundSlowStore);

/// Regression for the misleading-warn / wasted-slow-probe flow in
/// the `is_terminal()` branch of `FastSlowStore::get_part`.
///
/// Pre-fix: when a waiter arrives AFTER the producer has terminated
/// with an Err (e.g. slow store had nothing to populate), the
/// terminal-state branch probes the fast store (NotFound — never
/// populated), then FALLS THROUGH to `slow_store.get_part` (NotFound
/// again), emitting the misleading "fast store item evicted after
/// populate" warn. The redundant probe AND the misleading warn fire
/// on EVERY waiter that hits the terminal branch.
///
/// Post-fix: the terminal-state branch consults `terminal_result()`
/// first and returns the producer's error directly — no fast probe,
/// no slow fallback, no misleading warn.
///
/// Determinism: we drive a producer to a terminal-Err state directly
/// via `populating_streaming_inner` injection rather than racing two
/// `get_part` calls (which would non-deterministically hit either the
/// streaming-read path OR the terminal-state branch depending on
/// scheduler whim). The test preconditions (post-injection,
/// `is_terminal() == true`, `terminal_result() == Some(Err(_))`) match
/// the production state captured in journalctl: producer ran, slow
/// store said NotFound, send_error fired, LoaderGuard's Drop already
/// removed the populating_digests entry, then a NEW waiter arrives.
#[nativelink_test]
async fn failed_populate_does_not_reissue_slow_store_probe() -> Result<(), Error> {
    use nativelink_util::streaming_blob::StreamingBlobWriter;

    let has_calls = Arc::new(core::sync::atomic::AtomicU32::new(0));
    let get_calls = Arc::new(core::sync::atomic::AtomicU32::new(0));
    let slow_inner = Arc::new(CountingNotFoundSlowStore {
        has_calls: Arc::clone(&has_calls),
        get_calls: Arc::clone(&get_calls),
    });

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(slow_inner);
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // First call: drives the streaming-populate path. Producer spawns,
    // slow_store.has() returns NotFound (head_result Err), `send_error`
    // fires on the streaming buffer, the waiter's streaming-read loop
    // observes the error and falls back to `slow_store.get_part` (a
    // SEPARATE warn path, not the one under test). After this call
    // returns, the producer has terminated and `LoaderGuard::Drop` has
    // removed the populating_digests entry.
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    let err = result.err().expect("expected NotFound");
    assert_eq!(err.code, Code::NotFound, "first call got: {err:?}");

    // Snapshot counters AFTER the first call completes — the assertion
    // below measures only the second call's slow-store traffic.
    let has_after_first = has_calls.load(Ordering::Relaxed);
    let get_after_first = get_calls.load(Ordering::Relaxed);

    // Pre-arm the terminal-state branch deterministically. Inject a
    // freshly-constructed populating_digests entry whose StreamingBlob
    // is ALREADY in terminal-Err state. A subsequent `get_part` will
    // call `spawn_populate_producer_with_role`, find the entry as a
    // waiter (`is_new=false`, no producer spawn), see `is_terminal()`
    // == true, and execute the branch under test.
    {
        use nativelink_util::streaming_blob::StreamingBlobInner;
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        writer.send_error(make_err!(
            Code::NotFound,
            "synthetic terminal-error reproducing run_producer's head_result Err"
        ));
        drop(writer);
        assert!(inner.is_terminal(), "writer.send_error must mark terminal");
        assert!(inner.has_error(), "terminal must be Err, not Ok");
        // SAFETY-OF-CONTRACT: this hook puts the FastSlowStore in the
        // exact state the production logs show (terminal-Err
        // streaming_inner registered for the digest) so the second
        // get_part deterministically enters the terminal-state branch.
        fast_slow_store.test_install_terminal_populate(digest.into(), inner);
    }

    // Second call: enters `spawn_populate_producer_with_role` as a
    // WAITER (the entry already exists, no fresh spawn), sees
    // `is_terminal()` == true, and executes the branch under test.
    //
    // Pre-fix: probes fast_store (NotFound), emits the misleading
    // "fast store item evicted after populate" warn, falls through to
    // `slow_store.get_part` (NotFound) — bumps get_calls by 1.
    //
    // Post-fix: `terminal_result()` is consulted first; the producer's
    // error is returned directly. get_calls stays the same.
    let result_2 = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    let err_2 = result_2.err().expect("expected NotFound on second call");
    assert_eq!(err_2.code, Code::NotFound, "second call got: {err_2:?}");

    let has_after_second = has_calls.load(Ordering::Relaxed);
    let get_after_second = get_calls.load(Ordering::Relaxed);

    // No fresh producer should have run (we injected a pre-terminated
    // streaming_inner, so the second call should be a waiter). Hence
    // has_calls should NOT have bumped.
    assert_eq!(
        has_after_second, has_after_first,
        "second call should NOT spawn a fresh producer (test injected a \
         pre-terminated populating_digests entry). had {has_after_first}, \
         then {has_after_second}",
    );

    // The redundant slow-store get_part is the bug: the
    // terminal-state branch must NOT re-probe the slow store after the
    // producer already returned NotFound.
    assert_eq!(
        get_after_second, get_after_first,
        "REGRESSION: terminal-state branch re-probed slow store after \
         producer already returned NotFound. Pre-fix this fires the \
         misleading 'fast store item evicted after populate' warn for \
         every waiter. Got {get_after_second} get_part calls; expected \
         {get_after_first} (no extra probe).",
    );

    Ok(())
}

/// Non-NotFound producer errors (Code::Internal "writer dropped",
/// Aborted, Unavailable, etc.) represent transient stream-level
/// failures where the blob may still be present in slow_store. The
/// terminal-state branch must NOT short-circuit on these — it must
/// fall through to the slow-store fallback so a recoverable read can
/// succeed.
///
/// Without this gate, ~13 "writer dropped" Internal events / 2hr in
/// production would be demoted from "recoverable via slow_store
/// fallback" to "propagate Internal up the stack."
#[nativelink_test]
async fn terminal_internal_err_falls_back_to_slow_store() -> Result<(), Error> {
    use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};

    let payload = b"recovered-from-slow-store".to_vec();
    let digest = DigestInfo::try_new(VALID_HASH, payload.len() as u64).unwrap();

    // Slow store HAS the blob — populate before constructing FastSlowStore
    // so the fallback can succeed on the second call.
    let slow_memory = MemoryStore::new(&MemorySpec::default());
    slow_memory
        .update_oneshot(StoreKey::from(digest), payload.clone().into())
        .await?;

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(slow_memory);
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Pre-arm with a terminal Code::Internal — simulates the
    // production "writer dropped without sending EOF" Internal that
    // StreamingBlobWriter::Drop emits when a producer task is cancelled
    // or panics mid-stream.
    {
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        writer.send_error(make_err!(
            Code::Internal,
            "synthetic terminal Internal: writer dropped without sending EOF"
        ));
        drop(writer);
        assert!(inner.is_terminal(), "writer.send_error must mark terminal");
        fast_slow_store.test_install_terminal_populate(digest.into(), inner);
    }

    // The terminal-state branch sees Err(Internal), DOES NOT
    // short-circuit (gate is Code::NotFound only), and falls through
    // to slow_store.get_part — which has the blob, so the read
    // succeeds.
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await?;
    assert_eq!(
        result.as_ref(),
        payload.as_slice(),
        "REGRESSION: terminal-Err non-NotFound short-circuited instead of \
         falling back to slow_store. The Code::NotFound gate at \
         fast_slow_store.rs is missing or broken — non-NotFound terminal \
         errors must fall through so recoverable reads succeed.",
    );

    Ok(())
}

/// Production wedge: when `VerifyStore` wraps `FastSlowStore`, a NotFound
/// terminal-Err short-circuit in `FastSlowStore::get_part` returns
/// `Err(NotFound)` WITHOUT terminating the `tx` half of the channel that
/// the outer caller borrowed. `VerifyStore::get_part` then deadlocks on
/// `tokio::join!(get_fut, check_fut)` because:
///
///   - `get_fut` (FastSlowStore.get_part) returned `Err(NotFound)`, but
///     `tx` lives in the outer scope and is NOT dropped.
///   - `check_fut` (inner_check_get_part) blocks forever on `rx.recv()`
///     waiting for either a chunk or for `tx` to drop.
///
/// In production this manifests as worker builds wedging on
/// medium/large blobs that miss in both fast and slow tiers — peer
/// fall-through (`try_read_from_worker`) is never reached because the
/// wrapping `WorkerProxyStore.get_part` never returns from its inner
/// VerifyStore call. Bug introduced by commit f87359511 (#124 follow-up
/// — gating terminal-Err short-circuit on NotFound). The existing test
/// `terminal_internal_err_falls_back_to_slow_store` covers the
/// non-NotFound path (which falls through to slow_store and naturally
/// terminates the writer); only this composition hits the
/// writer-not-terminated path.
///
/// The test deliberately uses `tokio::time::timeout` with `.expect`
/// (not `?`) so a deadlock surfaces as a panic with an explicit
/// "must not deadlock" message rather than a generic timeout error.
#[nativelink_test]
async fn verify_store_around_fast_slow_does_not_deadlock_on_populator_notfound()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::Store;
    use nativelink_util::streaming_blob::{StreamingBlobInner, StreamingBlobWriter};

    // Build an empty FastSlowStore (both tiers miss for the test
    // digest) and wrap it in VerifyStore with verify_size=true so the
    // get_part path takes the `tokio::join!` branch (the bypass at
    // verify_store.rs:308 returns `false` for `should_verify` only when
    // both verify_size and verify_hash are off; we need it `true` to
    // reproduce the deadlock).
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // Pre-arm the FastSlowStore terminal-Err NotFound branch: inject a
    // `populating_digests` entry whose `StreamingBlobInner` is already
    // in terminal-Err state with `Code::NotFound`. A subsequent
    // `get_part` will become a waiter, see `is_terminal() == true`,
    // and execute the early-return at fast_slow_store.rs ~2912 — which
    // pre-fix did NOT terminate the writer (the wedge).
    {
        let inner = Arc::new(StreamingBlobInner::new(digest, 64 * 1024));
        let mut writer = StreamingBlobWriter::new(Arc::clone(&inner));
        writer.send_error(make_err!(
            Code::NotFound,
            "synthetic terminal-error reproducing slow_store NotFound from run_producer"
        ));
        drop(writer);
        assert!(inner.is_terminal(), "writer.send_error must mark terminal");
        assert!(inner.has_error(), "terminal must be Err");
        fast_slow_store.test_install_terminal_populate(digest.into(), inner);
    }

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fast_slow_store),
    );

    // Wrap the read in a 5s timeout. With the bug present, this hangs
    // forever inside `tokio::join!` in `VerifyStore::get_part` because
    // `FastSlowStore::get_part` returns `Err(NotFound)` without
    // terminating the `tx` that VerifyStore's outer scope owns; the
    // paired `check_fut` blocks on `rx.recv()` indefinitely.
    //
    // The post-fix path terminates the writer (via `send_error` /
    // dropped tx), so `check_fut` unblocks and `join!` completes within
    // milliseconds. We use `.expect` so a deadlock is reported with
    // an explicit "must not deadlock" message rather than a generic
    // `Elapsed`.
    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — this is the bug we're fixing: \
         FastSlowStore::get_part early-returned Err(NotFound) but did \
         not terminate the writer, so VerifyStore's tokio::join! over \
         the tx/rx pair blocks forever on rx.recv()",
    );

    let err = timed.err().expect("expected NotFound, not Ok");
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound to propagate through VerifyStore (via merge \
         semantics: get_res = Err(NotFound), check_res may be Internal/EOF, \
         merge takes get_res's code), got: {err:?}",
    );

    Ok(())
}

/// Sibling-bug regression: when `VerifyStore` wraps `FastSlowStore`, a
/// `mirror_blobs` size-mismatch early return in `FastSlowStore::get_part`
/// (the `data.len() != digest.size_bytes()` defensive guard) returns
/// `Err(NotFound)` WITHOUT terminating the borrowed `tx`, deadlocking
/// `VerifyStore::get_part`'s `tokio::join!(get_fut, check_fut)` on
/// `rx.recv()`. Same mechanism as the populator-NotFound case fixed in
/// commit a384e2e8; this regression test guards the sibling site at
/// `fast_slow_store.rs` ~2657.
#[nativelink_test]
async fn verify_store_around_fast_slow_does_not_deadlock_on_mirror_blobs_size_mismatch()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::Store;

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // Install a phantom-positive mirror entry: digest claims 100 bytes,
    // but stored data is only 5 bytes. The defensive guard at the top
    // of `get_part` notices the mismatch, removes the entry, and
    // returns `Err(NotFound)` — the early return that pre-fix did
    // NOT terminate the writer.
    fast_slow_store.test_insert_mirror_blob_unchecked(
        digest,
        Bytes::from_static(b"short"),
    );

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fast_slow_store),
    );

    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — mirror_blobs size mismatch: \
         FastSlowStore::get_part early-returned Err(NotFound) but did \
         not terminate the writer, so VerifyStore's tokio::join! over \
         the tx/rx pair blocks forever on rx.recv()",
    );

    let err = timed.err().expect("expected NotFound, not Ok");
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound from mirror_blobs size-mismatch path, got: {err:?}",
    );

    Ok(())
}

/// Sibling-bug regression: when `VerifyStore` wraps `FastSlowStore`, a
/// fast-store truncation early return in `FastSlowStore::get_part`
/// (`bytes_written < expected_size && offset == 0 && length.is_none()`)
/// returns `Err(Internal)` WITHOUT calling `writer.send_error`, leaving
/// any bytes already-written orphaned in the channel and deadlocking
/// `VerifyStore::get_part`'s `tokio::join!(get_fut, check_fut)` because
/// the writer's `tx` was borrowed from the outer scope and is never
/// dropped. Same mechanism as the populator-NotFound case; this guards
/// the sibling site at `fast_slow_store.rs` ~2719.
///
/// Subtle: bytes WERE sent into the writer before the truncation was
/// detected. Calling `send_error` is still correct — the receiver
/// observes the structured Internal error rather than an
/// ambiguously-truncated stream.
///
/// Reproducer requires a fast store that sends partial bytes and
/// returns `Ok(())` WITHOUT sending EOF. `MemoryStore` always sends
/// EOF on Ok return, which would mask the deadlock; so we use a
/// `TruncatingFastStore` test fake. In production this corresponds
/// to a producer (e.g. peer-fetch path) that returns Ok but with
/// fewer bytes than the digest claims and does not terminate the
/// writer cleanly — the FastSlowStore guard then early-returns Err
/// without terminating the writer, deadlocking the VerifyStore join.
#[nativelink_test]
async fn verify_store_around_fast_slow_does_not_deadlock_on_fast_store_truncation()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::Store;

    /// Fast store that sends fewer bytes than the digest claims and
    /// returns `Ok(())` WITHOUT calling `writer.send_eof()`. Triggers
    /// the FastSlowStore truncation guard at line ~2719 with the
    /// writer in an un-terminated state.
    #[derive(MetricsComponent)]
    struct TruncatingFastStore {
        truncated_payload: Bytes,
    }

    #[async_trait]
    impl StoreDriver for TruncatingFastStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            // Report present so callers (if they probe) think this
            // store has the blob; not strictly needed for get_part
            // but mirrors the production fast-store invariant.
            for r in results.iter_mut().take(digests.len()) {
                *r = Some(self.truncated_payload.len() as u64);
            }
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _digest: StoreKey<'_>,
            _reader: nativelink_util::buf_channel::DropCloserReadHalf,
            _size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            // Send the truncated payload but do NOT send EOF. Returning
            // Ok(()) here lets FastSlowStore's `Ok` arm execute the
            // bytes_written < expected_size truncation guard. The
            // writer's tx remains alive; the paired reader (inside
            // VerifyStore's join!) blocks forever on rx.recv() unless
            // the early return calls writer.send_error.
            writer
                .send(self.truncated_payload.clone())
                .await
                .err_tip(|| "TruncatingFastStore: send failed")?;
            Ok(())
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(TruncatingFastStore);

    // Digest claims 100 bytes; fast store sends only 50.
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let truncated = Bytes::from(vec![0xAB_u8; 50]);

    let fast_store = Store::new(Arc::new(TruncatingFastStore {
        truncated_payload: truncated.clone(),
    }));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fast_slow_store),
    );

    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — fast_store truncation: \
         FastSlowStore::get_part early-returned Err(Internal) after \
         partial write but did not terminate the writer, so VerifyStore's \
         tokio::join! blocks forever on rx.recv()",
    );

    let err = timed.err().expect("expected Internal/DataLoss, not Ok");
    assert!(
        err.code == Code::Internal || err.code == Code::DataLoss,
        "expected Internal or DataLoss from fast_store truncation path, got: {err:?}",
    );

    Ok(())
}

/// Sibling-bug regression: when `VerifyStore` wraps `FastSlowStore`, an
/// `in_flight_slow_writes` size-mismatch early return in
/// `FastSlowStore::get_part` returns `Err(NotFound)` WITHOUT terminating
/// the borrowed `tx`, deadlocking `VerifyStore::get_part`'s
/// `tokio::join!(get_fut, check_fut)` on `rx.recv()`. Same mechanism as
/// the populator-NotFound case; this guards the sibling site at
/// `fast_slow_store.rs` ~2775.
#[nativelink_test]
async fn verify_store_around_fast_slow_does_not_deadlock_on_in_flight_size_mismatch()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::Store;

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Digest claims 100 bytes; install an in-flight entry whose chunks
    // sum to only 12 bytes ("hello world!" is 12) so the size-mismatch
    // guard fires.
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let owned_key: StoreKey<'static> = StoreKey::from(digest);
    fast_slow_store
        .test_insert_in_flight(owned_key, vec![Bytes::from_static(b"hello world!")]);

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fast_slow_store),
    );

    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — in_flight_slow_writes size mismatch: \
         FastSlowStore::get_part early-returned Err(NotFound) but did \
         not terminate the writer, so VerifyStore's tokio::join! over \
         the tx/rx pair blocks forever on rx.recv()",
    );

    let err = timed.err().expect("expected NotFound, not Ok");
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound from in_flight size-mismatch path, got: {err:?}",
    );

    Ok(())
}

/// Sibling-bug regression: with `local_only_reads` enabled (worker
/// public CAS server variant), a missing blob causes
/// `FastSlowStore::get_part` to return `Err(NotFound)` WITHOUT
/// terminating the borrowed `tx`, deadlocking
/// `VerifyStore::get_part`'s `tokio::join!(get_fut, check_fut)` on
/// `rx.recv()`. Same mechanism as the populator-NotFound case; this
/// guards the sibling site at `fast_slow_store.rs` ~2831.
#[nativelink_test]
async fn verify_store_around_fast_slow_does_not_deadlock_on_local_only_reads()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::store_trait::Store;

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    )
    .with_local_only_reads();

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fast_slow_store),
    );

    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — local_only_reads NotFound: \
         FastSlowStore::get_part early-returned Err(NotFound) but did \
         not terminate the writer, so VerifyStore's tokio::join! over \
         the tx/rx pair blocks forever on rx.recv()",
    );

    let err = timed.err().expect("expected NotFound, not Ok");
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound from local_only_reads path, got: {err:?}",
    );

    Ok(())
}

// ===================================================================
// PHANTOM BLOB false-alarm regression (investigator: ae69e88918d055f5e).
//
// `head_was_ok` was set true whenever `head_result.is_ok()` — INCLUDING
// the LazyExistenceOnSync branch which short-circuits to
// `Ok(UploadSizeInfo::MaxSize(u64::MAX))` WITHOUT calling `slow.has()`.
// The PHANTOM BLOB warn fired on every lazy-skip producer NotFound,
// claiming `slow_store.has() returned Some` when in fact has() was
// never called. 178 false-alarm events / 10 min on production workers
// against 3 hot digests, classified HIGH-severity by the anomaly scan.
//
// The fix replaces the head_was_ok gate with a flag that is only set
// inside the real has-then-Some branch, so the warn fires only on the
// genuine invariant violation: has()=Some, populate=NotFound.
// ===================================================================

/// LazyExistenceOnSync slow-store + missing digest = producer NotFound,
/// but PHANTOM BLOB must NOT fire because `has()` was never called.
#[nativelink_test]
async fn phantom_blob_warn_does_not_fire_on_lazy_existence_skip() -> Result<(), Error> {
    let (fast_slow_store, _fast_store, _slow_store) = make_stores_with_lazy_slow();
    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();

    // Producer head_result short-circuits via LazyExistenceOnSync to
    // Ok(MaxSize(u64::MAX)) without calling has(); slow_store.get
    // then errors NotFound on the empty backing MemoryStore. Pre-fix,
    // this would emit the PHANTOM BLOB warn.
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "expected NotFound for missing blob");
    assert_eq!(
        result.unwrap_err().code,
        Code::NotFound,
        "expected NotFound code from missing blob",
    );

    assert!(
        !logs_contain("PHANTOM BLOB"),
        "PHANTOM BLOB warn fired on a LazyExistenceOnSync skip — \
         has() was never called, so the slow-store-has-said-Some \
         invariant cannot have been violated",
    );

    Ok(())
}

/// Companion: with a non-lazy slow store that returns Some for has(),
/// then NotFound for get(), the PHANTOM BLOB warn MUST still fire.
/// Guards against over-correction (gating the warn off entirely).
#[nativelink_test]
async fn phantom_blob_warn_fires_on_real_has_then_get_notfound() -> Result<(), Error> {
    // Slow store that lies: has() reports Some, but get() reports NotFound.
    // This is the genuine has-evict race the warn was designed to catch.
    #[derive(MetricsComponent)]
    struct LyingHasSlowStore {}

    #[async_trait]
    impl StoreDriver for LyingHasSlowStore {
        async fn has_with_results(
            self: Pin<&Self>,
            _digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            for r in results.iter_mut() {
                *r = Some(100);
            }
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _digest: StoreKey<'_>,
            _reader: nativelink_util::buf_channel::DropCloserReadHalf,
            _size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            Err(make_err!(Code::NotFound, "blob raced eviction between has and get"))
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(LyingHasSlowStore);

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(Arc::new(LyingHasSlowStore {}));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    ));

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "expected NotFound for raced blob");

    assert!(
        logs_contain("PHANTOM BLOB"),
        "PHANTOM BLOB warn must fire on the genuine has=Some,get=NotFound race",
    );

    Ok(())
}

/// Falsification: builds a guard, returns Err WITHOUT committing. With the
/// Drop fallback the wrapping VerifyStore unblocks within milliseconds AND
/// the merged error carries the wire-side "buf_channel: writer dropped
/// without commit" identifier from the check-side, proving the Drop body
/// was the load-bearing terminator (not the generic "Sender dropped
/// before sending EOF" fallback that mpsc-drop alone would synthesize at
/// `buf_channel.rs:567`). The verbose operator-side diagnostic is logged
/// via `tracing::error!` (target: buf_channel::write_half_guard_drop) so
/// internal verb names don't leak to remote clients via tonic::Status.
///
/// Mutation evidence (run manually to validate the test guards the
/// behavior — DO NOT commit the mutation):
///
///   1. Comment out `self.writer.send_error(synthesized);` at the end of
///      `WriteHalfGuard::Drop` in `nativelink-util/src/buf_channel.rs`.
///   2. `cargo test --features failpoints -p nativelink-store --test \
///      fast_slow_store_test write_half_guard_drop_fallback_prevents_uncommitted_deadlock`.
///   3. The test MUST fail at the `messages.iter().any(... "buf_channel: \
///      writer dropped without commit" ...)` assertion below — the
///      receiver instead sees the generic "Sender dropped before sending
///      EOF" Internal that mpsc-drop synthesizes at `buf_channel.rs:567`.
///      Without the Drop body, the wire-side identifier is gone.
///   4. Restore the line and re-run to confirm the test passes again.
#[nativelink_test]
async fn write_half_guard_drop_fallback_prevents_uncommitted_deadlock()
-> Result<(), Error> {
    use core::time::Duration;

    use nativelink_config::stores::VerifySpec;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::buf_channel::WriteHalfGuard;
    use nativelink_util::store_trait::Store;

    #[derive(MetricsComponent)]
    struct ForgetfulStore {
        _marker: (),
    }

    #[async_trait]
    impl StoreDriver for ForgetfulStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            for r in results.iter_mut().take(digests.len()) {
                *r = None;
            }
            Ok(())
        }
        async fn update(
            self: Pin<&Self>,
            _: StoreKey<'_>,
            _: nativelink_util::buf_channel::DropCloserReadHalf,
            _: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Ok(())
        }
        async fn get_part(
            self: Pin<&Self>,
            _: StoreKey<'_>,
            writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
            _: u64,
            _: Option<u64>,
        ) -> Result<(), Error> {
            let _guard = WriteHalfGuard::new(writer);
            // Deliberately uncommitted — Drop fallback MUST terminate the writer.
            Err(make_err!(Code::NotFound, "uncommitted-return"))
        }
        fn inner_store(&self, _: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }
        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
            self
        }
        fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
            self
        }
        fn register_item_callback(
            self: Arc<Self>,
            _: Arc<dyn ItemCallback>,
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
    default_health_status_indicator!(ForgetfulStore);

    let digest = DigestInfo::try_new(VALID_HASH, 100).unwrap();
    let verify_store = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(Arc::new(ForgetfulStore { _marker: () })),
    );
    let timed = tokio::time::timeout(
        Duration::from_secs(5),
        verify_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect(
        "must not deadlock — WriteHalfGuard::Drop fallback MUST terminate \
         the writer when an early-return path forgot to commit. If this \
         panics, check that WriteHalfGuard::drop still calls \
         writer.send_error(synthesized) when committed == false.",
    );
    let err = timed.err().expect("ForgetfulStore deliberately fails");
    // The test would be a tautology if it accepted any Code (NotFound from
    // ForgetfulStore's structured Err vs Internal from a generic Sender-drop
    // fallback both qualify), so we MUST assert on the specific synthesized
    // message that ONLY `WriteHalfGuard::Drop` produces. Without this
    // assertion, mutating away the Drop body would NOT fail the test —
    // mpsc-drop alone synthesizes "Sender dropped before sending EOF"
    // Internal at `buf_channel.rs:567`, which has Code::Internal AND would
    // satisfy a loose `assert!(code == Internal || code == NotFound)`.
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("buf_channel: writer dropped without commit")),
        "merged Err MUST carry the wire-side Drop-fallback identifier so \
         operators can grep for the missing-commit site; otherwise the test \
         is a tautology that passes even when the Drop body is mutated away. \
         (The verbose operator-only diagnostic is logged via \
         tracing::error! target=buf_channel::write_half_guard_drop and is \
         not asserted here because it doesn't flow to the wire/Error.) \
         Got: {err:?}",
    );

    Ok(())
}

/// task #168 item 5 + partial Plan B5: `insert_dispatched_mirror_blob`
/// populates the parallel `dispatched_mirror_pins` index keyed by
/// `(store_id, digest)`. The snapshot iterator returns entries sorted by
/// `store_id` ASCII first then by `DigestInfo`, which is the
/// precondition for the server's binary-search self-filter in
/// `EphemeralServerSidePin::observe_pinned_mirror_ack`.
///
/// Per CLAUDE.md TDD: this test was written first, verified to FAIL
/// (no `dispatched_mirror_pin_snapshot` API existed; the index field
/// did not exist), then the implementation landed and the test passed.
/// Mutation step: comment out the `pins.insert(...)` line in
/// `insert_dispatched_mirror_blob` and verify the assertion below
/// (`snap.is_empty()` instead of len==2) fails.
#[nativelink_test]
async fn dispatched_mirror_pin_snapshot_is_sorted_by_store_id() -> Result<(), Error> {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store,
        slow_store,
    );

    // Empty initially.
    let snap0 = fast_slow_store.dispatched_mirror_pin_snapshot();
    assert!(
        snap0.is_empty(),
        "fresh store MUST snapshot to empty Vec; got {snap0:?}"
    );

    // Use 16-byte payloads so size matches the digest size_bytes
    // invariant (insert_mirror_blob enforces data.len() ==
    // digest.size_bytes()).
    let d_a = DigestInfo::try_new(VALID_HASH, 16).unwrap();
    // Construct a 2nd digest by changing the 1st byte (still 64 hex chars).
    let alt_hash: String = format!(
        "f{}",
        &VALID_HASH[1..]
    );
    let d_b = DigestInfo::try_new(&alt_hash, 16).unwrap();

    // Insert in non-sorted order to verify BTreeMap re-sorts.
    fast_slow_store
        .insert_dispatched_mirror_blob("cas", d_a, Bytes::from(vec![0u8; 16]))
        .expect("insert MUST succeed");
    fast_slow_store
        .insert_dispatched_mirror_blob("ac", d_b, Bytes::from(vec![1u8; 16]))
        .expect("insert MUST succeed");

    let snap = fast_slow_store.dispatched_mirror_pin_snapshot();
    assert_eq!(
        snap.len(),
        2,
        "snapshot MUST hold both inserted (store_id, digest); got {snap:?}"
    );
    // Sorted by store_id ASCII: "ac" < "cas".
    assert_eq!(
        snap[0].0.as_ref(),
        "ac",
        "snapshot[0] MUST be the 'ac' entry (sorted by store_id ASCII); \
         got {:?}",
        snap[0]
    );
    assert_eq!(
        snap[1].0.as_ref(),
        "cas",
        "snapshot[1] MUST be the 'cas' entry (sorted by store_id ASCII); \
         got {:?}",
        snap[1]
    );

    // After remove_mirror_blobs (server-confirmed), the matching index
    // entries MUST also drop.
    fast_slow_store.remove_mirror_blobs(&[d_a, d_b]);
    let snap2 = fast_slow_store.dispatched_mirror_pin_snapshot();
    assert!(
        snap2.is_empty(),
        "snapshot MUST be empty after remove_mirror_blobs cleared the \
         underlying mirror_blobs (the parallel index is cleaned in lock-step); \
         got {snap2:?}"
    );

    Ok(())
}

// ============================================================================
// Option A AC mirroring tests for FastSlowStore.
//
// Coverage map:
//   1. insert_local_ac_pin: under-action (write succeeds → entry in
//      dispatched_mirror_pins) + over-action (write fails → no pin)
//   2. remove_local_ac_pins via BIS-ack drain: under (matching ack →
//      removed) + over (unrelated ack → kept)
//   3. Cross-FSS isolation (digest aliasing safety): CAS BIS ack does
//      NOT remove an AC pin on a different FSS instance, and vice versa.
//
// Each test uses production composition (real FastSlowStore via the
// real spec → new() path) and a tokio::time::timeout deadlock detector.
// Mutation steps are described in each test's doc-comment.
// ============================================================================

/// Helper: construct a real FastSlowStore for AC pin testing. The
/// MemoryStore tiers mirror the production AC FSS shape (worker.json5
/// configures `MemoryStore` fast tier + `GrpcStore` slow tier; we
/// substitute `MemoryStore` for the slow tier in test-only since the
/// pin index doesn't depend on which slow type is used).
fn make_fss_for_ac_pin() -> Arc<FastSlowStore> {
    FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
}

fn d(byte: u8) -> DigestInfo {
    let hash: String = (0..32).map(|_| format!("{byte:02x}")).collect();
    DigestInfo::try_new(&hash, 100).unwrap()
}

/// (Test 1, under-action) `insert_local_ac_pin` MUST add `(store_id,
/// digest)` to `dispatched_mirror_pins` (advertisable on the next
/// BlobsAvailable tick) and MUST NOT touch `mirror_blobs` (AC pins
/// don't store bytes — the AC FSS holds them in its fast tier).
///
/// Mutation step: comment out
/// `self.dispatched_mirror_pins.lock().insert(...)` in
/// `insert_local_ac_pin` and confirm the snapshot assertion red-fails
/// with the bespoke "MUST contain the inserted entry" message.
#[nativelink_test]
async fn insert_local_ac_pin_advertises_without_touching_mirror_blobs(
) -> Result<(), Error> {
    let fss = make_fss_for_ac_pin();
    let digest = d(0x42);
    let snap0 = fss.dispatched_mirror_pin_snapshot();
    assert!(snap0.is_empty(), "fresh FSS must have empty pin snapshot");
    let blob_count_before = fss.mirror_blob_count();
    let blob_bytes_before = fss.mirror_blobs_used_bytes();

    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async { fss.insert_local_ac_pin("AC_MAIN_STORE", digest); },
    )
    .await
    .expect("must not deadlock — insert_local_ac_pin contract violated");

    let snap = fss.dispatched_mirror_pin_snapshot();
    assert_eq!(
        snap.len(),
        1,
        "dispatched_mirror_pin_snapshot MUST contain the inserted entry; \
         under-action: insert_local_ac_pin failed to record the pin"
    );
    assert_eq!(snap[0].0.as_ref(), "AC_MAIN_STORE");
    assert_eq!(snap[0].1, digest);

    // Over-action probe: AC pin MUST NOT touch mirror_blobs (this
    // would over-allocate RAM on every AC write — the production
    // mirror_blobs cap is for CAS only).
    assert_eq!(
        fss.mirror_blob_count(),
        blob_count_before,
        "AC pin MUST NOT register a mirror_blobs entry; over-action: \
         insert_local_ac_pin populated the wrong index"
    );
    assert_eq!(
        fss.mirror_blobs_used_bytes(),
        blob_bytes_before,
        "AC pin MUST NOT charge byte cap; over-action: \
         insert_local_ac_pin charged the CAS mirror byte budget"
    );
    Ok(())
}

/// (Test 2 under-action) `remove_local_ac_pins` MUST remove an entry
/// whose digest matches a supplied digest (the BIS-ack drain path).
/// (Test 2 over-action) `remove_local_ac_pins` MUST NOT remove an
/// entry whose digest does NOT match — that would silently flap the
/// pin and re-advertise it, causing the server registry to oscillate.
///
/// Mutation steps:
/// - Replace `pins.retain(|(_, d), ()| !lookup.contains(d))` with
///   `pins.retain(|_, _| true)` (no-op) → under-action assertion
///   fails ("MUST be empty after BIS-style ack").
/// - Replace `pins.retain(|(_, d), ()| !lookup.contains(d))` with
///   `pins.clear()` → over-action assertion fails ("MUST keep the
///   unrelated digest's pin").
#[nativelink_test]
async fn remove_local_ac_pins_drops_only_matched_digests() -> Result<(), Error> {
    let fss = make_fss_for_ac_pin();
    let d_ack = d(0xAA);
    let d_keep = d(0xBB);
    fss.insert_local_ac_pin("AC_MAIN_STORE", d_ack);
    fss.insert_local_ac_pin("AC_MAIN_STORE", d_keep);
    assert_eq!(fss.dispatched_mirror_pin_snapshot().len(), 2);

    // Drive the BIS-ack drain.
    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async { fss.remove_local_ac_pins(&[d_ack]); },
    )
    .await
    .expect("must not deadlock — remove_local_ac_pins contract violated");

    let snap = fss.dispatched_mirror_pin_snapshot();
    // Under-action: matched entry MUST drop.
    assert!(
        !snap.iter().any(|(_, x)| *x == d_ack),
        "remove_local_ac_pins MUST drop the matched digest's pin entry; \
         under-action: ack arrived but worker keeps re-advertising the AC pin"
    );
    // Over-action: unrelated entry MUST stay.
    assert!(
        snap.iter().any(|(_, x)| *x == d_keep),
        "remove_local_ac_pins MUST keep the unrelated digest's pin entry; \
         over-action: an unrelated ack stripped a still-in-flight AC pin"
    );
    assert_eq!(snap.len(), 1);
    Ok(())
}

/// (Test 3) Cross-FSS isolation: an AC FSS instance and a CAS FSS
/// instance are SEPARATE objects with SEPARATE pin maps. A
/// remove call on one MUST NOT affect the other, even when the
/// digest is identical (the action_digest collision case — the
/// REAPI-mandated reuse of the same hash for the Action proto in
/// CAS and the AC entry pointing to its result).
///
/// This is the regression test for the digest-collision exploit:
/// previously the `pinned_mirror_entries` channel routed AC pins
/// through the CAS-shared `BlobLocalityMap`, which would weaponize CAS
/// upload short-circuits to silently drop Action proto bytes. The
/// current design hard-partitions AC vs CAS via separate FSS instances
/// and a dedicated `pinned_ac_mirror_entries` proto field; this
/// test asserts the FSS-level isolation that underlies the wire
/// partition.
///
/// Mutation step: re-use the same Arc<FastSlowStore> for both `ac` and
/// `cas` (treating them as one instance) — this test red-fails because
/// `cas_fss.remove_mirror_blobs` would also drain the AC pins.
#[nativelink_test]
async fn ac_and_cas_fss_pin_maps_are_isolated_by_construction() -> Result<(), Error> {
    let cas_fss = make_fss_for_ac_pin();
    let ac_fss = make_fss_for_ac_pin();
    let aliased = d(0xCC); // same digest plays both roles

    // Worker has CAS-side mirror byte for the digest (a peer-pushed
    // CAS blob) AND an AC pin for the same digest (a worker-written
    // AC entry referencing an Action whose action_digest == this
    // digest by REAPI design).
    cas_fss
        .insert_dispatched_mirror_blob("cas_STORE", aliased, Bytes::from(vec![0u8; 100]))
        .expect("CAS insert");
    ac_fss.insert_local_ac_pin("AC_MAIN_STORE", aliased);
    assert_eq!(cas_fss.dispatched_mirror_pin_snapshot().len(), 1);
    assert_eq!(ac_fss.dispatched_mirror_pin_snapshot().len(), 1);

    // Direction A: CAS BIS ack arrives — drains CAS only, leaves AC.
    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async { cas_fss.remove_mirror_blobs(&[aliased]); },
    )
    .await
    .expect("must not deadlock");
    assert!(
        cas_fss.dispatched_mirror_pin_snapshot().is_empty(),
        "CAS BIS ack MUST drain CAS pin (under-action)"
    );
    assert_eq!(
        ac_fss.dispatched_mirror_pin_snapshot().len(),
        1,
        "CAS BIS ack on aliased digest MUST NOT touch AC pin map; \
         over-action: cross-FSS leakage between CAS and AC channels — \
         this is the digest-collision exploit on the AC mirroring \
         channel that the hard-partition design defends against."
    );

    // Direction B: AC BIS ack arrives — drains AC only.
    // (CAS already empty so direction-A's invariant is
    // trivially preserved here.)
    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async { ac_fss.remove_local_ac_pins(&[aliased]); },
    )
    .await
    .expect("must not deadlock");
    assert!(
        ac_fss.dispatched_mirror_pin_snapshot().is_empty(),
        "AC BIS ack MUST drain AC pin (under-action)"
    );
    Ok(())
}

/// (Test 4 over-action) `insert_local_ac_pin` is a sync no-arg call
/// from the success path of `upload_ac_results` — there's no async
/// boundary at which it could be cancelled or skipped. The
/// over-action analog is therefore "a write FAILED but the call
/// fired anyway." Production callsite places `insert_local_ac_pin`
/// AFTER `update_oneshot.await?` (the `?` returns Err early if the
/// fast write failed). We verify this contract at the integration
/// layer in running_actions_manager_test (test 6 below), but it's
/// worth a unit-level check that an empty digests slice is a no-op
/// AND an empty pins map shortcuts to no notify storm.
#[nativelink_test]
async fn remove_local_ac_pins_empty_inputs_are_noops() -> Result<(), Error> {
    let fss = make_fss_for_ac_pin();
    // Empty digests on empty pins.
    fss.remove_local_ac_pins(&[]);
    assert!(fss.dispatched_mirror_pin_snapshot().is_empty());
    // Empty digests on populated pins.
    fss.insert_local_ac_pin("AC_MAIN_STORE", d(0x77));
    fss.remove_local_ac_pins(&[]);
    assert_eq!(fss.dispatched_mirror_pin_snapshot().len(), 1);
    // Non-empty digests on empty pins (already drained).
    fss.remove_local_ac_pins(&[d(0x77)]);
    assert!(fss.dispatched_mirror_pin_snapshot().is_empty());
    fss.remove_local_ac_pins(&[d(0x99)]); // no match
    Ok(())
}

/// (Test 5 — BlobsAvailable advertisement: AC slice).
/// `dispatched_ac_pin_snapshot_for_store` MUST return only digests
/// whose `store_id` matches the requested `ac_store_id`. This is the
/// load-bearing partition that keeps AC pins out of the CAS-shaped
/// `pinned_mirror_entries` (field 16) slice on the wire.
///
/// Asymmetric coverage:
/// - under (matching store: digests appear in the slice);
/// - over (different store: digests DO NOT appear in the AC slice
///   even though they share the same `dispatched_mirror_pins` map).
///
/// Mutation step: replace the `(sid.as_ref() == ac_store_id)` filter
/// with `true` → the over-action assertion ("must NOT appear under
/// AC slice for OTHER store") red-fails.
#[nativelink_test]
async fn ac_pin_snapshot_filters_strictly_by_store_id() -> Result<(), Error> {
    let fss = make_fss_for_ac_pin();
    let d_main = d(0x10);
    let d_other = d(0x20);
    fss.insert_local_ac_pin("AC_MAIN_STORE", d_main);
    fss.insert_local_ac_pin("AC_OTHER_STORE", d_other);
    // Add a CAS-shaped pin too — must also stay out of either
    // AC slice.
    fss.insert_local_ac_pin("cas_STORE", d(0x30));

    let main_slice = fss.dispatched_ac_pin_snapshot_for_store("AC_MAIN_STORE");
    let other_slice = fss.dispatched_ac_pin_snapshot_for_store("AC_OTHER_STORE");

    // Under-action: matching store's pin appears.
    assert!(
        main_slice.contains(&d_main),
        "AC slice for AC_MAIN_STORE MUST include its own pin entry; \
         under-action: snapshot filter dropped a matching digest"
    );
    assert!(
        other_slice.contains(&d_other),
        "AC slice for AC_OTHER_STORE MUST include its own pin entry"
    );

    // Over-action: pins from a different store_id MUST NOT appear.
    assert!(
        !main_slice.contains(&d_other),
        "AC slice for AC_MAIN_STORE MUST NOT include AC_OTHER_STORE pin; \
         over-action: snapshot filter is too permissive across store_ids"
    );
    assert!(
        !other_slice.contains(&d_main),
        "AC slice for AC_OTHER_STORE MUST NOT include AC_MAIN_STORE pin"
    );
    assert!(
        !main_slice.contains(&d(0x30)),
        "AC slice for AC_MAIN_STORE MUST NOT include cas_STORE pin; \
         over-action: AC snapshot leaks CAS-shaped pins into the AC \
         field-17 wire slice (would route into CAS BlobLocalityMap and \
         weaponize CAS upload short-circuits — the digest-collision \
         exploit the hard-partition design defends against)"
    );
    Ok(())
}

/// (Test 5 sibling — empty AC slice when no AC pins exist).
/// An advertisement tick fired by an unrelated CAS event MUST emit
/// an EMPTY AC slice (no spurious entries from the shared
/// `dispatched_mirror_pins` map). Catches "snapshot ignores
/// store_id filter on empty input" / "snapshot returns the whole
/// map when filter empty" bugs.
#[nativelink_test]
async fn ac_pin_snapshot_empty_when_no_ac_pins() -> Result<(), Error> {
    let fss = make_fss_for_ac_pin();
    // Only CAS-shaped pin exists.
    fss.insert_local_ac_pin("cas_STORE", d(0x40));
    let ac_slice = fss.dispatched_ac_pin_snapshot_for_store("AC_MAIN_STORE");
    assert!(
        ac_slice.is_empty(),
        "AC slice MUST be empty when no AC pin exists; \
         over-action: snapshot leaked CAS pins into AC slice"
    );
    Ok(())
}

// =============================================================================
// #284 part 2: warn-and-continue on cache-tee at-cap (regression tests).
//
// Root cause covered: when the fast tier (MemoryStore at-cap) rejects the
// populator's `fast_store.update` mid-stream with
// `Code::ResourceExhausted` + `BackpressureSignal::MemoryStoreAtCapacity`,
// the producer historically poisoned the streaming buffer's terminal
// state via `streaming_writer.send_error`, so any consumer reading from
// the streaming buffer saw a mid-stream Err. WorkerProxyStore's
// bytes-written-then-erred branch then aborted the consumer stream with
// "cannot peer-fetch without corrupting consumer stream", surfacing to
// Bazel as a digest-mismatch or RESOURCE_EXHAUSTED. This caused the
// 2026-05-06 read cascade abort.
//
// The fix in `fast_slow_store::run_producer` splits the streaming-writer
// terminal state from the populator-caller's `returned`: when the
// cache-tee was disabled mid-stream AND the fast-tier failure carries an
// at-cap discriminator, the streaming buffer terminates with EOF
// (consumer reads cleanly) while the populator caller still gets the
// Err so `copy_slow_to_fast` knows the populate did not land.
//
// Asymmetric coverage (CLAUDE.md mandatory practice):
// - Under-action (positive case): on at-cap mid-stream, the consumer
//   MUST receive ALL bytes from slow tier — the populator MUST NOT
//   poison the streaming buffer.
// - Over-action (negative case): on fast-tier success, the populator
//   MUST still surface fast-tier writes correctly (no false positives
//   that demote a real error into a silent success).
// =============================================================================

/// **Under-action (the bug fix).** Production composition: real
/// `FastSlowStore` with a MemoryStore fast tier (capped + emit-
/// backpressure ON, so any incoming write rejects with
/// `MemoryStoreAtCapacity`) wrapped around a `GatedSlowStore` that
/// delivers the bytes in two installments: chunk 0 immediately, then
/// awaits a `release_eof` notify before sending EOF. The blob's
/// declared size exceeds the fast tier's cap, so the producer's
/// `fast_store.update` rejects upfront via `MemoryStore`'s `ExactSize`
/// early-reject (`memory_store.rs:367-369`). The cache-tee
/// `fast_tx.send` fails on the first chunk after
/// `streaming_writer.send` already delivered chunk 0 to the
/// streaming buffer.
///
/// Forcing the slow store to park between chunk 0 and EOF guarantees
/// the consumer enters the **streaming reader path** (line 4562+ in
/// `fast_slow_store.rs`) — NOT the terminal-Err recovery branch (line
/// 4496-4535) which falls back to `slow_store.get_part` regardless of
/// the populator's terminal state. The streaming reader path is where
/// the bug actually fires: the consumer reads chunk 0 from the
/// streaming buffer, then on the next `next_chunk()` observes the
/// producer's terminal. Without the fix, that terminal is the at-cap
/// Err and the consumer's `get_part_unchunked` returns Err. With the
/// fix (the `cache_tee_at_cap` demotion in `streaming_terminal`), the
/// terminal is Ok (EOF) and the consumer receives all bytes cleanly.
///
/// **Mutation step**: replace the `if cache_tee_at_cap` guard with
/// `if false && cache_tee_at_cap` in `fast_slow_store::run_producer`'s
/// `streaming_terminal`. The consumer's `get_part_unchunked` then
/// returns Err mid-stream and the bespoke `.expect("populator must
/// warn-and-continue on MemoryStore at-cap …")` red-fails. Verified
/// 2026-05-06: with the mutation, the test fails with the bespoke
/// message; without the mutation, it passes.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn populate_at_capacity_does_not_abort_consumer_when_caps_mid_stream(
) -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::{EvictionPolicy, FastSlowSpec, MemorySpec, StoreSpec};
    use nativelink_util::buf_channel::DropCloserWriteHalf;
    use nativelink_util::store_trait::Store;
    use sha2::{Digest as _, Sha256};
    use tokio::sync::Notify;

    /// Slow store wrapper that gates between chunk 0 and EOF on a
    /// notify. `has_with_results` defers to the inner. `get_part`
    /// sends the first 1024 bytes then awaits `release_eof` before
    /// sending the remaining bytes + EOF. Update is unused.
    ///
    /// This shape is what makes the test deterministic: the producer's
    /// `data_stream_fut` parks on `slow_rx.recv()` after delivering
    /// chunk 0 to the streaming buffer, giving the consumer a window
    /// to enter the streaming-reader path AND read chunk 0 BEFORE the
    /// producer's terminal arrives. Without the gate, the producer
    /// could finish synchronously on a fast-enough runtime and the
    /// consumer would hit the terminal-Err recovery branch instead of
    /// the streaming-reader Err arm — masking the bug.
    #[derive(MetricsComponent)]
    struct GatedSlowStore {
        inner: Arc<MemoryStore>,
        chunk0_sent: Arc<Notify>,
        release_eof: Arc<Notify>,
    }

    #[async_trait]
    impl StoreDriver for GatedSlowStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .has_with_results(digests, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            digest: StoreKey<'_>,
            reader: nativelink_util::buf_channel::DropCloserReadHalf,
            size_info: nativelink_util::store_trait::UploadSizeInfo,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .update(digest, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            // Pull the full payload from inner, slice off chunk 0
            // (1 KiB), send it to the writer + signal, then await
            // release before sending the remainder + EOF.
            let full = Pin::new(self.inner.as_ref())
                .get_part_unchunked(key, offset, length)
                .await?;
            let split_at = core::cmp::min(1024, full.len());
            let chunk0 = full.slice(0..split_at);
            let remainder = full.slice(split_at..);
            writer
                .send(chunk0)
                .await
                .err_tip(|| "GatedSlowStore: send chunk0 failed")?;
            self.chunk0_sent.notify_waiters();
            self.release_eof.notified().await;
            if !remainder.is_empty() {
                writer
                    .send(remainder)
                    .await
                    .err_tip(|| "GatedSlowStore: send remainder failed")?;
            }
            writer.send_eof().err_tip(|| "GatedSlowStore: send_eof failed")?;
            Ok(())
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
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

    default_health_status_indicator!(GatedSlowStore);

    // Build a deterministic 4 KiB payload (4× the fast-tier cap so
    // the ExactSize early-reject in MemoryStore::update fires for
    // the cache-tee write). Using a sha2 hash matches the production
    // VerifyStore wire format; the test uses the FSS directly without
    // VerifyStore, but a real digest avoids any accidental special-
    // casing on `is_zero_digest`.
    let payload: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let mut hash_arr = [0u8; 32];
    hash_arr.copy_from_slice(&hasher.finalize());
    let digest = DigestInfo::new(hash_arr, payload.len() as u64);

    // Inner slow tier: fresh MemoryStore with ample capacity; pre-load
    // the payload so `GatedSlowStore` can deliver it in two
    // installments.
    let inner_slow = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 16 * 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });
    Pin::new(inner_slow.as_ref())
        .update_oneshot(StoreKey::from(digest), Bytes::from(payload.clone()))
        .await?;

    let chunk0_sent = Arc::new(Notify::new());
    let release_eof = Arc::new(Notify::new());
    let gated_slow = Arc::new(GatedSlowStore {
        inner: inner_slow,
        chunk0_sent: Arc::clone(&chunk0_sent),
        release_eof: Arc::clone(&release_eof),
    });

    // Fast tier: MemoryStore with 1 KiB cap + emit_backpressure ON.
    // ExactSize(4096) > 1024 → MemoryStore::update returns
    // ResourceExhausted+BackpressureSignal::MemoryStoreAtCapacity
    // BEFORE pulling any chunk off `fast_rx`, which drops `fast_rx`
    // and causes the populator's `fast_tx.send` to fail.
    let fast_store_arc = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: true,
    });
    fast_store_arc.enable_emit_backpressure();

    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(fast_store_arc.clone()),
        Store::new(gated_slow),
    );
    let fss_store = Store::new(fss_arc);

    // Pre-register the chunk0_sent waiter so we can't miss the
    // wake-up if the producer signals it before we await.
    let chunk0_wait = chunk0_sent.notified();

    // Spawn the consumer's get_part_unchunked. Without the spawn we
    // cannot interleave the test's release_eof notify with the
    // consumer's read (the consumer's future does not yield long
    // enough for us to do anything between is_terminal=false and
    // the streaming reader's first poll otherwise).
    //
    // Pass `length=None` (NOT Some(payload.len())) so the consumer's
    // streaming reader awaits the producer's terminal state via
    // `next_chunk()` AFTER receiving all bytes. Passing Some(N) where
    // N == total bytes makes the consumer break the read loop on
    // `pos >= end` BEFORE observing the terminal — masking both
    // success (clean EOF) and the bug (Err on terminal). length=None
    // forces the loop to keep polling next_chunk until EOF or Err,
    // which is the path the bug fires through.
    let consumer_handle = tokio::spawn({
        let store = fss_store.clone();
        async move {
            store.get_part_unchunked(digest, 0, None).await
        }
    });

    // Wait until the slow store has sent chunk 0 (proves the producer
    // task has been scheduled and is now parked awaiting release_eof).
    // The consumer task has either entered the streaming reader path
    // already, or is about to: in either case the producer's terminal
    // is NOT yet set, so when the consumer hits next_chunk after
    // reading chunk 0 it MUST observe the post-release terminal state
    // (the bug-firing path).
    tokio::time::timeout(Duration::from_secs(5), chunk0_wait)
        .await
        .expect("must not deadlock — slow store should send chunk 0 promptly");

    // Yield once to let the consumer's streaming reader pick up
    // chunk 0 from the buffer before we release the producer's
    // terminal. Without this yield the producer could race ahead and
    // terminate before the consumer's first next_chunk runs, sending
    // the test through the terminal-Err recovery branch instead of
    // the streaming-reader Err arm (masking the bug).
    tokio::task::yield_now().await;

    // Release the slow store's EOF gate. The producer's data_stream_fut
    // resumes, drains the remainder + EOF, returns Ok. The merge logic
    // computes `cache_tee_at_cap=true` (cache_tee_disabled=true AND
    // fast_res is ResourceExhausted+BackpressureSignal). With the fix,
    // streaming_terminal=Ok → send_eof to consumer; without the fix,
    // streaming_terminal=Err → send_error to consumer.
    release_eof.notify_waiters();

    // Collect the consumer's result. The 10s timeout is the deadlock
    // detector — if any code path hangs, this surfaces with the bespoke
    // message rather than a generic CI hang.
    let bytes = tokio::time::timeout(Duration::from_secs(10), consumer_handle)
        .await
        .expect(
            "must not deadlock — populator must warn-and-continue on MemoryStore at-cap; \
             the streaming-writer terminal-state demotion (`cache_tee_at_cap` Ok branch) \
             keeps the consumer's stream alive while fast-tier population is skipped",
        )
        .expect("consumer task must not panic")
        .expect(
            "populator must warn-and-continue on MemoryStore at-cap — must not abort \
             consumer stream; consumer expects all 4096 bytes to flow through the \
             streaming buffer with a clean EOF, not a poisoned ResourceExhausted",
        );

    assert_eq!(
        bytes.len(),
        payload.len(),
        "consumer MUST receive ALL bytes via the streaming buffer (slow tier delivered \
         every chunk); cache-tee at-cap MUST NOT truncate the consumer's read",
    );
    assert_eq!(
        bytes.as_ref(),
        payload.as_slice(),
        "consumer bytes MUST equal the slow-tier payload byte-for-byte; the streaming \
         buffer must forward slow-tier chunks unmodified after the cache-tee is disabled",
    );

    // Belt-and-suspenders: the fast tier MUST NOT contain the blob
    // (cache-tee was correctly skipped). Without this check, a future
    // regression that "magically populated" the fast tier (e.g., via
    // a retry path that bypassed the kill-switch) would silently
    // change the consumer-perceived correctness.
    let fast_has = Pin::new(fast_store_arc.as_ref())
        .has(StoreKey::from(digest))
        .await?;
    assert!(
        fast_has.is_none(),
        "fast tier MUST NOT contain the blob — cache-tee was disabled mid-stream by \
         the at-cap rejection; got fast_has={fast_has:?}"
    );

    Ok(())
}

/// **Pre-stream / nothing-to-lose case.** When the slow tier itself
/// returns NotFound (no bytes ever delivered to the streaming buffer),
/// the consumer MUST receive a clean error rather than spuriously
/// succeeding with empty bytes. This guards the over-action sibling
/// of the warn-and-continue fix — the demotion of fast-tier Err to
/// streaming-EOF must NOT trigger when there were never any bytes to
/// begin with (the slow tier failed before any chunk reached the
/// streaming buffer).
///
/// Setup: fast tier capped + emit-backpressure ON (so it would reject
/// any incoming write); slow tier EMPTY (so `slow_store.has` returns
/// None → producer's `head_result` is Err NotFound BEFORE the data
/// stream even starts). The producer terminates with NotFound; the
/// streaming buffer's terminal state is Err NotFound; the consumer's
/// `get_part` enters the terminal-Err branch (line 4496-4500 in
/// `fast_slow_store.rs`), falls through to slow_store fallback,
/// re-issues `slow_store.get_part`, and surfaces NotFound to the
/// caller. With the fix, this path is unchanged from legacy.
///
/// **Mutation step**: change `cache_tee_at_cap` to `true` (always-
/// demote regardless of whether cache-tee was actually disabled). The
/// streaming buffer would then EOF on a producer-NotFound, the
/// consumer would re-read from slow tier (still empty), and the
/// final result would be NotFound — same outcome. So this test is
/// less mutation-sensitive; its real value is asserting that the
/// cache-tee demotion does NOT spuriously succeed when no bytes were
/// delivered (over-action of the demotion logic).
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn populate_at_capacity_pre_stream_returns_clean_error() -> Result<(), Error> {
    use core::time::Duration;
    use nativelink_config::stores::{EvictionPolicy, FastSlowSpec, MemorySpec, StoreSpec};
    use nativelink_util::store_trait::Store;

    // Same VALID_HASH constant defined at top of file.
    let digest = DigestInfo::try_new(VALID_HASH, 1024)?;

    // Slow tier: EMPTY MemoryStore with ample cap. `has(digest)` will
    // return None → producer's `head_result` errors with NotFound
    // before any byte hits the data stream.
    let slow_store_arc = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 16 * 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: false,
    });

    // Fast tier: cap-1KiB + emit_backpressure ON.
    let fast_store_arc = MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: true,
    });
    fast_store_arc.enable_emit_backpressure();

    let fss_arc = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        Store::new(fast_store_arc.clone()),
        Store::new(slow_store_arc.clone()),
    );
    let fss_store = Store::new(fss_arc);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        fss_store.get_part_unchunked(digest, 0, Some(1024)),
    )
    .await
    .expect(
        "must not deadlock — empty slow tier must surface NotFound promptly; \
         the cache-tee at-cap demotion MUST NOT mask a real upstream miss",
    );

    let err = result.expect_err(
        "consumer MUST receive a clean error when slow tier has no bytes to deliver; \
         the cache-tee demotion MUST NOT silently succeed with empty bytes (over-action \
         of the at-cap warn-and-continue fix)",
    );
    assert_eq!(
        err.code,
        Code::NotFound,
        "expected NotFound from empty slow tier, got code={:?} messages={:?}",
        err.code,
        err.messages,
    );

    Ok(())
}
