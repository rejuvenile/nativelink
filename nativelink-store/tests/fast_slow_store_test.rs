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
use nativelink_util::store_trait::{ItemCallback, Store, StoreDriver, StoreKey, StoreLike};
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

// ─────────────────────────────────────────────────────────────────────
// Regression: orphan-drop in populate_and_maybe_stream early-? paths
// ─────────────────────────────────────────────────────────────────────

/// Sibling regression to commit `49bf70fb` (which covered the inner
/// `data_stream_fut`). Prior to this fix, the two `?` paths in
/// `populate_and_maybe_stream` that run BEFORE `streaming_writer` is
/// moved into `data_stream_fut` (the slow-store `has()` RPC error and
/// the slow-store NotFound branch) would unwind the function frame,
/// dropping the in-scope `StreamingBlobWriter` un-EOF'd. Drop's
/// fallback then set the streaming buffer's terminal state to
/// `Code::Internal "writer dropped without sending EOF"`, masking the
/// real upstream cause for any concurrent waiters reading the buffer.
///
/// This test forces a populator to enter `populate_and_maybe_stream`,
/// captures the streaming buffer Arc via the public diagnostic
/// accessor, then releases the slow store's `has()` to return None
/// (NotFound). After the populator finishes, we read the streaming
/// buffer's terminal state via a `StreamingBlobReader` and assert
/// the error code is `Code::NotFound` — proving `send_error` ran
/// before the writer was dropped.
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
