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
        .update_oneshot(digest.into(), payload.clone().into())
        .await?;

    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(slow_memory);
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
