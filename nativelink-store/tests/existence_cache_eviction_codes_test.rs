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

//! Regression tests for the ExistenceCacheStore stale-entry preservation
//! bug.
//!
//! Pre-fix `get_part` and `batch_get_part_unchunked` only evicted the
//! cached existence entry when the inner store returned `Code::NotFound`.
//! For `Code::DataLoss` (truncation/hash mismatch from VerifyStore),
//! `Code::Internal` (storage faults), and `Code::OutOfRange` the cache
//! entry was preserved — leaving subsequent `has()` calls reporting the
//! blob as present even though it cannot be retrieved.
//!
//! Operationally this manifests as: VerifyStore catches a corruption,
//! propagates DataLoss to the caller, the existence cache still says
//! "I have it", and the next FindMissingBlobs / has_with_results /
//! upload-skip fast-path keeps trusting the stale entry. The blob
//! becomes a zombie — present in the cache, unreadable from the inner
//! store, can't be re-uploaded because the cache claims it exists.

use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{ExistenceCacheSpec, MemorySpec, NoopSpec, StoreSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// A wrapping Store that delegates to an inner MemoryStore for `update`
/// (so we can stage data) but returns a configurable error `Code` from
/// `get_part`. This simulates VerifyStore-detected corruption,
/// internal storage faults, etc.
#[derive(Debug, MetricsComponent)]
struct ErrCodeOnGetStore {
    inner: Store,
    /// Error code to return on get_part.
    err_code: Code,
    /// Atomic count of get_part calls (to verify retry behavior etc).
    get_part_calls: AtomicU32,
}

impl ErrCodeOnGetStore {
    fn new(inner: Store, err_code: Code) -> Self {
        Self {
            inner,
            err_code,
            get_part_calls: AtomicU32::new(0),
        }
    }
}

default_health_status_indicator!(ErrCodeOnGetStore);

#[async_trait]
impl StoreDriver for ErrCodeOnGetStore {
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
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_part_calls.fetch_add(1, Ordering::Relaxed);
        Err(make_err!(
            self.err_code,
            "ErrCodeOnGetStore: simulated {:?} from inner store",
            self.err_code
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
}

/// Build an ExistenceCacheStore with the given inner-error code and
/// pre-stage a digest in the existence cache (writing succeeds; cache
/// records the entry; then reads return `err_code`).
///
/// The backing memory store is wrapped in `ErrCodeOnGetStore` so that:
/// - `update_oneshot` reaches the inner MemoryStore (blob is stored)
/// - `get_part` returns the configured error code
///
/// The cache observes the successful write via the item callback and
/// records the digest. We return the store + error-injection store +
/// digest so tests can verify cache behavior when reads fail.
async fn make_primed_cache_store(
    err_code: Code,
) -> Result<
    (
        Arc<ExistenceCacheStore<std::time::SystemTime>>,
        Arc<ErrCodeOnGetStore>,
        DigestInfo,
    ),
    Error,
> {
    let backing_mem = Store::new(MemoryStore::new(&MemorySpec::default()));
    let err_store = Arc::new(ErrCodeOnGetStore::new(backing_mem, err_code));
    let inner = Store::new(err_store.clone());
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: None,
    };
    let store = ExistenceCacheStore::new(&spec, inner);

    let data = Bytes::from_static(b"primed cache data");
    let digest = DigestInfo::try_new(VALID_HASH, data.len() as u64)?;

    // Write through the cache store. The update path writes to the
    // backing memory store (via ErrCodeOnGetStore which proxies writes
    // through) then the cache observes the digest.
    store
        .update_oneshot(digest, data)
        .await
        .err_tip(|| "setup: priming write")?;

    assert!(
        store.exists_in_cache(&digest).await,
        "cache must contain the digest after priming write"
    );

    Ok((store, err_store, digest))
}

// -------------------------------------------------------------------
// 1. get_part returns DataLoss → cache MUST evict the stale entry
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_data_loss_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::DataLoss).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err(), "get_part should fail with DataLoss");
    assert_eq!(result.unwrap_err().code, Code::DataLoss);

    // BUG: pre-fix, only NotFound triggered eviction. DataLoss left
    // the cache lying — `exists_in_cache` would return true.
    assert!(
        !store.exists_in_cache(&digest).await,
        "DataLoss must evict the cache entry — VerifyStore caught corruption \
         and the blob is unreadable"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 2. get_part returns Internal → cache MUST evict the stale entry
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_internal_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::Internal).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::Internal);

    assert!(
        !store.exists_in_cache(&digest).await,
        "Internal must evict the cache entry — storage fault means blob \
         is unrecoverable from this inner store"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 3. get_part returns OutOfRange → cache MUST evict the stale entry
//
// OutOfRange means the requested byte range exceeds blob length, which
// for a full read indicates the blob is truncated server-side.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_out_of_range_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::OutOfRange).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::OutOfRange);

    assert!(
        !store.exists_in_cache(&digest).await,
        "OutOfRange must evict the cache entry — blob is truncated"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 4. get_part returns NotFound → cache MUST evict (regression guard)
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_not_found_evicts_cache_unchanged() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::NotFound).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::NotFound);

    assert!(
        !store.exists_in_cache(&digest).await,
        "NotFound must continue to evict (existing behavior)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 5. get_part returns Unavailable → cache should NOT evict (transient)
//
// Unavailable signals a transient connectivity issue. Evicting on
// transient codes would force re-uploads on every blip, defeating the
// cache's purpose. Pre-fix this case fell into `Err(_) => {}` (no
// eviction). We want the FIX to preserve that exact behavior — we
// only widen on permanent-failure codes (DataLoss/Internal/OutOfRange/
// NotFound), not transient ones.
// -------------------------------------------------------------------
#[nativelink_test]
async fn get_part_unavailable_does_not_evict_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::Unavailable).await?;

    let result = store.get_part_unchunked(digest, 0, None).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, Code::Unavailable);

    assert!(
        store.exists_in_cache(&digest).await,
        "Unavailable is transient — cache entry must be preserved \
         (otherwise every connectivity blip forces a re-upload)"
    );

    Ok(())
}

// -------------------------------------------------------------------
// 6. batch_get_part_unchunked DataLoss → cache MUST evict
// -------------------------------------------------------------------
#[nativelink_test]
async fn batch_get_part_data_loss_evicts_cache() -> Result<(), Error> {
    let (store, _err_store, digest) = make_primed_cache_store(Code::DataLoss).await?;

    let keys: Vec<StoreKey<'_>> = vec![digest.into()];
    let results = store
        .as_store_driver_pin()
        .batch_get_part_unchunked(keys, None)
        .await;

    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    assert_eq!(results[0].as_ref().err().unwrap().code, Code::DataLoss);

    assert!(
        !store.exists_in_cache(&digest).await,
        "batch_get_part: DataLoss must evict the cache entry"
    );

    Ok(())
}
