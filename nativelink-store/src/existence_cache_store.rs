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
use std::borrow::Cow;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace};

use nativelink_config::stores::{EvictionPolicy, ExistenceCacheSpec};

// DEBUG INSTRUMENTATION (remove after wedge root cause confirmed):
// Targets the cover.o wedge digest to expose which path repopulates
// the existence cache for a blob the inner store does not actually have.
const DEBUG_DIGEST_HASH_HEX: &str =
    "3418dec2ac048e354993d688bc4cba02660d523f15a148f090a99f79d5adedaa";
const DEBUG_DIGEST_SIZE: u64 = 1_726_208;

#[inline]
fn debug_digest_match(d: &DigestInfo) -> bool {
    d.size_bytes() == DEBUG_DIGEST_SIZE
        && format!("{d}").starts_with(DEBUG_DIGEST_HASH_HEX)
}
use nativelink_error::{Code, Error, ResultExt, error_if};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::store_trait::{
    ItemCallback, PinDelegation, StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike,
    StoreOptimizations, UploadSizeInfo,
};

/// Returns `true` for error codes that indicate the inner store cannot
/// recover this blob (the cached "exists" claim is now a lie).
///
/// - `NotFound`   — blob was evicted from inner store
/// - `DataLoss`   — VerifyStore caught a hash/length mismatch (corruption)
/// - `Internal`   — storage fault (inner store can't read its own data)
/// - `OutOfRange` — requested range exceeds blob length (truncation)
///
/// Transient codes (`Unavailable`, `DeadlineExceeded`, `ResourceExhausted`,
/// etc.) are NOT included — re-evicting on every connectivity blip would
/// force re-uploads and defeat the cache's purpose. We err on the side of
/// over-evicting for permanent-looking errors and under-evicting for
/// transient ones; an over-eviction triggers at most one extra has() RPC,
/// while an under-eviction (false positive in the cache) can hide a
/// missing blob through repeated FindMissingBlobs cycles.
fn is_unrecoverable_read_error(code: Code) -> bool {
    matches!(
        code,
        Code::NotFound | Code::DataLoss | Code::Internal | Code::OutOfRange
    )
}

#[derive(Clone, Debug)]
struct ExistenceItem(u64);

impl LenEntry for ExistenceItem {
    #[inline]
    fn len(&self) -> u64 {
        self.0
    }

    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug, MetricsComponent)]
pub struct ExistenceCacheStore<I: InstantWrapper> {
    #[metric(group = "inner_store")]
    inner_store: Store,
    existence_cache: Arc<MokaEvictingMap<DigestInfo, DigestInfo, ExistenceItem, I>>,
    // Eviction callbacks fire immediately (no queuing). If a blob is
    // written and immediately evicted, the callback removes it from the
    // existence cache, then update() re-inserts it. Any transient stale
    // positive is self-correcting: get_part() removes on NotFound, and
    // update() bypasses the cache to check the inner store.
}

impl ExistenceCacheStore<SystemTime> {
    pub fn new(spec: &ExistenceCacheSpec, inner_store: Store) -> Arc<Self> {
        Self::new_with_time(spec, inner_store, SystemTime::now())
    }
}

impl<I: InstantWrapper> ItemCallback for ExistenceCacheStore<I> {
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        debug!(?store_key, "ExistenceCacheStore: eviction callback received");
        let digest = store_key.borrow().into_digest();
        Box::pin(async move {
            if debug_digest_match(&digest) {
                info!(?digest, source = "callback_inner_eviction", "DEBUG: ExistenceCacheStore removing wedge digest");
            }
            let deleted_key = self.existence_cache.remove(&digest).await;
            if deleted_key {
                debug!(?store_key, "ExistenceCacheStore: eviction callback removed key from cache");
            } else {
                debug!(?store_key, "ExistenceCacheStore: eviction callback key not in cache (already removed or never cached)");
            }
        })
    }
}

#[derive(Debug)]
struct ExistenceCacheCallback<I: InstantWrapper> {
    cache: Weak<ExistenceCacheStore<I>>,
}

impl<I: InstantWrapper> ItemCallback for ExistenceCacheCallback<I> {
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let cache = self.cache.upgrade();
        if let Some(local_cache) = cache {
            // Always fire callbacks immediately — removing a digest from
            // the existence cache is cheap and idempotent. The update()
            // path re-inserts after a successful write, so a concurrent
            // eviction callback cannot create a stale positive.
            let store_key = store_key.into_owned();
            return Box::pin(async move {
                local_cache.callback(store_key).await;
            });
        } else {
            debug!("ExistenceCacheStore: eviction callback skipped (cache dropped)");
        }
        Box::pin(async {})
    }
}

impl<I: InstantWrapper> ExistenceCacheStore<I> {
    /// Returns a reference to the wrapped inner store.
    pub fn inner_store(&self) -> &Store {
        &self.inner_store
    }

    pub fn new_with_time(
        spec: &ExistenceCacheSpec,
        inner_store: Store,
        anchor_time: I,
    ) -> Arc<Self> {
        let empty_policy = EvictionPolicy::default();
        let eviction_policy = spec.eviction_policy.as_ref().unwrap_or(&empty_policy);
        let existence_cache = Arc::new(MokaEvictingMap::with_anchor(eviction_policy, anchor_time));
        existence_cache.start_background_eviction();
        let existence_cache_store = Arc::new(Self {
            inner_store,
            existence_cache,
        });
        let other_ref = Arc::downgrade(&existence_cache_store);
        existence_cache_store
            .inner_store
            .register_item_callback(Arc::new(ExistenceCacheCallback { cache: other_ref }))
            .expect("Register item callback should work");
        existence_cache_store
    }

    pub async fn exists_in_cache(&self, digest: &DigestInfo) -> bool {
        let mut results = [None];
        self.existence_cache
            .sizes_for_keys([digest], &mut results[..], true /* peek */)
            .await;
        results[0].is_some()
    }

    pub async fn remove_from_cache(&self, digest: &DigestInfo) {
        self.existence_cache.remove(digest).await;
    }

    async fn inner_has_with_results(
        self: Pin<&Self>,
        keys: &[DigestInfo],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.existence_cache
            .sizes_for_keys(keys, results, true /* peek */)
            .await;

        let not_cached_keys: Vec<_> = keys
            .iter()
            .zip(results.iter())
            .filter_map(|(digest, result)| result.map_or_else(|| Some(digest.into()), |_| None))
            .collect();

        // Hot path optimization when all keys are cached.
        if not_cached_keys.is_empty() {
            return Ok(());
        }

        // Now query only the items not found in the cache.
        let mut inner_results = vec![None; not_cached_keys.len()];
        self.inner_store
            .has_with_results(&not_cached_keys, &mut inner_results)
            .await
            .err_tip(|| "In ExistenceCacheStore::inner_has_with_results")?;

        // Insert found from previous query into our cache.
        {
            // The iterator borrows not_cached_keys and inner_results which are
            // local — the borrow can't cross the insert_many() await boundary
            // (the iterator wouldn't be Send). Collect into a Vec first.
            let mut inserts = Vec::with_capacity(not_cached_keys.len());
            for (key, result) in not_cached_keys.iter().zip(inner_results.iter()) {
                if let Some(size) = result {
                    let digest = key.borrow().into_digest();
                    if debug_digest_match(&digest) {
                        info!(?digest, size = *size, source = "inner_has_with_results", "DEBUG: ExistenceCacheStore inserting wedge digest (inner.has returned Some)");
                    }
                    inserts.push((digest, ExistenceItem(*size)));
                }
            }
            drop(self.existence_cache.insert_many(inserts).await);
        }

        // Merge the results from the cache and the query.
        {
            let mut inner_results_iter = inner_results.into_iter();
            // We know at this point that any None in results was queried and will have
            // a result in inner_results_iter, so use this knowledge to fill in the results.
            for result in results.iter_mut() {
                if result.is_none() {
                    *result = inner_results_iter
                        .next()
                        .expect("has_with_results returned less results than expected");
                }
            }
            // Ensure that there was no logic error by ensuring our iterator is not empty.
            error_if!(
                inner_results_iter.next().is_some(),
                "has_with_results returned more results than expected"
            );
        }

        Ok(())
    }
}

#[async_trait]
impl<I: InstantWrapper> StoreDriver for ExistenceCacheStore<I> {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // TODO(palfrey) This is a bit of a hack to get around the lifetime issues with the
        // existence_cache. We need to convert the digests to owned values to be able to
        // insert them into the cache. In theory it should be able to elide this conversion
        // but it seems to be a bit tricky to get right.
        let digests: Vec<_> = digests
            .iter()
            .map(|key| key.borrow().into_digest())
            .collect();
        self.inner_has_with_results(&digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let digest = key.into_digest();
        // Check the inner store directly, bypassing the existence cache.
        // The existence cache may have a stale positive for a blob that was
        // evicted from the inner store (the async eviction callback may not
        // have fired yet). Trusting the cache here would skip the upload,
        // causing Bazel's "Lost inputs no longer available remotely" error.
        let mut exists = [None];
        self.inner_store
            .has_with_results(&[digest.into()], &mut exists)
            .await
            .err_tip(|| "In ExistenceCacheStore::update")?;
        if exists[0].is_some() {
            // Blob genuinely exists in the inner store — safe to skip.
            reader
                .drain()
                .await
                .err_tip(|| "In ExistenceCacheStore::update")?;
            // Refresh the existence cache since we verified it exists.
            if debug_digest_match(&digest) {
                info!(?digest, size = exists[0].unwrap(), source = "update_refresh_after_inner_has_some", "DEBUG: ExistenceCacheStore inserting wedge digest (update path: inner says present)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(exists[0].unwrap()))
                .await;
            return Ok(());
        }
        // If the existence cache had a stale entry, remove it now.
        if debug_digest_match(&digest) {
            info!(?digest, source = "update_remove_stale", "DEBUG: ExistenceCacheStore removing wedge digest (update path: inner.has=None)");
        }
        self.existence_cache.remove(&digest).await;
        // Track that an update is in progress. Eviction callbacks fire
        // normally (no queuing) — they just remove from the existence
        // cache, which is idempotent. We re-insert after a successful
        // write, so a concurrent eviction cannot create a stale positive.
        trace!(?digest, "Inserting into inner cache");

        // Failpoint: simulate inner store write failure. Verifies that the
        // existence cache is NOT populated on write failure (would cause a
        // stale positive where has() returns true but get_part() returns
        // NotFound).
        #[cfg(feature = "failpoints")]
        fail::fail_point!("existence_cache_inner_store_write_fail", |_| {
            Err(nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "failpoint: inner store write failed"
            ))
        });

        let update_start = std::time::Instant::now();
        let result = self.inner_store.update(digest, reader, size_info).await;
        let elapsed_ms = update_start.elapsed().as_millis() as u64;
        if let Err(ref err) = result {
            error!(
                ?digest,
                elapsed_ms,
                ?err,
                "ExistenceCacheStore::update: inner store write failed",
            );
        } else if elapsed_ms > 100 {
            info!(
                ?digest,
                elapsed_ms,
                "ExistenceCacheStore::update: inner store write slow",
            );
        }
        if result.is_ok() {
            trace!(?digest, "Inserting into existence cache");
            // Cache on both ExactSize and MaxSize — the digest carries the
            // authoritative size for content-addressed blobs.
            let size = match size_info {
                UploadSizeInfo::ExactSize(size) => size,
                UploadSizeInfo::MaxSize(_) => digest.size_bytes(),
            };
            if debug_digest_match(&digest) {
                info!(?digest, size, source = "update_after_successful_write", "DEBUG: ExistenceCacheStore inserting wedge digest (update path: inner.update succeeded)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(size))
                .await;
        }
        result
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        optimization == StoreOptimizations::SubscribesToUpdateOneshot
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        data: Bytes,
    ) -> Result<(), Error> {
        let digest = key.into_digest();
        // Bypass the existence cache and check inner store directly.
        // Same stale-positive prevention as update().
        let mut exists = [None];
        self.inner_store
            .has_with_results(&[digest.into()], &mut exists)
            .await
            .err_tip(|| "In ExistenceCacheStore::update_oneshot")?;
        if exists[0].is_some() {
            // Blob genuinely exists in the inner store — safe to skip.
            if debug_digest_match(&digest) {
                info!(?digest, size = exists[0].unwrap(), source = "update_oneshot_refresh_after_inner_has_some", "DEBUG: ExistenceCacheStore inserting wedge digest (update_oneshot path: inner says present)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(exists[0].unwrap()))
                .await;
            return Ok(());
        }
        // If the existence cache had a stale entry, remove it now.
        if debug_digest_match(&digest) {
            info!(?digest, source = "update_oneshot_remove_stale", "DEBUG: ExistenceCacheStore removing wedge digest (update_oneshot path: inner.has=None)");
        }
        self.existence_cache.remove(&digest).await;

        // Failpoint: simulate inner store oneshot write failure. Verifies
        // that the existence cache is NOT populated on write failure.
        #[cfg(feature = "failpoints")]
        fail::fail_point!("existence_cache_update_oneshot_fail", |_| {
            Err(nativelink_error::make_err!(
                nativelink_error::Code::Internal,
                "failpoint: update_oneshot inner store write failed"
            ))
        });

        trace!(?digest, "Inserting into inner cache via update_oneshot");
        let update_start = std::time::Instant::now();
        let size = u64::try_from(data.len())
            .err_tip(|| "Could not convert data.len() to u64 in update_oneshot")?;
        let result = self.inner_store.update_oneshot(digest, data).await;
        let elapsed_ms = update_start.elapsed().as_millis() as u64;
        if let Err(ref err) = result {
            error!(
                ?digest,
                elapsed_ms,
                ?err,
                "ExistenceCacheStore::update_oneshot: inner store write failed",
            );
        } else if elapsed_ms > 100 {
            info!(
                ?digest,
                elapsed_ms,
                "ExistenceCacheStore::update_oneshot: inner store write slow",
            );
        }
        if result.is_ok() {
            trace!(?digest, "Inserting into existence cache via update_oneshot");
            if debug_digest_match(&digest) {
                info!(?digest, size, source = "update_oneshot_after_successful_write", "DEBUG: ExistenceCacheStore inserting wedge digest (update_oneshot path: inner.update_oneshot succeeded)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(size))
                .await;
        }
        result
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let digest = key.into_digest();

        // Failpoint: simulate inner store returning NotFound during read.
        // Verifies that the existence cache removes stale entries when the
        // inner store reports a blob as missing (blob evicted after cache
        // recorded its existence).
        #[cfg(feature = "failpoints")]
        fail::fail_point!("existence_cache_get_part_not_found", |_| {
            Err(nativelink_error::Error::new(
                nativelink_error::Code::NotFound,
                "failpoint: blob not found in inner store".to_string(),
            ))
        });

        let result = self
            .inner_store
            .get_part(digest, writer, offset, length)
            .await;
        match &result {
            Ok(()) => {
                if debug_digest_match(&digest) {
                    info!(?digest, size = digest.size_bytes(), source = "get_part_after_successful_inner", "DEBUG: ExistenceCacheStore inserting wedge digest (get_part path: inner.get_part succeeded)");
                }
                let _ = self
                    .existence_cache
                    .insert(digest, ExistenceItem(digest.size_bytes()))
                    .await;
            }
            Err(err) if is_unrecoverable_read_error(err.code) => {
                // Blob is unrecoverable from the inner store — remove
                // the stale existence cache entry so subsequent has()
                // calls get an accurate result. Covers NotFound (evicted),
                // DataLoss (verifier caught corruption), Internal (storage
                // fault), and OutOfRange (truncation). Transient codes
                // (Unavailable, DeadlineExceeded, etc.) leave the cache
                // alone — re-evicting on every blip would force re-uploads.
                if debug_digest_match(&digest) {
                    info!(?digest, code = ?err.code, source = "get_part_remove_unrecoverable", "DEBUG: ExistenceCacheStore PRE-remove wedge digest (get_part path: inner unrecoverable error)");
                }
                self.existence_cache.remove(&digest).await;
                if debug_digest_match(&digest) {
                    info!(?digest, code = ?err.code, source = "get_part_remove_unrecoverable", "DEBUG: ExistenceCacheStore POST-remove wedge digest (cache.remove returned, about to return Err to caller)");
                }
            }
            Err(_) => {}
        }
        result
    }

    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let digests: Vec<DigestInfo> = keys.iter().map(|k| k.borrow().into_digest()).collect();
        let results = Pin::new(self.inner_store.as_store_driver())
            .batch_get_part_unchunked(keys, length)
            .await;
        // Batch-update existence cache: collect successful digests for a
        // single insert_many() call (one run_pending_tasks() at the end)
        // instead of N sequential insert() calls.
        let mut inserts = Vec::new();
        let mut removals = Vec::new();
        for (digest, result) in digests.iter().zip(results.iter()) {
            match result {
                Ok(_) => {
                    if debug_digest_match(digest) {
                        info!(?digest, size = digest.size_bytes(), source = "batch_get_part_unchunked_after_successful", "DEBUG: ExistenceCacheStore inserting wedge digest (batch_get_part_unchunked path)");
                    }
                    inserts.push((*digest, ExistenceItem(digest.size_bytes())));
                }
                Err(err) if is_unrecoverable_read_error(err.code) => {
                    // Same eviction policy as get_part: widen beyond just
                    // NotFound to include DataLoss / Internal / OutOfRange.
                    if debug_digest_match(digest) {
                        info!(?digest, code = ?err.code, source = "batch_get_part_unchunked_remove_unrecoverable", "DEBUG: ExistenceCacheStore removing wedge digest (batch_get_part_unchunked: unrecoverable)");
                    }
                    removals.push(*digest);
                }
                Err(_) => {}
            }
        }
        if !inserts.is_empty() {
            drop(self.existence_cache.insert_many(inserts).await);
        }
        for digest in removals {
            self.existence_cache.remove(&digest).await;
        }
        results
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
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
        callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        self.inner_store.register_item_callback(callback)
    }

    /// ExistenceCacheStore is a single-inner wrapper. The cache lives at
    /// this layer but does NOT participate in the BIS / pin chain — both
    /// flow through unchanged to `inner_store`. Trait defaults handle it.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner_store.as_store_driver())
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner_store.as_store_driver())
    }
}

#[async_trait]
impl<I: InstantWrapper> HealthStatusIndicator for ExistenceCacheStore<I> {
    fn get_name(&self) -> &'static str {
        "ExistenceCacheStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
