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

use core::ops::BitXor;
use core::pin::Pin;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, TryStreamExt};
use nativelink_config::stores::ShardSpec;
use nativelink_error::{Error, ResultExt, error_if};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DelegationChildren, ItemCallback, DurableDelegation, MarkStableDelegation, MergedNotifyState, PinDelegation,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tracing::warn;

#[derive(Debug, MetricsComponent)]
struct StoreAndWeight {
    #[metric(help = "The weight of the store")]
    weight: u32,
    #[metric(help = "The underlying store")]
    store: Store,
}

#[derive(Debug, MetricsComponent)]
pub struct ShardStore {
    // The weights will always be in ascending order a specific store is chosen based on the
    // the hash of the key hash that is nearest-binary searched using the u32 as the index.
    #[metric(
        group = "stores",
        help = "The weights and stores that are used to determine which store to use"
    )]
    weights_and_stores: Vec<StoreAndWeight>,
    /// Lazy-initialized merged Notify state backing the `Many` BIS chain
    /// across shards. Populated on first `stable_notify()` call. Owns the
    /// AbortOnDrop forwarder handles so wrapper drop aborts the spawned
    /// tasks (closes F2 task leak). Not metric'd.
    merged_stable_notify: OnceLock<MergedNotifyState>,
}

impl ShardStore {
    pub fn new(spec: &ShardSpec, stores: Vec<Store>) -> Result<Arc<Self>, Error> {
        error_if!(
            spec.stores.len() != stores.len(),
            "Config shards do not match stores length"
        );
        error_if!(
            spec.stores.is_empty(),
            "ShardStore must have at least one store"
        );
        let total_weight: u64 = spec
            .stores
            .iter()
            .map(|shard_config| u64::from(shard_config.weight.unwrap_or(1)))
            .sum();
        let mut weights: Vec<u32> = spec
            .stores
            .iter()
            .map(|shard_config| {
                u32::try_from(
                    u64::from(u32::MAX) * u64::from(shard_config.weight.unwrap_or(1))
                        / total_weight,
                )
                .unwrap_or(u32::MAX)
            })
            .scan(0, |state, weight| {
                *state += weight;
                Some(*state)
            })
            .collect();
        // Our last item should always be the max.
        *weights.last_mut().unwrap() = u32::MAX;
        let result = Arc::new(Self {
            weights_and_stores: weights
                .into_iter()
                .zip(stores)
                .map(|(weight, store)| StoreAndWeight { weight, store })
                .collect(),
            merged_stable_notify: OnceLock::new(),
        });
        // Eagerly initialize the merged-stable-notify forwarders so the
        // cold-path lost-wakeup window between construction and first
        // external `stable_notify()` call is closed (perf-optimizer
        // CRITICAL Finding 1, .claude/reviews/c-plus-d/perf-optimizer.md).
        // Requires a tokio runtime; all production callers and
        // #[nativelink_test] harnesses provide one.
        let _ = StoreDriver::stable_notify(result.as_ref());
        Ok(result)
    }

    /// Returns `true` if the merged-stable-notify state has been
    /// eagerly initialized (forwarder tasks running). Exposed for the
    /// production-composition test that verifies cold-path lost-wakeup
    /// safety; not used in production code.
    #[doc(hidden)]
    pub fn merged_stable_notify_initialized(&self) -> bool {
        self.merged_stable_notify.get().is_some()
    }

    fn get_store_index(&self, store_key: &StoreKey) -> usize {
        let key = match store_key {
            StoreKey::Digest(digest) => {
                // Quote from std primitive array documentation:
                //     Array’s try_from(slice) implementations (and the corresponding slice.try_into()
                //     array implementations) succeed if the input slice length is the same as the result
                //     array length. They optimize especially well when the optimizer can easily determine
                //     the slice length, e.g. <[u8; 4]>::try_from(&slice[4..8]).unwrap(). Array implements
                //     TryFrom returning.
                let size_bytes = digest.size_bytes().to_le_bytes();
                0.bitxor(u32::from_le_bytes(
                    digest.packed_hash()[0..4].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[4..8].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[8..12].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[12..16].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[16..20].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[20..24].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[24..28].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[28..32].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(size_bytes[0..4].try_into().unwrap()))
                .bitxor(u32::from_le_bytes(size_bytes[4..8].try_into().unwrap()))
            }
            StoreKey::Str(s) => {
                let hash = blake3::hash(s.as_bytes());
                let hash_bytes = hash.as_bytes();
                u32::from_le_bytes([hash_bytes[0], hash_bytes[1], hash_bytes[2], hash_bytes[3]])
            }
        };
        self.weights_and_stores
            .binary_search_by_key(&key, |item| item.weight)
            .unwrap_or_else(|index| index)
    }

    fn get_store(&self, key: &StoreKey) -> &Store {
        let index = self.get_store_index(key);
        &self.weights_and_stores[index].store
    }
}

#[async_trait]
impl StoreDriver for ShardStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        type KeyIdxVec = Vec<usize>;
        type KeyVec<'a> = Vec<StoreKey<'a>>;

        if keys.len() == 1 {
            // Hot path: It is very common to lookup only one key.
            let store_idx = self.get_store_index(&keys[0]);
            let store = &self.weights_and_stores[store_idx].store;
            return store
                .has_with_results(keys, results)
                .await
                .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}}");
        }
        let mut keys_for_store: Vec<(KeyIdxVec, KeyVec)> = self
            .weights_and_stores
            .iter()
            .map(|_| (Vec::new(), Vec::new()))
            .collect();
        // Bucket each key into the store that it belongs to.
        keys.iter()
            .enumerate()
            .map(|(key_idx, key)| (key, key_idx, self.get_store_index(key)))
            .for_each(|(key, key_idx, store_idx)| {
                keys_for_store[store_idx].0.push(key_idx);
                keys_for_store[store_idx].1.push(key.borrow());
            });

        // Build all our futures for each store.
        let mut future_stream: FuturesUnordered<_> = keys_for_store
            .into_iter()
            .enumerate()
            .map(|(store_idx, (key_idxs, keys))| async move {
                let store = &self.weights_and_stores[store_idx].store;
                let mut inner_results = vec![None; keys.len()];
                store
                    .has_with_results(&keys, &mut inner_results)
                    .await
                    .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}")?;
                Result::<_, Error>::Ok((key_idxs, inner_results))
            })
            .collect();

        // Wait for all the stores to finish and populate our output results.
        while let Some((key_idxs, inner_results)) = future_stream.try_next().await? {
            for (key_idx, inner_result) in key_idxs.into_iter().zip(inner_results) {
                results[key_idx] = inner_result;
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let store = self.get_store(&key);
        store
            .update(key, reader, size_info)
            .await
            .err_tip(|| "In ShardStore::update()")
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let store = self.get_store(&key);
        store
            .get_part(key, writer, offset, length)
            .await
            .err_tip(|| "In ShardStore::get_part()")
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        let Some(key) = key else {
            return self;
        };
        let index = self.get_store_index(&key);
        self.weights_and_stores[index].store.inner_store(Some(key))
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
        // Composite registration is not atomic — see FastSlowStore for the
        // contract notes. If a register fails partway through the shard list,
        // earlier shards retain the callback while later ones don't. The
        // StoreDriver trait has no `unregister_item_callback`, so we cannot
        // roll back. Warn loudly so the operator can detect the asymmetry.
        let total = self.weights_and_stores.len();
        for (idx, store) in self.weights_and_stores.iter().enumerate() {
            if let Err(err) = store.store.register_item_callback(callback.clone()) {
                warn!(
                    ?err,
                    succeeded = idx,
                    total,
                    "ShardStore: register_item_callback failed at shard index {idx} of \
                     {total} — earlier shards have the callback, this and later shards \
                     do not. Trait has no unregister API; restart to recover.",
                );
                return Err(err);
            }
        }
        Ok(())
    }

    /// ShardStore is a multi-inner wrapper. Every shard may participate in
    /// the BIS chain (e.g. when shards are FastSlowStore), so concatenate
    /// drains and merge notifies. The audit (#why-bis-bug-not-caught)
    /// flagged ShardStore as the next time-bomb — this declaration closes
    /// that gap.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        let children: DelegationChildren<'_> = self
            .weights_and_stores
            .iter()
            .map(|sw| sw.store.as_store_driver())
            .collect();
        StableDigestDelegation::Many {
            children,
            merged_state: &self.merged_stable_notify,
        }
    }

    /// Pin requests fan out to every shard. (A digest lives on exactly one
    /// shard but pin_digests has no key affinity at this layer; the OR-
    /// merge in the trait default surfaces success from the right shard.)
    fn pin_delegation(&self) -> PinDelegation<'_> {
        let children: DelegationChildren<'_> = self
            .weights_and_stores
            .iter()
            .map(|sw| sw.store.as_store_driver())
            .collect();
        PinDelegation::Many(children)
    }

    /// ShardStore needs PER-DIGEST routing (each digest lives on exactly
    /// ONE shard, picked via `get_store_index` hash), which the enum's
    /// `Many` arm (broadcast-to-all) cannot express without generating
    /// spurious BIS broadcasts to the worker for digests on other shards.
    /// Declare `Leaf` and override `mark_stable` directly — same pattern
    /// FastSlowStore uses to be the producer leaf for the BIS feeder.
    /// (Task #157 / C+D folded mark_stable into the forced-delegation
    /// enum mechanism; per-digest routers stay as Leaf+override.)
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }

    /// Each input digest lives on exactly ONE shard; route per-digest using
    /// the same `get_store_index` hash that the read/write paths use, so
    /// each shard's mark_stable receives only the digests it actually owns.
    /// (A `Many`-style fan-out would push each digest into every shard's
    /// `stable_digests` queue, generating spurious BIS broadcasts to the
    /// worker for digests on other shards. The per-digest routing here
    /// preserves the existing shard's pin/release identity.)
    /// (#140 / red-team F3.)
    fn mark_stable(&self, digests: &[DigestInfo]) {
        if digests.is_empty() {
            return;
        }
        // Bucket by shard index. Vec<Vec<_>> avoids HashMap overhead at the
        // small N (typical shard counts: 2-8).
        let n = self.weights_and_stores.len();
        let mut buckets: Vec<Vec<DigestInfo>> = (0..n).map(|_| Vec::new()).collect();
        for digest in digests {
            let key = StoreKey::Digest(*digest);
            let idx = self.get_store_index(&key);
            buckets[idx].push(*digest);
        }
        for (idx, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.weights_and_stores[idx].store.mark_stable(&bucket);
            }
        }
    }
}

default_health_status_indicator!(ShardStore);
