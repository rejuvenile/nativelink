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
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::SizePartitioningSpec;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DelegationChildren, ItemCallback, MergedNotifyState, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::join;
use tracing::warn;

#[derive(Debug, MetricsComponent)]
pub struct SizePartitioningStore {
    #[metric(help = "Size to partition our data")]
    partition_size: u64,
    #[metric(group = "lower_store")]
    lower_store: Store,
    #[metric(group = "upper_store")]
    upper_store: Store,
    /// Lazy-initialized merged Notify state for the `Many` BIS chain.
    /// Populated on first call to `stable_notify()` by the trait's default
    /// body. Owns both the merged Notify and the AbortOnDrop forwarder
    /// handles, so dropping the wrapper aborts the spawned forwarders
    /// (closes F2 task leak). Must be owned by the wrapper so subscribers
    /// see the same Notify across repeated calls; not metric'd.
    merged_stable_notify: OnceLock<MergedNotifyState>,
}

impl SizePartitioningStore {
    pub fn new(spec: &SizePartitioningSpec, lower_store: Store, upper_store: Store) -> Arc<Self> {
        let result = Arc::new(Self {
            partition_size: spec.size,
            lower_store,
            upper_store,
            merged_stable_notify: OnceLock::new(),
        });
        // Eagerly initialize the merged-stable-notify forwarders so the
        // cold-path lost-wakeup window between construction and first
        // external `stable_notify()` call is closed (perf-optimizer
        // CRITICAL Finding 1, .claude/reviews/c-plus-d/perf-optimizer.md).
        // Requires a tokio runtime; all production callers and
        // #[nativelink_test] harnesses provide one.
        let _ = StoreDriver::stable_notify(result.as_ref());
        result
    }

    /// Returns the size threshold that partitions blobs between lower and
    /// upper stores. Blobs with `size_bytes < partition_size` go to the
    /// lower store; all others go to the upper store.
    pub fn partition_size(&self) -> u64 {
        self.partition_size
    }

    /// Returns `true` if the merged-stable-notify state has been
    /// eagerly initialized (forwarder tasks running). Exposed for the
    /// production-composition test that verifies cold-path lost-wakeup
    /// safety; not used in production code.
    #[doc(hidden)]
    pub fn merged_stable_notify_initialized(&self) -> bool {
        self.merged_stable_notify.get().is_some()
    }
}

#[async_trait]
impl StoreDriver for SizePartitioningStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        let mut non_digest_sample = None;
        let (lower_digests, upper_digests): (Vec<_>, Vec<_>) =
            keys.iter().map(StoreKey::borrow).partition(|k| {
                let StoreKey::Digest(digest) = k else {
                    non_digest_sample = Some(k.borrow().into_owned());
                    return false;
                };
                digest.size_bytes() < self.partition_size
            });
        if let Some(non_digest) = non_digest_sample {
            return Err(make_input_err!(
                "SizePartitioningStore only supports Digest keys, got {non_digest:?}"
            ));
        }
        let (lower_results, upper_results) = join!(
            self.lower_store.has_many(&lower_digests),
            self.upper_store.has_many(&upper_digests),
        );
        let mut lower_results = match lower_results {
            Ok(lower_results) => lower_results.into_iter(),
            Err(err) => match upper_results {
                Ok(_) => return Err(err),
                Err(upper_err) => return Err(err.merge(upper_err)),
            },
        };
        let mut upper_digests = upper_digests.into_iter().peekable();
        let mut upper_results = upper_results?.into_iter();
        for (digest, result) in keys.iter().zip(results.iter_mut()) {
            if Some(digest) == upper_digests.peek() {
                upper_digests.next();
                *result = upper_results
                    .next()
                    .err_tip(|| "upper_results out of sync with upper_digests")?;
            } else {
                *result = lower_results
                    .next()
                    .err_tip(|| "lower_results out of sync with lower_digests")?;
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
        let digest = match key {
            StoreKey::Digest(digest) => digest,
            other @ StoreKey::Str(_) => {
                return Err(make_input_err!(
                    "SizePartitioningStore only supports Digest keys, got {other:?}"
                ));
            }
        };
        if digest.size_bytes() < self.partition_size {
            return self.lower_store.update(digest, reader, size_info).await;
        }
        self.upper_store.update(digest, reader, size_info).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let digest = match key {
            StoreKey::Digest(digest) => digest,
            other @ StoreKey::Str(_) => {
                return Err(make_input_err!(
                    "SizePartitioningStore only supports Digest keys, got {other:?}"
                ));
            }
        };
        if digest.size_bytes() < self.partition_size {
            return self
                .lower_store
                .get_part(digest, writer, offset, length)
                .await;
        }
        self.upper_store
            .get_part(digest, writer, offset, length)
            .await
    }

    async fn batch_get_part_unchunked(
        self: Pin<&Self>,
        keys: Vec<StoreKey<'_>>,
        length: Option<u64>,
    ) -> Vec<Result<Bytes, Error>> {
        let n = keys.len();
        let mut results: Vec<Result<Bytes, Error>> =
            (0..n).map(|_| Err(make_err!(Code::Internal, "batch slot not filled"))).collect();

        // Partition keys by size threshold into lower/upper batches.
        let mut lower_indices: Vec<usize> = Vec::with_capacity(n);
        let mut lower_keys: Vec<StoreKey<'_>> = Vec::with_capacity(n);
        let mut upper_indices: Vec<usize> = Vec::with_capacity(n);
        let mut upper_keys: Vec<StoreKey<'_>> = Vec::with_capacity(n);

        for (i, key) in keys.iter().enumerate() {
            match key {
                StoreKey::Digest(digest) if digest.size_bytes() < self.partition_size => {
                    lower_indices.push(i);
                    lower_keys.push(key.borrow());
                }
                StoreKey::Digest(_) => {
                    upper_indices.push(i);
                    upper_keys.push(key.borrow());
                }
                other => {
                    results[i] = Err(make_input_err!(
                        "SizePartitioningStore only supports Digest keys, got {other:?}"
                    ));
                }
            }
        }

        let (lower_results, upper_results) = join!(
            async {
                if lower_keys.is_empty() {
                    Vec::new()
                } else {
                    Pin::new(self.lower_store.as_store_driver())
                        .batch_get_part_unchunked(lower_keys, length)
                        .await
                }
            },
            async {
                if upper_keys.is_empty() {
                    Vec::new()
                } else {
                    Pin::new(self.upper_store.as_store_driver())
                        .batch_get_part_unchunked(upper_keys, length)
                        .await
                }
            },
        );

        for (slot, result) in lower_indices.into_iter().zip(lower_results) {
            results[slot] = result;
        }
        for (slot, result) in upper_indices.into_iter().zip(upper_results) {
            results[slot] = result;
        }

        results
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        let Some(key) = key else {
            return self;
        };
        let StoreKey::Digest(digest) = key else {
            return self;
        };
        if digest.size_bytes() < self.partition_size {
            return self.lower_store.inner_store(Some(digest));
        }
        self.upper_store.inner_store(Some(digest))
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
        // Composite registration is not atomic — if the second register fails
        // after the first succeeds, the inner stores are asymmetric. The
        // StoreDriver trait has no `unregister_item_callback`; we cannot roll
        // back. Most impls are infallible, so this rarely fires; if it does,
        // log loudly so the operator can detect the asymmetry.
        self.lower_store
            .register_item_callback(callback.clone())?;
        if let Err(err) = self.upper_store.register_item_callback(callback) {
            warn!(
                ?err,
                "SizePartitioningStore: upper_store register_item_callback failed AFTER \
                 lower_store succeeded — composite is in an asymmetric state. Trait has \
                 no unregister API; restart to recover."
            );
            return Err(err);
        }
        Ok(())
    }

    /// SizePartitioningStore is a multi-inner wrapper. The trait's default
    /// `drain_stable_digests` will concatenate drains from BOTH inners and
    /// `stable_notify` will lazy-build a merged Notify woken when EITHER
    /// inner's notify fires (using `merged_stable_notify` field).
    ///
    /// **This is the single load-bearing fix from the BIS broadcast wedge
    /// audit (`.claude/reviews/why-bis-bug-not-caught/audit.md`).** Before
    /// this declaration the trait silently inherited noop defaults and the
    /// production `cas_STORE` chain dropped 458K+ digests/day for 31 days.
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        let mut children = DelegationChildren::new();
        children.push(self.lower_store.as_store_driver());
        children.push(self.upper_store.as_store_driver());
        StableDigestDelegation::Many {
            children,
            merged_state: &self.merged_stable_notify,
        }
    }

    /// Pin requests fan out to both inner stores. (A digest above the
    /// partition lives only in the upper store; below, only in the lower
    /// — but pin_digests sends to both inner stores' `pin_digests` and
    /// each inner self-routes per its own delegation. The "wrong" inner's
    /// FilesystemStore reports `false` for absent digests via
    /// `pin_digests_with_results`; the OR-merge in the trait default
    /// surfaces the success.)
    fn pin_delegation(&self) -> PinDelegation<'_> {
        let mut children = DelegationChildren::new();
        children.push(self.lower_store.as_store_driver());
        children.push(self.upper_store.as_store_driver());
        PinDelegation::Many(children)
    }

    /// Route each digest to the inner store that owns it (by size partition)
    /// so the digest lands in the correct stable-digests queue downstream.
    /// If the inner store is itself a `FastSlowStore` it will push into its
    /// own `stable_digests`; the BIS broadcast loop's drain visits each
    /// terminal CAS store directly.
    ///
    /// `mark_stable` is not yet covered by the C+D enum dispatch (task #157);
    /// `drain_stable_digests` / `stable_notify` ARE (via `stable_delegation`
    /// above). Once #157 lands and the per-digest size-routing question is
    /// resolved, this override can collapse into the enum.
    fn mark_stable(&self, digests: &[DigestInfo]) {
        if digests.is_empty() {
            return;
        }
        let mut lower: Vec<DigestInfo> = Vec::new();
        let mut upper: Vec<DigestInfo> = Vec::new();
        for digest in digests {
            if digest.size_bytes() < self.partition_size {
                lower.push(*digest);
            } else {
                upper.push(*digest);
            }
        }
        if !lower.is_empty() {
            self.lower_store.mark_stable(&lower);
        }
        if !upper.is_empty() {
            self.upper_store.mark_stable(&upper);
        }
    }
}

default_health_status_indicator!(SizePartitioningStore);
