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
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, error, info, trace};

use nativelink_config::stores::{EvictionPolicy, ExistenceCacheSpec};
use nativelink_util::metrics_utils::CounterWithTime;

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
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use nativelink_util::o11_probes::ecs_hit_counters;
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
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

/// Possible causes of a stale positive observed on a WRITE path
/// (`update` / `update_oneshot`). Emitted as a structured `causes`
/// field on the `debug!` log so the prose lives in one place instead
/// of being duplicated per call site (and silently drifting). Three
/// distinct mechanisms; an operator who sees this log walks through
/// them to figure out which one fired (note that under the
/// worker-mirror durability protocol the cache=Some + inner=NotFound
/// state is EXPECTED for blobs that live only on peer workers — most
/// production occurrences are healthy mirror-tier consultations, not
/// contract violations):
///   (a) the inner store lost the blob after the cache observed it
///       (eviction, OOM kill mid-write, on-disk corruption),
///   (b) the cache was populated by a code path that did not verify
///       the inner store's `has()` (e.g. stale-positive propagated
///       upward from a downstream `has` call),
///   (c) moka's async eviction callback hasn't run yet — the
///       canonical race-window described above the `update()` site.
const STALE_POSITIVE_CAUSES_WRITE: &str =
    "(a) inner-store data loss after cache population, \
     (b) cache populated by a path other than a verified successful update, \
     (c) eviction race where moka's async eviction callback hasn't fired \
     yet to remove the entry";

/// Possible causes of a stale positive observed on a READ path
/// (`get_part` / `batch_get_part_unchunked`). Same three mechanisms as
/// the WRITE form but with the inner-store-data-loss bucket
/// elaborated (read paths have richer signal — we know the inner
/// store actively refused, not just that it claimed not to have the
/// blob). Operators correlating production stale positives between
/// read and write paths should expect the same root causes; the
/// extra detail in (a) is operational hint, not a different class.
const STALE_POSITIVE_CAUSES_READ: &str =
    "(a) inner-store data loss after cache population (eviction race / \
     OOM-killed mid-write / disk corruption), \
     (b) cache populated by a path other than a verified successful update \
     (has() return value trusted blindly), \
     (c) inner store's has() returned a stale positive itself \
     (e.g. cached existence beneath a missing blob on disk)";

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
    /// Set to `true` when `inner_store.register_item_callback` failed
    /// at construction (e.g. wrapping a Redis store with
    /// `enable_keyspace_notifications=false`, or any other inner store
    /// that returned `Err(Code::FailedPrecondition | Code::Internal)`
    /// from `register_item_callback`). Stale positives become
    /// self-correcting on the read path (`get_part` removes on
    /// `NotFound`) and on the write path (`update` bypasses the cache
    /// to check the inner store), so the cache stays correct under
    /// load — but the eager invalidation that the callback provides
    /// is gone, which expands the stale-positive window during inner
    /// store evictions. Operator-visible via metric +
    /// loud-startup-`error!`. RECONSIDER fix: prior behavior was
    /// `.expect()` panic at construction, which converted any
    /// inner-store hiccup at boot into a systemd restart loop —
    /// hostile to operator-driven flag-flips. CLAUDE.md "Never panic
    /// in library code" + "Mechanism, not operator" demand a
    /// recoverable signal.
    /// #11 (2026-06-10): the metric description was previously "1 if
    /// register_item_callback failed at construction", which described the
    /// wrong condition (registration failure is a post-gate asymmetry, not
    /// what latches this field). The actual meaning: construction-time
    /// `supports_removal_callbacks()==false` → eager invalidation disabled.
    /// "register_item_callback failed" is a different (post-gate) failure
    /// path that doesn't set this field (see the error! log at lines 291-306).
    #[metric(help = "1 if supports_removal_callbacks()==false at construction; \
                     eager invalidation disabled — stale positives self-correct \
                     via get_part/update bypass but the stale-positive window \
                     expands to the next read/write touch. To fix: ensure the \
                     inner store supports callbacks (RedisStore: set \
                     enable_keyspace_notifications=true).")]
    vulnerable_mode: bool,

    /// #11 Item 3 (2026-06-10): counts eviction callbacks that actually fired
    /// from the inner store and removed an existence cache entry. Operator
    /// signal: "callbacks registered but fired==0 over a long window" means
    /// the callback chain is dead (registered but never fires — possible when
    /// inner store reports support but keyspace notifications are misconfigured
    /// at the server level). Zero fired over a long window with active writes
    /// is the dead-chain signal.
    #[metric(help = "eviction callbacks fired from any registered tier (fast-tier LRU \
                     churn AND slow-tier evictions both increment this counter); \
                     nonzero does NOT prove the slow-tier chain is alive — fast-tier \
                     evictions fire callbacks even when slow tier still holds the blob; \
                     zero-over-a-churn-window with active writes is the meaningful \
                     dead-chain signal")]
    invalidation_callbacks_fired: CounterWithTime,

    /// When `true`, fire `info!` for every `NotFound` returned to the
    /// caller (inner_has slot=None + get_part Err path). Operator-set
    /// per-instance via `ExistenceCacheSpec.log_not_found_at_info`.
    /// Intended ONLY for AC instances; CAS-side leaves at false to
    /// avoid the FindMissingBlobs burst class documented on the spec.
    log_not_found_at_info: bool,
}

impl ExistenceCacheStore<SystemTime> {
    pub fn new(spec: &ExistenceCacheSpec, inner_store: Store) -> Arc<Self> {
        Self::new_with_time(spec, inner_store, SystemTime::now())
    }
}

impl<I: InstantWrapper> ItemCallback for ExistenceCacheStore<I> {
    // (#locality-map-drift) `_ts_*` unused: the existence cache is not a
    // holdings tracker; it only invalidates a cached-existence entry on evict.
    fn callback<'a>(
        &'a self,
        store_key: StoreKey<'a>,
        _ts_boot_epoch: u64,
        _ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let digest = store_key.borrow().into_digest();
        debug!(%digest, "ExistenceCacheStore: eviction callback received");
        Box::pin(async move {
            if debug_digest_match(&digest) {
                info!(?digest, source = "callback_inner_eviction", "DEBUG: ExistenceCacheStore removing wedge digest");
            }
            let deleted_key = self.existence_cache.remove(&digest).await;
            if deleted_key {
                // #11 Item 3 (2026-06-10): count callbacks that actually removed an
                // entry. fired==0 over a long window with active writes is the
                // operator's dead-chain signal (see metric help text).
                self.invalidation_callbacks_fired.inc();
                debug!(%digest, "ExistenceCacheStore: eviction callback removed key from cache");
            } else {
                debug!(%digest, "ExistenceCacheStore: eviction callback key not in cache (already removed or never cached)");
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
        ts_boot_epoch: u64,
        ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let cache = self.cache.upgrade();
        if let Some(local_cache) = cache {
            // Always fire callbacks immediately — removing a digest from
            // the existence cache is cheap and idempotent. The update()
            // path re-inserts after a successful write, so a concurrent
            // eviction callback cannot create a stale positive.
            let store_key = store_key.into_owned();
            return Box::pin(async move {
                local_cache.callback(store_key, ts_boot_epoch, ts_counter).await;
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

    /// Test accessor for the `vulnerable_mode` flag.
    ///
    /// Guards the #9 invariant: an ECS over a ref-wrapped registered backend
    /// must NOT enter vulnerable_mode. Production code observes this via the
    /// metric; tests need a direct accessor to assert the construction-time
    /// state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_vulnerable_mode(&self) -> bool {
        self.vulnerable_mode
    }

    /// Test accessor for the `invalidation_callbacks_fired` counter.
    ///
    /// Guards the #11 Item 3 invariant: callbacks registered via
    /// `register_item_callback` must actually fire when the inner store
    /// evicts an entry. `fired==0` over a long window with active writes
    /// is the dead-chain signal. Production code observes via the metric;
    /// tests assert the counter directly.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn invalidation_callbacks_fired(&self) -> u64 {
        self.invalidation_callbacks_fired
            .counter
            .load(std::sync::atomic::Ordering::Relaxed)
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
        // Pre-flight: ask the inner store whether it can accept removal
        // callbacks at all. If not, we still construct ECS but in
        // `vulnerable_mode` — the prior behavior (`.expect("Register item
        // callback should work")`) converted an operator-controllable
        // misconfiguration (e.g. RedisStore with
        // `enable_keyspace_notifications=false`, or cluster-mode forced
        // disable) into a systemd restart loop. CLAUDE.md "Never panic in
        // library code" + "Mechanism, not operator" demand a recoverable
        // signal; the operator who flips a config flag must not get a
        // boot-time hard failure when a graceful degradation is correct.
        //
        // Stale positives in vulnerable_mode are self-correcting on both
        // hot paths: `get_part` removes the cache entry on `NotFound`,
        // and `update` bypasses the cache when checking the inner store.
        // The eager invalidation from `register_item_callback` is what's
        // missing — the stale-positive window expands from "fired
        // immediately" to "next read/write touch."
        //
        // #9 RefStore-resolution asymmetry: `RefStore::supports_removal_callbacks`
        // deliberately returns `false` for an unresolved cell (commit cd980946,
        // #367) — this fires the operator-visible warn for genuinely-unregistered
        // refs. For the AC_INNER case the inner IS a registered RefStore whose
        // cell happens to be empty at construction time (store_factory wires the
        // StoreManager entries sequentially, so the ECS constructor runs before
        // `get_store()` has been called). Calling `inner_store(None)` here
        // triggers `RefStore::get_store()`, populating the cell; the subsequent
        // `supports_removal_callbacks()` then queries the resolved inner and
        // returns its actual value (true for MemoryStore/FilesystemStore/Redis-
        // with-notifications). If the ref target is NOT registered (missing name),
        // `get_store()` returns Err, `inner_store` returns `self` (the RefStore
        // itself), and `supports_removal_callbacks()` still returns `false` for
        // the unresolved cell — preserving cd980946's graceful-degradation
        // semantics (no panic, vulnerable_mode=true, loud error! at boot).
        let _resolved = inner_store.inner_store(None::<StoreKey<'_>>);
        let supports_callbacks = inner_store.supports_removal_callbacks();
        let existence_cache_store = Arc::new(Self {
            inner_store,
            existence_cache,
            vulnerable_mode: !supports_callbacks,
            invalidation_callbacks_fired: CounterWithTime::default(),
            log_not_found_at_info: spec.log_not_found_at_info,
        });
        if supports_callbacks {
            let weak_ref = Arc::downgrade(&existence_cache_store);
            if let Err(err) = existence_cache_store
                .inner_store
                .register_item_callback(Arc::new(ExistenceCacheCallback { cache: weak_ref }))
            {
                // Asymmetric: inner reported `supports_removal_callbacks=true`
                // but `register_item_callback` rejected. This is a contract
                // bug in the inner store. Log loudly; we cannot mutate
                // vulnerable_mode through the Arc now (other callers may
                // have cloned), so the metric will under-report — but the
                // operator-actionable signal is the error line.
                error!(
                    ?err,
                    "ExistenceCacheStore: inner_store reports supports_removal_callbacks=true \
                     but register_item_callback returned Err. This is a contract bug in the \
                     inner store impl. ExistenceCacheStore is effectively in vulnerable_mode \
                     (no eager invalidation) but the metric will report vulnerable_mode=false \
                     because the flag was set before this attempt. Stale-positive entries \
                     remain self-correcting via get_part/update bypass."
                );
            }
        } else {
            error!(
                "ExistenceCacheStore: inner_store.supports_removal_callbacks()=false at \
                 construction; entering vulnerable_mode. Stale positives are still \
                 self-correcting (get_part removes on NotFound; update bypasses cache), \
                 but eager invalidation from inner-store evictions is GONE — the \
                 stale-positive window expands to the size of the eviction-detection \
                 latency on the read/write paths. To restore eager invalidation: ensure \
                 the inner store supports register_item_callback (RedisStore: set \
                 enable_keyspace_notifications=true and grant the +config ACL; cluster \
                 mode forces this off and is incompatible with ExistenceCacheStore's \
                 eager-invalidation path today)."
            );
        }
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

    /// Shared skip-gate probe for the write paths (`update` /
    /// `update_oneshot`). Decides whether an incoming write may be SKIPPED
    /// because the blob is already DURABLY present in the inner store.
    ///
    /// FL-688 OPT-1 (2026-06-26): this probes `inner_store.has_durably` —
    /// the slow-tier-only query — NOT `has_with_results`. A blob present
    /// only in a RAM tier (`FastSlowStore` fast tier / `in_flight_slow_writes`
    /// / the RAM-only `mirror_blobs` map) is NOT durable, so it MUST NOT be
    /// skipped: the write has to flow through to `inner_store.update`, whose
    /// `FastSlowStore` normal path spawns the background slow-write that lands
    /// the durable copy. Probing `has_with_results` (RAM-INCLUSIVE) was the
    /// backfill non-convergence defect — a re-uploaded pinned-mirror blob hit
    /// the skip branch, drained + `Ok`'d without writing the durable tier, and
    /// `has_durably` stayed `None` so the worker-API pull feed re-solicited
    /// the upload forever.
    ///
    /// This intentionally bypasses the moka existence cache (it queries the
    /// inner store directly via the `Inner` durable-delegation route): the
    /// cache only records "the inner store has this blob" regardless of tier,
    /// so a cache hit is not proof of durability. `has_durably ⊆
    /// has_with_results`, so every stale-positive the old `has_with_results`
    /// bypass healed is still healed (cache-yes / inner-evicted), plus the
    /// fast-only / not-yet-durable case now heals — strictly safer.
    ///
    /// Returns `Some(size)` when the blob is durably present (caller skips +
    /// drains + refreshes the cache); `None` when the write must flow through
    /// (caller runs the stale-positive heal then writes).
    ///
    /// Extracting this into one helper called by BOTH write paths keeps the
    /// RAM-vs-durable skip semantics from re-diverging between `update` and
    /// `update_oneshot` (the two sites that previously carried the identical
    /// gate, where the `update_oneshot` copy is live via `BatchUpdateBlobs`).
    async fn should_skip_for_durable_presence(
        self: Pin<&Self>,
        digest: &DigestInfo,
    ) -> Result<Option<u64>, Error> {
        let mut durable = [None];
        self.inner_store
            .has_durably(&[(*digest).into()], &mut durable)
            .await
            .err_tip(|| "In ExistenceCacheStore::should_skip_for_durable_presence")?;
        Ok(durable[0])
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

        // Record per-key hit/miss counts: keys NOT in not_cached_keys were
        // served from the moka cache (hits); keys IN not_cached_keys missed.
        let miss_count = not_cached_keys.len() as u64;
        let hit_count = keys.len() as u64 - miss_count;
        ecs_hit_counters().record_hits(hit_count);
        ecs_hit_counters().record_misses(miss_count);

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
                let digest = key.borrow().into_digest();
                if let Some(size) = result {
                    if debug_digest_match(&digest) {
                        info!(?digest, size = *size, source = "inner_has_with_results", "DEBUG: ExistenceCacheStore inserting wedge digest (inner.has returned Some)");
                    }
                    inserts.push((digest, ExistenceItem(*size)));
                } else if self.log_not_found_at_info {
                    // Inner store reported NotFound; ECS will pass the
                    // None up to its caller. Gated behind the per-instance
                    // `log_not_found_at_info` flag (true only on AC
                    // instances per the spec doc-comment) so the CAS
                    // path stays silent under FindMissingBlobs bursts.
                    info!(
                        ?digest,
                        "ExistenceCacheStore: inner store reported NotFound; returning None to caller",
                    );
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
    /// Remove the entry: clear the moka existence cache then delegate to
    /// the inner store (#40 §2 delete-on-detection). Clearing the moka
    /// entry first prevents a brief window where `has_with_results` would
    /// return a stale positive from the in-process cache while the inner
    /// store's `remove` is in flight.
    ///
    /// Note: between the moka clear (step 1) and the inner-store delete
    /// (step 2) a concurrent `has_with_results` can re-populate moka with
    /// a stale positive. For the Redis-backed production AC chain the
    /// Valkey keyspace `DEL` event fires a second `existence_cache.remove`
    /// callback (~ms later), self-correcting the stale positive. For
    /// `MemoryStore` slow-tier configurations (tests, non-production), no
    /// keyspace event fires; the stale positive persists until TTL expiry
    /// or the `get_part` `NotFound` self-correction path clears it. A
    /// double-`remove` on the moka cache is a benign no-op.
    async fn remove(self: Pin<&Self>, key: StoreKey<'_>) -> Result<(), Error> {
        let digest = key.into_digest();
        self.existence_cache.remove(&digest).await;
        self.inner_store.remove(StoreKey::from(digest)).await
    }

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
        //
        // FL-688 OPT-1: the skip-gate probes DURABLE presence (has_durably,
        // slow-tier-only) — NOT has_with_results (RAM-inclusive). A blob held
        // only in a RAM tier (mirror_blobs / fast / in-flight) is NOT durable
        // and MUST flow through so the FastSlowStore background slow-write
        // forms the durable copy. See should_skip_for_durable_presence.
        // (Probe #6) Wall-clock for the bypass-cache inner has-check.
        // Surfaces in the existing slow-log paths below so we can
        // separate inner-has latency from inner-update latency when
        // the update step shows up as slow.
        let inner_has_start = Instant::now();
        let durable = self
            .should_skip_for_durable_presence(&digest)
            .await
            .err_tip(|| "In ExistenceCacheStore::update")?;
        let existence_cache_inner_has_elapsed_us =
            inner_has_start.elapsed().as_micros() as u64;
        if let Some(durable_size) = durable {
            // Blob is already DURABLY present in the inner store — safe to skip.
            reader
                .drain()
                .await
                .err_tip(|| "In ExistenceCacheStore::update")?;
            // Refresh the existence cache since we verified it exists.
            if debug_digest_match(&digest) {
                info!(?digest, size = durable_size, source = "update_refresh_after_inner_has_some", "DEBUG: ExistenceCacheStore inserting wedge digest (update path: inner says durably present)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(durable_size))
                .await;
            return Ok(());
        }
        // If the existence cache had a stale entry, remove it now.
        if debug_digest_match(&digest) {
            info!(?digest, source = "update_remove_stale", "DEBUG: ExistenceCacheStore removing wedge digest (update path: inner.has=None)");
        }
        // Peek-then-remove pattern: the cache size tells operators what
        // the cache had been claiming about this blob. Stale-positive
        // here means cache said "yes I have it (size=N)" but inner.has()
        // said "no I don't" — caller is now re-uploading to fix it.
        // Even though the upload heals the user-visible symptom, the
        // underlying invariant violation must surface so the systemic
        // cause gets fixed (otherwise repeated cache lies waste
        // bandwidth re-uploading the same blob).
        //
        // CANONICAL RACE-WINDOW NOTE (referenced by sibling sites in
        // `update_oneshot` ~line 426, `get_part` ~line 537, and
        // `batch_get_part_unchunked` ~line 621):
        // `size_for_key()` and `remove()` are separate awaits with no
        // lock held between them; under concurrent eviction (moka's
        // background eviction task removing the entry first) the
        // `remove()` returns `removed=true` for the race-loser path
        // while `size_for_key()` may have already observed `None`.
        // The error message lists "eviction race" as one of the
        // possible causes for exactly this reason — operators seeing
        // `prior_size=None` should treat it as "cache claimed presence
        // but the size was lost to a concurrent eviction" rather than
        // "the cache never had a size for this digest".
        let prior_size = self.existence_cache.size_for_key(&digest).await;
        let removed = self.existence_cache.remove(&digest).await;
        if removed {
            // Demoted from error: under the worker-mirror durability
            // protocol, EC.cache=Some + inner=NotFound is the EXPECTED
            // state when the blob lives only on peer workers (caller is
            // re-uploading harmlessly because the server's local inner
            // tier doesn't have it). Real durability gaps surface via
            // the upstream caller's NotFound chain ("not found in either
            // fast or slow store"), NL_REDIRECT exhaustion, or Bazel's
            // own retry-exhaustion errors — not here. Kept at debug for
            // operator drill-down.
            debug!(
                %digest,
                ?prior_size,
                causes = STALE_POSITIVE_CAUSES_WRITE,
                "existence cache stale positive (update path): cache claimed \
                 blob present but inner store's has() returned None; removed \
                 stale entry and will re-upload",
            );
        }
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
            // FU-6: intentional best-effort cache fan-out abandonments
            // (WorkerProxyStore mpsc full or Bazel consumer disconnected)
            // arrive as Code::Aborted with CACHE_FANOUT_ABANDONED_MARKER.
            // The Bazel read already succeeded; the blob is durable. Demote
            // to debug! to avoid misleading error!. Genuine inner-store
            // write failures (disk error, any other code/message) keep error!.
            if crate::worker_proxy_store::is_cache_fanout_abandonment(err) {
                debug!(
                    ?digest,
                    elapsed_ms,
                    existence_cache_inner_has_elapsed_us,
                    ?err,
                    "ExistenceCacheStore::update: cache fan-out abandoned \
                     (best-effort, not a genuine failure — FU-6)",
                );
            } else {
                error!(
                    ?digest,
                    elapsed_ms,
                    existence_cache_inner_has_elapsed_us,
                    ?err,
                    "ExistenceCacheStore::update: inner store write failed",
                );
            }
        } else if elapsed_ms > 100 {
            info!(
                ?digest,
                elapsed_ms,
                existence_cache_inner_has_elapsed_us,
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
        //
        // FL-688 OPT-1: the skip-gate probes DURABLE presence (has_durably,
        // slow-tier-only), NOT has_with_results (RAM-inclusive). Identical
        // gate to update(); this site is LIVE in production via
        // BatchUpdateBlobs (cas_server is_mirror=false). See
        // should_skip_for_durable_presence.
        let durable = self
            .should_skip_for_durable_presence(&digest)
            .await
            .err_tip(|| "In ExistenceCacheStore::update_oneshot")?;
        if let Some(durable_size) = durable {
            // Blob is already DURABLY present in the inner store — safe to skip.
            if debug_digest_match(&digest) {
                info!(?digest, size = durable_size, source = "update_oneshot_refresh_after_inner_has_some", "DEBUG: ExistenceCacheStore inserting wedge digest (update_oneshot path: inner says durably present)");
            }
            let _ = self
                .existence_cache
                .insert(digest, ExistenceItem(durable_size))
                .await;
            return Ok(());
        }
        // If the existence cache had a stale entry, remove it now.
        if debug_digest_match(&digest) {
            info!(?digest, source = "update_oneshot_remove_stale", "DEBUG: ExistenceCacheStore removing wedge digest (update_oneshot path: inner.has=None)");
        }
        // Mirror the update() path's stale-positive logging contract.
        // Race-window: see canonical note above the `update()` site
        // (~line 326). `size_for_key()` may observe `None` if a
        // concurrent eviction removed the entry between this peek and
        // the `remove()` below.
        let prior_size = self.existence_cache.size_for_key(&digest).await;
        let removed = self.existence_cache.remove(&digest).await;
        if removed {
            // Demoted from error: see canonical rationale above the
            // sibling `update` path log site (~line 376). Same
            // worker-mirror durability semantics — the blob may live
            // only on peer workers; caller will re-upload harmlessly.
            debug!(
                %digest,
                ?prior_size,
                causes = STALE_POSITIVE_CAUSES_WRITE,
                "existence cache stale positive (update_oneshot path): cache \
                 claimed blob present but inner store's has() returned None; \
                 removed stale entry and will re-upload",
            );
        }

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
                // Peek the prior cached size BEFORE removing so the
                // error log can report what the cache claimed. A stale
                // positive == cache had a sized entry but inner store
                // can't deliver it. If the cache had no entry to begin
                // with this isn't a stale positive, just an honest
                // NotFound from a never-cached digest — log nothing.
                // Race-window: see canonical note above the `update()`
                // site (~line 326). `size_for_key()` may observe `None`
                // if a concurrent eviction removed the entry between
                // this peek and the `remove()` below.
                let prior_size = self.existence_cache.size_for_key(&digest).await;
                let removed = self.existence_cache.remove(&digest).await;
                if removed {
                    // Demoted from error: under the worker-mirror
                    // durability protocol, EC.cache=Some + inner=NotFound
                    // is the EXPECTED state when the blob lives only on
                    // peer workers — the bytestream caller responds with
                    // NL_REDIRECT and the client retries via peer-fetch
                    // (typically delivered in 1-31 ms). Real durability
                    // gaps surface via the upstream caller's NotFound
                    // chain ("not found in either fast or slow store"),
                    // NL_REDIRECT exhaustion, or Bazel's own
                    // retry-exhaustion errors — not here. The 14,776 /
                    // 24h events seen on buildcache 2026-04-26 were almost
                    // entirely healthy mirror-tier consultations. Kept
                    // at debug for operator drill-down.
                    debug!(
                        %digest,
                        ?prior_size,
                        inner_code = ?err.code,
                        ?err,
                        causes = STALE_POSITIVE_CAUSES_READ,
                        "existence cache stale positive (get_part path): \
                         cache claimed blob present but inner store returned \
                         unrecoverable error on read; removed stale entry",
                    );
                }
                if debug_digest_match(&digest) {
                    info!(?digest, code = ?err.code, source = "get_part_remove_unrecoverable", "DEBUG: ExistenceCacheStore POST-remove wedge digest (cache.remove returned, about to return Err to caller)");
                }
            }
            Err(_) => {}
        }
        // Log every NotFound we return to the caller, gated behind the
        // per-instance `log_not_found_at_info` flag (true only on AC
        // instances per the spec doc-comment). The CAS side stays silent
        // — the `existence_cache_eviction_codes_test.rs::
        // get_part_not_found_does_not_log_for_never_cached_digest`
        // contract test covers the volume case (1k-10k/sec bursts during
        // FindMissingBlobs sweeps), and that test runs with the flag
        // defaulted to `false`.
        //
        // The debug! stale-positive log above only fires for the
        // EC.cache=Some + inner=NotFound subcase; this catches the
        // EC.cache=None + inner=NotFound case as well, which is the
        // common "we genuinely don't have it" path.
        if self.log_not_found_at_info {
            if let Err(ref err) = result {
                if err.code == Code::NotFound {
                    debug!(
                        ?digest,
                        "ExistenceCacheStore::get_part: returning NotFound to caller",
                    );
                }
            }
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
        // Carry both the digest and the inner-store error code so the
        // stale-positive log on the removal pass has the same context as
        // the per-digest `get_part` site (digest, prior cached size,
        // inner code). Without the code carried through, the operator
        // sees "stale positive" with no hint of WHY the inner store
        // refused (NotFound vs DataLoss vs Internal vs OutOfRange).
        let mut removals: Vec<(DigestInfo, Code, Error)> = Vec::new();
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
                    removals.push((*digest, err.code, err.clone()));
                }
                Err(_) => {}
            }
        }
        if !inserts.is_empty() {
            drop(self.existence_cache.insert_many(inserts).await);
        }
        for (digest, inner_code, err) in removals {
            // Mirror the per-digest get_part site's stale-positive
            // logging contract — peek the prior cached size, remove,
            // log error! only if a stale entry actually existed.
            // Race-window: see canonical note above the `update()` site
            // (~line 326). `size_for_key()` may observe `None` if a
            // concurrent eviction removed the entry between this peek
            // and the `remove()` below.
            let prior_size = self.existence_cache.size_for_key(&digest).await;
            let removed = self.existence_cache.remove(&digest).await;
            if removed {
                // Demoted from error: see canonical rationale above the
                // sibling `get_part` path log site (~line 599). Same
                // worker-mirror durability semantics apply to the batch
                // read path — most events are healthy mirror-tier
                // consultations, not contract violations.
                debug!(
                    %digest,
                    ?prior_size,
                    ?inner_code,
                    ?err,
                    causes = STALE_POSITIVE_CAUSES_READ,
                    "existence cache stale positive (batch_get_part path): \
                     cache claimed blob present but inner store returned \
                     unrecoverable error on read; removed stale entry",
                );
            }
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

    /// `mark_stable` forwards unchanged to `inner_store` via the trait
    /// default's `Inner` arm (task #157 / C+D folded mark_stable into the
    /// forced-delegation enum mechanism).
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner_store.as_store_driver())
    }

    /// `has_durably` forwards to `inner_store` (`Inner`) — it MUST NOT be
    /// satisfied from the existence cache. The cache records "the inner
    /// store has this blob" regardless of which tier holds it (fast or
    /// slow), so a cache hit is NOT proof of durability. The `Inner` route
    /// bypasses the cache (the trait default dispatches straight to
    /// `inner_store.has_durably`), so the durable-presence query reaches the
    /// FastSlowStore boundary undistorted. (durability-ack v3 §3.0.)
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner_store.as_store_driver())
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
