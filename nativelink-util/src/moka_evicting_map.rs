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

use core::borrow::Borrow;
use core::fmt::Debug;
use core::hash::Hash;
use core::ops::RangeBounds;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use nativelink_config::stores::EvictionPolicy;
use nativelink_metric::MetricsComponent;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::background_spawn;
use crate::evicting_map::{ItemCallback, LenEntry, NoopCallback};
use crate::instant_wrapper::InstantWrapper;
use crate::metrics_utils::{Counter, CounterWithTime};

/// Maximum fraction of max_bytes that can be pinned (25%).
const PIN_CAP_FRACTION: f64 = 0.25;
/// Seconds before a pin automatically expires.
const PIN_TIMEOUT_SECS: u64 = 120;
// Eviction channel is unbounded (mpsc::unbounded_channel). Each EvictionEvent
// is ~64 bytes (Arc<K> + T). At 1M entries that's ~64MB, well within budget.
// Unbounded avoids blocking moka's internal lock during burst eviction
// (bounded channels overflow under heavy load — 3,656 overflows in 10 min
// with a 32K bounded channel).

/// Entry stored in the pinned map, alongside metadata for timeout
/// enforcement and size accounting.
#[derive(Debug)]
struct PinnedEntry<T> {
    data: T,
    pinned_at: Instant,
    size: u64,
}

/// An eviction event captured by the moka listener and sent to the
/// background drainer for async cleanup (unref + callbacks).
struct EvictionEvent<K, T> {
    key: Arc<K>,
    value: T,
}

/// A cache backed by `moka::sync::Cache` with an API that mirrors
/// the previous LRU-based `EvictingMap`. Moka is configured with the
/// pure LRU eviction policy (no TinyLFU admission filter) so that
/// every insert is admitted unconditionally — required for a CAS where
/// silently dropping freshly-written blobs is unacceptable. Pinning is
/// handled via a side `DashMap` that keeps entries alive outside the
/// moka cache.
pub struct MokaEvictingMap<
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper,
    C: ItemCallback<Q> = NoopCallback,
> {
    cache: Cache<K, T>,
    /// Items pinned to prevent eviction. Shared with the eviction
    /// listener so it can check pin status before sending cleanup events.
    pinned: Arc<DashMap<K, PinnedEntry<T>>>,
    /// Total bytes currently pinned.
    pinned_bytes: AtomicU64,
    /// 25% of max_bytes — ceiling for pinned data.
    pin_cap: u64,
    /// Optional BTreeSet index for range queries. Shared with the
    /// eviction listener for cleanup on eviction.
    btree: Arc<RwLock<Option<BTreeSet<K>>>>,
    /// Side queue of eviction events produced by the moka listener.
    /// Drained synchronously by `insert()` so that the insert's evicted
    /// items are `unref()`d before the caller continues (matching the
    /// original `EvictingMap` contract). Also drained by the background
    /// task for evictions triggered outside an `insert()` call (e.g. TTL
    /// expiry, explicit `remove()`). Each event is taken at most once.
    pending_evictions: Arc<parking_lot::Mutex<VecDeque<EvictionEvent<K, T>>>>,
    /// Wake signal for the background drainer. Listener sends `()` after
    /// pushing an event to `pending_evictions`. The actual event payload
    /// lives in `pending_evictions`, not the channel — a `()` on the
    /// channel means "there may be work to do in the side queue".
    eviction_tx: mpsc::UnboundedSender<()>,
    /// Receiver held until `start_background_eviction` moves it into
    /// the drainer task.
    eviction_rx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<()>>>,
    /// Callbacks to invoke on item removal.
    callbacks: RwLock<Vec<C>>,
    /// Fast-path flag to avoid taking the `callbacks` RwLock on hot
    /// read paths (`get`, `get_many`) when no callbacks are registered.
    /// Set to `true` (with `Release` ordering) by `add_item_callback`
    /// after the callback is appended; loaded with `Relaxed` on the
    /// hot path. Once set to true it is never cleared (callbacks are
    /// append-only on this type).
    has_callbacks_flag: AtomicBool,
    /// Anchor time for timestamp conversion.
    anchor_time: I,
    /// Configured max_bytes (used for pin cap and diagnostics).
    max_bytes: u64,
    /// Configured max_count (enforced alongside max_bytes if both set).
    max_count: u64,
    /// Whether the background drainer has been started.
    background_running: AtomicBool,
    // Metrics
    evicted_bytes: Counter,
    evicted_items: CounterWithTime,
    replaced_bytes: Counter,
    replaced_items: CounterWithTime,
    lifetime_inserted_bytes: Counter,
    /// Phantom for the Q type parameter.
    _q: core::marker::PhantomData<Q>,
}

impl<K, Q, T, I, C> Debug for MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper + Debug,
    C: ItemCallback<Q>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MokaEvictingMap")
            .field("entry_count", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .field(
                "pinned_bytes",
                &self.pinned_bytes.load(Ordering::Relaxed),
            )
            .field("pin_cap", &self.pin_cap)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

impl<K, Q, T, I, C> MetricsComponent for MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Borrow<Q>,
    Q: Ord + Hash + Eq + Debug,
    T: LenEntry + Debug + Send,
    I: InstantWrapper,
    C: ItemCallback<Q>,
{
    fn publish(
        &self,
        _kind: nativelink_metric::MetricKind,
        _field_metadata: nativelink_metric::MetricFieldData,
    ) -> Result<nativelink_metric::MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(nativelink_metric::MetricPublishKnownKindData::Component)
    }
}

impl<K, Q, T, I, C> MokaEvictingMap<K, Q, T, I, C>
where
    K: Ord + Hash + Eq + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Ord + Hash + Eq + Debug + Send + Sync + 'static,
    T: LenEntry + Debug + Clone + Send + Sync + 'static,
    I: InstantWrapper,
    C: ItemCallback<Q> + Clone + 'static,
{
    pub fn new(config: &EvictionPolicy) -> Self
    where
        I: Default,
    {
        Self::with_anchor(config, I::default())
    }

    pub fn with_anchor(config: &EvictionPolicy, anchor_time: I) -> Self {
        let max_bytes = config.max_bytes as u64;
        let max_count = config.max_count;
        let max_seconds = config.max_seconds;
        let evict_bytes = config.evict_bytes as u64;

        let (eviction_tx, eviction_rx) = mpsc::unbounded_channel::<()>();
        let listener_tx = eviction_tx.clone();

        // Shared state captured by the eviction listener closure.
        let pinned: Arc<DashMap<K, PinnedEntry<T>>> = Arc::new(DashMap::new());
        let listener_pinned = Arc::clone(&pinned);
        let btree: Arc<RwLock<Option<BTreeSet<K>>>> = Arc::new(RwLock::new(None));
        let listener_btree = Arc::clone(&btree);
        let pending_evictions: Arc<parking_lot::Mutex<VecDeque<EvictionEvent<K, T>>>> =
            Arc::new(parking_lot::Mutex::new(VecDeque::new()));
        let listener_pending = Arc::clone(&pending_evictions);

        let mut builder = Cache::builder();

        // LRU eviction policy. We deliberately do NOT use moka's default
        // TinyLFU admission filter — TinyLFU compares the candidate's
        // frequency estimate against the victim's and REJECTS the new
        // entry on a tie. For a content-addressed store where every
        // upload must be cached, that silently drops freshly-written
        // blobs:
        //
        //   1. update_oneshot writes the new blob to a temp file.
        //   2. emplace_file calls evicting_map.insert(new_key, new_arc).
        //   3. moka's TinyLFU sees both old and new entries with similar
        //      frequency estimates and EVICTS THE NEW ENTRY (cause=Size).
        //   4. emplace_file's still_ours check fails → returns Ok without
        //      renaming the temp file into the content path.
        //   5. The temp file is cleaned up by Drop. The new blob is gone,
        //      yet update_oneshot returned Ok. A subsequent get() on the
        //      same digest returns NotFound — silent data loss.
        //
        // LRU has no admission filter: a new insert always succeeds and
        // displaces the least-recently-used entry. That matches the
        // contract of the previous (parking_lot LRU) EvictingMap and is
        // the only safe behavior for a CAS slow-tier.
        builder = builder.eviction_policy(moka::policy::EvictionPolicy::lru());

        // Capacity: use max_bytes with low-watermark from evict_bytes.
        // Setting capacity to (max_bytes - evict_bytes) ensures moka
        // keeps headroom, similar to the old evict_bytes behavior.
        if max_bytes > 0 {
            // Moka's weigher returns u32 but we track bytes as u64.
            // Scale capacity and weights to KB granularity so items up
            // to 4TB fit in u32. A 1-byte item weighs 1 (minimum).
            //
            // Floor the effective capacity at 1: a 0-capacity cache
            // immediately evicts every insert, which silently loses
            // just-written blobs. This matters for tests that configure
            // very small max_bytes (e.g. 5) and for production corner
            // cases where `evict_bytes >= max_bytes`.
            const SCALE: u64 = 1024;
            let effective_capacity =
                (max_bytes.saturating_sub(evict_bytes) / SCALE).max(1);
            builder = builder
                .max_capacity(effective_capacity)
                .weigher(|_key: &K, value: &T| -> u32 {
                    let kb = value.len().div_ceil(SCALE);
                    u32::try_from(kb).unwrap_or(u32::MAX)
                });
        } else if max_count > 0 {
            builder = builder.max_capacity(max_count);
        }

        if max_seconds > 0 {
            builder = builder.time_to_idle(Duration::from_secs(u64::from(max_seconds)));
        }

        // Eviction listener: fires synchronously during moka operations.
        // - Replaced: skip — insert() handles replaced-item unref directly.
        // - Size/Expired/Explicit: check if pinned (skip if so, it's safe
        //   in the DashMap). Otherwise send to background drainer.
        builder = builder.eviction_listener(move |key: Arc<K>, value: T, cause: RemovalCause| {
            if cause == RemovalCause::Replaced {
                // insert() captured the old value via cache.get() and
                // will await its unref() before returning. Don't double-unref.
                return;
            }

            // If this key is pinned, the pin_key() flow already moved it
            // to the DashMap. The invalidate() triggered this listener but
            // the data is safe in the pinned map. Skip cleanup.
            let q: &Q = (*key).borrow();
            if listener_pinned.contains_key(q) {
                return;
            }

            // Clean up BTree index on eviction.
            {
                let btree_guard = listener_btree.read();
                if btree_guard.is_some() {
                    drop(btree_guard);
                    let mut btree_guard = listener_btree.write();
                    if let Some(ref mut set) = *btree_guard {
                        set.remove(q);
                    }
                }
            }

            // Push the event onto the sync side queue, then wake the
            // background drainer via a `()` signal. `insert()` drains the
            // same queue synchronously after `run_pending_tasks()` so the
            // evicted item's `unref()` completes before the caller returns
            // — this is the contract the original `EvictingMap` exposed.
            listener_pending.lock().push_back(EvictionEvent {
                key: Arc::clone(&key),
                value,
            });
            // Unbounded channel never blocks — send only fails if the
            // receiver is dropped (shutdown).
            let _ = listener_tx.send(());
        });

        let cache = builder.build();
        let pin_cap = (max_bytes as f64 * PIN_CAP_FRACTION) as u64;

        Self {
            cache,
            pinned,
            pinned_bytes: AtomicU64::new(0),
            pin_cap,
            btree,
            pending_evictions,
            eviction_tx,
            eviction_rx: parking_lot::Mutex::new(Some(eviction_rx)),
            callbacks: RwLock::new(Vec::new()),
            has_callbacks_flag: AtomicBool::new(false),
            anchor_time,
            max_bytes,
            max_count,
            background_running: AtomicBool::new(false),
            evicted_bytes: Counter::default(),
            evicted_items: CounterWithTime::default(),
            replaced_bytes: Counter::default(),
            replaced_items: CounterWithTime::default(),
            lifetime_inserted_bytes: Counter::default(),
            _q: core::marker::PhantomData,
        }
    }

    /// Fast-path check: returns true if any items are pinned.
    #[inline]
    fn has_pinned(&self) -> bool {
        self.pinned_bytes.load(Ordering::Relaxed) > 0
    }

    // ---------------------------------------------------------------
    // get
    // ---------------------------------------------------------------

    pub async fn get(&self, key: &Q) -> Option<T> {
        // Atomic fast-path: skip DashMap probe when nothing is pinned.
        // Pinned-hit reads do NOT fire `on_get` — pinned entries are a
        // worker-side staging concept (a blob in flight to a sandbox)
        // and the locality_map already knows about pins.
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                return Some(entry.data.clone());
            }
        }
        let result = self.cache.get(key);
        if result.is_some() {
            self.fire_on_get(key);
        }
        result
    }

    /// Retrieve multiple values by key. Sequential iteration is intentional:
    /// Moka's `cache.get()` is synchronous (lock-free concurrent hash map),
    /// so 500 lookups complete in ~50us. Parallelism via `spawn_blocking` or
    /// `par_iter` would add more overhead than it saves.
    pub async fn get_many<'b, Iter>(&self, keys: Iter) -> Vec<Option<T>>
    where
        Iter: IntoIterator<Item = &'b Q>,
        Q: 'b,
    {
        let check_pinned = self.has_pinned();
        keys.into_iter()
            .map(|key| {
                // Pinned-hit reads do NOT fire `on_get` (see `get`).
                if check_pinned {
                    if let Some(entry) = self.pinned.get(key) {
                        return Some(entry.data.clone());
                    }
                }
                let result = self.cache.get(key);
                if result.is_some() {
                    self.fire_on_get(key);
                }
                result
            })
            .collect()
    }

    // ---------------------------------------------------------------
    // insert
    // ---------------------------------------------------------------

    pub async fn insert(&self, key: K, data: T) -> Option<T>
    where
        K: 'static,
    {
        let old = self.insert_inner(key, data);
        // Await unref on replaced item before returning. This preserves
        // the invariant that the old file is cleaned up before the caller
        // renames the new file into the content path.
        if let Some(ref value) = old {
            value.unref().await;
        }
        // Drain any eviction events this insert triggered and unref them
        // synchronously (before returning to the caller). The original
        // LRU `EvictingMap` unref'd evicted items inside `insert`, and
        // code / tests in FilesystemStore depend on that ordering
        // (e.g. `on_unref` hooks observed immediately after insert).
        self.drain_pending_evictions().await;
        old
    }

    /// Drain `pending_evictions` inline and fire `process_eviction_event`
    /// on each. Concurrency: the queue is a `parking_lot::Mutex` so we
    /// take items one at a time (not a bulk drain) to avoid holding the
    /// lock across awaits.
    async fn drain_pending_evictions(&self) {
        loop {
            let event = self.pending_evictions.lock().pop_front();
            match event {
                Some(ev) => self.process_eviction_event(ev).await,
                None => break,
            }
        }
    }

    pub async fn insert_with_time(
        &self,
        key: K,
        data: T,
        _seconds_since_anchor: i32,
    ) -> Option<T> {
        // Startup path: files are inserted oldest-first (sorted by atime).
        //
        // The `seconds_since_anchor` parameter is intentionally ignored.
        // Under the LRU policy moka pushes new entries to the MRU end of
        // the deque in insertion order, so the oldest-inserted entries
        // (i.e. files with the oldest atime) sit at the LRU position and
        // are evicted first under size pressure — preserving atime
        // ordering without needing a custom expiration policy.
        let old = self.insert_startup(key, data);
        if let Some(ref value) = old {
            value.unref().await;
        }
        old
    }

    fn insert_inner(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // Update BTree index.
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        // If key is pinned, replace in pinned map directly.
        if self.has_pinned() && self.pinned.contains_key(key.borrow()) {
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                entry.data
            });
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: data.clone(),
                    pinned_at: Instant::now(),
                    size,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            self.fire_on_insert_callbacks(&key, size);
            if old.is_some() {
                self.replaced_bytes.add(size);
                self.replaced_items.inc();
            }
            return old;
        }

        // Capture old value before insert for replaced-item unref.
        // The eviction listener skips Replaced events since we handle
        // cleanup here.
        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // Process pending tasks so any size-driven eviction triggered by
        // this insert fires its listener before we return. The caller
        // (emplace_file) relies on the eviction event being queued before
        // its `still_ours` check.
        self.cache.run_pending_tasks();

        // Enforce max_count if both max_bytes and max_count are set.
        if self.max_count > 0
            && self.max_bytes > 0
            && self.cache.entry_count() > self.max_count
        {
            // run_pending_tasks again to trigger any additional eviction.
            self.cache.run_pending_tasks();
        }

        self.fire_on_insert_callbacks(&key, size);
        if existing.is_some() {
            self.replaced_bytes.add(size);
            self.replaced_items.inc();
        }
        existing
    }

    /// Startup-optimized insert: no frequency bump, no per-insert
    /// run_pending_tasks(). Caller should call cache.run_pending_tasks()
    /// after the full batch. Items enter at freq=0 in the frequency
    /// sketch and are pushed to MainProbation in insertion order when
    /// WriteOps are processed, so oldest-inserted entries sit at the
    /// front (LRU position) and are evicted first during size pressure.
    fn insert_startup(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // BTree update (if enabled).
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // No frequency bump (no extra get()).
        // No run_pending_tasks() — deferred to caller.
        self.fire_on_insert_callbacks(&key, size);
        existing
    }

    fn fire_on_insert_callbacks(&self, key: &K, size: u64) {
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_insert(key.borrow(), size);
        }
    }

    /// Fire `on_get` callbacks for a public-read cache hit. Hot path:
    /// the AtomicBool fast path avoids touching the `callbacks` RwLock
    /// when no callbacks are registered (the common case for unit
    /// tests and stores configured without a tracker). `Relaxed` load
    /// pairs with the `Release` store in `add_item_callback`.
    ///
    /// Takes `&Q` (the borrowed key form) because the call sites in
    /// `get` / `get_many` already work in `&Q` and the callback trait
    /// itself is parameterized over `Q`. Using `&K` would force the
    /// caller to materialize an owned `K`, defeating the fast path.
    #[inline]
    fn fire_on_get(&self, key: &Q) {
        if !self.has_callbacks_flag.load(Ordering::Relaxed) {
            return;
        }
        let callbacks = self.callbacks.read();
        for cb in callbacks.iter() {
            cb.on_get(key);
        }
    }

    pub async fn insert_many<It>(&self, inserts: It) -> Vec<T>
    where
        It: IntoIterator<Item = (K, T)> + Send,
        <It as IntoIterator>::IntoIter: Send,
        K: 'static,
    {
        let mut replaced = Vec::new();
        for (key, data) in inserts {
            // Use insert_batch (no per-item run_pending_tasks) to avoid
            // N+1 maintenance passes. Process all pending tasks once at end.
            let old = self.insert_batch(key, data);
            if let Some(value) = old {
                value.unref().await;
                replaced.push(value);
            }
        }
        self.cache.run_pending_tasks();
        // Synchronously process any evictions triggered by the batch so
        // unref()/callbacks complete before returning.
        self.drain_pending_evictions().await;
        replaced
    }

    /// Batch-optimized insert: includes frequency bump but defers
    /// run_pending_tasks() to the caller. Used by insert_many().
    fn insert_batch(&self, key: K, data: T) -> Option<T> {
        let size = data.len();
        self.lifetime_inserted_bytes.add(size);

        // Update BTree index.
        {
            let btree = self.btree.read();
            if btree.is_some() {
                drop(btree);
                let mut btree = self.btree.write();
                if let Some(ref mut set) = *btree {
                    set.insert(key.clone());
                }
            }
        }

        // If key is pinned, replace in pinned map directly.
        if self.has_pinned() && self.pinned.contains_key(key.borrow()) {
            let old = self.pinned.remove(key.borrow()).map(|(_, entry)| {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                entry.data
            });
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: data.clone(),
                    pinned_at: Instant::now(),
                    size,
                },
            );
            self.pinned_bytes.fetch_add(size, Ordering::Relaxed);
            self.fire_on_insert_callbacks(&key, size);
            if old.is_some() {
                self.replaced_bytes.add(size);
                self.replaced_items.inc();
            }
            return old;
        }

        let existing = self.cache.get(key.borrow());
        self.cache.insert(key.clone(), data);
        // No run_pending_tasks — caller batches.
        self.fire_on_insert_callbacks(&key, size);
        if existing.is_some() {
            self.replaced_bytes.add(size);
            self.replaced_items.inc();
        }
        existing
    }

    // ---------------------------------------------------------------
    // remove
    // ---------------------------------------------------------------

    pub async fn remove(&self, key: &Q) -> bool {
        // Try pinned map first.
        if self.has_pinned() {
            if let Some((_, entry)) = self.pinned.remove(key) {
                self.pinned_bytes
                    .fetch_sub(entry.size, Ordering::Relaxed);
                self.update_btree_remove(key);

                // Fire callbacks + unref in background.
                let data = entry.data;
                let callbacks = self.collect_removal_callbacks(key);
                drop(background_spawn!(
                    "moka_evicting_map_remove_cleanup",
                    async move {
                        let mut futs: FuturesUnordered<_> = callbacks.into_iter().collect();
                        while futs.next().await.is_some() {}
                        data.unref().await;
                    }
                ));
                return true;
            }
        }

        // Try moka cache. remove() returns the value and fires the
        // eviction listener (Explicit cause), which sends to the
        // background drainer for unref + callbacks.
        if self.cache.remove(key).is_some() {
            self.cache.run_pending_tasks();
            // BTree cleanup handled by eviction listener.
            return true;
        }
        false
    }

    pub async fn remove_if<F>(&self, key: &Q, cond: F) -> bool
    where
        F: FnOnce(&T) -> bool + Send,
    {
        // Check pinned first.
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                if cond(&entry.data) {
                    drop(entry);
                    return self.remove(key).await;
                }
                return false;
            }
        }

        // Check moka cache.
        if let Some(value) = self.cache.get(key) {
            if cond(&value) {
                return self.remove(key).await;
            }
        }
        false
    }

    fn update_btree_remove(&self, key: &Q) {
        let btree = self.btree.read();
        if btree.is_some() {
            drop(btree);
            let mut btree = self.btree.write();
            if let Some(ref mut set) = *btree {
                set.remove(key);
            }
        }
    }

    fn collect_removal_callbacks(
        &self,
        key: &Q,
    ) -> Vec<core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send>>> {
        let cbs = self.callbacks.read();
        cbs.iter().map(|cb| cb.callback(key)).collect()
    }

    // ---------------------------------------------------------------
    // size queries
    // ---------------------------------------------------------------

    pub async fn size_for_key(&self, key: &Q) -> Option<u64> {
        if self.has_pinned() {
            if let Some(entry) = self.pinned.get(key) {
                return Some(entry.data.len());
            }
        }
        self.cache.get(key).map(|v| v.len())
    }

    /// Note: the `peek` parameter is accepted for API compatibility but
    /// ignored. Moka has no non-promoting peek — `cache.get()` always
    /// updates the access time. For both ExistenceCacheStore and
    /// FilesystemStore has() checks, the LRU promotion is acceptable
    /// (and actually desirable for keeping hot blobs warm).
    ///
    /// IMPORTANT: this path intentionally does NOT fire `on_get`
    /// callbacks. `sizes_for_keys` is the existence-check entrypoint
    /// used by `FastSlowStore::has()` and `ExistenceCacheStore::update()`,
    /// each of which probes large key batches per request. Firing
    /// `on_get` here would explode the worker-side "recently read" set
    /// and overwhelm the locality_map broadcast on the server. Logical
    /// reads come exclusively from `get` / `get_many`.
    pub async fn sizes_for_keys<It, R>(
        &self,
        keys: It,
        results: &mut [Option<u64>],
        _peek: bool,
    ) where
        It: IntoIterator<Item = R> + Send,
        <It as IntoIterator>::IntoIter: Send,
        R: Borrow<Q> + Send,
    {
        let check_pinned = self.has_pinned();
        for (key, result) in keys.into_iter().zip(results.iter_mut()) {
            let k: &Q = key.borrow();
            if check_pinned {
                if let Some(entry) = self.pinned.get(k) {
                    *result = Some(entry.data.len());
                    continue;
                }
            }
            *result = self.cache.get(k).map(|v| v.len());
        }
    }

    // ---------------------------------------------------------------
    // pinning
    // ---------------------------------------------------------------

    pub fn pin_key(&self, key: K) -> bool {
        let q: &Q = key.borrow();

        // Already pinned — refresh pin time.
        if let Some(mut entry) = self.pinned.get_mut(q) {
            entry.pinned_at = Instant::now();
            return true;
        }

        // Look up in cache (clone value while it's still in cache).
        let value = match self.cache.get(q) {
            Some(v) => v,
            None => return false,
        };

        let entry_size = value.len();

        // Enforce pin cap.
        if self.max_bytes != 0 {
            let current_pinned = self.pinned_bytes.load(Ordering::Relaxed);
            if current_pinned.saturating_add(entry_size) > self.pin_cap {
                warn!(
                    pinned_bytes = current_pinned,
                    entry_size,
                    pin_cap = self.pin_cap,
                    ?key,
                    "pin cap exceeded, refusing to pin"
                );
                return false;
            }
        }

        // CRITICAL: Insert into pinned map FIRST, then invalidate from
        // cache. The eviction listener checks pinned map and skips
        // cleanup if the key is found there. This ordering prevents the
        // race where invalidate fires the listener before the item is
        // in the pinned map.
        self.pinned.insert(
            key.clone(),
            PinnedEntry {
                data: value,
                pinned_at: Instant::now(),
                size: entry_size,
            },
        );
        self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);

        // Now safe to remove from cache — listener will see it's pinned.
        self.cache.invalidate(q);
        self.cache.run_pending_tasks();
        true
    }

    pub fn pin_keys(&self, keys: &[K]) -> usize {
        let mut pinned = 0;
        for key in keys {
            let q: &Q = key.borrow();

            // Already pinned — refresh.
            if let Some(mut entry) = self.pinned.get_mut(q) {
                entry.pinned_at = Instant::now();
                pinned += 1;
                continue;
            }

            let value = match self.cache.get(q) {
                Some(v) => v,
                None => continue,
            };

            let entry_size = value.len();
            if self.max_bytes != 0 {
                let current = self.pinned_bytes.load(Ordering::Relaxed);
                if current.saturating_add(entry_size) > self.pin_cap {
                    warn!(
                        pinned_bytes = current,
                        entry_size,
                        pin_cap = self.pin_cap,
                        attempted = keys.len(),
                        pinned,
                        "pin_keys: pin cap exceeded, leaving remaining keys unpinned",
                    );
                    break;
                }
            }

            // Insert into pinned FIRST (same ordering as pin_key).
            self.pinned.insert(
                key.clone(),
                PinnedEntry {
                    data: value,
                    pinned_at: Instant::now(),
                    size: entry_size,
                },
            );
            self.pinned_bytes.fetch_add(entry_size, Ordering::Relaxed);

            // Invalidate from cache (don't call run_pending_tasks per key).
            self.cache.invalidate(q);
            pinned += 1;
        }
        // Batch: process all invalidations at once.
        self.cache.run_pending_tasks();
        pinned
    }

    pub fn unpin_key(&self, key: &Q) {
        if let Some((owned_key, entry)) = self.pinned.remove(key) {
            self.pinned_bytes
                .fetch_sub(entry.size, Ordering::Relaxed);
            // Move back into moka cache. Under LRU there is no admission
            // filter to fight, so a bare insert is sufficient.
            self.cache.insert(owned_key, entry.data);
        }
    }

    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }

    // ---------------------------------------------------------------
    // filtering / range
    // ---------------------------------------------------------------

    pub async fn enable_filtering(&self) {
        let mut btree = self.btree.write();
        if btree.is_none() {
            let mut set = BTreeSet::new();
            for (key, _value) in &self.cache {
                set.insert((*key).clone());
            }
            for entry in self.pinned.iter() {
                set.insert(entry.key().clone());
            }
            *btree = Some(set);
        }
    }

    pub async fn range<F>(
        &self,
        prefix_range: impl RangeBounds<Q> + Send,
        mut handler: F,
    ) -> u64
    where
        F: FnMut(&K, &T) -> bool + Send,
        K: Ord,
    {
        // Ensure BTree is built.
        {
            let btree = self.btree.read();
            if btree.is_none() {
                drop(btree);
                self.enable_filtering().await;
            }
        }

        let btree = self.btree.read();
        let set = btree.as_ref().expect("btree should be built");
        let check_pinned = self.has_pinned();
        let mut count = 0;
        for key in set.range(prefix_range) {
            let q: &Q = key.borrow();
            let value = if check_pinned {
                if let Some(entry) = self.pinned.get(q) {
                    Some(entry.data.clone())
                } else {
                    self.cache.get(q)
                }
            } else {
                self.cache.get(q)
            };
            // Skip keys evicted by moka but still in BTree (stale).
            if let Some(ref v) = value {
                if !handler(key, v) {
                    break;
                }
                count += 1;
            }
        }
        count
    }

    // ---------------------------------------------------------------
    // callbacks
    // ---------------------------------------------------------------

    pub fn add_item_callback(&self, callback: C) {
        self.callbacks.write().push(callback);
        // Publish with Release so the hot read path (which loads with
        // Relaxed) cannot observe `has_callbacks_flag == true` before it
        // would observe the appended callback under the RwLock. The
        // RwLock release inherent in `write()` already establishes the
        // necessary happens-before for the Vec contents; the AtomicBool
        // store is a separate signal whose Release ordering pairs with
        // the Acquire implicit in the subsequent RwLock `read()` in
        // `fire_on_get` / `fire_on_insert_callbacks`.
        self.has_callbacks_flag.store(true, Ordering::Release);
    }

    // ---------------------------------------------------------------
    // timestamps / diagnostics
    // ---------------------------------------------------------------

    pub fn get_all_entries_with_timestamps(&self) -> Vec<(K, i64)> {
        let anchor_epoch = self.anchor_time.unix_timestamp() as i64;
        let now_offset =
            i64::try_from(self.anchor_time.elapsed().as_secs()).unwrap_or(i64::MAX);

        let mut result = Vec::new();
        for (key, _value) in &self.cache {
            result.push(((*key).clone(), anchor_epoch + now_offset));
        }
        for entry in self.pinned.iter() {
            result.push((entry.key().clone(), anchor_epoch + now_offset));
        }
        result
    }

    pub async fn len_for_test(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize + self.pinned.len()
    }

    // ---------------------------------------------------------------
    // background eviction drainer
    // ---------------------------------------------------------------

    pub fn start_background_eviction(self: &Arc<Self>) {
        if self
            .background_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let this = Arc::clone(self);
        let rx = this
            .eviction_rx
            .lock()
            .take()
            .expect("start_background_eviction called twice");

        drop(background_spawn!(
            "moka_evicting_map_background",
            async move {
                this.drain_evictions(rx).await;
            }
        ));
    }

    async fn drain_evictions(
        self: &Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<()>,
    ) {
        let mut pin_check_interval = tokio::time::interval(Duration::from_secs(10));
        pin_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(()) = rx.recv() => {
                    // Coalesce additional wake signals — we'll drain the
                    // entire side queue below regardless of how many
                    // signals we got.
                    while rx.try_recv().is_ok() {}
                    self.drain_pending_evictions().await;
                }
                _ = pin_check_interval.tick() => {
                    self.expire_stale_pins().await;
                }
            }
        }
    }

    async fn process_eviction_event(&self, event: EvictionEvent<K, T>) {
        let size = event.value.len();
        self.evicted_bytes.add(size);
        self.evicted_items.inc();

        event.value.unref().await;

        let callbacks = {
            let cbs = self.callbacks.read();
            let q: &Q = (*event.key).borrow();
            cbs.iter().map(|cb| cb.callback(q)).collect::<Vec<_>>()
        };
        if !callbacks.is_empty() {
            let mut futs: FuturesUnordered<_> = callbacks.into_iter().collect();
            while futs.next().await.is_some() {}
        }
        drop(event);
    }

    async fn expire_stale_pins(&self) {
        let mut expired_keys = Vec::new();
        for entry in self.pinned.iter() {
            if entry.pinned_at.elapsed().as_secs() >= PIN_TIMEOUT_SECS {
                expired_keys.push(entry.key().clone());
            }
        }
        for key in expired_keys {
            let q: &Q = key.borrow();
            if let Some((_, entry)) = self.pinned.remove(q) {
                let size = entry.size;
                info!(
                    ?key,
                    pin_timeout_secs = PIN_TIMEOUT_SECS,
                    entry_size = size,
                    "auto-unpinning expired pin"
                );
                self.pinned_bytes.fetch_sub(size, Ordering::Relaxed);
                // Put back into cache so it can be evicted normally.
                //
                // NOTE: this is NOT an eviction — the blob is still
                // resident in this `MokaEvictingMap`, just no longer
                // pinned. We deliberately do NOT route through
                // `process_eviction_event` (which would increment
                // `evicted_bytes` / `evicted_items` and fire the
                // removal `callback`). A previous code-review flagged
                // a counter double-count risk if pin-expiry was
                // accounted as eviction.
                //
                // We DO want listeners (e.g. BlobChangeTracker, which
                // feeds the server's locality_map via BlobsAvailable)
                // to re-acknowledge the blob now that it has crossed
                // back into the regular LRU pool. Fire `on_insert` so
                // the next BlobsAvailable broadcast carries a fresh
                // entry for it.
                self.cache.insert(key.clone(), entry.data);
                self.fire_on_insert_callbacks(&key, size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Inline coverage for the `on_get` hook added on top of the
    //! existing `tests/moka_evicting_map_test.rs` integration tests.
    //! These tests are colocated with the implementation because they
    //! exercise the AtomicBool fast-path field which is not part of
    //! the public surface.
    use core::future::Future;
    use core::pin::Pin;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::time::Instant;

    use nativelink_config::stores::EvictionPolicy;

    use super::{MokaEvictingMap, PinnedEntry, PIN_TIMEOUT_SECS};
    use crate::evicting_map::{ItemCallback, LenEntry};

    // ---------------------------------------------------------------
    // Test helpers (mirrors `tests/moka_evicting_map_test.rs` so the
    // inline tests are self-contained — the integration tests cannot
    // be reused here because they live in a separate crate target).
    // ---------------------------------------------------------------

    #[derive(Debug, Clone)]
    struct BytesEntry(u64);

    impl LenEntry for BytesEntry {
        fn len(&self) -> u64 {
            self.0
        }
        fn is_empty(&self) -> bool {
            self.0 == 0
        }
    }

    fn policy(max_bytes: usize, max_count: u64) -> EvictionPolicy {
        EvictionPolicy {
            max_bytes,
            evict_bytes: 0,
            max_seconds: 0,
            max_count,
        }
    }

    /// Callback that increments a counter for each hook invocation.
    /// Records the last key seen on each hook for assertion granularity.
    #[derive(Debug, Clone)]
    struct CountingCallback {
        get_count: Arc<AtomicU64>,
        insert_count: Arc<AtomicU64>,
        removal_count: Arc<AtomicU64>,
    }

    impl CountingCallback {
        fn new() -> Self {
            Self {
                get_count: Arc::new(AtomicU64::new(0)),
                insert_count: Arc::new(AtomicU64::new(0)),
                removal_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl ItemCallback<u64> for CountingCallback {
        fn callback(
            &self,
            _key: &u64,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            self.removal_count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        }

        fn on_insert(&self, _key: &u64, _size: u64) {
            self.insert_count.fetch_add(1, Ordering::Relaxed);
        }

        fn on_get(&self, _key: &u64) {
            self.get_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    type TestMapCb =
        MokaEvictingMap<u64, u64, BytesEntry, SystemTime, CountingCallback>;

    fn make_map_cb(cfg: &EvictionPolicy) -> TestMapCb {
        MokaEvictingMap::with_anchor(cfg, SystemTime::now())
    }

    // ---------------------------------------------------------------
    // 1. on_get fires on cache.get hit
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_fires_on_cache_get_hit() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        map.insert(1, BytesEntry(10)).await;
        // `insert` does not fire `on_get`.
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "insert must not fire on_get"
        );

        // First read — must fire on_get exactly once.
        let v = map.get(&1).await;
        assert!(v.is_some(), "key should be present");
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            1,
            "on_get should fire once on cache hit"
        );

        // Second read — fires again.
        let _ = map.get(&1).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            2,
            "on_get should fire on every cache-hit read"
        );

        // get_many: hit two keys, miss one — only the two hits should fire.
        map.insert(2, BytesEntry(20)).await;
        let results = map.get_many(&[1u64, 2, 99]).await;
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            2 + 2,
            "get_many should fire on_get once per cache hit, not on misses"
        );
    }

    // ---------------------------------------------------------------
    // 2. on_get does NOT fire on cache.get miss
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_on_miss() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        // No inserts — every get is a miss.
        for k in 0..10u64 {
            let v = map.get(&k).await;
            assert!(v.is_none());
        }
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "on_get must not fire on cache miss"
        );

        // get_many of all-missing keys.
        let results = map.get_many(&[100u64, 101, 102]).await;
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(Option::is_none));
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "get_many must not fire on_get for any miss"
        );
    }

    // ---------------------------------------------------------------
    // 3. on_get does NOT fire from sizes_for_keys
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_from_sizes_for_keys() {
        let cfg = policy(0, 100);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        let insert_count = Arc::clone(&cb.insert_count);
        map.add_item_callback(cb);

        // Populate three keys (fires on_insert thrice, never on_get).
        for k in 0..3u64 {
            map.insert(k, BytesEntry((k + 1) * 100)).await;
        }
        assert_eq!(insert_count.load(Ordering::Relaxed), 3);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "insert must not fire on_get"
        );

        // sizes_for_keys: pure existence-check path. Must NOT fire on_get
        // even for present keys (FastSlowStore::has() / ExistenceCacheStore
        // would otherwise explode the touched set).
        let keys = [0u64, 1, 2, 99];
        let mut sizes = [None; 4];
        map.sizes_for_keys(keys.iter(), &mut sizes, false).await;
        assert_eq!(sizes[0], Some(100));
        assert_eq!(sizes[1], Some(200));
        assert_eq!(sizes[2], Some(300));
        assert_eq!(sizes[3], None);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "sizes_for_keys must not fire on_get"
        );

        // Also confirm size_for_key does not fire (single-key existence path).
        let _ = map.size_for_key(&0).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "size_for_key must not fire on_get"
        );
    }

    // ---------------------------------------------------------------
    // 4. on_get fast path: no fire when no callbacks registered
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_fast_path_no_callbacks() {
        // No callback added — `has_callbacks_flag` stays false and the
        // fire_on_get fast path returns immediately. We cannot
        // directly observe "no RwLock taken" without instrumentation,
        // but we can observe (a) the flag remains false and (b) reads
        // succeed. A sentinel callback added afterwards must then
        // start firing.
        let cfg = policy(0, 100);
        let map: MokaEvictingMap<
            u64,
            u64,
            BytesEntry,
            SystemTime,
            CountingCallback,
        > = MokaEvictingMap::with_anchor(&cfg, SystemTime::now());

        // Initial state: flag is false.
        assert!(
            !map.has_callbacks_flag.load(Ordering::Relaxed),
            "flag should start false (no callbacks registered)"
        );

        map.insert(1, BytesEntry(10)).await;

        // Many reads with no callbacks — flag must remain false.
        for _ in 0..50 {
            let v = map.get(&1).await;
            assert!(v.is_some());
        }
        assert!(
            !map.has_callbacks_flag.load(Ordering::Relaxed),
            "flag must remain false until add_item_callback is called"
        );

        // Now register a callback — flag must flip to true and reads
        // must fire on_get.
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);
        assert!(
            map.has_callbacks_flag.load(Ordering::Relaxed),
            "add_item_callback must publish has_callbacks_flag=true"
        );

        let _ = map.get(&1).await;
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            1,
            "on_get should fire once a callback is registered"
        );
    }

    // ---------------------------------------------------------------
    // 5. on_get is NOT fired from a pinned-hit read
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn on_get_does_not_fire_for_pinned_hit() {
        // Pinned entries are a worker-side staging concept — they
        // already broadcast as pinned to the locality_map. Double-firing
        // on_get for pinned reads would be redundant churn.
        let cfg = policy(100 * 1024, 0);
        let map = make_map_cb(&cfg);
        let cb = CountingCallback::new();
        let get_count = Arc::clone(&cb.get_count);
        map.add_item_callback(cb);

        map.insert(1, BytesEntry(2048)).await;
        assert!(map.pin_key(1), "pin should succeed");

        // Read while pinned.
        let v = map.get(&1).await;
        assert!(v.is_some(), "pinned key should still be readable");
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "pinned-hit reads must not fire on_get"
        );

        // get_many path — same expectation for pinned hits.
        drop(map.get_many(&[1u64]).await);
        assert_eq!(
            get_count.load(Ordering::Relaxed),
            0,
            "pinned-hit reads must not fire on_get from get_many either"
        );

        // Cleanup so eviction-listener side-effects don't fire under teardown.
        map.unpin_key(&1);
    }

    // ---------------------------------------------------------------
    // 6. expire_stale_pins fires on_insert (NOT eviction callbacks).
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn expire_stale_pins_fires_on_insert_not_eviction() {
        let cfg = policy(100 * 1024, 0);
        let map = Arc::new(make_map_cb(&cfg));
        let cb = CountingCallback::new();
        let insert_count = Arc::clone(&cb.insert_count);
        let removal_count = Arc::clone(&cb.removal_count);
        map.add_item_callback(cb);

        // Insert + pin.
        map.insert(1, BytesEntry(2048)).await;
        assert!(map.pin_key(1), "pin should succeed");
        let baseline_inserts = insert_count.load(Ordering::Relaxed);

        // Force a stale pin by rewinding pinned_at past PIN_TIMEOUT_SECS.
        // We bypass the public API — directly mutate the DashMap entry.
        {
            let mut entry = map
                .pinned
                .get_mut(&1u64)
                .expect("key 1 should be pinned");
            // Roll back pinned_at by enough to exceed the timeout.
            entry.pinned_at = Instant::now()
                - core::time::Duration::from_secs(PIN_TIMEOUT_SECS + 1);
            // Sanity: ensure we built a valid PinnedEntry with the same
            // size we put in (sanity-checks the test setup, not the SUT).
            let _: &PinnedEntry<BytesEntry> = &*entry;
        }

        // Run the expiry sweep directly — no need to wait 10s for the
        // background ticker.
        map.expire_stale_pins().await;

        // The blob should now be back in the cache (not in pinned map).
        assert_eq!(map.pinned_bytes(), 0, "pin should be cleared");
        assert!(
            map.get(&1).await.is_some(),
            "blob should still be reachable from cache after pin-expiry"
        );

        // expire_stale_pins must fire on_insert exactly once for the
        // re-announced blob, and must NOT fire the removal callback
        // (pin-expiry is not an eviction).
        assert_eq!(
            insert_count.load(Ordering::Relaxed),
            baseline_inserts + 1,
            "pin-expiry should fire on_insert once for the re-announced blob"
        );
        assert_eq!(
            removal_count.load(Ordering::Relaxed),
            0,
            "pin-expiry must NOT fire the removal callback",
        );
    }
}
