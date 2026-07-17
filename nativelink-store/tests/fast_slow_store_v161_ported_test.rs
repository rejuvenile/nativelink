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

//! Ported from upstream v1.6.1 `fast_slow_store_test.rs`: net-new
//! concurrent-dedup, huge-blob bypass (#2415), `has()`/in-flight-visibility,
//! and stale-fast-map fall-through tests dropped during the fork's v1.6.1
//! merge (which kept the fork's diverged `fast_slow_store_test.rs`). These
//! exercise fork production behavior the fork tests do not cover with a
//! slow-store call COUNT: the `spawn_populate_producer_with_role` dedup
//! (collapse N concurrent reads to one `slow.get_part`), the
//! `bypass_dedup_threshold_bytes` fan-out, the `in_flight_slow_writes`
//! visibility + cleanup guard, and stale-fast-map get_part fall-through.
//!
//! Adapted to the fork's `FastSlowSpec` (extra `chunked_reads_enabled` +
//! `slow_writes_in_flight_max_bytes` fields). Standalone file, no special
//! cargo features; run via
//! `cargo test -p nativelink-store --test fast_slow_store_v161_ported_test`.

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation,
    StableDigestDelegation, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const MEGABYTE_SZ: usize = 1024 * 1024;
const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH_B: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH_C: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";
const VALID_HASH_D: &str = "0123456789abcdef000000000000000000040000000000000123456789abcdef";

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

#[derive(MetricsComponent)]
struct InstrumentedSlowStore {
    digest: DigestInfo,
    data: Vec<u8>,
    get_part_count: AtomicU64,
    /// If set, awaited at the start of `get_part` before any data flows.
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait]
impl StoreDriver for InstrumentedSlowStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for (key, result) in keys.iter().zip(results.iter_mut()) {
            if *key == self.digest.into() {
                *result = Some(self.digest.size_bytes());
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        // Drain anything sent so the writer side does not deadlock.
        reader.drain().await
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_part_count.fetch_add(1, Ordering::Acquire);
        // If a gate is configured, wait for the test to release it so the
        // populate can be held in flight long enough to exercise the
        // dedup paths.
        let gate = self.gate.lock().unwrap().take();
        if let Some(rx) = gate {
            let _ = rx.await;
        }
        writer.send(Bytes::copy_from_slice(&self.data)).await?;
        writer.send_eof()
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}
default_health_status_indicator!(InstrumentedSlowStore);

fn make_fast_slow_with_instrumented_slow(
    digest: DigestInfo,
    data: Vec<u8>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
    bypass_dedup_threshold_bytes: u64,
) -> (Store, Arc<InstrumentedSlowStore>) {
    let slow = Arc::new(InstrumentedSlowStore {
        digest,
        data,
        get_part_count: AtomicU64::new(0),
        gate: Mutex::new(gate),
    });
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes,
        },
        fast,
        Store::new(slow.clone()),
    ));
    (fast_slow, slow)
}

// Helpers for the gated-slow-store tests below. A `GatedSlowStore2` that
// blocks `update()` on a oneshot gate and signals when it starts, with
// `has_with_results` always returning all-None so the in-flight map is the
// only thing that can satisfy a concurrent `has()`.
#[derive(MetricsComponent)]
struct GatedSlowStore2 {
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    started_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[async_trait]
impl StoreDriver for GatedSlowStore2 {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let started_tx = self.started_tx.lock().unwrap().take();
        if let Some(tx) = started_tx {
            let _ = tx.send(());
        }
        let gate = self.gate.lock().unwrap().take();
        if let Some(rx) = gate {
            let _ = rx.await;
        }
        reader.drain().await
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        writer.send_eof()
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}
default_health_status_indicator!(GatedSlowStore2);

/// Wraps a `GatedSlowStore2` so `has_with_results` returns `Some` for any
/// digest pre-populated in `known` and `None` otherwise, while still
/// delegating writes through the gated inner so timing is controllable.
#[derive(MetricsComponent)]
struct MapBackedSlow {
    inner: Arc<GatedSlowStore2>,
    known: std::collections::HashMap<DigestInfo, u64>,
}

#[async_trait]
impl StoreDriver for MapBackedSlow {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for (k, r) in keys.iter().zip(results.iter_mut()) {
            if let StoreKey::Digest(d) = k
                && let Some(sz) = self.known.get(d)
            {
                *r = Some(*sz);
            }
        }
        Ok(())
    }
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        // Delegate to the gated inner so timing is controllable.
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }
    async fn get_part(
        self: Pin<&Self>,
        _k: StoreKey<'_>,
        w: &mut DropCloserWriteHalf,
        _o: u64,
        _l: Option<u64>,
    ) -> Result<(), Error> {
        w.send_eof()
    }
    fn inner_store(&self, _k: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}
default_health_status_indicator!(MapBackedSlow);

// Huge-blob dedup bypass: a `CountingSlowStore` counts get_part calls.
// Under dedup all readers collapse to one call; under bypass each reader
// drives its own and the fast tier stays empty.

/// `MemoryStore` wrapper that counts `get_part` invocations.
#[derive(MetricsComponent)]
struct CountingSlowStore {
    inner: Arc<MemoryStore>,
    get_part_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StoreDriver for CountingSlowStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(keys, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_part_calls.fetch_add(1, Ordering::AcqRel);
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
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

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}
default_health_status_indicator!(CountingSlowStore);

/// Fast store with a stale map entry: `has()` says present, `get_part`
/// returns `NotFound` (after `bytes_before_error` bytes, if non-zero).
#[derive(MetricsComponent)]
struct StaleFastStore {
    inner: Arc<MemoryStore>,
    reported_size: u64,
    bytes_before_error: u64,
    get_part_calls: Arc<AtomicU64>,
}

#[async_trait]
impl StoreDriver for StaleFastStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Stale entry: report present regardless of backing data.
        for result in results.iter_mut() {
            *result = Some(self.reported_size);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        // Accept the fall-through repopulate.
        Pin::new(self.inner.as_ref())
            .update(digest, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        self.get_part_calls.fetch_add(1, Ordering::AcqRel);
        if self.bytes_before_error > 0 {
            let partial_len = usize::try_from(self.bytes_before_error)
                .err_tip(|| "bytes_before_error exceeds usize")?;
            writer.send(Bytes::from(vec![0u8; partial_len])).await?;
        }
        Err(make_err!(
            Code::NotFound,
            "stale eviction-map entry: file missing on disk"
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

    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}
default_health_status_indicator!(StaleFastStore);

fn make_stores_with_stale_fast(
    reported_size: u64,
    bytes_before_error: u64,
) -> (Store, Store, Arc<AtomicU64>) {
    let get_part_calls = Arc::new(AtomicU64::new(0));
    let fast_store = Store::new(Arc::new(StaleFastStore {
        inner: MemoryStore::new(&MemorySpec::default()),
        reported_size,
        bytes_before_error,
        get_part_calls: get_part_calls.clone(),
    }));
    let slow_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast_store,
        slow_store.clone(),
    ));
    (fast_slow_store, slow_store, get_part_calls)
}

/// Many concurrent reads of the same digest must dedup down to a single
/// `slow_store.get_part` call.
#[nativelink_test]
async fn concurrent_reads_dedup_to_a_single_slow_store_call() -> Result<(), Error> {
    const N_CONCURRENT: usize = 16;
    let original_data = make_random_data(2048);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let (fast_slow_store, slow) =
        make_fast_slow_with_instrumented_slow(digest, original_data.clone(), Some(gate_rx), 0);

    let mut handles = Vec::with_capacity(N_CONCURRENT);
    for _ in 0..N_CONCURRENT {
        let store = fast_slow_store.clone();
        handles.push(tokio::spawn(async move {
            store.get_part_unchunked(digest, 0, None).await
        }));
    }

    // Give the spawned tasks a chance to register as followers on the
    // OnceCell before we let the leader's slow read complete.
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }

    gate_tx
        .send(())
        .map_err(|()| make_err!(Code::Internal, "Failed to release slow-store gate"))?;

    let results = join_all(handles).await;
    for r in results {
        let bytes = r
            .map_err(|e| make_err!(Code::Internal, "join error: {e:?}"))?
            .err_tip(|| "Concurrent get_part_unchunked failed")?;
        assert_eq!(
            bytes.as_ref(),
            original_data.as_slice(),
            "Every concurrent reader must observe the full, correct payload"
        );
    }

    let slow_calls = slow.get_part_count.load(Ordering::Acquire);
    assert_eq!(
        slow_calls, 1,
        "Expected the LoaderGuard dedup to collapse {N_CONCURRENT} concurrent reads to a single slow_store.get_part call, got {slow_calls}",
    );

    Ok(())
}

/// With an opt-in threshold set, reads of blobs at or above it skip the
/// dedup map and hit the slow store on every concurrent read. This is the
/// counterpart to `concurrent_reads_dedup_to_a_single_slow_store_call`,
/// which leaves the threshold at 0 (disabled) and therefore dedups.
#[nativelink_test]
async fn concurrent_reads_bypass_dedup_above_threshold() -> Result<(), Error> {
    const N_CONCURRENT: usize = 8;
    let original_data = make_random_data(4096);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();
    let blob_size = u64::try_from(original_data.len()).unwrap();

    // Threshold == blob size, so every read is at-or-above and bypasses
    // dedup. No gate: each reader hits the slow store directly.
    let (fast_slow_store, slow) =
        make_fast_slow_with_instrumented_slow(digest, original_data.clone(), None, blob_size);

    let mut handles = Vec::with_capacity(N_CONCURRENT);
    for _ in 0..N_CONCURRENT {
        let store = fast_slow_store.clone();
        handles.push(tokio::spawn(async move {
            store.get_part_unchunked(digest, 0, None).await
        }));
    }

    let results = join_all(handles).await;
    for r in results {
        let bytes = r
            .map_err(|e| make_err!(Code::Internal, "join error: {e:?}"))?
            .err_tip(|| "Concurrent bypass get_part_unchunked failed")?;
        assert_eq!(
            bytes.as_ref(),
            original_data.as_slice(),
            "Every bypassed reader must still observe the full, correct payload"
        );
    }

    let slow_calls = slow.get_part_count.load(Ordering::Acquire);
    assert_eq!(
        slow_calls, N_CONCURRENT as u64,
        "With the bypass threshold set, each of {N_CONCURRENT} reads must hit the slow store directly (no dedup), got {slow_calls}",
    );

    Ok(())
}

/// Dropping a follower's outer future must not cancel the leader's
/// populate.
#[nativelink_test]
async fn dropping_a_follower_does_not_cancel_the_leader() -> Result<(), Error> {
    let original_data = make_random_data(1024);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let (fast_slow_store, slow) =
        make_fast_slow_with_instrumented_slow(digest, original_data.clone(), Some(gate_rx), 0);

    let store_for_a = fast_slow_store.clone();
    let leader_handle =
        tokio::spawn(async move { store_for_a.get_part_unchunked(digest, 0, None).await });

    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    let store_for_b = fast_slow_store.clone();
    let b_result = tokio::time::timeout(
        Duration::from_millis(50),
        store_for_b.get_part_unchunked(digest, 0, None),
    )
    .await;
    assert!(
        b_result.is_err(),
        "Follower should still be waiting on the leader at this point",
    );

    gate_tx
        .send(())
        .map_err(|()| make_err!(Code::Internal, "Failed to release slow-store gate"))?;

    let leader_bytes = leader_handle
        .await
        .map_err(|e| make_err!(Code::Internal, "leader join error: {e:?}"))?
        .err_tip(|| "Leader's get_part_unchunked failed after follower drop")?;
    assert_eq!(
        leader_bytes.as_ref(),
        original_data.as_slice(),
        "Leader must observe the full, correct payload after a follower drop",
    );

    let slow_calls = slow.get_part_count.load(Ordering::Acquire);
    assert_eq!(
        slow_calls, 1,
        "Leader's populate must complete exactly once, got {slow_calls} slow_store.get_part calls",
    );

    Ok(())
}

#[nativelink_test]
async fn bypass_threshold_is_inclusive_at_exact_size() -> Result<(), Error> {
    // Bypass is `size >= threshold`, so size == threshold must bypass.
    const SIZE: usize = 4096;
    const READERS: usize = 4;

    let original_data = make_random_data(SIZE);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    let inner_slow = MemoryStore::new(&MemorySpec::default());
    inner_slow
        .update_oneshot(digest, original_data.clone().into())
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingSlowStore {
        inner: inner_slow,
        get_part_calls: calls.clone(),
    });
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(counting);
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: SIZE as u64,
        },
        fast_store.clone(),
        slow_store,
    ));

    let mut joins = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let store = fast_slow_store.clone();
        joins.push(tokio::spawn(async move {
            let _ignored = store.get_part_unchunked(digest, 0, None).await?;
            Ok::<_, Error>(())
        }));
    }
    for j in joins {
        j.await
            .map_err(|e| make_err!(Code::Internal, "join failed: {e}"))??;
    }

    assert_eq!(
        calls.load(Ordering::Acquire),
        READERS,
        "size == threshold should bypass dedup but observed dedup",
    );
    Ok(())
}

#[nativelink_test]
async fn huge_blob_bypasses_dedup_and_skips_populate() -> Result<(), Error> {
    // 1 KiB threshold so a 2 KiB blob trips the bypass.
    const THRESHOLD: u64 = 1024;
    const BLOB_SIZE: usize = 2 * 1024;
    const READERS: usize = 8;

    let original_data = make_random_data(BLOB_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    // Seed the slow tier directly so the fast tier starts empty.
    let inner_slow = MemoryStore::new(&MemorySpec::default());
    inner_slow
        .update_oneshot(digest, original_data.clone().into())
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingSlowStore {
        inner: inner_slow,
        get_part_calls: calls.clone(),
    });
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(counting);
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: THRESHOLD,
        },
        fast_store.clone(),
        slow_store,
    ));

    // Fan out READERS concurrent get_part calls.
    let mut joins = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let store = fast_slow_store.clone();
        let expected = original_data.clone();
        joins.push(tokio::spawn(async move {
            let got = store.get_part_unchunked(digest, 0, None).await?;
            assert_eq!(got.as_ref(), expected.as_slice(), "data mismatch");
            Ok::<_, Error>(())
        }));
    }
    for j in joins {
        j.await
            .map_err(|e| make_err!(Code::Internal, "join failed: {e}"))??;
    }

    // Bypass: one slow-store call per reader.
    assert_eq!(
        calls.load(Ordering::Acquire),
        READERS,
        "expected {READERS} slow-store get_part calls under bypass, observed dedup"
    );

    // Fast tier must stay empty.
    assert!(
        fast_store.has(digest).await?.is_none(),
        "huge-blob bypass populated the fast tier; that defeats the point of the bypass"
    );

    Ok(())
}

#[nativelink_test]
async fn small_blob_still_dedups_and_populates() -> Result<(), Error> {
    // 64-byte blob sits below the 1 MiB threshold, so dedup runs.
    const THRESHOLD: u64 = 1024 * 1024;
    const BLOB_SIZE: usize = 64;
    const READERS: usize = 8;

    let original_data = make_random_data(BLOB_SIZE);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();

    let inner_slow = MemoryStore::new(&MemorySpec::default());
    inner_slow
        .update_oneshot(digest, original_data.clone().into())
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingSlowStore {
        inner: inner_slow,
        get_part_calls: calls.clone(),
    });
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(counting);
    let fast_slow_store = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: THRESHOLD,
        },
        fast_store.clone(),
        slow_store,
    ));

    let mut joins = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let store = fast_slow_store.clone();
        let expected = original_data.clone();
        joins.push(tokio::spawn(async move {
            let got = store.get_part_unchunked(digest, 0, None).await?;
            assert_eq!(got.as_ref(), expected.as_slice(), "data mismatch");
            Ok::<_, Error>(())
        }));
    }
    for j in joins {
        j.await
            .map_err(|e| make_err!(Code::Internal, "join failed: {e}"))??;
    }

    // Dedup: all readers collapse to one slow-store call.
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "small-blob path lost dedup; observed >1 slow-store call"
    );

    // Fast tier should be populated.
    assert!(
        fast_store.has(digest).await?.is_some(),
        "small-blob path failed to populate the fast tier"
    );

    Ok(())
}

/// While one writer's slow-store write is in flight, a concurrent `has()`
/// must report the blob as present so the second writer does not race and
/// re-upload the same data.
#[nativelink_test]
async fn has_sees_in_flight_slow_writes() -> Result<(), Error> {
    #[derive(MetricsComponent)]
    struct GatedSlowStore {
        /// Released by the test to let the in-flight slow write complete.
        gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        /// Signalled once the slow-store `update` has begun draining.
        started_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        /// Signalled once the slow-store `update` has fully drained and
        /// returned, so the background write's in-flight entry is torn down.
        done_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl StoreDriver for GatedSlowStore {
        async fn post_init(self: Arc<Self>) -> Result<(), Error> {
            Ok(())
        }

        async fn has_with_results(
            self: Pin<&Self>,
            _keys: &[StoreKey<'_>],
            _results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            // Slow store reports nothing — the in-flight tracking is what
            // should fill the result in.
            Ok(())
        }

        async fn update(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            mut reader: DropCloserReadHalf,
            _size_info: UploadSizeInfo,
        ) -> Result<u64, Error> {
            let started_tx = self.started_tx.lock().unwrap().take();
            if let Some(tx) = started_tx {
                let _ = tx.send(());
            }
            let gate = self.gate.lock().unwrap().take();
            if let Some(rx) = gate {
                let _ = rx.await;
            }
            let drained = reader.drain().await;
            if let Some(tx) = self.done_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            drained
        }

        async fn get_part(
            self: Pin<&Self>,
            _key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            _offset: u64,
            _length: Option<u64>,
        ) -> Result<(), Error> {
            writer.send_eof()
        }

        fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

        fn durable_delegation(&self) -> DurableDelegation<'_> {
            DurableDelegation::Leaf
        }
    }

    default_health_status_indicator!(GatedSlowStore);

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let slow = Arc::new(GatedSlowStore {
        gate: Mutex::new(Some(gate_rx)),
        started_tx: Mutex::new(Some(started_tx)),
        done_tx: Mutex::new(Some(done_tx)),
    });
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow = Arc::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        Store::new(slow.clone()),
    ));

    let data = make_random_data(256);
    let digest = DigestInfo::try_new(VALID_HASH, data.len()).unwrap();

    // Sanity: nothing in flight, slow store has nothing, fast store has
    // nothing -> NotFound.
    assert_eq!(
        fast_slow.has(digest).await?,
        None,
        "Pre-condition: blob should be absent before any writer starts",
    );

    let writer_store = fast_slow.clone();
    let writer_data = data.clone();
    let writer = tokio::spawn(async move {
        writer_store
            .update_oneshot(digest, writer_data.into())
            .await
    });

    // Wait until the slow store's update is actually being driven, which
    // proves the in-flight registration is live.
    started_rx
        .await
        .map_err(|e| make_err!(Code::Internal, "started signal lost: {e:?}"))?;

    // Fork semantics (diverged from upstream, which BLOCKED has() until the
    // write completed): `has()` reads `in_flight_slow_writes` and reports the
    // blob present IMMEDIATELY while the background slow write is parked. The
    // slow store's own `has_with_results` returns nothing, so the only source
    // of this Some is the in-flight map.
    assert_eq!(
        fast_slow.has(digest).await?,
        Some(data.len() as u64),
        "has() must see the in-flight slow write via in_flight_slow_writes",
    );

    // The caller's `update_oneshot` future has already returned (the slow
    // write is a detached background task); join it for its Ok status.
    writer
        .await
        .map_err(|e| make_err!(Code::Internal, "writer join error: {e:?}"))??;

    // Release the gate and wait for the background write to fully drain,
    // which tears down its in-flight entry.
    gate_tx
        .send(())
        .map_err(|()| make_err!(Code::Internal, "Failed to release slow-store gate"))?;
    done_rx
        .await
        .map_err(|e| make_err!(Code::Internal, "done signal lost: {e:?}"))?;

    // After the in-flight entry is gone, has() must return None: the slow
    // store reports nothing and has() never falls back to the fast store.
    // The InFlightSlowWriteGuard drop runs as the detached task's future
    // completes — just after `done_tx` fires — so poll to a hard deadline
    // rather than racing that teardown.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fast_slow.has(digest).await?.is_none() {
                return Ok::<_, Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        make_err!(
            Code::Internal,
            "in-flight entry was not cleaned up within deadline; has() still reports the blob"
        )
    })??;

    Ok(())
}

/// In this fork, FSS `has()` NEVER consults the fast store — `has_with_results`
/// reads only the slow store (and the in-flight-slow-write map). This test pins
/// that contract: with the blob present in the slow store, the fast store's
/// `has` is not called (no extra round trip). (Upstream consulted the fast store
/// on a slow miss; the fork's `fast_store_only_value_is_reported_by_has` covers
/// the fast-tier visibility path separately.)
#[nativelink_test]
async fn has_does_not_consult_fast_store_when_slow_store_hits() -> Result<(), Error> {
    #[derive(MetricsComponent)]
    struct CountingFastStore {
        inner: Arc<MemoryStore>,
        has_calls: Arc<AtomicU64>,
    }

    #[async_trait]
    impl StoreDriver for CountingFastStore {
        async fn post_init(self: Arc<Self>) -> Result<(), Error> {
            Ok(())
        }

        async fn has_with_results(
            self: Pin<&Self>,
            keys: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            self.has_calls.fetch_add(1, Ordering::Acquire);
            Pin::new(self.inner.as_ref())
                .has_with_results(keys, results)
                .await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<u64, Error> {
            Pin::new(self.inner.as_ref())
                .update(key, reader, size_info)
                .await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            Pin::new(self.inner.as_ref())
                .get_part(key, writer, offset, length)
                .await
        }

        fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
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

        fn durable_delegation(&self) -> DurableDelegation<'_> {
            DurableDelegation::Leaf
        }
    }

    default_health_status_indicator!(CountingFastStore);

    let has_calls = Arc::new(AtomicU64::new(0));
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let fast = Store::new(Arc::new(CountingFastStore {
        inner: fast_inner,
        has_calls: has_calls.clone(),
    }));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow = Arc::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        slow.clone(),
    ));

    let data = make_random_data(128);
    let digest = DigestInfo::try_new(VALID_HASH, data.len()).unwrap();
    slow.update_oneshot(digest, data.clone().into()).await?;

    let before = has_calls.load(Ordering::Acquire);
    assert_eq!(
        fast_slow.has(digest).await?,
        Some(data.len() as u64),
        "Slow-store-only blob must be reported via slow lookup",
    );
    let after = has_calls.load(Ordering::Acquire);
    assert_eq!(
        after, before,
        "Fast store has() must not be consulted when the slow store already reports the blob",
    );

    Ok(())
}

/// `has_with_results` must independently classify each requested key. With
/// four keys — one only in the slow store, one with an in-flight slow write,
/// one only in the fast store, and one absent everywhere — the returned
/// slice must reflect each key's true state and not e.g. report the
/// in-flight size for unrelated keys.
#[nativelink_test]
async fn has_with_results_handles_mixed_key_sources() -> Result<(), Error> {
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let slow_inner = Arc::new(GatedSlowStore2 {
        gate: Mutex::new(Some(gate_rx)),
        started_tx: Mutex::new(Some(started_tx)),
    });

    // Populate the wrapper so `has_with_results` reports the slow-only key.
    let slow_only_size: u64 = 11;
    let in_flight_size: u64 = 22;
    let fast_only_size: u64 = 33;

    let slow_only_digest = DigestInfo::try_new(VALID_HASH, slow_only_size).unwrap();
    let in_flight_digest = DigestInfo::try_new(VALID_HASH_B, in_flight_size).unwrap();
    let fast_only_digest = DigestInfo::try_new(VALID_HASH_C, fast_only_size).unwrap();
    let missing_digest = DigestInfo::try_new(VALID_HASH_D, 44).unwrap();

    let mut known = std::collections::HashMap::new();
    known.insert(slow_only_digest, slow_only_size);
    let slow = Arc::new(MapBackedSlow {
        inner: slow_inner.clone(),
        known,
    });

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    // Fork adaptation: a WRITABLE fast tier. Upstream used `ReadOnly` here, but
    // in the fork `fast_direction == ReadOnly` sets `ignore_fast`, which routes
    // `update` straight to `slow_store.update` and NEVER registers the blob in
    // `in_flight_slow_writes` (fast_slow_store.rs:5933). The in-flight map is
    // only populated on the writable-fast defer path, which is what this test
    // needs to exercise. `has_with_results` still never consults the fast
    // store, so the fast-only key remains `None` regardless of direction.
    let fast_slow = Arc::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast.clone(),
        Store::new(slow.clone()),
    ));

    // Seed the fast store with the fast-only blob directly.
    fast.update_oneshot(
        fast_only_digest,
        make_random_data(usize::try_from(fast_only_size).unwrap()).into(),
    )
    .await?;

    // Kick off the in-flight slow write and wait until it's parked.
    let writer_store = fast_slow.clone();
    let writer = tokio::spawn(async move {
        writer_store
            .update_oneshot(
                in_flight_digest,
                make_random_data(usize::try_from(in_flight_size).unwrap()).into(),
            )
            .await
    });
    started_rx
        .await
        .map_err(|e| make_err!(Code::Internal, "started signal lost: {e:?}"))?;

    // Now query all four keys in one call.
    let keys: [StoreKey<'static>; 4] = [
        StoreKey::Digest(slow_only_digest),
        StoreKey::Digest(in_flight_digest),
        StoreKey::Digest(fast_only_digest),
        StoreKey::Digest(missing_digest),
    ];

    // Fork semantics (diverged from upstream, which BLOCKED on the in-flight
    // write): `has_with_results` classifies each key IMMEDIATELY — slow-store
    // hit, `in_flight_slow_writes` hit, and (deliberately) no fast-store
    // fallback — while the background slow write is still parked on the gate.
    let mut results: [Option<u64>; 4] = [None; 4];
    fast_slow
        .as_store_driver_pin()
        .has_with_results(&keys, &mut results)
        .await?;

    // Cleanup: release the gated writer and join the (already-returned)
    // caller future for its Ok status.
    gate_tx
        .send(())
        .map_err(|()| make_err!(Code::Internal, "Failed to release slow-store gate"))?;
    writer
        .await
        .map_err(|e| make_err!(Code::Internal, "writer join error: {e:?}"))??;

    assert_eq!(results[0], Some(slow_only_size), "slow-only key");
    assert_eq!(results[1], Some(in_flight_size), "in-flight key");
    assert_eq!(
        results[2], None,
        "fast-only key should be None because we do not check fast store"
    );
    assert_eq!(results[3], None, "missing key must stay None");

    Ok(())
}

/// A stale fast-store map entry must fall through to the slow store.
#[nativelink_test]
async fn get_part_falls_through_to_slow_on_stale_fast_map_entry() -> Result<(), Error> {
    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();
    let (fast_slow_store, slow_store, fast_get_part_calls) =
        make_stores_with_stale_fast(original_data.len() as u64, 0);

    // Only the slow store holds the data.
    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    let served = fast_slow_store
        .get_part_unchunked(digest, 0, None)
        .await
        .err_tip(|| "fast_slow get_part should fall through to slow store")?;

    // Fast store always errors, so bytes can only come from the slow store.
    assert_eq!(served, original_data, "served data must match slow store");
    assert_eq!(
        fast_get_part_calls.load(Ordering::Acquire),
        1,
        "fast store get_part must be attempted exactly once before falling through"
    );

    Ok(())
}

/// `NotFound` after partial bytes must propagate, not retry (would corrupt).
#[nativelink_test]
async fn get_part_propagates_not_found_after_partial_fast_read() -> Result<(), Error> {
    let original_data = make_random_data(MEGABYTE_SZ);
    let digest = DigestInfo::try_new(VALID_HASH, original_data.len()).unwrap();
    let (fast_slow_store, slow_store, fast_get_part_calls) =
        make_stores_with_stale_fast(original_data.len() as u64, 16);

    slow_store
        .update_oneshot(digest, original_data.clone().into())
        .await?;

    let result = fast_slow_store.get_part_unchunked(digest, 0, None).await;

    let err = result.expect_err("partial fast read then NotFound must not fall through");
    assert_eq!(
        err.code,
        Code::NotFound,
        "original NotFound must propagate, got: {err:?}"
    );
    assert_eq!(
        fast_get_part_calls.load(Ordering::Acquire),
        1,
        "fast store get_part must be attempted exactly once"
    );

    Ok(())
}

// NOTE: upstream v1.6.1's `dropping_update_future_cleans_up_in_flight_entry`
// was deliberately NOT ported. It asserts that dropping the CALLER's
// `update()` future cancels the in-flight slow write and tears down its
// `in_flight_slow_writes` entry. In the fork the slow write is a DETACHED
// background task (async-slow-write invariant: mirror_blobs + BIS ack), so the
// caller's `update_oneshot` future returns as soon as the background write is
// spawned — `writer.abort()` cannot cancel the detached slow write, and the
// in-flight entry is torn down when the background write completes, not when
// the caller drops. The upstream test's premise (inline, caller-cancellable
// slow write) does not hold here; porting it would test upstream semantics the
// fork intentionally diverged from, not fork behavior.
