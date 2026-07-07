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

use core::fmt::Debug;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use nativelink_config::stores::EvictionPolicy;
use nativelink_util::evicting_map::{ItemCallback, LenEntry, NoopCallback};
use nativelink_util::moka_evicting_map::MokaEvictingMap;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Simple entry that reports a configurable byte size.
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

/// Helper to build an `EvictionPolicy` with sensible defaults.
fn policy(
    max_bytes: usize,
    max_count: u64,
    max_seconds: u32,
    evict_bytes: usize,
) -> EvictionPolicy {
    EvictionPolicy {
        max_bytes,
        evict_bytes,
        max_seconds,
        max_count,
    }
}

type TestMap = MokaEvictingMap<u64, u64, BytesEntry, SystemTime, NoopCallback>;

fn make_map(cfg: &EvictionPolicy) -> TestMap {
    MokaEvictingMap::with_anchor(cfg, SystemTime::now())
}

type TestMapWithCallback =
    MokaEvictingMap<u64, u64, BytesEntry, SystemTime, TrackingCallback>;

fn make_map_cb(cfg: &EvictionPolicy) -> TestMapWithCallback {
    MokaEvictingMap::with_anchor(cfg, SystemTime::now())
}

// ---------------------------------------------------------------------------
// 1. Basic insert / get / remove
// ---------------------------------------------------------------------------

#[tokio::test]
async fn basic_insert_get_remove() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    // Insert
    let old = map.insert(1, BytesEntry(100)).await;
    assert!(old.is_none(), "first insert should return None");

    // Get
    let val = map.get(&1).await;
    assert!(val.is_some(), "should find inserted key");
    assert_eq!(val.unwrap().0, 100);

    // Remove
    let removed = map.remove(&1).await;
    assert!(removed, "remove should return true for existing key");

    // Verify gone
    let val = map.get(&1).await;
    assert!(val.is_none(), "key should be gone after remove");

    // Remove nonexistent
    let removed = map.remove(&999).await;
    assert!(!removed, "remove of nonexistent key should return false");
}

// ---------------------------------------------------------------------------
// 2. max_bytes eviction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn max_bytes_eviction() {
    // 10 KiB cache. Each entry is 2048 bytes. We can fit ~5.
    // moka scales to KB internally, so use multiples of 1024.
    let cfg = policy(10 * 1024, 0, 0, 0);
    let map = make_map(&cfg);

    for i in 0..10u64 {
        map.insert(i, BytesEntry(2048)).await;
    }

    // Force moka to process pending evictions.
    let count = map.len_for_test().await;
    // With 10 items * 2 KiB = 20 KiB > 10 KiB limit, some must be evicted.
    assert!(
        count < 10,
        "expected some evictions, got count={count}"
    );
    // Should keep roughly 5 items (10KiB / 2KiB).
    assert!(
        count <= 6,
        "expected at most ~5-6 items, got count={count}"
    );
}

// ---------------------------------------------------------------------------
// 3. max_count eviction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn max_count_eviction() {
    let cfg = policy(0, 5, 0, 0);
    let map = make_map(&cfg);

    for i in 0..10u64 {
        map.insert(i, BytesEntry(100)).await;
    }

    let count = map.len_for_test().await;
    assert!(
        count <= 6,
        "expected at most ~5-6 items with max_count=5, got {count}"
    );
}

// ---------------------------------------------------------------------------
// 4. TTL expiration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ttl_expiration() {
    let cfg = policy(0, 100, 1, 0); // max_seconds=1
    let map = make_map(&cfg);

    map.insert(1, BytesEntry(100)).await;
    assert!(map.get(&1).await.is_some(), "item should exist immediately");

    // Sleep longer than TTL.
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Moka lazily evicts on access / run_pending_tasks. A get triggers it.
    let val = map.get(&1).await;
    // Give moka another chance to process.
    let count = map.len_for_test().await;

    // Either the get returned None or it was evicted by now.
    assert!(
        val.is_none() || count == 0,
        "item should be evicted after TTL, val={val:?}, count={count}"
    );
}

// ---------------------------------------------------------------------------
// 5. Pin / unpin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pin_survives_eviction() {
    // 10 KiB cache, entries 2 KiB each.
    let cfg = policy(10 * 1024, 0, 0, 0);
    let map = make_map(&cfg);

    // Insert key 0 and pin it.
    map.insert(0, BytesEntry(2048)).await;
    let pinned = map.pin_key(0);
    assert!(pinned, "pin_key should succeed");

    // Flood with more entries to trigger eviction of unpinned items.
    for i in 1..20u64 {
        map.insert(i, BytesEntry(2048)).await;
    }

    // Pinned item should still be accessible.
    let val = map.get(&0).await;
    assert!(val.is_some(), "pinned item should survive eviction");
    assert_eq!(val.unwrap().0, 2048);

    // Unpin and verify still accessible (moved back to cache).
    map.unpin_key(&0);
    let val = map.get(&0).await;
    assert!(val.is_some(), "unpinned item should still be accessible");
}

// ---------------------------------------------------------------------------
// 6. Pin cap enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pin_cap_enforced() {
    // max_bytes=1000 => pin_cap = 250 (25%).
    // Scale to KB: moka weigher uses div_ceil(len, 1024).
    // Use max_bytes=100*1024 so pin_cap = 25*1024 = 25600 bytes.
    let cfg = policy(100 * 1024, 0, 0, 0);
    let map = make_map(&cfg);

    // Insert several items of 10 KiB each.
    for i in 0..5u64 {
        map.insert(i, BytesEntry(10 * 1024)).await;
    }

    // Pin items until we exceed pin cap (25 KiB).
    // First two: 10KiB + 10KiB = 20KiB < 25KiB => should succeed.
    assert!(map.pin_key(0), "pin 0 should succeed (10KiB < 25KiB cap)");
    assert!(map.pin_key(1), "pin 1 should succeed (20KiB < 25KiB cap)");
    // Third: 20KiB + 10KiB = 30KiB > 25KiB => should fail.
    assert!(
        !map.pin_key(2),
        "pin 2 should fail (would exceed 25KiB cap)"
    );

    // Cleanup.
    map.unpin_key(&0);
    map.unpin_key(&1);
}

// ---------------------------------------------------------------------------
// 7. Pin timeout - skipped (120s too slow for tests)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 8. Insert returns replaced item
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_returns_replaced_item() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    let first = map.insert(1, BytesEntry(100)).await;
    assert!(first.is_none(), "first insert should return None");

    let second = map.insert(1, BytesEntry(200)).await;
    assert!(second.is_some(), "second insert should return Some(old)");
    assert_eq!(second.unwrap().0, 100, "replaced value should be the original");

    // Verify new value is stored.
    let val = map.get(&1).await;
    assert_eq!(val.unwrap().0, 200);
}

// ---------------------------------------------------------------------------
// 9. insert_with_time (startup path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_with_time_accessible() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    // Insert items via startup path with various timestamps.
    map.insert_with_time(1, BytesEntry(100), -3600).await;
    map.insert_with_time(2, BytesEntry(200), -1800).await;
    map.insert_with_time(3, BytesEntry(300), -60).await;

    // All items should be accessible.
    assert!(map.get(&1).await.is_some());
    assert!(map.get(&2).await.is_some());
    assert!(map.get(&3).await.is_some());
    assert_eq!(map.get(&1).await.unwrap().0, 100);
    assert_eq!(map.get(&2).await.unwrap().0, 200);
    assert_eq!(map.get(&3).await.unwrap().0, 300);
}

// ---------------------------------------------------------------------------
// 10. sizes_for_keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sizes_for_keys() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    map.insert(10, BytesEntry(100)).await;
    map.insert(20, BytesEntry(200)).await;
    map.insert(30, BytesEntry(300)).await;

    let keys = [10u64, 20, 30, 99]; // 99 is missing
    let mut results = [None; 4];
    map.sizes_for_keys(keys.iter(), &mut results, false).await;

    assert_eq!(results[0], Some(100));
    assert_eq!(results[1], Some(200));
    assert_eq!(results[2], Some(300));
    assert_eq!(results[3], None, "missing key should return None");
}

// ---------------------------------------------------------------------------
// 11. Range queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn range_queries() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    map.enable_filtering().await;

    // Insert items with ordered keys.
    for i in 0..10u64 {
        map.insert(i, BytesEntry(i * 10)).await;
    }

    // Range [3..7) should yield keys 3,4,5,6.
    let mut collected = Vec::new();
    let count = map
        .range(3u64..7u64, |key, val| {
            collected.push((*key, val.0));
            true
        })
        .await;

    assert_eq!(count, 4, "range [3..7) should yield 4 items");
    assert_eq!(
        collected,
        vec![(3, 30), (4, 40), (5, 50), (6, 60)]
    );

    // Range with early termination: handler returns false to stop.
    // When handler returns false, count is NOT incremented for that item.
    // So collecting 2 items means: first returns true (count=1), second
    // returns false (break, count stays 1). We collect 2 but count is 1.
    let mut first_two = Vec::new();
    let count = map
        .range(0u64..10u64, |key, val| {
            first_two.push((*key, val.0));
            first_two.len() < 2 // stop after 2
        })
        .await;

    assert_eq!(first_two.len(), 2, "handler should have been called twice");
    assert_eq!(count, 1, "only the first item (where handler returned true) is counted");
}

// ---------------------------------------------------------------------------
// 11b. range() must not self-deadlock when a get() inside it triggers eviction
// ---------------------------------------------------------------------------
//
// Regression for the 2026-07-07 graceful-shutdown drain deadlock. During
// SIGTERM, `StoreManager::flush_slow_writes` calls `MemoryStore::list`, which
// delegates to `MokaEvictingMap::range`. `range` holds `self.btree.read()`
// across the iteration loop and calls `self.cache.get(q)` on each key. moka's
// `sync::Cache::get` performs amortized maintenance (`do_run_pending_tasks` →
// `evict_lru_entries`) and fires the eviction listener SYNCHRONOUSLY on the
// same stack; that listener acquires `self.btree.write()`. parking_lot's
// RwLock is non-reentrant and write-preferring, so the write acquire blocks
// forever waiting for the read guard held above it on the same stack — a
// self-deadlock (proven by addr2line of the wedged production binary:
// listener stuck in `RawRwLock::lock_exclusive_slow`, held under `range`'s
// `btree.read()` via `MemoryStore::list`).
//
// The fix snapshots the matching keys under the read lock, releases the lock,
// then does the `cache.get()` + handler pass with NO btree lock held, so the
// listener's `btree.write()` can never contend with a same-stack read guard.
//
// This test drives the exact re-entrancy: it over-fills the cache far past
// capacity via the deferred `insert_with_time` path (which does NOT call
// `run_pending_tasks`, so moka's write-op buffer fills and pending
// size-evictions queue up), then calls `range()`. The first `cache.get()`
// inside `range` runs the deferred maintenance → eviction listener →
// `btree.write()`. Without the fix `range` never returns; the timeout is the
// deadlock detector.
#[tokio::test]
async fn range_does_not_deadlock_on_eviction_during_iteration() {
    // Small byte cap (8 KiB) with 1 KiB entries: capacity ~8 entries. This
    // reproduces the exact production re-entrancy that wedged the server on
    // SIGTERM. moka's `sync::Cache::get` runs amortized maintenance
    // (`do_run_pending_tasks` → `evict_lru_entries`) once the read-op log
    // crosses READ_LOG_FLUSH_POINT (64) OR a prior partial eviction left the
    // `more_entries_to_evict` flag set — firing the eviction listener
    // SYNCHRONOUSLY on the calling stack. `range` calls `cache.get` while
    // holding `btree.read()`, and the listener acquires `btree.write()`.
    //
    // NOTE ON THE DEADLOCK DETECTOR: the bug is a parking_lot RwLock wait
    // (`lock_exclusive_slow`), which BLOCKS the OS thread — it does NOT yield
    // to the async runtime. So a plain `tokio::time::timeout` around `range`
    // cannot fire (its timer future never gets polled once the worker thread
    // is wedged). We therefore run `range` on a DEDICATED std::thread with
    // its own current-thread runtime and detect the hang by joining that
    // thread with a wall-clock deadline on THIS thread. If `range` wedges,
    // the dedicated thread never signals and we fail with a bespoke message.
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_thread = Arc::clone(&done);
    let count_slot: Arc<AtomicU64> = Arc::new(AtomicU64::new(u64::MAX));
    let count_thread = Arc::clone(&count_slot);

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async move {
            let cfg = policy(8 * 1024, 0, 0, 0);
            let map = make_map(&cfg);
            map.enable_filtering().await;

            // Phase 1: load + settle a full cache. These runtime inserts DO
            // run moka maintenance internally (moka's `insert` calls
            // `apply_reads_writes_if_needed`), so their evictions fire the
            // listener NOW — on the insert stack, where the btree lock is NOT
            // held (safe) — and drain to steady state. Keys 0..200.
            for i in 0..200u64 {
                map.insert(i, BytesEntry(1024)).await;
            }
            // Force any residual maintenance to completion so the cache is at
            // cap with an EMPTY pending-eviction backlog before phase 2.
            let _ = map.len_for_test().await;

            // Phase 2: queue fresh evictions via the STARTUP path
            // (`insert_with_time` → `insert_startup`), which deliberately
            // does NOT call `run_pending_tasks`. Keep the count under
            // WRITE_LOG_FLUSH_POINT (64) so moka does NOT auto-drain during
            // these inserts — the resulting size-driven evictions stay QUEUED
            // in moka's write log, to be processed by the first `get()` that
            // trips the flush point inside `range`. Keys 200..240 (40 < 64).
            for i in 200..240u64 {
                map.insert_with_time(i, BytesEntry(1024), -(i as i32)).await;
            }

            // Phase 3: `range` over the full key space (240 keys ⇒ >64
            // `cache.get` calls, guaranteeing the read-log flush point trips
            // and moka drains the queued phase-2 evictions mid-iteration →
            // eviction listener → `btree.write()`). If `range` holds
            // `btree.read()` across that `cache.get()`, the non-reentrant
            // write acquire blocks forever HERE and the thread never signals.
            let count = map
                .range(0u64..240u64, |_key, _val| true)
                .await;
            count_thread.store(count, Ordering::SeqCst);
            done_thread.store(true, Ordering::SeqCst);
        });
    });

    // Deadlock detector: poll the completion flag on THIS (independent)
    // thread for up to 10 s. `tokio::time::sleep` yields, so this loop stays
    // responsive even though the worker thread under test is blocked.
    let deadline = std::time::Instant::now() + core::time::Duration::from_secs(10);
    while !done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        tokio::time::sleep(core::time::Duration::from_millis(25)).await;
    }

    assert!(
        done.load(Ordering::SeqCst),
        "range() self-deadlocked: an eviction listener triggered by cache.get() \
         inside the iteration tried to take btree.write() while range held \
         btree.read() on the same stack (graceful-shutdown drain deadlock)"
    );
    // Thread finished; join it (returns immediately since it signaled done).
    handle.join().expect("range worker thread panicked");
    let count = count_slot.load(Ordering::SeqCst);
    assert!(
        count > 0 && count <= 240,
        "range should iterate the surviving entries, got count={count}"
    );
}

// ---------------------------------------------------------------------------
// 12. Concurrent stress test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_stress() {
    let cfg = policy(100 * 1024, 1000, 0, 0);
    let map = Arc::new(make_map(&cfg));

    let mut handles = Vec::new();
    for task_id in 0..10u64 {
        let map = Arc::clone(&map);
        handles.push(tokio::spawn(async move {
            let base = task_id * 1000;
            for i in 0..100u64 {
                let key = base + i;
                map.insert(key, BytesEntry(64)).await;
                let _ = map.get(&key).await;
                if i % 3 == 0 {
                    map.remove(&key).await;
                }
            }
        }));
    }

    // All tasks should complete without panics.
    for h in handles {
        h.await.expect("task should not panic");
    }

    // Map should be in a consistent state.
    let count = map.len_for_test().await;
    assert!(count > 0, "map should have some items after stress test");
}

// ---------------------------------------------------------------------------
// 13. Callbacks
// ---------------------------------------------------------------------------

/// Callback that tracks removal count and last-removed key.
#[derive(Debug, Clone)]
struct TrackingCallback {
    removal_count: Arc<AtomicU64>,
    insert_count: Arc<AtomicU64>,
}

impl TrackingCallback {
    fn new() -> Self {
        Self {
            removal_count: Arc::new(AtomicU64::new(0)),
            insert_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ItemCallback<u64> for TrackingCallback {
    fn callback(
        &self,
        _store_key: &u64,
        _ts_boot_epoch: u64,
        _ts_counter: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.removal_count.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {})
    }

    fn on_insert(&self, _store_key: &u64, _size: u64, _ts_boot_epoch: u64, _ts_counter: u64) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn callbacks_fire_on_insert_and_remove() {
    let cfg = policy(0, 100, 0, 0);
    let map = Arc::new(make_map_cb(&cfg));
    let cb = TrackingCallback::new();
    let removal_count = Arc::clone(&cb.removal_count);
    let insert_count = Arc::clone(&cb.insert_count);

    map.add_item_callback(cb);

    // Start background drainer so eviction callbacks are processed.
    map.start_background_eviction();

    // Insert fires on_insert callback.
    map.insert(1, BytesEntry(100)).await;
    assert_eq!(
        insert_count.load(Ordering::Relaxed),
        1,
        "on_insert should fire once"
    );

    map.insert(2, BytesEntry(200)).await;
    assert_eq!(
        insert_count.load(Ordering::Relaxed),
        2,
        "on_insert should fire again"
    );

    // Remove fires removal callback via eviction listener -> background drainer.
    map.remove(&1).await;
    // Give background task a moment to process the eviction event.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    assert_eq!(
        removal_count.load(Ordering::Relaxed),
        1,
        "removal callback should fire once"
    );
}

// ---------------------------------------------------------------------------
// Additional: size_for_key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn size_for_key_returns_correct_size() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    map.insert(42, BytesEntry(777)).await;
    assert_eq!(map.size_for_key(&42).await, Some(777));
    assert_eq!(map.size_for_key(&99).await, None);
}

// ---------------------------------------------------------------------------
// Additional: get_many
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_many_returns_correct_results() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    map.insert(1, BytesEntry(10)).await;
    map.insert(2, BytesEntry(20)).await;
    map.insert(3, BytesEntry(30)).await;

    let results = map.get_many(&[1, 2, 99, 3]).await;
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_ref().unwrap().0, 10);
    assert_eq!(results[1].as_ref().unwrap().0, 20);
    assert!(results[2].is_none());
    assert_eq!(results[3].as_ref().unwrap().0, 30);
}

// ---------------------------------------------------------------------------
// Additional: remove_if
// ---------------------------------------------------------------------------

#[tokio::test]
async fn remove_if_conditional() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    map.insert(1, BytesEntry(100)).await;

    // Condition false: should not remove.
    let removed = map.remove_if(&1, |entry| entry.0 > 200).await;
    assert!(!removed, "should not remove when condition is false");
    assert!(map.get(&1).await.is_some());

    // Condition true: should remove.
    let removed = map.remove_if(&1, |entry| entry.0 == 100).await;
    assert!(removed, "should remove when condition is true");
    assert!(map.get(&1).await.is_none());
}

// ---------------------------------------------------------------------------
// Additional: insert_many
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_many_batch() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    let items: Vec<(u64, BytesEntry)> = (0..5).map(|i| (i, BytesEntry(i * 100))).collect();
    map.insert_many(items).await;

    for i in 0..5u64 {
        let val = map.get(&i).await;
        assert!(val.is_some(), "key {i} should exist");
        assert_eq!(val.unwrap().0, i * 100);
    }
}

// ---------------------------------------------------------------------------
// Additional: pinned_bytes tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pinned_bytes_tracking() {
    let cfg = policy(100 * 1024, 0, 0, 0);
    let map = make_map(&cfg);

    map.insert(1, BytesEntry(1024)).await;
    map.insert(2, BytesEntry(2048)).await;
    assert_eq!(map.pinned_bytes(), 0);

    map.pin_key(1);
    assert_eq!(map.pinned_bytes(), 1024);

    map.pin_key(2);
    assert_eq!(map.pinned_bytes(), 1024 + 2048);

    map.unpin_key(&1);
    assert_eq!(map.pinned_bytes(), 2048);

    map.unpin_key(&2);
    assert_eq!(map.pinned_bytes(), 0);
}

// ---------------------------------------------------------------------------
// Additional: pin nonexistent key returns false
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pin_nonexistent_key_returns_false() {
    let cfg = policy(0, 100, 0, 0);
    let map = make_map(&cfg);

    assert!(!map.pin_key(999), "pinning nonexistent key should return false");
}
