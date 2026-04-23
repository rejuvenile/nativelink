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
use core::ptr::from_ref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use nativelink_config::stores::{MemorySpec, RefSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;
use std::sync::mpsc;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

fn setup_stores() -> (Arc<StoreManager>, Store, Store) {
    let store_manager = Arc::new(StoreManager::new());

    let memory_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    store_manager.add_store("foo", memory_store.clone());

    let ref_store = Store::new(RefStore::new(
        &RefSpec {
            name: "foo".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("bar", ref_store.clone());
    (store_manager, memory_store, ref_store)
}

#[nativelink_test]
async fn has_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into memory store.
        memory_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we check of ref_store has the data.
        let has_result = ref_store
            .has(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?)
            .await;
        assert_eq!(
            has_result,
            Ok(Some(VALUE1.len() as u64)),
            "Expected ref store to have data in ref store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn get_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into memory store.
        memory_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we read it from ref_store it has same data.
        let data = ref_store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            VALUE1.as_bytes(),
            "Expected ref store to have data in ref store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn update_test() -> Result<(), Error> {
    const VALUE1: &str = "13";

    let (_store_manager, memory_store, ref_store) = setup_stores();

    {
        // Insert data into ref_store.
        ref_store
            .update_oneshot(
                DigestInfo::try_new(VALID_HASH1, VALUE1.len())?,
                VALUE1.into(),
            )
            .await?;
    }
    {
        // Now check if we read it from memory_store it has same data.
        let data = memory_store
            .get_part_unchunked(DigestInfo::try_new(VALID_HASH1, VALUE1.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            data,
            VALUE1.as_bytes(),
            "Expected ref store to have data in memory store : {}",
            VALID_HASH1
        );
    }
    Ok(())
}

#[nativelink_test]
async fn inner_store_test() -> Result<(), Error> {
    let store_manager = Arc::new(StoreManager::new());

    let memory_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    store_manager.add_store("mem_store", memory_store.clone());

    let ref_store_inner = Store::new(RefStore::new(
        &RefSpec {
            name: "mem_store".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("ref_store_inner", ref_store_inner);

    let ref_store_outer = Store::new(RefStore::new(
        &RefSpec {
            name: "ref_store_inner".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    store_manager.add_store("ref_store_outer", ref_store_outer.clone());

    // Ensure the result of inner_store() points to exact same memory store.
    assert_eq!(
        from_ref::<dyn StoreDriver>(ref_store_outer.inner_store(Option::<DigestInfo>::None))
            .cast::<()>(),
        from_ref::<dyn StoreDriver>(memory_store.into_inner().as_ref()).cast::<()>(),
        "Expected inner store to be memory store"
    );
    Ok(())
}

// ---- Regression test for callback-race audit F1 -------------------------------------------
//
// `RefStore::register_item_callback` and `RefStore::get_store()` slow-path init must not
// interleave such that a callback registered concurrent with the slow-path init is silently
// dropped. The bug:
//
//   T1 (get_store slow path): snapshot `item_callbacks` -> [register on inner] -> publish *ref_store
//   T2 (register_item_callback): push into `item_callbacks` -> read *ref_store == None -> return
//
// If T1 takes its snapshot BEFORE T2 pushes, but T2 reads *ref_store BEFORE T1 publishes,
// then T2's callback never reaches the inner store: T1's snapshot doesn't include it, and T2
// can't propagate it directly.
//
// To reproduce deterministically we use a probe inner store whose `register_item_callback`
// blocks on a `Notify`, so that `get_store()` is paused exactly between snapshot (after the
// callback loop entered) and publish (after the loop completes).

#[derive(Debug)]
struct CountingCallback {
    on_insert_count: AtomicUsize,
}

impl CountingCallback {
    fn new() -> Self {
        Self {
            on_insert_count: AtomicUsize::new(0),
        }
    }
}

impl ItemCallback for CountingCallback {
    fn callback<'a>(
        &'a self,
        _store_key: StoreKey<'a>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }

    fn on_insert(&self, _store_key: StoreKey<'_>, _size: u64) {
        self.on_insert_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, MetricsComponent)]
struct ProbeStore {
    // Once a register call enters, signals `entered` (async-side) and blocks on `release_rx`.
    entered: Arc<Notify>,
    release_rx: Mutex<Option<mpsc::Receiver<()>>>,
    callbacks: Mutex<Vec<Arc<dyn ItemCallback>>>,
    // After the gate fires the first time, subsequent registers pass through immediately.
    gate_fired: std::sync::atomic::AtomicBool,
}

impl ProbeStore {
    fn new(entered: Arc<Notify>, release_rx: mpsc::Receiver<()>) -> Arc<Self> {
        Arc::new(Self {
            entered,
            release_rx: Mutex::new(Some(release_rx)),
            callbacks: Mutex::new(vec![]),
            gate_fired: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn fire_all_inserts(&self, key: StoreKey<'_>) {
        let cbs = self.callbacks.lock().clone();
        for cb in cbs {
            cb.on_insert(key.borrow(), 1);
        }
    }
}

#[async_trait]
impl StoreDriver for ProbeStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(nativelink_error::make_err!(
            nativelink_error::Code::NotFound,
            "probe store does not implement get_part",
        ))
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
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
        // Park the FIRST register call (the one inside RefStore::get_store slow-path snapshot)
        // until the test releases it. Subsequent calls (from RefStore::register_item_callback
        // direct propagation after publish) pass through immediately.
        let already_fired = self.gate_fired.swap(true, Ordering::SeqCst);
        if !already_fired {
            // Take ownership of the one-shot receiver and block this thread on it. We can't
            // `.await` here (sync trait method); the test releases us by sending on `release_tx`
            // from the async test body. parking_lot::Mutex is a sync mutex so this is safe.
            let rx = self
                .release_rx
                .lock()
                .take()
                .expect("release_rx already consumed");
            self.entered.notify_one();
            rx.recv().expect("release channel should fire");
        }
        self.callbacks.lock().push(callback);
        Ok(())
    }
}

default_health_status_indicator!(ProbeStore);

#[nativelink_test(flavor = "multi_thread", worker_threads = 4)]
async fn register_callback_race_with_slow_path_init() -> Result<(), Error> {
    // 1) Build a StoreManager that maps "probe" to our ProbeStore, and "ref_to_probe" to a
    //    RefStore pointing at it. The RefStore won't resolve "probe" until the first
    //    `get_store()` call.
    let store_manager = Arc::new(StoreManager::new());
    let entered = Arc::new(Notify::new());
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let probe = ProbeStore::new(entered.clone(), release_rx);
    store_manager.add_store("probe", Store::new(probe.clone()));

    let ref_store_arc = RefStore::new(
        &RefSpec {
            name: "probe".to_string(),
        },
        Arc::downgrade(&store_manager),
    );
    let ref_store = Store::new(ref_store_arc.clone());

    // 2) Pre-register callback A. Since ref_store hasn't resolved yet, A only goes into the
    //    `item_callbacks` Vec — nothing is propagated to the (still-None) inner store.
    let cb_a = Arc::new(CountingCallback::new());
    ref_store.register_item_callback(cb_a.clone())?;

    // 3) Spawn task T1: triggers `get_store()` slow path. Its callback-snapshot loop will
    //    call ProbeStore::register_item_callback for A, which blocks until we notify
    //    `release`. While blocked, *ref_store is still None.
    let ref_store_t1 = ref_store.clone();
    let t1 = tokio::spawn(async move {
        // `has` -> get_store() -> snapshot item_callbacks -> register A on probe (BLOCKS)
        drop(ref_store_t1.has(DigestInfo::try_new(VALID_HASH1, 1)?).await);
        Ok::<(), Error>(())
    });

    // 4) Wait until T1 is actually inside the blocked register call (i.e., T1 has taken
    //    `inner.mux` and is mid-snapshot).
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("T1 should reach blocked register within 5s");

    // 5) From a DIFFERENT task T2, call `RefStore::register_item_callback(B)`. With the bug,
    //    this completes immediately (sees *ref_store == None, doesn't propagate, returns Ok).
    //    With the fix, this BLOCKS on `inner.mux` until T1's get_store() finishes — so we
    //    spawn it.
    let cb_b = Arc::new(CountingCallback::new());
    let ref_store_t2 = ref_store.clone();
    let cb_b_for_t2 = cb_b.clone();
    let t2 = tokio::spawn(async move {
        ref_store_t2.register_item_callback(cb_b_for_t2)?;
        Ok::<(), Error>(())
    });

    // Give T2 a chance to run and hit the lock (or, in the buggy case, complete).
    // We use yield + a short polling loop instead of sleep-as-sync. T2 either finishes
    // (buggy code) or parks on `inner.mux` (fixed code). Either way, after a few yields its
    // state is settled.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }

    // 6) Release T1. This unblocks ProbeStore's first register, lets T1 publish *ref_store,
    //    drop `inner.mux`, and (with the fix) lets T2 acquire `inner.mux`, see Some(store),
    //    and propagate B.
    release_tx.send(()).expect("release send");

    t1.await
        .expect("t1 join")
        .expect("t1 inner has should not error");
    t2.await.expect("t2 join").expect("t2 register err");

    // 7) Trigger an insert on the probe store and check both callbacks fire.
    probe.fire_all_inserts(StoreKey::from(DigestInfo::try_new(VALID_HASH1, 1)?));

    assert_eq!(
        cb_a.on_insert_count.load(Ordering::SeqCst),
        1,
        "callback A (registered before slow-path init) must receive insert events"
    );
    assert_eq!(
        cb_b.on_insert_count.load(Ordering::SeqCst),
        1,
        "callback B (registered concurrently with slow-path init) must NOT be silently dropped"
    );

    Ok(())
}
