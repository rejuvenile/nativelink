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

//! Regression test for #261: scheduler tree-resolution misses peer-fetch path.
//!
//! Production wiring in `src/bin/nativelink.rs` builds the cas store, then
//! wraps it with `WorkerProxyStore` (peer-fetch fallback for tiny Directory
//! blobs that live only on a worker). The wrapped `Store` Arc is
//! re-installed into `StoreManager` under the same name. The scheduler
//! factory then captures the `cas_store` clone via
//! `StoreManager::get_store`.
//!
//! Prior to the #261 fix, `scheduler_factory` ran BEFORE the wrap, so the
//! scheduler captured the raw chain (`SizePartitioning → Memory →
//! Filesystem`) with no peer-fetch fallback. Tree resolution surfaced
//! NotFound for digests that lived only on a peer worker (because
//! `bytestream_server`'s fast-path skipped the server-side persist on the
//! strength of `WorkerProxyStore::has() = Some` from the locality_map),
//! cached the failure for 60s in `failed_dir_digests`, and ~117 dependent
//! input_root resolutions cascade-failed.
//!
//! These tests assert the wiring contract that the production fix relies
//! on: after wrap-then-factory, the scheduler's captured `cas_store`
//! downcasts to `WorkerProxyStore`. The mutation step (factory-then-wrap)
//! red-fails with the bespoke message.

use core::time::Duration;
use std::sync::Arc;

use nativelink_config::schedulers::{SchedulerSpec, SimpleSpec};
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::store_trait::Store;

const CAS_NAME: &str = "cas_main";

/// Build a `StoreManager` containing a single `MemoryStore` registered as
/// `CAS_NAME`. Mirrors the early portion of `inner_main` between the
/// `for StoreConfig` loop and the WorkerProxyStore wrap block.
fn fresh_store_manager_with_memory_cas() -> Arc<StoreManager> {
    let sm = Arc::new(StoreManager::new());
    let inner = MemoryStore::new(&MemorySpec::default());
    sm.add_store(CAS_NAME, Store::new(inner));
    sm
}

/// Wraps the existing `CAS_NAME` entry in `sm` with a `WorkerProxyStore`
/// and re-installs it. Mirrors `src/bin/nativelink.rs:319-369`.
fn wrap_cas_with_worker_proxy(sm: &StoreManager) {
    let original = sm.get_store(CAS_NAME).expect("cas store registered");
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(original, locality_map);
    sm.add_store(CAS_NAME, Store::new(proxy_arc));
}

/// `SchedulerSpec::Simple` with `cas_store: Some("cas_main")` so the
/// factory resolves the CAS store via `StoreManager::get_store`.
fn simple_spec_with_cas() -> SchedulerSpec {
    SchedulerSpec::Simple(SimpleSpec {
        cas_store: Some(CAS_NAME.to_string()),
        ..Default::default()
    })
}

/// Returns true iff the Store's IMMEDIATE inner StoreDriver is a
/// WorkerProxyStore. This is the production-composition assertion: the
/// scheduler must hold the WRAPPED chain, not the raw chain.
///
/// We MUST check the immediate driver via `into_inner()` rather than
/// `Store::inner_store(None)`, because `WorkerProxyStore::inner_store`
/// (worker_proxy_store.rs:3172) intentionally delegates DOWN to the
/// inner store so other consumers can downcast through the wrap to find
/// FastSlowStore etc. So `inner_store(None)` would walk PAST
/// WorkerProxyStore and return MemoryStore, defeating the test.
fn is_worker_proxy_wrapped(store: &Store) -> bool {
    let arc = store.clone().into_inner();
    arc.as_any().downcast_ref::<WorkerProxyStore>().is_some()
}

#[nativelink_test]
async fn wrap_then_factory_yields_worker_proxy_wrapped_cas_store() -> Result<(), Error> {
    // Production sequence (post-#261 fix): store registered →
    // WorkerProxyStore wrap → scheduler_factory. The factory's internal
    // `store_manager.get_store(CAS_NAME)` (see
    // `default_scheduler_factory.rs:122-126`) resolves the wrapped Arc,
    // and the scheduler exposes it via the `WorkerScheduler::cas_store`
    // trait method.
    let sm = fresh_store_manager_with_memory_cas();
    // MUTATION-TEST TOGGLE: comment out this line to simulate the
    // pre-#261-fix ordering. Test must red-fail with the bespoke
    // message in the assert! below — proving it actually guards the
    // wrap-then-factory ordering. Verified red-failing 2026-05-05.
    wrap_cas_with_worker_proxy(&sm);

    let spec = simple_spec_with_cas();
    let (_action, worker_sched) = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler_factory(&spec, &sm, None, None, None),
    )
    .await
    .expect("scheduler must reach peer-held tiny digests via WorkerProxyStore — #261")?;

    let worker_sched = worker_sched.expect("simple spec yields a worker scheduler");
    let captured = worker_sched
        .cas_store()
        .expect("scheduler must capture cas_store when SimpleSpec::cas_store is set");

    assert!(
        is_worker_proxy_wrapped(captured),
        "scheduler must reach peer-held tiny digests via WorkerProxyStore — #261",
    );
    Ok(())
}

#[nativelink_test]
async fn factory_then_wrap_captures_unwrapped_cas_store() -> Result<(), Error> {
    // Mutation reproduction of the PRE-#261-fix ordering: factory FIRST,
    // wrap AFTER. The scheduler captures a clone of the unwrapped Store.
    // Because the wrap mutates the StoreManager entry (not the cloned
    // Arc the scheduler already holds), the scheduler's captured
    // `cas_store` is still the raw chain. Asserting the absence of the
    // WorkerProxyStore wrap proves the test family is load-bearing —
    // the post-fix ordering case above is not passing trivially.
    let sm = fresh_store_manager_with_memory_cas();

    let spec = simple_spec_with_cas();
    let (_action, worker_sched) = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler_factory(&spec, &sm, None, None, None),
    )
    .await
    .expect("factory must complete within 5s")?;

    // Wrap AFTER the factory captured its clone — the wrap reaches the
    // StoreManager but not the scheduler's already-captured Arc.
    wrap_cas_with_worker_proxy(&sm);

    let worker_sched = worker_sched.expect("simple spec yields a worker scheduler");
    let captured = worker_sched
        .cas_store()
        .expect("scheduler must capture cas_store when SimpleSpec::cas_store is set");

    assert!(
        !is_worker_proxy_wrapped(captured),
        "pre-#261-fix ordering must capture the unwrapped cas_store \
         (this asserts the mutation step proves the contract is real)",
    );
    // Sanity: the StoreManager's entry IS now wrapped (so we know the
    // wrap actually ran) — even though the scheduler's clone isn't.
    let post_wrap = sm.get_store(CAS_NAME).expect("cas store registered");
    assert!(
        is_worker_proxy_wrapped(&post_wrap),
        "wrap_cas_with_worker_proxy must replace the StoreManager entry",
    );
    Ok(())
}

#[nativelink_test]
async fn no_cas_store_in_spec_yields_none() -> Result<(), Error> {
    // `WorkerScheduler::cas_store` returns None when no `cas_store` is
    // configured in the SimpleSpec — guards against accidental future
    // changes that always-Some the field.
    let sm = fresh_store_manager_with_memory_cas();
    let spec = SchedulerSpec::Simple(SimpleSpec {
        cas_store: None,
        ..Default::default()
    });
    let (_action, worker_sched) = scheduler_factory(&spec, &sm, None, None, None).await?;
    let worker_sched = worker_sched.expect("simple spec yields a worker scheduler");
    assert!(
        worker_sched.cas_store().is_none(),
        "scheduler must NOT capture a cas_store when the SimpleSpec field is None",
    );
    Ok(())
}
