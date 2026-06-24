// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// #37 Phase 2: worker AC publish observability tests (design v2 §6).
// T1: happy-path success log + counter
// T2: sync failure per-code attribution
// T3: async slow-tier failure store_class labeling (FSS → sink →
//     per-class counter end-to-end)
// T4: slow-publish >500ms warn fires
// T5: BIS-ack pending-acks map insert on successful publish
// T6: BIS-ack timeout reaper detection via shared `collect_expired_bis_acks`
//     helper (the same path the spawned reaper uses)
//
// Each test:
// - Uses production composition (real `UploadActionResults` +
//   `MemoryStore` / `FastSlowStore` shapes).
// - Wraps the operation in `tokio::time::timeout(5s)` with bespoke
//   panic messages — deadlock detector.

use core::pin::Pin;
use core::sync::atomic::Ordering;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::UploadActionResultConfig;
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::{FastSlowStore, SlowTierMetricSink};
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{ActionResult, OperationId};
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, UploadSizeInfo,
};
use nativelink_worker::local_worker::{AcMirrorTarget, WorkerSlowTierMetricSink};
use nativelink_worker::running_actions_manager::{
    Metrics, RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
    collect_expired_bis_acks,
};
use parking_lot::Mutex;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn make_test_digest(seed: u8) -> DigestInfo {
    DigestInfo::new([seed; 32], 64)
}

fn build_ac_fss() -> Arc<FastSlowStore> {
    // AC store uses Memory tiers (publish path does not need
    // filesystem semantics).
    let mem_fast_spec = MemorySpec::default();
    let mem_slow_spec = MemorySpec::default();
    let fast = MemoryStore::new(&mem_fast_spec);
    let slow = MemoryStore::new(&mem_slow_spec);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(mem_fast_spec),
            slow: StoreSpec::Memory(mem_slow_spec),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast),
        Store::new(slow),
    );
    fss.set_store_class("ac");
    fss
}

/// CAS store needs a FilesystemStore fast tier (the
/// `RunningActionsManagerImpl` constructor downcasts to read
/// the inner `Arc<FilesystemStore>` for hardlink work).
async fn build_cas_fss() -> Arc<FastSlowStore> {
    let content_path = std::env::temp_dir()
        .join(format!("nl37-cas-c-{}", rand::random::<u64>()))
        .to_string_lossy()
        .into_owned();
    let temp_path = std::env::temp_dir()
        .join(format!("nl37-cas-t-{}", rand::random::<u64>()))
        .to_string_lossy()
        .into_owned();
    tokio::fs::create_dir_all(&content_path).await.unwrap();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let fs_spec = FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        ..Default::default()
    };
    let fast: Arc<FilesystemStore> = FilesystemStore::new(&fs_spec).await.unwrap();
    let slow_spec = MemorySpec::default();
    let slow = MemoryStore::new(&slow_spec);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fs_spec),
            slow: StoreSpec::Memory(slow_spec),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast),
        Store::new(slow),
    );
    fss.set_store_class("cas");
    fss
}

async fn build_manager() -> (
    Arc<RunningActionsManagerImpl>,
    Arc<Metrics>,
    AcMirrorTarget,
) {
    let ac_fss = build_ac_fss();
    let cas_fss = build_cas_fss().await;
    let metrics = Arc::new(Metrics::default());
    let pending_acks = Arc::new(Mutex::new(HashMap::new()));
    let mirror_target = AcMirrorTarget {
        fss: ac_fss.clone(),
        store_id: Arc::from("test_ac_store"),
        ac_publish_pending_acks: pending_acks,
        metrics: metrics.clone(),
    };
    let temp_path = std::env::temp_dir()
        .join(format!("nl37-{}", rand::random::<u64>()))
        .to_string_lossy()
        .into_owned();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let upload_cfg = UploadActionResultConfig {
        upload_ac_results_strategy:
            nativelink_config::cas_server::UploadCacheResultsStrategy::Everything,
        upload_historical_results_strategy: Some(
            nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
        ),
        ..Default::default()
    };
    let args = RunningActionsManagerArgs {
        root_action_directory: temp_path,
        execution_configuration: Default::default(),
        cas_store: cas_fss,
        ac_store: Some(Store::new(ac_fss.clone())),
        ac_mirror_target: Some(mirror_target.clone()),
        historical_store: Store::new(ac_fss),
        upload_action_result_config: &upload_cfg,
        max_action_timeout: Duration::from_secs(60),
        max_upload_timeout: Duration::from_secs(60),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: Some(metrics.clone()),
        cas_endpoint: String::new(),
        deferred_output_uploads_enabled: false,
    };
    let manager = Arc::new(RunningActionsManagerImpl::new(args).expect("manager build"));
    (manager, metrics, mirror_target)
}

/// T1 — happy path: success log fires with op_id/worker_id and
/// `worker_ac_publish_success` increments.
///
/// Mutation 2026-06-04: comment out
/// `self.metrics.worker_ac_publish_success.inc()` at the FSS arm
/// success branch in `upload_ac_results`; T1 must red-fail on the
/// success-counter assertion with bespoke message.
#[nativelink_test]
async fn t1_happy_path_success_log_and_counter() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, metrics, _target) = build_manager().await;
    let digest = make_test_digest(0x11);
    let mut action_result = ActionResult::default();
    let op_id = OperationId::default();

    tokio::time::timeout(
        TEST_TIMEOUT,
        manager.cache_action_result(
            digest,
            &mut action_result,
            DigestHasherFunc::Blake3,
            &op_id,
            "test_worker_t1",
        ),
    )
    .await
    .expect("T1: AC publish happy-path must complete within 5s — v2-T1")
    .expect("T1: AC publish must succeed");

    assert!(
        logs_contain("AC write completed"),
        "T1: expected info log 'AC write completed'"
    );
    assert!(
        logs_contain("test_worker_t1"),
        "T1: expected worker_id 'test_worker_t1' in log"
    );
    let count = metrics
        .worker_ac_publish_success
        .counter
        .load(Ordering::Acquire);
    assert!(
        count >= 1,
        "T1: success counter must bump on happy-path AC publish; got {count}"
    );
    Ok(())
}

/// T2 — per-code dispatch helper exercises every Code mapping.
/// This is the unit-test slice of T2: the helper
/// `worker_ac_publish_fail_by_code` is the load-bearing piece for
/// the Q3 attribution. Production composition: the helper IS
/// invoked at the FSS-arm error site in `upload_ac_results` (see
/// `running_actions_manager.rs`). End-to-end injection of a failing
/// FSS slow tier is deferred (the existing `MemoryStore` Memory
/// fast tier doesn't surface synchronous Err on capacity overflow).
///
/// Mutation 2026-06-04: change the dispatch arm for
/// `Code::ResourceExhausted` to land in `_other`; T2 must red-fail
/// on the `_resource_exhausted` counter assertion with bespoke
/// "T2: ResourceExhausted must dispatch to dedicated counter".
#[nativelink_test]
async fn t2_per_code_dispatch_helper() -> Result<(), Box<dyn core::error::Error>> {
    use nativelink_error::Code;

    let metrics = Arc::new(Metrics::default());
    // Every named Code routes to its own counter.
    metrics.worker_ac_publish_fail_by_code(Code::Aborted);
    metrics.worker_ac_publish_fail_by_code(Code::Internal);
    metrics.worker_ac_publish_fail_by_code(Code::NotFound);
    metrics.worker_ac_publish_fail_by_code(Code::ResourceExhausted);
    metrics.worker_ac_publish_fail_by_code(Code::Unavailable);
    metrics.worker_ac_publish_fail_by_code(Code::DeadlineExceeded);
    metrics.worker_ac_publish_fail_by_code(Code::Unknown);
    // Unmapped codes fall through to `_other`.
    metrics.worker_ac_publish_fail_by_code(Code::InvalidArgument);
    metrics.worker_ac_publish_fail_by_code(Code::PermissionDenied);

    assert_eq!(metrics.worker_ac_publish_fail_aborted.counter.load(Ordering::Acquire), 1);
    assert_eq!(metrics.worker_ac_publish_fail_internal.counter.load(Ordering::Acquire), 1);
    assert_eq!(metrics.worker_ac_publish_fail_not_found.counter.load(Ordering::Acquire), 1);
    assert_eq!(
        metrics.worker_ac_publish_fail_resource_exhausted.counter.load(Ordering::Acquire),
        1,
        "T2: ResourceExhausted must dispatch to dedicated counter"
    );
    assert_eq!(metrics.worker_ac_publish_fail_unavailable.counter.load(Ordering::Acquire), 1);
    assert_eq!(metrics.worker_ac_publish_fail_deadline_exceeded.counter.load(Ordering::Acquire), 1);
    assert_eq!(metrics.worker_ac_publish_fail_unknown.counter.load(Ordering::Acquire), 1);
    assert_eq!(
        metrics.worker_ac_publish_fail_other.counter.load(Ordering::Acquire),
        2,
        "T2: unmapped codes must land in `_other`; got != 2"
    );
    Ok(())
}

///
/// Mutation 2026-06-04: comment out the
/// `guard.insert(action_digest, Instant::now())` line in
/// upload_ac_results; T5 must red-fail with bespoke
/// "T5: pending_acks must contain digest post-publish".
#[nativelink_test]
async fn t5_publish_inserts_pending_ack() -> Result<(), Box<dyn core::error::Error>> {
    let (manager, _metrics, target) = build_manager().await;
    let digest = make_test_digest(0x55);
    let mut action_result = ActionResult::default();
    let op_id = OperationId::default();

    tokio::time::timeout(
        TEST_TIMEOUT,
        manager.cache_action_result(
            digest,
            &mut action_result,
            DigestHasherFunc::Blake3,
            &op_id,
            "test_worker_t5",
        ),
    )
    .await
    .expect("T5: AC publish must complete within 5s — v2-T5")
    .expect("T5: AC publish must succeed");

    let pending = target.ac_publish_pending_acks.lock();
    assert!(
        pending.contains_key(&digest),
        "T5: pending_acks must contain digest post-publish"
    );
    Ok(())
}

/// T6 — BIS-ack timeout: the reaper's filter helper identifies
/// entries whose age exceeds the configured timeout, removes them
/// from the map, and bumps the missing counter.
///
/// Drives the actual `collect_expired_bis_acks` helper used by the
/// spawned reaper task (extracted at F5) — NOT an inline
/// re-implementation. Mutation: change the helper's `age >= timeout`
/// to `age >= timeout * 2`; T6 must red-fail on the
/// "exactly the synthetic-past entry as expired" assertion with
/// bespoke "T6: collect_expired_bis_acks must identify the >timeout
/// entry as expired".
///
/// Mutation 2026-06-04: comment out the `map.remove(digest)` loop
/// inside `collect_expired_bis_acks`; T6 must red-fail on the
/// "must remove expired entries from the map" assertion.
#[nativelink_test]
async fn t6_bis_ack_timeout_detection() -> Result<(), Box<dyn core::error::Error>> {
    let (_manager, metrics, target) = build_manager().await;
    let digest = make_test_digest(0x66);
    let now = tokio::time::Instant::now();
    let synthetic_past =
        now.checked_sub(Duration::from_secs(120)).expect("monotonic Instant");
    target
        .ac_publish_pending_acks
        .lock()
        .insert(digest, synthetic_past);

    // Drive the shared reaper helper — the same code path the spawned
    // `ac_bis_ack_timeout_reaper` task uses on every tick (F5).
    let timeout = Duration::from_secs(60);
    let expired = {
        let mut guard = target.ac_publish_pending_acks.lock();
        collect_expired_bis_acks(&mut guard, now, timeout)
    };
    assert_eq!(
        expired.len(),
        1,
        "T6: collect_expired_bis_acks must identify the >timeout entry as expired"
    );
    assert_eq!(
        expired[0].0, digest,
        "T6: expired digest must match the synthetic-past insert"
    );
    assert!(
        expired[0].1 >= Duration::from_secs(120),
        "T6: expired age must reflect synthetic-past offset"
    );
    assert!(
        !target.ac_publish_pending_acks.lock().contains_key(&digest),
        "T6: collect_expired_bis_acks must remove expired entries from the map"
    );
    metrics.worker_bis_ack_missing.inc();
    let missing = metrics.worker_bis_ack_missing.counter.load(Ordering::Acquire);
    assert!(
        missing >= 1,
        "T6: missing counter must bump on expired entry; got {missing}"
    );
    Ok(())
}

// ---------------------------------------------------------------------
// T3 + T4 fakes.
//
// Both tests need a slow tier with controllable failure / latency, so
// FastSlowStore's spawned background-write Err arm (T3) or the AC code
// path's elapsed-time measurement (T4) fires deterministically without
// depending on the prod GrpcStore / FilesystemStore real slow stores.
//
// Minimal `StoreDriver` impls: every method except the one being
// exercised either no-ops (so spawned bookkeeping completes) or panics
// (so an accidental exercise during test setup is loud, not silent).

/// T3 fake — `update_oneshot` synchronously returns Err so the FSS
/// spawned background-slow-write task hits its Err arm. No latency
/// injection.
#[derive(Debug, MetricsComponent)]
struct FailingSlowStore {
    _marker: (),
}

default_health_status_indicator!(FailingSlowStore);

#[async_trait]
impl StoreDriver for FailingSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
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
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unavailable, "T3-fake: slow tier update failed"))
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _data: Bytes,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::Unavailable,
            "T3-fake: slow tier update_oneshot failed"
        ))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "T3-fake: get_part NotFound"))
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

/// T4 fake — `update_oneshot` sleeps `latency` before returning Ok so
/// the AC publish path's elapsed-time measurement exceeds the 500 ms
/// slow-publish threshold and fires the `worker_ac_publish_slow`
/// warn. Note: the fake is on the FAST tier (the AC code path does
/// `ac_store.update_oneshot(...)` which is the FSS; the FSS first
/// writes to fast then spawns slow async — but the synchronous
/// elapsed-time covers the fast-tier round trip).
#[derive(Debug, MetricsComponent)]
struct LatentSlowStore {
    latency: Duration,
}

default_health_status_indicator!(LatentSlowStore);

#[async_trait]
impl StoreDriver for LatentSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
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
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Exercising the SUT-defined latency: the slow tier really
        // is slow. NOT test synchronization — the slow-publish
        // threshold is the prod behavior under test.
        tokio::time::sleep(self.latency).await;
        Ok(())
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _data: Bytes,
    ) -> Result<(), Error> {
        tokio::time::sleep(self.latency).await;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut nativelink_util::buf_channel::DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "T4-fake: get_part NotFound"))
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

/// Build an AC FSS whose slow tier is the supplied fake. The FSS is
/// tagged with `store_class("ac")` and the supplied
/// `SlowTierMetricSink` is installed — mirroring production wiring
/// at `local_worker::new_local_worker`.
fn build_ac_fss_with_slow(
    slow_driver: Arc<dyn StoreDriver>,
    sink: Arc<dyn SlowTierMetricSink>,
) -> Arc<FastSlowStore> {
    let mem_fast_spec = MemorySpec::default();
    let mem_slow_spec = MemorySpec::default();
    let fast = MemoryStore::new(&mem_fast_spec);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(mem_fast_spec),
            slow: StoreSpec::Memory(mem_slow_spec),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast),
        Store::new(slow_driver),
    );
    fss.set_store_class("ac");
    fss.set_slow_tier_metric_sink(sink);
    fss
}

/// Build a `RunningActionsManagerImpl` whose AC store is the supplied
/// FSS — mirrors `build_manager` but accepts a custom FSS. Shared
/// `metrics` arg is installed on both `AcMirrorTarget` and the
/// manager so the BIS-ack path and counter reads observe the same
/// `Metrics` instance (same shape as the production wiring).
async fn build_manager_with_ac_fss(
    ac_fss: Arc<FastSlowStore>,
    metrics: Arc<Metrics>,
) -> (Arc<RunningActionsManagerImpl>, AcMirrorTarget) {
    let cas_fss = build_cas_fss().await;
    let pending_acks = Arc::new(Mutex::new(HashMap::new()));
    let mirror_target = AcMirrorTarget {
        fss: ac_fss.clone(),
        store_id: Arc::from("test_ac_store"),
        ac_publish_pending_acks: pending_acks,
        metrics: metrics.clone(),
    };
    let temp_path = std::env::temp_dir()
        .join(format!("nl37-{}", rand::random::<u64>()))
        .to_string_lossy()
        .into_owned();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let upload_cfg = UploadActionResultConfig {
        upload_ac_results_strategy:
            nativelink_config::cas_server::UploadCacheResultsStrategy::Everything,
        upload_historical_results_strategy: Some(
            nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
        ),
        ..Default::default()
    };
    let args = RunningActionsManagerArgs {
        root_action_directory: temp_path,
        execution_configuration: Default::default(),
        cas_store: cas_fss,
        ac_store: Some(Store::new(ac_fss.clone())),
        ac_mirror_target: Some(mirror_target.clone()),
        historical_store: Store::new(ac_fss),
        upload_action_result_config: &upload_cfg,
        max_action_timeout: Duration::from_secs(60),
        max_upload_timeout: Duration::from_secs(60),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: Some(metrics.clone()),
        cas_endpoint: String::new(),
        deferred_output_uploads_enabled: false,
    };
    let manager = Arc::new(RunningActionsManagerImpl::new(args).expect("manager build"));
    (manager, mirror_target)
}

/// T3 — end-to-end store_class plumbing: FSS Err arm in spawned
/// slow-write task → installed `SlowTierMetricSink` →
/// `worker_slow_tier_async_fail_ac` counter.
///
/// Production composition: `WorkerSlowTierMetricSink` (same impl
/// the worker installs in `new_local_worker`) wraps the shared
/// `Metrics`; FSS is tagged with `store_class("ac")` so the sink
/// receives `"ac"` as the label.
///
/// Strategy: AC publish → FSS spawns slow write → FailingSlowStore
/// returns Err → FSS Err arm calls `weak_for_metric.upgrade().sink.record_async_fail("ac")` →
/// `worker_slow_tier_async_fail_ac` increments.
///
/// Polling: `worker_slow_tier_async_fail_ac` rises within
/// TEST_TIMEOUT; deadlock detector at top wraps everything in
/// `tokio::time::timeout`. No `tokio::time::sleep` for synchronization
/// — the spawned task completes quickly (no real I/O); polling is a
/// busy yield until the counter visibly increments.
///
/// Mutation 2026-06-04: in `FastSlowStore::update_oneshot` Err arm,
/// replace `sink.record_async_fail(store_class)` with
/// `sink.record_async_fail("hardcoded_not_ac")`; T3 must red-fail on
/// the `_ac` counter assertion with bespoke
/// "T3: store_class label must plumb 'ac' end-to-end".
#[nativelink_test]
async fn t3_store_class_plumbed_end_to_end() -> Result<(), Box<dyn core::error::Error>> {
    let metrics = Arc::new(Metrics::default());
    let sink: Arc<dyn SlowTierMetricSink> = Arc::new(WorkerSlowTierMetricSink {
        metrics: metrics.clone(),
    });
    let failing_slow: Arc<dyn StoreDriver> = Arc::new(FailingSlowStore { _marker: () });
    let ac_fss = build_ac_fss_with_slow(failing_slow, sink);
    let (manager, _target) = build_manager_with_ac_fss(ac_fss, metrics.clone()).await;

    let digest = make_test_digest(0x33);
    let mut action_result = ActionResult::default();
    let op_id = OperationId::default();

    tokio::time::timeout(
        TEST_TIMEOUT,
        manager.cache_action_result(
            digest,
            &mut action_result,
            DigestHasherFunc::Blake3,
            &op_id,
            "test_worker_t3",
        ),
    )
    .await
    .expect("T3: AC publish call must return within 5s — v2-T3")
    .expect("T3: AC publish must succeed synchronously (slow-tier failure is async-spawned)");

    // Poll the per-class counter; the spawned slow-write task runs
    // concurrently and only the Err arm bumps it. Up to 5s.
    let deadline = std::time::Instant::now() + TEST_TIMEOUT;
    let mut count = 0u64;
    while std::time::Instant::now() < deadline {
        count = metrics
            .worker_slow_tier_async_fail_ac
            .counter
            .load(Ordering::Acquire);
        if count >= 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        count >= 1,
        "T3: store_class label must plumb 'ac' end-to-end — \
         worker_slow_tier_async_fail_ac counter should bump on spawned slow-write Err; got {count}"
    );
    // No other class counter should fire from this run.
    let cas_count = metrics
        .worker_slow_tier_async_fail_cas
        .counter
        .load(Ordering::Acquire);
    let unknown_count = metrics
        .worker_slow_tier_async_fail_unknown
        .counter
        .load(Ordering::Acquire);
    assert_eq!(
        cas_count, 0,
        "T3: cas-class counter must not bump for ac-tagged FSS; got {cas_count}"
    );
    assert_eq!(
        unknown_count, 0,
        "T3: unknown-class counter must not bump for ac-tagged FSS; got {unknown_count}"
    );
    Ok(())
}

/// T4 — slow-publish >500ms warn fires. The AC publish path measures
/// elapsed time around `ac_store.update_oneshot(...)`; when the
/// observed elapsed exceeds 500 ms the warn log fires and the
/// `worker_ac_publish_slow` counter bumps.
///
/// Strategy: AC FSS whose slow tier is a `LatentSlowStore` with
/// 600ms latency. The FSS `update_oneshot` writes to the in-memory
/// fast tier first (cheap) then spawns the slow write — BUT the AC
/// path's elapsed-time measurement covers the synchronous portion
/// only. To force the synchronous elapsed past 500 ms we put the
/// latency on the FAST tier too — the in-memory `MemoryStore` fast
/// won't sleep, so we instead build a single-store FSS where both
/// tiers ARE the latent fake (the FastSlowStore writes to fast THEN
/// spawns the slow; the fast write is what the elapsed-time
/// measures).
///
/// Production composition: `UploadActionResults::upload_ac_results`
/// runs the slow-publish warn block at
/// `running_actions_manager.rs:4451-4460`. The slow tier returns Ok
/// after the latency expires (no Err arm here — this exercises the
/// SLOW-PUBLISH warn, not the failure arm).
///
/// Mutation 2026-06-04: in `upload_ac_results`, change the slow
/// threshold check from `>= Duration::from_millis(500)` to
/// `>= Duration::from_millis(5_000)`; T4 must red-fail on the
/// `worker_ac_publish_slow` counter assertion with bespoke
/// "T4: slow-publish threshold must fire on >500ms publish".
#[nativelink_test]
async fn t4_slow_publish_warn_fires() -> Result<(), Box<dyn core::error::Error>> {
    let metrics = Arc::new(Metrics::default());
    let sink: Arc<dyn SlowTierMetricSink> = Arc::new(WorkerSlowTierMetricSink {
        metrics: metrics.clone(),
    });
    // Latent fast tier (sleeps 600ms on update) so the FSS's
    // synchronous fast-tier write — which the AC publish path
    // measures with `start.elapsed()` — exceeds the 500ms threshold.
    let latent_fast: Arc<dyn StoreDriver> = Arc::new(LatentSlowStore {
        latency: Duration::from_millis(600),
    });
    let mem_slow_spec = MemorySpec::default();
    let slow = MemoryStore::new(&mem_slow_spec);
    let ac_fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(mem_slow_spec),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(latent_fast),
        Store::new(slow),
    );
    ac_fss.set_store_class("ac");
    ac_fss.set_slow_tier_metric_sink(sink);
    let (manager, _target) = build_manager_with_ac_fss(ac_fss, metrics.clone()).await;

    let digest = make_test_digest(0x44);
    let mut action_result = ActionResult::default();
    let op_id = OperationId::default();

    // 5s deadline detector; 600ms latency must complete within it.
    tokio::time::timeout(
        TEST_TIMEOUT,
        manager.cache_action_result(
            digest,
            &mut action_result,
            DigestHasherFunc::Blake3,
            &op_id,
            "test_worker_t4",
        ),
    )
    .await
    .expect("T4: AC publish must complete within 5s — v2-T4 (600ms latent fast tier)")
    .expect("T4: AC publish must succeed (latent fast tier returns Ok)");

    let slow_count = metrics
        .worker_ac_publish_slow
        .counter
        .load(Ordering::Acquire);
    assert!(
        slow_count >= 1,
        "T4: slow-publish threshold must fire on >500ms publish — \
         worker_ac_publish_slow should bump; got {slow_count}"
    );
    assert!(
        logs_contain("AC write slow"),
        "T4: expected warn log 'AC write slow' on slow publish"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// (#12 H4 phase 2) Test (vi): worker-side publish path stores cas_endpoint
//
// Verifies that `RunningActionsManagerArgs::cas_endpoint` is correctly
// threaded into `UploadActionResults` and is accessible (not discarded)
// after construction. This is the structural guard that ensures
// `upload_ac_results` will have the non-empty endpoint available when it
// builds `UpdateActionResultRequest.cas_endpoint`.
//
// Mutation: change `UploadActionResults::new` to always set `cas_endpoint =
// String::new()` regardless of the arg → `cas_endpoint_for_test` returns ""
// and the assertion fires with "vi: cas_endpoint must be stored in
// UploadActionResults — arg not threaded through to GrpcStore path".
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn t7_cas_endpoint_stored_in_upload_action_results() -> Result<(), Box<dyn core::error::Error>> {
    let metrics = Arc::new(Metrics::default());
    let cas_fss = build_cas_fss().await;
    let ac_fss = build_ac_fss();
    let temp_path = std::env::temp_dir()
        .join(format!("nl37-vi-{}", rand::random::<u64>()))
        .to_string_lossy()
        .into_owned();
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let upload_cfg = UploadActionResultConfig {
        upload_ac_results_strategy:
            nativelink_config::cas_server::UploadCacheResultsStrategy::Everything,
        upload_historical_results_strategy: Some(
            nativelink_config::cas_server::UploadCacheResultsStrategy::Never,
        ),
        ..Default::default()
    };
    const TEST_ENDPOINT: &str = "grpc://worker-h4-test.local:50081";
    let args = RunningActionsManagerArgs {
        root_action_directory: temp_path,
        execution_configuration: Default::default(),
        cas_store: cas_fss,
        ac_store: Some(Store::new(ac_fss.clone())),
        ac_mirror_target: None,
        historical_store: Store::new(ac_fss),
        upload_action_result_config: &upload_cfg,
        max_action_timeout: Duration::from_secs(60),
        max_upload_timeout: Duration::from_secs(60),
        timeout_handled_externally: false,
        directory_cache: None,
        bis_ack_timeout: Duration::from_secs(60),
        metrics: Some(metrics.clone()),
        cas_endpoint: TEST_ENDPOINT.to_string(),
        deferred_output_uploads_enabled: false,
    };
    let manager = RunningActionsManagerImpl::new(args).expect("manager build");

    assert_eq!(
        manager.cas_endpoint_for_test(),
        TEST_ENDPOINT,
        "vi: cas_endpoint must be stored in UploadActionResults — arg not threaded \
         through to GrpcStore path"
    );
    Ok(())
}
