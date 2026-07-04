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

//! Durability-ack v3 Stage 1 — KEYSTONE test.
//!
//! Contract: the worker API server's BlobsAvailable handler may only
//! `mark_stable` a digest (→ BIS broadcast → worker UNPIN) once the
//! server holds that digest **durably** — i.e. in the FastSlowStore's
//! SLOW tier — NOT merely in the volatile fast tier (RAM). The unpin oath
//! tells the worker "the server has a durable copy, you may drop yours";
//! firing it on a RAM-only presence would tell the worker to drop the only
//! durable copy of a mirror blob, losing data on the next server restart.
//!
//! The gate is `cas_store.has_durably(&present)` (the durable subset),
//! distinct from `has_with_results` (which the FSS satisfies from the fast
//! tier + in-flight maps + mirror — none of which are durable).
//!
//! ## Production composition (NOT MemoryStore-for-both-tiers)
//!
//! The keystone wraps the production CAS chain
//! `Verify(Ref(ExistenceCache(SizePartitioning(Ref(SMALL), Ref(FSS)))))`,
//! where the upper FSS's SLOW tier is a HOLDABLE store whose `update`
//! blocks until the test releases it. This is the load-bearing distinction
//! the v3 spec demands: with Memory-for-both-tiers the slow write would
//! complete instantly and the RAM-only window the gate protects could not
//! be observed.
//!
//! ## Mutation (TDD step 5)
//!
//! Revert the durable gate in `request_missing_blob_uploads` from
//! `has_durably` back to `has_with_results` — the RAM-only blob then looks
//! present, `mark_stable` fires while the slow write is still held, and
//! `mark_stable_does_not_fire_on_ram_only_presence_test` red-fails with
//! "BIS fired on RAM-only presence (durability oath violated)".

use core::pin::Pin;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_config::stores::{
    ExistenceCacheSpec, FastSlowSpec, MemorySpec, RefSpec, SizePartitioningSpec, StoreDirection,
    StoreSpec, VerifySpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableNotification, ConnectWorkerRequest, UpdateForScheduler, update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::ref_store::RefStore;
use nativelink_store::size_partitioning_store::SizePartitioningStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::store_trait::{
    DurableDelegation, ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
    Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
/// Bounded deadline for "BIS broadcast loop should pick this up". Doubles as
/// a deadlock detector (CLAUDE.md "test in production composition").
const BIS_TIMEOUT: Duration = Duration::from_secs(5);

#[expect(
    clippy::unnecessary_wraps,
    reason = "WorkerApiServer expects a fallible time fn"
)]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

// ----- HoldableSlowStore -----
//
// A slow tier whose `update` (and `update_oneshot`) BLOCK on a `Notify`
// until `release()` is called. Reads/has delegate to the inner MemoryStore.
// This lets the test drive the FSS into the "acked to fast tier, durable
// write NOT yet landed" window the durable gate protects.
#[derive(Debug, MetricsComponent)]
struct HoldableSlowStore {
    inner: Store,
    #[metric(help = "unused")]
    _unused: u64,
    release: Arc<Notify>,
    hold: std::sync::atomic::AtomicBool,
}

impl HoldableSlowStore {
    fn new(inner: Store) -> Arc<Self> {
        Arc::new(Self {
            inner,
            _unused: 0,
            release: Arc::new(Notify::new()),
            hold: std::sync::atomic::AtomicBool::new(true),
        })
    }
    /// Stop holding writes and wake any blocked write.
    fn release(&self) {
        self.hold.store(false, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_waiters();
    }
    async fn wait_if_held(&self) {
        while self.hold.load(std::sync::atomic::Ordering::SeqCst) {
            self.release.notified().await;
        }
    }
}

default_health_status_indicator!(HoldableSlowStore);

#[async_trait]
impl StoreDriver for HoldableSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(digests, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.wait_if_held().await;
        self.inner.update(key, reader, upload_size).await
    }

    async fn update_oneshot(self: Pin<&Self>, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        self.wait_if_held().await;
        self.inner.update_oneshot(key, data).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
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
    fn register_item_callback(self: Arc<Self>, _cb: Arc<dyn ItemCallback>) -> Result<(), Error> {
        Ok(())
    }
    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Inner(self.inner.as_store_driver())
    }
    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Inner(self.inner.as_store_driver())
    }
    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Inner(self.inner.as_store_driver())
    }
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Inner(self.inner.as_store_driver())
    }
}

// ----- MockWorkerStateManager (unused; satisfies the scheduler ctor) -----
#[derive(MetricsComponent)]
struct MockWorkerStateManager {
    #[metric(help = "unused")]
    _unused: u64,
}

#[async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        unreachable!("BlobsAvailable handling does not invoke update_operation")
    }
}

// ----- Production CAS composition with a HOLDABLE upper slow tier -----
//
// Returns `(cas_STORE, store_manager, hold_handle)`.
fn make_cas_store_with_holdable_slow() -> (Store, Arc<StoreManager>, Arc<HoldableSlowStore>) {
    let store_manager = Arc::new(StoreManager::new());

    // cas_FAST_SLOW_STORE: MemoryStore fast tier, HOLDABLE slow tier.
    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_slow_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let holdable = HoldableSlowStore::new(upper_slow_inner);
    let upper_slow = Store::new(holdable.clone());
    let upper_fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        upper_fast,
        upper_slow,
    ));
    store_manager.add_store("cas_FAST_SLOW_STORE", upper_fast_slow.clone());

    // SMALL_CAS_CACHED: Memory→Memory FastSlow (lower arm, not under test).
    let lower_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let lower_fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        lower_fast,
        lower_slow,
    ));
    store_manager.add_store("SMALL_CAS_CACHED", lower_fast_slow);

    let upper_ref = Store::new(RefStore::new(
        &RefSpec {
            name: "cas_FAST_SLOW_STORE".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    let lower_ref = Store::new(RefStore::new(
        &RefSpec {
            name: "SMALL_CAS_CACHED".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));

    let size_part = Store::new(SizePartitioningStore::new(
        &SizePartitioningSpec {
            size: 16384,
            lower_store: StoreSpec::RefStore(RefSpec {
                name: "SMALL_CAS_CACHED".to_string(),
            }),
            upper_store: StoreSpec::RefStore(RefSpec {
                name: "cas_FAST_SLOW_STORE".to_string(),
            }),
        },
        lower_ref,
        upper_ref,
    ));

    let inner = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        size_part,
    ));
    store_manager.add_store("cas_INNER", inner);

    let inner_ref = Store::new(RefStore::new(
        &RefSpec {
            name: "cas_INNER".to_string(),
        },
        Arc::downgrade(&store_manager),
    ));
    let cas_store = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::RefStore(RefSpec {
                name: "cas_INNER".to_string(),
            }),
            verify_size: false,
            verify_hash: false,
        },
        inner_ref,
    ));

    (cas_store, store_manager, holdable)
}

struct TestContext {
    _worker_api_server: WorkerApiServer,
    _connection_worker_stream: Box<
        dyn futures::Stream<
                Item = Result<
                    nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker,
                    tonic::Status,
                >,
            > + Unpin
            + Send,
    >,
    worker_stream: mpsc::Sender<Update>,
    cas_store: Store,
    hold: Arc<HoldableSlowStore>,
    _store_manager: Arc<StoreManager>,
}

async fn setup_context(cas_endpoint: &str) -> Result<TestContext, Error> {
    const SCHEDULER_NAME: &str = "DURABLE_GATE_TEST_SCHEDULER";
    const UUID_SIZE: usize = 36;

    let (cas_store, store_manager, hold) = make_cas_store_with_holdable_slow();

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager { _unused: 0 });
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager,
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler);
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map),
        Some(cas_store.clone()),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
        .await
        .unwrap();
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            let update = Ok(UpdateForScheduler {
                update: Some(update),
            });
            (update, rx)
        })
    }));
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    let maybe_first_message = connection_worker_stream.next().await;
    assert!(maybe_first_message.is_some(), "Expected first message");
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {other:?}"),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(TestContext {
        _worker_api_server: worker_api_server,
        _connection_worker_stream: Box::new(connection_worker_stream),
        worker_stream: tx,
        cas_store,
        hold,
        _store_manager: store_manager,
    })
}

async fn send_blobs_available(
    worker_stream: &mpsc::Sender<Update>,
    cas_endpoint: &str,
    digests: Vec<DigestInfo>,
) -> Result<(), Error> {
    worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: cas_endpoint.to_string(),
            digests: digests.into_iter().map(Into::into).collect(),
            is_full_snapshot: false,
            evicted_digests: vec![],
            evicted_blob_infos: Vec::new(),
            digest_infos: vec![],
            cpu_load_pct: 0,
            cached_directory_digests: vec![],
            added_subtree_digests: vec![],
            removed_subtree_digests: vec![],
            is_full_subtree_snapshot: false,
            p_core_load_pct: 0,
            e_core_load_pct: 0,
            pinned_mirror_digests: vec![],
            mirror_used_bytes: 0,
            mirror_max_bytes: 0,
            pinned_mirror_entries: vec![],
            pinned_ac_mirror_entries: Vec::new(),
            indefinite_pin_saturated: false,
            swap_used_bytes: 0,
            memory_pressure_level: 0,
            memory_pressured: false,
            available_disk_bytes: 0,
            disk_pressured: false,
        }))
        .await
        .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "send: {e}"))
}

/// Drain `stable_digests` until `target` appears, bounded by `BIS_TIMEOUT`.
async fn await_stable_drain_contains(cas_store: &Store, target: DigestInfo, ctx: &str) {
    let notify = cas_store.stable_notify();
    let deadline = std::time::Instant::now() + BIS_TIMEOUT;
    let mut accumulated: Vec<DigestInfo> = Vec::new();
    loop {
        let mut drained = cas_store.drain_stable_digests();
        accumulated.append(&mut drained);
        if accumulated.contains(&target) {
            return;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "{ctx}: within {BIS_TIMEOUT:?} the cas_store's drain_stable_digests never \
                 returned {target:?}. Drained so far: {accumulated:?}"
            );
        }
        let _ =
            tokio::time::timeout(remaining.min(Duration::from_millis(50)), notify.notified()).await;
    }
}

// ----- Tests -----

/// KEYSTONE (under-action of the unpin oath): a blob written through the
/// production CAS chain into a FastSlowStore whose SLOW write is HELD lands
/// in the fast tier (RAM) immediately but is NOT durable. When the worker
/// reports it in BlobsAvailable the server must NOT `mark_stable` it (the
/// unpin oath requires a durable copy). Once the slow write is RELEASED and
/// completes, the next BlobsAvailable MUST mark it stable.
#[nativelink_test]
async fn mark_stable_does_not_fire_on_ram_only_presence_test()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.66.7:50081";

    let test_context = setup_context(CAS_ENDPOINT).await?;

    // Write a >16 KiB blob so it routes to the upper (holdable) FSS arm.
    // The FSS legacy `update` path lands it in the fast tier and SPAWNS the
    // background slow write — which blocks inside
    // HoldableSlowStore::update_oneshot.
    let data = Bytes::from(vec![0x5Au8; 32 * 1024]);
    let target = DigestInfo::new([7u8; 32], data.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target, data.clone())
        .await
        .err_tip(|| "Failed to write blob through CAS chain")?;

    // Sanity: the blob is present (fast tier / in-flight) per the non-durable
    // has() path, but NOT durable per has_durably (slow write is held).
    let present = test_context.cas_store.has(target).await?;
    assert!(
        present.is_some(),
        "setup: blob should be present in the fast tier / in-flight map"
    );
    let mut durable = [None];
    test_context
        .cas_store
        .has_durably(&[target.into()], &mut durable)
        .await?;
    assert!(
        durable[0].is_none(),
        "setup precondition: blob must NOT be durable while the slow write is held; \
         has_durably returned {durable:?}"
    );

    // Drain any residual.
    drop(test_context.cas_store.drain_stable_digests());

    // Worker reports the RAM-only blob. The server MUST NOT mark it stable.
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target]).await?;

    // Poll for a window that a wrong (has_with_results-gated) mark_stable
    // would fire in. The blob must NOT appear.
    let deadline = std::time::Instant::now() + Duration::from_millis(750);
    while std::time::Instant::now() < deadline {
        let drained = test_context.cas_store.drain_stable_digests();
        assert!(
            !drained.contains(&target),
            "BIS fired on RAM-only presence (durability oath violated): the worker \
             API server marked digest {target:?} stable while the server held it \
             ONLY in the fast tier (RAM) — the durable slow write was still blocked. \
             request_missing_blob_uploads must gate mark_stable on has_durably, not \
             has_with_results. Drained: {drained:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now release the slow write; it completes and the blob becomes durable.
    test_context.hold.release();

    // The next BlobsAvailable must mark it stable (server now has a durable
    // copy). The async slow-write success arm ALSO pushes to stable_digests,
    // so either path satisfies the contract once the write lands.
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target]).await?;
    await_stable_drain_contains(
        &test_context.cas_store,
        target,
        "after the slow write is released, the durable blob must be mark_stable'd",
    )
    .await;

    Ok(())
}

/// Companion (over-action / positive control): a blob whose slow write has
/// ALREADY landed (durable from the start) must be mark_stable'd promptly on
/// BlobsAvailable. Proves the gate is not over-tight (does not refuse a
/// genuinely-durable blob).
#[nativelink_test]
async fn mark_stable_fires_for_durable_blob_test() -> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.66.8:50081";

    let test_context = setup_context(CAS_ENDPOINT).await?;
    // Release immediately so the slow write completes synchronously-ish.
    test_context.hold.release();

    let data = Bytes::from(vec![0x42u8; 32 * 1024]);
    let target = DigestInfo::new([8u8; 32], data.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target, data.clone())
        .await
        .err_tip(|| "Failed to write blob through CAS chain")?;

    // Wait until the blob is actually durable (slow write landed).
    let durable_deadline = std::time::Instant::now() + BIS_TIMEOUT;
    loop {
        let mut durable = [None];
        test_context
            .cas_store
            .has_durably(&[target.into()], &mut durable)
            .await?;
        if durable[0].is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < durable_deadline,
            "setup: released slow write never made the blob durable within {BIS_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    drop(test_context.cas_store.drain_stable_digests());

    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target]).await?;
    await_stable_drain_contains(
        &test_context.cas_store,
        target,
        "a genuinely-durable blob MUST be mark_stable'd on BlobsAvailable (gate not over-tight)",
    )
    .await;

    Ok(())
}
