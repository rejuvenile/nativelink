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

//! Tests for the BlobsAvailable -> mark_stable architecture (task #140 /
//! audit Path 2 — replaces the deleted `register_action_result_digests`
//! mechanism).
//!
//! New contract: the worker pins every digest it produces or receives.
//! On every BlobsAvailable tick the worker reports the digests it holds.
//! The server's BlobsAvailable handler verifies which of those digests
//! it has stably, and for the present subset calls
//! `cas_store.mark_stable(...)` so the BIS broadcast loop wakes up and
//! tells the worker it is safe to unpin.
//!
//! Why this site (not `register_action_result_digests`):
//!   - The old site fired ONLY for ExecuteResponse outputs — missed
//!     deduplicated/already-cached uploads, tree-children pinned via
//!     pin_digest, and mirror blobs.
//!   - The old site raced `evicted_digests` on the same `mpsc::channel(1)`
//!     and could permanently stale the locality map (audit Path 2).
//!   - BlobsAvailable is the AUTHORITATIVE channel for "worker holds these
//!     digests right now" — every pin path eventually reports through it.
//!
//! Two production-composition tests:
//!   * Positive: BlobsAvailable carries a digest the server has → mark_stable
//!     fires within `BIS_TIMEOUT`.
//!   * Negative: BlobsAvailable carries a digest the server does NOT have
//!     → mark_stable must NOT fire (would unpin the worker's only durable
//!     copy of a mirror_blob).

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_config::stores::{
    ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec, VerifySpec,
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
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::store_trait::{Store, StoreLike};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
/// Bounded deadline for "BIS broadcast loop should pick this up". The
/// queue is push-on-mark_stable, drain-on-poll; the assertion polls every
/// 50 ms via `Notify` until the deadline. A regression that omits the
/// new mark_stable call site fails with the `panic!` message below
/// (acting as the deadlock detector — see CLAUDE.md "Test in production
/// composition, not in isolation").
const BIS_TIMEOUT: Duration = Duration::from_secs(5);

#[expect(
    clippy::unnecessary_wraps,
    reason = "WorkerApiServer expects a fallible time fn"
)]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

// ----- MockWorkerStateManager -----
//
// The BlobsAvailable handler does NOT call into the WorkerStateManager
// for the new mark_stable path; the mock here exists only to satisfy
// the `ApiWorkerScheduler::new` signature. We never receive any calls
// on it during these tests. The `_unused` field is required because
// `MetricsComponent` is not derivable for unit structs.
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
        unreachable!(
            "BlobsAvailable handling does not invoke update_operation; \
             mock should never be called from this test"
        )
    }
}

// ----- Production CAS composition -----
//
// `ExistenceCacheStore -> VerifyStore -> FastSlowStore { fast: Memory, slow: Memory }`.
// This is the wrapper sequence used in production (per
// `~/fl/bld/infra/nativelink/prod-server.json5:94-153` and MEMORY.md
// `Server CAS Store Architecture`). The point of the production
// composition is to verify `mark_stable` propagates through every
// wrapper to the `FastSlowStore` that owns `stable_digests`.
fn make_production_cas_store() -> Store {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_slow = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        fast,
        slow,
    ));
    let verify = Store::new(VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: false,
            verify_hash: false,
        },
        fast_slow,
    ));
    Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
        },
        verify,
    ))
}

// ----- Test context + setup -----

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
}

async fn setup_context(cas_endpoint: &str) -> Result<TestContext, Error> {
    const SCHEDULER_NAME: &str = "MARK_STABLE_BIS_TEST_SCHEDULER";
    const UUID_SIZE: usize = 36;

    let cas_store = make_production_cas_store();

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
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map),
        Some(cas_store.clone()),
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
    assert!(
        maybe_first_message.is_some(),
        "Expected first message from stream"
    );
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field to be populated")?;
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
    })
}

/// Send a single BlobsAvailable carrying `digests` and nothing else.
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
        }))
        .await
        .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "send: {e}"))
}

/// Drain the cas_store's `stable_digests` until `target` appears, polling
/// via `stable_notify` (the production wake-up signal) bounded by
/// `BIS_TIMEOUT`. Panic with a specific contract message on timeout —
/// this is the deadlock detector + the regression message a future
/// developer will see if mark_stable is omitted (CLAUDE.md "Test in
/// production composition, not in isolation").
async fn await_stable_drain_contains(cas_store: &Store, target: DigestInfo) {
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
                "BIS must fire for digests the server has when worker reports them \
                 in BlobsAvailable. Within {BIS_TIMEOUT:?} the cas_store's \
                 drain_stable_digests never returned the target {target:?}. \
                 Drained so far: {accumulated:?}. \
                 The BlobsAvailable handler must call cas.has_with_results to find \
                 the present subset and cas.mark_stable(&present) for it; without \
                 that, the worker's pin (durable under pin v2) NEVER receives the \
                 unpin signal."
            );
        }
        let _ = tokio::time::timeout(
            remaining.min(Duration::from_millis(50)),
            notify.notified(),
        )
        .await;
    }
}

// ----- Tests -----

/// Positive case: a digest D pre-populated in the production-composition
/// cas_store. Worker reports D in BlobsAvailable. Server must
/// `mark_stable(&[D])` so D ends up in `drain_stable_digests` within
/// `BIS_TIMEOUT`.
///
/// The test pre-drains the queue after pre-population so the assertion
/// is unambiguously caused by the new BlobsAvailable handling path
/// (not by the existing slow-write success arm).
#[nativelink_test]
async fn blobs_available_marks_stable_for_present_digest_test()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.55.7:50081";

    let test_context = setup_context(CAS_ENDPOINT).await?;

    // Pre-populate D. update_oneshot also pushes D into stable_digests
    // via the existing slow-write success arm — drain that so the
    // post-BlobsAvailable assertion is unambiguous.
    let data = Bytes::from_static(b"already cached output bytes");
    let target = DigestInfo::new([42u8; 32], data.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target, data.clone())
        .await
        .err_tip(|| "Failed to pre-populate cas_store")?;
    // Wait for the pre-populated digest to show up in stable_digests
    // (proves the slow-write happened), then drain.
    await_stable_drain_contains(&test_context.cas_store, target).await;
    drop(test_context.cas_store.drain_stable_digests());

    // Send BlobsAvailable carrying D. The server's handle_blobs_available
    // MUST: (a) register D in locality_map (existing behavior), (b) check
    // cas.has(D) (new behavior), (c) mark_stable(&[D]) since the server
    // has D (new behavior).
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target]).await?;

    // The contract: D appears in drain_stable_digests within BIS_TIMEOUT.
    // Failure mode = missing mark_stable call site.
    await_stable_drain_contains(&test_context.cas_store, target).await;

    Ok(())
}

/// Negative case: a digest D' that is NOT in the cas_store. Worker
/// reports D' in BlobsAvailable (e.g. a pinned mirror_blob the server
/// has not yet stably received). Server must NOT call
/// `mark_stable(&[D'])` — doing so would tell the worker to unpin a
/// digest whose only durable copy is the worker's `mirror_blobs`,
/// causing data loss on next worker eviction.
#[nativelink_test]
async fn blobs_available_does_not_mark_stable_for_missing_digest_test()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.55.8:50081";

    let test_context = setup_context(CAS_ENDPOINT).await?;

    // Drain any residual stable_digests from setup.
    drop(test_context.cas_store.drain_stable_digests());

    // A digest that is NOT pre-populated in cas_store.
    let missing_digest = DigestInfo::new([99u8; 32], 4);

    send_blobs_available(
        &test_context.worker_stream,
        CAS_ENDPOINT,
        vec![missing_digest],
    )
    .await?;

    // Wait long enough that any mark_stable wiring would have fired,
    // then confirm the missing digest is NOT in stable_digests. We poll
    // so a regression that wrongly marks stable surfaces on the first
    // iteration.
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while std::time::Instant::now() < deadline {
        let drained = test_context.cas_store.drain_stable_digests();
        assert!(
            !drained.contains(&missing_digest),
            "BIS must NOT fire for digests the server does NOT have. \
             handle_blobs_available must verify presence with has_with_results \
             before calling mark_stable, otherwise the worker would unpin a \
             digest whose only durable copy is the worker's mirror_blobs. \
             Drained: {drained:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}
