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
// Mirrors the production cas_STORE chain from
// `~/fl/bld/infra/nativelink/prod-server.json5:94-153` and MEMORY.md
// `Server CAS Store Architecture`:
//
//     cas_STORE = Verify(Ref(cas_INNER))
//     cas_INNER = ExistenceCache(SizePartitioning(
//         lower = Ref(SMALL_CAS_CACHED),
//         upper = Ref(cas_FAST_SLOW_STORE)))
//
// Each leg's `FastSlowStore` is the terminal that owns `stable_digests`
// (and where the `mark_stable` push must land). The two `Ref` indirections
// in the chain are real: prior reviewer findings (#157 wrapper coverage,
// `aea1038e` C+D enum landing) hinge on the BIS pipeline composing
// correctly across every wrapper, including `Ref` and `SizePartitioning`.
//
// Earlier revisions of this test used a single direct `FastSlowStore`
// without `Ref` / `SizePartitioning` indirections, so a regression in
// either wrapper's `mark_stable` delegation would not have been
// detectable here — that is the bug class CLAUDE.md "Test in production
// composition, not in isolation" exists to catch.
//
// Returns `(cas_STORE, store_manager, [lower_fast_slow, upper_fast_slow])`
// — the `Ref` lookups need the manager to outlive the test, and the
// terminal `FastSlowStore` handles allow tests to assert on the actual
// `stable_digests` queue post-mark_stable propagation.
fn make_production_cas_store() -> (Store, Arc<StoreManager>, Store, Store) {
    let store_manager = Arc::new(StoreManager::new());

    // cas_FAST_SLOW_STORE: terminal big-blob store. We use Memory for both
    // tiers in the test (the real config uses Memory + Filesystem); the
    // BIS-pipeline behavior under test depends on `FastSlowStore::mark_stable`
    // — independent of whether the slow tier is Memory or Filesystem.
    let upper_fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let upper_slow = Store::new(MemoryStore::new(&MemorySpec::default()));
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

    // SMALL_CAS_CACHED: terminal small-blob store. Real config wraps a
    // Memory→Redis FastSlow; for the test, Memory→Memory is sufficient.
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
    store_manager.add_store("SMALL_CAS_CACHED", lower_fast_slow.clone());

    // RefStores wrapping each leaf — matches the production indirections.
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

    // SizePartitioningStore: 16KB partition matches prod-server.json5:136.
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

    // cas_INNER: ExistenceCache(SizePartitioning).
    let inner = Store::new(ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: None,
            log_not_found_at_info: false,
        },
        size_part,
    ));
    store_manager.add_store("cas_INNER", inner.clone());

    // Outer Ref(cas_INNER) wrapper inside Verify, matching the cas_STORE
    // declaration's `verify { backend: ref_store { name: cas_INNER } }`.
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

    (cas_store, store_manager, lower_fast_slow, upper_fast_slow)
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
    // Hold the StoreManager and the two terminal FastSlowStore handles
    // alive for the duration of the test. The Ref wrappers in
    // make_production_cas_store hold Weak references to the manager;
    // dropping the manager would break Ref resolution mid-test.
    _store_manager: Arc<StoreManager>,
    _lower_fast_slow: Store,
    _upper_fast_slow: Store,
}

async fn setup_context(cas_endpoint: &str) -> Result<TestContext, Error> {
    const SCHEDULER_NAME: &str = "MARK_STABLE_BIS_TEST_SCHEDULER";
    const UUID_SIZE: usize = 36;

    let (cas_store, store_manager, lower_fast_slow, upper_fast_slow) = make_production_cas_store();

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
        None, // no pending_output_locality_registry
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
        _store_manager: store_manager,
        _lower_fast_slow: lower_fast_slow,
        _upper_fast_slow: upper_fast_slow,
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

/// Multi-target sibling of [`await_stable_drain_contains`]. Drains the
/// cas_store's `stable_digests` until EVERY digest in `targets` has been
/// observed, accumulating across destructive drains. Necessary when more
/// than one target may flush in a single drain — calling
/// `await_stable_drain_contains` per target loses the other targets to
/// the per-call local accumulator.
///
/// Panics on timeout with a specific message naming the unsatisfied
/// target(s) — same deadlock-detector role as
/// [`await_stable_drain_contains`].
async fn await_stable_drain_contains_all(cas_store: &Store, targets: &[DigestInfo]) {
    let notify = cas_store.stable_notify();
    let deadline = std::time::Instant::now() + BIS_TIMEOUT;
    let mut accumulated: Vec<DigestInfo> = Vec::new();

    loop {
        let mut drained = cas_store.drain_stable_digests();
        accumulated.append(&mut drained);
        if targets.iter().all(|t| accumulated.contains(t)) {
            return;
        }

        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            let missing: Vec<_> = targets
                .iter()
                .filter(|t| !accumulated.contains(t))
                .collect();
            panic!(
                "BIS must fire for ALL digests the server has when worker reports \
                 them in a single BlobsAvailable. Within {BIS_TIMEOUT:?} the \
                 cas_store's drain_stable_digests never returned: {missing:?}. \
                 Drained so far: {accumulated:?}. The BlobsAvailable handler \
                 must call cas.has_with_results to find the present subset and \
                 cas.mark_stable(&present) for ALL of them; a regression that \
                 truncates the present subset (or gates mark_stable around the \
                 call site instead of per digest) would surface here."
            );
        }
        let _ =
            tokio::time::timeout(remaining.min(Duration::from_millis(50)), notify.notified()).await;
    }
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
        let _ =
            tokio::time::timeout(remaining.min(Duration::from_millis(50)), notify.notified()).await;
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

/// Regression for red-team F2: mark_stable MUST fire on every BlobsAvailable
/// arrival, not just the ticks that pass the per-worker
/// `BACKFILL_COOLDOWN_SECS` (5 s) gate. Before the decoupling fix the worker
/// would hold a v2 durable pin for up to 5 s after the server already had
/// the blob stably — which under high action throughput accumulates
/// thousands of pin-seconds of needless retention per worker.
///
/// Test sends TWO BlobsAvailable in rapid succession (well within the 5 s
/// cooldown). Both digests are pre-populated in cas_store. The contract:
/// BOTH digests must end up in `drain_stable_digests` within `BIS_TIMEOUT`
/// — not just the first one. A regression that gates mark_stable on the
/// cooldown only fires for the first BlobsAvailable; the second's digest
/// never appears in the stable queue (and the worker's pin leaks until the
/// next post-cooldown tick).
#[nativelink_test]
async fn mark_stable_fires_for_back_to_back_blobs_available_within_cooldown_test()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.55.7:50083";

    let test_context = setup_context(CAS_ENDPOINT).await?;

    // Pre-populate two distinct digests.
    let data_a = Bytes::from_static(b"first within-cooldown blob");
    let target_a = DigestInfo::new([13u8; 32], data_a.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target_a, data_a.clone())
        .await
        .err_tip(|| "Failed to pre-populate target_a")?;
    let data_b = Bytes::from_static(b"second within-cooldown blob");
    let target_b = DigestInfo::new([14u8; 32], data_b.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target_b, data_b.clone())
        .await
        .err_tip(|| "Failed to pre-populate target_b")?;
    // Drain the slow-write feed so the assertions below are unambiguous.
    await_stable_drain_contains(&test_context.cas_store, target_a).await;
    await_stable_drain_contains(&test_context.cas_store, target_b).await;
    drop(test_context.cas_store.drain_stable_digests());

    // First BlobsAvailable: target_a. This trips the cooldown gate.
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target_a]).await?;
    await_stable_drain_contains(&test_context.cas_store, target_a).await;
    drop(test_context.cas_store.drain_stable_digests());

    // Second BlobsAvailable: target_b. Sent IMMEDIATELY after the first —
    // well inside `BACKFILL_COOLDOWN_SECS=5`. The contract:
    // mark_stable must fire for target_b even though we are inside the
    // upload-backfill cooldown window.
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target_b]).await?;
    await_stable_drain_contains(&test_context.cas_store, target_b).await;

    Ok(())
}

/// Sub-call-granularity sibling for testing-czar MAJOR-2 (#140 follow-up):
/// the existing back-to-back test sends ONE digest per BlobsAvailable.
/// That payload shape only ever drives ONE `mark_stable` call per
/// BlobsAvailable, so a regression that gates `mark_stable` around the
/// CALL SITE (not the per-digest decision) — e.g. a regression that
/// re-engages the BACKFILL_COOLDOWN gate around the entire
/// `mark_stable` call BEFORE checking which digests are present —
/// would only suppress mark_stable on the second BlobsAvailable. With
/// single-digest payloads, you cannot distinguish "mark_stable was
/// truncated" from "mark_stable was suppressed" — only "mark_stable
/// did/didn't fire at all".
///
/// This sibling exercises the multi-digest-per-tick path:
///   1. First BlobsAvailable carries `[a, b]` together (one tick, two
///      present digests). Both must end up in `stable_digests`.
///   2. Second BlobsAvailable carries `[c]` immediately after (well
///      inside `BACKFILL_COOLDOWN_SECS=5`). `c` must end up in
///      `stable_digests` despite being inside the cooldown window.
///
/// A regression that wraps the `mark_stable` call in the cooldown gate
/// (rather than letting it run alongside the upload-protocol throttle)
/// fails specifically on `c`: the test panic message names `c` so the
/// regression's site is unambiguous (CLAUDE.md "Test in production
/// composition, not in isolation" — assertion message must be specific).
#[nativelink_test]
async fn mark_stable_fires_for_multi_digest_then_within_cooldown_test()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.55.7:50084";

    let test_context = setup_context(CAS_ENDPOINT).await?;

    // Pre-populate three distinct digests in cas_store. Use distinct hash
    // bytes so panics name the offender.
    let data_a = Bytes::from_static(b"multi-digest within-cooldown blob a");
    let target_a = DigestInfo::new([21u8; 32], data_a.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target_a, data_a.clone())
        .await
        .err_tip(|| "Failed to pre-populate target_a")?;
    let data_b = Bytes::from_static(b"multi-digest within-cooldown blob b");
    let target_b = DigestInfo::new([22u8; 32], data_b.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target_b, data_b.clone())
        .await
        .err_tip(|| "Failed to pre-populate target_b")?;
    let data_c = Bytes::from_static(b"multi-digest within-cooldown blob c");
    let target_c = DigestInfo::new([23u8; 32], data_c.len() as u64);
    test_context
        .cas_store
        .update_oneshot(target_c, data_c.clone())
        .await
        .err_tip(|| "Failed to pre-populate target_c")?;
    // Drain the slow-write feed so the assertions below are unambiguous.
    // `await_stable_drain_contains` is destructive across digests (a single
    // drain that returns multiple digests retains only the matched one in
    // the local accumulator), so wait for ALL three to flush via a single
    // multi-target loop and then drop.
    await_stable_drain_contains_all(&test_context.cas_store, &[target_a, target_b, target_c]).await;
    drop(test_context.cas_store.drain_stable_digests());

    // First BlobsAvailable carries TWO present digests in one tick.
    // The handler must mark BOTH stable in a single call. This trips the
    // cooldown gate (last_backfill_epoch_secs is set). Use the
    // multi-target waiter so a single drain returning [a, b] doesn't
    // lose `b` to the local accumulator.
    send_blobs_available(
        &test_context.worker_stream,
        CAS_ENDPOINT,
        vec![target_a, target_b],
    )
    .await?;
    await_stable_drain_contains_all(&test_context.cas_store, &[target_a, target_b]).await;
    drop(test_context.cas_store.drain_stable_digests());

    // Second BlobsAvailable carries `c` ALONE, sent IMMEDIATELY after
    // the first — well inside BACKFILL_COOLDOWN_SECS=5. mark_stable
    // must STILL fire for `c`. A regression that re-engages the
    // cooldown gate around the mark_stable call (not just around the
    // upload-protocol throttle) would suppress this and the test panics
    // with the specific deadlock-detector message naming target_c.
    send_blobs_available(&test_context.worker_stream, CAS_ENDPOINT, vec![target_c]).await?;
    await_stable_drain_contains(&test_context.cas_store, target_c).await;

    Ok(())
}

/// FL-688 v3 Stage C — BLOCK-1 ordering seam test.
///
/// Ordering contract: on the backfill path, `UploadMissingBlobs` MUST arrive
/// in the server→worker channel BEFORE `ReconcileComplete`. A regression at
/// `worker_api_server.rs:3284` that moves `send_reconcile_complete_static` to
/// BEFORE the `request_missing_blob_uploads` await (or to the synchronous
/// handler context) violates this ordering.
///
/// Setup: server CAS does NOT have the blob the worker reports → server enters
/// the backfill path (spawned task) → `UploadMissingBlobs` is sent inside the
/// task, then `ReconcileComplete` is sent as the LAST statement.
///
/// Mutation: move `Self::send_reconcile_complete_static(...)` to BEFORE
/// `Self::request_missing_blob_uploads(...)` in the `background_spawn!` task at
/// `worker_api_server.rs:3266-3289`. The test panics with:
/// "BLOCK-1 ordering regression: ReconcileComplete (tag 14) preceded
///  UploadMissingBlobs (tag 9) in the server→worker channel"
#[nativelink_test]
async fn v3c_block1_reconcile_complete_follows_upload_missing_blobs()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker;

    const CAS_ENDPOINT: &str = "grpc://192.168.55.8:50085";
    const UUID_SIZE: usize = 36;
    const ORDER_TIMEOUT: Duration = Duration::from_secs(5);

    // ----- Build the same production composition as setup_context -----
    let (cas_store, store_manager, lower_fast_slow, upper_fast_slow) =
        make_production_cas_store();
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
    schedulers.insert("BLOCK1_ORDER_SCHEDULER".to_string(), scheduler);
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: "BLOCK1_ORDER_SCHEDULER".to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [2u8; 6],
        Some(locality_map),
        Some(cas_store.clone()),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer for block-1 test")?;

    // ----- Connect a worker (consume ConnectionResult) -----
    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(ConnectWorkerRequest {
        cas_endpoint: CAS_ENDPOINT.to_string(),
        ..Default::default()
    }))
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
    let mut server_to_worker: Box<dyn futures::Stream<
        Item = Result<UpdateForWorker, tonic::Status>,
    > + Unpin + Send> = Box::new(
        worker_api_server
            .inner_connect_worker_for_testing(update_stream)
            .await?
            .into_inner(),
    );

    // Consume the ConnectionResult (first message).
    let first = tokio::time::timeout(ORDER_TIMEOUT, server_to_worker.next())
        .await
        .expect("timed out waiting for ConnectionResult")
        .expect("stream closed before ConnectionResult")
        .err_tip(|| "ConnectionResult was an error")?;
    match first.update.expect("ConnectionResult.update must be set") {
        update_for_worker::Update::ConnectionResult(cr) => {
            assert_eq!(cr.worker_id.len(), UUID_SIZE);
        }
        other => panic!("Expected ConnectionResult, got {other:?}"),
    }

    // ----- Send BlobsAvailable with a digest NOT in the server CAS -----
    // The server CAS is empty → this digest will be "missing" → backfill
    // path → UploadMissingBlobs is sent first, then ReconcileComplete.
    // is_full_snapshot = true to trigger the reconcile gate release.
    let missing_digest = DigestInfo::new([42u8; 32], 128);
    tx.send(Update::BlobsAvailable(BlobsAvailableNotification {
        worker_cas_endpoint: CAS_ENDPOINT.to_string(),
        digests: vec![missing_digest.into()],
        is_full_snapshot: true, // triggers reconcile gate path
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
    .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "send: {e}"))?;

    // ----- Collect the next two messages: must be [UploadMissingBlobs, ReconcileComplete] -----
    let first_msg = tokio::time::timeout(ORDER_TIMEOUT, server_to_worker.next())
        .await
        .expect(
            "BLOCK-1 ordering test: timed out waiting for first post-BlobsAvailable message \
             (expected UploadMissingBlobs); server did not write to worker channel within 5s",
        )
        .expect("stream closed before UploadMissingBlobs")
        .err_tip(|| "first post-BlobsAvailable message was an error")?;

    let second_msg = tokio::time::timeout(ORDER_TIMEOUT, server_to_worker.next())
        .await
        .expect(
            "BLOCK-1 ordering test: timed out waiting for second post-BlobsAvailable message \
             (expected ReconcileComplete); server did not write ReconcileComplete within 5s",
        )
        .expect("stream closed before ReconcileComplete")
        .err_tip(|| "second post-BlobsAvailable message was an error")?;

    // Assert UploadMissingBlobs came FIRST.
    match first_msg.update.expect("first message update must be set") {
        update_for_worker::Update::UploadMissingBlobs(_) => {} // correct
        update_for_worker::Update::ReconcileComplete(_) => {
            panic!(
                "BLOCK-1 ordering regression: ReconcileComplete (tag 14) preceded \
                 UploadMissingBlobs (tag 9) in the server→worker channel. \
                 Fix: `send_reconcile_complete_static` must be the LAST statement \
                 INSIDE the `background_spawn!` task at worker_api_server.rs:3284, \
                 AFTER `request_missing_blob_uploads` completes."
            );
        }
        other => {
            panic!(
                "BLOCK-1 ordering test: expected UploadMissingBlobs as first message, \
                 got {other:?}",
            );
        }
    }

    // Assert ReconcileComplete came SECOND.
    match second_msg.update.expect("second message update must be set") {
        update_for_worker::Update::ReconcileComplete(_) => {} // correct
        other => {
            panic!(
                "BLOCK-1 ordering test: expected ReconcileComplete as second message, \
                 got {other:?}",
            );
        }
    }

    // Suppress unused-variable lint on kept-alive handles.
    let _keep = (cas_store, store_manager, lower_fast_slow, upper_fast_slow);
    Ok(())
}

/// FL-688 v3 Stage C — exactly-once `ReconcileComplete` on fall-through path.
///
/// The fall-through path (no digests, or no cas_store endpoint) must send
/// `ReconcileComplete` exactly once when `is_full_snapshot: true`. The
/// `AtomicBool` compare_exchange in `send_reconcile_complete_static` guards
/// at-most-once globally; this test verifies at-least-once for the fall-through
/// path specifically.
///
/// Mutation: remove `self.try_send_reconcile_complete(is_full_snapshot)` at
/// `worker_api_server.rs:3320`. The test panics with:
/// "exactly-once fall-through: timed out waiting for ReconcileComplete;
///  the fall-through path must send ReconcileComplete on is_full_snapshot"
#[nativelink_test]
async fn v3c_reconcile_complete_exactly_once_fall_through_path()
-> Result<(), Box<dyn core::error::Error>> {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker;

    const CAS_ENDPOINT: &str = "grpc://192.168.55.9:50086";
    const UUID_SIZE: usize = 36;
    const RC_TIMEOUT: Duration = Duration::from_secs(5);

    // Build server with a CAS store that IS populated (so all blobs present →
    // no missing blobs → fall-through after mark_stable). Actually, we use an
    // EMPTY digest list in BlobsAvailable → direct fall-through (no backfill).
    let (cas_store, store_manager, lower_fast_slow, upper_fast_slow) =
        make_production_cas_store();
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
    schedulers.insert("FALLTHROUGH_SCHEDULER".to_string(), scheduler);
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: "FALLTHROUGH_SCHEDULER".to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [3u8; 6],
        Some(locality_map),
        Some(cas_store.clone()),
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer for fall-through test")?;

    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(ConnectWorkerRequest {
        cas_endpoint: CAS_ENDPOINT.to_string(),
        ..Default::default()
    }))
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
    let mut server_to_worker: Box<dyn futures::Stream<
        Item = Result<UpdateForWorker, tonic::Status>,
    > + Unpin + Send> = Box::new(
        worker_api_server
            .inner_connect_worker_for_testing(update_stream)
            .await?
            .into_inner(),
    );

    // Consume ConnectionResult.
    let first = tokio::time::timeout(RC_TIMEOUT, server_to_worker.next())
        .await
        .expect("timed out waiting for ConnectionResult in fall-through test")
        .expect("stream closed")
        .err_tip(|| "ConnectionResult was error")?;
    match first.update.expect("ConnectionResult.update must be set") {
        update_for_worker::Update::ConnectionResult(cr) => {
            assert_eq!(cr.worker_id.len(), UUID_SIZE);
        }
        other => panic!("Expected ConnectionResult, got {other:?}"),
    }

    // Send BlobsAvailable with EMPTY digest list → no backfill → fall-through.
    // is_full_snapshot = true to trigger reconcile gate.
    tx.send(Update::BlobsAvailable(BlobsAvailableNotification {
        worker_cas_endpoint: CAS_ENDPOINT.to_string(),
        digests: vec![], // empty → fall-through path
        is_full_snapshot: true,
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
    .map_err(|e| nativelink_error::make_err!(nativelink_error::Code::Internal, "send: {e}"))?;

    // The fall-through path sends ReconcileComplete synchronously before
    // returning from handle_blobs_available.
    let rc_msg = tokio::time::timeout(RC_TIMEOUT, server_to_worker.next())
        .await
        .expect(
            "exactly-once fall-through: timed out waiting for ReconcileComplete; \
             the fall-through path must send ReconcileComplete on is_full_snapshot=true. \
             MUTATION target: `self.try_send_reconcile_complete(is_full_snapshot)` at \
             worker_api_server.rs:3320 is the send site for the fall-through path.",
        )
        .expect("stream closed before ReconcileComplete")
        .err_tip(|| "fall-through ReconcileComplete message was an error")?;

    match rc_msg.update.expect("ReconcileComplete message update must be set") {
        update_for_worker::Update::ReconcileComplete(_) => {} // correct
        other => {
            panic!(
                "exactly-once fall-through: expected ReconcileComplete as first \
                 post-BlobsAvailable message (fall-through path with empty digests), \
                 got {other:?}. The fall-through path must call \
                 `try_send_reconcile_complete(is_full_snapshot)` before returning.",
            );
        }
    }

    let _keep = (cas_store, store_manager, lower_fast_slow, upper_fast_slow);
    Ok(())
}
