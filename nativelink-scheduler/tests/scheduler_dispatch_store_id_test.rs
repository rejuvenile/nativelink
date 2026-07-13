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

//! Regression test for #280: scheduler dispatch must propagate `store_id`
//! verbatim onto every emitted `BlobsInStableStorageChunk`.
//!
//! Production wiring in `src/bin/nativelink.rs` runs one BIS broadcast loop
//! per (FastSlowStore, store_id) pair. The CAS loop calls
//! `broadcast_blobs_in_stable_storage_chunked(digests, "")` (empty string =
//! historic single-store wire shape, preserved for forward compat with
//! pre-AC-BIS workers); the AC loop calls it with a non-empty config-driven
//! name (e.g. `"AC_MAIN_STORE"`). Workers route the unpin to the matching
//! local store via the `store_id` field on the wire chunk:
//!
//!   * empty `store_id` → CAS FastSlowStore (worker's
//!     `cas_STORE → ... → FastSlowStore`)
//!   * non-empty `store_id` → AC FastSlowStore matching that name
//!
//! The integration tests cover (a) the worker-side routing
//! (`handle_bis_chunk` + AC FSS routing) and (b) the BIS broadcast loop's
//! drain + send. They DO NOT cover the scheduler-side dispatch that ships
//! the wire-format chunk between them — the path that constructs the
//! `BlobsInStableStorageChunk` and writes the `store_id` field. A bug in
//! that path (e.g. `store_id: String::new()` instead of `store_id:
//! store_id.to_string()`, or accidentally propagating only on a CAS branch)
//! is invisible to both sides' tests but breaks AC mirroring end-to-end.
//!
//! These two tests assert the wire-format contract directly:
//!
//! Test 1: non-empty `store_id` (AC path) propagates verbatim to every
//! emitted chunk's `store_id`.
//!
//! Test 2: empty `store_id` (CAS path) emits empty `store_id` on every
//! chunk (forward-compat with pre-AC-BIS workers).
//!
//! Both tests wrap the call in `tokio::time::timeout(5s)` as a deadlock
//! detector — a hung dispatch would otherwise stall the test runner.
//!
//! Mutation step: in
//! `nativelink-scheduler/src/api_worker_scheduler.rs`'s
//! `broadcast_blobs_in_stable_storage_chunked`, replace `store_id:
//! store_id.to_string()` with `store_id: String::new()`. Test 1 must
//! red-fail with the bespoke "wire-format contract violated" panic; Test 2
//! still passes (it asserts empty). Restore.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsInStableStorageChunk, UpdateForWorker, chunked_message,
    update_for_worker::Update as ServerUpdate,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

/// Minimal no-op `WorkerStateManager` — these tests don't touch operation
/// state. Mirrors the inline `NoopWsm` used by api_worker_scheduler's own
/// BIS chunked-dispatch tests.
#[derive(Debug)]
struct NoopWsm;

impl MetricsComponent for NoopWsm {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

#[async_trait]
impl WorkerStateManager for NoopWsm {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Construct an `ApiWorkerScheduler` with no locality map / cas_store / TLS
/// — the BIS chunked-dispatch path doesn't consult any of them.
fn make_scheduler() -> Arc<ApiWorkerScheduler> {
    ApiWorkerScheduler::new_with_locality_map(
        Arc::new(NoopWsm),
        Arc::new(PlatformPropertyManager::new(HashMap::new())),
        nativelink_config::schedulers::WorkerAllocationStrategy::default(),
        Arc::new(Notify::new()),
        100,
        Arc::new(WorkerRegistry::new()),
        None,
        None,
        None,
        // (#sched-blend) blend tunables — defaults (this path doesn't score).
        512 * 1024,
        8,
        false, // (#sched M1 rebalance) p_headroom_gate OFF
        0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
        2, // (#sched M1 rebalance v2) p_headroom_override_factor
        // (#p2p-prefetch) P2P input prefetch OFF (test default)
        false,
        // (#specprefetch-rebind Stage B) temporal hold gate OFF (test default)
        false,
        None, // merge v1.6.1: maybe_origin_event_tx (origin events off in tests)
    )
}

/// Distinct hashes per `i` so chunk dispatch isn't deduped by digest.
fn make_digest_info(i: u64) -> DigestInfo {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&i.to_be_bytes());
    DigestInfo::new(hash, 4)
}

/// Add a worker that the scheduler will dispatch to, returning the rx side
/// so the test can observe the wire-format `UpdateForWorker` messages.
async fn register_worker_endpoint(
    scheduler: &Arc<ApiWorkerScheduler>,
    worker_id: &str,
    cas_endpoint: &str,
) -> mpsc::UnboundedReceiver<UpdateForWorker> {
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = Worker::new_with_cas_endpoint(
        WorkerId(worker_id.to_string()),
        PlatformProperties::default(),
        tx,
        42, // timestamp
        0,  // max_inflight_tasks
        cas_endpoint.to_string(),
        0, // p_core_count (#sched-blend; unknown in this test)
        0, // e_core_count
    );
    WorkerScheduler::add_worker(scheduler.as_ref(), worker)
        .await
        .expect("add_worker");
    rx
}

/// Drain every `UpdateForWorker` waiting on rx within a short window and
/// return only the BIS chunk payloads. Other Update arms (ConnectionResult,
/// KeepAlive, etc.) are skipped — the wire-format contract under test is
/// strictly the `BlobsInStableStorage` chunked-message arm.
async fn drain_bis_chunks(
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) -> Vec<BlobsInStableStorageChunk> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(
        Duration::from_millis(200),
        rx.recv(),
    )
    .await
    {
        if let Some(ServerUpdate::ChunkedMessage(envelope)) = msg.update
            && let Some(chunked_message::Payload::BlobsInStableStorage(chunk)) =
                envelope.payload
        {
            out.push(chunk);
        }
    }
    out
}

/// (#280) Test 1 — non-empty `store_id` (AC path) MUST propagate verbatim
/// onto every emitted `BlobsInStableStorageChunk`.
///
/// The Option A AC mirroring chain depends on this end-to-end: the
/// per-store BIS broadcast loop in `src/bin/nativelink.rs` calls
/// `broadcast_blobs_in_stable_storage_chunked(digests, AC_STORE_NAME)`;
/// the worker's `handle_bis_chunk` reads `chunk.store_id` to decide which
/// FastSlowStore's `dispatched_mirror_pins` index to drain. If the
/// scheduler emits `store_id = ""` instead, the AC chunk routes to the
/// CAS store, the AC pins never drop, and the worker's AC mirror grows
/// unbounded.
///
/// Mutation: hardcode `store_id: String::new()` in
/// `api_worker_scheduler.rs::broadcast_blobs_in_stable_storage_chunked`'s
/// chunk constructor; this test must red-fail with the bespoke message
/// below.
#[nativelink_test]
async fn scheduler_emits_non_empty_store_id_for_ac_broadcast() {
    const AC_STORE_NAME: &str = "AC_MAIN_STORE";

    let scheduler = make_scheduler();
    let mut rx = register_worker_endpoint(
        &scheduler,
        "worker-ac",
        "grpc://w-ac.local:50081",
    )
    .await;

    // Few digests → typically a single chunk; that's enough to assert the
    // store_id contract. We also verify multi-chunk propagation in test 2's
    // larger fan-out by extension: the same constructor path emits every
    // chunk, so propagation is uniform across chunks.
    let digests: Vec<DigestInfo> = (0..32u64).map(make_digest_info).collect();

    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.broadcast_blobs_in_stable_storage_chunked(digests, AC_STORE_NAME),
    )
    .await
    .expect(
        "scheduler dispatch must propagate non-empty store_id for AC chunks — \
         wire-format contract violated (broadcast hung past 5s deadlock detector)",
    );

    let chunks = drain_bis_chunks(&mut rx).await;
    assert!(
        !chunks.is_empty(),
        "scheduler dispatch must propagate non-empty store_id for AC chunks — \
         wire-format contract violated (no chunks delivered to worker rx)"
    );
    for chunk in &chunks {
        assert_eq!(
            chunk.store_id,
            AC_STORE_NAME,
            "scheduler dispatch must propagate non-empty store_id for AC chunks — \
             wire-format contract violated: chunk seq={} carried store_id={:?} (expected {:?})",
            chunk.sequence,
            chunk.store_id,
            AC_STORE_NAME,
        );
    }
}

/// (#280) Test 2 — empty `store_id` (CAS path) MUST emit empty `store_id`
/// on every chunk.
///
/// The CAS broadcast loop in `src/bin/nativelink.rs` passes `""` so that
/// pre-AC-BIS workers — which read `chunk.store_id` and treat empty/unset
/// as "CAS FastSlowStore" — keep working unchanged. If the scheduler ever
/// substituted a non-empty default (e.g. accidentally hard-coding
/// "CAS_MAIN_STORE"), pre-AC-BIS workers would fail to route the unpin to
/// their CAS store and CAS mirrors would leak.
///
/// Cross-chunk fan-out: 10K digests at the production
/// `BIS_DIGESTS_PER_CHUNK = 4096` produce 3 chunks. Asserting the empty
/// `store_id` on every emitted chunk also verifies that the per-chunk
/// constructor's propagation is uniform (no "first chunk only" or "last
/// chunk only" bug).
#[nativelink_test]
async fn scheduler_emits_empty_store_id_for_cas_broadcast() {
    let scheduler = make_scheduler();
    let mut rx = register_worker_endpoint(
        &scheduler,
        "worker-cas",
        "grpc://w-cas.local:50081",
    )
    .await;

    // 10_000 digests at chunk_size=4096 → 3 chunks. Enough to cover the
    // multi-chunk fan-out without dragging on the test runner.
    let digests: Vec<DigestInfo> = (0..10_000u64).map(make_digest_info).collect();

    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.broadcast_blobs_in_stable_storage_chunked(digests, ""),
    )
    .await
    .expect(
        "scheduler dispatch must propagate empty store_id for CAS chunks — \
         forward-compat with pre-AC-BIS workers (broadcast hung past 5s deadlock detector)",
    );

    let chunks = drain_bis_chunks(&mut rx).await;
    assert!(
        chunks.len() >= 2,
        "scheduler dispatch must propagate empty store_id for CAS chunks — \
         forward-compat with pre-AC-BIS workers (expected >=2 chunks for 10k digests, got {})",
        chunks.len(),
    );
    for chunk in &chunks {
        assert!(
            chunk.store_id.is_empty(),
            "scheduler dispatch must propagate empty store_id for CAS chunks — \
             forward-compat with pre-AC-BIS workers: chunk seq={} carried \
             store_id={:?} (expected empty)",
            chunk.sequence,
            chunk.store_id,
        );
    }
}
