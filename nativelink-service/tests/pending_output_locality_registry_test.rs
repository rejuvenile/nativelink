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

//! Integration tests for `WorkerApiServer::pending_output_locality_registry`
//! (#12 H4 invariant — phase 1/3).
//!
//! # What this tests
//!
//! The `pending_output_locality_registry` is a SECOND `AcPinRegistry`
//! instance wired into `WorkerApiServer` alongside the existing
//! `ac_pin_registry`. Its lifecycle mirrors the existing registry:
//!
//!   - **Registration:** when a worker `UpdateActionResult` carries a
//!     `cas_endpoint` field AND that endpoint is currently connected
//!     (liveness check), the server inserts the referenced output
//!     digests BEFORE the AC entry is committed. (Phase 2 wires the
//!     actual insertion; phase 1 proves the registry exists, has the
//!     right lifecycle, and is never consulted on the CAS has() path.)
//!
//!   - **Wipe on disconnect:** on worker disconnect the per-endpoint
//!     set is cleared — same as `ac_pin_registry`.
//!
//!   - **Wipe on boot-epoch change:** on reconnect with a new
//!     `boot_epoch_id` the per-endpoint set is cleared — same as
//!     `ac_pin_registry`.
//!
//!   - **Liveness validation:** a claimed `cas_endpoint` is only
//!     accepted if it appears in the server's live `endpoint_state`
//!     map. Unknown endpoints are logged at `warn!` and silently
//!     dropped — no insertion.
//!
//!   - **Short-circuit guard:** the `pending_output_locality_registry`
//!     MUST NOT be consulted by `has_with_results` paths. The CAS
//!     `BlobLocalityMap` guards the upload short-circuit; routing AC
//!     pins through it would cause `bytestream_server::write` /
//!     `cas_server::batch_update_blobs` to skip uploads of Action
//!     proto bytes, producing permanent silent data loss.
//!
//! # Production composition
//!
//! Real `WorkerApiServer`, real `ApiWorkerScheduler`, real
//! `AcPinRegistry` (for `pending_output_locality_registry`), real
//! `BlobLocalityMap`. Every asynchronous assertion is wrapped in a
//! `tokio::time::timeout(5s)` deadlock detector.
//!
//! # Mutation guidance
//!
//! - Comment out `pending_output_locality_registry.wipe_endpoint(...)`
//!   in the disconnect-cleanup path → test
//!   `wipe_endpoint_clears_on_disconnect` red-fails with bespoke
//!   "pending_output_locality_registry not wiped on disconnect".
//! - Comment out `pending_output_locality_registry.wipe_endpoint(...)`
//!   in the boot-epoch-flip path → test
//!   `wipe_endpoint_on_boot_epoch_change` red-fails with bespoke
//!   "pending_output_locality_registry not wiped on boot-epoch change".
//! - Remove the liveness check → test
//!   `liveness_check_rejects_unconnected_endpoint` red-fails with
//!   "liveness check allowed unconnected endpoint".
//! - Wire the registry into a `BlobLocalityMap` lookup →
//!   test `short_circuit_guard_registry_not_visible_in_has` red-fails
//!   with "registry leaked into has() — upload short-circuit trap".

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use async_trait::async_trait;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableNotification, ConnectWorkerRequest, MirrorPinEntry, UpdateForScheduler,
    update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::{ConnectWorkerStream, NowFn, WorkerApiServer};
use nativelink_util::ac_pin_registry::{SharedAcPinRegistry, new_shared_ac_pin_registry};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{
    SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
const AC_STORE_ID: &str = "AC_MAIN_STORE";

#[expect(
    dead_code,
    reason = "Mock trait impl: not all variants/fields are exercised in this test file"
)]
#[derive(Debug)]
enum WorkerStateManagerCalls {
    UpdateOperation((OperationId, WorkerId, UpdateOperationType)),
}

#[expect(dead_code, reason = "Mock trait impl: variant present for completeness")]
#[derive(Debug)]
enum WorkerStateManagerReturns {
    UpdateOperation(Result<(), Error>),
}

#[expect(dead_code, reason = "Mock trait impl: rx_call/tx_resp present for completeness")]
#[derive(MetricsComponent)]
struct MockWorkerStateManager {
    rx_call: Arc<AsyncMutex<mpsc::UnboundedReceiver<WorkerStateManagerCalls>>>,
    tx_call: mpsc::UnboundedSender<WorkerStateManagerCalls>,
    rx_resp: Arc<AsyncMutex<mpsc::UnboundedReceiver<WorkerStateManagerReturns>>>,
    tx_resp: mpsc::UnboundedSender<WorkerStateManagerReturns>,
}

impl MockWorkerStateManager {
    fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Arc::new(AsyncMutex::new(rx_call)),
            tx_call,
            rx_resp: Arc::new(AsyncMutex::new(rx_resp)),
            tx_resp,
        }
    }
}

#[async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        self.tx_call
            .send(WorkerStateManagerCalls::UpdateOperation((
                operation_id.clone(),
                worker_id.clone(),
                update,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerStateManagerReturns::UpdateOperation(result) => result,
        }
    }
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "NowFn requires a Result-returning closure"
)]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

/// Shared context for tests needing a connected worker.
struct WorkerCtx {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: Arc<WorkerApiServer>,
    _connection_worker_stream: ConnectWorkerStream,
    worker_stream: mpsc::Sender<Update>,
    locality_map: SharedBlobLocalityMap,
    ac_pin_registry: SharedAcPinRegistry,
    pending_output_locality_registry: SharedAcPinRegistry,
    cas_endpoint: String,
}

async fn setup_with_pending_registry(
    cas_endpoint: &str,
    boot_epoch_id: u64,
) -> Result<WorkerCtx, Error> {
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager,
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    None,
    );

    let locality_map = new_shared_blob_locality_map();
    let ac_pin_registry = new_shared_ac_pin_registry();
    let pending_output_locality_registry = new_shared_ac_pin_registry();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());

    let now_fn: NowFn = Box::new(static_now_fn);
    let worker_api_server = Arc::new(
        WorkerApiServer::new_with_now_fn(
            &WorkerApiConfig {
                scheduler: SCHEDULER_NAME.to_string(),
                compatible_build_shas: None,
            },
            &schedulers,
            now_fn,
            [1u8; 6],
            Some(locality_map.clone()),
            None,
            None,
            None,
            Some(ac_pin_registry.clone()),
            Some(pending_output_locality_registry.clone()),
        )
        .err_tip(|| "Error creating WorkerApiServer")?,
    );

    let (tx, rx) = mpsc::channel(1);
    tx.send(Update::ConnectWorkerRequest(ConnectWorkerRequest {
        cas_endpoint: cas_endpoint.to_string(),
        boot_epoch_id,
        ..Default::default()
    }))
    .await
    .unwrap();

    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            (
                Ok(UpdateForScheduler {
                    update: Some(update),
                }),
                rx,
            )
        })
    }));

    let mut conn_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    // Consume the ConnectionResult so the worker is registered.
    let first_msg = conn_stream
        .next()
        .await
        .expect("expected ConnectionResult")
        .err_tip(|| "ConnectionResult stream error")?;
    match first_msg.update {
        Some(update_for_worker::Update::ConnectionResult(_)) => {}
        other => unreachable!("expected ConnectionResult, got {:?}", other),
    }

    Ok(WorkerCtx {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        _connection_worker_stream: conn_stream,
        worker_stream: tx,
        locality_map,
        ac_pin_registry,
        pending_output_locality_registry,
        cas_endpoint: cas_endpoint.to_string(),
    })
}

fn d(byte: u8) -> DigestInfo {
    DigestInfo::new([byte; 32], 100)
}

fn ac_entry(digest: DigestInfo, store_id: &str) -> MirrorPinEntry {
    MirrorPinEntry {
        digest: Some(digest.into()),
        store_id: store_id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Test 1: registration → lookup roundtrip
//
// Directly call `register_ac_pin` on `pending_output_locality_registry`
// (phase 2 wires the registration from UpdateActionResult; phase 1 just
// verifies the registry is correctly constructed and returns entries).
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn registration_lookup_roundtrip() -> Result<(), Error> {
    let ctx = setup_with_pending_registry("grpc://worker1:50081", 1).await?;
    let reg = &ctx.pending_output_locality_registry;
    let store_id: Arc<str> = Arc::from(AC_STORE_ID);

    reg.register_ac_pin(&ctx.cas_endpoint, store_id.clone(), d(1));
    reg.register_ac_pin(&ctx.cas_endpoint, store_id.clone(), d(2));

    let snap = tokio::time::timeout(Duration::from_secs(5), async {
        reg.snapshot_endpoint(&ctx.cas_endpoint)
    })
    .await
    .expect("must not deadlock — registry.snapshot_endpoint timed out");

    assert!(
        snap.is_some(),
        "pending_output_locality_registry MUST return entries after registration"
    );
    let snap = snap.unwrap();
    assert_eq!(
        snap.len(),
        2,
        "expected 2 registered digests in pending_output_locality_registry"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 2: wipe_endpoint clears on disconnect
//
// Register an AC pin, then drop the worker stream (simulating disconnect).
// The disconnect-cleanup background task fires `wipe_endpoint`. Poll until
// the registry is empty within the 5s deadlock-detector timeout.
//
// Mutation: comment out `pending_output_locality_registry.wipe_endpoint(...)`
// in the disconnect-cleanup path → this test red-fails with the bespoke
// "pending_output_locality_registry not wiped on disconnect" message.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn wipe_endpoint_clears_on_disconnect() -> Result<(), Error> {
    let ctx = setup_with_pending_registry("grpc://worker1:50081", 1).await?;
    let reg = ctx.pending_output_locality_registry.clone();
    let store_id: Arc<str> = Arc::from(AC_STORE_ID);

    // Pre-seed one entry.
    reg.register_ac_pin(&ctx.cas_endpoint, store_id, d(42));
    assert!(
        reg.snapshot_endpoint(&ctx.cas_endpoint).is_some(),
        "precondition: entry must be present before disconnect"
    );

    // Drop the worker stream → causes the background WorkerConnection
    // task to observe stream end and run disconnect-cleanup (which
    // calls `pending_output_locality_registry.wipe_endpoint`).
    drop(ctx.worker_stream);

    // Poll until wiped or timeout.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if reg.snapshot_endpoint("grpc://worker1:50081").is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "pending_output_locality_registry not wiped on disconnect — \
         wipe_endpoint was not called from WorkerConnection disconnect-cleanup path",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3: liveness check rejects unconnected endpoint
//
// The server's `pending_output_endpoint_is_live` helper (phase 1 surface)
// MUST return `false` for an endpoint not present in `endpoint_state`.
// It MUST return `true` for a currently-connected endpoint.
//
// Mutation: remove the liveness check (always return true) → the
// "liveness check allowed unconnected endpoint" assertion fires.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn liveness_check_rejects_unconnected_endpoint() -> Result<(), Error> {
    let ctx = setup_with_pending_registry("grpc://worker1:50081", 1).await?;
    let server = &*ctx._worker_api_server;

    // Connected endpoint IS live.
    assert!(
        server.pending_output_endpoint_is_live("grpc://worker1:50081"),
        "connected endpoint MUST pass liveness check"
    );

    // Unknown endpoint is NOT live.
    assert!(
        !server.pending_output_endpoint_is_live("grpc://unknown:50081"),
        "liveness check allowed unconnected endpoint — \
         endpoint validation did not consult endpoint_state map"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 4: short-circuit guard — registry not visible in has()
//
// Inserting into `pending_output_locality_registry` MUST NOT affect what
// the `BlobLocalityMap` (CAS locality map) reports via
// `has_with_results`-equivalent. This guards the upload short-circuit trap:
// `WorkerProxyStore::has_with_results` reads from `locality_map`; if the
// pending registry were routed there, it would cause
// `bytestream_server::write` to skip uploading Action proto bytes.
//
// Concrete check: seed entry in `pending_output_locality_registry`;
// assert `BlobLocalityMap` does NOT list any endpoint for that digest.
//
// Mutation: wire `pending_output_locality_registry` registrations into
// `locality_map.register_blobs(...)` → the `locality_map.endpoints_for`
// assertion fires with "registry leaked into has() — upload short-circuit trap".
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn short_circuit_guard_registry_not_visible_in_has() -> Result<(), Error> {
    let ctx = setup_with_pending_registry("grpc://worker1:50081", 1).await?;
    let store_id: Arc<str> = Arc::from(AC_STORE_ID);
    let digest = d(99);

    // Insert into the pending output locality registry.
    ctx.pending_output_locality_registry.register_ac_pin(
        &ctx.cas_endpoint,
        store_id,
        digest,
    );

    // The CAS BlobLocalityMap must remain unaffected.
    let locality_endpoints = ctx
        .locality_map
        .read()
        .lookup_workers(&digest);

    assert!(
        locality_endpoints.is_empty(),
        "registry leaked into has() — upload short-circuit trap: \
         pending_output_locality_registry entry for digest {:?} appeared in \
         BlobLocalityMap.lookup_workers(); this would cause \
         bytestream_server::write to skip uploading Action proto bytes",
        digest
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 5: wipe_endpoint on boot-epoch change clears pending registry
//
// When a worker reconnects with a DIFFERENT boot_epoch_id, the server's
// boot-epoch-flip path MUST wipe the pending_output_locality_registry for
// that endpoint (same as it wipes locality_map and ac_pin_registry).
//
// Mutation: comment out `pending_output_locality_registry.wipe_endpoint(...)`
// in the boot-epoch-flip path → this test red-fails with bespoke
// "pending_output_locality_registry not wiped on boot-epoch change".
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn wipe_endpoint_on_boot_epoch_change() -> Result<(), Error> {
    const ENDPOINT: &str = "grpc://worker1:50081";

    // First connection with boot_epoch=1.
    let ctx = setup_with_pending_registry(ENDPOINT, 1).await?;
    let server = ctx._worker_api_server.clone();
    let reg = ctx.pending_output_locality_registry.clone();
    let store_id: Arc<str> = Arc::from(AC_STORE_ID);

    // Seed a pending output entry on the first connection.
    reg.register_ac_pin(ENDPOINT, store_id, d(77));
    assert!(
        reg.snapshot_endpoint(ENDPOINT).is_some(),
        "precondition: pending entry must be present before reconnect"
    );

    // Keep the old stream alive so disconnect-cleanup does NOT fire — the
    // wipe must come EXCLUSIVELY from the boot-epoch-flip path.
    let _old_stream = ctx.worker_stream;

    // Issue a second ConnectWorkerRequest on the SAME server with boot_epoch=2.
    // The boot-epoch-flip wipe fires inside inner_connect_worker before it
    // returns, so no separate server instance is needed.
    let (tx2, rx2) = mpsc::channel(1);
    tx2.send(Update::ConnectWorkerRequest(ConnectWorkerRequest {
        cas_endpoint: ENDPOINT.to_string(),
        boot_epoch_id: 2,
        ..Default::default()
    }))
    .await
    .unwrap();

    let update_stream2 = Box::pin(futures::stream::unfold(rx2, |mut rx| async move {
        rx.recv().await.map(|update| {
            (
                Ok(UpdateForScheduler {
                    update: Some(update),
                }),
                rx,
            )
        })
    }));

    let mut conn2 = server
        .inner_connect_worker_for_testing(update_stream2)
        .await
        .err_tip(|| "second connect_worker failed")?
        .into_inner();

    // Drain the ConnectionResult for the second connection.
    let msg2 = conn2
        .next()
        .await
        .expect("expected ConnectionResult on second connect")
        .err_tip(|| "second ConnectionResult error")?;
    match msg2.update {
        Some(update_for_worker::Update::ConnectionResult(_)) => {}
        other => unreachable!("expected ConnectionResult, got {:?}", other),
    }

    // Now the registry should be wiped because boot_epoch changed 1→2.
    // Poll with timeout in case the wipe is deferred by task scheduling.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if reg.snapshot_endpoint(ENDPOINT).is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "pending_output_locality_registry not wiped on boot-epoch change — \
         wipe_endpoint not called from the boot-epoch-flip path in inner_connect_worker",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 6: BlobsAvailable notification does NOT insert into
//         pending_output_locality_registry
//
// The pending_output_locality_registry is fed ONLY by the
// UpdateActionResult path (phase 2). BlobsAvailableNotification
// field-17 feeds the EXISTING `ac_pin_registry`, NOT the pending registry.
// This is the correct partitioning — the two registries serve different
// purposes (AC peer-fetch vs. output pre-registration).
//
// Synchronization: a negative assertion ("registry MUST NOT contain X")
// cannot stand alone — if the background task never ran, the assertion
// passes vacuously. A positive tripwire resolves this: we seed a digest
// into BlobsAvailable.pinned_ac_mirror_entries (field-17) and poll until
// `ac_pin_registry` shows the entry. That proves the background task
// processed the tick. Only then is the negative assertion on
// `pending_output_locality_registry` trustworthy.
//
// Mutation: wire field-17 notifications into pending_output_locality_registry
// → the "pending registry MUST NOT be fed by BlobsAvailable" assertion fires.
// ---------------------------------------------------------------------------
#[nativelink_test]
async fn blobs_available_does_not_insert_into_pending_registry() -> Result<(), Error> {
    const ENDPOINT: &str = "grpc://worker1:50081";
    let ctx = setup_with_pending_registry(ENDPOINT, 1).await?;
    let ac_reg = ctx
        ._worker_api_server
        .ac_pin_registry_for_testing()
        .expect("ac_pin_registry must be Some — setup wires it");
    let pending_reg = &ctx.pending_output_locality_registry;
    let store_id = AC_STORE_ID.to_string();
    let digest = d(55);

    // Send a BlobsAvailable with field-17 (pinned_ac_mirror_entries).
    // This should populate ac_pin_registry, NOT pending_output_locality_registry.
    ctx.worker_stream
        .send(Update::BlobsAvailable(BlobsAvailableNotification {
            worker_cas_endpoint: ENDPOINT.to_string(),
            pinned_ac_mirror_entries: vec![ac_entry(digest, &store_id)],
            ..Default::default()
        }))
        .await
        .unwrap();

    // Positive tripwire: poll until ac_pin_registry shows the field-17 entry.
    // This proves the background task finished processing the BlobsAvailable
    // tick before we make the negative assertion on pending_output_locality_registry.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ac_reg.snapshot_endpoint(ENDPOINT).is_some() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "ac_pin_registry not populated after field-17 BlobsAvailable within 5s — \
         precondition for the negative pending-registry check failed; \
         the background task may not have processed the tick",
    );

    // Now the negative assertion is trustworthy: the background task ran
    // and routed to ac_pin_registry, not to pending_output_locality_registry.
    let snap = pending_reg.snapshot_endpoint(ENDPOINT);
    assert!(
        snap.is_none(),
        "pending registry MUST NOT be fed by BlobsAvailable field-17 — \
         found {:?} entries in pending_output_locality_registry; \
         the two registries serve distinct channels",
        snap.as_ref().map(|v| v.len()),
    );

    Ok(())
}
