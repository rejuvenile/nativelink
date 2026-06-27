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

//! (FL-688 v3 Stage A fix) Server-side routing half of the AC-pin convergence
//! contract: `ApiWorkerScheduler::notify_ac_pin_resync_for_endpoint(endpoint)`
//! resolves the endpoint to the owning worker (via `endpoint_to_worker`) and
//! pushes a single `Update::AcPinResync` onto THAT worker's `tx`.
//!
//! This is the server side of the BLOCK pair-a found on Stage A: after an
//! OUT-OF-BAND AC-pin registry removal (BIS-ack sweep / AcProxy peer-NotFound /
//! cap-truncation) the server must NOTIFY the worker to re-advertise, because
//! the worker's skip-gate suppresses the unchanged tick. The worker-side half
//! (the resync clears `last_sent_ac_pin_set` → full re-advertisement) is in
//! `nativelink-worker/tests/ac_pin_resync_convergence_test.rs`.
//!
//! Production wiring: `src/bin/nativelink.rs`'s BIS-ack sweep + the bin-wired
//! `AcProxyStore` resync callback + `worker_api_server.rs`'s field-17 handling
//! all call this trait method; the `ApiWorkerScheduler` impl routes it.
//!
//! Tests:
//!   1. `resync_routes_to_owning_worker_only`: two workers on distinct
//!      endpoints; a resync for endpoint A lands on worker A's tx and NOT
//!      worker B's. Proves endpoint→worker routing (not a broadcast).
//!   2. `resync_for_unknown_endpoint_is_silent_noop`: a resync for an endpoint
//!      with no connected worker sends nothing and does not panic (the
//!      reconnect snapshot is the backstop).
//!
//! Both wrap the receive in `tokio::time::timeout` as a deadlock detector.
//!
//! Mutation guard: in `ApiWorkerScheduler::notify_ac_pin_resync_for_endpoint`,
//! comment out the `worker.tx.send(msg)` → test 1 red-fails with its bespoke
//! "endpoint→worker resync routing missing" message.

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
    UpdateForWorker, update_for_worker::Update as ServerUpdate,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::{Notify, mpsc};

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
        512 * 1024,
        8,
    )
}

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
        42,
        0,
        cas_endpoint.to_string(),
        0,
        0,
    );
    WorkerScheduler::add_worker(scheduler.as_ref(), worker)
        .await
        .expect("add_worker");
    rx
}

/// Drain the rx within a short window and return whether at least one
/// `AcPinResync` update arrived. Skips the initial `ConnectionResult` (sent on
/// add_worker) and any other arms.
async fn received_ac_pin_resync(rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>) -> bool {
    let mut saw_resync = false;
    while let Ok(Some(msg)) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
    {
        if let Some(ServerUpdate::AcPinResync(_)) = msg.update {
            saw_resync = true;
        }
    }
    saw_resync
}

/// Test 1: routing is endpoint-scoped, not a broadcast. A resync for endpoint A
/// must land on worker A's tx and MUST NOT land on worker B's tx.
#[nativelink_test]
async fn resync_routes_to_owning_worker_only() {
    let scheduler = make_scheduler();
    let endpoint_a = "grpc://worker-a:50081";
    let endpoint_b = "grpc://worker-b:50081";
    let mut rx_a = register_worker_endpoint(&scheduler, "worker-a", endpoint_a).await;
    let mut rx_b = register_worker_endpoint(&scheduler, "worker-b", endpoint_b).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.notify_ac_pin_resync_for_endpoint(endpoint_a),
    )
    .await
    .expect("notify_ac_pin_resync_for_endpoint must not hang");

    assert!(
        received_ac_pin_resync(&mut rx_a).await,
        "endpoint→worker resync routing missing: notify_ac_pin_resync_for_endpoint(A) did not push \
         an Update::AcPinResync to worker A's tx — after an out-of-band AC-pin registry removal the \
         server MUST push the resync so the worker re-advertises (the convergence path Stage A's \
         heartbeat removal otherwise leaves only to reconnect)"
    );
    assert!(
        !received_ac_pin_resync(&mut rx_b).await,
        "resync routing leaked to a non-owning worker: notify_ac_pin_resync_for_endpoint(A) pushed \
         an AcPinResync to worker B's tx — the push must be scoped to the endpoint's worker, not a \
         broadcast"
    );
}

/// Test 2: a resync for an endpoint with no connected worker is a silent no-op
/// (no panic, no send). The worker's reconnect full snapshot is the backstop.
#[nativelink_test]
async fn resync_for_unknown_endpoint_is_silent_noop() {
    let scheduler = make_scheduler();
    let endpoint_a = "grpc://worker-a:50081";
    let mut rx_a = register_worker_endpoint(&scheduler, "worker-a", endpoint_a).await;

    // Resync for an endpoint that has no connected worker.
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.notify_ac_pin_resync_for_endpoint("grpc://ghost-worker:50081"),
    )
    .await
    .expect("notify_ac_pin_resync_for_endpoint(unknown) must not hang");

    assert!(
        !received_ac_pin_resync(&mut rx_a).await,
        "resync for an UNKNOWN endpoint must not push to any connected worker; worker A received an \
         AcPinResync it was not the target of"
    );
}
