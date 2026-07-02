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
use nativelink_util::ac_pin_registry::AcPinRegistry;
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
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
        false, // (#sched M1 rebalance) p_headroom_gate OFF
        0, // (#sched M1 rebalance v2) p_idle_threshold_pct 0 = override OFF
        2, // (#sched M1 rebalance v2) p_headroom_override_factor
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

/// Test 3: the BIS-ack sweep gate — `remove_digests_for_endpoint_batch` returns
/// `false` when NOTHING was removed for that endpoint (no-op), and `true` when
/// entries were actually removed. The BIS sweep MUST gate the
/// `notify_ac_pin_resync_for_endpoint` push on `true`; a no-op removal MUST NOT
/// trigger a push to the endpoint's worker.
///
/// (M1/C8/P3 convergent fix — 4 reviewers flagged unconditional push for all
/// endpoints even when none of their pins matched the draining digests.)
///
/// Setup: endpoint_a has digest_a pinned; endpoint_b has digest_b (DIFFERENT).
/// Drain carries digest_a only. When BIS sweep processes endpoint_b:
/// `remove_digests_for_endpoint_batch(endpoint_b, [digest_a])` returns `false`
/// (digest_a not in endpoint_b's set), so no push fires for endpoint_b.
/// endpoint_b's worker (rx_b) must see NO AcPinResync.
///
/// Mutation guard: in `remove_digests_for_endpoint_batch`, change the
/// `removed_any` return to always `true` (e.g. `let removed_any = true;`).
/// The no-op arm now returns `true` → push fires to endpoint_b → rx_b sees
/// a spurious AcPinResync → test red-fails with the bespoke message:
/// "gate missing: no-op removal (endpoint B has digest_b, drain carries only
/// digest_a) triggered AcPinResync push to worker B ..."
#[nativelink_test]
async fn noop_removal_does_not_trigger_resync_push() {
    let scheduler = make_scheduler();
    let endpoint_a = "grpc://worker-a:50081";
    let endpoint_b = "grpc://worker-b:50081";

    // Register BOTH workers so endpoint_b's rx can detect spurious pushes.
    let mut rx_a = register_worker_endpoint(&scheduler, "worker-a", endpoint_a).await;
    let mut rx_b = register_worker_endpoint(&scheduler, "worker-b", endpoint_b).await;

    // AcPinRegistry: endpoint_a has digest_a; endpoint_b has digest_b (different).
    // Drain carries only digest_a → endpoint_b's set is UNCHANGED by this drain.
    let registry = AcPinRegistry::new();
    let store_id: Arc<str> = Arc::from("AC_MAIN_STORE");
    let digest_a = DigestInfo::new([0x01u8; 32], 100);
    let digest_b = DigestInfo::new([0x02u8; 32], 100);
    registry.register_ac_pin(endpoint_a, store_id.clone(), digest_a);
    registry.register_ac_pin(endpoint_b, store_id.clone(), digest_b);

    let drain_a = [digest_a];
    let drains: &[(Arc<str>, &[DigestInfo])] = &[(store_id.clone(), drain_a.as_slice())];

    // 1. No-op path: drain digest_a for endpoint_b (endpoint_b only has digest_b).
    //    Endpoint_b's set is unchanged → must return false.
    //    Gate: no push. Mutation (removed_any = true): fires push → rx_b gets resync.
    let removed_b = registry.remove_digests_for_endpoint_batch(endpoint_b, drains);
    assert!(
        !removed_b,
        "remove_digests_for_endpoint_batch must return false when the drain \
         digests do not overlap the endpoint's pinned set — no-op removal must \
         not gate a push (endpoint_b has digest_b; drain carries only digest_a)"
    );
    if removed_b {
        // Production gate: only push when removed_b is true. With the gate broken
        // (mutation forces true), this fires and rx_b sees a spurious AcPinResync.
        tokio::time::timeout(
            Duration::from_secs(5),
            scheduler.notify_ac_pin_resync_for_endpoint(endpoint_b),
        )
        .await
        .expect("notify must not hang");
    }
    assert!(
        !received_ac_pin_resync(&mut rx_b).await,
        "gate missing: no-op removal (endpoint B has digest_b, drain carries only \
         digest_a) triggered AcPinResync push to worker B — \
         remove_digests_for_endpoint_batch must return false when the drain does \
         not overlap the endpoint's pinned set; the BIS sweep must skip the push \
         (mutation: force removed_any = true in remove_digests_for_endpoint_batch)"
    );
    // endpoint_a must also be clean — no push was intended for it yet.
    assert!(
        !received_ac_pin_resync(&mut rx_a).await,
        "endpoint A received an AcPinResync during the no-op endpoint_b step — \
         no push should have fired for A before the real removal step"
    );

    // 2. Real removal path: drain digest_a for endpoint_a (endpoint_a has digest_a).
    //    Entry is removed → must return true → push fires.
    let removed_a = registry.remove_digests_for_endpoint_batch(endpoint_a, drains);
    assert!(
        removed_a,
        "remove_digests_for_endpoint_batch must return true when entries were \
         actually removed — real removal must enable the push so the worker \
         re-advertises its full AC-pin set and the registry reconverges"
    );
    if removed_a {
        tokio::time::timeout(
            Duration::from_secs(5),
            scheduler.notify_ac_pin_resync_for_endpoint(endpoint_a),
        )
        .await
        .expect("notify must not hang");
    }
    assert!(
        received_ac_pin_resync(&mut rx_a).await,
        "push missing: real removal for endpoint A did not trigger AcPinResync — \
         after remove_digests_for_endpoint_batch returned true the sweep MUST push \
         so the worker re-advertises its full AC-pin set and the registry reconverges"
    );
    // endpoint_b must NOT have received a resync from endpoint_a's push.
    assert!(
        !received_ac_pin_resync(&mut rx_b).await,
        "endpoint B received an AcPinResync from endpoint A's push — routing \
         leaked to a non-owning worker; notify_ac_pin_resync_for_endpoint must \
         be endpoint-scoped, not a broadcast"
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
