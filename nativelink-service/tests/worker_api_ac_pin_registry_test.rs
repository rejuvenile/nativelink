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

//! Server-side `AcPinRegistry` integration tests for Option A AC
//! mirroring (#268).
//!
//! Coverage:
//!   1. **Field-17 advertisement registers in the AC registry**
//!      (under-action): a `BlobsAvailableNotification` carrying
//!      `pinned_ac_mirror_entries` produces matching entries in the
//!      `AcPinRegistry`, keyed by `(worker_cas_endpoint, store_id,
//!      digest)`.
//!   2. **Empty field-17 produces no spurious entries** (over-action):
//!      a `BlobsAvailable` carrying NO AC entries does not introduce
//!      ghost rows.
//!   3. **Field 17 is hard-partitioned from field 16** (the digest-
//!      collision regression): the same digest advertised under
//!      field 17 (`pinned_ac_mirror_entries`) does NOT cause a CAS
//!      `BlobLocalityMap` registration. (And vice-versa.)
//!   4. **Worker disconnect wipes the AC registry endpoint**: after
//!      the connection drops, the AC pin registry's per-endpoint
//!      entry is cleared (sibling of the existing locality_map
//!      wipe).
//!
//! Production composition: real `WorkerApiServer`, real
//! `ApiWorkerScheduler`, real `AcPinRegistry`, real
//! `BlobLocalityMap`. Each asynchronous-handler assertion is
//! polled within a `tokio::time::timeout(5s)` so a missing
//! drain is reported as a deadlock with a bespoke message
//! rather than silently hanging the test runner.
//!
//! Mutation guidance:
//!   * Comment out `ac_pin_registry.register_ac_pin(...)` inside
//!     `worker_api_server.rs`'s field-17 handler block →
//!     test 1 (`field_17_populates_ac_registry_only`) red-fails
//!     with "must register AC pin".
//!   * Replace the field-17 dispatch's `register_ac_pin(...)` with
//!     `locality_map.register_blobs(...)` → test 3 (`field_17_does_not_touch_cas_locality_map`)
//!     red-fails with "AC pin MUST NOT register in CAS locality map".
//!   * Comment out the `ac_pin_registry.wipe_endpoint(...)` call on
//!     disconnect → test 4 (`worker_disconnect_wipes_ac_registry`)
//!     red-fails.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use async_trait::async_trait;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableNotification, ConnectWorkerRequest, MirrorPinEntry, UpdateForScheduler,
    update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::{
    ConnectWorkerStream, NowFn, WorkerApiServer,
};
use nativelink_util::ac_pin_registry::{
    SharedAcPinRegistry, new_shared_ac_pin_registry,
};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{
    SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{
    UpdateOperationType, WorkerStateManager,
};
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
const AC_STORE_NAME: &str = "AC_MAIN_STORE";
const OTHER_AC_STORE_NAME: &str = "AC_OTHER_STORE";

#[expect(dead_code, reason = "Mock trait impl: not all variants/fields are exercised in this test file")]
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

struct AcRegistryContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    locality_map: SharedBlobLocalityMap,
    ac_pin_registry: SharedAcPinRegistry,
    cas_endpoint: String,
}

async fn setup_with_ac_registry(cas_endpoint: &str) -> Result<AcRegistryContext, Error> {
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager::new());
    let worker_registry = Arc::new(WorkerRegistry::new());
    let scheduler = ApiWorkerScheduler::new(
        state_manager.clone(),
        platform_property_manager,
        WorkerAllocationStrategy::default(),
        tasks_or_worker_change_notify,
        BASE_WORKER_TIMEOUT_S,
        worker_registry,
    );

    let locality_map = new_shared_blob_locality_map();
    let ac_pin_registry = new_shared_ac_pin_registry();

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let now_fn: NowFn = Box::new(static_now_fn);
    let worker_api_server = WorkerApiServer::new_with_now_fn(
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
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(AcRegistryContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        locality_map,
        ac_pin_registry,
        cas_endpoint: cas_endpoint.to_string(),
    })
}

fn ac_entry(d: DigestInfo, store_id: &str) -> MirrorPinEntry {
    MirrorPinEntry {
        digest: Some(ProtoDigest::from(d)),
        store_id: store_id.to_string(),
    }
}

fn empty_ba(endpoint: &str) -> BlobsAvailableNotification {
    BlobsAvailableNotification {
        worker_cas_endpoint: endpoint.to_string(),
        digests: vec![],
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
        pinned_ac_mirror_entries: vec![],
    }
}

/// Poll a closure until it returns Some, with a per-call deadline.
/// Used so tests don't sleep blindly waiting for the
/// `BlobsAvailable` background handler to land.
async fn await_until<T, F>(label: &'static str, mut probe: F) -> T
where
    F: FnMut() -> Option<T>,
{
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(out) = probe() {
                return out;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    match result {
        Ok(v) => v,
        Err(_) => panic!(
            "must not deadlock — {label} contract violated (timed out waiting \
             for the AC registry side-effect to land)"
        ),
    }
}

/// Test 1 (under-action) + over-action probe of test 3:
/// `pinned_ac_mirror_entries` (field 17) registers in the
/// AcPinRegistry under the matching `(endpoint, store_id, digest)`,
/// AND those same digests do NOT register in the CAS
/// `BlobLocalityMap`.
#[nativelink_test]
async fn field_17_populates_ac_registry_only() -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.20:50081";
    let ctx = setup_with_ac_registry(cas_endpoint).await?;

    let d1 = DigestInfo::new([0xA1u8; 32], 100);
    let d2 = DigestInfo::new([0xA2u8; 32], 200);

    let mut notification = empty_ba("");
    notification.pinned_ac_mirror_entries =
        vec![ac_entry(d1, AC_STORE_NAME), ac_entry(d2, AC_STORE_NAME)];

    ctx.worker_stream
        .send(Update::BlobsAvailable(notification))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    // Under-action: AC registry MUST contain both entries.
    let entries = await_until("AC registry registration", || {
        let snap = ctx.ac_pin_registry.snapshot_endpoint(&ctx.cas_endpoint)?;
        if snap.len() == 2 { Some(snap) } else { None }
    })
    .await;
    let digests: Vec<_> = entries.iter().map(|(_, d)| *d).collect();
    assert!(digests.contains(&d1));
    assert!(digests.contains(&d2));
    for (sid, _) in &entries {
        assert_eq!(
            sid.as_ref(),
            AC_STORE_NAME,
            "AC pin must register under matching store_id"
        );
    }

    // Over-action: CAS locality_map MUST be untouched (the
    // digest-collision exploit defense — field 17 is hard-partitioned
    // from field 16 / locality_map).
    let map = ctx.locality_map.read();
    assert!(
        map.lookup_workers(&d1).is_empty(),
        "AC pin MUST NOT register in CAS locality map; \
         over-action: cross-channel leakage on field 17 \
         (this is the digest-collision exploit that drove the \
         revert of merge 563c8ebb)"
    );
    assert!(
        map.lookup_workers(&d2).is_empty(),
        "AC pin MUST NOT register in CAS locality map (sibling digest)"
    );
    assert_eq!(map.digest_count(), 0);

    Ok(())
}

/// Test 2 (over-action of registration): an empty field 17 MUST NOT
/// produce ghost entries. Catches "always insert empty hashset"
/// bugs and "register on any keep-alive" bugs.
#[nativelink_test]
async fn empty_field_17_creates_no_ac_entries() -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.21:50081";
    let ctx = setup_with_ac_registry(cas_endpoint).await?;

    ctx.worker_stream
        .send(Update::BlobsAvailable(empty_ba("")))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    // Give the handler time to run. We can't rely on a positive
    // signal (the assertion is "no entries"), so wait briefly via
    // a yield loop with a hard deadline. After 250 ms with no
    // registrations, declare the contract upheld.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    while tokio::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        ctx.ac_pin_registry.endpoint_count(),
        0,
        "empty field 17 MUST NOT create AC registry entries; \
         over-action: handler registered ghost rows on no input"
    );
    Ok(())
}

/// Test 3 (full hard-partition): field 16 (`pinned_mirror_entries`,
/// CAS) populates locality_map ONLY; field 17 populates AC registry
/// ONLY. Sending both with the SAME digest exercises both directions
/// of the partition simultaneously — neither cross-talks.
#[nativelink_test]
async fn fields_16_and_17_remain_hard_partitioned() -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.22:50081";
    let ctx = setup_with_ac_registry(cas_endpoint).await?;

    let aliased = DigestInfo::new([0xCCu8; 32], 50);
    let mut notification = empty_ba("");
    // Field 16 (CAS) carries the digest under cas_STORE.
    notification.pinned_mirror_entries = vec![ac_entry(aliased, "cas_STORE")];
    // Field 17 (AC) carries the SAME digest under AC_MAIN_STORE.
    notification.pinned_ac_mirror_entries = vec![ac_entry(aliased, AC_STORE_NAME)];

    ctx.worker_stream
        .send(Update::BlobsAvailable(notification))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    // Wait until BOTH side-effects land.
    await_until("CAS locality_map registration", || {
        if ctx.locality_map.read().lookup_workers(&aliased).is_empty() {
            None
        } else {
            Some(())
        }
    })
    .await;
    drop(
        await_until("AC registry registration on aliased digest", || {
            ctx.ac_pin_registry.snapshot_endpoint(&ctx.cas_endpoint)
        })
        .await,
    );

    // Field 16 → locality_map (CAS).
    let map = ctx.locality_map.read();
    let cas_workers = map.lookup_workers(&aliased);
    assert_eq!(cas_workers.len(), 1, "field 16 must register CAS locality");
    assert_eq!(&*cas_workers[0], cas_endpoint);
    drop(map);

    // Field 17 → AC registry (AC).
    let ac = ctx
        .ac_pin_registry
        .snapshot_endpoint(&ctx.cas_endpoint)
        .expect("AC registry must contain the field-17 entry");
    assert_eq!(ac.len(), 1);
    assert_eq!(ac[0].0.as_ref(), AC_STORE_NAME);
    assert_eq!(ac[0].1, aliased);

    // Hard partition assertions: neither index leaks into the other.
    let registry_counts = ctx.ac_pin_registry.endpoint_counts();
    assert_eq!(
        registry_counts.get(cas_endpoint).copied().unwrap_or(0),
        1,
        "AC registry must hold exactly the AC entry, not the CAS entry"
    );
    Ok(())
}

/// Test 4: a worker disconnect (the connection stream drops on the
/// server side) MUST wipe the worker's AC pin registry entry — the
/// AC registry is a sibling of the locality map for the
/// boot-epoch / disconnect lifecycle, not a separate state machine
/// with its own forgetfulness rules.
///
/// This is the under-action test of `wipe_endpoint`. The over-action
/// (wipe_endpoint NOT firing on a different endpoint) is covered in
/// the unit tests of `ac_pin_registry.rs`.
#[nativelink_test]
async fn worker_disconnect_wipes_ac_registry() -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.23:50081";
    let ctx = setup_with_ac_registry(cas_endpoint).await?;

    // Register an AC pin so disconnect has work to do.
    let d1 = DigestInfo::new([0xDDu8; 32], 12);
    let mut notification = empty_ba("");
    notification.pinned_ac_mirror_entries = vec![ac_entry(d1, AC_STORE_NAME)];
    ctx.worker_stream
        .send(Update::BlobsAvailable(notification))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    drop(
        await_until("AC registry seeded before disconnect", || {
            ctx.ac_pin_registry.snapshot_endpoint(&ctx.cas_endpoint)
        })
        .await,
    );

    // Drop the worker stream → server sees connection close.
    drop(ctx.worker_stream);
    // Drain the server-side stream so the disconnect handler runs.
    let mut stream = ctx.connection_worker_stream;
    let drain = async {
        while stream.next().await.is_some() {}
    };
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect(
            "must not deadlock — server-side disconnect drain timed out; \
             worker_api_server didn't propagate the closed connection",
        );

    // After disconnect the AC registry endpoint MUST be empty.
    await_until("AC registry wipe-on-disconnect", || {
        if ctx
            .ac_pin_registry
            .snapshot_endpoint(&ctx.cas_endpoint)
            .is_none()
        {
            Some(())
        } else {
            None
        }
    })
    .await;

    Ok(())
}

/// Test 5: per-store_id partition. Two AC entries advertised under
/// two different `store_id`s land as DISTINCT registry entries; a
/// later sweep that targets only `AC_MAIN_STORE` removes only the
/// matching `(store_id, digest)` pair. Mirrors the real BIS
/// broadcast loop's per-AC-store sweep.
#[nativelink_test]
async fn per_store_partitioning_in_ac_registry() -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.24:50081";
    let ctx = setup_with_ac_registry(cas_endpoint).await?;

    let d_main = DigestInfo::new([0x11u8; 32], 4);
    let d_other = DigestInfo::new([0x22u8; 32], 4);

    let mut notification = empty_ba("");
    notification.pinned_ac_mirror_entries = vec![
        ac_entry(d_main, AC_STORE_NAME),
        ac_entry(d_other, OTHER_AC_STORE_NAME),
    ];
    ctx.worker_stream
        .send(Update::BlobsAvailable(notification))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    let entries = await_until("AC registry double-store registration", || {
        let snap = ctx.ac_pin_registry.snapshot_endpoint(&ctx.cas_endpoint)?;
        if snap.len() == 2 { Some(snap) } else { None }
    })
    .await;
    let stores: Vec<_> = entries.iter().map(|(s, _)| s.as_ref().to_string()).collect();
    assert!(stores.iter().any(|s| s == AC_STORE_NAME));
    assert!(stores.iter().any(|s| s == OTHER_AC_STORE_NAME));

    // Drain on `AC_MAIN_STORE` only — `AC_OTHER_STORE`'s entry must
    // survive.
    ctx.ac_pin_registry.remove_digests_for_endpoint_in_store(
        &ctx.cas_endpoint,
        AC_STORE_NAME,
        &[d_main],
    );
    let after = ctx
        .ac_pin_registry
        .snapshot_endpoint(&ctx.cas_endpoint)
        .expect("OTHER store's entry MUST survive the AC_MAIN_STORE-only sweep");
    assert_eq!(
        after.len(),
        1,
        "per-store sweep MUST drain only the matching (store_id, digest)"
    );
    assert_eq!(after[0].0.as_ref(), OTHER_AC_STORE_NAME);
    assert_eq!(after[0].1, d_other);

    Ok(())
}
