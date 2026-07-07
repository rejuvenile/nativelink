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

//! (#sigkill-gap) Worker-intake quiesce contract.
//!
//! Two halves of one contract, both proven here in production composition (a
//! real `WorkerApiServer` driven through `inner_connect_worker_for_testing`,
//! the same path the per-connection `handle_blobs_available` handler runs on):
//!
//! 1. **Handler path is SUPPRESSED when quiesced.** After the SIGTERM Phase-0b
//!    latch is set (`shutdown_quiesce_handle().quiesce()`), a `BlobsAvailable`
//!    tick reporting a digest the server LACKS must NOT cause the server to send
//!    `UploadMissingBlobs` to the worker, AND the
//!    `shutdown_suppressed_backfill_solicitations_total` counter must increment
//!    (the quiesce-escape detector observes the suppression). This closes ALL
//!    THREE handler-invoked feeds (mark_stable/solicit + the pinned-mirror pull)
//!    because they early-return at the SAME function entry.
//!    **Mutation:** do NOT set the latch → the server solicits → red-fail.
//!
//! 2. **ShutdownPuller path STILL pulls when quiesced.** With the SAME latch
//!    SET, the server-initiated `pull_all_worker_blobs_at_shutdown` MUST still
//!    solicit + land a worker-only blob (it passes `None` for the latch — it is
//!    the drain, not the storm). This is the convergent BLOCK guard: the gate
//!    must distinguish handler-invoked (suppress) from ShutdownPuller-invoked
//!    (allow). **Mutation:** gate the ShutdownPuller call too (pass the latch)
//!    → the pull solicits nothing → the blob never lands → red-fail.
//!
//! No sleep-as-synchronization for the positive assertions: the puller's own
//! completion loop + a `tokio::time::timeout` deadlock detector synchronize.
//! The negative assertion (NO solicitation) necessarily polls a bounded window
//! — there is no event for "a message that never arrives" — but it is paired
//! with the counter assertion, which IS an edge-triggered signal.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableNotification, ConnectWorkerRequest, UpdateForScheduler, update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::small_blob_dispatcher::{SmallBlobDispatcher, SmallBlobDispatcherConfig};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{SharedBlobLocalityMap, new_shared_blob_locality_map};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;
const SCHEDULER_NAME: &str = "QUIESCE_TEST_SCHEDULER";
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[expect(
    clippy::unnecessary_wraps,
    reason = "WorkerApiServer expects a fallible time fn"
)]
const fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

// Minimal MockWorkerStateManager (the quiesce path never touches it).
#[derive(Debug, nativelink_metric::MetricsComponent)]
struct MockWorkerStateManager {
    _unused: u8,
}

#[async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        unreachable!("quiesce test does not invoke update_operation")
    }
}

fn make_server(
    cas_store: Store,
) -> Result<(Arc<WorkerApiServer>, SharedBlobLocalityMap, Arc<SmallBlobDispatcher>), Error> {
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
    let dispatcher = Arc::new(SmallBlobDispatcher::new(SmallBlobDispatcherConfig::default()));
    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler);
    let server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        Some(cas_store),
        None,
        Some(dispatcher.clone()),
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;
    Ok((Arc::new(server), locality_map, dispatcher))
}

fn digest_a() -> DigestInfo {
    DigestInfo::try_new(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        11,
    )
    .expect("valid digest")
}

fn blobs_available(cas_endpoint: &str, digests: Vec<DigestInfo>) -> Update {
    Update::BlobsAvailable(BlobsAvailableNotification {
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
        construct_latency_ms_mean: 0,
    })
}

/// HALF 1 — handler path SUPPRESSED when quiesced.
///
/// Drive a connected worker through `inner_connect_worker_for_testing`; SET the
/// worker-intake latch; send a `BlobsAvailable` reporting a digest the server
/// LACKS. The server must NOT emit `UploadMissingBlobs`, AND the suppression
/// counter must advance.
///
/// Mutation: do NOT call `quiesce()` → the handler solicits → an
/// `UploadMissingBlobs` arrives on the worker stream → red-fail with the
/// bespoke message.
#[nativelink_test]
async fn quiesce_suppresses_handler_backfill_solicitation()
-> Result<(), Box<dyn core::error::Error>> {
    const CAS_ENDPOINT: &str = "grpc://192.168.66.7:50081";
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, _locality_map, _dispatcher) = make_server(cas_store.clone())?;

    // Connect a worker.
    let (tx, rx) = mpsc::channel(4);
    tx.send(Update::ConnectWorkerRequest(ConnectWorkerRequest {
        cas_endpoint: CAS_ENDPOINT.to_string(),
        ..Default::default()
    }))
    .await
    .expect("send connect");
    let update_stream = Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| {
            (
                Ok::<_, tonic::Status>(UpdateForScheduler {
                    update: Some(update),
                }),
                rx,
            )
        })
    }));
    let mut worker_stream = server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();
    // Consume the ConnectionResult.
    let first = worker_stream.next().await.expect("first msg").expect("ok");
    assert!(matches!(
        first.update,
        Some(update_for_worker::Update::ConnectionResult(_))
    ));

    let metrics = server.metrics();
    let before = metrics
        .shutdown_suppressed_backfill_solicitations_total
        .load(core::sync::atomic::Ordering::Relaxed);

    // SET the Phase-0b worker-intake latch.
    server.shutdown_quiesce_handle().quiesce();

    // The server LACKS digest_a; without quiesce this tick would solicit an
    // UploadMissingBlobs. Send it.
    let missing = digest_a();
    assert!(
        cas_store.has(missing).await?.is_none(),
        "setup: server CAS must LACK the digest so a non-quiesced tick would solicit",
    );
    tx.send(blobs_available(CAS_ENDPOINT, vec![missing]))
        .await
        .expect("send blobs_available");

    // The handler-invoked backfill runs in a background_spawn!. Wait until the
    // suppression counter advances — an EDGE-triggered signal that the gate ran
    // and early-returned (no polling-for-absence ambiguity).
    let counter_advanced = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async {
        loop {
            let now = metrics
                .shutdown_suppressed_backfill_solicitations_total
                .load(core::sync::atomic::Ordering::Relaxed);
            if now > before {
                return now - before;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "quiesce did not suppress backfill solicitation: the suppression counter \
         never advanced within 5s — the handler-invoked request_missing_blob_uploads \
         did not early-return at the quiesce gate",
    );
    assert!(
        counter_advanced >= 1,
        "quiesce did not suppress backfill solicitation: \
         shutdown_suppressed_backfill_solicitations_total advanced by \
         {counter_advanced} (expected ≥1)",
    );

    // And NO UploadMissingBlobs must have been sent to the worker. Poll the
    // worker stream for a bounded window; any UploadMissingBlobs is a quiesce
    // escape.
    let escape = tokio::time::timeout(Duration::from_millis(300), worker_stream.next()).await;
    if let Ok(Some(Ok(msg))) = escape {
        assert!(
            !matches!(
                msg.update,
                Some(update_for_worker::Update::UploadMissingBlobs(_))
            ),
            "QUIESCE ESCAPE: the server sent UploadMissingBlobs on a quiesced \
             handler tick — the Phase-0b latch did not suppress the backfill \
             solicitation. (Mutation: skip quiesce() → this fires.)"
        );
    }
    Ok(())
}

/// HALF 2 — ShutdownPuller path STILL pulls when quiesced.
///
/// With the latch SET, the server-initiated pull must still solicit + land the
/// worker-only blob. The fake worker uploads on `UploadMissingBlobs`. If the
/// pull were (wrongly) gated by the latch, it would solicit nothing and the
/// blob would never appear.
///
/// Mutation: pass the latch to the ShutdownPuller's
/// `request_missing_blob_uploads` call (gate it) → the pull solicits nothing →
/// the blob never lands → the post-pull `has` is None → red-fail.
#[nativelink_test]
async fn shutdown_pull_still_solicits_when_quiesced() -> Result<(), Box<dyn core::error::Error>> {
    let cas_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (server, locality_map, dispatcher) = make_server(cas_store.clone())?;

    let digest = digest_a();
    let data = Bytes::from_static(b"hello-world");
    assert_eq!(data.len() as u64, digest.size_bytes(), "fixture self-check");
    assert!(
        cas_store.has(digest).await?.is_none(),
        "pre-condition: the blob must be worker-only",
    );

    // Register a fake worker that uploads on UploadMissingBlobs (mirrors the
    // shutdown_pull harness).
    let (wtx, mut wrx) = mpsc::unbounded_channel::<
        nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker,
    >();
    dispatcher.register_worker("grpc://worker-a:50071", 1, wtx);
    locality_map
        .write()
        .register_blobs("grpc://worker-a:50071", &[digest]);
    let cas_for_worker = cas_store.clone();
    let data_for_worker = data.clone();
    nativelink_util::background_spawn!("fake_worker_upload", async move {
        while let Some(msg) = wrx.recv().await {
            if let Some(update_for_worker::Update::UploadMissingBlobs(req)) = msg.update {
                for proto_digest in req.digests {
                    if let Ok(d) = DigestInfo::try_from(proto_digest) {
                        if d == digest {
                            drop(cas_for_worker.update_oneshot(d, data_for_worker.clone()).await);
                        }
                    }
                }
            }
        }
    });

    // SET the worker-intake latch — this MUST NOT stop the server-initiated pull.
    server.shutdown_quiesce_handle().quiesce();

    let summary = tokio::time::timeout(
        NO_DEADLOCK_TIMEOUT,
        server.pull_all_worker_blobs_at_shutdown(),
    )
    .await
    .expect(
        "DEADLOCK / NON-CONVERGENCE: pull_all_worker_blobs_at_shutdown did not \
         return within 5s while quiesced — the ShutdownPuller must run even when \
         the worker-intake latch is set (it passes None for the latch).",
    );

    let landed = {
        let keys = [StoreKey::from(digest)];
        let mut results = [None];
        cas_store.has_with_results(&keys, &mut results).await?;
        results[0]
    };
    assert!(
        landed.is_some(),
        "ShutdownPuller MUST still solicit + land the worker-only blob WHILE the \
         worker-intake latch is set — the pull is the shutdown DRAIN, not the \
         storm, so it passes None and is exempt from Phase-0b suppression. The \
         blob did not land (summary: pulled={}, at_risk_skipped={}). (Mutation: \
         gate the ShutdownPuller's request_missing_blob_uploads call with the \
         latch → this red-fails.)",
        summary.pulled,
        summary.at_risk_skipped,
    );
    Ok(())
}
