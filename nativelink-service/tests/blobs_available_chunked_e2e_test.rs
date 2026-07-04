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

//! (#99 — Fix #4) Production-composition end-to-end test for the
//! BlobsAvailable chunked path.
//!
//! ## Seams crossed
//!
//! Per CLAUDE.md "identify-the-seam discipline" + reviewer findings
//! (code-reviewer Critical #2, testing-czar CRIT-1, red-team
//! "no test crosses every seam"), this test composes:
//!
//!   1. **Producer** — `chunk_blobs_available()` (the worker-side
//!      chunker) emits `BlobsAvailableChunk` envelopes from a single
//!      `BlobsAvailableNotification`.
//!   2. **Wire envelope** — each chunk wrapped in
//!      `Update::ChunkedMessage(ChunkedMessage { payload: Some(
//!      chunked_message::Payload::BlobsAvailable(chunk)) })`, exactly
//!      as `worker_api_client_wrapper::chunked_message` would.
//!   3. **Server dispatch arm** —
//!      `worker_api_server.rs:876-928`'s match on
//!      `Update::ChunkedMessage(envelope)` → match on
//!      `envelope.payload`'s oneof variant.
//!   4. **Server accumulator** — `BlobsAvailableAccumulator::merge_chunk`
//!      via the dispatch arm.
//!   5. **Path A commit** — `handle_blobs_available(notification).await`
//!      fires only on terminal chunk + sequence-completeness gate.
//!   6. **Locality-map write** — `register_blobs_iter` reaches
//!      `SharedBlobLocalityMap`.
//!
//! ## Mutation step
//!
//! Comment out the `Update::ChunkedMessage` arm body in
//! `worker_api_server.rs:876-928` (or just the `merge_chunk` call):
//! the test MUST red-fail with the bespoke message
//! `"production composition: worker chunker → wire → server
//! accumulator → locality_map.write — must converge for N-entry
//! snapshot"`. If that path is removed, the locality_map stays
//! empty and the assertion in this test fires, not a generic
//! timeout.

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
    BlobDigestInfo, ChunkedMessage, ConnectWorkerRequest, UpdateForScheduler,
    chunked_message, update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::{ConnectWorkerStream, WorkerApiServer};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::{
    SharedBlobLocalityMap, new_shared_blob_locality_map,
};
use nativelink_util::blobs_available_chunking::{
    BLOBS_AVAILABLE_PER_CHUNK, chunk_blobs_available,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;

#[derive(Debug)]
enum WorkerStateManagerCalls {
    UpdateOperation((OperationId, WorkerId, UpdateOperationType)),
}

#[derive(Debug)]
enum WorkerStateManagerReturns {
    UpdateOperation(Result<(), Error>),
}

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

fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

struct LocalityTestContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    _connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    worker_stream: mpsc::Sender<Update>,
    locality_map: SharedBlobLocalityMap,
}

async fn setup_api_server_with_locality(
    cas_endpoint: &str,
) -> Result<LocalityTestContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
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

    let mut schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
            compatible_build_shas: None,
        },
        &schedulers,
        Box::new(static_now_fn),
        [1u8; 6],
        Some(locality_map.clone()),
        None,
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
    let (tx, rx) = mpsc::channel(64);
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
    let first_update = maybe_first_message
        .expect("first message")
        .err_tip(|| "Expected success")?
        .update
        .err_tip(|| "Expected update")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => {
            connection_result.worker_id
        }
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(LocalityTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        _connection_worker_stream: connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
        locality_map,
    })
}

fn build_notification(
    n: usize,
    cas_endpoint: &str,
) -> nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableNotification {
    let digest_infos: Vec<BlobDigestInfo> = (0..n)
        .map(|i| BlobDigestInfo {
            digest: Some(ProtoDigest {
                hash: format!("{:064x}", i),
                size_bytes: i64::try_from(i).unwrap_or(0),
            }),
            ts_boot_epoch: 0,
            ts_counter: 0,
        })
        .collect();
    nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableNotification {
        worker_cas_endpoint: cas_endpoint.to_string(),
        digests: vec![],
        is_full_snapshot: true,
        evicted_digests: vec![],
        evicted_blob_infos: Vec::new(),
        digest_infos,
        cpu_load_pct: 42,
        cached_directory_digests: vec![],
        added_subtree_digests: vec![],
        removed_subtree_digests: vec![],
        is_full_subtree_snapshot: false,
        p_core_load_pct: 40,
        e_core_load_pct: 30,
        pinned_mirror_digests: vec![],
        mirror_used_bytes: 1234,
        mirror_max_bytes: 65536,
        pinned_mirror_entries: vec![],
        pinned_ac_mirror_entries: vec![],
        indefinite_pin_saturated: false,
        swap_used_bytes: 0,
        memory_pressure_level: 0,
        memory_pressured: false,
        available_disk_bytes: 0,
        disk_pressured: false,
    }
}

fn dinfo(i: usize) -> DigestInfo {
    let hash_str = format!("{:064x}", i);
    let mut hash = [0u8; 32];
    hex::decode_to_slice(&hash_str, &mut hash).unwrap();
    DigestInfo::new(hash, i as u64)
}

async fn send_chunked_broadcast(
    worker_stream: &mpsc::Sender<Update>,
    notification: nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BlobsAvailableNotification,
    broadcast_id: u64,
    worker_instance_token: u64,
) -> Result<usize, Error> {
    let chunks = chunk_blobs_available(
        notification,
        broadcast_id,
        worker_instance_token,
        String::new(),
        BLOBS_AVAILABLE_PER_CHUNK,
    )
    .map_err(|reason| make_err!(tonic::Code::Internal, "chunker: {reason}"))?;
    let chunk_count = chunks.len();
    for chunk in chunks {
        let envelope = ChunkedMessage {
            payload: Some(chunked_message::Payload::BlobsAvailable(chunk)),
        };
        worker_stream
            .send(Update::ChunkedMessage(envelope))
            .await
            .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;
    }
    Ok(chunk_count)
}

async fn await_locality_count(
    locality_map: &SharedBlobLocalityMap,
    expected: usize,
    max_wait: Duration,
) -> usize {
    let deadline = std::time::Instant::now() + max_wait;
    loop {
        let count = locality_map.read().digest_count();
        if count >= expected {
            return count;
        }
        if std::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[nativelink_test]
async fn chunked_5k_entry_snapshot_populates_locality_map(
) -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.10:50081";
    let ctx = setup_api_server_with_locality(cas_endpoint).await?;

    const N: usize = 5_000;
    let notification = build_notification(N, cas_endpoint);
    let chunk_count = tokio::time::timeout(
        Duration::from_secs(5),
        send_chunked_broadcast(&ctx.worker_stream, notification, 1, 0xCAFEF00D),
    )
    .await
    .expect(
        "production composition: worker chunker → wire → server accumulator → \
         locality_map.write — chunked send must complete within 5s for 5K-entry \
         snapshot",
    )?;
    assert_eq!(chunk_count, 2, "5K / 4096 per chunk rounds up to 2 chunks");

    let observed = await_locality_count(&ctx.locality_map, N, Duration::from_secs(5)).await;
    assert_eq!(
        observed, N,
        "production composition: worker chunker → wire → server accumulator → \
         locality_map.write — must converge for {}-entry snapshot; saw {} entries",
        N, observed
    );

    let map = ctx.locality_map.read();
    let workers_d0 = map.lookup_workers(&dinfo(0));
    assert_eq!(
        workers_d0.len(),
        1,
        "first digest must be present and routed to our endpoint"
    );
    assert_eq!(&*workers_d0[0], cas_endpoint);
    let workers_dn = map.lookup_workers(&dinfo(N - 1));
    assert_eq!(
        workers_dn.len(),
        1,
        "last digest must be present and routed to our endpoint"
    );

    Ok(())
}

#[nativelink_test]
async fn chunked_50k_entry_snapshot_populates_locality_map(
) -> Result<(), Box<dyn core::error::Error>> {
    let cas_endpoint = "grpc://192.168.1.11:50081";
    let ctx = setup_api_server_with_locality(cas_endpoint).await?;

    const N: usize = 50_000;
    let notification = build_notification(N, cas_endpoint);
    let chunk_count = tokio::time::timeout(
        Duration::from_secs(10),
        send_chunked_broadcast(&ctx.worker_stream, notification, 2, 0xCAFEF00D),
    )
    .await
    .expect(
        "production composition: worker chunker → wire → server accumulator → \
         locality_map.write — chunked send must complete within 10s for 50K-entry \
         snapshot",
    )?;
    assert!(
        (12..=14).contains(&chunk_count),
        "50K / 4096 per chunk = ~13 chunks; saw {}",
        chunk_count
    );

    let observed = await_locality_count(&ctx.locality_map, N, Duration::from_secs(10)).await;
    assert_eq!(
        observed, N,
        "production composition: worker chunker → wire → server accumulator → \
         locality_map.write — must converge for {}-entry snapshot; saw {} entries",
        N, observed
    );

    Ok(())
}

#[nativelink_test]
async fn chunked_oversized_snapshot_rejected_by_chunker(
) -> Result<(), Box<dyn core::error::Error>> {
    use nativelink_util::blobs_available_chunking::BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST;

    let cas_endpoint = "grpc://192.168.1.12:50081";
    let ctx = setup_api_server_with_locality(cas_endpoint).await?;

    let n = 1 + BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST * BLOBS_AVAILABLE_PER_CHUNK;
    let notification = build_notification(n, cas_endpoint);

    let result = chunk_blobs_available(
        notification,
        3,
        0xCAFEF00D,
        String::new(),
        BLOBS_AVAILABLE_PER_CHUNK,
    );
    assert!(
        result.is_err(),
        "production composition: chunker MUST reject a snapshot exceeding \
         BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST = {} chunks; saw Ok({:?}) — \
         Fix #2 (dsr BLOCK-1) regression",
        BLOBS_AVAILABLE_MAX_CHUNKS_PER_BROADCAST,
        result.as_ref().map(|c| c.len()),
    );

    let observed =
        await_locality_count(&ctx.locality_map, 1, Duration::from_millis(200)).await;
    assert_eq!(
        observed, 0,
        "locality_map must remain empty when chunker rejects the broadcast; \
         saw {} entries",
        observed,
    );

    Ok(())
}
