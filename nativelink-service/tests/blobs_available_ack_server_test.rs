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

//! (FL-688 v3 §3.8) Server-side `BlobsAvailableAck` emission test.
//!
//! ## Seams crossed (production composition)
//!
//!   1. **Wire envelope** — `Update::ChunkedMessage(ChunkedMessage {
//!      payload: Some(BlobsAvailable(chunk)) })`, exactly as the worker's
//!      `chunked_message` RPC would send it.
//!   2. **Server dispatch arm** — `worker_api_server.rs`'s match on
//!      `Update::ChunkedMessage` → `BlobsAvailable` payload.
//!   3. **Accumulator acceptance gate** —
//!      `BlobsAvailableAccumulator::merge_chunk_outcome` returns
//!      `Accepted` / `Dropped`.
//!   4. **Ack emission** — on `Accepted` the server sends a
//!      `BlobsAvailableAck { broadcast_id, sequence, worker_instance_token
//!      }` back on the `UpdateForWorker` stream (per-chunk); on `Dropped`
//!      it sends NO ack (so the worker re-advertises on reconnect).
//!
//! `accepted_chunk_emits_ack_per_chunk` drives a 2-chunk broadcast and
//! asserts the server emits exactly one `BlobsAvailableAck` PER chunk,
//! each echoing the chunk's `(broadcast_id, sequence,
//! worker_instance_token)`. `token_zero_chunk_is_dropped_no_ack` drives a
//! chunk with `worker_instance_token=0` (the accumulator drops it) and
//! asserts NO ack is emitted.
//!
//! ## Mutation
//!
//! Delete the `instance.worker_tx.send(... BlobsAvailableAck ...)` in the
//! `MergeOutcome::Accepted` arm of `worker_api_server.rs` →
//! `accepted_chunk_emits_ack_per_chunk` MUST red-fail with
//! "server must emit a BlobsAvailableAck for each accepted chunk".
//! Change the `MergeOutcome::Dropped` arm to also ack →
//! `token_zero_chunk_is_dropped_no_ack` MUST red-fail with
//! "server must NOT ack a dropped chunk".

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::cas_server::WorkerApiConfig;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobDigestInfo, BlobsAvailableChunk, BlobsAvailableNotification, ChunkedMessage,
    ConnectWorkerRequest, UpdateForScheduler, chunked_message, update_for_worker,
};
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::worker_api_server::{ConnectWorkerStream, WorkerApiServer};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::blobs_available_chunking::chunk_blobs_available;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use tokio::sync::{Notify, mpsc};
use tokio_stream::StreamExt;

const BASE_WORKER_TIMEOUT_S: u64 = 100;
const WORKER_TOKEN: u64 = 0xBEEF_CAFE_1234_5678;

/// Minimal `WorkerStateManager` — these tests never drive an operation
/// (they only exercise the BlobsAvailable chunk → ack path), so
/// `update_operation` is unreachable and returns Ok. `#[derive(
/// MetricsComponent)]` requires a non-unit struct, so we keep a single
/// phantom field (mirrors `worker_api_build_sha_test`'s pattern).
#[derive(MetricsComponent)]
struct MockWorkerStateManager {
    #[metric(help = "placeholder field; the no-op state manager has no metrics")]
    _placeholder: u64,
}

#[async_trait]
impl WorkerStateManager for MockWorkerStateManager {
    async fn update_operation(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
        _update: UpdateOperationType,
    ) -> Result<(), Error> {
        Ok(())
    }
}

fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(10))
}

struct AckTestContext {
    _scheduler: Arc<ApiWorkerScheduler>,
    _worker_api_server: WorkerApiServer,
    /// The server→worker stream where `BlobsAvailableAck`s are observed.
    connection_worker_stream: ConnectWorkerStream,
    _worker_id: WorkerId,
    /// The worker→server stream where chunks are injected.
    worker_stream: mpsc::Sender<Update>,
}

async fn setup() -> Result<AckTestContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";
    const UUID_SIZE: usize = 36;

    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let tasks_or_worker_change_notify = Arc::new(Notify::new());
    let state_manager = Arc::new(MockWorkerStateManager { _placeholder: 0 });
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
        Some(locality_map),
        None,
        None,
        None,
        None,
        None,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let connect_worker_request = ConnectWorkerRequest {
        cas_endpoint: "grpc://127.0.0.1:50081".to_string(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(64);
    tx.send(Update::ConnectWorkerRequest(connect_worker_request))
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
    let mut connection_worker_stream = worker_api_server
        .inner_connect_worker_for_testing(update_stream)
        .await?
        .into_inner();

    // First server→worker message is the ConnectionResult.
    let first_update = connection_worker_stream
        .next()
        .await
        .expect("first message")
        .err_tip(|| "Expected success")?
        .update
        .err_tip(|| "Expected update")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => connection_result.worker_id,
        other => unreachable!("Expected ConnectionResult, got {other:?}"),
    };
    assert_eq!(worker_id.len(), UUID_SIZE);

    Ok(AckTestContext {
        _scheduler: scheduler,
        _worker_api_server: worker_api_server,
        connection_worker_stream,
        _worker_id: worker_id.into(),
        worker_stream: tx,
    })
}

fn build_notification(n: usize) -> BlobsAvailableNotification {
    let digest_infos: Vec<BlobDigestInfo> = (0..n)
        .map(|i| BlobDigestInfo {
            digest: Some(ProtoDigest {
                hash: format!("{i:064x}"),
                size_bytes: i64::try_from(i).unwrap_or(0),
            }),
        })
        .collect();
    BlobsAvailableNotification {
        worker_cas_endpoint: "grpc://127.0.0.1:50081".to_string(),
        is_full_snapshot: true,
        digest_infos,
        cpu_load_pct: 42,
        ..Default::default()
    }
}

/// Pull the next server→worker message within a timeout, returning the
/// inner `Update`. The timeout is the deadlock detector.
async fn next_update(
    stream: &mut ConnectWorkerStream,
) -> Result<update_for_worker::Update, Error> {
    let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("server must send a message within 5s (deadlock?)")
        .expect("stream not ended")
        .err_tip(|| "stream error")?;
    msg.update.err_tip(|| "expected update")
}

/// 1. The server emits one `BlobsAvailableAck` per ACCEPTED chunk,
///    echoing each chunk's identity.
#[nativelink_test]
async fn accepted_chunk_emits_ack_per_chunk() -> Result<(), Error> {
    let mut ctx = setup().await?;

    // A 2-chunk broadcast: 2 entries with a per-chunk cap of 1 → seq 0,1.
    let notification = build_notification(2);
    let chunks = chunk_blobs_available(notification, 77, WORKER_TOKEN, String::new(), 1)
        .map_err(|reason| make_err!(tonic::Code::Internal, "chunker: {reason}"))?;
    assert_eq!(chunks.len(), 2, "fixture must produce exactly 2 chunks");

    for chunk in &chunks {
        ctx.worker_stream
            .send(Update::ChunkedMessage(ChunkedMessage {
                payload: Some(chunked_message::Payload::BlobsAvailable(chunk.clone())),
            }))
            .await
            .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;
    }

    // Collect the next two server→worker messages; each MUST be a
    // BlobsAvailableAck echoing the matching chunk's (broadcast,seq,token).
    let mut acks = Vec::new();
    for _ in 0..chunks.len() {
        match next_update(&mut ctx.connection_worker_stream).await? {
            update_for_worker::Update::BlobsAvailableAck(ack) => acks.push(ack),
            other => panic!(
                "server must emit a BlobsAvailableAck for each accepted chunk; got {other:?}"
            ),
        }
    }
    acks.sort_by_key(|a| a.sequence);
    assert_eq!(acks.len(), 2, "two accepted chunks → two acks (per-chunk)");
    for (seq, ack) in acks.iter().enumerate() {
        assert_eq!(
            ack.broadcast_id, 77,
            "ack must echo the chunk broadcast_id"
        );
        assert_eq!(
            ack.sequence, seq as u32,
            "ack must echo the chunk sequence (per-chunk ack)"
        );
        assert_eq!(
            ack.worker_instance_token, WORKER_TOKEN,
            "ack must echo the worker_instance_token so the worker can guard its bounce"
        );
    }
    Ok(())
}

/// 2. A DROPPED chunk (token=0) produces NO ack.
#[nativelink_test]
async fn token_zero_chunk_is_dropped_no_ack() -> Result<(), Error> {
    let mut ctx = setup().await?;

    // A single terminal chunk with worker_instance_token=0 → accumulator
    // drops it (uninitialised token).
    let bad_chunk = BlobsAvailableChunk {
        broadcast_id: 5,
        sequence: 0,
        is_last: true,
        worker_instance_token: 0,
        worker_cas_endpoint: "grpc://127.0.0.1:50081".to_string(),
        is_full_snapshot: true,
        ..Default::default()
    };
    ctx.worker_stream
        .send(Update::ChunkedMessage(ChunkedMessage {
            payload: Some(chunked_message::Payload::BlobsAvailable(bad_chunk)),
        }))
        .await
        .map_err(|e| make_err!(tonic::Code::Internal, "send: {e}"))?;

    // No ack must be emitted. Give the server a moment to (not) reply: a
    // short timeout that EXPECTS to elapse confirms the absence.
    let observed = tokio::time::timeout(
        Duration::from_millis(300),
        ctx.connection_worker_stream.next(),
    )
    .await;
    match observed {
        Err(_elapsed) => Ok(()), // no message — correct
        Ok(Some(Ok(msg))) => match msg.update {
            Some(update_for_worker::Update::BlobsAvailableAck(ack)) => panic!(
                "server must NOT ack a dropped chunk; got ack {ack:?}"
            ),
            other => panic!("unexpected server→worker message for a dropped chunk: {other:?}"),
        },
        Ok(other) => panic!("unexpected stream state: {other:?}"),
    }
}
