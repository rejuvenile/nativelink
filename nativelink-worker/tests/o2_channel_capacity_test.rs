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

//! O2: control-plane `mpsc::channel` capacity tests.
//!
//! Guards the fix for O2 from the 2026-05-29 action-latency backlog:
//! the single-slot `channel(1)` at `worker_api_client_wrapper.rs:166`
//! serializes ALL control-plane writes under concurrent action completion.
//! Raising capacity to `WORKER_API_CHANNEL_CAPACITY` reduces head-of-line
//! blocking without changing FIFO ordering (the consumer remains sequential).
//!
//! Tests:
//! 1. `o2_channel_capacity_constant_is_correct` — constant-value guard per
//!    `.claude/rules/reviewer-dispatch.md`; fails on mutation to 1.
//! 2. `o2_concurrent_sends_complete_within_capacity_without_consumer` —
//!    proves N senders can enqueue N messages simultaneously when the
//!    consumer is stalled; with capacity=1 the 2nd send blocks (timeout).
//! 3. `o2_blobs_available_arrives_before_execution_response` — full
//!    in-process gRPC server; proves FIFO ordering is preserved across the
//!    channel depth increase (#129 external-consistency invariant).

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_server::{
    WorkerApi, WorkerApiServer,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsAvailableNotification, ConnectWorkerRequest, ExecuteComplete, ExecuteResult,
    UpdateForScheduler, UpdateForWorker,
};
use nativelink_worker::worker_api_client_wrapper::{
    WORKER_API_CHANNEL_CAPACITY, WorkerApiClientTrait, WorkerApiClientWrapper,
};
use tokio::sync::{mpsc, Notify};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Outer timeout. Tests guard against hangs — actual failure manifests as a
/// fast assertion failure, not a silent hang. Generous to avoid CI flakes.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// 1. Constant-value guard
// ---------------------------------------------------------------------------

/// The capacity constant at its declaration site must be exactly 16.
///
/// Mutation target: change `WORKER_API_CHANNEL_CAPACITY` to 1 and this
/// test fails with a bespoke message naming the O2 invariant.
///
/// Bespoke failure message:
/// "o2: WORKER_API_CHANNEL_CAPACITY must be 16 to avoid serializing
///  concurrent completions behind a 1-slot buffer; channel(1) is the
///  original defect"
#[test]
fn o2_channel_capacity_constant_is_correct() {
    assert_eq!(
        WORKER_API_CHANNEL_CAPACITY,
        16,
        "o2: WORKER_API_CHANNEL_CAPACITY must be 16 to avoid serializing \
         concurrent completions behind a 1-slot buffer; channel(1) is the \
         original defect"
    );
}

// ---------------------------------------------------------------------------
// 2. Concurrency — N senders don't block each other within capacity
// ---------------------------------------------------------------------------

/// Proves that 16 concurrent senders can each enqueue one item without
/// waiting for the consumer, when the channel is sized at
/// `WORKER_API_CHANNEL_CAPACITY`.
///
/// Mechanics: create a fresh `mpsc::channel(WORKER_API_CHANNEL_CAPACITY)`,
/// hold the receiver (consumer stalled), spawn 16 concurrent senders (a
/// fixed literal, independent of the constant), and verify ALL sends
/// complete within `TEST_TIMEOUT`.
///
/// Mutation target: change `WORKER_API_CHANNEL_CAPACITY` to 1 — a 16-item
/// load into a 1-slot channel with no consumer means 15 senders block
/// forever → the timeout fires with the bespoke message below.
/// Restoring capacity to 16 → all 16 senders fit in the 16-slot channel →
/// test passes. The literal `16` is intentionally independent of the
/// constant so the mutation is detectable.
///
/// Bespoke failure message:
/// "o2: 16 concurrent sends must not block on a depth-N channel; channel(1)
///  serializes them — O2 defect re-introduced"
#[tokio::test(flavor = "multi_thread")]
async fn o2_concurrent_sends_complete_within_capacity_without_consumer() {
    // Channel sized at the production constant; hold _rx so the channel
    // stays open (consumer stalled throughout the test).
    // CAPPED AT WORKER_API_CHANNEL_CAPACITY: test is gated on that constant.
    let (tx, _rx) = mpsc::channel::<u32>(WORKER_API_CHANNEL_CAPACITY);

    // 16 senders — a FIXED LITERAL independent of WORKER_API_CHANNEL_CAPACITY
    // so that mutating the constant to 1 makes 15 of these block.
    let handles: Vec<_> = (0u32..16)
        .map(|i| {
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(i).await.expect("send should not fail while rx held")
            })
        })
        .collect();

    // All 16 senders must complete within the timeout. With capacity=1 the
    // 2nd sender blocks forever (consumer is stalled) and we hit the
    // bespoke failure message below.
    let result = tokio::time::timeout(TEST_TIMEOUT, async {
        for h in handles {
            h.await.expect("sender task panicked")
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "o2: 16 concurrent sends must not block on a depth-{WORKER_API_CHANNEL_CAPACITY} \
         channel; channel(1) serializes them — O2 defect re-introduced"
    );
}

// ---------------------------------------------------------------------------
// 3. FIFO ordering — blobs_available before execution_response
// ---------------------------------------------------------------------------

/// Records scheduler-bound messages in arrival order at the server.
struct OrderRecordingServer {
    /// Receives `(message_type_tag, seq)` tuples in server-arrival order.
    arrival_tx: mpsc::UnboundedSender<&'static str>,
    /// Held until the test releases it, so the server handler stays alive.
    done_notify: Arc<Notify>,
}

#[tonic::async_trait]
impl WorkerApi for OrderRecordingServer {
    type ConnectWorkerStream =
        Pin<Box<dyn Stream<Item = Result<UpdateForWorker, Status>> + Send + 'static>>;

    async fn connect_worker(
        &self,
        request: Request<tonic::Streaming<UpdateForScheduler>>,
    ) -> Result<Response<Self::ConnectWorkerStream>, Status> {
        let mut stream = request.into_inner();
        let arrival_tx = self.arrival_tx.clone();
        let done_notify = self.done_notify.clone();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                if let Ok(msg) = msg {
                    let tag = match msg.update.as_ref() {
                        Some(Update::ConnectWorkerRequest(_)) => "ConnectWorkerRequest",
                        Some(Update::ExecuteResult(_)) => "ExecuteResult",
                        Some(Update::ExecuteComplete(_)) => "ExecuteComplete",
                        Some(Update::BlobsAvailable(_)) => "BlobsAvailable",
                        Some(Update::BisAck(_)) => "BisAck",
                        Some(Update::ChunkedMessage(_)) => "ChunkedMessage",
                        Some(Update::KeepAliveRequest(_)) => "KeepAliveRequest",
                        Some(Update::GoingAwayRequest(_)) => "GoingAwayRequest",
                        Some(_) | None => "Other",
                    };
                    let _ = arrival_tx.send(tag);
                }
            }
            done_notify.notify_one();
        });

        // Return an empty server→client stream; we only care about the
        // client→server direction (worker's control-plane messages).
        let empty: Pin<Box<dyn Stream<Item = Result<UpdateForWorker, Status>> + Send>> =
            Box::pin(futures::stream::empty());
        Ok(Response::new(empty))
    }
}

/// Spawn an in-process recording server. Returns (channel, arrival_rx,
/// done_notify, server_handle).
async fn spawn_recording_server() -> (
    Channel,
    mpsc::UnboundedReceiver<&'static str>,
    Arc<Notify>,
    tokio::task::JoinHandle<()>,
) {
    let (arrival_tx, arrival_rx) = mpsc::unbounded_channel();
    let done_notify = Arc::new(Notify::new());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let svc = WorkerApiServer::new(OrderRecordingServer {
        arrival_tx,
        done_notify: done_notify.clone(),
    });
    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await,
        );
    });

    let channel = Endpoint::try_from(format!("http://127.0.0.1:{port}"))
        .expect("valid endpoint")
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("connect to in-process recording server");

    (channel, arrival_rx, done_notify, handle)
}

/// Verifies that `blobs_available` arrives at the server BEFORE
/// `execution_response`, preserving the #129 external-consistency invariant.
///
/// The invariant: every blob the action produced MUST be observable from the
/// server BEFORE the client sees the ExecuteResult. The worker→scheduler
/// stream is processed in arrival order; sending BlobsAvailable first
/// guarantees the locality_map is populated by the time ExecuteResult is
/// processed.
///
/// This ordering must hold regardless of channel capacity: deepening the
/// buffer does NOT change FIFO ordering — the consumer is sequential.
#[tokio::test(flavor = "multi_thread")]
async fn o2_blobs_available_arrives_before_execution_response() {
    let (channel, mut arrival_rx, done_notify, server_handle) = spawn_recording_server().await;

    let mut wrapper = WorkerApiClientWrapper::from(WorkerApiClient::new(channel));

    // Initiate the bidirectional stream.
    let _stream = tokio::time::timeout(
        TEST_TIMEOUT,
        wrapper.connect_worker(ConnectWorkerRequest::default()),
    )
    .await
    .expect("connect_worker timed out")
    .expect("connect_worker failed");

    // Send blobs_available FIRST, then execution_response — exactly the
    // publish-closure ordering that preserves #129.
    tokio::time::timeout(TEST_TIMEOUT, async {
        wrapper
            .blobs_available(BlobsAvailableNotification::default())
            .await
            .expect("blobs_available send failed");
        wrapper
            .execution_response(ExecuteResult::default())
            .await
            .expect("execution_response send failed");
        wrapper
            .execution_complete(ExecuteComplete::default())
            .await
            .expect("execution_complete send failed");
    })
    .await
    .expect("sends timed out");

    // Drop the wrapper to close the stream; the server's reader task will
    // drain and notify done.
    drop(wrapper);

    // Wait for the server to drain all messages.
    tokio::time::timeout(TEST_TIMEOUT, done_notify.notified())
        .await
        .expect("server did not finish draining messages in time");

    server_handle.abort();

    // Collect arrival order; skip ConnectWorkerRequest (it was first, as
    // expected, but we're testing the ordering of the 3 action messages).
    let mut arrivals: Vec<&'static str> = Vec::new();
    while let Ok(tag) = arrival_rx.try_recv() {
        arrivals.push(tag);
    }

    // ConnectWorkerRequest must be the first message.
    assert_eq!(
        arrivals.first().copied(),
        Some("ConnectWorkerRequest"),
        "o2: first message must be ConnectWorkerRequest, got: {:?}",
        arrivals
    );

    // Among the action messages, BlobsAvailable must precede ExecuteResult.
    let blobs_pos = arrivals
        .iter()
        .position(|&t| t == "BlobsAvailable")
        .expect(
            "o2: BlobsAvailable not received at server — #129 FIFO ordering broken",
        );
    let result_pos = arrivals
        .iter()
        .position(|&t| t == "ExecuteResult")
        .expect(
            "o2: ExecuteResult not received at server — #129 FIFO ordering broken",
        );

    assert!(
        blobs_pos < result_pos,
        "o2: #129 ordering violated — BlobsAvailable (pos {blobs_pos}) must \
         arrive before ExecuteResult (pos {result_pos}); arrivals: {arrivals:?}. \
         Deepening the channel buffer must not reorder FIFO messages."
    );
}
