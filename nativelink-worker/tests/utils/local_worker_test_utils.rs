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

use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex;
use bytes::Bytes;
use hyper::body::Frame;
use nativelink_config::cas_server::{EndpointConfig, LocalWorkerConfig, WorkerProperty};
use nativelink_error::Error;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BisAck, BlobsAvailableNotification, ChunkedMessage, ConnectWorkerRequest, ExecuteComplete,
    ExecuteResult, GoingAwayRequest, KeepAliveRequest, UpdateForWorker,
};
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_worker::local_worker::LocalWorker;
use nativelink_worker::worker_api_client_wrapper::WorkerApiClientTrait;
use tokio::sync::{broadcast, mpsc};
use tonic::Status;
use tonic::{Response, Streaming, codec::CompressionEncoding};
use tonic_prost::ProstCodec;
// Needed for .decoder().
use tonic::codec::Codec;

use super::mock_running_actions_manager::MockRunningActionsManager;

/// Broadcast Channel Capacity
/// Note: The actual capacity may be greater than the provided capacity.
const BROADCAST_CAPACITY: usize = 1;

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "TODO Fix thix. Triggers on nightly"
)]
enum WorkerClientApiCalls {
    ConnectWorker(ConnectWorkerRequest),
    ExecutionResponse(ExecuteResult),
    BlobsAvailable(BlobsAvailableNotification),
    /// (#97) Recorded BisAck call from the dispatch arm. Tests that
    /// exercise the chunked-message envelope-decode path can pull this
    /// to assert the worker echoed the chunk's
    /// (broadcast_id, sequence, server_instance_token) into a
    /// real `worker_api_client_wrapper::bis_ack` call (vs the
    /// `bis_chunk_handler_test`'s direct ack-sink injection which
    /// bypasses the dispatch arm).
    BisAck(BisAck),
    /// (#99) Recorded `Update::ChunkedMessage(ChunkedMessage(BlobsAvailableChunk))`
    /// from the chunked-emit path. Tests that exercise the chunked
    /// envelope-encode path can pull these to verify slicing,
    /// `(broadcast_id, sequence, is_last)` triples, and that the per-
    /// chunk slices reassemble to the expected snapshot.
    ChunkedMessage(ChunkedMessage),
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "TODO Fix thix. Triggers on nightly"
)]
enum WorkerClientApiReturns {
    ConnectWorker(Result<Response<Streaming<UpdateForWorker>>, Status>),
    ExecutionResponse(Result<(), Error>),
    BlobsAvailable(Result<(), Error>),
    BisAck(Result<(), Error>),
    ChunkedMessage(Result<(), Error>),
}

#[derive(Clone)]
pub(crate) struct MockWorkerApiClient {
    rx_call: Arc<Mutex<mpsc::UnboundedReceiver<WorkerClientApiCalls>>>,
    tx_call: mpsc::UnboundedSender<WorkerClientApiCalls>,
    rx_resp: Arc<Mutex<mpsc::UnboundedReceiver<WorkerClientApiReturns>>>,
    tx_resp: mpsc::UnboundedSender<WorkerClientApiReturns>,
}

impl MockWorkerApiClient {
    pub(crate) fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Arc::new(Mutex::new(rx_call)),
            tx_call,
            rx_resp: Arc::new(Mutex::new(rx_resp)),
            tx_resp,
        }
    }
}

impl Default for MockWorkerApiClient {
    fn default() -> Self {
        unreachable!("We don't test this functionality")
    }
}

impl MockWorkerApiClient {
    pub(crate) async fn expect_connect_worker(
        &self,
        result: Result<Response<Streaming<UpdateForWorker>>, Status>,
    ) -> ConnectWorkerRequest {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiCalls::ConnectWorker(req) => req,
            other => panic!("expect_connect_worker expected ConnectWorker, got : {other:?}"),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ConnectWorker(result))
            .expect("Could not send request to mpsc");
        req
    }

    pub(crate) async fn expect_execution_response(
        &self,
        result: Result<(), Error>,
    ) -> ExecuteResult {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiCalls::ExecutionResponse(req) => req,
            other => panic!("expect_execution_response expected ExecutionResponse, got : {other:?}"),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ExecutionResponse(result))
            .expect("Could not send request to mpsc");
        req
    }

    /// Receive the next call as a BlobsAvailable, returning the notification
    /// payload. Used by ordering tests that need to assert BlobsAvailable
    /// arrives at the worker→scheduler stream BEFORE ExecuteResult so the
    /// server's locality_map is fully populated by the time the client sees
    /// the action result.
    pub(crate) async fn expect_blobs_available(
        &self,
        result: Result<(), Error>,
    ) -> BlobsAvailableNotification {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiCalls::BlobsAvailable(req) => req,
            other => panic!("expect_blobs_available expected BlobsAvailable, got : {other:?}"),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::BlobsAvailable(result))
            .expect("Could not send request to mpsc");
        req
    }

    /// (#97) Receive the next call as a BisAck. Used by the
    /// production-composition test that asserts the worker's
    /// `Update::ChunkedMessage(BlobsInStableStorageChunk)` dispatch
    /// arm wires through `worker_api_client_wrapper::bis_ack` —
    /// the ack-sink-injection path in `bis_chunk_handler_test`
    /// bypasses both the envelope-decode AND the wrapper.
    #[allow(dead_code, reason = "exercised only by BIS dispatch tests")]
    pub(crate) async fn expect_bis_ack(&self, result: Result<(), Error>) -> BisAck {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiCalls::BisAck(req) => req,
            other => panic!("expect_bis_ack expected BisAck, got : {other:?}"),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::BisAck(result))
            .expect("Could not send request to mpsc");
        req
    }

    /// (#99 / Fix #10) Receive the next call as a `ChunkedMessage`.
    /// Used by tests that exercise the worker's chunked
    /// `BlobsAvailable` emit path (e.g.
    /// `tests/blobs_available_chunked_handler_test.rs`); pairs with the
    /// test-utils mock's `chunked_message` impl at the bottom of this
    /// file.
    pub(crate) async fn expect_chunked_message(
        &self,
        result: Result<(), Error>,
    ) -> ChunkedMessage {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiCalls::ChunkedMessage(req) => req,
            other => panic!("expect_chunked_message expected ChunkedMessage, got : {other:?}"),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ChunkedMessage(result))
            .expect("Could not send request to mpsc");
        req
    }

    /// (#97) Drain calls until a `BisAck` arrives, auto-ack'ing every
    /// `BlobsAvailable` along the way (those are the periodic loop
    /// firing on every blob-set change and not what the BIS-dispatch
    /// test is asserting on). Returns the BisAck.
    ///
    /// The match-and-drain pattern is needed because the worker fires
    /// a `BlobsAvailable` immediately on start when
    /// `blobs_available_state` is set (the change-tracker's notify
    /// triggers on the first connection); using `expect_bis_ack`
    /// directly would panic on the first BlobsAvailable.
    #[allow(dead_code, reason = "exercised only by BIS dispatch tests")]
    pub(crate) async fn expect_bis_ack_skipping_blobs_available(&self) -> BisAck {
        loop {
            let mut rx_call_lock = self.rx_call.lock().await;
            let next = rx_call_lock
                .recv()
                .await
                .expect("Could not receive msg in mpsc");
            drop(rx_call_lock);
            match next {
                WorkerClientApiCalls::BisAck(req) => {
                    self.tx_resp
                        .send(WorkerClientApiReturns::BisAck(Ok(())))
                        .expect("Could not send response to mpsc");
                    return req;
                }
                WorkerClientApiCalls::BlobsAvailable(_ba) => {
                    self.tx_resp
                        .send(WorkerClientApiReturns::BlobsAvailable(Ok(())))
                        .expect("Could not send response to mpsc");
                    continue;
                }
                other => panic!(
                    "expect_bis_ack_skipping_blobs_available expected BisAck \
                     or BlobsAvailable, got : {other:?}"
                ),
            }
        }
    }
}

impl WorkerApiClientTrait for MockWorkerApiClient {
    async fn connect_worker(
        &mut self,
        request: ConnectWorkerRequest,
    ) -> Result<Response<Streaming<UpdateForWorker>>, Status> {
        self.tx_call
            .send(WorkerClientApiCalls::ConnectWorker(request))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiReturns::ConnectWorker(result) => result,
            resp => panic!("connect_worker expected ConnectWorker response, received {resp:?}"),
        }
    }

    async fn keep_alive(&mut self, _request: KeepAliveRequest) -> Result<(), Error> {
        unreachable!();
    }

    async fn going_away(&mut self, _request: GoingAwayRequest) -> Result<(), Error> {
        unreachable!();
    }

    async fn execution_response(&mut self, request: ExecuteResult) -> Result<(), Error> {
        self.tx_call
            .send(WorkerClientApiCalls::ExecutionResponse(request))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiReturns::ExecutionResponse(result) => result,
            resp => panic!("execution_response expected ExecutionResponse response, received {resp:?}"),
        }
    }

    async fn execution_complete(&mut self, _request: ExecuteComplete) -> Result<(), Error> {
        Ok(())
    }

    async fn blobs_available(
        &mut self,
        request: BlobsAvailableNotification,
    ) -> Result<(), Error> {
        self.tx_call
            .send(WorkerClientApiCalls::BlobsAvailable(request))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiReturns::BlobsAvailable(result) => result,
            resp => panic!("blobs_available expected BlobsAvailable response, received {resp:?}"),
        }
    }

    async fn bis_ack(
        &mut self,
        request: nativelink_proto::com::github::trace_machina::nativelink::remote_execution::BisAck,
    ) -> Result<(), Error> {
        // (#97) Record the call so the production-composition test
        // (`bis_chunked_dispatch_arm_round_trips_ack`) can assert the
        // dispatch arm fired through `worker_api_client_wrapper::bis_ack`.
        // Tests that don't await `expect_bis_ack` will see the call
        // queue grow but no test-side block, since the ack is fired
        // from a `tokio::spawn`'d task in the dispatch arm — the
        // mpsc::unbounded_channel under the hood means we don't
        // back-pressure the spawned task either.
        self.tx_call
            .send(WorkerClientApiCalls::BisAck(request))
            .expect("Could not send BisAck to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiReturns::BisAck(result) => result,
            resp => panic!("bis_ack expected BisAck response, received {resp:?}"),
        }
    }

    async fn chunked_message(&mut self, request: ChunkedMessage) -> Result<(), Error> {
        // (#99) Record the chunked-emit call. Server-side end-to-end
        // coverage lives in
        // `nativelink-service/tests/blobs_available_chunked_e2e_test.rs`,
        // which composes the same chunker through the real
        // `WorkerApiServer` + accumulator. This worker-side mock is
        // wired so future tests on the worker-emit path (drive the
        // `should_chunk` threshold, assert chunks-emitted-in-order)
        // can use the `expect_chunked_message` helper above.
        self.tx_call
            .send(WorkerClientApiCalls::ChunkedMessage(request))
            .expect("Could not send ChunkedMessage to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            WorkerClientApiReturns::ChunkedMessage(result) => result,
            resp => panic!("chunked_message expected ChunkedMessage response, received {resp:?}"),
        }
    }
}

pub(crate) fn setup_grpc_stream() -> (
    mpsc::Sender<Frame<Bytes>>,
    Response<Streaming<UpdateForWorker>>,
) {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<UpdateForWorker, UpdateForWorker>::default();
    let stream =
        Streaming::new_request(codec.decoder(), body, Some(CompressionEncoding::Gzip), None);
    (tx, Response::new(stream))
}

pub(crate) async fn setup_local_worker_with_config(
    local_worker_config: LocalWorkerConfig,
) -> TestContext {
    let mock_worker_api_client = MockWorkerApiClient::new();
    let mock_worker_api_client_clone = mock_worker_api_client.clone();
    let actions_manager = Arc::new(MockRunningActionsManager::new());
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(local_worker_config),
        actions_manager.clone(),
        Box::new(move || {
            let mock_worker_api_client = mock_worker_api_client_clone.clone();
            Box::pin(async move { Ok(mock_worker_api_client) })
        }),
        Box::new(move |_| Box::pin(async move { /* No sleep */ })),
        None, // No periodic BlobsAvailable in tests
        Vec::new(), // No CAS server guards in tests
        None, // No CAS shutdown signal in tests
    );
    let (shutdown_tx_test, _) = broadcast::channel::<ShutdownGuard>(BROADCAST_CAPACITY);

    let drop_guard = spawn!("local_worker_spawn", async move {
        worker.run(shutdown_tx_test.subscribe()).await
    });

    let (tx_stream, streaming_response) = setup_grpc_stream();
    TestContext {
        client: mock_worker_api_client,
        actions_manager,

        maybe_streaming_response: Some(streaming_response),
        maybe_tx_stream: Some(tx_stream),

        _drop_guard: drop_guard,
    }
}

pub(crate) async fn setup_local_worker(
    platform_properties: HashMap<String, WorkerProperty>,
) -> TestContext {
    const ARBITRARY_LARGE_TIMEOUT: f32 = 10000.;
    let local_worker_config = LocalWorkerConfig {
        platform_properties,
        worker_api_endpoint: EndpointConfig {
            timeout: Some(ARBITRARY_LARGE_TIMEOUT),
            ..Default::default()
        },
        ..Default::default()
    };
    setup_local_worker_with_config(local_worker_config).await
}

/// (#97) Same as [`setup_local_worker_with_config`] but plumbs through
/// a constructed [`nativelink_worker::local_worker::BlobsAvailableState`]
/// so the worker's `Update::ChunkedMessage(BlobsInStableStorageChunk)`
/// arm can fire (the arm warns + drops the chunk when the state is
/// `None`). Used by `bis_chunked_dispatch_arm_round_trips_ack`.
#[allow(dead_code, reason = "exercised only by BIS dispatch tests")]
pub(crate) async fn setup_local_worker_with_blobs_state(
    blobs_available_state: nativelink_worker::local_worker::BlobsAvailableState,
) -> TestContext {
    use nativelink_config::cas_server::LocalWorkerConfig;
    const ARBITRARY_LARGE_TIMEOUT: f32 = 10000.;
    let local_worker_config = LocalWorkerConfig {
        worker_api_endpoint: EndpointConfig {
            timeout: Some(ARBITRARY_LARGE_TIMEOUT),
            ..Default::default()
        },
        ..Default::default()
    };
    let mock_worker_api_client = MockWorkerApiClient::new();
    let mock_worker_api_client_clone = mock_worker_api_client.clone();
    let actions_manager = Arc::new(MockRunningActionsManager::new());
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(local_worker_config),
        actions_manager.clone(),
        Box::new(move || {
            let mock_worker_api_client = mock_worker_api_client_clone.clone();
            Box::pin(async move { Ok(mock_worker_api_client) })
        }),
        Box::new(move |_| Box::pin(async move { /* No sleep */ })),
        Some(blobs_available_state),
        Vec::new(),
        None,
    );
    let (shutdown_tx_test, _) = broadcast::channel::<ShutdownGuard>(BROADCAST_CAPACITY);

    let drop_guard = spawn!("local_worker_spawn_bis", async move {
        worker.run(shutdown_tx_test.subscribe()).await
    });

    let (tx_stream, streaming_response) = setup_grpc_stream();
    TestContext {
        client: mock_worker_api_client,
        actions_manager,
        maybe_streaming_response: Some(streaming_response),
        maybe_tx_stream: Some(tx_stream),
        _drop_guard: drop_guard,
    }
}

pub(crate) struct TestContext {
    pub client: MockWorkerApiClient,
    pub actions_manager: Arc<MockRunningActionsManager>,

    pub maybe_streaming_response: Option<Response<Streaming<UpdateForWorker>>>,
    pub maybe_tx_stream: Option<mpsc::Sender<Frame<Bytes>>>,

    _drop_guard: JoinHandleDropGuard<Result<(), Error>>,
}
