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

//! Integration tests for `WorkerApiClient` decoder limits.
//!
//! These tests guard the fix for Task #71: the worker constructed
//! `WorkerApiClient` without an explicit `max_decoding_message_size`,
//! falling back to tonic's 4 MiB default. Today's `MAX_PEER_HINTS = 16384`
//! and tomorrow's `BlobsInStableStorage` (unbounded `repeated Digest`)
//! can both blow past 4 MiB, silently breaking the connect_worker stream.
//!
//! Each test stands up a real in-process tonic server that streams a
//! single oversized `UpdateForWorker` and verifies the client behavior.
//! All tests run under a 5 s outer timeout so a regression manifests as
//! a fast failure, never a hang.

use core::time::Duration;
use std::pin::Pin;

use futures::stream::{Stream, StreamExt};
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_worker::Update;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_server::{
    WorkerApi, WorkerApiServer,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BlobsInStableStorage, ConnectWorkerRequest, UpdateForScheduler, UpdateForWorker,
};
use nativelink_worker::local_worker::WORKER_API_MAX_DECODING_MESSAGE_SIZE;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Outer timeout. If the client hangs (rather than returning an error
/// when the message is too large), the test fails within this budget.
/// Generous so contention from other parallel tests doesn't flake the
/// signal — the real assertion is "did the decoder return a clean
/// error?", not "how fast?". A regression manifests as outer-timeout
/// expiry, not as a wrong code.
const OUTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Server impl that streams a single `UpdateForWorker` whose
/// `BlobsInStableStorage.digests` payload reaches `payload_bytes`. Uses
/// `BlobsInStableStorage` rather than `StartExecute` because its
/// payload is a flat repeated-Digest list, easy to size deterministically.
struct OversizedUpdateServer {
    payload_bytes: usize,
}

#[tonic::async_trait]
impl WorkerApi for OversizedUpdateServer {
    type ConnectWorkerStream =
        Pin<Box<dyn Stream<Item = Result<UpdateForWorker, Status>> + Send + 'static>>;

    async fn connect_worker(
        &self,
        _request: Request<tonic::Streaming<UpdateForScheduler>>,
    ) -> Result<Response<Self::ConnectWorkerStream>, Status> {
        // Build a `BlobsInStableStorage` whose encoded length is approximately
        // `payload_bytes`. Each Digest with a 64-char SHA256 hex hash + a
        // small size_bytes encodes to ~70-72 bytes wire. We pad up by
        // adjusting the count.
        let bytes_per_digest = 72_usize;
        let count = self.payload_bytes.div_ceil(bytes_per_digest);
        let digests: Vec<Digest> = (0..count)
            .map(|i| Digest {
                hash: format!("{:064x}", i),
                size_bytes: i as i64,
            })
            .collect();
        let update = UpdateForWorker {
            update: Some(Update::BlobsInStableStorage(BlobsInStableStorage {
                digests,
            })),
        };

        // Single-item stream then close. The worker reads the first item
        // and we observe whether its decoder accepts or rejects.
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            drop(tx.send(Ok(update)).await);
        });
        let stream: Self::ConnectWorkerStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}

/// Spawn an in-process tonic server bound to an ephemeral port.
/// Returns `(channel, abort_handle)`.
async fn spawn_server(payload_bytes: usize) -> (Channel, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let svc = WorkerApiServer::new(OversizedUpdateServer { payload_bytes })
        // Server-side encoding limit must be raised — server defaults to
        // `usize::MAX` for encoding, but the codec's send buffer needs to
        // hold the message. Default `usize::MAX` is fine; explicit for clarity.
        .max_encoding_message_size(usize::MAX);

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
        .expect("connect to in-process server");
    (channel, handle)
}

/// Send a placeholder `ConnectWorkerRequest` so the bidi stream is
/// initiated. The server ignores its input — we only care about the
/// response stream.
fn make_request_stream() -> ReceiverStream<UpdateForScheduler> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        drop(
            tx.send(UpdateForScheduler {
                update: Some(
                    nativelink_proto::com::github::trace_machina::nativelink::remote_execution::update_for_scheduler::Update::ConnectWorkerRequest(
                        ConnectWorkerRequest::default(),
                    ),
                ),
            })
            .await,
        );
        // Hold the channel open so the server-side stream doesn't EOF
        // before the response is delivered.
        std::future::pending::<()>().await;
    });
    ReceiverStream::new(rx)
}

/// At the configured limit (64 MiB), a ~32 MiB `UpdateForWorker` decodes
/// successfully. Guards the lower bound of the fix: the new constant
/// must be large enough to cover today's worst-case realistic payload.
#[tokio::test(flavor = "multi_thread")]
async fn start_execute_at_decoder_limit_test() {
    let payload_bytes = 32 * 1024 * 1024;
    let (channel, server_handle) = spawn_server(payload_bytes).await;

    let mut client = WorkerApiClient::new(channel)
        .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE);

    let result = tokio::time::timeout(OUTER_TIMEOUT, async {
        let response = client
            .connect_worker(make_request_stream())
            .await
            .expect("connect_worker should succeed");
        let mut stream = response.into_inner();
        stream.next().await
    })
    .await;

    server_handle.abort();

    let item = result
        .expect("outer timeout — client never received the oversized message")
        .expect("stream ended without yielding the message");
    let update = item.expect("expected Ok(UpdateForWorker) at 32 MiB under 64 MiB limit");
    match update.update {
        Some(Update::BlobsInStableStorage(b)) => {
            assert!(
                !b.digests.is_empty(),
                "decoded BlobsInStableStorage was empty"
            );
        }
        other => panic!("unexpected update payload: {other:?}"),
    }
}

/// Above the configured limit (64 MiB), a ~96 MiB `UpdateForWorker` is
/// rejected by the client decoder with a clean error (not a hang).
/// Guards the upper bound: the limit is actually enforced on this client.
#[tokio::test(flavor = "multi_thread")]
async fn start_execute_over_decoder_limit_test() {
    let payload_bytes = 96 * 1024 * 1024;
    let (channel, server_handle) = spawn_server(payload_bytes).await;

    let mut client = WorkerApiClient::new(channel)
        .max_decoding_message_size(WORKER_API_MAX_DECODING_MESSAGE_SIZE);

    let result = tokio::time::timeout(OUTER_TIMEOUT, async {
        let response = client
            .connect_worker(make_request_stream())
            .await
            .expect("connect_worker should succeed (transport handshake is small)");
        let mut stream = response.into_inner();
        stream.next().await
    })
    .await;

    server_handle.abort();

    let item = result.expect("outer timeout — client hung instead of returning a clean error");
    let item = item.expect("stream ended without yielding the message");
    let err = item.expect_err(
        "expected decoder error for oversized message, got Ok — is the limit being applied?",
    );
    // Tonic returns OutOfRange for "message length too large" in the
    // codec; assert we get a structured error rather than a hang.
    let code = err.code();
    assert!(
        matches!(
            code,
            tonic::Code::OutOfRange
                | tonic::Code::ResourceExhausted
                | tonic::Code::Internal
                | tonic::Code::Unknown
        ),
        "unexpected error code {code:?} for oversized payload: {err}",
    );
}

/// Sanity check on the constant itself. If a future change accidentally
/// drops it below the tonic default (4 MiB) or to zero, this fails
/// loudly with a meaningful message.
#[test]
fn worker_api_decode_limit_constant_is_above_tonic_default() {
    const TONIC_DEFAULT_DECODE: usize = 4 * 1024 * 1024;
    assert!(
        WORKER_API_MAX_DECODING_MESSAGE_SIZE > TONIC_DEFAULT_DECODE,
        "WORKER_API_MAX_DECODING_MESSAGE_SIZE ({}) must exceed tonic's default ({}); \
         leaving it at the default reintroduces Task #71",
        WORKER_API_MAX_DECODING_MESSAGE_SIZE,
        TONIC_DEFAULT_DECODE,
    );
    // Also assert the agreed-upon value to catch silent shrinkage.
    assert_eq!(
        WORKER_API_MAX_DECODING_MESSAGE_SIZE,
        64 * 1024 * 1024,
        "WORKER_API_MAX_DECODING_MESSAGE_SIZE changed from 64 MiB; ensure listener / \
         peer-hints / BlobsInStableStorage worst case still fits"
    );
}
