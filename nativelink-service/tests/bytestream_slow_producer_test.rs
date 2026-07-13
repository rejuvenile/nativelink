// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! **Slow-producer regression coverage for the bytestream Write RPC.**
//!
//! ## Why this test exists (CLAUDE.md "Asymmetric contract coverage")
//!
//! A recent change removed wall-clock kills on the bytestream Write
//! RPC's primary writer + coalesced waiters:
//!
//! 1. `WRITE_TIMEOUT` (300s wall-clock around `inner_write` /
//!    `inner_write_oneshot`) — REMOVED.
//! 2. `COALESCE_TIMEOUT` (300s wall-clock around the coalesced-waiter
//!    `rx.changed()` loop) — REMOVED.
//!
//! The over-action contract — "server does NOT kill a slow-but-
//! progressing client" — has no existing coverage. This file fills
//! that gap. Per CLAUDE.md "Falsify before fixing", no app-layer
//! per-recv timer was added either; no-progress detection is fully
//! layered on transport keepalive (h2/QUIC/TCP) plus sender-drop on
//! connection close, mirroring the design from `fa6cdf43`
//! (2026-04-23: drop 300s outer write/coalesce deadlines).
//!
//! ## Production composition
//!
//! `ByteStreamServer` wired with a real `MemoryStore` via
//! `StoreManager` (the same code path `nativelink.rs` uses at
//! startup). The `ByteStream::write` entry point goes through
//! `inner_write` (multi-chunk path) which calls `tx.send(data).await`
//! into the buf_channel that `MemoryStore::update` consumes. Same
//! seams as production, minus the slow-tier wrappers (which are
//! irrelevant for the recv-loop timing contract this test exercises).
//!
//! ## Driving the slow producer
//!
//! `#[nativelink_test(flavor = "current_thread", start_paused = true)]`
//! gives us paused virtual time. The producer pushes one chunk, then
//! `tokio::time::sleep`s a long virtual interval, repeated for several
//! chunks — total virtual elapsed exceeds 300s. With the wall-clock
//! kill removed, the server processes every chunk and the upload
//! succeeds. With the OLD `WRITE_TIMEOUT = 300s` restored, paused-time
//! advancement trips at 300s of total wall-clock and the writer Errs
//! mid-upload.
//!
//! Notes on timing:
//! - 4 chunks × 100s sleep each = 400s total — comfortably exceeds the
//!   OLD 300s wall-clock kill, so a regression that re-introduces it
//!   will red-fail the test.
//!
//! ## Mutation step (CLAUDE.md TDD #5)
//!
//! Restoring `tokio::time::timeout(Duration::from_secs(300), write_fut)`
//! around the `write_fut.await` site (the body that the timeout
//! removal diff removed) makes this test red-fail with the bespoke
//! message:
//!
//!   "slow-but-progressing producer must NOT be killed by wall-clock
//!    timeout"
//!
//! If the test still passes after the mutation, the test does not
//! guard the behavior and is theatre.

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Frame;
use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteRequest;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::store_trait::StoreLike;
use nativelink_util::{spawn, task::JoinHandleDropGuard};
use pretty_assertions::assert_eq;
use tonic::Request;
use tonic::Streaming;
use tonic::codec::Codec;
use tonic_prost::ProstCodec;

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

/// Per-chunk virtual delay. With no app-layer recv timer in production,
/// the absolute value here is bounded only by total test runtime
/// (400s of virtual time) and the need to comfortably exceed the OLD
/// 300s wall-clock kills.
const CHUNK_DELAY: Duration = Duration::from_secs(100);

/// Number of chunks. With 4 chunks × 100s = 400s total, this comfortably
/// exceeds the OLD `WRITE_TIMEOUT` (300s) and OLD `COALESCE_TIMEOUT`
/// (300s), so a regression that re-introduces those wall-clock kills
/// red-fails this test.
const CHUNK_COUNT: usize = 4;

/// Bytes per chunk.
const CHUNK_SIZE: usize = 16;

/// Total upload size = `CHUNK_SIZE * CHUNK_COUNT`.
const TOTAL_SIZE: usize = CHUNK_SIZE * CHUNK_COUNT;

/// Production-shaped store wiring. Returns a `StoreManager` containing
/// `main_cas` = `MemoryStore`. Same store_factory entry that
/// `nativelink.rs` uses at startup.
async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_bytestream_server(store_manager: &StoreManager) -> Result<ByteStreamServer, Error> {
    let config = vec![WithInstanceName {
        instance_name: INSTANCE_NAME.to_string(),
        config: ByteStreamConfig {
            cas_store: "main_cas".to_string(),
            persist_stream_on_disconnect_timeout_s: 0,
            max_bytes_per_stream: 1024,
            ..Default::default()
        },
    }];
    ByteStreamServer::new(&config, store_manager, None)
}

fn make_resource_name(data_len: usize) -> String {
    format!(
        "{INSTANCE_NAME}/uploads/{}/blobs/{HASH1}/{data_len}",
        // randomly generated UUID per upload
        "5e0c0e0a-1389-4ab5-b188-4a59f22ceb4b",
    )
}

/// **Production-composition test for the slow-but-progressing producer
/// over-action contract (CLAUDE.md "Asymmetric contract coverage").**
///
/// A producer that sends one chunk every 100s of virtual time across 4
/// chunks (400s total, exceeding both removed 300s wall-clocks) MUST
/// succeed. With no app-layer recv timer in the server, the only
/// no-progress detection is transport keepalive plus sender-drop on
/// connection close — neither of which fires for a healthy paused-time
/// producer.
///
/// **Drive path:** real `ByteStreamServer` → `MemoryStore` (same as
/// production for the in-memory tier). The producer task slowly pushes
/// chunks via the `ChannelBody` plumbing that `tonic::Streaming`
/// consumes. With `start_paused = true`, virtual time advances only
/// when explicitly told via the producer's `tokio::time::sleep` calls.
///
/// **Mutation step (CLAUDE.md TDD #5):** restore
/// `tokio::time::timeout(Duration::from_secs(300), write_fut).await`
/// around the `write_fut.await` site in
/// `bytestream_server.rs::write` (the 300s `WRITE_TIMEOUT` that the
/// removal diff dropped). With the mutation in place, this test
/// red-fails with the bespoke message below at the `.expect(...)`
/// site, because `tokio::time::timeout` fires inside the virtual-time
/// advancement and the writer task returns `Err(DeadlineExceeded)`.
///
/// Outer wall-clock guard: the test relies on `start_paused = true`
/// for ~400s of virtual time. The cargo `timeout 60` invocation is the
/// real-time backstop for any regression that causes a non-virtual
/// hang.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn slow_but_progressing_producer_succeeds() {
    let store_manager = make_store_manager()
        .await
        .expect("store_manager construction must not fail");
    let bs_server =
        Arc::new(make_bytestream_server(store_manager.as_ref()).expect("bs_server new"));
    let store = store_manager.get_store("main_cas").expect("main_cas store");

    // Build the request stream from a `ChannelBody` so the producer task
    // can pace `Frame` deliveries through virtual-time sleeps.
    let (frame_tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream: Streaming<WriteRequest> =
        Streaming::new_request(codec.decoder(), body, None, None);

    // Spawn the writer side: the bytestream server consumes `stream`.
    // It must NOT return Err inside the 400s virtual window. Returning
    // before the producer is done is the bug.
    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("slow_producer_writer", async move {
        bs_server_for_writer
            .write(Request::new(stream))
            .await
    });

    // Producer side: emit CHUNK_COUNT chunks, sleeping CHUNK_DELAY of
    // virtual time between each. `start_paused = true` means each
    // `tokio::time::sleep(CHUNK_DELAY).await` advances virtual time
    // only when the runtime polls the timer; the writer task makes
    // forward progress on each chunk arrival.
    //
    // **Failure-mode hygiene:** if the writer returns Err mid-upload
    // (the regression we're guarding against), the `frame_tx.send()`
    // here will fail because the rx side closed. We MUST NOT panic in
    // that case — instead exit cleanly so the writer's `.expect(...)`
    // below fires the BESPOKE MESSAGE, not a noisy producer panic. The
    // producer panicking first masks the actual contract violation.
    let producer_handle: JoinHandleDropGuard<()> = spawn!("slow_producer_producer", async move {
        let resource_name = make_resource_name(TOTAL_SIZE);
        let chunk_data: Bytes = Bytes::from(vec![0xABu8; CHUNK_SIZE]);
        for i in 0..CHUNK_COUNT {
            // Delay BEFORE each chunk so the test exercises the recv
            // wait — this is what the per-recv timer guards. The
            // first iteration's pre-chunk delay is also load-bearing:
            // it forces the writer to wait the first 100s before any
            // data arrives, mimicking the production scenario where
            // a Bazel client is slow even for the very first frame.
            tokio::time::sleep(CHUNK_DELAY).await;
            let is_first = i == 0;
            let is_last = i + 1 == CHUNK_COUNT;
            let write_request = WriteRequest {
                resource_name: if is_first {
                    resource_name.clone()
                } else {
                    String::new()
                },
                write_offset: (i * CHUNK_SIZE) as i64,
                finish_write: is_last,
                data: chunk_data.clone(),
            };
            let frame = Frame::data(
                encode_stream_proto(&write_request)
                    .expect("encode_stream_proto must succeed (encoding a fixed proto)"),
            );
            // If `frame_tx.send` returns Err, the rx (writer side) is
            // closed — the writer must have returned Err already. Exit
            // the producer cleanly so the writer's `.expect(...)`
            // surfaces the BESPOKE message, not a producer panic.
            if frame_tx.send(frame).await.is_err() {
                return;
            }
        }
        // Drop frame_tx → ChannelBody EOF → end-of-stream observed by
        // the server.
        drop(frame_tx);
    });

    // Await both. Producer must finish (cleanly OR after early exit on
    // writer Err — see comment above). Writer MUST succeed; if the
    // 300s WRITE_TIMEOUT regression is present, the writer Errs first
    // and the bespoke `.expect(...)` below fires.
    producer_handle
        .await
        .expect("producer task must not panic (it should exit cleanly even when writer Errs)");

    let writer_result = writer_handle
        .await
        .expect(
            "writer task must not panic — slow-but-progressing producer \
             must NOT be killed by wall-clock timeout",
        )
        .expect(
            "slow-but-progressing producer must NOT be killed by wall-clock timeout",
        );

    let committed_size = writer_result.into_inner().committed_size;
    assert_eq!(
        usize::try_from(committed_size).expect("committed_size fits usize"),
        TOTAL_SIZE,
        "committed_size must equal the full upload size — \
         slow-but-progressing producer regression",
    );

    // Verify the blob is in the store with the right bytes.
    let digest = DigestInfo::try_new(HASH1, TOTAL_SIZE).expect("digest");
    assert!(
        store
            .has(digest)
            .await
            .expect("store.has must not error")
            .is_some(),
        "blob must be present in the MemoryStore after a successful slow-but-progressing upload"
    );
    let store_data = store
        .get_part_unchunked(digest, 0, None)
        .await
        .expect("get_part_unchunked must succeed");
    let expected: Vec<u8> = vec![0xABu8; TOTAL_SIZE];
    assert_eq!(
        store_data.as_ref(),
        expected.as_slice(),
        "stored bytes must match the slow-producer payload"
    );
}

