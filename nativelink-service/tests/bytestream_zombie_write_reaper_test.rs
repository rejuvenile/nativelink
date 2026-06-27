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

//! **Zombie write reaper regression tests (bytestream Write RPC).**
//!
//! ## What is a zombie write?
//!
//! After a server restart that drains a large backfill under an
//! ENHANCE_YOUR_CALM h2 GoAway storm, workers reconnect on new connections
//! but some old h2 streams remain open at the TCP/connection level (the
//! connection is keepalive-ACKing) while the worker has silently abandoned
//! the stream without sending RST_STREAM or END_STREAM. `process_client_stream`
//! parks at `stream.next().await`; the buf_channel sender is alive inside the
//! `ActiveStreamGuard`; `store_update_fut` parks at `rx.recv().await`. Both
//! arms of `try_join!` block indefinitely; `bytestream_write` never returns;
//! `_stall_guard` never drops; StallGuard fires every 30s at ~0.8/s.
//!
//! ## Root cause
//!
//! No existing mechanism distinguishes zombie streams (alive connection,
//! abandoned stream) from legitimate slow-but-progressing producers:
//!
//! - Keepalive (30s/60s PING) is connection-level — fires when the TCP
//!   peer stops ACKing; zombie connections DO ack PINGs.
//! - The idle-stream sweeper only covers entries where `maybe_idle.is_some()`;
//!   active writes have `maybe_idle = None` and are completely invisible.
//! - `StallGuard.progress_handle` suppresses dumps but does NOT reap.
//!
//! ## Fix
//!
//! The sweeper gains a "Pass 3" that tracks `bytes_received` for active
//! (non-idle) writes across sweeps. If `bytes_received` has not increased
//! for `>= 2 × idle_stream_timeout`, the sweeper fires a
//! `tokio::sync::oneshot` cancel signal stored in the map entry. `inner_write`
//! wraps its `try_join!` with `tokio::select!` racing the cancel receiver;
//! when fired the write returns `Err(Code::Aborted, "zombie write reaped ...")`.
//!
//! ## Threshold safety argument
//!
//! `2 × idle_stream_timeout` (≥ 2 × 60s = 120s in production) exceeds the
//! keepalive dead-peer detection window (30s ping + 60s timeout + 5s hyper
//! stall-grace = 95s). Any write killed by the sweeper CANNOT be on a live
//! connection with a legitimately slow producer — if the connection were alive
//! and the producer were alive, keepalive would have killed it within 95s.
//! Slow-but-alive producers sending any bytes within the window are unaffected
//! (the sweeper resets on any progress).
//!
//! ## Asymmetric contract coverage (CLAUDE.md)
//!
//! Two directions:
//! - Under-action: zombie write IS reaped — `zombie_write_is_reaped_after_threshold`
//! - Over-action: slow-but-alive write is NOT killed — `slow_alive_producer_not_killed`
//!
//! ## Mutation step (CLAUDE.md TDD #5)
//!
//! Commenting out the `tokio::select!` cancel arm (reverting to plain
//! `try_join!().await`) makes `zombie_write_is_reaped_after_threshold`
//! time out at its outer guard with the bespoke message:
//!
//!   "zombie write must be reaped — StallGuard and in-flight slot must not
//!    leak when producer goes silent on a live h2 connection"
//!
//! The `slow_alive_producer_not_killed` test must still pass after the mutation
//! (because the cancel arm is absent, slow producers are never killed — the
//! test asserts the upload SUCCEEDS, which it does in the absence of a cancel).

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
const HASH2: &str = "0123456789abcdef000000000000000000000000000000000123456789abcde0";

/// Short timeout used for both tests. `idle_stream_timeout = IDLE_TIMEOUT`.
/// `sweep_interval = IDLE_TIMEOUT / 2`. Zombie threshold = `2 * IDLE_TIMEOUT`.
const IDLE_TIMEOUT_SECS: usize = 2;
const IDLE_TIMEOUT: Duration = Duration::from_secs(IDLE_TIMEOUT_SECS as u64);

/// How far to advance virtual time to trigger zombie reap.
/// Must exceed `2 * IDLE_TIMEOUT + sweep_interval` = `2 * 2 + 1 = 5s`.
const ZOMBIE_ADVANCE: Duration = Duration::from_secs(6);

/// How long the slow-but-alive producer delays between chunks (< IDLE_TIMEOUT).
/// Each chunk advances the `bytes_received` counter, preventing zombie detection.
const ALIVE_CHUNK_DELAY: Duration = Duration::from_millis(500);

/// Chunk parameters for the slow-alive test.
const CHUNK_SIZE: usize = 16;
const CHUNK_COUNT: usize = 4;
const TOTAL_SIZE: usize = CHUNK_SIZE * CHUNK_COUNT;

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
            // Short idle_stream_timeout so the zombie threshold (2 × this) is
            // 4s of virtual time, keeping the test fast.
            persist_stream_on_disconnect_timeout: IDLE_TIMEOUT_SECS,
            max_bytes_per_stream: 1024,
            ..Default::default()
        },
    }];
    ByteStreamServer::new(&config, store_manager, None)
}

fn make_resource_name(hash: &str, data_len: usize, uuid: &str) -> String {
    format!("{INSTANCE_NAME}/uploads/{uuid}/blobs/{hash}/{data_len}")
}

/// Send a single non-terminal WriteRequest to get past `WriteRequestStreamWrapper::from`.
///
/// `WriteRequestStreamWrapper::from` reads the FIRST message from the stream before
/// returning. This helper sends exactly one chunk (write_offset=0, finish_write=false)
/// so the wrapper's `from()` call can complete, leaving the server blocked waiting
/// for the SECOND message.
async fn send_first_chunk(
    frame_tx: &tokio::sync::mpsc::Sender<Frame<Bytes>>,
    resource_name: String,
    chunk: Bytes,
) {
    let write_request = WriteRequest {
        resource_name,
        write_offset: 0,
        finish_write: false,
        data: chunk,
    };
    let frame = Frame::data(
        encode_stream_proto(&write_request).expect("encode first chunk"),
    );
    frame_tx
        .send(frame)
        .await
        .expect("first chunk send must succeed — writer must be alive");
}

/// Send a final WriteRequest with `finish_write=true`.
async fn send_final_chunk(
    frame_tx: &tokio::sync::mpsc::Sender<Frame<Bytes>>,
    write_offset: usize,
    chunk: Bytes,
) {
    let write_request = WriteRequest {
        resource_name: String::new(),
        write_offset: write_offset as i64,
        finish_write: true,
        data: chunk,
    };
    let frame = Frame::data(
        encode_stream_proto(&write_request).expect("encode final chunk"),
    );
    frame_tx
        .send(frame)
        .await
        .expect("final chunk send must succeed");
}

/// Send an intermediate WriteRequest (not the first, not the last).
async fn send_middle_chunk(
    frame_tx: &tokio::sync::mpsc::Sender<Frame<Bytes>>,
    write_offset: usize,
    chunk: Bytes,
) {
    let write_request = WriteRequest {
        resource_name: String::new(),
        write_offset: write_offset as i64,
        finish_write: false,
        data: chunk,
    };
    let frame = Frame::data(
        encode_stream_proto(&write_request).expect("encode middle chunk"),
    );
    frame_tx
        .send(frame)
        .await
        .expect("middle chunk send must succeed");
}

/// **Under-action test: zombie write IS reaped after `2 × idle_stream_timeout`.**
///
/// A producer sends the first chunk (needed to get past `WriteRequestStreamWrapper::from`),
/// then holds its sender alive but sends no more data. After virtual time advances
/// past `2 × idle_stream_timeout`, the sweeper fires the zombie cancel and
/// `inner_write` returns `Err(Code::Aborted, "zombie write reaped ...")`.
///
/// **Seams traversed:**
/// 1. `ByteStreamServer::write` (tonic gRPC entry point)
/// 2. `WriteRequestStreamWrapper::from` (reads first message)
/// 3. `bytestream_write` (StallGuard, cancel rx wiring)
/// 4. `inner_write` → `process_client_stream` (parks at `stream.next().await`)
/// 5. `bytestream_idle_stream_sweeper` (Pass 3 zombie detection → fires cancel)
/// 6. `tokio::select!` cancel arm → `Err(Code::Aborted, "zombie write reaped ...")`
///
/// **Mutation step:** comment out the `select!` cancel arm in `inner_write`
/// (revert to plain `try_join!().await`). This test then times out at the
/// outer `tokio::time::timeout(Duration::from_secs(10))` guard and panics with:
///   "zombie write must be reaped — StallGuard and in-flight slot must not
///    leak when producer goes silent on a live h2 connection"
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn zombie_write_is_reaped_after_threshold() {
    let store_manager = make_store_manager()
        .await
        .expect("store_manager construction must not fail");
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref()).expect("bs_server new"),
    );

    let (frame_tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream: Streaming<WriteRequest> =
        Streaming::new_request(codec.decoder(), body, None, None);

    // Spawn the writer side: this is the bytestream Write RPC handler.
    // It must NOT block indefinitely when the producer is a zombie.
    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("zombie_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // Send the first chunk to get past `WriteRequestStreamWrapper::from`.
    // After this, the server is blocked waiting for the second chunk.
    // We then hold `frame_tx` alive but send nothing — zombie simulation.
    let resource_name = make_resource_name(HASH1, 64, "aaaaaaaa-0000-0000-0000-000000000001");
    let first_chunk = Bytes::from(vec![0x42u8; 8]);
    send_first_chunk(&frame_tx, resource_name, first_chunk).await;

    // Producer side: hold sender alive (zombie), advance virtual time past
    // the zombie reap threshold. The sweeper fires the cancel at
    // `2 × idle_stream_timeout = 4s`. With `ZOMBIE_ADVANCE = 6s`, the
    // cancel fires on the sweep at ~3s (i.e., after 3 sweep intervals of 1s
    // each once the sweeper first sees the write as stale) and the writer
    // returns by ~4s of virtual time.
    //
    // NOTE: `frame_tx` is moved into this block to keep the sender alive
    // during the sleep, simulating the zombie connection scenario.
    let zombie_producer_handle: JoinHandleDropGuard<()> =
        spawn!("zombie_producer", async move {
            // Zombie: sender alive, sending nothing.
            // The server is blocked at stream.next().await.
            tokio::time::sleep(ZOMBIE_ADVANCE).await;
            // Sender dropped here — but the writer should have returned Err
            // already due to the sweeper-fired cancel.
            drop(frame_tx);
        });

    zombie_producer_handle
        .await
        .expect("zombie producer task must not panic");

    // The writer must have returned Err(Code::Aborted) with the zombie-reap
    // message. The outer `tokio::time::timeout` guards against an infinite
    // hang if the fix is absent or the cancel never fires.
    //
    // NOTE: With `start_paused = true`, the virtual time advances inside the
    // zombie_producer_handle sleep above. By the time we await writer_handle,
    // the sweeper has already run (virtual time is in the future), so the
    // writer_handle should resolve immediately.
    let writer_result = tokio::time::timeout(
        Duration::from_secs(10),
        writer_handle,
    )
    .await
    .expect(
        "zombie write must be reaped — StallGuard and in-flight slot must not \
         leak when producer goes silent on a live h2 connection",
    )
    .expect("writer task must not panic");

    let status = writer_result.expect_err(
        "zombie write must return Err — a silent producer on a live connection \
         must be reaped by the sweeper-based zombie detector",
    );
    let error_message = status.message();
    assert!(
        error_message.contains("zombie write reaped"),
        "zombie reap error must contain 'zombie write reaped'; got: {error_message:?}",
    );
}

/// **Over-action test: slow-but-alive producer is NOT killed.**
///
/// A producer sends chunks every `ALIVE_CHUNK_DELAY = 500ms` of virtual time —
/// well within the `IDLE_TIMEOUT = 2s` threshold. The sweeper sees `bytes_received`
/// advancing on each sweep and MUST NOT fire the zombie cancel. The upload
/// completes successfully.
///
/// This guards the asymmetric over-action contract: the zombie detector must
/// not misidentify a legitimately slow-but-alive producer as a zombie.
///
/// **Mutation step:** if the zombie threshold were set to 0 (or to
/// `< ALIVE_CHUNK_DELAY`), this test would fail because the cancel fires before
/// each new chunk arrives. The test uses `ALIVE_CHUNK_DELAY` deliberately to
/// exceed any reasonably fine-grained "timeout per chunk" while staying within
/// the coarser zombie-sweep window.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn slow_alive_producer_not_killed() {
    let store_manager = make_store_manager()
        .await
        .expect("store_manager construction must not fail");
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref()).expect("bs_server new"),
    );
    let store = store_manager.get_store("main_cas").expect("main_cas store");

    let (frame_tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream: Streaming<WriteRequest> =
        Streaming::new_request(codec.decoder(), body, None, None);

    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("alive_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // Spawn the producer. It sends CHUNK_COUNT chunks every ALIVE_CHUNK_DELAY
    // apart (much less than the zombie threshold = 4s). Each chunk advances
    // `bytes_received`, resetting zombie detection in the sweeper.
    let producer_handle: JoinHandleDropGuard<()> = spawn!("alive_producer", async move {
        let resource_name = make_resource_name(HASH2, TOTAL_SIZE, "bbbbbbbb-0000-0000-0000-000000000002");
        let chunk_data: Bytes = Bytes::from(vec![0xCDu8; CHUNK_SIZE]);

        // First chunk: gets past WriteRequestStreamWrapper::from.
        // Delay first so the server is known to be waiting before we send.
        tokio::time::sleep(ALIVE_CHUNK_DELAY).await;
        send_first_chunk(&frame_tx, resource_name, chunk_data.clone()).await;

        // Middle chunks: each one resets zombie detection.
        for i in 1..(CHUNK_COUNT - 1) {
            tokio::time::sleep(ALIVE_CHUNK_DELAY).await;
            send_middle_chunk(&frame_tx, i * CHUNK_SIZE, chunk_data.clone()).await;
        }

        // Final chunk: completes the upload.
        tokio::time::sleep(ALIVE_CHUNK_DELAY).await;
        send_final_chunk(&frame_tx, (CHUNK_COUNT - 1) * CHUNK_SIZE, chunk_data.clone()).await;
        drop(frame_tx);
    });

    producer_handle
        .await
        .expect("alive producer task must not panic");

    // Writer must SUCCEED — slow-but-alive producer must NOT be killed.
    let writer_result = tokio::time::timeout(Duration::from_secs(10), writer_handle)
        .await
        .expect(
            "alive producer writer must not deadlock — \
             slow-but-alive producer must not be killed by zombie detector",
        )
        .expect("writer task must not panic");

    let response = writer_result.expect(
        "slow-but-alive producer must NOT be killed by the zombie detector — \
         bytes_received advances on each chunk, resetting zombie detection",
    );

    let committed_size = response.into_inner().committed_size;
    assert_eq!(
        usize::try_from(committed_size).expect("committed_size fits usize"),
        TOTAL_SIZE,
        "committed_size must equal full upload size for slow-but-alive producer",
    );

    // Blob must be in the store.
    let digest = DigestInfo::try_new(HASH2, TOTAL_SIZE).expect("digest");
    assert!(
        store
            .has(digest)
            .await
            .expect("store.has must not error")
            .is_some(),
        "blob must be present in MemoryStore after slow-but-alive upload",
    );
}
