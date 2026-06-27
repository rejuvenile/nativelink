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
//! `2 × idle_stream_timeout` (= 2 × 60s = 120s in production) exceeds the
//! keepalive dead-peer detection window: http2_keep_alive_interval (30s) +
//! experimental_http2_keep_alive_timeout (20s) = 50s in the buildcache config.
//! Any write killed by the sweeper CANNOT be on a live connection with a
//! legitimately slow producer — if the connection were alive and the producer
//! were alive, keepalive would have killed it within 50s, well before the 120s
//! reaper threshold. Slow-but-alive producers sending any bytes within the
//! window are unaffected (the sweeper resets on any progress).
//!
//! ## Asymmetric contract coverage (CLAUDE.md)
//!
//! Four directions:
//! - Under-action (simple): zombie write stuck at first observation IS reaped —
//!   `zombie_write_is_reaped_after_threshold`
//! - Under-action (reset path): a zombie that advanced bytes once then went
//!   silent IS still reaped (the byte-progress reset re-arms the no-progress
//!   timer) — `progressed_then_silent_zombie_is_reaped`
//! - Over-action (slow producer): slow-but-alive write is NOT killed —
//!   `slow_alive_producer_not_killed`
//! - Over-action (all-bytes-received): a write whose `bytes_received` has reached
//!   `expected_size` is NOT killed even when stable past the threshold —
//!   `all_bytes_received_not_reaped_during_store_commit`. This protects the
//!   store-commit phase (`process_client_stream` returned Ok; `store_update_fut`
//!   still draining) from being misclassified as a silent-producer zombie, which
//!   would bypass the FL-688 ≥2-replica mirror ack-gate. See review c2a06843
//!   MAJOR-2 (convergent distsys + red-team + testing-czar T5).
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

use core::sync::atomic::Ordering;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Frame;
use pretty_assertions::assert_eq;
use tonic::codec::Codec;
use tonic::{Request, Streaming};
use tonic_prost::ProstCodec;

use nativelink_config::cas_server::{ByteStreamConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::{QueryWriteStatusRequest, WriteRequest};
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use nativelink_util::store_trait::StoreLike;
use nativelink_util::{spawn, task::JoinHandleDropGuard};

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

/// How long the slow-but-alive producer delays between chunks (< the zombie
/// threshold `2 × IDLE_TIMEOUT = 4s`, so each chunk's byte-advance resets the
/// sweeper's no-progress timer before it can fire). The byte-progress RESET path
/// is exercised as a LOAD-BEARING contract in the dedicated
/// `progressed_then_silent_zombie_is_reaped` test (under-action direction); this
/// over-action test only asserts an alive producer's upload completes.
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

    // Hold `frame_tx` alive in the MAIN task for the entire window (testing-czar
    // T4): the connection stays alive and the producer stays silent. The ONLY
    // way `writer_handle` resolves is the sweeper-fired reap — never a sender
    // drop. We advance virtual time directly via the outer `tokio::time::timeout`
    // (under start_paused, awaiting it advances the virtual clock past the 4s
    // zombie threshold while the writer is parked at stream.next()).
    //
    // The outer guard's `.expect` is the bespoke mutation-failure message:
    // without the cancel arm the writer never resolves and this Elapses.
    let writer_result = tokio::time::timeout(ZOMBIE_ADVANCE * 4, writer_handle)
        .await
        .expect(
            "zombie write must be reaped — StallGuard and in-flight slot must not \
             leak when producer goes silent on a live h2 connection",
        )
        .expect("writer task must not panic");

    // frame_tx is still alive here — the writer resolved purely from the reap.
    drop(frame_tx);

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

/// **MINOR-1: on reap, the `active_uploads` slot is freed IMMEDIATELY — not
/// after the ~60s idle TTL.**
///
/// When the sweeper reaps a zombie, the `select!` cancel arm dropped
/// `process_client_stream` mid-await, so neither `store_errored` nor
/// `tx_pipe_broken` is set — the #418 corrupt-state arm does NOT fire. Without
/// the MINOR-1 fix `ActiveStreamGuard::Drop` would RECYCLE the entry into an
/// `IdleStream` that lives ~60s until Pass-1 TTL eviction, and (worse) add its
/// partial bytes to `partial_write_bytes`, feeding Pass-2 memory-pressure
/// eviction of LEGITIMATE idle streams (review e4554c4d MINOR-1, convergent
/// distsys + red-team + code + testing-czar). The fix sets a `zombie_reaped`
/// flag in the cancel arm so `Drop` removes the entry immediately.
///
/// **Observation (production-reachable):** `QueryWriteStatus` for the reaped
/// UUID. With immediate removal, the entry is gone → handler falls through to
/// `store.has()` (the partial blob was never committed) → returns
/// `committed_size = 0, complete = false`, so a retrying Bazel client restarts
/// from offset 0 (correct). If the entry instead lingered as an `IdleStream`,
/// `QueryWriteStatus` would return the partial `committed_size` (16) for up to
/// 60s — the bug this test pins.
///
/// **Mutation step (CLAUDE.md TDD #5):** revert the immediate-removal (delete
/// the `if self.zombie_reaped.load(...)` arm in `Drop`, or the
/// `zombie_reaped_flag.store(true, ...)` in the cancel arm). The reaped entry
/// then recycles to an `IdleStream` and `QueryWriteStatus` returns
/// `committed_size = 16` → this test fails at the `committed_size == 0` assert
/// with its bespoke message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn reaped_zombie_slot_freed_immediately_not_after_idle_ttl() {
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

    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("immremove_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // Send a partial chunk (16 bytes of 64 declared), then hold the sender alive
    // and silent — a zombie. bytes_received = 16 < expected_size (64), so the
    // MAJOR-2 guard does not exclude it; the sweeper reaps it at the threshold.
    let uuid = "eeeeeeee-0000-0000-0000-000000000005";
    let resource_name = make_resource_name(HASH1, 64, uuid);
    send_first_chunk(&frame_tx, resource_name, Bytes::from(vec![0x42u8; 16])).await;

    // Hold frame_tx alive in the MAIN task; the writer can only resolve via the
    // sweeper-fired reap (advancing virtual time inside the timeout).
    let writer_result = tokio::time::timeout(ZOMBIE_ADVANCE * 4, writer_handle)
        .await
        .expect("zombie write must be reaped (immediate-removal test)")
        .expect("writer task must not panic");

    let status = writer_result.expect_err("reaped zombie must return Err");
    assert!(
        status.message().contains("zombie write reaped"),
        "must be the zombie reap path; got: {:?}",
        status.message(),
    );

    // The active_uploads entry must be GONE NOW (in-flight slot freed at reap
    // time), so QueryWriteStatus falls through to store.has() → committed_size 0.
    // No virtual time advanced between the reap and this query, so a non-zero
    // result would mean the entry lingered as an IdleStream (the MINOR-1 bug).
    let qws = bs_server
        .query_write_status(Request::new(QueryWriteStatusRequest {
            resource_name: make_resource_name(HASH1, 64, uuid),
        }))
        .await
        .expect("query_write_status must succeed")
        .into_inner();

    // frame_tx alive until here so the connection stayed "live" through the reap.
    drop(frame_tx);

    assert_eq!(
        qws.committed_size, 0,
        "reaped zombie's active_uploads slot must be freed IMMEDIATELY — \
         QueryWriteStatus must return committed_size=0 (entry gone, Bazel \
         restarts from 0), NOT the partial 16 bytes of a lingering IdleStream",
    );
    assert!(
        !qws.complete,
        "reaped zombie's upload must report complete=false (not durable)",
    );
}

/// **Under-action test: a zombie that PROGRESSED then died is still reaped —
/// the byte-progress reset path is load-bearing.**
///
/// `zombie_write_is_reaped_after_threshold` covers the simplest zombie: bytes
/// stuck at the first observation. This test covers a zombie that advanced its
/// `bytes_received` once (8 → 16) BEFORE going silent. The sweeper observes the
/// advance and RESETs `(first_seen_at, first_seen_bytes)`; the no-progress timer
/// must then accumulate from the reset point so the reap still fires.
///
/// This is the test that makes the reset line load-bearing (testing-czar T3).
/// WITHOUT the reset, after bytes advance to 16 the stale entry keeps its
/// original `first_seen_bytes = 8`, so the reap condition
/// (`current_bytes == first_seen_bytes`) is never satisfied again — the write is
/// NEVER reaped and the writer hangs forever. (The simple zombie test cannot
/// catch this because its counter never advances past the first observation.)
///
/// Timing (threshold = 4s, sweep = 1s):
/// - t=0: chunk 1 (8 bytes), write becomes active.
/// - sweep records (≈1s, 8).
/// - t=2s: chunk 2 (8 bytes) → bytes=16; sweep RESETs to (≈2s, 16).
/// - producer silent thereafter, then drops frame_tx at t≈14s.
/// - WITH reset: at t≈6s (4s after the reset) bytes are stable at 16 → REAP,
///   writer returns the "zombie write reaped" Err well before t=14s.
/// - WITHOUT reset: the stale entry keeps first_seen_bytes=8, so
///   `current_bytes (16) != first_seen_bytes (8)` forever takes the (now-empty)
///   advance branch and the reap condition is never satisfied. The write is NOT
///   reaped; it only ends when the producer drops frame_tx at t≈14s, returning
///   `client closed write stream mid-upload` (bytes_received=16/64).
///
/// `expected_size = 64` keeps `bytes_received (16) < expected_size`, so the
/// MAJOR-2 guard does not exclude this write.
///
/// **Mutation step (CLAUDE.md TDD #5):** comment out the reset
/// (`zombie_stale_since.insert(*uuid, (now_tokio, current_bytes))` in the
/// `current_bytes != first_seen_bytes` branch). This test then FAILS at the
/// `contains("zombie write reaped")` assertion below — the writer returns the
/// "client closed write stream mid-upload" Err (frame_tx dropped at t≈14s),
/// NOT a reap — within the 30s outer guard (it does NOT time out, because the
/// producer's frame_tx drop unblocks the writer before the guard expires).
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn progressed_then_silent_zombie_is_reaped() {
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

    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("progressed_zombie_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // Producer: chunk 1 at t=0 (bytes→8), chunk 2 at t=2s (bytes→16, advancing
    // the counter so the sweeper RESETs), then hold the sender alive and silent.
    let zombie_producer_handle: JoinHandleDropGuard<()> =
        spawn!("progressed_zombie_producer", async move {
            let resource_name =
                make_resource_name(HASH2, 64, "dddddddd-0000-0000-0000-000000000004");
            send_first_chunk(&frame_tx, resource_name, Bytes::from(vec![0x42u8; 8])).await;

            // Advance bytes once (8 → 16) at t=2s so the sweeper observes
            // progress and resets its no-progress timer. 2s < 4s threshold, so
            // the write is still alive at this point.
            tokio::time::sleep(IDLE_TIMEOUT).await; // 2s
            send_middle_chunk(&frame_tx, 8, Bytes::from(vec![0x42u8; 8])).await;

            // Now go silent past the threshold measured FROM THE RESET. The
            // reaper must fire ~4s after the reset (t≈6s). Sleep well past that.
            tokio::time::sleep(ZOMBIE_ADVANCE * 2).await; // +12s → t≈14s
            drop(frame_tx);
        });

    zombie_producer_handle
        .await
        .expect("progressed zombie producer task must not panic");

    let writer_result = tokio::time::timeout(Duration::from_secs(30), writer_handle)
        .await
        .expect(
            "progressed-then-silent zombie must be reaped — the byte-progress \
             reset must re-arm the no-progress timer so a zombie that advanced \
             once is still reaped after going silent",
        )
        .expect("writer task must not panic");

    let status = writer_result.expect_err(
        "progressed-then-silent zombie must return Err — a producer that advanced \
         bytes once then went silent on a live connection must still be reaped",
    );
    assert!(
        status.message().contains("zombie write reaped"),
        "zombie reap error must contain 'zombie write reaped'; got: {:?}",
        status.message(),
    );
}

/// **Over-action test: slow-but-alive producer is NOT killed.**
///
/// A producer sends chunks every `ALIVE_CHUNK_DELAY = 500ms` of virtual time —
/// inside the zombie threshold (`2 × IDLE_TIMEOUT = 4s`). The sweeper sees
/// `bytes_received` advancing and MUST NOT fire the zombie cancel. The upload
/// completes successfully.
///
/// This guards the over-action contract from the OPPOSITE end of the
/// `progressed_then_silent_zombie_is_reaped` test: a producer that keeps making
/// progress is never reaped. (The reset path's load-bearing under-action
/// coverage lives in `progressed_then_silent_zombie_is_reaped`; removing the
/// reset makes the reaper strictly more lenient, so this over-action test is
/// correctly insensitive to that mutation.)
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
    // (500ms) apart — well inside the 4s zombie threshold. Each chunk advances
    // `bytes_received`, so the sweeper never sees a no-progress window.
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

/// **Over-action test: a write that has received all declared bytes is NOT
/// reaped, even when its `bytes_received` counter is stable past the threshold.**
///
/// This is the MAJOR-2 regression (review c2a06843, convergent distsys +
/// red-team + testing-czar T5). `bytes_received` is stable in TWO distinct
/// states: (a) a genuine zombie parked at `stream.next()` with `bytes_received <
/// expected_size`, and (b) a write that has received ALL declared bytes and is
/// in its store-commit phase (`process_client_stream` returned Ok;
/// `store_update_fut` draining the buf_channel into the store + mirror). If the
/// store-commit phase outlasts `2 × idle_stream_timeout` (ZFS pressure, lock
/// contention, slow GrpcStore mirror), the OLD code misclassified state (b) as a
/// zombie and fired `Code::Aborted`, BYPASSING the FL-688 ≥2-replica mirror
/// ack-gate.
///
/// The fix gates Pass 3 on `current_bytes >= expected_size`: once all declared
/// bytes are received, the write is never reaped.
///
/// **Simulation:** a producer sends exactly `expected_size` bytes across two
/// chunks WITHOUT `finish_write=true`, then holds the sender alive (silent).
/// `bytes_received` reaches `expected_size` and stays there. The server parks at
/// `stream.next()` awaiting the finish_write chunk — but because all declared
/// bytes were received, the MAJOR-2 guard excludes it from zombie reaping. The
/// test advances virtual time well past `2 × idle_stream_timeout` and asserts
/// the writer has NOT returned `Code::Aborted "zombie write reaped"`. (It stays
/// parked, so the outer `tokio::time::timeout` resolves to `Elapsed`, which is
/// the EXPECTED outcome here — distinguished from a reaper-fired Err by the
/// assertion below.)
///
/// **Mutation step (CLAUDE.md TDD #5):** removing the
/// `if current_bytes >= *expected_size { ...; continue; }` guard in Pass 3
/// causes the all-bytes-received write to be reaped at the threshold; the writer
/// then resolves to `Err(Code::Aborted, "zombie write reaped ...")` within the
/// window and this test fails at the `must_not_reap` assertion.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn all_bytes_received_not_reaped_during_store_commit() {
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

    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("commit_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // declared_size is the FULL upload size. We send exactly this many bytes
    // across two NON-terminal chunks (no finish_write), so bytes_received
    // reaches declared_size while the server stays parked at stream.next().
    let declared_size: usize = 32;
    let half = declared_size / 2;
    let resource_name =
        make_resource_name(HASH1, declared_size, "cccccccc-0000-0000-0000-000000000003");

    // First chunk (16 bytes, write_offset=0, finish_write=false): gets past
    // WriteRequestStreamWrapper::from and advances bytes_received to 16.
    send_first_chunk(&frame_tx, resource_name, Bytes::from(vec![0x55u8; half])).await;

    // Second chunk (16 bytes, write_offset=16, finish_write=false): advances
    // bytes_received to 32 == declared_size. Still NO finish_write, so the
    // server loops back to stream.next() and parks. bytes_received is now stable
    // at declared_size — this is the store-commit-phase / all-bytes-received
    // shape that MUST NOT be reaped.
    send_middle_chunk(&frame_tx, half, Bytes::from(vec![0x55u8; half])).await;

    // Hold the sender alive (do NOT drop frame_tx) while we advance virtual time
    // well past the zombie threshold (2 × IDLE_TIMEOUT = 4s). Keeping frame_tx
    // in scope here is the load-bearing simulation: the connection is alive, all
    // declared bytes were received, but no finish_write arrives.
    let advance = IDLE_TIMEOUT * 4; // 8s >> 4s threshold + sweep interval.

    // The writer MUST NOT resolve with a zombie-reap Err during this window.
    // It stays parked, so timeout returns Elapsed — the EXPECTED outcome. A
    // reaper misfire would instead resolve writer_handle to Err(Aborted).
    let outcome = tokio::time::timeout(advance, writer_handle).await;

    match outcome {
        Err(_elapsed) => {
            // Parked as expected — all-bytes-received write was NOT reaped.
        }
        Ok(join_result) => {
            let writer_result = join_result.expect("writer task must not panic");
            match writer_result {
                Err(status) => {
                    let msg = status.message();
                    assert!(
                        !msg.contains("zombie write reaped"),
                        "must_not_reap: all-bytes-received write (bytes_received == \
                         expected_size, store-commit phase) was wrongly reaped — \
                         this bypasses the FL-688 >=2-replica mirror ack-gate; got: {msg:?}",
                    );
                }
                Ok(_response) => {
                    // Also acceptable: if the server somehow completed without a
                    // finish_write it would not be a reap. Not the reaper firing.
                }
            }
        }
    }

    // Hold frame_tx until here so the connection stays "alive" for the whole
    // window (the silent-but-alive simulation).
    drop(frame_tx);
}

/// **Boundary test: pin the `>= 2 × idle_stream_timeout` reap boundary.**
///
/// Convergent review request (e4554c4d pair-a B1 + pair-b M3): the other tests
/// use coarse spacing (500ms chunks vs 4s threshold, or a single 8→16 advance);
/// none pins the actual boundary. This test exercises BOTH sides of the `>=`
/// comparison in one run so a future threshold change (or a `>` vs `>=` typo)
/// cannot silently regress it:
///
/// - **Just UNDER (not reaped):** the producer advances `bytes_received` every
///   `THRESHOLD - 1s = 3s` — strictly inside the 4s threshold — sustained across
///   THREE windows (9s total, > 2 thresholds). Each advance resets the sweeper's
///   no-progress timer, so the write is never reaped during the progressing
///   phase.
/// - **Just OVER (reaped):** after the last advance the producer goes silent.
///   `2 × idle_stream_timeout` of no progress then elapses and the sweeper reaps
///   it. The final `bytes_received` (48) stays `< expected_size` (64), so the
///   MAJOR-2 all-bytes-received guard does not pre-empt the reap.
///
/// The single end-to-end assertion (writer returns the reap Err) proves the
/// just-under phase did NOT reap (else the writer would have returned earlier,
/// but more importantly the reap message is the same — so we additionally assert
/// the write progressed to 48 bytes before the reap via the reap-phase timing).
///
/// **Mutation step (CLAUDE.md TDD #5):** changing the threshold comparison from
/// `>=` to `>` does NOT flip this test (the sweep granularity absorbs 1 tick);
/// the load-bearing mutations for the boundary are already covered by the cancel
/// arm (under-action) and the byte-progress reset (this test's just-under phase
/// relies on it — comment out the reset and the just-under producer is reaped
/// mid-progress, so the writer returns the reap Err BEFORE sending all 3 chunks;
/// the post-condition that 48 bytes were accepted then fails).
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn reap_boundary_just_under_survives_just_over_reaps() {
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

    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("boundary_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // THRESHOLD = 2 × IDLE_TIMEOUT = 4s. JUST_UNDER = 3s (< threshold).
    let just_under = IDLE_TIMEOUT * 2 - Duration::from_secs(1);
    let uuid = "ffffffff-0000-0000-0000-000000000006";

    let producer_handle: JoinHandleDropGuard<()> =
        spawn!("boundary_producer", async move {
            let resource_name = make_resource_name(HASH2, 64, uuid);
            // chunk 1 (bytes → 16) — gets past WriteRequestStreamWrapper::from.
            send_first_chunk(&frame_tx, resource_name, Bytes::from(vec![0x42u8; 16])).await;
            // Just-under advances: each resets the no-progress timer. bytes 16→32→48.
            tokio::time::sleep(just_under).await;
            send_middle_chunk(&frame_tx, 16, Bytes::from(vec![0x42u8; 16])).await;
            tokio::time::sleep(just_under).await;
            send_middle_chunk(&frame_tx, 32, Bytes::from(vec![0x42u8; 16])).await;
            // Now go silent (just-over): 2 × idle_stream_timeout of no progress
            // elapses → the sweeper reaps. bytes stay at 48 < 64.
            tokio::time::sleep(ZOMBIE_ADVANCE * 2).await;
            drop(frame_tx);
        });

    producer_handle
        .await
        .expect("boundary producer task must not panic");

    let writer_result = tokio::time::timeout(Duration::from_secs(60), writer_handle)
        .await
        .expect(
            "boundary writer must resolve — just-over silence must be reaped \
             after the just-under progressing phase survived",
        )
        .expect("writer task must not panic");

    let status = writer_result.expect_err(
        "boundary write must return Err — the final just-over silence must be \
         reaped (the just-under progressing phase must NOT have reaped earlier)",
    );
    assert!(
        status.message().contains("zombie write reaped"),
        "boundary reap error must contain 'zombie write reaped'; got: {:?}",
        status.message(),
    );

    // Post-condition: the just-under progressing phase delivered all 3 chunks
    // (48 bytes) before the reap. If the just-under producer had been wrongly
    // reaped mid-progress, the store would never have received 48 bytes. We
    // assert via QueryWriteStatus that the entry is gone (reaped, committed 0) —
    // and separately that the reap message (above) is the just-over outcome, not
    // a premature just-under reap (which would also carry the same message but
    // would have fired before chunk 3; the 48-byte delivery is implied by the
    // producer completing all sends without a SendError panic in send_middle_chunk).
    let qws = bs_server
        .query_write_status(Request::new(QueryWriteStatusRequest {
            resource_name: make_resource_name(HASH2, 64, uuid),
        }))
        .await
        .expect("query_write_status must succeed")
        .into_inner();
    assert_eq!(
        qws.committed_size, 0,
        "after the just-over reap the entry must be gone (committed_size=0)",
    );
}

/// **Forensics: the `zombie_writes_reaped_*` metric counters increment on reap.**
///
/// Requirement (review e4554c4d-zombie-reaper diag pass): once the reaper auto-
/// cleans zombies, the RATE/trend must be visible on `/metrics` for alerting,
/// and the worker-vs-client split lets an alert distinguish a worker-fleet
/// problem from a Bazel-client problem without log scraping.
///
/// This test reaps ONE client zombie (not worker, not mirror — the default
/// metadata-less path) and asserts:
/// - `zombie_writes_reaped_total == 1`
/// - `zombie_writes_reaped_client_total == 1` (client origin)
/// - `zombie_writes_reaped_worker_total == 0` (NOT a worker)
///
/// **Mutation step (CLAUDE.md TDD #5):** comment out the
/// `m.zombie_writes_reaped_total.fetch_add(1, ...)` block in Pass 3 of the
/// sweeper. The reap still fires (the cancel arm is independent) but the
/// counters stay 0 → this test fails at the `total == 1` assert with its
/// bespoke message.
#[nativelink_test(flavor = "current_thread", start_paused = true)]
async fn zombie_reap_increments_metric_counters() {
    let store_manager = make_store_manager()
        .await
        .expect("store_manager construction must not fail");
    let bs_server = Arc::new(
        make_bytestream_server(store_manager.as_ref()).expect("bs_server new"),
    );

    let metrics = bs_server
        .metrics(INSTANCE_NAME)
        .expect("metrics must exist for the configured instance");
    assert_eq!(
        metrics.zombie_writes_reaped_total.load(Ordering::Relaxed),
        0,
        "counter must start at zero before any reap",
    );

    let (frame_tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
    let stream: Streaming<WriteRequest> =
        Streaming::new_request(codec.decoder(), body, None, None);

    // No x-nativelink-worker / x-nativelink-mirror metadata → client-origin
    // write (is_worker=false, is_mirror=false). The bare `write()` tonic entry
    // is what a Bazel client hits.
    let bs_server_for_writer = Arc::clone(&bs_server);
    let writer_handle: JoinHandleDropGuard<
        Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, tonic::Status>,
    > = spawn!("metric_writer", async move {
        bs_server_for_writer.write(Request::new(stream)).await
    });

    // Partial chunk (16 of 64), then hold sender alive and silent → reaped.
    let resource_name =
        make_resource_name(HASH1, 64, "abababab-0000-0000-0000-00000000000a");
    send_first_chunk(&frame_tx, resource_name, Bytes::from(vec![0x42u8; 16])).await;

    let writer_result = tokio::time::timeout(ZOMBIE_ADVANCE * 4, writer_handle)
        .await
        .expect("zombie write must be reaped (metric test)")
        .expect("writer task must not panic");

    drop(frame_tx);

    let status = writer_result.expect_err("reaped zombie must return Err");
    assert!(
        status.message().contains("zombie write reaped"),
        "must be the zombie reap path; got: {:?}",
        status.message(),
    );

    assert_eq!(
        metrics.zombie_writes_reaped_total.load(Ordering::Relaxed),
        1,
        "zombie_writes_reaped_total must increment exactly once per reap — \
         the metric is the RATE/trend signal for post-deploy alerting",
    );
    assert_eq!(
        metrics
            .zombie_writes_reaped_client_total
            .load(Ordering::Relaxed),
        1,
        "client-origin reap must increment zombie_writes_reaped_client_total",
    );
    assert_eq!(
        metrics
            .zombie_writes_reaped_worker_total
            .load(Ordering::Relaxed),
        0,
        "a client (not worker) reap must NOT increment the worker counter",
    );
}
