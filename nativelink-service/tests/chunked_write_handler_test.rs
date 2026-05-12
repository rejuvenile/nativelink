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

//! #212 Phase 2.2/2.3: production-composition tests for the
//! `WriteChunked` server-side RPC handler.
//!
//! These tests build a real `FilesystemStore` + a real
//! `ChunkedWriteHandler` and stream `WriteChunk` messages through the
//! handler's `write_chunked()` method via the same `tonic::Streaming`
//! plumbing (ProstCodec + ChannelBody) that the bytestream_server
//! tests use. This is the closest we get to the production
//! composition WITHOUT a full in-process tonic server (which would
//! add significant test setup for marginal additional coverage).
//!
//! Per CLAUDE.md test discipline:
//! - Every async test wrapped under `tokio::time::timeout(5s)` for
//!   deadlock-detection (the 5s is the deadlock alarm; the test
//!   itself takes ms in the happy path).
//! - Specific assertion messages on every `expect()` so a
//!   `tokio::time::Elapsed` cannot be confused with a real assertion
//!   failure.
//! - Production composition: real FilesystemStore (with sharded
//!   content_path layout, real chunked_partials map, real adapter
//!   methods).

#![cfg(feature = "chunked_fast_slow")]

use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Frame;
use nativelink_config::stores::FilesystemSpec;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, WriteChunk, backpressure_signal,
};
use nativelink_service::chunked_write_handler::{
    ChunkedWriteHandler, ChunkedWriteInFlight, wait_for_no_in_flight,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::{ChunkBudget, TOTAL_CHUNK_PERMITS};
use nativelink_store::chunked::chunked_driver::PER_BLOB_MPSC_CAP;
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked_signal::{
    encode_backpressure_signal_any, error_has_backpressure_reason, error_has_backpressure_signal,
};
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_util::channel_body_for_tests::ChannelBody;
use nativelink_util::common::{DigestInfo, encode_stream_proto};
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tonic::Streaming;
use tonic::codec::Codec;
use tonic_prost::ProstCodec;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/// Build a fresh `FilesystemStore` rooted at a unique per-test temp
/// directory. Returns the store + the content_path so the test can
/// stat the final CAS file directly.
async fn make_store() -> (Arc<FilesystemStore<FileEntryImpl>>, String) {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/chunked-write-test/content");
    let temp_path = format!("{base}/{nonce}/chunked-write-test/temp");
    let store = FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path: content_path.clone(),
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed");
    (store, content_path)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// Build the per-test handler with its OWN ChunkBudget AND an
/// explicit per-test chunk size (so 4 KiB micro-chunks exercise the
/// real out-of-order + commit logic without burning 1 MiB per chunk
/// in production-CHUNK_SIZE units). Returns the handler + the
/// in-flight tracker for direct inspection.
fn make_handler(
    store: Arc<FilesystemStore<FileEntryImpl>>,
    budget: &'static ChunkBudget,
    chunk_size: usize,
) -> (
    Arc<ChunkedWriteHandler>,
    Arc<ChunkedWriteInFlight>,
) {
    let in_flight = ChunkedWriteInFlight::new();
    let handler = Arc::new(ChunkedWriteHandler::new_with_state_and_chunk_size_for_test(
        store,
        Arc::clone(&in_flight),
        budget,
        chunk_size,
    ));
    (handler, in_flight)
}

/// Wrap an mpsc-driven body into a `tonic::Streaming<WriteChunk>`.
/// Sender side accepts already-grpc-framed bytes (use `frame_chunk`
/// to encode a `WriteChunk` into a `Frame<Bytes>`).
fn make_chunk_stream() -> (mpsc::Sender<Frame<Bytes>>, Streaming<WriteChunk>) {
    let (tx, body) = ChannelBody::new();
    let mut codec = ProstCodec::<WriteChunk, WriteChunk>::default();
    let stream = Streaming::new_request(codec.decoder(), body, None, None);
    (tx, stream)
}

/// gRPC-frame a single WriteChunk for sending into the channel body.
/// Mirrors `encode_stream_proto` from `nativelink-util::common` (which
/// the bytestream tests use); inlined here for clarity since we only
/// need the WriteChunk variant.
fn frame_chunk(chunk: &WriteChunk) -> Frame<Bytes> {
    let bytes = encode_stream_proto(chunk).expect("encode WriteChunk to grpc frame");
    Frame::data(bytes)
}

/// Build a fully-formed WriteChunk for offset `i*chunk_size` from
/// `bytes` + `digest`.
fn make_chunk(
    digest: DigestInfo,
    chunk_offset: u64,
    chunk_bytes: &[u8],
    finish: bool,
) -> WriteChunk {
    WriteChunk {
        digest: Some(digest.into()),
        chunk_offset,
        chunk_bytes: Bytes::copy_from_slice(chunk_bytes),
        chunk_sha256: sha256(chunk_bytes).to_vec(),
        finish_chunk: finish,
    }
}

// Test-only ChunkBudget singletons. Each test gets its own to avoid
// cross-test interference. Wrapped in OnceLock so static-lifetime is
// satisfied for the handler API.
fn make_test_budget() -> &'static ChunkBudget {
    Box::leak(Box::new(ChunkBudget::new()))
}

// -----------------------------------------------------------------------------
// Production-composition tests
// -----------------------------------------------------------------------------

/// Client streams 3 chunks (4 KiB each) + finish → server returns
/// WriteChunkedResponse with the right size; the FilesystemStore has
/// the file at the content_path layout, with the right contents.
#[nativelink_test]
async fn handler_streams_three_chunks_then_finish_commits_blob() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 3;
    let total = (N * CHUNK) as u64;

    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa0u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();

    // Spawn the handler in the background; feed the stream from this
    // task. This mirrors the bytestream_server test structure.
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let chunk = make_chunk(
                digest,
                (i * CHUNK) as u64,
                &blob[i * CHUNK..(i + 1) * CHUNK],
                i == N - 1,
            );
            tx.send(frame_chunk(&chunk))
                .await
                .expect("must not deadlock — channel send to handler");
        }
        drop(tx);
    })
    .await
    .expect("must not deadlock — sending 3 chunks should finish promptly");

    let response = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must respond within 5s")
        .expect("handler task must not panic")
        .expect("write_chunked must return Ok for hash-matching blob");
    let resp_inner = response.into_inner();
    assert_eq!(
        resp_inner.committed_size, total,
        "committed_size must equal blob length"
    );

    // Final file exists at content_path with correct length.
    let final_path = format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let meta = tokio::fs::metadata(&final_path)
        .await
        .expect("final file must exist after commit");
    assert_eq!(meta.len(), total, "final file must have correct length");

    // In-flight tracker drained.
    wait_for_no_in_flight(&in_flight, Duration::from_secs(2))
        .await
        .expect("in-flight entry must drain after successful commit");
}

/// Two concurrent streams for the SAME digest → second is rejected
/// with `Code::Aborted` + a `BackpressureSignal` retry hint (M-code-2
/// fixup). The historical `AlreadyExists` was misleading — gRPC
/// convention treats `AlreadyExists` as "the resource is durably
/// committed at the target," which a worker-side BIS-style auto-unpinner
/// could read as a license to drop its mirror pin. With `Aborted` the
/// worker correctly interprets this as "transaction failed, retry";
/// it must NOT unpin its mirror entry on this code.
#[nativelink_test]
async fn handler_concurrent_streams_for_same_digest_returns_aborted_with_retry_hint() {
    const CHUNK: usize = 4 * 1024;
    let total = CHUNK as u64;
    let blob = vec![0xb0u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx_a, stream_a) = make_chunk_stream();
    let (tx_b, stream_b) = make_chunk_stream();

    // First stream: send the first chunk so the driver is registered,
    // but DO NOT send finish; this leaves the in-flight entry alive.
    let h_a = Arc::clone(&handler);
    let writer_a =
        tokio::spawn(async move { h_a.write_chunked(tonic::Request::new(stream_a)).await });

    let first_chunk = make_chunk(digest, 0, &blob, false);
    tx_a.send(frame_chunk(&first_chunk))
        .await
        .expect("first stream chunk send must succeed");

    // Wait until the in-flight tracker has the entry. We need to
    // observe-then-act so the second stream's start is racy-clean.
    tokio::time::timeout(Duration::from_secs(5), async {
        while handler.in_flight().in_flight_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first stream must register an in-flight entry within 5s");

    // Second stream: also targets `digest` — must be rejected.
    let h_b = Arc::clone(&handler);
    let writer_b =
        tokio::spawn(async move { h_b.write_chunked(tonic::Request::new(stream_b)).await });
    // Second stream's first chunk:
    let second_first = make_chunk(digest, 0, &blob, true);
    tx_b.send(frame_chunk(&second_first))
        .await
        .expect("second stream send must succeed");
    drop(tx_b);
    let result_b = tokio::time::timeout(Duration::from_secs(5), writer_b)
        .await
        .expect("must not deadlock — second stream must reject promptly")
        .expect("second writer task must not panic");
    let status_b =
        result_b.expect_err("second stream must return Err (Aborted)");
    assert_eq!(
        status_b.code(),
        tonic::Code::Aborted,
        "second concurrent stream for same digest must be Aborted (NOT AlreadyExists — \
         that gRPC code carries 'resource exists at target' wire semantics that a worker-side \
         auto-unpinner could mis-interpret as durable commit; M-code-2 fixup); got {status_b:?}"
    );
    // Verify the BackpressureSignal retry hint is present so clients
    // can back off. The detail doubles as a discriminator for the
    // §13.1.1 point 2 dead-channel classifier.
    let err: nativelink_error::Error = status_b.into();
    assert!(
        err.details
            .iter()
            .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL),
        "Aborted concurrent-stream rejection must carry a BackpressureSignal retry hint; got details={:?}",
        err.details
    );

    // Tear down the first stream cleanly so the test exits.
    drop(tx_a);
    let _ = tokio::time::timeout(Duration::from_secs(5), writer_a).await;
}

/// Client disconnects mid-stream (no finish) → handler returns
/// Code::Aborted; in-flight entry drains; FilesystemStore content_path
/// has NO file (commit never ran).
#[nativelink_test]
async fn handler_client_drops_mid_stream_returns_aborted_no_commit() {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = (3 * CHUNK) as u64;
    let blob = vec![0xc0u8; total as usize];
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        // Send only chunk 0 + chunk 1, drop tx (no finish).
        let c0 = make_chunk(digest, 0, &blob[..CHUNK], false);
        let c1 = make_chunk(digest, CHUNK as u64, &blob[CHUNK..2 * CHUNK], false);
        tx.send(frame_chunk(&c0)).await.unwrap();
        tx.send(frame_chunk(&c1)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — sending two chunks then dropping");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must respond to client drop within 5s")
        .expect("writer task must not panic");
    let status =
        result.expect_err("client-drop-without-finish must return Err (Aborted)");
    assert_eq!(
        status.code(),
        tonic::Code::Aborted,
        "client-drop-without-finish must surface as Aborted; got {status:?}"
    );

    // No final file produced.
    let final_path = format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let meta = tokio::fs::metadata(&final_path).await;
    assert!(
        meta.is_err(),
        "no final file must exist when client drops mid-stream; got {meta:?}"
    );

    // In-flight tracker drained.
    wait_for_no_in_flight(&in_flight, Duration::from_secs(2))
        .await
        .expect("in-flight entry must drain after client drops");
}

/// Per-chunk SHA-256 mismatch → handler returns Code::InvalidArgument
/// with a specific error message. In-flight tracker drains; no commit.
#[nativelink_test]
async fn handler_per_chunk_sha256_mismatch_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = CHUNK as u64;
    let blob = vec![0xd0u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let mut bad_chunk = make_chunk(digest, 0, &blob, true);
    // Lie about the per-chunk hash.
    bad_chunk.chunk_sha256 = vec![0xffu8; 32];
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&bad_chunk)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — single-chunk send");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must respond to bad chunk within 5s")
        .expect("writer task must not panic");
    let status =
        result.expect_err("per-chunk SHA-256 mismatch must return Err (InvalidArgument)");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "per-chunk SHA-256 mismatch must be InvalidArgument; got {status:?}"
    );
    assert!(
        status.message().contains("per-chunk SHA-256 mismatch"),
        "error must name the contract; got {}",
        status.message()
    );

    let final_path = format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let meta = tokio::fs::metadata(&final_path).await;
    assert!(
        meta.is_err(),
        "no final file must exist after per-chunk SHA-256 mismatch; got {meta:?}"
    );

    wait_for_no_in_flight(&in_flight, Duration::from_secs(2))
        .await
        .expect("in-flight entry must drain after per-chunk SHA-256 mismatch");
}

/// Global ChunkBudget exhausted → handler returns Code::ResourceExhausted
/// with the BackpressureSignal type_url + GLOBAL_CHUNK_BUDGET_EXHAUSTED
/// reason set. The wire-stable detail is what the receiver-side
/// `looks_like_dead_channel` classifier matches against to avoid
/// evicting the underlying h2 channel on a backpressure event (per
/// design §13.1.1 point 2).
#[nativelink_test]
async fn handler_global_chunk_budget_exhausted_returns_resource_exhausted_with_backpressure_signal()
 {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = CHUNK as u64;
    let blob = vec![0xe0u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    // Drain the budget BEFORE starting the stream so the very first
    // chunk's admission fails.
    let mut hold: Vec<_> = (0..TOTAL_CHUNK_PERMITS)
        .map(|_| {
            budget
                .try_acquire_chunk()
                .expect("draining the test budget should succeed for every permit")
        })
        .collect();

    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let chunk = make_chunk(digest, 0, &blob, true);
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&chunk)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — single-chunk send");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject promptly when budget is exhausted")
        .expect("writer task must not panic");
    let status = result.expect_err("must return Err when budget is exhausted");
    assert_eq!(
        status.code(),
        tonic::Code::ResourceExhausted,
        "must classify as ResourceExhausted; got {status:?}"
    );

    // Verify the wire-stable BackpressureSignal detail. tonic's
    // `Status::details()` is the raw bytes; we go through the
    // `Status` → `nativelink_error::Error` round-trip the production
    // code uses (since `looks_like_dead_channel` matches on
    // `Error.details`).
    let err: nativelink_error::Error = status.clone().into();
    assert!(
        err.details
            .iter()
            .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL),
        "ResourceExhausted must carry a BackpressureSignal detail; got details={:?}",
        err.details
    );
    let signal_any = err
        .details
        .iter()
        .find(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
        .unwrap();
    let signal = BackpressureSignal::decode(&*signal_any.value)
        .expect("BackpressureSignal must decode cleanly");
    assert_eq!(
        signal.reason,
        backpressure_signal::Reason::GlobalChunkBudgetExhausted as i32,
        "must report GlobalChunkBudgetExhausted; got {signal:?}"
    );

    // Release the held permits so the next test's budget is clean.
    hold.clear();
}

/// Per-blob mpsc full → ResourceExhausted with PER_BLOB_MPSC_FULL.
/// We saturate the per-blob mpsc by sending PER_BLOB_MPSC_CAP+1 chunks
/// rapidly with a driver that processes them slowly. To avoid actually
/// depending on timing, we exercise the same admission path via a
/// direct unit-style path: send PER_BLOB_MPSC_CAP+1 chunks back-to-back
/// without yielding — at least one must trip the per-blob full path.
///
/// `N` MUST track `PER_BLOB_MPSC_CAP` symbolically (NOT a literal): the
/// rejection branch only becomes reachable when N exceeds the cap, and
/// past bumps (16 → 64 → 256) silently rendered the prior literal
/// (32 / 65) toothless until the integration test was refreshed.
///
/// Note: this is a probabilistic test in production conditions, but
/// here we deliberately construct a scenario where the driver task
/// has not had a chance to drain. Wrapped under a 5s timeout.
#[nativelink_test]
async fn handler_per_blob_mpsc_full_returns_resource_exhausted_with_backpressure_signal() {
    const CHUNK: usize = 4 * 1024;
    // N MUST be > PER_BLOB_MPSC_CAP — track the cap symbolically so a
    // future PER_BLOB_MPSC_CAP bump cannot silently render this
    // rejection branch unreachable (regression: at cap=16 the test
    // hardcoded N=32; at cap=64 it was already toothless; at cap=256
    // it was doubly so).
    const N: usize = PER_BLOB_MPSC_CAP + 1;
    let total = (N * CHUNK) as u64;
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        // Wrapping arithmetic: with N = PER_BLOB_MPSC_CAP + 1 = 257
        // a plain `0x10u8 + i as u8` would overflow at i = 240; use
        // wrapping_add so the test stays correct under future cap
        // bumps.
        let byte = 0x10u8.wrapping_add((i & 0xff) as u8);
        blob.extend(std::iter::repeat(byte).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(15), async {
        for i in 0..N {
            let chunk = make_chunk(
                digest,
                (i * CHUNK) as u64,
                &blob[i * CHUNK..(i + 1) * CHUNK],
                i == N - 1,
            );
            // Send all chunks back-to-back with no yield. The first
            // PER_BLOB_MPSC_CAP ChunkWorks fit in the per-blob mpsc;
            // subsequent admissions race the driver's drain. We expect
            // at least ONE per-blob-full rejection for one of these
            // admissions.
            //
            // If all N happen to make it through (driver drained fast
            // enough on this host), we still observe successful commit
            // + don't fail the test — the test's CONTRACT is "either
            // it commits OR the rejection is properly tagged" — we
            // observe the rejection path via the assertion below ONLY
            // when the rejection actually happens. To force-test the
            // rejection path we use a direct-driver test in
            // chunked_driver.rs (the unit tests).
            //
            // For THIS integration test we assert EITHER:
            //   - a rejection happens AND it is properly tagged
            //   - OR the commit happens AND no rejection is needed.
            tx.send(frame_chunk(&chunk)).await.unwrap();
        }
        drop(tx);
    })
    .await
    .expect("must not deadlock — burst-send PER_BLOB_MPSC_CAP+1 chunks");

    let result = tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("must not deadlock — handler must respond within 15s")
        .expect("writer task must not panic");

    match result {
        Ok(resp) => {
            // Driver drained fast enough; ALL N chunks were admitted
            // and committed. This is also a valid outcome on a fast
            // host — the contract test for the per-blob-full
            // rejection path is in the composite-invariant test
            // `chunked_driver::tests::global_chunk_budget_remains_4_gib_bound_at_cap_256`
            // (asserts the 17th saturated blob's first-chunk admission
            // is rejected by the global ChunkBudget, the dominant gate
            // at PER_BLOB_MPSC_CAP=256).
            assert_eq!(
                resp.into_inner().committed_size,
                total,
                "if no per-blob rejection happens, the commit must succeed"
            );
        }
        Err(status) => {
            assert_eq!(
                status.code(),
                tonic::Code::ResourceExhausted,
                "per-blob-full rejection must be ResourceExhausted; got {status:?}"
            );
            let err: nativelink_error::Error = status.into();
            assert!(
                err.details
                    .iter()
                    .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL),
                "per-blob-full ResourceExhausted must carry a BackpressureSignal; \
                 got details={:?}",
                err.details
            );
            let signal_any = err
                .details
                .iter()
                .find(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
                .unwrap();
            let signal = BackpressureSignal::decode(&*signal_any.value)
                .expect("decode BackpressureSignal");
            assert_eq!(
                signal.reason,
                backpressure_signal::Reason::PerBlobMpscFull as i32,
                "per-blob full reason must be PerBlobMpscFull; got {signal:?}"
            );
        }
    }
}

/// Stream switches digest mid-blob → InvalidArgument. Producer protocol
/// violation; the schema requires every chunk in a stream to carry the
/// SAME digest.
#[nativelink_test]
async fn handler_digest_switch_mid_stream_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let blob_a = vec![0xa1u8; CHUNK];
    let blob_b = vec![0xb1u8; CHUNK];
    let digest_a = DigestInfo::new(sha256(&blob_a), CHUNK as u64);
    let digest_b = DigestInfo::new(sha256(&blob_b), CHUNK as u64);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        // First chunk: digest_a.
        let c0 = make_chunk(digest_a, 0, &blob_a, false);
        tx.send(frame_chunk(&c0)).await.unwrap();
        // Second chunk: digest_b — protocol violation.
        let c1 = make_chunk(digest_b, 0, &blob_b, true);
        tx.send(frame_chunk(&c1)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — two-chunk send");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject digest switch within 5s")
        .expect("writer task must not panic");
    let status = result.expect_err("digest switch mid-stream must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "digest switch must be InvalidArgument; got {status:?}"
    );
    assert!(
        status.message().contains("switched digest mid-stream"),
        "error must name the contract; got {}",
        status.message()
    );
}

/// Empty stream (0 chunks) → InvalidArgument.
#[nativelink_test]
async fn handler_empty_stream_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    // Drop tx without sending anything.
    drop(tx);

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — empty stream must be rejected promptly")
        .expect("writer task must not panic");
    let status = result.expect_err("empty stream must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "empty stream must be InvalidArgument; got {status:?}"
    );
}

/// CHUNK_SIZE pin: the on-wire / on-disk constant must not silently
/// drift. The tests above use 4 KiB micro-chunks for speed; production
/// uses CHUNK_SIZE (1 MiB).
#[test]
fn chunk_size_constant_pinned_for_handler_tests() {
    assert_eq!(CHUNK_SIZE, 1024 * 1024);
}

// ----------------------------------------------------------------------
// M-code-1 fixup tests: zero-byte blob path
// ----------------------------------------------------------------------

/// SHA-256 of the empty string, pinned constant.
const EMPTY_SHA256: [u8; 32] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9,
    0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52,
    0xb8, 0x55,
];

/// Bazel emits the empty-string digest (`e3b0c44…-0`) frequently for
/// empty stdout/stderr in successful actions. Before the M-code-1
/// fixup, the per-blob driver expected `expected_chunk_count == 0` but
/// the producer MUST send a single finish chunk to terminate the
/// stream — the bitmap check rejected every empty-blob upload as
/// `InvalidArgument`.
#[nativelink_test]
async fn handler_zero_byte_blob_commits_with_single_finish_chunk() {
    const CHUNK: usize = 4 * 1024;
    let digest = DigestInfo::new(EMPTY_SHA256, 0);
    let (store, content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        // Single chunk: offset=0, empty bytes, finish=true,
        // chunk_sha256 = SHA-256("").
        let chunk = WriteChunk {
            digest: Some(digest.into()),
            chunk_offset: 0,
            chunk_bytes: Bytes::new(),
            chunk_sha256: EMPTY_SHA256.to_vec(),
            finish_chunk: true,
        };
        tx.send(frame_chunk(&chunk))
            .await
            .expect("send empty-blob chunk");
        drop(tx);
    })
    .await
    .expect("must not deadlock — empty-blob single-chunk path");

    let resp = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — empty-blob handler must respond promptly")
        .expect("writer task must not panic")
        .expect(
            "empty-blob commit must succeed — M-code-1 fixup ensures the zero-byte path \
             bypasses the driver bitmap check that previously rejected every empty digest",
        );
    let inner = resp.into_inner();
    assert_eq!(
        inner.committed_size, 0,
        "empty-blob committed_size must be 0; got {}",
        inner.committed_size
    );

    // Final empty file exists at the canonical CAS path.
    let final_path = format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let meta = tokio::fs::metadata(&final_path)
        .await
        .expect("empty-blob final file must exist");
    assert_eq!(meta.len(), 0, "empty-blob final file must be 0 bytes");

    // In-flight tracker stayed empty — handler did NOT register a
    // driver entry for the zero-byte path.
    assert_eq!(
        in_flight.in_flight_count(),
        0,
        "empty-blob path must not register an in-flight driver entry"
    );
}

/// Empty-blob path: producer lies about size (declares size>0 with
/// the empty SHA, OR declares size=0 with non-empty chunk_sha256). We
/// verify the second case (digest hash != EMPTY_SHA256) returns
/// InvalidArgument.
#[nativelink_test]
async fn handler_zero_byte_blob_rejects_non_empty_digest_hash() {
    const CHUNK: usize = 4 * 1024;
    // size=0 but a non-empty hash → producer is lying.
    let lying_hash = [0xcc_u8; 32];
    let digest = DigestInfo::new(lying_hash, 0);
    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let chunk = WriteChunk {
        digest: Some(digest.into()),
        chunk_offset: 0,
        chunk_bytes: Bytes::new(),
        chunk_sha256: EMPTY_SHA256.to_vec(),
        finish_chunk: true,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&chunk)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject promptly")
        .expect("writer task must not panic");
    let status = result
        .expect_err("zero-byte blob with non-empty digest hash must return Err (InvalidArgument)");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "non-empty digest hash on zero-size blob must be InvalidArgument; got {status:?}"
    );
}

// ----------------------------------------------------------------------
// M-code-3 fixup tests: chunk-shape validation
// ----------------------------------------------------------------------

/// `chunk_offset` MUST be a multiple of CHUNK_SIZE — a producer that
/// sends an unaligned offset (here: 1 byte off) is rejected with
/// `InvalidArgument`. Without M-code-3 the driver would silently
/// `pwrite` to a sparse-file hole that coincidentally lines up.
#[nativelink_test]
async fn handler_chunk_offset_not_multiple_of_chunk_size_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = 2 * CHUNK as u64;
    let blob = vec![0xa3_u8; total as usize];
    let digest = DigestInfo::new(sha256(&blob), total);
    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    // Unaligned offset (1 byte off). chunk_sha256 here is irrelevant —
    // the shape validation runs BEFORE the SHA-256 verify (M-perf-1
    // ordering, but specifically the cheap shape checks come first
    // even before the budget acquire).
    let bad = WriteChunk {
        digest: Some(digest.into()),
        chunk_offset: 1,
        chunk_bytes: Bytes::from(vec![0u8; CHUNK]),
        chunk_sha256: sha256(&vec![0u8; CHUNK]).to_vec(),
        finish_chunk: false,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock")
        .expect("writer task must not panic");
    let status = result.expect_err("unaligned offset must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "unaligned chunk_offset must be InvalidArgument; got {status:?}"
    );
    assert!(
        status.message().contains("multiple of CHUNK_SIZE"),
        "error must name the contract; got {}",
        status.message()
    );
}

/// Non-final chunk with `chunk_bytes.len()` != CHUNK_SIZE is rejected.
/// Without M-code-3 a producer could send oversized chunks that bypass
/// the per-permit byte accounting.
#[nativelink_test]
async fn handler_non_final_chunk_with_wrong_length_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = 2 * CHUNK as u64;
    let blob = vec![0xa4_u8; total as usize];
    let digest = DigestInfo::new(sha256(&blob), total);
    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    // Non-final but only half a chunk — protocol violation.
    let payload = vec![0u8; CHUNK / 2];
    let bad = WriteChunk {
        digest: Some(digest.into()),
        chunk_offset: 0,
        chunk_bytes: Bytes::from(payload.clone()),
        chunk_sha256: sha256(&payload).to_vec(),
        finish_chunk: false,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock")
        .expect("writer task must not panic");
    let status = result.expect_err("non-final-wrong-length must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "non-final wrong-length must be InvalidArgument; got {status:?}"
    );
    assert!(
        status.message().contains("equal CHUNK_SIZE"),
        "error must name the contract; got {}",
        status.message()
    );
}

// ----------------------------------------------------------------------
// B1 fixup test: end-to-end SHA-256 mismatch never lands at canonical
// CAS path (two-stage rename collapses the cancellation window).
// ----------------------------------------------------------------------

/// Producer streams chunks whose per-chunk SHA-256 hashes are honest
/// (the verify step passes) but the assembled blob's hash does NOT
/// match the digest's declared hash (i.e. producer is lying about the
/// blob's true SHA-256). Without B1's two-stage rename, the file would
/// land at the canonical CAS path BEFORE the e2e SHA-256 verify; a
/// concurrent reader would see the wrong-but-canonically-named bytes
/// (CAS poisoning). With the fixup, the file lives at
/// `<digest>.holding` until verify passes; on mismatch it is unlinked.
/// The canonical CAS path MUST never exist.
#[nativelink_test]
async fn handler_e2e_sha256_mismatch_never_lands_at_canonical_cas_path() {
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    let total = (N * CHUNK) as u64;
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xc1u8 + i as u8).take(CHUNK));
    }
    // Lie about the digest's hash — declare an all-0xff hash that does
    // not match the actual blob's SHA-256.
    let lying_hash = [0xff_u8; 32];
    let digest = DigestInfo::new(lying_hash, total);

    let (store, content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let bytes = &blob[i * CHUNK..(i + 1) * CHUNK];
            let chunk = make_chunk(digest, (i * CHUNK) as u64, bytes, i == N - 1);
            tx.send(frame_chunk(&chunk)).await.unwrap();
        }
        drop(tx);
    })
    .await
    .expect("must not deadlock — sending hash-lying blob");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject lying blob within 5s")
        .expect("writer task must not panic");
    let status = result.expect_err("e2e SHA-256 mismatch must return Err (InvalidArgument)");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "e2e SHA-256 mismatch must be InvalidArgument; got {status:?}"
    );

    // CRITICAL: no file at the canonical CAS path. This is the B1
    // contract — without two-stage rename, the file would land at the
    // canonical path BEFORE the e2e SHA verify, and a concurrent
    // reader would see wrong-but-canonically-named bytes.
    let final_path = format!(
        "{}/d/{:02x}/{}",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let meta = tokio::fs::metadata(&final_path).await;
    assert!(
        meta.is_err(),
        "B1 contract violated: file landed at canonical CAS path despite e2e SHA-256 \
         mismatch — CAS poisoning. Two-stage rename must keep the file at .holding \
         until verify passes; got {meta:?}"
    );

    // Also: no .holding file leftover after the mismatch — driver
    // unlink_holding ran and the path should be gone.
    let holding_path = format!(
        "{}/d/{:02x}/{}.holding",
        content_path,
        digest.packed_hash()[0],
        digest
    );
    let holding_meta = tokio::fs::metadata(&holding_path).await;
    assert!(
        holding_meta.is_err(),
        "after e2e mismatch, the .holding file must be unlinked; got {holding_meta:?}"
    );
}

/// Final chunk with `chunk_offset + chunk_bytes.len()` != size_bytes
/// is rejected.
#[nativelink_test]
async fn handler_final_chunk_total_length_mismatch_returns_invalid_argument() {
    const CHUNK: usize = 4 * 1024;
    let total: u64 = 2 * CHUNK as u64;
    let blob = vec![0xa5_u8; total as usize];
    let digest = DigestInfo::new(sha256(&blob), total);
    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, _in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    // Send a final chunk at offset 0 with a half-size payload — total
    // length CHUNK/2 != declared 2*CHUNK.
    let payload = vec![0u8; CHUNK / 2];
    let bad = WriteChunk {
        digest: Some(digest.into()),
        chunk_offset: 0,
        chunk_bytes: Bytes::from(payload.clone()),
        chunk_sha256: sha256(&payload).to_vec(),
        finish_chunk: true,
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock")
        .expect("writer task must not panic");
    let status = result.expect_err("total-length-mismatch on final chunk must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "final-chunk-length-math mismatch must be InvalidArgument; got {status:?}"
    );
    assert!(
        status.message().contains("equal digest.size_bytes")
            || status.message().contains("digest.size_bytes"),
        "error must name the contract; got {}",
        status.message()
    );
}

/// #213 testing-czar M4 fixup (§13.1.1 step 2 `Err(Closed)` driver-gone
/// admission). When the per-blob driver task has terminated (panic, abort,
/// or happy-path exit raced with an admission), `mpsc::Sender::try_send`
/// returns `Err(Closed)`. The admission path MUST:
///   1. drop the returned `ChunkWork` (releases the global ChunkBudget
///      permit and any PinBudget permit via `Drop`);
///   2. surface `Code::Aborted` to the producer (NOT `Code::ResourceExhausted`,
///      because the classifier-tightened `looks_like_dead_channel`
///      treats `ResourceExhausted` as backpressure not a dead channel —
///      see §13.1.1 point 2; the driver-gone case is a NEW stream
///      situation, not a transient backpressure event).
///
/// Test approach: bypass the full handler stack and call
/// [`nativelink_service::chunked_write_handler::admit_prepared_chunk`]
/// directly with an mpsc whose receiver has been DROPPED (so try_send
/// returns Closed). Under a 5s timeout deadlock detector with a
/// SPECIFIC `.expect(...)` message naming the contract.
///
/// Mutation step: revert the `Code::Aborted` arm in `admit_prepared_chunk`
/// to `Code::Internal`; this test then sees `Code::Internal` instead of
/// `Code::Aborted` and the assertion fires with the SPECIFIC message.
/// Also verified: revert to `Code::ResourceExhausted` would mis-label the
/// driver-gone case as backpressure and the assertion would catch that
/// too (different code).
#[nativelink_test]
async fn admit_prepared_chunk_returns_aborted_when_driver_mpsc_closed() {
    use nativelink_service::chunked_write_handler::{
        ChunkedWriteHandlerMetrics, PreparedChunk, admit_prepared_chunk,
    };
    use nativelink_store::chunked::chunked_driver::ChunkWork;

    const CHUNK: usize = 4 * 1024;
    let blob = vec![0xb6u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob), CHUNK as u64);
    let budget = make_test_budget();

    // Construct an mpsc with a CLOSED receiver. The receiver is
    // dropped IMMEDIATELY after construction so the very first
    // try_send returns Err(Closed) (not Full — Full requires a live
    // but un-polled receiver).
    let (tx, rx) = mpsc::channel::<ChunkWork>(16);
    drop(rx);

    let metrics = ChunkedWriteHandlerMetrics::default();
    let prepared = PreparedChunk {
        chunk_offset: 0,
        chunk_bytes: Bytes::from(blob.clone()),
        finish: true,
    };

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        admit_prepared_chunk(prepared, &tx, budget, None, CHUNK, digest, &metrics, None)
    })
    .await
    .expect(
        "must not deadlock — admit_prepared_chunk on a closed mpsc must return promptly \
         (#213 testing-czar M4)",
    );

    let err = result.expect_err(
        "must not deadlock — writer-termination contract violated for chunked driver: \
         driver-gone admission must return Err(Aborted), not Ok",
    );
    assert_eq!(
        err.code,
        nativelink_error::Code::Aborted,
        "driver-gone admission must classify as Code::Aborted (NOT Internal, NOT ResourceExhausted); got {err:?}"
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("driver task gone")
            || msg.contains("closed before finish_chunk"),
        "error must name the driver-gone contract; got {msg}"
    );

    // Reverse-release: dropping the returned ChunkWork inside
    // admit_prepared_chunk must have released the global ChunkBudget
    // permit so the budget gauge is unchanged.
    assert_eq!(
        budget.available_chunks(),
        TOTAL_CHUNK_PERMITS,
        "ChunkBudget permit MUST be released on Closed admission (reverse-release per §13.1.1 step 2); \
         got available_chunks={}",
        budget.available_chunks(),
    );
}

// -----------------------------------------------------------------------------
// #436 measurement-first PinBudget seam coverage
// -----------------------------------------------------------------------------

/// **#436 seam test** — a `PinBudget` admission rejection MUST carry
/// the wire shape that `chunked_client::classify_retryable` keys on so
/// the Bazel-facing client retries the chunk rather than treating the
/// rejection as a terminal failure. The pre-existing
/// `pin_budget_cap_rejects_admission_with_pinned_bytes_exhausted_signal`
/// test in `bazel_facing_internal_chunking_test.rs` asserts the proto
/// fields at the producer (`dispatch_chunks_to_driver`) seam. This
/// test adds the LOW-LEVEL admit-path seam (`admit_prepared_chunk`
/// directly — the function `dispatch_chunks_to_driver` invokes on
/// every chunk) AND asserts the EXACT predicate set that
/// `classify_retryable` consults, so a future refactor that strips the
/// `BackpressureSignal` discriminator from the PinBudget error path
/// is forced through a red-fail with a specific message.
///
/// **Seams crossed end-to-end:**
///   1. Producer: `admit_prepared_chunk` — the post-validation gate
///      at `chunked_write_handler.rs:1649` that calls
///      `PinBudget::try_acquire(chunk_bytes_len)`.
///   2. Error constructor: `Error::resource_exhausted_backpressure` —
///      packs `Code::ResourceExhausted` + the `BackpressureSignal`
///      detail into the wire-format `Error` the client receives.
///   3. Retry classifier predicates: `error_has_backpressure_signal`
///      + `error_has_backpressure_reason([PinnedBytesExhausted])` —
///      the exact pair `classify_retryable`
///      (`chunked_client.rs:512-538`) consults to map the error to
///      `RetryDecision::Retry { reason: ResourceExhausted, retry_after:
///      Duration::from_millis(100) }`.
///
/// **Why this matters for #436.** The pivot is measurement-first: the
/// 4 GiB cap stays in place; what changes is observability so we can
/// SEE the cap being hit before deciding whether to raise it (#437).
/// On the occasional residual exhaustion event today, the typed-signal
/// retry is the only thing keeping Bazel from aborting an action. The
/// gauge-publishing wiring landing alongside this test is meaningless
/// if the rejection itself doesn't carry the discriminator the client
/// needs to recognize it as transient — both the measurement AND the
/// retry contract must hold.
///
/// **Mutation step:** comment out the `let detail = encode_…(
/// PinnedBytesExhausted, PIN_BUDGET_RETRY_AFTER_MS);` block at
/// `chunked_write_handler.rs:1657` and emit a bare
/// `make_err!(Code::ResourceExhausted, ...)` instead. This test will
/// then see `error_has_backpressure_signal == false` and the
/// `error_has_backpressure_reason([PinnedBytesExhausted])` assertion
/// fires with the specific message — which is exactly the failure
/// mode that would make `classify_retryable` return `Abort` for what
/// should be a transient retryable backpressure event.
#[nativelink_test]
async fn pin_budget_exhausted_rejection_carries_correct_signal_and_retries_via_client() {
    use nativelink_service::chunked_write_handler::{
        ChunkedWriteHandlerMetrics, PreparedChunk, admit_prepared_chunk,
    };
    use nativelink_store::chunked::chunked_driver::ChunkWork;

    const CHUNK: usize = 4 * 1024;
    // PinBudget cap: exactly ONE chunk. The first admission acquires
    // CHUNK bytes; the second's try_acquire(CHUNK) MUST return None
    // because the budget is empty. Box::leak yields the 'static
    // reference matching `admit_prepared_chunk`'s signature.
    let pin_budget: &'static PinBudget = Box::leak(Box::new(PinBudget::new(CHUNK)));
    let chunk_budget = make_test_budget();

    let blob_a = vec![0xa6u8; CHUNK];
    let blob_b = vec![0xb6u8; CHUNK];
    // Two distinct digests so the shape-validation in
    // admit_prepared_chunk doesn't reject as "wrong digest". Both
    // declared at exactly CHUNK bytes so the `finish=true` final-chunk
    // size check passes.
    let digest_a = DigestInfo::new(sha256(&blob_a), CHUNK as u64);
    let digest_b = DigestInfo::new(sha256(&blob_b), CHUNK as u64);

    // mpsc with a live (un-polled) receiver so the global ChunkBudget
    // acquire + try_send Ok branch both succeed for the FIRST chunk.
    // The receiver is held across both admissions so the second
    // chunk's try_send would succeed if PinBudget didn't reject first
    // (i.e. PinBudget is the EXCLUSIVE rejection source for the
    // second chunk; this isolates the seam under test).
    let (tx, _rx) = mpsc::channel::<ChunkWork>(16);
    let metrics = ChunkedWriteHandlerMetrics::default();

    // First admission: succeeds, consumes the PinBudget's entire
    // capacity (CHUNK bytes).
    let prepared_a = PreparedChunk {
        chunk_offset: 0,
        chunk_bytes: Bytes::from(blob_a.clone()),
        finish: true,
    };
    admit_prepared_chunk(
        prepared_a,
        &tx,
        chunk_budget,
        Some(pin_budget),
        CHUNK,
        digest_a,
        &metrics,
        None,
    )
    .expect("first admission must succeed (PinBudget has exactly CHUNK bytes available)");

    // Second admission: PinBudget is empty. MUST reject.
    let prepared_b = PreparedChunk {
        chunk_offset: 0,
        chunk_bytes: Bytes::from(blob_b.clone()),
        finish: true,
    };
    let err = tokio::time::timeout(Duration::from_secs(5), async {
        admit_prepared_chunk(
            prepared_b,
            &tx,
            chunk_budget,
            Some(pin_budget),
            CHUNK,
            digest_b,
            &metrics,
            None,
        )
    })
    .await
    .expect(
        "must not deadlock — second admit_prepared_chunk must reject promptly when PinBudget \
         is exhausted (#436)",
    )
    .expect_err(
        "second admission MUST reject with Err — PinBudget at zero cannot grant another permit; \
         if this returns Ok, the composite invariant is broken: gate active without \
         compensating eviction/pin/TTL (the PinBudget gate IS the pin corner; its failure to \
         fire would let pinned-bytes grow unboundedly)",
    );

    // ── Assertion (a): code == ResourceExhausted ────────────────────────
    assert_eq!(
        err.code,
        nativelink_error::Code::ResourceExhausted,
        "PinBudget exhaustion MUST be Code::ResourceExhausted — classify_retryable's \
         Code match arm at chunked_client.rs:529 keys on this exact code; got {err:?}"
    );

    // ── Assertion (b): the typed BackpressureSignal detail decodes to
    //    Reason::PinnedBytesExhausted with retry_after_ms == 100 ──────────
    let signal_any = err
        .details
        .iter()
        .find(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
        .expect(
            "PinBudget exhaustion MUST carry a BackpressureSignal detail at the wire-stable \
             type_url — without this discriminator, classify_retryable returns Abort and Bazel \
             never retries, breaking the composite invariant the measurement-first wiring \
             depends on (#436)",
        );
    let signal = BackpressureSignal::decode(&*signal_any.value)
        .expect("BackpressureSignal proto MUST decode (wire-format contract)");
    assert_eq!(
        signal.reason,
        backpressure_signal::Reason::PinnedBytesExhausted as i32,
        "rejection reason MUST be PinnedBytesExhausted (NOT GlobalChunkBudgetExhausted or \
         PerBlobMpscFull) — operators distinguish these three gate types from the reason \
         discriminator; got reason={}",
        signal.reason,
    );
    assert_eq!(
        signal.retry_after_ms, 100,
        "PinBudget retry_after_ms MUST be PIN_BUDGET_RETRY_AFTER_MS (100) — the prompt's \
         spec hint and the client's default backoff align around this value; got {}",
        signal.retry_after_ms,
    );

    // ── Assertion (c): the predicate pair that classify_retryable
    //    consults BOTH return true on this error. This proves the
    //    end-to-end seam from `admit_prepared_chunk` →
    //    `Error::resource_exhausted_backpressure` →
    //    `error_has_backpressure_signal` (chunked_client.rs:523) →
    //    `RetryDecision::Retry`. classify_retryable itself is private
    //    to nativelink-store; we cross the exact same seam by
    //    invoking the public predicates the classifier delegates to.
    //    The classifier-internal test
    //    `classify_resource_exhausted_with_backpressure_is_retry`
    //    (chunked_client.rs:921) closes the loop for the
    //    `RetryDecision::Retry` mapping itself. ──────────────────────────
    assert!(
        error_has_backpressure_signal(&err),
        "error_has_backpressure_signal MUST return true — this is the gate \
         classify_retryable (chunked_client.rs:523) uses to admit retryable errors; \
         if it returns false, Bazel sees the PinBudget rejection as a terminal \
         ResourceExhausted (Abort) and the composite invariant fails. \
         Error was: {err:?}",
    );
    assert!(
        error_has_backpressure_reason(
            &err,
            &[backpressure_signal::Reason::PinnedBytesExhausted],
        ),
        "error_has_backpressure_reason([PinnedBytesExhausted]) MUST return true — \
         downstream classifiers (e.g. FastSlowStore::run_producer's cache_tee demotion \
         at fast_slow_store.rs) use this exact predicate to dispatch on the typed \
         reason. A future refactor that switches the encoded reason to \
         MemoryStoreAtCapacity (wrong-but-similar discriminator) would silently \
         demote unrelated rejections; this assertion guards the contract. Error \
         was: {err:?}",
    );

    // Defensive: encode the same signal independently and confirm it
    // matches bit-identically — pins the encoder's stability across
    // refactors. encode_backpressure_signal_any is the one production
    // producer of this Any; if its output diverges from what
    // admit_prepared_chunk emitted, the test catches the drift.
    let expected_any = encode_backpressure_signal_any(
        backpressure_signal::Reason::PinnedBytesExhausted,
        100,
    );
    assert_eq!(
        signal_any.type_url, expected_any.type_url,
        "wire-stable type_url drift: admit_prepared_chunk emitted {} but \
         encode_backpressure_signal_any produced {}",
        signal_any.type_url, expected_any.type_url,
    );
    assert_eq!(
        signal_any.value, expected_any.value,
        "wire-format byte drift: admit_prepared_chunk and encode_backpressure_signal_any \
         must produce bit-identical bytes (else the classifier-side decode can mis-read)",
    );

    // ── Reverse-release check: the ChunkBudget permit acquired for the
    //    rejected admission MUST be released so the budget gauge returns
    //    to (TOTAL_CHUNK_PERMITS - 1) — only the SUCCESSFUL first
    //    admission's permit is still held inside the ChunkWork queued
    //    on `tx`. This pins the §13.1.1 step 2 reverse-release contract
    //    for the PinBudget rejection arm specifically. ─────────────────
    assert_eq!(
        chunk_budget.available_chunks(),
        TOTAL_CHUNK_PERMITS - 1,
        "ChunkBudget permit MUST be released on PinBudget rejection (reverse-release \
         per §13.1.1 step 2) — only the first admission's permit (held inside the \
         queued ChunkWork) remains acquired; got available_chunks={}",
        chunk_budget.available_chunks(),
    );

    // Metric ticked.
    assert!(
        metrics
            .pin_budget_exhausted_rejections_total
            .load(core::sync::atomic::Ordering::Relaxed)
            >= 1,
        "metric pin_budget_exhausted_rejections_total MUST tick on PinBudget rejection",
    );
}

/// #213 d-s-r MAJOR-1 fixup: when an upstream stream closes
/// mid-blob (`Ok(None)` before `finish_chunk`), the WriteChunked
/// handler must explicitly discard the in-flight partial via
/// `discard_chunked` BEFORE returning the upstream error. Without
/// this eager-GC trigger the partial accumulates on disk until the
/// next FilesystemStore::new sweep — a long-running server under
/// sustained client-disconnect storms would degrade the
/// `chunk_budget_used_bytes` Q4 budget monotonically.
///
/// This test exercises the upstream-disconnect path specifically:
///   1. Send chunk 0 successfully → admitted, driver writes the
///      partial file at `<temp>/d/<XX>/<digest>.partial`.
///   2. Drop the upstream sender WITHOUT sending `finish_chunk`.
///   3. The WriteChunked handler observes `Ok(None)` from
///      `stream.message()`, returns `Err(Code::Aborted)`.
///   4. Pre-fix: partial file persists on disk. Post-fix:
///      `discard_partial_best_effort` removes it before returning.
///
/// Production composition: real FilesystemStore (sharded layout, real
/// chunked_partials map, real adapter methods). Wrapped under 5s
/// `tokio::time::timeout` deadlock detector with SPECIFIC assertion
/// messages naming the contract.
///
/// Mutation step: comment out the `discard_partial_best_effort(...)`
/// call in the `Ok(None)` arm of WriteChunked's loop; the partial
/// persists and this test's assertion fires with the SPECIFIC
/// message naming the d-s-r MAJOR-1 contract.
#[nativelink_test]
async fn handler_upstream_drop_mid_blob_eagerly_discards_partial_on_disk() {
    use std::path::PathBuf;
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    let total: u64 = (N * CHUNK) as u64;
    // Two-chunk blob so chunk 0 has a successful admission + partial
    // write, then disconnect happens before chunk 1.
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa3u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let partial_path: PathBuf = store.partial_path_for_digest(&digest);

    // Send chunk 0 (NOT finish), wait for the partial file to appear
    // on disk AND the chunked_partials map to register the entry,
    // then drop the sender to simulate upstream disconnect.
    //
    // Both signals are required to avoid a race: `open_or_create_partial`
    // creates the on-disk file BEFORE inserting into the map, so a test
    // that waits only for the file would see "no in-flight state" in
    // discard_chunked and the file would linger (failing the assertion
    // for the WRONG reason).
    tokio::time::timeout(Duration::from_secs(5), async {
        let chunk0 = make_chunk(digest, 0, &blob[0..CHUNK], false);
        tx.send(frame_chunk(&chunk0)).await.unwrap();
        loop {
            if tokio::fs::metadata(&partial_path).await.is_ok()
                && store.has_in_flight_chunked_partial(&digest)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Now drop the sender → upstream observes Ok(None).
        drop(tx);
    })
    .await
    .expect("must not deadlock — chunk0 send + partial-file wait + sender drop");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must respond promptly to upstream disconnect")
        .expect("writer task must not panic");
    let status = result.expect_err("upstream disconnect mid-blob must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::Aborted,
        "upstream disconnect before finish_chunk must classify as Aborted; got {status:?}"
    );

    // Wait for the in-flight tracker to clear (handler's cleanup_guard
    // drop) so any post-Err discard has had a chance to run.
    nativelink_service::chunked_write_handler::wait_for_no_in_flight(
        &in_flight,
        Duration::from_secs(5),
    )
    .await
    .expect("must not deadlock — in-flight tracker must drain after handler returns Err");

    // Post-fix: the partial MUST be gone. Pre-fix: this assertion fires.
    let exists = tokio::fs::metadata(&partial_path).await.is_ok();
    assert!(
        !exists,
        "partial file MUST be GC'd by discard_partial_best_effort on upstream disconnect \
         (#213 d-s-r MAJOR-1) — without the eager GC, the partial persists until next \
         FilesystemStore::new sweep; checked path={}",
        partial_path.display(),
    );
}

// =============================================================================
// #213 reviewer M6 fixup: sibling-bug audit coverage for the d-s-r
// MAJOR-1 eager-GC contract. The original commit covered only the
// `Ok(None)` branch (`handler_upstream_drop_mid_blob_eagerly_discards_partial_on_disk`).
// Per CLAUDE.md sibling-audit rule, every other early-Err in the
// chunked_write_handler.rs:470-516 loop must also fire eager GC. The
// tests below cover the two highest-frequency siblings:
// - parse_digest-Err on a non-first chunk (M1 fix added a new GC site)
// - admit_chunk-Err on a non-first chunk (line 511, sha-256 mismatch)
//
// Both assertions are the SPECIFIC "partial MUST be GC'd" message
// naming the d-s-r MAJOR-1 contract; mutation steps comment out the
// respective `discard_partial_best_effort(...)` and confirm the
// assertion fires.
// =============================================================================

/// Sibling test for `chunked_write_handler.rs:501` `parse_digest`
/// (M1 fixup). Send chunk 0 (partial created on disk), then send a
/// malformed chunk with `digest=None` — `parse_digest` returns
/// InvalidArgument. The eager GC MUST fire and remove the partial
/// before the handler returns.
///
/// Production composition: real FilesystemStore (sharded layout, real
/// chunked_partials map, real adapter methods) wrapped under 5s
/// `tokio::time::timeout` deadlock detector with SPECIFIC assertion
/// messages.
///
/// Mutation step: comment out the `discard_partial_best_effort(...)`
/// call in the new `parse_digest` Err arm; this test's final
/// assertion fires with the SPECIFIC d-s-r MAJOR-1 message.
#[nativelink_test]
async fn handler_subsequent_chunk_parse_digest_err_eagerly_discards_partial() {
    use std::path::PathBuf;
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    let total: u64 = (N * CHUNK) as u64;
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa7u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let partial_path: PathBuf = store.partial_path_for_digest(&digest);

    tokio::time::timeout(Duration::from_secs(5), async {
        let chunk0 = make_chunk(digest, 0, &blob[0..CHUNK], false);
        tx.send(frame_chunk(&chunk0)).await.unwrap();
        // Wait for the partial to land on disk + map (race-free per
        // the comment in the d-s-r MAJOR-1 test above).
        loop {
            if tokio::fs::metadata(&partial_path).await.is_ok()
                && store.has_in_flight_chunked_partial(&digest)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Now send a chunk with `digest=None` — parse_digest returns
        // InvalidArgument. This drives chunked_write_handler.rs:501.
        let bad = WriteChunk {
            digest: None,
            chunk_offset: CHUNK as u64,
            chunk_bytes: Bytes::copy_from_slice(&blob[CHUNK..2 * CHUNK]),
            chunk_sha256: sha256(&blob[CHUNK..2 * CHUNK]).to_vec(),
            finish_chunk: true,
        };
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — chunk0 + malformed digest=None send");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject parse_digest-Err within 5s")
        .expect("writer task must not panic");
    let status = result.expect_err("parse_digest=None on subsequent chunk must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "missing-digest on subsequent chunk must classify as InvalidArgument; got {status:?}"
    );

    nativelink_service::chunked_write_handler::wait_for_no_in_flight(
        &in_flight,
        Duration::from_secs(5),
    )
    .await
    .expect("must not deadlock — in-flight tracker must drain after handler returns Err");

    let exists = tokio::fs::metadata(&partial_path).await.is_ok();
    assert!(
        !exists,
        "partial file MUST be GC'd by discard_partial_best_effort on parse_digest-Err \
         (#213 reviewer M1 sibling) — without the eager GC, the partial persists until next \
         FilesystemStore::new sweep; checked path={}",
        partial_path.display(),
    );
}

/// Sibling test for the subsequent-chunk `admit_chunk` Err arm in
/// `chunked_write_handler.rs::write_chunked_inner`. Send chunk 0
/// (partial created on disk), then send chunk 1 with WRONG
/// `chunk_sha256` — `admit_chunk` -> `verify_and_prepare_chunk`'s
/// SHA-256 verify fails with InvalidArgument. The eager GC MUST
/// fire and remove the partial before the handler returns.
///
/// Production composition: same shape as the parse_digest sibling.
///
/// Mutation step: comment out the `discard_partial_best_effort(...)`
/// call in the `if let Err(err) = self.admit_chunk(next, ...)` arm
/// (the subsequent-chunk admit_chunk Err arm — line numbers drift
/// with file edits; identify by the loop-body conditional matching
/// `next` not `first_chunk`); this test's final assertion fires with
/// the SPECIFIC d-s-r MAJOR-1 message.
#[nativelink_test]
async fn handler_subsequent_chunk_admit_err_eagerly_discards_partial() {
    use std::path::PathBuf;
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    let total: u64 = (N * CHUNK) as u64;
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xa8u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let partial_path: PathBuf = store.partial_path_for_digest(&digest);

    tokio::time::timeout(Duration::from_secs(5), async {
        let chunk0 = make_chunk(digest, 0, &blob[0..CHUNK], false);
        tx.send(frame_chunk(&chunk0)).await.unwrap();
        loop {
            if tokio::fs::metadata(&partial_path).await.is_ok()
                && store.has_in_flight_chunked_partial(&digest)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Send chunk 1 with WRONG sha256 → admit_chunk's SHA-256
        // verify fails (line 511 path).
        let mut bad = make_chunk(digest, CHUNK as u64, &blob[CHUNK..2 * CHUNK], true);
        bad.chunk_sha256 = vec![0xffu8; 32]; // intentionally wrong
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — chunk0 + bad-sha-chunk1 send");

    let result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("must not deadlock — handler must reject admit_chunk-Err within 5s")
        .expect("writer task must not panic");
    let status = result.expect_err("admit_chunk-Err on subsequent chunk must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "wrong-sha on subsequent chunk must classify as InvalidArgument; got {status:?}"
    );

    nativelink_service::chunked_write_handler::wait_for_no_in_flight(
        &in_flight,
        Duration::from_secs(5),
    )
    .await
    .expect("must not deadlock — in-flight tracker must drain after handler returns Err");

    let exists = tokio::fs::metadata(&partial_path).await.is_ok();
    assert!(
        !exists,
        "partial file MUST be GC'd by discard_partial_best_effort on admit_chunk-Err \
         on subsequent chunk (#213 d-s-r MAJOR-1 subsequent-chunk admit_chunk Err arm sibling) — \
         without the eager GC, the partial persists until next FilesystemStore::new sweep; \
         checked path={}",
        partial_path.display(),
    );
}

/// #213 reviewer round-2 MAJOR-B (M2 mutation test): asserts that
/// the handler's `discard_partial_best_effort` is bounded by
/// `tokio::time::timeout(DISCARD_PARTIAL_TIMEOUT, ...)`. Without
/// the wrap, a wedged slow tier (filesystem `discard_chunked` hung)
/// would hang the handler forever and the gRPC stream would stay
/// open — strictly worse than the pre-fix "partial persists" bug
/// because it wedges the upstream caller too.
///
/// **Wedge mechanism.** Registers a 30s `discard_chunked` delay via
/// the `set_test_pre_discard_delay_ms` test hook on FilesystemStore.
/// Triggers a malformed-subsequent-chunk path (digest=None) so the
/// handler hits `discard_partial_best_effort` on its way out. The
/// test then asserts the handler returns within 7s (5s wrap timeout
/// + 2s slop for chunked-driver wind-down + scheduler jitter).
///
/// Production composition: real FilesystemStore + real
/// ChunkedWriteHandler under a 7s `tokio::time::timeout` deadlock
/// detector with a SPECIFIC `must not deadlock — handler must
/// bound discard_chunked under wedge` assertion message.
///
/// Mutation step: revert the
/// `tokio::time::timeout(DISCARD_PARTIAL_TIMEOUT, ...)` wrap in
/// `discard_partial_best_effort` to a bare
/// `filesystem_store.discard_chunked(digest).await`; the wedge then
/// holds the handler for the full 30s and the SPECIFIC deadlock
/// assertion fires within 7s. (Reverted in checked-in code.)
#[nativelink_test]
async fn handler_bounds_discard_partial_under_wedged_slow_tier() {
    use std::path::PathBuf;
    const CHUNK: usize = 4 * 1024;
    const N: usize = 2;
    const WEDGE_MS: u64 = 30_000;
    const ASSERT_BOUND_SECS: u64 = 7;
    let total: u64 = (N * CHUNK) as u64;
    let mut blob = Vec::with_capacity(N * CHUNK);
    for i in 0..N {
        blob.extend(std::iter::repeat(0xb1u8 + i as u8).take(CHUNK));
    }
    let digest = DigestInfo::new(sha256(&blob), total);

    let (store, _content_path) = make_store().await;
    let budget = make_test_budget();
    let (handler, in_flight) = make_handler(Arc::clone(&store), budget, CHUNK);

    // Wedge `discard_chunked` for this digest. RAII guard so a panic
    // inside the test doesn't leak the entry across siblings.
    store.set_test_pre_discard_delay_ms(&digest, WEDGE_MS);
    struct ResetDiscardDelay {
        store: Arc<FilesystemStore<FileEntryImpl>>,
        digest: DigestInfo,
    }
    impl Drop for ResetDiscardDelay {
        fn drop(&mut self) {
            self.store.clear_test_pre_discard_delay(&self.digest);
        }
    }
    let _reset_guard = ResetDiscardDelay {
        store: Arc::clone(&store),
        digest,
    };

    let (tx, stream) = make_chunk_stream();
    let h = Arc::clone(&handler);
    let writer = tokio::spawn(async move { h.write_chunked(tonic::Request::new(stream)).await });

    let partial_path: PathBuf = store.partial_path_for_digest(&digest);

    // Send chunk 0 to seed the partial; wait for it to land.
    tokio::time::timeout(Duration::from_secs(5), async {
        let chunk0 = make_chunk(digest, 0, &blob[0..CHUNK], false);
        tx.send(frame_chunk(&chunk0)).await.unwrap();
        loop {
            if tokio::fs::metadata(&partial_path).await.is_ok()
                && store.has_in_flight_chunked_partial(&digest)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Send a malformed second chunk (digest=None) to force the
        // handler down the parse_digest-Err arm; that arm calls
        // `discard_partial_best_effort` on its way out — the bounded
        // wrap is what we're testing.
        let bad = WriteChunk {
            digest: None,
            chunk_offset: CHUNK as u64,
            chunk_bytes: Bytes::copy_from_slice(&blob[CHUNK..2 * CHUNK]),
            chunk_sha256: sha256(&blob[CHUNK..2 * CHUNK]).to_vec(),
            finish_chunk: true,
        };
        tx.send(frame_chunk(&bad)).await.unwrap();
        drop(tx);
    })
    .await
    .expect("must not deadlock — chunk0 + bad-digest send");

    // The contract under test: handler returns within 5s wrap + 2s
    // slop, even though `discard_chunked` is wedged for 30s. Without
    // the wrap, this fires within 7s and the test panics with the
    // bespoke message naming the contract.
    let result = tokio::time::timeout(Duration::from_secs(ASSERT_BOUND_SECS), writer)
        .await
        .expect(
            "must not deadlock — handler must bound discard_chunked under wedge \
             (#213 reviewer round-2 MAJOR-B M2 mutation guard)",
        )
        .expect("writer task must not panic");
    let status = result.expect_err("malformed subsequent chunk must return Err");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "bad-digest second chunk must classify as InvalidArgument; got {status:?}"
    );
}

/// #394/#413 Phase 1 falsification probe (pulse-burst hypothesis).
///
/// Drives 200 admission ATTEMPTS through `admit_prepared_chunk` on a
/// fresh `ProducerArrivalProbe` within a single 100ms window and
/// asserts the burst-detect `warn!` fired (`BURST_THRESHOLD_CHUNKS_
/// PER_WINDOW = 50`).
///
/// **Why N=200, not 60** (red-team 20260512 / assumption-auditor CLAIM 4
/// PARTIAL fix): on loaded CI a tight 60-call loop CAN cross 100ms
/// mid-loop. If that happens the window rolls over and
/// `attempts_in_window` resets to 1, then climbs back toward (but maybe
/// not past) 50 in the next window — the warn never fires and the test
/// flakes. With N=200, even a single mid-loop window rollover leaves the
/// second window with >100 attempts (well past the 50 threshold), so the
/// warn is guaranteed to fire on either side of any rollover.
///
/// **Production composition:** calls `admit_prepared_chunk` (the
/// production helper used by both the worker WriteChunked RPC and the
/// Bazel-facing dispatch path) with `Some(&mut probe)` — the same
/// shape `dispatch_chunks_to_driver` uses on the cascade-prone path.
/// The test covers the seam between probe state mutation and the
/// `warn!` emission inside `admit_prepared_chunk`'s try_send Ok arm.
///
/// **Mutation step:** in `chunked_write_handler.rs::ProducerArrivalProbe::record_attempt`,
/// comment out the line `if !self.warned_this_window && self.attempts_in_window
/// > BURST_THRESHOLD_CHUNKS_PER_WINDOW`. The probe never emits the warn
/// → `logs_contain` returns false → assertion fires the bespoke message
/// "Phase 1 probe never warned — burst-detect gate stripped or threshold
/// raised" (referring to 200 admissions, not 60).
#[nativelink_test]
async fn producer_arrival_probe_warns_on_burst_above_threshold() {
    use nativelink_service::chunked_write_handler::{
        ChunkedWriteHandlerMetrics, PreparedChunk, ProducerArrivalProbe,
        admit_prepared_chunk,
    };
    use nativelink_store::chunked::chunked_driver::ChunkWork;

    const CHUNK: usize = 4 * 1024;
    // 200 admissions >> BURST_THRESHOLD_CHUNKS_PER_WINDOW (50); robust
    // against one mid-loop window rollover on loaded CI (see fn-level
    // doc). The declared digest size covers 200 chunks; FINAL chunk
    // would carry `finish=true` but we keep `finish=false` on all 200
    // (the probe doesn't care about finish — only the offset shape
    // needs to be valid for `admit_prepared_chunk`'s shape gate).
    const N: usize = 200;
    let total = (N * CHUNK + 1) as u64; // +1 ensures non-final chunks
    let blob_chunk = vec![0xc7u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob_chunk), total);
    let budget = make_test_budget();

    // Live receiver, never polled — we want try_send to land Ok 200
    // times without the receiver draining. The PER_BLOB_MPSC_CAP=256
    // channel can hold all 200 ChunkWork values; if PER_BLOB_MPSC_CAP
    // ever shrinks below N, the test will fail loudly at `expect`. Hold
    // _rx alive to keep the channel open (Closed would be a different
    // rejection arm).
    let (tx, _rx) = mpsc::channel::<ChunkWork>(PER_BLOB_MPSC_CAP);

    let metrics = ChunkedWriteHandlerMetrics::default();
    let mut probe = ProducerArrivalProbe::default();

    // Fire 200 admissions in tight succession. Each call is a few
    // microseconds of validation + a non-blocking try_send, so even on
    // a slow CI host the cumulative count crosses
    // BURST_THRESHOLD_CHUNKS_PER_WINDOW=50 well inside a single window;
    // and with N=200, even a window rollover mid-loop leaves the
    // second window with enough attempts to cross the threshold again.
    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let prepared = PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(blob_chunk.clone()),
                finish: false,
            };
            admit_prepared_chunk(
                prepared,
                &tx,
                budget,
                None,
                CHUNK,
                digest,
                &metrics,
                Some(&mut probe),
            )
            .expect("each admission must succeed (cap=256, fresh budget)");
        }
    })
    .await
    .expect(
        "must not deadlock — 200 non-blocking admissions on a 256-cap \
         mpsc must complete promptly",
    );

    // The contract under test: ProducerArrivalProbe::record_attempt
    // emits a `warn!` once per window when attempts_in_window crosses
    // BURST_THRESHOLD_CHUNKS_PER_WINDOW=50. The bespoke marker
    // "pulse-burst signature (#394/#413 Phase 1 probe)" is unique to
    // the probe's burst-detect arm and not produced by any other
    // log site.
    assert!(
        logs_contain("pulse-burst signature (#394/#413 Phase 1 probe)"),
        "Phase 1 probe never warned — burst-detect gate stripped or \
         threshold raised. Drove {N} admissions in <100ms (well above \
         the 50-attempt threshold); the `record_attempt` warn arm in \
         `chunked_write_handler.rs` must have fired."
    );
}

/// #394/#413 Phase 1 falsification probe — asymmetric-contract
/// coverage (red-team 20260512 + CLAUDE.md "Asymmetric contract
/// coverage" discipline).
///
/// **Over-action guard:** the warn arm must NOT fire when
/// `attempts_in_window` stays at or below
/// `BURST_THRESHOLD_CHUNKS_PER_WINDOW = 50`. The under-action direction
/// is tested by `producer_arrival_probe_warns_on_burst_above_threshold`
/// (fires when above); this test asserts the over-action direction:
/// if a healthy stream sends only ~40 chunks per 100ms window, the
/// operator must NOT see a spurious "pulse-burst" warn that would
/// trigger an investigation of a non-existent burst.
///
/// **Production composition:** identical to the under-action test —
/// calls `admit_prepared_chunk` with `Some(&mut probe)` so the test
/// crosses the same seam (probe state mutation → warn emission) as
/// `dispatch_chunks_to_driver`.
///
/// **Mutation step:** in `chunked_write_handler.rs`, change
/// `BURST_THRESHOLD_CHUNKS_PER_WINDOW: u32 = 50` to
/// `BURST_THRESHOLD_CHUNKS_PER_WINDOW: u32 = 0` (always-fire). With
/// the threshold at 0, even one admission crosses it → warn fires →
/// `logs_contain` returns true → assertion fires the bespoke message
/// "Phase 1 probe fired below threshold".
#[nativelink_test]
async fn producer_arrival_probe_quiet_below_threshold() {
    use nativelink_service::chunked_write_handler::{
        ChunkedWriteHandlerMetrics, PreparedChunk, ProducerArrivalProbe,
        admit_prepared_chunk,
    };
    use nativelink_store::chunked::chunked_driver::ChunkWork;

    const CHUNK: usize = 4 * 1024;
    // 40 admissions < BURST_THRESHOLD_CHUNKS_PER_WINDOW (50). This is
    // the healthy-stream regime: producer arrival comfortably below the
    // pulse-burst threshold. The warn must stay silent.
    const N: usize = 40;
    let total = (N * CHUNK + 1) as u64; // non-final chunks throughout
    let blob_chunk = vec![0xb1u8; CHUNK];
    let digest = DigestInfo::new(sha256(&blob_chunk), total);
    let budget = make_test_budget();

    let (tx, _rx) = mpsc::channel::<ChunkWork>(PER_BLOB_MPSC_CAP);

    let metrics = ChunkedWriteHandlerMetrics::default();
    let mut probe = ProducerArrivalProbe::default();

    tokio::time::timeout(Duration::from_secs(5), async {
        for i in 0..N {
            let prepared = PreparedChunk {
                chunk_offset: (i * CHUNK) as u64,
                chunk_bytes: Bytes::from(blob_chunk.clone()),
                finish: false,
            };
            admit_prepared_chunk(
                prepared,
                &tx,
                budget,
                None,
                CHUNK,
                digest,
                &metrics,
                Some(&mut probe),
            )
            .expect("each admission must succeed (cap=256, fresh budget)");
        }
    })
    .await
    .expect(
        "must not deadlock — 40 non-blocking admissions on a 256-cap \
         mpsc must complete promptly",
    );

    // The contract under test: ProducerArrivalProbe::record_attempt
    // does NOT emit the pulse-burst warn while attempts stay at or
    // below BURST_THRESHOLD_CHUNKS_PER_WINDOW=50. If this fires it
    // means the threshold was lowered or the gate stripped — false-
    // alarm hazard for operators triaging healthy streams.
    assert!(
        !logs_contain("pulse-burst signature (#394/#413 Phase 1 probe)"),
        "Phase 1 probe fired below threshold — the burst-detect arm \
         emitted a `warn!` after only {N} admissions (BURST_THRESHOLD_\
         CHUNKS_PER_WINDOW=50). Either the threshold was lowered or the \
         gate was stripped; healthy streams must not produce pulse-burst \
         warns."
    );
}
