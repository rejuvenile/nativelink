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

//! #212 Phase 2.4: production-composition tests for the worker-side
//! `WriteChunked` client (`chunked::chunked_client`).
//!
//! Tests use a fake `WriteChunkedDispatcher` that captures the
//! chunks sent and returns scripted responses (success / Aborted+
//! signal / ResourceExhausted+signal). This isolates the
//! chunked-client logic from tonic transport machinery while
//! exercising:
//! - per-chunk SHA-256 computation + transmission
//! - chunk count + offset alignment
//! - retry on Aborted+BackpressureSignal (concurrent same-digest)
//! - retry on ResourceExhausted+BackpressureSignal (Q8 budget)
//! - non-retry on bare ResourceExhausted (legacy h2 dead-channel)
//! - giving up after `max_attempts` exhausted
//!
//! Per CLAUDE.md test discipline:
//! - Every async test wrapped in `tokio::time::timeout(5s)` for
//!   deadlock detection.
//! - Specific assertion messages on every `expect()`.
//! - Mutation-step verification documented inline; the
//!   per-test-fail mutation reproduces in the inline `tests/` of
//!   `chunked_client.rs` (the unit-test layer); this integration
//!   layer asserts the composed behavior.

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk, WriteChunkedResponse, backpressure_signal,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunked_client::{
    ChunkedClientMetrics, ChunkedClientOptions, DispatchFuture,
    WriteChunkedDispatcher, write_chunked_stream,
};
use nativelink_store::chunked_signal::encode_backpressure_signal_any;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use sha2::{Digest as _, Sha256};

// ---------------------------------------------------------------------------
// Fake dispatcher: scripted responses + chunk capture
// ---------------------------------------------------------------------------

/// A fake `WriteChunkedDispatcher` that:
/// - captures every chunk batch handed to `dispatch`
/// - returns the next scripted result from a queue per attempt
///
/// The scripted-results queue defines the per-attempt behavior;
/// `attempts_so_far` exposes the count for assertions.
struct FakeDispatcher {
    /// Per-attempt scripted result. Pop-front per call. If empty,
    /// returns a default `Internal` error (test misconfiguration).
    scripted: Mutex<Vec<Result<WriteChunkedResponse, Error>>>,
    /// Captured chunk batches: one Vec<WriteChunk> per dispatch
    /// call. Used to assert "we sent these N chunks in this order
    /// with these per-chunk SHA-256 values."
    captured: Mutex<Vec<Vec<WriteChunk>>>,
    /// Counter for assertions.
    attempts_so_far: AtomicU32,
}

impl FakeDispatcher {
    fn new(scripted: Vec<Result<WriteChunkedResponse, Error>>) -> Arc<Self> {
        Arc::new(Self {
            scripted: Mutex::new(scripted),
            captured: Mutex::new(Vec::new()),
            attempts_so_far: AtomicU32::new(0),
        })
    }

    fn attempts(&self) -> u32 {
        self.attempts_so_far.load(Ordering::Relaxed)
    }

    fn first_attempt_chunks(&self) -> Vec<WriteChunk> {
        self.captured
            .lock()
            .unwrap()
            .first()
            .cloned()
            .unwrap_or_default()
    }
}

impl WriteChunkedDispatcher for FakeDispatcher {
    fn dispatch(&self, chunks: Vec<WriteChunk>) -> DispatchFuture {
        self.attempts_so_far.fetch_add(1, Ordering::Relaxed);
        self.captured.lock().unwrap().push(chunks);
        let next = self
            .scripted
            .lock()
            .unwrap()
            .drain(..1)
            .next()
            .unwrap_or_else(|| {
                Err(nativelink_error::make_err!(
                    Code::Internal,
                    "FakeDispatcher: no scripted result for this attempt"
                ))
            });
        Box::pin(async move { next })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

/// Build (digest, payload) for a synthetic blob of `n` bytes
/// where each byte is `(i % 256) as u8`.
fn synth_blob(n: usize) -> (DigestInfo, Vec<u8>) {
    let blob: Vec<u8> = (0..n).map(|i| (i & 0xff) as u8).collect();
    let digest = DigestInfo::new(sha256(&blob), n as u64);
    (digest, blob)
}

/// Push the entire blob into a fresh `DropCloserWriteHalf` and
/// return the read half. EOFs cleanly at the end so the chunked
/// client's `recv()` loop terminates.
fn reader_for_blob(blob: Vec<u8>) -> nativelink_util::buf_channel::DropCloserReadHalf {
    let (mut tx, rx) = make_buf_channel_pair();
    tokio::spawn(async move {
        drop(tx.send(Bytes::from(blob)).await);
        drop(tx.send_eof());
    });
    rx
}

fn ok_response(committed_size: u64) -> WriteChunkedResponse {
    WriteChunkedResponse {
        committed_digest: None,
        committed_size,
    }
}

// ---------------------------------------------------------------------------
// Production-composition tests
// ---------------------------------------------------------------------------

/// Single attempt, single full chunk, server returns Ok.
/// Assertions: chunks.len()==1, finish_chunk on the only chunk,
/// per-chunk SHA-256 matches the bytes, metrics counters increment.
#[nativelink_test]
async fn end_to_end_one_chunk_blob_succeeds() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let chunk_size = N; // single-chunk per attempt
    let dispatcher = FakeDispatcher::new(vec![Ok(ok_response(N as u64))]);
    let metrics = ChunkedClientMetrics::new();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob.clone()),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock — single-chunk blob must finish promptly")
    .expect("single-chunk happy path must succeed");

    assert_eq!(result, N as u64, "committed size must equal blob length");
    assert_eq!(dispatcher.attempts(), 1, "exactly one attempt on success");
    let captured = dispatcher.first_attempt_chunks();
    assert_eq!(captured.len(), 1, "single chunk for {N}-byte blob");
    assert!(
        captured[0].finish_chunk,
        "single chunk must carry finish_chunk=true"
    );
    assert_eq!(captured[0].chunk_offset, 0);
    assert_eq!(captured[0].chunk_bytes.len(), N);
    assert_eq!(
        captured[0].chunk_sha256,
        sha256(&blob).to_vec(),
        "chunk_sha256 must match the chunk bytes"
    );

    assert_eq!(metrics.attempted_total.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics.bytes_sent_total.load(Ordering::Relaxed),
        N as u64
    );
}

/// Multi-chunk aligned blob: 4 full chunks of 4 KiB each, server
/// returns Ok. Assertions: 4 chunks in order with offsets 0/4K/8K/12K
/// and finish_chunk=true ONLY on the last.
#[nativelink_test]
async fn end_to_end_4_chunk_aligned_blob_succeeds() {
    const CHUNK: usize = 4 * 1024;
    const N_CHUNKS: usize = 4;
    let total = CHUNK * N_CHUNKS;
    let (digest, blob) = synth_blob(total);

    let dispatcher = FakeDispatcher::new(vec![Ok(ok_response(total as u64))]);
    let metrics = ChunkedClientMetrics::new();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob.clone()),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: CHUNK,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock — 4-chunk happy path")
    .expect("happy path must succeed");

    assert_eq!(result, total as u64);
    let chunks = dispatcher.first_attempt_chunks();
    assert_eq!(chunks.len(), N_CHUNKS, "expected exactly {N_CHUNKS} chunks");
    for (i, c) in chunks.iter().enumerate() {
        assert_eq!(
            c.chunk_offset,
            (i * CHUNK) as u64,
            "chunk[{i}] offset must be {} bytes",
            i * CHUNK
        );
        assert_eq!(c.chunk_bytes.len(), CHUNK);
        assert_eq!(c.finish_chunk, i == N_CHUNKS - 1);
        let expected_sha = sha256(&blob[i * CHUNK..(i + 1) * CHUNK]);
        assert_eq!(
            c.chunk_sha256,
            expected_sha.to_vec(),
            "chunk[{i}] sha256 must match its bytes"
        );
    }
}

/// Unaligned blob: 2.5 chunks → 2 full + 1 partial. Final chunk
/// is smaller than chunk_size and carries finish_chunk=true.
#[nativelink_test]
async fn end_to_end_unaligned_blob_final_chunk_partial() {
    const CHUNK: usize = 1024;
    let total = CHUNK * 2 + 17; // 2 full + 1 partial of 17
    let (digest, blob) = synth_blob(total);

    let dispatcher = FakeDispatcher::new(vec![Ok(ok_response(total as u64))]);
    let metrics = ChunkedClientMetrics::new();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob.clone()),
            ChunkedClientOptions {
                max_attempts: 1,
                chunk_size: CHUNK,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock — partial-final-chunk happy path")
    .expect("happy path must succeed");

    assert_eq!(result, total as u64);
    let chunks = dispatcher.first_attempt_chunks();
    assert_eq!(chunks.len(), 3, "two full + one partial");
    assert_eq!(chunks[0].chunk_bytes.len(), CHUNK);
    assert_eq!(chunks[1].chunk_bytes.len(), CHUNK);
    assert_eq!(chunks[2].chunk_bytes.len(), 17);
    assert!(!chunks[0].finish_chunk);
    assert!(!chunks[1].finish_chunk);
    assert!(chunks[2].finish_chunk, "partial final must carry finish");
}

/// Server returns Aborted+BackpressureSignal on attempt 1; client
/// retries and succeeds on attempt 2. Honors `retry_after_ms` hint.
#[nativelink_test]
async fn retry_on_aborted_with_backpressure_signal() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let aborted = {
        let detail = encode_backpressure_signal_any(
            backpressure_signal::Reason::PerBlobMpscFull,
            10, // 10 ms — short enough for test
        );
        Err(Error::aborted_with_detail("concurrent stream", detail))
    };
    let dispatcher = FakeDispatcher::new(vec![aborted, Ok(ok_response(N as u64))]);
    let metrics = ChunkedClientMetrics::new();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock — retry path within 5s")
    .expect("retry must eventually succeed");

    assert_eq!(result, N as u64);
    assert_eq!(
        dispatcher.attempts(),
        2,
        "exactly 2 attempts: 1 Aborted + 1 success"
    );
    assert_eq!(
        metrics.aborted_retried_total.load(Ordering::Relaxed),
        1,
        "one Aborted+signal rejection counted"
    );
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 1);
}

/// Server returns ResourceExhausted+BackpressureSignal on attempt
/// 1; client retries and succeeds on attempt 2.
#[nativelink_test]
async fn retry_on_resource_exhausted_with_backpressure_signal() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let backpressure = {
        let detail = encode_backpressure_signal_any(
            backpressure_signal::Reason::GlobalChunkBudgetExhausted,
            5,
        );
        Err(Error::resource_exhausted_backpressure(
            "global budget exhausted",
            detail,
        ))
    };
    let dispatcher =
        FakeDispatcher::new(vec![backpressure, Ok(ok_response(N as u64))]);
    let metrics = ChunkedClientMetrics::new();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock")
    .expect("retry on RE+signal must succeed");

    assert_eq!(result, N as u64);
    assert_eq!(dispatcher.attempts(), 2);
    assert_eq!(
        metrics.resource_exhausted_total.load(Ordering::Relaxed),
        1
    );
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 1);
}

/// Server returns BARE ResourceExhausted (no signal) — must NOT
/// retry. The legacy h2 dead-channel shape is per design §13.1.1
/// point 2; producer that retries here would mask the transport
/// issue.
#[nativelink_test]
async fn no_retry_on_bare_resource_exhausted() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let bare_re = Err(nativelink_error::make_err!(
        Code::ResourceExhausted,
        "bare resource exhausted (h2 ENHANCE_YOUR_CALM)"
    ));
    let dispatcher = FakeDispatcher::new(vec![bare_re]);
    let metrics = ChunkedClientMetrics::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock — bare RE must propagate immediately")
    .expect_err("bare ResourceExhausted must propagate as Err");

    assert_eq!(err.code, Code::ResourceExhausted);
    assert_eq!(
        dispatcher.attempts(),
        1,
        "must NOT retry on bare ResourceExhausted"
    );
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics.resource_exhausted_total.load(Ordering::Relaxed),
        0,
        "bare RE counts as abort, not as backpressure-retry"
    );
}

/// All `max_attempts` fail with Aborted+signal → client gives up
/// and propagates the last Err.
#[nativelink_test]
async fn give_up_after_max_attempts_exhausted() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let make_aborted = || {
        let detail =
            encode_backpressure_signal_any(backpressure_signal::Reason::PerBlobMpscFull, 1);
        Err(Error::aborted_with_detail("concurrent", detail))
    };
    let dispatcher = FakeDispatcher::new(vec![
        make_aborted(),
        make_aborted(),
        make_aborted(),
    ]);
    let metrics = ChunkedClientMetrics::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock")
    .expect_err("3-of-3 Aborted attempts must propagate Err");

    assert_eq!(err.code, Code::Aborted);
    assert_eq!(dispatcher.attempts(), 3);
    assert_eq!(metrics.aborted_retried_total.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 0);
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("gave up after 3 attempts")),
        "must surface retry-budget exhaustion message; got {:?}",
        err.messages
    );
}

/// Server returns committed_size != declared digest size →
/// Internal error. Defends against silent corruption where the
/// server miscounts and the producer would otherwise treat the
/// blob as durable. Anti-#106 receive-side check.
#[nativelink_test]
async fn server_size_mismatch_returns_internal_error() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let bad_response = ok_response((N - 100) as u64); // server returned wrong size
    let dispatcher = FakeDispatcher::new(vec![Ok(bad_response)]);
    let metrics = ChunkedClientMetrics::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 1,
                chunk_size: N,
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock")
    .expect_err("size-mismatch must propagate Err");

    assert_eq!(err.code, Code::Internal);
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("refusing to treat as durable")),
        "must surface size-mismatch message; got {:?}",
        err.messages
    );
}

/// Anti-#203 invariant test (dispatcher-level): the dispatcher's
/// future is the ONLY thing held across the wire; once we drop it,
/// the caller's reader can be terminated freely. This test
/// constructs a custom dispatcher whose dispatch holds an external
/// "released" notify, asserts that `write_chunked_stream` returns
/// EXACTLY when dispatch resolves, and that the reader passed to
/// the chunked client is consumed by-move (no borrowed reference
/// outlives the call).
#[nativelink_test]
async fn anti_203_no_borrowed_reader_held_across_rpc() {
    use tokio::sync::Notify;

    /// Dispatcher that resolves only when its `released` Notify
    /// fires. Lets the test verify "the function returns the moment
    /// the dispatcher's future resolves, NOT before."
    struct GatedDispatcher {
        released: Arc<Notify>,
        size: u64,
    }
    impl WriteChunkedDispatcher for GatedDispatcher {
        fn dispatch(&self, _chunks: Vec<WriteChunk>) -> DispatchFuture {
            let released = Arc::clone(&self.released);
            let size = self.size;
            Box::pin(async move {
                released.notified().await;
                Ok(WriteChunkedResponse {
                    committed_digest: None,
                    committed_size: size,
                })
            })
        }
    }

    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let released = Arc::new(Notify::new());
    let dispatcher = Arc::new(GatedDispatcher {
        released: Arc::clone(&released),
        size: N as u64,
    });

    let metrics = ChunkedClientMetrics::new();
    let dispatcher_for_call: Arc<dyn WriteChunkedDispatcher> =
        Arc::clone(&dispatcher) as Arc<dyn WriteChunkedDispatcher>;
    let call = tokio::spawn(async move {
        write_chunked_stream(
            dispatcher_for_call.as_ref(),
            digest,
            reader_for_blob(blob),
            ChunkedClientOptions {
                max_attempts: 1,
                chunk_size: N,
            },
            metrics,
        )
        .await
    });

    // Give the chunked-client time to enter the dispatcher (which
    // is now waiting on `released`). With no timeout below the test
    // would hang — bound it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !call.is_finished(),
        "client should still be waiting on dispatcher's gated future"
    );

    // Releasing the gate unblocks the dispatcher; `call` must
    // resolve promptly. The 5s deadlock-detector applies.
    released.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("must not deadlock — call must resolve once dispatcher resolves")
        .expect("call task must not panic")
        .expect("happy path must succeed");
    assert_eq!(result, N as u64);
}

/// 1-byte blob → exactly 1 chunk of 1 byte with finish_chunk=true.
/// Defends against off-by-one in chunk-buffering for tiny blobs.
#[nativelink_test]
async fn end_to_end_one_byte_blob_succeeds() {
    let blob = vec![0x42u8];
    let digest = DigestInfo::new(sha256(&blob), 1);
    let dispatcher = FakeDispatcher::new(vec![Ok(ok_response(1))]);
    let metrics = ChunkedClientMetrics::new();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        write_chunked_stream(
            dispatcher.as_ref(),
            digest,
            reader_for_blob(blob.clone()),
            ChunkedClientOptions {
                max_attempts: 1,
                chunk_size: CHUNK_SIZE, // production chunk size; 1-byte will be a single tiny chunk
            },
            Arc::clone(&metrics),
        ),
    )
    .await
    .expect("must not deadlock")
    .expect("1-byte blob must succeed");

    assert_eq!(result, 1);
    let chunks = dispatcher.first_attempt_chunks();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].chunk_bytes.as_ref(), &[0x42u8][..]);
    assert!(chunks[0].finish_chunk);
    assert_eq!(chunks[0].chunk_offset, 0);
}

/// Erase Pin<&> to keep the compiler happy with the boxed dispatcher
/// pointer arithmetic. Not strictly needed in tests; kept for future
/// expansion if any test wants to hand the dispatcher to a sub-task.
#[allow(dead_code)]
fn _phantom_pin_use(_: Pin<&dyn WriteChunkedDispatcher>) {}
