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
use std::sync::Arc;

use bytes::Bytes;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    WriteChunk, WriteChunkedResponse, backpressure_signal,
};
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunked_client::{
    ChunkedClientMetrics, ChunkedClientOptions, DispatchFuture, WriteChunkedDispatcher,
    write_chunked_stream,
};
use nativelink_store::chunked_signal::encode_backpressure_signal_any;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
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
        self.captured.lock().first().cloned().unwrap_or_default()
    }
}

impl WriteChunkedDispatcher for FakeDispatcher {
    fn dispatch(&self, chunks: Vec<WriteChunk>) -> DispatchFuture {
        self.attempts_so_far.fetch_add(1, Ordering::Relaxed);
        self.captured.lock().push(chunks);
        let next = self.scripted.lock().drain(..1).next().unwrap_or_else(|| {
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
    assert_eq!(metrics.bytes_sent_total.load(Ordering::Relaxed), N as u64);
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
    let dispatcher = FakeDispatcher::new(vec![backpressure, Ok(ok_response(N as u64))]);
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
    assert_eq!(metrics.resource_exhausted_total.load(Ordering::Relaxed), 1);
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
    let dispatcher = FakeDispatcher::new(vec![make_aborted(), make_aborted(), make_aborted()]);
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

/// Anti-#203 invariant test (dispatcher-level): assert that
/// `write_chunked_stream` does NOT resolve while the dispatcher
/// future is still pending — i.e. the function does not detach the
/// RPC and return early, and does not hold any awaitable in front
/// of the dispatcher. The borrowed-reader half of the anti-#203
/// guarantee is enforced by the type system (the function takes
/// `mut reader: DropCloserReadHalf` by-MOVE on signature line 312
/// of `chunked_client.rs`, not `&mut`); this test exercises the
/// other half (no premature resolve).
///
/// Sync primitives: `entered` (fired by the dispatcher on entry;
/// proves the chunked-client has reached the dispatch boundary)
/// and `released` (fired by the test to release the dispatcher
/// future). Per CLAUDE.md test discipline, NO `tokio::time::sleep`
/// is used for synchronization; mutation step: comment out the
/// `entered.notify_one()` call below — the test then waits forever
/// on `entered.notified()` and fails the 5s outer timeout with the
/// "must not deadlock — dispatcher must enter before assertion"
/// message.
#[nativelink_test]
async fn anti_203_call_resolves_iff_dispatcher_resolves() {
    use tokio::sync::Notify;

    /// Dispatcher that fires `entered` on entry and waits on
    /// `released` before returning. Lets the test verify (a) the
    /// chunked-client reached the dispatch boundary AND (b) the
    /// function returns the moment the dispatcher's future
    /// resolves, NOT before.
    struct GatedDispatcher {
        entered: Arc<Notify>,
        released: Arc<Notify>,
        size: u64,
    }
    impl WriteChunkedDispatcher for GatedDispatcher {
        fn dispatch(&self, _chunks: Vec<WriteChunk>) -> DispatchFuture {
            let entered = Arc::clone(&self.entered);
            let released = Arc::clone(&self.released);
            let size = self.size;
            Box::pin(async move {
                entered.notify_one();
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
    let entered = Arc::new(Notify::new());
    let released = Arc::new(Notify::new());
    let dispatcher = Arc::new(GatedDispatcher {
        entered: Arc::clone(&entered),
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

    // Wait for the dispatcher to enter (proves the chunked-client
    // is now inside `dispatch().await` — no `sleep` race window).
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("must not deadlock — dispatcher must enter before assertion");
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

/// Upfront-buffering regression test. Locks in the v1 contract that
/// `write_chunked_stream` reads the ENTIRE payload into prepared
/// chunks BEFORE handing the first chunk to the dispatcher. Without
/// this property, retries cannot resend from a single-pass
/// `DropCloserReadHalf` (the buf_channel is non-rewindable). A future
/// streaming-retry refactor (Phase 2.5+) that lifts the
/// `MAX_CHUNKED_BLOB_SIZE` cap will need to flip this property
/// intentionally; today, accidentally relaxing it would silently
/// break the retry path under load.
///
/// Mechanism: stage two chunks of bytes into the reader without
/// sending EOF. Race the chunked-client task against a `Notify` the
/// dispatcher fires on entry. Assert that
/// (a) `dispatch` is NEVER called while the reader is mid-stream, and
/// (b) once EOF arrives, dispatch DOES fire and the call resolves.
///
/// Mutation step (manual): change `collect_and_hash_chunks` to start
/// emitting chunks as soon as one is full (i.e. interleave dispatch
/// with reader recv); this test must red-fail at the
/// `dispatch_called_before_eof must be false` assertion.
#[nativelink_test]
async fn upfront_buffering_dispatch_waits_for_eof() {
    use tokio::sync::Notify;

    /// Dispatcher that fires `entered` on entry and immediately
    /// returns Ok. The test uses `entered` to detect any dispatch
    /// call — we expect ZERO calls before EOF reaches the reader.
    struct EntryNotifyDispatcher {
        entered: Arc<Notify>,
        committed: u64,
    }
    impl WriteChunkedDispatcher for EntryNotifyDispatcher {
        fn dispatch(&self, _chunks: Vec<WriteChunk>) -> DispatchFuture {
            let entered = Arc::clone(&self.entered);
            let committed = self.committed;
            Box::pin(async move {
                entered.notify_one();
                Ok(WriteChunkedResponse {
                    committed_digest: None,
                    committed_size: committed,
                })
            })
        }
    }

    const CHUNK: usize = 1024;
    const N_CHUNKS: usize = 3;
    let total = CHUNK * N_CHUNKS;
    let (digest, blob) = synth_blob(total);

    let entered = Arc::new(Notify::new());
    let dispatcher = Arc::new(EntryNotifyDispatcher {
        entered: Arc::clone(&entered),
        committed: total as u64,
    });
    let dispatcher_for_call: Arc<dyn WriteChunkedDispatcher> =
        Arc::clone(&dispatcher) as Arc<dyn WriteChunkedDispatcher>;

    // Build a reader that emits chunk 1 + chunk 2 immediately but
    // withholds chunk 3 + EOF until we say go.
    let (mut tx, rx) = nativelink_util::buf_channel::make_buf_channel_pair();
    let go = Arc::new(Notify::new());
    let go_for_send = Arc::clone(&go);
    let blob_arc = blob.clone();
    let send_task = tokio::spawn(async move {
        let first_two = Bytes::from(blob_arc[..(CHUNK * 2)].to_vec());
        drop(tx.send(first_two).await);
        // Wait for the test to release us before sending the final
        // chunk + EOF.
        go_for_send.notified().await;
        let last = Bytes::from(blob_arc[(CHUNK * 2)..].to_vec());
        drop(tx.send(last).await);
        drop(tx.send_eof());
    });

    let metrics = ChunkedClientMetrics::new();
    let call = tokio::spawn(async move {
        write_chunked_stream(
            dispatcher_for_call.as_ref(),
            digest,
            rx,
            ChunkedClientOptions {
                max_attempts: 1,
                chunk_size: CHUNK,
            },
            metrics,
        )
        .await
    });

    // Try to observe `dispatch` firing before EOF. We poll for ~250
    // ms via a bounded `select!` — if dispatch fires (entered
    // notified) within that window without EOF having been sent, the
    // upfront-buffering contract is broken.
    let early_dispatch = tokio::time::timeout(Duration::from_millis(250), entered.notified()).await;
    assert!(
        early_dispatch.is_err(),
        "dispatch_called_before_eof must be false — chunked client must \
         buffer the WHOLE payload before invoking dispatch (v1 retry-safety \
         contract); a Phase 2.5+ streaming-retry refactor must flip this \
         test deliberately"
    );
    assert!(
        !call.is_finished(),
        "call must still be pending while reader is mid-stream"
    );

    // Release the reader; dispatch should now fire and the call
    // should resolve promptly.
    go.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("must not deadlock — call must resolve once reader hits EOF")
        .expect("call task must not panic")
        .expect("happy path must succeed once EOF arrives");
    assert_eq!(result, total as u64);
    drop(send_task.await);
}

/// Retry-budget twin for ResourceExhausted. The Aborted twin is
/// `give_up_after_max_attempts_exhausted`; this asserts the symmetric
/// path through `metrics.resource_exhausted_total`. Without it, a
/// future refactor that decoupled the two retry classifications
/// (e.g., per-class retry budgets) could silently break only the RE
/// half — invisible at the symmetric-aborted unit boundary.
#[nativelink_test]
async fn give_up_after_max_attempts_exhausted_resource_exhausted() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let make_re = || {
        let detail = encode_backpressure_signal_any(
            backpressure_signal::Reason::GlobalChunkBudgetExhausted,
            1,
        );
        Err(Error::resource_exhausted_backpressure(
            "global budget exhausted",
            detail,
        ))
    };
    let dispatcher = FakeDispatcher::new(vec![make_re(), make_re(), make_re()]);
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
    .expect_err("3-of-3 RE+signal attempts must propagate Err");

    assert_eq!(err.code, Code::ResourceExhausted);
    assert_eq!(dispatcher.attempts(), 3);
    assert_eq!(
        metrics.resource_exhausted_total.load(Ordering::Relaxed),
        3,
        "every RE+signal rejection must be counted (not just the eventual abort)"
    );
    assert_eq!(metrics.aborted_retried_total.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.succeeded_total.load(Ordering::Relaxed), 0);
    assert!(
        err.messages
            .iter()
            .any(|m| m.contains("gave up after 3 attempts")),
        "must surface retry-budget exhaustion message; got {:?}",
        err.messages
    );
}

/// Concurrent same-digest race: two `write_chunked_stream` tasks for
/// the same digest enter a shared dispatcher. The dispatcher rejects
/// the SECOND one with Aborted+BackpressureSignal on its first
/// attempt (simulating the server's `handle_concurrent` rejection
/// when a stream is already in flight for the same digest); on retry
/// the first call has finished, the second's retry succeeds.
///
/// This goes beyond `retry_on_aborted_with_backpressure_signal`
/// (which scripts a fixed sequence) by exercising the concurrent
/// in-flight path the design's Aborted retry was built for.
#[nativelink_test]
async fn concurrent_same_digest_second_call_retries_to_success() {
    use tokio::sync::Notify;

    /// Shared dispatcher. The FIRST task to call `dispatch` blocks
    /// until released, then succeeds. Concurrently arriving tasks
    /// receive Aborted+signal until the first has resolved.
    ///
    /// Uses `AtomicBool` for the "first done" edge rather than
    /// `Notify::notify_waiters`: `notify_waiters` only wakes tasks
    /// already parked on `notified().await`, so a retrying task that
    /// arrives AFTER the notify fires would never see it. An
    /// AtomicBool flag is the correct edge-survives-retry primitive.
    /// `rejections` counts how many times a non-first task bounced
    /// off so the test can synchronously wait until call_b has
    /// observed at least one rejection before releasing call_a (this
    /// makes the race deterministic without `sleep`).
    struct ConcurrentDispatcher {
        first_in_flight: Arc<std::sync::atomic::AtomicBool>,
        first_done: Arc<std::sync::atomic::AtomicBool>,
        rejections: Arc<std::sync::atomic::AtomicU32>,
        release_first: Arc<Notify>,
        committed: u64,
    }
    impl WriteChunkedDispatcher for ConcurrentDispatcher {
        fn dispatch(&self, _chunks: Vec<WriteChunk>) -> DispatchFuture {
            let was_first = self
                .first_in_flight
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
            let release_first = Arc::clone(&self.release_first);
            let first_done = Arc::clone(&self.first_done);
            let rejections = Arc::clone(&self.rejections);
            let committed = self.committed;
            if was_first {
                Box::pin(async move {
                    release_first.notified().await;
                    first_done.store(true, Ordering::SeqCst);
                    Ok(WriteChunkedResponse {
                        committed_digest: None,
                        committed_size: committed,
                    })
                })
            } else {
                Box::pin(async move {
                    // If the first has not yet resolved, reject with
                    // Aborted+signal so the chunked client retries.
                    if first_done.load(Ordering::SeqCst) {
                        Ok(WriteChunkedResponse {
                            committed_digest: None,
                            committed_size: committed,
                        })
                    } else {
                        rejections.fetch_add(1, Ordering::SeqCst);
                        let detail = encode_backpressure_signal_any(
                            backpressure_signal::Reason::PerBlobMpscFull,
                            5,
                        );
                        Err(Error::aborted_with_detail("concurrent stream", detail))
                    }
                })
            }
        }
    }

    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let dispatcher = Arc::new(ConcurrentDispatcher {
        first_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        first_done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        rejections: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        release_first: Arc::new(Notify::new()),
        committed: N as u64,
    });
    let metrics_a = ChunkedClientMetrics::new();
    let metrics_b = ChunkedClientMetrics::new();

    let dispatcher_a: Arc<dyn WriteChunkedDispatcher> =
        Arc::clone(&dispatcher) as Arc<dyn WriteChunkedDispatcher>;
    let dispatcher_b = Arc::clone(&dispatcher_a);
    let blob_a = blob.clone();
    let blob_b = blob.clone();
    let metrics_a_for_call = Arc::clone(&metrics_a);
    let metrics_b_for_call = Arc::clone(&metrics_b);
    let call_a = tokio::spawn(async move {
        write_chunked_stream(
            dispatcher_a.as_ref(),
            digest,
            reader_for_blob(blob_a),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            metrics_a_for_call,
        )
        .await
    });
    let call_b = tokio::spawn(async move {
        // Delay slightly so call_a wins the in-flight race.
        tokio::task::yield_now().await;
        write_chunked_stream(
            dispatcher_b.as_ref(),
            digest,
            reader_for_blob(blob_b),
            ChunkedClientOptions {
                max_attempts: 3,
                chunk_size: N,
            },
            metrics_b_for_call,
        )
        .await
    });

    // Wait until call_b has bounced off at least once, then release
    // call_a. Polling the AtomicU32 (rather than `sleep`) keeps the
    // race deterministic across loaded CI hosts. Bounded by an outer
    // timeout: if call_b never enters dispatch within 2s the test
    // fails fast rather than hanging.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if dispatcher.rejections.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("call_b must enter dispatch and bounce within 2s");
    dispatcher.release_first.notify_one();

    let result_a = tokio::time::timeout(Duration::from_secs(5), call_a)
        .await
        .expect("must not deadlock — call_a must resolve")
        .expect("call_a task must not panic")
        .expect("call_a must succeed first");
    let result_b = tokio::time::timeout(Duration::from_secs(5), call_b)
        .await
        .expect("must not deadlock — call_b must resolve via retry")
        .expect("call_b task must not panic")
        .expect("call_b must succeed via retry once call_a resolves");
    assert_eq!(result_a, N as u64);
    assert_eq!(result_b, N as u64);
    assert!(
        metrics_b.aborted_retried_total.load(Ordering::Relaxed) >= 1,
        "call_b must observe at least one Aborted+signal rejection on the racing path"
    );
}

/// `Code::DataLoss` (the design's e2e SHA-256 mismatch shape) without
/// a backpressure signal must NOT retry — there is no transient
/// recovery for a corrupted-payload commit, and a retry storm masks
/// the underlying defect. Mirror of
/// `classify_invalid_argument_with_signal_is_abort` for the DataLoss
/// code path.
#[nativelink_test]
async fn no_retry_on_data_loss() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let data_loss = Err(nativelink_error::make_err!(
        Code::DataLoss,
        "e2e SHA-256 mismatch on commit"
    ));
    let dispatcher = FakeDispatcher::new(vec![data_loss]);
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
    .expect_err("DataLoss must propagate as Err immediately");

    assert_eq!(err.code, Code::DataLoss);
    assert_eq!(
        dispatcher.attempts(),
        1,
        "DataLoss must NOT be retried — no backpressure signal, not transient"
    );
    assert_eq!(metrics.aborted_retried_total.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.resource_exhausted_total.load(Ordering::Relaxed), 0);
}

/// `Code::Aborted` WITHOUT a backpressure signal must NOT retry. The
/// retry decision MUST require both code AND signal — a bare Aborted
/// (e.g. from a server-side abort path that didn't attach the
/// `BackpressureSignal` detail) is treated as abort, not retry.
/// Mirrors `no_retry_on_bare_resource_exhausted` for the Aborted
/// half of the contract.
#[nativelink_test]
async fn no_retry_on_bare_aborted() {
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    let bare_aborted = Err(nativelink_error::make_err!(
        Code::Aborted,
        "bare aborted (no backpressure signal)"
    ));
    let dispatcher = FakeDispatcher::new(vec![bare_aborted]);
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
    .expect_err("bare Aborted must propagate as Err immediately");

    assert_eq!(err.code, Code::Aborted);
    assert_eq!(
        dispatcher.attempts(),
        1,
        "bare Aborted must NOT be retried — both code AND signal required"
    );
    assert_eq!(metrics.aborted_retried_total.load(Ordering::Relaxed), 0);
}

/// Server returns `committed_digest` on the response. Today the
/// chunked client only validates `committed_size` (line ~365 of
/// chunked_client.rs); the digest field is read by some operators
/// for audit but the client does NOT compare it to the requested
/// digest. This test pins TODAY's behavior (success on size match
/// regardless of `committed_digest`) so a future change that adds a
/// digest-equality check is detected and routed through a deliberate
/// review.
///
/// If/when the design adds client-side committed_digest verification,
/// this test must be updated (or deleted) intentionally — see the
/// follow-up tracker filed by #219.
#[nativelink_test]
async fn committed_digest_mismatch_today_does_not_fail() {
    use nativelink_proto::build::bazel::remote::execution::v2::Digest as ProtoDigest;
    const N: usize = 4 * 1024;
    let (digest, blob) = synth_blob(N);
    // Construct a wrong committed_digest (different hash) and ship
    // it back to the client.
    let wrong = ProtoDigest {
        hash: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        size_bytes: N as i64,
    };
    let response = WriteChunkedResponse {
        committed_digest: Some(wrong),
        committed_size: N as u64,
    };
    let dispatcher = FakeDispatcher::new(vec![Ok(response)]);
    let metrics = ChunkedClientMetrics::new();

    let result = tokio::time::timeout(
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
    .expect("must not deadlock");

    // Pin today's behavior: client returns Ok because committed_size
    // matched declared size. A future digest-equality check would
    // flip this to Err — that's a deliberate change, not a silent
    // regression.
    let committed = result
        .expect("today the client does NOT compare committed_digest; size match is sufficient");
    assert_eq!(committed, N as u64);
}

/// Erase Pin<&> to keep the compiler happy with the boxed dispatcher
/// pointer arithmetic. Not strictly needed in tests; kept for future
/// expansion if any test wants to hand the dispatcher to a sub-task.
#[allow(dead_code)]
fn _phantom_pin_use(_: Pin<&dyn WriteChunkedDispatcher>) {}
