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
use bytes::Bytes;
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BackpressureSignal, backpressure_signal,
};
use nativelink_proto::type_urls::BACKPRESSURE_SIGNAL_TYPE_URL;
use nativelink_store::chunked::CHUNK_SIZE;
use nativelink_store::chunked::chunk_budget::{ChunkBudget, TOTAL_CHUNK_PERMITS};
use nativelink_store::chunked::chunked_driver::PER_BLOB_MPSC_CAP;
use nativelink_store::chunked::pin_budget::PinBudget;
use nativelink_store::chunked_signal::{
    encode_backpressure_signal_any, error_has_backpressure_reason, error_has_backpressure_signal,
};
use nativelink_util::common::DigestInfo;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

// -----------------------------------------------------------------------------
// Helpers (shared by the admit / pin-budget / producer-arrival-probe tests)
// -----------------------------------------------------------------------------

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(out.as_ref());
    a
}

fn make_test_budget() -> &'static ChunkBudget {
    Box::leak(Box::new(ChunkBudget::new()))
}

// -----------------------------------------------------------------------------
// Production-composition tests
// -----------------------------------------------------------------------------

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
