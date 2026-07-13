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

//! #212 Phase 2.7 fixup D6 — Str-keyed `update()` must skip the
//! chunked-dispatch gate entirely.
//!
//! When the `chunked_fast_slow` kill-switch is ON and a chunked
//! dispatcher has been installed, the gate at
//! `fast_slow_store.rs::update` previously called
//! `key.borrow().into_digest()` BEFORE checking the size threshold.
//! For Str-keyed AC entries, that synthesizes a digest by Blake3-
//! hashing the entire key string — hot-path cost on every AC update
//! once the kill-switch is flipped ON in production (code-reviewer
//! §MAJOR 3, deferred follow-up D6).
//!
//! Fix: gate on `StoreKey::Digest` first; Str-keyed updates take the
//! legacy decoupled path with zero hashing.
//!
//! Coverage:
//!   1. Str-keyed `update()` (the streaming path the Bazel server
//!      actually hits) with kill-switch ON + tiny threshold +
//!      dispatcher installed: dispatcher MUST NOT be invoked, write
//!      MUST succeed, blob MUST be readable through the FastSlowStore
//!      (semantic-equivalence to kill-switch OFF).
//!   2. Positive control: same setup but with a Digest key larger than
//!      the threshold — dispatcher MUST be invoked exactly once.
//!
//! Mutation step: revert the `if let StoreKey::Digest(...)` gate to the
//! original `key.borrow().into_digest()` form and re-run; test (1)
//! red-fails with "dispatcher invoked for Str-keyed update".
//!
//! Note: the override `update_oneshot` on `FastSlowStore` does NOT route
//! through the chunked-dispatch gate (it has its own bypassing path
//! that writes the fast tier inline + spawns a background slow-tier
//! task). The Bazel server's gRPC handler hits `update()` (streaming
//! reader), so the test drives `update()` via a buf channel directly.

#![cfg(feature = "chunked_fast_slow")]

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::chunked::{
    BazelChunkedDispatcher, BazelChunkedDispatcherArc, disable_bazel_facing_internal_chunking,
    enable_bazel_facing_internal_chunking,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;

/// Process-wide kill-switch is global state. Cargo runs `#[nativelink_test]`
/// integration tests on a multi-threaded runtime; serialize ourselves so
/// a sibling test toggling the same switch doesn't observe a flipped state.
fn kill_switch_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Fake dispatcher that bumps a counter every time `dispatch` is called
/// and drains the reader to EOF (so the data-stream future doesn't
/// stall on backpressure). The counter is visible to the test via the
/// `Arc<AtomicUsize>` it shares with the constructor.
#[derive(Debug)]
struct CountingDispatcher {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl BazelChunkedDispatcher for CountingDispatcher {
    async fn dispatch(
        &self,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<u64, Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        // Drain the reader so the upstream tee future can complete.
        loop {
            let buf = reader.recv().await?;
            if buf.is_empty() {
                break;
            }
        }
        Ok(digest.size_bytes())
    }
}

fn make_fast_slow() -> (Arc<FastSlowStore>, Store, Store) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast.clone(),
        slow.clone(),
    );
    (fss, fast, slow)
}

/// Drive `StoreLike::update` (the streaming path; the chunked-dispatch
/// gate lives here, NOT in `update_oneshot`) with a single-shot
/// payload. Mirrors what the Bazel server's gRPC ByteStream Write
/// handler does to a `FastSlowStore`.
async fn drive_update(store: &Store, key: StoreKey<'_>, payload: Bytes) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let payload_len = payload.len() as u64;
    let send_fut = async move {
        tx.send(payload).await?;
        tx.send_eof()?;
        Ok::<(), Error>(())
    };
    let update_fut = store.update(key, rx, UploadSizeInfo::ExactSize(payload_len));
    let (send_res, update_res) = tokio::join!(send_fut, update_fut);
    send_res?;
    update_res
}

/// Test 1 — the D6 fix itself.
///
/// Kill-switch ON + tiny chunked-size threshold (1 byte) + dispatcher
/// installed. A Str-keyed `update()` must:
///   (a) succeed,
///   (b) write data to fast + slow tiers as normal,
///   (c) NOT invoke the chunked dispatcher even once,
///   (d) read back identical bytes.
///
/// The mutation step (revert the `StoreKey::Digest` gate) flips (c) to
/// "dispatcher invoked once" and the assertion red-fails with the
/// specific message below.
#[nativelink_test]
async fn str_keyed_update_skips_chunked_dispatcher() -> Result<(), Error> {
    let _guard = kill_switch_lock().lock().await;

    let (fss, _fast, _slow) = make_fast_slow();
    let invocations = Arc::new(AtomicUsize::new(0));
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(CountingDispatcher {
        invocations: invocations.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    // 1-byte threshold so any non-empty payload would otherwise dispatch.
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // Str key shaped like a real AC entry name (long enough that an
    // accidental Blake3 hash on it would have a measurable cost).
    let key_str =
        "ac/some-instance/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/4096";
    let key = StoreKey::from(key_str);
    let payload = Bytes::from_static(b"AC-style ActionResult payload bytes");

    let store: Store = Store::new(fss.clone());

    // Deadlock-detector timeout — a wedge in the data-stream/tee path
    // for Str keys would surface as a hang rather than a silent pass.
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, key.borrow(), payload.clone()),
    )
    .await
    .expect("update must not hang for Str-keyed update")?;

    let n = invocations.load(Ordering::SeqCst);

    // Restore kill-switch BEFORE the assertion so a failing assert
    // doesn't leak the toggle to sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    assert_eq!(
        n, 0,
        "dispatcher invoked for Str-keyed update — D6 gate (StoreKey::Digest first) regressed (count={n})",
    );

    // Behavior parity check: read-back works as if the kill-switch had
    // been OFF. The legacy decoupled path writes to the fast tier
    // inline + spawns the slow-tier write in the background; reads
    // resolve from the fast tier.
    let read_back = tokio::time::timeout(
        Duration::from_secs(5),
        store.get_part_unchunked(key.borrow(), 0, None),
    )
    .await
    .expect("get_part_unchunked must not hang for Str-keyed read-back")?;
    assert_eq!(
        read_back, payload,
        "Str-keyed read-back must equal the payload"
    );

    Ok(())
}

/// Test 2 — positive control. Confirms the dispatcher IS invoked for
/// Digest-keyed `update()` above the size threshold, so test 1's "0
/// invocations" assertion is meaningful (not just because the
/// dispatcher is broken everywhere).
#[nativelink_test]
async fn digest_keyed_update_invokes_chunked_dispatcher() -> Result<(), Error> {
    let _guard = kill_switch_lock().lock().await;

    let (fss, _fast, _slow) = make_fast_slow();
    let invocations = Arc::new(AtomicUsize::new(0));
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(CountingDispatcher {
        invocations: invocations.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // Digest key with size_bytes >= threshold (1 byte) — eligible.
    let payload = Bytes::from_static(b"some payload");
    let mut hash = [0u8; 32];
    hash[0] = 0x42;
    let digest = DigestInfo::new(hash, payload.len() as u64);
    let key: StoreKey<'static> = digest.into();

    let store: Store = Store::new(fss.clone());
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, key.borrow(), payload),
    )
    .await
    .expect("update must not hang for Digest-keyed update")?;

    let n = invocations.load(Ordering::SeqCst);

    // Restore kill-switch BEFORE the assertion so a failing assert
    // doesn't leak the toggle to sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    assert_eq!(
        n, 1,
        "dispatcher must be invoked exactly once for Digest-keyed update over threshold (count={n}); \
         if 0, the chunked-dispatch wiring itself is broken and test 1's 0-invocation assertion proves nothing",
    );

    Ok(())
}

// ---------------------------------------------------------------------
// M1 (cascade bundle pass-2 dsr review) — chunked-path mid-stream
// rejection must preserve typed BackpressureSignal across the
// generic data_res `Code::Internal` wrapper.
//
// Original guard (#334 bundle fixup #7 / red-team #3) used an
// admit-time RejectingDispatcher that dropped `chunk_rx` before
// observing any chunks; in that ordering the data-stream future
// raced to EOF and the legacy `match dispatch_res` arm at
// `:1248-1268` returned the typed err. The dsr review correctly
// identified the production-active case is MID-STREAM rejection
// (dispatcher consumes K chunks then rejects with PinnedBytesExhausted
// or GlobalChunkBudgetExhausted), where the data-stream future is
// guaranteed to be mid-`chunk_guard.send` when chunk_rx is dropped,
// reliably producing `data_res = Err(Code::Internal "...Failed to
// send to chunked dispatcher...")` BEFORE dispatch_res is consumed
// by the legacy arm. The fix is the new pre-`data_res` typed-signal
// guard at `fast_slow_store.rs:~1230`.
// ---------------------------------------------------------------------

/// Fake dispatcher that consumes K non-empty chunks then rejects with a
/// typed `BackpressureSignal::PinnedBytesExhausted` (the
/// production-active PinBudget-exhaustion shape from
/// `nativelink-service/src/chunked_write_handler.rs:1448-1452`). After
/// consuming the K-th chunk and BEFORE returning Err, the dispatcher
/// signals a barrier `Notify` so the test producer can release the next
/// chunk and guarantee the data-stream future's
/// `chunk_guard.send(...).await` is the in-flight site that observes
/// the dropped chunk_rx.
///
/// This fake reproduces the production race the dsr review identified:
///   1. `data_stream_fut` sends chunks 0..=K-1 to chunk_tx.
///   2. dispatcher consumes those K chunks.
///   3. dispatcher returns Err (typed BackpressureSignal). chunk_rx is
///      dropped at end-of-scope as `dispatch_fut` resolves.
///   4. `data_stream_fut` calls `chunk_guard.send(chunk K)` → fails with
///      channel-closed → returns `Err(Code::Internal "Failed to send to
///      chunked dispatcher")`.
///   5. `join3(...)` resolves with `data_res = Err(Internal)` and
///      `dispatch_res = Err(typed)`. Without the M1 guard, the generic
///      `data_res` arm fires FIRST and the typed signal is lost.
#[derive(Debug)]
struct MidStreamRejectingDispatcher {
    invocations: Arc<AtomicUsize>,
    chunks_to_consume_before_reject: usize,
    /// Notified after the K-th chunk has been consumed so the test
    /// producer can release the (K+1)-th chunk into chunk_tx, then the
    /// dispatcher returns Err and chunk_rx is dropped — the producer's
    /// in-flight send observes channel-closed.
    threshold_reached: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl BazelChunkedDispatcher for MidStreamRejectingDispatcher {
    async fn dispatch(
        &self,
        _digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<u64, Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let mut consumed = 0_usize;
        while consumed < self.chunks_to_consume_before_reject {
            let buf = reader.recv().await?;
            if buf.is_empty() {
                // Producer EOF before we hit the threshold — abort the
                // setup so the test surfaces "race didn't fire" rather
                // than a misleading pass.
                return Err(nativelink_error::make_err!(
                    nativelink_error::Code::FailedPrecondition,
                    "MidStreamRejectingDispatcher: producer EOF'd before \
                     consuming {} chunks (consumed {}); test fixture cannot \
                     reproduce mid-stream rejection — increase chunk count \
                     or reduce threshold",
                    self.chunks_to_consume_before_reject,
                    consumed,
                ));
            }
            consumed += 1;
        }
        // Signal the producer to release the next chunk so the producer
        // is mid-`chunk_guard.send` when we drop chunk_rx at our Err
        // return below. This is the production race ordering — without
        // the in-flight send, the producer would observe EOF instead of
        // channel-closed.
        self.threshold_reached.notify_one();
        // Yield the runtime so the producer's pending send is scheduled
        // and is observably blocked on chunk_tx capacity (chunk_tx has
        // size 128 = 128 buffers in flight; consuming K=1 reads the head
        // out so the producer's next send needs space).
        tokio::task::yield_now().await;
        // Drop reader implicitly via Err return. Producer's pending /
        // next chunk_guard.send fails with channel-closed → generic
        // Code::Internal in data_stream_fut closure.
        let detail = nativelink_store::chunked_signal::encode_backpressure_signal_any(
            nativelink_proto::com::github::trace_machina::nativelink::remote_execution::backpressure_signal::Reason::PinnedBytesExhausted,
            250,
        );
        Err(Error::resource_exhausted_backpressure(
            "MidStreamRejectingDispatcher: synthetic PinBudget exhaustion mid-stream",
            detail,
        ))
    }
}

/// Multi-chunk producer driving `StoreLike::update`. Sends `n_chunks`
/// non-empty buffers (each `chunk_size` bytes) then EOF, with a barrier
/// `Notify` between chunks `K-1` and `K` so the dispatcher fixture can
/// reach its consume-K-then-reject state with the producer mid-send on
/// chunk K. Mirrors the production gRPC ByteStream path which delivers
/// the payload as a stream of chunks, not a single oneshot buffer.
async fn drive_update_chunked(
    store: &Store,
    key: StoreKey<'_>,
    n_chunks: usize,
    chunk_size: usize,
    threshold_at: usize,
    threshold_reached: Arc<tokio::sync::Notify>,
) -> Result<(), Error> {
    use nativelink_util::buf_channel::make_buf_channel_pair;

    let (mut tx, rx) = make_buf_channel_pair();
    let total_size = (n_chunks * chunk_size) as u64;

    let send_fut = async move {
        let buf = Bytes::from(vec![0xAB_u8; chunk_size]);
        for i in 0..n_chunks {
            if i == threshold_at {
                // Block until the dispatcher has consumed `threshold_at`
                // chunks. The dispatcher then drops chunk_rx after this
                // notify, so this send observes channel-closed mid-flight.
                threshold_reached.notified().await;
            }
            // Best-effort send — the channel-closed Err on the
            // post-threshold chunk is the load-bearing signal that the
            // production race fired. The data_stream_fut consuming `rx`
            // surfaces it as `Code::Internal "Failed to send to chunked
            // dispatcher"`.
            if let Err(e) = tx.send(buf.clone()).await {
                return Err(e);
            }
        }
        tx.send_eof()?;
        Ok::<(), Error>(())
    };

    let update_fut = store.update(key, rx, UploadSizeInfo::ExactSize(total_size));
    let (send_res, update_res) = tokio::join!(send_fut, update_fut);
    // Producer-side Err (channel-closed mid-send) is expected and harmless;
    // the load-bearing assertion is on `update_res`.
    drop(send_res);
    update_res
}

/// **M1 (cascade bundle pass-2 dsr review).** When the chunked
/// dispatcher rejects a Digest-keyed `update()` MID-STREAM with a
/// typed `BackpressureSignal::*`, the FastSlowStore's chunked path
/// MUST surface the typed signal to the caller — NOT the generic
/// `Code::Internal "Failed to send to chunked dispatcher"` that the
/// data-stream future emits when `chunk_rx` is dropped by the
/// rejecting dispatcher.
///
/// **Production scenario.** The chunked dispatcher
/// (`nativelink-service/src/chunked_write_handler.rs`) emits typed
/// `BackpressureSignal::*` via `Error::resource_exhausted_backpressure`
/// at four sites:
///   - `:579` — per-blob mpsc full
///   - `:1422` — global chunk budget exhausted
///   - `:1448` — pinned bytes exhausted (this test)
///   - `:1487` — per-blob mpsc full
///   - `:1869` — concurrent same-digest stream (Code::Aborted)
///
/// Bazel respects backoff hints from the typed `BackpressureSignal`,
/// retrying with the dispatcher-suggested `retry_after_ms`. Bazel
/// does NOT honor backoff hints on `Code::Internal` — that is treated
/// as a hard failure. So the typed-vs-generic distinction is
/// load-bearing: if the typed signal is masked, builds fail rather
/// than backing off and recovering.
///
/// **Mutation step.** Comment out the new pre-`data_res` guard in
/// `fast_slow_store.rs` (the block introduced by M1 immediately
/// after the `fast_res_carries_typed_backpressure` block, around
/// `:~1230`). The test must red-fail with the bespoke
/// `"typed BackpressureSignal lost — pre-data_res guard removed"`
/// message because the generic `data_res` arm fires first and
/// returns `Code::Internal` before the legacy `match dispatch_res`
/// arm can be reached.
#[nativelink_test]
async fn m1_chunked_mid_stream_rejection_preserves_typed_backpressure(
) -> Result<(), Error> {
    let _guard = kill_switch_lock().lock().await;

    let (fss, _fast, _slow) = make_fast_slow();
    let invocations = Arc::new(AtomicUsize::new(0));
    let threshold_reached = Arc::new(tokio::sync::Notify::new());
    // Consume 1 chunk then signal the producer to release a 2nd chunk
    // before returning Err. The 2nd chunk's `chunk_guard.send(...).await`
    // is the in-flight site that observes the dropped chunk_rx — exactly
    // the production race the dsr review identified.
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(MidStreamRejectingDispatcher {
        invocations: invocations.clone(),
        chunks_to_consume_before_reject: 1,
        threshold_reached: threshold_reached.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // 4 MiB total in 4 × 1 MiB chunks. Multi-chunk send is required so
    // the data-stream future has more chunks queued AFTER the dispatcher
    // consumes its threshold and drops chunk_rx — the production race
    // the M1 fix is intended to catch. A single oneshot send would
    // race to EOF before the rejection ever observes a second buffer
    // (the failure mode the prior bundle-fixup-#7 fixture had: it
    // passed even with the fix absent because the legacy `match
    // dispatch_res` arm caught the typed err in a no-data-pending state).
    let chunk_size = 1024 * 1024;
    let n_chunks = 4;
    let total_size = (n_chunks * chunk_size) as u64;
    let mut hash = [0u8; 32];
    hash[0] = 0x42;
    let digest = DigestInfo::new(hash, total_size);
    let key: StoreKey<'static> = digest.into();

    let store: Store = Store::new(fss.clone());

    // 5s deadlock detector — chunked-path rejection must complete
    // promptly. A wedge here would surface as Elapsed rather than
    // a silent pass.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update_chunked(
            &store,
            key.borrow(),
            n_chunks,
            chunk_size,
            /* threshold_at */ 1,
            threshold_reached,
        ),
    )
    .await
    .expect(
        "update must not hang for Digest-keyed mid-stream rejection — \
         chunked-path must complete promptly",
    );

    // Restore kill-switch BEFORE the assertion so a failing assert
    // doesn't leak the toggle to sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    let err = result.expect_err(
        "chunked-path update with mid-stream rejecting dispatcher MUST return Err — \
         M1: typed BackpressureSignal from dispatch_res must surface, not be \
         masked by generic data_res Code::Internal",
    );

    use nativelink_error::Code;
    use nativelink_store::chunked_signal::error_has_backpressure_signal;
    assert!(
        (err.code == Code::ResourceExhausted || err.code == Code::Aborted)
            && error_has_backpressure_signal(&err),
        "typed BackpressureSignal lost — pre-data_res guard removed (M1 regression): \
         got code={:?} messages={:?} details_len={} \
         (chunked-path data_res check at fast_slow_store.rs:~1252 returned its \
         generic Internal before the new dispatch_res typed-signal guard at \
         :~1230 could fire. Bazel does NOT honor backoff hints on Code::Internal, \
         so this regression silently demotes the typed-signal-driven retry path \
         to a hard failure)",
        err.code,
        err.messages,
        err.details.len(),
    );

    let n = invocations.load(Ordering::SeqCst);
    assert_eq!(
        n, 1,
        "dispatcher MUST have been invoked exactly once (count={n}); \
         if 0, the chunked path didn't dispatch at all — the bug-precondition \
         race didn't fire and the assertion proves nothing"
    );

    Ok(())
}

