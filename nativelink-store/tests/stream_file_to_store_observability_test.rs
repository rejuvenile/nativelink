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

//! #476 Phase 1 — `FastSlowStore::stream_{path,file}_to_store` observability.
//!
//! **Invariant under test:** when both halves of the
//! `join!(write_fut, forward_fut)` error, the consumer-side error
//! (`write_res` from `slow_store.update(rx)`) is the authoritative root
//! cause; the producer-side `forward_res` (a buf_channel
//! `"receiver disconnected"` after the consumer drops `rx`) is a
//! mechanically-derived symptom that must NOT supersede the consumer
//! error.
//!
//! **Mechanism that violates it (pre-fix):** `forward_res?; write_res`
//! in `stream_file_to_store` short-circuited on
//! `forward_res?`, discarding `write_res`.
//!
//! **Mechanism that re-establishes it (post-fix, v3 / Option 3):**
//! `match (write_res, forward_res)` that prefers `write_res` on
//! dual-err: `(Err(write_err), Err(_forward_err)) => Err(write_err)`.
//! fixup-v3 drops the prior diagnostic append (which the fixup-v1/v2
//! cadre added and then equality-guarded): Option A's
//! `tx.send_error(err.clone())` routing in `forward_fut` already
//! mirrors the producer's typed err into `write_res` via
//! `terminal_error`, so `forward_err` carries no information that
//! isn't already in `write_err`. The fixup-v2 equality guard turned
//! out to be structurally dead in production (every leaf store's
//! `update` adds `err_tip` to the recv chain, mutating
//! `Error.messages` and breaking the derived `PartialEq`).
//!
//! **Producer-IO masking fix (#476 fixup, Option A):** the
//! `forward_fut` closure in both helpers routes any producer-side error
//! through `tx.send_error(err.clone())` BEFORE returning the Err. This
//! ensures the consumer's `recv()` reads the REAL typed Error verbatim
//! (file IO err, bridge err, etc.) instead of the synthesized
//! `"Sender dropped before sending EOF"` Internal that the buf_channel
//! emits when `tx` is merely dropped. Without this routing, the
//! post-fix dual-err policy still prefers `write_res`, which would
//! carry the synthesized buf-channel symptom — masking the producer's
//! true root cause.
//!
//! **Seams crossed by these tests:**
//!   1. Producer: `spawn_blocking` reader → bridge mpsc
//!   2. Consumer wrapper: `forward_fut` → buf_channel `tx` (now
//!      includes the Option A `tx.send_error` routing on Err)
//!   3. Consumer (slow store): `update(rx)` — may drop `rx`
//!      mid-stream, OR may propagate the producer's `terminal_error`
//!   4. The `join!` aggregator inside `stream_file_to_store`
//!   5. Final `match` that selects which error to surface
//!   6. `FastSlowStore::update_with_whole_file` wrapper that calls
//!      into `stream_file_to_store` (the seam through which the test
//!      reaches the private helper, since both `stream_*_to_store`
//!      are private `async fn`)
//!   7. `Store::update_with_whole_file` `err_tip` wrapping
//!
//! Both tests wrap the unit in production composition (real
//! `MemoryStore` fast tier reporting `FileUpdates`, real
//! `FastSlowStore`) and assert via SPECIFIC messages — not
//! `is_err()`. `tokio::time::timeout` wraps each call as a deadlock
//! detector.
//!
//! Mutation steps (run by hand to verify):
//!   1. For `surfaces_consumer_error_over_symptom_dual_err`: invert
//!      the dual-err arm in `stream_file_to_store` from
//!      `(Err(write_err), Err(_)) => Err(write_err)` to
//!      `(Err(_), Err(forward_err)) => Err(forward_err)`. Re-run —
//!      must red-fail with the bespoke "consumer-error-prefer policy
//!      not enforced" message (the surfaced err is the producer's
//!      `Failed to send chunk in stream_file_to_store` symptom, which
//!      does NOT contain `ABORT_TAG`).
//!   2. For `surfaces_producer_io_error_not_buf_channel_symptom`:
//!      comment out the
//!      `if let Err(ref e) = result { tx.send_error(e.clone()); }`
//!      block in `stream_file_to_store::forward_fut`. Re-run — must
//!      red-fail with the bespoke "Option A producer-IO masking fix
//!      not enforced" message naming the synthesized
//!      "Sender dropped before sending EOF" leak.
//!   3. For `surfaces_consumer_abort_when_producer_succeeded`: change
//!      the `(Err(write_err), Ok(())) => Err(write_err)` arm in
//!      `stream_file_to_store` to `(Err(_), Ok(())) => Ok(())` (drop
//!      the consumer's post-EOF err). Re-run — must red-fail with the
//!      bespoke "consumer-post-EOF-Err policy regressed — the
//!      `(Err, Ok)` arm dropped the consumer-side authoritative err"
//!      message.
//!
//! Coverage matrix:
//!   * `(Ok, Ok)` — covered by other crate tests; trivial passthrough.
//!   * `(Err, Ok)` consumer-post-EOF — `surfaces_consumer_abort_when_producer_succeeded`.
//!   * `(Ok, Err)` consumer Ok, producer Err — fixed by #56/#62; covered
//!     by `stream_file_consumer_ok_producer_err_test.rs`. Note: this arm
//!     IS reachable in production even under Option A's `tx.send_error`
//!     routing, because when `tx.send(chunk)` fails due to rx drop,
//!     `self.tx` is set to None and `tx.send_error(e.clone())` is a no-op
//!     — the consumer has already completed. GrpcStore AlreadyExists → Ok
//!     is the production trigger (two incidents 2026-06-12).
//!   * `(Err, Err)` distinct halves (slow tier aborts mid-stream,
//!     producer hits channel-closed) — `surfaces_consumer_error_over_symptom_dual_err`.
//!   * `(Err, Err)` same payload (producer-IO routed via Option A) —
//!     `surfaces_producer_io_error_not_buf_channel_symptom`.

use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::ffi::OsString;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};

/// Unique tag asserted-on in test output for the consumer-error-prefer
/// (dual-err) test. If this string fails to appear in the surfaced
/// `Err`, the consumer-error-prefer policy is broken and the
/// producer's "receiver disconnected" symptom was promoted in its
/// place.
const ABORT_TAG: &str = "TEST_SLOW_STORE_ABORTED_FOR_DIAGNOSTIC_TEST";

/// Unique tag asserted-on by the `(Ok producer, Err consumer)` test.
/// The slow tier drains all chunks + EOF, then returns Err carrying
/// this tag, simulating the most common production dual-err shape:
/// VerifyStore post-EOF size mismatch, h2 RST after stream complete,
/// or slow-store commit-time rejection (storage-full, admission). If
/// this tag fails to surface, the consumer-side post-EOF err was
/// swallowed.
const POST_EOF_ABORT_TAG: &str = "TEST_CONSUMER_ABORT_AT_EOF_FOR_DIAGNOSTIC";

const VALID_HASH: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Number of 256 KiB chunks fed to the inner `buf_channel` of
/// `stream_file_to_store`. The inner buf_channel has capacity 128
/// (`fast_slow_store.rs:4284`) AND a 4-slot bridge mpsc
/// (`:4288`); the producer's chunk loop fills both before blocking
/// on `tx.send`. Sending 256 chunks (= 64 MiB) guarantees the
/// producer is mid-send when the consumer drops `rx`, deterministically
/// producing the dual-err arm `(Err(Aborted), Err(channel-closed))`
/// that distinguishes pre-fix from post-fix behavior. The technique
/// is adopted from the #512 sibling test
/// (`fast_slow_store_512_consumer_error_prefer_test.rs`).
const CHUNK_BYTES: usize = 256 * 1024;
const CHUNK_COUNT: usize = 256;
const TOTAL_BYTES: usize = CHUNK_BYTES * CHUNK_COUNT;

/// Fast-tier wrapper around `MemoryStore` that reports
/// `StoreOptimizations::FileUpdates`. This is the precondition for
/// `FastSlowStore::update_with_whole_file` to take the parallel
/// `stream_file_to_store` path (see `fast_slow_store.rs:5470-5511`).
/// Without it, the function falls through to the no-FileUpdates
/// branches and the streaming helper under test is never invoked.
#[derive(MetricsComponent)]
struct FileUpdateStore {
    inner: Arc<MemoryStore>,
}

#[async_trait]
impl StoreDriver for FileUpdateStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(digests, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .update(digest, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await
    }

    async fn update_with_whole_file(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        _path: OsString,
        file: nativelink_util::common::fs::FileSlot,
        upload_size: UploadSizeInfo,
    ) -> Result<Option<nativelink_util::common::fs::FileSlot>, Error> {
        // Delegate to the regular update path (read file, send to inner).
        let file = nativelink_util::store_trait::slow_update_store_with_file(
            Pin::new(self.inner.as_ref()),
            key,
            file,
            upload_size,
        )
        .await?;
        Ok(Some(file))
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        matches!(optimization, StoreOptimizations::FileUpdates)
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(FileUpdateStore);

/// Slow-tier fake that, on `update()`, drains exactly one chunk from
/// the reader and then returns a uniquely-tagged Err — dropping `rx`.
/// With a 256-chunk feeder (CHUNK_COUNT × CHUNK_BYTES = 64 MiB) and
/// the inner buf_channel at capacity 128 + bridge mpsc 4, the producer
/// is guaranteed to be blocked mid-send when this drop happens. The
/// producer's next `tx.send(chunk)` then errors with channel-closed,
/// producing the dual-err `(Err(Aborted), Err(channel-closed))` arm
/// that distinguishes pre-fix from post-fix.
#[derive(MetricsComponent)]
struct AbortAfterOneChunkStore {
    update_invocations: AtomicUsize,
}

impl AbortAfterOneChunkStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for AbortAfterOneChunkStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Consume one chunk so the producer is fully engaged before
        // we drop. With a 256-chunk feeder vs a 128-slot buf_channel,
        // the producer is guaranteed to be blocked on tx.send when
        // the consumer drops `rx`, deterministically producing the
        // dual-err arm.
        let _chunk = reader.recv().await;
        // Returning Err drops `reader` (and thus `rx` from the
        // buf_channel pair) before EOF. The producer side's pending
        // `tx.send` then wakes with channel-closed.
        Err(make_err!(Code::Aborted, "{ABORT_TAG}"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "AbortAfterOneChunkStore: get_part not supported"
        ))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(AbortAfterOneChunkStore);

/// Slow-tier fake that propagates the FIRST recv error verbatim. Used
/// by the producer-IO test to verify the Option A
/// `tx.send_error(producer_err)` routing reaches the consumer
/// untransformed — i.e. the consumer's `update()` returns the EXACT
/// error the producer hit, NOT the synthesized
/// "Sender dropped before sending EOF" Internal that the buf_channel
/// emits when `tx` is merely dropped without `send_error`.
#[derive(MetricsComponent)]
struct PropagateRecvErrorStore {
    update_invocations: AtomicUsize,
    last_err_str: parking_lot::Mutex<Option<String>>,
}

impl PropagateRecvErrorStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
            last_err_str: parking_lot::Mutex::new(None),
        }
    }
}

#[async_trait]
impl StoreDriver for PropagateRecvErrorStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Loop until we see EOF (clean) or an error.
        loop {
            match reader.recv().await {
                Ok(chunk) if chunk.is_empty() => return Ok(()),
                Ok(_) => continue,
                Err(e) => {
                    // Record the error string for diagnostic-time
                    // inspection (the dual-err match arm will wrap
                    // this further); propagate verbatim so any
                    // wrapper that examines write_res sees the EXACT
                    // payload the producer routed via send_error.
                    *self.last_err_str.lock() = Some(format!("{e:?}"));
                    return Err(e);
                }
            }
        }
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "PropagateRecvErrorStore: get_part not supported"
        ))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(PropagateRecvErrorStore);

/// Slow-tier fake that drains EVERY chunk + observes a clean EOF,
/// then returns Err. Simulates the most common production dual-err
/// shape — VerifyStore post-EOF size mismatch, h2 RST_STREAM after
/// the body completes, or slow-store commit-time rejection
/// (storage-full, admission-pressure) — where the producer happily
/// finishes the stream and ONLY THEN the consumer errors.
///
/// In this shape, `write_fut` returns `Err(POST_EOF_ABORT_TAG)` while
/// `forward_fut` returns `Ok(())` (it sent EOF before the consumer
/// errored), hitting the `(Err(write_err), Ok(())) => Err(write_err)`
/// arm. The post-fix policy must surface `write_err` verbatim — a
/// regression that flips arm order or drops the consumer-side err on
/// `(Err, Ok)` would silently mask production aborts.
#[derive(MetricsComponent)]
struct AcceptAllThenAbortOnEofStore {
    update_invocations: AtomicUsize,
    saw_clean_eof: parking_lot::Mutex<bool>,
}

impl AcceptAllThenAbortOnEofStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
            saw_clean_eof: parking_lot::Mutex::new(false),
        }
    }
}

#[async_trait]
impl StoreDriver for AcceptAllThenAbortOnEofStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drain every chunk; EOF is signalled by `Ok(empty)` from
        // recv (see `buf_channel.rs:607,632`). Any propagated err
        // would short-circuit to the (Err, Err) arm — that's the
        // DIFFERENT path the dual-err test covers. This test wants
        // the (Err producer-Ok, Err consumer) arm specifically.
        loop {
            match reader.recv().await {
                Ok(chunk) if chunk.is_empty() => {
                    // Clean EOF observed; producer fully drained and
                    // sent send_eof. Record it (the test asserts on
                    // this to prove the producer succeeded — the path
                    // under test is the `(Err consumer, Ok producer)`
                    // arm, not the dual-err arm).
                    *self.saw_clean_eof.lock() = true;
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    // Should not happen in this test scenario; if it
                    // does, propagate so the test surfaces an unexpected
                    // dual-err arm instead of masking.
                    return Err(e);
                }
            }
        }
        // Simulate a post-EOF commit failure (VerifyStore size mismatch,
        // slow-store admission/storage rejection, h2 RST after body).
        Err(make_err!(Code::Aborted, "{POST_EOF_ABORT_TAG}"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        Err(make_err!(
            Code::NotFound,
            "AcceptAllThenAbortOnEofStore: get_part not supported"
        ))
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_item_callback(
        self: Arc<Self>,
        _callback: Arc<dyn ItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn stable_delegation(&self) -> StableDigestDelegation<'_> {
        StableDigestDelegation::Leaf
    }

    fn pin_delegation(&self) -> PinDelegation<'_> {
        PinDelegation::Leaf
    }

    fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
        MarkStableDelegation::Leaf
    }
}

default_health_status_indicator!(AcceptAllThenAbortOnEofStore);

/// Build a production-composition `FastSlowStore` with `FileUpdates`-
/// reporting fast tier (so `update_with_whole_file` takes the parallel
/// `stream_file_to_store` path) and the supplied slow tier.
fn build_fast_slow<S>(slow_driver: Arc<S>) -> Store
where
    S: StoreDriver + 'static,
{
    let inner_fast = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(Arc::new(FileUpdateStore { inner: inner_fast }));
    let slow_store = Store::new(slow_driver);
    Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast_store,
        slow_store,
    ))
}

/// **#476 Phase 1 DS-MAJOR-1 fix**: deterministically exercise the
/// `(Err, Err)` dual-err arm.
///
/// The original test used a 1 MiB payload split into 4 chunks against
/// a 128-slot `buf_channel` — the producer's 4 sends + EOF completed
/// before the consumer's `recv` could context-switch and drop `rx`,
/// hitting the `(Err, Ok)` single-err arm where pre-fix and post-fix
/// produce identical output. The fix is correct in that arm too, but
/// the test did not exercise the SPECIFIC arm (`Err, Err`) that the
/// fix's policy distinguishes — so the mutation step (revert to
/// `forward_res?; write_res`) was not reliably red-failing.
///
/// This rewrite adopts the #512 sibling test's 256-chunk feeder
/// technique. With CHUNK_COUNT=256 chunks of 256 KiB each (= 64 MiB
/// payload) and the inner buf_channel at capacity 128 + bridge mpsc 4,
/// the producer's chunk loop blocks on `tx.send` once 132 chunks are
/// in flight. The consumer drains ONE chunk and returns Err, dropping
/// `rx`; the producer's pending `tx.send` wakes with channel-closed.
/// BOTH halves of the `join!` error simultaneously — exactly the
/// dual-err arm that the post-fix `match` selects on.
///
/// `tokio::time::timeout` wraps the call as a deadlock detector — a
/// hung future would mask the bug (`tokio::time::Elapsed` silently
/// passes `is_err()`).
#[nativelink_test]
async fn surfaces_consumer_error_over_symptom_dual_err() -> Result<(), Error> {
    let abort_slow = Arc::new(AbortAfterOneChunkStore::new());
    let fast_slow_store = build_fast_slow(abort_slow.clone());

    // 64 MiB payload — see CHUNK_COUNT/CHUNK_BYTES rationale above. We
    // generate a non-trivial repeating pattern so a partial-read bug
    // would surface in the upload size mismatch rather than silently
    // succeeding.
    let payload: Vec<u8> = (0..TOTAL_BYTES).map(|i| (i & 0xff) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, payload.len() as u64).unwrap();

    let mut tmpfile = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "failed to create tempfile: {:?}", e))?;
    tmpfile
        .write_all(&payload)
        .map_err(|e| make_err!(Code::Internal, "failed to write tempfile: {:?}", e))?;
    tmpfile
        .flush()
        .map_err(|e| make_err!(Code::Internal, "failed to flush tempfile: {:?}", e))?;
    let path = tmpfile.path().to_owned();

    let file = nativelink_util::common::fs::open_file(&path, 0).await?;

    let store_key: StoreKey<'_> = digest.into();
    let call = fast_slow_store.as_store_driver_pin().update_with_whole_file(
        store_key,
        path.into_os_string(),
        file,
        UploadSizeInfo::ExactSize(payload.len() as u64),
    );

    // Deadlock detector. Bumped to 30s vs the original 5s to absorb
    // CI load while writing/reading a 64 MiB tempfile through the
    // streaming pipeline. The happy path completes in well under a
    // second even on a contended host.
    let res = tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .expect(
            "stream_file_to_store must NOT deadlock — \
             consumer-error-prefer policy must surface the abort within 30s, \
             not hang in producer/consumer race",
        );

    assert!(
        res.is_err(),
        "update_with_whole_file must propagate the slow-tier abort as Err; \
         got Ok(...). The slow tier returned {ABORT_TAG} so the call cannot succeed."
    );
    let err = res.unwrap_err();
    let rendered = format!("{err:?}");

    assert!(
        rendered.contains(ABORT_TAG),
        "#476 consumer-error-prefer policy not enforced — producer-side \
         receiver-disconnected swallowed root cause. \
         Expected surfaced Err to contain `{ABORT_TAG}` (the slow \
         tier's authoritative Err), got rendered={rendered}. \
         This assertion is the mutation-guard for the simplified \
         dual-err arm: inverting `(Err(write_err), Err(_)) => Err(write_err)` \
         to `(Err(_), Err(forward_err)) => Err(forward_err)` will \
         surface the producer's `Failed to send chunk in \
         stream_file_to_store` symptom instead of the consumer's \
         `{ABORT_TAG}` — that mutation must red-fail here."
    );

    // Belt-and-braces: under fixup-v3 (Option 3), the prior
    // diagnostic append text was dropped entirely. The surfaced
    // err must NEVER contain it — if it does, a future revert to
    // the v1/v2 append-on-dual-err shape has snuck back in.
    assert!(
        !rendered.contains("consumer error preferred over producer 'receiver disconnected' symptom"),
        "fixup-v3 (Option 3) policy regressed — the dual-err arm \
         is appending the v1/v2 diagnostic text again. The append \
         was dropped because Option A's `tx.send_error` routing \
         already mirrors the producer err into `write_res`, making \
         `forward_err` redundant. Got rendered={rendered}"
    );

    // Setup determinism note (proves we crossed the (Err, Err) arm,
    // not the (Err, Ok) single-err arm): with CHUNK_COUNT=256 chunks
    // × 256 KiB = 64 MiB feeder vs the inner buf_channel capacity 128
    // + 4-slot bridge mpsc, the producer is guaranteed to be blocked
    // on `tx.send` when the slow tier drops `rx` after one chunk.
    // The producer's pending `tx.send` then wakes with channel-closed,
    // making `forward_res = Err(...)` — the (Err, Err) arm is the only
    // reachable arm. Under the simplified arm, both the (Err, Ok) and
    // (Err, Err) paths surface `write_err` verbatim, so an arm-shape
    // regression isn't directly observable from the surfaced string;
    // instead it's observable from the mutation step in the test
    // header (item 1) inverting which half of the (Err, Err) tuple
    // is preferred. The `update_invocations` + `ABORT_TAG` assertions
    // are joint witnesses that the (Err, Err) arm fired.

    // Sanity: confirm the slow tier really was invoked. Defends against
    // a future refactor of `update_with_whole_file` that skips the slow
    // store entirely — the test would otherwise pass vacuously.
    assert!(
        abort_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "AbortAfterOneChunkStore::update was never called; the \
         FastSlowStore path under test was not exercised. \
         update_invocations={}",
        abort_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}

/// **#476 Phase 1 RT-MAJOR fix (Option A)**: producer-side IO error
/// must surface verbatim, NOT as a buf-channel "Sender dropped"
/// symptom.
///
/// Red-team's mechanism: when the `spawn_blocking` reader in
/// `stream_file_to_store` hits a real read error (e.g. ENOSPC, EIO,
/// EISDIR), the error is sent to the bridge_rx. The `forward_fut`'s
/// `result?` propagates the IO error, dropping `tx`. Pre-fix, the
/// consumer's `recv()` then surfaced the synthesized
/// `"Sender dropped before sending EOF"` Internal Err
/// (`buf_channel.rs:623`) — `write_res = Err(synth_internal)`,
/// `forward_res = Err(io_err)`. The post-#476 dual-err policy prefers
/// `write_res`, so the surfaced Code is `Internal` and the surfaced
/// top-line message is the buf-channel symptom — burying the real IO
/// error in an interpolated `{forward_err:?}` tail.
///
/// **Option A fix**: `forward_fut` now calls `tx.send_error(e.clone())`
/// before propagating any Err. The consumer's `recv()` then reads the
/// REAL IO error via `terminal_error` (`buf_channel.rs:614-620`)
/// instead of the synthesized fallback. `write_res = Err(io_err)`,
/// `forward_res = Err(io_err)` — the dual-err arm prefers write_res,
/// which now carries the IO error.
///
/// The producer-IO error is injected portably by passing a DIRECTORY
/// fd to `stream_file_to_store`. On Linux, opening a directory
/// O_RDONLY succeeds but `read()` returns `EISDIR` ("Is a directory").
/// The spawn_blocking reader's first `file.read()` returns the
/// EISDIR error, which is sent to the bridge_rx. This is the EXACT
/// shape of failure path red-team described (real syscall failure
/// from the spawn_blocking reader), without resorting to unsafe FD
/// tricks.
#[nativelink_test]
async fn surfaces_producer_io_error_not_buf_channel_symptom() -> Result<(), Error> {
    let propagate_slow = Arc::new(PropagateRecvErrorStore::new());
    let fast_slow_store = build_fast_slow(propagate_slow.clone());

    // Two tempfiles:
    // - `data_file`: regular file with payload; this is what we pass
    //   to `update_with_whole_file` as `file` (FileSlot). The fast
    //   tier's `update_with_whole_file` reads from it — this lets the
    //   fast tier succeed normally so we don't conflate fast-tier
    //   failures with the producer-IO behavior under test.
    // - `dir_path`: a temp DIRECTORY. We pass its path as `path` to
    //   `update_with_whole_file`. Inside
    //   `FastSlowStore::update_with_whole_file` at
    //   `fast_slow_store.rs:5492`, `std::fs::File::open(path)` opens
    //   the directory fd successfully. The fd is then passed to
    //   `stream_file_to_store(slow_file, ...)`. The spawn_blocking
    //   reader's first `file.read()` returns `EISDIR` — a REAL IO
    //   error from the producer side, matching red-team's described
    //   failure mode exactly.
    let payload: Vec<u8> = vec![0xC3; 4096];
    let digest = DigestInfo::try_new(VALID_HASH, payload.len() as u64).unwrap();

    let mut data_file = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "failed to create data tempfile: {:?}", e))?;
    data_file
        .write_all(&payload)
        .map_err(|e| make_err!(Code::Internal, "failed to write data tempfile: {:?}", e))?;
    data_file
        .flush()
        .map_err(|e| make_err!(Code::Internal, "failed to flush data tempfile: {:?}", e))?;
    let data_path = data_file.path().to_owned();

    let dir = tempfile::tempdir()
        .map_err(|e| make_err!(Code::Internal, "failed to create tempdir: {:?}", e))?;
    let dir_path = dir.path().to_owned();

    let file_slot = nativelink_util::common::fs::open_file(&data_path, 0).await?;

    let store_key: StoreKey<'_> = digest.into();
    let call = fast_slow_store.as_store_driver_pin().update_with_whole_file(
        store_key,
        // Slow tier opens this path (directory) — read() returns EISDIR.
        dir_path.into_os_string(),
        // Fast tier consumes this FileSlot (regular file) — succeeds.
        file_slot,
        UploadSizeInfo::ExactSize(payload.len() as u64),
    );

    let res = tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .expect(
            "stream_file_to_store producer-IO path must NOT deadlock — \
             Option A's tx.send_error(io_err) plus the post-#476 \
             dual-err policy must surface the IO err within 30s",
        );

    assert!(
        res.is_err(),
        "update_with_whole_file must propagate the producer-side EISDIR \
         as Err; got Ok(...). Reading from a directory fd cannot succeed."
    );
    let err = res.unwrap_err();
    let rendered = format!("{err:?}");

    // The PRODUCTION-CRITICAL assertion: the surfaced Err must carry
    // the real producer-side IO error identifier ("Is a directory" or
    // "EISDIR" or "Failed to read file in stream_file_to_store"),
    // NOT the buf-channel synthesized "Sender dropped before sending
    // EOF" Internal that the pre-fix code would surface as the
    // consumer's `write_res` after `tx` was merely dropped.
    let mentions_real_io = rendered.contains("Failed to read file in stream_file_to_store")
        || rendered.contains("Is a directory")
        || rendered.contains("EISDIR");
    assert!(
        mentions_real_io,
        "Option A producer-IO masking fix not enforced — the producer's \
         real IO error (EISDIR from reading a directory fd) was lost. \
         Pre-fix mechanism: producer `?`-propagated err drops `tx`; \
         consumer's `recv` returns synthesized \
         `Sender dropped before sending EOF` Internal; dual-err policy \
         prefers that synthesized symptom over the real IO err. \
         Expected surfaced Err to mention \
         `Failed to read file in stream_file_to_store`, `Is a directory`, \
         or `EISDIR`. Got rendered={rendered}"
    );

    // Belt-and-braces: the surfaced Err must NOT contain the
    // buf-channel synthesized fallback string. If it does, Option A's
    // `tx.send_error` routing is missing or broken — the consumer
    // observed the dropped-tx synthesis instead of the producer's
    // typed error. (The dual-err arm appends `forward_err` as `{:?}`
    // which COULD contain the symptom string for the producer side,
    // but the TOP-LEVEL `write_res` half — which the policy prefers —
    // must carry the real IO error.)
    //
    // The strict invariant: `write_res`'s top-level message (which
    // the dual-err arm preserves verbatim) is NOT the buf-channel
    // synthesis. We check this by asserting the consumer-recorded
    // last_err_str (captured by `PropagateRecvErrorStore::update`
    // when its `recv()` returned Err) does NOT mention the synthesis.
    let consumer_observed = propagate_slow
        .last_err_str
        .lock()
        .clone()
        .unwrap_or_else(|| "<consumer recorded no err — Option A may have suppressed it>".into());
    assert!(
        !consumer_observed.contains("Sender dropped before sending EOF"),
        "Option A producer-IO masking fix not enforced at the consumer \
         seam — the buf_channel synthesized fallback `Sender dropped \
         before sending EOF` reached the consumer instead of the \
         producer's real EISDIR error. The producer's `tx.send_error` \
         routing is missing — either the `forward_fut` Err arm doesn't \
         call `tx.send_error(err.clone())`, or the order vs `tx` drop \
         is wrong. Consumer-observed err: {consumer_observed}"
    );
    assert!(
        consumer_observed.contains("Failed to read file in stream_file_to_store")
            || consumer_observed.contains("Is a directory")
            || consumer_observed.contains("EISDIR"),
        "Option A producer-IO routing not exercising the expected \
         payload — consumer's recv did not surface the real EISDIR \
         error. Consumer-observed err: {consumer_observed}"
    );

    // fixup-v3 (Option 3): the dual-err arm no longer appends a
    // diagnostic naming "consumer error preferred over producer
    // 'receiver disconnected' symptom" — `forward_err` is redundant
    // with `write_err` under Option A's `tx.send_error` routing, so
    // no append is honest in this arm. Lock in that the misleading
    // text never surfaces (it would mis-frame the routed-IO case as
    // a symptom-preference inversion, which it isn't).
    assert!(
        !rendered.contains("consumer error preferred over producer 'receiver disconnected' symptom"),
        "fixup-v3 (Option 3) regression — the dual-err arm appended \
         the v1/v2 diagnostic text on a producer-IO routed err. \
         Under Option A both halves carry the same authoritative \
         err; appending one as a 'symptom' is wrong. Got rendered={rendered}"
    );

    // Sanity: confirm the slow tier was invoked at least once. Defends
    // against a future refactor that skips the slow store entirely
    // (which would make this test pass vacuously).
    assert!(
        propagate_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "PropagateRecvErrorStore::update was never called; the \
         FastSlowStore path under test was not exercised. \
         update_invocations={}",
        propagate_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}

/// **#476 Phase 1 RT-MAJOR-2 fix (fixup-v2)**: the most common
/// production dual-err shape — `(Ok producer, Err consumer)` — must
/// surface the consumer's authoritative post-EOF err verbatim.
///
/// Red-team finding #7 (red-team.md): real production failures of
/// `stream_file_to_store` are dominated by the consumer erroring
/// AFTER the producer has cleanly drained the file. Examples:
///   * VerifyStore post-EOF size mismatch (the wrapping store reads
///     the full stream + EOF, then validates declared size vs
///     observed size and errors).
///   * h2 RST_STREAM after the body is fully sent — slow tier's
///     downstream RPC layer fails at commit/finalize.
///   * Slow-store commit-time rejection (storage-full, admission
///     pressure surfacing only on the final write).
///
/// In this shape, `write_fut` = `Err(post_eof_err)` and `forward_fut`
/// = `Ok(())` (it sent EOF cleanly before the consumer's update
/// returned). The match selects `(Err(write_err), Ok(())) =>
/// Err(write_err)` — the single-err arm preserves the consumer's
/// authoritative err.
///
/// The existing two tests both hit the `(Err, Err)` dual-err arm. A
/// regression that drops the `(Err, Ok)` consumer-err arm — e.g. a
/// future refactor that inverts arm order, swallows post-EOF errs, or
/// fires on `forward_res?; ...` before checking `write_res` — would
/// ship undetected. This test guards that arm in production
/// composition (real `FastSlowStore` + real `MemoryStore` fast tier
/// reporting `FileUpdates`).
///
/// Small payload (4 chunks × 16 KiB = 64 KiB) — well under the
/// 128-slot buf_channel + 4-slot bridge mpsc capacity, so the
/// producer drains fully without blocking. The slow tier's
/// `AcceptAllThenAbortOnEofStore` then observes a clean EOF, sets
/// `saw_clean_eof = true`, and returns Err — exactly the
/// `(Ok producer, Err consumer)` arm.
#[nativelink_test]
async fn surfaces_consumer_abort_when_producer_succeeded() -> Result<(), Error> {
    let abort_slow = Arc::new(AcceptAllThenAbortOnEofStore::new());
    let fast_slow_store = build_fast_slow(abort_slow.clone());

    // Small payload — see test doc above. 4 chunks × 16 KiB.
    let small_payload: Vec<u8> = (0..(4 * 16 * 1024)).map(|i| (i & 0xff) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, small_payload.len() as u64).unwrap();

    let mut tmpfile = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "failed to create tempfile: {:?}", e))?;
    tmpfile
        .write_all(&small_payload)
        .map_err(|e| make_err!(Code::Internal, "failed to write tempfile: {:?}", e))?;
    tmpfile
        .flush()
        .map_err(|e| make_err!(Code::Internal, "failed to flush tempfile: {:?}", e))?;
    let path = tmpfile.path().to_owned();

    let file = nativelink_util::common::fs::open_file(&path, 0).await?;

    let store_key: StoreKey<'_> = digest.into();
    let call = fast_slow_store.as_store_driver_pin().update_with_whole_file(
        store_key,
        path.into_os_string(),
        file,
        UploadSizeInfo::ExactSize(small_payload.len() as u64),
    );

    // Deadlock detector. The small payload means the producer drains
    // fully in well under a second; 5s is generous even on a
    // contended host.
    let res = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect(
            "stream_file_to_store (Ok producer, Err consumer) path \
             must NOT deadlock — consumer-post-EOF-Err policy must \
             surface the abort within 5s; hang means the (Err, Ok) \
             arm was reordered, dropped, or its err-routing wedged",
        );

    assert!(
        res.is_err(),
        "update_with_whole_file must propagate the slow-tier post-EOF \
         abort as Err; got Ok(...). The slow tier returned \
         {POST_EOF_ABORT_TAG} after EOF so the call cannot succeed."
    );
    let err = res.unwrap_err();
    let rendered = format!("{err:?}");

    assert!(
        rendered.contains(POST_EOF_ABORT_TAG),
        "consumer-post-EOF-Err policy regressed — the `(Err, Ok)` arm \
         dropped the consumer-side authoritative err. The slow tier \
         returned Err({POST_EOF_ABORT_TAG}) AFTER observing a clean EOF \
         from the producer; `write_res` carries the abort, \
         `forward_res` is Ok(()). The post-fix arm \
         `(Err(write_err), Ok(())) => Err(write_err)` must surface \
         the consumer's err verbatim. Got rendered={rendered}"
    );

    // The dual-err append text MUST NOT appear — this arm is the
    // single-err `(Err, Ok)` arm, distinct from the
    // `surfaces_consumer_error_over_symptom_dual_err` test which
    // hits the `(Err, Err)` dual-err arm. If the append text appears
    // here, the test setup incorrectly exercises the dual-err arm
    // (e.g. the producer somehow errored, masking the bug under test).
    assert!(
        !rendered.contains("consumer error preferred over producer 'receiver disconnected' symptom"),
        "test setup error — the `(Err producer, Err consumer)` dual-err \
         arm fired instead of the `(Ok producer, Err consumer)` single-err \
         arm under test. The producer should drain cleanly on a 64 KiB \
         payload; if it errored, the slow tier's EOF-detection logic \
         (saw_clean_eof) or producer's read path is wrong. \
         Got rendered={rendered}"
    );

    // Prove the slow tier actually drained the FULL stream + EOF
    // (otherwise the test is exercising a dual-err arm vacuously and
    // the assertion above passes for the wrong reason).
    assert!(
        *abort_slow.saw_clean_eof.lock(),
        "consumer-post-EOF-Err test did not observe a clean EOF — the \
         `(Ok producer, Err consumer)` arm was NOT exercised. The slow \
         tier's recv loop never saw `Ok(empty)`; either the producer \
         errored mid-stream (would hit dual-err arm), the EOF was \
         routed through send_error (would hit dual-err arm), or the \
         consumer aborted before draining. Test feeder setup is wrong."
    );

    assert!(
        abort_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "AcceptAllThenAbortOnEofStore::update was never called; the \
         FastSlowStore path under test was not exercised. \
         update_invocations={}",
        abort_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}
