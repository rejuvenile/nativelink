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

//! #56/#59/#62 — `stream_file_to_store`: consumer-Ok-authoritative fix.
//!
//! **Invariant under test:** when the consumer (`store.update`) returns
//! `Ok(())`, the operation as a whole must succeed regardless of any
//! subsequent producer-side stream error. Consumer Ok is the
//! authoritative commit signal: the blob is durable (or already present)
//! in the store.
//!
//! **Production trigger (2026-06-12T16:18:23Z, worker 1f165aa0):**
//! GrpcStore received a 293 MB dSYM upload; the server already had the
//! blob and responded `AlreadyExists` → `Ok(committed_size=0)`, dropping
//! the inbound stream. The consumer future returned `Ok(())` in ~5 ms.
//! The `rx` was dropped, so the producer's next `tx.send(chunk)` returned
//! `"Failed to write to data, receiver disconnected"`. The
//! `(Ok consumer, Err producer)` arm in `stream_file_to_store` propagated
//! the producer error — failing an action whose outputs were already in
//! the CAS. Happened twice within 38 minutes.
//!
//! **Mechanism that violates it (pre-fix):**
//! `nativelink-store/src/fast_slow_store.rs` `stream_file_to_store`
//! `(Ok(()), Err(forward_err)) => Err(forward_err)` arm.
//!
//! **Mechanism that re-establishes it (fix):**
//! Change that arm to log the producer error (digest + error context) and
//! return `Ok(())`. Consumer Ok is authoritative; producer Err after
//! consumer Ok is a benign symptom of the receiver being dropped.
//!
//! **Why `tx.send_error` does NOT collapse this into `(Err, Err)`:**
//! When `tx.send(chunk)` fails (receiver dropped), `buf_channel`
//! sets `self.tx = None`. The subsequent `tx.send_error(e.clone())` in
//! `forward_fut` is an idempotent no-op (the OnceLock may still be set but
//! `tx` is already None so the receiver is already closed). The consumer
//! already called `rx.recv()` and has completed. The `send_error` call
//! does NOT reach the consumer. Therefore `write_res = Ok(())`,
//! `forward_res = Err(receiver disconnected)` — the `(Ok, Err)` arm IS
//! reachable in production, contrary to the claim in the #476 coverage
//! matrix comment.
//!
//! **Composite invariants this fix interacts with:**
//! * Digest-addressed store contract: consumer Ok means the blob for that
//!   exact digest is committed or already present; content correctness is
//!   not weakened.
//! * ≥2-replica durability: unaffected; the blob is server-side when the
//!   consumer says Ok.
//! * VerifyStore: verification happens inside the consumer chain before
//!   its Ok; the fix does not bypass it.
//! * `(consumer Err, producer Err/Ok)` arms: unchanged — consumer Err
//!   always propagates.
//!
//! **Seams crossed:**
//!   1. Producer: `spawn_blocking` reader → bridge mpsc
//!   2. Consumer wrapper: `forward_fut` → buf_channel `tx`
//!      (Option A `tx.send_error` routing — becomes a no-op here because
//!      `tx` was cleared by the failed `send`)
//!   3. Consumer (slow store): `update(rx)` — drops `rx` immediately and
//!      returns `Ok(())`
//!   4. The `join!` aggregator inside `stream_file_to_store`
//!   5. Final `match` — the `(Ok(()), Err(forward_err))` arm
//!   6. `FastSlowStore::update_with_whole_file` calling
//!      `stream_file_to_store` (the path that routes here for
//!      `StoreOptimizations::FileUpdates` fast tiers)
//!   7. `Store::update_with_whole_file` `err_tip` wrapping
//!
//! **Mutation step (CLAUDE.md mandatory TDD step 5):**
//!   Re-comment the fix in `stream_file_to_store`'s `(Ok(()), Err(_))`
//!   arm (revert to `Err(forward_err)`). Re-run
//!   `consumer_ok_swallows_producer_receiver_disconnect`. Must red-fail
//!   with the bespoke message:
//!   `"#56/#62 consumer-Ok-authoritative invariant not enforced — \
//!    (Ok, Err) arm propagated producer error when consumer already \
//!    committed the blob"`.
//!
//! **Inverse guard:**
//!   `consumer_err_still_propagates_when_producer_also_errs` — verifies
//!   that the fix does NOT swallow consumer Err. When the consumer Err +
//!   producer Err, the consumer's Err must still propagate verbatim
//!   (the existing #476 policy). This test must PASS both before and
//!   after the fix (it is an invariant guard, not a regression test for
//!   the fix itself).

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

/// Unique tag for the inverse-guard consumer-Err test. Must appear in the
/// surfaced error when the consumer errors; if it doesn't, the fix
/// accidentally swallowed a real consumer Err.
const CONSUMER_ERR_TAG: &str = "TEST_CONSUMER_ERR_FOR_56_62_INVERSE_GUARD";

const VALID_HASH: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Number of 256 KiB chunks fed into `stream_file_to_store`.
///
/// The inner `buf_channel` has capacity 128 (`fast_slow_store.rs:4656`)
/// and the bridge mpsc has 4 slots (`fast_slow_store.rs:4660`). The
/// producer's chunk loop blocks on `tx.send` once 132 chunks are in
/// flight. Sending 256 chunks (= 64 MiB) guarantees the producer is
/// mid-send when the consumer drops `rx`, deterministically producing
/// the `(Ok consumer, Err producer)` arm under test.
const CHUNK_BYTES: usize = 256 * 1024;
const CHUNK_COUNT: usize = 256;
const TOTAL_BYTES: usize = CHUNK_BYTES * CHUNK_COUNT;

/// Fast-tier wrapper that reports `StoreOptimizations::FileUpdates`,
/// routing `update_with_whole_file` through the `stream_file_to_store`
/// parallel path (see `fast_slow_store.rs` `update_with_whole_file`
/// dispatch). Without this, the function falls through to other branches
/// and the helper under test is never exercised.
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

/// Slow-tier fake that immediately drops `rx` and returns `Ok(())`,
/// simulating GrpcStore's `AlreadyExists → Ok` path (the server already
/// has the blob and closes the inbound stream). With a 256-chunk feeder
/// and the inner buf_channel at capacity 128 + bridge mpsc 4, the
/// producer is guaranteed to be mid-send when `rx` is dropped —
/// deterministically producing `(Ok consumer, Err producer)`.
#[derive(MetricsComponent)]
struct DropImmediatelyOkStore {
    update_invocations: AtomicUsize,
}

impl DropImmediatelyOkStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for DropImmediatelyOkStore {
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
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drop `_reader` immediately (rx dropped here) and return Ok.
        // This simulates GrpcStore receiving AlreadyExists from the server
        // and returning Ok without draining the stream, which leaves the
        // producer's in-flight tx.send to return "receiver disconnected".
        Ok(())
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
            "DropImmediatelyOkStore: get_part not supported"
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

default_health_status_indicator!(DropImmediatelyOkStore);

/// Slow-tier fake that drops `rx` immediately and returns `Err`.
/// Used by the inverse-guard test to verify that consumer Err is still
/// propagated even when the producer also errors. This is the existing
/// #476 dual-err policy: `(Err(write_err), Err(_)) => Err(write_err)`.
#[derive(MetricsComponent)]
struct DropImmediatelyErrStore {
    update_invocations: AtomicUsize,
}

impl DropImmediatelyErrStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for DropImmediatelyErrStore {
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
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drop `_reader` immediately (rx dropped) and return Err.
        // With a 256-chunk feeder, the producer also errors — dual-err.
        // The consumer's Err must survive as the authoritative result.
        Err(make_err!(Code::Aborted, "{CONSUMER_ERR_TAG}"))
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
            "DropImmediatelyErrStore: get_part not supported"
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

default_health_status_indicator!(DropImmediatelyErrStore);

/// Build a production-composition `FastSlowStore` with a
/// `FileUpdates`-reporting fast tier (routes `update_with_whole_file`
/// through `stream_file_to_store`) and the supplied slow tier.
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

/// **#56/#62 fix**: consumer Ok is authoritative — producer "receiver
/// disconnected" must NOT fail the action.
///
/// This test directly exercises the production failure shape:
///   1. A 64 MiB blob is streamed to `stream_file_to_store`.
///   2. The slow tier's `update()` drops `rx` immediately and returns
///      `Ok(())` (GrpcStore AlreadyExists → Ok path).
///   3. The producer's in-flight `tx.send(chunk)` returns "receiver
///      disconnected"; `forward_fut` returns `Err(...)`.
///   4. `join!(write_fut, forward_fut)` → `(Ok(()), Err(receiver
///      disconnected))` — the pre-fix arm returns `Err(forward_err)`,
///      failing a successfully-committed blob.
///   5. **Post-fix**: this arm must return `Ok(())`.
///
/// **Mutation step**: revert `(Ok(()), Err(_)) => { log; Ok(()) }` back
/// to `(Ok(()), Err(forward_err)) => Err(forward_err)` in
/// `stream_file_to_store`. Re-run — must red-fail with bespoke message:
/// "#56/#62 consumer-Ok-authoritative invariant not enforced — \
/// (Ok, Err) arm propagated producer error when consumer already \
/// committed the blob".
#[nativelink_test]
async fn consumer_ok_swallows_producer_receiver_disconnect() -> Result<(), Error> {
    let ok_slow = Arc::new(DropImmediatelyOkStore::new());
    let fast_slow_store = build_fast_slow(ok_slow.clone());

    // 64 MiB payload — ensures the producer is mid-send when the
    // consumer drops rx (the inner buf_channel capacity is 128 + bridge
    // mpsc 4 = 132 slots; with 256 chunks the producer blocks on
    // tx.send when the consumer has already returned Ok).
    let payload: Vec<u8> = (0..TOTAL_BYTES).map(|i| (i & 0xff) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, payload.len() as u64).unwrap();

    let mut tmpfile = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "failed to create tempfile: {e:?}"))?;
    tmpfile
        .write_all(&payload)
        .map_err(|e| make_err!(Code::Internal, "failed to write tempfile: {e:?}"))?;
    tmpfile
        .flush()
        .map_err(|e| make_err!(Code::Internal, "failed to flush tempfile: {e:?}"))?;
    let path = tmpfile.path().to_owned();

    let file = nativelink_util::common::fs::open_file(&path, 0).await?;

    let store_key: StoreKey<'_> = digest.into();
    let call = fast_slow_store.as_store_driver_pin().update_with_whole_file(
        store_key,
        path.into_os_string(),
        file,
        UploadSizeInfo::ExactSize(payload.len() as u64),
    );

    // Deadlock detector. 30 s absorbs CI load for a 64 MiB tempfile
    // through the streaming pipeline; the happy path completes in well
    // under a second on any host.
    let res = tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .expect(
            "stream_file_to_store must NOT deadlock — consumer-Ok-authoritative \
             policy must complete within 30s; a hang means the join! or the \
             (Ok, Err) arm is wedged",
        );

    assert!(
        res.is_ok(),
        "#56/#62 consumer-Ok-authoritative invariant not enforced — \
         (Ok, Err) arm propagated producer error when consumer already \
         committed the blob. The slow tier returned Ok(()) (simulating \
         GrpcStore AlreadyExists → Ok); producer hit receiver-disconnected. \
         Expected Ok(Some(_)) (fast tier returns the FileSlot), got: {res:?}"
    );

    // Sanity: confirm the slow tier was actually invoked.
    assert!(
        ok_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "DropImmediatelyOkStore::update was never called; the \
         FastSlowStore stream_file_to_store path under test was not \
         exercised. update_invocations={}",
        ok_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}

/// **#56/#62 inverse guard**: consumer Err still propagates.
///
/// The fix changes only the `(Ok(()), Err(_))` arm; the
/// `(Err(write_err), Err(_))` arm must continue to surface the
/// consumer's error as per the existing #476 dual-err policy.
///
/// This test uses `DropImmediatelyErrStore` (consumer drops rx and
/// returns Err) with the same 64 MiB payload, producing
/// `(Err consumer, Err producer)`. The consumer's tagged Err must
/// appear in the surfaced error.
///
/// This test must PASS both before and after the fix — it guards that
/// the fix does NOT regress the consumer-Err propagation path.
#[nativelink_test]
async fn consumer_err_still_propagates_when_producer_also_errs() -> Result<(), Error> {
    let err_slow = Arc::new(DropImmediatelyErrStore::new());
    let fast_slow_store = build_fast_slow(err_slow.clone());

    let payload: Vec<u8> = (0..TOTAL_BYTES).map(|i| (i & 0xff) as u8).collect();
    let digest = DigestInfo::try_new(VALID_HASH, payload.len() as u64).unwrap();

    let mut tmpfile = tempfile::NamedTempFile::new()
        .map_err(|e| make_err!(Code::Internal, "failed to create tempfile: {e:?}"))?;
    tmpfile
        .write_all(&payload)
        .map_err(|e| make_err!(Code::Internal, "failed to write tempfile: {e:?}"))?;
    tmpfile
        .flush()
        .map_err(|e| make_err!(Code::Internal, "failed to flush tempfile: {e:?}"))?;
    let path = tmpfile.path().to_owned();

    let file = nativelink_util::common::fs::open_file(&path, 0).await?;

    let store_key: StoreKey<'_> = digest.into();
    let call = fast_slow_store.as_store_driver_pin().update_with_whole_file(
        store_key,
        path.into_os_string(),
        file,
        UploadSizeInfo::ExactSize(payload.len() as u64),
    );

    let res = tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .expect(
            "stream_file_to_store must NOT deadlock — consumer-Err path \
             must complete within 30s; a hang means the join! is wedged",
        );

    assert!(
        res.is_err(),
        "#56/#62 inverse guard: consumer Err must still propagate; \
         got Ok. DropImmediatelyErrStore returned Err({CONSUMER_ERR_TAG}) \
         so the result cannot be Ok."
    );
    let rendered = format!("{:?}", res.unwrap_err());
    assert!(
        rendered.contains(CONSUMER_ERR_TAG),
        "#56/#62 inverse guard: consumer Err tag not found in surfaced error. \
         The (Err consumer, Err producer) dual-err arm must surface the \
         consumer's authoritative Err ({CONSUMER_ERR_TAG}), not the \
         producer's receiver-disconnected symptom. Got rendered={rendered}"
    );

    // Sanity: confirm the slow tier was actually invoked.
    assert!(
        err_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "DropImmediatelyErrStore::update was never called; the \
         FastSlowStore stream_file_to_store path under test was not \
         exercised. update_invocations={}",
        err_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}
