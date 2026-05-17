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
//! at `fast_slow_store.rs:4322-4327` (`stream_file_to_store`) and
//! `:4220-4252` (`stream_path_to_store`) short-circuits on
//! `forward_res?`, discarding `write_res`.
//!
//! **Mechanism that re-establishes it (post-fix):** `match (write_res,
//! forward_res)` that prefers `write_res` on dual-err with appended
//! diagnostic naming the symptom-vs-cause inversion.
//!
//! **Seams crossed by this test:**
//!   1. Producer: `spawn_blocking` reader → bridge mpsc
//!   2. Consumer wrapper: `forward_fut` → buf_channel `tx`
//!   3. Consumer (slow store): `update(rx)` — drops `rx` mid-stream
//!   4. The `join!` aggregator at `:4246` / `:4322`
//!   5. Final `match` that selects which error to surface
//!   6. `FastSlowStore::update_with_whole_file` wrapper that calls into
//!      `stream_file_to_store` (the seam through which the test reaches
//!      the private helper, since both `stream_*_to_store` are
//!      private `async fn`)
//!   7. `Store::update_with_whole_file` `err_tip` wrapping at `:5450`
//!
//! The test wraps the unit in production composition (real
//! `MemoryStore` fast tier reporting `FileUpdates`, real
//! `FastSlowStore`, fake slow tier that aborts mid-stream with a
//! uniquely-tagged Err) and asserts via SPECIFIC message — not
//! `is_err()`. `tokio::time::timeout` wraps the call as a deadlock
//! detector.
//!
//! Mutation step (run by hand to verify):
//!   1. Revert the dual-err arm to `forward_res?; write_res`.
//!   2. Re-run this test. It MUST red-fail with the bespoke
//!      "consumer-error-prefer policy not enforced" message.

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

/// Unique tag asserted-on in test output. If this string fails to appear
/// in the surfaced `Err`, the consumer-error-prefer policy is broken and
/// the producer's "receiver disconnected" symptom was promoted in its
/// place.
const ABORT_TAG: &str = "TEST_SLOW_STORE_ABORTED_FOR_DIAGNOSTIC_TEST";

const VALID_HASH: &str =
    "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const PAYLOAD_LEN: usize = 1024 * 1024; // 1 MiB — multi-chunk vs 256 KiB CHUNK_SIZE.

/// Fast-tier wrapper around `MemoryStore` that reports
/// `StoreOptimizations::FileUpdates`. This is the precondition for
/// `FastSlowStore::update_with_whole_file` to take the parallel
/// `stream_file_to_store` path (see `fast_slow_store.rs:5409-5450`).
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
/// The producer's next `tx.send(chunk)` then surfaces a buf_channel
/// "receiver disconnected" symptom. Both halves of the `join!` error;
/// the post-fix code MUST surface this `update()` error rather than
/// the producer's symptom.
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
        // Consume one chunk so the producer is fully engaged before we
        // drop. Without this, the test could race-win the send path and
        // dual-err would not be reliably reproduced.
        let _chunk = reader.recv().await;
        // Returning Err drops `reader` (and thus `rx` from the
        // buf_channel pair) before EOF. The producer side then errors
        // with "receiver disconnected" on its next send.
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

/// Production composition: `FastSlowStore` with `FileUpdates`-reporting
/// fast tier (so `update_with_whole_file` takes the parallel
/// `stream_file_to_store` path) and `AbortAfterOneChunkStore` as the
/// slow tier (so the consumer half errors with `ABORT_TAG`, dropping
/// `rx` and triggering the producer's "receiver disconnected" symptom).
/// Asserts the surfaced Err carries `ABORT_TAG`, NOT the buf_channel
/// symptom. `tokio::time::timeout` wraps the call as a deadlock
/// detector — a hung future would mask the bug.
#[nativelink_test]
async fn stream_file_to_store_surfaces_consumer_error_over_symptom() -> Result<(), Error> {
    let inner_fast = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(Arc::new(FileUpdateStore {
        inner: inner_fast,
    }));
    let abort_slow = Arc::new(AbortAfterOneChunkStore::new());
    let slow_store = Store::new(abort_slow.clone());
    let fast_slow_store = Store::new(FastSlowStore::new(
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
    ));

    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
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

    // Deadlock detector. Without the timeout, a wedge in either half of
    // the join! would silently consume the agent's time budget; with
    // it, we fail loudly within bounded wall-clock.
    let res = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect(
            "stream_file_to_store must NOT deadlock — \
             consumer-error-prefer policy must surface the abort within 5s, \
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
        "consumer-error-prefer policy not enforced — producer-side \
         receiver-disconnected swallowed root cause. \
         Expected surfaced Err to contain `{ABORT_TAG}` (the slow \
         tier's authoritative Err), got rendered={rendered}"
    );

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
