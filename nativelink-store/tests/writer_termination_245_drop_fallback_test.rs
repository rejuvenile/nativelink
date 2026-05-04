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

//! #245 follow-up — guard-removal regression coverage for the
//! `WriteHalfGuard::new(&mut tx)` wraps in `compression_store::update`
//! and `fast_slow_store::update`'s legacy + chunked `data_stream_fut`s.
//!
//! Per testing-czar review of #245 (`.claude/reviews/245-writer-termination-fix/
//! testing-czar.md`), the new guard wraps at:
//!   - `compression_store.rs:334` (write_fut),
//!   - `fast_slow_store.rs:731-732` (chunked data_stream_fut, two guards),
//!   - `fast_slow_store.rs:3102` (legacy data_stream_fut),
//! are defense-in-depth on `?` propagation paths. The original #245
//! tests cover `verify_store::inner_check_update` (the production
//! symptom site) but do NOT exercise these wraps. A future commit that
//! removed `let mut tx_guard = WriteHalfGuard::new(&mut tx);` from any
//! of those sites would silently regress — the success-branch tests in
//! `compression_store_test.rs` / `fast_slow_store_test.rs` would still
//! pass because `commit_eof()` is the only behavioral change on the
//! success path.
//!
//! These tests inject an Err on the `?`-propagation path and assert the
//! paired reader observes the WriteHalfGuard Drop fallback's wire-side
//! marker (`"buf_channel: writer dropped without commit"`) — which is
//! ONLY produced by the guard's `Drop`. Without the guard the reader
//! would observe the synthesized `"Sender dropped before sending EOF"`
//! Internal instead.
//!
//! ## Mutation step (per CLAUDE.md TDD step 5; load-bearing line)
//!
//! For each test, the mutation is to remove (or comment out) the
//! `WriteHalfGuard::new(&mut <tx>)` line, then update every
//! `(*<tx_guard>).send(...)` to `<tx>.send(...)` and every
//! `<tx_guard>.commit_eof()?` to `<tx>.send_eof()?` (the minimum
//! mechanical change to keep the call sites compiling). The test
//! observation `inner_observed.contains(DROP_FALLBACK_IDENTIFIER)` then
//! flips to false (the `?` exit drops raw `tx` silently → reader sees
//! `"Sender dropped before sending EOF"` instead of the marker), and
//! the bespoke assertion message panics.
//!
//! Verified manually 2026-05-04 during the testing-czar follow-up.

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::stores::{
    CompressionAlgorithm, CompressionSpec, Lz4Config, MemorySpec, StoreSpec,
};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::compression_store::CompressionStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, UploadSizeInfo,
};
use parking_lot::Mutex;

#[cfg(feature = "chunked_fast_slow")]
use core::sync::atomic::AtomicUsize;
#[cfg(feature = "chunked_fast_slow")]
use nativelink_config::stores::FastSlowSpec;
#[cfg(feature = "chunked_fast_slow")]
use nativelink_store::fast_slow_store::FastSlowStore;
#[cfg(feature = "chunked_fast_slow")]
use nativelink_store::memory_store::MemoryStore;
#[cfg(feature = "chunked_fast_slow")]
use nativelink_util::store_trait::StoreLike;

/// Tight upper bound. A non-deadlocked Err round-trip is sub-millisecond;
/// 5s leaves headroom for a slow CI runner without masking a real
/// deadlock.
const NO_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Wire-side identifier the synthesized "Sender dropped" error carries
/// when a writer drops `tx` without `send_eof`/`send_error`. Its
/// presence in the inner observation means the WriteHalfGuard wrap was
/// removed — Drop did NOT synthesize the structured marker.
const SENDER_DROPPED_IDENTIFIER: &str = "Sender dropped before sending EOF";

/// Wire-side identifier the `WriteHalfGuard::Drop` fallback synthesizes
/// when the owning function exits via `?` without committing. Its
/// presence in the inner observation proves the guard wrap is in place.
const DROP_FALLBACK_IDENTIFIER: &str = "buf_channel: writer dropped without commit";

/// Valid SHA-256 hex string used for digest construction.
const VALID_HASH_HEX: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// ===========================================================================
// Inner-store fakes
// ===========================================================================

/// Inner store that drains `rx` to terminal (EOF or Err) and records
/// the result. Used by the compression-store test to observe what the
/// inner store sees when `compression_store::update`'s write_fut bails
/// via `?`.
#[derive(Debug, MetricsComponent)]
struct ReaderObservingInnerStore {
    last_observed: Arc<Mutex<Option<Result<(), Error>>>>,
    update_was_called: Arc<AtomicBool>,
}

default_health_status_indicator!(ReaderObservingInnerStore);

impl ReaderObservingInnerStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            last_observed: Arc::new(Mutex::new(None)),
            update_was_called: Arc::new(AtomicBool::new(false)),
        })
    }
}

#[async_trait]
impl StoreDriver for ReaderObservingInnerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_was_called.store(true, Ordering::Release);
        let observed: Result<(), Error> = async {
            loop {
                let chunk = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to read buffer in fast_slow chunked dispatch")?;
                if chunk.is_empty() {
                    return Ok(());
                }
            }
        }
        .await;

        *self.last_observed.lock() = Some(observed.clone());
        observed
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        unreachable!("get_part not exercised by these tests")
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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

// ===========================================================================
// C7 / Test 1 — compression_store guard-removal regression
// ===========================================================================

/// `compression_store::update` wraps `tx` in `WriteHalfGuard` (the
/// `?`-propagation paths inside `write_fut` rely on the Drop fallback
/// to terminate `tx` so the inner store's `rx.recv()` returns a
/// structured Err instead of the synthesized "Sender dropped"
/// Internal). A future commit that removed the
/// `let mut tx_guard = WriteHalfGuard::new(&mut tx);` line at
/// `compression_store.rs:334` would silently regress this contract.
///
/// Mechanism: producer sends nothing then injects a structured
/// `send_error` on the OUTER reader. CompressionStore's `write_fut`
/// successfully sends the LZ4 header, then `reader.consume(...)`
/// returns Err — `?` propagates → `WriteHalfGuard::Drop` fires
/// `send_error("buf_channel: writer dropped without commit")` on `tx`.
/// The inner store's `rx.recv()` first sees the header chunk, then
/// observes the marker.
///
/// Without the guard, the `?` exit would drop `tx` silently and the
/// inner store would observe the synthesized "Sender dropped" Internal.
#[nativelink_test]
async fn compression_store_write_failure_propagates_via_drop_fallback() -> Result<(), Error> {
    const PRODUCER_FRAGMENT: &str = "COMPRESSION_PRODUCER_INJECTED_PROBE";

    let inner = ReaderObservingInnerStore::new();
    let store = CompressionStore::new(
        &CompressionSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            compression_algorithm: CompressionAlgorithm::Lz4(Lz4Config::default()),
        },
        Store::new(inner.clone()),
    )
    .err_tip(|| "Failed to create compression store")?;

    let digest = DigestInfo::try_new(VALID_HASH_HEX, 1024)?;
    let (mut tx, rx) = make_buf_channel_pair();

    // Producer: inject a structured terminal_error before sending any
    // data. CompressionStore's write_fut writes the LZ4 header, then
    // `reader.consume(...)` returns the injected Err — `?` propagates,
    // WriteHalfGuard Drop fires.
    let send_fut = async move {
        tx.send_error(make_err!(Code::Aborted, "{PRODUCER_FRAGMENT}"));
        Result::<(), Error>::Ok(())
    };
    let update_fut = async move {
        Pin::new(store.as_ref())
            .update(StoreKey::Digest(digest), rx, UploadSizeInfo::MaxSize(1024))
            .await
    };

    let outer_res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async move {
        let (send_res, update_res) = tokio::join!(send_fut, update_fut);
        update_res.or_else(|update_err| match send_res {
            Ok(()) => Err(update_err),
            Err(send_err) => Err(update_err.merge(send_err)),
        })
    })
    .await
    .expect(
        "WRITER_TERMINATION_VIOLATED_245_compression_drop_fallback: \
         CompressionStore::update did not return within 5s when write_fut \
         hit a `?`-propagation Err. The WriteHalfGuard wrap at \
         compression_store.rs:334 must terminate tx so the paired inner \
         store's rx.recv() returns instead of blocking forever.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on injected producer err; got {outer_res:?}",
    );

    let observed = inner
        .last_observed
        .lock()
        .as_ref()
        .expect("inner store must have observed the rx termination")
        .clone();
    let observed_err = observed.expect_err(
        "inner store must observe Err on rx when CompressionStore's write_fut \
         bails via `?` on producer-injected reader.consume failure",
    );

    assert!(
        observed_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "compression_store::update's `WriteHalfGuard::new(&mut tx)` wrap at \
         compression_store.rs:334 must fire the Drop fallback on `?` exit so \
         the paired inner store's rx.recv() observes the structured \
         {DROP_FALLBACK_IDENTIFIER:?} marker. If this fragment is absent the \
         guard wrap was removed — re-add it so `?` exits terminate tx \
         structurally instead of dropping it silently. Got: {observed_err:?}",
    );
    assert!(
        !observed_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "compression_store: inner observed the synthesized 'Sender dropped \
         before sending EOF' Internal instead of the WriteHalfGuard Drop \
         fallback marker — the guard wrap at compression_store.rs:334 was \
         removed. Got: {observed_err:?}",
    );
    assert!(
        inner.update_was_called.load(Ordering::Acquire),
        "inner store update must have been invoked",
    );

    Ok(())
}

// ===========================================================================
// C7 / Test 2 — fast_slow_store chunked data_stream_fut guard-removal
// ===========================================================================

/// Recording chunked dispatcher: drains `chunk_rx` to terminal, records
/// the result so the test can assert what the dispatcher observed when
/// `data_stream_fut` bailed via `?`.
#[cfg(feature = "chunked_fast_slow")]
#[derive(Debug)]
struct RecordingDispatcher {
    last_observed: Arc<Mutex<Option<Result<(), Error>>>>,
    invocations: Arc<AtomicUsize>,
}

#[cfg(feature = "chunked_fast_slow")]
#[async_trait]
impl nativelink_store::chunked::BazelChunkedDispatcher for RecordingDispatcher {
    async fn dispatch(
        &self,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<u64, Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let observed: Result<(), Error> = async {
            loop {
                let buf = reader.recv().await?;
                if buf.is_empty() {
                    return Ok(());
                }
            }
        }
        .await;
        *self.last_observed.lock() = Some(observed.clone());
        observed.map(|()| digest.size_bytes())
    }
}

/// Recording fast store: drains `rx` to terminal, records the result so
/// the test can assert what the fast tier observed when
/// `data_stream_fut` bailed via `?`. The chunked path passes the same
/// data into BOTH the fast tier and the dispatcher; both readers must
/// observe the structured marker, not the synthesized "Sender dropped".
#[cfg(feature = "chunked_fast_slow")]
#[derive(Debug, MetricsComponent)]
struct RecordingFastStore {
    last_observed: Arc<Mutex<Option<Result<(), Error>>>>,
}

#[cfg(feature = "chunked_fast_slow")]
default_health_status_indicator!(RecordingFastStore);

#[cfg(feature = "chunked_fast_slow")]
#[async_trait]
impl StoreDriver for RecordingFastStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Pretend nothing exists — so populate / pin paths do NOT short-circuit.
        for r in results.iter_mut() {
            *r = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let observed: Result<(), Error> = async {
            loop {
                let buf = reader.recv().await?;
                if buf.is_empty() {
                    return Ok(());
                }
            }
        }
        .await;
        *self.last_observed.lock() = Some(observed.clone());
        observed
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        unreachable!("get_part not exercised by this test")
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
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

/// `fast_slow_store::update_via_chunked_dispatcher`'s `data_stream_fut`
/// wraps BOTH `fast_tx` AND `chunk_tx` in `WriteHalfGuard`s. Any
/// `?`-propagated Err must fire BOTH guards' Drop fallbacks so the fast
/// tier and the chunked dispatcher both observe the structured marker
/// instead of the synthesized "Sender dropped before sending EOF"
/// Internal. A future commit that removed the
/// `let mut fast_guard = WriteHalfGuard::new(&mut fast_tx);` or
/// `let mut chunk_guard = WriteHalfGuard::new(&mut chunk_tx);` line at
/// `fast_slow_store.rs:731-732` would silently regress this contract.
///
/// Mechanism: producer sends nothing then injects `send_error` on the
/// outer reader. data_stream_fut's `reader.recv()` returns Err on the
/// FIRST iteration → `?` propagates → both guards' Drop fires
/// `send_error("buf_channel: writer dropped without commit")` on
/// `fast_tx` and `chunk_tx`. Both readers (recording fast store +
/// recording dispatcher) observe the marker.
///
/// Without the guards, both `tx`s drop silently and both readers
/// observe the synthesized "Sender dropped" Internal.
#[cfg(feature = "chunked_fast_slow")]
#[nativelink_test]
async fn fast_slow_store_chunked_data_stream_failure_drops_both_guards() -> Result<(), Error> {
    use nativelink_store::chunked::{
        BazelChunkedDispatcherArc, disable_bazel_facing_internal_chunking,
        enable_bazel_facing_internal_chunking,
    };

    // Process-wide kill-switch is global state; serialize with a
    // process-local mutex (mirrors the pattern in
    // `fast_slow_str_key_skips_chunked_dispatch_test.rs`).
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    const PRODUCER_FRAGMENT: &str = "FAST_SLOW_CHUNKED_PRODUCER_INJECTED_PROBE";

    let recording_fast = Arc::new(RecordingFastStore {
        last_observed: Arc::new(Mutex::new(None)),
    });
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fast_store_typed = Store::new(recording_fast.clone());

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: nativelink_config::stores::StoreDirection::default(),
            slow_direction: nativelink_config::stores::StoreDirection::default(),
            chunked_reads_enabled: false,
        },
        fast_store_typed,
        slow,
    );

    let dispatcher_observed: Arc<Mutex<Option<Result<(), Error>>>> =
        Arc::new(Mutex::new(None));
    let invocations = Arc::new(AtomicUsize::new(0));
    let dispatcher: BazelChunkedDispatcherArc = Arc::new(RecordingDispatcher {
        last_observed: dispatcher_observed.clone(),
        invocations: invocations.clone(),
    });
    fss.set_bazel_chunked_dispatcher(dispatcher);
    fss.set_chunked_size_threshold_for_test(1);

    enable_bazel_facing_internal_chunking();

    // Digest key, size > 1 byte threshold → routes through the chunked
    // dispatcher's `data_stream_fut`.
    let mut hash = [0u8; 32];
    hash[0] = 0x42;
    let digest = DigestInfo::new(hash, 1024);
    let key: StoreKey<'static> = digest.into();

    let store: Store = Store::new(fss.clone());
    let (mut tx, rx) = make_buf_channel_pair();

    // Producer: inject the err on the very first send so data_stream_fut's
    // `reader.recv()` returns Err on the first iteration → `?` exits BEFORE
    // any commit_eof, firing BOTH guards' Drop fallbacks.
    let send_fut = async move {
        tx.send_error(make_err!(Code::Aborted, "{PRODUCER_FRAGMENT}"));
        Result::<(), Error>::Ok(())
    };
    let update_fut = async move {
        Pin::new(store.as_store_driver())
            .update(key.borrow(), rx, UploadSizeInfo::MaxSize(1024))
            .await
    };

    let outer_res = tokio::time::timeout(NO_DEADLOCK_TIMEOUT, async move {
        let (send_res, update_res) = tokio::join!(send_fut, update_fut);
        update_res.or_else(|update_err| match send_res {
            Ok(()) => Err(update_err),
            Err(send_err) => Err(update_err.merge(send_err)),
        })
    })
    .await;

    // Restore kill-switch BEFORE assertion to avoid leaking state to
    // sibling tests in the same process.
    disable_bazel_facing_internal_chunking();

    let outer_res = outer_res.expect(
        "WRITER_TERMINATION_VIOLATED_245_fast_slow_chunked_drop_fallback: \
         FastSlowStore::update did not return within 5s when chunked \
         data_stream_fut hit a `?`-propagation Err. The WriteHalfGuard \
         wraps at fast_slow_store.rs:731-732 must terminate fast_tx and \
         chunk_tx so the paired fast-store + dispatcher rx.recv()s return \
         instead of blocking forever.",
    );

    assert!(
        outer_res.is_err(),
        "outer res must Err on injected producer err; got {outer_res:?}",
    );

    // Both readers must have observed the marker.
    let fast_observed = recording_fast
        .last_observed
        .lock()
        .as_ref()
        .expect("recording fast store must have observed the rx termination")
        .clone();
    let fast_err = fast_observed.expect_err(
        "fast tier must observe Err on rx when chunked data_stream_fut bails via `?`",
    );

    assert!(
        fast_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "fast_slow_store chunked data_stream_fut's `WriteHalfGuard::new(&mut \
         fast_tx)` wrap at fast_slow_store.rs:731 must fire the Drop fallback \
         on `?` exit so fast_store_fut's rx.recv() observes the structured \
         {DROP_FALLBACK_IDENTIFIER:?} marker. Got fast observed: {fast_err:?}",
    );
    assert!(
        !fast_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "fast tier observed the synthesized 'Sender dropped before sending EOF' \
         Internal — fast_guard wrap at fast_slow_store.rs:731 was removed. \
         Got: {fast_err:?}",
    );

    let dispatcher_observed_clone = dispatcher_observed
        .lock()
        .as_ref()
        .expect("recording dispatcher must have observed the chunk_rx termination")
        .clone();
    let chunk_err = dispatcher_observed_clone.expect_err(
        "dispatcher must observe Err on chunk_rx when chunked data_stream_fut \
         bails via `?`",
    );

    assert!(
        chunk_err
            .messages
            .iter()
            .any(|m| m.contains(DROP_FALLBACK_IDENTIFIER)),
        "fast_slow_store chunked data_stream_fut's `WriteHalfGuard::new(&mut \
         chunk_tx)` wrap at fast_slow_store.rs:732 must fire the Drop fallback \
         on `?` exit so dispatch_fut's rx.recv() observes the structured \
         {DROP_FALLBACK_IDENTIFIER:?} marker. Got dispatcher observed: \
         {chunk_err:?}",
    );
    assert!(
        !chunk_err
            .messages
            .iter()
            .any(|m| m.contains(SENDER_DROPPED_IDENTIFIER)),
        "dispatcher observed the synthesized 'Sender dropped before sending EOF' \
         Internal — chunk_guard wrap at fast_slow_store.rs:732 was removed. \
         Got: {chunk_err:?}",
    );

    assert!(
        invocations.load(Ordering::SeqCst) >= 1,
        "dispatcher must have been invoked at least once",
    );

    Ok(())
}
