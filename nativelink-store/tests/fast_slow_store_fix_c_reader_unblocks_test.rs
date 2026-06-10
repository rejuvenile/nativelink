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

//! #2 Fix-C production-composition FSS test.
//!
//! **Seams under test (three-seam chain):**
//!
//! 1. **Seam 1 — GrpcStore fast-abort:** `get_part_single_stream` returns
//!    `RetryResult::Err` after `consecutive_latched_fails >=
//!    LATCHED_POOL_ABORT_THRESHOLD`. Tested separately in
//!    `grpc_store_test.rs::fix_c_latched_pool_abort_returns_error_fast`.
//!
//! 2. **Seam 2 — FSS `run_producer` terminal write:** `FastSlowStore::run_producer`
//!    receives `slow_res = Err` from `slow_store.get_part`, propagates it to
//!    `merged`, and calls `writer_back.send_error(err)` at
//!    `fast_slow_store.rs:4051`.
//!
//! 3. **Seam 3 — StreamingBlobReader consumer unblocks:** the streaming
//!    reader polling the blob's `notify_rx` wake-path observes the terminal
//!    error state instead of burning up to `STREAMING_BLOB_NOTIFY_TIMEOUT`
//!    (30 s) per wait.
//!
//! This test crosses Seams 2 and 3 in production composition. Seam 1 is
//! simulated by the `InstantLatchedErrorSlowStore` fake: it returns the
//!  same error shape GrpcStore emits after Fix-C aborts (Code::Unknown +
//! "buffered service" substring — the classifier used by
//! `looks_like_latched_pool`). The FSS receives that error from the slow
//! store's `get_part`, and `run_producer` must call `send_error` so a
//! concurrent streaming reader unblocks within seconds.
//!
//! **Invariant:** `send_error` on `writer_back` must be called before
//! `run_producer` exits — this is the writer-termination contract for the
//! `FastSlowStore::run_producer` spawned task. Without it the spawned
//! task's Drop synthesizes a generic Internal error AFTER the
//! `STREAMING_BLOB_NOTIFY_TIMEOUT` (30 s) deadline, causing the starvation
//! symptom (#2).
//!
//! **Mutation step:** comment out the `writer_back.send_error(err)` call at
//! `fast_slow_store.rs:4051`. Both timeout assertions below MUST red-fail
//! with the bespoke "must not deadlock — Fix-C writer-termination contract
//! violated" message within 5 s (the timeout fires before the 30 s
//! STREAMING_BLOB_NOTIFY_TIMEOUT).
//!
//! **Over-action sibling:** a slow-but-progressing fake (100 ms delay then
//! Ok(EOF)) must NOT cause premature failure. The over-action test verifies
//! that a real slow store is served correctly when the fake succeeds.

use core::pin::Pin;
use core::sync::atomic::AtomicU64;
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const BLOB_SIZE: u64 = 1024;

/// Slow-tier fake that returns an instant latched-pool-signature error from
/// `get_part`, simulating the error GrpcStore emits after Fix-C aborts.
///
/// The error shape — `Code::Unknown + "buffered service failed"` — matches
/// the tower-Buffer ServiceError Display and the `looks_like_latched_pool`
/// classifier in `grpc_store.rs`. FSS `run_producer` does not inspect the
/// error code before writing the terminal; any `Err` propagates to
/// `writer_back.send_error`.
///
/// `has_with_results` returns `Some(BLOB_SIZE)` so FSS's `has()` check
/// (in the populate-or-wait fork) believes the blob exists and proceeds to
/// `get_part` rather than returning NotFound early. Without this, FSS
/// returns NotFound before `run_producer` is spawned — the populate path
/// is never exercised.
#[derive(MetricsComponent)]
struct InstantLatchedErrorSlowStore {
    // MetricsComponent proc-macro requires at least one field.
    #[metric(help = "placeholder metric")]
    _placeholder: AtomicU64,
}

#[async_trait]
impl StoreDriver for InstantLatchedErrorSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Claim the blob exists so FSS proceeds to `get_part` and spawns
        // `run_producer`. If we return None here FSS returns NotFound and
        // the streaming-reader path is never exercised.
        for slot in results.iter_mut() {
            *slot = Some(BLOB_SIZE);
        }
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Return an error matching the tower-Buffer ServiceError Display so
        // `looks_like_latched_pool` classifies it as a latch hit. FSS
        // `run_producer` propagates this error to `writer_back.send_error`
        // (the terminal write that unblocks streaming readers).
        Err(make_err!(
            Code::Unknown,
            "buffered service failed: channel closed — simulated Fix-C latched-pool abort"
        ))
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unimplemented, "update not supported"))
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

default_health_status_indicator!(InstantLatchedErrorSlowStore);

/// Slow-tier fake for the over-action sibling: yields once (simulating a
/// real in-flight RPC with non-zero latency) then writes a small payload
/// and EOF. The FSS `get_part` caller must receive `Ok(())` — Fix-C must
/// NOT trigger premature failure on slow-but-progressing slow stores.
#[derive(MetricsComponent)]
struct SlowSuccessSlowStore {
    // MetricsComponent proc-macro requires at least one field.
    #[metric(help = "placeholder metric")]
    _placeholder: AtomicU64,
}

#[async_trait]
impl StoreDriver for SlowSuccessSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for slot in results.iter_mut() {
            *slot = Some(BLOB_SIZE);
        }
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        // Yield once to simulate non-zero network latency. The streaming
        // reader must wait for data — but NOT time out prematurely.
        tokio::task::yield_now().await;
        // Write the full payload and EOF.
        writer
            .send(Bytes::from(vec![0xBBu8; BLOB_SIZE as usize]))
            .await
            .map_err(|e| make_err!(Code::Internal, "SlowSuccessSlowStore send failed: {e:?}"))?;
        writer
            .send_eof()
            .map_err(|e| make_err!(Code::Internal, "SlowSuccessSlowStore send_eof failed: {e:?}"))?;
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unimplemented, "update not supported"))
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

default_health_status_indicator!(SlowSuccessSlowStore);

/// Build a `FastSlowStore` with a real empty `MemoryStore` fast tier
/// (blob is absent) and the supplied slow-tier driver. Returns the
/// wrapping `Store` for caller-side invocation.
fn build_fss<D: StoreDriver>(slow_driver: Arc<D>) -> Store {
    let fast_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow_store = Store::new(slow_driver);
    let fss = FastSlowStore::new(
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
    );
    Store::new(fss)
}

/// **Fix-C FSS production-composition test — under-action coverage.**
///
/// Verifies that when the slow store returns an instant latched-pool-
/// signature error (simulating Fix-C post-abort from GrpcStore), the FSS
/// `run_producer` calls `writer_back.send_error` and BOTH the requesting
/// caller AND a concurrent streaming reader unblock within 5 s — not after
/// the 30 s `STREAMING_BLOB_NOTIFY_TIMEOUT`.
///
/// **Three seams crossed:**
/// 1. `InstantLatchedErrorSlowStore::get_part` → `slow_res = Err(...)`.
/// 2. `FastSlowStore::run_producer` receives `slow_res = Err`, computes
///    `streaming_terminal = Err`, calls `writer_back.send_error(err)` at
///    `fast_slow_store.rs:4051`.
/// 3. Concurrent `get_part` caller's `StreamingBlobReader::next_chunk`
///    observes the terminal error and returns it instead of blocking on
///    `notify_rx.changed()` for up to 30 s.
///
/// **Mutation step:** comment out `writer_back.send_error(err)` at
/// `fast_slow_store.rs:4051`. Both timeouts below fire within 5 s and
/// panic with "must not deadlock — Fix-C writer-termination contract
/// violated: streaming reader did not unblock within 5s of latched-pool
/// abort".
#[nativelink_test]
async fn fix_c_fss_latched_pool_error_unblocks_streaming_reader() -> Result<(), Error> {
    let store = build_fss(Arc::new(InstantLatchedErrorSlowStore {
        _placeholder: AtomicU64::new(0),
    }));
    let digest = DigestInfo::try_new(VALID_HASH, BLOB_SIZE).unwrap();

    // Populator task: calls get_part, triggering run_producer which
    // invokes slow_store.get_part — this returns an instant error.
    // run_producer must call writer_back.send_error before returning,
    // so the result is available quickly. Without Fix-C writer-termination
    // the spawned producer task holds the streaming_inner live and the
    // caller's get_part awaits the Drop-synthesized Internal error only
    // after the 30 s STREAMING_BLOB_NOTIFY_TIMEOUT fires.
    let (mut tx_populator, _rx_populator) = make_buf_channel_pair();
    let store_clone = store.clone();
    let key_populator: StoreKey<'static> = StoreKey::Digest(digest);
    let populator_handle = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(5),
            store_clone
                .as_store_driver_pin()
                .get_part(key_populator, &mut tx_populator, 0, None),
        )
        .await
        .expect(
            "must not deadlock — Fix-C writer-termination contract violated: \
             streaming reader did not unblock within 5s of latched-pool abort \
             (populator-caller path); check that writer_back.send_error is called \
             in FastSlowStore::run_producer before the task exits",
        )
    });

    // Concurrent waiter task: calls get_part on the SAME key while the
    // populator may still be in-flight. This joins the same
    // `populating_digests` entry and reads from a `StreamingBlobReader`.
    // When the producer calls send_error, the watch notify fires and the
    // reader's next_chunk returns the terminal error immediately.
    // Without send_error the reader waits up to STREAMING_BLOB_NOTIFY_TIMEOUT.
    let (mut tx_waiter, _rx_waiter) = make_buf_channel_pair();
    let store_clone2 = store.clone();
    let key_waiter: StoreKey<'static> = StoreKey::Digest(digest);
    let waiter_handle = tokio::spawn(async move {
        // Yield once to let the populator task start first. This increases
        // the probability that the populator has registered the
        // populating_digests entry before the waiter calls get_part.
        // The test is NOT sensitive to this ordering: if the waiter arrives
        // first it becomes the populator caller; if second it becomes the
        // streaming reader. Both paths must unblock within 5 s.
        tokio::task::yield_now().await;
        tokio::time::timeout(
            Duration::from_secs(5),
            store_clone2
                .as_store_driver_pin()
                .get_part(key_waiter, &mut tx_waiter, 0, None),
        )
        .await
        .expect(
            "must not deadlock — Fix-C writer-termination contract violated: \
             streaming reader did not unblock within 5s of latched-pool abort \
             (concurrent-waiter path); check that writer_back.send_error is called \
             in FastSlowStore::run_producer before the task exits",
        )
    });

    // Both tasks must complete within 5 s (enforced by the inner timeouts).
    // On completion, both should return Err (pool is permanently erroring).
    // We use explicit match instead of is_err() so tokio::time::Elapsed
    // (which passes is_err()) does not mask the real failure.
    let populator_result = populator_handle
        .await
        .expect("populator task did not panic");
    let waiter_result = waiter_handle.await.expect("waiter task did not panic");

    match populator_result {
        Err(_) => {} // expected: latched pool error propagated
        Ok(()) => panic!(
            "fix_c_fss_latched_pool_error_unblocks_streaming_reader: \
             populator get_part returned Ok but slow store always errors; \
             the instant error must surface as Err, not silently succeed"
        ),
    }
    match waiter_result {
        Err(_) => {} // expected: same latched pool error via StreamingBlobReader
        Ok(()) => panic!(
            "fix_c_fss_latched_pool_error_unblocks_streaming_reader: \
             concurrent waiter get_part returned Ok but slow store always errors; \
             the streaming reader must observe the terminal error, not spurious Ok"
        ),
    }

    Ok(())
}

/// **Fix-C FSS over-action test.**
///
/// A slow-but-progressing slow store (yields once, then writes full
/// payload) must NOT cause premature failure. Fix-C's fast-abort logic
/// lives in `GrpcStore::get_part_single_stream`; at the FSS level, a
/// non-error from the slow store must produce `Ok(())` for the caller.
///
/// This guards against a regression where `run_producer`'s terminal
/// selection erroneously writes an error even when `slow_res = Ok`.
///
/// **Mutation step:** force `streaming_terminal = Err(make_err!(...))` in
/// `FastSlowStore::run_producer` regardless of `merged`. The test MUST
/// red-fail with "over-action: slow-but-progressing slow store must not
/// cause premature failure via FSS writer-termination".
#[nativelink_test]
async fn fix_c_fss_over_action_slow_but_progressing_does_not_fail() -> Result<(), Error> {
    let store = build_fss(Arc::new(SlowSuccessSlowStore {
        _placeholder: AtomicU64::new(0),
    }));
    let digest = DigestInfo::try_new(VALID_HASH, BLOB_SIZE).unwrap();
    let key: StoreKey<'static> = StoreKey::Digest(digest);

    let (mut tx, mut rx) = make_buf_channel_pair();

    let get_result = tokio::time::timeout(
        Duration::from_secs(5),
        store.as_store_driver_pin().get_part(key, &mut tx, 0, None),
    )
    .await
    .expect(
        "must not deadlock — slow-but-progressing slow store hung for 5s; \
         FSS run_producer must call writer_back.send_eof (not send_error) \
         when slow_store.get_part returns Ok",
    );

    match get_result {
        Ok(()) => {} // expected
        Err(e) => panic!(
            "over-action: slow-but-progressing slow store must not cause \
             premature failure via FSS writer-termination; got Err({e:?})"
        ),
    }

    // Drain the reader to verify the full payload arrived (not just Ok+empty).
    // `rx.recv()` returns Ok(ZERO_DATA) on EOF — break on empty to avoid
    // looping forever (the channel stays closed but doesn't return Err on EOF).
    let mut received_bytes = 0usize;
    loop {
        let chunk = rx
            .recv()
            .await
            .expect("reader must not error during over-action drain");
        if chunk.is_empty() {
            break; // EOF
        }
        received_bytes += chunk.len();
    }
    assert_eq!(
        received_bytes, BLOB_SIZE as usize,
        "over-action: expected {BLOB_SIZE} bytes from slow store, got {received_bytes}; \
         FSS run_producer must not truncate the stream when slow_store.get_part returns Ok"
    );

    Ok(())
}
