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

//! #56/#59/#62 sibling fix-up — `FastSlowStore::update` (Ok, Err) arms.
//!
//! **Invariant under test:** when the slow-store consumer returns
//! `Ok(())` from `update()`, the overall operation MUST be treated as a
//! success regardless of any subsequent producer-side stream error (the
//! producer's `tx.send(chunk)` returns "receiver disconnected" because
//! the consumer dropped `rx` after returning Ok). Consumer Ok is the
//! authoritative commit signal; producer Err after consumer Ok is a
//! benign symptom of the receiver being dropped.
//!
//! This invariant was violated at TWO sites inside `FastSlowStore::update`
//! (the standard path, NOT `update_with_whole_file`):
//!
//! ## Site A — background slow-write path (fast_slow_store.rs:~5621)
//!
//! Inside `tokio::spawn` (the common non-shutdown production path for
//! worker CAS uploads). When the slow-store returns AlreadyExists→Ok,
//! `slow_rx` is dropped. The producer's next `slow_tx.send(chunk)` fails
//! with "receiver disconnected". The pre-fix match arm:
//!
//! ```
//! (Ok(()), Err(send_err)) => Err(send_err),
//! ```
//!
//! …returns `Err`, which drives the Err-arm side-effects:
//!
//! 1. `failed_slow_writes.lock().insert(digest)` — spurious insertion;
//!    triggers a spurious re-upload on the next worker reconnect via
//!    `drain_failed_digests` → `UploadMissingBlobs`.
//! 2. `fast_store.pin_digests(&[digest])` — spurious re-pin, consuming
//!    pin budget.
//! 3. `error!("background slow write FAILED …")` — false error log.
//! 4. `slow_tier_async_fail` metric increment — false metric.
//! 5. Re-upload churn loop: the re-upload hits AlreadyExists again →
//!    (Ok, Err) → `failed_slow_writes` insert again → loop.
//!
//! **Post-fix**: `(Ok(()), Err(send_err))` routes to the SUCCESS arm:
//! no `failed_slow_writes` insert, no re-pin, no fail-counter bump.
//! The swallowed producer error is logged at `info!` with digest + error.
//!
//! ## Site B — shutdown-flush path (fast_slow_store.rs:~5441)
//!
//! Inside the `if self.shutting_down` branch. Same defect → false action
//! failure during shutdown drain.
//!
//! **Post-fix**: same treatment; call returns `Ok(())`.
//!
//! ## Seams crossed (Site A)
//!
//!   1. Test feeder: sends chunks into outer `update(key, rx, ...)`.
//!   2. `FastSlowStore::update` `data_stream_fut`: reads from outer rx,
//!      forwards to fast tier, collects chunks into `Vec<Bytes>`.
//!   3. `tokio::spawn` background task: opens inner `buf_channel(128)`,
//!      re-sends collected chunks via `send_fut`, calls
//!      `slow_store.update(slow_rx, ...)`.
//!   4. `DropImmediatelyOkSlowStore::update`: drops `slow_rx` and
//!      returns `Ok(())` (simulates GrpcStore AlreadyExists → Ok path).
//!   5. `send_fut`'s next `slow_tx.send(chunk)` wakes with "receiver
//!      disconnected" (slow_rx was dropped in step 4).
//!   6. `tokio::join!(write_fut, send_fut)` aggregator in the spawned
//!      task → `(Ok(()), Err(receiver disconnected))`.
//!   7. The `match (write_result, send_result)` at line ~5621 — the fix
//!      site.
//!   8. `match &result` recovery block: must take OK arm (no
//!      `failed_slow_writes` insert, no re-pin, no fail-counter bump).
//!
//! ## Seams crossed (Site B)
//!
//!   1-3. Same as A through fast-tier commit.
//!   4. `shutting_down=true` → synchronous slow-write path (no spawn).
//!   5. Inner `send_fut` feeds chunks; `DropImmediatelyOkSlowStore`
//!      drops `rx` and returns `Ok(())` immediately.
//!   6. `tokio::join!(write_fut, send_fut)` → `(Ok(()), Err(_))`.
//!   7. `match (write_result, send_result)` at line ~5441 — fix site.
//!   8. `return match(...)` → `Ok(())` propagated back to caller.
//!
//! ## Mutation steps (CLAUDE.md mandatory TDD step 5)
//!
//! ### Site A mutation
//!
//! Revert `(Ok(()), Err(send_err)) => { info!(...); Ok(()) }` back to
//! `(Ok(()), Err(send_err)) => Err(send_err)` at the background
//! slow-write match (fast_slow_store.rs:~5621). Re-run
//! `bg_write_consumer_ok_producer_err_is_success`. Must red-fail with:
//!
//! ```
//! "#56/#62 bg-write consumer-Ok-authoritative invariant not enforced —
//!  (Ok, Err) arm drove failure side-effects (failed_slow_writes insert)
//!  on an already-committed blob"
//! ```
//!
//! Also assert the fail-counter and `failed_slow_writes` state.
//!
//! ### Site B mutation
//!
//! Revert `(Ok(()), Err(send_err)) => { info!(...); Ok(()) }` back to
//! `(Ok(()), Err(send_err)) => Err(send_err)` at the shutdown-flush
//! match (fast_slow_store.rs:~5441). Re-run
//! `shutdown_flush_consumer_ok_producer_err_is_success`. Must red-fail
//! with:
//!
//! ```
//! "#56/#62 shutdown-flush consumer-Ok-authoritative invariant not
//!  enforced — (Ok, Err) arm returned Err when slow-store consumer
//!  already committed the blob"
//! ```

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::fast_slow_store::{FastSlowStore, SlowTierMetricSink};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair_with_size,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, DurableDelegation, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Number of 4 KiB chunks fed into the outer `update()` reader.
/// The inner buf_channel created by `FastSlowStore::update`'s
/// background and shutdown branches both have capacity 128 (via
/// `make_buf_channel_pair_with_size(128)`). Sending 256 chunks
/// guarantees the producer is mid-send when the consumer drops `rx`,
/// deterministically producing the `(Ok consumer, Err producer)` arm.
const CHUNK_COUNT: usize = 256;
const CHUNK_BYTES: usize = 4096; // 256 * 4096 = 1 MiB
const TOTAL_BYTES: u64 = (CHUNK_COUNT * CHUNK_BYTES) as u64;

/// Slow-tier fake that drops `rx` immediately and returns `Ok(())`,
/// simulating GrpcStore's AlreadyExists → Ok path: the server already
/// has the blob and closes the inbound stream without draining it.
/// With a 256-chunk feeder and the inner buf_channel at capacity 128,
/// the producer is guaranteed to be mid-send when `rx` is dropped,
/// deterministically producing `(Ok consumer, Err producer)`.
#[derive(MetricsComponent)]
struct DropImmediatelyOkSlowStore {
    update_invocations: AtomicUsize,
}

impl DropImmediatelyOkSlowStore {
    fn new() -> Self {
        Self {
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for DropImmediatelyOkSlowStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Report missing so FastSlowStore does not short-circuit the
        // slow-write path.
        for slot in results.iter_mut() {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Drop `_reader` immediately (rx dropped) and return Ok.
        // Simulates GrpcStore receiving AlreadyExists from the server
        // and returning Ok without draining the stream body.
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
            "DropImmediatelyOkSlowStore: get_part not supported"
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
    fn durable_delegation(&self) -> DurableDelegation<'_> {
        DurableDelegation::Leaf
    }
}

default_health_status_indicator!(DropImmediatelyOkSlowStore);

/// `SlowTierMetricSink` that counts `record_async_fail` calls.
/// Installed on the FSS under test so Site A tests can assert the
/// fail-counter was NOT incremented on the (Ok, Err) arm.
#[derive(Debug)]
struct CountingMetricSink {
    async_fail_calls: Arc<AtomicU64>,
}

impl SlowTierMetricSink for CountingMetricSink {
    fn record_async_fail(&self, _store_class: &str) {
        self.async_fail_calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// Build a production-composition `FastSlowStore` with a real
/// `MemoryStore` fast tier and `DropImmediatelyOkSlowStore` as the
/// slow tier. Returns the wrapping `Store`, the underlying
/// `Arc<FastSlowStore>` (for state inspection), the slow-store
/// `Arc` (for invocation count), and the metric-sink counter
/// (for fail-count assertion).
fn build_fast_slow(
    ok_slow: Arc<DropImmediatelyOkSlowStore>,
) -> (Store, Arc<FastSlowStore>, Arc<AtomicU64>) {
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(fast_inner);
    let slow_store = Store::new(ok_slow);
    let fss: Arc<FastSlowStore> = FastSlowStore::new(
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
    let fail_counter = Arc::new(AtomicU64::new(0));
    fss.set_slow_tier_metric_sink(Arc::new(CountingMetricSink {
        async_fail_calls: fail_counter.clone(),
    }));
    let wrapped = Store::new(fss.clone());
    (wrapped, fss, fail_counter)
}

/// Feed `CHUNK_COUNT` 4 KiB chunks + EOF into `writer` from a detached
/// task. The outer buf_channel uses capacity 8 so the writer yields
/// quickly; the fast-tier collection drains it into `Vec<Bytes>` for
/// the background or shutdown branch.
fn spawn_feeder(mut writer: DropCloserWriteHalf) -> tokio::task::JoinHandle<Result<(), Error>> {
    tokio::spawn(async move {
        let chunk = Bytes::from(vec![0xA5u8; CHUNK_BYTES]);
        for _ in 0..CHUNK_COUNT {
            writer
                .send(chunk.clone())
                .await
                .map_err(|e| make_err!(Code::Internal, "test feeder send: {e:?}"))?;
        }
        writer
            .send_eof()
            .map_err(|e| make_err!(Code::Internal, "test feeder send_eof: {e:?}"))?;
        Ok(())
    })
}

/// #56/#62 Site A fix: background slow-write path — consumer Ok is
/// authoritative; the (Ok, Err) arm must NOT drive failure side-effects.
///
/// Topology:
///   - `FastSlowStore::update` (standard path, `shutting_down=false`)
///   - Slow tier: `DropImmediatelyOkSlowStore` (drops `rx`, returns Ok)
///   - 256-chunk feeder ensures the inner producer is mid-send when
///     `rx` is dropped, producing `(Ok consumer, Err producer)`.
///
/// Assertions (all three must hold):
///   1. `update()` returns `Ok(())` immediately (spawn path is
///      background-detached — the caller always gets Ok if fast tier
///      commits).
///   2. `failed_slow_writes` does NOT contain the digest after the
///      background task completes.
///   3. `slow_tier_async_fail` counter was NOT incremented.
///
/// Mutation step: revert `(Ok(()), Err(send_err)) => { info!(...); Ok(()) }`
/// back to `(Ok(()), Err(send_err)) => Err(send_err)` at the background
/// slow-write match (~5621). Re-run. Must red-fail with:
/// "#56/#62 bg-write consumer-Ok-authoritative invariant not enforced —
///  (Ok, Err) arm drove failure side-effects (failed_slow_writes insert)
///  on an already-committed blob"
#[nativelink_test]
async fn bg_write_consumer_ok_producer_err_is_success() -> Result<(), Error> {
    let ok_slow = Arc::new(DropImmediatelyOkSlowStore::new());
    let (wrapped, fss, fail_counter) = build_fast_slow(ok_slow.clone());

    let digest = DigestInfo::try_new(VALID_HASH, TOTAL_BYTES).unwrap();
    let store_key: StoreKey<'_> = digest.into();

    let (writer, reader) = make_buf_channel_pair_with_size(8);
    let feeder = spawn_feeder(writer);

    // `update()` returns as soon as the fast-tier commits + spawn fires.
    // The background slow-write task runs detached; the caller always
    // sees Ok from the spawn path.
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        wrapped
            .as_store_driver_pin()
            .update(store_key, reader, UploadSizeInfo::ExactSize(TOTAL_BYTES)),
    )
    .await
    .expect(
        "FastSlowStore::update must NOT deadlock in background-spawn path \
         (30-second outer timeout); the fast-tier commit + tokio::spawn \
         should complete quickly",
    );
    drop(feeder.await);

    assert!(
        res.is_ok(),
        "FastSlowStore::update must return Ok in the background-spawn path \
         (slow write is detached); got {res:?}"
    );

    // Wait for the background spawned task to reach a terminal state.
    // In the pre-fix world the task fires `failed_slow_writes.insert`
    // (Err arm). In the post-fix world the task fires `push_stable_digests`
    // (Ok arm) and does NOT insert. We poll until:
    //   (a) failed_slow_writes is NOT populated (post-fix) — confirmed at end
    //   (b) in_flight_slow_writes is drained — reliable terminal signal
    // The `in_flight_empty_notify` is not exposed publicly, but
    // `flush_slow_writes` (with a generous timeout) is. We flush to
    // let the spawned task run to completion.
    let remaining = tokio::time::timeout(
        Duration::from_secs(10),
        fss.flush_slow_writes(Duration::from_secs(8)),
    )
    .await
    .expect(
        "flush_slow_writes must NOT deadlock while waiting for the one \
         background slow-write spawned by this test to complete",
    );
    assert_eq!(
        remaining, 0,
        "expected all in-flight slow writes to drain within 8s; \
         {remaining} still in-flight — background task did not complete"
    );

    // Sanity: confirm the slow tier was actually invoked.
    assert!(
        ok_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "DropImmediatelyOkSlowStore::update was never called; the \
         background slow-write path was not exercised. \
         update_invocations={}",
        ok_slow.update_invocations.load(Ordering::SeqCst)
    );

    // Primary assertion (Site A fix): failed_slow_writes must NOT be
    // populated for a consumer-Ok result. Pre-fix the (Ok, Err) arm
    // returned Err, driving the Err-recovery path which inserts the
    // digest into failed_slow_writes. Post-fix the success arm fires
    // instead — no insert.
    assert!(
        !fss.failed_slow_writes_contains(&digest),
        "#56/#62 bg-write consumer-Ok-authoritative invariant not enforced — \
         (Ok, Err) arm drove failure side-effects (failed_slow_writes insert) \
         on an already-committed blob. The slow store dropped rx and returned \
         Ok(()) (simulating GrpcStore AlreadyExists); the producer hit \
         receiver-disconnected. The digest MUST NOT appear in \
         failed_slow_writes (no spurious re-upload churn), but it does."
    );

    // Secondary assertion: slow_tier_async_fail counter must NOT have been
    // incremented. Pre-fix the Err arm bumps the counter; post-fix the
    // success arm does not.
    let fail_count = fail_counter.load(Ordering::SeqCst);
    assert_eq!(
        fail_count, 0,
        "#56/#62 bg-write consumer-Ok-authoritative invariant not enforced — \
         slow_tier_async_fail counter was incremented ({fail_count}) on a \
         consumer-Ok result. The (Ok, Err) arm must NOT bump the fail counter \
         when the consumer already committed the blob."
    );

    Ok(())
}

/// #56/#62 Site B fix: shutdown-flush path — consumer Ok is authoritative;
/// the (Ok, Err) arm must return Ok, not Err.
///
/// Topology:
///   - `flush_slow_writes` sets `shutting_down=true` (the fence is
///     inherited from the `flush_slow_writes` call in the setup).
///   - `FastSlowStore::update` (standard path) takes the shutdown branch
///     and calls `slow_store.update` synchronously.
///   - Slow tier: `DropImmediatelyOkSlowStore` (drops `rx`, returns Ok).
///   - 256-chunk feeder ensures the inner `send_fut` is mid-send when
///     `rx` is dropped, producing `(Ok consumer, Err producer)`.
///   - The match at ~5441 either returns Ok (post-fix) or Err (pre-fix).
///
/// Mutation step: revert `(Ok(()), Err(send_err)) => { info!(...); Ok(()) }`
/// back to `(Ok(()), Err(send_err)) => Err(send_err)` at the shutdown
/// match (~5441). Re-run. Must red-fail with:
/// "#56/#62 shutdown-flush consumer-Ok-authoritative invariant not
///  enforced — (Ok, Err) arm returned Err when slow-store consumer
///  already committed the blob"
#[nativelink_test]
async fn shutdown_flush_consumer_ok_producer_err_is_success() -> Result<(), Error> {
    let ok_slow = Arc::new(DropImmediatelyOkSlowStore::new());
    let (wrapped, fss, _fail_counter) = build_fast_slow(ok_slow.clone());

    // Trip the shutting_down fence. With no in-flight writes,
    // flush_slow_writes returns immediately after setting the flag.
    let remaining = tokio::time::timeout(
        Duration::from_secs(5),
        fss.flush_slow_writes(Duration::from_millis(100)),
    )
    .await
    .expect(
        "flush_slow_writes setup call must not deadlock — \
         in-flight map should be empty at test start",
    );
    assert_eq!(
        remaining, 0,
        "test precondition: no in-flight slow writes before the test \
         call; got {remaining}"
    );

    let digest = DigestInfo::try_new(VALID_HASH, TOTAL_BYTES).unwrap();
    let store_key: StoreKey<'_> = digest.into();

    let (writer, reader) = make_buf_channel_pair_with_size(8);
    let feeder = spawn_feeder(writer);

    // `update()` takes the shutdown branch: calls `slow_store.update`
    // synchronously and returns its result. With the (Ok, Err) fix
    // applied the match returns Ok.
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        wrapped
            .as_store_driver_pin()
            .update(store_key, reader, UploadSizeInfo::ExactSize(TOTAL_BYTES)),
    )
    .await
    .expect(
        "FastSlowStore::update shutdown-flush path must NOT deadlock — \
         consumer-Ok must complete within 10s",
    );
    drop(feeder.await);

    // Sanity: confirm the slow tier was actually invoked via the
    // shutdown path, guarding against a future refactor that skips the
    // slow store during shutdown.
    assert!(
        ok_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "DropImmediatelyOkSlowStore::update was never called; the \
         shutdown-flush branch was not exercised. \
         update_invocations={}",
        ok_slow.update_invocations.load(Ordering::SeqCst)
    );

    assert!(
        res.is_ok(),
        "#56/#62 shutdown-flush consumer-Ok-authoritative invariant not \
         enforced — (Ok, Err) arm returned Err when slow-store consumer \
         already committed the blob. The slow tier dropped rx and returned \
         Ok(()) (simulating GrpcStore AlreadyExists); the producer hit \
         receiver-disconnected. Expected Ok(()), got: {res:?}"
    );

    Ok(())
}
