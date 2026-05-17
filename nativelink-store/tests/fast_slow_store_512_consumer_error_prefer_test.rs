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

//! #512 — sibling sites of #476 observability fix in
//! `FastSlowStore::update`. Two more `tokio::join!(write_fut, send_fut)`
//! aggregators used `send_result.and(write_result)`, which has the
//! quirk:
//!
//!   send_result.and(write_result):
//!     (Ok,  Ok)     => Ok
//!     (Ok,  Err(w)) => Err(w)   — same as post-fix
//!     (Err(s), Ok)  => Err(s)   — same as post-fix
//!     (Err(s), Err(w)) => Err(s)   — **DIFFERS** from post-fix Err(w)
//!
//! The only behavioral divergence between pre-fix and post-fix is in
//! the dual-err arm — when BOTH the consumer (`write_result` from
//! `slow_store.update`) and the producer (`send_result` from the
//! channel-feed closure) error. In production that's the common case:
//! consumer errors mid-stream (h2 reset, server reject, admission),
//! drops `rx`; producer's NEXT `tx.send` blocks on a full channel and
//! wakes with "channel closed".
//!
//! **Therefore the tests in this file MUST force the dual-err arm.**
//! A test that only exercises `(Err, Ok)` or `(Ok, Err)` arms would
//! pass both pre-fix and post-fix and would NOT be a valid regression
//! test (the mutation would not red-fail). The producer is forced into
//! the dual-err arm by feeding more chunks than the inner buf_channel
//! capacity (128) so that producer.send blocks on a full channel when
//! the consumer drops `rx`. The fake consumer recvs ONE chunk then
//! errors, dropping `rx` and freeing the producer's pending send into
//! a closed-channel error — both halves error simultaneously.
//!
//! **Invariant under test:** the surfaced/logged error on dual-err MUST
//! carry the consumer's authoritative root cause (`write_result`), NOT
//! the producer's downstream symptom (`send_result`).
//!
//! **Sites covered:**
//!   - `fast_slow_store.rs` shutdown-flush path inside
//!     `FastSlowStore::update` (gated by `shutting_down.load(...)`).
//!     The returned error is propagated to the caller of
//!     `update_oneshot`/`update` — the test directly asserts on the
//!     surfaced Err.
//!   - `fast_slow_store.rs` background slow-write path
//!     (`tokio::spawn` inside `FastSlowStore::update`). The result is
//!     consumed in-process to drive recovery (insert into
//!     `failed_slow_writes` + re-pin) and to log `error = ?e` — the
//!     ONLY operator-visible signal of WHY the slow write failed. We
//!     assert via `logs_contain` from `tracing-test` (auto-applied by
//!     `#[nativelink_test]`).
//!
//! **Mutation step (CLAUDE.md mandatory):**
//!   1. Revert the dual-err arm at the shutdown-flush site back to
//!      `send_result.and(write_result)`. Re-run
//!      `shutdown_flush_surfaces_consumer_error_over_symptom_dual_err`
//!      — it MUST red-fail with the bespoke "consumer-error-prefer
//!      policy not enforced" message naming
//!      `TEST_SLOW_STORE_ABORTED_FOR_512_SHUTDOWN`.
//!   2. Revert the dual-err arm at the background slow-write site.
//!      Re-run `background_slow_write_logs_consumer_error_over_symptom_dual_err`
//!      — it MUST red-fail with the bespoke "background slow-write
//!      log did not carry consumer error" message.
//!
//! **Seams crossed:**
//!   1. Test feeder: spawns a task feeding many chunks into the outer
//!      `update()` reader half.
//!   2. `FastSlowStore::update` data_stream_fut: collects chunks into
//!      a `Vec<Bytes>` while writing to the fast tier.
//!   3. fast-tier write completes successfully.
//!   4. Either shutdown branch (sync `slow_store.update` + `send_fut`)
//!      or background-spawn branch (detached `slow_store.update` +
//!      `send_fut`).
//!   5. The inner producer (`send_fut`): re-sends collected chunks
//!      into a fresh 128-slot buf_channel; blocks on full channel.
//!   6. Consumer: `slow_store.update(rx)` (the fake) — drops `rx`
//!      mid-stream after one recv.
//!   7. The `tokio::join!(write_fut, send_fut)` aggregator.
//!   8. The match-on-(write_result, send_result) that selects the
//!      surfaced error (the change site).
//!   9. For the shutdown path: return from `FastSlowStore::update` →
//!      `Store::update` caller observes the Err.
//!  10. For the background path: the `match &result` recovery block
//!      that fires `error!(error = ?e)` and inserts into
//!      `failed_slow_writes`. Test asserts via `logs_contain` for the
//!      error rendering AND polls `failed_slow_writes_contains` as a
//!      barrier so the log race is closed.

use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
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
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair_with_size,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};

/// Unique tag asserted-on in test output for the shutdown-flush path.
/// If the surfaced Err does NOT contain this string, the consumer-
/// error-prefer policy is broken and the producer's "channel closed"
/// symptom was promoted in its place.
const SHUTDOWN_ABORT_TAG: &str = "TEST_SLOW_STORE_ABORTED_FOR_512_SHUTDOWN";

/// Unique tag asserted-on via `logs_contain` for the background
/// slow-write path. The `error!(error = ?e)` log line in the
/// `Err(e)` recovery arm of `FastSlowStore::update`'s background
/// spawn renders this tag iff the consumer-prefer policy holds.
const BACKGROUND_ABORT_TAG: &str = "TEST_SLOW_STORE_ABORTED_FOR_512_BACKGROUND";

const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

/// Number of chunks fed into the outer `update()` reader. The inner
/// buf_channel created by `FastSlowStore::update`'s shutdown and
/// background branches both have capacity 128 (see
/// `make_buf_channel_pair_with_size(128)` at
/// `fast_slow_store.rs:4885` and `:4979`). The producer's
/// `for chunk in data { tx.send(chunk).await? }` blocks once the
/// channel is full. Sending 256 chunks therefore guarantees the
/// producer is mid-send when the consumer drops `rx`, forcing the
/// dual-err arm that distinguishes pre-fix from post-fix.
const CHUNK_COUNT: usize = 256;
const CHUNK_BYTES: usize = 4096; // 256 * 4096 = 1 MiB
const TOTAL_BYTES: u64 = (CHUNK_COUNT * CHUNK_BYTES) as u64;

/// Slow-tier fake that, on `update()`, drains ONE chunk from the
/// reader and then returns a uniquely-tagged Err — dropping `rx`.
/// With the inner buf_channel at capacity 128 and 256 chunks queued,
/// the producer is guaranteed to be blocked mid-send when this drop
/// happens, so the producer's next send wakes with channel-closed.
/// Both halves of the `join!` error; the post-fix code MUST surface
/// this `update()` error rather than the producer's symptom.
///
/// `tag` distinguishes the two test invocations so the same fake type
/// covers both sites without cross-contamination.
#[derive(MetricsComponent)]
struct AbortAfterOneChunkStore {
    tag: &'static str,
    update_invocations: AtomicUsize,
}

impl AbortAfterOneChunkStore {
    fn new(tag: &'static str) -> Self {
        Self {
            tag,
            update_invocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StoreDriver for AbortAfterOneChunkStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // Report "missing" for every query so the background slow-write
        // path is exercised (some code paths skip the slow store if
        // `has` returns Some).
        for slot in results.iter_mut() {
            *slot = None;
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        self.update_invocations.fetch_add(1, Ordering::SeqCst);
        // Consume one chunk so the producer's chunk loop has fully
        // started (and the producer has filled the channel + is blocked
        // on send). Then returning Err drops `reader` (and thus `rx`
        // from the buf_channel pair) before EOF. The producer side then
        // errors with channel-closed on its next send, producing the
        // dual-err that distinguishes pre-fix from post-fix.
        let _drain = reader.recv().await;
        Err(make_err!(Code::Aborted, "{}", self.tag))
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

/// Build a production-composition `FastSlowStore` with a real
/// `MemoryStore` fast tier and the supplied `AbortAfterOneChunkStore`
/// slow tier. Returns both the wrapping `Store` (for caller-side
/// invocation) and the underlying `Arc<FastSlowStore>` (for
/// shutdown-fence manipulation and recovery-state inspection).
fn build_fast_slow(abort_slow: Arc<AbortAfterOneChunkStore>) -> (Store, Arc<FastSlowStore>) {
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(fast_inner);
    let slow_store = Store::new(abort_slow);
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
    let wrapped = Store::new(fss.clone());
    (wrapped, fss)
}

/// Spawn a feeder task that sends `CHUNK_COUNT` chunks of `CHUNK_BYTES`
/// bytes each into `writer`, then `send_eof()`. Returns immediately
/// (caller awaits the join handle after the under-test call). The
/// feeder fills the OUTER buf_channel (the one we pass to
/// `update(key, rx, ...)`); the fast tier reads through it and
/// collects chunks into `data: Vec<Bytes>`. The shutdown / background
/// branch then re-sends those collected chunks into its OWN inner
/// 128-slot buf_channel — which is where the dual-err is produced.
fn spawn_feeder(
    mut writer: DropCloserWriteHalf,
) -> tokio::task::JoinHandle<Result<(), Error>> {
    tokio::spawn(async move {
        let chunk = Bytes::from(vec![0xA5u8; CHUNK_BYTES]);
        for _ in 0..CHUNK_COUNT {
            writer
                .send(chunk.clone())
                .await
                .map_err(|e| make_err!(Code::Internal, "test feeder send failed: {e:?}"))?;
        }
        writer
            .send_eof()
            .map_err(|e| make_err!(Code::Internal, "test feeder send_eof failed: {e:?}"))?;
        Ok(())
    })
}

/// #512 — shutdown-flush path: when `shutting_down` is true,
/// `FastSlowStore::update` calls `slow_store.update` synchronously
/// (no spawn) and returns its result to the caller. Pre-fix that
/// result was `send_result.and(write_result)`; post-fix it is a
/// 4-arm match that prefers `write_result` on dual-err. The test
/// flips the shutting_down fence via `flush_slow_writes`, issues
/// the underlying `update()` with a 256-chunk feeder so the inner
/// producer blocks on the 128-slot buf_channel, the consumer drops
/// after one recv, and BOTH halves error simultaneously — exercising
/// exactly the dual-err arm that distinguishes pre-fix from post-fix.
///
/// `tokio::time::timeout` wraps the call as a deadlock detector —
/// a hung future would mask the bug (`tokio::time::Elapsed`
/// silently passes `is_err()`).
#[nativelink_test]
async fn shutdown_flush_surfaces_consumer_error_over_symptom_dual_err() -> Result<(), Error> {
    let abort_slow = Arc::new(AbortAfterOneChunkStore::new(SHUTDOWN_ABORT_TAG));
    let (wrapped, fss) = build_fast_slow(abort_slow.clone());

    // Trip the shutting_down fence. `flush_slow_writes` sets it and
    // then waits for in-flight to drain; with zero in-flight it
    // returns immediately. This is the production path that triggers
    // the shutdown-flush branch inside `FastSlowStore::update`.
    let remaining = tokio::time::timeout(
        Duration::from_secs(5),
        fss.flush_slow_writes(Duration::from_millis(100)),
    )
    .await
    .expect(
        "flush_slow_writes must not deadlock in test setup — \
         in-flight map should be empty at test start",
    );
    assert_eq!(
        remaining, 0,
        "test precondition: no in-flight slow writes; got {remaining}"
    );

    let digest = DigestInfo::try_new(VALID_HASH, TOTAL_BYTES).unwrap();
    let store_key: StoreKey<'_> = digest.into();

    // Build the outer reader/writer and a feeder task; call the
    // streaming `update()` API directly (NOT `update_oneshot` — that
    // would deliver a single Bytes that fits in the inner 128-slot
    // channel, missing the dual-err arm).
    let (writer, reader) = make_buf_channel_pair_with_size(8);
    let feeder = spawn_feeder(writer);

    let call = wrapped
        .as_store_driver_pin()
        .update(store_key, reader, UploadSizeInfo::ExactSize(TOTAL_BYTES));
    let res = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect(
            "FastSlowStore::update shutdown-flush path must NOT deadlock — \
             consumer-error-prefer policy must surface the abort within 10s, \
             not hang in producer/consumer race",
        );
    // Drain the feeder so we don't leak the task; its error
    // (channel-closed on send when fast tier already errored) is
    // expected and not load-bearing for this test.
    let _ = feeder.await;

    assert!(
        res.is_err(),
        "FastSlowStore::update must propagate the slow-tier abort as Err \
         in shutdown-flush path; got Ok(...). The slow tier returned \
         {SHUTDOWN_ABORT_TAG} so the call cannot succeed."
    );
    let err = res.unwrap_err();
    let rendered = format!("{err:?}");

    assert!(
        rendered.contains(SHUTDOWN_ABORT_TAG),
        "#512 consumer-error-prefer policy not enforced at shutdown-flush \
         site — producer-side channel-closed swallowed consumer's root \
         cause. Expected surfaced Err to contain `{SHUTDOWN_ABORT_TAG}` \
         (the slow tier's authoritative Err), got rendered={rendered}"
    );

    // Sanity: confirm the slow tier was actually invoked, defending
    // against a future refactor that skips the slow store in the
    // shutdown path (which would make this test pass vacuously since
    // both sides would then be Ok).
    assert!(
        abort_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "AbortAfterOneChunkStore::update was never called in shutdown \
         path; FastSlowStore::update did not take the shutdown branch. \
         update_invocations={}",
        abort_slow.update_invocations.load(Ordering::SeqCst)
    );

    Ok(())
}

/// #512 — background slow-write path: when `shutting_down` is FALSE,
/// `FastSlowStore::update` spawns the slow write on a `tokio::spawn`
/// and returns Ok immediately. The spawned task drives
/// `slow_store.update` + `send_fut`, joins them, and consumes the
/// result internally (recovery: insert into `failed_slow_writes`,
/// re-pin in fast tier; observability: `error!(error = ?e)` log
/// line). The log line is the ONLY operator-visible signal of WHY
/// the slow write failed — the recovery side-effects are identical
/// regardless of which error variant won.
///
/// Same dual-err forcing as the shutdown sibling: 256 chunks fed into
/// the OUTER reader, fast tier collects, background spawn re-feeds
/// into its OWN 128-slot inner channel, producer blocks, consumer
/// drops `rx`, producer's pending send errors with channel-closed.
/// Pre-fix surface error = producer symptom; post-fix surface error
/// = consumer's tagged `Aborted` Err. Asserted via `logs_contain`
/// (`tracing-test` auto-applied by `#[nativelink_test]`).
///
/// `failed_slow_writes_contains` is the barrier: the `Err(e)`
/// recovery arm at `fast_slow_store.rs:~5079` BOTH inserts the
/// digest into `failed_slow_writes` and emits the `error!` log line
/// — sequentially within the same arm. Polling the insert confirms
/// the log has also been emitted, closing the race that would
/// otherwise make `logs_contain` flaky.
#[nativelink_test]
async fn background_slow_write_logs_consumer_error_over_symptom_dual_err() -> Result<(), Error>
{
    let abort_slow = Arc::new(AbortAfterOneChunkStore::new(BACKGROUND_ABORT_TAG));
    let (wrapped, fss) = build_fast_slow(abort_slow.clone());

    // shutting_down stays false → spawn path.
    let digest = DigestInfo::try_new(VALID_HASH, TOTAL_BYTES).unwrap();
    let store_key: StoreKey<'_> = digest.into();

    let (writer, reader) = make_buf_channel_pair_with_size(8);
    let feeder = spawn_feeder(writer);

    // Issue update(). Returns Ok as soon as fast-tier commit + spawn
    // complete — the slow write runs in a detached task. The actual
    // bug-shaped error happens in the spawned task.
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        wrapped
            .as_store_driver_pin()
            .update(store_key, reader, UploadSizeInfo::ExactSize(TOTAL_BYTES)),
    )
    .await
    .expect(
        "FastSlowStore::update must not deadlock in background-spawn path \
         — the spawned task is detached so the caller should return as \
         soon as fast-tier commit + spawn complete",
    );
    let _ = feeder.await;
    assert!(
        res.is_ok(),
        "FastSlowStore::update must return Ok in background-spawn path \
         (slow write detached); got {res:?}"
    );

    // Wait for the spawned task to terminate. `failed_slow_writes`
    // insert + `error!` log are sequential in the `Err(e)` arm of the
    // `match &result` block; polling the insert confirms BOTH have
    // happened. Cooperative yield (NOT sleep-as-sync) — wall-clock
    // bound via explicit deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut observed_failed = false;
    while tokio::time::Instant::now() < deadline {
        if fss.failed_slow_writes_contains(&digest) {
            observed_failed = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        observed_failed,
        "background slow-write recovery did not fire within 10s — \
         either the spawned task did not run (test setup wrong) or \
         the `Err(e)` arm at `fast_slow_store.rs:~5079` was bypassed. \
         Without the recovery firing, the `error!` log we're asserting \
         on was never emitted."
    );

    // Sanity: confirm the slow tier was actually invoked, defending
    // against a future refactor that skips the spawn entirely.
    assert!(
        abort_slow.update_invocations.load(Ordering::SeqCst) >= 1,
        "AbortAfterOneChunkStore::update was never called in \
         background-spawn path; FastSlowStore::update did not take the \
         spawn branch. update_invocations={}",
        abort_slow.update_invocations.load(Ordering::SeqCst)
    );

    // Load-bearing assertion: the operator-visible error log MUST
    // carry the consumer's authoritative Err tag, NOT the producer's
    // symptom. `logs_contain` is in scope via `#[nativelink_test]`'s
    // `#[traced_test]` macro expansion.
    assert!(
        logs_contain(BACKGROUND_ABORT_TAG),
        "#512 consumer-error-prefer policy not enforced at background \
         slow-write site — producer-side channel-closed swallowed \
         consumer's root cause in the `error!(error = ?e)` log line. \
         Expected log to contain `{BACKGROUND_ABORT_TAG}` (the slow \
         tier's authoritative Err), but it was absent — operators see \
         only the buf_channel symptom."
    );

    Ok(())
}
