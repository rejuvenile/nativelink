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

//! #334 production cascade — regression tests for two distinct
//! `FastSlowStore` failure modes observed today (2026-05-08, 67 GB RSS at
//! 1h15m, 2743 errors/min, 723 unique 16KB-1MB digests):
//!
//! **Fix A — typed `BackpressureSignal` preservation.** When the fast
//! tier (MemoryStore) emits the wire-stable
//! `Code::ResourceExhausted + BackpressureSignal::MemoryStoreAtCapacity`
//! signal, `FastSlowStore::update` must surface it to the caller. Prior
//! behavior dropped `fast_rx` first, the data-stream future failed with
//! a generic `Code::Internal "receiver disconnected"` wrapper, and the
//! match returned the Internal at line 3789 BEFORE checking `fast_res`.
//! Bazel saw the generic Internal, did NOT honor the typed retry hint,
//! retried immediately → cascade.
//!
//! **Fix B — aggregate slow-write byte cap.** `in_flight_slow_writes`
//! grew unbounded under slow-tier wedge conditions. The new
//! `slow_writes_in_flight_max_bytes` config (default 8 GiB) caps it via
//! a typed `BackpressureSignal::SlowWritesAtCapacity` rejection so the
//! caller can back off instead of OOMing the process.
//!
//! Asymmetric-contract coverage (CLAUDE.md mandatory practice):
//! - **Under-action** (positive): each fix MUST emit the typed
//!   discriminator at the right moment.
//! - **Over-action** (negative): the cap MUST NOT fire under the
//!   threshold, and the BackpressureSignal-preservation logic MUST NOT
//!   demote unrelated `fast_res` errors.
//!
//! Test pattern per CLAUDE.md:
//!   - Production composition: real `MemoryStore` (with
//!     `emit_backpressure_enabled`) inside a real `FastSlowStore`.
//!   - 5-second `tokio::time::timeout` deadlock-detector wrap.
//!   - SPECIFIC `.expect()` messages naming the contract.
//!   - Mutation step: comment out the fix; the test must red-fail with
//!     the SPECIFIC message.

#![cfg(feature = "chunked_fast_slow")]

use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::{
    EvictionPolicy, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use prost::Message;
use tokio::sync::Notify;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALID_HASH3: &str = "0123456789abcdef000000000000000000030000000000000123456789abcdef";
const VALID_HASH4: &str = "0123456789abcdef000000000000000000040000000000000123456789abcdef";

/// Assert that `err` carries `ResourceExhausted` AND the
/// `BackpressureSignal` discriminator with the expected `reason`.
fn assert_backpressure_signal(err: &Error, expected_reason: backpressure_signal::Reason) {
    assert_eq!(
        err.code,
        Code::ResourceExhausted,
        "expected ResourceExhausted, got code={:?} messages={:?}",
        err.code,
        err.messages,
    );
    let signal_detail = err
        .details
        .iter()
        .find(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL)
        .unwrap_or_else(|| {
            panic!(
                "must carry a BackpressureSignal detail (typed contract violated; \
                 caller will retry without honoring backoff hint, replaying the \
                 #334 production cascade). messages={:?} details_len={}",
                err.messages,
                err.details.len()
            )
        });
    let decoded = BackpressureSignal::decode(&*signal_detail.value)
        .expect("encoded BackpressureSignal must decode cleanly");
    assert_eq!(
        decoded.reason, expected_reason as i32,
        "expected reason={:?} got reason={}",
        expected_reason, decoded.reason,
    );
}

/// Drive `StoreLike::update` with a single-shot payload, with an
/// initial `yield_now()` in the producer so the inner data_stream_fut
/// suspends on `reader.recv().await` while the sibling fast_store_fut
/// gets polled. This is the precondition for the #334 small-blob bug:
/// fast_store_fut must early-reject (drop `fast_rx`) BEFORE
/// data_stream_fut tries to send. Without the yield, the producer
/// completes synchronously inside the same poll cycle and
/// data_stream_fut's send wins the race — `data_res = Ok` and the
/// `fast_res?` path naturally surfaces the typed signal even WITHOUT
/// Fix A in place. (Production has the same race window, but with
/// gRPC ByteStream chunks crossing the wire there is always
/// inter-chunk latency that lets the consumer poll first.)
async fn drive_update(store: &Store, key: StoreKey<'_>, payload: Bytes) -> Result<(), Error> {
    let (mut tx, rx) = make_buf_channel_pair();
    let payload_len = payload.len() as u64;
    let send_fut = async move {
        // Yield so the sibling future gets a poll window before any
        // send. In production this is the natural state — gRPC
        // ByteStream chunks arrive over the wire with non-zero
        // inter-arrival latency.
        tokio::task::yield_now().await;
        // Send may legitimately fail with channel-closed if the
        // sibling reader (fast_store_fut) early-rejected before this
        // send; the test caller handles that case.
        let _ = tx.send(payload).await;
        let _ = tx.send_eof();
        Ok::<(), Error>(())
    };
    let update_fut = store.update(key, rx, UploadSizeInfo::ExactSize(payload_len));
    let (_send_res, update_res) = tokio::join!(send_fut, update_fut);
    update_res
}

/// Build a real `FastSlowStore` with a tiny-cap `MemoryStore` fast tier
/// (`emit_backpressure_enabled = true`) and a real `MemoryStore` slow
/// tier. The fast tier mirrors the production `MemoryStore` wired
/// inside `cas_FAST_SLOW_STORE`'s `SizePartitioningStore` fast branch.
fn make_fast_slow_with_tiny_fast_cap(fast_cap_bytes: usize) -> (Arc<FastSlowStore>, Store, Store) {
    let fast = Store::new(MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: fast_cap_bytes,
            ..Default::default()
        }),
        emit_backpressure_enabled: true,
    }));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0, // 0 = uncapped (test default)
        },
        fast.clone(),
        slow.clone(),
    );
    (fss, fast, slow)
}

// ---------------------------------------------------------------------
// Fix A — small-blob non-chunked path (the production hot path).
// ---------------------------------------------------------------------

/// **Fix A — small blob, under-action coverage.** Fill the fast tier to
/// its byte cap with one blob; a second update that would force
/// eviction MUST surface `ResourceExhausted +
/// BackpressureSignal::MemoryStoreAtCapacity` through the FastSlowStore
/// boundary. WITHOUT the fix, the data-stream future fails first with
/// `Code::Internal "receiver disconnected"` (because `fast_rx` was
/// dropped) and the match at line 3789 returns it before checking
/// `fast_res`, masking the typed signal.
///
/// Mutation step: comment out the new "fast_res wins on
/// BackpressureSignal" branch in `fast_slow_store.rs::update`. The test
/// MUST red-fail with the bespoke "typed BackpressureSignal must be
/// preserved when MemoryStore early-rejects" panic message below.
#[nativelink_test]
async fn fix_a_small_blob_preserves_backpressure_signal_through_fast_slow_store()
-> Result<(), Error> {
    // 1 KiB cap on the fast tier: first 1024-byte insert fits; second
    // 1024-byte insert would exceed and triggers
    // `check_backpressure_gate` (`memory_store.rs:367`).
    let (fss, _fast, _slow) = make_fast_slow_with_tiny_fast_cap(1024);
    let store: Store = Store::new(fss);

    let payload1 = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest1.into(), Bytes::from(payload1)),
    )
    .await
    .expect("first insert must not deadlock — baseline contract")?;

    // Second insert: same size, would force eviction in the fast tier.
    let payload2 = vec![1u8; 1024];
    let digest2 = DigestInfo::try_new(VALID_HASH2, payload2.len() as u64)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest2.into(), Bytes::from(payload2)),
    )
    .await
    .expect(
        "second insert must not deadlock — Fix A path must return promptly with \
         the typed signal, not hang",
    );

    let err = result.expect_err(
        "second insert MUST return an Err — fast tier at capacity with \
         emit_backpressure_enabled=true",
    );
    assert_backpressure_signal(&err, backpressure_signal::Reason::MemoryStoreAtCapacity);
    // The mutation-step assertion below documents the canonical failure
    // string for the mutation-step. The `assert_backpressure_signal`
    // helper above panics with a similar message; this `assert!` below
    // is the primary contract guard.
    assert!(
        err.code == Code::ResourceExhausted && !err.details.is_empty(),
        "typed BackpressureSignal must be preserved when MemoryStore early-rejects \
         in the small-blob non-chunked FastSlowStore::update path — got code={:?} \
         messages={:?} details_len={} (would have shipped a generic Code::Internal \
         to Bazel, which would not honor the retry hint and would replay the #334 \
         production cascade)",
        err.code,
        err.messages,
        err.details.len(),
    );
    Ok(())
}

// ---------------------------------------------------------------------
// Fix B — slow-write byte cap.
// ---------------------------------------------------------------------

/// `GatedSlowStore` — a slow-store fake whose `update` blocks on a
/// `Notify` until the test releases the gate. Lets us pin in-flight
/// slow writes so we can deterministically drive the
/// `slow_writes_in_flight_max_bytes` cap.
#[derive(MetricsComponent)]
struct GatedSlowStore {
    /// Wakes when the test calls `release()` — gates the
    /// `update` future from completing.
    release: Arc<Notify>,
    /// Counts how many concurrent `update` invocations are blocked.
    in_flight: Arc<AtomicUsize>,
    /// Set by Drop so a leaked store at end-of-test surfaces.
    dropped: Arc<AtomicBool>,
}

impl GatedSlowStore {
    fn new() -> (Arc<Self>, Arc<Notify>, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let release = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                release: release.clone(),
                in_flight: in_flight.clone(),
                dropped: dropped.clone(),
            }),
            release,
            in_flight,
            dropped,
        )
    }
}

impl Drop for GatedSlowStore {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[async_trait]
impl StoreDriver for GatedSlowStore {
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
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Drain bytes immediately so the producer doesn't backpressure on
        // the buf_channel (the test wants the spawn-task to be alive +
        // pinning bytes, not stuck on send).
        reader.drain().await?;
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        // Block until the test releases.
        self.release.notified().await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
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
            "GatedSlowStore: get_part not supported"
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

default_health_status_indicator!(GatedSlowStore);

/// Build a real `FastSlowStore` with a real `MemoryStore` fast tier and
/// a `GatedSlowStore` slow tier. The fast tier is large so it doesn't
/// emit backpressure of its own — we want to exercise ONLY Fix B.
fn make_fast_slow_with_gated_slow(
    cap_bytes: u64,
) -> (
    Arc<FastSlowStore>,
    Store,
    Arc<Notify>,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
) {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (gated, release, in_flight, dropped) = GatedSlowStore::new();
    let slow = Store::new(gated);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: cap_bytes,
        },
        fast.clone(),
        slow.clone(),
    );
    let store: Store = Store::new(fss.clone());
    (fss, store, release, in_flight, dropped)
}

/// Wait until `cond` returns true OR the timeout fires.
async fn wait_until<F: Fn() -> bool>(label: &str, cond: F) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {label}"));
}

/// **Fix B — under-action coverage.** Cap=4096; submit two 2048-byte
/// writes that pin in-flight (each succeeds, in_flight bytes climbs to
/// 4096 = at cap). Submit a third 1024-byte write — it MUST return
/// `ResourceExhausted + BackpressureSignal::SlowWritesAtCapacity` (NOT
/// Internal, NOT Ok). Release the gate; in-flight drops; new write
/// succeeds.
///
/// Mutation step: comment out the cap-check in `fast_slow_store.rs`
/// (the new `if would_exceed_cap { return Err(...) }`). The test MUST
/// red-fail with the bespoke "in-flight slow-write byte cap not
/// enforced — unbounded memory growth contract violated" message.
#[nativelink_test]
async fn fix_b_slow_writes_in_flight_byte_cap_emits_typed_signal() -> Result<(), Error> {
    let cap_bytes: u64 = 4096;
    let (fss, store, release, in_flight, _dropped) = make_fast_slow_with_gated_slow(cap_bytes);

    let payload1 = vec![0u8; 2048];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest1.into(), Bytes::from(payload1)),
    )
    .await
    .expect("first 2048-byte insert must not deadlock")?;
    wait_until("first slow-write spawn pinned", || {
        in_flight.load(Ordering::SeqCst) == 1
    })
    .await;

    let payload2 = vec![1u8; 2048];
    let digest2 = DigestInfo::try_new(VALID_HASH2, payload2.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest2.into(), Bytes::from(payload2)),
    )
    .await
    .expect("second 2048-byte insert must not deadlock; in_flight at exactly cap")?;
    wait_until("second slow-write spawn pinned (in_flight=2)", || {
        in_flight.load(Ordering::SeqCst) == 2
    })
    .await;

    // Verify in-flight bytes counter exposed on the store equals cap.
    assert_eq!(
        fss.in_flight_slow_write_bytes(),
        cap_bytes,
        "in-flight bytes counter must equal exactly the sum of the two pinned \
         payloads (4096); if smaller, the increment-on-insert path is broken"
    );

    // Third insert exceeds the cap by 1024 bytes — MUST be rejected
    // with the typed signal.
    let payload3 = vec![2u8; 1024];
    let digest3 = DigestInfo::try_new(VALID_HASH3, payload3.len() as u64)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest3.into(), Bytes::from(payload3)),
    )
    .await
    .expect("third insert must not deadlock — Fix B must return promptly");

    let err = result.expect_err(
        "third insert MUST return Err — in-flight slow-write byte cap not \
         enforced — unbounded memory growth contract violated",
    );
    assert_backpressure_signal(&err, backpressure_signal::Reason::SlowWritesAtCapacity);

    // Sanity: in_flight_bytes counter MUST NOT have moved past the cap
    // (the rejected insert must NOT have incremented).
    assert_eq!(
        fss.in_flight_slow_write_bytes(),
        cap_bytes,
        "in-flight bytes counter must NOT have incremented for the rejected \
         insert; if larger than cap, the cap-check came AFTER the increment \
         (over-action — broken)"
    );

    // Release the gate; both pinned writes drain; counter goes to 0.
    release.notify_waiters();
    release.notify_waiters();
    wait_until("in-flight drains to zero after release", || {
        fss.in_flight_slow_write_bytes() == 0
    })
    .await;

    // Now a new write fits.
    let payload4 = vec![3u8; 1024];
    let digest4 = DigestInfo::try_new(VALID_HASH4, payload4.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest4.into(), Bytes::from(payload4)),
    )
    .await
    .expect("post-drain insert must succeed within timeout")?;
    Ok(())
}

/// **Fix B — over-action / negative coverage.** With the cap set to a
/// large value (1 GiB) and only modest writes in flight, NO insert MUST
/// be rejected. Guards against shipping a too-tight cap that would
/// regress healthy workloads.
#[nativelink_test]
async fn fix_b_does_not_fire_below_cap() -> Result<(), Error> {
    let cap_bytes: u64 = 1024 * 1024 * 1024; // 1 GiB
    let (_fss, store, release, _in_flight, _dropped) = make_fast_slow_with_gated_slow(cap_bytes);

    for (i, hash) in [VALID_HASH1, VALID_HASH2, VALID_HASH3, VALID_HASH4]
        .iter()
        .enumerate()
    {
        let payload = vec![i as u8; 4096];
        let digest = DigestInfo::try_new(*hash, payload.len() as u64)?;
        tokio::time::timeout(
            Duration::from_secs(5),
            drive_update(&store, digest.into(), Bytes::from(payload)),
        )
        .await
        .unwrap_or_else(|_| panic!("insert #{i} must not deadlock under uncapped load"))?;
    }
    // Release the pending writes so the test cleans up promptly.
    for _ in 0..4 {
        release.notify_waiters();
    }
    Ok(())
}

/// **Fix A + Fix B interaction.** When BOTH the fast tier emits
/// MemoryStoreAtCapacity AND the slow-writes cap would also reject, the
/// returned error MUST be ONE of the typed signals (not generic
/// Internal). Either is acceptable; cascading errors are not.
#[nativelink_test]
async fn fix_a_and_b_compose_to_typed_signal_only() -> Result<(), Error> {
    // Tight fast cap (1 KiB) + tight slow-write cap (4 KiB).
    let cap_bytes: u64 = 4096;
    let fast = Store::new(MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1024,
            ..Default::default()
        }),
        emit_backpressure_enabled: true,
    }));
    let (gated, release, in_flight, _dropped) = GatedSlowStore::new();
    let slow = Store::new(gated);
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: cap_bytes,
        },
        fast.clone(),
        slow.clone(),
    );
    let store: Store = Store::new(fss.clone());

    // Fill fast tier to cap with a 1 KiB insert; pins one slow write.
    let payload1 = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest1.into(), Bytes::from(payload1)),
    )
    .await
    .expect("priming insert must not deadlock")?;
    wait_until("priming write pinned", || {
        in_flight.load(Ordering::SeqCst) == 1
    })
    .await;

    // Now: a 4096-byte insert. Fast tier is at capacity — MemoryStore
    // would reject. Slow-write cap is 4096; in-flight already has 1024,
    // adding 4096 would push to 5120 > 4096 → slow-write cap also rejects.
    // Whichever fires first, the result MUST be a typed signal.
    let payload2 = vec![1u8; 4096];
    let digest2 = DigestInfo::try_new(VALID_HASH2, payload2.len() as u64)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest2.into(), Bytes::from(payload2)),
    )
    .await
    .expect("composed-error path must not deadlock");

    let err = result.expect_err(
        "composed-error insert MUST return Err — both fast tier AND \
         slow-write cap would reject",
    );
    assert_eq!(
        err.code,
        Code::ResourceExhausted,
        "composed-error MUST be Code::ResourceExhausted (NOT Internal). got code={:?} \
         messages={:?} (cascading-Internal would replay the #334 production cascade \
         because Bazel does not honor backoff hints on Internal)",
        err.code,
        err.messages,
    );
    let has_signal = err
        .details
        .iter()
        .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL);
    assert!(
        has_signal,
        "composed-error MUST carry the typed BackpressureSignal discriminator; \
         details_len={} (without the discriminator, looks_like_dead_channel \
         would tear down the h2 channel — #147 regression risk)",
        err.details.len(),
    );
    // Drain so the test cleans up.
    release.notify_waiters();
    release.notify_waiters();
    Ok(())
}
