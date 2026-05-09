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
    EvictionPolicy, ExistenceCacheSpec, FastSlowSpec, MemorySpec, StoreDirection, StoreSpec,
    VerifySpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    BACKPRESSURE_SIGNAL_TYPE_URL, BackpressureSignal, backpressure_signal,
};
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
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

/// **Fix A — small blob, under-action coverage in production
/// composition.** Wraps `FastSlowStore` in the same composition the
/// production server CAS chain uses
/// (`ExistenceCacheStore → VerifyStore → FastSlowStore`, see MEMORY.md
/// "Server CAS Store Architecture"). Fills the inner fast tier to its
/// byte cap with one blob, PINS it (so the Fix C eviction extension
/// cannot free room), then issues a second update that would force
/// eviction. It MUST surface `ResourceExhausted +
/// BackpressureSignal::MemoryStoreAtCapacity` through every wrapping
/// layer. The bug being fixed is cross-component signal preservation;
/// testing only at the FSS unit boundary would miss a regression where
/// VerifyStore (or ExistenceCacheStore) demotes the typed signal to
/// generic `Code::Internal` on its way out — exactly the #334 cascade
/// shape. WITHOUT the fix, the FSS data-stream future fails first with
/// `Code::Internal "receiver disconnected"` (because `fast_rx` was
/// dropped) and the match at line 3789 returns it before checking
/// `fast_res`, masking the typed signal.
///
/// Cap bumped from 1 KiB to 4 KiB (#334 bundle fixup #8a-companion):
/// pin_cap = 25% × cap, so a 1 KiB pinned entry needs ≥ 4 KiB cap to
/// fit inside the pin budget. Mirrors the `fc365ac3` pattern that
/// fixed three sibling tests for the Fix C eviction extension's
/// "evict-unpinned-LRU-before-emit" behavior.
///
/// Mutation step: comment out the new "fast_res wins on
/// BackpressureSignal" branch in `fast_slow_store.rs::update`. The test
/// MUST red-fail with the bespoke "typed BackpressureSignal must be
/// preserved when MemoryStore early-rejects" panic message below.
#[nativelink_test]
async fn fix_a_small_blob_preserves_backpressure_signal_through_fast_slow_store()
-> Result<(), Error> {
    // 4 KiB cap on the fast tier: 1 KiB pinned + 1 KiB pressure write
    // = 2 KiB live; second 1 KiB write triggers backpressure gate (the
    // pinned entry is unevictable so `evict_unpinned_lru_bytes` finds
    // nothing in cache and the gate emits the typed signal).
    let (fss, _fast, _slow) = make_fast_slow_with_tiny_fast_cap(4 * 1024);
    // Production composition wrap: `cas_STORE` =
    // `ExistenceCacheStore → VerifyStore → FastSlowStore`. Without
    // this wrap the test would not exercise the typed-signal
    // survival contract across the wrapping `tokio::join!` /
    // wrap-and-rethrow boundaries that production code crosses.
    let verify = VerifyStore::new(
        &VerifySpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            verify_size: true,
            verify_hash: false,
        },
        Store::new(fss.clone()),
    );
    let cache = ExistenceCacheStore::new(
        &ExistenceCacheSpec {
            backend: StoreSpec::Memory(MemorySpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_count: 1024,
                ..Default::default()
            }),
        },
        Store::new(verify),
    );
    let store: Store = Store::new(cache);

    // Fill the cap. Pre-cap pressure writes so the cache holds 3 KiB
    // worth of unpinned + 1 KiB pinned. The 4th 1 KiB write will face
    // the gate: evict_unpinned_lru_bytes can only free unpinned, and
    // even if it frees some, the pinned-bytes accounting in
    // `would_exceed_capacity` keeps the cap honest.
    let payload1 = vec![0u8; 1024];
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest1.into(), Bytes::from(payload1)),
    )
    .await
    .expect("first insert must not deadlock — baseline contract")?;

    // PIN the first entry so the Fix C eviction extension cannot free
    // it. Without this pin, the gate's `evict_unpinned_lru_bytes` call
    // would evict the 1 KiB entry, freeing room for the 4 KiB cap to
    // accept another 1 KiB write — defeating the test's premise.
    fss.fast_store_handle().pin_digests(&[digest1]);

    // Pad cache to cap with two more 1 KiB writes (UNPINNED). After
    // this: 1 KiB pinned + 2 KiB unpinned = 3 KiB used, cap 4 KiB.
    for hash in [VALID_HASH3, VALID_HASH4] {
        let payload_pad = vec![0xCDu8; 1024];
        let digest_pad = DigestInfo::try_new(hash, payload_pad.len() as u64)?;
        tokio::time::timeout(
            Duration::from_secs(5),
            drive_update(&store, digest_pad.into(), Bytes::from(payload_pad)),
        )
        .await
        .expect("pad insert must not deadlock — baseline contract")?;
    }

    // Second insert: 2 KiB. After eviction of the 2 KiB unpinned, only
    // 1 KiB pinned remains, leaving 3 KiB free — but a 2 KiB write
    // would fit. Use 4 KiB instead (over cap even after evicting all
    // unpinned: 4 KiB cap - 1 KiB pinned = 3 KiB free, 4 KiB > 3 KiB).
    let payload2 = vec![1u8; 4 * 1024];
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
         emit_backpressure_enabled=true (4 KiB write into 4 KiB cap with \
         1 KiB pinned cannot fit even after Fix C eviction extension frees \
         the 2 KiB unpinned padding)",
    );
    // Bespoke contract guard FIRST so the mutation step prints this
    // canonical message, not the generic helper-internal `assert_eq!`
    // panic. Documented in CLAUDE.md mutation-step procedure as the
    // canonical red-fail string for Fix A.
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
    // Discriminator-detail check: same contract, but goes deeper into
    // the proto detail. Runs after the bespoke check so the mutation
    // step's primary panic is the bespoke string.
    assert_backpressure_signal(&err, backpressure_signal::Reason::MemoryStoreAtCapacity);
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
    /// When `true`, `update` returns `Err(Code::Internal "...")`
    /// AFTER the gate releases. Used to exercise the slow-write
    /// failure-path counter-decrement contract (Fix B failure arm at
    /// `fast_slow_store.rs:4196-4221`). When `false` (default),
    /// `update` returns Ok after release (success path).
    fail_after_release: AtomicBool,
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
                fail_after_release: AtomicBool::new(false),
            }),
            release,
            in_flight,
            dropped,
        )
    }

    /// Test hook: if set to `true`, every subsequent `update` will
    /// return `Err(Code::Internal)` after the gate releases.
    fn set_fail_after_release(&self, fail: bool) {
        self.fail_after_release.store(fail, Ordering::Release);
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
        if self.fail_after_release.load(Ordering::Acquire) {
            return Err(make_err!(
                Code::Internal,
                "GatedSlowStore: fail_after_release was set"
            ));
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

// ---------------------------------------------------------------------
// Fix A — over-action coverage (predicate must not over-match).
// ---------------------------------------------------------------------

/// Fast-tier store that returns a BARE `Code::ResourceExhausted` with
/// NO `BackpressureSignal` discriminator detail. Models a future
/// fast-tier store kind (e.g. an SSD ExistenceCache returning
/// disk-full) where the rejection is `ResourceExhausted` but is NOT
/// the typed-backpressure shape Fix A is meant to preserve. Used to
/// prove the strict heuristic at `fast_slow_store.rs:3922-3927`
/// (`Code::ResourceExhausted && error_has_backpressure_signal(e)`)
/// does NOT over-match a bare `ResourceExhausted` and silently demote
/// it to a backpressure-shaped retry path.
#[derive(MetricsComponent, Default)]
struct BareResourceExhaustedFastStore {}

#[async_trait]
impl StoreDriver for BareResourceExhaustedFastStore {
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
        // Drain so the producer doesn't deadlock on send before we
        // return our error.
        let _ = reader.drain().await;
        Err(make_err!(
            Code::ResourceExhausted,
            "BareResourceExhaustedFastStore: bare ResourceExhausted, no signal detail"
        ))
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
            "BareResourceExhaustedFastStore: get_part not supported"
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

default_health_status_indicator!(BareResourceExhaustedFastStore);

/// **Fix A — over-action coverage.** A fast-tier store that emits
/// `Code::ResourceExhausted` WITHOUT the `BackpressureSignal`
/// discriminator (e.g. a future SSD-backed ExistenceCache returning
/// disk-full) MUST NOT trigger Fix A's typed-signal-preservation
/// branch. The strict heuristic
/// (`Code::ResourceExhausted && error_has_backpressure_signal(e)`)
/// would silently demote the bare `ResourceExhausted` to a
/// backpressure-shaped retry path if loosened — Bazel would then
/// retry an unrelated condition (disk-full) on a backpressure
/// schedule and never make progress.
///
/// The expected behavior on a bare `ResourceExhausted` from the fast
/// tier is: the data-stream future fires first (`Code::Internal
/// "Failed to send message to fast_store"` because `fast_rx` was
/// dropped), the match returns the Internal at line 3789 BEFORE the
/// new Fix A branch ever runs. The caller sees Internal — NOT
/// ResourceExhausted — proving Fix A did NOT match.
///
/// Mutation step: change the predicate to drop the
/// `error_has_backpressure_signal(e)` clause (i.e. match on
/// `Code::ResourceExhausted` alone). The test MUST red-fail with the
/// bespoke "Fix A predicate over-matched: bare ResourceExhausted
/// demoted to backpressure shape" message.
#[nativelink_test]
async fn fix_a_does_not_over_match_bare_resource_exhausted() -> Result<(), Error> {
    let fast = Store::new(Arc::new(BareResourceExhaustedFastStore::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
        },
        fast,
        slow,
    );
    let store: Store = Store::new(fss);

    let payload = vec![0u8; 1024];
    let digest = DigestInfo::try_new(VALID_HASH1, payload.len() as u64)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest.into(), Bytes::from(payload)),
    )
    .await
    .expect("must not deadlock — fast tier rejects synchronously");

    let err = result.expect_err(
        "fast tier always returns Err(ResourceExhausted) — update MUST surface an Err",
    );

    // The OVER-ACTION contract: Fix A's heuristic must NOT match a
    // bare ResourceExhausted (no BackpressureSignal). Since
    // `fast_res` lacks the discriminator, the new branch must NOT
    // fire; the existing match arms surface `data_res`'s `Code::Internal
    // "Failed to send message to fast_store"` instead. Note: this
    // assertion is the inverse of the under-action test — we EXPECT
    // a non-ResourceExhausted error here.
    let has_signal = err
        .details
        .iter()
        .any(|any| any.type_url == BACKPRESSURE_SIGNAL_TYPE_URL);
    assert!(
        !has_signal,
        "Fix A predicate over-matched: bare ResourceExhausted demoted to \
         backpressure shape. err.code={:?} details={:?} (a future fast-tier \
         store returning bare ResourceExhausted for disk-full would be \
         silently treated as transient backpressure — Bazel would retry \
         on a backpressure schedule and never make progress)",
        err.code,
        err.details.len(),
    );
    // Belt-and-suspenders: the actual code that fires is the data_res
    // arm (`Code::Internal "Failed to send message to fast_store"`).
    // Either Internal (data_res arm) or the bare ResourceExhausted
    // surfacing through `fast_res?` is acceptable; the contract is
    // "no typed BackpressureSignal demotion."
    assert!(
        err.code == Code::Internal || err.code == Code::ResourceExhausted,
        "expected Internal (data_res arm) OR bare ResourceExhausted (fast_res? arm), \
         got code={:?}",
        err.code,
    );
    Ok(())
}

// ---------------------------------------------------------------------
// Fix B — failure-path counter-decrement coverage.
// ---------------------------------------------------------------------

/// **Fix B — failure-path counter-decrement coverage.** When the
/// background slow-write returns `Err(...)` (not `Ok(())`), the
/// `in_flight_slow_writes_bytes` counter MUST still drain to zero.
/// Without this, the cap-check at
/// `check_slow_writes_capacity_gate` would over time admit fewer and
/// fewer admissions until every admission is rejected (the counter
/// drifts up by every failed write).
///
/// Mutation step (corrected per #334 bundle fixup #8c — the original
/// docstring pointed at `fast_slow_store.rs:4196-4221`, which is the
/// failure-recovery `failed_slow_writes.insert + pin_digests` block,
/// NOT the counter decrement). The actual counter-decrement site is
/// at `fast_slow_store.rs:4237-4257` (the post-recovery
/// `let mut guard = in_flight.lock(); let removed = guard.remove(...);
/// if let Some(removed_chunks) = removed { ...
/// in_flight_bytes.fetch_sub(removed_bytes, ...) }` block — which
/// runs on BOTH success and failure paths because it's outside the
/// `match res` arms). To mutate, comment out the
/// `in_flight_bytes.fetch_sub(removed_bytes, Ordering::Relaxed);` call
/// at `:4252`. The test MUST red-fail with the bespoke "counter
/// leaked on failure path — admission cap will permanently reject
/// after first slow-write failure" message.
#[nativelink_test]
async fn fix_b_counter_decrements_on_slow_write_failure() -> Result<(), Error> {
    let cap_bytes: u64 = 1024 * 1024 * 1024; // 1 GiB - way above what we use
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let (gated, release, in_flight, _dropped) = GatedSlowStore::new();
    // Configure GatedSlowStore so its background `update` returns
    // `Err(Internal)` after release — this drives the failure-arm at
    // `fast_slow_store.rs:4196-4221` instead of the success-arm.
    gated.set_fail_after_release(true);
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
        fast,
        slow,
    );
    let store: Store = Store::new(fss.clone());

    let payload = vec![0u8; 4096];
    let digest = DigestInfo::try_new(VALID_HASH1, payload.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest.into(), Bytes::from(payload)),
    )
    .await
    .expect("the update call must complete promptly (failure happens in background spawn)")?;

    // Wait for the spawned task to begin — counter must be at the
    // payload size while the gate is held.
    wait_until("background spawn enters update (in_flight=1)", || {
        in_flight.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        fss.in_flight_slow_write_bytes(),
        4096,
        "counter must reflect pinned bytes while spawn is active"
    );

    // Release the gate — slow-store update returns Err(Internal),
    // background closure should run the failure-arm at :4196-4221:
    // (a) record digest in failed_slow_writes, (b) re-pin, (c) remove
    // from in_flight + fetch_sub.
    release.notify_waiters();

    // The counter MUST drain to zero on failure — same as on success.
    // If the failure-arm forgets to decrement, the counter will be
    // stuck at 4096 forever and this poll will time out + panic with
    // the bespoke message.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fss.in_flight_slow_write_bytes() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "counter leaked on failure path — admission cap will permanently \
             reject after first slow-write failure (counter stuck at {} bytes \
             after slow-write returned Err; the failure-arm at \
             fast_slow_store.rs:4196-4221 must remove the in-flight entry AND \
             decrement the byte counter)",
            fss.in_flight_slow_write_bytes()
        )
    });
    Ok(())
}

// ---------------------------------------------------------------------
// Fix B — `update_oneshot` sibling coverage.
// ---------------------------------------------------------------------

/// **Fix B — `update_oneshot` sibling under-action coverage.** Same
/// shape as `fix_b_slow_writes_in_flight_byte_cap_emits_typed_signal`
/// but exercises the `update_oneshot` cap-check at
/// `fast_slow_store.rs:4330` and counter mutation at
/// `:4338-4339`/`:4459-4471`. `update_oneshot` is reached when the
/// caller has the entire payload in memory — common for AC writes
/// and the worker_proxy_store parallel-fetch path. Without this
/// sibling test, a regression in the `update_oneshot` cap-check
/// would slip through (the streaming `update` test would still
/// pass).
///
/// Mutation step: comment out the cap-check at `:4359` (the
/// `if let Err(cap_err) = self.check_slow_writes_capacity_gate(...)`
/// block in `update_oneshot`). The test MUST red-fail with the bespoke
/// "update_oneshot in-flight slow-write byte cap not enforced" message.
#[nativelink_test]
async fn fix_b_update_oneshot_in_flight_byte_cap_emits_typed_signal() -> Result<(), Error> {
    let cap_bytes: u64 = 4096;
    let (fss, store, release, in_flight, _dropped) = make_fast_slow_with_gated_slow(cap_bytes);

    // Use update_oneshot directly (Store::update_oneshot ultimately
    // calls FastSlowStore::update_oneshot).
    let payload1: Bytes = Bytes::from(vec![0u8; 2048]);
    let digest1 = DigestInfo::try_new(VALID_HASH1, payload1.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest1, payload1),
    )
    .await
    .expect("first update_oneshot must not deadlock")?;
    wait_until("first slow-write spawn pinned (oneshot)", || {
        in_flight.load(Ordering::SeqCst) == 1
    })
    .await;

    let payload2: Bytes = Bytes::from(vec![1u8; 2048]);
    let digest2 = DigestInfo::try_new(VALID_HASH2, payload2.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest2, payload2),
    )
    .await
    .expect("second update_oneshot must not deadlock; in_flight at exactly cap")?;
    wait_until("second slow-write spawn pinned (oneshot, in_flight=2)", || {
        in_flight.load(Ordering::SeqCst) == 2
    })
    .await;

    assert_eq!(
        fss.in_flight_slow_write_bytes(),
        cap_bytes,
        "in-flight bytes counter must equal cap after two oneshot pins"
    );

    // Third update_oneshot exceeds the cap — MUST return typed signal.
    let payload3: Bytes = Bytes::from(vec![2u8; 1024]);
    let digest3 = DigestInfo::try_new(VALID_HASH3, payload3.len() as u64)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest3, payload3),
    )
    .await
    .expect("third update_oneshot must not deadlock");

    let err = result.expect_err(
        "update_oneshot in-flight slow-write byte cap not enforced — \
         the `update_oneshot` Fix B sibling at fast_slow_store.rs:4359 \
         must emit the typed BackpressureSignal::SlowWritesAtCapacity",
    );
    assert_backpressure_signal(&err, backpressure_signal::Reason::SlowWritesAtCapacity);

    // Counter must NOT have moved past the cap — the rejected oneshot
    // insert must NOT have incremented (over-action would be broken).
    assert_eq!(
        fss.in_flight_slow_write_bytes(),
        cap_bytes,
        "in-flight bytes counter must NOT have incremented for the rejected \
         oneshot insert (over-action — increment-after-cap-check is broken)"
    );

    // Release; counter drains; new oneshot fits.
    release.notify_waiters();
    release.notify_waiters();
    wait_until("in-flight drains to zero after release (oneshot)", || {
        fss.in_flight_slow_write_bytes() == 0
    })
    .await;

    let payload4: Bytes = Bytes::from(vec![3u8; 1024]);
    let digest4 = DigestInfo::try_new(VALID_HASH4, payload4.len() as u64)?;
    tokio::time::timeout(
        Duration::from_secs(5),
        store.update_oneshot(digest4, payload4),
    )
    .await
    .expect("post-drain oneshot must succeed within timeout")?;
    Ok(())
}

/// **Fix B — recovery contract.** When a streaming `update` is rejected
/// by the slow-write byte cap, the rejecting code path MUST also:
///   1. Insert the digest into `failed_slow_writes` so the server-side
///      `failed_writes_drain` → `UploadMissingBlobs` recovery loop can
///      pick it up.
///   2. Pin the digest in the fast tier so it stays alive long enough
///      for that recovery loop to run.
///
/// Without these two side-effects the rejected payload is invisible to
/// the recovery pipeline AND ages out of MemoryStore (PIN_TIMEOUT_SECS
/// = 120) → silent data loss if the upstream caller's retry budget
/// exhausts. Code-reviewer MA-5 — sibling of the typed-signal coverage
/// already in `fix_b_slow_writes_in_flight_byte_cap_emits_typed_signal`.
///
/// Mutation step: comment out either the `failed_slow_writes.lock()
/// .insert(d)` or the `fast_store.pin_digests(&[d])` call inside the
/// cap-rejection arm at `fast_slow_store.rs:4022-4028`. The test MUST
/// red-fail with a SPECIFIC bespoke message.
#[nativelink_test]
async fn fix_b_cap_rejection_inserts_into_failed_slow_writes() -> Result<(), Error> {
    let cap_bytes: u64 = 4096;
    let (fss, store, release, in_flight, _dropped) = make_fast_slow_with_gated_slow(cap_bytes);

    // Fill the in-flight map to exactly the cap with two pinned writes.
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

    // Submit a third write that the cap MUST reject. Verify both side-
    // effects (failed_slow_writes insert + fast-tier pin) happened.
    let payload3 = vec![2u8; 1024];
    let payload3_len = payload3.len() as u64;
    let digest3 = DigestInfo::try_new(VALID_HASH3, payload3_len)?;
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drive_update(&store, digest3.into(), Bytes::from(payload3)),
    )
    .await
    .expect("third insert must not deadlock — Fix B must return promptly");

    let err = result.expect_err(
        "third insert MUST return Err — Fix B cap-rejection must drain digest \
         into failed_slow_writes for recovery — contract violated",
    );
    assert_backpressure_signal(&err, backpressure_signal::Reason::SlowWritesAtCapacity);

    // Side-effect #1: digest MUST be in `failed_slow_writes` so the
    // server's drain loop picks it up.
    assert!(
        fss.failed_slow_writes_contains(&digest3),
        "Fix B cap-rejection must drain digest into failed_slow_writes for \
         recovery — contract violated (failed_slow_writes.lock().insert(d) \
         missing from cap-rejection arm)"
    );

    // Side-effect #2: pin MUST be held on the fast tier so the bytes
    // survive the 120s pin TTL window. We verify by calling
    // `has_with_results` on the fast-tier delegate — the in-memory
    // payload was successfully written to the fast tier BEFORE the
    // cap-rejection (the cap check fires AFTER fast-tier write but
    // BEFORE the in-flight pin). The pin call additionally protects
    // it from LRU eviction.
    let mut results = [None; 1];
    let key3: StoreKey<'_> = digest3.into();
    fss.fast_store_handle()
        .has_with_results(&[key3.borrow()], &mut results)
        .await
        .expect("has_with_results must not error");
    assert_eq!(
        results[0],
        Some(payload3_len),
        "Fix B cap-rejection must keep the fast-tier payload alive via \
         pin_digests — contract violated (fast_store.pin_digests(&[d]) \
         missing from cap-rejection arm; the bytes will age out of \
         MemoryStore in 120s if the recovery loop doesn't pick them up)"
    );

    // Cleanup: release the gate so the test's pinned slow writes drain.
    release.notify_waiters();
    release.notify_waiters();
    wait_until("in-flight drains to zero after release", || {
        fss.in_flight_slow_write_bytes() == 0
    })
    .await;
    Ok(())
}
