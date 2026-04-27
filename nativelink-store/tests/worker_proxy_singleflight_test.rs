// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! Task #130 — singleflight/dedup concurrent peer-fetch retries.
//!
//! Production observability (per #171 audit logs) shows ~600 events/min
//! of `Tried to send while stream is closed` peer-fetch failures, plus a
//! recurring ~109K events / 3h pattern in which the **same digest** is
//! read 3-4 times within ~150 ms — all racing for the same upstream h2
//! channel. The first read on the channel succeeds; the subsequent
//! reads observe a poisoned stream-state and return `Code::Internal`
//! mid-stream.
//!
//! `WorkerProxyStore::get_part_sequential` →
//! `try_read_from_worker` → `get_part_and_cache` → `peer.get_part(...)`
//! is re-entered N times concurrently for the same digest because there
//! is no in-flight dedup. Singleflight collapses N concurrent
//! same-digest peer-fetches into 1 leader fetch + N-1 awaiters that
//! receive the leader's bytes; this eliminates the channel-poisoning
//! amplifier that turns 1 concurrent fetch into N-1 production-visible
//! failures.
//!
//! These tests are RED today. They MUST FAIL with specific assertion
//! messages naming the contract violated. The follow-up implementation
//! PR turns them green.
//!
//! Design choice for keying (see
//! `.claude/plans/130-singleflight-peer-fetch.md`):
//!
//! * **Option B: key alone, full-blob reads only** — dedup applies only
//!   when `offset == 0 && length.is_none()`. Partial-range reads bypass
//!   singleflight (matches the parallel CDN-tee partial-range skip
//!   pattern; partial reads are not the source of the production
//!   amplification).

use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::worker_proxy_store::WorkerProxyStore;
use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation, Store, StoreDriver,
    StoreKey, StoreLike, UploadSizeInfo,
};
use pretty_assertions::assert_eq;
use tokio::sync::watch;
use tokio::time::timeout;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

// =====================================================================
// Counting-fake peer Stores
// =====================================================================

/// In-process peer Store that returns a fixed payload on get_part. Counts
/// every entry into get_part via an atomic counter — that counter is the
/// load-bearing assertion for the singleflight tests below.
///
/// Models a healthy worker peer holding the blob: get_part returns the
/// full payload, has_with_results reports the size. The implementation
/// must NOT be a wrapper around MemoryStore — direct ownership of the
/// payload guarantees we count exactly the get_part calls that crossed
/// into the "peer" boundary, with no internal short-circuits.
///
/// IMPORTANT: get_part awaits `release` before producing data. The test
/// explicitly notifies `release` after spawning all N callers AND waiting
/// for them to all enter the peer-fetch path (observed via the counter
/// in the test's poll loop). This synchronizes all callers in the
/// peer-fetch state — the production race condition we are reproducing.
/// Without this barrier, the first caller's get_part_and_cache populates
/// the inner MemoryStore before subsequent callers' inner-check fires,
/// and they all hit the cache instead of racing to the peer (the
/// non-bug case).
#[derive(Debug, MetricsComponent)]
struct CountingPeerStore {
    payload: Bytes,
    /// Number of get_part entries observed. The singleflight contract
    /// is "N concurrent same-digest reads through the proxy => 1 entry
    /// here." Tests assert against this.
    get_part_calls: Arc<AtomicU32>,
    /// When true, get_part returns Err immediately after incrementing
    /// the counter. Used to verify failure-propagation semantics.
    fail: bool,
    /// Concurrency barrier: get_part awaits the watch channel to flip
    /// to `true` before producing data. `watch` is the right primitive
    /// here (NOT `Notify`): receivers that subscribe AFTER the sender
    /// flips immediately see the new value, so callers that arrive late
    /// are not stuck waiting. Notify::notified() with notify_waiters
    /// would deadlock late arrivers.
    ///
    /// The test holds the sender; in tests that exercise the race, the
    /// test polls the counter to confirm all N callers reached this
    /// barrier, then sends `true` to unblock them simultaneously.
    release_rx: watch::Receiver<bool>,
}

default_health_status_indicator!(CountingPeerStore);

#[async_trait]
impl StoreDriver for CountingPeerStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        for (idx, _) in digests.iter().enumerate() {
            if idx < results.len() {
                results[idx] = Some(self.payload.len() as u64);
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        Err(make_err!(Code::Unimplemented, "CountingPeerStore: update unused"))
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        // CRITICAL: count every entry, regardless of fail/success path.
        // The singleflight assertion is "exactly 1 entry here for N
        // concurrent same-digest reads through the proxy."
        self.get_part_calls.fetch_add(1, Ordering::SeqCst);

        // Concurrency barrier: hold the peer-fetch open until the test
        // explicitly releases all callers. This reproduces the production
        // race: N concurrent peer-fetches all in flight at the same time.
        // Without this, the first peer-fetch completes,
        // get_part_and_cache populates the inner MemoryStore, and
        // subsequent callers hit the cache — hiding the bug.
        let mut rx = self.release_rx.clone();
        if !*rx.borrow_and_update() {
            let _ = rx.changed().await;
        }

        if self.fail {
            return Err(make_err!(
                Code::Internal,
                "CountingPeerStore: simulated peer failure for singleflight test"
            ));
        }

        let payload_len = self.payload.len();
        let start = (offset as usize).min(payload_len);
        let end = match length {
            None => payload_len,
            Some(len) => start
                .saturating_add(len as usize)
                .min(payload_len),
        };
        let slice = self.payload.slice(start..end);

        if !slice.is_empty() {
            writer
                .send(slice)
                .await
                .err_tip(|| "CountingPeerStore: send chunk")?;
        }
        writer
            .send_eof()
            .err_tip(|| "CountingPeerStore: send_eof")?;
        Ok(())
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

// =====================================================================
// Helpers
// =====================================================================

/// Build a `WorkerProxyStore` whose inner store is an empty `MemoryStore`
/// (so every read falls through to the locality-map / peer fast path),
/// plus an injected `CountingPeerStore` peer holding `payload`.
///
/// Returns:
///   - `proxy` — wrap-as-Store for production-shaped get_part_unchunked
///   - `get_part_calls` — the peer's get_part entry counter (ASSERTED ON)
///   - `release_tx` — flip to `true` to release the peer's barrier
///     (call after observing all N callers reach the peer-fetch state)
fn make_proxy_with_counting_peer(
    payload: Bytes,
    digest: DigestInfo,
    fail: bool,
) -> (Store, Arc<AtomicU32>, watch::Sender<bool>) {
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    let locality_map = new_shared_blob_locality_map();
    let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());

    let get_part_calls = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(false);
    let peer = Arc::new(CountingPeerStore {
        payload,
        get_part_calls: get_part_calls.clone(),
        fail,
        release_rx,
    });
    let peer_endpoint = "grpc://counting-peer:50081";
    proxy_arc.inject_worker_connection(peer_endpoint, Store::new(peer));
    locality_map
        .write()
        .register_blobs(peer_endpoint, &[digest]);

    (Store::new(proxy_arc), get_part_calls, release_tx)
}

/// Poll the get_part counter until it reaches `target` or `deadline`
/// elapses. Used to wait for all spawned callers to reach the peer's
/// barrier before releasing them. NO sleep-as-synchronization: this
/// polls the atomic, with `tokio::task::yield_now` between checks.
async fn wait_for_counter(counter: &Arc<AtomicU32>, target: u32, deadline: Duration) -> u32 {
    let start = std::time::Instant::now();
    loop {
        let observed = counter.load(Ordering::SeqCst);
        if observed >= target {
            return observed;
        }
        if start.elapsed() >= deadline {
            return observed;
        }
        // Yield to let spawned callers make progress; do NOT sleep, so
        // we react as soon as the counter advances.
        tokio::task::yield_now().await;
    }
}

// =====================================================================
// Test 1: 16 concurrent full-blob reads MUST collapse to 1 peer-fetch
// =====================================================================
//
// Production pattern: same digest read N times within ~150ms because the
// server has no in-flight dedup. This test emulates that by spawning 16
// concurrent get_part_unchunked(digest, 0, None) calls and asserts that
// the peer's get_part entry counter == 1.
//
// RED today: there is no singleflight in WorkerProxyStore yet, so the
// counter will be 16 and the assertion fires with the specific
// "must collapse N concurrent reads into 1 peer-fetch" message.

#[nativelink_test]
async fn concurrent_same_digest_reads_dedup_to_one_peer_fetch() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
    let payload = Bytes::from(payload_vec);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

    let (proxy, get_part_calls, release_tx) =
        make_proxy_with_counting_peer(payload.clone(), digest, /*fail=*/ false);

    const N_CALLERS: usize = 16;
    let mut handles = Vec::with_capacity(N_CALLERS);
    for _ in 0..N_CALLERS {
        let proxy = proxy.clone();
        handles.push(tokio::spawn(async move {
            proxy.get_part_unchunked(digest, 0, None).await
        }));
    }

    // Wait for the spawned callers to reach a steady state — either all
    // N have entered the peer-fetch barrier (no singleflight) OR exactly
    // 1 entered and the rest are parked in the singleflight slot
    // (correctly deduped). 200ms is the polling-loop deadline; the loop
    // returns immediately as soon as the counter reaches N. This is NOT
    // sleep-as-synchronization (CLAUDE.md): it's an explicit-timeout
    // poll loop on an observable atomic.
    let observed_at_barrier =
        wait_for_counter(&get_part_calls, N_CALLERS as u32, Duration::from_millis(200)).await;

    // Now release the peer barrier. With singleflight, the leader
    // proceeds and tees data to the inner cache; the awaiters wake up
    // via the singleflight slot. Without singleflight, all N entered
    // and all N now proceed.
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    // 5s deadlock detector: if singleflight's cancel/wakeup wiring is
    // wrong, the test would otherwise hang. Better to fail loudly than
    // wedge CI. Specific message names the contract.
    let results = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(N_CALLERS);
        for h in handles {
            out.push(h.await.expect("singleflight test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "singleflight test: 16 concurrent same-digest reads must complete \
         within 5s after release — a hang here means a singleflight \
         awaiter wedged on the leader's result_tx (lost wakeup or \
         leader did not publish)",
    );

    // Diagnostic: how many were observed at the peer barrier before
    // release? Helps interpret a failure: 16 = no dedup (the bug); 1 =
    // dedup worked.
    eprintln!(
        "singleflight test: {observed_at_barrier} callers reached peer \
         barrier before release (target {N_CALLERS}); singleflight \
         expects 1, no-dedup expects {N_CALLERS}"
    );

    // Correctness: every caller received the same bytes.
    for (i, r) in results.into_iter().enumerate() {
        let bytes = r.unwrap_or_else(|e| panic!("caller {i} got error: {e:?}"));
        assert_eq!(
            bytes.as_ref(),
            payload.as_ref(),
            "singleflight test: caller {i} received wrong bytes"
        );
    }

    // Singleflight contract: 16 concurrent same-digest reads => 1 peer-fetch.
    let observed = get_part_calls.load(Ordering::SeqCst);
    assert_eq!(
        observed, 1,
        "singleflight must collapse N concurrent reads into 1 peer-fetch — \
         got {observed} peer-fetches for {N_CALLERS} callers (no dedup wired in \
         WorkerProxyStore::get_part_and_cache yet)"
    );

    Ok(())
}

// =====================================================================
// Test 2: 16 concurrent partial-range reads MUST bypass singleflight
//          (Option B: full-blob reads only are deduped)
// =====================================================================
//
// Per Option B in the design doc: dedup applies only to full-blob reads
// (offset == 0 && length.is_none()). Partial-range reads bypass and
// each issues its own peer-fetch.
//
// RED today: there is no dedup at all, so 16 partial reads => 16 peer
// fetches today, which already matches the design assertion. To make
// the test RED on red-state, the assertion is explicit and the *failure
// message* would name the design choice if a future change to "always
// dedup" sneaks in. (Today the assertion passes vacuously; that is
// CORRECT for an Option-B design — the test guards against a future
// regression toward Option A. The other three tests are the load-bearing
// red ones.)

#[nativelink_test]
async fn concurrent_partial_range_reads_match_design() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
    let payload = Bytes::from(payload_vec);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

    let (proxy, get_part_calls, release_tx) =
        make_proxy_with_counting_peer(payload, digest, /*fail=*/ false);

    const N_CALLERS: usize = 16;
    const RANGE_OFFSET: u64 = 10;
    const RANGE_LENGTH: u64 = 100;

    let mut handles = Vec::with_capacity(N_CALLERS);
    for _ in 0..N_CALLERS {
        let proxy = proxy.clone();
        handles.push(tokio::spawn(async move {
            proxy
                .get_part_unchunked(digest, RANGE_OFFSET, Some(RANGE_LENGTH))
                .await
        }));
    }

    // Wait for all callers to reach the peer barrier; with Option B
    // (partial reads bypass singleflight), all N MUST enter the peer.
    let _ = wait_for_counter(&get_part_calls, N_CALLERS as u32, Duration::from_millis(200)).await;
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let results = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(N_CALLERS);
        for h in handles {
            out.push(h.await.expect("partial-range test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "partial-range test: 16 concurrent partial-range reads must complete \
         within 5s — likely deadlock",
    );

    // Correctness: every caller received the requested range.
    for (i, r) in results.into_iter().enumerate() {
        let bytes = r.unwrap_or_else(|e| panic!("caller {i} got error: {e:?}"));
        assert_eq!(
            bytes.len(),
            RANGE_LENGTH as usize,
            "partial-range test: caller {i} received wrong byte count"
        );
    }

    // Option B: partial reads bypass singleflight; counter == N_CALLERS.
    // If a future "always dedup" regression lands, this assertion fires
    // with the design-choice message.
    let observed = get_part_calls.load(Ordering::SeqCst);
    assert_eq!(
        observed,
        N_CALLERS as u32,
        "design Option B: partial-range reads must bypass singleflight — \
         got {observed} peer-fetches (expected {N_CALLERS}); a future \
         regression toward Option A (key+offset+length keying) would \
         fire this assertion"
    );

    Ok(())
}

// =====================================================================
// Test 3: leader failure propagates to all waiters; only 1 peer-fetch
// =====================================================================
//
// If the leader's peer.get_part fails, ALL N awaiters MUST receive the
// same Err — they MUST NOT each independently retry (that's the bug we
// are fixing). The leader's own retrier (inside grpc_store.rs) handles
// retry; awaiters trust the leader's terminal result.
//
// RED today: no singleflight; 16 callers => 16 peer-fetches; counter
// will be 16 and the "must collapse to 1 peer-fetch even on failure"
// assertion fires.

#[nativelink_test]
async fn singleflight_failure_propagates_to_all_waiters() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload = Bytes::from(vec![0u8; PAYLOAD_LEN]);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

    let (proxy, get_part_calls, release_tx) =
        make_proxy_with_counting_peer(payload, digest, /*fail=*/ true);

    const N_CALLERS: usize = 16;
    let mut handles = Vec::with_capacity(N_CALLERS);
    for _ in 0..N_CALLERS {
        let proxy = proxy.clone();
        handles.push(tokio::spawn(async move {
            proxy.get_part_unchunked(digest, 0, None).await
        }));
    }

    // Wait for callers to settle at the peer barrier (or in singleflight
    // slot if implemented); 200ms polling deadline.
    let _ = wait_for_counter(&get_part_calls, N_CALLERS as u32, Duration::from_millis(200)).await;
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let results = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(N_CALLERS);
        for h in handles {
            out.push(h.await.expect("failure-fanout test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "failure-fanout test: 16 concurrent reads against a failing peer \
         must complete (with Err) within 5s — a hang here means a waiter \
         is stuck on a leader that never published its terminal result",
    );

    // Liveness: NO waiter hangs. Every caller got a terminal result.
    // (This is asserted implicitly by the timeout above completing.)

    // Correctness: every caller received Err. None saw a phantom Ok.
    for (i, r) in results.into_iter().enumerate() {
        assert!(
            r.is_err(),
            "failure-fanout test: caller {i} got Ok from a failing peer — \
             singleflight result-fanout is leaking a stale-positive across \
             waiters"
        );
    }

    // Singleflight contract: 16 concurrent failing reads => 1 peer-fetch
    // attempt (NOT 16 — that's the locality-amplification bug).
    let observed = get_part_calls.load(Ordering::SeqCst);
    assert_eq!(
        observed, 1,
        "singleflight must collapse N concurrent failing reads into 1 \
         peer-fetch — got {observed} peer-fetches for {N_CALLERS} callers \
         (each waiter is independently retrying the failing peer; that IS \
         the locality-amplification bug)"
    );

    Ok(())
}

// =====================================================================
// Test 4: leader cancelation does not kill other waiters
// =====================================================================
//
// Spawn 4 readers, immediately cancel the FIRST one (drop its handle's
// tokio::spawn future). The remaining 3 MUST still receive bytes, AND
// the peer's get_part counter MUST remain == 1 (one of the surviving
// awaiters is promoted to leader OR the original leader's task survives
// the first awaiter being dropped).
//
// RED today: no singleflight => the 3 surviving callers issue their own
// independent fetches; counter will be 3 (or 4 if the canceled one
// already started its fetch). The "leader-cancelation must not produce
// extra peer-fetches" assertion fires.

#[nativelink_test]
async fn leader_cancelation_does_not_kill_other_waiters() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
    let payload = Bytes::from(payload_vec);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

    let (proxy, get_part_calls, release_tx) =
        make_proxy_with_counting_peer(payload.clone(), digest, /*fail=*/ false);

    // Spawn 4 callers. We will cancel the FIRST one immediately.
    let mut handles = Vec::with_capacity(4);
    for _ in 0..4 {
        let proxy = proxy.clone();
        handles.push(tokio::spawn(async move {
            proxy.get_part_unchunked(digest, 0, None).await
        }));
    }

    // Wait for the callers to settle (either at peer barrier or in
    // singleflight slot). Without singleflight, all 4 reach the peer.
    // With singleflight, exactly 1 reaches the peer.
    let _ = wait_for_counter(&get_part_calls, 4, Duration::from_millis(200)).await;

    // Cancel the first caller. This drops the tokio task and (in the
    // singleflight design) drops its slot registration. If it was the
    // leader, the leader-cancel branch must promote one of the other 3
    // awaiters to leader.
    let first = handles.remove(0);
    first.abort();
    drop(first);

    // Now release the peer barrier. Surviving callers should complete
    // — either via the original (canceled) leader's bytes (singleflight
    // implementation must keep the peer-fetch alive while ≥1 awaiter
    // remains) or via promotion of a successor.
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let surviving = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.expect("leader-cancel test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "leader-cancel test: 3 surviving callers must complete within 5s \
         after the first is canceled — a hang here means leader-cancel \
         did not promote a successor (waiters wedged on a dead leader's \
         result_tx)",
    );

    // Correctness: all 3 survivors received the correct bytes.
    for (i, r) in surviving.into_iter().enumerate() {
        let bytes = r.unwrap_or_else(|e| {
            panic!("leader-cancel test: surviving caller {i} got error: {e:?}")
        });
        assert_eq!(
            bytes.as_ref(),
            payload.as_ref(),
            "leader-cancel test: surviving caller {i} received wrong bytes"
        );
    }

    // Singleflight contract: even after leader cancelation, the surviving
    // awaiters either (a) inherited the original leader's bytes (counter
    // == 1) or (b) one of them was promoted to leader and re-fetched
    // (counter == 1 if the original leader hadn't called peer.get_part
    // yet, == 2 in the worst-case promotion race). We accept ≤ 2 here:
    // the bug we are guarding against is the un-deduped 3-or-4 fetches.
    let observed = get_part_calls.load(Ordering::SeqCst);
    assert!(
        observed <= 2,
        "leader-cancel must not produce extra peer-fetches beyond the \
         leader-promotion race window — got {observed} peer-fetches for \
         3 surviving + 1 canceled caller (no singleflight wired: each \
         caller independently fetches, producing 3 or 4 fetches)"
    );

    Ok(())
}
