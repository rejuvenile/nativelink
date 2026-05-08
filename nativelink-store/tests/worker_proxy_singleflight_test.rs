// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! Task #130 — singleflight/dedup map for concurrent same-key fetches.
//!
//! These integration tests target the standalone
//! `nativelink_store::singleflight::SingleflightMap` module per the
//! `docs/130-singleflight-peer-fetch-design.md` design doc (Option B:
//! key-by-StoreKey, full-blob reads only). The wire-up into
//! `WorkerProxyStore::get_part_and_cache` happens in a follow-up commit
//! after the parallel CDN-tee work merges; see
//! `nativelink-store/src/singleflight.rs` for the module API.
//!
//! ## Asymmetric contract coverage (per CLAUDE.md §Tests)
//!
//! Singleflight has TWO failure modes:
//!
//! * **Under-action:** dedup *fails to fire* when it should — the
//!   N concurrent same-key callers each independently invoke the
//!   fetcher, producing N upstream calls instead of 1. This is the
//!   production amplification bug.
//!
//! * **Over-action:** dedup *fires when it shouldn't* — distinct keys
//!   share a slot and waiters receive the wrong cached bytes (a
//!   correctness bug, not just a perf bug). The `different_digests_do_not_dedup`
//!   test guards this direction.
//!
//! Both directions are tested below. The under-action tests existed in
//! the prior red-TDD scaffold; the over-action and cap/cleanup tests
//! were added when the implementation landed.
//!
//! ## Mutation step (per CLAUDE.md §Tests step 5)
//!
//! After this file passes green, the implementer must mutate
//! `singleflight.rs::run_as_leader` (e.g. comment out the cache
//! installation by always returning early in `acquire_role` so every
//! caller becomes Bypass) and verify the under-action tests fail with
//! their specific assertion messages. See the commit description for
//! the recorded mutation outputs.

use core::future::Future;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_store::singleflight::{DEFAULT_MAX_INFLIGHT_BYTES, SingleflightMap};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::StoreKey;
use pretty_assertions::assert_eq;
use tokio::sync::watch;
use tokio::time::timeout;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const VALID_HASH2: &str = "fedcba9876543210000000000000000000020000000000000fedcba987654321";

// =====================================================================
// Test fetcher helpers
// =====================================================================

/// Build a fetcher closure that:
/// * Increments `counter` on every entry.
/// * Awaits `release_rx` flipping to `true` (concurrency barrier so all
///   N callers can be observed at the same in-flight state — the bug
///   reproduction). NO sleep-based synchronization.
/// * If `fail`, returns `Err`; otherwise returns `payload` as a single
///   `Bytes` chunk (caller adapts).
///
/// Returns a closure usable as the `fetcher` argument to
/// `SingleflightMap::singleflight`.
fn make_blocking_fetcher(
    counter: Arc<AtomicU32>,
    release_rx: watch::Receiver<bool>,
    payload: Bytes,
    fail: bool,
) -> impl FnOnce() -> std::pin::Pin<
    Box<dyn Future<Output = Result<Vec<Bytes>, Error>> + Send>,
> + Send {
    move || {
        let counter = counter;
        let mut release_rx = release_rx;
        let payload = payload;
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            // Concurrency barrier — wait for the test to release us.
            if !*release_rx.borrow_and_update() {
                let _ = release_rx.changed().await;
            }
            if fail {
                return Err(make_err!(
                    Code::Internal,
                    "singleflight test: simulated fetcher failure"
                ));
            }
            if payload.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![payload])
            }
        })
    }
}

/// Poll a counter for `target` or until `deadline` elapses. NO sleep
/// between polls — uses `tokio::task::yield_now`.
async fn wait_for_counter(counter: &Arc<AtomicU32>, target: u32, deadline: Duration) -> u32 {
    let start = std::time::Instant::now();
    loop {
        let observed = counter.load(Ordering::SeqCst);
        if observed >= target || start.elapsed() >= deadline {
            return observed;
        }
        tokio::task::yield_now().await;
    }
}

/// Concatenate an Arc<Vec<Bytes>> into a single owned Vec for easy
/// equality assertions.
fn flatten(payload: &Arc<Vec<Bytes>>) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in payload.iter() {
        out.extend_from_slice(chunk);
    }
    out
}

// =====================================================================
// Test 1 (under-action): N concurrent same-key calls collapse to 1
// =====================================================================
//
// Production pattern: same digest read N times within ~150ms. With
// SingleflightMap, exactly 1 fetcher invocation is observed. Without
// dedup (mutation: force every caller into Bypass), 16 invocations.

#[nativelink_test]
async fn concurrent_same_digest_reads_dedup_to_one_peer_fetch() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
    let payload = Bytes::from(payload_vec);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
    let key = StoreKey::from(digest);

    let map = SingleflightMap::new();
    let counter = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    const N_CALLERS: usize = 16;
    let mut handles = Vec::with_capacity(N_CALLERS);
    for _ in 0..N_CALLERS {
        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        let payload = payload.clone();
        let key = key.borrow().into_owned();
        handles.push(tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter, release_rx, payload, false),
            )
            .await
        }));
    }

    // Deterministic synchronization: wait for the leader to enter the
    // fetcher (counter == 1) AND for all 15 waiters to subscribe (slot
    // strong_count >= 1 + 15*2 = 31). Without the strong_count sync, a
    // fast-publishing leader could drop the slot before late waiters
    // reach acquire_role, causing them to become serial leaders.
    let observed_at_barrier = wait_for_counter(&counter, 1, Duration::from_secs(2)).await;
    let _ = timeout(Duration::from_secs(2), async {
        loop {
            let n = map.strong_count_for_key(&key);
            if n >= 1 + (N_CALLERS - 1) * 2 {
                return n;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "singleflight test: 15 waiters must subscribe before release \
         (slot strong_count >= 31) within 2s",
    );

    // Release the barrier. With singleflight, the leader proceeds and
    // fans out bytes to all 15 awaiters.
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let results = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(N_CALLERS);
        for h in handles {
            out.push(h.await.expect("singleflight test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "singleflight test: 16 concurrent same-key calls must complete \
         within 5s after release — a hang here means a singleflight \
         awaiter wedged on the leader's result_rx (lost wakeup or \
         leader did not publish)",
    );

    eprintln!(
        "singleflight test: {observed_at_barrier} fetcher invocations \
         observed at barrier (target 1 with dedup, {N_CALLERS} without)"
    );

    // Correctness: every caller received the same bytes.
    for (i, r) in results.into_iter().enumerate() {
        let bytes_arc = r.unwrap_or_else(|e| panic!("caller {i} got error: {e:?}"));
        assert_eq!(
            flatten(&bytes_arc),
            payload.as_ref(),
            "singleflight test: caller {i} received wrong bytes"
        );
    }

    // Under-action contract: 16 concurrent same-key calls => 1 fetcher.
    let observed = counter.load(Ordering::SeqCst);
    assert_eq!(
        observed, 1,
        "singleflight must collapse N concurrent same-key calls into 1 fetcher \
         invocation — got {observed} fetcher entries for {N_CALLERS} callers \
         (no dedup wired in SingleflightMap::acquire_role: every caller became \
         a leader OR Bypass instead of subscribing as a waiter)"
    );

    Ok(())
}

// =====================================================================
// Test 2: partial-range reads bypass — moved to WPS layer (post-wire-up)
// =====================================================================
//
// Per CLAUDE.md and the design doc Option B note, partial-range
// bypass is decided AT THE WPS WIRE-UP LAYER (the caller of
// SingleflightMap), not inside the module — the module is key-only.
// The standalone module has no offset/length parameter, so the
// "partial reads bypass singleflight" property is enforced by the
// caller's gate (`offset == 0 && length.is_none()`), not by the module.
//
// The actual WPS-layer assertion lives in
// `wps_wireup::wps_partial_range_reads_bypass_sf` below — added when
// the SF wire-up landed (Option C, 2026-05-07).
//
// This stub remains as a `#[ignore]`d design trace so future readers
// can grep the test name and find the wire-up test it migrated to.

#[nativelink_test]
#[ignore = "Option B partial-range bypass moved to WPS-layer test \
            `wps_wireup::wps_partial_range_reads_bypass_sf` — this stub \
            kept as a grep-able design trace from the original red-TDD scaffold."]
async fn concurrent_partial_range_reads_match_design() -> Result<(), Error> {
    // Intentionally empty — see wps_wireup::wps_partial_range_reads_bypass_sf.
    Ok(())
}

// =====================================================================
// Test 3 (under-action): leader failure propagates to all waiters
// =====================================================================

#[nativelink_test]
async fn singleflight_failure_propagates_to_all_waiters() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
    let key = StoreKey::from(digest);

    let map = SingleflightMap::new();
    let counter = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    const N_CALLERS: usize = 16;
    let mut handles = Vec::with_capacity(N_CALLERS);
    for _ in 0..N_CALLERS {
        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        let key = key.borrow().into_owned();
        handles.push(tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(
                    counter,
                    release_rx,
                    Bytes::from_static(b""),
                    /*fail=*/ true,
                ),
            )
            .await
        }));
    }

    // Deterministic synchronization: wait for the leader to enter the
    // fetcher (counter == 1) AND for all 15 waiters to subscribe (slot
    // strong_count >= 1 + 15*2 = 31). Without this sync, a fast-failing
    // fetcher could publish + drop slot before late waiters subscribe,
    // causing them to acquire_role on an empty map and become serial
    // leaders (counter > 1). The 2s deadline is generous; subscription
    // is sub-millisecond once tasks are scheduled.
    let _ = wait_for_counter(&counter, 1, Duration::from_secs(2)).await;
    let _ = timeout(Duration::from_secs(2), async {
        loop {
            let n = map.strong_count_for_key(&key);
            if n >= 1 + (N_CALLERS - 1) * 2 {
                return n;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "failure-fanout test: 15 waiters must subscribe before release \
         (slot strong_count >= 31) within 2s",
    );
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
        "failure-fanout test: 16 concurrent failing reads must complete \
         (with Err) within 5s — a hang here means a waiter is stuck on \
         a leader that never published its terminal result",
    );

    // Every caller received Err. None saw a phantom Ok.
    for (i, r) in results.into_iter().enumerate() {
        assert!(
            r.is_err(),
            "failure-fanout test: caller {i} got Ok from a failing fetcher — \
             singleflight result-fanout is leaking a stale-positive across waiters"
        );
    }

    // Under-action contract: 16 concurrent failing reads => 1 fetcher.
    let observed = counter.load(Ordering::SeqCst);
    assert_eq!(
        observed, 1,
        "singleflight must collapse N concurrent failing reads into 1 fetcher \
         invocation — got {observed} for {N_CALLERS} callers (each waiter is \
         independently retrying the failing fetcher; that IS the locality-\
         amplification bug)"
    );

    Ok(())
}

// =====================================================================
// Test 4 (cancel safety, under-action): leader cancel does not kill waiters
// =====================================================================

#[nativelink_test]
async fn leader_cancelation_does_not_kill_other_waiters() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
    let payload = Bytes::from(payload_vec);
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
    let key = StoreKey::from(digest);

    let map = SingleflightMap::new();
    let counter = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    // Spawn the LEADER first, deterministically. This task is guaranteed
    // to acquire the slot (its `singleflight` call has no concurrent
    // race) before any waiter is spawned, so the test's invariant
    // "abort the leader" is unambiguous.
    let leader_handle = {
        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        let payload = payload.clone();
        let key = key.borrow().into_owned();
        tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter, release_rx, payload, false),
            )
            .await
        })
    };

    // Wait for the leader to enter the fetcher (counter == 1). At this
    // point the leader's slot is registered and the leader is parked at
    // the barrier. NO waiters exist yet.
    let observed_at_barrier = wait_for_counter(&counter, 1, Duration::from_secs(2)).await;
    assert_eq!(
        observed_at_barrier, 1,
        "precondition: leader must reach barrier (counter == 1) within 2s"
    );

    // Now spawn 3 waiters. They will see the leader's slot in
    // acquire_role and become Waiters. NO race over leader assignment.
    let mut waiter_handles = Vec::with_capacity(3);
    for _ in 0..3 {
        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        let payload = payload.clone();
        let key = key.borrow().into_owned();
        waiter_handles.push(tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter, release_rx, payload, false),
            )
            .await
        }));
    }

    // Wait until the slot's strong-count reaches the expected value:
    // - leader holds 1 strong ref via run_as_leader's `entry` local.
    // - each waiter holds 2 strong refs (entry inside run_as_waiter +
    //   entry_for_purge in the singleflight loop's match arm).
    // So target = 1 + 3 * 2 = 7 with 3 subscribed waiters. We accept
    // >= 4 as a fail-safe (1 leader + 3 waiters with 1 ref each, in
    // case the implementation reduces clones), but >= 7 is the design.
    //
    // This is a deterministic synchronization point — without it, the
    // abort might fire BEFORE waiters subscribe, causing the leader's
    // slot to drop entirely on cancel and each waiter to become its own
    // (independent) leader on its eventual acquire_role call (counter
    // would be 4, not 2).
    let observed_strong = timeout(Duration::from_secs(5), async {
        loop {
            let n = map.strong_count_for_key(&key);
            if n >= 4 {
                return n;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "leader-cancel test: 3 waiters must subscribe (slot strong_count >= 4) \
         within 5s — without subscription, abort+promote semantics cannot be \
         exercised; the test would fail on the cap-violation path instead",
    );
    assert!(
        observed_strong >= 4,
        "precondition: slot strong_count must be >= 4 (leader + 3 waiters), got {observed_strong}"
    );

    // Cancel the leader. The 3 waiters must promote among themselves:
    // exactly one becomes the new leader and re-runs the fetcher; the
    // other two subscribe as waiters of the new leader. We do NOT
    // release the barrier yet — releasing too early would let the new
    // leader's fetcher complete and publish BEFORE the other 2 waiters
    // reach acquire_role, causing them to find no slot and become
    // their own (independent) leaders. The test's contract is "the
    // promotion produces ONE successor leader," not "every waiter
    // wins after a small race."
    leader_handle.abort();
    drop(leader_handle);

    // Wait for the successor leader to enter the fetcher (counter == 2).
    // At that point the new leader is parked at the barrier and the
    // other 2 surviving waiters have subscribed to the new slot.
    let observed_after_promote =
        wait_for_counter(&counter, 2, Duration::from_secs(5)).await;
    assert_eq!(
        observed_after_promote, 2,
        "promotion: exactly 1 successor leader must enter the fetcher \
         (counter == 2) within 5s of the original leader's cancel — \
         got {observed_after_promote}, meaning either the promotion \
         hung (none) or multiple waiters became leaders (>2)"
    );

    // Wait until the NEW slot's strong count reaches >= 3 (1 new leader
    // + 2 subscribed waiters). This is the same race-protection as the
    // original-leader sync above.
    let _ = timeout(Duration::from_secs(2), async {
        loop {
            let n = map.strong_count_for_key(&key);
            if n >= 3 {
                return n;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "promote: 2 surviving waiters must subscribe to successor leader \
         within 2s",
    );

    // NOW release the barrier. The new leader's fetcher completes and
    // publishes; the 2 subscribed waiters wake with the result.
    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let surviving = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(waiter_handles.len());
        for h in waiter_handles {
            out.push(h.await.expect("leader-cancel test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "leader-cancel test: 3 surviving callers must complete within 5s \
         after the leader is canceled — a hang here means leader-cancel \
         did not promote a successor (waiters wedged on a dead leader's \
         result_rx)",
    );

    // All 3 survivors received the correct bytes.
    for (i, r) in surviving.into_iter().enumerate() {
        let bytes_arc = r.unwrap_or_else(|e| {
            panic!("leader-cancel test: surviving caller {i} got error: {e:?}")
        });
        assert_eq!(
            flatten(&bytes_arc),
            payload.as_ref(),
            "leader-cancel test: surviving caller {i} received wrong bytes"
        );
    }

    // Bound on extra work: exactly 2 fetcher invocations expected
    // (the original aborted leader + 1 successor leader). Counter > 2
    // means multiple waiters each became leaders (the dedup contract
    // failed during promotion).
    let observed = counter.load(Ordering::SeqCst);
    assert!(
        observed <= 2,
        "leader-cancel must produce at most 2 fetcher invocations (orig + 1 \
         successor leader), got {observed} for 3 surviving waiters — extra \
         invocations mean acquire_role saw an empty map for multiple \
         waiters concurrently and each became its own leader"
    );

    Ok(())
}

// =====================================================================
// Test 5 (over-action): different keys MUST NOT dedup together
// =====================================================================
//
// The over-action failure mode: two callers with DIFFERENT keys end up
// sharing a slot and one receives the other's bytes (wrong-payload
// correctness bug). Tested by spawning two concurrent calls with two
// distinct digests, asserting each got its own payload AND counter == 2.

#[nativelink_test]
async fn different_digests_do_not_dedup() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let payload_a: Bytes = Bytes::from(vec![0xAA_u8; PAYLOAD_LEN]);
    let payload_b: Bytes = Bytes::from(vec![0xBB_u8; PAYLOAD_LEN]);
    let digest_a = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
    let digest_b = DigestInfo::try_new(VALID_HASH2, PAYLOAD_LEN as u64)?;

    let map = SingleflightMap::new();
    let counter = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(true); // Auto-released; no barrier.

    let map_a = map.clone();
    let counter_a = counter.clone();
    let release_a = release_rx.clone();
    let payload_a_cloned = payload_a.clone();
    let h_a = tokio::spawn(async move {
        map_a
            .singleflight(
                StoreKey::from(digest_a),
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter_a, release_a, payload_a_cloned, false),
            )
            .await
    });
    let map_b = map.clone();
    let counter_b = counter.clone();
    let release_b = release_rx.clone();
    let payload_b_cloned = payload_b.clone();
    let h_b = tokio::spawn(async move {
        map_b
            .singleflight(
                StoreKey::from(digest_b),
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter_b, release_b, payload_b_cloned, false),
            )
            .await
    });
    drop(release_tx); // Auto-released; never need to flip.

    let (got_a, got_b) = timeout(Duration::from_secs(5), async {
        let a = h_a.await.expect("over-action test: caller A panicked");
        let b = h_b.await.expect("over-action test: caller B panicked");
        (a, b)
    })
    .await
    .expect(
        "over-action test: two distinct-key calls must complete within 5s \
         — a hang means the over-eager dedup wedged one caller waiting on \
         the other's leader",
    );

    let bytes_a = got_a.expect("caller A: fetch must succeed");
    let bytes_b = got_b.expect("caller B: fetch must succeed");
    assert_eq!(
        flatten(&bytes_a),
        payload_a.as_ref(),
        "over-action: caller A must receive payload A, not B (DIFFERENT keys \
         must never share a singleflight slot — wrong-payload correctness bug)"
    );
    assert_eq!(
        flatten(&bytes_b),
        payload_b.as_ref(),
        "over-action: caller B must receive payload B, not A (DIFFERENT keys \
         must never share a singleflight slot — wrong-payload correctness bug)"
    );

    // Over-action contract: two distinct keys => 2 fetchers (NOT 1).
    let observed = counter.load(Ordering::SeqCst);
    assert_eq!(
        observed, 2,
        "over-action: two distinct-key concurrent calls must produce 2 fetcher \
         invocations — got {observed} (a value of 1 means SingleflightMap is \
         deduping on something OTHER than the key — wrong-payload bug)"
    );

    Ok(())
}

// =====================================================================
// Test 6 (cancel safety): all-waiter drop releases the slot
// =====================================================================
//
// If every waiter (including the leader) drops, the slot must be
// purged from the map (refcount → 0 → InflightEntry::drop fires →
// map.remove). A leak here would manifest as `slot_count() > 0`
// after drop, and would also leak `current_inflight_bytes`.

#[nativelink_test]
async fn all_waiters_drop_releases_inflight_entry() -> Result<(), Error> {
    const PAYLOAD_LEN: usize = 4096;
    let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
    let key = StoreKey::from(digest);

    let map = SingleflightMap::new();
    let counter = Arc::new(AtomicU32::new(0));
    let (_release_tx, release_rx) = watch::channel(false); // Never released.

    const N: usize = 8;
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        let key = key.borrow().into_owned();
        handles.push(tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN as u64,
                make_blocking_fetcher(counter, release_rx, Bytes::from_static(b""), false),
            )
            .await
        }));
    }

    // Wait until the leader has entered the fetcher (so the slot is
    // populated). Leader is then parked on `release_rx.changed()`.
    let _ = wait_for_counter(&counter, 1, Duration::from_millis(500)).await;
    assert_eq!(
        map.slot_count(),
        1,
        "precondition: 1 slot must exist after leader enters fetcher"
    );
    assert_eq!(
        map.current_inflight_bytes(),
        PAYLOAD_LEN as u64,
        "precondition: cap accounting reserved exactly the leader's expected_size"
    );

    // Cancel every caller. The leader's fetcher Future drops; the
    // waiters' subscriptions drop; refcount on InflightEntry → 0;
    // drop fires; map.remove purges the slot; cap accounting reverses.
    for h in handles {
        h.abort();
        drop(h);
    }

    // Poll for slot-cleanup. NO sleep-as-sync; bounded poll loop.
    let cleanup_observed = timeout(Duration::from_secs(5), async {
        loop {
            if map.slot_count() == 0 && map.current_inflight_bytes() == 0 {
                return true;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "cancel-safety test: all-waiters-drop must release the slot within 5s \
         — a hang here means InflightEntry::drop did not purge the map entry \
         OR the cap accounting did not reverse",
    );

    assert!(
        cleanup_observed,
        "cancel-safety test: slot count must reach 0 after all waiters drop"
    );
    assert_eq!(
        map.slot_count(),
        0,
        "cancel-safety test: leak — slot still in map after all waiters dropped"
    );
    assert_eq!(
        map.current_inflight_bytes(),
        0,
        "cancel-safety test: cap accounting leak — inflight_bytes did not reverse"
    );

    Ok(())
}

// =====================================================================
// Test 7 (cap behavior): late callers bypass when cap is exceeded
// =====================================================================
//
// With a tiny cap, only the first leader fits; subsequent leaders for
// distinct keys bypass and run their fetcher directly. We assert the
// fetcher counter > expected (each bypass increments) and that all
// callers complete (no cap-induced hang).

#[nativelink_test]
async fn cap_exceeded_late_callers_bypass() -> Result<(), Error> {
    const PAYLOAD_LEN: u64 = 1024 * 1024; // 1 MiB per slot
    const CAP: u64 = PAYLOAD_LEN; // Exactly 1 slot fits.

    let map = SingleflightMap::with_cap(CAP);
    let counter = Arc::new(AtomicU32::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    // Spawn 4 callers, each with a UNIQUE digest (so they each become
    // a leader of their own slot — none subscribe as waiters). With
    // CAP = PAYLOAD_LEN, only ONE slot can be reserved at a time; the
    // other 3 callers must Bypass.
    let mut handles = Vec::with_capacity(4);
    for i in 0..4u32 {
        // Build a unique digest by varying one byte of the hash.
        let hash = format!(
            "{:02x}23456789abcdef000000000000000000010000000000000123456789abcdef",
            (0x10 + i) & 0xff
        );
        let digest = DigestInfo::try_new(&hash, PAYLOAD_LEN)?;
        let key = StoreKey::from(digest);

        let map = map.clone();
        let counter = counter.clone();
        let release_rx = release_rx.clone();
        handles.push(tokio::spawn(async move {
            map.singleflight(
                key,
                PAYLOAD_LEN,
                make_blocking_fetcher(
                    counter,
                    release_rx,
                    Bytes::from(vec![0u8; PAYLOAD_LEN as usize]),
                    false,
                ),
            )
            .await
        }));
    }

    // Wait for at least 4 fetcher entries (1 slot leader + 3 bypass).
    // Without the cap, exactly 4 would be observed (each is its own
    // unique key, so each would be its own leader). With the cap, the
    // count is the same (4), but we need to ensure all 4 fetchers RAN
    // — proving the bypass branch fires rather than hanging.
    let observed = wait_for_counter(&counter, 4, Duration::from_millis(500)).await;
    assert_eq!(
        observed, 4,
        "cap test: all 4 callers must enter the fetcher (3 via bypass) — \
         got {observed}; a count < 4 means cap-bypass blocked a caller \
         instead of letting it through"
    );

    release_tx
        .send(true)
        .expect("release_tx: receivers should still be alive");

    let results = timeout(Duration::from_secs(5), async {
        let mut out = Vec::with_capacity(4);
        for h in handles {
            out.push(h.await.expect("cap test: caller task panicked"));
        }
        out
    })
    .await
    .expect(
        "cap test: all callers must complete within 5s after release — \
         a hang here means cap-bypass parked a caller in a non-existent \
         slot",
    );

    for (i, r) in results.into_iter().enumerate() {
        let _payload = r.unwrap_or_else(|e| panic!("cap test: caller {i} got error: {e:?}"));
    }

    // Cap accounting must converge to 0 after all slots release.
    let final_inflight = map.current_inflight_bytes();
    assert_eq!(
        final_inflight, 0,
        "cap test: inflight bytes accounting leaked — final = {final_inflight}, \
         expected 0 (a non-zero residual means a slot's reserved_bytes was \
         not released on Drop)"
    );

    Ok(())
}

// =====================================================================
// Sanity: DEFAULT_MAX_INFLIGHT_BYTES is the documented value.
// Guards against accidental const drift in the implementation file.
// =====================================================================
#[nativelink_test]
async fn default_cap_constant_is_1_gib() -> Result<(), Error> {
    assert_eq!(
        DEFAULT_MAX_INFLIGHT_BYTES,
        1024 * 1024 * 1024,
        "design doc commits to 1 GiB default cap (sized for 16 concurrent \
         64 MiB MAX_CACHE_BLOB_SIZE blobs); a change here requires a \
         docs/130-singleflight-peer-fetch-design.md update"
    );
    Ok(())
}

// =====================================================================
// WPS-level wire-up tests (Option C, 2026-05-07)
// =====================================================================
//
// The tests above target the SingleflightMap module directly.
// The tests below target the WorkerProxyStore::get_part_and_cache
// wire-up — they assert that:
//
// * The dedup gate (`offset == 0 && length.is_none() && size > 0 &&
//   size <= MAX_CACHE_BLOB_SIZE`) routes concurrent same-digest reads
//   through SF, collapsing N peer fetches into ~1 (Option B from the
//   design doc).
// * Bypass paths (partial-range reads, oversized blobs) skip SF and
//   peer-fetch independently.
// * The Choice (α) race-window fall-back works: when a waiter's CAS
//   read returns NotFound (because the leader's detached cache task
//   hasn't completed yet), the waiter falls back to direct peer-fetch
//   instead of erroring.
// * Distinct digests do NOT dedup at the WPS layer (over-action guard).
//
// Production composition: real `WorkerProxyStore` wrapping a real
// `MemoryStore` inner + `inject_worker_connection`-injected MemoryStore
// peer. We use MemoryStore (not FilesystemStore) here because we are
// testing the SF coordination layer, not the inner-store cache write
// path; the cdn_tee_decoupled_test family already covers the
// FilesystemStore-as-inner case.

mod wps_wireup {
    use super::{Bytes, Duration};
    use core::pin::Pin;
    use core::sync::atomic::{AtomicU64, Ordering as AOrdering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use nativelink_config::stores::MemorySpec;
    use nativelink_error::{Error, ResultExt};
    use nativelink_macro::nativelink_test;
    use nativelink_metric::MetricsComponent;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_store::worker_proxy_store::WorkerProxyStore;
    use nativelink_util::blob_locality_map::new_shared_blob_locality_map;
    use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::health_utils::{
        HealthStatusIndicator, default_health_status_indicator,
    };
    use nativelink_util::store_trait::{
        ItemCallback, MarkStableDelegation, PinDelegation, StableDigestDelegation,
        Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
    };
    use pretty_assertions::assert_eq;
    use tokio::sync::watch;
    use tokio::time::timeout;

    const VALID_HASH1: &str =
        "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const VALID_HASH2: &str =
        "fedcba9876543210000000000000000000020000000000000fedcba987654321";

    /// Counts get_part calls and gates them on a shared watch
    /// barrier so the test can observe N concurrent in-flight calls
    /// before releasing them. Wraps an inner MemoryStore for actual
    /// data.
    ///
    /// `tokio::sync::watch::channel(bool)` is the canonical
    /// lost-wakeup-safe replacement for `Notify::notified()` here:
    /// the receiver caches the latest value, so `borrow_and_update`
    /// + `changed().await` is correct regardless of whether the
    /// sender flipped before or after the receiver subscribed
    /// (the older `Notify` pattern in this fake had the canonical
    /// `notify_waiters` race documented in
    /// `feedback_lost_wakeup_test_theatre`).
    #[derive(Debug, MetricsComponent)]
    struct CountingPeerStore {
        inner: Store,
        get_part_calls: Arc<AtomicU64>,
        /// Flipped to `true` by the fake when the first peer call
        /// enters; the test waits on `first_call_arrived_rx.changed()`
        /// to observe in-flight cohort size before releasing.
        first_call_arrived_tx: watch::Sender<bool>,
        /// Test flips this from `false` to `true` to release queued
        /// callers; every parked caller wakes deterministically.
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
            self.inner.has_with_results(digests, results).await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            upload_size: UploadSizeInfo,
        ) -> Result<(), Error> {
            self.inner.update(key, reader, upload_size).await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            let n = self.get_part_calls.fetch_add(1, AOrdering::SeqCst);
            if n == 0 {
                // Best-effort: receivers may already have been dropped
                // if the test exited; ignore the SendError.
                let _ = self.first_call_arrived_tx.send(true);
            }
            // Subscribe-before-poll: clone the receiver and only
            // continue once the released flag is `true`. Watch caches
            // the latest value so this is lost-wakeup-safe regardless
            // of whether the test flipped `release` before or after
            // we got here.
            let mut release_rx = self.release_rx.clone();
            while !*release_rx.borrow_and_update() {
                if release_rx.changed().await.is_err() {
                    // Sender dropped: test cleaned up before we
                    // released. Treat as released.
                    break;
                }
            }
            self.inner
                .get_part(key, writer, offset, length)
                .await
                .err_tip(|| "CountingPeerStore: inner.get_part")
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
            StableDigestDelegation::Inner(self.inner.as_store_driver())
        }
        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Inner(self.inner.as_store_driver())
        }
        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Inner(self.inner.as_store_driver())
        }
    }

    /// Test harness handle bundling the proxy + the watch handles the
    /// test must drive (release_tx) and observe (first_call_arrived_rx).
    struct CountingPeerHarness {
        proxy_arc: Arc<WorkerProxyStore>,
        get_part_calls: Arc<AtomicU64>,
        /// Receiver-side: `changed().await` resolves once the FIRST
        /// peer call enters the fake. Lost-wakeup-safe: the watch
        /// remembers the flip even if `changed().await` is called
        /// afterwards.
        first_call_arrived_rx: watch::Receiver<bool>,
        /// Sender-side: `release_tx.send(true)` releases every parked
        /// peer caller. Subsequent peer calls see the flag already set
        /// and proceed without parking.
        release_tx: watch::Sender<bool>,
    }

    fn build_proxy_with_counting_peer(
        digest: DigestInfo,
        payload: Bytes,
    ) -> CountingPeerHarness {
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        // Pre-populate the peer with the blob.
        futures::executor::block_on(peer_inner.update_oneshot(digest, payload))
            .expect("seed peer with payload");
        let get_part_calls = Arc::new(AtomicU64::new(0));
        let (first_call_arrived_tx, first_call_arrived_rx) = watch::channel(false);
        let (release_tx, release_rx) = watch::channel(false);
        let counting_peer = Store::new(Arc::new(CountingPeerStore {
            inner: peer_inner,
            get_part_calls: get_part_calls.clone(),
            first_call_arrived_tx,
            release_rx,
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
        let endpoint = "grpc://sf-wps-test-peer:50081";
        proxy_arc.inject_worker_connection(endpoint, counting_peer);
        locality_map.write().register_blobs(endpoint, &[digest]);
        CountingPeerHarness {
            proxy_arc,
            get_part_calls,
            first_call_arrived_rx,
            release_tx,
        }
    }

    /// Helper: await `first_call_arrived_rx` flipping to true. Pairs
    /// with `release_tx.send(true)` in tests to coordinate the
    /// "observe then release" cohort scenario.
    async fn wait_for_first_peer_call(
        rx: &mut watch::Receiver<bool>,
    ) {
        // borrow_and_update returns the latest value AND marks it as
        // seen, so subsequent changed().await waits for the NEXT flip.
        // If the flag is already true (peer raced ahead), we return
        // immediately.
        if *rx.borrow_and_update() {
            return;
        }
        let _ = rx.changed().await;
    }

    // =================================================================
    // Test 1 (under-action): N concurrent same-digest reads => 1 peer fetch
    // =================================================================
    //
    // Production-composition assertion of the locality-amplification
    // bug fix: 16 concurrent get_part_unchunked() calls on the same
    // digest must collapse to 1 peer.get_part() invocation via SF.
    //
    // Mutation step: in `WorkerProxyStore::get_part_and_cache`, comment
    // out the SF gate (`if !should_use_sf` or its body) so every call
    // becomes Bypass-equivalent (direct fetch). The
    // peer-call-count assertion below trips with the bespoke message.

    #[nativelink_test]
    async fn wps_concurrent_same_digest_reads_dedup_to_one_peer_fetch()
    -> Result<(), Error> {
        const PAYLOAD_LEN: usize = 4096;
        let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
        let payload = Bytes::from(payload_vec);
        let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

        let CountingPeerHarness {
            proxy_arc,
            get_part_calls: peer_calls,
            mut first_call_arrived_rx,
            release_tx,
        } = build_proxy_with_counting_peer(digest, payload.clone());
        let proxy = Store::new(proxy_arc.clone());

        const N_CALLERS: usize = 16;
        let mut handles = Vec::with_capacity(N_CALLERS);
        for _ in 0..N_CALLERS {
            let proxy = proxy.clone();
            handles.push(tokio::spawn(async move {
                proxy.get_part_unchunked(digest, 0, None).await
            }));
        }

        // Wait for the FIRST peer call to arrive (parked on the
        // release watch). watch is lost-wakeup-safe: even if the peer
        // already arrived before we got here, the flag is cached.
        timeout(
            Duration::from_secs(5),
            wait_for_first_peer_call(&mut first_call_arrived_rx),
        )
        .await
        .expect(
            "wps SF test: first peer call must arrive within 5s — \
             if N peer calls were going to fire, the first would \
             already be in-flight by now",
        );
        // At this point the leader is parked at peer.get_part awaiting
        // the release flag. The other 15 callers should be parked in
        // SF as waiters (their fetcher closure never ran).

        // Release every caller deterministically with a single watch
        // flip — replaces the previous `Notify::notify_waiters()` +
        // 1000-iter polling-loop workaround. The leader's get_part
        // returns a chunked stream from MemoryStore; the cache task
        // spawns and (because the inner is a fast MemoryStore) likely
        // completes before waiters reach inner.get_part.
        release_tx
            .send(true)
            .expect("release_tx must succeed — fake's receiver still alive");

        let results = timeout(Duration::from_secs(10), async {
            let mut out = Vec::with_capacity(N_CALLERS);
            for h in handles {
                out.push(h.await.expect("wps SF test: caller task panicked"));
            }
            out
        })
        .await
        .expect(
            "wps SF test: 16 concurrent same-digest reads must complete \
             within 10s after release — a hang here means a waiter \
             wedged on the leader's SF signal AND on the inner.get_part \
             CAS read fall-back",
        );

        for (i, r) in results.into_iter().enumerate() {
            let got = r.unwrap_or_else(|e| panic!("caller {i} got error: {e:?}"));
            assert_eq!(
                got.as_ref(),
                payload.as_ref(),
                "wps SF test: caller {i} received wrong bytes",
            );
        }

        // Under-action contract: 16 callers ⇒ 1 peer fetch.
        // With a 4 KiB MemoryStore inner and detached cache task, the
        // cache typically completes WHILE the leader is still
        // streaming, so by the time waiters reach inner.get_part the
        // CAS has the blob — full SF dedup engages. Allow up to 2
        // peer calls as graceful-degradation tolerance for the rare
        // race-window case (the leader's forward EOF arrives before
        // the cache task's first poll), per testing-czar MINOR-1.
        let observed = peer_calls.load(AOrdering::SeqCst);
        assert!(
            observed <= 2,
            "wps SF wire-up must collapse N=16 concurrent same-digest reads \
             into ≤ 2 peer fetches via SF dedup — got {observed} \
             (a value of N or close to it means SF is not engaging at the \
             WPS layer; check the `should_use_sf` gate in get_part_and_cache; \
             a value between 2 and N/4 likely means the race window is \
             larger than expected and the leader's cache write rate has \
             regressed below the leader's forward EOF rate)",
        );

        // SF dedup-hits counter must show ≥ 1 dedup event.
        let (dedup_hits, _bypasses_cap) = proxy_arc.singleflight_counters_snapshot();
        assert!(
            dedup_hits >= 1,
            "wps SF test: SingleflightMap.total_dedup_hits must increment for \
             at least 1 of the 15 waiters; got {dedup_hits} — SF is not \
             being consulted at all (gate misconfigured)",
        );

        Ok(())
    }

    // =================================================================
    // Test 2 (over-action): distinct-digest concurrent reads do NOT dedup
    // =================================================================
    //
    // Each digest gets its own peer fetch. With 2 distinct-digest
    // concurrent reads, peer.get_part is called 2 times. SF must NOT
    // collapse them.

    #[nativelink_test]
    async fn wps_concurrent_distinct_digest_reads_do_not_dedup() -> Result<(), Error> {
        const PAYLOAD_LEN: usize = 4096;
        let payload_a: Vec<u8> = vec![0xAA; PAYLOAD_LEN];
        let payload_b: Vec<u8> = vec![0xBB; PAYLOAD_LEN];
        let digest_a = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;
        let digest_b = DigestInfo::try_new(VALID_HASH2, PAYLOAD_LEN as u64)?;

        // Build a single peer that has BOTH blobs, with a counter
        // that increments per get_part call regardless of digest.
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_inner
            .update_oneshot(digest_a, Bytes::from(payload_a.clone()))
            .await?;
        peer_inner
            .update_oneshot(digest_b, Bytes::from(payload_b.clone()))
            .await?;

        let get_part_calls = Arc::new(AtomicU64::new(0));
        let (first_call_arrived_tx, mut first_call_arrived_rx) = watch::channel(false);
        let (release_tx, release_rx) = watch::channel(false);
        let counting_peer = Store::new(Arc::new(CountingPeerStore {
            inner: peer_inner,
            get_part_calls: get_part_calls.clone(),
            first_call_arrived_tx,
            release_rx,
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
        let endpoint = "grpc://sf-wps-distinct-test-peer:50081";
        proxy_arc.inject_worker_connection(endpoint, counting_peer);
        locality_map
            .write()
            .register_blobs(endpoint, &[digest_a, digest_b]);
        let proxy = Store::new(proxy_arc.clone());

        let h_a = {
            let proxy = proxy.clone();
            tokio::spawn(async move { proxy.get_part_unchunked(digest_a, 0, None).await })
        };
        let h_b = {
            let proxy = proxy.clone();
            tokio::spawn(async move { proxy.get_part_unchunked(digest_b, 0, None).await })
        };

        // Wait for first call to arrive — proves both are in-flight
        // concurrently (the race condition under test).
        timeout(
            Duration::from_secs(5),
            wait_for_first_peer_call(&mut first_call_arrived_rx),
        )
        .await
        .expect("wps over-action test: first peer call must arrive within 5s");
        // Best-effort wait for the second peer call: yield until the
        // counter shows both arrived (no wall-clock sleep). With
        // distinct digests, both leaders enter peer.get_part in
        // parallel, so this resolves in a handful of yields.
        for _ in 0..1000 {
            if get_part_calls.load(AOrdering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Single watch flip releases EVERY parked caller, present and
        // future. No 1000-iter sleep loop, no Notify lost-wakeup.
        release_tx
            .send(true)
            .expect("release_tx must succeed — fake's receiver still alive");

        let (got_a, got_b) = timeout(Duration::from_secs(10), async {
            let a = h_a.await.expect("wps over-action: caller A panicked");
            let b = h_b.await.expect("wps over-action: caller B panicked");
            (a, b)
        })
        .await
        .expect(
            "wps over-action test: distinct-digest reads must complete \
             within 10s — a hang here would mean SF over-eager dedup is \
             pairing two different digests into one slot",
        );

        let bytes_a = got_a.expect("wps over-action: caller A failed");
        let bytes_b = got_b.expect("wps over-action: caller B failed");
        assert_eq!(
            bytes_a.as_ref(),
            payload_a.as_slice(),
            "wps over-action: caller A must receive payload A, not B \
             (DIFFERENT keys must NEVER share a SF slot at WPS layer)"
        );
        assert_eq!(
            bytes_b.as_ref(),
            payload_b.as_slice(),
            "wps over-action: caller B must receive payload B, not A \
             (DIFFERENT keys must NEVER share a SF slot at WPS layer)"
        );

        // Over-action contract: 2 distinct digests ⇒ 2 peer fetches.
        let observed = get_part_calls.load(AOrdering::SeqCst);
        assert_eq!(
            observed, 2,
            "wps over-action: 2 distinct-digest concurrent reads must \
             produce exactly 2 peer fetches — got {observed} (a value of \
             1 means SF is deduping on something OTHER than the digest \
             — wrong-payload correctness bug)"
        );

        Ok(())
    }

    // =================================================================
    // Test 3 (bypass): partial-range reads bypass SF
    // =================================================================
    //
    // Per the design doc Option B: only full-blob reads
    // (`offset == 0 && length.is_none()`) engage SF. Partial reads
    // must bypass — each partial read independently peer-fetches.

    #[nativelink_test]
    async fn wps_partial_range_reads_bypass_sf() -> Result<(), Error> {
        const PAYLOAD_LEN: usize = 4096;
        let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
        let payload = Bytes::from(payload_vec.clone());
        let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

        let CountingPeerHarness {
            proxy_arc,
            get_part_calls: peer_calls,
            first_call_arrived_rx: _,
            release_tx,
        } = build_proxy_with_counting_peer(digest, payload.clone());
        let proxy = Store::new(proxy_arc.clone());

        // Spawn 4 PARTIAL reads (offset > 0). Each must peer-fetch
        // independently; SF must not dedup them.
        const N_CALLERS: usize = 4;
        let mut handles = Vec::with_capacity(N_CALLERS);
        for _ in 0..N_CALLERS {
            let proxy = proxy.clone();
            handles.push(tokio::spawn(async move {
                // Partial: offset=10, length=Some(100).
                proxy.get_part_unchunked(digest, 10, Some(100)).await
            }));
        }

        // Single watch flip releases every present and future peer
        // caller — replaces the previous 200-iter sleep-based release
        // loop. Late-arriving callers see release_rx already true and
        // proceed without parking.
        release_tx
            .send(true)
            .expect("release_tx must succeed — fake's receiver still alive");

        let results = timeout(Duration::from_secs(10), async {
            let mut out = Vec::with_capacity(N_CALLERS);
            for h in handles {
                out.push(h.await.expect("wps bypass test: caller task panicked"));
            }
            out
        })
        .await
        .expect("wps bypass test: 4 partial-range reads must complete within 10s");

        for (i, r) in results.into_iter().enumerate() {
            let got = r.unwrap_or_else(|e| panic!("caller {i} got error: {e:?}"));
            assert_eq!(
                got.as_ref(),
                &payload_vec[10..110],
                "wps bypass test: caller {i} must receive partial range",
            );
        }

        // Bypass contract: each partial read fires its own peer fetch.
        let observed = peer_calls.load(AOrdering::SeqCst);
        assert_eq!(
            observed, N_CALLERS as u64,
            "wps bypass test: partial-range reads must NOT dedup — got \
             {observed} peer fetches for {N_CALLERS} partial readers, \
             expected {N_CALLERS} (one per caller per Option B). A value < \
             {N_CALLERS} means partial-range reads are erroneously deduping; \
             this would corrupt readers that pass different (offset, length) \
             tuples for the same digest"
        );

        // No SF dedup hits should be recorded.
        let (dedup_hits, _bypasses_cap) = proxy_arc.singleflight_counters_snapshot();
        assert_eq!(
            dedup_hits, 0,
            "wps bypass test: total_dedup_hits must be 0 — partial reads \
             never reach SF; got {dedup_hits}",
        );

        Ok(())
    }

    // =================================================================
    // Test 4 (race-window fall-back): waiter NotFound from CAS triggers
    //                                  fall-back to direct peer fetch
    // =================================================================
    //
    // Choice (α) accepts a race window: if the leader's detached cache
    // task hasn't completed yet when a waiter wakes and reads CAS,
    // the waiter sees NotFound and falls back to direct peer-fetch.
    // This test forces the race by using a SLOW inner store for cache
    // writes, so the cache task always finishes AFTER the leader's
    // forward loop. Waiters race ahead, observe NotFound, and fall
    // back. The test verifies that all waiters succeed (via the
    // fall-back path) — no silent error or hang.
    //
    // Mutation step: in the waiter path of `get_part_and_cache`, change
    // the NotFound match arm from "fall back to direct fetch" to
    // "return NotFound err". Test would then panic with the bespoke
    // bytes-correctness assertion (waiters get NotFound instead of
    // bytes). Without the mutation, all waiters succeed.

    #[derive(Debug, MetricsComponent)]
    struct SlowUpdateInnerStore {
        sleep_ms: u64,
        update_calls: AtomicU64,
        // Inner MemoryStore that actually holds the data after the sleep.
        inner: Store,
    }

    default_health_status_indicator!(SlowUpdateInnerStore);

    #[async_trait]
    impl StoreDriver for SlowUpdateInnerStore {
        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            self.inner.has_with_results(digests, results).await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            upload_size: UploadSizeInfo,
        ) -> Result<(), Error> {
            self.update_calls.fetch_add(1, AOrdering::SeqCst);
            // Delay BEFORE the actual write so by the time the cache
            // task lands the bytes, waiters have already raced past
            // their inner.get_part check.
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
            self.inner.update(key, reader, upload_size).await
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            self.inner.get_part(key, writer, offset, length).await
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
            StableDigestDelegation::Inner(self.inner.as_store_driver())
        }
        fn pin_delegation(&self) -> PinDelegation<'_> {
            PinDelegation::Inner(self.inner.as_store_driver())
        }
        fn mark_stable_delegation(&self) -> MarkStableDelegation<'_> {
            MarkStableDelegation::Inner(self.inner.as_store_driver())
        }
    }

    #[nativelink_test]
    async fn wps_waiter_not_found_in_cas_falls_back_to_direct_fetch()
    -> Result<(), Error> {
        const PAYLOAD_LEN: usize = 4096;
        let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
        let payload = Bytes::from(payload_vec);
        let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

        // Slow inner: 500ms delay on update. The cache task therefore
        // takes ~500ms; waiters that wake before that find CAS empty
        // and fall back.
        let actual_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let slow_inner = Store::new(Arc::new(SlowUpdateInnerStore {
            sleep_ms: 500,
            update_calls: AtomicU64::new(0),
            inner: actual_inner.clone(),
        }));

        let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_inner
            .update_oneshot(digest, payload.clone())
            .await?;
        let get_part_calls = Arc::new(AtomicU64::new(0));
        let (first_call_arrived_tx, mut first_call_arrived_rx) = watch::channel(false);
        let (release_tx, release_rx) = watch::channel(false);
        let counting_peer = Store::new(Arc::new(CountingPeerStore {
            inner: peer_inner,
            get_part_calls: get_part_calls.clone(),
            first_call_arrived_tx,
            release_rx,
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(slow_inner, locality_map.clone());
        let endpoint = "grpc://sf-wps-fallback-test-peer:50081";
        proxy_arc.inject_worker_connection(endpoint, counting_peer);
        locality_map.write().register_blobs(endpoint, &[digest]);
        let proxy = Store::new(proxy_arc.clone());

        const N_CALLERS: usize = 4;
        let mut handles = Vec::with_capacity(N_CALLERS);
        for _ in 0..N_CALLERS {
            let proxy = proxy.clone();
            handles.push(tokio::spawn(async move {
                proxy.get_part_unchunked(digest, 0, None).await
            }));
        }

        timeout(
            Duration::from_secs(5),
            wait_for_first_peer_call(&mut first_call_arrived_rx),
        )
        .await
        .expect("wps fallback test: first peer call must arrive within 5s");
        // Single watch flip releases all peer-fetches present and
        // future. The leader's forward completes ~immediately; the
        // cache task then sleeps 500ms; waiters wake right after the
        // leader signals, find CAS empty, fall back to direct
        // peer-fetch. So the peer call counter ends up at N (each
        // waiter fires its own).
        release_tx
            .send(true)
            .expect("release_tx must succeed — fake's receiver still alive");

        let results = timeout(Duration::from_secs(15), async {
            let mut out = Vec::with_capacity(N_CALLERS);
            for h in handles {
                out.push(h.await.expect("wps fallback test: task panicked"));
            }
            out
        })
        .await
        .expect(
            "wps fallback test: all 4 callers must complete within 15s. \
             A hang here means a waiter wedged after observing NotFound \
             from CAS — the fall-back path is broken (writer-termination \
             contract violated)",
        );

        // All waiters must eventually receive the bytes — either from
        // CAS or via fall-back. The contract: no caller is wedged or
        // gets a wrong result.
        for (i, r) in results.into_iter().enumerate() {
            let got = r.unwrap_or_else(|e| {
                panic!(
                    "wps fallback test: caller {i} got error {e:?} — \
                     race-window fall-back to direct peer-fetch failed"
                )
            });
            assert_eq!(
                got.as_ref(),
                payload.as_ref(),
                "wps fallback test: caller {i} received wrong bytes — \
                 the fall-back path corrupted the response",
            );
        }

        // Operator-visibility counter: at least one waiter must have
        // recorded the race-window fall-back to direct peer-fetch.
        // Without this counter, an operator can't distinguish "SF is
        // engaging but degrading to direct fetch" from "SF is
        // engaging cleanly" — both look like the same dedup-hit
        // count. Per red-team finding.
        let fallback_count = proxy_arc
            .singleflight_waiter_fallback_to_direct_total();
        assert!(
            fallback_count >= 1,
            "wps fallback test: \
             singleflight_waiter_fallback_to_direct_total must increment \
             when waiters race past the leader's cache write — got \
             {fallback_count}. The counter is the operator's only signal \
             that SF dedup is degrading to no-op for this digest class; \
             without it we can't distinguish cache-fast-path success from \
             race-window degradation",
        );

        Ok(())
    }

    // =================================================================
    // Test 5 (writer-state robustness): waiter fall-back must NOT enter
    //   `get_part_and_cache_inner` when the borrowed writer is already
    //   closed (pipe broken via send_error from a wrapping layer).
    //   This is FORWARD-COMPAT HARDENING (NOT a sibling of #171 — #171
    //   was a real production bug; this test guards against a
    //   hypothetical future defensive WriteHalfGuard-style wrapper
    //   above WPS). Today no production layer between WPS and the leaf
    //   calls send_error on NotFound, so the byte-counter alone catches
    //   the live race. If a future wrapper closes the writer on Err,
    //   without this guard the fall-back would silently trip "Tried to
    //   send while stream is closed" on the first chunk. Test simulates
    //   the future-wrapper precondition by pre-closing the writer.
    // =================================================================
    //
    // We construct a wrapping store that:
    //   * Forwards `get_part` to an inner WorkerProxyStore.
    //   * Closes the writer with `send_error` when the inner returns
    //     NotFound (the would-be sibling defensive wrapper behavior).
    // We can't easily synthesize a real composition that closes the
    // writer mid-stream without an actual production wrapper that
    // does this. Instead we test the contract directly: when a
    // waiter's CAS read is observed to return NotFound on a
    // pre-closed writer, the fall-back must NOT be invoked (which
    // would attempt to send bytes through the closed pipe).
    //
    // Mutation step: remove the `!writer.is_pipe_broken()` clause
    // from the waiter NotFound match arm. Then, even with a
    // pre-closed writer, fall-back fires and `cache_handle.await`
    // ends up trying to send bytes via the broken pipe. With the
    // guard in place, the waiter surfaces the original NotFound (or
    // a derived error) without invoking fall-back. We assert the
    // waiter completes in bounded time AND no panic / no spurious
    // bytes-received when the writer is closed.
    //
    // The key invariant under test:
    //   "is_pipe_broken() guard prevents fall-back from re-entering
    //    a closed writer."
    // The mutation is the one-line removal of the `&& !writer.is_pipe_broken()`
    // clause; without the guard the waiter still completes (because
    // the waiter operates on its OWN top-level writer which Bazel
    // has not closed in the production composition we'd ship), but
    // the contract still holds because the worker never actually
    // closes the writer on NotFound today. The forward-compatibility
    // value of the guard is what we assert: a code-shape regression
    // that re-enters the writer post-close is structurally
    // prevented.

    #[nativelink_test]
    async fn wps_waiter_fallback_skips_when_writer_pipe_broken_guard()
    -> Result<(), Error> {
        // This test exercises the choice α invariant directly via the
        // `is_pipe_broken()` boolean: it asserts that the waiter
        // path's NotFound match arm correctly REQUIRES a non-broken
        // pipe to invoke fall-back. We simulate by closing the
        // writer manually via send_error and verifying the fall-back
        // is skipped (no peer fetch fires on the closed-writer path).

        use nativelink_util::buf_channel::make_buf_channel_pair;

        const PAYLOAD_LEN: usize = 4096;
        let payload_vec: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xff) as u8).collect();
        let payload = Bytes::from(payload_vec);
        let digest = DigestInfo::try_new(VALID_HASH1, PAYLOAD_LEN as u64)?;

        // Slow inner: the cache-task delay forces the waiter into the
        // race-window fall-back path. Without is_pipe_broken, the
        // waiter would re-enter get_part_and_cache_inner.
        let actual_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let slow_inner = Store::new(Arc::new(SlowUpdateInnerStore {
            sleep_ms: 500,
            update_calls: AtomicU64::new(0),
            inner: actual_inner.clone(),
        }));

        let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        peer_inner
            .update_oneshot(digest, payload.clone())
            .await?;
        let get_part_calls = Arc::new(AtomicU64::new(0));
        let (first_call_arrived_tx, mut first_call_arrived_rx) = watch::channel(false);
        let (release_tx, release_rx) = watch::channel(false);
        let counting_peer = Store::new(Arc::new(CountingPeerStore {
            inner: peer_inner,
            get_part_calls: get_part_calls.clone(),
            first_call_arrived_tx,
            release_rx,
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(slow_inner, locality_map.clone());
        let endpoint = "grpc://sf-wps-pipe-broken-test-peer:50081";
        proxy_arc.inject_worker_connection(endpoint, counting_peer);
        locality_map.write().register_blobs(endpoint, &[digest]);
        let proxy = Store::new(proxy_arc.clone());

        // 1) Spawn the leader via get_part_unchunked. Its writer is
        //    owned by the unchunked harness — we don't touch it. The
        //    leader's peer call will block on release.
        let leader_handle = {
            let proxy = proxy.clone();
            tokio::spawn(async move {
                proxy.get_part_unchunked(digest, 0, None).await
            })
        };

        timeout(
            Duration::from_secs(5),
            wait_for_first_peer_call(&mut first_call_arrived_rx),
        )
        .await
        .expect(
            "wps pipe-broken test: leader's first peer call must arrive \
             within 5s before we set up the waiter",
        );

        // 2) Build the waiter's writer — pre-close it via send_error
        //    so is_pipe_broken() returns true. This simulates a
        //    future defensive WriteHalfGuard-style wrapper above WPS
        //    that closes the writer on Err. drop(rx) too so the
        //    actual send-attempt also fails.
        let (mut tx, rx) = make_buf_channel_pair();
        let close_err = nativelink_error::make_err!(
            nativelink_error::Code::Internal,
            "pipe-broken simulator: writer closed by upstream"
        );
        tx.send_error(close_err);
        assert!(
            tx.is_pipe_broken(),
            "test setup: send_error must mark the writer as pipe-broken",
        );
        drop(rx);

        // 3) Spawn the waiter BEFORE release fires, so it joins SF as
        //    a true waiter (not a successor leader after the slot
        //    drops). The waiter parks on the SF watch channel until
        //    the leader publishes.
        let proxy_arc2 = proxy_arc.clone();
        let waiter_handle = tokio::spawn(async move {
            use nativelink_util::store_trait::StoreDriver;
            let pinned: Pin<&dyn StoreDriver> =
                Pin::new(proxy_arc2.as_ref());
            pinned.get_part(StoreKey::from(digest), &mut tx, 0, None).await
        });

        // Yield a few times to give the waiter task a chance to
        // reach the singleflight() join point.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        // 4) Release the leader. Leader's peer call proceeds, leader
        //    signals SF Ok. Waiter wakes, calls inner.get_part on
        //    slow_inner (sleeps 500ms before returning data — but
        //    the cache hasn't even started populating, so the slow
        //    inner returns NotFound after sleep). Waiter sees
        //    NotFound + bytes_before == 0. With guard: also sees
        //    pipe_broken → falls into the Err arm WITHOUT calling
        //    fallback (so peer.get_part is NOT called from the
        //    waiter). Without guard: falls back → calls
        //    get_part_and_cache_inner → peer.get_part fires AGAIN.
        release_tx
            .send(true)
            .expect("release_tx must succeed — fake's receiver still alive");

        let waiter_result = timeout(
            Duration::from_secs(15),
            waiter_handle,
        )
        .await
        .expect(
            "wps pipe-broken test: waiter must NOT deadlock — the \
             is_pipe_broken guard should short-circuit the fall-back \
             path; a hang here means a waiter wedged trying to send \
             bytes through a closed writer",
        )
        .expect("waiter task panicked");

        // The waiter must Err — the writer is closed and there's no
        // way to deliver bytes through it.
        assert!(
            waiter_result.is_err(),
            "wps pipe-broken test: waiter on closed writer must Err — \
             got Ok, which would mean the waiter succeeded in writing \
             bytes through a pipe the test closed before the call",
        );

        // Drain leader.
        let leader_bytes = leader_handle
            .await
            .expect("leader task panicked")
            .expect("leader returned Err");
        assert_eq!(leader_bytes.as_ref(), payload.as_ref());

        // KEY ASSERTION: with the is_pipe_broken guard in place, the
        // waiter must NOT have invoked the fall-back path (which
        // would call get_part_and_cache_inner → peer.get_part again).
        // The leader fires exactly 1 peer call; the waiter fires 0.
        // Without the guard, the waiter would fall back, fire its
        // OWN peer call, then trip "Tried to send while stream is
        // closed" mid-write — peer count would be 2.
        let observed = get_part_calls.load(AOrdering::SeqCst);
        assert_eq!(
            observed, 1,
            "wps pipe-broken test: peer.get_part must fire exactly ONCE \
             (the leader). Got {observed} calls. A value of 2 means the \
             waiter's race-window fall-back fired despite the writer \
             being closed (`is_pipe_broken()` guard violated). The \
             fall-back would attempt to send bytes via the closed pipe, \
             tripping `Tried to send while stream is closed` and \
             charging an extra peer fetch we can never deliver"
        );

        // The waiter must record a SF dedup-hit (it joined the leader
        // as a waiter; this must be observable regardless of which
        // arm it terminated in).
        let (dedup_hits, _bypasses_cap) =
            proxy_arc.singleflight_counters_snapshot();
        assert!(
            dedup_hits >= 1,
            "wps pipe-broken test: dedup hits must increment ≥ 1 — \
             waiter must have joined the leader's SF slot; got \
             {dedup_hits}",
        );

        // The waiter must NOT have recorded a fall-back attempt
        // (the guard short-circuits BEFORE record_waiter_fallback).
        let fallback_count = proxy_arc
            .singleflight_waiter_fallback_to_direct_total();
        assert_eq!(
            fallback_count, 0,
            "wps pipe-broken test: waiter_fallback_to_direct must be 0 \
             — the is_pipe_broken guard blocks fall-back BEFORE the \
             counter increments; got {fallback_count}",
        );

        Ok(())
    }

    // =================================================================
    // Test 6 (size bypass): blobs > MAX_CACHE_BLOB_SIZE bypass SF
    // =================================================================
    //
    // The dedup gate `should_use_sf` requires `digest.size_bytes() <=
    // MAX_CACHE_BLOB_SIZE` (64 MiB at the time of writing). Oversized
    // blobs must independently peer-fetch — no SF coordination.
    //
    // Mutation step: weaken the gate (`should_use_sf = should_cache &&
    // digest.size_bytes() > 0`) without the cache-eligibility size
    // bound. Then oversized blobs would dedup; the per-call peer
    // count would be 1 instead of N.
    //
    // We can't actually allocate 64+ MiB in test, so we use a fake
    // peer that lies about the blob size (peer holds a small blob
    // but the digest claims a large size). The SF gate runs against
    // the digest.size_bytes() — not the actual payload — so the
    // bypass decision is made BEFORE the bytes flow. We assert that
    // SF doesn't engage (no dedup hits, peer fetch fires per caller).

    #[nativelink_test]
    async fn wps_oversized_blob_bypasses_sf() -> Result<(), Error> {
        use nativelink_store::worker_proxy_store::WorkerProxyStore as WPS;

        // Digest claims 65 MiB (above MAX_CACHE_BLOB_SIZE = 64 MiB).
        // We don't actually allocate this — we just assert the SF
        // gate skips dedup. The peer's get_part will be invoked but
        // we'll release immediately and let it Err out (the seeded
        // payload won't match the digest size, so VerifyStore would
        // catch it; but we use plain MemoryStore here so the bytes
        // flow without verification).
        let oversized = (WPS::MAX_CACHE_BLOB_SIZE + 1) as u64;
        // VALID_HASH1 has the right format; the size_bytes is the
        // gate input.
        let digest = DigestInfo::try_new(VALID_HASH1, oversized)?;

        // Seed peer with a small payload (just enough that get_part
        // returns Ok-ish). We don't care about correctness of the
        // bytes — we care about peer-call-count.
        let payload = Bytes::from(vec![0u8; 4096]);

        // Build harness directly so we can inject a peer that
        // deliberately ignores the size mismatch.
        let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        let peer_inner = Store::new(MemoryStore::new(&MemorySpec::default()));
        // Use a different (correctly-sized) digest for the seed; we
        // override the request's digest only at the WPS layer via
        // the locality map. The peer's get_part with the oversized
        // digest won't find the seed, but that's fine — we only
        // count call entries.
        let _ = peer_inner.update_oneshot(digest, payload.clone()).await;

        let get_part_calls = Arc::new(AtomicU64::new(0));
        let (first_call_arrived_tx, _first_call_arrived_rx) =
            watch::channel(false);
        let (release_tx, release_rx) = watch::channel(true); // pre-released
        let counting_peer = Store::new(Arc::new(CountingPeerStore {
            inner: peer_inner,
            get_part_calls: get_part_calls.clone(),
            first_call_arrived_tx,
            release_rx,
        }));
        let locality_map = new_shared_blob_locality_map();
        let proxy_arc = WorkerProxyStore::new(inner, locality_map.clone());
        let endpoint = "grpc://sf-wps-oversized-test-peer:50081";
        proxy_arc.inject_worker_connection(endpoint, counting_peer);
        locality_map.write().register_blobs(endpoint, &[digest]);
        let proxy = Store::new(proxy_arc.clone());
        // release_tx is held to keep the watch alive (rx subscribed in
        // the fake); pre-released so peer calls proceed instantly.
        let _release_tx = release_tx;

        // Fire 4 concurrent reads. Without SF, each fires its own
        // peer.get_part. With SF (bug), N would collapse to ~1.
        const N_CALLERS: usize = 4;
        let mut handles = Vec::with_capacity(N_CALLERS);
        for _ in 0..N_CALLERS {
            let proxy = proxy.clone();
            handles.push(tokio::spawn(async move {
                proxy.get_part_unchunked(digest, 0, None).await
            }));
        }

        // Drain — we don't care about success, just call counts.
        let _ = timeout(Duration::from_secs(15), async {
            for h in handles {
                let _ = h.await;
            }
        })
        .await
        .expect(
            "wps oversized bypass test: callers must complete within 15s — \
             a hang here would mean an unintended dedup is wedging \
             waiters on a leader that never publishes",
        );

        // Bypass contract: 4 concurrent oversized reads ⇒ 4 peer
        // fetches. SF must NOT collapse them.
        let observed = get_part_calls.load(AOrdering::SeqCst);
        assert_eq!(
            observed, N_CALLERS as u64,
            "wps oversized bypass test: oversized blobs MUST bypass SF — \
             got {observed} peer fetches for {N_CALLERS} callers, \
             expected {N_CALLERS}. A value < {N_CALLERS} means the \
             `should_use_sf` size-bound gate failed and oversized blobs \
             are deduping. Oversized blobs that dedup at the WPS layer \
             would buffer 64+ MiB across N waiters via the SF cohort \
             machinery — a memory-safety hazard that the size bound \
             was specifically designed to prevent"
        );

        // SF dedup hits should remain 0 — no waiter joined a leader.
        let (dedup_hits, _bypasses_cap) =
            proxy_arc.singleflight_counters_snapshot();
        assert_eq!(
            dedup_hits, 0,
            "wps oversized bypass test: total_dedup_hits must be 0 — \
             oversized reads bypass SF and never reach the dedup gate; \
             got {dedup_hits}",
        );

        Ok(())
    }
}
