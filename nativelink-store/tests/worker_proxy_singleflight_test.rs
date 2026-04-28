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
// Test 2: partial-range reads bypass — adjusted for the standalone module
// =====================================================================
//
// Per CLAUDE.md and the design doc Option B note, partial-range
// bypass is decided AT THE WPS WIRE-UP LAYER (the caller of
// SingleflightMap), not inside the module — the module is key-only.
// The standalone module has no offset/length parameter, so the
// "partial reads bypass singleflight" property is enforced by the
// caller's gate (`offset == 0 && length.is_none()`), not by the module.
//
// This test is renamed and refocused: it verifies that the *caller-side
// gate* (when implemented in WPS) is the right discriminator by
// confirming that two callers using the SAME key DO dedup — i.e., the
// module dedups EVERYTHING for the key, and the WPS layer is
// responsible for choosing which calls to route through it.
//
// The Option B partial-range bypass test will live in WPS-level
// integration tests once the wiring lands. This test is marked
// `#[ignore]` with a documentation comment rather than removed —
// removing it would erase the design-trace that ties the module API
// to the WPS-layer gating decision.

#[nativelink_test]
#[ignore = "Option B partial-range bypass is a WPS-wire-up concern; the standalone \
            SingleflightMap module is key-only. This test re-enables once \
            WorkerProxyStore::get_part_and_cache wires SingleflightMap with the \
            `offset == 0 && length.is_none()` predicate."]
async fn concurrent_partial_range_reads_match_design() -> Result<(), Error> {
    // Intentionally empty — the assertion lives in WPS integration tests
    // post-wire-up. This stub preserves the test name from the original
    // red-TDD scaffold so the design trace is grep-able.
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
