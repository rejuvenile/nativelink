// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! #401: cancel-safety regression coverage for `BazelChunkedDispatcherImpl::dispatch`.
//!
//! Bug shape: between `chunked_in_flight_digests.insert(digest)` and the
//! matched `remove(digest)` in `dispatch`, the await on
//! `dispatch_bazel_facing_internal_chunking` is a cancellation point. If
//! the upstream caller is a `try_join!`/`select!` that short-circuits on
//! a producer Err, the future is dropped mid-await and removal NEVER
//! runs. Production observation: digest leaks until process restart;
//! `FastSlowStore::has_with_results` reports `Some(size)` for leaked
//! digests forever; `ExistenceCacheStore` caches the `Some`; FMB returns
//! "present" forever; build fails on populate. The shutdown-time WARN at
//! `fast_slow_store.rs:2012-2017` ("unflushed chunked-path write at
//! shutdown") is the leak detector that fired in production.
//!
//! Fix: replace the manual insert/remove with `InFlightChunkedGuard`, an
//! RAII guard that removes in `Drop`. Cancel-safe by construction. The
//! success path explicitly disarms the guard and hands the (set, digest,
//! notify) tuple to the post-dispatch reaper task, which removes only
//! after the chunked-driver in-flight tracker drains (preserving #210
//! graceful-drain semantics).
//!
//! Tests in this file:
//!   1. `guard_drop_removes_digest` — unit test of the RAII contract.
//!   2. `guard_drop_notifies_when_set_becomes_empty` — preserves the
//!      `flush_slow_writes` lost-wakeup contract on the cancel path.
//!   3. `cancel_mid_await_removes_digest` — the bug. A future holding the
//!      guard across an await is cancelled via `select!`; afterwards the
//!      set MUST be empty. With the bug present (manual insert/remove),
//!      this test red-fails with the bespoke message. With the RAII
//!      guard in place, this test passes. Mutation step:
//!      commenting out the `impl Drop` body re-introduces the bug and
//!      this test red-fails with "InFlightChunkedGuard MUST remove
//!      digest on cancellation — leak detected".
//!   4. `disarm_prevents_drop_removal` — the success path's contract:
//!      after `disarm()` the guard's Drop is a no-op so the spawned
//!      reaper can hand removal to the chunked-driver-drain trigger.

#![cfg(feature = "chunked_fast_slow")]

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_macro::nativelink_test;
use nativelink_service::chunked_write_handler::InFlightChunkedGuard;
use nativelink_store::fast_slow_store::ChunkedInFlightMap;
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;

// MAJOR-G (#499 followup) regression: two concurrent InFlightChunkedGuards
// for the same digest must BOTH be observable until BOTH drop. Pre-fix
// (HashSet), the first drop removed the entry while the second was
// still in-flight — a transient phantom-missing window for readers.
//
// This test exercises the refcount HashMap contract:
//   1. Create guard1 → refcount=1
//   2. Create guard2 → refcount=2 (entry still present)
//   3. Drop guard1 → refcount=1 (entry STILL present)
//   4. Drop guard2 → refcount=0 (entry removed; notify fires)
//
// Mutation: change `InFlightChunkedGuard::Drop` to always `guard.remove(...)`
// regardless of refcount. Test MUST red-fail with the bespoke message
// `"MAJOR-G: refcount removed digest while second guard still in-flight"`.
#[nativelink_test]
async fn major_g_refcount_keeps_digest_visible_until_both_guards_drop() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0x42);

    {
        // Guard 1: first session's RAII.
        let _g1 = InFlightChunkedGuard::new(
            Arc::clone(&set),
            digest,
            Some(Arc::clone(&notify)),
        );
        assert_eq!(
            set.lock().get(&digest).map(|(c, _n)| c.get()),
            Some(1),
            "MAJOR-G: first guard MUST bump refcount to 1"
        );

        {
            // Guard 2: second concurrent session's RAII for the SAME digest.
            let _g2 = InFlightChunkedGuard::new(
                Arc::clone(&set),
                digest,
                Some(Arc::clone(&notify)),
            );
            assert_eq!(
                set.lock().get(&digest).map(|(c, _n)| c.get()),
                Some(2),
                "MAJOR-G: second guard MUST bump refcount to 2 (NOT \
                 idempotent set-insert); pre-fix HashSet treated this as \
                 no-op, leading to first-drop-removes-second-still-in-flight"
            );
            // g2 drops here; refcount goes 2 → 1; entry remains.
        }

        assert_eq!(
            set.lock().get(&digest).map(|(c, _n)| c.get()),
            Some(1),
            "MAJOR-G: refcount removed digest while second guard still \
             in-flight — first guard drop must decrement, NOT remove, \
             when refcount > 1. This is the central MAJOR-G contract; \
             without it, BLOCK-B's reader-wait fails for the second \
             session's window."
        );
        // g1 drops here; refcount goes 1 → 0; entry removed; notify fires.
    }

    assert!(
        set.lock().is_empty(),
        "MAJOR-G: both guards dropped; set MUST be empty"
    );
}

fn make_digest(byte: u8) -> DigestInfo {
    let mut packed = [0u8; 32];
    packed[0] = byte;
    DigestInfo::new(packed, 1)
}

#[nativelink_test]
async fn guard_drop_removes_digest() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0xA1);

    {
        let _guard = InFlightChunkedGuard::new(Arc::clone(&set), digest, Some(Arc::clone(&notify)));
        assert!(
            set.lock().contains_key(&digest),
            "InFlightChunkedGuard::new MUST insert digest into set"
        );
    }
    assert!(
        !set.lock().contains_key(&digest),
        "InFlightChunkedGuard::drop MUST remove digest from set"
    );
    assert_eq!(set.lock().len(), 0, "set must be empty after guard drop");
}

#[nativelink_test]
async fn guard_drop_notifies_when_set_becomes_empty() {
    // Mirrors the lost-wakeup contract that `flush_slow_writes` relies
    // on: when `chunked_in_flight_digests` transitions from non-empty to
    // empty, `in_flight_empty_notify.notify_waiters()` MUST fire so the
    // graceful-drain wakes. The RAII guard preserves this on the cancel
    // path (where the manual code path never gets a chance to call
    // notify_waiters).
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0xB2);

    // Start a waiter BEFORE arming the guard. Notify::notified() is
    // edge-triggered after registration; the registration must happen
    // before the notify_waiters() call inside Drop.
    let notify_for_waiter = Arc::clone(&notify);
    let waiter = tokio::spawn(async move {
        notify_for_waiter.notified().await;
    });
    // Yield to let waiter register on the Notify.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    {
        let _guard = InFlightChunkedGuard::new(Arc::clone(&set), digest, Some(Arc::clone(&notify)));
        assert_eq!(set.lock().len(), 1, "guard inserted");
    }

    // Drop fired. Set must be empty AND notify must have woken the waiter.
    assert!(set.lock().is_empty(), "guard removed digest");
    tokio::time::timeout(core::time::Duration::from_secs(2), waiter)
        .await
        .expect(
            "InFlightChunkedGuard::drop MUST call notify_waiters when the \
             set transitions to empty — flush_slow_writes lost-wakeup \
             contract violated",
        )
        .expect("waiter task panicked");
}

/// **Production composition test for the #401 cancel-safety bug.**
///
/// Wraps an `InFlightChunkedGuard` in a future that awaits a long sleep
/// (proxy for `dispatch_bazel_facing_internal_chunking.await`), then
/// races that future against a short timer in `select!`. The short timer
/// wins, the dispatch future is dropped mid-await — modeling the
/// production cancellation path (`bytestream_server.rs:1761-1775`'s
/// `try_join!` short-circuiting on a producer Err).
///
/// **Mutation step (per CLAUDE.md TDD #5):** comment out the body of
/// `impl Drop for InFlightChunkedGuard` in
/// `nativelink-service/src/chunked_write_handler.rs`. This test MUST
/// red-fail with the bespoke message:
///
///   "InFlightChunkedGuard MUST remove digest on cancellation —
///    leak detected"
///
/// If the test still passes after the mutation, the test does not guard
/// the behavior and is theatre.
#[nativelink_test]
async fn cancel_mid_await_removes_digest() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0xC3);

    let set_for_dispatch = Arc::clone(&set);
    let notify_for_dispatch = Arc::clone(&notify);

    // Simulate `BazelChunkedDispatcherImpl::dispatch`: arm the guard,
    // then await something long (proxying the inner
    // `dispatch_bazel_facing_internal_chunking` call). Cancellation
    // partway through this await is the bug.
    let dispatch_proxy = async move {
        let _guard = InFlightChunkedGuard::new(
            set_for_dispatch,
            digest,
            Some(notify_for_dispatch),
        );
        // Long sleep representing the inner async work.
        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
        // If we ever reach here in this test, cancellation didn't fire.
        // Disarm so the test doesn't double-remove on its own success
        // path (irrelevant in this test because the timer wins).
        unreachable!("dispatch proxy should be cancelled by select!");
    };

    // Race against a short timer that wins → dispatch_proxy dropped
    // mid-await → guard's Drop must run.
    tokio::select! {
        () = tokio::time::sleep(core::time::Duration::from_millis(50)) => {
            // Timer won — dispatch_proxy dropped.
        }
        () = dispatch_proxy => {
            unreachable!("dispatch proxy should not complete first");
        }
    }

    // Yield once to let any tail Drop logic settle.
    tokio::task::yield_now().await;

    // The bug: with a manual insert/remove pattern, this assertion
    // fails — the digest leaks because cancellation skipped the remove.
    // The fix (RAII Drop) makes this assertion pass.
    assert!(
        set.lock().is_empty(),
        "InFlightChunkedGuard MUST remove digest on cancellation — \
         leak detected (set still contains {} entries after cancelled await; \
         this is bug #401 — manual insert/remove around an await is not \
         cancel-safe)",
        set.lock().len()
    );
}

#[nativelink_test]
async fn disarm_prevents_drop_removal() {
    // Success-path contract: the production code disarms the guard and
    // hands the (set, digest, notify) tuple to a spawned reaper task
    // that removes only after the chunked-driver in-flight tracker
    // drains. Disarm MUST make Drop a no-op so the digest stays in the
    // set during async-commit (preserving #210 graceful-drain).
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let digest = make_digest(0xD4);

    {
        let guard = InFlightChunkedGuard::new(Arc::clone(&set), digest, Some(Arc::clone(&notify)));
        assert_eq!(set.lock().len(), 1, "guard inserted");
        let (set_handed_off, dig_handed_off, notify_handed_off) = guard.disarm();
        // After disarm, Drop has fired but did NOT remove (no-op Drop).
        assert!(
            set.lock().contains_key(&digest),
            "disarm MUST leave digest in set so success-path reaper owns removal"
        );
        // Hand-off triple should be the same Arc/digest the caller passed in.
        assert!(Arc::ptr_eq(&set, &set_handed_off));
        assert_eq!(dig_handed_off, digest);
        let notify_handed_off_for_check = notify_handed_off
            .as_ref()
            .map(Arc::clone)
            .expect("notify present");
        assert!(Arc::ptr_eq(&notify, &notify_handed_off_for_check));

        // Simulate the spawned reaper performing the removal.
        let mut g = set_handed_off.lock();
        g.remove(&dig_handed_off);
        let became_empty = g.is_empty();
        drop(g);
        if became_empty {
            notify_handed_off.unwrap().notify_waiters();
        }
    }
    assert!(
        set.lock().is_empty(),
        "after disarm + manual reaper removal, set must be empty"
    );
}

/// BLOCK-2 (DS-reviewer, #499 v3 follow-up): each map entry carries its
/// own `Arc<Notify>` so a reader holding the cloned Arc wakes when the
/// last `InFlightChunkedGuard` drops. Under-action coverage: with two
/// concurrent guards, the per-digest Notify must NOT fire until BOTH
/// drop (refcount → 0); a single drop must NOT prematurely wake
/// readers (the readers' digest is still in flight via the second
/// guard).
///
/// Mutation: change `InFlightChunkedGuard::Drop` to fire the per-digest
/// notify on every drop (regardless of refcount). Test MUST red-fail
/// because the reader would wake after the first drop while the
/// second guard still holds the digest in-flight.
#[nativelink_test]
async fn block_2_per_digest_notify_fires_only_on_refcount_zero() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let drain_notify = Arc::new(Notify::new());
    let digest = make_digest(0xE5);

    // Create both guards, then grab the per-digest Notify out of the map.
    let g1 = InFlightChunkedGuard::new(
        Arc::clone(&set),
        digest,
        Some(Arc::clone(&drain_notify)),
    );
    let g2 = InFlightChunkedGuard::new(
        Arc::clone(&set),
        digest,
        Some(Arc::clone(&drain_notify)),
    );

    // Reader-side: clone the per-digest Notify out of the map slot.
    let per_digest_notify = set
        .lock()
        .get(&digest)
        .map(|(_c, n)| Arc::clone(n))
        .expect("entry exists with per-digest Notify");

    // Subscribe BEFORE any drop fires.
    let waker = tokio::spawn({
        let n = Arc::clone(&per_digest_notify);
        async move {
            n.notified().await;
        }
    });

    // Yield so the waker is parked.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Drop g1 — refcount 2 → 1. Per-digest Notify MUST NOT fire because
    // g2 still holds the entry in-flight.
    drop(g1);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        set.lock().get(&digest).map(|(c, _)| c.get()),
        Some(1),
        "BLOCK-2: refcount must be 1 after g1.drop while g2 still alive"
    );
    assert!(
        !waker.is_finished(),
        "BLOCK-2: per-digest Notify fired prematurely while g2 still holds \
         the digest in-flight — readers would wake and falsely observe the \
         digest as drained. Mutation: change Drop to fire notify on every \
         drop regardless of refcount — this assertion must red-fail."
    );

    // Drop g2 — refcount 1 → 0. Per-digest Notify MUST fire now.
    drop(g2);

    // The waker must complete within bounded wall-clock (deadlock-detector
    // 2s; the actual Notify fires immediately on drop).
    tokio::time::timeout(core::time::Duration::from_secs(2), waker)
        .await
        .expect(
            "BLOCK-2: per-digest Notify MUST fire on refcount → 0 \
             (InFlightChunkedGuard::Drop), waking any reader holding the \
             cloned Arc<Notify>. Mutation: comment out the \
             `n.notify_waiters()` call in InFlightChunkedGuard::Drop \
             where refcount transitions to zero — this assertion must \
             red-fail with `Elapsed`.",
        )
        .expect("waker task panicked");

    assert!(set.lock().is_empty(), "set must be empty after both drops");
}

/// BLOCK-2 (DS-reviewer, #499 v3 follow-up): the per-digest Notify
/// must fire AFTER the entry is removed from the map. A reader that
/// re-checks `chunked_in_flight_digests.contains_key(...)` after
/// waking must observe the drained state. The reader cloned the
/// `Arc<Notify>` while subscribing, so the wakeup survives the map
/// removal even though the entry's slot is gone. This test pins the
/// remove-then-notify ordering by:
///  1. subscribing the reader to the per-digest Notify,
///  2. dropping the guard,
///  3. asserting the reader wakes,
///  4. asserting the entry is gone from the map AFTER the reader wakes.
#[nativelink_test]
async fn block_2_per_digest_notify_fires_after_entry_removed() {
    let set: ChunkedInFlightMap = Arc::new(Mutex::new(HashMap::new()));
    let drain_notify = Arc::new(Notify::new());
    let digest = make_digest(0xE6);

    let g = InFlightChunkedGuard::new(
        Arc::clone(&set),
        digest,
        Some(Arc::clone(&drain_notify)),
    );

    let per_digest_notify = set
        .lock()
        .get(&digest)
        .map(|(_c, n)| Arc::clone(n))
        .expect("entry exists with per-digest Notify");

    let set_for_reader = Arc::clone(&set);
    let reader = tokio::spawn(async move {
        per_digest_notify.notified().await;
        // After waking, the entry must be absent — the writer's Drop
        // removes the entry from the map FIRST and then calls
        // notify_waiters AFTER `map.remove`. The reader (this task)
        // cloned the Arc<Notify> while subscribing, so the wakeup
        // survives the map removal, and on wakeup
        // `contains_key(&digest)` observes the drained state.
        let still_present = set_for_reader.lock().contains_key(&digest);
        assert!(
            !still_present,
            "BLOCK-2 ordering: reader woke but entry still present in map. \
             InFlightChunkedGuard::Drop must call notify_waiters AFTER \
             guard.remove(&self.digest). Mutation: swap the order so \
             notify_waiters fires BEFORE remove — this assertion must \
             red-fail."
        );
    });

    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    drop(g);

    tokio::time::timeout(core::time::Duration::from_secs(2), reader)
        .await
        .expect("reader must complete within 2s")
        .expect("reader task panicked");
}
