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

//! #402: cancel-safety regression coverage for `ByteStreamServer::write`
//! `in_flight_writes` map.
//!
//! Bug shape (sibling of #401, same source-file region): between
//! `in_flight_writes.lock().insert(digest, rx)` at
//! `bytestream_server.rs:2395` and the matched `remove(&digest)` at
//! `bytestream_server.rs:2505`, the wrapped future at `write_fut.await`
//! is a cancellation point. If the gRPC stream is cancelled (client
//! disconnect, gRPC stream RST, server shutdown, runtime drop), the
//! future is dropped mid-await and the `remove` NEVER runs. Production
//! impact: every cancelled ByteStream upload leaks one `HashMap` entry +
//! one watch channel for the lifetime of the process. New RPCs for the
//! same digest then `coalesce` onto the orphaned watch; the `tx` was
//! also dropped by cancellation so `rx.changed()` returns `Err`
//! immediately, and the loop translates that to "in-flight write
//! failed, retrying" — a confusing log AND wasted coalescing.
//!
//! Fix: replace the manual insert/remove with `InFlightWritesGuard`, an
//! RAII guard that removes from the map in `Drop`. Cancel-safe by
//! construction. The guard owns the watch `Sender` so that:
//!  * Success/error path: caller invokes `set_result(bool)` to publish
//!    the outcome to coalesced waiters; Drop then removes the map entry.
//!  * Cancellation path: `set_result` was never called; Drop removes the
//!    map entry AND drops the `Sender`. The dropped Sender causes any
//!    coalesced waiter's `rx.changed()` to return `Err`, which the
//!    waiter loop already translates to "false / failure" at
//!    `bytestream_server.rs:2354-2355`. So waiters never hang on a
//!    cancelled writer's orphaned channel.
//!
//! Tests in this file:
//!   1. `guard_drop_removes_entry` — unit test of the RAII contract.
//!   2. `guard_set_result_publishes_outcome` — success-path: `set_result`
//!      sends the outcome to coalesced waiters BEFORE Drop removes.
//!   3. `cancel_mid_await_removes_entry` — the bug. A future holding the
//!      guard across an await is cancelled via `select!`; afterwards the
//!      map MUST be empty. With the bug present (manual insert/remove),
//!      this test red-fails with the bespoke message. With the RAII
//!      guard in place, this test passes. **Mutation step:** commenting
//!      out the body of `impl Drop for InFlightWritesGuard` re-introduces
//!      the bug and this test red-fails with "InFlightWritesGuard MUST
//!      remove entry on cancellation — leak detected (this is bug #402 —
//!      manual insert/remove around an await is not cancel-safe)".
//!   4. `coalesced_waiter_observes_failure_on_writer_cancel` — cross-seam
//!      test. Composes the production pattern: primary writer creates
//!      guard, second arrival subscribes to the watch, primary is
//!      cancelled mid-await. Coalesced waiter MUST observe the writer's
//!      disappearance (rx.changed() = Err → translated to false at the
//!      production call site) within `tokio::time::timeout`.

use std::collections::HashMap;
use std::sync::Arc;

use nativelink_macro::nativelink_test;
use nativelink_service::bytestream_server::InFlightWritesGuard;
use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio::sync::watch;

type InFlightMap =
    Arc<Mutex<HashMap<DigestInfo, watch::Receiver<Option<bool>>>>>;

fn make_digest(byte: u8) -> DigestInfo {
    let mut packed = [0u8; 32];
    packed[0] = byte;
    DigestInfo::new(packed, 1)
}

fn fresh_map() -> InFlightMap {
    Arc::new(Mutex::new(HashMap::new()))
}

#[nativelink_test]
async fn guard_drop_removes_entry() {
    let map = fresh_map();
    let digest = make_digest(0xA1);
    let (tx, rx) = watch::channel(None);

    {
        let _guard = InFlightWritesGuard::new(Arc::clone(&map), digest, tx, rx);
        assert!(
            map.lock().contains_key(&digest),
            "InFlightWritesGuard::new MUST insert entry into map"
        );
    }
    assert!(
        !map.lock().contains_key(&digest),
        "InFlightWritesGuard::drop MUST remove entry from map"
    );
    assert_eq!(map.lock().len(), 0, "map must be empty after guard drop");
}

#[nativelink_test]
async fn guard_set_result_publishes_outcome() {
    // Success-path contract: the production code calls `set_result(true)`
    // (or false) BEFORE the guard is dropped, so coalesced waiters
    // observe the actual outcome rather than the cancellation-shaped
    // "sender dropped" failure signal.
    let map = fresh_map();
    let digest = make_digest(0xB2);
    let (tx, rx) = watch::channel(None);

    // Subscribe a coalesced-waiter rx BEFORE the guard takes ownership.
    let mut waiter_rx = rx.clone();

    {
        let mut guard = InFlightWritesGuard::new(Arc::clone(&map), digest, tx, rx);
        // Successful write completes — publish the outcome.
        guard.set_result(true);
        // Waiter MUST see Some(true) after set_result, before Drop.
        let val = *waiter_rx.borrow_and_update();
        assert_eq!(
            val,
            Some(true),
            "set_result MUST publish the outcome to coalesced waiters via the watch channel"
        );
    }
    // Drop ran — entry must be gone.
    assert!(
        !map.lock().contains_key(&digest),
        "guard drop MUST remove map entry on the success path too"
    );
}

/// **Production composition test for the #402 cancel-safety bug.**
///
/// Wraps an `InFlightWritesGuard` in a future that awaits a long sleep
/// (proxy for `inner_write` / `inner_write_oneshot.await`), then races
/// that future against a short timer in `select!`. The short timer
/// wins, the write future is dropped mid-await — modeling production
/// cancellation paths: client disconnect, gRPC RST_STREAM, server
/// shutdown, runtime drop.
///
/// **Mutation step (per CLAUDE.md TDD #5):** comment out the body of
/// `impl Drop for InFlightWritesGuard` in
/// `nativelink-service/src/bytestream_server.rs`. This test MUST
/// red-fail with the bespoke message:
///
///   "InFlightWritesGuard MUST remove entry on cancellation —
///    leak detected (this is bug #402 — manual insert/remove around an
///    await is not cancel-safe)"
///
/// If the test still passes after the mutation, the test does not
/// guard the behavior and is theatre.
#[nativelink_test]
async fn cancel_mid_await_removes_entry() {
    let map = fresh_map();
    let digest = make_digest(0xC3);
    let (tx, rx) = watch::channel(None);

    let map_for_writer = Arc::clone(&map);

    // Simulate the production write path: arm the guard, then await
    // something long (proxying the inner write call). Cancellation
    // partway through this await is the bug.
    let writer_proxy = async move {
        let _guard = InFlightWritesGuard::new(map_for_writer, digest, tx, rx);
        // Long sleep representing the inner async write work.
        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
        unreachable!("writer proxy should be cancelled by select!");
    };

    // Race against a short timer that wins → writer_proxy dropped
    // mid-await → guard's Drop must run.
    tokio::select! {
        () = tokio::time::sleep(core::time::Duration::from_millis(50)) => {
            // Timer won — writer_proxy dropped.
        }
        () = writer_proxy => {
            unreachable!("writer proxy should not complete first");
        }
    }

    // Yield once to let any tail Drop logic settle.
    tokio::task::yield_now().await;

    // The bug: with a manual insert/remove pattern, this assertion
    // fails — the entry leaks because cancellation skipped the remove.
    // The fix (RAII Drop) makes this assertion pass.
    let leaked = map.lock().len();
    assert_eq!(
        leaked, 0,
        "InFlightWritesGuard MUST remove entry on cancellation — \
         leak detected (map still contains {leaked} entries after cancelled await; \
         this is bug #402 — manual insert/remove around an await is not \
         cancel-safe)"
    );
}

/// **Cross-seam test:** a coalesced waiter (second RPC arrival for the
/// same digest) subscribes to the primary writer's watch channel via
/// `rx.clone()`. When the primary writer is cancelled mid-await, the
/// guard's Drop must drop the watch `Sender`, which causes the
/// coalesced waiter's `rx.changed()` to return `Err`. The production
/// loop at `bytestream_server.rs:2354-2355` translates that to "false /
/// failure", so waiters never hang on a cancelled writer's orphaned
/// channel.
///
/// `tokio::time::timeout` of 2s is the deadlock detector: if the waiter
/// hangs forever, the timeout fires with a specific failure message.
#[nativelink_test]
async fn coalesced_waiter_observes_failure_on_writer_cancel() {
    let map = fresh_map();
    let digest = make_digest(0xD4);
    let (tx, rx) = watch::channel(None);

    // Coalesced waiter clones the receiver before the guard takes
    // ownership of the original — production does the same dance via
    // `guard.get(&digest).cloned()` at bytestream_server.rs:2342-2343.
    let mut waiter_rx = rx.clone();

    let map_for_writer = Arc::clone(&map);
    let writer_proxy = async move {
        let _guard = InFlightWritesGuard::new(map_for_writer, digest, tx, rx);
        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
        unreachable!("writer proxy should be cancelled");
    };

    tokio::select! {
        () = tokio::time::sleep(core::time::Duration::from_millis(50)) => {}
        () = writer_proxy => {
            unreachable!("writer proxy should not complete first");
        }
    }

    // Mirror the production coalesced-waiter loop
    // (bytestream_server.rs:2349-2358): poll the watch until it yields a
    // result OR `changed()` returns Err (sender dropped = failure).
    let waiter_outcome = tokio::time::timeout(
        core::time::Duration::from_secs(2),
        async move {
            loop {
                if let Some(ok) = *waiter_rx.borrow_and_update() {
                    return ok;
                }
                if waiter_rx.changed().await.is_err() {
                    return false; // sender dropped = failure
                }
            }
        },
    )
    .await
    .expect(
        "coalesced waiter MUST NOT hang on a cancelled writer's watch \
         channel — InFlightWritesGuard::drop MUST drop the Sender so \
         rx.changed() returns Err and the waiter unblocks",
    );

    assert!(
        !waiter_outcome,
        "coalesced waiter MUST observe failure when primary writer is cancelled"
    );
    assert!(
        map.lock().is_empty(),
        "guard drop MUST remove the map entry on cancellation"
    );
}
