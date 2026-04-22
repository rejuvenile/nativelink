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

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_util::coalesce::{CoalesceOptions, InFlightMap, with_construction_lock};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;
use tokio::time::sleep;

fn fresh_map<K, V>() -> InFlightMap<K, V>
where
    K: Eq + core::hash::Hash,
{
    Arc::new(Mutex::new(HashMap::new()))
}

#[nativelink_test]
async fn single_leader_returns_compute_result() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let value = with_construction_lock(
        &map,
        "key",
        CoalesceOptions::leader_only(Duration::from_secs(5)),
        || async { Ok(42) },
    )
    .await?;
    assert_eq!(value, 42);
    assert!(
        map.lock().is_empty(),
        "in_flight entry must be removed after leader completes",
    );
    Ok(())
}

#[nativelink_test]
async fn coalesces_concurrent_calls_to_one_compute() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let invocations = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let mut handles = Vec::new();
    for _ in 0..10 {
        let map = Arc::clone(&map);
        let invocations = Arc::clone(&invocations);
        let release = Arc::clone(&release);
        handles.push(tokio::spawn(async move {
            with_construction_lock(
                &map,
                "shared",
                CoalesceOptions::leader_only(Duration::from_secs(5)),
                || async move {
                    invocations.fetch_add(1, Ordering::SeqCst);
                    // Wait until the test releases us so all 10 tasks
                    // have time to subscribe before compute completes.
                    release.notified().await;
                    Ok(7)
                },
            )
            .await
        }));
    }

    // Give all tasks time to enter with_construction_lock and either
    // become the leader or subscribe as waiters.
    sleep(Duration::from_millis(50)).await;
    release.notify_one();

    for h in handles {
        let v = h.await.expect("task panicked")?;
        assert_eq!(v, 7);
    }
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "compute should run exactly once",
    );
    assert!(map.lock().is_empty());
    Ok(())
}

#[nativelink_test]
async fn leader_cancellation_aborts_waiters_and_clears_slot() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let leader_started = Arc::new(Notify::new());
    let leader_started_for_leader = Arc::clone(&leader_started);

    // Spawn a leader that we can cancel mid-flight.
    let leader_map = Arc::clone(&map);
    let leader_handle = tokio::spawn(async move {
        with_construction_lock(
            &leader_map,
            "abort-key",
            CoalesceOptions::no_timeout(),
            || async move {
                leader_started_for_leader.notify_waiters();
                // Sleep effectively forever (the test will abort us first).
                sleep(Duration::from_secs(60)).await;
                Ok(0_u32)
            },
        )
        .await
    });

    // Wait for leader to enter compute and the entry to appear.
    leader_started.notified().await;
    // Spawn a waiter on the same key.
    let waiter_map = Arc::clone(&map);
    let waiter_handle = tokio::spawn(async move {
        with_construction_lock(
            &waiter_map,
            "abort-key",
            CoalesceOptions::leader_only(Duration::from_secs(60)),
            || async { Ok(0_u32) },
        )
        .await
    });
    // Give the waiter time to subscribe.
    sleep(Duration::from_millis(20)).await;

    // Cancel the leader.
    leader_handle.abort();
    let leader_result = leader_handle.await;
    assert!(
        leader_result.is_err() && leader_result.unwrap_err().is_cancelled(),
        "leader should have been cancelled",
    );

    // Waiter should observe an Aborted error (channel closed).
    let waiter_err = waiter_handle
        .await
        .expect("waiter task panicked")
        .expect_err("waiter should not succeed when leader was cancelled");
    assert_eq!(waiter_err.code, Code::Aborted, "got: {waiter_err:?}");

    // The entry must be cleared so a subsequent caller becomes a fresh
    // leader.
    assert!(
        map.lock().is_empty(),
        "in_flight should be empty after cancellation: {:?}",
        map.lock().keys().collect::<Vec<_>>(),
    );

    // A fresh call for the same key should succeed (we are a new leader).
    let fresh = with_construction_lock(
        &map,
        "abort-key",
        CoalesceOptions::leader_only(Duration::from_secs(5)),
        || async { Ok(99_u32) },
    )
    .await?;
    assert_eq!(fresh, 99);

    Ok(())
}

#[nativelink_test]
async fn leader_timeout_clears_slot_and_returns_error() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let result = with_construction_lock(
        &map,
        "slow",
        CoalesceOptions {
            leader_timeout: Some(Duration::from_millis(20)),
            waiter_timeout: Some(Duration::from_secs(5)),
        },
        || async {
            sleep(Duration::from_secs(5)).await;
            Ok(0_u32)
        },
    )
    .await;
    let err = result.expect_err("leader should have timed out");
    assert_eq!(err.code, Code::DeadlineExceeded, "got: {err:?}");
    assert!(map.lock().is_empty(), "slot must be cleared after timeout");
    Ok(())
}

#[nativelink_test]
async fn waiter_timeout_does_not_affect_leader() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let release = Arc::new(Notify::new());
    let release_leader = Arc::clone(&release);

    // Leader: takes 200ms. No leader timeout.
    let leader_map = Arc::clone(&map);
    let leader_handle = tokio::spawn(async move {
        with_construction_lock(
            &leader_map,
            "k",
            CoalesceOptions::no_timeout(),
            || async move {
                release_leader.notified().await;
                Ok(11_u32)
            },
        )
        .await
    });

    // Give leader time to install itself.
    sleep(Duration::from_millis(20)).await;

    // Waiter: 30ms timeout — should fail before leader is released.
    let waiter_map = Arc::clone(&map);
    let waiter_handle = tokio::spawn(async move {
        with_construction_lock(
            &waiter_map,
            "k",
            CoalesceOptions {
                leader_timeout: None,
                waiter_timeout: Some(Duration::from_millis(30)),
            },
            || async { Ok(0_u32) },
        )
        .await
    });

    let waiter_err = waiter_handle
        .await
        .expect("waiter task panicked")
        .expect_err("waiter should have timed out");
    assert_eq!(waiter_err.code, Code::DeadlineExceeded, "got: {waiter_err:?}");

    // Leader should still be running. Release it and confirm success.
    release.notify_one();
    let leader_value = leader_handle.await.expect("leader panicked")?;
    assert_eq!(leader_value, 11);
    assert!(map.lock().is_empty());
    Ok(())
}

#[nativelink_test]
async fn different_keys_progress_independently() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let invocations = Arc::new(AtomicUsize::new(0));

    // Each key has its own compute that waits on its own gate.
    let release_a = Arc::new(Notify::new());
    let release_b = Arc::new(Notify::new());

    let map_a = Arc::clone(&map);
    let inv_a = Arc::clone(&invocations);
    let release_a_inner = Arc::clone(&release_a);
    let task_a = tokio::spawn(async move {
        with_construction_lock(
            &map_a,
            "A",
            CoalesceOptions::leader_only(Duration::from_secs(5)),
            || async move {
                inv_a.fetch_add(1, Ordering::SeqCst);
                release_a_inner.notified().await;
                Ok(1_u32)
            },
        )
        .await
    });

    let map_b = Arc::clone(&map);
    let inv_b = Arc::clone(&invocations);
    let release_b_inner = Arc::clone(&release_b);
    let task_b = tokio::spawn(async move {
        with_construction_lock(
            &map_b,
            "B",
            CoalesceOptions::leader_only(Duration::from_secs(5)),
            || async move {
                inv_b.fetch_add(1, Ordering::SeqCst);
                release_b_inner.notified().await;
                Ok(2_u32)
            },
        )
        .await
    });

    // Let both leaders start.
    sleep(Duration::from_millis(20)).await;
    assert_eq!(invocations.load(Ordering::SeqCst), 2);

    // Release in interleaved order.
    release_b.notify_one();
    let v_b = task_b.await.expect("B panicked")?;
    release_a.notify_one();
    let v_a = task_a.await.expect("A panicked")?;
    assert_eq!(v_a, 1);
    assert_eq!(v_b, 2);
    assert!(map.lock().is_empty());
    Ok(())
}

#[nativelink_test]
async fn leader_panic_aborts_waiters_and_clears_slot() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let leader_in = Arc::new(Notify::new());
    let leader_in_inner = Arc::clone(&leader_in);

    let leader_map = Arc::clone(&map);
    let leader_handle = tokio::spawn(async move {
        // catch_unwind needed to keep the test runtime alive when the
        // future panics. The leader Drop must still clear the slot.
        let map_ref = &leader_map;
        let outer = async move {
            with_construction_lock(
                map_ref,
                "panic-key",
                CoalesceOptions::no_timeout(),
                || async move {
                    leader_in_inner.notify_waiters();
                    sleep(Duration::from_millis(20)).await;
                    panic!("leader exploded");
                    #[allow(unreachable_code)]
                    Ok(0_u32)
                },
            )
            .await
        };
        std::panic::AssertUnwindSafe(outer).catch_unwind().await
    });

    leader_in.notified().await;

    let waiter_map = Arc::clone(&map);
    let waiter_handle = tokio::spawn(async move {
        with_construction_lock(
            &waiter_map,
            "panic-key",
            CoalesceOptions::leader_only(Duration::from_secs(5)),
            || async { Ok(0_u32) },
        )
        .await
    });

    let _leader_outcome = leader_handle.await.expect("leader join panicked");
    // Waiter must observe Aborted (channel closed because leader's
    // sender was dropped without publishing).
    let waiter_err = waiter_handle
        .await
        .expect("waiter join panicked")
        .expect_err("waiter should not succeed when leader panicked");
    assert_eq!(waiter_err.code, Code::Aborted, "got: {waiter_err:?}");

    assert!(
        map.lock().is_empty(),
        "in_flight slot must be cleared after leader panic",
    );
    Ok(())
}

#[nativelink_test]
async fn high_contention_one_compute_for_one_thousand_callers() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let invocations = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let mut handles = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let map = Arc::clone(&map);
        let invocations = Arc::clone(&invocations);
        let release = Arc::clone(&release);
        handles.push(tokio::spawn(async move {
            with_construction_lock(
                &map,
                "stress",
                CoalesceOptions::leader_only(Duration::from_secs(10)),
                || async move {
                    invocations.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(123)
                },
            )
            .await
        }));
    }

    // Let everyone subscribe.
    sleep(Duration::from_millis(100)).await;
    // Notify all waiters of the leader's compute (only the leader is
    // actually waiting on `release`).
    release.notify_one();

    let mut succeeded = 0_usize;
    for h in handles {
        let v = h.await.expect("task panicked")?;
        assert_eq!(v, 123);
        succeeded += 1;
    }
    assert_eq!(succeeded, 1000);
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "compute should run exactly once even under 1000-way contention",
    );
    assert!(map.lock().is_empty());
    Ok(())
}

#[nativelink_test]
async fn leader_error_is_fanned_out_to_waiters() -> Result<(), Error> {
    let map: InFlightMap<&'static str, u32> = fresh_map();
    let release = Arc::new(Notify::new());

    let leader_map = Arc::clone(&map);
    let leader_release = Arc::clone(&release);
    let leader = tokio::spawn(async move {
        with_construction_lock(
            &leader_map,
            "err-key",
            CoalesceOptions::leader_only(Duration::from_secs(5)),
            || async move {
                leader_release.notified().await;
                Err::<u32, _>(nativelink_error::make_err!(
                    Code::FailedPrecondition,
                    "upstream said no"
                ))
            },
        )
        .await
    });

    // Give the leader time to install itself.
    sleep(Duration::from_millis(20)).await;
    let waiter_map = Arc::clone(&map);
    let waiter = tokio::spawn(async move {
        with_construction_lock(
            &waiter_map,
            "err-key",
            CoalesceOptions::leader_only(Duration::from_secs(5)),
            || async { Ok(0_u32) },
        )
        .await
    });
    // Give the waiter time to subscribe.
    sleep(Duration::from_millis(20)).await;

    release.notify_one();

    let leader_err = leader
        .await
        .expect("leader panicked")
        .expect_err("leader should have errored");
    assert_eq!(leader_err.code, Code::FailedPrecondition);
    let waiter_err = waiter
        .await
        .expect("waiter panicked")
        .expect_err("waiter should have received the leader's error");
    assert_eq!(waiter_err.code, Code::FailedPrecondition);
    assert!(map.lock().is_empty());
    Ok(())
}
