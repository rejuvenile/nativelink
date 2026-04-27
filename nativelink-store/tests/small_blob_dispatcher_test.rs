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

//! TDD tests for the Bug A small-CAS peer-mirror dispatcher.
//! Spec: `.claude/plans/bug-a-small-cas-peer-mirror.md`.
//!
//! These tests are derived FROM THE SPEC, not from the implementation. They
//! exist to drive the implementation (test-first development per CLAUDE.md
//! TDD rule) and to detect regressions in:
//!   1. `StoreDriver::observe_pinned_mirror_ack` trait extension (default no-op).
//!   2. `EphemeralServerSidePin` per-store pin set insert / remove / cap.
//!   3. `SmallBlobDispatcher::enqueue` precondition on blob size.
//!   4. Feature-flag inert-when-disabled (`small_blob_mirror_enabled = false`).
//!   5. Broadcast-routing self-filter via binary-search over `store_id` slice.
//!
//! Each test that exercises async / channels is wrapped in
//! `tokio::time::timeout` per the CLAUDE.md "deadlock detector" rule.

use core::time::Duration;

use bytes::Bytes;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::MirrorPinEntry;
use nativelink_store::small_blob_dispatcher::{
    EphemeralServerSidePin, SmallBlobDispatcher, SmallBlobDispatcherConfig,
    SMALL_BLOB_THRESHOLD,
};
use nativelink_util::common::DigestInfo;

/// 5-second deadlock detector for any test that touches mpsc/dispatcher.
const DEADLOCK_DETECTOR: Duration = Duration::from_secs(5);

fn make_digest(seed: u8, size: u64) -> DigestInfo {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    DigestInfo::new(bytes, size)
}

// ----------------------------------------------------------------------
// Test 1 (per plan B6 + C12): EphemeralServerSidePin insert / remove.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn ephemeral_pin_insert_and_remove_tracks_total_bytes() -> Result<(), Error> {
    let pin = EphemeralServerSidePin::new(/* cap= */ 1024);
    let d1 = make_digest(1, 100);
    let d2 = make_digest(2, 200);

    pin.insert(d1, Bytes::from(vec![0u8; 100]))
        .expect("first insert below cap must succeed");
    assert_eq!(pin.total_bytes(), 100);
    assert_eq!(pin.len(), 1);

    pin.insert(d2, Bytes::from(vec![0u8; 200]))
        .expect("second insert below cap must succeed");
    assert_eq!(pin.total_bytes(), 300);
    assert_eq!(pin.len(), 2);

    pin.remove_one(&d1);
    assert_eq!(pin.total_bytes(), 200);
    assert_eq!(pin.len(), 1);

    Ok(())
}

// ----------------------------------------------------------------------
// Test 2 (per plan C2 capacity gate): EphemeralServerSidePin cap rejects.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn ephemeral_pin_cap_exceeded_returns_resource_exhausted() -> Result<(), Error> {
    let pin = EphemeralServerSidePin::new(/* cap= */ 100);
    let d1 = make_digest(1, 50);
    let d2 = make_digest(2, 60);

    pin.insert(d1, Bytes::from(vec![0u8; 50]))
        .expect("under cap insert must succeed");
    let err = pin
        .insert(d2, Bytes::from(vec![0u8; 60]))
        .expect_err("over-cap insert MUST fail");
    assert_eq!(
        err.code, Code::ResourceExhausted,
        "EphemeralServerSidePin cap-exceeded MUST return ResourceExhausted (matches \
         insert_mirror_blob shape at fast_slow_store.rs:804); got {err:?}"
    );
    assert_eq!(
        pin.total_bytes(),
        50,
        "rejected insert MUST NOT mutate the pin set"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 3 (per plan C8 mutation coverage): observe_pinned_mirror_ack
// only removes entries whose store_id matches `self.store_id`.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn observe_pinned_mirror_ack_filters_by_store_id() -> Result<(), Error> {
    let pin = EphemeralServerSidePin::new(/* cap= */ 1024);
    let d1 = make_digest(1, 10);
    let d2 = make_digest(2, 20);
    let d3 = make_digest(3, 30);

    pin.insert(d1, Bytes::from(vec![0u8; 10]))?;
    pin.insert(d2, Bytes::from(vec![0u8; 20]))?;
    pin.insert(d3, Bytes::from(vec![0u8; 30]))?;
    assert_eq!(pin.len(), 3);

    // Build a sorted-by-store_id snapshot like the worker would produce.
    // Mix of "ac" and "cas" entries; THIS pin is for "cas" only.
    let mut entries: Vec<MirrorPinEntry> = vec![
        MirrorPinEntry { digest: Some(d1.into()), store_id: "ac".to_string() },
        MirrorPinEntry { digest: Some(d2.into()), store_id: "cas".to_string() },
        MirrorPinEntry { digest: Some(d3.into()), store_id: "cas".to_string() },
    ];
    entries.sort_by(|a, b| a.store_id.cmp(&b.store_id));

    pin.observe_pinned_mirror_ack("cas", &entries);

    // Only d2 + d3 (both "cas") removed; d1 ("ac") MUST be untouched.
    assert!(pin.contains(&d1), "ac entry MUST NOT be touched by cas store");
    assert!(!pin.contains(&d2), "cas d2 MUST be removed");
    assert!(!pin.contains(&d3), "cas d3 MUST be removed");
    assert_eq!(pin.total_bytes(), 10, "only d1 (10 bytes) remains");

    Ok(())
}

// ----------------------------------------------------------------------
// Test 4 (per plan #3 feature-flag inert-when-disabled): dispatcher
// constructed with `small_blob_mirror_enabled = false` MUST be a no-op
// at the enqueue site.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_disabled_enqueue_is_noop() -> Result<(), Error> {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: false,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = SmallBlobDispatcher::new(cfg);
    let d = make_digest(1, 100);

    let res = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue(
            "endpoint-a",
            /* boot_epoch_id= */ 1,
            "cas",
            d,
            Bytes::from(vec![0u8; 100]),
        ),
    )
    .await
    .expect("dispatcher.enqueue MUST NOT block when feature-flag disabled");

    assert!(
        res.is_ok(),
        "feature-flag-disabled enqueue MUST silently succeed (no-op); got {res:?}"
    );
    assert_eq!(
        dispatcher.dispatched_count(),
        0,
        "feature-flag-disabled enqueue MUST NOT dispatch any item"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 5 (per plan C9 precondition): dispatcher enqueue rejects blobs
// larger than SMALL_BLOB_THRESHOLD.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_rejects_oversize_blob() -> Result<(), Error> {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = SmallBlobDispatcher::new(cfg);
    let oversize = (SMALL_BLOB_THRESHOLD + 1) as u64;
    let d = make_digest(1, oversize);

    let res = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue(
            "endpoint-a",
            /* boot_epoch_id= */ 1,
            "cas",
            d,
            Bytes::from(vec![0u8; oversize as usize]),
        ),
    )
    .await
    .expect("dispatcher.enqueue MUST NOT block on precondition failure");

    let err = res.expect_err("oversize blob MUST be rejected by precondition");
    assert_eq!(
        err.code, Code::InvalidArgument,
        "oversize precondition MUST return InvalidArgument (per plan C9); got {err:?}"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 6 (per plan C11): dispatcher enqueue rejects empty store_id.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_rejects_empty_store_id() -> Result<(), Error> {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = SmallBlobDispatcher::new(cfg);
    let d = make_digest(1, 100);

    let res = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue(
            "endpoint-a",
            /* boot_epoch_id= */ 1,
            /* empty store_id */ "",
            d,
            Bytes::from(vec![0u8; 100]),
        ),
    )
    .await
    .expect("dispatcher.enqueue MUST NOT block on validation failure");

    let err = res.expect_err("empty store_id MUST be rejected per plan C11");
    assert_eq!(
        err.code, Code::InvalidArgument,
        "empty store_id MUST return InvalidArgument; got {err:?}"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 7 (per plan C11): dispatcher enqueue rejects malformed store_id.
// Format must be `[a-z][a-z0-9_]*` per C11.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_rejects_malformed_store_id() -> Result<(), Error> {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = SmallBlobDispatcher::new(cfg);
    let d = make_digest(1, 100);

    for bad in ["1cas", "C", "a-b", "ac.cas", "ac/cas"] {
        let res = tokio::time::timeout(
            DEADLOCK_DETECTOR,
            dispatcher.enqueue(
                "endpoint-a",
                /* boot_epoch_id= */ 1,
                bad,
                d,
                Bytes::from(vec![0u8; 100]),
            ),
        )
        .await
        .expect("dispatcher.enqueue MUST NOT block on validation failure");

        let err = res.expect_err(&format!(
            "malformed store_id {bad:?} MUST be rejected per plan C11"
        ));
        assert_eq!(
            err.code, Code::InvalidArgument,
            "malformed store_id {bad:?} MUST return InvalidArgument; got {err:?}"
        );
    }
    Ok(())
}

// ----------------------------------------------------------------------
// Test 8: SMALL_BLOB_THRESHOLD has the documented value (16 KiB).
// Doc-tying test so future bumps require explicit thought.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn small_blob_threshold_is_16kib() -> Result<(), Error> {
    assert_eq!(
        SMALL_BLOB_THRESHOLD,
        16 * 1024,
        "SMALL_BLOB_THRESHOLD is the doc-stated 16 KiB; \
         changing this must be explicit (it changes which blobs hit the \
         dispatcher vs the streaming path)"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 9 (per plan §"Order of operations" step 2: drainer wiring): a
// successful enqueue against a registered worker MUST result in a
// BatchWriteSmallBlobsRequest being delivered over the worker_tx mpsc
// AND the EphemeralServerSidePin set populated.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_delivers_batch_to_registered_worker() -> Result<(), Error> {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        UpdateForWorker, update_for_worker::Update,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 256 * 1024));
    dispatcher.register_pin_set("cas", pin.clone());

    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker("endpoint-a", /* boot_epoch_id= */ 1, worker_tx);

    let d = make_digest(7, 100);
    let payload = Bytes::from(vec![0xABu8; 100]);
    tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue("endpoint-a", 1, "cas", d, payload.clone()),
    )
    .await
    .expect("enqueue MUST NOT block on registered worker")
    .expect("enqueue MUST succeed");

    // The drainer task is async; wait for the batch to land on the wire.
    let msg = tokio::time::timeout(DEADLOCK_DETECTOR, worker_rx.recv())
        .await
        .expect("dispatcher MUST deliver a batch within deadlock-detector budget — \
                 drainer task missing or worker_tx not wired")
        .expect("worker_rx must yield a message; channel closed unexpectedly");

    let Some(Update::BatchWriteSmallBlobs(batch)) = msg.update else {
        panic!("dispatcher MUST emit Update::BatchWriteSmallBlobs; got {:?}", msg);
    };
    assert_eq!(
        batch.blobs.len(),
        1,
        "single enqueue MUST produce a single-entry batch; got {} entries",
        batch.blobs.len()
    );
    assert_eq!(batch.blobs[0].store_id, "cas");
    assert_eq!(batch.blobs[0].data, payload);

    // Pin set MUST contain the digest while the worker has not yet acked.
    assert!(
        pin.contains(&d),
        "EphemeralServerSidePin MUST hold the digest until worker acks via pinned_mirror_entries"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 10 (per plan §"State machine" + decision #3 zero-window coalesce):
// rapid back-to-back enqueues MUST coalesce into a single batch.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_zero_window_coalesces_pending_into_single_batch() -> Result<(), Error> {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        UpdateForWorker, update_for_worker::Update,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 256 * 1024));
    dispatcher.register_pin_set("cas", pin);

    let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<UpdateForWorker>();
    dispatcher.register_worker("endpoint-a", 1, worker_tx);

    // Enqueue 5 small blobs in quick succession. Per decision #3 the
    // drainer pulls the first item then try_recv-drains all pending into
    // the SAME batch.
    let payload = Bytes::from(vec![0u8; 50]);
    for i in 0..5u8 {
        dispatcher
            .enqueue("endpoint-a", 1, "cas", make_digest(100 + i, 50), payload.clone())
            .await
            .expect("enqueue must succeed");
    }

    // Collect all batches that arrive within the deadlock-detector
    // window. With zero-window coalesce we expect ONE batch with 5
    // entries (the drainer had time to drain pending while the producer
    // was still enqueuing). Worst case (drainer woke between every
    // enqueue) we get 5 batches with 1 entry each, but with zero-window
    // pulling we expect coalescing.
    let mut received: Vec<Update> = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(100), worker_rx.recv()).await {
        if let Some(update) = msg.update {
            received.push(update);
        }
    }

    let total_blobs: usize = received
        .iter()
        .map(|u| match u {
            Update::BatchWriteSmallBlobs(b) => b.blobs.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(
        total_blobs, 5,
        "all 5 enqueued blobs MUST appear in delivered batches; got {} blobs across {} batches",
        total_blobs,
        received.len()
    );
    // Coalesce assertion: at least SOME coalescing happened (we got
    // strictly fewer batches than enqueues).
    assert!(
        received.len() < 5,
        "zero-window coalesce MUST coalesce some pending items; got {} batches for 5 enqueues \
         (no coalescing means try_recv-drain was not implemented)",
        received.len()
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 168.A (task #168 item 1, broadcast wiring): dispatcher's
// `broadcast_pinned_mirror_ack` iterates all registered pin sets and
// each one's binary-search self-filter removes only its own slice.
// This is the production composition of test #3
// (`observe_pinned_mirror_ack_filters_by_store_id`) but exercised via
// the broadcast path that the WorkerApiServer actually calls.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_broadcast_pinned_mirror_ack_routes_to_each_pin_set()
-> Result<(), Error> {
    use std::sync::Arc;

    let cfg = SmallBlobDispatcherConfig::default();
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let cas_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    let ac_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    dispatcher.register_pin_set("cas", cas_pin.clone());
    dispatcher.register_pin_set("ac", ac_pin.clone());

    let d_cas = make_digest(10, 100);
    let d_ac = make_digest(11, 50);
    cas_pin.insert(d_cas, Bytes::from(vec![0u8; 100]))?;
    ac_pin.insert(d_ac, Bytes::from(vec![0u8; 50]))?;

    // Worker reports BOTH digests in its pinned_mirror_entries snapshot.
    // Sorted by store_id ASCII (worker invariant). Broadcast MUST clear
    // both pin sets via the per-store self-filter.
    let mut entries: Vec<MirrorPinEntry> = vec![
        MirrorPinEntry { digest: Some(d_ac.into()), store_id: "ac".to_string() },
        MirrorPinEntry { digest: Some(d_cas.into()), store_id: "cas".to_string() },
    ];
    entries.sort_by(|a, b| a.store_id.cmp(&b.store_id));

    dispatcher.broadcast_pinned_mirror_ack(&entries);

    assert!(
        cas_pin.is_empty(),
        "broadcast MUST drain cas pin (entry was acked); got len={}",
        cas_pin.len()
    );
    assert!(
        ac_pin.is_empty(),
        "broadcast MUST drain ac pin (entry was acked); got len={}",
        ac_pin.len()
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 168.B (task #168 item 1): broadcast on EMPTY entries is a no-op
// (does not lock pin sets, does not warn) — this is the common-case
// fast path.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_broadcast_pinned_mirror_ack_empty_is_noop() -> Result<(), Error> {
    use std::sync::Arc;

    let cfg = SmallBlobDispatcherConfig::default();
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    dispatcher.register_pin_set("cas", pin.clone());
    let d = make_digest(20, 100);
    pin.insert(d, Bytes::from(vec![0u8; 100]))?;

    dispatcher.broadcast_pinned_mirror_ack(&[]);
    assert_eq!(
        pin.len(),
        1,
        "empty broadcast MUST NOT touch any pin set; got len={}",
        pin.len()
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 168.C (task #168 item 6): register_worker followed by
// unregister_worker drops the worker_tx + per-(worker, store) queues.
// After unregister, enqueue for the same `(endpoint, boot_epoch_id)`
// MUST drop silently (no worker_tx → unregistered case).
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_register_then_unregister_worker_clears_state() -> Result<(), Error> {
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        ..SmallBlobDispatcherConfig::default()
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    dispatcher.register_pin_set("cas", pin.clone());

    let (tx, _rx) = mpsc::unbounded_channel();
    dispatcher.register_worker("ep1", 7, tx);

    // Sanity: register-then-enqueue lands in the queue.
    let d1 = make_digest(30, 100);
    let res1 = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue("ep1", 7, "cas", d1, Bytes::from(vec![0u8; 100])),
    )
    .await
    .expect("enqueue MUST NOT block")?;
    let _ = res1;
    assert_eq!(
        dispatcher.dispatched_count(),
        1,
        "first enqueue MUST land in queue while worker is registered"
    );

    // Unregister; the worker_tx and per-(worker, store) queues drop.
    dispatcher.unregister_worker("ep1", 7);

    // Subsequent enqueue MUST drop silently (no worker_tx).
    let d2 = make_digest(31, 100);
    let res2 = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue("ep1", 7, "cas", d2, Bytes::from(vec![0u8; 100])),
    )
    .await
    .expect("enqueue MUST NOT block on unregistered worker")?;
    let _ = res2;
    assert_eq!(
        dispatcher.dispatched_count(),
        1,
        "post-unregister enqueue MUST NOT increment dispatched_count"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Test 11 (per plan §"Concurrency design" + B4 boot_epoch_id keying):
// enqueueing for a worker that has NOT registered (or has a stale
// boot_epoch_id) MUST NOT panic; it MUST log + drop, and the pin set
// MUST NOT grow (server bytes never made it to the worker).
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_for_unregistered_worker_drops_silently() -> Result<(), Error> {
    use std::sync::Arc;

    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
        pin_ttl: Duration::from_secs(10),
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 256 * 1024));
    dispatcher.register_pin_set("cas", pin.clone());
    // NOTE: register_worker NOT called — simulates a worker that has not
    // connected, OR has reconnected with a new boot_epoch_id leaving the
    // stale (endpoint, OLD_boot_epoch_id, store_id) entry orphaned.

    let d = make_digest(50, 100);
    let res = tokio::time::timeout(
        DEADLOCK_DETECTOR,
        dispatcher.enqueue("nonexistent-endpoint", 999, "cas", d, Bytes::from(vec![0u8; 100])),
    )
    .await
    .expect("enqueue MUST NOT block on unregistered worker");
    assert!(
        res.is_ok(),
        "enqueue for unregistered worker MUST drop silently (Ok); got {res:?}. \
         Caller is post-Bazel-ack fire-and-forget — propagating Err to Bazel is wrong"
    );
    assert!(
        pin.is_empty(),
        "pin set MUST NOT grow when bytes never make it to a worker"
    );
    Ok(())
}
