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
// Format must be `[a-zA-Z_][a-zA-Z0-9_]*` per C11 (relaxed from the
// original lowercase-only spec to accept production names like
// `cas_STORE`; see #168).
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_enqueue_rejects_malformed_store_id() -> Result<(), Error> {
    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        max_pending_per_worker: 32,
        max_batch_bytes: 256 * 1024,
        pin_max_bytes: 256 * 1024 * 1024,
    };
    let dispatcher = SmallBlobDispatcher::new(cfg);
    let d = make_digest(1, 100);

    // Note: under the relaxed `[a-zA-Z_][a-zA-Z0-9_]*` regex, `"C"` and
    // `"Cas"` are now valid (single uppercase ASCII letter is fine);
    // we exercise the still-rejected classes: digit-start, embedded
    // hyphen/dot/slash, embedded whitespace, leading dollar.
    for bad in ["1cas", "a-b", "ac.cas", "ac/cas", "cas store", "$cas"] {
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

// ----------------------------------------------------------------------
// Test 12 (TDD per CLAUDE.md, refactor: delete TTL + add explicit
// unpin_on_disconnect): `EphemeralServerSidePin::unpin_on_disconnect`
// MUST clear the entire HashMap AND zero the `total_bytes` accounting.
//
// Production composition: called from the dispatcher's
// `unpin_on_disconnect(endpoint, boot_epoch_id)` method, which is in
// turn called from `WorkerApiServer`'s disconnect-cleanup task once
// the disconnected worker can no longer ack via
// `observe_pinned_mirror_ack`. Without this, every disconnect would
// leak the worker's in-flight pin entries forever — `pin_max_bytes`
// would steadily fill with ghost pins until the dispatcher rejects
// every new admission with `ResourceExhausted`.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn pin_set_unpin_on_disconnect_clears_state() -> Result<(), Error> {
    let pin = EphemeralServerSidePin::new(/* cap= */ 1024);
    let d1 = make_digest(60, 100);
    let d2 = make_digest(61, 200);
    let d3 = make_digest(62, 300);

    pin.insert(d1, Bytes::from(vec![0u8; 100]))?;
    pin.insert(d2, Bytes::from(vec![0u8; 200]))?;
    pin.insert(d3, Bytes::from(vec![0u8; 300]))?;
    assert_eq!(pin.len(), 3);
    assert_eq!(pin.total_bytes(), 600);

    pin.unpin_on_disconnect();

    assert_eq!(
        pin.len(),
        0,
        "unpin_on_disconnect MUST drop every entry; state was not cleared"
    );
    assert_eq!(
        pin.total_bytes(),
        0,
        "unpin_on_disconnect MUST zero total_bytes; accounting was not zeroed"
    );
    assert!(pin.is_empty(), "pin set MUST report is_empty after unpin");
    // Sanity: a fresh insert MUST work after unpin (the cap accounting
    // must not be inadvertently consumed).
    pin.insert(d1, Bytes::from(vec![0u8; 50]))?;
    assert_eq!(pin.total_bytes(), 50);
    Ok(())
}

// ----------------------------------------------------------------------
// Test 13: SmallBlobDispatcher::unpin_on_disconnect for a registered
// pin set MUST clear it AND drop the worker's queues / worker_tx
// (idempotent with unregister_worker call ordering).
//
// NOTE on per-worker push attribution: the per-store pin set is keyed
// by `DigestInfo`, NOT by worker. The dispatcher does NOT track which
// worker pushed which entries. So `unpin_on_disconnect(endpoint, epoch)`
// clears the ENTIRE pin set for every registered store. This is
// intentionally over-broad for the v1 implementation — see TODO in
// the doc comment. For an isolated single-worker disconnect this is
// correct (every in-flight push from that worker is lost regardless);
// for a staggered fleet it temporarily over-clears entries in flight
// to OTHER workers. Tracked for the v2 design.
// ----------------------------------------------------------------------
#[nativelink_test]
async fn dispatcher_unpin_on_disconnect_clears_registered_pin_sets() -> Result<(), Error> {
    use std::sync::Arc;

    let cfg = SmallBlobDispatcherConfig {
        small_blob_mirror_enabled: true,
        ..SmallBlobDispatcherConfig::default()
    };
    let dispatcher = Arc::new(SmallBlobDispatcher::new(cfg));
    let cas_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    let ac_pin = Arc::new(EphemeralServerSidePin::new(/* cap= */ 1024));
    dispatcher.register_pin_set("cas", cas_pin.clone());
    dispatcher.register_pin_set("ac", ac_pin.clone());

    let d_cas = make_digest(70, 100);
    let d_ac = make_digest(71, 50);
    cas_pin.insert(d_cas, Bytes::from(vec![0u8; 100]))?;
    ac_pin.insert(d_ac, Bytes::from(vec![0u8; 50]))?;
    assert_eq!(cas_pin.len(), 1);
    assert_eq!(ac_pin.len(), 1);

    dispatcher.unpin_on_disconnect("ep1", 7);

    assert!(
        cas_pin.is_empty(),
        "dispatcher.unpin_on_disconnect MUST clear cas pin set"
    );
    assert!(
        ac_pin.is_empty(),
        "dispatcher.unpin_on_disconnect MUST clear ac pin set"
    );
    assert_eq!(cas_pin.total_bytes(), 0);
    assert_eq!(ac_pin.total_bytes(), 0);
    Ok(())
}

// ----------------------------------------------------------------------
// Test 14 (perf-optimizer #153 MAJOR + red-team B4): `remove_one` MUST
// update `total_bytes` while still holding the state lock, OR fold the
// fetch_sub into the same critical section as the HashMap mutation.
// Otherwise a concurrent `len()` / `total_bytes()` reader can observe
// `len = 0` AND `total_bytes != 0` (or vice-versa), and a concurrent
// re-insert can race the late `fetch_sub` with the prior `total_bytes`
// snapshot used by the cap check, producing a spurious cap rejection
// or — worse — admit a payload that puts the actual byte total above
// `cap`.
//
// Functional regression check: after a sequence of insert + remove,
// `len()` and `total_bytes()` MUST be CONSISTENT (insert-of-N then
// remove-of-same-key MUST return total_bytes to its pre-insert value).
// ----------------------------------------------------------------------
#[nativelink_test]
async fn pin_set_remove_one_keeps_len_and_total_bytes_consistent() -> Result<(), Error> {
    let pin = EphemeralServerSidePin::new(/* cap= */ 1024);
    let d = make_digest(80, 100);

    pin.insert(d, Bytes::from(vec![0u8; 100]))?;
    assert_eq!(pin.len(), 1);
    assert_eq!(pin.total_bytes(), 100);

    pin.remove_one(&d);
    assert_eq!(pin.len(), 0, "len MUST reflect removal");
    assert_eq!(
        pin.total_bytes(),
        0,
        "total_bytes MUST be zeroed atomically with len"
    );

    // Re-insert MUST succeed (no leftover stale total_bytes).
    pin.insert(d, Bytes::from(vec![0u8; 100]))?;
    assert_eq!(pin.len(), 1);
    assert_eq!(pin.total_bytes(), 100);
    Ok(())
}

// ----------------------------------------------------------------------
// Test 15 (testing-czar #168 MAJOR-2 — concurrent race coverage):
// the existing single-threaded test only verifies the FUNCTIONAL property
// (insert-then-remove restores total_bytes), it would PASS with the
// buggy `drop(state); fetch_sub(...)` ordering because no concurrent
// reader is ever scheduled in between. This concurrent test exercises
// the lock-held atomic-update fix at the production-realistic scale.
//
// The strict invariant under attack: `EphemeralServerSidePin::insert`
// uses `total_bytes.store(projected, ...)` (overwrite, not fetch_add)
// for the cap-check / accounting update, while `remove_one` uses
// `fetch_sub`. With the LOCK-HELD fix in place, every modification of
// state is ATOMIC with the matching atomic update — so for any digest
// d the sequence (insert d → remove d) leaves total_bytes at its
// pre-insert value, even under concurrent contention.
//
// With the BUGGY ordering (`drop(state); fetch_sub(...)`), the
// following racing interleaving corrupts accounting:
//   T0: state=[d→100], atomic=100
//   T1: thread A: state.remove(d) → state=[], atomic still 100
//   T2: thread A drops state lock
//   T3: thread B: insert(d, 100): takes lock, current_total=100,
//                projected=100, atomic.store(100), state=[d→100]
//   T4: thread A: fetch_sub(100) → atomic = 0, but state=[d→100].
// The accounting has now drifted: state holds 100 bytes but
// total_bytes reports 0. After the post-test drain sweep removes
// the leaked d, fetch_sub will underflow (u64 wrap), producing a
// visibly-wrong final total_bytes that is neither 0 nor the actual
// state byte sum.
//
// Test design: many concurrent inserters and removers contend on the
// SAME (i, j) digest space so insert-then-remove on the same key
// happens often. After all tasks finish, a final sweep removes every
// digest. The invariant: total_bytes() MUST equal exactly the sum of
// bytes still held in state (here: 0, because every insert was
// paired with a remove sweep). Under the bug, total_bytes drifts
// below 0 (u64 wrap) and the assertion fires with the specific
// "(len, total_bytes) torn pair detected" message.
//
// Mutation step (per CLAUDE.md TDD step 5): in
// `EphemeralServerSidePin::remove_one`, change
//
//     let mut state = self.state.lock();
//     if let Some(data) = state.remove(digest) {
//         let removed = data.len() as u64;
//         self.total_bytes.fetch_sub(removed, Ordering::AcqRel);
//     }
//
// to the buggy version
//
//     let mut state = self.state.lock();
//     if let Some(data) = state.remove(digest) {
//         let removed = data.len() as u64;
//         drop(state);
//         self.total_bytes.fetch_sub(removed, Ordering::AcqRel);
//     }
//
// This test MUST then panic with "(len, total_bytes) torn pair detected".
// ----------------------------------------------------------------------
#[nativelink_test(flavor = "multi_thread", worker_threads = 4)]
async fn pin_set_concurrent_insert_remove_no_torn_pair() -> Result<(), Error> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const PAYLOAD_SIZE: usize = 100;
    const CAP: u64 = 64 * 1024 * 1024; // ample headroom — we are not testing cap.
    const INSERTERS: u8 = 16;
    const REMOVERS: u8 = 16;
    // 16 inserters × 64 iters × 16 (key-space sharing) → ~16k touches
    // per worker pair, well above the ~10k threshold the user requested
    // for race exposure.
    const ITERS_PER_WORKER: u64 = 64;

    let pin = Arc::new(EphemeralServerSidePin::new(CAP));
    let stop = Arc::new(AtomicBool::new(false));
    let payload = Bytes::from(vec![0u8; PAYLOAD_SIZE]);

    // Each inserter / remover shares the same digest key-space so
    // insert and remove RACE on the SAME key (where the
    // `drop(state); fetch_sub` bug fires).
    let mut inserter_handles = Vec::with_capacity(INSERTERS as usize);
    for i in 0..INSERTERS {
        let pin = pin.clone();
        let payload = payload.clone();
        let stop = stop.clone();
        inserter_handles.push(tokio::spawn(async move {
            for iter in 0..ITERS_PER_WORKER {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                for k in 0..16u64 {
                    let d = make_digest(i, k);
                    drop(pin.insert(d, payload.clone()));
                }
                if iter.is_multiple_of(8) {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    let mut remover_handles = Vec::with_capacity(REMOVERS as usize);
    for i in 0..REMOVERS {
        let pin = pin.clone();
        let stop = stop.clone();
        remover_handles.push(tokio::spawn(async move {
            for iter in 0..ITERS_PER_WORKER {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                for k in 0..16u64 {
                    let d = make_digest(i, k);
                    pin.remove_one(&d);
                }
                if iter.is_multiple_of(8) {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    // 5s deadlock detector wraps the entire concurrent workload.
    tokio::time::timeout(DEADLOCK_DETECTOR, async {
        for h in inserter_handles {
            h.await.expect("inserter task must not panic");
        }
        for h in remover_handles {
            h.await.expect("remover task must not panic");
        }
        // Drain any insert that landed AFTER its matched remove. After
        // this sweep the pin set MUST be empty AND total_bytes MUST
        // be 0 — the strict invariant the lock-held update fix
        // protects.
        for i in 0..INSERTERS {
            for k in 0..16u64 {
                pin.remove_one(&make_digest(i, k));
            }
        }
        stop.store(true, Ordering::Relaxed);
    })
    .await
    .expect(
        "concurrent insert/remove workload deadlocked — \
         5s deadlock-detector timeout fired",
    );

    // Final consistency: pin set is empty, total_bytes is 0. Under the
    // buggy `drop(state); fetch_sub` ordering, repeated insert/remove
    // races on the same key cause atomic underflow (u64 wrap) and/or
    // accounting drift, producing a final total_bytes that is neither
    // 0 nor matches the state byte sum.
    let final_len = pin.len();
    let final_total = pin.total_bytes();
    assert_eq!(
        final_len, 0,
        "final state: len MUST be 0 after the post-loop drain sweep; got len={final_len}, \
         total_bytes={final_total}"
    );
    assert_eq!(
        final_total, 0,
        "(len, total_bytes) torn pair detected: final total_bytes={final_total} after every \
         insert key was swept by remove (state len={final_len}) — accounting has drifted, \
         the lock-held atomic-update fix in remove_one has regressed (the \
         `drop(state); fetch_sub` race causes u64 underflow / accounting drift)"
    );

    Ok(())
}
