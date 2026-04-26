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
