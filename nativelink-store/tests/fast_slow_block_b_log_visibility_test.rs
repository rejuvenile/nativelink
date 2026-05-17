// Copyright 2024-2025 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.

//! #247/#477 BLOCK-B log-visibility regression test.
//!
//! The BLOCK-B per-digest `Notify` wait in
//! [`FastSlowStore::get_part`] (fast_slow_store.rs:6049-6105) is the
//! reader-cascade safety net that blocks `get_part` callers while a
//! chunked commit is mid-flight, then falls through to the slow tier
//! once the in-flight set drains. Its emissions were previously
//! `debug!` and therefore stripped by the workspace
//! `release_max_level_info` feature in release builds — production
//! journals showed ZERO BLOCK-B output even when the wait was firing.
//! Operators (and design reviewers asking "is BLOCK-B firing today?")
//! had no way to answer the question without redeploying with elevated
//! log level.
//!
//! This test exercises the BLOCK-B wait via direct manipulation of the
//! `chunked_in_flight_digests` map (the same primitive
//! `InFlightChunkedGuard::new`/`Drop` operate on, exposed via
//! `chunked_in_flight_digests_handle()`), then asserts the new
//! `INFO`-level entry emission lands in the tracing-test buffer.
//!
//! Production composition: real `FastSlowStore` wrapping a real
//! `MemoryStore` fast tier and a real on-disk `FilesystemStore` slow
//! tier (per CLAUDE.md "Test in production composition, not in
//! isolation"). The reader path traverses the production
//! `get_part` BLOCK-B branch end-to-end.
//!
//! Mutation step: in `fast_slow_store.rs` revert the new
//! `info!(..., "fast_slow get_part: digest is in chunked_in_flight_digests; \
//! blocking reader on per-digest Notify ...")` back to `debug!`. This
//! test red-fails with the bespoke message
//! `"BLOCK-B entry must emit at INFO level — regression to debug! \
//! would be stripped by release_max_level_info"`.

#![cfg(feature = "chunked_fast_slow")]

use core::num::NonZeroU32;
use core::time::Duration;
use std::sync::Arc;

use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntryImpl, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    a
}

/// Build a real on-disk `FilesystemStore` for the slow tier. The
/// BLOCK-B wait exits via fall-through to the slow tier; using a real
/// FilesystemStore here keeps the test crossing the same seam as
/// production (`tank/casdata/nativelink/stores/...`) rather than a
/// `MemoryStore` stand-in that bypasses the rename-into-canonical
/// path.
async fn make_real_filesystem_slow() -> Arc<FilesystemStore<FileEntryImpl>> {
    let base = std::env::var("TEST_TMPDIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_str().unwrap().to_string());
    let nonce: u64 = rand::random();
    let content_path = format!("{base}/{nonce}/block-b-log-slow/content");
    let temp_path = format!("{base}/{nonce}/block-b-log-slow/temp");
    FilesystemStore::<FileEntryImpl>::new(&FilesystemSpec {
        content_path,
        temp_path,
        eviction_policy: None,
        block_size: 1,
        ..Default::default()
    })
    .await
    .expect("FilesystemStore::new must succeed")
}

/// #247/#477 (entry log promotion): the BLOCK-B subscription entry
/// MUST emit at `INFO` level so it survives the
/// `release_max_level_info` strip in release builds. Without this
/// emission, production journals cannot answer "is BLOCK-B firing
/// today?" — a load-bearing input to the Option-C run_producer wait
/// design.
///
/// Composition: `FastSlowStore { fast: MemoryStore, slow:
/// FilesystemStore }` with `chunked_reads_enabled = true`. We
/// pre-populate the slow tier with the bytes, then manually insert
/// the digest into `chunked_in_flight_digests` with a per-digest
/// `Notify` (mirroring `InFlightChunkedGuard::new`). A reader is
/// spawned; after a yield-burst that parks it on `notified.await`, we
/// drain the map and fire `notify_waiters()` (mirroring the
/// `InFlightChunkedGuard::Drop` semantics). The reader wakes, falls
/// through to the slow tier, and returns the bytes. The
/// `tracing-test` buffer is then asserted to contain a line at
/// `INFO` level with the BLOCK-B entry-message substring.
///
/// 5 s deadlock detector wraps the reader.
#[nativelink_test]
async fn block_b_entry_emits_at_info_level() {
    let payload: Vec<u8> = (0..2048u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);

    let fast_store = MemoryStore::new(&MemorySpec::default());
    let slow_store = make_real_filesystem_slow().await;
    let slow_store_wrapped = Store::new(slow_store.clone());

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: true,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast_store),
        slow_store_wrapped.clone(),
    );

    // Pre-populate the slow tier directly so the BLOCK-B
    // fall-through-to-slow path has bytes to serve once the wait
    // releases. `update_oneshot` goes through the
    // FilesystemStore's rename-into-canonical-path; the
    // `evicting_map` is updated as part of the rename, so the
    // subsequent `get_part` observes the bytes.
    slow_store_wrapped
        .as_pin()
        .update_oneshot(digest, payload.clone().into())
        .await
        .expect("slow-tier write must succeed");

    // Insert the digest into `chunked_in_flight_digests` with a
    // per-digest `Notify`. This mirrors what `InFlightChunkedGuard::
    // new` does without taking the cross-crate dependency on
    // `nativelink-service`. We must keep an `Arc` of the `Notify` so
    // we can fire `notify_waiters()` from the drainer.
    let in_flight_map = fss.chunked_in_flight_digests_handle();
    let per_digest_notify = Arc::new(Notify::new());
    {
        let mut guard = in_flight_map.lock();
        guard.insert(
            digest,
            (
                NonZeroU32::new(1).expect("1 is non-zero"),
                Arc::clone(&per_digest_notify),
            ),
        );
    }

    // Barrier rendezvous: the drainer waits until the test body has
    // (a) spawned the reader, (b) yielded enough times for the reader
    // to register its `notified.await`. CLAUDE.md "No `sleep` as
    // synchronization" — barrier is the primitive.
    let drainer_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let drainer_barrier_for_task = Arc::clone(&drainer_barrier);
    let in_flight_for_drain = Arc::clone(&in_flight_map);
    let notify_for_drain = Arc::clone(&per_digest_notify);
    let drainer = tokio::spawn(async move {
        drainer_barrier_for_task.wait().await;
        // Remove from map FIRST, then fire `notify_waiters()` AFTER
        // — mirrors the documented `InFlightChunkedGuard::Drop`
        // ordering so the reader's re-check of `contains_key`
        // observes the drained state.
        {
            let mut guard = in_flight_for_drain.lock();
            guard.remove(&digest);
        }
        notify_for_drain.notify_waiters();
    });

    // Spawn the reader. It enters `FastSlowStore::get_part`, sees the
    // digest in `chunked_in_flight_digests`, emits the new `INFO`
    // BLOCK-B entry log, and parks on the per-digest `Notify`.
    let fss_store = Store::new(fss.clone());
    let reader = tokio::spawn(async move {
        fss_store.get_part_unchunked(digest, 0, None).await
    });

    // Cooperative yields let the spawned reader reach the await.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    // Release the drainer — it removes the entry and fires the
    // per-digest notify; the reader wakes and falls through to the
    // slow tier.
    drainer_barrier.wait().await;

    let bytes_read = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect(
            "BLOCK-B reader must complete within 5 s — the per-digest Notify \
             wait must wake on the drainer's `notify_waiters()` and fall through \
             to the slow tier where the bytes are pre-populated",
        )
        .expect("reader task panicked")
        .expect(
            "BLOCK-B reader must return Ok bytes — fall-through to the slow \
             tier with bytes pre-populated must succeed",
        );

    tokio::time::timeout(Duration::from_secs(5), drainer)
        .await
        .expect("drainer task must terminate")
        .expect("drainer task must not panic");

    assert_eq!(
        bytes_read.len(),
        payload.len(),
        "BLOCK-B reader returned wrong byte count ({} vs {})",
        bytes_read.len(),
        payload.len(),
    );
    assert_eq!(
        bytes_read.as_ref(),
        payload.as_slice(),
        "BLOCK-B reader returned wrong bytes (corruption?)",
    );

    // Load-bearing assertion: the BLOCK-B entry log MUST appear at
    // `INFO` level. `tracing-test` captures all events at all levels
    // in tests (`release_max_level_info` is INACTIVE in test builds),
    // so we MUST filter on the level prefix to detect a regression
    // back to `debug!` — otherwise a `debug!` would still match the
    // substring search and the mutation step would silently pass.
    // See `ac_pin_registry::cap_exceeded_emits_rate_limited_warn`
    // for the canonical pattern this follows.
    logs_assert(|lines: &[&str]| {
        let n = lines
            .iter()
            .filter(|l| {
                l.contains(" INFO ")
                    && l.contains(
                        "fast_slow get_part: digest is in chunked_in_flight_digests",
                    )
            })
            .count();
        if n == 0 {
            Err("BLOCK-B entry must emit at INFO level — regression to debug! \
                 would be stripped by release_max_level_info"
                .to_string())
        } else {
            Ok(())
        }
    });
}

/// **OVER-ACTION coverage (testing-czar M2 + auditor MAJOR-1 follow-up):**
/// BLOCK-B's entry `info!` MUST NOT fire when the digest is NOT in
/// `chunked_in_flight_digests`. Per CLAUDE.md "Asymmetric contract
/// coverage", every state-mutating side-effect (including log
/// emissions) has two failure modes:
///   - **Under-action:** info! doesn't fire when expected (covered
///     by `block_b_entry_emits_at_info_level` above).
///   - **Over-action:** info! fires too often or in the wrong branch
///     (this test).
///
/// A regression that hoisted the `info!` ABOVE the `if let Some(notify)`
/// guard, or that emitted it inside a polling loop, would still satisfy
/// the under-action test (extra emissions match the substring filter)
/// but would FLOOD the log at line rate for every reader. This test
/// drives a `get_part_unchunked` where the digest is pre-populated in
/// the slow tier but NEVER inserted into `chunked_in_flight_digests`;
/// the BLOCK-B branch must NOT be taken, and ZERO `info!` lines with
/// the BLOCK-B entry marker must appear for this digest.
///
/// **Mutation step:** hoist the `info!(...)` above the `if let Some(...)`
/// (or wrap it in an `else` arm that fires when the map miss happens)
/// — this test red-fails with the bespoke message.
#[nativelink_test]
async fn block_b_no_entry_emit_when_digest_not_in_flight() {
    // Use a distinctive digest byte so the substring filter is unique
    // and cross-test pollution cannot match.
    let payload: Vec<u8> = (100..1124u32).map(|i| (i & 0xFF) as u8).collect();
    let digest = DigestInfo::new(sha256(&payload), payload.len() as u64);
    let digest_disc = format!("{digest:?}");

    let fast_store = MemoryStore::new(&MemorySpec::default());
    let slow_store = make_real_filesystem_slow().await;
    let slow_store_wrapped = Store::new(slow_store.clone());

    let fss = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Filesystem(FilesystemSpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: true,
            slow_writes_in_flight_max_bytes: 0,
        },
        Store::new(fast_store),
        slow_store_wrapped.clone(),
    );

    // Pre-populate slow tier ONLY. Do NOT insert into
    // `chunked_in_flight_digests` — that's the over-action precondition.
    slow_store_wrapped
        .as_pin()
        .update_oneshot(digest, payload.clone().into())
        .await
        .expect("slow-tier write must succeed");

    // Drive a normal get_part. BLOCK-B's branch should NOT fire (map
    // miss); the read falls through to the slow tier directly.
    let fss_store = Store::new(fss.clone());
    let bytes_read = tokio::time::timeout(
        Duration::from_secs(5),
        fss_store.get_part_unchunked(digest, 0, None),
    )
    .await
    .expect("must not deadlock — over-action negative test must complete promptly")
    .expect("over-action negative-path reader must return Ok bytes from slow tier");

    assert_eq!(
        bytes_read.as_ref(),
        payload.as_slice(),
        "over-action test reader returned wrong bytes",
    );

    // ZERO INFO lines with the BLOCK-B entry marker AND this digest's
    // discriminator. Filtering on digest byte makes this assertion
    // pollution-proof: even if another test in this binary inserts
    // some other digest into `chunked_in_flight_digests` and fires
    // the BLOCK-B emit, that line wouldn't carry our digest's hex
    // prefix.
    logs_assert(move |lines: &[&str]| {
        let phantom_matches: Vec<&&str> = lines
            .iter()
            .filter(|l| {
                l.contains(" INFO ")
                    && l.contains(
                        "fast_slow get_part: digest is in chunked_in_flight_digests",
                    )
                    && l.contains(&digest_disc)
            })
            .collect();
        if phantom_matches.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "over-action regression: BLOCK-B entry info! fired for a \
                 digest NOT in chunked_in_flight_digests. The emit must be \
                 inside the `if let Some(notify) = notify_handle` arm. \
                 Found {} phantom line(s); first = {:?}",
                phantom_matches.len(),
                phantom_matches.first()
            ))
        }
    });
}
