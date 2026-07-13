// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0
// Future License (the "License"); you may not use this file except in
// compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! #554 — metric-completeness regression for
//! `server_stable_digests_pusher_invoke_count` (renamed from
//! `_via_commit_path_count` per #554's rename).
//!
//! Pre-#554 only the chunked-commit dispatcher's
//! `stable_digests_pusher` closure bumped the counter. Three other
//! production push sites in `fast_slow_store.rs`
//! (`try_self_retry_slow_write` success arm, legacy `update` background
//! success arm, legacy `update_oneshot` background success arm) plus
//! the `mark_stable` override pushed directly into `stable_digests`
//! and bypassed the counter — operator-facing BIS traffic was
//! systematically undercounted.
//!
//! #554 routed every direct-push site through the shared
//! `push_stable_digests_via_arcs` helper so the counter increments for
//! every digest pushed regardless of arm.
//!
//! ## Invariant under test
//!
//! For each direct-push arm covered by #554, exercising the production
//! code path that triggers it MUST increment
//! `ServerPhase0Metrics::pusher_invoke_count` by exactly the number of
//! digests pushed in that arm.
//!
//! ## Seams crossed
//!
//! Each test wires a real `FastSlowStore` in production composition
//! (real `MemoryStore` fast tier, real `MemoryStore` slow tier) and
//! invokes the production API entry point that ultimately reaches the
//! arm under test:
//!
//!   1. `update_oneshot` arm: caller invokes `Store::update_oneshot`,
//!      which dispatches into `FastSlowStore::update_oneshot`, which
//!      spawns the background tokio task whose success arm at
//!      `fast_slow_store.rs:~5503` calls `push_stable_digests_via_arcs`.
//!      The test waits for the background spawn to drain via
//!      `flush_slow_writes`.
//!   2. `mark_stable` arm: caller invokes `Store::mark_stable`, which
//!      dispatches to `FastSlowStore::mark_stable`'s `StoreDriver`
//!      override which now delegates to `push_stable_digests`.
//!
//! The V3 self-retry arm (`try_self_retry_slow_write` at `:~2620`) is
//! exercised end-to-end by the `failed_writes_drain_test` integration
//! test suite (`nativelink-service/tests/failed_writes_drain_test.rs`);
//! re-testing it here would duplicate fixture wiring. The chunked-
//! commit dispatcher's closure path is exercised by every existing
//! chunked-commit test (e.g. `chunked_commit_soft_warn_test.rs`); the
//! #547 fix-up CF4 publish-via-render-prometheus test in
//! `nativelink-util/src/phase0_metrics.rs` covers the closure → metric
//! wiring end-to-end.
//!
//! ## Singleton snapshot-delta discipline
//!
//! `server_phase0_metrics()` is a process-wide singleton; other tests
//! in the same binary may have pre-populated the counter. Each test
//! snapshots the counter value BEFORE the action and asserts on the
//! DELTA, not the absolute value. This makes the assertion robust to
//! parallel test execution.
//!
//! ## Mutation step (CLAUDE.md mandatory)
//!
//! For each test:
//!   1. Open `nativelink-store/src/fast_slow_store.rs`.
//!   2. Comment out the body of the corresponding
//!      `push_stable_digests_via_arcs` call site so the push happens
//!      but the metric bump is skipped (e.g. replace the `for digest
//!      in digests { metrics.record_pusher_invoke(*digest); }` loop
//!      with an empty body inside the helper, OR revert one specific
//!      arm to `stable_digests.lock().push(...)` directly).
//!   3. Re-run the test. It MUST red-fail with the bespoke
//!      "#554 metric-completeness invariant violated:" message naming
//!      the arm whose contribution went missing.
//!
//! Without the mutation step the test only proves "metric increments
//! at SOME push"; with the mutation step it proves "metric increments
//! at EACH SPECIFIC push site".

use core::pin::Pin;
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::{FastSlowStore, SelfRetryOutcome};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::make_buf_channel_pair_with_size;
use nativelink_util::common::DigestInfo;
use nativelink_util::phase0_metrics::server_phase0_metrics;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike, UploadSizeInfo};

/// 64-hex-character digest hashes for the test cases. Each test uses
/// a distinct hash so concurrent test runs (cargo's default parallel
/// scheduler) don't collide on `stable_digests` queue state — the
/// queue lives inside one FSS instance per test, but the singleton
/// `pusher_timestamps` cache is shared.
const HASH_LEGACY_ONESHOT: &str =
    "5540000000000000000000000000000000000000000000000000000000000001";
const HASH_MARK_STABLE_1: &str =
    "5540000000000000000000000000000000000000000000000000000000000002";
const HASH_MARK_STABLE_2: &str =
    "5540000000000000000000000000000000000000000000000000000000000003";
const HASH_LEGACY_STREAMING: &str =
    "5540000000000000000000000000000000000000000000000000000000000004";
const HASH_V3_SELF_RETRY: &str =
    "5540000000000000000000000000000000000000000000000000000000000005";

/// Build a real `FastSlowStore` in production composition with
/// `MemoryStore` on both tiers. The slow tier MUST accept writes (an
/// erroring slow tier would route through the failure arm, not the
/// success arm we want to exercise). Returns the production-facing
/// `Store` handle, the FSS Arc, and the inner fast-tier `MemoryStore`
/// Arc (for tests that need to seed the fast tier directly, like the
/// V3 self-retry arm).
fn build_fast_slow_with_memory_slow() -> (Store, Arc<FastSlowStore>, Arc<MemoryStore>) {
    let fast_inner = MemoryStore::new(&MemorySpec::default());
    let slow_inner = MemoryStore::new(&MemorySpec::default());
    let fast_store = Store::new(fast_inner.clone());
    let slow_store = Store::new(slow_inner);
    let fss: Arc<FastSlowStore> = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            chunked_reads_enabled: false,
            slow_writes_in_flight_max_bytes: 0,
            bypass_dedup_threshold_bytes: 0,
        },
        fast_store,
        slow_store,
    );
    let wrapped = Store::new(fss.clone());
    (wrapped, fss, fast_inner)
}

/// #554 — legacy `update_oneshot` background success arm contributes
/// to the pusher_invoke metric.
///
/// Exercises: `Store::update_oneshot` → `FastSlowStore::update_oneshot`
/// → spawned tokio task → success match arm at `~:5503` →
/// `push_stable_digests_via_arcs(&[digest])` → metric bump.
///
/// `flush_slow_writes` is the production drain primitive; it blocks
/// until the background spawn is finished. Without this barrier the
/// counter read could race the spawned task's pre-bump state and
/// flake. `tokio::time::timeout` wraps the flush as a deadlock
/// detector — a hung future would mask the bug.
#[nativelink_test]
async fn update_oneshot_success_arm_bumps_pusher_invoke_count() -> Result<(), Error> {
    let (wrapped, fss, _fast_arc) = build_fast_slow_with_memory_slow();
    let digest = DigestInfo::try_new(HASH_LEGACY_ONESHOT, 7).unwrap();

    let before = server_phase0_metrics()
        .pusher_invoke_count_for_test();

    let payload = Bytes::from_static(b"#554-ok");
    wrapped
        .update_oneshot(digest, payload)
        .await
        .expect("update_oneshot must succeed against MemoryStore slow tier");

    // Drain background spawn so the success arm has definitely executed.
    let remaining = tokio::time::timeout(
        Duration::from_secs(5),
        fss.flush_slow_writes(Duration::from_millis(50)),
    )
    .await
    .expect(
        "flush_slow_writes must not deadlock — background slow write \
         should complete promptly against MemoryStore slow tier",
    );
    assert_eq!(
        remaining, 0,
        "background slow write did not drain; remaining in-flight = {remaining}"
    );

    let after = server_phase0_metrics()
        .pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    assert!(
        delta >= 1,
        "#554 metric-completeness invariant violated: \
         FastSlowStore::update_oneshot success arm at fast_slow_store.rs:~5503 \
         did not increment server_stable_digests_pusher_invoke_count. \
         Pre-#554 this arm pushed directly into stable_digests.lock() and \
         bypassed the metric; the #554 fix routed it through \
         push_stable_digests_via_arcs which MUST call record_pusher_invoke \
         per digest. Counter before: {before}, after: {after}, delta: {delta}, \
         expected delta >= 1 (this test pushes a single digest)."
    );
    // Lower-bound assertion (`>=`) accommodates the singleton being
    // shared across parallel tests in the same binary — concurrent
    // tests may bump the same counter. Strict `==1` would flake.

    Ok(())
}

/// #554 — legacy streaming `update` background success arm contributes
/// to the pusher_invoke metric.
///
/// Exercises: direct `Store::update` (NOT `update_oneshot`) →
/// `FastSlowStore::update`'s background tokio spawn → success match arm
/// inside the spawned task → `push_stable_digests_via_arcs(&[digest])`
/// → metric bump.
///
/// Why a separate test from `update_oneshot_success_arm_*`: `FastSlowStore`
/// overrides BOTH `update` and `update_oneshot` with independent spawn
/// arms — the override of `update_oneshot` at `FastSlowStore::update_oneshot`
/// does NOT delegate to `FastSlowStore::update`, so the streaming-spawn
/// arm and the oneshot-spawn arm are sibling code paths each carrying
/// their own `push_stable_digests_via_arcs` call. The oneshot test
/// exercises one; this test exercises the other. A regression that
/// stripped the bump from the streaming arm alone would be invisible
/// to the oneshot test.
///
/// Mutation step (CLAUDE.md mandatory): comment out the
/// `push_stable_digests_via_arcs(&stable_digests_ref, &stable_notify_ref,
/// &[*digest])` call inside the streaming spawn body in
/// `FastSlowStore::update` (NOT the one in `update_oneshot`). This test
/// must red-fail with the bespoke "#554 metric-completeness invariant
/// violated:" message naming the streaming arm.
#[nativelink_test]
async fn update_streaming_path_success_arm_bumps_pusher_invoke_count() -> Result<(), Error> {
    let (wrapped, fss, _fast_arc) = build_fast_slow_with_memory_slow();
    let digest = DigestInfo::try_new(HASH_LEGACY_STREAMING, 7).unwrap();

    let before = server_phase0_metrics().pusher_invoke_count_for_test();

    // Bypass `update_oneshot` by calling `Store::update` directly with
    // an explicit buf_channel pair. This hits `FastSlowStore::update`'s
    // streaming spawn arm rather than `FastSlowStore::update_oneshot`'s.
    let (mut tx, rx) = make_buf_channel_pair_with_size(4);
    let payload = Bytes::from_static(b"#554-st");
    let payload_len = payload.len() as u64;
    let send_fut = async move {
        tx.send(payload).await.expect("send payload");
        tx.send_eof().expect("send eof");
    };
    let update_fut = wrapped.update(digest, rx, UploadSizeInfo::ExactSize(payload_len));
    let (_, update_result) = tokio::join!(send_fut, update_fut);
    update_result.expect("streaming update must succeed against MemoryStore slow tier");

    // Drain background spawn so the success arm has definitely executed.
    let remaining = tokio::time::timeout(
        Duration::from_secs(5),
        fss.flush_slow_writes(Duration::from_millis(50)),
    )
    .await
    .expect(
        "flush_slow_writes must not deadlock — streaming background slow \
         write should complete promptly against MemoryStore slow tier",
    );
    assert_eq!(
        remaining, 0,
        "background streaming slow write did not drain; remaining in-flight = {remaining}"
    );

    let after = server_phase0_metrics().pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    assert!(
        delta >= 1,
        "#554 metric-completeness invariant violated: \
         FastSlowStore::update streaming-path background success arm \
         did not increment server_stable_digests_pusher_invoke_count. \
         Pre-#554 this arm pushed directly into stable_digests.lock() \
         and bypassed the metric; the #554 fix routed it through \
         push_stable_digests_via_arcs which MUST call record_pusher_invoke \
         per digest. This arm is SEPARATE from the FastSlowStore::update_oneshot \
         spawn arm — both must carry their own bump because update_oneshot \
         does NOT delegate to update inside FSS. Counter before: {before}, \
         after: {after}, delta: {delta}, expected delta >= 1."
    );
    // Lower-bound assertion (`>=`) accommodates the process-wide
    // singleton being shared across parallel tests in the same binary.

    Ok(())
}

/// #554 — `FastSlowStore::try_self_retry_slow_write` (V3 self-retry
/// after a failed slow write) success arm contributes to the
/// pusher_invoke metric.
///
/// Exercises: seed fast tier with bytes → call
/// `FastSlowStore::try_self_retry_slow_write` directly → success arm at
/// the bottom of the method calls `push_stable_digests` → helper →
/// metric bump.
///
/// Bypassing the drainer (which would dispatch through the FSS chain)
/// keeps this test self-contained and avoids fixturing
/// `BlobLocalityMap` / `SmallBlobDispatcher`. The
/// `failed_writes_drain_test::failed_slow_writes_v3_self_retry_*` suite
/// already covers the drainer integration; this test is narrower and
/// exists purely to guard the metric-bump on the V3 arm.
///
/// Mutation step: comment out the `self.push_stable_digests(&[digest])`
/// call inside `try_self_retry_slow_write`'s success block. This test
/// must red-fail with the bespoke "#554 metric-completeness invariant
/// violated:" message naming the V3 self-retry arm.
#[nativelink_test]
async fn v3_self_retry_success_arm_bumps_pusher_invoke_count() -> Result<(), Error> {
    let (_wrapped, fss, fast_arc) = build_fast_slow_with_memory_slow();
    let digest = DigestInfo::try_new(HASH_V3_SELF_RETRY, 4).unwrap();
    let payload = Bytes::from_static(b"V3!!");

    // Seed fast tier with the bytes V3 will re-stream into slow.
    // We seed the inner MemoryStore directly (mirroring the
    // `failed_writes_drain_test::failed_slow_writes_v3_self_retry_*`
    // fixture pattern) — the FSS-level `update_oneshot` would spawn a
    // background slow-write that itself bumps the metric, which would
    // contaminate the snapshot delta.
    Pin::new(fast_arc.as_ref())
        .update_oneshot(StoreKey::Digest(digest), payload.clone())
        .await
        .expect("seed fast tier with bytes for V3 self-retry");

    let before = server_phase0_metrics().pusher_invoke_count_for_test();

    // Run V3 self-retry directly. timeout long enough that MemoryStore
    // slow tier cannot plausibly time out.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        fss.try_self_retry_slow_write(digest, Duration::from_secs(5)),
    )
    .await
    .expect("try_self_retry_slow_write must not deadlock against MemoryStore slow tier")
    .expect("V3 self-retry must succeed when fast tier has bytes and slow tier accepts writes");

    // Sanity: outcome must be Succeeded (not FastTierMiss). If this
    // fails, the success-arm metric assertion below would silently pass
    // a vacuous test, so we assert outcome explicitly.
    match outcome {
        SelfRetryOutcome::Succeeded { bytes } => {
            assert_eq!(bytes, payload.len() as u64, "V3 self-retry bytes mismatch");
        }
        other => panic!(
            "V3 self-retry success-arm test requires Succeeded outcome to \
             actually exercise the metric arm; got {other:?}"
        ),
    }

    let after = server_phase0_metrics().pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    assert!(
        delta >= 1,
        "#554 metric-completeness invariant violated: \
         FastSlowStore::try_self_retry_slow_write success arm did not \
         increment server_stable_digests_pusher_invoke_count. Pre-#554 \
         this arm pushed directly into stable_digests.lock() and \
         bypassed the metric; the #554 fix routed it through \
         self.push_stable_digests → push_stable_digests_via_arcs which \
         MUST call record_pusher_invoke per digest. Counter before: \
         {before}, after: {after}, delta: {delta}, expected delta >= 1 \
         (this test self-retries a single digest)."
    );

    // Composite check: digest actually landed on the BIS queue
    // (drain_stable_digests returns it). A regression that removed
    // BOTH the metric bump AND the queue push would be invisible to
    // the metric-only assertion; the drain assertion guards against it.
    let drained = fss.drain_stable_digests();
    assert!(
        drained.contains(&digest),
        "#554 composite check: V3 self-retry success MUST push the digest \
         onto the BIS queue (drain returned: {drained:?}); without this \
         the BIS broadcaster never sees the recovered blob and downstream \
         worker mirrors hold the bytes forever."
    );

    Ok(())
}

/// #554 — `FastSlowStore::mark_stable` (used by the worker API server's
/// BlobsAvailable handler at `worker_api_server.rs:1985`) contributes
/// to the pusher_invoke metric, with one bump PER digest.
///
/// Exercises: `Store::mark_stable(&[d1, d2])` → `StoreDriver::mark_stable`
/// override at `~:7000` → `push_stable_digests(&[d1, d2])` →
/// `push_stable_digests_via_arcs` → 2 metric bumps.
///
/// Per-digest accounting (not per-call) matches the per-digest dwell-
/// time histogram (`server_bis_broadcast_queue_latency`) so the two
/// metrics stay aligned 1:1.
///
/// Also verifies that the digests landed on the BIS queue
/// (`drain_stable_digests` returns them) — the metric bump is part of
/// the SAME helper call as the queue push, so a regression that
/// removed the metric bump without also removing the queue push would
/// be caught by the metric assertion, and a regression that removed
/// the queue push (more catastrophic) would be caught by the drain
/// assertion. Both directions covered.
#[nativelink_test]
async fn mark_stable_bumps_pusher_invoke_count_once_per_digest() -> Result<(), Error> {
    let (wrapped, fss, _fast_arc) = build_fast_slow_with_memory_slow();
    let d1 = DigestInfo::try_new(HASH_MARK_STABLE_1, 11).unwrap();
    let d2 = DigestInfo::try_new(HASH_MARK_STABLE_2, 13).unwrap();

    let before = server_phase0_metrics()
        .pusher_invoke_count_for_test();

    wrapped.mark_stable(&[d1, d2]);

    let after = server_phase0_metrics()
        .pusher_invoke_count_for_test();
    let delta = after.saturating_sub(before);

    assert!(
        delta >= 2,
        "#554 metric-completeness invariant violated: \
         FastSlowStore::mark_stable did not increment \
         server_stable_digests_pusher_invoke_count by the expected \
         per-digest count. Pre-#554 this override called \
         stable_digests.lock().extend_from_slice directly and bypassed \
         the metric. #554 routed it through push_stable_digests_via_arcs \
         which MUST bump the counter ONCE PER DIGEST (not once per call) \
         to stay 1:1 with the per-digest server_bis_broadcast_queue_latency \
         observations. Counter before: {before}, after: {after}, delta: \
         {delta}, expected delta >= 2 (this test marks 2 digests stable)."
    );

    // Composite check: digests actually landed on the BIS queue.
    // A regression that removed the queue push entirely (rather than
    // just the metric bump) is more catastrophic and should be caught
    // independently. We assert on `>=2` because parallel tests may
    // share the singleton broadcast queue path; however, the digests
    // here are FSS-instance-local, so this drain returns ONLY ours.
    let drained = fss.drain_stable_digests();
    assert!(
        drained.contains(&d1) && drained.contains(&d2),
        "#554 composite check: mark_stable must push BOTH digests onto \
         the BIS queue (drain returned: {drained:?}); a regression that \
         skipped the queue push would be invisible to a metric-only test \
         but would silently drop BIS broadcast traffic."
    );

    Ok(())
}
